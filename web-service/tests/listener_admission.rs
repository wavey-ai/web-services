//! What the HTTP/1.1+HTTP/2 listener takes on and how it lets go: queued admission, exempt
//! health checks, connection backpressure, request ids, body caps, errors and shutdown drain.

mod common;

use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::load_test_env;
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use tokio::sync::{Notify, Semaphore};
use tokio_rustls::rustls;
use web_service::{
    H2H3Server, H2H3ServerBuilder, HandlerResponse, HandlerResult, Router, Server, ServerBuilder,
    ServerError, ServerHandle, StreamWriter, WebSocketHandler, WebTransportHandler,
};

/// `/hold` waits for a release; `/fail` errors; everything else answers at once.
#[derive(Clone)]
struct Gate {
    started: Arc<Notify>,
    release: Arc<Semaphore>,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            release: Arc::new(Semaphore::new(0)),
        }
    }
}

#[async_trait]
impl Router for Gate {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        match req.uri().path() {
            "/hold" => {
                self.started.notify_one();
                self.release
                    .acquire()
                    .await
                    .map_err(|_| ServerError::Config("test release closed".into()))?
                    .forget();
            }
            "/fail" => return Err(ServerError::Config("handler failed".into())),
            _ => {}
        }
        Ok(HandlerResponse {
            body: Some(Bytes::from_static(b"ok")),
            ..Default::default()
        })
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Ok(())
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// A TLS HTTP/1.1+HTTP/2 server on a free port, shaped by `configure`.
async fn start(
    gate: Gate,
    configure: impl FnOnce(H2H3ServerBuilder) -> H2H3ServerBuilder,
) -> (ServerHandle, String) {
    ensure_rustls_provider();
    let (certificate, private_key, _) = load_test_env().expect("test TLS material");
    let port = pick_unused_port().expect("unused test port");
    let builder = H2H3Server::builder()
        .with_tls(certificate, private_key)
        .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_webtransport(false)
        .with_router(Box::new(gate));
    let handle = configure(builder).build().unwrap().start().await.unwrap();
    (handle, format!("https://127.0.0.1:{port}"))
}

/// An HTTP/1.1 client that closes each connection after its response, so a held connection
/// slot is only ever a request in flight.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

async fn get(origin: &str, path: &str) -> reqwest::Response {
    client().get(format!("{origin}{path}")).send().await.unwrap()
}

async fn hold(gate: &Gate, origin: &str) -> tokio::task::JoinHandle<reqwest::Response> {
    let held = tokio::spawn({
        let origin = origin.to_owned();
        async move { get(&origin, "/hold").await }
    });
    tokio::time::timeout(Duration::from_secs(2), gate.started.notified())
        .await
        .expect("held request did not start");
    held
}

async fn stop(handle: ServerHandle) {
    let _ = handle.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(5), handle.finished_rx)
        .await
        .expect("server shutdown timed out")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_waits_for_a_slot_and_is_served_when_one_frees() {
    let gate = Gate::default();
    let (handle, origin) = start(gate.clone(), |builder| {
        builder
            .with_max_in_flight_requests(1)
            .with_request_queue_timeout_ms(5_000)
    })
    .await;
    let held = hold(&gate, &origin).await;

    let queued = tokio::spawn({
        let origin = origin.clone();
        async move { get(&origin, "/fast").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!queued.is_finished(), "a full server answered at once");

    gate.release.add_permits(1);
    assert_eq!(held.await.unwrap().status(), StatusCode::OK);
    assert_eq!(queued.await.unwrap().status(), StatusCode::OK);
    stop(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_waits_too_long_is_refused_but_health_checks_are_not() {
    let gate = Gate::default();
    let (handle, origin) = start(gate.clone(), |builder| {
        builder
            .with_max_in_flight_requests(1)
            .with_request_queue_timeout_ms(100)
            .with_limit_exempt_paths(["/health"])
    })
    .await;
    let held = hold(&gate, &origin).await;

    let refused = get(&origin, "/fast").await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(refused.headers()["retry-after"], "1");
    assert!(refused.headers().contains_key("x-request-id"));
    assert_eq!(get(&origin, "/health").await.status(), StatusCode::OK);

    gate.release.add_permits(1);
    assert_eq!(held.await.unwrap().status(), StatusCode::OK);
    assert_eq!(get(&origin, "/fast").await.status(), StatusCode::OK);
    stop(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn past_the_connection_limit_new_connections_wait_instead_of_being_dropped() {
    let gate = Gate::default();
    let (handle, origin) = start(gate.clone(), |builder| builder.with_max_connections(1)).await;
    let held = hold(&gate, &origin).await;

    let waiting = tokio::spawn({
        let origin = origin.clone();
        async move { get(&origin, "/fast").await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished(), "a connection past the limit was served or dropped");

    gate.release.add_permits(1);
    assert_eq!(held.await.unwrap().status(), StatusCode::OK);
    assert_eq!(waiting.await.unwrap().status(), StatusCode::OK);
    stop(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_answer_carries_the_callers_request_id_or_a_new_one() {
    let (handle, origin) = start(Gate::default(), |builder| builder).await;
    let kept = client()
        .get(format!("{origin}/fast"))
        .header("x-request-id", "caller-123")
        .send()
        .await
        .unwrap();
    assert_eq!(kept.headers()["x-request-id"], "caller-123");
    let minted = get(&origin, "/fast").await;
    let minted = minted.headers()["x-request-id"].to_str().unwrap();
    assert!(!minted.is_empty() && minted != "caller-123");
    stop(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_handler_is_answered_500_and_the_connection_survives() {
    let (handle, origin) = start(Gate::default(), |builder| builder).await;
    let keep_alive = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .build()
        .unwrap();
    let failed = keep_alive.get(format!("{origin}/fail")).send().await.unwrap();
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let next = keep_alive.get(format!("{origin}/fast")).send().await.unwrap();
    assert_eq!(next.status(), StatusCode::OK);
    stop(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_body_to_a_route_that_reads_none_is_refused() {
    let (handle, origin) =
        start(Gate::default(), |builder| builder.with_max_unread_body_bytes(1024)).await;
    let small = client()
        .post(format!("{origin}/fast"))
        .body(vec![b'x'; 512])
        .send()
        .await
        .unwrap();
    assert_eq!(small.status(), StatusCode::OK);
    let large = client()
        .post(format!("{origin}/fast"))
        .body(vec![b'x'; 64 * 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(large.status(), StatusCode::PAYLOAD_TOO_LARGE);
    stop(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_lets_requests_in_flight_finish_within_the_drain() {
    let gate = Gate::default();
    let (handle, origin) =
        start(gate.clone(), |builder| builder.with_shutdown_drain_ms(5_000)).await;
    let held = hold(&gate, &origin).await;

    let _ = handle.shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release.add_permits(1);
    assert_eq!(held.await.unwrap().status(), StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(5), handle.finished_rx)
        .await
        .expect("server did not stop after its connections drained")
        .unwrap();
}

#[cfg(feature = "plain-http")]
mod plain_http {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_plain_listener_serves_http1_with_the_same_limits() {
        let gate = Gate::default();
        let port = pick_unused_port().expect("unused test port");
        let handle = H2H3Server::builder()
            .with_plain_http()
            .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
            .with_port(port)
            .with_max_in_flight_requests(1)
            .with_limit_exempt_paths(["/health"])
            .with_router(Box::new(gate.clone()))
            .build()
            .unwrap()
            .start()
            .await
            .unwrap();
        let origin = format!("http://127.0.0.1:{port}");
        let held = hold(&gate, &origin).await;

        let get = |path: &'static str| async move {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let request = format!("GET {path} HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response.to_ascii_lowercase()
        };
        let refused = get("/fast").await;
        assert!(refused.starts_with("http/1.1 503"), "{refused}");
        assert!(refused.contains("x-request-id: "), "{refused}");
        assert!(get("/health").await.starts_with("http/1.1 200"));

        gate.release.add_permits(1);
        assert_eq!(held.await.unwrap().status(), StatusCode::OK);
        assert!(get("/fast").await.starts_with("http/1.1 200"));
        stop(handle).await;
    }

    #[test]
    fn a_plain_listener_needs_no_certificate_and_refuses_http3() {
        let plain = || {
            H2H3Server::builder()
                .with_plain_http()
                .with_router(Box::new(Gate::default()))
        };
        assert!(plain().build().is_ok());
        assert!(plain().enable_h3(true).build().is_err());
        assert!(plain().with_client_ca("ca".into()).build().is_err());
    }
}
