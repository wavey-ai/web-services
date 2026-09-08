mod common;

use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use common::load_test_env;
use futures_util::StreamExt;
use http::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use portpicker::pick_unused_port;
use tokio::sync::{Notify, Semaphore};
use tokio_rustls::rustls::{self, pki_types::ServerName, ClientConfig, RootCertStore};
use tokio_tungstenite::WebSocketStream;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, Router, Server, ServerBuilder, ServerError,
    ServerHandle, StreamWriter, WebSocketHandler, WebTransportHandler,
};

const PARTIAL_BODY: &[u8] = b"partial-body-chunk";

struct TestServer {
    shutdown_tx: tokio::sync::watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone)]
struct LimitState {
    hold_started: Arc<Notify>,
    hold_release: Arc<Semaphore>,
    websocket_started: Arc<Notify>,
    stream_started: Arc<Notify>,
    stream_dropped: Arc<Notify>,
}

impl LimitState {
    fn new() -> Self {
        Self {
            hold_started: Arc::new(Notify::new()),
            hold_release: Arc::new(Semaphore::new(0)),
            websocket_started: Arc::new(Notify::new()),
            stream_started: Arc::new(Notify::new()),
            stream_dropped: Arc::new(Notify::new()),
        }
    }
}

struct StreamDropGuard(Arc<Notify>);

impl Drop for StreamDropGuard {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

struct LimitWebSocketHandler {
    state: LimitState,
}

#[async_trait]
impl WebSocketHandler for LimitWebSocketHandler {
    async fn handle_websocket(
        &self,
        _req: Request<()>,
        mut stream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    ) -> HandlerResult<()> {
        self.state.websocket_started.notify_one();
        while let Some(message) = stream.next().await {
            if message.is_err() {
                break;
            }
        }
        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        path == "/ws"
    }
}

struct LimitRouter {
    state: LimitState,
    websocket: LimitWebSocketHandler,
}

impl LimitRouter {
    fn new(state: LimitState) -> Self {
        Self {
            websocket: LimitWebSocketHandler {
                state: state.clone(),
            },
            state,
        }
    }
}

#[async_trait]
impl Router for LimitRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if req.uri().path() == "/hold" {
            self.state.hold_started.notify_one();
            let permit = self
                .state
                .hold_release
                .acquire()
                .await
                .map_err(|_| ServerError::Config("test release closed".into()))?;
            permit.forget();
        }
        Ok(HandlerResponse {
            status: StatusCode::OK,
            ..Default::default()
        })
    }

    fn is_streaming(&self, path: &str) -> bool {
        matches!(path, "/stream" | "/abort" | "/complete")
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        match req.uri().path() {
            // Head, a partial body, then failure without `finish`. The peer
            // must not be able to read this as a complete response.
            "/abort" => {
                stream_writer.send_response(Response::new(())).await?;
                stream_writer
                    .send_data(Bytes::from_static(PARTIAL_BODY))
                    .await?;
                return Err(ServerError::Config("handler failed mid-body".into()));
            }
            // Control: the same shape, completed properly.
            "/complete" => {
                stream_writer.send_response(Response::new(())).await?;
                stream_writer
                    .send_data(Bytes::from_static(PARTIAL_BODY))
                    .await?;
                stream_writer.finish().await?;
                return Ok(());
            }
            _ => {}
        }
        stream_writer.send_response(Response::new(())).await?;
        self.state.stream_started.notify_one();
        let _guard = StreamDropGuard(Arc::clone(&self.state.stream_dropped));
        std::future::pending::<()>().await;
        unreachable!("streaming test handler must be cancelled")
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        self.websocket
            .can_handle(path)
            .then_some(&self.websocket as &dyn WebSocketHandler)
    }
}

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

async fn start_server(
    state: LimitState,
    max_connections: usize,
    max_requests: usize,
) -> (TestServer, u16, String, String) {
    ensure_rustls_provider();
    let (certificate, private_key, host) = load_test_env().expect("test TLS material");
    let port = pick_unused_port().expect("unused test port");
    let server = H2H3Server::builder()
        .with_tls(certificate.clone(), private_key)
        .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_websocket(true)
        .enable_webtransport(false)
        .with_max_connections(max_connections)
        .with_max_in_flight_requests(max_requests)
        .with_router(Box::new(LimitRouter::new(state)))
        .build()
        .unwrap();
    let ServerHandle {
        shutdown_tx,
        ready_rx,
        finished_rx,
    } = server.start().await.unwrap();
    ready_rx.await.expect("server readiness");
    (
        TestServer {
            shutdown_tx,
            finished_rx,
        },
        port,
        certificate,
        host,
    )
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn http2_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn websocket_tls_config(certificate_base64: &str) -> ClientConfig {
    let certificate_pem = STANDARD.decode(certificate_base64).unwrap();
    let mut reader = Cursor::new(certificate_pem);
    let mut roots = RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut reader) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

async fn connect_websocket(
    port: u16,
    host: &str,
    certificate_base64: &str,
) -> WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let tcp =
        tokio::net::TcpStream::connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
            .await
            .unwrap();
    let connector =
        tokio_rustls::TlsConnector::from(Arc::new(websocket_tls_config(certificate_base64)));
    let server_name = ServerName::try_from(host.to_string()).unwrap();
    let tls = connector.connect(server_name, tcp).await.unwrap();
    let url = format!("wss://{host}:{port}/ws");
    let (stream, response) = tokio_tungstenite::client_async(url, tls).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    stream
}

async fn stop_server(handle: TestServer) {
    let _ = handle.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), handle.finished_rx)
        .await
        .expect("server shutdown timed out")
        .expect("server supervisor dropped completion signal");
}

#[tokio::test(flavor = "multi_thread")]
async fn global_limit_rejects_work_across_connections() {
    let state = LimitState::new();
    let (handle, port, _, _) = start_server(state.clone(), 4, 1).await;
    let origin = format!("https://127.0.0.1:{port}");
    let first_client = http_client();
    let first =
        tokio::spawn(async move { first_client.get(format!("{origin}/hold")).send().await });

    tokio::time::timeout(Duration::from_secs(1), state.hold_started.notified())
        .await
        .expect("holding request did not start");
    let overloaded = http_client()
        .get(format!("https://127.0.0.1:{port}/fast"))
        .send()
        .await
        .unwrap();
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(overloaded.headers()["retry-after"], "1");

    state.hold_release.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
    stop_server(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_holds_request_capacity_until_close() {
    let state = LimitState::new();
    let (handle, port, certificate, host) = start_server(state.clone(), 2, 1).await;
    let mut websocket = connect_websocket(port, &host, &certificate).await;
    tokio::time::timeout(Duration::from_secs(1), state.websocket_started.notified())
        .await
        .expect("WebSocket handler did not start");

    let overloaded = http_client()
        .get(format!("https://127.0.0.1:{port}/fast"))
        .send()
        .await
        .unwrap();
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);

    websocket.close(None).await.unwrap();
    let client = http_client();
    let response = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(response) = client
                .get(format!("https://127.0.0.1:{port}/fast"))
                .send()
                .await
            {
                if response.status() == StatusCode::OK {
                    break response;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("WebSocket request permit was not released");
    assert_eq!(response.status(), StatusCode::OK);
    stop_server(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_client_disconnect_releases_request_capacity() {
    let state = LimitState::new();
    let (handle, port, _, _) = start_server(state.clone(), 2, 1).await;
    let streaming = http_client()
        .get(format!("https://127.0.0.1:{port}/stream"))
        .send()
        .await
        .unwrap();
    assert_eq!(streaming.status(), StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(1), state.stream_started.notified())
        .await
        .expect("streaming handler did not start");

    drop(streaming);
    tokio::time::timeout(Duration::from_secs(1), state.stream_dropped.notified())
        .await
        .expect("disconnected streaming handler retained its request permit");
    let response = http_client()
        .get(format!("https://127.0.0.1:{port}/fast"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    stop_server(handle).await;
}

/// A handler that fails after sending the response head must not leave the
/// peer holding a short body that looks complete. The head is already on the
/// wire and its status cannot be revised, so the transfer has to be aborted.
///
/// The error may surface either while awaiting the head or while reading the
/// body — the server writes the head, the partial chunk and the abort back to
/// back, so a client can observe the abort before it yields the response. What
/// matters is that no client path ends with a complete body.
async fn read_aborted_stream(
    client: reqwest::Client,
    url: String,
) -> Result<Bytes, reqwest::Error> {
    client.get(url).send().await?.bytes().await
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_handler_error_after_head_aborts_the_body_over_http1() {
    let state = LimitState::new();
    let (handle, port, _, _) = start_server(state, 4, 4).await;

    // HTTP/1.1 has no stream reset, so an aborted body tears down the
    // connection. Either way the client sees a transport error, not a body.
    let result =
        read_aborted_stream(http_client(), format!("https://127.0.0.1:{port}/abort")).await;
    assert!(
        result.is_err(),
        "aborted stream delivered a complete body: {:?}",
        result.map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    );

    stop_server(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_handler_error_after_head_resets_the_stream_over_http2() {
    let state = LimitState::new();
    let (handle, port, _, _) = start_server(state, 4, 4).await;

    let result =
        read_aborted_stream(http2_client(), format!("https://127.0.0.1:{port}/abort")).await;
    let error = match result {
        Ok(body) => panic!(
            "aborted stream delivered a complete body: {:?}",
            String::from_utf8_lossy(&body)
        ),
        Err(error) => format!("{error:?}"),
    };
    // Pin the mechanism, not just the failure: the peer must see RST_STREAM
    // with INTERNAL_ERROR rather than a dropped connection or a short body.
    assert!(
        error.contains("Reset") && error.contains("INTERNAL_ERROR"),
        "expected an HTTP/2 stream reset, got: {error}"
    );

    stop_server(handle).await;
}

/// Control for the two tests above: the same handler shape, finished properly,
/// still delivers its body intact. Without this a reset-everything regression
/// would keep those tests green.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_handler_that_finishes_delivers_a_complete_body() {
    let state = LimitState::new();
    let (handle, port, _, _) = start_server(state, 4, 4).await;

    for (label, client) in [("http/1.1", http_client()), ("h2", http2_client())] {
        let response = client
            .get(format!("https://127.0.0.1:{port}/complete"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} request failed: {error}"));
        assert_eq!(response.status(), StatusCode::OK, "{label}");
        let body = response
            .bytes()
            .await
            .unwrap_or_else(|error| panic!("{label} body failed: {error}"));
        assert_eq!(body.as_ref(), PARTIAL_BODY, "{label}");
    }

    stop_server(handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn active_websocket_is_owned_and_cancelled_during_shutdown() {
    let state = LimitState::new();
    let (handle, port, certificate, host) = start_server(state.clone(), 1, 2).await;
    let _websocket = connect_websocket(port, &host, &certificate).await;
    tokio::time::timeout(Duration::from_secs(1), state.websocket_started.notified())
        .await
        .expect("WebSocket handler did not start");

    let blocked_connection = http_client()
        .get(format!("https://127.0.0.1:{port}/fast"))
        .send()
        .await;
    assert!(
        blocked_connection.is_err(),
        "WebSocket released its TCP connection permit"
    );

    stop_server(handle).await;
}
