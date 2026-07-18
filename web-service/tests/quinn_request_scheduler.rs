use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as base64_engine, Engine};
use bytes::{Buf, Bytes};
use http::{Method, Request, StatusCode};
use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Once,
    },
    time::Duration,
};
use tokio::sync::{Barrier, Notify};
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, Router, Server, ServerBuilder, ServerError,
    StreamWriter, WebSocketHandler, WebTransportHandler,
};

const BODY: &[u8] = b"quinn-request-task";
const SYNCHRONIZED_REQUESTS: usize = 128;

struct GatedRouter {
    ready: Arc<Barrier>,
    task_ids: Arc<Mutex<HashSet<String>>>,
}

struct BlockingRouter {
    active: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    dropped: Arc<Notify>,
}

struct ActiveRequestGuard {
    active: Arc<AtomicUsize>,
    dropped: Arc<Notify>,
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.dropped.notify_one();
    }
}

#[async_trait]
impl Router for GatedRouter {
    async fn route(&self, request: Request<()>) -> HandlerResult<HandlerResponse> {
        if request.method() != Method::GET || request.uri().path() != "/part.mp4" {
            return Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..HandlerResponse::default()
            });
        }

        self.task_ids
            .lock()
            .expect("task ID lock")
            .insert(format!("{:?}", tokio::task::id()));
        self.ready.wait().await;

        Ok(HandlerResponse {
            body: Some(Bytes::from_static(BODY)),
            content_type: Some("video/mp4".into()),
            ..HandlerResponse::default()
        })
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _request: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("not a streaming route".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

#[async_trait]
impl Router for BlockingRouter {
    async fn route(&self, request: Request<()>) -> HandlerResult<HandlerResponse> {
        if request.method() != Method::GET || request.uri().path() != "/part.mp4" {
            return Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..HandlerResponse::default()
            });
        }

        self.active.fetch_add(1, Ordering::AcqRel);
        let _active = ActiveRequestGuard {
            active: Arc::clone(&self.active),
            dropped: Arc::clone(&self.dropped),
        };
        self.entered.notify_one();
        std::future::pending::<()>().await;
        unreachable!("a permanently blocked test route must be cancelled")
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _request: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("not a streaming route".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

fn install_rustls_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn unused_udp_port() -> u16 {
    UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("bind UDP port")
        .local_addr()
        .expect("read UDP port")
        .port()
}

struct TestTls {
    certificate_base64: String,
    private_key_base64: String,
    certificate_der: rustls::pki_types::CertificateDer<'static>,
}

fn test_tls() -> TestTls {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate test certificate");
    TestTls {
        certificate_base64: base64_engine.encode(cert.pem()),
        private_key_base64: base64_engine.encode(key_pair.serialize_pem()),
        certificate_der: cert.der().clone(),
    }
}

fn client_config(
    certificate_der: rustls::pki_types::CertificateDer<'static>,
) -> rustls::ClientConfig {
    install_rustls_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(certificate_der)
        .expect("trust generated test certificate");
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h3".to_vec()];
    config
}

async fn get(
    mut sender: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    authority: &str,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("https://{authority}/part.mp4"))
        .body(())?;
    let mut stream = sender.send_request(request).await?;
    stream.finish().await?;
    let response = stream.recv_response().await?;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        let remaining = chunk.remaining();
        body.extend_from_slice(&chunk.copy_to_bytes(remaining));
    }
    Ok(Bytes::from(body))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synchronized_quinn_requests_run_in_independent_tasks() {
    let tls = test_tls();
    let port = unused_udp_port();
    let task_ids = Arc::new(Mutex::new(HashSet::new()));
    let router = GatedRouter {
        ready: Arc::new(Barrier::new(SYNCHRONIZED_REQUESTS)),
        task_ids: Arc::clone(&task_ids),
    };
    let server = H2H3Server::builder()
        .with_tls(tls.certificate_base64, tls.private_key_base64)
        .with_port(port)
        .enable_h2(false)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_router(Box::new(router))
        .build()
        .expect("build H3 server");
    let handle = server.start().await.expect("start H3 server");
    handle.ready_rx.await.expect("server ready");

    let quinn_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
        h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(client_config(
            tls.certificate_der,
        ))
        .expect("QUIC client TLS"),
    ));
    let mut endpoint =
        h3_quinn::quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .expect("client endpoint");
    endpoint.set_default_client_config(quinn_config);
    let connection = tokio::time::timeout(
        Duration::from_secs(5),
        endpoint
            .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)), "localhost")
            .expect("begin connect"),
    )
    .await
    .expect("connect timeout")
    .expect("connect H3");
    let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(connection.clone()))
        .await
        .expect("start H3 client");
    let driver_task = tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });

    let authority = format!("localhost:{port}");
    let responses = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join_all(
            (0..SYNCHRONIZED_REQUESTS).map(|_| get(sender.clone(), &authority)),
        ),
    )
    .await
    .expect("synchronized H3 requests timed out");
    for response in responses {
        assert_eq!(response.expect("H3 response"), Bytes::from_static(BODY));
    }

    assert_eq!(
        task_ids.lock().expect("task ID lock").len(),
        SYNCHRONIZED_REQUESTS,
        "each simultaneously blocked request must have its own Tokio task"
    );

    connection.close(0_u32.into(), b"test complete");
    driver_task.abort();
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_quinn_connection_cancels_blocked_requests_promptly() {
    let tls = test_tls();
    let port = unused_udp_port();
    let active = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let router = BlockingRouter {
        active: Arc::clone(&active),
        entered: Arc::clone(&entered),
        dropped: Arc::clone(&dropped),
    };
    let server = H2H3Server::builder()
        .with_tls(tls.certificate_base64, tls.private_key_base64)
        .with_port(port)
        .enable_h2(false)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_router(Box::new(router))
        .build()
        .expect("build H3 server");
    let handle = server.start().await.expect("start H3 server");
    handle.ready_rx.await.expect("server ready");

    let quinn_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
        h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(client_config(
            tls.certificate_der,
        ))
        .expect("QUIC client TLS"),
    ));
    let mut endpoint =
        h3_quinn::quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .expect("client endpoint");
    endpoint.set_default_client_config(quinn_config);
    let connection = tokio::time::timeout(
        Duration::from_secs(5),
        endpoint
            .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)), "localhost")
            .expect("begin connect"),
    )
    .await
    .expect("connect timeout")
    .expect("connect H3");
    let (mut driver, mut sender) = h3::client::new(h3_quinn::Connection::new(connection.clone()))
        .await
        .expect("start H3 client");
    let driver_task = tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });

    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("https://localhost:{port}/part.mp4"))
        .body(())
        .expect("build request");
    let mut stream = sender.send_request(request).await.expect("send request");
    stream.finish().await.expect("finish request");
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("blocked route was not entered");
    assert_eq!(active.load(Ordering::Acquire), 1);

    let dropped_request = dropped.notified();
    tokio::pin!(dropped_request);
    dropped_request.as_mut().enable();
    connection.close(0_u32.into(), b"cancel blocked request");
    tokio::time::timeout(Duration::from_millis(250), &mut dropped_request)
        .await
        .expect("blocked request survived its closed QUIC connection");
    assert_eq!(active.load(Ordering::Acquire), 0);

    driver_task.abort();
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
}
