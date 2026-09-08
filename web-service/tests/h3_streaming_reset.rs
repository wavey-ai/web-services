//! Streaming-response behaviour on both HTTP/3 backends.
//!
//! A handler that fails after sending the response head cannot revise its
//! status, so the stream must be reset rather than ended cleanly — otherwise a
//! truncated body reaches the peer looking complete. The reset mechanism
//! differs per backend (`stop_stream` on quinn, `OutboundFrame::PeerStreamError`
//! on tokio-quiche) and per route kind (`route_stream` owns its writer,
//! `route_body_stream` shares it), so each combination is covered here.

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as base64_engine, Engine};
use bytes::{Buf, Bytes};
use futures_util::StreamExt;
use http::{Request, Response, StatusCode};
use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{Arc, Once},
    time::Duration,
};
use web_service::{
    BodyStream, H2H3Server, H3Backend, HandlerResponse, HandlerResult, Router, Server,
    ServerBuilder, ServerError, StreamWriter, WebSocketHandler, WebTransportHandler,
};

const PARTIAL_BODY: &[u8] = b"partial-body-chunk";

/// `/abort*` sends a head and a chunk then fails; `/complete*` does the same
/// and finishes. The `-body` variants take the `route_body_stream` path.
struct StreamingRouter;

impl StreamingRouter {
    async fn drive(
        path: &str,
        stream_writer: &mut Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        stream_writer.send_response(Response::new(())).await?;
        stream_writer
            .send_data(Bytes::from_static(PARTIAL_BODY))
            .await?;
        if path.starts_with("/abort") {
            return Err(ServerError::Config("handler failed mid-body".into()));
        }
        stream_writer.finish().await
    }
}

#[async_trait]
impl Router for StreamingRouter {
    async fn route(&self, _request: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            status: StatusCode::NOT_FOUND,
            ..HandlerResponse::default()
        })
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == "/abort" || path == "/complete"
    }

    async fn route_stream(
        &self,
        request: Request<()>,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        let path = request.uri().path().to_string();
        Self::drive(&path, &mut stream_writer).await
    }

    fn has_body_stream_handler(&self, path: &str) -> bool {
        path == "/abort-body" || path == "/complete-body"
    }

    async fn route_body_stream(
        &self,
        request: Request<()>,
        mut body: BodyStream,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        let path = request.uri().path().to_string();
        // Drain the request body first so the failure is unambiguously on the
        // response side.
        while let Some(chunk) = body.next().await {
            chunk?;
        }
        Self::drive(&path, &mut stream_writer).await
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

/// Perform one request against a freshly started server and read it to
/// completion. The error may surface at the response head or during the body:
/// the server writes head, chunk and reset back to back, so what matters is
/// that no successful read yields a complete-looking short body.
async fn fetch(
    backend: H3Backend,
    path: &'static str,
) -> Result<(StatusCode, Bytes), String> {
    let tls = test_tls();
    let port = unused_udp_port();
    let server = H2H3Server::builder()
        .with_tls(tls.certificate_base64, tls.private_key_base64)
        .with_port(port)
        .enable_h2(false)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_h3_backend(backend)
        .with_router(Box::new(StreamingRouter))
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

    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let request = Request::builder()
            .method(if path.ends_with("-body") { "POST" } else { "GET" })
            .uri(format!("https://localhost:{port}{path}"))
            .body(())
            .map_err(|error| error.to_string())?;
        let mut stream = sender
            .send_request(request)
            .await
            .map_err(|error| error.to_string())?;
        if path.ends_with("-body") {
            stream
                .send_data(Bytes::from_static(b"request-body"))
                .await
                .map_err(|error| error.to_string())?;
        }
        stream.finish().await.map_err(|error| error.to_string())?;

        let response = stream
            .recv_response()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let mut body = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.map_err(|e| e.to_string())? {
            let remaining = chunk.remaining();
            body.extend_from_slice(&chunk.copy_to_bytes(remaining));
        }
        Ok::<_, String>((status, Bytes::from(body)))
    })
    .await
    .expect("H3 request timed out");

    connection.close(0_u32.into(), b"test complete");
    driver_task.abort();
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    result
}

/// The peer must see a *reset*, not merely some error: a failed handshake or a
/// closed connection would also make `fetch` return `Err`, and would pass a bare
/// `is_err()` check without exercising any of the reset paths.
///
/// The error code is deliberately not pinned. Both backends reset, but they
/// choose different codes for the same handler failure — quinn's
/// `stop_stream` sends H3_INTERNAL_ERROR, tokio-quiche's
/// `OutboundFrame::PeerStreamError` surfaces as H3_REQUEST_CANCELLED. Only the
/// reset itself is the contract here.
async fn assert_aborted(backend: H3Backend, path: &'static str) {
    let error = match fetch(backend, path).await {
        Err(error) => error,
        Ok((status, body)) => panic!(
            "aborted stream on {path} delivered a complete body: {status} {:?}",
            String::from_utf8_lossy(&body)
        ),
    };
    assert!(
        error.contains("Remote reset"),
        "aborted stream on {path} failed without resetting: {error}"
    );
}

async fn assert_completed(backend: H3Backend, path: &'static str) {
    let (status, body) = fetch(backend, path)
        .await
        .unwrap_or_else(|error| panic!("{path} should have completed: {error}"));
    assert_eq!(status, StatusCode::OK, "{path}");
    assert_eq!(body.as_ref(), PARTIAL_BODY, "{path}");
}

// --- quinn backend ---------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quinn_streaming_handler_error_resets_the_stream() {
    assert_aborted(H3Backend::Quinn, "/abort").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quinn_streaming_handler_that_finishes_delivers_the_body() {
    assert_completed(H3Backend::Quinn, "/complete").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quinn_body_stream_handler_error_resets_the_stream() {
    assert_aborted(H3Backend::Quinn, "/abort-body").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quinn_body_stream_handler_that_finishes_delivers_the_body() {
    assert_completed(H3Backend::Quinn, "/complete-body").await;
}

// --- tokio-quiche backend --------------------------------------------------

#[cfg(feature = "h3-tokio-quiche")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_quiche_streaming_handler_error_resets_the_stream() {
    assert_aborted(H3Backend::TokioQuiche, "/abort").await;
}

#[cfg(feature = "h3-tokio-quiche")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_quiche_streaming_handler_that_finishes_delivers_the_body() {
    assert_completed(H3Backend::TokioQuiche, "/complete").await;
}

#[cfg(feature = "h3-tokio-quiche")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_quiche_body_stream_handler_error_resets_the_stream() {
    assert_aborted(H3Backend::TokioQuiche, "/abort-body").await;
}

#[cfg(feature = "h3-tokio-quiche")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_quiche_body_stream_handler_that_finishes_delivers_the_body() {
    assert_completed(H3Backend::TokioQuiche, "/complete-body").await;
}
