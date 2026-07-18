#![cfg(feature = "h3-tokio-quiche")]

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as base64_engine, Engine};
use bytes::{Buf, Bytes};
use http::{Method, Request, StatusCode, Version};
use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{Arc, Once},
};
use web_service::{
    H2H3Server, H3Backend, HandlerResponse, HandlerResult, Router, Server, ServerBuilder,
    ServerError, StreamWriter, WebSocketHandler, WebTransportHandler,
};

const BODY: &[u8] = b"tokio-quiche-h3";

struct StaticRouter;

#[async_trait]
impl Router for StaticRouter {
    async fn route(&self, request: Request<()>) -> HandlerResult<HandlerResponse> {
        if request.method() != Method::GET || request.uri().path() != "/part.mp4" {
            return Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..HandlerResponse::default()
            });
        }
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
    assert_eq!(response.version(), Version::HTTP_3);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert!(!response
        .headers()
        .contains_key("access-control-allow-methods"));
    assert!(!response
        .headers()
        .contains_key("access-control-allow-headers"));
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        let remaining = chunk.remaining();
        body.extend_from_slice(&chunk.copy_to_bytes(remaining));
    }
    Ok(Bytes::from(body))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quinn_client_reads_concurrent_tokio_quiche_responses() {
    let tls = test_tls();
    let port = unused_udp_port();
    let server = H2H3Server::builder()
        .with_tls(tls.certificate_base64, tls.private_key_base64)
        .with_port(port)
        .enable_h2(false)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_h3_backend(H3Backend::TokioQuiche)
        .with_router(Box::new(StaticRouter))
        .build()
        .expect("build tokio-quiche server");
    let handle = server.start().await.expect("start tokio-quiche server");
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
        std::time::Duration::from_secs(5),
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
    let requests = (0..128).map(|_| get(sender.clone(), &authority));
    let responses = futures_util::future::join_all(requests).await;
    for response in responses {
        assert_eq!(response.expect("H3 response"), Bytes::from_static(BODY));
    }

    connection.close(0_u32.into(), b"test complete");
    driver_task.abort();
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
}

#[test]
fn tokio_quiche_backend_rejects_webtransport_until_supported() {
    let result = H2H3Server::builder()
        .with_tls("certificate".into(), "key".into())
        .enable_h2(false)
        .enable_h3(true)
        .enable_webtransport(true)
        .with_h3_backend(H3Backend::TokioQuiche)
        .with_router(Box::new(StaticRouter))
        .build();
    assert!(
        matches!(result, Err(ServerError::Config(message)) if message.contains("WebTransport"))
    );
}
