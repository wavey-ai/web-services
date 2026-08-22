use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as base64_engine, Engine as _};
use http::Request;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::sync::Once;
use std::time::Duration;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, Router, Server, ServerBuilder, ServerError,
    StreamWriter, WebSocketHandler, WebTransportHandler,
};

struct EmptyRouter;

#[async_trait]
impl Router for EmptyRouter {
    async fn route(&self, _request: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse::default())
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

fn test_tls() -> (String, String) {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate test certificate");
    (
        base64_engine.encode(cert.pem()),
        base64_engine.encode(key_pair.serialize_pem()),
    )
}

#[tokio::test]
async fn start_reports_a_listener_bind_failure() {
    let occupied =
        TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).expect("reserve TCP port");
    let port = occupied.local_addr().expect("read reserved port").port();
    let (certificate, private_key) = test_tls();
    let server = H2H3Server::builder()
        .with_tls(certificate, private_key)
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
        .with_router(Box::new(EmptyRouter))
        .build()
        .expect("build server");

    let result = tokio::time::timeout(Duration::from_secs(2), server.start())
        .await
        .expect("server startup timed out");
    let error = match result {
        Ok(_) => panic!("occupied port must fail startup"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("failed to start"));
}
