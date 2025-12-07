use crate::{
    config::ServerConfig,
    error::{H3Error, ServerError, ServerResult},
    traits::{BodyStream, Router, StreamWriter},
};
use bytes::{Buf, Bytes};
use h3::ext::Protocol;
use h3::server::{Connection, RequestStream};
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use h3_webtransport::server::WebTransportSession;
use http::{Method, Response};
use futures_util::stream;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tls_helpers::{load_certs_from_base64, load_keys_from_base64};
use tokio::sync::watch;

pub struct Http3Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
}

pub struct H3StreamWriter {
    stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
}

#[async_trait::async_trait]
impl StreamWriter for H3StreamWriter {
    async fn send_response(&mut self, response: Response<()>) -> Result<(), ServerError> {
        self.stream
            .send_response(response)
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        self.stream
            .send_data(data)
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        self.stream
            .finish()
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }
}

impl Http3Server {
    pub fn new(config: ServerConfig, router: Arc<dyn Router>) -> Self {
        Self { config, router }
    }

    pub async fn start(&self, mut shutdown_rx: watch::Receiver<()>) -> ServerResult<()> {
        let certs = load_certs_from_base64(&self.config.cert_pem_base64)
            .map_err(|e| ServerError::Tls(e.to_string()))?;
        let key = load_keys_from_base64(&self.config.privkey_pem_base64)
            .map_err(|e| ServerError::Tls(e.to_string()))?;

        let mut tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| ServerError::Tls(e.to_string()))?;

        tls_config.max_early_data_size = u32::MAX;
        tls_config.alpn_protocols = vec![b"h3".to_vec()];

        let server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(tls_config).map_err(|e| ServerError::Tls(e.to_string()))?,
        ));

        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.config.port);
        let endpoint = quinn::Endpoint::server(server_config, addr)
            .map_err(|e| ServerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        let router = Arc::clone(&self.router);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                res = endpoint.accept() => {
                    if let Some(new_conn) = res {
                        let router = Arc::clone(&router);
                        tokio::spawn(async move {
                            if let Ok(conn) = new_conn.await {
                                let builder = configure_h3_connection(h3::server::builder(), &Http3Config::default());
                                if let Ok(h3_conn) = builder.build(h3_quinn::Connection::new(conn)).await {
                                    if let Err(e) = handle_h3_connection(h3_conn, router).await {
                                        tracing::error!("Failed to handle HTTP/3 connection: {}", e);
                                    }
                                }
                            }
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_h3_connection(
    mut conn: Connection<h3_quinn::Connection, Bytes>,
    router: Arc<dyn Router>,
) -> Result<(), H3Error> {
    loop {
        match conn.accept().await {
            Ok(Some(resolver)) => {
                let (req, stream) = resolver
                    .resolve_request()
                    .await
                    .map_err(|e| H3Error::Transport(e.to_string()))?;
                let ext = req.extensions();
                if req.method() == &Method::CONNECT
                    && ext.get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT)
                {
                    if let Some(handler) = router.webtransport_handler() {
                        let session = WebTransportSession::accept(req, stream, conn)
                            .await
                            .map_err(|e| H3Error::Transport(e.to_string()))?;
                        handler
                            .handle_session(session)
                            .await
                            .map_err(H3Error::Router)?;
                    } else {
                        return Err(H3Error::Transport(
                            "No WebTransport handler configured".into(),
                        ));
                    }
                    return Ok(());
                }
                let router = Arc::clone(&router);
                let path = req.uri().path().to_string();
                tokio::spawn(async move {
                    if router.is_streaming(&path) {
                        let writer = H3StreamWriter { stream };
                        let _ = router.route_stream(req, Box::new(writer)).await;
                    } else {
                        let _ = handle_h3_request(req, stream, router).await;
                    }
                });
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    Ok(())
}

async fn handle_h3_request(
    req: http::Request<()>,
    mut stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    router: Arc<dyn Router>,
) -> Result<(), H3Error> {
    let path = req.uri().path().to_string();
    if router.has_body_handler(&path) {
        let mut collected = Vec::new();
        while let Some(mut chunk) = stream
            .recv_data()
            .await
            .map_err(|e| H3Error::Transport(e.to_string()))?
        {
            let data = chunk.copy_to_bytes(chunk.remaining());
            collected.push(Bytes::from(data));
        }
        let body_stream: BodyStream =
            Box::pin(stream::iter(collected.into_iter().map(Ok::<_, ServerError>)));
        let handler_response = router
            .route_body(req, body_stream)
            .await
            .map_err(H3Error::Router)?;
        return send_h3_response(handler_response, stream).await;
    }

    let handler_response = router.route(req).await.map_err(H3Error::Router)?;
    send_h3_response(handler_response, stream).await
}

async fn send_h3_response(
    handler_response: crate::traits::HandlerResponse,
    mut stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
) -> Result<(), H3Error> {
    let mut builder = http::Response::builder().status(handler_response.status);
    if let Some(ct) = handler_response.content_type {
        builder = builder.header("content-type", ct);
    }
    if let Some(etag) = handler_response.etag {
        builder = builder.header("etag", etag.to_string());
    }
    for (key, value) in handler_response.headers {
        builder = builder.header(&key, &value);
    }
    let resp = builder
        .header("access-control-allow-origin", "*")
        .header(
            "access-control-allow-methods",
            "GET, POST, PUT, DELETE, OPTIONS",
        )
        .header("access-control-allow-headers", "*")
        .body(())
        .map_err(H3Error::Header)?;

    stream
        .send_response(resp)
        .await
        .map_err(|e| H3Error::Transport(e.to_string()))?;
    if let Some(body) = handler_response.body {
        stream
            .send_data(body)
            .await
            .map_err(|e| H3Error::Transport(e.to_string()))?;
    }
    stream
        .finish()
        .await
        .map_err(|e| H3Error::Transport(e.to_string()))
}

pub struct Http3Config {
    pub enable_webtransport: bool,
    pub enable_connect: bool,
    pub enable_datagram: bool,
    pub max_webtransport_sessions: u64,
    pub send_grease: bool,
}

impl Default for Http3Config {
    fn default() -> Self {
        Self {
            enable_webtransport: true,
            enable_connect: true,
            enable_datagram: true,
            max_webtransport_sessions: 100,
            send_grease: true,
        }
    }
}

fn configure_h3_connection(
    mut builder: h3::server::Builder,
    config: &Http3Config,
) -> h3::server::Builder {
    builder.enable_extended_connect(config.enable_connect);
    builder.enable_datagram(config.enable_datagram);
    builder.send_grease(config.send_grease);
    if config.enable_webtransport {
        builder.enable_webtransport(true);
        builder.max_webtransport_sessions(config.max_webtransport_sessions);
    }
    builder
}
