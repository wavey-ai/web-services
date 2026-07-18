use crate::{
    config::ServerConfig,
    error::{H3Error, ServerError, ServerResult},
    http_range::apply_byte_range,
    traits::{response_header_name, response_header_value, BodyStream, Router, StreamWriter},
};
use bytes::{Buf, Bytes};
use futures_util::stream::unfold;
use h3::ext::Protocol;
use h3::server::{Connection, RequestStream};
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use h3_webtransport::server::WebTransportSession;
use http::{header::RANGE, HeaderName, HeaderValue, Method, Response, StatusCode};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tls_helpers::{load_certs_from_base64, load_keys_from_base64};
use tokio::{
    sync::{watch, Mutex},
    task::{JoinError, JoinSet},
};

// Stay below the 16,384-byte QUIC varint boundary to avoid 4-byte length encodings.
const H3_MAX_DATA_CHUNK: usize = 16 * 1024 - 1;

// Keep request work independently schedulable while bounding the tasks retained by a connection.
// This matches the QUIC concurrent bidirectional-stream limit configured below.
const H3_MAX_IN_FLIGHT_REQUESTS: usize = 256;

pub struct Http3Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
}

pub struct H3StreamWriter {
    stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
}

#[async_trait::async_trait]
impl StreamWriter for H3StreamWriter {
    async fn send_response(&mut self, mut response: Response<()>) -> Result<(), ServerError> {
        add_cors_response_headers(&mut response);
        self.stream
            .send_response(response)
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        if data.len() <= H3_MAX_DATA_CHUNK {
            return self
                .stream
                .send_data(data)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))));
        }

        let mut remaining = data;
        while remaining.len() > H3_MAX_DATA_CHUNK {
            let chunk = remaining.split_to(H3_MAX_DATA_CHUNK);
            self.stream
                .send_data(chunk)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))?;
        }
        if !remaining.is_empty() {
            self.stream
                .send_data(remaining)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        self.stream
            .finish()
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }
}

/// Stream writer that uses a shared (mutex-protected) stream for bidirectional streaming
pub struct H3SharedStreamWriter {
    stream: Arc<Mutex<RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>>>,
}

#[async_trait::async_trait]
impl StreamWriter for H3SharedStreamWriter {
    async fn send_response(&mut self, mut response: Response<()>) -> Result<(), ServerError> {
        add_cors_response_headers(&mut response);
        let mut guard = self.stream.lock().await;
        guard
            .send_response(response)
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        let mut guard = self.stream.lock().await;
        if data.len() <= H3_MAX_DATA_CHUNK {
            return guard
                .send_data(data)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))));
        }

        let mut remaining = data;
        while remaining.len() > H3_MAX_DATA_CHUNK {
            let chunk = remaining.split_to(H3_MAX_DATA_CHUNK);
            guard
                .send_data(chunk)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))?;
        }
        if !remaining.is_empty() {
            guard
                .send_data(remaining)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        // Don't finish here - the caller will finish after route_body_stream returns
        Ok(())
    }
}

/// Stream writer using split send half for true bidirectional streaming without lock contention
pub struct H3SplitStreamWriter {
    stream: Arc<Mutex<RequestStream<h3_quinn::SendStream<Bytes>, Bytes>>>,
}

#[async_trait::async_trait]
impl StreamWriter for H3SplitStreamWriter {
    async fn send_response(&mut self, mut response: Response<()>) -> Result<(), ServerError> {
        add_cors_response_headers(&mut response);
        let mut guard = self.stream.lock().await;
        guard
            .send_response(response)
            .await
            .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        let mut guard = self.stream.lock().await;
        if data.len() <= H3_MAX_DATA_CHUNK {
            return guard
                .send_data(data)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))));
        }

        let mut remaining = data;
        while remaining.len() > H3_MAX_DATA_CHUNK {
            let chunk = remaining.split_to(H3_MAX_DATA_CHUNK);
            guard
                .send_data(chunk)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))?;
        }
        if !remaining.is_empty() {
            guard
                .send_data(remaining)
                .await
                .map_err(|e| ServerError::Handler(Box::new(H3Error::Transport(e.to_string()))))?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        // Don't finish here - the caller will finish after route_body_stream returns
        Ok(())
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

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(tls_config).map_err(|e| ServerError::Tls(e.to_string()))?,
        ));
        server_config.transport_config(Arc::new(build_quic_transport_config()));

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
                        let config = self.config.clone();
                        tokio::spawn(async move {
                            if let Ok(conn) = new_conn.await {
                                let h3_config = Http3Config::from_server_config(&config);
                                let builder = configure_h3_connection(h3::server::builder(), &h3_config);
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
    let mut request_tasks = JoinSet::new();
    loop {
        tokio::select! {
            accepted = conn.accept(), if request_tasks.len() < H3_MAX_IN_FLIGHT_REQUESTS => match accepted {
                Ok(Some(resolver)) => {
                    let resolving = resolver.resolve_request();
                    tokio::pin!(resolving);
                    let resolved = loop {
                        tokio::select! {
                            resolved = &mut resolving => break resolved,
                            Some(result) = request_tasks.join_next(), if !request_tasks.is_empty() => {
                                log_h3_request_task_result(result, "HTTP/3 request failed");
                            }
                        }
                    };
                    let (req, stream) = resolved
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
                            send_h3_empty_response(StatusCode::NOT_FOUND, stream).await?;
                            continue;
                        }
                        return Ok(());
                    }
                    let router = Arc::clone(&router);
                    let has_body_stream_handler = router.has_body_stream_handler(req.uri().path());
                    let is_streaming =
                        !has_body_stream_handler && router.is_streaming(req.uri().path());
                    let has_body_handler = !has_body_stream_handler
                        && !is_streaming
                        && router.has_body_handler(req.uri().path());
                    request_tasks.spawn(dispatch_h3_request(
                        req,
                        stream,
                        router,
                        has_body_stream_handler,
                        is_streaming,
                        has_body_handler,
                    ));
                }
                Ok(None) => break,
                Err(_) => break,
            },
            Some(result) = request_tasks.join_next(), if !request_tasks.is_empty() => {
                log_h3_request_task_result(result, "HTTP/3 request failed");
            }
        }
    }

    // A request can be blocked waiting for future live media. Once the owning
    // QUIC connection is gone, keeping those tasks alive until their router
    // deadline wastes capacity and lets one load tier bleed into the next.
    request_tasks.abort_all();
    while let Some(result) = request_tasks.join_next().await {
        log_h3_request_task_result(result, "HTTP/3 request failed while closing connection");
    }
    Ok(())
}

fn log_h3_request_task_result(
    result: Result<Result<(), H3Error>, JoinError>,
    message: &'static str,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, message),
        Err(error) if error.is_cancelled() => {}
        Err(error) => tracing::warn!(%error, message = "HTTP/3 request task failed"),
    }
}

async fn dispatch_h3_request(
    req: http::Request<()>,
    stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    router: Arc<dyn Router>,
    has_body_stream_handler: bool,
    is_streaming: bool,
    has_body_handler: bool,
) -> Result<(), H3Error> {
    if has_body_stream_handler {
        handle_h3_body_stream_request(req, stream, router).await
    } else if is_streaming {
        let writer = H3StreamWriter { stream };
        router
            .route_stream(req, Box::new(writer))
            .await
            .map_err(H3Error::Router)
    } else {
        handle_h3_request(req, stream, router, has_body_handler).await
    }
}

async fn handle_h3_body_stream_request(
    req: http::Request<()>,
    stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    router: Arc<dyn Router>,
) -> Result<(), H3Error> {
    let shared_stream = Arc::new(Mutex::new(stream));
    let body_stream: BodyStream =
        Box::pin(unfold(Arc::clone(&shared_stream), |shared| async move {
            let recv = {
                let mut guard = shared.lock().await;
                guard.recv_data().await
            };
            match recv {
                Ok(Some(mut chunk)) => {
                    let data = chunk.copy_to_bytes(chunk.remaining());
                    Some((Ok(data), shared))
                }
                Ok(None) => None,
                Err(e) => Some((Err(ServerError::Handler(Box::new(e))), shared)),
            }
        }));
    let writer = H3SharedStreamWriter {
        stream: Arc::clone(&shared_stream),
    };
    router
        .route_body_stream(req, body_stream, Box::new(writer))
        .await
        .map_err(H3Error::Router)?;
    let mut guard = shared_stream.lock().await;
    guard
        .finish()
        .await
        .map_err(|e| H3Error::Transport(e.to_string()))?;
    Ok(())
}

async fn handle_h3_request(
    req: http::Request<()>,
    mut stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    router: Arc<dyn Router>,
    has_body_handler: bool,
) -> Result<(), H3Error> {
    let is_preflight = req.method() == Method::OPTIONS;
    let range_header = req.headers().get(RANGE).cloned();
    if has_body_handler {
        let shared_stream = Arc::new(Mutex::new(stream));
        let body_stream: BodyStream =
            Box::pin(unfold(Arc::clone(&shared_stream), |shared| async move {
                let recv = {
                    let mut guard = shared.lock().await;
                    guard.recv_data().await
                };
                match recv {
                    Ok(Some(mut chunk)) => {
                        let data = chunk.copy_to_bytes(chunk.remaining());
                        Some((Ok(data), shared))
                    }
                    Ok(None) => None,
                    Err(e) => Some((Err(ServerError::Handler(Box::new(e))), shared)),
                }
            }));
        let handler_response = router
            .route_body(req, body_stream)
            .await
            .map_err(H3Error::Router)?;
        let mut guard = shared_stream.lock().await;
        return send_h3_response(
            apply_byte_range(range_header.as_ref(), handler_response),
            &mut *guard,
            is_preflight,
        )
        .await;
    }

    let handler_response = router.route(req).await.map_err(H3Error::Router)?;
    send_h3_response(
        apply_byte_range(range_header.as_ref(), handler_response),
        &mut stream,
        is_preflight,
    )
    .await
}

async fn send_h3_response(
    handler_response: crate::traits::HandlerResponse,
    stream: &mut RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    is_preflight: bool,
) -> Result<(), H3Error> {
    let mut builder = http::Response::builder().status(handler_response.status);
    if let Some(ct) = handler_response.content_type {
        builder = builder.header(
            "content-type",
            response_header_value(ct).map_err(|error| H3Error::Header(error.into()))?,
        );
    }
    if let Some(etag) = handler_response.etag {
        builder = builder.header("etag", etag.to_string());
    }
    for (key, value) in handler_response.headers {
        builder = builder.header(
            response_header_name(key).map_err(|error| H3Error::Header(error.into()))?,
            response_header_value(value).map_err(|error| H3Error::Header(error.into()))?,
        );
    }
    let mut resp = builder.body(()).map_err(H3Error::Header)?;
    if is_preflight {
        add_cors_preflight_headers(&mut resp);
    } else {
        add_cors_response_headers(&mut resp);
    }

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

impl Http3Config {
    fn from_server_config(config: &ServerConfig) -> Self {
        Self {
            enable_webtransport: config.enable_webtransport,
            ..Self::default()
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

async fn send_h3_empty_response(
    status: StatusCode,
    mut stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
) -> Result<(), H3Error> {
    let mut response = Response::builder()
        .status(status)
        .body(())
        .map_err(H3Error::Header)?;
    add_cors_response_headers(&mut response);
    stream
        .send_response(response)
        .await
        .map_err(|e| H3Error::Transport(e.to_string()))?;
    stream
        .finish()
        .await
        .map_err(|e| H3Error::Transport(e.to_string()))
}

pub(crate) fn add_cors_response_headers<B>(res: &mut Response<B>) {
    res.headers_mut().insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_static("*"),
    );
    res.headers_mut()
        .remove(HeaderName::from_static("access-control-allow-methods"));
    res.headers_mut()
        .remove(HeaderName::from_static("access-control-allow-headers"));
    res.headers_mut().insert(
        HeaderName::from_static("access-control-expose-headers"),
        HeaderValue::from_static(
            "x-sequence, stream-id, etag, content-length, accept-ranges, content-range",
        ),
    );
}

pub(crate) fn add_cors_preflight_headers<B>(res: &mut Response<B>) {
    add_cors_response_headers(res);
    res.headers_mut().insert(
        HeaderName::from_static("access-control-allow-methods"),
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    res.headers_mut().insert(
        HeaderName::from_static("access-control-allow-headers"),
        HeaderValue::from_static("*"),
    );
}

fn build_quic_transport_config() -> quinn::TransportConfig {
    // Increase flow-control headroom for large uploads and responses.
    const STREAM_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
    const MAX_CONCURRENT_STREAMS: u32 = 256;

    let mut transport = quinn::TransportConfig::default();
    transport
        .stream_receive_window(STREAM_WINDOW_BYTES.into())
        .max_concurrent_bidi_streams(MAX_CONCURRENT_STREAMS.into())
        .max_concurrent_uni_streams(MAX_CONCURRENT_STREAMS.into());
    transport
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_cors_response_omits_preflight_only_fields() {
        let mut response = Response::builder()
            .header("access-control-allow-methods", "GET")
            .header("access-control-allow-headers", "range")
            .body(())
            .unwrap();
        add_cors_response_headers(&mut response);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        assert!(!response
            .headers()
            .contains_key("access-control-allow-methods"));
        assert!(!response
            .headers()
            .contains_key("access-control-allow-headers"));
        assert!(response
            .headers()
            .contains_key("access-control-expose-headers"));
    }

    #[test]
    fn preflight_cors_response_keeps_method_and_header_permissions() {
        let mut response = Response::new(());
        add_cors_preflight_headers(&mut response);
        assert!(response
            .headers()
            .contains_key("access-control-allow-methods"));
        assert!(response
            .headers()
            .contains_key("access-control-allow-headers"));
    }
}
