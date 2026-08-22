use crate::{
    config::ServerConfig,
    error::{H2Error, ServerError, ServerResult},
    http_range::apply_byte_range,
    request_limit::{RequestLimiter, RequestPermit},
    traits::{
        response_header_name, response_header_value, BodyStream, HandlerResponse, Router,
        StartupSender, StreamWriter,
    },
};
use bytes::Bytes;
use futures_util::stream::unfold;
use http::header::{HeaderName, HeaderValue, RANGE};
use http::{Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::upgrade;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use sha2::{Digest, Sha256};
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex, Once},
};
use tls_helpers::{certs_from_base64, privkey_from_base64, tls_acceptor_from_base64};
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, Duration};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    tungstenite::{handshake::derive_accept_key, protocol::Role},
    WebSocketStream,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info};

const H2_MAX_CONCURRENT_STREAMS: u32 = 256;
static INSTALL_CRYPTO_PROVIDER: Once = Once::new();

/// Identity of a mutually authenticated client certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedClientCertificate {
    sha256_fingerprint: [u8; 32],
}

impl VerifiedClientCertificate {
    fn from_der(der: &[u8]) -> Self {
        Self {
            sha256_fingerprint: Sha256::digest(der).into(),
        }
    }

    /// Create an identity after an external TLS terminator verifies it.
    pub fn from_sha256_fingerprint(sha256_fingerprint: [u8; 32]) -> Self {
        Self { sha256_fingerprint }
    }

    pub fn sha256_fingerprint(&self) -> [u8; 32] {
        self.sha256_fingerprint
    }
}

pub struct Http2Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
    request_limit: Arc<RequestLimiter>,
}

type H2ResponseBody = BoxBody<Bytes, Infallible>;
type ConnectionPermitSlot = Arc<StdMutex<Option<OwnedSemaphorePermit>>>;

struct H2StreamWriter {
    response_tx: Option<oneshot::Sender<Result<Response<()>, ServerError>>>,
    data_tx: Option<mpsc::Sender<Bytes>>,
}

struct H2StreamBodyState {
    data_rx: mpsc::Receiver<Bytes>,
    handler_cancellation: CancellationToken,
}

impl Drop for H2StreamBodyState {
    fn drop(&mut self) {
        self.handler_cancellation.cancel();
    }
}

impl H2StreamWriter {
    fn new(
        response_tx: oneshot::Sender<Result<Response<()>, ServerError>>,
        data_tx: mpsc::Sender<Bytes>,
    ) -> Self {
        Self {
            response_tx: Some(response_tx),
            data_tx: Some(data_tx),
        }
    }
}

#[async_trait::async_trait]
impl StreamWriter for H2StreamWriter {
    async fn send_response(&mut self, response: Response<()>) -> Result<(), ServerError> {
        let tx = self
            .response_tx
            .take()
            .ok_or_else(|| ServerError::Config("stream response already sent".into()))?;
        tx.send(Ok(response))
            .map_err(|_| ServerError::Config("failed to send stream response head".into()))
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        let tx = self
            .data_tx
            .as_ref()
            .ok_or_else(|| ServerError::Config("stream already finished".into()))?;
        tx.send(data)
            .await
            .map_err(|_| ServerError::Config("failed to send stream body chunk".into()))
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        self.data_tx.take();
        Ok(())
    }
}

impl Http2Server {
    pub fn new(config: ServerConfig, router: Arc<dyn Router>) -> Self {
        let request_limit = Arc::new(RequestLimiter::new(config.max_in_flight_requests));
        Self {
            config,
            router,
            request_limit,
        }
    }

    pub(crate) fn new_with_request_limit(
        config: ServerConfig,
        router: Arc<dyn Router>,
        request_limit: Arc<RequestLimiter>,
    ) -> Self {
        Self {
            config,
            router,
            request_limit,
        }
    }

    pub async fn start(&self, shutdown_rx: watch::Receiver<()>) -> ServerResult<()> {
        self.run(shutdown_rx, None).await
    }

    pub(crate) async fn start_with_ready(
        &self,
        shutdown_rx: watch::Receiver<()>,
        startup_tx: StartupSender,
    ) -> ServerResult<()> {
        self.run(shutdown_rx, Some(startup_tx)).await
    }

    async fn run(
        &self,
        mut shutdown_rx: watch::Receiver<()>,
        startup_tx: Option<StartupSender>,
    ) -> ServerResult<()> {
        let addr = SocketAddr::new(self.config.bind_addr, self.config.port);
        let startup: ServerResult<_> = (|| {
            let tls_acceptor = build_tls_acceptor(&self.config)?;
            let listener = bind_tcp_listener(addr)?;
            Ok((tls_acceptor, listener))
        })();
        let (tls_acceptor, listener) = match startup {
            Ok(startup) => startup,
            Err(error) => {
                if let Some(startup_tx) = startup_tx {
                    let _ = startup_tx.send(Err(error.to_string()));
                }
                return Err(error);
            }
        };
        if let Some(startup_tx) = startup_tx {
            let _ = startup_tx.send(Ok(()));
        }
        info!("HTTP/1.1+HTTP/2 server listening at {}", addr);
        let enable_websocket = self.config.enable_websocket;
        let require_client_certificate = self.config.client_ca_pem_base64.is_some();
        let connection_limit = Arc::new(Semaphore::new(self.config.max_connections.max(1)));
        let handshake_timeout = Duration::from_millis(self.config.handshake_timeout_ms.max(1));
        let mut connection_tasks = JoinSet::new();
        let detached_tasks = TaskTracker::new();
        let detached_shutdown = CancellationToken::new();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("HTTP/1.1+HTTP/2 server shutting down");
                    break;
                }
                Some(result) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    if let Err(error) = result {
                        error!(%error, "HTTP connection task failed");
                    }
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, peer)) => {
                            let Ok(connection_permit) = Arc::clone(&connection_limit).try_acquire_owned() else {
                                debug!(%peer, "HTTP connection limit reached");
                                continue;
                            };
                            let tls_acceptor = tls_acceptor.clone();
                            let router = Arc::clone(&self.router);
                            let request_limit = Arc::clone(&self.request_limit);
                            let detached_tasks = detached_tasks.clone();
                            let detached_shutdown = detached_shutdown.clone();
                            connection_tasks.spawn(async move {
                                let connection_permit = Arc::new(StdMutex::new(Some(connection_permit)));
                                let tls_stream = match timeout(handshake_timeout, tls_acceptor.accept(stream)).await {
                                    Ok(Ok(stream)) => stream,
                                    Err(_) => {
                                        debug!(%peer, "TLS handshake timed out");
                                        return;
                                    }
                                    Ok(Err(e)) => {
                                        debug!(%peer, %e, "TLS handshake failed");
                                        return;
                                    }
                                };

                                let alpn = tls_stream.get_ref().1.alpn_protocol();
                                let is_h2 = matches!(alpn, Some(proto) if proto == b"h2");
                                if require_client_certificate && !is_h2 {
                                    debug!(%peer, "mutual TLS listener rejected a non-HTTP/2 client");
                                    return;
                                }
                                let client_certificate = require_client_certificate
                                    .then(|| {
                                        tls_stream
                                            .get_ref()
                                            .1
                                            .peer_certificates()
                                            .and_then(|certificates| certificates.first())
                                            .map(|certificate| {
                                                VerifiedClientCertificate::from_der(
                                                    certificate.as_ref(),
                                                )
                                            })
                                    })
                                    .flatten();
                                if require_client_certificate && client_certificate.is_none() {
                                    debug!(%peer, "mutual TLS connection omitted its client identity");
                                    return;
                                }

                                let service = service_fn(move |mut req: http::Request<Incoming>| {
                                    let router = Arc::clone(&router);
                                    let request_limit = Arc::clone(&request_limit);
                                    let connection_permit = Arc::clone(&connection_permit);
                                    let detached_tasks = detached_tasks.clone();
                                    let detached_shutdown = detached_shutdown.clone();
                                    if let Some(client_certificate) = client_certificate {
                                        req.extensions_mut().insert(client_certificate);
                                    }
                                    async move {
                                        let Ok(request_permit) = request_limit.try_acquire_owned() else {
                                            return Ok(overloaded_h2_response());
                                        };
                                        match handle_h2_request(
                                            req,
                                            router,
                                            enable_websocket,
                                            request_permit,
                                            connection_permit,
                                            detached_tasks,
                                            detached_shutdown,
                                        )
                                        .await
                                        {
                                            Ok(resp) => Ok(resp),
                                            Err(e) => {
                                                error!("Request handling error: {}", e);
                                                Err("request failed")
                                            }
                                        }
                                    }
                                });

                                if is_h2 {
                                    let mut builder = http2::Builder::new(TokioExecutor::new());
                                    builder.max_concurrent_streams(H2_MAX_CONCURRENT_STREAMS);
                                    if let Err(e) = builder
                                        .serve_connection(TokioIo::new(tls_stream), service)
                                        .await
                                    {
                                        error!("Serving HTTP/2 connection failed: {}", e);
                                    }
                                } else {
                                    let builder = http1::Builder::new();
                                    let conn =
                                        builder.serve_connection(TokioIo::new(tls_stream), service);
                                    let result = if enable_websocket {
                                        conn.with_upgrades().await
                                    } else {
                                        conn.await
                                    };
                                    if let Err(e) = result {
                                        error!("Serving HTTP/1.1 connection failed: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Accept failed: {}", e);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }

        detached_shutdown.cancel();
        connection_tasks.shutdown().await;
        detached_tasks.close();
        detached_tasks.wait().await;

        Ok(())
    }
}

fn build_tls_acceptor(config: &ServerConfig) -> ServerResult<TlsAcceptor> {
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    let Some(client_ca) = config.client_ca_pem_base64.as_deref() else {
        return tls_acceptor_from_base64(
            &config.cert_pem_base64,
            &config.privkey_pem_base64,
            true,
            true,
        )
        .map_err(|error| ServerError::Tls(error.to_string()));
    };

    let certificates = certs_from_base64(&config.cert_pem_base64)
        .map_err(|error| ServerError::Tls(error.to_string()))?;
    let private_key = privkey_from_base64(&config.privkey_pem_base64)
        .map_err(|error| ServerError::Tls(error.to_string()))?;
    let mut client_roots = RootCertStore::empty();
    for certificate in
        certs_from_base64(client_ca).map_err(|error| ServerError::Tls(error.to_string()))?
    {
        client_roots
            .add(certificate)
            .map_err(|error| ServerError::Tls(error.to_string()))?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .map_err(|error| ServerError::Tls(error.to_string()))?;
    let mut tls_config = RustlsServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates, private_key)
        .map_err(|error| ServerError::Tls(error.to_string()))?;
    tls_config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(tls_config)))
}

fn bind_tcp_listener(addr: SocketAddr) -> ServerResult<TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }
    .map_err(ServerError::Io)?;
    let _ = socket.set_reuseaddr(true);
    socket.bind(addr).map_err(ServerError::Io)?;
    socket.listen(1024).map_err(ServerError::Io)
}

async fn handle_h2_request(
    req: http::Request<Incoming>,
    router: Arc<dyn Router>,
    enable_websocket: bool,
    request_permit: RequestPermit,
    connection_permit: ConnectionPermitSlot,
    detached_tasks: TaskTracker,
    detached_shutdown: CancellationToken,
) -> Result<Response<H2ResponseBody>, H2Error> {
    if enable_websocket && is_websocket_upgrade(&req) {
        if let Some(key) = req.headers().get("sec-websocket-key") {
            let has_handler = router.websocket_handler(req.uri().path()).is_some();
            if !has_handler {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::new()).boxed())
                    .map_err(|e| H2Error::Router(ServerError::Http(e)));
            }

            let accept_key = derive_accept_key(key.as_bytes());

            let method = req.method().clone();
            let uri = req.uri().clone();
            let version = req.version();
            let headers = req.headers().clone();
            let upgrade_fut = upgrade::on(req);
            let router = Arc::clone(&router);
            let websocket_connection_permit = connection_permit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();

            let websocket_task = async move {
                match upgrade_fut.await {
                    Ok(upgraded) => {
                        tracing::info!("Accepted WebSocket upgrade for {}", uri);
                        let mut builder = http::Request::builder()
                            .method(method)
                            .uri(uri)
                            .version(version);
                        for (name, value) in headers.iter() {
                            builder = builder.header(name, value);
                        }

                        let ws_request = match builder.body(()) {
                            Ok(req) => req,
                            Err(e) => {
                                error!("Failed to rebuild WebSocket request: {}", e);
                                return;
                            }
                        };

                        let mut ws_stream = WebSocketStream::from_raw_socket(
                            TokioIo::new(upgraded),
                            Role::Server,
                            None,
                        )
                        .await;

                        if let Some(handler) = router.websocket_handler(ws_request.uri().path()) {
                            if let Err(e) = handler.handle_websocket(ws_request, ws_stream).await {
                                error!("WebSocket handler error: {}", e);
                            }
                        } else {
                            error!(
                                "WebSocket handler went missing for path {}",
                                ws_request.uri().path()
                            );
                            // best effort close
                            let _ = ws_stream.close(None).await;
                        }
                    }
                    Err(e) => error!("WebSocket upgrade failed: {}", e),
                }
            };
            drop(detached_tasks.spawn(async move {
                let _request_permit = request_permit;
                let _connection_permit = websocket_connection_permit;
                tokio::select! {
                    _ = detached_shutdown.cancelled() => {}
                    _ = websocket_task => {}
                }
            }));

            let response = Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(
                    HeaderName::from_static("upgrade"),
                    HeaderValue::from_static("websocket"),
                )
                .header(
                    HeaderName::from_static("connection"),
                    HeaderValue::from_static("Upgrade"),
                )
                .header(
                    HeaderName::from_static("sec-websocket-accept"),
                    HeaderValue::from_str(&accept_key)?,
                )
                .body(Full::new(Bytes::new()).boxed())
                .map_err(|e| H2Error::Router(ServerError::Http(e)))?;

            return Ok(response);
        }
    }

    let (parts, body) = req.into_parts();
    let range_header = parts.headers.get(RANGE).cloned();
    if router.has_body_stream_handler(parts.uri.path()) {
        let stream = incoming_body_stream(body);
        let req = http::Request::from_parts(parts, ());
        return handle_h2_body_stream(
            req,
            stream,
            router,
            request_permit,
            detached_tasks,
            detached_shutdown,
        )
        .await;
    }

    if router.is_streaming(parts.uri.path()) {
        let req = http::Request::from_parts(parts, ());
        return handle_h2_stream(
            req,
            router,
            request_permit,
            detached_tasks,
            detached_shutdown,
        )
        .await;
    }

    if router.has_body_handler(parts.uri.path()) {
        let stream = incoming_body_stream(body);
        let req = http::Request::from_parts(parts, ());
        let handler_response = router
            .route_body(req, stream)
            .await
            .map_err(H2Error::Router)?;
        return build_buffered_response(apply_byte_range(range_header.as_ref(), handler_response));
    }

    let mut body = body;
    while let Some(frame) = body.frame().await {
        if let Err(err) = frame {
            return Err(H2Error::Router(ServerError::Handler(Box::new(err))));
        }
    }

    let req = http::Request::from_parts(parts, ());
    let handler_response = router.route(req).await.map_err(H2Error::Router)?;
    build_buffered_response(apply_byte_range(range_header.as_ref(), handler_response))
}

fn incoming_body_stream(body: Incoming) -> BodyStream {
    Box::pin(unfold(body, |mut b: Incoming| async move {
        match b.frame().await {
            Some(Ok(frame)) => {
                let data = frame.into_data().unwrap_or_default();
                Some((Ok(data), b))
            }
            Some(Err(e)) => Some((Err(ServerError::Handler(Box::new(e))), b)),
            None => None,
        }
    }))
}

async fn handle_h2_stream(
    req: http::Request<()>,
    router: Arc<dyn Router>,
    request_permit: RequestPermit,
    detached_tasks: TaskTracker,
    detached_shutdown: CancellationToken,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (response_tx, response_rx) = oneshot::channel();
    let (data_tx, data_rx) = mpsc::channel(32);
    let handler_cancellation = CancellationToken::new();
    let task_cancellation = handler_cancellation.clone();
    drop(detached_tasks.spawn(async move {
        let _request_permit = request_permit;
        let writer = H2StreamWriter::new(response_tx, data_tx);
        tokio::select! {
            _ = detached_shutdown.cancelled() => {}
            _ = task_cancellation.cancelled() => {}
            result = router.route_stream(req, Box::new(writer)) => {
                if let Err(err) = result {
                    error!("streaming handler error: {}", err);
                }
            }
        }
    }));
    await_h2_stream_response(
        response_rx,
        H2StreamBodyState {
            data_rx,
            handler_cancellation,
        },
    )
    .await
}

async fn handle_h2_body_stream(
    req: http::Request<()>,
    body: BodyStream,
    router: Arc<dyn Router>,
    request_permit: RequestPermit,
    detached_tasks: TaskTracker,
    detached_shutdown: CancellationToken,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (response_tx, response_rx) = oneshot::channel();
    let (data_tx, data_rx) = mpsc::channel(32);
    let handler_cancellation = CancellationToken::new();
    let task_cancellation = handler_cancellation.clone();
    drop(detached_tasks.spawn(async move {
        let _request_permit = request_permit;
        let writer = H2StreamWriter::new(response_tx, data_tx);
        tokio::select! {
            _ = detached_shutdown.cancelled() => {}
            _ = task_cancellation.cancelled() => {}
            result = router.route_body_stream(req, body, Box::new(writer)) => {
                if let Err(err) = result {
                    error!("streaming body handler error: {}", err);
                }
            }
        }
    }));
    await_h2_stream_response(
        response_rx,
        H2StreamBodyState {
            data_rx,
            handler_cancellation,
        },
    )
    .await
}

async fn await_h2_stream_response(
    response_rx: oneshot::Receiver<Result<Response<()>, ServerError>>,
    body_state: H2StreamBodyState,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let response = match response_rx.await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => return Err(H2Error::Router(err)),
        Err(_) => {
            return Err(H2Error::Router(ServerError::Config(
                "stream handler finished before sending response".into(),
            )));
        }
    };
    build_streaming_response(response, body_state)
}

fn build_buffered_response(
    handler_response: HandlerResponse,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let mut response = Response::new(Full::from(handler_response.body.unwrap_or_default()).boxed());
    *response.status_mut() = handler_response.status;

    if let Some(ct) = handler_response.content_type {
        response.headers_mut().insert(
            HeaderName::from_static("content-type"),
            response_header_value(ct)?,
        );
    }
    if let Some(etag) = handler_response.etag {
        response.headers_mut().insert(
            HeaderName::from_static("etag"),
            HeaderValue::from_str(&etag.to_string())?,
        );
    }
    for (k, v) in handler_response.headers {
        response
            .headers_mut()
            .insert(response_header_name(k)?, response_header_value(v)?);
    }

    add_cors_headers(&mut response);

    Ok(response)
}

fn overloaded_h2_response() -> Response<H2ResponseBody> {
    let mut response = Response::new(Full::from(Bytes::from_static(b"service overloaded")).boxed());
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response.headers_mut().insert(
        HeaderName::from_static("retry-after"),
        HeaderValue::from_static("1"),
    );
    add_cors_headers(&mut response);
    response
}

fn build_streaming_response(
    response_head: Response<()>,
    body_state: H2StreamBodyState,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (parts, ()) = response_head.into_parts();
    let body_stream = unfold(body_state, |mut state| async move {
        state
            .data_rx
            .recv()
            .await
            .map(|chunk| (Ok::<Frame<Bytes>, Infallible>(Frame::data(chunk)), state))
    });
    let mut response = Response::from_parts(parts, StreamBody::new(body_stream).boxed());
    add_cors_headers(&mut response);
    Ok(response)
}

fn add_cors_headers<B>(res: &mut Response<B>) {
    res.headers_mut().insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_static("*"),
    );
    res.headers_mut().insert(
        HeaderName::from_static("access-control-allow-methods"),
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    res.headers_mut().insert(
        HeaderName::from_static("access-control-allow-headers"),
        HeaderValue::from_static("*"),
    );
    res.headers_mut()
        .entry(HeaderName::from_static("access-control-expose-headers"))
        .or_insert_with(|| {
            HeaderValue::from_static(
                "x-sequence, stream-id, etag, content-length, accept-ranges, content-range",
            )
        });
}

fn is_websocket_upgrade(req: &http::Request<Incoming>) -> bool {
    req.method() == http::Method::GET
        && req.version() == http::Version::HTTP_11
        && header_has_token(req.headers(), "connection", "upgrade")
        && header_has_token(req.headers(), "upgrade", "websocket")
        && req.headers().get("sec-websocket-key").is_some()
        && req
            .headers()
            .get("sec-websocket-version")
            .map(|v| v == "13")
            .unwrap_or(false)
}

fn header_has_token(headers: &http::HeaderMap, name: &str, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(token))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cors_headers_keep_handler_exposure_fields() {
        let mut response = Response::builder()
            .header(
                "access-control-expose-headers",
                "Link, Retry-After, X-Needletail-Alternate-Edges",
            )
            .body(())
            .unwrap();
        add_cors_headers(&mut response);
        assert_eq!(
            response.headers()["access-control-expose-headers"],
            "Link, Retry-After, X-Needletail-Alternate-Edges"
        );
    }
}
