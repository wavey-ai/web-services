use crate::{
    config::ServerConfig,
    error::{H2Error, ServerError, ServerResult},
    http_range::apply_byte_range,
    request_limit::{Admission, RequestLimiter, RequestPermit},
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
use hyper::body::{Body as _, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::upgrade;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use sha2::{Digest, Sha256};
use std::{
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex, Once,
    },
    task::{Context, Poll},
};
use tls_helpers::{certs_from_base64, privkey_from_base64, tls_acceptor_from_base64};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, Duration, Instant};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    tungstenite::{handshake::derive_accept_key, protocol::Role},
    WebSocketStream,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, info_span, trace, Instrument, Span};

const H2_MAX_CONCURRENT_STREAMS: u32 = 256;
const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
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

type H2ResponseBody = BoxBody<Bytes, ServerError>;
type ConnectionPermitSlot = Arc<StdMutex<Option<OwnedSemaphorePermit>>>;

/// Wrap a fully buffered body in the fallible body type shared with streaming responses.
fn buffered_body(bytes: Bytes) -> H2ResponseBody {
    Full::new(bytes)
        .map_err(|never: Infallible| match never {})
        .boxed()
}

struct H2StreamWriter {
    response_tx: Option<oneshot::Sender<Result<Response<()>, ServerError>>>,
    data_tx: Option<mpsc::Sender<Bytes>>,
    completed: Arc<AtomicBool>,
}

struct H2StreamBodyState {
    data_rx: mpsc::Receiver<Bytes>,
    handler_cancellation: CancellationToken,
    /// Set only by `StreamWriter::finish`. A closed body channel with this
    /// unset means the handler stopped early, so the stream must be reset
    /// rather than ended cleanly.
    completed: Arc<AtomicBool>,
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
        completed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            response_tx: Some(response_tx),
            data_tx: Some(data_tx),
            completed,
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
        self.completed.store(true, Ordering::Release);
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
        let plain = plain_http(&self.config);
        let startup: ServerResult<_> = (|| {
            let tls_acceptor = if plain {
                None
            } else {
                Some(build_tls_acceptor(&self.config)?)
            };
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
        let max_connections = self.config.max_connections.max(1);
        info!(
            max_connections,
            max_in_flight_requests = self.config.max_in_flight_requests,
            request_queue_timeout_ms = self.config.request_queue_timeout_ms,
            "{} server listening at {}",
            if plain {
                "HTTP/1.1 (plain)"
            } else {
                "HTTP/1.1+HTTP/2"
            },
            addr
        );
        let connection_limit = Arc::new(Semaphore::new(max_connections));
        let mut connection_tasks = JoinSet::new();
        let shared = Arc::new(Listener {
            tls_acceptor,
            handshake_timeout: Duration::from_millis(self.config.handshake_timeout_ms.max(1)),
            require_client_certificate: self.config.client_ca_pem_base64.is_some(),
            enable_websocket: self.config.enable_websocket,
            http1_header_read_timeout: (self.config.http1_header_read_timeout_ms > 0)
                .then(|| Duration::from_millis(self.config.http1_header_read_timeout_ms)),
            max_unread_body_bytes: self.config.max_unread_body_bytes,
            router: Arc::clone(&self.router),
            admission: Admission::new(
                Arc::clone(&self.request_limit),
                Duration::from_millis(self.config.request_queue_timeout_ms),
                &self.config.limit_exempt_paths,
            ),
            detached_tasks: TaskTracker::new(),
            detached_shutdown: CancellationToken::new(),
            draining: CancellationToken::new(),
        });

        loop {
            // A connection slot first, then accept: at the limit new connections wait in the
            // kernel's backlog, so the client or load balancer feels the pressure.
            let connection_permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    debug!(
                        limit = max_connections,
                        "HTTP connection limit reached; accepting again when one closes"
                    );
                    tokio::select! {
                        _ = shutdown_rx.changed() => break,
                        Some(result) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                            log_connection_task(result);
                            continue;
                        }
                        permit = Arc::clone(&connection_limit).acquire_owned() => {
                            permit.expect("the connection semaphore is never closed")
                        }
                    }
                }
            };
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                Some(result) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    log_connection_task(result);
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, peer)) => {
                            trace!(
                                %peer,
                                open = max_connections - connection_limit.available_permits(),
                                "HTTP connection accepted"
                            );
                            connection_tasks.spawn(
                                Arc::clone(&shared).serve_connection(stream, peer, connection_permit),
                            );
                        }
                        Err(e) => {
                            error!("Accept failed: {}", e);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
        info!("HTTP/1.1+HTTP/2 server shutting down");
        drop(listener);

        let drain = Duration::from_millis(self.config.shutdown_drain_ms);
        if !drain.is_zero() {
            info!(
                drain_ms = self.config.shutdown_drain_ms,
                open = connection_tasks.len(),
                "draining open HTTP connections"
            );
            shared.draining.cancel();
            let drained = timeout(drain, async {
                while let Some(result) = connection_tasks.join_next().await {
                    log_connection_task(result);
                }
                shared.detached_tasks.close();
                shared.detached_tasks.wait().await;
            })
            .await;
            if drained.is_err() {
                info!(
                    open = connection_tasks.len(),
                    "drain window over; closing the connections left"
                );
            }
        }
        shared.detached_shutdown.cancel();
        connection_tasks.shutdown().await;
        shared.detached_tasks.close();
        shared.detached_tasks.wait().await;

        Ok(())
    }
}

fn plain_http(config: &ServerConfig) -> bool {
    #[cfg(feature = "plain-http")]
    {
        config.plain_http
    }
    #[cfg(not(feature = "plain-http"))]
    {
        let _ = config;
        false
    }
}

fn log_connection_task(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        error!(%error, "HTTP connection task failed");
    }
}

/// A connection's byte stream: TLS, or plain TCP on a `plain-http` listener.
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// What every connection and request on one HTTP/1.1+HTTP/2 listener shares.
struct Listener {
    /// `None` on a plain HTTP listener.
    tls_acceptor: Option<TlsAcceptor>,
    handshake_timeout: Duration,
    require_client_certificate: bool,
    enable_websocket: bool,
    http1_header_read_timeout: Option<Duration>,
    max_unread_body_bytes: u64,
    router: Arc<dyn Router>,
    admission: Admission,
    /// Streaming handlers and WebSocket sessions, which outlive the request that started them.
    detached_tasks: TaskTracker,
    /// Cancelled when the server stops: detached tasks end.
    detached_shutdown: CancellationToken,
    /// Cancelled when a shutdown drain starts: connections finish what they serve, then close.
    draining: CancellationToken,
}

impl Listener {
    async fn serve_connection(
        self: Arc<Self>,
        stream: TcpStream,
        peer: SocketAddr,
        connection_permit: OwnedSemaphorePermit,
    ) {
        let connection_permit = Arc::new(StdMutex::new(Some(connection_permit)));
        let (io, is_h2, client_certificate): (Box<dyn Io>, bool, _) = match &self.tls_acceptor {
            None => (Box::new(stream), false, None),
            Some(tls_acceptor) => {
                let tls_stream =
                    match timeout(self.handshake_timeout, tls_acceptor.accept(stream)).await {
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
                if self.require_client_certificate && !is_h2 {
                    debug!(%peer, "mutual TLS listener rejected a non-HTTP/2 client");
                    return;
                }
                let client_certificate = self
                    .require_client_certificate
                    .then(|| {
                        tls_stream
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|certificates| certificates.first())
                            .map(|certificate| {
                                VerifiedClientCertificate::from_der(certificate.as_ref())
                            })
                    })
                    .flatten();
                if self.require_client_certificate && client_certificate.is_none() {
                    debug!(%peer, "mutual TLS connection omitted its client identity");
                    return;
                }
                (Box::new(tls_stream), is_h2, client_certificate)
            }
        };

        let listener = Arc::clone(&self);
        let service = service_fn(move |mut req: http::Request<Incoming>| {
            let listener = Arc::clone(&listener);
            let connection_permit = Arc::clone(&connection_permit);
            if let Some(client_certificate) = client_certificate {
                req.extensions_mut().insert(client_certificate);
            }
            async move { Ok::<_, Infallible>(listener.serve_request(req, connection_permit).await) }
        });

        if is_h2 {
            let mut builder = http2::Builder::new(TokioExecutor::new());
            builder.max_concurrent_streams(H2_MAX_CONCURRENT_STREAMS);
            let connection = builder.serve_connection(TokioIo::new(io), service);
            tokio::pin!(connection);
            let result = tokio::select! {
                result = connection.as_mut() => result,
                _ = self.draining.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(e) = result {
                error!("Serving HTTP/2 connection failed: {}", e);
            }
        } else {
            let mut builder = http1::Builder::new();
            if let Some(header_read_timeout) = self.http1_header_read_timeout {
                builder
                    .timer(TokioTimer::new())
                    .header_read_timeout(header_read_timeout);
            }
            // Upgrades are only ever answered when WebSockets are enabled.
            let connection = builder
                .serve_connection(TokioIo::new(io), service)
                .with_upgrades();
            tokio::pin!(connection);
            let result = tokio::select! {
                result = connection.as_mut() => result,
                _ = self.draining.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(e) = result {
                if e.is_timeout() {
                    trace!(%peer, "HTTP/1.1 connection idle past the header read timeout");
                } else {
                    error!("Serving HTTP/1.1 connection failed: {}", e);
                }
            }
        }
        trace!(%peer, "HTTP connection closed");
    }

    /// One request, in its own span: admitted against the in-flight limit, routed, and
    /// answered with its request id.
    async fn serve_request(
        &self,
        mut req: http::Request<Incoming>,
        connection_permit: ConnectionPermitSlot,
    ) -> Response<H2ResponseBody> {
        let id = request_id(req.headers());
        if let Ok(value) = HeaderValue::from_str(&id) {
            req.headers_mut().insert(X_REQUEST_ID, value);
        }
        let span = info_span!(
            "request",
            request_id = %id,
            method = %req.method(),
            path = %req.uri().path(),
        );
        async move {
            let started = Instant::now();
            trace!("request received");
            let Some(request_permit) = self.admission.admit(req.uri().path()).await else {
                return with_request_id(self.server_response(StatusCode::SERVICE_UNAVAILABLE), &id);
            };
            let response = match handle_h2_request(
                req,
                Arc::clone(&self.router),
                self.enable_websocket,
                self.max_unread_body_bytes,
                request_permit,
                connection_permit,
                self.detached_tasks.clone(),
                self.detached_shutdown.clone(),
            )
            .await
            {
                Ok(response) => response,
                Err(e) => {
                    error!("Request handling error: {}", e);
                    self.server_response(StatusCode::INTERNAL_SERVER_ERROR)
                }
            };
            debug!(
                status = response.status().as_u16(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "request answered"
            );
            with_request_id(response, &id)
        }
        .instrument(span)
        .await
    }
}

impl Listener {
    /// The server's own answer for `status`, in the router's shape; a 503 also carries
    /// `Retry-After`.
    fn server_response(&self, status: StatusCode) -> Response<H2ResponseBody> {
        let mut response = build_buffered_response(self.router.server_response(status))
            .unwrap_or_else(|error| {
                error!(%error, "the router's server response is not a valid response");
                let mut response = Response::new(buffered_body(Bytes::new()));
                *response.status_mut() = status;
                add_cors_headers(&mut response);
                response
            });
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response.headers_mut().insert(
                HeaderName::from_static("retry-after"),
                HeaderValue::from_static("1"),
            );
        }
        response
    }
}

/// The caller's `x-request-id` when it is a short visible-ASCII token, else a new UUIDv7.
fn request_id(headers: &http::HeaderMap) -> String {
    headers
        .get(X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value.bytes().all(|byte| byte.is_ascii_graphic())
        })
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string())
}

fn with_request_id(mut response: Response<H2ResponseBody>, id: &str) -> Response<H2ResponseBody> {
    if let Ok(value) = HeaderValue::from_str(id) {
        response.headers_mut().insert(X_REQUEST_ID, value);
    }
    response
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

#[allow(clippy::too_many_arguments)]
async fn handle_h2_request(
    req: http::Request<Incoming>,
    router: Arc<dyn Router>,
    enable_websocket: bool,
    max_unread_body_bytes: u64,
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
                    .body(buffered_body(Bytes::new()))
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
            drop(
                detached_tasks.spawn(
                    async move {
                        let _request_permit = request_permit;
                        let _connection_permit = websocket_connection_permit;
                        tokio::select! {
                            _ = detached_shutdown.cancelled() => {}
                            _ = websocket_task => {}
                        }
                    }
                    .instrument(Span::current()),
                ),
            );

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
                .body(buffered_body(Bytes::new()))
                .map_err(|e| H2Error::Router(ServerError::Http(e)))?;

            return Ok(response);
        }
    }

    let (parts, body) = req.into_parts();
    let range_header = parts.headers.get(RANGE).cloned();
    // HTTP/1.x has no stream reset: a body left unread when the handler answers early is
    // drained in the background so the answer reaches the client.
    let drain_unread = parts.version < http::Version::HTTP_2;
    if router.has_body_stream_handler(parts.uri.path()) {
        let stream = incoming_body_stream(body, drain_unread);
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
        let stream = incoming_body_stream(body, drain_unread);
        let req = http::Request::from_parts(parts, ());
        let handler_response = router
            .route_body(req, stream)
            .await
            .map_err(H2Error::Router)?;
        return build_buffered_response(apply_byte_range(range_header.as_ref(), handler_response));
    }

    if !skip_unread_body(body, max_unread_body_bytes, drain_unread).await? {
        debug!(
            limit = max_unread_body_bytes,
            "request body to a route that reads none is too large"
        );
        return build_buffered_response(router.server_response(StatusCode::PAYLOAD_TOO_LARGE));
    }

    let req = http::Request::from_parts(parts, ());
    let handler_response = router.route(req).await.map_err(H2Error::Router)?;
    build_buffered_response(apply_byte_range(range_header.as_ref(), handler_response))
}

/// Read and drop the body of a request whose route takes none. `false` once its declared
/// length or what has arrived passes `limit`; the rest of an HTTP/1.x body is then left to
/// [`RequestBody`]'s bounded drain so the 413 still reaches the client.
async fn skip_unread_body(body: Incoming, limit: u64, drain_unread: bool) -> Result<bool, H2Error> {
    let declared = body.size_hint().lower();
    let mut body = RequestBody::new(body, drain_unread);
    if declared > limit {
        return Ok(false);
    }
    let mut read = 0u64;
    while let Some(data) = futures_util::StreamExt::next(&mut body).await {
        read = read.saturating_add(data.map_err(H2Error::Router)?.len() as u64);
        if read > limit {
            return Ok(false);
        }
    }
    Ok(true)
}

fn incoming_body_stream(body: Incoming, drain_unread: bool) -> BodyStream {
    Box::pin(RequestBody::new(body, drain_unread))
}

/// Unread HTTP/1.x request body drained after the handler has answered, at most this much
/// and for at most this long; past either the connection is closed instead.
const UNREAD_DRAIN_LIMIT: u64 = 8 * 1024 * 1024;
const UNREAD_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Bodies drained at once across the process. Past this an unread body is dropped and its
/// connection closed, as for one over [`UNREAD_DRAIN_LIMIT`].
static UNREAD_DRAINS: Semaphore = Semaphore::const_new(64);

/// A request body as a [`BodyStream`]. Dropped before its end on HTTP/1.x, it hands the rest
/// to a bounded background drain: otherwise hyper closes the connection with the body unread,
/// the kernel resets it, and a client still sending never reads the handler's early answer.
struct RequestBody {
    body: Option<Incoming>,
    drain_unread: bool,
}

impl RequestBody {
    fn new(body: Incoming, drain_unread: bool) -> Self {
        Self {
            body: Some(body),
            drain_unread,
        }
    }
}

impl futures_util::Stream for RequestBody {
    type Item = Result<Bytes, ServerError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(body) = self.body.as_mut() else {
                return Poll::Ready(None);
            };
            match std::task::ready!(Pin::new(body).poll_frame(cx)) {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => return Poll::Ready(Some(Ok(data))),
                    Err(_) => continue, // trailers
                },
                Some(Err(error)) => {
                    self.body = None;
                    return Poll::Ready(Some(Err(ServerError::Handler(Box::new(error)))));
                }
                None => {
                    self.body = None;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Drop for RequestBody {
    fn drop(&mut self) {
        let Some(body) = self.body.take() else {
            return;
        };
        if !self.drain_unread
            || body.is_end_stream()
            || body.size_hint().lower() > UNREAD_DRAIN_LIMIT
        {
            return;
        }
        let Ok(slot) = UNREAD_DRAINS.try_acquire() else {
            trace!("unread body drains at their limit; closing the connection instead");
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(
                async move {
                    let _slot = slot;
                    drain_unread_body(body).await;
                }
                .instrument(Span::current()),
            );
        }
    }
}

async fn drain_unread_body(mut body: Incoming) {
    let read = async {
        let mut left = UNREAD_DRAIN_LIMIT;
        while let Some(frame) = body.frame().await {
            let Ok(frame) = frame else { return false };
            if let Ok(data) = frame.into_data() {
                let Some(rest) = left.checked_sub(data.len() as u64) else {
                    return false;
                };
                left = rest;
            }
        }
        true
    };
    match timeout(UNREAD_DRAIN_TIMEOUT, read).await {
        Ok(true) => trace!("drained the unread request body"),
        _ => debug!("unread request body too large or slow; closing the connection"),
    }
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
    let completed = Arc::new(AtomicBool::new(false));
    let writer_completed = Arc::clone(&completed);
    drop(
        detached_tasks.spawn(
            async move {
                let _request_permit = request_permit;
                let writer = H2StreamWriter::new(response_tx, data_tx, writer_completed);
                tokio::select! {
                    _ = detached_shutdown.cancelled() => {}
                    _ = task_cancellation.cancelled() => {}
                    result = router.route_stream(req, Box::new(writer)) => {
                        if let Err(err) = result {
                            error!("streaming handler error: {}", err);
                        }
                    }
                }
            }
            .instrument(Span::current()),
        ),
    );
    await_h2_stream_response(
        response_rx,
        H2StreamBodyState {
            data_rx,
            handler_cancellation,
            completed,
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
    let completed = Arc::new(AtomicBool::new(false));
    let writer_completed = Arc::clone(&completed);
    drop(
        detached_tasks.spawn(
            async move {
                let _request_permit = request_permit;
                let writer = H2StreamWriter::new(response_tx, data_tx, writer_completed);
                tokio::select! {
                    _ = detached_shutdown.cancelled() => {}
                    _ = task_cancellation.cancelled() => {}
                    result = router.route_body_stream(req, body, Box::new(writer)) => {
                        if let Err(err) = result {
                            error!("streaming body handler error: {}", err);
                        }
                    }
                }
            }
            .instrument(Span::current()),
        ),
    );
    await_h2_stream_response(
        response_rx,
        H2StreamBodyState {
            data_rx,
            handler_cancellation,
            completed,
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
    let mut response = Response::new(buffered_body(handler_response.body.unwrap_or_default()));
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
        let name = response_header_name(k)?;
        let value = response_header_value(v)?;
        // Set-Cookie and Vary may repeat; every other header replaces.
        if name == http::header::SET_COOKIE || name == http::header::VARY {
            response.headers_mut().append(name, value);
        } else {
            response.headers_mut().insert(name, value);
        }
    }

    add_cors_headers(&mut response);

    Ok(response)
}

fn build_streaming_response(
    response_head: Response<()>,
    body_state: H2StreamBodyState,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (parts, ()) = response_head.into_parts();
    let body_stream = unfold(Some(body_state), |state| async move {
        let mut state = state?;
        match state.data_rx.recv().await {
            Some(chunk) => Some((Ok(Frame::data(chunk)), Some(state))),
            // The handler dropped its writer without calling `finish`, so the
            // body is short. Fail the body to reset the stream instead of
            // letting the peer read a truncated response as a complete one.
            None if !state.completed.load(Ordering::Acquire) => Some((
                Err(ServerError::Config(
                    "streaming response ended before the handler finished".into(),
                )),
                None,
            )),
            None => None,
        }
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
    fn a_callers_request_id_is_kept_and_a_malformed_one_replaced() {
        let mut headers = http::HeaderMap::new();
        assert!(uuid::Uuid::parse_str(&request_id(&headers)).is_ok());
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("abc-123"));
        assert_eq!(request_id(&headers), "abc-123");
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("has space"));
        let minted = request_id(&headers);
        assert_ne!(minted, "has space");
        assert!(uuid::Uuid::parse_str(&minted).is_ok());
    }

    #[test]
    fn buffered_responses_keep_every_set_cookie_and_vary() {
        let response = build_buffered_response(HandlerResponse {
            headers: vec![
                ("set-cookie".into(), "a=1".into()),
                ("set-cookie".into(), "b=2".into()),
                ("vary".into(), "origin".into()),
                ("vary".into(), "accept-encoding".into()),
                ("cache-control".into(), "no-store".into()),
                ("cache-control".into(), "private".into()),
            ],
            ..Default::default()
        })
        .unwrap();
        let all = |name| {
            response
                .headers()
                .get_all(name)
                .iter()
                .map(|value| value.to_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(all("set-cookie"), ["a=1", "b=2"]);
        assert_eq!(all("vary"), ["origin", "accept-encoding"]);
        assert_eq!(all("cache-control"), ["private"]);
    }

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
