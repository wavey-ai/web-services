use crate::{
    config::ServerConfig,
    error::{H2Error, ServerError, ServerResult},
    traits::{BodyStream, HandlerResponse, Router, StreamWriter},
};
use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use http_body_util::{combinators::BoxBody, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::upgrade;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tls_helpers::tls_acceptor_from_base64;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{
    tungstenite::{handshake::derive_accept_key, protocol::Role},
    WebSocketStream,
};
use tracing::{error, info};
use async_trait::async_trait;

pub struct Http2Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
}

impl Http2Server {
    pub fn new(config: ServerConfig, router: Arc<dyn Router>) -> Self {
        Self { config, router }
    }

    pub async fn start(&self, mut shutdown_rx: watch::Receiver<()>) -> ServerResult<()> {
        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.config.port);
        let tls_acceptor = tls_acceptor_from_base64(
            &self.config.cert_pem_base64,
            &self.config.privkey_pem_base64,
            true,
            true,
        )
        .map_err(|e| ServerError::Tls(e.to_string()))?;

        let listener = bind_tcp_listener(addr)?;
        info!("HTTP/1.1+HTTP/2 server listening at {}", addr);
        let enable_websocket = self.config.enable_websocket;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("HTTP/1.1+HTTP/2 server shutting down");
                    break;
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _peer)) => {
                            let tls_acceptor = tls_acceptor.clone();
                            let router = Arc::clone(&self.router);
                            tokio::spawn(async move {
                                let tls_stream = match tls_acceptor.accept(stream).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        error!("TLS handshake failed: {}", e);
                                        return;
                                    }
                                };

                                let alpn = tls_stream.get_ref().1.alpn_protocol();
                                let service = service_fn(move |req: http::Request<Incoming>| {
                                    let router = Arc::clone(&router);
                                    async move {
                                        match handle_h2_request(
                                            req,
                                            router,
                                            enable_websocket,
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

                                if matches!(alpn, Some(proto) if proto == b"h2") {
                                    let builder = http2::Builder::new(TokioExecutor::new());
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
                        }
                    }
                }
            }
        }

        Ok(())
    }
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

/// Type alias for a boxed body that can be either buffered or streaming
type ResponseBody = BoxBody<Bytes, Infallible>;

async fn handle_h2_request(
    req: http::Request<Incoming>,
    router: Arc<dyn Router>,
    enable_websocket: bool,
) -> Result<Response<ResponseBody>, H2Error> {
    if enable_websocket && is_websocket_upgrade(&req) {
        if let Some(key) = req.headers().get("sec-websocket-key") {
            let has_handler = router.websocket_handler(req.uri().path()).is_some();
            if !has_handler {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::new()).map_err(|_| unreachable!()).boxed())
                    .map_err(|e| H2Error::Router(ServerError::Http(e)))?);
            }

            let accept_key = derive_accept_key(key.as_bytes());

            let method = req.method().clone();
            let uri = req.uri().clone();
            let version = req.version();
            let headers = req.headers().clone();
            let upgrade_fut = upgrade::on(req);
            let router = Arc::clone(&router);

            tokio::spawn(async move {
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
            });

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
                .body(Full::new(Bytes::new()).map_err(|_| unreachable!()).boxed())
                .map_err(|e| H2Error::Router(ServerError::Http(e)))?;

            return Ok(response);
        }
    }

    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();

    // Check for bidirectional streaming first (body in, stream out)
    if router.is_body_streaming(&path) {
        use futures_util::stream::unfold;
        let body_stream: BodyStream = Box::pin(unfold(body, |mut b: Incoming| async move {
            match b.frame().await {
                Some(Ok(frame)) => {
                    let data = frame
                        .into_data()
                        .map(Bytes::from)
                        .unwrap_or_else(|_| Bytes::new());
                    Some((Ok(data), b))
                }
                Some(Err(e)) => Some((Err(ServerError::Handler(Box::new(e))), b)),
                None => None,
            }
        }));

        // Create channel for streaming response
        let (tx, rx) = mpsc::channel::<Bytes>(32);
        let stream_writer = Box::new(ChannelStreamWriter::new(tx));

        let req = http::Request::from_parts(parts, ());
        let router_clone = Arc::clone(&router);

        // Spawn handler task that writes to channel
        tokio::spawn(async move {
            if let Err(e) = router_clone.route_body_stream(req, body_stream, stream_writer).await {
                error!("Body stream handler error: {}", e);
            }
        });

        // Return streaming response
        return build_streaming_response(rx);
    }

    if router.has_body_handler(&path) {
        use futures_util::stream::unfold;
        let stream: BodyStream = Box::pin(unfold(body, |mut b: Incoming| async move {
            match b.frame().await {
                Some(Ok(frame)) => {
                    let data = frame
                        .into_data()
                        .map(Bytes::from)
                        .unwrap_or_else(|_| Bytes::new());
                    Some((Ok(data), b))
                }
                Some(Err(e)) => Some((Err(ServerError::Handler(Box::new(e))), b)),
                None => None,
            }
        }));
        let req = http::Request::from_parts(parts, ());
        let handler_response = router
            .route_body(req, stream)
            .await
            .map_err(H2Error::Router)?;
        return build_response(handler_response);
    }

    let mut body = body;
    while let Some(frame) = body.frame().await {
        if let Err(err) = frame {
            return Err(H2Error::Router(ServerError::Handler(Box::new(err))));
        }
    }

    let req = http::Request::from_parts(parts, ());
    let handler_response = router.route(req).await.map_err(H2Error::Router)?;
    build_response(handler_response)
}

/// StreamWriter implementation that writes to an mpsc channel
struct ChannelStreamWriter {
    tx: mpsc::Sender<Bytes>,
    response_sent: bool,
}

impl ChannelStreamWriter {
    fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self { tx, response_sent: false }
    }
}

#[async_trait]
impl StreamWriter for ChannelStreamWriter {
    async fn send_response(&mut self, _response: Response<()>) -> Result<(), ServerError> {
        // For HTTP/2, headers are sent automatically with first data frame
        self.response_sent = true;
        Ok(())
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        self.tx.send(data).await.map_err(|e| {
            ServerError::Handler(Box::new(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                e.to_string(),
            )))
        })
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        // Dropping the sender will close the channel
        Ok(())
    }
}

fn build_streaming_response(rx: mpsc::Receiver<Bytes>) -> Result<Response<ResponseBody>, H2Error> {
    use futures_util::StreamExt as _;
    use tokio_stream::wrappers::ReceiverStream;

    // Convert receiver to a stream of Result<Frame<Bytes>, Infallible>
    let stream = ReceiverStream::new(rx).map(|bytes| Ok::<_, Infallible>(Frame::data(bytes)));
    let body: ResponseBody = BodyExt::boxed(StreamBody::new(stream));

    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static("application/octet-stream"),
    );
    add_cors_headers(&mut response);
    Ok(response)
}

fn build_response(handler_response: HandlerResponse) -> Result<Response<ResponseBody>, H2Error> {
    let body_bytes = handler_response.body.unwrap_or_else(Bytes::new);
    let body: ResponseBody = Full::from(body_bytes).map_err(|_| unreachable!()).boxed();
    let mut response = Response::new(body);
    *response.status_mut() = handler_response.status;

    if let Some(ct) = handler_response.content_type {
        response.headers_mut().insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_str(&ct)?,
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
            .insert(k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?);
    }

    add_cors_headers(&mut response);

    Ok(response)
}

fn add_cors_headers<T>(res: &mut Response<T>) {
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
