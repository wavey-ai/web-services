use crate::{
    config::ServerConfig,
    error::{H2Error, ServerError, ServerResult},
    traits::{BodyStream, HandlerResponse, Router, StreamWriter},
};
use bytes::Bytes;
use futures_util::stream::unfold;
use http::header::{HeaderName, HeaderValue};
use http::{Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
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
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::{
    tungstenite::{handshake::derive_accept_key, protocol::Role},
    WebSocketStream,
};
use tracing::{error, info};

pub struct Http2Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
}

type H2ResponseBody = BoxBody<Bytes, Infallible>;

struct H2StreamWriter {
    response_tx: Option<oneshot::Sender<Result<Response<()>, ServerError>>>,
    data_tx: Option<mpsc::Sender<Bytes>>,
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

async fn handle_h2_request(
    req: http::Request<Incoming>,
    router: Arc<dyn Router>,
    enable_websocket: bool,
) -> Result<Response<H2ResponseBody>, H2Error> {
    if enable_websocket && is_websocket_upgrade(&req) {
        if let Some(key) = req.headers().get("sec-websocket-key") {
            let has_handler = router.websocket_handler(req.uri().path()).is_some();
            if !has_handler {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::new()).boxed())
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
                .body(Full::new(Bytes::new()).boxed())
                .map_err(|e| H2Error::Router(ServerError::Http(e)))?;

            return Ok(response);
        }
    }

    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();
    if router.has_body_stream_handler(&path) {
        let stream = incoming_body_stream(body);
        let req = http::Request::from_parts(parts, ());
        return handle_h2_body_stream(req, stream, router).await;
    }

    if router.is_streaming(&path) {
        let req = http::Request::from_parts(parts, ());
        return handle_h2_stream(req, router).await;
    }

    if router.has_body_handler(&path) {
        let stream = incoming_body_stream(body);
        let req = http::Request::from_parts(parts, ());
        let handler_response = router
            .route_body(req, stream)
            .await
            .map_err(H2Error::Router)?;
        return build_buffered_response(handler_response);
    }

    let mut body = body;
    while let Some(frame) = body.frame().await {
        if let Err(err) = frame {
            return Err(H2Error::Router(ServerError::Handler(Box::new(err))));
        }
    }

    let req = http::Request::from_parts(parts, ());
    let handler_response = router.route(req).await.map_err(H2Error::Router)?;
    build_buffered_response(handler_response)
}

fn incoming_body_stream(body: Incoming) -> BodyStream {
    Box::pin(unfold(body, |mut b: Incoming| async move {
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
    }))
}

async fn handle_h2_stream(
    req: http::Request<()>,
    router: Arc<dyn Router>,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (response_tx, response_rx) = oneshot::channel();
    let (data_tx, data_rx) = mpsc::channel(32);
    tokio::spawn(async move {
        let writer = H2StreamWriter::new(response_tx, data_tx);
        if let Err(err) = router.route_stream(req, Box::new(writer)).await {
            error!("streaming handler error: {}", err);
        }
    });
    await_h2_stream_response(response_rx, data_rx).await
}

async fn handle_h2_body_stream(
    req: http::Request<()>,
    body: BodyStream,
    router: Arc<dyn Router>,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (response_tx, response_rx) = oneshot::channel();
    let (data_tx, data_rx) = mpsc::channel(32);
    tokio::spawn(async move {
        let writer = H2StreamWriter::new(response_tx, data_tx);
        if let Err(err) = router.route_body_stream(req, body, Box::new(writer)).await {
            error!("streaming body handler error: {}", err);
        }
    });
    await_h2_stream_response(response_rx, data_rx).await
}

async fn await_h2_stream_response(
    response_rx: oneshot::Receiver<Result<Response<()>, ServerError>>,
    data_rx: mpsc::Receiver<Bytes>,
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
    build_streaming_response(response, data_rx)
}

fn build_buffered_response(
    handler_response: HandlerResponse,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let mut response =
        Response::new(Full::from(handler_response.body.unwrap_or_else(Bytes::new)).boxed());
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

fn build_streaming_response(
    response_head: Response<()>,
    data_rx: mpsc::Receiver<Bytes>,
) -> Result<Response<H2ResponseBody>, H2Error> {
    let (parts, ()) = response_head.into_parts();
    let body_stream = unfold(data_rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk| (Ok::<Frame<Bytes>, Infallible>(Frame::data(chunk)), rx))
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
