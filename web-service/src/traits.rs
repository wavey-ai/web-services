use crate::error::ServerError;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use h3_webtransport::server::WebTransportSession;
use http::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;

/// Result type for handlers
pub type HandlerResult<T> = Result<T, ServerError>;

/// Response type that handlers return
#[derive(Debug)]
pub struct HandlerResponse {
    pub status: StatusCode,
    pub body: Option<Bytes>,
    pub content_type: Option<String>,
    pub headers: Vec<(String, String)>,
    pub etag: Option<u64>,
}

/// Stream type for request bodies
pub type BodyStream = BoxStream<'static, Result<Bytes, ServerError>>;

impl Default for HandlerResponse {
    fn default() -> Self {
        Self {
            status: StatusCode::OK,
            body: None,
            content_type: None,
            headers: vec![],
            etag: None,
        }
    }
}

/// Main trait for HTTP request handlers
#[async_trait]
pub trait RequestHandler: Send + Sync + 'static {
    /// Handle an HTTP request
    async fn handle(
        &self,
        req: Request<()>,
        path_parts: Vec<&str>,
        query: Option<&str>,
    ) -> HandlerResult<HandlerResponse>;

    /// Check if this handler can handle the given path
    fn can_handle(&self, path: &str) -> bool;
}

/// Trait for streaming responses (like Server-Sent Events or tail functionality)
#[async_trait]
pub trait StreamingHandler: Send + Sync + 'static {
    /// Handle a streaming request
    async fn handle_stream(
        &self,
        req: Request<()>,
        path_parts: Vec<&str>,
        stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()>;

    /// Check if this is a streaming endpoint
    fn is_streaming(&self, path: &str) -> bool;
}

/// Trait for writing to a stream
#[async_trait]
pub trait StreamWriter: Send + Sync {
    async fn send_response(&mut self, response: Response<()>) -> Result<(), ServerError>;

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError>;

    async fn finish(&mut self) -> Result<(), ServerError>;
}

#[async_trait]
pub trait WebTransportHandler: Send + Sync + 'static {
    async fn handle_session(
        &self,
        session: WebTransportSession<h3_quinn::Connection, Bytes>,
    ) -> HandlerResult<()>;
}

#[async_trait]
pub trait WebSocketHandler: Send + Sync + 'static {
    async fn handle_websocket(
        &self,
        req: Request<()>,
        stream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    ) -> HandlerResult<()>;

    fn can_handle(&self, path: &str) -> bool;
}

#[async_trait]
pub trait RawTcpHandler: Send + Sync + 'static {
    async fn handle_stream(&self, stream: Box<dyn RawStream>, is_tls: bool) -> HandlerResult<()>;
}

pub trait RawStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> RawStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Router trait for composing multiple handlers
#[async_trait]
pub trait Router: Send + Sync + 'static {
    /// Route a request to the appropriate handler
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse>;

    /// Route a request that expects a streaming body
    async fn route_body(
        &self,
        req: Request<()>,
        body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        // Default to ignoring the body and using the normal route
        let _ = body;
        self.route(req).await
    }

    /// Check if a streaming-body handler exists for this path
    fn has_body_handler(&self, _path: &str) -> bool {
        false
    }

    /// Check if this is a streaming endpoint
    fn is_streaming(&self, path: &str) -> bool;

    /// Route a streaming request
    async fn route_stream(
        &self,
        req: Request<()>,
        stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()>;

    /// Get WebTransport handler if available
    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler>;

    /// Get WebSocket handler if available
    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler>;
}

/// Server builder trait
pub trait ServerBuilder: Sized {
    type Server: Server;

    fn new() -> Self;
    fn with_tls(self, cert: String, key: String) -> Self;
    fn with_port(self, port: u16) -> Self;
    fn with_router(self, router: Box<dyn Router>) -> Self;
    fn enable_h2(self, enable: bool) -> Self;
    fn enable_h3(self, enable: bool) -> Self;
    fn enable_websocket(self, enable: bool) -> Self;
    fn enable_raw_tcp(self, enable: bool) -> Self;
    fn with_raw_tcp_port(self, port: u16) -> Self;
    fn with_raw_tcp_tls(self, enable: bool) -> Self;
    fn with_raw_tcp_handler(self, handler: Box<dyn RawTcpHandler>) -> Self;
    fn build(self) -> Result<Self::Server, ServerError>;
}

/// Main server trait
#[async_trait]
pub trait Server: Send + Sync {
    /// Start the server
    async fn start(&self) -> HandlerResult<ServerHandle>;
}

/// Handle for controlling a running server
pub struct ServerHandle {
    pub shutdown_tx: tokio::sync::watch::Sender<()>,
    pub ready_rx: tokio::sync::oneshot::Receiver<()>,
    pub finished_rx: tokio::sync::oneshot::Receiver<()>,
}
