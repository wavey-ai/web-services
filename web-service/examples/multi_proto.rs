use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, StatusCode};
use std::env;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, RequestHandler, Router, Server, ServerBuilder,
    ServerError, WebSocketHandler,
};

/// Basic HTTP handler that replies on `/`.
struct HelloHandler;

#[async_trait::async_trait]
impl RequestHandler for HelloHandler {
    async fn handle(
        &self,
        _req: Request<()>,
        _path_parts: Vec<&str>,
        _query: Option<&str>,
    ) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(Bytes::from_static(b"hello from h1/h2")),
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        })
    }

    fn can_handle(&self, path: &str) -> bool {
        path == "/"
    }
}

/// Simple WebSocket echo handler on `/ws`.
struct EchoWebSocketHandler;

#[async_trait::async_trait]
impl WebSocketHandler for EchoWebSocketHandler {
    async fn handle_websocket(
        &self,
        req: Request<()>,
        mut stream: tokio_tungstenite::WebSocketStream<
            hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>,
        >,
    ) -> HandlerResult<()> {
        let path = req.uri().path();
        if path != "/ws" {
            return Err(ServerError::Config("unsupported websocket path".into()));
        }

        while let Some(frame) = stream.next().await {
            match frame {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    stream
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            format!("echo: {}", text).into(),
                        ))
                        .await
                        .map_err(|e| ServerError::Handler(Box::new(e)))?;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Binary(bin)) => {
                    stream
                        .send(tokio_tungstenite::tungstenite::Message::Binary(bin))
                        .await
                        .map_err(|e| ServerError::Handler(Box::new(e)))?;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Ping(p)) => {
                    let _ = stream
                        .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                        .await;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    let _ = stream.close(frame).await;
                    break;
                }
                Ok(_) => {}
                Err(e) => return Err(ServerError::Handler(Box::new(e))),
            }
        }

        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        path == "/ws"
    }
}

/// Minimal router wiring HTTP + WebSocket.
struct ExampleRouter {
    http: HelloHandler,
    ws: EchoWebSocketHandler,
}

impl ExampleRouter {
    fn new() -> Self {
        Self {
            http: HelloHandler,
            ws: EchoWebSocketHandler,
        }
    }
}

#[async_trait::async_trait]
impl Router for ExampleRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(str::to_string);
        if self.http.can_handle(&path) {
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            return self.http.handle(req, parts, query.as_deref()).await;
        }

        Ok(HandlerResponse {
            status: StatusCode::NOT_FOUND,
            ..Default::default()
        })
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn web_service::StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("no streaming endpoints".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn web_service::WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        if self.ws.can_handle(path) {
            Some(&self.ws)
        } else {
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load local .env for TLS materials when running the example
    let _ = dotenvy::from_filename(".env");

    // TLS materials are expected in base64-encoded PEM form.
    let cert_pem_base64 =
        env::var("TLS_CERT_BASE64").expect("set TLS_CERT_BASE64 to base64-encoded cert PEM");
    let key_pem_base64 =
        env::var("TLS_KEY_BASE64").expect("set TLS_KEY_BASE64 to base64-encoded key PEM");

    let router = Box::new(ExampleRouter::new());

    let server = H2H3Server::builder()
        .with_tls(cert_pem_base64, key_pem_base64)
        .with_port(443)
        .enable_h2(true)
        .enable_websocket(true)
        .with_router(router)
        .build()?;

    let handle = server.start().await?;
    let _ = handle.ready_rx.await;
    println!("Server ready on h1/h2 and wss at /ws");

    tokio::signal::ctrl_c().await?;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    Ok(())
}
