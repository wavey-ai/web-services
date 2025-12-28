use super::api;
use super::context;
use super::proxy;
use super::state::ProxyState;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{Method, Request, StatusCode};
use std::sync::Arc;
use tracing::{debug, debug_span, Instrument};
use crate::{
    BodyStream, HandlerResponse, HandlerResult, Router, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

pub struct ProxyRouter {
    state: Arc<ProxyState>,
    ws_handler: ProxyWebSocketHandler,
}

impl ProxyRouter {
    pub fn new(state: Arc<ProxyState>) -> Self {
        Self {
            ws_handler: ProxyWebSocketHandler {
                state: Arc::clone(&state),
            },
            state,
        }
    }

    async fn handle_request(
        &self,
        mut req: Request<()>,
        body: Option<BodyStream>,
    ) -> HandlerResult<HandlerResponse> {
        let ctx = context::ensure_context(&mut req);
        let request_id = ctx.request_id.clone();
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        let span = debug_span!(
            "proxy_request",
            request_id = %request_id,
            method = %method,
            path = %path,
            query = %query
        );

        let response = async move {
            if method == Method::OPTIONS {
                return Ok(HandlerResponse {
                    status: StatusCode::NO_CONTENT,
                    body: None,
                    content_type: None,
                    headers: vec![],
                    etag: None,
                });
            }

            if path.starts_with("/api") {
                let bytes = match body {
                    Some(body) => collect_body(body).await?,
                    None => Bytes::new(),
                };
                return api::handle_api(&self.state, req, bytes).await;
            }

            proxy::proxy_http(&self.state, req, body).await
        }
        .instrument(span)
        .await?;

        let mut response = response;
        context::attach_request_id(&mut response, &request_id);
        debug!(
            status = %response.status,
            "proxy request completed"
        );
        Ok(response)
    }
}

#[async_trait::async_trait]
impl Router for ProxyRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        self.handle_request(req, None).await
    }

    async fn route_body(
        &self,
        req: Request<()>,
        body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        self.handle_request(req, Some(body)).await
    }

    fn has_body_handler(&self, _path: &str) -> bool {
        true
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("streaming not supported".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        if self.ws_handler.can_handle(path) {
            Some(&self.ws_handler)
        } else {
            None
        }
    }
}

struct ProxyWebSocketHandler {
    state: Arc<ProxyState>,
}

#[async_trait::async_trait]
impl WebSocketHandler for ProxyWebSocketHandler {
    async fn handle_websocket(
        &self,
        mut req: Request<()>,
        stream: tokio_tungstenite::WebSocketStream<
            hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>,
        >,
    ) -> HandlerResult<()> {
        let ctx = context::ensure_context(&mut req);
        let request_id = ctx.request_id.clone();
        let method = req.method().clone();
        let path = req.uri().path().to_string();

        let span = debug_span!(
            "proxy_websocket",
            request_id = %request_id,
            method = %method,
            path = %path
        );
        proxy::proxy_websocket(&self.state, req, stream)
            .instrument(span)
            .await
    }

    fn can_handle(&self, path: &str) -> bool {
        !path.starts_with("/api")
    }
}

async fn collect_body(mut body: BodyStream) -> Result<Bytes, ServerError> {
    let mut data = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        data.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::balancer::LoadBalancingMode;
    use http::StatusCode;
    use std::sync::Once;

    fn init_crypto() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    fn has_header(response: &HandlerResponse, name: &str, value: &str) -> bool {
        response.headers.iter().any(|(key, val)| {
            key.eq_ignore_ascii_case(name) && val == value
        })
    }

    #[tokio::test]
    async fn options_returns_no_content_and_request_id() {
        init_crypto();
        let state = Arc::new(ProxyState::new(LoadBalancingMode::LeastConn).unwrap());
        let router = ProxyRouter::new(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("https://example.com/")
            .header(context::REQUEST_ID_HEADER, "req-opts")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert!(response.body.is_none());
        assert!(has_header(&response, context::REQUEST_ID_HEADER, "req-opts"));
    }

    #[tokio::test]
    async fn api_health_attaches_request_id() {
        init_crypto();
        let state = Arc::new(ProxyState::new(LoadBalancingMode::LeastConn).unwrap());
        let router = ProxyRouter::new(state);

        let req = Request::builder()
            .method(Method::GET)
            .uri("https://example.com/api/health")
            .header(context::REQUEST_ID_HEADER, "req-health")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert!(has_header(&response, context::REQUEST_ID_HEADER, "req-health"));
    }
}
