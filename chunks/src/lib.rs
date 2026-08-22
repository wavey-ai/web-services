use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use playlists::chunk_cache::{ChunkCache as Cache, StreamHandle};
use std::sync::Arc;
use tokio::time::{Duration, Instant, timeout_at};
use web_service::{
    HandlerResponse, HandlerResult, RequestHandler, Router, ServerError, StreamWriter,
    StreamingHandler, WebSocketHandler, WebTransportHandler,
};

pub struct ChunkRouter {
    handlers: Vec<Box<dyn RequestHandler>>,
    streaming_handlers: Vec<Box<dyn StreamingHandler>>,
}

impl ChunkRouter {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
            streaming_handlers: Vec::new(),
        }
    }
    pub fn add_handler(mut self, handler: Box<dyn RequestHandler>) -> Self {
        self.handlers.push(handler);
        self
    }
    pub fn add_streaming_handler(mut self, handler: Box<dyn StreamingHandler>) -> Self {
        self.streaming_handlers.push(handler);
        self
    }
    fn parse_path(path: &str) -> Vec<&str> {
        path.split('/').filter(|s| !s.is_empty()).collect()
    }
}

impl Default for ChunkRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Router for ChunkRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if req.method() == Method::OPTIONS {
            return Ok(HandlerResponse {
                status: StatusCode::OK,
                ..Default::default()
            });
        }
        let path_str = req.uri().path().to_string();
        let path_parts = Self::parse_path(&path_str);
        let query_str = req.uri().query().map(str::to_string);
        for handler in &self.handlers {
            if handler.can_handle(&path_str) {
                return handler.handle(req, path_parts, query_str.as_deref()).await;
            }
        }
        Ok(HandlerResponse {
            status: StatusCode::NOT_FOUND,
            ..Default::default()
        })
    }

    fn is_streaming(&self, _: &str) -> bool {
        false
    }
    async fn route_stream(&self, _: Request<()>, _: Box<dyn StreamWriter>) -> HandlerResult<()> {
        Err(ServerError::Config(
            "No streaming handler found for path".into(),
        ))
    }
    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }
    fn websocket_handler(&self, _: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

pub struct ChunkHandler {
    cache: Arc<Cache>,
}
impl ChunkHandler {
    pub fn new(cache: Arc<Cache>) -> Self {
        Self { cache }
    }
    fn detect_content_type(file: &str) -> Option<&'static str> {
        if file.ends_with(".json") {
            Some("application/json")
        } else {
            None
        }
    }
    fn extract_id(s: &str) -> Option<usize> {
        let bytes = s.as_bytes();
        if !matches!(bytes.first(), Some(b's' | b'p')) {
            return None;
        }
        let numeric = s[1..].strip_suffix(".json")?;
        if numeric.is_empty() || !numeric.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        numeric.parse().ok()
    }
    async fn get_part_with_blocking(
        &self,
        handle: StreamHandle,
        part: usize,
    ) -> Option<(Bytes, u64)> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(data) = self.cache.get_for_handle(handle, part).await {
                return Some(data);
            }
            if self.cache.resolve_stream(handle.stream_id()) != Some(handle) {
                return None;
            }

            let notifier = self.cache.exact_part_waiter(handle.stream_id(), part)?;
            let update = notifier.notified();
            tokio::pin!(update);
            update.as_mut().enable();
            if let Some(data) = self.cache.get_for_handle(handle, part).await {
                return Some(data);
            }
            if self.cache.resolve_stream(handle.stream_id()) != Some(handle) {
                return None;
            }
            timeout_at(deadline, update).await.ok()?;
        }
    }
}
#[async_trait]
impl RequestHandler for ChunkHandler {
    async fn handle(
        &self,
        req: Request<()>,
        parts: Vec<&str>,
        _query: Option<&str>,
    ) -> HandlerResult<HandlerResponse> {
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return Ok(HandlerResponse {
                status: StatusCode::METHOD_NOT_ALLOWED,
                ..Default::default()
            });
        }
        match parts.as_slice() {
            ["up"] => Ok(HandlerResponse {
                status: StatusCode::OK,
                body: Some(Bytes::from("OK")),
                content_type: Some("text/plain".into()),
                ..Default::default()
            }),
            [sid, file] => {
                let sid = sid
                    .parse::<u64>()
                    .map_err(|_| ServerError::Config("Invalid stream ID".into()))?;
                if file.starts_with('p') {
                    if let Some(id) = Self::extract_id(file) {
                        if let Some(handle) = self.cache.resolve_stream(sid) {
                            let data = self.get_part_with_blocking(handle, id).await;
                            if let Some(d) = data {
                                Ok(HandlerResponse {
                                    status: StatusCode::OK,
                                    body: Some(d.0),
                                    content_type: Self::detect_content_type(file).map(Into::into),
                                    etag: Some(d.1),
                                    ..Default::default()
                                })
                            } else {
                                Ok(HandlerResponse {
                                    status: StatusCode::NOT_FOUND,
                                    ..Default::default()
                                })
                            }
                        } else {
                            Ok(HandlerResponse {
                                status: StatusCode::NOT_FOUND,
                                ..Default::default()
                            })
                        }
                    } else {
                        Ok(HandlerResponse {
                            status: StatusCode::NOT_FOUND,
                            ..Default::default()
                        })
                    }
                } else {
                    Ok(HandlerResponse {
                        status: StatusCode::NOT_FOUND,
                        ..Default::default()
                    })
                }
            }
            _ => Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..Default::default()
            }),
        }
    }
    fn can_handle(&self, _path: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkHandler;

    #[test]
    fn chunk_id_parser_accepts_only_canonical_names() {
        assert_eq!(ChunkHandler::extract_id("p12.json"), Some(12));
        assert_eq!(ChunkHandler::extract_id("s3.json"), Some(3));
        assert_eq!(ChunkHandler::extract_id("p0.json"), Some(0));
        assert_eq!(ChunkHandler::extract_id("x12.json"), None);
        assert_eq!(ChunkHandler::extract_id("p.json"), None);
        assert_eq!(ChunkHandler::extract_id("p12.mp4"), None);
        assert_eq!(ChunkHandler::extract_id("p12x.json"), None);
        assert_eq!(
            ChunkHandler::extract_id("p184467440737095516160.json"),
            None
        );
    }
}
