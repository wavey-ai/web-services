use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use playlists::fmp4_cache::Fmp4Cache as Cache;
use regex::Regex;
use std::{collections::HashMap, sync::Arc};
use tokio::time::{Duration, sleep, timeout};
use tracing::{error, info};
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
        Regex::new(r"(s|p)(\d+)(\.json)$")
            .unwrap()
            .captures(s)
            .and_then(|c| c.get(2))
            .and_then(|m| m.as_str().parse().ok())
    }
    async fn get_part(&self, idx: u64, part: usize) -> Option<(Bytes, u64)> {
        self.cache.get(idx as usize, part).await
    }
    
    async fn get_part_with_blocking(&self, idx: u64, part: usize) -> Option<(Bytes, u64)> {
        let to = Duration::from_secs(3);
        let iv = Duration::from_millis(3);
        timeout(to, async {
            loop {
                if let Some(d) = self.cache.get(idx as usize, part).await {
                    return Some(d);
                }
                sleep(iv).await;
            }
        })
        .await
        .ok()?
    }
}
#[async_trait]
impl RequestHandler for ChunkHandler {
    async fn handle(
        &self,
        req: Request<()>,
        parts: Vec<&str>,
        query: Option<&str>,
    ) -> HandlerResult<HandlerResponse> {
        if req.method() != &Method::GET && req.method() != &Method::HEAD {
            return Ok(HandlerResponse {
                status: StatusCode::METHOD_NOT_ALLOWED,
                ..Default::default()
            });
        }
        let qp: HashMap<&str, &str> = query
            .map(|q| q.split('&').filter_map(|p| p.split_once('=')).collect())
            .unwrap_or_default();
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
                        if let Some(idx) = self.cache.get_stream_idx(sid).await {
                            let data = 
                                self.get_part_with_blocking(idx as u64, id).await; 
                            if let Some(d) = data {
                                Ok(HandlerResponse {
                                    status: StatusCode::OK,
                                    body: Some(d.0),
                                    content_type: Self::detect_content_type(file).map(String::from),
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
