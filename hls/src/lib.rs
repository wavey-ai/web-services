use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use playlists::fmp4_cache::Fmp4Cache;
use playlists::m3u8_cache::M3u8Cache;
use regex::Regex;
use std::{collections::HashMap, sync::Arc};
use tokio::time::{Duration, sleep, timeout};
use web_service::{
    HandlerResponse, HandlerResult, RequestHandler, Router, ServerError, StreamWriter,
    StreamingHandler, WebSocketHandler, WebTransportHandler,
};

pub mod streaming;

/// HLS-specific router implementation
pub struct HlsRouter {
    handlers: Vec<Box<dyn RequestHandler>>,
    streaming_handlers: Vec<Box<dyn StreamingHandler>>,
    webtransport_handler: Option<Box<dyn WebTransportHandler>>,
    websocket_handlers: Vec<Box<dyn WebSocketHandler>>,
}

impl HlsRouter {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
            streaming_handlers: Vec::new(),
            webtransport_handler: None,
            websocket_handlers: Vec::new(),
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
    pub fn with_webtransport(mut self, handler: Box<dyn WebTransportHandler>) -> Self {
        self.webtransport_handler = Some(handler);
        self
    }
    pub fn add_websocket_handler(mut self, handler: Box<dyn WebSocketHandler>) -> Self {
        self.websocket_handlers.push(handler);
        self
    }
    pub fn with_websocket_tail(mut self, fmp4_cache: Arc<Fmp4Cache>) -> Self {
        self.websocket_handlers
            .push(Box::new(streaming::TailWebSocketHandler::new(fmp4_cache)));
        self
    }
    fn parse_path(path: &str) -> Vec<&str> {
        path.split('/').filter(|s| !s.is_empty()).collect()
    }
}

#[async_trait]
impl Router for HlsRouter {
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
    fn is_streaming(&self, path: &str) -> bool {
        self.streaming_handlers.iter().any(|h| h.is_streaming(path))
    }
    async fn route_stream(
        &self,
        req: Request<()>,
        writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        let path_str = req.uri().path().to_string();
        let parts = Self::parse_path(&path_str);
        for handler in &self.streaming_handlers {
            if handler.is_streaming(&path_str) {
                return handler.handle_stream(req, parts, writer).await;
            }
        }
        Err(ServerError::Config(
            "No streaming handler found for path".into(),
        ))
    }
    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        self.webtransport_handler.as_deref()
    }
    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        self.websocket_handlers
            .iter()
            .find(|handler| handler.can_handle(path))
            .map(|handler| handler.as_ref())
    }
}

/// Main HLS handler
pub struct HlsHandler {
    fmp4_cache: Arc<Fmp4Cache>,
    m3u8_cache: Arc<M3u8Cache>,
}
impl HlsHandler {
    pub fn new(fmp4_cache: Arc<Fmp4Cache>, m3u8_cache: Arc<M3u8Cache>) -> Self {
        Self {
            fmp4_cache,
            m3u8_cache,
        }
    }
    fn detect_content_type(file: &str) -> Option<&'static str> {
        if file.ends_with(".m3u8") {
            Some("application/vnd.apple.mpegurl")
        } else if file.ends_with(".mp4") {
            Some("video/mp4")
        } else if file.ends_with(".ts") {
            Some("video/mp2t")
        } else if file.ends_with(".jpeg") {
            Some("image/jpeg")
        } else {
            None
        }
    }
    fn extract_id(s: &str) -> Option<usize> {
        Regex::new(r"(s|p)(\d+)(\.mp4|\.ts)$")
            .unwrap()
            .captures(s)
            .and_then(|c| c.get(2))
            .and_then(|m| m.as_str().parse().ok())
    }
    async fn handle_m3u8(
        &self,
        id: u64,
        qp: HashMap<&str, &str>,
    ) -> HandlerResult<HandlerResponse> {
        let mut data = None;
        if let (Some(msn), Some(part)) = (qp.get("_HLS_msn"), qp.get("_HLS_part")) {
            if let (Ok(m), Ok(p)) = (msn.parse(), part.parse()) {
                data = self.get_m3u8_with_blocking(id, m, p).await;
            }
        } else if qp.contains_key("_HLS_skip") {
            data = self.m3u8_cache.last(id).unwrap();
        } else {
            data = self.m3u8_cache.last(id).unwrap();
        }
        if let Some((b, h)) = data {
            Ok(HandlerResponse {
                status: StatusCode::OK,
                body: Some(b),
                content_type: Some("application/vnd.apple.mpegurl".to_string()),
                headers: vec![
                    ("content-encoding".into(), "gzip".into()),
                    ("vary".into(), "accept-encoding".into()),
                ],
                etag: Some(h),
            })
        } else {
            Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..Default::default()
            })
        }
    }
    async fn get_m3u8_with_blocking(
        &self,
        sid: u64,
        msn: usize,
        part: usize,
    ) -> Option<(Bytes, u64)> {
        let to = Duration::from_secs(3);
        let iv = Duration::from_millis(3);
        timeout(to, async {
            loop {
                if let Some(d) = self.m3u8_cache.get(sid, msn, part).unwrap() {
                    return Some(d.clone());
                }
                if let Some(d) = self.m3u8_cache.get(sid, msn + 1, 0).unwrap() {
                    return Some(d.clone());
                }
                sleep(iv).await;
            }
        })
        .await
        .ok()?
    }
    async fn get_segment(&self, sid: u64, idx: usize) -> Option<(Bytes, u64)> {
        self.m3u8_cache.get(sid, idx, 0).unwrap_or(None)
    }
    async fn get_part(&self, idx: u64, part: usize) -> Option<(Bytes, u64)> {
        self.fmp4_cache.get(idx as usize, part).await
    }
}
#[async_trait]
impl RequestHandler for HlsHandler {
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
            ["play"] => {
                let html = include_str!("../public/index.html");
                Ok(HandlerResponse {
                    status: StatusCode::OK,
                    body: Some(Bytes::from(html)),
                    content_type: Some("text/html".into()),
                    ..Default::default()
                })
            }
            ["hls.min.js"] => {
                let js = include_str!("../public/hls.min.js");
                Ok(HandlerResponse {
                    status: StatusCode::OK,
                    body: Some(Bytes::from(js)),
                    content_type: Some("text/javascript".into()),
                    ..Default::default()
                })
            }
            [sid, file] => {
                let sid = sid
                    .parse::<u64>()
                    .map_err(|_| ServerError::Config("Invalid stream ID".into()))?;
                if file == &"stream.m3u8" {
                    self.handle_m3u8(sid, qp).await
                } else if file == &"init.mp4" {
                    if let Ok(data) = self.m3u8_cache.get_init(sid) {
                        Ok(HandlerResponse {
                            status: StatusCode::OK,
                            body: Some(data),
                            content_type: Some("video/mp4".into()),
                            ..Default::default()
                        })
                    } else {
                        Ok(HandlerResponse {
                            status: StatusCode::NOT_FOUND,
                            ..Default::default()
                        })
                    }
                } else if file.starts_with('s') {
                    if let Some(id) = Self::extract_id(file) {
                        if let Some(d) = self.get_segment(sid, id).await {
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
                } else if file.starts_with('p') {
                    if let Some(id) = Self::extract_id(file) {
                        if let Some(idx) = self.fmp4_cache.get_stream_idx(sid).await {
                            if let Some(d) = self.get_part(idx as u64, id).await {
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
