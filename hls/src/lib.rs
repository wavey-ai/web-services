use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use flate2::read::GzDecoder;
use http::{
    Method, Request, StatusCode,
    header::{ACCEPT_ENCODING, ACCEPT_RANGES, RANGE, VARY},
};
use playlists::chunk_cache::ChunkCache;
use playlists::m3u8_cache::M3u8Cache;
use regex::Regex;
use std::io::Read;
use std::{collections::HashMap, sync::Arc};
use tokio::time::{Duration, sleep, timeout};
use tracing::debug;
use web_service::{
    HandlerResponse, HandlerResult, RequestHandler, Router, ServerError, StreamWriter,
    StreamingHandler, WebSocketHandler, WebTransportHandler,
};

pub mod streaming;

const BLOCKING_RELOAD_TIMEOUT: Duration = Duration::from_secs(18);
const BLOCKING_RELOAD_POLL_INTERVAL: Duration = Duration::from_millis(3);

enum BlockingPlaylistReload {
    Found(Bytes, u64),
    Status(StatusCode),
}

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
    pub fn with_websocket_tail(mut self, chunk_cache: Arc<ChunkCache>) -> Self {
        self.websocket_handlers
            .push(Box::new(streaming::TailWebSocketHandler::new(chunk_cache)));
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
    chunk_cache: Arc<ChunkCache>,
    m3u8_cache: Arc<M3u8Cache>,
}
impl HlsHandler {
    pub fn new(chunk_cache: Arc<ChunkCache>, m3u8_cache: Arc<M3u8Cache>) -> Self {
        Self {
            chunk_cache,
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

    async fn resolve_chunk_stream_idx(&self, stream_id: u64) -> Option<usize> {
        if let Some(idx) = self.chunk_cache.get_stream_idx(stream_id).await {
            return Some(idx);
        }

        let requested_idx = usize::try_from(stream_id).ok()?;
        if requested_idx < self.chunk_cache.options.num_playlists {
            Some(requested_idx)
        } else {
            None
        }
    }

    async fn handle_m3u8(
        &self,
        req: &Request<()>,
        id: u64,
        qp: HashMap<&str, &str>,
    ) -> HandlerResult<HandlerResponse> {
        debug!(
            stream_id = id,
            hls_msn = qp.get("_HLS_msn").copied().unwrap_or(""),
            hls_part = qp.get("_HLS_part").copied().unwrap_or(""),
            hls_skip = qp.get("_HLS_skip").copied().unwrap_or(""),
            "LL-HLS playlist request received"
        );
        if qp.contains_key("_HLS_part") && !qp.contains_key("_HLS_msn") {
            debug!(
                stream_id = id,
                "rejecting LL-HLS playlist request with _HLS_part but no _HLS_msn"
            );
            return Ok(HandlerResponse {
                status: StatusCode::BAD_REQUEST,
                ..Default::default()
            });
        }
        let skip = Self::skip_requested(&qp);

        let data = if let Some(msn) = qp.get("_HLS_msn") {
            let Ok(msn) = msn.parse::<usize>() else {
                debug!(
                    stream_id = id,
                    hls_msn = *msn,
                    "rejecting LL-HLS playlist request with invalid _HLS_msn"
                );
                return Ok(HandlerResponse {
                    status: StatusCode::BAD_REQUEST,
                    ..Default::default()
                });
            };
            let part = match qp.get("_HLS_part") {
                Some(part) => match part.parse::<usize>() {
                    Ok(part) => Some(part),
                    Err(_) => {
                        debug!(
                            stream_id = id,
                            hls_part = *part,
                            "rejecting LL-HLS playlist request with invalid _HLS_part"
                        );
                        return Ok(HandlerResponse {
                            status: StatusCode::BAD_REQUEST,
                            ..Default::default()
                        });
                    }
                },
                None => None,
            };
            match self.get_m3u8_with_blocking(id, msn, part, skip).await {
                BlockingPlaylistReload::Found(bytes, hash) => {
                    debug!(
                        stream_id = id,
                        msn,
                        part = part.unwrap_or(0),
                        skip,
                        bytes = bytes.len(),
                        hash,
                        "LL-HLS blocking playlist request resolved"
                    );
                    Some((bytes, hash))
                }
                BlockingPlaylistReload::Status(status) => {
                    debug!(
                        stream_id = id,
                        msn,
                        part = part.unwrap_or(0),
                        skip,
                        status = status.as_u16(),
                        "LL-HLS blocking playlist request returned status"
                    );
                    return Ok(HandlerResponse {
                        status,
                        ..Default::default()
                    });
                }
            }
        } else {
            self.latest_m3u8(id, skip)
        };

        if let Some((bytes, hash)) = data {
            debug!(
                stream_id = id,
                skip,
                bytes = bytes.len(),
                hash,
                "LL-HLS playlist response ready"
            );
            let gzip = Self::request_accepts_gzip(req) && !req.headers().contains_key(RANGE);
            let body = if gzip {
                bytes
            } else {
                Self::decompress_gzip_playlist(bytes)?
            };
            let mut headers = vec![
                (ACCEPT_RANGES.as_str().to_string(), "none".to_string()),
                (VARY.as_str().to_string(), "accept-encoding".to_string()),
            ];
            if gzip {
                headers.push(("content-encoding".into(), "gzip".into()));
            }
            Ok(HandlerResponse {
                status: StatusCode::OK,
                body: Some(body),
                content_type: Some("application/vnd.apple.mpegurl".to_string()),
                headers,
                etag: Some(hash),
            })
        } else {
            debug!(stream_id = id, skip, "LL-HLS playlist response not found");
            Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..Default::default()
            })
        }
    }

    fn request_accepts_gzip(req: &Request<()>) -> bool {
        let Some(value) = req
            .headers()
            .get(ACCEPT_ENCODING)
            .and_then(|value| value.to_str().ok())
        else {
            return false;
        };

        let mut wildcard_accepts = false;
        for encoding in value.split(',') {
            let mut parts = encoding.split(';').map(str::trim);
            let Some(coding) = parts.next().filter(|coding| !coding.is_empty()) else {
                continue;
            };
            let q = parts
                .find_map(|part| {
                    let (name, value) = part.split_once('=')?;
                    name.trim()
                        .eq_ignore_ascii_case("q")
                        .then(|| value.trim().parse::<f32>().ok())
                        .flatten()
                })
                .unwrap_or(1.0);

            if coding.eq_ignore_ascii_case("gzip") {
                return q > 0.0;
            }
            if coding == "*" {
                wildcard_accepts = q > 0.0;
            }
        }

        wildcard_accepts
    }

    fn decompress_gzip_playlist(bytes: Bytes) -> HandlerResult<Bytes> {
        let mut decoder = GzDecoder::new(bytes.as_ref());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).map_err(|error| {
            ServerError::Config(format!("failed to decompress playlist: {error}"))
        })?;
        Ok(Bytes::from(decoded))
    }

    fn skip_requested(qp: &HashMap<&str, &str>) -> bool {
        matches!(qp.get("_HLS_skip"), Some(&"YES" | &"v2"))
    }

    fn latest_m3u8(&self, sid: u64, skip: bool) -> Option<(Bytes, u64)> {
        if skip {
            match self.m3u8_cache.last_delta(sid) {
                Ok(Some(delta)) => {
                    debug!(
                        stream_id = sid,
                        bytes = delta.0.len(),
                        hash = delta.1,
                        "serving latest LL-HLS delta playlist"
                    );
                    return Some(delta);
                }
                Ok(None) => {
                    debug!(
                        stream_id = sid,
                        "latest LL-HLS delta unavailable; falling back to full playlist"
                    );
                }
                Err(error) => {
                    debug!(
                        stream_id = sid,
                        %error,
                        "failed to produce LL-HLS delta playlist"
                    );
                }
            }
        }
        self.m3u8_cache.last(sid).unwrap_or(None)
    }

    fn blocking_request_is_behind_latest(&self, sid: u64, msn: usize, part: Option<usize>) -> bool {
        let Some((last_msn, last_part)) = self.m3u8_cache.last_position(sid) else {
            return false;
        };
        msn < last_msn || (msn == last_msn && part.unwrap_or(0) <= last_part)
    }

    fn get_m3u8_snapshot(
        &self,
        sid: u64,
        msn: usize,
        part: usize,
        skip: bool,
    ) -> Option<(Bytes, u64)> {
        if skip {
            match self.m3u8_cache.get_delta(sid, msn, part) {
                Ok(Some(delta)) => return Some(delta),
                Ok(None) => {}
                Err(error) => {
                    debug!(
                        stream_id = sid,
                        msn,
                        part,
                        %error,
                        "failed to produce LL-HLS delta playlist snapshot"
                    );
                }
            }
        }

        match self.m3u8_cache.get(sid, msn, part) {
            Ok(Some(d)) => Some(d),
            Ok(None) => None,
            Err(error) => {
                debug!(
                    stream_id = sid,
                    msn,
                    part,
                    %error,
                    "failed to read LL-HLS playlist snapshot"
                );
                None
            }
        }
    }

    async fn get_m3u8_with_blocking(
        &self,
        sid: u64,
        msn: usize,
        part: Option<usize>,
        skip: bool,
    ) -> BlockingPlaylistReload {
        if self.blocking_request_is_behind_latest(sid, msn, part) {
            debug!(
                stream_id = sid,
                msn,
                part = part.unwrap_or(0),
                skip,
                "LL-HLS blocking playlist request is behind latest cache"
            );
            return self
                .latest_m3u8(sid, skip)
                .map(|(bytes, hash)| BlockingPlaylistReload::Found(bytes, hash))
                .unwrap_or(BlockingPlaylistReload::Status(StatusCode::NOT_FOUND));
        }

        let requested_part = part.unwrap_or(0);
        let result = timeout(BLOCKING_RELOAD_TIMEOUT, async {
            loop {
                if let Some((bytes, hash)) = self.get_m3u8_snapshot(sid, msn, requested_part, skip)
                {
                    return BlockingPlaylistReload::Found(bytes, hash);
                }
                if part.is_some() {
                    if let Some(next_msn) = msn.checked_add(1) {
                        if let Some((bytes, hash)) =
                            self.get_m3u8_snapshot(sid, next_msn, 0, skip)
                        {
                            return BlockingPlaylistReload::Found(bytes, hash);
                        }
                    }
                }
                if self.blocking_request_is_behind_latest(sid, msn, part) {
                    debug!(
                        stream_id = sid,
                        msn,
                        part = part.unwrap_or(0),
                        "LL-HLS blocking playlist request fell behind cache; serving latest playlist"
                    );
                    return self
                        .latest_m3u8(sid, skip)
                        .map(|(bytes, hash)| BlockingPlaylistReload::Found(bytes, hash))
                        .unwrap_or(BlockingPlaylistReload::Status(StatusCode::NOT_FOUND));
                }
                sleep(BLOCKING_RELOAD_POLL_INTERVAL).await;
            }
        })
        .await;

        result.unwrap_or_else(|_| {
            debug!(
                stream_id = sid,
                msn,
                part = part.unwrap_or(0),
                "LL-HLS blocking playlist request timed out"
            );
            BlockingPlaylistReload::Status(StatusCode::SERVICE_UNAVAILABLE)
        })
    }

    async fn get_segment(&self, sid: u64, segment_id: usize) -> Option<Bytes> {
        let (start, end) = self.m3u8_cache.get_idxs(sid, segment_id).ok()??;
        let stream_idx = self.resolve_chunk_stream_idx(sid).await?;
        let mut bytes = BytesMut::new();
        for part_id in start..end {
            let (part, _hash) = self.get_part(stream_idx as u64, part_id).await?;
            bytes.extend_from_slice(&part);
        }
        (!bytes.is_empty()).then(|| bytes.freeze())
    }
    async fn get_part(&self, idx: u64, part: usize) -> Option<(Bytes, u64)> {
        self.chunk_cache.get(idx as usize, part).await
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
                    self.handle_m3u8(&req, sid, qp).await
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
                        if let Some(bytes) = self.get_segment(sid, id).await {
                            Ok(HandlerResponse {
                                status: StatusCode::OK,
                                body: Some(bytes),
                                content_type: Self::detect_content_type(file).map(String::from),
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
                        if let Some(idx) = self.resolve_chunk_stream_idx(sid).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use http::header::CONTENT_ENCODING;
    use playlists::{
        Options, chunk_cache::ChunkCache, m3u8_cache::M3u8Cache, m3u8_manifest::M3u8Manifest,
    };
    use std::io::Read;

    fn response_header<'a>(response: &'a HandlerResponse, name: &str) -> Option<&'a str> {
        response.headers.iter().find_map(|(candidate, value)| {
            candidate
                .eq_ignore_ascii_case(name)
                .then_some(value.as_str())
        })
    }

    fn decompress_body(response: &HandlerResponse) -> String {
        let body = response.body.as_ref().expect("response body");
        if response_header(response, CONTENT_ENCODING.as_str())
            .is_some_and(|value| value.eq_ignore_ascii_case("gzip"))
        {
            let mut decoder = GzDecoder::new(body.as_ref());
            let mut decoded = String::new();
            decoder.read_to_string(&mut decoded).unwrap();
            decoded
        } else {
            String::from_utf8(body.to_vec()).expect("playlist utf8")
        }
    }

    fn generated_ll_playlist(options: Options) -> (Bytes, usize, usize, usize) {
        let mut manifest = M3u8Manifest::new(options);
        let mut latest = None;
        for _ in 0..12 {
            latest = Some(manifest.add_part(1000, true));
        }
        let (playlist, segment_id, seq, idx, _) = latest.unwrap();
        (playlist, segment_id, seq, idx)
    }

    fn handler_with_cached_playlist(options: Options) -> (HlsHandler, String) {
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let playlist = M3u8Manifest::new(options).m3u8();
        let playlist_text = String::from_utf8(playlist.to_vec()).expect("playlist utf8");
        m3u8_cache.add(1, 1, 0, 0, playlist).unwrap();
        (HlsHandler::new(chunk_cache, m3u8_cache), playlist_text)
    }

    #[tokio::test]
    async fn playlist_range_request_serves_identity_full_playlist() {
        let (handler, expected_playlist) = handler_with_cached_playlist(Options::default());

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8")
                    .header(RANGE, "bytes=0-1445")
                    .header(ACCEPT_ENCODING, "identity")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response_header(&response, ACCEPT_RANGES.as_str()),
            Some("none")
        );
        assert!(response_header(&response, CONTENT_ENCODING.as_str()).is_none());
        assert_eq!(decompress_body(&response), expected_playlist);
    }

    #[tokio::test]
    async fn playlist_gzip_request_serves_gzip_full_playlist() {
        let (handler, expected_playlist) = handler_with_cached_playlist(Options::default());

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8")
                    .header(ACCEPT_ENCODING, "gzip, identity;q=0.5")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response_header(&response, ACCEPT_RANGES.as_str()),
            Some("none")
        );
        assert_eq!(
            response_header(&response, CONTENT_ENCODING.as_str()),
            Some("gzip")
        );
        assert_eq!(decompress_body(&response), expected_playlist);
    }

    #[tokio::test]
    async fn stale_blocking_playlist_reload_serves_latest_playlist() {
        let options = Options::default();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        m3u8_cache
            .add(1, 137, 0, 0, Bytes::from_static(b"latest-playlist"))
            .unwrap();
        m3u8_cache
            .add(1, 137, 1, 1, Bytes::from_static(b"newer-playlist"))
            .unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_msn=137&_HLS_part=0&_HLS_skip=YES")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_msn=137&_HLS_part=0&_HLS_skip=YES"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some("application/vnd.apple.mpegurl")
        );
    }

    #[tokio::test]
    async fn skip_directive_returns_delta_playlist_when_advertised() {
        let options = Options {
            max_segments: 10,
            segment_min_ms: 1000,
            ..Options::default()
        };
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let (playlist, segment_id, seq, idx) = generated_ll_playlist(options);
        m3u8_cache.add(1, segment_id, seq, idx, playlist).unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_skip=YES")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_skip=YES"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some("application/vnd.apple.mpegurl")
        );
        let playlist = decompress_body(&response);
        assert!(playlist.contains("CAN-SKIP-UNTIL=6.00000"));
        assert!(playlist.contains("#EXT-X-SKIP:SKIPPED-SEGMENTS="));
        assert_eq!(playlist.matches("#EXT-X-SKIP:").count(), 1);
    }

    #[tokio::test]
    async fn skip_directive_is_ignored_when_not_advertised() {
        let options = Options::default();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let playlist = M3u8Manifest::new(options).m3u8();
        m3u8_cache.add(1, 1, 0, 0, playlist).unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_skip=YES")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_skip=YES"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let playlist = decompress_body(&response);
        assert!(!playlist.contains("#EXT-X-SKIP:"));
        assert!(!playlist.contains("CAN-SKIP-UNTIL"));
    }

    #[tokio::test]
    async fn stale_blocking_reload_with_skip_returns_delta_playlist() {
        let options = Options {
            max_segments: 10,
            segment_min_ms: 1000,
            ..Options::default()
        };
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let (playlist, segment_id, seq, idx) = generated_ll_playlist(options);
        m3u8_cache.add(1, segment_id, seq, idx, playlist).unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_msn=1&_HLS_part=0&_HLS_skip=YES")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_msn=1&_HLS_part=0&_HLS_skip=YES"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let playlist = decompress_body(&response);
        assert!(playlist.contains("#EXT-X-SKIP:SKIPPED-SEGMENTS="));
    }

    #[tokio::test]
    async fn stale_old_blocking_playlist_reload_serves_latest_playlist() {
        let options = Options {
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        };
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        m3u8_cache
            .add(1, 12, 0, 0, Bytes::from_static(b"old-playlist"))
            .unwrap();
        m3u8_cache
            .add(1, 14, 0, 0, Bytes::from_static(b"latest-playlist"))
            .unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_msn=12&_HLS_part=0&_HLS_skip=YES")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_msn=12&_HLS_part=0&_HLS_skip=YES"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn part_directive_without_msn_returns_bad_request() {
        let options = Options::default();
        let handler = HlsHandler::new(
            Arc::new(ChunkCache::new(options)),
            Arc::new(M3u8Cache::new(options)),
        );

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_part=0")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_part=0"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn future_blocking_playlist_reload_waits_for_snapshot() {
        let options = Options::default();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let writer_cache = Arc::clone(&m3u8_cache);
        m3u8_cache
            .add(1, 137, 0, 0, Bytes::from_static(b"latest-playlist"))
            .unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response_fut = handler.handle(
            Request::builder()
                .uri("/1/stream.m3u8?_HLS_msn=140&_HLS_part=0")
                .body(())
                .unwrap(),
            vec!["1", "stream.m3u8"],
            Some("_HLS_msn=140&_HLS_part=0"),
        );
        let publish_fut = async move {
            sleep(Duration::from_millis(10)).await;
            writer_cache
                .add(1, 140, 0, 0, Bytes::from_static(b"future-playlist"))
                .unwrap();
        };

        let (response, _) = tokio::join!(response_fut, publish_fut);
        let response = response.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_ne!(response.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn blocking_playlist_reload_waits_for_high_part_index_snapshot() {
        let options = Options::default();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let writer_cache = Arc::clone(&m3u8_cache);
        m3u8_cache
            .add(1, 9, 49, 49, Bytes::from_static(b"part-49-playlist"))
            .unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response_fut = handler.handle(
            Request::builder()
                .uri("/1/stream.m3u8?_HLS_msn=9&_HLS_part=50")
                .body(())
                .unwrap(),
            vec!["1", "stream.m3u8"],
            Some("_HLS_msn=9&_HLS_part=50"),
        );
        let publish_fut = async move {
            sleep(Duration::from_millis(10)).await;
            writer_cache
                .add(1, 9, 50, 50, Bytes::from_static(b"part-50-playlist"))
                .unwrap();
        };

        let (response, _) = timeout(Duration::from_secs(1), async {
            tokio::join!(response_fut, publish_fut)
        })
        .await
        .expect("blocking reload resolved before server timeout");
        let response = response.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_ne!(response.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn blocking_playlist_reload_for_missing_part_resolves_on_next_segment() {
        let options = Options::default();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let writer_cache = Arc::clone(&m3u8_cache);
        m3u8_cache
            .add(1, 9, 49, 49, Bytes::from_static(b"part-49-playlist"))
            .unwrap();
        let handler = HlsHandler::new(chunk_cache, m3u8_cache);

        let response_fut = handler.handle(
            Request::builder()
                .uri("/1/stream.m3u8?_HLS_msn=9&_HLS_part=50")
                .body(())
                .unwrap(),
            vec!["1", "stream.m3u8"],
            Some("_HLS_msn=9&_HLS_part=50"),
        );
        let publish_fut = async move {
            sleep(Duration::from_millis(10)).await;
            writer_cache
                .add(1, 10, 50, 0, Bytes::from_static(b"segment-10-playlist"))
                .unwrap();
        };

        let (response, _) = timeout(Duration::from_secs(1), async {
            tokio::join!(response_fut, publish_fut)
        })
        .await
        .expect("blocking reload resolved on next segment");
        let response = response.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_ne!(response.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn non_numeric_blocking_reload_directives_return_bad_request() {
        let options = Options::default();
        let handler = HlsHandler::new(
            Arc::new(ChunkCache::new(options)),
            Arc::new(M3u8Cache::new(options)),
        );

        let response = handler
            .handle(
                Request::builder()
                    .uri("/1/stream.m3u8?_HLS_msn=bad&_HLS_part=0")
                    .body(())
                    .unwrap(),
                vec!["1", "stream.m3u8"],
                Some("_HLS_msn=bad&_HLS_part=0"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn closed_segment_uri_serves_concatenated_media_parts() {
        let options = Options::default();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));
        let stream_idx = chunk_cache.add_stream_id(1).await;
        chunk_cache
            .add(stream_idx, 1, Bytes::from_static(b"part-one"))
            .await
            .unwrap();
        chunk_cache
            .add(stream_idx, 2, Bytes::from_static(b"part-two"))
            .await
            .unwrap();
        chunk_cache
            .add(stream_idx, 3, Bytes::from_static(b"next-segment"))
            .await
            .unwrap();
        m3u8_cache
            .add(1, 1, 1, 0, Bytes::from_static(b"playlist-a"))
            .unwrap();
        m3u8_cache
            .add(1, 1, 2, 1, Bytes::from_static(b"playlist-b"))
            .unwrap();
        m3u8_cache
            .add(1, 2, 3, 0, Bytes::from_static(b"playlist-c"))
            .unwrap();

        let handler = HlsHandler::new(chunk_cache, m3u8_cache);
        let response = handler
            .handle(
                Request::builder().uri("/1/s1.mp4").body(()).unwrap(),
                vec!["1", "s1.mp4"],
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(response.body.as_deref(), Some(&b"part-onepart-two"[..]));
    }
}
