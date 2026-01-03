use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode};
use http_pack::stream::{
    decode_frame, encode_frame, StreamFrame, StreamHeaders,
    StreamRequestHeaders, StreamResponseHeaders,
};
use playlists::chunk_cache::ChunkCache;
use playlists::Options;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, warn};
use futures_util::{SinkExt, StreamExt};
use hyper_util::rt::TokioIo;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use web_service::{
    BodyStream, HandlerResponse, HandlerResult, Router, ServerError, StreamWriter,
    WebSocketHandler, WebTransportHandler,
};

mod watcher;
pub use watcher::ResponseWatcher;

mod srt;
pub use srt::{AllowAll, AllowAllEncrypted, SrtAuth, SrtIngest};

mod webrtc;
pub use webrtc::{AllowAllWebRtc, WebRtcAuth, WebRtcIngest};

// For RTMP support, use rtmp-ingress with the "upload-response" feature:
// rtmp-ingress = { ..., features = ["upload-response"] }
// use rtmp_ingress::upload::{RtmpUploadIngest, RtmpAuth, AllowAll};

/// End-of-stream marker - empty slot
const END_MARKER: &[u8] = b"";

/// Configuration for the upload-response service
#[derive(Debug, Clone)]
pub struct UploadResponseConfig {
    /// Maximum number of concurrent streams
    pub num_streams: usize,
    /// Buffer size per slot in KB
    pub slot_size_kb: usize,
    /// Maximum slots per stream (headers + body chunks + end)
    pub slots_per_stream: usize,
    /// Maximum time to wait for a response in milliseconds
    pub response_timeout_ms: u64,
}

impl UploadResponseConfig {
    /// Slot capacity in bytes
    pub fn slot_bytes(&self) -> usize {
        self.slot_size_kb * 1024
    }
}

impl Default for UploadResponseConfig {
    fn default() -> Self {
        Self {
            num_streams: 100,
            slot_size_kb: 64,         // 64KB per slot (sweet spot for throughput)
            slots_per_stream: 16384,  // ~1GB max per request at 64KB slots
            response_timeout_ms: 30000,
        }
    }
}

/// Response type sent through oneshot channels
pub type ResponseResult = Result<(StatusCode, Bytes), String>;

/// Main service for handling upload-response lifecycle.
///
/// Format per stream (playlist):
/// - Slot 1: HPKS Headers frame (method, path, headers)
/// - Slot 2..N-1: Raw body bytes (no framing overhead)
/// - Slot N: END marker
pub struct UploadResponseService {
    request_cache: Arc<ChunkCache>,
    response_cache: Arc<ChunkCache>,
    slot_semaphore: Arc<Semaphore>,
    next_stream_id: AtomicU64,
    response_channels: Arc<RwLock<HashMap<u64, oneshot::Sender<ResponseResult>>>>,
    /// Per-stream worker count: how many workers are currently reading/processing
    stream_worker_counts: Vec<AtomicU64>,
    /// Per-stream worker sets: which worker IDs are reading/processing each stream
    stream_workers: Arc<RwLock<Vec<std::collections::HashSet<String>>>>,
    /// Per-stream response claim: None = unclaimed, Some(worker_id) = exclusive write access
    response_claims: Arc<RwLock<Vec<Option<String>>>>,
    config: UploadResponseConfig,
}

impl UploadResponseService {
    /// Create a new upload-response service with the given configuration
    pub fn new(config: UploadResponseConfig) -> Self {
        let mut options = Options::default();
        options.num_playlists = config.num_streams;
        options.max_segments = 1;
        options.max_parts_per_segment = config.slots_per_stream;
        options.buffer_size_kb = config.slot_size_kb;

        let request_cache = Arc::new(ChunkCache::new(options));
        let response_cache = Arc::new(ChunkCache::new(options));
        let slot_semaphore = Arc::new(Semaphore::new(config.num_streams));

        // Initialize per-stream worker counts
        let stream_worker_counts: Vec<AtomicU64> = (0..config.num_streams)
            .map(|_| AtomicU64::new(0))
            .collect();

        // Initialize per-stream worker sets (for readers)
        let stream_workers: Vec<std::collections::HashSet<String>> = (0..config.num_streams)
            .map(|_| std::collections::HashSet::new())
            .collect();

        // Initialize per-stream response claims (for exclusive writer)
        let response_claims: Vec<Option<String>> = (0..config.num_streams)
            .map(|_| None)
            .collect();

        Self {
            request_cache,
            response_cache,
            slot_semaphore,
            next_stream_id: AtomicU64::new(1),
            response_channels: Arc::new(RwLock::new(HashMap::new())),
            stream_worker_counts,
            stream_workers: Arc::new(RwLock::new(stream_workers)),
            response_claims: Arc::new(RwLock::new(response_claims)),
            config,
        }
    }

    // ==================== Reader Registration (Multiple Workers) ====================

    /// Register a worker as reading/processing a stream.
    ///
    /// Multiple workers can register on the same stream for reading.
    /// Returns `true` if this worker was newly registered.
    /// Returns `false` if this worker was already registered on this stream.
    pub async fn register_reader(&self, stream_id: u64, worker_id: &str) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let mut workers = self.stream_workers.write().await;
        let inserted = workers[stream_idx].insert(worker_id.to_string());
        if inserted {
            self.stream_worker_counts[stream_idx].fetch_add(1, Ordering::SeqCst);
            debug!(stream_id, worker_id, "Reader registered");
        }
        inserted
    }

    /// Unregister a reader worker from a stream.
    ///
    /// Returns `true` if the worker was registered and is now removed.
    /// Returns `false` if the worker was not registered on this stream.
    pub async fn unregister_reader(&self, stream_id: u64, worker_id: &str) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let mut workers = self.stream_workers.write().await;
        let removed = workers[stream_idx].remove(worker_id);
        if removed {
            self.stream_worker_counts[stream_idx].fetch_sub(1, Ordering::SeqCst);
            debug!(stream_id, worker_id, "Reader unregistered");
        }
        removed
    }

    /// Get the number of readers currently processing a stream (lock-free).
    pub fn reader_count(&self, stream_id: u64) -> u64 {
        let stream_idx = self.stream_idx(stream_id);
        self.stream_worker_counts[stream_idx].load(Ordering::SeqCst)
    }

    /// Check if any readers are processing a stream (lock-free).
    pub fn has_readers(&self, stream_id: u64) -> bool {
        self.reader_count(stream_id) > 0
    }

    /// Check if a specific reader is registered on a stream.
    pub async fn is_reader_registered(&self, stream_id: u64, worker_id: &str) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let workers = self.stream_workers.read().await;
        workers[stream_idx].contains(worker_id)
    }

    /// Get all reader worker IDs currently processing a stream.
    pub async fn get_readers(&self, stream_id: u64) -> Vec<String> {
        let stream_idx = self.stream_idx(stream_id);
        let workers = self.stream_workers.read().await;
        workers[stream_idx].iter().cloned().collect()
    }

    /// Clear all readers from a stream (for cleanup/recovery).
    pub async fn clear_readers(&self, stream_id: u64) {
        let stream_idx = self.stream_idx(stream_id);
        let mut workers = self.stream_workers.write().await;
        workers[stream_idx].clear();
        self.stream_worker_counts[stream_idx].store(0, Ordering::SeqCst);
        debug!(stream_id, "All readers cleared");
    }

    // ==================== Response Writer Claim (Exclusive) ====================

    /// Try to claim exclusive write access to a stream's response.
    ///
    /// Only one worker can hold the response claim at a time.
    /// Returns `true` if the claim succeeded (response was unclaimed).
    /// Returns `false` if another worker already claimed this response.
    pub async fn try_claim_response(&self, stream_id: u64, worker_id: &str) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let mut claims = self.response_claims.write().await;
        if claims[stream_idx].is_none() {
            claims[stream_idx] = Some(worker_id.to_string());
            debug!(stream_id, worker_id, "Response claimed");
            true
        } else {
            false
        }
    }

    /// Release a previously claimed response.
    ///
    /// Returns `true` if released successfully (caller was the owner).
    /// Returns `false` if the response was not claimed by this worker.
    pub async fn release_response(&self, stream_id: u64, worker_id: &str) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let mut claims = self.response_claims.write().await;
        if claims[stream_idx].as_deref() == Some(worker_id) {
            claims[stream_idx] = None;
            debug!(stream_id, worker_id, "Response released");
            true
        } else {
            false
        }
    }

    /// Force-release a response regardless of owner (for cleanup/recovery).
    pub async fn force_release_response(&self, stream_id: u64) {
        let stream_idx = self.stream_idx(stream_id);
        let mut claims = self.response_claims.write().await;
        claims[stream_idx] = None;
        debug!(stream_id, "Response force-released");
    }

    /// Get the worker ID that currently holds the response claim, if any.
    pub async fn response_owner(&self, stream_id: u64) -> Option<String> {
        let stream_idx = self.stream_idx(stream_id);
        let claims = self.response_claims.read().await;
        claims[stream_idx].clone()
    }

    /// Check if the response is claimed by a specific worker.
    pub async fn is_response_claimed_by(&self, stream_id: u64, worker_id: &str) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let claims = self.response_claims.read().await;
        claims[stream_idx].as_deref() == Some(worker_id)
    }

    /// Check if the response is currently claimed by anyone.
    pub async fn is_response_claimed(&self, stream_id: u64) -> bool {
        let stream_idx = self.stream_idx(stream_id);
        let claims = self.response_claims.read().await;
        claims[stream_idx].is_some()
    }

    /// Get a reference to the request cache for external consumers
    pub fn request_cache(&self) -> Arc<ChunkCache> {
        Arc::clone(&self.request_cache)
    }

    /// Get a reference to the response cache for external consumers
    pub fn response_cache(&self) -> Arc<ChunkCache> {
        Arc::clone(&self.response_cache)
    }

    /// Get the configuration
    pub fn config(&self) -> &UploadResponseConfig {
        &self.config
    }

    /// Get the response channels map for the watcher
    pub fn response_channels(&self) -> Arc<RwLock<HashMap<u64, oneshot::Sender<ResponseResult>>>> {
        Arc::clone(&self.response_channels)
    }

    /// Acquire a stream slot, blocking if at capacity
    pub async fn acquire_stream(&self) -> Result<OwnedSemaphorePermit, String> {
        self.slot_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "streams closed".to_string())
    }

    /// Get the next sequential stream ID
    pub fn next_id(&self) -> u64 {
        self.next_stream_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Peek at the next stream ID that will be assigned (without incrementing)
    pub fn peek_next_id(&self) -> u64 {
        self.next_stream_id.load(Ordering::SeqCst)
    }

    /// Get stream index from stream ID
    fn stream_idx(&self, stream_id: u64) -> usize {
        (stream_id % self.config.num_streams as u64) as usize
    }

    /// Write HPKS headers frame to slot 1 of request stream
    pub async fn write_request_headers(&self, stream_id: u64, headers: StreamHeaders) -> Result<(), String> {
        let stream_idx = self.stream_idx(stream_id);
        let encoded = encode_frame(&StreamFrame::Headers(headers));
        self.request_cache
            .add(stream_idx, 1, Bytes::from(encoded))
            .await
            .map_err(|e| e.to_string())
    }

    /// Append raw body bytes to request stream (slots 2+)
    pub async fn append_request_body(&self, stream_id: u64, data: Bytes) -> Result<(), String> {
        let stream_idx = self.stream_idx(stream_id);
        self.request_cache
            .append(stream_idx, data)
            .await
            .map_err(|e| e.to_string())
    }

    /// Write end marker to request stream
    pub async fn end_request(&self, stream_id: u64) -> Result<(), String> {
        let stream_idx = self.stream_idx(stream_id);
        self.request_cache
            .append(stream_idx, Bytes::from_static(END_MARKER))
            .await
            .map_err(|e| e.to_string())
    }

    /// Write HPKS headers frame to slot 1 of response stream
    pub async fn write_response_headers(&self, stream_id: u64, headers: StreamHeaders) -> Result<(), String> {
        let stream_idx = self.stream_idx(stream_id);
        let encoded = encode_frame(&StreamFrame::Headers(headers));
        self.response_cache
            .add(stream_idx, 1, Bytes::from(encoded))
            .await
            .map_err(|e| e.to_string())
    }

    /// Append raw body bytes to response stream (slots 2+)
    pub async fn append_response_body(&self, stream_id: u64, data: Bytes) -> Result<(), String> {
        let stream_idx = self.stream_idx(stream_id);
        self.response_cache
            .append(stream_idx, data)
            .await
            .map_err(|e| e.to_string())
    }

    /// Write end marker to response stream
    pub async fn end_response(&self, stream_id: u64) -> Result<(), String> {
        let stream_idx = self.stream_idx(stream_id);
        self.response_cache
            .append(stream_idx, Bytes::from_static(END_MARKER))
            .await
            .map_err(|e| e.to_string())
    }

    /// Get last slot index for a request stream
    pub fn request_last(&self, stream_id: u64) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id);
        self.request_cache.last(stream_idx)
    }

    /// Get raw bytes from request stream slot
    pub async fn request_get(&self, stream_id: u64, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id);
        let (bytes, _hash) = self.request_cache.get(stream_idx, slot_id).await?;
        Some(bytes)
    }

    /// Get last slot index for a response stream
    pub fn response_last(&self, stream_id: u64) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id);
        self.response_cache.last(stream_idx)
    }

    /// Get raw bytes from response stream slot
    pub async fn response_get(&self, stream_id: u64, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id);
        let (bytes, _hash) = self.response_cache.get(stream_idx, slot_id).await?;
        Some(bytes)
    }

    /// Check if slot is the end marker (empty)
    pub fn is_end_marker(data: &[u8]) -> bool {
        data.is_empty()
    }

    /// Register a response channel for a given stream ID
    pub async fn register_response(&self, stream_id: u64) -> oneshot::Receiver<ResponseResult> {
        let (tx, rx) = oneshot::channel();
        let mut channels = self.response_channels.write().await;
        channels.insert(stream_id, tx);
        debug!(stream_id, "Registered response channel");
        rx
    }

    /// Complete a response for a given stream ID (called by watcher or external consumer)
    pub async fn complete_response(&self, stream_id: u64, result: ResponseResult) {
        let tx = {
            let mut channels = self.response_channels.write().await;
            channels.remove(&stream_id)
        };
        if let Some(tx) = tx {
            let _ = tx.send(result);
            debug!(stream_id, "Completed response");
        } else {
            warn!(stream_id, "No response channel found");
        }
    }

    /// Drop a response channel without completing (e.g., on timeout)
    pub async fn drop_response_channel(&self, stream_id: u64) {
        let mut channels = self.response_channels.write().await;
        channels.remove(&stream_id);
    }
}

/// Router implementation for upload-response
pub struct UploadResponseRouter {
    service: Arc<UploadResponseService>,
    ws_handler: UploadResponseWsHandler,
}

impl UploadResponseRouter {
    /// Create a new router with the given service
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        let ws_handler = UploadResponseWsHandler::new(Arc::clone(&service));
        Self { service, ws_handler }
    }

    /// Get a reference to the underlying service
    pub fn service(&self) -> Arc<UploadResponseService> {
        Arc::clone(&self.service)
    }

    /// Stream a request into the cache and wait for response
    ///
    /// Format:
    /// - Slot 1: HPKS headers frame
    /// - Slot 2..N-1: Raw body bytes
    /// - Slot N: END marker
    async fn stream_request(
        &self,
        req: Request<()>,
        mut body: Option<BodyStream>,
    ) -> HandlerResult<HandlerResponse> {
        // Acquire stream (blocks if at capacity)
        let _permit = self
            .service
            .acquire_stream()
            .await
            .map_err(|e| ServerError::Config(e))?;

        // Get next stream ID
        let stream_id = self.service.next_id();
        debug!(stream_id, uri = %req.uri(), "Streaming request");

        // Slot 1: HPKS headers frame
        let headers = StreamHeaders::from_request(stream_id, &req)
            .map_err(|e| ServerError::Config(e.to_string()))?;
        self.service
            .write_request_headers(stream_id, headers)
            .await
            .map_err(|e| ServerError::Config(e))?;

        // Slots 2+: Raw body bytes - write immediately as data arrives
        if let Some(ref mut body_stream) = body {
            use futures_util::StreamExt;

            let slot_bytes = self.service.config.slot_bytes();

            while let Some(chunk) = body_stream.next().await {
                let chunk = chunk?;
                if chunk.is_empty() {
                    continue;
                }

                // If chunk fits in a slot, write immediately
                if chunk.len() <= slot_bytes {
                    self.service
                        .append_request_body(stream_id, chunk)
                        .await
                        .map_err(|e| ServerError::Config(e))?;
                } else {
                    // Chunk too large - split into slot-sized pieces
                    let mut remaining = chunk;
                    while !remaining.is_empty() {
                        let take = remaining.len().min(slot_bytes);
                        let data = remaining.split_to(take);
                        self.service
                            .append_request_body(stream_id, data)
                            .await
                            .map_err(|e| ServerError::Config(e))?;
                    }
                }
            }
        }

        // Final slot: END marker
        self.service
            .end_request(stream_id)
            .await
            .map_err(|e| ServerError::Config(e))?;

        debug!(stream_id, "Request complete, waiting for response");

        // Register response channel and wait
        let rx = self.service.register_response(stream_id).await;

        let timeout_duration = Duration::from_millis(self.service.config.response_timeout_ms);
        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok((status, body)))) => {
                debug!(stream_id, ?status, "Received response");
                Ok(HandlerResponse {
                    status,
                    body: Some(body),
                    ..Default::default()
                })
            }
            Ok(Ok(Err(e))) => {
                error!(stream_id, error = %e, "Response error");
                self.service.drop_response_channel(stream_id).await;
                Err(ServerError::Config(e))
            }
            Ok(Err(_)) => {
                error!(stream_id, "Response channel closed");
                self.service.drop_response_channel(stream_id).await;
                Err(ServerError::Config("response channel closed".to_string()))
            }
            Err(_) => {
                error!(stream_id, "Response timeout");
                self.service.drop_response_channel(stream_id).await;
                Err(ServerError::Config("response timeout".to_string()))
            }
        }
    }
}

#[async_trait]
impl Router for UploadResponseRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        self.stream_request(req, None).await
    }

    async fn route_body(
        &self,
        req: Request<()>,
        body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        self.stream_request(req, Some(body)).await
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
        Err(ServerError::Config(
            "streaming responses not supported".to_string(),
        ))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        if path.starts_with("/upload") || path.starts_with("/ws") {
            Some(&self.ws_handler)
        } else {
            None
        }
    }
}

/// WebSocket handler for upload-response
pub struct UploadResponseWsHandler {
    service: Arc<UploadResponseService>,
}

impl UploadResponseWsHandler {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl WebSocketHandler for UploadResponseWsHandler {
    async fn handle_websocket(
        &self,
        req: Request<()>,
        mut stream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    ) -> HandlerResult<()> {
        // Acquire stream slot
        let _permit = self
            .service
            .acquire_stream()
            .await
            .map_err(|e| ServerError::Config(e))?;

        let stream_id = self.service.next_id();
        debug!(stream_id, uri = %req.uri(), "WebSocket stream started");

        // Slot 1: HPKS headers frame
        let headers = StreamHeaders::from_request(stream_id, &req)
            .map_err(|e| ServerError::Config(e.to_string()))?;
        self.service
            .write_request_headers(stream_id, headers)
            .await
            .map_err(|e| ServerError::Config(e))?;

        let slot_bytes = self.service.config.slot_bytes();

        // Read binary frames and write to request cache
        // Empty binary frame signals end of upload
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if data.is_empty() {
                        // Empty binary = end marker from client
                        break;
                    }
                    // Split into slot-sized chunks if needed
                    if data.len() <= slot_bytes {
                        self.service
                            .append_request_body(stream_id, Bytes::from(data))
                            .await
                            .map_err(|e| ServerError::Config(e))?;
                    } else {
                        let mut remaining = Bytes::from(data);
                        while !remaining.is_empty() {
                            let take = remaining.len().min(slot_bytes);
                            let chunk = remaining.split_to(take);
                            self.service
                                .append_request_body(stream_id, chunk)
                                .await
                                .map_err(|e| ServerError::Config(e))?;
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => continue, // Ignore text, ping, pong
                Err(e) => {
                    error!(stream_id, error = %e, "WebSocket error");
                    break;
                }
            }
        }

        // End marker
        self.service
            .end_request(stream_id)
            .await
            .map_err(|e| ServerError::Config(e))?;

        debug!(stream_id, "Request complete, waiting for response");

        // Wait for response
        let rx = self.service.register_response(stream_id).await;
        let timeout_duration = Duration::from_millis(self.service.config.response_timeout_ms);

        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok((status, body)))) => {
                debug!(stream_id, ?status, "Sending WebSocket response");
                // Send response as binary frame
                if let Err(e) = stream.send(Message::Binary(body.to_vec().into())).await {
                    error!(stream_id, error = %e, "Failed to send WebSocket response");
                }
                let _ = stream.close(None).await;
            }
            Ok(Ok(Err(e))) => {
                error!(stream_id, error = %e, "Response error");
                self.service.drop_response_channel(stream_id).await;
                let _ = stream.close(None).await;
            }
            Ok(Err(_)) => {
                error!(stream_id, "Response channel closed");
                self.service.drop_response_channel(stream_id).await;
                let _ = stream.close(None).await;
            }
            Err(_) => {
                error!(stream_id, "Response timeout");
                self.service.drop_response_channel(stream_id).await;
                let _ = stream.close(None).await;
            }
        }

        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        path.starts_with("/upload") || path.starts_with("/ws")
    }
}

/// Re-export for external consumers
pub use http_pack::stream::{decode_frame as decode_hpks_frame, encode_frame as encode_hpks_frame};

/// Slot content for workers tailing a request stream.
#[derive(Debug, Clone)]
pub enum TailSlot {
    /// Slot 1: Parsed request headers (method, path, headers)
    Headers(StreamRequestHeaders),
    /// Slots 2..N-1: Raw body bytes (zero-copy from cache)
    Body(Bytes),
    /// Final slot: End marker
    End,
}

impl UploadResponseService {
    /// Tail a request stream slot by slot.
    ///
    /// - Slot 1: Returns parsed headers
    /// - Slots 2..N-1: Returns raw body bytes (zero-copy)
    /// - Final slot: Returns End when END marker encountered
    pub async fn tail_request(&self, stream_id: u64, slot_id: usize) -> Option<TailSlot> {
        let bytes = self.request_get(stream_id, slot_id).await?;

        if slot_id == 1 {
            // First slot is HPKS headers frame
            let frame = decode_frame(&bytes).ok()?;
            if let StreamFrame::Headers(StreamHeaders::Request(req)) = frame {
                Some(TailSlot::Headers(req))
            } else {
                None
            }
        } else if Self::is_end_marker(&bytes) {
            Some(TailSlot::End)
        } else {
            // Raw body bytes - zero-copy
            Some(TailSlot::Body(bytes))
        }
    }

    /// Tail a response stream slot by slot.
    pub async fn tail_response(&self, stream_id: u64, slot_id: usize) -> Option<TailSlot> {
        let bytes = self.response_get(stream_id, slot_id).await?;

        if slot_id == 1 {
            // First slot is HPKS headers frame - use get_response_headers() instead
            None
        } else if Self::is_end_marker(&bytes) {
            Some(TailSlot::End)
        } else {
            Some(TailSlot::Body(bytes))
        }
    }

    /// Parse response headers from slot 1
    pub async fn get_response_headers(&self, stream_id: u64) -> Option<StreamResponseHeaders> {
        let bytes = self.response_get(stream_id, 1).await?;
        let frame = decode_frame(&bytes).ok()?;
        if let StreamFrame::Headers(StreamHeaders::Response(resp)) = frame {
            Some(resp)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_service_creation() {
        let config = UploadResponseConfig::default();
        let service = UploadResponseService::new(config);
        assert!(service.next_id() >= 1);
    }

    #[tokio::test]
    async fn test_stream_acquisition() {
        let config = UploadResponseConfig {
            num_streams: 2,
            ..Default::default()
        };
        let service = UploadResponseService::new(config);

        let _permit1 = service.acquire_stream().await.unwrap();
        let _permit2 = service.acquire_stream().await.unwrap();

        // Third acquisition should block
        let result = timeout(Duration::from_millis(10), service.acquire_stream()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stream_format() {
        let config = UploadResponseConfig::default();
        let service = UploadResponseService::new(config);

        let stream_id = 1u64;

        // Slot 1: HPKS headers frame
        let headers = StreamHeaders::Request(StreamRequestHeaders {
            stream_id,
            version: http_pack::HttpVersion::Http11,
            method: b"POST".to_vec(),
            scheme: None,
            authority: Some(b"example.com".to_vec()),
            path: b"/upload".to_vec(),
            headers: vec![],
        });
        service.write_request_headers(stream_id, headers).await.unwrap();

        // Slot 2: Raw body bytes
        service.append_request_body(stream_id, Bytes::from("hello")).await.unwrap();

        // Slot 3: More raw body bytes
        service.append_request_body(stream_id, Bytes::from(" world")).await.unwrap();

        // Slot 4: END marker
        service.end_request(stream_id).await.unwrap();

        // Verify slot contents
        let slot1 = service.request_get(stream_id, 1).await.unwrap();
        assert!(slot1.starts_with(b"HPKS")); // HPKS magic

        let slot2 = service.request_get(stream_id, 2).await.unwrap();
        assert_eq!(slot2, Bytes::from("hello"));

        let slot3 = service.request_get(stream_id, 3).await.unwrap();
        assert_eq!(slot3, Bytes::from(" world"));

        let slot4 = service.request_get(stream_id, 4).await.unwrap();
        assert!(UploadResponseService::is_end_marker(&slot4));
    }

    #[tokio::test]
    async fn test_tail_request() {
        let config = UploadResponseConfig::default();
        let service = UploadResponseService::new(config);

        let stream_id = 1u64;

        // Write stream
        let headers = StreamHeaders::Request(StreamRequestHeaders {
            stream_id,
            version: http_pack::HttpVersion::Http11,
            method: b"POST".to_vec(),
            scheme: None,
            authority: Some(b"example.com".to_vec()),
            path: b"/upload".to_vec(),
            headers: vec![],
        });
        service.write_request_headers(stream_id, headers).await.unwrap();
        service.append_request_body(stream_id, Bytes::from("hello world")).await.unwrap();
        service.end_request(stream_id).await.unwrap();

        // Tail the stream
        let slot1 = service.tail_request(stream_id, 1).await.unwrap();
        if let TailSlot::Headers(h) = slot1 {
            assert_eq!(h.method, b"POST");
            assert_eq!(h.path, b"/upload");
        } else {
            panic!("Expected headers");
        }

        let slot2 = service.tail_request(stream_id, 2).await.unwrap();
        if let TailSlot::Body(data) = slot2 {
            assert_eq!(data, Bytes::from("hello world"));
        } else {
            panic!("Expected body");
        }

        let slot3 = service.tail_request(stream_id, 3).await.unwrap();
        assert!(matches!(slot3, TailSlot::End));
    }

    #[tokio::test]
    async fn test_response_channel_roundtrip() {
        let config = UploadResponseConfig::default();
        let service = Arc::new(UploadResponseService::new(config));

        let stream_id = service.next_id();
        let rx = service.register_response(stream_id).await;

        service
            .complete_response(stream_id, Ok((StatusCode::OK, Bytes::from("ok"))))
            .await;

        let (status, body) = rx.await.unwrap().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from("ok"));
    }

    #[tokio::test]
    async fn test_reader_registration() {
        let config = UploadResponseConfig {
            num_streams: 10,
            ..Default::default()
        };
        let service = UploadResponseService::new(config);

        let stream_id = 1u64;

        // Initially no readers
        assert_eq!(service.reader_count(stream_id), 0);
        assert!(!service.has_readers(stream_id));

        // Register multiple readers
        assert!(service.register_reader(stream_id, "worker-1").await);
        assert!(service.register_reader(stream_id, "worker-2").await);
        assert!(service.register_reader(stream_id, "worker-3").await);

        assert_eq!(service.reader_count(stream_id), 3);
        assert!(service.has_readers(stream_id));
        assert!(service.is_reader_registered(stream_id, "worker-1").await);
        assert!(service.is_reader_registered(stream_id, "worker-2").await);

        // Duplicate registration returns false
        assert!(!service.register_reader(stream_id, "worker-1").await);
        assert_eq!(service.reader_count(stream_id), 3);

        // Unregister one reader
        assert!(service.unregister_reader(stream_id, "worker-2").await);
        assert_eq!(service.reader_count(stream_id), 2);
        assert!(!service.is_reader_registered(stream_id, "worker-2").await);

        // Unregister non-existent returns false
        assert!(!service.unregister_reader(stream_id, "worker-2").await);

        // Clear all readers
        service.clear_readers(stream_id).await;
        assert_eq!(service.reader_count(stream_id), 0);
    }

    #[tokio::test]
    async fn test_response_claim() {
        let config = UploadResponseConfig {
            num_streams: 10,
            ..Default::default()
        };
        let service = UploadResponseService::new(config);

        let stream_id = 1u64;

        // Initially unclaimed
        assert!(service.response_owner(stream_id).await.is_none());
        assert!(!service.is_response_claimed(stream_id).await);

        // Worker 1 claims successfully
        assert!(service.try_claim_response(stream_id, "writer-1").await);
        assert_eq!(service.response_owner(stream_id).await, Some("writer-1".to_string()));
        assert!(service.is_response_claimed_by(stream_id, "writer-1").await);
        assert!(!service.is_response_claimed_by(stream_id, "writer-2").await);

        // Worker 2 cannot claim (already claimed)
        assert!(!service.try_claim_response(stream_id, "writer-2").await);
        assert_eq!(service.response_owner(stream_id).await, Some("writer-1".to_string()));

        // Worker 2 cannot release (not owner)
        assert!(!service.release_response(stream_id, "writer-2").await);
        assert_eq!(service.response_owner(stream_id).await, Some("writer-1".to_string()));

        // Worker 1 releases successfully
        assert!(service.release_response(stream_id, "writer-1").await);
        assert!(service.response_owner(stream_id).await.is_none());

        // Now worker 2 can claim
        assert!(service.try_claim_response(stream_id, "writer-2").await);
        assert_eq!(service.response_owner(stream_id).await, Some("writer-2".to_string()));

        // Force release works regardless of owner
        service.force_release_response(stream_id).await;
        assert!(service.response_owner(stream_id).await.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_response_claim() {
        use std::sync::atomic::AtomicUsize;

        let config = UploadResponseConfig {
            num_streams: 10,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));

        let stream_id = 1u64;
        let claim_count = Arc::new(AtomicUsize::new(0));

        // Spawn 10 workers trying to claim the same response
        let mut handles = vec![];
        for i in 0..10 {
            let svc = Arc::clone(&service);
            let count = Arc::clone(&claim_count);
            let worker_id = format!("worker-{}", i);
            handles.push(tokio::spawn(async move {
                if svc.try_claim_response(stream_id, &worker_id).await {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Exactly one worker should have claimed it
        assert_eq!(claim_count.load(Ordering::SeqCst), 1);
        assert!(service.response_owner(stream_id).await.is_some());
    }
}
