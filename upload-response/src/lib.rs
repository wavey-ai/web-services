use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, StatusCode};
use http_pack::stream::{
    decode_frame, encode_frame, StreamFrame, StreamHeaders, StreamRequestHeaders,
    StreamResponseHeaders,
};
use hyper_util::rt::TokioIo;
use playlists::chunk_cache::ChunkCache;
use playlists::Options;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, error, warn};
use web_service::{
    BodyStream, HandlerResponse, HandlerResult, Router, ServerError, StreamWriter,
    WebSocketHandler, WebTransportHandler,
};

mod watcher;
pub use watcher::ResponseWatcher;

mod bridge;
pub use bridge::{
    build_streaming_response_head, clone_request_head, handler_response_from_cached,
    request_from_headers_slot, request_from_stream_headers, response_content_type, CachedIngress,
    CachedRequestGuard, IngressProxyConfig,
};

mod remote;
pub use remote::{
    discover_ingress_origins, RemoteIngressClient, RemoteRequestSlot, RemoteStageSlot,
    RemoteStreamInfo,
};

mod response_writer;
pub use response_writer::ResponseCacheWriter;

#[cfg(feature = "srt")]
mod srt;
#[cfg(feature = "srt")]
pub use srt::{AllowAll, AllowAllEncrypted, SrtAuth, SrtIngest};

#[cfg(feature = "rist")]
mod rist;
#[cfg(feature = "rist")]
pub use rist::{AllowAllRist, RistAuth, RistIngest};

#[cfg(feature = "webrtc")]
mod webrtc;
#[cfg(feature = "webrtc")]
pub use webrtc::{AllowAllWebRtc, WebRtcAuth, WebRtcIngest};

mod tcp;
pub use tcp::{AllowAllTcp, RequireClientCert, TcpAuth, TcpIngest};

#[cfg(feature = "udp-fec")]
mod udp_fec;
#[cfg(feature = "udp-fec")]
pub use udp_fec::{
    UdpFecIngest, UdpFecSender, DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, HEADER_LEN,
};

// For RTMP support, use rtmp-ingress with the "upload-response" feature:
// rtmp-ingress = { ..., features = ["upload-response"] }
// use rtmp_ingress::upload::{RtmpUploadIngest, RtmpAuth, AllowAll};

/// End-of-stream marker - empty slot
const END_MARKER: &[u8] = b"";
const REQUEST_CONTROL_MAGIC: &[u8; 8] = b"URCTRL1\0";

/// Control message embedded in a request stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestControl {
    Finalize,
    KeepAlive,
}

impl RequestControl {
    fn to_code(self) -> u8 {
        match self {
            Self::Finalize => 1,
            Self::KeepAlive => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Finalize),
            2 => Some(Self::KeepAlive),
            _ => None,
        }
    }

    fn as_slot_type(self) -> &'static str {
        match self {
            Self::Finalize => "control-finalize",
            Self::KeepAlive => "control-keepalive",
        }
    }
}

fn encode_request_control(control: RequestControl) -> Bytes {
    let mut payload = Vec::with_capacity(REQUEST_CONTROL_MAGIC.len() + 1);
    payload.extend_from_slice(REQUEST_CONTROL_MAGIC);
    payload.push(control.to_code());
    Bytes::from(payload)
}

fn decode_request_control(bytes: &[u8]) -> Option<RequestControl> {
    if bytes.len() != REQUEST_CONTROL_MAGIC.len() + 1 {
        return None;
    }
    if &bytes[..REQUEST_CONTROL_MAGIC.len()] != REQUEST_CONTROL_MAGIC {
        return None;
    }
    RequestControl::from_code(bytes[REQUEST_CONTROL_MAGIC.len()])
}

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
            // ChunkCache eagerly allocates its ring buffers, so keep defaults sized for
            // real streaming workloads instead of theoretical multi-GB uploads.
            num_streams: 16,
            slot_size_kb: 32,
            slots_per_stream: 1024,
            response_timeout_ms: 30000,
        }
    }
}

fn chunk_cache_options(config: &UploadResponseConfig) -> Options {
    let mut options = Options::default();
    options.num_playlists = config.num_streams;
    options.max_segments = 1;
    options.max_parts_per_segment = config.slots_per_stream;
    options.buffer_size_kb = config.slot_size_kb;
    options
}

/// Response type sent through oneshot channels
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedResponse {
    pub status: StatusCode,
    pub body: Bytes,
    pub headers: Vec<(String, String)>,
}

pub type ResponseResult = Result<CachedResponse, String>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeatUpdate {
    pub stage: String,
    pub max_inflight: usize,
    pub inflight: usize,
    pub available_slots: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub worker_id: String,
    pub stage: String,
    pub max_inflight: usize,
    pub inflight: usize,
    pub available_slots: usize,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapacitySummary {
    pub workers: usize,
    pub total_max_inflight: usize,
    pub total_inflight: usize,
    pub total_available_slots: usize,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Snapshot of an active stream slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveStreamSlot {
    pub stream_id: u64,
    pub stream_idx: usize,
}

/// Metadata returned by the internal cache API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveStreamInfo {
    pub stream_id: u64,
    pub stream_idx: usize,
    pub request_last: usize,
    pub response_last: usize,
    pub reader_count: u64,
    pub response_owner: Option<String>,
    pub stages: BTreeMap<String, StageState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageState {
    pub last: usize,
    pub owner: Option<String>,
}

impl ActiveStreamInfo {
    pub fn stage_last(&self, stage: &str) -> usize {
        self.stages.get(stage).map(|state| state.last).unwrap_or(0)
    }

    pub fn stage_owner(&self, stage: &str) -> Option<&str> {
        self.stages
            .get(stage)
            .and_then(|state| state.owner.as_deref())
    }
}

struct StageLane {
    cache: Arc<ChunkCache>,
    started: Vec<AtomicBool>,
    claims: Arc<RwLock<Vec<Option<String>>>>,
}

impl StageLane {
    fn new(config: &UploadResponseConfig) -> Self {
        let cache = Arc::new(ChunkCache::new(chunk_cache_options(config)));
        let started: Vec<AtomicBool> = (0..config.num_streams)
            .map(|_| AtomicBool::new(false))
            .collect();
        let claims: Vec<Option<String>> = (0..config.num_streams).map(|_| None).collect();

        Self {
            cache,
            started,
            claims: Arc::new(RwLock::new(claims)),
        }
    }

    fn last(&self, stream_idx: usize) -> usize {
        if !self.started[stream_idx].load(Ordering::SeqCst) {
            return 0;
        }
        self.cache.last(stream_idx).unwrap_or(0)
    }

    async fn clear_slot_state(&self, stream_idx: usize) {
        self.started[stream_idx].store(false, Ordering::SeqCst);
        let mut claims = self.claims.write().await;
        claims[stream_idx] = None;
    }
}

/// RAII handle for an active upload-response stream.
pub struct UploadStream {
    service: Arc<UploadResponseService>,
    stream_id: u64,
    stream_idx: usize,
    permit: Option<OwnedSemaphorePermit>,
}

impl UploadStream {
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub fn stream_idx(&self) -> usize {
        self.stream_idx
    }

    pub async fn close(mut self) {
        self.service.close_stream(self.stream_id).await;
        drop(self.permit.take());
    }
}

impl Drop for UploadStream {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };

        let service = Arc::clone(&self.service);
        let stream_id = self.stream_id;

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                service.close_stream(stream_id).await;
                drop(permit);
            });
        } else {
            drop(permit);
        }
    }
}

/// Main service for handling upload-response lifecycle.
///
/// Format per stream (playlist):
/// - Slot 1: HPKS Headers frame (method, path, headers)
/// - Slot 2..N-1: Raw body bytes (no framing overhead)
/// - Slot N: END marker
pub struct UploadResponseService {
    request_cache: Arc<ChunkCache>,
    response_cache: Arc<ChunkCache>,
    stages: Arc<RwLock<HashMap<String, Arc<StageLane>>>>,
    slot_semaphore: Arc<Semaphore>,
    next_stream_id: AtomicU64,
    stream_to_slot: StdRwLock<HashMap<u64, usize>>,
    free_slots: StdMutex<Vec<usize>>,
    slot_stream_ids: Vec<AtomicU64>,
    response_channels: Arc<RwLock<HashMap<u64, oneshot::Sender<ResponseResult>>>>,
    /// Per-stream worker count: how many workers are currently reading/processing
    stream_worker_counts: Vec<AtomicU64>,
    request_started: Vec<AtomicBool>,
    response_started: Vec<AtomicBool>,
    /// Per-stream worker sets: which worker IDs are reading/processing each stream
    stream_workers: Arc<RwLock<Vec<std::collections::HashSet<String>>>>,
    /// Per-stream response claim: None = unclaimed, Some(worker_id) = exclusive write access
    response_claims: Arc<RwLock<Vec<Option<String>>>>,
    /// Worker heartbeat/capacity registry keyed by worker id.
    workers: Arc<RwLock<HashMap<String, WorkerHeartbeat>>>,
    config: UploadResponseConfig,
}

impl UploadResponseService {
    /// Create a new upload-response service with the given configuration
    pub fn new(config: UploadResponseConfig) -> Self {
        let options = chunk_cache_options(&config);
        let request_cache = Arc::new(ChunkCache::new(options));
        let response_cache = Arc::new(ChunkCache::new(chunk_cache_options(&config)));
        let slot_semaphore = Arc::new(Semaphore::new(config.num_streams));
        let free_slots: Vec<usize> = (0..config.num_streams).rev().collect();

        // Initialize per-stream worker counts
        let stream_worker_counts: Vec<AtomicU64> =
            (0..config.num_streams).map(|_| AtomicU64::new(0)).collect();
        let request_started: Vec<AtomicBool> = (0..config.num_streams)
            .map(|_| AtomicBool::new(false))
            .collect();
        let response_started: Vec<AtomicBool> = (0..config.num_streams)
            .map(|_| AtomicBool::new(false))
            .collect();

        // Initialize per-stream worker sets (for readers)
        let stream_workers: Vec<std::collections::HashSet<String>> = (0..config.num_streams)
            .map(|_| std::collections::HashSet::new())
            .collect();

        // Initialize per-stream response claims (for exclusive writer)
        let response_claims: Vec<Option<String>> = (0..config.num_streams).map(|_| None).collect();

        Self {
            request_cache,
            response_cache,
            stages: Arc::new(RwLock::new(HashMap::new())),
            slot_semaphore,
            next_stream_id: AtomicU64::new(1),
            stream_to_slot: StdRwLock::new(HashMap::new()),
            free_slots: StdMutex::new(free_slots),
            slot_stream_ids: (0..config.num_streams).map(|_| AtomicU64::new(0)).collect(),
            response_channels: Arc::new(RwLock::new(HashMap::new())),
            stream_worker_counts,
            request_started,
            response_started,
            stream_workers: Arc::new(RwLock::new(stream_workers)),
            response_claims: Arc::new(RwLock::new(response_claims)),
            workers: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    fn allocate_slot(&self) -> Option<usize> {
        self.free_slots.lock().ok()?.pop()
    }

    fn release_slot(&self, stream_idx: usize) {
        if let Ok(mut free_slots) = self.free_slots.lock() {
            free_slots.push(stream_idx);
        }
    }

    fn stream_idx(&self, stream_id: u64) -> Option<usize> {
        self.stream_to_slot
            .read()
            .ok()
            .and_then(|slots| slots.get(&stream_id).copied())
    }

    pub fn slot_stream_id(&self, stream_idx: usize) -> Option<u64> {
        let stream_id = self.slot_stream_ids[stream_idx].load(Ordering::Acquire);
        (stream_id != 0).then_some(stream_id)
    }

    pub fn active_stream_slots(&self) -> Vec<ActiveStreamSlot> {
        self.slot_stream_ids
            .iter()
            .enumerate()
            .filter_map(|(stream_idx, stream_id)| {
                let stream_id = stream_id.load(Ordering::Acquire);
                (stream_id != 0).then_some(ActiveStreamSlot {
                    stream_id,
                    stream_idx,
                })
            })
            .collect()
    }

    async fn get_or_create_stage_lane(&self, stage: &str) -> Arc<StageLane> {
        {
            let stages = self.stages.read().await;
            if let Some(lane) = stages.get(stage) {
                return Arc::clone(lane);
            }
        }

        let mut stages = self.stages.write().await;
        Arc::clone(
            stages
                .entry(stage.to_string())
                .or_insert_with(|| Arc::new(StageLane::new(&self.config))),
        )
    }

    async fn get_stage_lane(&self, stage: &str) -> Option<Arc<StageLane>> {
        let stages = self.stages.read().await;
        stages.get(stage).cloned()
    }

    pub async fn stage_names(&self) -> Vec<String> {
        let stages = self.stages.read().await;
        let mut names: Vec<_> = stages.keys().cloned().collect();
        names.sort();
        names
    }

    pub async fn active_streams(&self) -> Vec<ActiveStreamInfo> {
        let claims = self.response_claims.read().await;
        let stage_names = self.stage_names().await;
        let mut active = Vec::new();

        for slot in self.active_stream_slots() {
            let mut stages = BTreeMap::new();
            for stage_name in &stage_names {
                if let Some(lane) = self.get_stage_lane(stage_name).await {
                    let owner = {
                        let stage_claims = lane.claims.read().await;
                        stage_claims[slot.stream_idx].clone()
                    };
                    let last = lane.last(slot.stream_idx);
                    if last > 0 || owner.is_some() {
                        stages.insert(stage_name.clone(), StageState { last, owner });
                    }
                }
            }

            active.push(ActiveStreamInfo {
                stream_id: slot.stream_id,
                stream_idx: slot.stream_idx,
                request_last: self.request_cache.last(slot.stream_idx).unwrap_or(0),
                response_last: self.response_cache.last(slot.stream_idx).unwrap_or(0),
                reader_count: self.stream_worker_counts[slot.stream_idx].load(Ordering::SeqCst),
                response_owner: claims[slot.stream_idx].clone(),
                stages,
            });
        }

        active
    }

    async fn clear_slot_state(&self, stream_idx: usize) {
        {
            let mut workers = self.stream_workers.write().await;
            workers[stream_idx].clear();
        }
        self.stream_worker_counts[stream_idx].store(0, Ordering::SeqCst);
        self.request_started[stream_idx].store(false, Ordering::SeqCst);
        self.response_started[stream_idx].store(false, Ordering::SeqCst);
        {
            let stages = self.stages.read().await;
            let lanes: Vec<_> = stages.values().cloned().collect();
            drop(stages);
            for lane in lanes {
                lane.clear_slot_state(stream_idx).await;
            }
        }
        {
            let mut claims = self.response_claims.write().await;
            claims[stream_idx] = None;
        }
    }

    pub async fn close_stream(&self, stream_id: u64) {
        let stream_idx = {
            let mut stream_to_slot = match self.stream_to_slot.write() {
                Ok(stream_to_slot) => stream_to_slot,
                Err(_) => return,
            };
            match stream_to_slot.remove(&stream_id) {
                Some(stream_idx) => stream_idx,
                None => return,
            }
        };

        self.slot_stream_ids[stream_idx].store(0, Ordering::Release);
        self.clear_slot_state(stream_idx).await;
        self.drop_response_channel(stream_id).await;
        self.release_slot(stream_idx);
        debug!(stream_id, stream_idx, "Stream closed");
    }

    pub async fn open_stream(self: &Arc<Self>) -> Result<UploadStream, String> {
        let permit = self
            .slot_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "streams closed".to_string())?;

        let stream_idx = self
            .allocate_slot()
            .ok_or_else(|| "no free slot available".to_string())?;
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);

        self.clear_slot_state(stream_idx).await;

        {
            let mut stream_to_slot = self
                .stream_to_slot
                .write()
                .map_err(|_| "stream slot registry poisoned".to_string())?;
            stream_to_slot.insert(stream_id, stream_idx);
        }
        self.slot_stream_ids[stream_idx].store(stream_id, Ordering::Release);

        debug!(stream_id, stream_idx, "Stream opened");

        Ok(UploadStream {
            service: Arc::clone(self),
            stream_id,
            stream_idx,
            permit: Some(permit),
        })
    }

    // ==================== Reader Registration (Multiple Workers) ====================

    /// Register a worker as reading/processing a stream.
    ///
    /// Multiple workers can register on the same stream for reading.
    /// Returns `true` if this worker was newly registered.
    /// Returns `false` if this worker was already registered on this stream.
    pub async fn register_reader(&self, stream_id: u64, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
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
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
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
        self.stream_idx(stream_id)
            .map(|stream_idx| self.stream_worker_counts[stream_idx].load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Check if any readers are processing a stream (lock-free).
    pub fn has_readers(&self, stream_id: u64) -> bool {
        self.reader_count(stream_id) > 0
    }

    /// Check if a specific reader is registered on a stream.
    pub async fn is_reader_registered(&self, stream_id: u64, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let workers = self.stream_workers.read().await;
        workers[stream_idx].contains(worker_id)
    }

    /// Get all reader worker IDs currently processing a stream.
    pub async fn get_readers(&self, stream_id: u64) -> Vec<String> {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return Vec::new();
        };
        let workers = self.stream_workers.read().await;
        workers[stream_idx].iter().cloned().collect()
    }

    /// Clear all readers from a stream (for cleanup/recovery).
    pub async fn clear_readers(&self, stream_id: u64) {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return;
        };
        let mut workers = self.stream_workers.write().await;
        workers[stream_idx].clear();
        self.stream_worker_counts[stream_idx].store(0, Ordering::SeqCst);
        debug!(stream_id, "All readers cleared");
    }

    // ==================== Response Writer Claim (Exclusive) ====================

    /// Try to claim exclusive access to a named stage on a stream.
    pub async fn try_claim_stage(&self, stream_id: u64, stage: &str, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let lane = self.get_or_create_stage_lane(stage).await;
        let mut claims = lane.claims.write().await;
        if claims[stream_idx].is_none() {
            claims[stream_idx] = Some(worker_id.to_string());
            debug!(stream_id, stage, worker_id, "Stage claimed");
            true
        } else {
            false
        }
    }

    /// Release a previously claimed stage.
    pub async fn release_stage(&self, stream_id: u64, stage: &str, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let Some(lane) = self.get_stage_lane(stage).await else {
            return false;
        };
        let mut claims = lane.claims.write().await;
        if claims[stream_idx].as_deref() == Some(worker_id) {
            claims[stream_idx] = None;
            debug!(stream_id, stage, worker_id, "Stage released");
            true
        } else {
            false
        }
    }

    /// Force-release stage ownership regardless of owner (for cleanup/recovery).
    pub async fn force_release_stage(&self, stream_id: u64, stage: &str) {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return;
        };
        let Some(lane) = self.get_stage_lane(stage).await else {
            return;
        };
        let mut claims = lane.claims.write().await;
        claims[stream_idx] = None;
        debug!(stream_id, stage, "Stage force-released");
    }

    /// Get the worker ID that currently holds a stage claim, if any.
    pub async fn stage_owner(&self, stream_id: u64, stage: &str) -> Option<String> {
        let stream_idx = self.stream_idx(stream_id)?;
        let lane = self.get_stage_lane(stage).await?;
        let claims = lane.claims.read().await;
        claims[stream_idx].clone()
    }

    /// Check if a stage is claimed by anyone.
    pub async fn is_stage_claimed(&self, stream_id: u64, stage: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let Some(lane) = self.get_stage_lane(stage).await else {
            return false;
        };
        let claims = lane.claims.read().await;
        claims[stream_idx].is_some()
    }

    /// Try to claim exclusive write access to a stream's response.
    ///
    /// Only one worker can hold the response claim at a time.
    /// Returns `true` if the claim succeeded (response was unclaimed).
    /// Returns `false` if another worker already claimed this response.
    pub async fn try_claim_response(&self, stream_id: u64, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
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
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
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
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return;
        };
        let mut claims = self.response_claims.write().await;
        claims[stream_idx] = None;
        debug!(stream_id, "Response force-released");
    }

    /// Get the worker ID that currently holds the response claim, if any.
    pub async fn response_owner(&self, stream_id: u64) -> Option<String> {
        let stream_idx = self.stream_idx(stream_id)?;
        let claims = self.response_claims.read().await;
        claims[stream_idx].clone()
    }

    /// Check if the response is claimed by a specific worker.
    pub async fn is_response_claimed_by(&self, stream_id: u64, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let claims = self.response_claims.read().await;
        claims[stream_idx].as_deref() == Some(worker_id)
    }

    /// Check if the response is currently claimed by anyone.
    pub async fn is_response_claimed(&self, stream_id: u64) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let claims = self.response_claims.read().await;
        claims[stream_idx].is_some()
    }

    fn prune_stale_workers_locked(
        workers: &mut HashMap<String, WorkerHeartbeat>,
        stale_before_ms: u64,
    ) {
        workers.retain(|_, worker| worker.updated_at_ms >= stale_before_ms);
    }

    pub async fn upsert_worker_heartbeat(
        &self,
        worker_id: &str,
        update: WorkerHeartbeatUpdate,
    ) -> WorkerHeartbeat {
        let heartbeat = WorkerHeartbeat {
            worker_id: worker_id.to_string(),
            stage: update.stage,
            max_inflight: update.max_inflight,
            inflight: update.inflight.min(update.max_inflight),
            available_slots: update.available_slots.min(update.max_inflight),
            updated_at_ms: now_unix_ms(),
        };
        let mut workers = self.workers.write().await;
        workers.insert(worker_id.to_string(), heartbeat.clone());
        heartbeat
    }

    pub async fn worker_heartbeat(&self, worker_id: &str) -> Option<WorkerHeartbeat> {
        let workers = self.workers.read().await;
        workers.get(worker_id).cloned()
    }

    pub async fn list_workers(&self, ttl_ms: Option<u64>) -> Vec<WorkerHeartbeat> {
        let stale_before_ms = ttl_ms.map(|ttl| now_unix_ms().saturating_sub(ttl));
        let mut workers = self.workers.write().await;
        if let Some(stale_before_ms) = stale_before_ms {
            Self::prune_stale_workers_locked(&mut workers, stale_before_ms);
        }
        let mut listed: Vec<_> = workers.values().cloned().collect();
        listed.sort_by(|left, right| left.worker_id.cmp(&right.worker_id));
        listed
    }

    pub async fn worker_capacity_summary(&self, ttl_ms: Option<u64>) -> WorkerCapacitySummary {
        let workers = self.list_workers(ttl_ms).await;
        WorkerCapacitySummary {
            workers: workers.len(),
            total_max_inflight: workers.iter().map(|worker| worker.max_inflight).sum(),
            total_inflight: workers.iter().map(|worker| worker.inflight).sum(),
            total_available_slots: workers.iter().map(|worker| worker.available_slots).sum(),
        }
    }

    /// Get a reference to the request cache for external consumers
    pub fn request_cache(&self) -> Arc<ChunkCache> {
        Arc::clone(&self.request_cache)
    }

    /// Get a reference to a named stage cache for external consumers.
    pub async fn stage_cache(&self, stage: &str) -> Arc<ChunkCache> {
        let lane = self.get_or_create_stage_lane(stage).await;
        Arc::clone(&lane.cache)
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

    /// Write HPKS headers frame to slot 1 of request stream
    pub async fn write_request_headers(
        &self,
        stream_id: u64,
        headers: StreamHeaders,
    ) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let encoded = encode_frame(&StreamFrame::Headers(headers));
        self.request_cache
            .add(stream_idx, 1, Bytes::from(encoded))
            .await
            .map_err(|e| e.to_string())?;
        self.request_started[stream_idx].store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Append raw body bytes to request stream (slots 2+)
    pub async fn append_request_body(&self, stream_id: u64, data: Bytes) -> Result<(), String> {
        if data.is_empty() {
            return Err("request body chunks cannot be empty".to_string());
        }
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        if !self.request_started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!(
                "request headers not written for stream: {stream_id}"
            ));
        }
        self.request_cache
            .append(stream_idx, data)
            .await
            .map_err(|e| e.to_string())
    }

    /// Append a request control marker after the headers slot.
    pub async fn append_request_control(
        &self,
        stream_id: u64,
        control: RequestControl,
    ) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        if !self.request_started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!(
                "request headers not written for stream: {stream_id}"
            ));
        }
        self.request_cache
            .append(stream_idx, encode_request_control(control))
            .await
            .map_err(|e| e.to_string())
    }

    /// Write end marker to request stream
    pub async fn end_request(&self, stream_id: u64) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        if !self.request_started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!(
                "request headers not written for stream: {stream_id}"
            ));
        }
        self.request_cache
            .append(stream_idx, Bytes::from_static(END_MARKER))
            .await
            .map_err(|e| e.to_string())
    }

    /// Write opaque stage head bytes to slot 1 of a named stage stream.
    pub async fn write_stage_head(
        &self,
        stream_id: u64,
        stage: &str,
        head: Bytes,
    ) -> Result<(), String> {
        if head.is_empty() {
            return Err("stage head cannot be empty".to_string());
        }
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let lane = self.get_or_create_stage_lane(stage).await;
        lane.cache
            .add(stream_idx, 1, head)
            .await
            .map_err(|e| e.to_string())?;
        lane.started[stream_idx].store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Append opaque bytes to a named stage stream (slots 2+).
    pub async fn append_stage_body(
        &self,
        stream_id: u64,
        stage: &str,
        data: Bytes,
    ) -> Result<(), String> {
        if data.is_empty() {
            return Err("stage body chunks cannot be empty".to_string());
        }
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let lane = self.get_or_create_stage_lane(stage).await;
        if !lane.started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!("stage head not written for stream: {stream_id}"));
        }
        lane.cache
            .append(stream_idx, data)
            .await
            .map_err(|e| e.to_string())
    }

    /// Append a stage control marker after the stage head slot.
    pub async fn append_stage_control(
        &self,
        stream_id: u64,
        stage: &str,
        control: RequestControl,
    ) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let lane = self.get_or_create_stage_lane(stage).await;
        if !lane.started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!("stage head not written for stream: {stream_id}"));
        }
        lane.cache
            .append(stream_idx, encode_request_control(control))
            .await
            .map_err(|e| e.to_string())
    }

    /// Write end marker to a named stage stream.
    pub async fn end_stage(&self, stream_id: u64, stage: &str) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let lane = self.get_or_create_stage_lane(stage).await;
        if !lane.started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!("stage head not written for stream: {stream_id}"));
        }
        lane.cache
            .append(stream_idx, Bytes::from_static(END_MARKER))
            .await
            .map_err(|e| e.to_string())
    }

    /// Write HPKS headers frame to slot 1 of response stream
    pub async fn write_response_headers(
        &self,
        stream_id: u64,
        headers: StreamHeaders,
    ) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let encoded = encode_frame(&StreamFrame::Headers(headers));
        self.response_cache
            .add(stream_idx, 1, Bytes::from(encoded))
            .await
            .map_err(|e| e.to_string())?;
        self.response_started[stream_idx].store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Append raw body bytes to response stream (slots 2+)
    pub async fn append_response_body(&self, stream_id: u64, data: Bytes) -> Result<(), String> {
        if data.is_empty() {
            return Err("response body chunks cannot be empty".to_string());
        }
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        if !self.response_started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!(
                "response headers not written for stream: {stream_id}"
            ));
        }
        self.response_cache
            .append(stream_idx, data)
            .await
            .map_err(|e| e.to_string())
    }

    /// Write end marker to response stream
    pub async fn end_response(&self, stream_id: u64) -> Result<(), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        if !self.response_started[stream_idx].load(Ordering::SeqCst) {
            return Err(format!(
                "response headers not written for stream: {stream_id}"
            ));
        }
        self.response_cache
            .append(stream_idx, Bytes::from_static(END_MARKER))
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn write_handler_response(
        &self,
        stream_id: u64,
        response: HandlerResponse,
    ) -> Result<(), String> {
        let mut builder = http::Response::builder().status(response.status);
        if let Some(content_type) = &response.content_type {
            builder = builder.header(http::header::CONTENT_TYPE, content_type);
        }
        if let Some(etag) = response.etag {
            builder = builder.header(http::header::ETAG, etag.to_string());
        }
        for (name, value) in &response.headers {
            builder = builder.header(name, value);
        }

        let response_head = builder
            .body(())
            .map_err(|error| format!("failed to build response: {error}"))?;
        let headers = StreamHeaders::from_response(stream_id, &response_head)
            .map_err(|error| format!("failed to encode response headers: {error}"))?;
        self.write_response_headers(stream_id, headers).await?;

        if let Some(body) = response.body {
            if !body.is_empty() {
                self.append_response_body(stream_id, body).await?;
            }
        }

        self.end_response(stream_id).await
    }

    /// Get last slot index for a request stream
    pub fn request_last(&self, stream_id: u64) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id)?;
        if !self.request_started[stream_idx].load(Ordering::SeqCst) {
            return Some(0);
        }
        self.request_cache.last(stream_idx)
    }

    /// Get raw bytes from request stream slot
    pub async fn request_get(&self, stream_id: u64, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id)?;
        if !self.request_started[stream_idx].load(Ordering::SeqCst) {
            return None;
        }
        let (bytes, _hash) = self.request_cache.get(stream_idx, slot_id).await?;
        Some(bytes)
    }

    /// Get last slot index for a response stream
    pub fn response_last(&self, stream_id: u64) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id)?;
        if !self.response_started[stream_idx].load(Ordering::SeqCst) {
            return Some(0);
        }
        self.response_cache.last(stream_idx)
    }

    /// Get last slot index for a named stage stream.
    pub async fn stage_last(&self, stream_id: u64, stage: &str) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id)?;
        let lane = self.get_stage_lane(stage).await?;
        if !lane.started[stream_idx].load(Ordering::SeqCst) {
            return Some(0);
        }
        lane.cache.last(stream_idx)
    }

    /// Get raw bytes from a named stage stream slot.
    pub async fn stage_get(&self, stream_id: u64, stage: &str, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id)?;
        let lane = self.get_stage_lane(stage).await?;
        if !lane.started[stream_idx].load(Ordering::SeqCst) {
            return None;
        }
        let (bytes, _hash) = lane.cache.get(stream_idx, slot_id).await?;
        Some(bytes)
    }

    /// Get raw bytes from response stream slot
    pub async fn response_get(&self, stream_id: u64, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id)?;
        if !self.response_started[stream_idx].load(Ordering::SeqCst) {
            return None;
        }
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
        Self {
            service,
            ws_handler,
        }
    }

    /// Get a reference to the underlying service
    pub fn service(&self) -> Arc<UploadResponseService> {
        Arc::clone(&self.service)
    }

    fn is_internal_path(path: &str) -> bool {
        path == "/_upload_response/streams"
            || path.starts_with("/_upload_response/streams/")
            || path == "/_upload_response/workers"
            || path.starts_with("/_upload_response/workers/")
            || path == "/_upload_response/capacity"
    }

    fn text_response(status: StatusCode, body: impl Into<String>) -> HandlerResponse {
        HandlerResponse {
            status,
            body: Some(Bytes::from(body.into())),
            content_type: Some("text/plain; charset=utf-8".to_string()),
            ..Default::default()
        }
    }

    fn json_response<T: Serialize>(status: StatusCode, value: &T) -> HandlerResponse {
        HandlerResponse {
            status,
            body: Some(Bytes::from(
                serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec()),
            )),
            content_type: Some("application/json".to_string()),
            ..Default::default()
        }
    }

    fn binary_response(
        status: StatusCode,
        body: Bytes,
        slot_type: Option<&str>,
    ) -> HandlerResponse {
        let mut headers = Vec::new();
        if let Some(slot_type) = slot_type {
            headers.push((
                "x-upload-response-slot-type".to_string(),
                slot_type.to_string(),
            ));
        }
        HandlerResponse {
            status,
            body: Some(body),
            content_type: Some("application/octet-stream".to_string()),
            headers,
            ..Default::default()
        }
    }

    async fn collect_body(mut body: Option<BodyStream>) -> Result<Bytes, ServerError> {
        let Some(ref mut body_stream) = body else {
            return Ok(Bytes::new());
        };

        let mut collected = Vec::new();
        while let Some(chunk) = body_stream.next().await {
            let chunk = chunk?;
            collected.extend_from_slice(&chunk);
        }

        Ok(Bytes::from(collected))
    }

    fn parse_u64_component(value: &str, name: &str) -> Result<u64, ServerError> {
        value
            .parse::<u64>()
            .map_err(|_| ServerError::Config(format!("invalid {name}: {value}")))
    }

    fn parse_usize_component(value: &str, name: &str) -> Result<usize, ServerError> {
        value
            .parse::<usize>()
            .map_err(|_| ServerError::Config(format!("invalid {name}: {value}")))
    }

    fn format_stream_info(info: &ActiveStreamInfo) -> String {
        let stages = serde_json::to_string(&info.stages).unwrap_or_else(|_| "{}".to_string());
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            info.stream_id,
            info.stream_idx,
            info.request_last,
            info.response_last,
            info.reader_count,
            info.response_owner.as_deref().unwrap_or("-"),
            stages
        )
    }

    fn parse_json_body<T: for<'de> Deserialize<'de>>(body: Bytes) -> Result<T, ServerError> {
        serde_json::from_slice(&body)
            .map_err(|error| ServerError::Config(format!("invalid json body: {error}")))
    }

    async fn route_internal(
        &self,
        req: Request<()>,
        body: Option<BodyStream>,
    ) -> HandlerResult<HandlerResponse> {
        let method = req.method().clone();
        let path_parts: Vec<&str> = req
            .uri()
            .path()
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();

        match (method.as_str(), path_parts.as_slice()) {
            ("GET", ["_upload_response", "streams"]) => {
                let mut lines = vec![
                    "stream_id\tstream_idx\trequest_last\tresponse_last\treaders\tresponse_owner\tstages_json"
                        .to_string(),
                ];
                for info in self.service.active_streams().await {
                    lines.push(Self::format_stream_info(&info));
                }
                Ok(Self::text_response(StatusCode::OK, lines.join("\n")))
            }
            ("GET", ["_upload_response", "workers"]) => Ok(Self::json_response(
                StatusCode::OK,
                &self.service.list_workers(None).await,
            )),
            ("GET", ["_upload_response", "workers", worker_id]) => {
                match self.service.worker_heartbeat(worker_id).await {
                    Some(worker) => Ok(Self::json_response(StatusCode::OK, &worker)),
                    None => Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "worker not found",
                    )),
                }
            }
            ("GET", ["_upload_response", "capacity"]) => Ok(Self::json_response(
                StatusCode::OK,
                &self.service.worker_capacity_summary(None).await,
            )),
            ("PUT", ["_upload_response", "workers", worker_id, "heartbeat"]) => {
                let body = Self::collect_body(body).await?;
                let update = Self::parse_json_body::<WorkerHeartbeatUpdate>(body)?;
                let worker = self
                    .service
                    .upsert_worker_heartbeat(worker_id, update)
                    .await;
                Ok(Self::json_response(StatusCode::OK, &worker))
            }
            ("GET", ["_upload_response", "streams", stream_id]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                match self
                    .service
                    .active_streams()
                    .await
                    .into_iter()
                    .find(|info| info.stream_id == stream_id)
                {
                    Some(info) => Ok(Self::text_response(
                        StatusCode::OK,
                        Self::format_stream_info(&info),
                    )),
                    None => Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    )),
                }
            }
            ("GET", ["_upload_response", "streams", stream_id, "request", "last"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                match self.service.request_last(stream_id) {
                    Some(last) => Ok(Self::text_response(StatusCode::OK, last.to_string())),
                    None => Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    )),
                }
            }
            ("GET", ["_upload_response", "streams", stream_id, "response", "last"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                match self.service.response_last(stream_id) {
                    Some(last) => Ok(Self::text_response(StatusCode::OK, last.to_string())),
                    None => Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    )),
                }
            }
            ("GET", ["_upload_response", "streams", stream_id, "stages", stage, "last"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                match self.service.stage_last(stream_id, stage).await {
                    Some(last) => Ok(Self::text_response(StatusCode::OK, last.to_string())),
                    None => Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream or stage not found",
                    )),
                }
            }
            ("GET", ["_upload_response", "streams", stream_id, "request", "slots", slot_id]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                let slot_id = Self::parse_usize_component(slot_id, "slot_id")?;
                match self.service.request_get(stream_id, slot_id).await {
                    Some(bytes) => {
                        let slot_type = if slot_id == 1 {
                            Some("headers")
                        } else if UploadResponseService::is_end_marker(&bytes) {
                            Some("end")
                        } else if let Some(control) = decode_request_control(&bytes) {
                            Some(control.as_slot_type())
                        } else {
                            Some("body")
                        };
                        Ok(Self::binary_response(StatusCode::OK, bytes, slot_type))
                    }
                    None => Ok(Self::text_response(StatusCode::NOT_FOUND, "slot not found")),
                }
            }
            ("GET", ["_upload_response", "streams", stream_id, "response", "slots", slot_id]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                let slot_id = Self::parse_usize_component(slot_id, "slot_id")?;
                match self.service.response_get(stream_id, slot_id).await {
                    Some(bytes) => {
                        let slot_type = if slot_id == 1 {
                            Some("headers")
                        } else if UploadResponseService::is_end_marker(&bytes) {
                            Some("end")
                        } else {
                            Some("body")
                        };
                        Ok(Self::binary_response(StatusCode::OK, bytes, slot_type))
                    }
                    None => Ok(Self::text_response(StatusCode::NOT_FOUND, "slot not found")),
                }
            }
            (
                "GET",
                ["_upload_response", "streams", stream_id, "stages", stage, "slots", slot_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                let slot_id = Self::parse_usize_component(slot_id, "slot_id")?;
                match self.service.stage_get(stream_id, stage, slot_id).await {
                    Some(bytes) => {
                        let slot_type = if slot_id == 1 {
                            Some("head")
                        } else if UploadResponseService::is_end_marker(&bytes) {
                            Some("end")
                        } else if let Some(control) = decode_request_control(&bytes) {
                            Some(control.as_slot_type())
                        } else {
                            Some("body")
                        };
                        Ok(Self::binary_response(StatusCode::OK, bytes, slot_type))
                    }
                    None => Ok(Self::text_response(StatusCode::NOT_FOUND, "slot not found")),
                }
            }
            ("PUT", ["_upload_response", "streams", stream_id, "readers", worker_id]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let inserted = self.service.register_reader(stream_id, worker_id).await;
                let status = if inserted {
                    StatusCode::OK
                } else {
                    StatusCode::NO_CONTENT
                };
                Ok(Self::text_response(status, "ok"))
            }
            ("DELETE", ["_upload_response", "streams", stream_id, "readers", worker_id]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let removed = self.service.unregister_reader(stream_id, worker_id).await;
                let status = if removed {
                    StatusCode::OK
                } else {
                    StatusCode::NO_CONTENT
                };
                Ok(Self::text_response(status, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "response", "claim", worker_id]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                if self.service.try_claim_response(stream_id, worker_id).await {
                    Ok(Self::text_response(StatusCode::OK, "claimed"))
                } else {
                    let owner = self
                        .service
                        .response_owner(stream_id)
                        .await
                        .unwrap_or_else(|| "-".to_string());
                    Ok(Self::text_response(
                        StatusCode::CONFLICT,
                        format!("already claimed by {owner}"),
                    ))
                }
            }
            (
                "DELETE",
                ["_upload_response", "streams", stream_id, "response", "claim", worker_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let released = self.service.release_response(stream_id, worker_id).await;
                let status = if released {
                    StatusCode::OK
                } else {
                    StatusCode::CONFLICT
                };
                Ok(Self::text_response(status, "ok"))
            }
            (
                "PUT",
                ["_upload_response", "streams", stream_id, "stages", stage, "claim", worker_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                if self
                    .service
                    .try_claim_stage(stream_id, stage, worker_id)
                    .await
                {
                    Ok(Self::text_response(StatusCode::OK, "claimed"))
                } else {
                    let owner = self
                        .service
                        .stage_owner(stream_id, stage)
                        .await
                        .unwrap_or_else(|| "-".to_string());
                    Ok(Self::text_response(
                        StatusCode::CONFLICT,
                        format!("already claimed by {owner}"),
                    ))
                }
            }
            (
                "DELETE",
                ["_upload_response", "streams", stream_id, "stages", stage, "claim", worker_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let released = self
                    .service
                    .release_stage(stream_id, stage, worker_id)
                    .await;
                let status = if released {
                    StatusCode::OK
                } else {
                    StatusCode::CONFLICT
                };
                Ok(Self::text_response(status, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "stages", stage, "head"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let body = Self::collect_body(body).await?;
                self.service
                    .write_stage_head(stream_id, stage, body)
                    .await
                    .map_err(ServerError::Config)?;
                Ok(Self::text_response(StatusCode::OK, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "stages", stage, "body"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let body = Self::collect_body(body).await?;
                self.service
                    .append_stage_body(stream_id, stage, body)
                    .await
                    .map_err(ServerError::Config)?;
                Ok(Self::text_response(StatusCode::OK, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "stages", stage, "control"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let body = Self::collect_body(body).await?;
                let control = decode_request_control(&body).ok_or_else(|| {
                    ServerError::Config("expected encoded stage control marker".to_string())
                })?;
                self.service
                    .append_stage_control(stream_id, stage, control)
                    .await
                    .map_err(ServerError::Config)?;
                Ok(Self::text_response(StatusCode::OK, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "stages", stage, "end"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                self.service
                    .end_stage(stream_id, stage)
                    .await
                    .map_err(ServerError::Config)?;
                Ok(Self::text_response(StatusCode::OK, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "response", "headers"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let body = Self::collect_body(body).await?;
                let frame = decode_frame(&body)
                    .map_err(|e| ServerError::Config(format!("invalid HPKS headers frame: {e}")))?;
                match frame {
                    StreamFrame::Headers(StreamHeaders::Response(resp))
                        if resp.stream_id == stream_id =>
                    {
                        self.service
                            .write_response_headers(stream_id, StreamHeaders::Response(resp))
                            .await
                            .map_err(ServerError::Config)?;
                        Ok(Self::text_response(StatusCode::OK, "ok"))
                    }
                    StreamFrame::Headers(StreamHeaders::Response(_)) => Ok(Self::text_response(
                        StatusCode::BAD_REQUEST,
                        "stream id mismatch",
                    )),
                    _ => Ok(Self::text_response(
                        StatusCode::BAD_REQUEST,
                        "expected response headers frame",
                    )),
                }
            }
            ("PUT", ["_upload_response", "streams", stream_id, "response", "body"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let body = Self::collect_body(body).await?;
                self.service
                    .append_response_body(stream_id, body)
                    .await
                    .map_err(ServerError::Config)?;
                Ok(Self::text_response(StatusCode::OK, "ok"))
            }
            ("PUT", ["_upload_response", "streams", stream_id, "response", "end"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                self.service
                    .end_response(stream_id)
                    .await
                    .map_err(ServerError::Config)?;
                Ok(Self::text_response(StatusCode::OK, "ok"))
            }
            _ => Ok(Self::text_response(StatusCode::NOT_FOUND, "not found")),
        }
    }

    async fn await_response(
        &self,
        stream_id: u64,
        rx: oneshot::Receiver<ResponseResult>,
    ) -> HandlerResult<HandlerResponse> {
        let timeout_duration = Duration::from_millis(self.service.config.response_timeout_ms);
        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok(cached))) => {
                debug!(stream_id, status = ?cached.status, "Received response");
                let mut content_type = None;
                let mut headers = Vec::new();
                for (name, value) in cached.headers {
                    if name.eq_ignore_ascii_case("content-type") {
                        content_type = Some(value);
                    } else {
                        headers.push((name, value));
                    }
                }
                Ok(HandlerResponse {
                    status: cached.status,
                    body: Some(cached.body),
                    content_type,
                    headers,
                    etag: None,
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
        let stream = self
            .service
            .open_stream()
            .await
            .map_err(ServerError::Config)?;
        let stream_id = stream.stream_id();
        let rx = self.service.register_response(stream_id).await;
        debug!(stream_id, uri = %req.uri(), "Streaming request");

        let result = async {
            let headers = StreamHeaders::from_request(stream_id, &req)
                .map_err(|e| ServerError::Config(e.to_string()))?;
            self.service
                .write_request_headers(stream_id, headers)
                .await
                .map_err(ServerError::Config)?;

            if let Some(ref mut body_stream) = body {
                use futures_util::StreamExt;

                let slot_bytes = self.service.config.slot_bytes();

                while let Some(chunk) = body_stream.next().await {
                    let chunk = chunk?;
                    if chunk.is_empty() {
                        continue;
                    }

                    if chunk.len() <= slot_bytes {
                        self.service
                            .append_request_body(stream_id, chunk)
                            .await
                            .map_err(ServerError::Config)?;
                    } else {
                        let mut remaining = chunk;
                        while !remaining.is_empty() {
                            let take = remaining.len().min(slot_bytes);
                            let data = remaining.split_to(take);
                            self.service
                                .append_request_body(stream_id, data)
                                .await
                                .map_err(ServerError::Config)?;
                        }
                    }
                }
            }

            self.service
                .end_request(stream_id)
                .await
                .map_err(ServerError::Config)?;

            debug!(stream_id, "Request complete, waiting for response");
            self.await_response(stream_id, rx).await
        }
        .await;

        stream.close().await;
        result
    }
}

#[async_trait]
impl Router for UploadResponseRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if Self::is_internal_path(req.uri().path()) {
            return self.route_internal(req, None).await;
        }
        self.stream_request(req, None).await
    }

    async fn route_body(
        &self,
        req: Request<()>,
        body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        if Self::is_internal_path(req.uri().path()) {
            return self.route_internal(req, Some(body)).await;
        }
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
        let upload_stream = self
            .service
            .open_stream()
            .await
            .map_err(ServerError::Config)?;
        let stream_id = upload_stream.stream_id();
        let rx = self.service.register_response(stream_id).await;
        debug!(stream_id, uri = %req.uri(), "WebSocket stream started");

        let result = async {
            let headers = StreamHeaders::from_request(stream_id, &req)
                .map_err(|e| ServerError::Config(e.to_string()))?;
            self.service
                .write_request_headers(stream_id, headers)
                .await
                .map_err(ServerError::Config)?;

            let slot_bytes = self.service.config.slot_bytes();

            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(Message::Binary(data)) => {
                        if data.is_empty() {
                            break;
                        }
                        if data.len() <= slot_bytes {
                            self.service
                                .append_request_body(stream_id, Bytes::from(data))
                                .await
                                .map_err(ServerError::Config)?;
                        } else {
                            let mut remaining = Bytes::from(data);
                            while !remaining.is_empty() {
                                let take = remaining.len().min(slot_bytes);
                                let chunk = remaining.split_to(take);
                                self.service
                                    .append_request_body(stream_id, chunk)
                                    .await
                                    .map_err(ServerError::Config)?;
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => continue,
                    Err(e) => {
                        error!(stream_id, error = %e, "WebSocket error");
                        break;
                    }
                }
            }

            self.service
                .end_request(stream_id)
                .await
                .map_err(ServerError::Config)?;

            debug!(stream_id, "Request complete, waiting for response");
            let timeout_duration = Duration::from_millis(self.service.config.response_timeout_ms);
            match timeout(timeout_duration, rx).await {
                Ok(Ok(Ok(cached))) => {
                    debug!(stream_id, status = ?cached.status, "Sending WebSocket response");
                    if let Err(e) = stream
                        .send(Message::Binary(cached.body.to_vec().into()))
                        .await
                    {
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
        .await;

        upload_stream.close().await;
        result
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
    /// Request control marker embedded after the headers slot.
    Control(RequestControl),
    /// Final slot: End marker
    End,
}

/// Slot content for workers tailing a named intermediate stage stream.
#[derive(Debug, Clone)]
pub enum StageTailSlot {
    /// Slot 1: opaque stage head bytes
    Head(Bytes),
    /// Stage control marker embedded after the head slot.
    Control(RequestControl),
    /// Slots 2..N-1: opaque stage body bytes
    Body(Bytes),
    /// Final slot: end marker
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
        } else if let Some(control) = decode_request_control(&bytes) {
            Some(TailSlot::Control(control))
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

    /// Tail a named stage stream slot by slot.
    pub async fn tail_stage(
        &self,
        stream_id: u64,
        stage: &str,
        slot_id: usize,
    ) -> Option<StageTailSlot> {
        let bytes = self.stage_get(stream_id, stage, slot_id).await?;

        if slot_id == 1 {
            Some(StageTailSlot::Head(bytes))
        } else if Self::is_end_marker(&bytes) {
            Some(StageTailSlot::End)
        } else if let Some(control) = decode_request_control(&bytes) {
            Some(StageTailSlot::Control(control))
        } else {
            Some(StageTailSlot::Body(bytes))
        }
    }

    /// Return the opaque stage head from slot 1.
    pub async fn get_stage_head(&self, stream_id: u64, stage: &str) -> Option<Bytes> {
        self.stage_get(stream_id, stage, 1).await
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
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

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
        service
            .write_request_headers(stream_id, headers)
            .await
            .unwrap();

        // Slot 2: Raw body bytes
        service
            .append_request_body(stream_id, Bytes::from("hello"))
            .await
            .unwrap();

        // Slot 3: More raw body bytes
        service
            .append_request_body(stream_id, Bytes::from(" world"))
            .await
            .unwrap();

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

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_tail_request() {
        let config = UploadResponseConfig::default();
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

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
        service
            .write_request_headers(stream_id, headers)
            .await
            .unwrap();
        service
            .append_request_body(stream_id, Bytes::from("hello world"))
            .await
            .unwrap();
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

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_response_channel_roundtrip() {
        let config = UploadResponseConfig::default();
        let service = Arc::new(UploadResponseService::new(config));

        let stream_id = service.next_id();
        let rx = service.register_response(stream_id).await;

        service
            .complete_response(
                stream_id,
                Ok(CachedResponse {
                    status: StatusCode::OK,
                    body: Bytes::from("ok"),
                    headers: Vec::new(),
                }),
            )
            .await;

        let cached = rx.await.unwrap().unwrap();
        assert_eq!(cached.status, StatusCode::OK);
        assert_eq!(cached.body, Bytes::from("ok"));
    }

    #[tokio::test]
    async fn test_reader_registration() {
        let config = UploadResponseConfig {
            num_streams: 10,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

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

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_response_claim() {
        let config = UploadResponseConfig {
            num_streams: 10,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        // Initially unclaimed
        assert!(service.response_owner(stream_id).await.is_none());
        assert!(!service.is_response_claimed(stream_id).await);

        // Worker 1 claims successfully
        assert!(service.try_claim_response(stream_id, "writer-1").await);
        assert_eq!(
            service.response_owner(stream_id).await,
            Some("writer-1".to_string())
        );
        assert!(service.is_response_claimed_by(stream_id, "writer-1").await);
        assert!(!service.is_response_claimed_by(stream_id, "writer-2").await);

        // Worker 2 cannot claim (already claimed)
        assert!(!service.try_claim_response(stream_id, "writer-2").await);
        assert_eq!(
            service.response_owner(stream_id).await,
            Some("writer-1".to_string())
        );

        // Worker 2 cannot release (not owner)
        assert!(!service.release_response(stream_id, "writer-2").await);
        assert_eq!(
            service.response_owner(stream_id).await,
            Some("writer-1".to_string())
        );

        // Worker 1 releases successfully
        assert!(service.release_response(stream_id, "writer-1").await);
        assert!(service.response_owner(stream_id).await.is_none());

        // Now worker 2 can claim
        assert!(service.try_claim_response(stream_id, "writer-2").await);
        assert_eq!(
            service.response_owner(stream_id).await,
            Some("writer-2".to_string())
        );

        // Force release works regardless of owner
        service.force_release_response(stream_id).await;
        assert!(service.response_owner(stream_id).await.is_none());

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_concurrent_response_claim() {
        use std::sync::atomic::AtomicUsize;

        let config = UploadResponseConfig {
            num_streams: 10,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();
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

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_worker_heartbeat_summary() {
        let service = UploadResponseService::new(UploadResponseConfig::default());

        service
            .upsert_worker_heartbeat(
                "worker-a",
                WorkerHeartbeatUpdate {
                    stage: "processing".to_string(),
                    max_inflight: 4,
                    inflight: 1,
                    available_slots: 3,
                },
            )
            .await;
        service
            .upsert_worker_heartbeat(
                "worker-b",
                WorkerHeartbeatUpdate {
                    stage: "response".to_string(),
                    max_inflight: 2,
                    inflight: 2,
                    available_slots: 0,
                },
            )
            .await;

        let workers = service.list_workers(None).await;
        assert_eq!(workers.len(), 2);
        assert_eq!(workers[0].worker_id, "worker-a");
        assert_eq!(workers[0].stage, "processing");
        assert_eq!(workers[1].worker_id, "worker-b");
        assert_eq!(workers[1].stage, "response");

        let summary = service.worker_capacity_summary(None).await;
        assert_eq!(summary.workers, 2);
        assert_eq!(summary.total_max_inflight, 6);
        assert_eq!(summary.total_inflight, 3);
        assert_eq!(summary.total_available_slots, 3);
    }

    #[tokio::test]
    async fn test_worker_heartbeat_ttl_prunes_stale_workers() {
        let service = UploadResponseService::new(UploadResponseConfig::default());

        service
            .upsert_worker_heartbeat(
                "worker-a",
                WorkerHeartbeatUpdate {
                    stage: "processing".to_string(),
                    max_inflight: 2,
                    inflight: 0,
                    available_slots: 2,
                },
            )
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        let workers = service.list_workers(Some(1)).await;
        assert!(workers.is_empty());

        let summary = service.worker_capacity_summary(Some(1)).await;
        assert_eq!(summary.workers, 0);
        assert_eq!(summary.total_available_slots, 0);
    }
}
