use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, Response, StatusCode};
use http_pack::stream::{
    decode_frame, encode_frame, StreamFrame, StreamHeaders, StreamRequestHeaders,
    StreamResponseHeaders,
};
use hyper_util::rt::TokioIo;
use playlists::chunk_cache::ChunkCache;
use playlists::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tokio::sync::{
    oneshot, Mutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, RwLock, Semaphore,
    TryAcquireError,
};
use tokio::time::{timeout, timeout_at, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, error, warn};
use web_service::{
    BodyStream, HandlerResponse, HandlerResult, Router, ServerError, StreamWriter,
    VerifiedClientCertificate, WebSocketHandler, WebTransportHandler,
};

mod watcher;
pub use watcher::{ResponseWatcher, ResponseWatcherHandle};

mod bridge;
pub use bridge::{
    build_streaming_response_head, clone_request_head, handler_response_from_cached,
    proxy_streaming_response_with, request_from_headers_slot, request_from_stream_headers,
    response_content_type, CachedIngress, CachedRequestGuard, IngressProxyConfig,
};

mod remote;
pub use remote::{
    discover_ingress_origins, RemoteIngressClient, RemoteRequestSlot, RemoteStageSlot,
    RemoteStreamInfo,
};

mod response_writer;
pub use response_writer::ResponseCacheWriter;

pub(crate) const RESPONSE_WATCHER_READER_ID: &str = "__upload_response_watcher";

#[cfg(feature = "srt")]
mod srt;
#[cfg(feature = "srt")]
pub use srt::{AllowAll, AllowAllEncrypted, SrtAuth, SrtIngest};

#[cfg(feature = "rist")]
mod rist;
#[cfg(feature = "rist")]
pub use rist::{AllowAllRist, RistAuth, RistIngest, RistProfile};

#[cfg(feature = "rist-pure")]
mod pure_rist;
#[cfg(feature = "rist-pure")]
pub use pure_rist::{
    AllowAllPureRist, PureRistAuth, PureRistIngest, PureRistIngestMetrics, PureRistIngestStats,
    PureRistProfile,
};

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
    SequenceStats, UdpFecIngest, UdpFecSender, DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, HEADER_LEN,
};

// For RTMP support, use rtmp-ingress with the "upload-response" feature:
// rtmp-ingress = { ..., features = ["upload-response"] }
// use rtmp_ingress::upload::{RtmpUploadIngest, RtmpAuth, AllowAll};

/// End-of-stream marker - empty slot
const END_MARKER: &[u8] = b"";
const REQUEST_CONTROL_MAGIC: &[u8; 8] = b"URCTRL1\0";
const MAX_INTERNAL_BODY_BYTES: usize = 64 * 1024;
const MAX_READER_ID_BYTES: usize = 128;
const MAX_READERS_PER_STREAM: usize = 256;
const MAX_STAGE_NAME_BYTES: usize = 64;
const MAX_STAGE_LANES: usize = 16;
const MAX_WORKER_HEARTBEATS: usize = 4_096;
const RESPONSE_CAPABILITY_BYTES: usize = 32;
const EAGER_CACHE_LANES: usize = 2;
const ESTIMATED_CACHE_SLOT_METADATA_BYTES: u128 = 128;
const ESTIMATED_CACHE_STREAM_METADATA_BYTES: u128 = 512;
const ESTIMATED_SERVICE_STREAM_METADATA_BYTES: u128 = 4 * 1024;
pub const MAX_UPLOAD_RESPONSE_STREAMS: usize = 65_536;
pub const MAX_UPLOAD_RESPONSE_SLOTS_PER_STREAM: usize = 1_048_576;
pub const MAX_UPLOAD_RESPONSE_SLOT_BYTES: u128 = 64 * 1024 * 1024;
pub const MAX_UPLOAD_RESPONSE_LOGICAL_BYTES: u128 = 1024 * 1024 * 1024 * 1024;
pub const MAX_UPLOAD_RESPONSE_ESTIMATED_METADATA_BYTES: u128 = 512 * 1024 * 1024;
pub const RESPONSE_CAPABILITY_HEADER: &str = "x-upload-response-capability";
pub const RESPONSE_SEQUENCE_HEADER: &str = "x-upload-response-sequence";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseWriteKind {
    Headers,
    Body,
    End,
}

impl ResponseWriteKind {
    fn digest(self, data: &[u8]) -> [u8; 32] {
        let tag = match self {
            Self::Headers => b"headers".as_slice(),
            Self::Body => b"body".as_slice(),
            Self::End => b"end".as_slice(),
        };
        let mut digest = Sha256::new();
        digest.update(tag);
        digest.update(data);
        digest.finalize().into()
    }
}

#[derive(Debug, Clone, Copy)]
struct ResponseWriteRecord {
    sequence: u64,
    digest: [u8; 32],
}

#[derive(Debug, Clone)]
struct ResponseClaim {
    worker_id: String,
    capability_digest: [u8; 32],
    expires_at: Instant,
    next_sequence: u64,
    last_write: Option<ResponseWriteRecord>,
}

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
    /// Legacy response and backpressure timeout retained for migration.
    pub response_timeout_ms: u64,
}

/// Independent deadlines for upload-response work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadResponseTimeouts {
    /// Maximum wait after a request finishes before its response must complete.
    /// Applies to buffered delivery, where the whole body arrives at once.
    pub response_deadline_ms: u64,
    /// Maximum wait for the *next* slot of a streaming response.
    ///
    /// Streaming responses are bounded by idle time rather than total
    /// duration: a worker that keeps producing may run indefinitely, while one
    /// that stalls fails after this long. A total deadline would cap how long a
    /// response may take to generate, which is wrong for incremental output.
    pub response_idle_timeout_ms: u64,
    /// Maximum wait before a writer may overwrite a cache ring slot.
    pub reader_backpressure_timeout_ms: u64,
    /// Maximum wait for a protocol adapter to acquire a stream slot.
    pub stream_admission_timeout_ms: u64,
    /// Maximum duration for one remote worker control request.
    pub remote_io_timeout_ms: u64,
}

impl UploadResponseTimeouts {
    /// Map the legacy response timeout into the migration policy.
    pub const fn from_legacy_response_timeout(response_timeout_ms: u64) -> Self {
        Self {
            response_deadline_ms: response_timeout_ms,
            response_idle_timeout_ms: response_timeout_ms,
            reader_backpressure_timeout_ms: response_timeout_ms,
            stream_admission_timeout_ms: response_timeout_ms,
            remote_io_timeout_ms: 60_000,
        }
    }
}

impl Default for UploadResponseTimeouts {
    fn default() -> Self {
        Self::from_legacy_response_timeout(30_000)
    }
}

/// Worst-case capacity implied by an upload-response configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadResponseCapacity {
    pub streams: usize,
    pub slots_per_stream: usize,
    pub slot_bytes: u128,
    pub cache_lanes: usize,
    pub logical_maximum_bytes: u128,
    pub estimated_metadata_bytes: u128,
    pub eager_estimated_metadata_bytes: u128,
}

/// Configuration failure reported before cache allocation begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadResponseConfigError {
    pub reason: String,
    pub logical_maximum_bytes: u128,
    pub estimated_metadata_bytes: u128,
}

impl UploadResponseConfigError {
    fn new(reason: impl Into<String>, capacity: UploadResponseCapacity) -> Self {
        Self {
            reason: reason.into(),
            logical_maximum_bytes: capacity.logical_maximum_bytes,
            estimated_metadata_bytes: capacity.estimated_metadata_bytes,
        }
    }
}

impl fmt::Display for UploadResponseConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; logical maximum: {} bytes; estimated metadata: {} bytes",
            self.reason, self.logical_maximum_bytes, self.estimated_metadata_bytes
        )
    }
}

impl std::error::Error for UploadResponseConfigError {}

#[derive(Default)]
struct CapacityArithmetic {
    overflowed: bool,
}

impl CapacityArithmetic {
    fn multiply(&mut self, left: u128, right: u128) -> u128 {
        left.checked_mul(right).unwrap_or_else(|| {
            self.overflowed = true;
            u128::MAX
        })
    }

    fn add(&mut self, left: u128, right: u128) -> u128 {
        left.checked_add(right).unwrap_or_else(|| {
            self.overflowed = true;
            u128::MAX
        })
    }
}

impl UploadResponseConfig {
    /// Slot capacity in bytes
    pub fn slot_bytes(&self) -> usize {
        self.slot_size_kb.saturating_mul(1024)
    }

    /// Derive the migration timeout policy from `response_timeout_ms`.
    pub const fn legacy_timeouts(&self) -> UploadResponseTimeouts {
        UploadResponseTimeouts::from_legacy_response_timeout(self.response_timeout_ms)
    }

    fn normalize(&mut self) {
        self.num_streams = self.num_streams.max(1);
        self.slot_size_kb = self.slot_size_kb.max(1);
        self.slots_per_stream = self.slots_per_stream.max(1);
    }

    fn capacity_estimate(&self) -> (UploadResponseCapacity, bool) {
        let mut arithmetic = CapacityArithmetic::default();
        let streams = self.num_streams as u128;
        let slots_per_stream = self.slots_per_stream as u128;
        let slot_bytes = arithmetic.multiply(self.slot_size_kb as u128, 1024);
        let cache_lanes = EAGER_CACHE_LANES + MAX_STAGE_LANES;
        let cache_lanes_u128 = cache_lanes as u128;
        let slots_per_cache = arithmetic.multiply(streams, slots_per_stream);
        let total_cache_slots = arithmetic.multiply(slots_per_cache, cache_lanes_u128);
        let total_cache_streams = arithmetic.multiply(streams, cache_lanes_u128);
        let initialization_kb = arithmetic.multiply(Options::default().init_size_kb as u128, 1024);
        let initialization_bytes = arithmetic.multiply(initialization_kb, total_cache_streams);
        let maximum_slot_bytes = arithmetic.multiply(total_cache_slots, slot_bytes);
        let logical_maximum_bytes = arithmetic.add(maximum_slot_bytes, initialization_bytes);
        let estimated_service_metadata =
            arithmetic.multiply(streams, ESTIMATED_SERVICE_STREAM_METADATA_BYTES);
        let estimated_slot_metadata =
            arithmetic.multiply(total_cache_slots, ESTIMATED_CACHE_SLOT_METADATA_BYTES);
        let estimated_stream_metadata =
            arithmetic.multiply(total_cache_streams, ESTIMATED_CACHE_STREAM_METADATA_BYTES);
        let estimated_cache_metadata =
            arithmetic.add(estimated_slot_metadata, estimated_stream_metadata);
        let estimated_metadata_bytes =
            arithmetic.add(estimated_cache_metadata, estimated_service_metadata);
        let eager_cache_slots = arithmetic.multiply(slots_per_cache, EAGER_CACHE_LANES as u128);
        let eager_cache_streams = arithmetic.multiply(streams, EAGER_CACHE_LANES as u128);
        let eager_slot_metadata =
            arithmetic.multiply(eager_cache_slots, ESTIMATED_CACHE_SLOT_METADATA_BYTES);
        let eager_stream_metadata =
            arithmetic.multiply(eager_cache_streams, ESTIMATED_CACHE_STREAM_METADATA_BYTES);
        let eager_cache_metadata = arithmetic.add(eager_slot_metadata, eager_stream_metadata);
        let eager_estimated_metadata_bytes =
            arithmetic.add(eager_cache_metadata, estimated_service_metadata);

        (
            UploadResponseCapacity {
                streams: self.num_streams,
                slots_per_stream: self.slots_per_stream,
                slot_bytes,
                cache_lanes,
                logical_maximum_bytes,
                estimated_metadata_bytes,
                eager_estimated_metadata_bytes,
            },
            arithmetic.overflowed,
        )
    }

    /// Validate capacity before the service allocates cache metadata.
    pub fn validate(&self) -> Result<UploadResponseCapacity, UploadResponseConfigError> {
        let mut config = self.clone();
        config.normalize();
        let (capacity, arithmetic_overflowed) = config.capacity_estimate();
        if config.num_streams > MAX_UPLOAD_RESPONSE_STREAMS {
            return Err(UploadResponseConfigError::new(
                format!(
                    "num_streams {} exceeds limit {MAX_UPLOAD_RESPONSE_STREAMS}",
                    config.num_streams
                ),
                capacity,
            ));
        }
        if config.slots_per_stream > MAX_UPLOAD_RESPONSE_SLOTS_PER_STREAM {
            return Err(UploadResponseConfigError::new(
                format!(
                    "slots_per_stream {} exceeds limit {MAX_UPLOAD_RESPONSE_SLOTS_PER_STREAM}",
                    config.slots_per_stream
                ),
                capacity,
            ));
        }
        if arithmetic_overflowed {
            return Err(UploadResponseConfigError::new(
                "capacity arithmetic overflow",
                capacity,
            ));
        }
        if capacity.slot_bytes > MAX_UPLOAD_RESPONSE_SLOT_BYTES {
            return Err(UploadResponseConfigError::new(
                format!(
                    "slot size {} bytes exceeds limit {MAX_UPLOAD_RESPONSE_SLOT_BYTES}",
                    capacity.slot_bytes
                ),
                capacity,
            ));
        }
        if capacity.estimated_metadata_bytes > MAX_UPLOAD_RESPONSE_ESTIMATED_METADATA_BYTES {
            return Err(UploadResponseConfigError::new(
                format!(
                    "estimated metadata {} bytes exceeds limit {MAX_UPLOAD_RESPONSE_ESTIMATED_METADATA_BYTES}",
                    capacity.estimated_metadata_bytes
                ),
                capacity,
            ));
        }
        if capacity.logical_maximum_bytes > MAX_UPLOAD_RESPONSE_LOGICAL_BYTES {
            return Err(UploadResponseConfigError::new(
                format!(
                    "logical maximum {} bytes exceeds limit {MAX_UPLOAD_RESPONSE_LOGICAL_BYTES}",
                    capacity.logical_maximum_bytes
                ),
                capacity,
            ));
        }
        Ok(capacity)
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
    Options {
        num_playlists: config.num_streams,
        max_segments: 1,
        max_parts_per_segment: config.slots_per_stream,
        buffer_size_kb: config.slot_size_kb,
        ..Options::default()
    }
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
    reader_positions: Arc<RwLock<Vec<HashMap<String, usize>>>>,
    reader_notifies: Vec<Arc<Notify>>,
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
            reader_positions: new_reader_positions(config.num_streams),
            reader_notifies: new_reader_notifies(config.num_streams),
        }
    }

    fn last(&self, stream_idx: usize) -> usize {
        if !self.started[stream_idx].load(Ordering::SeqCst) {
            return 0;
        }
        self.cache.last(stream_idx).unwrap_or(0)
    }

    async fn clear_slot_state(&self, stream_idx: usize) {
        self.cache.reset_stream_idx(stream_idx);
        self.started[stream_idx].store(false, Ordering::SeqCst);
        {
            let mut claims = self.claims.write().await;
            claims[stream_idx] = None;
        }
        {
            let mut positions = self.reader_positions.write().await;
            positions[stream_idx].clear();
        }
        self.reader_notifies[stream_idx].notify_waiters();
    }
}

fn new_reader_positions(num_streams: usize) -> Arc<RwLock<Vec<HashMap<String, usize>>>> {
    Arc::new(RwLock::new(
        (0..num_streams).map(|_| HashMap::new()).collect(),
    ))
}

fn new_reader_notifies(num_streams: usize) -> Vec<Arc<Notify>> {
    (0..num_streams).map(|_| Arc::new(Notify::new())).collect()
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
            match tokio::runtime::Builder::new_current_thread().build() {
                Ok(runtime) => runtime.block_on(service.close_stream(stream_id)),
                Err(error) => {
                    error!(stream_id, %error, "failed to create stream cleanup runtime");
                }
            }
            drop(permit);
        }
    }
}

enum UploadLane {
    Request,
    Response,
    Stage { name: String, lane: Arc<StageLane> },
}

/// Generation-fenced read access to one active upload cache lane.
pub struct UploadLaneHandle {
    service: Arc<UploadResponseService>,
    stream_id: u64,
    stream_idx: usize,
    lane: UploadLane,
}

impl UploadLaneHandle {
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub fn lane_name(&self) -> &str {
        match &self.lane {
            UploadLane::Request => "request",
            UploadLane::Response => "response",
            UploadLane::Stage { name, .. } => name,
        }
    }

    pub fn is_current(&self) -> bool {
        self.service
            .is_current_slot(self.stream_id, self.stream_idx)
    }

    fn cache(&self) -> &ChunkCache {
        match &self.lane {
            UploadLane::Request => &self.service.request_cache,
            UploadLane::Response => &self.service.response_cache,
            UploadLane::Stage { lane, .. } => &lane.cache,
        }
    }

    /// Return the last published slot while this handle remains current.
    pub fn last(&self) -> Option<usize> {
        if !self.is_current() {
            return None;
        }
        let last = match &self.lane {
            UploadLane::Request => {
                if self.service.request_started[self.stream_idx].load(Ordering::Acquire) {
                    self.service
                        .request_cache
                        .last(self.stream_idx)
                        .unwrap_or(0)
                } else {
                    0
                }
            }
            UploadLane::Response => {
                if self.service.response_started[self.stream_idx].load(Ordering::Acquire) {
                    self.service
                        .response_cache
                        .last(self.stream_idx)
                        .unwrap_or(0)
                } else {
                    0
                }
            }
            UploadLane::Stage { lane, .. } => lane.last(self.stream_idx),
        };
        self.is_current().then_some(last)
    }

    /// Read one slot and reject data from a reused physical stream slot.
    pub async fn get(&self, slot_id: usize) -> Option<Bytes> {
        self.get_with_hash(slot_id).await.map(|(bytes, _)| bytes)
    }

    /// Read one slot and its hash while fencing physical slot reuse.
    pub async fn get_with_hash(&self, slot_id: usize) -> Option<(Bytes, u64)> {
        if !self.is_current() {
            return None;
        }
        let value = self.cache().get(self.stream_idx, slot_id).await;
        self.is_current().then_some(value).flatten()
    }

    /// Return the lane notifier while this handle remains current.
    ///
    /// Recheck the handle after every notification because stream closure also wakes waiters.
    pub fn update_notifier(&self) -> Option<Arc<Notify>> {
        if !self.is_current() {
            return None;
        }
        let notifier = self.cache().update_notifier(self.stream_idx)?;
        self.is_current().then_some(notifier)
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
    slot_locks: Vec<Arc<Mutex<()>>>,
    response_channels: Arc<RwLock<HashMap<u64, oneshot::Sender<ResponseResult>>>>,
    /// Per-stream worker count: how many workers are currently reading/processing
    stream_worker_counts: Vec<AtomicU64>,
    request_started: Vec<AtomicBool>,
    response_started: Vec<AtomicBool>,
    /// Per-stream worker sets: which worker IDs are reading/processing each stream
    stream_workers: Arc<RwLock<Vec<std::collections::HashSet<String>>>>,
    request_reader_positions: Arc<RwLock<Vec<HashMap<String, usize>>>>,
    request_reader_notifies: Vec<Arc<Notify>>,
    response_reader_positions: Arc<RwLock<Vec<HashMap<String, usize>>>>,
    response_reader_notifies: Vec<Arc<Notify>>,
    /// Per-stream response claim and write capability.
    response_claims: Vec<Mutex<Option<ResponseClaim>>>,
    response_dirty: Vec<AtomicBool>,
    response_updates: Arc<Notify>,
    /// Worker heartbeat/capacity registry keyed by worker id.
    workers: Arc<RwLock<HashMap<String, WorkerHeartbeat>>>,
    config: UploadResponseConfig,
    timeouts: UploadResponseTimeouts,
}

impl UploadResponseService {
    /// Create a new upload-response service with the given configuration
    pub fn new(config: UploadResponseConfig) -> Self {
        Self::try_new(config)
            .unwrap_or_else(|error| panic!("invalid upload-response config: {error}"))
    }

    /// Create a service with independent timeout purposes.
    pub fn new_with_timeouts(
        config: UploadResponseConfig,
        timeouts: UploadResponseTimeouts,
    ) -> Self {
        Self::try_new_with_timeouts(config, timeouts)
            .unwrap_or_else(|error| panic!("invalid upload-response config: {error}"))
    }

    /// Validate capacity before allocation and create a service.
    pub fn try_new(config: UploadResponseConfig) -> Result<Self, UploadResponseConfigError> {
        let timeouts = config.legacy_timeouts();
        Self::try_new_with_timeouts(config, timeouts)
    }

    /// Validate capacity and create a service with independent timeouts.
    pub fn try_new_with_timeouts(
        mut config: UploadResponseConfig,
        timeouts: UploadResponseTimeouts,
    ) -> Result<Self, UploadResponseConfigError> {
        config.normalize();
        let capacity = config.validate()?;
        let options = chunk_cache_options(&config);
        let request_cache = Arc::new(ChunkCache::try_new(options).map_err(|error| {
            UploadResponseConfigError::new(format!("invalid request cache: {error}"), capacity)
        })?);
        let response_cache = Arc::new(ChunkCache::try_new(chunk_cache_options(&config)).map_err(
            |error| {
                UploadResponseConfigError::new(format!("invalid response cache: {error}"), capacity)
            },
        )?);
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
        let response_claims = (0..config.num_streams).map(|_| Mutex::new(None)).collect();

        Ok(Self {
            request_cache,
            response_cache,
            stages: Arc::new(RwLock::new(HashMap::new())),
            slot_semaphore,
            next_stream_id: AtomicU64::new(1),
            stream_to_slot: StdRwLock::new(HashMap::new()),
            free_slots: StdMutex::new(free_slots),
            slot_stream_ids: (0..config.num_streams).map(|_| AtomicU64::new(0)).collect(),
            slot_locks: (0..config.num_streams)
                .map(|_| Arc::new(Mutex::new(())))
                .collect(),
            response_channels: Arc::new(RwLock::new(HashMap::new())),
            stream_worker_counts,
            request_started,
            response_started,
            stream_workers: Arc::new(RwLock::new(stream_workers)),
            request_reader_positions: new_reader_positions(config.num_streams),
            request_reader_notifies: new_reader_notifies(config.num_streams),
            response_reader_positions: new_reader_positions(config.num_streams),
            response_reader_notifies: new_reader_notifies(config.num_streams),
            response_claims,
            response_dirty: (0..config.num_streams)
                .map(|_| AtomicBool::new(false))
                .collect(),
            response_updates: Arc::new(Notify::new()),
            workers: Arc::new(RwLock::new(HashMap::new())),
            config,
            timeouts,
        })
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

    fn is_current_slot(&self, stream_id: u64, stream_idx: usize) -> bool {
        self.slot_stream_ids
            .get(stream_idx)
            .is_some_and(|current| current.load(Ordering::Acquire) == stream_id)
            && self.stream_idx(stream_id) == Some(stream_idx)
    }

    fn notify_response_update(&self, stream_idx: usize) {
        if let Some(dirty) = self.response_dirty.get(stream_idx) {
            dirty.store(true, Ordering::Release);
            self.response_updates.notify_one();
        }
    }

    pub(crate) fn take_response_dirty_slots(&self) -> Vec<usize> {
        self.response_dirty
            .iter()
            .enumerate()
            .filter_map(|(stream_idx, dirty)| {
                dirty.swap(false, Ordering::AcqRel).then_some(stream_idx)
            })
            .collect()
    }

    async fn lock_stream_slot(
        &self,
        stream_id: u64,
    ) -> Result<(usize, OwnedMutexGuard<()>), String> {
        let stream_idx = self
            .stream_idx(stream_id)
            .ok_or_else(|| format!("unknown stream: {stream_id}"))?;
        let slot_lock = self
            .slot_locks
            .get(stream_idx)
            .cloned()
            .ok_or_else(|| format!("invalid stream slot: {stream_idx}"))?;
        let guard = slot_lock.lock_owned().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return Err(format!("stream closed: {stream_id}"));
        }
        Ok((stream_idx, guard))
    }

    pub fn slot_stream_id(&self, stream_idx: usize) -> Option<u64> {
        let stream_id = self
            .slot_stream_ids
            .get(stream_idx)?
            .load(Ordering::Acquire);
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

    fn valid_reader_id(reader_id: &str) -> bool {
        !reader_id.is_empty() && reader_id.len() <= MAX_READER_ID_BYTES
    }

    fn valid_stage_name(stage: &str) -> bool {
        !stage.is_empty()
            && stage.len() <= MAX_STAGE_NAME_BYTES
            && stage
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }

    async fn get_or_create_stage_lane(&self, stage: &str) -> Result<Arc<StageLane>, String> {
        if !Self::valid_stage_name(stage) {
            return Err("invalid stage name".to_string());
        }
        {
            let stages = self.stages.read().await;
            if let Some(lane) = stages.get(stage) {
                return Ok(Arc::clone(lane));
            }
        }

        let mut stages = self.stages.write().await;
        if let Some(lane) = stages.get(stage) {
            return Ok(Arc::clone(lane));
        }
        if stages.len() >= MAX_STAGE_LANES {
            return Err(format!("stage lane limit reached: {MAX_STAGE_LANES}"));
        }
        let lane = Arc::new(StageLane::new(&self.config));
        stages.insert(stage.to_string(), Arc::clone(&lane));
        Ok(lane)
    }

    async fn get_stage_lane(&self, stage: &str) -> Option<Arc<StageLane>> {
        let stages = self.stages.read().await;
        stages.get(stage).cloned()
    }

    async fn wait_for_reader_capacity(
        &self,
        stream_id: u64,
        stream_idx: usize,
        next_slot: usize,
        positions: &Arc<RwLock<Vec<HashMap<String, usize>>>>,
        notifies: &[Arc<Notify>],
        lane: &str,
    ) -> Result<(), String> {
        let capacity = self.config.slots_per_stream.max(1);
        if next_slot <= capacity {
            return Ok(());
        }

        let overwrite_slot = next_slot - capacity;
        let deadline =
            Instant::now() + Duration::from_millis(self.timeouts.reader_backpressure_timeout_ms);
        loop {
            let notified = notifies[stream_idx].notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.slot_stream_ids[stream_idx].load(Ordering::Acquire) != stream_id {
                return Err(format!("{lane} stream closed: {stream_id}"));
            }

            let (can_append, reader_count, min_consumed) = {
                let positions = positions.read().await;
                let readers = &positions[stream_idx];
                let min_consumed = readers.values().copied().min();
                (
                    min_consumed.is_some_and(|min_consumed| min_consumed >= overwrite_slot),
                    readers.len(),
                    min_consumed,
                )
            };
            if can_append {
                return Ok(());
            }

            debug!(
                stream_id,
                stream_idx,
                lane,
                next_slot,
                overwrite_slot,
                reader_count,
                min_consumed = min_consumed.unwrap_or(0),
                "waiting for upload-response reader capacity"
            );
            timeout_at(deadline, notified).await.map_err(|_| {
                format!(
                    "{lane} buffer capacity wait timed out for stream {stream_id} before slot {next_slot}"
                )
            })?;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn append_cache_with_backpressure(
        &self,
        stream_id: u64,
        stream_idx: usize,
        cache: &ChunkCache,
        positions: &Arc<RwLock<Vec<HashMap<String, usize>>>>,
        notifies: &[Arc<Notify>],
        lane: &str,
        data: Bytes,
    ) -> Result<(), String> {
        let next_slot = cache
            .last(stream_idx)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| format!("{lane} slot index overflow for stream {stream_id}"))?;
        let bytes = data.len();
        self.wait_for_reader_capacity(stream_id, stream_idx, next_slot, positions, notifies, lane)
            .await?;
        if !self.is_current_slot(stream_id, stream_idx) {
            return Err(format!("{lane} stream closed: {stream_id}"));
        }
        cache
            .add(stream_idx, next_slot, data)
            .await
            .map_err(|e| e.to_string())?;
        if !self.is_current_slot(stream_id, stream_idx) {
            return Err(format!("{lane} stream closed: {stream_id}"));
        }
        debug!(
            stream_id,
            stream_idx,
            lane,
            slot_id = next_slot,
            bytes,
            "upload-response slot written"
        );
        Ok(())
    }

    async fn register_stream_reader_at(
        &self,
        stream_idx: usize,
        stream_id: u64,
        worker_id: &str,
    ) -> Option<bool> {
        if !Self::valid_reader_id(worker_id) {
            return None;
        }
        let mut workers = self.stream_workers.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return None;
        }
        if !workers[stream_idx].contains(worker_id)
            && workers[stream_idx].len() >= MAX_READERS_PER_STREAM
        {
            return None;
        }
        let inserted = workers[stream_idx].insert(worker_id.to_string());
        if inserted {
            self.stream_worker_counts[stream_idx].fetch_add(1, Ordering::Relaxed);
        }
        debug!(stream_id, worker_id, "Reader registered");
        Some(inserted)
    }

    async fn register_lane_reader(
        &self,
        stream_id: u64,
        stream_idx: usize,
        worker_id: &str,
        positions: &Arc<RwLock<Vec<HashMap<String, usize>>>>,
        notifies: &[Arc<Notify>],
        lane: &str,
    ) -> bool {
        let mut positions = positions.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return false;
        }
        positions[stream_idx]
            .entry(worker_id.to_string())
            .or_insert(0);
        debug!(stream_id, worker_id, lane, "Reader registered");
        notifies[stream_idx].notify_waiters();
        true
    }

    #[allow(clippy::too_many_arguments)]
    async fn mark_lane_reader_position_at(
        &self,
        stream_idx: usize,
        stream_id: u64,
        worker_id: &str,
        slot_id: usize,
        positions: &Arc<RwLock<Vec<HashMap<String, usize>>>>,
        notifies: &[Arc<Notify>],
        lane: &str,
    ) -> bool {
        let advanced = {
            let mut positions = positions.write().await;
            if !self.is_current_slot(stream_id, stream_idx) {
                return false;
            }
            let Some(position) = positions[stream_idx].get_mut(worker_id) else {
                return false;
            };
            if slot_id > *position {
                *position = slot_id;
                true
            } else {
                false
            }
        };
        if advanced {
            debug!(stream_id, worker_id, slot_id, lane, "Reader advanced");
            notifies[stream_idx].notify_waiters();
        }
        true
    }

    pub async fn stage_names(&self) -> Vec<String> {
        let stages = self.stages.read().await;
        let mut names: Vec<_> = stages.keys().cloned().collect();
        names.sort();
        names
    }

    pub async fn active_streams(&self) -> Vec<ActiveStreamInfo> {
        let stage_lanes: Vec<(String, Arc<StageLane>)> = {
            let stages = self.stages.read().await;
            stages
                .iter()
                .map(|(name, lane)| (name.clone(), Arc::clone(lane)))
                .collect()
        };
        let mut active = Vec::new();

        for slot in self.active_stream_slots() {
            let mut stages = BTreeMap::new();
            for (stage_name, lane) in &stage_lanes {
                let owner = {
                    let stage_claims = lane.claims.read().await;
                    stage_claims[slot.stream_idx].clone()
                };
                let last = lane.last(slot.stream_idx);
                if last > 0 || owner.is_some() {
                    stages.insert(stage_name.clone(), StageState { last, owner });
                }
            }

            if !self.is_current_slot(slot.stream_id, slot.stream_idx) {
                continue;
            }

            let response_owner = self.response_claims[slot.stream_idx]
                .lock()
                .await
                .as_ref()
                .filter(|claim| claim.expires_at > Instant::now())
                .map(|claim| claim.worker_id.clone());
            if !self.is_current_slot(slot.stream_id, slot.stream_idx) {
                continue;
            }

            active.push(ActiveStreamInfo {
                stream_id: slot.stream_id,
                stream_idx: slot.stream_idx,
                request_last: self.request_cache.last(slot.stream_idx).unwrap_or(0),
                response_last: self.response_cache.last(slot.stream_idx).unwrap_or(0),
                reader_count: self.stream_worker_counts[slot.stream_idx].load(Ordering::SeqCst),
                response_owner,
                stages,
            });
        }

        active
    }

    async fn clear_slot_state(&self, stream_idx: usize) {
        self.request_cache.reset_stream_idx(stream_idx);
        self.response_cache.reset_stream_idx(stream_idx);
        {
            let mut workers = self.stream_workers.write().await;
            workers[stream_idx].clear();
        }
        {
            let mut positions = self.request_reader_positions.write().await;
            positions[stream_idx].clear();
        }
        self.request_reader_notifies[stream_idx].notify_waiters();
        {
            let mut positions = self.response_reader_positions.write().await;
            positions[stream_idx].clear();
        }
        self.response_reader_notifies[stream_idx].notify_waiters();
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
            *self.response_claims[stream_idx].lock().await = None;
        }
        self.notify_response_update(stream_idx);
    }

    async fn notify_slot_waiters(&self, stream_idx: usize) {
        if let Some(notify) = self.request_reader_notifies.get(stream_idx) {
            notify.notify_waiters();
        }
        if let Some(notify) = self.response_reader_notifies.get(stream_idx) {
            notify.notify_waiters();
        }
        self.notify_response_update(stream_idx);
        let lanes: Vec<_> = {
            let stages = self.stages.read().await;
            stages.values().cloned().collect()
        };
        for lane in lanes {
            if let Some(notify) = lane.reader_notifies.get(stream_idx) {
                notify.notify_waiters();
            }
        }
    }

    fn allocate_stream_id(&self) -> u64 {
        loop {
            let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
            if stream_id != 0 && self.stream_idx(stream_id).is_none() {
                return stream_id;
            }
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
        self.notify_slot_waiters(stream_idx).await;
        let slot_guard = Arc::clone(&self.slot_locks[stream_idx]).lock_owned().await;
        self.clear_slot_state(stream_idx).await;
        drop(slot_guard);
        self.drop_response_channel(stream_id).await;
        self.release_slot(stream_idx);
        debug!(stream_id, stream_idx, "Stream closed");
    }

    pub async fn open_stream(self: &Arc<Self>) -> Result<UploadStream, String> {
        let permit = self.acquire_stream().await?;

        self.open_stream_with_permit(permit).await
    }

    /// Open a stream without waiting for another stream to release capacity.
    pub async fn try_open_stream(self: &Arc<Self>) -> Result<UploadStream, String> {
        let permit = self
            .slot_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| "no stream capacity available".to_string())?;

        self.open_stream_with_permit(permit).await
    }

    async fn open_stream_with_permit(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
    ) -> Result<UploadStream, String> {
        let stream_idx = self
            .allocate_slot()
            .ok_or_else(|| "no free slot available".to_string())?;
        let slot_guard = Arc::clone(&self.slot_locks[stream_idx]).lock_owned().await;
        let stream_id = self.allocate_stream_id();

        self.clear_slot_state(stream_idx).await;

        let registry_result = self.stream_to_slot.write().map(|mut stream_to_slot| {
            stream_to_slot.insert(stream_id, stream_idx);
        });
        if registry_result.is_err() {
            drop(slot_guard);
            self.release_slot(stream_idx);
            drop(permit);
            return Err("stream slot registry poisoned".to_string());
        }
        self.slot_stream_ids[stream_idx].store(stream_id, Ordering::Release);
        drop(slot_guard);

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
        self.register_stream_reader_at(stream_idx, stream_id, worker_id)
            .await
            .unwrap_or(false)
    }

    /// Register a worker as a request-stream reader.
    ///
    /// Request readers drive backpressure for request body producers. Slot `1`
    /// remains protected until the worker marks it consumed.
    pub async fn register_request_reader(&self, stream_id: u64, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let Some(inserted) = self
            .register_stream_reader_at(stream_idx, stream_id, worker_id)
            .await
        else {
            return false;
        };
        if !self
            .register_lane_reader(
                stream_id,
                stream_idx,
                worker_id,
                &self.request_reader_positions,
                &self.request_reader_notifies,
                "request",
            )
            .await
        {
            return false;
        }
        inserted
    }

    /// Register a worker as a reader of a named stage stream.
    pub async fn register_stage_reader(
        &self,
        stream_id: u64,
        stage: &str,
        worker_id: &str,
    ) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let Ok(lane) = self.get_or_create_stage_lane(stage).await else {
            return false;
        };
        let Some(inserted) = self
            .register_stream_reader_at(stream_idx, stream_id, worker_id)
            .await
        else {
            return false;
        };
        if !self
            .register_lane_reader(
                stream_id,
                stream_idx,
                worker_id,
                &lane.reader_positions,
                &lane.reader_notifies,
                stage,
            )
            .await
        {
            return false;
        }
        inserted
    }

    /// Mark a request-stream slot as consumed by a request reader.
    pub async fn mark_request_reader_position(
        &self,
        stream_id: u64,
        worker_id: &str,
        slot_id: usize,
    ) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        self.mark_lane_reader_position_at(
            stream_idx,
            stream_id,
            worker_id,
            slot_id,
            &self.request_reader_positions,
            &self.request_reader_notifies,
            "request",
        )
        .await
    }

    /// Mark a stage-stream slot as consumed by a stage reader.
    pub async fn mark_stage_reader_position(
        &self,
        stream_id: u64,
        stage: &str,
        worker_id: &str,
        slot_id: usize,
    ) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let Some(lane) = self.get_stage_lane(stage).await else {
            return false;
        };
        self.mark_lane_reader_position_at(
            stream_idx,
            stream_id,
            worker_id,
            slot_id,
            &lane.reader_positions,
            &lane.reader_notifies,
            stage,
        )
        .await
    }

    /// Register a response-stream reader.
    ///
    /// Response producers retain every unread slot for every registered reader.
    pub async fn register_response_reader(&self, stream_id: u64, reader_id: &str) -> bool {
        if !Self::valid_reader_id(reader_id) {
            return false;
        }
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let mut positions = self.response_reader_positions.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return false;
        }
        let inserted = if positions[stream_idx].contains_key(reader_id)
            || positions[stream_idx].len() >= MAX_READERS_PER_STREAM
        {
            false
        } else {
            positions[stream_idx].insert(reader_id.to_string(), 0);
            true
        };
        drop(positions);
        self.response_reader_notifies[stream_idx].notify_waiters();
        inserted
    }

    /// Mark a response-stream slot as consumed by a response reader.
    pub async fn mark_response_reader_position(
        &self,
        stream_id: u64,
        reader_id: &str,
        slot_id: usize,
    ) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        self.mark_lane_reader_position_at(
            stream_idx,
            stream_id,
            reader_id,
            slot_id,
            &self.response_reader_positions,
            &self.response_reader_notifies,
            "response",
        )
        .await
    }

    /// Remove a response-stream reader and release its retained slots.
    pub async fn unregister_response_reader(&self, stream_id: u64, reader_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let mut positions = self.response_reader_positions.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return false;
        }
        let removed = positions[stream_idx].remove(reader_id).is_some();
        drop(positions);
        self.response_reader_notifies[stream_idx].notify_waiters();
        removed
    }

    pub(crate) async fn is_response_reader_registered(
        &self,
        stream_id: u64,
        reader_id: &str,
    ) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let positions = self.response_reader_positions.read().await;
        let registered = positions[stream_idx].contains_key(reader_id);
        drop(positions);
        registered && self.is_current_slot(stream_id, stream_idx)
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
        if !self.is_current_slot(stream_id, stream_idx) {
            return false;
        }
        let removed = workers[stream_idx].remove(worker_id);
        if removed {
            self.stream_worker_counts[stream_idx].fetch_sub(1, Ordering::Relaxed);
            debug!(stream_id, worker_id, "Reader unregistered");
        }
        drop(workers);
        {
            let mut positions = self.request_reader_positions.write().await;
            if self.is_current_slot(stream_id, stream_idx) {
                positions[stream_idx].remove(worker_id);
            }
        }
        self.request_reader_notifies[stream_idx].notify_waiters();
        {
            let stages = self.stages.read().await;
            let lanes: Vec<_> = stages.values().cloned().collect();
            drop(stages);
            for lane in lanes {
                let mut positions = lane.reader_positions.write().await;
                if self.is_current_slot(stream_id, stream_idx) {
                    positions[stream_idx].remove(worker_id);
                }
                lane.reader_notifies[stream_idx].notify_waiters();
            }
        }
        removed
    }

    /// Get the number of readers currently processing a stream (lock-free).
    pub fn reader_count(&self, stream_id: u64) -> u64 {
        self.stream_idx(stream_id)
            .filter(|stream_idx| self.is_current_slot(stream_id, *stream_idx))
            .map(|stream_idx| self.stream_worker_counts[stream_idx].load(Ordering::Relaxed))
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
        let registered = workers[stream_idx].contains(worker_id);
        drop(workers);
        registered && self.is_current_slot(stream_id, stream_idx)
    }

    /// Get all reader worker IDs currently processing a stream.
    pub async fn get_readers(&self, stream_id: u64) -> Vec<String> {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return Vec::new();
        };
        let workers = self.stream_workers.read().await;
        let readers = workers[stream_idx].iter().cloned().collect();
        drop(workers);
        if self.is_current_slot(stream_id, stream_idx) {
            readers
        } else {
            Vec::new()
        }
    }

    /// Clear all readers from a stream (for cleanup/recovery).
    pub async fn clear_readers(&self, stream_id: u64) {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return;
        };
        let mut workers = self.stream_workers.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return;
        }
        workers[stream_idx].clear();
        drop(workers);
        {
            let mut positions = self.request_reader_positions.write().await;
            if self.is_current_slot(stream_id, stream_idx) {
                positions[stream_idx].clear();
            }
        }
        self.request_reader_notifies[stream_idx].notify_waiters();
        {
            let stages = self.stages.read().await;
            let lanes: Vec<_> = stages.values().cloned().collect();
            drop(stages);
            for lane in lanes {
                let mut positions = lane.reader_positions.write().await;
                if self.is_current_slot(stream_id, stream_idx) {
                    positions[stream_idx].clear();
                }
                lane.reader_notifies[stream_idx].notify_waiters();
            }
        }
        self.stream_worker_counts[stream_idx].store(0, Ordering::Relaxed);
        debug!(stream_id, "All readers cleared");
    }

    // ==================== Response Writer Claim (Exclusive) ====================

    /// Try to claim exclusive access to a named stage on a stream.
    pub async fn try_claim_stage(&self, stream_id: u64, stage: &str, worker_id: &str) -> bool {
        if !Self::valid_reader_id(worker_id) {
            return false;
        }
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let Ok(lane) = self.get_or_create_stage_lane(stage).await else {
            return false;
        };
        let mut claims = lane.claims.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            return false;
        }
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
        if !self.is_current_slot(stream_id, stream_idx) {
            return false;
        }
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
        if !self.is_current_slot(stream_id, stream_idx) {
            return;
        }
        claims[stream_idx] = None;
        debug!(stream_id, stage, "Stage force-released");
    }

    /// Get the worker ID that currently holds a stage claim, if any.
    pub async fn stage_owner(&self, stream_id: u64, stage: &str) -> Option<String> {
        let stream_idx = self.stream_idx(stream_id)?;
        let lane = self.get_stage_lane(stage).await?;
        let claims = lane.claims.read().await;
        let owner = claims[stream_idx].clone();
        drop(claims);
        self.is_current_slot(stream_id, stream_idx)
            .then_some(owner)
            .flatten()
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
        let claimed = claims[stream_idx].is_some();
        drop(claims);
        claimed && self.is_current_slot(stream_id, stream_idx)
    }

    fn response_claim_ttl(&self) -> Duration {
        Duration::from_millis(self.timeouts.response_deadline_ms.max(1_000))
    }

    fn capability_digest(capability: &str) -> [u8; 32] {
        Sha256::digest(capability.as_bytes()).into()
    }

    fn new_response_capability() -> Result<(String, [u8; 32]), String> {
        let mut bytes = [0_u8; RESPONSE_CAPABILITY_BYTES];
        getrandom::fill(&mut bytes)
            .map_err(|error| format!("failed to generate response capability: {error}"))?;
        let capability = URL_SAFE_NO_PAD.encode(bytes);
        let digest = Self::capability_digest(&capability);
        Ok((capability, digest))
    }

    /// Try to claim exclusive write access and return its bearer capability.
    pub async fn try_claim_response_with_capability(
        &self,
        stream_id: u64,
        worker_id: &str,
    ) -> Result<Option<String>, String> {
        if !Self::valid_reader_id(worker_id) {
            return Ok(None);
        }
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let mut claim = self.response_claims[stream_idx].lock().await;
        let now = Instant::now();
        if claim.as_ref().is_some_and(|claim| claim.expires_at > now) {
            return Ok(None);
        }

        let (capability, capability_digest) = Self::new_response_capability()?;
        *claim = Some(ResponseClaim {
            worker_id: worker_id.to_string(),
            capability_digest,
            expires_at: now + self.response_claim_ttl(),
            next_sequence: 1,
            last_write: None,
        });
        debug!(stream_id, worker_id, "Response claimed");
        Ok(Some(capability))
    }

    /// Claim a response for trusted in-process writers.
    ///
    /// Remote writers must use `try_claim_response_with_capability`.
    pub async fn try_claim_response(&self, stream_id: u64, worker_id: &str) -> bool {
        self.try_claim_response_with_capability(stream_id, worker_id)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    /// Release a previously claimed response.
    ///
    /// Returns `true` if released successfully (caller was the owner).
    /// Returns `false` if the response was not claimed by this worker.
    pub async fn release_response(&self, stream_id: u64, worker_id: &str) -> bool {
        let Ok((stream_idx, _slot_guard)) = self.lock_stream_slot(stream_id).await else {
            return false;
        };
        let mut claim = self.response_claims[stream_idx].lock().await;
        if claim
            .as_ref()
            .is_some_and(|claim| claim.worker_id == worker_id)
        {
            *claim = None;
            debug!(stream_id, worker_id, "Response released");
            true
        } else {
            false
        }
    }

    /// Release a remote response claim using its bearer capability.
    pub async fn release_response_with_capability(
        &self,
        stream_id: u64,
        worker_id: &str,
        capability: &str,
    ) -> bool {
        let Ok((stream_idx, _slot_guard)) = self.lock_stream_slot(stream_id).await else {
            return false;
        };
        let supplied_digest = Self::capability_digest(capability);
        let mut claim = self.response_claims[stream_idx].lock().await;
        let authorized = claim.as_ref().is_some_and(|claim| {
            claim.worker_id == worker_id
                && bool::from(claim.capability_digest.ct_eq(&supplied_digest))
                && claim.expires_at > Instant::now()
        });
        if authorized {
            *claim = None;
            debug!(stream_id, worker_id, "Response capability released");
        }
        authorized
    }

    /// Force-release a response regardless of owner (for cleanup/recovery).
    pub async fn force_release_response(&self, stream_id: u64) {
        let Ok((stream_idx, _slot_guard)) = self.lock_stream_slot(stream_id).await else {
            return;
        };
        *self.response_claims[stream_idx].lock().await = None;
        debug!(stream_id, "Response force-released");
    }

    /// Get the worker ID that currently holds the response claim, if any.
    pub async fn response_owner(&self, stream_id: u64) -> Option<String> {
        let stream_idx = self.stream_idx(stream_id)?;
        let claim = self.response_claims[stream_idx].lock().await;
        let owner = claim
            .as_ref()
            .filter(|claim| claim.expires_at > Instant::now())
            .map(|claim| claim.worker_id.clone());
        drop(claim);
        self.is_current_slot(stream_id, stream_idx)
            .then_some(owner)
            .flatten()
    }

    /// Check if the response is claimed by a specific worker.
    pub async fn is_response_claimed_by(&self, stream_id: u64, worker_id: &str) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let claim = self.response_claims[stream_idx].lock().await;
        let claimed = claim
            .as_ref()
            .is_some_and(|claim| claim.worker_id == worker_id && claim.expires_at > Instant::now());
        drop(claim);
        claimed && self.is_current_slot(stream_id, stream_idx)
    }

    /// Check if the response is currently claimed by anyone.
    pub async fn is_response_claimed(&self, stream_id: u64) -> bool {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return false;
        };
        let claim = self.response_claims[stream_idx].lock().await;
        let claimed = claim
            .as_ref()
            .is_some_and(|claim| claim.expires_at > Instant::now());
        drop(claim);
        claimed && self.is_current_slot(stream_id, stream_idx)
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
        if !Self::valid_reader_id(worker_id) || !Self::valid_stage_name(&heartbeat.stage) {
            return heartbeat;
        }
        let mut workers = self.workers.write().await;
        if !workers.contains_key(worker_id) && workers.len() >= MAX_WORKER_HEARTBEATS {
            if let Some(oldest) = workers
                .iter()
                .min_by_key(|(_, worker)| worker.updated_at_ms)
                .map(|(worker_id, _)| worker_id.clone())
            {
                workers.remove(&oldest);
            }
        }
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
            total_max_inflight: workers.iter().fold(0usize, |total, worker| {
                total.saturating_add(worker.max_inflight)
            }),
            total_inflight: workers.iter().fold(0usize, |total, worker| {
                total.saturating_add(worker.inflight)
            }),
            total_available_slots: workers.iter().fold(0usize, |total, worker| {
                total.saturating_add(worker.available_slots)
            }),
        }
    }

    /// Return generation-fenced request-lane access for one active stream.
    pub fn request_lane_handle(self: &Arc<Self>, stream_id: u64) -> Option<UploadLaneHandle> {
        let stream_idx = self.stream_idx(stream_id)?;
        self.is_current_slot(stream_id, stream_idx)
            .then(|| UploadLaneHandle {
                service: Arc::clone(self),
                stream_id,
                stream_idx,
                lane: UploadLane::Request,
            })
    }

    /// Return generation-fenced response-lane access for one active stream.
    pub fn response_lane_handle(self: &Arc<Self>, stream_id: u64) -> Option<UploadLaneHandle> {
        let stream_idx = self.stream_idx(stream_id)?;
        self.is_current_slot(stream_id, stream_idx)
            .then(|| UploadLaneHandle {
                service: Arc::clone(self),
                stream_id,
                stream_idx,
                lane: UploadLane::Response,
            })
    }

    /// Return generation-fenced stage-lane access for one active stream.
    pub async fn stage_lane_handle(
        self: &Arc<Self>,
        stream_id: u64,
        stage: &str,
    ) -> Result<Option<UploadLaneHandle>, String> {
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            return Ok(None);
        };
        if !self.is_current_slot(stream_id, stream_idx) {
            return Ok(None);
        }
        let lane = self.get_or_create_stage_lane(stage).await?;
        Ok(self
            .is_current_slot(stream_id, stream_idx)
            .then(|| UploadLaneHandle {
                service: Arc::clone(self),
                stream_id,
                stream_idx,
                lane: UploadLane::Stage {
                    name: stage.to_string(),
                    lane,
                },
            }))
    }

    /// Get a reference to the request cache for external consumers
    #[deprecated(note = "use request_lane_handle to fence stream-slot reuse")]
    pub fn request_cache(&self) -> Arc<ChunkCache> {
        Arc::clone(&self.request_cache)
    }

    /// Get a reference to a named stage cache for external consumers.
    #[deprecated(note = "use stage_lane_handle to fence stream-slot reuse")]
    pub async fn stage_cache(&self, stage: &str) -> Result<Arc<ChunkCache>, String> {
        let lane = self.get_or_create_stage_lane(stage).await?;
        Ok(Arc::clone(&lane.cache))
    }

    /// Get a reference to the response cache for external consumers
    #[deprecated(note = "use response_lane_handle to fence stream-slot reuse")]
    pub fn response_cache(&self) -> Arc<ChunkCache> {
        Arc::clone(&self.response_cache)
    }

    /// Return a notification source for any response-lane lifecycle or write.
    pub fn response_updates(&self) -> Arc<Notify> {
        Arc::clone(&self.response_updates)
    }

    /// Return the fixed-lane response notification source for one active stream.
    pub fn response_update_notifier(&self, stream_id: u64) -> Option<Arc<Notify>> {
        let stream_idx = self.stream_idx(stream_id)?;
        let notifier = self.response_cache.update_notifier(stream_idx)?;
        self.is_current_slot(stream_id, stream_idx)
            .then_some(notifier)
    }

    /// Get the configuration
    pub fn config(&self) -> &UploadResponseConfig {
        &self.config
    }

    /// Get the independent timeout policy.
    pub fn timeouts(&self) -> &UploadResponseTimeouts {
        &self.timeouts
    }

    /// Get the response channels map for the watcher
    pub fn response_channels(&self) -> Arc<RwLock<HashMap<u64, oneshot::Sender<ResponseResult>>>> {
        Arc::clone(&self.response_channels)
    }

    /// Acquire a stream slot before the admission deadline.
    pub async fn acquire_stream(&self) -> Result<OwnedSemaphorePermit, String> {
        let semaphore = Arc::clone(&self.slot_semaphore);
        match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => return Ok(permit),
            Err(TryAcquireError::Closed) => return Err("streams closed".to_string()),
            Err(TryAcquireError::NoPermits) => {}
        }
        let timeout_duration = Duration::from_millis(self.timeouts.stream_admission_timeout_ms);
        if timeout_duration.is_zero() {
            return Err("stream admission timed out".to_string());
        }
        timeout(timeout_duration, semaphore.acquire_owned())
            .await
            .map_err(|_| "stream admission timed out".to_string())?
            .map_err(|_| "streams closed".to_string())
    }

    /// Get the next sequential stream ID
    pub fn next_id(&self) -> u64 {
        self.allocate_stream_id()
    }

    /// Peek at the next stream ID that will be assigned (without incrementing)
    pub fn peek_next_id(&self) -> u64 {
        self.next_stream_id.load(Ordering::Relaxed).max(1)
    }

    /// Write HPKS headers frame to slot 1 of request stream
    pub async fn write_request_headers(
        &self,
        stream_id: u64,
        headers: StreamHeaders,
    ) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        if self.request_started[stream_idx].load(Ordering::Acquire) {
            return Err(format!(
                "request headers already written for stream: {stream_id}"
            ));
        }
        let encoded = encode_frame(&StreamFrame::Headers(headers));
        self.request_cache
            .add(stream_idx, 1, Bytes::from(encoded))
            .await
            .map_err(|e| e.to_string())?;
        if !self.is_current_slot(stream_id, stream_idx) {
            return Err(format!("request stream closed: {stream_id}"));
        }
        self.request_started[stream_idx].store(true, Ordering::Release);
        debug!(
            stream_id,
            stream_idx,
            lane = "request",
            slot_id = 1,
            "upload-response request headers written"
        );
        Ok(())
    }

    /// Append raw body bytes to request stream (slots 2+)
    pub async fn append_request_body(&self, stream_id: u64, data: Bytes) -> Result<(), String> {
        if data.is_empty() {
            return Err("request body chunks cannot be empty".to_string());
        }
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        if !self.request_started[stream_idx].load(Ordering::Acquire) {
            return Err(format!(
                "request headers not written for stream: {stream_id}"
            ));
        }
        self.append_cache_with_backpressure(
            stream_id,
            stream_idx,
            &self.request_cache,
            &self.request_reader_positions,
            &self.request_reader_notifies,
            "request",
            data,
        )
        .await
    }

    /// Append a request control marker after the headers slot.
    pub async fn append_request_control(
        &self,
        stream_id: u64,
        control: RequestControl,
    ) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        if !self.request_started[stream_idx].load(Ordering::Acquire) {
            return Err(format!(
                "request headers not written for stream: {stream_id}"
            ));
        }
        self.append_cache_with_backpressure(
            stream_id,
            stream_idx,
            &self.request_cache,
            &self.request_reader_positions,
            &self.request_reader_notifies,
            "request",
            encode_request_control(control),
        )
        .await
    }

    /// Write end marker to request stream
    pub async fn end_request(&self, stream_id: u64) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        if !self.request_started[stream_idx].load(Ordering::Acquire) {
            return Err(format!(
                "request headers not written for stream: {stream_id}"
            ));
        }
        self.append_cache_with_backpressure(
            stream_id,
            stream_idx,
            &self.request_cache,
            &self.request_reader_positions,
            &self.request_reader_notifies,
            "request",
            Bytes::from_static(END_MARKER),
        )
        .await
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
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let lane = self.get_or_create_stage_lane(stage).await?;
        if lane.started[stream_idx].load(Ordering::Acquire) {
            return Err(format!(
                "stage head already written for stream: {stream_id}"
            ));
        }
        lane.cache
            .add(stream_idx, 1, head)
            .await
            .map_err(|e| e.to_string())?;
        if !self.is_current_slot(stream_id, stream_idx) {
            return Err(format!("stage stream closed: {stream_id}"));
        }
        lane.started[stream_idx].store(true, Ordering::Release);
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
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let lane = self.get_or_create_stage_lane(stage).await?;
        if !lane.started[stream_idx].load(Ordering::Acquire) {
            return Err(format!("stage head not written for stream: {stream_id}"));
        }
        self.append_cache_with_backpressure(
            stream_id,
            stream_idx,
            &lane.cache,
            &lane.reader_positions,
            &lane.reader_notifies,
            stage,
            data,
        )
        .await
    }

    /// Append a stage control marker after the stage head slot.
    pub async fn append_stage_control(
        &self,
        stream_id: u64,
        stage: &str,
        control: RequestControl,
    ) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let lane = self.get_or_create_stage_lane(stage).await?;
        if !lane.started[stream_idx].load(Ordering::Acquire) {
            return Err(format!("stage head not written for stream: {stream_id}"));
        }
        self.append_cache_with_backpressure(
            stream_id,
            stream_idx,
            &lane.cache,
            &lane.reader_positions,
            &lane.reader_notifies,
            stage,
            encode_request_control(control),
        )
        .await
    }

    /// Write end marker to a named stage stream.
    pub async fn end_stage(&self, stream_id: u64, stage: &str) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let lane = self.get_or_create_stage_lane(stage).await?;
        if !lane.started[stream_idx].load(Ordering::Acquire) {
            return Err(format!("stage head not written for stream: {stream_id}"));
        }
        self.append_cache_with_backpressure(
            stream_id,
            stream_idx,
            &lane.cache,
            &lane.reader_positions,
            &lane.reader_notifies,
            stage,
            Bytes::from_static(END_MARKER),
        )
        .await
    }

    async fn write_response_at(
        &self,
        stream_id: u64,
        stream_idx: usize,
        kind: ResponseWriteKind,
        data: Bytes,
    ) -> Result<(), String> {
        match kind {
            ResponseWriteKind::Headers => {
                if self.response_started[stream_idx].load(Ordering::Acquire) {
                    return Err(format!(
                        "response headers already written for stream: {stream_id}"
                    ));
                }
                self.response_cache
                    .add(stream_idx, 1, data)
                    .await
                    .map_err(|error| error.to_string())?;
                if !self.is_current_slot(stream_id, stream_idx) {
                    return Err(format!("response stream closed: {stream_id}"));
                }
                self.response_started[stream_idx].store(true, Ordering::Release);
            }
            ResponseWriteKind::Body => {
                if data.is_empty() {
                    return Err("response body chunks cannot be empty".to_string());
                }
                if !self.response_started[stream_idx].load(Ordering::Acquire) {
                    return Err(format!(
                        "response headers not written for stream: {stream_id}"
                    ));
                }
                self.append_cache_with_backpressure(
                    stream_id,
                    stream_idx,
                    &self.response_cache,
                    &self.response_reader_positions,
                    &self.response_reader_notifies,
                    "response",
                    data,
                )
                .await?;
            }
            ResponseWriteKind::End => {
                if !self.response_started[stream_idx].load(Ordering::Acquire) {
                    return Err(format!(
                        "response headers not written for stream: {stream_id}"
                    ));
                }
                self.append_cache_with_backpressure(
                    stream_id,
                    stream_idx,
                    &self.response_cache,
                    &self.response_reader_positions,
                    &self.response_reader_notifies,
                    "response",
                    Bytes::from_static(END_MARKER),
                )
                .await?;
            }
        }
        self.notify_response_update(stream_idx);
        Ok(())
    }

    async fn write_claimed_response(
        &self,
        stream_id: u64,
        capability: &str,
        sequence: u64,
        kind: ResponseWriteKind,
        data: Bytes,
    ) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let supplied_digest = Self::capability_digest(capability);
        let write_digest = kind.digest(&data);
        let mut claim = self.response_claims[stream_idx].lock().await;
        let claim = claim
            .as_mut()
            .ok_or_else(|| "response capability required".to_string())?;
        if !bool::from(claim.capability_digest.ct_eq(&supplied_digest)) {
            return Err("invalid response capability".to_string());
        }
        if claim.expires_at <= Instant::now() {
            return Err("response capability expired".to_string());
        }
        if sequence < claim.next_sequence {
            let retry_matches = claim.last_write.is_some_and(|last| {
                last.sequence == sequence && bool::from(last.digest.ct_eq(&write_digest))
            });
            return if retry_matches {
                Ok(())
            } else {
                Err(format!("conflicting response retry sequence: {sequence}"))
            };
        }
        if sequence != claim.next_sequence {
            return Err(format!(
                "unexpected response sequence: {sequence}; expected {}",
                claim.next_sequence
            ));
        }
        let next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| "response sequence exhausted".to_string())?;

        self.write_response_at(stream_id, stream_idx, kind, data)
            .await?;
        claim.next_sequence = next_sequence;
        claim.last_write = Some(ResponseWriteRecord {
            sequence,
            digest: write_digest,
        });
        claim.expires_at = Instant::now() + self.response_claim_ttl();
        Ok(())
    }

    /// Write an HPKS headers frame from a trusted in-process writer.
    pub async fn write_response_headers(
        &self,
        stream_id: u64,
        headers: StreamHeaders,
    ) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        let encoded = Bytes::from(encode_frame(&StreamFrame::Headers(headers)));
        self.write_response_at(stream_id, stream_idx, ResponseWriteKind::Headers, encoded)
            .await
    }

    pub async fn write_response_headers_claimed(
        &self,
        stream_id: u64,
        capability: &str,
        sequence: u64,
        headers: StreamHeaders,
    ) -> Result<(), String> {
        let encoded = Bytes::from(encode_frame(&StreamFrame::Headers(headers)));
        self.write_claimed_response(
            stream_id,
            capability,
            sequence,
            ResponseWriteKind::Headers,
            encoded,
        )
        .await
    }

    /// Append raw body bytes from a trusted in-process writer.
    pub async fn append_response_body(&self, stream_id: u64, data: Bytes) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        self.write_response_at(stream_id, stream_idx, ResponseWriteKind::Body, data)
            .await
    }

    pub async fn append_response_body_claimed(
        &self,
        stream_id: u64,
        capability: &str,
        sequence: u64,
        data: Bytes,
    ) -> Result<(), String> {
        self.write_claimed_response(
            stream_id,
            capability,
            sequence,
            ResponseWriteKind::Body,
            data,
        )
        .await
    }

    /// End a response from a trusted in-process writer.
    pub async fn end_response(&self, stream_id: u64) -> Result<(), String> {
        let (stream_idx, _slot_guard) = self.lock_stream_slot(stream_id).await?;
        self.write_response_at(stream_id, stream_idx, ResponseWriteKind::End, Bytes::new())
            .await
    }

    pub async fn end_response_claimed(
        &self,
        stream_id: u64,
        capability: &str,
        sequence: u64,
    ) -> Result<(), String> {
        self.write_claimed_response(
            stream_id,
            capability,
            sequence,
            ResponseWriteKind::End,
            Bytes::new(),
        )
        .await
    }

    pub async fn write_handler_response(
        &self,
        stream_id: u64,
        response: HandlerResponse,
    ) -> Result<(), String> {
        let mut builder = http::Response::builder().status(response.status);
        if let Some(content_type) = &response.content_type {
            builder = builder.header(http::header::CONTENT_TYPE, content_type.as_ref());
        }
        if let Some(etag) = response.etag {
            builder = builder.header(http::header::ETAG, etag.to_string());
        }
        for (name, value) in &response.headers {
            builder = builder.header(name.as_ref(), value.as_ref());
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
        let last = if self.request_started[stream_idx].load(Ordering::Acquire) {
            self.request_cache.last(stream_idx)
        } else {
            Some(0)
        };
        self.is_current_slot(stream_id, stream_idx)
            .then_some(last)
            .flatten()
    }

    /// Get raw bytes from request stream slot
    pub async fn request_get(&self, stream_id: u64, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id)?;
        if !self.request_started[stream_idx].load(Ordering::Acquire) {
            debug!(
                stream_id,
                stream_idx,
                slot_id,
                lane = "request",
                "upload-response request slot miss: stream not started"
            );
            return None;
        }
        let Some((bytes, hash)) = self.request_cache.get(stream_idx, slot_id).await else {
            debug!(
                stream_id,
                stream_idx,
                slot_id,
                lane = "request",
                "upload-response request slot miss"
            );
            return None;
        };
        if !self.is_current_slot(stream_id, stream_idx) {
            return None;
        }
        debug!(
            stream_id,
            stream_idx,
            slot_id,
            lane = "request",
            bytes = bytes.len(),
            hash,
            "upload-response request slot read"
        );
        Some(bytes)
    }

    /// Get last slot index for a response stream
    pub fn response_last(&self, stream_id: u64) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id)?;
        let last = if self.response_started[stream_idx].load(Ordering::Acquire) {
            self.response_cache.last(stream_idx)
        } else {
            Some(0)
        };
        self.is_current_slot(stream_id, stream_idx)
            .then_some(last)
            .flatten()
    }

    /// Get last slot index for a named stage stream.
    pub async fn stage_last(&self, stream_id: u64, stage: &str) -> Option<usize> {
        let stream_idx = self.stream_idx(stream_id)?;
        let lane = self.get_stage_lane(stage).await?;
        let last = if lane.started[stream_idx].load(Ordering::Acquire) {
            lane.cache.last(stream_idx)
        } else {
            Some(0)
        };
        self.is_current_slot(stream_id, stream_idx)
            .then_some(last)
            .flatten()
    }

    /// Get raw bytes from a named stage stream slot.
    pub async fn stage_get(&self, stream_id: u64, stage: &str, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id)?;
        let lane = self.get_stage_lane(stage).await?;
        if !lane.started[stream_idx].load(Ordering::Acquire) {
            debug!(
                stream_id,
                stream_idx,
                slot_id,
                lane = stage,
                "upload-response stage slot miss: stream not started"
            );
            return None;
        }
        let Some((bytes, hash)) = lane.cache.get(stream_idx, slot_id).await else {
            debug!(
                stream_id,
                stream_idx,
                slot_id,
                lane = stage,
                "upload-response stage slot miss"
            );
            return None;
        };
        if !self.is_current_slot(stream_id, stream_idx) {
            return None;
        }
        debug!(
            stream_id,
            stream_idx,
            slot_id,
            lane = stage,
            bytes = bytes.len(),
            hash,
            "upload-response stage slot read"
        );
        Some(bytes)
    }

    /// Get raw bytes from response stream slot
    pub async fn response_get(&self, stream_id: u64, slot_id: usize) -> Option<Bytes> {
        let stream_idx = self.stream_idx(stream_id)?;
        if !self.response_started[stream_idx].load(Ordering::Acquire) {
            return None;
        }
        let (bytes, _hash) = self.response_cache.get(stream_idx, slot_id).await?;
        self.is_current_slot(stream_id, stream_idx).then_some(bytes)
    }

    /// Check if slot is the end marker (empty)
    pub fn is_end_marker(data: &[u8]) -> bool {
        data.is_empty()
    }

    /// Register a response channel for a given stream ID
    pub async fn register_response(&self, stream_id: u64) -> oneshot::Receiver<ResponseResult> {
        let (tx, rx) = oneshot::channel();
        let Some(stream_idx) = self.stream_idx(stream_id) else {
            let _ = tx.send(Err(format!("unknown stream: {stream_id}")));
            return rx;
        };
        let mut channels = self.response_channels.write().await;
        if !self.is_current_slot(stream_id, stream_idx) {
            drop(channels);
            let _ = tx.send(Err(format!("stream closed: {stream_id}")));
            return rx;
        }
        if channels.contains_key(&stream_id) {
            drop(channels);
            let _ = tx.send(Err(format!(
                "response channel already registered for stream: {stream_id}"
            )));
            return rx;
        }
        channels.insert(stream_id, tx);
        drop(channels);
        let _ = self
            .register_response_reader(stream_id, RESPONSE_WATCHER_READER_ID)
            .await;
        self.notify_response_update(stream_idx);
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
        let _ = self
            .unregister_response_reader(stream_id, RESPONSE_WATCHER_READER_ID)
            .await;
    }

    /// Drop a response channel without completing (e.g., on timeout)
    pub async fn drop_response_channel(&self, stream_id: u64) {
        let mut channels = self.response_channels.write().await;
        channels.remove(&stream_id);
        drop(channels);
        let _ = self
            .unregister_response_reader(stream_id, RESPONSE_WATCHER_READER_ID)
            .await;
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
            content_type: Some("text/plain; charset=utf-8".into()),
            ..Default::default()
        }
    }

    fn json_response<T: Serialize>(status: StatusCode, value: &T) -> HandlerResponse {
        HandlerResponse {
            status,
            body: Some(Bytes::from(
                serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec()),
            )),
            content_type: Some("application/json".into()),
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
                "x-upload-response-slot-type".into(),
                slot_type.to_string().into(),
            ));
        }
        HandlerResponse {
            status,
            body: Some(body),
            content_type: Some("application/octet-stream".into()),
            headers,
            ..Default::default()
        }
    }

    async fn collect_body(
        mut body: Option<BodyStream>,
        max_bytes: usize,
    ) -> Result<Bytes, ServerError> {
        let Some(ref mut body_stream) = body else {
            return Ok(Bytes::new());
        };

        let mut collected = Vec::new();
        while let Some(chunk) = body_stream.next().await {
            let chunk = chunk?;
            let next_len = collected
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| ServerError::Config("request body size overflow".to_string()))?;
            if next_len > max_bytes {
                return Err(ServerError::Config(format!(
                    "request body exceeds {max_bytes} bytes"
                )));
            }
            collected
                .try_reserve(chunk.len())
                .map_err(|_| ServerError::Config("request body allocation failed".to_string()))?;
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

    fn response_capability(req: &Request<()>) -> Option<&str> {
        req.headers()
            .get(RESPONSE_CAPABILITY_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
    }

    fn response_write_authority(req: &Request<()>) -> Result<(String, u64), HandlerResponse> {
        let capability = Self::response_capability(req)
            .ok_or_else(|| Self::text_response(StatusCode::UNAUTHORIZED, "capability required"))?
            .to_string();
        let sequence = req
            .headers()
            .get(RESPONSE_SEQUENCE_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| {
                Self::text_response(StatusCode::BAD_REQUEST, "positive sequence required")
            })?;
        Ok((capability, sequence))
    }

    fn claimed_response_error(error: String) -> HandlerResponse {
        let status = if error.contains("capability required") {
            StatusCode::UNAUTHORIZED
        } else if error.contains("invalid response capability")
            || error.contains("response capability expired")
        {
            StatusCode::FORBIDDEN
        } else if error.contains("sequence") || error.contains("already written") {
            StatusCode::CONFLICT
        } else if error.contains("unknown stream") || error.contains("stream closed") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::BAD_REQUEST
        };
        Self::text_response(status, error)
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
                let body = Self::collect_body(body, MAX_INTERNAL_BODY_BYTES).await?;
                let update = Self::parse_json_body::<WorkerHeartbeatUpdate>(body)?;
                if !UploadResponseService::valid_reader_id(worker_id)
                    || !UploadResponseService::valid_stage_name(&update.stage)
                {
                    return Ok(Self::text_response(
                        StatusCode::BAD_REQUEST,
                        "invalid worker id or stage",
                    ));
                }
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
            (
                "PUT",
                ["_upload_response", "streams", stream_id, "request", "readers", worker_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let inserted = self
                    .service
                    .register_request_reader(stream_id, worker_id)
                    .await;
                let status = if inserted {
                    StatusCode::OK
                } else {
                    StatusCode::NO_CONTENT
                };
                Ok(Self::text_response(status, "ok"))
            }
            (
                "PUT",
                ["_upload_response", "streams", stream_id, "request", "readers", worker_id, "slots", slot_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                let slot_id = Self::parse_usize_component(slot_id, "slot_id")?;
                if self
                    .service
                    .mark_request_reader_position(stream_id, worker_id, slot_id)
                    .await
                {
                    Ok(Self::text_response(StatusCode::OK, "ok"))
                } else {
                    Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "reader not found",
                    ))
                }
            }
            (
                "PUT",
                ["_upload_response", "streams", stream_id, "stages", stage, "readers", worker_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let inserted = self
                    .service
                    .register_stage_reader(stream_id, stage, worker_id)
                    .await;
                let status = if inserted {
                    StatusCode::OK
                } else {
                    StatusCode::NO_CONTENT
                };
                Ok(Self::text_response(status, "ok"))
            }
            (
                "PUT",
                ["_upload_response", "streams", stream_id, "stages", stage, "readers", worker_id, "slots", slot_id],
            ) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                let slot_id = Self::parse_usize_component(slot_id, "slot_id")?;
                if self
                    .service
                    .mark_stage_reader_position(stream_id, stage, worker_id, slot_id)
                    .await
                {
                    Ok(Self::text_response(StatusCode::OK, "ok"))
                } else {
                    Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "reader not found",
                    ))
                }
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
                match self
                    .service
                    .try_claim_response_with_capability(stream_id, worker_id)
                    .await
                    .map_err(ServerError::Config)?
                {
                    Some(capability) => {
                        let mut response = Self::text_response(StatusCode::OK, "claimed");
                        response
                            .headers
                            .push((RESPONSE_CAPABILITY_HEADER.into(), capability.into()));
                        response
                            .headers
                            .push(("cache-control".into(), "no-store".into()));
                        Ok(response)
                    }
                    None => {
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
                let Some(capability) = Self::response_capability(&req) else {
                    return Ok(Self::text_response(
                        StatusCode::UNAUTHORIZED,
                        "capability required",
                    ));
                };
                let released = self
                    .service
                    .release_response_with_capability(stream_id, worker_id, capability)
                    .await;
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
                let body = Self::collect_body(body, self.service.config().slot_bytes()).await?;
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
                let body = Self::collect_body(body, self.service.config().slot_bytes()).await?;
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
                let body = Self::collect_body(body, REQUEST_CONTROL_MAGIC.len() + 1).await?;
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
                let (capability, sequence) = match Self::response_write_authority(&req) {
                    Ok(authority) => authority,
                    Err(response) => return Ok(response),
                };
                let body = Self::collect_body(body, self.service.config().slot_bytes()).await?;
                let frame = decode_frame(&body)
                    .map_err(|e| ServerError::Config(format!("invalid HPKS headers frame: {e}")))?;
                match frame {
                    StreamFrame::Headers(StreamHeaders::Response(resp))
                        if resp.stream_id == stream_id =>
                    {
                        self.service
                            .write_response_headers_claimed(
                                stream_id,
                                &capability,
                                sequence,
                                StreamHeaders::Response(resp),
                            )
                            .await
                            .map_or_else(
                                |error| Ok(Self::claimed_response_error(error)),
                                |_| Ok(Self::text_response(StatusCode::OK, "ok")),
                            )
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
                let (capability, sequence) = match Self::response_write_authority(&req) {
                    Ok(authority) => authority,
                    Err(response) => return Ok(response),
                };
                let body = Self::collect_body(body, self.service.config().slot_bytes()).await?;
                match self
                    .service
                    .append_response_body_claimed(stream_id, &capability, sequence, body)
                    .await
                {
                    Ok(()) => Ok(Self::text_response(StatusCode::OK, "ok")),
                    Err(error) => Ok(Self::claimed_response_error(error)),
                }
            }
            ("PUT", ["_upload_response", "streams", stream_id, "response", "end"]) => {
                let stream_id = Self::parse_u64_component(stream_id, "stream_id")?;
                if self.service.stream_idx(stream_id).is_none() {
                    return Ok(Self::text_response(
                        StatusCode::NOT_FOUND,
                        "stream not found",
                    ));
                }
                let (capability, sequence) = match Self::response_write_authority(&req) {
                    Ok(authority) => authority,
                    Err(response) => return Ok(response),
                };
                match self
                    .service
                    .end_response_claimed(stream_id, &capability, sequence)
                    .await
                {
                    Ok(()) => Ok(Self::text_response(StatusCode::OK, "ok")),
                    Err(error) => Ok(Self::claimed_response_error(error)),
                }
            }
            _ => Ok(Self::text_response(StatusCode::NOT_FOUND, "not found")),
        }
    }

    async fn await_response(
        &self,
        stream_id: u64,
        rx: oneshot::Receiver<ResponseResult>,
    ) -> HandlerResult<HandlerResponse> {
        let timeout_duration = Duration::from_millis(self.service.timeouts.response_deadline_ms);
        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok(cached))) => {
                debug!(stream_id, status = ?cached.status, "Received response");
                let mut content_type = None;
                let mut headers = Vec::new();
                for (name, value) in cached.headers {
                    if name.eq_ignore_ascii_case("content-type") {
                        content_type = Some(value.into());
                    } else {
                        headers.push((name.into(), value.into()));
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

    /// Copy a request body into the cache one slot at a time.
    async fn copy_request_body(
        &self,
        stream_id: u64,
        body: Option<&mut BodyStream>,
    ) -> HandlerResult<()> {
        let Some(body_stream) = body else {
            return Ok(());
        };
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
        Ok(())
    }

    /// Send a bodiless status through a stream writer and complete it.
    async fn write_stream_status(
        mut stream_writer: Box<dyn StreamWriter>,
        status: StatusCode,
        headers: &[(&str, &str)],
    ) -> HandlerResult<()> {
        let mut builder = Response::builder().status(status);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = builder.body(()).map_err(ServerError::Http)?;
        stream_writer.send_response(response).await?;
        stream_writer.finish().await
    }

    /// Stream a request into the cache and stream the response straight back
    /// out of it, slot by slot, without assembling a whole body first.
    async fn stream_request_and_response(
        &self,
        req: Request<()>,
        mut body: Option<BodyStream>,
        stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        let stream = match self.service.try_open_stream().await {
            Ok(stream) => stream,
            Err(error) => {
                debug!(%error, "upload-response admission capacity is full");
                return Self::write_stream_status(
                    stream_writer,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &[("retry-after", "1")],
                )
                .await;
            }
        };
        let stream_id = stream.stream_id();
        // Deliberately no `register_response` here: that assigns the lane to
        // ResponseWatcher, which would buffer the whole body before delivery.
        debug!(stream_id, uri = %req.uri(), "Streaming request and response");

        let result = async {
            let headers = StreamHeaders::from_request(stream_id, &req)
                .map_err(|e| ServerError::Config(e.to_string()))?;
            self.service
                .write_request_headers(stream_id, headers)
                .await
                .map_err(ServerError::Config)?;

            self.copy_request_body(stream_id, body.as_mut()).await?;

            self.service
                .end_request(stream_id)
                .await
                .map_err(ServerError::Config)?;

            debug!(stream_id, "Request complete, streaming response");
            proxy_streaming_response_with(
                &self.service,
                self.service.timeouts(),
                stream_id,
                stream_writer,
            )
            .await
        }
        .await;

        stream.close().await;
        result
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
        let stream = match self.service.try_open_stream().await {
            Ok(stream) => stream,
            Err(error) => {
                debug!(%error, "upload-response admission capacity is full");
                return Ok(HandlerResponse {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    headers: vec![("retry-after".into(), "1".into())],
                    ..Default::default()
                });
            }
        };
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

            self.copy_request_body(stream_id, body.as_mut()).await?;

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
            return Ok(Self::text_response(StatusCode::NOT_FOUND, "not found"));
        }
        self.stream_request(req, None).await
    }

    async fn route_body(
        &self,
        req: Request<()>,
        body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        if Self::is_internal_path(req.uri().path()) {
            return Ok(Self::text_response(StatusCode::NOT_FOUND, "not found"));
        }
        self.stream_request(req, Some(body)).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        !Self::is_internal_path(path)
    }

    /// Public traffic takes the combined path: the request body streams into
    /// the cache and the response streams back out of it. This is checked
    /// ahead of `has_body_handler` and `is_streaming` by every backend.
    fn has_body_stream_handler(&self, path: &str) -> bool {
        !Self::is_internal_path(path)
    }

    async fn route_body_stream(
        &self,
        req: Request<()>,
        body: BodyStream,
        stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if Self::is_internal_path(req.uri().path()) {
            return Self::write_stream_status(stream_writer, StatusCode::NOT_FOUND, &[]).await;
        }
        self.stream_request_and_response(req, Some(body), stream_writer)
            .await
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

/// Private router for mutually authenticated worker coordination.
pub struct UploadResponseControlRouter {
    inner: UploadResponseRouter,
}

impl UploadResponseControlRouter {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            inner: UploadResponseRouter::new(service),
        }
    }

    pub fn service(&self) -> Arc<UploadResponseService> {
        self.inner.service()
    }

    fn authorize(req: &Request<()>) -> Option<HandlerResponse> {
        req.extensions()
            .get::<VerifiedClientCertificate>()
            .is_none()
            .then(|| {
                UploadResponseRouter::text_response(
                    StatusCode::UNAUTHORIZED,
                    "verified client certificate required",
                )
            })
    }
}

#[async_trait]
impl Router for UploadResponseControlRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if !UploadResponseRouter::is_internal_path(req.uri().path()) {
            return Ok(UploadResponseRouter::text_response(
                StatusCode::NOT_FOUND,
                "not found",
            ));
        }
        if let Some(response) = Self::authorize(&req) {
            return Ok(response);
        }
        self.inner.route_internal(req, None).await
    }

    async fn route_body(
        &self,
        req: Request<()>,
        body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        if !UploadResponseRouter::is_internal_path(req.uri().path()) {
            return Ok(UploadResponseRouter::text_response(
                StatusCode::NOT_FOUND,
                "not found",
            ));
        }
        if let Some(response) = Self::authorize(&req) {
            return Ok(response);
        }
        self.inner.route_internal(req, Some(body)).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        UploadResponseRouter::is_internal_path(path)
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

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
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
            .try_open_stream()
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
                                .append_request_body(stream_id, data)
                                .await
                                .map_err(ServerError::Config)?;
                        } else {
                            let mut remaining = data;
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
            let timeout_duration =
                Duration::from_millis(self.service.timeouts.response_deadline_ms);
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

    #[test]
    fn capacity_estimate_includes_all_cache_lanes() {
        let config = UploadResponseConfig::default();
        let capacity = config.validate().unwrap();
        let expected_slots = (config.num_streams as u128)
            * (config.slots_per_stream as u128)
            * (capacity.cache_lanes as u128);
        let expected_initialization = (config.num_streams as u128)
            * (capacity.cache_lanes as u128)
            * (Options::default().init_size_kb as u128)
            * 1024;

        assert_eq!(capacity.cache_lanes, EAGER_CACHE_LANES + MAX_STAGE_LANES);
        assert_eq!(
            capacity.logical_maximum_bytes,
            expected_slots * (config.slot_bytes() as u128) + expected_initialization
        );
        assert!(capacity.eager_estimated_metadata_bytes < capacity.estimated_metadata_bytes);
    }

    #[test]
    fn capacity_validation_allows_high_stream_concurrency() {
        let capacity = UploadResponseConfig {
            num_streams: 4_096,
            slot_size_kb: 32,
            slots_per_stream: 16,
            ..Default::default()
        }
        .validate()
        .unwrap();

        assert_eq!(capacity.streams, 4_096);
        assert_eq!(capacity.slots_per_stream, 16);
        assert!(capacity.logical_maximum_bytes < MAX_UPLOAD_RESPONSE_LOGICAL_BYTES);
        assert!(capacity.estimated_metadata_bytes < MAX_UPLOAD_RESPONSE_ESTIMATED_METADATA_BYTES);
    }

    #[test]
    fn legacy_response_timeout_maps_to_original_purposes() {
        let config = UploadResponseConfig {
            response_timeout_ms: 1_234,
            ..Default::default()
        };

        assert_eq!(
            config.legacy_timeouts(),
            UploadResponseTimeouts {
                response_deadline_ms: 1_234,
                response_idle_timeout_ms: 1_234,
                reader_backpressure_timeout_ms: 1_234,
                stream_admission_timeout_ms: 1_234,
                remote_io_timeout_ms: 60_000,
            }
        );
    }

    #[test]
    fn try_new_normalizes_zero_capacity() {
        let service = UploadResponseService::try_new(UploadResponseConfig {
            num_streams: 0,
            slot_size_kb: 0,
            slots_per_stream: 0,
            response_timeout_ms: 0,
        })
        .unwrap();
        assert_eq!(service.config().num_streams, 1);
        assert_eq!(service.config().slot_bytes(), 1024);
        assert_eq!(service.config().slots_per_stream, 1);
    }

    #[test]
    fn try_new_rejects_extreme_capacity_before_allocation() {
        let stream_error = UploadResponseService::try_new(UploadResponseConfig {
            num_streams: usize::MAX,
            ..Default::default()
        })
        .err()
        .expect("extreme stream count must fail");
        assert!(stream_error.reason.contains("num_streams"));
        assert!(stream_error.logical_maximum_bytes > MAX_UPLOAD_RESPONSE_LOGICAL_BYTES);

        let arithmetic_overflow = UploadResponseConfig {
            num_streams: usize::MAX,
            slot_size_kb: usize::MAX,
            slots_per_stream: usize::MAX,
            ..Default::default()
        };
        let (overflow_capacity, overflowed) = arithmetic_overflow.capacity_estimate();
        assert!(overflowed);
        assert_eq!(overflow_capacity.logical_maximum_bytes, u128::MAX);

        let metadata_error = UploadResponseService::try_new(UploadResponseConfig {
            num_streams: 4_096,
            slot_size_kb: 1,
            slots_per_stream: 512,
            ..Default::default()
        })
        .err()
        .expect("extreme metadata must fail");
        assert!(metadata_error.reason.contains("estimated metadata"));
        assert!(
            metadata_error.estimated_metadata_bytes > MAX_UPLOAD_RESPONSE_ESTIMATED_METADATA_BYTES
        );

        let logical_error = UploadResponseService::try_new(UploadResponseConfig {
            slot_size_kb: (MAX_UPLOAD_RESPONSE_SLOT_BYTES / 1024) as usize,
            ..Default::default()
        })
        .err()
        .expect("extreme logical capacity must fail");
        assert!(logical_error.reason.contains("logical maximum"));
        assert!(logical_error.to_string().contains(&format!(
            "estimated metadata: {} bytes",
            logical_error.estimated_metadata_bytes
        )));
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
    async fn stream_admission_has_an_independent_deadline() {
        let service = Arc::new(UploadResponseService::new_with_timeouts(
            UploadResponseConfig {
                num_streams: 1,
                response_timeout_ms: 1_000,
                ..Default::default()
            },
            UploadResponseTimeouts {
                stream_admission_timeout_ms: 20,
                ..Default::default()
            },
        ));
        let active = service.open_stream().await.unwrap();

        let result = timeout(Duration::from_millis(250), service.open_stream())
            .await
            .expect("stream admission ignored its deadline");
        assert_eq!(result.err().as_deref(), Some("stream admission timed out"));

        active.close().await;
    }

    #[tokio::test]
    async fn response_wait_has_an_independent_deadline() {
        let service = Arc::new(UploadResponseService::new_with_timeouts(
            UploadResponseConfig {
                response_timeout_ms: 1_000,
                ..Default::default()
            },
            UploadResponseTimeouts {
                response_deadline_ms: 20,
                ..Default::default()
            },
        ));
        let router = UploadResponseRouter::new(service);
        let request = Request::builder().uri("/upload").body(()).unwrap();

        let result = timeout(Duration::from_millis(250), router.route(request))
            .await
            .expect("response wait ignored its deadline");
        assert!(matches!(
            result,
            Err(ServerError::Config(error)) if error == "response timeout"
        ));
    }

    #[tokio::test]
    async fn router_rejects_immediately_when_stream_capacity_is_full() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            ..Default::default()
        }));
        let _active = service.open_stream().await.unwrap();
        let router = UploadResponseRouter::new(service);
        let request = Request::builder().uri("/upload").body(()).unwrap();

        let response = timeout(Duration::from_millis(50), router.route(request))
            .await
            .expect("full admission waited instead of rejecting")
            .unwrap();
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers, vec![("retry-after".into(), "1".into())]);
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
    async fn concurrent_request_appends_publish_distinct_slots() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 64,
            ..Default::default()
        }));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();
        service
            .write_request_headers(
                stream_id,
                StreamHeaders::Request(StreamRequestHeaders {
                    stream_id,
                    version: http_pack::HttpVersion::Http11,
                    method: b"POST".to_vec(),
                    scheme: None,
                    authority: None,
                    path: b"/concurrent".to_vec(),
                    headers: vec![],
                }),
            )
            .await
            .unwrap();

        let mut appends = Vec::new();
        for value in 0u8..32 {
            let service = Arc::clone(&service);
            appends.push(tokio::spawn(async move {
                service
                    .append_request_body(stream_id, Bytes::copy_from_slice(&[value]))
                    .await
            }));
        }
        for append in appends {
            append.await.unwrap().unwrap();
        }

        let mut values = std::collections::HashSet::new();
        for slot_id in 2..=33 {
            let bytes = service.request_get(stream_id, slot_id).await.unwrap();
            assert!(values.insert(bytes[0]));
        }
        assert_eq!(values.len(), 32);
        upload_stream.close().await;
    }

    #[tokio::test]
    async fn blocked_writer_cannot_cross_slot_reuse() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 2,
            response_timeout_ms: 1_000,
            ..Default::default()
        }));
        let first_stream = service.open_stream().await.unwrap();
        let first_id = first_stream.stream_id();
        service
            .write_request_headers(
                first_id,
                StreamHeaders::Request(StreamRequestHeaders {
                    stream_id: first_id,
                    version: http_pack::HttpVersion::Http11,
                    method: b"POST".to_vec(),
                    scheme: None,
                    authority: None,
                    path: b"/old".to_vec(),
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        service
            .append_request_body(first_id, Bytes::from_static(b"retained"))
            .await
            .unwrap();

        let old_service = Arc::clone(&service);
        let blocked = tokio::spawn(async move {
            old_service
                .append_request_body(first_id, Bytes::from_static(b"must-not-cross"))
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!blocked.is_finished());

        first_stream.close().await;
        let error = blocked.await.unwrap().unwrap_err();
        assert!(error.contains("stream closed"));

        let replacement = service.open_stream().await.unwrap();
        assert_ne!(replacement.stream_id(), first_id);
        assert_eq!(service.request_last(replacement.stream_id()), Some(0));
        assert!(service
            .request_get(replacement.stream_id(), 2)
            .await
            .is_none());
        replacement.close().await;
    }

    #[test]
    fn drop_without_a_runtime_releases_the_stream_slot() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            ..Default::default()
        }));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let stream = runtime.block_on(service.open_stream()).unwrap();
        drop(runtime);
        drop(stream);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let replacement = runtime.block_on(service.open_stream()).unwrap();
        runtime.block_on(replacement.close());
    }

    #[tokio::test]
    async fn test_reused_stream_slot_starts_with_empty_lanes() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 8,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let first_stream = service.open_stream().await.unwrap();
        let first_id = first_stream.stream_id();

        service
            .write_request_headers(
                first_id,
                StreamHeaders::Request(StreamRequestHeaders {
                    stream_id: first_id,
                    version: http_pack::HttpVersion::Http11,
                    method: b"POST".to_vec(),
                    scheme: None,
                    authority: Some(b"example.com".to_vec()),
                    path: b"/upload".to_vec(),
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        service
            .append_request_body(first_id, Bytes::from_static(b"old-request"))
            .await
            .unwrap();
        service.end_request(first_id).await.unwrap();

        service
            .write_stage_head(first_id, "decode", Bytes::from_static(b"old-head"))
            .await
            .unwrap();
        service
            .append_stage_body(first_id, "decode", Bytes::from_static(b"old-stage"))
            .await
            .unwrap();
        service.end_stage(first_id, "decode").await.unwrap();

        service
            .write_response_headers(
                first_id,
                StreamHeaders::Response(StreamResponseHeaders {
                    stream_id: first_id,
                    version: http_pack::HttpVersion::Http11,
                    status: 200,
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        service
            .append_response_body(first_id, Bytes::from_static(b"old-response"))
            .await
            .unwrap();
        service.end_response(first_id).await.unwrap();
        let old_request = service.request_lane_handle(first_id).unwrap();
        let old_stage = service
            .stage_lane_handle(first_id, "decode")
            .await
            .unwrap()
            .unwrap();
        let old_response = service.response_lane_handle(first_id).unwrap();
        assert_eq!(
            old_request.get(2).await.unwrap(),
            Bytes::from_static(b"old-request")
        );
        assert_eq!(
            old_stage.get(2).await.unwrap(),
            Bytes::from_static(b"old-stage")
        );
        assert_eq!(
            old_response.get(2).await.unwrap(),
            Bytes::from_static(b"old-response")
        );
        first_stream.close().await;

        let second_stream = service.open_stream().await.unwrap();
        let second_id = second_stream.stream_id();
        assert_ne!(first_id, second_id);

        assert_eq!(service.request_last(second_id), Some(0));
        assert_eq!(service.response_last(second_id), Some(0));
        assert_eq!(service.stage_last(second_id, "decode").await, Some(0));
        assert!(service.request_get(second_id, 1).await.is_none());
        assert!(service.request_get(second_id, 2).await.is_none());
        assert!(service.stage_get(second_id, "decode", 1).await.is_none());
        assert!(service.stage_get(second_id, "decode", 2).await.is_none());
        assert!(service.response_get(second_id, 1).await.is_none());
        assert!(service.response_get(second_id, 2).await.is_none());

        service
            .write_request_headers(
                second_id,
                StreamHeaders::Request(StreamRequestHeaders {
                    stream_id: second_id,
                    version: http_pack::HttpVersion::Http11,
                    method: b"POST".to_vec(),
                    scheme: None,
                    authority: Some(b"example.com".to_vec()),
                    path: b"/upload".to_vec(),
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        service
            .append_request_body(second_id, Bytes::from_static(b"new-request"))
            .await
            .unwrap();
        service
            .write_stage_head(second_id, "decode", Bytes::from_static(b"new-head"))
            .await
            .unwrap();
        service
            .append_stage_body(second_id, "decode", Bytes::from_static(b"new-stage"))
            .await
            .unwrap();
        service
            .write_response_headers(
                second_id,
                StreamHeaders::Response(StreamResponseHeaders {
                    stream_id: second_id,
                    version: http_pack::HttpVersion::Http11,
                    status: 200,
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        service
            .append_response_body(second_id, Bytes::from_static(b"new-response"))
            .await
            .unwrap();

        assert!(!old_request.is_current());
        assert!(!old_stage.is_current());
        assert!(!old_response.is_current());
        assert_eq!(old_request.last(), None);
        assert_eq!(old_stage.last(), None);
        assert_eq!(old_response.last(), None);
        assert!(old_request.get(2).await.is_none());
        assert!(old_stage.get(2).await.is_none());
        assert!(old_response.get(2).await.is_none());
        assert!(old_request.update_notifier().is_none());

        let new_request = service.request_lane_handle(second_id).unwrap();
        let new_stage = service
            .stage_lane_handle(second_id, "decode")
            .await
            .unwrap()
            .unwrap();
        let new_response = service.response_lane_handle(second_id).unwrap();
        assert_eq!(new_request.last(), Some(2));
        assert_eq!(new_stage.last(), Some(2));
        assert_eq!(new_response.last(), Some(2));
        assert_eq!(
            new_request.get(2).await.unwrap(),
            Bytes::from_static(b"new-request")
        );
        assert_eq!(
            new_stage.get(2).await.unwrap(),
            Bytes::from_static(b"new-stage")
        );
        assert_eq!(
            new_response.get(2).await.unwrap(),
            Bytes::from_static(b"new-response")
        );
        assert!(new_response.update_notifier().is_some());

        second_stream.close().await;
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
    async fn test_request_backpressure_waits_for_reader_progress() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 3,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

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
        assert!(service.register_request_reader(stream_id, "worker-1").await);

        service
            .append_request_body(stream_id, Bytes::from_static(b"a"))
            .await
            .unwrap();
        service
            .append_request_body(stream_id, Bytes::from_static(b"b"))
            .await
            .unwrap();

        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_request_body(stream_id, Bytes::from_static(b"c"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());

        assert!(
            service
                .mark_request_reader_position(stream_id, "worker-1", 1)
                .await
        );
        timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            service.request_get(stream_id, 4).await.unwrap(),
            Bytes::from_static(b"c")
        );

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_request_backpressure_waits_for_first_reader() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 2,
            response_timeout_ms: 500,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();
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
            .append_request_body(stream_id, Bytes::from_static(b"a"))
            .await
            .unwrap();
        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_request_body(stream_id, Bytes::from_static(b"b"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());
        assert!(service.request_get(stream_id, 1).await.is_some());
        assert!(service.register_request_reader(stream_id, "worker-1").await);
        assert!(
            service
                .mark_request_reader_position(stream_id, "worker-1", 1)
                .await
        );
        timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_stage_backpressure_waits_for_reader_progress() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 3,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        service
            .write_stage_head(stream_id, "pcm", Bytes::from_static(b"head"))
            .await
            .unwrap();
        assert!(
            service
                .register_stage_reader(stream_id, "pcm", "worker-1")
                .await
        );

        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"a"))
            .await
            .unwrap();
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"b"))
            .await
            .unwrap();

        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_stage_body(stream_id, "pcm", Bytes::from_static(b"c"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());

        assert!(
            service
                .mark_stage_reader_position(stream_id, "pcm", "worker-1", 1)
                .await
        );
        timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            service.stage_get(stream_id, "pcm", 4).await.unwrap(),
            Bytes::from_static(b"c")
        );

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_stage_backpressure_waits_for_first_reader() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 3,
            response_timeout_ms: 500,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        service
            .write_stage_head(stream_id, "pcm", Bytes::from_static(b"head"))
            .await
            .unwrap();
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"a"))
            .await
            .unwrap();
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"b"))
            .await
            .unwrap();

        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_stage_body(stream_id, "pcm", Bytes::from_static(b"c"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());
        assert_eq!(
            service.stage_get(stream_id, "pcm", 1).await.unwrap(),
            Bytes::from_static(b"head")
        );

        assert!(
            service
                .register_stage_reader(stream_id, "pcm", "worker-1")
                .await
        );
        assert!(
            service
                .mark_stage_reader_position(stream_id, "pcm", "worker-1", 1)
                .await
        );
        timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            service.stage_get(stream_id, "pcm", 4).await.unwrap(),
            Bytes::from_static(b"c")
        );

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_backpressure_uses_slowest_reader() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 3,
            response_timeout_ms: 500,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        service
            .write_stage_head(stream_id, "pcm", Bytes::from_static(b"head"))
            .await
            .unwrap();
        assert!(
            service
                .register_stage_reader(stream_id, "pcm", "fast")
                .await
        );
        assert!(
            service
                .register_stage_reader(stream_id, "pcm", "slow")
                .await
        );
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"a"))
            .await
            .unwrap();
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"b"))
            .await
            .unwrap();

        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_stage_body(stream_id, "pcm", Bytes::from_static(b"c"))
                .await
        });

        assert!(
            service
                .mark_stage_reader_position(stream_id, "pcm", "fast", 1)
                .await
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());
        assert!(
            service
                .mark_stage_reader_position(stream_id, "pcm", "slow", 1)
                .await
        );
        timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_backpressure_times_out_without_reader_and_preserves_slots() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 2,
            response_timeout_ms: 1_000,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new_with_timeouts(
            config,
            UploadResponseTimeouts {
                reader_backpressure_timeout_ms: 30,
                ..Default::default()
            },
        ));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        service
            .write_stage_head(stream_id, "pcm", Bytes::from_static(b"head"))
            .await
            .unwrap();
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"body"))
            .await
            .unwrap();
        let error = service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"overwrite"))
            .await
            .unwrap_err();

        assert!(error.contains("buffer capacity wait timed out"));
        assert_eq!(
            service.stage_get(stream_id, "pcm", 1).await.unwrap(),
            Bytes::from_static(b"head")
        );
        assert_eq!(
            service.stage_get(stream_id, "pcm", 2).await.unwrap(),
            Bytes::from_static(b"body")
        );

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_backpressure_wakes_when_stream_closes() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 2,
            response_timeout_ms: 1_000,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        service
            .write_stage_head(stream_id, "pcm", Bytes::from_static(b"head"))
            .await
            .unwrap();
        service
            .append_stage_body(stream_id, "pcm", Bytes::from_static(b"body"))
            .await
            .unwrap();
        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_stage_body(stream_id, "pcm", Bytes::from_static(b"blocked"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());
        upload_stream.close().await;
        let error = timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.contains("stream closed"));
    }

    #[tokio::test]
    async fn test_response_watcher_drains_small_ring_without_loss() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 3,
            response_timeout_ms: 500,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let watcher = ResponseWatcher::new(Arc::clone(&service))
            .with_poll_interval_ms(1)
            .spawn();
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();
        let rx = service.register_response(stream_id).await;

        service
            .write_response_headers(
                stream_id,
                StreamHeaders::Response(StreamResponseHeaders {
                    stream_id,
                    version: http_pack::HttpVersion::Http11,
                    status: 200,
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        for byte in b"complete-response" {
            service
                .append_response_body(stream_id, Bytes::copy_from_slice(&[*byte]))
                .await
                .unwrap();
        }
        service.end_response(stream_id).await.unwrap();

        let response = timeout(Duration::from_millis(500), rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.body, Bytes::from_static(b"complete-response"));

        upload_stream.close().await;
        watcher.abort();
    }

    #[tokio::test]
    async fn test_response_backpressure_waits_for_first_reader() {
        let config = UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 2,
            response_timeout_ms: 500,
            ..Default::default()
        };
        let service = Arc::new(UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        service
            .write_response_headers(
                stream_id,
                StreamHeaders::Response(StreamResponseHeaders {
                    stream_id,
                    version: http_pack::HttpVersion::Http11,
                    status: 200,
                    headers: vec![],
                }),
            )
            .await
            .unwrap();
        service
            .append_response_body(stream_id, Bytes::from_static(b"a"))
            .await
            .unwrap();
        let append_service = Arc::clone(&service);
        let append = tokio::spawn(async move {
            append_service
                .append_response_body(stream_id, Bytes::from_static(b"b"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!append.is_finished());
        assert!(service.response_get(stream_id, 1).await.is_some());
        assert!(service.register_response_reader(stream_id, "client").await);
        assert!(
            service
                .mark_response_reader_position(stream_id, "client", 1)
                .await
        );
        timeout(Duration::from_millis(250), append)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_duplicate_response_reader_registration_preserves_progress() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            ..Default::default()
        }));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();
        let stream_idx = upload_stream.stream_idx();

        assert!(service.register_response_reader(stream_id, "client").await);
        assert!(
            service
                .mark_response_reader_position(stream_id, "client", 5)
                .await
        );
        assert!(!service.register_response_reader(stream_id, "client").await);
        let positions = service.response_reader_positions.read().await;
        assert_eq!(positions[stream_idx].get("client"), Some(&5));
        drop(positions);

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn test_response_channel_roundtrip() {
        let config = UploadResponseConfig::default();
        let service = Arc::new(UploadResponseService::new(config));

        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();
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
        upload_stream.close().await;
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
    async fn response_capability_fences_writers_retries_and_slot_reuse() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slots_per_stream: 16,
            ..Default::default()
        }));
        let first_stream = service.open_stream().await.unwrap();
        let first_id = first_stream.stream_id();
        let capability = service
            .try_claim_response_with_capability(first_id, "writer-1")
            .await
            .unwrap()
            .unwrap();
        let headers = || {
            StreamHeaders::Response(StreamResponseHeaders {
                stream_id: first_id,
                version: http_pack::HttpVersion::Http11,
                status: 200,
                headers: vec![],
            })
        };

        let wrong_capability = service
            .write_response_headers_claimed(first_id, "wrong", 1, headers())
            .await
            .unwrap_err();
        assert!(wrong_capability.contains("invalid response capability"));

        service
            .write_response_headers_claimed(first_id, &capability, 1, headers())
            .await
            .unwrap();
        service
            .write_response_headers_claimed(first_id, &capability, 1, headers())
            .await
            .unwrap();
        let conflicting_retry = service
            .append_response_body_claimed(
                first_id,
                &capability,
                1,
                Bytes::from_static(b"different"),
            )
            .await
            .unwrap_err();
        assert!(conflicting_retry.contains("conflicting response retry"));
        let skipped_sequence = service
            .append_response_body_claimed(first_id, &capability, 3, Bytes::from_static(b"body"))
            .await
            .unwrap_err();
        assert!(skipped_sequence.contains("expected 2"));

        service
            .append_response_body_claimed(first_id, &capability, 2, Bytes::from_static(b"body"))
            .await
            .unwrap();
        service
            .append_response_body_claimed(first_id, &capability, 2, Bytes::from_static(b"body"))
            .await
            .unwrap();
        assert_eq!(service.response_last(first_id), Some(2));
        service
            .end_response_claimed(first_id, &capability, 3)
            .await
            .unwrap();

        first_stream.close().await;
        let second_stream = service.open_stream().await.unwrap();
        let second_id = second_stream.stream_id();
        assert_ne!(first_id, second_id);
        let stale = service
            .write_response_headers_claimed(second_id, &capability, 1, headers())
            .await
            .unwrap_err();
        assert!(stale.contains("capability required"));
        second_stream.close().await;
    }

    #[tokio::test]
    async fn response_backpressure_does_not_block_unrelated_claims() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 2,
            slots_per_stream: 1,
            response_timeout_ms: 1_000,
            ..Default::default()
        }));
        let first_stream = service.open_stream().await.unwrap();
        let second_stream = service.open_stream().await.unwrap();
        let first_id = first_stream.stream_id();
        let second_id = second_stream.stream_id();
        let first_capability = service
            .try_claim_response_with_capability(first_id, "writer-1")
            .await
            .unwrap()
            .unwrap();
        let second_capability = service
            .try_claim_response_with_capability(second_id, "writer-2")
            .await
            .unwrap()
            .unwrap();
        let headers = |stream_id| {
            StreamHeaders::Response(StreamResponseHeaders {
                stream_id,
                version: http_pack::HttpVersion::Http11,
                status: 200,
                headers: vec![],
            })
        };

        assert!(service.register_response_reader(first_id, "reader-1").await);
        assert!(
            service
                .register_response_reader(second_id, "reader-2")
                .await
        );
        service
            .write_response_headers_claimed(first_id, &first_capability, 1, headers(first_id))
            .await
            .unwrap();
        service
            .write_response_headers_claimed(second_id, &second_capability, 1, headers(second_id))
            .await
            .unwrap();

        let blocked_service = Arc::clone(&service);
        let blocked = tokio::spawn(async move {
            blocked_service
                .append_response_body_claimed(
                    first_id,
                    &first_capability,
                    2,
                    Bytes::from_static(b"first"),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());

        assert!(
            service
                .mark_response_reader_position(second_id, "reader-2", 1)
                .await
        );
        timeout(
            Duration::from_millis(250),
            service.append_response_body_claimed(
                second_id,
                &second_capability,
                2,
                Bytes::from_static(b"second"),
            ),
        )
        .await
        .expect("unrelated response write must not wait for the first stream")
        .unwrap();

        assert!(
            service
                .mark_response_reader_position(first_id, "reader-1", 1)
                .await
        );
        blocked.await.unwrap().unwrap();
        first_stream.close().await;
        second_stream.close().await;
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
