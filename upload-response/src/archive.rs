use async_trait::async_trait;
use bytes::Bytes;
use getrandom::fill as fill_random;
use http::{header, Method, Request, StatusCode};
use memmap2::MmapOptions;
use playlists::{chunk_cache::ChunkCache, Options};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::sync::{Mutex, Notify};
use tokio::time::{timeout_at, Instant};
use web_service::{
    HandlerResponse, HandlerResult, Router, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const ARCHIVE_CHUNK_MAGIC: &[u8; 4] = b"IAR1";
const ARCHIVE_CHUNK_VERSION: u16 = 1;
const ARCHIVE_CHUNK_HEADER_BYTES: usize = 52;
const ARCHIVE_ID_FILE: &str = ".archive-id";
const ARCHIVE_MANIFEST_LOG: &str = ".archive-manifest-v1.ndjson";
const ARCHIVE_MANIFEST_VERSION: u16 = 1;
const ARCHIVE_HOT_SLOTS: usize = 64;
const ARCHIVE_HOT_MAX_CHUNK_KB: usize = 64 * 1024;
const ARCHIVE_CACHE_LOCK_SHARDS: usize = 64;
const DEFAULT_MANIFEST_LIMIT: usize = 256;
const MAX_MANIFEST_LIMIT: usize = 1_024;
const MAX_MANIFEST_WAIT_SECONDS: u64 = 25;

#[derive(Debug, Error)]
pub enum ArchiveStoreError {
    #[error("archive I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("archive metadata is invalid: {0}")]
    Invalid(String),
    #[error("archive manifest serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("archive cache configuration failed: {0}")]
    Cache(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveChunkDescriptor {
    pub sequence: u64,
    pub sample_position: i64,
    pub transport_generation: u32,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub payload_bytes: u32,
    pub channel_count: u16,
    pub bits_per_sample: u8,
    pub discontinuity: bool,
    pub available_unix_ms: u64,
    pub object_bytes: u64,
    pub sha256: String,
    pub object_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub version: u16,
    pub archive_id: String,
    pub first_available_sequence: Option<u64>,
    pub last_available_sequence: Option<u64>,
    pub next_after: u64,
    pub finalized: bool,
    pub chunks: Vec<ArchiveChunkDescriptor>,
}

pub struct ArchiveStore {
    directory: PathBuf,
    archive_id: String,
    chunks: StdRwLock<BTreeMap<u64, ArchiveChunkDescriptor>>,
    manifest_log: StdMutex<()>,
    hot_cache: Arc<ChunkCache>,
    cache_miss_locks: [Mutex<()>; ARCHIVE_CACHE_LOCK_SHARDS],
    finalized: AtomicBool,
    update: Notify,
}

impl std::fmt::Debug for ArchiveStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArchiveStore")
            .field("directory", &self.directory)
            .field("archive_id", &self.archive_id)
            .field(
                "chunk_count",
                &self
                    .chunks
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .field("finalized", &self.is_finalized())
            .finish_non_exhaustive()
    }
}

impl ArchiveStore {
    pub fn open(directory: impl Into<PathBuf>) -> Result<Arc<Self>, ArchiveStoreError> {
        let directory = directory.into();
        if directory.as_os_str().is_empty() {
            return Err(ArchiveStoreError::Invalid(
                "archive directory is empty".to_string(),
            ));
        }
        fs::create_dir_all(&directory)?;
        let archive_id = load_or_create_archive_id(&directory)?;
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: ARCHIVE_HOT_SLOTS,
            buffer_size_kb: ARCHIVE_HOT_MAX_CHUNK_KB,
            ..Options::default()
        };
        let hot_cache = Arc::new(
            ChunkCache::try_new(options)
                .map_err(|error| ArchiveStoreError::Cache(error.to_string()))?,
        );
        let store = Arc::new(Self {
            directory,
            archive_id,
            chunks: StdRwLock::new(BTreeMap::new()),
            manifest_log: StdMutex::new(()),
            hot_cache,
            cache_miss_locks: std::array::from_fn(|_| Mutex::new(())),
            finalized: AtomicBool::new(false),
            update: Notify::new(),
        });
        store.recover()?;
        Ok(store)
    }

    pub fn archive_id(&self) -> &str {
        &self.archive_id
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn mark_active(&self) {
        self.finalized.store(false, Ordering::Release);
        self.update.notify_waiters();
    }

    pub fn mark_finalized(&self) {
        self.finalized.store(true, Ordering::Release);
        self.update.notify_waiters();
    }

    pub fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    pub fn publish_iarc(
        self: &Arc<Self>,
        sequence: u64,
    ) -> Result<ArchiveChunkDescriptor, ArchiveStoreError> {
        let path = self.chunk_path(sequence);
        let bytes = map_file(&path)?;
        let mut descriptor = descriptor_from_iarc(sequence, &bytes)?;
        descriptor.object_path = self.object_path(sequence);

        let inserted = {
            let mut chunks = self
                .chunks
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match chunks.get(&sequence) {
                Some(existing) if existing == &descriptor => false,
                Some(_) => {
                    return Err(ArchiveStoreError::Invalid(format!(
                        "archive sequence {sequence} conflicts with its published descriptor"
                    )))
                }
                None => {
                    chunks.insert(sequence, descriptor.clone());
                    true
                }
            }
        };

        if inserted {
            let log_result = self.append_manifest_record(&descriptor);
            self.preheat(sequence, bytes);
            self.update.notify_waiters();
            log_result?;
        }
        Ok(descriptor)
    }

    pub fn manifest(&self, after: u64, limit: usize) -> ArchiveManifest {
        let chunks = self
            .chunks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let first_available_sequence = chunks.first_key_value().map(|(sequence, _)| *sequence);
        let last_available_sequence = chunks.last_key_value().map(|(sequence, _)| *sequence);
        let selected = chunks
            .range((Bound::Excluded(after), Bound::Unbounded))
            .take(limit.clamp(1, MAX_MANIFEST_LIMIT))
            .map(|(_, descriptor)| descriptor.clone())
            .collect::<Vec<_>>();
        let next_after = selected
            .last()
            .map(|descriptor| descriptor.sequence)
            .unwrap_or(after);
        ArchiveManifest {
            version: ARCHIVE_MANIFEST_VERSION,
            archive_id: self.archive_id.clone(),
            first_available_sequence,
            last_available_sequence,
            next_after,
            finalized: self.is_finalized(),
            chunks: selected,
        }
    }

    pub async fn wait_manifest(&self, after: u64, limit: usize, wait: Duration) -> ArchiveManifest {
        let wait = wait.min(Duration::from_secs(MAX_MANIFEST_WAIT_SECONDS));
        let deadline = Instant::now() + wait;
        loop {
            let notified = self.update.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let manifest = self.manifest(after, limit);
            if !manifest.chunks.is_empty() || manifest.finalized || wait.is_zero() {
                return manifest;
            }
            if timeout_at(deadline, notified).await.is_err() {
                return self.manifest(after, limit);
            }
        }
    }

    pub fn descriptor(&self, sequence: u64) -> Option<ArchiveChunkDescriptor> {
        self.chunks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&sequence)
            .cloned()
    }

    pub async fn load_chunk(&self, sequence: u64) -> Result<Option<Bytes>, ArchiveStoreError> {
        let Some(descriptor) = self.descriptor(sequence) else {
            return Ok(None);
        };
        let Some(slot_id) = usize::try_from(sequence).ok() else {
            return self
                .map_chunk(sequence, descriptor.object_bytes)
                .await
                .map(Some);
        };
        if let Some((bytes, _)) = self.hot_cache.get(0, slot_id).await {
            if bytes.len() as u64 == descriptor.object_bytes {
                return Ok(Some(bytes));
            }
        }

        let shard = sequence as usize % ARCHIVE_CACHE_LOCK_SHARDS;
        let _guard = self.cache_miss_locks[shard].lock().await;
        if let Some((bytes, _)) = self.hot_cache.get(0, slot_id).await {
            if bytes.len() as u64 == descriptor.object_bytes {
                return Ok(Some(bytes));
            }
        }
        let bytes = self.map_chunk(sequence, descriptor.object_bytes).await?;
        let _ = self.hot_cache.set(0, slot_id, bytes.clone()).await;
        Ok(Some(bytes))
    }

    fn recover(&self) -> Result<(), ArchiveStoreError> {
        self.load_manifest_log()?;
        let mut recovered = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("iarc") {
                continue;
            }
            let Some(sequence) = path
                .file_stem()
                .and_then(|value| value.to_str())
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            if self
                .chunks
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&sequence)
            {
                continue;
            }
            let bytes = map_file(&path)?;
            match descriptor_from_iarc(sequence, &bytes) {
                Ok(mut descriptor) => {
                    descriptor.object_path = self.object_path(sequence);
                    recovered.push(descriptor);
                }
                Err(error) => {
                    tracing::warn!(sequence, %error, "ignoring invalid archive object during recovery");
                }
            }
        }
        recovered.sort_by_key(|descriptor| descriptor.sequence);
        for descriptor in recovered {
            self.append_manifest_record(&descriptor)?;
            self.chunks
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(descriptor.sequence, descriptor);
        }
        Ok(())
    }

    fn load_manifest_log(&self) -> Result<(), ArchiveStoreError> {
        let path = self.directory.join(ARCHIVE_MANIFEST_LOG);
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let mut chunks = self
            .chunks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let Ok(descriptor) = serde_json::from_str::<ArchiveChunkDescriptor>(&line) else {
                continue;
            };
            if validate_logged_descriptor(&self.directory, &self.archive_id, &descriptor) {
                chunks.entry(descriptor.sequence).or_insert(descriptor);
            }
        }
        Ok(())
    }

    fn append_manifest_record(
        &self,
        descriptor: &ArchiveChunkDescriptor,
    ) -> Result<(), ArchiveStoreError> {
        let _guard = self
            .manifest_log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut encoded = serde_json::to_vec(descriptor)?;
        encoded.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join(ARCHIVE_MANIFEST_LOG))?;
        file.write_all(&encoded)?;
        file.sync_data()?;
        Ok(())
    }

    fn preheat(self: &Arc<Self>, sequence: u64, bytes: Bytes) {
        let Ok(slot_id) = usize::try_from(sequence) else {
            return;
        };
        let cache = Arc::clone(&self.hot_cache);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = cache.add(0, slot_id, bytes).await;
            });
        }
    }

    async fn map_chunk(
        &self,
        sequence: u64,
        expected_bytes: u64,
    ) -> Result<Bytes, ArchiveStoreError> {
        let path = self.chunk_path(sequence);
        let bytes = tokio::task::spawn_blocking(move || map_file(&path))
            .await
            .map_err(|error| {
                ArchiveStoreError::Invalid(format!("archive map task failed: {error}"))
            })??;
        if bytes.len() as u64 != expected_bytes {
            return Err(ArchiveStoreError::Invalid(format!(
                "archive object {sequence} changed length"
            )));
        }
        Ok(bytes)
    }

    fn chunk_path(&self, sequence: u64) -> PathBuf {
        self.directory.join(format!("{sequence:020}.iarc"))
    }

    fn object_path(&self, sequence: u64) -> String {
        format!("/v1/archives/{}/chunks/{sequence}", self.archive_id)
    }
}

pub struct ArchiveHttpRouter {
    store: Arc<ArchiveStore>,
    bearer_digest: [u8; 32],
}

impl ArchiveHttpRouter {
    pub fn new(store: Arc<ArchiveStore>, bearer_token: &str) -> Result<Self, ArchiveStoreError> {
        if bearer_token.is_empty() || bearer_token.len() > 4_096 {
            return Err(ArchiveStoreError::Invalid(
                "archive bearer token length is invalid".to_string(),
            ));
        }
        Ok(Self {
            store,
            bearer_digest: Sha256::digest(bearer_token.as_bytes()).into(),
        })
    }

    pub fn store(&self) -> Arc<ArchiveStore> {
        Arc::clone(&self.store)
    }

    fn authorized(&self, request: &Request<()>) -> bool {
        let Some(value) = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        let candidate: [u8; 32] = Sha256::digest(value.as_bytes()).into();
        bool::from(candidate.ct_eq(&self.bearer_digest))
    }

    fn unauthorized() -> HandlerResponse {
        HandlerResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: vec![(
                Cow::Borrowed(header::WWW_AUTHENTICATE.as_str()),
                Cow::Borrowed("Bearer"),
            )],
            ..HandlerResponse::default()
        }
    }

    fn not_found() -> HandlerResponse {
        HandlerResponse {
            status: StatusCode::NOT_FOUND,
            ..HandlerResponse::default()
        }
    }

    fn bad_request(message: impl Into<String>) -> HandlerResponse {
        HandlerResponse {
            status: StatusCode::BAD_REQUEST,
            body: Some(Bytes::from(message.into())),
            content_type: Some(Cow::Borrowed("text/plain; charset=utf-8")),
            ..HandlerResponse::default()
        }
    }

    fn not_modified(etag: String) -> HandlerResponse {
        HandlerResponse {
            status: StatusCode::NOT_MODIFIED,
            headers: vec![(Cow::Borrowed(header::ETAG.as_str()), Cow::Owned(etag))],
            ..HandlerResponse::default()
        }
    }

    fn if_none_match(request: &Request<()>, etag: &str) -> bool {
        request
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(',').any(|value| value.trim() == etag))
    }

    fn if_range_matches(request: &Request<()>, etag: &str) -> bool {
        request
            .headers()
            .get(header::IF_RANGE)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| value.trim() == etag)
    }

    async fn manifest_response(&self, request: &Request<()>) -> HandlerResponse {
        let (after, limit, wait) = match parse_manifest_query(request.uri().query()) {
            Ok(query) => query,
            Err(error) => return Self::bad_request(error),
        };
        let manifest = self
            .store
            .wait_manifest(after, limit, Duration::from_secs(wait))
            .await;
        let body = match serde_json::to_vec(&manifest) {
            Ok(body) => Bytes::from(body),
            Err(error) => {
                return Self::bad_request(format!("manifest encoding failed: {error}"));
            }
        };
        let etag = quoted_sha256(&body);
        if Self::if_none_match(request, &etag) {
            return Self::not_modified(etag);
        }
        let is_head = request.method() == Method::HEAD;
        HandlerResponse {
            status: StatusCode::OK,
            body: (!is_head).then_some(body.clone()),
            content_type: Some(Cow::Borrowed("application/json")),
            headers: vec![
                (Cow::Borrowed(header::ETAG.as_str()), Cow::Owned(etag)),
                (
                    Cow::Borrowed(header::CACHE_CONTROL.as_str()),
                    Cow::Borrowed("no-store"),
                ),
                (
                    Cow::Borrowed(header::CONTENT_LENGTH.as_str()),
                    Cow::Owned(body.len().to_string()),
                ),
            ],
            etag: None,
        }
    }

    async fn chunk_response(&self, request: &Request<()>, sequence: u64) -> HandlerResponse {
        let Some(descriptor) = self.store.descriptor(sequence) else {
            return Self::not_found();
        };
        let etag = format!("\"{}\"", descriptor.sha256);
        if Self::if_none_match(request, &etag) {
            return Self::not_modified(etag);
        }
        let is_head = request.method() == Method::HEAD;
        let body = if is_head {
            None
        } else {
            match self.store.load_chunk(sequence).await {
                Ok(Some(bytes)) => Some(bytes),
                Ok(None) => return Self::not_found(),
                Err(_) => {
                    return HandlerResponse {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        ..HandlerResponse::default()
                    }
                }
            }
        };
        let mut headers = vec![
            (
                Cow::Borrowed(header::ETAG.as_str()),
                Cow::Owned(etag.clone()),
            ),
            (
                Cow::Borrowed(header::CACHE_CONTROL.as_str()),
                Cow::Borrowed("private, max-age=31536000, immutable"),
            ),
            (
                Cow::Borrowed(header::CONTENT_LENGTH.as_str()),
                Cow::Owned(descriptor.object_bytes.to_string()),
            ),
        ];
        if !Self::if_range_matches(request, &etag) {
            headers.push((
                Cow::Borrowed(header::ACCEPT_RANGES.as_str()),
                Cow::Borrowed("none"),
            ));
        }
        HandlerResponse {
            status: StatusCode::OK,
            body,
            content_type: Some(Cow::Borrowed("application/vnd.infidelity.archive-chunk")),
            headers,
            etag: None,
        }
    }
}

#[async_trait]
impl Router for ArchiveHttpRouter {
    async fn route(&self, request: Request<()>) -> HandlerResult<HandlerResponse> {
        if request.method() == Method::OPTIONS {
            return Ok(HandlerResponse {
                status: StatusCode::NO_CONTENT,
                headers: vec![
                    (Cow::Borrowed("allow"), Cow::Borrowed("GET, HEAD, OPTIONS")),
                    (
                        Cow::Borrowed("access-control-allow-headers"),
                        Cow::Borrowed("authorization, if-none-match, if-range, range"),
                    ),
                ],
                ..HandlerResponse::default()
            });
        }
        if !matches!(*request.method(), Method::GET | Method::HEAD) {
            return Ok(HandlerResponse {
                status: StatusCode::METHOD_NOT_ALLOWED,
                headers: vec![(Cow::Borrowed("allow"), Cow::Borrowed("GET, HEAD, OPTIONS"))],
                ..HandlerResponse::default()
            });
        }
        if !self.authorized(&request) {
            return Ok(Self::unauthorized());
        }

        let path = request
            .uri()
            .path()
            .trim_matches('/')
            .split('/')
            .collect::<Vec<_>>();
        let response = match path.as_slice() {
            ["v1", "archives", archive_id, "manifest"]
                if *archive_id == self.store.archive_id() =>
            {
                self.manifest_response(&request).await
            }
            ["v1", "archives", archive_id, "chunks", sequence]
                if *archive_id == self.store.archive_id() =>
            {
                match sequence.parse::<u64>() {
                    Ok(sequence) if sequence > 0 => self.chunk_response(&request, sequence).await,
                    _ => Self::bad_request("chunk sequence is invalid"),
                }
            }
            _ => Self::not_found(),
        };
        Ok(response)
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _request: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config(
            "archive streaming responses are not enabled".to_string(),
        ))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

fn parse_manifest_query(query: Option<&str>) -> Result<(u64, usize, u64), String> {
    let mut after = 0u64;
    let mut limit = DEFAULT_MANIFEST_LIMIT;
    let mut wait = 0u64;
    for component in query.unwrap_or_default().split('&') {
        if component.is_empty() {
            continue;
        }
        let Some((name, value)) = component.split_once('=') else {
            return Err("manifest query is invalid".to_string());
        };
        match name {
            "after" => {
                after = value
                    .parse::<u64>()
                    .map_err(|_| "manifest after value is invalid".to_string())?;
            }
            "limit" => {
                limit = value
                    .parse::<usize>()
                    .ok()
                    .filter(|value| (1..=MAX_MANIFEST_LIMIT).contains(value))
                    .ok_or_else(|| "manifest limit is invalid".to_string())?;
            }
            "wait" => {
                wait = value
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value <= MAX_MANIFEST_WAIT_SECONDS)
                    .ok_or_else(|| "manifest wait is invalid".to_string())?;
            }
            _ => return Err(format!("unknown manifest query field: {name}")),
        }
    }
    Ok((after, limit, wait))
}

fn load_or_create_archive_id(directory: &Path) -> Result<String, ArchiveStoreError> {
    let path = directory.join(ARCHIVE_ID_FILE);
    match fs::read_to_string(&path) {
        Ok(value) => {
            let value = value.trim();
            if value.len() == 32
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Ok(value.to_string());
            }
            return Err(ArchiveStoreError::Invalid(
                "persisted archive ID is invalid".to_string(),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let mut random = [0u8; 16];
    fill_random(&mut random).map_err(|error| {
        ArchiveStoreError::Invalid(format!("archive ID generation failed: {error}"))
    })?;
    let archive_id = hex(&random);
    let partial = directory.join(format!("{ARCHIVE_ID_FILE}.partial"));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial)?;
    file.write_all(archive_id.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(partial, path)?;
    File::open(directory)?.sync_all()?;
    Ok(archive_id)
}

fn descriptor_from_iarc(
    expected_sequence: u64,
    bytes: &[u8],
) -> Result<ArchiveChunkDescriptor, ArchiveStoreError> {
    if bytes.len() < ARCHIVE_CHUNK_HEADER_BYTES || &bytes[..4] != ARCHIVE_CHUNK_MAGIC {
        return Err(ArchiveStoreError::Invalid(
            "archive chunk header is invalid".to_string(),
        ));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    let header_bytes = u16::from_le_bytes(bytes[6..8].try_into().unwrap()) as usize;
    if version != ARCHIVE_CHUNK_VERSION || header_bytes != ARCHIVE_CHUNK_HEADER_BYTES {
        return Err(ArchiveStoreError::Invalid(
            "archive chunk version is unsupported".to_string(),
        ));
    }
    let sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if sequence == 0 || sequence != expected_sequence {
        return Err(ArchiveStoreError::Invalid(
            "archive chunk sequence does not match its object name".to_string(),
        ));
    }
    let sample_position = i64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let transport_generation = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    let sample_rate = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    let frame_count = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
    let payload_bytes = u32::from_le_bytes(bytes[36..40].try_into().unwrap());
    let channel_count = u16::from_le_bytes(bytes[40..42].try_into().unwrap());
    let bits_per_sample = bytes[42];
    let flags = bytes[43];
    let available_unix_ms = u64::from_le_bytes(bytes[44..52].try_into().unwrap());
    let expected_object_bytes = ARCHIVE_CHUNK_HEADER_BYTES
        .checked_add(payload_bytes as usize)
        .ok_or_else(|| ArchiveStoreError::Invalid("archive chunk length overflow".to_string()))?;
    if bytes.len() != expected_object_bytes
        || transport_generation == 0
        || sample_rate == 0
        || frame_count == 0
        || channel_count == 0
        || bits_per_sample != 24
        || flags & !1 != 0
    {
        return Err(ArchiveStoreError::Invalid(
            "archive chunk fields are invalid".to_string(),
        ));
    }
    let expected_payload = usize::try_from(frame_count)
        .ok()
        .and_then(|frames| frames.checked_mul(usize::from(channel_count)))
        .and_then(|samples| samples.checked_mul(3));
    if expected_payload != Some(payload_bytes as usize) {
        return Err(ArchiveStoreError::Invalid(
            "archive chunk PCM dimensions are invalid".to_string(),
        ));
    }
    Ok(ArchiveChunkDescriptor {
        sequence,
        sample_position,
        transport_generation,
        sample_rate,
        frame_count,
        payload_bytes,
        channel_count,
        bits_per_sample,
        discontinuity: flags & 1 != 0,
        available_unix_ms,
        object_bytes: bytes.len() as u64,
        sha256: hex(&Sha256::digest(bytes)),
        object_path: String::new(),
    })
}

fn validate_logged_descriptor(
    directory: &Path,
    archive_id: &str,
    descriptor: &ArchiveChunkDescriptor,
) -> bool {
    if descriptor.sequence == 0
        || descriptor.sha256.len() != 64
        || descriptor.object_path
            != format!("/v1/archives/{archive_id}/chunks/{}", descriptor.sequence)
    {
        return false;
    }
    let path = directory.join(format!("{:020}.iarc", descriptor.sequence));
    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.len() == descriptor.object_bytes)
        .unwrap_or(false)
}

fn map_file(path: &Path) -> io::Result<Bytes> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "archive object is empty",
        ));
    }
    // The spool never changes a committed .iarc file. The mapping therefore
    // stays immutable for its complete lifetime.
    let mapping = unsafe { MmapOptions::new().map(&file)? };
    Ok(Bytes::from_owner(mapping))
}

fn quoted_sha256(bytes: &[u8]) -> String {
    format!("\"{}\"", hex(&Sha256::digest(bytes)))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[usize::from(byte >> 4)] as char);
        encoded.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn unique_directory() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "upload-response-archive-{}-{stamp}-{}",
            std::process::id(),
            DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn iarc(sequence: u64, payload_byte: u8) -> Bytes {
        let sample_rate = 8u32;
        let channels = 2u16;
        let frames = 4u32;
        let payload = vec![payload_byte; frames as usize * channels as usize * 3];
        let mut bytes = Vec::with_capacity(ARCHIVE_CHUNK_HEADER_BYTES + payload.len());
        bytes.extend_from_slice(ARCHIVE_CHUNK_MAGIC);
        bytes.extend_from_slice(&ARCHIVE_CHUNK_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(ARCHIVE_CHUNK_HEADER_BYTES as u16).to_le_bytes());
        bytes.extend_from_slice(&sequence.to_le_bytes());
        bytes.extend_from_slice(&(sequence as i64 * 4).to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&sample_rate.to_le_bytes());
        bytes.extend_from_slice(&frames.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&channels.to_le_bytes());
        bytes.extend_from_slice(&[24, 0]);
        bytes.extend_from_slice(&1234u64.to_le_bytes());
        bytes.extend_from_slice(&payload);
        Bytes::from(bytes)
    }

    fn write_chunk(directory: &Path, sequence: u64, bytes: &[u8]) {
        fs::create_dir_all(directory).unwrap();
        fs::write(directory.join(format!("{sequence:020}.iarc")), bytes).unwrap();
    }

    fn request(method: Method, uri: String, token: Option<&str>) -> Request<()> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(()).unwrap()
    }

    #[test]
    fn store_recovers_committed_chunks_and_keeps_its_archive_id() {
        let directory = unique_directory();
        write_chunk(&directory, 1, &iarc(1, 7));
        fs::write(
            directory.join("00000000000000000002.partial"),
            b"incomplete",
        )
        .unwrap();

        let first = ArchiveStore::open(&directory).unwrap();
        let archive_id = first.archive_id().to_string();
        assert_eq!(first.manifest(0, 10).chunks.len(), 1);
        drop(first);

        let reopened = ArchiveStore::open(&directory).unwrap();
        assert_eq!(reopened.archive_id(), archive_id);
        assert_eq!(reopened.manifest(0, 10).chunks[0].sequence, 1);
        assert_eq!(
            reopened.manifest(u64::MAX, 10).chunks,
            Vec::<ArchiveChunkDescriptor>::new()
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn recovery_keeps_valid_objects_when_an_object_or_log_tail_is_corrupt() {
        let directory = unique_directory();
        write_chunk(&directory, 1, &iarc(1, 7));
        write_chunk(&directory, 2, b"not-an-archive-object");
        let first = ArchiveStore::open(&directory).unwrap();
        assert_eq!(first.manifest(0, 10).chunks.len(), 1);
        drop(first);
        let mut log = OpenOptions::new()
            .append(true)
            .open(directory.join(ARCHIVE_MANIFEST_LOG))
            .unwrap();
        log.write_all(b"{\"torn\":").unwrap();
        log.sync_all().unwrap();

        let reopened = ArchiveStore::open(&directory).unwrap();
        assert_eq!(reopened.manifest(0, 10).chunks.len(), 1);
        assert_eq!(reopened.manifest(0, 10).chunks[0].sequence, 1);
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn published_chunk_wakes_manifest_waiters_and_serves_shared_bytes() {
        let directory = unique_directory();
        let store = ArchiveStore::open(&directory).unwrap();
        let waiter_store = Arc::clone(&store);
        let waiter = tokio::spawn(async move {
            waiter_store
                .wait_manifest(0, 10, Duration::from_secs(2))
                .await
        });
        tokio::task::yield_now().await;

        let bytes = iarc(1, 9);
        write_chunk(&directory, 1, &bytes);
        store.publish_iarc(1).unwrap();
        let manifest = waiter.await.unwrap();
        assert_eq!(manifest.chunks.len(), 1);
        assert_eq!(manifest.chunks[0].sequence, 1);
        let loaded = store.load_chunk(1).await.unwrap().unwrap();
        assert_eq!(loaded, bytes);
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn router_requires_auth_and_supports_conditional_chunk_reads() {
        let directory = unique_directory();
        let bytes = iarc(1, 4);
        write_chunk(&directory, 1, &bytes);
        let store = ArchiveStore::open(&directory).unwrap();
        let archive_id = store.archive_id().to_string();
        let router = ArchiveHttpRouter::new(store, "pairing-secret").unwrap();
        let uri = format!("/v1/archives/{archive_id}/chunks/1");

        let denied = router
            .route(request(Method::GET, uri.clone(), None))
            .await
            .unwrap();
        assert_eq!(denied.status, StatusCode::UNAUTHORIZED);

        let response = router
            .route(request(Method::GET, uri.clone(), Some("pairing-secret")))
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body.as_deref(), Some(bytes.as_ref()));
        let etag = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(header::ETAG.as_str()))
            .unwrap()
            .1
            .to_string();

        let conditional = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer pairing-secret")
            .header(header::IF_NONE_MATCH, etag)
            .body(())
            .unwrap();
        let response = router.route(conditional).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_MODIFIED);
        assert!(response.body.is_none());
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn concurrent_readers_receive_the_same_immutable_object() {
        let directory = unique_directory();
        let bytes = iarc(1, 5);
        write_chunk(&directory, 1, &bytes);
        let store = ArchiveStore::open(&directory).unwrap();
        let mut readers = Vec::new();
        for _ in 0..64 {
            let store = Arc::clone(&store);
            readers.push(tokio::spawn(async move {
                store.load_chunk(1).await.unwrap().unwrap()
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), bytes);
        }
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn an_evicted_hot_object_remains_available_from_disk() {
        let directory = unique_directory();
        let store = ArchiveStore::open(&directory).unwrap();
        let first = iarc(1, 1);
        for sequence in 1..=ARCHIVE_HOT_SLOTS as u64 + 1 {
            let bytes = if sequence == 1 {
                first.clone()
            } else {
                iarc(sequence, sequence as u8)
            };
            write_chunk(&directory, sequence, &bytes);
            store.publish_iarc(sequence).unwrap();
        }
        tokio::task::yield_now().await;
        assert_eq!(store.load_chunk(1).await.unwrap().unwrap(), first);
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn malformed_manifest_queries_and_chunk_names_are_rejected() {
        let directory = unique_directory();
        let store = ArchiveStore::open(&directory).unwrap();
        let archive_id = store.archive_id().to_string();
        let router = ArchiveHttpRouter::new(store, "secret").unwrap();
        let response = router
            .route(request(
                Method::GET,
                format!("/v1/archives/{archive_id}/manifest?wait=26"),
                Some("secret"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        let response = router
            .route(request(
                Method::GET,
                format!("/v1/archives/{archive_id}/chunks/../../1"),
                Some("secret"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        let _ = fs::remove_dir_all(directory);
    }
}
