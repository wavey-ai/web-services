use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http::{Method, Request, StatusCode};
use playlists::{chunk_cache::ChunkCache, Options as CacheOptions};
use rist_core_pure::{packet::rtcp::NackMode, time::ntp_now, ReceivedPayload};
use rist_mio_pure::{MainMioReceiver, SimpleMioReceiver};
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{watch, RwLock},
    time::{interval, sleep, MissedTickBehavior},
};
use tracing::{debug, info, warn};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter,
};

#[cfg(test)]
const DEFAULT_FLOW_ID: u32 = 0x7273_7401;
const DEFAULT_STREAM_ID: u64 = 1;
const MAX_DRAIN_PER_TICK: usize = 128;
const RIST_POLL_MS: u64 = 1;
const RTCP_INTERVAL_MS: u64 = 20;
const PART_WAIT_MS: u64 = 3_000;
const HLS_JS: &str = include_str!("../../../hls/public/hls.min.js");
const INDEX_HTML: &str = include_str!("static/index.html");
const STYLES_CSS: &str = include_str!("static/styles.css");
const APP_JS: &str = include_str!("static/app.js");

#[derive(Debug, Clone, Parser)]
#[command(
    name = "obs-rist-llhls",
    about = "Receive OBS RIST MPEG-TS with pure Rust RIST and serve live HLS playback"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:7000")]
    rist_bind: SocketAddr,

    #[arg(long, value_enum, default_value = "main")]
    rist_profile: RistProfile,

    #[arg(long, value_parser = parse_u32_auto, default_value = "0x72737401")]
    flow_id: u32,

    #[arg(long, default_value_t = 9444)]
    http_port: u16,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,

    #[arg(long, default_value_t = DEFAULT_STREAM_ID)]
    stream_id: u64,

    #[arg(long, default_value_t = 500)]
    part_ms: u64,

    #[arg(long, default_value_t = 4)]
    parts_per_segment: usize,

    #[arg(long, default_value_t = 24)]
    window_parts: usize,

    #[arg(long, default_value_t = 2048)]
    slot_kb: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RistProfile {
    Simple,
    Main,
}

impl RistProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Main => "main",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "obs_rist_llhls=info,web_service=info".into()),
        )
        .init();

    let args = Args::parse().normalized()?;
    let (cert, key) = load_tls(&args)?;
    let cache = TsHlsCache::new(
        args.stream_id,
        Duration::from_millis(args.part_ms),
        args.parts_per_segment,
        args.window_parts,
        args.slot_kb,
    )
    .await;

    let router = Box::new(AppRouter::new(Arc::clone(&cache)));
    let server = H2H3Server::builder()
        .with_tls(cert, key)
        .with_port(args.http_port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_websocket(false)
        .with_router(router)
        .build()?;
    let handle = server.start().await?;
    let _ = handle.ready_rx.await;

    let shutdown_rx = handle.shutdown_tx.subscribe();
    let receiver_args = args.clone();
    let receiver_cache = Arc::clone(&cache);
    let receiver_task =
        tokio::spawn(
            async move { run_rist_receiver(receiver_args, receiver_cache, shutdown_rx).await },
        );

    println!("playback: https://127.0.0.1:{}/", args.http_port);
    println!(
        "OBS RIST target: rist://127.0.0.1:{}  profile={} flow_id=0x{:08x}",
        args.rist_bind.port(),
        args.rist_profile.as_str(),
        args.flow_id
    );
    info!(
        http_port = args.http_port,
        rist_bind = %args.rist_bind,
        profile = args.rist_profile.as_str(),
        flow_id = format_args!("0x{:08x}", args.flow_id),
        "OBS RIST LL-HLS example ready"
    );

    tokio::signal::ctrl_c().await?;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    receiver_task
        .await
        .context("RIST receiver task join failed")??;
    Ok(())
}

impl Args {
    fn normalized(mut self) -> Result<Self> {
        if self.part_ms < 100 {
            bail!("--part-ms must be at least 100");
        }
        self.parts_per_segment = self.parts_per_segment.max(1);
        self.window_parts = self.window_parts.max(self.parts_per_segment * 3).max(6);
        self.slot_kb = self.slot_kb.max(64);
        Ok(self)
    }
}

fn load_tls(args: &Args) -> Result<(String, String)> {
    match (&args.cert, &args.key) {
        (Some(cert), Some(key)) => load_tls_base64_from_paths(cert, key).with_context(|| {
            format!(
                "failed to load TLS files {} and {}",
                cert.display(),
                key.display()
            )
        }),
        (None, None) => load_default_tls_base64()
            .context("failed to load default TLS files from web-services/tls/local.wavey.ai"),
        _ => bail!("--cert and --key must be provided together"),
    }
}

fn parse_u32_auto(value: &str) -> std::result::Result<u32, String> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u32::from_str_radix(hex, 16).map_err(|err| err.to_string())
    } else {
        trimmed.parse::<u32>().map_err(|err| err.to_string())
    }
}

enum Receiver {
    Simple(SimpleMioReceiver),
    Main(MainMioReceiver),
}

impl Receiver {
    fn bind(profile: RistProfile, addr: SocketAddr, flow_id: u32) -> io::Result<Self> {
        match profile {
            RistProfile::Simple => {
                SimpleMioReceiver::bind(addr, flow_id, "obs-rist-llhls", NackMode::Range)
                    .map(Self::Simple)
            }
            RistProfile::Main => {
                MainMioReceiver::bind(addr, flow_id, "obs-rist-llhls", NackMode::Range)
                    .map(Self::Main)
            }
        }
    }

    fn try_recv_payload(
        &mut self,
        buf: &mut [u8],
    ) -> io::Result<Option<(SocketAddr, ReceivedPayload)>> {
        match self {
            Self::Simple(receiver) => receiver.try_recv_payload(buf),
            Self::Main(receiver) => receiver.try_recv_payload(buf),
        }
    }

    fn poll_rtcp_and_send(&mut self, now: Instant, now_ntp: u64) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
            Self::Main(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
        }
    }
}

async fn run_rist_receiver(
    args: Args,
    cache: Arc<TsHlsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut receiver = Receiver::bind(args.rist_profile, args.rist_bind, args.flow_id)
        .with_context(|| format!("failed to bind RIST receiver on {}", args.rist_bind))?;
    let mut buf = vec![0u8; 65_536];
    let mut poll = interval(Duration::from_millis(RIST_POLL_MS));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_rtcp = Instant::now();

    info!(
        address = %args.rist_bind,
        profile = args.rist_profile.as_str(),
        "pure Rust RIST receiver listening"
    );

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                cache.rotate_if_due(true).await?;
                info!("pure Rust RIST receiver shutting down");
                return Ok(());
            }
            _ = poll.tick() => {
                for _ in 0..MAX_DRAIN_PER_TICK {
                    match receiver.try_recv_payload(&mut buf) {
                        Ok(Some((peer, payload))) => {
                            if let Err(error) = cache.push_payload(&payload).await {
                                warn!(peer = %peer, error = %error, "failed to cache RIST payload");
                            }
                        }
                        Ok(None) => break,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => {
                            warn!(error = %error, "pure Rust RIST receive failed");
                            break;
                        }
                    }
                }

                cache.rotate_if_due(false).await?;

                let now = Instant::now();
                if now.duration_since(last_rtcp) >= Duration::from_millis(RTCP_INTERVAL_MS) {
                    if let Err(error) = receiver.poll_rtcp_and_send(now, ntp_now()) {
                        if error.kind() != io::ErrorKind::WouldBlock {
                            debug!(error = %error, "pure Rust RIST RTCP poll failed");
                        }
                    }
                    last_rtcp = now;
                }
            }
        }
    }
}

struct TsHlsCache {
    chunk_cache: Arc<ChunkCache>,
    stream_id: u64,
    stream_idx: usize,
    part_target: Duration,
    parts_per_segment: usize,
    window_parts: usize,
    max_part_bytes: usize,
    state: RwLock<LiveState>,
}

impl TsHlsCache {
    async fn new(
        stream_id: u64,
        part_target: Duration,
        parts_per_segment: usize,
        window_parts: usize,
        slot_kb: usize,
    ) -> Arc<Self> {
        let mut options = CacheOptions::default();
        options.num_playlists = 1;
        options.max_segments = 1;
        options.max_parts_per_segment = window_parts.saturating_mul(2).max(8);
        options.buffer_size_kb = slot_kb;
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let stream_idx = chunk_cache.get_or_create_stream_idx(stream_id).await;
        Arc::new(Self {
            chunk_cache,
            stream_id,
            stream_idx,
            part_target,
            parts_per_segment,
            window_parts,
            max_part_bytes: slot_kb * 1024,
            state: RwLock::new(LiveState::new()),
        })
    }

    async fn push_payload(&self, payload: &ReceivedPayload) -> Result<()> {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        let finalized = {
            let mut state = self.state.write().await;
            state.packets_received += 1;
            state.last_packet_unix_ms = Some(now_ms);

            if payload.duplicate {
                state.duplicate_packets += 1;
                return Ok(());
            }
            if payload.recovered {
                state.recovered_packets += 1;
            }

            state.bytes_received += payload.payload.len() as u64;
            if state.current.is_empty() {
                state.current_started = now;
                state.current_started_unix_ms = now_ms;
            }
            state.current.extend_from_slice(&payload.payload);

            if now.duration_since(state.current_started) >= self.part_target
                || state.current.len() >= self.max_part_bytes
            {
                state.take_current(now, now_ms)
            } else {
                None
            }
        };

        if let Some(part) = finalized {
            self.commit_part(part).await?;
        }
        Ok(())
    }

    async fn rotate_if_due(&self, force: bool) -> Result<()> {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        let finalized = {
            let mut state = self.state.write().await;
            if force || now.duration_since(state.current_started) >= self.part_target {
                state.take_current(now, now_ms)
            } else {
                None
            }
        };
        if let Some(part) = finalized {
            self.commit_part(part).await?;
        }
        Ok(())
    }

    async fn commit_part(&self, part: PendingPart) -> Result<()> {
        self.chunk_cache
            .add(
                self.stream_idx,
                part.meta.seq as usize,
                Bytes::from(part.data),
            )
            .await
            .map_err(|err| anyhow!("chunk cache write failed: {err}"))?;

        let mut state = self.state.write().await;
        state.parts.push_back(part.meta);
        while state.parts.len() > self.window_parts {
            state.parts.pop_front();
        }
        Ok(())
    }

    async fn playlist(&self) -> String {
        let state = self.state.read().await;
        let parts: Vec<PartMeta> = state.parts.iter().copied().collect();
        let media_sequence = parts
            .first()
            .map(|part| part.seq / self.parts_per_segment as u64)
            .unwrap_or(0);
        let next_part = state.next_seq;
        let part_target = self.part_target.as_secs_f64();
        let target_duration = (part_target * self.parts_per_segment as f64)
            .ceil()
            .max(1.0) as u64;
        drop(state);

        let mut groups: BTreeMap<u64, Vec<PartMeta>> = BTreeMap::new();
        for part in parts {
            groups
                .entry(part.seq / self.parts_per_segment as u64)
                .or_default()
                .push(part);
        }

        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:9\n");
        out.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));
        out.push_str(&format!("#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n"));
        out.push_str(&format!(
            "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        ));

        for (segment, group) in groups {
            let mut duration = 0.0;
            for part in &group {
                let part_duration = part.duration_ms as f64 / 1000.0;
                duration += part_duration;
                out.push_str(&format!(
                    "#EXT-X-PART:DURATION={part_duration:.3},URI=\"part{}.ts\"\n",
                    part.seq
                ));
            }
            if group.len() == self.parts_per_segment {
                out.push_str(&format!("#EXTINF:{duration:.3},\n"));
                out.push_str(&format!("seg{segment}.ts\n"));
            }
        }

        out.push_str(&format!(
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.ts\"\n"
        ));
        out
    }

    async fn get_part_blocking(&self, seq: u64) -> Option<(Bytes, u64)> {
        let deadline = Instant::now() + Duration::from_millis(PART_WAIT_MS);
        loop {
            let (known, too_old, too_far) = {
                let state = self.state.read().await;
                let first = state.parts.front().map(|part| part.seq);
                let known = state.parts.iter().any(|part| part.seq == seq);
                let too_old = first.map(|first| seq < first).unwrap_or(false);
                let too_far = seq > state.next_seq;
                (known, too_old, too_far)
            };

            if known {
                return self.chunk_cache.get(self.stream_idx, seq as usize).await;
            }
            if too_old || too_far || Instant::now() >= deadline {
                return None;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn get_segment(&self, segment: u64) -> Option<Bytes> {
        let first_part = segment.checked_mul(self.parts_per_segment as u64)?;
        let mut out = Vec::new();
        for offset in 0..self.parts_per_segment {
            let seq = first_part + offset as u64;
            let (bytes, _) = self.get_part_blocking(seq).await?;
            out.extend_from_slice(&bytes);
        }
        Some(Bytes::from(out))
    }

    async fn stats(&self) -> StatsSnapshot {
        let state = self.state.read().await;
        let now_ms = now_unix_ms();
        StatsSnapshot {
            stream_id: self.stream_id,
            part_target_ms: self.part_target.as_millis() as u64,
            parts_per_segment: self.parts_per_segment,
            window_parts: self.window_parts,
            packets_received: state.packets_received,
            duplicate_packets: state.duplicate_packets,
            recovered_packets: state.recovered_packets,
            bytes_received: state.bytes_received,
            cached_parts: state.parts.len(),
            current_part_bytes: state.current.len(),
            latest_part: state.parts.back().map(|part| part.seq),
            latest_part_bytes: state.parts.back().map(|part| part.bytes),
            latest_part_duration_ms: state.parts.back().map(|part| part.duration_ms),
            latest_part_age_ms: state
                .parts
                .back()
                .map(|part| now_ms.saturating_sub(part.committed_unix_ms)),
            latest_part_started_age_ms: state
                .parts
                .back()
                .map(|part| now_ms.saturating_sub(part.started_unix_ms)),
            last_packet_age_ms: state
                .last_packet_unix_ms
                .map(|last| now_ms.saturating_sub(last)),
        }
    }
}

struct LiveState {
    current: Vec<u8>,
    current_started: Instant,
    current_started_unix_ms: u64,
    next_seq: u64,
    parts: VecDeque<PartMeta>,
    packets_received: u64,
    duplicate_packets: u64,
    recovered_packets: u64,
    bytes_received: u64,
    last_packet_unix_ms: Option<u64>,
}

impl LiveState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            current: Vec::new(),
            current_started: now,
            current_started_unix_ms: now_unix_ms(),
            next_seq: 0,
            parts: VecDeque::new(),
            packets_received: 0,
            duplicate_packets: 0,
            recovered_packets: 0,
            bytes_received: 0,
            last_packet_unix_ms: None,
        }
    }

    fn take_current(&mut self, now: Instant, now_ms: u64) -> Option<PendingPart> {
        if self.current.is_empty() {
            self.current_started = now;
            self.current_started_unix_ms = now_ms;
            return None;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let data = std::mem::take(&mut self.current);
        let duration_ms = now
            .duration_since(self.current_started)
            .as_millis()
            .max(1)
            .min(u128::from(u64::MAX)) as u64;
        let meta = PartMeta {
            seq,
            duration_ms,
            bytes: data.len(),
            started_unix_ms: self.current_started_unix_ms,
            committed_unix_ms: now_ms,
        };
        self.current_started = now;
        self.current_started_unix_ms = now_ms;
        Some(PendingPart { meta, data })
    }
}

#[derive(Debug, Clone, Copy)]
struct PartMeta {
    seq: u64,
    duration_ms: u64,
    bytes: usize,
    started_unix_ms: u64,
    committed_unix_ms: u64,
}

struct PendingPart {
    meta: PartMeta,
    data: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct StatsSnapshot {
    stream_id: u64,
    part_target_ms: u64,
    parts_per_segment: usize,
    window_parts: usize,
    packets_received: u64,
    duplicate_packets: u64,
    recovered_packets: u64,
    bytes_received: u64,
    cached_parts: usize,
    current_part_bytes: usize,
    latest_part: Option<u64>,
    latest_part_bytes: Option<usize>,
    latest_part_duration_ms: Option<u64>,
    latest_part_age_ms: Option<u64>,
    latest_part_started_age_ms: Option<u64>,
    last_packet_age_ms: Option<u64>,
}

struct AppRouter {
    cache: Arc<TsHlsCache>,
}

impl AppRouter {
    fn new(cache: Arc<TsHlsCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl Router for AppRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if req.method() == Method::OPTIONS {
            return Ok(response(StatusCode::NO_CONTENT, None, None));
        }
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED, None, None));
        }

        let path = req.uri().path();
        match path {
            "/" | "/play" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(INDEX_HTML.as_bytes())),
                Some("text/html; charset=utf-8"),
            )),
            "/styles.css" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(STYLES_CSS.as_bytes())),
                Some("text/css; charset=utf-8"),
            )),
            "/app.js" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(APP_JS.as_bytes())),
                Some("text/javascript; charset=utf-8"),
            )),
            "/hls.min.js" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(HLS_JS.as_bytes())),
                Some("text/javascript; charset=utf-8"),
            )),
            "/live/stream.m3u8" => {
                let playlist = self.cache.playlist().await;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(playlist)),
                    Some("application/vnd.apple.mpegurl"),
                )
                .with_no_store())
            }
            "/api/stats" => {
                let json = serde_json::to_vec(&self.cache.stats().await)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(json)),
                    Some("application/json"),
                )
                .with_no_store())
            }
            "/up" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(b"OK")),
                Some("text/plain"),
            )),
            _ => {
                if let Some(seq) = parse_part_path(path) {
                    if let Some((bytes, hash)) = self.cache.get_part_blocking(seq).await {
                        return Ok(response(StatusCode::OK, Some(bytes), Some("video/mp2t"))
                            .with_etag(hash));
                    }
                    return Ok(response(StatusCode::NOT_FOUND, None, None));
                }

                if let Some(segment) = parse_segment_path(path) {
                    if let Some(bytes) = self.cache.get_segment(segment).await {
                        return Ok(response(StatusCode::OK, Some(bytes), Some("video/mp2t")));
                    }
                    return Ok(response(StatusCode::NOT_FOUND, None, None));
                }

                Ok(response(StatusCode::NOT_FOUND, None, None))
            }
        }
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("no streaming endpoints".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn web_service::WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn web_service::WebSocketHandler> {
        None
    }
}

trait ResponseExt {
    fn with_no_store(self) -> Self;
    fn with_etag(self, etag: u64) -> Self;
}

impl ResponseExt for HandlerResponse {
    fn with_no_store(mut self) -> Self {
        self.headers
            .push(("cache-control".into(), "no-store, max-age=0".into()));
        self
    }

    fn with_etag(mut self, etag: u64) -> Self {
        self.etag = Some(etag);
        self
    }
}

fn response(
    status: StatusCode,
    body: Option<Bytes>,
    content_type: Option<&'static str>,
) -> HandlerResponse {
    HandlerResponse {
        status,
        body,
        content_type: content_type.map(str::to_string),
        headers: vec![
            ("access-control-allow-origin".into(), "*".into()),
            (
                "access-control-allow-methods".into(),
                "GET, HEAD, OPTIONS".into(),
            ),
        ],
        etag: None,
    }
}

fn parse_part_path(path: &str) -> Option<u64> {
    path.strip_prefix("/live/part")?
        .strip_suffix(".ts")?
        .parse()
        .ok()
}

fn parse_segment_path(path: &str) -> Option<u64> {
    path.strip_prefix("/live/seg")?
        .strip_suffix(".ts")?
        .parse()
        .ok()
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimal_and_hex_flow_ids() {
        assert_eq!(parse_u32_auto("1920168961").unwrap(), DEFAULT_FLOW_ID);
        assert_eq!(parse_u32_auto("0x72737401").unwrap(), DEFAULT_FLOW_ID);
    }

    #[tokio::test]
    async fn playlist_includes_cached_parts() {
        let cache = TsHlsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        cache
            .push_payload(&ReceivedPayload {
                sequence: 0,
                recovered: false,
                duplicate: false,
                newly_missing: Vec::new(),
                payload: vec![0x47; 188],
            })
            .await
            .unwrap();
        cache.rotate_if_due(true).await.unwrap();
        let playlist = cache.playlist().await;
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("#EXT-X-PRELOAD-HINT"));
    }
}
