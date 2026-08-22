use crate::{ResponseResult, UploadResponseService, UploadStream};
use bytes::BytesMut;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use mio::event::Source;
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use rist_core_pure::packet::gre::GreKeepalive;
use rist_core_pure::packet::rtcp::NackMode;
use rist_core_pure::time::ntp_now;
use rist_core_pure::{OrderedPayloadBuffer, ReceivedPayload};
use rist_mio_pure::{MainMioReceiver, MainReceiverEvent, SimpleMioReceiver};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{Duration, MissedTickBehavior};
use tracing::{debug, error, info, warn};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(1);
const DEFAULT_RTCP_INTERVAL: Duration = Duration::from_millis(20);
const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_INGRESS_QUEUE_PACKETS: usize = 16_384;
const DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const MAX_REORDERED_PACKETS: usize = 16_384;
const RIST_KEEPALIVE_ID: [u8; 6] = [0x02, 0x57, 0x41, 0x56, 0x45, 0x59];
const RIST_SOCKET_TOKEN: Token = Token(0);
const RIST_SHUTDOWN_TOKEN: Token = Token(1);

/// Point-in-time counters for the bounded RIST receive-to-writer handoff.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PureRistIngestMetrics {
    pub received_packets: u64,
    pub enqueued_packets: u64,
    pub dequeued_packets: u64,
    pub overflow_packets: u64,
    pub queue_depth_packets: u64,
    pub queue_high_watermark_packets: u64,
    pub queue_capacity_packets: u64,
    pub requested_socket_receive_buffer_bytes: u64,
    pub effective_socket_receive_buffer_bytes: u64,
    pub writer_active: u64,
    pub writer_state: u64,
    pub receive_errors: u64,
    pub protocol_missing_packets: u64,
    pub last_received_sequence: u64,
    pub receiver_active: u64,
    pub receiver_exit_reason: u64,
    pub receiver_state: u64,
}

/// Lock-free metrics shared with a running [`PureRistIngest`] instance.
#[derive(Debug)]
pub struct PureRistIngestStats {
    received_packets: AtomicU64,
    enqueued_packets: AtomicU64,
    dequeued_packets: AtomicU64,
    overflow_packets: AtomicU64,
    queue_depth_packets: AtomicU64,
    queue_high_watermark_packets: AtomicU64,
    queue_capacity_packets: AtomicU64,
    requested_socket_receive_buffer_bytes: AtomicU64,
    effective_socket_receive_buffer_bytes: AtomicU64,
    writer_active: AtomicU64,
    writer_state: AtomicU64,
    receive_errors: AtomicU64,
    protocol_missing_packets: AtomicU64,
    last_received_sequence: AtomicU64,
    receiver_active: AtomicU64,
    receiver_exit_reason: AtomicU64,
    receiver_state: AtomicU64,
}

impl PureRistIngestStats {
    fn new(queue_capacity_packets: usize, socket_receive_buffer_bytes: usize) -> Self {
        Self {
            received_packets: AtomicU64::new(0),
            enqueued_packets: AtomicU64::new(0),
            dequeued_packets: AtomicU64::new(0),
            overflow_packets: AtomicU64::new(0),
            queue_depth_packets: AtomicU64::new(0),
            queue_high_watermark_packets: AtomicU64::new(0),
            queue_capacity_packets: AtomicU64::new(queue_capacity_packets as u64),
            requested_socket_receive_buffer_bytes: AtomicU64::new(
                socket_receive_buffer_bytes as u64,
            ),
            effective_socket_receive_buffer_bytes: AtomicU64::new(0),
            writer_active: AtomicU64::new(0),
            writer_state: AtomicU64::new(0),
            receive_errors: AtomicU64::new(0),
            protocol_missing_packets: AtomicU64::new(0),
            last_received_sequence: AtomicU64::new(0),
            receiver_active: AtomicU64::new(0),
            receiver_exit_reason: AtomicU64::new(0),
            receiver_state: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> PureRistIngestMetrics {
        PureRistIngestMetrics {
            received_packets: self.received_packets.load(Ordering::Relaxed),
            enqueued_packets: self.enqueued_packets.load(Ordering::Relaxed),
            dequeued_packets: self.dequeued_packets.load(Ordering::Relaxed),
            overflow_packets: self.overflow_packets.load(Ordering::Relaxed),
            queue_depth_packets: self.queue_depth_packets.load(Ordering::Relaxed),
            queue_high_watermark_packets: self.queue_high_watermark_packets.load(Ordering::Relaxed),
            queue_capacity_packets: self.queue_capacity_packets.load(Ordering::Relaxed),
            requested_socket_receive_buffer_bytes: self
                .requested_socket_receive_buffer_bytes
                .load(Ordering::Relaxed),
            effective_socket_receive_buffer_bytes: self
                .effective_socket_receive_buffer_bytes
                .load(Ordering::Relaxed),
            writer_active: self.writer_active.load(Ordering::Relaxed),
            writer_state: self.writer_state.load(Ordering::Relaxed),
            receive_errors: self.receive_errors.load(Ordering::Relaxed),
            protocol_missing_packets: self.protocol_missing_packets.load(Ordering::Relaxed),
            last_received_sequence: self.last_received_sequence.load(Ordering::Relaxed),
            receiver_active: self.receiver_active.load(Ordering::Relaxed),
            receiver_exit_reason: self.receiver_exit_reason.load(Ordering::Relaxed),
            receiver_state: self.receiver_state.load(Ordering::Relaxed),
        }
    }

    fn begin_enqueue(&self) -> u64 {
        self.queue_depth_packets.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn record_enqueued(&self, depth: u64) {
        self.enqueued_packets.fetch_add(1, Ordering::Relaxed);
        self.queue_high_watermark_packets
            .fetch_max(depth, Ordering::Relaxed);
    }

    fn cancel_enqueue(&self) {
        self.decrement_depth();
    }

    fn record_dequeue(&self) {
        self.dequeued_packets.fetch_add(1, Ordering::Relaxed);
        self.decrement_depth();
    }

    fn decrement_depth(&self) {
        let _ =
            self.queue_depth_packets
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                    Some(depth.saturating_sub(1))
                });
    }
}

struct RistIngressPacket {
    peer: SocketAddr,
    payload: ReceivedPayload,
    overflow_epoch: u64,
}

#[derive(Clone, Copy)]
struct RistWriterConfig {
    local_addr: SocketAddr,
    profile: PureRistProfile,
    flow_id: u32,
    session_idle_timeout: Duration,
    body_flush_bytes: usize,
}

/// Pure Rust RIST profile used by [`PureRistIngest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PureRistProfile {
    Simple,
    Main,
}

impl PureRistProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Main => "main",
        }
    }
}

/// Auth callback for pure Rust RIST datagrams.
pub trait PureRistAuth: Send + Sync + 'static {
    /// Returns true to allow the peer, false to drop its packets.
    fn authenticate(&self, peer_addr: &SocketAddr) -> bool;
}

/// Default auth that allows all peers.
pub struct AllowAllPureRist;

impl PureRistAuth for AllowAllPureRist {
    fn authenticate(&self, _peer_addr: &SocketAddr) -> bool {
        true
    }
}

struct PureRistRequest {
    stream: UploadStream,
    response_rx: oneshot::Receiver<ResponseResult>,
    pending: BytesMut,
    body_flush_bytes: usize,
    ordered_payloads: OrderedPayloadBuffer,
    last_payload_at: Instant,
}

enum Receiver {
    Simple(Box<SimpleMioReceiver>),
    Main(Box<MainMioReceiver>),
}

enum ReceiverRead {
    Payload(SocketAddr, ReceivedPayload),
    Control,
    Empty,
}

impl Receiver {
    fn bind(profile: PureRistProfile, addr: SocketAddr, flow_id: u32) -> io::Result<Self> {
        match profile {
            PureRistProfile::Simple => {
                SimpleMioReceiver::bind(addr, flow_id, "web-services-pure-rist", NackMode::Range)
                    .map(Box::new)
                    .map(Self::Simple)
            }
            PureRistProfile::Main => {
                MainMioReceiver::bind(addr, flow_id, "web-services-pure-rist", NackMode::Range)
                    .map(Box::new)
                    .map(Self::Main)
            }
        }
    }

    fn try_recv(&mut self, buf: &mut [u8]) -> io::Result<ReceiverRead> {
        match self {
            Self::Simple(receiver) => Ok(match receiver.try_recv_payload(buf)? {
                Some((peer, payload)) => ReceiverRead::Payload(peer, payload),
                None => ReceiverRead::Empty,
            }),
            Self::Main(receiver) => Ok(match receiver.try_recv_event(buf)? {
                Some(MainReceiverEvent::Payload { from, payload }) => {
                    ReceiverRead::Payload(from, payload)
                }
                Some(_) => ReceiverRead::Control,
                None => ReceiverRead::Empty,
            }),
        }
    }

    fn poll_rtcp_and_send(&mut self, now: Instant, now_ntp: u64) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
            Self::Main(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
        }
    }

    fn poll_session_and_send_keepalive(&mut self, now: Instant) -> io::Result<()> {
        match self {
            Self::Simple(_) => Ok(()),
            Self::Main(receiver) => receiver
                .poll_session_and_send_keepalive(
                    now,
                    GreKeepalive::librist_default(RIST_KEEPALIVE_ID),
                )
                .map(|_| ()),
        }
    }

    fn socket_receive_buffer_size(&self) -> io::Result<usize> {
        match self {
            Self::Simple(receiver) => {
                let sizes = receiver.socket_buffer_sizes()?;
                Ok(sizes.rtp.receive.min(sizes.rtcp.receive))
            }
            Self::Main(receiver) => Ok(receiver.socket_buffer_sizes()?.receive),
        }
    }

    fn set_socket_receive_buffer_size(&self, receive: usize) -> io::Result<usize> {
        match self {
            Self::Simple(receiver) => {
                let current = receiver.socket_buffer_sizes()?;
                let send = current.rtp.send.max(current.rtcp.send);
                let sizes = receiver.set_socket_buffer_sizes(receive, send)?;
                Ok(sizes.rtp.receive.min(sizes.rtcp.receive))
            }
            Self::Main(receiver) => {
                let send = receiver.socket_buffer_sizes()?.send;
                Ok(receiver.set_socket_buffer_sizes(receive, send)?.receive)
            }
        }
    }
}

impl Source for Receiver {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.register(registry, token, interests),
            Self::Main(receiver) => receiver.register(registry, token, interests),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.reregister(registry, token, interests),
            Self::Main(receiver) => receiver.reregister(registry, token, interests),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.deregister(registry),
            Self::Main(receiver) => receiver.deregister(registry),
        }
    }
}

/// Pure Rust RIST ingest server that feeds into [`UploadResponseService`].
///
/// This uses `rist-mio`/`rist-core` instead of the librist C wrapper used by
/// [`crate::RistIngest`]. The two implementations intentionally coexist under
/// separate Cargo features:
///
/// - `rist` enables the existing C-wrapper/librist path.
/// - `rist-pure` enables this pure Rust path.
pub struct PureRistIngest<A: PureRistAuth = AllowAllPureRist> {
    service: Arc<UploadResponseService>,
    auth: Arc<A>,
    profile: PureRistProfile,
    flow_id: u32,
    session_idle_timeout: Duration,
    body_flush_bytes: Option<usize>,
    ingress_queue_packets: usize,
    socket_receive_buffer_bytes: usize,
    stats: Arc<PureRistIngestStats>,
}

impl PureRistIngest<AllowAllPureRist> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAllPureRist),
            profile: PureRistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
            session_idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            body_flush_bytes: None,
            ingress_queue_packets: DEFAULT_INGRESS_QUEUE_PACKETS,
            socket_receive_buffer_bytes: DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES,
            stats: Arc::new(PureRistIngestStats::new(
                DEFAULT_INGRESS_QUEUE_PACKETS,
                DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES,
            )),
        }
    }
}

impl<A: PureRistAuth> PureRistIngest<A> {
    pub fn with_auth(service: Arc<UploadResponseService>, auth: A) -> Self {
        Self {
            service,
            auth: Arc::new(auth),
            profile: PureRistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
            session_idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            body_flush_bytes: None,
            ingress_queue_packets: DEFAULT_INGRESS_QUEUE_PACKETS,
            socket_receive_buffer_bytes: DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES,
            stats: Arc::new(PureRistIngestStats::new(
                DEFAULT_INGRESS_QUEUE_PACKETS,
                DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES,
            )),
        }
    }

    pub fn with_profile(mut self, profile: PureRistProfile) -> Self {
        self.profile = profile;
        self
    }

    pub fn with_flow_id(mut self, flow_id: u32) -> Self {
        self.flow_id = flow_id;
        self
    }

    /// End an upload request after this interval without a media payload.
    pub fn with_session_idle_timeout(mut self, timeout: Duration) -> Self {
        self.session_idle_timeout = timeout;
        self
    }

    /// Flush ordered payload bytes at this threshold instead of the cache slot size.
    pub fn with_body_flush_bytes(mut self, bytes: usize) -> Self {
        self.body_flush_bytes = Some(bytes.max(1));
        self
    }

    /// Bound the receive-to-writer queue by packet count.
    pub fn with_ingress_queue_packets(mut self, packets: usize) -> Self {
        self.ingress_queue_packets = packets.max(1);
        self.stats
            .queue_capacity_packets
            .store(self.ingress_queue_packets as u64, Ordering::Relaxed);
        self
    }

    /// Request this operating-system UDP receive-buffer size.
    pub fn with_socket_receive_buffer_bytes(mut self, bytes: usize) -> Self {
        self.socket_receive_buffer_bytes = bytes.max(1);
        self.stats
            .requested_socket_receive_buffer_bytes
            .store(self.socket_receive_buffer_bytes as u64, Ordering::Relaxed);
        self
    }

    /// Return a live metrics handle that remains valid after [`Self::start`] consumes self.
    pub fn stats(&self) -> Arc<PureRistIngestStats> {
        Arc::clone(&self.stats)
    }

    /// Start the pure Rust RIST receiver on the given address.
    ///
    /// The returned sender stops the polling task when any value is sent.
    pub async fn start(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = self.service;
        let auth = self.auth;
        let profile = self.profile;
        let flow_id = self.flow_id;
        let session_idle_timeout = self.session_idle_timeout;
        let body_flush_bytes = self
            .body_flush_bytes
            .unwrap_or_else(|| service.config().slot_bytes().max(1));
        let ingress_queue_packets = self.ingress_queue_packets;
        let socket_receive_buffer_bytes = self.socket_receive_buffer_bytes;
        let stats = self.stats;
        let mut receiver = Receiver::bind(profile, addr, flow_id)?;
        let effective_socket_receive_buffer_bytes = receiver
            .set_socket_receive_buffer_size(socket_receive_buffer_bytes)
            .or_else(|error| {
                warn!(
                    error = %error,
                    requested_bytes = socket_receive_buffer_bytes,
                    "could not increase pure Rust RIST UDP receive buffer"
                );
                receiver.socket_receive_buffer_size()
            })?;
        stats.effective_socket_receive_buffer_bytes.store(
            effective_socket_receive_buffer_bytes as u64,
            Ordering::Relaxed,
        );
        let (ingress_tx, ingress_rx) = mpsc::channel(ingress_queue_packets);

        info!(
            address = %addr,
            profile = profile.as_str(),
            flow_id,
            ingress_queue_packets,
            requested_socket_receive_buffer_bytes = socket_receive_buffer_bytes,
            effective_socket_receive_buffer_bytes,
            "pure Rust RIST ingest server listening"
        );

        if effective_socket_receive_buffer_bytes < socket_receive_buffer_bytes {
            warn!(
                requested_bytes = socket_receive_buffer_bytes,
                effective_bytes = effective_socket_receive_buffer_bytes,
                "pure Rust RIST UDP receive buffer is below the requested size"
            );
        }

        let receiver_stop = Arc::new(AtomicBool::new(false));
        let receiver_poll = Poll::new()?;
        receiver_poll
            .registry()
            .register(&mut receiver, RIST_SOCKET_TOKEN, Interest::READABLE)?;
        let receiver_waker = Arc::new(Waker::new(receiver_poll.registry(), RIST_SHUTDOWN_TOKEN)?);
        let receiver_thread_stop = Arc::clone(&receiver_stop);
        let receiver_thread_waker = Arc::clone(&receiver_waker);
        let receiver_stats = Arc::clone(&stats);
        let receiver_thread_name = format!("pure-rist-recv-{}", addr.port());
        let receiver_thread =
            thread::Builder::new()
                .name(receiver_thread_name)
                .spawn(move || {
                    run_receiver(
                        receiver,
                        receiver_poll,
                        auth,
                        ingress_tx,
                        receiver_stats,
                        receiver_thread_stop,
                    );
                })?;

        let writer_service = Arc::clone(&service);
        let writer_stats = Arc::clone(&stats);
        let writer_config = RistWriterConfig {
            local_addr: addr,
            profile,
            flow_id,
            session_idle_timeout,
            body_flush_bytes,
        };
        let writer_task = tokio::spawn(async move {
            run_writer(ingress_rx, writer_service, writer_stats, writer_config).await;
        });

        tokio::spawn(async move {
            let _ = shutdown_rx.changed().await;
            receiver_stop.store(true, Ordering::Release);
            if let Err(error) = receiver_thread_waker.wake() {
                debug!(%error, "failed to wake pure Rust RIST receiver for shutdown");
            }
            match tokio::task::spawn_blocking(move || receiver_thread.join()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => warn!("pure Rust RIST receiver thread panicked"),
                Err(error) => warn!(%error, "pure Rust RIST receiver join task failed"),
            }
            if let Err(error) = writer_task.await {
                warn!(%error, "pure Rust RIST writer task failed");
            }
        });

        Ok(shutdown_tx)
    }
}

fn run_receiver<A: PureRistAuth>(
    mut receiver: Receiver,
    mut receiver_poll: Poll,
    auth: Arc<A>,
    ingress_tx: mpsc::Sender<RistIngressPacket>,
    stats: Arc<PureRistIngestStats>,
    stop: Arc<AtomicBool>,
) {
    stats.receiver_active.store(1, Ordering::Relaxed);
    stats.receiver_state.store(1, Ordering::Relaxed);
    let mut buf = vec![0u8; 65_536];
    let mut events = Events::with_capacity(2);
    let mut last_rtcp = Instant::now();

    while !stop.load(Ordering::Acquire) {
        let drained = match drain_receiver(
            &mut receiver,
            auth.as_ref(),
            &mut buf,
            &ingress_tx,
            stats.as_ref(),
        ) {
            Some(drained) => drained,
            None => {
                stats.receiver_exit_reason.store(2, Ordering::Relaxed);
                break;
            }
        };

        let now = Instant::now();
        if now.duration_since(last_rtcp) >= DEFAULT_RTCP_INTERVAL {
            stats.receiver_state.store(2, Ordering::Relaxed);
            if let Err(error) = receiver.poll_rtcp_and_send(now, ntp_now()) {
                if error.kind() != io::ErrorKind::WouldBlock {
                    debug!(error = %error, "pure Rust RIST RTCP poll failed");
                }
            }
            if let Err(error) = receiver.poll_session_and_send_keepalive(now) {
                if error.kind() != io::ErrorKind::WouldBlock {
                    debug!(error = %error, "pure Rust RIST keepalive poll failed");
                }
            }
            last_rtcp = now;
            stats.receiver_state.store(1, Ordering::Relaxed);
        }

        if drained == 0 {
            stats.receiver_state.store(3, Ordering::Relaxed);
            let poll_timeout = DEFAULT_RTCP_INTERVAL
                .saturating_sub(Instant::now().saturating_duration_since(last_rtcp));
            match receiver_poll.poll(&mut events, Some(poll_timeout)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                    stats.receiver_exit_reason.store(3, Ordering::Relaxed);
                    error!(%error, "pure Rust RIST readiness poll failed");
                    break;
                }
            }
            stats.receiver_state.store(1, Ordering::Relaxed);
        } else if drained == 128 {
            thread::yield_now();
        }
    }

    if stop.load(Ordering::Acquire) {
        stats.receiver_exit_reason.store(1, Ordering::Relaxed);
        info!("pure Rust RIST ingest server shutting down");
    }
    stats.receiver_active.store(0, Ordering::Relaxed);
    stats.receiver_state.store(0, Ordering::Relaxed);
}

fn drain_receiver<A: PureRistAuth>(
    receiver: &mut Receiver,
    auth: &A,
    buf: &mut [u8],
    ingress_tx: &mpsc::Sender<RistIngressPacket>,
    stats: &PureRistIngestStats,
) -> Option<usize> {
    let mut drained = 0;
    for _ in 0..128 {
        let received = match receiver.try_recv(buf) {
            Ok(ReceiverRead::Payload(peer, payload)) => (peer, payload),
            Ok(ReceiverRead::Control) => {
                drained += 1;
                continue;
            }
            Ok(ReceiverRead::Empty) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                drained += 1;
                stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                debug!(error = %error, "invalid pure Rust RIST datagram discarded");
                continue;
            }
            Err(error) => {
                stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                error!(error = %error, "pure Rust RIST receive failed");
                break;
            }
        };

        let (peer, payload) = received;
        drained += 1;
        stats.received_packets.fetch_add(1, Ordering::Relaxed);
        stats
            .protocol_missing_packets
            .fetch_add(payload.newly_missing.len() as u64, Ordering::Relaxed);
        stats
            .last_received_sequence
            .store(payload.sequence as u64, Ordering::Relaxed);
        if !auth.authenticate(&peer) {
            debug!(peer = %peer, "pure Rust RIST peer rejected");
            continue;
        }

        if !enqueue_ingress_packet(ingress_tx, stats, peer, payload) {
            return None;
        }
    }

    Some(drained)
}

fn enqueue_ingress_packet(
    ingress_tx: &mpsc::Sender<RistIngressPacket>,
    stats: &PureRistIngestStats,
    peer: SocketAddr,
    payload: ReceivedPayload,
) -> bool {
    let depth = stats.begin_enqueue();
    let overflow_epoch = stats.overflow_packets.load(Ordering::Relaxed);
    match ingress_tx.try_send(RistIngressPacket {
        peer,
        payload,
        overflow_epoch,
    }) {
        Ok(()) => {
            stats.record_enqueued(depth);
            true
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            stats.cancel_enqueue();
            let overflow_packets = stats.overflow_packets.fetch_add(1, Ordering::Relaxed) + 1;
            if overflow_packets.is_power_of_two() {
                warn!(
                    overflow_packets,
                    queue_capacity_packets = stats.queue_capacity_packets.load(Ordering::Relaxed),
                    "pure Rust RIST receive queue overflow; packet dropped"
                );
            }
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            stats.cancel_enqueue();
            false
        }
    }
}

async fn run_writer(
    mut ingress_rx: mpsc::Receiver<RistIngressPacket>,
    service: Arc<UploadResponseService>,
    stats: Arc<PureRistIngestStats>,
    config: RistWriterConfig,
) {
    stats.writer_active.store(1, Ordering::Relaxed);
    stats.writer_state.store(1, Ordering::Relaxed);
    let mut requests: HashMap<SocketAddr, PureRistRequest> = HashMap::new();
    let mut overflow_epoch = 0;
    let mut idle_poll = tokio::time::interval(DEFAULT_POLL_INTERVAL);
    idle_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut completion_tasks = JoinSet::new();

    loop {
        while let Some(result) = completion_tasks.try_join_next() {
            log_completion_task_result(result);
        }

        tokio::select! {
            biased;
            packet = ingress_rx.recv() => {
                let Some(packet) = packet else { break };
                stats.record_dequeue();
                stats.writer_state.store(2, Ordering::Relaxed);

                if packet.overflow_epoch != overflow_epoch {
                    for (peer, failed) in requests.drain() {
                        warn!(
                            stream_id = failed.stream.stream_id(),
                            %peer,
                            overflow_packets = packet.overflow_epoch - overflow_epoch,
                            "aborting pure Rust RIST stream at receive-queue discontinuity"
                        );
                        abort_request_in_background(&mut completion_tasks, failed);
                    }
                    overflow_epoch = packet.overflow_epoch;
                }

                process_ingress_packet(
                    &service,
                    &mut requests,
                    &mut completion_tasks,
                    packet,
                    &config,
                ).await;
                stats.writer_state.store(1, Ordering::Relaxed);
            }
            _ = idle_poll.tick() => {
                finish_idle_requests(
                    &service,
                    &mut requests,
                    &mut completion_tasks,
                    config.session_idle_timeout,
                );
            }
            Some(result) = completion_tasks.join_next(), if !completion_tasks.is_empty() => {
                log_completion_task_result(result);
            }
        }
    }

    stats.queue_depth_packets.store(0, Ordering::Relaxed);
    stats.writer_active.store(0, Ordering::Relaxed);
    stats.writer_state.store(0, Ordering::Relaxed);
    for request in requests.into_values() {
        finish_request_in_background(&mut completion_tasks, &service, request);
    }
    drain_completion_tasks(
        &mut completion_tasks,
        Duration::from_millis(service.timeouts().response_deadline_ms),
    )
    .await;
}

async fn process_ingress_packet(
    service: &Arc<UploadResponseService>,
    requests: &mut HashMap<SocketAddr, PureRistRequest>,
    completion_tasks: &mut JoinSet<()>,
    packet: RistIngressPacket,
    config: &RistWriterConfig,
) {
    let peer = packet.peer;
    if let std::collections::hash_map::Entry::Vacant(entry) = requests.entry(peer) {
        match open_request(
            service,
            config.local_addr,
            peer,
            config.profile,
            config.flow_id,
            config.body_flush_bytes,
        )
        .await
        {
            Some(opened) => {
                entry.insert(opened);
            }
            None => return,
        }
    }

    let ordered = {
        let Some(opened) = requests.get_mut(&peer) else {
            return;
        };
        opened.last_payload_at = Instant::now();
        opened.ordered_payloads.push(packet.payload)
    };
    let ordered = match ordered {
        Ok(ordered) => ordered,
        Err(error) => {
            let stream_id = requests
                .get(&peer)
                .map(|opened| opened.stream.stream_id())
                .unwrap_or_default();
            error!(
                stream_id,
                %peer,
                error = %error,
                "pure Rust RIST sequence gap exceeded reorder bound; aborting stream"
            );
            if let Some(failed) = requests.remove(&peer) {
                abort_request_in_background(completion_tasks, failed);
            }
            return;
        }
    };

    for payload in ordered {
        let result = match requests.get_mut(&peer) {
            Some(opened) => append_payload(service.as_ref(), opened, &payload.payload).await,
            None => return,
        };
        if let Err(error) = result {
            let stream_id = requests
                .get(&peer)
                .map(|opened| opened.stream.stream_id())
                .unwrap_or_default();
            error!(
                stream_id,
                %peer,
                error = %error,
                "failed to write pure Rust RIST body; closing stream"
            );
            if let Some(failed) = requests.remove(&peer) {
                abort_request_in_background(completion_tasks, failed);
            }
            return;
        }
    }
}

fn finish_idle_requests(
    service: &Arc<UploadResponseService>,
    requests: &mut HashMap<SocketAddr, PureRistRequest>,
    completion_tasks: &mut JoinSet<()>,
    session_idle_timeout: Duration,
) {
    let now = Instant::now();
    let idle_peers = requests
        .iter()
        .filter_map(|(peer, opened)| {
            (now.duration_since(opened.last_payload_at) >= session_idle_timeout).then_some(*peer)
        })
        .collect::<Vec<_>>();
    for peer in idle_peers {
        if let Some(finished) = requests.remove(&peer) {
            let pending_packets = finished.ordered_payloads.pending_len();
            if pending_packets > 0 {
                warn!(
                    stream_id = finished.stream.stream_id(),
                    %peer,
                    pending_packets,
                    "aborting idle pure Rust RIST stream with an unresolved sequence gap"
                );
                abort_request_in_background(completion_tasks, finished);
            } else {
                finish_request_in_background(completion_tasks, service, finished);
            }
        }
    }
}

async fn open_request(
    service: &Arc<UploadResponseService>,
    local_addr: SocketAddr,
    peer: SocketAddr,
    profile: PureRistProfile,
    flow_id: u32,
    body_flush_bytes: usize,
) -> Option<PureRistRequest> {
    let stream = match service.try_open_stream().await {
        Ok(stream) => stream,
        Err(error) => {
            debug!(%peer, error = %error, "could not open pure Rust RIST stream");
            return None;
        }
    };

    let stream_id = stream.stream_id();
    let response_rx = service.register_response(stream_id).await;

    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: format!("/rist-pure/{}/{}", profile.as_str(), local_addr.port()).into_bytes(),
        headers: vec![
            HeaderField {
                name: b"x-rist-backend".to_vec(),
                value: b"pure-rust".to_vec(),
            },
            HeaderField {
                name: b"x-rist-profile".to_vec(),
                value: profile.as_str().as_bytes().to_vec(),
            },
            HeaderField {
                name: b"x-rist-flow-id".to_vec(),
                value: flow_id.to_string().into_bytes(),
            },
            HeaderField {
                name: b"x-rist-peer-addr".to_vec(),
                value: peer.to_string().into_bytes(),
            },
            HeaderField {
                name: b"x-rist-local-addr".to_vec(),
                value: local_addr.to_string().into_bytes(),
            },
        ],
    });

    if let Err(error) = service.write_request_headers(stream_id, headers).await {
        error!(stream_id, error = %error, "failed to write pure Rust RIST headers");
        stream.close().await;
        return None;
    }

    debug!(stream_id, peer = %peer, "pure Rust RIST request opened");
    Some(PureRistRequest {
        stream,
        response_rx,
        pending: BytesMut::with_capacity(body_flush_bytes),
        body_flush_bytes,
        ordered_payloads: OrderedPayloadBuffer::new(MAX_REORDERED_PACKETS),
        last_payload_at: Instant::now(),
    })
}

async fn append_payload(
    service: &UploadResponseService,
    request: &mut PureRistRequest,
    payload: &[u8],
) -> Result<(), String> {
    if payload.is_empty() {
        return Ok(());
    }

    let stream_id = request.stream.stream_id();
    let slot_bytes = request.body_flush_bytes;
    request.pending.extend_from_slice(payload);
    debug!(
        stream_id,
        payload_bytes = payload.len(),
        pending_bytes = request.pending.len(),
        "pure Rust RIST payload buffered"
    );

    while request.pending.len() >= slot_bytes {
        let chunk = request.pending.split_to(slot_bytes).freeze();
        let chunk_bytes = chunk.len();
        service.append_request_body(stream_id, chunk).await?;
        debug!(
            stream_id,
            chunk_bytes,
            pending_bytes = request.pending.len(),
            "pure Rust RIST body slot written"
        );
    }
    Ok(())
}

fn abort_request_in_background(completion_tasks: &mut JoinSet<()>, request: PureRistRequest) {
    completion_tasks.spawn(async move {
        request.stream.close().await;
    });
}

fn finish_request_in_background(
    completion_tasks: &mut JoinSet<()>,
    service: &Arc<UploadResponseService>,
    request: PureRistRequest,
) {
    let service = Arc::clone(service);
    completion_tasks.spawn(async move {
        finish_request(service.as_ref(), request).await;
    });
}

fn log_completion_task_result(result: Result<(), tokio::task::JoinError>) {
    match result {
        Ok(()) => {}
        Err(error) if error.is_cancelled() => {}
        Err(error) => warn!(%error, "pure Rust RIST completion task failed"),
    }
}

async fn drain_completion_tasks(completion_tasks: &mut JoinSet<()>, deadline: Duration) {
    let drain = async {
        while let Some(result) = completion_tasks.join_next().await {
            log_completion_task_result(result);
        }
    };
    if tokio::time::timeout(deadline, drain).await.is_ok() {
        return;
    }

    let remaining = completion_tasks.len();
    warn!(
        remaining,
        "aborting pure Rust RIST completion tasks at shutdown deadline"
    );
    completion_tasks.abort_all();
    while let Some(result) = completion_tasks.join_next().await {
        log_completion_task_result(result);
    }
}

async fn finish_request(service: &UploadResponseService, mut request: PureRistRequest) {
    let stream_id = request.stream.stream_id();

    if !request.pending.is_empty() {
        let final_bytes = request.pending.len();
        if let Err(error) = service
            .append_request_body(stream_id, request.pending.split().freeze())
            .await
        {
            error!(stream_id, error = %error, "failed to write pure Rust RIST final body");
        } else {
            debug!(
                stream_id,
                final_bytes, "pure Rust RIST final body slot written"
            );
        }
    }

    if let Err(error) = service.end_request(stream_id).await {
        error!(stream_id, error = %error, "failed to end pure Rust RIST request");
    }

    debug!(
        stream_id,
        "pure Rust RIST request complete, waiting for response"
    );

    let timeout_duration = Duration::from_millis(service.timeouts().response_deadline_ms);
    match tokio::time::timeout(timeout_duration, request.response_rx).await {
        Ok(Ok(Ok(cached))) => {
            debug!(
                stream_id,
                status = ?cached.status,
                len = cached.body.len(),
                "pure Rust RIST response received"
            );
        }
        Ok(Ok(Err(error))) => {
            error!(stream_id, error = %error, "pure Rust RIST response error");
            service.drop_response_channel(stream_id).await;
        }
        Ok(Err(_)) => {
            error!(stream_id, "pure Rust RIST response channel closed");
            service.drop_response_channel(stream_id).await;
        }
        Err(_) => {
            warn!(stream_id, "pure Rust RIST response timeout");
            service.drop_response_channel(stream_id).await;
        }
    }

    request.stream.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TailSlot, UploadResponseConfig};
    use rist_core_pure::time::ntp_now;
    use rist_mio_pure::MainMioSender;
    use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

    fn received_payload(sequence: u32) -> ReceivedPayload {
        ReceivedPayload {
            sequence,
            recovered: false,
            duplicate: false,
            newly_missing: Vec::new(),
            payload: vec![sequence as u8],
        }
    }

    #[test]
    fn bounded_ingress_queue_reports_overflow_and_fences_the_next_packet() {
        let stats = PureRistIngestStats::new(1, DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES);
        let (tx, mut rx) = mpsc::channel(1);
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_000));

        assert!(enqueue_ingress_packet(
            &tx,
            &stats,
            peer,
            received_payload(1)
        ));
        assert!(enqueue_ingress_packet(
            &tx,
            &stats,
            peer,
            received_payload(2)
        ));
        let first = rx.try_recv().unwrap();
        stats.record_dequeue();
        assert_eq!(first.overflow_epoch, 0);
        assert!(enqueue_ingress_packet(
            &tx,
            &stats,
            peer,
            received_payload(3)
        ));
        let after_overflow = rx.try_recv().unwrap();
        stats.record_dequeue();

        assert_eq!(after_overflow.overflow_epoch, 1);
        assert_eq!(
            stats.snapshot(),
            PureRistIngestMetrics {
                received_packets: 0,
                enqueued_packets: 2,
                dequeued_packets: 2,
                overflow_packets: 1,
                queue_depth_packets: 0,
                queue_high_watermark_packets: 1,
                queue_capacity_packets: 1,
                requested_socket_receive_buffer_bytes: DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES as u64,
                effective_socket_receive_buffer_bytes: 0,
                writer_active: 0,
                writer_state: 0,
                receive_errors: 0,
                protocol_missing_packets: 0,
                last_received_sequence: 0,
                receiver_active: 0,
                receiver_exit_reason: 0,
                receiver_state: 0,
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_rist_reconstructs_an_exact_multi_slot_byte_stream() {
        const RIST_PACKET_BYTES: usize = 1_316;
        const SLOT_KB: usize = 47;
        const SLOT_BYTES: usize = SLOT_KB * 1_024;
        // 231 body slots is exactly 8,448 full RIST packets. This test crosses
        // the 8,192-packet recovery window at approximately 21 Mbit/s.
        const BODY_SLOTS: usize = 231;
        const PAYLOAD_BYTES: usize = SLOT_BYTES * BODY_SLOTS;

        assert_eq!(PAYLOAD_BYTES % RIST_PACKET_BYTES, 0);
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slot_size_kb: SLOT_KB,
            slots_per_stream: 256,
            response_timeout_ms: 1_000,
        }));
        let probe = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let ingest_addr = probe.local_addr().unwrap();
        drop(probe);
        let ingest = PureRistIngest::new(service.clone()).with_profile(PureRistProfile::Main);
        let stats = ingest.stats();
        let shutdown = ingest.start(ingest_addr).await.unwrap();

        let mut sender = MainMioSender::connect(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ingest_addr,
            DEFAULT_FLOW_ID,
            16_384,
        )
        .unwrap();
        let expected: Vec<u8> = (0..PAYLOAD_BYTES)
            .map(|index| ((index * 31 + index / 188) % 251) as u8)
            .collect();
        let sender_payload = expected.clone();
        let sender_summary = tokio::task::spawn_blocking(move || {
            let mut feedback = vec![0u8; 65_536];
            for (index, chunk) in sender_payload.chunks(RIST_PACKET_BYTES).enumerate() {
                sender
                    .send_payload(chunk, ntp_now(), Instant::now())
                    .unwrap();
                if index % 8 == 7 {
                    let _ = sender.poll_rtcp_and_send(Instant::now(), ntp_now());
                    let _ = sender.poll_session_and_send_keepalive(
                        Instant::now(),
                        GreKeepalive::librist_default(RIST_KEEPALIVE_ID),
                    );
                    while let Ok(Some(_)) = sender.try_recv_feedback_and_retransmit(&mut feedback) {
                    }
                    std::thread::sleep(Duration::from_millis(4));
                }
            }
            (sender.stats(), sender.pending_send_len())
        })
        .await
        .expect("pure RIST sender task failed");

        let stream_result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(stream) = service
                    .active_streams()
                    .await
                    .into_iter()
                    .find(|stream| stream.request_last > BODY_SLOTS)
                {
                    break stream;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let stream = match stream_result {
            Ok(stream) => stream,
            Err(_) => panic!(
                "pure RIST body slots did not arrive; ingest={:?}; sender={:?}; active_streams={:?}",
                stats.snapshot(),
                sender_summary,
                service.active_streams().await
            ),
        };

        let mut actual = Vec::with_capacity(PAYLOAD_BYTES);
        for slot in 2..=(BODY_SLOTS + 1) {
            match service.tail_request(stream.stream_id, slot).await {
                Some(TailSlot::Body(bytes)) => actual.extend_from_slice(&bytes),
                other => panic!("expected body at slot {slot}, got {other:?}"),
            }
        }
        assert_eq!(actual.len(), expected.len());
        let first_mismatch = actual
            .iter()
            .zip(&expected)
            .position(|(actual, expected)| actual != expected);
        assert_eq!(
            first_mismatch,
            None,
            "first mismatch at byte {:?}, RIST packet {:?}",
            first_mismatch,
            first_mismatch.map(|offset| offset / RIST_PACKET_BYTES)
        );
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pure_rist_flushes_before_the_cache_slot_is_full_when_configured() {
        const RIST_PACKET_BYTES: usize = 1_316;
        const FLUSH_BYTES: usize = RIST_PACKET_BYTES * 2;
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slot_size_kb: 47,
            slots_per_stream: 16,
            response_timeout_ms: 1_000,
        }));
        let probe = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let ingest_addr = probe.local_addr().unwrap();
        drop(probe);
        let shutdown = PureRistIngest::new(service.clone())
            .with_profile(PureRistProfile::Main)
            .with_body_flush_bytes(FLUSH_BYTES)
            .start(ingest_addr)
            .await
            .unwrap();

        let mut sender = MainMioSender::connect(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ingest_addr,
            DEFAULT_FLOW_ID,
            64,
        )
        .unwrap();
        let first = vec![0x31; RIST_PACKET_BYTES];
        let second = vec![0x52; RIST_PACKET_BYTES];
        sender
            .send_payload(&first, ntp_now(), Instant::now())
            .unwrap();
        sender
            .send_payload(&second, ntp_now(), Instant::now())
            .unwrap();

        let stream = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(stream) = service
                    .active_streams()
                    .await
                    .into_iter()
                    .find(|stream| stream.request_last >= 2)
                {
                    break stream;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("low-latency RIST body slot did not arrive");

        let body = match service.tail_request(stream.stream_id, 2).await {
            Some(TailSlot::Body(body)) => body,
            other => panic!("expected low-latency body slot, got {other:?}"),
        };
        assert_eq!(body.len(), FLUSH_BYTES);
        assert_eq!(&body[..RIST_PACKET_BYTES], first);
        assert_eq!(&body[RIST_PACKET_BYTES..], second);
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_rist_keeps_concurrent_peer_sequence_spaces_separate() {
        const RIST_PACKET_BYTES: usize = 1_316;
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 2,
            slot_size_kb: 47,
            slots_per_stream: 16,
            response_timeout_ms: 1_000,
        }));
        let probe = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let ingest_addr = probe.local_addr().unwrap();
        drop(probe);
        let shutdown = PureRistIngest::new(service.clone())
            .with_profile(PureRistProfile::Main)
            .with_body_flush_bytes(RIST_PACKET_BYTES)
            .start(ingest_addr)
            .await
            .unwrap();

        let mut first_sender = MainMioSender::connect(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ingest_addr,
            DEFAULT_FLOW_ID,
            64,
        )
        .unwrap();
        let mut second_sender = MainMioSender::connect(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ingest_addr,
            DEFAULT_FLOW_ID,
            64,
        )
        .unwrap();
        first_sender
            .send_payload(&[0x31; RIST_PACKET_BYTES], ntp_now(), Instant::now())
            .unwrap();
        second_sender
            .send_payload(&[0x52; RIST_PACKET_BYTES], ntp_now(), Instant::now())
            .unwrap();

        let streams = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let streams = service.active_streams().await;
                if streams.len() == 2 && streams.iter().all(|stream| stream.request_last >= 2) {
                    break streams;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("concurrent RIST peers did not publish separate streams");

        let mut first_bytes = Vec::new();
        for stream in streams {
            match service.tail_request(stream.stream_id, 2).await {
                Some(TailSlot::Body(body)) => first_bytes.push(body[0]),
                other => panic!("expected concurrent RIST body slot, got {other:?}"),
            }
        }
        first_bytes.sort_unstable();
        assert_eq!(first_bytes, vec![0x31, 0x52]);
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unresolved_idle_sequence_gap_aborts_instead_of_finishing() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slot_size_kb: 1,
            slots_per_stream: 8,
            response_timeout_ms: 1_000,
        }));
        let stream = service.try_open_stream().await.unwrap();
        let stream_id = stream.stream_id();
        let response_rx = service.register_response(stream_id).await;
        let mut ordered_payloads = OrderedPayloadBuffer::new(8);
        assert_eq!(ordered_payloads.push(received_payload(1)).unwrap().len(), 1);
        assert!(ordered_payloads
            .push(received_payload(3))
            .unwrap()
            .is_empty());

        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_001));
        let mut requests = HashMap::from([(
            peer,
            PureRistRequest {
                stream,
                response_rx,
                pending: BytesMut::new(),
                body_flush_bytes: 1_024,
                ordered_payloads,
                last_payload_at: Instant::now() - Duration::from_secs(1),
            },
        )]);

        let mut completion_tasks = JoinSet::new();
        finish_idle_requests(
            &service,
            &mut requests,
            &mut completion_tasks,
            Duration::from_millis(1),
        );
        assert!(requests.is_empty());
        drain_completion_tasks(&mut completion_tasks, Duration::from_millis(250)).await;
        assert!(completion_tasks.is_empty());
        assert!(service.active_streams().await.is_empty());
    }

    #[tokio::test]
    async fn completion_drain_aborts_work_at_its_deadline() {
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut completion_tasks = JoinSet::new();
        completion_tasks.spawn(async move {
            let _ = release_rx.await;
        });

        drain_completion_tasks(&mut completion_tasks, Duration::from_millis(10)).await;

        assert!(completion_tasks.is_empty());
        assert!(release_tx.send(()).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn response_wait_does_not_block_the_next_rist_request() {
        const RIST_PACKET_BYTES: usize = 1_316;
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 2,
            slot_size_kb: 47,
            slots_per_stream: 16,
            response_timeout_ms: 10_000,
        }));
        let probe = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let ingest_addr = probe.local_addr().unwrap();
        drop(probe);
        let shutdown = PureRistIngest::new(service.clone())
            .with_profile(PureRistProfile::Main)
            .with_session_idle_timeout(Duration::from_millis(25))
            .with_body_flush_bytes(RIST_PACKET_BYTES)
            .start(ingest_addr)
            .await
            .unwrap();

        let mut sender = MainMioSender::connect(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            ingest_addr,
            DEFAULT_FLOW_ID,
            64,
        )
        .unwrap();
        let first_payload = [0x31; RIST_PACKET_BYTES];
        sender
            .send_payload(&first_payload, ntp_now(), Instant::now())
            .unwrap();

        let first_stream_id = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(stream) = service
                    .active_streams()
                    .await
                    .into_iter()
                    .find(|stream| stream.request_last >= 3)
                {
                    break stream.stream_id;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("first RIST request did not become idle");

        let second_payload = [0x52; RIST_PACKET_BYTES];
        sender
            .send_payload(&second_payload, ntp_now(), Instant::now())
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if service
                    .active_streams()
                    .await
                    .into_iter()
                    .any(|stream| stream.stream_id != first_stream_id && stream.request_last >= 2)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("response wait blocked the next RIST request");

        let _ = shutdown.send(());
    }
}
