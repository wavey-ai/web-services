use crate::{ResponseResult, UploadResponseService, UploadStream};
use bytes::BytesMut;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use mio::event::Source;
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use rist_core_pure::packet::gre::GreKeepalive;
use rist_core_pure::packet::rtcp::NackMode;
use rist_core_pure::time::ntp_now;
use rist_core_pure::{
    CongestionControlMode, OrderedPayloadBuffer, OrderedStats, ReceivedPayload, RecoveryConfig,
};
use rist_mio_pure::{MainMioReceiver, MainReceiverEvent, SimpleMioReceiver, SimpleReceiverEvent};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;
/// The longest the receiver thread sleeps without a NACK, report or
/// keepalive due: it also bounds how late idle peers are evicted.
const MAX_RECEIVER_IDLE_WAIT: Duration = Duration::from_millis(100);
/// The shortest receiver wait, so a feedback deadline that stays due (the
/// receiver's `next_timeout` may be early) cannot spin the thread.
const MIN_RECEIVER_WAIT: Duration = Duration::from_millis(1);
/// How often the receiver thread forgets peers that went quiet.
const PEER_EVICTION_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_INGRESS_QUEUE_PACKETS: usize = 16_384;
const DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
/// Payloads one request may hold behind a gap. About 2 s at 337 Mbit/s, so
/// the reorder deadline, not this bound, decides when a gap is skipped.
const DEFAULT_REORDER_CAPACITY_PACKETS: usize = 65_536;
/// At most one skipped-gap warning per stream in this interval.
const REORDER_SKIP_WARN_INTERVAL: Duration = Duration::from_secs(1);
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
    /// Gaps given up at the reorder deadline (or at the reorder capacity, or
    /// at the end of a stream); the stream carries on past each one.
    pub reorder_gaps_skipped: u64,
    /// Sequences inside the skipped gaps.
    pub reorder_skipped_packets: u64,
    /// Payloads (late repairs) that arrived after their gap was skipped and
    /// were dropped.
    pub reorder_late_dropped: u64,
    /// Times a request's reorder buffer restarted at a sender that jumped
    /// far back for good (a restarted or re-created sender session).
    pub reorder_resyncs: u64,
    /// RTCP packets sent that carried a NACK, summed over every peer.
    pub nack_packets_sent: u64,
    /// Sequences requested in the NACK packets sent.
    pub nack_entries_sent: u64,
    /// Main profile keepalives sent, summed over every peer.
    pub keepalives_sent: u64,
    /// Main profile peers forgotten after going quiet.
    pub evicted_peers: u64,
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
    reorder_gaps_skipped: AtomicU64,
    reorder_skipped_packets: AtomicU64,
    reorder_late_dropped: AtomicU64,
    reorder_resyncs: AtomicU64,
    nack_packets_sent: AtomicU64,
    nack_entries_sent: AtomicU64,
    keepalives_sent: AtomicU64,
    evicted_peers: AtomicU64,
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
            reorder_gaps_skipped: AtomicU64::new(0),
            reorder_skipped_packets: AtomicU64::new(0),
            reorder_late_dropped: AtomicU64::new(0),
            reorder_resyncs: AtomicU64::new(0),
            nack_packets_sent: AtomicU64::new(0),
            nack_entries_sent: AtomicU64::new(0),
            keepalives_sent: AtomicU64::new(0),
            evicted_peers: AtomicU64::new(0),
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
            reorder_gaps_skipped: self.reorder_gaps_skipped.load(Ordering::Relaxed),
            reorder_skipped_packets: self.reorder_skipped_packets.load(Ordering::Relaxed),
            reorder_late_dropped: self.reorder_late_dropped.load(Ordering::Relaxed),
            reorder_resyncs: self.reorder_resyncs.load(Ordering::Relaxed),
            nack_packets_sent: self.nack_packets_sent.load(Ordering::Relaxed),
            nack_entries_sent: self.nack_entries_sent.load(Ordering::Relaxed),
            keepalives_sent: self.keepalives_sent.load(Ordering::Relaxed),
            evicted_peers: self.evicted_peers.load(Ordering::Relaxed),
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

    /// Adds what a request's reorder buffer counted since `reported`.
    fn record_reorder(&self, reported: &mut OrderedStats, current: OrderedStats) {
        let add = |counter: &AtomicU64, now: u64, before: u64| {
            if now > before {
                counter.fetch_add(now - before, Ordering::Relaxed);
            }
        };
        add(
            &self.reorder_gaps_skipped,
            current.gaps_skipped,
            reported.gaps_skipped,
        );
        add(
            &self.reorder_skipped_packets,
            current.packets_skipped,
            reported.packets_skipped,
        );
        add(
            &self.reorder_late_dropped,
            current.late_dropped,
            reported.late_dropped,
        );
        add(&self.reorder_resyncs, current.resyncs, reported.resyncs);
        *reported = current;
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
    /// The receiver session the payload came from (see [`PeerSessions`]).
    session: u64,
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
    reorder_deadline: Duration,
    reorder_capacity_packets: usize,
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
    /// Payloads released by `ordered_payloads`, reused across pushes.
    released: Vec<ReceivedPayload>,
    /// The reorder counters already added to the ingest stats.
    reported_order: OrderedStats,
    last_skip_warning_at: Option<Instant>,
    last_payload_at: Instant,
    /// The receiver session this request's sequence space belongs to.
    session: u64,
}

impl PureRistRequest {
    /// When this request next needs the writer without a new packet: a
    /// gap's reorder deadline, or the end of the idle timeout. An idle
    /// timeout too long to represent never ends.
    fn next_timer(&self, session_idle_timeout: Duration) -> Option<Instant> {
        let idle_at = self.last_payload_at.checked_add(session_idle_timeout);
        match (self.ordered_payloads.next_deadline(), idle_at) {
            (Some(deadline), Some(idle_at)) => Some(deadline.min(idle_at)),
            (deadline, idle_at) => deadline.or(idle_at),
        }
    }
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

    fn set_recovery_config(
        &mut self,
        recovery: RecoveryConfig,
        congestion_control: CongestionControlMode,
    ) {
        match self {
            Self::Simple(receiver) => receiver.set_recovery_config(recovery, congestion_control),
            Self::Main(receiver) => receiver.set_recovery_config(recovery, congestion_control),
        }
    }

    /// Reads one datagram. A Simple receiver reads its RTP socket first and
    /// then its RTCP socket (sender reports and echo requests, answered).
    fn try_recv(&mut self, buf: &mut [u8]) -> io::Result<ReceiverRead> {
        match self {
            Self::Simple(receiver) => Ok(match receiver.try_recv_event(buf)? {
                Some(SimpleReceiverEvent::Payload { from, payload }) => {
                    ReceiverRead::Payload(from, payload)
                }
                Some(SimpleReceiverEvent::Rtcp { .. }) => ReceiverRead::Control,
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

    /// When a NACK, report, echo request or keepalive is next due, for any
    /// peer. It can be early, never late.
    fn next_timeout(&self) -> Option<Instant> {
        match self {
            Self::Simple(receiver) => receiver.next_timeout(),
            Self::Main(receiver) => receiver.next_timeout(),
        }
    }

    /// Sends every due NACK and report, to every peer (not only the last
    /// one heard from). Returns whether anything was sent.
    fn poll_rtcp_and_send_all(&mut self, now: Instant, now_ntp: u64) -> io::Result<bool> {
        match self {
            Self::Simple(receiver) => Ok(receiver.poll_rtcp_and_send(now, now_ntp)?.is_some()),
            Self::Main(receiver) => Ok(receiver.poll_rtcp_and_send_all(now, now_ntp)? > 0),
        }
    }

    /// Sends each Main profile peer its keepalive when its own timer is due.
    fn poll_keepalive_all(&mut self, now: Instant) -> io::Result<usize> {
        match self {
            Self::Simple(_) => Ok(0),
            Self::Main(receiver) => {
                receiver.poll_keepalive_all(now, GreKeepalive::librist_default(RIST_KEEPALIVE_ID))
            }
        }
    }

    /// Forgets Main profile peers that sent nothing for `idle`, freeing the
    /// receiver's peer capacity for new senders.
    fn evict_idle_peers(&mut self, now: Instant, idle: Duration) -> usize {
        match self {
            Self::Simple(_) => 0,
            Self::Main(receiver) => receiver.evict_idle_peers(now, idle),
        }
    }

    /// Whether the receiver still has a session for `peer`. A Simple
    /// receiver has one session whatever the peer and never evicts it.
    fn knows_peer(&self, peer: SocketAddr) -> bool {
        match self {
            Self::Simple(_) => true,
            Self::Main(receiver) => receiver.peer_flow_count(peer).is_some(),
        }
    }

    /// The key [`Self::nack_counters`] counts `peer` under: a Simple
    /// receiver has one session whatever the peer, a Main receiver one per
    /// peer.
    fn nack_key(&self, peer: SocketAddr) -> Option<SocketAddr> {
        match self {
            Self::Simple(_) => None,
            Self::Main(_) => Some(peer),
        }
    }

    /// NACK packets and entries sent so far under `key` (summed over the
    /// peer's flows), or `None` when the receiver no longer knows the peer.
    fn nack_counters(&self, key: Option<SocketAddr>) -> Option<(u64, u64)> {
        match self {
            Self::Simple(receiver) => {
                let stats = receiver.stats();
                Some((stats.nack_packets, stats.nack_entries_sent))
            }
            Self::Main(receiver) => {
                let peer = key?;
                let flows = receiver.peer_flow_ids(peer)?;
                Some(
                    flows
                        .into_iter()
                        .filter_map(|flow_id| receiver.peer_flow_stats(peer, flow_id))
                        .fold((0, 0), |(packets, entries), stats| {
                            (
                                packets + stats.nack_packets,
                                entries + stats.nack_entries_sent,
                            )
                        }),
                )
            }
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
    recovery: Option<(RecoveryConfig, CongestionControlMode)>,
    reorder_deadline: Option<Duration>,
    reorder_capacity_packets: usize,
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
            recovery: None,
            reorder_deadline: None,
            reorder_capacity_packets: DEFAULT_REORDER_CAPACITY_PACKETS,
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
            recovery: None,
            reorder_deadline: None,
            reorder_capacity_packets: DEFAULT_REORDER_CAPACITY_PACKETS,
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

    /// Use this receiver recovery configuration instead of rist-core's
    /// default (buffer L = 1000 ms, rtt_min 5 ms, max bitrate 100 Mbit/s).
    /// Unless [`Self::with_reorder_deadline`] says otherwise, a gap is then
    /// skipped once the payload after it has waited the config's
    /// `length_max`.
    pub fn with_recovery_config(
        mut self,
        recovery: RecoveryConfig,
        congestion_control: CongestionControlMode,
    ) -> Self {
        self.recovery = Some((recovery, congestion_control));
        self
    }

    /// Skip a sequence gap the transport has not repaired once the first
    /// payload after it has waited this long, and carry on with the same
    /// request. The default is the receiver's buffer (1000 ms by default):
    /// the transport stops asking for a hole at that age.
    pub fn with_reorder_deadline(mut self, deadline: Duration) -> Self {
        self.reorder_deadline = Some(deadline);
        self
    }

    /// Bound the payloads one request holds behind gaps; past it the oldest
    /// gap is skipped early. The default (65 536) lets the deadline decide.
    pub fn with_reorder_capacity_packets(mut self, packets: usize) -> Self {
        self.reorder_capacity_packets = packets.max(1);
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
        let reorder_deadline = self.reorder_deadline.unwrap_or_else(|| {
            self.recovery.as_ref().map_or_else(
                || RecoveryConfig::default().length_max,
                |(recovery, _)| recovery.length_max,
            )
        });
        let reorder_capacity_packets = self.reorder_capacity_packets;
        let stats = self.stats;
        let mut receiver = Receiver::bind(profile, addr, flow_id)?;
        if let Some((recovery, congestion_control)) = self.recovery {
            receiver.set_recovery_config(recovery, congestion_control);
        }
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
            reorder_deadline_ms = reorder_deadline.as_millis() as u64,
            reorder_capacity_packets,
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
                        session_idle_timeout,
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
            reorder_deadline,
            reorder_capacity_packets,
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

/// NACK counters already added to the ingest stats, per receiver session.
#[derive(Default)]
struct NackCounters {
    reported: HashMap<Option<SocketAddr>, (u64, u64)>,
    last_key: Option<Option<SocketAddr>>,
}

impl NackCounters {
    /// Remembers the session a payload came from (one map lookup per
    /// change of peer, not per packet).
    fn note(&mut self, key: Option<SocketAddr>) {
        if self.last_key != Some(key) {
            self.reported.entry(key).or_insert((0, 0));
            self.last_key = Some(key);
        }
    }

    /// Adds what every known session sent since the last call, and forgets
    /// sessions the receiver no longer has.
    fn record(&mut self, receiver: &Receiver, stats: &PureRistIngestStats) {
        let mut forgotten = false;
        self.reported.retain(|key, reported| {
            let Some((packets, entries)) = receiver.nack_counters(*key) else {
                forgotten = true;
                return false;
            };
            // A re-created session counts from zero again.
            let delta = |now: u64, before: u64| if now >= before { now - before } else { now };
            stats
                .nack_packets_sent
                .fetch_add(delta(packets, reported.0), Ordering::Relaxed);
            stats
                .nack_entries_sent
                .fetch_add(delta(entries, reported.1), Ordering::Relaxed);
            *reported = (packets, entries);
            true
        });
        if forgotten {
            self.last_key = None;
        }
    }
}

/// Numbers the receiver sessions payloads come from. A Main profile peer
/// the receiver evicted and then re-created starts its sequences again
/// from scratch, so the writer must not feed them to the request (and the
/// reorder buffer) opened for the evicted session.
struct PeerSessions {
    per_peer: bool,
    ids: HashMap<SocketAddr, u64>,
    next_id: u64,
    last: Option<(SocketAddr, u64)>,
}

impl PeerSessions {
    fn new(receiver: &Receiver) -> Self {
        Self {
            per_peer: matches!(receiver, Receiver::Main(_)),
            ids: HashMap::new(),
            next_id: 0,
            last: None,
        }
    }

    /// The session `peer`'s payloads belong to now (one map lookup per
    /// change of peer, not per packet).
    fn session(&mut self, peer: SocketAddr) -> u64 {
        if !self.per_peer {
            return 0;
        }
        if let Some((last_peer, id)) = self.last {
            if last_peer == peer {
                return id;
            }
        }
        let next_id = &mut self.next_id;
        let id = *self.ids.entry(peer).or_insert_with(|| {
            *next_id += 1;
            *next_id
        });
        self.last = Some((peer, id));
        id
    }

    /// Forgets the sessions of peers the receiver no longer has, so a peer
    /// that comes back gets a new session.
    fn forget_evicted(&mut self, receiver: &Receiver) {
        self.ids.retain(|peer, _| receiver.knows_peer(*peer));
        self.last = None;
    }
}

#[allow(clippy::too_many_arguments)]
fn run_receiver<A: PureRistAuth>(
    mut receiver: Receiver,
    mut receiver_poll: Poll,
    auth: Arc<A>,
    ingress_tx: mpsc::Sender<RistIngressPacket>,
    stats: Arc<PureRistIngestStats>,
    stop: Arc<AtomicBool>,
    session_idle_timeout: Duration,
) {
    stats.receiver_active.store(1, Ordering::Relaxed);
    stats.receiver_state.store(1, Ordering::Relaxed);
    let mut buf = vec![0u8; 65_536];
    let mut events = Events::with_capacity(2);
    let mut nacks = NackCounters::default();
    let mut sessions = PeerSessions::new(&receiver);
    // A peer is forgotten well after its request finished idle, so a
    // sender that pauses briefly keeps its session.
    let peer_idle = session_idle_timeout.saturating_mul(2);
    let mut next_eviction = Instant::now() + PEER_EVICTION_INTERVAL;

    while !stop.load(Ordering::Acquire) {
        let drained = match drain_receiver(
            &mut receiver,
            auth.as_ref(),
            &mut buf,
            &ingress_tx,
            stats.as_ref(),
            &mut nacks,
            &mut sessions,
        ) {
            Some(drained) => drained,
            None => {
                stats.receiver_exit_reason.store(2, Ordering::Relaxed);
                break;
            }
        };

        // Feedback is deadline driven: NACKs go out when the receiver says
        // they are due (for every peer), not on a fixed period.
        let now = Instant::now();
        if receiver.next_timeout().is_some_and(|due| due <= now) {
            stats.receiver_state.store(2, Ordering::Relaxed);
            match receiver.poll_rtcp_and_send_all(now, ntp_now()) {
                Ok(true) => nacks.record(&receiver, stats.as_ref()),
                Ok(false) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => debug!(error = %error, "pure Rust RIST RTCP poll failed"),
            }
            match receiver.poll_keepalive_all(now) {
                Ok(sent) => {
                    stats
                        .keepalives_sent
                        .fetch_add(sent as u64, Ordering::Relaxed);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => debug!(error = %error, "pure Rust RIST keepalive poll failed"),
            }
            stats.receiver_state.store(1, Ordering::Relaxed);
        }

        if now >= next_eviction {
            let evicted = receiver.evict_idle_peers(now, peer_idle);
            if evicted > 0 {
                stats
                    .evicted_peers
                    .fetch_add(evicted as u64, Ordering::Relaxed);
                nacks.record(&receiver, stats.as_ref());
                sessions.forget_evicted(&receiver);
                debug!(evicted, "pure Rust RIST idle peers evicted");
            }
            next_eviction = now + PEER_EVICTION_INTERVAL;
        }

        if drained == 0 {
            stats.receiver_state.store(3, Ordering::Relaxed);
            let wake_at = receiver
                .next_timeout()
                .map_or(next_eviction, |due| due.min(next_eviction));
            let poll_timeout = wake_at
                .saturating_duration_since(Instant::now())
                .clamp(MIN_RECEIVER_WAIT, MAX_RECEIVER_IDLE_WAIT);
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
    nacks: &mut NackCounters,
    sessions: &mut PeerSessions,
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
        nacks.note(receiver.nack_key(peer));
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

        let session = sessions.session(peer);
        if !enqueue_ingress_packet(ingress_tx, stats, peer, session, payload) {
            return None;
        }
    }

    Some(drained)
}

fn enqueue_ingress_packet(
    ingress_tx: &mpsc::Sender<RistIngressPacket>,
    stats: &PureRistIngestStats,
    peer: SocketAddr,
    session: u64,
    payload: ReceivedPayload,
) -> bool {
    let depth = stats.begin_enqueue();
    let overflow_epoch = stats.overflow_packets.load(Ordering::Relaxed);
    match ingress_tx.try_send(RistIngressPacket {
        peer,
        session,
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
    let mut completion_tasks = JoinSet::new();
    // Sleeps until the earliest reorder deadline or idle timeout of any
    // request, instead of ticking on a fixed period.
    let timer = tokio::time::sleep(Duration::from_secs(3_600));
    tokio::pin!(timer);
    let mut timer_at: Option<Instant> = None;

    loop {
        while let Some(result) = completion_tasks.try_join_next() {
            log_completion_task_result(result);
        }

        let next_timer = requests
            .values()
            .filter_map(|request| request.next_timer(config.session_idle_timeout))
            .min();
        // Re-arm only for an earlier instant: each packet moves its idle
        // end later, and a timer that fires early finds nothing due and is
        // re-armed from the requests then.
        if let Some(at) = next_timer {
            if timer_at.is_none_or(|armed| at < armed) {
                timer.as_mut().reset(tokio::time::Instant::from_std(at));
                timer_at = Some(at);
            }
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
                    &stats,
                    packet,
                    &config,
                ).await;
                stats.writer_state.store(1, Ordering::Relaxed);
            }
            _ = &mut timer, if timer_at.is_some() => {
                // Re-arm from the requests' state on the next pass.
                timer_at = None;
                service_request_timers(
                    &service,
                    &mut requests,
                    &mut completion_tasks,
                    &stats,
                    config.session_idle_timeout,
                    Instant::now(),
                ).await;
            }
            Some(result) = completion_tasks.join_next(), if !completion_tasks.is_empty() => {
                log_completion_task_result(result);
            }
        }
    }

    stats.queue_depth_packets.store(0, Ordering::Relaxed);
    stats.writer_active.store(0, Ordering::Relaxed);
    stats.writer_state.store(0, Ordering::Relaxed);
    let peers = requests.keys().copied().collect::<Vec<_>>();
    for peer in peers {
        release_and_finish(&service, &mut requests, &mut completion_tasks, &stats, peer).await;
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
    stats: &PureRistIngestStats,
    packet: RistIngressPacket,
    config: &RistWriterConfig,
) {
    let peer = packet.peer;
    if let Some(request) = requests
        .get(&peer)
        .filter(|request| request.session != packet.session)
    {
        // The receiver evicted this peer and re-created it: its sequences
        // start again, so they get a new request. (Usually the old one
        // finished idle long before; not when the writer lags.)
        debug!(
            stream_id = request.stream.stream_id(),
            %peer,
            "pure Rust RIST peer session restarted; finishing its request"
        );
        release_and_finish(service, requests, completion_tasks, stats, peer).await;
    }
    if let std::collections::hash_map::Entry::Vacant(entry) = requests.entry(peer) {
        match open_request(service, config, peer, packet.session).await {
            Some(opened) => {
                entry.insert(opened);
            }
            None => return,
        }
    }

    let result = {
        let Some(opened) = requests.get_mut(&peer) else {
            return;
        };
        let now = Instant::now();
        opened.last_payload_at = now;
        let mut released = std::mem::take(&mut opened.released);
        let pushed = opened
            .ordered_payloads
            .push_into(packet.payload, now, &mut released);
        record_reorder(stats, opened, peer, now);
        let result = match pushed {
            // Only a fail-closed buffer refuses a push; requests use
            // `with_deadline`, which skips the oldest gap instead.
            Err(error) => Err(format!(
                "pure Rust RIST sequence gap exceeded reorder bound: {error}"
            )),
            Ok(_) => append_released(service.as_ref(), opened, &mut released).await,
        };
        released.clear();
        opened.released = released;
        result
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
            "failed to write pure Rust RIST body; aborting stream"
        );
        if let Some(failed) = requests.remove(&peer) {
            abort_request_in_background(completion_tasks, failed);
        }
    }
}

/// Releases what waited past its reorder deadline, and finishes requests
/// idle for `session_idle_timeout`: what they still hold is released past
/// its gaps first, so an unrepaired gap never turns into an abort.
async fn service_request_timers(
    service: &Arc<UploadResponseService>,
    requests: &mut HashMap<SocketAddr, PureRistRequest>,
    completion_tasks: &mut JoinSet<()>,
    stats: &PureRistIngestStats,
    session_idle_timeout: Duration,
    now: Instant,
) {
    let due = requests
        .iter()
        .filter(|(_, request)| {
            request
                .next_timer(session_idle_timeout)
                .is_some_and(|at| at <= now)
        })
        .map(|(peer, _)| *peer)
        .collect::<Vec<_>>();
    for peer in due {
        let Some(request) = requests.get_mut(&peer) else {
            continue;
        };
        if now.saturating_duration_since(request.last_payload_at) >= session_idle_timeout {
            release_and_finish(service, requests, completion_tasks, stats, peer).await;
            continue;
        }
        let mut released = std::mem::take(&mut request.released);
        request
            .ordered_payloads
            .release_expired_into(now, &mut released);
        record_reorder(stats, request, peer, now);
        let result = append_released(service.as_ref(), request, &mut released).await;
        released.clear();
        request.released = released;
        if let Err(error) = result {
            error!(
                stream_id = request.stream.stream_id(),
                %peer,
                error = %error,
                "failed to write pure Rust RIST body; aborting stream"
            );
            if let Some(failed) = requests.remove(&peer) {
                abort_request_in_background(completion_tasks, failed);
            }
        }
    }
}

/// Ends `peer`'s request: releases everything still held (skipping its
/// gaps), writes it, and finishes the request in the background.
async fn release_and_finish(
    service: &Arc<UploadResponseService>,
    requests: &mut HashMap<SocketAddr, PureRistRequest>,
    completion_tasks: &mut JoinSet<()>,
    stats: &PureRistIngestStats,
    peer: SocketAddr,
) {
    let Some(mut request) = requests.remove(&peer) else {
        return;
    };
    let held = request.ordered_payloads.pending_len();
    let mut released = std::mem::take(&mut request.released);
    request.ordered_payloads.flush_all_into(&mut released);
    record_reorder(stats, &mut request, peer, Instant::now());
    if held > 0 {
        debug!(
            stream_id = request.stream.stream_id(),
            %peer,
            held,
            "pure Rust RIST stream ended behind a gap; released past it"
        );
    }
    match append_released(service.as_ref(), &mut request, &mut released).await {
        Ok(()) => finish_request_in_background(completion_tasks, service, request),
        Err(error) => {
            error!(
                stream_id = request.stream.stream_id(),
                %peer,
                error = %error,
                "failed to write pure Rust RIST body; aborting stream"
            );
            abort_request_in_background(completion_tasks, request);
        }
    }
}

/// Adds the reorder counters a request moved since the last call to the
/// ingest stats, and warns (at most once a second per stream) when gaps
/// were skipped.
fn record_reorder(
    stats: &PureRistIngestStats,
    request: &mut PureRistRequest,
    peer: SocketAddr,
    now: Instant,
) {
    let current = request.ordered_payloads.stats();
    let reported = request.reported_order;
    if current.gaps_skipped > reported.gaps_skipped || current.resyncs > reported.resyncs {
        let warn_due = request
            .last_skip_warning_at
            .is_none_or(|at| now.saturating_duration_since(at) >= REORDER_SKIP_WARN_INTERVAL);
        if warn_due {
            request.last_skip_warning_at = Some(now);
            warn!(
                stream_id = request.stream.stream_id(),
                %peer,
                gaps_skipped = current.gaps_skipped,
                packets_skipped = current.packets_skipped,
                late_dropped = current.late_dropped,
                resyncs = current.resyncs,
                "pure Rust RIST skipped an unrepaired sequence gap; stream continues"
            );
        }
    }
    stats.record_reorder(&mut request.reported_order, current);
}

async fn append_released(
    service: &UploadResponseService,
    request: &mut PureRistRequest,
    released: &mut Vec<ReceivedPayload>,
) -> Result<(), String> {
    for payload in released.drain(..) {
        append_payload(service, request, &payload.payload).await?;
    }
    Ok(())
}

async fn open_request(
    service: &Arc<UploadResponseService>,
    config: &RistWriterConfig,
    peer: SocketAddr,
    session: u64,
) -> Option<PureRistRequest> {
    let local_addr = config.local_addr;
    let profile = config.profile;
    let flow_id = config.flow_id;
    let body_flush_bytes = config.body_flush_bytes;
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
        ordered_payloads: OrderedPayloadBuffer::with_deadline(
            config.reorder_capacity_packets,
            config.reorder_deadline,
        ),
        released: Vec::new(),
        reported_order: OrderedStats::default(),
        last_skip_warning_at: None,
        last_payload_at: Instant::now(),
        session,
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
            1,
            received_payload(1)
        ));
        assert!(enqueue_ingress_packet(
            &tx,
            &stats,
            peer,
            1,
            received_payload(2)
        ));
        let first = rx.try_recv().unwrap();
        stats.record_dequeue();
        assert_eq!(first.overflow_epoch, 0);
        assert!(enqueue_ingress_packet(
            &tx,
            &stats,
            peer,
            1,
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
                reorder_gaps_skipped: 0,
                reorder_skipped_packets: 0,
                reorder_late_dropped: 0,
                reorder_resyncs: 0,
                nack_packets_sent: 0,
                nack_entries_sent: 0,
                keepalives_sent: 0,
                evicted_peers: 0,
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

    fn writer_config(local_addr: SocketAddr, reorder_deadline: Duration) -> RistWriterConfig {
        RistWriterConfig {
            local_addr,
            profile: PureRistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
            session_idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            body_flush_bytes: 1_024,
            reorder_deadline,
            reorder_capacity_packets: DEFAULT_REORDER_CAPACITY_PACKETS,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unresolved_idle_sequence_gap_finishes_after_releasing_past_it() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slot_size_kb: 1,
            slots_per_stream: 8,
            response_timeout_ms: 1_000,
        }));
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_000));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_001));
        let stats = PureRistIngestStats::new(1, DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES);
        // A deadline far beyond the idle timeout: only the idle end skips.
        let config = writer_config(local, Duration::from_secs(60));
        let mut requests = HashMap::new();
        let mut completion_tasks = JoinSet::new();
        for sequence in [1, 3] {
            process_ingress_packet(
                &service,
                &mut requests,
                &mut completion_tasks,
                &stats,
                RistIngressPacket {
                    peer,
                    session: 1,
                    payload: received_payload(sequence),
                    overflow_epoch: 0,
                },
                &config,
            )
            .await;
        }
        let request = requests.get_mut(&peer).unwrap();
        assert_eq!(request.ordered_payloads.pending_len(), 1);
        let stream_id = request.stream.stream_id();
        request.last_payload_at = Instant::now() - Duration::from_secs(1);

        service_request_timers(
            &service,
            &mut requests,
            &mut completion_tasks,
            &stats,
            Duration::from_millis(1),
            Instant::now(),
        )
        .await;
        assert!(requests.is_empty());

        // Finished, not aborted: the held payload is written past the gap,
        // then the End marker.
        let ended = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(TailSlot::End) = service.tail_request(stream_id, 3).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(ended.is_ok(), "idle request did not end");
        match service.tail_request(stream_id, 2).await {
            Some(TailSlot::Body(body)) => assert_eq!(&body[..], &[1, 3]),
            other => panic!("expected the body past the gap, got {other:?}"),
        }
        let metrics = stats.snapshot();
        assert_eq!(metrics.reorder_gaps_skipped, 1);
        assert_eq!(metrics.reorder_skipped_packets, 1);
        service
            .complete_response(stream_id, Err("test done".to_string()))
            .await;
        drain_completion_tasks(&mut completion_tasks, Duration::from_millis(500)).await;
        assert!(completion_tasks.is_empty());
    }

    fn writer_test_service() -> Arc<UploadResponseService> {
        Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 2,
            slot_size_kb: 1,
            slots_per_stream: 8,
            response_timeout_ms: 1_000,
        }))
    }

    async fn wait_for_end(service: &UploadResponseService, stream_id: u64, slot: usize) {
        let ended = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(TailSlot::End) = service.tail_request(stream_id, slot).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(
            ended.is_ok(),
            "request {stream_id} did not end at slot {slot}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_re_created_peer_session_gets_a_new_request() {
        let service = writer_test_service();
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_000));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_001));
        let stats = PureRistIngestStats::new(1, DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES);
        let config = writer_config(local, Duration::from_secs(60));
        let mut requests = HashMap::new();
        let mut completion_tasks = JoinSet::new();
        // Session 1 ran past one 16-bit epoch; after eviction the receiver
        // extends the same sender's sequences from scratch (session 2), so
        // they are far behind the old release point.
        for (session, sequence) in [(1, 70_000), (1, 70_001), (2, 4_500), (2, 4_501)] {
            process_ingress_packet(
                &service,
                &mut requests,
                &mut completion_tasks,
                &stats,
                RistIngressPacket {
                    peer,
                    session,
                    payload: received_payload(sequence),
                    overflow_epoch: 0,
                },
                &config,
            )
            .await;
        }

        let request = requests.get(&peer).unwrap();
        assert_eq!(request.session, 2);
        let new_stream = request.stream.stream_id();
        assert_eq!(stats.snapshot().reorder_late_dropped, 0);
        let mut streams = service
            .active_streams()
            .await
            .into_iter()
            .map(|stream| stream.stream_id)
            .collect::<Vec<_>>();
        streams.sort_unstable();
        assert_eq!(streams.len(), 2, "{streams:?}");
        let old_stream = *streams.iter().find(|id| **id != new_stream).unwrap();
        // The old request was finished with its own bytes only.
        wait_for_end(&service, old_stream, 3).await;
        match service.tail_request(old_stream, 2).await {
            Some(TailSlot::Body(body)) => {
                assert_eq!(&body[..], &[70_000u32 as u8, 70_001u32 as u8])
            }
            other => panic!("expected the old session's body, got {other:?}"),
        }

        let request = requests.remove(&peer).unwrap();
        finish_request_in_background(&mut completion_tasks, &service, request);
        wait_for_end(&service, new_stream, 3).await;
        match service.tail_request(new_stream, 2).await {
            Some(TailSlot::Body(body)) => {
                assert_eq!(&body[..], &[4_500u32 as u8, 4_501u32 as u8])
            }
            other => panic!("expected the new session's body, got {other:?}"),
        }
        for stream_id in [old_stream, new_stream] {
            service
                .complete_response(stream_id, Err("test done".to_string()))
                .await;
        }
        drain_completion_tasks(&mut completion_tasks, Duration::from_millis(500)).await;
        assert!(completion_tasks.is_empty());
    }

    #[test]
    fn an_evicted_peer_comes_back_in_a_new_session() {
        let receiver =
            Receiver::bind(PureRistProfile::Main, localhost_any(), DEFAULT_FLOW_ID).unwrap();
        let mut sessions = PeerSessions::new(&receiver);
        let first = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_001));
        let second = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_002));
        let a = sessions.session(first);
        let b = sessions.session(second);
        assert_ne!(a, b);
        assert_eq!(sessions.session(first), a);
        // The receiver knows neither peer (as after their eviction).
        sessions.forget_evicted(&receiver);
        let again = sessions.session(first);
        assert!(again != a && again != b);

        let simple =
            Receiver::bind(PureRistProfile::Simple, localhost_any(), DEFAULT_FLOW_ID).unwrap();
        let mut sessions = PeerSessions::new(&simple);
        assert_eq!(sessions.session(first), sessions.session(second));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unbounded_idle_timeout_never_ends_a_request() {
        let service = writer_test_service();
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_000));
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9_001));
        let stats = PureRistIngestStats::new(1, DEFAULT_SOCKET_RECEIVE_BUFFER_BYTES);
        let mut config = writer_config(local, Duration::from_millis(1));
        config.session_idle_timeout = Duration::MAX;
        let mut requests = HashMap::new();
        let mut completion_tasks = JoinSet::new();
        for sequence in [1, 3] {
            process_ingress_packet(
                &service,
                &mut requests,
                &mut completion_tasks,
                &stats,
                RistIngressPacket {
                    peer,
                    session: 1,
                    payload: received_payload(sequence),
                    overflow_epoch: 0,
                },
                &config,
            )
            .await;
        }
        let request = requests.get(&peer).unwrap();
        let deadline = request.next_timer(config.session_idle_timeout);
        assert!(deadline.is_some(), "the held gap has a deadline");
        tokio::time::sleep(Duration::from_millis(5)).await;
        service_request_timers(
            &service,
            &mut requests,
            &mut completion_tasks,
            &stats,
            config.session_idle_timeout,
            Instant::now(),
        )
        .await;
        let request = requests.get(&peer).expect("the request must stay open");
        assert_eq!(request.ordered_payloads.pending_len(), 0);
        assert_eq!(request.next_timer(config.session_idle_timeout), None);
        assert_eq!(stats.snapshot().reorder_gaps_skipped, 1);
        let stream_id = request.stream.stream_id();
        service
            .complete_response(stream_id, Err("test done".to_string()))
            .await;
        for (_, request) in requests.drain() {
            abort_request_in_background(&mut completion_tasks, request);
        }
        drain_completion_tasks(&mut completion_tasks, Duration::from_millis(500)).await;
    }

    const TEST_PACKET_BYTES: usize = 1_316;

    fn localhost_any() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
    }

    fn free_udp_addr() -> SocketAddr {
        let probe = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        probe.local_addr().unwrap()
    }

    /// A Main profile ingest that writes each payload as its own body slot.
    async fn start_packet_per_slot_ingest(
        service: &Arc<UploadResponseService>,
        reorder_deadline: Option<Duration>,
    ) -> (SocketAddr, Arc<PureRistIngestStats>, watch::Sender<()>) {
        let ingest_addr = free_udp_addr();
        let mut ingest = PureRistIngest::new(service.clone())
            .with_profile(PureRistProfile::Main)
            .with_body_flush_bytes(TEST_PACKET_BYTES);
        if let Some(deadline) = reorder_deadline {
            ingest = ingest.with_reorder_deadline(deadline);
        }
        let stats = ingest.stats();
        let shutdown = ingest.start(ingest_addr).await.unwrap();
        (ingest_addr, stats, shutdown)
    }

    fn packet_service(num_streams: usize) -> Arc<UploadResponseService> {
        Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams,
            slot_size_kb: 47,
            slots_per_stream: 64,
            response_timeout_ms: 1_000,
        }))
    }

    /// Sends payloads filled with each of `fills`, except that the ones in
    /// `lost` are only built (kept in the sender's history, never sent).
    fn send_fills(sender: &mut MainMioSender, fills: impl IntoIterator<Item = u8>, lost: &[u8]) {
        for fill in fills {
            let payload = [fill; TEST_PACKET_BYTES];
            if lost.contains(&fill) {
                sender.build_payload(&payload, ntp_now(), Instant::now());
            } else {
                sender
                    .send_payload(&payload, ntp_now(), Instant::now())
                    .unwrap();
            }
        }
    }

    async fn wait_for_metrics(
        stats: &PureRistIngestStats,
        what: &str,
        mut done: impl FnMut(&PureRistIngestMetrics) -> bool,
    ) {
        let waited = tokio::time::timeout(Duration::from_secs(3), async {
            while !done(&stats.snapshot()) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(waited.is_ok(), "{what}; ingest={:?}", stats.snapshot());
    }

    /// Waits until the only active stream has `count` body slots and
    /// returns the fill byte of each, in order.
    async fn single_stream_fills(service: &UploadResponseService, count: usize) -> Vec<u8> {
        let stream = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let streams = service.active_streams().await;
                if let Some(stream) = streams.iter().find(|stream| stream.request_last > count) {
                    assert_eq!(streams.len(), 1, "the gap split the stream: {streams:?}");
                    break stream.clone();
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("body slots did not arrive");
        let mut fills = Vec::with_capacity(count);
        for slot in 2..=(count + 1) {
            match service.tail_request(stream.stream_id, slot).await {
                Some(TailSlot::Body(body)) => {
                    assert_eq!(body.len(), TEST_PACKET_BYTES);
                    assert!(body.iter().all(|byte| *byte == body[0]));
                    fills.push(body[0]);
                }
                other => panic!("expected body at slot {slot}, got {other:?}"),
            }
        }
        assert_eq!(service.active_streams().await.len(), 1);
        fills
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_rist_skips_an_unrepaired_gap_at_the_deadline_and_keeps_one_stream() {
        let service = packet_service(2);
        let (ingest_addr, stats, shutdown) =
            start_packet_per_slot_ingest(&service, Some(Duration::from_millis(100))).await;
        let mut sender =
            MainMioSender::connect(localhost_any(), ingest_addr, DEFAULT_FLOW_ID, 64).unwrap();

        // Packet 4 is lost and its NACKs are never read, so it is never
        // repaired: 5..10 wait behind it until the deadline.
        send_fills(&mut sender, 0..10, &[4]);
        wait_for_metrics(&stats, "the gap was not skipped", |metrics| {
            metrics.reorder_gaps_skipped == 1
        })
        .await;
        send_fills(&mut sender, 10..20, &[]);

        let fills = single_stream_fills(&service, 19).await;
        let expected = (0..20).filter(|fill| *fill != 4).collect::<Vec<u8>>();
        assert_eq!(fills, expected);
        let metrics = stats.snapshot();
        assert_eq!(metrics.reorder_gaps_skipped, 1);
        assert_eq!(metrics.reorder_skipped_packets, 1);
        assert_eq!(metrics.reorder_late_dropped, 0);
        assert!(metrics.nack_packets_sent >= 1, "{metrics:?}");
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_rist_drops_a_repair_that_arrives_after_its_gap_was_skipped() {
        let service = packet_service(2);
        let (ingest_addr, stats, shutdown) =
            start_packet_per_slot_ingest(&service, Some(Duration::from_millis(50))).await;
        let mut sender =
            MainMioSender::connect(localhost_any(), ingest_addr, DEFAULT_FLOW_ID, 64).unwrap();

        send_fills(&mut sender, 0..10, &[4]);
        wait_for_metrics(&stats, "the gap was not skipped", |metrics| {
            metrics.reorder_gaps_skipped == 1
        })
        .await;

        // Now answer the NACKs: the repair of packet 4 reaches the ingest
        // after the skip and must not be written out of order.
        let mut feedback = vec![0u8; 65_536];
        let repaired = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                while let Ok(Some(_)) = sender.try_recv_feedback_and_retransmit(&mut feedback) {}
                if stats.snapshot().reorder_late_dropped >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(
            repaired.is_ok(),
            "no late repair was dropped; ingest={:?}; sender={:?}",
            stats.snapshot(),
            sender.stats()
        );
        assert!(sender.stats().retransmitted_packets >= 1);
        send_fills(&mut sender, 10..15, &[]);

        let fills = single_stream_fills(&service, 14).await;
        let expected = (0..15).filter(|fill| *fill != 4).collect::<Vec<u8>>();
        assert_eq!(fills, expected);
        let metrics = stats.snapshot();
        assert_eq!(metrics.reorder_gaps_skipped, 1);
        assert_eq!(metrics.reorder_skipped_packets, 1);
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_rist_sends_nacks_to_every_peer() {
        let service = packet_service(2);
        let (ingest_addr, stats, shutdown) = start_packet_per_slot_ingest(&service, None).await;
        let mut first =
            MainMioSender::connect(localhost_any(), ingest_addr, DEFAULT_FLOW_ID, 64).unwrap();
        let mut second =
            MainMioSender::connect(localhost_any(), ingest_addr, DEFAULT_FLOW_ID, 64).unwrap();

        // Each peer loses one packet; the second peer is the last one heard
        // from, which is the only one a last-peer RTCP poll would serve.
        send_fills(&mut first, 0x10..0x15, &[0x12]);
        send_fills(&mut second, 0x20..0x25, &[0x22]);

        let mut feedback = vec![0u8; 65_536];
        let both_nacked = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                for sender in [&mut first, &mut second] {
                    while let Ok(Some(_)) = sender.try_recv_feedback_and_retransmit(&mut feedback) {
                    }
                }
                if first.stats().nack_requests > 0 && second.stats().nack_requests > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(
            both_nacked.is_ok(),
            "a peer got no NACK; first={:?}; second={:?}; ingest={:?}",
            first.stats(),
            second.stats(),
            stats.snapshot()
        );

        // Both repairs arrive before the deadline: two complete streams.
        let streams = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let streams = service.active_streams().await;
                if streams.len() == 2 && streams.iter().all(|stream| stream.request_last > 5) {
                    break streams;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("both repaired streams did not complete");
        let mut bodies = Vec::new();
        for stream in streams {
            let mut fills = Vec::new();
            for slot in 2..=6 {
                match service.tail_request(stream.stream_id, slot).await {
                    Some(TailSlot::Body(body)) => fills.push(body[0]),
                    other => panic!("expected body at slot {slot}, got {other:?}"),
                }
            }
            bodies.push(fills);
        }
        bodies.sort();
        assert_eq!(
            bodies,
            vec![
                (0x10..0x15).collect::<Vec<u8>>(),
                (0x20..0x25).collect::<Vec<u8>>()
            ]
        );
        let metrics = stats.snapshot();
        assert!(metrics.nack_packets_sent >= 2, "{metrics:?}");
        assert_eq!(metrics.reorder_gaps_skipped, 0);
        let _ = shutdown.send(());
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
