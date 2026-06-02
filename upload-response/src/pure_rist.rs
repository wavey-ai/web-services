use crate::{ResponseResult, UploadResponseService, UploadStream};
use bytes::Bytes;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use rist_core_pure::packet::rtcp::NackMode;
use rist_core_pure::time::ntp_now;
use rist_core_pure::ReceivedPayload;
use rist_mio_pure::{MainMioReceiver, SimpleMioReceiver};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{oneshot, watch};
use tokio::time::{Duration, MissedTickBehavior};
use tracing::{debug, error, info, warn};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(1);
const DEFAULT_RTCP_INTERVAL: Duration = Duration::from_millis(20);

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
    pending: Vec<u8>,
}

enum Receiver {
    Simple(SimpleMioReceiver),
    Main(MainMioReceiver),
}

impl Receiver {
    fn bind(profile: PureRistProfile, addr: SocketAddr, flow_id: u32) -> io::Result<Self> {
        match profile {
            PureRistProfile::Simple => {
                SimpleMioReceiver::bind(addr, flow_id, "web-services-pure-rist", NackMode::Range)
                    .map(Self::Simple)
            }
            PureRistProfile::Main => {
                MainMioReceiver::bind(addr, flow_id, "web-services-pure-rist", NackMode::Range)
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
}

impl PureRistIngest<AllowAllPureRist> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAllPureRist),
            profile: PureRistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
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
        let mut receiver = Receiver::bind(profile, addr, flow_id)?;

        info!(
            address = %addr,
            profile = profile.as_str(),
            flow_id,
            "pure Rust RIST ingest server listening"
        );

        tokio::spawn(async move {
            let mut request = None;
            let mut buf = vec![0u8; 65_536];
            let mut poll = tokio::time::interval(DEFAULT_POLL_INTERVAL);
            poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut last_rtcp = Instant::now();

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("pure Rust RIST ingest server shutting down");
                        break;
                    }
                    _ = poll.tick() => {
                        drain_receiver(
                            &mut receiver,
                            &mut request,
                            &service,
                            auth.as_ref(),
                            addr,
                            profile,
                            flow_id,
                            &mut buf,
                        ).await;

                        let now = Instant::now();
                        if now.duration_since(last_rtcp) >= DEFAULT_RTCP_INTERVAL {
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

            if let Some(request) = request.take() {
                finish_request(service.as_ref(), request).await;
            }
        });

        Ok(shutdown_tx)
    }
}

async fn drain_receiver<A: PureRistAuth>(
    receiver: &mut Receiver,
    request: &mut Option<PureRistRequest>,
    service: &Arc<UploadResponseService>,
    auth: &A,
    local_addr: SocketAddr,
    profile: PureRistProfile,
    flow_id: u32,
    buf: &mut [u8],
) {
    for _ in 0..128 {
        let received = match receiver.try_recv_payload(buf) {
            Ok(Some(received)) => received,
            Ok(None) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => {
                error!(error = %error, "pure Rust RIST receive failed");
                break;
            }
        };

        let (peer, payload) = received;
        if !auth.authenticate(&peer) {
            debug!(peer = %peer, "pure Rust RIST peer rejected");
            continue;
        }

        if request.is_none() {
            match open_request(service, local_addr, peer, profile, flow_id).await {
                Some(opened) => *request = Some(opened),
                None => continue,
            }
        }

        if let Some(opened) = request.as_mut() {
            append_payload(service.as_ref(), opened, &payload.payload).await;
        }
    }
}

async fn open_request(
    service: &Arc<UploadResponseService>,
    local_addr: SocketAddr,
    peer: SocketAddr,
    profile: PureRistProfile,
    flow_id: u32,
) -> Option<PureRistRequest> {
    let stream = match service.open_stream().await {
        Ok(stream) => stream,
        Err(error) => {
            error!(error = %error, "failed to open pure Rust RIST stream");
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
        pending: Vec::new(),
    })
}

async fn append_payload(
    service: &UploadResponseService,
    request: &mut PureRistRequest,
    payload: &[u8],
) {
    if payload.is_empty() {
        return;
    }

    let stream_id = request.stream.stream_id();
    let slot_bytes = service.config().slot_bytes().max(1);
    request.pending.extend_from_slice(payload);
    debug!(
        stream_id,
        payload_bytes = payload.len(),
        pending_bytes = request.pending.len(),
        "pure Rust RIST payload buffered"
    );

    while request.pending.len() >= slot_bytes {
        let chunk: Vec<u8> = request.pending.drain(..slot_bytes).collect();
        let chunk_bytes = chunk.len();
        if let Err(error) = service
            .append_request_body(stream_id, Bytes::from(chunk))
            .await
        {
            error!(stream_id, error = %error, "failed to write pure Rust RIST body");
            break;
        }
        debug!(
            stream_id,
            chunk_bytes,
            pending_bytes = request.pending.len(),
            "pure Rust RIST body slot written"
        );
    }
}

async fn finish_request(service: &UploadResponseService, mut request: PureRistRequest) {
    let stream_id = request.stream.stream_id();

    if !request.pending.is_empty() {
        let final_bytes = request.pending.len();
        if let Err(error) = service
            .append_request_body(stream_id, Bytes::from(std::mem::take(&mut request.pending)))
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

    let timeout_duration = Duration::from_millis(service.config().response_timeout_ms);
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
