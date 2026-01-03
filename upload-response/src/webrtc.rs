use crate::UploadResponseService;
use bytes::Bytes;
use futures::{select, FutureExt};
use futures_timer::Delay;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use matchbox_socket::{PeerId, PeerState, WebRtcSocket};

const CHANNEL_ID: usize = 0;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info};

/// Auth callback for WebRTC connections
pub trait WebRtcAuth: Send + Sync + 'static {
    fn authenticate(&self, peer_id: &PeerId, room: &str) -> bool;
}

/// Default auth that allows all connections
pub struct AllowAllWebRtc;

impl WebRtcAuth for AllowAllWebRtc {
    fn authenticate(&self, _peer_id: &PeerId, _room: &str) -> bool {
        true
    }
}

struct PeerContext {
    stream_id: u64,
    pending: Vec<u8>,
}

/// WebRTC Data Channel ingest server that feeds into UploadResponseService
pub struct WebRtcIngest<A: WebRtcAuth = AllowAllWebRtc> {
    service: Arc<UploadResponseService>,
    auth: Arc<A>,
}

impl WebRtcIngest<AllowAllWebRtc> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAllWebRtc),
        }
    }
}

impl<A: WebRtcAuth> WebRtcIngest<A> {
    pub fn with_auth(service: Arc<UploadResponseService>, auth: A) -> Self {
        Self {
            service,
            auth: Arc::new(auth),
        }
    }

    /// Start the WebRTC ingest on the given signaling server URL
    /// signaling_url: e.g., "ws://localhost:3536/room_id"
    pub async fn start(
        self,
        signaling_url: String,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = self.service;
        let auth = self.auth;

        let room = signaling_url
            .split('/')
            .last()
            .unwrap_or("default")
            .to_string();

        tokio::spawn(async move {
            // Use reliable data channels for request/response
            let (mut socket, loop_fut) = WebRtcSocket::new_reliable(&signaling_url);
            let loop_fut = loop_fut.fuse();
            futures::pin_mut!(loop_fut);

            let timeout = Delay::new(Duration::from_millis(10));
            futures::pin_mut!(timeout);

            let mut peer_contexts: HashMap<PeerId, PeerContext> = HashMap::new();
            let slot_bytes = service.config().slot_bytes();

            info!("WebRTC ingest listening on {}", signaling_url);

            loop {
                select! {
                    _ = (&mut timeout).fuse() => {
                        // Handle peer state changes
                        for (peer, state) in socket.update_peers() {
                            match state {
                                PeerState::Connected => {
                                    if !auth.authenticate(&peer, &room) {
                                        info!("WebRTC peer rejected: {}", peer);
                                        continue;
                                    }

                                    info!("WebRTC peer connected: {}", peer);

                                    // Acquire stream slot
                                    let permit = match service.acquire_stream().await {
                                        Ok(p) => p,
                                        Err(e) => {
                                            error!("Failed to acquire stream for peer {}: {}", peer, e);
                                            continue;
                                        }
                                    };

                                    let stream_id = service.next_id();

                                    // Write headers
                                    let headers = StreamHeaders::Request(StreamRequestHeaders {
                                        stream_id,
                                        version: HttpVersion::Http11,
                                        method: b"POST".to_vec(),
                                        scheme: None,
                                        authority: None,
                                        path: format!("/webrtc/{}/{}", room, peer).into_bytes(),
                                        headers: vec![
                                            HeaderField {
                                                name: b"x-webrtc-peer-id".to_vec(),
                                                value: peer.to_string().into_bytes(),
                                            },
                                            HeaderField {
                                                name: b"x-webrtc-room".to_vec(),
                                                value: room.clone().into_bytes(),
                                            },
                                        ],
                                    });

                                    if let Err(e) = service.write_request_headers(stream_id, headers).await {
                                        error!("Failed to write headers for peer {}: {}", peer, e);
                                        continue;
                                    }

                                    // Keep permit alive by storing it (it drops when peer disconnects)
                                    std::mem::forget(permit);

                                    peer_contexts.insert(peer, PeerContext {
                                        stream_id,
                                        pending: Vec::new(),
                                    });
                                }
                                PeerState::Disconnected => {
                                    info!("WebRTC peer disconnected: {}", peer);

                                    if let Some(ctx) = peer_contexts.remove(&peer) {
                                        // Flush remaining data
                                        if !ctx.pending.is_empty() {
                                            let _ = service.append_request_body(
                                                ctx.stream_id,
                                                Bytes::from(ctx.pending),
                                            ).await;
                                        }

                                        // End request
                                        let _ = service.end_request(ctx.stream_id).await;

                                        // Wait for response and send back
                                        let rx = service.register_response(ctx.stream_id).await;
                                        let timeout_duration = Duration::from_millis(service.config().response_timeout_ms);

                                        match tokio::time::timeout(timeout_duration, rx).await {
                                            Ok(Ok(Ok((status, body)))) => {
                                                debug!(stream_id = ctx.stream_id, ?status, len = body.len(), "Sending WebRTC response");
                                                socket.channel_mut(CHANNEL_ID).send(body, peer);
                                            }
                                            Ok(Ok(Err(e))) => {
                                                error!(stream_id = ctx.stream_id, error = %e, "Response error");
                                                service.drop_response_channel(ctx.stream_id).await;
                                            }
                                            Ok(Err(_)) => {
                                                error!(stream_id = ctx.stream_id, "Response channel closed");
                                                service.drop_response_channel(ctx.stream_id).await;
                                            }
                                            Err(_) => {
                                                error!(stream_id = ctx.stream_id, "Response timeout");
                                                service.drop_response_channel(ctx.stream_id).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Receive data from peers
                        for (peer, packet) in socket.channel_mut(CHANNEL_ID).receive() {
                            if let Some(ctx) = peer_contexts.get_mut(&peer) {
                                ctx.pending.extend_from_slice(&packet);

                                // Write full slots
                                while ctx.pending.len() >= slot_bytes {
                                    let chunk: Vec<u8> = ctx.pending.drain(..slot_bytes).collect();
                                    if let Err(e) = service.append_request_body(
                                        ctx.stream_id,
                                        Bytes::from(chunk),
                                    ).await {
                                        error!("Failed to write body for peer {}: {}", peer, e);
                                        break;
                                    }
                                }
                            }
                        }

                        timeout.reset(Duration::from_millis(10));
                    }

                    _ = &mut loop_fut => {
                        info!("WebRTC socket loop ended");
                        break;
                    }

                    _ = shutdown_rx.changed().fuse() => {
                        info!("WebRTC ingest shutting down");
                        break;
                    }
                }
            }
        });

        Ok(shutdown_tx)
    }
}
