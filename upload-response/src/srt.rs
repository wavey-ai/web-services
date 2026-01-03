use crate::UploadResponseService;
use bytes::Bytes;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use srt::{AsyncListener, AsyncStream, ListenerCallbackAction, ListenerOption};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info};

const MAX_SRT_PACKET_SIZE: usize = 1316;
const READ_TIMEOUT_SECS: u64 = 30;

/// Auth callback for SRT connections
/// The stream_id typically contains: "bearer:<token>" or just the stream key
pub trait SrtAuth: Send + Sync + 'static {
    fn authenticate(&self, stream_id: Option<&str>) -> bool;
}

/// Default auth that allows all connections
pub struct AllowAll;

impl SrtAuth for AllowAll {
    fn authenticate(&self, _stream_id: Option<&str>) -> bool {
        true
    }
}

/// SRT ingest server that feeds into UploadResponseService
pub struct SrtIngest<A: SrtAuth = AllowAll> {
    service: Arc<UploadResponseService>,
    auth: Arc<A>,
}

impl SrtIngest<AllowAll> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAll),
        }
    }
}

impl<A: SrtAuth> SrtIngest<A> {
    pub fn with_auth(service: Arc<UploadResponseService>, auth: A) -> Self {
        Self {
            service,
            auth: Arc::new(auth),
        }
    }

    /// Start the SRT listener on the given address
    /// Returns a shutdown sender to stop the server
    pub async fn start(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        self.start_with_options(addr, false).await
    }

    /// Start the SRT listener with high-throughput mode
    /// High-throughput mode adds larger send buffer and zero peer latency for bulk transfer
    pub async fn start_high_throughput(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        self.start_with_options(addr, true).await
    }

    async fn start_with_options(
        self,
        addr: SocketAddr,
        high_throughput: bool,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = self.service;
        let auth = self.auth;

        let auth_clone = Arc::clone(&auth);

        let options: Vec<ListenerOption> = if high_throughput {
            // High-throughput mode: maximize buffer sizes and minimize latency
            vec![
                ListenerOption::TimestampBasedPacketDeliveryMode(false),
                ListenerOption::TooLatePacketDrop(false),
                ListenerOption::ReceiveBufferSize(36400000),
                ListenerOption::SendBufferSize(36400000),
                ListenerOption::PeerLatency(0),
            ]
        } else {
            // Default mode: optimized for live media with some reliability
            vec![
                ListenerOption::TimestampBasedPacketDeliveryMode(false),
                ListenerOption::TooLatePacketDrop(false),
                ListenerOption::ReceiveBufferSize(36400000),
            ]
        };

        let listener = AsyncListener::bind_with_options(addr, options)?
            .with_callback(move |stream_id: Option<&str>| {
                if auth_clone.authenticate(stream_id) {
                    ListenerCallbackAction::Allow { passphrase: None }
                } else {
                    ListenerCallbackAction::Deny
                }
            })?;

        info!("SRT ingest server listening on {}", addr);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("SRT ingest server shutting down");
                        break;
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                let service = Arc::clone(&service);
                                tokio::spawn(async move {
                                    if let Err(e) = handle_srt_connection(stream, peer_addr, service).await {
                                        error!("SRT connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("SRT accept error: {:?}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(shutdown_tx)
    }
}

async fn handle_srt_connection(
    mut stream: AsyncStream,
    peer_addr: SocketAddr,
    service: Arc<UploadResponseService>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Acquire stream slot
    let _permit = service
        .acquire_stream()
        .await
        .map_err(|e| format!("Failed to acquire stream: {}", e))?;

    let stream_id = service.next_id();
    let srt_stream_id = stream.id().cloned().unwrap_or_default();

    debug!(
        stream_id,
        srt_stream_id = %srt_stream_id,
        peer = %peer_addr,
        "SRT connection established"
    );

    // Slot 1: Headers (synthesize HTTP-like headers for consistency)
    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: format!("/srt/{}", srt_stream_id).into_bytes(),
        headers: vec![
            HeaderField {
                name: b"x-srt-stream-id".to_vec(),
                value: srt_stream_id.clone().into_bytes(),
            },
            HeaderField {
                name: b"x-peer-addr".to_vec(),
                value: peer_addr.to_string().into_bytes(),
            },
        ],
    });

    service
        .write_request_headers(stream_id, headers)
        .await
        .map_err(|e| format!("Failed to write headers: {}", e))?;

    let slot_bytes = service.config.slot_bytes();
    let mut buf = vec![0u8; MAX_SRT_PACKET_SIZE];
    let mut pending = Vec::new();

    // Read SRT packets and write to request cache
    loop {
        match timeout(Duration::from_secs(READ_TIMEOUT_SECS), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {
                // Connection closed
                debug!(stream_id, "SRT connection closed by peer");
                break;
            }
            Ok(Ok(n)) => {
                pending.extend_from_slice(&buf[..n]);

                // Write full slots
                while pending.len() >= slot_bytes {
                    let chunk: Vec<u8> = pending.drain(..slot_bytes).collect();
                    service
                        .append_request_body(stream_id, Bytes::from(chunk))
                        .await
                        .map_err(|e| format!("Failed to write body: {}", e))?;
                }
            }
            Ok(Err(e)) => {
                error!(stream_id, error = %e, "SRT read error");
                break;
            }
            Err(_) => {
                debug!(stream_id, "SRT read timeout");
                break;
            }
        }
    }

    // Flush remaining data
    if !pending.is_empty() {
        service
            .append_request_body(stream_id, Bytes::from(pending))
            .await
            .map_err(|e| format!("Failed to write final body: {}", e))?;
    }

    // End marker
    service
        .end_request(stream_id)
        .await
        .map_err(|e| format!("Failed to end request: {}", e))?;

    debug!(stream_id, "SRT request complete, waiting for response");

    // Wait for response and stream it back
    let rx = service.register_response(stream_id).await;
    let timeout_duration = Duration::from_millis(service.config.response_timeout_ms);

    match timeout(timeout_duration, rx).await {
        Ok(Ok(Ok((status, body)))) => {
            debug!(stream_id, ?status, len = body.len(), "Sending SRT response");
            if let Err(e) = stream.write_all(&body).await {
                error!(stream_id, error = %e, "Failed to send SRT response");
            }
        }
        Ok(Ok(Err(e))) => {
            error!(stream_id, error = %e, "Response error");
            service.drop_response_channel(stream_id).await;
        }
        Ok(Err(_)) => {
            error!(stream_id, "Response channel closed");
            service.drop_response_channel(stream_id).await;
        }
        Err(_) => {
            error!(stream_id, "Response timeout");
            service.drop_response_channel(stream_id).await;
        }
    }

    Ok(())
}
