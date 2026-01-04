use crate::UploadResponseService;
use bytes::Bytes;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use rist::tokio::AsyncReceiver;
use rist::Profile;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::Duration;
use tracing::{debug, error, info};

/// Auth callback for RIST connections
pub trait RistAuth: Send + Sync + 'static {
    /// Returns true to allow the connection, false to deny
    fn authenticate(&self, peer_addr: &SocketAddr) -> bool;
}

/// Default auth that allows all connections
pub struct AllowAllRist;

impl RistAuth for AllowAllRist {
    fn authenticate(&self, _peer_addr: &SocketAddr) -> bool {
        true
    }
}

/// RIST ingest server that feeds into UploadResponseService
pub struct RistIngest<A: RistAuth = AllowAllRist> {
    service: Arc<UploadResponseService>,
    #[allow(dead_code)]
    auth: Arc<A>,
    profile: Profile,
}

impl RistIngest<AllowAllRist> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAllRist),
            profile: Profile::Main,
        }
    }
}

impl<A: RistAuth> RistIngest<A> {
    pub fn with_auth(service: Arc<UploadResponseService>, auth: A) -> Self {
        Self {
            service,
            auth: Arc::new(auth),
            profile: Profile::Main,
        }
    }

    /// Set the RIST profile (Simple, Main, or Advanced)
    pub fn with_profile(mut self, profile: Profile) -> Self {
        self.profile = profile;
        self
    }

    /// Start the RIST receiver on the given address
    /// Returns a shutdown sender to stop the server
    pub async fn start(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = self.service;
        let profile = self.profile;

        // Create RIST URL for listening
        let url = format!("rist://@:{}", addr.port());

        let receiver = AsyncReceiver::bind(profile, &url)?;

        info!("RIST ingest server listening on {}", addr);

        tokio::spawn(async move {
            // Acquire stream slot for this receiver
            let _permit = match service.acquire_stream().await {
                Ok(p) => p,
                Err(e) => {
                    error!("Failed to acquire stream: {}", e);
                    return;
                }
            };

            let stream_id = service.next_id();
            debug!(stream_id, "RIST receiver started");

            // Slot 1: Headers
            let headers = StreamHeaders::Request(StreamRequestHeaders {
                stream_id,
                version: HttpVersion::Http11,
                method: b"POST".to_vec(),
                scheme: None,
                authority: None,
                path: format!("/rist/{}", addr.port()).into_bytes(),
                headers: vec![HeaderField {
                    name: b"x-rist-url".to_vec(),
                    value: url.clone().into_bytes(),
                }],
            });

            if let Err(e) = service.write_request_headers(stream_id, headers).await {
                error!(stream_id, error = %e, "Failed to write headers");
                return;
            }

            let slot_bytes = service.config.slot_bytes();
            let mut pending = Vec::new();

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("RIST ingest server shutting down");
                        break;
                    }
                    result = receiver.recv_timeout(Duration::from_secs(1)) => {
                        match result {
                            Ok(Some(data)) => {
                                let payload = data.payload();
                                pending.extend_from_slice(payload);

                                // Write full slots
                                while pending.len() >= slot_bytes {
                                    let chunk: Vec<u8> = pending.drain(..slot_bytes).collect();
                                    if let Err(e) = service
                                        .append_request_body(stream_id, Bytes::from(chunk))
                                        .await
                                    {
                                        error!(stream_id, error = %e, "Failed to write body");
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {
                                // Timeout, continue
                            }
                            Err(e) => {
                                error!(stream_id, error = %e, "RIST read error");
                                break;
                            }
                        }
                    }
                }
            }

            // Flush remaining data
            if !pending.is_empty() {
                if let Err(e) = service
                    .append_request_body(stream_id, Bytes::from(pending))
                    .await
                {
                    error!(stream_id, error = %e, "Failed to write final body");
                }
            }

            // End marker
            if let Err(e) = service.end_request(stream_id).await {
                error!(stream_id, error = %e, "Failed to end request");
            }

            debug!(stream_id, "RIST request complete, waiting for response");

            // Wait for response
            let rx = service.register_response(stream_id).await;
            let timeout_duration = Duration::from_millis(service.config.response_timeout_ms);

            match tokio::time::timeout(timeout_duration, rx).await {
                Ok(Ok(Ok((status, body)))) => {
                    debug!(stream_id, ?status, len = body.len(), "RIST response received");
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
        });

        Ok(shutdown_tx)
    }
}
