use crate::{
    encode_request_control, request_from_headers_slot, RequestControl, StageState,
    UploadResponseTimeouts, WorkerCapacitySummary, WorkerHeartbeat, WorkerHeartbeatUpdate,
    RESPONSE_CAPABILITY_HEADER, RESPONSE_SEQUENCE_HEADER,
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{header::CONTENT_TYPE, Request};
use http_pack::stream::{encode_frame, StreamFrame, StreamHeaders};
use reqwest::{Certificate, Client, ClientBuilder, Identity, StatusCode};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use web_service::HandlerResponse;

type RemoteResponseClaimKey = (String, u64);
type RemoteResponseClaims =
    Arc<RwLock<HashMap<RemoteResponseClaimKey, Arc<Mutex<RemoteResponseClaim>>>>>;

#[derive(Clone)]
pub struct RemoteIngressClient {
    client: Client,
    slot_bytes: usize,
    response_claims: RemoteResponseClaims,
}

#[derive(Debug)]
struct RemoteResponseClaim {
    capability: String,
    next_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStreamInfo {
    pub stream_id: u64,
    pub request_last: usize,
    pub response_owner: Option<String>,
    pub stages: BTreeMap<String, StageState>,
}

impl RemoteStreamInfo {
    pub fn stage_last(&self, stage: &str) -> usize {
        self.stages.get(stage).map(|state| state.last).unwrap_or(0)
    }

    pub fn stage_owner(&self, stage: &str) -> Option<&str> {
        self.stages
            .get(stage)
            .and_then(|state| state.owner.as_deref())
    }
}

#[derive(Debug)]
pub enum RemoteRequestSlot {
    Headers(Bytes),
    Body(Bytes),
    Control(RequestControl),
    End,
}

#[derive(Debug)]
pub enum RemoteStageSlot {
    Head(Bytes),
    Control(RequestControl),
    Body(Bytes),
    End,
}

impl RemoteIngressClient {
    pub fn new(slot_bytes: usize, insecure_tls: bool) -> Result<Self> {
        Self::new_with_timeouts(slot_bytes, insecure_tls, UploadResponseTimeouts::default())
    }

    /// Create a client with an independent remote I/O deadline.
    pub fn new_with_timeouts(
        slot_bytes: usize,
        insecure_tls: bool,
        timeouts: UploadResponseTimeouts,
    ) -> Result<Self> {
        Self::from_builder(
            slot_bytes,
            Client::builder().tls_danger_accept_invalid_certs(insecure_tls),
            timeouts.remote_io_timeout_ms,
        )
    }

    /// Create a client for a private mutual TLS control listener.
    ///
    /// `identity_pem` must contain the worker certificate and private key.
    pub fn new_with_mtls_pem(
        slot_bytes: usize,
        server_ca_pem: &[u8],
        identity_pem: &[u8],
    ) -> Result<Self> {
        Self::new_with_mtls_pem_and_timeouts(
            slot_bytes,
            server_ca_pem,
            identity_pem,
            UploadResponseTimeouts::default(),
        )
    }

    /// Create a mutual TLS client with an independent remote I/O deadline.
    pub fn new_with_mtls_pem_and_timeouts(
        slot_bytes: usize,
        server_ca_pem: &[u8],
        identity_pem: &[u8],
        timeouts: UploadResponseTimeouts,
    ) -> Result<Self> {
        let server_roots = Certificate::from_pem_bundle(server_ca_pem)
            .map_err(|error| anyhow!("failed to parse control server CA: {error}"))?;
        anyhow::ensure!(!server_roots.is_empty(), "control server CA is empty");
        let identity = Identity::from_pem(identity_pem)
            .map_err(|error| anyhow!("failed to parse worker TLS identity: {error}"))?;
        Self::from_builder(
            slot_bytes,
            Client::builder()
                .tls_certs_only(server_roots)
                .identity(identity),
            timeouts.remote_io_timeout_ms,
        )
    }

    fn from_builder(
        slot_bytes: usize,
        builder: ClientBuilder,
        remote_io_timeout_ms: u64,
    ) -> Result<Self> {
        let client = builder
            .http2_adaptive_window(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_millis(remote_io_timeout_ms))
            .build()
            .map_err(|error| anyhow!("failed to build reqwest client: {error}"))?;
        Ok(Self {
            client,
            slot_bytes: slot_bytes.max(1),
            response_claims: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    fn response_claim_key(origin: &str, stream_id: u64) -> RemoteResponseClaimKey {
        (origin.trim_end_matches('/').to_string(), stream_id)
    }

    async fn response_claim(
        &self,
        origin: &str,
        stream_id: u64,
    ) -> Result<Arc<Mutex<RemoteResponseClaim>>> {
        self.response_claims
            .read()
            .await
            .get(&Self::response_claim_key(origin, stream_id))
            .cloned()
            .ok_or_else(|| anyhow!("response capability not claimed for stream {stream_id}"))
    }

    async fn remove_response_claim_if_current(
        &self,
        key: &RemoteResponseClaimKey,
        expected: &Arc<Mutex<RemoteResponseClaim>>,
    ) {
        let mut claims = self.response_claims.write().await;
        if claims
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            claims.remove(key);
        }
    }

    async fn send_response_write(
        &self,
        origin: &str,
        stream_id: u64,
        operation: &str,
        body: Bytes,
    ) -> Result<()> {
        let claim_key = Self::response_claim_key(origin, stream_id);
        let claim_handle = self.response_claim(origin, stream_id).await?;
        let mut claim = claim_handle.lock().await;
        let sequence = claim.next_sequence;
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/response/{operation}"
            ))
            .header(RESPONSE_CAPABILITY_HEADER, &claim.capability)
            .header(RESPONSE_SEQUENCE_HEADER, sequence)
            .body(body)
            .send()
            .await
            .map_err(|error| {
                anyhow!("write response {operation} for stream {stream_id}: {error}")
            })?;
        let status = response.status();
        let response_body = response.bytes().await.unwrap_or_default();
        if !status.is_success() {
            let terminal = matches!(
                status,
                StatusCode::UNAUTHORIZED
                    | StatusCode::FORBIDDEN
                    | StatusCode::NOT_FOUND
                    | StatusCode::CONFLICT
            );
            drop(claim);
            if terminal {
                self.remove_response_claim_if_current(&claim_key, &claim_handle)
                    .await;
            }
            return Err(anyhow!(
                "write response {operation} for stream {stream_id} returned {status}: {}",
                String::from_utf8_lossy(&response_body)
            ));
        }
        claim.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("response sequence exhausted for stream {stream_id}"))?;
        Ok(())
    }

    pub async fn list_streams(&self, origin: &str) -> Result<Vec<RemoteStreamInfo>> {
        let response = self
            .client
            .get(format!("{origin}/_upload_response/streams"))
            .send()
            .await
            .map_err(|error| anyhow!("failed to list streams from {origin}: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| anyhow!("failed to read stream list from {origin}: {error}"))?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected stream list status {status} from {origin}: {body}"
        );

        let mut streams = Vec::new();
        for line in body.lines() {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() < 7 || fields[0] == "stream_id" {
                continue;
            }
            streams.push(RemoteStreamInfo {
                stream_id: fields[0]
                    .parse()
                    .map_err(|error| anyhow!("invalid stream id in {origin}: {error}"))?,
                request_last: fields[2]
                    .parse()
                    .map_err(|error| anyhow!("invalid request_last in {origin}: {error}"))?,
                response_owner: match fields[5] {
                    "" | "-" => None,
                    other => Some(other.to_string()),
                },
                stages: serde_json::from_str(fields[6])
                    .map_err(|error| anyhow!("invalid stages_json in {origin}: {error}"))?,
            });
        }

        Ok(streams)
    }

    pub async fn list_workers(&self, origin: &str) -> Result<Vec<WorkerHeartbeat>> {
        let response = self
            .client
            .get(format!("{origin}/_upload_response/workers"))
            .send()
            .await
            .map_err(|error| anyhow!("failed to list workers from {origin}: {error}"))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|error| anyhow!("failed to read worker list from {origin}: {error}"))?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected worker list status {status} from {origin}: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body)
            .map_err(|error| anyhow!("invalid worker list from {origin}: {error}"))
    }

    pub async fn worker_capacity(&self, origin: &str) -> Result<WorkerCapacitySummary> {
        let response = self
            .client
            .get(format!("{origin}/_upload_response/capacity"))
            .send()
            .await
            .map_err(|error| anyhow!("failed to read worker capacity from {origin}: {error}"))?;
        let status = response.status();
        let body = response.bytes().await.map_err(|error| {
            anyhow!("failed to read worker capacity body from {origin}: {error}")
        })?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected worker capacity status {status} from {origin}: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body)
            .map_err(|error| anyhow!("invalid worker capacity from {origin}: {error}"))
    }

    pub async fn heartbeat_worker(
        &self,
        origin: &str,
        worker_id: &str,
        update: &WorkerHeartbeatUpdate,
    ) -> Result<WorkerHeartbeat> {
        let encoded = serde_json::to_vec(update).map_err(|error| {
            anyhow!("failed to encode worker heartbeat for {worker_id}: {error}")
        })?;
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/workers/{worker_id}/heartbeat"
            ))
            .header(CONTENT_TYPE, "application/json")
            .body(encoded)
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to heartbeat worker {worker_id} to {origin}: {error}")
            })?;
        let status = response.status();
        let body = response.bytes().await.map_err(|error| {
            anyhow!("failed to read worker heartbeat body from {origin}: {error}")
        })?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected worker heartbeat status {status} from {origin}: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body)
            .map_err(|error| anyhow!("invalid worker heartbeat response from {origin}: {error}"))
    }

    pub async fn request_last(&self, origin: &str, stream_id: u64) -> Result<usize> {
        let response = self
            .client
            .get(format!(
                "{origin}/_upload_response/streams/{stream_id}/request/last"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to read request_last for {stream_id}: {error}"))?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            anyhow!("failed to read request_last body for {stream_id}: {error}")
        })?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected request_last status {status} for stream {stream_id}: {body}"
        );
        body.trim()
            .parse()
            .map_err(|error| anyhow!("invalid request_last for stream {stream_id}: {error}"))
    }

    pub async fn request_headers(
        &self,
        origin: &str,
        stream_id: u64,
    ) -> Result<Option<Request<()>>> {
        match self.request_slot(origin, stream_id, 1).await? {
            Some(RemoteRequestSlot::Headers(bytes)) => request_from_headers_slot(&bytes).map(Some),
            Some(_) | None => Ok(None),
        }
    }

    pub async fn request_slot(
        &self,
        origin: &str,
        stream_id: u64,
        slot_id: usize,
    ) -> Result<Option<RemoteRequestSlot>> {
        let response = self
            .client
            .get(format!(
                "{origin}/_upload_response/streams/{stream_id}/request/slots/{slot_id}"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to fetch stream {stream_id} slot {slot_id}: {error}")
            })?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let slot_type = response
            .headers()
            .get("x-upload-response-slot-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = response.bytes().await.map_err(|error| {
            anyhow!("failed to read stream {stream_id} slot {slot_id}: {error}")
        })?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected slot status {status} for stream {stream_id} slot {slot_id}"
        );
        let slot = match slot_type.as_deref().unwrap_or("body") {
            "headers" => RemoteRequestSlot::Headers(bytes),
            "control-finalize" => RemoteRequestSlot::Control(RequestControl::Finalize),
            "control-keepalive" => RemoteRequestSlot::Control(RequestControl::KeepAlive),
            "end" => RemoteRequestSlot::End,
            _ => RemoteRequestSlot::Body(bytes),
        };
        Ok(Some(slot))
    }

    pub async fn stage_last(&self, origin: &str, stream_id: u64, stage: &str) -> Result<usize> {
        let response = self
            .client
            .get(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/last"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to read stage_last for {stream_id}/{stage}: {error}")
            })?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            anyhow!("failed to read stage_last body for {stream_id}/{stage}: {error}")
        })?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected stage_last status {status} for stream {stream_id}/{stage}: {body}"
        );
        body.trim()
            .parse()
            .map_err(|error| anyhow!("invalid stage_last for stream {stream_id}/{stage}: {error}"))
    }

    pub async fn stage_slot(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        slot_id: usize,
    ) -> Result<Option<RemoteStageSlot>> {
        let response = self
            .client
            .get(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/slots/{slot_id}"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to fetch stage {stage} stream {stream_id} slot {slot_id}: {error}")
            })?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let slot_type = response
            .headers()
            .get("x-upload-response-slot-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = response.bytes().await.map_err(|error| {
            anyhow!("failed to read stage {stage} stream {stream_id} slot {slot_id}: {error}")
        })?;
        anyhow::ensure!(
            status.is_success(),
            "unexpected stage slot status {status} for stream {stream_id}/{stage} slot {slot_id}"
        );
        let slot = match slot_type.as_deref().unwrap_or("body") {
            "head" => RemoteStageSlot::Head(bytes),
            "control-finalize" => RemoteStageSlot::Control(RequestControl::Finalize),
            "control-keepalive" => RemoteStageSlot::Control(RequestControl::KeepAlive),
            "end" => RemoteStageSlot::End,
            _ => RemoteStageSlot::Body(bytes),
        };
        Ok(Some(slot))
    }

    pub async fn register_reader(
        &self,
        origin: &str,
        stream_id: u64,
        worker_id: &str,
    ) -> Result<()> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/readers/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to register reader for {stream_id}: {error}"))?;
        anyhow::ensure!(
            response.status().is_success(),
            "reader registration failed for stream {stream_id} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn register_request_reader(
        &self,
        origin: &str,
        stream_id: u64,
        worker_id: &str,
    ) -> Result<()> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/request/readers/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to register request reader for {stream_id}: {error}")
            })?;
        anyhow::ensure!(
            response.status().is_success(),
            "request reader registration failed for stream {stream_id} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn mark_request_reader_position(
        &self,
        origin: &str,
        stream_id: u64,
        worker_id: &str,
        slot_id: usize,
    ) -> Result<()> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/request/readers/{worker_id}/slots/{slot_id}"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to mark request reader position for {stream_id}: {error}")
            })?;
        anyhow::ensure!(
            response.status().is_success(),
            "request reader position update failed for stream {stream_id} slot {slot_id} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn register_stage_reader(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        worker_id: &str,
    ) -> Result<()> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/readers/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to register stage {stage} reader for {stream_id}: {error}")
            })?;
        anyhow::ensure!(
            response.status().is_success(),
            "stage {stage} reader registration failed for stream {stream_id} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn mark_stage_reader_position(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        worker_id: &str,
        slot_id: usize,
    ) -> Result<()> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/readers/{worker_id}/slots/{slot_id}"
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("failed to mark stage {stage} reader position for {stream_id}: {error}")
            })?;
        anyhow::ensure!(
            response.status().is_success(),
            "stage {stage} reader position update failed for stream {stream_id} slot {slot_id} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn unregister_reader(
        &self,
        origin: &str,
        stream_id: u64,
        worker_id: &str,
    ) -> Result<()> {
        let response = self
            .client
            .delete(format!(
                "{origin}/_upload_response/streams/{stream_id}/readers/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to unregister reader for {stream_id}: {error}"))?;
        anyhow::ensure!(
            response.status().is_success() || response.status() == StatusCode::NOT_FOUND,
            "reader unregister failed for stream {stream_id} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn try_claim_response(
        &self,
        origin: &str,
        stream_id: u64,
        worker_id: &str,
    ) -> Result<bool> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/response/claim/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to claim response for {stream_id}: {error}"))?;
        match response.status() {
            StatusCode::OK | StatusCode::CREATED => {
                let capability = response
                    .headers()
                    .get(RESPONSE_CAPABILITY_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!("claim response omitted capability for stream {stream_id}")
                    })?
                    .to_string();
                self.response_claims.write().await.insert(
                    Self::response_claim_key(origin, stream_id),
                    Arc::new(Mutex::new(RemoteResponseClaim {
                        capability,
                        next_sequence: 1,
                    })),
                );
                Ok(true)
            }
            StatusCode::CONFLICT | StatusCode::NOT_FOUND => {
                self.response_claims
                    .write()
                    .await
                    .remove(&Self::response_claim_key(origin, stream_id));
                Ok(false)
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(anyhow!(
                    "unexpected claim status {status} for stream {stream_id}: {body}"
                ))
            }
        }
    }

    pub async fn release_response(
        &self,
        origin: &str,
        stream_id: u64,
        worker_id: &str,
    ) -> Result<()> {
        let claim_key = Self::response_claim_key(origin, stream_id);
        let claim_handle = self.response_claim(origin, stream_id).await?;
        let claim = claim_handle.lock().await;
        let response = self
            .client
            .delete(format!(
                "{origin}/_upload_response/streams/{stream_id}/response/claim/{worker_id}"
            ))
            .header(RESPONSE_CAPABILITY_HEADER, &claim.capability)
            .send()
            .await
            .map_err(|error| anyhow!("failed to release response for {stream_id}: {error}"))?;
        let status = response.status();
        drop(claim);
        if status.is_success()
            || matches!(
                status,
                StatusCode::UNAUTHORIZED
                    | StatusCode::FORBIDDEN
                    | StatusCode::NOT_FOUND
                    | StatusCode::CONFLICT
            )
        {
            self.remove_response_claim_if_current(&claim_key, &claim_handle)
                .await;
        }
        anyhow::ensure!(
            status.is_success() || status == StatusCode::NOT_FOUND,
            "release response failed for stream {stream_id} with status {status}"
        );
        Ok(())
    }

    pub async fn try_claim_stage(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        worker_id: &str,
    ) -> Result<bool> {
        let response = self
            .client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/claim/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to claim stage {stage} for {stream_id}: {error}"))?;
        match response.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(true),
            StatusCode::CONFLICT | StatusCode::NOT_FOUND => Ok(false),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(anyhow!(
                    "unexpected stage claim status {status} for stream {stream_id}/{stage}: {body}"
                ))
            }
        }
    }

    pub async fn release_stage(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        worker_id: &str,
    ) -> Result<()> {
        let response = self
            .client
            .delete(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/claim/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to release stage {stage} for {stream_id}: {error}"))?;
        anyhow::ensure!(
            response.status().is_success() || response.status() == StatusCode::NOT_FOUND,
            "release stage failed for stream {stream_id}/{stage} with status {}",
            response.status()
        );
        Ok(())
    }

    pub async fn write_stage_head(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        head: Bytes,
    ) -> Result<()> {
        self.client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/head"
            ))
            .body(head)
            .send()
            .await
            .map_err(|error| anyhow!("write stage head for stream {stream_id}/{stage}: {error}"))?
            .error_for_status()
            .map_err(|error| anyhow!("write stage head for stream {stream_id}/{stage}: {error}"))?;
        Ok(())
    }

    pub async fn append_stage_body(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        body: Bytes,
    ) -> Result<()> {
        for chunk in body.chunks(self.slot_bytes) {
            if chunk.is_empty() {
                continue;
            }
            self.client
                .put(format!(
                    "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/body"
                ))
                .body(Bytes::copy_from_slice(chunk))
                .send()
                .await
                .map_err(|error| {
                    anyhow!("write stage body for stream {stream_id}/{stage}: {error}")
                })?
                .error_for_status()
                .map_err(|error| {
                    anyhow!("write stage body for stream {stream_id}/{stage}: {error}")
                })?;
        }
        Ok(())
    }

    pub async fn append_stage_control(
        &self,
        origin: &str,
        stream_id: u64,
        stage: &str,
        control: RequestControl,
    ) -> Result<()> {
        let encoded = encode_request_control(control);
        self.client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/control"
            ))
            .body(encoded)
            .send()
            .await
            .map_err(|error| {
                anyhow!("write stage control for stream {stream_id}/{stage}: {error}")
            })?
            .error_for_status()
            .map_err(|error| {
                anyhow!("write stage control for stream {stream_id}/{stage}: {error}")
            })?;
        Ok(())
    }

    pub async fn end_stage(&self, origin: &str, stream_id: u64, stage: &str) -> Result<()> {
        self.client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/stages/{stage}/end"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("finish stage {stage} for stream {stream_id}: {error}"))?
            .error_for_status()
            .map_err(|error| anyhow!("finish stage {stage} for stream {stream_id}: {error}"))?;
        Ok(())
    }

    pub async fn write_handler_response(
        &self,
        origin: &str,
        stream_id: u64,
        response: HandlerResponse,
    ) -> Result<()> {
        let mut builder = http::Response::builder().status(response.status);
        if let Some(content_type) = &response.content_type {
            builder = builder.header(CONTENT_TYPE, content_type.as_ref());
        }
        if let Some(etag) = response.etag {
            builder = builder.header(http::header::ETAG, etag.to_string());
        }
        for (name, value) in &response.headers {
            builder = builder.header(name.as_ref(), value.as_ref());
        }
        let response_head = builder
            .body(())
            .map_err(|error| anyhow!("failed to build handler response: {error}"))?;
        let headers = StreamHeaders::from_response(stream_id, &response_head).map_err(|error| {
            anyhow!("failed to encode response headers for stream {stream_id}: {error}")
        })?;
        self.write_response_headers(origin, stream_id, headers)
            .await?;
        if let Some(body) = response.body {
            self.append_response_body(origin, stream_id, body).await?;
        }
        self.end_response(origin, stream_id).await
    }

    pub async fn write_response_headers(
        &self,
        origin: &str,
        stream_id: u64,
        headers: StreamHeaders,
    ) -> Result<()> {
        let encoded = encode_frame(&StreamFrame::Headers(headers));
        self.send_response_write(origin, stream_id, "headers", Bytes::from(encoded))
            .await
    }

    pub async fn append_response_body(
        &self,
        origin: &str,
        stream_id: u64,
        body: Bytes,
    ) -> Result<()> {
        for chunk in body.chunks(self.slot_bytes) {
            if chunk.is_empty() {
                continue;
            }
            self.send_response_write(origin, stream_id, "body", Bytes::copy_from_slice(chunk))
                .await?;
        }
        Ok(())
    }

    pub async fn end_response(&self, origin: &str, stream_id: u64) -> Result<()> {
        self.send_response_write(origin, stream_id, "end", Bytes::new())
            .await
    }
}

pub async fn discover_ingress_origins(
    ingress_urls: &[String],
    discovery_dns: Option<&str>,
) -> Result<Vec<String>> {
    let mut origins = BTreeSet::new();

    for origin in ingress_urls {
        let trimmed = origin.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            origins.insert(trimmed.to_string());
        }
    }

    if let Some(discovery_dns) = discovery_dns {
        let discovery_dns = discovery_dns.trim();
        if !discovery_dns.is_empty() {
            for socket in timeout(Duration::from_secs(5), lookup_host(discovery_dns))
                .await
                .map_err(|_| anyhow!("DNS lookup timed out for {discovery_dns}"))?
                .map_err(|error| anyhow!("failed to resolve {discovery_dns}: {error}"))?
            {
                origins.insert(format!("https://{}", socket));
            }
        }
    }

    Ok(origins.into_iter().collect())
}
