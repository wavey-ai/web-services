use crate::{
    request_from_headers_slot, RequestControl, WorkerCapacitySummary, WorkerHeartbeat,
    WorkerHeartbeatUpdate,
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{header::CONTENT_TYPE, Request};
use http_pack::stream::{encode_frame, StreamFrame, StreamHeaders};
use reqwest::{Client, StatusCode};
use std::collections::BTreeSet;
use tokio::net::lookup_host;
use web_service::HandlerResponse;

#[derive(Clone)]
pub struct RemoteIngressClient {
    client: Client,
    slot_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStreamInfo {
    pub stream_id: u64,
    pub request_last: usize,
    pub response_owner: Option<String>,
}

#[derive(Debug)]
pub enum RemoteRequestSlot {
    Headers(Bytes),
    Body(Bytes),
    Control(RequestControl),
    End,
}

impl RemoteIngressClient {
    pub fn new(slot_bytes: usize, insecure_tls: bool) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(insecure_tls)
            .http2_adaptive_window(true)
            .build()
            .map_err(|error| anyhow!("failed to build reqwest client: {error}"))?;
        Ok(Self {
            client,
            slot_bytes: slot_bytes.max(1),
        })
    }

    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
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
            if fields.len() < 6 || fields[0] == "stream_id" {
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
            StatusCode::OK | StatusCode::CREATED => Ok(true),
            StatusCode::CONFLICT => Ok(false),
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
        let response = self
            .client
            .delete(format!(
                "{origin}/_upload_response/streams/{stream_id}/response/claim/{worker_id}"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("failed to release response for {stream_id}: {error}"))?;
        anyhow::ensure!(
            response.status().is_success() || response.status() == StatusCode::NOT_FOUND,
            "release response failed for stream {stream_id} with status {}",
            response.status()
        );
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
            builder = builder.header(CONTENT_TYPE, content_type);
        }
        if let Some(etag) = response.etag {
            builder = builder.header(http::header::ETAG, etag.to_string());
        }
        for (name, value) in &response.headers {
            builder = builder.header(name, value);
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
        self.client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/response/headers"
            ))
            .body(encoded)
            .send()
            .await
            .map_err(|error| anyhow!("write response headers for stream {stream_id}: {error}"))?
            .error_for_status()
            .map_err(|error| anyhow!("write response headers for stream {stream_id}: {error}"))?;
        Ok(())
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
            self.client
                .put(format!(
                    "{origin}/_upload_response/streams/{stream_id}/response/body"
                ))
                .body(Bytes::copy_from_slice(chunk))
                .send()
                .await
                .map_err(|error| anyhow!("write response body for stream {stream_id}: {error}"))?
                .error_for_status()
                .map_err(|error| anyhow!("write response body for stream {stream_id}: {error}"))?;
        }
        Ok(())
    }

    pub async fn end_response(&self, origin: &str, stream_id: u64) -> Result<()> {
        self.client
            .put(format!(
                "{origin}/_upload_response/streams/{stream_id}/response/end"
            ))
            .send()
            .await
            .map_err(|error| anyhow!("finish response for stream {stream_id}: {error}"))?
            .error_for_status()
            .map_err(|error| anyhow!("finish response for stream {stream_id}: {error}"))?;
        Ok(())
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
            for socket in lookup_host(discovery_dns)
                .await
                .map_err(|error| anyhow!("failed to resolve {discovery_dns}: {error}"))?
            {
                origins.insert(format!("https://{}", socket));
            }
        }
    }

    Ok(origins.into_iter().collect())
}
