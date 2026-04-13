use crate::{RemoteIngressClient, UploadResponseService};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::Response;
use http_pack::stream::StreamHeaders;
use std::sync::Arc;
use web_service::HandlerResponse;

enum ResponseCacheTarget {
    Local(Arc<UploadResponseService>),
    Remote {
        client: RemoteIngressClient,
        origin: String,
    },
}

pub struct ResponseCacheWriter {
    target: ResponseCacheTarget,
    stream_id: u64,
    slot_bytes: usize,
    started: bool,
    finished: bool,
}

impl ResponseCacheWriter {
    pub fn local(service: Arc<UploadResponseService>, stream_id: u64, slot_bytes: usize) -> Self {
        Self {
            target: ResponseCacheTarget::Local(service),
            stream_id,
            slot_bytes: slot_bytes.max(1),
            started: false,
            finished: false,
        }
    }

    pub fn remote(
        client: RemoteIngressClient,
        origin: impl Into<String>,
        stream_id: u64,
        slot_bytes: usize,
    ) -> Self {
        Self {
            target: ResponseCacheTarget::Remote {
                client,
                origin: origin.into(),
            },
            stream_id,
            slot_bytes: slot_bytes.max(1),
            started: false,
            finished: false,
        }
    }

    pub async fn ensure_started(&mut self, response_head: Response<()>) -> Result<()> {
        if self.started {
            return Ok(());
        }

        let headers =
            StreamHeaders::from_response(self.stream_id, &response_head).map_err(|error| {
                anyhow!("failed to encode response headers for stream {}: {error}", self.stream_id)
            })?;

        match &self.target {
            ResponseCacheTarget::Local(service) => service
                .write_response_headers(self.stream_id, headers)
                .await
                .map_err(|error| anyhow!(error))?,
            ResponseCacheTarget::Remote { client, origin } => {
                client
                    .write_response_headers(origin, self.stream_id, headers)
                    .await?
            }
        }

        self.started = true;
        Ok(())
    }

    pub async fn send_body(&self, body: Bytes) -> Result<()> {
        for chunk in body.chunks(self.slot_bytes) {
            if chunk.is_empty() {
                continue;
            }
            match &self.target {
                ResponseCacheTarget::Local(service) => service
                    .append_response_body(self.stream_id, Bytes::copy_from_slice(chunk))
                    .await
                    .map_err(|error| anyhow!(error))?,
                ResponseCacheTarget::Remote { client, origin } => {
                    client
                        .append_response_body(origin, self.stream_id, Bytes::copy_from_slice(chunk))
                        .await?
                }
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        anyhow::ensure!(self.started, "response headers must be written before finish");
        match &self.target {
            ResponseCacheTarget::Local(service) => service
                .end_response(self.stream_id)
                .await
                .map_err(|error| anyhow!(error))?,
            ResponseCacheTarget::Remote { client, origin } => {
                client.end_response(origin, self.stream_id).await?
            }
        }
        self.finished = true;
        Ok(())
    }

    pub async fn write_handler_response(&mut self, response: HandlerResponse) -> Result<()> {
        match &self.target {
            ResponseCacheTarget::Local(service) => service
                .write_handler_response(self.stream_id, response)
                .await
                .map_err(|error| anyhow!(error)),
            ResponseCacheTarget::Remote { client, origin } => {
                client.write_handler_response(origin, self.stream_id, response).await
            }
        }
    }
}
