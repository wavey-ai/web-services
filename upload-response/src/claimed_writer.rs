use std::sync::Arc;

use bytes::Bytes;
use http::Response;
use http_pack::stream::StreamHeaders;
use web_service::HandlerResponse;

use crate::UploadResponseService;

/// Exclusive response writer with a renewable ownership lease.
/// Dropping this handle releases only the claim that created it.
pub struct ClaimedResponseWriter {
    service: Arc<UploadResponseService>,
    stream_id: u64,
    worker_id: String,
    capability: String,
    sequence: u64,
    started: bool,
    finished: bool,
}

impl ClaimedResponseWriter {
    pub(crate) fn new(
        service: Arc<UploadResponseService>,
        stream_id: u64,
        worker_id: String,
        capability: String,
    ) -> Self {
        Self {
            service,
            stream_id,
            worker_id,
            capability,
            sequence: 1,
            started: false,
            finished: false,
        }
    }

    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Renew before the lease expires during work that does not produce response bytes.
    pub async fn renew(&self) -> Result<(), String> {
        self.service
            .renew_response_claim(self.stream_id, &self.capability)
            .await
    }

    pub async fn ensure_started(&mut self, head: Response<()>) -> Result<(), String> {
        if self.finished {
            return Err("response already finished".into());
        }
        if self.started {
            return self.renew().await;
        }
        let headers = StreamHeaders::from_response(self.stream_id, &head)
            .map_err(|error| error.to_string())?;
        self.service
            .write_response_headers_claimed(
                self.stream_id,
                &self.capability,
                self.sequence,
                headers,
            )
            .await?;
        self.sequence += 1;
        self.started = true;
        Ok(())
    }

    pub async fn send_body(&mut self, body: Bytes) -> Result<(), String> {
        if !self.started || self.finished {
            return Err("response body requires an unfinished response head".into());
        }
        for chunk in body.chunks(self.service.config().slot_bytes()) {
            self.service
                .append_response_body_claimed(
                    self.stream_id,
                    &self.capability,
                    self.sequence,
                    Bytes::copy_from_slice(chunk),
                )
                .await?;
            self.sequence += 1;
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if !self.started {
            return Err("response headers must be written before finish".into());
        }
        self.service
            .end_response_claimed(self.stream_id, &self.capability, self.sequence)
            .await?;
        self.sequence += 1;
        self.finished = true;
        Ok(())
    }

    pub async fn write_handler_response(
        &mut self,
        response: HandlerResponse,
    ) -> Result<(), String> {
        let mut head = Response::builder().status(response.status);
        if let Some(content_type) = response.content_type {
            head = head.header(http::header::CONTENT_TYPE, content_type.as_ref());
        }
        if let Some(etag) = response.etag {
            head = head.header(http::header::ETAG, etag.to_string());
        }
        for (name, value) in response.headers {
            head = head.header(name.as_ref(), value.as_ref());
        }
        self.ensure_started(head.body(()).map_err(|error| error.to_string())?)
            .await?;
        if let Some(body) = response.body {
            self.send_body(body).await?;
        }
        self.finish().await
    }

    pub async fn release(mut self) -> bool {
        let released = self
            .service
            .release_response_with_capability(self.stream_id, &self.worker_id, &self.capability)
            .await;
        self.capability.clear();
        released
    }
}

impl Drop for ClaimedResponseWriter {
    fn drop(&mut self) {
        if self.capability.is_empty() {
            return;
        }
        let service = Arc::clone(&self.service);
        let stream_id = self.stream_id;
        let worker_id = std::mem::take(&mut self.worker_id);
        let capability = std::mem::take(&mut self.capability);
        let release = async move {
            service
                .release_response_with_capability(stream_id, &worker_id, &capability)
                .await;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(release);
        } else if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() {
            runtime.block_on(release);
        }
    }
}
