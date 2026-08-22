use bytes::Bytes;
use http::StatusCode;
use http_pack::stream::{decode_frame, StreamFrame, StreamHeaders, StreamResponseHeaders};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, trace};

use crate::{CachedResponse, UploadResponseService, END_MARKER, RESPONSE_WATCHER_READER_ID};

/// Watches the response cache for complete responses and delivers them
/// to waiting clients via oneshot channels.
pub struct ResponseWatcher {
    service: Arc<UploadResponseService>,
}

/// Owns one response watcher task and stops it when the handle is dropped.
pub struct ResponseWatcherHandle {
    task: JoinHandle<()>,
}

impl ResponseWatcherHandle {
    pub fn abort(&self) {
        self.task.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

impl Future for ResponseWatcherHandle {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.task).poll(context)
    }
}

impl Drop for ResponseWatcherHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// State for tracking a response stream being assembled
struct ResponseAssembly {
    status: Option<u16>,
    headers: Vec<(String, String)>,
    body_chunks: Vec<Bytes>,
    body_bytes: usize,
    max_body_bytes: usize,
}

impl ResponseAssembly {
    fn new(max_body_bytes: usize) -> Self {
        Self {
            status: None,
            headers: Vec::new(),
            body_chunks: Vec::new(),
            body_bytes: 0,
            max_body_bytes,
        }
    }

    fn set_headers(&mut self, headers: StreamResponseHeaders) {
        self.status = Some(headers.status);
        self.headers = headers
            .headers
            .into_iter()
            .map(|header| {
                (
                    String::from_utf8_lossy(&header.name).into_owned(),
                    String::from_utf8_lossy(&header.value).into_owned(),
                )
            })
            .collect();
    }

    fn add_body(&mut self, data: Bytes) -> Result<(), String> {
        let body_bytes = self
            .body_bytes
            .checked_add(data.len())
            .ok_or_else(|| "response body size overflow".to_string())?;
        if body_bytes > self.max_body_bytes {
            return Err(format!(
                "response body exceeds {} bytes",
                self.max_body_bytes
            ));
        }
        self.body_bytes = body_bytes;
        self.body_chunks.push(data);
        Ok(())
    }

    fn finalize(self) -> Result<CachedResponse, String> {
        let status = self.status.ok_or("missing status")?;
        let status = StatusCode::from_u16(status).map_err(|e| e.to_string())?;

        let total_len = self.body_chunks.iter().try_fold(0usize, |total, chunk| {
            total
                .checked_add(chunk.len())
                .ok_or("response body too large")
        })?;
        let mut body = Vec::with_capacity(total_len);
        for chunk in self.body_chunks {
            body.extend_from_slice(&chunk);
        }

        Ok(CachedResponse {
            status,
            body: Bytes::from(body),
            headers: self.headers,
        })
    }
}

impl ResponseWatcher {
    /// Create a new response watcher
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self { service }
    }

    /// Retain source compatibility with the former polling watcher.
    pub fn with_poll_interval_ms(self, _ms: u64) -> Self {
        self
    }

    /// Start the watcher loop in a background task
    pub fn spawn(self) -> ResponseWatcherHandle {
        let task = tokio::spawn(async move {
            self.watch_loop().await;
        });
        ResponseWatcherHandle { task }
    }

    /// Process all response streams after each cache update.
    async fn watch_loop(self) {
        let updates = self.service.response_updates();
        let num_streams = self.service.config().num_streams;
        let max_body_bytes = self
            .service
            .config()
            .slot_bytes()
            .saturating_mul(self.service.config().slots_per_stream);

        // Track the stream currently assigned to each slot and the last slot seen for it.
        let mut stream_ids: Vec<u64> = vec![0; num_streams];
        let mut last_seen: Vec<usize> = vec![0; num_streams];
        let mut assemblies: HashMap<u64, ResponseAssembly> = HashMap::new();

        loop {
            let notified = updates.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let dirty_slots = self.service.take_response_dirty_slots();
            if dirty_slots.is_empty() {
                notified.await;
                continue;
            }

            for stream_idx in dirty_slots {
                let stream_id = self.service.slot_stream_id(stream_idx).unwrap_or(0);
                let previous_stream_id = stream_ids[stream_idx];

                if stream_id == 0 {
                    if previous_stream_id != 0 {
                        assemblies.remove(&previous_stream_id);
                        last_seen[stream_idx] = 0;
                        stream_ids[stream_idx] = 0;
                    }
                    continue;
                }

                if previous_stream_id != stream_id {
                    if previous_stream_id != 0 {
                        assemblies.remove(&previous_stream_id);
                    }
                    stream_ids[stream_idx] = stream_id;
                    last_seen[stream_idx] = 0;
                }

                if !self
                    .service
                    .is_response_reader_registered(stream_id, RESPONSE_WATCHER_READER_ID)
                    .await
                {
                    assemblies.remove(&stream_id);
                    last_seen[stream_idx] = 0;
                    continue;
                }

                let current_last = self.service.response_last(stream_id).unwrap_or(0);
                let seen = last_seen[stream_idx];

                if current_last <= seen {
                    continue;
                }

                let mut processed_last = seen;
                for slot_id in (seen + 1)..=current_last {
                    let Some(bytes) = self.service.response_get(stream_id, slot_id).await else {
                        break;
                    };
                    if slot_id == 1 {
                        match decode_frame(&bytes) {
                            Ok(StreamFrame::Headers(StreamHeaders::Response(resp)))
                                if resp.stream_id == stream_id =>
                            {
                                trace!(stream_id, status = resp.status, "Response headers");
                                let mut assembly = ResponseAssembly::new(max_body_bytes);
                                assembly.set_headers(resp);
                                assemblies.insert(stream_id, assembly);
                            }
                            Ok(_) => {
                                self.deliver_response(
                                    stream_id,
                                    Err("slot 1 was not matching response headers".to_string()),
                                )
                                .await;
                                processed_last = slot_id;
                                break;
                            }
                            Err(error) => {
                                self.deliver_response(
                                    stream_id,
                                    Err(format!("invalid response headers: {error}")),
                                )
                                .await;
                                processed_last = slot_id;
                                break;
                            }
                        }
                    } else if bytes.as_ref() == END_MARKER {
                        debug!(stream_id, "Response end");
                        if let Some(assembly) = assemblies.remove(&stream_id) {
                            let result = assembly.finalize();
                            self.deliver_response(stream_id, result).await;
                        }
                    } else {
                        trace!(stream_id, len = bytes.len(), "Response body chunk");
                        if let Some(assembly) = assemblies.get_mut(&stream_id) {
                            if let Err(error) = assembly.add_body(bytes) {
                                assemblies.remove(&stream_id);
                                self.deliver_response(stream_id, Err(error)).await;
                                processed_last = slot_id;
                                break;
                            }
                        }
                    }
                    let _ = self
                        .service
                        .mark_response_reader_position(
                            stream_id,
                            RESPONSE_WATCHER_READER_ID,
                            slot_id,
                        )
                        .await;
                    processed_last = slot_id;
                }

                last_seen[stream_idx] = processed_last;
            }

            notified.await;
        }
    }

    /// Deliver a completed response to the waiting client
    async fn deliver_response(&self, stream_id: u64, result: Result<CachedResponse, String>) {
        self.service.complete_response(stream_id, result).await;
        debug!(stream_id, "Response delivered");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UploadResponseConfig;

    #[tokio::test]
    async fn test_response_assembly() {
        let config = UploadResponseConfig::default();
        let service = Arc::new(crate::UploadResponseService::new(config));
        let upload_stream = service.open_stream().await.unwrap();
        let stream_id = upload_stream.stream_id();

        // Register a response channel
        let rx = service.register_response(stream_id).await;

        // Write response: headers + body + end
        let headers = StreamHeaders::Response(StreamResponseHeaders {
            stream_id,
            version: http_pack::HttpVersion::Http11,
            status: 200,
            headers: vec![],
        });
        service
            .write_response_headers(stream_id, headers)
            .await
            .unwrap();
        service
            .append_response_body(stream_id, Bytes::from("hello"))
            .await
            .unwrap();
        service.end_response(stream_id).await.unwrap();

        // Create watcher and process manually
        let watcher = ResponseWatcher::new(service.clone());

        // Simulate one iteration
        let mut assemblies = HashMap::new();

        for slot_id in 1..=3 {
            if let Some(bytes) = service.response_get(stream_id, slot_id).await {
                if slot_id == 1 {
                    if let Ok(StreamFrame::Headers(StreamHeaders::Response(resp))) =
                        decode_frame(&bytes)
                    {
                        let mut assembly = ResponseAssembly::new(
                            service
                                .config()
                                .slot_bytes()
                                .saturating_mul(service.config().slots_per_stream),
                        );
                        assembly.set_headers(resp);
                        assemblies.insert(stream_id, assembly);
                    }
                } else if bytes.as_ref() == END_MARKER {
                    if let Some(assembly) = assemblies.remove(&stream_id) {
                        let result = assembly.finalize();
                        watcher.deliver_response(stream_id, result).await;
                    }
                } else {
                    if let Some(assembly) = assemblies.get_mut(&stream_id) {
                        assembly.add_body(bytes).unwrap();
                    }
                }
            }
        }

        // Check that response was delivered
        let cached = rx.await.unwrap().unwrap();
        assert_eq!(cached.status, StatusCode::OK);
        assert_eq!(cached.body, Bytes::from("hello"));

        upload_stream.close().await;
    }

    #[tokio::test]
    async fn watcher_handle_can_abort_the_background_task() {
        let service = Arc::new(crate::UploadResponseService::new(
            UploadResponseConfig::default(),
        ));
        let handle = ResponseWatcher::new(service).spawn();

        handle.abort();
        let error = handle.await.expect_err("aborted watcher must not complete");
        assert!(error.is_cancelled());
    }
}
