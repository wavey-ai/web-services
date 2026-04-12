use bytes::Bytes;
use http::StatusCode;
use http_pack::stream::{decode_frame, StreamFrame, StreamHeaders, StreamResponseHeaders};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, trace, warn};

use crate::{CachedResponse, UploadResponseService, END_MARKER};

/// Watches the response cache for complete responses and delivers them
/// to waiting clients via oneshot channels.
pub struct ResponseWatcher {
    service: Arc<UploadResponseService>,
    poll_interval_ms: u64,
}

/// State for tracking a response stream being assembled
struct ResponseAssembly {
    status: Option<u16>,
    headers: Vec<(String, String)>,
    body_chunks: Vec<Bytes>,
}

impl ResponseAssembly {
    fn new() -> Self {
        Self {
            status: None,
            headers: Vec::new(),
            body_chunks: Vec::new(),
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

    fn add_body(&mut self, data: Bytes) {
        self.body_chunks.push(data);
    }

    fn finalize(self) -> Result<CachedResponse, String> {
        let status = self.status.ok_or("missing status")?;
        let status = StatusCode::from_u16(status).map_err(|e| e.to_string())?;

        let total_len: usize = self.body_chunks.iter().map(|c| c.len()).sum();
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
        Self {
            service,
            poll_interval_ms: 1,
        }
    }

    /// Set the poll interval in milliseconds
    pub fn with_poll_interval_ms(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms;
        self
    }

    /// Start the watcher loop in a background task
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.watch_loop().await;
        })
    }

    /// Main watch loop - polls all response streams for new slots
    async fn watch_loop(self) {
        let mut poll_interval = interval(Duration::from_millis(self.poll_interval_ms));
        let num_streams = self.service.config().num_streams;

        // Track the stream currently assigned to each slot and the last slot seen for it.
        let mut stream_ids: Vec<u64> = vec![0; num_streams];
        let mut last_seen: Vec<usize> = vec![0; num_streams];
        let mut assemblies: HashMap<u64, ResponseAssembly> = HashMap::new();

        loop {
            poll_interval.tick().await;

            for stream_idx in 0..num_streams {
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

                let current_last = self.service.response_last(stream_id).unwrap_or(0);
                let seen = last_seen[stream_idx];

                if current_last <= seen {
                    continue;
                }

                for slot_id in (seen + 1)..=current_last {
                    if let Some(bytes) = self.service.response_get(stream_id, slot_id).await {
                        if slot_id == 1 {
                            if let Ok(frame) = decode_frame(&bytes) {
                                if let StreamFrame::Headers(StreamHeaders::Response(resp)) = frame {
                                    trace!(stream_id, status = resp.status, "Response headers");
                                    let mut assembly = ResponseAssembly::new();
                                    assembly.set_headers(resp);
                                    assemblies.insert(stream_id, assembly);
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
                                assembly.add_body(bytes);
                            }
                        }
                    }
                }

                last_seen[stream_idx] = current_last;
            }
        }
    }

    /// Deliver a completed response to the waiting client
    async fn deliver_response(&self, stream_id: u64, result: Result<CachedResponse, String>) {
        let channels = self.service.response_channels();
        let tx = {
            let mut guard = channels.write().await;
            guard.remove(&stream_id)
        };

        if let Some(tx) = tx {
            if tx.send(result).is_err() {
                debug!(stream_id, "Client already disconnected");
            } else {
                debug!(stream_id, "Response delivered");
            }
        } else {
            warn!(stream_id, "No waiting client for response");
        }
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
                    if let Ok(frame) = decode_frame(&bytes) {
                        if let StreamFrame::Headers(StreamHeaders::Response(resp)) = frame {
                            let mut assembly = ResponseAssembly::new();
                            assembly.set_headers(resp);
                            assemblies.insert(stream_id, assembly);
                        }
                    }
                } else if bytes.as_ref() == END_MARKER {
                    if let Some(assembly) = assemblies.remove(&stream_id) {
                        let result = assembly.finalize();
                        watcher.deliver_response(stream_id, result).await;
                    }
                } else {
                    if let Some(assembly) = assemblies.get_mut(&stream_id) {
                        assembly.add_body(bytes);
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
}
