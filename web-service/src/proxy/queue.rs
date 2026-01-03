use super::HTTP_PACK_FRAME_OVERHEAD_BYTES;
use bytes::Bytes;
use http::StatusCode;
use http_pack::packetizer::HttpPackStreamMessage;
use http_pack::stream::StreamFrame;
use playlists::chunk_cache::ChunkCache;
use playlists::Options;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::debug;

/// Queue manager using ChunkCache for request/response buffering
pub struct ProxyQueue {
    num_workers: usize,
    request_queue: Arc<ChunkCache>,
    response_queue: Arc<ChunkCache>,
    request_stream_idx: usize,
    response_stream_idx: usize,
    request_write_lock: Mutex<()>,
    response_write_lock: Mutex<()>,
    slot_semaphore: Arc<Semaphore>,
    max_slot_bytes: usize,
    max_body_chunk_bytes: usize,
    /// Map stream_id to response channel
    response_channels: Arc<RwLock<HashMap<u64, oneshot::Sender<Result<(StatusCode, Bytes), String>>>>>,
}

impl ProxyQueue {
    pub fn new(
        num_workers: usize,
        queue_depth: usize,
        per_request_kb: usize,
        slot_kb: usize,
    ) -> Self {
        let num_workers = num_workers.max(1);
        let slot_kb = slot_kb.max(1);
        let per_request_kb = per_request_kb.max(slot_kb);
        // Reserve enough slots to buffer each in-flight request.
        let slots_per_request = slots_per_request(per_request_kb, slot_kb).max(1);
        let inflight_per_backend = queue_depth.saturating_add(1).max(1);
        let max_requests = num_workers.saturating_mul(inflight_per_backend).max(1);
        let max_slots = max_requests.saturating_mul(slots_per_request).max(1);

        // Configure ring buffer options
        let mut options = Options::default();
        options.num_playlists = 2; // One for requests, one for responses
        options.max_segments = 1;
        options.max_parts_per_segment = max_slots;
        options.buffer_size_kb = slot_kb;

        let request_queue = Arc::new(ChunkCache::new(options));
        let response_queue = Arc::new(ChunkCache::new(options));

        // Use stream ID 0 for requests, 1 for responses
        let request_stream_idx = 0;
        let response_stream_idx = 1;

        let slot_semaphore = Arc::new(Semaphore::new(max_requests));
        let max_slot_bytes = slot_kb.saturating_mul(1024);
        let max_body_chunk_bytes = max_slot_bytes
            .saturating_sub(HTTP_PACK_FRAME_OVERHEAD_BYTES)
            .max(1);

        Self {
            num_workers,
            request_queue,
            response_queue,
            request_stream_idx,
            response_stream_idx,
            request_write_lock: Mutex::new(()),
            response_write_lock: Mutex::new(()),
            slot_semaphore,
            max_slot_bytes,
            max_body_chunk_bytes,
            response_channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    pub fn max_slot_bytes(&self) -> usize {
        self.max_slot_bytes
    }

    pub fn max_body_chunk_bytes(&self) -> usize {
        self.max_body_chunk_bytes
    }

    pub fn request_last(&self) -> usize {
        self.request_queue
            .last(self.request_stream_idx)
            .unwrap_or(0)
    }

    pub async fn request_get(&self, queue_id: usize) -> Option<Bytes> {
        self.request_queue
            .get(self.request_stream_idx, queue_id)
            .await
            .map(|(bytes, _hash)| bytes)
    }

    pub fn response_last(&self) -> usize {
        self.response_queue
            .last(self.response_stream_idx)
            .unwrap_or(0)
    }

    pub async fn response_get(&self, queue_id: usize) -> Option<Bytes> {
        self.response_queue
            .get(self.response_stream_idx, queue_id)
            .await
            .map(|(bytes, _hash)| bytes)
    }

    pub async fn begin_request(
        &self,
        stream_id: u64,
    ) -> Result<oneshot::Receiver<Result<(StatusCode, Bytes), String>>, String> {
        let (tx, rx) = oneshot::channel();
        let mut channels = self.response_channels.write().await;
        if channels.insert(stream_id, tx).is_some() {
            return Err(format!("response channel already exists for stream {stream_id}"));
        }
        debug!(stream_id, "Registered response channel");
        Ok(rx)
    }

    pub async fn acquire_slot(&self) -> Result<OwnedSemaphorePermit, String> {
        self.slot_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "queue slots closed".to_string())
    }

    pub async fn drop_response_channel(&self, stream_id: u64) {
        let mut channels = self.response_channels.write().await;
        channels.remove(&stream_id);
    }

    pub async fn take_response_channel(
        &self,
        stream_id: u64,
    ) -> Option<oneshot::Sender<Result<(StatusCode, Bytes), String>>> {
        self.response_channels.write().await.remove(&stream_id)
    }

    pub async fn enqueue_request_frame(&self, frame: StreamFrame) -> Result<(), String> {
        let payload = HttpPackStreamMessage::from_frame(&frame).payload;
        if payload.len() > self.max_slot_bytes {
            return Err(format!(
                "frame size {} exceeds slot size {}",
                payload.len(),
                self.max_slot_bytes
            ));
        }
        let _guard = self.request_write_lock.lock().await;
        self.request_queue
            .append(self.request_stream_idx, Bytes::from(payload))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn enqueue_response_frame(&self, frame: StreamFrame) -> Result<(), String> {
        let payload = HttpPackStreamMessage::from_frame(&frame).payload;
        if payload.len() > self.max_slot_bytes {
            return Err(format!(
                "frame size {} exceeds slot size {}",
                payload.len(),
                self.max_slot_bytes
            ));
        }
        let _guard = self.response_write_lock.lock().await;
        self.response_queue
            .append(self.response_stream_idx, Bytes::from(payload))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

fn slots_per_request(per_request_kb: usize, slot_kb: usize) -> usize {
    let slot_kb = slot_kb.max(1);
    let per_request_kb = per_request_kb.max(slot_kb);
    let slots = per_request_kb
        .saturating_add(slot_kb - 1)
        .saturating_div(slot_kb);
    slots.saturating_add(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;
    use http_pack::stream::{decode_frame, StreamBody, StreamEnd, StreamFrame, StreamHeaders};

    #[tokio::test]
    async fn test_request_frames_roundtrip() {
        let queue = ProxyQueue::new(4, 1, 512, 64);
        let stream_id = 1;

        let req = Request::get("https://example.com/test")
            .body(())
            .unwrap();
        let headers = StreamHeaders::from_request(stream_id, &req).unwrap();
        queue
            .enqueue_request_frame(StreamFrame::Headers(headers))
            .await
            .unwrap();
        queue
            .enqueue_request_frame(StreamFrame::Body(StreamBody {
                stream_id,
                data: Bytes::from("hello"),
            }))
            .await
            .unwrap();
        queue
            .enqueue_request_frame(StreamFrame::End(StreamEnd { stream_id }))
            .await
            .unwrap();

        let first = queue.request_get(1).await.expect("header frame");
        let frame = decode_frame(&first).unwrap();
        assert!(matches!(frame, StreamFrame::Headers(_)));

        let second = queue.request_get(2).await.expect("body frame");
        let frame = decode_frame(&second).unwrap();
        assert!(matches!(frame, StreamFrame::Body(_)));

        let third = queue.request_get(3).await.expect("end frame");
        let frame = decode_frame(&third).unwrap();
        assert!(matches!(frame, StreamFrame::End(_)));
    }

    #[tokio::test]
    async fn test_response_channel_roundtrip() {
        let queue = ProxyQueue::new(4, 1, 512, 64);
        let stream_id = 42;

        let rx = queue.begin_request(stream_id).await.unwrap();
        let tx = queue.take_response_channel(stream_id).await.unwrap();
        tx.send(Ok((StatusCode::OK, Bytes::from("ok"))))
            .unwrap();

        let (status, body) = rx.await.unwrap().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from("ok"));
    }
}
