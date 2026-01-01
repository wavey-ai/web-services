use super::balancer::LoadBalancingMode;
use super::queue::ProxyQueue;
use super::state::ProxyState;
use bytes::Bytes;
use futures_util::stream;
use http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, StreamBody as HttpStreamBody};
use http_pack::stream::{decode_frame, StreamBody as StreamBodyFrame, StreamEnd, StreamFrame, StreamHeaders};
use hyper::body::Frame;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

const BODY_CHANNEL_SIZE: usize = 16;

/// Worker that processes requests from the queue
pub struct ProxyWorker {
    worker_id: usize,
    queue: Arc<ProxyQueue>,
    state: Arc<ProxyState>,
}

impl ProxyWorker {
    pub fn new(worker_id: usize, queue: Arc<ProxyQueue>, state: Arc<ProxyState>) -> Self {
        Self {
            worker_id,
            queue,
            state,
        }
    }

    /// Start the worker loop
    pub async fn run(self) {
        info!(worker_id = self.worker_id, "Worker started");

        // Track the last processed queue ID (consumer cursor)
        let mut last_processed = 0usize;
        let mut body_channels: HashMap<u64, mpsc::Sender<Result<Frame<Bytes>, Infallible>>> =
            HashMap::new();

        loop {
            let next_id = last_processed + 1;

            if let Some(bytes) = self.queue.request_get(next_id).await {
                last_processed = next_id;

                let frame = match decode_frame(&bytes) {
                    Ok(frame) => frame,
                    Err(err) => {
                        warn!(
                            worker_id = self.worker_id,
                            queue_id = next_id,
                            error = %err,
                            "Failed to decode request frame"
                        );
                        continue;
                    }
                };

                let stream_id = frame.stream_id();
                if stream_id % self.queue.num_workers() as u64 != self.worker_id as u64 {
                    continue;
                }

                match frame {
                    StreamFrame::Headers(headers) => {
                        if body_channels.contains_key(&stream_id) {
                            warn!(
                                worker_id = self.worker_id,
                                stream_id,
                                "Duplicate headers frame, resetting stream"
                            );
                            body_channels.remove(&stream_id);
                        }

                        let response_tx = self.queue.take_response_channel(stream_id).await;
                        let (body_tx, body_rx) = mpsc::channel(BODY_CHANNEL_SIZE);
                        body_channels.insert(stream_id, body_tx);

                        let queue = Arc::clone(&self.queue);
                        let state = Arc::clone(&self.state);
                        let worker_id = self.worker_id;

                        tokio::spawn(async move {
                            process_stream_request(
                                worker_id,
                                stream_id,
                                headers,
                                body_rx,
                                response_tx,
                                state,
                                queue,
                            )
                            .await;
                        });
                    }
                    StreamFrame::Body(body) => {
                        if let Some(tx) = body_channels.get_mut(&stream_id) {
                            if tx.send(Ok(Frame::data(body.data))).await.is_err() {
                                warn!(
                                    worker_id = self.worker_id,
                                    stream_id,
                                    "Body channel closed"
                                );
                                body_channels.remove(&stream_id);
                            }
                        } else {
                            warn!(
                                worker_id = self.worker_id,
                                stream_id,
                                "Body frame received before headers"
                            );
                        }
                    }
                    StreamFrame::End(end) => {
                        if body_channels.remove(&end.stream_id).is_none() {
                            warn!(
                                worker_id = self.worker_id,
                                stream_id = end.stream_id,
                                "End frame received without active stream"
                            );
                        }
                    }
                }
            } else {
                // No request available yet, sleep briefly (HLS pattern uses 1-5ms)
                sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

async fn process_stream_request(
    worker_id: usize,
    stream_id: u64,
    headers: StreamHeaders,
    body_rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
    response_tx: Option<tokio::sync::oneshot::Sender<Result<(StatusCode, Bytes), String>>>,
    state: Arc<ProxyState>,
    queue: Arc<ProxyQueue>,
) {
    let request_headers = match headers {
        StreamHeaders::Request(req) => req,
        StreamHeaders::Response(_) => {
            warn!(worker_id, stream_id, "Received response headers in request queue");
            return;
        }
    };

    let backend_lease = match state.acquire_http_with_mode(LoadBalancingMode::Queue).await {
        Ok(lease) => lease,
        Err(err) => {
            error!(worker_id, stream_id, "No backend available: {:?}", err);
            send_stream_error(
                &queue,
                response_tx,
                stream_id,
                "No backend available".to_string(),
            )
            .await;
            return;
        }
    };

    let backend_url = &backend_lease.backend().url;
    let client = match state.http_client().cloned() {
        Some(client) => client,
        None => {
            send_stream_error(
                &queue,
                response_tx,
                stream_id,
                "No HTTP client configured".to_string(),
            )
            .await;
            return;
        }
    };

    let method = match Method::from_bytes(&request_headers.method) {
        Ok(method) => method,
        Err(_) => {
            send_stream_error(
                &queue,
                response_tx,
                stream_id,
                "Invalid request method".to_string(),
            )
            .await;
            return;
        }
    };

    let path = if request_headers.path.is_empty() {
        "/".to_string()
    } else {
        match String::from_utf8(request_headers.path.clone()) {
            Ok(path) => path,
            Err(_) => {
                send_stream_error(
                    &queue,
                    response_tx,
                    stream_id,
                    "Invalid request path".to_string(),
                )
                .await;
                return;
            }
        }
    };

    let upstream_uri = format!(
        "{}://{}:{}{}",
        backend_url.scheme(),
        backend_url.host_str().unwrap_or("localhost"),
        backend_url.port().unwrap_or(443),
        path
    );

    let body_stream = stream::unfold(body_rx, |mut rx| async {
        match rx.recv().await {
            Some(frame) => Some((frame, rx)),
            None => None,
        }
    });

    let body = HttpStreamBody::new(body_stream).boxed();

    let mut builder = Request::builder()
        .method(method)
        .uri(upstream_uri)
        .version(http::Version::HTTP_11);

    let mut has_host = false;
    for header in request_headers.headers {
        let name = match HeaderName::from_bytes(&header.name) {
            Ok(name) => name,
            Err(_) => {
                debug!(worker_id, stream_id, "Skipping invalid header name");
                continue;
            }
        };
        let value = match HeaderValue::from_bytes(&header.value) {
            Ok(value) => value,
            Err(_) => {
                debug!(worker_id, stream_id, "Skipping invalid header value");
                continue;
            }
        };
        if name == http::header::HOST {
            has_host = true;
        }
        builder = builder.header(name, value);
    }
    if !has_host {
        if let Some(host) = backend_url.host_str() {
            let authority = match backend_url.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_string(),
            };
            builder = builder.header(http::header::HOST, authority);
        }
    }

    let upstream_req = match builder.body(body) {
        Ok(req) => req,
        Err(err) => {
            send_stream_error(
                &queue,
                response_tx,
                stream_id,
                format!("Failed to build request: {err}"),
            )
            .await;
            return;
        }
    };

    debug!(
        worker_id,
        stream_id,
        backend = %backend_url,
        "Sending request to backend"
    );

    let response = match client.request(upstream_req).await {
        Ok(resp) => resp,
        Err(err) => {
            send_stream_error(
                &queue,
                response_tx,
                stream_id,
                format!("Backend request failed: {err}"),
            )
            .await;
            return;
        }
    };

    let status = response.status();
    debug!(
        worker_id,
        stream_id,
        status = %status,
        "Received response from backend"
    );

    if let Ok(headers) = StreamHeaders::from_response(stream_id, &response) {
        if let Err(err) = queue
            .enqueue_response_frame(StreamFrame::Headers(headers))
            .await
        {
            warn!(worker_id, stream_id, error = %err, "Failed to enqueue response headers");
        }
    } else {
        warn!(worker_id, stream_id, "Failed to encode response headers");
    }

    let mut body_bytes = Vec::new();
    let mut resp_body = response.into_body();
    let chunk_size = queue.max_body_chunk_bytes().max(1);

    while let Some(frame) = resp_body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(err) => {
                send_stream_error(
                    &queue,
                    response_tx,
                    stream_id,
                    format!("Failed to read response body: {err}"),
                )
                .await;
                return;
            }
        };
        if let Ok(data) = frame.into_data() {
            let mut offset = 0;
            while offset < data.len() {
                let end = (offset + chunk_size).min(data.len());
                let chunk = data.slice(offset..end);
                if let Err(err) = queue
                    .enqueue_response_frame(StreamFrame::Body(StreamBodyFrame {
                        stream_id,
                        data: chunk.clone(),
                    }))
                    .await
                {
                    warn!(worker_id, stream_id, error = %err, "Failed to enqueue response body");
                }
                body_bytes.extend_from_slice(&chunk);
                offset = end;
            }
        }
    }

    if let Err(err) = queue
        .enqueue_response_frame(StreamFrame::End(StreamEnd { stream_id }))
        .await
    {
        warn!(worker_id, stream_id, error = %err, "Failed to enqueue response end");
    }

    if let Some(tx) = response_tx {
        let _ = tx.send(Ok((status, Bytes::from(body_bytes))));
    }
}

async fn send_stream_error(
    queue: &ProxyQueue,
    response_tx: Option<tokio::sync::oneshot::Sender<Result<(StatusCode, Bytes), String>>>,
    stream_id: u64,
    message: String,
) {
    let status = StatusCode::INTERNAL_SERVER_ERROR;
    let body = Bytes::from(message.clone());

    let response = Response::builder().status(status).body(());
    if let Ok(response) = response {
        if let Ok(headers) = StreamHeaders::from_response(stream_id, &response) {
            let _ = queue
                .enqueue_response_frame(StreamFrame::Headers(headers))
                .await;
        }
    }

    let chunk_size = queue.max_body_chunk_bytes().max(1);
    let mut offset = 0;
    while offset < body.len() {
        let end = (offset + chunk_size).min(body.len());
        let chunk = body.slice(offset..end);
        let _ = queue
            .enqueue_response_frame(StreamFrame::Body(StreamBodyFrame {
                stream_id,
                data: chunk,
            }))
            .await;
        offset = end;
    }

    let _ = queue
        .enqueue_response_frame(StreamFrame::End(StreamEnd { stream_id }))
        .await;

    if let Some(tx) = response_tx {
        let _ = tx.send(Err(message));
    }
}

/// Worker pool manager
pub struct WorkerPool {
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl WorkerPool {
    pub fn new(num_workers: usize, queue: Arc<ProxyQueue>, state: Arc<ProxyState>) -> Self {
        let mut workers = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let worker = ProxyWorker::new(worker_id, Arc::clone(&queue), Arc::clone(&state));
            let handle = tokio::spawn(async move {
                worker.run().await;
            });
            workers.push(handle);
        }

        info!(num_workers, "Worker pool started");

        Self { workers }
    }

    pub async fn shutdown(self) {
        for handle in self.workers {
            handle.abort();
        }
    }
}
