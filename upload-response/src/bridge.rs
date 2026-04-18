use crate::{
    CachedResponse, RequestControl, ResponseResult, TailSlot, UploadResponseService, UploadStream,
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response};
use http_pack::stream::{
    decode_frame, StreamFrame, StreamHeaders, StreamRequestHeaders, StreamResponseHeaders,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{interval, timeout, Duration};
use web_service::{BodyStream, HandlerResponse, HandlerResult, ServerError, StreamWriter};

#[derive(Debug, Clone, Copy)]
pub struct IngressProxyConfig {
    pub response_timeout_ms: u64,
    pub watch_poll_ms: u64,
}

#[derive(Clone)]
pub struct CachedIngress {
    service: Arc<UploadResponseService>,
    config: IngressProxyConfig,
}

pub struct CachedRequestGuard {
    stream: UploadStream,
    response_rx: Option<oneshot::Receiver<ResponseResult>>,
}

impl CachedIngress {
    pub fn new(service: Arc<UploadResponseService>, config: IngressProxyConfig) -> Self {
        Self { service, config }
    }

    pub fn service(&self) -> Arc<UploadResponseService> {
        Arc::clone(&self.service)
    }

    pub async fn open_buffered_request(&self) -> Result<CachedRequestGuard> {
        let stream = self
            .service
            .open_stream()
            .await
            .map_err(|error| anyhow!(error))?;
        let response_rx = Some(self.service.register_response(stream.stream_id()).await);
        Ok(CachedRequestGuard {
            stream,
            response_rx,
        })
    }

    pub async fn open_streaming_request(&self) -> Result<CachedRequestGuard> {
        let stream = self
            .service
            .open_stream()
            .await
            .map_err(|error| anyhow!(error))?;
        let response_rx = self.service.register_response(stream.stream_id()).await;
        drop(response_rx);
        Ok(CachedRequestGuard {
            stream,
            response_rx: None,
        })
    }

    pub async fn write_request_headers(&self, stream_id: u64, req: &Request<()>) -> Result<()> {
        self.write_request_headers_with(stream_id, req, |_| Ok(()))
            .await
    }

    pub async fn write_request_headers_with<F>(
        &self,
        stream_id: u64,
        req: &Request<()>,
        mutate_headers: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut HeaderMap) -> Result<()>,
    {
        let request = clone_request_head(req, mutate_headers)?;
        let headers = StreamHeaders::from_request(stream_id, &request)
            .map_err(|error| anyhow!("failed to encode request headers: {error}"))?;
        self.service
            .write_request_headers(stream_id, headers)
            .await
            .map_err(|error| anyhow!(error))
    }

    pub async fn copy_request_body(&self, stream_id: u64, mut body: BodyStream) -> Result<()> {
        let chunk_size = self.service.config().slot_bytes().max(1);
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| anyhow!("failed to read request body: {error}"))?;
            if chunk.is_empty() {
                continue;
            }
            self.append_request_body_sliced(stream_id, &chunk, chunk_size)
                .await?;
        }
        Ok(())
    }

    pub async fn append_request_body(&self, stream_id: u64, bytes: Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.service
            .append_request_body(stream_id, bytes)
            .await
            .map_err(|error| anyhow!(error))
    }

    pub async fn append_request_body_sliced(
        &self,
        stream_id: u64,
        bytes: &[u8],
        chunk_size: usize,
    ) -> Result<()> {
        let chunk_size = chunk_size.max(1);
        for chunk in bytes.chunks(chunk_size) {
            if chunk.is_empty() {
                continue;
            }
            self.service
                .append_request_body(stream_id, Bytes::copy_from_slice(chunk))
                .await
                .map_err(|error| anyhow!(error))?;
        }
        Ok(())
    }

    pub async fn append_request_control(
        &self,
        stream_id: u64,
        control: RequestControl,
    ) -> Result<()> {
        self.service
            .append_request_control(stream_id, control)
            .await
            .map_err(|error| anyhow!(error))
    }

    pub async fn end_request(&self, stream_id: u64) -> Result<()> {
        self.service
            .end_request(stream_id)
            .await
            .map_err(|error| anyhow!(error))
    }

    pub async fn await_response(
        &self,
        stream_id: u64,
        rx: oneshot::Receiver<ResponseResult>,
    ) -> Result<HandlerResponse> {
        let timeout_duration = Duration::from_millis(self.config.response_timeout_ms);
        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok(cached))) => Ok(handler_response_from_cached(cached)),
            Ok(Ok(Err(error))) => {
                self.service.drop_response_channel(stream_id).await;
                Err(anyhow!(error))
            }
            Ok(Err(_)) => {
                self.service.drop_response_channel(stream_id).await;
                Err(anyhow!("response channel closed"))
            }
            Err(_) => {
                self.service.drop_response_channel(stream_id).await;
                Err(anyhow!("response timeout"))
            }
        }
    }

    pub async fn proxy_streaming_response(
        &self,
        stream_id: u64,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        let timeout_duration = Duration::from_millis(self.config.response_timeout_ms);
        timeout(timeout_duration, async {
            let mut poll = interval(Duration::from_millis(self.config.watch_poll_ms.max(1)));
            let mut last_slot = 0usize;
            let mut headers_sent = false;

            loop {
                poll.tick().await;

                if !headers_sent {
                    if let Some(headers) = self.service.get_response_headers(stream_id).await {
                        stream_writer
                            .send_response(build_streaming_response_head(&headers)?)
                            .await?;
                        headers_sent = true;
                        last_slot = 1;
                    } else {
                        continue;
                    }
                }

                let current_last = self.service.response_last(stream_id).unwrap_or(0);
                if current_last <= last_slot {
                    continue;
                }

                for slot_id in (last_slot + 1)..=current_last {
                    match self.service.tail_response(stream_id, slot_id).await {
                        Some(TailSlot::Body(bytes)) => {
                            stream_writer.send_data(bytes).await?;
                        }
                        Some(TailSlot::End) => {
                            stream_writer.finish().await?;
                            return Ok(());
                        }
                        _ => {}
                    }
                }

                last_slot = current_last;
            }
        })
        .await
        .map_err(|_| ServerError::Config("response timeout".into()))?
    }

    pub async fn write_handler_response(&self, stream_id: u64, response: HandlerResponse) {
        let _ = self
            .service
            .write_handler_response(stream_id, response)
            .await;
    }
}

impl CachedRequestGuard {
    pub fn stream_id(&self) -> u64 {
        self.stream.stream_id()
    }

    pub fn take_response_receiver(&mut self) -> Option<oneshot::Receiver<ResponseResult>> {
        self.response_rx.take()
    }

    pub async fn close(self) {
        self.stream.close().await;
    }
}

pub fn clone_request_head<F>(req: &Request<()>, mutate_headers: F) -> Result<Request<()>>
where
    F: FnOnce(&mut HeaderMap) -> Result<()>,
{
    let mut builder = Request::builder()
        .method(req.method().clone())
        .uri(req.uri().clone())
        .version(req.version());

    let headers = builder
        .headers_mut()
        .ok_or_else(|| anyhow!("failed to construct request headers"))?;

    for (name, value) in req.headers() {
        headers.insert(name, value.clone());
    }
    mutate_headers(headers)?;

    builder
        .body(())
        .map_err(|error| anyhow!("failed to clone request head: {error}"))
}

pub fn handler_response_from_cached(cached: CachedResponse) -> HandlerResponse {
    let mut content_type = None;
    let mut headers = Vec::new();
    for (name, value) in cached.headers {
        if name.eq_ignore_ascii_case("content-type") {
            content_type = Some(value);
        } else {
            headers.push((name, value));
        }
    }

    HandlerResponse {
        status: cached.status,
        body: Some(cached.body),
        content_type,
        headers,
        etag: None,
    }
}

pub fn build_streaming_response_head(
    headers: &StreamResponseHeaders,
) -> HandlerResult<Response<()>> {
    let mut builder = Response::builder().status(headers.status);
    for header in &headers.headers {
        let name = http::HeaderName::from_bytes(&header.name).map_err(|error| {
            ServerError::Config(format!("invalid response header name: {error}"))
        })?;
        let value = HeaderValue::from_bytes(&header.value).map_err(|error| {
            ServerError::Config(format!("invalid response header value for {name}: {error}"))
        })?;
        builder = builder.header(name, value);
    }
    builder
        .body(())
        .map_err(|error| ServerError::Config(format!("failed to build response head: {error}")))
}

pub fn response_content_type(headers: &StreamResponseHeaders) -> Option<String> {
    headers.headers.iter().find_map(|header| {
        let name = String::from_utf8_lossy(&header.name);
        name.eq_ignore_ascii_case("content-type")
            .then(|| String::from_utf8_lossy(&header.value).to_ascii_lowercase())
    })
}

pub fn request_from_headers_slot(bytes: &[u8]) -> Result<Request<()>> {
    match decode_frame(bytes)
        .map_err(|error| anyhow!("failed to decode request headers: {error}"))?
    {
        StreamFrame::Headers(StreamHeaders::Request(headers)) => {
            request_from_stream_headers(headers)
        }
        _ => Err(anyhow!("slot 1 was not a request headers frame")),
    }
}

pub fn request_from_stream_headers(headers: StreamRequestHeaders) -> Result<Request<()>> {
    build_request_from_parts(
        headers.method,
        headers.path,
        headers.authority,
        headers
            .headers
            .into_iter()
            .map(|header| (header.name, header.value))
            .collect(),
    )
}

fn build_request_from_parts(
    method: Vec<u8>,
    path: Vec<u8>,
    authority: Option<Vec<u8>>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Request<()>> {
    let method = String::from_utf8(method)
        .map_err(|error| anyhow!("invalid request method bytes: {error}"))?;
    let uri =
        String::from_utf8(path).map_err(|error| anyhow!("invalid request path bytes: {error}"))?;

    let mut builder = Request::builder().method(method.as_str()).uri(uri.as_str());
    if let Some(authority) = authority {
        builder = builder.header(
            http::header::HOST,
            HeaderValue::from_bytes(&authority)
                .map_err(|error| anyhow!("invalid authority header: {error}"))?,
        );
    }

    for (name_bytes, value_bytes) in headers {
        let name = HeaderName::from_bytes(&name_bytes)
            .map_err(|error| anyhow!("invalid header name: {error}"))?;
        let value = HeaderValue::from_bytes(&value_bytes)
            .map_err(|error| anyhow!("invalid header value: {error}"))?;
        builder = builder.header(name, value);
    }

    builder
        .body(())
        .map_err(|error| anyhow!("failed to build request: {error}"))
}
