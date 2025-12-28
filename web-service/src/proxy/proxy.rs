use super::context;
use super::pool::AcquireError;
use super::state::ProxyState;
use super::upstream::UpstreamProtocol;
use bytes::{Buf, Bytes};
use futures_util::{SinkExt, StreamExt};
use http::header::HeaderName;
use http::{HeaderMap, Request, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, field, debug_span, Instrument};
use crate::{BodyStream, HandlerResponse, HandlerResult, ServerError};

pub async fn proxy_http(
    state: &ProxyState,
    req: Request<()>,
    body: Option<BodyStream>,
) -> HandlerResult<HandlerResponse> {
    debug!(
        method = %req.method(),
        uri = %req.uri(),
        "proxy http request"
    );
    let lease = match state.acquire_http().await {
        Ok(lease) => lease,
        Err(AcquireError::NoAvailable) => {
            debug!("proxy http: no backends available");
            return Ok(text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "no http backends available",
            ));
        }
        Err(AcquireError::QueueFull) => {
            debug!("proxy http: backend queue full");
            return Ok(text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "http backend queue full",
            ));
        }
    };

    debug!(
        backend_url = %lease.backend().url,
        "proxy http selected backend"
    );
    let backend_url = match compose_backend_url(&lease.backend().url, req.uri()) {
        Ok(url) => url,
        Err(err) => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                &format!("invalid backend url: {err}"),
            ))
        }
    };

    let body = match body {
        Some(body) => collect_body(body).await?,
        None => Bytes::new(),
    };

    let protocol = state.upstream_protocol();
    if protocol == UpstreamProtocol::Http3 {
        return proxy_http_h3(state, req, backend_url, body).await;
    }

    let version = match protocol {
        UpstreamProtocol::Http1 => http::Version::HTTP_11,
        UpstreamProtocol::Http2 => http::Version::HTTP_2,
        UpstreamProtocol::Http3 => http::Version::HTTP_11,
    };

    proxy_http_hyper(state, req, &backend_url, body, version).await
}

async fn proxy_http_hyper(
    state: &ProxyState,
    req: Request<()>,
    backend_url: &url::Url,
    body: Bytes,
    version: http::Version,
) -> HandlerResult<HandlerResponse> {
    let backend_uri: Uri = match backend_url.as_str().parse() {
        Ok(uri) => uri,
        Err(err) => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                &format!("invalid backend uri: {err}"),
            ))
        }
    };

    let mut builder = http::Request::builder()
        .method(req.method().clone())
        .uri(backend_uri.clone())
        .version(version);

    for (name, value) in req.headers().iter() {
        if should_skip_request_header(name) {
            continue;
        }
        builder = builder.header(name.clone(), value.clone());
    }

    if let Some(request_id) = context::request_id(&req) {
        if !req.headers().contains_key(context::REQUEST_ID_HEADER) {
            builder = builder.header(context::REQUEST_ID_HEADER, request_id);
        }
    }

    if let Some(host) = req.headers().get(http::header::HOST) {
        builder = builder.header("x-forwarded-host", host.clone());
    }
    if !req.headers().contains_key("x-forwarded-proto") {
        builder = builder.header("x-forwarded-proto", "https");
    }
    if let Some(authority) = backend_uri.authority() {
        builder = builder.header(http::header::HOST, authority.as_str());
    }

    let upstream_span = debug_span!(
        "upstream_request",
        backend = %backend_url,
        status = field::Empty
    );

    let upstream_request = builder
        .body(Full::new(body))
        .map_err(|err| ServerError::Handler(Box::new(err)))?;

    let client = match state.http_client() {
        Some(client) => client,
        None => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "http upstream client not configured",
            ))
        }
    };

    let response = match async { client.request(upstream_request).await }
        .instrument(upstream_span.clone())
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                &format!("upstream error: {err}"),
            ))
        }
    };

    upstream_span.record("status", &field::display(response.status()));
    debug!(
        backend_status = %response.status(),
        "proxy http upstream response"
    );
    map_upstream_response(response).await
}

async fn proxy_http_h3(
    state: &ProxyState,
    req: Request<()>,
    backend_url: url::Url,
    body: Bytes,
) -> HandlerResult<HandlerResponse> {
    if backend_url.scheme() != "https" {
        return Ok(text_response(
            StatusCode::BAD_GATEWAY,
            "http3 upstream requires https scheme",
        ));
    }

    let host = match backend_url.host_str() {
        Some(host) => host,
        None => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "http3 upstream requires host",
            ))
        }
    };
    let port = backend_url.port_or_known_default().unwrap_or(443);

    let upstream_request = match build_h3_request(req, &backend_url) {
        Ok(request) => request,
        Err(err) => return Ok(text_response(StatusCode::BAD_GATEWAY, &err)),
    };

    let pool = match state.h3_pool() {
        Some(pool) => pool,
        None => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "http3 upstream not configured",
            ))
        }
    };

    let mut send_request = match pool.get_or_connect(host, port).await {
        Ok(send_request) => send_request,
        Err(err) => return Ok(text_response(StatusCode::BAD_GATEWAY, &err)),
    };

    let response: Result<HandlerResponse, String> = (|| async {
        let mut stream = send_request
            .send_request(upstream_request)
            .await
            .map_err(|err| format!("upstream error: {err}"))?;

        if !body.is_empty() {
            stream
                .send_data(body)
                .await
                .map_err(|err| format!("upstream error: {err}"))?;
        }
        stream
            .finish()
            .await
            .map_err(|err| format!("upstream error: {err}"))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|err| format!("upstream error: {err}"))?;

        let mut response_body = Vec::new();
        loop {
            let next = stream
                .recv_data()
                .await
                .map_err(|err| format!("upstream error: {err}"))?;
            let mut chunk = match next {
                Some(chunk) => chunk,
                None => break,
            };
            let bytes = chunk.copy_to_bytes(chunk.remaining());
            response_body.extend_from_slice(&bytes);
        }

        Ok(handler_response_from_parts(
            response.status(),
            response.headers(),
            Bytes::from(response_body),
        ))
    })()
    .await;

    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            pool.invalidate(host, port).await;
            Ok(text_response(StatusCode::BAD_GATEWAY, &err))
        }
    }
}

fn build_h3_request(
    req: Request<()>,
    backend_url: &url::Url,
) -> Result<http::Request<()>, String> {
    let mut builder = http::Request::builder()
        .method(req.method().clone())
        .uri(backend_url.as_str())
        .version(http::Version::HTTP_3);

    for (name, value) in req.headers().iter() {
        if should_skip_request_header(name) {
            continue;
        }
        builder = builder.header(name.clone(), value.clone());
    }

    if let Some(request_id) = context::request_id(&req) {
        if !req.headers().contains_key(context::REQUEST_ID_HEADER) {
            builder = builder.header(context::REQUEST_ID_HEADER, request_id);
        }
    }

    if let Some(host) = req.headers().get(http::header::HOST) {
        builder = builder.header("x-forwarded-host", host.clone());
    }
    if !req.headers().contains_key("x-forwarded-proto") {
        builder = builder.header("x-forwarded-proto", "https");
    }

    builder
        .body(())
        .map_err(|err| format!("invalid upstream request: {err}"))
}

pub async fn proxy_websocket(
    state: &ProxyState,
    req: Request<()>,
    stream: WebSocketStream<hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>>,
) -> HandlerResult<()> {
    debug!(
        method = %req.method(),
        uri = %req.uri(),
        "proxy websocket request"
    );
    let lease = match state.acquire_ws().await {
        Ok(lease) => lease,
        Err(AcquireError::NoAvailable) => {
            debug!("proxy websocket: no backends available");
            return Err(ServerError::Handler(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no websocket backends available",
            ))));
        }
        Err(AcquireError::QueueFull) => {
            debug!("proxy websocket: backend queue full");
            return Err(ServerError::Handler(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "websocket backend queue full",
            ))));
        }
    };

    debug!(
        backend_url = %lease.backend().url,
        "proxy websocket selected backend"
    );
    let backend_url = compose_backend_url(&lease.backend().url, req.uri())
        .map_err(|err| ServerError::Handler(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            err,
        ))))?;

    let mut ws_request = http::Request::builder()
        .method(http::Method::GET)
        .uri(backend_url.as_str());
    if let Some(request_id) = context::request_id(&req) {
        ws_request = ws_request.header(context::REQUEST_ID_HEADER, request_id);
    }
    let ws_request = ws_request
        .body(())
        .map_err(|err| ServerError::Handler(Box::new(err)))?;

    let connect_span = debug_span!("upstream_websocket_connect", backend = %backend_url);
    let (backend_ws, _) = tokio_tungstenite::connect_async(ws_request)
        .instrument(connect_span)
        .await
        .map_err(|err| ServerError::Handler(Box::new(err)))?;

    let (mut client_sink, mut client_stream) = stream.split();
    let (mut backend_sink, mut backend_stream) = backend_ws.split();

    let client_to_backend = async {
        while let Some(msg) = client_stream.next().await {
            let msg = msg.map_err(|err| ServerError::Handler(Box::new(err)))?;
            backend_sink
                .send(msg)
                .await
                .map_err(|err| ServerError::Handler(Box::new(err)))?;
        }
        Ok::<(), ServerError>(())
    };

    let backend_to_client = async {
        while let Some(msg) = backend_stream.next().await {
            let msg = msg.map_err(|err| ServerError::Handler(Box::new(err)))?;
            client_sink
                .send(msg)
                .await
                .map_err(|err| ServerError::Handler(Box::new(err)))?;
        }
        Ok::<(), ServerError>(())
    };

    tokio::try_join!(client_to_backend, backend_to_client).map(|_| ())
}

async fn map_upstream_response(
    response: http::Response<Incoming>,
) -> HandlerResult<HandlerResponse> {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|err| ServerError::Handler(Box::new(err)))?;
    let body = body.to_bytes();

    Ok(handler_response_from_parts(status, &headers, body))
}

fn handler_response_from_parts(
    status: StatusCode,
    headers: &HeaderMap,
    body: Bytes,
) -> HandlerResponse {
    let mut out_headers = Vec::new();
    let mut content_type = None;
    for (name, value) in headers.iter() {
        if should_skip_response_header(name) {
            continue;
        }
        if name == http::header::CONTENT_TYPE {
            if let Ok(value) = value.to_str() {
                content_type = Some(value.to_string());
            }
            continue;
        }
        if let Ok(value) = value.to_str() {
            out_headers.push((name.to_string(), value.to_string()));
        }
    }

    HandlerResponse {
        status,
        body: Some(body),
        content_type,
        headers: out_headers,
        etag: None,
    }
}

fn compose_backend_url(base: &url::Url, uri: &Uri) -> Result<url::Url, String> {
    let req_path = uri.path();
    let req_query = uri.query();

    let base_path = base.path().trim_end_matches('/');
    let req_path = req_path.trim_start_matches('/');

    let combined = if base_path.is_empty() {
        if req_path.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", req_path)
        }
    } else if req_path.is_empty() {
        base_path.to_string()
    } else {
        format!("{}/{}", base_path, req_path)
    };

    let mut url = base.clone();
    url.set_path(&combined);
    url.set_query(req_query);
    Ok(url)
}

fn should_skip_request_header(name: &HeaderName) -> bool {
    if is_hop_by_hop_header(name) {
        return true;
    }
    name == http::header::HOST
}

fn should_skip_response_header(name: &HeaderName) -> bool {
    is_hop_by_hop_header(name)
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "proxy-connection"
    )
}

fn text_response(status: StatusCode, message: &str) -> HandlerResponse {
    HandlerResponse {
        status,
        body: Some(Bytes::from(message.to_string())),
        content_type: Some("text/plain".to_string()),
        headers: vec![],
        etag: None,
    }
}

async fn collect_body(mut body: BodyStream) -> Result<Bytes, ServerError> {
    let mut data = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        data.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderName;

    #[test]
    fn compose_backend_url_handles_paths_and_queries() {
        let base = url::Url::parse("https://example.com").unwrap();
        let uri: Uri = "/v1/items?limit=10".parse().unwrap();
        let out = compose_backend_url(&base, &uri).unwrap();
        assert_eq!(out.as_str(), "https://example.com/v1/items?limit=10");

        let base = url::Url::parse("https://example.com/base").unwrap();
        let uri: Uri = "/child".parse().unwrap();
        let out = compose_backend_url(&base, &uri).unwrap();
        assert_eq!(out.as_str(), "https://example.com/base/child");

        let base = url::Url::parse("https://example.com/base/").unwrap();
        let uri: Uri = "/child/".parse().unwrap();
        let out = compose_backend_url(&base, &uri).unwrap();
        assert_eq!(out.as_str(), "https://example.com/base/child/");

        let base = url::Url::parse("https://example.com/base").unwrap();
        let uri: Uri = "/".parse().unwrap();
        let out = compose_backend_url(&base, &uri).unwrap();
        assert_eq!(out.as_str(), "https://example.com/base");
    }

    #[test]
    fn should_skip_request_header_filters_hop_by_hop() {
        let connection = HeaderName::from_static("connection");
        let upgrade = HeaderName::from_static("upgrade");
        let host = HeaderName::from_static("host");
        let custom = HeaderName::from_static("x-custom");

        assert!(should_skip_request_header(&connection));
        assert!(should_skip_request_header(&upgrade));
        assert!(should_skip_request_header(&host));
        assert!(!should_skip_request_header(&custom));
    }
}
