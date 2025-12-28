use http::{HeaderMap, Request};

use super::id;
use crate::HandlerResponse;

pub const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
}

pub fn ensure_context(req: &mut Request<()>) -> RequestContext {
    if let Some(existing) = req.extensions().get::<RequestContext>().cloned() {
        return existing;
    }

    let request_id = request_id_from_headers(req.headers())
        .unwrap_or_else(|| id::next_id_string());

    let context = RequestContext { request_id };
    req.extensions_mut().insert(context.clone());
    context
}

pub fn request_id(req: &Request<()>) -> Option<&str> {
    req.extensions()
        .get::<RequestContext>()
        .map(|ctx| ctx.request_id.as_str())
}

pub fn attach_request_id(response: &mut HandlerResponse, request_id: &str) {
    let exists = response
        .headers
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case(REQUEST_ID_HEADER));
    if !exists {
        response
            .headers
            .push((REQUEST_ID_HEADER.to_string(), request_id.to_string()));
    }
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    #[test]
    fn ensure_context_uses_existing_header() {
        let mut req = Request::builder()
            .uri("https://example.com/")
            .header(REQUEST_ID_HEADER, "req-123")
            .body(())
            .unwrap();

        let ctx = ensure_context(&mut req);
        assert_eq!(ctx.request_id, "req-123");
        assert_eq!(request_id(&req), Some("req-123"));
    }

    #[test]
    fn ensure_context_persists_generated_id() {
        let mut req = Request::builder()
            .uri("https://example.com/")
            .body(())
            .unwrap();

        let ctx = ensure_context(&mut req);
        let second = ensure_context(&mut req);
        assert_eq!(ctx.request_id, second.request_id);
    }

    #[test]
    fn attach_request_id_does_not_duplicate() {
        let mut response = crate::HandlerResponse {
            status: http::StatusCode::OK,
            body: None,
            content_type: None,
            headers: vec![(REQUEST_ID_HEADER.to_string(), "req-1".to_string())],
            etag: None,
        };

        attach_request_id(&mut response, "req-1");
        let count = response
            .headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(REQUEST_ID_HEADER))
            .count();
        assert_eq!(count, 1);
    }
}
