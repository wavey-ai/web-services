use crate::traits::HandlerResponse;
use bytes::Bytes;
use http::{
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE},
    HeaderMap, StatusCode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedRange {
    Unsupported,
    Satisfiable { start: usize, end: usize },
    Unsatisfiable,
}

pub(crate) fn apply_byte_range(
    headers: &HeaderMap,
    mut response: HandlerResponse,
) -> HandlerResponse {
    let Some(body) = response.body.take() else {
        return response;
    };

    if response.status != StatusCode::OK {
        response.body = Some(body);
        return response;
    }

    remove_generated_range_headers(&mut response.headers);
    response
        .headers
        .push((ACCEPT_RANGES.as_str().to_string(), "bytes".to_string()));

    let Some(range) = headers.get(RANGE).and_then(|value| value.to_str().ok()) else {
        response.body = Some(body);
        return response;
    };

    let total_len = body.len();
    match parse_range_header(range, total_len) {
        ParsedRange::Unsupported => {
            response.body = Some(body);
            response
        }
        ParsedRange::Satisfiable { start, end } => {
            let body = body.slice(start..end + 1);
            response.status = StatusCode::PARTIAL_CONTENT;
            response.headers.push((
                CONTENT_RANGE.as_str().to_string(),
                format!("bytes {start}-{end}/{total_len}"),
            ));
            response
                .headers
                .push((CONTENT_LENGTH.as_str().to_string(), body.len().to_string()));
            response.body = Some(body);
            response
        }
        ParsedRange::Unsatisfiable => {
            response.status = StatusCode::RANGE_NOT_SATISFIABLE;
            response.headers.push((
                CONTENT_RANGE.as_str().to_string(),
                format!("bytes */{}", body.len()),
            ));
            response
                .headers
                .push((CONTENT_LENGTH.as_str().to_string(), "0".to_string()));
            response.body = Some(Bytes::new());
            response
        }
    }
}

fn parse_range_header(value: &str, len: usize) -> ParsedRange {
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return ParsedRange::Unsupported;
    };
    if spec.contains(',') || spec.is_empty() || len == 0 {
        return ParsedRange::Unsatisfiable;
    }

    let Some((start, end)) = spec.split_once('-') else {
        return ParsedRange::Unsatisfiable;
    };

    if start.is_empty() {
        let Ok(suffix_len) = end.parse::<usize>() else {
            return ParsedRange::Unsatisfiable;
        };
        if suffix_len == 0 {
            return ParsedRange::Unsatisfiable;
        }
        let suffix_len = suffix_len.min(len);
        return ParsedRange::Satisfiable {
            start: len - suffix_len,
            end: len - 1,
        };
    }

    let Ok(start) = start.parse::<usize>() else {
        return ParsedRange::Unsatisfiable;
    };
    if start >= len {
        return ParsedRange::Unsatisfiable;
    }

    let end = if end.is_empty() {
        len - 1
    } else {
        let Ok(end) = end.parse::<usize>() else {
            return ParsedRange::Unsatisfiable;
        };
        if end < start {
            return ParsedRange::Unsatisfiable;
        }
        end.min(len - 1)
    };

    ParsedRange::Satisfiable { start, end }
}

fn remove_generated_range_headers(headers: &mut Vec<(String, String)>) {
    headers.retain(|(name, _)| {
        !name.eq_ignore_ascii_case(ACCEPT_RANGES.as_str())
            && !name.eq_ignore_ascii_case(CONTENT_LENGTH.as_str())
            && !name.eq_ignore_ascii_case(CONTENT_RANGE.as_str())
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;

    fn response(body: &'static [u8]) -> HandlerResponse {
        HandlerResponse {
            status: StatusCode::OK,
            body: Some(Bytes::from_static(body)),
            content_type: Some("video/mp4".into()),
            headers: vec![],
            etag: None,
        }
    }

    fn headers(range: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_str(range).unwrap());
        headers
    }

    #[test]
    fn byte_range_slices_response_body() {
        let response = apply_byte_range(&headers("bytes=2-5"), response(b"abcdefghi"));

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.body.as_deref(), Some(&b"cdef"[..]));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-range") && value == "bytes 2-5/9"
        }));
    }

    #[test]
    fn open_ended_range_slices_to_end() {
        let response = apply_byte_range(&headers("bytes=4-"), response(b"abcdefghi"));

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.body.as_deref(), Some(&b"efghi"[..]));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-range") && value == "bytes 4-8/9"
        }));
    }

    #[test]
    fn suffix_range_slices_from_end() {
        let response = apply_byte_range(&headers("bytes=-3"), response(b"abcdefghi"));

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.body.as_deref(), Some(&b"ghi"[..]));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-range") && value == "bytes 6-8/9"
        }));
    }

    #[test]
    fn unsatisfiable_range_returns_416() {
        let response = apply_byte_range(&headers("bytes=99-100"), response(b"abcdefghi"));

        assert_eq!(response.status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.body.as_deref(), Some(&b""[..]));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-range") && value == "bytes */9"
        }));
    }

    #[test]
    fn unsupported_range_unit_serves_full_body() {
        let response = apply_byte_range(&headers("items=0-3"), response(b"abcdefghi"));

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body.as_deref(), Some(&b"abcdefghi"[..]));
    }
}
