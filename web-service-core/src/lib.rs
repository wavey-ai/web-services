//! The types a handler returns: [`HandlerResponse`], [`ServerError`] and the result and body
//! stream aliases over them.
//!
//! `av-web-service` (the HTTP/1.1, HTTP/2, HTTP/3, WebTransport and WebSocket servers)
//! re-exports everything here, so its users see no change. Code that only builds responses
//! depends on this crate and links none of the server stack.

use bytes::Bytes;
use futures_core::stream::BoxStream;
use http::StatusCode;
use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("handler error: {0}")]
    Handler(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type ServerResult<T> = Result<T, ServerError>;

/// Result type for handlers
pub type HandlerResult<T> = Result<T, ServerError>;

/// Response type that handlers return
#[derive(Debug)]
pub struct HandlerResponse {
    pub status: StatusCode,
    pub body: Option<Bytes>,
    pub content_type: Option<Cow<'static, str>>,
    pub headers: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    pub etag: Option<u64>,
}

/// Stream type for request bodies
pub type BodyStream = BoxStream<'static, Result<Bytes, ServerError>>;

impl Default for HandlerResponse {
    fn default() -> Self {
        Self {
            status: StatusCode::OK,
            body: None,
            content_type: None,
            headers: vec![],
            etag: None,
        }
    }
}
