use http::header::{InvalidHeaderName, InvalidHeaderValue};
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

#[derive(Debug, Error)]
pub enum H2Error {
    #[error("router error: {0}")]
    Router(#[from] ServerError),

    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),

    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
}
