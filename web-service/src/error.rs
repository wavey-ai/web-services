use http::header::{InvalidHeaderName, InvalidHeaderValue};
use http::Error as HttpError;
use thiserror::Error;

pub use web_service_core::{ServerError, ServerResult};

#[derive(Debug, Error)]
pub enum H2Error {
    #[error("router error: {0}")]
    Router(#[from] ServerError),

    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),

    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
}

#[derive(Debug, Error)]
pub enum H3Error {
    #[error("router error: {0}")]
    Router(#[from] ServerError),

    #[error("response builder error: {0}")]
    Header(#[from] HttpError),

    #[error("h3 transport error: {0}")]
    Transport(String),
}
