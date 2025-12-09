// h2h3-server/src/lib.rs

pub mod config;
pub mod error;
pub mod h2;
pub mod h3;
pub mod raw_tcp;
pub mod server;
pub mod traits;

pub use config::ServerConfig;
pub use error::{ServerError, ServerResult};
pub use server::{H2H3Server, H2H3ServerBuilder};
pub use traits::{
    BodyStream, HandlerResponse, HandlerResult, RawTcpHandler, RequestHandler, Router, Server,
    ServerBuilder, ServerHandle, StreamWriter, StreamingHandler, WebSocketHandler,
    WebTransportHandler,
};
