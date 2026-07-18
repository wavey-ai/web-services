// h2h3-server/src/lib.rs

pub mod config;
pub mod error;
pub mod h2;
pub mod h3;
#[cfg(feature = "h3-tokio-quiche")]
pub mod h3_tokio_quiche;
mod http_range;
pub mod proxy;
pub mod quic_relay;
pub mod raw_tcp;
pub mod server;
pub mod tls;
pub mod traits;

pub use config::{H3Backend, ServerConfig};
pub use error::{ServerError, ServerResult};
pub use proxy::{
    Backend, BackendScheme, BackendView, LoadBalancingMode, ProxyConfig, ProxyIngress, ProxyRouter,
    ProxyState, UpstreamProtocol,
};
pub use quic_relay::QuicRelayConfig;
pub use raw_tcp::{read_length_prefixed_frame, write_length_prefixed_frame};
pub use server::{H2H3Server, H2H3ServerBuilder};
pub use tls::{default_tls_paths, load_default_tls_base64, load_tls_base64_from_paths};
pub use traits::{
    BodyStream, HandlerResponse, HandlerResult, RawTcpHandler, RequestHandler, Router, Server,
    ServerBuilder, ServerHandle, StreamWriter, StreamingHandler, WebSocketHandler,
    WebTransportHandler,
};
