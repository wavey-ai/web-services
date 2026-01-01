// h2h3-server/src/lib.rs

pub mod config;
pub mod error;
pub mod h2;
#[cfg(feature = "proxy")]
pub mod proxy;
#[cfg(feature = "proxy")]
pub mod quic_relay;
pub mod raw_tcp;
pub mod server;
pub mod tls;
pub mod traits;

pub use config::ServerConfig;
pub use error::{ServerError, ServerResult};
#[cfg(feature = "proxy")]
pub use proxy::{
    Backend, BackendScheme, BackendView, LoadBalancingMode, ProxyConfig, ProxyIngress, ProxyRouter,
    ProxyState, UpstreamProtocol,
};
#[cfg(feature = "proxy")]
pub use quic_relay::QuicRelayConfig;
pub use server::{H2H3Server, H2H3ServerBuilder};
pub use traits::{
    BodyStream, HandlerResponse, HandlerResult, RawTcpHandler, RequestHandler, Router, Server,
    ServerBuilder, ServerHandle, StreamWriter, StreamingHandler, WebSocketHandler,
    WebTransportHandler,
};
pub use tls::{default_tls_paths, load_default_tls_base64, load_tls_base64_from_paths};
