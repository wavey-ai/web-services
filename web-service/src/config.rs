// h2h3-server/src/config.rs

use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum H3Backend {
    #[default]
    Quinn,
    #[cfg(feature = "h3-tokio-quiche")]
    TokioQuiche,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub cert_pem_base64: String,
    pub privkey_pem_base64: String,
    pub bind_addr: IpAddr,
    pub port: u16,
    /// Client CA for an HTTP/2-only mutual TLS listener.
    pub client_ca_pem_base64: Option<String>,
    pub enable_h2: bool,
    pub enable_h3: bool,
    pub h3_backend: H3Backend,
    pub enable_webtransport: bool,
    pub enable_websocket: bool,
    pub enable_raw_tcp: bool,
    pub raw_tcp_port: u16,
    pub raw_tcp_tls: bool,
    /// Maximum active connections for each enabled transport.
    pub max_connections: usize,
    /// Maximum TLS or QUIC handshake time.
    pub handshake_timeout_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            cert_pem_base64: String::new(),
            privkey_pem_base64: String::new(),
            bind_addr: Ipv4Addr::UNSPECIFIED.into(),
            port: 443,
            client_ca_pem_base64: None,
            enable_h2: true,
            enable_h3: true,
            h3_backend: H3Backend::default(),
            enable_webtransport: true,
            enable_websocket: true,
            enable_raw_tcp: false,
            raw_tcp_port: 9000,
            raw_tcp_tls: false,
            max_connections: 4_096,
            handshake_timeout_ms: 10_000,
        }
    }
}
