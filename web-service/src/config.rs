// h2h3-server/src/config.rs

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
    pub port: u16,
    pub enable_h2: bool,
    pub enable_h3: bool,
    pub h3_backend: H3Backend,
    pub enable_webtransport: bool,
    pub enable_websocket: bool,
    pub enable_raw_tcp: bool,
    pub raw_tcp_port: u16,
    pub raw_tcp_tls: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            cert_pem_base64: String::new(),
            privkey_pem_base64: String::new(),
            port: 443,
            enable_h2: true,
            enable_h3: true,
            h3_backend: H3Backend::default(),
            enable_webtransport: true,
            enable_websocket: true,
            enable_raw_tcp: false,
            raw_tcp_port: 9000,
            raw_tcp_tls: false,
        }
    }
}
