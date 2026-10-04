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
    /// Maximum active request or upgraded-session handlers across all transports.
    pub max_in_flight_requests: usize,
    /// Maximum TLS or QUIC handshake time.
    pub handshake_timeout_ms: u64,
    /// How long an HTTP/1.1 or HTTP/2 request waits for an in-flight slot before it is
    /// answered 503 with `Retry-After`. Zero refuses at once.
    pub request_queue_timeout_ms: u64,
    /// Paths that skip the in-flight request limit on HTTP/1.1 and HTTP/2, matched exactly.
    /// Meant for load balancer health checks, so a busy server is not replaced as a dead one.
    pub limit_exempt_paths: Vec<String>,
    /// How long open HTTP/1.1 and HTTP/2 connections get to finish their requests after
    /// shutdown, with no new connections taken. Zero closes them at once.
    pub shutdown_drain_ms: u64,
    /// Most a client may send to a route that reads no body. The server reads and drops such
    /// a body so the connection can be reused, and past this answers 413.
    pub max_unread_body_bytes: u64,
    /// HTTP/1.1 time to read a request head, counted from the end of the previous response,
    /// so it also closes idle keep-alive connections. Behind a load balancer keep it above
    /// the balancer's idle timeout. Zero leaves it unbounded.
    pub http1_header_read_timeout_ms: u64,
    /// Serve the HTTP/1.1+HTTP/2 listener as cleartext HTTP/1.1, with no TLS.
    #[cfg(feature = "plain-http")]
    pub plain_http: bool,
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
            max_in_flight_requests: 4_096,
            handshake_timeout_ms: 10_000,
            request_queue_timeout_ms: 0,
            limit_exempt_paths: Vec::new(),
            shutdown_drain_ms: 0,
            max_unread_body_bytes: 16 * 1024 * 1024,
            http1_header_read_timeout_ms: 0,
            #[cfg(feature = "plain-http")]
            plain_http: false,
        }
    }
}
