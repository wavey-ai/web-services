use super::balancer::LoadBalancingMode;
use super::upstream::UpstreamProtocol;
use super::{
    DEFAULT_MAX_BACKENDS, DEFAULT_MAX_QUEUE_PER_BACKEND, DEFAULT_QUEUE_REQUEST_KB,
    DEFAULT_QUEUE_SLOT_KB,
};
use anyhow::Context;
use crate::{default_tls_paths, load_default_tls_base64};
use crate::quic_relay::QuicRelayConfig;
use std::env;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub cert_pem_base64: String,
    pub key_pem_base64: String,
    pub port: u16,
    pub enable_h2: bool,
    pub enable_websocket: bool,
    pub initial_mode: LoadBalancingMode,
    pub upstream_protocol: UpstreamProtocol,
    pub max_queue: usize,
    pub max_backends: usize,
    pub queue_request_kb: usize,
    pub queue_slot_kb: usize,
    pub quic_relay: Option<QuicRelayConfig>,
}

impl ProxyConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let cert_env = env::var("TLS_CERT_BASE64").ok();
        let key_env = env::var("TLS_KEY_BASE64").ok();
        let (cert_pem_base64, key_pem_base64) = match (cert_env, key_env) {
            (Some(cert), Some(key)) => (cert, key),
            (None, None) => {
                let (cert_path, key_path) = default_tls_paths();
                load_default_tls_base64().with_context(|| {
                    format!(
                        "TLS_CERT_BASE64 and TLS_KEY_BASE64 unset; default certs missing at {} and {}",
                        cert_path.display(),
                        key_path.display()
                    )
                })?
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "TLS_CERT_BASE64 and TLS_KEY_BASE64 must both be set or both omitted"
                ))
            }
        };

        let port = env::var("PROXY_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(443);

        let enable_h2 = env_bool("ENABLE_H2", true);
        let enable_websocket = env_bool("ENABLE_WEBSOCKET", true);
        let max_queue =
            env_optional_usize("LB_MAX_QUEUE")?.unwrap_or(DEFAULT_MAX_QUEUE_PER_BACKEND);

        let upstream_protocol = match env::var("UPSTREAM_PROTOCOL") {
            Ok(value) => value
                .parse::<UpstreamProtocol>()
                .map_err(|err: String| anyhow::anyhow!(err))?,
            Err(_) => UpstreamProtocol::Http1,
        };

        let initial_mode = match env::var("LB_MODE") {
            Ok(value) => value
                .parse::<LoadBalancingMode>()
                .map_err(|err: String| anyhow::anyhow!(err))?,
            Err(_) => LoadBalancingMode::LeastConn,
        };

        let max_backends = match env_optional_usize("PROXY_MAX_BACKENDS")? {
            Some(value) => value,
            None => match env_optional_usize("PROXY_WORKERS")? {
                Some(value) => value,
                None => DEFAULT_MAX_BACKENDS,
            },
        }
        .max(1);

        let queue_slot_kb = env_optional_usize("PROXY_QUEUE_SLOT_KB")?
            .unwrap_or(DEFAULT_QUEUE_SLOT_KB)
            .max(1);
        let queue_request_kb = env_optional_usize("PROXY_QUEUE_REQUEST_KB")?
            .unwrap_or(DEFAULT_QUEUE_REQUEST_KB)
            .max(queue_slot_kb);

        let quic_relay = match (
            env::var("QUIC_FORWARD_ADDR").ok(),
            env::var("QUIC_FORWARD_LISTEN_ADDR").ok(),
            env::var("QUIC_FORWARD_BACKEND_ADDR").ok(),
        ) {
            (Some(forward_addr), Some(listen_addr), Some(backend_addr)) => {
                let forward_addr = parse_socket_addr("QUIC_FORWARD_ADDR", &forward_addr)?;
                let listen_addr = parse_socket_addr("QUIC_FORWARD_LISTEN_ADDR", &listen_addr)?;
                let backend_addr = parse_socket_addr("QUIC_FORWARD_BACKEND_ADDR", &backend_addr)?;
                let pool_size = env_optional_usize("QUIC_FORWARD_POOL_SIZE")?.unwrap_or(1);
                Some(QuicRelayConfig {
                    forward_addr,
                    listen_addr,
                    backend_addr,
                    pool_size,
                    cert_pem_base64: cert_pem_base64.clone(),
                    key_pem_base64: key_pem_base64.clone(),
                })
            }
            (None, None, None) => None,
            _ => {
                return Err(anyhow::anyhow!(
                    "QUIC_FORWARD_ADDR, QUIC_FORWARD_LISTEN_ADDR, and QUIC_FORWARD_BACKEND_ADDR must all be set or all omitted"
                ))
            }
        };

        Ok(Self {
            cert_pem_base64,
            key_pem_base64,
            port,
            enable_h2,
            enable_websocket,
            initial_mode,
            upstream_protocol,
            max_queue,
            max_backends,
            queue_request_kb,
            queue_slot_kb,
            quic_relay,
        })
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"),
        Err(_) => default,
    }
}

fn env_optional_usize(name: &str) -> anyhow::Result<Option<usize>> {
    match env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed = trimmed
                .parse::<usize>()
                .with_context(|| format!("{name} must be a non-negative integer"))?;
            Ok(Some(parsed))
        }
        Err(_) => Ok(None),
    }
}

fn parse_socket_addr(name: &str, value: &str) -> anyhow::Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .with_context(|| format!("{name} must be in host:port format"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard<'a> {
        _lock: MutexGuard<'a, ()>,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl<'a> EnvGuard<'a> {
        fn new(keys: &[&'static str]) -> Self {
            let lock = ENV_LOCK.lock().unwrap();
            let saved = keys.iter().map(|key| (*key, env::var(key).ok())).collect();
            Self { _lock: lock, saved }
        }
    }

    impl Drop for EnvGuard<'_> {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn from_env_reads_config() {
        let _guard = EnvGuard::new(&[
            "TLS_CERT_BASE64",
            "TLS_KEY_BASE64",
            "PROXY_PORT",
            "ENABLE_H2",
            "ENABLE_WEBSOCKET",
            "LB_MODE",
            "LB_MAX_QUEUE",
            "PROXY_MAX_BACKENDS",
            "PROXY_WORKERS",
            "PROXY_QUEUE_REQUEST_KB",
            "PROXY_QUEUE_SLOT_KB",
            "UPSTREAM_PROTOCOL",
            "QUIC_FORWARD_ADDR",
            "QUIC_FORWARD_LISTEN_ADDR",
            "QUIC_FORWARD_BACKEND_ADDR",
            "QUIC_FORWARD_POOL_SIZE",
        ]);

        env::set_var("TLS_CERT_BASE64", "cert");
        env::set_var("TLS_KEY_BASE64", "key");
        env::set_var("PROXY_PORT", "8443");
        env::set_var("ENABLE_H2", "false");
        env::set_var("ENABLE_WEBSOCKET", "no");
        env::set_var("LB_MODE", "queue");
        env::set_var("LB_MAX_QUEUE", "2");
        env::set_var("PROXY_MAX_BACKENDS", "6");
        env::set_var("PROXY_QUEUE_REQUEST_KB", "4096");
        env::set_var("PROXY_QUEUE_SLOT_KB", "128");
        env::set_var("UPSTREAM_PROTOCOL", "http2");
        env::set_var("QUIC_FORWARD_ADDR", "127.0.0.1:9101");
        env::set_var("QUIC_FORWARD_LISTEN_ADDR", "127.0.0.1:9102");
        env::set_var("QUIC_FORWARD_BACKEND_ADDR", "127.0.0.1:9103");
        env::set_var("QUIC_FORWARD_POOL_SIZE", "4");

        let config = ProxyConfig::from_env().unwrap();
        assert_eq!(config.cert_pem_base64, "cert");
        assert_eq!(config.key_pem_base64, "key");
        assert_eq!(config.port, 8443);
        assert!(!config.enable_h2);
        assert!(!config.enable_websocket);
        assert_eq!(config.initial_mode, LoadBalancingMode::Queue);
        assert_eq!(config.upstream_protocol, UpstreamProtocol::Http2);
        assert_eq!(config.max_queue, 2);
        assert_eq!(config.max_backends, 6);
        assert_eq!(config.queue_request_kb, 4096);
        assert_eq!(config.queue_slot_kb, 128);
        let quic = config.quic_relay.expect("quic forward config");
        assert_eq!(quic.forward_addr, "127.0.0.1:9101".parse().unwrap());
        assert_eq!(quic.listen_addr, "127.0.0.1:9102".parse().unwrap());
        assert_eq!(quic.backend_addr, "127.0.0.1:9103".parse().unwrap());
        assert_eq!(quic.pool_size, 4);
    }
}
