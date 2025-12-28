use super::backend::{Backend, BackendScheme, BackendView};
use super::balancer::LoadBalancingMode;
use super::h3_pool::H3ConnectionPool;
use super::id;
use super::pool::{AcquireError, BackendLease, BackendPool};
use super::upstream::UpstreamProtocol;
use bytes::Bytes;
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;
use tls_helpers::load_certs_from_base64;
use webpki_roots::TLS_SERVER_ROOTS;
use url::Url;

pub type ProxyHttpClient =
    Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub struct ProxyState {
    http_pool: Arc<BackendPool>,
    ws_pool: Arc<BackendPool>,
    mode: RwLock<LoadBalancingMode>,
    upstream_protocol: UpstreamProtocol,
    http_client: Option<ProxyHttpClient>,
    h3_pool: Option<H3ConnectionPool>,
}

impl ProxyState {
    pub fn new(mode: LoadBalancingMode) -> anyhow::Result<Self> {
        Self::new_with_limits_and_protocol(mode, None, UpstreamProtocol::Http1, None)
    }

    pub fn new_with_limits(
        mode: LoadBalancingMode,
        max_queue: Option<usize>,
    ) -> anyhow::Result<Self> {
        Self::new_with_limits_and_protocol(mode, max_queue, UpstreamProtocol::Http1, None)
    }

    pub fn new_with_limits_and_protocol(
        mode: LoadBalancingMode,
        max_queue: Option<usize>,
        upstream_protocol: UpstreamProtocol,
        upstream_ca_cert_base64: Option<&str>,
    ) -> anyhow::Result<Self> {
        let (http_client, h3_pool) = match upstream_protocol {
            UpstreamProtocol::Http1 => (
                Some(build_proxy_client(
                    UpstreamProtocol::Http1,
                    upstream_ca_cert_base64,
                )?),
                None,
            ),
            UpstreamProtocol::Http2 => (
                Some(build_proxy_client(
                    UpstreamProtocol::Http2,
                    upstream_ca_cert_base64,
                )?),
                None,
            ),
            UpstreamProtocol::Http3 => {
                let ca_cert = upstream_ca_cert_base64.ok_or_else(|| {
                    anyhow::anyhow!("http3 upstream requires a CA cert")
                })?;
                let endpoint = build_h3_endpoint(ca_cert)?;
                (None, Some(H3ConnectionPool::new(endpoint)))
            }
        };

        Ok(Self {
            http_pool: Arc::new(BackendPool::new(max_queue)),
            ws_pool: Arc::new(BackendPool::new(max_queue)),
            mode: RwLock::new(mode),
            upstream_protocol,
            http_client,
            h3_pool,
        })
    }

    pub fn new_with_client(
        mode: LoadBalancingMode,
        client: ProxyHttpClient,
        max_queue: Option<usize>,
    ) -> Self {
        Self {
            http_pool: Arc::new(BackendPool::new(max_queue)),
            ws_pool: Arc::new(BackendPool::new(max_queue)),
            mode: RwLock::new(mode),
            upstream_protocol: UpstreamProtocol::Http1,
            http_client: Some(client),
            h3_pool: None,
        }
    }

    pub fn upstream_protocol(&self) -> UpstreamProtocol {
        self.upstream_protocol
    }

    pub fn http_client(&self) -> Option<&ProxyHttpClient> {
        self.http_client.as_ref()
    }

    pub(crate) fn h3_pool(&self) -> Option<&H3ConnectionPool> {
        self.h3_pool.as_ref()
    }

    pub async fn mode(&self) -> LoadBalancingMode {
        *self.mode.read().await
    }

    pub async fn set_mode(&self, mode: LoadBalancingMode) {
        *self.mode.write().await = mode;
        debug!(mode = %mode.as_str(), "load balancer mode updated");
    }

    pub async fn add_backend(
        &self,
        url: Url,
        max_connections: Option<usize>,
    ) -> Result<BackendView, String> {
        let scheme = BackendScheme::from_url(&url)
            .ok_or_else(|| format!("unsupported backend scheme: {}", url.scheme()))?;
        if let Some(0) = max_connections {
            return Err("max_connections must be > 0".to_string());
        }
        let backend = Backend {
            id: id::next_id_string(),
            url,
            scheme,
            max_connections,
        };

        let view = if scheme.is_http() {
            self.http_pool.add_backend(backend).await
        } else {
            self.ws_pool.add_backend(backend).await
        };
        Ok(view)
    }

    pub async fn remove_backend(&self, id: &str) -> Option<BackendView> {
        if let Some(view) = self.http_pool.remove_backend(id).await {
            return Some(view);
        }
        self.ws_pool.remove_backend(id).await
    }

    pub async fn list_backends(&self) -> (Vec<BackendView>, Vec<BackendView>) {
        let http = self.http_pool.list_backends().await;
        let ws = self.ws_pool.list_backends().await;
        (http, ws)
    }

    pub async fn acquire_http(&self) -> Result<BackendLease, AcquireError> {
        let mode = self.mode().await;
        self.http_pool.acquire(mode).await
    }

    pub async fn acquire_ws(&self) -> Result<BackendLease, AcquireError> {
        let mode = self.mode().await;
        self.ws_pool.acquire(mode).await
    }
}

fn build_proxy_client(
    protocol: UpstreamProtocol,
    ca_cert_base64: Option<&str>,
) -> anyhow::Result<ProxyHttpClient> {
    let builder = match ca_cert_base64 {
        Some(ca_cert_base64) => {
            HttpsConnectorBuilder::new().with_tls_config(build_upstream_tls_config(ca_cert_base64)?)
        }
        None => HttpsConnectorBuilder::new().with_webpki_roots(),
    };
    let builder = builder.https_or_http();
    let https = match protocol {
        UpstreamProtocol::Http1 => builder.enable_http1().build(),
        UpstreamProtocol::Http2 => builder.enable_http2().build(),
        UpstreamProtocol::Http3 => builder.enable_http1().build(),
    };
    let mut client_builder = Client::builder(TokioExecutor::new());
    if matches!(protocol, UpstreamProtocol::Http2) {
        client_builder.http2_only(true);
    }
    Ok(client_builder.build(https))
}

fn build_h3_endpoint(ca_cert_base64: &str) -> anyhow::Result<quinn::Endpoint> {
    let mut tls_config = build_upstream_tls_config(ca_cert_base64)?;
    tls_config.enable_early_data = true;
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let mut endpoint =
        quinn::Endpoint::client(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0))?;
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?,
    ));
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

fn build_upstream_tls_config(ca_cert_base64: &str) -> anyhow::Result<rustls::ClientConfig> {
    let certs = load_certs_from_base64(ca_cert_base64)
        .map_err(|err| anyhow::anyhow!("failed to load upstream CA certs: {err}"))?;
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(TLS_SERVER_ROOTS.iter().cloned());
    let _ = roots.add_parsable_certificates(certs);
    if roots.is_empty() {
        return Err(anyhow::anyhow!("no upstream CA certificates available"));
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::balancer::LoadBalancingMode;
    use std::sync::Once;
    use url::Url;

    fn init_crypto() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    #[tokio::test]
    async fn add_backend_rejects_invalid_scheme() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();
        let url = Url::parse("ftp://example.com").unwrap();
        let err = state.add_backend(url, None).await.unwrap_err();
        assert!(err.contains("unsupported backend scheme"));
    }

    #[tokio::test]
    async fn add_backend_rejects_zero_max_connections() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();
        let url = Url::parse("https://example.com").unwrap();
        let err = state.add_backend(url, Some(0)).await.unwrap_err();
        assert!(err.contains("max_connections must be > 0"));
    }

    #[tokio::test]
    async fn list_backends_separates_http_and_ws() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();
        let http_url = Url::parse("https://example.com").unwrap();
        let ws_url = Url::parse("wss://example.com").unwrap();

        state.add_backend(http_url, None).await.unwrap();
        state.add_backend(ws_url, None).await.unwrap();

        let (http, ws) = state.list_backends().await;
        assert_eq!(http.len(), 1);
        assert_eq!(ws.len(), 1);
        assert_eq!(http[0].scheme, BackendScheme::Https);
        assert_eq!(ws[0].scheme, BackendScheme::Wss);
    }
}
