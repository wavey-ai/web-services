use super::{ProxyConfig, ProxyRouter, ProxyState};
use crate::{H2H3Server, HandlerResult, Server, ServerBuilder, ServerHandle};
use std::sync::Arc;

pub struct ProxyIngress {
    state: Arc<ProxyState>,
    server: H2H3Server,
}

impl ProxyIngress {
    pub fn from_config(config: ProxyConfig) -> anyhow::Result<Self> {
        let state = Arc::new(ProxyState::new_with_limits_and_protocol(
            config.initial_mode,
            config.max_queue,
            config.upstream_protocol,
            Some(config.cert_pem_base64.as_str()),
        )?);
        let router = ProxyRouter::new(Arc::clone(&state));
        let server = H2H3Server::builder()
            .with_tls(config.cert_pem_base64, config.key_pem_base64)
            .with_port(config.port)
            .enable_h2(config.enable_h2)
            .enable_h3(config.enable_h3)
            .enable_websocket(config.enable_websocket)
            .with_router(Box::new(router))
            .build()
            .map_err(|err| anyhow::anyhow!(err))?;

        Ok(Self { state, server })
    }

    pub fn state(&self) -> Arc<ProxyState> {
        Arc::clone(&self.state)
    }

    pub async fn start(&self) -> HandlerResult<ServerHandle> {
        self.server.start().await
    }
}
