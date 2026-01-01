use super::{ProxyConfig, ProxyQueue, ProxyRouter, ProxyState, WorkerPool};
use crate::quic_relay::QuicRelayPool;
use crate::{H2H3Server, HandlerResult, Server, ServerBuilder, ServerHandle};
use std::sync::Arc;

pub struct ProxyIngress {
    state: Arc<ProxyState>,
    server: H2H3Server,
    queue: Arc<ProxyQueue>,
    worker_pool: WorkerPool,
    quic_relay: Option<Arc<QuicRelayPool>>,
}

impl ProxyIngress {
    pub fn from_config(config: ProxyConfig) -> anyhow::Result<Self> {
        let quic_relay = match config.quic_relay.clone() {
            Some(quic) => Some(QuicRelayPool::new(quic)?),
            None => None,
        };
        let max_backends = config.max_backends.max(1);
        let backend_queue_limit = max_backends.saturating_mul(config.max_queue);
        let state = Arc::new(ProxyState::new_with_limits_and_protocol(
            config.initial_mode,
            Some(backend_queue_limit),
            config.upstream_protocol,
            Some(config.cert_pem_base64.as_str()),
        )?);

        // Create queue and worker pool
        let queue = Arc::new(ProxyQueue::new(
            max_backends,
            config.max_queue,
            config.queue_request_kb,
            config.queue_slot_kb,
        ));
        let worker_pool = WorkerPool::new(max_backends, Arc::clone(&queue), Arc::clone(&state));

        let router = ProxyRouter::new(Arc::clone(&state), Arc::clone(&queue), quic_relay.clone());
        let server = H2H3Server::builder()
            .with_tls(config.cert_pem_base64, config.key_pem_base64)
            .with_port(config.port)
            .enable_h2(config.enable_h2)
            .enable_websocket(config.enable_websocket)
            .with_router(Box::new(router))
            .build()
            .map_err(|err| anyhow::anyhow!(err))?;

        Ok(Self {
            state,
            server,
            queue,
            worker_pool,
            quic_relay,
        })
    }

    pub fn state(&self) -> Arc<ProxyState> {
        Arc::clone(&self.state)
    }

    pub async fn start(&self) -> HandlerResult<ServerHandle> {
        let handle = self.server.start().await?;
        if let Some(relay) = &self.quic_relay {
            relay.start(handle.shutdown_tx.subscribe());
        }
        Ok(handle)
    }
}
