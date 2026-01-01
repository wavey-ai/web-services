// h2h3-server/src/server.rs

use crate::{
    config::ServerConfig,
    error::ServerError,
    h2::Http2Server,
    h3::Http3Server,
    raw_tcp::RawTcpServer,
    traits::{HandlerResult, RawTcpHandler, Router, Server, ServerBuilder, ServerHandle},
};
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

pub struct H2H3Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
    raw_tcp_handler: Option<Arc<dyn RawTcpHandler>>,
}

impl H2H3Server {
    pub fn builder() -> H2H3ServerBuilder {
        H2H3ServerBuilder::new()
    }
}

#[async_trait::async_trait]
impl Server for H2H3Server {
    async fn start(&self) -> HandlerResult<ServerHandle> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (ready_tx, ready_rx) = oneshot::channel();
        let (finished_tx, finished_rx) = oneshot::channel();

        let mut tasks = vec![];

        // Start Raw TCP server if enabled
        if self.config.enable_raw_tcp {
            if let Some(handler) = &self.raw_tcp_handler {
                let raw_server = RawTcpServer::new(self.config.clone(), Arc::clone(handler));
                let shutdown_rx = shutdown_rx.clone();

                let handle = tokio::spawn(async move {
                    if let Err(e) = raw_server.start(shutdown_rx).await {
                        tracing::error!("Raw TCP server error: {}", e);
                    }
                });

                tasks.push(handle);
            } else {
                tracing::warn!("Raw TCP enabled but no handler configured");
            }
        }

        // Start HTTP/2 server if enabled
        if self.config.enable_h2 {
            let h2_server = Http2Server::new(self.config.clone(), Arc::clone(&self.router));
            let shutdown_rx = shutdown_rx.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = h2_server.start(shutdown_rx).await {
                    tracing::error!("HTTP/2 server error: {}", e);
                }
            });

            tasks.push(handle);
        }

        // Start HTTP/3 server if enabled
        if self.config.enable_h3 {
            let h3_server = Http3Server::new(self.config.clone(), Arc::clone(&self.router));
            let shutdown_rx = shutdown_rx.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = h3_server.start(shutdown_rx).await {
                    tracing::error!("HTTP/3 server error: {}", e);
                }
            });

            tasks.push(handle);
        }

        // Wait for all servers to finish
        tokio::spawn(async move {
            for task in tasks {
                let _ = task.await;
            }
            let _ = finished_tx.send(());
        });

        // Signal that servers are ready
        let _ = ready_tx.send(());

        Ok(ServerHandle {
            shutdown_tx,
            ready_rx,
            finished_rx,
        })
    }
}

pub struct H2H3ServerBuilder {
    config: ServerConfig,
    router: Option<Box<dyn Router>>,
    raw_tcp_handler: Option<Box<dyn RawTcpHandler>>,
}

impl ServerBuilder for H2H3ServerBuilder {
    type Server = H2H3Server;

    fn new() -> Self {
        Self {
            config: ServerConfig::default(),
            router: None,
            raw_tcp_handler: None,
        }
    }

    fn with_tls(mut self, cert: String, key: String) -> Self {
        self.config.cert_pem_base64 = cert;
        self.config.privkey_pem_base64 = key;
        self
    }

    fn with_port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    fn with_router(mut self, router: Box<dyn Router>) -> Self {
        self.router = Some(router);
        self
    }

    fn enable_h2(mut self, enable: bool) -> Self {
        self.config.enable_h2 = enable;
        self
    }

    fn enable_h3(mut self, enable: bool) -> Self {
        self.config.enable_h3 = enable;
        self
    }

    fn enable_websocket(mut self, enable: bool) -> Self {
        self.config.enable_websocket = enable;
        self
    }

    fn enable_raw_tcp(mut self, enable: bool) -> Self {
        self.config.enable_raw_tcp = enable;
        self
    }

    fn with_raw_tcp_port(mut self, port: u16) -> Self {
        self.config.raw_tcp_port = port;
        self
    }

    fn with_raw_tcp_tls(mut self, enable: bool) -> Self {
        self.config.raw_tcp_tls = enable;
        self
    }

    fn with_raw_tcp_handler(mut self, handler: Box<dyn RawTcpHandler>) -> Self {
        self.raw_tcp_handler = Some(handler);
        self
    }

    fn build(self) -> Result<Self::Server, ServerError> {
        let router = self
            .router
            .ok_or_else(|| ServerError::Config("Router not configured".into()))?;

        if self.config.cert_pem_base64.is_empty() || self.config.privkey_pem_base64.is_empty() {
            return Err(ServerError::Config(
                "TLS certificate and key must be provided".into(),
            ));
        }

        if !self.config.enable_h2 && !self.config.enable_h3 {
            return Err(ServerError::Config(
                "At least one protocol (HTTP/2 or HTTP/3) must be enabled".into(),
            ));
        }
        if self.config.enable_raw_tcp && self.raw_tcp_handler.is_none() {
            return Err(ServerError::Config(
                "Raw TCP enabled but no handler provided".into(),
            ));
        }

        Ok(H2H3Server {
            config: self.config,
            router: Arc::from(router),
            raw_tcp_handler: self.raw_tcp_handler.map(Arc::from),
        })
    }
}
