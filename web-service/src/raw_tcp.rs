use crate::{config::ServerConfig, error::ServerResult, traits::{RawTcpHandler, RawStream}};
use std::{net::SocketAddr, sync::Arc};
use tls_helpers::tls_acceptor_from_base64;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info};

type DynStream = Box<dyn RawStream>;

pub struct RawTcpServer {
    config: ServerConfig,
    handler: Arc<dyn RawTcpHandler>,
}

impl RawTcpServer {
    pub fn new(config: ServerConfig, handler: Arc<dyn RawTcpHandler>) -> Self {
        Self { config, handler }
    }

    pub async fn start(&self, mut shutdown_rx: watch::Receiver<()>) -> ServerResult<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.raw_tcp_port));
        let listener = TcpListener::bind(addr).await.map_err(crate::error::ServerError::Io)?;
        info!(
            "Raw TCP server listening at {} (tls={})",
            addr, self.config.raw_tcp_tls
        );

        let tls_acceptor = if self.config.raw_tcp_tls {
            Some(
                tls_acceptor_from_base64(
                    &self.config.cert_pem_base64,
                    &self.config.privkey_pem_base64,
                    true,
                    false,
                )
                .map_err(|e| crate::error::ServerError::Tls(e.to_string()))?,
            )
        } else {
            None
        };

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Raw TCP server shutting down");
                    break;
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _peer)) => {
                            let handler = Arc::clone(&self.handler);
                            let tls_acceptor = tls_acceptor.clone();
                            let is_tls = self.config.raw_tcp_tls;
                            tokio::spawn(async move {
                                let boxed_stream: DynStream = if let Some(acceptor) = tls_acceptor {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => Box::new(tls_stream),
                                        Err(e) => {
                                            error!("TLS accept failed: {}", e);
                                            return;
                                        }
                                    }
                                } else {
                                    Box::new(stream)
                                };

                                if let Err(e) = handler.handle_stream(boxed_stream, is_tls).await {
                                    error!("Raw TCP handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Raw TCP accept failed: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
