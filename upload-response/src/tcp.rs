//! Raw TCP+TLS transport for upload-response
//!
//! Simple framed TCP with TLS. Each connection is one stream.
//! Optional mTLS for client certificate authentication.

use crate::UploadResponseService;
use bytes::Bytes;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::{timeout, Duration};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info};

const READ_TIMEOUT_SECS: u64 = 30;

/// Auth callback for TCP connections
/// Receives the client certificate chain (if mTLS) for verification
pub trait TcpAuth: Send + Sync + 'static {
    /// Authenticate based on client certificates
    /// Returns true to allow, false to deny
    fn authenticate(&self, client_certs: Option<&[CertificateDer<'static>]>) -> bool;
}

/// Allow all connections (no client cert required)
pub struct AllowAllTcp;

impl TcpAuth for AllowAllTcp {
    fn authenticate(&self, _client_certs: Option<&[CertificateDer<'static>]>) -> bool {
        true
    }
}

/// Require valid client certificate (mTLS)
pub struct RequireClientCert;

impl TcpAuth for RequireClientCert {
    fn authenticate(&self, client_certs: Option<&[CertificateDer<'static>]>) -> bool {
        client_certs.map(|c| !c.is_empty()).unwrap_or(false)
    }
}

/// TCP+TLS ingest server
pub struct TcpIngest<A: TcpAuth = AllowAllTcp> {
    service: Arc<UploadResponseService>,
    auth: Arc<A>,
}

impl TcpIngest<AllowAllTcp> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAllTcp),
        }
    }
}

impl<A: TcpAuth> TcpIngest<A> {
    pub fn with_auth(service: Arc<UploadResponseService>, auth: A) -> Self {
        Self {
            service,
            auth: Arc::new(auth),
        }
    }

    /// Start TCP+TLS server (no client cert verification)
    pub async fn start(
        self,
        addr: SocketAddr,
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        self.start_inner(addr, cert_pem, key_pem, None).await
    }

    /// Start TCP+TLS server with mTLS (require client certs)
    pub async fn start_mtls(
        self,
        addr: SocketAddr,
        cert_pem: &[u8],
        key_pem: &[u8],
        client_ca_pem: &[u8],
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        self.start_inner(addr, cert_pem, key_pem, Some(client_ca_pem))
            .await
    }

    async fn start_inner(
        self,
        addr: SocketAddr,
        cert_pem: &[u8],
        key_pem: &[u8],
        client_ca_pem: Option<&[u8]>,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = self.service;
        let auth = self.auth;

        // Parse server certs
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(cert_pem))
                .filter_map(|r| r.ok())
                .collect();

        let key: PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut BufReader::new(key_pem))?
                .ok_or("no private key found")?;

        // Build TLS config
        let config = if let Some(ca_pem) = client_ca_pem {
            // mTLS: require client certs
            let mut root_store = RootCertStore::empty();
            let ca_certs: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut BufReader::new(ca_pem))
                    .filter_map(|r| r.ok())
                    .collect();
            for cert in ca_certs {
                root_store.add(cert)?;
            }

            let verifier = WebPkiClientVerifier::builder(Arc::new(root_store)).build()?;

            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        } else {
            // No client cert required
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)?
        };

        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(addr).await?;

        let mode = if client_ca_pem.is_some() {
            "mTLS"
        } else {
            "TLS"
        };
        info!("TCP+{} server listening on {}", mode, addr);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("TCP server shutting down");
                        break;
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((tcp_stream, peer_addr)) => {
                                let service = Arc::clone(&service);
                                let auth = Arc::clone(&auth);
                                let acceptor = acceptor.clone();
                                tokio::spawn(async move {
                                    match acceptor.accept(tcp_stream).await {
                                        Ok(tls_stream) => {
                                            // Get client certs for auth
                                            let client_certs: Option<Vec<CertificateDer<'static>>> =
                                                tls_stream.get_ref().1.peer_certificates()
                                                    .map(|c| c.to_vec());

                                            if !auth.authenticate(client_certs.as_deref()) {
                                                error!(peer = %peer_addr, "TCP auth failed");
                                                return;
                                            }

                                            if let Err(e) = handle_tcp_connection(
                                                tls_stream,
                                                peer_addr,
                                                service,
                                            ).await {
                                                error!("TCP connection error: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            error!("TLS handshake error: {}", e);
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!("TCP accept error: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(shutdown_tx)
    }
}

async fn handle_tcp_connection<S>(
    mut stream: S,
    peer_addr: SocketAddr,
    service: Arc<UploadResponseService>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Acquire stream slot
    let _permit = service
        .acquire_stream()
        .await
        .map_err(|e| format!("Failed to acquire stream: {}", e))?;

    let stream_id = service.next_id();

    debug!(
        stream_id,
        peer = %peer_addr,
        "TCP connection established"
    );

    // Write headers (synthesize HTTP-like headers)
    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: b"/tcp".to_vec(),
        headers: vec![HeaderField {
            name: b"x-peer-addr".to_vec(),
            value: peer_addr.to_string().into_bytes(),
        }],
    });

    service
        .write_request_headers(stream_id, headers)
        .await
        .map_err(|e| format!("Failed to write headers: {}", e))?;

    let slot_bytes = service.config.slot_bytes();
    let mut buf = vec![0u8; slot_bytes];
    let mut pending = Vec::new();

    // Read data and write to cache
    loop {
        match timeout(Duration::from_secs(READ_TIMEOUT_SECS), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {
                debug!(stream_id, "TCP connection closed by peer");
                break;
            }
            Ok(Ok(n)) => {
                pending.extend_from_slice(&buf[..n]);

                // Write full slots
                while pending.len() >= slot_bytes {
                    let chunk: Vec<u8> = pending.drain(..slot_bytes).collect();
                    service
                        .append_request_body(stream_id, Bytes::from(chunk))
                        .await
                        .map_err(|e| format!("Failed to write body: {}", e))?;
                }
            }
            Ok(Err(e)) => {
                error!(stream_id, error = %e, "TCP read error");
                break;
            }
            Err(_) => {
                debug!(stream_id, "TCP read timeout");
                break;
            }
        }
    }

    // Flush remaining data
    if !pending.is_empty() {
        service
            .append_request_body(stream_id, Bytes::from(pending))
            .await
            .map_err(|e| format!("Failed to write final body: {}", e))?;
    }

    // End marker
    service
        .end_request(stream_id)
        .await
        .map_err(|e| format!("Failed to end request: {}", e))?;

    debug!(stream_id, "TCP request complete, waiting for response");

    // Wait for response
    let rx = service.register_response(stream_id).await;
    let timeout_duration = Duration::from_millis(service.config.response_timeout_ms);

    match timeout(timeout_duration, rx).await {
        Ok(Ok(Ok((_status, body)))) => {
            debug!(stream_id, len = body.len(), "Sending TCP response");
            // Send response length + body
            let len_bytes = (body.len() as u32).to_be_bytes();
            stream.write_all(&len_bytes).await?;
            stream.write_all(&body).await?;
        }
        Ok(Ok(Err(e))) => {
            error!(stream_id, error = %e, "Response error");
            service.drop_response_channel(stream_id).await;
        }
        Ok(Err(_)) => {
            error!(stream_id, "Response channel closed");
            service.drop_response_channel(stream_id).await;
        }
        Err(_) => {
            error!(stream_id, "Response timeout");
            service.drop_response_channel(stream_id).await;
        }
    }

    Ok(())
}
