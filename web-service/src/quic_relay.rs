use base64::engine::general_purpose::STANDARD as base64_engine;
use base64::Engine;
use bytes::Bytes;
use http_pack::packetizer::HttpPackStreamMessage;
use http_pack::stream::{Http1StreamRebuilder, StreamDecoder, StreamFrame};
use playlists::chunk_cache::ChunkCache;
use playlists::Options as PlaylistOptions;
use quinn::{ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::CertificateDer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, trace, warn};

const DEFAULT_SLOTS: usize = 100;
const BUFFER_MAX_SEGMENTS: usize = 1;
const BUFFER_MAX_PARTS: usize = 256;
const BUFFER_SIZE_KB: usize = 64;
const BUFFER_INIT_KB: usize = 4;
const POLL_INTERVAL: Duration = Duration::from_millis(5);
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const BACKEND_POOL_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct QuicRelayConfig {
    pub forward_addr: SocketAddr,
    pub listen_addr: SocketAddr,
    pub backend_addr: SocketAddr,
    pub pool_size: usize,
    pub cert_pem_base64: String,
    pub key_pem_base64: String,
}

#[derive(Debug)]
pub enum QuicRelayError {
    Buffer(&'static str),
}

impl std::fmt::Display for QuicRelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuicRelayError::Buffer(msg) => write!(f, "buffer error: {}", msg),
        }
    }
}

impl std::error::Error for QuicRelayError {}

#[derive(Clone)]
pub struct RelayBuffers {
    slots: usize,
    requests: Arc<ChunkCache>,
    responses: Arc<ChunkCache>,
}

impl RelayBuffers {
    pub fn new(slots: usize) -> Self {
        let requests = Arc::new(build_cache(slots));
        let responses = Arc::new(build_cache(slots));
        Self {
            slots,
            requests,
            responses,
        }
    }

    pub fn slots(&self) -> usize {
        self.slots
    }

    pub fn slot_for_stream_id(&self, stream_id: u64) -> usize {
        slot_for_stream_id(stream_id, self.slots)
    }

    pub fn request_last(&self, slot: usize) -> usize {
        self.requests.last(slot).unwrap_or(0)
    }

    pub async fn request_get(&self, slot: usize, id: usize) -> Option<Bytes> {
        match self.requests.get(slot, id).await {
            Some((bytes, _)) => {
                trace!(slot, id, len = bytes.len(), "QUIC relay request buffer read");
                Some(bytes)
            }
            None => {
                trace!(slot, id, "QUIC relay request buffer read miss");
                None
            }
        }
    }

    pub async fn request_append(&self, slot: usize, data: Bytes) -> Result<usize, &'static str> {
        let len = data.len();
        self.requests.append(slot, data).await?;
        let id = self.request_last(slot);
        trace!(slot, id, len, "QUIC relay request buffer write");
        Ok(id)
    }

    pub fn response_last(&self, slot: usize) -> usize {
        self.responses.last(slot).unwrap_or(0)
    }

    pub async fn response_get(&self, slot: usize, id: usize) -> Option<Bytes> {
        match self.responses.get(slot, id).await {
            Some((bytes, _)) => {
                trace!(slot, id, len = bytes.len(), "QUIC relay response buffer read");
                Some(bytes)
            }
            None => {
                trace!(slot, id, "QUIC relay response buffer read miss");
                None
            }
        }
    }

    pub async fn response_append(&self, slot: usize, data: Bytes) -> Result<usize, &'static str> {
        let len = data.len();
        self.responses.append(slot, data).await?;
        let id = self.response_last(slot);
        trace!(slot, id, len, "QUIC relay response buffer write");
        Ok(id)
    }
}

struct BackendConnectionPool {
    backend_addr: SocketAddr,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl BackendConnectionPool {
    async fn new(backend_addr: SocketAddr, pool_size: usize) -> std::io::Result<Self> {
        Ok(Self {
            backend_addr,
            semaphore: Arc::new(tokio::sync::Semaphore::new(pool_size)),
        })
    }

    async fn send_request(&self, data: &[u8]) -> std::io::Result<()> {
        let _permit = self.semaphore.acquire().await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "semaphore closed")
        })?;

        let mut stream = TcpStream::connect(self.backend_addr).await?;
        stream.set_nodelay(true)?;

        stream.write_all(data).await?;

        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }

        let _ = stream.shutdown().await;

        Ok(())
    }
}

pub struct QuicRelayPool {
    slots: usize,
    config: QuicRelayConfig,
    buffers: Arc<RelayBuffers>,
}

impl QuicRelayPool {
    pub fn new(mut config: QuicRelayConfig) -> anyhow::Result<Arc<Self>> {
        if config.pool_size == 0 {
            config.pool_size = 1;
        }
        let slots = DEFAULT_SLOTS;
        let buffers = Arc::new(RelayBuffers::new(slots));
        Ok(Arc::new(Self {
            slots,
            config,
            buffers,
        }))
    }

    pub fn buffers(&self) -> Arc<RelayBuffers> {
        Arc::clone(&self.buffers)
    }

    pub fn slot_for_stream_id(&self, stream_id: u64) -> usize {
        self.buffers.slot_for_stream_id(stream_id)
    }

    pub fn response_last(&self, stream_id: u64) -> usize {
        let slot = self.slot_for_stream_id(stream_id);
        self.buffers.response_last(slot)
    }

    pub async fn response_get(&self, stream_id: u64, id: usize) -> Option<Bytes> {
        let slot = self.slot_for_stream_id(stream_id);
        self.buffers.response_get(slot, id).await
    }

    pub async fn enqueue_frame(
        &self,
        stream_id: u64,
        frame: StreamFrame,
    ) -> Result<(), QuicRelayError> {
        let slot = self.slot_for_stream_id(stream_id);
        let frame_kind = match &frame {
            StreamFrame::Headers(_) => "headers",
            StreamFrame::Body(_) => "body",
            StreamFrame::End(_) => "end",
        };
        let payload = HttpPackStreamMessage::from_frame(&frame).payload;
        trace!(
            stream_id,
            slot,
            frame_kind,
            payload_len = payload.len(),
            "QUIC relay enqueue frame"
        );
        self.buffers
            .request_append(slot, Bytes::from(payload))
            .await
            .map_err(QuicRelayError::Buffer)?;
        Ok(())
    }

    pub fn start(&self, shutdown_rx: watch::Receiver<()>) {
        let pool_size = self.config.pool_size.min(self.slots).max(1);
        for worker_id in 0..pool_size {
            let worker = QuicRelayWorker {
                id: worker_id,
                pool_size,
                slots: self.slots,
                forward_addr: self.config.forward_addr,
                buffers: Arc::clone(&self.buffers),
            };
            worker.start(shutdown_rx.clone());
        }

        let receiver = QuicRelayReceiver {
            listen_addr: self.config.listen_addr,
            backend_addr: self.config.backend_addr,
            cert_pem_base64: self.config.cert_pem_base64.clone(),
            key_pem_base64: self.config.key_pem_base64.clone(),
            buffers: Arc::clone(&self.buffers),
            slots: self.slots,
        };
        receiver.start(shutdown_rx);
    }
}

struct QuicRelayWorker {
    id: usize,
    pool_size: usize,
    slots: usize,
    forward_addr: SocketAddr,
    buffers: Arc<RelayBuffers>,
}

impl QuicRelayWorker {
    fn start(self, mut shutdown_rx: watch::Receiver<()>) {
        tokio::spawn(async move {
            let mut last_seen = vec![0usize; self.slots];

            let mut connection = match self.connect_with_backoff(&mut shutdown_rx).await {
                Some(conn) => conn,
                None => return,
            };
            info!("QUIC relay sender {} initial connection established", self.id);

            let mut interval = tokio::time::interval(POLL_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => return,
                    _ = interval.tick() => {
                        match self.flush_pending(&mut connection, &mut last_seen).await {
                            Ok(_) => continue,
                            Err(err) => {
                                warn!("QUIC relay sender {} flush failed: {}, reconnecting", self.id, err);
                                connection = match self.connect_with_backoff(&mut shutdown_rx).await {
                                    Some(conn) => conn,
                                    None => return,
                                };
                            }
                        }
                    }
                }
            }
        });
    }

    async fn connect_with_backoff(
        &self,
        shutdown_rx: &mut watch::Receiver<()>,
    ) -> Option<quinn::Connection> {
        let mut backoff = INITIAL_BACKOFF;

        let client_config = match create_client_config() {
            Ok(config) => config,
            Err(err) => {
                error!("QUIC relay sender {} client config failed: {}", self.id, err);
                return None;
            }
        };

        let mut endpoint = match Endpoint::client("0.0.0.0:0".parse().unwrap()) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                error!("QUIC relay sender {} endpoint creation failed: {}", self.id, err);
                return None;
            }
        };
        endpoint.set_default_client_config(client_config);

        loop {
            match endpoint.connect(self.forward_addr, "localhost") {
                Ok(connecting) => {
                    match connecting.await {
                        Ok(connection) => {
                            info!(
                                "QUIC relay sender {} connected to {}",
                                self.id, self.forward_addr
                            );
                            return Some(connection);
                        }
                        Err(err) => {
                            warn!(
                                "QUIC relay sender {} connect await failed: {}, retrying in {:?}",
                                self.id, err, backoff
                            );
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        "QUIC relay sender {} connect failed: {}, retrying in {:?}",
                        self.id, err, backoff
                    );
                }
            }
            tokio::select! {
                _ = shutdown_rx.changed() => return None,
                _ = sleep(backoff) => {
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    async fn flush_pending(
        &self,
        connection: &mut quinn::Connection,
        last_seen: &mut [usize],
    ) -> anyhow::Result<()> {
        for slot in 0..self.slots {
            if slot % self.pool_size != self.id {
                continue;
            }
            let last = self.buffers.request_last(slot);
            let seen = last_seen[slot];
            if last <= seen {
                continue;
            }
            trace!(
                worker_id = self.id,
                slot,
                from_id = seen + 1,
                to_id = last,
                "QUIC relay sender flush pending"
            );

            // Open a new QUIC stream for this discrete request
            let (mut send, _recv) = connection.open_bi().await?;

            for id in (seen + 1)..=last {
                let data = match self.buffers.request_get(slot, id).await {
                    Some(data) => data,
                    None => {
                        last_seen[slot] = last;
                        break;
                    }
                };
                trace!(
                    worker_id = self.id,
                    slot,
                    id,
                    payload_len = data.len(),
                    "QUIC relay sender send frame on stream"
                );

                send.write_all(&data).await?;
            }

            // Finish the stream
            send.finish()?;
            last_seen[slot] = last;
        }
        Ok(())
    }
}

struct QuicRelayReceiver {
    listen_addr: SocketAddr,
    backend_addr: SocketAddr,
    cert_pem_base64: String,
    key_pem_base64: String,
    buffers: Arc<RelayBuffers>,
    slots: usize,
}

impl QuicRelayReceiver {
    fn start(self, mut shutdown_rx: watch::Receiver<()>) {
        tokio::spawn(async move {
            let server_config = match create_server_config(&self.cert_pem_base64, &self.key_pem_base64) {
                Ok(config) => config,
                Err(err) => {
                    error!("QUIC relay receiver server config failed: {}", err);
                    return;
                }
            };

            let endpoint = match Endpoint::server(server_config, self.listen_addr) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    error!("QUIC relay receiver bind failed: {}", err);
                    return;
                }
            };
            info!("QUIC relay receiver listening at {}", self.listen_addr);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    Some(connecting) = endpoint.accept() => {
                        let connection = match connecting.await {
                            Ok(conn) => conn,
                            Err(err) => {
                                error!("QUIC relay receiver connection failed: {}", err);
                                continue;
                            }
                        };
                        debug!("QUIC relay receiver accepted connection from {}", connection.remote_address());

                        let backend_addr = self.backend_addr;
                        let buffers = Arc::clone(&self.buffers);
                        let slots = self.slots;

                        tokio::spawn(async move {
                            handle_quic_connection(connection, backend_addr, buffers, slots).await;
                        });
                    }
                }
            }
        });
    }
}

async fn handle_quic_connection(
    connection: quinn::Connection,
    backend_addr: SocketAddr,
    buffers: Arc<RelayBuffers>,
    slots: usize,
) {
    let backend_pool = match BackendConnectionPool::new(backend_addr, BACKEND_POOL_SIZE).await {
        Ok(pool) => Arc::new(pool),
        Err(err) => {
            error!("QUIC relay backend pool init failed: {}", err);
            return;
        }
    };

    // Handle incoming QUIC streams (one per discrete request)
    loop {
        let stream = match connection.accept_bi().await {
            Ok((send, recv)) => (send, recv),
            Err(err) => {
                debug!("QUIC relay receiver accept stream failed: {}", err);
                break;
            }
        };

        let backend_pool_clone = Arc::clone(&backend_pool);
        let buffers_clone = Arc::clone(&buffers);

        tokio::spawn(async move {
            handle_quic_stream(
                stream,
                backend_pool_clone,
                buffers_clone,
                slots,
            )
            .await;
        });
    }
}

async fn handle_quic_stream(
    (_send, mut recv): (SendStream, RecvStream),
    backend_pool: Arc<BackendConnectionPool>,
    buffers: Arc<RelayBuffers>,
    slots: usize,
) {
    let mut decoder = StreamDecoder::new();
    let mut rebuilder = Http1StreamRebuilder::new();
    let mut streams: HashMap<u64, mpsc::Sender<Bytes>> = HashMap::new();

    let mut buf = vec![0u8; 65536];

    loop {
        let n = match recv.read(&mut buf).await {
            Ok(Some(n)) => n,
            Ok(None) => break,
            Err(err) => {
                warn!("QUIC relay receiver read failed: {}", err);
                break;
            }
        };
        trace!(bytes = n, "QUIC relay receiver read from stream");

        decoder.push(&buf[..n]);
        loop {
            match decoder.try_decode() {
                Ok(Some(frame)) => {
                    process_frame(
                        frame,
                        &mut rebuilder,
                        &mut streams,
                        &backend_pool,
                        &buffers,
                        slots,
                    )
                    .await;
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("QUIC relay decode failed: {}", err);
                    return;
                }
            }
        }
    }
}

async fn process_frame(
    frame: StreamFrame,
    rebuilder: &mut Http1StreamRebuilder,
    streams: &mut HashMap<u64, mpsc::Sender<Bytes>>,
    backend_pool: &Arc<BackendConnectionPool>,
    buffers: &Arc<RelayBuffers>,
    slots: usize,
) {
    let stream_id = frame.stream_id();
    let slot = slot_for_stream_id(stream_id, slots);
    let is_end = matches!(frame, StreamFrame::End(_));
    let frame_kind = match &frame {
        StreamFrame::Headers(_) => "headers",
        StreamFrame::Body(_) => "body",
        StreamFrame::End(_) => "end",
    };
    debug!(
        stream_id,
        frame_kind,
        "QUIC relay receiver decoded frame"
    );
    let out = match rebuilder.push_frame(frame) {
        Ok(out) => out,
        Err(err) => {
            warn!("QUIC relay rebuild failed: {}", err);
            return;
        }
    };
    debug!(stream_id, chunk_count = out.len(), "QUIC relay rebuilder output");

    if out.is_empty() {
        return;
    }
    trace!(
        stream_id,
        chunk_count = out.len(),
        is_end,
        "QUIC relay receiver rebuilt payload"
    );

    let sender = match streams.entry(stream_id) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let sender = start_backend_sender(Arc::clone(backend_pool), stream_id);
            entry.insert(sender.clone());
            trace!(stream_id, "QUIC relay backend sender started");
            sender
        }
    };

    for chunk in out {
        debug!(
            stream_id,
            slot,
            chunk_len = chunk.len(),
            "QUIC relay receiver output chunk"
        );
        if let Err(err) = buffers.response_append(slot, chunk.clone()).await {
            warn!("QUIC relay response buffer write failed: {}", err);
        }

        if sender.send(chunk).await.is_err() {
            warn!("QUIC relay backend sender closed for stream {}", stream_id);
            streams.remove(&stream_id);
            break;
        }
    }

    if is_end {
        streams.remove(&stream_id);
    }
}

fn start_backend_sender(pool: Arc<BackendConnectionPool>, stream_id: u64) -> mpsc::Sender<Bytes> {
    let (tx, mut rx) = mpsc::channel::<Bytes>(256);
    tokio::spawn(async move {
        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }

        if chunks.is_empty() {
            return;
        }

        let mut combined = Vec::new();
        for chunk in &chunks {
            combined.extend_from_slice(chunk);
        }

        trace!(
            stream_id,
            total_len = combined.len(),
            "QUIC relay backend sending combined request"
        );

        if let Err(err) = pool.send_request(&combined).await {
            warn!(
                "QUIC relay backend send failed for stream {}: {}",
                stream_id, err
            );
        }
    });
    tx
}

fn build_cache(slots: usize) -> ChunkCache {
    let options = PlaylistOptions {
        num_playlists: slots,
        max_segments: BUFFER_MAX_SEGMENTS,
        max_parts_per_segment: BUFFER_MAX_PARTS,
        max_parted_segments: 1,
        segment_min_ms: 0,
        buffer_size_kb: BUFFER_SIZE_KB,
        init_size_kb: BUFFER_INIT_KB,
    };
    ChunkCache::new(options)
}

fn slot_for_stream_id(stream_id: u64, slots: usize) -> usize {
    if slots == 0 {
        return 0;
    }
    (stream_id % slots as u64) as usize
}

fn create_client_config() -> anyhow::Result<ClientConfig> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
    )))
}

fn create_server_config(
    cert_pem_base64: &str,
    key_pem_base64: &str,
) -> anyhow::Result<ServerConfig> {
    let cert_pem = base64_engine.decode(cert_pem_base64)?;
    let key_pem = base64_engine.decode(key_pem_base64)?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())?
        .ok_or_else(|| anyhow::anyhow!("no private key found"))?;

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    server_crypto.alpn_protocols = vec![b"h3".to_vec()];

    Ok(ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?,
    )))
}

#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}
