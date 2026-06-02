//! UDP ingest integration for the reusable `raptorq-datagram-fec` protocol crate.

use crate::{UploadResponseService, UploadStream};
use bytes::Bytes;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use raptorq_datagram_fec::{DatagramFecDecoder, ENCODING_PACKET_HEADER_LEN};
pub use raptorq_datagram_fec::{
    SequenceStats, UdpFecSender, DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, HEADER_LEN,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, error, info};

/// UDP+RaptorQ ingest server.
///
/// Listens on a UDP socket, reassembles FEC blocks, and feeds decoded payloads
/// into an [`UploadResponseService`] slot, matching the pattern used by
/// [`TcpIngest`](crate::TcpIngest).
pub struct UdpFecIngest {
    service: Arc<UploadResponseService>,
}

impl UdpFecIngest {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self { service }
    }

    /// Bind to `addr` and start accepting UDP datagrams.
    ///
    /// Returns a [`watch::Sender`] whose drop or send triggers graceful
    /// shutdown, matching the convention used by the other ingest types.
    pub async fn start(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let service = self.service;

        info!("UDP+FEC ingest listening on {}", addr);

        tokio::spawn(async move {
            let mut peer_streams: HashMap<SocketAddr, UploadStream> = HashMap::new();
            let mut peer_decoders: HashMap<SocketAddr, DatagramFecDecoder> = HashMap::new();
            let mut datagram = vec![0u8; 65536];

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("UDP+FEC ingest shutting down");
                        break;
                    }
                    result = socket.recv_from(&mut datagram) => {
                        match result {
                            Ok((n, peer)) => {
                                let buf = &datagram[..n];
                                if let Err(error) = handle_datagram(
                                    buf,
                                    peer,
                                    &service,
                                    &mut peer_streams,
                                    &mut peer_decoders,
                                ).await {
                                    error!(peer = %peer, "UDP+FEC error: {}", error);
                                }
                            }
                            Err(error) => {
                                error!("UDP recv error: {}", error);
                            }
                        }
                    }
                }
            }
        });

        Ok(shutdown_tx)
    }
}

async fn handle_datagram(
    buf: &[u8],
    peer: SocketAddr,
    service: &Arc<UploadResponseService>,
    peer_streams: &mut HashMap<SocketAddr, UploadStream>,
    peer_decoders: &mut HashMap<SocketAddr, DatagramFecDecoder>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if buf.len() < HEADER_LEN + ENCODING_PACKET_HEADER_LEN {
        return Ok(());
    }

    let stream_id = ensure_peer_stream(peer, service, peer_streams).await?;
    let decoder = peer_decoders.entry(peer).or_default();

    if let Some(decoded) = decoder.push_datagram(buf)? {
        let stats = decoder.sequence_stats();
        debug!(stream_id, bytes = decoded.len(), "UDP+FEC block decoded");
        if stats.missing > 0 || stats.duplicate_or_reordered > 0 {
            debug!(
                stream_id,
                received = stats.received,
                missing = stats.missing,
                duplicate_or_reordered = stats.duplicate_or_reordered,
                loss_fraction = stats.loss_fraction(),
                "UDP+FEC sequence recovery stats"
            );
        }
        service
            .append_request_body(stream_id, Bytes::from(decoded))
            .await
            .map_err(|error| format!("append_request_body: {error}"))?;
    }

    Ok(())
}

async fn ensure_peer_stream(
    peer: SocketAddr,
    service: &Arc<UploadResponseService>,
    peer_streams: &mut HashMap<SocketAddr, UploadStream>,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(stream) = peer_streams.get(&peer) {
        return Ok(stream.stream_id());
    }

    let stream = service
        .open_stream()
        .await
        .map_err(|error| format!("open_stream: {error}"))?;
    let stream_id = stream.stream_id();

    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: b"/udp-fec".to_vec(),
        headers: vec![HeaderField {
            name: b"x-peer-addr".to_vec(),
            value: peer.to_string().into_bytes(),
        }],
    });
    service
        .write_request_headers(stream_id, headers)
        .await
        .map_err(|error| format!("write_request_headers: {error}"))?;

    peer_streams.insert(peer, stream);
    debug!(stream_id, peer = %peer, "UDP+FEC new stream");
    Ok(stream_id)
}
