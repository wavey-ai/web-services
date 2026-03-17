//! UDP transport with RaptorQ forward error correction.
//!
//! # Protocol
//!
//! Each UDP datagram carries a 12-byte header followed by a serialised
//! RaptorQ [`EncodingPacket`]:
//!
//! ```text
//! 0               4               8              12
//! ├───────────────┼───────────────┼───────────────┤
//! │   block_id    │transfer_length│src_syms│sym_sz │  header (12 bytes)
//! └───────────────┴───────────────┴───────────────┘
//! │          EncodingPacket bytes …               │  variable
//! ```
//!
//! * `block_id` — monotonically increasing u32, identifies the FEC block.
//! * `transfer_length` — u32 LE, total payload bytes in this block.
//! * `src_syms` — u16 LE, K (number of source symbols per block).
//! * `sym_sz` — u16 LE, T (symbol size in bytes).
//!
//! The receiver reconstructs the
//! [`ObjectTransmissionInformation`] from these fields and feeds packets into a
//! per-block [`Decoder`].  Once K symbols are received the block is decoded and
//! delivered — even if some datagrams were lost, provided at most R were dropped.
//!
//! # Latency
//!
//! Latency overhead = time to fill one FEC block.  With the defaults
//! (K = 4, T = 1316, R = 1) at 48 kHz / 960-sample frames (~20 ms each)
//! the overhead is ≈ 80 ms and the transport can recover from any single
//! datagram loss per group of 5 without retransmission.  Tune K/R to taste.

use crate::UploadResponseService;
use bytes::Bytes;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use raptorq::{Decoder, EncodingPacket, Encoder, ObjectTransmissionInformation};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, error, info};

// ── wire constants ──────────────────────────────────────────────────────────

/// Bytes in the per-datagram header (block_id u32 + transfer_length u32 +
/// src_syms u16 + sym_sz u16).
pub const HEADER_LEN: usize = 12;

/// Default symbol size (bytes).  Chosen to fit within a standard Ethernet MTU
/// with room for IP/UDP headers.
pub const DEFAULT_SYMBOL_SIZE: u16 = 1316;

/// Default source symbols per FEC block (K).
pub const DEFAULT_SOURCE_SYMBOLS: u16 = 4;

/// Default repair symbols per FEC block (R).  One repair symbol lets the
/// receiver recover from any single datagram loss per block.
pub const DEFAULT_REPAIR_SYMBOLS: u32 = 1;

// ── wire header ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct WireHeader {
    block_id: u32,
    transfer_length: u32,
    src_syms: u16,
    sym_sz: u16,
}

impl WireHeader {
    fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.block_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.transfer_length.to_le_bytes());
        buf[8..10].copy_from_slice(&self.src_syms.to_le_bytes());
        buf[10..12].copy_from_slice(&self.sym_sz.to_le_bytes());
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_LEN {
            return None;
        }
        Some(Self {
            block_id: u32::from_le_bytes(buf[0..4].try_into().ok()?),
            transfer_length: u32::from_le_bytes(buf[4..8].try_into().ok()?),
            src_syms: u16::from_le_bytes(buf[8..10].try_into().ok()?),
            sym_sz: u16::from_le_bytes(buf[10..12].try_into().ok()?),
        })
    }

    fn oti(&self) -> ObjectTransmissionInformation {
        ObjectTransmissionInformation::with_defaults(
            self.transfer_length as u64,
            self.sym_sz,
        )
    }
}

// ── sender ───────────────────────────────────────────────────────────────────

/// Sends data blocks as UDP datagrams protected by RaptorQ FEC.
///
/// Each call to [`UdpFecSender::send`] encodes one block and fires K + R
/// datagrams at the target address.  The call returns once all datagrams have
/// been handed to the kernel; it does not wait for ACKs.
pub struct UdpFecSender {
    socket: Arc<UdpSocket>,
    target: SocketAddr,
    block_id: u32,
    source_symbols: u16,
    repair_symbols: u32,
    symbol_size: u16,
}

impl UdpFecSender {
    /// Bind a new UDP socket on any available local port and aim it at `target`.
    pub async fn new(target: SocketAddr) -> std::io::Result<Self> {
        let bind_addr: SocketAddr = if target.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
            target,
            block_id: 0,
            source_symbols: DEFAULT_SOURCE_SYMBOLS,
            repair_symbols: DEFAULT_REPAIR_SYMBOLS,
            symbol_size: DEFAULT_SYMBOL_SIZE,
        })
    }

    /// Override the number of source symbols per block (K).
    ///
    /// Lower K = lower latency but less FEC headroom.
    pub fn with_source_symbols(mut self, k: u16) -> Self {
        self.source_symbols = k;
        self
    }

    /// Override the number of repair symbols per block (R).
    ///
    /// Higher R = more redundancy, more bandwidth.
    pub fn with_repair_symbols(mut self, r: u32) -> Self {
        self.repair_symbols = r;
        self
    }

    /// Override the symbol (datagram payload) size in bytes.
    pub fn with_symbol_size(mut self, t: u16) -> Self {
        self.symbol_size = t;
        self
    }

    /// Encode `data` with RaptorQ and send K + R UDP datagrams to the target.
    pub async fn send(&mut self, data: &[u8]) -> std::io::Result<()> {
        let encoder = Encoder::with_defaults(data, self.symbol_size);
        let packets = encoder.get_encoded_packets(self.repair_symbols);

        let hdr = WireHeader {
            block_id: self.block_id,
            transfer_length: data.len() as u32,
            src_syms: self.source_symbols,
            sym_sz: self.symbol_size,
        };

        let mut buf = Vec::with_capacity(HEADER_LEN + self.symbol_size as usize + 4);
        for packet in &packets {
            let serialised = packet.serialize();
            buf.clear();
            buf.resize(HEADER_LEN, 0);
            hdr.encode(&mut buf[..HEADER_LEN]);
            buf.extend_from_slice(&serialised);
            self.socket.send_to(&buf, self.target).await?;
        }

        self.block_id = self.block_id.wrapping_add(1);
        Ok(())
    }
}

// ── receiver / ingest ────────────────────────────────────────────────────────

/// Per-block decoder state held by the receiver.
struct BlockState {
    decoder: Decoder,
    hdr: WireHeader,
}

/// UDP+RaptorQ ingest server.
///
/// Listens on a UDP socket, reassembles FEC blocks, and feeds decoded payloads
/// into an [`UploadResponseService`] slot — matching the pattern of
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
    /// Returns a [`watch::Sender`] whose drop (or any send) triggers a graceful
    /// shutdown matching the convention used by the other ingest types.
    pub async fn start(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let service = self.service;

        info!("UDP+FEC ingest listening on {}", addr);

        tokio::spawn(async move {
            // Each unique (peer_addr) gets its own stream slot in the service.
            let mut peer_streams: HashMap<SocketAddr, u64> = HashMap::new();
            // Per-block FEC state, keyed by (peer_addr, block_id).
            let mut blocks: HashMap<(SocketAddr, u32), BlockState> = HashMap::new();

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
                                if let Err(e) = handle_datagram(
                                    buf,
                                    peer,
                                    &service,
                                    &mut peer_streams,
                                    &mut blocks,
                                ).await {
                                    error!(peer = %peer, "UDP+FEC error: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("UDP recv error: {}", e);
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
    peer_streams: &mut HashMap<SocketAddr, u64>,
    blocks: &mut HashMap<(SocketAddr, u32), BlockState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let hdr = match WireHeader::decode(buf) {
        Some(h) => h,
        None => return Ok(()), // too short, ignore
    };

    let packet = EncodingPacket::deserialize(&buf[HEADER_LEN..]);

    // Ensure this peer has a stream slot.
    let stream_id = if let Some(&id) = peer_streams.get(&peer) {
        id
    } else {
        let _permit = service
            .acquire_stream()
            .await
            .map_err(|e| format!("acquire_stream: {e}"))?;
        let id = service.next_id();

        let headers = StreamHeaders::Request(StreamRequestHeaders {
            stream_id: id,
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
            .write_request_headers(id, headers)
            .await
            .map_err(|e| format!("write_request_headers: {e}"))?;

        peer_streams.insert(peer, id);
        debug!(stream_id = id, peer = %peer, "UDP+FEC new stream");
        id
    };

    // Feed the packet into (or create) the block decoder.
    let key = (peer, hdr.block_id);
    let state = blocks.entry(key).or_insert_with(|| BlockState {
        decoder: Decoder::new(hdr.oti()),
        hdr,
    });

    if let Some(decoded) = state.decoder.decode(packet) {
        // Block fully reconstructed — deliver to service.
        debug!(
            stream_id,
            block_id = hdr.block_id,
            bytes = decoded.len(),
            "UDP+FEC block decoded"
        );
        service
            .append_request_body(stream_id, Bytes::from(decoded))
            .await
            .map_err(|e| format!("append_request_body: {e}"))?;

        // Drop stale block entries (anything older than current block_id - 8).
        let cutoff = hdr.block_id.wrapping_sub(8);
        blocks.retain(|(p, bid), _| *p != peer || bid.wrapping_sub(cutoff) < u32::MAX / 2);
    }

    Ok(())
}
