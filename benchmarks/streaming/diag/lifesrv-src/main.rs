//! Streaming load-test server built on `web_service::H2H3Server`.
//!
//! Routes:
//! - `GET /stream?chunks=N&size=S&cpu=P&fresh=F` goes through `Router::route_stream`.
//!   The handler sends N chunks of S bytes. Each chunk is a new allocation when F=1
//!   (the default), or a zero-copy slice of a shared buffer when F=0. When P>0, the
//!   handler computes SHA-256 over the chunk P times and writes each digest into the
//!   first 32 bytes of the chunk.
//! - `POST /echo` goes through `Router::route_body_stream`. The handler sends the
//!   response head, then sends each request body chunk back as it arrives.
//! - `GET /` goes through `Router::route` and returns a 2-byte body.

use std::{
    io::Write as _,
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use futures_util::StreamExt;
use http::{Request, Response, StatusCode};
use sha2::{Digest, Sha256};
use web_service::{
    BodyStream, H2H3Server, HandlerResponse, HandlerResult, Router, Server, ServerBuilder,
    ServerError, StreamWriter, WebSocketHandler, WebTransportHandler,
};

const TEMPLATE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct StreamParams {
    chunks: usize,
    size: usize,
    cpu: usize,
    fresh: bool,
}

impl StreamParams {
    fn parse(query: Option<&str>) -> Self {
        let mut params = Self {
            chunks: 4,
            size: 16 * 1024,
            cpu: 0,
            fresh: true,
        };
        for pair in query.unwrap_or("").split('&') {
            let mut kv = pair.splitn(2, '=');
            let (Some(key), Some(value)) = (kv.next(), kv.next()) else {
                continue;
            };
            let Ok(value) = value.parse::<usize>() else {
                continue;
            };
            match key {
                "chunks" => params.chunks = value,
                "size" => params.size = value.clamp(1, TEMPLATE_BYTES),
                "cpu" => params.cpu = value,
                "fresh" => params.fresh = value != 0,
                _ => {}
            }
        }
        params
    }
}

struct BenchRouter {
    template: Bytes,
}

impl BenchRouter {
    fn new() -> Self {
        // Pseudo-random content, so that no layer can compress or deduplicate it.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut data = Vec::with_capacity(TEMPLATE_BYTES);
        while data.len() < TEMPLATE_BYTES {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            data.extend_from_slice(&state.to_le_bytes());
        }
        data.truncate(TEMPLATE_BYTES);
        Self {
            template: Bytes::from(data),
        }
    }

    fn chunk(&self, params: &StreamParams, index: usize) -> Bytes {
        let offset = (index * 4099) % (TEMPLATE_BYTES - params.size + 1);
        let source = &self.template[offset..offset + params.size];
        if !params.fresh && params.cpu == 0 {
            return self.template.slice(offset..offset + params.size);
        }
        let mut data = source.to_vec();
        for _ in 0..params.cpu {
            let digest = Sha256::digest(&data);
            let n = digest.len().min(data.len());
            data[..n].copy_from_slice(&digest[..n]);
        }
        life::make(data)
    }
}

fn stream_head() -> Result<Response<()>, ServerError> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .body(())
        .map_err(ServerError::Http)
}

#[async_trait]
impl Router for BenchRouter {
    async fn route(&self, _req: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(Bytes::from_static(b"ok")),
            content_type: Some("text/plain".into()),
            ..Default::default()
        })
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == "/stream"
    }

    fn has_body_stream_handler(&self, path: &str) -> bool {
        path == "/echo"
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        let params = StreamParams::parse(req.uri().query());
        let entered = std::time::Instant::now();
        writer.send_response(stream_head()?).await?;
        let head_sent = std::time::Instant::now();
        for index in 0..params.chunks {
            writer.send_data(self.chunk(&params, index)).await?;
        }
        let result = writer.finish().await;
        life::handler(entered, head_sent);
        result
    }

    async fn route_body_stream(
        &self,
        _req: Request<()>,
        mut body: BodyStream,
        mut writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        writer.send_response(stream_head()?).await?;
        while let Some(chunk) = body.next().await {
            writer.send_data(chunk?).await?;
        }
        writer.finish().await
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

struct Args {
    port: u16,
    workers: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        port: 8443,
        workers: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let value = it.next().unwrap_or_default();
        match arg.as_str() {
            "--port" => args.port = value.parse().expect("--port"),
            "--workers" => args.workers = value.parse().expect("--workers"),
            other => panic!("unknown argument {other}"),
        }
    }
    args
}

fn main() {
    let args = parse_args();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.workers)
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run(args));
}

async fn run(args: Args) {
    let certified = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("self-signed certificate");
    let cert_b64 = STANDARD.encode(certified.cert.pem());
    let key_b64 = STANDARD.encode(certified.key_pair.serialize_pem());

    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(args.port)
        .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .enable_h2(true)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_max_connections(1_000_000)
        .with_max_in_flight_requests(1_000_000)
        .with_handshake_timeout_ms(60_000)
        .with_router(Box::new(BenchRouter::new()))
        .build()
        .expect("server build");
    let handle = server.start().await.expect("server start");
    let _ = handle.ready_rx.await;
    println!(
        "READY pid={} port={} workers={}",
        std::process::id(),
        args.port,
        args.workers
    );
    let _ = std::io::stdout().flush();

    std::thread::spawn(life::report);
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    let _ = handle.shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.finished_rx).await;
}

/// Diagnosis only: counts live response chunks and records how long each one lives,
/// from allocation in the handler to the drop of its last reference.
mod life {
    use bytes::Bytes;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
    use std::time::Instant;
    pub static LIVE: AtomicI64 = AtomicI64::new(0);
    // Lifetime buckets in ms: <0.5 <1 <2 <5 <10 <20 <50 >=50; then count, sum_us.
    pub static LIFE: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];
    // Handler: count, sum of entry->head_sent us, sum of head_sent->finish us.
    pub static HANDLER: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
    struct Owner { buf: Vec<u8>, born: Instant }
    impl AsRef<[u8]> for Owner { fn as_ref(&self) -> &[u8] { &self.buf } }
    impl Drop for Owner {
        fn drop(&mut self) {
            LIVE.fetch_sub(1, Relaxed);
            let us = self.born.elapsed().as_micros() as u64;
            let ms = us as f64 / 1000.0;
            let i = [0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0].iter().position(|&b| ms < b).unwrap_or(7);
            LIFE[i].fetch_add(1, Relaxed);
            LIFE[8].fetch_add(1, Relaxed);
            LIFE[9].fetch_add(us, Relaxed);
        }
    }
    pub fn make(buf: Vec<u8>) -> Bytes {
        LIVE.fetch_add(1, Relaxed);
        Bytes::from_owner(Owner { buf, born: Instant::now() })
    }
    pub fn handler(entered: Instant, head_sent: Instant) {
        HANDLER[0].fetch_add(1, Relaxed);
        HANDLER[1].fetch_add((head_sent - entered).as_micros() as u64, Relaxed);
        HANDLER[2].fetch_add(head_sent.elapsed().as_micros() as u64, Relaxed);
    }
    pub fn report() {
        let mut prev = [0u64; 10];
        let mut prev_h = [0u64; 3];
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let cur: Vec<u64> = LIFE.iter().map(|a| a.load(Relaxed)).collect();
            let h: Vec<u64> = HANDLER.iter().map(|a| a.load(Relaxed)).collect();
            let d: Vec<u64> = (0..10).map(|i| cur[i] - prev[i]).collect();
            let hn = (h[0] - prev_h[0]).max(1);
            let n = d[8].max(1);
            eprintln!(
                "live_chunks={} freed/s={} mean_life_ms={:.2} life_ms_buckets[<.5,<1,<2,<5,<10,<20,<50,>=50]={:?} handler/s={} mean_entry_to_head_ms={:.3} mean_head_to_finish_ms={:.3}",
                LIVE.load(Relaxed), d[8], d[9] as f64 / n as f64 / 1000.0,
                &d[0..8], h[0] - prev_h[0],
                (h[1] - prev_h[1]) as f64 / hn as f64 / 1000.0,
                (h[2] - prev_h[2]) as f64 / hn as f64 / 1000.0
            );
            prev.copy_from_slice(&cur);
            prev_h.copy_from_slice(&h);
        }
    }
}
