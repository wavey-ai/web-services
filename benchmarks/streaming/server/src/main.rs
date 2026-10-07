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
        Bytes::from(data)
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
        writer.send_response(stream_head()?).await?;
        for index in 0..params.chunks {
            writer.send_data(self.chunk(&params, index)).await?;
        }
        writer.finish().await
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

    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    let _ = handle.shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.finished_rx).await;
}
