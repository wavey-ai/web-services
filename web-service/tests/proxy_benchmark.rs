mod common;

use std::{
    env,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{atomic::{AtomicUsize, Ordering}, Arc, OnceLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use tokio_rustls::rustls;
use tls_helpers::from_base64_raw;
use web_service::{
    BodyStream, LoadBalancingMode, ProxyConfig, ProxyIngress, ProxyState, Server, ServerBuilder,
    UpstreamProtocol, H2H3Server, HandlerResponse, HandlerResult, Router, ServerError,
    WebSocketHandler, WebTransportHandler,
};
use xxhash_rust::xxh3::xxh3_64;
use common::load_test_env;

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_REQUESTS_PER_WORKER: usize = 200;
const DEFAULT_PAYLOAD_BYTES: usize = 1024 * 1024; // 1MB

struct HashEchoRouter;

#[async_trait]
impl Router for HashEchoRouter {
    async fn route(&self, _req: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(hash_response(Bytes::new()))
    }

    async fn route_body(
        &self,
        _req: Request<()>,
        mut body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        let mut data = Vec::new();
        while let Some(chunk) = body.next().await {
            data.extend_from_slice(&chunk?);
        }
        Ok(hash_response(Bytes::from(data)))
    }

    fn has_body_handler(&self, _path: &str) -> bool {
        true
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn web_service::StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("no streaming".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

struct BackendHandle {
    port: u16,
    shutdown_tx: tokio::sync::watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
}

struct ProxyHandle {
    port: u16,
    shutdown_tx: tokio::sync::watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
    state: Arc<ProxyState>,
}

struct BenchStats {
    label: String,
    completed: usize,
    rps: f64,
    avg_ms: f64,
}

struct BenchConfig {
    enabled: bool,
    concurrency: usize,
    requests_per_worker: usize,
    payload_bytes: usize,
    backends: usize,
    run_h1: bool,
    run_h2: bool,
}

static SERVER_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
}

impl BenchConfig {
    fn from_env_args() -> Self {
        let mut enabled = false;
        let mut concurrency = None;
        let mut requests_per_worker = None;
        let mut payload_bytes = None;
        let mut backends = None;
        let mut run_h1 = false;
        let mut run_h2 = false;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--benchmark" => enabled = true,
                "--h1" => {
                    run_h1 = true;
                }
                "--h2" => {
                    run_h2 = true;
                }
                "--payload-bytes" => {
                    if let Some(value) = args.next() {
                        payload_bytes = value.parse::<usize>().ok();
                    }
                }
                "--response-bytes" => {
                    if let Some(value) = args.next() {
                        if payload_bytes.is_none() {
                            payload_bytes = value.parse::<usize>().ok();
                        }
                    }
                }
                "--concurrency" => {
                    if let Some(value) = args.next() {
                        concurrency = value.parse::<usize>().ok();
                    }
                }
                "--backends" => {
                    if let Some(value) = args.next() {
                        backends = value.parse::<usize>().ok();
                    }
                }
                "--requests" => {
                    if let Some(value) = args.next() {
                        requests_per_worker = value.parse::<usize>().ok();
                    }
                }
                _ => {}
            }
        }

        if !enabled {
            if let Ok(value) = env::var("BENCHMARK") {
                let value = value.trim().to_ascii_lowercase();
                if matches!(value.as_str(), "1" | "true" | "yes" | "on") {
                    enabled = true;
                }
            }
        }

        if run_h1 || run_h2 {
            enabled = true;
        }

        if enabled && !(run_h1 || run_h2) {
            run_h1 = true;
            run_h2 = true;
        }

        if concurrency.is_none() {
            concurrency = env::var("BENCH_CONCURRENCY")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        if backends.is_none() {
            backends = env::var("BENCH_BACKENDS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        if requests_per_worker.is_none() {
            requests_per_worker = env::var("BENCH_REQUESTS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        if payload_bytes.is_none() {
            payload_bytes = env::var("BENCH_PAYLOAD_BYTES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }
        if payload_bytes.is_none() {
            payload_bytes = env::var("BENCH_RESPONSE_BYTES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        let backends = backends.unwrap_or_else(default_concurrency).max(1);
        let concurrency = concurrency.unwrap_or(backends + 1).max(1);
        let requests_per_worker = requests_per_worker.unwrap_or(DEFAULT_REQUESTS_PER_WORKER).max(1);
        let payload_bytes = payload_bytes.unwrap_or(DEFAULT_PAYLOAD_BYTES).max(8);

        Self {
            enabled,
            concurrency,
            requests_per_worker,
            payload_bytes,
            backends,
            run_h1,
            run_h2,
        }
    }

    fn max_inflight(&self) -> usize {
        let limit = self.backends.saturating_add(1);
        limit.min(self.concurrency).max(1)
    }
}

fn hash_hex(payload: &[u8]) -> String {
    format!("{:016x}", xxh3_64(payload))
}

fn hash_response(payload: Bytes) -> HandlerResponse {
    HandlerResponse {
        status: StatusCode::OK,
        body: Some(Bytes::from(hash_hex(&payload))),
        content_type: Some("text/plain".into()),
        ..Default::default()
    }
}

fn make_payload(index: u64, size: usize) -> Bytes {
    let size = size.max(8);
    let mut data = vec![0u8; size];
    data[..8].copy_from_slice(&index.to_be_bytes());
    let mut fill = (index as u8).wrapping_add(1);
    for byte in data.iter_mut().skip(8) {
        *byte = fill;
        fill = fill.wrapping_add(1);
    }
    Bytes::from(data)
}

async fn send_reqwest_with_retry(
    client: &reqwest::Client,
    url: &str,
    payload: Bytes,
    expected_hash: &str,
) -> BenchResult<()> {
    const MAX_ATTEMPTS: usize = 3;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let resp = match client.post(url).body(payload.clone()).send().await {
            Ok(resp) => resp,
            Err(err) => {
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                    continue;
                }
                return Err(err.into());
            }
        };
        let status = resp.status();
        let version = resp.version();
        let body = match resp.bytes().await {
            Ok(body) => body,
            Err(err) => {
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                    continue;
                }
                return Err(err.into());
            }
        };
        if status != StatusCode::OK {
            let body_text = String::from_utf8_lossy(&body);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unexpected status {status}: {body_text}"),
            )
            .into());
        }
        if version != reqwest::Version::HTTP_11 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unexpected http version {version:?}"),
            )
            .into());
        }
        if body.as_ref() != expected_hash.as_bytes() {
            let body_text = String::from_utf8_lossy(&body);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unexpected hash {body_text} expected {expected_hash}"),
            )
            .into());
        }
        return Ok(());
    }
}

async fn wait_for_port(port: u16) {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        if tokio::net::TcpStream::connect((IpAddr::V4(Ipv4Addr::LOCALHOST), port))
            .await
            .is_ok()
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("server did not start listening on port {}", port);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn start_backend(
    protocol: UpstreamProtocol,
    cert_b64: &str,
    key_b64: &str,
) -> io::Result<BackendHandle> {
    let port = pick_unused_port().expect("pick backend port");
    let router = Box::new(HashEchoRouter);
    let server = H2H3Server::builder()
        .with_tls(cert_b64.to_string(), key_b64.to_string())
        .with_port(port)
        .enable_h2(matches!(protocol, UpstreamProtocol::Http1 | UpstreamProtocol::Http2))
        .enable_websocket(false)
        .with_router(router)
        .build()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

    let handle = server
        .start()
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
    let web_service::ServerHandle {
        shutdown_tx,
        ready_rx,
        finished_rx,
    } = handle;
    ready_rx
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

    wait_for_port(port).await;

    Ok(BackendHandle {
        port,
        shutdown_tx,
        finished_rx,
    })
}

async fn start_backends(
    protocol: UpstreamProtocol,
    cert_b64: &str,
    key_b64: &str,
    count: usize,
) -> io::Result<Vec<BackendHandle>> {
    let mut backends = Vec::with_capacity(count);
    for _ in 0..count {
        backends.push(start_backend(protocol, cert_b64, key_b64).await?);
    }
    Ok(backends)
}

async fn start_proxy(
    cert_b64: &str,
    key_b64: &str,
    upstream_protocol: UpstreamProtocol,
    max_backends: usize,
) -> io::Result<ProxyHandle> {
    let port = pick_unused_port().expect("pick proxy port");
    let config = ProxyConfig {
        cert_pem_base64: cert_b64.to_string(),
        key_pem_base64: key_b64.to_string(),
        port,
        enable_h2: true,
        enable_websocket: false,
        initial_mode: LoadBalancingMode::LeastConn,
        upstream_protocol,
        max_queue: 1,
        max_backends,
        queue_request_kb: 5 * 1024,
        queue_slot_kb: 200,
        quic_relay: None,
    };
    let ingress = ProxyIngress::from_config(config)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
    let state = ingress.state();
    let handle = ingress
        .start()
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
    let web_service::ServerHandle {
        shutdown_tx,
        ready_rx,
        finished_rx,
    } = handle;
    ready_rx
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
    wait_for_port(port).await;

    Ok(ProxyHandle {
        port,
        shutdown_tx,
        finished_rx,
        state,
    })
}

async fn run_reqwest_benchmark(
    label: &str,
    client: reqwest::Client,
    url: String,
    config: &BenchConfig,
) -> BenchResult<BenchStats> {
    let total_requests = config.concurrency * config.requests_per_worker;
    let max_inflight = config.max_inflight().min(total_requests);
    println!(
        "{label} benchmark starting: concurrency={}, backends={}, max_inflight={}, requests_per_worker={}, total_requests={}, payload_bytes={}",
        config.concurrency,
        config.backends,
        max_inflight,
        config.requests_per_worker,
        total_requests,
        config.payload_bytes
    );

    // Warmup phase (not timed)
    println!("{label} warmup: sending {} warmup requests", max_inflight);
    let warmup_counter = Arc::new(AtomicUsize::new(0));
    let mut warmup_tasks = Vec::with_capacity(max_inflight);
    for _ in 0..max_inflight {
        let client = client.clone();
        let url = url.clone();
        let payload_bytes = config.payload_bytes;
        let warmup_counter = Arc::clone(&warmup_counter);
        warmup_tasks.push(tokio::spawn(async move {
            let idx = warmup_counter.fetch_add(1, Ordering::SeqCst);
            let payload = make_payload(idx as u64, payload_bytes);
            let expected_hash = hash_hex(&payload);
            send_reqwest_with_retry(&client, &url, payload, &expected_hash).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }));
    }
    for task in warmup_tasks {
        task.await??;
    }

    // Now run the timed benchmark
    let start = Instant::now();
    let next_request = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(max_inflight);
    for _ in 0..max_inflight {
        let client = client.clone();
        let url = url.clone();
        let payload_bytes = config.payload_bytes;
        let total_requests = total_requests;
        let next_request = Arc::clone(&next_request);
        tasks.push(tokio::spawn(async move {
            let mut completed = 0usize;
            loop {
                let idx = next_request.fetch_add(1, Ordering::SeqCst);
                if idx >= total_requests {
                    break;
                }
                let payload = make_payload(idx as u64, payload_bytes);
                let expected_hash = hash_hex(&payload);
                send_reqwest_with_retry(&client, &url, payload, &expected_hash).await?;
                completed += 1;
            }
            Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(completed)
        }));
    }

    let mut completed = 0usize;
    for task in tasks {
        completed += task.await??;
    }
    let elapsed = start.elapsed();

    let rps = completed as f64 / elapsed.as_secs_f64();
    let avg_ms = (elapsed.as_secs_f64() * 1000.0) / completed as f64;
    println!(
        "{label} benchmark complete: completed={}, elapsed={:?}, req/s={:.2}, avg_ms={:.2}",
        completed, elapsed, rps, avg_ms
    );

    Ok(BenchStats {
        label: label.to_string(),
        completed,
        rps,
        avg_ms,
    })
}

async fn run_proxy_benchmark(
    protocol: UpstreamProtocol,
    cert_b64: &str,
    key_b64: &str,
    host: &str,
    config: &BenchConfig,
) -> BenchResult<BenchStats> {
    let backends = start_backends(protocol, cert_b64, key_b64, config.backends).await?;
    let proxy = start_proxy(cert_b64, key_b64, protocol, config.backends).await?;

    for backend in &backends {
        let backend_url = url::Url::parse(&format!("https://{host}:{}", backend.port))?;
        proxy
            .state
            .add_backend(backend_url, Some(1))
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    }

    let cert_pem = from_base64_raw(cert_b64)?;
    let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem)?;
    let url = format!("https://{host}:{}/", proxy.port);

    let max_inflight = config.max_inflight();
    let mut builder = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .http1_only()
        .pool_max_idle_per_host(max_inflight);
    let local_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), proxy.port);
    builder = builder.resolve(host, local_addr);
    let client = builder.build()?;

    let label = format!("proxy {} ingress", protocol.as_str());
    let stats = run_reqwest_benchmark(&label, client, url, config).await?;

    let _ = proxy.shutdown_tx.send(());
    let _ = proxy.finished_rx.await;
    for backend in backends {
        let _ = backend.shutdown_tx.send(());
        let _ = backend.finished_rx.await;
    }

    Ok(stats)
}

async fn run_benchmarks(
    cert_b64: &str,
    key_b64: &str,
    host: &str,
    config: &BenchConfig,
) -> BenchResult<()> {
    let _guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let mut stats = Vec::new();

    // H1 and H2 ingress benchmarks (using reqwest)
    if config.run_h1 {
        stats.push(
            run_proxy_benchmark(UpstreamProtocol::Http1, cert_b64, key_b64, host, config).await?,
        );
    }
    if config.run_h2 {
        stats.push(
            run_proxy_benchmark(UpstreamProtocol::Http2, cert_b64, key_b64, host, config).await?,
        );
    }

    println!("proxy benchmark summary (higher req/s is better):");
    for stat in stats {
        println!(
            "{label}: req/s={rps:.2}, avg_ms={avg_ms:.2}, completed={completed}",
            label = stat.label,
            rps = stat.rps,
            avg_ms = stat.avg_ms,
            completed = stat.completed
        );
    }

    Ok(())
}

fn main() -> BenchResult<()> {
    ensure_rustls_provider();
    init_tracing();
    let config = BenchConfig::from_env_args();
    if !config.enabled {
        println!(
            "skipping proxy benchmarks; run with --benchmark (defaults to h1/h2; optional: --h1 --h2 --concurrency N --backends N --requests N --payload-bytes N [--response-bytes alias])"
        );
        return Ok(());
    }

    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(value) => value,
        None => {
            eprintln!("skipping proxy benchmarks: missing TLS env");
            return Ok(());
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_benchmarks(&cert_b64, &key_b64, &host, &config))?;

    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init();
}
