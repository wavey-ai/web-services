mod common;

use std::{
    env,
    io,
    net::{IpAddr, Ipv4Addr},
    sync::OnceLock,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use reqwest::header::CONNECTION;
use tokio_rustls::rustls;
use tls_helpers::from_base64_raw;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, RequestHandler, Router, Server, ServerBuilder,
    ServerError,
};
use common::load_test_env;

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_REQUESTS_PER_WORKER: usize = 200;
const DEFAULT_RESPONSE: &[u8] = b"hello from bench";
const DEFAULT_RESPONSE_BYTES: usize = 1024 * 1024; // 1MB

struct HelloHandler {
    payload: Bytes,
}

#[async_trait]
impl RequestHandler for HelloHandler {
    async fn handle(
        &self,
        _req: Request<()>,
        _path_parts: Vec<&str>,
        _query: Option<&str>,
    ) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(self.payload.clone()),
            content_type: Some("text/plain".into()),
            ..Default::default()
        })
    }

    fn can_handle(&self, path: &str) -> bool {
        path == "/"
    }
}

struct BenchRouter {
    http: HelloHandler,
}

impl BenchRouter {
    fn new(payload: Bytes) -> Self {
        Self {
            http: HelloHandler { payload },
        }
    }
}

#[async_trait]
impl Router for BenchRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(str::to_string);
        if self.http.can_handle(&path) {
            return self.http.handle(req, vec![], query.as_deref()).await;
        }

        Ok(HandlerResponse {
            status: StatusCode::NOT_FOUND,
            ..Default::default()
        })
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

    fn webtransport_handler(&self) -> Option<&dyn web_service::WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn web_service::WebSocketHandler> {
        None
    }
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

struct BenchConfig {
    enabled: bool,
    concurrency: usize,
    requests_per_worker: usize,
    run_h1: bool,
    run_h2: bool,
    run_new_connections: bool,
    run_reuse_connections: bool,
    response_bytes: usize,
}

impl BenchConfig {
    fn from_env_args() -> Self {
        let mut enabled = false;
        let mut concurrency = None;
        let mut requests_per_worker = None;
        let mut run_h1 = false;
        let mut run_h2 = false;
        let mut run_new_connections = false;
        let mut run_reuse_connections = false;
        let mut response_bytes = None;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--benchmark" => enabled = true,
                "--h1" => run_h1 = true,
                "--h2" => run_h2 = true,
                "--new-connections" => run_new_connections = true,
                "--reuse-connections" => run_reuse_connections = true,
                "--response-bytes" => {
                    if let Some(value) = args.next() {
                        response_bytes = value.parse::<usize>().ok();
                    }
                }
                "--concurrency" => {
                    if let Some(value) = args.next() {
                        concurrency = value.parse::<usize>().ok();
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

        if run_new_connections || run_reuse_connections {
            enabled = true;
        }

        if enabled && !(run_h1 || run_h2) {
            run_h1 = true;
            run_h2 = true;
        }

        if enabled && !(run_new_connections || run_reuse_connections) {
            run_new_connections = true;
            run_reuse_connections = true;
        }

        if concurrency.is_none() {
            concurrency = env::var("BENCH_CONCURRENCY")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        if requests_per_worker.is_none() {
            requests_per_worker = env::var("BENCH_REQUESTS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        if response_bytes.is_none() {
            response_bytes = env::var("BENCH_RESPONSE_BYTES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
        }

        let mut concurrency = concurrency.unwrap_or_else(default_concurrency);
        if concurrency == 0 {
            concurrency = 1;
        }

        let mut requests_per_worker = requests_per_worker.unwrap_or(DEFAULT_REQUESTS_PER_WORKER);
        if requests_per_worker == 0 {
            requests_per_worker = DEFAULT_REQUESTS_PER_WORKER;
        }

        let mut response_bytes = response_bytes.unwrap_or(DEFAULT_RESPONSE_BYTES);
        if response_bytes == 0 {
            response_bytes = DEFAULT_RESPONSE_BYTES;
        }

        Self {
            enabled,
            concurrency,
            requests_per_worker,
            run_h1,
            run_h2,
            run_new_connections,
            run_reuse_connections,
            response_bytes,
        }
    }

    fn scenarios(&self) -> Vec<ConnectionScenario> {
        let mut scenarios = Vec::new();
        if self.run_new_connections {
            scenarios.push(ConnectionScenario::NewConnections);
        }
        if self.run_reuse_connections {
            scenarios.push(ConnectionScenario::ReuseConnections);
        }
        if scenarios.is_empty() {
            scenarios.push(ConnectionScenario::NewConnections);
        }
        scenarios
    }
}

#[derive(Clone, Copy)]
enum ConnectionScenario {
    NewConnections,
    ReuseConnections,
}

impl ConnectionScenario {
    fn label(self) -> &'static str {
        match self {
            ConnectionScenario::NewConnections => "new-conn",
            ConnectionScenario::ReuseConnections => "reuse-conn",
        }
    }

    fn pool_max_idle(self, config: &BenchConfig) -> usize {
        match self {
            ConnectionScenario::NewConnections => 0,
            ConnectionScenario::ReuseConnections => config.concurrency,
        }
    }
}

fn build_payload(response_bytes: usize) -> Bytes {
    if response_bytes == DEFAULT_RESPONSE_BYTES {
        return Bytes::from_static(DEFAULT_RESPONSE);
    }
    Bytes::from(vec![b'a'; response_bytes])
}

struct BenchStats {
    label: String,
    rps: f64,
    avg_ms: f64,
}

fn print_summary(stats: &[BenchStats]) {
    if stats.is_empty() {
        return;
    }

    let mut ordered: Vec<&BenchStats> = stats.iter().collect();
    ordered.sort_by(|a, b| b.rps.partial_cmp(&a.rps).unwrap_or(std::cmp::Ordering::Equal));

    println!();
    println!("benchmark summary (higher req/s is better):");
    for stat in &ordered {
        println!(
            "{:>8.2} req/s | {:>6.2} ms avg | {}",
            stat.rps, stat.avg_ms, stat.label
        );
    }

    if let Some(best) = ordered.first() {
        println!("best overall: {} at {:.2} req/s", best.label, best.rps);
    }

    let has_new = stats.iter().any(|stat| stat.label.contains("(new-conn)"));
    let has_reuse = stats.iter().any(|stat| stat.label.contains("(reuse-conn)"));
    if has_new {
        if let Some(best) = stats
            .iter()
            .filter(|stat| stat.label.contains("(new-conn)"))
            .max_by(|a, b| a.rps.partial_cmp(&b.rps).unwrap_or(std::cmp::Ordering::Equal))
        {
            println!("best new-conn: {} at {:.2} req/s", best.label, best.rps);
        }
    }
    if has_reuse {
        if let Some(best) = stats
            .iter()
            .filter(|stat| stat.label.contains("(reuse-conn)"))
            .max_by(|a, b| a.rps.partial_cmp(&b.rps).unwrap_or(std::cmp::Ordering::Equal))
        {
            println!(
                "best reuse-conn: {} at {:.2} req/s",
                best.label, best.rps
            );
        }
    }
}

async fn start_server(
    cert_b64: &str,
    key_b64: &str,
    port: u16,
    payload: Bytes,
) -> HandlerResult<web_service::ServerHandle> {
    let router = Box::new(BenchRouter::new(payload));
    let server = H2H3Server::builder()
        .with_tls(cert_b64.to_string(), key_b64.to_string())
        .with_port(port)
        .enable_h2(true)
        .enable_websocket(false)
        .with_router(router)
        .build()?;

    server.start().await
}

async fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if tokio::net::TcpStream::connect((IpAddr::V4(Ipv4Addr::LOCALHOST), port))
            .await
            .is_ok()
        {
            break;
        }
        if Instant::now() > deadline {
            panic!("server did not start listening on port {}", port);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn run_reqwest_benchmark(
    label: &str,
    client: reqwest::Client,
    url: String,
    config: &BenchConfig,
    expected_version: reqwest::Version,
    close_header: bool,
) -> BenchResult<BenchStats> {
    let total_requests = config.concurrency * config.requests_per_worker;
    println!(
        "{label} benchmark starting: concurrency={}, requests_per_worker={}, total_requests={}, response_bytes={}",
        config.concurrency,
        config.requests_per_worker,
        total_requests,
        config.response_bytes
    );

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let client = client.clone();
        let url = url.clone();
        let requests = config.requests_per_worker;
        tasks.push(tokio::spawn(async move {
            let mut completed = 0usize;
            for _ in 0..requests {
                let request = if close_header {
                    client.get(&url).header(CONNECTION, "close")
                } else {
                    client.get(&url)
                };
                let resp = request.send().await?;
                let status = resp.status();
                let version = resp.version();
                let _ = resp.bytes().await?;
                if status != StatusCode::OK {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("unexpected status {status}"),
                    )
                    .into());
                }
                if version != expected_version {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("unexpected http version {version:?}"),
                    )
                    .into());
                }
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
        rps,
        avg_ms,
    })
}

async fn run_h1_benchmark(
    cert_b64: &str,
    host: &str,
    port: u16,
    config: &BenchConfig,
    scenario: ConnectionScenario,
) -> BenchResult<BenchStats> {
    let cert_pem = from_base64_raw(cert_b64)?;
    let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem)?;
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .http1_only()
        .pool_max_idle_per_host(scenario.pool_max_idle(config))
        .build()?;

    let url = format!("https://{host}:{port}/");
    let label = format!("h1.1 ({})", scenario.label());
    run_reqwest_benchmark(
        &label,
        client,
        url,
        config,
        reqwest::Version::HTTP_11,
        matches!(scenario, ConnectionScenario::NewConnections),
    )
    .await
}

async fn run_h2_benchmark(
    cert_b64: &str,
    host: &str,
    port: u16,
    config: &BenchConfig,
    scenario: ConnectionScenario,
) -> BenchResult<BenchStats> {
    let cert_pem = from_base64_raw(cert_b64)?;
    let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem)?;
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .http2_prior_knowledge()
        .pool_max_idle_per_host(scenario.pool_max_idle(config))
        .build()?;

    let url = format!("https://{host}:{port}/");
    let label = format!("h2 ({})", scenario.label());
    run_reqwest_benchmark(
        &label,
        client,
        url,
        config,
        reqwest::Version::HTTP_2,
        false,
    )
    .await
}

async fn run_with_server<F, Fut>(
    cert_b64: &str,
    key_b64: &str,
    payload: Bytes,
    bench_fn: F,
) -> BenchResult<Vec<BenchStats>>
where
    F: FnOnce(u16) -> Fut,
    Fut: std::future::Future<Output = BenchResult<Vec<BenchStats>>>,
{
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let port = pick_unused_port()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "pick port"))?;
    let handle = start_server(cert_b64, key_b64, port, payload).await?;
    handle.ready_rx.await?;
    wait_for_port(port).await;

    let result = bench_fn(port).await;

    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    drop(guard);

    result
}

async fn run_benchmarks(
    cert_b64: &str,
    key_b64: &str,
    host: &str,
    config: &BenchConfig,
) -> BenchResult<()> {
    let payload = build_payload(config.response_bytes);
    let mut all_stats = Vec::new();
    if config.run_h1 || config.run_h2 {
        let scenarios = config.scenarios();
        let stats = run_with_server(cert_b64, key_b64, payload.clone(), |port| async move {
            let mut stats = Vec::new();
            for scenario in scenarios {
                if config.run_h1 {
                    stats.push(run_h1_benchmark(cert_b64, host, port, config, scenario).await?);
                }
                if config.run_h2 {
                    stats.push(run_h2_benchmark(cert_b64, host, port, config, scenario).await?);
                }
            }
            Ok(stats)
        })
        .await?;
        all_stats.extend(stats);
    }

    print_summary(&all_stats);
    Ok(())
}

fn main() -> BenchResult<()> {
    let config = BenchConfig::from_env_args();
    if !config.enabled {
        eprintln!(
            "skipping benchmarks; run with --benchmark (defaults to h1/h2 + both connection modes; optional: --h1 --h2 --concurrency N --requests N --new-connections --reuse-connections --response-bytes N)"
        );
        return Ok(());
    }

    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping benchmarks: missing TLS env");
            return Ok(());
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_benchmarks(&cert_b64, &key_b64, &host, &config))?;
    Ok(())
}
