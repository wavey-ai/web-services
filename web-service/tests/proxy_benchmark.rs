mod common;

use std::{
    env,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use tokio_rustls::rustls;
use tls_helpers::from_base64_raw;
use web_service::{
    LoadBalancingMode, ProxyConfig, ProxyIngress, ProxyState, Server, ServerBuilder,
    UpstreamProtocol, H2H3Server, HandlerResponse, HandlerResult, Router, ServerError,
    WebSocketHandler, WebTransportHandler,
};
use common::load_test_env;

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_REQUESTS_PER_WORKER: usize = 200;
const DEFAULT_RESPONSE_BYTES: usize = 1024;

struct PayloadRouter {
    payload: Bytes,
}

#[async_trait]
impl Router for PayloadRouter {
    async fn route(&self, _req: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(self.payload.clone()),
            content_type: Some("text/plain".into()),
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
    response_bytes: usize,
    run_h1: bool,
    run_h2: bool,
    run_h3: bool,
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
        let mut response_bytes = None;
        let mut run_h1 = false;
        let mut run_h2 = false;
        let mut run_h3 = false;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--benchmark" => enabled = true,
                "--h1" => run_h1 = true,
                "--h2" => run_h2 = true,
                "--h3" => run_h3 = true,
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

        if run_h1 || run_h2 || run_h3 {
            enabled = true;
        }

        if enabled && !(run_h1 || run_h2 || run_h3) {
            run_h1 = true;
            run_h2 = true;
            run_h3 = true;
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

        let concurrency = concurrency.unwrap_or_else(default_concurrency);
        let requests_per_worker = requests_per_worker.unwrap_or(DEFAULT_REQUESTS_PER_WORKER);
        let response_bytes = response_bytes.unwrap_or(DEFAULT_RESPONSE_BYTES);

        Self {
            enabled,
            concurrency,
            requests_per_worker,
            response_bytes,
            run_h1,
            run_h2,
            run_h3,
        }
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
    response_bytes: usize,
) -> io::Result<BackendHandle> {
    let port = pick_unused_port().expect("pick backend port");
    let payload = Bytes::from(vec![b'a'; response_bytes]);
    let router = Box::new(PayloadRouter { payload });
    let server = H2H3Server::builder()
        .with_tls(cert_b64.to_string(), key_b64.to_string())
        .with_port(port)
        .enable_h2(matches!(protocol, UpstreamProtocol::Http1 | UpstreamProtocol::Http2))
        .enable_h3(matches!(protocol, UpstreamProtocol::Http3))
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

    if matches!(protocol, UpstreamProtocol::Http3) {
        tokio::time::sleep(Duration::from_millis(150)).await;
    } else {
        wait_for_port(port).await;
    }

    Ok(BackendHandle {
        port,
        shutdown_tx,
        finished_rx,
    })
}

async fn start_proxy(
    cert_b64: &str,
    key_b64: &str,
    upstream_protocol: UpstreamProtocol,
) -> io::Result<ProxyHandle> {
    let port = pick_unused_port().expect("pick proxy port");
    let config = ProxyConfig {
        cert_pem_base64: cert_b64.to_string(),
        key_pem_base64: key_b64.to_string(),
        port,
        enable_h2: true,
        enable_h3: false,
        enable_websocket: false,
        initial_mode: LoadBalancingMode::LeastConn,
        upstream_protocol,
        max_queue: None,
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
        let expected_bytes = config.response_bytes;
        tasks.push(tokio::spawn(async move {
            let mut completed = 0usize;
            for _ in 0..requests {
                let resp = client.get(&url).send().await?;
                let status = resp.status();
                let version = resp.version();
                let body = resp.bytes().await?;
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
                if body.len() != expected_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("unexpected body size {}", body.len()),
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
    let backend = start_backend(protocol, cert_b64, key_b64, config.response_bytes).await?;
    let proxy = start_proxy(cert_b64, key_b64, protocol).await?;

    let backend_url = url::Url::parse(&format!("https://{host}:{}", backend.port))?;
    proxy
        .state
        .add_backend(backend_url, None)
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    let cert_pem = from_base64_raw(cert_b64)?;
    let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem)?;
    let url = format!("https://{host}:{}/", proxy.port);

    let mut builder = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .http1_only()
        .pool_max_idle_per_host(config.concurrency);
    let local_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), proxy.port);
    builder = builder.resolve(host, local_addr);
    let client = builder.build()?;

    let label = format!("proxy upstream {}", protocol.as_str());
    let stats = run_reqwest_benchmark(&label, client, url, config).await?;

    let _ = proxy.shutdown_tx.send(());
    let _ = proxy.finished_rx.await;
    let _ = backend.shutdown_tx.send(());
    let _ = backend.finished_rx.await;

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
    if config.run_h3 {
        stats.push(
            run_proxy_benchmark(UpstreamProtocol::Http3, cert_b64, key_b64, host, config).await?,
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
    let config = BenchConfig::from_env_args();
    if !config.enabled {
        println!(
            "skipping proxy benchmarks; run with --benchmark (defaults to h1/h2/h3; optional: --h1 --h2 --h3 --concurrency N --requests N --response-bytes N)"
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
