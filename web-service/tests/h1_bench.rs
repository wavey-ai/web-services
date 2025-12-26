use std::{
    env,
    io,
    net::{IpAddr, Ipv4Addr},
    sync::OnceLock,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use dotenvy::dotenv;
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use reqwest::header::CONNECTION;
use tokio_rustls::rustls;
use tls_helpers::from_base64_raw;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, RequestHandler, Router, Server, ServerBuilder,
    ServerError,
};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_REQUESTS_PER_WORKER: usize = 200;

struct HelloHandler;

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
            body: Some(Bytes::from_static(b"hello from bench")),
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
    fn new() -> Self {
        Self { http: HelloHandler }
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

fn load_env() -> Option<(String, String, String)> {
    dotenv().ok();
    let cert = env::var("TLS_CERT_BASE64").ok()?;
    let key = env::var("TLS_KEY_BASE64").ok()?;
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "local.aldea.ai".into());
    Some((cert, key, host))
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
}

impl BenchConfig {
    fn from_env_args() -> Self {
        let mut enabled = false;
        let mut concurrency = None;
        let mut requests_per_worker = None;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--benchmark" => enabled = true,
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

        let mut concurrency = concurrency.unwrap_or_else(default_concurrency);
        if concurrency == 0 {
            concurrency = 1;
        }

        let mut requests_per_worker = requests_per_worker.unwrap_or(DEFAULT_REQUESTS_PER_WORKER);
        if requests_per_worker == 0 {
            requests_per_worker = DEFAULT_REQUESTS_PER_WORKER;
        }

        Self {
            enabled,
            concurrency,
            requests_per_worker,
        }
    }
}

async fn start_server(
    cert_b64: String,
    key_b64: String,
    port: u16,
) -> HandlerResult<web_service::ServerHandle> {
    let router = Box::new(BenchRouter::new());
    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
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

async fn run_benchmark(
    cert_b64: String,
    key_b64: String,
    host: String,
    config: BenchConfig,
) -> BenchResult<()> {
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let port = pick_unused_port()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "pick port"))?;
    let handle = start_server(cert_b64.clone(), key_b64, port).await?;
    handle.ready_rx.await?;
    wait_for_port(port).await;

    let result = async {
        let cert_pem = from_base64_raw(&cert_b64)?;
        let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem)?;
        // Force new connections to stress the accept/listener path.
        let client = reqwest::Client::builder()
            .add_root_certificate(reqwest_cert)
            .http1_only()
            .pool_max_idle_per_host(0)
            .build()?;

        let url = format!("https://{host}:{port}/");
        let total_requests = config.concurrency * config.requests_per_worker;
        println!(
            "h1.1 benchmark starting: concurrency={}, requests_per_worker={}, total_requests={}",
            config.concurrency, config.requests_per_worker, total_requests
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
                    let resp = client.get(&url).header(CONNECTION, "close").send().await?;
                    let status = resp.status();
                    let _ = resp.bytes().await?;
                    if status != StatusCode::OK {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("unexpected status {status}"),
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
            "h1.1 benchmark complete: completed={}, elapsed={:?}, req/s={:.2}, avg_ms={:.2}",
            completed, elapsed, rps, avg_ms
        );

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    drop(guard);

    result
}

fn main() -> BenchResult<()> {
    let config = BenchConfig::from_env_args();
    if !config.enabled {
        eprintln!(
            "skipping h1.1 benchmark; run with --benchmark (optional: --concurrency N, --requests N)"
        );
        return Ok(());
    }

    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h1.1 benchmark: missing TLS env");
            return Ok(());
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_benchmark(cert_b64, key_b64, host, config))?;
    Ok(())
}
