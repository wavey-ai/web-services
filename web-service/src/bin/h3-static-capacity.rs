use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use clap::{Parser, ValueEnum};
use futures_util::{future::BoxFuture, FutureExt, StreamExt};
use http::{Method, Request, StatusCode, Version};
use serde::Serialize;
use std::io::BufReader;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use web_service::{
    load_tls_base64_from_paths, H2H3Server, H3Backend, HandlerResponse, HandlerResult, Router,
    Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler, WebTransportHandler,
};

const DEFAULT_PCM_PART_BYTES: usize = 5_760;
const LATENCY_SAMPLE_INTERVAL: u64 = 16;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum Mode {
    #[default]
    SelfTest,
    Server,
    Client,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum ServerBackend {
    #[default]
    Quinn,
    #[cfg(feature = "h3-tokio-quiche")]
    TokioQuiche,
}

impl ServerBackend {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quinn => "quinn",
            #[cfg(feature = "h3-tokio-quiche")]
            Self::TokioQuiche => "tokio-quiche",
        }
    }
}

impl From<ServerBackend> for H3Backend {
    fn from(value: ServerBackend) -> Self {
        match value {
            ServerBackend::Quinn => Self::Quinn,
            #[cfg(feature = "h3-tokio-quiche")]
            ServerBackend::TokioQuiche => Self::TokioQuiche,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "h3-static-capacity",
    about = "Isolate production H2H3Server TLS/QUIC/H3 static-response capacity"
)]
struct Args {
    #[arg(long, value_enum, default_value_t)]
    mode: Mode,
    /// H3 implementation used by server and self-test modes; client mode records the expected backend.
    #[arg(long, value_enum, default_value_t)]
    server_backend: ServerBackend,
    /// Also serve HTTP/1.1 and HTTP/2 on the same port during server mode.
    #[arg(long)]
    enable_h2: bool,
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    #[arg(long)]
    tls_key: Option<PathBuf>,
    #[arg(long)]
    tls_ca: Option<PathBuf>,
    #[arg(long, default_value = "local.infidelity.io")]
    server_name: String,
    #[arg(long, default_value_t = DEFAULT_PCM_PART_BYTES)]
    response_bytes: usize,
    #[arg(long, default_value_t = 2.0)]
    duration_seconds: f64,
    #[arg(long, default_value = "1,2,4,8,16,24")]
    connection_steps: String,
    #[arg(long, default_value_t = 8)]
    pipeline_depth: usize,
    /// Fixed request rate on each connection; zero runs an unpaced saturation test.
    #[arg(long, default_value_t = 0)]
    requests_per_second_per_connection: u64,
    /// UDP port used by server mode. Self-test mode chooses an unused port.
    #[arg(long, default_value_t = 19_447)]
    port: u16,
    /// Remote H3 server address used by client mode.
    #[arg(long)]
    target: Option<SocketAddr>,
    /// Maximum server-only lifetime; Ctrl-C also shuts it down.
    #[arg(long, default_value_t = 300)]
    server_seconds: u64,
}

struct StaticRouter {
    payload: Bytes,
}

struct RunningServer {
    shutdown_tx: tokio::sync::watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
}

#[async_trait]
impl Router for StaticRouter {
    async fn route(&self, request: Request<()>) -> HandlerResult<HandlerResponse> {
        if request.method() != Method::GET || request.uri().path() != "/part.mp4" {
            return Ok(HandlerResponse {
                status: StatusCode::NOT_FOUND,
                ..HandlerResponse::default()
            });
        }
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(self.payload.clone()),
            content_type: Some("video/mp4".into()),
            ..HandlerResponse::default()
        })
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _request: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config(
            "static capacity route is not streaming".into(),
        ))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

struct H3Client {
    _endpoint: h3_quinn::quinn::Endpoint,
    connection: h3_quinn::quinn::Connection,
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    driver: tokio::task::JoinHandle<()>,
    authority: String,
}

impl H3Client {
    async fn connect(edge: SocketAddr, server_name: &str, tls_ca: &Path) -> Result<Self> {
        let crypto = tls_client_config(tls_ca)?;
        let client_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
            h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));
        let mut endpoint =
            h3_quinn::quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))?;
        endpoint.set_default_client_config(client_config);
        let connection = endpoint.connect(edge, server_name)?.await?;
        let handshake = connection
            .handshake_data()
            .context("H3 connection omitted TLS handshake data")?
            .downcast::<h3_quinn::quinn::crypto::rustls::HandshakeData>()
            .map_err(|_| anyhow::anyhow!("H3 connection returned unknown TLS handshake data"))?;
        if handshake.protocol.as_deref() != Some(b"h3") {
            bail!("connection did not negotiate H3 with TLS ALPN");
        }
        let (mut driver, send_request) =
            h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
        let driver = tokio::spawn(async move {
            let _ = driver.wait_idle().await;
        });
        Ok(Self {
            _endpoint: endpoint,
            connection,
            send_request,
            driver,
            authority: format!("{server_name}:{}", edge.port()),
        })
    }
}

impl Drop for H3Client {
    fn drop(&mut self) {
        self.connection
            .close(0_u32.into(), b"capacity step complete");
        self.driver.abort();
    }
}

#[derive(Default)]
struct ConnectionResult {
    requests: u64,
    response_bytes: u64,
    errors: u64,
    wire_bytes: u64,
    scheduling_backpressure: u64,
    latency_ns: Vec<u64>,
    first_errors: Vec<String>,
}

#[derive(Serialize)]
struct Percentiles {
    samples: usize,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Serialize)]
struct StepReport {
    connections: usize,
    customer_connection_pairs: f64,
    pipeline_depth_per_connection: usize,
    requested_rate_per_connection: Option<u64>,
    expected_requests: Option<u64>,
    completion_ratio: Option<f64>,
    nominal_rate_qualified: Option<bool>,
    strict_rate_qualified: Option<bool>,
    scheduling_backpressure: u64,
    duration_seconds: f64,
    requests: u64,
    errors: u64,
    requests_per_second: f64,
    customer_equivalents_at_400_part_requests_per_second: f64,
    payload_gbit_per_second: f64,
    client_observed_wire_gbit_per_second: f64,
    sampled_response_latency: Percentiles,
    first_errors: Vec<String>,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    generated_unix_ms: u128,
    boundary: String,
    target_os: &'static str,
    target_arch: &'static str,
    available_parallelism: usize,
    tls_protocol: &'static str,
    alpn: &'static str,
    server: &'static str,
    server_backend: &'static str,
    response_bytes: usize,
    connection_steps: Vec<usize>,
    generator_location: String,
    generator_note: String,
    steps: Vec<StepReport>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    match args.mode {
        Mode::SelfTest => run_self_test(&args).await,
        Mode::Server => run_server(&args).await,
        Mode::Client => run_client(&args).await,
    }
}

async fn run_self_test(args: &Args) -> Result<()> {
    let port = unused_udp_port()?;
    let edge = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let server_handle = start_server(args, port).await?;
    let tls_ca = args
        .tls_ca
        .as_deref()
        .or(args.tls_cert.as_deref())
        .context("self-test mode requires --tls-ca or --tls-cert")?;
    let report = run_capacity(
        args,
        edge,
        tls_ca,
        "B4_production_H2H3Server_static_Bytes_loopback",
        "same_process_loopback",
        "server and Rust generator share CPU; this is a development check, not a server-capacity qualification",
    )
    .await;
    let _ = server_handle.shutdown_tx.send(());
    let _ = server_handle.finished_rx.await;
    println!("{}", serde_json::to_string_pretty(&report?)?);
    Ok(())
}

async fn run_server(args: &Args) -> Result<()> {
    let server_handle = start_server(args, args.port).await?;
    println!(
        "{}",
        serde_json::json!({
            "schema": "needletail.web-service.h3-static-capacity-server.v2",
            "ready": true,
            "port": args.port,
            "response_bytes": args.response_bytes,
            "server": "web_service::H2H3Server",
            "server_backend": args.server_backend.as_str(),
            "h1_h2_enabled": args.enable_h2,
            "alpn": "h3"
        })
    );
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = tokio::time::sleep(Duration::from_secs(args.server_seconds)) => {}
    }
    let _ = server_handle.shutdown_tx.send(());
    let _ = server_handle.finished_rx.await;
    Ok(())
}

async fn run_client(args: &Args) -> Result<()> {
    let edge = args.target.context("client mode requires --target")?;
    let tls_ca = args
        .tls_ca
        .as_deref()
        .context("client mode requires --tls-ca")?;
    let report = run_capacity(
        args,
        edge,
        tls_ca,
        "B6_production_H2H3Server_static_Bytes_two_host",
        "separate_client_host",
        "server and pure-Rust load generator run on different hosts; observed latency includes their network RTT",
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn start_server(args: &Args, port: u16) -> Result<RunningServer> {
    let cert_path = args
        .tls_cert
        .as_deref()
        .context("server requires --tls-cert")?;
    let key_path = args
        .tls_key
        .as_deref()
        .context("server requires --tls-key")?;
    let (cert, key) = load_tls_base64_from_paths(cert_path, key_path)?;
    let payload = Bytes::from(vec![0x5a_u8; args.response_bytes]);
    let server = H2H3Server::builder()
        .with_tls(cert, key)
        .with_port(port)
        .enable_h2(args.enable_h2)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_h3_backend(args.server_backend.into())
        .with_router(Box::new(StaticRouter { payload }))
        .build()?;
    let web_service::ServerHandle {
        shutdown_tx,
        ready_rx,
        finished_rx,
    } = server.start().await?;
    ready_rx
        .await
        .context("H3 capacity server did not become ready")?;
    Ok(RunningServer {
        shutdown_tx,
        finished_rx,
    })
}

async fn run_capacity(
    args: &Args,
    edge: SocketAddr,
    tls_ca: &Path,
    boundary: &str,
    generator_location: &str,
    generator_note: &str,
) -> Result<Report> {
    let connection_steps = parse_connection_steps(&args.connection_steps)?;
    let duration = Duration::from_secs_f64(args.duration_seconds);
    let mut steps = Vec::new();
    for connections in connection_steps.iter().copied() {
        steps.push(
            run_step(
                edge,
                &args.server_name,
                tls_ca,
                connections,
                args.pipeline_depth,
                args.response_bytes,
                duration,
                args.requests_per_second_per_connection,
            )
            .await?,
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(Report {
        schema: "needletail.web-service.h3-static-capacity.v2",
        generated_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before Unix epoch")?
            .as_millis(),
        boundary: boundary.to_owned(),
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
        available_parallelism: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        tls_protocol: "TLSv1.3",
        alpn: "h3",
        server: "web_service::H2H3Server",
        server_backend: args.server_backend.as_str(),
        response_bytes: args.response_bytes,
        connection_steps,
        generator_location: generator_location.to_owned(),
        generator_note: generator_note.to_owned(),
        steps,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_step(
    edge: SocketAddr,
    server_name: &str,
    tls_ca: &Path,
    connections: usize,
    pipeline_depth: usize,
    response_bytes: usize,
    duration: Duration,
    requests_per_second_per_connection: u64,
) -> Result<StepReport> {
    let mut clients = Vec::with_capacity(connections);
    for _ in 0..connections {
        clients.push(H3Client::connect(edge, server_name, tls_ca).await?);
    }
    let start_at = Instant::now() + Duration::from_millis(100);
    let deadline = start_at + duration;
    let mut tasks = Vec::with_capacity(connections);
    for client in clients {
        tasks.push(tokio::spawn(run_connection(
            client,
            start_at,
            deadline,
            pipeline_depth,
            response_bytes,
            requests_per_second_per_connection,
        )));
    }

    let mut aggregate = ConnectionResult::default();
    for task in tasks {
        let result = task.await.context("H3 capacity connection task failed")?;
        aggregate.requests += result.requests;
        aggregate.response_bytes += result.response_bytes;
        aggregate.errors += result.errors;
        aggregate.wire_bytes += result.wire_bytes;
        aggregate.scheduling_backpressure += result.scheduling_backpressure;
        aggregate.latency_ns.extend(result.latency_ns);
        for error in result.first_errors {
            if aggregate.first_errors.len() < 20 {
                aggregate.first_errors.push(error);
            }
        }
    }
    let elapsed_seconds = start_at.elapsed().as_secs_f64();
    let requests_per_second = aggregate.requests as f64 / elapsed_seconds;
    let requested_rate_per_connection =
        (requests_per_second_per_connection > 0).then_some(requests_per_second_per_connection);
    let expected_requests = requested_rate_per_connection
        .map(|rate| (duration.as_secs_f64() * rate as f64 * connections as f64).round() as u64);
    let (completion_ratio, nominal_rate_qualified, strict_rate_qualified) = rate_qualification(
        expected_requests,
        aggregate.requests,
        aggregate.errors,
        aggregate.scheduling_backpressure,
    );
    Ok(StepReport {
        connections,
        customer_connection_pairs: connections as f64 / 2.0,
        pipeline_depth_per_connection: pipeline_depth,
        requested_rate_per_connection,
        expected_requests,
        completion_ratio,
        nominal_rate_qualified,
        strict_rate_qualified,
        scheduling_backpressure: aggregate.scheduling_backpressure,
        duration_seconds: elapsed_seconds,
        requests: aggregate.requests,
        errors: aggregate.errors,
        requests_per_second,
        customer_equivalents_at_400_part_requests_per_second: requests_per_second / 400.0,
        payload_gbit_per_second: aggregate.response_bytes as f64 * 8.0 / elapsed_seconds / 1e9,
        client_observed_wire_gbit_per_second: aggregate.wire_bytes as f64 * 8.0
            / elapsed_seconds
            / 1e9,
        sampled_response_latency: percentiles(aggregate.latency_ns),
        first_errors: aggregate.first_errors,
    })
}

fn rate_qualification(
    expected_requests: Option<u64>,
    completed_requests: u64,
    errors: u64,
    scheduling_backpressure: u64,
) -> (Option<f64>, Option<bool>, Option<bool>) {
    let expected_requests = expected_requests.filter(|expected| *expected > 0);
    let completion_ratio =
        expected_requests.map(|expected| completed_requests as f64 / expected as f64);
    let nominal =
        completion_ratio.map(|ratio| ratio >= 0.995 && errors == 0 && scheduling_backpressure == 0);
    let strict = expected_requests.map(|expected| {
        completed_requests == expected && errors == 0 && scheduling_backpressure == 0
    });
    (completion_ratio, nominal, strict)
}

async fn run_connection(
    client: H3Client,
    start_at: Instant,
    deadline: Instant,
    pipeline_depth: usize,
    response_bytes: usize,
    requests_per_second_per_connection: u64,
) -> ConnectionResult {
    tokio::time::sleep_until(tokio::time::Instant::from_std(start_at)).await;
    let before = client.connection.stats();
    let mut result = if requests_per_second_per_connection == 0 {
        run_unpaced_requests(&client, deadline, pipeline_depth, response_bytes).await
    } else {
        run_paced_requests(
            &client,
            deadline,
            pipeline_depth,
            response_bytes,
            requests_per_second_per_connection,
        )
        .await
    };
    let after = client.connection.stats();
    result.wire_bytes = after
        .udp_tx
        .bytes
        .saturating_sub(before.udp_tx.bytes)
        .saturating_add(after.udp_rx.bytes.saturating_sub(before.udp_rx.bytes));
    result
}

type RequestFuture = BoxFuture<'static, (Duration, Result<usize, String>)>;

async fn run_unpaced_requests(
    client: &H3Client,
    deadline: Instant,
    pipeline_depth: usize,
    response_bytes: usize,
) -> ConnectionResult {
    let mut in_flight = futures_util::stream::FuturesUnordered::<RequestFuture>::new();
    let mut scheduled = 0_u64;
    while in_flight.len() < pipeline_depth {
        in_flight.push(
            static_get(
                client.send_request.clone(),
                client.authority.clone(),
                response_bytes,
            )
            .boxed(),
        );
        scheduled += 1;
    }

    let mut result = ConnectionResult::default();
    while let Some((latency, response)) = in_flight.next().await {
        record_request_result(&mut result, latency, response);
        if Instant::now() < deadline {
            in_flight.push(
                static_get(
                    client.send_request.clone(),
                    client.authority.clone(),
                    response_bytes,
                )
                .boxed(),
            );
            scheduled += 1;
        }
    }
    debug_assert_eq!(scheduled, result.requests + result.errors);
    result
}

async fn run_paced_requests(
    client: &H3Client,
    deadline: Instant,
    pipeline_depth: usize,
    response_bytes: usize,
    requests_per_second: u64,
) -> ConnectionResult {
    let interval = Duration::from_secs_f64(1.0 / requests_per_second as f64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut in_flight = futures_util::stream::FuturesUnordered::<RequestFuture>::new();
    let mut result = ConnectionResult::default();
    loop {
        if Instant::now() >= deadline {
            break;
        }
        if in_flight.is_empty() {
            ticker.tick().await;
            if Instant::now() >= deadline {
                break;
            }
            schedule_paced_request(client, &mut in_flight, response_bytes);
            continue;
        }
        tokio::select! {
            _ = ticker.tick() => {
                if Instant::now() >= deadline {
                    break;
                } else if in_flight.len() < pipeline_depth {
                    schedule_paced_request(client, &mut in_flight, response_bytes);
                } else {
                    result.scheduling_backpressure = result.scheduling_backpressure.saturating_add(1);
                }
            }
            completed = in_flight.next() => {
                if let Some((latency, response)) = completed {
                    record_request_result(&mut result, latency, response);
                }
            }
        }
    }
    while let Some((latency, response)) = in_flight.next().await {
        record_request_result(&mut result, latency, response);
    }
    result
}

fn schedule_paced_request(
    client: &H3Client,
    in_flight: &mut futures_util::stream::FuturesUnordered<RequestFuture>,
    response_bytes: usize,
) {
    in_flight.push(
        static_get(
            client.send_request.clone(),
            client.authority.clone(),
            response_bytes,
        )
        .boxed(),
    );
}

fn record_request_result(
    result: &mut ConnectionResult,
    latency: Duration,
    response: Result<usize, String>,
) {
    match response {
        Ok(bytes) => {
            result.requests += 1;
            result.response_bytes += bytes as u64;
        }
        Err(error) => {
            result.errors += 1;
            if result.first_errors.len() < 4 {
                result.first_errors.push(error);
            }
        }
    }
    if (result.requests + result.errors).is_multiple_of(LATENCY_SAMPLE_INTERVAL) {
        result.latency_ns.push(
            latency
                .as_nanos()
                .min(u128::from(u64::MAX))
                .try_into()
                .unwrap_or(u64::MAX),
        );
    }
}

async fn static_get(
    mut send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    authority: String,
    response_bytes: usize,
) -> (Duration, Result<usize, String>) {
    let started = Instant::now();
    let result = async {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("https://{authority}/part.mp4"))
            .body(())
            .map_err(|error| error.to_string())?;
        let mut stream = send_request
            .send_request(request)
            .await
            .map_err(|error| error.to_string())?;
        stream.finish().await.map_err(|error| error.to_string())?;
        let response = stream
            .recv_response()
            .await
            .map_err(|error| error.to_string())?;
        if response.status() != StatusCode::OK || response.version() != Version::HTTP_3 {
            return Err(format!(
                "unexpected H3 response status={} version={:?}",
                response.status(),
                response.version()
            ));
        }
        let mut received = 0_usize;
        while let Some(mut chunk) = stream
            .recv_data()
            .await
            .map_err(|error| error.to_string())?
        {
            received = received.saturating_add(chunk.remaining());
            chunk.advance(chunk.remaining());
        }
        if received != response_bytes {
            return Err(format!(
                "response carried {received} bytes, expected {response_bytes}"
            ));
        }
        Ok(received)
    }
    .await;
    (started.elapsed(), result)
}

fn validate_args(args: &Args) -> Result<()> {
    if !args.duration_seconds.is_finite()
        || args.duration_seconds < 0.25
        || args.duration_seconds > 300.0
    {
        bail!("--duration-seconds must be between 0.25 and 300");
    }
    if args.response_bytes == 0 || args.response_bytes > 16 * 1024 * 1024 {
        bail!("--response-bytes must be between 1 and 16777216");
    }
    if args.pipeline_depth == 0 || args.pipeline_depth > 256 {
        bail!("--pipeline-depth must be between 1 and 256");
    }
    match args.mode {
        Mode::SelfTest if args.tls_cert.is_none() || args.tls_key.is_none() => {
            bail!("self-test mode requires --tls-cert and --tls-key");
        }
        Mode::Server if args.tls_cert.is_none() || args.tls_key.is_none() => {
            bail!("server mode requires --tls-cert and --tls-key");
        }
        Mode::Client if args.target.is_none() || args.tls_ca.is_none() => {
            bail!("client mode requires --target and --tls-ca");
        }
        _ => {}
    }
    if args.server_seconds == 0 || args.server_seconds > 86_400 {
        bail!("--server-seconds must be between 1 and 86400");
    }
    Ok(())
}

fn parse_connection_steps(value: &str) -> Result<Vec<usize>> {
    let steps = value
        .split(',')
        .map(|step| {
            step.parse::<usize>()
                .with_context(|| format!("invalid connection step: {step}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if steps.is_empty() || steps.iter().any(|step| *step == 0 || *step > 4_096) {
        bail!("--connection-steps must contain values between 1 and 4096");
    }
    Ok(steps)
}

fn unused_udp_port() -> Result<u16> {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    Ok(socket.local_addr()?.port())
}

fn tls_client_config(tls_ca: &Path) -> Result<rustls::ClientConfig> {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    let file = std::fs::File::open(tls_ca)
        .with_context(|| format!("failed to open TLS CA PEM: {}", tls_ca.display()))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse TLS CA PEM: {}", tls_ca.display()))?;
    if certs.is_empty() {
        bail!("TLS CA PEM contained no certificates: {}", tls_ca.display());
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert)?;
    }
    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    Ok(crypto)
}

fn percentiles(mut values_ns: Vec<u64>) -> Percentiles {
    values_ns.sort_unstable();
    let at = |percentile: usize| {
        if values_ns.is_empty() {
            return 0.0;
        }
        let rank = values_ns.len().saturating_mul(percentile).div_ceil(100);
        values_ns[rank.clamp(1, values_ns.len()) - 1] as f64 / 1_000_000.0
    };
    Percentiles {
        samples: values_ns.len(),
        p50_ms: at(50),
        p95_ms: at(95),
        p99_ms: at(99),
        max_ms: values_ns.last().copied().unwrap_or(0) as f64 / 1_000_000.0,
    }
}

#[cfg(test)]
mod tests {
    use super::rate_qualification;

    #[test]
    fn strict_qualification_requires_every_request_and_no_pressure() {
        let (_, nominal, strict) = rate_qualification(Some(20_000), 20_000, 0, 0);
        assert_eq!(nominal, Some(true));
        assert_eq!(strict, Some(true));

        let (_, nominal, strict) = rate_qualification(Some(20_000), 20_000, 0, 1);
        assert_eq!(nominal, Some(false));
        assert_eq!(strict, Some(false));

        let (_, nominal, strict) = rate_qualification(Some(20_000), 19_999, 0, 0);
        assert_eq!(nominal, Some(true));
        assert_eq!(strict, Some(false));
    }

    #[test]
    fn unpaced_run_has_no_rate_qualification() {
        assert_eq!(rate_qualification(None, 20_000, 0, 0), (None, None, None));
    }
}
