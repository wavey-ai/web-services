//! Streaming load generator for wsb-server.
//!
//! Modes:
//! - `closed`: a closed loop. Each of `--conc` virtual clients sends a request, reads
//!   the full response, and sends the next request. The client discards results for
//!   `--warmup` seconds and then measures for `--measure` seconds.
//! - `stall`: slow readers. The client warms up with `--warm-path`, then opens `--conc`
//!   streams at the same time. Each stream reads the response head and the first data
//!   frame, then stops reading for `--stall` seconds. Then all streams read to the end.
//!
//! When `--server-pid` is set, the client reads the server CPU time, context switches
//! and RSS from /proc at the edges of the measurement window, samples VmRSS every
//! 50 ms, and resets VmHWM (clear_refs 5) at the start of the window.

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, StreamBody};
use hyper::body::Frame;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::json;
use tokio::{net::TcpSocket, sync::Semaphore};

type ReqBody = BoxBody<Bytes, Infallible>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Proto {
    H1,
    H2,
    H3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Closed,
    Stall,
}

#[derive(Clone, Debug)]
struct Cfg {
    addr: SocketAddr,
    proto: Proto,
    mode: Mode,
    conc: usize,
    per_conn: usize,
    path: String,
    warm_path: String,
    post_bytes: usize,
    post_chunk: usize,
    expect_bytes: u64,
    warmup: Duration,
    measure: Duration,
    stall: Duration,
    grace: Duration,
    server_pid: Option<u32>,
    threads: usize,
    connect_parallel: usize,
    rcvbuf: Option<u32>,
    read_delay: Option<Duration>,
    send_first: bool,
    stream_timeout: Option<Duration>,
    h2_stream_window: u32,
    h2_conn_window: u32,
    label: String,
    out: Option<String>,
}

fn parse_args() -> Cfg {
    let mut cfg = Cfg {
        addr: "127.0.0.1:8443".parse().unwrap(),
        proto: Proto::H2,
        mode: Mode::Closed,
        conc: 64,
        per_conn: 64,
        path: "/stream?chunks=4&size=16384".into(),
        warm_path: "/stream?chunks=4&size=16384".into(),
        post_bytes: 0,
        post_chunk: 64 * 1024,
        expect_bytes: 0,
        warmup: Duration::from_secs(5),
        measure: Duration::from_secs(10),
        stall: Duration::from_secs(5),
        grace: Duration::from_secs(2),
        server_pid: None,
        threads: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
        connect_parallel: 64,
        rcvbuf: None,
        read_delay: None,
        send_first: false,
        stream_timeout: None,
        h2_stream_window: 2 * 1024 * 1024,
        h2_conn_window: 16 * 1024 * 1024,
        label: String::new(),
        out: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        let secs = |v: &str| Duration::from_secs_f64(v.parse::<f64>().expect("seconds"));
        match arg.as_str() {
            "--addr" => cfg.addr = v.parse().expect("--addr"),
            "--proto" => {
                cfg.proto = match v.as_str() {
                    "h1" => Proto::H1,
                    "h2" => Proto::H2,
                    "h3" => Proto::H3,
                    _ => panic!("--proto h1|h2|h3"),
                }
            }
            "--mode" => {
                cfg.mode = match v.as_str() {
                    "closed" => Mode::Closed,
                    "stall" => Mode::Stall,
                    _ => panic!("--mode closed|stall"),
                }
            }
            "--conc" => cfg.conc = v.parse().expect("--conc"),
            "--per-conn" => cfg.per_conn = v.parse().expect("--per-conn"),
            "--path" => cfg.path = v,
            "--warm-path" => cfg.warm_path = v,
            "--post-bytes" => cfg.post_bytes = v.parse().expect("--post-bytes"),
            "--post-chunk" => cfg.post_chunk = v.parse().expect("--post-chunk"),
            "--expect-bytes" => cfg.expect_bytes = v.parse().expect("--expect-bytes"),
            "--warmup" => cfg.warmup = secs(&v),
            "--measure" => cfg.measure = secs(&v),
            "--stall" => cfg.stall = secs(&v),
            "--grace" => cfg.grace = secs(&v),
            "--server-pid" => cfg.server_pid = Some(v.parse().expect("--server-pid")),
            "--threads" => cfg.threads = v.parse().expect("--threads"),
            "--connect-parallel" => cfg.connect_parallel = v.parse().expect("--connect-parallel"),
            "--rcvbuf" => cfg.rcvbuf = Some(v.parse().expect("--rcvbuf")),
            "--send-first" => cfg.send_first = v != "0",
            "--stream-timeout" => cfg.stream_timeout = Some(secs(&v)),
            "--read-delay-ms" => {
                cfg.read_delay = Some(Duration::from_millis(v.parse().expect("--read-delay-ms")))
            }
            "--h2-stream-window" => cfg.h2_stream_window = v.parse().expect("--h2-stream-window"),
            "--h2-conn-window" => cfg.h2_conn_window = v.parse().expect("--h2-conn-window"),
            "--label" => cfg.label = v,
            "--out" => cfg.out = Some(v),
            other => panic!("unknown argument {other}"),
        }
    }
    if cfg.proto == Proto::H1 {
        cfg.per_conn = 1;
    }
    cfg.per_conn = cfg.per_conn.max(1);
    cfg
}

// ---------------------------------------------------------------- TLS

#[derive(Debug)]
struct AcceptAnyCert(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn tls_config(alpn: &[u8]) -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
        .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    Arc::new(config)
}

// ---------------------------------------------------------------- connections

#[derive(Clone)]
enum Shared {
    H2(hyper::client::conn::http2::SendRequest<ReqBody>),
    H3(h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>),
}

enum Handle {
    H1(hyper::client::conn::http1::SendRequest<ReqBody>),
    Shared(Shared),
}

struct Connector {
    cfg: Arc<Cfg>,
    tls: Arc<rustls::ClientConfig>,
    quic: Option<quinn::ClientConfig>,
}

impl Connector {
    fn new(cfg: Arc<Cfg>) -> Self {
        let (tls, quic) = match cfg.proto {
            Proto::H1 => (tls_config(b"http/1.1"), None),
            Proto::H2 => (tls_config(b"h2"), None),
            Proto::H3 => {
                let tls = tls_config(b"h3");
                let quic = quinn::ClientConfig::new(Arc::new(
                    quinn::crypto::rustls::QuicClientConfig::try_from(Arc::clone(&tls))
                        .expect("quic client config"),
                ));
                (tls, Some(quic))
            }
        };
        Self { cfg, tls, quic }
    }

    async fn tls_stream(
        &self,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, BoxError> {
        let socket = TcpSocket::new_v4()?;
        if let Some(rcvbuf) = self.cfg.rcvbuf {
            socket.set_recv_buffer_size(rcvbuf)?;
        }
        let tcp = socket.connect(self.cfg.addr).await?;
        tcp.set_nodelay(true)?;
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls));
        let name = rustls::pki_types::ServerName::try_from("localhost")?.to_owned();
        Ok(connector.connect(name, tcp).await?)
    }

    /// Returns the handle, and for HTTP/3 the endpoint that owns the UDP socket.
    async fn connect(&self) -> Result<(Handle, Option<quinn::Endpoint>), BoxError> {
        match self.cfg.proto {
            Proto::H1 => {
                let io = TokioIo::new(self.tls_stream().await?);
                let (send, conn) = hyper::client::conn::http1::handshake(io).await?;
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                Ok((Handle::H1(send), None))
            }
            Proto::H2 => {
                let io = TokioIo::new(self.tls_stream().await?);
                let (send, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                    .initial_stream_window_size(self.cfg.h2_stream_window)
                    .initial_connection_window_size(self.cfg.h2_conn_window)
                    .handshake(io)
                    .await?;
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                Ok((Handle::Shared(Shared::H2(send)), None))
            }
            Proto::H3 => {
                let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap())?;
                endpoint.set_default_client_config(self.quic.clone().unwrap());
                let conn = endpoint.connect(self.cfg.addr, "localhost")?.await?;
                let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(conn)).await?;
                tokio::spawn(async move {
                    let _ = futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await;
                });
                Ok((Handle::Shared(Shared::H3(send)), Some(endpoint)))
            }
        }
    }
}

/// One HTTP/2 or HTTP/3 connection shared by `per_conn` virtual clients.
struct Group {
    state: tokio::sync::Mutex<GroupState>,
}

struct GroupState {
    generation: u64,
    shared: Option<Shared>,
    _endpoint: Option<quinn::Endpoint>,
}

impl Group {
    /// Replaces the connection unless another client of this group did it already.
    async fn reconnect(&self, connector: &Connector, seen: u64) -> Option<(u64, Shared)> {
        let mut state = self.state.lock().await;
        if state.generation == seen {
            match connector.connect().await {
                Ok((Handle::Shared(shared), endpoint)) => {
                    state.generation += 1;
                    state.shared = Some(shared);
                    state._endpoint = endpoint;
                }
                _ => return None,
            }
        }
        state.shared.clone().map(|s| (state.generation, s))
    }
}

// ---------------------------------------------------------------- per-stream

#[derive(Default)]
#[repr(align(64))]
struct WorkerCounters {
    bytes: AtomicU64,
}

#[derive(Default)]
struct WorkerResults {
    /// (ttfb_us, ttlb_us) for streams that completed inside the window.
    samples: Vec<(u32, u32)>,
    completed: u64,
    err_status: u64,
    err_short: u64,
    err_request: u64,
    err_body: u64,
    err_timeout: u64,
    err_connect: u64,
    total_errors_any_phase: u64,
    first_error: Option<String>,
}

enum StreamError {
    Timeout,
    Status(u16),
    Short(u64),
    Request(String),
    Body(String),
}

struct StreamOk {
    ttfb: Duration,
    ttlb: Duration,
}

/// Gate between the first data frame and the rest of the body, for stall mode.
struct StallGate {
    heads: AtomicUsize,
    release: tokio::sync::watch::Receiver<bool>,
}

/// The request body, and a receiver that fires once hyper has taken the last frame.
fn request_body(
    cfg: &Cfg,
    template: &Bytes,
) -> (ReqBody, Option<tokio::sync::oneshot::Receiver<()>>) {
    if cfg.post_bytes == 0 {
        return (Empty::<Bytes>::new().boxed(), None);
    }
    let chunk = cfg.post_chunk.max(1);
    let total = cfg.post_bytes;
    let template = template.clone();
    let frames = (0..total.div_ceil(chunk)).map(move |i| {
        let start = i * chunk;
        let len = chunk.min(total - start);
        let offset = (i * 4099) % (template.len() - len + 1);
        Ok::<_, Infallible>(Frame::data(template.slice(offset..offset + len)))
    });
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
    use futures_util::StreamExt as _;
    let done = futures_util::stream::once(async move {
        let _ = sent_tx.send(());
    })
    .filter_map(|()| async { None::<Result<Frame<Bytes>, Infallible>> });
    let frames = futures_util::stream::iter(frames).chain(done);
    let body = BodyExt::boxed(StreamBody::new(frames));
    (body, Some(sent_rx))
}

async fn run_stream(
    handle: &mut Handle,
    cfg: &Cfg,
    path: &str,
    expect: u64,
    template: &Bytes,
    counter: &AtomicU64,
    gate: Option<&StallGate>,
) -> Result<StreamOk, StreamError> {
    let stream = run_stream_inner(handle, cfg, path, expect, template, counter, gate);
    match cfg.stream_timeout {
        Some(limit) => tokio::time::timeout(limit, stream)
            .await
            .unwrap_or(Err(StreamError::Timeout)),
        None => stream.await,
    }
}

async fn run_stream_inner(
    handle: &mut Handle,
    cfg: &Cfg,
    path: &str,
    expect: u64,
    template: &Bytes,
    counter: &AtomicU64,
    gate: Option<&StallGate>,
) -> Result<StreamOk, StreamError> {
    let started = Instant::now();
    let method = if cfg.post_bytes > 0 { "POST" } else { "GET" };
    let mut first: Option<Instant> = None;
    let mut received = 0u64;
    let mut gate_passed = gate.is_none();

    async fn pass_gate(gate: &StallGate) {
        gate.heads.fetch_add(1, Ordering::AcqRel);
        let mut release = gate.release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                break;
            }
        }
    }

    match handle {
        Handle::H1(_) | Handle::Shared(Shared::H2(_)) => {
            let uri = match handle {
                Handle::H1(_) => path.to_string(),
                _ => format!("https://localhost:{}{}", cfg.addr.port(), path),
            };
            let req = http::Request::builder()
                .method(method)
                .uri(uri)
                .header(http::header::HOST, "localhost")
                .body(())
                .unwrap();
            let (body, body_sent) = request_body(cfg, template);
            let req = req.map(|()| body);
            let response = match handle {
                Handle::H1(send) => {
                    send.ready()
                        .await
                        .map_err(|e| StreamError::Request(chain(&e)))?;
                    send.send_request(req).await
                }
                Handle::Shared(Shared::H2(send)) => {
                    send.ready()
                        .await
                        .map_err(|e| StreamError::Request(chain(&e)))?;
                    send.send_request(req).await
                }
                _ => unreachable!(),
            }
            .map_err(|e| StreamError::Request(chain(&e)))?;
            if response.status() != http::StatusCode::OK {
                return Err(StreamError::Status(response.status().as_u16()));
            }
            // One stream in 16, to keep the shared vector off the hot path.
            if DIAG_ON.load(Ordering::Relaxed) && DIAG_N.fetch_add(1, Ordering::Relaxed) % 16 == 0 {
                let us = started.elapsed().as_micros() as u32;
                DIAG_HEAD.lock().unwrap().push(us);
            }
            // A send-first client reads no response body until its request body is sent.
            if cfg.send_first {
                if let Some(sent) = body_sent {
                    let _ = sent.await;
                }
            }
            let mut body = response.into_body();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| StreamError::Body(chain(&e)))?;
                if let Some(data) = frame.data_ref() {
                    if data.is_empty() {
                        continue;
                    }
                    first.get_or_insert_with(Instant::now);
                    received += data.len() as u64;
                    counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                    if !gate_passed {
                        pass_gate(gate.unwrap()).await;
                        gate_passed = true;
                    }
                    if let Some(delay) = cfg.read_delay {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        Handle::Shared(Shared::H3(send)) => {
            let req = http::Request::builder()
                .method(method)
                .uri(format!("https://localhost:{}{}", cfg.addr.port(), path))
                .body(())
                .unwrap();
            let mut stream = send
                .send_request(req)
                .await
                .map_err(|e| StreamError::Request(chain(&e)))?;
            if cfg.post_bytes > 0 {
                let chunk = cfg.post_chunk.max(1);
                let mut sent = 0;
                while sent < cfg.post_bytes {
                    let len = chunk.min(cfg.post_bytes - sent);
                    stream
                        .send_data(template.slice(0..len))
                        .await
                        .map_err(|e| StreamError::Request(chain(&e)))?;
                    sent += len;
                }
            }
            stream
                .finish()
                .await
                .map_err(|e| StreamError::Request(chain(&e)))?;
            let response = stream
                .recv_response()
                .await
                .map_err(|e| StreamError::Request(chain(&e)))?;
            if response.status() != http::StatusCode::OK {
                return Err(StreamError::Status(response.status().as_u16()));
            }
            while let Some(mut chunk) = stream
                .recv_data()
                .await
                .map_err(|e| StreamError::Body(chain(&e)))?
            {
                let n = chunk.remaining();
                chunk.advance(n);
                if n == 0 {
                    continue;
                }
                first.get_or_insert_with(Instant::now);
                received += n as u64;
                counter.fetch_add(n as u64, Ordering::Relaxed);
                if !gate_passed {
                    pass_gate(gate.unwrap()).await;
                    gate_passed = true;
                }
                if let Some(delay) = cfg.read_delay {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    let ended = Instant::now();
    if expect > 0 && received != expect {
        return Err(StreamError::Short(received));
    }
    Ok(StreamOk {
        ttfb: first.unwrap_or(ended) - started,
        ttlb: ended - started,
    })
}

/// The error and its sources, joined.
fn chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        text.push_str(": ");
        text.push_str(&s.to_string());
        source = s.source();
    }
    text
}

fn handle_is_closed(handle: &Handle) -> bool {
    match handle {
        Handle::H1(send) => send.is_closed(),
        Handle::Shared(Shared::H2(send)) => send.is_closed(),
        // h3 exposes no closed flag; a failed send_request is treated as closed.
        Handle::Shared(Shared::H3(_)) => false,
    }
}

// ---------------------------------------------------------------- /proc

#[derive(Clone, Copy, Default, Debug)]
struct ProcSnap {
    cpu_ticks: u64,
    ctx_vol: u64,
    ctx_invol: u64,
    rss_kb: u64,
    hwm_kb: u64,
    threads: u64,
}

fn clk_tck() -> f64 {
    100.0
}

fn proc_snap(pid: &str) -> ProcSnap {
    let mut snap = ProcSnap::default();
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        if let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) {
            let fields: Vec<&str> = rest.split_whitespace().collect();
            let utime: u64 = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
            let stime: u64 = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
            snap.cpu_ticks = utime + stime;
        }
    }
    if let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
        for line in status.lines() {
            let mut parts = line.split_whitespace();
            let key = parts.next().unwrap_or("");
            let value: u64 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            match key {
                "VmRSS:" => snap.rss_kb = value,
                "VmHWM:" => snap.hwm_kb = value,
                "Threads:" => snap.threads = value,
                _ => {}
            }
        }
    }
    if let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) {
        for task in tasks.flatten() {
            if let Ok(status) = std::fs::read_to_string(task.path().join("status")) {
                for line in status.lines() {
                    if let Some(v) = line.strip_prefix("voluntary_ctxt_switches:") {
                        snap.ctx_vol += v.trim().parse::<u64>().unwrap_or(0);
                    } else if let Some(v) = line.strip_prefix("nonvoluntary_ctxt_switches:") {
                        snap.ctx_invol += v.trim().parse::<u64>().unwrap_or(0);
                    }
                }
            }
        }
    }
    snap
}

fn reset_hwm(pid: &str) -> bool {
    std::fs::write(format!("/proc/{pid}/clear_refs"), b"5").is_ok()
}

fn rss_kb(pid: &str) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

/// Samples server VmRSS every 50 ms while `active` is set.
struct RssSampler {
    active: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<(f64, u64)>>>,
}

impl RssSampler {
    fn start(pid: Option<String>, origin: Instant) -> Self {
        let active = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        if let Some(pid) = pid {
            let (active, stop, samples) = (active.clone(), stop.clone(), samples.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if active.load(Ordering::Relaxed) {
                        let rss = rss_kb(&pid);
                        samples
                            .lock()
                            .unwrap()
                            .push((origin.elapsed().as_secs_f64(), rss));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            });
        }
        Self {
            active,
            stop,
            samples,
        }
    }
}

// ---------------------------------------------------------------- statistics

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1] as f64 / 1000.0
}

fn latency_json(values: &mut [u32]) -> serde_json::Value {
    values.sort_unstable();
    let mean = if values.is_empty() {
        f64::NAN
    } else {
        values.iter().map(|&v| v as f64).sum::<f64>() / values.len() as f64 / 1000.0
    };
    json!({
        "n": values.len(),
        "mean_ms": mean,
        "p50_ms": percentile(values, 50.0),
        "p99_ms": percentile(values, 99.0),
        "p999_ms": percentile(values, 99.9),
        "max_ms": values.last().map(|&v| v as f64 / 1000.0).unwrap_or(f64::NAN),
    })
}

fn template() -> Bytes {
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut data = Vec::with_capacity(4 << 20);
    while data.len() < 4 << 20 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        data.extend_from_slice(&state.to_le_bytes());
    }
    Bytes::from(data)
}

// ---------------------------------------------------------------- main

fn main() {
    let cfg = parse_args();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let result = runtime.block_on(run(Arc::new(cfg)));
    let mut heads = DIAG_HEAD.lock().unwrap().clone();
    heads.sort_unstable();
    if !heads.is_empty() {
        let p = |q: f64| heads[((q * heads.len() as f64) as usize).min(heads.len() - 1)] as f64 / 1000.0;
        eprintln!("time to response head (ms): n={} p50={:.2} p90={:.2} p99={:.2}", heads.len(), p(0.5), p(0.9), p(0.99));
    }
    let text = serde_json::to_string(&result).unwrap();
    println!("{text}");
    // Do not wait for streams cut at the end of the window.
    runtime.shutdown_timeout(Duration::from_millis(200));
    std::process::exit(0);
}

struct Worker {
    counters: Arc<WorkerCounters>,
    results: Arc<Mutex<WorkerResults>>,
}

async fn connect_all(
    cfg: &Arc<Cfg>,
    connector: &Arc<Connector>,
) -> Result<(Vec<Handle>, Vec<Arc<Group>>, Vec<usize>), BoxError> {
    let n_conns = cfg.conc.div_ceil(cfg.per_conn);
    let sem = Arc::new(Semaphore::new(cfg.connect_parallel.max(1)));
    let mut tasks = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let sem = sem.clone();
        let connector = connector.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let mut last = None;
            for attempt in 0..5 {
                match connector.connect().await {
                    Ok(c) => return Ok(c),
                    Err(e) => {
                        last = Some(e.to_string());
                        tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                    }
                }
            }
            Err(last.unwrap_or_default())
        }));
    }
    let mut handles = Vec::with_capacity(cfg.conc);
    let mut groups = Vec::new();
    let mut group_of = Vec::with_capacity(cfg.conc);
    for (i, task) in tasks.into_iter().enumerate() {
        let (handle, endpoint) = task.await?.map_err(|e| format!("connect failed: {e}"))?;
        let members = cfg.per_conn.min(cfg.conc - i * cfg.per_conn);
        match handle {
            Handle::H1(send) => {
                handles.push(Handle::H1(send));
                group_of.push(usize::MAX);
            }
            Handle::Shared(shared) => {
                for _ in 0..members {
                    handles.push(Handle::Shared(shared.clone()));
                    group_of.push(groups.len());
                }
                groups.push(Arc::new(Group {
                    state: tokio::sync::Mutex::new(GroupState {
                        generation: 0,
                        shared: Some(shared),
                        _endpoint: endpoint,
                    }),
                }));
            }
        }
    }
    Ok((handles, groups, group_of))
}

/// Records a failed stream and replaces the connection if it is closed.
async fn on_error(
    err: StreamError,
    in_window: bool,
    handle: &mut Handle,
    generation: &mut u64,
    group: Option<&Arc<Group>>,
    connector: &Connector,
    results: &Mutex<WorkerResults>,
) {
    let (text, request_level) = match &err {
        StreamError::Status(s) => (format!("status {s}"), false),
        StreamError::Short(n) => (format!("short body {n} bytes"), false),
        StreamError::Request(e) => (format!("request: {e}"), true),
        StreamError::Body(e) => (format!("body: {e}"), false),
        // A timed-out HTTP/1.1 stream leaves its connection mid-response.
        StreamError::Timeout => ("stream timeout".to_string(), true),
    };
    {
        let mut r = results.lock().unwrap();
        r.total_errors_any_phase += 1;
        if r.first_error.is_none() {
            r.first_error = Some(text);
        }
        if in_window {
            match err {
                StreamError::Status(_) => r.err_status += 1,
                StreamError::Short(_) => r.err_short += 1,
                StreamError::Request(_) => r.err_request += 1,
                StreamError::Body(_) => r.err_body += 1,
                StreamError::Timeout => r.err_timeout += 1,
            }
        }
    }
    let closed = handle_is_closed(handle) || (request_level && matches!(handle, Handle::Shared(Shared::H3(_))));
    let h1_needs_new = matches!(handle, Handle::H1(_)) && (closed || request_level);
    if !(closed || h1_needs_new) {
        return;
    }
    let ok = match (handle_is_h1(handle), group) {
        (true, _) => match connector.connect().await {
            Ok((h, _)) => {
                *handle = h;
                true
            }
            Err(_) => false,
        },
        (false, Some(group)) => match group.reconnect(connector, *generation).await {
            Some((g, shared)) => {
                *generation = g;
                *handle = Handle::Shared(shared);
                true
            }
            None => false,
        },
        _ => false,
    };
    if !ok {
        if in_window {
            results.lock().unwrap().err_connect += 1;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn handle_is_h1(handle: &Handle) -> bool {
    matches!(handle, Handle::H1(_))
}

async fn run(cfg: Arc<Cfg>) -> serde_json::Value {
    let origin = Instant::now();
    let connector = Arc::new(Connector::new(cfg.clone()));
    let template = template();
    let pid = cfg.server_pid.map(|p| p.to_string());
    let idle_snap = pid.as_deref().map(proc_snap).unwrap_or_default();

    let (handles, groups, group_of) = match connect_all(&cfg, &connector).await {
        Ok(v) => v,
        Err(e) => return json!({"label": cfg.label, "fatal": e.to_string()}),
    };
    let connect_secs = origin.elapsed().as_secs_f64();
    let after_connect_snap = pid.as_deref().map(proc_snap).unwrap_or_default();

    let sampler = RssSampler::start(pid.clone(), origin);
    let result = match cfg.mode {
        Mode::Closed => {
            run_closed(&cfg, &connector, handles, groups, group_of, &template, pid.as_deref(), &sampler).await
        }
        Mode::Stall => {
            run_stall(&cfg, &connector, handles, groups, group_of, &template, pid.as_deref(), &sampler).await
        }
    };
    sampler.stop.store(true, Ordering::Relaxed);

    let mut result = result;
    let obj = result.as_object_mut().unwrap();
    obj.insert("label".into(), json!(cfg.label));
    obj.insert(
        "config".into(),
        json!({
            "proto": format!("{:?}", cfg.proto).to_lowercase(),
            "mode": format!("{:?}", cfg.mode).to_lowercase(),
            "conc": cfg.conc,
            "per_conn": cfg.per_conn,
            "connections": cfg.conc.div_ceil(cfg.per_conn),
            "path": cfg.path,
            "warm_path": cfg.warm_path,
            "post_bytes": cfg.post_bytes,
            "post_chunk": cfg.post_chunk,
            "expect_bytes": cfg.expect_bytes,
            "warmup_s": cfg.warmup.as_secs_f64(),
            "measure_s": cfg.measure.as_secs_f64(),
            "stall_s": cfg.stall.as_secs_f64(),
            "client_threads": cfg.threads,
            "rcvbuf": cfg.rcvbuf,
            "read_delay_ms": cfg.read_delay.map(|d| d.as_millis() as u64),
            "send_first": cfg.send_first,
            "stream_timeout_s": cfg.stream_timeout.map(|d| d.as_secs_f64()),
            "h2_stream_window": cfg.h2_stream_window,
            "h2_conn_window": cfg.h2_conn_window,
        }),
    );
    obj.insert("connect_s".into(), json!(connect_secs));
    obj.insert("server_rss_idle_kb".into(), json!(idle_snap.rss_kb));
    obj.insert("server_rss_after_connect_kb".into(), json!(after_connect_snap.rss_kb));
    obj.insert("server_threads".into(), json!(after_connect_snap.threads));
    result
}

fn self_cpu_ticks() -> u64 {
    proc_snap("self").cpu_ticks
}

#[allow(clippy::too_many_arguments)]
async fn run_closed(
    cfg: &Arc<Cfg>,
    connector: &Arc<Connector>,
    handles: Vec<Handle>,
    groups: Vec<Arc<Group>>,
    group_of: Vec<usize>,
    template: &Bytes,
    pid: Option<&str>,
    sampler: &RssSampler,
) -> serde_json::Value {
    let start = tokio::time::Instant::now();
    let win_start = start + cfg.warmup;
    let win_end = win_start + cfg.measure;
    let (w_start, w_end) = (win_start.into_std(), win_end.into_std());
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::with_capacity(handles.len());
    let mut joins = Vec::with_capacity(handles.len());
    for (i, mut handle) in handles.into_iter().enumerate() {
        let counters = Arc::new(WorkerCounters::default());
        let results = Arc::new(Mutex::new(WorkerResults::default()));
        workers.push(Worker {
            counters: counters.clone(),
            results: results.clone(),
        });
        let group = groups.get(group_of[i]).cloned();
        let (cfg, connector, template, stop) =
            (cfg.clone(), connector.clone(), template.clone(), stop.clone());
        joins.push(tokio::spawn(async move {
            let mut generation = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let outcome = run_stream(
                    &mut handle,
                    &cfg,
                    &cfg.path,
                    cfg.expect_bytes,
                    &template,
                    &counters.bytes,
                    None,
                )
                .await;
                let now = Instant::now();
                let in_window = now >= w_start && now <= w_end;
                match outcome {
                    Ok(ok) => {
                        if in_window {
                            let mut r = results.lock().unwrap();
                            r.completed += 1;
                            r.samples.push((
                                ok.ttfb.as_micros().min(u32::MAX as u128) as u32,
                                ok.ttlb.as_micros().min(u32::MAX as u128) as u32,
                            ));
                        }
                    }
                    Err(err) => {
                        on_error(
                            err,
                            in_window,
                            &mut handle,
                            &mut generation,
                            group.as_ref(),
                            &connector,
                            &results,
                        )
                        .await
                    }
                }
            }
        }));
    }

    let bytes_sum = |workers: &[Worker]| -> u64 {
        workers
            .iter()
            .map(|w| w.counters.bytes.load(Ordering::Relaxed))
            .sum()
    };

    tokio::time::sleep_until(win_start).await;
    DIAG_ON.store(true, Ordering::Relaxed);
    let hwm_reset = pid.map(reset_hwm).unwrap_or(false);
    let s0 = pid.map(proc_snap).unwrap_or_default();
    let c0 = self_cpu_ticks();
    let b0 = bytes_sum(&workers);
    let t0 = Instant::now();
    sampler.active.store(true, Ordering::Relaxed);

    tokio::time::sleep_until(win_end).await;
    DIAG_ON.store(false, Ordering::Relaxed);
    let b1 = bytes_sum(&workers);
    let t1 = Instant::now();
    let s1 = pid.map(proc_snap).unwrap_or_default();
    let c1 = self_cpu_ticks();
    sampler.active.store(false, Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    tokio::time::sleep(cfg.grace).await;
    for j in &joins {
        j.abort();
    }

    let window = (t1 - t0).as_secs_f64();
    let mut ttfb = Vec::new();
    let mut ttlb = Vec::new();
    let mut agg = WorkerResults::default();
    let mut first_errors: Vec<String> = Vec::new();
    for w in &workers {
        let r = w.results.lock().unwrap();
        for &(a, b) in &r.samples {
            ttfb.push(a);
            ttlb.push(b);
        }
        agg.completed += r.completed;
        agg.err_status += r.err_status;
        agg.err_short += r.err_short;
        agg.err_request += r.err_request;
        agg.err_body += r.err_body;
        agg.err_timeout += r.err_timeout;
        agg.err_connect += r.err_connect;
        agg.total_errors_any_phase += r.total_errors_any_phase;
        if let Some(e) = &r.first_error {
            if first_errors.len() < 5 && !first_errors.contains(e) {
                first_errors.push(e.clone());
            }
        }
    }
    let samples = sampler.samples.lock().unwrap().clone();
    let rss_max = samples.iter().map(|s| s.1).max().unwrap_or(0);
    let rss_mean = if samples.is_empty() {
        0.0
    } else {
        samples.iter().map(|s| s.1 as f64).sum::<f64>() / samples.len() as f64
    };
    let server_cpu = (s1.cpu_ticks.saturating_sub(s0.cpu_ticks)) as f64 / clk_tck();
    let client_cpu = (c1.saturating_sub(c0)) as f64 / clk_tck();
    let completed = agg.completed.max(1) as f64;
    json!({
        "window_s": window,
        "streams": agg.completed,
        "streams_per_s": agg.completed as f64 / window,
        "bytes": b1 - b0,
        "bytes_per_s": (b1 - b0) as f64 / window,
        "gib_per_s": (b1 - b0) as f64 / window / (1u64 << 30) as f64,
        "ttfb": latency_json(&mut ttfb),
        "ttlb": latency_json(&mut ttlb),
        "errors": {
            "status": agg.err_status,
            "short": agg.err_short,
            "request": agg.err_request,
            "body": agg.err_body,
            "timeout": agg.err_timeout,
            "connect": agg.err_connect,
            "total_in_window": agg.err_status + agg.err_short + agg.err_request + agg.err_body + agg.err_connect + agg.err_timeout,
            "total_any_phase": agg.total_errors_any_phase,
            "first": first_errors,
        },
        "server": {
            "cpu_s": server_cpu,
            "cpu_util_cores": server_cpu / window,
            "cpu_us_per_stream": server_cpu * 1e6 / completed,
            "ctx_switches_vol": s1.ctx_vol.saturating_sub(s0.ctx_vol),
            "ctx_switches_invol": s1.ctx_invol.saturating_sub(s0.ctx_invol),
            "ctx_switches_per_stream": (s1.ctx_vol + s1.ctx_invol).saturating_sub(s0.ctx_vol + s0.ctx_invol) as f64 / completed,
            "rss_window_start_kb": s0.rss_kb,
            "rss_window_end_kb": s1.rss_kb,
            "rss_sampled_max_kb": rss_max,
            "rss_sampled_mean_kb": rss_mean,
            "hwm_window_kb": s1.hwm_kb,
            "hwm_reset_ok": hwm_reset,
        },
        "client": {
            "cpu_s": client_cpu,
            "cpu_util_cores": client_cpu / window,
            "cpu_util_frac": client_cpu / window / cfg.threads as f64,
        },
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_stall(
    cfg: &Arc<Cfg>,
    connector: &Arc<Connector>,
    handles: Vec<Handle>,
    groups: Vec<Arc<Group>>,
    group_of: Vec<usize>,
    template: &Bytes,
    pid: Option<&str>,
    sampler: &RssSampler,
) -> serde_json::Value {
    let n = handles.len();
    // Warm-up: a closed loop on the warm path over the same connections.
    let warm_stop = Arc::new(AtomicBool::new(false));
    let warm_count = Arc::new(AtomicU64::new(0));
    let mut warm_joins = Vec::with_capacity(n);
    for (i, mut handle) in handles.into_iter().enumerate() {
        let group = groups.get(group_of[i]).cloned();
        let (cfg, connector, template, stop, count) = (
            cfg.clone(),
            connector.clone(),
            template.clone(),
            warm_stop.clone(),
            warm_count.clone(),
        );
        warm_joins.push(tokio::spawn(async move {
            let counter = AtomicU64::new(0);
            let results = Mutex::new(WorkerResults::default());
            let mut generation = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match run_stream(&mut handle, &cfg, &cfg.warm_path, 0, &template, &counter, None)
                    .await
                {
                    Ok(_) => {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        on_error(err, false, &mut handle, &mut generation, group.as_ref(), &connector, &results)
                            .await
                    }
                }
            }
            (handle, generation)
        }));
    }
    tokio::time::sleep(cfg.warmup).await;
    warm_stop.store(true, Ordering::Relaxed);
    let mut handles = Vec::with_capacity(n);
    for j in warm_joins {
        handles.push(j.await.expect("warm worker"));
    }
    // Let the server free what the warm-up used.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (release_tx, release_rx) = tokio::sync::watch::channel(false);
    let gate = Arc::new(StallGate {
        heads: AtomicUsize::new(0),
        release: release_rx,
    });
    let baseline = pid.map(proc_snap).unwrap_or_default();
    let hwm_reset = pid.map(reset_hwm).unwrap_or(false);
    let c0 = self_cpu_ticks();
    sampler.active.store(true, Ordering::Relaxed);
    let t0 = Instant::now();

    let counters: Vec<Arc<WorkerCounters>> = (0..n).map(|_| Arc::new(WorkerCounters::default())).collect();
    let results: Vec<Arc<Mutex<WorkerResults>>> =
        (0..n).map(|_| Arc::new(Mutex::new(WorkerResults::default()))).collect();
    let mut joins = Vec::with_capacity(n);
    for (i, (mut handle, mut generation)) in handles.into_iter().enumerate() {
        let group = groups.get(group_of[i]).cloned();
        let (cfg, connector, template, gate, counter, result) = (
            cfg.clone(),
            connector.clone(),
            template.clone(),
            gate.clone(),
            counters[i].clone(),
            results[i].clone(),
        );
        joins.push(tokio::spawn(async move {
            let outcome = run_stream(
                &mut handle,
                &cfg,
                &cfg.path,
                cfg.expect_bytes,
                &template,
                &counter.bytes,
                Some(&gate),
            )
            .await;
            match outcome {
                Ok(ok) => {
                    let mut r = result.lock().unwrap();
                    r.completed += 1;
                    r.samples.push((
                        ok.ttfb.as_micros().min(u32::MAX as u128) as u32,
                        ok.ttlb.as_micros().min(u32::MAX as u128) as u32,
                    ));
                }
                Err(err) => {
                    // Count the stream past the gate so the coordinator does not wait for it.
                    gate.heads.fetch_add(1, Ordering::AcqRel);
                    on_error(err, true, &mut handle, &mut generation, group.as_ref(), &connector, &result).await
                }
            }
        }));
    }

    // Wait until every stream has its first data frame (bounded).
    let heads_deadline = Instant::now() + Duration::from_secs(30);
    while gate.heads.load(Ordering::Acquire) < n && Instant::now() < heads_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let heads_s = t0.elapsed().as_secs_f64();
    let heads = gate.heads.load(Ordering::Acquire);
    tokio::time::sleep(cfg.stall).await;
    let plateau = pid.map(proc_snap).unwrap_or_default();
    // RSS one second before the end of the stall, to show whether it is still growing.
    let samples_now = sampler.samples.lock().unwrap().clone();
    let last_sample_t = origin_offset(&samples_now);
    let rss_1s_before = samples_now
        .iter()
        .rev()
        .find(|s| s.0 <= last_sample_t - 1.0)
        .map(|s| s.1)
        .unwrap_or(0);

    let t_release = Instant::now();
    let _ = release_tx.send(true);
    let drain_deadline = Instant::now() + Duration::from_secs(300);
    for j in joins.iter_mut() {
        let left = drain_deadline.saturating_duration_since(Instant::now());
        if tokio::time::timeout(left, j).await.is_err() {
            break;
        }
    }
    let drain_s = t_release.elapsed().as_secs_f64();
    let end = pid.map(proc_snap).unwrap_or_default();
    let c1 = self_cpu_ticks();
    sampler.active.store(false, Ordering::Relaxed);
    let total_s = t0.elapsed().as_secs_f64();

    let mut ttfb = Vec::new();
    let mut ttlb = Vec::new();
    let mut completed = 0;
    let (mut e_status, mut e_short, mut e_req, mut e_body, mut e_conn, mut e_timeout) = (0, 0, 0, 0, 0, 0);
    let mut first_errors: Vec<String> = Vec::new();
    for r in &results {
        let r = r.lock().unwrap();
        for &(a, b) in &r.samples {
            ttfb.push(a);
            ttlb.push(b);
        }
        completed += r.completed;
        e_status += r.err_status;
        e_short += r.err_short;
        e_req += r.err_request;
        e_body += r.err_body;
        e_timeout += r.err_timeout;
        e_conn += r.err_connect;
        if let Some(e) = &r.first_error {
            if first_errors.len() < 5 && !first_errors.contains(e) {
                first_errors.push(e.clone());
            }
        }
    }
    let bytes: u64 = counters.iter().map(|c| c.bytes.load(Ordering::Relaxed)).sum();
    let samples = sampler.samples.lock().unwrap().clone();
    let rss_max = samples.iter().map(|s| s.1).max().unwrap_or(0);
    let server_cpu = end.cpu_ticks.saturating_sub(baseline.cpu_ticks) as f64 / clk_tck();
    let client_cpu = c1.saturating_sub(c0) as f64 / clk_tck();
    let per = |kb: u64| (kb.saturating_sub(baseline.rss_kb)) as f64 / n.max(1) as f64;
    let series: Vec<serde_json::Value> = samples
        .iter()
        .step_by(4)
        .map(|s| json!([(s.0 * 1000.0).round() / 1000.0, s.1]))
        .collect();
    json!({
        "streams": completed,
        "heads_received": heads,
        "heads_s": heads_s,
        "drain_s": drain_s,
        "total_s": total_s,
        "bytes": bytes,
        "drain_gib_per_s": bytes as f64 / drain_s / (1u64 << 30) as f64,
        "ttfb": latency_json(&mut ttfb),
        "ttlb": latency_json(&mut ttlb),
        "errors": {
            "status": e_status, "short": e_short, "request": e_req, "body": e_body, "connect": e_conn, "timeout": e_timeout,
            "total_in_window": e_status + e_short + e_req + e_body + e_conn + e_timeout,
            "first": first_errors,
        },
        "server": {
            "cpu_s": server_cpu,
            "cpu_us_per_stream": server_cpu * 1e6 / completed.max(1) as f64,
            "ctx_switches_per_stream": (end.ctx_vol + end.ctx_invol).saturating_sub(baseline.ctx_vol + baseline.ctx_invol) as f64 / completed.max(1) as f64,
            "rss_baseline_kb": baseline.rss_kb,
            "rss_plateau_kb": plateau.rss_kb,
            "rss_1s_before_plateau_kb": rss_1s_before,
            "rss_sampled_max_kb": rss_max,
            "hwm_window_kb": end.hwm_kb,
            "hwm_reset_ok": hwm_reset,
            "plateau_kb_per_stream": per(plateau.rss_kb),
            "peak_kb_per_stream": per(end.hwm_kb.max(rss_max)),
            "rss_series": series,
        },
        "client": {
            "cpu_s": client_cpu,
        },
    })
}

fn origin_offset(samples: &[(f64, u64)]) -> f64 {
    samples.last().map(|s| s.0).unwrap_or(0.0)
}

static DIAG_ON: AtomicBool = AtomicBool::new(false);
static DIAG_HEAD: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static DIAG_N: AtomicU64 = AtomicU64::new(0);
