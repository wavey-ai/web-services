mod common;

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, OnceLock},
    time::Duration,
};

use bytes::Bytes;
use common::load_test_env;
use http::{Request, Response, StatusCode, Version};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use portpicker::pick_unused_port;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_rustls::rustls;
use web_service::{LoadBalancingMode, ProxyConfig, ProxyIngress, ProxyState, UpstreamProtocol};

struct BackendHandle {
    port: u16,
    version_rx: oneshot::Receiver<Version>,
    task: tokio::task::JoinHandle<()>,
}

struct ProxyHandle {
    port: u16,
    shutdown_tx: tokio::sync::watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
    state: Arc<ProxyState>,
}

static SERVER_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

async fn wait_for_port(port: u16) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
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
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn start_backend(
    _protocol: UpstreamProtocol,
    _cert_b64: &str,
    _key_b64: &str,
) -> io::Result<BackendHandle> {
    let port = pick_unused_port().expect("pick backend port");
    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
    let listener = TcpListener::bind(addr).await?;
    let (version_tx, version_rx) = oneshot::channel();
    let version_tx = Arc::new(Mutex::new(Some(version_tx)));

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(err) => {
                    eprintln!("backend accept error: {err}");
                    break;
                }
            };
            let version_tx = Arc::clone(&version_tx);
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let version_tx = Arc::clone(&version_tx);
                    let version = req.version();
                    async move {
                        if let Some(tx) = version_tx.lock().await.take() {
                            let _ = tx.send(version);
                        }
                        Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from_static(b"backend ok")))
                                .expect("build backend response"),
                        )
                    }
                });
                if let Err(err) = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    eprintln!("backend serve error: {err}");
                }
            });
        }
    });

    Ok(BackendHandle {
        port,
        version_rx,
        task,
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
        enable_websocket: false,
        initial_mode: LoadBalancingMode::LeastConn,
        upstream_protocol,
        max_queue: 1,
        max_backends: 1,
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

async fn run_proxy_test(protocol: UpstreamProtocol, expected: Version) {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping proxy upstream test: missing TLS env");
            return;
        }
    };

    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let backend = start_backend(protocol, &cert_b64, &key_b64)
        .await
        .expect("start backend");

    let proxy = start_proxy(&cert_b64, &key_b64, protocol)
        .await
        .expect("start proxy");

    let backend_url =
        url::Url::parse(&format!("http://127.0.0.1:{}", backend.port)).expect("backend url");
    proxy
        .state
        .add_backend(backend_url, None)
        .await
        .expect("add backend");

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .build()
        .expect("client build");
    let url = format!("https://{host}:{}/", proxy.port);
    let resp = client.get(url).send().await.expect("proxy request");
    assert_eq!(resp.version(), reqwest::Version::HTTP_11);
    let body = resp.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), b"backend ok");

    let version = tokio::time::timeout(Duration::from_secs(2), backend.version_rx)
        .await
        .expect("backend version timeout")
        .expect("backend version recv");
    assert_eq!(version, expected);

    let _ = proxy.shutdown_tx.send(());
    let _ = proxy.finished_rx.await;
    backend.task.abort();
    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_http1_ingress_upstreams_http1() {
    // Proxy always uses HTTP/1.1 for upstream connections regardless of setting
    run_proxy_test(UpstreamProtocol::Http1, Version::HTTP_11).await;
}

// HTTP/2 upstream tests removed - proxy now always uses HTTP/1.1 for upstream
// regardless of the UpstreamProtocol setting. The proxy still accepts H1/H2 on ingress.
