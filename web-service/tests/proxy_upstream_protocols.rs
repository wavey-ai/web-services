mod common;

use std::{
    io,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode, Version};
use portpicker::pick_unused_port;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_rustls::rustls;
use web_service::{
    LoadBalancingMode, ProxyConfig, ProxyIngress, ProxyState, Server, ServerBuilder,
    UpstreamProtocol, H2H3Server, HandlerResponse, HandlerResult, Router, ServerError,
    WebSocketHandler, WebTransportHandler,
};
use common::load_test_env;

struct VersionRouter {
    version_tx: Arc<Mutex<Option<oneshot::Sender<Version>>>>,
}

impl VersionRouter {
    fn new(version_tx: oneshot::Sender<Version>) -> Self {
        Self {
            version_tx: Arc::new(Mutex::new(Some(version_tx))),
        }
    }
}

#[async_trait]
impl Router for VersionRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if let Some(tx) = self.version_tx.lock().await.take() {
            let _ = tx.send(req.version());
        }
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(Bytes::from_static(b"backend ok")),
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
    version_rx: oneshot::Receiver<Version>,
    shutdown_tx: tokio::sync::watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
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
    protocol: UpstreamProtocol,
    cert_b64: &str,
    key_b64: &str,
) -> io::Result<BackendHandle> {
    let port = pick_unused_port().expect("pick backend port");
    let (version_tx, version_rx) = oneshot::channel();
    let router = Box::new(VersionRouter::new(version_tx));
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
        version_rx,
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

    let backend_url = url::Url::parse(&format!("https://{host}:{}", backend.port))
        .expect("backend url");
    proxy
        .state
        .add_backend(backend_url, None)
        .await
        .expect("add backend");

    let cert_pem = tls_helpers::from_base64_raw(&cert_b64).expect("decode cert");
    let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
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
    let _ = backend.shutdown_tx.send(());
    let _ = backend.finished_rx.await;
    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_http1_ingress_upstreams_http1() {
    run_proxy_test(UpstreamProtocol::Http1, Version::HTTP_11).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_http1_ingress_upstreams_http2() {
    run_proxy_test(UpstreamProtocol::Http2, Version::HTTP_2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_http1_ingress_upstreams_http3() {
    run_proxy_test(UpstreamProtocol::Http3, Version::HTTP_3).await;
}
