mod common;

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, OnceLock},
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode, Version};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use portpicker::pick_unused_port;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tls_helpers::{from_base64_raw, load_certs_from_base64};
use web_service::{LoadBalancingMode, ProxyConfig, ProxyIngress, UpstreamProtocol};
use common::load_test_env;

struct BackendHandle {
    port: u16,
    version_rx: oneshot::Receiver<Version>,
    task: tokio::task::JoinHandle<()>,
}

static SERVER_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn tls_client_config(cert_pem_b64: &str) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    if let Ok(native) = rustls_native_certs::load_native_certs() {
        for cert in native {
            let _ = roots.add(cert);
        }
    }
    if let Ok(certs) = load_certs_from_base64(cert_pem_b64) {
        for cert in certs {
            let _ = roots.add(cert);
        }
    }

    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
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

async fn start_backend() -> io::Result<BackendHandle> {
    let port = pick_unused_port().expect("pick backend port");
    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
    let listener = TcpListener::bind(addr).await?;
    let (version_tx, version_rx) = oneshot::channel();
    let version_tx = Arc::new(tokio::sync::Mutex::new(Some(version_tx)));

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

async fn run_with_proxy<F, Fut>(
    cert_b64: String,
    key_b64: String,
    host: String,
    enable_h2: bool,
    client_fn: F,
) -> Version
where
    F: FnOnce(u16, String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let backend = start_backend().await.expect("start backend");
    let mut proxy_port = pick_unused_port().expect("pick proxy port");
    while proxy_port == backend.port {
        proxy_port = pick_unused_port().expect("pick proxy port");
    }

    let config = ProxyConfig {
        cert_pem_base64: cert_b64,
        key_pem_base64: key_b64,
        port: proxy_port,
        enable_h2,
        enable_websocket: false,
        initial_mode: LoadBalancingMode::LeastConn,
        upstream_protocol: UpstreamProtocol::Http1,
        max_queue: 1,
        max_backends: 1,
        queue_request_kb: 5 * 1024,
        queue_slot_kb: 200,
        quic_relay: None,
    };
    let ingress = ProxyIngress::from_config(config).expect("proxy config");
    let state = ingress.state();
    let backend_url = url::Url::parse(&format!("http://127.0.0.1:{}", backend.port))
        .expect("backend url");
    state
        .add_backend(backend_url, None)
        .await
        .expect("add backend");

    let handle = ingress.start().await.expect("start proxy");
    handle.ready_rx.await.expect("proxy ready");
    if enable_h2 {
        wait_for_port(proxy_port).await;
    }

    client_fn(proxy_port, host).await;

    let version = tokio::time::timeout(Duration::from_secs(2), backend.version_rx)
        .await
        .expect("backend version timeout")
        .expect("backend version recv");

    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    backend.task.abort();

    drop(guard);

    version
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_api_adds_backend_during_live_usage() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping proxy api add backend: missing TLS env");
            return;
        }
    };

    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let backend = start_backend().await.expect("start backend");
    let mut proxy_port = pick_unused_port().expect("pick proxy port");
    while proxy_port == backend.port {
        proxy_port = pick_unused_port().expect("pick proxy port");
    }

    let config = ProxyConfig {
        cert_pem_base64: cert_b64.clone(),
        key_pem_base64: key_b64,
        port: proxy_port,
        enable_h2: true,
        enable_websocket: false,
        initial_mode: LoadBalancingMode::LeastConn,
        upstream_protocol: UpstreamProtocol::Http1,
        max_queue: 1,
        max_backends: 1,
        queue_request_kb: 5 * 1024,
        queue_slot_kb: 200,
        quic_relay: None,
    };
    let ingress = ProxyIngress::from_config(config).expect("proxy config");
    let handle = ingress.start().await.expect("start proxy");
    handle.ready_rx.await.expect("proxy ready");
    wait_for_port(proxy_port).await;

    let cert_pem = from_base64_raw(&cert_b64).expect("decode cert");
    let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .http1_only()
        .build()
        .expect("client build");

    let base_url = format!("https://{host}:{proxy_port}/");
    let response = client
        .get(&base_url)
        .send()
        .await
        .expect("proxy request without backend");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let backend_url = format!("http://127.0.0.1:{}", backend.port);
    let api_url = format!("https://{host}:{proxy_port}/api/backends");
    let response = client
        .post(&api_url)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&serde_json::json!({
            "url": backend_url,
            "max_connections": null
        }))
        .expect("serialize backend"))
        .send()
        .await
        .expect("api add backend");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = client
        .get(&base_url)
        .send()
        .await
        .expect("proxy request after add");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), b"backend ok");

    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    backend.task.abort();
    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_http1_ingress_upstreams_http1() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping proxy http1: missing TLS env");
            return;
        }
    };
    let client_cert = cert_b64.clone();

    let version = run_with_proxy(
        cert_b64,
        key_b64,
        host.clone(),
        true,
        move |port, host| {
            let client_cert = client_cert.clone();
            async move {
                let cert_pem = from_base64_raw(&client_cert).expect("decode cert");
                let reqwest_cert =
                    reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");
                let client = reqwest::Client::builder()
                    .add_root_certificate(reqwest_cert)
                    .http1_only()
                    .build()
                    .unwrap();
                let url = format!("https://{host}:{port}/");
                let resp = client.get(url).send().await.expect("http1 request");
                assert_eq!(resp.version(), reqwest::Version::HTTP_11);
                let body = resp.bytes().await.expect("read body");
                assert_eq!(body.as_ref(), b"backend ok");
            }
        },
    )
    .await;

    assert_eq!(version, Version::HTTP_11);
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_http2_ingress_upstreams_http1() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping proxy http2: missing TLS env");
            return;
        }
    };
    let client_cert = cert_b64.clone();

    let version = run_with_proxy(
        cert_b64,
        key_b64,
        host.clone(),
        true,
        move |port, host| {
            let client_cert = client_cert.clone();
            async move {
                let cert_pem = from_base64_raw(&client_cert).expect("decode cert");
                let reqwest_cert =
                    reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");
                let client = reqwest::Client::builder()
                    .add_root_certificate(reqwest_cert)
                    .build()
                    .unwrap();
                let url = format!("https://{host}:{port}/");
                let resp = client.get(url).send().await.expect("http2 request");
                assert_eq!(resp.version(), reqwest::Version::HTTP_2);
                let body = resp.bytes().await.expect("read body");
                assert_eq!(body.as_ref(), b"backend ok");
            }
        },
    )
    .await;

    assert_eq!(version, Version::HTTP_11);
}
