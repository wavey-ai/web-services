use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use rustls_native_certs;
use tls_helpers::{from_base64_raw, load_certs_from_base64};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::lookup_host;
use tokio_rustls::rustls::{self, pki_types::ServerName, ClientConfig, RootCertStore};
use tokio_tungstenite::tungstenite::Message;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, RawTcpHandler, RequestHandler, Router, Server,
    ServerBuilder, ServerError, WebSocketHandler,
};

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
            body: Some(Bytes::from_static(b"hello from test")),
            content_type: Some("text/plain".into()),
            ..Default::default()
        })
    }

    fn can_handle(&self, path: &str) -> bool {
        path == "/"
    }
}

struct EchoWsHandler;

#[async_trait]
impl WebSocketHandler for EchoWsHandler {
    async fn handle_websocket(
        &self,
        _req: Request<()>,
        mut stream: tokio_tungstenite::WebSocketStream<
            hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>,
        >,
    ) -> HandlerResult<()> {
        println!("ws handler started");
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Text(t)) => {
                    println!("ws received text: {t}");
                    stream
                        .send(Message::Text(format!("echo: {t}").into()))
                        .await
                        .map_err(|e| ServerError::Handler(Box::new(e)))?;
                }
                Ok(Message::Binary(b)) => {
                    println!("ws received binary {} bytes", b.len());
                    stream
                        .send(Message::Binary(b))
                        .await
                        .map_err(|e| ServerError::Handler(Box::new(e)))?;
                }
                Ok(Message::Ping(p)) => {
                    let _ = stream.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(frame)) => {
                    let _ = stream.close(frame).await;
                    break;
                }
                Ok(_) => {}
                Err(e) => return Err(ServerError::Handler(Box::new(e))),
            }
        }
        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        path == "/ws"
    }
}

struct TestRouter {
    http: HelloHandler,
    ws: EchoWsHandler,
}

impl TestRouter {
    fn new() -> Self {
        Self {
            http: HelloHandler,
            ws: EchoWsHandler,
        }
    }
}

#[async_trait]
impl Router for TestRouter {
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

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        if self.ws.can_handle(path) {
            Some(&self.ws)
        } else {
            None
        }
    }
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

async fn start_server(
    cert_b64: String,
    key_b64: String,
    port: u16,
) -> web_service::HandlerResult<web_service::ServerHandle> {
    let router = Box::new(TestRouter::new());
    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(port)
        .enable_h2(true)
        .enable_h3(true)
        .enable_websocket(true)
        .with_router(router)
        .build()?;

    server.start().await
}

fn load_env() -> Option<(String, String, String)> {
    dotenv().ok();
    let cert = std::env::var("TLS_CERT_BASE64").ok()?;
    let key = std::env::var("TLS_KEY_BASE64").ok()?;
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local.aldea.ai".into());
    Some((cert, key, host))
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

async fn run_with_server<F, Fut>(cert_b64: String, key_b64: String, host: String, test_fn: F)
where
    F: FnOnce(u16, String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let port = pick_unused_port().expect("pick port");
    let handle = start_server(cert_b64.clone(), key_b64.clone(), port)
        .await
        .expect("start server");
    handle.ready_rx.await.expect("server ready");

    wait_for_port(port).await;

    test_fn(port, host.clone()).await;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    drop(guard);
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    let len: u16 = data
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 2];
    reader.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

struct BidiRawHandler;

#[async_trait]
impl RawTcpHandler for BidiRawHandler {
    async fn handle_stream(
        &self,
        mut stream: Box<dyn web_service::traits::RawStream>,
        is_tls: bool,
    ) -> HandlerResult<()> {
        let greeting: &[u8] = if is_tls { b"hello-tls" } else { b"hello-plain" };
        write_frame(&mut stream, greeting).await?;
        let inbound = read_frame(&mut stream).await?;
        let response = format!(
            "echo-{}:{}",
            if is_tls { "tls" } else { "plain" },
            String::from_utf8_lossy(&inbound)
        );
        write_frame(&mut stream, response.as_bytes()).await?;
        Ok(())
    }
}

async fn start_server_with_raw(
    cert_b64: String,
    key_b64: String,
    http_port: u16,
    raw_port: u16,
    raw_tls: bool,
    handler: Box<dyn RawTcpHandler>,
) -> web_service::HandlerResult<web_service::ServerHandle> {
    let router = Box::new(TestRouter::new());
    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(http_port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_websocket(false)
        .enable_raw_tcp(true)
        .with_raw_tcp_port(raw_port)
        .with_raw_tcp_tls(raw_tls)
        .with_raw_tcp_handler(handler)
        .with_router(router)
        .build()?;

    server.start().await
}

async fn run_with_raw_server<F, Fut>(
    cert_b64: String,
    key_b64: String,
    host: String,
    raw_tls: bool,
    test_fn: F,
) where
    F: FnOnce(u16, String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let http_port = pick_unused_port().expect("pick http port");
    let mut raw_port = pick_unused_port().expect("pick raw port");
    while raw_port == http_port {
        raw_port = pick_unused_port().expect("pick raw port");
    }

    let handle = start_server_with_raw(
        cert_b64.clone(),
        key_b64.clone(),
        http_port,
        raw_port,
        raw_tls,
        Box::new(BidiRawHandler),
    )
    .await
    .expect("start server with raw tcp");
    handle.ready_rx.await.expect("server ready");
    wait_for_port(http_port).await;
    wait_for_port(raw_port).await;

    test_fn(raw_port, host.clone()).await;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn http1_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping http1: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host| async move {
            let cert_pem = from_base64_raw(&cert_b64).expect("decode cert");
            let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");
            let client = reqwest::Client::builder()
                .add_root_certificate(reqwest_cert)
                .http1_only()
                .build()
                .unwrap();
            let url = format!("https://{host}:{port}/");
            let resp = client.get(url).send().await.expect("http1 request");
            assert_eq!(resp.version(), reqwest::Version::HTTP_11);
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http2_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping http2: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host| async move {
            let cert_pem = from_base64_raw(&cert_b64).expect("decode cert");
            let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");
            let client = reqwest::Client::builder()
                .add_root_certificate(reqwest_cert)
                .build()
                .unwrap();
            let url = format!("https://{host}:{port}/");
            let resp = client.get(url).send().await.expect("http2 request");
            assert_eq!(resp.version(), reqwest::Version::HTTP_2);
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping ws: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host| async move {
            let ws_url = format!("wss://{host}:{port}/ws");
            let tcp = tokio::net::TcpStream::connect(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                port,
            ))
            .await
            .expect("connect ws tcp");
            let mut ws_cfg = tls_client_config(&std::env::var("TLS_CERT_BASE64").unwrap());
            ws_cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
            let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(ws_cfg));
            let server_name = ServerName::try_from(host.clone()).expect("sni");
            let tls_stream = tls_connector
                .connect(server_name, tcp)
                .await
                .expect("ws tls connect");
            let (mut ws, resp) = tokio_tungstenite::client_async(ws_url, tls_stream)
                .await
                .expect("ws handshake");
            assert_eq!(resp.status(), http::StatusCode::SWITCHING_PROTOCOLS);
            ws.send(Message::Text("hi".into())).await.expect("ws send");
            match ws.next().await {
                Some(Ok(frame)) => {
                    assert_eq!(frame.into_text().expect("ws text"), "echo: hi");
                }
                Some(Err(e)) => {
                    if !e.to_string().contains("close_notify") {
                        panic!("ws recv error: {e}");
                    }
                }
                None => eprintln!("ws closed without echo"),
            }
            let _ = ws.close(None).await;
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http3_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h3: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host| async move {
            let mut h3_client_cfg = tls_client_config(&std::env::var("TLS_CERT_BASE64").unwrap());
            h3_client_cfg.alpn_protocols = vec![b"h3".to_vec()];
            let quic_crypto =
                quinn::crypto::rustls::QuicClientConfig::try_from(h3_client_cfg).expect("quic cfg");
            let quic_cfg = quinn::ClientConfig::new(Arc::new(quic_crypto));
            let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))
                .expect("create client endpoint");
            endpoint.set_default_client_config(quic_cfg);
            let target_ip = lookup_host((host.as_str(), port))
                .await
                .ok()
                .and_then(|mut iter| iter.next().map(|sa| sa.ip()))
                .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
            let conn = endpoint
                .connect(SocketAddr::new(target_ip, port), &host)
                .expect("connect h3")
                .await
                .expect("h3 handshake");
            let (_h3_conn, mut sender): (
                h3::client::Connection<h3_quinn::Connection, Bytes>,
                h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
            ) = h3::client::builder()
                .build(h3_quinn::Connection::new(conn))
                .await
                .expect("h3 build");
            let uri = format!("https://{host}:{port}/");
            let mut req_stream = sender
                .send_request(Request::get(uri).body(()).unwrap())
                .await
                .expect("h3 request");
            let response = req_stream.recv_response().await.expect("h3 response");
            assert_eq!(response.status(), StatusCode::OK);
            let mut body = Vec::new();
            while let Some(mut chunk) = req_stream.recv_data().await.expect("recv data") {
                let data = chunk.copy_to_bytes(chunk.remaining());
                body.extend_from_slice(&data);
            }
            assert_eq!(body, b"hello from test");
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_tcp_plain_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping raw tcp plain: missing TLS env");
            return;
        }
    };

    run_with_raw_server(
        cert_b64,
        key_b64,
        host,
        false,
        |raw_port, _host| async move {
            let mut stream = tokio::net::TcpStream::connect(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                raw_port,
            ))
            .await
            .expect("connect raw tcp");
            let greeting = read_frame(&mut stream).await.expect("read greeting");
            assert_eq!(greeting, b"hello-plain");
            write_frame(&mut stream, b"ping").await.expect("write ping");
            let response = read_frame(&mut stream).await.expect("read echo");
            assert_eq!(response, b"echo-plain:ping");
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_tcp_tls_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping raw tcp tls: missing TLS env");
            return;
        }
    };
    let client_cert = cert_b64.clone();

    run_with_raw_server(
        cert_b64,
        key_b64,
        host.clone(),
        true,
        move |raw_port, host| {
            let client_cert = client_cert.clone();
            async move {
                let tcp = tokio::net::TcpStream::connect(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    raw_port,
                ))
                .await
                .expect("connect raw tls tcp");
                let mut cfg = tls_client_config(&client_cert);
                cfg.alpn_protocols.clear();
                let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg));
                let server_name = ServerName::try_from(host.clone()).expect("sni");
                let mut tls = connector
                    .connect(server_name, tcp)
                    .await
                    .expect("tls connect");

                let greeting = read_frame(&mut tls).await.expect("read greeting");
                assert_eq!(greeting, b"hello-tls");
                write_frame(&mut tls, b"ping-tls")
                    .await
                    .expect("write ping");
                let response = read_frame(&mut tls).await.expect("read echo");
                assert_eq!(response, b"echo-tls:ping-tls");
            }
        },
    )
    .await;
}
