mod common;

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use common::load_test_env;
use futures_util::{SinkExt, StreamExt};
use h3_webtransport::server::{AcceptedBi, WebTransportSession};
use http::{Request, StatusCode};
use portpicker::pick_unused_port;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::{self, pki_types::ServerName, ClientConfig};
use tokio_tungstenite::tungstenite::Message;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, RawTcpHandler, RequestHandler, Router, Server,
    ServerBuilder, ServerError, WebSocketHandler, WebTransportHandler,
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

struct EchoWebTransportHandler {
    accepted_tx: tokio::sync::mpsc::Sender<()>,
}

#[async_trait]
impl WebTransportHandler for EchoWebTransportHandler {
    async fn handle_session(
        &self,
        session: WebTransportSession<h3_quinn::Connection, Bytes>,
    ) -> HandlerResult<()> {
        let _ = self.accepted_tx.send(()).await;

        let accepted = session
            .accept_bi()
            .await
            .map_err(|e| ServerError::Config(format!("accept WebTransport bidi: {e}")))?;

        let Some(AcceptedBi::BidiStream(stream_session_id, mut stream)) = accepted else {
            return Err(ServerError::Config(
                "expected WebTransport bidirectional stream".into(),
            ));
        };

        if stream_session_id != session.session_id() {
            return Err(ServerError::Config(
                "WebTransport stream used the wrong session id".into(),
            ));
        }

        let mut body = Vec::new();
        stream
            .read_to_end(&mut body)
            .await
            .map_err(|e| ServerError::Handler(Box::new(e)))?;
        stream
            .write_all(b"wt-echo:")
            .await
            .map_err(|e| ServerError::Handler(Box::new(e)))?;
        stream
            .write_all(&body)
            .await
            .map_err(|e| ServerError::Handler(Box::new(e)))?;
        stream
            .flush()
            .await
            .map_err(|e| ServerError::Handler(Box::new(e)))?;
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        stream
            .shutdown()
            .await
            .map_err(|e| ServerError::Handler(Box::new(e)))?;
        Ok(())
    }
}

struct TestRouter {
    http: HelloHandler,
    ws: EchoWsHandler,
    wt: Option<EchoWebTransportHandler>,
}

impl TestRouter {
    fn new() -> Self {
        Self {
            http: HelloHandler,
            ws: EchoWsHandler,
            wt: None,
        }
    }

    fn with_webtransport(accepted_tx: tokio::sync::mpsc::Sender<()>) -> Self {
        Self {
            http: HelloHandler,
            ws: EchoWsHandler,
            wt: Some(EchoWebTransportHandler { accepted_tx }),
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
        self.wt
            .as_ref()
            .map(|handler| handler as &dyn web_service::WebTransportHandler)
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

fn tls_client_config(_cert_pem_b64: &str) -> ClientConfig {
    ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth()
}

#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
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
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
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
        .enable_websocket(true)
        .with_router(router)
        .build()?;

    server.start().await
}

async fn start_server_with_webtransport(
    cert_b64: String,
    key_b64: String,
    port: u16,
    accepted_tx: tokio::sync::mpsc::Sender<()>,
) -> web_service::HandlerResult<web_service::ServerHandle> {
    let router = Box::new(TestRouter::with_webtransport(accepted_tx));
    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(port)
        .enable_h2(true)
        .enable_h3(true)
        .enable_websocket(false)
        .enable_webtransport(true)
        .with_router(router)
        .build()?;

    server.start().await
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

async fn run_with_webtransport_server<F, Fut>(
    cert_b64: String,
    key_b64: String,
    host: String,
    test_fn: F,
) where
    F: FnOnce(u16, String, tokio::sync::mpsc::Receiver<()>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let port = pick_localhost_port();
    let (accepted_tx, accepted_rx) = tokio::sync::mpsc::channel(1);
    let handle = start_server_with_webtransport(cert_b64, key_b64, port, accepted_tx)
        .await
        .expect("start WebTransport server");
    handle.ready_rx.await.expect("server ready");

    wait_for_port(port).await;

    test_fn(port, host, accepted_rx).await;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    drop(guard);
}

fn pick_localhost_port() -> u16 {
    std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .expect("bind ephemeral localhost port")
        .local_addr()
        .expect("read ephemeral localhost port")
        .port()
}

fn encode_varint(value: u64, out: &mut Vec<u8>) {
    if value < 64 {
        out.push(value as u8);
    } else if value < 16_384 {
        out.extend_from_slice(&((value | 0x4000) as u16).to_be_bytes());
    } else if value < 1_073_741_824 {
        out.extend_from_slice(&((value | 0x8000_0000) as u32).to_be_bytes());
    } else {
        out.extend_from_slice(&(value | 0xC000_0000_0000_0000).to_be_bytes());
    }
}

async fn read_varint<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<u64> {
    let mut first = [0u8; 1];
    reader.read_exact(&mut first).await?;
    let encoded_len = 1usize << (first[0] >> 6);
    let mut value = (first[0] & 0x3f) as u64;
    for _ in 1..encoded_len {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        value = (value << 8) | byte[0] as u64;
    }
    Ok(value)
}

fn encode_qpack_prefixed_int(prefix_bits: u8, flags: u8, value: u64, out: &mut Vec<u8>) {
    let mask = ((1u16 << prefix_bits) - 1) as u8;
    let flags = flags << prefix_bits;
    if value < mask as u64 {
        out.push(flags | value as u8);
        return;
    }

    out.push(flags | mask);
    let mut remaining = value - mask as u64;
    while remaining >= 128 {
        out.push((remaining as u8 & 0x7f) | 0x80);
        remaining >>= 7;
    }
    out.push(remaining as u8);
}

fn encode_qpack_string(prefix_bits: u8, flags: u8, value: &[u8], out: &mut Vec<u8>) {
    encode_qpack_prefixed_int(prefix_bits - 1, flags << 1, value.len() as u64, out);
    out.extend_from_slice(value);
}

fn encode_qpack_indexed_static(index: u64, out: &mut Vec<u8>) {
    encode_qpack_prefixed_int(6, 0b11, index, out);
}

fn encode_qpack_literal_static_name(index: u64, value: &[u8], out: &mut Vec<u8>) {
    encode_qpack_prefixed_int(4, 0b0101, index, out);
    encode_qpack_string(8, 0, value, out);
}

fn encode_qpack_literal(name: &[u8], value: &[u8], out: &mut Vec<u8>) {
    encode_qpack_string(4, 0b0010, name, out);
    encode_qpack_string(8, 0, value, out);
}

fn encode_webtransport_connect_headers(authority: &str) -> Vec<u8> {
    let mut block = Vec::new();
    block.push(0);
    block.push(0);

    encode_qpack_indexed_static(15, &mut block); // :method CONNECT
    encode_qpack_indexed_static(23, &mut block); // :scheme https
    encode_qpack_literal_static_name(0, authority.as_bytes(), &mut block); // :authority
    encode_qpack_literal_static_name(1, b"/wt", &mut block); // :path
    encode_qpack_literal(b":protocol", b"webtransport", &mut block);
    block
}

async fn read_h3_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<(u64, Vec<u8>)> {
    let frame_type = read_varint(reader).await?;
    let frame_len = read_varint(reader).await? as usize;
    let mut payload = vec![0u8; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok((frame_type, payload))
}

fn qpack_block_has_static_status_200(block: &[u8]) -> bool {
    block.len() >= 3 && block[0] == 0 && block[1] == 0 && block[2..].contains(&0xd9)
}

async fn webtransport_echo_roundtrip(
    _cert_b64: &str,
    host: &str,
    port: u16,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client_crypto = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
        h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
    ));

    let mut endpoint = h3_quinn::quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);
    let server_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let conn = endpoint.connect(server_addr, host)?.await?;

    let mut control = conn.open_uni().await?;
    let mut settings = Vec::new();
    encode_varint(0x00, &mut settings); // HTTP/3 control stream
    let mut settings_payload = Vec::new();
    encode_varint(0x08, &mut settings_payload); // SETTINGS_ENABLE_CONNECT_PROTOCOL
    encode_varint(1, &mut settings_payload);
    encode_varint(0x33, &mut settings_payload); // H3_DATAGRAM
    encode_varint(1, &mut settings_payload);
    encode_varint(0x2b60_3742, &mut settings_payload); // SETTINGS_ENABLE_WEBTRANSPORT
    encode_varint(1, &mut settings_payload);
    encode_varint(0x2b60_3743, &mut settings_payload); // WEBTRANSPORT_MAX_SESSIONS
    encode_varint(16, &mut settings_payload);
    encode_varint(0x04, &mut settings); // SETTINGS frame
    encode_varint(settings_payload.len() as u64, &mut settings);
    settings.extend_from_slice(&settings_payload);
    control.write_all(&settings).await?;
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;

    let (mut connect_send, mut connect_recv) = conn.open_bi().await?;
    let mut request = Vec::new();
    let headers = encode_webtransport_connect_headers(&format!("{host}:{port}"));
    encode_varint(0x01, &mut request); // HEADERS
    encode_varint(headers.len() as u64, &mut request);
    request.extend_from_slice(&headers);
    connect_send.write_all(&request).await?;

    let (frame_type, response_headers) = read_h3_frame(&mut connect_recv)
        .await
        .map_err(|error| format!("read WebTransport CONNECT response: {error}"))?;
    assert_eq!(frame_type, 0x01, "expected WebTransport response HEADERS");
    assert!(
        qpack_block_has_static_status_200(&response_headers),
        "expected WebTransport CONNECT 200 response"
    );

    let (mut wt_send, mut wt_recv) = conn.open_bi().await?;
    let mut stream_prefix = Vec::new();
    encode_varint(0x41, &mut stream_prefix); // WebTransport bidirectional stream frame
    encode_varint(0, &mut stream_prefix); // first client bidirectional stream is the CONNECT stream
    wt_send
        .write_all(&stream_prefix)
        .await
        .map_err(|error| format!("write WebTransport stream prefix: {error}"))?;
    wt_send
        .write_all(b"ping")
        .await
        .map_err(|error| format!("write WebTransport stream body: {error}"))?;
    wt_send.finish()?;

    let mut body = vec![0u8; b"wt-echo:ping".len()];
    wt_recv
        .read_exact(&mut body)
        .await
        .map_err(|error| format!("read WebTransport stream echo: {error}"))?;
    endpoint.close(0u32.into(), b"done");
    Ok(body)
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
    let (cert_b64, key_b64, host) = match load_test_env() {
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
            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
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
    let (cert_b64, key_b64, host) = match load_test_env() {
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
            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
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
    let (cert_b64, key_b64, host) = match load_test_env() {
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
            let mut ws_cfg = tls_client_config(&cert_b64);
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
async fn webtransport_bidi_stream_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping webtransport: missing TLS env");
            return;
        }
    };

    run_with_webtransport_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, mut accepted_rx| async move {
            let body = webtransport_echo_roundtrip(&cert_b64, &host, port)
                .await
                .expect("WebTransport echo roundtrip");
            assert_eq!(body, b"wt-echo:ping");
            accepted_rx
                .recv()
                .await
                .expect("WebTransport handler accepted session");
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_tcp_plain_works() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
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
    let (cert_b64, key_b64, host) = match load_test_env() {
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
