//! Latency of responses written as several small writes.
//!
//! A streaming response goes out as separate writes, and a raw TCP frame goes out as a length
//! prefix and then a payload. With Nagle's algorithm on, each small write after the first
//! waits for the ACK of the one before it, and a client that delays its ACK adds about 40 ms
//! per exchange. With `TCP_NODELAY` on the accepted socket, an exchange on loopback takes a
//! few milliseconds. HTTP/2 output is batched below TLS instead; a test here checks that many
//! concurrent streams still arrive intact.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use portpicker::pick_unused_port;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::{self, pki_types::ServerName, ClientConfig, RootCertStore};
use web_service::{
    read_length_prefixed_frame, write_length_prefixed_frame, H2H3Server, HandlerResponse,
    HandlerResult, RawTcpHandler, Router, Server, ServerBuilder, ServerHandle, StreamWriter,
    WebSocketHandler, WebTransportHandler,
};

/// Well under the 40 ms of a delayed ACK, and well over a loopback exchange.
const LARGE_CHUNK: usize = 16 * 1024;
const LARGE_CHUNKS: usize = 64;
const EXCHANGE_LIMIT: Duration = Duration::from_millis(20);
/// Enough exchanges for a Linux client to leave its quick-ACK mode and delay its ACKs. The
/// median is compared with the limit, so that a scheduling pause on a busy test machine does
/// not fail the test; a delayed ACK delays every exchange.
const EXCHANGES: usize = 21;

/// `/two-writes` streams its body as two chunks with a pause between them, so that hyper
/// writes them separately.
struct TwoWrites;

#[async_trait]
impl Router for TwoWrites {
    async fn route(&self, _req: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse::default())
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == "/two-writes" || path == "/large"
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if req.uri().path() == "/large" {
            writer.send_response(Response::new(())).await?;
            for index in 0..LARGE_CHUNKS {
                writer
                    .send_data(Bytes::from(vec![index as u8; LARGE_CHUNK]))
                    .await?;
            }
            return writer.finish().await;
        }
        writer.send_response(Response::new(())).await?;
        writer.send_data(Bytes::from_static(b"first;")).await?;
        tokio::time::sleep(Duration::from_millis(2)).await;
        writer.send_data(Bytes::from_static(b"second")).await?;
        writer.finish().await
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

/// Answers each length-prefixed frame with the same frame.
struct FrameEcho;

#[async_trait]
impl RawTcpHandler for FrameEcho {
    async fn handle_stream(
        &self,
        mut stream: Box<dyn web_service::traits::RawStream>,
        _is_tls: bool,
    ) -> HandlerResult<()> {
        while let Ok(Some(frame)) = read_length_prefixed_frame(&mut stream, 1024).await {
            write_length_prefixed_frame(&mut stream, &frame).await?;
        }
        Ok(())
    }
}

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

struct TestServer {
    handle: ServerHandle,
    port: u16,
    raw_port: u16,
    roots: Arc<RootCertStore>,
}

async fn start() -> TestServer {
    ensure_rustls_provider();
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let mut attempts = 0;
    // Another test can take a free port before this server binds it.
    let (mut handle, port, raw_port) = loop {
        let port = pick_unused_port().expect("unused test port");
        let raw_port = pick_unused_port().expect("unused test port");
        if raw_port == port {
            continue;
        }
        let started = H2H3Server::builder()
            .with_tls(
                STANDARD.encode(cert.pem()),
                STANDARD.encode(key_pair.serialize_pem()),
            )
            .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
            .with_port(port)
            .enable_h2(true)
            .enable_h3(false)
            .enable_websocket(false)
            .enable_webtransport(false)
            .enable_raw_tcp(true)
            .with_raw_tcp_port(raw_port)
            .with_raw_tcp_tls(false)
            .with_raw_tcp_handler(Box::new(FrameEcho))
            .with_router(Box::new(TwoWrites))
            .build()
            .unwrap()
            .start()
            .await;
        match started {
            Ok(handle) => break (handle, port, raw_port),
            Err(error) if attempts < 5 && error.to_string().contains("Address already in use") => {
                attempts += 1;
            }
            Err(error) => panic!("server start failed: {error}"),
        }
    };
    (&mut handle.ready_rx).await.expect("server readiness");
    TestServer {
        handle,
        port,
        raw_port,
        roots: Arc::new(roots),
    }
}

impl TestServer {
    async fn tls(&self, alpn: &[u8]) -> tokio_rustls::client::TlsStream<TcpStream> {
        let tcp = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, self.port)))
            .await
            .unwrap();
        let mut config = ClientConfig::builder()
            .with_root_certificates(Arc::clone(&self.roots))
            .with_no_client_auth();
        config.alpn_protocols = vec![alpn.to_vec()];
        tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap()
    }

    async fn stop(self) {
        let _ = self.handle.shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(5), self.handle.finished_rx)
            .await
            .expect("server shutdown timed out")
            .unwrap();
    }
}

fn two_writes_request() -> Request<Empty<Bytes>> {
    Request::get("https://localhost/two-writes")
        .body(Empty::new())
        .unwrap()
}

fn assert_fast(mut exchanges: Vec<Duration>, what: &str) {
    exchanges.sort();
    let median = exchanges[exchanges.len() / 2];
    assert!(
        median < EXCHANGE_LIMIT,
        "{what}: median exchange took {median:?}; all: {exchanges:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn http1_responses_in_several_writes_arrive_without_delay() {
    let server = start().await;
    let io = TokioIo::new(server.tls(b"http/1.1").await);
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
    let connection = tokio::spawn(connection);
    let mut exchanges = Vec::new();
    for _ in 0..EXCHANGES {
        let started = Instant::now();
        sender.ready().await.unwrap();
        let response = sender.send_request(two_writes_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"first;second");
        exchanges.push(started.elapsed());
    }
    assert_fast(exchanges, "HTTP/1.1");
    connection.abort();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http2_responses_in_several_writes_arrive_without_delay() {
    let server = start().await;
    let io = TokioIo::new(server.tls(b"h2").await);
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(TokioExecutor::new(), io)
            .await
            .unwrap();
    let connection = tokio::spawn(connection);
    let mut exchanges = Vec::new();
    for _ in 0..EXCHANGES {
        let started = Instant::now();
        sender.ready().await.unwrap();
        let response = sender.send_request(two_writes_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"first;second");
        exchanges.push(started.elapsed());
    }
    assert_fast(exchanges, "HTTP/2");
    connection.abort();
    server.stop().await;
}

/// HTTP/2 output is batched below TLS. Many concurrent streams of many frames arrive intact.
#[tokio::test(flavor = "multi_thread")]
async fn http2_streams_through_batched_writes_arrive_intact() {
    let server = start().await;
    let io = TokioIo::new(server.tls(b"h2").await);
    let (sender, connection) =
        hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(TokioExecutor::new(), io)
            .await
            .unwrap();
    let connection = tokio::spawn(connection);
    let mut streams = Vec::new();
    for _ in 0..32 {
        let mut sender = sender.clone();
        streams.push(tokio::spawn(async move {
            sender.ready().await.unwrap();
            let response = sender
                .send_request(
                    Request::get("https://localhost/large")
                        .body(Empty::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            response.into_body().collect().await.unwrap().to_bytes()
        }));
    }
    for stream in streams {
        let body = tokio::time::timeout(Duration::from_secs(30), stream)
            .await
            .expect("a stream stalled")
            .unwrap();
        assert_eq!(body.len(), LARGE_CHUNK * LARGE_CHUNKS);
        for (index, chunk) in body.chunks(LARGE_CHUNK).enumerate() {
            assert!(chunk.iter().all(|byte| *byte == index as u8));
        }
    }
    connection.abort();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_tcp_frames_in_two_writes_arrive_without_delay() {
    let server = start().await;
    let mut stream = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, server.raw_port)))
        .await
        .unwrap();
    // The client writes each frame in one write, so only the server's writes are measured.
    stream.set_nodelay(true).unwrap();
    let mut exchanges = Vec::new();
    for exchange in 0..EXCHANGES {
        let payload = format!("frame {exchange}");
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(payload.as_bytes());
        let started = Instant::now();
        stream.write_all(&frame).await.unwrap();
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut echoed = vec![0u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(echoed, payload.as_bytes());
        exchanges.push(started.elapsed());
    }
    assert_fast(exchanges, "raw TCP");
    drop(stream);
    server.stop().await;
}

/// The plain listener shares the TLS listener's connection path.
#[cfg(feature = "plain-http")]
#[tokio::test(flavor = "multi_thread")]
async fn plain_http1_responses_in_several_writes_arrive_without_delay() {
    let port = pick_unused_port().expect("unused test port");
    let mut handle = H2H3Server::builder()
        .with_plain_http()
        .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .with_port(port)
        .with_router(Box::new(TwoWrites))
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    (&mut handle.ready_rx).await.expect("server readiness");
    let tcp = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
        .unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let mut exchanges = Vec::new();
    for _ in 0..EXCHANGES {
        let started = Instant::now();
        sender.ready().await.unwrap();
        let response = sender
            .send_request(
                Request::get("http://localhost/two-writes")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"first;second");
        exchanges.push(started.elapsed());
    }
    assert_fast(exchanges, "plain HTTP/1.1");
    connection.abort();
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
}
