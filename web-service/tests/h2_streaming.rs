mod common;

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::OnceLock,
};

use async_trait::async_trait;
use bytes::Bytes;
use common::load_test_env;
use futures_util::StreamExt;
use http::{Request, Response, StatusCode};
use portpicker::pick_unused_port;
use reqwest::Client;
use tls_helpers::from_base64_raw;
use tokio_rustls::rustls;
use web_service::{
    BodyStream, H2H3Server, HandlerResponse, HandlerResult, Router, Server, ServerBuilder,
    ServerError, StreamWriter, WebSocketHandler, WebTransportHandler,
};

/// Router that supports bidirectional streaming on /stream endpoint
struct StreamingRouter;

#[async_trait]
impl Router for StreamingRouter {
    async fn route(&self, _req: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(Bytes::from_static(b"ok")),
            content_type: Some("text/plain".into()),
            ..Default::default()
        })
    }

    fn has_body_handler(&self, _path: &str) -> bool {
        false
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    fn is_body_streaming(&self, path: &str) -> bool {
        path == "/stream"
    }

    async fn route_body_stream(
        &self,
        _req: Request<()>,
        mut body: BodyStream,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        // Send initial response headers
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(())
            .map_err(|e| ServerError::Http(e))?;
        stream_writer.send_response(response).await?;

        // Echo back each chunk with a prefix as it arrives
        while let Some(chunk_result) = body.next().await {
            match chunk_result {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    // Echo with "ECHO:" prefix
                    let mut echo = Vec::with_capacity(5 + chunk.len());
                    echo.extend_from_slice(b"ECHO:");
                    echo.extend_from_slice(&chunk);
                    stream_writer.send_data(Bytes::from(echo)).await?;
                }
                Err(e) => {
                    tracing::warn!("Body stream error: {}", e);
                    break;
                }
            }
        }

        stream_writer.finish().await
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("not implemented".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
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

async fn start_server(
    cert_b64: String,
    key_b64: String,
    port: u16,
) -> web_service::HandlerResult<web_service::ServerHandle> {
    let router = Box::new(StreamingRouter);
    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(port)
        .enable_h2(true)
        .enable_websocket(false)
        .with_router(router)
        .build()?;

    server.start().await
}

async fn wait_for_port(port: u16) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
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
    F: FnOnce(u16, String, String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let guard = SERVER_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let port = pick_unused_port().expect("pick port");
    let handle = start_server(cert_b64.clone(), key_b64, port)
        .await
        .expect("start server");
    handle.ready_rx.await.expect("server ready");

    wait_for_port(port).await;

    test_fn(port, host, cert_b64).await;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn h2_body_streaming_echo() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h2_body_streaming_echo: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, cert_b64| async move {
            let cert_pem = from_base64_raw(&cert_b64).expect("decode cert");
            let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");

            let client = Client::builder()
                .add_root_certificate(reqwest_cert)
                .http2_prior_knowledge()
                .build()
                .expect("build client");

            // Send multiple chunks and receive streaming response
            let chunks: Vec<&[u8]> = vec![b"hello", b"world", b"test"];
            let body_data: Vec<u8> = chunks.concat();

            let url = format!("https://{host}:{port}/stream");
            let response = client
                .post(&url)
                .body(body_data.clone())
                .send()
                .await
                .expect("send request");

            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.version(), reqwest::Version::HTTP_2);

            // Read the streamed response
            let body = response.bytes().await.expect("read body");

            // The server echoes with "ECHO:" prefix for each chunk
            // Since reqwest sends the body in one chunk, we expect one echo
            let expected = format!("ECHO:{}", String::from_utf8_lossy(&body_data));
            assert_eq!(
                String::from_utf8_lossy(&body),
                expected,
                "Response should echo back the input with ECHO: prefix"
            );
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn h2_body_streaming_chunked() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h2_body_streaming_chunked: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, cert_b64| async move {
            let cert_pem = from_base64_raw(&cert_b64).expect("decode cert");
            let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");

            let client = Client::builder()
                .add_root_certificate(reqwest_cert)
                .http2_prior_knowledge()
                .build()
                .expect("build client");

            // Use a streaming body to send chunks
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(10);
            let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            let body = reqwest::Body::wrap(http_body_util::StreamBody::new(
                body_stream.map(|r| r.map(|b| hyper::body::Frame::data(b))),
            ));

            let url = format!("https://{host}:{port}/stream");

            // Spawn task to send chunks
            let send_task = tokio::spawn(async move {
                for chunk in [b"chunk1".to_vec(), b"chunk2".to_vec(), b"chunk3".to_vec()] {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if tx.send(Ok(Bytes::from(chunk))).await.is_err() {
                        break;
                    }
                }
                // Drop sender to signal end
                drop(tx);
            });

            let response = client
                .post(&url)
                .body(body)
                .send()
                .await
                .expect("send request");

            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.version(), reqwest::Version::HTTP_2);

            // Read the streamed response
            let response_body = response.bytes().await.expect("read body");

            send_task.await.expect("send task");

            // Each chunk should be echoed with ECHO: prefix
            // The exact format depends on how the chunks are received
            let response_str = String::from_utf8_lossy(&response_body);
            assert!(
                response_str.contains("ECHO:"),
                "Response should contain ECHO: prefix, got: {}",
                response_str
            );
            assert!(
                response_str.contains("chunk1") || response_str.contains("chunk2") || response_str.contains("chunk3"),
                "Response should contain chunked data, got: {}",
                response_str
            );
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn h2_body_streaming_large_data() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h2_body_streaming_large_data: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, cert_b64| async move {
            let cert_pem = from_base64_raw(&cert_b64).expect("decode cert");
            let reqwest_cert = reqwest::Certificate::from_pem(&cert_pem).expect("reqwest cert");

            let client = Client::builder()
                .add_root_certificate(reqwest_cert)
                .http2_prior_knowledge()
                .build()
                .expect("build client");

            // Send 1MB of data
            let data_size = 1024 * 1024;
            let body_data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();

            let url = format!("https://{host}:{port}/stream");
            let response = client
                .post(&url)
                .body(body_data.clone())
                .send()
                .await
                .expect("send request");

            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.version(), reqwest::Version::HTTP_2);

            // Read the streamed response
            let response_body = response.bytes().await.expect("read body");

            // Verify we got the data back with prefix
            assert!(
                response_body.len() > data_size,
                "Response should be larger than input due to ECHO: prefix"
            );
            assert!(
                response_body.starts_with(b"ECHO:"),
                "Response should start with ECHO:"
            );
        },
    )
    .await;
}
