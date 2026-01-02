mod common;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use common::load_test_env;
use futures_util::StreamExt;
use h3_quinn::quinn;
use http::{Request, Response, StatusCode};
use portpicker::pick_unused_port;
use tls_helpers::load_certs_from_base64;
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
        .enable_h3(true)
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
    // Extra wait for H3/QUIC to be ready
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    test_fn(port, host, cert_b64).await;
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;

    drop(guard);
}

/// Create an HTTP/3 client and send a request, returning the response body
async fn h3_request(
    port: u16,
    host: &str,
    cert_b64: &str,
    path: &str,
    body_data: Bytes,
) -> Result<Vec<u8>, String> {
    // Build client config with our test cert
    let mut roots = rustls::RootCertStore::empty();
    if let Ok(certs) = load_certs_from_base64(cert_b64) {
        for cert in certs {
            let _ = roots.add(cert);
        }
    }

    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| format!("QUIC config error: {}", e))?,
    ));

    let mut endpoint = quinn::Endpoint::client(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0))
        .map_err(|e| format!("Failed to create QUIC endpoint: {}", e))?;
    endpoint.set_default_client_config(client_config);

    let server_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
    let conn = endpoint
        .connect(server_addr, host)
        .map_err(|e| format!("QUIC connect error: {}", e))?
        .await
        .map_err(|e| format!("QUIC connection failed: {}", e))?;

    let quinn_conn = h3_quinn::Connection::new(conn);
    let (mut driver, mut send_request) = h3::client::new(quinn_conn)
        .await
        .map_err(|e| format!("H3 client error: {}", e))?;

    // Drive the connection in the background
    let drive_handle = tokio::spawn(async move {
        futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await
    });

    // Build request
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("https://{host}:{port}{path}"))
        .body(())
        .map_err(|e| format!("Request build error: {}", e))?;

    let mut stream = send_request
        .send_request(req)
        .await
        .map_err(|e| format!("H3 send request error: {}", e))?;

    // Send body
    stream
        .send_data(body_data)
        .await
        .map_err(|e| format!("H3 send data error: {}", e))?;

    stream
        .finish()
        .await
        .map_err(|e| format!("H3 finish error: {}", e))?;

    // Get response
    let response = stream
        .recv_response()
        .await
        .map_err(|e| format!("H3 recv response error: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("HTTP/3 error: {}", response.status()));
    }

    // Read body - collect all chunks
    let mut body = Vec::new();
    while let Some(mut chunk) = stream
        .recv_data()
        .await
        .map_err(|e| format!("H3 recv data error: {}", e))?
    {
        body.extend_from_slice(chunk.chunk());
        chunk.advance(chunk.remaining());
    }

    drop(stream);
    drop(send_request);
    drive_handle.abort();

    Ok(body)
}

#[tokio::test(flavor = "multi_thread")]
async fn h3_body_streaming_echo() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h3_body_streaming_echo: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, cert_b64| async move {
            let body_data = Bytes::from_static(b"helloworldtest");

            let response_body = h3_request(port, &host, &cert_b64, "/stream", body_data.clone())
                .await
                .expect("H3 request failed");

            // The server echoes with "ECHO:" prefix
            let expected = format!("ECHO:{}", String::from_utf8_lossy(&body_data));
            assert_eq!(
                String::from_utf8_lossy(&response_body),
                expected,
                "Response should echo back the input with ECHO: prefix"
            );
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn h3_body_streaming_large_data() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h3_body_streaming_large_data: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, cert_b64| async move {
            // Send 1MB of data
            let data_size = 1024 * 1024;
            let body_data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();

            let response_body = h3_request(port, &host, &cert_b64, "/stream", Bytes::from(body_data.clone()))
                .await
                .expect("H3 request failed");

            // Verify we got the data back with prefix(es)
            // Note: QUIC may chunk the body, so we may get multiple ECHO: prefixes
            assert!(
                response_body.len() >= data_size,
                "Response should be at least as large as input, got {} expected >= {}",
                response_body.len(),
                data_size
            );
            assert!(
                response_body.starts_with(b"ECHO:"),
                "Response should start with ECHO:"
            );

            // Extract all echoed data by removing ECHO: prefixes
            let mut extracted_data = Vec::new();
            let mut remaining = &response_body[..];
            while !remaining.is_empty() {
                if remaining.starts_with(b"ECHO:") {
                    remaining = &remaining[5..];
                    // Find next ECHO: or end
                    if let Some(pos) = remaining.windows(5).position(|w| w == b"ECHO:") {
                        extracted_data.extend_from_slice(&remaining[..pos]);
                        remaining = &remaining[pos..];
                    } else {
                        extracted_data.extend_from_slice(remaining);
                        break;
                    }
                } else {
                    // Shouldn't happen, but handle gracefully
                    extracted_data.extend_from_slice(remaining);
                    break;
                }
            }

            assert_eq!(
                extracted_data.len(),
                body_data.len(),
                "Extracted data length should match input"
            );
            assert_eq!(
                extracted_data,
                body_data,
                "Extracted data should match input exactly"
            );

            println!("  1MB test passed: {} bytes in, {} bytes out ({} ECHO: chunks)",
                data_size,
                response_body.len(),
                (response_body.len() - data_size) / 5
            );
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn h3_body_streaming_verify_no_data_loss() {
    ensure_rustls_provider();
    let (cert_b64, key_b64, host) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("skipping h3_body_streaming_verify_no_data_loss: missing TLS env");
            return;
        }
    };

    run_with_server(
        cert_b64.clone(),
        key_b64,
        host.clone(),
        |port, host, cert_b64| async move {
            // Test with various sizes to catch buffering issues
            for size in [100, 1000, 10_000, 100_000, 500_000] {
                let body_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

                let response_body = h3_request(port, &host, &cert_b64, "/stream", Bytes::from(body_data.clone()))
                    .await
                    .expect(&format!("H3 request failed for size {}", size));

                // Response should be at least as large as input (with ECHO: prefix(es))
                assert!(
                    response_body.len() >= size,
                    "Size {}: Response length {} should be >= input size {}",
                    size,
                    response_body.len(),
                    size
                );

                // Verify content by extracting data after removing ECHO: prefixes
                assert!(
                    response_body.starts_with(b"ECHO:"),
                    "Size {}: Response should start with ECHO:",
                    size
                );

                // Extract all echoed data by removing ECHO: prefixes
                let mut extracted_data = Vec::new();
                let mut remaining = &response_body[..];
                while !remaining.is_empty() {
                    if remaining.starts_with(b"ECHO:") {
                        remaining = &remaining[5..];
                        if let Some(pos) = remaining.windows(5).position(|w| w == b"ECHO:") {
                            extracted_data.extend_from_slice(&remaining[..pos]);
                            remaining = &remaining[pos..];
                        } else {
                            extracted_data.extend_from_slice(remaining);
                            break;
                        }
                    } else {
                        extracted_data.extend_from_slice(remaining);
                        break;
                    }
                }

                assert_eq!(
                    extracted_data.len(),
                    size,
                    "Size {}: Extracted data length {} should match input {}",
                    size,
                    extracted_data.len(),
                    size
                );
                assert_eq!(
                    extracted_data,
                    body_data,
                    "Size {}: Echoed data should match input exactly",
                    size
                );

                let num_chunks = (response_body.len() - size) / 5;
                println!("  Size {} bytes: OK ({} chunks)", size, num_chunks);
            }
        },
    )
    .await;
}
