use bytes::Bytes;
use http::StatusCode;
use http_pack::stream::{StreamHeaders, StreamResponseHeaders};
use portpicker::pick_unused_port;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::time::{interval, Duration};
use upload_response::{
    ResponseWatcher, TailSlot, UploadResponseConfig, UploadResponseRouter, UploadResponseService,
};
use web_service::{H2H3Server, Server, ServerBuilder};

// HTTP/3 imports
use h3_quinn::quinn;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tls_helpers::from_base64_raw;

// WebSocket imports
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

const SLOT_SIZE_KB: usize = 64;

fn load_test_env() -> Option<(String, String)> {
    dotenvy::dotenv().ok();
    let cert = std::env::var("TLS_CERT_BASE64").ok()?;
    let key = std::env::var("TLS_KEY_BASE64").ok()?;
    Some((cert, key))
}

/// Worker that processes requests and returns xxhash of body
async fn run_worker(service: Arc<UploadResponseService>) {
    let mut poll = interval(Duration::from_micros(50));
    let num_streams = service.config().num_streams;
    let mut last_seen: Vec<usize> = vec![0; num_streams];
    let mut hashers: std::collections::HashMap<u64, xxhash_rust::xxh64::Xxh64> =
        std::collections::HashMap::new();

    loop {
        poll.tick().await;

        for stream_idx in 0..num_streams {
            let stream_id = stream_idx as u64;
            let current_last = service.request_last(stream_id).unwrap_or(0);

            if current_last <= last_seen[stream_idx] {
                continue;
            }

            for slot_id in (last_seen[stream_idx] + 1)..=current_last {
                match service.tail_request(stream_id, slot_id).await {
                    Some(TailSlot::Headers(_h)) => {
                        hashers.insert(stream_id, xxhash_rust::xxh64::Xxh64::new(0));
                    }
                    Some(TailSlot::Body(data)) => {
                        if let Some(hasher) = hashers.get_mut(&stream_id) {
                            hasher.update(&data);
                        }
                    }
                    Some(TailSlot::End) => {
                        if let Some(hasher) = hashers.remove(&stream_id) {
                            let hash = hasher.digest();

                            let resp_headers = StreamHeaders::Response(StreamResponseHeaders {
                                stream_id,
                                version: http_pack::HttpVersion::Http11,
                                status: 200,
                                headers: vec![],
                            });
                            service
                                .write_response_headers(stream_id, resp_headers)
                                .await
                                .unwrap();
                            service
                                .append_response_body(
                                    stream_id,
                                    Bytes::from(format!("{:016x}", hash)),
                                )
                                .await
                                .unwrap();
                            service.end_response(stream_id).await.unwrap();
                        }
                    }
                    None => {}
                }
            }

            last_seen[stream_idx] = current_last;
        }
    }
}

async fn wait_for_port(port: u16) {
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        if Instant::now() > deadline {
            panic!("server did not start on port {}", port);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Generate deterministic test data and compute expected hash
fn generate_test_data(size_bytes: usize) -> (Vec<u8>, u64) {
    let chunk_size = 64 * 1024;
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
    let mut data = Vec::with_capacity(size_bytes);

    let mut pos = 0;
    while pos < size_bytes {
        let remaining = size_bytes - pos;
        let chunk_len = remaining.min(chunk_size);
        let chunk: Vec<u8> = (0..chunk_len).map(|i| ((pos + i) % 256) as u8).collect();
        hasher.update(&chunk);
        data.extend_from_slice(&chunk);
        pos += chunk_len;
    }

    (data, hasher.digest())
}

async fn run_upload_test(
    port: u16,
    upload_size_mb: usize,
    use_http2: bool,
) -> (f64, bool) {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    let proto = if use_http2 { "HTTP/2" } else { "HTTP/1.1" };
    println!("Generating {} MB test data for {}...", upload_size_mb, proto);
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let mut builder = reqwest::Client::builder()
        .danger_accept_invalid_certs(true);

    if !use_http2 {
        builder = builder.http1_only();
    }
    // Let reqwest negotiate HTTP/2 via ALPN for use_http2=true

    let client = builder.build().expect("build client");

    println!("Uploading {} MB via {}...", upload_size_mb, proto);
    let start = Instant::now();

    let response = client
        .post(format!("https://localhost:{}/upload", port))
        .body(data)
        .send()
        .await
        .expect("send request");

    let total_elapsed = start.elapsed();

    let status = response.status();
    let body = response.text().await.expect("read body");

    let expected_hex = format!("{:016x}", expected_hash);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "{}: {:.2}s ({:.1} MB/s)",
        proto,
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      {}", body.trim());

    let passed = status == StatusCode::OK && body.trim() == expected_hex;
    if passed {
        println!("{} PASSED\n", proto);
    } else {
        println!("{} FAILED\n", proto);
    }

    (throughput, passed)
}

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn parse_certs_and_key(
    cert_b64: &str,
    key_b64: &str,
) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let cert_pem = from_base64_raw(cert_b64).expect("decode cert");
    let key_pem = from_base64_raw(key_b64).expect("decode key");

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .filter_map(|r| r.ok())
        .collect();

    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .expect("parse key")
        .expect("no key found");

    (certs, key)
}

async fn run_h3_upload_test(
    port: u16,
    upload_size_mb: usize,
    cert_b64: &str,
    key_b64: &str,
) -> (f64, bool) {
    ensure_rustls_provider();

    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for HTTP/3...", upload_size_mb);
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    // Parse certs for client verification skip
    let (certs, _key) = parse_certs_and_key(cert_b64, key_b64);

    // Build QUIC client with cert verification disabled for self-signed
    let mut root_store = rustls::RootCertStore::empty();
    for cert in &certs {
        let _ = root_store.add(cert.clone());
    }

    let mut client_crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).expect("quic config"),
    ));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).expect("bind endpoint");
    endpoint.set_default_client_config(client_config);

    let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    println!("Uploading {} MB via HTTP/3...", upload_size_mb);
    let start = Instant::now();

    // Connect via QUIC
    let conn = endpoint
        .connect(server_addr, "localhost")
        .expect("connect")
        .await
        .expect("connection");

    let quinn_conn = h3_quinn::Connection::new(conn);
    let (mut driver, mut send_request) = h3::client::new(quinn_conn).await.expect("h3 client");

    // Drive the connection in background
    tokio::spawn(async move {
        let _ = futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    // Build request
    let req = http::Request::post(format!("https://localhost:{}/upload", port))
        .body(())
        .expect("build request");

    let mut stream = send_request.send_request(req).await.expect("send request");

    // Send body in chunks
    let chunk_size = 64 * 1024;
    for chunk in data.chunks(chunk_size) {
        stream
            .send_data(Bytes::copy_from_slice(chunk))
            .await
            .expect("send data");
    }
    stream.finish().await.expect("finish stream");

    // Receive response
    let resp = stream.recv_response().await.expect("recv response");
    let status = resp.status();

    let mut body_data = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.expect("recv data") {
        use bytes::Buf;
        while chunk.has_remaining() {
            let bytes = chunk.chunk();
            body_data.extend_from_slice(bytes);
            let len = bytes.len();
            chunk.advance(len);
        }
    }
    let body = String::from_utf8_lossy(&body_data).to_string();

    let total_elapsed = start.elapsed();

    let expected_hex = format!("{:016x}", expected_hash);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "HTTP/3: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      {}", body.trim());

    let passed = status == StatusCode::OK && body.trim() == expected_hex;
    if passed {
        println!("HTTP/3 PASSED\n");
    } else {
        println!("HTTP/3 FAILED\n");
    }

    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;

    (throughput, passed)
}

async fn run_wss_upload_test(
    port: u16,
    upload_size_mb: usize,
    cert_b64: &str,
    _key_b64: &str,
) -> (f64, bool) {
    ensure_rustls_provider();

    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for WSS...", upload_size_mb);
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    // Parse certs for client verification skip
    let (certs, _key) = parse_certs_and_key(cert_b64, _key_b64);

    // Build TLS config with cert verification disabled for self-signed
    let mut root_store = rustls::RootCertStore::empty();
    for cert in &certs {
        let _ = root_store.add(cert.clone());
    }

    let client_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(client_config));

    println!("Uploading {} MB via WSS...", upload_size_mb);
    let start = Instant::now();

    let url = format!("wss://localhost:{}/upload", port);
    let (mut ws_stream, _response) = tokio_tungstenite::connect_async_tls_with_config(
        &url,
        None,
        false,
        Some(connector),
    )
    .await
    .expect("WebSocket connect");

    // Send data in 64KB chunks
    let chunk_size = 64 * 1024;
    for chunk in data.chunks(chunk_size) {
        ws_stream
            .send(Message::Binary(chunk.to_vec().into()))
            .await
            .expect("send binary");
    }

    // Signal end of upload with empty binary frame
    ws_stream
        .send(Message::Binary(Vec::new().into()))
        .await
        .expect("send end marker");

    // Wait for response (keep connection open)
    let mut body_data = Vec::new();
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if data.is_empty() {
                    continue;
                }
                body_data.extend_from_slice(&data);
                break; // Response received, exit loop
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => continue,
            Err(e) => {
                eprintln!("WebSocket recv error: {}", e);
                break;
            }
        }
    }

    // Now close
    let _ = ws_stream.close(None).await;

    let body = String::from_utf8_lossy(&body_data).to_string();

    let total_elapsed = start.elapsed();

    let expected_hex = format!("{:016x}", expected_hash);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "WSS: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      {}", body.trim());

    let passed = body.trim() == expected_hex;
    if passed {
        println!("WSS PASSED\n");
    } else {
        println!("WSS FAILED\n");
    }

    (throughput, passed)
}

#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
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

async fn setup_server(
    cert_b64: String,
    key_b64: String,
    port: u16,
    upload_size_mb: usize,
) -> tokio::sync::watch::Sender<()> {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;
    let num_slots = upload_size_bytes / (SLOT_SIZE_KB * 1024) + 100;

    let config = UploadResponseConfig {
        num_streams: 10,
        slot_size_kb: SLOT_SIZE_KB,
        slots_per_stream: num_slots,
        response_timeout_ms: 600_000,
    };
    let service = Arc::new(UploadResponseService::new(config));

    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    let worker_service = service.clone();
    tokio::spawn(async move {
        run_worker(worker_service).await;
    });

    let router = Box::new(UploadResponseRouter::new(service.clone()));

    let server = H2H3Server::builder()
        .with_tls(cert_b64, key_b64)
        .with_port(port)
        .enable_h2(true)
        .enable_h3(true)
        .enable_websocket(true)
        .with_router(router)
        .build()
        .expect("build server");

    let handle = server.start().await.expect("start server");
    let shutdown_tx = handle.shutdown_tx.clone();
    handle.ready_rx.await.expect("server ready");
    wait_for_port(port).await;

    shutdown_tx
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http1_100mb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64, port, 100).await;

    println!("\n=== HTTP/1.1 100MB Upload Test ===");
    let (throughput, passed) = run_upload_test(port, 100, false).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==================================\n");

    assert!(passed, "HTTP/1.1 test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http2_100mb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64, port, 100).await;

    println!("\n=== HTTP/2 100MB Upload Test ===");
    let (throughput, passed) = run_upload_test(port, 100, true).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("================================\n");

    assert!(passed, "HTTP/2 test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http1_1gb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64, port, 1024).await;

    println!("\n=== HTTP/1.1 1GB Upload Test ===");
    let (throughput, passed) = run_upload_test(port, 1024, false).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("================================\n");

    assert!(passed, "HTTP/1.1 1GB test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http2_1gb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64, port, 1024).await;

    println!("\n=== HTTP/2 1GB Upload Test ===");
    let (throughput, passed) = run_upload_test(port, 1024, true).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==============================\n");

    assert!(passed, "HTTP/2 1GB test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http3_100mb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 100).await;

    println!("\n=== HTTP/3 100MB Upload Test ===");
    let (throughput, passed) = run_h3_upload_test(port, 100, &cert_b64, &key_b64).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("================================\n");

    assert!(passed, "HTTP/3 test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http3_1gb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 1024).await;

    println!("\n=== HTTP/3 1GB Upload Test ===");
    let (throughput, passed) = run_h3_upload_test(port, 1024, &cert_b64, &key_b64).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==============================\n");

    assert!(passed, "HTTP/3 1GB test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wss_100mb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 100).await;

    println!("\n=== WSS 100MB Upload Test ===");
    let (throughput, passed) = run_wss_upload_test(port, 100, &cert_b64, &key_b64).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("=============================\n");

    assert!(passed, "WSS test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wss_1gb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 1024).await;

    println!("\n=== WSS 1GB Upload Test ===");
    let (throughput, passed) = run_wss_upload_test(port, 1024, &cert_b64, &key_b64).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("===========================\n");

    assert!(passed, "WSS 1GB test failed");
    let _ = shutdown_tx.send(());
}

/// Compare all protocols
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_comparison() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 512).await;

    println!("\n========================================");
    println!("    Protocol Comparison (512 MB)");
    println!("========================================\n");

    let (h1_throughput, h1_passed) = run_upload_test(port, 512, false).await;
    let (h2_throughput, h2_passed) = run_upload_test(port, 512, true).await;
    let (h3_throughput, h3_passed) = run_h3_upload_test(port, 512, &cert_b64, &key_b64).await;
    let (wss_throughput, wss_passed) = run_wss_upload_test(port, 512, &cert_b64, &key_b64).await;

    println!("========================================");
    println!("    Results Summary");
    println!("========================================");
    println!("HTTP/1.1: {:.1} MB/s {}", h1_throughput, if h1_passed { "✓" } else { "✗" });
    println!("HTTP/2:   {:.1} MB/s {}", h2_throughput, if h2_passed { "✓" } else { "✗" });
    println!("HTTP/3:   {:.1} MB/s {}", h3_throughput, if h3_passed { "✓" } else { "✗" });
    println!("WSS:      {:.1} MB/s {}", wss_throughput, if wss_passed { "✓" } else { "✗" });
    println!("========================================\n");

    assert!(h1_passed && h2_passed && h3_passed && wss_passed, "Protocol tests failed");
    let _ = shutdown_tx.send(());
}
