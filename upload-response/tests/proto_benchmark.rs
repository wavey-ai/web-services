use bytes::Bytes;
use http::StatusCode;
use http_pack::stream::{StreamHeaders, StreamResponseHeaders};
use portpicker::pick_unused_port;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{interval, Duration};
use upload_response::{
    ResponseWatcher, TailSlot, UploadResponseConfig, UploadResponseRouter, UploadResponseService,
};
use web_service::{H2H3Server, Server, ServerBuilder};

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
    cert_b64: &str,
    upload_size_mb: usize,
    use_http2: bool,
) -> (f64, bool) {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    let proto = if use_http2 { "HTTP/2" } else { "HTTP/1.1" };
    println!("Generating {} MB test data for {}...", upload_size_mb, proto);
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let cert_pem = tls_helpers::from_base64_raw(cert_b64).expect("decode cert");
    let cert = reqwest::Certificate::from_pem(&cert_pem).expect("parse cert");

    let mut builder = reqwest::Client::builder()
        .add_root_certificate(cert)
        .danger_accept_invalid_certs(true);

    if use_http2 {
        builder = builder.http2_prior_knowledge();
    } else {
        builder = builder.http1_only();
    }

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
    let (throughput, passed) = run_upload_test(port, &cert_b64, 100, false).await;
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
    let (throughput, passed) = run_upload_test(port, &cert_b64, 100, true).await;
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
    let (throughput, passed) = run_upload_test(port, &cert_b64, 1024, false).await;
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
    let (throughput, passed) = run_upload_test(port, &cert_b64, 1024, true).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==============================\n");

    assert!(passed, "HTTP/2 1GB test failed");
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
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64, port, 512).await;

    println!("\n========================================");
    println!("    Protocol Comparison (512 MB)");
    println!("========================================\n");

    let (h1_throughput, h1_passed) = run_upload_test(port, &cert_b64, 512, false).await;
    let (h2_throughput, h2_passed) = run_upload_test(port, &cert_b64, 512, true).await;

    println!("========================================");
    println!("    Results Summary");
    println!("========================================");
    println!("HTTP/1.1: {:.1} MB/s {}", h1_throughput, if h1_passed { "✓" } else { "✗" });
    println!("HTTP/2:   {:.1} MB/s {}", h2_throughput, if h2_passed { "✓" } else { "✗" });
    println!("========================================\n");

    assert!(h1_passed && h2_passed, "Protocol tests failed");
    let _ = shutdown_tx.send(());
}
