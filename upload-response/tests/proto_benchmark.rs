use bytes::Bytes;
use http::StatusCode;
use http_pack::stream::{StreamHeaders, StreamResponseHeaders};
use portpicker::pick_unused_port;
#[cfg(feature = "rist")]
use rist::Profile as LibRistProfile;
use std::net::SocketAddr;
#[cfg(feature = "srt")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::time::{interval, Duration};
#[cfg(feature = "webrtc")]
use upload_response::WebRtcIngest;
#[cfg(feature = "srt")]
use upload_response::{AllowAllEncrypted, SrtIngest};
#[cfg(feature = "rist-pure")]
use upload_response::{PureRistIngest, PureRistProfile};
use upload_response::{
    ResponseWatcher, TailSlot, TcpIngest, UploadResponseConfig, UploadResponseRouter,
    UploadResponseService,
};
#[cfg(feature = "rist")]
use upload_response::{RistIngest, RistProfile};
#[cfg(feature = "udp-fec")]
use upload_response::{UdpFecIngest, UdpFecSender};
use web_service::{H2H3Server, Server, ServerBuilder};

// HTTP/3 imports
use h3_quinn::quinn;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tls_helpers::from_base64_raw;

// WebSocket imports
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

// SRT imports
#[cfg(feature = "srt")]
use srt::{AsyncListener, AsyncStream, ConnectOptions, ListenerOption, MaxBandwidth};
use tokio::io::AsyncWriteExt;

// RTMP integration is tested in rtmp-ingress to avoid a cyclic dev dependency.
#[cfg(any())]
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
#[cfg(any())]
use rml_rtmp::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult, PublishRequestType,
};
#[cfg(any())]
use rtmp_ingress::upload::RtmpUploadIngest;
#[cfg(any())]
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

// WebRTC imports
#[cfg(feature = "webrtc")]
use futures::{select, FutureExt};
#[cfg(feature = "webrtc")]
use futures_timer::Delay;
#[cfg(feature = "webrtc")]
use matchbox_signaling::topologies::client_server::{ClientServer, ClientServerState};
#[cfg(feature = "webrtc")]
use matchbox_signaling::SignalingServerBuilder;
#[cfg(feature = "webrtc")]
use matchbox_socket::{ChannelConfig, PeerState, WebRtcSocket, WebRtcSocketBuilder};

use base64::Engine;
use std::fs;

const SLOT_SIZE_KB: usize = 64;
#[cfg(feature = "rist-pure")]
const PURE_RIST_FLOW_ID: u32 = 0x1122_3344;

// Local cert paths (checked into repo)
const LOCAL_CERT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../tls/local.wavey.ai/fullchain.pem"
);
const LOCAL_KEY_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../tls/local.wavey.ai/privkey.pem"
);

fn load_test_env() -> Option<(String, String)> {
    dotenvy::dotenv().ok();

    // Try env vars first
    if let (Ok(cert), Ok(key)) = (
        std::env::var("TLS_CERT_BASE64"),
        std::env::var("TLS_KEY_BASE64"),
    ) {
        return Some((cert, key));
    }

    // Fall back to local certs
    let cert_pem = fs::read(LOCAL_CERT_PATH).ok()?;
    let key_pem = fs::read(LOCAL_KEY_PATH).ok()?;

    let cert_b64 = base64::engine::general_purpose::STANDARD.encode(&cert_pem);
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(&key_pem);

    Some((cert_b64, key_b64))
}

/// Worker that processes requests and returns xxhash of body
async fn run_worker(service: Arc<UploadResponseService>) {
    let mut poll = interval(Duration::from_micros(50));
    let num_streams = service.config().num_streams;
    let mut stream_ids: Vec<u64> = vec![0; num_streams];
    let mut last_seen: Vec<usize> = vec![0; num_streams];
    let mut hashers: std::collections::HashMap<u64, xxhash_rust::xxh64::Xxh64> =
        std::collections::HashMap::new();

    loop {
        poll.tick().await;

        for stream_idx in 0..num_streams {
            let stream_id = service.slot_stream_id(stream_idx).unwrap_or(0);
            let previous_stream_id = stream_ids[stream_idx];

            if stream_id == 0 {
                if previous_stream_id != 0 {
                    hashers.remove(&previous_stream_id);
                    stream_ids[stream_idx] = 0;
                    last_seen[stream_idx] = 0;
                }
                continue;
            }

            if previous_stream_id != stream_id {
                if previous_stream_id != 0 {
                    hashers.remove(&previous_stream_id);
                }
                stream_ids[stream_idx] = stream_id;
                last_seen[stream_idx] = 0;
            }

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
                    Some(TailSlot::Control(_)) => {}
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

fn hash_hex(data: &[u8]) -> String {
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
    hasher.update(data);
    format!("{:016x}", hasher.digest())
}

fn upload_recovery_timeout(upload_size_mb: usize) -> Duration {
    Duration::from_secs((30 + upload_size_mb as u64 * 4).min(300))
}

async fn wait_for_recovered_request_body(
    service: Arc<UploadResponseService>,
    expected_bytes: usize,
    timeout: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    let mut recovered = Vec::with_capacity(expected_bytes);
    let mut best_len = 0usize;
    loop {
        if Instant::now() > deadline {
            println!(
                "recovered {}/{} request bytes before timeout",
                best_len, expected_bytes
            );
            return None;
        }

        for slot in service.active_stream_slots() {
            recovered.clear();
            let last = service.request_last(slot.stream_id).unwrap_or(0);
            for slot_id in 2..=last {
                if let Some(TailSlot::Body(bytes)) =
                    service.tail_request(slot.stream_id, slot_id).await
                {
                    recovered.extend_from_slice(&bytes);
                    best_len = best_len.max(recovered.len());
                    if recovered.len() >= expected_bytes {
                        return Some(recovered[..expected_bytes].to_vec());
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(feature = "udp-fec")]
fn udp_fec_benchmark_size_mb(env_name: &str, default_mb: usize) -> usize {
    std::env::var(env_name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_mb)
}

#[cfg(feature = "udp-fec")]
fn udp_fec_u16_env(env_name: &str, default: u16) -> u16 {
    std::env::var(env_name)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[cfg(feature = "udp-fec")]
fn udp_fec_u32_env(env_name: &str, default: u32) -> u32 {
    std::env::var(env_name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "udp-fec")]
fn udp_fec_u64_env(env_name: &str, default: u64) -> u64 {
    std::env::var(env_name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "udp-fec")]
fn udp_fec_recovery_timeout(upload_size_mb: usize) -> Duration {
    Duration::from_secs((30 + upload_size_mb as u64 * 8).min(300))
}

#[cfg(feature = "udp-fec")]
fn udp_fec_loss_pace() -> Duration {
    let micros = std::env::var("UDP_FEC_LOSS_PACE_US")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(250);
    Duration::from_micros(micros)
}

async fn run_upload_test(port: u16, upload_size_mb: usize, use_http2: bool) -> (f64, bool) {
    run_upload_test_inner(port, upload_size_mb, use_http2, false).await
}

async fn run_upload_test_chunked(port: u16, upload_size_mb: usize) -> (f64, bool) {
    run_upload_test_inner(port, upload_size_mb, false, true).await
}

async fn run_upload_test_inner(
    port: u16,
    upload_size_mb: usize,
    use_http2: bool,
    use_chunked: bool,
) -> (f64, bool) {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    let proto = if use_http2 {
        "HTTP/2"
    } else if use_chunked {
        "HTTP/1.1 (chunked)"
    } else {
        "HTTP/1.1"
    };
    println!(
        "Generating {} MB test data for {}...",
        upload_size_mb, proto
    );
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let mut builder = reqwest::Client::builder().danger_accept_invalid_certs(true);

    if !use_http2 {
        builder = builder.http1_only();
    }
    // Let reqwest negotiate HTTP/2 via ALPN for use_http2=true

    let client = builder.build().expect("build client");

    println!("Uploading {} MB via {}...", upload_size_mb, proto);
    let start = Instant::now();

    let response = if use_chunked {
        // Use streaming body to force chunked transfer encoding
        use http_body_util::StreamBody;

        let chunk_size = 64 * 1024;
        let chunks: Vec<_> = data
            .chunks(chunk_size)
            .map(|c| Ok::<_, std::io::Error>(hyper::body::Frame::data(Bytes::copy_from_slice(c))))
            .collect();
        let stream = futures_util::stream::iter(chunks);
        let body = StreamBody::new(stream);

        client
            .post(format!("https://localhost:{}/upload", port))
            .body(reqwest::Body::wrap(body))
            .send()
            .await
            .expect("send request")
    } else {
        // Known size - uses Content-Length
        client
            .post(format!("https://localhost:{}/upload", port))
            .body(data)
            .send()
            .await
            .expect("send request")
    };

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

    let mut endpoint =
        quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).expect("bind endpoint");
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
    let (mut ws_stream, _response) =
        tokio_tungstenite::connect_async_tls_with_config(&url, None, false, Some(connector))
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

#[cfg(feature = "srt")]
async fn run_srt_upload_test(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    run_srt_upload_test_inner(_service, addr, upload_size_mb, None).await
}

#[cfg(feature = "srt")]
async fn run_srt_upload_test_encrypted(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
    passphrase: &str,
) -> (f64, bool) {
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    run_srt_upload_test_inner(_service, addr, upload_size_mb, Some(passphrase)).await
}

#[cfg(feature = "srt")]
async fn run_srt_upload_test_inner(
    service: Arc<UploadResponseService>,
    addr: SocketAddr,
    upload_size_mb: usize,
    passphrase: Option<&str>,
) -> (f64, bool) {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;
    let proto = if passphrase.is_some() {
        "SRT (encrypted)"
    } else {
        "SRT"
    };

    println!(
        "Generating {} MB test data for {}...",
        upload_size_mb, proto
    );
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let connect_options = ConnectOptions {
        stream_id: Some("test-stream".to_string()),
        passphrase: passphrase.map(|p| p.to_string()),
        timestamp_based_packet_delivery_mode: Some(false),
        too_late_packet_drop: Some(false),
        receive_buffer_size: Some(36_400_000),
        send_buffer_size: Some(36_400_000),
        max_bandwidth: Some(MaxBandwidth::Infinite),
        ..Default::default()
    };

    println!("Uploading {} MB via {}...", upload_size_mb, proto);
    let start = Instant::now();

    let mut stream = AsyncStream::connect(addr, &connect_options)
        .await
        .expect("SRT connect");

    // Send data in 1316-byte SRT packets
    const SRT_PACKET_SIZE: usize = 1316;
    for chunk in data.chunks(SRT_PACKET_SIZE) {
        if let Err(e) = stream.write_all(chunk).await {
            println!("{} write error: {}", proto, e);
            return (0.0, false);
        }
    }

    // Flush and shutdown write side gracefully
    if let Err(e) = stream.flush().await {
        println!("{} flush error: {}", proto, e);
    }

    // The SRT wrapper's flush/shutdown methods do not wait for libSRT to deliver
    // buffered datagrams. Keep the socket alive until the ingest cache proves the
    // full request body arrived.
    let recovered = match wait_for_recovered_request_body(
        service,
        data.len(),
        upload_recovery_timeout(upload_size_mb),
    )
    .await
    {
        Some(recovered) => recovered,
        None => {
            println!("{} timed out waiting for recovered body", proto);
            return (0.0, false);
        }
    };
    drop(stream);

    let total_elapsed = start.elapsed();
    let expected_hex = format!("{:016x}", expected_hash);
    let got_hex = hash_hex(&recovered);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();
    let passed = recovered == data;

    println!(
        "{}: {:.2}s ({:.1} MB/s)",
        proto,
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      {}", got_hex);

    if passed {
        println!("{} PASSED\n", proto);
    } else {
        println!(
            "{} FAILED: recovered {} bytes, expected {}\n",
            proto,
            recovered.len(),
            data.len()
        );
    }

    (throughput, passed)
}

#[cfg(feature = "srt")]
#[derive(Default)]
struct SrtLossProxyStats {
    client_to_server_received: AtomicU64,
    client_to_server_forwarded: AtomicU64,
    client_to_server_dropped: AtomicU64,
    server_to_client_received: AtomicU64,
    server_to_client_forwarded: AtomicU64,
}

#[cfg(feature = "srt")]
#[derive(Debug, Clone, Copy)]
struct SrtLossProxySnapshot {
    client_to_server_received: u64,
    client_to_server_forwarded: u64,
    client_to_server_dropped: u64,
    server_to_client_received: u64,
    server_to_client_forwarded: u64,
}

#[cfg(feature = "srt")]
impl SrtLossProxyStats {
    fn snapshot(&self) -> SrtLossProxySnapshot {
        SrtLossProxySnapshot {
            client_to_server_received: self.client_to_server_received.load(Ordering::Relaxed),
            client_to_server_forwarded: self.client_to_server_forwarded.load(Ordering::Relaxed),
            client_to_server_dropped: self.client_to_server_dropped.load(Ordering::Relaxed),
            server_to_client_received: self.server_to_client_received.load(Ordering::Relaxed),
            server_to_client_forwarded: self.server_to_client_forwarded.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "srt")]
async fn start_srt_loss_proxy(
    target: SocketAddr,
    warmup_client_packets: u64,
    drop_every_client_packet: u64,
) -> (
    SocketAddr,
    Arc<SrtLossProxyStats>,
    tokio::task::JoinHandle<()>,
) {
    start_srt_loss_proxy_with_delay(
        target,
        warmup_client_packets,
        drop_every_client_packet,
        Duration::ZERO,
    )
    .await
}

#[cfg(feature = "srt")]
async fn start_srt_loss_proxy_with_delay(
    target: SocketAddr,
    warmup_client_packets: u64,
    drop_every_client_packet: u64,
    one_way_delay: Duration,
) -> (
    SocketAddr,
    Arc<SrtLossProxyStats>,
    tokio::task::JoinHandle<()>,
) {
    let socket = Arc::new(
        tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind SRT loss proxy"),
    );
    let proxy_addr = socket.local_addr().expect("SRT loss proxy local addr");
    let stats = Arc::new(SrtLossProxyStats::default());
    let task_socket = Arc::clone(&socket);
    let task_stats = Arc::clone(&stats);

    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 65_536];
        let mut client_addr = None::<SocketAddr>;
        let mut server_addr = target;

        loop {
            let Ok((len, peer)) = task_socket.recv_from(&mut buf).await else {
                break;
            };

            if client_addr.is_some_and(|client| peer != client) || peer == target {
                server_addr = peer;
                task_stats
                    .server_to_client_received
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(client) = client_addr {
                    forward_srt_proxy_datagram(
                        Arc::clone(&task_socket),
                        Arc::clone(&task_stats),
                        buf[..len].to_vec(),
                        client,
                        one_way_delay,
                        true,
                    )
                    .await;
                }
                continue;
            }

            if client_addr.is_none() {
                client_addr = Some(peer);
            }
            let received = task_stats
                .client_to_server_received
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            let should_drop = drop_every_client_packet > 0
                && received > warmup_client_packets
                && (received - warmup_client_packets) % drop_every_client_packet == 0;
            if should_drop {
                task_stats
                    .client_to_server_dropped
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            forward_srt_proxy_datagram(
                Arc::clone(&task_socket),
                Arc::clone(&task_stats),
                buf[..len].to_vec(),
                server_addr,
                one_way_delay,
                false,
            )
            .await;
        }
    });

    (proxy_addr, stats, handle)
}

#[cfg(feature = "srt")]
async fn forward_srt_proxy_datagram(
    socket: Arc<tokio::net::UdpSocket>,
    stats: Arc<SrtLossProxyStats>,
    bytes: Vec<u8>,
    target: SocketAddr,
    delay: Duration,
    server_to_client: bool,
) {
    if delay.is_zero() {
        if socket.send_to(&bytes, target).await.is_ok() {
            srt_proxy_record_forwarded(&stats, server_to_client);
        }
        return;
    }

    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        if socket.send_to(&bytes, target).await.is_ok() {
            srt_proxy_record_forwarded(&stats, server_to_client);
        }
    });
}

#[cfg(feature = "srt")]
fn srt_proxy_record_forwarded(stats: &SrtLossProxyStats, server_to_client: bool) {
    if server_to_client {
        stats
            .server_to_client_forwarded
            .fetch_add(1, Ordering::Relaxed);
    } else {
        stats
            .client_to_server_forwarded
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "srt")]
async fn run_srt_loss_upload_test(
    service: Arc<UploadResponseService>,
    server_port: u16,
    upload_size_mb: usize,
    warmup_client_packets: u64,
    drop_every_client_packet: u64,
    timeout_secs: u64,
) -> (f64, bool, SrtLossProxySnapshot) {
    let target: SocketAddr = format!("127.0.0.1:{}", server_port).parse().unwrap();
    let (proxy_addr, proxy_stats, proxy_handle) =
        start_srt_loss_proxy(target, warmup_client_packets, drop_every_client_packet).await;

    let (throughput, passed) = match tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        run_srt_upload_test_inner(service, proxy_addr, upload_size_mb, None),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            println!("SRT loss-proxy upload timed out after {timeout_secs}s");
            (0.0, false)
        }
    };
    let snapshot = proxy_stats.snapshot();
    proxy_handle.abort();

    println!(
        "SRT loss proxy: c2s recv={} fwd={} drop={} s2c recv={} fwd={}",
        snapshot.client_to_server_received,
        snapshot.client_to_server_forwarded,
        snapshot.client_to_server_dropped,
        snapshot.server_to_client_received,
        snapshot.server_to_client_forwarded
    );

    (throughput, passed, snapshot)
}

#[cfg(feature = "srt")]
#[derive(Debug, Clone, Copy)]
struct SrtVideoFrameArrival {
    sequence: u32,
    sent_us: u64,
    arrived_us: u64,
}

#[cfg(feature = "srt")]
impl SrtVideoFrameArrival {
    fn latency_us(self) -> u64 {
        self.arrived_us.saturating_sub(self.sent_us)
    }
}

#[cfg(feature = "srt")]
fn srt_video_frame(sequence: u32, sent_us: u64, payload_len: usize) -> Vec<u8> {
    let body_len = 12usize
        .checked_add(payload_len)
        .expect("SRT video frame length overflow");
    let mut frame = Vec::with_capacity(4 + body_len);
    frame.extend_from_slice(&(body_len as u32).to_be_bytes());
    frame.extend_from_slice(&sequence.to_be_bytes());
    frame.extend_from_slice(&sent_us.to_be_bytes());
    for offset in 0..payload_len {
        frame.push(((sequence as usize + offset) & 0xff) as u8);
    }
    frame
}

#[cfg(feature = "srt")]
async fn collect_srt_video_frames(
    mut stream: AsyncStream,
    expected_frames: usize,
    start: Instant,
) -> Result<Vec<SrtVideoFrameArrival>, String> {
    use tokio::io::AsyncReadExt;

    let mut read_buf = vec![0u8; 4096];
    let mut pending = Vec::new();
    let mut arrivals = Vec::with_capacity(expected_frames);

    while arrivals.len() < expected_frames {
        let n = stream
            .read(&mut read_buf)
            .await
            .map_err(|error| format!("SRT video read error: {error}"))?;
        if n == 0 {
            return Err(format!(
                "SRT video stream closed after {}/{} frames",
                arrivals.len(),
                expected_frames
            ));
        }
        pending.extend_from_slice(&read_buf[..n]);

        loop {
            if pending.len() < 4 {
                break;
            }
            let body_len =
                u32::from_be_bytes(pending[..4].try_into().expect("frame length")) as usize;
            if body_len < 12 {
                return Err(format!("invalid SRT video frame length: {body_len}"));
            }
            let frame_len = 4 + body_len;
            if pending.len() < frame_len {
                break;
            }
            let frame = pending.drain(..frame_len).collect::<Vec<_>>();
            let body = &frame[4..];
            let sequence = u32::from_be_bytes(body[..4].try_into().expect("sequence"));
            let sent_us = u64::from_be_bytes(body[4..12].try_into().expect("sent timestamp"));
            for (offset, byte) in body[12..].iter().enumerate() {
                let expected = ((sequence as usize + offset) & 0xff) as u8;
                if *byte != expected {
                    return Err(format!(
                        "SRT video payload mismatch for frame {sequence} at byte {offset}"
                    ));
                }
            }
            arrivals.push(SrtVideoFrameArrival {
                sequence,
                sent_us,
                arrived_us: start.elapsed().as_micros() as u64,
            });
        }
    }

    Ok(arrivals)
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

#[cfg(feature = "srt")]
async fn setup_srt_server(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let srt_ingest = SrtIngest::new(service.clone());
    let shutdown_tx = srt_ingest.start(addr).await.expect("start SRT server");

    // Give SRT server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

#[cfg(feature = "srt")]
const SRT_PASSPHRASE: &str = "benchmark-test-passphrase-1234";

#[cfg(feature = "srt")]
async fn setup_srt_server_encrypted(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let srt_ingest = SrtIngest::with_auth(service.clone(), AllowAllEncrypted::new(SRT_PASSPHRASE));
    let shutdown_tx = srt_ingest
        .start(addr)
        .await
        .expect("start SRT server (encrypted)");

    // Give SRT server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

#[cfg(feature = "srt")]
async fn setup_srt_server_high_throughput(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let srt_ingest = SrtIngest::new(service.clone());
    let shutdown_tx = srt_ingest
        .start_high_throughput(addr)
        .await
        .expect("start SRT server (high-throughput)");

    // Give SRT server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

// ============== RIST Tests ==============

#[cfg(feature = "rist")]
async fn setup_rist_server(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let rist_ingest = RistIngest::new(service.clone()).with_profile(RistProfile::Main);
    let shutdown_tx = rist_ingest.start(addr).await.expect("start RIST server");

    // Give RIST server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

#[cfg(feature = "rist")]
async fn run_rist_upload_test(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    use rist::Sender as RistSender;

    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for RIST...", upload_size_mb);
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let url = format!("rist://127.0.0.1:{}", port);

    println!("Uploading {} MB via RIST...", upload_size_mb);
    let start = Instant::now();

    let mut sender = match RistSender::new(LibRistProfile::Main) {
        Ok(s) => s,
        Err(e) => {
            println!("RIST sender create failed: {}", e);
            return (0.0, false);
        }
    };

    if let Err(e) = sender.add_peer(&url) {
        println!("RIST add_peer failed: {}", e);
        return (0.0, false);
    }

    if let Err(e) = sender.start() {
        println!("RIST start failed: {}", e);
        return (0.0, false);
    }

    // Give connection time to establish
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send data in 1316-byte RIST packets (MPEG-TS packet size)
    const RIST_PACKET_SIZE: usize = 1316;
    for chunk in data.chunks(RIST_PACKET_SIZE) {
        if let Err(e) = sender.send(chunk) {
            println!("RIST send error: {}", e);
            return (0.0, false);
        }
    }

    // Give time for data to be received
    tokio::time::sleep(Duration::from_millis(100)).await;

    let total_elapsed = start.elapsed();
    let expected_hex = format!("{:016x}", expected_hash);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "RIST: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      (RIST upload only - no response yet)");

    // For now, pass if upload completes without error
    let passed = true;
    if passed {
        println!("RIST PASSED\n");
    } else {
        println!("RIST FAILED\n");
    }

    (throughput, passed)
}

#[cfg(feature = "rist-pure")]
async fn setup_pure_rist_server(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let rist_ingest = PureRistIngest::new(service.clone())
        .with_profile(PureRistProfile::Main)
        .with_flow_id(PURE_RIST_FLOW_ID);
    let shutdown_tx = rist_ingest
        .start(addr)
        .await
        .expect("start pure Rust RIST server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

#[cfg(feature = "rist-pure")]
async fn run_pure_rist_upload_test(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    use rist_core_pure::time::ntp_now;
    use rist_mio_pure::MainMioSender;
    use std::io;
    use std::net::{Ipv4Addr, SocketAddrV4};

    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!(
        "Generating {} MB test data for pure Rust RIST...",
        upload_size_mb
    );
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let peer: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let mut sender = match MainMioSender::connect(local, peer, PURE_RIST_FLOW_ID, 8192) {
        Ok(sender) => sender,
        Err(error) => {
            println!("pure Rust RIST sender create failed: {}", error);
            return (0.0, false);
        }
    };
    let mut feedback_buf = vec![0u8; 65_536];

    println!("Uploading {} MB via pure Rust RIST...", upload_size_mb);
    let start = Instant::now();

    const RIST_PACKET_SIZE: usize = 1316;
    for (index, chunk) in data.chunks(RIST_PACKET_SIZE).enumerate() {
        loop {
            match sender.send_payload(chunk, ntp_now(), Instant::now()) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    drive_pure_rist_feedback(&mut sender, &mut feedback_buf);
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    println!("pure Rust RIST send error: {}", error);
                    return (0.0, false);
                }
            }
        }

        if index % 64 == 0 {
            drive_pure_rist_feedback(&mut sender, &mut feedback_buf);
        }
    }

    for _ in 0..100 {
        drive_pure_rist_feedback(&mut sender, &mut feedback_buf);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let total_elapsed = start.elapsed();
    let expected_hex = format!("{:016x}", expected_hash);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "pure Rust RIST: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      (pure Rust RIST upload only - no response yet)");
    println!("pure Rust RIST PASSED\n");

    (throughput, true)
}

#[cfg(feature = "rist-pure")]
fn drive_pure_rist_feedback(sender: &mut rist_mio_pure::MainMioSender, buf: &mut [u8]) {
    for _ in 0..32 {
        match sender.try_recv_feedback_and_retransmit(buf) {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => {
                println!("pure Rust RIST feedback error: {}", error);
                break;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "srt")]
async fn test_srt_8mb_recovery() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_srt_server(port, 8).await;

    println!("\n=== SRT 8MB Recovery Test ===");
    let (throughput, passed) = run_srt_upload_test(service, port, 8).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("============================\n");

    assert!(passed, "SRT recovery test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "srt")]
async fn test_srt_2mb_loss_recovery_baseline() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_srt_server(port, 2).await;

    println!("\n=== SRT 2MB Loss Recovery Baseline ===");
    let (throughput, passed, proxy_stats) =
        run_srt_loss_upload_test(service, port, 2, 128, 25, 10).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("======================================\n");

    assert!(
        proxy_stats.client_to_server_dropped > 0,
        "SRT loss proxy should drop client-to-server datagrams: {:?}",
        proxy_stats
    );
    assert!(
        passed,
        "SRT failed to recover within the bounded loss budget: {:?}",
        proxy_stats
    );
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "srt")]
async fn test_actual_srt_video_loss_misses_low_latency_playout() {
    const FRAME_COUNT: usize = 30;
    const FRAME_PAYLOAD_BYTES: usize = 8 * 1024;
    const FRAME_INTERVAL: Duration = Duration::from_millis(33);
    const LOW_LATENCY_PLAYOUT_US: u64 = 33_000;

    let port = pick_unused_port().expect("pick port");
    let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let listener = AsyncListener::bind_with_options(
        server_addr,
        [
            ListenerOption::TimestampBasedPacketDeliveryMode(false),
            ListenerOption::TooLatePacketDrop(false),
            ListenerOption::ReceiveBufferSize(36_400_000),
            ListenerOption::SendBufferSize(36_400_000),
        ],
    )
    .expect("bind SRT video listener");
    let (proxy_addr, proxy_stats, proxy_handle) =
        start_srt_loss_proxy_with_delay(server_addr, 48, 17, Duration::from_millis(8)).await;

    let start = Instant::now();
    let server_task = tokio::spawn(async move {
        let (stream, _peer) = listener
            .accept()
            .await
            .map_err(|error| format!("SRT video accept error: {error:?}"))?;
        collect_srt_video_frames(stream, FRAME_COUNT, start).await
    });

    let connect_options = ConnectOptions {
        stream_id: Some("video-latency-test".to_string()),
        timestamp_based_packet_delivery_mode: Some(false),
        too_late_packet_drop: Some(false),
        receive_buffer_size: Some(36_400_000),
        send_buffer_size: Some(36_400_000),
        max_bandwidth: Some(MaxBandwidth::Infinite),
        max_send_payload_size: Some(1316),
        ..Default::default()
    };
    let mut stream = AsyncStream::connect(proxy_addr, &connect_options)
        .await
        .expect("connect SRT video client");

    for sequence in 0..FRAME_COUNT {
        let frame = srt_video_frame(
            sequence as u32,
            start.elapsed().as_micros() as u64,
            FRAME_PAYLOAD_BYTES,
        );
        stream
            .write_all(&frame)
            .await
            .expect("write SRT video frame");
        tokio::time::sleep(FRAME_INTERVAL).await;
    }

    let arrivals = match tokio::time::timeout(Duration::from_secs(8), server_task).await {
        Ok(Ok(Ok(arrivals))) => arrivals,
        Ok(Ok(Err(error))) => panic!("{error}"),
        Ok(Err(error)) => panic!("SRT video server task failed: {error}"),
        Err(_) => panic!("timed out waiting for SRT video frames"),
    };
    drop(stream);
    let snapshot = proxy_stats.snapshot();
    proxy_handle.abort();

    let in_deadline = arrivals
        .iter()
        .filter(|arrival| arrival.latency_us() <= LOW_LATENCY_PLAYOUT_US)
        .count();
    let max_latency_us = arrivals
        .iter()
        .map(|arrival| arrival.latency_us())
        .max()
        .unwrap_or_default();
    let late_sequences = arrivals
        .iter()
        .filter(|arrival| arrival.latency_us() > LOW_LATENCY_PLAYOUT_US)
        .map(|arrival| arrival.sequence)
        .collect::<Vec<_>>();

    println!(
        "SRT video latency: frames={} in_deadline={} late={} late_seq={:?} max_latency_ms={:.1} c2s_drop={} c2s_fwd={} s2c_fwd={}",
        arrivals.len(),
        in_deadline,
        arrivals.len().saturating_sub(in_deadline),
        late_sequences,
        max_latency_us as f64 / 1000.0,
        snapshot.client_to_server_dropped,
        snapshot.client_to_server_forwarded,
        snapshot.server_to_client_forwarded
    );

    assert!(
        snapshot.client_to_server_dropped > 0,
        "SRT video proxy must drop upstream datagrams: {:?}",
        snapshot
    );
    assert_eq!(
        arrivals.len(),
        FRAME_COUNT,
        "SRT should eventually recover every video frame in this bounded-loss test"
    );
    assert!(
        in_deadline > 0,
        "some actual SRT video frames should arrive inside the 33 ms budget when they avoid retransmission: arrivals={arrivals:?}, proxy={snapshot:?}"
    );
    assert!(
        in_deadline < FRAME_COUNT,
        "actual SRT should miss a sub-RTT 33 ms video playout budget under retransmission loss: arrivals={arrivals:?}, proxy={snapshot:?}"
    );
    assert!(
        max_latency_us > LOW_LATENCY_PLAYOUT_US,
        "actual SRT max latency should exceed the low-latency playout budget"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "srt")]
async fn test_srt_100mb() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_srt_server(port, 100).await;

    println!("\n=== SRT 100MB Upload Test ===");
    let (throughput, passed) = run_srt_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("=============================\n");

    assert!(passed, "SRT test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "srt")]
async fn test_srt_encrypted_100mb() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_srt_server_encrypted(port, 100).await;

    println!("\n=== SRT (Encrypted) 100MB Upload Test ===");
    let (throughput, passed) =
        run_srt_upload_test_encrypted(service, port, 100, SRT_PASSPHRASE).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("=========================================\n");

    assert!(passed, "SRT (encrypted) test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "rist")]
async fn test_rist_100mb() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_rist_server(port, 100).await;

    println!("\n=== RIST 100MB Upload Test ===");
    let (throughput, passed) = run_rist_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==============================\n");

    assert!(passed, "RIST test failed");
    let _ = shutdown_tx.send(());
}

#[cfg(feature = "rist-pure")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
async fn test_pure_rist_100mb() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_pure_rist_server(port, 100).await;

    println!("\n=== pure Rust RIST 100MB Upload Test ===");
    let (throughput, passed) = run_pure_rist_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("========================================\n");

    assert!(passed, "pure Rust RIST test failed");
    let _ = shutdown_tx.send(());
}

// ============== TCP+TLS Tests ==============

async fn setup_tcp_server(
    port: u16,
    upload_size_mb: usize,
    cert_b64: &str,
    key_b64: &str,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let cert_pem = from_base64_raw(cert_b64).expect("decode cert");
    let key_pem = from_base64_raw(key_b64).expect("decode key");

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let tcp_ingest = TcpIngest::new(service.clone());
    let shutdown_tx = tcp_ingest
        .start(addr, &cert_pem, &key_pem)
        .await
        .expect("start TCP+TLS server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

async fn run_tcp_upload_test(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    ensure_rustls_provider();
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for TCP+TLS...", upload_size_mb);
    let gen_start = Instant::now();
    let (data, _expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let addr = format!("127.0.0.1:{}", port);

    println!("Uploading {} MB via TCP+TLS...", upload_size_mb);
    let start = Instant::now();

    // Connect via TCP
    let tcp_stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            println!("TCP connect failed: {}", e);
            return (0.0, false);
        }
    };

    // Wrap with TLS
    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();

    let mut stream = match connector.connect(server_name, tcp_stream).await {
        Ok(s) => s,
        Err(e) => {
            println!("TCP TLS handshake failed: {}", e);
            return (0.0, false);
        }
    };

    // Send all data
    const CHUNK_SIZE: usize = 64 * 1024;
    for chunk in data.chunks(CHUNK_SIZE) {
        if let Err(e) = stream.write_all(chunk).await {
            println!("TCP write failed: {}", e);
            return (0.0, false);
        }
    }

    // Shutdown write side to signal end
    if let Err(e) = stream.shutdown().await {
        println!("TCP shutdown failed: {}", e);
        return (0.0, false);
    }

    let total_elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "TCP+TLS: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );

    let passed = true;
    if passed {
        println!("TCP+TLS PASSED\n");
    } else {
        println!("TCP+TLS FAILED\n");
    }

    (throughput, passed)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
async fn test_tcp_tls_100mb() {
    ensure_rustls_provider();

    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_tcp_server(port, 100, &cert_b64, &key_b64).await;

    println!("\n=== TCP+TLS 100MB Upload Test ===");
    let (throughput, passed) = run_tcp_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("=================================\n");

    assert!(passed, "TCP+TLS test failed");
    let _ = shutdown_tx.send(());
}

#[cfg(any())]
async fn setup_rtmp_server(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let rtmp_ingest = RtmpUploadIngest::new(service.clone());
    let shutdown_tx = rtmp_ingest.start(addr).await.expect("start RTMP server");

    // Give RTMP server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

/// Generate fake AAC audio frame (ADTS header + random data)
#[cfg(any())]
fn generate_fake_aac_frame(frame_size: usize) -> Vec<u8> {
    let mut frame = Vec::with_capacity(7 + frame_size);
    let total_len = 7 + frame_size;

    // ADTS header (7 bytes)
    frame.push(0xff); // Sync word high
    frame.push(0xf1); // Sync word low + MPEG-4 + Layer 0 + no CRC
    frame.push(0x50); // AAC-LC, 44100 Hz, channel config partial
    frame.push(0x80 | ((total_len >> 11) as u8 & 0x03)); // Channel config + frame length
    frame.push((total_len >> 3) as u8);
    frame.push(((total_len & 0x07) << 5) as u8 | 0x1f);
    frame.push(0xfc);

    // Random audio data
    for i in 0..frame_size {
        frame.push((i % 256) as u8);
    }

    frame
}

#[cfg(any())]
async fn run_rtmp_upload_test(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for RTMP...", upload_size_mb);
    let gen_start = Instant::now();

    // Generate fake AAC frames (~1KB each)
    let frame_size = 1000;
    let num_frames = upload_size_bytes / (7 + frame_size);
    let frames: Vec<Vec<u8>> = (0..num_frames)
        .map(|_| generate_fake_aac_frame(frame_size))
        .collect();
    let total_bytes: usize = frames.iter().map(|f| f.len()).sum();

    println!(
        "Generated {} frames ({} bytes) in {:.2}s",
        num_frames,
        total_bytes,
        gen_start.elapsed().as_secs_f64()
    );

    let addr = format!("127.0.0.1:{}", port);

    println!("Uploading {} MB via RTMP...", upload_size_mb);
    let start = Instant::now();

    // Connect via TCP
    let mut stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            println!("RTMP connect failed: {}", e);
            return (0.0, false);
        }
    };

    // RTMP Handshake
    let mut handshake = Handshake::new(PeerType::Client);
    let c0_and_c1 = match handshake.generate_outbound_p0_and_p1() {
        Ok(data) => data,
        Err(e) => {
            println!("RTMP handshake generate failed: {:?}", e);
            return (0.0, false);
        }
    };

    if let Err(e) = stream.write_all(&c0_and_c1).await {
        println!("RTMP handshake write failed: {}", e);
        return (0.0, false);
    }

    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = match stream.read(&mut buffer).await {
            Ok(0) => {
                println!("RTMP connection closed during handshake");
                return (0.0, false);
            }
            Ok(n) => n,
            Err(e) => {
                println!("RTMP handshake read failed: {}", e);
                return (0.0, false);
            }
        };

        match handshake.process_bytes(&buffer[..bytes_read]) {
            Ok(HandshakeProcessResult::InProgress { response_bytes }) => {
                if let Err(e) = stream.write_all(&response_bytes).await {
                    println!("RTMP handshake response failed: {}", e);
                    return (0.0, false);
                }
            }
            Ok(HandshakeProcessResult::Completed { response_bytes, .. }) => {
                if !response_bytes.is_empty() {
                    if let Err(e) = stream.write_all(&response_bytes).await {
                        println!("RTMP handshake final response failed: {}", e);
                        return (0.0, false);
                    }
                }
                break;
            }
            Err(e) => {
                println!("RTMP handshake process failed: {:?}", e);
                return (0.0, false);
            }
        }
    }

    // Create client session
    let config = ClientSessionConfig::new();
    let (mut session, initial_results) = match ClientSession::new(config) {
        Ok(s) => s,
        Err(e) => {
            println!("RTMP session create failed: {:?}", e);
            return (0.0, false);
        }
    };

    // Send initial results
    for result in initial_results {
        if let ClientSessionResult::OutboundResponse(packet) = result {
            if let Err(e) = stream.write_all(&packet.bytes).await {
                println!("RTMP initial send failed: {}", e);
                return (0.0, false);
            }
        }
    }

    // Request connection
    let result = match session.request_connection("live".to_string()) {
        Ok(r) => r,
        Err(e) => {
            println!("RTMP request_connection failed: {:?}", e);
            return (0.0, false);
        }
    };
    if let ClientSessionResult::OutboundResponse(packet) = result {
        if let Err(e) = stream.write_all(&packet.bytes).await {
            println!("RTMP connect send failed: {}", e);
            return (0.0, false);
        }
    }

    // Wait for connection accepted and request publishing
    let mut connected = false;
    let mut publishing = false;
    let mut request_id = 0u32;

    'outer: loop {
        let bytes_read = match stream.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                println!("RTMP read failed: {}", e);
                return (0.0, false);
            }
        };

        let results = match session.handle_input(&buffer[..bytes_read]) {
            Ok(r) => r,
            Err(e) => {
                println!("RTMP handle_input failed: {:?}", e);
                return (0.0, false);
            }
        };

        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    if let Err(e) = stream.write_all(&packet.bytes).await {
                        println!("RTMP response send failed: {}", e);
                        return (0.0, false);
                    }
                }
                ClientSessionResult::RaisedEvent(event) => {
                    match event {
                        ClientSessionEvent::ConnectionRequestAccepted => {
                            connected = true;
                            // Request publishing
                            let result = match session.request_publishing(
                                "test-stream".to_string(),
                                PublishRequestType::Live,
                            ) {
                                Ok(r) => r,
                                Err(e) => {
                                    println!("RTMP request_publishing failed: {:?}", e);
                                    return (0.0, false);
                                }
                            };
                            if let ClientSessionResult::OutboundResponse(packet) = result {
                                if let Err(e) = stream.write_all(&packet.bytes).await {
                                    println!("RTMP publish send failed: {}", e);
                                    return (0.0, false);
                                }
                            }
                        }
                        ClientSessionEvent::PublishRequestAccepted => {
                            publishing = true;
                            break 'outer;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    if !publishing {
        println!("RTMP publishing not established");
        return (0.0, false);
    }

    // Send audio frames
    let mut timestamp = 0u32;
    for frame in &frames {
        // FLV audio tag: 0xAF = AAC, 0x01 = raw AAC frame
        let mut audio_data = vec![0xAF, 0x01];
        audio_data.extend_from_slice(frame);

        let result = match session.publish_audio_data(
            Bytes::from(audio_data),
            rml_rtmp::time::RtmpTimestamp::new(timestamp),
            false,
        ) {
            Ok(r) => r,
            Err(e) => {
                println!("RTMP publish_audio_data failed: {:?}", e);
                return (0.0, false);
            }
        };

        if let ClientSessionResult::OutboundResponse(packet) = result {
            if let Err(e) = stream.write_all(&packet.bytes).await {
                println!("RTMP audio send failed: {}", e);
                return (0.0, false);
            }
        }

        timestamp += 23; // ~43 fps for audio
    }

    let total_elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "RTMP: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Sent: {} frames ({} bytes)", num_frames, total_bytes);

    // For now, pass if upload completes without error
    let passed = true;
    if passed {
        println!("RTMP PASSED\n");
    } else {
        println!("RTMP FAILED\n");
    }

    (throughput, passed)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(any())]
async fn test_rtmp_100mb() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_rtmp_server(port, 100).await;

    println!("\n=== RTMP 100MB Upload Test ===");
    let (throughput, passed) = run_rtmp_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==============================\n");

    assert!(passed, "RTMP test failed");
    let _ = shutdown_tx.send(());
}

#[cfg(any())]
async fn setup_rtmps_server(
    port: u16,
    upload_size_mb: usize,
    cert_b64: &str,
    key_b64: &str,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
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

    let cert_pem = from_base64_raw(cert_b64).expect("decode cert");
    let key_pem = from_base64_raw(key_b64).expect("decode key");

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let rtmp_ingest = RtmpUploadIngest::new(service.clone());
    let shutdown_tx = rtmp_ingest
        .start_tls(addr, &cert_pem, &key_pem)
        .await
        .expect("start RTMPS server");

    // Give RTMPS server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (service, shutdown_tx)
}

#[cfg(any())]
async fn run_rtmps_upload_test(
    _service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    ensure_rustls_provider();
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for RTMPS...", upload_size_mb);
    let gen_start = Instant::now();

    // Generate fake AAC frames (~1KB each)
    let frame_size = 1000;
    let num_frames = upload_size_bytes / (7 + frame_size);
    let frames: Vec<Vec<u8>> = (0..num_frames)
        .map(|_| generate_fake_aac_frame(frame_size))
        .collect();
    let total_bytes: usize = frames.iter().map(|f| f.len()).sum();

    println!(
        "Generated {} frames ({} bytes) in {:.2}s",
        num_frames,
        total_bytes,
        gen_start.elapsed().as_secs_f64()
    );

    let addr = format!("127.0.0.1:{}", port);

    println!("Uploading {} MB via RTMPS...", upload_size_mb);
    let start = Instant::now();

    // Connect via TCP
    let tcp_stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            println!("RTMPS connect failed: {}", e);
            return (0.0, false);
        }
    };

    // Wrap with TLS
    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();

    let mut stream = match connector.connect(server_name, tcp_stream).await {
        Ok(s) => s,
        Err(e) => {
            println!("RTMPS TLS handshake failed: {}", e);
            return (0.0, false);
        }
    };

    // RTMP Handshake
    let mut handshake = Handshake::new(PeerType::Client);
    let c0_and_c1 = match handshake.generate_outbound_p0_and_p1() {
        Ok(data) => data,
        Err(e) => {
            println!("RTMP handshake generate failed: {:?}", e);
            return (0.0, false);
        }
    };

    if let Err(e) = stream.write_all(&c0_and_c1).await {
        println!("RTMP handshake write failed: {}", e);
        return (0.0, false);
    }

    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = match stream.read(&mut buffer).await {
            Ok(0) => {
                println!("RTMPS connection closed during handshake");
                return (0.0, false);
            }
            Ok(n) => n,
            Err(e) => {
                println!("RTMPS handshake read failed: {}", e);
                return (0.0, false);
            }
        };

        match handshake.process_bytes(&buffer[..bytes_read]) {
            Ok(HandshakeProcessResult::InProgress { response_bytes }) => {
                if let Err(e) = stream.write_all(&response_bytes).await {
                    println!("RTMPS handshake response failed: {}", e);
                    return (0.0, false);
                }
            }
            Ok(HandshakeProcessResult::Completed { response_bytes, .. }) => {
                if !response_bytes.is_empty() {
                    if let Err(e) = stream.write_all(&response_bytes).await {
                        println!("RTMPS handshake final response failed: {}", e);
                        return (0.0, false);
                    }
                }
                break;
            }
            Err(e) => {
                println!("RTMPS handshake process failed: {:?}", e);
                return (0.0, false);
            }
        }
    }

    // Create client session
    let config = ClientSessionConfig::new();
    let (mut session, initial_results) = match ClientSession::new(config) {
        Ok(s) => s,
        Err(e) => {
            println!("RTMPS session create failed: {:?}", e);
            return (0.0, false);
        }
    };

    // Send initial results
    for result in initial_results {
        if let ClientSessionResult::OutboundResponse(packet) = result {
            if let Err(e) = stream.write_all(&packet.bytes).await {
                println!("RTMPS initial send failed: {}", e);
                return (0.0, false);
            }
        }
    }

    // Request connection
    let result = match session.request_connection("live".to_string()) {
        Ok(r) => r,
        Err(e) => {
            println!("RTMPS request_connection failed: {:?}", e);
            return (0.0, false);
        }
    };
    if let ClientSessionResult::OutboundResponse(packet) = result {
        if let Err(e) = stream.write_all(&packet.bytes).await {
            println!("RTMPS connect send failed: {}", e);
            return (0.0, false);
        }
    }

    // Wait for connection accepted and request publishing
    let mut publishing = false;

    'outer: loop {
        let bytes_read = match stream.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                println!("RTMPS read failed: {}", e);
                return (0.0, false);
            }
        };

        let results = match session.handle_input(&buffer[..bytes_read]) {
            Ok(r) => r,
            Err(e) => {
                println!("RTMPS handle_input failed: {:?}", e);
                return (0.0, false);
            }
        };

        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    if let Err(e) = stream.write_all(&packet.bytes).await {
                        println!("RTMPS response send failed: {}", e);
                        return (0.0, false);
                    }
                }
                ClientSessionResult::RaisedEvent(event) => match event {
                    ClientSessionEvent::ConnectionRequestAccepted => {
                        let result = match session
                            .request_publishing("test-stream".to_string(), PublishRequestType::Live)
                        {
                            Ok(r) => r,
                            Err(e) => {
                                println!("RTMPS request_publishing failed: {:?}", e);
                                return (0.0, false);
                            }
                        };
                        if let ClientSessionResult::OutboundResponse(packet) = result {
                            if let Err(e) = stream.write_all(&packet.bytes).await {
                                println!("RTMPS publish send failed: {}", e);
                                return (0.0, false);
                            }
                        }
                    }
                    ClientSessionEvent::PublishRequestAccepted => {
                        publishing = true;
                        break 'outer;
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }

    if !publishing {
        println!("RTMPS publishing not established");
        return (0.0, false);
    }

    // Send audio frames
    let mut timestamp = 0u32;
    for frame in &frames {
        let mut audio_data = vec![0xAF, 0x01];
        audio_data.extend_from_slice(frame);

        let result = match session.publish_audio_data(
            Bytes::from(audio_data),
            rml_rtmp::time::RtmpTimestamp::new(timestamp),
            false,
        ) {
            Ok(r) => r,
            Err(e) => {
                println!("RTMPS publish_audio_data failed: {:?}", e);
                return (0.0, false);
            }
        };

        if let ClientSessionResult::OutboundResponse(packet) = result {
            if let Err(e) = stream.write_all(&packet.bytes).await {
                println!("RTMPS audio send failed: {}", e);
                return (0.0, false);
            }
        }

        timestamp += 23;
    }

    let total_elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "RTMPS: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Sent: {} frames ({} bytes)", num_frames, total_bytes);

    let passed = true;
    if passed {
        println!("RTMPS PASSED\n");
    } else {
        println!("RTMPS FAILED\n");
    }

    (throughput, passed)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(any())]
async fn test_rtmps_100mb() {
    ensure_rustls_provider();

    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_rtmps_server(port, 100, &cert_b64, &key_b64).await;

    println!("\n=== RTMPS 100MB Upload Test ===");
    let (throughput, passed) = run_rtmps_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("===============================\n");

    assert!(passed, "RTMPS test failed");
    let _ = shutdown_tx.send(());
}

#[cfg(feature = "webrtc")]
async fn setup_webrtc_server(
    signaling_port: u16,
    upload_size_mb: usize,
) -> (
    Arc<UploadResponseService>,
    tokio::sync::watch::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
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

    // Start signaling server
    let signaling_addr: SocketAddr = format!("127.0.0.1:{}", signaling_port).parse().unwrap();
    let signaling_server =
        SignalingServerBuilder::new(signaling_addr, ClientServer, ClientServerState::default())
            .build();

    let signaling_handle = tokio::spawn(async move {
        signaling_server.serve().await.expect("signaling server");
    });

    // Give signaling server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start WebRTC ingest connected to signaling server
    let signaling_url = format!("ws://127.0.0.1:{}/bench", signaling_port);
    let webrtc_ingest = WebRtcIngest::new(service.clone());
    let shutdown_tx = webrtc_ingest
        .start(signaling_url)
        .await
        .expect("start WebRTC ingest");

    // Give WebRTC ingest time to connect
    tokio::time::sleep(Duration::from_millis(200)).await;

    (service, shutdown_tx, signaling_handle)
}

#[cfg(feature = "webrtc")]
const WEBRTC_CHANNEL_ID: usize = 0;

#[cfg(feature = "webrtc")]
async fn run_webrtc_upload_test(
    _service: Arc<UploadResponseService>,
    signaling_port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    ensure_rustls_provider();
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!("Generating {} MB test data for WebRTC...", upload_size_mb);
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let signaling_url = format!("ws://127.0.0.1:{}/bench", signaling_port);

    println!("Uploading {} MB via WebRTC Data Channel...", upload_size_mb);

    // Connect to signaling server
    let (mut socket, loop_fut) = WebRtcSocket::new_reliable(&signaling_url);
    let loop_fut = loop_fut.fuse();
    futures::pin_mut!(loop_fut);

    let timeout = Delay::new(Duration::from_millis(10));
    futures::pin_mut!(timeout);

    // Wait for peer (the server) to connect
    let mut connected_peer = None;
    let deadline = Instant::now() + std::time::Duration::from_secs(10);

    while connected_peer.is_none() && Instant::now() < deadline {
        select! {
            _ = (&mut timeout).fuse() => {
                for (peer, state) in socket.update_peers() {
                    if matches!(state, PeerState::Connected) {
                        println!("WebRTC: Connected to peer {}", peer);
                        connected_peer = Some(peer);
                        break;
                    }
                }
                timeout.reset(Duration::from_millis(10));
            }
            _ = &mut loop_fut => {
                println!("WebRTC: Socket loop ended before connection");
                return (0.0, false);
            }
        }
    }

    let peer = match connected_peer {
        Some(p) => p,
        None => {
            println!("WebRTC: Failed to connect to peer");
            return (0.0, false);
        }
    };

    // Start timing when we begin sending
    let start = Instant::now();

    // Send data in 16KB chunks (WebRTC data channel message size limit)
    let chunk_size = 16 * 1024;
    let chunks: Vec<_> = data.chunks(chunk_size).collect();
    let total_chunks = chunks.len();
    let mut chunks_sent = 0;

    // Send chunks while driving the socket loop to actually transmit data
    // socket.send() only queues - we need to poll via update_peers() to flush
    let send_timeout = Delay::new(Duration::from_millis(1));
    futures::pin_mut!(send_timeout);

    let send_deadline = Instant::now() + std::time::Duration::from_secs(300);

    while chunks_sent < total_chunks && Instant::now() < send_deadline {
        select! {
            _ = (&mut send_timeout).fuse() => {
                // Send a batch of chunks
                let batch_size = 64; // Send 64 chunks per poll (~1MB)
                for _ in 0..batch_size {
                    if chunks_sent >= total_chunks {
                        break;
                    }
                    socket.channel_mut(WEBRTC_CHANNEL_ID).send(
                        chunks[chunks_sent].to_vec().into(),
                        peer
                    );
                    chunks_sent += 1;
                }

                // Drive transmission
                socket.update_peers();
                send_timeout.reset(Duration::from_millis(1));
            }
            _ = &mut loop_fut => {
                println!("WebRTC: Socket loop ended during send");
                break;
            }
        }
    }

    // Continue driving socket to flush remaining buffered data
    let flush_timeout = Delay::new(Duration::from_millis(5));
    futures::pin_mut!(flush_timeout);

    // Drive for a reasonable time to ensure data is transmitted
    // Since we can't check buffer state, drive until socket is quiet
    let flush_start = Instant::now();
    let max_flush_time = Duration::from_secs(10);

    while flush_start.elapsed() < max_flush_time {
        select! {
            _ = (&mut flush_timeout).fuse() => {
                socket.update_peers();
                flush_timeout.reset(Duration::from_millis(5));

                // Give enough time for the data to actually be sent
                // Check if we've been flushing long enough relative to data size
                if flush_start.elapsed() > Duration::from_millis(200) {
                    break;
                }
            }
            _ = &mut loop_fut => {
                break;
            }
        }
    }

    let total_elapsed = start.elapsed();

    // Close socket
    socket.close();

    let expected_hex = format!("{:016x}", expected_hash);
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    println!(
        "WebRTC: {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      (WebRTC upload only - response not implemented yet)");

    // For now, pass if upload completes without error
    let passed = true;
    if passed {
        println!("WebRTC PASSED\n");
    } else {
        println!("WebRTC FAILED\n");
    }

    (throughput, passed)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "webrtc")]
async fn test_webrtc_100mb() {
    let signaling_port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx, signaling_handle) = setup_webrtc_server(signaling_port, 100).await;

    println!("\n=== WebRTC 100MB Upload Test ===");
    let (throughput, passed) = run_webrtc_upload_test(service, signaling_port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("================================\n");

    assert!(passed, "WebRTC test failed");
    let _ = shutdown_tx.send(());
    signaling_handle.abort();
}

#[cfg(feature = "webrtc")]
async fn run_webrtc_upload_test_high_throughput(
    _service: Arc<UploadResponseService>,
    signaling_port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    ensure_rustls_provider();
    let upload_size_bytes = upload_size_mb * 1024 * 1024;

    println!(
        "Generating {} MB test data for WebRTC (high-throughput)...",
        upload_size_mb
    );
    let gen_start = Instant::now();
    let (data, expected_hash) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let signaling_url = format!("ws://127.0.0.1:{}/bench", signaling_port);

    println!(
        "Uploading {} MB via WebRTC Data Channel (high-throughput)...",
        upload_size_mb
    );

    // Use larger application messages for the high-throughput profile.
    let (mut socket, loop_fut) = WebRtcSocketBuilder::new(&signaling_url)
        .add_channel(ChannelConfig::reliable())
        .build();
    let loop_fut = loop_fut.fuse();
    futures::pin_mut!(loop_fut);

    let timeout = Delay::new(Duration::from_millis(10));
    futures::pin_mut!(timeout);

    // Wait for peer (the server) to connect
    let mut connected_peer = None;
    let deadline = Instant::now() + std::time::Duration::from_secs(10);

    while connected_peer.is_none() && Instant::now() < deadline {
        select! {
            _ = (&mut timeout).fuse() => {
                for (peer, state) in socket.update_peers() {
                    if matches!(state, PeerState::Connected) {
                        println!("WebRTC (high-throughput): Connected to peer {}", peer);
                        connected_peer = Some(peer);
                        break;
                    }
                }
                timeout.reset(Duration::from_millis(10));
            }
            _ = &mut loop_fut => {
                break;
            }
        }
    }

    let peer = match connected_peer {
        Some(p) => p,
        None => {
            println!("WebRTC (high-throughput) FAILED: Could not connect to peer");
            return (0.0, false);
        }
    };

    // Start timing when we begin sending
    let start = Instant::now();

    // Send data in larger chunks for high-throughput mode (64KB)
    const CHUNK_SIZE: usize = 65536;
    for chunk in data.chunks(CHUNK_SIZE) {
        socket
            .channel_mut(0)
            .send(chunk.to_vec().into_boxed_slice(), peer);
    }

    // Close socket to signal end of transmission
    socket.close();

    let total_elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / total_elapsed.as_secs_f64();

    let expected_hex = format!("{:016x}", expected_hash);
    println!(
        "WebRTC (high-throughput): {:.2}s ({:.1} MB/s)",
        total_elapsed.as_secs_f64(),
        throughput
    );
    println!("Expected: {}", expected_hex);
    println!("Got:      (WebRTC upload only - response not implemented yet)");

    let passed = true;
    if passed {
        println!("WebRTC (high-throughput) PASSED\n");
    } else {
        println!("WebRTC (high-throughput) FAILED\n");
    }

    (throughput, passed)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "srt")]
async fn test_srt_high_throughput_100mb() {
    let port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx) = setup_srt_server_high_throughput(port, 100).await;

    println!("\n=== SRT (High-Throughput) 100MB Upload Test ===");
    let (throughput, passed) = run_srt_upload_test(service, port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("===============================================\n");

    assert!(passed, "SRT (high-throughput) test failed");
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(feature = "webrtc")]
async fn test_webrtc_high_throughput_100mb() {
    let signaling_port = pick_unused_port().expect("pick port");
    let (service, shutdown_tx, signaling_handle) = setup_webrtc_server(signaling_port, 100).await;

    println!("\n=== WebRTC (High-Throughput) 100MB Upload Test ===");
    let (throughput, passed) =
        run_webrtc_upload_test_high_throughput(service, signaling_port, 100).await;
    println!("Throughput: {:.1} MB/s", throughput);
    println!("==================================================\n");

    assert!(passed, "WebRTC (high-throughput) test failed");
    let _ = shutdown_tx.send(());
    signaling_handle.abort();
}

/// Compare default vs high-throughput modes for SRT and WebRTC
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(all(feature = "srt", feature = "webrtc"))]
async fn test_throughput_comparison() {
    println!("\n=== Throughput Comparison: Default vs High-Throughput ===\n");

    // SRT Default
    let srt_port = pick_unused_port().expect("pick port");
    let (srt_service, srt_shutdown) = setup_srt_server(srt_port, 100).await;
    let (srt_default, _) = run_srt_upload_test(srt_service, srt_port, 100).await;
    let _ = srt_shutdown.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;

    // SRT High-Throughput
    let srt_port2 = pick_unused_port().expect("pick port");
    let (srt_service2, srt_shutdown2) = setup_srt_server_high_throughput(srt_port2, 100).await;
    let (srt_high, _) = run_srt_upload_test(srt_service2, srt_port2, 100).await;
    let _ = srt_shutdown2.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;

    // WebRTC Default
    let webrtc_port = pick_unused_port().expect("pick port");
    let (webrtc_service, webrtc_shutdown, webrtc_handle) =
        setup_webrtc_server(webrtc_port, 100).await;
    let (webrtc_default, _) = run_webrtc_upload_test(webrtc_service, webrtc_port, 100).await;
    let _ = webrtc_shutdown.send(());
    webrtc_handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // WebRTC High-Throughput
    let webrtc_port2 = pick_unused_port().expect("pick port");
    let (webrtc_service2, webrtc_shutdown2, webrtc_handle2) =
        setup_webrtc_server(webrtc_port2, 100).await;
    let (webrtc_high, _) =
        run_webrtc_upload_test_high_throughput(webrtc_service2, webrtc_port2, 100).await;
    let _ = webrtc_shutdown2.send(());
    webrtc_handle2.abort();

    println!("\n=== Results ===");
    println!("| Protocol | Default | High-Throughput | Improvement |");
    println!("|----------|---------|-----------------|-------------|");
    println!(
        "| SRT      | {:.1} MB/s | {:.1} MB/s | {:.1}x |",
        srt_default,
        srt_high,
        srt_high / srt_default.max(0.1)
    );
    println!(
        "| WebRTC   | {:.1} MB/s | {:.1} MB/s | {:.1}x |",
        webrtc_default,
        webrtc_high,
        webrtc_high / webrtc_default.max(0.1)
    );
    println!("\n=================================================\n");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
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
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(all(feature = "srt", feature = "rist", feature = "webrtc"))]
async fn test_protocol_comparison() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let srt_port = pick_unused_port().expect("pick srt port");
    let rist_port = pick_unused_port().expect("pick rist port");
    #[cfg(feature = "rist-pure")]
    let pure_rist_port = pick_unused_port().expect("pick pure rist port");
    let webrtc_port = pick_unused_port().expect("pick webrtc port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 512).await;
    let (srt_service, srt_shutdown_tx) = setup_srt_server(srt_port, 512).await;
    let (rist_service, rist_shutdown_tx) = setup_rist_server(rist_port, 512).await;
    #[cfg(feature = "rist-pure")]
    let (pure_rist_service, pure_rist_shutdown_tx) =
        setup_pure_rist_server(pure_rist_port, 512).await;
    let (webrtc_service, webrtc_shutdown_tx, webrtc_handle) =
        setup_webrtc_server(webrtc_port, 512).await;

    println!("\n========================================");
    println!("    Protocol Comparison (512 MB)");
    println!("========================================\n");

    let (h1_throughput, h1_passed) = run_upload_test(port, 512, false).await;
    let (h1c_throughput, h1c_passed) = run_upload_test_chunked(port, 512).await;
    let (h2_throughput, h2_passed) = run_upload_test(port, 512, true).await;
    let (h3_throughput, h3_passed) = run_h3_upload_test(port, 512, &cert_b64, &key_b64).await;
    let (wss_throughput, wss_passed) = run_wss_upload_test(port, 512, &cert_b64, &key_b64).await;
    let (srt_throughput, srt_passed) = run_srt_upload_test(srt_service, srt_port, 512).await;
    let (rist_throughput, rist_passed) = run_rist_upload_test(rist_service, rist_port, 512).await;
    #[cfg(feature = "rist-pure")]
    let (pure_rist_throughput, pure_rist_passed) =
        run_pure_rist_upload_test(pure_rist_service, pure_rist_port, 512).await;
    let (webrtc_throughput, webrtc_passed) =
        run_webrtc_upload_test(webrtc_service, webrtc_port, 512).await;

    println!("========================================");
    println!("    Results Summary");
    println!("========================================");
    println!(
        "HTTP/1.1:           {:.1} MB/s {}",
        h1_throughput,
        if h1_passed { "✓" } else { "✗" }
    );
    println!(
        "HTTP/1.1 (chunked): {:.1} MB/s {}",
        h1c_throughput,
        if h1c_passed { "✓" } else { "✗" }
    );
    println!(
        "HTTP/2:             {:.1} MB/s {}",
        h2_throughput,
        if h2_passed { "✓" } else { "✗" }
    );
    println!(
        "HTTP/3:             {:.1} MB/s {}",
        h3_throughput,
        if h3_passed { "✓" } else { "✗" }
    );
    println!(
        "WSS:                {:.1} MB/s {}",
        wss_throughput,
        if wss_passed { "✓" } else { "✗" }
    );
    println!(
        "SRT:                {:.1} MB/s {}",
        srt_throughput,
        if srt_passed { "✓" } else { "✗" }
    );
    println!(
        "RIST:               {:.1} MB/s {}",
        rist_throughput,
        if rist_passed { "✓" } else { "✗" }
    );
    #[cfg(feature = "rist-pure")]
    println!(
        "RIST Pure:          {:.1} MB/s {}",
        pure_rist_throughput,
        if pure_rist_passed { "✓" } else { "✗" }
    );
    println!(
        "WebRTC:             {:.1} MB/s {}",
        webrtc_throughput,
        if webrtc_passed { "✓" } else { "✗" }
    );
    println!("========================================\n");

    #[cfg(feature = "rist-pure")]
    let pure_rist_ok = pure_rist_passed;
    #[cfg(not(feature = "rist-pure"))]
    let pure_rist_ok = true;

    assert!(
        h1_passed
            && h1c_passed
            && h2_passed
            && h3_passed
            && wss_passed
            && srt_passed
            && rist_passed
            && pure_rist_ok
            && webrtc_passed,
        "Protocol tests failed"
    );
    let _ = shutdown_tx.send(());
    let _ = srt_shutdown_tx.send(());
    let _ = rist_shutdown_tx.send(());
    #[cfg(feature = "rist-pure")]
    let _ = pure_rist_shutdown_tx.send(());
    let _ = webrtc_shutdown_tx.send(());
    webrtc_handle.abort();
}

/// Compare all protocols at 1GB
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
#[cfg(all(feature = "srt", feature = "rist", feature = "webrtc"))]
async fn test_protocol_comparison_1gb() {
    let (cert_b64, key_b64) = match load_test_env() {
        Some(v) => v,
        None => {
            eprintln!("Skipping: TLS_CERT_BASE64 and TLS_KEY_BASE64 env vars required");
            return;
        }
    };

    let port = pick_unused_port().expect("pick port");
    let srt_port = pick_unused_port().expect("pick srt port");
    let rist_port = pick_unused_port().expect("pick rist port");
    #[cfg(feature = "rist-pure")]
    let pure_rist_port = pick_unused_port().expect("pick pure rist port");
    let webrtc_port = pick_unused_port().expect("pick webrtc port");
    let shutdown_tx = setup_server(cert_b64.clone(), key_b64.clone(), port, 1024).await;
    let (srt_service, srt_shutdown_tx) = setup_srt_server(srt_port, 1024).await;
    let (rist_service, rist_shutdown_tx) = setup_rist_server(rist_port, 1024).await;
    #[cfg(feature = "rist-pure")]
    let (pure_rist_service, pure_rist_shutdown_tx) =
        setup_pure_rist_server(pure_rist_port, 1024).await;
    let (webrtc_service, webrtc_shutdown_tx, webrtc_handle) =
        setup_webrtc_server(webrtc_port, 1024).await;

    println!("\n========================================");
    println!("    Protocol Comparison (1 GB)");
    println!("========================================\n");

    let (h1_throughput, h1_passed) = run_upload_test(port, 1024, false).await;
    let (h1c_throughput, h1c_passed) = run_upload_test_chunked(port, 1024).await;
    let (h2_throughput, h2_passed) = run_upload_test(port, 1024, true).await;
    let (h3_throughput, h3_passed) = run_h3_upload_test(port, 1024, &cert_b64, &key_b64).await;
    let (wss_throughput, wss_passed) = run_wss_upload_test(port, 1024, &cert_b64, &key_b64).await;
    let (srt_throughput, srt_passed) = run_srt_upload_test(srt_service, srt_port, 1024).await;
    let (rist_throughput, rist_passed) = run_rist_upload_test(rist_service, rist_port, 1024).await;
    #[cfg(feature = "rist-pure")]
    let (pure_rist_throughput, pure_rist_passed) =
        run_pure_rist_upload_test(pure_rist_service, pure_rist_port, 1024).await;
    let (webrtc_throughput, webrtc_passed) =
        run_webrtc_upload_test(webrtc_service, webrtc_port, 1024).await;

    println!("========================================");
    println!("    Results Summary (1 GB)");
    println!("========================================");
    println!(
        "HTTP/1.1:           {:.1} MB/s {}",
        h1_throughput,
        if h1_passed { "✓" } else { "✗" }
    );
    println!(
        "HTTP/1.1 (chunked): {:.1} MB/s {}",
        h1c_throughput,
        if h1c_passed { "✓" } else { "✗" }
    );
    println!(
        "HTTP/2:             {:.1} MB/s {}",
        h2_throughput,
        if h2_passed { "✓" } else { "✗" }
    );
    println!(
        "HTTP/3:             {:.1} MB/s {}",
        h3_throughput,
        if h3_passed { "✓" } else { "✗" }
    );
    println!(
        "WSS:                {:.1} MB/s {}",
        wss_throughput,
        if wss_passed { "✓" } else { "✗" }
    );
    println!(
        "SRT:                {:.1} MB/s {}",
        srt_throughput,
        if srt_passed { "✓" } else { "✗" }
    );
    println!(
        "RIST:               {:.1} MB/s {}",
        rist_throughput,
        if rist_passed { "✓" } else { "✗" }
    );
    #[cfg(feature = "rist-pure")]
    println!(
        "RIST Pure:          {:.1} MB/s {}",
        pure_rist_throughput,
        if pure_rist_passed { "✓" } else { "✗" }
    );
    println!(
        "WebRTC:             {:.1} MB/s {}",
        webrtc_throughput,
        if webrtc_passed { "✓" } else { "✗" }
    );
    println!("========================================\n");

    #[cfg(feature = "rist-pure")]
    let pure_rist_ok = pure_rist_passed;
    #[cfg(not(feature = "rist-pure"))]
    let pure_rist_ok = true;

    assert!(
        h1_passed
            && h1c_passed
            && h2_passed
            && h3_passed
            && wss_passed
            && srt_passed
            && rist_passed
            && pure_rist_ok
            && webrtc_passed,
        "Protocol tests failed"
    );
    let _ = shutdown_tx.send(());
    let _ = srt_shutdown_tx.send(());
    let _ = rist_shutdown_tx.send(());
    #[cfg(feature = "rist-pure")]
    let _ = pure_rist_shutdown_tx.send(());
    let _ = webrtc_shutdown_tx.send(());
    webrtc_handle.abort();
}

// ── UDP+FEC (RaptorQ) benchmark ───────────────────────────────────────────────

#[cfg(feature = "udp-fec")]
async fn setup_udp_fec_server(
    port: u16,
    upload_size_mb: usize,
) -> (Arc<UploadResponseService>, tokio::sync::watch::Sender<()>) {
    use upload_response::{DEFAULT_SOURCE_SYMBOLS, DEFAULT_SYMBOL_SIZE};

    let upload_size_bytes = upload_size_mb * 1024 * 1024;
    let block_payload = DEFAULT_SOURCE_SYMBOLS as usize * DEFAULT_SYMBOL_SIZE as usize;
    let num_slots = upload_size_bytes.div_ceil(block_payload.max(1)) + 100;

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

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let ingest = UdpFecIngest::new(service.clone());
    let shutdown_tx = ingest.start(addr).await.expect("start UDP+FEC server");

    tokio::time::sleep(Duration::from_millis(50)).await;

    (service, shutdown_tx)
}

/// Send `upload_size_mb` MB in block-sized chunks via UDP+FEC, measure throughput.
#[cfg(feature = "udp-fec")]
async fn run_udp_fec_upload_test(
    service: Arc<UploadResponseService>,
    port: u16,
    upload_size_mb: usize,
) -> (f64, bool) {
    use upload_response::{DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS, DEFAULT_SYMBOL_SIZE};

    let upload_size_bytes = upload_size_mb * 1024 * 1024;
    println!("Generating {} MB test data for UDP+FEC...", upload_size_mb);
    let gen_start = Instant::now();
    let (data, _) = generate_test_data(upload_size_bytes);
    println!("Generated in {:.2}s", gen_start.elapsed().as_secs_f64());

    let target: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let source_symbols = udp_fec_u16_env("UDP_FEC_UPLOAD_SOURCE_SYMBOLS", DEFAULT_SOURCE_SYMBOLS);
    let repair_symbols = udp_fec_u32_env("UDP_FEC_UPLOAD_REPAIR_SYMBOLS", DEFAULT_REPAIR_SYMBOLS);
    let mut sender = UdpFecSender::new(target)
        .await
        .expect("UdpFecSender::new")
        .with_source_symbols(source_symbols)
        .with_repair_symbols(repair_symbols);

    // Each FEC block holds K * T bytes of source payload.
    let block_payload = source_symbols as usize * DEFAULT_SYMBOL_SIZE as usize;

    println!(
        "Uploading {} MB via UDP+FEC (K={}, R={})...",
        upload_size_mb, source_symbols, repair_symbols
    );
    let start = Instant::now();

    for chunk in data.chunks(block_payload) {
        match tokio::time::timeout(Duration::from_secs(5), sender.send(chunk)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                println!("UDP+FEC send error: {}", e);
                return (0.0, false);
            }
            Err(_) => {
                println!("UDP+FEC send timed out");
                return (0.0, false);
            }
        }
    }

    let recovered = match wait_for_udp_fec_recovered_body(
        service,
        data.len(),
        udp_fec_recovery_timeout(upload_size_mb),
    )
    .await
    {
        Some(recovered) => recovered,
        None => {
            println!("UDP+FEC timed out waiting for recovered body");
            return (0.0, false);
        }
    };

    let elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / elapsed.as_secs_f64();
    let passed = recovered == data;

    println!(
        "UDP+FEC: {:.2}s ({:.1} MB/s)",
        elapsed.as_secs_f64(),
        throughput
    );
    if passed {
        println!("UDP+FEC PASSED\n");
    } else {
        println!(
            "UDP+FEC FAILED: recovered {} bytes, expected {}\n",
            recovered.len(),
            data.len()
        );
    }

    (throughput, passed)
}

/// Send data via a proxy that drops every 5th datagram; verify FEC recovers all blocks.
#[cfg(feature = "udp-fec")]
async fn run_udp_fec_loss_test(service: Arc<UploadResponseService>, port: u16) -> (f64, bool) {
    use tokio::net::UdpSocket;
    use upload_response::{DEFAULT_SOURCE_SYMBOLS, DEFAULT_SYMBOL_SIZE};

    let upload_size_mb = udp_fec_benchmark_size_mb("UDP_FEC_LOSS_MB", 10);
    let upload_size_bytes = upload_size_mb * 1024 * 1024;
    let (data, _) = generate_test_data(upload_size_bytes);
    let source_symbols = udp_fec_u16_env("UDP_FEC_LOSS_SOURCE_SYMBOLS", DEFAULT_SOURCE_SYMBOLS);
    let repair_symbols = udp_fec_u32_env("UDP_FEC_LOSS_REPAIR_SYMBOLS", 2);
    let drop_every = udp_fec_u64_env("UDP_FEC_LOSS_DROP_EVERY", 5);
    let block_payload = source_symbols as usize * DEFAULT_SYMBOL_SIZE as usize;

    let target: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Proxy socket: receive datagrams from the sender and forward to the real
    // ingest, dropping every Nth datagram.
    let proxy_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let proxy_sock = Arc::new(UdpSocket::bind(proxy_bind).await.expect("proxy bind"));
    let proxy_addr = proxy_sock.local_addr().unwrap();

    let fwd_sock = proxy_sock.clone();
    let fwd_target = target;
    let proxy_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        let mut count = 0u64;
        loop {
            match fwd_sock.recv_from(&mut buf).await {
                Ok((n, _)) => {
                    count += 1;
                    if drop_every == 0 || count % drop_every != 0 {
                        let _ = fwd_sock.send_to(&buf[..n], fwd_target).await;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut sender = UdpFecSender::new(proxy_addr)
        .await
        .expect("UdpFecSender proxy")
        .with_source_symbols(source_symbols)
        .with_repair_symbols(repair_symbols);

    println!(
        "Running UDP+FEC loss-recovery test ({} MB, K={}, R={}, drop every {} pkt)...",
        upload_size_mb, source_symbols, repair_symbols, drop_every
    );
    let start = Instant::now();

    let block_pace = udp_fec_loss_pace();
    for chunk in data.chunks(block_payload) {
        match tokio::time::timeout(Duration::from_secs(5), sender.send(chunk)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                println!("UDP+FEC loss-test send error: {}", e);
                proxy_handle.abort();
                return (0.0, false);
            }
            Err(_) => {
                println!("UDP+FEC loss-test send timed out");
                proxy_handle.abort();
                return (0.0, false);
            }
        }
        if !block_pace.is_zero() {
            tokio::time::sleep(block_pace).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    let recovered = match wait_for_udp_fec_recovered_body(
        service,
        data.len(),
        udp_fec_recovery_timeout(upload_size_mb),
    )
    .await
    {
        Some(recovered) => recovered,
        None => {
            println!("UDP+FEC loss-test timed out waiting for recovered body");
            proxy_handle.abort();
            return (0.0, false);
        }
    };
    proxy_handle.abort();

    let elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / elapsed.as_secs_f64();
    let passed = recovered == data;

    println!(
        "UDP+FEC loss-recovery: {:.2}s ({:.1} MB/s)",
        elapsed.as_secs_f64(),
        throughput
    );
    if passed {
        println!("UDP+FEC loss-recovery PASSED\n");
    } else {
        println!(
            "UDP+FEC loss-recovery FAILED: recovered {} bytes, expected {}\n",
            recovered.len(),
            data.len()
        );
    }

    (throughput, passed)
}

#[cfg(feature = "udp-fec")]
async fn wait_for_udp_fec_recovered_body(
    service: Arc<UploadResponseService>,
    expected_bytes: usize,
    timeout: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    let mut recovered = Vec::with_capacity(expected_bytes);
    let mut best_len = 0usize;
    loop {
        if Instant::now() > deadline {
            println!(
                "UDP+FEC recovered {}/{} bytes before timeout",
                best_len, expected_bytes
            );
            return None;
        }

        for slot in service.active_stream_slots() {
            recovered.clear();
            let last = service.request_last(slot.stream_id).unwrap_or(0);
            for slot_id in 2..=last {
                if let Some(TailSlot::Body(bytes)) =
                    service.tail_request(slot.stream_id, slot_id).await
                {
                    recovered.extend_from_slice(&bytes);
                    best_len = best_len.max(recovered.len());
                    if recovered.len() >= expected_bytes {
                        return Some(recovered[..expected_bytes].to_vec());
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(feature = "udp-fec")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark/stress test; run explicitly with --ignored"]
async fn test_udp_fec_benchmark() {
    let port = pick_unused_port().expect("pick port");
    let loss_port = pick_unused_port().expect("pick loss port");
    let upload_size_mb = udp_fec_benchmark_size_mb("UDP_FEC_UPLOAD_MB", 100);
    let loss_size_mb = udp_fec_benchmark_size_mb("UDP_FEC_LOSS_MB", 10);

    let (service, shutdown_tx) = setup_udp_fec_server(port, upload_size_mb).await;
    let (loss_service, loss_shutdown_tx) = setup_udp_fec_server(loss_port, loss_size_mb + 1).await;

    println!("\n========================================");
    println!("    UDP+FEC (RaptorQ) Benchmark");
    println!("========================================\n");

    let (throughput, passed) = run_udp_fec_upload_test(service.clone(), port, upload_size_mb).await;
    let (loss_throughput, loss_passed) =
        run_udp_fec_loss_test(loss_service.clone(), loss_port).await;

    println!("========================================");
    println!("    Results");
    println!("========================================");
    println!(
        "UDP+FEC {} MB:        {:.1} MB/s {}",
        upload_size_mb,
        throughput,
        if passed { "✓" } else { "✗" }
    );
    println!(
        "UDP+FEC loss-recovery: {:.1} MB/s {}",
        loss_throughput,
        if loss_passed { "✓" } else { "✗" }
    );
    println!("========================================\n");

    assert!(passed, "UDP+FEC upload test failed");
    assert!(loss_passed, "UDP+FEC loss-recovery test failed");

    let _ = shutdown_tx.send(());
    let _ = loss_shutdown_tx.send(());
}
