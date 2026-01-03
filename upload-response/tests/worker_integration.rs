use bytes::Bytes;
use futures_util::stream;
use http::{Request, StatusCode};
use http_pack::stream::{StreamHeaders, StreamRequestHeaders, StreamResponseHeaders};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{interval, Duration};
use upload_response::{
    ResponseWatcher, TailSlot, UploadResponseConfig, UploadResponseRouter, UploadResponseService,
};
use web_service::Router;
use xxhash_rust::xxh64::xxh64;

/// Simulates a worker that:
/// 1. Tails request streams for new data
/// 2. Reads headers and body chunks
/// 3. Computes xxhash of the body
/// 4. Writes response with the hash
async fn run_worker(service: Arc<UploadResponseService>, stream_id: u64) {
    let mut poll = interval(Duration::from_millis(1));
    let mut last_slot = 0usize;
    let mut body_chunks: Vec<Bytes> = Vec::new();

    loop {
        poll.tick().await;

        // Check for new slots
        let current_last = service.request_last(stream_id).unwrap_or(0);
        if current_last <= last_slot {
            continue;
        }

        // Process new slots
        for slot_id in (last_slot + 1)..=current_last {
            match service.tail_request(stream_id, slot_id).await {
                Some(TailSlot::Headers(_h)) => {
                    // Headers available - could inspect method, path, etc.
                }
                Some(TailSlot::Body(data)) => {
                    body_chunks.push(data);
                }
                Some(TailSlot::End) => {
                    // Compute xxhash of concatenated body
                    let total_len: usize = body_chunks.iter().map(|c| c.len()).sum();
                    let mut body = Vec::with_capacity(total_len);
                    for chunk in &body_chunks {
                        body.extend_from_slice(chunk);
                    }
                    let hash = xxh64(&body, 0);

                    // Write response with hash
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

                    // Body is the hash as hex string
                    let hash_body = format!("{:016x}", hash);
                    service
                        .append_response_body(stream_id, Bytes::from(hash_body))
                        .await
                        .unwrap();

                    service.end_response(stream_id).await.unwrap();
                    return;
                }
                None => {}
            }
        }
        last_slot = current_last;
    }
}

#[tokio::test]
async fn test_worker_computes_xxhash() {
    let config = UploadResponseConfig {
        num_streams: 10,
        slot_size_kb: 64,
        slots_per_stream: 100,
        response_timeout_ms: 5000,
    };
    let service = Arc::new(UploadResponseService::new(config));

    // Start response watcher
    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    let stream_id = 0u64;

    // Register response channel before worker starts
    let rx = service.register_response(stream_id).await;

    // Start worker
    let worker_service = service.clone();
    let worker_handle = tokio::spawn(async move {
        run_worker(worker_service, stream_id).await;
    });

    // Write request
    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: http_pack::HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: Some(b"example.com".to_vec()),
        path: b"/upload".to_vec(),
        headers: vec![],
    });
    service
        .write_request_headers(stream_id, headers)
        .await
        .unwrap();

    // Write body in chunks
    let body_part1 = b"hello ";
    let body_part2 = b"world";
    service
        .append_request_body(stream_id, Bytes::from_static(body_part1))
        .await
        .unwrap();
    service
        .append_request_body(stream_id, Bytes::from_static(body_part2))
        .await
        .unwrap();

    // End request
    service.end_request(stream_id).await.unwrap();

    // Wait for response
    let result = tokio::time::timeout(Duration::from_secs(5), rx).await;
    let (status, body) = result.unwrap().unwrap().unwrap();

    // Verify response
    assert_eq!(status, StatusCode::OK);

    // Compute expected hash
    let full_body = b"hello world";
    let expected_hash = xxh64(full_body, 0);
    let expected_hex = format!("{:016x}", expected_hash);

    assert_eq!(body, Bytes::from(expected_hex));

    // Cleanup
    worker_handle.await.unwrap();
}

#[tokio::test]
async fn test_worker_handles_large_body() {
    let config = UploadResponseConfig {
        num_streams: 10,
        slot_size_kb: 1, // 1KB slots to force multiple chunks
        slots_per_stream: 1000,
        response_timeout_ms: 5000,
    };
    let service = Arc::new(UploadResponseService::new(config));

    // Start response watcher
    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    let stream_id = 0u64;

    // Register response channel
    let rx = service.register_response(stream_id).await;

    // Start worker
    let worker_service = service.clone();
    let worker_handle = tokio::spawn(async move {
        run_worker(worker_service, stream_id).await;
    });

    // Write request headers
    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: http_pack::HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: b"/large".to_vec(),
        headers: vec![],
    });
    service
        .write_request_headers(stream_id, headers)
        .await
        .unwrap();

    // Write large body in chunks (10KB total, split across 1KB slots)
    let chunk_size = 1024;
    let num_chunks = 10;
    let mut full_body = Vec::new();

    for i in 0..num_chunks {
        let chunk: Vec<u8> = (0..chunk_size).map(|j| ((i * chunk_size + j) % 256) as u8).collect();
        full_body.extend_from_slice(&chunk);
        service
            .append_request_body(stream_id, Bytes::from(chunk))
            .await
            .unwrap();
    }

    // End request
    service.end_request(stream_id).await.unwrap();

    // Wait for response
    let result = tokio::time::timeout(Duration::from_secs(5), rx).await;
    let (status, body) = result.unwrap().unwrap().unwrap();

    assert_eq!(status, StatusCode::OK);

    // Verify hash
    let expected_hash = xxh64(&full_body, 0);
    let expected_hex = format!("{:016x}", expected_hash);
    assert_eq!(body, Bytes::from(expected_hex));

    worker_handle.await.unwrap();
}

#[tokio::test]
async fn test_worker_handles_empty_body() {
    let config = UploadResponseConfig::default();
    let service = Arc::new(UploadResponseService::new(config));

    // Start response watcher
    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    let stream_id = 0u64;

    // Register response channel
    let rx = service.register_response(stream_id).await;

    // Start worker
    let worker_service = service.clone();
    let worker_handle = tokio::spawn(async move {
        run_worker(worker_service, stream_id).await;
    });

    // Write request with no body
    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: http_pack::HttpVersion::Http11,
        method: b"GET".to_vec(),
        scheme: None,
        authority: None,
        path: b"/empty".to_vec(),
        headers: vec![],
    });
    service
        .write_request_headers(stream_id, headers)
        .await
        .unwrap();

    // End request immediately (no body)
    service.end_request(stream_id).await.unwrap();

    // Wait for response
    let result = tokio::time::timeout(Duration::from_secs(5), rx).await;
    let (status, body) = result.unwrap().unwrap().unwrap();

    assert_eq!(status, StatusCode::OK);

    // Hash of empty body
    let expected_hash = xxh64(&[], 0);
    let expected_hex = format!("{:016x}", expected_hash);
    assert_eq!(body, Bytes::from(expected_hex));

    worker_handle.await.unwrap();
}

/// Streaming worker that computes hash incrementally without buffering entire body
async fn run_streaming_worker(service: Arc<UploadResponseService>, stream_id: u64) {
    let mut poll = interval(Duration::from_micros(100));
    let mut last_slot = 0usize;
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);

    loop {
        poll.tick().await;

        let current_last = service.request_last(stream_id).unwrap_or(0);
        if current_last <= last_slot {
            continue;
        }

        for slot_id in (last_slot + 1)..=current_last {
            match service.tail_request(stream_id, slot_id).await {
                Some(TailSlot::Headers(_h)) => {}
                Some(TailSlot::Body(data)) => {
                    hasher.update(&data);
                }
                Some(TailSlot::End) => {
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

                    let hash_body = format!("{:016x}", hash);
                    service
                        .append_response_body(stream_id, Bytes::from(hash_body))
                        .await
                        .unwrap();

                    service.end_response(stream_id).await.unwrap();
                    return;
                }
                None => {}
            }
        }
        last_slot = current_last;
    }
}

/// Run a benchmark with configurable slot size and upload size
async fn run_upload_benchmark(slot_size_kb: usize, upload_size_mb: usize) -> f64 {
    let upload_size_bytes = upload_size_mb * 1024 * 1024;
    let slot_size_bytes = slot_size_kb * 1024;
    let num_slots = upload_size_bytes / slot_size_bytes + 10;

    let config = UploadResponseConfig {
        num_streams: 4,
        slot_size_kb,
        slots_per_stream: num_slots,
        response_timeout_ms: 300_000,
    };
    let service = Arc::new(UploadResponseService::new(config));

    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    let stream_id = 0u64;
    let rx = service.register_response(stream_id).await;

    let worker_service = service.clone();
    let worker_handle = tokio::spawn(async move {
        run_streaming_worker(worker_service, stream_id).await;
    });

    let mut expected_hasher = xxhash_rust::xxh64::Xxh64::new(0);

    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: http_pack::HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: b"/benchmark".to_vec(),
        headers: vec![],
    });
    service
        .write_request_headers(stream_id, headers)
        .await
        .unwrap();

    let start = Instant::now();
    let mut bytes_written = 0usize;

    while bytes_written < upload_size_bytes {
        let remaining = upload_size_bytes - bytes_written;
        let chunk_size = remaining.min(slot_size_bytes);

        let chunk: Vec<u8> = (0..chunk_size)
            .map(|i| ((bytes_written + i) % 256) as u8)
            .collect();

        expected_hasher.update(&chunk);
        service
            .append_request_body(stream_id, Bytes::from(chunk))
            .await
            .unwrap();

        bytes_written += chunk_size;
    }

    service.end_request(stream_id).await.unwrap();

    let expected_hash = expected_hasher.digest();
    let expected_hex = format!("{:016x}", expected_hash);

    let result = tokio::time::timeout(Duration::from_secs(300), rx).await;
    let (status, body) = result.unwrap().unwrap().unwrap();

    let elapsed = start.elapsed();
    let throughput = upload_size_mb as f64 / elapsed.as_secs_f64();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), expected_hex.as_bytes());

    worker_handle.await.unwrap();

    throughput
}

#[tokio::test]
async fn test_slot_size_benchmark() {
    const UPLOAD_SIZE_MB: usize = 512; // Use 512MB for faster iteration

    let slot_sizes_kb = [16, 32, 64, 100, 128, 256, 512, 768, 1024, 1536, 2048, 4096];

    println!("\n=== Slot Size Throughput Benchmark ===");
    println!("Upload size: {} MB", UPLOAD_SIZE_MB);
    println!("{:>12} | {:>12} | {:>12}", "Slot Size", "Throughput", "Slots Used");
    println!("{:-<12}-+-{:-<12}-+-{:-<12}", "", "", "");

    let mut results = Vec::new();

    for &slot_kb in &slot_sizes_kb {
        let throughput = run_upload_benchmark(slot_kb, UPLOAD_SIZE_MB).await;
        let slots_used = (UPLOAD_SIZE_MB * 1024) / slot_kb;

        println!(
            "{:>10} KB | {:>9.1} MB/s | {:>12}",
            slot_kb, throughput, slots_used
        );

        results.push((slot_kb, throughput));
    }

    // Find best slot size
    let (best_slot, best_throughput) = results
        .iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();

    println!("{:-<12}-+-{:-<12}-+-{:-<12}", "", "", "");
    println!("Best: {} KB @ {:.1} MB/s", best_slot, best_throughput);
    println!("======================================\n");
}

#[tokio::test]
async fn test_gigabyte_upload_benchmark() {
    const SLOT_SIZE_KB: usize = 64; // Default slot size
    const UPLOAD_SIZE_MB: usize = 1024;

    println!("\n=== Gigabyte Upload Benchmark ===");
    println!("Upload size: {} MB", UPLOAD_SIZE_MB);
    println!("Slot size: {} KB", SLOT_SIZE_KB);
    println!("Total slots: {}", (UPLOAD_SIZE_MB * 1024) / SLOT_SIZE_KB);

    let throughput = run_upload_benchmark(SLOT_SIZE_KB, UPLOAD_SIZE_MB).await;

    println!("Throughput: {:.1} MB/s", throughput);
    println!("=================================\n");
}

#[tokio::test]
async fn test_100mb_upload_validation() {
    const SLOT_SIZE_KB: usize = 64;
    const UPLOAD_SIZE_MB: usize = 100;
    const UPLOAD_SIZE_BYTES: usize = UPLOAD_SIZE_MB * 1024 * 1024;
    const SLOT_SIZE_BYTES: usize = SLOT_SIZE_KB * 1024;
    const NUM_SLOTS: usize = UPLOAD_SIZE_BYTES / SLOT_SIZE_BYTES + 10;

    let config = UploadResponseConfig {
        num_streams: 4,
        slot_size_kb: SLOT_SIZE_KB,
        slots_per_stream: NUM_SLOTS,
        response_timeout_ms: 60_000,
    };
    let service = Arc::new(UploadResponseService::new(config));

    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    let stream_id = 0u64;
    let rx = service.register_response(stream_id).await;

    let worker_service = service.clone();
    let worker_handle = tokio::spawn(async move {
        run_streaming_worker(worker_service, stream_id).await;
    });

    let headers = StreamHeaders::Request(StreamRequestHeaders {
        stream_id,
        version: http_pack::HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: b"/100mb-upload".to_vec(),
        headers: vec![],
    });
    service
        .write_request_headers(stream_id, headers)
        .await
        .unwrap();

    let mut expected_hasher = xxhash_rust::xxh64::Xxh64::new(0);
    let start = Instant::now();
    let mut bytes_written = 0usize;

    while bytes_written < UPLOAD_SIZE_BYTES {
        let remaining = UPLOAD_SIZE_BYTES - bytes_written;
        let chunk_size = remaining.min(SLOT_SIZE_BYTES);

        let chunk: Vec<u8> = (0..chunk_size)
            .map(|i| ((bytes_written + i) % 256) as u8)
            .collect();

        expected_hasher.update(&chunk);
        service
            .append_request_body(stream_id, Bytes::from(chunk))
            .await
            .unwrap();

        bytes_written += chunk_size;
    }

    service.end_request(stream_id).await.unwrap();

    let expected_hash = expected_hasher.digest();
    let expected_hex = format!("{:016x}", expected_hash);

    let result = tokio::time::timeout(Duration::from_secs(60), rx).await;
    let (status, body) = result.unwrap().unwrap().unwrap();

    let elapsed = start.elapsed();
    println!(
        "\n100MB upload: {:.2}s ({:.1} MB/s), hash: {}",
        elapsed.as_secs_f64(),
        UPLOAD_SIZE_MB as f64 / elapsed.as_secs_f64(),
        expected_hex
    );

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(expected_hex));

    worker_handle.await.unwrap();
}

/// Worker that processes ALL streams (not just one specific stream_id)
async fn run_multi_stream_worker(service: Arc<UploadResponseService>) {
    let mut poll = interval(Duration::from_micros(100));
    let num_streams = service.config().num_streams;
    let mut last_seen: Vec<usize> = vec![0; num_streams];
    let mut assemblies: std::collections::HashMap<u64, (Option<StreamRequestHeaders>, Vec<Bytes>)> =
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
                    Some(TailSlot::Headers(h)) => {
                        assemblies.insert(stream_id, (Some(h), Vec::new()));
                    }
                    Some(TailSlot::Body(data)) => {
                        if let Some((_, ref mut chunks)) = assemblies.get_mut(&stream_id) {
                            chunks.push(data);
                        }
                    }
                    Some(TailSlot::End) => {
                        if let Some((_, chunks)) = assemblies.remove(&stream_id) {
                            // Compute hash
                            let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
                            for chunk in &chunks {
                                hasher.update(chunk);
                            }
                            let hash = hasher.digest();

                            // Write response
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
                                .append_response_body(stream_id, Bytes::from(format!("{:016x}", hash)))
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

/// Test the full Router path - this is what H1/H2/H3 servers use
#[tokio::test]
async fn test_router_route_body() {
    let config = UploadResponseConfig {
        num_streams: 10,
        slot_size_kb: 64,
        slots_per_stream: 100,
        response_timeout_ms: 5000,
    };
    let service = Arc::new(UploadResponseService::new(config));
    let router = UploadResponseRouter::new(service.clone());

    // Start response watcher
    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    // Start multi-stream worker
    let worker_service = service.clone();
    let _worker_handle = tokio::spawn(async move {
        run_multi_stream_worker(worker_service).await;
    });

    // Create HTTP request
    let req = Request::builder()
        .method("POST")
        .uri("/upload")
        .body(())
        .unwrap();

    // Create body stream
    let body_data = b"Hello from the Router test!";
    let expected_hash = xxh64(body_data, 0);
    let expected_hex = format!("{:016x}", expected_hash);

    let body_stream: web_service::BodyStream = Box::pin(stream::iter(vec![
        Ok(Bytes::from(&body_data[..14])), // "Hello from the"
        Ok(Bytes::from(&body_data[14..])), // " Router test!"
    ]));

    // Call route_body - this is what H1/H2/H3 handlers call
    let response = router.route_body(req, body_stream).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body.unwrap(), Bytes::from(expected_hex));
}

/// Test multiple concurrent requests through the Router
#[tokio::test]
async fn test_router_concurrent_requests() {
    let config = UploadResponseConfig {
        num_streams: 10,
        slot_size_kb: 64,
        slots_per_stream: 100,
        response_timeout_ms: 5000,
    };
    let service = Arc::new(UploadResponseService::new(config));
    let router = Arc::new(UploadResponseRouter::new(service.clone()));

    // Start response watcher
    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher_handle = watcher.spawn();

    // Start multi-stream worker
    let worker_service = service.clone();
    let _worker_handle = tokio::spawn(async move {
        run_multi_stream_worker(worker_service).await;
    });

    // Spawn 5 concurrent requests
    let mut handles = Vec::new();

    for i in 0..5 {
        let router = router.clone();
        let handle = tokio::spawn(async move {
            let req = Request::builder()
                .method("POST")
                .uri(format!("/upload/{}", i))
                .body(())
                .unwrap();

            let body_data = format!("Request body {}", i);
            let expected_hash = xxh64(body_data.as_bytes(), 0);
            let expected_hex = format!("{:016x}", expected_hash);

            let body_stream: web_service::BodyStream =
                Box::pin(stream::iter(vec![Ok(Bytes::from(body_data))]));

            let response = router.route_body(req, body_stream).await.unwrap();

            assert_eq!(response.status, StatusCode::OK);
            assert_eq!(response.body.unwrap(), Bytes::from(expected_hex));
            i
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    let mut completed = Vec::new();
    for handle in handles {
        completed.push(handle.await.unwrap());
    }

    // All 5 requests completed
    assert_eq!(completed.len(), 5);
    println!("\n5 concurrent Router requests completed successfully\n");
}
