# Design, correctness, and scalability review

Review date: 2026-08-22

## Executive decision

Keep the current architecture. Do not rewrite it.

The previous implementation selected useful concurrency primitives. However, its completion report did not match the behavior under sustained traffic and slot reuse.

This review retained the useful structure and corrected the unsafe lifecycle paths. The result is suitable for continued development and controlled deployment.

Three production gates remain open. Secure the internal worker API, enforce response ownership, and complete a production soak test.

## Reviewed revisions

| Component | Reviewed revision |
| --- | --- |
| `playlists` | `c5bb8856` |
| `rist-rs` | `44f7d430` |
| `web-services` | This change set after `066bf4b` |

The `rist-rs` revision is the current upstream `main` revision. Crates.io also reports `wavey-rist 0.1.0` and `rist-sys 0.1.1` as current.

## Architecture

The design converts each client operation into bounded cache lanes. Workers read request lanes and publish results through response lanes.

```text
client
  -> protocol server
  -> UploadResponseRouter
  -> request ring
  -> worker stages
  -> response ring
  -> ResponseWatcher
  -> client
```

Each logical request has a `stream_id`. The service maps that identifier to a reusable physical slot.

Each physical slot has one lifecycle lock. Request, response, and named stage lanes use the same slot identity.

Readers publish their consumed positions. Writers use the slowest position to prevent an overwrite of unread data.

The design has four unusual strengths:

- It connects network ingress and external workers without one large request allocation.
- It uses the same cache model for HLS media, upload processing, and intermediate worker stages.
- It supports many ingress protocols without changing the worker data format.
- It permits zero-copy `Bytes` sharing after data enters a cache slot.

The design also has important costs:

- One logical stream has ordered writes, so one hot stream cannot scale across writers.
- A stalled reader applies backpressure to its complete lane.
- Slot reuse requires strict identity checks at every asynchronous boundary.
- Worker coordination currently assumes a trusted internal network.
- Protocol adapters do not all have identical session and shutdown semantics.

## Assessment of the previous implementation

It was worth retaining, but it was not safe to accept unchanged.

I would choose its bounded RIST queue, dedicated receive thread, per-slot lock, and event-driven response watcher. These choices fit the workload.

I would not choose its original completion boundary. It missed stale-slot races, false startup readiness, and a sustained RIST CPU failure.

The implementation also completed damaged RIST input as a normal request. That behavior could send truncated media to a worker.

The revised implementation now aborts damaged streams. It opens a new stream only after a valid post-failure packet arrives.

## Changes in this review

| Area | Change | Result |
| --- | --- | --- |
| Cache identity | Use generation-safe stream handles in HLS and chunk routes. | A stale request cannot read a reused logical stream. |
| HLS waits | Replace polling with cache notifications and exact-part waiters. | Blocking reloads do not wake every millisecond. |
| Segment reads | Validate the stream identity before and after asynchronous reads. | Slot reuse cannot splice bytes from another stream. |
| Upload slots | Serialize writes per physical slot and fence close/open operations. | An old writer cannot cross into a replacement stream. |
| Admission | Add nonblocking stream admission for HTTP and RIST. | Full capacity returns `503` or drops new RIST admission. |
| Backpressure | Keep reader progress independent from the writer lock. | A blocked writer does not prevent its reader from advancing. |
| Response watcher | Return an owned task handle that aborts on drop. | A forgotten watcher does not retain the service forever. |
| Proxy worker | Replace the one-millisecond poll with notifications. | Idle workers do not consume periodic CPU. |
| Proxy ring | Detect a reader that falls behind retained data. | The worker does not replay overwritten frames as current data. |
| Server startup | Report readiness after every listener binds successfully. | `start()` now returns occupied-port and TLS startup errors. |
| Server limits | Add connection limits, handshake timeouts, and task supervision. | Slow handshakes and completed tasks have bounded retention. |
| HTTP/2 | Advertise a 256-stream limit for each connection. | One connection cannot create unlimited concurrent streams. |
| RIST queue | Bound the handoff queue and expose lock-free metrics. | Receiver memory has a configured packet limit. |
| RIST bytes | Replace front-draining `Vec` work with `BytesMut::split_to`. | Slot batching no longer shifts pending bytes repeatedly. |
| RIST peers | Keep one ordered request per source address. | Concurrent senders do not share one sequence space. |
| RIST failure | Abort on queue loss or an unresolved sequence gap. | A damaged byte stream never receives a normal end marker. |
| RIST core | Remove per-packet session construction and empty loss scans. | Sustained Main Profile receive throughput no longer collapses. |
| Test TLS | Generate a fresh local certificate for each test process. | Benchmarks do not depend on an expired repository certificate. |
| Metrics | Measure observed request latency and actual payload size. | Benchmark labels now match their measured values. |

## Measured results

These local measurements used release builds on an eight-core Apple Silicon host. They are engineering controls, not production capacity claims.

### Playlist cache

| Workload | Workers | Rate | Sampled p50 | Sampled p99 | CPU cores | Failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Independent handle writes | 1 | 130,835 writes/s | 3.96 us | 10.83 us | 0.70 | 0 |
| Independent handle writes | 8 | 606,000 writes/s | 4.38 us | 24.38 us | 3.35 | 0 |
| Fifteen reads per write | 1 | 1.94 million operations/s | 0.08 us | 7.29 us | 0.70 | 0 |
| Fifteen reads per write | 8 | 4.98 million operations/s | 0.29 us | 8.17 us | 3.12 | 0 |
| Cached delta read on one stream | 1 | 46.64 million reads/s | 0.04 us | 0.08 us | 0.98 | 0 |
| Cached delta read on one stream | 8 | 12.13 million reads/s | 0.58 us | 1.08 us | 6.32 | 0 |

The shared hot read loses aggregate throughput because all cores contend for the same cache lines. The absolute rate remains high.

Independent playlist writes reached 109,083 operations/s at eight workers. They used approximately 6.82 million allocation calls during the one-second sample.

One hot playlist reached 43,964 operations/s with one worker. It fell to 29,878 operations/s with eight contending writers.

The hot playlist result is expected for ordered writes. The allocation rate is not expected and is the main remaining playlist CPU target.

The latency sampler collected few hot-write samples. Do not use its tail values for a service objective.

### HTTP server

The test used 128 persistent clients. Each response contained 65,536 bytes.

| Protocol | Requests/s | Payload rate | Observed mean response time |
| --- | ---: | ---: | ---: |
| HTTP/1.1 | 14,698 | 918.65 MiB/s | 8.47 ms |
| HTTP/2 | 9,596 | 599.76 MiB/s | 13.17 ms |

The complete client and server process used 55.4 MB maximum RSS. It used 5.52 CPU-seconds during 2.63 wall-clock seconds.

This memory value includes clients, TLS, runtimes, and the server. It is not a server-only measurement.

### Proxy request writes

The test used 64 clients, 63 backend workers, and 65,536-byte request bodies.

| Upstream protocol | Requests/s | Request payload rate | Observed mean response time |
| --- | ---: | ---: | ---: |
| HTTP/1.1 | 3,293 | 205.81 MiB/s | 19.37 ms |
| HTTP/2 | 3,711 | 231.96 MiB/s | 17.19 ms |

The combined client, proxy, and 63 in-process backends used 296.2 MB maximum RSS. This value includes every test component.

### RIST

The real librist interoperability test transferred 66,560 packets and 87,592,960 bytes. It crossed the 16-bit sequence boundary.

The transfer completed in 28.033 seconds at 24.997 Mbit/s. It reported zero missing packets, duplicates, or recovery errors.

The web-services regression transferred 8,448 packets and approximately 11.1 MB in 5.43 seconds. The debug build reconstructed every byte.

The upstream CPU defect created a new session and two 8,192-entry loss trackers for each packet. Empty loss scans also touched every tracker slot.

Revision `44f7d430` creates state only for a new peer or flow. It also skips loss scans when no packet is missing.

## CPU and memory interpretation

The chunk rings retain `Bytes` values. A write normally replaces one pointer-backed value and does not copy its complete payload.

Ring metadata is allocated during cache construction. Payload capacity is logical and fills only when writers publish distinct byte allocations.

The default upload configuration permits 512 MiB of logical request payload and 512 MiB of logical response payload. Stage lanes add separate capacity.

This logical total is not immediate RSS. However, a valid workload can fill it, so deployment limits must include the complete lane count.

The principal CPU costs are manifest rendering, TLS, proxy hashing, and shared hot-stream contention. Periodic one-millisecond polling is no longer used in HLS or proxy workers.

The pure RIST receiver still polls an idle nonblocking socket each millisecond. That loop can cause approximately 1,000 idle wakeups each second.

## Production gates

### P0: Isolate and authenticate the internal worker API

The `/_upload_response` routes currently share the public router. The routes do not authenticate a worker.

Do not expose these routes on a public listener. A network policy is necessary, but it is not sufficient authentication.

Recommended implementation:

1. Create a separate `UploadResponseControlRouter` for all `/_upload_response` routes.
2. Add a configurable bind address to `ServerConfig`.
3. Run the control router on a private HTTP/2 listener.
4. Add a client CA option to the internal listener.
5. Build a rustls `WebPkiClientVerifier` from that CA.
6. Put the verified certificate identity in each request extension.
7. Require that identity on every reader, claim, heartbeat, and write operation.
8. Keep the public router unable to match the internal prefix.
9. Add Kubernetes `NetworkPolicy` rules as a second boundary.
10. Test an absent certificate, an unknown certificate, and a valid worker certificate.

A bearer token can provide a short transition. Do not make an unauthenticated mode the production default.

### P0: Enforce response ownership on every write

`try_claim_response` records a worker name. The response write routes do not prove that the caller owns that claim.

Recommended implementation:

1. Return a random 256-bit capability from the claim operation.
2. Store its SHA-256 digest with the worker name and stream generation.
3. Require the capability in a request header, not in the URL.
4. Compare the digest in constant time.
5. Validate the lease inside `UploadResponseService`, not only inside the router.
6. Require the lease for response headers, body writes, and the end marker.
7. Revoke the lease on release, timeout, stream close, or slot reuse.
8. Add a monotonically increasing response write sequence.
9. Make an exact retry idempotent and reject a conflicting retry.
10. Test stale capabilities after slot reuse and concurrent writes from two workers.

### P0: Complete a production soak test

Run the test for at least one hour after a ten-minute warmup. Use twice the expected steady connection count.

Include stalled readers, canceled clients, slow workers, worker restarts, stage churn, and forced RIST queue overflow.

Record RSS, allocation rate, task count, file descriptors, CPU, queue depth, p99 latency, and every stream identity error.

Fail the test after any byte mismatch or stale-slot access. Also fail if RSS, task count, or file descriptors grow without a plateau.

## Dependency advisories

GitHub currently reports seven open Dependabot alerts. They include two critical, one high, one medium, and three low alerts.

The critical alerts affect `failure 0.1.8` through `xmpegts` and `bytesio`. The flaw requires a hostile in-process `Fail` implementation.

Derived `Fail` implementations do not trigger that flaw. Remote media cannot define a Rust trait implementation, but the crate is unsupported.

The high alert affects `rustls-webpki 0.101.7`. Its malformed-CRL panic requires optional CRL checking, which this service does not enable.

The medium alert affects `opentelemetry_sdk 0.31.0` through `tokio-quiche`. This code does not install its vulnerable baggage propagator.

The remaining low alerts affect legacy certificate name constraints and `atty`. These findings do not change the controlled saturation test.

Recommended implementation:

1. Replace `failure` in `xmpegts` and `bytesio`, or maintain patched Wavey forks.
2. Upgrade the legacy rustls dependency chain in `message-packetizer`, `http-pack`, and `matchbox_socket`.
3. Upgrade `tokio-quiche` when it accepts `opentelemetry_sdk 0.32.1` or later.
4. Reject `baggage` headers larger than 8,192 bytes until that upgrade is available.
5. Replace `structopt` with `clap 4` and remove the unmaintained `atty` dependency.
6. Add a dependency audit to the required continuous-integration gate.

Resolve each alert or record a reviewed reachability exception before public production deployment.

## P1 work

### Split timeout purposes

`response_timeout_ms` controls application responses and cache backpressure. These events need different operating limits.

Add `response_deadline_ms`, `reader_backpressure_timeout_ms`, `stream_admission_timeout_ms`, and `remote_io_timeout_ms`. Preserve the old field during one migration release.

### Validate memory configuration before allocation

`UploadResponseService::new` normalizes zero values, but extreme trusted values can panic or exhaust memory.

Add `UploadResponseService::try_new`. Use checked multiplication for streams, slots, lanes, and slot bytes.

Return the logical maximum and estimated metadata bytes in the validation error. Keep `new` only as a compatibility wrapper.

### Bound all request and upgraded-session work

HTTP/2 now limits each connection to 256 streams. A global request limit is still necessary across all connections.

Add a global request semaphore to each server transport. Reject excess buffered work with `503` and retain flow-control backpressure for streaming bodies.

Transfer the TCP connection permit to an upgraded WebSocket task. Track that task until shutdown instead of detaching it.

Add tests for connection upgrades, global overload, client disconnects, and shutdown with active streams.

### Remove lifecycle bypasses

Raw cache getters let local callers bypass stream identity and lane locks. Mark these APIs as deprecated.

Add generation-safe lane handles for request, response, and stage access. Remove raw access in the next breaking release.

### Replace the RIST idle poll

Expose socket readiness from `rist-mio`, or add a blocking poll method with an RTCP deadline.

Wake the receiver thread for socket input, keepalive work, or shutdown. Keep the bounded channel and current overflow metrics.

### Supervise completion tasks

RIST request completion currently creates detached tasks. Their count is bounded indirectly by stream capacity, but shutdown does not own them.

Store completion work in a `JoinSet` or `TaskTracker`. Abort or drain it during shutdown with a configured deadline.

### Support multiple WebTransport sessions explicitly

The Quinn H3 handler returns after one WebTransport session. That return closes the owning H3 connection.

Confirm whether one session per connection is a product rule. Otherwise, drive session tasks while the connection continues to accept requests.

## P2 optimization work

### Reduce playlist rendering allocations

Profile `M3u8Manifest::add_part` and its render functions with allocation stacks. The benchmark shows approximately 62 allocation calls for each playlist write.

Keep one reusable render buffer per stream. Precompute invariant tags and use direct integer formatting for changing fields.

Retain the exact output bytes as the compatibility test. Require lower allocation counts before accepting a more complex renderer.

### Reduce shared hot-read cache traffic

One cached playlist value is faster on one core than on eight contending cores. Shared atomic and lock cache lines cause this result.

Measure per-core snapshot replication before changing the cache. Use replication only when the read fan-out justifies its memory cost.

### Separate service-only memory measurements

The current benchmark runs clients and servers in one process. Add an external load generator and collect server RSS independently.

Record steady RSS after warmup and after client churn. Report payload bytes separately from allocator and protocol memory.

## Verification

The `playlists` all-feature library suite passed 95 tests. Its strict all-target Clippy pass has no warnings.

The `rist-rs` all-feature workspace passed 187 unit tests. Its strict all-target Clippy pass has no warnings.

The web-services workspace passed all regular unit, integration, smoke, and documentation tests. Stress benchmarks remain explicit opt-in tests.

Use these commands for the final local gate:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
```

## Final recommendation

Do not redo the implementation. Keep the cache-lane design and the corrected lifecycle model.

Do not treat the internal worker interface as a public service. Complete both security gates before that interface crosses a trust boundary.

After those gates, prioritize the soak test. Use its evidence to set deployment limits before further micro-optimization.
