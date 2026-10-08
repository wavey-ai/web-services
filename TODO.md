# TODO

Findings from the local Kanu Rust API migration review on September 21, 2026.
These items concern the upstream libraries.

## Completed work

- [x] Replace expired local TLS files with a local certificate generation script.
  `scripts/generate-local-tls.sh` preserves the local CA when it issues a new server certificate.
  Generated certificates and private keys are stored in a directory that Git ignores.
- [x] Add notification waits for active streams and request or stage slots.
  `watch_active_streams()` observes publication, ownership changes, closure, and response lease expiry.
  `UploadLaneHandle::wait_for_slot()` checks the stream identity before it returns data.
- [x] Enforce the local response claim contract.
  `claim_response_writer()` returns a writer with a capability for checked writes.
  Existing local write methods reject streams that have had a claim.
  Explicit `_unchecked` methods remain available for adapters with one writer per stream.
- [x] Add response lease renewal for long local work.
  `response_claim_lease_ms` controls the lease independently of response delivery timeouts.
  Successful writes renew ownership. `ClaimedResponseWriter::renew()` renews ownership before output starts.
  Expired or replaced writers cannot write or renew their claims.

## Deferred work

- [ ] Add a reusable streaming response reader when a local job consumer requires it.
  HTTP response delivery already streams through `proxy_streaming_response_with`.
  Keep an explicit size limit for consumers that collect a complete response.

## Transport scope

The server continues to require TLS. Cleartext server support is excluded at the user's request.

## Listener streaming

The listener runs streaming handlers inline, sets `TCP_NODELAY`, and batches
HTTP/2 writes below TLS. The measurements are in
`docs/listener-streaming-benchmarks.md`.

### Planned work

- [ ] Reduce the TCP segments of an HTTP/1.1 response under `TCP_NODELAY`.
  `echo-h1-c1000` has 156 segments per stream against 86 before the merge, and
  its throughput decreases 22.9%. `WriteBatch` covers HTTP/2 connections only.
  `http1::Builder::writev(false)` (branch `h1-flat-writes`) does not recover
  it: `echo-h1-c1000` gives 13,700 streams/s against 14,365 without it, and
  `short-h1-c1000` and `short-h1-c10000` decrease 8% and 11% (one repeat,
  October 8, 2026).
- [ ] Find the cause of the 9.7% throughput decrease of `echo-h2-c64` and the
  9.2% decrease of `echo-h2old-c64`. The decrease starts with HTTP/2 write
  batching. With the h2 0.4.19 client, `echo-h2-c64` has 3,000 to 3,400
  `too_many_data_frames` errors in each window on all trees.
- [ ] Find the cause of the short HTTP/2 result at 10,000 streams: throughput
  decreases 3.6%, and TTFB p50 increases from 7.7 ms to 23.0 ms. At 5,000
  streams, throughput increases 5.6%, and TTLB p99 decreases from 51.4 ms to
  31.1 ms.

### Open questions

- [ ] Decide whether a send-first echo larger than N MB is a requirement. On
  HTTP/2 with one stream on each connection, the listener completes 2 MiB.
  Before the merge it completed 3 MiB. All trees time out at 4 MiB.
