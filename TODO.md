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

## Listener streaming merge

The measurements are in `docs/listener-streaming-benchmarks.md`.

### Planned work

- [ ] Find the cause of the short HTTP/2 result at 5,000 and 10,000 streams on
  `inline-streaming-nodelay` (`8a211ec`). Against `main`, TTFB p50 is 2.8-3.0
  times higher and peak RSS is 1.9 times higher.
- [ ] Find the cause of the `listener-nodelay` (`400b5c9`) throughput decrease
  of 19.4% for `short-h2-c10000`.
- [ ] Find the cause of the echo results on `inline-streaming-nodelay`.
  Throughput decreases 22.9% for `echo-h1-c1000`, 9.7% for `echo-h2-c64` and
  9.2% for `echo-h2old-c64`.
- [ ] For each change, run a focused set of about 20 decision scenarios with
  `benchmarks/streaming/run.sh` and `ONLY` or `SCENARIO_FILE`.
- [ ] Before the merge into `main`, run the full matrix: 56 scenarios, 3 trees,
  3 repeats (504 runs).

### Open questions

- [ ] Decide whether a send-first echo larger than N MB is a requirement. On
  HTTP/2 with one stream on each connection, `main` completes 3 MiB and
  `inline-streaming-nodelay` completes 2 MiB. All trees time out at 4 MiB.
