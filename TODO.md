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
