# Response streaming

How a response travels from a worker back to a client, which hops stream, which
buffer, and what each layer guarantees. Written because "do we support response
streaming?" has four different answers depending on which hop you mean.

## The four hops

A request/response cycle through `upload-response` crosses four boundaries. All
four stream for HTTP traffic.

| Hop | Streams? | Where |
| --- | --- | --- |
| Client → request cache | yes | each `BodyStream` chunk is appended to a slot as it arrives (`UploadResponseRouter::copy_request_body`) |
| Request cache → worker | yes | `tail_request` slot by slot, woken by the lane notifier |
| Worker → response cache | yes | `append_response_body` writes one slot per call; `response_update_notifier` fires per write |
| Response cache → client | yes, for HTTP | `proxy_streaming_response_with` tails the response lane and writes each slot to the peer (`upload-response/src/bridge.rs`) |

The transport was never the constraint. Streaming responses are implemented on
all three backends: `web-service/src/h2.rs`, `web-service/src/h3.rs` (quinn),
and `web-service/src/h3_tokio_quiche.rs`.

## Which router hook, and why

Public HTTP traffic goes through `has_body_stream_handler` → `route_body_stream`,
**not** `is_streaming` → `route_stream`.

This matters: `route_stream` receives only a `Request<()>` and a `StreamWriter`.
It has no request body, because every backend checks `is_streaming` *instead of*
`has_body_handler`. Routing upload-response through it would stream responses
beautifully and silently discard every upload. `route_body_stream` is the only
hook that carries both a request `BodyStream` and a response `StreamWriter`, and
all three backends check it ahead of the other two.

## What still buffers, deliberately

`ResponseWatcher` and the `register_response` oneshot are still the delivery
path for consumers that need a complete message rather than a byte stream:

- the WebSocket handler, which sends one binary frame
- the WebRTC data channel (`webrtc.rs`), `tcp.rs`, `rist.rs`, `pure_rist.rs`
- `route_body`, retained for direct callers and tests

The streaming HTTP path deliberately does **not** call `register_response`.
That call assigns the lane to `ResponseWatcher`, and the watcher skips any
stream where `RESPONSE_WATCHER_READER_ID` is not registered — that check is the
seam that lets both paths coexist over one cache.

Buffered delivery still carries a hard ceiling: `ResponseWatcher` caps assembly
at `slot_bytes * slots_per_stream` and fails the whole response with
`"response body exceeds N bytes"` rather than applying backpressure. Streaming
responses have no such cap.

## Backpressure

The response ring decouples the worker from the client. `wait_for_reader_capacity`
returns immediately while `next_slot <= slots_per_stream`
(`upload-response/src/lib.rs:1006`), so a worker can run a **full ring ahead** —
1024 slots by default — without ever waiting on the peer. Client speed does not
enter into it until then.

Only when the worker would overwrite a slot the slowest registered reader has
not yet consumed does it wait, bounded by `reader_backpressure_timeout_ms`.
That is a bounded buffer doing its job rather than the worker tracking the
client: reaching that point means the client is already more than one whole ring
behind, and continuing would destroy data the reader still needs.

`proxy_streaming_response_with` registers itself as a response reader for the
duration and calls `mark_response_reader_position` as it goes, which is what
keeps slots from being recycled underneath it mid-response.

Registration happens after `end_request`, so a worker could in principle reach
the capacity check before a reader exists. This is safe twice over: the check is
skipped entirely until the ring wraps, and with no reader registered it blocks
rather than overwriting, while `register_response_reader` calls
`notify_waiters()` to wake the blocked writer — with the wait armed before the
check, so the wakeup cannot be missed.

**Sizing.** A producer writing one slot per unit of output exhausts the ring
after `slots_per_stream` writes and paces to the client for the remainder. For
incremental output such as token generation, either size the ring for the
expected length or batch several units per slot. The default ring is 1024 slots
of 32KB.

## Deadlines

Buffered and streaming delivery are bounded differently, because they fail
differently.

| Delivery | Field | Meaning |
| --- | --- | --- |
| Buffered (`await_response`) | `response_deadline_ms` | total wait for the whole response |
| Streaming (`proxy_streaming_response_with`) | `response_idle_timeout_ms` | wait for the **next** slot |

A total deadline on a streaming response caps how long the response is allowed
to take to *generate*, which is wrong for incrementally produced output — a
model emitting tokens for two minutes is healthy, not stuck. The streaming path
is therefore bounded by idle time: a worker that keeps producing may run
indefinitely, while one that stops is cut off after
`response_idle_timeout_ms`.

A stall after the head has been sent cannot be reported as a status code, so it
resets the stream under the contract below. The peer sees a failure, never a
truncated answer.

`from_legacy_response_timeout` maps both fields from the single legacy value, so
existing configuration keeps its magnitude while gaining the corrected shape.

## The `StreamWriter` completion contract

A response head cannot be un-sent. Once `send_response` has gone out, the only
way to report a failure is to reset the stream — so `finish` is the sole marker
of a complete response.

**Dropping a writer that sent a head but was never finished resets the stream**
(HTTP/2 `RST_STREAM`, HTTP/3 `H3_INTERNAL_ERROR`). A handler that returns
without finishing is reporting failure.

Enforced per backend:

- `h2.rs` — `H2ResponseBody` carries a real error type; the body yields `Err`
  when the writer's channel closes without the completion flag set, and hyper
  resets the stream.
- `h3.rs` — `H3StreamWriter::drop` calls `stop_stream(Code::H3_INTERNAL_ERROR)`.
  `handle_h3_body_stream_request` resets rather than finishes on handler error.
- `h3_tokio_quiche.rs` — `TokioQuicheStreamWriter::drop` sends
  `OutboundFrame::PeerStreamError`.

Because it hangs off `Drop`, it covers what an explicit error path misses: a
handler returning `Err` mid-body, a panic, and shutdown cancellation.

Before this existed a mid-stream failure was logged and the body simply stopped,
so a truncated response was indistinguishable from a complete one. Buffering hid
it: the watcher delivered `Err` and the router turned it into a clean 5xx. This
had to land before streaming egress, not after.

## Tests that pin this down

- `web-service/tests/request_limits.rs` —
  `streaming_handler_error_after_head_aborts_the_body_over_http1` and
  `..._resets_the_stream_over_http2` assert a client cannot read a truncated
  body as complete; the h2 case asserts specifically on `RST_STREAM` /
  `INTERNAL_ERROR`. `streaming_handler_that_finishes_delivers_a_complete_body`
  is the control that keeps those honest.
- `upload-response/tests/worker_integration.rs` —
  `router_streams_response_slots_as_the_worker_produces_them` gates each worker
  chunk on the previous one reaching the peer, so a buffering egress deadlocks
  rather than passing. It runs with no `ResponseWatcher` at all.
- `slow_but_productive_worker_outlives_the_legacy_response_deadline` and
  `stalled_worker_fails_the_streaming_response` pin the idle deadline from both
  sides: a total deadline fails the first, and no deadline at all hangs the
  second.

The reset and streaming-egress tests were each verified to fail with their fix
reverted.

## Planned work

1. ~~Stream reset on mid-stream handler error.~~ Done.
2. ~~Wire streaming egress into `UploadResponseRouter`.~~ Done, via
   `has_body_stream_handler`/`route_body_stream`.
3. Reimplement `CachedResponse` as a collect over the streaming reader and
   delete `ResponseAssembly` and its size cap, so accumulation is an opt-in
   adapter for the message-oriented consumers listed above rather than a global
   task that buffers every stream.
4. Partly done: `response_idle_timeout_ms` is now its own field, so response
   delivery no longer shares a deadline with request admission. Still
   outstanding is `reader_backpressure_timeout_ms`, which continues to inherit
   the legacy value. Lower priority than it first looked: it applies only once
   the ring is full, by which point the client is a full ring behind, and a
   generous wait before giving up is defensible.

## Known gaps

- The streaming path skips `apply_byte_range` and the `etag` field; both live
  only in `build_buffered_response`. Since public HTTP traffic now takes the
  streaming path, `Range` requests are no longer served on those routes.
- `H3SplitStreamWriter` (`h3.rs`) is defined but never constructed.
