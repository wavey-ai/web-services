<div align="center">

# upload-response

High-performance streaming request/response proxy via shared-memory cache

[![Rust](https://img.shields.io/badge/rust-1.75+-ff6b9d.svg?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org)
[![Throughput](https://img.shields.io/badge/throughput-1.4_GB/s-ff6b9d.svg?style=flat-square)](/)

</div>

---

## Architecture

```
Client Request (H1.1/H2/H3)
       │
       ▼
┌─────────────────────────┐
│  UploadResponseRouter   │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Request ChunkCache     │  ◄── Worker tails slots
│  ┌─────┬─────┬─────┐    │
│  │HPKS │Body │Body │    │
│  │Hdrs │Bytes│Bytes│    │
│  └─────┴─────┴─────┘    │
└─────────────────────────┘
            │
            ▼
┌─────────────────────────┐
│  Response ChunkCache    │  ◄── Worker writes response
│  ┌─────┬─────┬─────┐    │
│  │HPKS │Body │Empty│    │
│  │Hdrs │Bytes│(END)│    │
│  └─────┴─────┴─────┘    │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  ResponseWatcher        │
│  (delivers to client)   │
└─────────────────────────┘
```

## Stream Format

| Slot | Content |
|:-----|:--------|
| `1` | HPKS Headers frame |
| `2..N-1` | Raw body bytes |
| `N` | Empty slot (end marker) |

## Configuration

```rust
let config = UploadResponseConfig {
    num_streams: 100,         // Max concurrent requests
    slot_size_kb: 64,         // 64KB per slot
    slots_per_stream: 16384,  // ~1GB max per request
    response_timeout_ms: 30000,
};
```

## Performance

```
┌─────────────┬──────────────┬──────────────┐
│  Slot Size  │  Throughput  │  Slots Used  │
├─────────────┼──────────────┼──────────────┤
│      16 KB  │   1397 MB/s  │       32768  │
│      64 KB  │   1390 MB/s  │        8192  │
│     512 KB  │   1430 MB/s  │        1024  │
│    1024 KB  │   1322 MB/s  │         512  │
└─────────────┴──────────────┴──────────────┘

1GB Upload: ~1390 MB/s @ 64KB slots
```

## Worker Integration

```rust
match service.tail_request(stream_id, slot_id).await {
    Some(TailSlot::Headers(h)) => { /* method, path, headers */ }
    Some(TailSlot::Body(data)) => { /* zero-copy Bytes */ }
    Some(TailSlot::End) => { /* write response */ }
    None => {}
}
```

## Testing

```bash
cargo test -p upload-response --release -- --nocapture
```

---

<div align="center">
<sub>Built with 🩷 by <a href="https://github.com/wavey-ai">wavey.ai</a></sub>
</div>
