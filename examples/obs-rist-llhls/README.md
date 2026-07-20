# OBS RIST LL-HLS Example

This example keeps `rist-rs` focused on protocol code and puts the application
pipeline in `web-services`.

Pipeline:

```text
OBS MPEG-TS over RIST -> pure Rust rist-mio receiver -> playlists ChunkCache -> HLS playlist -> hls.js
```

The server expects OBS to send MPEG-TS containing browser-playable codecs
such as H.264 video and AAC audio. It does not transcode or remux yet. It
chunks received transport stream payloads into short cached parts and serves
a low-latency live HLS playlist.

## Run

```bash
cargo run -p obs-rist-llhls -- \
  --rist-bind 0.0.0.0:7000 \
  --rist-profile main \
  --http-port 9444
```

Open `https://127.0.0.1:9444/`.

The default TLS certificate is loaded from `web-services/tls/local.wavey.ai`.
Use `--cert` and `--key` to provide another certificate pair.

## OBS

Use OBS Custom Output (FFmpeg) with:

- Container format: `mpegts`
- Video: H.264
- Audio: AAC
- URL: `rist://127.0.0.1:7000`

If your sender sets an explicit RIST flow id, pass the same value to the server:

```bash
cargo run -p obs-rist-llhls -- --flow-id 0x72737401
```

The UI displays hls.js live latency when the browser exposes it, plus receiver
packet counts, ingest bitrate, recovered packet count, and cache freshness.
