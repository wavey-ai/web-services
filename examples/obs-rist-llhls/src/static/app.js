const video = document.getElementById("video");
const state = document.getElementById("state");
const latency = document.getElementById("latency");
const rate = document.getElementById("rate");
const packets = document.getElementById("packets");
const parts = document.getElementById("parts");
const age = document.getElementById("age");
const recovered = document.getElementById("recovered");

let hls = null;
let lastBytes = 0;
let lastStatsAt = performance.now();

function setState(label, className) {
  state.textContent = label;
  state.className = `state ${className || ""}`.trim();
}

function formatLatency(seconds) {
  if (!Number.isFinite(seconds)) return "-";
  return `${(seconds * 1000).toFixed(0)} ms`;
}

function formatRate(bytesPerSecond) {
  if (!Number.isFinite(bytesPerSecond) || bytesPerSecond <= 0) return "-";
  return `${((bytesPerSecond * 8) / 1_000_000).toFixed(2)} Mbps`;
}

function attachPlayer() {
  const source = "/live/stream.m3u8";
  if (window.Hls && Hls.isSupported()) {
    hls = new Hls({
      lowLatencyMode: true,
      backBufferLength: 12,
      liveSyncDurationCount: 1,
      liveMaxLatencyDurationCount: 4,
      enableWorker: true,
    });
    hls.on(Hls.Events.MANIFEST_PARSED, () => {
      setState("live", "live");
      video.play().catch(() => {});
    });
    hls.on(Hls.Events.ERROR, (_event, data) => {
      if (data.fatal) {
        setState("error", "error");
        hls.destroy();
        hls = null;
        setTimeout(attachPlayer, 1000);
      }
    });
    hls.attachMedia(video);
    hls.loadSource(source);
    return;
  }

  if (video.canPlayType("application/vnd.apple.mpegurl")) {
    video.src = source;
    video.play().catch(() => {});
    setState("live", "live");
    return;
  }

  setState("error", "error");
}

async function pollStats() {
  try {
    const res = await fetch("/api/stats", { cache: "no-store" });
    if (!res.ok) throw new Error(`stats ${res.status}`);
    const stats = await res.json();
    const now = performance.now();
    const elapsed = Math.max((now - lastStatsAt) / 1000, 0.001);
    const bytesPerSecond = (stats.bytes_received - lastBytes) / elapsed;

    lastBytes = stats.bytes_received;
    lastStatsAt = now;

    latency.textContent = formatLatency(hls ? hls.latency : NaN);
    rate.textContent = formatRate(bytesPerSecond);
    packets.textContent = stats.packets_received.toLocaleString();
    parts.textContent = stats.cached_parts.toLocaleString();
    age.textContent =
      stats.last_packet_age_ms == null ? "-" : `${stats.last_packet_age_ms} ms`;
    recovered.textContent = stats.recovered_packets.toLocaleString();

    if (stats.packets_received > 0 && state.textContent === "waiting") {
      setState("buffering", "");
    }
  } catch (_error) {
    setState("error", "error");
  } finally {
    setTimeout(pollStats, 500);
  }
}

attachPlayer();
pollStats();
