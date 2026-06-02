#!/usr/bin/env node

if (process.env.LLHLS_INSECURE_TLS === "1") {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

const playlistUrl = process.argv[2] || "https://local.bitneedle.com:19443/1/stream.m3u8";
const maxChecks = Number.parseInt(process.env.LLHLS_MAX_CHECKS || "48", 10);
const timeoutMs = Number.parseInt(process.env.LLHLS_TIMEOUT_MS || "5000", 10);

function parseAttributes(value) {
  const attrs = new Map();
  const re = /([^=,]+)=("[^"]*"|[^,]*)/g;
  let match;
  while ((match = re.exec(value)) !== null) {
    attrs.set(match[1].trim(), match[2].replace(/^"|"$/g, ""));
  }
  return attrs;
}

function parseByteRange(value) {
  const [length, offset] = value.split("@");
  const parsedLength = Number.parseInt(length, 10);
  const parsedOffset = offset === undefined ? undefined : Number.parseInt(offset, 10);
  if (!Number.isSafeInteger(parsedLength) || parsedLength <= 0) {
    return null;
  }
  if (offset !== undefined && (!Number.isSafeInteger(parsedOffset) || parsedOffset < 0)) {
    return null;
  }
  return { length: parsedLength, offset: parsedOffset };
}

function rangeHeader(byteRange, previousEndByUri, uri) {
  if (!byteRange) {
    return "bytes=0-0";
  }
  const start = byteRange.offset ?? previousEndByUri.get(uri);
  if (!Number.isSafeInteger(start) || start < 0) {
    return null;
  }
  const end = start + byteRange.length - 1;
  previousEndByUri.set(uri, end + 1);
  return `bytes=${start}-${end}`;
}

async function fetchWithTimeout(url, init = {}) {
  return fetch(url, {
    ...init,
    signal: AbortSignal.timeout(timeoutMs),
  });
}

async function main() {
  const playlistResponse = await fetchWithTimeout(playlistUrl, {
    headers: { "accept-encoding": "identity" },
  });
  const playlist = await playlistResponse.text();
  if (playlistResponse.status !== 200) {
    throw new Error(`playlist returned HTTP ${playlistResponse.status}`);
  }
  if (!playlist.startsWith("#EXTM3U")) {
    throw new Error("playlist does not start with #EXTM3U");
  }

  const base = new URL(playlistUrl);
  const checks = [];
  const previousEndByUri = new Map();
  let pendingSegmentRange = null;

  for (const line of playlist.split(/\r?\n/)) {
    if (line.startsWith("#EXT-X-PART:")) {
      const attrs = parseAttributes(line.slice("#EXT-X-PART:".length));
      const uri = attrs.get("URI");
      if (!uri) {
        throw new Error(`EXT-X-PART without URI: ${line}`);
      }
      const byteRange = attrs.has("BYTERANGE") ? parseByteRange(attrs.get("BYTERANGE")) : null;
      checks.push({ kind: "part", uri, byteRange });
      continue;
    }

    if (line.startsWith("#EXT-X-BYTERANGE:")) {
      pendingSegmentRange = parseByteRange(line.slice("#EXT-X-BYTERANGE:".length));
      if (!pendingSegmentRange) {
        throw new Error(`invalid EXT-X-BYTERANGE: ${line}`);
      }
      continue;
    }

    if (line.startsWith("#EXT-X-PRELOAD-HINT:")) {
      const attrs = parseAttributes(line.slice("#EXT-X-PRELOAD-HINT:".length));
      const uri = attrs.get("URI");
      const type = attrs.get("TYPE");
      const start = attrs.get("BYTERANGE-START");
      if (type === "PART" && uri && start !== undefined) {
        console.log(`hint ${uri} starts at ${start} bytes`);
      }
      continue;
    }

    if (line && !line.startsWith("#") && !line.endsWith(".m3u8")) {
      checks.push({ kind: "segment", uri: line, byteRange: pendingSegmentRange });
      pendingSegmentRange = null;
    }
  }

  const selected = checks.slice(-Math.max(1, maxChecks));
  const failures = [];
  for (const check of selected) {
    const url = new URL(check.uri, base);
    const range = rangeHeader(check.byteRange, previousEndByUri, check.uri);
    if (!range) {
      failures.push(`${check.kind} ${check.uri}: cannot derive byte range`);
      continue;
    }
    const response = await fetchWithTimeout(url, {
      headers: {
        "accept-encoding": "identity",
        range,
      },
    });
    if (response.status !== 206) {
      failures.push(`${check.kind} ${check.uri} ${range}: HTTP ${response.status}`);
    }
    await response.arrayBuffer();
  }

  console.log(
    `validated ${selected.length}/${checks.length} advertised media ranges from ${playlistUrl}`,
  );
  if (failures.length > 0) {
    for (const failure of failures) {
      console.error(failure);
    }
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
