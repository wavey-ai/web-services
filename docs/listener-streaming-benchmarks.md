# Listener streaming benchmarks

This document records the streaming load measurements of the HTTP/1.1,
HTTP/2 and HTTP/3 listener in `av-web-service`. The measurements are from
October 6 and October 7, 2026. They compare `main` with three changes:
inline streaming handlers, `TCP_NODELAY` on accepted sockets, and HTTP/2
write batching.

The harness is in [`benchmarks/streaming/`](../benchmarks/streaming/). The
`summary.md` and `summary.csv` files of each round are in
`benchmarks/streaming/results/<round>/`. The raw runs (`runs.jsonl`) and the
logs are not in the repository.

## Terms

- **Stream**: one HTTP request and its complete response.
- **TTFB**: the time from the request to the first response body byte.
- **TTLB**: the time from the request to the last response body byte.
- **Cell**: one metric of one scenario for one tree. Each cell is the median
  of three repeats, with the minimum and the maximum.
- **Separated**: the ranges (minimum to maximum) of two trees do not overlap.
  `summary.md` marks a separated delta with `*`.
- **Send-first echo**: an echo request where the client sends the full request
  body before it reads the response.

## Trees

| Label | Commit | Content |
| --- | --- | --- |
| `main` | `2399227` | Baseline. `handle_h2_body_stream` and the `route_stream` path in `web-service/src/h2.rs` run the handler on a spawned task. `mpsc::channel(32)` holds up to 32 chunks between the handler and hyper. |
| `inline-streaming` | `18ed9e1` | `c4b9fb5` and `18ed9e1` on `main`. The handler runs inside the response body (`web-service/src/stream_response.rs`). The writer holds one chunk. |
| `listener-nodelay` (round 3) | `63a68de` | `disable_nagle` sets `TCP_NODELAY` on each accepted socket of the HTTP/1.1+HTTP/2 listener and of the raw TCP listener. |
| `listener-nodelay` (round 5) | `400b5c9` | `63a68de` plus `WriteBatch` (`web-service/src/write_batch.rs`). On an HTTP/2 connection, `WriteBatch` copies TLS output into a buffer of at most 64 KiB (`BATCH_BYTES`) and writes the buffer in one call. A raw TCP frame goes out in one vectored write. |
| `inline-streaming-nodelay` (rounds 3 and 4) | `991d374` | Merge of `2534bc3` and `63a68de`. `2534bc3` adds a task yield to `18ed9e1` after 500 µs of handler work without a wait (`HANDLER_SLICE`). |
| `inline-streaming-nodelay` (round 5) | `8a211ec` | Merge of `991d374` and `400b5c9`. This tree is the merge candidate. |

## Method

### Server

`wsb-server` is an `H2H3Server` application. It has three routes:

- `GET /stream?chunks=N&size=S&cpu=P` uses `Router::route_stream`. The handler
  sends N chunks of S bytes. Each chunk is a new allocation. With `cpu=1`, the
  handler calculates SHA-256 over each chunk.
- `POST /echo` uses `Router::route_body_stream`. The handler sends the response
  head. Then it sends each request body chunk back when the chunk arrives.
- `GET /` uses `Router::route` and returns a 2-byte body.

The server uses TLS with a self-signed `rcgen` certificate. Its connection
limit and in-flight request limit are 1,048,576. `run.sh` builds one server
for each tree. The server's `av-web-service` path dependency points to the
tree. The tree's `Cargo.lock` is the seed of the server lock file.

### Client

`wsb-client` uses the hyper connection API for HTTP/1.1 and HTTP/2, and
`h3-quinn` for HTTP/3. It sets the number of connections and the number of
streams on each connection. It has two modes:

- `closed`: each virtual client sends a request, reads the full response, and
  sends the next request. Warm-up is 5 s. Measurement is 10 s.
- `stall`: after a 3 s warm-up, the client opens all streams. Each stream reads
  the response head and the first data frame, and then stops for 5 s. Then all
  streams read to the end.

At the start and end of the window, the client reads these server values
from `/proc`: CPU time, context switches, write system calls and RSS. It samples
VmRSS every 50 ms and resets VmHWM at the start of the window. It reads
machine-wide TCP output segments from `/proc/net/snmp`.

The client has two builds. One uses h2 0.4.19. The other uses h2 0.4.17,
which is the version before the h2 DATA frame budget check (0.4.18). The
scenarios with `h2old` in the name use h2 0.4.17.

### Scenarios

Rounds 2, 3 and 5 use 56 scenarios. Round 1 uses the first 45 scenarios,
without the send-first echo scenarios.

| Family | Request | Protocols and concurrent streams |
| --- | --- | --- |
| `short` | 4 chunks of 16 KiB | `h1`, `h2` (64 streams on each connection): 64, 1,000, 5,000, 10,000. `h2x1` (1 stream on each connection): 1,000. `h3`: 64, 1,000. |
| `long256k` | 32 chunks of 256 KiB | `h1`, `h2`: 64, 1,000, 5,000. `h3`: 64. |
| `long1m` | 16 chunks of 1 MiB | `h1`, `h2`: 64, 1,000. |
| `cpu` | 64 chunks of 64 KiB, SHA-256 for each chunk | `h1`, `h2`: 4, 64, 1,000. |
| `echo` | 1 MiB body in 64 KiB chunks. The client sends and reads at the same time. | `h1`, `h2`, `h2old`: 64, 1,000. |
| `echosf<N>m` | N MiB send-first echo. 5 s stream timeout. 256 KiB client receive buffer. | 16 streams. `h1`: 2, 4, 8, 16, 64 MiB. `h2old` (1 stream on each connection): 2, 3, 4, 16, 64 MiB. `h2oldmux` (16 streams on one connection): 2 MiB. |
| `slow64k` | 256 chunks of 64 KiB, stall mode | `h1`, `h2`: 64, 1,000, 5,000. `h3`: 1,000. |
| `slow256k` | 128 chunks of 256 KiB, stall mode | `h1`, `h2`: 64, 1,000. |

The `slow` HTTP/1.1 and HTTP/2 scenarios use a 256 KiB client receive buffer.
The `slow` HTTP/2 scenarios use a 128 MiB connection window.

### Procedure

1. `run.sh` starts a new server process on a new port for each run.
2. Before each run, `run.sh` waits until the machine CPU use is 1% or less.
3. Inside each repeat, `run.sh` runs the trees one after the other for each
   scenario.
4. `run.sh` rotates the tree order by one place for each repeat.
5. `summarize.py` writes the median and the range of each cell.

## Machine

| Item | Value |
| --- | --- |
| Instance | EC2 `mastering-probe`, m8g.16xlarge, eu-north-1 |
| CPU | 64 Neoverse-V2 cores (Graviton4), 1 NUMA node |
| Memory | 247 GiB |
| Kernel | Linux 7.0.0-1013-aws, aarch64 |
| Rust | rustc 1.98.1 |
| Server cores | 0-23, tokio `worker_threads=24` |
| Client cores | 24-63, 40 client threads |
| Network | Loopback, 127.0.0.1 |
| File descriptors | 1,048,576 for each process |
| `tcp_rmem` | 4096 131072 33554432 |
| `tcp_wmem` | 4096 16384 4194304 |

A pilot run used 32 server cores and 32 client cores. In that split, the
client used 92-100% of its cores in the high-throughput scenarios. With 24
server cores and 40 client cores, short HTTP/1.1 streams at 10,000 increased
by 11%. In rounds 2, 3 and 5, the maximum client CPU use is 79%, 75% and 80%.

## Noise floor

The spread of a cell is (maximum − minimum) / median over the three repeats.

| Round | streams/s | TTLB p50 | TTLB p99 | CPU per stream | Peak RSS |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.9% | 0.9% | 1.9% | 0.8% | 2.3% |
| 2 | 1.0% | 1.4% | 1.1% | 1.3% | 3.3% |
| 3 | 1.1% | 1.1% | 1.7% | 1.3% | 3.3% |
| 4 | 0.6% | 1.2% | 1.3% | 0.5% | 2.4% |
| 5 | 1.0% | 1.3% | 2.2% | 1.4% | 3.8% |

The values are the median spread over all scenarios and trees. Each
`summary.md` gives the maximum spread and the spread of each cell. Scenarios
with stream timeouts have the largest maximum values.

The `main` tree ran in rounds 1, 2, 3 and 5. The table gives the difference
of its medians from round 1, over the scenarios that complete streams.

| Round | streams/s, median | streams/s, maximum | CPU per stream, median | CPU per stream, maximum |
| --- | --- | --- | --- | --- |
| 2 | 0.4% | 6.6% | 0.6% | 9.4% |
| 3 | 1.5% | 14.4% | 1.1% | 5.8% |
| 5 | 1.1% | 7.6% | 1.0% | 6.0% |

This document uses these limits:

- A throughput or CPU-per-stream difference is a result when it is separated
  and larger than 3%.
- A p99 latency or peak RSS difference smaller than 10% is in the noise.

## Round 1: `main` baseline

Results: `results/20261006T125035Z-main/`. Tree: `main` `2399227`. The round
has 135 runs: 45 scenarios, 3 repeats.

| Scenario | streams/s | TTLB p50 / p99 ms | CPU µs per stream | Peak RSS MiB |
| --- | --- | --- | --- | --- |
| `short-h1-c1000` | 495,199 | 1.82 / 4.87 | 47.9 | 40 |
| `short-h1-c10000` | 473,807 | 24.55 / 37.71 | 50.3 | 266 |
| `short-h2-c1000` | 253,032 | 3.49 / 44.43 | 77.0 | 72 |
| `short-h2-c10000` | 390,090 | 14.99 / 64.97 | 59.3 | 287 |
| `short-h2x1-c1000` | 24,410 | 41.01 / 42.13 | 67.3 | 80 |
| `long256k-h1-c1000` | 5,437 | 181.4 / 338.5 | 4,407 | 5,866 |
| `long256k-h2-c1000` | 3,392 | 299.8 / 326.7 | 5,117 | 7,577 |
| `cpu-h1-c4` | 139 | 44.75 / 46.72 | 5,202 | 29 |
| `cpu-h2-c1000` | 4,506 | 223.9 / 281.9 | 5,317 | 2,324 |
| `echo-h1-c64` | 1,545 | 42.00 / 43.03 | 1,333 | 24 |
| `echo-h1-c1000` | 18,358 | 54.02 / 83.58 | 1,298 | 157 |

Slow readers on `main`:

| Chunk size | Protocol | Plateau KiB per stalled stream | Peak RSS at 5,000 stalled streams |
| --- | --- | --- | --- |
| 64 KiB | HTTP/1.1 | 2,471-2,749 | 12,532 MiB |
| 64 KiB | HTTP/2 | 2,734-3,694 | 14,210 MiB |
| 64 KiB | HTTP/3 | 239 | not measured |
| 256 KiB | HTTP/1.1 | 8,980-9,398 | not measured |
| 256 KiB | HTTP/2 | 9,522-10,117 | not measured |

Other results of round 1:

- Several scenarios have a TTLB of 41-45 ms with idle server CPU:
  `short-h2x1-c1000`, `echo-h1-c64`, `cpu-h1-c4`, `cpu-h1-c64`, and the p99 of
  `short-h2-c1000`. `main` does not set `TCP_NODELAY` on the HTTP/1.1+HTTP/2
  listener. Nagle's algorithm and the client's delayed ACK cause this delay.
- `echo-h2-c64` and `echo-h2-c1000` have 3,380 and 62,404 errors in each
  window. The h2 0.4.19 client closes the connection with
  `too_many_data_frames`. With the h2 0.4.17 client (`echo-h2old`), these
  scenarios have no errors.
- One HTTP/2 connection carries at most about 1.2 GiB/s. The `short-h2-c64`,
  `long256k-h2-c64`, `long1m-h2-c64`, `cpu-h2-c4` and `cpu-h2-c64` scenarios
  use one connection. Their throughput is 1.03-1.20 GiB/s.

## Round 2: `main` and `inline-streaming`

Results: `results/20261006T134042Z-main-vs-inline-streaming/`. Trees: `main`
`2399227` and `inline-streaming` `18ed9e1`. The round has 336 runs: 56
scenarios, 2 trees, 3 repeats. All deltas in the table are separated.

| Scenario | Metric | `main` | `inline-streaming` | Delta |
| --- | --- | --- | --- | --- |
| `short-h1-c1000` | streams/s | 499,059 | 516,569 | +3.5% |
| `short-h1-c10000` | streams/s | 469,331 | 507,148 | +8.1% |
| `short-h2-c1000` | streams/s | 252,162 | 281,146 | +11.5% |
| `short-h2-c1000` | TTLB p99 ms | 44.60 | 6.86 | −84.6% |
| `short-h2-c5000` | TTFB p50 ms | 3.21 | 7.61 | +136.8% |
| `short-h2-c5000` | peak RSS MiB | 181 | 298 | +64.0% |
| `short-h2-c10000` | streams/s | 389,019 | 358,331 | −7.9% |
| `short-h2-c10000` | TTFB p50 ms | 7.80 | 18.61 | +138.5% |
| `short-h2-c10000` | peak RSS MiB | 286 | 489 | +70.7% |
| `long256k-h1-c64` | streams/s | 4,474 | 5,626 | +25.8% |
| `long256k-h1-c1000` | peak RSS MiB | 5,914 | 993 | −83.2% |
| `long1m-h1-c64` | streams/s | 1,409 | 2,411 | +71.0% |
| `cpu-h1-c1000` | TTFB p50 ms | 68.0 | 169.7 | +149.6% |
| `cpu-h1-c1000` | TTLB p99 ms | 437.9 | 575.9 | +31.5% |
| `cpu-h1-c1000` | peak RSS MiB | 1,864 | 51 | −97.3% |
| `echosf3m-h2old-c16` | streams/s | 676 | 0 (32 timeouts) | −100% |

Slow readers in round 2:

| Scenario | Metric | `main` | `inline-streaming` | Delta |
| --- | --- | --- | --- | --- |
| `slow64k-h1-c1000` | plateau KiB per stream | 2,568 | 386 | −85.0% |
| `slow64k-h2-c1000` | plateau KiB per stream | 2,822 | 673 | −76.1% |
| `slow256k-h1-c1000` | plateau KiB per stream | 9,013 | 690 | −92.3% |
| `slow256k-h2-c1000` | plateau KiB per stream | 9,584 | 1,273 | −86.7% |
| `slow64k-h1-c5000` | peak RSS MiB | 12,550 | 1,999 | −84.1% |
| `slow64k-h2-c5000` | peak RSS MiB | 14,256 | 3,611 | −74.7% |

The HTTP/3 scenarios use a different code path. Their deltas are in the
noise.

The `cpu-h1-c1000` TTFB increase has this cause. A handler that does not wait
runs its full response in one task poll. On HTTP/1.1, hyper flushes only when
its write buffer of about 400 KB is full or the body returns `Pending`.
Commit `2534bc3` yields the task after 500 µs of handler work without a wait.
On HTTP/2, hyper 1.11.1 polls the body before it reserves send capacity. A
stream that waits for send capacity has its next chunk ready.

## Round 3: `main`, `listener-nodelay` and `inline-streaming-nodelay`

Results: `results/20261007T093350Z-main-vs-nodelay-vs-inline-nodelay/`.
Trees: `main` `2399227`, `listener-nodelay` `63a68de` and
`inline-streaming-nodelay` `991d374`. The round has 504 runs: 56 scenarios,
3 trees, 3 repeats. All deltas in this section are separated.

### `TCP_NODELAY` and the 41-45 ms delays

| Scenario | Metric | `main` | `listener-nodelay` | Delta |
| --- | --- | --- | --- | --- |
| `short-h2x1-c1000` | TTLB p50 ms | 41.01 | 2.69 | −93.4% |
| `short-h2x1-c1000` | streams/s | 24,423 | 336,700 | +1,279% |
| `echo-h1-c64` | TTLB p50 ms | 42.00 | 3.49 | −91.7% |
| `echosf2m-h1-c16` | TTLB p50 ms | 43.00 | 2.74 | −93.6% |
| `echosf2m-h2old-c16` | TTLB p50 ms | 42.83 | 2.80 | −93.5% |
| `cpu-h1-c4` | TTLB p50 ms | 44.75 | 4.19 | −90.6% |
| `cpu-h1-c64` | TTLB p50 ms | 44.71 | 10.23 | −77.1% |
| `long1m-h1-c64` | TTLB p50 ms | 50.00 | 21.76 | −56.5% |
| `short-h2-c1000` | TTLB p99 ms | 44.44 | 7.85 | −82.3% |

### `TCP_NODELAY` cost without write batching

| Scenario | Metric | `main` | `listener-nodelay` | Delta |
| --- | --- | --- | --- | --- |
| `long256k-h2-c1000` | streams/s | 3,437 | 3,127 | −9.0% |
| `long256k-h2-c1000` | CPU µs per stream | 5,086 | 5,618 | +10.4% |
| `long256k-h2-c5000` | streams/s | 4,839 | 4,336 | −10.4% |
| `long1m-h2-c64` | CPU µs per stream | 8,616 | 11,330 | +31.5% |
| `long1m-h2-c1000` | streams/s | 1,761 | 1,568 | −11.0% |
| `short-h2-c5000` | streams/s | 397,161 | 372,336 | −6.3% |
| `short-h2-c10000` | streams/s | 394,839 | 330,264 | −16.4% |
| `short-h2-c10000` | CPU µs per stream | 58.7 | 68.9 | +17.5% |
| `short-h2-c10000` | TTFB p50 ms | 7.49 | 19.47 | +160.0% |
| `echo-h1-c1000` | streams/s | 18,503 | 15,451 | −16.5% |
| `echo-h1-c1000` | CPU µs per stream | 1,288 | 1,552 | +20.5% |

### Inline streaming on `TCP_NODELAY`

| Scenario | Metric | `listener-nodelay` | `inline-streaming-nodelay` | Delta |
| --- | --- | --- | --- | --- |
| `cpu-h1-c1000` | TTFB p50 ms | 69.37 | 12.22 | −82.4% |
| `cpu-h1-c1000` | TTFB p99 ms | 272.6 | 31.1 | −88.6% |
| `cpu-h1-c1000` | TTLB p99 ms | 463.3 | 276.6 | −40.3% |
| `cpu-h1-c1000` | peak RSS MiB | 1,869 | 152 | −91.9% |
| `cpu-h1-c4` | streams/s | 951 | 812 | −14.7% |
| `short-h1-c1000` | streams/s | 489,878 | 529,486 | +8.1% |
| `short-h1-c10000` | streams/s | 480,908 | 509,388 | +5.9% |
| `short-h2-c5000` | streams/s | 372,336 | 384,077 | +3.2% |
| `short-h2-c5000` | peak RSS MiB | 247 | 368 | +48.9% |
| `short-h2-c10000` | streams/s | 330,264 | 360,134 | +9.0% |
| `short-h2-c10000` | peak RSS MiB | 373 | 635 | +70.3% |
| `long256k-h1-c64` | streams/s | 4,728 | 5,681 | +20.2% |
| `echo-h1-c1000` | streams/s | 15,451 | 14,316 | −7.3% |
| `echosf3m-h2old-c16` | streams/s | 3,803 | 0 (32 timeouts) | −100% |

## Diagnosis of short HTTP/2 streams at 10,000

The diagnosis ran after round 3, on the same trees as round 3. Perf adds
overhead, so the profile throughput does not compare with the rounds.

### Streams per connection (round 4)

Results: `results/20261007T120923Z-diag-h2-sweep/`. The scenarios are in
`diag-h2-sweep.txt`. Each request is 4 chunks of 16 KiB, except `1x64k`.

| Scenario | streams/s `main` / `listener-nodelay` / `inline-streaming-nodelay` | TTFB p50 ms | Peak RSS MiB |
| --- | --- | --- | --- |
| 10,000 streams, 16 on each connection | 433,097 / 357,431 / 359,294 | 3.42 / 27.49 / 25.51 | 189 / 203 / 585 |
| 10,000 streams, 32 on each connection | 422,222 / 344,506 / 360,333 | 4.37 / 27.93 / 24.63 | 168 / 194 / 569 |
| 10,000 streams, 64 on each connection | 395,599 / 329,226 / 360,218 | 7.48 / 19.52 / 18.10 | 283 / 375 / 633 |
| 10,000 streams, 128 on each connection | 383,513 / 346,803 / 359,775 | 9.36 / 14.38 / 14.33 | 455 / 548 / 653 |
| 2,500 streams, 16 on each connection | 380,244 / 410,502 / 386,102 | 0.70 / 5.37 / 5.02 | 74 / 77 / 187 |
| 10,000 streams, 64 on each connection, 1 chunk of 64 KiB | 439,837 / 387,519 / 396,450 | 6.00 / 16.81 / 16.31 | 351 / 490 / 736 |

### Profile

The profile used `diag-h2-profile.sh` at 10,000 streams and 64 streams on each
connection. On October 7, 2026, the raw perf data was on the EC2 machine in
`~/ws-bench/results/20261007T125134Z-diag-h2-profile/`.

- All 24 server workers ran at 95-97% CPU on all three trees.
- The server made about 4.1 socket writes of about 16 KB for each stream on
  each tree.
- TCP output segments per stream were 2.85 on `main`, 6.13 on
  `listener-nodelay`, and 6.19 on `inline-streaming-nodelay`. With Nagle's
  algorithm, the kernel merged the writes into fewer, larger loopback segments.
- The kernel share of server CPU increased from 23% to 31-32%. This is about
  7.5 µs of the 10 µs increase in CPU per stream.
- The added kernel time is per-packet loopback receive work:
  `_raw_spin_unlock_irqrestore`, `__nf_conntrack_find_get`,
  `__inet_lookup_established` and `skb_release_data`.
- The time to the response head was 7.2 ms, 19.3 ms and 18.1 ms. It is equal to
  the TTFB on each tree.

### Chunk lifetime and allocator

A server variant (`diag/lifesrv-src/main.rs`) measured the time from chunk
allocation to chunk release.

| Tree | Mean chunk lifetime | Live chunks |
| --- | --- | --- |
| `main` | 3.0 ms | 3,900-5,900 |
| `listener-nodelay` | 6.5-6.9 ms | 7,100-8,200 |
| `inline-streaming-nodelay` | 9.4-9.7 ms | 11,600-13,300 |

- The handler gives all four chunks to the writer within about 10 µs on each
  tree. The chunks then wait in the h2 send queue of the connection.
- On `inline-streaming-nodelay`, the handler allocates its chunks when the
  stream task first runs. On `listener-nodelay`, the spawned handler task
  starts about 3 ms later. The total time per stream is about equal.
- Worker parking was 54,000, 97,000 and 54,000 context switches per second.
- glibc arena lock waits (`__lll_lock_wait_private` in `cfree` and `malloc`)
  were 36,000, 35,000 and 68,000 per second.
- With mimalloc in `wsb-server`, context switches per stream on
  `inline-streaming-nodelay` decreased from 0.38 to 0.16. The value on `main`
  was 0.15. The RSS difference from `listener-nodelay` decreased from about
  260 MiB to about 125 MiB.
- With mimalloc, the throughput order stayed `main`, then
  `inline-streaming-nodelay`, then `listener-nodelay`.

## Round 5: `main`, batched `listener-nodelay` and the merge candidate

Results: `results/20261007T164700Z-main-vs-nodelay-batch-vs-inline-batch/`.
Trees: `main` `2399227`, `listener-nodelay` `400b5c9` and
`inline-streaming-nodelay` `8a211ec`. The round has 504 runs: 56 scenarios,
3 trees, 3 repeats.

### Cell counts

The table counts the cells that are separated and have a median difference
larger than 3%. "All metrics" uses every metric that `summary.md` compares.
"Main metrics" uses streams/s, TTFB p50 and p99, TTLB p50 and p99, CPU per
stream, peak RSS, plateau and peak KiB per stream, drain time, errors and
timeouts.

| Comparison | All metrics, better | All metrics, worse | Main metrics, better | Main metrics, worse |
| --- | --- | --- | --- | --- |
| `listener-nodelay` against `main` | 213 | 135 | 97 | 85 |
| `inline-streaming-nodelay` against `listener-nodelay` | 258 | 64 | 175 | 28 |
| `inline-streaming-nodelay` against `main` | 326 | 117 | 194 | 65 |

### Write batching

| Scenario | Metric | `main` | `listener-nodelay` | Delta |
| --- | --- | --- | --- | --- |
| `short-h2-c10000` | TCP segments per stream | 2.77 | 2.29 | −17.1% |
| `long256k-h2-c1000` | TCP segments per stream | 359.5 | 265.1 | −26.3% |
| `long256k-h2-c1000` | streams/s | 3,423 | 3,558 | +4.0% |
| `long256k-h2-c5000` | streams/s | 4,819 | 4,794 | −0.5% (not separated) |
| `long1m-h2-c64` | CPU µs per stream | 8,824 | 9,327 | +5.7% |
| `long1m-h2-c1000` | CPU µs per stream | 9,208 | 9,675 | +5.1% |
| `short-h2-c5000` | streams/s | 395,239 | 411,135 | +4.0% |
| `short-h2-c10000` | streams/s | 393,732 | 317,293 | −19.4% |
| `short-h2-c10000` | CPU µs per stream | 58.9 | 72.1 | +22.5% |
| `short-h2-c10000` | TTFB p50 ms | 7.66 | 29.54 | +285.6% |
| `echo-h1-c1000` | streams/s | 18,508 | 15,375 | −16.9% |

`WriteBatch` applies only to HTTP/2 connections. HTTP/1.1 connections write
one flush for each message.

### Merge candidate: cells better than `main`

All deltas in this table are separated.

| Scenario | Metric | `main` | `inline-streaming-nodelay` | Delta |
| --- | --- | --- | --- | --- |
| `short-h2x1-c1000` | streams/s | 24,422 | 373,607 | +1,430% |
| `short-h2x1-c1000` | TTLB p99 ms | 42.13 | 4.74 | −88.7% |
| `echosf2m-h1-c16` | streams/s | 380 | 5,889 | +1,451% |
| `echosf2m-h1-c16` | TTLB p99 ms | 44.02 | 2.88 | −93.5% |
| `echo-h1-c64` | streams/s | 1,544 | 18,070 | +1,070% |
| `echosf2m-h2old-c16` | streams/s | 681 | 6,007 | +783% |
| `echosf4m-h1-c16` | streams/s | 358 | 3,018 | +743% |
| `cpu-h1-c4` | streams/s | 148 | 819 | +452% |
| `cpu-h1-c4` | TTLB p99 ms | 46.06 | 5.07 | −89.0% |
| `cpu-h1-c64` | streams/s | 2,374 | 4,778 | +101% |
| `short-h2-c1000` | streams/s | 254,973 | 313,069 | +22.8% |
| `short-h2-c1000` | TTLB p99 ms | 44.45 | 5.34 | −88.0% |
| `short-h2-c64` | CPU µs per stream | 107.2 | 79.3 | −26.0% |
| `short-h1-c64` | streams/s | 430,782 | 490,390 | +13.8% |
| `short-h1-c5000` | streams/s | 483,016 | 511,445 | +5.9% |
| `long256k-h1-c64` | streams/s | 4,457 | 5,685 | +27.6% |
| `long1m-h1-c64` | streams/s | 1,530 | 2,549 | +66.6% |
| `cpu-h1-c1000` | TTLB p99 ms | 450.2 | 277.1 | −38.5% |
| `cpu-h1-c1000` | peak RSS MiB | 1,792 | 153 | −91.5% |
| `long256k-h1-c5000` | peak RSS MiB | 19,450 | 3,776 | −80.6% |
| `long256k-h2-c5000` | peak RSS MiB | 38,610 | 7,851 | −79.7% |

### Merge candidate: slow readers

| Scenario | Plateau KiB per stream, `main` → candidate | Peak RSS MiB, `main` → candidate | CPU µs per stream, delta |
| --- | --- | --- | --- |
| `slow64k-h1-c64` | 2,794 → 449 | 193 → 42 | −25.6% |
| `slow64k-h1-c1000` | 2,543 → 380 | 2,620 → 420 | −21.4% |
| `slow64k-h1-c5000` | 2,475 → 399 | 12,520 → 2,213 | −18.6% |
| `slow64k-h2-c64` | 3,505 → 624 | 314 → 78 | −14.7% |
| `slow64k-h2-c1000` | 2,798 → 662 | 3,998 → 1,210 | −5.4% |
| `slow64k-h2-c5000` | 2,745 → 645 | 14,246 → 3,595 | −11.0% |
| `slow256k-h1-c64` | 9,276 → 813 | 663 → 66 | −22.6% |
| `slow256k-h1-c1000` | 8,993 → 791 | 9,227 → 910 | −23.1% |
| `slow256k-h2-c64` | 10,117 → 1,298 | 908 → 134 | −16.0% |
| `slow256k-h2-c1000` | 9,576 → 1,265 | 10,295 → 2,084 | −11.2% |
| `slow64k-h3-c1000` | 244 → 237 | 356 → 355 | −0.4% (not separated) |

### Merge candidate: cells worse than `main`

All deltas in this table are separated.

| Scenario | Metric | `main` | `inline-streaming-nodelay` | Delta |
| --- | --- | --- | --- | --- |
| `short-h2-c5000` | TTFB p50 ms | 3.19 | 8.98 | +181.1% |
| `short-h2-c5000` | peak RSS MiB | 183 | 341 | +86.4% |
| `short-h2-c10000` | TTFB p50 ms | 7.66 | 22.97 | +199.8% |
| `short-h2-c10000` | peak RSS MiB | 284 | 531 | +87.0% |
| `short-h2-c10000` | streams/s | 393,732 | 379,750 | −3.6% |
| `echo-h1-c1000` | streams/s | 18,508 | 14,276 | −22.9% |
| `echo-h1-c1000` | TTLB p99 ms | 81.05 | 187.88 | +131.8% |
| `echo-h1-c1000` | CPU µs per stream | 1,288 | 1,681 | +30.5% |
| `echo-h2-c64` | streams/s | 508 | 459 | −9.7% |
| `echo-h2old-c64` | streams/s | 656 | 596 | −9.2% |
| `long256k-h2-c5000` | TTFB p50 ms | 35.37 | 49.56 | +40.1% |
| `long256k-h2-c5000` | TTLB p99 ms | 1,162 | 1,714 | +47.5% |
| `cpu-h2-c1000` | TTFB p50 ms | 9.40 | 14.18 | +50.8% |
| `short-h2x1-c1000` | TTFB p50 ms | 0.15 | 1.18 | +682.8% |
| `echosf3m-h2old-c16` | streams/s | 703 | 0 (32 timeouts) | −100% |

`summary.md` lists all other separated cells.

### Send-first echo limit

The table gives the result of each send-first scenario in round 5. "Timeout"
means 0 completed streams and 32 stream timeouts in each window, in each
repeat.

| Scenario | `main` | `listener-nodelay` | `inline-streaming-nodelay` |
| --- | --- | --- | --- |
| `echosf2m-h1-c16` | 380 streams/s | 5,837 streams/s | 5,889 streams/s |
| `echosf4m-h1-c16` | 358 streams/s | 3,003 streams/s | 3,018 streams/s |
| `echosf8m-h1-c16` | Timeout | 161 streams/s, 30 timeouts | Timeout |
| `echosf16m-h1-c16` | Timeout | Timeout | Timeout |
| `echosf64m-h1-c16` | Timeout | Timeout | Timeout |
| `echosf2m-h2old-c16` | 681 streams/s | 5,970 streams/s | 6,007 streams/s |
| `echosf3m-h2old-c16` | 703 streams/s | 4,006 streams/s | Timeout |
| `echosf4m-h2old-c16` | Timeout | Timeout | Timeout |
| `echosf16m-h2old-c16` | Timeout | Timeout | Timeout |
| `echosf64m-h2old-c16` | Timeout | Timeout | Timeout |
| `echosf2m-h2oldmux-c16` | Timeout | Timeout | Timeout |

The send-first echo stops on all trees when the body is larger than a limit.
This sequence causes the stop:

1. The `/echo` handler reads the next request chunk only after `send_data`
   returns.
2. The client reads no response before it sends the full body.
3. The client can send only the flow-control windows plus the response
   buffer of the server.
4. When these are full, the client and the server both wait.

On `main`, the response buffer is `mpsc::channel(32)`: 32 chunks between the
handler and hyper. On `inline-streaming-nodelay`, the writer in
`stream_response.rs` holds one chunk. `send_data` waits until hyper takes the
previous chunk. With 64 KiB chunks, the HTTP/2 limit is about 1 MiB lower with
inline streaming.

The HTTP/1.1 limit is the same on all trees. The kernel socket buffers set the
HTTP/1.1 limit.

### Errors on all trees

- `echo-h2-c64` and `echo-h2-c1000`: the h2 0.4.19 client closes connections
  with `too_many_data_frames`. The errors in each window are 3,138-3,415 at 64
  streams and 62,803-63,217 at 1,000 streams, on all trees. The `/echo`
  handler sends each request chunk back unchanged. The client's h2 cuts its
  upload into small chunks to fit the flow-control credit from the server.
  With the h2 0.4.17 client, the same scenarios have no errors.
- `echosf2m-h2oldmux-c16`: 16 send-first streams on one HTTP/2 connection fill
  the connection window. All trees time out.

## Merge decision

`inline-streaming-nodelay` (`8a211ec`) is not merged into `main`. These items
are open:

- Short HTTP/2 streams at 5,000 and 10,000: TTFB p50 increases 2.8-3.0 times
  and peak RSS increases 1.9 times against `main`. At 10,000 streams,
  throughput decreases 3.6%. `listener-nodelay` alone decreases throughput
  19.4% at 10,000 streams. At 5,000 streams, throughput increases 5.6%, and
  TTLB p99 decreases from 51.4 ms to 31.1 ms.
- `echo-h1-c1000`: throughput decreases 22.9%.
- `echo-h2-c64` and `echo-h2old-c64`: throughput decreases 9.7% and 9.2%.

The send-first echo limit is a property of send-first clients on all trees.
The planned work and the open questions are in [`TODO.md`](../TODO.md).
