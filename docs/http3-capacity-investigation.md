# HTTP/3 capacity investigation

Status: active investigation, 18 July 2026.

## Discussion summary

We are keeping persistent HTTP/3 as the production transport for LL-HLS and
WebTransport. HTTP/1.1 and HTTP/2 are useful controls, but the purpose of this
work is to find correctness or performance bugs in our H3 implementation, not
to move delivery away from H3.

The first isolated result is encouraging and concerning at the same time. A
two-vCPU `web-service` server sustained the requested cadence through 48
simulated 16-channel PCM customers, which is almost 1 Gbit/s of H3 traffic. It
then failed to hold cadence at 56 customers. The TCP control rates are much
higher, but those controls were collected locally with a different harness, so
the size of the apparent gap is a reason for a controlled A/B investigation,
not yet a protocol conclusion.

The profile's strongest lead is below the application router: the GCP virtio
NIC reported `tx-udp-segmentation: off [fixed]`. Quinn can batch UDP output with
GSO, but this host still has to split the large UDP batches into packets in the
kernel. The profile consequently spent substantial time in `sendmsg`, UDP, and
software segmentation paths. Packet capture then showed that Quinn was already
coalescing the H3 headers and data correctly; it was not emitting an avoidable
extra packet per part. We also found avoidable work in our own H3 path; each
item will be changed and measured independently.

## Workload

The capacity runner uses the production `web_service::H2H3Server`, TLS, Quinn,
and the `h3` crate. It returns a fixed `5,760`-byte body, the size of 5 ms of
48 kHz, 24-bit PCM for eight channels:

```text
48,000 samples/s * 0.005 s * 8 channels * 3 bytes = 5,760 bytes
```

One simulated 16-channel customer reads two eight-channel renditions. It
therefore requests 400 media parts per second and receives `18.432 Mbit/s`
before HTTP/3, QUIC, UDP, and IP overhead. Connections are persistent; the test
does not create a new QUIC connection for each media part.

The measured path was a London load generator to an Amsterdam server. Both were
GCP `n2-standard-2` instances with two vCPUs. Reference ICMP RTT averaged about
`12.94 ms` with no observed ping loss.

## Results

The following steps used one remote load-generator host and Quinn `0.11.11`.
An "exact" result completed every scheduled request with no reported request
error or scheduling backpressure.

| Customers | Requests/s | Payload Mbit/s | Wire Mbit/s | p99 request ms | Result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 25 | 10,000 | 460.8 | 493.8 | 11.8 | exact |
| 32 | 12,800 | 589.8 | 631.9 | 15.3 | exact |
| 40 | 16,000 | 737.3 | 789.8 | 13.0 | exact |
| 48 | 19,200 | 884.7 | 947.7 | 14.1 | exact |
| 56 | 22,400 scheduled | 1,032.2 scheduled | 1,036.0 achieved wire | 58.7 | 95.22% complete; 5,354 backpressure events |
| 64 | 25,600 scheduled | 1,179.6 scheduled | 970.7 achieved wire | 189.0 | 82.70% complete; 22,134 backpressure events |

At 48 customers the server used about `177%` CPU and the single generator used
about `186%`. At 56 customers both approached `190%`, so that run cannot assign
the ceiling solely to the server.

A later two-generator attempt confirmed that the load-generator boundary must
also be qualified. With 20 London customers and 20 New York customers, London
was exact while the long-RTT New York generator reported backpressure and late
requests even though server CPU remained around `116%` to `120%`. Increasing
the client pipeline did not remove the asymmetry. That result is evidence of a
long-RTT client scheduling or transport limit, not a server saturation result.
The capacity runner must therefore require exact request count, zero errors,
and zero backpressure independently for every generator.

The local control harness reported approximately `56,600` persistent H1.1
responses/s and `42,100` persistent H2 responses/s for the same body size. Those
numbers are deliberately not in the table: client and server shared a host and
the H3 test used two hosts, so comparing them as if only the protocol changed
would be misleading.

## What the profile showed

The server profile sampled the two-generator run at 499 Hz with no lost samples.
Inclusive call-tree percentages attributed approximately:

- `21.2%` of samples to system-call descendants;
- `14.8%` to the `sendmsg` family and `12.2%` to `udp_sendmsg` descendants;
- visible kernel work in `__udp_gso_segment` and software `skb_segment`;
- `1.8%` flat to AES-GCM sealing and another roughly `2%` to lower-level AES and
  GHASH work;
- about `3.1%` flat to `malloc` plus `free`;
- `1.3%` flat to stateless QPACK string encoding; and
- about `0.4%` flat to Tokio task scheduling.

The host had generic segmentation enabled, but UDP transmit segmentation was
fixed off by the virtual NIC. This means Quinn's UDP GSO batching can reduce
syscall count while packet segmentation still consumes kernel CPU. A 5,760-byte
part also spans several QUIC packets, so this cost repeats 19,200 times per
second at the 48-customer step.

These percentages do not prove that the kernel is the only bottleneck. They do
show that per-request Tokio task creation is not, by itself, a plausible
explanation for a several-times gap in this sample.

## Packetization result

A capture of one persistent H3 connection at 200 responses per second showed
about five server packets for every 5,760-byte response at the GCP path's MTU.
The predominant server packet length was `1,420` bytes, with a smaller final
packet. There was no repeatable sixth header-only packet. On receive, generic
receive offload commonly presented approximately `6,024`-byte batches to the
client.

The response therefore already uses the minimum practical packet count at this
MTU, and the H3 `HEADERS` plus `DATA` ordering is not defeating Quinn's batching.
Making each part smaller is not an option because it would change the measured
audio format; making QUIC packets larger requires a different network MTU. The
remaining packet-rate cost is the QUIC/UDP packets themselves and software
egress segmentation on the tested virtual NIC.

## GCP and Linode control

The same exact 40-customer workload was repeated twice on a dedicated two-core
Linode server. Both clouds used a virtio network device with
`tx-udp-segmentation: off [fixed]` and an MTU near 1,500 bytes.

| Server | Exact repeats | Server CPU | p99 request latency |
| --- | ---: | ---: | ---: |
| GCP `n2-standard-2` | 2/2 | `123.55%`, `122.63%` | `13.15 ms`, `13.08 ms` |
| dedicated Linode, AMD EPYC 7713 | 2/2 | `132.53%`, `132.37%` | `21.89 ms`, `16.23 ms` |

Hardware, clock rate, kernel, and path latency differed, so this is not a
processor-normalized benchmark. It does show that the H3 behavior is not unique
to GCP and gives no evidence that moving the test to Linode fixes the gap. GCP
remains the primary controlled environment.

## Why compare another Rust backend

Cloudflare's [quiche](https://github.com/cloudflare/quiche) is a Rust QUIC and
HTTP/3 implementation used at its edge. Cloudflare's
[tokio-quiche](https://github.com/cloudflare/tokio-quiche) integrates that stack
with Tokio and is used by production proxies. This is useful counter-evidence
to the idea that Rust itself explains the result. C implementations such as
[ngtcp2](https://github.com/ngtcp2/ngtcp2) remain useful external controls, but
a language rewrite is not justified by the current measurements.

`web-service` therefore keeps both Rust implementations behind an explicit
runtime selection:

- Quinn plus the Hyperium `h3` crates remains the default and continues to
  provide WebTransport.
- Building with `h3-tokio-quiche` adds `H3Backend::TokioQuiche` for ordinary H3
  requests through the same `Router` boundary.
- The tokio-quiche backend cannot be combined with WebTransport yet; the builder
  rejects that configuration instead of silently changing behavior.
- A Quinn client test makes 128 concurrent requests over one connection to the
  tokio-quiche server and verifies protocol version, status, headers, CORS, and
  body bytes. This proves cross-implementation behavior before cloud load
  testing.

This feature deliberately adds a Cargo dependency instead of copying
`web-service` into a load-test binary. It also keeps the Quinn dependencies in
the build so the runner can switch the server backend while using the same
Quinn client.

## Application-path audit

Inspection of [`web-service/src/h3.rs`](../web-service/src/h3.rs) found the
following candidates. None is being declared the root cause until an isolated
A/B test measures it:

- the ordinary request path allocates its URI path in the connection loop and
  allocates it again in the handler;
- it clones the complete request header map when only `Range` is needed;
- every accepted request creates a Tokio task and currently discards the task's
  handler error;
- every response constructs the same four CORS headers;
- the current `h3` response encoder uses stateless QPACK, so repeated response
  fields are encoded on every response; and
- transport and H3 defaults need explicit tests for stream limits, flow-control
  windows, datagrams, WebTransport, and GREASE under this part cadence.

The cache and routing layers are not current suspects. Direct release tests
returned cached parts at roughly `322,000` to `562,000` requests/s, cached
playlists at roughly `243,000` to `431,000` requests/s, and the underlying cache
at `4.7` to `9.2` million reads/s. Each is comfortably above the isolated H3
rate.

## Investigation plan

All performance changes must keep H3 and use the same fixed workload so a
regression cannot be hidden by changing the protocol or media shape.

1. Make the runner's qualification strict per generator: exact request count,
   zero errors, and zero scheduling backpressure. A nominal percentage alone is
   not a valid pass.
2. Run the same 40-customer GCP workload against Quinn and tokio-quiche, with a
   Quinn client in both cases. Compare exactness, CPU, wire bytes, and latency.
3. Add production-boundary H3 tests for response bytes, range requests,
   concurrent streams, disconnects, stream-limit pressure, and surfaced handler
   errors.
4. Remove the duplicate path allocation and full header-map clone, then A/B
   CPU, request rate, latency, allocation rate, and wire bytes.
5. Measure the canonical server against a minimal Quinn/`h3` static server on
   the same machines. This isolates `web-service` overhead without moving away
   from H3.
6. Compare UDP packet and raw QUIC controls with identical payload and pacing to
   separate kernel packetization, QUIC crypto/recovery, H3 framing, and router
   costs.
7. Repeat on a host with hardware UDP segmentation if one is available. Record
   NIC offload features with every capacity result.

An implementation change qualifies only when it preserves the exact H3
response behavior and improves repeated two-host results. H1/H2 remain controls;
they are not substitutes for a correct, high-capacity H3 path.
