#!/usr/bin/env bash
# Streaming load benchmark for av-web-service.
#
# Usage:
#   run.sh <web-services-source-dir> <label> [<source-dir> <label> ...]
#
# With one tree, the script measures that tree. With two or more trees, the script
# interleaves the trees inside each repeat and rotates their order between repeats,
# so that drift on the machine affects all trees equally. The summary compares each
# tree with the tree listed before it.
#
# Environment variables (defaults in brackets):
#   REPEATS [3]          runs of each scenario for each tree
#   ONLY [.]             extended regex; run only the scenarios whose name matches
#   SERVER_CPUS [0-23]   server cores (taskset)
#   CLIENT_CPUS [24-63]  load generator cores (taskset)
#   WARMUP [5]  MEASURE [10]   closed-loop warm-up and measurement seconds
#   STALL_WARMUP [3]  STALL [5] slow-reader warm-up and stall seconds
#   IDLE_MAX_PCT [1.0]   maximum machine CPU use (percent of all cores) before a run
#   RESULTS_NAME         results directory name [<UTC timestamp>-<labels>]
#   SCENARIO_FILE        file with scenario lines that replace the built-in table
#   WSB_ROOT [~/ws-bench] work directory for the Cargo cache, builds and results
#
# Results: $WSB_ROOT/results/<RESULTS_NAME>/ (runs.jsonl, summary.md, summary.csv,
# environment, build records, and the exact command of every process).
set -euo pipefail
set -f  # request paths contain '?'; no pathname expansion anywhere

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The work directory is outside the source tree, so that a run adds no files to the
# web-services checkout.
ROOT="${WSB_ROOT:-$HOME/ws-bench}"
export CARGO_HOME="$ROOT/cargo-home"
export PATH="$HOME/.cargo/bin:$PATH"

if (( $# < 2 || $# % 2 != 0 )); then
  sed -n '2,26p' "$0"; exit 2
fi
mkdir -p "$ROOT"

REPEATS="${REPEATS:-3}"
ONLY="${ONLY:-.}"
SERVER_CPUS="${SERVER_CPUS:-0-23}"
CLIENT_CPUS="${CLIENT_CPUS:-24-63}"
WARMUP="${WARMUP:-5}"
MEASURE="${MEASURE:-10}"
STALL_WARMUP="${STALL_WARMUP:-3}"
STALL="${STALL:-5}"
IDLE_MAX_PCT="${IDLE_MAX_PCT:-1.0}"

count_cpus() {  # "0-31" or "0-15,32-47" -> number of cores
  local n=0 part a b
  IFS=',' read -ra parts <<< "$1"
  for part in "${parts[@]}"; do
    if [[ $part == *-* ]]; then a=${part%-*}; b=${part#*-}; n=$((n + b - a + 1)); else n=$((n + 1)); fi
  done
  echo "$n"
}
SERVER_WORKERS="$(count_cpus "$SERVER_CPUS")"
CLIENT_THREADS="$(count_cpus "$CLIENT_CPUS")"

ARGS_ORIG="$*"
TREES=(); LABELS=()
while (( $# )); do
  TREES+=("$(cd "$1" && pwd)"); LABELS+=("$2"); shift 2
done
LABEL_JOIN="$(IFS=-; echo "${LABELS[*]}")"
RESULTS_NAME="${RESULTS_NAME:-$(date -u +%Y%m%dT%H%M%SZ)-$LABEL_JOIN}"
OUT="$ROOT/results/$RESULTS_NAME"
mkdir -p "$OUT/logs" "$OUT/builds"
CMDLOG="$OUT/commands.log"
echo "# $(date -u +%FT%TZ) $0 $ARGS_ORIG" >> "$CMDLOG"
echo "# env: REPEATS=$REPEATS ONLY=$ONLY SERVER_CPUS=$SERVER_CPUS CLIENT_CPUS=$CLIENT_CPUS WARMUP=$WARMUP MEASURE=$MEASURE STALL_WARMUP=$STALL_WARMUP STALL=$STALL IDLE_MAX_PCT=$IDLE_MAX_PCT" >> "$CMDLOG"
log() { echo "[$(date -u +%T)] $*" | tee -a "$OUT/progress.log" >&2; }

# ------------------------------------------------------------------ environment
{
  echo "date_utc: $(date -u +%FT%TZ)"
  echo "host: $(hostname)"
  echo "kernel: $(uname -srvm)"
  echo "rustc: $(rustc -V)"
  echo "cargo: $(cargo -V)"
  echo "server_cpus: $SERVER_CPUS (tokio worker_threads=$SERVER_WORKERS)"
  echo "client_cpus: $CLIENT_CPUS (tokio worker_threads=$CLIENT_THREADS)"
  echo "warmup_s: $WARMUP measure_s: $MEASURE stall_warmup_s: $STALL_WARMUP stall_s: $STALL repeats: $REPEATS"
  echo "ulimit_nofile_per_process: 1048576 (hard limit $(ulimit -Hn))"
  for f in /proc/sys/net/ipv4/tcp_rmem /proc/sys/net/ipv4/tcp_wmem /proc/sys/net/ipv4/tcp_mem \
           /proc/sys/net/core/somaxconn /proc/sys/net/ipv4/ip_local_port_range \
           /proc/sys/net/ipv4/tcp_tw_reuse /proc/sys/net/core/rmem_max /proc/sys/net/core/wmem_max \
           /sys/kernel/mm/transparent_hugepage/enabled; do
    echo "$f: $(tr '\t\n' '  ' < "$f")"
  done
  echo "--- lscpu"; lscpu
  echo "--- free -g"; free -g
} > "$OUT/environment.txt"

# ------------------------------------------------------------------ builds
CLIENT_SRC="$BENCH_DIR/client"
CLIENT_TARGET="$ROOT/build/client-target"
log "building client"
( cd "$CLIENT_SRC" && CARGO_TARGET_DIR="$CLIENT_TARGET" cargo build --release --locked ) > "$OUT/builds/client.log" 2>&1
CLIENT_BIN="$CLIENT_TARGET/release/wsb-client"
sha256sum "$CLIENT_BIN" > "$OUT/builds/client.sha256"

# The same client with h2 0.4.17, which predates the h2 DATA frame budget check
# (0.4.18). Scenarios that name `@client=h2-0417` use it.
LEGACY_DIR="$ROOT/build/client-h2-0417"
log "building client with h2 0.4.17"
rm -rf "$LEGACY_DIR" && mkdir -p "$LEGACY_DIR"
cp -r "$CLIENT_SRC/src" "$CLIENT_SRC/Cargo.toml" "$CLIENT_SRC/Cargo.lock" "$LEGACY_DIR/"
( cd "$LEGACY_DIR" && cargo update -p h2 --precise 0.4.17 \
  && CARGO_TARGET_DIR="$ROOT/build/client-h2-0417-target" cargo build --release ) \
  > "$OUT/builds/client-h2-0417.log" 2>&1
LEGACY_CLIENT_BIN="$ROOT/build/client-h2-0417-target/release/wsb-client"
sha256sum "$LEGACY_CLIENT_BIN" >> "$OUT/builds/client.sha256"

SERVER_BINS=()
for i in "${!TREES[@]}"; do
  tree="${TREES[$i]}"; label="${LABELS[$i]}"
  bdir="$ROOT/build/$label/server"
  log "building server for $label from $tree"
  rm -rf "$bdir/src" && mkdir -p "$bdir"
  cp -r "$BENCH_DIR/server/src" "$bdir/src"
  sed "s#@WS_SRC@#$tree#" "$BENCH_DIR/server/Cargo.toml.in" > "$bdir/Cargo.toml"
  # Seed the lock file from the tree under test, so that shared dependencies use the
  # versions that the tree pins. Cargo adds only what the bench server needs on top.
  cp "$tree/Cargo.lock" "$bdir/Cargo.lock"
  cp "$tree/Cargo.lock" "$OUT/builds/$label.Cargo.lock.seed"
  ( cd "$bdir" && CARGO_TARGET_DIR="$ROOT/build/$label/target" cargo build --release ) \
    > "$OUT/builds/$label.server-build.log" 2>&1
  bin="$ROOT/build/$label/target/release/wsb-server"
  SERVER_BINS+=("$bin")
  {
    echo "label: $label"
    echo "source: $tree"
    echo "git_head: $(git -C "$tree" rev-parse HEAD 2>/dev/null || echo n/a)"
    echo "git_branch: $(git -C "$tree" rev-parse --abbrev-ref HEAD 2>/dev/null || echo n/a)"
    echo "git_status_porcelain:"; git -C "$tree" status --porcelain 2>/dev/null | sed 's/^/  /'
    echo "server_binary_sha256: $(sha256sum "$bin" | cut -d' ' -f1)"
    echo "client_binary_sha256: $(cut -d' ' -f1 < "$OUT/builds/client.sha256")"
    echo "lock file: packages in both the seed and the build with different versions, and packages added:"
    python3 - "$tree/Cargo.lock" "$bdir/Cargo.lock" <<'PY'
import re, sys
def pk(p):
    d = {}
    for name, ver in re.findall(r'name = "([^"]+)"\nversion = "([^"]+)"', open(p).read()):
        d.setdefault(name, set()).add(ver)
    return d
seed, used = pk(sys.argv[1]), pk(sys.argv[2])
for n in sorted(used):
    if n not in seed:
        print(f"  added {n} {sorted(used[n])}")
    elif not used[n] <= seed[n]:
        print(f"  changed {n} {sorted(seed[n])} -> {sorted(used[n])}")
PY
  } > "$OUT/builds/$label.txt"
  cp "$bdir/Cargo.lock" "$OUT/builds/$label.Cargo.lock.used"
done

# ------------------------------------------------------------------ scenarios
# name | proto | mode | conc | per_conn | path | expect_bytes | extra client args
# echo-*: the client sends the request body and reads the response at the same time.
# echosf*: the client sends the whole request body before it reads any of the response
# (--send-first). A stream that does not finish in 5 s counts as a timeout error.
SCENARIOS="$(cat <<'EOF'
short-h1-c64        |h1|closed|64   |1 |/stream?chunks=4&size=16384|65536|
short-h1-c1000      |h1|closed|1000 |1 |/stream?chunks=4&size=16384|65536|
short-h1-c5000      |h1|closed|5000 |1 |/stream?chunks=4&size=16384|65536|
short-h1-c10000     |h1|closed|10000|1 |/stream?chunks=4&size=16384|65536|
short-h2-c64        |h2|closed|64   |64|/stream?chunks=4&size=16384|65536|
short-h2-c1000      |h2|closed|1000 |64|/stream?chunks=4&size=16384|65536|
short-h2-c5000      |h2|closed|5000 |64|/stream?chunks=4&size=16384|65536|
short-h2-c10000     |h2|closed|10000|64|/stream?chunks=4&size=16384|65536|
short-h2x1-c1000    |h2|closed|1000 |1 |/stream?chunks=4&size=16384|65536|
short-h3-c64        |h3|closed|64   |64|/stream?chunks=4&size=16384|65536|
short-h3-c1000      |h3|closed|1000 |64|/stream?chunks=4&size=16384|65536|
long256k-h1-c64     |h1|closed|64   |1 |/stream?chunks=32&size=262144|8388608|
long256k-h1-c1000   |h1|closed|1000 |1 |/stream?chunks=32&size=262144|8388608|
long256k-h1-c5000   |h1|closed|5000 |1 |/stream?chunks=32&size=262144|8388608|
long256k-h2-c64     |h2|closed|64   |64|/stream?chunks=32&size=262144|8388608|
long256k-h2-c1000   |h2|closed|1000 |64|/stream?chunks=32&size=262144|8388608|
long256k-h2-c5000   |h2|closed|5000 |64|/stream?chunks=32&size=262144|8388608|
long256k-h3-c64     |h3|closed|64   |64|/stream?chunks=32&size=262144|8388608|
long1m-h1-c64       |h1|closed|64   |1 |/stream?chunks=16&size=1048576|16777216|
long1m-h1-c1000     |h1|closed|1000 |1 |/stream?chunks=16&size=1048576|16777216|
long1m-h2-c64       |h2|closed|64   |64|/stream?chunks=16&size=1048576|16777216|
long1m-h2-c1000     |h2|closed|1000 |64|/stream?chunks=16&size=1048576|16777216|
cpu-h1-c4           |h1|closed|4    |1 |/stream?chunks=64&size=65536&cpu=1|4194304|
cpu-h1-c64          |h1|closed|64   |1 |/stream?chunks=64&size=65536&cpu=1|4194304|
cpu-h1-c1000        |h1|closed|1000 |1 |/stream?chunks=64&size=65536&cpu=1|4194304|
cpu-h2-c4           |h2|closed|4    |64|/stream?chunks=64&size=65536&cpu=1|4194304|
cpu-h2-c64          |h2|closed|64   |64|/stream?chunks=64&size=65536&cpu=1|4194304|
cpu-h2-c1000        |h2|closed|1000 |64|/stream?chunks=64&size=65536&cpu=1|4194304|
echo-h1-c64         |h1|closed|64   |1 |/echo|1048576|--post-bytes 1048576 --post-chunk 65536
echo-h1-c1000       |h1|closed|1000 |1 |/echo|1048576|--post-bytes 1048576 --post-chunk 65536
echo-h2-c64         |h2|closed|64   |64|/echo|1048576|--post-bytes 1048576 --post-chunk 65536
echo-h2-c1000       |h2|closed|1000 |64|/echo|1048576|--post-bytes 1048576 --post-chunk 65536
echo-h2old-c64      |h2|closed|64   |64|/echo|1048576|--post-bytes 1048576 --post-chunk 65536 @client=h2-0417
echo-h2old-c1000    |h2|closed|1000 |64|/echo|1048576|--post-bytes 1048576 --post-chunk 65536 @client=h2-0417
echosf2m-h1-c16     |h1|closed|16   |1 |/echo|2097152|--post-bytes 2097152 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144
echosf2m-h2old-c16  |h2|closed|16   |1 |/echo|2097152|--post-bytes 2097152 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144 @client=h2-0417
echosf2m-h2oldmux-c16|h2|closed|16   |16|/echo|2097152|--post-bytes 2097152 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144 @client=h2-0417
echosf4m-h1-c16     |h1|closed|16   |1 |/echo|4194304|--post-bytes 4194304 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144
echosf3m-h2old-c16  |h2|closed|16   |1 |/echo|3145728|--post-bytes 3145728 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144 @client=h2-0417
echosf4m-h2old-c16  |h2|closed|16   |1 |/echo|4194304|--post-bytes 4194304 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144 @client=h2-0417
echosf8m-h1-c16     |h1|closed|16   |1 |/echo|8388608|--post-bytes 8388608 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144
echosf16m-h1-c16    |h1|closed|16   |1 |/echo|16777216|--post-bytes 16777216 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144
echosf16m-h2old-c16 |h2|closed|16   |1 |/echo|16777216|--post-bytes 16777216 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144 @client=h2-0417
echosf64m-h1-c16    |h1|closed|16   |1 |/echo|67108864|--post-bytes 67108864 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144
echosf64m-h2old-c16 |h2|closed|16   |1 |/echo|67108864|--post-bytes 67108864 --post-chunk 65536 --send-first 1 --stream-timeout 5 --rcvbuf 262144 @client=h2-0417
slow64k-h1-c64      |h1|stall |64   |1 |/stream?chunks=256&size=65536|16777216|--rcvbuf 262144
slow64k-h1-c1000    |h1|stall |1000 |1 |/stream?chunks=256&size=65536|16777216|--rcvbuf 262144
slow64k-h1-c5000    |h1|stall |5000 |1 |/stream?chunks=256&size=65536|16777216|--rcvbuf 262144
slow64k-h2-c64      |h2|stall |64   |64|/stream?chunks=256&size=65536|16777216|--rcvbuf 262144 --h2-conn-window 134217728
slow64k-h2-c1000    |h2|stall |1000 |64|/stream?chunks=256&size=65536|16777216|--rcvbuf 262144 --h2-conn-window 134217728
slow64k-h2-c5000    |h2|stall |5000 |64|/stream?chunks=256&size=65536|16777216|--rcvbuf 262144 --h2-conn-window 134217728
slow64k-h3-c1000    |h3|stall |1000 |64|/stream?chunks=256&size=65536|16777216|
slow256k-h1-c64     |h1|stall |64   |1 |/stream?chunks=128&size=262144|33554432|--rcvbuf 262144
slow256k-h1-c1000   |h1|stall |1000 |1 |/stream?chunks=128&size=262144|33554432|--rcvbuf 262144
slow256k-h2-c64     |h2|stall |64   |64|/stream?chunks=128&size=262144|33554432|--rcvbuf 262144 --h2-conn-window 134217728
slow256k-h2-c1000   |h2|stall |1000 |64|/stream?chunks=128&size=262144|33554432|--rcvbuf 262144 --h2-conn-window 134217728
EOF
)"
# SCENARIO_FILE replaces the table above with the lines of that file (same format).
if [[ -n ${SCENARIO_FILE:-} ]]; then SCENARIOS="$(grep -v '^#' "$SCENARIO_FILE")"; fi
echo "$SCENARIOS" > "$OUT/scenarios.txt"

# ------------------------------------------------------------------ helpers
cpu_busy_pct() {  # whole-machine CPU use over one second, percent of all cores
  local a b
  a=($(head -1 /proc/stat)); sleep 1; b=($(head -1 /proc/stat))
  local idle=$(( (b[4]+b[5]) - (a[4]+a[5]) )) total=0 i
  for i in 1 2 3 4 5 6 7 8; do total=$(( total + b[i] - a[i] )); done
  awk -v i="$idle" -v t="$total" 'BEGIN { printf "%.2f", (t > 0 ? 100 * (t - i) / t : 0) }'
}

wait_idle() {
  local pct tries=0
  while :; do
    pct="$(cpu_busy_pct)"
    if awk -v p="$pct" -v m="$IDLE_MAX_PCT" 'BEGIN { exit !(p <= m) }'; then echo "$pct"; return; fi
    tries=$((tries + 1))
    log "machine busy (${pct}% of all cores); waiting"
    ps -eo pid,user,pcpu,comm --sort=-pcpu | head -6 >> "$OUT/progress.log"
    if (( tries >= 60 )); then log "machine did not become idle; continuing"; echo "$pct"; return; fi
    sleep 2
  done
}

RUN_SEQ=0
run_one() {  # scenario-line tree-index repeat
  local line="$1" ti="$2" rep="$3"
  IFS='|' read -r name proto mode conc per path expect extra <<< "$line"
  name="$(echo $name)"; proto="$(echo $proto)"; mode="$(echo $mode)"
  conc="$(echo $conc)"; per="$(echo $per)"; path="$(echo $path)"; expect="$(echo $expect)"
  local label="${LABELS[$ti]}" sbin="${SERVER_BINS[$ti]}" cbin="$CLIENT_BIN"
  if [[ $extra == *@client=h2-0417* ]]; then
    cbin="$LEGACY_CLIENT_BIN"; extra="${extra//@client=h2-0417/}"
  fi
  RUN_SEQ=$((RUN_SEQ + 1))
  local port=$(( 20000 + (RUN_SEQ % 400) * 10 ))
  local tag="${name}.${label}.r${rep}"
  local idle; idle="$(wait_idle)"

  local scmd="taskset -c $SERVER_CPUS $sbin --port $port --workers $SERVER_WORKERS"
  echo "$tag server: ulimit -n 1048576; $scmd" >> "$CMDLOG"
  ( ulimit -n 1048576; exec $scmd ) > "$OUT/logs/$tag.server.log" 2>&1 &
  local spid=$!
  local waited=0
  until grep -q '^READY' "$OUT/logs/$tag.server.log" 2>/dev/null; do
    sleep 0.1; waited=$((waited + 1))
    if (( waited > 200 )) || ! kill -0 "$spid" 2>/dev/null; then
      log "$tag: server did not start"; kill -9 "$spid" 2>/dev/null || true; return
    fi
  done

  local timing
  if [[ $mode == stall ]]; then timing="--warmup $STALL_WARMUP --stall $STALL"; else timing="--warmup $WARMUP --measure $MEASURE"; fi
  local ccmd="taskset -c $CLIENT_CPUS $cbin --addr 127.0.0.1:$port --proto $proto --mode $mode --conc $conc --per-conn $per --path $path --expect-bytes $expect --threads $CLIENT_THREADS --server-pid $spid --label $tag $timing $extra"
  echo "$tag client: ulimit -n 1048576; $ccmd" >> "$CMDLOG"
  local json
  json="$( ( ulimit -n 1048576; exec $ccmd ) 2> "$OUT/logs/$tag.client.log" | tail -1 )" || true

  kill -TERM "$spid" 2>/dev/null || true
  for _ in $(seq 1 100); do kill -0 "$spid" 2>/dev/null || break; sleep 0.1; done
  kill -9 "$spid" 2>/dev/null || true
  wait "$spid" 2>/dev/null || true

  if [[ -z $json || ${json:0:1} != "{" ]]; then
    log "$tag: client produced no result"
    json='{"fatal":"no client output"}'
  fi
  python3 - "$json" "$name" "$label" "$rep" "$idle" "$port" >> "$OUT/runs.jsonl" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
d.update(scenario=sys.argv[2], tree=sys.argv[3], repeat=int(sys.argv[4]),
         idle_busy_pct_before=float(sys.argv[5]), port=int(sys.argv[6]))
print(json.dumps(d))
PY
  python3 - "$json" "$tag" <<'PY' 2>&1 | tee -a "$OUT/progress.log" >&2 || true
import json, sys
d = json.loads(sys.argv[1]); t = sys.argv[2]
if "fatal" in d: print(f"  {t}: FATAL {d['fatal']}"); sys.exit()
def f(v, spec):
    return "n/a" if v is None else format(v, spec)
e = d["errors"]["total_in_window"]; s = d["server"]
if d["config"]["mode"] == "stall":
    print(f"  {t}: plateau {f(s['plateau_kb_per_stream'], '.0f')} KB/stream peak {f(s['peak_kb_per_stream'], '.0f')} KB/stream "
          f"drain {f(d['drain_s'], '.2f')}s ttfb p50 {f(d['ttfb']['p50_ms'], '.1f')}ms errors {e}")
else:
    print(f"  {t}: {f(d['streams_per_s'], '.0f')} streams/s {f(d['gib_per_s'], '.2f')} GiB/s ttlb p50 {f(d['ttlb']['p50_ms'], '.1f')}ms "
          f"p99 {f(d['ttlb']['p99_ms'], '.1f')}ms srv {f(s['cpu_util_cores'], '.1f')} cores cli {f(d['client']['cpu_util_frac'] * 100, '.0f')}% errors {e}")
PY
  sleep 1
}

# ------------------------------------------------------------------ main loop
log "results in $OUT"
for rep in $(seq 1 "$REPEATS"); do
  while IFS= read -r line; do
    [[ -z $line ]] && continue
    name="$(echo ${line%%|*})"  # unquoted: trims the padding
    [[ $name =~ $ONLY ]] || continue
    # Rotate the tree order by one place for each repeat, so that each tree runs
    # first in one repeat (with three trees and three repeats).
    order=(); nt=${#TREES[@]}
    for k in $(seq 0 $((nt - 1))); do order+=($(( (k + rep - 1) % nt ))); done
    for ti in "${order[@]}"; do
      run_one "$line" "$ti" "$rep"
    done
  done <<< "$SCENARIOS"
done

python3 "$BENCH_DIR/summarize.py" "$OUT"
log "done: $OUT/summary.md"
