#!/usr/bin/env bash
# Profiles the server during one closed-loop HTTP/2 short-stream load.
# This is a diagnosis aid; it is not part of the measured benchmark. perf adds
# overhead, so the throughput of these runs is not comparable with run.sh results.
#
# Usage: diag-h2-profile.sh <label> <server-binary> <out-dir> [client args...]
#
# Phases inside one load run (after a 5 s warm-up):
#   1. per-thread CPU and context switches, server write syscalls and bytes, and TCP
#      segments over 5 s, from /proc (no profiler)
#   2. perf record, CPU samples with DWARF call graphs, 5 s
#   3. perf record, sched:sched_switch events with call graphs, 3 s
set -euo pipefail
set -f
label="$1" sbin="$2" out="$3"; shift 3
ROOT="${WSB_ROOT:-$HOME/ws-bench}"  # the run.sh work directory
CLIENT_BIN="${CLIENT_BIN:-$ROOT/build/client-target/release/wsb-client}"
SERVER_CPUS="${SERVER_CPUS:-0-23}" CLIENT_CPUS="${CLIENT_CPUS:-24-63}"
PORT="${PORT:-27443}"
mkdir -p "$out"

( ulimit -n 1048576; exec taskset -c "$SERVER_CPUS" "$sbin" --port "$PORT" --workers 24 ) > "$out/$label.server.log" 2>&1 &
spid=$!
# Stop the server however the script ends.
trap 'kill -TERM "$spid" 2>/dev/null || true' EXIT
until grep -q '^READY' "$out/$label.server.log"; do sleep 0.1; done

( ulimit -n 1048576; exec taskset -c "$CLIENT_CPUS" "$CLIENT_BIN" --addr 127.0.0.1:$PORT --threads 40 \
    --server-pid "$spid" --warmup 5 --measure 22 --label "$label" "$@" ) > "$out/$label.client.json" 2> "$out/$label.client.err" &
cpid=$!

threads() {  # tid comm utime+stime(ticks) voluntary nonvoluntary
  local t
  for t in $(find /proc/$spid/task -mindepth 1 -maxdepth 1); do
    local tid=${t##*/} st comm ticks v n
    st="$(cat "$t/stat" 2>/dev/null)" || continue
    comm="${st#*(}"; comm="${comm%%)*}"
    ticks=$(echo "${st##*)}" | awk '{print $12 + $13}')
    v=$(awk '/^voluntary_ctxt_switches/ {print $2}' "$t/status")
    n=$(awk '/^nonvoluntary_ctxt_switches/ {print $2}' "$t/status")
    echo "$tid ${comm// /_} $ticks $v $n"
  done
}

io_snap() {  # write syscalls, write bytes of the server; TCP segments sent machine-wide
  echo "$(awk '/^syscw/ {print $2}' /proc/$spid/io) $(awk '/^wchar/ {print $2}' /proc/$spid/io) $(awk '/^Tcp:/ && $2 ~ /^[0-9]/ {print $12}' /proc/net/snmp)"
}
sleep 7
threads > "$out/$label.threads.t0"; t0=$(date +%s.%N); io0=$(io_snap)
sleep 5
threads > "$out/$label.threads.t1"; t1=$(date +%s.%N); io1=$(io_snap)
echo "$io0 $io1 $t0 $t1" > "$out/$label.io.raw"
python3 - "$out/$label.threads.t0" "$out/$label.threads.t1" "$t0" "$t1" > "$out/$label.threads.txt" <<'PY'
import sys
a = {l.split()[0]: l.split() for l in open(sys.argv[1])}
b = {l.split()[0]: l.split() for l in open(sys.argv[2])}
dt = float(sys.argv[4]) - float(sys.argv[3])
rows = []
for tid, r in b.items():
    if tid not in a: continue
    cpu = (int(r[2]) - int(a[tid][2])) / 100 / dt
    vol = (int(r[3]) - int(a[tid][3])) / dt
    inv = (int(r[4]) - int(a[tid][4])) / dt
    rows.append((cpu, tid, r[1], vol, inv))
rows.sort(reverse=True)
print(f"window {dt:.2f}s; per-thread CPU as a fraction of one core; context switches per second")
print(f"{'tid':>8} {'thread':<18} {'cpu':>6} {'vol/s':>9} {'invol/s':>9}")
for cpu, tid, comm, vol, inv in rows:
    if cpu > 0.005 or vol > 10:
        print(f"{tid:>8} {comm:<18} {cpu:6.2f} {vol:9.0f} {inv:9.0f}")
tot = sum(r[0] for r in rows)
print(f"total cpu {tot:.2f} cores over {len(rows)} threads; vol/s {sum(r[3] for r in rows):.0f}; invol/s {sum(r[4] for r in rows):.0f}")
PY

sudo -n perf record -F 199 --call-graph dwarf,16384 -p "$spid" -o "$out/$label.cpu.perf.data" -- sleep 5 \
  > "$out/$label.perf-cpu.log" 2>&1 || true
sudo -n perf record -e 'sched:sched_switch/call-graph=dwarf,stack-size=8192/' -p "$spid" \
  -o "$out/$label.sched.perf.data" -- sleep 3 > "$out/$label.perf-sched.log" 2>&1 || true
sudo -n chown "$(id -u):$(id -g)" "$out/$label.cpu.perf.data" "$out/$label.sched.perf.data" || true

wait "$cpid" || true
kill -TERM "$spid"; wait "$spid" 2>/dev/null || true

# Reports: flat CPU profile, and the most common call stacks that lead to a switch.
perf report -i "$out/$label.cpu.perf.data" --no-children --sort symbol --stdio --percent-limit 0.5 \
  2>/dev/null | grep -v '^$' | head -120 > "$out/$label.cpu.flat.txt" || true
perf report -i "$out/$label.cpu.perf.data" --children --sort symbol --stdio -g none --percent-limit 2 \
  2>/dev/null | grep -v '^$' | head -120 > "$out/$label.cpu.children.txt" || true
perf script -i "$out/$label.sched.perf.data" -F comm,tid,event,ip,sym 2>/dev/null \
  | python3 -c '
import sys, collections
stacks = collections.Counter(); cur = []; n = 0
def flush():
    global cur, n
    if cur:
        n += 1
        frames = [f for f in cur if not any(x in f for x in ("__schedule", "schedule", "[unknown]", "__switch_to", "context_switch"))]
        stacks[" <- ".join(frames[:7])] += 1
    cur = []
for line in sys.stdin:
    s = line.strip()
    if not s:
        flush(); continue
    if "sched:sched_switch" in s:
        flush(); continue
    parts = s.split(None, 1)
    if len(parts) == 2: cur.append(parts[1].split("+0x")[0])
flush()
print(f"{n} context switches sampled")
for st, c in stacks.most_common(25):
    print(f"{100*c/max(n,1):5.1f}%  {st}")
' > "$out/$label.sched.stacks.txt" || true
echo "done $label"
