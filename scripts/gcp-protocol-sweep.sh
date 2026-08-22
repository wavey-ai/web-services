#!/usr/bin/env bash

set -euo pipefail

target=${1:?"usage: gcp-protocol-sweep.sh HOST:PORT OUTPUT"}
output=${2:?"usage: gcp-protocol-sweep.sh HOST:PORT OUTPUT"}
duration=${DURATION:-8s}
warmup=${WARMUP:-2s}
path=${BENCH_PATH:-/part.mp4}
authority=${BENCH_AUTHORITY:-local.infidelity.io}

: >"$output"

run_step() {
    local protocol=$1
    local connections=$2
    local streams=$3
    local threads=$connections
    local -a protocol_args=()

    if ((threads > 8)); then
        threads=8
    fi
    if [[ $protocol == h1 ]]; then
        protocol_args=(--h1)
    fi

    printf 'BEGIN protocol=%s connections=%s streams=%s threads=%s\n' \
        "$protocol" "$connections" "$streams" "$threads" >>"$output"
    /usr/bin/time \
        -f 'RESOURCE user=%U sys=%S cpu=%P elapsed=%e maxrss_kb=%M exit=%x' \
        h2load \
        "${protocol_args[@]}" \
        -D "$duration" \
        --warm-up-time="$warmup" \
        -c "$connections" \
        -m "$streams" \
        -t "$threads" \
        --connect-to="$target" \
        "https://${authority}:${target##*:}${path}" >>"$output" 2>&1
    printf 'END protocol=%s connections=%s\n' "$protocol" "$connections" >>"$output"
}

for connections in 1 2 4 8 16 24 32 48 64 96 128; do
    run_step h2 "$connections" 8
done

for connections in 1 2 4 8 16 24 32 48 64 96 128 192 256; do
    run_step h1 "$connections" 1
done

touch "${output}.done"
