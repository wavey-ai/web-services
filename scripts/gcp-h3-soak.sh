#!/usr/bin/env bash

set -euo pipefail

binary=${1:?"usage: gcp-h3-soak.sh BINARY TARGET CA OUTPUT_PREFIX"}
target=${2:?"usage: gcp-h3-soak.sh BINARY TARGET CA OUTPUT_PREFIX"}
ca=${3:?"usage: gcp-h3-soak.sh BINARY TARGET CA OUTPUT_PREFIX"}
output_prefix=${4:?"usage: gcp-h3-soak.sh BINARY TARGET CA OUTPUT_PREFIX"}
warmup_seconds=${WARMUP_SECONDS:-600}
duration_seconds=${DURATION_SECONDS:-3600}
connections=${CONNECTIONS:-32}
pipeline_depth=${PIPELINE_DEPTH:-8}
response_bytes=${RESPONSE_BYTES:-5760}
server_name=${SERVER_NAME:-local.infidelity.io}

run_segment() {
    local segment_name=$1
    local seconds=$2

    /usr/bin/time -v -o "${output_prefix}-${segment_name}-client-time.txt" \
        "$binary" \
        --mode client \
        --tls-ca "$ca" \
        --server-name "$server_name" \
        --target "$target" \
        --response-bytes "$response_bytes" \
        --duration-seconds "$seconds" \
        --connection-steps "$connections" \
        --pipeline-depth "$pipeline_depth" \
        >"${output_prefix}-${segment_name}.json" \
        2>"${output_prefix}-${segment_name}.err"
}

run_phase() {
    local phase=$1
    local remaining=$2
    local segment=1

    while ((remaining > 0)); do
        local seconds=$remaining
        local segment_name
        if ((seconds > 300)); then
            seconds=300
        fi
        printf -v segment_name '%s-%03d' "$phase" "$segment"
        run_segment "$segment_name" "$seconds"
        remaining=$((remaining - seconds))
        segment=$((segment + 1))
    done
}

run_phase warmup "$warmup_seconds"
run_phase soak "$duration_seconds"
touch "${output_prefix}.done"
