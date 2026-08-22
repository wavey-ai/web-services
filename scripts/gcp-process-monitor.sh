#!/usr/bin/env bash

set -euo pipefail

pid=${1:?"usage: gcp-process-monitor.sh PID PORT OUTPUT [SAMPLES]"}
port=${2:?"usage: gcp-process-monitor.sh PID PORT OUTPUT [SAMPLES]"}
output=${3:?"usage: gcp-process-monitor.sh PID PORT OUTPUT [SAMPLES]"}
samples=${4:-430}
interval_seconds=${INTERVAL_SECONDS:-10}

if [[ ! $pid =~ ^[1-9][0-9]*$ ]] || [[ ! $port =~ ^[1-9][0-9]*$ ]]; then
    echo "PID and port must be positive integers" >&2
    exit 2
fi

printf 'timestamp_utc\tpid\trss_kb\tthreads\tfds\ttcp_recv_q\ttcp_send_q\tudp_recv_q\tudp_send_q\n' >"$output"

for ((sample = 0; sample < samples; sample++)); do
    if [[ ! -r /proc/$pid/status ]]; then
        break
    fi

    rss_kb=$(awk '/^VmRSS:/ { print $2 }' "/proc/$pid/status")
    threads=$(awk '/^Threads:/ { print $2 }' "/proc/$pid/status")
    fds=$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)
    read -r tcp_recv_q tcp_send_q < <(
        ss -H -ltn "sport = :$port" | awk '{ recv += $2; send += $3 } END { print recv + 0, send + 0 }'
    )
    read -r udp_recv_q udp_send_q < <(
        ss -H -lun "sport = :$port" | awk '{ recv += $2; send += $3 } END { print recv + 0, send + 0 }'
    )
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        "$pid" \
        "${rss_kb:-0}" \
        "${threads:-0}" \
        "$fds" \
        "$tcp_recv_q" \
        "$tcp_send_q" \
        "$udp_recv_q" \
        "$udp_send_q" >>"$output"
    sleep "$interval_seconds"
done
