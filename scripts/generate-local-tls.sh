#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tls_output="${1:-$repo_root/tls/local.wavey.ai/generated}"
tls_config="$repo_root/tls/local.wavey.ai/openssl.cnf"
mkdir -p "$tls_output"
tls_output="$(cd "$tls_output" && pwd)"
tls_staging="$(mktemp -d "$tls_output/.issue.XXXXXX")"

cleanup() {
    rm -f "$tls_staging/privkey.pem" "$tls_staging/request.pem" \
        "$tls_staging/cert.pem" "$tls_staging/fullchain.pem" "$tls_staging/openssl.log"
    rmdir "$tls_staging"
}
trap cleanup EXIT

run_openssl() {
    if ! openssl "$@" 2> "$tls_staging/openssl.log"; then
        sed -n '1,20p' "$tls_staging/openssl.log" >&2
        exit 1
    fi
}

if [[ -f "$tls_output/ca-key.pem" || -f "$tls_output/chain.pem" ]]; then
    if [[ ! -f "$tls_output/ca-key.pem" || ! -f "$tls_output/chain.pem" ]]; then
        printf 'The local CA requires both ca-key.pem and chain.pem in %s.\n' "$tls_output" >&2
        exit 1
    fi
    run_openssl x509 -in "$tls_output/chain.pem" -checkend 7776000 -noout
else
    run_openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
        -subj '/CN=Wavey Local Development CA' -config "$tls_config" \
        -extensions ca_extensions -keyout "$tls_output/ca-key.pem" \
        -out "$tls_output/chain.pem"
fi

run_openssl req -new -newkey rsa:2048 -nodes -config "$tls_config" \
    -keyout "$tls_staging/privkey.pem" -out "$tls_staging/request.pem"
run_openssl x509 -req -in "$tls_staging/request.pem" \
    -CA "$tls_output/chain.pem" -CAkey "$tls_output/ca-key.pem" \
    -set_serial "0x$(openssl rand -hex 16)" -days 90 \
    -extfile "$tls_config" -extensions server_extensions -out "$tls_staging/cert.pem"
run_openssl verify -CAfile "$tls_output/chain.pem" -purpose sslserver "$tls_staging/cert.pem"
openssl x509 -in "$tls_staging/cert.pem" -out "$tls_staging/fullchain.pem"
openssl x509 -in "$tls_output/chain.pem" >> "$tls_staging/fullchain.pem"
mv "$tls_staging/privkey.pem" "$tls_output/privkey.pem"
mv "$tls_staging/cert.pem" "$tls_output/cert.pem"
mv "$tls_staging/fullchain.pem" "$tls_output/fullchain.pem"
printf 'Local TLS files: %s\nClient CA certificate: %s/chain.pem\n' "$tls_output" "$tls_output"
