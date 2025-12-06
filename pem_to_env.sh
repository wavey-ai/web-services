#!/usr/bin/env bash
set -euo pipefail

PEM_PATH="${1:-example.pem}"
OUT_PATH="${2:-.env}"

if [[ ! -f "${PEM_PATH}" ]]; then
  echo "PEM file not found: ${PEM_PATH}" >&2
  exit 1
fi

cert_block="$(awk '/BEGIN CERTIFICATE/{flag=1} flag{print} /END CERTIFICATE/{flag=0}' "${PEM_PATH}")"
if [[ -z "${cert_block}" ]]; then
  echo "No certificate block found in ${PEM_PATH}" >&2
  exit 1
fi

key_block="$(awk '/BEGIN PRIVATE KEY/{flag=1} flag{print} /END PRIVATE KEY/{flag=0}' "${PEM_PATH}")"
if [[ -z "${key_block}" ]]; then
  key_block="$(awk '/BEGIN RSA PRIVATE KEY/{flag=1} flag{print} /END RSA PRIVATE KEY/{flag=0}' "${PEM_PATH}")"
fi
if [[ -z "${key_block}" ]]; then
  echo "No private key block found in ${PEM_PATH}" >&2
  exit 1
fi

cert_b64="$(printf "%s\n" "${cert_block}" | base64 | tr -d '\n')"
key_b64="$(printf "%s\n" "${key_block}" | base64 | tr -d '\n')"

cat > "${OUT_PATH}" <<EOF
TLS_CERT_BASE64=${cert_b64}
TLS_KEY_BASE64=${key_b64}
EOF

echo "Wrote TLS_CERT_BASE64 and TLS_KEY_BASE64 to ${OUT_PATH}"
