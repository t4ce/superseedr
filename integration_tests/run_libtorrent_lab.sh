#!/usr/bin/env bash
set -euo pipefail

TIMEOUT="${LIBTORRENT_LAB_TIMEOUT_SECS:-120}"

ARGS=(--timeout-secs "$TIMEOUT")

if [[ $# -gt 0 && "${1}" != --* ]]; then
  ARGS+=(--scenario "$1")
  shift
elif [[ "${LIBTORRENT_LAB_SCENARIO:-}" != "" ]]; then
  ARGS+=(--scenario "$LIBTORRENT_LAB_SCENARIO")
fi

ARGS+=("$@")

python3 -m integration_tests.libtorrent_lab.run "${ARGS[@]}"
