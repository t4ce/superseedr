#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025 The superseedr Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNS="${FUZZ_RUNS:-1000}"

cd "$ROOT"
python3 scripts/fuzz_seed_corpus.py

targets=(
  utp_packet_decode
  utp_packet_roundtrip
  tcp_message_parse
  tcp_message_roundtrip
  torrent_file_parse
  torrent_info_parse
  krpc_message_decode
  krpc_compact_decode
  krpc_query_roundtrip
  dht_lifecycle_reduce
)

for target in "${targets[@]}"; do
  dict="fuzz/dictionaries/${target}.dict"
  args=("-runs=${RUNS}")
  if [[ -f "$dict" ]]; then
    args+=("-dict=${dict}")
  fi

  cargo +nightly fuzz run "$target" -- "${args[@]}"
done
