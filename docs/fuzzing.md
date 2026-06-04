# Fuzzing

`superseedr` has a narrow `cargo-fuzz` setup for protocol parser surfaces that
should reject malformed input without panicking, aborting, or hanging. It also
includes reducer targets for action/effect logic that can be fuzzed without
starting the full application runtime.

## Setup

Install the fuzz runner and nightly toolchain:

```sh
cargo install cargo-fuzz
rustup toolchain install nightly
```

List available targets:

```sh
cargo fuzz list
```

## Targets

- `utp_packet_decode` drives the BEP 29/uTP packet decoder.
- `utp_packet_roundtrip` builds valid uTP packets from fuzz bytes and checks
  encode/decode invariants.
- `tcp_message_parse` drives the TCP peer message parser.
- `tcp_message_roundtrip` builds peer messages from fuzz bytes and checks
  encode/decode invariants.
- `torrent_file_parse` drives torrent metainfo bencode parsing.
- `torrent_info_parse` drives raw `info` dictionary parsing.
- `krpc_message_decode` drives DHT KRPC bencode/message decoding.
- `krpc_compact_decode` drives compact peer/node decoding for IPv4 and IPv6.
- `krpc_query_roundtrip` builds KRPC queries from fuzz bytes and checks that
  encoded queries decode as queries.
- `dht_lifecycle_reduce` drives DHT lifecycle actions and checks exact emitted
  effects for each action variant.

## Seeds And Dictionaries

The suite uses protocol dictionaries under `fuzz/dictionaries/`.

Generated seed corpora are ignored under `fuzz/corpus/`; regenerate them with:

```sh
python3 scripts/fuzz_seed_corpus.py
```

## Smoke Runs

Use the wrapper for quick local validation. It regenerates seed corpora and
passes the matching dictionary to each target:

```sh
bash scripts/fuzz_smoke.sh
```

Override the run count with `FUZZ_RUNS`:

```sh
FUZZ_RUNS=10000 bash scripts/fuzz_smoke.sh
```

Individual targets can still be run directly:

```sh
cargo +nightly fuzz run utp_packet_decode -- -runs=1000 -dict=fuzz/dictionaries/utp_packet_decode.dict
```

Longer runs belong in scheduled jobs or dedicated local soaks:

```sh
cargo +nightly fuzz run utp_packet_decode
```

Artifacts, corpora, and coverage output are ignored under `fuzz/`. Build output
uses Cargo's normal ignored `target/` directory.
