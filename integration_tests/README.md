# Integration Tests Harness

Dockerized integration harness for cross-client torrent interoperability.

Current stable scope:
- `superseedr -> superseedr` (seed + leech)
- `superseedr -> qbittorrent` (seed + leech)
- `qbittorrent -> superseedr` (seed + leech)

Experimental scope:
- `superseedr -> transmission` (seed + leech, currently `v1` only)
- `transmission -> superseedr` (seed + leech, currently `v1` only)

## Purpose

This harness exists to validate real interoperability behavior, not just unit-level correctness.

- Verify that one client can seed data that another client can fully download and validate.
- Catch protocol and metadata compatibility issues across torrent modes (`v1`, `v2`, `hybrid`).
- Produce deterministic artifacts (status snapshots, logs, validator reports) for local debugging and CI triage.
- Provide a stable adapter/scenario framework so additional clients can be added with minimal redesign.

## Test Design

Each mode run (`v1`, `v2`, or `hybrid`) follows the same design:

1. Generate deterministic fixture binaries and mode-specific torrent files with a local announce URL.
2. Create isolated runtime directories per run/mode (seed data, leech output, configs, logs).
3. Start Docker services in controlled order:
   - build image once
   - start tracker first and wait for readiness
   - start seed + leech clients
4. Poll client status periodically (currently Superseedr state JSON) and write normalized snapshots.
5. Validate leech output against expected filesystem manifest/hash from `integration_tests/test_data`.
6. Emit per-mode artifacts and final run summary.

Pass criteria:

- No missing files and no hash/content mismatches in the leech output.

Failure criteria:

- Timeout before convergence, or any missing/mismatched files.

## Requirements

- Docker + Docker Compose plugin (`docker compose`)
- Python 3.12+ (3.10+ may work, CI uses 3.12)
- Python deps from `requirements-integration.txt`
- Git checkout of this repo
- Optional: Rust/Cargo for broader project tests (`cargo test`) outside this harness

Install Python dependencies:

```bash
python3 -m pip install -r requirements-integration.txt
```

## Commands

### Main local entrypoint

```bash
./integration_tests/run_interop.sh [all|v1|v2|hybrid] [scenario]
```

Environment variables:

- `INTEROP_TIMEOUT_SECS` (default `300`): per-mode timeout
- `INTEROP_SCENARIO` (default `superseedr_to_superseedr`): scenario name when not passed as arg 2

Example:

```bash
INTEROP_TIMEOUT_SECS=300 ./integration_tests/run_interop.sh all
```

Example (mixed client):

```bash
INTEROP_TIMEOUT_SECS=300 ./integration_tests/run_interop.sh all superseedr_to_qbittorrent
```

### Direct Python harness entrypoint

```bash
python3 -m integration_tests.harness.run \
  --scenario superseedr_to_superseedr \
  --mode all|v1|v2|hybrid \
  --timeout-secs 300 \
  [--run-id run_YYYYMMDD_HHMMSS] \
  [--skip-generation]
```

Accepted arguments:

- `--scenario`: one of:
  - `superseedr_to_superseedr`
  - `superseedr_to_qbittorrent`
  - `qbittorrent_to_superseedr`
  - `superseedr_to_transmission` (experimental)
  - `transmission_to_superseedr` (experimental)
- `--mode`: `all`, `v1`, `v2`, `hybrid`
- `--timeout-secs`: timeout per mode in seconds
- `--run-id`: optional explicit run id
- `--skip-generation`: skip fixture/torrent regeneration

### Pytest wrapper

Unit tests (fast, no Docker):

```bash
python3 -m pytest integration_tests/harness/tests -m "not interop"
```

Interop tests via pytest (Docker):

```bash
RUN_INTEROP=1 INTEROP_TIMEOUT_SECS=300 \
python3 -m pytest integration_tests/harness/tests -m interop
```

## Cluster CLI Harness

The cluster CLI lane is separate from the Docker interop harness, but it follows
the same Docker-first testing model. It runs two Linux Superseedr containers
against one mounted shared root, reuses checked-in torrent fixtures, and
exercises shared offline, leader, follower, failover, and failback CLI flows.

Main entrypoints:

```bash
./integration_tests/run_cluster_cli.sh
```

or:

```bash
python3 -m integration_tests.cluster_cli.run
```

Pytest wrapper:

```bash
RUN_CLUSTER_CLI=1 python3 -m pytest integration_tests/cluster_cli/tests -m cluster_cli
```

Artifacts are written under:

- `integration_tests/artifacts/cluster_cli/<run_id>/`

## Libtorrent Lab

The libtorrent lab is a separate Docker-first lane for direct libtorrent-backed
test peers. It is intended for future protocol, behavior, performance, and
settings probes without pulling in a full client UI stack.

Main entrypoint:

```bash
./integration_tests/run_libtorrent_lab.sh
```

or:

```bash
python3 -m integration_tests.libtorrent_lab.run --scenario basic_ul_dl
```

Matrix entrypoint:

```bash
./integration_tests/run_libtorrent_lab.sh --matrix smoke
./integration_tests/run_libtorrent_lab.sh --matrix config
./integration_tests/run_libtorrent_lab.sh --matrix behavior
./integration_tests/run_libtorrent_lab.sh --matrix full --repeat 2
```

Profile entrypoint:

```bash
./integration_tests/run_libtorrent_lab.sh --profile quick
./integration_tests/run_libtorrent_lab.sh --profile premerge
./integration_tests/run_libtorrent_lab.sh --profile stress
./integration_tests/run_libtorrent_lab.sh --profile soak
```

Readiness entrypoint for final uTP merge/release validation:

```bash
./integration_tests/run_libtorrent_lab.sh --readiness quick --fail-fast
./integration_tests/run_libtorrent_lab.sh --readiness release --fail-fast
```

Readiness summaries fail on any failed scenario attempt or any Superseedr
error-level app log line. Warning-level Superseedr log lines are counted and
surfaced in `readiness_summary.json`/`.md`.

Network impairment can be applied to any scenario or matrix with the `netem`
flags:

```bash
./integration_tests/run_libtorrent_lab.sh --matrix transport \
  --netem-delay-ms 50 \
  --netem-jitter-ms 10 \
  --netem-loss-pct 0.5
```

Initial scenario names:

- `basic_ul_dl`
- `superseedr_to_libtorrent`
- `libtorrent_to_superseedr`
- `superseedr_utp_to_libtorrent`
- `libtorrent_utp_to_superseedr`
- `superseedr_to_libtorrent_v1_multi_file`
- `libtorrent_to_superseedr_v1_nested`
- `superseedr_to_libtorrent_v2_multi_file`
- `libtorrent_to_superseedr_hybrid_nested`
- `superseedr_to_libtorrent_tcp_fanout`
- `libtorrent_to_superseedr_tcp_fanout`
- `basic_ul_dl_tcp_only`
- `basic_ul_dl_utp_only`
- `basic_ul_dl_dht_lsd_enabled`
- `superseedr_all_to_libtorrent_dual_stack`
- `libtorrent_dual_stack_to_superseedr_all`

Artifacts are written under:

- `integration_tests/artifacts/libtorrent_lab/<run_id>/`

Matrix runs also write `matrix_summary.json` and `matrix_summary.md` in their
matrix artifact directory.

Profile runs also write `profile_summary.json` and `profile_summary.md` in
their profile artifact directory.

Per-scenario `summary.json` also includes stronger `assertions`, summarized
`libtorrent_events`, and `behavior_probes` so completed-but-suspicious runs can
fail or at least emit trendable warnings.

## Artifacts and Monitoring

Per run output:

- `integration_tests/artifacts/runs/<run_id>/summary.json`
- `integration_tests/artifacts/runs/<run_id>/<mode>/validator_report.json`
- `integration_tests/artifacts/runs/<run_id>/<mode>/normalized_status.json`
- `integration_tests/artifacts/runs/<run_id>/<mode>/raw_client_status/*`
- `integration_tests/artifacts/runs/<run_id>/<mode>/logs/*`

Monitoring model:

- Superseedr is polled via its status JSON (`app_state.json`) and normalized into harness snapshots.
- Final pass/fail is determined by filesystem manifest/hash validation vs `integration_tests/test_data`.
- Tracker readiness is explicitly waited on before starting seed/leech services to reduce hybrid-mode flakes.

## CI

GitHub Actions workflow:

- `.github/workflows/integration-interop.yml`
- `.github/workflows/integration-cluster-cli.yml`

Behavior:

- Runs matrix over scenarios and modes:
  - full modes (`v1`, `v2`, `hybrid`): `superseedr_to_superseedr`, `superseedr_to_qbittorrent`, `qbittorrent_to_superseedr`
  - `v1` only: `superseedr_to_transmission`, `transmission_to_superseedr`
- Supports manual `workflow_dispatch` inputs:
  - `mode` (`all|v1|v2|hybrid`)
  - `timeout_secs`
- Uploads artifacts from `integration_tests/artifacts/`

## Current Status

As of February 21, 2026:

- `superseedr -> superseedr` passes for `v1`, `v2`, and `hybrid`.
- `superseedr -> qbittorrent` passes for `v1`, `v2`, and `hybrid`, with manifest/hash validation.
- `qbittorrent -> superseedr` now runs ungated in pytest and passes for `v1`, `v2`, and `hybrid` in local validation.
- qBittorrent container/auth/add/polling/log collection are implemented in `integration_tests/harness/clients/qbittorrent.py`.
- CI interop matrix now enforces all three scenarios (`superseedr_to_superseedr`, `superseedr_to_qbittorrent`, `qbittorrent_to_superseedr`) across all three modes.
- qBittorrent and tracker host ports are dynamically allocated in qBittorrent scenarios/tests to reduce local port-collision flakes.
- Transmission adapter now supports auth/session handshake, torrent add, status polling, and log collection.
- Transmission scenarios now run in CI for `v1` (`superseedr_to_transmission`, `transmission_to_superseedr`).
- Transmission `v2`/`hybrid` adds currently fail with RPC result `unrecognized info` on the linuxserver image.

## Plan / Next Tasks

1. Validate Transmission `v2`/`hybrid` compatibility and enable non-`v1` modes when supported.
2. Add focused diagnostics for reverse failures (piece-level mapping/torrent-level correlation) to shorten triage loops.
3. Extend transmission CI coverage to `v2`/`hybrid` once compatibility is available.
