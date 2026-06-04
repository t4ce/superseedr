# Libtorrent Lab

Dockerized lab for programmable libtorrent-backed interop and behavior tests.

This lab is separate from the existing client-to-client interop harness. The
current seed/leech smoke keeps the surface intentionally small while leaving a
clean place for future scenarios with different peer counts, libtorrent
settings, transport modes, extension behavior, and performance probes.

## Basic Smoke

```bash
./integration_tests/run_libtorrent_lab.sh
```

or:

```bash
python3 -m integration_tests.libtorrent_lab.run \
  --scenario basic_ul_dl \
  --timeout-secs 120
```

The smoke scenario:

1. Generates deterministic integration fixture data and torrents with the
   Docker-internal tracker announce URL.
2. Starts the local tracker.
3. Starts a libtorrent seed peer with the fixture payload mounted at `/data`.
4. Starts a libtorrent leech peer with an empty `/data`.
5. Validates the downloaded file by size and sha256.

The first Superseedr interop scenarios are TCP baselines:

```bash
./integration_tests/run_libtorrent_lab.sh superseedr_to_libtorrent
./integration_tests/run_libtorrent_lab.sh libtorrent_to_superseedr
```

These prove the two implementations can exchange the fixture through the local
tracker and write byte-identical output. Future uTP-only and mixed-transport
scenarios can reuse the same manifests by changing the Superseedr transport env
and libtorrent settings.

uTP-only baselines disable TCP on both sides:

```bash
./integration_tests/run_libtorrent_lab.sh superseedr_utp_to_libtorrent
./integration_tests/run_libtorrent_lab.sh libtorrent_utp_to_superseedr
```

Directory and metainfo-mode scenarios cover multi-file, nested, v2, and hybrid
torrents without widening into a full matrix:

```bash
./integration_tests/run_libtorrent_lab.sh superseedr_to_libtorrent_v1_multi_file
./integration_tests/run_libtorrent_lab.sh libtorrent_to_superseedr_v1_nested
./integration_tests/run_libtorrent_lab.sh superseedr_to_libtorrent_v2_multi_file
./integration_tests/run_libtorrent_lab.sh libtorrent_to_superseedr_hybrid_nested
```

Fanout scenarios activate three libtorrent peers on one side:

```bash
./integration_tests/run_libtorrent_lab.sh superseedr_to_libtorrent_tcp_fanout
./integration_tests/run_libtorrent_lab.sh libtorrent_to_superseedr_tcp_fanout
```

## Matrix Runs

Use matrix mode when you want a single pass/fail summary across a scenario set:

```bash
./integration_tests/run_libtorrent_lab.sh --matrix smoke
./integration_tests/run_libtorrent_lab.sh --matrix transport
./integration_tests/run_libtorrent_lab.sh --matrix fixtures
./integration_tests/run_libtorrent_lab.sh --matrix fanout
./integration_tests/run_libtorrent_lab.sh --matrix config
./integration_tests/run_libtorrent_lab.sh --matrix behavior
./integration_tests/run_libtorrent_lab.sh --matrix full
```

Repeat mode is the first flake detector:

```bash
./integration_tests/run_libtorrent_lab.sh --matrix smoke --repeat 3
```

Each matrix writes `matrix_summary.json` and `matrix_summary.md` under its
artifact directory, with links to the per-scenario run artifacts. Matrix rows
also show whether the stronger assertions and behavior probes passed and how
many probe warnings were emitted.

`config` covers focused libtorrent settings and transport-mode variants:

- TCP-only libtorrent.
- uTP-only libtorrent.
- libtorrent DHT and local service discovery enabled.
- Superseedr `all` mode against dual-stack libtorrent.
- dual-stack libtorrent against Superseedr `all` mode.

`behavior` is the first explicit probe lane. It runs the focused config
surfaces that should keep producing stable protocol behavior artifacts.

## Profile Runs

Profiles bundle one or more matrix runs into named local/CI lanes:

```bash
./integration_tests/run_libtorrent_lab.sh --profile quick
./integration_tests/run_libtorrent_lab.sh --profile premerge
./integration_tests/run_libtorrent_lab.sh --profile stress
./integration_tests/run_libtorrent_lab.sh --profile soak
```

Current profiles:

- `quick`: smoke matrix.
- `premerge`: full clean matrix plus a mild impaired transport matrix.
- `stress`: repeated full matrix plus repeated impaired fanout matrix.
- `soak`: longer repeated full and impaired transport matrices for scheduled
  endurance runs.

Profile runs write `profile_summary.json` and `profile_summary.md`, plus the
normal per-step matrix summaries. `--repeat N` multiplies each profile step's
repeat count, and explicit `--netem-*` flags override each profile step's
default impairment.

## uTP Readiness Runs

Readiness suites are release-oriented gates on top of the matrix runner. They
preserve the per-scenario artifacts, but add an overall `readiness_summary.json`
and `readiness_summary.md` with explicit Superseedr error counts.

```bash
./integration_tests/run_libtorrent_lab.sh --readiness quick --fail-fast
./integration_tests/run_libtorrent_lab.sh --readiness release --fail-fast
```

Current readiness suites:

- `quick`: behavior matrix only, for fast validation of probe wiring and
  Superseedr log health.
- `release`: full clean matrix, focused config matrix, behavior matrix,
  impaired transport matrix, and impaired fanout matrix.

Readiness passes only when all planned steps complete, no scenario attempt
fails, and Superseedr emits zero error-level log lines. Warning-level
Superseedr log lines are kept as warnings in the summaries so expected lab
environment noise remains visible without hiding transfer failures.

## Network Impairment

Scenario and matrix runs can apply Docker `tc netem` impairment to active peer
containers:

```bash
./integration_tests/run_libtorrent_lab.sh superseedr_to_libtorrent \
  --netem-delay-ms 50 \
  --netem-jitter-ms 10 \
  --netem-loss-pct 0.5
```

The lab images include `iproute2`, and peer containers run with `NET_ADMIN` so
the runner can apply delay, jitter, loss, duplicate, corruption, and reorder
knobs before validation.

The Superseedr containers use `docker/Dockerfile.superseedr`, which builds a
debug binary for fast lab iteration instead of the production release image.

Artifacts are written under:

```text
integration_tests/artifacts/libtorrent_lab/<run_id>/
```

Important files:

- `summary.json`: scenario result and final peer status.
- `summary.json.assertions`: stronger pass/fail checks for completed payload
  bytes, participant completion, seed upload floors, and leech download floors.
- `summary.json.behavior_probes`: probe results and warnings for transfer
  accounting, libtorrent event health, tracker announces, progress timing, and
  Superseedr log health.
- `summary.json.libtorrent_events`: summarized libtorrent alert/event counts
  and per-peer progress timing.
- `summary.json.superseedr`: summarized Superseedr app logs, including
  error/warning counts and sampled log lines.
- `peers/seed/status.json`: latest seed status emitted by the peer.
- `peers/seed/events.jsonl`: seed libtorrent events and alerts.
- `peers/leech/status.json`: latest leech status emitted by the peer.
- `peers/leech/events.jsonl`: leech libtorrent events and alerts.
- `logs/*.log`: compose logs, container logs, and copied Superseedr app logs.

## Scenario Contract

Scenarios live in `integration_tests/libtorrent_lab/scenarios/*.json`.

Each scenario declares seed/leech client types, one torrent, one payload file,
listen ports, timeout, optional libtorrent seed/leech counts, Superseedr peer
transport, optional behavior probes, optional assertion settings, and the
libtorrent settings applied to libtorrent peers. The libtorrent peer process
takes a generated `/config/peer.json` and writes JSON status plus JSONL events
to `/artifacts`.

Future scenarios should add knobs to the scenario manifest rather than baking
new topology assumptions into the runner.
