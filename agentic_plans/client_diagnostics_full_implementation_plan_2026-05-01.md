# Full Client Diagnostics Implementation Plan

Date: 2026-05-01

## Purpose

Replace scattered developer-only tracing switches with a coherent diagnostics system that can support normal release troubleshooting, long soak analysis, DHT planner debugging, protocol-level investigation, and peer-level tracing without exposing users to hidden environment variables.

The system should be explicitly scoped, bounded, redactable, and easy to turn on and off from the client or CLI.

## Non-Goals

- Do not keep ad hoc debug environment variables as the public interface.
- Do not emit unbounded logs by default.
- Do not require recompilation to collect useful diagnostics.
- Do not make peer-level tracing part of normal logging.
- Do not leak full file names, torrent display names, peer IDs, or full info hashes unless a diagnostic profile explicitly requests unredacted local output.

## User-Facing Shape

Add a first-class diagnostics command surface:

```text
superseedr diagnostics status
superseedr diagnostics start --profile dht-soak --duration 30m
superseedr diagnostics start --profile peer-trace --torrent <hash-prefix> --peer <ip:port> --duration 5m
superseedr diagnostics stop
superseedr diagnostics bundle --latest
superseedr diagnostics summarize --latest
```

TUI follow-up:

- Add a diagnostics modal/status row showing active profile, remaining time, output directory, dropped event count, and bundle command.
- Add a confirmation step for profiles that include peer-level or protocol payload detail.

## Profiles

### `client-health`

Low overhead. Safe for normal users.

Capture:

- periodic status snapshots
- runtime settings summary
- warnings/errors
- disk/network health
- torrent counts by state
- DHT health counters
- tracker error counts
- persistence writer status

### `dht-soak`

Operational soak profile for release validation and regression checks.

Capture:

- periodic status snapshots
- DHT health snapshots
- planner aggregate counters
- launch class mix
- launch reasons
- demand class transitions
- lookup starts/finishes/parks/drains
- query pressure
- route counts
- peer yield summaries
- invariant violations

Do not capture raw KRPC payloads or per-peer protocol messages.

### `dht-planner`

Detailed planner replay/profile mode.

Capture:

- every planner action/effect
- normalized demand metrics
- selected candidates
- skipped candidates with reason class, not full peer data
- class budgets and token bucket state
- active lookup slot state
- parked crawl quality
- drain lifecycle
- deterministic replay fixture output

### `dht-protocol`

Protocol investigation mode.

Capture:

- KRPC query kind
- transaction id
- source/target endpoint
- request/response timing
- response source validation result
- decoded node/peer counts
- token present/absent, but not token bytes by default
- decode errors and bencode guard rejects

Raw payload capture must be opt-in with a short duration and size cap.

### `peer-trace`

Targeted peer-level tracing.

Required scope:

- `--torrent <info-hash-prefix>`
- optional `--peer <ip:port>`
- max duration default 5 minutes
- max output size default 64 MB

Capture:

- peer connection attempt reason
- whether peer came from tracker, DHT, PEX, incoming, or resume state
- seeder/leecher classification source
- handshake result
- extension negotiation summary
- choke/interested transitions
- request/cancel/piece flow counts
- disconnect reason
- known seeder cache hit/miss
- per-peer rates over coarse intervals
- metadata exchange state, without dumping metadata payloads by default

Optional deep mode:

- message-level event stream
- request block identifiers
- extension message kind
- raw payload length and hashes
- raw payload bytes only with explicit `--raw-payloads` and local-only warning

### `full-debug`

Developer-only aggregate profile. It should require an explicit CLI confirmation flag:

```text
superseedr diagnostics start --profile full-debug --i-understand
```

This can compose `dht-planner`, `dht-protocol`, `peer-trace`, client health, and selected raw payload capture with strict caps.

## Architecture

### Logging Strategy

Do not replace `tracing`, and do not build a custom logging library.

Use `tracing` as the logging and event substrate because it already gives the client the right Rust primitives:

- `INFO`, `DEBUG`, and `TRACE` levels
- structured fields
- targets/scopes such as `superseedr::dht::planner` or `superseedr::peer`
- spans for correlation
- custom layers and sinks
- JSON output support
- low disabled-path overhead

Diagnostics should be a product layer on top of `tracing`, not a competing logger.

The split should be:

- `tracing` owns normal process logs, developer logs, target filters, and level filters.
- `diagnostics` owns capture policy: active profile, torrent/peer scope, duration, byte caps, redaction, output bundle, replay fixture generation, and whether raw payloads are allowed.
- typed diagnostics events are the source of truth for diagnostic bundles.
- diagnostics events may optionally be mirrored to `tracing` targets for developer readability.

Do not rely on `TRACE` logs alone for diagnostics. Log levels cannot express product-level constraints such as "only this torrent", "only this peer", "stop after 5 minutes", "redact display names", "drop diagnostics instead of stalling DHT", or "include this run in a bundle".

For peer-level tracing, require both a diagnostics profile and a scope:

```text
superseedr diagnostics start --profile peer-trace --torrent <hash-prefix> --peer <ip:port> --duration 5m
```

That profile can still emit `tracing::trace!` records under a scoped target, but capture decisions must come from diagnostics session state.

### Diagnostics Coordinator

New module:

```text
src/diagnostics/
  mod.rs
  command.rs
  config.rs
  event.rs
  registry.rs
  sink.rs
  redaction.rs
  summary.rs
```

Responsibilities:

- own active diagnostic session state
- validate profile scope
- enforce duration and byte caps
- assign run id
- create output directory
- expose lightweight event emitters to subsystems
- publish current diagnostic status to app/TUI
- handle stop/bundle/summarize commands

### Event Registry

Each subsystem registers event domains:

- `client`
- `torrent_manager`
- `peer`
- `tracker`
- `dht.service`
- `dht.planner`
- `dht.runtime`
- `dht.transport`
- `disk`
- `persistence`
- `rss`
- `watcher`

Each domain exposes typed events, not formatted strings. Sinks decide how to serialize.

### Event Shape

Base fields:

```text
schema_version
run_id
timestamp_unix_ms
monotonic_ms
domain
event
severity
correlation_id
```

Torrent fields:

```text
info_hash_prefix
demand_class
torrent_status
complete
connected_peers
download_speed_bps
upload_speed_bps
```

Peer fields:

```text
peer_addr
peer_id_prefix
source
connection_id
session_direction
known_seeder
classification_confidence
```

DHT fields:

```text
family
lookup_id
transaction_id
query_kind
node_addr
node_id_prefix
selection_reason
slice_class
power_multiplier
unique_peer_cap
stop_reason
```

Payload fields must be opt-in and should default to hashes/lengths only.

### Sinks

Initial sinks:

- JSONL event file
- periodic status sample JSONL
- bounded app log copy
- summary JSON
- human-readable summary text

Future sinks:

- in-memory ring buffer for TUI
- test replay fixture writer
- compressed bundle writer

## Redaction Policy

Default redaction:

- full info hash -> 8 hex characters
- peer id -> 8 hex characters
- file paths -> root-relative or anonymized shape
- torrent display names -> omitted unless explicitly allowed
- tokens -> present/absent/length only
- raw payload bytes -> omitted

Explicit local-only unredacted mode:

```text
--redaction local-full
```

This should be rejected for shared/follower output paths unless explicitly forced.

## Runtime Control

Diagnostics should be runtime-toggleable through the existing control path instead of process startup environment variables.

Implementation shape:

- CLI command sends `ControlRequest::DiagnosticsStart`
- app applies diagnostics config
- subsystems receive a cheap shared diagnostics handle
- event emission checks an atomic profile mask
- disabled fast path is one atomic load and return

## Performance Requirements

Disabled:

- no allocations on hot paths
- one cheap branch or atomic read at most
- no formatting before enabled check

Enabled:

- bounded channels
- dropped event counter
- profile-specific sampling
- max bytes per sink
- max duration
- backpressure should drop diagnostics, not stall torrent or DHT work

Peer-level tracing:

- require scope filters before enabling
- no global all-peer message logging by default
- aggregate counters preferred over per-message logs unless deep mode is explicitly selected

## Invariant Checking

Invariant checks should be a diagnostics feature, not a separate environment variable.

Profiles:

- `dht-soak`: aggregate invariant failures only
- `dht-planner`: full invariant failure event with planner state summary
- `full-debug`: optional state snapshot around violation

Invariant failures should be events with severity `error`; diagnostics must not panic in release mode.

## Replay Support

Replay generation should be explicit:

```text
superseedr diagnostics start --profile dht-planner --replay-fixture
```

Output:

- normalized replay JSONL
- deterministic replay text fixture
- replay metadata with binary version and git SHA

Tests should consume checked-in replay fixtures directly, not depend on environment variables to print hidden traces.

## Implementation Phases

### Phase 1: Foundation

- Add diagnostics module and typed event model.
- Add disabled no-op handle.
- Add bounded JSONL sink.
- Add diagnostics status model.
- Add CLI command parsing for `diagnostics status/start/stop`.
- Add unit tests for redaction, caps, disabled fast path, and sink rotation.

### Phase 2: DHT Soak Profile

- Port current DHT soak counters into typed diagnostics events.
- Add status sampling.
- Add summary generation equivalent to `scripts/summarize_dht_soak.py`.
- Add threshold assertions as CLI options.
- Keep no raw payloads.

### Phase 3: Planner and Invariants

- Route planner action/effect monitor through diagnostics registry.
- Route planner invariant checks through diagnostics registry.
- Add deterministic replay fixture writer.
- Add tests proving disabled diagnostics do not allocate formatted event strings.

### Phase 4: Protocol and Runtime

- Add DHT transport/runtime event domains.
- Add transaction timing and source validation events.
- Add bounded protocol payload metadata.
- Add explicit raw-payload mode with strict caps.

### Phase 5: Peer-Level Tracing

- Add peer trace event domain in torrent manager and peer session.
- Correlate peer source: tracker, DHT, PEX, incoming, resume.
- Track known-seeder cache decisions.
- Track handshake, extension negotiation, request flow, choke/interested transitions, disconnect reason.
- Add per-peer summary.

### Phase 6: Bundle and TUI

- Add bundle command.
- Include summary, event JSONL, samples, app log window, config snapshot, and version metadata.
- Add TUI diagnostics status.
- Add user-visible path to latest bundle.

## Acceptance Criteria

- No hidden diagnostics environment variables are required.
- Diagnostics can be started and stopped while the client is running.
- Disabled diagnostics have negligible cost.
- DHT soak profile can reproduce current release validation summaries.
- Peer trace can answer why a specific peer was connected, skipped, classified as seeder, disconnected, or not requested.
- Bundles are bounded, redactable, and useful for issue reports.
- CI covers disabled diagnostics, redaction, caps, and at least one deterministic replay fixture.
