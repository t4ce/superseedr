# Peer Transport Abstraction Plan For Future uTP And QUIC

Date: 2026-05-13

## Purpose

Introduce a transport-neutral peer networking layer so Superseedr can keep TCP behavior unchanged now while leaving a clear path for future uTP and QUIC support.

This is a living agent handoff. Each agent that works on this area should refine the plan as code facts change, record completed decisions, and keep the first implementation behavior-preserving unless a later decision explicitly expands scope.

## Agent Execution Handoff

Use this section when starting or resuming a long-running implementation session.

### Objective

Implement a TCP-only peer transport abstraction in Superseedr that removes raw `TcpStream` from app-to-manager peer wiring and outbound manager connection setup, preserves current TCP behavior, and leaves typed extension points for future uTP and QUIC support.

### Constraints

- Treat this as abstraction-only work.
- Keep all production peer traffic on TCP.
- Do not implement uTP, QUIC, or UDP demux in this pass.
- Do not change tracker announce, DHT announce, or private torrent behavior.
- Do not add a transport crate dependency.
- Keep each patch behavior-preserving unless this plan is explicitly updated.
- Update this document as milestones complete or key decisions change.

### Completion Criteria

Treat the work as complete only when the "Done Means For The Abstraction-Only Work" section is satisfied and validation evidence has been recorded in this file or in the final agent report.

Do not treat a partial type layer as completion. The boundary must actually carry production TCP peer traffic through the abstraction.

### Checkpoints

After each milestone, record:

- status: `pending`, `in progress`, `blocked`, or `complete`
- changed files
- behavior intended to remain unchanged
- tests run
- direct TCP references intentionally left behind
- new risks or decisions discovered

If work expands into UDP ownership, uTP, QUIC, certificate policy, or public-swarm compatibility, pause this work and create a new follow-up plan.

## Implementation Status

Last updated: 2026-05-14

Overall status: TCP-only peer transport abstraction is implemented for the production app-to-manager inbound peer boundary and outbound manager connect path. Follow-up passes added an env-gated homegrown uTP path with TCP fallback, uTP-only test mode, transport metrics, selective ACK handling, adaptive retransmit timeouts, LEDBAT-style delay-based congestion control, shared UDP socket ownership for DHT/uTP, same-port outbound uTP, and inbound uTP handoff through the existing `PeerConnection` boundary. Default-on uTP and QUIC remain future work.

## Follow-up Status: Homegrown uTP Path

Last updated: 2026-05-14

Status: implemented as an experimental uTP path behind `SUPERSEEDR_ENABLE_UTP`.

### What Changed

- Added `src/networking/utp.rs` with a homegrown BEP 29/uTP packet layer, outbound SYN/STATE handshake, DATA/STATE reliability, retransmission, FIN/RESET handling, extension-chain parsing, and selective ACK extension support.
- Added out-of-order receive buffering with selective ACK responses and selective ACK processing for outbound packet acknowledgement.
- Added adaptive retransmit timeout tracking and LEDBAT-style delay-based congestion control using remote timestamp differences, base-delay history, loss response, remote receive window limits, and packet-size adaptation.
- Added `src/networking/shared_udp.rs` as the shared UDP socket owner and classifier for DHT and uTP datagrams.
- Reworked DHT transport to subscribe to shared UDP DHT datagrams instead of owning an exclusive UDP receive loop.
- Added inbound uTP demux and listener support so accepted uTP streams route through the existing app-to-manager `PeerConnection` handoff.
- Moved outbound uTP to the configured client UDP port so DHT, inbound uTP, and outbound uTP share the same public UDP socket.
- Added `UtpPeerTransport::connect` returning the same `PeerConnection` abstraction used by TCP.
- Kept default production behavior TCP-only. Outbound uTP is attempted only when `SUPERSEEDR_ENABLE_UTP` is truthy (`1`, `true`, `yes`, or `on`).
- If the uTP probe fails or times out, the manager falls back to TCP for the same peer.
- Added `SUPERSEEDR_UTP_ONLY`, which implies uTP is enabled and disables outbound TCP fallback plus the inbound TCP peer listener for isolated uTP testing.
- Added `SUPERSEEDR_LOG_UTP_CONNECT` as an opt-in diagnostic switch for distinguishing uTP transport connects, BitTorrent session success, and selected-transport session failures.
- Added peer transport selection to manager state so metrics can distinguish TCP and uTP peers.
- Added status metrics:
  - `tcp_peer_count`
  - `utp_peer_count`
  - `beneficial_tcp_peer_count`
  - `beneficial_utp_peer_count`
- "Beneficial" currently means the peer has moved payload in either direction (`total_bytes_downloaded > 0` or `total_bytes_uploaded > 0`).

### Key Decisions From This Follow-up

- Do not enable uTP by default until it has real-world soak evidence against public uTP peers.
- Do not add external transport dependencies for this pass.
- Use one shared UDP socket owner for DHT and uTP. The shared runtime classifies bencoded KRPC packets as DHT and BEP 29 version/type packets as uTP.
- Keep TCP fallback mandatory while uTP is experimental, except when `SUPERSEEDR_UTP_ONLY=1` is used for isolated transport testing.
- Allow `SUPERSEEDR_UTP_ONLY=1` only as a test switch; it disables the TCP peer listener and outbound TCP fallback so inbound and outbound uTP can be tested in isolation.
- Treat BitTorrent session success over the selected transport as the proof point, not just a uTP SYN/STATE transport connect.
- Keep `ip:port` peer state keys for this pass because only one transport is selected per outbound peer attempt. A future simultaneous TCP/uTP identity migration still needs transport-qualified peer keys.

### Remaining Work

- Run longer public-swarm soak tests before default-on uTP.
- Add broader interoperability tests against real public uTP peers.
- Decide how, if at all, to expose transport counts in the TUI; status JSON has the first observable surface.
- QUIC still requires a separate compatibility and protocol decision.

### Validation Evidence For uTP Follow-up

- `cargo fmt`
- `cargo check`
- `cargo check --features synthetic-load`
- `cargo test networking::utp`
- `cargo test counts_transport_peers_and_payload_movers`
- `cargo test integrations::status::tests::test_serialize_torrents_hex_keys`
- `cargo test --features synthetic-load outbound_connect_sample_tracks_transport_breakdown`
- `cargo test networking::utp -- --nocapture`
- `cargo test dht::transport -- --nocapture`
- `cargo test inbound_listener_accepts_utp_stream -- --nocapture`
- `cargo test dht_and_utp_share_udp_port -- --nocapture`
- `cargo test listener_set_bind_can_run_utp_without_tcp -- --nocapture`
- `cargo test`
- `cargo test utp_only_mode_disables_tcp_fallback -- --nocapture`
- `cargo test enabled_utp_mode_keeps_tcp_fallback -- --nocapture`
- `cargo build`
- `SUPERSEEDR_ENABLE_UTP=1 cargo run --release --features synthetic-load -- benchmark --start-torrents 2 --start-peers 8 --max-torrents 2 --max-peers 16 --max-steps 1 --disk-budget 64MiB --size-per-torrent 4MiB --piece-size 256KiB --duration-secs 8 --warmup-secs 1 --metrics-interval-ms 1000 --peer-add-burst-size 8 --target-gbps 1 --out tmp/utp-transport-synthetic`
  - Result: 3/3 benchmark steps passed.
  - Artifact: `tmp/utp-transport-synthetic/benchmark_20260513_153649/benchmark_summary.json`.
  - Expected fallback evidence: TCP established all fallback attempts; uTP attempts failed against TCP-only synthetic peers; `protocol_errors=0`.
- Launched `target/release/superseedr.exe` with isolated shared config at `tmp/utp-client-root`, host id `utp-branch-client`, port `16682`, and `SUPERSEEDR_ENABLE_UTP=1`.
  - Status verified via `target/release/superseedr.exe status --json`.
  - Runtime evidence: DHT bound to `0.0.0.0:16682` and `[::]:16682`; no stderr output; branch process stopped cleanly through `stop-client --json`.
- Launched debug client against the normal public-swarm test environment with `SUPERSEEDR_UTP_ONLY=1` and `SUPERSEEDR_LOG_UTP_CONNECT=1`.
  - Runtime evidence: uTP-only mode produced uTP connects and BitTorrent sessions, zero TCP sessions in the inspected log window, no fallback lines, and the process stopped cleanly.
- Launched debug client against the normal public-swarm test environment with `SUPERSEEDR_ENABLE_UTP=1` and `SUPERSEEDR_LOG_UTP_CONNECT=1`.
  - Runtime evidence: fallback mode produced both uTP and TCP BitTorrent sessions in the inspected log window, preserving TCP fallback while exercising the uTP path.

### Completed Milestones

- Milestone 1: complete
  - Added `src/networking/transport.rs`.
  - Added `PeerTransportKind`, `PeerEndpoint`, `PeerConnectionDirection`, `PeerConnection`, `PeerStream`, and `TcpPeerTransport`.
  - Transport identity can format canonical keys such as `tcp://127.0.0.1:6881` while display formatting remains plain `127.0.0.1:6881`.
- Milestone 2: complete
  - `ListenerSet::accept` now returns `PeerConnection`.
  - Incoming peer channels now carry `(PeerConnection, handshake)` instead of `(TcpStream, handshake)`.
  - Incoming handshake routing still reads the same 68-byte BitTorrent handshake and routes by info hash.
  - `MarkPortOpen` still uses the remote socket address.
- Milestone 3: complete
  - Outbound manager peer connections now call `TcpPeerTransport::connect`.
  - Existing permit acquisition, connect timeout, unresponsive-peer behavior, and synthetic failure classification remain in place.
- Milestone 5: complete for the abstraction-only pass
  - Synthetic incoming manager wiring now uses `PeerConnection`.
  - Manager synthetic connect events carry `PeerTransportKind`.
  - Synthetic outbound connect samples keep existing aggregate counters and add `by_transport` breakdown entries.

### Deferred Milestones

- Milestone 4: partially complete
  - New connection boundaries carry typed endpoint metadata and a transport-qualified key.
  - Existing torrent-manager peer maps and UI-facing peer ids still use plain `ip:port` strings to preserve current behavior while only TCP is active.
  - Before enabling a second transport, audit and migrate duplicate-peer handling, disconnect commands, PEX handling, peer rows, and telemetry keys so TCP/uTP/QUIC peers at the same socket address do not collide.
- Milestone 6: pending
  - No UDP ownership or demux implementation was started.
  - A follow-up UDP plan is still required before uTP or QUIC work.

### Changed Files

- `src/networking/transport.rs`
- `src/networking/mod.rs`
- `src/app.rs`
- `src/torrent_manager/mod.rs`
- `src/torrent_manager/manager.rs`
- `src/synthetic_load.rs`

### Behavior Intended To Remain Unchanged

- All production peer traffic remains TCP.
- Listener IPv4/IPv6 fallback and rebind behavior remains owned by the app listener set.
- BitTorrent session framing, request scheduling, storage, DHT, tracker announce, PEX, and private torrent behavior are unchanged.
- UI peer display still shows plain socket addresses.
- Synthetic load remains TCP-only by default.

### Direct TCP References Intentionally Left

- `src/networking/transport.rs`: owns current TCP connect and accepted-stream wrapping.
- `src/networking/protocol.rs`: TCP fixtures for protocol tests.
- `src/synthetic_load.rs`: synthetic seeder/leecher harness sockets remain TCP-specific; the manager-facing incoming path uses `PeerConnection`.

### Validation Evidence

- `cargo fmt`
- `cargo check`
- `cargo check --features synthetic-load`
- `cargo test networking::transport`
- `cargo test listener_set`
- `cargo test networking::session::`
- `cargo test torrent_manager::manager::`
- `cargo test --features synthetic-load outbound_connect_sample_tracks_transport_breakdown`
- `cargo test`
- `cargo test --features synthetic-load synthetic_load::tests::`
- `cargo run --release --features synthetic-load -- benchmark --start-torrents 4 --start-peers 64 --max-torrents 8 --max-peers 256 --max-steps 2 --disk-budget 256MiB --size-per-torrent 8MiB --piece-size 256KiB --duration-secs 15 --warmup-secs 2 --metrics-interval-ms 1000 --peer-add-burst-size 32 --target-gbps 1 --out tmp/peer-transport-synthetic`
  - Result: 6/6 benchmark steps passed across download, upload, and swarm.
  - Artifact: `tmp/peer-transport-synthetic/benchmark_20260513_142930/benchmark_summary.json`.
  - Transport evidence: download step summaries reported `tcp:64/64/0`; swarm step summaries reported `tcp:32/32/0`; all step summaries reported `protocol_errors=0` and `outbound_failed=0`.
- Launched `target/release/superseedr.exe` from this branch using isolated shared config at `tmp/branch-client-root`, host id `transport-branch-client`, and port `16681`.
  - Status verified via `target/release/superseedr.exe status --json`.
  - Runtime evidence: branch process running, status reports DHT bound to `0.0.0.0:16681` and `[::]:16681`, no torrents loaded in the isolated config.

### New Decisions

- Keep production peer state keys as plain `ip:port` for this pass, but expose `PeerConnection::transport_key()` for future transport-qualified identity work.
- Make synthetic connect telemetry transport-aware now, but keep existing aggregate fields for compatibility.
- Do not move listener ownership out of `App` until there is a second transport or a dedicated network runtime design.

## Current Shape

- `PeerSession::run` accepts a generic async stream, so the BitTorrent message/session layer is transport-neutral at the stream boundary.
- The production app-to-manager inbound peer boundary carries `PeerConnection` instead of raw `TcpStream`.
- Outbound manager peer connection setup goes through `TcpPeerTransport::connect`.
- `ListenerSet` still owns `TcpListener` accept/rebind behavior.
- Synthetic load uses `PeerConnection` for manager-facing incoming peers, but its seeder/leecher harness still uses TCP listeners and sockets directly.
- DHT already owns UDP sockets on the configured client port, so future UDP transports cannot be bolted on independently without a UDP ownership decision.
- PEX, manager state maps, disconnect commands, and UI peer rows still assume compact `ip:port` peers; typed transport identity exists at the new connection boundary but broader state-key migration is deferred.

## Goals

- Create a TCP-only transport abstraction first.
- Preserve current user-visible behavior during the initial migration.
- Make peer identity transport-aware before adding any second transport.
- Keep BitTorrent message parsing, request scheduling, upload/download state, and storage logic independent of TCP/uTP/QUIC.
- Provide clear extension points for future homegrown uTP and QUIC work.
- Make synthetic load, diagnostics, and telemetry able to label transport kind even while only TCP is active.

## Non-Goals For The First Pass

- Do not implement uTP.
- Do not implement QUIC.
- Do not change tracker announce behavior.
- Do not change DHT announce behavior.
- Do not change private torrent restrictions.
- Do not introduce a new external transport dependency as part of the abstraction-only pass.
- Do not redesign the BitTorrent peer protocol layer.

## Requirements

1. Add first-class transport identity.
- Define a peer transport kind with at least `Tcp`, `Utp`, and `Quic` variants.
- Define a typed peer endpoint that includes transport kind and socket address.
- Replace string-only peer identity at new boundaries with a typed identity or a canonical transport-qualified key such as `tcp://1.2.3.4:6881`.
- Keep display formatting separate from identity formatting.

2. Add a peer stream wrapper.
- Define a project-owned peer connection type that wraps an async stream plus endpoint metadata.
- The stream must be usable by `PeerSession::run` without changing BitTorrent message handling.
- The connection metadata should include remote address, transport kind, and direction when practical.

3. Move TCP behind the abstraction.
- Introduce a TCP transport implementation that owns today's connect and accept behavior.
- Keep the existing IPv4/IPv6 listener behavior and port rebinding semantics.
- Route inbound handshakes through the same torrent hash lookup as today, but pass a peer connection instead of a raw `TcpStream`.
- Route outbound connection attempts through the transport layer instead of calling `TcpStream::connect` inside the torrent manager.

4. Keep manager/session contracts stable.
- The torrent manager should not know whether the concrete stream is TCP, future uTP, or future QUIC.
- Peer session logic should only receive a stream-like object and endpoint metadata.
- Resource permits should continue to account for peer sessions, not per-protocol socket internals.

5. Make transport kind observable.
- Manager events and synthetic-load counters should be able to report transport kind for connect attempts, successes, and failures.
- UI labels can remain TCP-neutral until multiple transports exist, but telemetry should not lose the transport distinction.

6. Preserve tests and behavior.
- Existing TCP peer tests should continue to pass.
- New tests should cover typed endpoint formatting, inbound routing, outbound connect dispatch, and duplicate-peer identity behavior.
- Synthetic load may remain TCP-only, but it should use the abstraction boundary where feasible.

## Key Decisions

### Decision 1: Abstract At The Peer Connection Boundary

Do not abstract individual BitTorrent messages or protocol handlers. The right boundary is:

- accept/connect
- peer endpoint identity
- stream ownership
- inbound handshake routing
- rebind/shutdown lifecycle
- transport-aware telemetry

`PeerSession::run` should remain the main consumer of a stream-like object.

### Decision 2: TCP-Only First

The first implementation must be a no-behavior-change TCP migration. This creates confidence in the abstraction before adding UDP complexity.

uTP and QUIC should not be started until TCP passes existing tests and synthetic smoke coverage through the new boundary.

### Decision 3: Transport Kind Is Part Of Peer Identity

Future TCP, uTP, and QUIC connections may target the same socket address. The identity model must not collapse them into one `ip:port` string.

Agents should audit all peer maps, duplicate checks, disconnect commands, PEX handling, UI peer rows, and telemetry keys before broadening identity changes.

### Decision 4: One UDP Owner Later

Future uTP and QUIC cannot safely assume their own unrelated UDP listener on the client port because DHT already owns UDP transport state.

Before implementing either UDP transport, add a follow-up design for UDP packet ownership and demux:

- DHT KRPC bencode packets
- uTP packets
- QUIC long-header and short-header packets
- unknown/drop behavior
- bind/rebind lifecycle
- IPv4/IPv6 behavior
- shutdown and inflight-drain behavior

### Decision 5: QUIC Requires A Separate Compatibility Decision

uTP is a BitTorrent ecosystem transport. QUIC is not generally useful for public swarm peer compatibility unless peers explicitly support the same extension.

Future QUIC work must decide whether the target is:

- standards-compliant QUIC
- Superseedr-only QUIC streams
- a QUIC-like private encrypted UDP stream

Do not let the abstraction imply this decision has already been made.

## Proposed Milestones

### Milestone 1: Types And TCP Wrapper

- Add `src/networking/transport.rs` or a similarly named module.
- Define transport kind, endpoint, peer key, connection direction, and peer connection wrapper.
- Add formatting/parsing helpers only where needed for stable keys and display.
- Add unit tests for identity equality and canonical formatting.

### Milestone 2: Inbound TCP Migration

- Replace incoming peer channel payloads from raw `TcpStream` to peer connection.
- Keep the inbound 68-byte BitTorrent handshake routing behavior unchanged.
- Preserve current invalid-handshake rejection and torrent hash lookup behavior.
- Ensure `MarkPortOpen` still uses the remote socket address.

### Milestone 3: Outbound TCP Migration

- Move `TcpStream::connect` out of the torrent manager into the TCP transport implementation.
- Keep current connect timeout and error classification behavior.
- Preserve permit acquisition behavior and shutdown race behavior.
- Ensure failed connect paths still mark peers unresponsive and emit current synthetic-load failures.

### Milestone 4: Identity Audit

- Audit maps and commands keyed by peer id or `ip:port`.
- Convert only the boundaries needed to avoid future collisions.
- Keep UI display readable and avoid exposing internal URI-style keys where a plain address is enough.
- Add regression tests for duplicate peer handling when the same address appears with different transport kinds.

### Milestone 5: Synthetic And Diagnostics Alignment

- Route synthetic load through the same abstraction where practical.
- Add transport-kind labels to connect attempt/success/failure counters.
- Keep the existing TCP synthetic workload as the default.
- Document any remaining direct TCP test harness code as intentionally TCP-specific.

### Milestone 6: UDP Design Gate

Before implementing uTP or QUIC, write a short follow-up plan that settles UDP ownership and demux. This is a required gate because it affects DHT, port rebinding, public reachability, and VPN/firewall behavior.

## Validation Strategy

- Run focused unit tests for new transport identity/types.
- Run existing peer session and torrent manager tests.
- Run DHT transport/rebind tests after any change near UDP or client port handling.
- Run synthetic-load smoke only after TCP migration compiles and unit tests pass.
- For every milestone, record:
  - changed files
  - behavior intended to remain unchanged
  - tests run
  - known direct TCP references left behind

## Agent Operating Rules

- Keep each patch small enough to review.
- Prefer a behavior-preserving TCP migration before broader cleanup.
- Update this plan when a key decision changes or a milestone finishes.
- Do not start uTP or QUIC implementation until the abstraction is stable and validated.
- If existing user changes are present in nearby files, work with them and do not revert unrelated edits.
- When adding new abstractions, follow the repo's current async/Tokio patterns and avoid overbuilding until a second transport is real.

## Open Questions

- Should the stable long-term peer key become a displayable URI string or a small typed struct used internally with separate display formatting?
- Should TCP listener ownership move out of `App` once a second transport exists, or only after a dedicated network runtime is designed?
- Should incoming handshake routing stay at the app layer long term, or move into a transport/network runtime layer?
- What is the minimum identity migration needed to avoid future TCP/uTP/QUIC collisions without rewriting state all at once?

## Done Means For The Abstraction-Only Work

- All production peer traffic still uses TCP.
- `TcpStream` no longer crosses the app-to-manager incoming-peer boundary.
- Outbound peer connection setup goes through a transport abstraction rather than direct manager-level `TcpStream::connect`.
- Peer identity can represent transport kind.
- Existing TCP behavior and tests remain intact.
- The next agent can choose uTP or QUIC work without first untangling TCP from torrent manager/session wiring.
