# DHT Resumable Crawls Plan

## Summary

This plan proposes moving DHT peer discovery from the current "start a full lookup job and let it run to completion" model to a shared-budget model with resumable crawl state per active `info_hash`.

The goal is not to replace the current Kademlia lookup engine because it is incorrect. The current engine in [`src/dht/lookup.rs`](../src/dht/lookup.rs) and [`src/dht/mod.rs`](../src/dht/mod.rs) already performs bounded iterative lookups and yields peers correctly.

The problem we are solving is higher-level:
- many torrents can demand DHT help at the same time
- the app runs one shared DHT node, not one DHT instance per torrent
- full lookups are long-lived enough to create bursty background pressure
- preemption is coarse because canceling a lookup throws away its frontier
- restarting a lookup later repeats some of the same discovery work from scratch

Resumable crawls are a scheduler-quality feature, not a lookup-correctness feature.

## Problem Statement

Today the DHT service in [`src/dht/service.rs`](../src/dht/service.rs) schedules demand and starts full `get_peers` lookups. Those lookups:
- seed from cached responders, bootstrap nodes, and routing-table nodes
- walk outward until the lookup converges or exhausts itself
- can remain active for many seconds
- only downgrade future scheduling once they finish

That creates a mismatch between the current engine and the desired policy:
- demand is global and shared across many torrents
- lookup execution is still shaped like one long-lived traversal per search

The result is predictable:
- background work is bursty instead of smooth
- routine/non-urgent lookups can occupy many slots for long periods
- urgent work can only preempt by canceling existing work and losing progress
- healthy torrents and underfilled torrents still compete through the same long-lived primitive

## Why Resumable Crawls

Resumable crawls would let the DHT service:
- run a short slice of work for one target
- park the frontier and responder state
- rotate to other targets under the same shared budget
- later resume the same crawl without starting from scratch

That buys:
- steadier network pressure
- better fairness across many demanded torrents
- cheaper preemption
- less repeated frontier rediscovery
- better separation between "this torrent wants peers" and "this torrent gets a full traversal now"

This is specifically useful because `superseedr` is already structured around:
- one shared DHT runtime in [`src/dht/service.rs`](../src/dht/service.rs)
- many torrent managers contributing demand in [`src/torrent_manager/manager.rs`](../src/torrent_manager/manager.rs)

## What Other Engines Do

### libtorrent

The local libtorrent tree in `C:\Users\jagat\Projects\libtorrent` uses a mature traversal algorithm, but it is still a run-to-completion traversal model.

Relevant behavior:
- `search_branching = 5` in `include/libtorrent/kademlia/dht_settings.hpp`
- traversal keeps branch-factor pressure near the closest frontier in `src/kademlia/traversal_algorithm.cpp`
- short timeouts can temporarily expand branch factor before full failure
- traversal finishes when it has converged on the top `k` results with no relevant outstanding requests

This is good lookup behavior, but it does not provide a shared multi-torrent crawl planner or resumable parked frontier state.

### mainline crate

The local Rust `mainline` crate in `mainline-6.1.1` is even simpler:
- request timeout defaults to `2s` in `src/rpc/socket.rs`
- iterative query visits closest candidates up to `MAX_BUCKET_SIZE_K = 20`
- a query is done when no inflight requests remain in the socket

This is a bounded short-lived query model, but it also does not implement resumable per-target crawl state or a shared torrent-aware DHT budget planner.

### Takeaway

Both engines already solve the single-lookup problem reasonably well.

Neither engine solves the exact problem `superseedr` has:
- many torrent demands
- one shared DHT node
- one shared query budget
- desire for fairness and low steady-state background pressure

So resumable crawls here would not be "beating" those engines on lookup quality. They would be adding a scheduler capability those engines do not need to expose at their current abstraction layer.

## Goals

- Preserve current lookup quality for urgent searches.
- Reduce burstiness from background DHT work.
- Avoid restarting routine searches from scratch when they still have a useful frontier.
- Let the DHT planner rotate work across many torrents under one shared budget.
- Keep the design keyed by `info_hash`, not by `TorrentManager`, so multiple consumers share one crawl state.

## Non-Goals

- Do not create one routing table per torrent.
- Do not replace Kademlia traversal with a completely new algorithm.
- Do not attempt arbitrary pause/resume in the middle of outstanding inflight RPCs in the first version.
- Do not solve every DHT policy issue in the same patch as the crawl-state refactor.

## Proposed Design

### 1. Add a service-owned `DemandEntry`

In [`src/dht/service.rs`](../src/dht/service.rs), add one persistent entry per active `info_hash`:

```rust
struct DemandEntry {
    demand: DhtDemandState,
    subscriber_count: usize,
    last_search_started_at: Option<Instant>,
    last_search_finished_at: Option<Instant>,
    last_yield_at: Option<Instant>,
    last_progress_at: Option<Instant>,
    recent_peer_yield: usize,
    crawl: Option<DemandCrawlState>,
}
```

This separates:
- long-lived demand bookkeeping
- optional live/resumable crawl state

### 2. Introduce `DemandCrawlState`

The crawl state should live above the routing table but below the planner:

```rust
struct DemandCrawlState {
    info_hash: InfoHash,
    ipv4: Option<FamilyCrawlState>,
    ipv6: Option<FamilyCrawlState>,
    created_at: Instant,
    last_resumed_at: Option<Instant>,
    reset_count: u32,
}
```

And per family:

```rust
struct FamilyCrawlState {
    lookup_state: LookupState,
    last_progress_at: Instant,
    last_yield_at: Option<Instant>,
    yielded_unique_peers: usize,
    consecutive_bad_nodes: u32,
}
```

The key point is to reuse the existing `LookupState` from [`src/dht/lookup.rs`](../src/dht/lookup.rs) instead of inventing a second frontier representation.

### 3. Make the lookup engine resumable

In [`src/dht/mod.rs`](../src/dht/mod.rs), split the current fresh-start API into:
- fresh-start convenience methods
- methods that accept an existing `LookupState`

Target shape:

```rust
pub async fn start_get_peers_with_state(
    &mut self,
    family: AddressFamily,
    info_hash: InfoHash,
    state: LookupState,
) -> io::Result<(LookupId, Receiver<Vec<SocketAddr>>)>;
```

And internally:
- seed a fresh `LookupState` only if the caller does not already have one
- otherwise continue from the saved frontier/visited/responders state

### 4. Use slice execution, not full traversal execution

The planner should not resume a crawl and let it run to natural exhaustion by default.

Instead, define a `CrawlSlicePlan`:

```rust
struct CrawlSlicePlan {
    max_new_queries: usize,
    max_wall_time: Duration,
    max_idle_gap: Duration,
    max_unique_peers: Option<usize>,
    drain_timeout: Duration,
    allow_ipv6_hedge: bool,
}
```

Slice execution rules:
- resume a crawl state
- allow it to issue only up to `max_new_queries`
- collect yielded peers
- once the slice budget is spent, stop issuing new queries
- briefly drain responses for already-issued inflight work
- park the updated crawl state back into `DemandEntry`

This turns the engine from "full burst job" into "planner-controlled unit of work".

### 5. First version should only park at quiescent boundaries

To reduce correctness risk, the first resumable implementation should not attempt to serialize or preserve arbitrary inflight transport state.

Instead:
- allow active RPCs to complete or time out during the short drain phase
- only park the crawl after that small quiescent window
- if it does not quiesce cleanly, either:
  - continue briefly, or
  - reset and restart later

This avoids the hardest bug class in v1.

### 6. Add reset rules

A resumable crawl must be resettable.

Reset conditions:
- too many consecutive bad or timed-out nodes
- no closer-node progress for too long
- no peer yield after enough total work
- crawl state parked too long
- demand class changes sharply
- state quality clearly degrades

Reset action:
- drop `DemandCrawlState`
- keep `DemandEntry`
- next planner turn starts a fresh seeded crawl

### 7. Scheduler becomes slot-based

Instead of "launch full lookup if due", the service should own a small number of active crawl slots.

Per tick:
- rank active demand entries
- choose which entries get a slice
- run slices
- store updated crawl states

That is the main policy payoff:
- planner controls which crawl advances
- crawl state makes that advancement incremental instead of restart-heavy

## Why This Is Better Than Another Timer Tweak

Timer or cooldown tuning can reduce load, but it cannot address:
- restart waste
- coarse cancellation
- long-lived background burst shape
- inability to cheaply interleave progress across many torrents

Resumable crawls directly address those.

## Risks

- Pausing too aggressively can reduce lookup quality if urgent searches do not get enough uninterrupted progress.
- Poor reset heuristics can keep poisoned crawl state alive too long.
- Preserving too much state for too many torrents can grow memory unnecessarily.
- Trying to preserve arbitrary inflight state in v1 is likely too risky.

## Risk Mitigation

- Use resumable crawls first for background/recovery classes, not urgent classes.
- Let urgent classes keep larger slice budgets and fewer interruptions.
- Start with quiescent-boundary parking only.
- Cap per-crawl retained state:
  - frontier size
  - visited set
  - retained yielded peers
- Add clear reset rules and counters.

## Phased Implementation

### Phase 1: Structural Groundwork

- Add `DemandEntry` and `DemandCrawlState` to [`src/dht/service.rs`](../src/dht/service.rs)
- Keep current fresh-start scheduling behavior
- No resumable execution yet
- Add tests for demand entry lifecycle

### Phase 2: Runtime Resume Hook

- Refactor [`src/dht/mod.rs`](../src/dht/mod.rs) so a lookup can start from an existing `LookupState`
- Keep old API as a convenience wrapper
- Add tests that a resumed state behaves like a continued fresh traversal

### Phase 3: Slice Execution

- Add `CrawlSlicePlan`
- Implement service-side slice execution
- Park crawl state only after a short drain window
- Keep urgent classes closer to current full traversal behavior at first

### Phase 4: Slot Planner

- Replace "full lookup launch" with a slot-based planner
- Let each active slot run one slice for one `DemandEntry`
- Add per-class budgets

Current note:
- the current implementation now has resumable slices and parked-crawl reuse across class changes
- the next gap is `NoConnectedPeers` reset quality: low-quality resets are still too conservative there compared to `RoutineRefresh`
- the first slot-planner step should be a real shared active-slot cap, because the old "up to N launches per tick" behavior can still over-admit slices during busy periods

### Phase 5: Reset Rules And Telemetry

- Add reset heuristics
- Surface:
  - resume count
  - reset count
  - average slice wall time
  - peers yielded per slice
  - parked crawls by class

## Acceptance Criteria

- `AwaitingMetadata` and other urgent demand should not regress noticeably in time-to-first-peer.
- Background DHT work should no longer appear as long-lived bursts across many torrents.
- Canceling or deprioritizing a background crawl should not throw away all progress.
- Steady-state query pressure should stay materially lower than the old full-burst model.
- Memory growth for parked crawls should remain bounded and explainable.

## Recommendation

This is worth pursuing, but it should be implemented as:
- a reuse of `LookupState`
- a service-owned parked crawl state
- slice-based execution with reset rules

It should not start with:
- arbitrary inflight transport preservation
- one permanent heavyweight crawl state per torrent with no eviction path

That gives `superseedr` the main advantage of resumable crawls without immediately taking on the riskiest form of the refactor.
