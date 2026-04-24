# DHT Global Planner Budget Plan

## Summary

The next DHT scheduler step should move from "each torrent eventually becomes due" to "the DHT service owns a fixed global work budget and chooses the best candidates inside that budget."

The committed baseline is `0b9006e Tune DHT drain and no-peer backoff`. That commit keeps the useful fixes from the recent soak work:
- demand lookups drain instead of immediately throwing away late peer replies
- drain work is bounded and does not pump new queries
- no-peer retry backoff now reaches a five-minute max interval
- no-peer work is less aggressive after repeated low-yield slices

Those changes helped, but they are still not the final shape. The remaining scaling problem is that per-torrent timers still translate catalog size into launch pressure.

## Problem

The current service in [`src/dht/service.rs`](../src/dht/service.rs) already has a shared DHT runtime and demand planner, but scheduling is still partly timer-driven per torrent:
- a torrent becomes due based on its own demand state and backoff
- due candidates are ranked
- class slot caps limit concurrent active work
- drain work consumes virtual slots

That is better than launching everything immediately, but it still scales as `torrent_count / interval`.

Examples:
- 100 no-peer torrents at a 60s retry interval can offer about 100 launches/minute.
- 100 no-peer torrents at a 5m retry interval can offer about 20 launches/minute.
- 500 no-peer torrents at a 5m retry interval can offer about 100 launches/minute.
- 1000 no-peer torrents at a 5m retry interval can offer about 200 launches/minute.

So increasing the interval buys time, but it does not solve the large-catalog problem. A fully global planner should make the launch rate mostly independent of catalog size.

## What The Recent Soaks Showed

The soak instrumentation showed two important things:
- Minute-end `q` snapshots can be misleading because the system is bursty. Sampled `q` is a better indicator of real pressure.
- Drain was not the main long-term problem after smart drain was added. Late-window no-peer churn was the bigger issue: many low-yield no-peer searches were still being launched for very little return.

The best interim direction was:
- keep drain acceptance for useful late replies
- cap drain pressure
- back off no-peer work harder after low yield
- use a longer no-peer max interval

The global planner should keep those lessons but stop relying on timer length as the main pressure-control mechanism.

## Goal

Build a DHT planner where the service decides how much total DHT work to spend per time window, then assigns that budget to the best candidates.

Target behavior:
- metadata waiters remain urgent and bounded by active slots
- downloading torrents with too few peers get higher priority than background research
- seeding/no-peer torrents can still use spare capacity, but only under a global budget
- old non-yielding torrents eventually get another chance without forcing linear catalog churn
- drain preserves useful late replies without consuming unlimited future capacity
- query pressure and launch rate stay bounded for 100, 500, and 1000 torrent catalogs

## Non-Goals

- Do not rewrite Kademlia lookup correctness.
- Do not create one DHT node or routing table per torrent.
- Do not remove resumable crawl state.
- Do not tune constants indefinitely without adding budget accounting.
- Do not make every old torrent run just because its timer expired.

## Proposed Model

### 1. Per-torrent state becomes eligibility, not permission

Each torrent can still track:
- demand class
- last started time
- last finished time
- last useful yield
- connected peer count
- parked crawl quality
- no-peer backoff step
- subscriber count

But being "due" should only mean "eligible to compete for global budget." It should not guarantee a launch.

### 2. Add global token buckets

Add a `DemandPlannerBudget` owned by the DHT service. It should track launch tokens by work class.

Initial classes:
- `awaiting_metadata`
- `no_connected_peers`
- `routine_refresh`
- `spare_research`
- `drain`

Each class should have:
- refill rate per minute
- burst cap
- optional active slot cap
- optional minimum trickle
- optional global query cap contribution

The key change is that no-peer launch rate becomes something like "at most N launches/minute" instead of "one launch every I seconds per torrent."

### 3. Rank globally across all eligible candidates

Candidate ranking should happen across the whole catalog, not independently per class timer.

Inputs:
- demand class urgency
- how long the candidate has waited
- current connected peer count
- whether metadata is missing
- recent unique peer yield
- parked crawl reuse quality
- repeated zero-yield or weak-yield history
- whether the candidate is already draining
- whether a reset is needed due to stale or weak crawl state

The planner should produce one ordered launch list, then consume budget tokens as it accepts candidates.

### 4. Use floors, caps, and age boost

The planner should not rely on a single score.

Recommended rules:
- Metadata gets the strongest floor, because a torrent without metadata cannot make progress.
- Downloading/no-peer gets a high cap and high priority, because it directly affects transfer progress.
- Seeding/no-peer and routine refresh get smaller budgets and mostly use spare capacity.
- Very old candidates get age boost so they are not permanently choked.
- Low-yield candidates keep their backoff, but can still receive an occasional trickle slot.

This replaces the dedicated "oldest reserve" slot with a general fairness rule.

### 5. Separate active slots, launch tokens, and drain capacity

These are different controls and should remain separate:
- Active slots limit how many slices are running now.
- Launch tokens limit how many new slices can start per time window.
- Drain capacity limits how much parked inflight work can be preserved.
- Query pressure telemetry measures actual network pressure.

This avoids the current failure mode where lowering one cap appears to help while another path still creates churn.

### 6. Keep resumable crawls as the execution primitive

The global planner should keep the current resumable crawl direction:
- short slice
- bounded wall time
- bounded idle timeout
- bounded unique peer cap
- park crawl state
- optionally drain useful inflight replies

Resumable crawl state is still worth keeping because it reduces repeated frontier rediscovery and makes preemption cheaper. The global planner decides when a crawl gets another slice.

## Per-Torrent Work To Remove From Linear Scaling

These are the per-torrent behaviors that should become globally budgeted:
- no-peer retry launches
- routine refresh launches
- spare research for seeding torrents
- reset/retry after weak parked crawl quality
- drain admission after slices stop
- any future "research old torrents" behavior

The torrent can own the facts. The DHT service should own the rate.

## Initial Budget Defaults

These are starting values for testing, not final constants:
- metadata: `30 launches/min`, burst `8`, active cap from the existing metadata slot cap
- downloading no-peers: `30 launches/min`, burst `10`, active cap from existing no-peer slots
- seeding/spare no-peers: `10 launches/min`, burst `5`, only when urgent queues are below cap
- routine refresh: `5 launches/min`, burst `5`
- stale trickle: `2 launches/min`, burst `2`, for old low-yield candidates
- drain: keep virtual slots, plus a global drain inflight cap

For a 1000 torrent catalog, this would still start around tens of launches/minute, not hundreds.

## Implementation Plan

1. Add `DemandPlannerBudget`.

Create a service-owned budget object with deterministic token refill. It should be testable without Tokio time by passing `Instant`.

2. Add budget-aware candidate selection.

Update `start_due_demands` so candidates are first ranked globally, then accepted only if their class can consume a token and active slots are available.

3. Convert due timers into eligibility hints.

Keep existing backoff timestamps, but treat them as candidate filters. A due timestamp should not bypass the global launch budget.

4. Replace special oldest-reserve behavior with age boost.

Use waited time as a score component and optional stale-trickle budget. This gives old hashes a chance without a fixed slot that can behave differently from the rest of the planner.

5. Add budget telemetry.

Add disposable and then permanent counters for:
- candidates offered by class
- candidates launched by class
- candidates throttled by class
- launch tokens available/consumed by class
- age of oldest throttled candidate
- peers per launch by class
- sampled active/drain query pressure

6. Add scheduler tests.

Add deterministic tests for:
- 100 no-peer candidates
- 500 no-peer candidates
- 1000 no-peer candidates
- metadata candidates overtaking no-peer candidates
- stale low-yield candidates eventually getting trickle budget
- drain capacity not blocking urgent metadata launches

7. Run live soaks.

Use the existing disposable soak/q instrumentation while implementing this. Once the global planner is validated, discard the instrumentation and keep only the stable planner metrics.

## Acceptance Criteria

- With 1000 no-peer eligible torrents, launches/minute stays near the configured no-peer budget instead of `1000 / interval`.
- Metadata waiters launch promptly even when many no-peer candidates are eligible.
- No-peer searches still find peers at a comparable or better peers-per-launch rate than the current `0b9006e` baseline.
- Sampled `q` remains bounded and does not drift upward as catalog size grows.
- Old low-yield hashes eventually receive retries without requiring a dedicated oldest slot.
- Drain contributes useful late replies but does not dominate query pressure.

## Suggested First Patch

Start with the smallest architectural change:
- add `DemandPlannerBudget`
- wire it into `start_due_demands`
- add tests proving launch tokens cap no-peer launches across large candidate sets
- leave existing constants and scoring mostly intact

That should give us the scaling primitive before more tuning.
