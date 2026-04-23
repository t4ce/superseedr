# DHT Soak Follow-Up: Changes To Keep After Instrumentation Discard

Context: before adding the 15-hour soak instrumentation, there was one outstanding functional cleanup on top of commit `e80dd49 Tune DHT demand planner outcomes`.

## Keep After Soak

- Cap `no_connected_peers_backoff_step` at the first step that reaches the configured max interval.
- With the current policy of `8s` base and `60s` max, the useful cap is step `3`.
- Ensure accelerated healthy-zero backoff cannot keep increasing the stored step after the effective interval is already capped.
- Keep the regression test `no_connected_peers_backoff_step_stays_capped_at_max_interval`.

## Discard After Soak

- `SUPERSEEDR_DHT_SOAK_LOG`.
- Five-minute aggregate `superseedr::dht_soak` summaries.
- Cancelled in-flight query accounting added only for soak analysis.
- Late cancelled-reply ledger and response usefulness counters.
- Soak-only counters for launches, stops, outcomes, spare launches, and peer totals.

## Intended Flow

1. Run the soak with the temporary instrumentation enabled.
2. Analyze the aggregate soak summaries.
3. Discard the soak instrumentation changes.
4. Re-apply and commit only the backoff-step cap cleanup and its regression test.
