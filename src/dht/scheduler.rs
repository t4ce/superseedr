// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::types::InfoHash;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DhtDemandState {
    pub awaiting_metadata: bool,
    pub connected_peers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DueDemandCandidate {
    pub info_hash: InfoHash,
    pub demand: DhtDemandState,
    pub next_eligible_at: Instant,
    pub subscriber_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DemandEntrySnapshot {
    pub info_hash: InfoHash,
    pub demand: DhtDemandState,
    pub next_eligible_at: Instant,
    pub subscriber_count: usize,
    pub in_progress: bool,
    pub retrigger_pending: bool,
    pub no_connected_peers_backoff_step: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemandFinishMode {
    Standard,
    AcceleratedNoConnectedPeersBackoff,
}

impl DemandFinishMode {
    fn no_connected_peers_backoff_extra_steps(self) -> u8 {
        match self {
            Self::Standard => 0,
            Self::AcceleratedNoConnectedPeersBackoff => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DemandClass {
    RoutineRefresh,
    NoConnectedPeers,
    AwaitingMetadata,
}

impl DemandClass {
    fn from_demand(demand: DhtDemandState) -> Self {
        if demand.awaiting_metadata {
            Self::AwaitingMetadata
        } else if demand.connected_peers == 0 {
            Self::NoConnectedPeers
        } else {
            Self::RoutineRefresh
        }
    }
}

#[derive(Debug)]
struct DemandEntry {
    subscriber_count: usize,
    next_eligible_at: Instant,
    in_progress: bool,
    demand: DhtDemandState,
    retrigger_pending: bool,
    no_connected_peers_backoff_step: u8,
}

#[derive(Debug)]
pub(super) struct DemandScheduler {
    entries: HashMap<InfoHash, DemandEntry>,
    routine_refresh_interval: Duration,
    no_connected_peers_base_interval: Duration,
    no_connected_peers_max_interval: Duration,
    awaiting_metadata_interval: Duration,
}

impl DemandScheduler {
    pub(super) fn new(
        routine_refresh_interval: Duration,
        no_connected_peers_base_interval: Duration,
        no_connected_peers_max_interval: Duration,
        awaiting_metadata_interval: Duration,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            routine_refresh_interval,
            no_connected_peers_base_interval,
            no_connected_peers_max_interval,
            awaiting_metadata_interval,
        }
    }

    fn interval_for_demand(
        &self,
        demand: DhtDemandState,
        no_connected_peers_backoff_step: u8,
    ) -> Duration {
        match DemandClass::from_demand(demand) {
            DemandClass::RoutineRefresh => self.routine_refresh_interval,
            DemandClass::AwaitingMetadata => self.awaiting_metadata_interval,
            DemandClass::NoConnectedPeers => {
                let multiplier = 1u32
                    .checked_shl(u32::from(no_connected_peers_backoff_step))
                    .unwrap_or(u32::MAX);
                let interval = self
                    .no_connected_peers_base_interval
                    .saturating_mul(multiplier);
                std::cmp::min(interval, self.no_connected_peers_max_interval)
            }
        }
    }

    fn no_connected_peers_backoff_step_cap(&self) -> u8 {
        if self.no_connected_peers_base_interval.is_zero()
            || self.no_connected_peers_base_interval >= self.no_connected_peers_max_interval
        {
            return 0;
        }

        let mut step = 0u8;
        let mut interval = self.no_connected_peers_base_interval;
        while interval < self.no_connected_peers_max_interval && step < u8::MAX {
            step = step.saturating_add(1);
            interval = interval.saturating_mul(2);
        }
        step
    }

    fn capped_no_connected_peers_backoff_step(&self, step: u8) -> u8 {
        step.min(self.no_connected_peers_backoff_step_cap())
    }

    fn apply_demand_update(entry: &mut DemandEntry, demand: DhtDemandState, now: Instant) {
        let previous_class = DemandClass::from_demand(entry.demand);
        let next_class = DemandClass::from_demand(demand);
        entry.demand = demand;
        if next_class != DemandClass::NoConnectedPeers {
            entry.no_connected_peers_backoff_step = 0;
        } else if previous_class != DemandClass::NoConnectedPeers {
            entry.no_connected_peers_backoff_step = 0;
        }

        if next_class > previous_class {
            if entry.in_progress {
                entry.retrigger_pending = true;
            } else {
                entry.next_eligible_at = now;
            }
        }
    }

    pub(super) fn register(&mut self, info_hash: InfoHash, demand: DhtDemandState, now: Instant) {
        use std::collections::hash_map::Entry;

        match self.entries.entry(info_hash) {
            Entry::Vacant(slot) => {
                slot.insert(DemandEntry {
                    subscriber_count: 1,
                    next_eligible_at: now,
                    in_progress: false,
                    demand,
                    retrigger_pending: false,
                    no_connected_peers_backoff_step: 0,
                });
            }
            Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                entry.subscriber_count = entry.subscriber_count.saturating_add(1);
                Self::apply_demand_update(entry, demand, now);
            }
        }
    }

    pub(super) fn unregister(&mut self, info_hash: InfoHash) -> bool {
        let Some(entry) = self.entries.get_mut(&info_hash) else {
            return false;
        };

        entry.subscriber_count = entry.subscriber_count.saturating_sub(1);
        if entry.subscriber_count == 0 {
            self.entries.remove(&info_hash);
            return true;
        }

        false
    }

    pub(super) fn update(&mut self, info_hash: InfoHash, demand: DhtDemandState, now: Instant) {
        let Some(entry) = self.entries.get_mut(&info_hash) else {
            return;
        };

        Self::apply_demand_update(entry, demand, now);
    }

    pub(super) fn demand_state(&self, info_hash: InfoHash) -> Option<DhtDemandState> {
        self.entries.get(&info_hash).map(|entry| entry.demand)
    }

    pub(super) fn entry_snapshot(&self, info_hash: InfoHash) -> Option<DemandEntrySnapshot> {
        self.entries
            .get(&info_hash)
            .map(|entry| DemandEntrySnapshot {
                info_hash,
                demand: entry.demand,
                next_eligible_at: entry.next_eligible_at,
                subscriber_count: entry.subscriber_count,
                in_progress: entry.in_progress,
                retrigger_pending: entry.retrigger_pending,
                no_connected_peers_backoff_step: entry.no_connected_peers_backoff_step,
            })
    }

    pub(super) fn entry_snapshots(&self) -> Vec<DemandEntrySnapshot> {
        self.entries
            .iter()
            .map(|(info_hash, entry)| DemandEntrySnapshot {
                info_hash: *info_hash,
                demand: entry.demand,
                next_eligible_at: entry.next_eligible_at,
                subscriber_count: entry.subscriber_count,
                in_progress: entry.in_progress,
                retrigger_pending: entry.retrigger_pending,
                no_connected_peers_backoff_step: entry.no_connected_peers_backoff_step,
            })
            .collect()
    }

    pub(super) fn due_candidates(&self, now: Instant) -> Vec<DueDemandCandidate> {
        let mut due = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry.subscriber_count > 0 && !entry.in_progress && entry.next_eligible_at <= now
            })
            .map(|(info_hash, entry)| {
                (
                    DueDemandCandidate {
                        info_hash: *info_hash,
                        demand: entry.demand,
                        next_eligible_at: entry.next_eligible_at,
                        subscriber_count: entry.subscriber_count,
                    },
                    DemandClass::from_demand(entry.demand),
                )
            })
            .collect::<Vec<_>>();

        due.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| left.0.next_eligible_at.cmp(&right.0.next_eligible_at))
                .then_with(|| right.0.subscriber_count.cmp(&left.0.subscriber_count))
        });

        due.into_iter().map(|(candidate, _)| candidate).collect()
    }

    pub(super) fn mark_in_progress(&mut self, info_hash: InfoHash) -> bool {
        let Some(entry) = self.entries.get_mut(&info_hash) else {
            return false;
        };
        if entry.subscriber_count == 0 || entry.in_progress {
            return false;
        }
        entry.in_progress = true;
        entry.retrigger_pending = false;
        true
    }

    pub(super) fn take_due(&mut self, now: Instant, limit: usize) -> Vec<InfoHash> {
        let info_hashes = self
            .due_candidates(now)
            .into_iter()
            .take(limit)
            .map(|candidate| candidate.info_hash)
            .collect::<Vec<_>>();
        for info_hash in &info_hashes {
            let _ = self.mark_in_progress(*info_hash);
        }

        info_hashes
    }

    pub(super) fn finish(&mut self, info_hash: InfoHash, now: Instant) {
        self.finish_with_mode(info_hash, now, DemandFinishMode::Standard);
    }

    pub(super) fn finish_with_mode(
        &mut self,
        info_hash: InfoHash,
        now: Instant,
        mode: DemandFinishMode,
    ) {
        let Some((retrigger_pending, demand, no_connected_peers_backoff_step)) =
            self.entries.get(&info_hash).map(|entry| {
                (
                    entry.retrigger_pending,
                    entry.demand,
                    entry.no_connected_peers_backoff_step,
                )
            })
        else {
            return;
        };
        let demand_class = DemandClass::from_demand(demand);
        let effective_no_connected_peers_backoff_step =
            if demand_class == DemandClass::NoConnectedPeers {
                self.capped_no_connected_peers_backoff_step(
                    no_connected_peers_backoff_step
                        .saturating_add(mode.no_connected_peers_backoff_extra_steps()),
                )
            } else {
                no_connected_peers_backoff_step
            };
        let next_eligible_at = if retrigger_pending {
            now
        } else {
            now + self.interval_for_demand(demand, effective_no_connected_peers_backoff_step)
        };
        let next_interval = next_eligible_at.saturating_duration_since(now);
        let next_no_connected_peers_backoff_step = if demand_class == DemandClass::NoConnectedPeers
        {
            if next_interval < self.no_connected_peers_max_interval {
                self.capped_no_connected_peers_backoff_step(
                    effective_no_connected_peers_backoff_step.saturating_add(1),
                )
            } else {
                effective_no_connected_peers_backoff_step
            }
        } else {
            0
        };

        let Some(entry) = self.entries.get_mut(&info_hash) else {
            return;
        };
        entry.in_progress = false;
        entry.next_eligible_at = next_eligible_at;
        entry.retrigger_pending = false;
        if retrigger_pending {
            return;
        }

        if demand_class == DemandClass::NoConnectedPeers {
            entry.no_connected_peers_backoff_step = next_no_connected_peers_backoff_step;
        } else {
            entry.no_connected_peers_backoff_step = 0;
        }
    }

    pub(super) fn reset_active(&mut self, now: Instant) {
        for entry in self.entries.values_mut() {
            entry.in_progress = false;
            if entry.retrigger_pending {
                entry.next_eligible_at = now;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    fn info_hash(byte: u8) -> InfoHash {
        InfoHash::from([byte; InfoHash::LEN])
    }

    fn demand(awaiting_metadata: bool, connected_peers: usize) -> DhtDemandState {
        DhtDemandState {
            awaiting_metadata,
            connected_peers,
        }
    }

    #[derive(Debug, Clone)]
    enum SchedulerOp {
        Register {
            key: u8,
            demand: DhtDemandState,
            advance_ms: u16,
        },
        Update {
            key: u8,
            demand: DhtDemandState,
            advance_ms: u16,
        },
        Unregister {
            key: u8,
            advance_ms: u16,
        },
        MarkInProgress {
            key: u8,
            advance_ms: u16,
        },
        Finish {
            key: u8,
            accelerated: bool,
            advance_ms: u16,
        },
        ResetActive {
            advance_ms: u16,
        },
        TakeDue {
            limit: u8,
            advance_ms: u16,
        },
    }

    fn demand_strategy() -> impl Strategy<Value = DhtDemandState> {
        (any::<bool>(), 0usize..=16).prop_map(|(awaiting_metadata, connected_peers)| {
            DhtDemandState {
                awaiting_metadata,
                connected_peers,
            }
        })
    }

    fn scheduler_op_strategy() -> impl Strategy<Value = SchedulerOp> {
        let key = 0u8..32;
        let advance_ms = 0u16..=5_000;

        prop_oneof![
            (key.clone(), demand_strategy(), advance_ms.clone()).prop_map(
                |(key, demand, advance_ms)| SchedulerOp::Register {
                    key,
                    demand,
                    advance_ms,
                }
            ),
            (key.clone(), demand_strategy(), advance_ms.clone()).prop_map(
                |(key, demand, advance_ms)| SchedulerOp::Update {
                    key,
                    demand,
                    advance_ms,
                }
            ),
            (key.clone(), advance_ms.clone())
                .prop_map(|(key, advance_ms)| { SchedulerOp::Unregister { key, advance_ms } }),
            (key.clone(), advance_ms.clone())
                .prop_map(|(key, advance_ms)| { SchedulerOp::MarkInProgress { key, advance_ms } }),
            (key, any::<bool>(), advance_ms.clone()).prop_map(|(key, accelerated, advance_ms)| {
                SchedulerOp::Finish {
                    key,
                    accelerated,
                    advance_ms,
                }
            }),
            advance_ms
                .clone()
                .prop_map(|advance_ms| SchedulerOp::ResetActive { advance_ms }),
            (0u8..=16, advance_ms)
                .prop_map(|(limit, advance_ms)| SchedulerOp::TakeDue { limit, advance_ms }),
        ]
    }

    fn assert_scheduler_invariants(
        scheduler: &DemandScheduler,
        now: Instant,
    ) -> Result<(), TestCaseError> {
        let snapshots = scheduler.entry_snapshots();
        let mut seen = HashSet::new();
        for snapshot in &snapshots {
            prop_assert!(seen.insert(snapshot.info_hash));
            prop_assert!(snapshot.subscriber_count > 0);
            prop_assert!(
                snapshot.no_connected_peers_backoff_step
                    <= scheduler.no_connected_peers_backoff_step_cap()
            );
            if DemandClass::from_demand(snapshot.demand) != DemandClass::NoConnectedPeers {
                prop_assert_eq!(snapshot.no_connected_peers_backoff_step, 0);
            }
        }

        let due = scheduler.due_candidates(now);
        let mut previous_class = None;
        for candidate in due {
            let snapshot = scheduler
                .entry_snapshot(candidate.info_hash)
                .expect("due candidate must have a scheduler entry");
            prop_assert!(snapshot.subscriber_count > 0);
            prop_assert!(!snapshot.in_progress);
            prop_assert!(snapshot.next_eligible_at <= now);
            prop_assert_eq!(snapshot.demand, candidate.demand);
            prop_assert_eq!(snapshot.subscriber_count, candidate.subscriber_count);
            prop_assert_eq!(snapshot.next_eligible_at, candidate.next_eligible_at);

            let class = DemandClass::from_demand(candidate.demand);
            if let Some(previous_class) = previous_class {
                prop_assert!(previous_class >= class);
            }
            previous_class = Some(class);
        }

        Ok(())
    }

    #[test]
    fn register_is_due_immediately() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );

        scheduler.register(info_hash(1), demand(false, 2), now);

        assert_eq!(scheduler.take_due(now, 8), vec![info_hash(1)]);
    }

    #[test]
    fn more_urgent_update_during_active_lookup_requeues_immediately() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let hash = info_hash(2);

        scheduler.register(hash, demand(false, 3), now);
        assert_eq!(scheduler.take_due(now, 8), vec![hash]);

        scheduler.update(hash, demand(false, 0), now + Duration::from_secs(1));
        scheduler.finish(hash, now + Duration::from_secs(2));

        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(2), 8),
            vec![hash]
        );
    }

    #[test]
    fn urgent_entries_are_prioritized() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let normal = info_hash(3);
        let urgent = info_hash(4);

        scheduler.register(normal, demand(false, 4), now - Duration::from_secs(10));
        scheduler.take_due(now - Duration::from_secs(10), 8);
        scheduler.finish(normal, now - Duration::from_secs(9));

        scheduler.register(urgent, demand(true, 0), now);

        let picked = scheduler.take_due(now + Duration::from_secs(60), 8);
        assert_eq!(picked.first().copied(), Some(urgent));
        assert!(picked.contains(&normal));
    }

    #[test]
    fn less_urgent_update_does_not_force_immediate_rerun() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let hash = info_hash(5);

        scheduler.register(hash, demand(true, 0), now);
        assert_eq!(scheduler.take_due(now, 8), vec![hash]);
        scheduler.finish(hash, now);

        scheduler.update(hash, demand(false, 0), now + Duration::from_millis(100));
        assert!(scheduler
            .take_due(now + Duration::from_millis(999), 8)
            .is_empty());
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(1), 8),
            vec![hash]
        );
    }

    #[test]
    fn finish_uses_reason_specific_intervals() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let no_peers = info_hash(6);
        let metadata = info_hash(7);

        scheduler.register(no_peers, demand(false, 0), now);
        assert_eq!(scheduler.take_due(now, 8), vec![no_peers]);
        scheduler.finish(no_peers, now);
        assert!(scheduler
            .take_due(now + Duration::from_secs(7), 8)
            .is_empty());
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(8), 8),
            vec![no_peers]
        );

        scheduler.register(metadata, demand(true, 0), now);
        let picked = scheduler.take_due(now, 8);
        assert!(picked.contains(&metadata));
        scheduler.finish(metadata, now);
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(1), 8),
            vec![metadata]
        );
    }

    #[test]
    fn no_connected_peers_backoff_grows_to_cap() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let hash = info_hash(8);

        scheduler.register(hash, demand(false, 0), now);
        assert_eq!(scheduler.take_due(now, 8), vec![hash]);
        scheduler.finish(hash, now);
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(8), 8),
            vec![hash]
        );
        scheduler.finish(hash, now + Duration::from_secs(8));
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(24), 8),
            vec![hash]
        );
        scheduler.finish(hash, now + Duration::from_secs(24));
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(56), 8),
            vec![hash]
        );
        scheduler.finish(hash, now + Duration::from_secs(56));
        assert!(scheduler
            .take_due(now + Duration::from_secs(115), 8)
            .is_empty());
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(116), 8),
            vec![hash]
        );
    }

    #[test]
    fn accelerated_no_connected_peers_backoff_skips_one_step() {
        let now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let hash = info_hash(9);

        scheduler.register(hash, demand(false, 0), now);
        assert_eq!(scheduler.take_due(now, 8), vec![hash]);
        scheduler.finish_with_mode(
            hash,
            now,
            DemandFinishMode::AcceleratedNoConnectedPeersBackoff,
        );
        assert!(scheduler
            .take_due(now + Duration::from_secs(15), 8)
            .is_empty());
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(16), 8),
            vec![hash]
        );

        scheduler.finish(hash, now + Duration::from_secs(16));
        assert!(scheduler
            .take_due(now + Duration::from_secs(47), 8)
            .is_empty());
        assert_eq!(
            scheduler.take_due(now + Duration::from_secs(48), 8),
            vec![hash]
        );
    }

    #[test]
    fn no_connected_peers_backoff_step_stays_capped_at_max_interval() {
        let mut now = Instant::now();
        let mut scheduler = DemandScheduler::new(
            Duration::from_secs(60),
            Duration::from_secs(8),
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let hash = info_hash(10);

        assert_eq!(scheduler.no_connected_peers_backoff_step_cap(), 3);

        scheduler.register(hash, demand(false, 0), now);
        for _ in 0..8 {
            assert_eq!(scheduler.take_due(now, 8), vec![hash]);
            scheduler.finish_with_mode(
                hash,
                now,
                DemandFinishMode::AcceleratedNoConnectedPeersBackoff,
            );
            let snapshot = scheduler.entry_snapshot(hash).expect("demand entry");
            assert!(snapshot.no_connected_peers_backoff_step <= 3);
            now = snapshot.next_eligible_at;
        }

        assert_eq!(
            scheduler
                .entry_snapshot(hash)
                .expect("demand entry")
                .no_connected_peers_backoff_step,
            3
        );
    }

    proptest! {
        #[test]
        fn demand_scheduler_state_fuzz_keeps_entries_consistent(
            ops in prop::collection::vec(scheduler_op_strategy(), 1..160)
        ) {
            let mut now = Instant::now();
            let mut scheduler = DemandScheduler::new(
                Duration::from_secs(60),
                Duration::from_secs(8),
                Duration::from_secs(60),
                Duration::from_secs(1),
            );

            for op in ops {
                let advance_ms = match &op {
                    SchedulerOp::Register { advance_ms, .. }
                    | SchedulerOp::Update { advance_ms, .. }
                    | SchedulerOp::Unregister { advance_ms, .. }
                    | SchedulerOp::MarkInProgress { advance_ms, .. }
                    | SchedulerOp::Finish { advance_ms, .. }
                    | SchedulerOp::ResetActive { advance_ms }
                    | SchedulerOp::TakeDue { advance_ms, .. } => *advance_ms,
                };
                now += Duration::from_millis(u64::from(advance_ms));

                match op {
                    SchedulerOp::Register { key, demand, .. } => {
                        scheduler.register(info_hash(key), demand, now);
                    }
                    SchedulerOp::Update { key, demand, .. } => {
                        scheduler.update(info_hash(key), demand, now);
                    }
                    SchedulerOp::Unregister { key, .. } => {
                        scheduler.unregister(info_hash(key));
                    }
                    SchedulerOp::MarkInProgress { key, .. } => {
                        let hash = info_hash(key);
                        let marked = scheduler.mark_in_progress(hash);
                        if marked {
                            let snapshot = scheduler.entry_snapshot(hash).expect("marked entry");
                            prop_assert!(snapshot.in_progress);
                        }
                    }
                    SchedulerOp::Finish {
                        key, accelerated, ..
                    } => {
                        let mode = if accelerated {
                            DemandFinishMode::AcceleratedNoConnectedPeersBackoff
                        } else {
                            DemandFinishMode::Standard
                        };
                        scheduler.finish_with_mode(info_hash(key), now, mode);
                    }
                    SchedulerOp::ResetActive { .. } => {
                        scheduler.reset_active(now);
                    }
                    SchedulerOp::TakeDue { limit, .. } => {
                        let expected = scheduler
                            .due_candidates(now)
                            .into_iter()
                            .take(usize::from(limit))
                            .map(|candidate| candidate.info_hash)
                            .collect::<Vec<_>>();
                        let actual = scheduler.take_due(now, usize::from(limit));
                        prop_assert_eq!(&actual, &expected);
                        for hash in actual {
                            let snapshot = scheduler.entry_snapshot(hash).expect("taken entry");
                            prop_assert!(snapshot.in_progress);
                        }
                    }
                }

                assert_scheduler_invariants(&scheduler, now)?;
            }
        }
    }
}
