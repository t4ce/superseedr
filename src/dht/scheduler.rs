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

    pub(super) fn take_due(&mut self, now: Instant, limit: usize) -> Vec<InfoHash> {
        let mut due = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry.subscriber_count > 0 && !entry.in_progress && entry.next_eligible_at <= now
            })
            .map(|(info_hash, entry)| {
                (
                    *info_hash,
                    DemandClass::from_demand(entry.demand),
                    entry.next_eligible_at,
                    entry.subscriber_count,
                )
            })
            .collect::<Vec<_>>();

        due.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| left.2.cmp(&right.2))
                .then_with(|| right.3.cmp(&left.3))
        });
        due.truncate(limit);

        let info_hashes = due
            .into_iter()
            .map(|(info_hash, _, _, _)| info_hash)
            .collect::<Vec<_>>();
        for info_hash in &info_hashes {
            if let Some(entry) = self.entries.get_mut(info_hash) {
                entry.in_progress = true;
                entry.retrigger_pending = false;
            }
        }

        info_hashes
    }

    pub(super) fn finish(&mut self, info_hash: InfoHash, now: Instant) {
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
        let next_eligible_at = if retrigger_pending {
            now
        } else {
            now + self.interval_for_demand(demand, no_connected_peers_backoff_step)
        };
        let demand_class = DemandClass::from_demand(demand);
        let next_interval = next_eligible_at.saturating_duration_since(now);

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
            if next_interval < self.no_connected_peers_max_interval {
                entry.no_connected_peers_backoff_step =
                    entry.no_connected_peers_backoff_step.saturating_add(1);
            }
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

    fn info_hash(byte: u8) -> InfoHash {
        InfoHash::from([byte; InfoHash::LEN])
    }

    fn demand(awaiting_metadata: bool, connected_peers: usize) -> DhtDemandState {
        DhtDemandState {
            awaiting_metadata,
            connected_peers,
        }
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
}
