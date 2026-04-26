// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

#[cfg(test)]
#[path = "planner_tests.rs"]
mod tests;

#[derive(Debug, Clone)]
pub(super) struct ActiveDemandLookup {
    pub(super) lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    pub(super) slice_class: DemandSliceClass,
}

#[derive(Debug, Clone)]
pub(super) struct DrainingDemandLookup {
    pub(super) lookup_ids: Vec<LookupId>,
    pub(super) slice_class: DemandSliceClass,
    pub(super) stop_reason: DemandSliceStopReason,
    pub(super) started_at: Instant,
    pub(super) total_peers: usize,
    pub(super) initial_unique_peers: usize,
    pub(super) unique_peers: HashSet<SocketAddr>,
    pub(super) deadline: Instant,
    pub(super) no_late_yield_deadline: Instant,
    pub(super) initial_inflight_queries: usize,
    pub(super) score: i32,
}

impl DrainingDemandLookup {
    pub(super) fn record_peers(&mut self, peers: &[SocketAddr]) -> usize {
        let previous_unique_peers = self.unique_peers.len();
        self.total_peers = self.total_peers.saturating_add(peers.len());
        self.unique_peers.extend(peers.iter().copied());
        self.unique_peers
            .len()
            .saturating_sub(previous_unique_peers)
    }

    pub(super) fn unique_peer_count(&self) -> usize {
        self.unique_peers.len()
    }

    pub(super) fn late_unique_peer_count(&self) -> usize {
        self.unique_peer_count()
            .saturating_sub(self.initial_unique_peers)
    }

    pub(super) fn duration_ms(&self, now: Instant) -> u64 {
        duration_ms(now.saturating_duration_since(self.started_at))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DrainedDemandOutcome {
    pub(super) slice_class: DemandSliceClass,
    pub(super) stop_reason: DemandSliceStopReason,
    pub(super) total_peers: usize,
    pub(super) unique_peers: usize,
    pub(super) parked_outcome: Option<DemandParkedSliceOutcome>,
    pub(super) drain_duration_ms: u64,
    pub(super) finalized_after_deadline: bool,
    pub(super) finalized_early_no_yield: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct DemandPlannerState {
    pub(super) last_started_at: Option<Instant>,
    pub(super) last_finished_at: Option<Instant>,
    pub(super) last_useful_yield_at: Option<Instant>,
    pub(super) last_unique_peers: usize,
}

impl DemandPlannerState {
    pub(super) fn note_start(&mut self, now: Instant) {
        self.last_started_at = Some(now);
    }

    pub(super) fn note_finish(&mut self, now: Instant, unique_peers: usize) {
        self.last_finished_at = Some(now);
        self.last_unique_peers = unique_peers;
        if unique_peers > 0 {
            self.last_useful_yield_at = Some(now);
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DemandLaunchTokenBucket {
    pub(super) tokens_scaled: u64,
    pub(super) burst_scaled: u64,
    pub(super) refill_per_minute: u64,
    pub(super) refill_remainder: u128,
    pub(super) last_refill_at: Instant,
}

impl DemandLaunchTokenBucket {
    pub(super) fn new(refill_per_minute: u64, burst: u64, now: Instant) -> Self {
        let burst_scaled = burst.saturating_mul(DHT_PLANNER_TOKEN_SCALE);
        Self {
            tokens_scaled: burst_scaled,
            burst_scaled,
            refill_per_minute,
            refill_remainder: 0,
            last_refill_at: now,
        }
    }

    pub(super) fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill_at);
        if elapsed.is_zero() {
            return;
        }

        let elapsed_ms = elapsed.as_millis();
        let refill_units = u128::from(self.refill_per_minute)
            .saturating_mul(u128::from(DHT_PLANNER_TOKEN_SCALE))
            .saturating_mul(elapsed_ms)
            .saturating_add(self.refill_remainder);
        let add_scaled = (refill_units / 60_000) as u64;
        self.refill_remainder = refill_units % 60_000;
        self.tokens_scaled = self
            .tokens_scaled
            .saturating_add(add_scaled)
            .min(self.burst_scaled);
        if self.tokens_scaled == self.burst_scaled {
            self.refill_remainder = 0;
        }
        self.last_refill_at = now;
    }

    pub(super) fn try_consume(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens_scaled < DHT_PLANNER_TOKEN_SCALE {
            return false;
        }

        self.tokens_scaled = self.tokens_scaled.saturating_sub(DHT_PLANNER_TOKEN_SCALE);
        true
    }

    pub(super) fn refund(&mut self) {
        self.tokens_scaled = self
            .tokens_scaled
            .saturating_add(DHT_PLANNER_TOKEN_SCALE)
            .min(self.burst_scaled);
    }

    pub(super) fn available(&self) -> usize {
        (self.tokens_scaled / DHT_PLANNER_TOKEN_SCALE) as usize
    }
}

#[derive(Debug, Clone)]
pub(super) struct DemandPlannerBudget {
    pub(super) awaiting_metadata: DemandLaunchTokenBucket,
    pub(super) no_connected_peers: DemandLaunchTokenBucket,
    pub(super) routine_refresh: DemandLaunchTokenBucket,
}

impl DemandPlannerBudget {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            awaiting_metadata: DemandLaunchTokenBucket::new(
                DHT_AWAITING_METADATA_LAUNCHES_PER_MINUTE,
                DHT_AWAITING_METADATA_LAUNCH_BURST,
                now,
            ),
            no_connected_peers: DemandLaunchTokenBucket::new(
                DHT_NO_CONNECTED_PEERS_LAUNCHES_PER_MINUTE,
                DHT_NO_CONNECTED_PEERS_LAUNCH_BURST,
                now,
            ),
            routine_refresh: DemandLaunchTokenBucket::new(
                DHT_ROUTINE_REFRESH_LAUNCHES_PER_MINUTE,
                DHT_ROUTINE_REFRESH_LAUNCH_BURST,
                now,
            ),
        }
    }

    pub(super) fn bucket_mut(&mut self, class: DemandSliceClass) -> &mut DemandLaunchTokenBucket {
        match class {
            DemandSliceClass::AwaitingMetadata => &mut self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &mut self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &mut self.routine_refresh,
        }
    }

    pub(super) fn refill(&mut self, now: Instant) {
        self.awaiting_metadata.refill(now);
        self.no_connected_peers.refill(now);
        self.routine_refresh.refill(now);
    }

    pub(super) fn try_consume(&mut self, class: DemandSliceClass, now: Instant) -> bool {
        self.bucket_mut(class).try_consume(now)
    }

    pub(super) fn refund(&mut self, class: DemandSliceClass) {
        self.bucket_mut(class).refund();
    }

    pub(super) fn available(&mut self, class: DemandSliceClass, now: Instant) -> usize {
        self.bucket_mut(class).refill(now);
        self.bucket_mut(class).available()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct DemandSlotCounts {
    pub(super) awaiting_metadata: usize,
    pub(super) no_connected_peers: usize,
    pub(super) routine_refresh: usize,
}

impl DemandSlotCounts {
    pub(super) fn count(self, class: DemandSliceClass) -> usize {
        match class {
            DemandSliceClass::AwaitingMetadata => self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => self.routine_refresh,
        }
    }

    pub(super) fn total(self) -> usize {
        self.awaiting_metadata
            .saturating_add(self.no_connected_peers)
            .saturating_add(self.routine_refresh)
    }

    pub(super) fn record(&mut self, class: DemandSliceClass) {
        match class {
            DemandSliceClass::AwaitingMetadata => {
                self.awaiting_metadata = self.awaiting_metadata.saturating_add(1);
            }
            DemandSliceClass::NoConnectedPeers => {
                self.no_connected_peers = self.no_connected_peers.saturating_add(1);
            }
            DemandSliceClass::RoutineRefresh => {
                self.routine_refresh = self.routine_refresh.saturating_add(1);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct DemandPlannerSelectionStats {
    pub(super) offered: DemandSlotCounts,
    pub(super) launched: DemandSlotCounts,
    pub(super) throttled: DemandSlotCounts,
    pub(super) oldest_throttled_awaiting_ms: u64,
    pub(super) oldest_throttled_no_peers_ms: u64,
    pub(super) oldest_throttled_routine_ms: u64,
}

impl DemandPlannerSelectionStats {
    pub(super) fn record_throttled_age(&mut self, class: DemandSliceClass, age_ms: u64) {
        match class {
            DemandSliceClass::AwaitingMetadata => {
                self.oldest_throttled_awaiting_ms = self.oldest_throttled_awaiting_ms.max(age_ms);
            }
            DemandSliceClass::NoConnectedPeers => {
                self.oldest_throttled_no_peers_ms = self.oldest_throttled_no_peers_ms.max(age_ms);
            }
            DemandSliceClass::RoutineRefresh => {
                self.oldest_throttled_routine_ms = self.oldest_throttled_routine_ms.max(age_ms);
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct DemandPlannerSelection {
    pub(super) launches: Vec<DueDemandCandidate>,
    pub(super) stats: DemandPlannerSelectionStats,
}

#[derive(Debug)]
pub(super) enum DemandPlannerAction<'a> {
    RuntimeReset {
        now: Instant,
    },
    DemandRegistered {
        info_hash: InfoHash,
        demand: DhtDemandState,
        now: Instant,
    },
    DemandUpdated {
        info_hash: InfoHash,
        demand: DhtDemandState,
        now: Instant,
    },
    DemandSubscriberRemoved {
        info_hash: InfoHash,
    },
    PeersReceived {
        info_hash: InfoHash,
        peers: &'a [SocketAddr],
    },
    DrainTick {
        now: Instant,
        runtime_ready: HashMap<InfoHash, bool>,
    },
    PlanDue {
        now: Instant,
        runtime_available: bool,
    },
    LookupStarted {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    },
    LookupStartFailed {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        now: Instant,
    },
    LookupFinished {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        total_peers: usize,
        unique_peers: usize,
        now: Instant,
    },
    LookupParkRequested {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: HashSet<SocketAddr>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    },
    LookupParkResolved {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: usize,
        parked_outcome: Option<DemandParkedSliceOutcome>,
        drain_admission: Option<DemandDrainAdmissionSnapshot>,
        previous: Option<DemandEntrySnapshot>,
        now: Instant,
    },
    DrainedLookupFinalized {
        info_hash: InfoHash,
        outcome: DrainedDemandOutcome,
        previous: Option<DemandEntrySnapshot>,
        now: Instant,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandStartLookupEffect {
    pub(super) candidate: DueDemandCandidate,
    pub(super) plan: DemandLookupPlan,
    pub(super) selection_reason: DemandSelectionReason,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandLookupFinishedEffect {
    pub(super) info_hash: InfoHash,
    pub(super) slice_class: DemandSliceClass,
    pub(super) total_peers: usize,
    pub(super) unique_peers: usize,
    pub(super) previous: Option<DemandEntrySnapshot>,
    pub(super) current: Option<DemandEntrySnapshot>,
    pub(super) finished_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DemandDrainAdmissionSnapshot {
    pub(super) initial_inflight_queries: usize,
    pub(super) score: i32,
    pub(super) deadline_ms: u64,
}

#[derive(Debug, Clone)]
pub(super) struct DemandAdmitDrainEffect {
    pub(super) info_hash: InfoHash,
    pub(super) slice_class: DemandSliceClass,
    pub(super) stop_reason: DemandSliceStopReason,
    pub(super) total_peers: usize,
    pub(super) unique_peers: HashSet<SocketAddr>,
    pub(super) lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    pub(super) previous: Option<DemandEntrySnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandLookupParkedEffect {
    pub(super) info_hash: InfoHash,
    pub(super) slice_class: DemandSliceClass,
    pub(super) stop_reason: DemandSliceStopReason,
    pub(super) total_peers: usize,
    pub(super) unique_peers: usize,
    pub(super) parked_outcome: Option<DemandParkedSliceOutcome>,
    pub(super) drain_admission: Option<DemandDrainAdmissionSnapshot>,
    pub(super) previous: Option<DemandEntrySnapshot>,
    pub(super) current: Option<DemandEntrySnapshot>,
    pub(super) parked_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandDrainFinalizedEffect {
    pub(super) info_hash: InfoHash,
    pub(super) outcome: DrainedDemandOutcome,
    pub(super) finish_mode: DemandFinishMode,
    pub(super) previous: Option<DemandEntrySnapshot>,
    pub(super) current: Option<DemandEntrySnapshot>,
    pub(super) finalized_at: Instant,
    pub(super) parked: bool,
}

#[derive(Debug, Clone)]
pub(super) struct DemandParkActiveLookupEffect {
    pub(super) info_hash: InfoHash,
    pub(super) slice_class: DemandSliceClass,
    pub(super) lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
}

#[derive(Debug, Clone)]
pub(super) struct DemandCancelDrainingLookupEffect {
    pub(super) info_hash: InfoHash,
    pub(super) lookup_ids: Vec<LookupId>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandFinalizeDrainingLookupEffect {
    pub(super) info_hash: InfoHash,
    pub(super) force: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandDrainPeersRecordedEffect {
    pub(super) info_hash: InfoHash,
    pub(super) peer_count: usize,
    pub(super) unique_added: usize,
    pub(super) initial_unique_peers: usize,
}

#[derive(Debug, Clone)]
pub(super) enum DemandPlannerEffect {
    StartLookup(DemandStartLookupEffect),
    LookupFinished(DemandLookupFinishedEffect),
    AdmitDrain(DemandAdmitDrainEffect),
    LookupParked(DemandLookupParkedEffect),
    DrainFinalized(DemandDrainFinalizedEffect),
    ParkActiveLookup(DemandParkActiveLookupEffect),
    CancelDrainingLookup(DemandCancelDrainingLookupEffect),
    FinalizeDrainingLookup(DemandFinalizeDrainingLookupEffect),
    DrainPeersRecorded(DemandDrainPeersRecordedEffect),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DemandPlannerPlanStats {
    pub(super) launch_budget: usize,
    pub(super) due_total: usize,
    pub(super) selection_stats: DemandPlannerSelectionStats,
    pub(super) spare_selected: usize,
    pub(super) active_counts: DemandSlotCounts,
    pub(super) parked_count: usize,
    pub(super) draining_count: usize,
    pub(super) drain_virtual_slots: usize,
    pub(super) budget_awaiting: usize,
    pub(super) budget_no_peers: usize,
    pub(super) budget_routine: usize,
}

#[derive(Debug, Default)]
pub(super) struct DemandPlannerReduction {
    pub(super) effects: Vec<DemandPlannerEffect>,
    pub(super) plan_stats: Option<DemandPlannerPlanStats>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DemandPlannerActionView {
    pub(super) kind: &'static str,
    pub(super) info_hash: Option<InfoHash>,
    pub(super) demand_class: Option<DemandSliceClass>,
    pub(super) slice_class: Option<DemandSliceClass>,
    pub(super) peer_count: Option<usize>,
    pub(super) total_peers: Option<usize>,
    pub(super) unique_peers: Option<usize>,
    pub(super) runtime_available: Option<bool>,
    pub(super) runtime_ready_count: Option<usize>,
    pub(super) stop_reason: Option<DemandSliceStopReason>,
}

impl DemandPlannerActionView {
    pub(super) fn from_action(action: &DemandPlannerAction<'_>) -> Self {
        match action {
            DemandPlannerAction::RuntimeReset { .. } => Self {
                kind: "runtime_reset",
                ..Self::default()
            },
            DemandPlannerAction::DemandRegistered {
                info_hash, demand, ..
            } => Self {
                kind: "demand_registered",
                info_hash: Some(*info_hash),
                demand_class: Some(DemandSliceClass::from_demand(*demand)),
                ..Self::default()
            },
            DemandPlannerAction::DemandUpdated {
                info_hash, demand, ..
            } => Self {
                kind: "demand_updated",
                info_hash: Some(*info_hash),
                demand_class: Some(DemandSliceClass::from_demand(*demand)),
                ..Self::default()
            },
            DemandPlannerAction::DemandSubscriberRemoved { info_hash } => Self {
                kind: "demand_subscriber_removed",
                info_hash: Some(*info_hash),
                ..Self::default()
            },
            DemandPlannerAction::PeersReceived { info_hash, peers } => Self {
                kind: "peers_received",
                info_hash: Some(*info_hash),
                peer_count: Some(peers.len()),
                ..Self::default()
            },
            DemandPlannerAction::DrainTick { runtime_ready, .. } => Self {
                kind: "drain_tick",
                runtime_ready_count: Some(runtime_ready.values().filter(|ready| **ready).count()),
                ..Self::default()
            },
            DemandPlannerAction::PlanDue {
                runtime_available, ..
            } => Self {
                kind: "plan_due",
                runtime_available: Some(*runtime_available),
                ..Self::default()
            },
            DemandPlannerAction::LookupStarted {
                info_hash,
                slice_class,
                ..
            } => Self {
                kind: "lookup_started",
                info_hash: Some(*info_hash),
                slice_class: Some(*slice_class),
                ..Self::default()
            },
            DemandPlannerAction::LookupStartFailed {
                info_hash,
                slice_class,
                ..
            } => Self {
                kind: "lookup_start_failed",
                info_hash: Some(*info_hash),
                slice_class: Some(*slice_class),
                ..Self::default()
            },
            DemandPlannerAction::LookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
                ..
            } => Self {
                kind: "lookup_finished",
                info_hash: Some(*info_hash),
                slice_class: Some(*slice_class),
                total_peers: Some(*total_peers),
                unique_peers: Some(*unique_peers),
                ..Self::default()
            },
            DemandPlannerAction::LookupParkRequested {
                info_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers,
                ..
            } => Self {
                kind: "lookup_park_requested",
                info_hash: Some(*info_hash),
                slice_class: Some(*slice_class),
                total_peers: Some(*total_peers),
                unique_peers: Some(unique_peers.len()),
                stop_reason: Some(*stop_reason),
                ..Self::default()
            },
            DemandPlannerAction::LookupParkResolved {
                info_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers,
                ..
            } => Self {
                kind: "lookup_park_resolved",
                info_hash: Some(*info_hash),
                slice_class: Some(*slice_class),
                total_peers: Some(*total_peers),
                unique_peers: Some(*unique_peers),
                stop_reason: Some(*stop_reason),
                ..Self::default()
            },
            DemandPlannerAction::DrainedLookupFinalized {
                info_hash, outcome, ..
            } => Self {
                kind: "drained_lookup_finalized",
                info_hash: Some(*info_hash),
                slice_class: Some(outcome.slice_class),
                total_peers: Some(outcome.total_peers),
                unique_peers: Some(outcome.unique_peers),
                stop_reason: Some(outcome.stop_reason),
                ..Self::default()
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DemandPlannerEffectView {
    pub(super) kind: &'static str,
    pub(super) info_hash: Option<InfoHash>,
    pub(super) demand_class: Option<DemandSliceClass>,
    pub(super) slice_class: Option<DemandSliceClass>,
    pub(super) selection_reason: Option<DemandSelectionReason>,
    pub(super) stop_reason: Option<DemandSliceStopReason>,
    pub(super) total_peers: Option<usize>,
    pub(super) unique_peers: Option<usize>,
    pub(super) peer_count: Option<usize>,
    pub(super) lookup_count: Option<usize>,
    pub(super) unique_added: Option<usize>,
    pub(super) force: Option<bool>,
    pub(super) parked: Option<bool>,
    pub(super) finish_mode: Option<DemandFinishMode>,
}

impl DemandPlannerEffectView {
    pub(super) fn from_effect(effect: &DemandPlannerEffect) -> Self {
        match effect {
            DemandPlannerEffect::StartLookup(start) => Self {
                kind: "start_lookup",
                info_hash: Some(start.candidate.info_hash),
                demand_class: Some(DemandSliceClass::from_demand(start.candidate.demand)),
                slice_class: Some(start.plan.class),
                selection_reason: Some(start.selection_reason),
                ..Self::default()
            },
            DemandPlannerEffect::LookupFinished(finished) => Self {
                kind: "lookup_finished",
                info_hash: Some(finished.info_hash),
                slice_class: Some(finished.slice_class),
                total_peers: Some(finished.total_peers),
                unique_peers: Some(finished.unique_peers),
                ..Self::default()
            },
            DemandPlannerEffect::AdmitDrain(admit) => Self {
                kind: "admit_drain",
                info_hash: Some(admit.info_hash),
                slice_class: Some(admit.slice_class),
                stop_reason: Some(admit.stop_reason),
                total_peers: Some(admit.total_peers),
                unique_peers: Some(admit.unique_peers.len()),
                ..Self::default()
            },
            DemandPlannerEffect::LookupParked(parked) => Self {
                kind: "lookup_parked",
                info_hash: Some(parked.info_hash),
                slice_class: Some(parked.slice_class),
                stop_reason: Some(parked.stop_reason),
                total_peers: Some(parked.total_peers),
                unique_peers: Some(parked.unique_peers),
                parked: Some(parked.drain_admission.is_some()),
                ..Self::default()
            },
            DemandPlannerEffect::DrainFinalized(finalized) => Self {
                kind: "drain_finalized",
                info_hash: Some(finalized.info_hash),
                slice_class: Some(finalized.outcome.slice_class),
                stop_reason: Some(finalized.outcome.stop_reason),
                total_peers: Some(finalized.outcome.total_peers),
                unique_peers: Some(finalized.outcome.unique_peers),
                parked: Some(finalized.parked),
                finish_mode: Some(finalized.finish_mode),
                ..Self::default()
            },
            DemandPlannerEffect::ParkActiveLookup(park) => Self {
                kind: "park_active_lookup",
                info_hash: Some(park.info_hash),
                slice_class: Some(park.slice_class),
                ..Self::default()
            },
            DemandPlannerEffect::CancelDrainingLookup(cancel) => Self {
                kind: "cancel_draining_lookup",
                info_hash: Some(cancel.info_hash),
                lookup_count: Some(cancel.lookup_ids.len()),
                ..Self::default()
            },
            DemandPlannerEffect::FinalizeDrainingLookup(finalize) => Self {
                kind: "finalize_draining_lookup",
                info_hash: Some(finalize.info_hash),
                force: Some(finalize.force),
                ..Self::default()
            },
            DemandPlannerEffect::DrainPeersRecorded(recorded) => Self {
                kind: "drain_peers_recorded",
                info_hash: Some(recorded.info_hash),
                peer_count: Some(recorded.peer_count),
                unique_added: Some(recorded.unique_added),
                unique_peers: Some(recorded.initial_unique_peers + recorded.unique_added),
                ..Self::default()
            },
        }
    }
}

pub(super) fn demand_planner_monitor_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| env::var_os(DHT_PLANNER_MONITOR_ENV).is_some())
}

pub(super) fn short_info_hash(info_hash: InfoHash) -> String {
    hex::encode(&info_hash.as_ref()[..4])
}

pub(super) fn optional_info_hash_label(info_hash: Option<InfoHash>) -> String {
    info_hash.map(short_info_hash).unwrap_or_default()
}

pub(super) fn trace_demand_planner_reduction(
    action: DemandPlannerActionView,
    reduction: &DemandPlannerReduction,
    model: &DemandPlannerModel,
) {
    if !demand_planner_monitor_enabled() {
        return;
    }

    let effect_names = reduction
        .effects
        .iter()
        .map(|effect| DemandPlannerEffectView::from_effect(effect).kind)
        .collect::<Vec<_>>()
        .join(",");
    let plan = reduction.plan_stats;
    tracing::info!(
        target: "superseedr::dht_planner",
        event = "reduce",
        action = action.kind,
        info_hash = %optional_info_hash_label(action.info_hash),
        demand_class = ?action.demand_class,
        slice_class = ?action.slice_class,
        peer_count = ?action.peer_count,
        total_peers = ?action.total_peers,
        unique_peers = ?action.unique_peers,
        runtime_available = ?action.runtime_available,
        runtime_ready_count = ?action.runtime_ready_count,
        stop_reason = ?action.stop_reason,
        effect_count = reduction.effects.len(),
        effects = %effect_names,
        plan_launch_budget = ?plan.map(|plan| plan.launch_budget),
        plan_due_total = ?plan.map(|plan| plan.due_total),
        plan_spare_selected = ?plan.map(|plan| plan.spare_selected),
        plan_parked_count = ?plan.map(|plan| plan.parked_count),
        plan_draining_count = ?plan.map(|plan| plan.draining_count),
        plan_drain_virtual_slots = ?plan.map(|plan| plan.drain_virtual_slots),
        plan_budget_awaiting = ?plan.map(|plan| plan.budget_awaiting),
        plan_budget_no_peers = ?plan.map(|plan| plan.budget_no_peers),
        plan_budget_routine = ?plan.map(|plan| plan.budget_routine),
        plan_offered_awaiting = ?plan.map(|plan| plan.selection_stats.offered.awaiting_metadata),
        plan_offered_no_peers = ?plan.map(|plan| plan.selection_stats.offered.no_connected_peers),
        plan_offered_routine = ?plan.map(|plan| plan.selection_stats.offered.routine_refresh),
        plan_launched_awaiting = ?plan.map(|plan| plan.selection_stats.launched.awaiting_metadata),
        plan_launched_no_peers = ?plan.map(|plan| plan.selection_stats.launched.no_connected_peers),
        plan_launched_routine = ?plan.map(|plan| plan.selection_stats.launched.routine_refresh),
        planner_active = model.active.len(),
        planner_draining = model.draining_demands.len(),
        planner_parked = model.parked_crawls.len(),
        planner_scheduler_entries = model.scheduler.entry_snapshots().len(),
        "DHT planner action reduced",
    );

    for effect in &reduction.effects {
        trace_demand_planner_effect("emit", effect);
    }
}

pub(super) fn trace_demand_planner_effect(stage: &'static str, effect: &DemandPlannerEffect) {
    if !demand_planner_monitor_enabled() {
        return;
    }

    let view = DemandPlannerEffectView::from_effect(effect);
    tracing::info!(
        target: "superseedr::dht_planner",
        event = "effect",
        stage,
        effect = view.kind,
        info_hash = %optional_info_hash_label(view.info_hash),
        demand_class = ?view.demand_class,
        slice_class = ?view.slice_class,
        selection_reason = ?view.selection_reason,
        stop_reason = ?view.stop_reason,
        total_peers = ?view.total_peers,
        unique_peers = ?view.unique_peers,
        peer_count = ?view.peer_count,
        lookup_count = ?view.lookup_count,
        unique_added = ?view.unique_added,
        force = ?view.force,
        parked = ?view.parked,
        finish_mode = ?view.finish_mode,
        "DHT planner effect observed",
    );
}

#[derive(Debug)]
pub(super) struct DemandPlannerModel {
    pub(super) scheduler: DemandScheduler,
    pub(super) active: HashMap<InfoHash, ActiveDemandLookup>,
    pub(super) parked_crawls: HashMap<InfoHash, DemandCrawlState>,
    pub(super) draining_demands: HashMap<InfoHash, DrainingDemandLookup>,
    pub(super) state: HashMap<InfoHash, DemandPlannerState>,
    pub(super) budget: DemandPlannerBudget,
}

impl DemandPlannerModel {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            scheduler: DemandScheduler::new(
                DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL,
                DHT_NO_CONNECTED_PEERS_BASE_INTERVAL,
                DHT_NO_CONNECTED_PEERS_MAX_INTERVAL,
                DHT_AWAITING_METADATA_REFRESH_INTERVAL,
            ),
            active: HashMap::new(),
            parked_crawls: HashMap::new(),
            draining_demands: HashMap::new(),
            state: HashMap::new(),
            budget: DemandPlannerBudget::new(now),
        }
    }

    pub(super) fn has_draining_demands(&self) -> bool {
        !self.draining_demands.is_empty()
    }

    pub(super) fn metadata_waiter_count(&self) -> usize {
        self.scheduler
            .entry_snapshots()
            .into_iter()
            .filter(|snapshot| snapshot.demand.awaiting_metadata && snapshot.subscriber_count > 0)
            .count()
    }

    pub(super) fn entry_snapshot(&self, info_hash: InfoHash) -> Option<DemandEntrySnapshot> {
        self.scheduler.entry_snapshot(info_hash)
    }
}

#[derive(Debug)]
pub(super) struct DemandCrawlState {
    pub(super) ipv4: Option<LookupState>,
    pub(super) ipv6: Option<LookupState>,
    pub(super) class: DemandSliceClass,
    pub(super) updated_at: Instant,
    pub(super) reset_count: u32,
    pub(super) consecutive_stalled_low_yield_slices: u32,
    pub(super) consecutive_healthy_zero_yield_slices: u32,
}

impl DemandCrawlState {
    pub(super) fn new(now: Instant, class: DemandSliceClass) -> Self {
        Self {
            ipv4: None,
            ipv6: None,
            class,
            updated_at: now,
            reset_count: 0,
            consecutive_stalled_low_yield_slices: 0,
            consecutive_healthy_zero_yield_slices: 0,
        }
    }

    pub(super) fn take_family_state(&mut self, family: AddressFamily) -> Option<LookupState> {
        let state = match family {
            AddressFamily::Ipv4 => self.ipv4.take(),
            AddressFamily::Ipv6 => self.ipv6.take(),
        };
        if state.is_some() {
            self.updated_at = Instant::now();
        }
        state
    }

    pub(super) fn store_family_state(&mut self, class: DemandSliceClass, state: LookupState) {
        match state.family() {
            AddressFamily::Ipv4 => self.ipv4 = Some(state),
            AddressFamily::Ipv6 => self.ipv6 = Some(state),
        }
        self.class = class;
        self.updated_at = Instant::now();
    }

    pub(super) fn is_empty(&self) -> bool {
        self.ipv4.is_none() && self.ipv6.is_none()
    }

    pub(super) fn is_stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.updated_at) >= DHT_PARKED_CRAWL_MAX_AGE
    }

    pub(super) fn reset_reason_for(
        &self,
        class: DemandSliceClass,
        now: Instant,
    ) -> Option<DemandCrawlResetReason> {
        if self.is_stale(now) {
            Some(DemandCrawlResetReason::Stale)
        } else if self.class == class
            && self.consecutive_stalled_low_yield_slices
                >= class.stalled_empty_slice_reset_threshold()
        {
            Some(DemandCrawlResetReason::LowQuality)
        } else {
            None
        }
    }

    pub(super) fn should_reset_for(&self, class: DemandSliceClass, now: Instant) -> bool {
        self.reset_reason_for(class, now).is_some()
    }

    pub(super) fn reset_for(&mut self, class: DemandSliceClass, now: Instant) {
        self.ipv4 = None;
        self.ipv6 = None;
        self.class = class;
        self.updated_at = now;
        self.reset_count = self.reset_count.saturating_add(1);
        self.consecutive_stalled_low_yield_slices = 0;
        self.consecutive_healthy_zero_yield_slices = 0;
    }

    pub(super) fn observe_parked_slice(
        &mut self,
        class: DemandSliceClass,
        outcome: DemandParkedSliceOutcome,
    ) {
        if self.class != class {
            self.class = class;
            self.consecutive_stalled_low_yield_slices = 0;
            self.consecutive_healthy_zero_yield_slices = 0;
        }
        self.class = class;
        self.updated_at = Instant::now();
        match outcome {
            DemandParkedSliceOutcome::WeakLowYield => {
                self.consecutive_stalled_low_yield_slices =
                    self.consecutive_stalled_low_yield_slices.saturating_add(1);
                self.consecutive_healthy_zero_yield_slices = 0;
            }
            DemandParkedSliceOutcome::HealthyZeroYield => {
                self.consecutive_stalled_low_yield_slices = 0;
                self.consecutive_healthy_zero_yield_slices =
                    self.consecutive_healthy_zero_yield_slices.saturating_add(1);
            }
            DemandParkedSliceOutcome::HealthyLowYield
            | DemandParkedSliceOutcome::UsefulYield
            | DemandParkedSliceOutcome::Ignored => {
                self.consecutive_stalled_low_yield_slices = 0;
                self.consecutive_healthy_zero_yield_slices = 0;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemandSliceClass {
    RoutineRefresh,
    NoConnectedPeers,
    AwaitingMetadata,
}

impl DemandSliceClass {
    pub(super) fn from_demand(demand: DhtDemandState) -> Self {
        if demand.awaiting_metadata {
            Self::AwaitingMetadata
        } else if demand.connected_peers == 0 {
            Self::NoConnectedPeers
        } else {
            Self::RoutineRefresh
        }
    }

    pub(super) fn stalled_empty_slice_reset_threshold(self) -> u32 {
        match self {
            DemandSliceClass::AwaitingMetadata => {
                DHT_AWAITING_METADATA_STALLED_EMPTY_SLICE_RESET_THRESHOLD
            }
            DemandSliceClass::NoConnectedPeers => {
                DHT_NO_CONNECTED_PEERS_STALLED_EMPTY_SLICE_RESET_THRESHOLD
            }
            DemandSliceClass::RoutineRefresh => DHT_ROUTINE_STALLED_EMPTY_SLICE_RESET_THRESHOLD,
        }
    }

    pub(super) fn stalled_low_yield_slice_max_unique_peers(self) -> usize {
        match self {
            DemandSliceClass::AwaitingMetadata => {
                DHT_AWAITING_METADATA_STALLED_LOW_YIELD_SLICE_MAX_UNIQUE_PEERS
            }
            DemandSliceClass::NoConnectedPeers => {
                DHT_NO_CONNECTED_PEERS_STALLED_LOW_YIELD_SLICE_MAX_UNIQUE_PEERS
            }
            DemandSliceClass::RoutineRefresh => {
                DHT_ROUTINE_STALLED_LOW_YIELD_SLICE_MAX_UNIQUE_PEERS
            }
        }
    }

    pub(super) fn parked_slice_outcome(
        self,
        stop_reason: DemandSliceStopReason,
        unique_peers: usize,
        weak_parked_state: bool,
    ) -> DemandParkedSliceOutcome {
        if !matches!(
            stop_reason,
            DemandSliceStopReason::WallTime | DemandSliceStopReason::IdleTimeout
        ) {
            return if unique_peers > 0 {
                DemandParkedSliceOutcome::UsefulYield
            } else {
                DemandParkedSliceOutcome::Ignored
            };
        }

        if unique_peers > self.stalled_low_yield_slice_max_unique_peers() {
            DemandParkedSliceOutcome::UsefulYield
        } else if weak_parked_state {
            DemandParkedSliceOutcome::WeakLowYield
        } else if unique_peers == 0 {
            DemandParkedSliceOutcome::HealthyZeroYield
        } else {
            DemandParkedSliceOutcome::HealthyLowYield
        }
    }

    pub(super) fn parked_quality_is_weak(self, snapshot: AggregateLookupQualitySnapshot) -> bool {
        match self {
            DemandSliceClass::AwaitingMetadata => false,
            DemandSliceClass::NoConnectedPeers => {
                snapshot.visited_len >= DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MIN_VISITED
                    && snapshot.eligible_responder_count
                        <= DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MAX_RESPONDERS
                    && snapshot.frontier_len <= DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MAX_FRONTIER
                    && snapshot.received_peer_count
                        <= DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MAX_RECEIVED_PEERS
            }
            DemandSliceClass::RoutineRefresh => {
                snapshot.visited_len >= DHT_ROUTINE_WEAK_PARKED_MIN_VISITED
                    && snapshot.eligible_responder_count <= DHT_ROUTINE_WEAK_PARKED_MAX_RESPONDERS
                    && snapshot.frontier_len <= DHT_ROUTINE_WEAK_PARKED_MAX_FRONTIER
                    && snapshot.received_peer_count <= DHT_ROUTINE_WEAK_PARKED_MAX_RECEIVED_PEERS
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct AggregateLookupQualitySnapshot {
    pub(super) frontier_len: usize,
    pub(super) inflight_len: usize,
    pub(super) visited_len: usize,
    pub(super) eligible_responder_count: usize,
    pub(super) received_peer_count: usize,
}

impl AggregateLookupQualitySnapshot {
    pub(super) fn extend(&mut self, snapshot: LookupQualitySnapshot) {
        self.frontier_len = self.frontier_len.saturating_add(snapshot.frontier_len);
        self.inflight_len = self.inflight_len.saturating_add(snapshot.inflight_len);
        self.visited_len = self.visited_len.saturating_add(snapshot.visited_len);
        self.eligible_responder_count = self
            .eligible_responder_count
            .saturating_add(snapshot.eligible_responder_count);
        self.received_peer_count = self
            .received_peer_count
            .saturating_add(snapshot.received_peer_count);
    }
}

pub(super) fn aggregate_parked_crawl_quality(
    crawl: &DemandCrawlState,
) -> AggregateLookupQualitySnapshot {
    let mut aggregate = AggregateLookupQualitySnapshot::default();
    if let Some(state) = crawl.ipv4.as_ref() {
        aggregate.extend(state.quality_snapshot());
    }
    if let Some(state) = crawl.ipv6.as_ref() {
        aggregate.extend(state.quality_snapshot());
    }
    aggregate
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemandSliceStopReason {
    NaturalFinish,
    WallTime,
    IdleTimeout,
    FirstBatch,
    UniquePeerCap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemandParkedSliceOutcome {
    UsefulYield,
    WeakLowYield,
    HealthyZeroYield,
    HealthyLowYield,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemandCrawlResetReason {
    Stale,
    ClassChanged,
    LowQuality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemandSelectionReason {
    ReusableParked,
    UsefulYieldHistory,
    OverdueScarce,
    SpareCapacity,
}

#[derive(Debug, Clone, Default)]
pub(super) struct DemandSliceClassMetrics {
    pub(super) fresh_starts: u64,
    pub(super) resumed_starts: u64,
    pub(super) selected_reusable_parked: u64,
    pub(super) selected_useful_yield_history: u64,
    pub(super) selected_overdue_scarce: u64,
    pub(super) selected_spare_capacity: u64,
    pub(super) natural_finishes: u64,
    pub(super) wall_time_stops: u64,
    pub(super) idle_timeout_stops: u64,
    pub(super) first_batch_stops: u64,
    pub(super) unique_peer_cap_stops: u64,
    pub(super) peers_yielded: u64,
    pub(super) unique_peers_yielded: u64,
    pub(super) stale_resets: u64,
    pub(super) class_change_resets: u64,
    pub(super) low_quality_resets: u64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct DemandSliceMetrics {
    pub(super) awaiting_metadata: DemandSliceClassMetrics,
    pub(super) no_connected_peers: DemandSliceClassMetrics,
    pub(super) routine_refresh: DemandSliceClassMetrics,
}

impl DemandSliceMetrics {
    pub(super) fn class_mut(&mut self, class: DemandSliceClass) -> &mut DemandSliceClassMetrics {
        match class {
            DemandSliceClass::AwaitingMetadata => &mut self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &mut self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &mut self.routine_refresh,
        }
    }

    pub(super) fn class_ref(&self, class: DemandSliceClass) -> &DemandSliceClassMetrics {
        match class {
            DemandSliceClass::AwaitingMetadata => &self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &self.routine_refresh,
        }
    }

    pub(super) fn record_start(&mut self, class: DemandSliceClass, resumed: bool) {
        let metrics = self.class_mut(class);
        if resumed {
            metrics.resumed_starts = metrics.resumed_starts.saturating_add(1);
        } else {
            metrics.fresh_starts = metrics.fresh_starts.saturating_add(1);
        }
    }

    pub(super) fn record_selection(
        &mut self,
        class: DemandSliceClass,
        reason: DemandSelectionReason,
    ) {
        let metrics = self.class_mut(class);
        match reason {
            DemandSelectionReason::ReusableParked => {
                metrics.selected_reusable_parked =
                    metrics.selected_reusable_parked.saturating_add(1)
            }
            DemandSelectionReason::UsefulYieldHistory => {
                metrics.selected_useful_yield_history =
                    metrics.selected_useful_yield_history.saturating_add(1)
            }
            DemandSelectionReason::OverdueScarce => {
                metrics.selected_overdue_scarce = metrics.selected_overdue_scarce.saturating_add(1)
            }
            DemandSelectionReason::SpareCapacity => {
                metrics.selected_spare_capacity = metrics.selected_spare_capacity.saturating_add(1)
            }
        }
    }

    pub(super) fn record_stop(
        &mut self,
        class: DemandSliceClass,
        reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: usize,
    ) {
        let metrics = self.class_mut(class);
        match reason {
            DemandSliceStopReason::NaturalFinish => {
                metrics.natural_finishes = metrics.natural_finishes.saturating_add(1)
            }
            DemandSliceStopReason::WallTime => {
                metrics.wall_time_stops = metrics.wall_time_stops.saturating_add(1)
            }
            DemandSliceStopReason::IdleTimeout => {
                metrics.idle_timeout_stops = metrics.idle_timeout_stops.saturating_add(1)
            }
            DemandSliceStopReason::FirstBatch => {
                metrics.first_batch_stops = metrics.first_batch_stops.saturating_add(1)
            }
            DemandSliceStopReason::UniquePeerCap => {
                metrics.unique_peer_cap_stops = metrics.unique_peer_cap_stops.saturating_add(1)
            }
        }
        metrics.peers_yielded = metrics.peers_yielded.saturating_add(total_peers as u64);
        metrics.unique_peers_yielded = metrics
            .unique_peers_yielded
            .saturating_add(unique_peers as u64);
    }

    pub(super) fn record_reset(&mut self, class: DemandSliceClass, reason: DemandCrawlResetReason) {
        let metrics = self.class_mut(class);
        match reason {
            DemandCrawlResetReason::Stale => {
                metrics.stale_resets = metrics.stale_resets.saturating_add(1)
            }
            DemandCrawlResetReason::ClassChanged => {
                metrics.class_change_resets = metrics.class_change_resets.saturating_add(1)
            }
            DemandCrawlResetReason::LowQuality => {
                metrics.low_quality_resets = metrics.low_quality_resets.saturating_add(1)
            }
        }
    }

    pub(super) fn has_activity(&self) -> bool {
        for class in [
            DemandSliceClass::AwaitingMetadata,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceClass::RoutineRefresh,
        ] {
            let metrics = self.class_ref(class);
            if metrics.fresh_starts > 0
                || metrics.resumed_starts > 0
                || metrics.selected_reusable_parked > 0
                || metrics.selected_useful_yield_history > 0
                || metrics.selected_overdue_scarce > 0
                || metrics.selected_spare_capacity > 0
                || metrics.natural_finishes > 0
                || metrics.wall_time_stops > 0
                || metrics.idle_timeout_stops > 0
                || metrics.first_batch_stops > 0
                || metrics.unique_peer_cap_stops > 0
                || metrics.peers_yielded > 0
                || metrics.unique_peers_yielded > 0
                || metrics.stale_resets > 0
                || metrics.class_change_resets > 0
                || metrics.low_quality_resets > 0
            {
                return true;
            }
        }
        false
    }

    pub(super) fn summary(&self) -> String {
        fn fmt(label: &str, metrics: &DemandSliceClassMetrics) -> String {
            format!(
                "{label}(fresh={} resumed={} sel_reuse={} sel_yield={} sel_due={} sel_spare={} natural={} wall={} idle={} first={} cap={} peers={} unique={} reset_stale={} reset_class={} reset_quality={})",
                metrics.fresh_starts,
                metrics.resumed_starts,
                metrics.selected_reusable_parked,
                metrics.selected_useful_yield_history,
                metrics.selected_overdue_scarce,
                metrics.selected_spare_capacity,
                metrics.natural_finishes,
                metrics.wall_time_stops,
                metrics.idle_timeout_stops,
                metrics.first_batch_stops,
                metrics.unique_peer_cap_stops,
                metrics.peers_yielded,
                metrics.unique_peers_yielded,
                metrics.stale_resets,
                metrics.class_change_resets,
                metrics.low_quality_resets,
            )
        }

        [
            fmt("awaiting", &self.awaiting_metadata),
            fmt("no_peers", &self.no_connected_peers),
            fmt("routine", &self.routine_refresh),
        ]
        .join(" ")
    }
}

pub(super) fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DemandLookupPlan {
    pub(super) class: DemandSliceClass,
    pub(super) idle_timeout: Duration,
    pub(super) max_wall_time: Duration,
    pub(super) stop_after_first_batch: bool,
    pub(super) unique_peer_cap: usize,
}

impl DemandLookupPlan {
    pub(super) fn for_demand(demand: DhtDemandState) -> Self {
        match DemandSliceClass::from_demand(demand) {
            DemandSliceClass::AwaitingMetadata => Self {
                class: DemandSliceClass::AwaitingMetadata,
                idle_timeout: DHT_AWAITING_METADATA_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_AWAITING_METADATA_SLICE_WALL_TIME,
                stop_after_first_batch: false,
                unique_peer_cap: DHT_AWAITING_METADATA_SLICE_UNIQUE_PEER_CAP,
            },
            DemandSliceClass::NoConnectedPeers => Self {
                class: DemandSliceClass::NoConnectedPeers,
                idle_timeout: DHT_NO_CONNECTED_PEERS_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_NO_CONNECTED_PEERS_SLICE_WALL_TIME,
                stop_after_first_batch: false,
                unique_peer_cap: DHT_NO_CONNECTED_PEERS_SLICE_UNIQUE_PEER_CAP,
            },
            DemandSliceClass::RoutineRefresh => Self {
                class: DemandSliceClass::RoutineRefresh,
                idle_timeout: DHT_ROUTINE_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_ROUTINE_SLICE_WALL_TIME,
                stop_after_first_batch: true,
                unique_peer_cap: DHT_ROUTINE_SLICE_UNIQUE_PEER_CAP,
            },
        }
    }
}

pub(super) fn take_parked_family_state(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    slice_metrics: Option<&mut DemandSliceMetrics>,
    info_hash: InfoHash,
    family: AddressFamily,
    slice_class: DemandSliceClass,
) -> Option<LookupState> {
    let now = Instant::now();
    let mut slice_metrics = slice_metrics;
    let mut remove_entry = false;
    let state = parked_crawls.get_mut(&info_hash).and_then(|crawl| {
        if let Some(reason) = crawl.reset_reason_for(slice_class, now) {
            if let Some(metrics) = slice_metrics.as_mut() {
                metrics.record_reset(crawl.class, reason);
            }
            crawl.reset_for(slice_class, now);
            remove_entry = true;
            None
        } else {
            let state = crawl.take_family_state(family);
            remove_entry = crawl.is_empty();
            state
        }
    });
    if remove_entry {
        parked_crawls.remove(&info_hash);
    }
    state
}

pub(super) fn store_parked_lookup_states(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    states: Vec<LookupState>,
) -> Option<DemandParkedSliceOutcome> {
    if states.is_empty() {
        return None;
    }

    let now = Instant::now();
    let quality = aggregate_lookup_quality(&states);
    let parked_outcome =
        parked_slice_outcome_for_quality(slice_class, stop_reason, unique_peers, quality);
    let crawl = parked_crawls
        .entry(info_hash)
        .or_insert_with(|| DemandCrawlState::new(now, slice_class));
    if let Some(outcome) = parked_outcome {
        crawl.observe_parked_slice(slice_class, outcome);
    }
    for state in states {
        crawl.store_family_state(slice_class, state);
    }
    parked_outcome
}

pub(super) fn parked_slice_outcome_for_quality(
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    quality: AggregateLookupQualitySnapshot,
) -> Option<DemandParkedSliceOutcome> {
    let weak_parked_state = slice_class.parked_quality_is_weak(quality);
    stop_reason
        .map(|reason| slice_class.parked_slice_outcome(reason, unique_peers, weak_parked_state))
}

pub(super) fn aggregate_lookup_quality(states: &[LookupState]) -> AggregateLookupQualitySnapshot {
    let mut aggregate = AggregateLookupQualitySnapshot::default();
    for state in states {
        aggregate.extend(state.quality_snapshot());
    }
    aggregate
}

pub(super) fn park_lookup_ids(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
) -> Option<DemandParkedSliceOutcome> {
    let lookup_ids = {
        let mut lookup_ids = lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return None;
        }
        std::mem::take(&mut *lookup_ids)
    };

    let Some(active_runtime) = active_runtime else {
        return None;
    };

    let mut parked_states = Vec::new();
    for lookup_id in lookup_ids {
        if let Some(state) = active_runtime
            .runtime
            .cancel_lookup_and_take_state(lookup_id)
        {
            parked_states.push(state);
        }
    }

    store_parked_lookup_states(
        parked_crawls,
        info_hash,
        slice_class,
        stop_reason,
        unique_peers,
        parked_states,
    )
}

pub(super) fn schedule_drained_demand_finalize(
    command_tx: &DhtCommandSender,
    info_hash: InfoHash,
    delay: Duration,
) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = send_dht_command(
            &command_tx,
            DhtCommand::FinalizeDrainedDemandLookups { info_hash },
        );
    });
}

pub(super) fn demand_drain_duration(
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    parked_outcome: Option<DemandParkedSliceOutcome>,
    unique_peers: usize,
) -> Option<Duration> {
    let mut duration = match (slice_class, parked_outcome) {
        (DemandSliceClass::AwaitingMetadata, Some(DemandParkedSliceOutcome::UsefulYield)) => {
            Duration::from_secs(5)
        }
        (
            DemandSliceClass::AwaitingMetadata,
            Some(
                DemandParkedSliceOutcome::WeakLowYield
                | DemandParkedSliceOutcome::HealthyZeroYield
                | DemandParkedSliceOutcome::HealthyLowYield,
            ),
        ) => Duration::from_secs(2),
        (DemandSliceClass::AwaitingMetadata, _) if unique_peers > 0 => Duration::from_secs(2),
        (DemandSliceClass::NoConnectedPeers, Some(DemandParkedSliceOutcome::UsefulYield)) => {
            Duration::from_secs(5)
        }
        (DemandSliceClass::NoConnectedPeers, Some(DemandParkedSliceOutcome::HealthyLowYield)) => {
            Duration::from_secs(2)
        }
        (
            DemandSliceClass::NoConnectedPeers,
            Some(
                DemandParkedSliceOutcome::WeakLowYield | DemandParkedSliceOutcome::HealthyZeroYield,
            ),
        ) => Duration::from_secs(1),
        (DemandSliceClass::RoutineRefresh, Some(DemandParkedSliceOutcome::UsefulYield)) => {
            Duration::from_secs(2)
        }
        (
            DemandSliceClass::RoutineRefresh,
            Some(
                DemandParkedSliceOutcome::WeakLowYield
                | DemandParkedSliceOutcome::HealthyZeroYield
                | DemandParkedSliceOutcome::HealthyLowYield,
            ),
        ) => Duration::from_secs(1),
        _ => Duration::ZERO,
    };

    if matches!(stop_reason, DemandSliceStopReason::UniquePeerCap) {
        duration = duration.min(Duration::from_secs(2));
    }
    if matches!(stop_reason, DemandSliceStopReason::FirstBatch) {
        duration = duration.min(Duration::from_secs(1));
    }
    if matches!(stop_reason, DemandSliceStopReason::IdleTimeout) && unique_peers == 0 {
        duration = duration.min(Duration::from_secs(1));
    }

    (duration > Duration::ZERO).then_some(duration)
}

pub(super) fn demand_drain_no_late_yield_grace(slice_class: DemandSliceClass) -> Duration {
    match slice_class {
        DemandSliceClass::AwaitingMetadata => DHT_AWAITING_METADATA_DRAIN_NO_LATE_YIELD_GRACE,
        DemandSliceClass::NoConnectedPeers => DHT_DEMAND_DRAIN_NO_LATE_YIELD_GRACE,
        DemandSliceClass::RoutineRefresh => DHT_ROUTINE_DRAIN_NO_LATE_YIELD_GRACE,
    }
}

pub(super) fn demand_drain_score(
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    parked_outcome: Option<DemandParkedSliceOutcome>,
    unique_peers: usize,
    inflight_queries: usize,
) -> i32 {
    let class_score = match slice_class {
        DemandSliceClass::AwaitingMetadata => 60,
        DemandSliceClass::NoConnectedPeers => 30,
        DemandSliceClass::RoutineRefresh => 5,
    };
    let outcome_score = match parked_outcome {
        Some(DemandParkedSliceOutcome::UsefulYield) => 60,
        Some(DemandParkedSliceOutcome::HealthyLowYield) => 15,
        Some(DemandParkedSliceOutcome::WeakLowYield) => 5,
        Some(DemandParkedSliceOutcome::HealthyZeroYield) => -20,
        Some(DemandParkedSliceOutcome::Ignored) | None => -80,
    };
    let stop_score = match stop_reason {
        DemandSliceStopReason::NaturalFinish => -80,
        DemandSliceStopReason::IdleTimeout => -15,
        DemandSliceStopReason::WallTime => 0,
        DemandSliceStopReason::FirstBatch => -5,
        DemandSliceStopReason::UniquePeerCap => -10,
    };
    let peer_score = unique_peers.min(64) as i32;
    let inflight_penalty = (inflight_queries / 12) as i32;

    class_score + outcome_score + stop_score + peer_score - inflight_penalty
}

pub(super) fn draining_demand_inflight(
    active_runtime: &ActiveRuntime,
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
) -> usize {
    draining_demands
        .values()
        .flat_map(|drain| drain.lookup_ids.iter().copied())
        .filter_map(|lookup_id| active_runtime.runtime.lookup_quality_snapshot(lookup_id))
        .map(|snapshot| snapshot.inflight_len)
        .sum()
}

pub(super) fn demand_drain_admission_snapshot(
    drain: &DrainingDemandLookup,
) -> DemandDrainAdmissionSnapshot {
    DemandDrainAdmissionSnapshot {
        initial_inflight_queries: drain.initial_inflight_queries,
        score: drain.score,
        deadline_ms: duration_ms(drain.deadline.saturating_duration_since(drain.started_at)),
    }
}

pub(super) fn cancel_lookup_ids_to_parked(
    active_runtime: &mut ActiveRuntime,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    unique_peer_count: usize,
    lookup_ids: Vec<LookupId>,
) {
    let mut parked_states = Vec::new();
    for lookup_id in lookup_ids {
        if let Some(state) = active_runtime
            .runtime
            .cancel_lookup_and_take_state(lookup_id)
        {
            parked_states.push(state);
        }
    }

    store_parked_lookup_states(
        parked_crawls,
        info_hash,
        slice_class,
        Some(stop_reason),
        unique_peer_count,
        parked_states,
    );
}

pub(super) fn drain_lookup_ids(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    command_tx: &DhtCommandSender,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    total_peers: usize,
    unique_peers: HashSet<SocketAddr>,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
) -> Option<DemandParkedSliceOutcome> {
    let lookup_ids = {
        let mut lookup_ids = lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return None;
        }
        std::mem::take(&mut *lookup_ids)
    };

    let Some(active_runtime) = active_runtime else {
        return None;
    };

    if let Some(previous) = draining_demands.remove(&info_hash) {
        for lookup_id in previous.lookup_ids {
            active_runtime.runtime.cancel_lookup(lookup_id);
        }
    }

    let mut quality = AggregateLookupQualitySnapshot::default();
    let mut drainable_lookup_ids = Vec::new();
    for lookup_id in lookup_ids {
        if let Some(snapshot) = active_runtime.runtime.lookup_quality_snapshot(lookup_id) {
            quality.extend(snapshot);
            drainable_lookup_ids.push(lookup_id);
        }
    }

    if drainable_lookup_ids.is_empty() {
        return None;
    }

    let unique_peer_count = unique_peers.len();
    let parked_outcome = parked_slice_outcome_for_quality(
        slice_class,
        Some(stop_reason),
        unique_peer_count,
        quality,
    );
    let drain_duration =
        demand_drain_duration(slice_class, stop_reason, parked_outcome, unique_peer_count);
    let drain_score = demand_drain_score(
        slice_class,
        stop_reason,
        parked_outcome,
        unique_peer_count,
        quality.inflight_len,
    );
    let current_drain_inflight = draining_demand_inflight(active_runtime, draining_demands);
    let over_inflight_cap = current_drain_inflight.saturating_add(quality.inflight_len)
        > DHT_DEMAND_DRAIN_MAX_INFLIGHT_QUERIES;
    if quality.inflight_len == 0
        || drain_duration.is_none()
        || drain_score <= 0
        || over_inflight_cap
    {
        cancel_lookup_ids_to_parked(
            active_runtime,
            parked_crawls,
            info_hash,
            slice_class,
            stop_reason,
            unique_peer_count,
            drainable_lookup_ids,
        );
        return None;
    }

    let mut drained_lookup_ids = Vec::new();
    for lookup_id in drainable_lookup_ids {
        if active_runtime
            .runtime
            .pause_lookup_for_drain(lookup_id)
            .is_some()
        {
            drained_lookup_ids.push(lookup_id);
        }
    }

    if drained_lookup_ids.is_empty() {
        return None;
    }

    let now = Instant::now();
    let drain_duration = drain_duration.expect("checked drain duration");
    let no_late_yield_grace = demand_drain_no_late_yield_grace(slice_class).min(drain_duration);
    draining_demands.insert(
        info_hash,
        DrainingDemandLookup {
            lookup_ids: drained_lookup_ids,
            slice_class,
            stop_reason,
            started_at: now,
            total_peers,
            initial_unique_peers: unique_peer_count,
            unique_peers,
            deadline: now + drain_duration,
            no_late_yield_deadline: now + no_late_yield_grace,
            initial_inflight_queries: quality.inflight_len,
            score: drain_score,
        },
    );
    schedule_drained_demand_finalize(command_tx, info_hash, DHT_DEMAND_DRAIN_POLL_INTERVAL);
    parked_outcome
}

pub(super) fn drained_demand_lookup_runtime_ready(
    active_runtime: Option<&ActiveRuntime>,
    drain: &DrainingDemandLookup,
) -> bool {
    active_runtime.is_none_or(|active| active.runtime.drained_lookups_ready(&drain.lookup_ids))
}

pub(super) fn record_drain_peers_received(
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    info_hash: InfoHash,
    peers: &[SocketAddr],
) -> DemandPlannerReduction {
    let Some(drain) = draining_demands.get_mut(&info_hash) else {
        return DemandPlannerReduction::default();
    };
    let unique_added = drain.record_peers(peers);
    DemandPlannerReduction {
        effects: vec![DemandPlannerEffect::DrainPeersRecorded(
            DemandDrainPeersRecordedEffect {
                info_hash,
                peer_count: peers.len(),
                unique_added,
                initial_unique_peers: drain.initial_unique_peers,
            },
        )],
        plan_stats: None,
    }
}

impl DemandPlannerModel {
    pub(super) fn drain_runtime_readiness(
        &self,
        active_runtime: Option<&ActiveRuntime>,
    ) -> HashMap<InfoHash, bool> {
        self.draining_demands
            .iter()
            .map(|(&info_hash, drain)| {
                (
                    info_hash,
                    drained_demand_lookup_runtime_ready(active_runtime, drain),
                )
            })
            .collect()
    }

    pub(super) fn take_parked_family_state(
        &mut self,
        slice_metrics: Option<&mut DemandSliceMetrics>,
        info_hash: InfoHash,
        family: AddressFamily,
        slice_class: DemandSliceClass,
    ) -> Option<LookupState> {
        take_parked_family_state(
            &mut self.parked_crawls,
            slice_metrics,
            info_hash,
            family,
            slice_class,
        )
    }

    pub(super) fn park_lookup_ids(
        &mut self,
        active_runtime: Option<&mut ActiveRuntime>,
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: Option<DemandSliceStopReason>,
        unique_peers: usize,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    ) -> Option<DemandParkedSliceOutcome> {
        park_lookup_ids(
            active_runtime,
            &mut self.parked_crawls,
            info_hash,
            slice_class,
            stop_reason,
            unique_peers,
            lookup_ids,
        )
    }

    pub(super) fn drain_lookup_ids(
        &mut self,
        active_runtime: Option<&mut ActiveRuntime>,
        command_tx: &DhtCommandSender,
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: HashSet<SocketAddr>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    ) -> Option<DemandParkedSliceOutcome> {
        drain_lookup_ids(
            active_runtime,
            &mut self.parked_crawls,
            &mut self.draining_demands,
            command_tx,
            info_hash,
            slice_class,
            stop_reason,
            total_peers,
            unique_peers,
            lookup_ids,
        )
    }

    pub(super) fn drain_admission_snapshot(
        &self,
        info_hash: InfoHash,
    ) -> Option<DemandDrainAdmissionSnapshot> {
        self.draining_demands
            .get(&info_hash)
            .map(demand_drain_admission_snapshot)
    }

    pub(super) fn finalize_drained_lookup(
        &mut self,
        active_runtime: Option<&mut ActiveRuntime>,
        command_tx: &DhtCommandSender,
        info_hash: InfoHash,
        force: bool,
    ) -> Option<DrainedDemandOutcome> {
        finalize_drained_demand_lookup(
            active_runtime,
            &mut self.parked_crawls,
            &mut self.draining_demands,
            command_tx,
            info_hash,
            force,
        )
    }
}

pub(super) fn drained_demand_lookup_ready_for_finalize(
    runtime_ready: bool,
    drain: &DrainingDemandLookup,
    now: Instant,
) -> (bool, bool) {
    let early_no_yield = !runtime_ready
        && now >= drain.no_late_yield_deadline
        && drain.late_unique_peer_count() == 0;
    let ready_to_finalize = runtime_ready || early_no_yield || now >= drain.deadline;
    (ready_to_finalize, early_no_yield)
}

pub(super) fn finalize_drained_demand_lookup(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    command_tx: &DhtCommandSender,
    info_hash: InfoHash,
    force: bool,
) -> Option<DrainedDemandOutcome> {
    let drain = draining_demands.get(&info_hash).cloned()?;
    let now = Instant::now();
    let runtime_ready = drained_demand_lookup_runtime_ready(
        active_runtime.as_ref().map(|active| &**active),
        &drain,
    );
    let (ready_to_finalize, early_no_yield) =
        drained_demand_lookup_ready_for_finalize(runtime_ready, &drain, now);
    if !force && !ready_to_finalize {
        schedule_drained_demand_finalize(command_tx, info_hash, DHT_DEMAND_DRAIN_POLL_INTERVAL);
        return None;
    }

    let drain = draining_demands.remove(&info_hash)?;
    let drain_duration_ms = drain.duration_ms(now);
    let finalized_after_deadline = now >= drain.deadline;
    let unique_peers = drain.unique_peer_count();
    let mut drained_states = Vec::new();
    if let Some(active_runtime) = active_runtime {
        for lookup_id in drain.lookup_ids {
            if let Some(state) = active_runtime.runtime.finish_drained_lookup(lookup_id) {
                drained_states.push(state);
            }
        }
    }

    let parked_outcome = store_parked_lookup_states(
        parked_crawls,
        info_hash,
        drain.slice_class,
        Some(drain.stop_reason),
        unique_peers,
        drained_states,
    );

    Some(DrainedDemandOutcome {
        slice_class: drain.slice_class,
        stop_reason: drain.stop_reason,
        total_peers: drain.total_peers,
        unique_peers,
        parked_outcome,
        drain_duration_ms,
        finalized_after_deadline,
        finalized_early_no_yield: early_no_yield && !finalized_after_deadline,
    })
}

pub(super) fn evict_stale_parked_crawls(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    now: Instant,
) {
    parked_crawls.retain(|_, crawl| !crawl.is_stale(now) && !crawl.is_empty());
}

pub(super) fn active_demand_lookup_slot_count(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
) -> usize {
    demand_lookup_ids.len()
}

pub(super) fn active_demand_lookup_slot_counts(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
) -> DemandSlotCounts {
    let mut counts = DemandSlotCounts::default();
    for lookup in demand_lookup_ids.values() {
        counts.record(lookup.slice_class);
    }
    counts
}

pub(super) fn draining_demand_slot_counts(
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
) -> DemandSlotCounts {
    let mut counts = DemandSlotCounts::default();
    for drain in draining_demands.values() {
        counts.record(drain.slice_class);
    }
    counts
}

pub(super) fn drain_virtual_slot_count(draining_lookup_count: usize) -> usize {
    if draining_lookup_count == 0 {
        0
    } else {
        draining_lookup_count.saturating_add(DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT - 1)
            / DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT
    }
}

pub(super) fn demand_lookup_launch_budget(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
    draining_lookup_count: usize,
) -> usize {
    let consumed_slots = active_demand_lookup_slot_count(demand_lookup_ids)
        .saturating_add(drain_virtual_slot_count(draining_lookup_count));
    let available_slots = DHT_DEMAND_LOOKUP_SLOT_COUNT.saturating_sub(consumed_slots);
    available_slots.min(DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK)
}

pub(super) fn demand_lookup_class_slot_cap(class: DemandSliceClass) -> usize {
    match class {
        DemandSliceClass::AwaitingMetadata => DHT_AWAITING_METADATA_SLOT_CAP,
        DemandSliceClass::NoConnectedPeers => DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        DemandSliceClass::RoutineRefresh => DHT_ROUTINE_LOOKUP_SLOT_CAP,
    }
}

pub(super) fn demand_slice_class_priority(class: DemandSliceClass) -> u8 {
    match class {
        DemandSliceClass::AwaitingMetadata => 3,
        DemandSliceClass::NoConnectedPeers => 2,
        DemandSliceClass::RoutineRefresh => 1,
    }
}

pub(super) fn due_candidate_has_reusable_parked_crawl(
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    candidate: DueDemandCandidate,
    now: Instant,
) -> bool {
    let class = DemandSliceClass::from_demand(candidate.demand);
    parked_crawls
        .get(&candidate.info_hash)
        .is_some_and(|crawl| !crawl.is_empty() && !crawl.should_reset_for(class, now))
}

pub(super) fn candidate_last_useful_yield_age(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> Option<Duration> {
    planner_state
        .get(&info_hash)
        .and_then(|state| state.last_useful_yield_at)
        .map(|at| now.saturating_duration_since(at))
}

pub(super) fn candidate_last_unique_peers(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
) -> usize {
    planner_state
        .get(&info_hash)
        .map(|state| state.last_unique_peers)
        .unwrap_or(0)
}

pub(super) fn candidate_due_age(candidate: DueDemandCandidate, now: Instant) -> Duration {
    now.saturating_duration_since(candidate.next_eligible_at)
}

pub(super) fn candidate_has_fairness_age(candidate: DueDemandCandidate, now: Instant) -> bool {
    candidate_due_age(candidate, now) >= DHT_DEMAND_FAIRNESS_AGE
}

pub(super) fn candidate_has_useful_yield_history(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> bool {
    candidate_last_useful_yield_age(planner_state, info_hash, now).is_some()
        && candidate_last_unique_peers(planner_state, info_hash) > 0
}

pub(super) fn candidate_selection_reason(
    candidate: DueDemandCandidate,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    now: Instant,
) -> DemandSelectionReason {
    if candidate_has_useful_yield_history(planner_state, candidate.info_hash, now) {
        DemandSelectionReason::UsefulYieldHistory
    } else if due_candidate_has_reusable_parked_crawl(parked_crawls, candidate, now) {
        DemandSelectionReason::ReusableParked
    } else {
        DemandSelectionReason::OverdueScarce
    }
}

pub(super) fn candidate_last_activity_age(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> Option<Duration> {
    planner_state.get(&info_hash).and_then(|state| {
        state
            .last_finished_at
            .or(state.last_started_at)
            .map(|at| now.saturating_duration_since(at))
    })
}

pub(super) fn spare_research_candidate_ready(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> bool {
    candidate_last_activity_age(planner_state, info_hash, now)
        .map(|age| age >= DHT_DEMAND_SPARE_RESEARCH_MIN_INTERVAL)
        .unwrap_or(true)
}

pub(super) fn demand_planner_selection_stats(
    offered_candidates: &[DueDemandCandidate],
    launched_candidates: &[DueDemandCandidate],
    now: Instant,
) -> DemandPlannerSelectionStats {
    let launched_hashes = launched_candidates
        .iter()
        .map(|candidate| candidate.info_hash)
        .collect::<HashSet<_>>();
    let mut stats = DemandPlannerSelectionStats::default();

    for candidate in offered_candidates {
        let class = DemandSliceClass::from_demand(candidate.demand);
        stats.offered.record(class);
        if launched_hashes.contains(&candidate.info_hash) {
            stats.launched.record(class);
        } else {
            stats.throttled.record(class);
            stats.record_throttled_age(
                class,
                duration_ms(now.saturating_duration_since(candidate.next_eligible_at)),
            );
        }
    }

    stats
}

pub(super) fn select_spare_research_launches(
    demand_snapshots: &[DemandEntrySnapshot],
    active_counts: DemandSlotCounts,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
    total_budget: usize,
) -> Vec<DueDemandCandidate> {
    if total_budget == 0 || active_counts.total() >= DHT_DEMAND_SPARE_RESEARCH_MAX_ACTIVE {
        return Vec::new();
    }

    let take_count = total_budget.min(DHT_DEMAND_SPARE_RESEARCH_LAUNCH_LIMIT);
    let mut candidates = demand_snapshots
        .iter()
        .copied()
        .filter(|snapshot| {
            snapshot.subscriber_count > 0
                && !snapshot.in_progress
                && snapshot.next_eligible_at > now
                && DemandSliceClass::from_demand(snapshot.demand)
                    == DemandSliceClass::NoConnectedPeers
                && spare_research_candidate_ready(planner_state, snapshot.info_hash, now)
        })
        .map(|snapshot| DueDemandCandidate {
            info_hash: snapshot.info_hash,
            demand: snapshot.demand,
            next_eligible_at: snapshot.next_eligible_at,
            subscriber_count: snapshot.subscriber_count,
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        let left_activity_age = candidate_last_activity_age(planner_state, left.info_hash, now);
        let right_activity_age = candidate_last_activity_age(planner_state, right.info_hash, now);
        let left_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *left, now);
        let right_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *right, now);
        match (left_activity_age, right_activity_age) {
            (Some(left_age), Some(right_age)) => right_age.cmp(&left_age),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| left.next_eligible_at.cmp(&right.next_eligible_at))
        .then_with(|| right_reusable.cmp(&left_reusable))
        .then_with(|| {
            left.demand
                .connected_peers
                .cmp(&right.demand.connected_peers)
        })
        .then_with(|| right.subscriber_count.cmp(&left.subscriber_count))
    });

    let mut selected = Vec::new();
    for candidate in candidates {
        if selected.len() >= take_count {
            break;
        }
        if !planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, now) {
            break;
        }
        selected.push(candidate);
    }

    selected
}

pub(super) fn select_due_demand_launches(
    due_candidates: &[DueDemandCandidate],
    active_counts: DemandSlotCounts,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
    total_budget: usize,
) -> Vec<DueDemandCandidate> {
    select_due_demand_launches_with_stats(
        due_candidates,
        active_counts,
        parked_crawls,
        planner_state,
        planner_budget,
        now,
        total_budget,
    )
    .launches
}

pub(super) fn select_due_demand_launches_with_stats(
    due_candidates: &[DueDemandCandidate],
    active_counts: DemandSlotCounts,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
    total_budget: usize,
) -> DemandPlannerSelection {
    let mut selected = Vec::new();
    let mut planned_counts = active_counts;
    let mut candidates = due_candidates.to_vec();

    candidates.sort_by(|left, right| {
        let left_class = DemandSliceClass::from_demand(left.demand);
        let right_class = DemandSliceClass::from_demand(right.demand);
        let left_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *left, now);
        let right_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *right, now);
        let left_useful_age = candidate_last_useful_yield_age(planner_state, left.info_hash, now);
        let right_useful_age = candidate_last_useful_yield_age(planner_state, right.info_hash, now);
        let left_last_unique = candidate_last_unique_peers(planner_state, left.info_hash);
        let right_last_unique = candidate_last_unique_peers(planner_state, right.info_hash);
        let left_fairness = candidate_has_fairness_age(*left, now);
        let right_fairness = candidate_has_fairness_age(*right, now);

        demand_slice_class_priority(right_class)
            .cmp(&demand_slice_class_priority(left_class))
            .then_with(|| right_fairness.cmp(&left_fairness))
            .then_with(|| match (left_useful_age, right_useful_age) {
                (Some(left_age), Some(right_age)) => left_age.cmp(&right_age),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then_with(|| right_last_unique.cmp(&left_last_unique))
            .then_with(|| right_reusable.cmp(&left_reusable))
            .then_with(|| {
                now.saturating_duration_since(right.next_eligible_at)
                    .cmp(&now.saturating_duration_since(left.next_eligible_at))
            })
            .then_with(|| {
                left.demand
                    .connected_peers
                    .cmp(&right.demand.connected_peers)
            })
            .then_with(|| right.subscriber_count.cmp(&left.subscriber_count))
    });

    for candidate in candidates {
        if selected.len() >= total_budget {
            break;
        }

        let class = DemandSliceClass::from_demand(candidate.demand);
        if planned_counts.count(class) >= demand_lookup_class_slot_cap(class) {
            continue;
        }
        if !planner_budget.try_consume(class, now) {
            continue;
        }
        planned_counts.record(class);
        selected.push(candidate);
    }

    DemandPlannerSelection {
        stats: demand_planner_selection_stats(due_candidates, &selected, now),
        launches: selected,
    }
}

impl DemandPlannerModel {
    pub(super) fn update(&mut self, action: DemandPlannerAction<'_>) -> DemandPlannerReduction {
        let action_view = DemandPlannerActionView::from_action(&action);
        let reduction = {
            let demand_scheduler = &mut self.scheduler;
            let demand_lookup_ids = &mut self.active;
            let parked_crawls = &mut self.parked_crawls;
            let draining_demands = &mut self.draining_demands;
            let planner_state = &mut self.state;
            let planner_budget = &mut self.budget;

            match action {
                DemandPlannerAction::RuntimeReset { now } => {
                    demand_scheduler.reset_active(now);
                    demand_lookup_ids.clear();
                    parked_crawls.clear();
                    draining_demands.clear();
                    planner_state.clear();
                    *planner_budget = DemandPlannerBudget::new(now);
                    DemandPlannerReduction::default()
                }
                DemandPlannerAction::DemandRegistered {
                    info_hash,
                    demand,
                    now,
                } => {
                    demand_scheduler.register(info_hash, demand, now);
                    let effects = if draining_demands.get(&info_hash).is_some_and(|drain| {
                        drain.slice_class != DemandSliceClass::from_demand(demand)
                    }) {
                        vec![DemandPlannerEffect::FinalizeDrainingLookup(
                            DemandFinalizeDrainingLookupEffect {
                                info_hash,
                                force: true,
                            },
                        )]
                    } else {
                        Vec::new()
                    };
                    DemandPlannerReduction {
                        effects,
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::DemandUpdated {
                    info_hash,
                    demand,
                    now,
                } => {
                    demand_scheduler.update(info_hash, demand, now);
                    let effects = if draining_demands.get(&info_hash).is_some_and(|drain| {
                        drain.slice_class != DemandSliceClass::from_demand(demand)
                    }) {
                        vec![DemandPlannerEffect::FinalizeDrainingLookup(
                            DemandFinalizeDrainingLookupEffect {
                                info_hash,
                                force: true,
                            },
                        )]
                    } else {
                        Vec::new()
                    };
                    DemandPlannerReduction {
                        effects,
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::DemandSubscriberRemoved { info_hash } => {
                    let slice_class = demand_scheduler
                        .demand_state(info_hash)
                        .map(DemandSliceClass::from_demand)
                        .unwrap_or(DemandSliceClass::RoutineRefresh);
                    let mut effects = Vec::new();
                    if demand_scheduler.unregister(info_hash) {
                        if let Some(lookup) = demand_lookup_ids.remove(&info_hash) {
                            effects.push(DemandPlannerEffect::ParkActiveLookup(
                                DemandParkActiveLookupEffect {
                                    info_hash,
                                    slice_class,
                                    lookup_ids: lookup.lookup_ids,
                                },
                            ));
                        }
                        if let Some(drain) = draining_demands.remove(&info_hash) {
                            effects.push(DemandPlannerEffect::CancelDrainingLookup(
                                DemandCancelDrainingLookupEffect {
                                    info_hash,
                                    lookup_ids: drain.lookup_ids,
                                },
                            ));
                        }
                    }
                    DemandPlannerReduction {
                        effects,
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::PeersReceived { info_hash, peers } => {
                    record_drain_peers_received(draining_demands, info_hash, peers)
                }
                DemandPlannerAction::DrainTick { now, runtime_ready } => {
                    let effects = draining_demands
                        .iter()
                        .filter_map(|(&info_hash, drain)| {
                            let ready = runtime_ready.get(&info_hash).copied().unwrap_or(false);
                            let (ready_to_finalize, _) =
                                drained_demand_lookup_ready_for_finalize(ready, drain, now);
                            ready_to_finalize.then_some(
                                DemandPlannerEffect::FinalizeDrainingLookup(
                                    DemandFinalizeDrainingLookupEffect {
                                        info_hash,
                                        force: false,
                                    },
                                ),
                            )
                        })
                        .collect();
                    DemandPlannerReduction {
                        effects,
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::PlanDue {
                    now,
                    runtime_available,
                } => {
                    if !runtime_available {
                        DemandPlannerReduction::default()
                    } else {
                        evict_stale_parked_crawls(parked_crawls, now);
                        let drain_virtual_slots = drain_virtual_slot_count(draining_demands.len());
                        let launch_budget =
                            demand_lookup_launch_budget(demand_lookup_ids, draining_demands.len());
                        if launch_budget == 0 {
                            DemandPlannerReduction::default()
                        } else {
                            planner_budget.refill(now);
                            let active_counts = active_demand_lookup_slot_counts(demand_lookup_ids);
                            let due_candidates = demand_scheduler
                                .due_candidates(now)
                                .into_iter()
                                .filter(|candidate| {
                                    !draining_demands.contains_key(&candidate.info_hash)
                                })
                                .collect::<Vec<_>>();
                            let demand_snapshots = demand_scheduler
                                .entry_snapshots()
                                .into_iter()
                                .filter(|snapshot| {
                                    !draining_demands.contains_key(&snapshot.info_hash)
                                })
                                .collect::<Vec<_>>();
                            let due_selection = select_due_demand_launches_with_stats(
                                &due_candidates,
                                active_counts,
                                parked_crawls,
                                planner_state,
                                planner_budget,
                                now,
                                launch_budget,
                            );
                            let selection_stats = due_selection.stats;
                            let mut planned_launches = due_selection
                                .launches
                                .into_iter()
                                .map(|candidate| {
                                    (
                                        candidate,
                                        candidate_selection_reason(
                                            candidate,
                                            parked_crawls,
                                            planner_state,
                                            now,
                                        ),
                                    )
                                })
                                .collect::<Vec<_>>();

                            if planned_launches.is_empty() {
                                planned_launches = select_spare_research_launches(
                                    &demand_snapshots,
                                    active_counts,
                                    parked_crawls,
                                    planner_state,
                                    planner_budget,
                                    now,
                                    launch_budget,
                                )
                                .into_iter()
                                .map(|candidate| (candidate, DemandSelectionReason::SpareCapacity))
                                .collect();
                            }

                            let spare_selected = planned_launches
                                .iter()
                                .filter(|(_, reason)| {
                                    *reason == DemandSelectionReason::SpareCapacity
                                })
                                .count();
                            let mut effects = Vec::new();
                            for (candidate, selection_reason) in planned_launches {
                                let plan = DemandLookupPlan::for_demand(candidate.demand);
                                if !demand_scheduler.mark_in_progress(candidate.info_hash) {
                                    planner_budget.refund(plan.class);
                                    continue;
                                }
                                planner_state
                                    .entry(candidate.info_hash)
                                    .or_default()
                                    .note_start(now);
                                effects.push(DemandPlannerEffect::StartLookup(
                                    DemandStartLookupEffect {
                                        candidate,
                                        plan,
                                        selection_reason,
                                    },
                                ));
                            }

                            let budget_awaiting =
                                planner_budget.available(DemandSliceClass::AwaitingMetadata, now);
                            let budget_no_peers =
                                planner_budget.available(DemandSliceClass::NoConnectedPeers, now);
                            let budget_routine =
                                planner_budget.available(DemandSliceClass::RoutineRefresh, now);

                            DemandPlannerReduction {
                                effects,
                                plan_stats: Some(DemandPlannerPlanStats {
                                    launch_budget,
                                    due_total: due_candidates.len(),
                                    selection_stats,
                                    spare_selected,
                                    active_counts,
                                    parked_count: parked_crawls.len(),
                                    draining_count: draining_demands.len(),
                                    drain_virtual_slots,
                                    budget_awaiting,
                                    budget_no_peers,
                                    budget_routine,
                                }),
                            }
                        }
                    }
                }
                DemandPlannerAction::LookupStarted {
                    info_hash,
                    slice_class,
                    lookup_ids,
                } => {
                    demand_lookup_ids.insert(
                        info_hash,
                        ActiveDemandLookup {
                            lookup_ids,
                            slice_class,
                        },
                    );
                    DemandPlannerReduction::default()
                }
                DemandPlannerAction::LookupStartFailed {
                    info_hash,
                    slice_class,
                    now,
                } => {
                    planner_budget.refund(slice_class);
                    demand_scheduler.finish(info_hash, now);
                    DemandPlannerReduction::default()
                }
                DemandPlannerAction::LookupFinished {
                    info_hash,
                    slice_class,
                    total_peers,
                    unique_peers,
                    now,
                } => {
                    let previous = demand_scheduler.entry_snapshot(info_hash);
                    demand_lookup_ids.remove(&info_hash);
                    planner_state
                        .entry(info_hash)
                        .or_default()
                        .note_finish(now, unique_peers);
                    demand_scheduler.finish(info_hash, now);
                    DemandPlannerReduction {
                        effects: vec![DemandPlannerEffect::LookupFinished(
                            DemandLookupFinishedEffect {
                                info_hash,
                                slice_class,
                                total_peers,
                                unique_peers,
                                previous,
                                current: demand_scheduler.entry_snapshot(info_hash),
                                finished_at: now,
                            },
                        )],
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::LookupParkRequested {
                    info_hash,
                    slice_class,
                    stop_reason,
                    total_peers,
                    unique_peers,
                    lookup_ids,
                } => {
                    let previous = demand_scheduler.entry_snapshot(info_hash);
                    demand_lookup_ids.remove(&info_hash);
                    DemandPlannerReduction {
                        effects: vec![DemandPlannerEffect::AdmitDrain(DemandAdmitDrainEffect {
                            info_hash,
                            slice_class,
                            stop_reason,
                            total_peers,
                            unique_peers,
                            lookup_ids,
                            previous,
                        })],
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::LookupParkResolved {
                    info_hash,
                    slice_class,
                    stop_reason,
                    total_peers,
                    unique_peers,
                    parked_outcome,
                    drain_admission,
                    previous,
                    now,
                } => {
                    if drain_admission.is_none() {
                        planner_state
                            .entry(info_hash)
                            .or_default()
                            .note_finish(now, unique_peers);
                        demand_scheduler.finish(info_hash, now);
                    }
                    let should_finalize_drain = drain_admission.is_some()
                        && demand_scheduler
                            .demand_state(info_hash)
                            .map(DemandSliceClass::from_demand)
                            .is_some_and(|current_class| current_class != slice_class);
                    let mut effects = vec![DemandPlannerEffect::LookupParked(
                        DemandLookupParkedEffect {
                            info_hash,
                            slice_class,
                            stop_reason,
                            total_peers,
                            unique_peers,
                            parked_outcome,
                            drain_admission,
                            previous,
                            current: demand_scheduler.entry_snapshot(info_hash),
                            parked_at: now,
                        },
                    )];
                    if should_finalize_drain {
                        effects.push(DemandPlannerEffect::FinalizeDrainingLookup(
                            DemandFinalizeDrainingLookupEffect {
                                info_hash,
                                force: true,
                            },
                        ));
                    }
                    DemandPlannerReduction {
                        effects,
                        plan_stats: None,
                    }
                }
                DemandPlannerAction::DrainedLookupFinalized {
                    info_hash,
                    outcome,
                    previous,
                    now,
                } => {
                    planner_state
                        .entry(info_hash)
                        .or_default()
                        .note_finish(now, outcome.unique_peers);
                    let finish_mode = if outcome.slice_class == DemandSliceClass::NoConnectedPeers
                        && outcome.parked_outcome
                            == Some(DemandParkedSliceOutcome::HealthyZeroYield)
                    {
                        DemandFinishMode::AcceleratedNoConnectedPeersBackoff
                    } else {
                        DemandFinishMode::Standard
                    };
                    demand_scheduler.finish_with_mode(info_hash, now, finish_mode);
                    DemandPlannerReduction {
                        effects: vec![DemandPlannerEffect::DrainFinalized(
                            DemandDrainFinalizedEffect {
                                info_hash,
                                outcome,
                                finish_mode,
                                previous,
                                current: demand_scheduler.entry_snapshot(info_hash),
                                finalized_at: now,
                                parked: parked_crawls.contains_key(&info_hash),
                            },
                        )],
                        plan_stats: None,
                    }
                }
            }
        };
        trace_demand_planner_reduction(action_view, &reduction, self);
        reduction
    }
}
