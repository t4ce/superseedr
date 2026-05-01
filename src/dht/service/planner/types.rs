// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::super::*;

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct ActiveDemandLookup {
    pub(in crate::dht::service) lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    pub(in crate::dht::service) slice_class: DemandSliceClass,
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct DrainingDemandLookup {
    pub(in crate::dht::service) lookup_ids: Vec<LookupId>,
    pub(in crate::dht::service) slice_class: DemandSliceClass,
    pub(in crate::dht::service) stop_reason: DemandSliceStopReason,
    pub(in crate::dht::service) started_at: Instant,
    pub(in crate::dht::service) total_peers: usize,
    pub(in crate::dht::service) initial_unique_peers: usize,
    pub(in crate::dht::service) unique_peers: HashSet<SocketAddr>,
    pub(in crate::dht::service) deadline: Instant,
    pub(in crate::dht::service) no_late_yield_deadline: Instant,
    pub(in crate::dht::service) initial_inflight_queries: usize,
    pub(in crate::dht::service) score: i32,
}

impl DrainingDemandLookup {
    pub(in crate::dht::service) fn record_peers(&mut self, peers: &[SocketAddr]) -> usize {
        let previous_unique_peers = self.unique_peers.len();
        self.total_peers = self.total_peers.saturating_add(peers.len());
        self.unique_peers.extend(peers.iter().copied());
        self.unique_peers
            .len()
            .saturating_sub(previous_unique_peers)
    }

    pub(in crate::dht::service) fn unique_peer_count(&self) -> usize {
        self.unique_peers.len()
    }

    pub(in crate::dht::service) fn late_unique_peer_count(&self) -> usize {
        self.unique_peer_count()
            .saturating_sub(self.initial_unique_peers)
    }

    pub(in crate::dht::service) fn duration_ms(&self, now: Instant) -> u64 {
        duration_ms(now.saturating_duration_since(self.started_at))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dht::service) struct DrainedDemandOutcome {
    pub(in crate::dht::service) slice_class: DemandSliceClass,
    pub(in crate::dht::service) stop_reason: DemandSliceStopReason,
    pub(in crate::dht::service) total_peers: usize,
    pub(in crate::dht::service) unique_peers: usize,
    pub(in crate::dht::service) parked_outcome: Option<DemandParkedSliceOutcome>,
    pub(in crate::dht::service) drain_duration_ms: u64,
    pub(in crate::dht::service) finalized_after_deadline: bool,
    pub(in crate::dht::service) finalized_early_no_yield: bool,
}

#[derive(Debug, Clone, Default)]
pub(in crate::dht::service) struct DemandPlannerState {
    pub(in crate::dht::service) last_started_at: Option<Instant>,
    pub(in crate::dht::service) last_finished_at: Option<Instant>,
    pub(in crate::dht::service) last_useful_yield_at: Option<Instant>,
    pub(in crate::dht::service) last_unique_peers: usize,
}

impl DemandPlannerState {
    pub(in crate::dht::service) fn note_start(&mut self, now: Instant) {
        self.last_started_at = Some(now);
    }

    pub(in crate::dht::service) fn note_finish(&mut self, now: Instant, unique_peers: usize) {
        self.last_finished_at = Some(now);
        self.last_unique_peers = unique_peers;
        if unique_peers > 0 {
            self.last_useful_yield_at = Some(now);
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct DemandLaunchTokenBucket {
    pub(in crate::dht::service) tokens_scaled: u64,
    pub(in crate::dht::service) burst_scaled: u64,
    pub(in crate::dht::service) refill_per_minute: u64,
    pub(in crate::dht::service) refill_remainder: u128,
    pub(in crate::dht::service) last_refill_at: Instant,
}

impl DemandLaunchTokenBucket {
    pub(in crate::dht::service) fn new(refill_per_minute: u64, burst: u64, now: Instant) -> Self {
        let burst_scaled = burst.saturating_mul(DHT_PLANNER_TOKEN_SCALE);
        Self {
            tokens_scaled: burst_scaled,
            burst_scaled,
            refill_per_minute,
            refill_remainder: 0,
            last_refill_at: now,
        }
    }

    pub(in crate::dht::service) fn refill(&mut self, now: Instant) {
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

    pub(in crate::dht::service) fn try_consume(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens_scaled < DHT_PLANNER_TOKEN_SCALE {
            return false;
        }

        self.tokens_scaled = self.tokens_scaled.saturating_sub(DHT_PLANNER_TOKEN_SCALE);
        true
    }

    pub(in crate::dht::service) fn refund(&mut self) {
        self.tokens_scaled = self
            .tokens_scaled
            .saturating_add(DHT_PLANNER_TOKEN_SCALE)
            .min(self.burst_scaled);
    }

    pub(in crate::dht::service) fn available(&self) -> usize {
        (self.tokens_scaled / DHT_PLANNER_TOKEN_SCALE) as usize
    }
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct DemandPlannerBudget {
    pub(in crate::dht::service) awaiting_metadata: DemandLaunchTokenBucket,
    pub(in crate::dht::service) no_connected_peers: DemandLaunchTokenBucket,
    pub(in crate::dht::service) routine_refresh: DemandLaunchTokenBucket,
}

impl DemandPlannerBudget {
    pub(in crate::dht::service) fn new(now: Instant) -> Self {
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

    pub(in crate::dht::service) fn bucket_mut(
        &mut self,
        class: DemandSliceClass,
    ) -> &mut DemandLaunchTokenBucket {
        match class {
            DemandSliceClass::AwaitingMetadata => &mut self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &mut self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &mut self.routine_refresh,
        }
    }

    pub(in crate::dht::service) fn refill(&mut self, now: Instant) {
        self.awaiting_metadata.refill(now);
        self.no_connected_peers.refill(now);
        self.routine_refresh.refill(now);
    }

    pub(in crate::dht::service) fn try_consume(
        &mut self,
        class: DemandSliceClass,
        now: Instant,
    ) -> bool {
        self.bucket_mut(class).try_consume(now)
    }

    pub(in crate::dht::service) fn refund(&mut self, class: DemandSliceClass) {
        self.bucket_mut(class).refund();
    }

    pub(in crate::dht::service) fn available(
        &mut self,
        class: DemandSliceClass,
        now: Instant,
    ) -> usize {
        self.bucket_mut(class).refill(now);
        self.bucket_mut(class).available()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::dht::service) struct DemandSlotCounts {
    pub(in crate::dht::service) awaiting_metadata: usize,
    pub(in crate::dht::service) no_connected_peers: usize,
    pub(in crate::dht::service) routine_refresh: usize,
}

impl DemandSlotCounts {
    pub(in crate::dht::service) fn count(self, class: DemandSliceClass) -> usize {
        match class {
            DemandSliceClass::AwaitingMetadata => self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => self.routine_refresh,
        }
    }

    pub(in crate::dht::service) fn total(self) -> usize {
        self.awaiting_metadata
            .saturating_add(self.no_connected_peers)
            .saturating_add(self.routine_refresh)
    }

    pub(in crate::dht::service) fn record(&mut self, class: DemandSliceClass) {
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
pub(in crate::dht::service) struct DemandPlannerSelectionStats {
    pub(in crate::dht::service) offered: DemandSlotCounts,
    pub(in crate::dht::service) launched: DemandSlotCounts,
    pub(in crate::dht::service) throttled: DemandSlotCounts,
    pub(in crate::dht::service) oldest_throttled_awaiting_ms: u64,
    pub(in crate::dht::service) oldest_throttled_no_peers_ms: u64,
    pub(in crate::dht::service) oldest_throttled_routine_ms: u64,
}

impl DemandPlannerSelectionStats {
    pub(in crate::dht::service) fn record_throttled_age(
        &mut self,
        class: DemandSliceClass,
        age_ms: u64,
    ) {
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
pub(in crate::dht::service) struct DemandPlannerSelection {
    pub(in crate::dht::service) launches: Vec<DueDemandCandidate>,
    pub(in crate::dht::service) stats: DemandPlannerSelectionStats,
}

#[derive(Debug)]
pub(in crate::dht::service) enum DemandPlannerAction<'a> {
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
    DemandMetricsUpdated {
        info_hash: InfoHash,
        metrics: DhtDemandMetrics,
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
pub(in crate::dht::service) struct DemandStartLookupEffect {
    pub(in crate::dht::service) candidate: DueDemandCandidate,
    pub(in crate::dht::service) plan: DemandLookupPlan,
    pub(in crate::dht::service) selection_reason: DemandSelectionReason,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::dht::service) struct DemandLookupFinishedEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) slice_class: DemandSliceClass,
    pub(in crate::dht::service) total_peers: usize,
    pub(in crate::dht::service) unique_peers: usize,
    pub(in crate::dht::service) previous: Option<DemandEntrySnapshot>,
    pub(in crate::dht::service) current: Option<DemandEntrySnapshot>,
    pub(in crate::dht::service) finished_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dht::service) struct DemandDrainAdmissionSnapshot {
    pub(in crate::dht::service) initial_inflight_queries: usize,
    pub(in crate::dht::service) score: i32,
    pub(in crate::dht::service) deadline_ms: u64,
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct DemandAdmitDrainEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) slice_class: DemandSliceClass,
    pub(in crate::dht::service) stop_reason: DemandSliceStopReason,
    pub(in crate::dht::service) total_peers: usize,
    pub(in crate::dht::service) unique_peers: HashSet<SocketAddr>,
    pub(in crate::dht::service) lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    pub(in crate::dht::service) previous: Option<DemandEntrySnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::dht::service) struct DemandLookupParkedEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) slice_class: DemandSliceClass,
    pub(in crate::dht::service) stop_reason: DemandSliceStopReason,
    pub(in crate::dht::service) total_peers: usize,
    pub(in crate::dht::service) unique_peers: usize,
    pub(in crate::dht::service) parked_outcome: Option<DemandParkedSliceOutcome>,
    pub(in crate::dht::service) drain_admission: Option<DemandDrainAdmissionSnapshot>,
    pub(in crate::dht::service) previous: Option<DemandEntrySnapshot>,
    pub(in crate::dht::service) current: Option<DemandEntrySnapshot>,
    pub(in crate::dht::service) parked_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::dht::service) struct DemandDrainFinalizedEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) outcome: DrainedDemandOutcome,
    pub(in crate::dht::service) finish_mode: DemandFinishMode,
    pub(in crate::dht::service) previous: Option<DemandEntrySnapshot>,
    pub(in crate::dht::service) current: Option<DemandEntrySnapshot>,
    pub(in crate::dht::service) finalized_at: Instant,
    pub(in crate::dht::service) parked: bool,
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct DemandParkActiveLookupEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) slice_class: DemandSliceClass,
    pub(in crate::dht::service) lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) struct DemandCancelDrainingLookupEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) lookup_ids: Vec<LookupId>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::dht::service) struct DemandFinalizeDrainingLookupEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) force: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::dht::service) struct DemandDrainPeersRecordedEffect {
    pub(in crate::dht::service) info_hash: InfoHash,
    pub(in crate::dht::service) peer_count: usize,
    pub(in crate::dht::service) unique_added: usize,
    pub(in crate::dht::service) initial_unique_peers: usize,
}

#[derive(Debug, Clone)]
pub(in crate::dht::service) enum DemandPlannerEffect {
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
pub(in crate::dht::service) struct DemandPlannerPlanStats {
    pub(in crate::dht::service) launch_budget: usize,
    pub(in crate::dht::service) due_total: usize,
    pub(in crate::dht::service) selection_stats: DemandPlannerSelectionStats,
    pub(in crate::dht::service) spare_selected: usize,
    pub(in crate::dht::service) idle_probe_selected: usize,
    pub(in crate::dht::service) idle_probe_active: bool,
    pub(in crate::dht::service) idle_probe_demand_count: usize,
    pub(in crate::dht::service) active_counts: DemandSlotCounts,
    pub(in crate::dht::service) parked_count: usize,
    pub(in crate::dht::service) draining_count: usize,
    pub(in crate::dht::service) drain_virtual_slots: usize,
    pub(in crate::dht::service) budget_awaiting: usize,
    pub(in crate::dht::service) budget_no_peers: usize,
    pub(in crate::dht::service) budget_routine: usize,
}

#[derive(Debug, Default)]
pub(in crate::dht::service) struct DemandPlannerReduction {
    pub(in crate::dht::service) effects: Vec<DemandPlannerEffect>,
    pub(in crate::dht::service) plan_stats: Option<DemandPlannerPlanStats>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::dht::service) struct DemandPlannerIdleSpeedProbeStatus {
    pub(in crate::dht::service) active: bool,
    pub(in crate::dht::service) demand_count: usize,
    pub(in crate::dht::service) multiplier: u8,
}

#[derive(Debug)]
pub(in crate::dht::service) struct DemandPlannerIdleSpeedProbe {
    idle_since: Option<Instant>,
    current_multiplier: u8,
    decay_since: Option<Instant>,
}

impl Default for DemandPlannerIdleSpeedProbe {
    fn default() -> Self {
        Self {
            idle_since: None,
            current_multiplier: 1,
            decay_since: None,
        }
    }
}

impl DemandPlannerIdleSpeedProbe {
    pub(in crate::dht::service) fn current_multiplier(&self, _now: Instant) -> u8 {
        self.current_multiplier.max(1)
    }

    pub(in crate::dht::service) fn observe(
        &mut self,
        snapshots: &[DemandEntrySnapshot],
        now: Instant,
    ) -> DemandPlannerIdleSpeedProbeStatus {
        let mut activity = 0u64;
        let mut demand_count = 0usize;
        for snapshot in snapshots {
            if snapshot.subscriber_count == 0 {
                continue;
            }
            activity = activity.saturating_add(snapshot.metrics.activity_bps_or_bytes());
            if snapshot.metrics.wants_idle_speed_probe_for(snapshot.demand) {
                demand_count = demand_count.saturating_add(1);
            }
        }

        if demand_count == 0 {
            self.current_multiplier = 1;
            self.idle_since = None;
            self.decay_since = None;
            return DemandPlannerIdleSpeedProbeStatus::default();
        }

        if activity > 0 {
            self.idle_since = None;
            self.decay_after_activity(now);
            return self.status(demand_count);
        }

        self.decay_since = None;
        let idle_since = *self.idle_since.get_or_insert(now);
        let idle_age = now.saturating_duration_since(idle_since);
        let idle_multiplier = if idle_age >= DHT_IDLE_SPEED_PROBE_4X_MIN_IDLE {
            4
        } else if idle_age >= DHT_IDLE_SPEED_PROBE_3X_MIN_IDLE {
            3
        } else if idle_age >= DHT_IDLE_SPEED_PROBE_2X_MIN_IDLE {
            2
        } else {
            1
        };
        self.current_multiplier = self.current_multiplier.max(idle_multiplier);
        self.status(demand_count)
    }

    fn decay_after_activity(&mut self, now: Instant) {
        if self.current_multiplier <= 1 {
            self.current_multiplier = 1;
            self.decay_since = None;
            return;
        }

        let mut decay_since = *self.decay_since.get_or_insert(now);
        while self.current_multiplier > 1 {
            let Some(next_decay_at) = decay_since.checked_add(DHT_IDLE_SPEED_PROBE_DECAY_INTERVAL)
            else {
                self.current_multiplier = 1;
                self.decay_since = None;
                return;
            };
            if now < next_decay_at {
                break;
            }
            self.current_multiplier = self.current_multiplier.saturating_sub(1).max(1);
            decay_since = next_decay_at;
        }
        self.decay_since = (self.current_multiplier > 1).then_some(decay_since);
    }

    fn status(&self, demand_count: usize) -> DemandPlannerIdleSpeedProbeStatus {
        let multiplier = self.current_multiplier.max(1);
        DemandPlannerIdleSpeedProbeStatus {
            active: multiplier > 1,
            demand_count,
            multiplier,
        }
    }
}

#[derive(Debug)]
pub(in crate::dht::service) struct DemandPlannerModel {
    pub(in crate::dht::service) scheduler: DemandScheduler,
    pub(in crate::dht::service) active: HashMap<InfoHash, ActiveDemandLookup>,
    pub(in crate::dht::service) pending_starts: HashMap<InfoHash, DemandSliceClass>,
    pub(in crate::dht::service) pending_parks: HashMap<InfoHash, DemandSliceClass>,
    pub(in crate::dht::service) parked_crawls: HashMap<InfoHash, DemandCrawlState>,
    pub(in crate::dht::service) draining_demands: HashMap<InfoHash, DrainingDemandLookup>,
    pub(in crate::dht::service) state: HashMap<InfoHash, DemandPlannerState>,
    pub(in crate::dht::service) budget: DemandPlannerBudget,
    pub(in crate::dht::service) idle_speed_probe: DemandPlannerIdleSpeedProbe,
}

impl DemandPlannerModel {
    pub(in crate::dht::service) fn new(now: Instant) -> Self {
        Self {
            scheduler: DemandScheduler::new(
                DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL,
                DHT_NO_CONNECTED_PEERS_BASE_INTERVAL,
                DHT_NO_CONNECTED_PEERS_MAX_INTERVAL,
                DHT_AWAITING_METADATA_REFRESH_INTERVAL,
            ),
            active: HashMap::new(),
            pending_starts: HashMap::new(),
            pending_parks: HashMap::new(),
            parked_crawls: HashMap::new(),
            draining_demands: HashMap::new(),
            state: HashMap::new(),
            budget: DemandPlannerBudget::new(now),
            idle_speed_probe: DemandPlannerIdleSpeedProbe::default(),
        }
    }

    pub(in crate::dht::service) fn has_draining_demands(&self) -> bool {
        !self.draining_demands.is_empty()
    }

    pub(in crate::dht::service) fn metadata_waiter_count(&self) -> usize {
        self.scheduler
            .entry_snapshots()
            .into_iter()
            .filter(|snapshot| {
                snapshot.demand.is_awaiting_metadata() && snapshot.subscriber_count > 0
            })
            .count()
    }

    pub(in crate::dht::service) fn entry_snapshot(
        &self,
        info_hash: InfoHash,
    ) -> Option<DemandEntrySnapshot> {
        self.scheduler.entry_snapshot(info_hash)
    }
}

#[derive(Debug)]
pub(in crate::dht::service) struct DemandCrawlState {
    pub(in crate::dht::service) ipv4: Option<LookupState>,
    pub(in crate::dht::service) ipv6: Option<LookupState>,
    pub(in crate::dht::service) class: DemandSliceClass,
    pub(in crate::dht::service) updated_at: Instant,
    pub(in crate::dht::service) reset_count: u32,
    pub(in crate::dht::service) consecutive_stalled_low_yield_slices: u32,
    pub(in crate::dht::service) consecutive_healthy_zero_yield_slices: u32,
}

impl DemandCrawlState {
    pub(in crate::dht::service) fn new(now: Instant, class: DemandSliceClass) -> Self {
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

    pub(in crate::dht::service) fn take_family_state(
        &mut self,
        family: AddressFamily,
    ) -> Option<LookupState> {
        let state = match family {
            AddressFamily::Ipv4 => self.ipv4.take(),
            AddressFamily::Ipv6 => self.ipv6.take(),
        };
        if state.is_some() {
            self.updated_at = Instant::now();
        }
        state
    }

    pub(in crate::dht::service) fn store_family_state(
        &mut self,
        class: DemandSliceClass,
        state: LookupState,
    ) {
        match state.family() {
            AddressFamily::Ipv4 => self.ipv4 = Some(state),
            AddressFamily::Ipv6 => self.ipv6 = Some(state),
        }
        self.class = class;
        self.updated_at = Instant::now();
    }

    pub(in crate::dht::service) fn is_empty(&self) -> bool {
        self.ipv4.is_none() && self.ipv6.is_none()
    }

    pub(in crate::dht::service) fn is_stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.updated_at) >= DHT_PARKED_CRAWL_MAX_AGE
    }

    pub(in crate::dht::service) fn reset_reason_for(
        &self,
        class: DemandSliceClass,
        now: Instant,
    ) -> Option<DemandCrawlResetReason> {
        if self.is_stale(now) {
            Some(DemandCrawlResetReason::Stale)
        } else if self.class == class
            && (self.consecutive_stalled_low_yield_slices
                >= class.stalled_empty_slice_reset_threshold()
                || self.consecutive_healthy_zero_yield_slices
                    >= class.stalled_empty_slice_reset_threshold())
        {
            Some(DemandCrawlResetReason::LowQuality)
        } else {
            None
        }
    }

    pub(in crate::dht::service) fn should_reset_for(
        &self,
        class: DemandSliceClass,
        now: Instant,
    ) -> bool {
        self.reset_reason_for(class, now).is_some()
    }

    pub(in crate::dht::service) fn reset_for(&mut self, class: DemandSliceClass, now: Instant) {
        self.ipv4 = None;
        self.ipv6 = None;
        self.class = class;
        self.updated_at = now;
        self.reset_count = self.reset_count.saturating_add(1);
        self.consecutive_stalled_low_yield_slices = 0;
        self.consecutive_healthy_zero_yield_slices = 0;
    }

    pub(in crate::dht::service) fn observe_parked_slice(
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
pub(in crate::dht::service) enum DemandSliceClass {
    RoutineRefresh,
    NoConnectedPeers,
    AwaitingMetadata,
}

impl DemandSliceClass {
    pub(in crate::dht::service) fn from_demand(demand: DhtDemandState) -> Self {
        if demand.is_awaiting_metadata() {
            Self::AwaitingMetadata
        } else if demand.has_no_connected_peers() {
            Self::NoConnectedPeers
        } else {
            Self::RoutineRefresh
        }
    }

    pub(in crate::dht::service) fn stalled_empty_slice_reset_threshold(self) -> u32 {
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

    pub(in crate::dht::service) fn stalled_low_yield_slice_max_unique_peers(self) -> usize {
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

    pub(in crate::dht::service) fn parked_slice_outcome(
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

    pub(in crate::dht::service) fn parked_quality_is_weak(
        self,
        snapshot: AggregateLookupQualitySnapshot,
    ) -> bool {
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
pub(in crate::dht::service) struct AggregateLookupQualitySnapshot {
    pub(in crate::dht::service) frontier_len: usize,
    pub(in crate::dht::service) inflight_len: usize,
    pub(in crate::dht::service) visited_len: usize,
    pub(in crate::dht::service) eligible_responder_count: usize,
    pub(in crate::dht::service) received_peer_count: usize,
}

impl AggregateLookupQualitySnapshot {
    pub(in crate::dht::service) fn extend(&mut self, snapshot: LookupQualitySnapshot) {
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

pub(in crate::dht::service) fn aggregate_parked_crawl_quality(
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
pub(in crate::dht::service) enum DemandSliceStopReason {
    NaturalFinish,
    WallTime,
    IdleTimeout,
    FirstBatch,
    UniquePeerCap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dht::service) enum DemandParkedSliceOutcome {
    UsefulYield,
    WeakLowYield,
    HealthyZeroYield,
    HealthyLowYield,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dht::service) enum DemandCrawlResetReason {
    Stale,
    ClassChanged,
    LowQuality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dht::service) enum DemandSelectionReason {
    ReusableParked,
    SwarmSupport,
    UsefulYieldHistory,
    Fairness,
    OverdueScarce,
    SpareCapacity,
    IdleSpeedProbe,
}

#[derive(Debug, Clone, Default)]
pub(in crate::dht::service) struct DemandSliceClassMetrics {
    pub(in crate::dht::service) fresh_starts: u64,
    pub(in crate::dht::service) resumed_starts: u64,
    pub(in crate::dht::service) selected_reusable_parked: u64,
    pub(in crate::dht::service) selected_swarm_support: u64,
    pub(in crate::dht::service) selected_useful_yield_history: u64,
    pub(in crate::dht::service) selected_fairness: u64,
    pub(in crate::dht::service) selected_overdue_scarce: u64,
    pub(in crate::dht::service) selected_spare_capacity: u64,
    pub(in crate::dht::service) selected_idle_speed_probe: u64,
    pub(in crate::dht::service) natural_finishes: u64,
    pub(in crate::dht::service) wall_time_stops: u64,
    pub(in crate::dht::service) idle_timeout_stops: u64,
    pub(in crate::dht::service) first_batch_stops: u64,
    pub(in crate::dht::service) unique_peer_cap_stops: u64,
    pub(in crate::dht::service) peers_yielded: u64,
    pub(in crate::dht::service) unique_peers_yielded: u64,
    pub(in crate::dht::service) stale_resets: u64,
    pub(in crate::dht::service) class_change_resets: u64,
    pub(in crate::dht::service) low_quality_resets: u64,
}

#[derive(Debug, Clone, Default)]
pub(in crate::dht::service) struct DemandSliceMetrics {
    pub(in crate::dht::service) awaiting_metadata: DemandSliceClassMetrics,
    pub(in crate::dht::service) no_connected_peers: DemandSliceClassMetrics,
    pub(in crate::dht::service) routine_refresh: DemandSliceClassMetrics,
}

impl DemandSliceMetrics {
    pub(in crate::dht::service) fn class_mut(
        &mut self,
        class: DemandSliceClass,
    ) -> &mut DemandSliceClassMetrics {
        match class {
            DemandSliceClass::AwaitingMetadata => &mut self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &mut self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &mut self.routine_refresh,
        }
    }

    pub(in crate::dht::service) fn class_ref(
        &self,
        class: DemandSliceClass,
    ) -> &DemandSliceClassMetrics {
        match class {
            DemandSliceClass::AwaitingMetadata => &self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &self.routine_refresh,
        }
    }

    pub(in crate::dht::service) fn record_start(&mut self, class: DemandSliceClass, resumed: bool) {
        let metrics = self.class_mut(class);
        if resumed {
            metrics.resumed_starts = metrics.resumed_starts.saturating_add(1);
        } else {
            metrics.fresh_starts = metrics.fresh_starts.saturating_add(1);
        }
    }

    pub(in crate::dht::service) fn record_selection(
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
            DemandSelectionReason::SwarmSupport => {
                metrics.selected_swarm_support = metrics.selected_swarm_support.saturating_add(1)
            }
            DemandSelectionReason::UsefulYieldHistory => {
                metrics.selected_useful_yield_history =
                    metrics.selected_useful_yield_history.saturating_add(1)
            }
            DemandSelectionReason::Fairness => {
                metrics.selected_fairness = metrics.selected_fairness.saturating_add(1)
            }
            DemandSelectionReason::OverdueScarce => {
                metrics.selected_overdue_scarce = metrics.selected_overdue_scarce.saturating_add(1)
            }
            DemandSelectionReason::SpareCapacity => {
                metrics.selected_spare_capacity = metrics.selected_spare_capacity.saturating_add(1)
            }
            DemandSelectionReason::IdleSpeedProbe => {
                metrics.selected_idle_speed_probe =
                    metrics.selected_idle_speed_probe.saturating_add(1)
            }
        }
    }

    pub(in crate::dht::service) fn record_stop(
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

    pub(in crate::dht::service) fn record_reset(
        &mut self,
        class: DemandSliceClass,
        reason: DemandCrawlResetReason,
    ) {
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

    pub(in crate::dht::service) fn has_activity(&self) -> bool {
        for class in [
            DemandSliceClass::AwaitingMetadata,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceClass::RoutineRefresh,
        ] {
            let metrics = self.class_ref(class);
            if metrics.fresh_starts > 0
                || metrics.resumed_starts > 0
                || metrics.selected_reusable_parked > 0
                || metrics.selected_swarm_support > 0
                || metrics.selected_useful_yield_history > 0
                || metrics.selected_fairness > 0
                || metrics.selected_overdue_scarce > 0
                || metrics.selected_spare_capacity > 0
                || metrics.selected_idle_speed_probe > 0
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

    pub(in crate::dht::service) fn summary(&self) -> String {
        fn fmt(label: &str, metrics: &DemandSliceClassMetrics) -> String {
            format!(
                "{label}(fresh={} resumed={} sel_reuse={} sel_support={} sel_yield={} sel_fair={} sel_due={} sel_spare={} sel_idle_probe={} natural={} wall={} idle={} first={} cap={} peers={} unique={} reset_stale={} reset_class={} reset_quality={})",
                metrics.fresh_starts,
                metrics.resumed_starts,
                metrics.selected_reusable_parked,
                metrics.selected_swarm_support,
                metrics.selected_useful_yield_history,
                metrics.selected_fairness,
                metrics.selected_overdue_scarce,
                metrics.selected_spare_capacity,
                metrics.selected_idle_speed_probe,
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

pub(in crate::dht::service) fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Copy)]
pub(in crate::dht::service) struct DemandLookupPlan {
    pub(in crate::dht::service) class: DemandSliceClass,
    pub(in crate::dht::service) idle_timeout: Duration,
    pub(in crate::dht::service) max_wall_time: Duration,
    pub(in crate::dht::service) stop_after_first_batch: bool,
    pub(in crate::dht::service) unique_peer_cap: usize,
    pub(in crate::dht::service) power_multiplier: u8,
}

impl DemandLookupPlan {
    pub(in crate::dht::service) fn for_demand(demand: DhtDemandState) -> Self {
        Self::for_demand_with_metrics(demand, DhtDemandMetrics::default())
    }

    pub(in crate::dht::service) fn for_demand_with_metrics(
        demand: DhtDemandState,
        metrics: DhtDemandMetrics,
    ) -> Self {
        match DemandSliceClass::from_demand(demand) {
            DemandSliceClass::AwaitingMetadata => Self {
                class: DemandSliceClass::AwaitingMetadata,
                idle_timeout: DHT_AWAITING_METADATA_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_AWAITING_METADATA_SLICE_WALL_TIME,
                stop_after_first_batch: false,
                unique_peer_cap: DHT_AWAITING_METADATA_SLICE_UNIQUE_PEER_CAP,
                power_multiplier: 1,
            },
            DemandSliceClass::NoConnectedPeers => Self {
                class: DemandSliceClass::NoConnectedPeers,
                idle_timeout: DHT_NO_CONNECTED_PEERS_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_NO_CONNECTED_PEERS_SLICE_WALL_TIME,
                stop_after_first_batch: false,
                unique_peer_cap: DHT_NO_CONNECTED_PEERS_SLICE_UNIQUE_PEER_CAP,
                power_multiplier: 1,
            },
            DemandSliceClass::RoutineRefresh if metrics.wants_extended_routine_search() => Self {
                class: DemandSliceClass::RoutineRefresh,
                idle_timeout: DHT_ROUTINE_SUPPORT_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_ROUTINE_SUPPORT_SLICE_WALL_TIME,
                stop_after_first_batch: false,
                unique_peer_cap: DHT_ROUTINE_SUPPORT_SLICE_UNIQUE_PEER_CAP,
                power_multiplier: 1,
            },
            DemandSliceClass::RoutineRefresh => Self {
                class: DemandSliceClass::RoutineRefresh,
                idle_timeout: DHT_ROUTINE_SLICE_IDLE_TIMEOUT,
                max_wall_time: DHT_ROUTINE_SLICE_WALL_TIME,
                stop_after_first_batch: true,
                unique_peer_cap: DHT_ROUTINE_SLICE_UNIQUE_PEER_CAP,
                power_multiplier: 1,
            },
        }
    }

    pub(in crate::dht::service) fn for_candidate(
        candidate: DueDemandCandidate,
        planner_state: &HashMap<InfoHash, DemandPlannerState>,
        selection_reason: DemandSelectionReason,
        idle_probe: DemandPlannerIdleSpeedProbeStatus,
        now: Instant,
    ) -> Self {
        Self::for_demand_with_metrics(candidate.demand, candidate.metrics).with_power_multiplier(
            demand_lookup_power_multiplier(
                candidate,
                planner_state,
                selection_reason,
                idle_probe,
                now,
            ),
        )
    }

    fn with_power_multiplier(mut self, multiplier: u8) -> Self {
        let multiplier = multiplier.max(1);
        self.power_multiplier = multiplier;
        if multiplier > 1 {
            let multiplier = u32::from(multiplier);
            self.max_wall_time = multiply_duration(self.max_wall_time, multiplier);
            self.unique_peer_cap = self.unique_peer_cap.saturating_mul(multiplier as usize);
        }
        self
    }
}

fn multiply_duration(duration: Duration, multiplier: u32) -> Duration {
    duration.checked_mul(multiplier).unwrap_or(Duration::MAX)
}

fn demand_lookup_power_multiplier(
    candidate: DueDemandCandidate,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    selection_reason: DemandSelectionReason,
    idle_probe: DemandPlannerIdleSpeedProbeStatus,
    now: Instant,
) -> u8 {
    let class = DemandSliceClass::from_demand(candidate.demand);
    let idle_probe_multiplier = if idle_probe.active
        && candidate
            .metrics
            .wants_idle_speed_probe_for(candidate.demand)
    {
        idle_probe.multiplier
    } else {
        1
    };

    if class == DemandSliceClass::AwaitingMetadata {
        return 2.max(idle_probe_multiplier);
    }
    if class == DemandSliceClass::RoutineRefresh
        && candidate.metrics.wants_extended_routine_search()
    {
        return 2.max(idle_probe_multiplier);
    }

    if !matches!(selection_reason, DemandSelectionReason::UsefulYieldHistory) {
        return idle_probe_multiplier;
    }

    let Some(state) = planner_state.get(&candidate.info_hash) else {
        return idle_probe_multiplier;
    };
    let Some(last_yield_at) = state.last_useful_yield_at else {
        return idle_probe_multiplier;
    };
    let age = now.saturating_duration_since(last_yield_at);
    if age > DHT_DEMAND_USEFUL_YIELD_BOOST_MAX_AGE || state.last_unique_peers == 0 {
        return idle_probe_multiplier;
    }
    let yield_multiplier = if age <= DHT_DEMAND_STRONG_YIELD_BOOST_MAX_AGE
        && state.last_unique_peers >= DHT_DEMAND_STRONG_YIELD_BOOST_MIN_UNIQUE_PEERS
    {
        3
    } else {
        2
    };
    yield_multiplier.max(idle_probe_multiplier)
}
