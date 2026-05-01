// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

mod types;
pub(super) use types::*;

mod selection;
pub(super) use selection::*;

mod drain;
pub(super) use drain::*;

mod invariants;
pub(super) use invariants::*;

#[cfg(test)]
#[path = "planner/test_support.rs"]
mod test_support;

#[cfg(test)]
#[path = "planner/selection_tests.rs"]
mod selection_tests;

#[cfg(test)]
#[path = "planner/drain_tests.rs"]
mod drain_tests;

#[cfg(test)]
#[path = "planner/reducer_tests.rs"]
mod reducer_tests;

#[cfg(test)]
#[path = "planner/invariant_tests.rs"]
mod invariant_tests;

#[cfg(test)]
#[path = "planner/replay_tests.rs"]
mod replay_tests;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DemandPlannerActionView {
    pub(super) kind: &'static str,
    pub(super) info_hash: Option<InfoHash>,
    pub(super) demand_class: Option<DemandSliceClass>,
    pub(super) demand_awaiting_metadata: Option<bool>,
    pub(super) demand_connected_peers: Option<usize>,
    pub(super) slice_class: Option<DemandSliceClass>,
    pub(super) peer_count: Option<usize>,
    pub(super) total_peers: Option<usize>,
    pub(super) unique_peers: Option<usize>,
    pub(super) runtime_available: Option<bool>,
    pub(super) runtime_ready_count: Option<usize>,
    pub(super) stop_reason: Option<DemandSliceStopReason>,
    pub(super) metrics_paused: Option<bool>,
    pub(super) metrics_accepting_new_peers: Option<bool>,
    pub(super) metrics_complete: Option<bool>,
    pub(super) metrics_total_pieces: Option<u32>,
    pub(super) metrics_completed_pieces: Option<u32>,
    pub(super) metrics_connected_peers: Option<usize>,
    pub(super) metrics_interested_peers: Option<usize>,
    pub(super) metrics_peers_interested_in_us: Option<usize>,
    pub(super) metrics_unchoked_download_peers: Option<usize>,
    pub(super) metrics_unchoked_upload_peers: Option<usize>,
    pub(super) metrics_downloading_peers: Option<usize>,
    pub(super) metrics_uploading_peers: Option<usize>,
    pub(super) metrics_download_speed_bps: Option<u64>,
    pub(super) metrics_upload_speed_bps: Option<u64>,
    pub(super) metrics_bytes_downloaded_this_tick: Option<u64>,
    pub(super) metrics_bytes_uploaded_this_tick: Option<u64>,
    pub(super) metrics_activity: Option<u64>,
    pub(super) metrics_wants_extended_routine: Option<bool>,
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
                ..Self::default()
            }
            .with_demand(*demand),
            DemandPlannerAction::DemandUpdated {
                info_hash, demand, ..
            } => Self {
                kind: "demand_updated",
                info_hash: Some(*info_hash),
                ..Self::default()
            }
            .with_demand(*demand),
            DemandPlannerAction::DemandMetricsUpdated { info_hash, metrics } => Self {
                kind: "demand_metrics_updated",
                info_hash: Some(*info_hash),
                ..Self::default()
            }
            .with_metrics(*metrics),
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

    fn with_demand(mut self, demand: DhtDemandState) -> Self {
        self.demand_class = Some(DemandSliceClass::from_demand(demand));
        self.demand_awaiting_metadata = Some(demand.awaiting_metadata);
        self.demand_connected_peers = Some(demand.connected_peers);
        self
    }

    fn with_metrics(mut self, metrics: DhtDemandMetrics) -> Self {
        self.metrics_paused = Some(metrics.paused);
        self.metrics_accepting_new_peers = Some(metrics.accepting_new_peers);
        self.metrics_complete = Some(metrics.complete);
        self.metrics_total_pieces = Some(metrics.total_pieces);
        self.metrics_completed_pieces = Some(metrics.completed_pieces);
        self.metrics_connected_peers = Some(metrics.connected_peers);
        self.metrics_interested_peers = Some(metrics.interested_peers);
        self.metrics_peers_interested_in_us = Some(metrics.peers_interested_in_us);
        self.metrics_unchoked_download_peers = Some(metrics.unchoked_download_peers);
        self.metrics_unchoked_upload_peers = Some(metrics.unchoked_upload_peers);
        self.metrics_downloading_peers = Some(metrics.downloading_peers);
        self.metrics_uploading_peers = Some(metrics.uploading_peers);
        self.metrics_download_speed_bps = Some(metrics.download_speed_bps);
        self.metrics_upload_speed_bps = Some(metrics.upload_speed_bps);
        self.metrics_bytes_downloaded_this_tick = Some(metrics.bytes_downloaded_this_tick);
        self.metrics_bytes_uploaded_this_tick = Some(metrics.bytes_uploaded_this_tick);
        self.metrics_activity = Some(metrics.activity_bps_or_bytes());
        self.metrics_wants_extended_routine = Some(metrics.wants_extended_routine_search());
        self
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DemandPlannerEffectView {
    pub(super) kind: &'static str,
    pub(super) info_hash: Option<InfoHash>,
    pub(super) demand_class: Option<DemandSliceClass>,
    pub(super) demand_awaiting_metadata: Option<bool>,
    pub(super) demand_connected_peers: Option<usize>,
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
    pub(super) subscriber_count: Option<usize>,
    pub(super) plan_idle_timeout_ms: Option<u64>,
    pub(super) plan_max_wall_time_ms: Option<u64>,
    pub(super) plan_stop_after_first_batch: Option<bool>,
    pub(super) plan_unique_peer_cap: Option<usize>,
    pub(super) plan_power_multiplier: Option<u8>,
    pub(super) metrics_paused: Option<bool>,
    pub(super) metrics_accepting_new_peers: Option<bool>,
    pub(super) metrics_complete: Option<bool>,
    pub(super) metrics_total_pieces: Option<u32>,
    pub(super) metrics_completed_pieces: Option<u32>,
    pub(super) metrics_connected_peers: Option<usize>,
    pub(super) metrics_interested_peers: Option<usize>,
    pub(super) metrics_peers_interested_in_us: Option<usize>,
    pub(super) metrics_unchoked_download_peers: Option<usize>,
    pub(super) metrics_unchoked_upload_peers: Option<usize>,
    pub(super) metrics_downloading_peers: Option<usize>,
    pub(super) metrics_uploading_peers: Option<usize>,
    pub(super) metrics_download_speed_bps: Option<u64>,
    pub(super) metrics_upload_speed_bps: Option<u64>,
    pub(super) metrics_bytes_downloaded_this_tick: Option<u64>,
    pub(super) metrics_bytes_uploaded_this_tick: Option<u64>,
    pub(super) metrics_activity: Option<u64>,
    pub(super) metrics_wants_extended_routine: Option<bool>,
    pub(super) metrics_wants_idle_probe: Option<bool>,
}

impl DemandPlannerEffectView {
    pub(super) fn from_effect(effect: &DemandPlannerEffect) -> Self {
        match effect {
            DemandPlannerEffect::StartLookup(start) => Self {
                kind: "start_lookup",
                info_hash: Some(start.candidate.info_hash),
                slice_class: Some(start.plan.class),
                selection_reason: Some(start.selection_reason),
                subscriber_count: Some(start.candidate.subscriber_count),
                plan_idle_timeout_ms: Some(duration_ms(start.plan.idle_timeout)),
                plan_max_wall_time_ms: Some(duration_ms(start.plan.max_wall_time)),
                plan_stop_after_first_batch: Some(start.plan.stop_after_first_batch),
                plan_unique_peer_cap: Some(start.plan.unique_peer_cap),
                plan_power_multiplier: Some(start.plan.power_multiplier),
                ..Self::default()
            }
            .with_demand(start.candidate.demand)
            .with_metrics(start.candidate.metrics, Some(start.candidate.demand)),
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

    fn with_demand(mut self, demand: DhtDemandState) -> Self {
        self.demand_class = Some(DemandSliceClass::from_demand(demand));
        self.demand_awaiting_metadata = Some(demand.awaiting_metadata);
        self.demand_connected_peers = Some(demand.connected_peers);
        self
    }

    fn with_metrics(mut self, metrics: DhtDemandMetrics, demand: Option<DhtDemandState>) -> Self {
        self.metrics_paused = Some(metrics.paused);
        self.metrics_accepting_new_peers = Some(metrics.accepting_new_peers);
        self.metrics_complete = Some(metrics.complete);
        self.metrics_total_pieces = Some(metrics.total_pieces);
        self.metrics_completed_pieces = Some(metrics.completed_pieces);
        self.metrics_connected_peers = Some(metrics.connected_peers);
        self.metrics_interested_peers = Some(metrics.interested_peers);
        self.metrics_peers_interested_in_us = Some(metrics.peers_interested_in_us);
        self.metrics_unchoked_download_peers = Some(metrics.unchoked_download_peers);
        self.metrics_unchoked_upload_peers = Some(metrics.unchoked_upload_peers);
        self.metrics_downloading_peers = Some(metrics.downloading_peers);
        self.metrics_uploading_peers = Some(metrics.uploading_peers);
        self.metrics_download_speed_bps = Some(metrics.download_speed_bps);
        self.metrics_upload_speed_bps = Some(metrics.upload_speed_bps);
        self.metrics_bytes_downloaded_this_tick = Some(metrics.bytes_downloaded_this_tick);
        self.metrics_bytes_uploaded_this_tick = Some(metrics.bytes_uploaded_this_tick);
        self.metrics_activity = Some(metrics.activity_bps_or_bytes());
        self.metrics_wants_extended_routine = Some(metrics.wants_extended_routine_search());
        self.metrics_wants_idle_probe =
            demand.map(|demand| metrics.wants_idle_speed_probe_for(demand));
        self
    }
}

pub(super) fn dht_actor_monitor_enabled() -> bool {
    false
}

pub(super) fn demand_planner_monitor_enabled() -> bool {
    false
}

pub(super) fn dht_invariant_checks_enabled() -> bool {
    false
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
        demand_awaiting_metadata = ?action.demand_awaiting_metadata,
        demand_connected_peers = ?action.demand_connected_peers,
        slice_class = ?action.slice_class,
        peer_count = ?action.peer_count,
        total_peers = ?action.total_peers,
        unique_peers = ?action.unique_peers,
        runtime_available = ?action.runtime_available,
        runtime_ready_count = ?action.runtime_ready_count,
        stop_reason = ?action.stop_reason,
        metrics_paused = ?action.metrics_paused,
        metrics_accepting_new_peers = ?action.metrics_accepting_new_peers,
        metrics_complete = ?action.metrics_complete,
        metrics_total_pieces = ?action.metrics_total_pieces,
        metrics_completed_pieces = ?action.metrics_completed_pieces,
        metrics_connected_peers = ?action.metrics_connected_peers,
        metrics_interested_peers = ?action.metrics_interested_peers,
        metrics_peers_interested_in_us = ?action.metrics_peers_interested_in_us,
        metrics_unchoked_download_peers = ?action.metrics_unchoked_download_peers,
        metrics_unchoked_upload_peers = ?action.metrics_unchoked_upload_peers,
        metrics_downloading_peers = ?action.metrics_downloading_peers,
        metrics_uploading_peers = ?action.metrics_uploading_peers,
        metrics_download_speed_bps = ?action.metrics_download_speed_bps,
        metrics_upload_speed_bps = ?action.metrics_upload_speed_bps,
        metrics_bytes_downloaded_this_tick = ?action.metrics_bytes_downloaded_this_tick,
        metrics_bytes_uploaded_this_tick = ?action.metrics_bytes_uploaded_this_tick,
        metrics_activity = ?action.metrics_activity,
        metrics_wants_extended_routine = ?action.metrics_wants_extended_routine,
        effect_count = reduction.effects.len(),
        effects = %effect_names,
        plan_launch_budget = ?plan.map(|plan| plan.launch_budget),
        plan_due_total = ?plan.map(|plan| plan.due_total),
        plan_spare_selected = ?plan.map(|plan| plan.spare_selected),
        plan_idle_probe_selected = ?plan.map(|plan| plan.idle_probe_selected),
        plan_idle_probe_active = ?plan.map(|plan| plan.idle_probe_active),
        plan_idle_probe_demand_count = ?plan.map(|plan| plan.idle_probe_demand_count),
        plan_parked_count = ?plan.map(|plan| plan.parked_count),
        plan_draining_count = ?plan.map(|plan| plan.draining_count),
        plan_drain_virtual_slots = ?plan.map(|plan| plan.drain_virtual_slots),
        plan_budget_awaiting = ?plan.map(|plan| plan.budget_awaiting),
        plan_budget_no_peers = ?plan.map(|plan| plan.budget_no_peers),
        plan_budget_routine = ?plan.map(|plan| plan.budget_routine),
        plan_active_awaiting = ?plan.map(|plan| plan.active_counts.awaiting_metadata),
        plan_active_no_peers = ?plan.map(|plan| plan.active_counts.no_connected_peers),
        plan_active_routine = ?plan.map(|plan| plan.active_counts.routine_refresh),
        plan_offered_awaiting = ?plan.map(|plan| plan.selection_stats.offered.awaiting_metadata),
        plan_offered_no_peers = ?plan.map(|plan| plan.selection_stats.offered.no_connected_peers),
        plan_offered_routine = ?plan.map(|plan| plan.selection_stats.offered.routine_refresh),
        plan_launched_awaiting = ?plan.map(|plan| plan.selection_stats.launched.awaiting_metadata),
        plan_launched_no_peers = ?plan.map(|plan| plan.selection_stats.launched.no_connected_peers),
        plan_launched_routine = ?plan.map(|plan| plan.selection_stats.launched.routine_refresh),
        plan_throttled_awaiting = ?plan.map(|plan| plan.selection_stats.throttled.awaiting_metadata),
        plan_throttled_no_peers = ?plan.map(|plan| plan.selection_stats.throttled.no_connected_peers),
        plan_throttled_routine = ?plan.map(|plan| plan.selection_stats.throttled.routine_refresh),
        plan_oldest_throttled_awaiting_ms = ?plan.map(|plan| plan.selection_stats.oldest_throttled_awaiting_ms),
        plan_oldest_throttled_no_peers_ms = ?plan.map(|plan| plan.selection_stats.oldest_throttled_no_peers_ms),
        plan_oldest_throttled_routine_ms = ?plan.map(|plan| plan.selection_stats.oldest_throttled_routine_ms),
        planner_active = model.active.len(),
        planner_draining = model.draining_demands.len(),
        planner_parked = model.parked_crawls.len(),
        planner_scheduler_entries = model.scheduler.entry_snapshots().len(),
        planner_idle_probe_multiplier = ?Some(model.idle_speed_probe.current_multiplier(Instant::now())),
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
        demand_awaiting_metadata = ?view.demand_awaiting_metadata,
        demand_connected_peers = ?view.demand_connected_peers,
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
        subscriber_count = ?view.subscriber_count,
        plan_idle_timeout_ms = ?view.plan_idle_timeout_ms,
        plan_max_wall_time_ms = ?view.plan_max_wall_time_ms,
        plan_stop_after_first_batch = ?view.plan_stop_after_first_batch,
        plan_unique_peer_cap = ?view.plan_unique_peer_cap,
        plan_power_multiplier = ?view.plan_power_multiplier,
        metrics_paused = ?view.metrics_paused,
        metrics_accepting_new_peers = ?view.metrics_accepting_new_peers,
        metrics_complete = ?view.metrics_complete,
        metrics_total_pieces = ?view.metrics_total_pieces,
        metrics_completed_pieces = ?view.metrics_completed_pieces,
        metrics_connected_peers = ?view.metrics_connected_peers,
        metrics_interested_peers = ?view.metrics_interested_peers,
        metrics_peers_interested_in_us = ?view.metrics_peers_interested_in_us,
        metrics_unchoked_download_peers = ?view.metrics_unchoked_download_peers,
        metrics_unchoked_upload_peers = ?view.metrics_unchoked_upload_peers,
        metrics_downloading_peers = ?view.metrics_downloading_peers,
        metrics_uploading_peers = ?view.metrics_uploading_peers,
        metrics_download_speed_bps = ?view.metrics_download_speed_bps,
        metrics_upload_speed_bps = ?view.metrics_upload_speed_bps,
        metrics_bytes_downloaded_this_tick = ?view.metrics_bytes_downloaded_this_tick,
        metrics_bytes_uploaded_this_tick = ?view.metrics_bytes_uploaded_this_tick,
        metrics_activity = ?view.metrics_activity,
        metrics_wants_extended_routine = ?view.metrics_wants_extended_routine,
        metrics_wants_idle_probe = ?view.metrics_wants_idle_probe,
        "DHT planner effect observed",
    );
}

impl DemandPlannerModel {
    pub(super) fn update(&mut self, action: DemandPlannerAction<'_>) -> DemandPlannerReduction {
        let action_view = DemandPlannerActionView::from_action(&action);
        let reduction = {
            let demand_scheduler = &mut self.scheduler;
            let demand_lookup_ids = &mut self.active;
            let pending_starts = &mut self.pending_starts;
            let pending_parks = &mut self.pending_parks;
            let parked_crawls = &mut self.parked_crawls;
            let draining_demands = &mut self.draining_demands;
            let planner_state = &mut self.state;
            let planner_budget = &mut self.budget;
            let idle_speed_probe = &mut self.idle_speed_probe;

            match action {
                DemandPlannerAction::RuntimeReset { now } => {
                    demand_scheduler.reset_active(now);
                    demand_lookup_ids.clear();
                    pending_starts.clear();
                    pending_parks.clear();
                    parked_crawls.clear();
                    draining_demands.clear();
                    planner_state.clear();
                    *planner_budget = DemandPlannerBudget::new(now);
                    *idle_speed_probe = DemandPlannerIdleSpeedProbe::default();
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
                DemandPlannerAction::DemandMetricsUpdated { info_hash, metrics } => {
                    demand_scheduler.update_metrics(info_hash, metrics);
                    DemandPlannerReduction::default()
                }
                DemandPlannerAction::DemandSubscriberRemoved { info_hash } => {
                    let slice_class = demand_scheduler
                        .demand_state(info_hash)
                        .map(DemandSliceClass::from_demand)
                        .unwrap_or(DemandSliceClass::RoutineRefresh);
                    let mut effects = Vec::new();
                    if demand_scheduler.unregister(info_hash) {
                        pending_starts.remove(&info_hash);
                        pending_parks.remove(&info_hash);
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
                            let idle_probe = idle_speed_probe.observe(&demand_snapshots, now);
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

                            let mut planned_counts = active_counts;
                            let mut excluded = HashSet::new();
                            for (candidate, _) in &planned_launches {
                                planned_counts
                                    .record(DemandSliceClass::from_demand(candidate.demand));
                                excluded.insert(candidate.info_hash);
                            }
                            let idle_probe_selected = if idle_probe.active
                                && planned_launches.len() < launch_budget
                            {
                                let remaining_budget =
                                    launch_budget.saturating_sub(planned_launches.len());
                                let launches = select_idle_speed_probe_launches(
                                    &demand_snapshots,
                                    planned_counts,
                                    &excluded,
                                    parked_crawls,
                                    planner_state,
                                    planner_budget,
                                    now,
                                    remaining_budget,
                                );
                                let selected_count = launches.len();
                                planned_launches.extend(launches.into_iter().map(|candidate| {
                                    (candidate, DemandSelectionReason::IdleSpeedProbe)
                                }));
                                selected_count
                            } else {
                                0
                            };

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
                                let plan = DemandLookupPlan::for_candidate(
                                    candidate,
                                    planner_state,
                                    selection_reason,
                                    idle_probe,
                                    now,
                                );
                                if !demand_scheduler.mark_in_progress(candidate.info_hash) {
                                    planner_budget.refund(plan.class);
                                    continue;
                                }
                                planner_state
                                    .entry(candidate.info_hash)
                                    .or_default()
                                    .note_start(now);
                                pending_starts.insert(candidate.info_hash, plan.class);
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
                                    idle_probe_selected,
                                    idle_probe_active: idle_probe.active,
                                    idle_probe_demand_count: idle_probe.demand_count,
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
                    pending_starts.remove(&info_hash);
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
                    pending_starts.remove(&info_hash);
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
                    pending_parks.insert(info_hash, slice_class);
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
                    pending_parks.remove(&info_hash);
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
        observe_demand_planner_invariants(action_view.kind, self);
        trace_demand_planner_reduction(action_view, &reduction, self);
        reduction
    }
}
