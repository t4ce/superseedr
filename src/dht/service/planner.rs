// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

mod types;
pub(super) use types::*;

mod selection;
pub(super) use selection::*;

mod drain;
pub(super) use drain::*;

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
