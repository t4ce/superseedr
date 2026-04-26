// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

pub(in crate::dht::service) async fn start_due_demands_for_state(
    active_runtime: &mut Option<ActiveRuntime>,
    command_tx: &DhtCommandSender,
    service_state: &mut DhtServiceState,
) {
    start_due_demands(
        active_runtime.as_mut(),
        command_tx,
        &mut service_state.demand_planner,
        &mut service_state.slice_metrics,
    )
    .await;
}

pub(in crate::dht::service) fn apply_demand_planner_effects_for_state(
    active_runtime: Option<&mut ActiveRuntime>,
    command_tx: &DhtCommandSender,
    service_state: &mut DhtServiceState,
    effects: Vec<DemandPlannerEffect>,
) -> bool {
    apply_demand_planner_effects(
        active_runtime,
        &mut service_state.demand_planner,
        command_tx,
        &mut service_state.slice_metrics,
        effects,
    )
}

pub(in crate::dht::service) async fn apply_dht_service_effects(
    effects: Vec<DhtServiceEffect>,
    service_state: &mut DhtServiceState,
    active_runtime: &mut Option<ActiveRuntime>,
    status_tx: &watch::Sender<DhtStatus>,
    command_tx: &DhtCommandSender,
) {
    for effect in effects {
        match effect {
            DhtServiceEffect::ResetDemandPlanner => {
                service_state
                    .demand_planner
                    .update(DemandPlannerAction::RuntimeReset {
                        now: Instant::now(),
                    });
            }
            DhtServiceEffect::PublishStatus => {
                publish_status(
                    status_tx,
                    active_runtime.as_ref(),
                    service_state.service.warning_owned(),
                    service_state.service.generation(),
                    service_state.service.config().preferred_backend,
                );
            }
            DhtServiceEffect::StartDueDemands => {
                start_due_demands_for_state(active_runtime, command_tx, service_state).await;
            }
        }
    }
}

pub(in crate::dht::service) async fn apply_dht_lifecycle_effects(
    effects: Vec<DhtLifecycleEffect>,
    service_state: &mut DhtServiceState,
    active_runtime: &mut Option<ActiveRuntime>,
    status_tx: &watch::Sender<DhtStatus>,
    command_tx: &DhtCommandSender,
) {
    let mut pending_effects = VecDeque::from(effects);

    while let Some(effect) = pending_effects.pop_front() {
        match effect {
            DhtLifecycleEffect::RunStartupBootstrap => {
                if let Some(active) = active_runtime.as_mut() {
                    let reduction = match active.runtime.bootstrap_startup().await {
                        Ok(()) => {
                            DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapSucceeded)
                        }
                        Err(error) => {
                            DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapFailed {
                                warning: format!("DHT startup bootstrap failed: {error}"),
                                retry_at: Instant::now() + DHT_STARTUP_BOOTSTRAP_DELAY,
                            })
                        }
                    };
                    pending_effects.extend(reduction.effects);
                }
            }
            DhtLifecycleEffect::ClearStartupBootstrapDue => {
                if let Some(active) = active_runtime.as_mut() {
                    active.startup_bootstrap_due = None;
                }
            }
            DhtLifecycleEffect::SetStartupBootstrapDue(due) => {
                if let Some(active) = active_runtime.as_mut() {
                    active.startup_bootstrap_due = Some(due);
                }
            }
            DhtLifecycleEffect::RunMaintenance => {
                if let Some(active) = active_runtime.as_mut() {
                    if let Err(error) = active.runtime.run_maintenance().await {
                        let reduction =
                            DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceFailed {
                                warning: format!("DHT maintenance failed: {error}"),
                            });
                        pending_effects.extend(reduction.effects);
                    }
                }
            }
            DhtLifecycleEffect::RecordRuntimeWarning {
                warning,
                publish_status,
            } => {
                let reduction = service_state
                    .service
                    .update(DhtServiceAction::RuntimeWarning { warning });
                if publish_status {
                    apply_dht_service_effects(
                        reduction.effects,
                        service_state,
                        active_runtime,
                        status_tx,
                        command_tx,
                    )
                    .await;
                }
            }
            DhtLifecycleEffect::PublishStatus => {
                publish_status(
                    status_tx,
                    active_runtime.as_ref(),
                    service_state.service.warning_owned(),
                    service_state.service.generation(),
                    service_state.service.config().preferred_backend,
                );
            }
            DhtLifecycleEffect::ExpireRecentUniquePeers => {
                service_state.expire_recent_peers();
            }
            DhtLifecycleEffect::SaveRuntimeState => {
                if let Some(active) = active_runtime.as_ref() {
                    let _ = active.runtime.save_state().await;
                }
            }
        }
    }
}

pub(in crate::dht::service) fn apply_demand_subscriber_effects(
    service_state: &mut DhtServiceState,
    mut active_runtime: Option<&mut ActiveRuntime>,
    command_tx: &DhtCommandSender,
    effects: Vec<DemandSubscriberEffect>,
) {
    let DhtServiceState {
        demand_planner,
        demand_subscribers,
        slice_metrics,
        ..
    } = service_state;
    let mut pending_effects = VecDeque::from(effects);

    while let Some(effect) = pending_effects.pop_front() {
        match effect {
            DemandSubscriberEffect::Registered {
                info_hash,
                demand,
                subscriber_id,
            } => {
                let _ = subscriber_id;
                let reduction = demand_planner.update(DemandPlannerAction::DemandRegistered {
                    info_hash,
                    demand,
                    now: Instant::now(),
                });
                apply_demand_planner_effects(
                    active_runtime.as_deref_mut(),
                    demand_planner,
                    command_tx,
                    slice_metrics,
                    reduction.effects,
                );
            }
            DemandSubscriberEffect::SubscriberRemoved { info_hash } => {
                let reduction = demand_planner
                    .update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
                apply_demand_planner_effects(
                    active_runtime.as_deref_mut(),
                    demand_planner,
                    command_tx,
                    slice_metrics,
                    reduction.effects,
                );
            }
            DemandSubscriberEffect::DeliverPeers {
                info_hash,
                peers,
                deliveries,
            } => {
                let dead_subscribers = deliveries
                    .into_iter()
                    .filter_map(|delivery| {
                        delivery
                            .subscriber_tx
                            .send(peers.clone())
                            .is_err()
                            .then_some(delivery.subscriber_id)
                    })
                    .collect::<Vec<_>>();
                if !dead_subscribers.is_empty() {
                    let reduction =
                        demand_subscribers.update(DemandSubscriberAction::PruneDeadSubscribers {
                            info_hash,
                            subscriber_ids: dead_subscribers,
                        });
                    pending_effects.extend(reduction.effects);
                }
            }
        }
    }
}

pub(in crate::dht::service) async fn apply_dht_runtime_command_effects(
    effects: Vec<DhtRuntimeCommandEffect>,
    active_runtime: &mut Option<ActiveRuntime>,
    command_tx: &DhtCommandSender,
    service_state: &mut DhtServiceState,
) {
    for effect in effects {
        match effect {
            DhtRuntimeCommandEffect::StartGetPeers {
                info_hash,
                response_tx,
            } => {
                let result = start_get_peers_lookup(
                    active_runtime.as_mut(),
                    command_tx,
                    &mut service_state.demand_planner,
                    None,
                    info_hash,
                    DemandSliceClass::RoutineRefresh,
                    false,
                )
                .await;
                let _ = response_tx.send(result);
            }
            DhtRuntimeCommandEffect::AttachLookupFamily(request) => {
                let _ = attach_lookup_family(
                    active_runtime.as_mut(),
                    &mut service_state.demand_planner,
                    if request.record_metrics {
                        Some(&mut service_state.slice_metrics)
                    } else {
                        None
                    },
                    request.info_hash,
                    request.family,
                    request.slice_class,
                    request.merged_tx,
                    request.lookup_ids,
                    request.first_batch_seen,
                    request.accepting_families,
                )
                .await;
            }
            DhtRuntimeCommandEffect::CancelLookups { lookup_ids } => {
                if let Some(active_runtime) = active_runtime.as_mut() {
                    for lookup_id in lookup_ids {
                        active_runtime.runtime.cancel_lookup(lookup_id);
                    }
                }
            }
            DhtRuntimeCommandEffect::ParkDemandLookups {
                info_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers,
                lookup_ids,
            } => {
                let requested =
                    service_state
                        .demand_planner
                        .update(DemandPlannerAction::LookupParkRequested {
                            info_hash,
                            slice_class,
                            stop_reason,
                            total_peers,
                            unique_peers,
                            lookup_ids,
                        });
                apply_demand_planner_effects_for_state(
                    active_runtime.as_mut(),
                    command_tx,
                    service_state,
                    requested.effects,
                );
            }
            DhtRuntimeCommandEffect::FinalizeDrainedDemandLookups { info_hash } => {
                finish_drained_demand_lookup(
                    active_runtime.as_mut(),
                    &mut service_state.demand_planner,
                    command_tx,
                    &mut service_state.slice_metrics,
                    info_hash,
                    false,
                );
            }
            DhtRuntimeCommandEffect::AnnouncePeer {
                info_hash,
                port,
                response_tx,
            } => {
                let success = announce_peer(active_runtime.as_mut(), info_hash, port).await;
                let _ = response_tx.send(success);
            }
            DhtRuntimeCommandEffect::StartDueDemands => {
                start_due_demands_for_state(active_runtime, command_tx, service_state).await;
            }
        }
    }
}

pub(in crate::dht::service) fn apply_demand_planner_effects(
    mut active_runtime: Option<&mut ActiveRuntime>,
    demand_planner: &mut DemandPlannerModel,
    command_tx: &DhtCommandSender,
    slice_metrics: &mut DemandSliceMetrics,
    effects: Vec<DemandPlannerEffect>,
) -> bool {
    let mut finalized_any = false;
    let mut pending_effects = VecDeque::from(effects);

    while let Some(effect) = pending_effects.pop_front() {
        trace_demand_planner_effect("apply", &effect);
        match effect {
            DemandPlannerEffect::LookupFinished(finished) => {
                slice_metrics.record_stop(
                    finished.slice_class,
                    DemandSliceStopReason::NaturalFinish,
                    finished.total_peers,
                    finished.unique_peers,
                );
            }
            DemandPlannerEffect::AdmitDrain(admit) => {
                let initial_unique_peers = admit.unique_peers.len();
                let parked_outcome = demand_planner.drain_lookup_ids(
                    active_runtime.as_deref_mut(),
                    command_tx,
                    admit.info_hash,
                    admit.slice_class,
                    admit.stop_reason,
                    admit.total_peers,
                    admit.unique_peers,
                    admit.lookup_ids,
                );
                let drain_admission = demand_planner.drain_admission_snapshot(admit.info_hash);
                let resolved = demand_planner.update(DemandPlannerAction::LookupParkResolved {
                    info_hash: admit.info_hash,
                    slice_class: admit.slice_class,
                    stop_reason: admit.stop_reason,
                    total_peers: admit.total_peers,
                    unique_peers: initial_unique_peers,
                    parked_outcome,
                    drain_admission,
                    previous: admit.previous,
                    now: Instant::now(),
                });
                pending_effects.extend(resolved.effects);
            }
            DemandPlannerEffect::LookupParked(parked) => {
                if parked.drain_admission.is_none() {
                    slice_metrics.record_stop(
                        parked.slice_class,
                        parked.stop_reason,
                        parked.total_peers,
                        parked.unique_peers,
                    );
                }
            }
            DemandPlannerEffect::DrainFinalized(finalized) => {
                slice_metrics.record_stop(
                    finalized.outcome.slice_class,
                    finalized.outcome.stop_reason,
                    finalized.outcome.total_peers,
                    finalized.outcome.unique_peers,
                );
            }
            DemandPlannerEffect::DrainPeersRecorded(recorded) => {
                let _ = recorded.info_hash;
                let _ = recorded.peer_count;
                let _ = recorded.unique_added;
                let _ = recorded.initial_unique_peers;
            }
            DemandPlannerEffect::FinalizeDrainingLookup(effect) => {
                finalized_any |= finish_drained_demand_lookup(
                    active_runtime.as_deref_mut(),
                    demand_planner,
                    command_tx,
                    slice_metrics,
                    effect.info_hash,
                    effect.force,
                );
            }
            DemandPlannerEffect::StartLookup(_) => {
                debug_assert!(
                    false,
                    "start lookup effects must be handled by start_due_demands"
                );
            }
            DemandPlannerEffect::ParkActiveLookup(effect) => {
                demand_planner.park_lookup_ids(
                    active_runtime.as_deref_mut(),
                    effect.info_hash,
                    effect.slice_class,
                    None,
                    0,
                    effect.lookup_ids,
                );
            }
            DemandPlannerEffect::CancelDrainingLookup(effect) => {
                let _ = effect.info_hash;
                if let Some(active_runtime) = active_runtime.as_deref_mut() {
                    for lookup_id in effect.lookup_ids {
                        active_runtime.runtime.cancel_lookup(lookup_id);
                    }
                }
            }
        }
    }

    finalized_any
}

pub(in crate::dht::service) fn finish_drained_demand_lookup(
    active_runtime: Option<&mut ActiveRuntime>,
    demand_planner: &mut DemandPlannerModel,
    command_tx: &DhtCommandSender,
    slice_metrics: &mut DemandSliceMetrics,
    info_hash: InfoHash,
    force: bool,
) -> bool {
    let previous = demand_planner.entry_snapshot(info_hash);
    let Some(outcome) =
        demand_planner.finalize_drained_lookup(active_runtime, command_tx, info_hash, force)
    else {
        return false;
    };

    let now = Instant::now();
    let reduction = demand_planner.update(DemandPlannerAction::DrainedLookupFinalized {
        info_hash,
        outcome,
        previous,
        now,
    });
    apply_demand_planner_effects(
        None,
        demand_planner,
        command_tx,
        slice_metrics,
        reduction.effects,
    );

    true
}

pub(in crate::dht::service) async fn start_due_demands(
    mut active_runtime: Option<&mut ActiveRuntime>,
    command_tx: &DhtCommandSender,
    demand_planner: &mut DemandPlannerModel,
    slice_metrics: &mut DemandSliceMetrics,
) {
    let now = Instant::now();
    let runtime_available = active_runtime.is_some();
    let reduction = demand_planner.update(DemandPlannerAction::PlanDue {
        now,
        runtime_available,
    });
    for effect in reduction.effects {
        trace_demand_planner_effect("apply", &effect);
        let DemandPlannerEffect::StartLookup(start) = effect else {
            continue;
        };
        let candidate = start.candidate;
        let info_hash = candidate.info_hash;
        let plan = start.plan;
        slice_metrics.record_selection(plan.class, start.selection_reason);
        match start_get_peers_lookup(
            active_runtime.as_mut().map(|active| &mut **active),
            command_tx,
            demand_planner,
            Some(slice_metrics),
            info_hash,
            plan.class,
            true,
        )
        .await
        {
            Ok(started) => {
                demand_planner.update(DemandPlannerAction::LookupStarted {
                    info_hash,
                    slice_class: plan.class,
                    lookup_ids: started.lookup_ids.clone(),
                });
                let mut receiver = started.receiver;
                let command_tx = command_tx.clone();
                let lookup_ids = started.lookup_ids.clone();
                let accepting_families = started.accepting_families.clone();
                tokio::spawn(async move {
                    let mut idle_sleep = Box::pin(tokio::time::sleep(plan.idle_timeout));
                    let overall_sleep = tokio::time::sleep(plan.max_wall_time);
                    tokio::pin!(overall_sleep);
                    let mut unique_peers = HashSet::new();
                    let mut total_peers = 0usize;
                    let mut stop_reason = None;

                    loop {
                        tokio::select! {
                            _ = &mut overall_sleep => {
                                stop_reason = Some(DemandSliceStopReason::WallTime);
                                break;
                            }
                            _ = &mut idle_sleep => {
                                stop_reason = Some(DemandSliceStopReason::IdleTimeout);
                                break;
                            }
                            maybe_peers = receiver.recv() => {
                                let Some(peers) = maybe_peers else {
                                    break;
                                };
                                total_peers = total_peers.saturating_add(peers.len());
                                for peer in &peers {
                                    unique_peers.insert(*peer);
                                }
                                let _ = send_dht_command(
                                    &command_tx,
                                    DhtCommand::DemandPeers { info_hash, peers },
                                );
                                if plan.stop_after_first_batch {
                                    stop_reason = Some(DemandSliceStopReason::FirstBatch);
                                    break;
                                }
                                if unique_peers.len() >= plan.unique_peer_cap {
                                    stop_reason = Some(DemandSliceStopReason::UniquePeerCap);
                                    break;
                                }
                                idle_sleep
                                    .as_mut()
                                    .reset(tokio::time::Instant::now() + plan.idle_timeout);
                            }
                        }
                    }

                    if let Some(reason) = stop_reason {
                        accepting_families.store(false, Ordering::Release);
                        let _ = send_dht_command(
                            &command_tx,
                            DhtCommand::ParkDemandLookups {
                                info_hash,
                                slice_class: plan.class,
                                stop_reason: reason,
                                total_peers,
                                unique_peers,
                                lookup_ids,
                            },
                        );
                        let drain_sleep = tokio::time::sleep(
                            DHT_DEMAND_DRAIN_MAX_AGE + DHT_DEMAND_DRAIN_POLL_INTERVAL,
                        );
                        tokio::pin!(drain_sleep);
                        loop {
                            tokio::select! {
                                _ = &mut drain_sleep => break,
                                maybe_peers = receiver.recv() => {
                                    let Some(peers) = maybe_peers else {
                                        break;
                                    };
                                    let _ = send_dht_command(&command_tx, DhtCommand::DemandPeers {
                                        info_hash,
                                        peers,
                                    });
                                }
                            }
                        }
                    } else {
                        let unique_peer_count = unique_peers.len();
                        let _ = send_dht_command(
                            &command_tx,
                            DhtCommand::DemandLookupFinished {
                                info_hash,
                                slice_class: plan.class,
                                total_peers,
                                unique_peers: unique_peer_count,
                            },
                        );
                    }
                });
            }
            Err(_) => {
                demand_planner.update(DemandPlannerAction::LookupStartFailed {
                    info_hash,
                    slice_class: plan.class,
                    now: Instant::now(),
                });
            }
        }
    }
}
