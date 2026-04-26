// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

#[derive(Debug)]
pub(in crate::dht::service) enum LoopEvent {
    Shutdown,
    Command(DhtCommand),
    DrainTick,
    DemandTick,
    MaintenanceTick,
    HealthTick,
    RuntimeStep(Result<bool, String>),
    CommandClosed,
}

pub(in crate::dht::service) fn command_event(maybe_command: Option<DhtCommand>) -> LoopEvent {
    match maybe_command {
        Some(command) => LoopEvent::Command(command),
        None => LoopEvent::CommandClosed,
    }
}

pub(in crate::dht::service) async fn run_service(
    config: DhtServiceConfig,
    local_node_id: NodeId,
    mut active_runtime: Option<ActiveRuntime>,
    warning: Option<String>,
    status_tx: watch::Sender<DhtStatus>,
    wave_telemetry_tx: watch::Sender<DhtWaveTelemetry>,
    command_tx: DhtCommandSender,
    mut command_rx: DhtCommandReceiver,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut demand_tick = tokio::time::interval(DHT_DEMAND_SCHEDULER_INTERVAL);
    demand_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut drain_interval = tokio::time::interval(DHT_DEMAND_DRAIN_POLL_INTERVAL);
    drain_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut maintenance_interval = tokio::time::interval(DHT_MAINTENANCE_INTERVAL);
    let mut health_interval = tokio::time::interval(DHT_HEALTH_REFRESH_INTERVAL);
    let mut service_state = DhtServiceState::new(config, status_tx.borrow().generation, warning);

    loop {
        if let Some(active) = active_runtime.as_ref() {
            if let Some(due) = active.startup_bootstrap_due {
                let reduction =
                    DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
                        now: Instant::now(),
                        due,
                        active_user_lookup_count: active.runtime.active_user_lookup_count(),
                    });
                apply_dht_lifecycle_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &status_tx,
                    &command_tx,
                )
                .await;
            }
        }

        let event = if let Some(active) = active_runtime.as_mut() {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => LoopEvent::Shutdown,
                _ = drain_interval.tick(), if service_state.has_draining_demands() => LoopEvent::DrainTick,
                maybe_command = command_rx.recv() => command_event(maybe_command),
                _ = demand_tick.tick() => LoopEvent::DemandTick,
                _ = maintenance_interval.tick() => LoopEvent::MaintenanceTick,
                _ = health_interval.tick() => LoopEvent::HealthTick,
                step_result = active.runtime.step() => LoopEvent::RuntimeStep(step_result.map_err(|error| error.to_string())),
            }
        } else {
            tokio::select! {
                _ = shutdown_rx.recv() => LoopEvent::Shutdown,
                _ = drain_interval.tick(), if service_state.has_draining_demands() => LoopEvent::DrainTick,
                maybe_command = command_rx.recv() => command_event(maybe_command),
                _ = demand_tick.tick() => LoopEvent::DemandTick,
                _ = maintenance_interval.tick() => LoopEvent::MaintenanceTick,
                _ = health_interval.tick() => LoopEvent::HealthTick,
            }
        };

        match event {
            LoopEvent::Shutdown | LoopEvent::CommandClosed => {
                let reduction = DhtLifecycleModel::update(DhtLifecycleAction::Shutdown);
                apply_dht_lifecycle_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &status_tx,
                    &command_tx,
                )
                .await;
                break;
            }
            LoopEvent::Command(DhtCommand::Reconfigure(new_config)) => {
                let reduction = match build_runtime(&new_config, local_node_id).await {
                    Ok(built) => {
                        if let Some(previous) = active_runtime.as_ref() {
                            let _ = previous.runtime.save_state().await;
                        }
                        active_runtime = built.active_runtime;
                        service_state
                            .service
                            .update(DhtServiceAction::ReconfigureSucceeded {
                                config: new_config,
                                warning: built.warning,
                            })
                    }
                    Err(error) => service_state
                        .service
                        .update(DhtServiceAction::ReconfigureFailed { warning: error }),
                };
                apply_dht_service_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &status_tx,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::RegisterDemand {
                info_hash,
                demand,
                subscriber_tx,
                response_tx,
            }) => {
                let reduction =
                    service_state.update_demand_command(DhtDemandCommandAction::Register {
                        info_hash,
                        demand,
                        subscriber_tx,
                        response_tx,
                    });
                apply_dht_demand_command_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UpdateDemand { info_hash, demand }) => {
                let reduction =
                    service_state.update_demand_command(DhtDemandCommandAction::Update {
                        info_hash,
                        demand,
                        now: Instant::now(),
                    });
                apply_dht_demand_command_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UnregisterDemand {
                info_hash,
                subscriber_id,
            }) => {
                let reduction =
                    service_state.update_demand_command(DhtDemandCommandAction::Unregister {
                        info_hash,
                        subscriber_id,
                    });
                apply_dht_demand_command_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::DemandPeers { info_hash, peers }) => {
                let reduction =
                    service_state.update_demand_command(DhtDemandCommandAction::PeersReceived {
                        info_hash,
                        peers,
                    });
                apply_dht_demand_command_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::DemandLookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
            }) => {
                let reduction =
                    service_state.update_demand_command(DhtDemandCommandAction::LookupFinished {
                        info_hash,
                        slice_class,
                        total_peers,
                        unique_peers,
                        now: Instant::now(),
                    });
                apply_dht_demand_command_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::StartGetPeers {
                info_hash,
                response_tx,
            }) => {
                let reduction =
                    DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::StartGetPeers {
                        info_hash,
                        response_tx,
                    });
                apply_dht_runtime_command_effects(
                    reduction.effects,
                    &mut active_runtime,
                    &command_tx,
                    &mut service_state,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::StartGetPeersFamily {
                info_hash,
                family,
                slice_class,
                record_metrics,
                merged_tx,
                lookup_ids,
                first_batch_seen,
                accepting_families,
            }) => {
                let reduction = DhtRuntimeCommandModel::update(
                    DhtRuntimeCommandAction::StartGetPeersFamily(DhtRuntimeLookupFamilyRequest {
                        info_hash,
                        family,
                        slice_class,
                        record_metrics,
                        merged_tx,
                        lookup_ids,
                        first_batch_seen,
                        accepting_families,
                    }),
                );
                apply_dht_runtime_command_effects(
                    reduction.effects,
                    &mut active_runtime,
                    &command_tx,
                    &mut service_state,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::CancelLookups { lookup_ids }) => {
                let reduction =
                    DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::CancelLookups {
                        lookup_ids,
                    });
                apply_dht_runtime_command_effects(
                    reduction.effects,
                    &mut active_runtime,
                    &command_tx,
                    &mut service_state,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::ParkDemandLookups {
                info_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers,
                lookup_ids,
            }) => {
                let reduction =
                    DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::ParkDemandLookups {
                        info_hash,
                        slice_class,
                        stop_reason,
                        total_peers,
                        unique_peers,
                        lookup_ids,
                    });
                apply_dht_runtime_command_effects(
                    reduction.effects,
                    &mut active_runtime,
                    &command_tx,
                    &mut service_state,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::FinalizeDrainedDemandLookups { info_hash }) => {
                let reduction = DhtRuntimeCommandModel::update(
                    DhtRuntimeCommandAction::FinalizeDrainedDemandLookups { info_hash },
                );
                apply_dht_runtime_command_effects(
                    reduction.effects,
                    &mut active_runtime,
                    &command_tx,
                    &mut service_state,
                )
                .await;
            }
            LoopEvent::DrainTick => {
                let runtime_ready = service_state
                    .demand_planner
                    .drain_runtime_readiness(active_runtime.as_ref());
                let reduction =
                    service_state
                        .demand_planner
                        .update(DemandPlannerAction::DrainTick {
                            now: Instant::now(),
                            runtime_ready,
                        });
                let finalized_any = apply_demand_planner_effects_for_state(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut service_state,
                    reduction.effects,
                );
                if finalized_any {
                    start_due_demands_for_state(
                        &mut active_runtime,
                        &command_tx,
                        &mut service_state,
                    )
                    .await;
                }
            }
            LoopEvent::Command(DhtCommand::AnnouncePeer {
                info_hash,
                port,
                response_tx,
            }) => {
                let reduction =
                    DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::AnnouncePeer {
                        info_hash,
                        port,
                        response_tx,
                    });
                apply_dht_runtime_command_effects(
                    reduction.effects,
                    &mut active_runtime,
                    &command_tx,
                    &mut service_state,
                )
                .await;
            }
            LoopEvent::DemandTick => {
                start_due_demands_for_state(&mut active_runtime, &command_tx, &mut service_state)
                    .await;
            }
            LoopEvent::MaintenanceTick => {
                let reduction = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
                    active_user_lookup_count: active_runtime
                        .as_ref()
                        .map(|active| active.runtime.active_user_lookup_count()),
                });
                apply_dht_lifecycle_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &status_tx,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::HealthTick => {
                let reduction = DhtLifecycleModel::update(DhtLifecycleAction::HealthTick);
                apply_dht_lifecycle_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &status_tx,
                    &command_tx,
                )
                .await;
            }
            LoopEvent::RuntimeStep(Ok(_)) => {}
            LoopEvent::RuntimeStep(Err(error)) => {
                let reduction = DhtLifecycleModel::update(DhtLifecycleAction::RuntimeStepFailed {
                    warning: format!("DHT runtime step failed: {error}"),
                });
                apply_dht_lifecycle_effects(
                    reduction.effects,
                    &mut service_state,
                    &mut active_runtime,
                    &status_tx,
                    &command_tx,
                )
                .await;
            }
        }

        publish_wave_telemetry(
            &wave_telemetry_tx,
            active_runtime.as_ref(),
            &mut service_state.recent_unique_peers,
        );
    }
}
