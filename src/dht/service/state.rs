// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::net::SocketAddr;
use std::time::Instant;

use super::{
    DemandPlannerAction, DemandPlannerEffect, DemandPlannerModel, DemandSliceClass,
    DemandSliceMetrics, DemandSubscriberAction, DemandSubscriberEffect, DemandSubscriberRegistry,
    DhtDemandState, DhtServiceConfig, InfoHash, RecentUniquePeers, DHT_UNIQUE_PEERS_FOUND_WINDOW,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub(super) enum DhtServiceAction {
    ReconfigureSucceeded {
        config: DhtServiceConfig,
        warning: Option<String>,
    },
    ReconfigureFailed {
        warning: String,
    },
    RuntimeWarning {
        warning: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DhtServiceEffect {
    ResetDemandPlanner,
    PublishStatus,
    StartDueDemands,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct DhtServiceReduction {
    pub(super) effects: Vec<DhtServiceEffect>,
}

pub(in crate::dht::service) enum DhtDemandCommandAction {
    Register {
        info_hash: InfoHash,
        demand: DhtDemandState,
        subscriber_tx: mpsc::UnboundedSender<Vec<SocketAddr>>,
        response_tx: oneshot::Sender<Option<u64>>,
    },
    Update {
        info_hash: InfoHash,
        demand: DhtDemandState,
        now: Instant,
    },
    Unregister {
        info_hash: InfoHash,
        subscriber_id: u64,
    },
    PeersReceived {
        info_hash: InfoHash,
        peers: Vec<SocketAddr>,
    },
    LookupFinished {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        total_peers: usize,
        unique_peers: usize,
        now: Instant,
    },
}

pub(in crate::dht::service) enum DhtDemandCommandEffect {
    SendRegisterResponse {
        response_tx: oneshot::Sender<Option<u64>>,
        subscriber_id: Option<u64>,
    },
    ApplySubscriberEffects(Vec<DemandSubscriberEffect>),
    ApplyPlannerEffects(Vec<DemandPlannerEffect>),
    StartDueDemands,
}

#[derive(Default)]
pub(in crate::dht::service) struct DhtDemandCommandReduction {
    pub(in crate::dht::service) effects: Vec<DhtDemandCommandEffect>,
}

#[derive(Debug)]
pub(super) struct DhtServiceModel {
    config: DhtServiceConfig,
    generation: u64,
    warning: Option<String>,
}

impl DhtServiceModel {
    pub(super) fn new(config: DhtServiceConfig, generation: u64, warning: Option<String>) -> Self {
        Self {
            config,
            generation,
            warning,
        }
    }

    pub(super) fn config(&self) -> &DhtServiceConfig {
        &self.config
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn warning_owned(&self) -> Option<String> {
        self.warning.clone()
    }

    pub(super) fn update(&mut self, action: DhtServiceAction) -> DhtServiceReduction {
        match action {
            DhtServiceAction::ReconfigureSucceeded { config, warning } => {
                self.config = config;
                self.generation = self.generation.saturating_add(1);
                self.warning = warning;
                DhtServiceReduction {
                    effects: vec![
                        DhtServiceEffect::ResetDemandPlanner,
                        DhtServiceEffect::PublishStatus,
                        DhtServiceEffect::StartDueDemands,
                    ],
                }
            }
            DhtServiceAction::ReconfigureFailed { warning } => {
                self.warning = Some(warning);
                DhtServiceReduction {
                    effects: vec![
                        DhtServiceEffect::ResetDemandPlanner,
                        DhtServiceEffect::PublishStatus,
                        DhtServiceEffect::StartDueDemands,
                    ],
                }
            }
            DhtServiceAction::RuntimeWarning { warning } => {
                self.warning = Some(warning);
                DhtServiceReduction {
                    effects: vec![DhtServiceEffect::PublishStatus],
                }
            }
        }
    }
}

pub(super) struct DhtServiceState {
    pub(super) service: DhtServiceModel,
    pub(super) demand_planner: DemandPlannerModel,
    pub(super) demand_subscribers: DemandSubscriberRegistry,
    pub(super) slice_metrics: DemandSliceMetrics,
    pub(super) recent_unique_peers: RecentUniquePeers,
}

impl DhtServiceState {
    pub(super) fn new(config: DhtServiceConfig, generation: u64, warning: Option<String>) -> Self {
        Self {
            service: DhtServiceModel::new(config, generation, warning),
            demand_planner: DemandPlannerModel::new(Instant::now()),
            demand_subscribers: DemandSubscriberRegistry::new(),
            slice_metrics: DemandSliceMetrics::default(),
            recent_unique_peers: RecentUniquePeers::new(DHT_UNIQUE_PEERS_FOUND_WINDOW),
        }
    }

    pub(super) fn has_draining_demands(&self) -> bool {
        self.demand_planner.has_draining_demands()
    }

    pub(super) fn record_recent_peers(&mut self, peers: &[SocketAddr]) {
        self.recent_unique_peers.record_batch(Instant::now(), peers);
    }

    pub(super) fn expire_recent_peers(&mut self) {
        let _ = self.recent_unique_peers.unique_count(Instant::now());
    }

    pub(in crate::dht::service) fn update_demand_command(
        &mut self,
        action: DhtDemandCommandAction,
    ) -> DhtDemandCommandReduction {
        match action {
            DhtDemandCommandAction::Register {
                info_hash,
                demand,
                subscriber_tx,
                response_tx,
            } => {
                let reduction = self
                    .demand_subscribers
                    .update(DemandSubscriberAction::Register {
                        info_hash,
                        demand,
                        subscriber_tx,
                    });
                DhtDemandCommandReduction {
                    effects: vec![
                        DhtDemandCommandEffect::SendRegisterResponse {
                            response_tx,
                            subscriber_id: reduction.subscriber_id,
                        },
                        DhtDemandCommandEffect::ApplySubscriberEffects(reduction.effects),
                        DhtDemandCommandEffect::StartDueDemands,
                    ],
                }
            }
            DhtDemandCommandAction::Update {
                info_hash,
                demand,
                now,
            } => {
                let reduction = self
                    .demand_planner
                    .update(DemandPlannerAction::DemandUpdated {
                        info_hash,
                        demand,
                        now,
                    });
                DhtDemandCommandReduction {
                    effects: vec![
                        DhtDemandCommandEffect::ApplyPlannerEffects(reduction.effects),
                        DhtDemandCommandEffect::StartDueDemands,
                    ],
                }
            }
            DhtDemandCommandAction::Unregister {
                info_hash,
                subscriber_id,
            } => {
                let reduction =
                    self.demand_subscribers
                        .update(DemandSubscriberAction::Unregister {
                            info_hash,
                            subscriber_id,
                        });
                DhtDemandCommandReduction {
                    effects: vec![DhtDemandCommandEffect::ApplySubscriberEffects(
                        reduction.effects,
                    )],
                }
            }
            DhtDemandCommandAction::PeersReceived { info_hash, peers } => {
                self.record_recent_peers(&peers);
                let planner_reduction =
                    self.demand_planner
                        .update(DemandPlannerAction::PeersReceived {
                            info_hash,
                            peers: &peers,
                        });
                let subscriber_reduction = self
                    .demand_subscribers
                    .update(DemandSubscriberAction::DeliverPeers { info_hash, peers });
                DhtDemandCommandReduction {
                    effects: vec![
                        DhtDemandCommandEffect::ApplyPlannerEffects(planner_reduction.effects),
                        DhtDemandCommandEffect::ApplySubscriberEffects(
                            subscriber_reduction.effects,
                        ),
                    ],
                }
            }
            DhtDemandCommandAction::LookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
                now,
            } => {
                let reduction = self
                    .demand_planner
                    .update(DemandPlannerAction::LookupFinished {
                        info_hash,
                        slice_class,
                        total_peers,
                        unique_peers,
                        now,
                    });
                DhtDemandCommandReduction {
                    effects: vec![
                        DhtDemandCommandEffect::ApplyPlannerEffects(reduction.effects),
                        DhtDemandCommandEffect::StartDueDemands,
                    ],
                }
            }
        }
    }
}
