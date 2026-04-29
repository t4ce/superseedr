// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::super::{observe_action_effect_reduction, DhtServiceConfig};

#[derive(Debug)]
pub(in crate::dht::service) enum DhtServiceAction {
    ReconfigureRequested {
        config: DhtServiceConfig,
    },
    ReconfigureSucceeded {
        config: DhtServiceConfig,
        warning: Option<String>,
    },
    ReconfigureFailed {
        warning: String,
        runtime_reset: bool,
    },
    RuntimeWarning {
        warning: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::dht::service) enum DhtServiceEffect {
    BuildRuntime { config: DhtServiceConfig },
    ResetDemandPlanner,
    PublishStatus,
    StartDueDemands,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::dht::service) struct DhtServiceReduction {
    pub(in crate::dht::service) effects: Vec<DhtServiceEffect>,
}

#[derive(Debug)]
pub(in crate::dht::service) struct DhtServiceModel {
    config: DhtServiceConfig,
    generation: u64,
    warning: Option<String>,
}

impl DhtServiceModel {
    pub(in crate::dht::service) fn new(
        config: DhtServiceConfig,
        generation: u64,
        warning: Option<String>,
    ) -> Self {
        Self {
            config,
            generation,
            warning,
        }
    }

    pub(in crate::dht::service) fn config(&self) -> &DhtServiceConfig {
        &self.config
    }

    pub(in crate::dht::service) fn generation(&self) -> u64 {
        self.generation
    }

    pub(in crate::dht::service) fn warning_owned(&self) -> Option<String> {
        self.warning.clone()
    }

    pub(in crate::dht::service) fn update(
        &mut self,
        action: DhtServiceAction,
    ) -> DhtServiceReduction {
        let action_kind = action.kind();
        let reduction = match action {
            DhtServiceAction::ReconfigureRequested { config } => DhtServiceReduction {
                effects: vec![DhtServiceEffect::BuildRuntime { config }],
            },
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
            DhtServiceAction::ReconfigureFailed {
                warning,
                runtime_reset,
            } => {
                self.warning = Some(warning);
                let effects = if runtime_reset {
                    vec![
                        DhtServiceEffect::ResetDemandPlanner,
                        DhtServiceEffect::PublishStatus,
                        DhtServiceEffect::StartDueDemands,
                    ]
                } else {
                    vec![DhtServiceEffect::PublishStatus]
                };
                DhtServiceReduction { effects }
            }
            DhtServiceAction::RuntimeWarning { warning } => {
                self.warning = Some(warning);
                DhtServiceReduction {
                    effects: vec![DhtServiceEffect::PublishStatus],
                }
            }
        };
        observe_action_effect_reduction(
            "service",
            action_kind,
            reduction.effects.iter().map(DhtServiceEffect::kind),
        );
        reduction
    }
}

impl DhtServiceAction {
    fn kind(&self) -> &'static str {
        match self {
            DhtServiceAction::ReconfigureRequested { .. } => "reconfigure_requested",
            DhtServiceAction::ReconfigureSucceeded { .. } => "reconfigure_succeeded",
            DhtServiceAction::ReconfigureFailed { .. } => "reconfigure_failed",
            DhtServiceAction::RuntimeWarning { .. } => "runtime_warning",
        }
    }
}

impl DhtServiceEffect {
    fn kind(&self) -> &'static str {
        match self {
            DhtServiceEffect::BuildRuntime { .. } => "build_runtime",
            DhtServiceEffect::ResetDemandPlanner => "reset_demand_planner",
            DhtServiceEffect::PublishStatus => "publish_status",
            DhtServiceEffect::StartDueDemands => "start_due_demands",
        }
    }
}
