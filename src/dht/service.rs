// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::lookup::LookupQualitySnapshot;
use super::persist::{PersistenceConfig, PersistenceManager};
pub use super::scheduler::DhtDemandState;
use super::scheduler::{
    DemandEntrySnapshot, DemandFinishMode, DemandScheduler, DueDemandCandidate,
};
use super::types::{AddressFamily, InfoHash, LookupId, NodeId};
use super::{LookupState, Runtime, RuntimeConfig};
use crate::config::{self, Settings};
use rand::random;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::lookup_host;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

mod api;
mod commands;
mod driver;
mod effects;
mod lifecycle;
mod planner;
mod runtime;
mod state;
mod status;
mod subscribers;

#[cfg(test)]
pub(crate) use self::api::TestDhtRecorder;
pub use self::api::{
    configured_status_from_settings, DhtDemandSubscription, DhtHandle, DhtLookupRun, DhtService,
};
pub(in crate::dht::service) use self::api::{
    send_dht_command, DhtCommand, DhtCommandReceiver, DhtCommandSender, DhtDemandSubscriptionInner,
};
use self::commands::{
    DhtRuntimeCommandAction, DhtRuntimeCommandEffect, DhtRuntimeCommandModel,
    DhtRuntimeLookupFamilyRequest,
};
pub(in crate::dht::service) use self::driver::{command_event, run_service, LoopEvent};
use self::effects::*;
use self::lifecycle::{DhtLifecycleAction, DhtLifecycleEffect, DhtLifecycleModel};
use self::planner::*;
pub(super) use self::runtime::*;
use self::state::{DhtServiceAction, DhtServiceEffect, DhtServiceModel, DhtServiceState};
use self::status::*;
use self::subscribers::{DemandSubscriberAction, DemandSubscriberEffect, DemandSubscriberRegistry};

const DHT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL: Duration = DHT_MAINTENANCE_INTERVAL;
const DHT_NO_CONNECTED_PEERS_BASE_INTERVAL: Duration = Duration::from_secs(16);
const DHT_NO_CONNECTED_PEERS_MAX_INTERVAL: Duration = Duration::from_secs(5 * 60);
const DHT_AWAITING_METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DHT_HEALTH_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const DHT_DEMAND_SCHEDULER_INTERVAL: Duration = Duration::from_millis(250);
const DHT_DEMAND_LOOKUP_SLOT_COUNT: usize = 8;
const DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK: usize = 4;
const DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT: usize = 16;
const DHT_PLANNER_TOKEN_SCALE: u64 = 1_000;
const DHT_AWAITING_METADATA_LAUNCHES_PER_MINUTE: u64 = 30;
const DHT_AWAITING_METADATA_LAUNCH_BURST: u64 = 8;
const DHT_NO_CONNECTED_PEERS_LAUNCHES_PER_MINUTE: u64 = 30;
const DHT_NO_CONNECTED_PEERS_LAUNCH_BURST: u64 = 10;
const DHT_ROUTINE_REFRESH_LAUNCHES_PER_MINUTE: u64 = 5;
const DHT_ROUTINE_REFRESH_LAUNCH_BURST: u64 = 5;
const DHT_DEMAND_FAIRNESS_AGE: Duration = Duration::from_secs(10 * 60);
const DHT_DEMAND_SPARE_RESEARCH_MAX_ACTIVE: usize = 1;
const DHT_DEMAND_SPARE_RESEARCH_LAUNCH_LIMIT: usize = 1;
const DHT_DEMAND_SPARE_RESEARCH_MIN_INTERVAL: Duration = Duration::from_secs(20);
const DHT_AWAITING_METADATA_SLOT_CAP: usize = DHT_DEMAND_LOOKUP_SLOT_COUNT;
const DHT_NO_CONNECTED_PEERS_SLOT_CAP: usize = 7;
const DHT_ROUTINE_LOOKUP_SLOT_CAP: usize = 2;
const DHT_PERSISTENCE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DHT_STARTUP_BOOTSTRAP_DELAY: Duration = Duration::from_secs(5);
const DHT_IPV6_HEDGE_DELAY: Duration = Duration::from_millis(750);
const DHT_LOOKUP_BOOTSTRAP_WAIT: Duration = Duration::from_secs(2);
const DHT_UNIQUE_PEERS_FOUND_WINDOW: Duration = Duration::from_secs(10);
const DHT_PARKED_CRAWL_MAX_AGE: Duration = Duration::from_secs(5 * 60);
const DHT_DEMAND_DRAIN_MAX_AGE: Duration = Duration::from_secs(5);
const DHT_DEMAND_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DHT_DEMAND_DRAIN_MAX_INFLIGHT_QUERIES: usize = 192;
const DHT_DEMAND_DRAIN_NO_LATE_YIELD_GRACE: Duration = Duration::from_millis(1500);
const DHT_AWAITING_METADATA_DRAIN_NO_LATE_YIELD_GRACE: Duration = Duration::from_secs(2);
const DHT_ROUTINE_DRAIN_NO_LATE_YIELD_GRACE: Duration = Duration::from_millis(750);
const DHT_PLANNER_MONITOR_ENV: &str = "SUPERSEEDR_DHT_PLANNER_TRACE";
const DHT_AWAITING_METADATA_SLICE_WALL_TIME: Duration = Duration::from_secs(6);
const DHT_AWAITING_METADATA_SLICE_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const DHT_NO_CONNECTED_PEERS_SLICE_WALL_TIME: Duration = Duration::from_secs(4);
const DHT_NO_CONNECTED_PEERS_SLICE_IDLE_TIMEOUT: Duration = Duration::from_millis(1500);
const DHT_ROUTINE_SLICE_WALL_TIME: Duration = Duration::from_secs(2);
const DHT_ROUTINE_SLICE_IDLE_TIMEOUT: Duration = Duration::from_millis(750);
const DHT_AWAITING_METADATA_SLICE_UNIQUE_PEER_CAP: usize = 128;
const DHT_NO_CONNECTED_PEERS_SLICE_UNIQUE_PEER_CAP: usize = 48;
const DHT_ROUTINE_SLICE_UNIQUE_PEER_CAP: usize = 16;
const DHT_AWAITING_METADATA_STALLED_EMPTY_SLICE_RESET_THRESHOLD: u32 = 4;
const DHT_NO_CONNECTED_PEERS_STALLED_EMPTY_SLICE_RESET_THRESHOLD: u32 = 3;
const DHT_ROUTINE_STALLED_EMPTY_SLICE_RESET_THRESHOLD: u32 = 2;
const DHT_AWAITING_METADATA_STALLED_LOW_YIELD_SLICE_MAX_UNIQUE_PEERS: usize = 0;
const DHT_NO_CONNECTED_PEERS_STALLED_LOW_YIELD_SLICE_MAX_UNIQUE_PEERS: usize = 2;
const DHT_ROUTINE_STALLED_LOW_YIELD_SLICE_MAX_UNIQUE_PEERS: usize = 1;
const DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MIN_VISITED: usize = 12;
const DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MAX_RESPONDERS: usize = 3;
const DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MAX_FRONTIER: usize = 8;
const DHT_NO_CONNECTED_PEERS_WEAK_PARKED_MAX_RECEIVED_PEERS: usize = 12;
const DHT_ROUTINE_WEAK_PARKED_MIN_VISITED: usize = 8;
const DHT_ROUTINE_WEAK_PARKED_MAX_RESPONDERS: usize = 1;
const DHT_ROUTINE_WEAK_PARKED_MAX_FRONTIER: usize = 4;
const DHT_ROUTINE_WEAK_PARKED_MAX_RECEIVED_PEERS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DhtBackendKind {
    #[default]
    Disabled,
    Mainline,
    InternalPrototype,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtServiceConfig {
    pub port: u16,
    pub bootstrap_nodes: Vec<String>,
    pub preferred_backend: DhtBackendKind,
    #[cfg(test)]
    pub force_internal_failure: bool,
}

impl DhtServiceConfig {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            port: settings.client_port,
            bootstrap_nodes: settings.bootstrap_nodes.clone(),
            preferred_backend: std::env::var("SUPERSEEDR_DHT_BACKEND")
                .ok()
                .as_deref()
                .and_then(DhtBackendKind::from_override)
                .unwrap_or(DhtBackendKind::InternalPrototype),
            #[cfg(test)]
            force_internal_failure: false,
        }
    }
}

impl DhtBackendKind {
    fn from_override(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" => Some(Self::Disabled),
            "mainline" | "compat" => Some(Self::Mainline),
            "internal" | "internal-prototype" | "builtin" => Some(Self::InternalPrototype),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DhtHealthSnapshot {
    pub backend: DhtBackendKind,
    pub preferred_backend: Option<DhtBackendKind>,
    pub recovery_pending: bool,
    pub enabled: bool,
    pub local_addr: Option<SocketAddr>,
    pub ipv4_local_addr: Option<SocketAddr>,
    pub ipv6_local_addr: Option<SocketAddr>,
    pub bound_family_count: usize,
    pub cached_ipv4_routes: usize,
    pub cached_ipv6_routes: usize,
    pub active_ipv4_routes: usize,
    pub active_ipv6_routes: usize,
    pub cached_ipv4_announce_tokens: usize,
    pub cached_ipv6_announce_tokens: usize,
    pub cached_lookup_results: usize,
    pub inflight_lookups: usize,
    pub inflight_ipv4_queries: usize,
    pub inflight_ipv6_queries: usize,
    pub public_addr: Option<SocketAddr>,
    pub firewalled: Option<bool>,
    pub server_mode: Option<bool>,
    pub exported_bootstrap_nodes: usize,
    pub dht_size_estimate: Option<DhtSizeEstimate>,
    pub ipv4_bootstrap_nodes: usize,
    pub ipv6_bootstrap_nodes: usize,
    pub responsive_ipv4_bootstrap_nodes: usize,
    pub responsive_ipv6_bootstrap_nodes: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DhtSizeEstimate {
    pub node_count: usize,
    pub std_dev: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DhtStatus {
    pub generation: u64,
    pub warning: Option<String>,
    pub health: DhtHealthSnapshot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DhtWaveTelemetry {
    pub active_lookups: usize,
    pub active_user_lookups: usize,
    pub inflight_ipv4_queries: usize,
    pub inflight_ipv6_queries: usize,
    pub unique_peers_found_last_10s: usize,
}

#[derive(Debug)]
struct RecentUniquePeers {
    window: Duration,
    events: VecDeque<(Instant, SocketAddr)>,
    last_seen: HashMap<SocketAddr, Instant>,
}

impl RecentUniquePeers {
    fn new(window: Duration) -> Self {
        Self {
            window,
            events: VecDeque::new(),
            last_seen: HashMap::new(),
        }
    }

    fn record_batch(&mut self, now: Instant, peers: &[SocketAddr]) {
        self.evict_expired(now);
        for &peer in peers {
            self.events.push_back((now, peer));
            self.last_seen.insert(peer, now);
        }
    }

    fn evict_expired(&mut self, now: Instant) {
        while let Some((seen_at, peer)) = self.events.front().copied() {
            if now.saturating_duration_since(seen_at) < self.window {
                break;
            }
            self.events.pop_front();
            if self.last_seen.get(&peer).copied() == Some(seen_at) {
                self.last_seen.remove(&peer);
            }
        }
    }

    fn unique_count(&mut self, now: Instant) -> usize {
        self.evict_expired(now);
        self.last_seen.len()
    }
}

fn persistence_config() -> Option<PersistenceConfig> {
    if std::env::var_os("SUPERSEEDR_DHT_DISABLE_PERSISTENCE").is_some()
        || std::env::var_os("SUPERSEEDR_DHT_FRESH_BOOTSTRAP").is_some()
    {
        return None;
    }
    let path = config::runtime_persistence_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("dht_state.json");
    Some(PersistenceConfig {
        path,
        max_age: DHT_PERSISTENCE_MAX_AGE,
    })
}

fn forced_internal_backend_error(config: &DhtServiceConfig) -> Option<String> {
    #[cfg(test)]
    if config.force_internal_failure {
        return Some("forced internal backend failure".to_string());
    }

    let _ = config;
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(addr: &str) -> SocketAddr {
        addr.parse().expect("valid socket address")
    }

    fn hash_index(index: u32) -> InfoHash {
        let mut bytes = [0u8; InfoHash::LEN];
        bytes[..4].copy_from_slice(&index.to_be_bytes());
        InfoHash::from(bytes)
    }

    fn active_lookup(lookup_id: LookupId, class: DemandSliceClass) -> ActiveDemandLookup {
        ActiveDemandLookup {
            lookup_ids: Arc::new(StdMutex::new(vec![lookup_id])),
            slice_class: class,
        }
    }

    fn synthetic_peers(key: u8, count: u8) -> HashSet<SocketAddr> {
        (0..count)
            .map(|index| {
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, key, index, key.wrapping_add(index))),
                    40_000 + u16::from(index),
                )
            })
            .collect()
    }

    fn lookup_state_for_family(
        lookup_id: LookupId,
        family: AddressFamily,
        target_index: u32,
        now: Instant,
    ) -> LookupState {
        let bootstrap = match family {
            AddressFamily::Ipv4 => vec![peer("127.0.0.10:6881")],
            AddressFamily::Ipv6 => vec![peer("[::1]:6881")],
        };
        let routing = crate::dht::routing::RoutingSnapshot {
            family,
            buckets: Vec::new(),
            nodes: Vec::new(),
            replacement_count: 0,
            refresh_due_count: 0,
        };
        crate::dht::lookup::LookupManager::new(crate::dht::lookup::LookupConfig::default()).start(
            crate::dht::lookup::LookupRequest {
                lookup_id,
                kind: crate::dht::lookup::LookupKind::GetPeers,
                target: crate::dht::lookup::LookupTarget::InfoHash(hash_index(target_index)),
            },
            family,
            &routing,
            &bootstrap,
            &[],
            now,
        )
    }

    fn disabled_service_config() -> DhtServiceConfig {
        DhtServiceConfig {
            port: 0,
            bootstrap_nodes: Vec::new(),
            preferred_backend: DhtBackendKind::Disabled,
            force_internal_failure: false,
        }
    }

    fn initial_disabled_status(config: &DhtServiceConfig) -> DhtStatus {
        build_status(
            None,
            DhtBackendKind::Disabled,
            config.preferred_backend,
            None,
            0,
            literal_bootstrap_summary(&config.bootstrap_nodes),
        )
    }

    #[test]
    fn dht_service_model_reconfigure_success_updates_state_and_emits_followups() {
        let initial = DhtServiceConfig {
            port: 6881,
            bootstrap_nodes: vec!["198.51.100.10:6881".to_string()],
            preferred_backend: DhtBackendKind::InternalPrototype,
            force_internal_failure: false,
        };
        let next = DhtServiceConfig {
            port: 6882,
            bootstrap_nodes: vec!["203.0.113.20:6881".to_string()],
            preferred_backend: DhtBackendKind::Disabled,
            force_internal_failure: false,
        };
        let mut model = DhtServiceModel::new(initial, 7, Some("old warning".to_string()));

        let reduction = model.update(DhtServiceAction::ReconfigureSucceeded {
            config: next.clone(),
            warning: None,
        });

        assert_eq!(model.config(), &next);
        assert_eq!(model.generation(), 8);
        assert_eq!(model.warning_owned(), None);
        assert_eq!(
            reduction.effects,
            vec![
                DhtServiceEffect::ResetDemandPlanner,
                DhtServiceEffect::PublishStatus,
                DhtServiceEffect::StartDueDemands,
            ]
        );
    }

    #[test]
    fn dht_service_model_reconfigure_failure_preserves_config_and_generation() {
        let initial = DhtServiceConfig {
            port: 6881,
            bootstrap_nodes: vec!["198.51.100.10:6881".to_string()],
            preferred_backend: DhtBackendKind::InternalPrototype,
            force_internal_failure: false,
        };
        let mut model = DhtServiceModel::new(initial.clone(), 3, None);

        let reduction = model.update(DhtServiceAction::ReconfigureFailed {
            warning: "runtime unavailable".to_string(),
        });

        assert_eq!(model.config(), &initial);
        assert_eq!(model.generation(), 3);
        assert_eq!(
            model.warning_owned().as_deref(),
            Some("runtime unavailable")
        );
        assert_eq!(
            reduction.effects,
            vec![
                DhtServiceEffect::ResetDemandPlanner,
                DhtServiceEffect::PublishStatus,
                DhtServiceEffect::StartDueDemands,
            ]
        );
    }

    #[test]
    fn dht_service_model_runtime_warning_only_publishes_status() {
        let config = disabled_service_config();
        let mut model = DhtServiceModel::new(config.clone(), 11, None);

        let reduction = model.update(DhtServiceAction::RuntimeWarning {
            warning: "maintenance failed".to_string(),
        });

        assert_eq!(model.config(), &config);
        assert_eq!(model.generation(), 11);
        assert_eq!(model.warning_owned().as_deref(), Some("maintenance failed"));
        assert_eq!(reduction.effects, vec![DhtServiceEffect::PublishStatus]);
    }

    #[test]
    fn dht_service_state_initializes_helper_models() {
        let config = disabled_service_config();
        let mut state =
            DhtServiceState::new(config.clone(), 42, Some("initial warning".to_string()));

        assert_eq!(state.service.config(), &config);
        assert_eq!(state.service.generation(), 42);
        assert_eq!(
            state.service.warning_owned().as_deref(),
            Some("initial warning")
        );
        assert!(!state.has_draining_demands());
        assert!(state.demand_subscribers.subscribers.is_empty());

        state.record_recent_peers(&[peer("198.51.100.30:6881")]);
        assert_eq!(state.recent_unique_peers.unique_count(Instant::now()), 1);
        state.expire_recent_peers();
    }

    #[test]
    fn dht_lifecycle_model_startup_bootstrap_runs_only_when_due_and_idle() {
        let now = Instant::now();
        let due = now - Duration::from_millis(1);

        let reduction = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
            now,
            due,
            active_user_lookup_count: 0,
        });
        assert_eq!(
            reduction.effects,
            vec![DhtLifecycleEffect::RunStartupBootstrap]
        );

        let not_due = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
            now,
            due: now + Duration::from_millis(1),
            active_user_lookup_count: 0,
        });
        assert!(not_due.effects.is_empty());

        let busy = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
            now,
            due,
            active_user_lookup_count: 1,
        });
        assert!(busy.effects.is_empty());
    }

    #[test]
    fn dht_lifecycle_model_startup_bootstrap_result_updates_retry_state() {
        let retry_at = Instant::now() + DHT_STARTUP_BOOTSTRAP_DELAY;

        let failed = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapFailed {
            warning: "DHT startup bootstrap failed: route lookup failed".to_string(),
            retry_at,
        });
        assert_eq!(
            failed.effects,
            vec![
                DhtLifecycleEffect::RecordRuntimeWarning {
                    warning: "DHT startup bootstrap failed: route lookup failed".to_string(),
                    publish_status: false,
                },
                DhtLifecycleEffect::SetStartupBootstrapDue(retry_at),
            ]
        );

        let succeeded = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapSucceeded);
        assert_eq!(
            succeeded.effects,
            vec![DhtLifecycleEffect::ClearStartupBootstrapDue]
        );
    }

    #[test]
    fn dht_lifecycle_model_maintenance_only_runs_when_runtime_idle() {
        let no_runtime = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
            active_user_lookup_count: None,
        });
        assert!(no_runtime.effects.is_empty());

        let busy = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
            active_user_lookup_count: Some(2),
        });
        assert!(busy.effects.is_empty());

        let idle = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
            active_user_lookup_count: Some(0),
        });
        assert_eq!(idle.effects, vec![DhtLifecycleEffect::RunMaintenance]);
    }

    #[test]
    fn dht_lifecycle_model_health_tick_publishes_expires_and_saves() {
        let reduction = DhtLifecycleModel::update(DhtLifecycleAction::HealthTick);

        assert_eq!(
            reduction.effects,
            vec![
                DhtLifecycleEffect::PublishStatus,
                DhtLifecycleEffect::ExpireRecentUniquePeers,
                DhtLifecycleEffect::SaveRuntimeState,
            ]
        );
    }

    #[test]
    fn dht_lifecycle_model_runtime_failures_publish_warning_status() {
        let maintenance = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceFailed {
            warning: "DHT maintenance failed: maintenance error".to_string(),
        });
        assert_eq!(
            maintenance.effects,
            vec![DhtLifecycleEffect::RecordRuntimeWarning {
                warning: "DHT maintenance failed: maintenance error".to_string(),
                publish_status: true,
            }]
        );

        let runtime_step = DhtLifecycleModel::update(DhtLifecycleAction::RuntimeStepFailed {
            warning: "DHT runtime step failed: step error".to_string(),
        });
        assert_eq!(
            runtime_step.effects,
            vec![DhtLifecycleEffect::RecordRuntimeWarning {
                warning: "DHT runtime step failed: step error".to_string(),
                publish_status: true,
            }]
        );
    }

    #[test]
    fn dht_lifecycle_model_shutdown_saves_runtime_state() {
        let reduction = DhtLifecycleModel::update(DhtLifecycleAction::Shutdown);

        assert_eq!(
            reduction.effects,
            vec![DhtLifecycleEffect::SaveRuntimeState]
        );
    }

    #[test]
    fn demand_subscriber_registry_registers_and_unregisters_once() {
        let mut registry = DemandSubscriberRegistry::new();
        let info_hash = hash_index(42);
        let demand = DhtDemandState {
            awaiting_metadata: true,
            connected_peers: 0,
        };
        let (subscriber_tx, _subscriber_rx) = mpsc::unbounded_channel();

        let registered = registry.update(DemandSubscriberAction::Register {
            info_hash,
            demand,
            subscriber_tx,
        });

        assert_eq!(registered.subscriber_id, Some(1));
        assert_eq!(registry.subscriber_count(info_hash), 1);
        assert_eq!(registered.effects.len(), 1);
        match &registered.effects[0] {
            DemandSubscriberEffect::Registered {
                info_hash: registered_hash,
                demand: registered_demand,
                subscriber_id,
            } => {
                assert_eq!(*registered_hash, info_hash);
                assert_eq!(*registered_demand, demand);
                assert_eq!(*subscriber_id, 1);
            }
            _ => panic!("expected registered effect"),
        }

        let removed = registry.update(DemandSubscriberAction::Unregister {
            info_hash,
            subscriber_id: 1,
        });

        assert_eq!(registry.subscriber_count(info_hash), 0);
        assert_eq!(removed.effects.len(), 1);
        match &removed.effects[0] {
            DemandSubscriberEffect::SubscriberRemoved {
                info_hash: removed_hash,
            } => assert_eq!(*removed_hash, info_hash),
            _ => panic!("expected subscriber removed effect"),
        }

        let duplicate = registry.update(DemandSubscriberAction::Unregister {
            info_hash,
            subscriber_id: 1,
        });
        assert!(duplicate.effects.is_empty());
    }

    #[test]
    fn demand_subscriber_registry_delivery_prunes_closed_subscribers() {
        let mut registry = DemandSubscriberRegistry::new();
        let info_hash = hash_index(43);
        let demand = DhtDemandState {
            awaiting_metadata: false,
            connected_peers: 0,
        };
        let (live_tx, mut live_rx) = mpsc::unbounded_channel();
        let (dead_tx, dead_rx) = mpsc::unbounded_channel();
        drop(dead_rx);

        let live_id = registry
            .update(DemandSubscriberAction::Register {
                info_hash,
                demand,
                subscriber_tx: live_tx,
            })
            .subscriber_id
            .expect("live subscriber id");
        let _dead_id = registry
            .update(DemandSubscriberAction::Register {
                info_hash,
                demand,
                subscriber_tx: dead_tx,
            })
            .subscriber_id
            .expect("dead subscriber id");
        assert_eq!(registry.subscriber_count(info_hash), 2);

        let peers = vec![peer("127.0.0.1:6881"), peer("127.0.0.1:6882")];
        let delivery = registry.update(DemandSubscriberAction::DeliverPeers {
            info_hash,
            peers: peers.clone(),
        });
        assert_eq!(delivery.effects.len(), 1);
        let DemandSubscriberEffect::DeliverPeers {
            info_hash: delivered_hash,
            peers: delivered_peers,
            deliveries,
        } = delivery
            .effects
            .into_iter()
            .next()
            .expect("delivery effect")
        else {
            panic!("expected peer delivery effect");
        };
        assert_eq!(delivered_hash, info_hash);
        assert_eq!(delivered_peers, peers);
        assert_eq!(deliveries.len(), 2);

        let dead_subscribers = deliveries
            .into_iter()
            .filter_map(|delivery| {
                delivery
                    .subscriber_tx
                    .send(delivered_peers.clone())
                    .is_err()
                    .then_some(delivery.subscriber_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(live_rx.try_recv().expect("live peers delivered"), peers);
        assert_eq!(dead_subscribers.len(), 1);

        let pruned = registry.update(DemandSubscriberAction::PruneDeadSubscribers {
            info_hash,
            subscriber_ids: dead_subscribers,
        });
        assert_eq!(registry.subscriber_count(info_hash), 1);
        assert_eq!(pruned.effects.len(), 1);
        assert!(matches!(
            pruned.effects.as_slice(),
            [DemandSubscriberEffect::SubscriberRemoved {
                info_hash: removed_hash
            }] if *removed_hash == info_hash
        ));

        let remaining = registry.update(DemandSubscriberAction::DeliverPeers { info_hash, peers });
        let Some(DemandSubscriberEffect::DeliverPeers { deliveries, .. }) =
            remaining.effects.into_iter().next()
        else {
            panic!("expected remaining delivery effect");
        };
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].subscriber_id, live_id);
    }

    #[test]
    fn dht_runtime_command_model_routes_start_get_peers_and_announce() {
        let info_hash = hash_index(44);
        let (lookup_response_tx, _lookup_response_rx) = oneshot::channel();

        let mut reduction =
            DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::StartGetPeers {
                info_hash,
                response_tx: lookup_response_tx,
            });

        assert_eq!(reduction.effects.len(), 1);
        match reduction.effects.pop().expect("start get peers effect") {
            DhtRuntimeCommandEffect::StartGetPeers {
                info_hash: effect_hash,
                ..
            } => assert_eq!(effect_hash, info_hash),
            _ => panic!("expected start get peers effect"),
        }

        let (announce_response_tx, _announce_response_rx) = oneshot::channel();
        let mut reduction = DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::AnnouncePeer {
            info_hash,
            port: Some(6881),
            response_tx: announce_response_tx,
        });

        assert_eq!(reduction.effects.len(), 1);
        match reduction.effects.pop().expect("announce effect") {
            DhtRuntimeCommandEffect::AnnouncePeer {
                info_hash: effect_hash,
                port,
                ..
            } => {
                assert_eq!(effect_hash, info_hash);
                assert_eq!(port, Some(6881));
            }
            _ => panic!("expected announce peer effect"),
        }
    }

    #[test]
    fn dht_runtime_command_model_routes_family_attach_and_cancel() {
        let info_hash = hash_index(45);
        let (merged_tx, _merged_rx) = mpsc::unbounded_channel();
        let lookup_ids = Arc::new(StdMutex::new(Vec::new()));
        let expected_lookup_ids = lookup_ids.clone();
        let first_batch_seen = Arc::new(AtomicBool::new(false));
        let expected_first_batch_seen = first_batch_seen.clone();
        let accepting_families = Arc::new(AtomicBool::new(true));
        let expected_accepting_families = accepting_families.clone();

        let mut reduction = DhtRuntimeCommandModel::update(
            DhtRuntimeCommandAction::StartGetPeersFamily(DhtRuntimeLookupFamilyRequest {
                info_hash,
                family: AddressFamily::Ipv6,
                slice_class: DemandSliceClass::AwaitingMetadata,
                record_metrics: true,
                merged_tx,
                lookup_ids,
                first_batch_seen,
                accepting_families,
            }),
        );

        assert_eq!(reduction.effects.len(), 1);
        match reduction.effects.pop().expect("attach family effect") {
            DhtRuntimeCommandEffect::AttachLookupFamily(request) => {
                assert_eq!(request.info_hash, info_hash);
                assert_eq!(request.family, AddressFamily::Ipv6);
                assert_eq!(request.slice_class, DemandSliceClass::AwaitingMetadata);
                assert!(request.record_metrics);
                assert!(Arc::ptr_eq(&request.lookup_ids, &expected_lookup_ids));
                assert!(Arc::ptr_eq(
                    &request.first_batch_seen,
                    &expected_first_batch_seen
                ));
                assert!(Arc::ptr_eq(
                    &request.accepting_families,
                    &expected_accepting_families
                ));
            }
            _ => panic!("expected attach lookup family effect"),
        }

        let mut reduction =
            DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::CancelLookups {
                lookup_ids: vec![LookupId(7), LookupId(9)],
            });

        assert_eq!(reduction.effects.len(), 1);
        match reduction.effects.pop().expect("cancel effect") {
            DhtRuntimeCommandEffect::CancelLookups { lookup_ids } => {
                assert_eq!(lookup_ids, vec![LookupId(7), LookupId(9)]);
            }
            _ => panic!("expected cancel lookups effect"),
        }
    }

    #[test]
    fn dht_runtime_command_model_routes_planner_work_with_start_due_followup() {
        let info_hash = hash_index(46);
        let lookup_ids = Arc::new(StdMutex::new(vec![LookupId(11)]));
        let expected_lookup_ids = lookup_ids.clone();
        let unique_peers = HashSet::from([peer("127.0.0.1:6881")]);

        let reduction =
            DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::ParkDemandLookups {
                info_hash,
                slice_class: DemandSliceClass::NoConnectedPeers,
                stop_reason: DemandSliceStopReason::WallTime,
                total_peers: 3,
                unique_peers: unique_peers.clone(),
                lookup_ids,
            });

        assert_eq!(reduction.effects.len(), 2);
        match &reduction.effects[0] {
            DhtRuntimeCommandEffect::ParkDemandLookups {
                info_hash: effect_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers: effect_unique_peers,
                lookup_ids,
            } => {
                assert_eq!(*effect_hash, info_hash);
                assert_eq!(*slice_class, DemandSliceClass::NoConnectedPeers);
                assert_eq!(*stop_reason, DemandSliceStopReason::WallTime);
                assert_eq!(*total_peers, 3);
                assert_eq!(effect_unique_peers, &unique_peers);
                assert!(Arc::ptr_eq(lookup_ids, &expected_lookup_ids));
            }
            _ => panic!("expected park demand lookups effect"),
        }
        assert!(matches!(
            reduction.effects[1],
            DhtRuntimeCommandEffect::StartDueDemands
        ));

        let reduction =
            DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::FinalizeDrainedDemandLookups {
                info_hash,
            });
        assert_eq!(reduction.effects.len(), 2);
        assert!(matches!(
            reduction.effects[0],
            DhtRuntimeCommandEffect::FinalizeDrainedDemandLookups { info_hash: effect_hash }
                if effect_hash == info_hash
        ));
        assert!(matches!(
            reduction.effects[1],
            DhtRuntimeCommandEffect::StartDueDemands
        ));
    }

    async fn local_ipv4_active_runtime() -> ActiveRuntime {
        let bootstrap_addr = peer("127.0.0.1:9");
        local_ipv4_active_runtime_with_bootstrap(vec![bootstrap_addr]).await
    }

    async fn local_ipv4_active_runtime_without_bootstrap() -> ActiveRuntime {
        local_ipv4_active_runtime_with_bootstrap(Vec::new()).await
    }

    async fn local_ipv4_active_runtime_with_bootstrap(
        bootstrap_nodes: Vec<SocketAddr>,
    ) -> ActiveRuntime {
        let runtime = Runtime::bind(RuntimeConfig {
            local_node_id: NodeId::from([9u8; NodeId::LEN]),
            bootstrap_nodes: bootstrap_nodes.clone(),
            ipv4_bind_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            ipv6_bind_addr: None,
            persistence: None,
        })
        .await
        .expect("bind local ipv4 runtime");

        ActiveRuntime {
            runtime,
            backend: DhtBackendKind::InternalPrototype,
            bootstrap: BootstrapSummary {
                total: bootstrap_nodes.len(),
                ipv4: bootstrap_nodes.iter().filter(|addr| addr.is_ipv4()).count(),
                ipv6: 0,
            },
            startup_bootstrap_due: None,
        }
    }

    fn insert_synthetic_drain(
        draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
        info_hash: InfoHash,
        key: u8,
        lookup_id: LookupId,
        slice_class: DemandSliceClass,
        unique_peers: u8,
        now: Instant,
    ) {
        insert_synthetic_drain_with_stop_reason(
            draining_demands,
            info_hash,
            key,
            lookup_id,
            slice_class,
            DemandSliceStopReason::WallTime,
            unique_peers,
            now,
        );
    }

    fn insert_synthetic_drain_with_stop_reason(
        draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
        info_hash: InfoHash,
        key: u8,
        lookup_id: LookupId,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        unique_peers: u8,
        now: Instant,
    ) {
        let unique_peers = synthetic_peers(key, unique_peers);
        let unique_peer_count = unique_peers.len();
        let parked_outcome =
            slice_class.parked_slice_outcome(stop_reason, unique_peer_count, false);
        let duration = demand_drain_duration(
            slice_class,
            stop_reason,
            Some(parked_outcome),
            unique_peer_count,
        )
        .unwrap_or(Duration::from_secs(1));
        draining_demands.insert(
            info_hash,
            DrainingDemandLookup {
                lookup_ids: vec![lookup_id],
                slice_class,
                stop_reason,
                started_at: now,
                total_peers: unique_peer_count,
                initial_unique_peers: unique_peer_count,
                unique_peers,
                deadline: now + duration,
                no_late_yield_deadline: now
                    + demand_drain_no_late_yield_grace(slice_class).min(duration),
                initial_inflight_queries: 1,
                score: 1,
            },
        );
    }

    #[test]
    fn recent_unique_peers_dedupes_and_expires_entries() {
        let start = Instant::now();
        let mut recent = RecentUniquePeers::new(Duration::from_secs(30));
        let peer_a = peer("127.0.0.1:1000");
        let peer_b = peer("127.0.0.2:1000");

        recent.record_batch(start, &[peer_a, peer_a, peer_b]);
        assert_eq!(recent.unique_count(start), 2);

        let refresh = start + Duration::from_secs(10);
        recent.record_batch(refresh, &[peer_a]);
        assert_eq!(recent.unique_count(refresh), 2);

        assert_eq!(recent.unique_count(start + Duration::from_secs(31)), 1);
        assert_eq!(recent.unique_count(start + Duration::from_secs(41)), 0);
    }

    #[test]
    fn literal_bootstrap_summary_counts_literal_socket_addresses() {
        let summary = literal_bootstrap_summary(&[
            "127.0.0.1:6881".to_string(),
            "[::1]:6881".to_string(),
            "node.example.invalid:6881".to_string(),
        ]);

        assert_eq!(summary.total, 3);
        assert_eq!(summary.ipv4, 1);
        assert_eq!(summary.ipv6, 1);
    }

    #[test]
    fn build_status_without_runtime_reports_disabled_state_and_bootstrap() {
        let bootstrap = BootstrapSummary {
            total: 3,
            ipv4: 2,
            ipv6: 1,
        };
        let status = build_status(
            None,
            DhtBackendKind::Disabled,
            DhtBackendKind::InternalPrototype,
            Some("test warning".to_string()),
            7,
            bootstrap,
        );

        assert_eq!(status.generation, 7);
        assert_eq!(status.warning.as_deref(), Some("test warning"));
        assert_eq!(status.health.backend, DhtBackendKind::Disabled);
        assert_eq!(
            status.health.preferred_backend,
            Some(DhtBackendKind::InternalPrototype)
        );
        assert!(!status.health.enabled);
        assert_eq!(status.health.exported_bootstrap_nodes, 3);
        assert_eq!(status.health.ipv4_bootstrap_nodes, 2);
        assert_eq!(status.health.ipv6_bootstrap_nodes, 1);
        assert_eq!(status.health.bound_family_count, 0);
        assert_eq!(status.health.inflight_lookups, 0);
    }

    #[test]
    fn build_wave_telemetry_without_runtime_preserves_recent_unique_count() {
        let telemetry = build_wave_telemetry(None, 12);

        assert_eq!(telemetry.unique_peers_found_last_10s, 12);
        assert_eq!(telemetry.active_lookups, 0);
        assert_eq!(telemetry.active_user_lookups, 0);
        assert_eq!(telemetry.inflight_ipv4_queries, 0);
        assert_eq!(telemetry.inflight_ipv6_queries, 0);
    }

    #[tokio::test]
    async fn start_get_peers_lookup_without_runtime_returns_empty_lookup() {
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let mut planner = DemandPlannerModel::new(Instant::now());

        let started = start_get_peers_lookup(
            None,
            &command_tx,
            &mut planner,
            None,
            hash_index(73),
            DemandSliceClass::RoutineRefresh,
            false,
        )
        .await
        .expect("empty lookup should succeed");

        assert!(started
            .lookup_ids
            .lock()
            .expect("test lookup ids")
            .is_empty());
        assert!(!started.accepting_families.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn disabled_service_command_loop_delivers_peers_and_honors_unregister() {
        let config = disabled_service_config();
        let (status_tx, _status_rx) = watch::channel(initial_disabled_status(&config));
        let (wave_tx, _wave_rx) = watch::channel(DhtWaveTelemetry::default());
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let task = tokio::spawn(run_service(
            config,
            NodeId::from([1u8; NodeId::LEN]),
            None,
            None,
            status_tx,
            wave_tx,
            command_tx.clone(),
            command_rx,
            shutdown_rx,
        ));

        let info_hash = hash_index(74);
        let (subscriber_one_tx, mut subscriber_one_rx) = mpsc::unbounded_channel();
        let (subscriber_two_tx, mut subscriber_two_rx) = mpsc::unbounded_channel();
        let (response_one_tx, response_one_rx) = oneshot::channel();
        let (response_two_tx, response_two_rx) = oneshot::channel();

        send_dht_command(
            &command_tx,
            DhtCommand::RegisterDemand {
                info_hash,
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                subscriber_tx: subscriber_one_tx,
                response_tx: response_one_tx,
            },
        )
        .expect("register subscriber one");
        send_dht_command(
            &command_tx,
            DhtCommand::RegisterDemand {
                info_hash,
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                subscriber_tx: subscriber_two_tx,
                response_tx: response_two_tx,
            },
        )
        .expect("register subscriber two");
        let subscriber_one_id = response_one_rx
            .await
            .expect("subscriber one response")
            .unwrap();
        let subscriber_two_id = response_two_rx
            .await
            .expect("subscriber two response")
            .unwrap();
        assert_ne!(subscriber_one_id, subscriber_two_id);

        let first_batch = vec![peer("127.0.0.21:6881"), peer("127.0.0.22:6881")];
        send_dht_command(
            &command_tx,
            DhtCommand::DemandPeers {
                info_hash,
                peers: first_batch.clone(),
            },
        )
        .expect("send peers");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), subscriber_one_rx.recv())
                .await
                .expect("subscriber one peers"),
            Some(first_batch.clone())
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), subscriber_two_rx.recv())
                .await
                .expect("subscriber two peers"),
            Some(first_batch)
        );

        send_dht_command(
            &command_tx,
            DhtCommand::UnregisterDemand {
                info_hash,
                subscriber_id: subscriber_one_id,
            },
        )
        .expect("unregister subscriber one");
        let second_batch = vec![peer("127.0.0.23:6881")];
        send_dht_command(
            &command_tx,
            DhtCommand::DemandPeers {
                info_hash,
                peers: second_batch.clone(),
            },
        )
        .expect("send peers after unregister");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), subscriber_two_rx.recv())
                .await
                .expect("subscriber two second peers"),
            Some(second_batch)
        );
        let stale_subscriber_result =
            tokio::time::timeout(Duration::from_millis(50), subscriber_one_rx.recv()).await;
        assert_ne!(
            stale_subscriber_result.ok().flatten(),
            Some(vec![peer("127.0.0.23:6881")])
        );

        let _ = shutdown_tx.send(());
        task.await.expect("service task join");
    }

    #[tokio::test]
    async fn disabled_service_command_loop_returns_empty_lookup_and_failed_announce() {
        let config = disabled_service_config();
        let (status_tx, _status_rx) = watch::channel(initial_disabled_status(&config));
        let (wave_tx, _wave_rx) = watch::channel(DhtWaveTelemetry::default());
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let task = tokio::spawn(run_service(
            config,
            NodeId::from([2u8; NodeId::LEN]),
            None,
            None,
            status_tx,
            wave_tx,
            command_tx.clone(),
            command_rx,
            shutdown_rx,
        ));

        let (lookup_response_tx, lookup_response_rx) = oneshot::channel();
        send_dht_command(
            &command_tx,
            DhtCommand::StartGetPeers {
                info_hash: hash_index(75),
                response_tx: lookup_response_tx,
            },
        )
        .expect("start get peers");
        let started = lookup_response_rx
            .await
            .expect("lookup response")
            .expect("empty lookup result");
        assert!(started
            .lookup_ids
            .lock()
            .expect("test lookup ids")
            .is_empty());
        assert!(!started.accepting_families.load(Ordering::Acquire));

        let (announce_response_tx, announce_response_rx) = oneshot::channel();
        send_dht_command(
            &command_tx,
            DhtCommand::AnnouncePeer {
                info_hash: hash_index(75),
                port: Some(6881),
                response_tx: announce_response_tx,
            },
        )
        .expect("announce peer");
        assert!(!announce_response_rx.await.expect("announce response"));

        let _ = shutdown_tx.send(());
        task.await.expect("service task join");
    }

    #[tokio::test]
    async fn disabled_service_reconfigure_failure_publishes_warning_without_generation_bump() {
        let config = disabled_service_config();
        let (status_tx, mut status_rx) = watch::channel(initial_disabled_status(&config));
        let (wave_tx, _wave_rx) = watch::channel(DhtWaveTelemetry::default());
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let task = tokio::spawn(run_service(
            config,
            NodeId::from([3u8; NodeId::LEN]),
            None,
            None,
            status_tx,
            wave_tx,
            command_tx.clone(),
            command_rx,
            shutdown_rx,
        ));

        send_dht_command(
            &command_tx,
            DhtCommand::Reconfigure(DhtServiceConfig {
                port: 0,
                bootstrap_nodes: Vec::new(),
                preferred_backend: DhtBackendKind::InternalPrototype,
                force_internal_failure: true,
            }),
        )
        .expect("send reconfigure");

        tokio::time::timeout(Duration::from_secs(1), status_rx.changed())
            .await
            .expect("status update")
            .expect("status channel open");
        let status = status_rx.borrow().clone();
        assert_eq!(status.generation, 0);
        assert_eq!(status.health.backend, DhtBackendKind::Disabled);
        assert_eq!(
            status.health.preferred_backend,
            Some(DhtBackendKind::Disabled)
        );
        assert_eq!(
            status.warning.as_deref(),
            Some("forced internal backend failure")
        );

        let _ = shutdown_tx.send(());
        task.await.expect("service task join");
    }

    #[tokio::test]
    async fn active_service_reconfigure_to_disabled_publishes_status_and_preserves_subscriber() {
        let config = DhtServiceConfig {
            port: 0,
            bootstrap_nodes: Vec::new(),
            preferred_backend: DhtBackendKind::InternalPrototype,
            force_internal_failure: false,
        };
        let active_runtime = local_ipv4_active_runtime().await;
        let initial_status = build_status(
            Some(&active_runtime),
            DhtBackendKind::InternalPrototype,
            config.preferred_backend,
            None,
            0,
            active_runtime.bootstrap,
        );
        let (status_tx, mut status_rx) = watch::channel(initial_status);
        let (wave_tx, _wave_rx) = watch::channel(DhtWaveTelemetry::default());
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let task = tokio::spawn(run_service(
            config,
            NodeId::from([4u8; NodeId::LEN]),
            Some(active_runtime),
            None,
            status_tx,
            wave_tx,
            command_tx.clone(),
            command_rx,
            shutdown_rx,
        ));

        let info_hash = hash_index(88);
        let (subscriber_tx, mut subscriber_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = oneshot::channel();
        send_dht_command(
            &command_tx,
            DhtCommand::RegisterDemand {
                info_hash,
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                subscriber_tx,
                response_tx,
            },
        )
        .expect("register demand before reconfigure");
        let subscriber_id = response_rx.await.expect("subscriber response");
        assert_eq!(subscriber_id, Some(1));

        send_dht_command(
            &command_tx,
            DhtCommand::Reconfigure(disabled_service_config()),
        )
        .expect("send disabled reconfigure");
        let status = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                status_rx.changed().await.expect("status channel open");
                let status = status_rx.borrow().clone();
                if status.generation == 1 && status.health.backend == DhtBackendKind::Disabled {
                    break status;
                }
            }
        })
        .await
        .expect("disabled status update");
        assert_eq!(status.generation, 1);
        assert_eq!(status.health.backend, DhtBackendKind::Disabled);
        assert_eq!(
            status.health.preferred_backend,
            Some(DhtBackendKind::Disabled)
        );
        assert!(!status.health.enabled);

        let peers = vec![peer("127.0.0.88:6881")];
        send_dht_command(
            &command_tx,
            DhtCommand::DemandPeers {
                info_hash,
                peers: peers.clone(),
            },
        )
        .expect("send peers after disabled reconfigure");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), subscriber_rx.recv())
                .await
                .expect("subscriber peers after disabled reconfigure"),
            Some(peers)
        );

        let _ = shutdown_tx.send(());
        task.await.expect("service task join");
    }

    #[tokio::test]
    async fn runtime_backed_park_lookup_moves_active_state_to_parked_crawl() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let info_hash = hash_index(76);
        let (lookup_id, peer_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start runtime lookup");
        let _keep_receiver_open = peer_rx;
        assert_eq!(active_runtime.runtime.active_user_lookup_count(), 1);

        let lookup_ids = Arc::new(StdMutex::new(vec![lookup_id]));
        let mut parked_crawls = HashMap::new();
        let parked_outcome = park_lookup_ids(
            Some(&mut active_runtime),
            &mut parked_crawls,
            info_hash,
            DemandSliceClass::NoConnectedPeers,
            Some(DemandSliceStopReason::WallTime),
            1,
            lookup_ids.clone(),
        );

        assert_eq!(
            parked_outcome,
            Some(DemandParkedSliceOutcome::HealthyLowYield)
        );
        assert!(lookup_ids.lock().expect("test lookup ids").is_empty());
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);
        let parked = take_parked_family_state(
            &mut parked_crawls,
            None,
            info_hash,
            AddressFamily::Ipv4,
            DemandSliceClass::NoConnectedPeers,
        )
        .expect("parked runtime state");
        assert_eq!(parked.family(), AddressFamily::Ipv4);
        assert!(!parked_crawls.contains_key(&info_hash));
    }

    #[tokio::test]
    async fn runtime_backed_drain_lookup_pauses_and_force_finalize_finishes_state() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let info_hash = hash_index(77);
        let (lookup_id, peer_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start runtime lookup");
        let _keep_receiver_open = peer_rx;

        let mut planner = DemandPlannerModel::new(Instant::now());
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now: Instant::now(),
        });
        assert!(planner.scheduler.mark_in_progress(info_hash));
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let lookup_ids = Arc::new(StdMutex::new(vec![lookup_id]));
        let parked_outcome = planner.drain_lookup_ids(
            Some(&mut active_runtime),
            &command_tx,
            info_hash,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::WallTime,
            3,
            synthetic_peers(77, 3),
            lookup_ids,
        );

        assert_eq!(parked_outcome, Some(DemandParkedSliceOutcome::UsefulYield));
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);
        assert_eq!(active_runtime.runtime.draining_lookup_count(), 1);
        assert!(planner.draining_demands.contains_key(&info_hash));

        let mut slice_metrics = DemandSliceMetrics::default();
        let finalized = finish_drained_demand_lookup(
            Some(&mut active_runtime),
            &mut planner,
            &command_tx,
            &mut slice_metrics,
            info_hash,
            true,
        );

        assert!(finalized);
        assert_eq!(active_runtime.runtime.draining_lookup_count(), 0);
        assert!(!planner.draining_demands.contains_key(&info_hash));
        assert!(
            !planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );
        assert!(planner.parked_crawls.contains_key(&info_hash));
        assert_eq!(slice_metrics.no_connected_peers.wall_time_stops, 1);
        assert_eq!(slice_metrics.no_connected_peers.unique_peers_yielded, 3);
    }

    #[tokio::test]
    async fn runtime_backed_cancel_draining_effect_removes_runtime_lookup() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let info_hash = hash_index(78);
        let (lookup_id, peer_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start runtime lookup");
        let _keep_receiver_open = peer_rx;

        let mut planner = DemandPlannerModel::new(Instant::now());
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now: Instant::now(),
        });
        assert!(planner.scheduler.mark_in_progress(info_hash));
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let parked_outcome = planner.drain_lookup_ids(
            Some(&mut active_runtime),
            &command_tx,
            info_hash,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::WallTime,
            3,
            synthetic_peers(78, 3),
            Arc::new(StdMutex::new(vec![lookup_id])),
        );
        assert_eq!(parked_outcome, Some(DemandParkedSliceOutcome::UsefulYield));
        assert_eq!(active_runtime.runtime.draining_lookup_count(), 1);

        let removal = planner.update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
        let mut slice_metrics = DemandSliceMetrics::default();
        apply_demand_planner_effects(
            Some(&mut active_runtime),
            &mut planner,
            &command_tx,
            &mut slice_metrics,
            removal.effects,
        );

        assert_eq!(active_runtime.runtime.draining_lookup_count(), 0);
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);
        assert!(planner.scheduler.entry_snapshot(info_hash).is_none());
        assert!(!planner.draining_demands.contains_key(&info_hash));
    }

    #[tokio::test]
    async fn attach_lookup_family_ignores_closed_acceptance_and_unbound_family() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let mut planner = DemandPlannerModel::new(Instant::now());
        let mut metrics = DemandSliceMetrics::default();
        let (merged_tx, _merged_rx) = mpsc::unbounded_channel();
        let lookup_ids = Arc::new(StdMutex::new(Vec::new()));
        let first_batch_seen = Arc::new(AtomicBool::new(false));

        attach_lookup_family(
            Some(&mut active_runtime),
            &mut planner,
            Some(&mut metrics),
            hash_index(79),
            AddressFamily::Ipv4,
            DemandSliceClass::NoConnectedPeers,
            merged_tx.clone(),
            lookup_ids.clone(),
            first_batch_seen.clone(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("closed accepting flag is not an error");
        assert!(lookup_ids.lock().expect("test lookup ids").is_empty());
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);

        attach_lookup_family(
            Some(&mut active_runtime),
            &mut planner,
            Some(&mut metrics),
            hash_index(79),
            AddressFamily::Ipv6,
            DemandSliceClass::NoConnectedPeers,
            merged_tx,
            lookup_ids.clone(),
            first_batch_seen,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .expect("unbound family is not an error");
        assert!(lookup_ids.lock().expect("test lookup ids").is_empty());
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);
        assert_eq!(metrics.no_connected_peers.fresh_starts, 0);
        assert_eq!(metrics.no_connected_peers.resumed_starts, 0);
    }

    #[tokio::test]
    async fn attach_lookup_family_records_fresh_and_resumed_state() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let mut planner = DemandPlannerModel::new(Instant::now());
        let mut metrics = DemandSliceMetrics::default();
        let (merged_tx, _merged_rx) = mpsc::unbounded_channel();
        let lookup_ids = Arc::new(StdMutex::new(Vec::new()));
        let first_batch_seen = Arc::new(AtomicBool::new(false));

        attach_lookup_family(
            Some(&mut active_runtime),
            &mut planner,
            Some(&mut metrics),
            hash_index(80),
            AddressFamily::Ipv4,
            DemandSliceClass::NoConnectedPeers,
            merged_tx.clone(),
            lookup_ids.clone(),
            first_batch_seen.clone(),
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .expect("fresh attach");
        assert_eq!(lookup_ids.lock().expect("test lookup ids").len(), 1);
        assert_eq!(metrics.no_connected_peers.fresh_starts, 1);
        assert_eq!(metrics.no_connected_peers.resumed_starts, 0);

        let parked_hash = hash_index(81);
        store_parked_lookup_states(
            &mut planner.parked_crawls,
            parked_hash,
            DemandSliceClass::NoConnectedPeers,
            Some(DemandSliceStopReason::WallTime),
            1,
            vec![lookup_state_for_family(
                LookupId(81),
                AddressFamily::Ipv4,
                81,
                Instant::now(),
            )],
        );
        let resumed_lookup_ids = Arc::new(StdMutex::new(Vec::new()));
        attach_lookup_family(
            Some(&mut active_runtime),
            &mut planner,
            Some(&mut metrics),
            parked_hash,
            AddressFamily::Ipv4,
            DemandSliceClass::NoConnectedPeers,
            merged_tx,
            resumed_lookup_ids.clone(),
            first_batch_seen,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .expect("resumed attach");

        assert_eq!(resumed_lookup_ids.lock().expect("test lookup ids").len(), 1);
        assert_eq!(metrics.no_connected_peers.fresh_starts, 1);
        assert_eq!(metrics.no_connected_peers.resumed_starts, 1);
        assert!(!planner.parked_crawls.contains_key(&parked_hash));
    }

    #[tokio::test]
    async fn runtime_backed_drain_rejection_parks_lookup_when_no_queries_are_inflight() {
        let mut active_runtime = local_ipv4_active_runtime_without_bootstrap().await;
        let info_hash = hash_index(82);
        let (lookup_id, peer_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start runtime lookup");
        let _keep_receiver_open = peer_rx;
        assert_eq!(
            active_runtime
                .runtime
                .lookup_quality_snapshot(lookup_id)
                .expect("lookup quality")
                .inflight_len,
            0
        );

        let mut planner = DemandPlannerModel::new(Instant::now());
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let parked_outcome = planner.drain_lookup_ids(
            Some(&mut active_runtime),
            &command_tx,
            info_hash,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::WallTime,
            1,
            synthetic_peers(82, 1),
            Arc::new(StdMutex::new(vec![lookup_id])),
        );

        assert!(parked_outcome.is_none());
        assert!(planner.draining_demands.is_empty());
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);
        assert!(planner.parked_crawls.contains_key(&info_hash));
    }

    #[tokio::test]
    async fn runtime_backed_drain_rejection_parks_lookup_when_score_is_not_productive() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let info_hash = hash_index(83);
        let (lookup_id, peer_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start runtime lookup");
        let _keep_receiver_open = peer_rx;

        let mut planner = DemandPlannerModel::new(Instant::now());
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let parked_outcome = planner.drain_lookup_ids(
            Some(&mut active_runtime),
            &command_tx,
            info_hash,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::IdleTimeout,
            0,
            HashSet::new(),
            Arc::new(StdMutex::new(vec![lookup_id])),
        );

        assert!(parked_outcome.is_none());
        assert!(planner.draining_demands.is_empty());
        assert_eq!(active_runtime.runtime.active_lookup_count(), 0);
        assert!(planner.parked_crawls.contains_key(&info_hash));
    }

    #[tokio::test]
    async fn runtime_backed_drain_replaces_previous_drain_for_same_demand() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let info_hash = hash_index(84);
        let (first_lookup_id, first_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start first runtime lookup");
        let _keep_first_receiver_open = first_rx;

        let mut planner = DemandPlannerModel::new(Instant::now());
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        assert_eq!(
            planner.drain_lookup_ids(
                Some(&mut active_runtime),
                &command_tx,
                info_hash,
                DemandSliceClass::NoConnectedPeers,
                DemandSliceStopReason::WallTime,
                3,
                synthetic_peers(84, 3),
                Arc::new(StdMutex::new(vec![first_lookup_id])),
            ),
            Some(DemandParkedSliceOutcome::UsefulYield)
        );
        assert_eq!(active_runtime.runtime.draining_lookup_count(), 1);

        let (second_lookup_id, second_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start second runtime lookup");
        let _keep_second_receiver_open = second_rx;
        assert_eq!(
            planner.drain_lookup_ids(
                Some(&mut active_runtime),
                &command_tx,
                info_hash,
                DemandSliceClass::NoConnectedPeers,
                DemandSliceStopReason::WallTime,
                3,
                synthetic_peers(85, 3),
                Arc::new(StdMutex::new(vec![second_lookup_id])),
            ),
            Some(DemandParkedSliceOutcome::UsefulYield)
        );

        assert_eq!(active_runtime.runtime.draining_lookup_count(), 1);
        assert!(active_runtime
            .runtime
            .lookup_quality_snapshot(first_lookup_id)
            .is_none());
        assert!(planner
            .draining_demands
            .get(&info_hash)
            .expect("replacement drain")
            .lookup_ids
            .contains(&second_lookup_id));
    }

    #[tokio::test]
    async fn finalize_drained_lookup_not_ready_keeps_drain_when_not_forced() {
        let mut active_runtime = local_ipv4_active_runtime().await;
        let info_hash = hash_index(86);
        let (lookup_id, peer_rx) = active_runtime
            .runtime
            .start_get_peers(AddressFamily::Ipv4, info_hash)
            .await
            .expect("start runtime lookup");
        let _keep_receiver_open = peer_rx;

        let mut planner = DemandPlannerModel::new(Instant::now());
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        assert_eq!(
            planner.drain_lookup_ids(
                Some(&mut active_runtime),
                &command_tx,
                info_hash,
                DemandSliceClass::NoConnectedPeers,
                DemandSliceStopReason::WallTime,
                3,
                synthetic_peers(86, 3),
                Arc::new(StdMutex::new(vec![lookup_id])),
            ),
            Some(DemandParkedSliceOutcome::UsefulYield)
        );

        let outcome = planner.finalize_drained_lookup(
            Some(&mut active_runtime),
            &command_tx,
            info_hash,
            false,
        );

        assert!(outcome.is_none());
        assert!(planner.draining_demands.contains_key(&info_hash));
        assert_eq!(active_runtime.runtime.draining_lookup_count(), 1);
    }

    #[tokio::test]
    async fn managed_lookup_receiver_drop_sends_cancel_for_non_empty_lookup_ids() {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (_peer_tx, peer_rx) = mpsc::unbounded_channel();
        let lookup_ids_arc = Arc::new(StdMutex::new(vec![LookupId(90), LookupId(91)]));

        drop(ManagedLookupReceiver::new(
            peer_rx,
            command_tx,
            lookup_ids_arc.clone(),
        ));

        let command = tokio::time::timeout(Duration::from_secs(1), command_rx.recv())
            .await
            .expect("cancel command")
            .expect("command channel open");
        let LoopEvent::Command(DhtCommand::CancelLookups { lookup_ids }) =
            command_event(Some(command))
        else {
            panic!("expected cancel command");
        };
        assert_eq!(lookup_ids, vec![LookupId(90), LookupId(91)]);
        assert!(lookup_ids_arc.lock().expect("test lookup ids").is_empty());
    }

    #[tokio::test]
    async fn managed_lookup_receiver_drop_ignores_empty_lookup_ids() {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (_peer_tx, peer_rx) = mpsc::unbounded_channel();

        drop(ManagedLookupReceiver::new(
            peer_rx,
            command_tx,
            Arc::new(StdMutex::new(Vec::new())),
        ));

        let maybe_command = tokio::time::timeout(Duration::from_millis(50), command_rx.recv())
            .await
            .ok()
            .flatten();
        assert!(maybe_command.is_none());
    }

    #[tokio::test]
    async fn dht_demand_subscription_drop_sends_unregister_for_service_subscription() {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (_subscriber_tx, receiver) = mpsc::unbounded_channel();
        let info_hash = hash_index(87);

        drop(DhtDemandSubscription {
            receiver,
            inner: DhtDemandSubscriptionInner::Service {
                command_tx,
                info_hash,
                subscriber_id: 42,
            },
        });

        let command = tokio::time::timeout(Duration::from_secs(1), command_rx.recv())
            .await
            .expect("unregister command")
            .expect("command channel open");
        let LoopEvent::Command(DhtCommand::UnregisterDemand {
            info_hash: command_hash,
            subscriber_id,
        }) = command_event(Some(command))
        else {
            panic!("expected unregister command");
        };
        assert_eq!(command_hash, info_hash);
        assert_eq!(subscriber_id, 42);
    }

    #[tokio::test]
    async fn summarize_lookup_receiver_counts_unique_peer_families() {
        let (peer_tx, peer_rx) = mpsc::unbounded_channel();
        peer_tx
            .send(vec![peer("127.0.0.30:6881"), peer("[::1]:6881")])
            .expect("first batch");
        peer_tx
            .send(vec![peer("127.0.0.30:6881"), peer("127.0.0.31:6881")])
            .expect("second batch");
        drop(peer_tx);

        let mut receiver = ManagedLookupReceiver {
            receiver: peer_rx,
            cancel_guard: None,
        };
        let summary = summarize_lookup_receiver(
            &mut receiver,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .expect("lookup summary");

        assert_eq!(summary.batch_count, 2);
        assert_eq!(summary.total_peers, 4);
        assert_eq!(summary.unique_peers, 3);
        assert_eq!(summary.unique_ipv4_peers, 2);
        assert_eq!(summary.unique_ipv6_peers, 1);
        assert!(summary.first_batch_ms.is_some());
        assert!(summary.first_ipv4_batch_ms.is_some());
        assert!(summary.first_ipv6_batch_ms.is_some());
    }
}
