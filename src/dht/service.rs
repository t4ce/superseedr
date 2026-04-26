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
#[path = "service/test_support.rs"]
mod test_support;

#[cfg(test)]
#[path = "service/state_tests.rs"]
mod state_tests;

#[cfg(test)]
#[path = "service/lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "service/subscriber_tests.rs"]
mod subscriber_tests;

#[cfg(test)]
#[path = "service/command_tests.rs"]
mod command_tests;

#[cfg(test)]
#[path = "service/status_tests.rs"]
mod status_tests;

#[cfg(test)]
#[path = "service/driver_tests.rs"]
mod driver_tests;

#[cfg(test)]
#[path = "service/runtime_effect_tests.rs"]
mod runtime_effect_tests;

#[cfg(test)]
#[path = "service/api_tests.rs"]
mod api_tests;

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
