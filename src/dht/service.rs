// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::lookup::LookupQualitySnapshot;
use super::persist::{PersistenceConfig, PersistenceManager};
use super::scheduler::DemandScheduler;
pub use super::scheduler::DhtDemandState;
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
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::lookup_host;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
const DHT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL: Duration = DHT_MAINTENANCE_INTERVAL;
const DHT_NO_CONNECTED_PEERS_BASE_INTERVAL: Duration = Duration::from_secs(8);
const DHT_NO_CONNECTED_PEERS_MAX_INTERVAL: Duration = DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL;
const DHT_AWAITING_METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DHT_HEALTH_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const DHT_DEMAND_SCHEDULER_INTERVAL: Duration = Duration::from_millis(250);
const DHT_DEMAND_LOOKUP_SLOT_COUNT: usize = 8;
const DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK: usize = 4;
const DHT_PERSISTENCE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DHT_STARTUP_BOOTSTRAP_DELAY: Duration = Duration::from_secs(5);
const DHT_IPV6_HEDGE_DELAY: Duration = Duration::from_millis(750);
const DHT_LOOKUP_BOOTSTRAP_WAIT: Duration = Duration::from_secs(2);
const DHT_UNIQUE_PEERS_FOUND_WINDOW: Duration = Duration::from_secs(10);
const DHT_PARKED_CRAWL_MAX_AGE: Duration = Duration::from_secs(5 * 60);
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

#[derive(Debug, Clone, Default)]
pub struct DhtLookupRun {
    pub batch_count: usize,
    pub total_peers: usize,
    pub unique_peers: usize,
    pub unique_ipv4_peers: usize,
    pub unique_ipv6_peers: usize,
    pub first_batch_ms: Option<u64>,
    pub first_ipv4_batch_ms: Option<u64>,
    pub first_ipv6_batch_ms: Option<u64>,
}

#[derive(Debug)]
struct StartedLookup {
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    receiver: mpsc::UnboundedReceiver<Vec<SocketAddr>>,
}

#[derive(Debug)]
struct DemandCrawlState {
    ipv4: Option<LookupState>,
    ipv6: Option<LookupState>,
    class: DemandSliceClass,
    updated_at: Instant,
    reset_count: u32,
    consecutive_stalled_low_yield_slices: u32,
}

impl DemandCrawlState {
    fn new(now: Instant, class: DemandSliceClass) -> Self {
        Self {
            ipv4: None,
            ipv6: None,
            class,
            updated_at: now,
            reset_count: 0,
            consecutive_stalled_low_yield_slices: 0,
        }
    }

    fn take_family_state(&mut self, family: AddressFamily) -> Option<LookupState> {
        let state = match family {
            AddressFamily::Ipv4 => self.ipv4.take(),
            AddressFamily::Ipv6 => self.ipv6.take(),
        };
        if state.is_some() {
            self.updated_at = Instant::now();
        }
        state
    }

    fn store_family_state(&mut self, class: DemandSliceClass, state: LookupState) {
        match state.family() {
            AddressFamily::Ipv4 => self.ipv4 = Some(state),
            AddressFamily::Ipv6 => self.ipv6 = Some(state),
        }
        self.class = class;
        self.updated_at = Instant::now();
    }

    fn is_empty(&self) -> bool {
        self.ipv4.is_none() && self.ipv6.is_none()
    }

    fn is_stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.updated_at) >= DHT_PARKED_CRAWL_MAX_AGE
    }

    fn reset_reason_for(
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

    fn should_reset_for(&self, class: DemandSliceClass, now: Instant) -> bool {
        self.reset_reason_for(class, now).is_some()
    }

    fn reset_for(&mut self, class: DemandSliceClass, now: Instant) {
        self.ipv4 = None;
        self.ipv6 = None;
        self.class = class;
        self.updated_at = now;
        self.reset_count = self.reset_count.saturating_add(1);
        self.consecutive_stalled_low_yield_slices = 0;
    }

    fn observe_parked_slice(
        &mut self,
        class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        unique_peers: usize,
        weak_parked_state: bool,
    ) {
        if self.class != class {
            self.class = class;
            self.consecutive_stalled_low_yield_slices = 0;
        }
        self.class = class;
        self.updated_at = Instant::now();
        if (unique_peers <= class.stalled_low_yield_slice_max_unique_peers() || weak_parked_state)
            && matches!(
                stop_reason,
                DemandSliceStopReason::WallTime | DemandSliceStopReason::IdleTimeout
            )
        {
            self.consecutive_stalled_low_yield_slices =
                self.consecutive_stalled_low_yield_slices.saturating_add(1);
        } else {
            self.consecutive_stalled_low_yield_slices = 0;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemandSliceClass {
    RoutineRefresh,
    NoConnectedPeers,
    AwaitingMetadata,
}

impl DemandSliceClass {
    fn from_demand(demand: DhtDemandState) -> Self {
        if demand.awaiting_metadata {
            Self::AwaitingMetadata
        } else if demand.connected_peers == 0 {
            Self::NoConnectedPeers
        } else {
            Self::RoutineRefresh
        }
    }

    fn stalled_empty_slice_reset_threshold(self) -> u32 {
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

    fn stalled_low_yield_slice_max_unique_peers(self) -> usize {
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

    fn parked_quality_is_weak(self, snapshot: AggregateLookupQualitySnapshot) -> bool {
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
struct AggregateLookupQualitySnapshot {
    frontier_len: usize,
    inflight_len: usize,
    visited_len: usize,
    eligible_responder_count: usize,
    received_peer_count: usize,
}

impl AggregateLookupQualitySnapshot {
    fn extend(&mut self, snapshot: LookupQualitySnapshot) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemandSliceStopReason {
    NaturalFinish,
    WallTime,
    IdleTimeout,
    FirstBatch,
    UniquePeerCap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemandCrawlResetReason {
    Stale,
    ClassChanged,
    LowQuality,
}

#[derive(Debug, Clone, Default)]
struct DemandSliceClassMetrics {
    fresh_starts: u64,
    resumed_starts: u64,
    natural_finishes: u64,
    wall_time_stops: u64,
    idle_timeout_stops: u64,
    first_batch_stops: u64,
    unique_peer_cap_stops: u64,
    peers_yielded: u64,
    unique_peers_yielded: u64,
    stale_resets: u64,
    class_change_resets: u64,
    low_quality_resets: u64,
}

#[derive(Debug, Clone, Default)]
struct DemandSliceMetrics {
    awaiting_metadata: DemandSliceClassMetrics,
    no_connected_peers: DemandSliceClassMetrics,
    routine_refresh: DemandSliceClassMetrics,
}

impl DemandSliceMetrics {
    fn class_mut(&mut self, class: DemandSliceClass) -> &mut DemandSliceClassMetrics {
        match class {
            DemandSliceClass::AwaitingMetadata => &mut self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &mut self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &mut self.routine_refresh,
        }
    }

    fn class_ref(&self, class: DemandSliceClass) -> &DemandSliceClassMetrics {
        match class {
            DemandSliceClass::AwaitingMetadata => &self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &self.routine_refresh,
        }
    }

    fn record_start(&mut self, class: DemandSliceClass, resumed: bool) {
        let metrics = self.class_mut(class);
        if resumed {
            metrics.resumed_starts = metrics.resumed_starts.saturating_add(1);
        } else {
            metrics.fresh_starts = metrics.fresh_starts.saturating_add(1);
        }
    }

    fn record_stop(
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

    fn record_reset(&mut self, class: DemandSliceClass, reason: DemandCrawlResetReason) {
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

    fn has_activity(&self) -> bool {
        for class in [
            DemandSliceClass::AwaitingMetadata,
            DemandSliceClass::NoConnectedPeers,
            DemandSliceClass::RoutineRefresh,
        ] {
            let metrics = self.class_ref(class);
            if metrics.fresh_starts > 0
                || metrics.resumed_starts > 0
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

    fn summary(&self) -> String {
        fn fmt(label: &str, metrics: &DemandSliceClassMetrics) -> String {
            format!(
                "{label}(fresh={} resumed={} natural={} wall={} idle={} first={} cap={} peers={} unique={} reset_stale={} reset_class={} reset_quality={})",
                metrics.fresh_starts,
                metrics.resumed_starts,
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

#[derive(Debug, Clone, Copy)]
struct DemandLookupPlan {
    class: DemandSliceClass,
    idle_timeout: Duration,
    max_wall_time: Duration,
    stop_after_first_batch: bool,
    unique_peer_cap: usize,
}

impl DemandLookupPlan {
    fn for_demand(demand: DhtDemandState) -> Self {
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

struct LookupCancelGuard {
    command_tx: mpsc::UnboundedSender<DhtCommand>,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
}

impl Drop for LookupCancelGuard {
    fn drop(&mut self) {
        let mut lookup_ids = self.lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return;
        }
        let _ = self.command_tx.send(DhtCommand::CancelLookups {
            lookup_ids: std::mem::take(&mut *lookup_ids),
        });
    }
}

struct ManagedLookupReceiver {
    receiver: mpsc::UnboundedReceiver<Vec<SocketAddr>>,
    cancel_guard: Option<LookupCancelGuard>,
}

impl ManagedLookupReceiver {
    fn new(
        receiver: mpsc::UnboundedReceiver<Vec<SocketAddr>>,
        command_tx: mpsc::UnboundedSender<DhtCommand>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    ) -> Self {
        let has_lookup_ids = !lookup_ids
            .lock()
            .expect("managed dht lookup ids lock")
            .is_empty();
        let cancel_guard = has_lookup_ids.then_some(LookupCancelGuard {
            command_tx,
            lookup_ids,
        });
        Self {
            receiver,
            cancel_guard,
        }
    }

    fn empty() -> Self {
        let (_tx, receiver) = mpsc::unbounded_channel();
        Self {
            receiver,
            cancel_guard: None,
        }
    }

    async fn recv(&mut self) -> Option<Vec<SocketAddr>> {
        self.receiver.recv().await
    }
}

#[derive(Debug)]
enum DhtDemandSubscriptionInner {
    Service {
        command_tx: mpsc::UnboundedSender<DhtCommand>,
        info_hash: InfoHash,
        subscriber_id: u64,
    },
    #[cfg(test)]
    Recorder,
    Disabled,
}

#[derive(Debug)]
pub struct DhtDemandSubscription {
    receiver: mpsc::UnboundedReceiver<Vec<SocketAddr>>,
    inner: DhtDemandSubscriptionInner,
}

impl DhtDemandSubscription {
    fn empty() -> Self {
        let (_tx, receiver) = mpsc::unbounded_channel();
        Self {
            receiver,
            inner: DhtDemandSubscriptionInner::Disabled,
        }
    }

    pub async fn recv(&mut self) -> Option<Vec<SocketAddr>> {
        self.receiver.recv().await
    }
}

impl Drop for DhtDemandSubscription {
    fn drop(&mut self) {
        if let DhtDemandSubscriptionInner::Service {
            command_tx,
            info_hash,
            subscriber_id,
        } = &self.inner
        {
            let _ = command_tx.send(DhtCommand::UnregisterDemand {
                info_hash: *info_hash,
                subscriber_id: *subscriber_id,
            });
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct TestDhtRecorder {
    announce_requests: Arc<StdMutex<Vec<(Vec<u8>, Option<u16>)>>>,
}

#[cfg(test)]
impl TestDhtRecorder {
    pub(crate) fn recorded_announces(&self) -> Vec<(Vec<u8>, Option<u16>)> {
        self.announce_requests
            .lock()
            .expect("test dht recorder lock")
            .clone()
    }
}

#[derive(Debug)]
enum DhtCommand {
    Reconfigure(DhtServiceConfig),
    RegisterDemand {
        info_hash: InfoHash,
        demand: DhtDemandState,
        subscriber_tx: mpsc::UnboundedSender<Vec<SocketAddr>>,
        response_tx: oneshot::Sender<Option<u64>>,
    },
    UpdateDemand {
        info_hash: InfoHash,
        demand: DhtDemandState,
    },
    UnregisterDemand {
        info_hash: InfoHash,
        subscriber_id: u64,
    },
    DemandPeers {
        info_hash: InfoHash,
        peers: Vec<SocketAddr>,
    },
    DemandLookupFinished {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        total_peers: usize,
        unique_peers: usize,
    },
    StartGetPeers {
        info_hash: InfoHash,
        response_tx: oneshot::Sender<Result<StartedLookup, String>>,
    },
    StartGetPeersFamily {
        info_hash: InfoHash,
        family: AddressFamily,
        slice_class: DemandSliceClass,
        record_metrics: bool,
        merged_tx: mpsc::UnboundedSender<Vec<SocketAddr>>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
        first_batch_seen: Arc<AtomicBool>,
    },
    CancelLookups {
        lookup_ids: Vec<LookupId>,
    },
    ParkDemandLookups {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: usize,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    },
    AnnouncePeer {
        info_hash: InfoHash,
        port: Option<u16>,
        response_tx: oneshot::Sender<bool>,
    },
}

#[derive(Debug)]
enum LoopEvent {
    Shutdown,
    Command(DhtCommand),
    DemandTick,
    MaintenanceTick,
    HealthTick,
    RuntimeStep(Result<bool, String>),
    CommandClosed,
}

#[derive(Debug, Clone, Copy, Default)]
struct BootstrapSummary {
    total: usize,
    ipv4: usize,
    ipv6: usize,
}

#[derive(Debug)]
struct ActiveRuntime {
    runtime: Runtime,
    backend: DhtBackendKind,
    bootstrap: BootstrapSummary,
    startup_bootstrap_due: Option<Instant>,
}

#[derive(Debug)]
struct BuiltRuntime {
    active_runtime: Option<ActiveRuntime>,
    backend: DhtBackendKind,
    warning: Option<String>,
    bootstrap: BootstrapSummary,
}

#[derive(Debug)]
pub struct DhtService {
    handle: DhtHandle,
    status_rx: watch::Receiver<DhtStatus>,
    wave_telemetry_rx: watch::Receiver<DhtWaveTelemetry>,
    command_tx: mpsc::UnboundedSender<DhtCommand>,
    #[allow(dead_code)]
    task: Option<JoinHandle<()>>,
}

impl DhtService {
    pub async fn new(
        config: DhtServiceConfig,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<Self, String> {
        let local_node_id = configured_or_persisted_local_node_id();
        let initial = build_runtime(&config, local_node_id).await?;
        let initial_status = build_status(
            initial.active_runtime.as_ref(),
            initial.backend,
            config.preferred_backend,
            initial.warning.clone(),
            0,
            initial.bootstrap,
        );
        let initial_wave_telemetry = build_wave_telemetry(initial.active_runtime.as_ref(), 0);

        let (status_tx, status_rx) = watch::channel(initial_status);
        let (wave_telemetry_tx, wave_telemetry_rx) = watch::channel(initial_wave_telemetry);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let handle = DhtHandle {
            inner: DhtHandleInner::Service {
                command_tx: command_tx.clone(),
                status_rx: status_rx.clone(),
            },
        };
        let task = Some(tokio::spawn(run_service(
            config,
            local_node_id,
            initial.active_runtime,
            initial.warning,
            status_tx,
            wave_telemetry_tx,
            command_tx.clone(),
            command_rx,
            shutdown_rx,
        )));

        Ok(Self {
            handle,
            status_rx,
            wave_telemetry_rx,
            command_tx,
            task,
        })
    }

    pub fn handle(&self) -> DhtHandle {
        self.handle.clone()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<DhtStatus> {
        self.status_rx.clone()
    }

    pub fn current_status(&self) -> DhtStatus {
        self.status_rx.borrow().clone()
    }

    pub fn current_wave_telemetry(&self) -> DhtWaveTelemetry {
        self.wave_telemetry_rx.borrow().clone()
    }

    pub fn current_warning(&self) -> Option<String> {
        self.status_rx.borrow().warning.clone()
    }

    pub fn reconfigure(&self, config: DhtServiceConfig) {
        let _ = self.command_tx.send(DhtCommand::Reconfigure(config));
    }
}

fn configured_or_persisted_local_node_id() -> NodeId {
    if let Some(configured) = env::var("SUPERSEEDR_DHT_NODE_ID_HEX")
        .ok()
        .and_then(|value| hex::decode(value).ok())
        .and_then(|bytes| NodeId::try_from(bytes.as_slice()).ok())
    {
        return configured;
    }

    if let Some(persistence) = persistence_config() {
        let manager = PersistenceManager::new(persistence);
        if let Ok(Some(snapshot)) = manager.load_snapshot(std::time::SystemTime::now()) {
            return snapshot.node_id;
        }
    }

    NodeId::from(random::<[u8; 20]>())
}

#[cfg(test)]
impl DhtService {
    pub(crate) fn from_test_recorder(recorder: TestDhtRecorder) -> Self {
        let handle = DhtHandle::from_test_recorder(recorder);
        let status_rx = handle.status_rx().clone();
        let (_wave_telemetry_tx, wave_telemetry_rx) = watch::channel(DhtWaveTelemetry::default());
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        Self {
            handle,
            status_rx,
            wave_telemetry_rx,
            command_tx,
            task: None,
        }
    }
}

pub fn configured_status_from_settings(settings: &Settings) -> DhtStatus {
    configured_status_from_config(&DhtServiceConfig::from_settings(settings))
}

fn configured_status_from_config(config: &DhtServiceConfig) -> DhtStatus {
    let bootstrap = literal_bootstrap_summary(&config.bootstrap_nodes);
    DhtStatus {
        generation: 0,
        warning: None,
        health: DhtHealthSnapshot {
            backend: config.preferred_backend,
            preferred_backend: Some(config.preferred_backend),
            enabled: !matches!(config.preferred_backend, DhtBackendKind::Disabled),
            exported_bootstrap_nodes: bootstrap.total,
            ipv4_bootstrap_nodes: bootstrap.ipv4,
            ipv6_bootstrap_nodes: bootstrap.ipv6,
            ..Default::default()
        },
    }
}

#[derive(Clone)]
pub struct DhtHandle {
    inner: DhtHandleInner,
}

#[derive(Clone)]
enum DhtHandleInner {
    Service {
        command_tx: mpsc::UnboundedSender<DhtCommand>,
        status_rx: watch::Receiver<DhtStatus>,
    },
    #[cfg(test)]
    Recorder {
        recorder: TestDhtRecorder,
        status_rx: watch::Receiver<DhtStatus>,
    },
    Disabled {
        status_rx: watch::Receiver<DhtStatus>,
    },
}

impl std::fmt::Debug for DhtHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = self.status_rx().borrow().clone();
        f.debug_struct("DhtHandle")
            .field("generation", &status.generation)
            .field("backend", &status.health.backend)
            .finish()
    }
}

impl Default for DhtHandle {
    fn default() -> Self {
        Self::disabled()
    }
}

impl DhtHandle {
    pub fn disabled() -> Self {
        let (_status_tx, status_rx) = watch::channel(DhtStatus {
            generation: 0,
            warning: None,
            health: DhtHealthSnapshot {
                backend: DhtBackendKind::Disabled,
                preferred_backend: Some(DhtBackendKind::Disabled),
                enabled: false,
                ..Default::default()
            },
        });
        Self {
            inner: DhtHandleInner::Disabled { status_rx },
        }
    }

    #[cfg(test)]
    fn from_test_recorder(recorder: TestDhtRecorder) -> Self {
        let (_status_tx, status_rx) = watch::channel(DhtStatus {
            generation: 0,
            warning: None,
            health: DhtHealthSnapshot {
                backend: DhtBackendKind::InternalPrototype,
                preferred_backend: Some(DhtBackendKind::InternalPrototype),
                enabled: true,
                ..Default::default()
            },
        });
        Self {
            inner: DhtHandleInner::Recorder {
                recorder,
                status_rx,
            },
        }
    }

    pub async fn status_snapshot(&self) -> DhtStatus {
        match &self.inner {
            DhtHandleInner::Service { status_rx, .. } => status_rx.borrow().clone(),
            #[cfg(test)]
            DhtHandleInner::Recorder { status_rx, .. } => status_rx.borrow().clone(),
            DhtHandleInner::Disabled { status_rx } => status_rx.borrow().clone(),
        }
    }

    pub fn spawn_lookup_task(
        &self,
        info_hash: Vec<u8>,
        initial_demand: DhtDemandState,
        dht_tx: Sender<Vec<SocketAddr>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Option<JoinHandle<()>> {
        let info_hash = InfoHash::from(<[u8; 20]>::try_from(info_hash).ok()?);
        match &self.inner {
            DhtHandleInner::Service { .. } => {
                let handle = self.clone();
                Some(tokio::spawn(async move {
                    let mut subscription = match handle
                        .register_demand(info_hash.as_ref().to_vec(), initial_demand)
                        .await
                    {
                        Some(subscription) => subscription,
                        None => return,
                    };

                    loop {
                        tokio::select! {
                            _ = shutdown_rx.recv() => break,
                            maybe_peers = subscription.recv() => {
                                let Some(peers) = maybe_peers else {
                                    break;
                                };
                                if dht_tx.send(peers).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }))
            }
            #[cfg(test)]
            DhtHandleInner::Recorder { .. } | DhtHandleInner::Disabled { .. } => {
                Some(tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = shutdown_rx.recv() => break,
                            _ = std::future::pending::<()>() => {}
                        }
                    }
                }))
            }
            #[cfg(not(test))]
            DhtHandleInner::Disabled { .. } => Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => break,
                        _ = std::future::pending::<()>() => {}
                    }
                }
            })),
        }
    }

    pub async fn lookup_once(
        &self,
        info_hash: Vec<u8>,
        idle_timeout: Duration,
        overall_timeout: Duration,
    ) -> Option<DhtLookupRun> {
        let info_hash = InfoHash::from(<[u8; 20]>::try_from(info_hash).ok()?);
        match &self.inner {
            DhtHandleInner::Service { .. } => {
                let mut peers_rx = self.start_lookup_receiver(info_hash).await?;
                summarize_lookup_receiver(&mut peers_rx, idle_timeout, overall_timeout).await
            }
            #[cfg(test)]
            DhtHandleInner::Recorder { .. } | DhtHandleInner::Disabled { .. } => {
                Some(DhtLookupRun::default())
            }
            #[cfg(not(test))]
            DhtHandleInner::Disabled { .. } => Some(DhtLookupRun::default()),
        }
    }

    pub async fn announce_peer(&self, info_hash: Vec<u8>, port: Option<u16>) -> bool {
        let Ok(info_hash) = <[u8; 20]>::try_from(info_hash) else {
            return false;
        };
        match &self.inner {
            DhtHandleInner::Service { command_tx, .. } => {
                if command_tx.is_closed() {
                    return false;
                }

                let (response_tx, response_rx) = oneshot::channel();
                let command = DhtCommand::AnnouncePeer {
                    info_hash: InfoHash::from(info_hash),
                    port,
                    response_tx,
                };
                if command_tx.send(command).is_err() {
                    return false;
                }
                response_rx.await.unwrap_or(false)
            }
            #[cfg(test)]
            DhtHandleInner::Recorder { recorder, .. } => {
                recorder
                    .announce_requests
                    .lock()
                    .expect("test dht recorder lock")
                    .push((info_hash.to_vec(), port));
                true
            }
            DhtHandleInner::Disabled { .. } => false,
        }
    }

    pub async fn register_demand(
        &self,
        info_hash: Vec<u8>,
        demand: DhtDemandState,
    ) -> Option<DhtDemandSubscription> {
        let Ok(info_hash) = <[u8; 20]>::try_from(info_hash) else {
            return None;
        };

        match &self.inner {
            DhtHandleInner::Service { command_tx, .. } => {
                let (subscriber_tx, receiver) = mpsc::unbounded_channel();
                let (response_tx, response_rx) = oneshot::channel();
                let command = DhtCommand::RegisterDemand {
                    info_hash: InfoHash::from(info_hash),
                    demand,
                    subscriber_tx,
                    response_tx,
                };
                if command_tx.send(command).is_err() {
                    return None;
                }

                let subscriber_id = response_rx.await.ok().flatten()?;
                Some(DhtDemandSubscription {
                    receiver,
                    inner: DhtDemandSubscriptionInner::Service {
                        command_tx: command_tx.clone(),
                        info_hash: InfoHash::from(info_hash),
                        subscriber_id,
                    },
                })
            }
            #[cfg(test)]
            DhtHandleInner::Recorder { .. } => Some(DhtDemandSubscription {
                receiver: mpsc::unbounded_channel().1,
                inner: DhtDemandSubscriptionInner::Recorder,
            }),
            DhtHandleInner::Disabled { .. } => Some(DhtDemandSubscription::empty()),
        }
    }

    pub fn update_demand(&self, info_hash: Vec<u8>, demand: DhtDemandState) -> bool {
        let Ok(info_hash) = <[u8; 20]>::try_from(info_hash) else {
            return false;
        };

        match &self.inner {
            DhtHandleInner::Service { command_tx, .. } => command_tx
                .send(DhtCommand::UpdateDemand {
                    info_hash: InfoHash::from(info_hash),
                    demand,
                })
                .is_ok(),
            #[cfg(test)]
            DhtHandleInner::Recorder { .. } => true,
            DhtHandleInner::Disabled { .. } => true,
        }
    }

    async fn start_lookup_receiver(&self, info_hash: InfoHash) -> Option<ManagedLookupReceiver> {
        let status_rx = self.status_rx();
        match &self.inner {
            DhtHandleInner::Service { command_tx, .. } => {
                if command_tx.is_closed()
                    && matches!(status_rx.borrow().health.backend, DhtBackendKind::Disabled)
                {
                    return Some(ManagedLookupReceiver::empty());
                }

                let (response_tx, response_rx) = oneshot::channel();
                let command = DhtCommand::StartGetPeers {
                    info_hash,
                    response_tx,
                };
                if command_tx.send(command).is_err() {
                    return if matches!(status_rx.borrow().health.backend, DhtBackendKind::Disabled)
                    {
                        Some(ManagedLookupReceiver::empty())
                    } else {
                        None
                    };
                }

                match response_rx.await.ok()? {
                    Ok(started) => Some(ManagedLookupReceiver::new(
                        started.receiver,
                        command_tx.clone(),
                        started.lookup_ids,
                    )),
                    Err(_) => Some(ManagedLookupReceiver::empty()),
                }
            }
            _ => Some(ManagedLookupReceiver::empty()),
        }
    }

    fn status_rx(&self) -> &watch::Receiver<DhtStatus> {
        match &self.inner {
            DhtHandleInner::Service { status_rx, .. } => status_rx,
            #[cfg(test)]
            DhtHandleInner::Recorder { status_rx, .. } => status_rx,
            DhtHandleInner::Disabled { status_rx } => status_rx,
        }
    }
}

async fn run_service(
    mut config: DhtServiceConfig,
    local_node_id: NodeId,
    mut active_runtime: Option<ActiveRuntime>,
    mut warning: Option<String>,
    status_tx: watch::Sender<DhtStatus>,
    wave_telemetry_tx: watch::Sender<DhtWaveTelemetry>,
    command_tx: mpsc::UnboundedSender<DhtCommand>,
    mut command_rx: mpsc::UnboundedReceiver<DhtCommand>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let slice_metrics_log_enabled = env::var_os("SUPERSEEDR_DHT_SLICE_LOG").is_some();
    let mut demand_tick = tokio::time::interval(DHT_DEMAND_SCHEDULER_INTERVAL);
    demand_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut maintenance_interval = tokio::time::interval(DHT_MAINTENANCE_INTERVAL);
    let mut health_interval = tokio::time::interval(DHT_HEALTH_REFRESH_INTERVAL);
    let mut generation = status_tx.borrow().generation;
    let mut demand_scheduler = DemandScheduler::new(
        DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL,
        DHT_NO_CONNECTED_PEERS_BASE_INTERVAL,
        DHT_NO_CONNECTED_PEERS_MAX_INTERVAL,
        DHT_AWAITING_METADATA_REFRESH_INTERVAL,
    );
    let mut demand_subscribers: HashMap<
        InfoHash,
        HashMap<u64, mpsc::UnboundedSender<Vec<SocketAddr>>>,
    > = HashMap::new();
    let mut demand_lookup_ids: HashMap<InfoHash, Arc<StdMutex<Vec<LookupId>>>> = HashMap::new();
    let mut parked_crawls: HashMap<InfoHash, DemandCrawlState> = HashMap::new();
    let mut slice_metrics = DemandSliceMetrics::default();
    let mut recent_unique_peers = RecentUniquePeers::new(DHT_UNIQUE_PEERS_FOUND_WINDOW);
    let mut next_subscriber_id = 1u64;

    loop {
        if let Some(active) = active_runtime.as_mut() {
            if let Some(startup_due) = active.startup_bootstrap_due {
                if Instant::now() >= startup_due && active.runtime.active_user_lookup_count() == 0 {
                    match active.runtime.bootstrap_startup().await {
                        Ok(()) => active.startup_bootstrap_due = None,
                        Err(error) => {
                            warning = Some(format!("DHT startup bootstrap failed: {error}"));
                            active.startup_bootstrap_due =
                                Some(Instant::now() + DHT_STARTUP_BOOTSTRAP_DELAY);
                        }
                    }
                }
            }
        }

        let event = if let Some(active) = active_runtime.as_mut() {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => LoopEvent::Shutdown,
                maybe_command = command_rx.recv() => maybe_command.map_or(LoopEvent::CommandClosed, LoopEvent::Command),
                _ = demand_tick.tick() => LoopEvent::DemandTick,
                _ = maintenance_interval.tick() => LoopEvent::MaintenanceTick,
                _ = health_interval.tick() => LoopEvent::HealthTick,
                step_result = active.runtime.step() => LoopEvent::RuntimeStep(step_result.map_err(|error| error.to_string())),
            }
        } else {
            tokio::select! {
                _ = shutdown_rx.recv() => LoopEvent::Shutdown,
                maybe_command = command_rx.recv() => maybe_command.map_or(LoopEvent::CommandClosed, LoopEvent::Command),
                _ = demand_tick.tick() => LoopEvent::DemandTick,
                _ = maintenance_interval.tick() => LoopEvent::MaintenanceTick,
                _ = health_interval.tick() => LoopEvent::HealthTick,
            }
        };

        match event {
            LoopEvent::Shutdown | LoopEvent::CommandClosed => {
                if let Some(active) = active_runtime.as_ref() {
                    let _ = active.runtime.save_state().await;
                }
                break;
            }
            LoopEvent::Command(DhtCommand::Reconfigure(new_config)) => {
                match build_runtime(&new_config, local_node_id).await {
                    Ok(built) => {
                        if let Some(previous) = active_runtime.as_ref() {
                            let _ = previous.runtime.save_state().await;
                        }
                        config = new_config;
                        generation = generation.saturating_add(1);
                        warning = built.warning;
                        active_runtime = built.active_runtime;
                    }
                    Err(error) => {
                        warning = Some(error);
                    }
                }
                demand_scheduler.reset_active(Instant::now());
                demand_lookup_ids.clear();
                parked_crawls.clear();
                publish_status(
                    &status_tx,
                    active_runtime.as_ref(),
                    warning.clone(),
                    generation,
                    config.preferred_backend,
                );
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::RegisterDemand {
                info_hash,
                demand,
                subscriber_tx,
                response_tx,
            }) => {
                let subscriber_id = next_subscriber_id;
                next_subscriber_id = next_subscriber_id.saturating_add(1);
                demand_subscribers
                    .entry(info_hash)
                    .or_default()
                    .insert(subscriber_id, subscriber_tx);
                demand_scheduler.register(info_hash, demand, Instant::now());
                let _ = response_tx.send(Some(subscriber_id));
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UpdateDemand { info_hash, demand }) => {
                demand_scheduler.update(info_hash, demand, Instant::now());
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UnregisterDemand {
                info_hash,
                subscriber_id,
            }) => {
                let slice_class = demand_scheduler
                    .demand_state(info_hash)
                    .map(DemandSliceClass::from_demand)
                    .unwrap_or(DemandSliceClass::RoutineRefresh);
                let mut removed = false;
                if let Some(subscribers) = demand_subscribers.get_mut(&info_hash) {
                    removed = subscribers.remove(&subscriber_id).is_some();
                    if subscribers.is_empty() {
                        demand_subscribers.remove(&info_hash);
                    }
                }
                if removed && demand_scheduler.unregister(info_hash) {
                    if let Some(lookup_ids) = demand_lookup_ids.remove(&info_hash) {
                        park_lookup_ids(
                            active_runtime.as_mut(),
                            &mut parked_crawls,
                            info_hash,
                            slice_class,
                            None,
                            0,
                            lookup_ids,
                        );
                    }
                }
            }
            LoopEvent::Command(DhtCommand::DemandPeers { info_hash, peers }) => {
                recent_unique_peers.record_batch(Instant::now(), &peers);
                let Some(subscribers) = demand_subscribers.get_mut(&info_hash) else {
                    continue;
                };

                let subscriber_count_before = subscribers.len();
                subscribers.retain(|_, subscriber_tx| subscriber_tx.send(peers.clone()).is_ok());
                let removed = subscriber_count_before.saturating_sub(subscribers.len());
                let mut drained = false;
                let slice_class = demand_scheduler
                    .demand_state(info_hash)
                    .map(DemandSliceClass::from_demand)
                    .unwrap_or(DemandSliceClass::RoutineRefresh);
                for _ in 0..removed {
                    if demand_scheduler.unregister(info_hash) {
                        drained = true;
                        break;
                    }
                }
                if subscribers.is_empty() {
                    demand_subscribers.remove(&info_hash);
                    if drained {
                        if let Some(lookup_ids) = demand_lookup_ids.remove(&info_hash) {
                            park_lookup_ids(
                                active_runtime.as_mut(),
                                &mut parked_crawls,
                                info_hash,
                                slice_class,
                                None,
                                0,
                                lookup_ids,
                            );
                        }
                    }
                }
            }
            LoopEvent::Command(DhtCommand::DemandLookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
            }) => {
                demand_lookup_ids.remove(&info_hash);
                slice_metrics.record_stop(
                    slice_class,
                    DemandSliceStopReason::NaturalFinish,
                    total_peers,
                    unique_peers,
                );
                demand_scheduler.finish(info_hash, Instant::now());
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::StartGetPeers {
                info_hash,
                response_tx,
            }) => {
                let result = start_get_peers_lookup(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut parked_crawls,
                    None,
                    info_hash,
                    DemandSliceClass::RoutineRefresh,
                    false,
                )
                .await;
                let _ = response_tx.send(result);
            }
            LoopEvent::Command(DhtCommand::StartGetPeersFamily {
                info_hash,
                family,
                slice_class,
                record_metrics,
                merged_tx,
                lookup_ids,
                first_batch_seen,
            }) => {
                let _ = attach_lookup_family(
                    active_runtime.as_mut(),
                    &mut parked_crawls,
                    if record_metrics {
                        Some(&mut slice_metrics)
                    } else {
                        None
                    },
                    info_hash,
                    family,
                    slice_class,
                    merged_tx,
                    lookup_ids,
                    first_batch_seen,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::CancelLookups { lookup_ids }) => {
                if let Some(active_runtime) = active_runtime.as_mut() {
                    for lookup_id in lookup_ids {
                        active_runtime.runtime.cancel_lookup(lookup_id);
                    }
                }
            }
            LoopEvent::Command(DhtCommand::ParkDemandLookups {
                info_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers,
                lookup_ids,
            }) => {
                demand_lookup_ids.remove(&info_hash);
                slice_metrics.record_stop(slice_class, stop_reason, total_peers, unique_peers);
                park_lookup_ids(
                    active_runtime.as_mut(),
                    &mut parked_crawls,
                    info_hash,
                    slice_class,
                    Some(stop_reason),
                    unique_peers,
                    lookup_ids,
                );
                demand_scheduler.finish(info_hash, Instant::now());
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::AnnouncePeer {
                info_hash,
                port,
                response_tx,
            }) => {
                let success = announce_peer(active_runtime.as_mut(), info_hash, port).await;
                let _ = response_tx.send(success);
            }
            LoopEvent::DemandTick => {
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::MaintenanceTick => {
                if let Some(active) = active_runtime.as_mut() {
                    if active.runtime.active_user_lookup_count() == 0 {
                        if let Err(error) = active.runtime.run_maintenance().await {
                            warning = Some(format!("DHT maintenance failed: {error}"));
                            publish_status(
                                &status_tx,
                                active_runtime.as_ref(),
                                warning.clone(),
                                generation,
                                config.preferred_backend,
                            );
                        }
                    }
                }
            }
            LoopEvent::HealthTick => {
                publish_status(
                    &status_tx,
                    active_runtime.as_ref(),
                    warning.clone(),
                    generation,
                    config.preferred_backend,
                );
                if let Some(active) = active_runtime.as_ref() {
                    let _ = active.runtime.save_state().await;
                }
                if slice_metrics_log_enabled && slice_metrics.has_activity() {
                    tracing::info!(
                        target: "superseedr::dht_slice",
                        summary = %slice_metrics.summary(),
                        "DHT slice metrics"
                    );
                }
            }
            LoopEvent::RuntimeStep(Ok(_)) => {}
            LoopEvent::RuntimeStep(Err(error)) => {
                warning = Some(format!("DHT runtime step failed: {error}"));
                publish_status(
                    &status_tx,
                    active_runtime.as_ref(),
                    warning.clone(),
                    generation,
                    config.preferred_backend,
                );
            }
        }

        publish_wave_telemetry(
            &wave_telemetry_tx,
            active_runtime.as_ref(),
            &mut recent_unique_peers,
        );
    }
}

async fn start_get_peers_lookup(
    active_runtime: Option<&mut ActiveRuntime>,
    command_tx: &mpsc::UnboundedSender<DhtCommand>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    slice_metrics: Option<&mut DemandSliceMetrics>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    record_metrics: bool,
) -> Result<StartedLookup, String> {
    let Some(active_runtime) = active_runtime else {
        return Ok(StartedLookup {
            lookup_ids: Arc::new(StdMutex::new(Vec::new())),
            receiver: ManagedLookupReceiver::empty().receiver,
        });
    };

    let lookup_ids = Arc::new(StdMutex::new(Vec::new()));
    let (merged_tx, merged_rx) = mpsc::unbounded_channel();
    let first_batch_seen = Arc::new(AtomicBool::new(false));

    let primary_family = if active_runtime.runtime.family_bound(AddressFamily::Ipv4) {
        Some(AddressFamily::Ipv4)
    } else if active_runtime.runtime.family_bound(AddressFamily::Ipv6) {
        Some(AddressFamily::Ipv6)
    } else {
        None
    };

    if let Some(family) = primary_family {
        ensure_lookup_routes(active_runtime, family).await?;
        active_runtime.runtime.cancel_maintenance_lookups();
        attach_lookup_family(
            Some(active_runtime),
            parked_crawls,
            slice_metrics,
            info_hash,
            family,
            slice_class,
            merged_tx.clone(),
            lookup_ids.clone(),
            first_batch_seen.clone(),
        )
        .await?;
    }

    if primary_family == Some(AddressFamily::Ipv4)
        && active_runtime.runtime.family_bound(AddressFamily::Ipv6)
    {
        let command_tx = command_tx.clone();
        let merged_tx = merged_tx.clone();
        let lookup_ids = lookup_ids.clone();
        let first_batch_seen = first_batch_seen.clone();
        tokio::spawn(async move {
            tokio::time::sleep(DHT_IPV6_HEDGE_DELAY).await;
            if merged_tx.is_closed() {
                return;
            }
            let _ = command_tx.send(DhtCommand::StartGetPeersFamily {
                info_hash,
                family: AddressFamily::Ipv6,
                slice_class,
                record_metrics,
                merged_tx,
                lookup_ids,
                first_batch_seen,
            });
        });
    }

    if lookup_ids
        .lock()
        .expect("managed dht lookup ids lock")
        .is_empty()
    {
        return Ok(StartedLookup {
            lookup_ids: Arc::new(StdMutex::new(Vec::new())),
            receiver: ManagedLookupReceiver::empty().receiver,
        });
    }

    drop(merged_tx);

    Ok(StartedLookup {
        lookup_ids,
        receiver: merged_rx,
    })
}

fn take_parked_family_state(
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

fn store_parked_lookup_states(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    states: Vec<LookupState>,
) {
    if states.is_empty() {
        return;
    }

    let now = Instant::now();
    let quality = aggregate_lookup_quality(&states);
    let weak_parked_state = slice_class.parked_quality_is_weak(quality);
    let crawl = parked_crawls
        .entry(info_hash)
        .or_insert_with(|| DemandCrawlState::new(now, slice_class));
    if let Some(stop_reason) = stop_reason {
        crawl.observe_parked_slice(slice_class, stop_reason, unique_peers, weak_parked_state);
    }
    for state in states {
        crawl.store_family_state(slice_class, state);
    }
}

fn aggregate_lookup_quality(states: &[LookupState]) -> AggregateLookupQualitySnapshot {
    let mut aggregate = AggregateLookupQualitySnapshot::default();
    for state in states {
        aggregate.extend(state.quality_snapshot());
    }
    aggregate
}

fn park_lookup_ids(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
) {
    let lookup_ids = {
        let mut lookup_ids = lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return;
        }
        std::mem::take(&mut *lookup_ids)
    };

    let Some(active_runtime) = active_runtime else {
        return;
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
    );
}

fn evict_stale_parked_crawls(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    now: Instant,
) {
    parked_crawls.retain(|_, crawl| !crawl.is_stale(now) && !crawl.is_empty());
}

fn active_demand_lookup_slot_count(
    demand_lookup_ids: &HashMap<InfoHash, Arc<StdMutex<Vec<LookupId>>>>,
) -> usize {
    demand_lookup_ids.len()
}

fn demand_lookup_launch_budget(
    demand_lookup_ids: &HashMap<InfoHash, Arc<StdMutex<Vec<LookupId>>>>,
) -> usize {
    let available_slots = DHT_DEMAND_LOOKUP_SLOT_COUNT
        .saturating_sub(active_demand_lookup_slot_count(demand_lookup_ids));
    available_slots.min(DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK)
}

async fn start_due_demands(
    active_runtime: Option<&mut ActiveRuntime>,
    command_tx: &mpsc::UnboundedSender<DhtCommand>,
    demand_scheduler: &mut DemandScheduler,
    demand_lookup_ids: &mut HashMap<InfoHash, Arc<StdMutex<Vec<LookupId>>>>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    slice_metrics: &mut DemandSliceMetrics,
) {
    let Some(active_runtime) = active_runtime else {
        return;
    };

    evict_stale_parked_crawls(parked_crawls, Instant::now());
    let launch_budget = demand_lookup_launch_budget(demand_lookup_ids);
    if launch_budget == 0 {
        return;
    }
    let due = demand_scheduler.take_due(Instant::now(), launch_budget);
    for info_hash in due {
        let plan = DemandLookupPlan::for_demand(
            demand_scheduler.demand_state(info_hash).unwrap_or_default(),
        );
        match start_get_peers_lookup(
            Some(active_runtime),
            command_tx,
            parked_crawls,
            Some(slice_metrics),
            info_hash,
            plan.class,
            true,
        )
        .await
        {
            Ok(started) => {
                demand_lookup_ids.insert(info_hash, started.lookup_ids.clone());
                let mut receiver = started.receiver;
                let command_tx = command_tx.clone();
                let lookup_ids = started.lookup_ids.clone();
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
                                let _ = command_tx.send(DhtCommand::DemandPeers {
                                    info_hash,
                                    peers: peers.clone(),
                                });
                                for peer in peers {
                                    unique_peers.insert(peer);
                                }
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
                        let _ = command_tx.send(DhtCommand::ParkDemandLookups {
                            info_hash,
                            slice_class: plan.class,
                            stop_reason: reason,
                            total_peers,
                            unique_peers: unique_peers.len(),
                            lookup_ids,
                        });
                    } else {
                        let _ = command_tx.send(DhtCommand::DemandLookupFinished {
                            info_hash,
                            slice_class: plan.class,
                            total_peers,
                            unique_peers: unique_peers.len(),
                        });
                    }
                });
            }
            Err(_) => {
                demand_scheduler.finish(info_hash, Instant::now());
            }
        }
    }
}

async fn ensure_lookup_routes(
    active_runtime: &mut ActiveRuntime,
    family: AddressFamily,
) -> Result<(), String> {
    if active_runtime.runtime.active_route_count(family) > 0 {
        return Ok(());
    }

    active_runtime
        .runtime
        .bootstrap_startup()
        .await
        .map_err(|error| error.to_string())?;
    active_runtime.startup_bootstrap_due = None;

    let deadline = Instant::now() + DHT_LOOKUP_BOOTSTRAP_WAIT;
    while Instant::now() < deadline && active_runtime.runtime.active_route_count(family) == 0 {
        match tokio::time::timeout(Duration::from_millis(200), active_runtime.runtime.step()).await
        {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => break,
            Ok(Err(error)) => return Err(error.to_string()),
            Err(_) => {}
        }
    }

    Ok(())
}

async fn attach_lookup_family(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    slice_metrics: Option<&mut DemandSliceMetrics>,
    info_hash: InfoHash,
    family: AddressFamily,
    slice_class: DemandSliceClass,
    merged_tx: mpsc::UnboundedSender<Vec<SocketAddr>>,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    first_batch_seen: Arc<AtomicBool>,
) -> Result<(), String> {
    let Some(active_runtime) = active_runtime else {
        return Ok(());
    };
    if !active_runtime.runtime.family_bound(family) {
        return Ok(());
    }

    let mut slice_metrics = slice_metrics;
    let resumed_state = take_parked_family_state(
        parked_crawls,
        slice_metrics.as_mut().map(|metrics| &mut **metrics),
        info_hash,
        family,
        slice_class,
    );
    let resumed = resumed_state.is_some();
    let (lookup_id, mut family_rx) = match resumed_state {
        Some(state) => active_runtime
            .runtime
            .start_get_peers_with_state(state)
            .await
            .map_err(|error| error.to_string())?,
        None => active_runtime
            .runtime
            .start_get_peers(family, info_hash)
            .await
            .map_err(|error| error.to_string())?,
    };
    if let Some(metrics) = slice_metrics {
        metrics.record_start(slice_class, resumed);
    }
    lookup_ids
        .lock()
        .expect("managed dht lookup ids lock")
        .push(lookup_id);

    tokio::spawn(async move {
        while let Some(batch) = family_rx.recv().await {
            first_batch_seen.store(true, Ordering::Release);
            if merged_tx.send(batch).is_err() {
                break;
            }
        }
    });

    Ok(())
}

async fn announce_peer(
    active_runtime: Option<&mut ActiveRuntime>,
    info_hash: InfoHash,
    port: Option<u16>,
) -> bool {
    let Some(active_runtime) = active_runtime else {
        return false;
    };

    let mut announced = false;
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        if !active_runtime.runtime.family_bound(family) {
            continue;
        }
        match active_runtime
            .runtime
            .announce_peer(family, info_hash, port)
            .await
        {
            Ok(success) => announced |= success,
            Err(_) => {}
        }
    }

    announced
}

async fn build_runtime(
    config: &DhtServiceConfig,
    local_node_id: NodeId,
) -> Result<BuiltRuntime, String> {
    if let Some(error) = forced_internal_backend_error(config) {
        return Err(error);
    }

    let disable_ipv4 = std::env::var_os("SUPERSEEDR_DHT_DISABLE_IPV4").is_some();
    let disable_ipv6 = std::env::var_os("SUPERSEEDR_DHT_DISABLE_IPV6").is_some();

    if matches!(config.preferred_backend, DhtBackendKind::Disabled) {
        let bootstrap = literal_bootstrap_summary(&config.bootstrap_nodes);
        return Ok(BuiltRuntime {
            active_runtime: None,
            backend: DhtBackendKind::Disabled,
            warning: None,
            bootstrap,
        });
    }

    let bootstrap_nodes = resolve_bootstrap_nodes(&config.bootstrap_nodes).await;
    let bootstrap = BootstrapSummary {
        total: bootstrap_nodes.len(),
        ipv4: bootstrap_nodes.iter().filter(|addr| addr.is_ipv4()).count(),
        ipv6: bootstrap_nodes.iter().filter(|addr| addr.is_ipv6()).count(),
    };
    let warning = match config.preferred_backend {
        DhtBackendKind::Mainline => {
            Some("mainline backend setting now maps to the internal runtime".to_string())
        }
        _ => None,
    };
    let runtime = Runtime::bind(RuntimeConfig {
        local_node_id,
        bootstrap_nodes,
        ipv4_bind_addr: (!disable_ipv4).then_some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            config.port,
        )),
        ipv6_bind_addr: (!disable_ipv6).then_some(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            config.port,
        )),
        persistence: persistence_config(),
    })
    .await
    .map_err(|error| error.to_string())?;
    let startup_bootstrap_due = (std::env::var_os("SUPERSEEDR_DHT_SKIP_STARTUP_BOOTSTRAP")
        .is_none())
    .then_some(Instant::now() + DHT_STARTUP_BOOTSTRAP_DELAY);

    Ok(BuiltRuntime {
        active_runtime: Some(ActiveRuntime {
            runtime,
            backend: DhtBackendKind::InternalPrototype,
            bootstrap,
            startup_bootstrap_due,
        }),
        backend: DhtBackendKind::InternalPrototype,
        warning,
        bootstrap,
    })
}

fn build_status(
    active_runtime: Option<&ActiveRuntime>,
    backend: DhtBackendKind,
    preferred_backend: DhtBackendKind,
    warning: Option<String>,
    generation: u64,
    bootstrap: BootstrapSummary,
) -> DhtStatus {
    let mut health = DhtHealthSnapshot {
        backend,
        preferred_backend: Some(preferred_backend),
        enabled: !matches!(backend, DhtBackendKind::Disabled),
        exported_bootstrap_nodes: bootstrap.total,
        ipv4_bootstrap_nodes: bootstrap.ipv4,
        ipv6_bootstrap_nodes: bootstrap.ipv6,
        ..Default::default()
    };

    if let Some(active_runtime) = active_runtime {
        let runtime_health = active_runtime.runtime.health_snapshot();
        let ipv4_local_addr = active_runtime.runtime.ipv4_local_addr();
        let ipv6_local_addr = active_runtime.runtime.ipv6_local_addr();
        health.local_addr = ipv4_local_addr.or(ipv6_local_addr);
        health.ipv4_local_addr = ipv4_local_addr;
        health.ipv6_local_addr = ipv6_local_addr;
        health.bound_family_count = active_runtime.runtime.bound_family_count();
        health.cached_ipv4_routes = runtime_health.routing_nodes_ipv4;
        health.cached_ipv6_routes = runtime_health.routing_nodes_ipv6;
        health.active_ipv4_routes = runtime_health.routing_nodes_ipv4;
        health.active_ipv6_routes = runtime_health.routing_nodes_ipv6;
        health.inflight_lookups = active_runtime.runtime.active_lookup_count();
        health.inflight_ipv4_queries = runtime_health.inflight_queries_ipv4;
        health.inflight_ipv6_queries = runtime_health.inflight_queries_ipv6;
        health.server_mode = Some(health.bound_family_count > 0);

        let responsive = runtime_health.bootstrap_responsive_count;
        let responsive_ipv4 = responsive.min(active_runtime.bootstrap.ipv4);
        let responsive_ipv6 = responsive
            .saturating_sub(responsive_ipv4)
            .min(active_runtime.bootstrap.ipv6);
        health.responsive_ipv4_bootstrap_nodes = responsive_ipv4;
        health.responsive_ipv6_bootstrap_nodes = responsive_ipv6;
    }

    DhtStatus {
        generation,
        warning,
        health,
    }
}

fn publish_status(
    status_tx: &watch::Sender<DhtStatus>,
    active_runtime: Option<&ActiveRuntime>,
    warning: Option<String>,
    generation: u64,
    preferred_backend: DhtBackendKind,
) {
    let backend = active_runtime
        .map(|active| active.backend)
        .unwrap_or(DhtBackendKind::Disabled);
    let bootstrap = active_runtime
        .map(|active| active.bootstrap)
        .unwrap_or_default();
    let _ = status_tx.send(build_status(
        active_runtime,
        backend,
        preferred_backend,
        warning,
        generation,
        bootstrap,
    ));
}

fn build_wave_telemetry(
    active_runtime: Option<&ActiveRuntime>,
    unique_peers_found_last_10s: usize,
) -> DhtWaveTelemetry {
    let Some(active_runtime) = active_runtime else {
        return DhtWaveTelemetry {
            unique_peers_found_last_10s,
            ..DhtWaveTelemetry::default()
        };
    };

    let (inflight_ipv4_queries, inflight_ipv6_queries) =
        active_runtime.runtime.inflight_query_counts();

    DhtWaveTelemetry {
        active_lookups: active_runtime.runtime.active_lookup_count(),
        active_user_lookups: active_runtime.runtime.active_user_lookup_count(),
        inflight_ipv4_queries,
        inflight_ipv6_queries,
        unique_peers_found_last_10s,
    }
}

fn publish_wave_telemetry(
    wave_telemetry_tx: &watch::Sender<DhtWaveTelemetry>,
    active_runtime: Option<&ActiveRuntime>,
    recent_unique_peers: &mut RecentUniquePeers,
) {
    let telemetry = build_wave_telemetry(
        active_runtime,
        recent_unique_peers.unique_count(Instant::now()),
    );
    if *wave_telemetry_tx.borrow() != telemetry {
        let _ = wave_telemetry_tx.send(telemetry);
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

fn literal_bootstrap_summary(bootstrap_nodes: &[String]) -> BootstrapSummary {
    let mut summary = BootstrapSummary {
        total: bootstrap_nodes.len(),
        ..Default::default()
    };
    for value in bootstrap_nodes {
        if let Ok(addr) = value.parse::<SocketAddr>() {
            if addr.is_ipv4() {
                summary.ipv4 += 1;
            } else {
                summary.ipv6 += 1;
            }
        }
    }
    summary
}

async fn resolve_bootstrap_nodes(bootstrap_nodes: &[String]) -> Vec<SocketAddr> {
    let mut resolved = Vec::new();
    let mut seen = HashSet::new();

    for bootstrap in bootstrap_nodes {
        let Ok(addresses) = lookup_host(bootstrap.as_str()).await else {
            continue;
        };
        for addr in addresses {
            if seen.insert(addr) {
                resolved.push(addr);
            }
        }
    }

    resolved
}

async fn summarize_lookup_receiver(
    peers_rx: &mut ManagedLookupReceiver,
    idle_timeout: Duration,
    overall_timeout: Duration,
) -> Option<DhtLookupRun> {
    let started_at = std::time::Instant::now();
    let mut idle_sleep = Box::pin(tokio::time::sleep(idle_timeout));
    let overall_sleep = tokio::time::sleep(overall_timeout);
    tokio::pin!(overall_sleep);

    let mut unique_peers = HashSet::new();
    let mut batch_count = 0usize;
    let mut total_peers = 0usize;
    let mut first_batch_ms = None;
    let mut first_ipv4_batch_ms = None;
    let mut first_ipv6_batch_ms = None;

    loop {
        tokio::select! {
            _ = &mut overall_sleep => break,
            _ = &mut idle_sleep => break,
            maybe_batch = peers_rx.recv() => {
                let Some(peers) = maybe_batch else {
                    break;
                };
                batch_count += 1;
                total_peers += peers.len();
                let elapsed_ms = started_at.elapsed().as_millis() as u64;
                for peer in peers {
                    if peer.is_ipv4() && first_ipv4_batch_ms.is_none() {
                        first_ipv4_batch_ms = Some(elapsed_ms);
                    }
                    if peer.is_ipv6() && first_ipv6_batch_ms.is_none() {
                        first_ipv6_batch_ms = Some(elapsed_ms);
                    }
                    unique_peers.insert(peer);
                }
                if first_batch_ms.is_none() {
                    first_batch_ms = Some(elapsed_ms);
                }
                idle_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + idle_timeout);
            }
        }
    }

    let unique_ipv4_peers = unique_peers.iter().filter(|peer| peer.is_ipv4()).count();
    let unique_ipv6_peers = unique_peers.len().saturating_sub(unique_ipv4_peers);

    Some(DhtLookupRun {
        batch_count,
        total_peers,
        unique_peers: unique_peers.len(),
        unique_ipv4_peers,
        unique_ipv6_peers,
        first_batch_ms,
        first_ipv4_batch_ms,
        first_ipv6_batch_ms,
    })
}

#[cfg(feature = "dht")]
async fn summarize_lookup_stream<S>(
    peers_stream: &mut S,
    idle_timeout: Duration,
    overall_timeout: Duration,
) -> Option<DhtLookupRun>
where
    S: tokio_stream::Stream<Item = Vec<SocketAddr>> + Unpin,
{
    let started_at = std::time::Instant::now();
    let mut idle_sleep = Box::pin(tokio::time::sleep(idle_timeout));
    let overall_sleep = tokio::time::sleep(overall_timeout);
    tokio::pin!(overall_sleep);

    let mut unique_peers = HashSet::new();
    let mut batch_count = 0usize;
    let mut total_peers = 0usize;
    let mut first_batch_ms = None;
    let mut first_ipv4_batch_ms = None;
    let mut first_ipv6_batch_ms = None;

    loop {
        tokio::select! {
            _ = &mut overall_sleep => break,
            _ = &mut idle_sleep => break,
            maybe_batch = peers_stream.next() => {
                let Some(peers) = maybe_batch else {
                    break;
                };
                batch_count += 1;
                total_peers += peers.len();
                let elapsed_ms = started_at.elapsed().as_millis() as u64;
                for peer in peers {
                    if peer.is_ipv4() && first_ipv4_batch_ms.is_none() {
                        first_ipv4_batch_ms = Some(elapsed_ms);
                    }
                    if peer.is_ipv6() && first_ipv6_batch_ms.is_none() {
                        first_ipv6_batch_ms = Some(elapsed_ms);
                    }
                    unique_peers.insert(peer);
                }
                if first_batch_ms.is_none() {
                    first_batch_ms = Some(elapsed_ms);
                }
                idle_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + idle_timeout);
            }
        }
    }

    let unique_ipv4_peers = unique_peers.iter().filter(|peer| peer.is_ipv4()).count();
    let unique_ipv6_peers = unique_peers.len().saturating_sub(unique_ipv4_peers);

    Some(DhtLookupRun {
        batch_count,
        total_peers,
        unique_peers: unique_peers.len(),
        unique_ipv4_peers,
        unique_ipv6_peers,
        first_batch_ms,
        first_ipv4_batch_ms,
        first_ipv6_batch_ms,
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
    fn demand_lookup_plan_varies_by_demand_class() {
        let metadata = DemandLookupPlan::for_demand(DhtDemandState {
            awaiting_metadata: true,
            connected_peers: 0,
        });
        let no_peers = DemandLookupPlan::for_demand(DhtDemandState {
            awaiting_metadata: false,
            connected_peers: 0,
        });
        let routine = DemandLookupPlan::for_demand(DhtDemandState {
            awaiting_metadata: false,
            connected_peers: 3,
        });

        assert_eq!(
            metadata.max_wall_time,
            DHT_AWAITING_METADATA_SLICE_WALL_TIME
        );
        assert_eq!(
            no_peers.max_wall_time,
            DHT_NO_CONNECTED_PEERS_SLICE_WALL_TIME
        );
        assert_eq!(routine.max_wall_time, DHT_ROUTINE_SLICE_WALL_TIME);
        assert!(!metadata.stop_after_first_batch);
        assert!(!no_peers.stop_after_first_batch);
        assert!(routine.stop_after_first_batch);
        assert!(metadata.unique_peer_cap > no_peers.unique_peer_cap);
        assert!(no_peers.unique_peer_cap > routine.unique_peer_cap);
    }

    #[test]
    fn demand_crawl_state_reuses_across_class_change_and_resets_on_staleness_or_low_quality() {
        let now = Instant::now();
        let mut crawl = DemandCrawlState::new(now, DemandSliceClass::RoutineRefresh);

        assert_eq!(
            crawl.reset_reason_for(
                DemandSliceClass::RoutineRefresh,
                now + Duration::from_secs(1)
            ),
            None
        );
        assert_eq!(
            crawl.reset_reason_for(
                DemandSliceClass::NoConnectedPeers,
                now + Duration::from_secs(1)
            ),
            None
        );
        assert_eq!(
            crawl.reset_reason_for(
                DemandSliceClass::RoutineRefresh,
                now + DHT_PARKED_CRAWL_MAX_AGE
            ),
            Some(DemandCrawlResetReason::Stale)
        );

        let mut low_quality = DemandCrawlState::new(now, DemandSliceClass::RoutineRefresh);
        low_quality.observe_parked_slice(
            DemandSliceClass::RoutineRefresh,
            DemandSliceStopReason::IdleTimeout,
            0,
            false,
        );
        assert_eq!(
            low_quality.reset_reason_for(
                DemandSliceClass::RoutineRefresh,
                now + Duration::from_secs(1)
            ),
            None
        );
        low_quality.observe_parked_slice(
            DemandSliceClass::RoutineRefresh,
            DemandSliceStopReason::WallTime,
            0,
            false,
        );
        assert_eq!(
            low_quality.reset_reason_for(
                DemandSliceClass::RoutineRefresh,
                now + Duration::from_secs(1)
            ),
            Some(DemandCrawlResetReason::LowQuality)
        );

        let mut no_peers_low_yield = DemandCrawlState::new(now, DemandSliceClass::NoConnectedPeers);
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::IdleTimeout,
            2,
            false,
        );
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::WallTime,
            1,
            false,
        );
        assert_eq!(
            no_peers_low_yield.reset_reason_for(
                DemandSliceClass::NoConnectedPeers,
                now + Duration::from_secs(1)
            ),
            None
        );
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::IdleTimeout,
            2,
            false,
        );
        assert_eq!(
            no_peers_low_yield.reset_reason_for(
                DemandSliceClass::NoConnectedPeers,
                now + Duration::from_secs(1)
            ),
            Some(DemandCrawlResetReason::LowQuality)
        );
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::UniquePeerCap,
            10,
            false,
        );
        assert_eq!(
            no_peers_low_yield.reset_reason_for(
                DemandSliceClass::NoConnectedPeers,
                now + Duration::from_secs(1)
            ),
            None
        );

        crawl.reset_for(
            DemandSliceClass::AwaitingMetadata,
            now + Duration::from_secs(2),
        );
        assert_eq!(crawl.class, DemandSliceClass::AwaitingMetadata);
        assert_eq!(crawl.reset_count, 1);
        assert!(crawl.is_empty());
    }

    #[test]
    fn parked_quality_thresholds_match_class_expectations() {
        let weak_routine = AggregateLookupQualitySnapshot {
            frontier_len: 3,
            inflight_len: 0,
            visited_len: 9,
            eligible_responder_count: 1,
            received_peer_count: 4,
        };
        let weak_no_peers = AggregateLookupQualitySnapshot {
            frontier_len: 8,
            inflight_len: 0,
            visited_len: 12,
            eligible_responder_count: 3,
            received_peer_count: 12,
        };
        let healthy_no_peers = AggregateLookupQualitySnapshot {
            frontier_len: 9,
            inflight_len: 1,
            visited_len: 12,
            eligible_responder_count: 4,
            received_peer_count: 12,
        };

        assert!(DemandSliceClass::RoutineRefresh.parked_quality_is_weak(weak_routine));
        assert!(DemandSliceClass::NoConnectedPeers.parked_quality_is_weak(weak_no_peers));
        assert!(!DemandSliceClass::NoConnectedPeers.parked_quality_is_weak(healthy_no_peers));
        assert!(!DemandSliceClass::AwaitingMetadata.parked_quality_is_weak(weak_no_peers));
    }

    #[test]
    fn demand_slice_metrics_record_starts_stops_and_resets() {
        let mut metrics = DemandSliceMetrics::default();

        metrics.record_start(DemandSliceClass::AwaitingMetadata, false);
        metrics.record_start(DemandSliceClass::AwaitingMetadata, true);
        metrics.record_stop(
            DemandSliceClass::AwaitingMetadata,
            DemandSliceStopReason::WallTime,
            12,
            7,
        );
        metrics.record_stop(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::NaturalFinish,
            4,
            3,
        );
        metrics.record_reset(
            DemandSliceClass::RoutineRefresh,
            DemandCrawlResetReason::LowQuality,
        );
        metrics.record_reset(
            DemandSliceClass::RoutineRefresh,
            DemandCrawlResetReason::Stale,
        );
        metrics.record_reset(
            DemandSliceClass::RoutineRefresh,
            DemandCrawlResetReason::ClassChanged,
        );

        assert!(metrics.has_activity());
        assert_eq!(metrics.awaiting_metadata.fresh_starts, 1);
        assert_eq!(metrics.awaiting_metadata.resumed_starts, 1);
        assert_eq!(metrics.awaiting_metadata.wall_time_stops, 1);
        assert_eq!(metrics.awaiting_metadata.peers_yielded, 12);
        assert_eq!(metrics.awaiting_metadata.unique_peers_yielded, 7);
        assert_eq!(metrics.no_connected_peers.natural_finishes, 1);
        assert_eq!(metrics.routine_refresh.class_change_resets, 1);
        assert_eq!(metrics.routine_refresh.stale_resets, 1);
        assert_eq!(metrics.routine_refresh.low_quality_resets, 1);
        assert!(metrics.summary().contains("awaiting("));
        assert!(metrics.summary().contains("reset_quality=1"));
    }

    #[test]
    fn demand_lookup_launch_budget_respects_active_slot_cap() {
        let mut active = HashMap::new();
        let make_ids = || Arc::new(StdMutex::new(Vec::<LookupId>::new()));
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);

        assert_eq!(
            demand_lookup_launch_budget(&active),
            DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK
        );

        for byte in 0..6u8 {
            active.insert(hash(byte), make_ids());
        }
        assert_eq!(demand_lookup_launch_budget(&active), 2);

        for byte in 6..10u8 {
            active.insert(hash(byte), make_ids());
        }
        assert_eq!(demand_lookup_launch_budget(&active), 0);
    }
}
