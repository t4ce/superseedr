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
    accepting_families: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct ActiveDemandLookup {
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    slice_class: DemandSliceClass,
}

#[derive(Debug, Clone)]
struct DrainingDemandLookup {
    lookup_ids: Vec<LookupId>,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    started_at: Instant,
    total_peers: usize,
    initial_unique_peers: usize,
    unique_peers: HashSet<SocketAddr>,
    deadline: Instant,
    no_late_yield_deadline: Instant,
    initial_inflight_queries: usize,
    score: i32,
}

impl DrainingDemandLookup {
    fn record_peers(&mut self, peers: &[SocketAddr]) -> usize {
        let previous_unique_peers = self.unique_peers.len();
        self.total_peers = self.total_peers.saturating_add(peers.len());
        self.unique_peers.extend(peers.iter().copied());
        self.unique_peers
            .len()
            .saturating_sub(previous_unique_peers)
    }

    fn unique_peer_count(&self) -> usize {
        self.unique_peers.len()
    }

    fn late_unique_peer_count(&self) -> usize {
        self.unique_peer_count()
            .saturating_sub(self.initial_unique_peers)
    }

    fn duration_ms(&self, now: Instant) -> u64 {
        duration_ms(now.saturating_duration_since(self.started_at))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DrainedDemandOutcome {
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    total_peers: usize,
    unique_peers: usize,
    parked_outcome: Option<DemandParkedSliceOutcome>,
    drain_duration_ms: u64,
    finalized_after_deadline: bool,
    finalized_early_no_yield: bool,
}

#[derive(Debug, Clone, Default)]
struct DemandPlannerState {
    last_started_at: Option<Instant>,
    last_finished_at: Option<Instant>,
    last_useful_yield_at: Option<Instant>,
    last_unique_peers: usize,
}

impl DemandPlannerState {
    fn note_start(&mut self, now: Instant) {
        self.last_started_at = Some(now);
    }

    fn note_finish(&mut self, now: Instant, unique_peers: usize) {
        self.last_finished_at = Some(now);
        self.last_unique_peers = unique_peers;
        if unique_peers > 0 {
            self.last_useful_yield_at = Some(now);
        }
    }
}

#[derive(Debug, Clone)]
struct DemandLaunchTokenBucket {
    tokens_scaled: u64,
    burst_scaled: u64,
    refill_per_minute: u64,
    refill_remainder: u128,
    last_refill_at: Instant,
}

impl DemandLaunchTokenBucket {
    fn new(refill_per_minute: u64, burst: u64, now: Instant) -> Self {
        let burst_scaled = burst.saturating_mul(DHT_PLANNER_TOKEN_SCALE);
        Self {
            tokens_scaled: burst_scaled,
            burst_scaled,
            refill_per_minute,
            refill_remainder: 0,
            last_refill_at: now,
        }
    }

    fn refill(&mut self, now: Instant) {
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

    fn try_consume(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens_scaled < DHT_PLANNER_TOKEN_SCALE {
            return false;
        }

        self.tokens_scaled = self.tokens_scaled.saturating_sub(DHT_PLANNER_TOKEN_SCALE);
        true
    }

    fn refund(&mut self) {
        self.tokens_scaled = self
            .tokens_scaled
            .saturating_add(DHT_PLANNER_TOKEN_SCALE)
            .min(self.burst_scaled);
    }

    fn available(&self) -> usize {
        (self.tokens_scaled / DHT_PLANNER_TOKEN_SCALE) as usize
    }
}

#[derive(Debug, Clone)]
struct DemandPlannerBudget {
    awaiting_metadata: DemandLaunchTokenBucket,
    no_connected_peers: DemandLaunchTokenBucket,
    routine_refresh: DemandLaunchTokenBucket,
}

impl DemandPlannerBudget {
    fn new(now: Instant) -> Self {
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

    fn bucket_mut(&mut self, class: DemandSliceClass) -> &mut DemandLaunchTokenBucket {
        match class {
            DemandSliceClass::AwaitingMetadata => &mut self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => &mut self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => &mut self.routine_refresh,
        }
    }

    fn refill(&mut self, now: Instant) {
        self.awaiting_metadata.refill(now);
        self.no_connected_peers.refill(now);
        self.routine_refresh.refill(now);
    }

    fn try_consume(&mut self, class: DemandSliceClass, now: Instant) -> bool {
        self.bucket_mut(class).try_consume(now)
    }

    fn refund(&mut self, class: DemandSliceClass) {
        self.bucket_mut(class).refund();
    }

    fn available(&mut self, class: DemandSliceClass, now: Instant) -> usize {
        self.bucket_mut(class).refill(now);
        self.bucket_mut(class).available()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DemandSlotCounts {
    awaiting_metadata: usize,
    no_connected_peers: usize,
    routine_refresh: usize,
}

impl DemandSlotCounts {
    fn count(self, class: DemandSliceClass) -> usize {
        match class {
            DemandSliceClass::AwaitingMetadata => self.awaiting_metadata,
            DemandSliceClass::NoConnectedPeers => self.no_connected_peers,
            DemandSliceClass::RoutineRefresh => self.routine_refresh,
        }
    }

    fn total(self) -> usize {
        self.awaiting_metadata
            .saturating_add(self.no_connected_peers)
            .saturating_add(self.routine_refresh)
    }

    fn record(&mut self, class: DemandSliceClass) {
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

#[derive(Debug)]
struct DemandCrawlState {
    ipv4: Option<LookupState>,
    ipv6: Option<LookupState>,
    class: DemandSliceClass,
    updated_at: Instant,
    reset_count: u32,
    consecutive_stalled_low_yield_slices: u32,
    consecutive_healthy_zero_yield_slices: u32,
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
            consecutive_healthy_zero_yield_slices: 0,
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
        self.consecutive_healthy_zero_yield_slices = 0;
    }

    fn observe_parked_slice(&mut self, class: DemandSliceClass, outcome: DemandParkedSliceOutcome) {
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

    fn parked_slice_outcome(
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

fn aggregate_parked_crawl_quality(crawl: &DemandCrawlState) -> AggregateLookupQualitySnapshot {
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
enum DemandSliceStopReason {
    NaturalFinish,
    WallTime,
    IdleTimeout,
    FirstBatch,
    UniquePeerCap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemandParkedSliceOutcome {
    UsefulYield,
    WeakLowYield,
    HealthyZeroYield,
    HealthyLowYield,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemandCrawlResetReason {
    Stale,
    ClassChanged,
    LowQuality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemandSelectionReason {
    ReusableParked,
    UsefulYieldHistory,
    OverdueScarce,
    SpareCapacity,
}

#[derive(Debug, Clone, Default)]
struct DhtSoakMetrics;

impl DhtSoakMetrics {
    fn new(_now: Instant) -> Self {
        Self
    }

    fn record_launch_batch(&mut self, _selected: usize, _spare_selected: usize) {}

    fn record_launch_failure(&mut self) {}

    fn record_peer_batch(&mut self, _peer_count: usize, _delivered: bool) {}

    fn record_drain_peer_batch(
        &mut self,
        _peer_count: usize,
        _unique_added: usize,
        _initial_unique_peers: usize,
    ) {
    }

    fn record_drain_start(
        &mut self,
        _total_peers: usize,
        _unique_peers: usize,
        _outcome: Option<DemandParkedSliceOutcome>,
    ) {
    }

    fn record_drain_replaced(&mut self) {}

    fn record_drain_rejected(&mut self, _inflight_queries: usize) {}

    fn record_natural_finish(&mut self, _total_peers: usize, _unique_peers: usize) {}

    fn record_drain_finalize(
        &mut self,
        _outcome: DrainedDemandOutcome,
        _duration_ms: u64,
        _after_deadline: bool,
    ) {
    }
}
fn ratio_u64(part: u64, total: u64) -> u64 {
    if total == 0 {
        0
    } else {
        part / total
    }
}

#[derive(Debug, Clone, Default)]
struct DemandSliceClassMetrics {
    fresh_starts: u64,
    resumed_starts: u64,
    selected_reusable_parked: u64,
    selected_useful_yield_history: u64,
    selected_overdue_scarce: u64,
    selected_spare_capacity: u64,
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

    fn record_selection(&mut self, class: DemandSliceClass, reason: DemandSelectionReason) {
        let metrics = self.class_mut(class);
        match reason {
            DemandSelectionReason::ReusableParked => {
                metrics.selected_reusable_parked =
                    metrics.selected_reusable_parked.saturating_add(1)
            }
            DemandSelectionReason::UsefulYieldHistory => {
                metrics.selected_useful_yield_history =
                    metrics.selected_useful_yield_history.saturating_add(1)
            }
            DemandSelectionReason::OverdueScarce => {
                metrics.selected_overdue_scarce = metrics.selected_overdue_scarce.saturating_add(1)
            }
            DemandSelectionReason::SpareCapacity => {
                metrics.selected_spare_capacity = metrics.selected_spare_capacity.saturating_add(1)
            }
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
                || metrics.selected_reusable_parked > 0
                || metrics.selected_useful_yield_history > 0
                || metrics.selected_overdue_scarce > 0
                || metrics.selected_spare_capacity > 0
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
                "{label}(fresh={} resumed={} sel_reuse={} sel_yield={} sel_due={} sel_spare={} natural={} wall={} idle={} first={} cap={} peers={} unique={} reset_stale={} reset_class={} reset_quality={})",
                metrics.fresh_starts,
                metrics.resumed_starts,
                metrics.selected_reusable_parked,
                metrics.selected_useful_yield_history,
                metrics.selected_overdue_scarce,
                metrics.selected_spare_capacity,
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

fn short_info_hash_hex(info_hash: InfoHash) -> String {
    hex::encode(&info_hash.as_ref()[..4])
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn due_in_ms(snapshot: DemandEntrySnapshot, now: Instant) -> u64 {
    duration_ms(snapshot.next_eligible_at.saturating_duration_since(now))
}

fn demand_slice_class_label(class: DemandSliceClass) -> &'static str {
    match class {
        DemandSliceClass::AwaitingMetadata => "awaiting_metadata",
        DemandSliceClass::NoConnectedPeers => "no_connected_peers",
        DemandSliceClass::RoutineRefresh => "routine_refresh",
    }
}

fn demand_selection_reason_label(reason: DemandSelectionReason) -> &'static str {
    match reason {
        DemandSelectionReason::ReusableParked => "reusable_parked",
        DemandSelectionReason::UsefulYieldHistory => "useful_yield_history",
        DemandSelectionReason::OverdueScarce => "overdue_scarce",
        DemandSelectionReason::SpareCapacity => "spare_capacity",
    }
}

fn demand_stop_reason_label(reason: DemandSliceStopReason) -> &'static str {
    match reason {
        DemandSliceStopReason::NaturalFinish => "natural_finish",
        DemandSliceStopReason::WallTime => "wall_time",
        DemandSliceStopReason::IdleTimeout => "idle_timeout",
        DemandSliceStopReason::FirstBatch => "first_batch",
        DemandSliceStopReason::UniquePeerCap => "unique_peer_cap",
    }
}

fn demand_parked_slice_outcome_label(outcome: DemandParkedSliceOutcome) -> &'static str {
    match outcome {
        DemandParkedSliceOutcome::UsefulYield => "useful_yield",
        DemandParkedSliceOutcome::WeakLowYield => "weak_low_yield",
        DemandParkedSliceOutcome::HealthyZeroYield => "healthy_zero_yield",
        DemandParkedSliceOutcome::HealthyLowYield => "healthy_low_yield",
        DemandParkedSliceOutcome::Ignored => "ignored",
    }
}

fn demand_reset_reason_label(reason: DemandCrawlResetReason) -> &'static str {
    match reason {
        DemandCrawlResetReason::Stale => "stale",
        DemandCrawlResetReason::ClassChanged => "class_changed",
        DemandCrawlResetReason::LowQuality => "low_quality",
    }
}

fn demand_diagnostics_log_enabled() -> bool {
    env::var_os("SUPERSEEDR_DHT_DEMAND_LOG").is_some()
}

fn snapshot_class_label(snapshot: Option<DemandEntrySnapshot>) -> &'static str {
    snapshot
        .map(|snapshot| demand_slice_class_label(DemandSliceClass::from_demand(snapshot.demand)))
        .unwrap_or("none")
}

fn log_demand_state_event(
    action: &'static str,
    info_hash: InfoHash,
    requested_demand: Option<DhtDemandState>,
    before: Option<DemandEntrySnapshot>,
    after: Option<DemandEntrySnapshot>,
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    now: Instant,
) {
    let active_lookup_class = demand_lookup_ids
        .get(&info_hash)
        .map(|lookup| demand_slice_class_label(lookup.slice_class))
        .unwrap_or("none");
    let parked_class = parked_crawls
        .get(&info_hash)
        .map(|crawl| demand_slice_class_label(crawl.class))
        .unwrap_or("none");

    tracing::info!(
        target: "superseedr::dht_demand",
        action = action,
        info_hash = %short_info_hash_hex(info_hash),
        requested_demand = ?requested_demand,
        previous_demand = ?before.map(|snapshot| snapshot.demand),
        current_demand = ?after.map(|snapshot| snapshot.demand),
        previous_class = snapshot_class_label(before),
        current_class = snapshot_class_label(after),
        current_subscribers = after.map(|snapshot| snapshot.subscriber_count).unwrap_or(0),
        current_in_progress = after.map(|snapshot| snapshot.in_progress).unwrap_or(false),
        current_retrigger_pending = after.map(|snapshot| snapshot.retrigger_pending).unwrap_or(false),
        current_no_peers_backoff_step = after
            .map(|snapshot| snapshot.no_connected_peers_backoff_step)
            .unwrap_or(0),
        current_due_in_ms = after.map(|snapshot| due_in_ms(snapshot, now)).unwrap_or(0),
        active_lookup_class,
        parked_class,
        "dht demand state"
    );
}

fn log_demand_scheduler_summary(
    demand_scheduler: &DemandScheduler,
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
) {
    let mut entries = DemandSlotCounts::default();
    let mut due = DemandSlotCounts::default();
    let mut in_progress = DemandSlotCounts::default();
    let mut retrigger_pending = 0usize;
    let mut subscribers = 0usize;
    let mut max_no_peers_backoff_step = 0u8;
    let mut next_due_ms: Option<u64> = None;

    for snapshot in demand_scheduler.entry_snapshots() {
        let class = DemandSliceClass::from_demand(snapshot.demand);
        entries.record(class);
        subscribers = subscribers.saturating_add(snapshot.subscriber_count);
        max_no_peers_backoff_step =
            max_no_peers_backoff_step.max(snapshot.no_connected_peers_backoff_step);
        if snapshot.in_progress {
            in_progress.record(class);
        }
        if snapshot.retrigger_pending {
            retrigger_pending = retrigger_pending.saturating_add(1);
        }
        if snapshot.subscriber_count > 0 && !snapshot.in_progress {
            if snapshot.next_eligible_at <= now {
                due.record(class);
            } else {
                let candidate_due_ms = due_in_ms(snapshot, now);
                next_due_ms = Some(
                    next_due_ms.map_or(candidate_due_ms, |current| current.min(candidate_due_ms)),
                );
            }
        }
    }

    let active = active_demand_lookup_slot_counts(demand_lookup_ids);
    let drain_virtual_slots = drain_virtual_slot_count(draining_demands.len());
    planner_budget.refill(now);
    tracing::info!(
        target: "superseedr::dht_demand",
        entries_total = entries.total(),
        entries_awaiting = entries.awaiting_metadata,
        entries_no_peers = entries.no_connected_peers,
        entries_routine = entries.routine_refresh,
        subscribers,
        due_total = due.total(),
        due_awaiting = due.awaiting_metadata,
        due_no_peers = due.no_connected_peers,
        due_routine = due.routine_refresh,
        active_total = active.total(),
        active_awaiting = active.awaiting_metadata,
        active_no_peers = active.no_connected_peers,
        active_routine = active.routine_refresh,
        retrigger_pending,
        parked = parked_crawls.len(),
        draining = draining_demands.len(),
        drain_virtual_slots,
        launch_budget = demand_lookup_launch_budget(demand_lookup_ids, draining_demands.len()),
        budget_awaiting = planner_budget.available(DemandSliceClass::AwaitingMetadata, now),
        budget_no_peers = planner_budget.available(DemandSliceClass::NoConnectedPeers, now),
        budget_routine = planner_budget.available(DemandSliceClass::RoutineRefresh, now),
        next_due_ms = next_due_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        max_no_peers_backoff_step,
        "dht demand summary"
    );
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
        accepting_families: Arc<AtomicBool>,
    },
    CancelLookups {
        lookup_ids: Vec<LookupId>,
    },
    ParkDemandLookups {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: HashSet<SocketAddr>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    },
    FinalizeDrainedDemandLookups {
        info_hash: InfoHash,
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
    let demand_log_enabled = env::var_os("SUPERSEEDR_DHT_DEMAND_LOG").is_some();
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
    let mut demand_lookup_ids: HashMap<InfoHash, ActiveDemandLookup> = HashMap::new();
    let mut parked_crawls: HashMap<InfoHash, DemandCrawlState> = HashMap::new();
    let mut draining_demands: HashMap<InfoHash, DrainingDemandLookup> = HashMap::new();
    let mut planner_state: HashMap<InfoHash, DemandPlannerState> = HashMap::new();
    let mut planner_budget = DemandPlannerBudget::new(Instant::now());
    let mut slice_metrics = DemandSliceMetrics::default();
    let mut soak_metrics = DhtSoakMetrics::new(Instant::now());
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
                draining_demands.clear();
                planner_state.clear();
                planner_budget = DemandPlannerBudget::new(Instant::now());
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
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
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
                let now = Instant::now();
                let previous = if demand_log_enabled {
                    demand_scheduler.entry_snapshot(info_hash)
                } else {
                    None
                };
                demand_scheduler.register(info_hash, demand, now);
                if demand_log_enabled {
                    log_demand_state_event(
                        "register",
                        info_hash,
                        Some(demand),
                        previous,
                        demand_scheduler.entry_snapshot(info_hash),
                        &demand_lookup_ids,
                        &parked_crawls,
                        now,
                    );
                }
                let _ = response_tx.send(Some(subscriber_id));
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UpdateDemand { info_hash, demand }) => {
                let now = Instant::now();
                let previous = if demand_log_enabled {
                    demand_scheduler.entry_snapshot(info_hash)
                } else {
                    None
                };
                demand_scheduler.update(info_hash, demand, now);
                if demand_log_enabled {
                    log_demand_state_event(
                        "update",
                        info_hash,
                        Some(demand),
                        previous,
                        demand_scheduler.entry_snapshot(info_hash),
                        &demand_lookup_ids,
                        &parked_crawls,
                        now,
                    );
                }
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
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
                    if let Some(lookup) = demand_lookup_ids.remove(&info_hash) {
                        park_lookup_ids(
                            active_runtime.as_mut(),
                            &mut parked_crawls,
                            info_hash,
                            slice_class,
                            None,
                            0,
                            lookup.lookup_ids,
                        );
                    }
                    if let Some(drain) = draining_demands.remove(&info_hash) {
                        if let Some(active_runtime) = active_runtime.as_mut() {
                            for lookup_id in drain.lookup_ids {
                                active_runtime.runtime.cancel_lookup(lookup_id);
                            }
                        }
                    }
                }
                if demand_log_enabled && removed {
                    log_demand_state_event(
                        "unregister",
                        info_hash,
                        None,
                        None,
                        demand_scheduler.entry_snapshot(info_hash),
                        &demand_lookup_ids,
                        &parked_crawls,
                        Instant::now(),
                    );
                }
            }
            LoopEvent::Command(DhtCommand::DemandPeers { info_hash, peers }) => {
                recent_unique_peers.record_batch(Instant::now(), &peers);
                if let Some(drain) = draining_demands.get_mut(&info_hash) {
                    let unique_added = drain.record_peers(&peers);
                    soak_metrics.record_drain_peer_batch(
                        peers.len(),
                        unique_added,
                        drain.initial_unique_peers,
                    );
                }
                let Some(subscribers) = demand_subscribers.get_mut(&info_hash) else {
                    soak_metrics.record_peer_batch(peers.len(), false);
                    if demand_log_enabled {
                        tracing::info!(
                            target: "superseedr::dht_demand",
                            info_hash = %short_info_hash_hex(info_hash),
                            peers = peers.len(),
                            "dht demand peers dropped without subscribers"
                        );
                    }
                    continue;
                };

                let subscriber_count_before = subscribers.len();
                soak_metrics.record_peer_batch(peers.len(), true);
                if demand_log_enabled {
                    tracing::info!(
                        target: "superseedr::dht_demand",
                        info_hash = %short_info_hash_hex(info_hash),
                        peers = peers.len(),
                        subscriber_count = subscriber_count_before,
                        "dht demand peers"
                    );
                }
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
                        if let Some(lookup) = demand_lookup_ids.remove(&info_hash) {
                            park_lookup_ids(
                                active_runtime.as_mut(),
                                &mut parked_crawls,
                                info_hash,
                                slice_class,
                                None,
                                0,
                                lookup.lookup_ids,
                            );
                        }
                        if let Some(drain) = draining_demands.remove(&info_hash) {
                            if let Some(active_runtime) = active_runtime.as_mut() {
                                for lookup_id in drain.lookup_ids {
                                    active_runtime.runtime.cancel_lookup(lookup_id);
                                }
                            }
                        }
                    }
                }
                if demand_log_enabled && removed > 0 {
                    log_demand_state_event(
                        "subscriber_prune",
                        info_hash,
                        None,
                        None,
                        demand_scheduler.entry_snapshot(info_hash),
                        &demand_lookup_ids,
                        &parked_crawls,
                        Instant::now(),
                    );
                }
            }
            LoopEvent::Command(DhtCommand::DemandLookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
            }) => {
                let previous = if demand_log_enabled {
                    demand_scheduler.entry_snapshot(info_hash)
                } else {
                    None
                };
                demand_lookup_ids.remove(&info_hash);
                planner_state
                    .entry(info_hash)
                    .or_default()
                    .note_finish(Instant::now(), unique_peers);
                slice_metrics.record_stop(
                    slice_class,
                    DemandSliceStopReason::NaturalFinish,
                    total_peers,
                    unique_peers,
                );
                soak_metrics.record_natural_finish(total_peers, unique_peers);
                let now = Instant::now();
                demand_scheduler.finish(info_hash, now);
                if demand_log_enabled {
                    tracing::info!(
                        target: "superseedr::dht_demand",
                        info_hash = %short_info_hash_hex(info_hash),
                        class = demand_slice_class_label(slice_class),
                        stop_reason = demand_stop_reason_label(DemandSliceStopReason::NaturalFinish),
                        total_peers,
                        unique_peers,
                        "dht demand lookup stopped"
                    );
                    log_demand_state_event(
                        "finish",
                        info_hash,
                        None,
                        previous,
                        demand_scheduler.entry_snapshot(info_hash),
                        &demand_lookup_ids,
                        &parked_crawls,
                        now,
                    );
                }
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
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
                accepting_families,
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
                    accepting_families,
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
                let previous = if demand_log_enabled {
                    demand_scheduler.entry_snapshot(info_hash)
                } else {
                    None
                };
                demand_lookup_ids.remove(&info_hash);
                let initial_unique_peers = unique_peers.len();
                let parked_outcome = drain_lookup_ids(
                    active_runtime.as_mut(),
                    &mut parked_crawls,
                    &mut draining_demands,
                    &command_tx,
                    &mut soak_metrics,
                    info_hash,
                    slice_class,
                    stop_reason,
                    total_peers,
                    unique_peers,
                    lookup_ids,
                );
                let drain_admission = draining_demands.get(&info_hash).map(|drain| {
                    (
                        drain.initial_inflight_queries,
                        drain.score,
                        duration_ms(drain.deadline.saturating_duration_since(drain.started_at)),
                    )
                });
                if parked_outcome.is_none() {
                    planner_state
                        .entry(info_hash)
                        .or_default()
                        .note_finish(Instant::now(), initial_unique_peers);
                    slice_metrics.record_stop(
                        slice_class,
                        stop_reason,
                        total_peers,
                        initial_unique_peers,
                    );
                    demand_scheduler.finish(info_hash, Instant::now());
                }
                if parked_outcome.is_some() {
                    soak_metrics.record_drain_start(
                        total_peers,
                        initial_unique_peers,
                        parked_outcome,
                    );
                }
                if demand_log_enabled {
                    tracing::info!(
                        target: "superseedr::dht_demand",
                        info_hash = %short_info_hash_hex(info_hash),
                        class = demand_slice_class_label(slice_class),
                        stop_reason = demand_stop_reason_label(stop_reason),
                        parked_outcome = parked_outcome
                            .map(demand_parked_slice_outcome_label)
                            .unwrap_or("none"),
                        total_peers,
                        unique_peers = initial_unique_peers,
                        drain_admitted = drain_admission.is_some(),
                        drain_initial_inflight = drain_admission
                            .map(|(inflight, _, _)| inflight)
                            .unwrap_or_default(),
                        drain_score = drain_admission
                            .map(|(_, score, _)| score)
                            .unwrap_or_default(),
                        drain_deadline_ms = drain_admission
                            .map(|(_, _, deadline_ms)| deadline_ms)
                            .unwrap_or_default(),
                        "dht demand lookup draining"
                    );
                    log_demand_state_event(
                        "park",
                        info_hash,
                        None,
                        previous,
                        demand_scheduler.entry_snapshot(info_hash),
                        &demand_lookup_ids,
                        &parked_crawls,
                        Instant::now(),
                    );
                }
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::FinalizeDrainedDemandLookups { info_hash }) => {
                let previous = if demand_log_enabled {
                    demand_scheduler.entry_snapshot(info_hash)
                } else {
                    None
                };
                let drained_outcome = finalize_drained_demand_lookup(
                    active_runtime.as_mut(),
                    &mut parked_crawls,
                    &mut draining_demands,
                    &command_tx,
                    info_hash,
                );
                if let Some(outcome) = drained_outcome {
                    planner_state
                        .entry(info_hash)
                        .or_default()
                        .note_finish(Instant::now(), outcome.unique_peers);
                    slice_metrics.record_stop(
                        outcome.slice_class,
                        outcome.stop_reason,
                        outcome.total_peers,
                        outcome.unique_peers,
                    );
                    soak_metrics.record_drain_finalize(
                        outcome,
                        outcome.drain_duration_ms,
                        outcome.finalized_after_deadline,
                    );
                    let now = Instant::now();
                    let finish_mode = if outcome.slice_class == DemandSliceClass::NoConnectedPeers
                        && outcome.parked_outcome
                            == Some(DemandParkedSliceOutcome::HealthyZeroYield)
                    {
                        DemandFinishMode::AcceleratedNoConnectedPeersBackoff
                    } else {
                        DemandFinishMode::Standard
                    };
                    demand_scheduler.finish_with_mode(info_hash, now, finish_mode);
                    if demand_log_enabled {
                        tracing::info!(
                            target: "superseedr::dht_demand",
                            info_hash = %short_info_hash_hex(info_hash),
                            class = demand_slice_class_label(outcome.slice_class),
                            stop_reason = demand_stop_reason_label(outcome.stop_reason),
                            parked_outcome = outcome
                                .parked_outcome
                                .map(demand_parked_slice_outcome_label)
                                .unwrap_or("none"),
                            accelerated_no_peers_backoff = matches!(
                                finish_mode,
                                DemandFinishMode::AcceleratedNoConnectedPeersBackoff
                            ),
                            total_peers = outcome.total_peers,
                            unique_peers = outcome.unique_peers,
                            drain_duration_ms = outcome.drain_duration_ms,
                            finalized_after_deadline = outcome.finalized_after_deadline,
                            finalized_early_no_yield = outcome.finalized_early_no_yield,
                            parked = parked_crawls.contains_key(&info_hash),
                            "dht demand drain finalized"
                        );
                        log_demand_state_event(
                            "drain_finish",
                            info_hash,
                            None,
                            previous,
                            demand_scheduler.entry_snapshot(info_hash),
                            &demand_lookup_ids,
                            &parked_crawls,
                            now,
                        );
                    }
                }
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_scheduler,
                    &mut demand_lookup_ids,
                    &mut parked_crawls,
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
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
                    &draining_demands,
                    &mut planner_state,
                    &mut planner_budget,
                    &mut slice_metrics,
                    &mut soak_metrics,
                    demand_log_enabled,
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
                if demand_log_enabled {
                    log_demand_scheduler_summary(
                        &demand_scheduler,
                        &demand_lookup_ids,
                        &draining_demands,
                        &parked_crawls,
                        &mut planner_budget,
                        Instant::now(),
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
            accepting_families: Arc::new(AtomicBool::new(false)),
        });
    };

    let lookup_ids = Arc::new(StdMutex::new(Vec::new()));
    let (merged_tx, merged_rx) = mpsc::unbounded_channel();
    let first_batch_seen = Arc::new(AtomicBool::new(false));
    let accepting_families = Arc::new(AtomicBool::new(true));

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
            accepting_families.clone(),
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
        let accepting_families = accepting_families.clone();
        tokio::spawn(async move {
            tokio::time::sleep(DHT_IPV6_HEDGE_DELAY).await;
            if merged_tx.is_closed() || !accepting_families.load(Ordering::Acquire) {
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
                accepting_families,
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
            accepting_families: Arc::new(AtomicBool::new(false)),
        });
    }

    drop(merged_tx);

    Ok(StartedLookup {
        lookup_ids,
        receiver: merged_rx,
        accepting_families,
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
            if demand_diagnostics_log_enabled() {
                let quality = aggregate_parked_crawl_quality(crawl);
                tracing::info!(
                    target: "superseedr::dht_demand",
                    info_hash = %short_info_hash_hex(info_hash),
                    requested_class = demand_slice_class_label(slice_class),
                    previous_class = demand_slice_class_label(crawl.class),
                    reset_reason = demand_reset_reason_label(reason),
                    requested_family = ?family,
                    parked_age_ms = duration_ms(now.saturating_duration_since(crawl.updated_at)),
                    low_yield_streak = crawl.consecutive_stalled_low_yield_slices,
                    reset_count = crawl.reset_count,
                    healthy_zero_yield_streak = crawl.consecutive_healthy_zero_yield_slices,
                    frontier = quality.frontier_len,
                    inflight = quality.inflight_len,
                    visited = quality.visited_len,
                    eligible_responders = quality.eligible_responder_count,
                    received_peers = quality.received_peer_count,
                    "dht demand parked crawl reset"
                );
            }
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
) -> Option<DemandParkedSliceOutcome> {
    if states.is_empty() {
        return None;
    }

    let now = Instant::now();
    let quality = aggregate_lookup_quality(&states);
    let parked_outcome =
        parked_slice_outcome_for_quality(slice_class, stop_reason, unique_peers, quality);
    let weak_parked_state = slice_class.parked_quality_is_weak(quality);
    let crawl = parked_crawls
        .entry(info_hash)
        .or_insert_with(|| DemandCrawlState::new(now, slice_class));
    let previous_low_yield_streak = crawl.consecutive_stalled_low_yield_slices;
    let previous_healthy_zero_yield_streak = crawl.consecutive_healthy_zero_yield_slices;
    let previous_reset_count = crawl.reset_count;
    if let Some(outcome) = parked_outcome {
        crawl.observe_parked_slice(slice_class, outcome);
    }
    if demand_diagnostics_log_enabled() {
        tracing::info!(
            target: "superseedr::dht_demand",
            info_hash = %short_info_hash_hex(info_hash),
            class = demand_slice_class_label(slice_class),
            stop_reason = stop_reason
                .map(demand_stop_reason_label)
                .unwrap_or("none"),
            unique_peers,
            parked_outcome = parked_outcome
                .map(demand_parked_slice_outcome_label)
                .unwrap_or("none"),
            state_count = states.len(),
            frontier = quality.frontier_len,
            inflight = quality.inflight_len,
            visited = quality.visited_len,
            eligible_responders = quality.eligible_responder_count,
            received_peers = quality.received_peer_count,
            weak_parked_state,
            previous_low_yield_streak,
            current_low_yield_streak = crawl.consecutive_stalled_low_yield_slices,
            previous_healthy_zero_yield_streak,
            current_healthy_zero_yield_streak = crawl.consecutive_healthy_zero_yield_slices,
            reset_count = crawl.reset_count,
            reset_count_delta = crawl.reset_count.saturating_sub(previous_reset_count),
            "dht demand parked crawl quality"
        );
    }
    for state in states {
        crawl.store_family_state(slice_class, state);
    }
    parked_outcome
}

fn parked_slice_outcome_for_quality(
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    quality: AggregateLookupQualitySnapshot,
) -> Option<DemandParkedSliceOutcome> {
    let weak_parked_state = slice_class.parked_quality_is_weak(quality);
    stop_reason
        .map(|reason| slice_class.parked_slice_outcome(reason, unique_peers, weak_parked_state))
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
) -> Option<DemandParkedSliceOutcome> {
    let lookup_ids = {
        let mut lookup_ids = lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return None;
        }
        std::mem::take(&mut *lookup_ids)
    };

    let Some(active_runtime) = active_runtime else {
        return None;
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
    )
}

fn schedule_drained_demand_finalize(
    command_tx: &mpsc::UnboundedSender<DhtCommand>,
    info_hash: InfoHash,
    delay: Duration,
) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = command_tx.send(DhtCommand::FinalizeDrainedDemandLookups { info_hash });
    });
}

fn demand_drain_duration(
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    parked_outcome: Option<DemandParkedSliceOutcome>,
    unique_peers: usize,
) -> Option<Duration> {
    let mut duration = match (slice_class, parked_outcome) {
        (DemandSliceClass::AwaitingMetadata, Some(DemandParkedSliceOutcome::UsefulYield)) => {
            Duration::from_secs(5)
        }
        (
            DemandSliceClass::AwaitingMetadata,
            Some(
                DemandParkedSliceOutcome::WeakLowYield
                | DemandParkedSliceOutcome::HealthyZeroYield
                | DemandParkedSliceOutcome::HealthyLowYield,
            ),
        ) => Duration::from_secs(2),
        (DemandSliceClass::AwaitingMetadata, _) if unique_peers > 0 => Duration::from_secs(2),
        (DemandSliceClass::NoConnectedPeers, Some(DemandParkedSliceOutcome::UsefulYield)) => {
            Duration::from_secs(5)
        }
        (DemandSliceClass::NoConnectedPeers, Some(DemandParkedSliceOutcome::HealthyLowYield)) => {
            Duration::from_secs(2)
        }
        (
            DemandSliceClass::NoConnectedPeers,
            Some(
                DemandParkedSliceOutcome::WeakLowYield | DemandParkedSliceOutcome::HealthyZeroYield,
            ),
        ) => Duration::from_secs(1),
        (DemandSliceClass::RoutineRefresh, Some(DemandParkedSliceOutcome::UsefulYield)) => {
            Duration::from_secs(2)
        }
        (
            DemandSliceClass::RoutineRefresh,
            Some(
                DemandParkedSliceOutcome::WeakLowYield
                | DemandParkedSliceOutcome::HealthyZeroYield
                | DemandParkedSliceOutcome::HealthyLowYield,
            ),
        ) => Duration::from_secs(1),
        _ => Duration::ZERO,
    };

    if matches!(stop_reason, DemandSliceStopReason::UniquePeerCap) {
        duration = duration.min(Duration::from_secs(2));
    }
    if matches!(stop_reason, DemandSliceStopReason::FirstBatch) {
        duration = duration.min(Duration::from_secs(1));
    }
    if matches!(stop_reason, DemandSliceStopReason::IdleTimeout) && unique_peers == 0 {
        duration = duration.min(Duration::from_secs(1));
    }

    (duration > Duration::ZERO).then_some(duration)
}

fn demand_drain_no_late_yield_grace(slice_class: DemandSliceClass) -> Duration {
    match slice_class {
        DemandSliceClass::AwaitingMetadata => DHT_AWAITING_METADATA_DRAIN_NO_LATE_YIELD_GRACE,
        DemandSliceClass::NoConnectedPeers => DHT_DEMAND_DRAIN_NO_LATE_YIELD_GRACE,
        DemandSliceClass::RoutineRefresh => DHT_ROUTINE_DRAIN_NO_LATE_YIELD_GRACE,
    }
}

fn demand_drain_score(
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    parked_outcome: Option<DemandParkedSliceOutcome>,
    unique_peers: usize,
    inflight_queries: usize,
) -> i32 {
    let class_score = match slice_class {
        DemandSliceClass::AwaitingMetadata => 60,
        DemandSliceClass::NoConnectedPeers => 30,
        DemandSliceClass::RoutineRefresh => 5,
    };
    let outcome_score = match parked_outcome {
        Some(DemandParkedSliceOutcome::UsefulYield) => 60,
        Some(DemandParkedSliceOutcome::HealthyLowYield) => 15,
        Some(DemandParkedSliceOutcome::WeakLowYield) => 5,
        Some(DemandParkedSliceOutcome::HealthyZeroYield) => -20,
        Some(DemandParkedSliceOutcome::Ignored) | None => -80,
    };
    let stop_score = match stop_reason {
        DemandSliceStopReason::NaturalFinish => -80,
        DemandSliceStopReason::IdleTimeout => -15,
        DemandSliceStopReason::WallTime => 0,
        DemandSliceStopReason::FirstBatch => -5,
        DemandSliceStopReason::UniquePeerCap => -10,
    };
    let peer_score = unique_peers.min(64) as i32;
    let inflight_penalty = (inflight_queries / 12) as i32;

    class_score + outcome_score + stop_score + peer_score - inflight_penalty
}

fn draining_demand_inflight(
    active_runtime: &ActiveRuntime,
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
) -> usize {
    draining_demands
        .values()
        .flat_map(|drain| drain.lookup_ids.iter().copied())
        .filter_map(|lookup_id| active_runtime.runtime.lookup_quality_snapshot(lookup_id))
        .map(|snapshot| snapshot.inflight_len)
        .sum()
}

fn cancel_lookup_ids_to_parked(
    active_runtime: &mut ActiveRuntime,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    unique_peer_count: usize,
    lookup_ids: Vec<LookupId>,
) {
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
        Some(stop_reason),
        unique_peer_count,
        parked_states,
    );
}

fn drain_lookup_ids(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    command_tx: &mpsc::UnboundedSender<DhtCommand>,
    soak_metrics: &mut DhtSoakMetrics,
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    total_peers: usize,
    unique_peers: HashSet<SocketAddr>,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
) -> Option<DemandParkedSliceOutcome> {
    let lookup_ids = {
        let mut lookup_ids = lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return None;
        }
        std::mem::take(&mut *lookup_ids)
    };

    let Some(active_runtime) = active_runtime else {
        return None;
    };

    if let Some(previous) = draining_demands.remove(&info_hash) {
        soak_metrics.record_drain_replaced();
        for lookup_id in previous.lookup_ids {
            active_runtime.runtime.cancel_lookup(lookup_id);
        }
    }

    let mut quality = AggregateLookupQualitySnapshot::default();
    let mut drainable_lookup_ids = Vec::new();
    for lookup_id in lookup_ids {
        if let Some(snapshot) = active_runtime.runtime.lookup_quality_snapshot(lookup_id) {
            quality.extend(snapshot);
            drainable_lookup_ids.push(lookup_id);
        }
    }

    if drainable_lookup_ids.is_empty() {
        return None;
    }

    let unique_peer_count = unique_peers.len();
    let parked_outcome = parked_slice_outcome_for_quality(
        slice_class,
        Some(stop_reason),
        unique_peer_count,
        quality,
    );
    let drain_duration =
        demand_drain_duration(slice_class, stop_reason, parked_outcome, unique_peer_count);
    let drain_score = demand_drain_score(
        slice_class,
        stop_reason,
        parked_outcome,
        unique_peer_count,
        quality.inflight_len,
    );
    let current_drain_inflight = draining_demand_inflight(active_runtime, draining_demands);
    let over_inflight_cap = current_drain_inflight.saturating_add(quality.inflight_len)
        > DHT_DEMAND_DRAIN_MAX_INFLIGHT_QUERIES;
    if quality.inflight_len == 0
        || drain_duration.is_none()
        || drain_score <= 0
        || over_inflight_cap
    {
        soak_metrics.record_drain_rejected(quality.inflight_len);
        cancel_lookup_ids_to_parked(
            active_runtime,
            parked_crawls,
            info_hash,
            slice_class,
            stop_reason,
            unique_peer_count,
            drainable_lookup_ids,
        );
        return None;
    }

    let mut drained_lookup_ids = Vec::new();
    for lookup_id in drainable_lookup_ids {
        if active_runtime
            .runtime
            .pause_lookup_for_drain(lookup_id)
            .is_some()
        {
            drained_lookup_ids.push(lookup_id);
        }
    }

    if drained_lookup_ids.is_empty() {
        return None;
    }

    let now = Instant::now();
    let drain_duration = drain_duration.expect("checked drain duration");
    let no_late_yield_grace = demand_drain_no_late_yield_grace(slice_class).min(drain_duration);
    draining_demands.insert(
        info_hash,
        DrainingDemandLookup {
            lookup_ids: drained_lookup_ids,
            slice_class,
            stop_reason,
            started_at: now,
            total_peers,
            initial_unique_peers: unique_peer_count,
            unique_peers,
            deadline: now + drain_duration,
            no_late_yield_deadline: now + no_late_yield_grace,
            initial_inflight_queries: quality.inflight_len,
            score: drain_score,
        },
    );
    schedule_drained_demand_finalize(command_tx, info_hash, DHT_DEMAND_DRAIN_POLL_INTERVAL);
    parked_outcome
}

fn finalize_drained_demand_lookup(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    command_tx: &mpsc::UnboundedSender<DhtCommand>,
    info_hash: InfoHash,
) -> Option<DrainedDemandOutcome> {
    let drain = draining_demands.get(&info_hash).cloned()?;
    let now = Instant::now();
    let ready = active_runtime
        .as_ref()
        .is_none_or(|active| active.runtime.drained_lookups_ready(&drain.lookup_ids));
    let early_no_yield =
        !ready && now >= drain.no_late_yield_deadline && drain.late_unique_peer_count() == 0;
    if !ready && !early_no_yield && now < drain.deadline {
        schedule_drained_demand_finalize(command_tx, info_hash, DHT_DEMAND_DRAIN_POLL_INTERVAL);
        return None;
    }

    let drain = draining_demands.remove(&info_hash)?;
    let drain_duration_ms = drain.duration_ms(now);
    let finalized_after_deadline = now >= drain.deadline;
    let unique_peers = drain.unique_peer_count();
    let mut drained_states = Vec::new();
    if let Some(active_runtime) = active_runtime {
        for lookup_id in drain.lookup_ids {
            if let Some(state) = active_runtime.runtime.finish_drained_lookup(lookup_id) {
                drained_states.push(state);
            }
        }
    }

    let parked_outcome = store_parked_lookup_states(
        parked_crawls,
        info_hash,
        drain.slice_class,
        Some(drain.stop_reason),
        unique_peers,
        drained_states,
    );

    Some(DrainedDemandOutcome {
        slice_class: drain.slice_class,
        stop_reason: drain.stop_reason,
        total_peers: drain.total_peers,
        unique_peers,
        parked_outcome,
        drain_duration_ms,
        finalized_after_deadline,
        finalized_early_no_yield: early_no_yield && !finalized_after_deadline,
    })
}

fn evict_stale_parked_crawls(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    now: Instant,
) {
    parked_crawls.retain(|_, crawl| !crawl.is_stale(now) && !crawl.is_empty());
}

fn active_demand_lookup_slot_count(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
) -> usize {
    demand_lookup_ids.len()
}

fn active_demand_lookup_slot_counts(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
) -> DemandSlotCounts {
    let mut counts = DemandSlotCounts::default();
    for lookup in demand_lookup_ids.values() {
        counts.record(lookup.slice_class);
    }
    counts
}

fn draining_demand_slot_counts(
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
) -> DemandSlotCounts {
    let mut counts = DemandSlotCounts::default();
    for drain in draining_demands.values() {
        counts.record(drain.slice_class);
    }
    counts
}

fn drain_virtual_slot_count(draining_lookup_count: usize) -> usize {
    if draining_lookup_count == 0 {
        0
    } else {
        draining_lookup_count.saturating_add(DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT - 1)
            / DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT
    }
}

fn demand_lookup_launch_budget(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
    draining_lookup_count: usize,
) -> usize {
    let consumed_slots = active_demand_lookup_slot_count(demand_lookup_ids)
        .saturating_add(drain_virtual_slot_count(draining_lookup_count));
    let available_slots = DHT_DEMAND_LOOKUP_SLOT_COUNT.saturating_sub(consumed_slots);
    available_slots.min(DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK)
}

fn demand_lookup_class_slot_cap(class: DemandSliceClass) -> usize {
    match class {
        DemandSliceClass::AwaitingMetadata => DHT_AWAITING_METADATA_SLOT_CAP,
        DemandSliceClass::NoConnectedPeers => DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        DemandSliceClass::RoutineRefresh => DHT_ROUTINE_LOOKUP_SLOT_CAP,
    }
}

fn demand_slice_class_priority(class: DemandSliceClass) -> u8 {
    match class {
        DemandSliceClass::AwaitingMetadata => 3,
        DemandSliceClass::NoConnectedPeers => 2,
        DemandSliceClass::RoutineRefresh => 1,
    }
}

fn due_candidate_has_reusable_parked_crawl(
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    candidate: DueDemandCandidate,
    now: Instant,
) -> bool {
    let class = DemandSliceClass::from_demand(candidate.demand);
    parked_crawls
        .get(&candidate.info_hash)
        .is_some_and(|crawl| !crawl.is_empty() && !crawl.should_reset_for(class, now))
}

fn candidate_last_useful_yield_age(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> Option<Duration> {
    planner_state
        .get(&info_hash)
        .and_then(|state| state.last_useful_yield_at)
        .map(|at| now.saturating_duration_since(at))
}

fn candidate_last_unique_peers(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
) -> usize {
    planner_state
        .get(&info_hash)
        .map(|state| state.last_unique_peers)
        .unwrap_or(0)
}

fn candidate_has_useful_yield_history(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> bool {
    candidate_last_useful_yield_age(planner_state, info_hash, now).is_some()
        && candidate_last_unique_peers(planner_state, info_hash) > 0
}

fn candidate_selection_reason(
    candidate: DueDemandCandidate,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    now: Instant,
) -> DemandSelectionReason {
    if candidate_has_useful_yield_history(planner_state, candidate.info_hash, now) {
        DemandSelectionReason::UsefulYieldHistory
    } else if due_candidate_has_reusable_parked_crawl(parked_crawls, candidate, now) {
        DemandSelectionReason::ReusableParked
    } else {
        DemandSelectionReason::OverdueScarce
    }
}

fn candidate_last_activity_age(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> Option<Duration> {
    planner_state.get(&info_hash).and_then(|state| {
        state
            .last_finished_at
            .or(state.last_started_at)
            .map(|at| now.saturating_duration_since(at))
    })
}

fn spare_research_candidate_ready(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> bool {
    candidate_last_activity_age(planner_state, info_hash, now)
        .map(|age| age >= DHT_DEMAND_SPARE_RESEARCH_MIN_INTERVAL)
        .unwrap_or(true)
}

fn select_spare_research_launches(
    demand_snapshots: &[DemandEntrySnapshot],
    active_counts: DemandSlotCounts,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
    total_budget: usize,
) -> Vec<DueDemandCandidate> {
    if total_budget == 0 || active_counts.total() >= DHT_DEMAND_SPARE_RESEARCH_MAX_ACTIVE {
        return Vec::new();
    }

    let take_count = total_budget.min(DHT_DEMAND_SPARE_RESEARCH_LAUNCH_LIMIT);
    let mut candidates = demand_snapshots
        .iter()
        .copied()
        .filter(|snapshot| {
            snapshot.subscriber_count > 0
                && !snapshot.in_progress
                && snapshot.next_eligible_at > now
                && DemandSliceClass::from_demand(snapshot.demand)
                    == DemandSliceClass::NoConnectedPeers
                && spare_research_candidate_ready(planner_state, snapshot.info_hash, now)
        })
        .map(|snapshot| DueDemandCandidate {
            info_hash: snapshot.info_hash,
            demand: snapshot.demand,
            next_eligible_at: snapshot.next_eligible_at,
            subscriber_count: snapshot.subscriber_count,
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        let left_activity_age = candidate_last_activity_age(planner_state, left.info_hash, now);
        let right_activity_age = candidate_last_activity_age(planner_state, right.info_hash, now);
        let left_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *left, now);
        let right_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *right, now);
        match (left_activity_age, right_activity_age) {
            (Some(left_age), Some(right_age)) => right_age.cmp(&left_age),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| left.next_eligible_at.cmp(&right.next_eligible_at))
        .then_with(|| right_reusable.cmp(&left_reusable))
        .then_with(|| {
            left.demand
                .connected_peers
                .cmp(&right.demand.connected_peers)
        })
        .then_with(|| right.subscriber_count.cmp(&left.subscriber_count))
    });

    let mut selected = Vec::new();
    for candidate in candidates {
        if selected.len() >= take_count {
            break;
        }
        if !planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, now) {
            break;
        }
        selected.push(candidate);
    }

    selected
}

fn select_due_demand_launches(
    due_candidates: &[DueDemandCandidate],
    active_counts: DemandSlotCounts,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
    total_budget: usize,
) -> Vec<DueDemandCandidate> {
    let mut selected = Vec::new();
    let mut planned_counts = active_counts;
    let mut candidates = due_candidates.to_vec();

    candidates.sort_by(|left, right| {
        let left_class = DemandSliceClass::from_demand(left.demand);
        let right_class = DemandSliceClass::from_demand(right.demand);
        let left_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *left, now);
        let right_reusable = due_candidate_has_reusable_parked_crawl(parked_crawls, *right, now);
        let left_useful_age = candidate_last_useful_yield_age(planner_state, left.info_hash, now);
        let right_useful_age = candidate_last_useful_yield_age(planner_state, right.info_hash, now);
        let left_last_unique = candidate_last_unique_peers(planner_state, left.info_hash);
        let right_last_unique = candidate_last_unique_peers(planner_state, right.info_hash);

        demand_slice_class_priority(right_class)
            .cmp(&demand_slice_class_priority(left_class))
            .then_with(|| match (left_useful_age, right_useful_age) {
                (Some(left_age), Some(right_age)) => left_age.cmp(&right_age),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then_with(|| right_last_unique.cmp(&left_last_unique))
            .then_with(|| right_reusable.cmp(&left_reusable))
            .then_with(|| {
                now.saturating_duration_since(right.next_eligible_at)
                    .cmp(&now.saturating_duration_since(left.next_eligible_at))
            })
            .then_with(|| {
                left.demand
                    .connected_peers
                    .cmp(&right.demand.connected_peers)
            })
            .then_with(|| right.subscriber_count.cmp(&left.subscriber_count))
    });

    for candidate in candidates {
        if selected.len() >= total_budget {
            break;
        }

        let class = DemandSliceClass::from_demand(candidate.demand);
        if planned_counts.count(class) >= demand_lookup_class_slot_cap(class) {
            continue;
        }
        if !planner_budget.try_consume(class, now) {
            continue;
        }
        planned_counts.record(class);
        selected.push(candidate);
    }

    selected
}

async fn start_due_demands(
    active_runtime: Option<&mut ActiveRuntime>,
    command_tx: &mpsc::UnboundedSender<DhtCommand>,
    demand_scheduler: &mut DemandScheduler,
    demand_lookup_ids: &mut HashMap<InfoHash, ActiveDemandLookup>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
    planner_state: &mut HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    slice_metrics: &mut DemandSliceMetrics,
    soak_metrics: &mut DhtSoakMetrics,
    demand_log_enabled: bool,
) {
    let Some(active_runtime) = active_runtime else {
        return;
    };

    evict_stale_parked_crawls(parked_crawls, Instant::now());
    let drain_virtual_slots = drain_virtual_slot_count(draining_demands.len());
    let launch_budget = demand_lookup_launch_budget(demand_lookup_ids, draining_demands.len());
    if launch_budget == 0 {
        return;
    }
    let now = Instant::now();
    planner_budget.refill(now);
    let active_counts = active_demand_lookup_slot_counts(demand_lookup_ids);
    let due_candidates = demand_scheduler
        .due_candidates(now)
        .into_iter()
        .filter(|candidate| !draining_demands.contains_key(&candidate.info_hash))
        .collect::<Vec<_>>();
    let demand_snapshots = demand_scheduler
        .entry_snapshots()
        .into_iter()
        .filter(|snapshot| !draining_demands.contains_key(&snapshot.info_hash))
        .collect::<Vec<_>>();
    let due_launches = select_due_demand_launches(
        &due_candidates,
        active_counts,
        parked_crawls,
        planner_state,
        planner_budget,
        now,
        launch_budget,
    );
    let mut planned_launches = due_launches
        .into_iter()
        .map(|candidate| {
            (
                candidate,
                candidate_selection_reason(candidate, parked_crawls, planner_state, now),
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
        .filter(|(_, reason)| *reason == DemandSelectionReason::SpareCapacity)
        .count();
    if !planned_launches.is_empty() {
        soak_metrics.record_launch_batch(planned_launches.len(), spare_selected);
    }

    if demand_log_enabled && !planned_launches.is_empty() {
        tracing::info!(
            target: "superseedr::dht_demand",
            launch_budget,
            due_total = due_candidates.len(),
            selected = planned_launches.len(),
            spare_selected,
            active_total = active_counts.total(),
            active_awaiting = active_counts.awaiting_metadata,
            active_no_peers = active_counts.no_connected_peers,
            active_routine = active_counts.routine_refresh,
            parked = parked_crawls.len(),
            draining = draining_demands.len(),
            drain_virtual_slots,
            budget_awaiting = planner_budget.available(DemandSliceClass::AwaitingMetadata, now),
            budget_no_peers = planner_budget.available(DemandSliceClass::NoConnectedPeers, now),
            budget_routine = planner_budget.available(DemandSliceClass::RoutineRefresh, now),
            "dht demand launch batch"
        );
    }
    for (candidate, selection_reason) in planned_launches {
        let info_hash = candidate.info_hash;
        if !demand_scheduler.mark_in_progress(info_hash) {
            planner_budget.refund(DemandSliceClass::from_demand(candidate.demand));
            if demand_log_enabled {
                tracing::info!(
                    target: "superseedr::dht_demand",
                    info_hash = %short_info_hash_hex(info_hash),
                    demand = ?candidate.demand,
                    "dht demand launch skipped after planner selection"
                );
            }
            continue;
        }
        let plan = DemandLookupPlan::for_demand(candidate.demand);
        slice_metrics.record_selection(plan.class, selection_reason);
        planner_state.entry(info_hash).or_default().note_start(now);
        if demand_log_enabled {
            tracing::info!(
                target: "superseedr::dht_demand",
                info_hash = %short_info_hash_hex(info_hash),
                class = demand_slice_class_label(plan.class),
                selection_reason = demand_selection_reason_label(selection_reason),
                awaiting_metadata = candidate.demand.awaiting_metadata,
                connected_peers = candidate.demand.connected_peers,
                subscribers = candidate.subscriber_count,
                overdue_ms = duration_ms(now.saturating_duration_since(candidate.next_eligible_at)),
                early_ms = duration_ms(candidate.next_eligible_at.saturating_duration_since(now)),
                parked = parked_crawls.contains_key(&info_hash),
                idle_timeout_ms = duration_ms(plan.idle_timeout),
                max_wall_time_ms = duration_ms(plan.max_wall_time),
                unique_peer_cap = plan.unique_peer_cap,
                stop_after_first_batch = plan.stop_after_first_batch,
                "dht demand launch"
            );
        }
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
                if demand_log_enabled {
                    let lookup_count = started
                        .lookup_ids
                        .lock()
                        .expect("managed dht lookup ids lock")
                        .len();
                    tracing::info!(
                        target: "superseedr::dht_demand",
                        info_hash = %short_info_hash_hex(info_hash),
                        class = demand_slice_class_label(plan.class),
                        lookup_count,
                        "dht demand launch started"
                    );
                }
                demand_lookup_ids.insert(
                    info_hash,
                    ActiveDemandLookup {
                        lookup_ids: started.lookup_ids.clone(),
                        slice_class: plan.class,
                    },
                );
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
                        accepting_families.store(false, Ordering::Release);
                        let _ = command_tx.send(DhtCommand::ParkDemandLookups {
                            info_hash,
                            slice_class: plan.class,
                            stop_reason: reason,
                            total_peers,
                            unique_peers,
                            lookup_ids,
                        });
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
                                    let _ = command_tx.send(DhtCommand::DemandPeers {
                                        info_hash,
                                        peers,
                                    });
                                }
                            }
                        }
                    } else {
                        let unique_peer_count = unique_peers.len();
                        let _ = command_tx.send(DhtCommand::DemandLookupFinished {
                            info_hash,
                            slice_class: plan.class,
                            total_peers,
                            unique_peers: unique_peer_count,
                        });
                    }
                });
            }
            Err(error) => {
                planner_budget.refund(plan.class);
                soak_metrics.record_launch_failure();
                if demand_log_enabled {
                    tracing::info!(
                        target: "superseedr::dht_demand",
                        info_hash = %short_info_hash_hex(info_hash),
                        class = demand_slice_class_label(plan.class),
                        error = %error,
                        "dht demand launch failed"
                    );
                }
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
    accepting_families: Arc<AtomicBool>,
) -> Result<(), String> {
    let Some(active_runtime) = active_runtime else {
        return Ok(());
    };
    if !accepting_families.load(Ordering::Acquire) {
        return Ok(());
    }
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
            DemandParkedSliceOutcome::HealthyZeroYield,
        );
        assert_eq!(
            low_quality.reset_reason_for(
                DemandSliceClass::RoutineRefresh,
                now + Duration::from_secs(1)
            ),
            None
        );
        assert_eq!(low_quality.consecutive_healthy_zero_yield_slices, 1);
        low_quality.observe_parked_slice(
            DemandSliceClass::RoutineRefresh,
            DemandParkedSliceOutcome::HealthyZeroYield,
        );
        assert_eq!(
            low_quality.reset_reason_for(
                DemandSliceClass::RoutineRefresh,
                now + Duration::from_secs(1)
            ),
            None
        );
        assert_eq!(low_quality.consecutive_healthy_zero_yield_slices, 2);
        low_quality.observe_parked_slice(
            DemandSliceClass::RoutineRefresh,
            DemandParkedSliceOutcome::WeakLowYield,
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
            DemandParkedSliceOutcome::WeakLowYield,
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
            DemandParkedSliceOutcome::HealthyLowYield,
        );
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandParkedSliceOutcome::HealthyLowYield,
        );
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandParkedSliceOutcome::HealthyZeroYield,
        );
        assert_eq!(
            no_peers_low_yield.reset_reason_for(
                DemandSliceClass::NoConnectedPeers,
                now + Duration::from_secs(1)
            ),
            None
        );
        assert_eq!(no_peers_low_yield.consecutive_healthy_zero_yield_slices, 1);
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandParkedSliceOutcome::WeakLowYield,
        );
        no_peers_low_yield.observe_parked_slice(
            DemandSliceClass::NoConnectedPeers,
            DemandParkedSliceOutcome::WeakLowYield,
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
            DemandParkedSliceOutcome::WeakLowYield,
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
            DemandParkedSliceOutcome::UsefulYield,
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
    fn parked_slice_outcome_separates_healthy_zero_from_weak_low_yield() {
        assert_eq!(
            DemandSliceClass::NoConnectedPeers.parked_slice_outcome(
                DemandSliceStopReason::IdleTimeout,
                0,
                false,
            ),
            DemandParkedSliceOutcome::HealthyZeroYield
        );
        assert_eq!(
            DemandSliceClass::NoConnectedPeers.parked_slice_outcome(
                DemandSliceStopReason::IdleTimeout,
                0,
                true,
            ),
            DemandParkedSliceOutcome::WeakLowYield
        );
        assert_eq!(
            DemandSliceClass::NoConnectedPeers.parked_slice_outcome(
                DemandSliceStopReason::WallTime,
                1,
                false,
            ),
            DemandParkedSliceOutcome::HealthyLowYield
        );
        assert_eq!(
            DemandSliceClass::NoConnectedPeers.parked_slice_outcome(
                DemandSliceStopReason::WallTime,
                4,
                true,
            ),
            DemandParkedSliceOutcome::UsefulYield
        );
        assert_eq!(
            DemandSliceClass::NoConnectedPeers.parked_slice_outcome(
                DemandSliceStopReason::UniquePeerCap,
                0,
                true,
            ),
            DemandParkedSliceOutcome::Ignored
        );
    }

    #[test]
    fn draining_demand_records_late_unique_peers_without_double_counting() {
        let initial_peer = peer("127.0.0.1:4000");
        let late_peer = peer("127.0.0.2:4000");
        let mut unique_peers = HashSet::new();
        unique_peers.insert(initial_peer);
        let mut drain = DrainingDemandLookup {
            lookup_ids: vec![LookupId(1)],
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::IdleTimeout,
            started_at: Instant::now(),
            total_peers: 1,
            initial_unique_peers: 1,
            unique_peers,
            deadline: Instant::now() + Duration::from_secs(10),
            no_late_yield_deadline: Instant::now() + Duration::from_secs(2),
            initial_inflight_queries: 8,
            score: 100,
        };

        drain.record_peers(&[initial_peer, late_peer]);

        assert_eq!(drain.total_peers, 3);
        assert_eq!(drain.unique_peer_count(), 2);
        assert_eq!(
            parked_slice_outcome_for_quality(
                drain.slice_class,
                Some(drain.stop_reason),
                drain.unique_peer_count(),
                AggregateLookupQualitySnapshot::default(),
            ),
            Some(DemandParkedSliceOutcome::HealthyLowYield)
        );
    }

    #[test]
    fn drain_policy_prefers_productive_slices_and_rejects_idle_no_peer_work() {
        let productive_score = demand_drain_score(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::WallTime,
            Some(DemandParkedSliceOutcome::UsefulYield),
            24,
            24,
        );
        let idle_zero_score = demand_drain_score(
            DemandSliceClass::NoConnectedPeers,
            DemandSliceStopReason::IdleTimeout,
            Some(DemandParkedSliceOutcome::HealthyZeroYield),
            0,
            24,
        );

        assert!(productive_score > 0);
        assert!(idle_zero_score <= 0);
        assert_eq!(
            demand_drain_duration(
                DemandSliceClass::NoConnectedPeers,
                DemandSliceStopReason::WallTime,
                Some(DemandParkedSliceOutcome::UsefulYield),
                24,
            ),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            demand_drain_duration(
                DemandSliceClass::NoConnectedPeers,
                DemandSliceStopReason::IdleTimeout,
                Some(DemandParkedSliceOutcome::HealthyZeroYield),
                0,
            ),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            demand_drain_duration(
                DemandSliceClass::RoutineRefresh,
                DemandSliceStopReason::UniquePeerCap,
                Some(DemandParkedSliceOutcome::UsefulYield),
                16,
            ),
            Some(Duration::from_secs(2))
        );
    }

    #[test]
    fn demand_slice_metrics_record_starts_stops_and_resets() {
        let mut metrics = DemandSliceMetrics::default();

        metrics.record_start(DemandSliceClass::AwaitingMetadata, false);
        metrics.record_start(DemandSliceClass::AwaitingMetadata, true);
        metrics.record_selection(
            DemandSliceClass::AwaitingMetadata,
            DemandSelectionReason::ReusableParked,
        );
        metrics.record_selection(
            DemandSliceClass::NoConnectedPeers,
            DemandSelectionReason::UsefulYieldHistory,
        );
        metrics.record_selection(
            DemandSliceClass::RoutineRefresh,
            DemandSelectionReason::OverdueScarce,
        );
        metrics.record_selection(
            DemandSliceClass::NoConnectedPeers,
            DemandSelectionReason::SpareCapacity,
        );
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
        assert_eq!(metrics.awaiting_metadata.selected_reusable_parked, 1);
        assert_eq!(metrics.no_connected_peers.selected_useful_yield_history, 1);
        assert_eq!(metrics.no_connected_peers.selected_spare_capacity, 1);
        assert_eq!(metrics.routine_refresh.selected_overdue_scarce, 1);
        assert_eq!(metrics.awaiting_metadata.wall_time_stops, 1);
        assert_eq!(metrics.awaiting_metadata.peers_yielded, 12);
        assert_eq!(metrics.awaiting_metadata.unique_peers_yielded, 7);
        assert_eq!(metrics.no_connected_peers.natural_finishes, 1);
        assert_eq!(metrics.routine_refresh.class_change_resets, 1);
        assert_eq!(metrics.routine_refresh.stale_resets, 1);
        assert_eq!(metrics.routine_refresh.low_quality_resets, 1);
        assert!(metrics.summary().contains("awaiting("));
        assert!(metrics.summary().contains("sel_reuse=1"));
        assert!(metrics.summary().contains("sel_yield=1"));
        assert!(metrics.summary().contains("sel_due=1"));
        assert!(metrics.summary().contains("sel_spare=1"));
        assert!(metrics.summary().contains("reset_quality=1"));
    }

    #[test]
    fn demand_lookup_launch_budget_respects_active_slot_cap() {
        let mut active = HashMap::new();
        let make_ids = || Arc::new(StdMutex::new(Vec::<LookupId>::new()));
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);

        assert_eq!(
            demand_lookup_launch_budget(&active, 0),
            DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK
        );

        for byte in 0..6u8 {
            active.insert(
                hash(byte),
                ActiveDemandLookup {
                    lookup_ids: make_ids(),
                    slice_class: DemandSliceClass::NoConnectedPeers,
                },
            );
        }
        assert_eq!(demand_lookup_launch_budget(&active, 0), 2);

        for byte in 6..10u8 {
            active.insert(
                hash(byte),
                ActiveDemandLookup {
                    lookup_ids: make_ids(),
                    slice_class: DemandSliceClass::RoutineRefresh,
                },
            );
        }
        assert_eq!(demand_lookup_launch_budget(&active, 0), 0);
    }

    #[test]
    fn drain_virtual_slots_reduce_launch_budget_fractionally() {
        let mut active = HashMap::new();
        let make_ids = || Arc::new(StdMutex::new(Vec::<LookupId>::new()));
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);

        assert_eq!(drain_virtual_slot_count(0), 0);
        assert_eq!(drain_virtual_slot_count(1), 1);
        assert_eq!(
            drain_virtual_slot_count(DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT),
            1
        );
        assert_eq!(
            drain_virtual_slot_count(DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT + 1),
            2
        );

        for byte in 0..4u8 {
            active.insert(
                hash(byte),
                ActiveDemandLookup {
                    lookup_ids: make_ids(),
                    slice_class: DemandSliceClass::NoConnectedPeers,
                },
            );
        }

        assert_eq!(demand_lookup_launch_budget(&active, 0), 4);
        assert_eq!(demand_lookup_launch_budget(&active, 16), 3);
        assert_eq!(demand_lookup_launch_budget(&active, 54), 0);
    }

    #[test]
    fn select_due_demand_launches_respects_class_slot_caps() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: true,
                    connected_peers: 0,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(2),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(3),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 1,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(4),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 1,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            },
        ];

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_due_demand_launches(
            &due,
            DemandSlotCounts {
                awaiting_metadata: 0,
                no_connected_peers: DHT_NO_CONNECTED_PEERS_SLOT_CAP,
                routine_refresh: DHT_ROUTINE_LOOKUP_SLOT_CAP,
            },
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            1,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].info_hash, hash(1));
    }

    #[test]
    fn select_due_demand_launches_prefers_reusable_parked_crawls_within_class() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(30),
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(2),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(10),
                subscriber_count: 1,
            },
        ];

        let mut parked_crawls = HashMap::new();
        let mut crawl = DemandCrawlState::new(now, DemandSliceClass::NoConnectedPeers);
        let manager =
            crate::dht::lookup::LookupManager::new(crate::dht::lookup::LookupConfig::default());
        let routing = crate::dht::routing::RoutingSnapshot {
            family: AddressFamily::Ipv4,
            buckets: Vec::new(),
            nodes: Vec::new(),
            replacement_count: 0,
            refresh_due_count: 0,
        };
        crawl.ipv4 = Some(manager.start(
            crate::dht::lookup::LookupRequest {
                lookup_id: LookupId(1),
                kind: crate::dht::lookup::LookupKind::GetPeers,
                target: crate::dht::lookup::LookupTarget::InfoHash(hash(2)),
            },
            AddressFamily::Ipv4,
            &routing,
            &[],
            &[],
            now,
        ));
        parked_crawls.insert(hash(2), crawl);

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &parked_crawls,
            &HashMap::new(),
            &mut planner_budget,
            now,
            1,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].info_hash, hash(2));
    }

    #[test]
    fn select_due_demand_launches_prefers_recently_productive_crawls_within_class() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(30),
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(2),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(10),
                subscriber_count: 1,
            },
        ];

        let mut planner_state = HashMap::new();
        planner_state.insert(
            hash(2),
            DemandPlannerState {
                last_started_at: Some(now - Duration::from_secs(20)),
                last_finished_at: Some(now - Duration::from_secs(5)),
                last_useful_yield_at: Some(now - Duration::from_secs(5)),
                last_unique_peers: 8,
            },
        );

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &planner_state,
            &mut planner_budget,
            now,
            1,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].info_hash, hash(2));
    }

    #[test]
    fn select_due_demand_launches_prefers_stale_productive_crawls_within_class() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(60),
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(2),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(10),
                subscriber_count: 1,
            },
        ];

        let mut planner_state = HashMap::new();
        planner_state.insert(
            hash(2),
            DemandPlannerState {
                last_started_at: Some(now - Duration::from_secs(80)),
                last_finished_at: Some(now - Duration::from_secs(70)),
                last_useful_yield_at: Some(now - Duration::from_secs(70)),
                last_unique_peers: 8,
            },
        );

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &planner_state,
            &mut planner_budget,
            now,
            1,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].info_hash, hash(2));
    }

    #[test]
    fn select_due_demand_launches_does_not_bypass_class_cap_for_oldest_due_candidate() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(120),
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(2),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(10),
                subscriber_count: 1,
            },
        ];

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_due_demand_launches(
            &due,
            DemandSlotCounts {
                awaiting_metadata: 0,
                no_connected_peers: DHT_NO_CONNECTED_PEERS_SLOT_CAP,
                routine_refresh: 0,
            },
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            1,
        );

        assert!(selected.is_empty());
    }

    #[test]
    fn demand_planner_budget_caps_repeated_no_peer_launch_batches() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = (0..32u8)
            .map(|byte| DueDemandCandidate {
                info_hash: hash(byte),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            })
            .collect::<Vec<_>>();
        let mut planner_budget = DemandPlannerBudget::new(now);

        let first = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        );
        let second = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        );
        let third = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        );

        assert_eq!(first.len(), DHT_NO_CONNECTED_PEERS_SLOT_CAP);
        assert_eq!(
            second.len(),
            (DHT_NO_CONNECTED_PEERS_LAUNCH_BURST as usize)
                .saturating_sub(DHT_NO_CONNECTED_PEERS_SLOT_CAP)
        );
        assert!(third.is_empty());
    }

    #[test]
    fn demand_planner_budget_refills_no_peer_tokens_over_time() {
        let now = Instant::now();
        let mut planner_budget = DemandPlannerBudget::new(now);

        for _ in 0..DHT_NO_CONNECTED_PEERS_LAUNCH_BURST {
            assert!(planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, now));
        }
        assert!(!planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, now));

        let later = now + Duration::from_secs(2);
        assert!(planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, later));
        assert!(!planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, later));
    }

    #[test]
    fn exhausted_no_peer_budget_does_not_block_metadata_launches() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            },
            DueDemandCandidate {
                info_hash: hash(2),
                demand: DhtDemandState {
                    awaiting_metadata: true,
                    connected_peers: 0,
                },
                next_eligible_at: now,
                subscriber_count: 1,
            },
        ];
        let mut planner_budget = DemandPlannerBudget::new(now);
        for _ in 0..DHT_NO_CONNECTED_PEERS_LAUNCH_BURST {
            assert!(planner_budget.try_consume(DemandSliceClass::NoConnectedPeers, now));
        }

        let selected = select_due_demand_launches(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            2,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].info_hash, hash(2));
    }

    #[test]
    fn no_peer_launch_budget_is_independent_of_catalog_size() {
        fn hash(index: u32) -> InfoHash {
            let mut bytes = [0u8; InfoHash::LEN];
            bytes[..4].copy_from_slice(&index.to_be_bytes());
            InfoHash::from(bytes)
        }

        fn immediate_launches(candidate_count: u32, now: Instant) -> usize {
            let due = (0..candidate_count)
                .map(|index| DueDemandCandidate {
                    info_hash: hash(index),
                    demand: DhtDemandState {
                        awaiting_metadata: false,
                        connected_peers: 0,
                    },
                    next_eligible_at: now,
                    subscriber_count: 1,
                })
                .collect::<Vec<_>>();
            let mut planner_budget = DemandPlannerBudget::new(now);
            let mut selected_count = 0usize;

            for _ in 0..10 {
                selected_count = selected_count.saturating_add(
                    select_due_demand_launches(
                        &due,
                        DemandSlotCounts::default(),
                        &HashMap::new(),
                        &HashMap::new(),
                        &mut planner_budget,
                        now,
                        DHT_DEMAND_LOOKUP_SLOT_COUNT,
                    )
                    .len(),
                );
            }

            selected_count
        }

        let now = Instant::now();
        let hundred = immediate_launches(100, now);
        let thousand = immediate_launches(1000, now);

        assert_eq!(hundred, DHT_NO_CONNECTED_PEERS_LAUNCH_BURST as usize);
        assert_eq!(thousand, hundred);
    }

    #[test]
    fn select_spare_research_launches_uses_idle_capacity_for_backed_off_no_peer_work() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let snapshot = |byte: u8, demand: DhtDemandState| DemandEntrySnapshot {
            info_hash: hash(byte),
            demand,
            next_eligible_at: now + Duration::from_secs(40),
            subscriber_count: 1,
            in_progress: false,
            retrigger_pending: false,
            no_connected_peers_backoff_step: 3,
        };
        let snapshots = vec![
            snapshot(
                1,
                DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
            ),
            snapshot(
                2,
                DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
            ),
            snapshot(
                3,
                DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 4,
                },
            ),
        ];
        let mut planner_state = HashMap::new();
        planner_state.insert(
            hash(1),
            DemandPlannerState {
                last_started_at: Some(now - Duration::from_secs(35)),
                last_finished_at: Some(now - Duration::from_secs(30)),
                last_useful_yield_at: None,
                last_unique_peers: 0,
            },
        );
        planner_state.insert(
            hash(2),
            DemandPlannerState {
                last_started_at: Some(now - Duration::from_secs(10)),
                last_finished_at: Some(now - Duration::from_secs(5)),
                last_useful_yield_at: None,
                last_unique_peers: 0,
            },
        );

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_spare_research_launches(
            &snapshots,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &planner_state,
            &mut planner_budget,
            now,
            4,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].info_hash, hash(1));
    }

    #[test]
    fn select_spare_research_launches_waits_when_demand_lookup_is_active() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let snapshots = vec![DemandEntrySnapshot {
            info_hash: hash(1),
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            next_eligible_at: now + Duration::from_secs(40),
            subscriber_count: 1,
            in_progress: false,
            retrigger_pending: false,
            no_connected_peers_backoff_step: 3,
        }];

        let mut planner_budget = DemandPlannerBudget::new(now);
        let selected = select_spare_research_launches(
            &snapshots,
            DemandSlotCounts {
                awaiting_metadata: 0,
                no_connected_peers: 1,
                routine_refresh: 0,
            },
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            4,
        );

        assert!(selected.is_empty());
    }

    #[test]
    fn candidate_selection_reason_prefers_yield_then_reuse_then_due() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let candidate = DueDemandCandidate {
            info_hash: hash(1),
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            next_eligible_at: now,
            subscriber_count: 1,
        };

        let mut parked_crawls = HashMap::new();
        let manager =
            crate::dht::lookup::LookupManager::new(crate::dht::lookup::LookupConfig::default());
        let routing = crate::dht::routing::RoutingSnapshot {
            family: AddressFamily::Ipv4,
            buckets: Vec::new(),
            nodes: Vec::new(),
            replacement_count: 0,
            refresh_due_count: 0,
        };
        let mut crawl = DemandCrawlState::new(now, DemandSliceClass::NoConnectedPeers);
        crawl.ipv4 = Some(manager.start(
            crate::dht::lookup::LookupRequest {
                lookup_id: LookupId(1),
                kind: crate::dht::lookup::LookupKind::GetPeers,
                target: crate::dht::lookup::LookupTarget::InfoHash(hash(1)),
            },
            AddressFamily::Ipv4,
            &routing,
            &[],
            &[],
            now,
        ));
        parked_crawls.insert(hash(1), crawl);

        assert_eq!(
            candidate_selection_reason(candidate, &parked_crawls, &HashMap::new(), now),
            DemandSelectionReason::ReusableParked
        );

        let mut planner_state = HashMap::new();
        planner_state.insert(
            hash(1),
            DemandPlannerState {
                last_started_at: Some(now - Duration::from_secs(10)),
                last_finished_at: Some(now - Duration::from_secs(5)),
                last_useful_yield_at: Some(now - Duration::from_secs(5)),
                last_unique_peers: 3,
            },
        );
        assert_eq!(
            candidate_selection_reason(candidate, &parked_crawls, &planner_state, now),
            DemandSelectionReason::UsefulYieldHistory
        );

        parked_crawls.clear();
        assert_eq!(
            candidate_selection_reason(candidate, &parked_crawls, &planner_state, now),
            DemandSelectionReason::UsefulYieldHistory
        );

        planner_state.clear();
        assert_eq!(
            candidate_selection_reason(candidate, &parked_crawls, &planner_state, now),
            DemandSelectionReason::OverdueScarce
        );
    }
}
