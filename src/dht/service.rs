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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DemandPlannerSelectionStats {
    offered: DemandSlotCounts,
    launched: DemandSlotCounts,
    throttled: DemandSlotCounts,
    oldest_throttled_awaiting_ms: u64,
    oldest_throttled_no_peers_ms: u64,
    oldest_throttled_routine_ms: u64,
}

impl DemandPlannerSelectionStats {
    fn record_throttled_age(&mut self, class: DemandSliceClass, age_ms: u64) {
        match class {
            DemandSliceClass::AwaitingMetadata => {
                self.oldest_throttled_awaiting_ms = self.oldest_throttled_awaiting_ms.max(age_ms);
            }
            DemandSliceClass::NoConnectedPeers => {
                self.oldest_throttled_no_peers_ms = self.oldest_throttled_no_peers_ms.max(age_ms);
            }
            DemandSliceClass::RoutineRefresh => {
                self.oldest_throttled_routine_ms = self.oldest_throttled_routine_ms.max(age_ms);
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DemandPlannerSelection {
    launches: Vec<DueDemandCandidate>,
    stats: DemandPlannerSelectionStats,
}

#[derive(Debug)]
enum DemandPlannerAction<'a> {
    RuntimeReset {
        now: Instant,
    },
    DemandRegistered {
        info_hash: InfoHash,
        demand: DhtDemandState,
        now: Instant,
    },
    DemandUpdated {
        info_hash: InfoHash,
        demand: DhtDemandState,
        now: Instant,
    },
    DemandSubscriberRemoved {
        info_hash: InfoHash,
    },
    PeersReceived {
        info_hash: InfoHash,
        peers: &'a [SocketAddr],
    },
    DrainTick {
        now: Instant,
        runtime_ready: HashMap<InfoHash, bool>,
    },
    PlanDue {
        now: Instant,
        runtime_available: bool,
    },
    LookupStarted {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    },
    LookupStartFailed {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        now: Instant,
    },
    LookupFinished {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        total_peers: usize,
        unique_peers: usize,
        now: Instant,
    },
    LookupParkRequested {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: HashSet<SocketAddr>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    },
    LookupParkResolved {
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: usize,
        parked_outcome: Option<DemandParkedSliceOutcome>,
        drain_admission: Option<DemandDrainAdmissionSnapshot>,
        previous: Option<DemandEntrySnapshot>,
        now: Instant,
    },
    DrainedLookupFinalized {
        info_hash: InfoHash,
        outcome: DrainedDemandOutcome,
        previous: Option<DemandEntrySnapshot>,
        now: Instant,
    },
}

#[derive(Debug, Clone, Copy)]
struct DemandStartLookupEffect {
    candidate: DueDemandCandidate,
    plan: DemandLookupPlan,
    selection_reason: DemandSelectionReason,
}

#[derive(Debug, Clone, Copy)]
struct DemandLookupFinishedEffect {
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    total_peers: usize,
    unique_peers: usize,
    previous: Option<DemandEntrySnapshot>,
    current: Option<DemandEntrySnapshot>,
    finished_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DemandDrainAdmissionSnapshot {
    initial_inflight_queries: usize,
    score: i32,
    deadline_ms: u64,
}

#[derive(Debug, Clone)]
struct DemandAdmitDrainEffect {
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    total_peers: usize,
    unique_peers: HashSet<SocketAddr>,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    previous: Option<DemandEntrySnapshot>,
}

#[derive(Debug, Clone, Copy)]
struct DemandLookupParkedEffect {
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    stop_reason: DemandSliceStopReason,
    total_peers: usize,
    unique_peers: usize,
    parked_outcome: Option<DemandParkedSliceOutcome>,
    drain_admission: Option<DemandDrainAdmissionSnapshot>,
    previous: Option<DemandEntrySnapshot>,
    current: Option<DemandEntrySnapshot>,
    parked_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct DemandDrainFinalizedEffect {
    info_hash: InfoHash,
    outcome: DrainedDemandOutcome,
    finish_mode: DemandFinishMode,
    previous: Option<DemandEntrySnapshot>,
    current: Option<DemandEntrySnapshot>,
    finalized_at: Instant,
    parked: bool,
}

#[derive(Debug, Clone)]
struct DemandParkActiveLookupEffect {
    info_hash: InfoHash,
    slice_class: DemandSliceClass,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
}

#[derive(Debug, Clone)]
struct DemandCancelDrainingLookupEffect {
    info_hash: InfoHash,
    lookup_ids: Vec<LookupId>,
}

#[derive(Debug, Clone, Copy)]
struct DemandFinalizeDrainingLookupEffect {
    info_hash: InfoHash,
    force: bool,
}

#[derive(Debug, Clone, Copy)]
struct DemandDrainPeersRecordedEffect {
    info_hash: InfoHash,
    peer_count: usize,
    unique_added: usize,
    initial_unique_peers: usize,
}

#[derive(Debug, Clone)]
enum DemandPlannerEffect {
    StartLookup(DemandStartLookupEffect),
    LookupFinished(DemandLookupFinishedEffect),
    AdmitDrain(DemandAdmitDrainEffect),
    LookupParked(DemandLookupParkedEffect),
    DrainFinalized(DemandDrainFinalizedEffect),
    ParkActiveLookup(DemandParkActiveLookupEffect),
    CancelDrainingLookup(DemandCancelDrainingLookupEffect),
    FinalizeDrainingLookup(DemandFinalizeDrainingLookupEffect),
    DrainPeersRecorded(DemandDrainPeersRecordedEffect),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DemandPlannerPlanStats {
    launch_budget: usize,
    due_total: usize,
    selection_stats: DemandPlannerSelectionStats,
    spare_selected: usize,
    active_counts: DemandSlotCounts,
    parked_count: usize,
    draining_count: usize,
    drain_virtual_slots: usize,
    budget_awaiting: usize,
    budget_no_peers: usize,
    budget_routine: usize,
}

#[derive(Debug, Default)]
struct DemandPlannerReduction {
    effects: Vec<DemandPlannerEffect>,
    plan_stats: Option<DemandPlannerPlanStats>,
}

#[derive(Debug)]
struct DemandPlannerModel {
    scheduler: DemandScheduler,
    active: HashMap<InfoHash, ActiveDemandLookup>,
    parked_crawls: HashMap<InfoHash, DemandCrawlState>,
    draining_demands: HashMap<InfoHash, DrainingDemandLookup>,
    state: HashMap<InfoHash, DemandPlannerState>,
    budget: DemandPlannerBudget,
}

impl DemandPlannerModel {
    fn new(now: Instant) -> Self {
        Self {
            scheduler: DemandScheduler::new(
                DHT_ROUTINE_LOOKUP_REFRESH_INTERVAL,
                DHT_NO_CONNECTED_PEERS_BASE_INTERVAL,
                DHT_NO_CONNECTED_PEERS_MAX_INTERVAL,
                DHT_AWAITING_METADATA_REFRESH_INTERVAL,
            ),
            active: HashMap::new(),
            parked_crawls: HashMap::new(),
            draining_demands: HashMap::new(),
            state: HashMap::new(),
            budget: DemandPlannerBudget::new(now),
        }
    }

    fn has_draining_demands(&self) -> bool {
        !self.draining_demands.is_empty()
    }

    fn metadata_waiter_count(&self) -> usize {
        self.scheduler
            .entry_snapshots()
            .into_iter()
            .filter(|snapshot| snapshot.demand.awaiting_metadata && snapshot.subscriber_count > 0)
            .count()
    }

    fn entry_snapshot(&self, info_hash: InfoHash) -> Option<DemandEntrySnapshot> {
        self.scheduler.entry_snapshot(info_hash)
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

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
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

type DhtCommandSender = mpsc::UnboundedSender<DhtCommand>;
type DhtCommandReceiver = mpsc::UnboundedReceiver<DhtCommand>;

fn send_dht_command(
    command_tx: &DhtCommandSender,
    command: DhtCommand,
) -> Result<(), mpsc::error::SendError<DhtCommand>> {
    command_tx.send(command)
}

struct LookupCancelGuard {
    command_tx: DhtCommandSender,
    lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
}

impl Drop for LookupCancelGuard {
    fn drop(&mut self) {
        let mut lookup_ids = self.lookup_ids.lock().expect("managed dht lookup ids lock");
        if lookup_ids.is_empty() {
            return;
        }
        let _ = send_dht_command(
            &self.command_tx,
            DhtCommand::CancelLookups {
                lookup_ids: std::mem::take(&mut *lookup_ids),
            },
        );
    }
}

struct ManagedLookupReceiver {
    receiver: mpsc::UnboundedReceiver<Vec<SocketAddr>>,
    cancel_guard: Option<LookupCancelGuard>,
}

impl ManagedLookupReceiver {
    fn new(
        receiver: mpsc::UnboundedReceiver<Vec<SocketAddr>>,
        command_tx: DhtCommandSender,
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
        command_tx: DhtCommandSender,
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
            let _ = send_dht_command(
                command_tx,
                DhtCommand::UnregisterDemand {
                    info_hash: *info_hash,
                    subscriber_id: *subscriber_id,
                },
            );
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
    DrainTick,
    DemandTick,
    MaintenanceTick,
    HealthTick,
    RuntimeStep(Result<bool, String>),
    CommandClosed,
}

fn command_event(maybe_command: Option<DhtCommand>) -> LoopEvent {
    match maybe_command {
        Some(command) => LoopEvent::Command(command),
        None => LoopEvent::CommandClosed,
    }
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
    command_tx: DhtCommandSender,
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
        let _ = send_dht_command(&self.command_tx, DhtCommand::Reconfigure(config));
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
        command_tx: DhtCommandSender,
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
                if send_dht_command(command_tx, command).is_err() {
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
                if send_dht_command(command_tx, command).is_err() {
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
            DhtHandleInner::Service { command_tx, .. } => send_dht_command(
                command_tx,
                DhtCommand::UpdateDemand {
                    info_hash: InfoHash::from(info_hash),
                    demand,
                },
            )
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
                if send_dht_command(command_tx, command).is_err() {
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
    let mut generation = status_tx.borrow().generation;
    let mut demand_planner = DemandPlannerModel::new(Instant::now());
    let mut demand_subscribers: HashMap<
        InfoHash,
        HashMap<u64, mpsc::UnboundedSender<Vec<SocketAddr>>>,
    > = HashMap::new();
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
                _ = drain_interval.tick(), if demand_planner.has_draining_demands() => LoopEvent::DrainTick,
                maybe_command = command_rx.recv() => command_event(maybe_command),
                _ = demand_tick.tick() => LoopEvent::DemandTick,
                _ = maintenance_interval.tick() => LoopEvent::MaintenanceTick,
                _ = health_interval.tick() => LoopEvent::HealthTick,
                step_result = active.runtime.step() => LoopEvent::RuntimeStep(step_result.map_err(|error| error.to_string())),
            }
        } else {
            tokio::select! {
                _ = shutdown_rx.recv() => LoopEvent::Shutdown,
                _ = drain_interval.tick(), if demand_planner.has_draining_demands() => LoopEvent::DrainTick,
                maybe_command = command_rx.recv() => command_event(maybe_command),
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
                demand_planner.update(DemandPlannerAction::RuntimeReset {
                    now: Instant::now(),
                });
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
                    &mut demand_planner,
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
                let now = Instant::now();
                demand_planner.update(DemandPlannerAction::DemandRegistered {
                    info_hash,
                    demand,
                    now,
                });
                let _ = response_tx.send(Some(subscriber_id));
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_planner,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UpdateDemand { info_hash, demand }) => {
                let now = Instant::now();
                let reduction = demand_planner.update(DemandPlannerAction::DemandUpdated {
                    info_hash,
                    demand,
                    now,
                });
                apply_demand_planner_effects(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    reduction.effects,
                );
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_planner,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::UnregisterDemand {
                info_hash,
                subscriber_id,
            }) => {
                let mut removed = false;
                if let Some(subscribers) = demand_subscribers.get_mut(&info_hash) {
                    removed = subscribers.remove(&subscriber_id).is_some();
                    if subscribers.is_empty() {
                        demand_subscribers.remove(&info_hash);
                    }
                }
                if removed {
                    let reduction = demand_planner
                        .update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
                    apply_demand_planner_effects(
                        active_runtime.as_mut(),
                        &mut demand_planner,
                        &command_tx,
                        &mut slice_metrics,
                        reduction.effects,
                    );
                }
            }
            LoopEvent::Command(DhtCommand::DemandPeers { info_hash, peers }) => {
                recent_unique_peers.record_batch(Instant::now(), &peers);
                let reduction = demand_planner.update(DemandPlannerAction::PeersReceived {
                    info_hash,
                    peers: &peers,
                });
                apply_demand_planner_effects(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    reduction.effects,
                );
                let Some(subscribers) = demand_subscribers.get_mut(&info_hash) else {
                    continue;
                };

                let subscriber_count_before = subscribers.len();
                subscribers.retain(|_, subscriber_tx| subscriber_tx.send(peers.clone()).is_ok());
                let removed = subscriber_count_before.saturating_sub(subscribers.len());
                let mut cleanup_effects = Vec::new();
                for _ in 0..removed {
                    let reduction = demand_planner
                        .update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
                    cleanup_effects.extend(reduction.effects);
                }
                if subscribers.is_empty() {
                    demand_subscribers.remove(&info_hash);
                }
                apply_demand_planner_effects(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    cleanup_effects,
                );
            }
            LoopEvent::Command(DhtCommand::DemandLookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
            }) => {
                let now = Instant::now();
                let reduction = demand_planner.update(DemandPlannerAction::LookupFinished {
                    info_hash,
                    slice_class,
                    total_peers,
                    unique_peers,
                    now,
                });
                apply_demand_planner_effects(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    reduction.effects,
                );
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_planner,
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
                    &mut demand_planner,
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
                    &mut demand_planner,
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
                let requested = demand_planner.update(DemandPlannerAction::LookupParkRequested {
                    info_hash,
                    slice_class,
                    stop_reason,
                    total_peers,
                    unique_peers,
                    lookup_ids,
                });
                apply_demand_planner_effects(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    requested.effects,
                );
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_planner,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::Command(DhtCommand::FinalizeDrainedDemandLookups { info_hash }) => {
                finish_drained_demand_lookup(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    info_hash,
                    false,
                );
                start_due_demands(
                    active_runtime.as_mut(),
                    &command_tx,
                    &mut demand_planner,
                    &mut slice_metrics,
                )
                .await;
            }
            LoopEvent::DrainTick => {
                let runtime_ready = demand_planner.drain_runtime_readiness(active_runtime.as_ref());
                let reduction = demand_planner.update(DemandPlannerAction::DrainTick {
                    now: Instant::now(),
                    runtime_ready,
                });
                let finalized_any = apply_demand_planner_effects(
                    active_runtime.as_mut(),
                    &mut demand_planner,
                    &command_tx,
                    &mut slice_metrics,
                    reduction.effects,
                );
                if finalized_any {
                    start_due_demands(
                        active_runtime.as_mut(),
                        &command_tx,
                        &mut demand_planner,
                        &mut slice_metrics,
                    )
                    .await;
                }
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
                    &mut demand_planner,
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
                let backend = active_runtime
                    .as_ref()
                    .map(|active| active.backend)
                    .unwrap_or(DhtBackendKind::Disabled);
                let bootstrap = active_runtime
                    .as_ref()
                    .map(|active| active.bootstrap)
                    .unwrap_or_default();
                let status = build_status(
                    active_runtime.as_ref(),
                    backend,
                    config.preferred_backend,
                    warning.clone(),
                    generation,
                    bootstrap,
                );
                let _ = status_tx.send(status.clone());
                let _ = recent_unique_peers.unique_count(Instant::now());

                if let Some(active) = active_runtime.as_ref() {
                    let _ = active.runtime.save_state().await;
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
    command_tx: &DhtCommandSender,
    demand_planner: &mut DemandPlannerModel,
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
            demand_planner,
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
            let _ = send_dht_command(
                &command_tx,
                DhtCommand::StartGetPeersFamily {
                    info_hash,
                    family: AddressFamily::Ipv6,
                    slice_class,
                    record_metrics,
                    merged_tx,
                    lookup_ids,
                    first_batch_seen,
                    accepting_families,
                },
            );
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
    let crawl = parked_crawls
        .entry(info_hash)
        .or_insert_with(|| DemandCrawlState::new(now, slice_class));
    if let Some(outcome) = parked_outcome {
        crawl.observe_parked_slice(slice_class, outcome);
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
    command_tx: &DhtCommandSender,
    info_hash: InfoHash,
    delay: Duration,
) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = send_dht_command(
            &command_tx,
            DhtCommand::FinalizeDrainedDemandLookups { info_hash },
        );
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

fn demand_drain_admission_snapshot(drain: &DrainingDemandLookup) -> DemandDrainAdmissionSnapshot {
    DemandDrainAdmissionSnapshot {
        initial_inflight_queries: drain.initial_inflight_queries,
        score: drain.score,
        deadline_ms: duration_ms(drain.deadline.saturating_duration_since(drain.started_at)),
    }
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
    command_tx: &DhtCommandSender,
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

fn drained_demand_lookup_runtime_ready(
    active_runtime: Option<&ActiveRuntime>,
    drain: &DrainingDemandLookup,
) -> bool {
    active_runtime.is_none_or(|active| active.runtime.drained_lookups_ready(&drain.lookup_ids))
}

fn record_drain_peers_received(
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    info_hash: InfoHash,
    peers: &[SocketAddr],
) -> DemandPlannerReduction {
    let Some(drain) = draining_demands.get_mut(&info_hash) else {
        return DemandPlannerReduction::default();
    };
    let unique_added = drain.record_peers(peers);
    DemandPlannerReduction {
        effects: vec![DemandPlannerEffect::DrainPeersRecorded(
            DemandDrainPeersRecordedEffect {
                info_hash,
                peer_count: peers.len(),
                unique_added,
                initial_unique_peers: drain.initial_unique_peers,
            },
        )],
        plan_stats: None,
    }
}

impl DemandPlannerModel {
    fn drain_runtime_readiness(
        &self,
        active_runtime: Option<&ActiveRuntime>,
    ) -> HashMap<InfoHash, bool> {
        self.draining_demands
            .iter()
            .map(|(&info_hash, drain)| {
                (
                    info_hash,
                    drained_demand_lookup_runtime_ready(active_runtime, drain),
                )
            })
            .collect()
    }

    fn take_parked_family_state(
        &mut self,
        slice_metrics: Option<&mut DemandSliceMetrics>,
        info_hash: InfoHash,
        family: AddressFamily,
        slice_class: DemandSliceClass,
    ) -> Option<LookupState> {
        take_parked_family_state(
            &mut self.parked_crawls,
            slice_metrics,
            info_hash,
            family,
            slice_class,
        )
    }

    fn park_lookup_ids(
        &mut self,
        active_runtime: Option<&mut ActiveRuntime>,
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: Option<DemandSliceStopReason>,
        unique_peers: usize,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    ) -> Option<DemandParkedSliceOutcome> {
        park_lookup_ids(
            active_runtime,
            &mut self.parked_crawls,
            info_hash,
            slice_class,
            stop_reason,
            unique_peers,
            lookup_ids,
        )
    }

    fn drain_lookup_ids(
        &mut self,
        active_runtime: Option<&mut ActiveRuntime>,
        command_tx: &DhtCommandSender,
        info_hash: InfoHash,
        slice_class: DemandSliceClass,
        stop_reason: DemandSliceStopReason,
        total_peers: usize,
        unique_peers: HashSet<SocketAddr>,
        lookup_ids: Arc<StdMutex<Vec<LookupId>>>,
    ) -> Option<DemandParkedSliceOutcome> {
        drain_lookup_ids(
            active_runtime,
            &mut self.parked_crawls,
            &mut self.draining_demands,
            command_tx,
            info_hash,
            slice_class,
            stop_reason,
            total_peers,
            unique_peers,
            lookup_ids,
        )
    }

    fn drain_admission_snapshot(
        &self,
        info_hash: InfoHash,
    ) -> Option<DemandDrainAdmissionSnapshot> {
        self.draining_demands
            .get(&info_hash)
            .map(demand_drain_admission_snapshot)
    }

    fn finalize_drained_lookup(
        &mut self,
        active_runtime: Option<&mut ActiveRuntime>,
        command_tx: &DhtCommandSender,
        info_hash: InfoHash,
        force: bool,
    ) -> Option<DrainedDemandOutcome> {
        finalize_drained_demand_lookup(
            active_runtime,
            &mut self.parked_crawls,
            &mut self.draining_demands,
            command_tx,
            info_hash,
            force,
        )
    }
}

fn drained_demand_lookup_ready_for_finalize(
    runtime_ready: bool,
    drain: &DrainingDemandLookup,
    now: Instant,
) -> (bool, bool) {
    let early_no_yield = !runtime_ready
        && now >= drain.no_late_yield_deadline
        && drain.late_unique_peer_count() == 0;
    let ready_to_finalize = runtime_ready || early_no_yield || now >= drain.deadline;
    (ready_to_finalize, early_no_yield)
}

fn finalize_drained_demand_lookup(
    active_runtime: Option<&mut ActiveRuntime>,
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    draining_demands: &mut HashMap<InfoHash, DrainingDemandLookup>,
    command_tx: &DhtCommandSender,
    info_hash: InfoHash,
    force: bool,
) -> Option<DrainedDemandOutcome> {
    let drain = draining_demands.get(&info_hash).cloned()?;
    let now = Instant::now();
    let runtime_ready = drained_demand_lookup_runtime_ready(
        active_runtime.as_ref().map(|active| &**active),
        &drain,
    );
    let (ready_to_finalize, early_no_yield) =
        drained_demand_lookup_ready_for_finalize(runtime_ready, &drain, now);
    if !force && !ready_to_finalize {
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

fn apply_demand_planner_effects(
    mut active_runtime: Option<&mut ActiveRuntime>,
    demand_planner: &mut DemandPlannerModel,
    command_tx: &DhtCommandSender,
    slice_metrics: &mut DemandSliceMetrics,
    effects: Vec<DemandPlannerEffect>,
) -> bool {
    let mut finalized_any = false;
    let mut pending_effects = VecDeque::from(effects);

    while let Some(effect) = pending_effects.pop_front() {
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

fn finish_drained_demand_lookup(
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

fn candidate_due_age(candidate: DueDemandCandidate, now: Instant) -> Duration {
    now.saturating_duration_since(candidate.next_eligible_at)
}

fn candidate_has_fairness_age(candidate: DueDemandCandidate, now: Instant) -> bool {
    candidate_due_age(candidate, now) >= DHT_DEMAND_FAIRNESS_AGE
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

fn demand_planner_selection_stats(
    offered_candidates: &[DueDemandCandidate],
    launched_candidates: &[DueDemandCandidate],
    now: Instant,
) -> DemandPlannerSelectionStats {
    let launched_hashes = launched_candidates
        .iter()
        .map(|candidate| candidate.info_hash)
        .collect::<HashSet<_>>();
    let mut stats = DemandPlannerSelectionStats::default();

    for candidate in offered_candidates {
        let class = DemandSliceClass::from_demand(candidate.demand);
        stats.offered.record(class);
        if launched_hashes.contains(&candidate.info_hash) {
            stats.launched.record(class);
        } else {
            stats.throttled.record(class);
            stats.record_throttled_age(
                class,
                duration_ms(now.saturating_duration_since(candidate.next_eligible_at)),
            );
        }
    }

    stats
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
    select_due_demand_launches_with_stats(
        due_candidates,
        active_counts,
        parked_crawls,
        planner_state,
        planner_budget,
        now,
        total_budget,
    )
    .launches
}

fn select_due_demand_launches_with_stats(
    due_candidates: &[DueDemandCandidate],
    active_counts: DemandSlotCounts,
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    planner_budget: &mut DemandPlannerBudget,
    now: Instant,
    total_budget: usize,
) -> DemandPlannerSelection {
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
        let left_fairness = candidate_has_fairness_age(*left, now);
        let right_fairness = candidate_has_fairness_age(*right, now);

        demand_slice_class_priority(right_class)
            .cmp(&demand_slice_class_priority(left_class))
            .then_with(|| right_fairness.cmp(&left_fairness))
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

    DemandPlannerSelection {
        stats: demand_planner_selection_stats(due_candidates, &selected, now),
        launches: selected,
    }
}

impl DemandPlannerModel {
    fn update(&mut self, action: DemandPlannerAction<'_>) -> DemandPlannerReduction {
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
                DemandPlannerReduction::default()
            }
            DemandPlannerAction::DemandUpdated {
                info_hash,
                demand,
                now,
            } => {
                demand_scheduler.update(info_hash, demand, now);
                let effects = if draining_demands
                    .get(&info_hash)
                    .is_some_and(|drain| drain.slice_class != DemandSliceClass::from_demand(demand))
                {
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
                        ready_to_finalize.then_some(DemandPlannerEffect::FinalizeDrainingLookup(
                            DemandFinalizeDrainingLookupEffect {
                                info_hash,
                                force: false,
                            },
                        ))
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
                    return DemandPlannerReduction::default();
                }

                evict_stale_parked_crawls(parked_crawls, now);
                let drain_virtual_slots = drain_virtual_slot_count(draining_demands.len());
                let launch_budget =
                    demand_lookup_launch_budget(demand_lookup_ids, draining_demands.len());
                if launch_budget == 0 {
                    return DemandPlannerReduction::default();
                }

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
                    .filter(|(_, reason)| *reason == DemandSelectionReason::SpareCapacity)
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
                    effects.push(DemandPlannerEffect::StartLookup(DemandStartLookupEffect {
                        candidate,
                        plan,
                        selection_reason,
                    }));
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
                    && outcome.parked_outcome == Some(DemandParkedSliceOutcome::HealthyZeroYield)
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
    }
}

async fn start_due_demands(
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
    demand_planner: &mut DemandPlannerModel,
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
    let resumed_state = demand_planner.take_parked_family_state(
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
    use proptest::prelude::*;

    fn peer(addr: &str) -> SocketAddr {
        addr.parse().expect("valid socket address")
    }

    fn hash_index(index: u32) -> InfoHash {
        let mut bytes = [0u8; InfoHash::LEN];
        bytes[..4].copy_from_slice(&index.to_be_bytes());
        InfoHash::from(bytes)
    }

    fn demand_for_fuzz_class(class: u8, connected_peers: u8) -> DhtDemandState {
        match class % 3 {
            0 => DhtDemandState {
                awaiting_metadata: true,
                connected_peers: usize::from(connected_peers),
            },
            1 => DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            _ => DhtDemandState {
                awaiting_metadata: false,
                connected_peers: usize::from(connected_peers.max(1)),
            },
        }
    }

    fn count_candidate_classes(candidates: &[DueDemandCandidate]) -> DemandSlotCounts {
        let mut counts = DemandSlotCounts::default();
        for candidate in candidates {
            counts.record(DemandSliceClass::from_demand(candidate.demand));
        }
        counts
    }

    fn test_instant_saturating_sub(now: Instant, duration: Duration) -> Instant {
        now.checked_sub(duration).unwrap_or(now)
    }

    #[derive(Debug, Clone)]
    struct PlannerCandidateSpec {
        index: u16,
        demand_class: u8,
        connected_peers: u8,
        overdue_ms: u32,
        subscribers: u8,
        useful_yield_age_ms: Option<u32>,
        last_unique_peers: u8,
    }

    fn planner_candidate_strategy() -> impl Strategy<Value = PlannerCandidateSpec> {
        (
            0u16..512,
            0u8..3,
            0u8..32,
            0u32..=1_200_000,
            1u8..=8,
            prop::option::of(0u32..=1_200_000),
            0u8..=96,
        )
            .prop_map(
                |(
                    index,
                    demand_class,
                    connected_peers,
                    overdue_ms,
                    subscribers,
                    useful_yield_age_ms,
                    last_unique_peers,
                )| PlannerCandidateSpec {
                    index,
                    demand_class,
                    connected_peers,
                    overdue_ms,
                    subscribers,
                    useful_yield_age_ms,
                    last_unique_peers,
                },
            )
    }

    #[derive(Debug, Clone)]
    enum PlannerMachineOp {
        Register {
            key: u8,
            demand: DhtDemandState,
            advance_ms: u16,
        },
        Update {
            key: u8,
            demand: DhtDemandState,
            advance_ms: u16,
        },
        Unregister {
            key: u8,
            advance_ms: u16,
        },
        PlanTick {
            runtime_available: bool,
            fail_mask: u8,
            advance_ms: u16,
        },
        FinishActive {
            key: u8,
            unique_peers: u8,
            advance_ms: u16,
        },
        ParkActive {
            key: u8,
            unique_peers: u8,
            stop_reason: u8,
            advance_ms: u16,
        },
        AddDrainPeers {
            key: u8,
            peer_count: u8,
            advance_ms: u16,
        },
        FinalizeDrain {
            key: u8,
            advance_ms: u16,
        },
        DrainTick {
            runtime_ready: bool,
            advance_ms: u16,
        },
        RuntimeReset {
            advance_ms: u16,
        },
        ResetActive {
            advance_ms: u16,
        },
    }

    fn planner_machine_op_strategy() -> impl Strategy<Value = PlannerMachineOp> {
        let key = 0u8..64;
        let advance_ms = 0u16..=5_000;

        prop_oneof![
            (key.clone(), demand_strategy(), advance_ms.clone()).prop_map(
                |(key, demand, advance_ms)| PlannerMachineOp::Register {
                    key,
                    demand,
                    advance_ms,
                }
            ),
            (key.clone(), demand_strategy(), advance_ms.clone()).prop_map(
                |(key, demand, advance_ms)| PlannerMachineOp::Update {
                    key,
                    demand,
                    advance_ms,
                }
            ),
            (key.clone(), advance_ms.clone())
                .prop_map(|(key, advance_ms)| { PlannerMachineOp::Unregister { key, advance_ms } }),
            (any::<bool>(), any::<u8>(), advance_ms.clone()).prop_map(
                |(runtime_available, fail_mask, advance_ms)| PlannerMachineOp::PlanTick {
                    runtime_available,
                    fail_mask,
                    advance_ms,
                },
            ),
            (key.clone(), 0u8..=96, advance_ms.clone()).prop_map(
                |(key, unique_peers, advance_ms)| PlannerMachineOp::FinishActive {
                    key,
                    unique_peers,
                    advance_ms,
                }
            ),
            (key.clone(), 0u8..=96, any::<u8>(), advance_ms.clone()).prop_map(
                |(key, unique_peers, stop_reason, advance_ms)| PlannerMachineOp::ParkActive {
                    key,
                    unique_peers,
                    stop_reason,
                    advance_ms,
                }
            ),
            (key.clone(), 0u8..=32, advance_ms.clone()).prop_map(
                |(key, peer_count, advance_ms)| PlannerMachineOp::AddDrainPeers {
                    key,
                    peer_count,
                    advance_ms,
                }
            ),
            (key, advance_ms.clone()).prop_map(|(key, advance_ms)| {
                PlannerMachineOp::FinalizeDrain { key, advance_ms }
            }),
            (any::<bool>(), advance_ms.clone()).prop_map(|(runtime_ready, advance_ms)| {
                PlannerMachineOp::DrainTick {
                    runtime_ready,
                    advance_ms,
                }
            }),
            advance_ms
                .clone()
                .prop_map(|advance_ms| PlannerMachineOp::RuntimeReset { advance_ms }),
            advance_ms.prop_map(|advance_ms| PlannerMachineOp::ResetActive { advance_ms }),
        ]
    }

    fn demand_strategy() -> impl Strategy<Value = DhtDemandState> {
        (any::<bool>(), 0usize..=32).prop_map(|(awaiting_metadata, connected_peers)| {
            DhtDemandState {
                awaiting_metadata,
                connected_peers,
            }
        })
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

    fn prop_stop_reason(code: u8) -> DemandSliceStopReason {
        match code % 4 {
            0 => DemandSliceStopReason::WallTime,
            1 => DemandSliceStopReason::IdleTimeout,
            2 => DemandSliceStopReason::FirstBatch,
            _ => DemandSliceStopReason::UniquePeerCap,
        }
    }

    struct PlannerMachine {
        now: Instant,
        planner: DemandPlannerModel,
        next_lookup_id: u64,
    }

    impl PlannerMachine {
        fn new() -> Self {
            let now = Instant::now();
            Self {
                now,
                planner: DemandPlannerModel::new(now),
                next_lookup_id: 1,
            }
        }

        fn advance(&mut self, advance_ms: u16) {
            self.now += Duration::from_millis(u64::from(advance_ms));
        }

        fn plan_tick(&mut self, runtime_available: bool, fail_mask: u8) {
            let reduction = self.planner.update(DemandPlannerAction::PlanDue {
                now: self.now,
                runtime_available,
            });

            let mut launch_index = 0u8;
            for effect in reduction.effects {
                let DemandPlannerEffect::StartLookup(start) = effect else {
                    continue;
                };
                let fail_start = (fail_mask & (1 << (launch_index % 8))) != 0;
                launch_index = launch_index.wrapping_add(1);
                if fail_start {
                    self.planner.update(DemandPlannerAction::LookupStartFailed {
                        info_hash: start.candidate.info_hash,
                        slice_class: start.plan.class,
                        now: self.now,
                    });
                    continue;
                }
                let lookup_id = LookupId(self.next_lookup_id);
                self.next_lookup_id = self.next_lookup_id.saturating_add(1);
                self.planner.update(DemandPlannerAction::LookupStarted {
                    info_hash: start.candidate.info_hash,
                    slice_class: start.plan.class,
                    lookup_ids: active_lookup(lookup_id, start.plan.class).lookup_ids,
                });
            }
        }

        fn finish_active(&mut self, key: u8, unique_peers: u8) {
            let info_hash = hash_index(u32::from(key));
            let Some(active) = self.planner.active.get(&info_hash) else {
                return;
            };
            let slice_class = active.slice_class;
            self.planner.update(DemandPlannerAction::LookupFinished {
                info_hash,
                slice_class,
                total_peers: usize::from(unique_peers),
                unique_peers: usize::from(unique_peers),
                now: self.now,
            });
        }

        fn park_active(&mut self, key: u8, unique_peers: u8, stop_reason: u8) {
            let info_hash = hash_index(u32::from(key));
            let Some(active) = self.planner.active.get(&info_hash).cloned() else {
                return;
            };
            let stop_reason = prop_stop_reason(stop_reason);
            let requested = self
                .planner
                .update(DemandPlannerAction::LookupParkRequested {
                    info_hash,
                    slice_class: active.slice_class,
                    stop_reason,
                    total_peers: usize::from(unique_peers),
                    unique_peers: synthetic_peers(key, unique_peers),
                    lookup_ids: active.lookup_ids,
                });
            for effect in requested.effects {
                let DemandPlannerEffect::AdmitDrain(admit) = effect else {
                    continue;
                };
                let unique_peer_count = admit.unique_peers.len();
                let admit_drain =
                    unique_peer_count > 0 || admit.slice_class != DemandSliceClass::RoutineRefresh;
                let parked_outcome = if admit_drain {
                    let lookup_id = admit
                        .lookup_ids
                        .lock()
                        .expect("test lookup id lock")
                        .first()
                        .copied()
                        .unwrap_or(LookupId(0));
                    insert_synthetic_drain_with_stop_reason(
                        &mut self.planner.draining_demands,
                        admit.info_hash,
                        key,
                        lookup_id,
                        admit.slice_class,
                        admit.stop_reason,
                        unique_peers,
                        self.now,
                    );
                    Some(admit.slice_class.parked_slice_outcome(
                        admit.stop_reason,
                        unique_peer_count,
                        false,
                    ))
                } else {
                    None
                };
                let drain_admission = self
                    .planner
                    .draining_demands
                    .get(&admit.info_hash)
                    .map(demand_drain_admission_snapshot);
                self.planner
                    .update(DemandPlannerAction::LookupParkResolved {
                        info_hash: admit.info_hash,
                        slice_class: admit.slice_class,
                        stop_reason: admit.stop_reason,
                        total_peers: admit.total_peers,
                        unique_peers: unique_peer_count,
                        parked_outcome,
                        drain_admission,
                        previous: admit.previous,
                        now: self.now,
                    });
            }
        }

        fn finalize_drain(&mut self, key: u8) {
            self.finalize_drain_hash(hash_index(u32::from(key)));
        }

        fn finalize_drain_hash(&mut self, info_hash: InfoHash) {
            let Some(drain) = self.planner.draining_demands.remove(&info_hash) else {
                return;
            };
            let unique_peers = drain.unique_peer_count();
            let previous = self.planner.scheduler.entry_snapshot(info_hash);
            let parked_outcome =
                drain
                    .slice_class
                    .parked_slice_outcome(drain.stop_reason, unique_peers, false);
            self.planner
                .update(DemandPlannerAction::DrainedLookupFinalized {
                    info_hash,
                    outcome: DrainedDemandOutcome {
                        slice_class: drain.slice_class,
                        stop_reason: drain.stop_reason,
                        total_peers: drain.total_peers,
                        unique_peers,
                        parked_outcome: Some(parked_outcome),
                        drain_duration_ms: drain.duration_ms(self.now),
                        finalized_after_deadline: self.now >= drain.deadline,
                        finalized_early_no_yield: false,
                    },
                    previous,
                    now: self.now,
                });
        }

        fn apply(&mut self, op: PlannerMachineOp) {
            let advance_ms = match &op {
                PlannerMachineOp::Register { advance_ms, .. }
                | PlannerMachineOp::Update { advance_ms, .. }
                | PlannerMachineOp::Unregister { advance_ms, .. }
                | PlannerMachineOp::PlanTick { advance_ms, .. }
                | PlannerMachineOp::FinishActive { advance_ms, .. }
                | PlannerMachineOp::ParkActive { advance_ms, .. }
                | PlannerMachineOp::AddDrainPeers { advance_ms, .. }
                | PlannerMachineOp::FinalizeDrain { advance_ms, .. }
                | PlannerMachineOp::DrainTick { advance_ms, .. }
                | PlannerMachineOp::RuntimeReset { advance_ms }
                | PlannerMachineOp::ResetActive { advance_ms } => *advance_ms,
            };
            self.advance(advance_ms);

            match op {
                PlannerMachineOp::Register { key, demand, .. } => {
                    self.planner.update(DemandPlannerAction::DemandRegistered {
                        info_hash: hash_index(u32::from(key)),
                        demand,
                        now: self.now,
                    });
                }
                PlannerMachineOp::Update { key, demand, .. } => {
                    let info_hash = hash_index(u32::from(key));
                    let reduction = self.planner.update(DemandPlannerAction::DemandUpdated {
                        info_hash,
                        demand,
                        now: self.now,
                    });
                    for effect in reduction.effects {
                        if let DemandPlannerEffect::FinalizeDrainingLookup(_) = effect {
                            self.finalize_drain(key);
                        }
                    }
                }
                PlannerMachineOp::Unregister { key, .. } => {
                    let info_hash = hash_index(u32::from(key));
                    self.planner
                        .update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
                }
                PlannerMachineOp::PlanTick {
                    runtime_available,
                    fail_mask,
                    ..
                } => self.plan_tick(runtime_available, fail_mask),
                PlannerMachineOp::FinishActive {
                    key, unique_peers, ..
                } => self.finish_active(key, unique_peers),
                PlannerMachineOp::ParkActive {
                    key,
                    unique_peers,
                    stop_reason,
                    ..
                } => self.park_active(key, unique_peers, stop_reason),
                PlannerMachineOp::AddDrainPeers {
                    key, peer_count, ..
                } => {
                    let peers = synthetic_peers(key.wrapping_add(1), peer_count)
                        .into_iter()
                        .collect::<Vec<_>>();
                    self.planner.update(DemandPlannerAction::PeersReceived {
                        info_hash: hash_index(u32::from(key)),
                        peers: &peers,
                    });
                }
                PlannerMachineOp::FinalizeDrain { key, .. } => self.finalize_drain(key),
                PlannerMachineOp::DrainTick { runtime_ready, .. } => {
                    let runtime_ready = self
                        .planner
                        .draining_demands
                        .keys()
                        .copied()
                        .map(|info_hash| (info_hash, runtime_ready))
                        .collect();
                    let reduction = self.planner.update(DemandPlannerAction::DrainTick {
                        now: self.now,
                        runtime_ready,
                    });
                    for effect in reduction.effects {
                        if let DemandPlannerEffect::FinalizeDrainingLookup(finalize) = effect {
                            self.finalize_drain_hash(finalize.info_hash);
                        }
                    }
                }
                PlannerMachineOp::RuntimeReset { .. } => {
                    self.planner
                        .update(DemandPlannerAction::RuntimeReset { now: self.now });
                }
                PlannerMachineOp::ResetActive { .. } => {
                    self.planner.active.clear();
                    self.planner.draining_demands.clear();
                    self.planner.scheduler.reset_active(self.now);
                }
            }
        }

        fn assert_invariants(&self) -> Result<(), TestCaseError> {
            let mut occupied = HashSet::new();
            let mut lookup_ids = HashSet::new();
            for (&info_hash, active) in &self.planner.active {
                prop_assert!(occupied.insert(info_hash));
                let snapshot = self
                    .planner
                    .scheduler
                    .entry_snapshot(info_hash)
                    .expect("active demand must have scheduler entry");
                prop_assert!(snapshot.in_progress);
                let active_ids = active.lookup_ids.lock().expect("test lookup id lock");
                prop_assert_eq!(active_ids.len(), 1);
                for lookup_id in active_ids.iter().copied() {
                    prop_assert!(lookup_ids.insert(lookup_id));
                }
            }

            for (&info_hash, drain) in &self.planner.draining_demands {
                prop_assert!(occupied.insert(info_hash));
                let snapshot = self
                    .planner
                    .scheduler
                    .entry_snapshot(info_hash)
                    .expect("draining demand must have scheduler entry");
                prop_assert!(snapshot.in_progress);
                prop_assert!(!drain.lookup_ids.is_empty());
                for lookup_id in drain.lookup_ids.iter().copied() {
                    prop_assert!(lookup_ids.insert(lookup_id));
                }
                prop_assert!(drain.deadline >= drain.started_at);
                prop_assert!(drain.no_late_yield_deadline <= drain.deadline);
                prop_assert!(drain.unique_peer_count() >= drain.initial_unique_peers);
                prop_assert!(drain.late_unique_peer_count() <= drain.unique_peer_count());
                prop_assert!(drain.total_peers >= drain.unique_peer_count());
                prop_assert!(drain.initial_inflight_queries > 0);
            }

            let scheduler_snapshots = self.planner.scheduler.entry_snapshots();
            for snapshot in &scheduler_snapshots {
                prop_assert!(snapshot.subscriber_count > 0);
                if snapshot.in_progress {
                    prop_assert!(
                        self.planner.active.contains_key(&snapshot.info_hash)
                            || self
                                .planner
                                .draining_demands
                                .contains_key(&snapshot.info_hash)
                    );
                }
            }
            let expected_metadata_waiters = scheduler_snapshots
                .iter()
                .filter(|snapshot| snapshot.demand.awaiting_metadata)
                .count();
            prop_assert_eq!(
                self.planner.metadata_waiter_count(),
                expected_metadata_waiters
            );

            let active_counts = active_demand_lookup_slot_counts(&self.planner.active);
            prop_assert!(active_counts.awaiting_metadata <= DHT_AWAITING_METADATA_SLOT_CAP);
            prop_assert!(active_counts.no_connected_peers <= DHT_NO_CONNECTED_PEERS_SLOT_CAP);
            prop_assert!(active_counts.routine_refresh <= DHT_ROUTINE_LOOKUP_SLOT_CAP);
            prop_assert!(
                self.planner
                    .active
                    .len()
                    .saturating_add(drain_virtual_slot_count(
                        self.planner.draining_demands.len()
                    ))
                    <= DHT_DEMAND_LOOKUP_SLOT_COUNT
            );

            for candidate in self.planner.scheduler.due_candidates(self.now) {
                prop_assert!(!self.planner.active.contains_key(&candidate.info_hash));
                prop_assert!(!self
                    .planner
                    .draining_demands
                    .contains_key(&candidate.info_hash));
            }

            Ok(())
        }
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
    fn drain_finalize_readiness_bounds_waiting_drains() {
        let start = Instant::now();
        let initial_peer = peer("127.0.0.1:4000");
        let late_peer = peer("127.0.0.2:4000");
        let mut unique_peers = HashSet::new();
        unique_peers.insert(initial_peer);
        let mut drain = DrainingDemandLookup {
            lookup_ids: vec![LookupId(1)],
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            started_at: start,
            total_peers: 1,
            initial_unique_peers: 1,
            unique_peers,
            deadline: start + Duration::from_secs(2),
            no_late_yield_deadline: start + Duration::from_secs(1),
            initial_inflight_queries: 8,
            score: 100,
        };

        assert_eq!(
            drained_demand_lookup_ready_for_finalize(
                false,
                &drain,
                start + Duration::from_millis(999),
            ),
            (false, false)
        );
        assert_eq!(
            drained_demand_lookup_ready_for_finalize(false, &drain, start + Duration::from_secs(1)),
            (true, true)
        );

        drain.record_peers(&[late_peer]);
        assert_eq!(
            drained_demand_lookup_ready_for_finalize(
                false,
                &drain,
                start + Duration::from_millis(1500),
            ),
            (false, false)
        );
        assert_eq!(
            drained_demand_lookup_ready_for_finalize(false, &drain, start + Duration::from_secs(2)),
            (true, false)
        );
        assert_eq!(
            drained_demand_lookup_ready_for_finalize(true, &drain, start),
            (true, false)
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
    fn demand_planner_plan_due_starts_due_demands_by_class_and_marks_state() {
        let now = Instant::now();
        let metadata_hash = hash_index(60);
        let no_peer_hash = hash_index(61);
        let routine_hash = hash_index(62);
        let mut planner = DemandPlannerModel::new(now);

        for (info_hash, demand) in [
            (
                metadata_hash,
                DhtDemandState {
                    awaiting_metadata: true,
                    connected_peers: 0,
                },
            ),
            (
                no_peer_hash,
                DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
            ),
            (
                routine_hash,
                DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 2,
                },
            ),
        ] {
            planner.update(DemandPlannerAction::DemandRegistered {
                info_hash,
                demand,
                now,
            });
        }

        let reduction = planner.update(DemandPlannerAction::PlanDue {
            now,
            runtime_available: true,
        });
        let starts = reduction
            .effects
            .iter()
            .filter_map(|effect| match effect {
                DemandPlannerEffect::StartLookup(start) => Some(*start),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(starts.len(), 3);
        assert!(starts.iter().any(|start| {
            start.candidate.info_hash == metadata_hash
                && start.plan.class == DemandSliceClass::AwaitingMetadata
        }));
        assert!(starts.iter().any(|start| {
            start.candidate.info_hash == no_peer_hash
                && start.plan.class == DemandSliceClass::NoConnectedPeers
        }));
        assert!(starts.iter().any(|start| {
            start.candidate.info_hash == routine_hash
                && start.plan.class == DemandSliceClass::RoutineRefresh
        }));

        for start in starts {
            assert_eq!(start.selection_reason, DemandSelectionReason::OverdueScarce);
            assert!(
                planner
                    .scheduler
                    .entry_snapshot(start.candidate.info_hash)
                    .expect("demand entry")
                    .in_progress
            );
            assert_eq!(
                planner
                    .state
                    .get(&start.candidate.info_hash)
                    .expect("planner state")
                    .last_started_at,
                Some(now)
            );
        }
    }

    #[test]
    fn demand_planner_plan_due_skips_draining_demands_but_launches_independent_work() {
        let now = Instant::now();
        let draining_hash = hash_index(63);
        let metadata_hash = hash_index(64);
        let mut planner = DemandPlannerModel::new(now);

        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash: draining_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        });
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash: metadata_hash,
            demand: DhtDemandState {
                awaiting_metadata: true,
                connected_peers: 0,
            },
            now,
        });
        assert!(planner.scheduler.mark_in_progress(draining_hash));
        insert_synthetic_drain(
            &mut planner.draining_demands,
            draining_hash,
            63,
            LookupId(63),
            DemandSliceClass::NoConnectedPeers,
            1,
            now,
        );

        let reduction = planner.update(DemandPlannerAction::PlanDue {
            now,
            runtime_available: true,
        });
        let starts = reduction
            .effects
            .iter()
            .filter_map(|effect| match effect {
                DemandPlannerEffect::StartLookup(start) => Some(*start),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].candidate.info_hash, metadata_hash);
        assert_eq!(starts[0].plan.class, DemandSliceClass::AwaitingMetadata);
        assert!(planner.draining_demands.contains_key(&draining_hash));
        assert!(
            planner
                .scheduler
                .entry_snapshot(draining_hash)
                .expect("draining demand entry")
                .in_progress
        );
    }

    #[test]
    fn demand_planner_drained_lookup_lifecycle_keeps_late_peer_yield_in_state() {
        let now = Instant::now();
        let info_hash = hash_index(65);
        let mut planner = DemandPlannerModel::new(now);
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        });
        assert!(planner.scheduler.mark_in_progress(info_hash));
        let active = active_lookup(LookupId(65), DemandSliceClass::NoConnectedPeers);
        planner.active.insert(info_hash, active.clone());

        let requested = planner.update(DemandPlannerAction::LookupParkRequested {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 1,
            unique_peers: synthetic_peers(65, 1),
            lookup_ids: active.lookup_ids,
        });
        assert!(planner.active.is_empty());
        let DemandPlannerEffect::AdmitDrain(admit) =
            requested.effects.into_iter().next().expect("admit effect")
        else {
            panic!("expected admit drain effect");
        };

        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            65,
            LookupId(65),
            DemandSliceClass::NoConnectedPeers,
            1,
            now,
        );
        let drain_admission = planner
            .draining_demands
            .get(&info_hash)
            .map(demand_drain_admission_snapshot);
        planner.update(DemandPlannerAction::LookupParkResolved {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 1,
            unique_peers: 1,
            parked_outcome: Some(DemandParkedSliceOutcome::HealthyLowYield),
            drain_admission,
            previous: admit.previous,
            now,
        });
        assert!(
            planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );

        let late_peers = synthetic_peers(66, 3).into_iter().collect::<Vec<_>>();
        let recorded = planner.update(DemandPlannerAction::PeersReceived {
            info_hash,
            peers: &late_peers,
        });
        let DemandPlannerEffect::DrainPeersRecorded(recorded) = recorded
            .effects
            .into_iter()
            .next()
            .expect("recorded effect")
        else {
            panic!("expected drain peers recorded effect");
        };
        assert_eq!(recorded.peer_count, 3);
        assert_eq!(recorded.unique_added, 3);
        assert_eq!(
            planner
                .draining_demands
                .get(&info_hash)
                .expect("draining demand")
                .unique_peer_count(),
            4
        );

        let finalized_at = now + Duration::from_secs(2);
        let drain = planner
            .draining_demands
            .remove(&info_hash)
            .expect("draining demand");
        let previous = planner.scheduler.entry_snapshot(info_hash);
        let finalized = planner.update(DemandPlannerAction::DrainedLookupFinalized {
            info_hash,
            outcome: DrainedDemandOutcome {
                slice_class: drain.slice_class,
                stop_reason: drain.stop_reason,
                total_peers: drain.total_peers,
                unique_peers: drain.unique_peer_count(),
                parked_outcome: Some(DemandParkedSliceOutcome::UsefulYield),
                drain_duration_ms: drain.duration_ms(finalized_at),
                finalized_after_deadline: finalized_at >= drain.deadline,
                finalized_early_no_yield: false,
            },
            previous,
            now: finalized_at,
        });

        let state = planner.state.get(&info_hash).expect("planner state");
        assert_eq!(state.last_finished_at, Some(finalized_at));
        assert_eq!(state.last_useful_yield_at, Some(finalized_at));
        assert_eq!(state.last_unique_peers, 4);
        assert!(
            !planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );
        let DemandPlannerEffect::DrainFinalized(finalized) = finalized
            .effects
            .into_iter()
            .next()
            .expect("finalized effect")
        else {
            panic!("expected drain finalized effect");
        };
        assert_eq!(finalized.finish_mode, DemandFinishMode::Standard);
        assert_eq!(finalized.outcome.unique_peers, 4);
    }

    #[test]
    fn demand_planner_uses_spare_capacity_for_backed_off_no_peer_state() {
        let now = Instant::now();
        let info_hash = hash_index(67);
        let mut planner = DemandPlannerModel::new(now);
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        });
        assert!(planner.scheduler.mark_in_progress(info_hash));
        let previous = planner.scheduler.entry_snapshot(info_hash);

        planner.update(DemandPlannerAction::DrainedLookupFinalized {
            info_hash,
            outcome: DrainedDemandOutcome {
                slice_class: DemandSliceClass::NoConnectedPeers,
                stop_reason: DemandSliceStopReason::IdleTimeout,
                total_peers: 0,
                unique_peers: 0,
                parked_outcome: Some(DemandParkedSliceOutcome::HealthyZeroYield),
                drain_duration_ms: 1_000,
                finalized_after_deadline: false,
                finalized_early_no_yield: true,
            },
            previous,
            now,
        });
        let backed_off = planner
            .scheduler
            .entry_snapshot(info_hash)
            .expect("demand entry");
        assert!(backed_off.no_connected_peers_backoff_step > 0);

        let spare_at = now + DHT_DEMAND_SPARE_RESEARCH_MIN_INTERVAL;
        assert!(backed_off.next_eligible_at > spare_at);
        let reduction = planner.update(DemandPlannerAction::PlanDue {
            now: spare_at,
            runtime_available: true,
        });
        let starts = reduction
            .effects
            .iter()
            .filter_map(|effect| match effect {
                DemandPlannerEffect::StartLookup(start) => Some(*start),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].candidate.info_hash, info_hash);
        assert_eq!(starts[0].plan.class, DemandSliceClass::NoConnectedPeers);
        assert_eq!(
            starts[0].selection_reason,
            DemandSelectionReason::SpareCapacity
        );
        assert!(
            planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );
    }

    #[test]
    fn parked_family_state_round_trips_each_family_and_clears_entry() {
        let now = Instant::now();
        let info_hash = hash_index(68);
        let mut parked_crawls = HashMap::new();

        let outcome = store_parked_lookup_states(
            &mut parked_crawls,
            info_hash,
            DemandSliceClass::NoConnectedPeers,
            Some(DemandSliceStopReason::WallTime),
            2,
            vec![
                lookup_state_for_family(LookupId(68), AddressFamily::Ipv4, 68, now),
                lookup_state_for_family(LookupId(69), AddressFamily::Ipv6, 68, now),
            ],
        );

        assert_eq!(outcome, Some(DemandParkedSliceOutcome::HealthyLowYield));
        assert!(parked_crawls.contains_key(&info_hash));

        let ipv4 = take_parked_family_state(
            &mut parked_crawls,
            None,
            info_hash,
            AddressFamily::Ipv4,
            DemandSliceClass::NoConnectedPeers,
        )
        .expect("parked ipv4 state");
        assert_eq!(ipv4.family(), AddressFamily::Ipv4);
        assert!(parked_crawls.contains_key(&info_hash));

        let ipv6 = take_parked_family_state(
            &mut parked_crawls,
            None,
            info_hash,
            AddressFamily::Ipv6,
            DemandSliceClass::NoConnectedPeers,
        )
        .expect("parked ipv6 state");
        assert_eq!(ipv6.family(), AddressFamily::Ipv6);
        assert!(!parked_crawls.contains_key(&info_hash));
    }

    #[test]
    fn parked_family_state_reset_drops_low_quality_crawl_and_records_reason() {
        let now = Instant::now();
        let info_hash = hash_index(69);
        let mut parked_crawls = HashMap::new();
        let mut metrics = DemandSliceMetrics::default();

        let mut crawl = DemandCrawlState::new(now, DemandSliceClass::RoutineRefresh);
        crawl.ipv4 = Some(lookup_state_for_family(
            LookupId(70),
            AddressFamily::Ipv4,
            69,
            now,
        ));
        crawl.consecutive_stalled_low_yield_slices =
            DHT_ROUTINE_STALLED_EMPTY_SLICE_RESET_THRESHOLD;
        parked_crawls.insert(info_hash, crawl);

        let reset = take_parked_family_state(
            &mut parked_crawls,
            Some(&mut metrics),
            info_hash,
            AddressFamily::Ipv4,
            DemandSliceClass::RoutineRefresh,
        );

        assert!(reset.is_none());
        assert!(!parked_crawls.contains_key(&info_hash));
        assert_eq!(metrics.routine_refresh.low_quality_resets, 1);
    }

    #[test]
    fn demand_planner_lookup_start_failed_releases_scheduler_entry_and_refunds_slot() {
        let now = Instant::now();
        let info_hash = hash_index(70);
        let mut planner = DemandPlannerModel::new(now);
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        });

        let planned = planner.update(DemandPlannerAction::PlanDue {
            now,
            runtime_available: true,
        });
        assert!(planned.effects.iter().any(|effect| matches!(
            effect,
            DemandPlannerEffect::StartLookup(start)
                if start.candidate.info_hash == info_hash
        )));
        assert!(
            planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );

        planner.update(DemandPlannerAction::LookupStartFailed {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            now,
        });
        let snapshot = planner
            .scheduler
            .entry_snapshot(info_hash)
            .expect("demand entry");
        assert!(!snapshot.in_progress);
        assert!(snapshot.next_eligible_at > now);

        let later = now + DHT_NO_CONNECTED_PEERS_BASE_INTERVAL;
        let retry = planner.update(DemandPlannerAction::PlanDue {
            now: later,
            runtime_available: true,
        });
        assert!(retry.effects.iter().any(|effect| matches!(
            effect,
            DemandPlannerEffect::StartLookup(start)
                if start.candidate.info_hash == info_hash
        )));
    }

    #[test]
    fn demand_planner_duplicate_subscribers_keep_lookup_until_final_unsubscribe() {
        let now = Instant::now();
        let info_hash = hash_index(71);
        let mut planner = DemandPlannerModel::new(now);
        let demand = DhtDemandState {
            awaiting_metadata: false,
            connected_peers: 0,
        };
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand,
            now,
        });
        planner.update(DemandPlannerAction::DemandRegistered {
            info_hash,
            demand,
            now,
        });
        assert_eq!(
            planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .subscriber_count,
            2
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));
        planner.active.insert(
            info_hash,
            active_lookup(LookupId(71), DemandSliceClass::NoConnectedPeers),
        );

        let first = planner.update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
        assert!(first.effects.is_empty());
        assert!(planner.active.contains_key(&info_hash));
        assert_eq!(
            planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .subscriber_count,
            1
        );

        let final_removal =
            planner.update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
        assert!(planner.scheduler.entry_snapshot(info_hash).is_none());
        assert!(!planner.active.contains_key(&info_hash));
        assert!(final_removal.effects.iter().any(|effect| matches!(
            effect,
            DemandPlannerEffect::ParkActiveLookup(park)
                if park.info_hash == info_hash
                    && park.slice_class == DemandSliceClass::NoConnectedPeers
        )));
    }

    #[test]
    fn demand_planner_drain_runtime_readiness_defaults_ready_without_runtime() {
        let now = Instant::now();
        let info_hash = hash_index(72);
        let mut planner = DemandPlannerModel::new(now);
        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            72,
            LookupId(72),
            DemandSliceClass::NoConnectedPeers,
            2,
            now,
        );

        assert_eq!(
            planner.drain_runtime_readiness(None),
            HashMap::from([(info_hash, true)])
        );
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
    fn select_due_demand_launches_fairness_age_overtakes_yield_history() {
        let hash = |byte: u8| InfoHash::from([byte; InfoHash::LEN]);
        let now = Instant::now();
        let due = vec![
            DueDemandCandidate {
                info_hash: hash(1),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - DHT_DEMAND_FAIRNESS_AGE - Duration::from_secs(1),
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
        assert_eq!(selected[0].info_hash, hash(1));
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
    fn demand_planner_selection_stats_report_throttled_due_candidates() {
        fn hash(index: u32) -> InfoHash {
            let mut bytes = [0u8; InfoHash::LEN];
            bytes[..4].copy_from_slice(&index.to_be_bytes());
            InfoHash::from(bytes)
        }

        let now = Instant::now();
        let due = (0..16u32)
            .map(|index| DueDemandCandidate {
                info_hash: hash(index),
                demand: DhtDemandState {
                    awaiting_metadata: false,
                    connected_peers: 0,
                },
                next_eligible_at: now - Duration::from_secs(u64::from(index + 1)),
                subscriber_count: 1,
            })
            .collect::<Vec<_>>();
        let mut planner_budget = DemandPlannerBudget::new(now);

        let selection = select_due_demand_launches_with_stats(
            &due,
            DemandSlotCounts::default(),
            &HashMap::new(),
            &HashMap::new(),
            &mut planner_budget,
            now,
            DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        );

        assert_eq!(selection.launches.len(), DHT_NO_CONNECTED_PEERS_SLOT_CAP);
        assert_eq!(selection.stats.offered.no_connected_peers, 16);
        assert_eq!(
            selection.stats.launched.no_connected_peers,
            DHT_NO_CONNECTED_PEERS_SLOT_CAP
        );
        assert_eq!(
            selection.stats.throttled.no_connected_peers,
            16 - DHT_NO_CONNECTED_PEERS_SLOT_CAP
        );
        assert!(selection.stats.oldest_throttled_no_peers_ms >= 8_000);
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

    #[test]
    fn demand_planner_runtime_reset_action_clears_runtime_state_and_preserves_demands() {
        let now = Instant::now();
        let info_hash = hash_index(41);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));
        planner.active.insert(
            info_hash,
            active_lookup(LookupId(6), DemandSliceClass::NoConnectedPeers),
        );
        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            41,
            LookupId(6),
            DemandSliceClass::NoConnectedPeers,
            1,
            now,
        );
        planner.state.entry(info_hash).or_default().note_start(now);
        planner.parked_crawls.insert(
            info_hash,
            DemandCrawlState::new(now, DemandSliceClass::NoConnectedPeers),
        );

        let reset_at = now + Duration::from_secs(1);
        let reduction = planner.update(DemandPlannerAction::RuntimeReset { now: reset_at });

        assert!(reduction.effects.is_empty());
        assert!(planner.active.is_empty());
        assert!(planner.draining_demands.is_empty());
        assert!(planner.parked_crawls.is_empty());
        assert!(planner.state.is_empty());
        let snapshot = planner
            .scheduler
            .entry_snapshot(info_hash)
            .expect("demand entry");
        assert!(!snapshot.in_progress);
        assert_eq!(snapshot.next_eligible_at, now);
    }

    #[test]
    fn demand_planner_lookup_finished_action_updates_state_and_emits_metrics_effect() {
        let now = Instant::now();
        let info_hash = hash_index(42);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));

        planner.active = HashMap::from([(
            info_hash,
            active_lookup(LookupId(7), DemandSliceClass::NoConnectedPeers),
        )]);

        let reduction = planner.update(DemandPlannerAction::LookupFinished {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            total_peers: 11,
            unique_peers: 5,
            now,
        });

        assert!(planner.active.is_empty());
        let snapshot = planner
            .scheduler
            .entry_snapshot(info_hash)
            .expect("demand entry");
        assert!(!snapshot.in_progress);
        assert!(snapshot.next_eligible_at > now);
        let state = planner.state.get(&info_hash).expect("planner state");
        assert_eq!(state.last_finished_at, Some(now));
        assert_eq!(state.last_useful_yield_at, Some(now));
        assert_eq!(state.last_unique_peers, 5);
        assert_eq!(reduction.effects.len(), 1);
        let DemandPlannerEffect::LookupFinished(effect) = &reduction.effects[0] else {
            panic!("expected lookup finished effect");
        };
        assert_eq!(effect.info_hash, info_hash);
        assert_eq!(effect.slice_class, DemandSliceClass::NoConnectedPeers);
        assert_eq!(effect.total_peers, 11);
        assert_eq!(effect.unique_peers, 5);
        assert!(effect.previous.expect("previous snapshot").in_progress);
        assert!(!effect.current.expect("current snapshot").in_progress);
    }

    #[test]
    fn demand_planner_update_action_requests_drain_finalize_on_class_mismatch() {
        let now = Instant::now();
        let info_hash = hash_index(46);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));
        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            46,
            LookupId(10),
            DemandSliceClass::NoConnectedPeers,
            1,
            now,
        );

        let reduction = planner.update(DemandPlannerAction::DemandUpdated {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 4,
            },
            now,
        });

        assert_eq!(
            planner.scheduler.demand_state(info_hash),
            Some(DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 4,
            })
        );
        assert!(planner.draining_demands.contains_key(&info_hash));
        assert!(reduction.effects.iter().any(|effect| matches!(
            effect,
            DemandPlannerEffect::FinalizeDrainingLookup(finalize)
                if finalize.info_hash == info_hash && finalize.force
        )));
    }

    #[test]
    fn demand_planner_subscriber_removed_action_detaches_lookup_work_on_final_subscriber() {
        let now = Instant::now();
        let info_hash = hash_index(47);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));
        planner.active.insert(
            info_hash,
            active_lookup(LookupId(11), DemandSliceClass::NoConnectedPeers),
        );

        let reduction = planner.update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });

        assert!(planner.scheduler.entry_snapshot(info_hash).is_none());
        assert!(!planner.active.contains_key(&info_hash));
        let DemandPlannerEffect::ParkActiveLookup(effect) = reduction
            .effects
            .into_iter()
            .find(|effect| matches!(effect, DemandPlannerEffect::ParkActiveLookup(_)))
            .expect("park active lookup effect")
        else {
            panic!("expected park active lookup effect");
        };
        assert_eq!(effect.info_hash, info_hash);
        assert_eq!(effect.slice_class, DemandSliceClass::NoConnectedPeers);
        assert_eq!(
            effect
                .lookup_ids
                .lock()
                .expect("test lookup id lock")
                .as_slice(),
            &[LookupId(11)]
        );
    }

    #[test]
    fn demand_planner_peers_received_action_records_drain_unique_peers() {
        let now = Instant::now();
        let info_hash = hash_index(48);
        let mut planner = DemandPlannerModel::new(now);
        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            48,
            LookupId(12),
            DemandSliceClass::NoConnectedPeers,
            1,
            now,
        );
        let peers = synthetic_peers(49, 3).into_iter().collect::<Vec<_>>();

        let reduction = planner.update(DemandPlannerAction::PeersReceived {
            info_hash,
            peers: &peers,
        });

        let DemandPlannerEffect::DrainPeersRecorded(recorded) = reduction
            .effects
            .into_iter()
            .next()
            .expect("drain peers recorded effect")
        else {
            panic!("expected drain peers recorded effect");
        };
        assert_eq!(recorded.info_hash, info_hash);
        assert_eq!(recorded.peer_count, 3);
        assert_eq!(recorded.unique_added, 3);
        assert_eq!(recorded.initial_unique_peers, 1);
        assert_eq!(
            planner
                .draining_demands
                .get(&info_hash)
                .expect("draining demand")
                .unique_peer_count(),
            4
        );
    }

    #[test]
    fn demand_planner_drain_tick_action_requests_finalize_for_ready_drains() {
        let now = Instant::now();
        let info_hash = hash_index(50);
        let mut planner = DemandPlannerModel::new(now);
        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            50,
            LookupId(14),
            DemandSliceClass::NoConnectedPeers,
            1,
            now,
        );

        let waiting = planner.update(DemandPlannerAction::DrainTick {
            now,
            runtime_ready: HashMap::from([(info_hash, false)]),
        });
        assert!(waiting.effects.is_empty());

        let ready = planner.update(DemandPlannerAction::DrainTick {
            now: now + DHT_DEMAND_DRAIN_NO_LATE_YIELD_GRACE,
            runtime_ready: HashMap::from([(info_hash, false)]),
        });

        assert!(ready.effects.iter().any(|effect| matches!(
            effect,
            DemandPlannerEffect::FinalizeDrainingLookup(finalize)
                if finalize.info_hash == info_hash && !finalize.force
        )));
    }

    #[test]
    fn demand_planner_lookup_park_rejection_finishes_scheduler_entry() {
        let now = Instant::now();
        let info_hash = hash_index(43);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 4,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));

        planner.active = HashMap::from([(
            info_hash,
            active_lookup(LookupId(8), DemandSliceClass::RoutineRefresh),
        )]);

        let requested = planner.update(DemandPlannerAction::LookupParkRequested {
            info_hash,
            slice_class: DemandSliceClass::RoutineRefresh,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 0,
            unique_peers: HashSet::new(),
            lookup_ids: active_lookup(LookupId(8), DemandSliceClass::RoutineRefresh).lookup_ids,
        });
        assert!(planner.active.is_empty());
        let DemandPlannerEffect::AdmitDrain(admit) =
            requested.effects.into_iter().next().expect("admit effect")
        else {
            panic!("expected admit drain effect");
        };
        assert!(admit.previous.expect("previous snapshot").in_progress);

        let resolved = planner.update(DemandPlannerAction::LookupParkResolved {
            info_hash,
            slice_class: DemandSliceClass::RoutineRefresh,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 0,
            unique_peers: 0,
            parked_outcome: None,
            drain_admission: None,
            previous: admit.previous,
            now,
        });

        assert!(
            !planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );
        assert_eq!(
            planner
                .state
                .get(&info_hash)
                .expect("planner state")
                .last_finished_at,
            Some(now)
        );
        let DemandPlannerEffect::LookupParked(parked) =
            resolved.effects.into_iter().next().expect("parked effect")
        else {
            panic!("expected parked effect");
        };
        assert!(parked.drain_admission.is_none());
        assert!(parked.current.expect("current snapshot").in_progress == false);
    }

    #[test]
    fn demand_planner_lookup_park_admission_keeps_scheduler_entry_in_progress() {
        let now = Instant::now();
        let info_hash = hash_index(44);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));

        planner.active = HashMap::from([(
            info_hash,
            active_lookup(LookupId(9), DemandSliceClass::NoConnectedPeers),
        )]);

        let requested = planner.update(DemandPlannerAction::LookupParkRequested {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 3,
            unique_peers: synthetic_peers(44, 3),
            lookup_ids: active_lookup(LookupId(9), DemandSliceClass::NoConnectedPeers).lookup_ids,
        });
        assert!(planner.active.is_empty());
        let DemandPlannerEffect::AdmitDrain(admit) =
            requested.effects.into_iter().next().expect("admit effect")
        else {
            panic!("expected admit drain effect");
        };

        let resolved = planner.update(DemandPlannerAction::LookupParkResolved {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 3,
            unique_peers: 3,
            parked_outcome: Some(DemandParkedSliceOutcome::UsefulYield),
            drain_admission: Some(DemandDrainAdmissionSnapshot {
                initial_inflight_queries: 3,
                score: 42,
                deadline_ms: 5_000,
            }),
            previous: admit.previous,
            now,
        });

        assert!(
            planner
                .scheduler
                .entry_snapshot(info_hash)
                .expect("demand entry")
                .in_progress
        );
        assert!(planner.state.get(&info_hash).is_none());
        let DemandPlannerEffect::LookupParked(parked) =
            resolved.effects.into_iter().next().expect("parked effect")
        else {
            panic!("expected parked effect");
        };
        assert_eq!(parked.drain_admission.expect("drain admission").score, 42);
        assert!(parked.current.expect("current snapshot").in_progress);
    }

    #[test]
    fn demand_planner_lookup_park_admission_requests_finalize_after_class_change() {
        let now = Instant::now();
        let info_hash = hash_index(49);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));
        let previous = planner.scheduler.entry_snapshot(info_hash);
        let _ = planner.update(DemandPlannerAction::DemandUpdated {
            info_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 4,
            },
            now,
        });
        insert_synthetic_drain(
            &mut planner.draining_demands,
            info_hash,
            49,
            LookupId(13),
            DemandSliceClass::NoConnectedPeers,
            2,
            now,
        );

        let resolved = planner.update(DemandPlannerAction::LookupParkResolved {
            info_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 2,
            unique_peers: 2,
            parked_outcome: Some(DemandParkedSliceOutcome::UsefulYield),
            drain_admission: Some(DemandDrainAdmissionSnapshot {
                initial_inflight_queries: 2,
                score: 7,
                deadline_ms: 5_000,
            }),
            previous,
            now,
        });

        assert!(resolved
            .effects
            .iter()
            .any(|effect| matches!(effect, DemandPlannerEffect::LookupParked(_))));
        assert!(resolved.effects.iter().any(|effect| matches!(
            effect,
            DemandPlannerEffect::FinalizeDrainingLookup(finalize)
                if finalize.info_hash == info_hash && finalize.force
        )));
    }

    #[test]
    fn demand_planner_drain_finalized_action_finishes_and_applies_backoff_mode() {
        let now = Instant::now();
        let info_hash = hash_index(45);
        let mut planner = DemandPlannerModel::new(now);
        planner.scheduler.register(
            info_hash,
            DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            now,
        );
        assert!(planner.scheduler.mark_in_progress(info_hash));

        let previous = planner.scheduler.entry_snapshot(info_hash);

        let reduction = planner.update(DemandPlannerAction::DrainedLookupFinalized {
            info_hash,
            outcome: DrainedDemandOutcome {
                slice_class: DemandSliceClass::NoConnectedPeers,
                stop_reason: DemandSliceStopReason::IdleTimeout,
                total_peers: 0,
                unique_peers: 0,
                parked_outcome: Some(DemandParkedSliceOutcome::HealthyZeroYield),
                drain_duration_ms: 1_000,
                finalized_after_deadline: false,
                finalized_early_no_yield: true,
            },
            previous,
            now,
        });

        let snapshot = planner
            .scheduler
            .entry_snapshot(info_hash)
            .expect("demand entry");
        assert!(!snapshot.in_progress);
        assert!(snapshot.next_eligible_at > now);
        assert!(snapshot.no_connected_peers_backoff_step > 0);
        let state = planner.state.get(&info_hash).expect("planner state");
        assert_eq!(state.last_finished_at, Some(now));
        assert_eq!(state.last_useful_yield_at, None);
        assert_eq!(state.last_unique_peers, 0);

        let DemandPlannerEffect::DrainFinalized(finalized) = reduction
            .effects
            .into_iter()
            .next()
            .expect("finalized effect")
        else {
            panic!("expected drain finalized effect");
        };
        assert_eq!(
            finalized.finish_mode,
            DemandFinishMode::AcceleratedNoConnectedPeersBackoff
        );
        assert_eq!(finalized.outcome.unique_peers, 0);
        assert!(finalized.previous.expect("previous snapshot").in_progress);
        assert!(!finalized.current.expect("current snapshot").in_progress);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            ..ProptestConfig::default()
        })]

        #[test]
        fn demand_planner_selection_fuzz_respects_caps_budget_and_stats(
            specs in prop::collection::vec(planner_candidate_strategy(), 0..96),
            active_awaiting in 0usize..=12,
            active_no_peers in 0usize..=12,
            active_routine in 0usize..=12,
            total_budget in 0usize..=12,
        ) {
            let now = Instant::now();
            let mut seen = HashSet::new();
            let mut due_candidates = Vec::new();
            let mut planner_state = HashMap::new();

            for spec in specs {
                if !seen.insert(spec.index) {
                    continue;
                }

                let info_hash = hash_index(u32::from(spec.index));
                due_candidates.push(DueDemandCandidate {
                    info_hash,
                    demand: demand_for_fuzz_class(spec.demand_class, spec.connected_peers),
                    next_eligible_at: test_instant_saturating_sub(
                        now,
                        Duration::from_millis(u64::from(spec.overdue_ms)),
                    ),
                    subscriber_count: usize::from(spec.subscribers),
                });

                if let Some(useful_yield_age_ms) = spec.useful_yield_age_ms {
                    let useful_yield_at = test_instant_saturating_sub(
                        now,
                        Duration::from_millis(u64::from(useful_yield_age_ms)),
                    );
                    planner_state.insert(
                        info_hash,
                        DemandPlannerState {
                            last_started_at: Some(test_instant_saturating_sub(
                                useful_yield_at,
                                Duration::from_millis(250),
                            )),
                            last_finished_at: Some(useful_yield_at),
                            last_useful_yield_at: Some(useful_yield_at),
                            last_unique_peers: usize::from(spec.last_unique_peers),
                        },
                    );
                }
            }

            let active_counts = DemandSlotCounts {
                awaiting_metadata: active_awaiting,
                no_connected_peers: active_no_peers,
                routine_refresh: active_routine,
            };
            let mut planner_budget = DemandPlannerBudget::new(now);
            let selection = select_due_demand_launches_with_stats(
                &due_candidates,
                active_counts,
                &HashMap::new(),
                &planner_state,
                &mut planner_budget,
                now,
                total_budget,
            );

            prop_assert!(selection.launches.len() <= total_budget);

            let input_hashes = due_candidates
                .iter()
                .map(|candidate| candidate.info_hash)
                .collect::<HashSet<_>>();
            let mut launched_hashes = HashSet::new();
            let mut launched_counts = DemandSlotCounts::default();
            for launched in &selection.launches {
                prop_assert!(input_hashes.contains(&launched.info_hash));
                prop_assert!(launched_hashes.insert(launched.info_hash));
                launched_counts.record(DemandSliceClass::from_demand(launched.demand));
            }

            prop_assert!(
                launched_counts.awaiting_metadata
                    <= DHT_AWAITING_METADATA_SLOT_CAP.saturating_sub(active_awaiting)
            );
            prop_assert!(
                launched_counts.no_connected_peers
                    <= DHT_NO_CONNECTED_PEERS_SLOT_CAP.saturating_sub(active_no_peers)
            );
            prop_assert!(
                launched_counts.routine_refresh
                    <= DHT_ROUTINE_LOOKUP_SLOT_CAP.saturating_sub(active_routine)
            );

            let offered_counts = count_candidate_classes(&due_candidates);
            prop_assert_eq!(selection.stats.offered, offered_counts);
            prop_assert_eq!(selection.stats.launched, launched_counts);
            prop_assert_eq!(
                selection.stats.throttled.awaiting_metadata,
                offered_counts
                    .awaiting_metadata
                    .saturating_sub(launched_counts.awaiting_metadata)
            );
            prop_assert_eq!(
                selection.stats.throttled.no_connected_peers,
                offered_counts
                    .no_connected_peers
                    .saturating_sub(launched_counts.no_connected_peers)
            );
            prop_assert_eq!(
                selection.stats.throttled.routine_refresh,
                offered_counts
                    .routine_refresh
                    .saturating_sub(launched_counts.routine_refresh)
            );
        }

        #[test]
        fn demand_planner_state_machine_fuzz_preserves_capacity_and_entry_invariants(
            ops in prop::collection::vec(planner_machine_op_strategy(), 1..260)
        ) {
            let mut machine = PlannerMachine::new();

            for op in ops {
                machine.apply(op);
                machine.assert_invariants()?;
            }
        }
    }
}
