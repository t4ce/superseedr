// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{ActiveRuntime, BootstrapSummary, DhtBackendKind};

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
    pub demand_power_multiplier: u8,
}

#[derive(Debug)]
pub(in crate::dht::service) struct RecentUniquePeers {
    window: Duration,
    events: VecDeque<(Instant, SocketAddr)>,
    last_seen: HashMap<SocketAddr, Instant>,
}

impl RecentUniquePeers {
    pub(in crate::dht::service) fn new(window: Duration) -> Self {
        Self {
            window,
            events: VecDeque::new(),
            last_seen: HashMap::new(),
        }
    }

    pub(in crate::dht::service) fn record_batch(&mut self, now: Instant, peers: &[SocketAddr]) {
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

    pub(in crate::dht::service) fn unique_count(&mut self, now: Instant) -> usize {
        self.evict_expired(now);
        self.last_seen.len()
    }
}

pub(super) fn build_status(
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
        health.public_addr = runtime_health
            .confirmed_public_addr_ipv4
            .or(runtime_health.confirmed_public_addr_ipv6);
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

pub(super) fn publish_status(
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

pub(super) fn build_wave_telemetry(
    active_runtime: Option<&ActiveRuntime>,
    unique_peers_found_last_10s: usize,
    demand_power_multiplier: u8,
) -> DhtWaveTelemetry {
    let Some(active_runtime) = active_runtime else {
        return DhtWaveTelemetry {
            unique_peers_found_last_10s,
            demand_power_multiplier,
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
        demand_power_multiplier,
    }
}

pub(super) fn publish_wave_telemetry(
    wave_telemetry_tx: &watch::Sender<DhtWaveTelemetry>,
    active_runtime: Option<&ActiveRuntime>,
    recent_unique_peers: &mut RecentUniquePeers,
    demand_power_multiplier: u8,
) {
    let telemetry = build_wave_telemetry(
        active_runtime,
        recent_unique_peers.unique_count(Instant::now()),
        demand_power_multiplier,
    );
    if *wave_telemetry_tx.borrow() != telemetry {
        let _ = wave_telemetry_tx.send(telemetry);
    }
}
