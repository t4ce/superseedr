// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::super::*;
use super::*;

pub(in crate::dht::service) fn take_parked_family_state(
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

pub(in crate::dht::service) fn store_parked_lookup_states(
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

pub(in crate::dht::service) fn parked_slice_outcome_for_quality(
    slice_class: DemandSliceClass,
    stop_reason: Option<DemandSliceStopReason>,
    unique_peers: usize,
    quality: AggregateLookupQualitySnapshot,
) -> Option<DemandParkedSliceOutcome> {
    let weak_parked_state = slice_class.parked_quality_is_weak(quality);
    stop_reason
        .map(|reason| slice_class.parked_slice_outcome(reason, unique_peers, weak_parked_state))
}

pub(in crate::dht::service) fn aggregate_lookup_quality(
    states: &[LookupState],
) -> AggregateLookupQualitySnapshot {
    let mut aggregate = AggregateLookupQualitySnapshot::default();
    for state in states {
        aggregate.extend(state.quality_snapshot());
    }
    aggregate
}

pub(in crate::dht::service) fn park_lookup_ids(
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

pub(in crate::dht::service) fn schedule_drained_demand_finalize(
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

pub(in crate::dht::service) fn demand_drain_duration(
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

pub(in crate::dht::service) fn demand_drain_no_late_yield_grace(
    slice_class: DemandSliceClass,
) -> Duration {
    match slice_class {
        DemandSliceClass::AwaitingMetadata => DHT_AWAITING_METADATA_DRAIN_NO_LATE_YIELD_GRACE,
        DemandSliceClass::NoConnectedPeers => DHT_DEMAND_DRAIN_NO_LATE_YIELD_GRACE,
        DemandSliceClass::RoutineRefresh => DHT_ROUTINE_DRAIN_NO_LATE_YIELD_GRACE,
    }
}

pub(in crate::dht::service) fn demand_drain_score(
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

pub(in crate::dht::service) fn draining_demand_inflight(
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

pub(in crate::dht::service) fn demand_drain_admission_snapshot(
    drain: &DrainingDemandLookup,
) -> DemandDrainAdmissionSnapshot {
    DemandDrainAdmissionSnapshot {
        initial_inflight_queries: drain.initial_inflight_queries,
        score: drain.score,
        deadline_ms: duration_ms(drain.deadline.saturating_duration_since(drain.started_at)),
    }
}

pub(in crate::dht::service) fn cancel_lookup_ids_to_parked(
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

pub(in crate::dht::service) fn drain_lookup_ids(
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

pub(in crate::dht::service) fn drained_demand_lookup_runtime_ready(
    active_runtime: Option<&ActiveRuntime>,
    drain: &DrainingDemandLookup,
) -> bool {
    active_runtime.is_none_or(|active| active.runtime.drained_lookups_ready(&drain.lookup_ids))
}

pub(in crate::dht::service) fn record_drain_peers_received(
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
    pub(in crate::dht::service) fn drain_runtime_readiness(
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

    pub(in crate::dht::service) fn take_parked_family_state(
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

    pub(in crate::dht::service) fn park_lookup_ids(
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

    pub(in crate::dht::service) fn drain_lookup_ids(
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

    pub(in crate::dht::service) fn drain_admission_snapshot(
        &self,
        info_hash: InfoHash,
    ) -> Option<DemandDrainAdmissionSnapshot> {
        self.draining_demands
            .get(&info_hash)
            .map(demand_drain_admission_snapshot)
    }

    pub(in crate::dht::service) fn finalize_drained_lookup(
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

pub(in crate::dht::service) fn drained_demand_lookup_ready_for_finalize(
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

pub(in crate::dht::service) fn finalize_drained_demand_lookup(
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

pub(in crate::dht::service) fn evict_stale_parked_crawls(
    parked_crawls: &mut HashMap<InfoHash, DemandCrawlState>,
    now: Instant,
) {
    parked_crawls.retain(|_, crawl| !crawl.is_stale(now) && !crawl.is_empty());
}
