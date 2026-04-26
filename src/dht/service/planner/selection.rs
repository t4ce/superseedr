// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::super::*;
use super::*;

pub(in crate::dht::service) fn active_demand_lookup_slot_count(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
) -> usize {
    demand_lookup_ids.len()
}

pub(in crate::dht::service) fn active_demand_lookup_slot_counts(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
) -> DemandSlotCounts {
    let mut counts = DemandSlotCounts::default();
    for lookup in demand_lookup_ids.values() {
        counts.record(lookup.slice_class);
    }
    counts
}

pub(in crate::dht::service) fn draining_demand_slot_counts(
    draining_demands: &HashMap<InfoHash, DrainingDemandLookup>,
) -> DemandSlotCounts {
    let mut counts = DemandSlotCounts::default();
    for drain in draining_demands.values() {
        counts.record(drain.slice_class);
    }
    counts
}

pub(in crate::dht::service) fn drain_virtual_slot_count(draining_lookup_count: usize) -> usize {
    if draining_lookup_count == 0 {
        0
    } else {
        draining_lookup_count.saturating_add(DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT - 1)
            / DHT_DRAIN_LOOKUPS_PER_VIRTUAL_SLOT
    }
}

pub(in crate::dht::service) fn demand_lookup_launch_budget(
    demand_lookup_ids: &HashMap<InfoHash, ActiveDemandLookup>,
    draining_lookup_count: usize,
) -> usize {
    let consumed_slots = active_demand_lookup_slot_count(demand_lookup_ids)
        .saturating_add(drain_virtual_slot_count(draining_lookup_count));
    let available_slots = DHT_DEMAND_LOOKUP_SLOT_COUNT.saturating_sub(consumed_slots);
    available_slots.min(DHT_DEMAND_LOOKUP_SLOT_FILL_PER_TICK)
}

pub(in crate::dht::service) fn demand_lookup_class_slot_cap(class: DemandSliceClass) -> usize {
    match class {
        DemandSliceClass::AwaitingMetadata => DHT_AWAITING_METADATA_SLOT_CAP,
        DemandSliceClass::NoConnectedPeers => DHT_NO_CONNECTED_PEERS_SLOT_CAP,
        DemandSliceClass::RoutineRefresh => DHT_ROUTINE_LOOKUP_SLOT_CAP,
    }
}

pub(in crate::dht::service) fn demand_slice_class_priority(class: DemandSliceClass) -> u8 {
    match class {
        DemandSliceClass::AwaitingMetadata => 3,
        DemandSliceClass::NoConnectedPeers => 2,
        DemandSliceClass::RoutineRefresh => 1,
    }
}

pub(in crate::dht::service) fn due_candidate_has_reusable_parked_crawl(
    parked_crawls: &HashMap<InfoHash, DemandCrawlState>,
    candidate: DueDemandCandidate,
    now: Instant,
) -> bool {
    let class = DemandSliceClass::from_demand(candidate.demand);
    parked_crawls
        .get(&candidate.info_hash)
        .is_some_and(|crawl| !crawl.is_empty() && !crawl.should_reset_for(class, now))
}

pub(in crate::dht::service) fn candidate_last_useful_yield_age(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> Option<Duration> {
    planner_state
        .get(&info_hash)
        .and_then(|state| state.last_useful_yield_at)
        .map(|at| now.saturating_duration_since(at))
}

pub(in crate::dht::service) fn candidate_last_unique_peers(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
) -> usize {
    planner_state
        .get(&info_hash)
        .map(|state| state.last_unique_peers)
        .unwrap_or(0)
}

pub(in crate::dht::service) fn candidate_due_age(
    candidate: DueDemandCandidate,
    now: Instant,
) -> Duration {
    now.saturating_duration_since(candidate.next_eligible_at)
}

pub(in crate::dht::service) fn candidate_has_fairness_age(
    candidate: DueDemandCandidate,
    now: Instant,
) -> bool {
    candidate_due_age(candidate, now) >= DHT_DEMAND_FAIRNESS_AGE
}

pub(in crate::dht::service) fn candidate_has_useful_yield_history(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> bool {
    candidate_last_useful_yield_age(planner_state, info_hash, now).is_some()
        && candidate_last_unique_peers(planner_state, info_hash) > 0
}

pub(in crate::dht::service) fn candidate_selection_reason(
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

pub(in crate::dht::service) fn candidate_last_activity_age(
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

pub(in crate::dht::service) fn spare_research_candidate_ready(
    planner_state: &HashMap<InfoHash, DemandPlannerState>,
    info_hash: InfoHash,
    now: Instant,
) -> bool {
    candidate_last_activity_age(planner_state, info_hash, now)
        .map(|age| age >= DHT_DEMAND_SPARE_RESEARCH_MIN_INTERVAL)
        .unwrap_or(true)
}

pub(in crate::dht::service) fn demand_planner_selection_stats(
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

pub(in crate::dht::service) fn select_spare_research_launches(
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

pub(in crate::dht::service) fn select_due_demand_launches(
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

pub(in crate::dht::service) fn select_due_demand_launches_with_stats(
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
