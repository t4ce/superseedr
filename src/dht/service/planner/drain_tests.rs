use super::super::*;
use super::test_support::*;
use super::*;

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
        Some(DemandCrawlResetReason::LowQuality)
    );
    assert_eq!(low_quality.consecutive_healthy_zero_yield_slices, 2);

    let mut low_quality = DemandCrawlState::new(now, DemandSliceClass::RoutineRefresh);
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
fn awaiting_metadata_parked_crawl_resets_after_repeated_zero_yield() {
    let now = Instant::now();
    let mut crawl = DemandCrawlState::new(now, DemandSliceClass::AwaitingMetadata);

    for _ in 0..DHT_AWAITING_METADATA_STALLED_EMPTY_SLICE_RESET_THRESHOLD.saturating_sub(1) {
        crawl.observe_parked_slice(
            DemandSliceClass::AwaitingMetadata,
            DemandParkedSliceOutcome::HealthyZeroYield,
        );
        assert_eq!(
            crawl.reset_reason_for(
                DemandSliceClass::AwaitingMetadata,
                now + Duration::from_secs(1)
            ),
            None
        );
    }

    crawl.observe_parked_slice(
        DemandSliceClass::AwaitingMetadata,
        DemandParkedSliceOutcome::HealthyZeroYield,
    );
    assert_eq!(
        crawl.reset_reason_for(
            DemandSliceClass::AwaitingMetadata,
            now + Duration::from_secs(1)
        ),
        Some(DemandCrawlResetReason::LowQuality)
    );
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
        drained_demand_lookup_ready_for_finalize(false, &drain, start + Duration::from_millis(999),),
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
        DemandSelectionReason::SwarmSupport,
    );
    metrics.record_selection(
        DemandSliceClass::RoutineRefresh,
        DemandSelectionReason::Fairness,
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
    assert_eq!(metrics.routine_refresh.selected_swarm_support, 1);
    assert_eq!(metrics.routine_refresh.selected_fairness, 1);
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
    assert!(metrics.summary().contains("sel_support=1"));
    assert!(metrics.summary().contains("sel_yield=1"));
    assert!(metrics.summary().contains("sel_fair=1"));
    assert!(metrics.summary().contains("sel_due=1"));
    assert!(metrics.summary().contains("sel_spare=1"));
    assert!(metrics.summary().contains("reset_quality=1"));
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
    crawl.consecutive_stalled_low_yield_slices = DHT_ROUTINE_STALLED_EMPTY_SLICE_RESET_THRESHOLD;
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

    assert_eq!(demand_lookup_launch_budget(&active, 0), 5);
    assert_eq!(demand_lookup_launch_budget(&active, 16), 5);
    assert_eq!(demand_lookup_launch_budget(&active, 54), 2);
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
    assert!(!parked.current.expect("current snapshot").in_progress);
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
    assert!(!planner.state.contains_key(&info_hash));
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
