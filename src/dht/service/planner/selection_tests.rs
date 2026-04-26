use super::super::*;
use super::test_support::*;
use super::*;
use proptest::prelude::*;

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
}
