use super::super::*;
use super::test_support::*;
use super::*;
use proptest::prelude::*;

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

    let final_removal = planner.update(DemandPlannerAction::DemandSubscriberRemoved { info_hash });
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
fn demand_planner_duplicate_register_requests_drain_finalize_on_class_mismatch() {
    let now = Instant::now();
    let info_hash = hash_index(47);
    let mut planner = DemandPlannerModel::new(now);
    planner.update(DemandPlannerAction::DemandRegistered {
        info_hash,
        demand: DhtDemandState {
            awaiting_metadata: false,
            connected_peers: 0,
        },
        now,
    });
    insert_synthetic_drain(
        &mut planner.draining_demands,
        info_hash,
        47,
        LookupId(17),
        DemandSliceClass::NoConnectedPeers,
        1,
        now,
    );

    let same_class = planner.update(DemandPlannerAction::DemandRegistered {
        info_hash,
        demand: DhtDemandState {
            awaiting_metadata: false,
            connected_peers: 0,
        },
        now,
    });
    assert!(same_class.effects.is_empty());
    assert_eq!(
        planner
            .scheduler
            .entry_snapshot(info_hash)
            .expect("demand entry")
            .subscriber_count,
        2
    );

    let class_change = planner.update(DemandPlannerAction::DemandRegistered {
        info_hash,
        demand: DhtDemandState {
            awaiting_metadata: true,
            connected_peers: 0,
        },
        now,
    });

    assert!(class_change.effects.iter().any(|effect| matches!(
        effect,
        DemandPlannerEffect::FinalizeDrainingLookup(finalize)
            if finalize.info_hash == info_hash && finalize.force
    )));
    let snapshot = planner
        .scheduler
        .entry_snapshot(info_hash)
        .expect("demand entry");
    assert_eq!(snapshot.subscriber_count, 3);
    assert!(snapshot.demand.awaiting_metadata);
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

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]

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
