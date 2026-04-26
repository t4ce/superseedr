use super::super::*;
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
        (key.clone(), 0u8..=96, advance_ms.clone()).prop_map(|(key, unique_peers, advance_ms)| {
            PlannerMachineOp::FinishActive {
                key,
                unique_peers,
                advance_ms,
            }
        }),
        (key.clone(), 0u8..=96, any::<u8>(), advance_ms.clone()).prop_map(
            |(key, unique_peers, stop_reason, advance_ms)| PlannerMachineOp::ParkActive {
                key,
                unique_peers,
                stop_reason,
                advance_ms,
            }
        ),
        (key.clone(), 0u8..=32, advance_ms.clone()).prop_map(|(key, peer_count, advance_ms)| {
            PlannerMachineOp::AddDrainPeers {
                key,
                peer_count,
                advance_ms,
            }
        }),
        (key, advance_ms.clone())
            .prop_map(|(key, advance_ms)| { PlannerMachineOp::FinalizeDrain { key, advance_ms } }),
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
    (any::<bool>(), 0usize..=32).prop_map(|(awaiting_metadata, connected_peers)| DhtDemandState {
        awaiting_metadata,
        connected_peers,
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
    let parked_outcome = slice_class.parked_slice_outcome(stop_reason, unique_peer_count, false);
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
