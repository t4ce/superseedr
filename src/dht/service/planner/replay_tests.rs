use super::super::*;
use super::test_support::*;
use super::*;

#[derive(Debug)]
struct PlannerReplay {
    base: Instant,
    now: Instant,
    planner: DemandPlannerModel,
    next_lookup_id: u64,
    transcript: Vec<String>,
}

impl PlannerReplay {
    fn new() -> Self {
        let base = Instant::now();
        Self {
            base,
            now: base,
            planner: DemandPlannerModel::new(base),
            next_lookup_id: 1,
            transcript: Vec::new(),
        }
    }

    fn advance(&mut self, duration: Duration) {
        self.now += duration;
    }

    fn register(&mut self, label: &str, key: u32, demand: DhtDemandState) {
        self.reduce(
            label,
            DemandPlannerAction::DemandRegistered {
                info_hash: hash_index(key),
                demand,
                now: self.now,
            },
        );
    }

    fn update(&mut self, label: &str, key: u32, demand: DhtDemandState) {
        self.reduce(
            label,
            DemandPlannerAction::DemandUpdated {
                info_hash: hash_index(key),
                demand,
                now: self.now,
            },
        );
    }

    fn update_metrics(&mut self, label: &str, key: u32, metrics: DhtDemandMetrics) {
        self.reduce(
            label,
            DemandPlannerAction::DemandMetricsUpdated {
                info_hash: hash_index(key),
                metrics,
            },
        );
    }

    fn plan(&mut self, label: &str, runtime_available: bool) {
        let reduction = self.reduce(
            label,
            DemandPlannerAction::PlanDue {
                now: self.now,
                runtime_available,
            },
        );
        for effect in reduction.effects {
            let DemandPlannerEffect::StartLookup(start) = effect else {
                continue;
            };
            let lookup_id = LookupId(self.next_lookup_id);
            self.next_lookup_id = self.next_lookup_id.saturating_add(1);
            self.reduce(
                "lookup-started",
                DemandPlannerAction::LookupStarted {
                    info_hash: start.candidate.info_hash,
                    slice_class: start.plan.class,
                    lookup_ids: active_lookup(lookup_id, start.plan.class).lookup_ids,
                },
            );
        }
    }

    fn finish(&mut self, label: &str, key: u32, total_peers: usize, unique_peers: usize) {
        let info_hash = hash_index(key);
        let slice_class = self
            .planner
            .active
            .get(&info_hash)
            .expect("active demand for finish")
            .slice_class;
        self.reduce(
            label,
            DemandPlannerAction::LookupFinished {
                info_hash,
                slice_class,
                total_peers,
                unique_peers,
                now: self.now,
            },
        );
    }

    fn park_active(
        &mut self,
        label: &str,
        key: u32,
        total_peers: usize,
        unique_peers: u8,
        stop_reason: DemandSliceStopReason,
    ) {
        let info_hash = hash_index(key);
        let active = self
            .planner
            .active
            .get(&info_hash)
            .cloned()
            .expect("active demand for park");
        let requested = self.reduce(
            label,
            DemandPlannerAction::LookupParkRequested {
                info_hash,
                slice_class: active.slice_class,
                stop_reason,
                total_peers,
                unique_peers: synthetic_peers(key as u8, unique_peers),
                lookup_ids: active.lookup_ids,
            },
        );

        for effect in requested.effects {
            let DemandPlannerEffect::AdmitDrain(admit) = effect else {
                continue;
            };
            insert_synthetic_drain_with_stop_reason(
                &mut self.planner.draining_demands,
                admit.info_hash,
                key as u8,
                LookupId(100 + u64::from(key)),
                admit.slice_class,
                admit.stop_reason,
                unique_peers,
                self.now,
            );
            let drain_admission = self.planner.drain_admission_snapshot(admit.info_hash);
            let unique_peer_count = admit.unique_peers.len();
            let parked_outcome = Some(admit.slice_class.parked_slice_outcome(
                admit.stop_reason,
                unique_peer_count,
                false,
            ));
            self.reduce(
                "lookup-park-resolved",
                DemandPlannerAction::LookupParkResolved {
                    info_hash: admit.info_hash,
                    slice_class: admit.slice_class,
                    stop_reason: admit.stop_reason,
                    total_peers: admit.total_peers,
                    unique_peers: unique_peer_count,
                    parked_outcome,
                    drain_admission,
                    previous: admit.previous,
                    now: self.now,
                },
            );
        }
    }

    fn add_drain_peers(&mut self, label: &str, key: u32, peer_count: u8) {
        let peers = synthetic_peers((key as u8).wrapping_add(40), peer_count)
            .into_iter()
            .collect::<Vec<_>>();
        self.reduce(
            label,
            DemandPlannerAction::PeersReceived {
                info_hash: hash_index(key),
                peers: &peers,
            },
        );
    }

    fn drain_tick(&mut self, label: &str, runtime_ready: bool) {
        let runtime_ready = self
            .planner
            .draining_demands
            .keys()
            .copied()
            .map(|info_hash| (info_hash, runtime_ready))
            .collect();
        let reduction = self.reduce(
            label,
            DemandPlannerAction::DrainTick {
                now: self.now,
                runtime_ready,
            },
        );
        for effect in reduction.effects {
            let DemandPlannerEffect::FinalizeDrainingLookup(finalize) = effect else {
                continue;
            };
            self.finalize_drained("drain-finalized", finalize.info_hash);
        }
    }

    fn finalize_drained(&mut self, label: &str, info_hash: InfoHash) {
        let drain = self
            .planner
            .draining_demands
            .remove(&info_hash)
            .expect("draining demand for finalize");
        let unique_peers = drain.unique_peer_count();
        let previous = self.planner.scheduler.entry_snapshot(info_hash);
        let parked_outcome =
            drain
                .slice_class
                .parked_slice_outcome(drain.stop_reason, unique_peers, false);
        self.reduce(
            label,
            DemandPlannerAction::DrainedLookupFinalized {
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
            },
        );
    }

    fn runtime_reset(&mut self, label: &str) {
        self.reduce(label, DemandPlannerAction::RuntimeReset { now: self.now });
    }

    fn reduce(&mut self, label: &str, action: DemandPlannerAction<'_>) -> DemandPlannerReduction {
        let reduction = self.planner.update(action);
        check_demand_planner_invariants(&self.planner).unwrap_or_else(|violation| {
            panic!("{label} violated planner invariant: {violation:?}")
        });
        self.transcript.push(format!(
            "{label}: effects=[{}] plan=[{}]",
            effect_labels(&reduction.effects).join(","),
            plan_label(reduction.plan_stats),
        ));
        reduction
    }

    fn rendered(&self) -> String {
        let mut lines = self.transcript.clone();
        lines.push(format!(
            "final-state: {}",
            state_label(self.base, &self.planner)
        ));
        lines.join("\n")
    }
}

fn effect_labels(effects: &[DemandPlannerEffect]) -> Vec<String> {
    effects.iter().map(effect_label).collect()
}

fn effect_label(effect: &DemandPlannerEffect) -> String {
    match effect {
        DemandPlannerEffect::StartLookup(start) => format!(
            "start:{}:{:?}:{:?}:{}x:cap{}",
            hash_label(start.candidate.info_hash),
            start.plan.class,
            start.selection_reason,
            start.plan.power_multiplier,
            start.plan.unique_peer_cap,
        ),
        DemandPlannerEffect::LookupFinished(finished) => format!(
            "finish:{}:{:?}:total{}:unique{}",
            hash_label(finished.info_hash),
            finished.slice_class,
            finished.total_peers,
            finished.unique_peers,
        ),
        DemandPlannerEffect::AdmitDrain(admit) => format!(
            "admit-drain:{}:{:?}:{:?}:total{}:unique{}",
            hash_label(admit.info_hash),
            admit.slice_class,
            admit.stop_reason,
            admit.total_peers,
            admit.unique_peers.len(),
        ),
        DemandPlannerEffect::LookupParked(parked) => format!(
            "parked:{}:{:?}:{:?}:total{}:unique{}:admitted{}",
            hash_label(parked.info_hash),
            parked.slice_class,
            parked.stop_reason,
            parked.total_peers,
            parked.unique_peers,
            parked.drain_admission.is_some(),
        ),
        DemandPlannerEffect::DrainFinalized(finalized) => format!(
            "drain-final:{}:{:?}:{:?}:total{}:unique{}:{:?}:parked{}",
            hash_label(finalized.info_hash),
            finalized.outcome.slice_class,
            finalized.outcome.stop_reason,
            finalized.outcome.total_peers,
            finalized.outcome.unique_peers,
            finalized.finish_mode,
            finalized.parked,
        ),
        DemandPlannerEffect::ParkActiveLookup(park) => format!(
            "park-active:{}:{:?}:lookups{}",
            hash_label(park.info_hash),
            park.slice_class,
            park.lookup_ids.lock().expect("test lookup id lock").len(),
        ),
        DemandPlannerEffect::CancelDrainingLookup(cancel) => format!(
            "cancel-drain:{}:lookups{}",
            hash_label(cancel.info_hash),
            cancel.lookup_ids.len(),
        ),
        DemandPlannerEffect::FinalizeDrainingLookup(finalize) => format!(
            "finalize-drain:{}:force{}",
            hash_label(finalize.info_hash),
            finalize.force,
        ),
        DemandPlannerEffect::DrainPeersRecorded(recorded) => format!(
            "drain-peers:{}:count{}:added{}:initial{}",
            hash_label(recorded.info_hash),
            recorded.peer_count,
            recorded.unique_added,
            recorded.initial_unique_peers,
        ),
    }
}

fn plan_label(plan: Option<DemandPlannerPlanStats>) -> String {
    let Some(plan) = plan else {
        return String::new();
    };
    format!(
        "budget{}:due{}:spare{}:idle{}:{}x:active{}/{}/{}:drain{}",
        plan.launch_budget,
        plan.due_total,
        plan.spare_selected,
        plan.idle_probe_selected,
        if plan.idle_probe_active { 1 } else { 0 },
        plan.active_counts.awaiting_metadata,
        plan.active_counts.no_connected_peers,
        plan.active_counts.routine_refresh,
        plan.draining_count,
    )
}

fn state_label(base: Instant, planner: &DemandPlannerModel) -> String {
    format!(
        "entries{{{}}};active{{{}}};pending{{{}}};drain{{{}}};history{{{}}}",
        entry_labels(base, planner).join("|"),
        active_labels(planner).join("|"),
        pending_labels(planner).join("|"),
        drain_labels(base, planner).join("|"),
        history_labels(base, planner).join("|"),
    )
}

fn entry_labels(base: Instant, planner: &DemandPlannerModel) -> Vec<String> {
    let mut labels = planner
        .scheduler
        .entry_snapshots()
        .into_iter()
        .map(|snapshot| {
            format!(
                "{}:{:?}:sub{}:in{}:next{}:retry{}:probe{}",
                hash_label(snapshot.info_hash),
                DemandSliceClass::from_demand(snapshot.demand),
                snapshot.subscriber_count,
                snapshot.in_progress,
                instant_ms(base, snapshot.next_eligible_at),
                snapshot.no_connected_peers_backoff_step,
                snapshot.metrics.wants_idle_speed_probe_for(snapshot.demand),
            )
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn active_labels(planner: &DemandPlannerModel) -> Vec<String> {
    let mut labels = planner
        .active
        .iter()
        .map(|(info_hash, active)| {
            format!(
                "{}:{:?}:ids{}",
                hash_label(*info_hash),
                active.slice_class,
                active.lookup_ids.lock().expect("test lookup id lock").len(),
            )
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn pending_labels(planner: &DemandPlannerModel) -> Vec<String> {
    let mut labels = planner
        .pending_starts
        .iter()
        .map(|(info_hash, slice_class)| format!("start:{}:{slice_class:?}", hash_label(*info_hash)))
        .chain(
            planner
                .pending_parks
                .iter()
                .map(|(info_hash, slice_class)| {
                    format!("park:{}:{slice_class:?}", hash_label(*info_hash))
                }),
        )
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn drain_labels(base: Instant, planner: &DemandPlannerModel) -> Vec<String> {
    let mut labels = planner
        .draining_demands
        .iter()
        .map(|(info_hash, drain)| {
            format!(
                "{}:{:?}:{:?}:total{}:unique{}:deadline{}",
                hash_label(*info_hash),
                drain.slice_class,
                drain.stop_reason,
                drain.total_peers,
                drain.unique_peer_count(),
                instant_ms(base, drain.deadline),
            )
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn history_labels(base: Instant, planner: &DemandPlannerModel) -> Vec<String> {
    let mut labels = planner
        .state
        .iter()
        .map(|(info_hash, state)| {
            format!(
                "{}:start{}:finish{}:yield{}:unique{}",
                hash_label(*info_hash),
                optional_instant_ms(base, state.last_started_at),
                optional_instant_ms(base, state.last_finished_at),
                optional_instant_ms(base, state.last_useful_yield_at),
                state.last_unique_peers,
            )
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn optional_instant_ms(base: Instant, instant: Option<Instant>) -> String {
    instant
        .map(|instant| instant_ms(base, instant).to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn instant_ms(base: Instant, instant: Instant) -> u64 {
    duration_ms(instant.saturating_duration_since(base))
}

fn hash_label(info_hash: InfoHash) -> String {
    short_info_hash(info_hash)
}

fn metadata_demand() -> DhtDemandState {
    DhtDemandState {
        awaiting_metadata: true,
        connected_peers: 0,
    }
}

fn no_peer_demand() -> DhtDemandState {
    DhtDemandState {
        awaiting_metadata: false,
        connected_peers: 0,
    }
}

fn routine_demand(connected_peers: usize) -> DhtDemandState {
    DhtDemandState {
        awaiting_metadata: false,
        connected_peers,
    }
}

fn active_complete_upload_metrics(connected_peers: usize) -> DhtDemandMetrics {
    DhtDemandMetrics {
        accepting_new_peers: true,
        complete: true,
        total_pieces: 100,
        completed_pieces: 100,
        connected_peers,
        peers_interested_in_us: 4,
        unchoked_upload_peers: 1,
        upload_speed_bps: 32_000,
        ..Default::default()
    }
}

fn idle_probe_metrics() -> DhtDemandMetrics {
    DhtDemandMetrics {
        accepting_new_peers: true,
        total_pieces: 100,
        completed_pieces: 20,
        connected_peers: 0,
        ..Default::default()
    }
}

fn demand_from_trace_class(class: &str, connected_peers: usize) -> DhtDemandState {
    match class {
        "metadata" => metadata_demand(),
        "no-peer" => no_peer_demand(),
        "routine" => routine_demand(connected_peers.max(1)),
        _ => panic!("unknown trace demand class: {class}"),
    }
}

fn stop_reason_from_trace(token: &str) -> DemandSliceStopReason {
    match token {
        "wall" => DemandSliceStopReason::WallTime,
        "idle" => DemandSliceStopReason::IdleTimeout,
        "first-batch" => DemandSliceStopReason::FirstBatch,
        "cap" => DemandSliceStopReason::UniquePeerCap,
        _ => panic!("unknown trace stop reason: {token}"),
    }
}

fn metrics_from_trace(token: &str, connected_peers: usize) -> DhtDemandMetrics {
    match token {
        "idle" => idle_probe_metrics(),
        "upload" => active_complete_upload_metrics(connected_peers.max(1)),
        "download-starved" => DhtDemandMetrics {
            accepting_new_peers: true,
            complete: false,
            total_pieces: 100,
            completed_pieces: 20,
            connected_peers: connected_peers.max(1),
            download_speed_bps: 0,
            ..Default::default()
        },
        _ => panic!("unknown trace metrics kind: {token}"),
    }
}

fn replay_normalized_trace_fixture(script: &str) -> String {
    let mut replay = PlannerReplay::new();
    for raw_line in script.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts = line.split_whitespace().collect::<Vec<_>>();
        match parts.as_slice() {
            ["register", label, key, class] => replay.register(
                label,
                key.parse().expect("trace register key"),
                demand_from_trace_class(class, 0),
            ),
            ["register", label, key, class, peers] => replay.register(
                label,
                key.parse().expect("trace register key"),
                demand_from_trace_class(class, peers.parse().expect("trace connected peers")),
            ),
            ["update", label, key, class, peers] => replay.update(
                label,
                key.parse().expect("trace update key"),
                demand_from_trace_class(class, peers.parse().expect("trace connected peers")),
            ),
            ["metrics", label, key, kind, peers] => replay.update_metrics(
                label,
                key.parse().expect("trace metrics key"),
                metrics_from_trace(kind, peers.parse().expect("trace metrics peers")),
            ),
            ["plan", label, runtime_available] => {
                replay.plan(
                    label,
                    runtime_available.parse().expect("trace runtime flag"),
                );
            }
            ["advance-ms", millis] => {
                replay.advance(Duration::from_millis(
                    millis.parse().expect("trace advance millis"),
                ));
            }
            ["finish", label, key, total_peers, unique_peers] => replay.finish(
                label,
                key.parse().expect("trace finish key"),
                total_peers.parse().expect("trace total peers"),
                unique_peers.parse().expect("trace unique peers"),
            ),
            ["park", label, key, total_peers, unique_peers, stop_reason] => replay.park_active(
                label,
                key.parse().expect("trace park key"),
                total_peers.parse().expect("trace park total peers"),
                unique_peers.parse().expect("trace park unique peers"),
                stop_reason_from_trace(stop_reason),
            ),
            ["drain-peers", label, key, peer_count] => replay.add_drain_peers(
                label,
                key.parse().expect("trace drain-peers key"),
                peer_count.parse().expect("trace drain peer count"),
            ),
            ["drain-tick", label, runtime_ready] => {
                replay.drain_tick(
                    label,
                    runtime_ready.parse().expect("trace runtime-ready flag"),
                );
            }
            ["runtime-reset", label] => replay.runtime_reset(label),
            _ => panic!("invalid trace fixture line: {line}"),
        }
    }
    replay.rendered()
}

#[test]
fn demand_planner_replays_normalized_trace_fixture() {
    let rendered = replay_normalized_trace_fixture(
        r#"
        # This is the compact form we can derive from captured planner traces.
        register captured-metadata 21 metadata
        register captured-no-peer 22 no-peer
        register captured-routine 23 routine 3
        metrics captured-routine-metrics 23 upload 3
        plan captured-plan true
        "#,
    );
    let expected = r#"
captured-metadata: effects=[] plan=[]
captured-no-peer: effects=[] plan=[]
captured-routine: effects=[] plan=[]
captured-routine-metrics: effects=[] plan=[]
captured-plan: effects=[start:00000015:AwaitingMetadata:OverdueScarce:2x:cap256,start:00000017:RoutineRefresh:SwarmSupport:2x:cap96,start:00000016:NoConnectedPeers:OverdueScarce:1x:cap48] plan=[budget5:due3:spare0:idle0:0x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
lookup-started: effects=[] plan=[]
lookup-started: effects=[] plan=[]
final-state: entries{00000015:AwaitingMetadata:sub1:intrue:next0:retry0:probefalse|00000016:NoConnectedPeers:sub1:intrue:next0:retry0:probefalse|00000017:RoutineRefresh:sub1:intrue:next0:retry0:probetrue};active{00000015:AwaitingMetadata:ids1|00000016:NoConnectedPeers:ids1|00000017:RoutineRefresh:ids1};pending{};drain{};history{00000015:start0:finish-:yield-:unique0|00000016:start0:finish-:yield-:unique0|00000017:start0:finish-:yield-:unique0}
"#
    .trim();

    assert_eq!(rendered, expected);
}

#[test]
fn demand_planner_replays_fixed_trace_with_stable_effects_and_state() {
    let mut replay = PlannerReplay::new();

    replay.register("register-metadata", 1, metadata_demand());
    replay.register("register-no-peer", 2, no_peer_demand());
    replay.register("register-routine", 3, routine_demand(4));
    replay.update_metrics("routine-metrics", 3, active_complete_upload_metrics(4));
    replay.plan("initial-plan", true);

    replay.advance(Duration::from_millis(1_000));
    replay.finish("finish-metadata", 1, 0, 0);
    replay.park_active("park-no-peer", 2, 2, 2, DemandSliceStopReason::WallTime);
    replay.add_drain_peers("late-drain-peers", 2, 3);
    replay.drain_tick("drain-waiting", false);

    replay.advance(DHT_DEMAND_DRAIN_MAX_AGE);
    replay.drain_tick("drain-ready", true);
    replay.finish("finish-routine", 3, 96, 72);
    replay.update("no-peer-becomes-routine", 2, routine_demand(5));
    replay.runtime_reset("runtime-reset");

    let rendered = replay.rendered();
    let expected = r#"
register-metadata: effects=[] plan=[]
register-no-peer: effects=[] plan=[]
register-routine: effects=[] plan=[]
routine-metrics: effects=[] plan=[]
initial-plan: effects=[start:00000001:AwaitingMetadata:OverdueScarce:2x:cap256,start:00000003:RoutineRefresh:SwarmSupport:2x:cap96,start:00000002:NoConnectedPeers:OverdueScarce:1x:cap48] plan=[budget5:due3:spare0:idle0:0x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
lookup-started: effects=[] plan=[]
lookup-started: effects=[] plan=[]
finish-metadata: effects=[finish:00000001:AwaitingMetadata:total0:unique0] plan=[]
park-no-peer: effects=[admit-drain:00000002:NoConnectedPeers:WallTime:total2:unique2] plan=[]
lookup-park-resolved: effects=[parked:00000002:NoConnectedPeers:WallTime:total2:unique2:admittedtrue] plan=[]
late-drain-peers: effects=[drain-peers:00000002:count3:added3:initial2] plan=[]
drain-waiting: effects=[] plan=[]
drain-ready: effects=[finalize-drain:00000002:forcefalse] plan=[]
drain-finalized: effects=[drain-final:00000002:NoConnectedPeers:WallTime:total5:unique5:Standard:parkedfalse] plan=[]
finish-routine: effects=[finish:00000003:RoutineRefresh:total96:unique72] plan=[]
no-peer-becomes-routine: effects=[] plan=[]
runtime-reset: effects=[] plan=[]
final-state: entries{00000001:AwaitingMetadata:sub1:infalse:next2000:retry0:probefalse|00000002:RoutineRefresh:sub1:infalse:next22000:retry0:probefalse|00000003:RoutineRefresh:sub1:infalse:next66000:retry0:probetrue};active{};pending{};drain{};history{}
"#
    .trim();
    assert_eq!(rendered, expected);
}

#[test]
fn demand_planner_replays_idle_speed_probe_boost_without_wall_clock_or_network() {
    let mut replay = PlannerReplay::new();

    replay.register("register-idle-probe", 10, no_peer_demand());
    replay.update_metrics("idle-probe-metrics", 10, idle_probe_metrics());
    replay.plan("start-0", true);
    replay.finish("finish-0", 10, 0, 0);

    replay.advance(DHT_NO_CONNECTED_PEERS_BASE_INTERVAL);
    replay.plan("start-16s", true);
    replay.finish("finish-16s", 10, 0, 0);

    replay.advance(DHT_NO_CONNECTED_PEERS_BASE_INTERVAL * 2);
    replay.plan("start-48s", true);
    replay.finish("finish-48s", 10, 0, 0);

    replay.advance(DHT_NO_CONNECTED_PEERS_BASE_INTERVAL * 4);
    replay.plan("start-112s", true);
    replay.finish("finish-112s", 10, 0, 0);

    replay.advance(DHT_IDLE_SPEED_PROBE_4X_MIN_IDLE);
    replay.plan("idle-4x-before-next-eligible", true);

    let rendered = replay.rendered();
    let expected = r#"
register-idle-probe: effects=[] plan=[]
idle-probe-metrics: effects=[] plan=[]
start-0: effects=[start:0000000a:NoConnectedPeers:OverdueScarce:1x:cap48] plan=[budget5:due1:spare0:idle0:0x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
finish-0: effects=[finish:0000000a:NoConnectedPeers:total0:unique0] plan=[]
start-16s: effects=[start:0000000a:NoConnectedPeers:OverdueScarce:1x:cap48] plan=[budget5:due1:spare0:idle0:0x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
finish-16s: effects=[finish:0000000a:NoConnectedPeers:total0:unique0] plan=[]
start-48s: effects=[start:0000000a:NoConnectedPeers:OverdueScarce:2x:cap96] plan=[budget5:due1:spare0:idle0:1x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
finish-48s: effects=[finish:0000000a:NoConnectedPeers:total0:unique0] plan=[]
start-112s: effects=[start:0000000a:NoConnectedPeers:OverdueScarce:3x:cap144] plan=[budget5:due1:spare0:idle0:1x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
finish-112s: effects=[finish:0000000a:NoConnectedPeers:total0:unique0] plan=[]
idle-4x-before-next-eligible: effects=[start:0000000a:NoConnectedPeers:IdleSpeedProbe:4x:cap192] plan=[budget5:due0:spare0:idle1:1x:active0/0/0:drain0]
lookup-started: effects=[] plan=[]
final-state: entries{0000000a:NoConnectedPeers:sub1:intrue:next240000:retry4:probetrue};active{0000000a:NoConnectedPeers:ids1};pending{};drain{};history{0000000a:start232000:finish112000:yield-:unique0}
"#
    .trim();
    assert_eq!(rendered, expected);
}
