use super::test_support::*;
use super::*;

struct ServiceReplay {
    base: Instant,
    now: Instant,
    state: DhtServiceState,
    transcript: Vec<String>,
}

impl ServiceReplay {
    fn new() -> Self {
        let config = disabled_service_config();
        let base = Instant::now();
        Self {
            base,
            now: base,
            state: DhtServiceState::new(config, 0, None),
            transcript: Vec::new(),
        }
    }

    fn advance(&mut self, duration: Duration) {
        self.now += duration;
    }

    fn service(&mut self, label: &'static str, action: DhtServiceAction) {
        let reduction = self.state.update_service_action(action);
        self.transcript.push(format!(
            "{label}: effects=[{}] state=[{}]",
            service_effect_labels(&reduction.effects).join(","),
            service_state_label(self.base, &self.state),
        ));
    }

    fn demand(&mut self, label: &'static str, action: DhtDemandCommandAction) {
        let reduction = self.state.update_demand_command(action);
        self.transcript.push(format!(
            "{label}: effects=[{}] state=[{}]",
            demand_command_effect_labels(&reduction.effects).join(","),
            service_state_label(self.base, &self.state),
        ));
    }

    fn render(&self) -> String {
        self.transcript.join("\n")
    }
}

fn service_effect_labels(effects: &[DhtServiceEffect]) -> Vec<String> {
    effects
        .iter()
        .map(|effect| match effect {
            DhtServiceEffect::BuildRuntime { config } => format!(
                "build-runtime:backend{:?}:port{}:bootstrap{}",
                config.preferred_backend,
                config.port,
                config.bootstrap_nodes.len(),
            ),
            DhtServiceEffect::ResetDemandPlanner => "reset-planner".to_string(),
            DhtServiceEffect::PublishStatus => "publish-status".to_string(),
            DhtServiceEffect::StartDueDemands => "start-due".to_string(),
        })
        .collect()
}

fn demand_command_effect_labels(effects: &[DhtDemandCommandEffect]) -> Vec<String> {
    effects
        .iter()
        .map(|effect| match effect {
            DhtDemandCommandEffect::SendRegisterResponse { subscriber_id, .. } => {
                format!("register-response:{subscriber_id:?}")
            }
            DhtDemandCommandEffect::ApplySubscriberEffects(effects) => {
                format!(
                    "subscriber[{}]",
                    subscriber_effect_labels(effects).join(",")
                )
            }
            DhtDemandCommandEffect::ApplyPlannerEffects(effects) => {
                format!("planner[{}]", planner_effect_labels(effects).join(","))
            }
            DhtDemandCommandEffect::StartDueDemands => "start-due".to_string(),
        })
        .collect()
}

fn subscriber_effect_labels(effects: &[DemandSubscriberEffect]) -> Vec<String> {
    effects
        .iter()
        .map(|effect| match effect {
            DemandSubscriberEffect::Registered {
                info_hash,
                demand,
                subscriber_id,
            } => format!(
                "registered:{}:{:?}:sub{}",
                short_info_hash(*info_hash),
                DemandSliceClass::from_demand(*demand),
                subscriber_id,
            ),
            DemandSubscriberEffect::SubscriberRemoved { info_hash } => {
                format!("removed:{}", short_info_hash(*info_hash))
            }
            DemandSubscriberEffect::DeliverPeers {
                info_hash,
                peers,
                deliveries,
            } => format!(
                "deliver:{}:peers{}:subs{}",
                short_info_hash(*info_hash),
                peers.len(),
                deliveries.len(),
            ),
        })
        .collect()
}

fn planner_effect_labels(effects: &[DemandPlannerEffect]) -> Vec<String> {
    effects
        .iter()
        .map(|effect| match effect {
            DemandPlannerEffect::StartLookup(start) => format!(
                "start:{}:{:?}:{:?}:{}x",
                short_info_hash(start.candidate.info_hash),
                start.plan.class,
                start.selection_reason,
                start.plan.power_multiplier,
            ),
            DemandPlannerEffect::LookupFinished(finished) => format!(
                "finished:{}:{:?}:total{}:unique{}",
                short_info_hash(finished.info_hash),
                finished.slice_class,
                finished.total_peers,
                finished.unique_peers,
            ),
            DemandPlannerEffect::AdmitDrain(admit) => format!(
                "admit-drain:{}:{:?}:unique{}",
                short_info_hash(admit.info_hash),
                admit.slice_class,
                admit.unique_peers.len(),
            ),
            DemandPlannerEffect::LookupParked(parked) => format!(
                "parked:{}:{:?}:unique{}",
                short_info_hash(parked.info_hash),
                parked.slice_class,
                parked.unique_peers,
            ),
            DemandPlannerEffect::DrainFinalized(finalized) => format!(
                "drain-final:{}:{:?}:unique{}",
                short_info_hash(finalized.info_hash),
                finalized.outcome.slice_class,
                finalized.outcome.unique_peers,
            ),
            DemandPlannerEffect::ParkActiveLookup(park) => format!(
                "park-active:{}:{:?}",
                short_info_hash(park.info_hash),
                park.slice_class,
            ),
            DemandPlannerEffect::CancelDrainingLookup(cancel) => {
                format!("cancel-drain:{}", short_info_hash(cancel.info_hash))
            }
            DemandPlannerEffect::FinalizeDrainingLookup(finalize) => format!(
                "finalize-drain:{}:force{}",
                short_info_hash(finalize.info_hash),
                finalize.force,
            ),
            DemandPlannerEffect::DrainPeersRecorded(recorded) => format!(
                "drain-peers:{}:count{}:added{}",
                short_info_hash(recorded.info_hash),
                recorded.peer_count,
                recorded.unique_added,
            ),
        })
        .collect()
}

fn service_state_label(base: Instant, state: &DhtServiceState) -> String {
    format!(
        "service{{backend{:?}:port{}:gen{}:warn{}}};subs{{{}}};entries{{{}}}",
        state.service.config().preferred_backend,
        state.service.config().port,
        state.service.generation(),
        state
            .service
            .warning_owned()
            .unwrap_or_else(|| "-".to_string()),
        subscriber_labels(state).join("|"),
        entry_labels(base, state).join("|"),
    )
}

fn subscriber_labels(state: &DhtServiceState) -> Vec<String> {
    let mut labels = state
        .demand_subscribers
        .subscribers
        .iter()
        .map(|(info_hash, subscribers)| {
            format!("{}:{}", short_info_hash(*info_hash), subscribers.len())
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn entry_labels(base: Instant, state: &DhtServiceState) -> Vec<String> {
    let mut labels = state
        .demand_planner
        .scheduler
        .entry_snapshots()
        .into_iter()
        .map(|snapshot| {
            format!(
                "{}:{:?}:sub{}:in{}:next{}:retry{}",
                short_info_hash(snapshot.info_hash),
                DemandSliceClass::from_demand(snapshot.demand),
                snapshot.subscriber_count,
                snapshot.in_progress,
                duration_ms(snapshot.next_eligible_at.saturating_duration_since(base)),
                snapshot.no_connected_peers_backoff_step,
            )
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn register_action(
    info_hash: InfoHash,
    demand: DhtDemandState,
    now: Instant,
) -> DhtDemandCommandAction {
    let (subscriber_tx, _subscriber_rx) = mpsc::unbounded_channel();
    let (response_tx, _response_rx) = oneshot::channel();
    DhtDemandCommandAction::Register {
        info_hash,
        demand,
        subscriber_tx,
        response_tx,
        now,
    }
}

fn no_peer_demand() -> DhtDemandState {
    DhtDemandState {
        awaiting_metadata: false,
        connected_peers: 0,
    }
}

fn metadata_demand() -> DhtDemandState {
    DhtDemandState {
        awaiting_metadata: true,
        connected_peers: 0,
    }
}

fn routine_demand(connected_peers: usize) -> DhtDemandState {
    DhtDemandState {
        awaiting_metadata: false,
        connected_peers,
    }
}

#[test]
fn dht_service_state_replays_demand_and_service_reductions_deterministically() {
    let mut replay = ServiceReplay::new();
    let info_hash = hash_index(201);

    replay.service(
        "request-reconfigure",
        DhtServiceAction::ReconfigureRequested {
            config: DhtServiceConfig {
                port: 6881,
                bootstrap_nodes: vec!["198.51.100.10:6881".to_string()],
                preferred_backend: DhtBackendKind::InternalPrototype,
                force_internal_failure: false,
            },
        },
    );
    replay.service(
        "failed-reconfigure",
        DhtServiceAction::ReconfigureFailed {
            warning: "bind failed".to_string(),
            runtime_reset: false,
        },
    );
    replay.service(
        "successful-reconfigure",
        DhtServiceAction::ReconfigureSucceeded {
            config: disabled_service_config(),
            warning: None,
        },
    );

    replay.demand(
        "register-metadata",
        register_action(info_hash, metadata_demand(), replay.now),
    );
    replay.demand(
        "update-metrics",
        DhtDemandCommandAction::UpdateMetrics {
            info_hash,
            metrics: DhtDemandMetrics {
                accepting_new_peers: true,
                total_pieces: 20,
                completed_pieces: 4,
                connected_peers: 0,
                ..Default::default()
            },
        },
    );
    replay.demand(
        "peers-received",
        DhtDemandCommandAction::PeersReceived {
            info_hash,
            peers: vec![peer("127.0.0.1:4101"), peer("127.0.0.2:4102")],
        },
    );
    replay.advance(Duration::from_millis(500));
    replay.demand(
        "lookup-finished",
        DhtDemandCommandAction::LookupFinished {
            info_hash,
            slice_class: DemandSliceClass::AwaitingMetadata,
            total_peers: 2,
            unique_peers: 2,
            now: replay.now,
        },
    );
    replay.demand(
        "update-demand",
        DhtDemandCommandAction::Update {
            info_hash,
            demand: routine_demand(3),
            now: replay.now,
        },
    );
    replay.demand(
        "unregister",
        DhtDemandCommandAction::Unregister {
            info_hash,
            subscriber_id: 1,
            now: replay.now,
        },
    );
    replay.demand(
        "register-no-peer",
        register_action(hash_index(202), no_peer_demand(), replay.now),
    );

    let expected = r#"
request-reconfigure: effects=[build-runtime:backendInternalPrototype:port6881:bootstrap1] state=[service{backendDisabled:port0:gen0:warn-};subs{};entries{}]
failed-reconfigure: effects=[publish-status] state=[service{backendDisabled:port0:gen0:warnbind failed};subs{};entries{}]
successful-reconfigure: effects=[reset-planner,publish-status,start-due] state=[service{backendDisabled:port0:gen1:warn-};subs{};entries{}]
register-metadata: effects=[register-response:Some(1),subscriber[registered:000000c9:AwaitingMetadata:sub1],planner[],start-due] state=[service{backendDisabled:port0:gen1:warn-};subs{000000c9:1};entries{000000c9:AwaitingMetadata:sub1:infalse:next0:retry0}]
update-metrics: effects=[planner[]] state=[service{backendDisabled:port0:gen1:warn-};subs{000000c9:1};entries{000000c9:AwaitingMetadata:sub1:infalse:next0:retry0}]
peers-received: effects=[planner[],subscriber[deliver:000000c9:peers2:subs1]] state=[service{backendDisabled:port0:gen1:warn-};subs{000000c9:1};entries{000000c9:AwaitingMetadata:sub1:infalse:next0:retry0}]
lookup-finished: effects=[planner[finished:000000c9:AwaitingMetadata:total2:unique2],start-due] state=[service{backendDisabled:port0:gen1:warn-};subs{000000c9:1};entries{000000c9:AwaitingMetadata:sub1:infalse:next1500:retry0}]
update-demand: effects=[planner[],start-due] state=[service{backendDisabled:port0:gen1:warn-};subs{000000c9:1};entries{000000c9:RoutineRefresh:sub1:infalse:next1500:retry0}]
unregister: effects=[subscriber[removed:000000c9],planner[]] state=[service{backendDisabled:port0:gen1:warn-};subs{};entries{}]
register-no-peer: effects=[register-response:Some(2),subscriber[registered:000000ca:NoConnectedPeers:sub2],planner[],start-due] state=[service{backendDisabled:port0:gen1:warn-};subs{000000ca:1};entries{000000ca:NoConnectedPeers:sub1:infalse:next500:retry0}]
"#
    .trim();
    let rendered = replay.render();
    assert_eq!(rendered, expected);
}
