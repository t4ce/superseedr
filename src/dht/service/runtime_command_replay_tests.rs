use super::test_support::*;
use super::*;

#[derive(Default)]
struct RuntimeCommandReplay {
    transcript: Vec<String>,
}

impl RuntimeCommandReplay {
    fn action(&mut self, label: &'static str, action: DhtRuntimeCommandAction) {
        let reduction = DhtRuntimeCommandModel::update(action);
        self.transcript.push(format!(
            "{label}: effects=[{}]",
            runtime_effect_labels(&reduction.effects).join(","),
        ));
    }

    fn command(&mut self, label: &'static str, command: DhtCommand) {
        let Some(reduction) = DhtRuntimeCommandModel::update_command(command) else {
            self.transcript
                .push(format!("{label}: effects=[not-runtime]"));
            return;
        };
        self.transcript.push(format!(
            "{label}: effects=[{}]",
            runtime_effect_labels(&reduction.effects).join(","),
        ));
    }

    fn render(&self) -> String {
        self.transcript.join("\n")
    }
}

fn runtime_effect_labels(effects: &[DhtRuntimeCommandEffect]) -> Vec<String> {
    effects
        .iter()
        .map(|effect| match effect {
            DhtRuntimeCommandEffect::StartGetPeers { info_hash, .. } => {
                format!("start-get-peers:{}", short_info_hash(*info_hash))
            }
            DhtRuntimeCommandEffect::AttachLookupFamily(request) => format!(
                "attach-family:{}:{:?}:{:?}:metrics{}:ids{}:first{}:accept{}",
                short_info_hash(request.info_hash),
                request.family,
                request.slice_class,
                request.record_metrics,
                request
                    .lookup_ids
                    .lock()
                    .expect("test lookup id lock")
                    .len(),
                request.first_batch_seen.load(Ordering::Acquire),
                request.accepting_families.load(Ordering::Acquire),
            ),
            DhtRuntimeCommandEffect::CancelLookups { lookup_ids } => format!(
                "cancel:{}",
                lookup_ids
                    .iter()
                    .map(|lookup_id| lookup_id.0.to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            DhtRuntimeCommandEffect::ParkDemandLookups {
                info_hash,
                slice_class,
                stop_reason,
                total_peers,
                unique_peers,
                lookup_ids,
            } => format!(
                "park:{}:{:?}:{:?}:total{}:unique{}:ids{}",
                short_info_hash(*info_hash),
                slice_class,
                stop_reason,
                total_peers,
                unique_peers.len(),
                lookup_ids.lock().expect("test lookup id lock").len(),
            ),
            DhtRuntimeCommandEffect::FinalizeDrainedDemandLookups { info_hash } => {
                format!("finalize-drain:{}", short_info_hash(*info_hash))
            }
            DhtRuntimeCommandEffect::AnnouncePeer {
                info_hash, port, ..
            } => {
                format!("announce:{}:{port:?}", short_info_hash(*info_hash))
            }
            DhtRuntimeCommandEffect::StartDueDemands => "start-due".to_string(),
        })
        .collect()
}

fn family_request(
    info_hash: InfoHash,
    family: AddressFamily,
    slice_class: DemandSliceClass,
    lookup_ids: Vec<LookupId>,
    first_batch_seen: bool,
    accepting_families: bool,
) -> DhtRuntimeLookupFamilyRequest {
    let (merged_tx, _merged_rx) = mpsc::unbounded_channel();
    DhtRuntimeLookupFamilyRequest {
        info_hash,
        family,
        slice_class,
        record_metrics: true,
        merged_tx,
        lookup_ids: Arc::new(StdMutex::new(lookup_ids)),
        first_batch_seen: Arc::new(AtomicBool::new(first_batch_seen)),
        accepting_families: Arc::new(AtomicBool::new(accepting_families)),
    }
}

#[test]
fn dht_runtime_command_replays_effect_shape_deterministically() {
    let mut replay = RuntimeCommandReplay::default();
    let primary_hash = hash_index(301);
    let demand_hash = hash_index(302);

    let (start_tx, _start_rx) = oneshot::channel();
    replay.command(
        "command-start-get-peers",
        DhtCommand::StartGetPeers {
            info_hash: primary_hash,
            response_tx: start_tx,
        },
    );

    let (subscriber_tx, _subscriber_rx) = mpsc::unbounded_channel();
    let (response_tx, _response_rx) = oneshot::channel();
    replay.command(
        "command-register-demand",
        DhtCommand::RegisterDemand {
            info_hash: primary_hash,
            demand: DhtDemandState {
                awaiting_metadata: false,
                connected_peers: 0,
            },
            subscriber_tx,
            response_tx,
        },
    );

    replay.action(
        "attach-ipv6-family",
        DhtRuntimeCommandAction::StartGetPeersFamily(family_request(
            demand_hash,
            AddressFamily::Ipv6,
            DemandSliceClass::AwaitingMetadata,
            vec![LookupId(11), LookupId(12)],
            true,
            true,
        )),
    );
    replay.action(
        "cancel-lookups",
        DhtRuntimeCommandAction::CancelLookups {
            lookup_ids: vec![LookupId(11), LookupId(12)],
        },
    );
    replay.action(
        "park-demand-lookups",
        DhtRuntimeCommandAction::ParkDemandLookups {
            info_hash: demand_hash,
            slice_class: DemandSliceClass::NoConnectedPeers,
            stop_reason: DemandSliceStopReason::WallTime,
            total_peers: 5,
            unique_peers: synthetic_peers(42, 3),
            lookup_ids: Arc::new(StdMutex::new(vec![LookupId(21), LookupId(22)])),
        },
    );
    replay.action(
        "finalize-drained",
        DhtRuntimeCommandAction::FinalizeDrainedDemandLookups {
            info_hash: demand_hash,
        },
    );

    let (announce_tx, _announce_rx) = oneshot::channel();
    replay.action(
        "announce-peer",
        DhtRuntimeCommandAction::AnnouncePeer {
            info_hash: primary_hash,
            port: Some(6881),
            response_tx: announce_tx,
        },
    );

    let expected = r#"
command-start-get-peers: effects=[start-get-peers:0000012d]
command-register-demand: effects=[not-runtime]
attach-ipv6-family: effects=[attach-family:0000012e:Ipv6:AwaitingMetadata:metricstrue:ids2:firsttrue:accepttrue]
cancel-lookups: effects=[cancel:11|12]
park-demand-lookups: effects=[park:0000012e:NoConnectedPeers:WallTime:total5:unique3:ids2,start-due]
finalize-drained: effects=[finalize-drain:0000012e,start-due]
announce-peer: effects=[announce:0000012d:Some(6881)]
"#
    .trim();
    let rendered = replay.render();
    assert_eq!(rendered, expected);
}
