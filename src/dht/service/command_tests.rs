use super::test_support::*;
use super::*;

#[test]
fn dht_runtime_command_model_routes_start_get_peers_and_announce() {
    let info_hash = hash_index(44);
    let (lookup_response_tx, _lookup_response_rx) = oneshot::channel();

    let mut reduction = DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::StartGetPeers {
        info_hash,
        response_tx: lookup_response_tx,
    });

    assert_eq!(reduction.effects.len(), 1);
    match reduction.effects.pop().expect("start get peers effect") {
        DhtRuntimeCommandEffect::StartGetPeers {
            info_hash: effect_hash,
            ..
        } => assert_eq!(effect_hash, info_hash),
        _ => panic!("expected start get peers effect"),
    }

    let (announce_response_tx, _announce_response_rx) = oneshot::channel();
    let mut reduction = DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::AnnouncePeer {
        info_hash,
        port: Some(6881),
        response_tx: announce_response_tx,
    });

    assert_eq!(reduction.effects.len(), 1);
    match reduction.effects.pop().expect("announce effect") {
        DhtRuntimeCommandEffect::AnnouncePeer {
            info_hash: effect_hash,
            port,
            ..
        } => {
            assert_eq!(effect_hash, info_hash);
            assert_eq!(port, Some(6881));
        }
        _ => panic!("expected announce peer effect"),
    }
}
#[test]
fn dht_runtime_command_model_routes_family_attach_and_cancel() {
    let info_hash = hash_index(45);
    let (merged_tx, _merged_rx) = mpsc::unbounded_channel();
    let lookup_ids = Arc::new(StdMutex::new(Vec::new()));
    let expected_lookup_ids = lookup_ids.clone();
    let first_batch_seen = Arc::new(AtomicBool::new(false));
    let expected_first_batch_seen = first_batch_seen.clone();
    let accepting_families = Arc::new(AtomicBool::new(true));
    let expected_accepting_families = accepting_families.clone();

    let mut reduction = DhtRuntimeCommandModel::update(
        DhtRuntimeCommandAction::StartGetPeersFamily(DhtRuntimeLookupFamilyRequest {
            info_hash,
            family: AddressFamily::Ipv6,
            slice_class: DemandSliceClass::AwaitingMetadata,
            record_metrics: true,
            merged_tx,
            lookup_ids,
            first_batch_seen,
            accepting_families,
        }),
    );

    assert_eq!(reduction.effects.len(), 1);
    match reduction.effects.pop().expect("attach family effect") {
        DhtRuntimeCommandEffect::AttachLookupFamily(request) => {
            assert_eq!(request.info_hash, info_hash);
            assert_eq!(request.family, AddressFamily::Ipv6);
            assert_eq!(request.slice_class, DemandSliceClass::AwaitingMetadata);
            assert!(request.record_metrics);
            assert!(Arc::ptr_eq(&request.lookup_ids, &expected_lookup_ids));
            assert!(Arc::ptr_eq(
                &request.first_batch_seen,
                &expected_first_batch_seen
            ));
            assert!(Arc::ptr_eq(
                &request.accepting_families,
                &expected_accepting_families
            ));
        }
        _ => panic!("expected attach lookup family effect"),
    }

    let mut reduction = DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::CancelLookups {
        lookup_ids: vec![LookupId(7), LookupId(9)],
    });

    assert_eq!(reduction.effects.len(), 1);
    match reduction.effects.pop().expect("cancel effect") {
        DhtRuntimeCommandEffect::CancelLookups { lookup_ids } => {
            assert_eq!(lookup_ids, vec![LookupId(7), LookupId(9)]);
        }
        _ => panic!("expected cancel lookups effect"),
    }
}
#[test]
fn dht_runtime_command_model_routes_planner_work_with_start_due_followup() {
    let info_hash = hash_index(46);
    let lookup_ids = Arc::new(StdMutex::new(vec![LookupId(11)]));
    let expected_lookup_ids = lookup_ids.clone();
    let unique_peers = HashSet::from([peer("127.0.0.1:6881")]);

    let reduction = DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::ParkDemandLookups {
        info_hash,
        slice_class: DemandSliceClass::NoConnectedPeers,
        stop_reason: DemandSliceStopReason::WallTime,
        total_peers: 3,
        unique_peers: unique_peers.clone(),
        lookup_ids,
    });

    assert_eq!(reduction.effects.len(), 2);
    match &reduction.effects[0] {
        DhtRuntimeCommandEffect::ParkDemandLookups {
            info_hash: effect_hash,
            slice_class,
            stop_reason,
            total_peers,
            unique_peers: effect_unique_peers,
            lookup_ids,
        } => {
            assert_eq!(*effect_hash, info_hash);
            assert_eq!(*slice_class, DemandSliceClass::NoConnectedPeers);
            assert_eq!(*stop_reason, DemandSliceStopReason::WallTime);
            assert_eq!(*total_peers, 3);
            assert_eq!(effect_unique_peers, &unique_peers);
            assert!(Arc::ptr_eq(lookup_ids, &expected_lookup_ids));
        }
        _ => panic!("expected park demand lookups effect"),
    }
    assert!(matches!(
        reduction.effects[1],
        DhtRuntimeCommandEffect::StartDueDemands
    ));

    let reduction =
        DhtRuntimeCommandModel::update(DhtRuntimeCommandAction::FinalizeDrainedDemandLookups {
            info_hash,
        });
    assert_eq!(reduction.effects.len(), 2);
    assert!(matches!(
        reduction.effects[0],
        DhtRuntimeCommandEffect::FinalizeDrainedDemandLookups { info_hash: effect_hash }
            if effect_hash == info_hash
    ));
    assert!(matches!(
        reduction.effects[1],
        DhtRuntimeCommandEffect::StartDueDemands
    ));
}
