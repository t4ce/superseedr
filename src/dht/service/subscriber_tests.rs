use super::test_support::*;
use super::*;

#[test]
fn demand_subscriber_registry_registers_and_unregisters_once() {
    let mut registry = DemandSubscriberRegistry::new();
    let info_hash = hash_index(42);
    let demand = DhtDemandState {
        awaiting_metadata: true,
        connected_peers: 0,
    };
    let (subscriber_tx, _subscriber_rx) = mpsc::unbounded_channel();

    let registered = registry.update(DemandSubscriberAction::Register {
        info_hash,
        demand,
        subscriber_tx,
    });

    assert_eq!(registered.subscriber_id, Some(1));
    assert_eq!(registry.subscriber_count(info_hash), 1);
    assert_eq!(registered.effects.len(), 1);
    match &registered.effects[0] {
        DemandSubscriberEffect::Registered {
            info_hash: registered_hash,
            demand: registered_demand,
            subscriber_id,
        } => {
            assert_eq!(*registered_hash, info_hash);
            assert_eq!(*registered_demand, demand);
            assert_eq!(*subscriber_id, 1);
        }
        _ => panic!("expected registered effect"),
    }

    let removed = registry.update(DemandSubscriberAction::Unregister {
        info_hash,
        subscriber_id: 1,
    });

    assert_eq!(registry.subscriber_count(info_hash), 0);
    assert_eq!(removed.effects.len(), 1);
    match &removed.effects[0] {
        DemandSubscriberEffect::SubscriberRemoved {
            info_hash: removed_hash,
        } => assert_eq!(*removed_hash, info_hash),
        _ => panic!("expected subscriber removed effect"),
    }

    let duplicate = registry.update(DemandSubscriberAction::Unregister {
        info_hash,
        subscriber_id: 1,
    });
    assert!(duplicate.effects.is_empty());
}
#[test]
fn demand_subscriber_registry_delivery_prunes_closed_subscribers() {
    let mut registry = DemandSubscriberRegistry::new();
    let info_hash = hash_index(43);
    let demand = DhtDemandState {
        awaiting_metadata: false,
        connected_peers: 0,
    };
    let (live_tx, mut live_rx) = mpsc::unbounded_channel();
    let (dead_tx, dead_rx) = mpsc::unbounded_channel();
    drop(dead_rx);

    let live_id = registry
        .update(DemandSubscriberAction::Register {
            info_hash,
            demand,
            subscriber_tx: live_tx,
        })
        .subscriber_id
        .expect("live subscriber id");
    let _dead_id = registry
        .update(DemandSubscriberAction::Register {
            info_hash,
            demand,
            subscriber_tx: dead_tx,
        })
        .subscriber_id
        .expect("dead subscriber id");
    assert_eq!(registry.subscriber_count(info_hash), 2);

    let peers = vec![peer("127.0.0.1:6881"), peer("127.0.0.1:6882")];
    let delivery = registry.update(DemandSubscriberAction::DeliverPeers {
        info_hash,
        peers: peers.clone(),
    });
    assert_eq!(delivery.effects.len(), 1);
    let DemandSubscriberEffect::DeliverPeers {
        info_hash: delivered_hash,
        peers: delivered_peers,
        deliveries,
    } = delivery
        .effects
        .into_iter()
        .next()
        .expect("delivery effect")
    else {
        panic!("expected peer delivery effect");
    };
    assert_eq!(delivered_hash, info_hash);
    assert_eq!(delivered_peers, peers);
    assert_eq!(deliveries.len(), 2);

    let dead_subscribers = deliveries
        .into_iter()
        .filter_map(|delivery| {
            delivery
                .subscriber_tx
                .send(delivered_peers.clone())
                .is_err()
                .then_some(delivery.subscriber_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(live_rx.try_recv().expect("live peers delivered"), peers);
    assert_eq!(dead_subscribers.len(), 1);

    let pruned = registry.update(DemandSubscriberAction::PruneDeadSubscribers {
        info_hash,
        subscriber_ids: dead_subscribers,
    });
    assert_eq!(registry.subscriber_count(info_hash), 1);
    assert_eq!(pruned.effects.len(), 1);
    assert!(matches!(
        pruned.effects.as_slice(),
        [DemandSubscriberEffect::SubscriberRemoved {
            info_hash: removed_hash
        }] if *removed_hash == info_hash
    ));

    let remaining = registry.update(DemandSubscriberAction::DeliverPeers { info_hash, peers });
    let Some(DemandSubscriberEffect::DeliverPeers { deliveries, .. }) =
        remaining.effects.into_iter().next()
    else {
        panic!("expected remaining delivery effect");
    };
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].subscriber_id, live_id);
}
