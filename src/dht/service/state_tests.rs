use super::test_support::*;
use super::*;

#[test]
fn dht_service_model_reconfigure_success_updates_state_and_emits_followups() {
    let initial = DhtServiceConfig {
        port: 6881,
        bootstrap_nodes: vec!["198.51.100.10:6881".to_string()],
        preferred_backend: DhtBackendKind::InternalPrototype,
        force_internal_failure: false,
    };
    let next = DhtServiceConfig {
        port: 6882,
        bootstrap_nodes: vec!["203.0.113.20:6881".to_string()],
        preferred_backend: DhtBackendKind::Disabled,
        force_internal_failure: false,
    };
    let mut model = DhtServiceModel::new(initial, 7, Some("old warning".to_string()));

    let reduction = model.update(DhtServiceAction::ReconfigureSucceeded {
        config: next.clone(),
        warning: None,
    });

    assert_eq!(model.config(), &next);
    assert_eq!(model.generation(), 8);
    assert_eq!(model.warning_owned(), None);
    assert_eq!(
        reduction.effects,
        vec![
            DhtServiceEffect::ResetDemandPlanner,
            DhtServiceEffect::PublishStatus,
            DhtServiceEffect::StartDueDemands,
        ]
    );
}
#[test]
fn dht_service_model_reconfigure_failure_preserves_config_and_generation() {
    let initial = DhtServiceConfig {
        port: 6881,
        bootstrap_nodes: vec!["198.51.100.10:6881".to_string()],
        preferred_backend: DhtBackendKind::InternalPrototype,
        force_internal_failure: false,
    };
    let mut model = DhtServiceModel::new(initial.clone(), 3, None);

    let reduction = model.update(DhtServiceAction::ReconfigureFailed {
        warning: "runtime unavailable".to_string(),
    });

    assert_eq!(model.config(), &initial);
    assert_eq!(model.generation(), 3);
    assert_eq!(
        model.warning_owned().as_deref(),
        Some("runtime unavailable")
    );
    assert_eq!(
        reduction.effects,
        vec![
            DhtServiceEffect::ResetDemandPlanner,
            DhtServiceEffect::PublishStatus,
            DhtServiceEffect::StartDueDemands,
        ]
    );
}
#[test]
fn dht_service_model_runtime_warning_only_publishes_status() {
    let config = disabled_service_config();
    let mut model = DhtServiceModel::new(config.clone(), 11, None);

    let reduction = model.update(DhtServiceAction::RuntimeWarning {
        warning: "maintenance failed".to_string(),
    });

    assert_eq!(model.config(), &config);
    assert_eq!(model.generation(), 11);
    assert_eq!(model.warning_owned().as_deref(), Some("maintenance failed"));
    assert_eq!(reduction.effects, vec![DhtServiceEffect::PublishStatus]);
}
#[test]
fn dht_service_state_initializes_helper_models() {
    let config = disabled_service_config();
    let mut state = DhtServiceState::new(config.clone(), 42, Some("initial warning".to_string()));

    assert_eq!(state.service.config(), &config);
    assert_eq!(state.service.generation(), 42);
    assert_eq!(
        state.service.warning_owned().as_deref(),
        Some("initial warning")
    );
    assert!(!state.has_draining_demands());
    assert!(state.demand_subscribers.subscribers.is_empty());

    state.record_recent_peers(&[peer("198.51.100.30:6881")]);
    assert_eq!(state.recent_unique_peers.unique_count(Instant::now()), 1);
    state.expire_recent_peers();
}
