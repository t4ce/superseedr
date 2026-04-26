use super::test_support::*;
use super::*;

#[test]
fn dht_lifecycle_model_startup_bootstrap_runs_only_when_due_and_idle() {
    let now = Instant::now();
    let due = now - Duration::from_millis(1);

    let reduction = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
        now,
        due,
        active_user_lookup_count: 0,
    });
    assert_eq!(
        reduction.effects,
        vec![DhtLifecycleEffect::RunStartupBootstrap]
    );

    let not_due = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
        now,
        due: now + Duration::from_millis(1),
        active_user_lookup_count: 0,
    });
    assert!(not_due.effects.is_empty());

    let busy = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapDue {
        now,
        due,
        active_user_lookup_count: 1,
    });
    assert!(busy.effects.is_empty());
}
#[test]
fn dht_lifecycle_model_startup_bootstrap_result_updates_retry_state() {
    let retry_at = Instant::now() + DHT_STARTUP_BOOTSTRAP_DELAY;

    let failed = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapFailed {
        warning: "DHT startup bootstrap failed: route lookup failed".to_string(),
        retry_at,
    });
    assert_eq!(
        failed.effects,
        vec![
            DhtLifecycleEffect::RecordRuntimeWarning {
                warning: "DHT startup bootstrap failed: route lookup failed".to_string(),
                publish_status: false,
            },
            DhtLifecycleEffect::SetStartupBootstrapDue(retry_at),
        ]
    );

    let succeeded = DhtLifecycleModel::update(DhtLifecycleAction::StartupBootstrapSucceeded);
    assert_eq!(
        succeeded.effects,
        vec![DhtLifecycleEffect::ClearStartupBootstrapDue]
    );
}
#[test]
fn dht_lifecycle_model_maintenance_only_runs_when_runtime_idle() {
    let no_runtime = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
        active_user_lookup_count: None,
    });
    assert!(no_runtime.effects.is_empty());

    let busy = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
        active_user_lookup_count: Some(2),
    });
    assert!(busy.effects.is_empty());

    let idle = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceTick {
        active_user_lookup_count: Some(0),
    });
    assert_eq!(idle.effects, vec![DhtLifecycleEffect::RunMaintenance]);
}
#[test]
fn dht_lifecycle_model_health_tick_publishes_expires_and_saves() {
    let reduction = DhtLifecycleModel::update(DhtLifecycleAction::HealthTick);

    assert_eq!(
        reduction.effects,
        vec![
            DhtLifecycleEffect::PublishStatus,
            DhtLifecycleEffect::ExpireRecentUniquePeers,
            DhtLifecycleEffect::SaveRuntimeState,
        ]
    );
}
#[test]
fn dht_lifecycle_model_runtime_failures_publish_warning_status() {
    let maintenance = DhtLifecycleModel::update(DhtLifecycleAction::MaintenanceFailed {
        warning: "DHT maintenance failed: maintenance error".to_string(),
    });
    assert_eq!(
        maintenance.effects,
        vec![DhtLifecycleEffect::RecordRuntimeWarning {
            warning: "DHT maintenance failed: maintenance error".to_string(),
            publish_status: true,
        }]
    );

    let runtime_step = DhtLifecycleModel::update(DhtLifecycleAction::RuntimeStepFailed {
        warning: "DHT runtime step failed: step error".to_string(),
    });
    assert_eq!(
        runtime_step.effects,
        vec![DhtLifecycleEffect::RecordRuntimeWarning {
            warning: "DHT runtime step failed: step error".to_string(),
            publish_status: true,
        }]
    );
}
#[test]
fn dht_lifecycle_model_shutdown_saves_runtime_state() {
    let reduction = DhtLifecycleModel::update(DhtLifecycleAction::Shutdown);

    assert_eq!(
        reduction.effects,
        vec![DhtLifecycleEffect::SaveRuntimeState]
    );
}
