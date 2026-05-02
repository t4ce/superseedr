use super::test_support::*;
use super::*;
use tokio::sync::watch;

#[test]
fn recent_unique_peers_dedupes_and_expires_entries() {
    let start = Instant::now();
    let mut recent = RecentUniquePeers::new(Duration::from_secs(30));
    let peer_a = peer("127.0.0.1:1000");
    let peer_b = peer("127.0.0.2:1000");

    recent.record_batch(start, &[peer_a, peer_a, peer_b]);
    assert_eq!(recent.unique_count(start), 2);

    let refresh = start + Duration::from_secs(10);
    recent.record_batch(refresh, &[peer_a]);
    assert_eq!(recent.unique_count(refresh), 2);

    assert_eq!(recent.unique_count(start + Duration::from_secs(31)), 1);
    assert_eq!(recent.unique_count(start + Duration::from_secs(41)), 0);
}
#[test]
fn literal_bootstrap_summary_counts_literal_socket_addresses() {
    let summary = literal_bootstrap_summary(&[
        "127.0.0.1:6881".to_string(),
        "[::1]:6881".to_string(),
        "node.example.invalid:6881".to_string(),
    ]);

    assert_eq!(summary.total, 3);
    assert_eq!(summary.ipv4, 1);
    assert_eq!(summary.ipv6, 1);
}
#[test]
fn build_status_without_runtime_reports_disabled_state_and_bootstrap() {
    let bootstrap = BootstrapSummary {
        total: 3,
        ipv4: 2,
        ipv6: 1,
    };
    let status = build_status(
        None,
        DhtBackendKind::Disabled,
        DhtBackendKind::InternalPrototype,
        Some("test warning".to_string()),
        7,
        bootstrap,
    );

    assert_eq!(status.generation, 7);
    assert_eq!(status.warning.as_deref(), Some("test warning"));
    assert_eq!(status.health.backend, DhtBackendKind::Disabled);
    assert_eq!(
        status.health.preferred_backend,
        Some(DhtBackendKind::InternalPrototype)
    );
    assert!(!status.health.enabled);
    assert_eq!(status.health.exported_bootstrap_nodes, 3);
    assert_eq!(status.health.ipv4_bootstrap_nodes, 2);
    assert_eq!(status.health.ipv6_bootstrap_nodes, 1);
    assert_eq!(status.health.bound_family_count, 0);
    assert_eq!(status.health.inflight_lookups, 0);
}

#[test]
fn publish_status_without_runtime_preserves_configured_bootstrap() {
    let bootstrap = BootstrapSummary {
        total: 3,
        ipv4: 1,
        ipv6: 1,
    };
    let (status_tx, status_rx) = watch::channel(DhtStatus::default());

    publish_status(
        &status_tx,
        None,
        Some("runtime unavailable".to_string()),
        11,
        DhtBackendKind::InternalPrototype,
        bootstrap,
    );

    let status = status_rx.borrow().clone();
    assert_eq!(status.generation, 11);
    assert_eq!(status.warning.as_deref(), Some("runtime unavailable"));
    assert_eq!(status.health.backend, DhtBackendKind::Disabled);
    assert_eq!(
        status.health.preferred_backend,
        Some(DhtBackendKind::InternalPrototype)
    );
    assert_eq!(status.health.exported_bootstrap_nodes, 3);
    assert_eq!(status.health.ipv4_bootstrap_nodes, 1);
    assert_eq!(status.health.ipv6_bootstrap_nodes, 1);
}

#[test]
fn build_wave_telemetry_without_runtime_preserves_recent_unique_count() {
    let telemetry = build_wave_telemetry(None, 12, 6);

    assert_eq!(telemetry.unique_peers_found_last_10s, 12);
    assert_eq!(telemetry.active_lookups, 0);
    assert_eq!(telemetry.active_user_lookups, 0);
    assert_eq!(telemetry.inflight_ipv4_queries, 0);
    assert_eq!(telemetry.inflight_ipv6_queries, 0);
    assert_eq!(telemetry.demand_power_multiplier, 3);
    assert_eq!(telemetry.demand_power_scale_halves, 6);
}
