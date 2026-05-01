use super::monitor::*;

#[test]
fn action_effect_snapshot_records_reduction_shape() {
    let snapshot =
        action_effect_snapshot("service", "reconfigure_requested", vec!["build_runtime"]);

    assert_eq!(snapshot.domain, "service");
    assert_eq!(snapshot.action, "reconfigure_requested");
    assert_eq!(snapshot.effect_count, 1);
    assert_eq!(snapshot.effects, vec!["build_runtime"]);
}
