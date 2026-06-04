from __future__ import annotations

from pathlib import Path

from integration_tests.libtorrent_lab.run import (
    CLIENT_LIBTORRENT,
    CLIENT_SUPERSEEDR,
    LabScenario,
    NetworkImpairment,
    _client_payload_path,
    _collect_superseedr_logs,
    _matrix_markdown,
    _netem_command,
    _probe_superseedr_health,
    _profile_for_name,
    _profile_markdown,
    _readiness_for_name,
    _readiness_markdown,
    _probe_tracker_announces,
    _project_name,
    _run_behavior_probes,
    _scenario_names_for_matrix,
    _summarize_libtorrent_events_for_role,
    _summarize_superseedr_logs,
    _summarize_tracker_log,
    _superseedr_seed_is_ready,
    _superseedr_download_path,
    _validate_transfer_accounting,
    _validate_superseedr_transport_observations,
    _validate_download,
    _validate_download_set,
)


def test_project_name_is_compose_safe() -> None:
    assert _project_name("libtorrent_lab_basic_ul_dl_2026-05-17") == "ltlablibtorrentlabbasiculdl20260517"


def test_load_basic_scenario() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/basic_ul_dl.json")
    )
    assert scenario.name == "basic_ul_dl"
    assert scenario.seed_client == CLIENT_LIBTORRENT
    assert scenario.leech_client == CLIENT_LIBTORRENT
    assert scenario.mode == "v1"
    assert scenario.seed_listen_port != scenario.leech_listen_port
    assert scenario.libtorrent_seed_count == 1
    assert scenario.libtorrent_leech_count == 1
    assert scenario.libtorrent_settings["enable_dht"] is False


def test_load_superseedr_to_libtorrent_scenario() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/superseedr_to_libtorrent.json")
    )
    assert scenario.seed_client == CLIENT_SUPERSEEDR
    assert scenario.leech_client == CLIENT_LIBTORRENT
    assert scenario.superseedr_peer_transport == "tcp"
    assert scenario.libtorrent_settings["enable_incoming_utp"] is False


def test_load_utp_only_scenarios_disable_tcp() -> None:
    for name in ("superseedr_utp_to_libtorrent", "libtorrent_utp_to_superseedr"):
        scenario = LabScenario.from_file(
            Path(f"integration_tests/libtorrent_lab/scenarios/{name}.json")
        )

        assert scenario.superseedr_peer_transport == "utp"
        assert scenario.libtorrent_settings["enable_incoming_tcp"] is False
        assert scenario.libtorrent_settings["enable_outgoing_tcp"] is False
        assert scenario.libtorrent_settings["enable_incoming_utp"] is True
        assert scenario.libtorrent_settings["enable_outgoing_utp"] is True


def test_superseedr_payload_path_preserves_fixture_bucket(tmp_path: Path) -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/libtorrent_to_superseedr.json")
    )
    assert _client_payload_path(CLIENT_SUPERSEEDR, tmp_path, scenario) == (
        tmp_path / "v1" / "single" / "single_16k.bin"
    )
    assert _superseedr_download_path("leech", scenario) == (
        "/superseedr-data/leech/v1/single"
    )


def test_directory_payload_paths_use_torrent_bucket(tmp_path: Path) -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/libtorrent_to_superseedr_v1_nested.json")
    )

    assert _client_payload_path(CLIENT_LIBTORRENT, tmp_path, scenario) == (
        tmp_path / "nested"
    )
    assert _client_payload_path(CLIENT_SUPERSEEDR, tmp_path, scenario) == (
        tmp_path / "v1" / "nested"
    )
    assert _superseedr_download_path("seed", scenario) == (
        "/superseedr-data/seed/v1/nested"
    )


def test_load_directory_and_mode_scenarios() -> None:
    expected = {
        "superseedr_to_libtorrent_v1_multi_file": ("v1", "multi_file"),
        "libtorrent_to_superseedr_v1_nested": ("v1", "nested"),
        "superseedr_to_libtorrent_v2_multi_file": ("v2", "multi_file"),
        "libtorrent_to_superseedr_hybrid_nested": ("hybrid", "nested"),
    }

    for name, (mode, payload) in expected.items():
        scenario = LabScenario.from_file(
            Path(f"integration_tests/libtorrent_lab/scenarios/{name}.json")
        )
        assert scenario.mode == mode
        assert scenario.payload == payload
        assert scenario.download_name == payload
        assert scenario.superseedr_peer_transport == "tcp"


def test_load_fanout_scenarios() -> None:
    superseedr_seed = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/superseedr_to_libtorrent_tcp_fanout.json")
    )
    libtorrent_seed = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/libtorrent_to_superseedr_tcp_fanout.json")
    )

    assert superseedr_seed.seed_client == CLIENT_SUPERSEEDR
    assert superseedr_seed.leech_client == CLIENT_LIBTORRENT
    assert superseedr_seed.libtorrent_seed_count == 1
    assert superseedr_seed.libtorrent_leech_count == 3
    assert libtorrent_seed.seed_client == CLIENT_LIBTORRENT
    assert libtorrent_seed.leech_client == CLIENT_SUPERSEEDR
    assert libtorrent_seed.libtorrent_seed_count == 3
    assert libtorrent_seed.libtorrent_leech_count == 1


def test_load_config_scenarios() -> None:
    tcp_only = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/basic_ul_dl_tcp_only.json")
    )
    utp_only = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/basic_ul_dl_utp_only.json")
    )
    dual_stack = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/superseedr_all_to_libtorrent_dual_stack.json")
    )
    dht_lsd = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/basic_ul_dl_dht_lsd_enabled.json")
    )

    assert tcp_only.libtorrent_settings["enable_incoming_tcp"] is True
    assert tcp_only.libtorrent_settings["enable_incoming_utp"] is False
    assert utp_only.libtorrent_settings["enable_incoming_tcp"] is False
    assert utp_only.libtorrent_settings["enable_incoming_utp"] is True
    assert dual_stack.superseedr_peer_transport == "all"
    assert dual_stack.libtorrent_settings["enable_outgoing_tcp"] is True
    assert dual_stack.libtorrent_settings["enable_outgoing_utp"] is True
    assert dht_lsd.libtorrent_settings["enable_dht"] is True
    assert dht_lsd.libtorrent_settings["enable_lsd"] is True


def test_matrix_scenario_sets_are_stable() -> None:
    assert _scenario_names_for_matrix("smoke") == [
        "basic_ul_dl",
        "superseedr_to_libtorrent",
        "libtorrent_to_superseedr",
    ]
    assert _scenario_names_for_matrix("fanout") == [
        "superseedr_to_libtorrent_tcp_fanout",
        "libtorrent_to_superseedr_tcp_fanout",
    ]
    assert _scenario_names_for_matrix("config") == [
        "basic_ul_dl_tcp_only",
        "basic_ul_dl_utp_only",
        "basic_ul_dl_dht_lsd_enabled",
        "superseedr_all_to_libtorrent_dual_stack",
        "libtorrent_dual_stack_to_superseedr_all",
    ]
    assert _scenario_names_for_matrix("behavior") == [
        "basic_ul_dl_tcp_only",
        "basic_ul_dl_utp_only",
        "superseedr_all_to_libtorrent_dual_stack",
        "libtorrent_dual_stack_to_superseedr_all",
    ]
    assert len(_scenario_names_for_matrix("full")) == 11


def test_profile_presets_are_stable() -> None:
    quick = _profile_for_name("quick")
    premerge = _profile_for_name("premerge")
    stress = _profile_for_name("stress")
    soak = _profile_for_name("soak")

    assert [step.matrix for step in quick.steps] == ["smoke"]
    assert [step.name for step in premerge.steps] == [
        "clean_full",
        "mild_netem_transport",
    ]
    assert premerge.steps[1].network_impairment.enabled() is True
    assert stress.steps[0].repeat == 2
    assert stress.steps[1].matrix == "fanout"
    assert soak.steps[0].repeat > stress.steps[0].repeat


def test_readiness_presets_are_stable() -> None:
    quick = _readiness_for_name("quick")
    release = _readiness_for_name("release")

    assert [step.matrix for step in quick.steps] == ["behavior"]
    assert [step.name for step in release.steps] == [
        "clean_full",
        "focused_config",
        "behavior_probes",
        "impaired_transport",
        "impaired_fanout",
    ]
    assert release.steps[3].network_impairment.enabled() is True
    assert release.steps[4].matrix == "fanout"


def test_netem_command_includes_impairment_knobs() -> None:
    command = _netem_command(
        NetworkImpairment(
            delay_ms=25,
            jitter_ms=5,
            loss_pct=1.5,
            duplicate_pct=0.25,
            corrupt_pct=0.1,
            reorder_pct=2.0,
        )
    )

    assert command == [
        "tc",
        "qdisc",
        "replace",
        "dev",
        "eth0",
        "root",
        "netem",
        "delay",
        "25ms",
        "5ms",
        "loss",
        "1.5%",
        "duplicate",
        "0.25%",
        "corrupt",
        "0.1%",
        "reorder",
        "2%",
        "50%",
    ]


def test_matrix_markdown_summarizes_results() -> None:
    markdown = _matrix_markdown(
        {
            "matrix": "smoke",
            "ok": False,
            "scenario_count": 1,
            "attempt_count": 2,
            "passed_attempts": 1,
            "failed_attempts": 1,
            "repeat_count": 2,
            "duration_secs": 12.5,
            "artifacts_dir": "/tmp/lab",
            "results": [
                {
                    "scenario": "basic_ul_dl",
                    "iteration": 1,
                    "ok": True,
                    "duration_secs": 3.0,
                    "artifacts_dir": "/tmp/lab/one",
                },
                {
                    "scenario": "basic_ul_dl",
                    "iteration": 2,
                    "ok": False,
                    "duration_secs": 4.0,
                    "artifacts_dir": "/tmp/lab/two",
                },
            ],
        }
    )

    assert "Result: FAIL" in markdown
    assert "| basic_ul_dl | 2 | FAIL | PASS | PASS | n/a | 0 | 4.0s | `/tmp/lab/two` |" in markdown


def test_profile_markdown_summarizes_steps() -> None:
    markdown = _profile_markdown(
        {
            "profile": "premerge",
            "description": "Full clean matrix plus a mild impaired transport pass.",
            "ok": True,
            "step_count": 2,
            "completed_steps": 2,
            "attempt_count": 15,
            "passed_attempts": 15,
            "failed_attempts": 0,
            "duration_secs": 42.0,
            "artifacts_dir": "/tmp/profile",
            "steps": [
                {
                    "name": "clean_full",
                    "matrix": "full",
                    "ok": True,
                    "repeat_count": 1,
                    "attempt_count": 11,
                    "failed_attempts": 0,
                    "duration_secs": 30.0,
                    "artifacts_dir": "/tmp/profile/clean",
                    "network_impairment": {"enabled": False},
                },
                {
                    "name": "mild_netem_transport",
                    "matrix": "transport",
                    "ok": True,
                    "repeat_count": 1,
                    "attempt_count": 4,
                    "failed_attempts": 0,
                    "duration_secs": 12.0,
                    "artifacts_dir": "/tmp/profile/netem",
                    "network_impairment": {"enabled": True},
                },
            ],
        }
    )

    assert "Libtorrent Lab Profile: premerge" in markdown
    assert "| clean_full | full | PASS | 1 | 11 | 0 | off | 0 | 0 | 30.0s | `/tmp/profile/clean` |" in markdown
    assert "| mild_netem_transport | transport | PASS | 1 | 4 | 0 | on | 0 | 0 | 12.0s | `/tmp/profile/netem` |" in markdown


def test_readiness_markdown_summarizes_gates() -> None:
    markdown = _readiness_markdown(
        {
            "readiness": "quick",
            "description": "Fast gate.",
            "ok": False,
            "step_count": 1,
            "completed_steps": 1,
            "attempt_count": 4,
            "passed_attempts": 3,
            "failed_attempts": 1,
            "behavior_warning_count": 2,
            "superseedr_error_count": 1,
            "superseedr_warning_count": 1,
            "duration_secs": 18.0,
            "artifacts_dir": "/tmp/readiness",
            "steps": [
                {
                    "name": "behavior",
                    "matrix": "behavior",
                    "ok": False,
                    "repeat_count": 1,
                    "attempt_count": 4,
                    "failed_attempts": 1,
                    "behavior_warning_count": 2,
                    "superseedr_error_count": 1,
                    "superseedr_warning_count": 1,
                    "duration_secs": 18.0,
                    "artifacts_dir": "/tmp/readiness/behavior",
                    "network_impairment": {"enabled": False},
                },
            ],
        }
    )

    assert "uTP Readiness Suite: quick" in markdown
    assert "- Superseedr errors: 1" in markdown
    assert "| behavior | behavior | FAIL | 1 | 4 | 1 | off | 1 | 2 | 18.0s | `/tmp/readiness/behavior` |" in markdown


def test_superseedr_lab_uses_fast_lab_image() -> None:
    compose = Path(
        "integration_tests/libtorrent_lab/docker/docker-compose.libtorrent-lab.yml"
    ).read_text(encoding="utf-8")

    assert "integration_tests/libtorrent_lab/docker/Dockerfile.superseedr" in compose
    assert "dockerfile: Dockerfile" not in compose


def test_lab_images_include_netem_prerequisites() -> None:
    peer_dockerfile = Path(
        "integration_tests/libtorrent_lab/docker/Dockerfile.peer"
    ).read_text(encoding="utf-8")
    superseedr_dockerfile = Path(
        "integration_tests/libtorrent_lab/docker/Dockerfile.superseedr"
    ).read_text(encoding="utf-8")
    compose = Path(
        "integration_tests/libtorrent_lab/docker/docker-compose.libtorrent-lab.yml"
    ).read_text(encoding="utf-8")

    assert "iproute2" in peer_dockerfile
    assert "iproute2" in superseedr_dockerfile
    assert "NET_ADMIN" in compose


def test_superseedr_seed_ready_accepts_completed_data() -> None:
    status = {
        "status": "ok",
        "activity_messages": ["Finished"],
        "complete_torrents": 1,
        "data_available_torrents": 1,
    }

    assert _superseedr_seed_is_ready(status)


def test_utp_only_scenario_requires_superseedr_utp_payload() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/libtorrent_utp_to_superseedr.json")
    )
    seed_status = {"client": CLIENT_LIBTORRENT}
    leech_status = {
        "status": "ok",
        "tcp_peer_count": 0,
        "utp_peer_count": 1,
        "beneficial_utp_peer_count": 1,
    }

    assert _validate_superseedr_transport_observations(
        scenario,
        seed_status,
        leech_status,
    ) == {"ok": True, "issues": []}


def test_utp_only_scenario_rejects_superseedr_tcp_payload() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/superseedr_utp_to_libtorrent.json")
    )
    seed_status = {
        "status": "ok",
        "tcp_peer_count": 1,
        "utp_peer_count": 0,
        "beneficial_utp_peer_count": 0,
    }
    leech_status = {"client": CLIENT_LIBTORRENT}

    report = _validate_superseedr_transport_observations(
        scenario,
        seed_status,
        leech_status,
    )

    assert report["ok"] is False
    assert report["issues"] == [
        "seed Superseedr observed 1 TCP peer(s) in uTP-only mode",
        "seed Superseedr did not observe a uTP peer",
        "seed Superseedr did not move payload over uTP",
    ]


def test_transfer_accounting_requires_completed_counters() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/basic_ul_dl_tcp_only.json")
    )
    report = _validate_transfer_accounting(
        scenario=scenario,
        source_payload_size=16,
        active_seed_count=1,
        active_leech_count=1,
        validation={"ok": True, "issues": []},
        seed_status={
            "client": CLIENT_LIBTORRENT,
            "is_seed": True,
            "total_done": 16,
            "total_upload": 15,
        },
        leech_status={
            "client": CLIENT_LIBTORRENT,
            "is_seed": True,
            "total_done": 16,
            "total_download": 16,
        },
    )

    assert report["ok"] is False
    assert report["issues"] == [
        "seed libtorrent total_upload expected>=16 actual=15",
    ]


def test_transfer_accounting_handles_libtorrent_fanout() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/superseedr_to_libtorrent_tcp_fanout.json")
    )
    report = _validate_transfer_accounting(
        scenario=scenario,
        source_payload_size=16,
        active_seed_count=0,
        active_leech_count=3,
        validation={"ok": True, "issues": []},
        seed_status={
            "client": CLIENT_SUPERSEEDR,
            "status": "ok",
            "complete_torrents": 1,
            "data_available_torrents": 1,
            "session_total_uploaded": 16,
        },
        leech_status={
            "client": CLIENT_LIBTORRENT,
            "complete_peers": 3,
            "total_download": 48,
            "participants": {
                "leech": {"is_seed": True, "total_done": 16},
                "leech_2": {"is_seed": True, "total_done": 16},
                "leech_3": {"is_seed": True, "total_done": 16},
            },
        },
    )

    assert report["ok"] is True
    assert report["issues"] == []


def test_libtorrent_event_summary_extracts_probe_metrics(tmp_path: Path) -> None:
    artifacts = tmp_path / "peer"
    events = artifacts / "events.jsonl"
    events.parent.mkdir()
    events.write_text(
        "\n".join(
            [
                '{"event":"starting","peer_id":"leech"}',
                '{"event":"status","status":{"num_peers":1,"progress":0.5,"total_done":8,"uptime_secs":1.25}}',
                '{"alert":"tracker_error_alert","message":"temporary"}',
                '{"event":"complete","status":{"total_done":16}}',
            ]
        ),
        encoding="utf-8",
    )

    summary = _summarize_libtorrent_events_for_role({1: artifacts}, "leech")
    participant = summary["participants"]["leech"]

    assert participant["event_counts"]["status"] == 1
    assert participant["tracker_error_count"] == 1
    assert participant["first_peer_secs"] == 1.25
    assert participant["first_progress_secs"] == 1.25
    assert participant["completed"] is True


def test_tracker_probe_warns_by_default_and_can_fail() -> None:
    events = {
        "seed": {
            "participants": {
                "seed": {
                    "tracker_error_count": 2,
                    "tracker_reply_count": 0,
                }
            }
        },
        "leech": {"participants": {}},
    }
    tracker = {
        "ok": True,
        "issues": [],
        "announce_count": 2,
        "unique_peer_count": 2,
    }

    warn_only = _probe_tracker_announces(
        events,
        tracker_summary=tracker,
        fail_on_tracker_error=False,
    )
    fail = _probe_tracker_announces(
        events,
        tracker_summary=tracker,
        fail_on_tracker_error=True,
    )

    assert warn_only["ok"] is True
    assert warn_only["warnings"] == ["seed seed saw 2 tracker error alert(s)"]
    assert fail["ok"] is False
    assert fail["issues"] == ["seed seed saw 2 tracker error alert(s)"]


def test_tracker_log_summary_requires_expected_announcers() -> None:
    summary = _summarize_tracker_log(
        "\n".join(
            [
                "tracker-1 | announce info_hash=aa peer_id=seed ip=172.18.0.2 port=1 left=0 peers_out=0",
                "tracker-1 | announce info_hash=aa peer_id=leech ip=172.18.0.3 port=2 left=0 peers_out=1",
            ]
        ),
        expected_peer_count=2,
    )
    missing = _summarize_tracker_log("", expected_peer_count=2)

    assert summary["ok"] is True
    assert summary["announce_count"] == 2
    assert summary["unique_peer_count"] == 2
    assert missing["ok"] is False
    assert missing["issues"] == [
        "tracker log did not record any announces",
        "tracker announced peers expected>=2 actual=0",
    ]


def test_superseedr_log_summary_flags_errors_and_warnings() -> None:
    summary = _summarize_superseedr_logs(
        {
            "superseedr_seed": "\n".join(
                [
                    "2026-05-17T10:00:00Z  INFO superseedr: ready",
                    "2026-05-17T10:00:01Z  WARN superseedr::watcher: transient watch issue",
                    "2026-05-17T10:00:02Z ERROR superseedr::utp: simulated transport fault",
                ]
            )
        }
    )

    assert summary["ok"] is False
    assert summary["error_count"] == 1
    assert summary["warning_count"] == 1
    assert summary["issues"] == ["superseedr_seed emitted 1 Superseedr error log line(s)"]
    assert summary["warnings"] == [
        "superseedr_seed emitted 1 Superseedr warning log line(s)"
    ]


def test_collect_superseedr_logs_reads_runtime_log_files(tmp_path: Path) -> None:
    share_root = tmp_path / "share"
    logs_root = share_root / "logs"
    logs_root.mkdir(parents=True)
    (logs_root / "app.2026-05-17.log").write_text(
        "2026-05-17T10:00:00Z  INFO superseedr: ready\n",
        encoding="utf-8",
    )

    logs = _collect_superseedr_logs({"superseedr_seed": share_root})

    assert logs["superseedr_seed"]["files"] == [str(logs_root / "app.2026-05-17.log")]
    assert "ready" in logs["superseedr_seed"]["text"]


def test_superseedr_log_summary_requires_collected_app_logs() -> None:
    summary = _summarize_superseedr_logs(
        {
            "superseedr_seed": {
                "files": [],
                "text": "",
            }
        }
    )

    assert summary["ok"] is False
    assert summary["issues"] == [
        "superseedr_seed did not produce a Superseedr app log file",
    ]


def test_superseedr_health_probe_fails_on_error_logs() -> None:
    probe = _probe_superseedr_health(
        {
            "ok": False,
            "issues": ["superseedr_leech emitted 1 Superseedr error log line(s)"],
            "warnings": [],
            "service_count": 1,
            "line_count": 3,
            "error_count": 1,
            "warning_count": 0,
            "services": {},
        }
    )

    assert probe["ok"] is False
    assert probe["issues"] == ["superseedr_leech emitted 1 Superseedr error log line(s)"]
    assert probe["metrics"]["error_count"] == 1


def test_behavior_probes_include_transfer_and_event_health() -> None:
    scenario = LabScenario.from_file(
        Path("integration_tests/libtorrent_lab/scenarios/basic_ul_dl_tcp_only.json")
    )
    report = _run_behavior_probes(
        scenario=scenario,
        transfer_assertions={"ok": True, "issues": [], "checks": [{"name": "download"}]},
        event_summary={
            "seed": {"participants": {}},
            "leech": {
                "participants": {
                    "leech": {
                        "event_counts": {"status": 1},
                        "hard_alert_counts": {},
                        "status_sample_count": 1,
                        "first_progress_secs": 0.5,
                        "max_total_done": 16,
                    }
                }
            },
        },
        tracker_summary={
            "ok": True,
            "issues": [],
            "announce_count": 2,
            "unique_peer_count": 2,
        },
        superseedr_summary={
            "ok": True,
            "issues": [],
            "warnings": [],
            "service_count": 0,
            "line_count": 0,
            "error_count": 0,
            "warning_count": 0,
            "services": {},
        },
    )

    assert report["ok"] is True
    assert [probe["name"] for probe in report["probes"]] == [
        "transfer_accounting",
        "libtorrent_event_health",
        "tracker_announces",
        "progress_timeline",
        "superseedr_health",
    ]


def test_validate_download_reports_hash_match(tmp_path: Path) -> None:
    expected = tmp_path / "expected.bin"
    actual = tmp_path / "actual.bin"
    expected.write_bytes(b"deterministic payload")
    actual.write_bytes(b"deterministic payload")

    report = _validate_download(actual, expected)

    assert report["ok"] is True
    assert report["issues"] == []
    assert report["expected_sha256"] == report["actual_sha256"]


def test_validate_download_reports_missing_file(tmp_path: Path) -> None:
    expected = tmp_path / "expected.bin"
    missing = tmp_path / "missing.bin"
    expected.write_bytes(b"deterministic payload")

    report = _validate_download(missing, expected)

    assert report["ok"] is False
    assert report["issues"] == ["missing missing.bin"]


def test_validate_download_set_reports_all_participants(tmp_path: Path) -> None:
    expected = tmp_path / "expected.bin"
    one = tmp_path / "one.bin"
    two = tmp_path / "two.bin"
    expected.write_bytes(b"fanout")
    one.write_bytes(b"fanout")
    two.write_bytes(b"different")

    report = _validate_download_set({"one": one, "two": two}, expected)

    assert report["ok"] is False
    assert report["participant_count"] == 2
    assert report["participants"]["one"]["ok"] is True
    assert report["participants"]["two"]["ok"] is False
    assert report["issues"][0].startswith("two: size expected=6 actual=9")


def test_validate_download_reports_directory_hash_match(tmp_path: Path) -> None:
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    (expected / "subdir").mkdir(parents=True)
    (actual / "subdir").mkdir(parents=True)
    (expected / "root.bin").write_bytes(b"root")
    (actual / "root.bin").write_bytes(b"root")
    (expected / "subdir" / "leaf.bin").write_bytes(b"leaf")
    (actual / "subdir" / "leaf.bin").write_bytes(b"leaf")

    report = _validate_download(actual, expected)

    assert report["ok"] is True
    assert report["issues"] == []
    assert report["expected_files"] == 2
    assert report["actual_files"] == 2


def test_validate_download_reports_directory_mismatch(tmp_path: Path) -> None:
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    expected.mkdir()
    actual.mkdir()
    (expected / "same.bin").write_bytes(b"same")
    (actual / "same.bin").write_bytes(b"different")
    (expected / "missing.bin").write_bytes(b"missing")
    (actual / "extra.bin").write_bytes(b"extra")

    report = _validate_download(actual, expected)

    assert report["ok"] is False
    assert report["missing"] == ["missing.bin"]
    assert report["extra"] == ["extra.bin"]
    assert report["mismatched"][0].startswith("same.bin size expected=4 actual=9")
