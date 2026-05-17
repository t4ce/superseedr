from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as url_error
from urllib import request as url_request

from integration_tests.harness.config import resolve_paths
from integration_tests.harness.docker_ctl import DockerCompose


LAB_ROOT = Path(__file__).resolve().parent
SCENARIOS_ROOT = LAB_ROOT / "scenarios"
COMPOSE_FILE = LAB_ROOT / "docker" / "docker-compose.libtorrent-lab.yml"
TRACKER_ANNOUNCE_URL = "http://tracker:6969/announce"
CLIENT_LIBTORRENT = "libtorrent"
CLIENT_SUPERSEEDR = "superseedr"
CLIENTS = {CLIENT_LIBTORRENT, CLIENT_SUPERSEEDR}
MAX_LIBTORRENT_FANOUT = 3
LAB_MATRIXES: dict[str, list[str]] = {
    "smoke": [
        "basic_ul_dl",
        "superseedr_to_libtorrent",
        "libtorrent_to_superseedr",
    ],
    "transport": [
        "superseedr_to_libtorrent",
        "libtorrent_to_superseedr",
        "superseedr_utp_to_libtorrent",
        "libtorrent_utp_to_superseedr",
    ],
    "fixtures": [
        "superseedr_to_libtorrent_v1_multi_file",
        "libtorrent_to_superseedr_v1_nested",
        "superseedr_to_libtorrent_v2_multi_file",
        "libtorrent_to_superseedr_hybrid_nested",
    ],
    "fanout": [
        "superseedr_to_libtorrent_tcp_fanout",
        "libtorrent_to_superseedr_tcp_fanout",
    ],
}
LAB_MATRIXES["full"] = [
    *LAB_MATRIXES["smoke"],
    "superseedr_utp_to_libtorrent",
    "libtorrent_utp_to_superseedr",
    *LAB_MATRIXES["fixtures"],
    *LAB_MATRIXES["fanout"],
]


@dataclass(frozen=True)
class LabScenario:
    name: str
    seed_client: str
    leech_client: str
    mode: str
    torrent: str
    payload: str
    download_name: str
    timeout_secs: int
    seed_listen_port: int
    leech_listen_port: int
    libtorrent_seed_count: int
    libtorrent_leech_count: int
    superseedr_peer_transport: str
    libtorrent_settings: dict[str, object]

    @classmethod
    def from_file(cls, path: Path) -> "LabScenario":
        raw = json.loads(path.read_text(encoding="utf-8"))
        seed_client = str(raw.get("seed_client", CLIENT_LIBTORRENT))
        leech_client = str(raw.get("leech_client", CLIENT_LIBTORRENT))
        if seed_client not in CLIENTS:
            raise ValueError(f"Unsupported seed_client={seed_client!r} in {path}")
        if leech_client not in CLIENTS:
            raise ValueError(f"Unsupported leech_client={leech_client!r} in {path}")
        seed_count = int(raw.get("libtorrent_seed_count", 1))
        leech_count = int(raw.get("libtorrent_leech_count", 1))
        for label, count in (
            ("libtorrent_seed_count", seed_count),
            ("libtorrent_leech_count", leech_count),
        ):
            if count < 1 or count > MAX_LIBTORRENT_FANOUT:
                raise ValueError(
                    f"{label}={count} outside supported range 1..{MAX_LIBTORRENT_FANOUT} in {path}"
                )
        return cls(
            name=str(raw["name"]),
            seed_client=seed_client,
            leech_client=leech_client,
            mode=str(raw["mode"]),
            torrent=str(raw["torrent"]),
            payload=str(raw["payload"]),
            download_name=str(raw["download_name"]),
            timeout_secs=int(raw.get("timeout_secs", 120)),
            seed_listen_port=int(raw.get("seed_listen_port", 26881)),
            leech_listen_port=int(raw.get("leech_listen_port", 26882)),
            libtorrent_seed_count=seed_count,
            libtorrent_leech_count=leech_count,
            superseedr_peer_transport=str(raw.get("superseedr_peer_transport", "tcp")),
            libtorrent_settings=dict(raw.get("libtorrent_settings", {})),
        )


@dataclass(frozen=True)
class NetworkImpairment:
    delay_ms: int = 0
    jitter_ms: int = 0
    loss_pct: float = 0.0
    duplicate_pct: float = 0.0
    corrupt_pct: float = 0.0
    reorder_pct: float = 0.0

    def enabled(self) -> bool:
        return any(
            value > 0
            for value in (
                self.delay_ms,
                self.jitter_ms,
                self.loss_pct,
                self.duplicate_pct,
                self.corrupt_pct,
                self.reorder_pct,
            )
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled(),
            "delay_ms": self.delay_ms,
            "jitter_ms": self.jitter_ms,
            "loss_pct": self.loss_pct,
            "duplicate_pct": self.duplicate_pct,
            "corrupt_pct": self.corrupt_pct,
            "reorder_pct": self.reorder_pct,
        }


def _validate_impairment(config: NetworkImpairment) -> None:
    if config.delay_ms < 0 or config.jitter_ms < 0:
        raise ValueError("Network impairment delay and jitter must be non-negative")
    for label, value in (
        ("loss_pct", config.loss_pct),
        ("duplicate_pct", config.duplicate_pct),
        ("corrupt_pct", config.corrupt_pct),
        ("reorder_pct", config.reorder_pct),
    ):
        if value < 0 or value > 100:
            raise ValueError(f"Network impairment {label} must be in range 0..100")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _project_name(run_id: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "" for ch in run_id)
    return f"ltlab{safe}"[:48]


def _scenario_names_for_matrix(matrix: str) -> list[str]:
    try:
        return LAB_MATRIXES[matrix]
    except KeyError as exc:
        known = ", ".join(sorted(LAB_MATRIXES))
        raise ValueError(f"Unknown libtorrent lab matrix {matrix!r}; expected one of: {known}") from exc


def _libtorrent_service(role: str, index: int) -> str:
    base = f"libtorrent_{role}"
    return base if index == 1 else f"{base}_{index}"


def _libtorrent_label(role: str, index: int) -> str:
    return role if index == 1 else f"{role}_{index}"


def _libtorrent_slot_root(runtime_root: Path, role: str, index: int, kind: str) -> Path:
    suffix = "" if index == 1 else f"_{index}"
    return runtime_root / f"libtorrent_{role}{suffix}_{kind}"


def _libtorrent_artifacts_root(run_root: Path, role: str, index: int) -> Path:
    return run_root / "peers" / _libtorrent_service(role, index)


def _active_libtorrent_count(scenario: LabScenario, role: str) -> int:
    client = scenario.seed_client if role == "seed" else scenario.leech_client
    if client != CLIENT_LIBTORRENT:
        return 0
    return scenario.libtorrent_seed_count if role == "seed" else scenario.libtorrent_leech_count


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _pct(value: float) -> str:
    text = f"{value:.3f}".rstrip("0").rstrip(".")
    return text or "0"


def _netem_command(config: NetworkImpairment) -> list[str]:
    _validate_impairment(config)
    args = ["tc", "qdisc", "replace", "dev", "eth0", "root", "netem"]
    if config.delay_ms > 0 or config.jitter_ms > 0:
        args.extend(["delay", f"{config.delay_ms}ms"])
        if config.jitter_ms > 0:
            args.append(f"{config.jitter_ms}ms")
    if config.loss_pct > 0:
        args.extend(["loss", f"{_pct(config.loss_pct)}%"])
    if config.duplicate_pct > 0:
        args.extend(["duplicate", f"{_pct(config.duplicate_pct)}%"])
    if config.corrupt_pct > 0:
        args.extend(["corrupt", f"{_pct(config.corrupt_pct)}%"])
    if config.reorder_pct > 0:
        args.extend(["reorder", f"{_pct(config.reorder_pct)}%", "50%"])
    return args


def _apply_network_impairment(
    compose: DockerCompose,
    services: list[str],
    config: NetworkImpairment,
) -> dict[str, object]:
    if not config.enabled():
        return {
            "enabled": False,
            "config": config.as_dict(),
            "services": [],
            "issues": [],
            "ok": True,
        }

    command = _netem_command(config)
    issues: list[str] = []
    applied_services: list[str] = []
    for service in services:
        result = compose.exec(service, command, check=False, capture=True)
        if result.returncode == 0:
            applied_services.append(service)
            continue
        issues.append(
            f"{service}: tc exited {result.returncode}: "
            f"{(result.stderr or result.stdout).strip()}"
        )

    return {
        "enabled": True,
        "config": config.as_dict(),
        "command": command,
        "services": applied_services,
        "issues": issues,
        "ok": not issues,
    }


def _wait_for_tracker(port: int, timeout_secs: int = 20) -> None:
    deadline = time.monotonic() + timeout_secs
    url = f"http://127.0.0.1:{port}/announce"
    while time.monotonic() < deadline:
        try:
            with url_request.urlopen(url, timeout=1) as resp:
                if resp.status in (200, 400):
                    return
        except url_error.HTTPError as exc:
            if exc.code == 400:
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Tracker did not become ready within {timeout_secs}s on {url}")


def _read_status(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"status": "missing"}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"status": "invalid", "error": str(exc)}


def _payload_parent(scenario: LabScenario) -> Path:
    parent = Path(scenario.payload).parent
    return Path() if parent.as_posix() == "." else parent


def _payload_is_directory(scenario: LabScenario) -> bool:
    return Path(scenario.payload).suffix == ""


def _superseedr_payload_bucket(scenario: LabScenario) -> Path:
    if _payload_is_directory(scenario):
        return Path(scenario.payload)
    return _payload_parent(scenario)


def _client_payload_path(client: str, data_root: Path, scenario: LabScenario) -> Path:
    if client == CLIENT_SUPERSEEDR:
        bucket = _superseedr_payload_bucket(scenario)
        if _payload_is_directory(scenario):
            return data_root / scenario.mode / bucket
        return data_root / scenario.mode / bucket / scenario.download_name
    return data_root / scenario.download_name


def _superseedr_download_path(role: str, scenario: LabScenario) -> str:
    parts = ["/superseedr-data", role, scenario.mode]
    bucket = _superseedr_payload_bucket(scenario).as_posix()
    if bucket:
        parts.append(bucket)
    return "/".join(parts)


def _status_torrents(raw: dict[str, object]) -> list[dict[str, object]]:
    torrents = raw.get("torrents", {})
    if isinstance(torrents, dict):
        return [value for value in torrents.values() if isinstance(value, dict)]
    return []


def _read_superseedr_status(share_root: Path, role: str) -> dict[str, object]:
    status_file = share_root / "status_files" / "app_state.json"
    raw = _read_status(status_file)
    if raw.get("status") in {"missing", "invalid"}:
        return {
            "client": CLIENT_SUPERSEEDR,
            "role": role,
            "status": raw.get("status"),
            "error": raw.get("error"),
            "status_path": str(status_file),
        }

    torrents = _status_torrents(raw)
    return {
        "client": CLIENT_SUPERSEEDR,
        "role": role,
        "status": "ok",
        "status_path": str(status_file),
        "torrent_count": len(torrents),
        "complete_torrents": sum(1 for t in torrents if t.get("is_complete") is True),
        "data_available_torrents": sum(1 for t in torrents if t.get("data_available") is True),
        "activity_messages": sorted(
            {str(t.get("activity_message", "")) for t in torrents if t.get("activity_message")}
        ),
        "session_total_downloaded": sum(int(t.get("session_total_downloaded", 0)) for t in torrents),
        "session_total_uploaded": sum(int(t.get("session_total_uploaded", 0)) for t in torrents),
        "connected_peers": sum(int(t.get("number_of_successfully_connected_peers", 0)) for t in torrents),
        "tcp_peer_count": sum(int(t.get("tcp_peer_count", 0)) for t in torrents),
        "utp_peer_count": sum(int(t.get("utp_peer_count", 0)) for t in torrents),
        "beneficial_tcp_peer_count": sum(
            int(t.get("beneficial_tcp_peer_count", 0)) for t in torrents
        ),
        "beneficial_utp_peer_count": sum(
            int(t.get("beneficial_utp_peer_count", 0)) for t in torrents
        ),
    }


def _superseedr_seed_is_ready(status: dict[str, object]) -> bool:
    if status.get("status") != "ok":
        return False
    messages = set(status.get("activity_messages", []))
    if messages.intersection({"Seeding", "Finished"}):
        return True
    return (
        int(status.get("complete_torrents", 0)) > 0
        and int(status.get("data_available_torrents", 0)) > 0
    )


def _validate_superseedr_transport_observations(
    scenario: LabScenario,
    seed_status: dict[str, object],
    leech_status: dict[str, object],
) -> dict[str, object]:
    issues: list[str] = []
    if scenario.superseedr_peer_transport != "utp":
        return {"ok": True, "issues": issues}

    for role, client, status in (
        ("seed", scenario.seed_client, seed_status),
        ("leech", scenario.leech_client, leech_status),
    ):
        if client != CLIENT_SUPERSEEDR:
            continue
        if status.get("status") != "ok":
            issues.append(f"{role} Superseedr status is not ok")
            continue
        tcp_peers = int(status.get("tcp_peer_count", 0))
        utp_peers = int(status.get("utp_peer_count", 0))
        beneficial_utp_peers = int(status.get("beneficial_utp_peer_count", 0))
        if tcp_peers != 0:
            issues.append(f"{role} Superseedr observed {tcp_peers} TCP peer(s) in uTP-only mode")
        if utp_peers < 1:
            issues.append(f"{role} Superseedr did not observe a uTP peer")
        if beneficial_utp_peers < 1:
            issues.append(f"{role} Superseedr did not move payload over uTP")

    return {"ok": not issues, "issues": issues}


def _wait_for_superseedr_seed_ready(share_root: Path, timeout_secs: int) -> None:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        status = _read_superseedr_status(share_root, role="seed")
        if _superseedr_seed_is_ready(status):
            return
        time.sleep(0.5)
    raise RuntimeError(f"Superseedr seed did not become ready within {timeout_secs}s")


def _wait_for_superseedr_counter_at_least(
    share_root: Path,
    role: str,
    field: str,
    minimum: int,
    timeout_secs: int,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_secs
    last_status: dict[str, object] = {"status": "missing"}
    while time.monotonic() < deadline:
        last_status = _read_superseedr_status(share_root, role=role)
        try:
            current = int(last_status.get(field, 0))
        except (TypeError, ValueError):
            current = 0
        if current >= minimum:
            return last_status
        time.sleep(0.25)
    return last_status


def _wait_for_seed_ready(status_path: Path, timeout_secs: int) -> None:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        status = _read_status(status_path)
        if status.get("is_seed") is True:
            return
        time.sleep(0.5)
    raise RuntimeError(f"Seed peer did not become ready within {timeout_secs}s")


def _wait_for_seed_status(status_path: Path, timeout_secs: int) -> dict[str, object]:
    deadline = time.monotonic() + timeout_secs
    last_status: dict[str, object] = {"status": "missing"}
    while time.monotonic() < deadline:
        last_status = _read_status(status_path)
        if last_status.get("is_seed") is True:
            return last_status
        time.sleep(0.25)
    return last_status


def _wait_for_counter_at_least(
    status_path: Path,
    field: str,
    minimum: int,
    timeout_secs: int,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_secs
    last_status: dict[str, object] = {"status": "missing"}
    while time.monotonic() < deadline:
        last_status = _read_status(status_path)
        try:
            current = int(last_status.get(field, 0))
        except (TypeError, ValueError):
            current = 0
        if current >= minimum:
            return last_status
        time.sleep(0.25)
    return last_status


def _validate_download(actual_path: Path, expected_path: Path) -> dict[str, object]:
    if expected_path.is_dir():
        return _validate_directory(actual_path, expected_path)

    issues: list[str] = []
    if not actual_path.exists():
        issues.append(f"missing {actual_path.name}")
        return {"ok": False, "issues": issues}

    expected_size = expected_path.stat().st_size
    actual_size = actual_path.stat().st_size
    if actual_size != expected_size:
        issues.append(f"size expected={expected_size} actual={actual_size}")

    expected_hash = _sha256_file(expected_path)
    actual_hash = _sha256_file(actual_path)
    if actual_hash != expected_hash:
        issues.append(f"sha256 expected={expected_hash} actual={actual_hash}")

    return {
        "ok": not issues,
        "issues": issues,
        "expected_size": expected_size,
        "actual_size": actual_size,
        "expected_sha256": expected_hash,
        "actual_sha256": actual_hash,
    }


def _directory_manifest(root: Path) -> dict[str, tuple[int, str]]:
    manifest: dict[str, tuple[int, str]] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.name.startswith("."):
            rel = path.relative_to(root).as_posix()
            manifest[rel] = (path.stat().st_size, _sha256_file(path))
    return manifest


def _manifest_total_size(manifest: dict[str, tuple[int, str]]) -> int:
    return sum(size for size, _sha in manifest.values())


def _payload_total_size(path: Path) -> int:
    if path.is_dir():
        return _manifest_total_size(_directory_manifest(path))
    return path.stat().st_size


def _validate_directory(actual_path: Path, expected_path: Path) -> dict[str, object]:
    if not actual_path.exists():
        return {
            "ok": False,
            "issues": [f"missing {actual_path.name}"],
            "missing": ["."],
            "extra": [],
            "mismatched": [],
        }
    if not actual_path.is_dir():
        return {
            "ok": False,
            "issues": [f"expected directory at {actual_path.name}"],
            "missing": [],
            "extra": [],
            "mismatched": [],
        }

    expected_manifest = _directory_manifest(expected_path)
    actual_manifest = _directory_manifest(actual_path)
    missing: list[str] = []
    extra: list[str] = []
    mismatched: list[str] = []

    for rel, (expected_size, expected_sha) in expected_manifest.items():
        actual = actual_manifest.get(rel)
        if actual is None:
            missing.append(rel)
            continue
        actual_size, actual_sha = actual
        if actual_size != expected_size:
            mismatched.append(f"{rel} size expected={expected_size} actual={actual_size}")
            continue
        if actual_sha != expected_sha:
            mismatched.append(f"{rel} sha256 expected={expected_sha} actual={actual_sha}")

    for rel in sorted(set(actual_manifest) - set(expected_manifest)):
        extra.append(rel)

    issues = [*missing, *extra, *mismatched]
    return {
        "ok": not issues,
        "issues": issues,
        "missing": missing,
        "extra": extra,
        "mismatched": mismatched,
        "expected_files": len(expected_manifest),
        "actual_files": len(actual_manifest),
        "expected_size": _manifest_total_size(expected_manifest),
        "actual_size": _manifest_total_size(actual_manifest),
    }


def _validate_download_set(
    actual_paths: dict[str, Path],
    expected_path: Path,
) -> dict[str, object]:
    if len(actual_paths) == 1:
        return _validate_download(next(iter(actual_paths.values())), expected_path)

    participant_reports: dict[str, object] = {}
    issues: list[str] = []
    for label, actual_path in sorted(actual_paths.items()):
        report = _validate_download(actual_path, expected_path)
        participant_reports[label] = report
        if not report["ok"]:
            issues.extend(f"{label}: {issue}" for issue in report.get("issues", []))

    return {
        "ok": not issues,
        "issues": issues,
        "participant_count": len(actual_paths),
        "participants": participant_reports,
    }


def _read_libtorrent_statuses(
    artifacts_roots: dict[int, Path],
    role: str,
) -> dict[str, object]:
    statuses: dict[str, dict[str, object]] = {}
    for index, artifacts_root in sorted(artifacts_roots.items()):
        status = _read_status(artifacts_root / "status.json")
        status["client"] = CLIENT_LIBTORRENT
        status["slot"] = index
        statuses[_libtorrent_label(role, index)] = status

    if len(statuses) == 1:
        return next(iter(statuses.values()))

    return _aggregate_libtorrent_statuses(statuses, role)


def _aggregate_libtorrent_statuses(
    statuses: dict[str, dict[str, object]],
    role: str,
) -> dict[str, object]:
    return {
        "client": CLIENT_LIBTORRENT,
        "role": role,
        "peer_count": len(statuses),
        "complete_peers": sum(1 for status in statuses.values() if status.get("is_seed") is True),
        "total_done": sum(int(status.get("total_done", 0)) for status in statuses.values()),
        "total_download": sum(int(status.get("total_download", 0)) for status in statuses.values()),
        "total_upload": sum(int(status.get("total_upload", 0)) for status in statuses.values()),
        "participants": statuses,
    }


def _wait_for_libtorrent_seed_statuses(
    artifacts_roots: dict[int, Path],
    role: str,
    timeout_secs: int,
) -> dict[str, object]:
    if len(artifacts_roots) == 1:
        status = _wait_for_seed_status(next(iter(artifacts_roots.values())) / "status.json", timeout_secs)
        status["client"] = CLIENT_LIBTORRENT
        return status

    deadline = time.monotonic() + timeout_secs
    last_status = _read_libtorrent_statuses(artifacts_roots, role)
    while time.monotonic() < deadline:
        last_status = _read_libtorrent_statuses(artifacts_roots, role)
        if int(last_status.get("complete_peers", 0)) == len(artifacts_roots):
            return last_status
        time.sleep(0.25)
    return last_status


def _wait_for_libtorrent_total_counter_at_least(
    artifacts_roots: dict[int, Path],
    role: str,
    field: str,
    minimum: int,
    timeout_secs: int,
) -> dict[str, object]:
    if len(artifacts_roots) == 1:
        status = _wait_for_counter_at_least(
            next(iter(artifacts_roots.values())) / "status.json",
            field,
            minimum,
            timeout_secs,
        )
        status["client"] = CLIENT_LIBTORRENT
        return status

    deadline = time.monotonic() + timeout_secs
    last_status = _read_libtorrent_statuses(artifacts_roots, role)
    aggregate_field = field
    while time.monotonic() < deadline:
        last_status = _read_libtorrent_statuses(artifacts_roots, role)
        current = int(last_status.get(aggregate_field, 0))
        if current >= minimum:
            return last_status
        time.sleep(0.25)
    return last_status


def _generate_torrents(repo_root: Path, output_root: Path) -> None:
    subprocess.run(["python3", "scripts/generate_integration_bins.py"], cwd=repo_root, check=True)
    subprocess.run(
        [
            "python3",
            "scripts/generate_integration_torrents.py",
            "--announce-url",
            TRACKER_ANNOUNCE_URL,
            "--output-root",
            str(output_root),
        ],
        cwd=repo_root,
        check=True,
    )


def _write_peer_config(
    path: Path,
    *,
    peer_id: str,
    role: str,
    listen_port: int,
    torrent_path: str,
    save_path: str,
    timeout_secs: int,
    settings: dict[str, object],
) -> None:
    _write_json(
        path,
        {
            "peer_id": peer_id,
            "role": role,
            "listen_port": listen_port,
            "torrent_path": torrent_path,
            "save_path": save_path,
            "timeout_secs": timeout_secs,
            "exit_when_seed": role == "leech",
            "status_path": "/artifacts/status.json",
            "events_path": "/artifacts/events.jsonl",
            "settings": settings,
        },
    )


def _write_superseedr_settings(
    path: Path,
    *,
    role: str,
    scenario: LabScenario,
    client_port: int,
) -> None:
    role_root = f"/superseedr-data/{role}/{scenario.mode}"
    download_path = _superseedr_download_path(role, scenario)
    torrent_name = scenario.torrent.removesuffix(".torrent")
    client_id = "-SS1000-SEEDCLIENT01" if role == "seed" else "-SS1000-LEECHCLIENT1"
    lines = [
        f'client_id = "{client_id}"',
        f"client_port = {client_port}",
        "lifetime_downloaded = 0",
        "lifetime_uploaded = 0",
        "private_client = false",
        'torrent_sort_column = "Up"',
        'torrent_sort_direction = "Ascending"',
        'peer_sort_column = "UL"',
        'peer_sort_direction = "Ascending"',
        'ui_theme = "catppuccin_mocha"',
        f'default_download_folder = "{role_root}"',
        "max_connected_peers = 500",
        "output_status_interval = 1",
        "bootstrap_nodes = []",
        "global_download_limit_bps = 0",
        "global_upload_limit_bps = 0",
        "max_concurrent_validations = 16",
        "connection_attempt_permits = 16",
        "upload_slots = 8",
        "peer_upload_in_flight_limit = 4",
        "tracker_fallback_interval_secs = 10",
        "client_leeching_fallback_interval_secs = 10",
        "",
        "[[torrents]]",
        f'torrent_or_magnet = "/fixtures/torrents/{scenario.mode}/{scenario.torrent}"',
        f'name = "{torrent_name}"',
        "validation_status = false",
        f'download_path = "{download_path}"',
        'container_name = ""',
        'torrent_control_state = "Running"',
        "",
        "[torrents.file_priorities]",
        '0 = "Normal"',
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _copy_payload(source: Path, dest: Path) -> None:
    if source.is_dir():
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, dest)
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)


def run_lab_scenario(
    *,
    scenario: LabScenario,
    run_id: str,
    timeout_secs: int | None = None,
    skip_build: bool = False,
    network_impairment: NetworkImpairment | None = None,
) -> dict[str, object]:
    paths = resolve_paths()
    timeout = timeout_secs or scenario.timeout_secs
    impairment = network_impairment or NetworkImpairment()
    _validate_impairment(impairment)
    run_root = paths.artifacts_root / "libtorrent_lab" / run_id
    runtime_root = run_root / "runtime"
    fixtures_root = runtime_root / "fixtures"
    torrents_root = fixtures_root / "torrents"
    lt_seed_data_roots = {
        index: _libtorrent_slot_root(runtime_root, "seed", index, "data")
        for index in range(1, MAX_LIBTORRENT_FANOUT + 1)
    }
    lt_leech_data_roots = {
        index: _libtorrent_slot_root(runtime_root, "leech", index, "data")
        for index in range(1, MAX_LIBTORRENT_FANOUT + 1)
    }
    lt_seed_config_roots = {
        index: _libtorrent_slot_root(runtime_root, "seed", index, "config")
        for index in range(1, MAX_LIBTORRENT_FANOUT + 1)
    }
    lt_leech_config_roots = {
        index: _libtorrent_slot_root(runtime_root, "leech", index, "config")
        for index in range(1, MAX_LIBTORRENT_FANOUT + 1)
    }
    lt_seed_artifacts_roots = {
        index: _libtorrent_artifacts_root(run_root, "seed", index)
        for index in range(1, MAX_LIBTORRENT_FANOUT + 1)
    }
    lt_leech_artifacts_roots = {
        index: _libtorrent_artifacts_root(run_root, "leech", index)
        for index in range(1, MAX_LIBTORRENT_FANOUT + 1)
    }
    ss_seed_data_root = runtime_root / "superseedr_seed_data"
    ss_leech_data_root = runtime_root / "superseedr_leech_data"
    ss_seed_config_root = runtime_root / "superseedr_seed_config"
    ss_leech_config_root = runtime_root / "superseedr_leech_config"
    ss_seed_share_root = runtime_root / "superseedr_seed_share"
    ss_leech_share_root = runtime_root / "superseedr_leech_share"
    logs_root = run_root / "logs"

    if run_root.exists():
        shutil.rmtree(run_root)
    for path in (
        torrents_root,
        ss_seed_data_root,
        ss_leech_data_root,
        ss_seed_config_root,
        ss_leech_config_root,
        ss_seed_share_root,
        ss_leech_share_root,
        logs_root,
        *lt_seed_data_roots.values(),
        *lt_leech_data_roots.values(),
        *lt_seed_config_roots.values(),
        *lt_leech_config_roots.values(),
        *lt_seed_artifacts_roots.values(),
        *lt_leech_artifacts_roots.values(),
    ):
        path.mkdir(parents=True, exist_ok=True)

    _generate_torrents(paths.root, torrents_root)

    source_payload = paths.test_data_root / scenario.payload
    active_seed_count = _active_libtorrent_count(scenario, "seed")
    active_leech_count = _active_libtorrent_count(scenario, "leech")
    active_seed_artifacts = {
        index: lt_seed_artifacts_roots[index] for index in range(1, active_seed_count + 1)
    }
    active_leech_artifacts = {
        index: lt_leech_artifacts_roots[index] for index in range(1, active_leech_count + 1)
    }

    if scenario.seed_client == CLIENT_SUPERSEEDR:
        seed_payload = _client_payload_path(CLIENT_SUPERSEEDR, ss_seed_data_root, scenario)
        _copy_payload(source_payload, seed_payload)
    else:
        for index in range(1, active_seed_count + 1):
            seed_payload = _client_payload_path(CLIENT_LIBTORRENT, lt_seed_data_roots[index], scenario)
            _copy_payload(source_payload, seed_payload)

    if scenario.leech_client == CLIENT_SUPERSEEDR:
        leech_payloads = {
            "superseedr_leech": _client_payload_path(CLIENT_SUPERSEEDR, ss_leech_data_root, scenario)
        }
    else:
        leech_payloads = {
            _libtorrent_label("leech", index): _client_payload_path(
                CLIENT_LIBTORRENT,
                lt_leech_data_roots[index],
                scenario,
            )
            for index in range(1, active_leech_count + 1)
        }
    source_payload_size = _payload_total_size(source_payload)

    torrent_path = f"/fixtures/torrents/{scenario.mode}/{scenario.torrent}"
    settings = dict(scenario.libtorrent_settings)
    for index in range(1, MAX_LIBTORRENT_FANOUT + 1):
        _write_peer_config(
            lt_seed_config_roots[index] / "peer.json",
            peer_id=_libtorrent_label("seed", index),
            role="seed",
            listen_port=scenario.seed_listen_port + index - 1,
            torrent_path=torrent_path,
            save_path="/data",
            timeout_secs=timeout,
            settings=settings,
        )
        _write_peer_config(
            lt_leech_config_roots[index] / "peer.json",
            peer_id=_libtorrent_label("leech", index),
            role="leech",
            listen_port=scenario.leech_listen_port + index - 1,
            torrent_path=torrent_path,
            save_path="/data",
            timeout_secs=timeout,
            settings=settings,
        )
    _write_superseedr_settings(
        ss_seed_config_root / "settings.toml",
        role="seed",
        scenario=scenario,
        client_port=16881,
    )
    _write_superseedr_settings(
        ss_leech_config_root / "settings.toml",
        role="leech",
        scenario=scenario,
        client_port=16882,
    )

    project_name = _project_name(run_id)
    tracker_port = _reserve_local_port()
    compose_env = {
        "LIBTORRENT_LAB_TRACKER_PORT": str(tracker_port),
        "LIBTORRENT_LAB_TRACKER_SCRIPT_PATH": str(paths.tracker_script.resolve()),
        "LIBTORRENT_LAB_FIXTURES_PATH": str(fixtures_root.resolve()),
        "LIBTORRENT_LAB_SUPERSEEDR_PEER_TRANSPORT": scenario.superseedr_peer_transport,
        "LIBTORRENT_LAB_SUPERSEEDR_SEED_DATA_PATH": str(ss_seed_data_root.resolve()),
        "LIBTORRENT_LAB_SUPERSEEDR_LEECH_DATA_PATH": str(ss_leech_data_root.resolve()),
        "LIBTORRENT_LAB_SUPERSEEDR_SEED_CONFIG_PATH": str(ss_seed_config_root.resolve()),
        "LIBTORRENT_LAB_SUPERSEEDR_LEECH_CONFIG_PATH": str(ss_leech_config_root.resolve()),
        "LIBTORRENT_LAB_SUPERSEEDR_SEED_SHARE_PATH": str(ss_seed_share_root.resolve()),
        "LIBTORRENT_LAB_SUPERSEEDR_LEECH_SHARE_PATH": str(ss_leech_share_root.resolve()),
    }
    for index in range(1, MAX_LIBTORRENT_FANOUT + 1):
        suffix = "" if index == 1 else f"_{index}"
        compose_env[f"LIBTORRENT_LAB_SEED_DATA_PATH{suffix}"] = str(
            lt_seed_data_roots[index].resolve()
        )
        compose_env[f"LIBTORRENT_LAB_LEECH_DATA_PATH{suffix}"] = str(
            lt_leech_data_roots[index].resolve()
        )
        compose_env[f"LIBTORRENT_LAB_SEED_CONFIG_PATH{suffix}"] = str(
            lt_seed_config_roots[index].resolve()
        )
        compose_env[f"LIBTORRENT_LAB_LEECH_CONFIG_PATH{suffix}"] = str(
            lt_leech_config_roots[index].resolve()
        )
        compose_env[f"LIBTORRENT_LAB_SEED_ARTIFACTS_PATH{suffix}"] = str(
            lt_seed_artifacts_roots[index].resolve()
        )
        compose_env[f"LIBTORRENT_LAB_LEECH_ARTIFACTS_PATH{suffix}"] = str(
            lt_leech_artifacts_roots[index].resolve()
        )
    compose = DockerCompose(COMPOSE_FILE, project_name, compose_env)

    summary: dict[str, object] = {
        "run_id": run_id,
        "scenario": scenario.name,
        "seed_client": scenario.seed_client,
        "leech_client": scenario.leech_client,
        "libtorrent_seed_count": active_seed_count,
        "libtorrent_leech_count": active_leech_count,
        "superseedr_peer_transport": scenario.superseedr_peer_transport,
        "network_impairment": impairment.as_dict(),
        "artifacts_dir": str(run_root),
        "ok": False,
    }
    started_at = time.monotonic()
    seed_services = (
        ["superseedr_seed"]
        if scenario.seed_client == CLIENT_SUPERSEEDR
        else [_libtorrent_service("seed", index) for index in range(1, active_seed_count + 1)]
    )
    leech_services = (
        ["superseedr_leech"]
        if scenario.leech_client == CLIENT_SUPERSEEDR
        else [_libtorrent_service("leech", index) for index in range(1, active_leech_count + 1)]
    )
    active_services = ["tracker", *seed_services, *leech_services]

    try:
        compose.down()
        if not skip_build:
            compose.run(["build", "libtorrent_seed"])
            if CLIENT_SUPERSEEDR in {scenario.seed_client, scenario.leech_client}:
                compose.run(["build", "superseedr_seed"])
        compose.up(["tracker"], no_build=True)
        _wait_for_tracker(tracker_port)
        compose.up(seed_services, no_build=True)
        if scenario.seed_client == CLIENT_SUPERSEEDR:
            _wait_for_superseedr_seed_ready(ss_seed_share_root, timeout_secs=30)
        else:
            for artifacts_root in active_seed_artifacts.values():
                _wait_for_seed_ready(artifacts_root / "status.json", timeout_secs=30)
        compose.up(leech_services, no_build=True)
        impairment_result = _apply_network_impairment(
            compose,
            [*seed_services, *leech_services],
            impairment,
        )
        summary["network_impairment"] = impairment_result
        if impairment_result.get("issues"):
            raise RuntimeError(
                "Failed to apply network impairment: "
                + "; ".join(str(issue) for issue in impairment_result["issues"])
            )

        deadline = time.monotonic() + timeout
        validation: dict[str, object] = {"ok": False, "issues": ["not checked"]}
        seed_status: dict[str, object] | None = None
        leech_status: dict[str, object] | None = None
        while time.monotonic() < deadline:
            validation = _validate_download_set(leech_payloads, source_payload)
            if validation["ok"]:
                summary["ok"] = True
                if scenario.leech_client == CLIENT_LIBTORRENT:
                    leech_status = _wait_for_libtorrent_seed_statuses(
                        active_leech_artifacts,
                        "leech",
                        timeout_secs=10,
                    )
                else:
                    leech_status = _wait_for_superseedr_counter_at_least(
                        ss_leech_share_root,
                        role="leech",
                        field="session_total_downloaded",
                        minimum=source_payload_size,
                        timeout_secs=10,
                    )

                if scenario.seed_client == CLIENT_LIBTORRENT:
                    seed_status = _wait_for_libtorrent_total_counter_at_least(
                        active_seed_artifacts,
                        "seed",
                        "total_upload",
                        minimum=source_payload_size,
                        timeout_secs=10,
                    )
                else:
                    seed_status = _wait_for_superseedr_counter_at_least(
                        ss_seed_share_root,
                        role="seed",
                        field="session_total_uploaded",
                        minimum=source_payload_size * max(1, active_leech_count),
                        timeout_secs=10,
                    )
                break
            time.sleep(1)

        final_seed_status = (
            seed_status
            if seed_status is not None
            else (
                _read_superseedr_status(ss_seed_share_root, "seed")
                if scenario.seed_client == CLIENT_SUPERSEEDR
                else _read_libtorrent_statuses(active_seed_artifacts, "seed")
            )
        )
        final_leech_status = (
            leech_status
            if leech_status is not None
            else (
                _read_superseedr_status(ss_leech_share_root, "leech")
                if scenario.leech_client == CLIENT_SUPERSEEDR
                else _read_libtorrent_statuses(active_leech_artifacts, "leech")
            )
        )
        transport_validation = _validate_superseedr_transport_observations(
            scenario,
            final_seed_status,
            final_leech_status,
        )
        if not transport_validation["ok"]:
            summary["ok"] = False

        summary.update(
            {
                "duration_secs": round(time.monotonic() - started_at, 3),
                "validation": validation,
                "transport_validation": transport_validation,
                "seed_status": final_seed_status,
                "leech_status": final_leech_status,
            }
        )
        return summary
    finally:
        (logs_root / "compose_ps.txt").write_text(compose.ps(), encoding="utf-8")
        for service in active_services:
            (logs_root / f"{service}.log").write_text(
                compose.logs(service, tail=1000),
                encoding="utf-8",
            )
        _write_json(run_root / "summary.json", summary)
        compose.down()


def _scenario_path(name: str) -> Path:
    return SCENARIOS_ROOT / f"{name}.json"


def _load_scenario(name: str) -> LabScenario:
    path = _scenario_path(name)
    if not path.exists():
        raise ValueError(f"Unknown libtorrent lab scenario: {name}")
    return LabScenario.from_file(path)


def _short_result(summary: dict[str, object]) -> dict[str, object]:
    validation = summary.get("validation", {})
    transport_validation = summary.get("transport_validation", {})
    return {
        "run_id": summary.get("run_id", ""),
        "scenario": summary.get("scenario", ""),
        "ok": bool(summary.get("ok", False)),
        "duration_secs": summary.get("duration_secs", 0),
        "artifacts_dir": summary.get("artifacts_dir", ""),
        "validation_ok": validation.get("ok") if isinstance(validation, dict) else None,
        "validation_issues": validation.get("issues", []) if isinstance(validation, dict) else [],
        "transport_ok": (
            transport_validation.get("ok") if isinstance(transport_validation, dict) else None
        ),
        "transport_issues": (
            transport_validation.get("issues", [])
            if isinstance(transport_validation, dict)
            else []
        ),
    }


def _matrix_markdown(summary: dict[str, object]) -> str:
    lines = [
        f"# Libtorrent Lab Matrix: {summary['matrix']}",
        "",
        f"- Result: {'PASS' if summary['ok'] else 'FAIL'}",
        f"- Scenarios: {summary['scenario_count']}",
        f"- Attempts: {summary['attempt_count']}",
        f"- Passed: {summary['passed_attempts']}",
        f"- Failed: {summary['failed_attempts']}",
        f"- Repeat count: {summary['repeat_count']}",
        f"- Duration: {summary['duration_secs']}s",
        f"- Artifacts: `{summary['artifacts_dir']}`",
        "",
        "| Scenario | Iteration | Result | Duration | Artifacts |",
        "| --- | ---: | --- | ---: | --- |",
    ]
    for result in summary["results"]:
        status = "PASS" if result.get("ok") else "FAIL"
        duration = result.get("duration_secs", 0)
        lines.append(
            f"| {result['scenario']} | {result['iteration']} | {status} | "
            f"{duration}s | `{result.get('artifacts_dir', '')}` |"
        )
    return "\n".join(lines) + "\n"


def run_lab_matrix(
    *,
    matrix: str,
    run_id: str,
    timeout_secs: int | None = None,
    skip_build: bool = False,
    repeat: int = 1,
    fail_fast: bool = False,
    network_impairment: NetworkImpairment | None = None,
) -> dict[str, object]:
    if repeat < 1:
        raise ValueError("repeat must be at least 1")

    scenario_names = _scenario_names_for_matrix(matrix)
    paths = resolve_paths()
    matrix_root = paths.artifacts_root / "libtorrent_lab" / run_id
    matrix_root.mkdir(parents=True, exist_ok=True)
    impairment = network_impairment or NetworkImpairment()
    _validate_impairment(impairment)

    started_at = time.monotonic()
    results: list[dict[str, object]] = []
    libtorrent_image_built = False
    superseedr_image_built = False

    for iteration in range(1, repeat + 1):
        for scenario_name in scenario_names:
            scenario = _load_scenario(scenario_name)
            scenario_run_id = f"{run_id}_{scenario.name}_r{iteration}"
            needs_superseedr = CLIENT_SUPERSEEDR in {scenario.seed_client, scenario.leech_client}
            effective_skip_build = skip_build or (
                libtorrent_image_built and (superseedr_image_built or not needs_superseedr)
            )
            try:
                scenario_summary = run_lab_scenario(
                    scenario=scenario,
                    run_id=scenario_run_id,
                    timeout_secs=timeout_secs,
                    skip_build=effective_skip_build,
                    network_impairment=impairment,
                )
                libtorrent_image_built = True
                if needs_superseedr:
                    superseedr_image_built = True
                result = {
                    **_short_result(scenario_summary),
                    "iteration": iteration,
                }
            except Exception as exc:
                result = {
                    "run_id": scenario_run_id,
                    "scenario": scenario.name,
                    "iteration": iteration,
                    "ok": False,
                    "duration_secs": 0,
                    "artifacts_dir": str(paths.artifacts_root / "libtorrent_lab" / scenario_run_id),
                    "error": f"{type(exc).__name__}: {exc}",
                    "validation_ok": None,
                    "validation_issues": [str(exc)],
                    "transport_ok": None,
                    "transport_issues": [],
                }
            results.append(result)
            print(
                "LIBTORRENT_LAB_MATRIX_STEP "
                f"{'PASS' if result['ok'] else 'FAIL'} "
                f"matrix={matrix} scenario={scenario.name} iteration={iteration} "
                f"artifacts={result['artifacts_dir']}"
            )
            if fail_fast and not result["ok"]:
                break
        if fail_fast and results and not results[-1]["ok"]:
            break

    passed = sum(1 for result in results if result.get("ok") is True)
    failed = len(results) - passed
    summary: dict[str, object] = {
        "run_id": run_id,
        "matrix": matrix,
        "ok": failed == 0,
        "scenario_count": len(scenario_names),
        "attempt_count": len(results),
        "passed_attempts": passed,
        "failed_attempts": failed,
        "repeat_count": repeat,
        "fail_fast": fail_fast,
        "network_impairment": impairment.as_dict(),
        "duration_secs": round(time.monotonic() - started_at, 3),
        "artifacts_dir": str(matrix_root),
        "results": results,
    }
    _write_json(matrix_root / "matrix_summary.json", summary)
    (matrix_root / "matrix_summary.md").write_text(_matrix_markdown(summary), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Dockerized libtorrent lab scenarios")
    p.add_argument("--scenario", default="basic_ul_dl")
    p.add_argument("--matrix", choices=sorted(LAB_MATRIXES), default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--timeout-secs", type=int, default=0)
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--fail-fast", action="store_true")
    p.add_argument("--netem-delay-ms", type=int, default=0)
    p.add_argument("--netem-jitter-ms", type=int, default=0)
    p.add_argument("--netem-loss-pct", type=float, default=0.0)
    p.add_argument("--netem-duplicate-pct", type=float, default=0.0)
    p.add_argument("--netem-corrupt-pct", type=float, default=0.0)
    p.add_argument("--netem-reorder-pct", type=float, default=0.0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    impairment = NetworkImpairment(
        delay_ms=args.netem_delay_ms,
        jitter_ms=args.netem_jitter_ms,
        loss_pct=args.netem_loss_pct,
        duplicate_pct=args.netem_duplicate_pct,
        corrupt_pct=args.netem_corrupt_pct,
        reorder_pct=args.netem_reorder_pct,
    )
    _validate_impairment(impairment)
    if args.matrix:
        run_id = args.run_id or f"libtorrent_lab_matrix_{args.matrix}_{_utc_stamp()}"
        summary = run_lab_matrix(
            matrix=args.matrix,
            run_id=run_id,
            timeout_secs=args.timeout_secs or None,
            skip_build=args.skip_build,
            repeat=args.repeat,
            fail_fast=args.fail_fast,
            network_impairment=impairment,
        )
        print(
            "LIBTORRENT_LAB_MATRIX_RESULT "
            f"{'PASS' if summary['ok'] else 'FAIL'} artifacts={summary['artifacts_dir']}"
        )
        return 0 if summary["ok"] else 1

    scenario = _load_scenario(args.scenario)
    run_id = args.run_id or f"libtorrent_lab_{scenario.name}_{_utc_stamp()}"
    summary = run_lab_scenario(
        scenario=scenario,
        run_id=run_id,
        timeout_secs=args.timeout_secs or None,
        skip_build=args.skip_build,
        network_impairment=impairment,
    )
    print(f"LIBTORRENT_LAB_RESULT {'PASS' if summary['ok'] else 'FAIL'} artifacts={summary['artifacts_dir']}")
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
