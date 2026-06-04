#!/usr/bin/env python3
"""Small libtorrent peer process for Docker lab scenarios."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    import libtorrent as lt
except ImportError as exc:  # pragma: no cover - exercised inside Docker image
    raise SystemExit(f"failed to import libtorrent: {exc}") from exc


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _append_event(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True))
        f.write("\n")


def _libtorrent_version() -> str:
    version = getattr(lt, "__version__", None)
    if version:
        return str(version)
    version = getattr(lt, "version", None)
    if version:
        return str(version)
    return "unknown"


def _session(settings: dict[str, Any]) -> Any:
    try:
        return lt.session(settings)
    except TypeError:
        session = lt.session()
        session.apply_settings(settings)
        return session


def _status_payload(peer_id: str, role: str, handle: Any, started_at: float) -> dict[str, Any]:
    status = handle.status()
    is_seed = bool(handle.is_seed())
    return {
        "peer_id": peer_id,
        "role": role,
        "observed_at": int(time.time()),
        "uptime_secs": round(time.monotonic() - started_at, 3),
        "is_seed": is_seed,
        "state": str(getattr(status, "state", "unknown")),
        "progress": round(float(getattr(status, "progress", 0.0)), 6),
        "num_peers": int(getattr(status, "num_peers", 0)),
        "download_rate_bps": int(getattr(status, "download_rate", 0)),
        "upload_rate_bps": int(getattr(status, "upload_rate", 0)),
        "total_done": int(getattr(status, "total_done", 0)),
        "total_download": int(getattr(status, "total_download", 0)),
        "total_upload": int(getattr(status, "total_upload", 0)),
        "libtorrent_version": _libtorrent_version(),
    }


def _consume_alerts(session: Any, events_path: Path, peer_id: str) -> None:
    for alert in session.pop_alerts():
        _append_event(
            events_path,
            {
                "ts": int(time.time()),
                "peer_id": peer_id,
                "alert": type(alert).__name__,
                "message": str(alert),
            },
        )


def _load_config() -> dict[str, Any]:
    config_path = Path(os.environ.get("LIBTORRENT_LAB_CONFIG", "/config/peer.json"))
    return json.loads(config_path.read_text(encoding="utf-8"))


def main() -> int:
    config = _load_config()
    peer_id = str(config["peer_id"])
    role = str(config["role"])
    listen_port = int(config["listen_port"])
    torrent_path = str(config["torrent_path"])
    save_path = str(config["save_path"])
    timeout_secs = int(config.get("timeout_secs", 120))
    exit_when_seed = bool(config.get("exit_when_seed", role == "leech"))
    status_path = Path(config.get("status_path", "/artifacts/status.json"))
    events_path = Path(config.get("events_path", "/artifacts/events.jsonl"))

    settings = dict(config.get("settings", {}))
    settings.setdefault("listen_interfaces", f"0.0.0.0:{listen_port}")
    settings.setdefault("user_agent", f"superseedr-libtorrent-lab/{peer_id}")

    Path(save_path).mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()
    _append_event(
        events_path,
        {
            "ts": int(time.time()),
            "peer_id": peer_id,
            "event": "starting",
            "role": role,
            "listen_port": listen_port,
            "torrent_path": torrent_path,
            "save_path": save_path,
            "libtorrent_version": _libtorrent_version(),
        },
    )

    session = _session(settings)
    torrent_info = lt.torrent_info(torrent_path)
    handle = session.add_torrent({"ti": torrent_info, "save_path": save_path})
    handle.force_reannounce()

    last_status_write = 0.0
    deadline = started_at + timeout_secs
    while time.monotonic() < deadline:
        session.wait_for_alert(500)
        _consume_alerts(session, events_path, peer_id)

        now = time.monotonic()
        if now - last_status_write >= 1.0:
            last_status_write = now
            payload = _status_payload(peer_id, role, handle, started_at)
            _write_json(status_path, payload)
            _append_event(
                events_path,
                {
                    "ts": int(time.time()),
                    "peer_id": peer_id,
                    "event": "status",
                    "status": payload,
                },
            )
            if exit_when_seed and payload["is_seed"]:
                _append_event(
                    events_path,
                    {
                        "ts": int(time.time()),
                        "peer_id": peer_id,
                        "event": "complete",
                        "status": payload,
                    },
                )
                return 0

    payload = _status_payload(peer_id, role, handle, started_at)
    _write_json(status_path, payload)
    _append_event(
        events_path,
        {
            "ts": int(time.time()),
            "peer_id": peer_id,
            "event": "timeout",
            "timeout_secs": timeout_secs,
            "status": payload,
        },
    )
    return 2 if exit_when_seed else 0


if __name__ == "__main__":
    sys.exit(main())
