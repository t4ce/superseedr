from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from integration_tests.harness.clients.qbittorrent import QBittorrentAdapter
from integration_tests.harness.clients.transmission import TransmissionAdapter


def test_qbittorrent_temporary_password_extraction() -> None:
    logs = "foo\ntemporary password is provided for this session: token123\nbar"
    assert QBittorrentAdapter._extract_temporary_password(logs) == "token123"


def test_qbittorrent_temporary_password_extraction_case_insensitive() -> None:
    logs = "A temporary password is provided for this session: TokenABC"
    assert QBittorrentAdapter._extract_temporary_password(logs) == "TokenABC"


def test_qbittorrent_temporary_password_extraction_missing() -> None:
    logs = "no password line in these logs"
    assert QBittorrentAdapter._extract_temporary_password(logs) is None


class _QbittorrentLoginResponse:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body

    def __enter__(self) -> "_QbittorrentLoginResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_qbittorrent_login_accepts_legacy_ok_body(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = QBittorrentAdapter()

    def _open(_request: object, timeout: int = 5) -> _QbittorrentLoginResponse:
        assert timeout == 5
        return _QbittorrentLoginResponse(200, b"Ok.")

    monkeypatch.setattr(adapter._opener, "open", _open)
    assert adapter._login_once("password") is True


def test_qbittorrent_login_accepts_empty_204(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = QBittorrentAdapter()

    def _open(_request: object, timeout: int = 5) -> _QbittorrentLoginResponse:
        assert timeout == 5
        return _QbittorrentLoginResponse(204, b"")

    monkeypatch.setattr(adapter._opener, "open", _open)
    assert adapter._login_once("password") is True


def test_qbittorrent_login_rejects_failed_200(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = QBittorrentAdapter()

    def _open(_request: object, timeout: int = 5) -> _QbittorrentLoginResponse:
        assert timeout == 5
        return _QbittorrentLoginResponse(200, b"Fails.")

    monkeypatch.setattr(adapter._opener, "open", _open)
    assert adapter._login_once("password") is False


def test_qbittorrent_authenticate_falls_back_to_temp_password(monkeypatch: pytest.MonkeyPatch) -> None:
    class _ComposeStub:
        def logs(self, _service: str, tail: int = 200) -> str:
            _ = tail
            return "temporary password is provided for this session: temp-pass"

    adapter = QBittorrentAdapter(
        compose=cast(Any, _ComposeStub()),
        password="wrong-pass",
        auth_timeout_secs=3,
    )
    attempts: list[str] = []

    def _fake_login(password: str) -> bool:
        attempts.append(password)
        return password == "temp-pass"

    monkeypatch.setattr(adapter, "_login_once", _fake_login)
    adapter.authenticate()
    assert attempts[0] == "wrong-pass"
    assert "temp-pass" in attempts


def test_qbittorrent_authenticate_retries_temp_password_until_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    class _ComposeStub:
        def logs(self, _service: str, tail: int = 200) -> str:
            _ = tail
            return "temporary password is provided for this session: temp-pass"

    adapter = QBittorrentAdapter(
        compose=cast(Any, _ComposeStub()),
        password="wrong-pass",
        auth_timeout_secs=5,
    )
    attempts: list[str] = []
    temp_attempts = 0

    def _fake_login(password: str) -> bool:
        nonlocal temp_attempts
        attempts.append(password)
        if password == "temp-pass":
            temp_attempts += 1
            return temp_attempts >= 2
        return False

    monkeypatch.setattr(adapter, "_login_once", _fake_login)
    monkeypatch.setattr("integration_tests.harness.clients.qbittorrent.time.sleep", lambda _secs: None)
    adapter.authenticate()
    assert temp_attempts >= 2
    assert attempts.count("temp-pass") >= 2


def test_qbittorrent_build_multipart_form_includes_file_and_fields() -> None:
    payload, content_type = QBittorrentAdapter._build_multipart_form(
        fields={"savepath": "/downloads/leech", "paused": "false"},
        file_field="torrents",
        filename="sample.torrent",
        file_bytes=b"torrent-bytes",
    )
    assert content_type.startswith("multipart/form-data; boundary=")
    assert b'name="savepath"' in payload
    assert b"/downloads/leech" in payload
    assert b'name="torrents"; filename="sample.torrent"' in payload
    assert b"torrent-bytes" in payload


def test_qbittorrent_add_torrent_posts_to_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    torrent = tmp_path / "sample.torrent"
    torrent.write_bytes(b"fake-torrent")
    adapter = QBittorrentAdapter()
    adapter._authenticated = True

    captured: dict[str, object] = {}

    def _fake_request(path: str, **kwargs: object) -> tuple[int, bytes]:
        captured["path"] = path
        captured.update(kwargs)
        return 200, b"Ok."

    monkeypatch.setattr(adapter, "_request", _fake_request)
    adapter.add_torrent(str(torrent), "/downloads/leech")
    assert captured["path"] == "/api/v2/torrents/add"
    assert captured["method"] == "POST"
    assert isinstance(captured["body"], bytes)
    assert isinstance(captured["headers"], dict)
    assert "multipart/form-data" in str(captured["headers"]["Content-Type"])


def test_qbittorrent_add_torrent_accepts_json_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    torrent = tmp_path / "sample.torrent"
    torrent.write_bytes(b"fake-torrent")
    adapter = QBittorrentAdapter()
    adapter._authenticated = True

    monkeypatch.setattr(
        adapter,
        "_request",
        lambda _path, **_kwargs: (
            200,
            b'{"added_torrent_ids":["abc"],"failure_count":0,"pending_count":0,"success_count":1}',
        ),
    )

    adapter.add_torrent(str(torrent), "/downloads/leech")


def test_qbittorrent_add_torrent_rejects_json_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    torrent = tmp_path / "sample.torrent"
    torrent.write_bytes(b"fake-torrent")
    adapter = QBittorrentAdapter()
    adapter._authenticated = True

    monkeypatch.setattr(
        adapter,
        "_request",
        lambda _path, **_kwargs: (
            200,
            b'{"added_torrent_ids":[],"failure_count":1,"pending_count":0,"success_count":0}',
        ),
    )

    with pytest.raises(RuntimeError, match="Failed to add torrent"):
        adapter.add_torrent(str(torrent), "/downloads/leech")


def test_qbittorrent_wait_for_download_success(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = QBittorrentAdapter()
    adapter._authenticated = True
    snapshots = [
        [{"state": "downloading", "amount_left": 42}],
        [{"state": "uploading", "amount_left": 0}],
    ]

    def _fake_list_torrents() -> list[dict[str, int | str]]:
        return snapshots.pop(0) if snapshots else [{"state": "uploading", "amount_left": 0}]

    monkeypatch.setattr(adapter, "_list_torrents", _fake_list_torrents)
    monkeypatch.setattr("integration_tests.harness.clients.qbittorrent.time.sleep", lambda _secs: None)
    assert adapter.wait_for_download(expected_manifest={}, timeout_secs=2) is True


def test_qbittorrent_wait_for_download_error_state(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = QBittorrentAdapter()
    adapter._authenticated = True
    monkeypatch.setattr(
        adapter,
        "_list_torrents",
        lambda: [{"state": "error", "amount_left": 123}],
    )
    assert adapter.wait_for_download(expected_manifest={}, timeout_secs=2) is False


def test_transmission_add_torrent_sends_metainfo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    torrent = tmp_path / "sample.torrent"
    torrent.write_bytes(b"fake-transmission-torrent")
    adapter = TransmissionAdapter()

    captured: dict[str, object] = {}

    def _fake_rpc(method: str, arguments: dict[str, object] | None = None) -> dict[str, object]:
        captured["method"] = method
        captured["arguments"] = arguments or {}
        return {}

    monkeypatch.setattr(adapter, "_rpc", _fake_rpc)
    adapter.add_torrent(str(torrent), "/downloads/v1")

    assert captured["method"] == "torrent-add"
    args = captured["arguments"]
    assert isinstance(args, dict)
    assert args["download-dir"] == "/downloads/v1"
    assert args["paused"] is False
    assert isinstance(args["metainfo"], str)


def test_transmission_wait_for_download_success(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = TransmissionAdapter()
    snapshots = [
        [{"error": 0, "leftUntilDone": 100}],
        [{"error": 0, "leftUntilDone": 0}],
    ]

    def _fake_list_torrents() -> list[dict[str, int]]:
        return snapshots.pop(0) if snapshots else [{"error": 0, "leftUntilDone": 0}]

    monkeypatch.setattr(adapter, "_list_torrents", _fake_list_torrents)
    monkeypatch.setattr("integration_tests.harness.clients.transmission.time.sleep", lambda _secs: None)
    assert adapter.wait_for_download(expected_manifest={}, timeout_secs=2) is True


def test_transmission_wait_for_download_error_state(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = TransmissionAdapter()
    monkeypatch.setattr(adapter, "_list_torrents", lambda: [{"error": 3, "leftUntilDone": 500}])
    assert adapter.wait_for_download(expected_manifest={}, timeout_secs=2) is False
