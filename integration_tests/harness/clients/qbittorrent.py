from __future__ import annotations

import http.cookiejar
import json
import re
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any
from urllib import error as url_error

from integration_tests.harness.clients.base import ClientAdapter
from integration_tests.harness.docker_ctl import DockerCompose


class QBittorrentAdapter(ClientAdapter):
    def __init__(
        self,
        compose: DockerCompose | None = None,
        service_name: str = "qbittorrent",
        base_url: str = "http://127.0.0.1:18080",
        username: str = "admin",
        password: str = "adminadmin",
        auth_timeout_secs: int = 60,
    ) -> None:
        self.compose = compose
        self.service_name = service_name
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.auth_timeout_secs = auth_timeout_secs
        self.poll_interval_secs = 1.0
        self._cookie_jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self._cookie_jar))
        self._authenticated = False

    @staticmethod
    def _extract_temporary_password(logs: str) -> str | None:
        # linuxserver/qbittorrent emits this when no saved web UI password exists.
        pattern = r"temporary password[^:\n]*:\s*(\S+)"
        match = re.search(pattern, logs, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def _login_once(self, password: str) -> bool:
        payload = urllib.parse.urlencode({"username": self.username, "password": password}).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}/api/v2/auth/login",
            data=payload,
            method="POST",
        )
        try:
            with self._opener.open(request, timeout=5) as response:
                body = response.read().decode("utf-8", errors="replace").strip()
                return response.status in (200, 204) and body in ("", "Ok.")
        except url_error.HTTPError as exc:
            if exc.code in (401, 403):
                return False
            raise

    def authenticate(self) -> None:
        deadline = time.monotonic() + self.auth_timeout_secs
        attempts: list[str] = [self.password]
        temp_password: str | None = None
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            if not attempts:
                if self.compose is not None:
                    discovered_temp = self._extract_temporary_password(self.compose.logs(self.service_name, tail=200))
                    if discovered_temp:
                        temp_password = discovered_temp

                # Try configured password and known temporary password until timeout.
                attempts.append(self.password)
                if temp_password and temp_password != self.password:
                    attempts.append(temp_password)

            current_password = attempts.pop(0)
            try:
                if self._login_once(current_password):
                    self._authenticated = True
                    return
            except Exception as exc:
                last_error = exc

            time.sleep(1)

        raise RuntimeError(
            f"Failed to authenticate to qBittorrent at {self.base_url} as {self.username}"
        ) from last_error

    def start(self) -> None:
        if self.compose is not None:
            self.compose.up([self.service_name], no_build=True)
        self.authenticate()

    def stop(self) -> None:
        if self.compose is not None:
            self.compose.run(["stop", self.service_name], check=False)

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 10,
    ) -> tuple[int, bytes]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers=headers or {},
        )
        with self._opener.open(request, timeout=timeout) as response:
            return response.status, response.read()

    def _request_json(self, path: str) -> Any:
        status, body = self._request(path, method="GET")
        if status != 200:
            raise RuntimeError(f"qBittorrent request failed path={path} status={status}")
        return json.loads(body.decode("utf-8", errors="replace"))

    @staticmethod
    def _build_multipart_form(
        fields: dict[str, str],
        file_field: str,
        filename: str,
        file_bytes: bytes,
    ) -> tuple[bytes, str]:
        boundary = f"----interop-{uuid.uuid4().hex}"
        parts: list[bytes] = []
        for key, value in fields.items():
            parts.extend(
                [
                    f"--{boundary}\r\n".encode("utf-8"),
                    f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                    value.encode("utf-8"),
                    b"\r\n",
                ]
            )

        parts.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                (
                    f'Content-Disposition: form-data; name="{file_field}"; '
                    f'filename="{filename}"\r\n'
                ).encode("utf-8"),
                b"Content-Type: application/x-bittorrent\r\n\r\n",
                file_bytes,
                b"\r\n",
                f"--{boundary}--\r\n".encode("utf-8"),
            ]
        )
        content_type = f"multipart/form-data; boundary={boundary}"
        return b"".join(parts), content_type

    @staticmethod
    def _torrent_add_succeeded(status: int, response_text: str) -> bool:
        if status != 200:
            return False
        if response_text in ("", "Ok."):
            return True

        try:
            payload = json.loads(response_text)
        except json.JSONDecodeError:
            return False

        if not isinstance(payload, dict):
            return False

        try:
            failure_count = int(payload.get("failure_count", 1))
            success_count = int(payload.get("success_count", 0))
        except (TypeError, ValueError):
            return False

        return failure_count == 0 and success_count > 0

    def _list_torrents(self) -> list[dict[str, Any]]:
        status, body = self._request("/api/v2/torrents/info", method="GET")
        if status != 200:
            raise RuntimeError(f"Failed to list qBittorrent torrents (status={status})")

        payload = json.loads(body.decode("utf-8", errors="replace"))
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected qBittorrent torrents/info payload shape")
        return payload

    def add_torrent(self, torrent_path: str, download_dir: str) -> None:
        if not self._authenticated:
            self.authenticate()
        path = Path(torrent_path)
        if not path.exists():
            raise FileNotFoundError(f"Torrent file not found: {torrent_path}")

        payload, content_type = self._build_multipart_form(
            fields={
                "savepath": download_dir,
                "paused": "false",
                "skip_checking": "false",
                "autoTMM": "false",
            },
            file_field="torrents",
            filename=path.name,
            file_bytes=path.read_bytes(),
        )
        status, body = self._request(
            "/api/v2/torrents/add",
            method="POST",
            body=payload,
            headers={"Content-Type": content_type},
        )
        response_text = body.decode("utf-8", errors="replace").strip()
        if not self._torrent_add_succeeded(status, response_text):
            raise RuntimeError(
                f"Failed to add torrent to qBittorrent: status={status} body={response_text!r}"
            )

    def set_force_start(self, info_hash: str, enabled: bool = True) -> None:
        if not self._authenticated:
            self.authenticate()

        payload = urllib.parse.urlencode(
            {
                "hashes": info_hash,
                "value": "true" if enabled else "false",
            }
        ).encode("utf-8")
        status, body = self._request(
            "/api/v2/torrents/setForceStart",
            method="POST",
            body=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response_text = body.decode("utf-8", errors="replace").strip()
        if status != 200 or response_text:
            raise RuntimeError(
                f"Failed to set force-start on qBittorrent torrent {info_hash}: "
                f"status={status} body={response_text!r}"
            )

    def wait_for_download(self, expected_manifest: dict, timeout_secs: int) -> bool:
        _ = expected_manifest
        if not self._authenticated:
            self.authenticate()
        deadline = time.monotonic() + timeout_secs

        while time.monotonic() < deadline:
            torrents = self._list_torrents()
            if torrents:
                has_error = any(str(t.get("state", "")).startswith("error") for t in torrents)
                if has_error:
                    return False

                all_complete = all(int(t.get("amount_left", 1)) == 0 for t in torrents)
                if all_complete:
                    return True
            time.sleep(self.poll_interval_secs)
        return False

    def collect_logs(self, dest_dir: Path) -> None:
        if self.compose is None:
            return
        dest_dir.mkdir(parents=True, exist_ok=True)
        logs = self.compose.logs(self.service_name, tail=1000)
        (dest_dir / f"{self.service_name}.log").write_text(logs, encoding="utf-8")

    def read_status(self) -> dict[str, Any]:
        try:
            torrents = self._list_torrents()
        except Exception as exc:
            return {
                "service": self.service_name,
                "status": "api_error",
                "error": str(exc),
                "observed_at": int(time.time()),
            }

        completed = sum(1 for t in torrents if int(t.get("amount_left", 1)) == 0)
        return {
            "service": self.service_name,
            "status": "ok",
            "observed_at": int(time.time()),
            "torrent_count": len(torrents),
            "completed_count": completed,
            "raw": torrents,
        }
