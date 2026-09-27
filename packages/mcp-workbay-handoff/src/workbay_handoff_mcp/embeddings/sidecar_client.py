"""Deadline-bounded client for the resident embedding sidecar."""

from __future__ import annotations

import base64
import json
import math
import re
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from workbay_handoff_mcp.embeddings.model_pin import EMBEDDING_DIM, MODEL_ID
from workbay_handoff_mcp.embeddings.sidecar import (
    _MAX_FRAME_BYTES,
    _read_frame,
    _write_frame,
    resolve_load_timeout_seconds,
    resolve_state_dir,
)

_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


@dataclass(frozen=True)
class SidecarUnavailable:
    """Typed, machine-readable result for a sidecar refusal or transport failure."""

    reason: str


class EmbeddingClient:
    """Transport requests to one resident sidecar without abandoned worker threads."""

    def __init__(
        self,
        state_dir: str | Path | None = None,
        *,
        model_id: str = MODEL_ID,
        dim: int = EMBEDDING_DIM,
        load_timeout_seconds: float | None = None,
    ) -> None:
        if not isinstance(model_id, str) or not model_id:
            raise ValueError("model_id must be a non-empty string")
        if isinstance(dim, bool) or not isinstance(dim, int) or dim <= 0:
            raise ValueError("dim must be a positive integer")
        if load_timeout_seconds is not None and (not math.isfinite(load_timeout_seconds) or load_timeout_seconds <= 0):
            raise ValueError("load_timeout_seconds must be finite and positive")
        self.state_dir = resolve_state_dir(state_dir)
        self.embedding_dir = self.state_dir / "embeddings"
        self.socket_path = self.embedding_dir / "embed.sock"
        self.log_path = self.embedding_dir / "sidecar.log"
        self.model_id = model_id
        self.dim = dim
        self.load_timeout_seconds = (
            resolve_load_timeout_seconds() if load_timeout_seconds is None else float(load_timeout_seconds)
        )
        self._startup_lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("deadline")
        return remaining

    def _exchange(self, request: dict[str, object], timeout_seconds: float) -> dict[str, object]:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise TimeoutError("deadline")
        deadline = time.monotonic() + timeout_seconds
        payload = json.dumps(request, separators=(",", ":")).encode("utf-8")
        if len(payload) <= 0 or len(payload) > _MAX_FRAME_BYTES:
            raise ValueError("request_too_large")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(self._remaining(deadline))
            connection.connect(str(self.socket_path))
            connection.settimeout(self._remaining(deadline))
            _write_frame(connection, payload)
            connection.settimeout(self._remaining(deadline))
            response_payload = _read_frame(connection)
        response = json.loads(response_payload)
        if not isinstance(response, dict):
            raise ValueError("invalid_response")
        return response

    def _metadata_error(self, response: dict[str, object]) -> SidecarUnavailable | None:
        model_id = response.get("model_id")
        if not isinstance(model_id, str):
            return SidecarUnavailable("invalid_response")
        if model_id != self.model_id:
            return SidecarUnavailable("model_mismatch")
        dim = response.get("dim")
        if isinstance(dim, bool) or not isinstance(dim, int):
            return SidecarUnavailable("invalid_response")
        if dim != self.dim:
            return SidecarUnavailable("dimension_mismatch")
        return None

    def _attempt(
        self, request: dict[str, object], timeout_seconds: float
    ) -> tuple[dict[str, object] | None, SidecarUnavailable | None]:
        try:
            response = self._exchange(request, timeout_seconds)
        except (socket.timeout, TimeoutError):
            return None, SidecarUnavailable("deadline")
        except FileNotFoundError:
            return None, SidecarUnavailable("socket_missing")
        except ConnectionRefusedError:
            return None, SidecarUnavailable("connection_refused")
        except OSError:
            return None, SidecarUnavailable("unavailable")
        except (ValueError, json.JSONDecodeError):
            return None, SidecarUnavailable("invalid_response")
        metadata_error = self._metadata_error(response)
        if metadata_error is not None:
            return None, metadata_error
        if response.get("ok") is not True:
            reason = response.get("reason")
            if isinstance(reason, str) and _REASON_RE.fullmatch(reason) is not None:
                return None, SidecarUnavailable(reason)
            return None, SidecarUnavailable("request_refused")
        return response, None

    def _launch(self) -> SidecarUnavailable | None:
        command = [
            sys.executable,
            "-m",
            "workbay_handoff_mcp.embeddings.sidecar",
            "--state-dir",
            str(self.state_dir),
        ]
        try:
            self.embedding_dir.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("ab") as log_file:
                self._process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=log_file,
                    start_new_session=True,
                    close_fds=True,
                )
        except OSError:
            return SidecarUnavailable("spawn_failed")
        return None

    def _ping_attempt(self, timeout_seconds: float) -> SidecarUnavailable | None:
        _response, error = self._attempt({"op": "ping"}, timeout_seconds)
        return error

    def ensure_started(self) -> SidecarUnavailable | None:
        """Start one detached process if needed and wait a bounded load interval for ping."""
        with self._startup_lock:
            initial = self._ping_attempt(min(0.25, self.load_timeout_seconds))
            if initial is None:
                return None
            if initial.reason in ("model_mismatch", "dimension_mismatch", "invalid_response"):
                return initial
            launch_error = self._launch()
            if launch_error is not None:
                return launch_error
            deadline = time.monotonic() + self.load_timeout_seconds
            while time.monotonic() < deadline:
                process = self._process
                if process is not None and process.poll() not in (None, 0):
                    return SidecarUnavailable("startup_failed")
                remaining = deadline - time.monotonic()
                ping_error = self._ping_attempt(min(0.25, remaining))
                if ping_error is None:
                    return None
                if ping_error.reason in ("model_mismatch", "dimension_mismatch", "invalid_response"):
                    return ping_error
                time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
            process = self._process
            self._stop_stuck_start(process)
            return SidecarUnavailable("load_deadline")

    @staticmethod
    def _stop_stuck_start(process: subprocess.Popen[bytes] | None) -> None:
        """Reap a child that still owns the lock after missing its load deadline."""
        if process is None or process.poll() is not None:
            return
        try:
            process.terminate()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    pass
        except OSError:
            return

    def _request(
        self, request: dict[str, object], deadline_seconds: float
    ) -> tuple[dict[str, object] | None, SidecarUnavailable | None]:
        response, error = self._attempt(request, deadline_seconds)
        if error is None or error.reason not in ("socket_missing", "connection_refused", "unavailable"):
            return response, error
        startup_error = self.ensure_started()
        if startup_error is not None:
            return None, startup_error
        # Inference time begins only after the service has answered its ready ping.
        return self._attempt(request, deadline_seconds)

    def embed(
        self,
        texts: list[str],
        *,
        deadline_seconds: float,
    ) -> list[np.ndarray] | SidecarUnavailable:
        if not isinstance(texts, list) or any(not isinstance(text, str) for text in texts):
            return SidecarUnavailable("invalid_texts")
        if not math.isfinite(deadline_seconds) or deadline_seconds <= 0:
            return SidecarUnavailable("invalid_deadline")
        response, error = self._request({"op": "embed", "texts": texts}, deadline_seconds)
        if error is not None or response is None:
            return error or SidecarUnavailable("unavailable")
        vectors_b64 = response.get("vectors_b64")
        if not isinstance(vectors_b64, list) or len(vectors_b64) != len(texts):
            return SidecarUnavailable("invalid_response")
        vectors: list[np.ndarray] = []
        try:
            expected_bytes = self.dim * np.dtype("<f4").itemsize
            for value in vectors_b64:
                if not isinstance(value, str):
                    return SidecarUnavailable("invalid_response")
                raw = base64.b64decode(value, validate=True)
                if len(raw) != expected_bytes:
                    return SidecarUnavailable("invalid_response")
                vector = np.frombuffer(raw, dtype=np.dtype("<f4"))
                if vector.shape != (self.dim,):
                    return SidecarUnavailable("invalid_response")
                vectors.append(vector.copy())
        except (ValueError, TypeError):
            return SidecarUnavailable("invalid_response")
        return vectors

    def ping(self, *, timeout_seconds: float = 1.0) -> bool | SidecarUnavailable:
        response, error = self._attempt({"op": "ping"}, timeout_seconds)
        if error is not None:
            return error
        if response is None:
            return SidecarUnavailable("unavailable")
        return True

    def stats(self, *, timeout_seconds: float = 1.0) -> dict[str, object] | SidecarUnavailable:
        response, error = self._attempt({"op": "stats"}, timeout_seconds)
        if error is not None:
            return error
        if response is None:
            return SidecarUnavailable("unavailable")
        return response
