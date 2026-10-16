"""Single-owner, Unix-socket service for the resident embedding provider."""

from __future__ import annotations

import argparse
import base64
import errno
import fcntl
import json
import os
import signal
import socket
import stat
import struct
import sys
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from workbay_handoff_mcp.embeddings.provider import EMBED_SUB_BATCH_SIZE, EmbeddingProvider

ENV_LOAD_TIMEOUT_SECONDS = "WORKBAY_EMBED_SIDECAR_LOAD_TIMEOUT_SECONDS"
ENV_IDLE_SECONDS = "WORKBAY_EMBED_SIDECAR_IDLE_SECONDS"
DEFAULT_LOAD_TIMEOUT_SECONDS = 60.0
DEFAULT_IDLE_SECONDS = 900.0
_HEADER = struct.Struct("!I")
_MAX_FRAME_BYTES = 16 * 1024 * 1024
_ACCEPT_TIMEOUT_SECONDS = 0.2
_CONNECTION_TIMEOUT_SECONDS = 60.0


class SidecarStartupError(RuntimeError):
    """A machine-readable reason the resident process could not start."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class _LockHeld(Exception):
    pass


def resolve_state_dir(state_dir: str | Path | None = None) -> Path:
    """Resolve the shared state path used by clients and the sidecar process."""
    if state_dir is None:
        state_dir = os.environ.get("WORKBAY_HANDOFF_STATE_DIR") or (Path.cwd() / ".task-state")
    return Path(state_dir).expanduser().resolve()


def _positive_seconds(raw: object, default: float) -> float:
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        return default
    if not np.isfinite(seconds) or seconds <= 0:
        return default
    return seconds


def resolve_load_timeout_seconds(env: Mapping[str, str] | None = None) -> float:
    source = os.environ if env is None else env
    return _positive_seconds(source.get(ENV_LOAD_TIMEOUT_SECONDS), DEFAULT_LOAD_TIMEOUT_SECONDS)


def resolve_idle_seconds(env: Mapping[str, str] | None = None) -> float:
    source = os.environ if env is None else env
    return _positive_seconds(source.get(ENV_IDLE_SECONDS), DEFAULT_IDLE_SECONDS)


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    received = 0
    while received < size:
        chunk = connection.recv(size - received)
        if not chunk:
            raise ConnectionError("incomplete frame")
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def _read_frame(connection: socket.socket) -> bytes:
    header = _recv_exact(connection, _HEADER.size)
    (size,) = _HEADER.unpack(header)
    if size <= 0 or size > _MAX_FRAME_BYTES:
        raise ValueError("invalid frame size")
    return _recv_exact(connection, size)


def _write_frame(connection: socket.socket, payload: bytes) -> None:
    if not payload or len(payload) > _MAX_FRAME_BYTES:
        raise ValueError("invalid frame size")
    connection.sendall(_HEADER.pack(len(payload)) + payload)


def _load_provider() -> EmbeddingProvider:
    provider = EmbeddingProvider.from_env()
    if provider is None:
        raise SidecarStartupError("provider_unconfigured")
    return provider


class EmbeddingSidecar:
    """Serve embed, ping, and stats requests serially from one provider."""

    def __init__(
        self,
        state_dir: str | Path,
        *,
        provider: Any | None = None,
        idle_seconds: float | None = None,
    ) -> None:
        self.state_dir = resolve_state_dir(state_dir)
        self.embedding_dir = self.state_dir / "embeddings"
        self.socket_path = self.embedding_dir / "embed.sock"
        self.lock_path = self.embedding_dir / "sidecar.lock"
        self._provided_provider = provider
        self._provider: Any | None = None
        self.idle_seconds = _positive_seconds(idle_seconds, resolve_idle_seconds())
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._listener: socket.socket | None = None
        self._request_count = 0
        self._started_at: float | None = None

    @property
    def ready(self) -> bool:
        return self._ready_event.is_set()

    def stop(self) -> None:
        self._stop_event.set()

    def _prepare_provider(self) -> None:
        provider = self._provided_provider if self._provided_provider is not None else _load_provider()
        load = getattr(provider, "_ensure_loaded", None)
        if callable(load):
            try:
                load()
            except Exception as exc:
                raise SidecarStartupError("provider_load_failed") from exc
        self._provider = provider

    def _identity(self) -> tuple[str, int]:
        provider = self._provider
        if provider is None:
            raise SidecarStartupError("provider_unavailable")
        model_id = getattr(provider, "model_id", None)
        dim = getattr(provider, "dim", None)
        if not isinstance(model_id, str) or not model_id:
            raise SidecarStartupError("provider_invalid_model_id")
        if isinstance(dim, bool) or not isinstance(dim, int) or dim <= 0:
            raise SidecarStartupError("provider_invalid_dimension")
        return model_id, dim

    def _unlink_socket(self) -> None:
        try:
            info = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISSOCK(info.st_mode):
            self.socket_path.unlink(missing_ok=True)

    def _handle_request(self, payload: bytes) -> dict[str, object]:
        try:
            request = json.loads(payload)
            if not isinstance(request, dict):
                raise ValueError("request must be an object")
            op = request.get("op")
            if op == "ping":
                return self._success(op="ping")
            if op == "stats":
                return self._success(
                    op="stats",
                    requests=self._request_count,
                    uptime_seconds=max(0.0, time.monotonic() - (self._started_at or time.monotonic())),
                )
            if op != "embed":
                return self._failure("unknown_operation")
            texts = request.get("texts")
            if not isinstance(texts, list) or any(not isinstance(text, str) for text in texts):
                return self._failure("invalid_texts")
            vectors = self._embed(texts)
            model_id, dim = self._identity()
            vectors_b64 = [base64.b64encode(row.tobytes()).decode("ascii") for row in vectors]
            self._request_count += 1
            return {
                "ok": True,
                "model_id": model_id,
                "dim": dim,
                "vectors_b64": vectors_b64,
            }
        except (ValueError, TypeError, json.JSONDecodeError):
            return self._failure("invalid_request")
        except Exception:
            return self._failure("inference_failed")

    def _success(self, *, op: str, **extra: object) -> dict[str, object]:
        model_id, dim = self._identity()
        return {"ok": True, "op": op, "model_id": model_id, "dim": dim, **extra}

    def _failure(self, reason: str) -> dict[str, object]:
        model_id, dim = self._identity()
        return {"ok": False, "model_id": model_id, "dim": dim, "reason": reason}

    def _embed(self, texts: list[str]) -> list[np.ndarray]:
        provider = self._provider
        if provider is None:
            raise SidecarStartupError("provider_unavailable")
        _, dim = self._identity()
        vectors: list[np.ndarray] = []
        for start in range(0, len(texts), EMBED_SUB_BATCH_SIZE):
            batch = texts[start : start + EMBED_SUB_BATCH_SIZE]
            output = np.asarray(provider.embed(batch), dtype=np.dtype("<f4"))
            if output.shape != (len(batch), dim):
                raise ValueError("provider returned invalid vector geometry")
            output = np.ascontiguousarray(output, dtype=np.dtype("<f4"))
            vectors.extend(output)
        return vectors

    def _serve_connection(self, connection: socket.socket) -> None:
        connection.settimeout(_CONNECTION_TIMEOUT_SECONDS)
        try:
            request_payload = _read_frame(connection)
            response = self._handle_request(request_payload)
        except (OSError, ValueError, ConnectionError, json.JSONDecodeError):
            response = self._failure("invalid_request")
        try:
            response_payload = json.dumps(response, separators=(",", ":")).encode("utf-8")
            _write_frame(connection, response_payload)
        except OSError:
            # The caller may have reached its socket deadline and closed the fd.
            return

    def serve_forever(self) -> bool:
        """Hold the host lock until idle or stopped; false means another owner exists."""
        self.embedding_dir.mkdir(parents=True, exist_ok=True)
        lock_file = self.lock_path.open("a+b")
        owns_lock = False
        try:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise _LockHeld from exc
            owns_lock = True
            self._unlink_socket()
            self._prepare_provider()
            self._identity()
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._listener = listener
            listener.settimeout(_ACCEPT_TIMEOUT_SECONDS)
            listener.bind(str(self.socket_path))
            listener.listen(8)
            self._started_at = time.monotonic()
            last_activity = self._started_at
            self._ready_event.set()
            while not self._stop_event.is_set():
                if time.monotonic() - last_activity >= self.idle_seconds:
                    break
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                with connection:
                    last_activity = time.monotonic()
                    self._serve_connection(connection)
                    last_activity = time.monotonic()
            return True
        except _LockHeld:
            return False
        finally:
            self._ready_event.clear()
            listener = self._listener
            self._listener = None
            if listener is not None:
                listener.close()
            if owns_lock:
                self._unlink_socket()
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()


def _install_signal_handlers(server: EmbeddingSidecar) -> tuple[bool, dict[int, Any]]:
    if threading.current_thread() is not threading.main_thread():
        return False, {}
    previous: dict[int, Any] = {}

    def request_stop(_signum: int, _frame: object) -> None:
        server.stop()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.signal(signum, request_stop)
    return True, previous


def _restore_signal_handlers(installed: bool, previous: Mapping[int, Any]) -> None:
    if installed:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resident embedding model sidecar")
    parser.add_argument("--state-dir", required=True, help="shared Workbay state directory")
    args = parser.parse_args(argv)
    server = EmbeddingSidecar(args.state_dir)
    installed, previous = _install_signal_handlers(server)
    try:
        server.serve_forever()
        return 0
    except SidecarStartupError as exc:
        print(f"embedding sidecar refused startup: {exc.reason}", file=sys.stderr)
        return 1
    except OSError as exc:
        reason = (
            "socket_unavailable" if exc.errno in (errno.EADDRINUSE, errno.EACCES, errno.ENOENT) else "startup_failed"
        )
        print(f"embedding sidecar refused startup: {reason}", file=sys.stderr)
        return 1
    finally:
        _restore_signal_handlers(installed, previous)


if __name__ == "__main__":
    raise SystemExit(main())
