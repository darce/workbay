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
from contextlib import ExitStack
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


_SUN_PATH_MAX = 100  # AF_UNIX sun_path usable bytes (macOS 104 incl. NUL)


def _check_socket_dir(info: os.stat_result, *, private: bool) -> None:
    """One trust predicate for every socket path and its ancestors."""
    if not stat.S_ISDIR(info.st_mode):
        raise RuntimeError("socket path component is not a directory")
    if private:
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise RuntimeError("socket directory is not owner-private")
    elif info.st_uid not in (0, os.geteuid()) or (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX):
        raise RuntimeError("untrusted socket directory ancestor")


def _open_socket_dir(path: Path, *, create: bool = False, private: bool = True) -> int:
    """Walk openat-style without following symlinks; return the verified fd.

    Existing permissions are never repaired. The caller owns the final fd;
    each preceding component stays open until the next has been checked.
    """
    path = path.absolute()
    if ".." in path.parts:
        raise RuntimeError("socket directory must not contain parent traversal")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path.anchor, flags)
    try:
        _check_socket_dir(os.fstat(fd), private=private and len(path.parts) == 1)
        for index, part in enumerate(path.parts[1:], start=1):
            if create:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            next_fd = os.open(part, flags, dir_fd=fd)
            try:
                _check_socket_dir(os.fstat(next_fd), private=private and index == len(path.parts) - 1)
            except BaseException:
                os.close(next_fd)
                raise
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


def _ensure_private_socket_dir(path: Path, *, strict: bool = True) -> None:
    fd = _open_socket_dir(path, create=True, private=strict)
    os.close(fd)


def _private_socket_base() -> Path:
    """Per-user base nobody else can write: XDG_RUNTIME_DIR, else ~/.cache.

    Trust is never derived from a shared TMPDIR. Every component is
    created 0700 and fd-verified; a candidate that fails verification is
    skipped, not repaired.
    """
    candidates: list[Path] = []
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        candidates.append(Path(xdg) / "workbay")
    candidates.append(Path.home() / ".cache" / "workbay" / "run")
    for base in candidates:
        try:
            if xdg and base == Path(xdg) / "workbay":
                # XDG itself is a private boundary, not merely an ancestor.
                fd = _open_socket_dir(Path(xdg))
                try:
                    if stat.S_IMODE(os.fstat(fd).st_mode) != 0o700:
                        raise RuntimeError("XDG runtime directory is not exactly 0700")
                finally:
                    os.close(fd)
            _ensure_private_socket_dir(base)
            return base
        except (OSError, RuntimeError):
            continue
    raise RuntimeError("no owner-private socket base available")


def resolve_socket_path(embedding_dir: Path) -> Path:
    """Socket path for ``embedding_dir``, shortened when AF_UNIX would refuse it.

    Deep checkouts / pytest tmp roots push ``state_dir/embeddings/embed.sock``
    past the sun_path limit and bind() fails with 'AF_UNIX path too long'.
    Relocate deterministically (client and server derive the same path) to
    a hashed, owner-private directory. The final encoded length is always
    validated: a deep TMPDIR must not reintroduce the same failure.
    """
    candidate = embedding_dir / "embed.sock"
    if len(str(candidate).encode()) <= _SUN_PATH_MAX:
        try:
            _ensure_private_socket_dir(embedding_dir)
            return candidate
        except (OSError, RuntimeError):
            pass  # untrusted/short-parent: fall through to relocated base
    import hashlib

    digest = hashlib.sha256(str(embedding_dir).encode()).hexdigest()[:16]
    private_dir = _private_socket_base() / f"wb-{digest}"
    sock = private_dir / "embed.sock"
    if len(str(sock).encode()) <= _SUN_PATH_MAX:
        _ensure_private_socket_dir(private_dir)
        return sock
    # Deep HOME: shortest verified fallback — a 0700 owner dir directly
    # under sticky /tmp (sticky bit + ownership prevent content swap).
    tmp_dir = Path("/tmp") / f"wb-{os.geteuid()}-{digest}"
    _ensure_private_socket_dir(tmp_dir)
    sock = tmp_dir / "embed.sock"
    if len(str(sock).encode()) <= _SUN_PATH_MAX:
        return sock
    raise RuntimeError("owner-private socket path still exceeds AF_UNIX limit")


class SidecarStartupError(RuntimeError):
    """A machine-readable reason the resident process could not start."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class _LockHeld(Exception):
    pass


class _PeerUntrusted(RuntimeError):
    """No supported credential API authenticated the connected peer as euid."""


def _peer_uid(connection: socket.socket) -> int:
    """Read kernel peer credentials, never infer identity from a pathname."""
    try:
        if sys.platform.startswith("linux"):
            creds = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
            _pid, uid, _gid = struct.unpack("3i", creds)
            return uid
        if sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd", "dragonfly")):
            import ctypes

            getpeereid = ctypes.CDLL(None, use_errno=True).getpeereid
            getpeereid.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_uint), ctypes.POINTER(ctypes.c_uint)]
            getpeereid.restype = ctypes.c_int
            uid = ctypes.c_uint()
            gid = ctypes.c_uint()
            if getpeereid(connection.fileno(), ctypes.byref(uid), ctypes.byref(gid)) != 0:
                raise OSError(ctypes.get_errno(), "getpeereid failed")
            return uid.value
    except (AttributeError, ImportError, OSError, struct.error) as exc:
        raise _PeerUntrusted("peer_untrusted") from exc
    raise _PeerUntrusted("peer_untrusted")


def _verify_peer_uid(connection: socket.socket) -> None:
    if _peer_uid(connection) != os.geteuid():
        raise _PeerUntrusted("peer_untrusted")


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
        self.socket_path = resolve_socket_path(self.embedding_dir)
        self._socket_dir_fd: int | None = None
        self._socket_parent_fd: int | None = None
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

    @staticmethod
    def _directory_matches(path: Path, held_fd: int, *, private: bool = True) -> bool:
        try:
            fd = _open_socket_dir(path, private=private)
            try:
                current, held = os.fstat(fd), os.fstat(held_fd)
                return (current.st_dev, current.st_ino) == (held.st_dev, held.st_ino)
            finally:
                os.close(fd)
        except (OSError, RuntimeError):
            return False

    def _unlink_socket(self) -> None:
        fd = self._socket_dir_fd
        if fd is None or not self._directory_matches(self.socket_path.parent, fd):
            return
        try:
            info = os.lstat(self.socket_path.name, dir_fd=fd)
        except FileNotFoundError:
            return
        if stat.S_ISSOCK(info.st_mode) and info.st_uid == os.geteuid():
            try:
                os.unlink(self.socket_path.name, dir_fd=fd)
            except FileNotFoundError:
                pass

    def _remove_socket_directory(self) -> None:
        """Reclaim only this relocated digest, through its held parent fd."""
        parent_fd, fd = self._socket_parent_fd, self._socket_dir_fd
        if parent_fd is None or fd is None:
            return
        if not self._directory_matches(self.socket_path.parent.parent, parent_fd, private=False):
            return
        try:
            current = os.lstat(self.socket_path.parent.name, dir_fd=parent_fd)
            held = os.fstat(fd)
            if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
                return
            _check_socket_dir(current, private=True)
            os.rmdir(self.socket_path.parent.name, dir_fd=parent_fd)
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.ENOTEMPTY, errno.EEXIST):
                raise

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
        try:
            _verify_peer_uid(connection)
        except _PeerUntrusted:
            return
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
        with ExitStack() as resources:
            try:
                self._socket_dir_fd = _open_socket_dir(self.socket_path.parent, create=True)
                resources.callback(os.close, self._socket_dir_fd)
                if self.socket_path.parent != self.embedding_dir:
                    self._socket_parent_fd = _open_socket_dir(self.socket_path.parent.parent, private=False)
                    resources.callback(os.close, self._socket_parent_fd)
                return self._serve_with_directory()
            finally:
                self._socket_dir_fd = None
                self._socket_parent_fd = None

    def _serve_with_directory(self) -> bool:
        self.embedding_dir.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_file:
            return self._serve_with_lock(lock_file)

    def _serve_with_lock(self, lock_file: Any) -> bool:
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
            if self._socket_dir_fd is None or not self._directory_matches(self.socket_path.parent, self._socket_dir_fd):
                raise SidecarStartupError("socket_directory_untrusted")
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
                try:
                    self._unlink_socket()
                    self._remove_socket_directory()
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


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
