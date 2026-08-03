"""Wedged-writer liveness registry + reaper (internal / T14).

Long-running SQLite write holders can wedge ``handoff.db`` for hours; a
``busy_timeout`` of a few seconds cannot outlast them. This module keeps a
**sidecar** writers registry (JSON next to the DB) because heartbeat updates
cannot share the exclusive write lock held by a long transaction in the same
DB ([DATA-14] single-source constants).

On connect (and on lock errors) the reaper:

- clears writers whose PID is dead
- clears writers whose heartbeat is older than ``WRITER_HEARTBEAT_STALE_SECONDS``
  and, when the PID is still live, sends SIGTERM so the OS releases the lock
- **never** kills a live PID with a fresh heartbeat

Scope: self-heal + observability only. Events are logged under the named
event ``wedged_writer_reaped`` and retained in a process-local ring buffer for
tests / doctor surfaces.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import signal
import subprocess
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

_log = logging.getLogger("workbay_handoff_mcp")

# Single-source liveness window ([DATA-14]). Conservative: only reaps after
# multi-minute silence so healthy long ops with heartbeats stay safe.
WRITER_HEARTBEAT_STALE_SECONDS = 300

_EVENT_WEDGED_WRITER_REAPED = "wedged_writer_reaped"
_MAX_RECENT_EVENTS = 64
_recent_events: deque[dict[str, Any]] = deque(maxlen=_MAX_RECENT_EVENTS)
_events_lock = threading.Lock()
_registry_lock = threading.Lock()


@dataclass
class WriterRegistration:
    pid: int
    heartbeat_ts: float
    started_at: float
    label: str
    writer_id: str
    # S7-A-02 [RES-10]: process start-time identity captured at registration.
    # None for legacy rows or when the platform cannot answer; the reaper never
    # SIGTERMs without a positive identity match against this value.
    proc_start: str | None = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> WriterRegistration:
        raw_proc_start = raw.get("proc_start")
        return cls(
            pid=int(raw["pid"]),
            heartbeat_ts=float(raw["heartbeat_ts"]),
            started_at=float(raw["started_at"]),
            label=str(raw.get("label") or ""),
            writer_id=str(raw["writer_id"]),
            proc_start=str(raw_proc_start) if raw_proc_start else None,
        )


def writers_registry_path(db_path: Path | str) -> Path:
    """Sidecar path for the writers registry (next to ``handoff.db``)."""
    path = Path(db_path)
    return path.with_name(path.name + ".writers.json")


def recent_reaper_events(*, clear: bool = False) -> list[dict[str, Any]]:
    """Return a copy of recent reaper events (test/doctor helper)."""
    with _events_lock:
        items = list(_recent_events)
        if clear:
            _recent_events.clear()
        return items


def _record_event(payload: dict[str, Any]) -> None:
    with _events_lock:
        _recent_events.append(dict(payload))
    _log.warning(
        "%s pid=%s writer_id=%s reason=%s label=%s",
        payload.get("event"),
        payload.get("pid"),
        payload.get("writer_id"),
        payload.get("reason"),
        payload.get("label"),
    )


@contextmanager
def _registry_file_lock(path: Path) -> Iterator[None]:
    """Cross-process exclusive lock for sidecar read-modify-write (S7-A-01).

    The in-process ``_registry_lock`` cannot serialize two *processes* racing
    the read-modify-write of ``.writers.json`` — a concurrent register/reap
    could drop a live writer's just-refreshed heartbeat and get it falsely
    reaped + SIGTERMed [CON-05]. An ``fcntl.flock`` on a sibling ``.lock``
    file makes the whole cycle atomic across processes. Filesystems without
    flock support degrade to the in-process lock only (never fail the write).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    try:
        fh = open(lock_path, "a+", encoding="utf-8")
    except OSError:
        yield
        return
    try:
        locked = False
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            locked = True
        except OSError:
            locked = False
        try:
            yield
        finally:
            if locked:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
    finally:
        fh.close()


def _process_start_time(pid: int) -> str | None:
    """Best-effort process start-time identity for ``pid`` (S7-A-02 [RES-10]).

    psutil-free and darwin-safe: ``/proc`` is not portable, so use
    ``ps -o lstart= -p <pid>`` (POSIX ps; works on darwin and linux). Returns
    None when the platform/pid cannot be resolved — callers must treat None
    as "identity unverified" and never SIGTERM on it.
    """
    if pid <= 0:
        return None
    try:
        proc = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    value = (proc.stdout or "").strip()
    return value or None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we cannot signal it — treat as live.
        return True
    except OSError:
        return False
    return True


def _read_registry(path: Path) -> list[WriterRegistration]:
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    writers = raw.get("writers") if isinstance(raw, dict) else None
    if not isinstance(writers, list):
        return []
    out: list[WriterRegistration] = []
    for item in writers:
        if not isinstance(item, dict):
            continue
        try:
            out.append(WriterRegistration.from_dict(item))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _write_registry(path: Path, writers: list[WriterRegistration]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"writers": [w.to_dict() for w in writers]}
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def register_db_writer(
    db_path: Path | str,
    *,
    label: str = "",
    writer_id: str | None = None,
    pid: int | None = None,
    now: float | None = None,
) -> WriterRegistration:
    """Register (or refresh) this process as an active DB writer."""
    path = writers_registry_path(db_path)
    ts = time.time() if now is None else now
    resolved_pid = os.getpid() if pid is None else int(pid)
    registration = WriterRegistration(
        pid=resolved_pid,
        heartbeat_ts=ts,
        started_at=ts,
        label=label or "",
        writer_id=writer_id or f"{os.getpid()}-{int(ts * 1000)}",
        proc_start=_process_start_time(resolved_pid),
    )
    with _registry_lock, _registry_file_lock(path):
        writers = [w for w in _read_registry(path) if w.writer_id != registration.writer_id]
        # Drop any prior row for this pid so restarts do not pile up.
        writers = [w for w in writers if w.pid != registration.pid]
        writers.append(registration)
        _write_registry(path, writers)
    return registration


def refresh_db_writer_heartbeat(
    db_path: Path | str,
    *,
    writer_id: str,
    now: float | None = None,
) -> bool:
    """Refresh heartbeat for ``writer_id``. Returns False if not registered."""
    path = writers_registry_path(db_path)
    ts = time.time() if now is None else now
    with _registry_lock, _registry_file_lock(path):
        writers = _read_registry(path)
        found = False
        for writer in writers:
            if writer.writer_id == writer_id:
                writer.heartbeat_ts = ts
                found = True
                break
        if found:
            _write_registry(path, writers)
        return found


def unregister_db_writer(db_path: Path | str, *, writer_id: str) -> None:
    path = writers_registry_path(db_path)
    with _registry_lock, _registry_file_lock(path):
        writers = [w for w in _read_registry(path) if w.writer_id != writer_id]
        _write_registry(path, writers)


@contextmanager
def db_writer_heartbeat(
    db_path: Path | str,
    *,
    label: str = "",
    interval_seconds: float = 30.0,
) -> Iterator[WriterRegistration]:
    """Context manager: register, heartbeat on a side thread, unregister.

    Use around long-running write transactions so a live holder never looks
    stale. Interval must be well under ``WRITER_HEARTBEAT_STALE_SECONDS``.
    """
    registration = register_db_writer(db_path, label=label)
    stop = threading.Event()

    def _pulse() -> None:
        # S7-A-03: a transient refresh failure (e.g. brief sidecar I/O error)
        # must not silently kill the heartbeat thread and leave a healthy
        # long-running writer looking stale — log and retry on the next tick.
        while not stop.wait(timeout=max(1.0, interval_seconds)):
            try:
                refresh_db_writer_heartbeat(db_path, writer_id=registration.writer_id)
            except Exception as exc:
                _log.warning(
                    "db-writer-heartbeat refresh failed for writer_id=%s (will retry): %s",
                    registration.writer_id,
                    exc,
                )

    thread = threading.Thread(target=_pulse, name="db-writer-heartbeat", daemon=True)
    thread.start()
    try:
        yield registration
    finally:
        stop.set()
        thread.join(timeout=2.0)
        unregister_db_writer(db_path, writer_id=registration.writer_id)


def reap_stale_db_writers(
    db_path: Path | str,
    *,
    now: float | None = None,
    stale_after_seconds: int | None = None,
    kill_stale_live: bool = True,
) -> list[dict[str, Any]]:
    """Reap dead or heartbeat-stale writers. Never kill live+fresh holders.

    Returns the list of reaper event dicts produced this call.
    """
    path = writers_registry_path(db_path)
    ts = time.time() if now is None else now
    window = WRITER_HEARTBEAT_STALE_SECONDS if stale_after_seconds is None else int(stale_after_seconds)
    events: list[dict[str, Any]] = []

    with _registry_lock, _registry_file_lock(path):
        writers = _read_registry(path)
        if not writers:
            return []
        kept: list[WriterRegistration] = []
        for writer in writers:
            alive = _pid_is_alive(writer.pid)
            # S7-A-02 [RES-10]: PID liveness alone is not identity — the PID
            # may have been reused by an unrelated process. Verify the recorded
            # start-time identity before trusting (and especially before
            # signalling) the PID.
            identity_match: bool | None = None
            if alive and writer.proc_start is not None:
                current_start = _process_start_time(writer.pid)
                identity_match = None if current_start is None else current_start == writer.proc_start
                if identity_match is False:
                    # PID reused by a different process: the writer is gone.
                    alive = False
            age = ts - writer.heartbeat_ts
            stale = age > window
            if alive and not stale:
                kept.append(writer)
                continue
            if not alive:
                reason = "pid_reused" if identity_match is False else "pid_dead"
            else:
                reason = "heartbeat_stale"
            if alive and stale and kill_stale_live:
                if identity_match is True:
                    try:
                        os.kill(writer.pid, signal.SIGTERM)
                    except (ProcessLookupError, PermissionError, OSError) as exc:
                        reason = f"heartbeat_stale_kill_failed:{type(exc).__name__}"
                else:
                    # Never SIGTERM without a positive identity match [RES-10]:
                    # legacy rows without proc_start or an unanswerable ps stay
                    # unsignalled; the stale row is still cleared.
                    reason = "heartbeat_stale_identity_unverified"
            event = {
                "event": _EVENT_WEDGED_WRITER_REAPED,
                "pid": writer.pid,
                "writer_id": writer.writer_id,
                "label": writer.label,
                "reason": reason,
                "heartbeat_age_seconds": round(age, 3),
                "stale_after_seconds": window,
                "db_path": str(db_path),
            }
            events.append(event)
            _record_event(event)
        if len(kept) != len(writers):
            _write_registry(path, kept)
    return events
