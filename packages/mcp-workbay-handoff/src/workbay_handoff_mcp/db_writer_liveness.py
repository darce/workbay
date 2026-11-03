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

# Connect-path reaper rate limit (TAIL-R1-02 / REGSHARD-R1-01 / REVC-BUDGET-STARVE).
# Every ``_get_db_connection`` invokes
# ``reap_stale_db_writers(..., non_blocking=True)``; without a floor the reaper
# scans the full registry on every open. Cap at one real pass per *db_path* per
# window.
#
# MUST stay decoupled from ``WRITER_HEARTBEAT_STALE_SECONDS``. When the interval
# equalled the stale window (300s), a scan at T_h+299 saw a still-fresh row,
# armed the budget, and suppressed every connect-path reap until ~T_h+599 — so
# a wedged live holder survived ~600s after its last heartbeat while victims
# exhausted the 120s lock-retry budget (DEFAULT_LOCK_RETRY_BUDGET_SECONDS in
# write_retry.py). Worst-case *post-stale* reclaim delay is approximately this
# interval (budget armed just before the writer crossed the stale threshold).
#
# Arithmetic (REVC-BUDGET-STARVE-LIVE-STALE):
#   B = DEFAULT_LOCK_RETRY_BUDGET_SECONDS = 120
#   S = WRITER_HEARTBEAT_STALE_SECONDS = 300
#   I = REAPER_CONNECT_MIN_INTERVAL_SECONDS
#   Need I << B so a victim still inside its retry budget can observe reclaim
#   with room for SIGTERM delivery, process exit, and remaining retry work.
#   Choose I = B/4 = 30s → worst-case post-stale reclaim ~30s, ~90s spare in B.
#   A storm of opens still pays at most one full scan per db every 30s.
# Overridable for tests. Keyed by resolved absolute path so a successful reap
# of db A does not suppress self-heal on db B in the same MCP process.
REAPER_CONNECT_MIN_INTERVAL_SECONDS = 30.0
# Bounded map: resolved absolute db path -> monotonic time of last successful
# non-empty connect-path scan. Bound prevents unbounded growth across many
# workspaces opened over a long-lived process lifetime.
_CONNECT_REAP_BUDGET_MAX_ENTRIES = 256
_last_connect_reap_by_db: dict[str, float] = {}

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
    """Sidecar path for the legacy single-file writers registry (next to ``handoff.db``)."""
    path = Path(db_path)
    return path.with_name(path.name + ".writers.json")


def writers_shard_dir(db_path: Path | str) -> Path:
    """Per-writer shard directory beside the legacy ``.writers.json`` registry.

    TAIL-R1-01: register/unregister/refresh write one small file under this
    directory instead of a full-file RMW under a blocking flock.
    """
    legacy = writers_registry_path(db_path)
    return legacy.with_name(legacy.name[: -len(".json")] + ".d")


def _shard_filename_for_writer_id(writer_id: str) -> str:
    """Map a writer_id to a single path segment (no directory traversal)."""
    # writer_ids are ``{pid}-{time_ns}`` in production; still neutralize
    # separators so a hostile id cannot escape the shard directory.
    safe = writer_id.replace("/", "_").replace("\\", "_").replace("\0", "_")
    if not safe or safe in {".", ".."}:
        safe = "_invalid_writer_id"
    return f"{safe}.json"


def writer_shard_path(db_path: Path | str, writer_id: str) -> Path:
    """Path of the shard file for ``writer_id`` under ``.writers.d/``."""
    return writers_shard_dir(db_path) / _shard_filename_for_writer_id(writer_id)


def recent_reaper_events(*, clear: bool = False) -> list[dict[str, Any]]:
    """Return a copy of recent reaper events (test/doctor helper)."""
    with _events_lock:
        items = list(_recent_events)
        if clear:
            _recent_events.clear()
        return items


def _connect_reap_budget_key(db_path: Path | str) -> str:
    """Stable key for the per-db connect-reap rate limit."""
    try:
        return str(Path(db_path).resolve())
    except OSError:
        return str(Path(db_path))


def reset_connect_reap_budget(db_path: Path | str | None = None) -> None:
    """Clear connect-path reap rate-limit state (test isolation helper).

    When *db_path* is given, only that database's entry is cleared; otherwise
    the entire process map is emptied.
    """
    if db_path is None:
        _last_connect_reap_by_db.clear()
        return
    _last_connect_reap_by_db.pop(_connect_reap_budget_key(db_path), None)


def _arm_connect_reap_budget(db_path: Path | str, *, now_m: float | None = None) -> None:
    """Record a successful non-empty connect-path scan for *db_path*.

    Bound the map by dropping the oldest entry when full so a long-lived
    process that opens many workspaces cannot grow the dict without limit.
    Re-arming a known key refreshes insertion order (approximate LRU).
    """
    key = _connect_reap_budget_key(db_path)
    _last_connect_reap_by_db.pop(key, None)
    if len(_last_connect_reap_by_db) >= _CONNECT_REAP_BUDGET_MAX_ENTRIES:
        oldest = next(iter(_last_connect_reap_by_db))
        _last_connect_reap_by_db.pop(oldest, None)
    _last_connect_reap_by_db[key] = time.monotonic() if now_m is None else now_m


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


# Event name when connect-path reaper cannot take the registry lock or is
# rate-limited by the per-db connect budget ([OBS-08] / REVC-BUDGET-SILENT-SUPPRESS).
_EVENT_REAPER_SKIPPED = "wedged_writer_reaper_skipped"
# Event name when a SIGTERM is aborted because a re-read of the writer's own
# shard shows a fresh heartbeat (REVB-REFRESH-REAPER-CROSS-PROC-RACE).
_EVENT_KILL_ABORTED = "wedged_writer_kill_aborted"


@contextmanager
def _registry_file_lock(path: Path, *, blocking: bool = True) -> Iterator[bool]:
    """Cross-process exclusive lock for sidecar read-modify-write (S7-A-01).

    The in-process ``_registry_lock`` cannot serialize two *processes* racing
    the read-modify-write of ``.writers.json`` — a concurrent register/reap
    could drop a live writer's just-refreshed heartbeat and get it falsely
    reaped + SIGTERMed [CON-05]. An ``fcntl.flock`` on a sibling ``.lock``
    file makes the whole cycle atomic across processes. Filesystems without
    flock support degrade to the in-process lock only (never fail the write).

    Yields ``True`` when the critical section may run, ``False`` when a
    non-blocking acquire could not proceed (contention or this thread holds
    RESERVED). Blocking mode always yields ``True`` after best-effort acquire
    (matching the prior always-proceed semantics for register/refresh).

    Non-blocking mode ([L478]): uses ``LOCK_EX | LOCK_NB`` via raw ``fcntl``
    and never calls the hold-path barrier — so contention degrades to an
    observable skip instead of raise-then-swallowed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    try:
        fh = open(lock_path, "a+", encoding="utf-8")
    except OSError:
        # No flock file — proceed with in-process lock only.
        yield True
        return
    try:
        locked = False
        if not blocking:
            # Skip cleanly if this thread already holds RESERVED: taking any
            # cross-process flock on the hold path is forbidden ([L313],
            # [L359], [L478]). Do not raise — reaper degrades observably.
            try:
                from .shared_schema import process_holds_write_lock  # noqa: PLC0415

                if process_holds_write_lock():
                    yield False
                    return
            except Exception:  # noqa: BLE001 — import/scan must not abort reaper
                pass
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except BlockingIOError:
                yield False
                return
            except OSError:
                # flock unsupported: proceed without cross-process lock.
                locked = False
            try:
                yield True
            finally:
                if locked:
                    try:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass
            return

        try:
            # Barrier first: never take cross-process flock on a SQLite hold
            # path ([L313], [L359], [L478], [CON-18]).
            from .shared_write_context import acquire_flock  # noqa: PLC0415

            acquire_flock(fh.fileno(), fcntl.LOCK_EX)
            locked = True
        except OSError:
            locked = False
        try:
            yield True
        finally:
            if locked:
                try:
                    from .shared_write_context import acquire_flock  # noqa: PLC0415

                    acquire_flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
    finally:
        fh.close()


# Self-pid start-time cache (WRITERREG-PERF-01). A process's own start time is
# invariant for its lifetime; re-forking ``ps`` on every register_db_writer was
# ~94% of registration cost. Keyed by pid so a fork()'d child — whose
# os.getpid() differs from the inherited dict keys — misses and recomputes
# rather than reporting its parent's start time. Only self-pid is stored:
# caching foreign pids would make PID reuse look like an identity match and
# could SIGTERM an innocent process. None results are never stored so a
# transient ps failure does not poison subsequent lookups.
#
# After fork the child inherits the parent's dict entry. Pid reuse can then
# make a descendant's self-pid lookup hit the inherited parent start time
# (finding 14824). Clear the cache in the child via register_at_fork so the
# child's identity is always recomputed; chosen over an owner-pid guard
# because the platform hook runs exactly once per fork and needs no hot-path
# getpid comparison.
_self_proc_start_by_pid: dict[int, str] = {}
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_self_proc_start_by_pid.clear)


def _process_start_time(pid: int) -> str | None:
    """Best-effort process start-time identity for ``pid`` (S7-A-02 [RES-10]).

    psutil-free and darwin-safe: ``/proc`` is not portable, so use
    ``ps -o lstart= -p <pid>`` (POSIX ps; works on darwin and linux). Returns
    None when the platform/pid cannot be resolved — callers must treat None
    as "identity unverified" and never SIGTERM on it.

    Memoizes only when ``pid == os.getpid()`` (self). Non-self lookups always
    shell out so the reaper's identity_match gate against a recorded foreign
    pid stays live and cannot be poisoned by PID reuse.
    """
    if pid <= 0:
        return None
    self_pid = os.getpid()
    if pid == self_pid:
        cached = _self_proc_start_by_pid.get(pid)
        if cached is not None:
            return cached
    try:
        from .shared_write_context import run_subprocess  # noqa: PLC0415

        proc = run_subprocess(
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
    value = (proc.stdout or "").strip() or None
    if value is not None and pid == self_pid:
        _self_proc_start_by_pid[pid] = value
    return value


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


def _read_legacy_registry(path: Path) -> list[WriterRegistration]:
    """Read the legacy single-file ``.writers.json`` only (no shard union)."""
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


def _read_writer_shard_file(path: Path) -> WriterRegistration | None:
    """Parse one shard file. Missing/unparseable files return None (skip)."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        # Unlink race during readdir scan — normal under concurrent unregister.
        return None
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    # Accept either a bare writer object or ``{"writers": [one]}`` for
    # defensive compatibility with accidental full-registry writes.
    if "writer_id" in raw:
        item = raw
    else:
        writers = raw.get("writers")
        if not isinstance(writers, list) or not writers:
            return None
        item = writers[0]
        if not isinstance(item, dict):
            return None
    try:
        return WriterRegistration.from_dict(item)
    except (KeyError, TypeError, ValueError):
        return None


def _read_sharded_writers(db_path: Path | str) -> list[WriterRegistration]:
    """Read every parseable shard under ``.writers.d/``; tolerate vanish races."""
    shard_dir = writers_shard_dir(db_path)
    if not shard_dir.is_dir():
        return []
    out: list[WriterRegistration] = []
    try:
        entries = list(shard_dir.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.endswith(".json"):
            continue
        if entry.name.endswith(".tmp") or entry.name.endswith(".json.tmp"):
            continue
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        writer = _read_writer_shard_file(entry)
        if writer is not None:
            out.append(writer)
    return out


def _read_registry(db_path: Path | str) -> list[WriterRegistration]:
    """Union legacy ``.writers.json`` with ``.writers.d/`` shards.

    Shard entries override legacy rows with the same ``writer_id`` so a process
    that re-registered into the sharded layout supersedes an older single-file
    row. Unparseable shards and mid-scan unlinks are skipped, never raised.
    """
    by_id: dict[str, WriterRegistration] = {}
    legacy_path = writers_registry_path(db_path)
    for writer in _read_legacy_registry(legacy_path):
        by_id[writer.writer_id] = writer
    for writer in _read_sharded_writers(db_path):
        by_id[writer.writer_id] = writer
    return list(by_id.values())


def list_registered_db_writers(db_path: Path | str) -> list[WriterRegistration]:
    """Public snapshot of registered writers (legacy + shards union)."""
    return _read_registry(db_path)


def _write_registry(path: Path, writers: list[WriterRegistration]) -> None:
    """Atomic rewrite of the legacy single-file registry (reaper compaction)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"writers": [w.to_dict() for w in writers]}
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_writer_shard(db_path: Path | str, registration: WriterRegistration) -> None:
    """Atomic write of one writer's shard (write ``.tmp`` + ``os.replace``).

    No peer read, no global flock — registration is O(1) in live writers.
    """
    shard_dir = writers_shard_dir(db_path)
    shard_dir.mkdir(parents=True, exist_ok=True)
    target = writer_shard_path(db_path, registration.writer_id)
    tmp = target.with_name(target.name + ".tmp")
    payload = registration.to_dict()
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, target)


def _unlink_writer_shard(db_path: Path | str, writer_id: str) -> None:
    """Remove one writer's shard if present; missing file is a no-op."""
    path = writer_shard_path(db_path, writer_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        # Best-effort: reaper / concurrent unregister may race.
        return


def register_db_writer(
    db_path: Path | str,
    *,
    label: str = "",
    writer_id: str | None = None,
    pid: int | None = None,
    now: float | None = None,
) -> WriterRegistration:
    """Register (or refresh) this process as an active DB writer.

    TAIL-R1-01: writes only this writer's shard under ``.writers.d/`` via
    atomic replace. Does **not** take the global registry flock and does not
    read peer rows — per-op work is O(1) in the number of live writers.
    """
    ts = time.time() if now is None else now
    resolved_pid = os.getpid() if pid is None else int(pid)
    registration = WriterRegistration(
        pid=resolved_pid,
        heartbeat_ts=ts,
        started_at=ts,
        label=label or "",
        # time_ns avoids same-millisecond collisions for nested heartbeats in
        # one process (two opens in the same ms must not share a writer_id).
        writer_id=writer_id or f"{os.getpid()}-{time.time_ns()}",
        proc_start=_process_start_time(resolved_pid),
    )
    # In-process lock only: serialize same-process nested register of the same
    # writer_id against itself. No cross-process flock — peers write distinct
    # shard files.
    with _registry_lock:
        _write_writer_shard(db_path, registration)
    return registration


def refresh_db_writer_heartbeat(
    db_path: Path | str,
    *,
    writer_id: str,
    now: float | None = None,
) -> bool:
    """Refresh heartbeat for ``writer_id``. Returns False if not registered.

    Rewrites only that writer's shard when present. Legacy-only rows (older
    process still on the single-file layout) are refreshed under the flock as
    a compatibility path so mixed-version fleets keep heartbeating.
    """
    ts = time.time() if now is None else now
    shard = writer_shard_path(db_path, writer_id)
    with _registry_lock:
        if shard.is_file():
            existing = _read_writer_shard_file(shard)
            if existing is None or existing.writer_id != writer_id:
                return False
            existing.heartbeat_ts = ts
            _write_writer_shard(db_path, existing)
            return True
        # Compatibility: writer may still live only in the legacy file.
        path = writers_registry_path(db_path)
        with _registry_file_lock(path):
            writers = _read_legacy_registry(path)
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
    """Drop ``writer_id`` by unlinking its shard file (no global flock).

    Legacy single-file rows for the same id (older process layout) are left
    for the reaper's legacy compaction — unregister itself never takes the
    registry flock and never rewrites peers.
    """
    with _registry_lock:
        _unlink_writer_shard(db_path, writer_id)


# ---------------------------------------------------------------------------
# Process-wide shared heartbeat (WRITERREG-PERF-02)
# ---------------------------------------------------------------------------
# Per-open Thread start/join under writerreg-s3 factory wiring dominated the
# concurrent-writer path (16 writers: ~73ms vs main ~5.7ms). Ablation: remove
# only the heartbeat thread → parity with main; remove flock while keeping the
# thread → ~8× worse. Keep the flock; make the pulse O(1) per process.
#
# Design: one daemon thread per process refreshes every live registration on
# the min requested interval. Started lazily on first add; exits voluntarily
# when the live set is empty (no hot-path join — sequential short opens would
# otherwise thrash start/join the same way as per-open threads).
#
# Hazards pinned by tests:
# - os.fork: child inherits no running threads; owner-pid check + at_fork reset
# - concurrent register/unregister: _shared_heartbeat_lock (Condition)
# - last-unregister / new-register race: idle-exit sets _shared_heartbeat_exiting
#   under the lock so concurrent add treats the winding-down thread as not
#   running and starts a replacement (is_alive stays True until finally ends)


@dataclass(frozen=True)
class _LiveHeartbeatTarget:
    db_path: Path
    writer_id: str
    interval_seconds: float


# Condition over a non-reentrant Lock so add/remove can wake the pulse
# immediately when a shorter interval appears. The pulse waits the real
# remaining interval (or idle grace), not a 20 Hz poll clamp — notify_all on
# add/remove/ensure is what re-arms early (HEARTBEAT-PERF02-PULSE-POLLS-20HZ,
# EXITRACE-R1-01). Condition() default RLock is avoided so accidental nested
# acquire still self-deadlocks (EXITRACE-R1-03).
_shared_heartbeat_lock = threading.Condition(threading.Lock())
_live_heartbeat_targets: dict[str, _LiveHeartbeatTarget] = {}
_shared_heartbeat_thread: threading.Thread | None = None
_shared_heartbeat_owner_pid: int | None = None
# Generation bumps when a new thread is started so a stale pulse loop exits
# promptly after fork/restart without needing a cross-thread Event join.
_shared_heartbeat_generation: int = 0
# Set under the lock at the idle-exit return decision so concurrent add does
# not treat a still-alive winding-down thread as a live pulse (is_alive is
# True until the finally block completes and the thread fully exits).
_shared_heartbeat_exiting: bool = False
# After the last live target is removed, keep the pulse thread warm for this
# many seconds so sequential short opens in the same process reuse it instead
# of thrashing Thread.start (the original per-open cost under factory wiring).
_SHARED_HEARTBEAT_IDLE_GRACE_SECONDS = 2.0


def _reset_shared_heartbeat_state_for_fork() -> None:
    """Child-after-fork: inherited thread refs are dead; drop parent state.

    No ``notify_all``: the child has no inherited threads, so there is no
    waiter to wake. Generation is bumped only so a later start in this child
    cannot be confused with a parent generation number.
    """
    global _shared_heartbeat_thread, _shared_heartbeat_owner_pid, _shared_heartbeat_generation
    global _shared_heartbeat_exiting
    # No lock: only the forked child runs this, and no other threads exist yet.
    _live_heartbeat_targets.clear()
    _shared_heartbeat_thread = None
    _shared_heartbeat_owner_pid = os.getpid()
    _shared_heartbeat_generation += 1
    _shared_heartbeat_exiting = False


def _shared_heartbeat_pid_is_current() -> bool:
    return _shared_heartbeat_owner_pid == os.getpid()


def _shared_heartbeat_thread_is_running() -> bool:
    """True only when this process owns a live, non-exiting shared pulse thread.

    An idle-exiting thread keeps ``Thread.is_alive()`` True until its ``finally``
    finishes; treating that window as "running" would let concurrent add skip
    starting a replacement and orphan the newly registered writer.
    """
    thread = _shared_heartbeat_thread
    return (
        _shared_heartbeat_pid_is_current()
        and thread is not None
        and thread.is_alive()
        and not _shared_heartbeat_exiting
    )


def _shared_heartbeat_pulse(generation: int) -> None:
    """Refresh every live registration on its interval; idle-exit after grace.

    S7-A-03: a transient refresh failure must not kill the loop; log + retry.
    When the live set is empty the thread stays warm for
    ``_SHARED_HEARTBEAT_IDLE_GRACE_SECONDS`` so sequential short opens reuse
    it (no hot-path join, no per-open Thread.start thrash).

    Waits on ``_shared_heartbeat_lock`` (a Condition) for the real remaining
    interval or idle-grace time. Add/remove/ensure call ``notify_all`` so a
    newly registered shorter interval is not stranded behind a previously
    armed longer timeout — there is no 20 Hz poll clamp on the live path.
    """
    global _shared_heartbeat_thread, _shared_heartbeat_exiting
    empty_since: float | None = None
    last_pulse_at = 0.0
    try:
        while True:
            targets: list[_LiveHeartbeatTarget] = []
            do_refresh = False
            with _shared_heartbeat_lock:
                if generation != _shared_heartbeat_generation:
                    return
                if not _shared_heartbeat_pid_is_current():
                    return
                if _live_heartbeat_targets:
                    empty_since = None
                    interval = min(t.interval_seconds for t in _live_heartbeat_targets.values())
                    targets = list(_live_heartbeat_targets.values())
                    # Match prior per-open floor of max(1.0, interval) for intervals
                    # >= 1s; allow sub-second intervals in tests without a 1s floor.
                    wait_s = interval if interval < 1.0 else max(1.0, interval)
                    now = time.monotonic()
                    if last_pulse_at == 0.0:
                        # First arming: wait a full interval before the first pulse
                        # (same shape as the prior Event.wait-then-refresh loop).
                        last_pulse_at = now
                    if (now - last_pulse_at) >= wait_s:
                        last_pulse_at = now
                        do_refresh = True
                    else:
                        remaining = wait_s - (now - last_pulse_at)
                        # Wait the real remaining interval. Re-evaluation wakes
                        # come from notify_all on add/remove/ensure — not a poll.
                        _shared_heartbeat_lock.wait(timeout=max(0.0, remaining))
                        continue
                else:
                    now = time.monotonic()
                    if empty_since is None:
                        empty_since = now
                        grace_left = _SHARED_HEARTBEAT_IDLE_GRACE_SECONDS
                    else:
                        grace_left = _SHARED_HEARTBEAT_IDLE_GRACE_SECONDS - (now - empty_since)
                    if grace_left <= 0:
                        # Mark exiting under the lock before releasing it: concurrent
                        # add must not observe is_alive() and skip restart.
                        _shared_heartbeat_exiting = True
                        return
                    # Wait remaining idle grace (not a 20 Hz poll). Add during grace
                    # notify_all-wakes this wait so a new live target is armed promptly.
                    _shared_heartbeat_lock.wait(timeout=max(0.0, grace_left))
                    continue
            if do_refresh and targets:
                for target in targets:
                    try:
                        refresh_db_writer_heartbeat(target.db_path, writer_id=target.writer_id)
                    except Exception as exc:
                        _log.warning(
                            "db-writer-heartbeat refresh failed for writer_id=%s (will retry): %s",
                            target.writer_id,
                            exc,
                        )
    finally:
        with _shared_heartbeat_lock:
            if generation == _shared_heartbeat_generation and _shared_heartbeat_thread is threading.current_thread():
                _shared_heartbeat_thread = None
                _shared_heartbeat_exiting = False


def _ensure_shared_heartbeat_thread_locked() -> None:
    """Start the shared pulse thread if this process does not have a live one.

    Caller must hold ``_shared_heartbeat_lock``. Treats an exiting thread as
    not-running so a concurrent add during the idle-exit window starts a
    replacement before the winding-down thread clears the reference.
    """
    global _shared_heartbeat_thread, _shared_heartbeat_owner_pid, _shared_heartbeat_generation
    global _shared_heartbeat_exiting
    if _shared_heartbeat_thread_is_running():
        return
    # Post-fork without at_fork: inherited owner_pid is the parent. Drop any
    # inherited live set before publishing a child-owned thread. Do NOT clear
    # when owner_pid is None (first start in this process) — the caller just
    # added a target we must keep.
    if _shared_heartbeat_owner_pid is not None and _shared_heartbeat_owner_pid != os.getpid():
        _live_heartbeat_targets.clear()
    _shared_heartbeat_generation += 1
    generation = _shared_heartbeat_generation
    _shared_heartbeat_owner_pid = os.getpid()
    _shared_heartbeat_exiting = False
    thread = threading.Thread(
        target=_shared_heartbeat_pulse,
        name="db-writer-heartbeat",
        args=(generation,),
        daemon=True,
    )
    _shared_heartbeat_thread = thread
    thread.start()
    # Wake any older-generation pulse still sitting on a long Condition wait
    # so it notices the generation bump promptly (no poll clamp left to do it).
    _shared_heartbeat_lock.notify_all()


def _add_live_heartbeat_target(
    *,
    db_path: Path | str,
    writer_id: str,
    interval_seconds: float,
) -> None:
    path = Path(db_path)
    key = writer_id
    target = _LiveHeartbeatTarget(
        db_path=path,
        writer_id=writer_id,
        interval_seconds=float(interval_seconds),
    )
    with _shared_heartbeat_lock:
        if _shared_heartbeat_owner_pid is not None and _shared_heartbeat_owner_pid != os.getpid():
            # Inherited parent state after fork without at_fork hook: drop it.
            _live_heartbeat_targets.clear()
        _live_heartbeat_targets[key] = target
        _ensure_shared_heartbeat_thread_locked()
        # Wake the pulse so a newly added shorter interval is honoured promptly
        # rather than waiting out a previously armed longer Condition timeout.
        _shared_heartbeat_lock.notify_all()


def _remove_live_heartbeat_target(writer_id: str) -> None:
    """Drop a target from the live set. Does not join the pulse thread.

    The pulse loop exits on its next wake when the set is empty. Avoiding a
    hot-path join is the concurrency fix: sequential short opens must not pay
    Thread.start + join per connection.
    """
    with _shared_heartbeat_lock:
        _live_heartbeat_targets.pop(writer_id, None)
        _shared_heartbeat_lock.notify_all()


def shared_heartbeat_is_running() -> bool:
    """Test/doctor helper: whether this process has a live shared pulse thread."""
    with _shared_heartbeat_lock:
        return _shared_heartbeat_thread_is_running()


def shared_heartbeat_live_count() -> int:
    """Test helper: number of targets the shared pulse currently refreshes."""
    with _shared_heartbeat_lock:
        if not _shared_heartbeat_pid_is_current() and _shared_heartbeat_owner_pid is not None:
            return 0
        return len(_live_heartbeat_targets)


try:
    os.register_at_fork(after_in_child=_reset_shared_heartbeat_state_for_fork)
except (AttributeError, OSError):
    # register_at_fork is POSIX/CPython; non-fork platforms need no reset.
    pass


@contextmanager
def db_writer_heartbeat(
    db_path: Path | str,
    *,
    label: str = "",
    interval_seconds: float = 30.0,
) -> Iterator[WriterRegistration]:
    """Context manager: register, pulse via process-wide thread, unregister.

    Use around long-running write transactions so a live holder never looks
    stale. Interval must be well under ``WRITER_HEARTBEAT_STALE_SECONDS``.

    Short factory-mediated opens share one process-wide daemon thread rather
    than starting and joining a Thread per connection (WRITERREG-PERF-02).
    Registration and unregistration remain synchronous; each touches only
    that writer's shard (no global registry flock on the hot path).
    """
    registration = register_db_writer(db_path, label=label)
    _add_live_heartbeat_target(
        db_path=db_path,
        writer_id=registration.writer_id,
        interval_seconds=interval_seconds,
    )
    try:
        yield registration
    finally:
        _remove_live_heartbeat_target(registration.writer_id)
        unregister_db_writer(db_path, writer_id=registration.writer_id)


def reap_stale_db_writers(
    db_path: Path | str,
    *,
    now: float | None = None,
    stale_after_seconds: int | None = None,
    kill_stale_live: bool = True,
    non_blocking: bool = False,
) -> list[dict[str, Any]]:
    """Reap dead or heartbeat-stale writers. Never kill live+fresh holders.

    Returns the list of reaper event dicts produced this call.

    When *non_blocking* is True (connect path, [L478]), the registry flock is
    acquired with ``LOCK_NB``. Contention or a held write lock on this thread
    yields a single ``wedged_writer_reaper_skipped`` event and returns without
    raising — degradation is observable ([OBS-08], [RES-06]).

    Connect-path passes are also rate-limited to at most one successful scan
    per database path per ``REAPER_CONNECT_MIN_INTERVAL_SECONDS`` (after lock
    acquire) so a storm of opens does not re-walk the registry every time.
    The budget is keyed by resolved absolute ``db_path`` so a successful scan
    of one workspace cannot suppress self-heal on another (REGSHARD-R1-01).
    """
    path = writers_registry_path(db_path)
    ts = time.time() if now is None else now
    window = WRITER_HEARTBEAT_STALE_SECONDS if stale_after_seconds is None else int(stale_after_seconds)
    events: list[dict[str, Any]] = []
    budget_key = _connect_reap_budget_key(db_path)

    # Flock still serializes legacy-file compaction (the remaining multi-writer
    # RMW). Shard unlinks do not need it, but holding it for the whole pass
    # keeps skip-under-contention behaviour on the connect path unchanged.
    with _registry_lock, _registry_file_lock(path, blocking=not non_blocking) as acquired:
        if not acquired:
            # Observable degrade — not a failure log ([OBS-08], [RES-06]).
            skip = {
                "event": _EVENT_REAPER_SKIPPED,
                "reason": "registry_lock_busy",
                "db_path": str(db_path),
            }
            events.append(skip)
            with _events_lock:
                _recent_events.append(dict(skip))
            _log.info(
                "%s reason=%s db_path=%s",
                _EVENT_REAPER_SKIPPED,
                "registry_lock_busy",
                db_path,
            )
            return events
        # Rate-limit after lock so a held write lock still surfaces the
        # observable skip above. Empty-registry scans do not arm the budget:
        # a fixture/bootstrap open with no writers must not suppress a next
        # connect that needs to reap a dead or stale holder. Per-db keying
        # (REGSHARD-R1-01): arming db A must not suppress reaping db B.
        # Budget short-circuit is observable ([OBS-08] / REVC-BUDGET-SILENT-SUPPRESS):
        # operators must distinguish "reaper found nothing" from "budget-suppressed".
        if non_blocking:
            now_m = time.monotonic()
            last = _last_connect_reap_by_db.get(budget_key)
            if last is not None and (now_m - last) < float(REAPER_CONNECT_MIN_INTERVAL_SECONDS):
                skip = {
                    "event": _EVENT_REAPER_SKIPPED,
                    "reason": "connect_reap_budget",
                    "db_path": str(db_path),
                }
                events.append(skip)
                with _events_lock:
                    _recent_events.append(dict(skip))
                _log.info(
                    "%s reason=%s db_path=%s",
                    _EVENT_REAPER_SKIPPED,
                    "connect_reap_budget",
                    db_path,
                )
                return events
        writers = _read_registry(db_path)
        if not writers:
            return []
        if non_blocking:
            _arm_connect_reap_budget(db_path)
        kept: list[WriterRegistration] = []
        reaped_ids: list[str] = []
        for writer in writers:
            alive = _pid_is_alive(writer.pid)
            # TAIL-R1-02: compute staleness FIRST. Only reaping candidates may
            # shell out for process start-time identity. A fresh, alive writer
            # keeps its row with zero forks — required for O(1) steady state
            # when N writers all heartbeat within the window.
            #
            # Honest consequence: a FRESH row whose pid was reused (recorded
            # proc_start no longer matches) now survives up to one stale-window
            # longer before eviction with reason ``pid_reused``. Nothing blocks
            # on that row's presence and no kill decision changes (SIGTERM still
            # requires a live positive identity match on a stale candidate).
            age = ts - writer.heartbeat_ts
            stale = age > window
            identity_match: bool | None = None
            if alive and not stale:
                kept.append(writer)
                continue
            # Reaping candidate (dead pid and/or stale heartbeat).
            # S7-A-02 [RES-10]: PID liveness alone is not identity — the PID
            # may have been reused by an unrelated process. Verify the recorded
            # start-time identity only when the writer is still alive and we
            # may need to signal it (stale path). Dead pids need no fork.
            if alive and writer.proc_start is not None:
                current_start = _process_start_time(writer.pid)
                identity_match = None if current_start is None else current_start == writer.proc_start
                if identity_match is False:
                    # PID reused by a different process: the writer is gone.
                    alive = False
            if not alive:
                reason = "pid_reused" if identity_match is False else "pid_dead"
            else:
                reason = "heartbeat_stale"
            if alive and stale and kill_stale_live:
                if identity_match is True:
                    # REVB-REFRESH-REAPER-CROSS-PROC-RACE: snapshot was taken at
                    # entry; a suspended-then-resumed writer may have refreshed
                    # its own shard under only the in-process lock. Re-read
                    # *one* shard (O(1), not the full registry) immediately
                    # before SIGTERM and abort the kill if the heartbeat is now
                    # fresh. The live writer is kept; only the signal is aborted.
                    latest = _read_writer_shard_file(writer_shard_path(db_path, writer.writer_id))
                    if (
                        latest is not None
                        and latest.writer_id == writer.writer_id
                        and (ts - latest.heartbeat_ts) <= window
                    ):
                        abort = {
                            "event": _EVENT_KILL_ABORTED,
                            "reason": "heartbeat_refreshed_before_kill",
                            "pid": writer.pid,
                            "writer_id": writer.writer_id,
                            "label": writer.label,
                            "snapshot_heartbeat_age_seconds": round(age, 3),
                            "fresh_heartbeat_age_seconds": round(ts - latest.heartbeat_ts, 3),
                            "stale_after_seconds": window,
                            "db_path": str(db_path),
                        }
                        events.append(abort)
                        _record_event(abort)
                        # Keep the refreshed registration (update in-memory row).
                        kept.append(latest)
                        continue
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
            reaped_ids.append(writer.writer_id)
        if reaped_ids:
            # Drop reaped shards (O(reaped)); leave kept shards untouched.
            for writer_id in reaped_ids:
                _unlink_writer_shard(db_path, writer_id)
            # Compact the legacy single-file registry under the flock we hold.
            # Only rewrite when the legacy file still lists a reaped id.
            legacy = _read_legacy_registry(path)
            if legacy:
                kept_ids = {w.writer_id for w in kept}
                new_legacy = [w for w in legacy if w.writer_id in kept_ids]
                if len(new_legacy) != len(legacy):
                    _write_registry(path, new_legacy)
    return events
