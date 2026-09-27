"""Caller-side SQLite write-lock retry for handoff MCP tools.

When a live peer holds a write lock past ``HANDOFF_SQLITE_BUSY_TIMEOUT_MS``,
MCP write tools surface ``sqlite3.OperationalError`` ("database is locked" /
"database is busy"). The reaper in :mod:`.db_writer_liveness` correctly keeps
alive writers with a fresh heartbeat, so the remaining gap is caller-side
retry rather than more aggressive reaping.

This module retries whole tool calls with jittered exponential backoff
bounded by an *inter-attempt* wall-clock budget, then returns a typed
``db_busy`` failure naming the best-effort lock holder. That budget is a
design target for sleep/elapsed between attempts ([RES-02] aspiration), not
proof that every allowlisted path is already fully RES-01/RES-02 compliant —
membership still requires a verified idempotency guard (see below).

**Budget vs busy_timeout (honest ceiling):** the 120s default bounds only
elapsed time measured when ``fn()`` returns and inter-attempt sleep. Each
attempt can still block inside SQLite for the full
``HANDOFF_SQLITE_BUSY_TIMEOUT_MS`` (30s) because per-connection busy_timeout
is owned by the connect path, not this wrapper. Worst-case wall time is
therefore approximately ``budget + busy_timeout`` (~150s default), and a
final attempt that starts with budget remaining may still block another
full busy_timeout. Shrinking per-attempt busy_timeout to remaining budget
requires a connect-path change outside this module's allowlist.

Whole-call retry is safe **only** for handlers with a verified idempotency
guard on identical re-invocation — **not** merely because a tool might take
the write lock via ``BEGIN IMMEDIATE`` (that historical premise was false for
most candidates and is not used as a safety argument here). A lock error can
also escape from post-commit projection work after a successful mutation;
retrying that call must not double-apply, mint a new identity, or invert
success into a revision conflict. Surviving allowlist members and the guard
each relies on:

* ``close_slice`` — early-return idempotency on ``(task_ref, decision, session)``;
  post-commit projection lock errors are degraded in-core to
  ``projection_stale`` warnings on an ``ok:true`` envelope so they do not
  re-enter this wrapper after a committed mutation.
* ``review_findings`` — only the operations listed in
  :data:`LOCK_RETRY_WRITE_OPERATIONS` (``record`` / ``batch_record`` upserts
  on ``(task_ref, finding_id)``). Other operations of this multiplexed tool
  (``reanchor``, ``repair_provenance``, reopen ``update``, …) invert
  ok:true into ok:false on identical replay and must execute once.
* ``artifacts`` — only the operations listed in
  :data:`LOCK_RETRY_WRITE_OPERATIONS` (``record`` upserts on
  ``(task_ref, lane_id, source_kind, source_label)`` with stable
  ``source_id``). Other operations of this multiplexed tool fall through
  unretried.

``continuation`` is **excluded**: artifact-row counts stay stable under
identical save, but each replay mints a new ``packet_id`` and records a
phantom supersession (``prior_packet_id`` of the previous id). That is not
identity-idempotent whole-call safety.

Tool-name membership comes from :data:`LOCK_RETRY_WRITE_TOOLS`. When a tool
also appears in :data:`LOCK_RETRY_WRITE_OPERATIONS`, that mapping **refines**
membership: the call is retried only when the payload ``operation`` is in the
listed set. Missing/malformed payloads fail closed (no retry). Tools excluded
because whole-call retry is unsafe live in :data:`LOCK_RETRY_EXCLUDED_TOOLS`.
"""

from __future__ import annotations

import functools
import inspect
import logging
import os
import random
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Final, Mapping, TypeVar

if TYPE_CHECKING:
    import subprocess

from .db_writer_liveness import WriterRegistration
from .shared_primitives import _envelope
from .sqlite_lock_errors import (
    RetryBoundary,
    SqliteLockKind,
    classify_sqlite_lock_error,
    is_lock_contention_error,
    is_retryable_lock_error,
)

T = TypeVar("T")

_log = logging.getLogger("workbay_handoff_mcp")

# Explicit sentinel when a lock event cannot name its database path
# (in-memory connections, or a call site that has not hoisted the path yet).
# A missing key and an unknown value are different facts ([OBS-03], [CARD-07]):
# always emit the key; use this sentinel rather than omitting or inventing.
DATABASE_PATH_UNAVAILABLE: Final[str] = "<unavailable>"

# Resolver-guard telemetry: unset ContextVar on a worker thread is steady
# state, not a one-off. Count failures process-wide and emit one warning so
# operators still see the degradation without a per-call flood ([OBS-01]).
_db_path_resolver_failure_count: int = 0
_db_path_resolver_failure_warned: bool = False

# Allowlisted MCP write tools only. Do not blanket-wrap every tool: membership
# requires a verified idempotency guard (see module docstring). Auditable
# module-level constant; pin via behavioural double-call probes in tests.
LOCK_RETRY_WRITE_TOOLS: frozenset[str] = frozenset(
    {
        "close_slice",
        "review_findings",
        "artifacts",
        "activate_tool_domain",
    }
)

# Explicit exclusions: tools that look like write-lock candidates but must NOT
# be whole-call retried. Reasons are one-line probes that failed review
# (or ``unprobed: …`` when a failure mode could not be driven in-process).
LOCK_RETRY_EXCLUDED_TOOLS: dict[str, str] = {
    "record_event": ("identical re-invocation duplicates rows (blockers / verified_tests)"),
    "set_handoff_state": (
        "identical re-invocation with stale expected_revision returns "
        "ok:false revision conflict after a successful first write"
    ),
    "continuation": (
        "identical save is row-count stable but mints a new packet_id each "
        "call and records a phantom supersession (prior_packet_id)"
    ),
    "archive": (
        "identical re-archive returns ok:true but rewrites snapshot_json (content length changes) rather than no-opping"
    ),
    "compaction": (
        "identical record re-invocation allocates a new compaction_id and session_compactions row each call"
    ),
    "import_handoff_state": ("identical merge re-import grows next_actions row count"),
    "next_actions": ("identical add re-invocation inserts a second next_actions row"),
    "reconcile_reviewer_scratch_findings_gc": (
        "unprobed: bulk multi-coordinator apply without a call-level identity "
        "key; empty-DB double-call is a no-op and eligibility retirement was "
        "not driven in-process"
    ),
    "review_runs": ("identical record re-invocation returns ok:false (review_run_id already exists)"),
    "terminal_guard_telemetry": (
        "identical record kwargs without created_at mint a new event_key "
        "each second and duplicate terminal_guard_events rows"
    ),
    "touched_files": ("identical record re-invocation always INSERT; touched_files row count grows"),
}

# Operation-scoped refinement of LOCK_RETRY_WRITE_TOOLS for multiplexed tools.
# Key: tool name already on the tool-name allowlist. Value: the ONLY operations
# of that tool that may be whole-call retried.
#
# review_findings: record / batch_record are upserts keyed on
# (task_ref, finding_id) and are the only verified-idempotent operations of
# that tool. Every other operation (reanchor, repair_provenance, reopen
# update, resolve, merge, integrate, disposition, list, …) falls through
# unretried — identical replay can invert ok:true into ok:false.
#
# artifacts: record is an upsert on
# (task_ref, lane_id, source_kind, source_label). purge/search/get are not
# on this set (purge is delete-if-matches; reads need no write-lock retry).
LOCK_RETRY_WRITE_OPERATIONS: dict[str, frozenset[str]] = {
    "review_findings": frozenset({"record", "batch_record"}),
    "artifacts": frozenset({"record"}),
}

# Payload parameter name that carries the ``operation`` key for each
# operation-scoped tool. Used by the lock-retry wrapper to decide membership.
_LOCK_RETRY_OPERATION_PAYLOAD_PARAM: dict[str, str] = {
    "review_findings": "review",
    "artifacts": "artifact",
}
_MISSING = object()

DEFAULT_LOCK_RETRY_BUDGET_SECONDS = 120.0
_ENV_LOCK_RETRY_SECONDS = "WORKBAY_HANDOFF_LOCK_RETRY_SECONDS"

# Backoff starts well under 1s and caps per-sleep growth so a freed lock is
# picked up promptly rather than after a long sleep ([RES-02]).
_INITIAL_BACKOFF_SECONDS = 0.05
_MAX_BACKOFF_SECONDS = 2.0
_BACKOFF_FACTOR = 2.0
# Clamp the power exponent before exponentiation. With initial=0.05 and
# factor=2.0, any exponent >= 6 already exceeds _MAX_BACKOFF_SECONDS (2.0), so
# clamping is behaviour-preserving for the subsequent min() cap. Without this
# clamp, large operator budgets (WORKBAY_HANDOFF_LOCK_RETRY_SECONDS) drive
# attempt past ~1024 and float power raises OverflowError inside the except
# handler, replacing the typed DbBusyError ([RES-02]).
_MAX_BACKOFF_EXPONENT = 6


def is_sqlite_lock_error(
    exc: BaseException,
    *,
    boundary: RetryBoundary | str = RetryBoundary.STATEMENT,
) -> bool:
    """True when the wait-retry path should handle this exception.

    Primary decision uses :func:`classify_sqlite_lock_error` /
    :func:`is_retryable_lock_error` keyed on ``exc.sqlite_errorcode``, with
    the caller's explicit :class:`~.sqlite_lock_errors.RetryBoundary`
    ([RES-01]).

    * ``SQLITE_BUSY`` (and other primary-BUSY extended codes except SNAPSHOT)
      → True at every boundary (wait can clear a peer write lock).
    * ``SQLITE_BUSY_SNAPSHOT`` → True only at ``WHOLE_CALL`` (fresh txn /
      snapshot). False at ``STATEMENT``: waiting cannot restore an invalidated
      DEFERRED snapshot inside the same open transaction.
    * ``SQLITE_LOCKED`` → False at every boundary (shared-cache / nested use;
      fresh whole-call does not reliably clear a sibling holder).
    * Missing ``sqlite_errorcode`` (``UNKNOWN``): degraded path for bare
      ``OperationalError`` fixtures / older interpreters. Message membership
      (``database is locked`` / ``database is busy``) is checked first; when
      the message matches, the error is retryable at ``STATEMENT`` only.
      Whole-call replay cannot be shown safe for an unclassified error, so
      ``WHOLE_CALL`` returns False. Callers still see classification
      ``UNKNOWN`` in telemetry when present.

    Default ``boundary`` is ``STATEMENT`` so callers that omit it keep
    historical BUSY-only semantics; :func:`call_with_write_lock_retry` passes
    ``WHOLE_CALL`` explicitly.
    """
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    kind = classify_sqlite_lock_error(exc)
    if kind is SqliteLockKind.UNKNOWN:
        message = str(exc).lower()
        if "database is locked" not in message and "database is busy" not in message:
            return False
        # Codeless lock: retryable at STATEMENT only. Whole-call replay is not
        # safe to assume when classification could not name the condition
        # ([RES-01], [RES-06]).
        if isinstance(boundary, str):
            try:
                boundary = RetryBoundary(boundary)
            except ValueError as exc_boundary:
                raise ValueError(
                    f"unknown retry boundary {boundary!r}; expected one of {[b.value for b in RetryBoundary]}"
                ) from exc_boundary
        return boundary is RetryBoundary.STATEMENT
    return is_retryable_lock_error(exc, boundary=boundary)


def resolve_database_path_for_log(db_path: Path | str | None) -> str:
    """Return the absolute filesystem path for lock telemetry, or a sentinel.

    The path is threaded in as data from the layer that opened the database
    ([ARCH-13], [REF-37]); this helper only normalizes — it does not re-derive
    the path from ambient config or environment. Basename alone is not an
    identifier (multiple worktrees share ``handoff.db``); always prefer the
    resolved absolute path ([OBS-06], [CARD-11]).

    Returns :data:`DATABASE_PATH_UNAVAILABLE` when ``db_path`` is ``None`` or
    names an in-memory / URI-memory database so a missing path is an explicit
    measured value, not a silent omission ([CARD-07]).
    """
    if db_path is None:
        return DATABASE_PATH_UNAVAILABLE
    raw = str(db_path).strip()
    if not raw:
        return DATABASE_PATH_UNAVAILABLE
    lowered = raw.lower()
    if raw == ":memory:" or "mode=memory" in lowered:
        return DATABASE_PATH_UNAVAILABLE
    try:
        return str(Path(raw).expanduser().resolve())
    except (OSError, RuntimeError, ValueError):
        # Unresolvable path still names something; keep the raw form rather
        # than dropping the field. Prefer absolute when Path can parse it.
        try:
            p = Path(raw)
            if p.is_absolute():
                return str(p)
        except (OSError, RuntimeError, ValueError):
            pass
        return raw


def _log_lock_event(
    *,
    exc: BaseException,
    tool: str,
    attempt: int,
    elapsed_s: float,
    will_retry: bool,
    database_path: str,
) -> None:
    """Emit one structured log record per observed lock error ([OBS-01/02/03/06]).

    High-cardinality ``errorname`` / classification / ``database_path`` are
    kept intact so BUSY vs BUSY_SNAPSHOT vs LOCKED remain queryable and the
    failing database is attributable; do not bucket them down to a bool.
    ``database_path`` must be supplied by the caller (resolved absolute path
    or :data:`DATABASE_PATH_UNAVAILABLE`) — never reconstructed here.
    """
    kind = classify_sqlite_lock_error(exc)
    errorcode = getattr(exc, "sqlite_errorcode", None)
    errorname = getattr(exc, "sqlite_errorname", None)
    elapsed_ms = int(round(float(elapsed_s) * 1000.0))
    _log.info(
        "sqlite_lock_event tool=%s database_path=%s classification=%s "
        "errorname=%s errorcode=%s elapsed_ms=%s attempt=%s will_retry=%s "
        "last_error=%s",
        tool,
        database_path,
        kind.value,
        errorname,
        errorcode,
        elapsed_ms,
        attempt,
        will_retry,
        str(exc),
        extra={
            "event": "sqlite_lock_event",
            "tool": tool,
            "operation": tool,
            "database_path": database_path,
            "classification": kind.value,
            "errorname": errorname,
            "errorcode": errorcode,
            "elapsed_ms": elapsed_ms,
            "attempt": attempt,
            "will_retry": will_retry,
            "last_error": str(exc),
        },
    )


def resolve_lock_retry_budget_seconds(
    override: float | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> float:
    """Resolve wall-clock retry budget (seconds).

    Precedence: explicit ``override`` > ``WORKBAY_HANDOFF_LOCK_RETRY_SECONDS``
    > :data:`DEFAULT_LOCK_RETRY_BUDGET_SECONDS`. Non-positive/unparseable env
    values clamp to ``0.0`` / default respectively so a bad env cannot disable
    the bound unexpectedly for unparseable text.
    """
    if override is not None:
        return max(0.0, float(override))
    source: Mapping[str, str] = os.environ if env is None else env
    raw = source.get(_ENV_LOCK_RETRY_SECONDS)
    if raw is None or str(raw).strip() == "":
        return DEFAULT_LOCK_RETRY_BUDGET_SECONDS
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_LOCK_RETRY_BUDGET_SECONDS


def _read_registry_writers(db_path: Path | str) -> list[WriterRegistration]:
    """Load writers via the public union reader (legacy + ``.writers.d`` shards)."""
    from .db_writer_liveness import list_registered_db_writers  # noqa: PLC0415

    return list_registered_db_writers(db_path)


# Hard ceiling for the victim-time OS holder probe ([CARD-09]). Never unbounded.
OS_HOLDER_PROBE_TIMEOUT_SECONDS: Final[float] = 2.0

# Distinct attribution outcomes ([OBS-08]). Collapsing none-found with
# probe-unavailable into "holder unknown" is the observability defect this
# module repairs. Sibling-thread in-process hold is a fifth honest outcome
# (finding 14259) — never collapse it to none_found or reuse skipped_write_lock_held.
HOLDER_ATTR_NONE_FOUND: Final[str] = "no OS-level holder found"
HOLDER_ATTR_PROBE_UNAVAILABLE_PREFIX: Final[str] = "OS holder probe unavailable"
# Operator-facing string for OS_OUTCOME_IN_PROCESS_OTHER_THREAD. Must remain
# distinguishable from skipped_write_lock_held's
# "<prefix>: write lock held in this process (probe skipped)".
HOLDER_ATTR_IN_PROCESS_OTHER_THREAD: Final[str] = (
    "write lock held by another thread in this process (not named as holder)"
)
# Outcome key: OS evidence says this pid holds WRITE on the path, but the
# call-stack barrier did not fire (this thread holds nothing). Distinct from
# skipped_write_lock_held (this stack holds RESERVED; probe skipped).
OS_OUTCOME_IN_PROCESS_OTHER_THREAD: Final[str] = "in_process_other_thread"


def _registry_holder_dicts(
    db_path: Path | str,
    *,
    now: float,
) -> list[dict[str, Any]]:
    """Secondary enrichment only — never the sole holder source ([OBS-12])."""
    holders: list[dict[str, Any]] = []
    for writer in _read_registry_writers(db_path):
        holders.append(
            {
                "pid": writer.pid,
                "label": writer.label,
                "writer_id": writer.writer_id,
                "heartbeat_age_seconds": round(now - writer.heartbeat_ts, 3),
                "started_at": writer.started_at,
                "heartbeat_ts": writer.heartbeat_ts,
                "source": "registry",
            }
        )
    holders.sort(key=lambda h: float(h["heartbeat_age_seconds"]))
    return holders


def _run_os_probe_command(
    argv: list[str],
    *,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Run a probe subprocess. Caller must have already passed the write-lock barrier."""
    import subprocess  # noqa: PLC0415 — keep module import surface narrow

    # Intentionally use raw subprocess.run (not run_subprocess): the barrier
    # was already asserted by the probe entry; re-entering the barrier helper
    # is fine but would couple probe budget accounting to the general helper.
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=max(0.05, timeout),
        check=False,
    )


def _path_device_inodes(db_path: Path) -> set[tuple[int, int, int]]:
    """Return ``(major, minor, inode)`` tuples for the DB and WAL/SHM companions."""
    out: set[tuple[int, int, int]] = set()
    path_str = str(db_path)
    for candidate in (path_str, path_str + "-wal", path_str + "-shm"):
        try:
            st = os.stat(candidate)
        except OSError:
            continue
        out.add((os.major(st.st_dev), os.minor(st.st_dev), int(st.st_ino)))
    return out


def _parse_proc_locks_write_pids(
    text: str,
    targets: set[tuple[int, int, int]] | None,
) -> set[int]:
    """Parse ``/proc/locks`` body for WRITE-lock *holder* pids.

    Kernel formats (fs/locks.c):

    * holder: ``id: TYPE MODE {READ|WRITE} pid maj:min:inode start end``
    * waiter: ``id: -> TYPE MODE {READ|WRITE} pid maj:min:inode start end``

    Waiter lines (the ``->`` form) are **not** holders and must be skipped —
    they shift field indices and would otherwise invite off-by-one mistakes
    that reintroduce confidently-wrong names (finding 14260).

    When *targets* is a non-empty set, only locks whose ``maj:min:inode`` is
    in *targets* are kept (device:inode intersection). When *targets* is
    ``None``, every WRITE holder pid is returned (test/diagnostic only).
    When *targets* is empty, returns an empty set.
    """
    if targets is not None and not targets:
        return set()
    holders: set[int] = set()
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 6:
            continue
        # Waiter lines insert "->" after the id token and are not holders.
        if parts[1] == "->":
            continue
        # parts[0] is "N:" — type at 1, mode at 2, RW at 3, pid at 4, id at 5
        if parts[3].upper() != "WRITE":
            continue
        if not parts[4].isdigit():
            continue
        pid = int(parts[4])
        if pid <= 0:
            continue
        dev_inode = parts[5]
        try:
            maj_s, min_s, ino_s = dev_inode.split(":", 2)
            key = (int(maj_s, 16), int(min_s, 16), int(ino_s))
        except ValueError:
            continue
        if targets is None or key in targets:
            holders.add(pid)
    return holders


def _linux_write_lock_pids(db_path: Path) -> set[int] | None:
    """PIDs holding POSIX/FLOCK *WRITE* locks on *db_path* inodes via ``/proc/locks``.

    Returns ``None`` when ``/proc/locks`` is unavailable (macOS, restricted
    environments) so callers can degrade to honest candidate-set wording.
    Returns an empty set when the file is readable but no WRITE lock matches.
    Bounded pure-file read — no subprocess, no wait ([CARD-09]).
    """
    try:
        text = Path("/proc/locks").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    targets = _path_device_inodes(db_path)
    return _parse_proc_locks_write_pids(text, targets)


def _collect_os_holder_pids(
    db_path: Path,
    *,
    budget_seconds: float,
) -> tuple[str, list[int]]:
    """Return (status, pids).

    status is one of:
    * ``named`` — pids are write-lock holders (Linux ``/proc/locks`` WRITE
      intersection with file openers). Authoritative enough for singular
      ``holder pid=`` wording when the set size is 1.
    * ``openers`` — pids have the file open but could not be narrowed to
      RESERVED/write-lock holders (macOS, no ``/proc``, or openers that are
      readers only). Must render as a *candidate set*, never as *the* holder.
    * ``none_found`` — no foreign openers / write-lock holders.
    * ``in_process_other_thread`` — OS evidence names this process as the
      WRITE holder, but this call stack does not hold RESERVED (sibling
      thread). Never collapses to ``none_found`` (finding 14259).
    * ``unavailable:<reason>`` — probe tools failed or timed out.

    Always excludes ``os.getpid()`` from *named* / *openers* pid lists so a
    victim never accuses itself by pid.
    """
    import shutil  # noqa: PLC0415
    import subprocess  # noqa: PLC0415

    deadline = time.monotonic() + budget_seconds
    path_str = str(db_path)
    self_pid = os.getpid()
    # Prefer lsof -t; fall back to fuser.
    candidates: list[list[str]] = []
    if shutil.which("lsof"):
        candidates.append(["lsof", "-t", "--", path_str])
        # WAL/SHM companions often hold the real writer fd.
        for suffix in ("-wal", "-shm"):
            companion = Path(path_str + suffix)
            if companion.exists():
                candidates.append(["lsof", "-t", "--", str(companion)])
    if shutil.which("fuser"):
        candidates.append(["fuser", path_str])
        for suffix in ("-wal", "-shm"):
            companion = Path(path_str + suffix)
            if companion.exists():
                candidates.append(["fuser", str(companion)])
    if not candidates:
        return "unavailable:lsof_and_fuser_not_found", []

    pids: set[int] = set()
    saw_success = False
    last_err = "no_probe_output"
    for argv in candidates:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            # Partial progress: still classify what we have (self-excluded).
            return _classify_os_pids(db_path, pids - {self_pid}, partial_timeout=True)
        try:
            proc = _run_os_probe_command(argv, timeout=remaining)
        except subprocess.TimeoutExpired:
            return _classify_os_pids(db_path, pids - {self_pid}, partial_timeout=True)
        except OSError as exc:
            last_err = f"os_error:{type(exc).__name__}"
            continue
        # fuser writes pids to stderr; lsof -t to stdout. Accept either.
        text = f"{proc.stdout or ''}\n{proc.stderr or ''}"
        found_any = False
        for token in text.replace(",", " ").split():
            token = token.strip()
            if token.isdigit():
                pid = int(token)
                if pid > 0:
                    pids.add(pid)
                    found_any = True
        if found_any:
            saw_success = True
        elif proc.returncode in (0, 1):
            # lsof/fuser: 0/1 with no PIDs is a successful empty probe
            # (no holder), not a tool failure ([OBS-08]).
            saw_success = True
        else:
            last_err = f"exit_{proc.returncode}:{argv[0]}"

    foreign = pids - {self_pid}
    if foreign:
        return _classify_os_pids(db_path, foreign, partial_timeout=False)
    if saw_success:
        # Openers were only us (or none) — never accuse self by pid.
        # If OS WRITE evidence still names this pid for *this* path, a sibling
        # thread holds RESERVED (call-stack barrier already passed). That is
        # not none_found (finding 14259 / ATTRIBREV-R1-F01).
        if self_pid in pids:
            write_pids = _linux_write_lock_pids(db_path)
            if write_pids is not None and self_pid in write_pids:
                return OS_OUTCOME_IN_PROCESS_OTHER_THREAD, []
        return "none_found", []
    return f"unavailable:{last_err}", []


def _classify_os_pids(
    db_path: Path,
    opener_pids: set[int],
    *,
    partial_timeout: bool,
) -> tuple[str, list[int]]:
    """Classify opener pids as write-lock holders (``named``) or openers-only."""
    if not opener_pids:
        if partial_timeout:
            return "unavailable:probe_timeout", []
        return "none_found", []

    write_pids = _linux_write_lock_pids(db_path)
    if write_pids is None:
        # No /proc/locks (macOS etc.): openers are candidates, not holders.
        if partial_timeout and not opener_pids:
            return "unavailable:probe_timeout", []
        return "openers", sorted(opener_pids)

    # Prefer the intersection: openers that also hold a WRITE lock on the inode.
    narrowed = opener_pids & write_pids
    if narrowed:
        return "named", sorted(narrowed)
    # Write locks on the inode from pids lsof did not list (rare) — still
    # authoritative when /proc/locks names them for our inodes.
    foreign_writers = write_pids - {os.getpid()}
    if foreign_writers:
        return "named", sorted(foreign_writers)
    # Only this process holds WRITE on the path (self excluded from foreign
    # writers). Call-stack barrier already ensures *this* thread does not hold
    # RESERVED — so a sibling thread does (finding 14259).
    if os.getpid() in write_pids:
        return OS_OUTCOME_IN_PROCESS_OTHER_THREAD, []
    # Openers exist but none hold a WRITE lock (pure readers). Report honestly.
    return "openers", sorted(opener_pids)


def _ps_describe_pids(
    pids: list[int],
    *,
    budget_seconds: float,
    source: str = "os",
) -> list[dict[str, Any]]:
    """Describe *pids* via ``ps -o pid,lstart,command``.

    *source* is stamped on every row so structured consumers can distinguish
    write-lock holders (``os``) from unauthoritative file openers
    (``os_openers``).
    """
    import subprocess  # noqa: PLC0415

    if not pids:
        return []
    remaining = max(0.05, budget_seconds)
    argv = ["ps", "-o", "pid=,lstart=,command=", "-p", ",".join(str(p) for p in pids)]
    try:
        proc = _run_os_probe_command(argv, timeout=remaining)
    except (subprocess.TimeoutExpired, OSError):
        # Fall back to pid-only rows so the outcome is preserved.
        return [
            {
                "pid": pid,
                "label": f"pid:{pid}",
                "writer_id": "",
                "heartbeat_age_seconds": None,
                "started_at": None,
                "command": None,
                "source": source,
            }
            for pid in pids
        ]
    rows_by_pid: dict[int, dict[str, Any]] = {}
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        # pid is first whitespace-separated field; lstart is typically
        # "Day Mon DD HH:MM:SS YYYY" (5 tokens); remainder is command.
        parts = line.split(None, 6)
        if len(parts) < 1 or not parts[0].isdigit():
            continue
        pid = int(parts[0])
        if len(parts) >= 6:
            lstart = " ".join(parts[1:6])
            command = parts[6] if len(parts) > 6 else ""
        else:
            lstart = None
            command = " ".join(parts[1:]) if len(parts) > 1 else ""
        label = (command or f"pid:{pid}").strip()
        if len(label) > 120:
            label = label[:117] + "..."
        rows_by_pid[pid] = {
            "pid": pid,
            "label": label,
            "writer_id": "",
            "heartbeat_age_seconds": None,
            "started_at": lstart,
            "command": command or None,
            "source": source,
        }
    out: list[dict[str, Any]] = []
    for pid in pids:
        if pid in rows_by_pid:
            out.append(rows_by_pid[pid])
        else:
            out.append(
                {
                    "pid": pid,
                    "label": f"pid:{pid}",
                    "writer_id": "",
                    "heartbeat_age_seconds": None,
                    "started_at": None,
                    "command": None,
                    "source": source,
                }
            )
    return out


def _probe_os_lock_holders(
    db_path: Path,
    *,
    timeout_seconds: float = OS_HOLDER_PROBE_TIMEOUT_SECONDS,
) -> tuple[str, list[dict[str, Any]]]:
    """Victim-time OS holder probe.

    Returns ``(outcome_key, holders)`` where *outcome_key* is one of:
    ``named`` (write-lock holders), ``openers`` (file openers only),
    ``none_found``, ``in_process_other_thread``, ``unavailable:<reason>``,
    ``skipped_write_lock_held``.

    Non-negotiable properties ([OBS-12], [CARD-09], [CON-18]):
    1. Runs only at victim time (caller already failed with SQLITE_BUSY).
    2. Asserts the write-lock barrier first; skips the probe if *this thread*
       holds RESERVED (a victim in ``call_with_write_lock_retry`` may still
       hold a lock on a different connection). Sibling-thread holds do not
       trip the barrier and surface as ``in_process_other_thread``.
    3. Bounded by *timeout_seconds* (default 2 s).
    4. Distinguishes named / openers / none-found / in-process-other-thread /
       unavailable as distinct.
    """
    from .shared_write_context import (  # noqa: PLC0415
        WriteLockHeldAcrossBlockingWorkError,
        assert_no_write_lock_held,
    )

    try:
        assert_no_write_lock_held("os_holder_probe")
    except WriteLockHeldAcrossBlockingWorkError:
        return "skipped_write_lock_held", []

    deadline = time.monotonic() + max(0.05, float(timeout_seconds))
    status, pids = _collect_os_holder_pids(db_path, budget_seconds=max(0.05, deadline - time.monotonic()))
    if status.startswith("unavailable:"):
        return status, []
    if status == OS_OUTCOME_IN_PROCESS_OTHER_THREAD:
        # No confident holder_pid — do not name our own pid.
        return OS_OUTCOME_IN_PROCESS_OTHER_THREAD, []
    if status == "none_found" or not pids:
        return "none_found", []
    remaining = max(0.05, deadline - time.monotonic())
    # Stamp provenance so as_data can refuse confident holder_pid for openers.
    row_source = "os" if status == "named" else "os_openers"
    holders = _ps_describe_pids(pids, budget_seconds=remaining, source=row_source)
    return status, holders


def _registry_notes_fragment(registry_holders: list[dict[str, Any]]) -> str:
    if not registry_holders:
        return ""
    r0 = registry_holders[0]
    return f"; registry notes (not authoritative): pid={r0['pid']} label={r0.get('label')!r}"


def _format_holder_attribution(
    *,
    os_outcome: str,
    os_holders: list[dict[str, Any]],
    registry_holders: list[dict[str, Any]],
    db_path: Path | str | None,
) -> str:
    """Build the three-way attribution string for DbBusyError ([OBS-08]).

    Singular ``holder pid=... source=os`` is reserved for a *single*
    write-lock-narrowed OS holder. Multi-holder or openers-only outcomes
    report a candidate set so operators never treat a reader or the victim
    as *the* lock holder.
    """
    reg_notes = _registry_notes_fragment(registry_holders)

    if os_outcome == "named" and os_holders:
        reg_by_pid = {int(h["pid"]): h for h in registry_holders if h.get("pid") is not None}
        extras: list[str] = []
        for h in os_holders:
            reg = reg_by_pid.get(int(h["pid"]))
            if reg is not None and reg.get("label"):
                extras.append(f"registry_label={reg['label']!r} writer_id={reg.get('writer_id')!r}")
        extra_suffix = f"; registry notes: {'; '.join(extras)}" if extras else ""
        if len(os_holders) == 1:
            primary = os_holders[0]
            return (
                f"holder pid={primary['pid']} label={primary.get('label')!r} "
                f"started_at={primary.get('started_at')!r} source=os"
                f"{extra_suffix}"
            )
        # Multiple write-lock holders — report the set, no singular holder.
        pid_list = ",".join(str(h["pid"]) for h in os_holders)
        return f"write-lock holder candidates pids=[{pid_list}] source=os{extra_suffix}"

    if os_outcome == "openers" and os_holders:
        pid_list = ",".join(str(h["pid"]) for h in os_holders)
        return f"file-opener candidates (not necessarily lock holders) pids=[{pid_list}] source=os_openers{reg_notes}"

    if os_outcome == "none_found":
        base = HOLDER_ATTR_NONE_FOUND
        if db_path is not None:
            base = f"{base} for {db_path}"
        # Enrichment only — registry is never the decider.
        return base + reg_notes
    if os_outcome == OS_OUTCOME_IN_PROCESS_OTHER_THREAD:
        # Distinct from skipped_write_lock_held ("… (probe skipped)"): here
        # the probe ran and found an in-process sibling holder without naming
        # our own pid as *the* holder (finding 14259).
        return HOLDER_ATTR_IN_PROCESS_OTHER_THREAD + reg_notes
    if os_outcome == "skipped_write_lock_held":
        return f"{HOLDER_ATTR_PROBE_UNAVAILABLE_PREFIX}: write lock held in this process (probe skipped)" + reg_notes
    # unavailable:<reason>
    reason = os_outcome.split(":", 1)[1] if ":" in os_outcome else os_outcome
    return f"{HOLDER_ATTR_PROBE_UNAVAILABLE_PREFIX}: {reason}" + reg_notes


def describe_lock_holder_report(
    db_path: Path | str | None,
    *,
    now: float | None = None,
    probe_timeout_seconds: float = OS_HOLDER_PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Full holder report: holders list + three-way attribution string.

    Keys: ``holders``, ``attribution``, ``os_outcome``, ``holder_source``.

    ``holder_source`` is the structured provenance of any confident primary
    holder: ``os`` (single write-lock holder), ``os_openers`` (candidate set),
    ``registry`` (non-authoritative notes only), ``in_process_other_thread``
    (sibling-thread hold; never promotes ``holder_pid``), ``none``, or
    ``unavailable``.
    """
    ts = time.time() if now is None else float(now)
    if db_path is None:
        return {
            "holders": [],
            "attribution": f"{HOLDER_ATTR_PROBE_UNAVAILABLE_PREFIX}: database_path_unavailable",
            "os_outcome": "unavailable:database_path_unavailable",
            "holder_source": "unavailable",
        }
    path = Path(db_path)
    registry = _registry_holder_dicts(path, now=ts)
    os_outcome, os_holders = _probe_os_lock_holders(path, timeout_seconds=probe_timeout_seconds)
    # Prefer OS rows as the primary list; fall back to registry rows only
    # as enrichment metadata (still returned for structured as_data consumers
    # but never promoted to confident holder_pid).
    if os_holders:
        # Merge registry labels into OS rows where pids match.
        reg_by_pid = {int(h["pid"]): h for h in registry if h.get("pid") is not None}
        merged: list[dict[str, Any]] = []
        for row in os_holders:
            item = dict(row)
            base_source = str(item.get("source") or "os")
            reg = reg_by_pid.get(int(item["pid"]))
            if reg is not None:
                if reg.get("label") and (not item.get("label") or str(item.get("label")).startswith("pid:")):
                    item["label"] = reg["label"]
                item["writer_id"] = reg.get("writer_id") or item.get("writer_id") or ""
                if reg.get("heartbeat_age_seconds") is not None:
                    item["heartbeat_age_seconds"] = reg["heartbeat_age_seconds"]
                # Keep opener vs write-lock distinction; only annotate registry merge.
                if base_source in ("os", "os+registry"):
                    item["source"] = "os+registry"
                else:
                    item["source"] = f"{base_source}+registry"
            merged.append(item)
        holders = merged
        if os_outcome == "named" and len(os_holders) == 1:
            holder_source = "os"
        elif os_outcome == "named":
            holder_source = "os_multi"
        else:
            holder_source = "os_openers"
    else:
        holders = list(registry)
        if os_outcome == OS_OUTCOME_IN_PROCESS_OTHER_THREAD:
            # Registry rows may still enrich the envelope, but provenance must
            # not promote our own pid as confident holder_pid (finding 14259).
            holder_source = OS_OUTCOME_IN_PROCESS_OTHER_THREAD
        elif registry:
            holder_source = "registry"
        elif os_outcome == "none_found":
            holder_source = "none"
        elif os_outcome.startswith("unavailable") or os_outcome == "skipped_write_lock_held":
            holder_source = "unavailable"
        else:
            holder_source = "none"
    attribution = _format_holder_attribution(
        os_outcome=os_outcome,
        os_holders=os_holders,
        registry_holders=registry,
        db_path=path,
    )
    return {
        "holders": holders,
        "attribution": attribution,
        "os_outcome": os_outcome,
        "holder_source": holder_source,
    }


# Sources that may promote a singular confident holder_pid in as_data.
_AUTHORITATIVE_HOLDER_SOURCES = frozenset({"os", "os+registry"})


class DbBusyError(RuntimeError):
    """Typed, actionable failure when the lock-retry budget is exhausted.

    Matches the surrounding tool refusal shape (``error`` + ``error_code`` +
    structured fields) used by :class:`~.shared_schema.SchemaVersionMismatchError`
    so MCP wrappers can return a v2 envelope without inventing a new contract.
    """

    error_code = "db_busy"

    def __init__(
        self,
        *,
        tool: str,
        budget_seconds: float,
        holders: list[dict[str, Any]] | None = None,
        last_error: str | None = None,
        database_path: str | None = None,
        classification: str | None = None,
        holder_attribution: str | None = None,
        holder_source: str | None = None,
        attempts: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> None:
        self.tool = tool
        self.budget_seconds = float(budget_seconds)
        self.holders = list(holders or [])
        self.last_error = last_error or "database is locked"
        # Always emit the path key; unknown stays the explicit sentinel so a
        # missing path and an unknown path remain different facts ([CARD-07]).
        self.database_path = database_path if database_path is not None else DATABASE_PATH_UNAVAILABLE
        self.classification = classification if classification is not None else SqliteLockKind.UNKNOWN.value
        # Exhaustion diagnostics (REVB-06 residual): how many whole-call attempts
        # ran and how long the inter-attempt budget loop observed. None when a
        # caller constructs DbBusyError without a retry loop (tests / direct raise).
        self.attempts = int(attempts) if attempts is not None else None
        self.elapsed_seconds = float(elapsed_seconds) if elapsed_seconds is not None else None
        # Provenance for structured as_data: never invent "os" from a registry ghost.
        if holder_source is not None:
            self.holder_source = holder_source
        elif self.holders:
            src = str(self.holders[0].get("source") or "")
            if src in _AUTHORITATIVE_HOLDER_SOURCES and len(self.holders) == 1:
                self.holder_source = "os"
            elif src.startswith("os_openers"):
                self.holder_source = "os_openers"
            elif src == "registry":
                self.holder_source = "registry"
            else:
                self.holder_source = src or "unknown"
        else:
            self.holder_source = "none"
        if holder_attribution is not None:
            holder_msg = holder_attribution
        else:
            primary = self.holders[0] if self.holders else None
            if (
                primary is not None
                and self.holder_source == "os"
                and str(primary.get("source") or "") in _AUTHORITATIVE_HOLDER_SOURCES
            ):
                age = primary.get("heartbeat_age_seconds")
                age_part = f" heartbeat_age_seconds={age}" if age is not None else ""
                holder_msg = f"holder pid={primary['pid']} label={primary['label']!r}{age_part} source=os"
            elif primary is not None and self.holder_source == "os_openers":
                pid_list = ",".join(str(h["pid"]) for h in self.holders)
                holder_msg = (
                    f"file-opener candidates (not necessarily lock holders) pids=[{pid_list}] source=os_openers"
                )
            elif primary is not None and self.holder_source == "registry":
                holder_msg = (
                    f"registry notes (not authoritative): "
                    f"pid={primary['pid']} label={primary['label']!r} "
                    f"heartbeat_age_seconds={primary.get('heartbeat_age_seconds')}"
                )
            elif primary is not None:
                holder_msg = (
                    f"holder candidates pid={primary['pid']} label={primary['label']!r} "
                    f"source={primary.get('source')!r}"
                )
            else:
                holder_msg = f"{HOLDER_ATTR_PROBE_UNAVAILABLE_PREFIX}: no_attribution"
        self.holder_attribution = holder_msg
        super().__init__(
            f"db_busy: write lock not acquired within {self.budget_seconds:g}s "
            f"for tool={tool}; {holder_msg}; last_error={self.last_error}"
        )

    def as_data(self) -> dict[str, object]:
        primary = self.holders[0] if self.holders else None
        data: dict[str, object] = {
            "error": str(self),
            "error_code": self.error_code,
            "tool": self.tool,
            "budget_seconds": self.budget_seconds,
            "last_error": self.last_error,
            "holders": list(self.holders),
            "database_path": self.database_path,
            "classification": self.classification,
            "holder_attribution": self.holder_attribution,
            "holder_source": self.holder_source,
            "remedy": (
                "retry after the live peer writer finishes its transaction; "
                "use holder_pid only when holder_source is 'os' (write-lock "
                "narrowed). Live writer registration lives under "
                "handoff.db.writers.d/ (one json shard per writer id; the "
                "legacy handoff.db.writers.json file is compatibility-only) "
                "and is never an authoritative lock-holder oracle. "
                "Note: budget_seconds bounds inter-attempt sleep/elapsed only; "
                "each attempt may still wait up to HANDOFF_SQLITE_BUSY_TIMEOUT_MS "
                "inside SQLite, so observed wall time can exceed the budget."
            ),
        }
        # Always emit attempts / elapsed when known so exhaustion envelopes are
        # complete without inventing zeros for direct constructors (REVB-06).
        if self.attempts is not None:
            data["attempts"] = self.attempts
        if self.elapsed_seconds is not None:
            data["elapsed_seconds"] = self.elapsed_seconds
        # Confident singular holder_pid: only a single write-lock-narrowed OS row.
        if (
            primary is not None
            and self.holder_source == "os"
            and len(self.holders) == 1
            and str(primary.get("source") or "") in _AUTHORITATIVE_HOLDER_SOURCES
        ):
            data["holder_pid"] = primary["pid"]
            data["holder_label"] = primary["label"]
            data["holder_heartbeat_age_seconds"] = primary["heartbeat_age_seconds"]
            data["holder_writer_id"] = primary["writer_id"]
        elif primary is not None and self.holder_source == "registry":
            # Weaker keys — structured consumers must opt in ([OBS-08]).
            data["registry_holder_pid"] = primary["pid"]
            data["registry_holder_label"] = primary["label"]
            data["registry_holder_heartbeat_age_seconds"] = primary["heartbeat_age_seconds"]
            data["registry_holder_writer_id"] = primary["writer_id"]
        elif primary is not None and self.holder_source in ("os_openers", "os_multi"):
            data["holder_candidate_pids"] = [h["pid"] for h in self.holders]
        return data


def _default_retry_sleep(seconds: float) -> None:
    """Default sleep for lock retry: barrier then sleep ([CON-18])."""
    from .shared_write_context import blocking_sleep  # noqa: PLC0415

    blocking_sleep(seconds)


def call_with_write_lock_retry(
    fn: Callable[[], T],
    *,
    tool: str,
    budget_seconds: float | None = None,
    db_path: Path | str | None = None,
    sleep: Callable[[float], None] = _default_retry_sleep,
    clock: Callable[[], float] = time.monotonic,
    rng: random.Random | None = None,
    now_wall: Callable[[], float] | None = None,
) -> T:
    """Invoke ``fn``; on genuine lock errors retry with jittered backoff until budget.

    Whole-call retry boundary ([RES-01]): ``fn`` is re-invoked from scratch, so
    a fresh transaction/snapshot is taken on each attempt. That makes
    ``SQLITE_BUSY_SNAPSHOT`` retryable here (unlike statement-level retry).

    Parameters
    ----------
    fn:
        Zero-arg callable (bind ``*args/**kwargs`` at the call site). Application
        ``ok:false`` envelopes are never retried — only raised lock errors.
    budget_seconds:
        Wall-clock budget override; default from env / 120s.
    db_path:
        Resolved filesystem path of the database this call talks to. Threaded
        into lock telemetry as ``database_path`` ([OBS-03], [ARCH-13]); also
        used for writers-registry holder lookup on exhaustion. Pass ``None``
        only when genuinely unknown (emits
        :data:`DATABASE_PATH_UNAVAILABLE`).
    sleep / clock / rng:
        Injected for tests so budget exhaustion never real-sleeps.
    now_wall:
        Wall clock used for heartbeat-age when building the exhaustion error
        (defaults to ``time.time``).
    """
    budget = resolve_lock_retry_budget_seconds(budget_seconds)
    start = clock()
    attempt = 0  # zero-based index for backoff exponent / log field
    attempts = 0  # count of fn() invocations (REVB-06)
    last_exc: sqlite3.OperationalError | None = None
    rand = rng if rng is not None else random.Random()
    # Hoist once: every lock event for this call names the same database.
    logged_database_path = resolve_database_path_for_log(db_path)

    while True:
        attempts += 1
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            elapsed = clock() - start
            # Whole-call boundary: re-invoking fn starts a fresh transaction.
            retryable = is_sqlite_lock_error(exc, boundary=RetryBoundary.WHOLE_CALL)
            # Instrument every named lock outcome (and legacy UNKNOWN message
            # matches), including non-retryable LOCKED, so the high-cardinality
            # errorname and database_path are visible even when we re-raise.
            # Telemetry must never abort retry or replace the typed envelope
            # ([RES-02], [OBS-01]): a raising log sink (custom Handler.emit,
            # FileHandler open on missing dir, …) is still only enrichment.
            if retryable or is_lock_contention_error(exc):
                try:
                    _log_lock_event(
                        exc=exc,
                        tool=tool,
                        attempt=attempt,
                        elapsed_s=elapsed,
                        will_retry=retryable,
                        database_path=logged_database_path,
                    )
                except Exception:
                    pass
            if not retryable:
                raise
            last_exc = exc
            remaining = budget - elapsed
            if remaining <= 0:
                break
            # Clamp exponent before power so large attempt counts under long
            # operator budgets cannot OverflowError out of this handler.
            exponent = min(attempt, _MAX_BACKOFF_EXPONENT)
            raw = _INITIAL_BACKOFF_SECONDS * (_BACKOFF_FACTOR**exponent)
            capped = min(_MAX_BACKOFF_SECONDS, raw)
            # Full jitter in [0, capped] so concurrent waiters desynchronize
            # and a freed lock is observed promptly ([RES-02], [CON-05]).
            delay = rand.uniform(0.0, capped) if capped > 0 else 0.0
            delay = min(delay, remaining)
            # Zero-jitter (or delay clamped to 0) must still advance the budget
            # so injected fake clocks cannot spin forever without progress.
            if delay <= 0:
                delay = min(remaining, max(_INITIAL_BACKOFF_SECONDS * 0.1, 1e-6))
            sleep(delay)
            attempt += 1
            if clock() - start >= budget:
                break

    # Holder lookup / classification enrich the typed envelope only. A
    # registry read, wall-clock probe, or classifier failure must not replace
    # DbBusyError with a raw enrichment exception ([RES-02], [OBS-01]).
    holders: list[dict[str, Any]] = []
    holder_attribution: str | None = None
    holder_source: str | None = None
    try:
        wall = time.time if now_wall is None else now_wall
        report = describe_lock_holder_report(db_path, now=wall())
        holders = list(report.get("holders") or [])
        holder_attribution = str(report.get("attribution") or "") or None
        raw_source = report.get("holder_source")
        holder_source = str(raw_source) if raw_source is not None else None
    except Exception:
        holders = []
        holder_attribution = f"{HOLDER_ATTR_PROBE_UNAVAILABLE_PREFIX}: enrichment_error"
        holder_source = "unavailable"
    try:
        kind = classify_sqlite_lock_error(last_exc) if last_exc is not None else SqliteLockKind.UNKNOWN
    except Exception:
        kind = SqliteLockKind.UNKNOWN
    raise DbBusyError(
        tool=tool,
        budget_seconds=budget,
        holders=holders,
        last_error=str(last_exc) if last_exc is not None else None,
        database_path=logged_database_path,
        classification=kind.value,
        holder_attribution=holder_attribution,
        holder_source=holder_source,
        attempts=attempts,
        elapsed_seconds=max(0.0, clock() - start),
    )


def _extract_payload_operation(
    tool: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    signature: inspect.Signature,
) -> str | None:
    """Return the payload ``operation`` string for an operation-scoped tool call.

    Fails closed: missing payload, non-mapping / no-attribute payload, or a
    missing/non-string ``operation`` yields ``None`` (caller must not retry).
    """
    param_name = _LOCK_RETRY_OPERATION_PAYLOAD_PARAM.get(tool)
    if param_name is None:
        return None
    payload: Any = kwargs.get(param_name, _MISSING)
    if payload is _MISSING:
        try:
            bound = signature.bind_partial(*args, **kwargs)
            payload = bound.arguments.get(param_name, _MISSING)
        except TypeError:
            payload = _MISSING
    if payload is _MISSING:
        return None
    if isinstance(payload, Mapping):
        op = payload.get("operation")
    else:
        # MCP may pass a validated pydantic model (has .operation attribute).
        op = getattr(payload, "operation", None)
    return op if isinstance(op, str) and op else None


def call_is_lock_retryable(
    tool: str,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    *,
    signature: inspect.Signature | None = None,
) -> bool:
    """True when this invocation may be whole-call lock-retried.

    Tools not listed in :data:`LOCK_RETRY_WRITE_OPERATIONS` are fully
    retryable under tool-name allowlist membership alone. Tools listed there
    are retryable only when the payload ``operation`` is a member of the
    allowlisted set. Unresolvable operations fail closed (not retryable).
    """
    allowed_ops = LOCK_RETRY_WRITE_OPERATIONS.get(tool)
    if allowed_ops is None:
        return True
    kw = kwargs if kwargs is not None else {}
    sig = signature if signature is not None else inspect.Signature()
    op = _extract_payload_operation(tool, args, kw, sig)
    if op is None:
        return False
    return op in allowed_ops


def wrap_mcp_write_with_lock_retry(
    handler: Callable[..., Any],
    *,
    tool: str,
    db_path_resolver: Callable[[], Path | str | None] | None = None,
    budget_seconds: float | None = None,
    sleep: Callable[[float], None] = _default_retry_sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Callable[..., Any]:
    """Wrap an MCP tool handler: retry lock errors; on exhaustion return typed envelope.

    Application-level ``ok:false`` dicts pass through unchanged (no retry).
    Non-lock ``OperationalError`` propagates immediately.

    When ``tool`` is listed in :data:`LOCK_RETRY_WRITE_OPERATIONS`, only the
    allowlisted operations are retried; every other operation executes once.

    Async handlers are refused at wrap time (HARM-10): a sync ``_wrapped``
    around ``async def`` would return an un-awaited coroutine, skip the except
    arm, and silently disable retry with no test failure.
    """
    if inspect.iscoroutinefunction(handler):
        raise TypeError(
            f"wrap_mcp_write_with_lock_retry does not support async handlers "
            f"(tool={tool!r}); keep LOCK_RETRY_WRITE_TOOLS handlers sync or "
            f"add an async-aware retry path before wrapping"
        )
    signature = inspect.signature(handler)

    def _resolve_db_path() -> Path | str | None:
        # Only the call-site resolver may name the database. Ambient runtime
        # config can point at a different worktree's handoff.db and would emit
        # a confidently wrong absolute path in lock telemetry — strictly worse
        # than the explicit unavailable sentinel ([OBS-01], [OBS-04], [ARCH-13]).
        if db_path_resolver is None:
            return None
        try:
            return db_path_resolver()
        except Exception as exc:
            # Telemetry enrichment must never fail the write it describes
            # ([RES-02], [OBS-01]). A broken resolver (unset ContextVar on a
            # worker thread, miswired lambda, …) falls back to the sentinel.
            # Steady-state failures must not flood: one warning + running count
            # ([OBS-01]); subsequent identical failures stay silent.
            global _db_path_resolver_failure_count, _db_path_resolver_failure_warned
            _db_path_resolver_failure_count += 1
            if not _db_path_resolver_failure_warned:
                _db_path_resolver_failure_warned = True
                try:
                    _log.warning(
                        "db_path_resolver failed; database_path will use "
                        "unavailable sentinel (failure_count=%s; further "
                        "failures counted silently): %s",
                        _db_path_resolver_failure_count,
                        exc,
                    )
                except Exception:
                    pass
            return None

    @functools.wraps(handler)
    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        # Operation-scoped tools: non-allowlisted (or unresolvable) operations
        # must run exactly once — guessing wrong would re-run a non-idempotent write.
        if not call_is_lock_retryable(tool, args, kwargs, signature=signature):
            return handler(*args, **kwargs)
        try:
            return call_with_write_lock_retry(
                lambda: handler(*args, **kwargs),
                tool=tool,
                budget_seconds=budget_seconds,
                db_path=_resolve_db_path(),
                sleep=sleep,
                clock=clock,
            )
        except DbBusyError as exc:
            return _envelope(ok=False, tool=tool, data=exc.as_data())

    _wrapped.__signature__ = signature  # type: ignore[attr-defined]
    return _wrapped
