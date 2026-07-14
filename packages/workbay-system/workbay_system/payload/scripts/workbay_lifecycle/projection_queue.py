"""Projection spool queue helpers.

This module owns filesystem operations around the pending projection queue so
foreground lifecycle code does not need to open or move the JSONL spool directly.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PENDING_EVENTS_REL = Path(".task-state") / "pending-workflow-events.jsonl"
QUARANTINE_REL = Path(".task-state") / "quarantine"
DRAINING_GLOB = "pending-workflow-events.jsonl.draining.*"
BREAKER_STATE_REL = Path(".task-state") / "projection-breaker.json"
DEAD_LETTER_REL = Path(".task-state") / "projection-dead-letter.jsonl"
# Claim-by-rename snapshot name for the dead-letter drain (sibling of DRAINING_GLOB
# for the live spool). A crash after the claim rename -- or a partial drain from an
# older build -- leaves one of these behind; the auto-drain reclaims them and
# status/doctor count them so stranded events stay visible. The trailing ``*`` also
# matches a ``.checkpoint`` sidecar, so a single glob reclaims both a claim and any
# remainder checkpoint written beside it.
DEAD_LETTER_DRAINING_GLOB = "projection-dead-letter.jsonl.draining.*"
# Records when the dead-letter sink was last auto-drained so the drain can be
# backoff-gated (never truncate/replay the sink on every lifecycle command) and
# ``status`` can surface ``dead_letter_last_drain_at``.
DEAD_LETTER_DRAIN_STATE_REL = Path(".task-state") / "projection-dead-letter-drain.json"
_SAMPLE_ROWS = 3
# Hard cap on lines scanned while collecting the sample so an all-corrupt or
# multi-GiB spool cannot be read to EOF on this path.
_SAMPLE_SCAN_LINE_CAP = 10_000
# Sentinel for ProjectionQueueHealth.live_depth when depth was not measured
# (a stat-only probe asked spool_health to skip the bounded line scan).
_DEPTH_NOT_MEASURED = -1


def _int_env(name: str, default: int) -> int:
    """Read an int limit override from the environment, falling back to default."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


# Single source of truth for the projection-queue limits. The replay handler,
# status, and doctor import these instead of redeclaring their own copies, and
# each is overridable by env var so tests can exercise the guards cheaply.
AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES = _int_env(
    "WORKBAY_PROJECTION_AUTO_DRAIN_MAX_BYTES", 10 * 1024 * 1024
)
SPOOL_DEPTH_SUMMARY_LIMIT = _int_env("WORKBAY_PROJECTION_DEPTH_LIMIT", 10_000)
SPOOL_HARD_LIMIT_BYTES = _int_env(
    "WORKBAY_PROJECTION_HARD_LIMIT_BYTES", 64 * 1024 * 1024
)
BREAKER_FAILURE_THRESHOLD = _int_env("WORKBAY_PROJECTION_BREAKER_FAILURE_THRESHOLD", 1)
# Per-entry replay retry budget. A spool entry that the backend keeps loudly
# rejecting (zero-exit ok:false -> "spooled") is re-spooled with a bumped attempt
# count; once it exceeds this budget the drain routes it to the dead-letter sink
# (reason="max_retry_exceeded") and continues past it, so a single poison entry
# cannot head-of-line-block every entry behind it forever.
SPOOL_MAX_REPLAY_ATTEMPTS = _int_env("WORKBAY_PROJECTION_MAX_REPLAY_ATTEMPTS", 5)
# The dead-letter sink is an accumulator and so needs its own bound (Release-It
# steady state): otherwise a sustained benign-rejection storm fills the 64 MiB
# spool and then grows the dead-letter file without limit -- the same unbounded
# host-disk pressure this module exists to stop, just relocated one file over.
DEAD_LETTER_HARD_LIMIT_BYTES = _int_env(
    "WORKBAY_PROJECTION_DEAD_LETTER_HARD_LIMIT_BYTES", 64 * 1024 * 1024
)
# Repos for which we have already emitted the one-shot dead-letter-overflow
# warning this process, so an over-cap storm cannot spam stderr line-per-event.
_dead_letter_overflow_warned: set[str] = set()

MUTATING_COMMANDS = frozenset(
    {
        "task-start",
        "task-finish",
        "slice-start",
        "slice-commit",
        "review-ready",
        "plan-accept",
        "plan-done",
        "finalize-plan",
        "sync-task-plan-checklist",
    }
)
PREFLIGHT_EXEMPT_COMMANDS = frozenset(
    {
        "project-events-replay",
        "status",
        "doctor",
        "tasks",
        "context",
        # sync-task-plan-checklist READS handoff evidence (best-effort, degrades
        # to empty Evidence when the CLI is unreachable) and writes only a plan
        # markdown file -- it never projects into the handoff spool. Gating it on
        # the projection breaker contradicts its degrade-to-empty contract: an
        # open breaker (a WRITE-path failure) must not block a read+plan-file
        # sweep, and its dry-run/apply must still emit a valid receipt when the
        # backend is down. Reads keep working while the write breaker is open, so
        # exempting lets it still tick whatever evidence it can gather.
        "sync-task-plan-checklist",
    }
)
# Commands that PROJECT into handoff (they spool a workflow_intent via
# skill_broadcast) but do not mutate git. They are not in MUTATING_COMMANDS, yet
# the dominant spool writer -- plan-review / plan-analyze -- must still fail fast
# when the breaker is open or the spool is over the hard limit, instead of
# hammering a down CLI and piling onto the spool. projection_preflight gates this
# set the same as MUTATING_COMMANDS.
PROJECTING_COMMANDS = frozenset({"plan-review", "plan-analyze"})


@dataclass(frozen=True)
class ProjectionQueueHealth:
    live_size_bytes: int
    # live_depth is _DEPTH_NOT_MEASURED (-1) when a stat-only probe skipped the
    # bounded line scan; callers that need depth must request include_depth=True.
    live_depth: int
    live_depth_capped: bool
    has_orphan_draining: bool
    has_drainable_spool: bool
    can_auto_drain: bool
    auto_drain_skip_reason: str | None


@dataclass(frozen=True)
class ProjectionBreakerState:
    state: str
    consecutive_failures: int
    last_failure_returncode: int | None
    updated_at_epoch: float | None


def _breaker_threshold() -> int:
    return max(1, _int_env("WORKBAY_PROJECTION_BREAKER_FAILURE_THRESHOLD", BREAKER_FAILURE_THRESHOLD))


def max_replay_attempts() -> int:
    """Per-entry replay retry budget, re-read from env so tests can lower it."""
    return max(0, _int_env("WORKBAY_PROJECTION_MAX_REPLAY_ATTEMPTS", SPOOL_MAX_REPLAY_ATTEMPTS))


def _read_breaker_data(repo: Path) -> dict[str, Any]:
    path = repo / BREAKER_STATE_REL
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_breaker_data(repo: Path, data: dict[str, Any]) -> None:
    path = repo / BREAKER_STATE_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write atomically (temp + os.replace) so a concurrent reader never observes
    # a half-written file: _read_breaker_data swallows a JSONDecodeError to {},
    # which would momentarily report the breaker CLOSED while it should be open.
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def breaker_state(repo: Path) -> ProjectionBreakerState:
    data = _read_breaker_data(repo)
    failures = int(data.get("consecutive_failures") or 0)
    state = "open" if failures >= _breaker_threshold() else "closed"
    return ProjectionBreakerState(
        state=state,
        consecutive_failures=failures,
        last_failure_returncode=(
            int(data["last_failure_returncode"])
            if data.get("last_failure_returncode") is not None
            else None
        ),
        updated_at_epoch=(
            float(data["updated_at_epoch"])
            if data.get("updated_at_epoch") is not None
            else None
        ),
    )


def record_projection_failure(repo: Path, *, returncode: int | None = None) -> ProjectionBreakerState:
    data = _read_breaker_data(repo)
    failures = int(data.get("consecutive_failures") or 0) + 1
    _write_breaker_data(
        repo,
        {
            "consecutive_failures": failures,
            "last_failure_returncode": returncode,
            "updated_at_epoch": time.time(),
        },
    )
    return breaker_state(repo)


def record_projection_success(repo: Path) -> ProjectionBreakerState:
    _write_breaker_data(
        repo,
        {
            "consecutive_failures": 0,
            "last_failure_returncode": None,
            "updated_at_epoch": time.time(),
        },
    )
    return breaker_state(repo)


def dead_letter(
    repo: Path,
    payload: dict[str, Any],
    *,
    reason: str,
    hard_limit_bytes: int = DEAD_LETTER_HARD_LIMIT_BYTES,
) -> bool:
    """Append a refused event to the dead-letter sink; return False if shed.

    The sink is itself an accumulator, so it is bounded by ``hard_limit_bytes``.
    Once it is at/over the cap a further event cannot be recorded without
    unbounded disk growth, so it is shed and a single loud warning is emitted
    per process+repo. The boolean return lets callers see that the event was
    not durably parked instead of assuming the sink always accepts.
    """
    target = repo / DEAD_LETTER_REL
    try:
        if target.is_file() and target.stat().st_size >= hard_limit_bytes:
            key = str(target)
            if key not in _dead_letter_overflow_warned:
                _dead_letter_overflow_warned.add(key)
                sys.stderr.write(
                    f"projection dead-letter sink at {target} is over the "
                    f"{hard_limit_bytes}-byte hard limit; shedding further "
                    "refused events until it is drained ("
                    "make project-events-replay "
                    'LIFECYCLE_ARGS="--input <sink> --max-entries N --json", '
                    "then truncate the sink). Operator action required.\n"
                )
            return False
    except OSError:
        pass
    target.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "reason": reason,
        "recorded_at_epoch": time.time(),
        "payload": payload,
    }
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return True


def event_idempotency_key(payload: dict[str, Any]) -> str:
    """Return a stable replay-local key for a spooled projection event."""
    kind = str(payload.get("kind") or "")
    if kind == "decision":
        identity: tuple[Any, ...] = (
            kind,
            payload.get("session"),
            payload.get("task_ref"),
            payload.get("decision_id"),
            payload.get("branch"),
            payload.get("commit_sha"),
        )
    elif kind == "test_result":
        identity = (
            kind,
            payload.get("session"),
            payload.get("task_ref"),
            payload.get("command"),
            # Outcome fields are part of the identity: two genuinely distinct
            # results for the same command+session+commit (e.g. a flaky re-run at
            # the same HEAD while the backend is down -- fail then pass, both
            # spooled) must NOT collide to one key, or the second is dropped as a
            # false duplicate. verified_tests has no natural key to absorb this.
            payload.get("passed"),
            payload.get("exit_code"),
            payload.get("result"),
            payload.get("branch"),
            payload.get("commit_sha"),
        )
    elif kind == "state_sync":
        identity = (
            kind,
            payload.get("task_ref"),
            payload.get("target_branch"),
            payload.get("target_worktree_path"),
            payload.get("status"),
            payload.get("branch"),
            payload.get("commit_sha"),
        )
    else:
        identity = (kind, json.dumps(payload, sort_keys=True, default=str))
    blob = json.dumps(identity, sort_keys=False, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def dead_letter_count(
    repo: Path, *, limit: int = SPOOL_DEPTH_SUMMARY_LIMIT
) -> tuple[int, bool]:
    path = repo / DEAD_LETTER_REL
    if not path.is_file():
        return 0, False
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.strip():
                count += 1
                if count > limit:
                    return limit, True
    return count, False


def dead_letter_orphan_count(
    repo: Path, *, limit: int = SPOOL_DEPTH_SUMMARY_LIMIT
) -> tuple[int, bool]:
    """Count events stranded in orphan dead-letter drain snapshots.

    A crash after the drain claim-by-rename (or a partial drain from an older
    build) leaves ``projection-dead-letter.jsonl.draining.*`` files -- claims and
    their ``.checkpoint`` sidecars -- that the live-sink :func:`dead_letter_count`
    never sees. Counting them keeps stranded events visible in status/doctor and
    lets the auto-drain know it must reclaim them. Bounded like ``dead_letter_count``
    (returns ``(limit, True)`` once the cap is exceeded).
    """
    state_dir = repo / ".task-state"
    if not state_dir.is_dir():
        return 0, False
    count = 0
    for snapshot in sorted(state_dir.glob(DEAD_LETTER_DRAINING_GLOB)):
        if not snapshot.is_file():
            continue
        try:
            with snapshot.open("r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if line.strip():
                        count += 1
                        if count > limit:
                            return limit, True
        except OSError:
            continue
    return count, False


def read_dead_letter_drain_epoch(repo: Path) -> float | None:
    """Return the epoch of the last dead-letter auto-drain, or None if never."""
    path = repo / DEAD_LETTER_DRAIN_STATE_REL
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    raw = data.get("last_drain_at_epoch")
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def record_dead_letter_drain(repo: Path, *, epoch: float | None = None) -> None:
    """Stamp the dead-letter drain state file atomically (temp + os.replace)."""
    ts = time.time() if epoch is None else epoch
    path = repo / DEAD_LETTER_DRAIN_STATE_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(
        json.dumps({"last_drain_at_epoch": ts}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def dead_letter_last_drain_iso(repo: Path) -> str | None:
    """Return the last dead-letter drain time as a UTC ISO-8601 string, or None."""
    epoch = read_dead_letter_drain_epoch(repo)
    if epoch is None:
        return None
    import datetime as _dt  # noqa: PLC0415 -- localised; avoids a top-level import

    return _dt.datetime.fromtimestamp(epoch, _dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _relative_to_repo(repo: Path, path: Path) -> str:
    try:
        return str(path.relative_to(repo))
    except ValueError:
        return str(path)


def _sample_rows(
    path: Path,
    *,
    limit: int = _SAMPLE_ROWS,
    max_lines_scanned: int = _SAMPLE_SCAN_LINE_CAP,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    scanned = 0
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            # Bound the scan: a huge or all-corrupt spool must not be read to
            # EOF just to collect a 3-row sample (responsiveness constraint).
            scanned += 1
            if scanned > max_lines_scanned:
                break
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(row, dict):
                rows.append(row)
            if len(rows) >= limit:
                break
    return rows


def live_spool_size(repo: Path) -> int:
    """Return live projection spool size without reading the spool."""
    spool = repo / PENDING_EVENTS_REL
    if not spool.is_file():
        return 0
    try:
        return spool.stat().st_size
    except OSError:
        return 0


def _spool_depth_summary(repo: Path, *, limit: int) -> tuple[int, bool]:
    spool = repo / PENDING_EVENTS_REL
    if not spool.is_file():
        return 0, False
    count = 0
    with spool.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            count += 1
            if count > limit:
                return limit, True
    return count, False


def iter_jsonl_entries(path: Path) -> Iterator[dict[str, Any]]:
    if not path.is_file():
        return
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(entry, dict):
                yield entry


def spool_health(
    repo: Path,
    *,
    depth_limit: int = SPOOL_DEPTH_SUMMARY_LIMIT,
    auto_drain_max_bytes: int = AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES,
    include_depth: bool = True,
) -> ProjectionQueueHealth:
    live_size = live_spool_size(repo)
    # Depth is the only field that reads spool lines (bounded to depth_limit).
    # Stat-only hot probes (has_drainable_spool / auto_drain_skip_reason /
    # can_auto_drain) pass include_depth=False so they stay size+glob only, as
    # 9f7dba4e required to keep these per-command probes off the line-read path.
    if include_depth:
        live_depth, live_depth_capped = _spool_depth_summary(repo, limit=depth_limit)
    else:
        live_depth, live_depth_capped = _DEPTH_NOT_MEASURED, False
    state_dir = repo / ".task-state"
    has_orphan_draining = state_dir.is_dir() and any(state_dir.glob(DRAINING_GLOB))
    has_drainable = live_size > 0 or has_orphan_draining

    skip_reason: str | None = None
    if live_size > auto_drain_max_bytes:
        skip_reason = (
            f"live projection spool is {live_size} bytes; "
            "manual replay/quarantine required"
        )
    elif state_dir.is_dir():
        for snapshot in sorted(state_dir.glob(DRAINING_GLOB)):
            try:
                snapshot_size = snapshot.stat().st_size
            except OSError:
                continue
            if snapshot_size > auto_drain_max_bytes:
                skip_reason = (
                    f"claimed projection spool is {snapshot_size} bytes; "
                    "manual replay/quarantine required"
                )
                break

    return ProjectionQueueHealth(
        live_size_bytes=live_size,
        live_depth=live_depth,
        live_depth_capped=live_depth_capped,
        has_orphan_draining=has_orphan_draining,
        has_drainable_spool=has_drainable,
        can_auto_drain=has_drainable and skip_reason is None,
        auto_drain_skip_reason=skip_reason,
    )


def can_auto_drain(
    repo: Path,
    *,
    auto_drain_max_bytes: int = AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES,
) -> bool:
    return spool_health(
        repo,
        auto_drain_max_bytes=auto_drain_max_bytes,
        include_depth=False,
    ).can_auto_drain


def can_accept_spool_append(
    repo: Path,
    *,
    hard_limit_bytes: int = SPOOL_HARD_LIMIT_BYTES,
) -> bool:
    """Whether the live spool may still accept an append under the hard limit.

    Pure, stat-only size predicate. This is now the write-side gate, consumed by
    ``projection._spool``: an append over the hard limit routes the payload to
    the dead-letter sink (``dead_letter``) instead of growing the spool without
    bound. The hard byte limit is the *only* drop boundary on the write path --
    the circuit breaker gates whole commands at ``projection_preflight`` and must
    never divert an in-flight event here.
    """
    return live_spool_size(repo) < hard_limit_bytes


def spool_append(
    repo: Path,
    payload: dict[str, Any],
    *,
    reason: str,
    hard_limit_bytes: int = SPOOL_HARD_LIMIT_BYTES,
    dead_letter_hard_limit_bytes: int = DEAD_LETTER_HARD_LIMIT_BYTES,
) -> bool:
    """Append a projection event through the single gated spool chokepoint."""
    row = dict(payload)
    # Stamp a CONTENT-stable event_id (not a per-spooling uuid4): the handoff
    # write enforces exactly-once via the projection_event_dedupe PRIMARY KEY on
    # this id, so the SAME logical event re-spooled twice during one outage must
    # carry the SAME id or it double-applies on a keyless table (verified_tests).
    # This mirrors the online write path, which already uses the content key.
    row.setdefault("event_id", event_idempotency_key(row))
    if not can_accept_spool_append(repo, hard_limit_bytes=hard_limit_bytes):
        dead_letter(
            repo,
            row,
            reason=reason,
            hard_limit_bytes=dead_letter_hard_limit_bytes,
        )
        return False
    target = repo / PENDING_EVENTS_REL
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return True


def _handoff_cli_reachable(repo: Path) -> tuple[bool, int | None]:
    # Resolve the probe binary exactly as the projection write path does:
    # _common.mcp_handoff_bin prefers the workspace .venv console script. A bare
    # PATH "mcp-workbay-handoff" is normally absent in a venv-only install, so
    # probing it would FileNotFoundError -> 127 forever; the half-open breaker
    # could then never close and every mutating command would stay locked out
    # permanently. (mcp_handoff_bin already honours MCP_WORKBAY_HANDOFF_BIN.)
    from handlers import _common  # lazy import: avoid a queue<->handlers cycle

    binary = _common.mcp_handoff_bin(repo)
    try:
        proc = subprocess.run(
            [binary, "--help"],
            cwd=repo,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=6,
            check=False,
        )
    except FileNotFoundError:
        return False, 127
    except subprocess.TimeoutExpired:
        return False, 124
    except OSError:
        return False, 127
    return proc.returncode == 0, proc.returncode


def projection_preflight(repo: Path, command: str) -> dict[str, Any] | None:
    """Return a fail-fast receipt for unsafe mutating/projecting commands, else None."""
    if command in PREFLIGHT_EXEMPT_COMMANDS or command not in (
        MUTATING_COMMANDS | PROJECTING_COMMANDS
    ):
        return None

    state = breaker_state(repo)
    health = spool_health(repo, include_depth=False)
    hard_limit = _int_env("WORKBAY_PROJECTION_HARD_LIMIT_BYTES", SPOOL_HARD_LIMIT_BYTES)
    over_hard_limit = health.live_size_bytes >= hard_limit

    # Healthy: breaker closed and spool under the hard limit -> no probe, no cost.
    if state.state != "open" and not over_hard_limit:
        return None

    # Degraded. Probe the handoff CLI so an open breaker can self-heal
    # (half-open) the moment the backend is reachable again, rather than staying
    # latched until an operator deletes the breaker file by hand. The probe runs
    # whenever the breaker is open OR the spool is over the hard limit.
    reachable, returncode = _handoff_cli_reachable(repo)
    if reachable and not over_hard_limit:
        # The breaker was open only because the backend had been unreachable; it
        # is back and the spool is healthy, so close the breaker and admit.
        record_projection_success(repo)
        return None
    if not reachable:
        record_projection_failure(repo, returncode=returncode)
    # Still block before any git mutation: either the CLI is unreachable, or the
    # spool is over the hard limit (which a mere reachable ``--help`` must not
    # paper over -- the operator has to quarantine/drain the backlog first).

    if over_hard_limit:
        recovery_command = (
            'make project-events-replay LIFECYCLE_ARGS="--json --quarantine-oversized"'
        )
    else:
        recovery_command = (
            "handoff CLI unreachable; the breaker half-opens and auto-recovers "
            "once `mcp-workbay-handoff` responds to the next mutating command"
        )
    return {
        "ok": False,
        "command": command,
        "error": "projection_preflight_failed",
        "events": ["projection_preflight_blocked"],
        "handoff_projection": "pending",
        "recovery_command": recovery_command,
        "projection_queue": {
            "live_size_bytes": health.live_size_bytes,
            "hard_limit_bytes": hard_limit,
            "breaker_state": breaker_state(repo).state,
            "consecutive_failures": breaker_state(repo).consecutive_failures,
            "handoff_cli_reachable": reachable,
            "handoff_cli_returncode": returncode,
        },
    }


def _fsync_dir(path: Path) -> None:
    """Best-effort fsync of a directory so a copy-fallback move is durable."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _move_file(src: Path, dst: Path) -> None:
    try:
        os.rename(src, dst)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
    # Cross-device fallback: copy + fsync + delete (accepts transient 2x disk).
    try:
        with src.open("rb") as in_fh, dst.open("wb") as out_fh:
            shutil.copyfileobj(in_fh, out_fh)
            out_fh.flush()
            os.fsync(out_fh.fileno())
    except OSError:
        # Never leave a partial, manifest-less payload behind when the copy
        # fails mid-stream (e.g. ENOSPC under the disk pressure this guards
        # against). The source spool is still intact, so callers lose nothing.
        with contextlib.suppress(OSError):
            dst.unlink()
        raise
    _fsync_dir(dst.parent)
    src.unlink()
    # fsync the SOURCE directory too: without it a host crash right after the
    # unlink can leave the source spool's removal non-durable, so the multi-GiB
    # live spool reappears at full size after reboot while the manifest already
    # recorded live_spool_size_bytes=0 (and a re-run would duplicate the payload).
    _fsync_dir(src.parent)


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Write ``data`` as pretty JSON via temp-file + fsync + os.replace.

    Mirrors the durability posture of ``_write_breaker_data`` /
    ``_write_entries_atomic`` so a manifest can never be observed half-written
    (the corrupt-manifest window that made a quarantined payload unpurgeable).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps(data, indent=2, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def quarantine_live_spool(
    repo: Path,
    *,
    reason: str = "oversized_live_spool",
    soft_limit_bytes: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Move the live projection spool out of the hot path and write a manifest."""
    live = repo / PENDING_EVENTS_REL
    if not live.is_file():
        return {
            "quarantined": False,
            "reason": "live_spool_missing",
            "live_spool_size_bytes": 0,
        }

    live_stat = live.stat()
    original_size = live_stat.st_size
    if (
        soft_limit_bytes is not None
        and original_size <= soft_limit_bytes
        and not force
    ):
        return {
            "quarantined": False,
            "reason": "below_soft_limit",
            "original_size_bytes": original_size,
            "live_spool_size_bytes": original_size,
        }

    quarantine_dir = repo / QUARANTINE_REL
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    unique = f"{stamp}-{os.getpid()}-{time.time_ns()}"
    payload = quarantine_dir / f"pending-workflow-events.{unique}.jsonl"
    manifest = quarantine_dir / f"pending-workflow-events.{unique}.manifest.json"
    samples = _sample_rows(live)

    manifest_data: dict[str, Any] = {
        "schema_version": 1,
        "reason": reason,
        "original_path": str(PENDING_EVENTS_REL),
        "payload_path": _relative_to_repo(repo, payload),
        "manifest_path": _relative_to_repo(repo, manifest),
        "original_size_bytes": original_size,
        "original_mtime_epoch": live_stat.st_mtime,
        "live_spool_size_bytes": 0,
        "sample_rows": samples,
        # Automated replay-back via `project-events-replay --input` IS
        # implemented. Quarantined payloads are oversized by construction, so
        # the manifest advertises only the BOUNDED replay form
        # (--max-entries + --checkpoint): an unbounded --input drain would load
        # the whole multi-GiB payload into memory and re-trigger the very OOM
        # this quarantine exists to prevent.
        "replay_supported": True,
        "recovery_note": (
            "Payload preserved out of the hot path at "
            f"{_relative_to_repo(repo, payload)}. Replay it in BOUNDED batches "
            "(do not drain it unbounded -- it is oversized): make "
            'project-events-replay LIFECYCLE_ARGS="--input '
            f"{_relative_to_repo(repo, payload)} --max-entries 500 --checkpoint "
            f"{_relative_to_repo(repo, payload)}.checkpoint --json\". Do not "
            "delete this payload until it has been fully replayed or the loss "
            "is accepted. To reclaim disk after accepting the loss, run: make "
            'project-events-replay LIFECYCLE_ARGS="--purge-quarantine-manifest '
            f"{_relative_to_repo(repo, manifest)} --force --json\""
        ),
        "created_at_epoch": time.time(),
    }
    # Write the manifest atomically BEFORE moving the (multi-GiB) payload so the
    # payload can never exist without a manifest to purge it — closing the
    # orphan-payload window where a crash between move and manifest-write left the
    # quarantined payload permanently unpurgeable (purge reclaims disk only via a
    # manifest). A crash after this and before the move leaves only a tiny orphan
    # manifest pointing at a not-yet-existent payload (benign — purge tolerates a
    # missing payload), and the live spool is still intact.
    _atomic_write_json(manifest, manifest_data)
    try:
        _move_file(live, payload)
    except OSError:
        # The move failed (e.g. ENOSPC under the very disk pressure this guards):
        # drop the pre-written manifest so it cannot misreport a quarantine that
        # never happened. The source spool is intact, so the caller loses nothing.
        with contextlib.suppress(OSError):
            manifest.unlink()
        raise
    # Stamp completion only AFTER the payload is durably moved. A crash between the
    # manifest write above and this point leaves an orphan manifest WITHOUT
    # payload_moved (and live_spool_size_bytes:0 while the live spool is intact);
    # count_complete_quarantine_manifests excludes it so status does not overcount a
    # quarantine that never finished.
    manifest_data["payload_moved"] = True
    _atomic_write_json(manifest, manifest_data)
    return {
        "quarantined": True,
        "reason": reason,
        "payload_path": _relative_to_repo(repo, payload),
        "manifest_path": _relative_to_repo(repo, manifest),
        "original_size_bytes": original_size,
        "live_spool_size_bytes": 0,
        "sample_rows": samples,
    }


def purge_quarantined_payload(
    repo: Path,
    manifest_path: Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Delete a quarantined payload and update its manifest.

    ``force`` is required for now because replay-success integration lands in a
    later slice; this keeps disk relief explicit and auditable.
    """
    manifest = manifest_path
    if not manifest.is_absolute():
        manifest = repo / manifest
    rel_manifest = _relative_to_repo(repo, manifest)
    if not manifest.is_file():
        return {
            "purged": False,
            "reason": "manifest_missing",
            "manifest_path": rel_manifest,
        }
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        # A corrupt or unreadable manifest must not crash the operator recovery
        # command with a traceback; return a clean skip receipt instead.
        return {
            "purged": False,
            "reason": "manifest_unreadable",
            "manifest_path": rel_manifest,
        }
    if not isinstance(data, dict):
        return {
            "purged": False,
            "reason": "manifest_unreadable",
            "manifest_path": rel_manifest,
        }
    payload_value = data.get("payload_path")
    if not isinstance(payload_value, str) or not payload_value:
        return {
            "purged": False,
            "reason": "payload_path_missing",
            "manifest_path": rel_manifest,
        }
    if data.get("payload_purged"):
        # Idempotent: a prior purge already reclaimed the payload. Do not
        # re-run the delete or clobber the recorded reclaim audit
        # (payload_purged_bytes) with a fresh zero.
        return {
            "purged": False,
            "reason": "already_purged",
            "manifest_path": rel_manifest,
            "payload_path": payload_value,
            "payload_purged_bytes": data.get("payload_purged_bytes", 0),
        }
    if not force:
        return {
            "purged": False,
            "reason": "force_required",
            "manifest_path": rel_manifest,
            "payload_path": payload_value,
        }

    payload = Path(payload_value)
    if not payload.is_absolute():
        payload = repo / payload
    # Confine the unlink to the quarantine directory. quarantine_live_spool always
    # writes a quarantine-relative payload_path, but a tampered / hand-edited
    # manifest whose payload_path is absolute or escapes via '..' (or a symlink)
    # would otherwise let --force unlink an arbitrary writable file. Resolve and
    # require containment before deleting; otherwise return a clean skip receipt.
    quarantine_root = (repo / QUARANTINE_REL).resolve()
    try:
        resolved_payload = payload.resolve()
    except OSError:
        resolved_payload = payload
    if not resolved_payload.is_relative_to(quarantine_root):
        return {
            "purged": False,
            "reason": "payload_path_outside_quarantine",
            "manifest_path": rel_manifest,
            "payload_path": payload_value,
        }
    try:
        deleted_bytes = resolved_payload.stat().st_size
        resolved_payload.unlink()
    except FileNotFoundError:
        deleted_bytes = 0

    data["payload_purged"] = True
    data["payload_purged_at_epoch"] = time.time()
    data["payload_purged_bytes"] = deleted_bytes
    manifest.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "purged": True,
        "manifest_path": _relative_to_repo(repo, manifest),
        "payload_path": payload_value,
        "payload_purged_bytes": deleted_bytes,
    }


def count_complete_quarantine_manifests(repo: Path) -> int:
    """Count quarantine manifests whose payload was actually moved.

    Manifest-first ordering (the payload can never exist without a manifest) means
    a crash after the manifest write but before the move can leave an orphan
    manifest that records ``live_spool_size_bytes: 0`` while the live spool is
    still intact. Such an incomplete manifest must not be counted as a real
    quarantine. A manifest is complete when it carries ``payload_moved``, has
    already been purged, or its payload file still exists on disk.
    """
    quarantine_dir = repo / QUARANTINE_REL
    if not quarantine_dir.is_dir():
        return 0
    count = 0
    for manifest in quarantine_dir.glob("*.manifest.json"):
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        if data.get("payload_moved") or data.get("payload_purged"):
            count += 1
            continue
        payload_value = data.get("payload_path")
        if isinstance(payload_value, str) and payload_value:
            payload = Path(payload_value)
            if not payload.is_absolute():
                payload = repo / payload
            if payload.exists():
                count += 1
    return count
