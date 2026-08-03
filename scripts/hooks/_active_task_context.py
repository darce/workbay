#!/usr/bin/env python3
"""Shared active-task resolver for branch-isolation and advisory hooks.

Lifted from `_worktree_drift.py` so multiple hooks (PreToolUse drift
guard, SessionStart / UserPromptSubmit advisory) can resolve the active
task identity from the same source of truth without duplicating the
`workbay_handoff_mcp` import dance and the canonicalization logic.
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _resolve_package_src() -> Path:
    base = Path(__file__).resolve()
    candidates = (
        base.parents[2] / "packages" / "mcp-workbay-handoff" / "src",
        base.parents[3] / "mcp-workbay-handoff" / "src",
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


PACKAGE_SRC = _resolve_package_src()

if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

# CURRENT_TASK.json workspace-summary schema emitted by the handoff writer.
_SNAPSHOT_SCHEMA_VERSION = 2
# Wall-clock budget for the handoff-package fallback when the snapshot is
# absent or unusable. A Python import cannot be preempted in-process, so this
# is observability rather than a hard interrupt: the probe still returns its
# answer, but over-budget paths set probe_error so the cold path is not silent
# [OBS-08]. Overridable via WORKBAY_HANDOFF_FALLBACK_BUDGET_S (seconds).
_HANDOFF_FALLBACK_BUDGET_S = 1.0
_HANDOFF_FALLBACK_BUDGET_ENV = "WORKBAY_HANDOFF_FALLBACK_BUDGET_S"
# Live task statuses for snapshot fast-path selection. Mirrors
# workbay_handoff_mcp.shared_primitives.LIVE_ACTIVE_STATUSES — hooks cannot
# import the handoff package under the bare interpreter, so this is a local
# pin of the same three-element tuple.
_LIVE_ACTIVE_STATUSES: tuple[str, ...] = ("in_progress", "review", "blocked")
# Terminal task statuses for snapshot fast-path clean-negatives. Mirrors the
# handoff_state CHECK set minus the live statuses above — only ``done`` is
# terminal; hooks cannot import the handoff package, so this is a local pin.
_TERMINAL_TASK_STATUSES: tuple[str, ...] = ("done",)


def _handoff_fallback_budget_s() -> float:
    """Resolve the handoff-fallback wall-clock budget in seconds."""
    raw = (os.environ.get(_HANDOFF_FALLBACK_BUDGET_ENV) or "").strip()
    if not raw:
        return _HANDOFF_FALLBACK_BUDGET_S
    try:
        value = float(raw)
    except ValueError:
        return _HANDOFF_FALLBACK_BUDGET_S
    if value <= 0:
        return _HANDOFF_FALLBACK_BUDGET_S
    return value


@dataclass(frozen=True)
class ActiveTaskContext:
    task_ref: str | None
    target_worktree: str | None
    target_branch: str | None
    primary_worktree: str
    # implementation note D3: carried so advisory surfaces can detect plan-bound drift
    # (task_plan_path is a docs/plans/<NNNN>-*.md but the branch/worktree lack
    # the matching -plan<NNNN> suffix). Present in the same identity envelope
    # already fetched, so reading it needs no extra MCP round-trip. Defaulted so
    # the partial/empty constructions below stay 4-positional.
    task_plan_path: str | None = None
    # When the active task row was selected from several live candidates rather
    # than resolved unambiguously, carries a short note so the drift guard can
    # fall back instead of adjudicating path drift against a guessed row.
    resolution_note: str | None = None
    # Distinguishes "probe could not answer" from a clean "no active task".
    # Defaulted so existing 4-positional constructions keep working [CON-17].
    # Values are short stable tokens (not prose) so callers can discriminate
    # without string-matching sentences [OBS-08].
    probe_error: str | None = None


def _load_handoff_exports() -> tuple[Any, Any, Any, Any] | None:
    try:
        module = importlib.import_module("workbay_handoff_mcp")
    except ImportError:
        return None
    try:
        return (
            getattr(module, "RuntimeConfig"),
            getattr(module, "configure_runtime"),
            getattr(module, "get_handoff_state"),
            getattr(module, "UnresolvedTaskContextError", ValueError),
        )
    except AttributeError:
        return None


def _primary_workspace_root(workspace_root: Path) -> str | None:
    """Resolve the primary worktree path via on-disk git metadata.

    Thin delegate to ``_worktree_identity.primary_workspace_root`` so every
    caller shares one implementation [ARCH-13]. Resolution is pure filesystem
    work and cannot stall on process creation [RES-03].

    Propagates the identity helper's contract: returns ``None`` when the
    layout cannot be determined (e.g. outside any git repo). Never fabricates
    an answer by returning the caller's own directory [OBS-08]. Call sites
    that need a path anchor apply an explicit local fallback where that is
    intentional; guard paths must consume ``None`` fail-closed.

    An unimportable ``_worktree_identity`` (hooks dir not on ``sys.path``,
    overlay skew, nested import) degrades to ``None`` rather than raising:
    a guard that crashes is worse than one that cannot determine an answer.
    """
    try:
        from _worktree_identity import primary_workspace_root
    except ImportError:
        # ModuleNotFoundError is a subclass; bare-name import fails when the
        # hooks directory is not on sys.path [identity-import pin].
        return None

    return primary_workspace_root(Path(workspace_root))


def _ambiguity_fallback_disabled() -> bool:
    """True only when WORKBAY_GUARD_AMBIGUITY_FALLBACK is an explicit off token.

    Matching is case-insensitive after stripping surrounding whitespace. The
    empty string is not an off token, so the valve stays enabled by default.
    """
    return os.environ.get("WORKBAY_GUARD_AMBIGUITY_FALLBACK", "").strip().lower() in {
        "0",
        "false",
        "off",
        "no",
    }


def _empty_context(
    primary: str,
    *,
    probe_error: str | None = None,
    resolution_note: str | None = None,
) -> ActiveTaskContext:
    return ActiveTaskContext(
        task_ref=None,
        target_worktree=None,
        target_branch=None,
        primary_worktree=primary,
        task_plan_path=None,
        resolution_note=resolution_note,
        probe_error=probe_error,
    )


def _context_from_task_entry(
    entry: dict[str, Any],
    primary: str,
    *,
    resolution_note: str | None = None,
) -> ActiveTaskContext | None:
    """Build a context from a per-task projection / active payload.

    Returns a populated context only for live statuses (see
    ``_LIVE_ACTIVE_STATUSES``). A non-live or unrecognised status is not a
    confident snapshot answer — returns None so the caller falls through to
    the authoritative handoff probe rather than emitting a clean negative.
    """
    status = entry.get("status")
    if status not in _LIVE_ACTIVE_STATUSES:
        # Unrecognised / non-live: fall through. Do not claim "known and not
        # active" — that clean negative drops branch isolation silently.
        return None

    task_ref = entry.get("task_ref")
    target_worktree_path = entry.get("target_worktree_path")
    target_branch = entry.get("target_branch")
    task_plan_path = entry.get("task_plan_path")
    return ActiveTaskContext(
        task_ref=str(task_ref) if isinstance(task_ref, str) and task_ref else None,
        target_worktree=(
            str(target_worktree_path)
            if isinstance(target_worktree_path, str) and target_worktree_path
            else None
        ),
        target_branch=(
            str(target_branch)
            if isinstance(target_branch, str) and target_branch
            else None
        ),
        primary_worktree=primary,
        task_plan_path=(
            str(task_plan_path)
            if isinstance(task_plan_path, str) and task_plan_path
            else None
        ),
        resolution_note=resolution_note,
        probe_error=None,
    )


def _scan_workspace_ambiguous_tasks(
    tasks: list[Any],
    workspace_root: Path,
    primary: str,
) -> ActiveTaskContext | None:
    """Select among workspace_ambiguous tasks[] for this worktree.

    Prefer a live match (``_LIVE_ACTIVE_STATUSES``: in_progress, review,
    blocked) over any terminal match. Two live matches for the same worktree,
    or no match at all, are not confident snapshot answers: return None so the
    caller falls through to the handoff probe.

    When multiple live tasks exist in the workspace but exactly one matches
    this worktree, carry ``resolution_note`` so the drift guard's ambiguity
    fallback valve can stand down instead of blocking against a guessed row.
    """
    try:
        want = str(Path(workspace_root).expanduser().resolve(strict=False))
    except (RuntimeError, OSError, ValueError, TypeError):
        # Unusable snapshot outcome: fall through rather than emit a terminal
        # probe_error that short-circuits the authoritative probe [OBS-08].
        return None

    live_matches: list[dict[str, Any]] = []
    terminal_matches: list[dict[str, Any]] = []
    for entry in tasks:
        if not isinstance(entry, dict):
            continue
        entry_path = entry.get("target_worktree_path")
        if not isinstance(entry_path, str) or not entry_path:
            continue
        # expanduser can raise RuntimeError for an unresolvable ~user prefix;
        # one malformed entry must not abort the whole scan [CON-11][D6].
        try:
            canonical = _canonical_target_worktree(entry_path)
        except (RuntimeError, OSError, ValueError, TypeError):
            continue
        if canonical is None or canonical != want:
            continue
        status = entry.get("status")
        if status in _LIVE_ACTIVE_STATUSES:
            live_matches.append(entry)
        elif status in _TERMINAL_TASK_STATUSES:
            terminal_matches.append(entry)
        else:
            # Unrecognised status must never contribute to a clean negative —
            # fall through so the authoritative probe answers [OBS-08].
            return None

    # Env-bound identity is authoritative over worktree-path selection.
    # A lane binding can never be proven from the snapshot (no lane field),
    # so fall through whenever WORKBAY_LANE_ID is set [OBS-08][finding 9944].
    lane_id = os.environ.get("WORKBAY_LANE_ID", "").strip()
    if lane_id:
        return None
    active_pin = os.environ.get("WORKBAY_HANDOFF_ACTIVE_TASK", "").strip()

    if len(live_matches) > 1:
        # Ambiguous live rows are not a confident snapshot answer — fall through.
        return None
    if len(live_matches) == 1:
        entry = live_matches[0]
        if active_pin:
            # Serve only when the selected entry agrees with the env pin;
            # otherwise the DB resolver would answer a different task.
            task_ref = entry.get("task_ref")
            if not isinstance(task_ref, str) or task_ref != active_pin:
                return None
        # Multi-task workspace: keep the ambiguity valve alive so drift can
        # fall back when the env allows it, and stay fail-closed when
        # WORKBAY_GUARD_AMBIGUITY_FALLBACK disables the valve.
        note: str | None = None
        if not _ambiguity_fallback_disabled():
            note = (
                "snapshot_workspace_ambiguous: selected by target_worktree_path "
                "match among multiple live tasks"
            )
        return _context_from_task_entry(entry, primary, resolution_note=note)
    if terminal_matches:
        # Projection status is stale-capable (file lag vs DB). An affirmative
        # clean negative here fails open when the DB still has a live row —
        # fall through so the authoritative probe answers [OBS-08][finding 9984].
        return None
    # Tasks present but none name this worktree — not a confident snapshot
    # answer; fall through to the authoritative probe.
    return None


def _handoff_db_authority_mtime(
    db_path: Path,
    projection_dir: Path | None = None,
) -> float | None:
    """Newest mtime among handoff.db, -wal/-shm, and projection JSONs, or None.

    Under WAL mode a committed write updates the ``-wal`` sidecar while the
    main database file mtime can stay unchanged until checkpoint. The
    authority timestamp is therefore the newest mtime among whichever of the
    three DB files exist, plus the newest mtime among ``*.json`` files in
    ``projection_dir`` when that directory is provided. Returns None when none
    exist (undefined authority).

    Counterpart: workbay_handoff_mcp.current_task_rendering._handoff_db_authority_mtime
    — both copies must define the timestamp identically. Hooks cannot import
    the handoff package, so this helper is intentionally duplicated.
    """
    mtimes: list[float] = []
    for path in (
        db_path,
        Path(str(db_path) + "-wal"),
        Path(str(db_path) + "-shm"),
    ):
        try:
            mtimes.append(path.stat().st_mtime)
        except FileNotFoundError:
            continue
    if projection_dir is not None:
        try:
            if projection_dir.is_dir():
                for entry in projection_dir.iterdir():
                    if not entry.is_file() or entry.suffix != ".json":
                        continue
                    try:
                        mtimes.append(entry.stat().st_mtime)
                    except FileNotFoundError:
                        continue
        except OSError:
            raise
    if not mtimes:
        return None
    return max(mtimes)


def _snapshot_fresh_enough(snapshot_path: Path, payload: dict[str, Any]) -> bool:
    """Return True when CURRENT_TASK.json may be used as a cache answer.

    Stat-only comparison of snapshot mtime against the writer-stamped authority
    paths embedded in the payload (``authority_db_path`` +
    ``authority_projection_dir``): newest mtime among the db, -wal/-shm
    sidecars, and projection JSON files. No subprocess, no package import.
    When either authority field is missing/empty/non-string, the authority is
    newer than the snapshot, or mtimes cannot be determined, return False so
    the caller returns None and falls through to the handoff probe
    [REVHOO-RED-01]. Do not re-derive a hardcoded db path [DATA-01].

    A completely absent authority (no main file, no sidecars, no projection
    JSON) is not evidence of freshness: ``.task-state/`` is disposable, so
    serving an arbitrarily old CURRENT_TASK.json when authority is gone fails
    open. Return False so the caller falls through [OBS-08][finding 9943].
    A partially-initialised state (main file absent but ``-wal`` present)
    also answers not-fresh rather than computing an authority.
    When the payload names an authority database path and that path does not
    exist, and neither write-ahead sidecar (``-wal`` / ``-shm``) exists,
    answer not-fresh even if projection JSON files survive: those projections
    must never silently become the authority on their own [OBS-08].
    """
    try:
        snap_mtime = snapshot_path.stat().st_mtime
    except OSError:
        return False
    db_raw = payload.get("authority_db_path")
    proj_raw = payload.get("authority_projection_dir")
    if not isinstance(db_raw, str) or not db_raw:
        return False
    if not isinstance(proj_raw, str) or not proj_raw:
        return False
    db_path = Path(db_raw)
    projection_dir = Path(proj_raw)
    try:
        main_missing = not db_path.exists()
    except OSError:
        return False
    if main_missing:
        # Partial init (main gone, -wal/-shm present) is already not-fresh.
        # Named main absent with neither sidecar must also be not-fresh so
        # surviving projection files cannot collapse into a false authority
        # [OBS-08].
        return False
    try:
        authority = _handoff_db_authority_mtime(db_path, projection_dir)
    except OSError:
        return False
    if authority is None:
        # Fully absent authority is not freshness — fall through [finding 9943].
        return False
    return snap_mtime >= authority


def _try_load_active_task_from_snapshot(
    workspace_root: Path,
) -> ActiveTaskContext | None:
    """Answer from CURRENT_TASK.json when it can; None when it cannot.

    Handles the three shapes the writer emits (``single``, ``none``,
    ``workspace_ambiguous``). Missing or unusable snapshots return None so the
    caller falls through to the handoff package. Only a confident snapshot
    answer (resolved task, clean negative for a finished worktree task, or
    shape=none) returns an ActiveTaskContext. Unusable outcomes must not emit
    a terminal ``probe_error`` context that short-circuits the authoritative
    probe: that wedge hard-blocks primary-checkout edits under the drift guard.

    The snapshot is a freshness-checked cache, not an authority that replaces
    the handoff probe: when the DB authority timestamp is newer (or
    unstattable) this returns None so the caller falls through [REVHOO-RED-01].
    """
    primary = _primary_workspace_root(workspace_root)
    if primary is None:
        # Identity could not resolve a primary worktree (import degrade, layout
        # unknown). Do not fabricate an anchor and treat shape=none as a clean
        # negative: that composition fails OPEN when handoff.db is also missing
        # (identity-degrade/missing-DB). Fall through to the handoff probe so a
        # could-not-determine path carries probe_error rather than silence.
        return None

    snapshot_path = Path(primary) / "CURRENT_TASK.json"
    try:
        raw = snapshot_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        # Unreadable snapshot is not evidence about the task — fall through.
        return None

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None

    if not isinstance(parsed, dict):
        return None

    if not _snapshot_fresh_enough(snapshot_path, parsed):
        # Stale, pre-authority, or unstattable cache → fall through.
        return None

    # Unrecognised schema is not a confident answer — fall through.
    if parsed.get("schema_version") != _SNAPSHOT_SCHEMA_VERSION:
        return None

    shape = parsed.get("shape")
    if shape == "none":
        return _empty_context(primary, probe_error=None)

    if shape == "single":
        active = parsed.get("active")
        if not isinstance(active, dict):
            return None
        return _context_from_task_entry(active, primary)

    if shape == "workspace_ambiguous":
        tasks = parsed.get("tasks")
        if not isinstance(tasks, list):
            return None
        try:
            return _scan_workspace_ambiguous_tasks(tasks, workspace_root, primary)
        except (KeyError, TypeError, ValueError, RuntimeError):
            return None

    # Unrecognised shape value is not a confident answer — fall through.
    return None


def _with_over_budget_probe_error(result: ActiveTaskContext) -> ActiveTaskContext:
    """Label a successful fallback answer that exceeded the wall-clock budget.

    A slow answer is still better than no answer: keep task identity fields and
    surface ``handoff_probe_over_budget`` so the cold path is not silent [OBS-08].
    Paths that already carry a more specific probe_error keep that token.
    """
    if result.probe_error is not None:
        return result
    return ActiveTaskContext(
        task_ref=result.task_ref,
        target_worktree=result.target_worktree,
        target_branch=result.target_branch,
        primary_worktree=result.primary_worktree,
        task_plan_path=result.task_plan_path,
        resolution_note=result.resolution_note,
        probe_error="handoff_probe_over_budget",
    )


def _load_active_task_via_handoff(workspace_root: Path) -> ActiveTaskContext:
    """Authoritative handoff-package probe (import + get_handoff_state)."""
    exports = _load_handoff_exports()
    if exports is None:
        primary = _primary_workspace_root(workspace_root)
        if primary is None:
            primary = str(Path(workspace_root).resolve(strict=False))
        return _empty_context(primary, probe_error="handoff_unavailable")
    (
        RuntimeConfig,
        configure_runtime,
        get_handoff_state,
        unresolved_task_context_error,
    ) = exports

    try:
        runtime = RuntimeConfig.for_repo(workspace_root)
        configure_runtime(runtime)
        task_ref_arg = None
        resolution_note = None
        try:
            from workbay_handoff_mcp.shared_primitives import (
                resolve_active_task_ref_for_hook,
            )
            from workbay_handoff_mcp.shared_schema import _get_db_connection

            strict = _ambiguity_fallback_disabled()
            with _get_db_connection() as conn:
                resolution = resolve_active_task_ref_for_hook(conn, strict=strict)
            task_ref_arg = resolution.task_ref
            resolution_note = resolution.tiebreak_note
        except Exception:
            # Resolver is an optimisation: it supplies a task_ref overload for
            # get_handoff_state. When it cannot supply one (import failure, no
            # active task, ambiguity, DB error), fall through to the
            # sections-only call so the existing response-message handler can
            # classify the outcome. Re-raise only when the operator kill switch
            # demands fail-closed (WORKBAY_GUARD_AMBIGUITY_FALLBACK off).
            if _ambiguity_fallback_disabled():
                raise
        # Prefer the task_ref overload when the resolver returned one. Older
        # get_handoff_state callables may not accept task_ref=; fall back to the
        # sections-only signature only for that overload mismatch.
        if task_ref_arg:
            try:
                raw = get_handoff_state(task_ref=task_ref_arg, sections="identity")
            except TypeError:
                raw = get_handoff_state(sections="identity")
        else:
            raw = get_handoff_state(sections="identity")
    except Exception as exc:
        if isinstance(exc, unresolved_task_context_error):
            raise
        primary = _primary_workspace_root(workspace_root)
        if primary is None:
            primary = str(Path(workspace_root).resolve(strict=False))
        return ActiveTaskContext(
            task_ref=None,
            target_worktree=None,
            target_branch=None,
            primary_worktree=primary,
            probe_error="handoff_probe_failed",
        )

    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return ActiveTaskContext(
            task_ref=None,
            target_worktree=None,
            target_branch=None,
            primary_worktree=str(Path(runtime.workspace_root).resolve(strict=False)),
            probe_error="handoff_json_invalid",
        )

    if isinstance(parsed, dict) and parsed.get("ok") is False:
        error = parsed.get("error")
        if not isinstance(error, str):
            data = parsed.get("data")
            if isinstance(data, dict) and isinstance(data.get("error"), str):
                error = data.get("error")
        if isinstance(error, str) and (
            "Ambiguous active task" in error
            or "No active task in handoff_state" in error
        ):
            raise ValueError(error)

    data = parsed.get("data") if isinstance(parsed, dict) else None
    active = data.get("active") if isinstance(data, dict) else None
    if not isinstance(active, dict):
        # Package answered but identity envelope lacked an active object —
        # could not determine from the probe payload [OBS-08].
        return ActiveTaskContext(
            task_ref=None,
            target_worktree=None,
            target_branch=None,
            primary_worktree=str(Path(runtime.workspace_root).resolve(strict=False)),
            probe_error="handoff_active_missing",
        )

    task_ref = active.get("task_ref")
    target_worktree_path = active.get("target_worktree_path")
    target_branch = active.get("target_branch")
    task_plan_path = active.get("task_plan_path")
    return ActiveTaskContext(
        task_ref=str(task_ref) if isinstance(task_ref, str) and task_ref else None,
        target_worktree=(
            str(target_worktree_path)
            if isinstance(target_worktree_path, str) and target_worktree_path
            else None
        ),
        target_branch=(
            str(target_branch)
            if isinstance(target_branch, str) and target_branch
            else None
        ),
        primary_worktree=str(runtime.workspace_root),
        task_plan_path=(
            str(task_plan_path)
            if isinstance(task_plan_path, str) and task_plan_path
            else None
        ),
        resolution_note=resolution_note,
        probe_error=None,
    )


def _load_active_task(workspace_root: Path) -> ActiveTaskContext:
    # Cheap snapshot cache first (freshness-checked against handoff.db); never
    # import the package when the snapshot already answered — including clean
    # negatives for a finished worktree task. Stale/missing snapshot → None
    # and fall through to the probe [REVHOO-RED-01].
    snapshot = _try_load_active_task_from_snapshot(workspace_root)
    if snapshot is not None:
        return snapshot

    # Absent/unusable snapshot: measure the whole handoff fallback (import +
    # get_handoff_state). Cannot preempt an in-process import, so over-budget
    # is observability rather than a hard interrupt [OBS-08].
    started = time.monotonic()
    budget = _handoff_fallback_budget_s()
    result = _load_active_task_via_handoff(workspace_root)
    if (time.monotonic() - started) > budget:
        return _with_over_budget_probe_error(result)
    return result


def _workspace_root() -> Path:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return Path.cwd()
    if proc.returncode == 0 and proc.stdout.strip():
        return Path(proc.stdout.strip())
    return Path.cwd()


def _canonical_target_worktree(target_worktree_path: str | None) -> str | None:
    if not target_worktree_path:
        return None
    # expanduser raises RuntimeError when ~user cannot be resolved; treat as
    # uncanonicalizable rather than crashing the hook [CON-11].
    try:
        return str(Path(target_worktree_path).expanduser().resolve(strict=False))
    except (RuntimeError, OSError, ValueError):
        return None
