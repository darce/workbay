"""Bounded single-flight retirement sweep for terminal lane worktrees.

Admission-opportunity fairness applies within each objective class (worktree
or branch-only): once eligible, no other eligible candidate of that class is
admitted twice before it is admitted once. Eligibility excludes live retry
deadlines, live owners, dirty worktrees and failed probes, with typed keep
reasons. Branch-only work follows eligible worktree-bearing candidates.
Admission requires the actual remaining allowance before pre-close snapshot
collection/decision, whose time consumes that allowance. ``--max`` bounds
admissions; lack of an allowance reports retire_admission_starved. This is not
a pass-count or elapsed-time guarantee. Attempts survive restart; the sweep
does not change an eligible row's updated_at unless it closes that row.
Dry run and apply resolve one pass budget and evaluate one admission gate; a
dry run reports would_* decisions and never runs a mutating actuator.
"""

from __future__ import annotations

import argparse
import ctypes
import fcntl
import json
import math
import multiprocessing
import os
import queue
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO
from unittest.mock import patch

if TYPE_CHECKING:
    from workbay_orchestrator_mcp.orchestration.lane_disposition import SharedReads

_MODULE = "workbay_orchestrator_mcp.orchestration.lane_retire_sweep"
_LOCK_RELATIVE = Path(".task-state/locks/lane-retire-sweep.lock")
_LAUNCH_LOCK_RELATIVE = Path(".task-state/locks/lane-retire-sweep-launch.lock")
_RECEIPT_RELATIVE = Path(".task-state/lane-retire-sweep.jsonl")
_RETRY_STATE_RELATIVE = Path(".task-state/lane-retire-sweep-state.json")
_SCAN_COST_RELATIVE = Path(".task-state/lane-retire-scan-cost.json")
_SCAN_COST_PHASES = (
    "lane_rows_s",
    "tombstone_drain_s",
    "worktree_inventory_s",
    "branch_inventory_s",
    "identity_and_linkage_filter_s",
)
_SCAN_COST_SAMPLE_LIMIT = 8
_SCAN_COST_FRESH_S = 3600.0
_SCAN_COST_BOOTSTRAP_S = 30.0
_CHILD_MARKER_RELATIVE = Path(".task-state/lane-retire-sweep-child.json")
_WAKE_MARKER_RELATIVE = Path(".task-state/lane-retire-sweep-wake.json")
_LOG_RELATIVE = Path(".task-state/lane-retire-sweep.log")
_RECEIPT_LIMIT_BYTES = 1024 * 1024
_STATE_MAX_ENTRIES = 500
_STATE_TTL_S = 7 * 24 * 60 * 60
_RETRY_BASE_S = 5 * 60
_RETRY_MAX_S = 60 * 60
_DETAIL_CAP = 200
_RETIRE_SPAWN_DETAIL_CAP = 200
# Plural token components such as max_tokens may redact; hiding a possible credential is safer.
_RETIRE_SPAWN_SECRET_RE = re.compile(
    r"(?i)(?<![a-z0-9_-])"
    r"(?P<label>(?:[a-z0-9]+[-_]+)*[-_]*(?:api[-_]?key|tokens?|secret|password|passwd|credential)"
    r"(?:[-_]+[a-z0-9]+)*[-_]*)"
    r"(?P<key_quote>[\"']?)"
    r"(?P<separator>\s*[:=]\s*)"
    r"(?:"
    r'(?P<double_quote>")(?P<double_value>(?:\\.|[^"\\])*)(?P=double_quote)'
    r"|(?P<single_quote>')(?P<single_value>(?:\\.|[^'\\])*)(?P=single_quote)"
    r"|(?P<value>[^\s,;]+)"
    r")"
    r"|(?<![a-z0-9_-])(?P<scheme>bearer)(?P<scheme_separator>\s+)(?P<bearer_value>[^\s,;]+)"
)
_RETIRE_SPAWN_PATH_RE = re.compile(r"(?<![\w.])/(?:[^\s,;:'\"]+)")


def _clamp_retire_operation_timeout(value: object) -> float:
    try:
        timeout = 45.0 if value is None else float(value)
    except (TypeError, ValueError):
        timeout = 45.0
    if math.isnan(timeout):
        timeout = 45.0
    return min(300.0, max(5.0, timeout))


def _retire_operation_timeout_from_env() -> float:
    return _clamp_retire_operation_timeout(os.environ.get("WORKBAY_RETIRE_OPERATION_TIMEOUT_S"))


_RETIRE_OPERATION_TIMEOUT_S = _retire_operation_timeout_from_env()
_RETIRE_TIMEOUT_CLEANUP_S = 0.2
# The child may spend 300 seconds waiting for landing quiescence, then use its
# 30-second sweep budget before its marker is considered expired.
_SWEEP_CHILD_LIFETIME_S = 360.0
_SWEEP_CHILD_LIFETIME_MARGIN_S = 30.0
# Interpreter startup and imports happen before the child can report alive.
# Keep that budget separate from the ready handshake, whose purpose is to bound
# runtime configuration and identity verification.
_CHILD_HANDSHAKE_TIMEOUT_S = 5.0
_CHILD_STARTUP_ALLOWANCE_ENV = "WORKBAY_RETIRE_CHILD_STARTUP_ALLOWANCE_S"
_CHILD_STARTUP_ALLOWANCE_DEFAULT_S = 30.0
_CHILD_STARTUP_ALLOWANCE_MIN_S = 5.0
_CHILD_STARTUP_ALLOWANCE_MAX_S = 120.0
_CHILD_STARTUP_POLL_INTERVAL_S = 0.1
_RETIRE_EXPECTED_CHILD_STARTUP_S = 3.0


def _child_startup_allowance_from_env() -> float:
    raw_value = os.environ.get(_CHILD_STARTUP_ALLOWANCE_ENV)
    try:
        value = _CHILD_STARTUP_ALLOWANCE_DEFAULT_S if raw_value is None else float(raw_value)
    except (TypeError, ValueError):
        value = _CHILD_STARTUP_ALLOWANCE_DEFAULT_S
    if math.isnan(value):
        value = _CHILD_STARTUP_ALLOWANCE_DEFAULT_S
    return min(_CHILD_STARTUP_ALLOWANCE_MAX_S, max(_CHILD_STARTUP_ALLOWANCE_MIN_S, value))


_CHILD_STARTUP_ALLOWANCE_S = _child_startup_allowance_from_env()
_RETIRE_CHILD_STARTUP_ALLOWANCE_S = _CHILD_STARTUP_ALLOWANCE_S + _CHILD_HANDSHAKE_TIMEOUT_S
_RUNTIME_CONFIGURATION_API: tuple[Any, Any] | None = None
_RUNTIME_CONFIGURATION_API_ERROR: BaseException | None = None
_SWEEP_LOG_LIMIT_BYTES = 256 * 1024
_DEFAULT_SWEEP_BUDGET_S = 30.0
_MAX_PASS_BUDGET_S = 600.0
_DEFAULT_MAX_LANES = 20
_MAX_BACKGROUND_SWEEP_CONTINUATIONS = 10
_DEFAULT_LANDING_QUIESCENCE_WAIT_S = 300.0
_DEFAULT_SWEEP_WAKE_MAX_DELAY_S = 3600.0
_DEFAULT_SWEEP_WAKE_MAX_IDLE_CHAIN = 6
_SWEEP_WAKE_MIN_DELAY_S = 30.0
_SWEEP_WAKE_POLL_S = 30.0
_LANDING_QUIESCENCE_POLL_S = 0.25
_GIT_STATUS_TIMEOUT_S = 5.0
_PROCESS_PROBE_TIMEOUT_S = 5.0
_IDENTITY_PROBE_FAMILIES = ("pass_lookup", "git_facts", "absorbed_landing")


def _empty_identity_filter_timing() -> dict[str, Any]:
    return {
        "rows_visited": 0,
        **{family: {"calls": 0, "seconds": 0.0} for family in _IDENTITY_PROBE_FAMILIES},
    }


def _rounded_identity_filter_timing(values: dict[str, Any]) -> dict[str, Any]:
    return {
        "rows_visited": int(values.get("rows_visited") or 0),
        **{
            family: {
                "calls": int((values.get(family) or {}).get("calls") or 0),
                "seconds": round(max(0.0, float((values.get(family) or {}).get("seconds") or 0.0)), 3),
            }
            for family in _IDENTITY_PROBE_FAMILIES
        },
    }


_ACTIVE_MERGED_PROBE_TIMEOUT_S = 5.0
_DEFAULT_WORK_COMPLETE_GRACE_S = 30 * 60.0
# A review pass that ended without leaving a report keeps its lane this long
# (harvest or commit lag) before the sweep closes it as review_failed_no_report.
_REVIEW_NO_REPORT_GRACE_S = 30 * 60.0
_REVIEW_SCRATCH_GLOBS = (".review/**",)
# Review-retirement reason codes; additive to the classifier's own reasons.
REVIEW_NO_PASS_RECORD = "no_pass_record"
REVIEW_REPORT_MISSING = "review_report_missing"
REVIEW_FAILED_NO_REPORT = "review_failed_no_report"
REVIEW_TERMINAL_TIME_UNKNOWN = "terminal_time_unknown"
REVIEW_SUBJECT_UNKNOWN = "review_subject_unknown"
REVIEW_SUBJECT_UNREVIEWED_EVENT = "review_subject_unreviewed"
_Popen = subprocess.Popen
_RETIRE_PHASE_TIMING_KEYS = (
    "runtime_configuration_s",
    "bundle_s",
    "worktree_removal_s",
    "branch_deletion_s",
    "handoff_row_writes_s",
    "disposition_s",
    "owner_probe_s",
    "other_s",
)
_RETIRE_PHASE_TIMING_MAX_S = 24 * 60 * 60.0
_RETIRE_PROGRESS_PATH: Path | None = None
_RETIRE_PROGRESS_ERROR: str | None = None
_RETIRE_RUNTIME_ELAPSED_S = 0.0


class _BudgetExhausted(RuntimeError):
    """Lane discovery reached the sweep deadline after bounded progress."""

    outcome = "budget_exhausted"

    def __init__(
        self,
        *,
        pages_fetched: int,
        rows_fetched: int,
        total_matching: int | None,
    ) -> None:
        super().__init__("budget_exhausted")
        self.pages_fetched = pages_fetched
        self.rows_fetched = rows_fetched
        self.total_matching = total_matching


def _monotonic() -> float:
    return time.monotonic()


def _load_runtime_configuration_api() -> tuple[Any, Any]:
    from workbay_handoff_mcp import configure_runtime  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    return configure_runtime, RuntimeConfig


def _configure_runtime(repo_root: Path) -> Path:
    """Bind handoff APIs to this repository before touching lane state."""
    if _RUNTIME_CONFIGURATION_API_ERROR is not None:
        raise _RUNTIME_CONFIGURATION_API_ERROR
    configure_runtime, RuntimeConfig = _RUNTIME_CONFIGURATION_API or _load_runtime_configuration_api()
    config = RuntimeConfig.for_repo(repo_root)
    configure_runtime(config)
    return Path(config.workspace_root).resolve()


def _try_acquire_lock(repo_root: Path) -> tuple[TextIO | None, str | None]:
    return _try_acquire_file_lock(repo_root, _LOCK_RELATIVE)


def _release_lock(handle: TextIO) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _try_acquire_file_lock(repo_root: Path, relative: Path) -> tuple[TextIO | None, str | None]:
    path = repo_root / relative
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return None, "already_running"
        return handle, None
    except OSError as exc:
        return None, f"sweep_lock_failed:{type(exc).__name__}:{exc}"


def _atomic_json_replace(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _list_lane_rows_page(
    *,
    task_ref: str | None,
    offset: int,
    after_id: int | None,
    limit: int,
) -> dict[str, Any]:
    """Fetch one lane-list page using the appropriate task scope."""
    if task_ref is not None:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            _lane_payload_dict,
        )

        return _lane_payload_dict(
            manage_worktree_lane(
                operation="list",
                task_ref=task_ref,
                status="all",
                limit=limit,
                offset=offset,
            )
        )

    from workbay_orchestrator_mcp.orchestration.lane_worktree import (  # noqa: PLC0415
        _list_lanes,
    )

    envelope = _list_lanes(after_id=after_id, limit=limit)
    if not isinstance(envelope, dict):
        return {}
    data = envelope.get("data")
    if not isinstance(data, dict):
        return envelope
    flattened = dict(envelope)
    flattened.update(data)
    return flattened


def _load_lane_rows(
    repo_root: Path,
    task_ref: str | None,
    *,
    deadline: float,
    progress: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Read a complete paged lane-row snapshot, checking the deadline per page."""
    del repo_root  # Listing is bound to the configured handoff runtime.
    from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
        _MERGECLOSE_LIST_MAX_PAGES,
        _MERGECLOSE_LIST_PAGE_SIZE,
    )

    all_lanes: list[Any] = []
    after_id: int | None = None
    offset = 0
    last_listed: dict[str, Any] = {}
    pages_fetched = 0
    total_matching: int | None = None

    def update_progress() -> None:
        if progress is not None:
            progress.update(
                {
                    "pages_fetched": pages_fetched,
                    "rows_fetched": len(all_lanes),
                    "total_matching": total_matching,
                }
            )

    def budget_exhausted() -> _BudgetExhausted:
        update_progress()
        return _BudgetExhausted(
            pages_fetched=pages_fetched,
            rows_fetched=len(all_lanes),
            total_matching=total_matching,
        )

    for page_idx in range(_MERGECLOSE_LIST_MAX_PAGES):
        if _monotonic() >= deadline:
            raise budget_exhausted()
        listed = _list_lane_rows_page(
            task_ref=task_ref,
            offset=offset,
            after_id=after_id,
            limit=_MERGECLOSE_LIST_PAGE_SIZE,
        )
        last_listed = listed
        if not isinstance(listed, dict) or listed.get("ok") is not True:
            detail = listed.get("error") if isinstance(listed, dict) else None
            raise RuntimeError(str(detail or "lane_list_failed"))
        page = listed.get("lanes")
        if not isinstance(page, list):
            raise RuntimeError(str(listed.get("error") or "lane_list_malformed"))
        all_lanes.extend(page)
        pages_fetched = page_idx + 1
        raw_total = listed.get("total_matching")
        total_matching = raw_total if type(raw_total) is int else None
        update_progress()
        if any(not isinstance(row, dict) for row in all_lanes):
            raise RuntimeError("lane_list_malformed")

        if _monotonic() >= deadline:
            raise budget_exhausted()

        if task_ref is not None:
            if listed.get("has_more") is not True:
                return all_lanes
            if not page:
                break
            offset += len(page)
            continue

        has_more = listed.get("has_more")
        if not isinstance(has_more, bool):
            raise RuntimeError("cross_task_list_lanes has_more missing or non-bool")
        if not has_more:
            if type(raw_total) is not int:
                raise RuntimeError("cross_task_list_lanes total_matching missing or non-int")
            if raw_total != len(all_lanes):
                raise RuntimeError(f"total_matching drift: collected={len(all_lanes)} total_matching={raw_total}")
            return all_lanes
        if not page:
            break
        next_cursor = listed.get("next_after_id")
        if type(next_cursor) is not int:
            raise RuntimeError("cross_task_list_lanes next_after_id missing while has_more")
        after_id = next_cursor
    else:
        raise RuntimeError(str(last_listed.get("list_truncation_reason") or "lane_list_truncated"))

    raise RuntimeError(str(last_listed.get("list_truncation_reason") or "lane_list_truncated"))


class _PathUnresolvable(RuntimeError):
    """A non-blank path that Path.resolve() cannot canonicalise: its identity is unknown, not absent."""


def _resolved_path(path: object, *, relative_to: Path | None = None) -> str | None:
    """Canonical path, or None only for blank or absent input. A failed resolve() raises _PathUnresolvable."""
    if not isinstance(path, (str, os.PathLike)) or not str(path).strip():
        return None
    candidate = Path(path).expanduser()
    if not candidate.is_absolute() and relative_to is not None:
        candidate = relative_to / candidate
    try:
        return str(candidate.resolve())
    except (OSError, RuntimeError, ValueError) as exc:
        raise _PathUnresolvable(f"{type(exc).__name__}:{exc}") from exc


def _resolved_path_or_none(path: object) -> str | None:
    """Probe view of _resolved_path: unresolvable reads as None, which the owner and cwd probes already fail closed on."""
    try:
        return _resolved_path(path)
    except _PathUnresolvable:
        return None


def _list_linked_worktrees(
    repo_root: Path,
    *,
    timeout_s: float = _GIT_STATUS_TIMEOUT_S,
) -> dict[str, dict[str, Any]]:
    """Return resolved paths from Git's worktree registry, including primary."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
            timeout=min(_GIT_STATUS_TIMEOUT_S, timeout_s),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"worktree_list_failed:{type(exc).__name__}:{exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"worktree_list_failed:{(proc.stderr or proc.stdout).strip()}")

    worktrees: dict[str, dict[str, Any]] = {}
    for block in proc.stdout.split("\n\n"):
        path: str | None = None
        branch: str | None = None
        bare = False
        for line in block.splitlines():
            if line.startswith("worktree "):
                path = line[len("worktree ") :]
            elif line.startswith("branch "):
                branch = line[len("branch ") :]
            elif line == "bare":
                bare = True
        try:
            resolved = _resolved_path(path)
        except _PathUnresolvable as exc:
            # Dropping this block would read a registered worktree as absent: the inventory itself fails.
            raise RuntimeError(f"worktree_list_failed:path_unresolvable:{exc}") from exc
        if resolved is not None:
            worktrees[resolved] = {"path": path, "branch": branch, "bare": bare}
    if not worktrees:
        raise RuntimeError("worktree_list_empty")
    return worktrees


def _git_status(worktree_path: str, *, timeout_s: float = _GIT_STATUS_TIMEOUT_S) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", worktree_path, "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, min(_GIT_STATUS_TIMEOUT_S, timeout_s)),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def _only_deleted_tracked(status_output: str) -> bool:
    lines = status_output.splitlines()
    return bool(lines) and all(line.startswith(" D ") for line in lines)


def _worktree_branch_matches(worktree_path: str, recorded_branch: str, *, timeout_s: float) -> bool:
    expected = recorded_branch.strip().removeprefix("refs/heads/")
    if not expected:
        return False
    try:
        result = subprocess.run(
            ["git", "-C", worktree_path, "symbolic-ref", "--quiet", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, timeout_s),
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and (result.stdout or "").strip() == expected


def _restore_torn_worktree(worktree_path: str, *, timeout_s: float) -> bool:
    try:
        result = subprocess.run(
            ["git", "-C", worktree_path, "restore", "--worktree", "--", "."],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, timeout_s),
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _process_snapshot(*, timeout_s: float = _PROCESS_PROBE_TIMEOUT_S) -> tuple[list[str], list[str]] | None:
    """Capture cwd and command-line evidence once for the whole sweep."""
    lsof: subprocess.CompletedProcess[str] | None = None
    ps: subprocess.CompletedProcess[str] | None = None
    per_probe_timeout = max(0.01, min(_PROCESS_PROBE_TIMEOUT_S, timeout_s / 2))
    try:
        lsof = subprocess.run(
            ["lsof", "-a", "-d", "cwd", "-Fn"],
            capture_output=True,
            text=True,
            check=False,
            timeout=per_probe_timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        ps = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            capture_output=True,
            text=True,
            check=False,
            timeout=per_probe_timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass
    if lsof is None or ps is None:
        return None
    if lsof.returncode != 0 or ps.returncode != 0:
        return None
    cwd_paths = [line[1:].removesuffix(" (deleted)") for line in lsof.stdout.splitlines() if line.startswith("n")]
    command_lines = [line.strip() for line in ps.stdout.splitlines() if line.strip()]
    return cwd_paths, command_lines


def _path_is_within(path: str, parent: str) -> bool:
    try:
        Path(path).expanduser().resolve().relative_to(Path(parent).expanduser().resolve())
        return True
    except (OSError, RuntimeError, ValueError):
        return False


def _has_live_process(worktree_path: str, snapshot: tuple[list[str], list[str]]) -> bool:
    cwd_paths, command_lines = snapshot
    if any(_path_is_within(path, worktree_path) for path in cwd_paths):
        return True
    return any(worktree_path in command for command in command_lines)


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _retire_lane(
    *, lane_id: str | None, task_ref: str | None, apply: bool, delete_merged_branch: bool
) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import _retire_worktree_lane  # noqa: PLC0415

    return _retire_worktree_lane(
        lane_id=lane_id,
        task_ref=task_ref,
        apply=apply,
        delete_merged_branch=delete_merged_branch,
    )


def _read_task_manifest(repo_root: Path, task_ref: str) -> dict[str, Any] | None:
    if not task_ref or Path(task_ref).name != task_ref:
        return None
    try:
        value = json.loads((repo_root / "config/lane-orchestration" / f"{task_ref}.json").read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _full_commit_sha(value: object) -> str | None:
    candidate = str(value or "").strip()
    if len(candidate) != 40 or any(character not in "0123456789abcdefABCDEF" for character in candidate):
        return None
    return candidate.lower()


def _retire_timeout_receipt_keys(repo_root: Path) -> set[tuple[str, str]]:
    """Read a bounded tail of durable receipts for lane-specific retire timeouts."""
    receipt = repo_root / _RECEIPT_RELATIVE
    keys: set[tuple[str, str]] = set()
    for path in (receipt, receipt.with_name(f"{receipt.name}.1")):
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                start = max(0, size - _RECEIPT_LIMIT_BYTES)
                handle.seek(start)
                contents = handle.read(_RECEIPT_LIMIT_BYTES)
        except OSError:
            continue
        lines = contents.splitlines()
        if start and lines:
            lines = lines[1:]
        for line in lines:
            try:
                receipt_row = json.loads(line)
            except (UnicodeDecodeError, ValueError):
                continue
            if not isinstance(receipt_row, dict):
                continue
            lane_timings = receipt_row.get("lane_timings")
            could_not_act = receipt_row.get("could_not_act")
            lane_timings = lane_timings if isinstance(lane_timings, list) else []
            could_not_act = could_not_act if isinstance(could_not_act, list) else []
            lane_timeout = any(
                isinstance(item, dict) and item.get("outcome") == "retire_timeout" for item in lane_timings
            )
            failure_timeout = any(
                isinstance(item, dict) and item.get("reason") == "retire_timeout" for item in could_not_act
            )
            if not lane_timeout and not failure_timeout:
                continue
            entries = [
                *(item for item in lane_timings if isinstance(item, dict) and item.get("outcome") == "retire_timeout"),
                *(item for item in could_not_act if isinstance(item, dict) and item.get("reason") == "retire_timeout"),
            ]
            for item in entries:
                task = item.get("task_ref")
                lane = item.get("lane_id")
                if isinstance(task, str) and task and isinstance(lane, str) and lane:
                    keys.add((task, lane))
    return keys


def _worktree_head_sha(worktree_path: str, *, timeout_s: float) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", worktree_path, "rev-parse", "--verify", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, timeout_s),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return _full_commit_sha(result.stdout.strip()) if result.returncode == 0 else None


def _has_torn_removal_evidence(
    repo_root: Path,
    row: dict[str, Any],
    worktree_path: str,
    *,
    work_complete: bool,
    timeout_receipts: set[tuple[str, str]],
    timeout_s: float,
) -> bool:
    """Require an exact durable intent or a matching legacy timeout receipt."""
    lane_id = str(row.get("lane_id") or "")
    task_ref = str(row.get("task_ref") or "")
    branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    if (
        not lane_id
        or not task_ref
        or not branch
        or not _worktree_branch_matches(worktree_path, branch, timeout_s=timeout_s)
    ):
        return False
    head_sha = _worktree_head_sha(worktree_path, timeout_s=timeout_s)
    if head_sha is None:
        return False
    from . import worktree_tombstone  # noqa: PLC0415

    if worktree_tombstone.has_intent_for_worktree(
        repo_root,
        lane_id=lane_id,
        task_ref=task_ref,
        worktree_path=worktree_path,
        branch=branch,
        head_sha=head_sha,
    ):
        return True
    from workbay_orchestrator_mcp.orchestration.lane_postmerge import TERMINAL_STATUSES  # noqa: PLC0415

    if str(row.get("status") or "").strip().lower() not in TERMINAL_STATUSES and not work_complete:
        return False
    if (task_ref, lane_id) not in timeout_receipts:
        return False
    recorded_tip = row.get("branch_tip_sha")
    if recorded_tip:
        expected_tip = _full_commit_sha(recorded_tip)
    else:
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_root), "rev-parse", "--verify", f"refs/heads/{branch}"],
                capture_output=True,
                text=True,
                check=False,
                timeout=max(0.01, timeout_s),
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        expected_tip = _full_commit_sha(result.stdout.strip()) if result.returncode == 0 else None
    return expected_tip is not None and head_sha == expected_tip


def _manifest_supersedes_lane(manifest: dict[str, Any], lane_id: str) -> bool:
    lanes = manifest.get("lanes")
    if not isinstance(lanes, dict):
        return False
    if lane_id not in lanes:
        return True
    for other_lane, entry in lanes.items():
        if other_lane == lane_id or not isinstance(entry, dict):
            continue
        supersedes = entry.get("supersedes")
        if isinstance(supersedes, list) and lane_id in supersedes:
            return True
    return False


def _latest_lane_pass_from_index(
    task_ref: str,
    lane_id: str,
    indexed: Any,
) -> dict[str, Any] | None:
    from .offload_pass import _latest_pass_payload  # noqa: PLC0415

    if indexed.status != "ok":
        return {"status": "unknown", "pass_id": None}
    records = []
    for record in indexed.records:
        value = record.payload
        if value.get("task_ref") != task_ref or value.get("lane_id") != lane_id:
            continue
        records.append(record)
    if not records:
        return None
    latest = _latest_pass_payload(records)
    return latest if latest is not None else {"status": "unknown", "pass_id": None}


def _latest_lane_pass(
    repo_root: Path,
    task_ref: str,
    lane_id: str,
    pass_records_reader: Any | None = None,
) -> dict[str, Any] | None:
    """Read the latest pass, using the sweep snapshot only for candidate filtering.

    Calls without a reader recheck durable records before authorizing release
    or retirement; unreadable records fail closed in both paths.
    """
    if pass_records_reader is not None:
        indexed = pass_records_reader.lane_records(task_ref, lane_id)
        return _latest_lane_pass_from_index(task_ref, lane_id, indexed)

    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    from .offload_pass_index import lane_pass_records  # noqa: PLC0415

    state_dir = RuntimeConfig.for_repo(repo_root).state_dir
    indexed = lane_pass_records(state_dir, task_ref=task_ref, lane_id=lane_id)
    return _latest_lane_pass_from_index(task_ref, lane_id, indexed)


def _authoritative_base_sha(
    repo_root: Path,
    row: dict[str, Any],
    manifest: dict[str, Any] | None,
    pass_record: dict[str, Any] | None,
    *,
    timeout_s: float,
    deadline: float | None = None,
) -> tuple[str | None, str | None]:
    """Resolve a pinned base from lane evidence, falling back to branch creation reflog."""
    candidates: list[tuple[object, str]] = [(row.get("base_sha"), "lane_row")]
    lane_id = str(row.get("lane_id") or "")
    lanes = manifest.get("lanes") if isinstance(manifest, dict) else None
    entry = lanes.get(lane_id) if isinstance(lanes, dict) else None
    if isinstance(entry, dict):
        candidates.append((entry.get("base_sha"), "manifest"))
    if isinstance(pass_record, dict):
        result = pass_record.get("result")
        candidates.extend(
            (
                (pass_record.get("base_sha"), "pass_state"),
                (result.get("base_sha") if isinstance(result, dict) else None, "pass_result"),
            )
        )
    for value, source in candidates:
        resolved = _full_commit_sha(value)
        if resolved is not None:
            return resolved, source

    branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    if not branch or timeout_s <= 0:
        return None, None
    if deadline is not None:
        timeout_s = min(timeout_s, deadline - _monotonic())
    if timeout_s <= 0:
        return None, "deadline_exceeded"
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "reflog", "show", "--reverse", "--format=%H", f"refs/heads/{branch}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_s,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    if proc.returncode != 0:
        return None, None
    for line in proc.stdout.splitlines():
        resolved = _full_commit_sha(line)
        if resolved is not None:
            return resolved, "branch_creation_reflog"
    return None, None


def _no_work_terminal_pass(pass_record: object) -> bool:
    if not isinstance(pass_record, dict) or str(pass_record.get("status") or "").strip().lower() != "done":
        return False
    result = pass_record.get("result")
    outcome = result.get("outcome") if isinstance(result, dict) else None
    from .offload_pass import PASS_OUTCOMES  # noqa: PLC0415
    from .pass_terminality import pass_outcome_is_terminal  # noqa: PLC0415

    return isinstance(outcome, str) and outcome in PASS_OUTCOMES and pass_outcome_is_terminal(outcome)


def _pass_outcome_detail(outcome: object) -> str:
    if outcome is None:
        return "<missing>"
    if isinstance(outcome, str):
        return outcome or "<empty>"
    return repr(outcome)


def _no_work_commit_count(
    repo_root: Path,
    row: dict[str, Any],
    pass_record: dict[str, Any],
) -> tuple[int | None, str]:
    """Use the recorded lane base and current branch tip for own-work proof."""
    from workbay_orchestrator_mcp.lane_reaping import _no_work_lane_commit_count  # noqa: PLC0415

    return _no_work_lane_commit_count(row=row, pass_record=pass_record, repo_root=repo_root)


def _work_complete_grace_s() -> tuple[float | None, str | None]:
    raw = os.environ.get("WORKBAY_SWEEP_WORK_COMPLETE_GRACE_S")
    if raw is None:
        return _DEFAULT_WORK_COMPLETE_GRACE_S, None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None, "invalid_work_complete_grace"
    if not math.isfinite(value) or value < 0:
        return None, "invalid_work_complete_grace"
    return value, None


def _review_harvest_exists(*, task_ref: str, lane_id: str, branch: str, pass_id: str | None) -> bool | None:
    """Check handoff review ledgers through their read-only connection seam."""
    from workbay_orchestrator_mcp.orchestration.lane_reclaim import _scan_read_connection  # noqa: PLC0415

    try:
        with _scan_read_connection() as conn:
            run_refs = tuple(dict.fromkeys(value for value in (lane_id, pass_id) if value))
            finding_row = conn.execute(
                "SELECT 1 FROM review_findings WHERE task_ref = ? "
                "AND (lane_id = ? OR review_run_id = ? OR review_run_id = ?) LIMIT 1",
                (task_ref, lane_id, run_refs[0] if run_refs else "", run_refs[-1] if run_refs else ""),
            ).fetchone()
            if finding_row is not None:
                return True
            review_run_row = conn.execute(
                "SELECT 1 FROM review_runs WHERE task_ref = ? "
                "AND (branch = ? OR branch = ? OR subject_path = ? OR review_run_id = ?) LIMIT 1",
                (task_ref, branch, f"refs/heads/{branch}", branch, pass_id or ""),
            ).fetchone()
            return review_run_row is not None
    except Exception:  # noqa: BLE001 — an unreadable receipt is not evidence of harvest
        return None


def _close_lane_cas(
    row: dict[str, Any],
    *,
    target_status: str,
    note: str,
    deadline: float | None = None,
    rule: str | int | None = None,
    disposition_kind: str | None = None,
    authorized_tip: str | None = None,
    evidence: dict[str, Any] | None = None,
    integration_ref: str | None = None,
    integration_tip: str | None = None,
) -> dict[str, Any]:
    """Close only the exact observed lane row revision and prune terminal inbox rows."""
    from workbay_handoff_mcp.shared_schema import HANDOFF_SQLITE_BUSY_TIMEOUT_MS, _get_db_connection  # noqa: PLC0415

    task_ref = str(row.get("task_ref") or "")
    lane_id = str(row.get("lane_id") or "")
    observed_id = row.get("id")
    observed_status = str(row.get("status") or "")
    observed_updated_at = row.get("updated_at")
    if not task_ref or not lane_id or type(observed_id) is not int:
        return {"ok": False, "reason": "lane_row_revision_unavailable"}
    if (rule is not None or disposition_kind is not None) and authorized_tip is None:
        return {"ok": False, "reason": "authority_mismatch"}
    if deadline is not None and deadline - _monotonic() <= 0:
        return {"ok": False, "reason": "deadline_exceeded"}

    def _record_moved_after_close_blocker() -> str:
        """Persist the typed ``moved_after_close`` event once per tip; ``""`` or the failure reason."""
        description = json.dumps(
            {"kind": "moved_after_close", "row_id": observed_id, "authorized_tip": row.get("branch_tip_sha")},
            sort_keys=True,
        )
        try:
            with _get_db_connection() as conn:
                conn.execute(
                    "INSERT INTO blockers (task_ref, lane_id, description, status) "
                    "SELECT ?, ?, ?, 'open' WHERE NOT EXISTS ("
                    "SELECT 1 FROM blockers WHERE task_ref = ? AND lane_id = ? AND description = ? AND status = 'open')",
                    (task_ref, lane_id, description, task_ref, lane_id, description),
                )
        except Exception as blocker_exc:  # noqa: BLE001 — explicitly report failed event persistence
            return f"moved_after_close:blocker_event_failed:{type(blocker_exc).__name__}"
        return ""

    try:
        # The shared connection factory's BEGIN IMMEDIATE uses its fixed
        # busy_timeout. For a deadline-bound close, open through the same
        # registered writer path, lower this connection's timeout to the
        # remaining budget, and then acquire the write lock explicitly.
        connection_kwargs = {"deadline": deadline} if deadline is not None else {}
        with _get_db_connection(begin_immediate=deadline is None, **connection_kwargs) as conn:
            if deadline is not None:
                conn.isolation_level = None
                remaining_s = deadline - _monotonic()
                if remaining_s <= 0:
                    return {"ok": False, "reason": "deadline_exceeded"}
                busy_timeout_ms = min(HANDOFF_SQLITE_BUSY_TIMEOUT_MS, int(remaining_s * 1000))
                conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
                remaining_s = deadline - _monotonic()
                if remaining_s <= 0:
                    return {"ok": False, "reason": "deadline_exceeded"}
                remaining_ms = min(HANDOFF_SQLITE_BUSY_TIMEOUT_MS, int(remaining_s * 1000))
                if remaining_ms < busy_timeout_ms:
                    conn.execute(f"PRAGMA busy_timeout={remaining_ms}")
                if deadline - _monotonic() <= 0:
                    return {"ok": False, "reason": "deadline_exceeded"}
                conn.execute("BEGIN IMMEDIATE")
                if deadline - _monotonic() <= 0:
                    return {"ok": False, "reason": "deadline_exceeded"}
            columns = {item["name"] for item in conn.execute("PRAGMA table_info(worktree_lanes)")}
            identity_select = ", identity_seq" if "identity_seq" in columns else ""
            snapshot_keys = (
                "id",
                "status",
                "updated_at",
                "notes",
                "worktree_path",
                "branch",
                "lane_kind",
                "branch_tip_sha",
                "owner_agent",
            )
            current = conn.execute(
                f"SELECT {', '.join(snapshot_keys)}{identity_select} "
                "FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
                (task_ref, lane_id),
            ).fetchone()
            if current is None:
                return {"ok": False, "reason": "lane_row_missing"}
            current_row = dict(current)
            if (
                current_row.get("id") != observed_id
                or current_row.get("status") != observed_status
                or current_row.get("updated_at") != observed_updated_at
                or any(
                    current_row.get(key) != row.get(key)
                    for key in ("notes", "worktree_path", "branch", "lane_kind", "branch_tip_sha", "owner_agent")
                )
                or ("identity_seq" in row and current_row.get("identity_seq") != row["identity_seq"])
            ):
                return {"ok": False, "reason": "lane_row_revision_changed"}
            prior_note = str(current_row.get("notes") or "").strip()
            if prior_note:
                try:
                    note_envelope = json.loads(prior_note)
                except ValueError:
                    note_envelope = None
                if isinstance(note_envelope, dict):
                    close_notes = note_envelope.get("close_notes")
                    if isinstance(close_notes, list):
                        close_notes.append(note)
                    elif close_notes is None:
                        note_envelope["close_notes"] = [note]
                    else:
                        note_envelope["close_notes"] = [close_notes, note]
                    notes = json.dumps(note_envelope, sort_keys=True)
                else:
                    notes = f"{prior_note} [{note}]"
            else:
                notes = note
            if authorized_tip is not None:
                if not isinstance(authorized_tip, str) or re.fullmatch(r"[0-9a-f]{40}", authorized_tip) is None:
                    return {"ok": False, "reason": "authority_mismatch"}
                # Always use the existing JSON note envelope. Plain legacy
                # notes remain as close_notes rather than being discarded.
                try:
                    note_envelope = json.loads(notes)
                except ValueError:
                    note_envelope = None
                if not isinstance(note_envelope, dict):
                    note_envelope = {"close_notes": [notes]}
                note_envelope.pop("close_authority", None)
                notes = json.dumps(note_envelope, sort_keys=True)
            if deadline is not None and deadline - _monotonic() <= 0:
                return {"ok": False, "reason": "deadline_exceeded"}
            updated = conn.execute(
                "UPDATE worktree_lanes SET status = ?, notes = ?, "
                + ("branch_tip_sha = ?, " if authorized_tip is not None else "")
                + "updated_at = datetime('now') "
                "WHERE id = ? AND status = ? AND updated_at IS ?",
                (
                    target_status,
                    notes,
                    *((authorized_tip,) if authorized_tip is not None else ()),
                    observed_id,
                    observed_status,
                    observed_updated_at,
                ),
            )
            if int(updated.rowcount or 0) != 1:
                return {"ok": False, "reason": "lane_row_revision_changed"}
            if authorized_tip is not None:
                closed_row = dict(
                    conn.execute(
                        f"SELECT {', '.join(snapshot_keys)}{identity_select} FROM worktree_lanes WHERE id = ?",
                        (observed_id,),
                    ).fetchone()
                )
                closed_identity = {
                    key: closed_row.get(key) for key in ("id", "identity_seq", "branch", "worktree_path", "owner_agent")
                }
                note_envelope["close_authority"] = {
                    "schema": 1,
                    "rule": rule,
                    "disposition_kind": disposition_kind,
                    "authorized_tip": authorized_tip,
                    "evidence": evidence or {},
                    "integration_ref": integration_ref,
                    "integration_tip": integration_tip,
                    "closed_identity": closed_identity,
                    # Strip only close_authority from notes when comparing the
                    # legacy exact snapshot: embedding itself would recurse.
                    "closed_snapshot": closed_row,
                }
                conn.execute(
                    "UPDATE worktree_lanes SET notes = ? WHERE id = ?",
                    (json.dumps(note_envelope, sort_keys=True), observed_id),
                )
            if deadline is not None and deadline - _monotonic() <= 0:
                conn.rollback()
                return {"ok": False, "reason": "deadline_exceeded"}
            conn.execute(
                "DELETE FROM lane_messages WHERE task_ref = ? AND lane_id = ? AND status != 'open'",
                (task_ref, lane_id),
            )
            if deadline is not None and deadline - _monotonic() <= 0:
                conn.rollback()
                return {"ok": False, "reason": "deadline_exceeded"}
        result: dict[str, Any] = {"ok": True, "status": target_status}
        if target_status == "blocked" and note == "moved_after_close":
            # A reopened row carries the same typed event as a refused reopen.
            blocker_event = _record_moved_after_close_blocker()
            if blocker_event:
                result["blocker_event"] = blocker_event
        return result
    except Exception as exc:  # noqa: BLE001 — the row stays live when durable close fails
        if target_status == "blocked" and note == "moved_after_close":
            # Some deployed transition rules can leave closed rows terminal.
            # Surface a durable, typed blocker; retries deduplicate this event.
            blocker_event = _record_moved_after_close_blocker()
            if blocker_event:
                return {"ok": False, "reason": blocker_event}
            return {
                "ok": False,
                "reason": f"moved_after_close:{type(exc).__name__}",
                "blocker_type": "moved_after_close",
                "blocker_recorded": True,
            }
        if deadline is not None:
            remaining_s = deadline - _monotonic()
            busy_timeout_expired = isinstance(exc, sqlite3.OperationalError) and any(
                word in str(exc).lower() for word in ("locked", "busy")
            )
            if remaining_s <= 0 or (busy_timeout_expired and remaining_s <= 0.005):
                return {"ok": False, "reason": "deadline_exceeded"}
        return {"ok": False, "reason": f"close_failed:{type(exc).__name__}"}


def _work_complete_git_facts(
    repo_root: Path,
    row: dict[str, Any],
    manifest: dict[str, Any] | None,
    pass_record: dict[str, Any] | None,
    integration_refs: list[str],
    *,
    timeout_s: float,
    deadline: float | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Collect a pinned base and ask disposition code to prove branch facts."""
    branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    if not branch:
        return None, "superseded_branch_missing"
    base_sha, base_source = _authoritative_base_sha(
        repo_root,
        row,
        manifest,
        pass_record,
        timeout_s=timeout_s,
        deadline=deadline,
    )
    if base_sha is None:
        if base_source == "deadline_exceeded":
            return None, "deadline_exceeded"
        return None, "superseded_base_sha_unavailable"
    from workbay_orchestrator_mcp.orchestration.lane_disposition import (  # noqa: PLC0415
        collect_superseded_lane_git_facts,
    )

    return collect_superseded_lane_git_facts(
        repo_root,
        branch,
        base_sha,
        base_source,
        integration_refs,
        timeout_s=timeout_s,
        deadline=deadline,
    )


def _review_pass_terminal_age_s(pass_record: dict[str, Any]) -> float | None:
    """Seconds since the pass went terminal; None when no recorded stamp parses."""
    # offload_pass._finish records terminal_at; start/creation are not
    # evidence of when a potentially long-running review ended.
    stamp = _parse_timestamp(pass_record.get("terminal_at"))
    if stamp is not None:
        return max(0.0, (datetime.now(timezone.utc) - stamp).total_seconds())
    return None


def _review_subject_unreviewed_event(
    row: dict[str, Any], pass_record: dict[str, Any], *, task_ref: str, lane_id: str
) -> dict[str, Any]:
    """Typed event naming what a report-less review left unreviewed (at-least-once)."""
    from workbay_orchestrator_mcp.orchestration.secure_sandbox import (  # noqa: PLC0415
        review_subject_from_lane_row,
    )

    refs = review_subject_from_lane_row(row)
    return {
        "type": REVIEW_SUBJECT_UNREVIEWED_EVENT,
        "reason": REVIEW_FAILED_NO_REPORT,
        "task_ref": task_ref,
        "review_lane_id": lane_id,
        "review_pass_id": pass_record.get("pass_id"),
        "subject_lane_id": None,
        "subject_base_ref": refs[0] if refs else None,
        "subject_tip_ref": refs[1] if refs else None,
    }


def _resolve_subject_lane_id(event: dict[str, Any], rows: list[dict[str, Any]]) -> str | None:
    """Name the one implement lane whose branch or tip is the review's subject tip."""
    tip_ref = event.get("subject_tip_ref")
    if not isinstance(tip_ref, str) or not tip_ref:
        return None
    wanted = tip_ref.removeprefix("refs/heads/")
    matches = [
        str(candidate.get("lane_id"))
        for candidate in rows
        if candidate.get("task_ref") == event.get("task_ref")
        and candidate.get("lane_id")
        and candidate.get("lane_id") != event.get("review_lane_id")
        and candidate.get("lane_kind") != "review"
        and (
            str(candidate.get("branch") or "").removeprefix("refs/heads/") == wanted
            or candidate.get("branch_tip_sha") == tip_ref
        )
    ]
    return matches[0] if len(matches) == 1 else None


def retire_preserved_review_lane(
    *,
    repo_root: Path,
    row: dict[str, Any],
    apply: bool = True,
    retire: bool = True,
    remaining_s: float = 60.0,
    deadline: float | None = None,
    emit_event: Callable[[dict[str, Any]], None] | None = None,
    latest_pass_lookup: Callable[[str, str], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """One preservation/terminalization owner for pass completion and sweep.

    The sweep retains its bounded retirement scheduler (``retire=False``).
    Pass completion retires immediately with the same landing mutation fence.
    Lower-level retirement still owns bundle pinning and global owner guards.

    Beyond ``review_harvested``, two scratch-only cases retire a review lane:
    ``no_pass_record`` (a terminal row with no pass ever recorded) and
    ``review_failed_no_report`` (the latest pass is terminal, the grace has
    passed and no report exists). The latter closes the row, so it only runs
    when ``emit_event`` can carry the unreviewed-subject event to the journal.
    """
    from workbay_orchestrator_mcp.orchestration import lane_disposition  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.pass_terminality import (  # noqa: PLC0415
        pass_outcome_is_terminal,
    )

    status = str(row.get("status") or "")
    deadline = deadline if deadline is not None else _monotonic() + max(0.0, float(remaining_s))

    def keep(reason: str, **details: Any) -> dict[str, Any]:
        return {"ok": False, "status": status, "reason": reason, **details}

    def remaining() -> float:
        return deadline - _monotonic()

    def probe_timeout(limit_s: float = 5.0) -> float | None:
        remaining_s_now = remaining()
        return min(limit_s, remaining_s_now) if remaining_s_now > 0 else None

    if row.get("lane_kind") != "review":
        return keep("not_review_lane")
    if remaining() <= 0:
        return keep("deadline_exceeded")
    task_ref = str(row.get("task_ref") or "")
    lane_id = str(row.get("lane_id") or "")
    branch = str(row.get("branch") or "").removeprefix("refs/heads/")
    worktree = str(row.get("worktree_path") or "")
    try:
        with _retire_owner_lock(repo_root) as acquired:
            if acquired is not True:
                return keep("retire_owner_active")
            if remaining() <= 0:
                return keep("deadline_exceeded")
            pass_record = (
                latest_pass_lookup(task_ref, lane_id)
                if latest_pass_lookup is not None
                else _latest_lane_pass(repo_root, task_ref, lane_id)
            )
            if remaining() <= 0:
                return keep("deadline_exceeded")
            if status in lane_disposition._TERMINAL_STATUSES and worktree and not Path(worktree).exists():
                timeout_s = probe_timeout()
                if timeout_s is None:
                    return keep("deadline_exceeded")
                try:
                    absent = subprocess.run(
                        ["git", "-C", str(repo_root), "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
                        capture_output=True,
                        text=True,
                        check=False,
                        timeout=timeout_s,
                    )
                except (OSError, subprocess.TimeoutExpired):
                    return keep("deadline_exceeded" if remaining() <= 0 else "review_git_probe_failed")
                if remaining() <= 0:
                    return keep("deadline_exceeded")
                if absent.returncode == 1:
                    # Both resources are already gone: no close or deletion is authorized here.
                    # Physical idempotence says nothing about the latest review's harvest.
                    absent_reason = REVIEW_NO_PASS_RECORD
                    evidence: dict[str, Any] = {"pass_record": None}
                    if pass_record is not None:
                        if isinstance(pass_record, dict) and pass_record.get("status") != "done":
                            return keep(
                                "latest_pass_not_terminal",
                                pass_id=pass_record.get("pass_id"),
                                pass_status=pass_record.get("status"),
                            )
                        latest, outcome, evidence = lane_disposition._review_terminal_pass(
                            repo_root, (row,), pass_record
                        )
                        if latest is None:
                            refusal = evidence.get("reason") or (
                                "unknown_pass_outcome"
                                if "unknown_pass_outcome" in evidence
                                else "latest_pass_not_terminal"
                            )
                            return keep(str(refusal), evidence=evidence)
                        absent_reason = "review_retired_unharvested"
                        result = latest["result"]
                        evidence = {
                            "pass_id": latest.get("pass_id"),
                            "pass_outcome": outcome,
                            "review_verdict": result.get("review_verdict"),
                            "harvest_verdict": result.get("harvest_verdict"),
                        }
                        harvest = result.get("findings_harvest")
                        if harvest is not None and not isinstance(harvest, dict):
                            return keep("receipt_malformed:findings_harvest", evidence=evidence)
                        if isinstance(harvest, dict):
                            refusal = lane_disposition._harvest_receipt_refusal(harvest)
                            if refusal is not None:
                                return keep(refusal, evidence=evidence)
                            # Only the native complete, fully recorded per-pass run is proof.
                            # Legacy lane/branch matches cannot certify this latest pass.
                            if outcome == "review_complete" and "review_run_receipt" in harvest:
                                from .lane_reclaim import _scan_read_connection  # noqa: PLC0415
                                from .offload_pass import _PASSING_HARVEST_VERDICTS, _harvest_verdict  # noqa: PLC0415

                                if _harvest_verdict(harvest) not in _PASSING_HARVEST_VERDICTS:
                                    return keep("review_harvest_failed", evidence=evidence)
                                if remaining() <= 0:
                                    return keep("deadline_exceeded", evidence=evidence)
                                try:
                                    with _scan_read_connection() as conn:
                                        conn.execute(f"PRAGMA busy_timeout = {max(0, int(remaining() * 1000))}")
                                        receipt = conn.execute(
                                            "SELECT branch, commit_sha, verdict FROM review_runs "
                                            "WHERE task_ref = ? AND review_run_id = ?",
                                            (task_ref, latest.get("pass_id")),
                                        ).fetchone()
                                except Exception as exc:  # noqa: BLE001 — unreadable is not absent
                                    return keep(
                                        "deadline_exceeded" if remaining() <= 0 else "review_harvest_lookup_failed",
                                        evidence=evidence,
                                        error=type(exc).__name__,
                                    )
                                if remaining() <= 0:
                                    return keep("deadline_exceeded", evidence=evidence)
                                if receipt is not None:
                                    if (
                                        not isinstance(latest.get("pass_id"), str)
                                        or not latest["pass_id"]
                                        or not isinstance(latest.get("branch"), str)
                                        or not latest["branch"]
                                        or latest["branch"].removeprefix("refs/heads/") != branch
                                        or not lane_disposition._is_full_object_id(latest.get("tip_sha"))
                                        or receipt["branch"] != latest["branch"]
                                        or receipt["commit_sha"] != latest["tip_sha"]
                                    ):
                                        return keep("review_run_receipt_identity_mismatch", evidence=evidence)
                                    absent_reason = "review_harvested"
                                    evidence.update(harvest_receipt=True, receipt_verdict=receipt["verdict"])
                    return {
                        "ok": True,
                        "status": status,
                        "reason": absent_reason,
                        "evidence": evidence,
                        "retire": {"ok": True, "outcome": "already_retired"},
                    }
            reason_code = "review_harvested"
            if pass_record is None:
                # No pass was ever recorded. An unreadable history comes back as
                # {"status": "unknown"} instead and stays latest_pass_not_terminal.
                if status not in lane_disposition._TERMINAL_STATUSES:
                    return keep(REVIEW_NO_PASS_RECORD, pass_id=None, pass_status=None)
                reason_code = REVIEW_NO_PASS_RECORD
            else:
                pass_result = pass_record.get("result")
                pass_outcome = pass_result.get("outcome") if isinstance(pass_result, dict) else None
                if pass_record.get("status") != "done":
                    return keep(
                        "latest_pass_not_terminal",
                        pass_id=pass_record.get("pass_id"),
                        pass_status=pass_record.get("status"),
                    )
                from .offload_pass import PASS_OUTCOMES  # noqa: PLC0415

                if not isinstance(pass_outcome, str) or pass_outcome not in PASS_OUTCOMES:
                    return keep(
                        "unknown_pass_outcome",
                        evidence={"unknown_pass_outcome": _pass_outcome_detail(pass_outcome)},
                    )
                if not pass_outcome_is_terminal(pass_outcome):
                    return keep(
                        "latest_pass_not_terminal",
                        pass_id=pass_record.get("pass_id"),
                        pass_status=pass_record.get("status"),
                    )
            manifest = _read_task_manifest(repo_root, task_ref)
            if remaining() <= 0:
                return keep("deadline_exceeded")
            names = [str((manifest or {}).get("integration_branch") or "main")]
            timeout_s = probe_timeout()
            if timeout_s is None:
                return keep("deadline_exceeded")
            facts, error = _work_complete_git_facts(
                repo_root,
                row,
                manifest,
                pass_record,
                names,
                timeout_s=timeout_s,
                deadline=deadline,
            )
            if remaining() <= 0 or error == "deadline_exceeded":
                return keep("deadline_exceeded")
            if facts is None:
                return keep(error or "review_git_probe_failed")
            timeout_s = probe_timeout()
            if timeout_s is None:
                return keep("deadline_exceeded")
            paths, error = _branch_changed_paths(
                repo_root,
                base_sha=str(facts["base_sha"]),
                tip=str(facts["tip"]),
                timeout_s=timeout_s,
            )
            if remaining() <= 0:
                return keep("deadline_exceeded")
            if paths is None:
                return keep(error or "review_product_paths_unknown")
            product = tuple(path for path in paths if not lane_disposition._is_scratch(path, _REVIEW_SCRATCH_GLOBS))
            if pass_record is None:
                if product:
                    return keep(
                        REVIEW_NO_PASS_RECORD,
                        pass_id=None,
                        pass_status=None,
                        evidence={"product_paths": list(product[: lane_disposition._MAX_EVIDENCE_PATHS])},
                    )
                preserved, evidence = True, {"pass_record": None, "scratch_paths": list(paths)}
            else:
                preserved, evidence = lane_disposition._review_harvest_snapshot_fact(
                    repo_root,
                    branch=branch,
                    lane_rows=({**row, "status": "closed"},),
                    changed_paths=paths,
                    scratch_globs=_REVIEW_SCRATCH_GLOBS,
                    pass_record=pass_record,
                    deadline=deadline,
                )
            if remaining() <= 0 or evidence.get("reason") == "deadline_exceeded":
                return keep("deadline_exceeded", evidence=evidence)
            if (
                not preserved
                and pass_record is not None
                and not product
                and evidence.get("reason") == REVIEW_REPORT_MISSING
                and _no_work_terminal_pass(pass_record)
            ):
                # Terminal pass and nothing but scratch on the branch: the review
                # failed to produce a report. Inside the grace it is only kept.
                age_s = _review_pass_terminal_age_s(pass_record)
                if age_s is None:
                    return keep(REVIEW_TERMINAL_TIME_UNKNOWN, evidence=evidence)
                if age_s is not None and age_s >= _REVIEW_NO_REPORT_GRACE_S and emit_event is not None:
                    preserved, reason_code = True, REVIEW_FAILED_NO_REPORT
                    evidence = {
                        "reason": REVIEW_FAILED_NO_REPORT,
                        "pass_id": pass_record.get("pass_id"),
                        "pass_outcome": pass_record["result"]["outcome"],
                        "terminal_age_s": age_s,
                        "scratch_paths": list(paths),
                    }
                else:
                    evidence = {
                        **evidence,
                        "terminal_age_s": age_s,
                        "grace_s": _REVIEW_NO_REPORT_GRACE_S,
                        "event_channel": emit_event is not None,
                    }
            if not preserved:
                reason = str(
                    evidence.get("reason")
                    or (
                        "unknown_pass_outcome"
                        if "unknown_pass_outcome" in evidence
                        else "latest_pass_not_terminal"
                        if "nonterminal_pass_outcome" in evidence
                        else "review_pass_not_terminal"
                    )
                )
                return keep(reason, evidence=evidence)
            if Path(worktree).exists():
                timeout_s = probe_timeout()
                if timeout_s is None:
                    return keep("deadline_exceeded")
                dirty = _git_status(worktree, timeout_s=timeout_s)
                if remaining() <= 0:
                    return keep("deadline_exceeded")
                if dirty is None:
                    return keep("review_worktree_status_unknown")
                # Only untracked scratch is disposable. Tracked edits always stay.
                if any(not line.startswith("?? .review/") for line in dirty.splitlines()):
                    return keep("review_worktree_dirty")
            subject_event = None
            if reason_code == REVIEW_FAILED_NO_REPORT:
                assert isinstance(pass_record, dict)
                subject_event = _review_subject_unreviewed_event(row, pass_record, task_ref=task_ref, lane_id=lane_id)
                try:
                    subject_rows = _load_lane_rows(repo_root, task_ref, deadline=deadline)
                except Exception as exc:  # noqa: BLE001 — unknown subject never authorizes close/reap
                    return keep(REVIEW_SUBJECT_UNKNOWN, error=f"{type(exc).__name__}: {exc}")
                if remaining() <= 0:
                    return keep("deadline_exceeded")
                subject_event["subject_lane_id"] = _resolve_subject_lane_id(subject_event, subject_rows)
                if subject_event["subject_lane_id"] is None:
                    return keep(REVIEW_SUBJECT_UNKNOWN, evidence=evidence)
            if not apply:
                if remaining() <= 0:
                    return keep("deadline_exceeded")
                return {"ok": True, "status": status, "reason": reason_code, "evidence": evidence}
            if reason_code == REVIEW_FAILED_NO_REPORT:
                assert emit_event is not None and subject_event is not None
                try:
                    # Journal first. The event uses stable row/pass identity so
                    # the receipt seam can deduplicate it if close must retry.
                    emit_event(subject_event)
                except Exception as exc:  # noqa: BLE001 — an unjournaled subject must stay retryable
                    return keep("review_event_emit_failed", error=f"{type(exc).__name__}: {exc}")
            if status not in lane_disposition._TERMINAL_STATUSES:
                if remaining() <= 0:
                    return keep("deadline_exceeded")
                closed = _close_lane_cas(
                    row,
                    target_status="closed",
                    note=reason_code,
                    deadline=deadline,
                    rule=reason_code,
                    disposition_kind="review_persisted",
                    authorized_tip=str(facts["tip"]),
                    evidence=evidence,
                    integration_ref=str(facts.get("integration_ref") or names[0]),
                    integration_tip=facts.get("integration_tip"),
                )
                if closed.get("ok") is not True:
                    return keep(str(closed.get("reason") or "review_close_failed"))
                status = "closed"
            result = {"ok": True, "status": status, "reason": reason_code, "evidence": evidence}
            if retire:
                remaining_s_now = remaining()
                if remaining_s_now <= 0:
                    return keep("deadline_exceeded", evidence=evidence)
                retired = _retire_lane_bounded(
                    repo_root=repo_root,
                    task_ref=task_ref,
                    lane_id=lane_id,
                    apply=True,
                    remaining_s=remaining_s_now,
                )
                result["retire"] = retired
                if retired.get("ok") is not True:
                    result.update(ok=False, reason="review_retire_failed")
            return result
    except Exception as exc:  # noqa: BLE001 — retirement never rewrites a review outcome
        if remaining() <= 0:
            return keep("deadline_exceeded")
        return keep("review_retire_failed", error=f"{type(exc).__name__}: {exc}")


def _branch_changed_paths(
    repo_root: Path, *, base_sha: str, tip: str, timeout_s: float
) -> tuple[tuple[str, ...] | None, str | None]:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "diff-tree", "-r", "--no-renames", "-z", "--name-only", base_sha, tip],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_s,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, "review_product_paths_unknown"
    if result.returncode != 0:
        return None, "review_product_paths_unknown"
    return tuple(path.removeprefix("./") for path in result.stdout.split("\0") if path), None


def _work_complete_snapshot(
    row: dict[str, Any],
    *,
    tip: str,
    integration_ref: str,
    integration_tip: str,
    changed_paths: tuple[str, ...] = (),
    work_complete_empty: bool = False,
    review_harvested: bool = False,
    evidence: dict[str, Any] | None = None,
) -> Any:
    """Build the pure disposition input from facts already gathered by the sweep."""
    from workbay_orchestrator_mcp.orchestration.lane_disposition import (  # noqa: PLC0415
        CarrierResult,
        Snapshot,
    )

    return Snapshot(
        branch=str(row.get("branch") or "").strip().removeprefix("refs/heads/"),
        integration_ref=integration_ref,
        integration_tip=integration_tip,
        tip=tip,
        now=time.time(),
        tip_commit_time=None,
        age_floor_s=0.0,
        lane_rows=(row,),
        worktree_paths=(),
        landing=None,
        unverified_receipts=(),
        lane_receipts=(),
        tombstone_groups=(),
        stale_tombstones=(),
        carrier=CarrierResult(tip, integration_ref, integration_tip, False, None, None, None),
        survivors=(),
        changed_paths=changed_paths,
        changed_off_main=changed_paths,
        scratch_globs=(".review/**",),
        probes={},
        work_complete_empty=work_complete_empty,
        review_harvested=review_harvested,
        work_complete_guarded=True,
        work_complete_evidence=evidence or {},
    )


def _integration_refs_cached(
    repo_root: Path,
    task_ref: str,
    cache: dict[str, tuple[list[str], str | None]],
) -> tuple[list[str], str | None]:
    if task_ref not in cache:
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            _resolve_close_integration_refs,
        )

        cache[task_ref] = _resolve_close_integration_refs(
            orchestrator_root=repo_root,
            task_ref=task_ref,
            integration_refs=None,
        )
    return cache[task_ref]


def _branch_inventory(repo_root: Path, *, timeout_s: float = _GIT_STATUS_TIMEOUT_S) -> set[str]:
    """Read the local branch namespace once for a sweep."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "for-each-ref", "--format=%(refname:short)", "refs/heads"],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, min(_GIT_STATUS_TIMEOUT_S, timeout_s)),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"branch_inventory_failed:{type(exc).__name__}:{exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"branch_inventory_failed:{(proc.stderr or proc.stdout).strip()}")
    return {line.strip().removeprefix("refs/heads/") for line in proc.stdout.splitlines() if line.strip()}


def _worktree_path_exists(path: str) -> bool | None:
    try:
        Path(path).lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return None


def _read_process_identity(pid: int) -> tuple[str, dict[str, Any] | None]:
    """Read a process start token, cwd, process group, and session from the OS."""
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return "absent", None

    if sys.platform.startswith("linux"):
        proc_dir = Path("/proc") / str(pid)
        try:
            stat_text = (proc_dir / "stat").read_text(encoding="utf-8")
            # comm is parenthesized and may itself contain spaces or parentheses.
            close_paren = stat_text.rfind(")")
            if close_paren < 0:
                return "unknown", None
            fields = stat_text[close_paren + 2 :].split()
            if len(fields) <= 19:
                return "unknown", None
            if fields[0] in {"Z", "X"}:
                return "absent", None
            pgid = int(fields[2])
            sid = int(fields[3])
            start_ticks = fields[19]
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
            cwd = os.readlink(proc_dir / "cwd")
            resolved_cwd = _resolved_path_or_none(cwd)
            if not boot_id or not start_ticks or resolved_cwd is None:
                return "unknown", None
            verify_text = (proc_dir / "stat").read_text(encoding="utf-8")
            verify_close_paren = verify_text.rfind(")")
            verify_fields = verify_text[verify_close_paren + 2 :].split() if verify_close_paren >= 0 else []
            if (
                verify_text.split(" ", 1)[0] != str(pid)
                or len(verify_fields) <= 19
                or verify_fields[0] in {"Z", "X"}
                or verify_fields[2] != str(pgid)
                or verify_fields[3] != str(sid)
                or verify_fields[19] != start_ticks
            ):
                return "unknown", None
            return "available", {
                "pid": pid,
                "start_token": f"linux:{boot_id}:{start_ticks}",
                "repo_root": resolved_cwd,
                "pgid": pgid,
                "sid": sid,
            }
        except FileNotFoundError:
            return "absent", None
        except (OSError, ValueError, IndexError):
            return "unknown", None

    getpgid = getattr(os, "getpgid", None)
    getsid = getattr(os, "getsid", None)
    if not callable(getpgid) or not callable(getsid):
        return "unknown", None

    def read_group_session() -> tuple[str, tuple[int, int] | None]:
        try:
            pgid = getpgid(pid)
            sid = getsid(pid)
        except ProcessLookupError:
            return "absent", None
        except (OSError, NotImplementedError):
            return "unknown", None
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (pgid, sid)):
            return "unknown", None
        return "available", (pgid, sid)

    def read_start_token() -> tuple[str, str | None]:
        try:
            proc = subprocess.run(
                ["ps", "-p", str(pid), "-o", "lstart="],
                capture_output=True,
                text=True,
                check=False,
                timeout=1.0,
            )
        except (OSError, subprocess.TimeoutExpired):
            return "unknown", None
        lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        if proc.returncode != 0 or len(lines) != 1:
            return "unknown", None
        return "available", "ps:" + " ".join(lines[0].split())

    group_state, group_session = read_group_session()
    if group_state != "available" or group_session is None:
        return group_state, None
    start_state, start_token = read_start_token()
    if start_state != "available" or start_token is None:
        return start_state, None
    try:
        cwd_proc = subprocess.run(
            ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
            capture_output=True,
            text=True,
            check=False,
            timeout=1.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown", None
    cwd_paths = [line[1:] for line in cwd_proc.stdout.splitlines() if line.startswith("n")]
    resolved_cwd = _resolved_path_or_none(cwd_paths[0]) if cwd_proc.returncode == 0 and len(cwd_paths) == 1 else None
    if resolved_cwd is None:
        return "unknown", None
    verify_state, verify_start_token = read_start_token()
    if verify_state != "available" or verify_start_token != start_token:
        return "unknown", None
    verify_group_state, verify_group_session = read_group_session()
    if verify_group_state != "available" or verify_group_session != group_session:
        return "unknown", None
    pgid, sid = group_session
    return "available", {
        "pid": pid,
        "start_token": start_token,
        "repo_root": resolved_cwd,
        "pgid": pgid,
        "sid": sid,
    }


def _identity_fields_match(
    actual: object,
    expected: object,
    *,
    repo_root: Path,
    pid: int,
    require_start_token: bool = True,
) -> bool:
    if not isinstance(actual, dict) or not isinstance(expected, dict):
        return False
    root = _resolved_path_or_none(repo_root)
    if root is None:
        return False
    required = ("pid", "repo_root", "pgid", "sid")
    if require_start_token:
        required = (*required, "start_token")
    if any(key not in actual or key not in expected for key in required):
        return False
    try:
        actual_pid = int(actual["pid"])
        expected_pid = int(expected["pid"])
        actual_pgid = int(actual["pgid"])
        expected_pgid = int(expected["pgid"])
        actual_sid = int(actual["sid"])
        expected_sid = int(expected["sid"])
    except (TypeError, ValueError):
        return False
    return (
        actual_pid == expected_pid == pid
        and actual_pgid == expected_pgid == pid
        and actual_sid == expected_sid == pid
        and (
            not require_start_token
            or (
                isinstance(actual["start_token"], str)
                and bool(actual["start_token"])
                and actual["start_token"] == expected["start_token"]
            )
        )
        and actual["repo_root"] == expected["repo_root"] == root
    )


def _open_pidfd(pid: int) -> int:
    """Pin a Linux PID across the final identity check and group signal."""
    pidfd_open = getattr(os, "pidfd_open", None)
    if callable(pidfd_open):
        return pidfd_open(pid, 0)
    machine = os.uname().machine.lower()
    if machine not in {"x86_64", "amd64", "aarch64", "arm64", "riscv64", "ppc64le", "s390x"}:
        raise OSError("pidfd_open_unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    syscall = libc.syscall
    syscall.restype = ctypes.c_long
    descriptor = syscall(ctypes.c_long(434), ctypes.c_int(pid), ctypes.c_uint(0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return int(descriptor)


def _signal_owned_group(identity: dict[str, Any], repo_root: Path, sig: int) -> str:
    """Signal a group only after a fresh full identity and session check."""
    pid = identity.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return "unknown"
    try:
        pidfd: int | None = None
        if sys.platform.startswith("linux"):
            pidfd = _open_pidfd(pid)
        try:
            state, current = _read_process_identity(pid)
            if state == "absent":
                return "absent"
            if state != "available" or not _identity_fields_match(current, identity, repo_root=repo_root, pid=pid):
                return "unknown"
            os.killpg(pid, sig)
            return "signaled"
        finally:
            if pidfd is not None:
                os.close(pidfd)
    except ProcessLookupError:
        return "absent"
    except OSError:
        return "unknown"


def _retire_progress_path(root: Path, nonce: str) -> Path:
    return (root / _CHILD_MARKER_RELATIVE).with_name(f"lane-retire-phase-{nonce}.jsonl")


def _write_retire_phase_progress(phases: dict[str, Any], running: str | None, started: float) -> None:
    global _RETIRE_PROGRESS_ERROR
    if _RETIRE_PROGRESS_PATH is not None:
        # Local append-only progress has no pipe backpressure. Each closed,
        # flushed line remains readable after SIGKILL; no fsync is needed.
        try:
            with _RETIRE_PROGRESS_PATH.open("a", encoding="utf-8") as progress_file:
                progress_file.write(
                    json.dumps(
                        {
                            "kind": "progress",
                            "phase_timings_s": _bounded_phase_timings(phases),
                            "running": running,
                            "started": started,
                        }
                    )
                    + "\n"
                )
                progress_file.flush()
        except OSError:
            # Instrumentation cannot change retire decisions. Report the
            # failed channel in the result, including after a normal success.
            _RETIRE_PROGRESS_ERROR = "retire_phase_progress_unavailable"


def _retire_child_operation(
    repo_root: str, lane_id: str, task_ref: str, apply: bool, landing_lock_held: bool = False
) -> dict[str, Any]:
    """Run retirement with per-phase timings; runtime is configured before go."""
    operation_started = _monotonic()
    phase_timings = {key: 0.0 for key in _RETIRE_PHASE_TIMING_KEYS}
    phase_timings["runtime_configuration_s"] = _RETIRE_RUNTIME_ELAPSED_S
    _write_retire_phase_progress(phase_timings, "other_s", operation_started)
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    from . import (
        branch_reclaim_delete,  # noqa: PLC0415
        lane_disposition,
        lane_reclaim,
        worktree_tombstone,  # noqa: PLC0415
    )

    active_key = "other_s"
    active_started = operation_started

    def switch_phase(key: str | None) -> None:
        nonlocal active_key, active_started
        now = _monotonic()
        if active_key is not None:
            phase_timings[active_key] += max(0.0, now - active_started)
        active_key, active_started = key, now
        _write_retire_phase_progress(phase_timings, key, now)

    switch_phase("other_s")

    def timed_call(key: str, function: Any, *args: Any, **kwargs: Any) -> Any:
        previous_key = active_key
        switch_phase(key)
        try:
            return function(*args, **kwargs)
        finally:
            switch_phase(previous_key)

    bundle_before_reap = lane_reaping._bundle_before_reap

    def timed_bundle(*args: Any, **kwargs: Any) -> Any:
        return timed_call("bundle_s", bundle_before_reap, *args, **kwargs)

    removal_details: list[str] = []

    def timed_remove(*args: Any, **kwargs: Any) -> Any:
        repo = args[0] if args else kwargs.get("repo_root")
        worktree = args[1] if len(args) > 1 else kwargs.get("worktree_path")
        if repo is None or worktree is None:
            return False, "worktree_removal_identity_missing"
        result = timed_call(
            "worktree_removal_s",
            worktree_tombstone.retire_worktree,
            repo,
            str(worktree),
            lane_id=lane_id,
            task_ref=task_ref,
            landing_lock_held=landing_lock_held,
            **{
                key: value
                for key, value in kwargs.items()
                if key not in {"repo_root", "worktree_path", "lane_id", "task_ref", "landing_lock_held"}
            },
        )
        if isinstance(result, tuple) and len(result) > 1 and isinstance(result[1], str):
            removal_details.append(result[1])
        return result

    delete_authorized_branch = branch_reclaim_delete.delete_authorized_branch

    def timed_authorized_delete(*args: Any, **kwargs: Any) -> Any:
        if landing_lock_held:
            # The parent sweep owns lane_land's fence for the whole bounded
            # child operation, so pass the helper's existing capability on.
            kwargs["_lock_held"] = True
        return timed_call("branch_deletion_s", delete_authorized_branch, *args, **kwargs)

    reclaim_command = lane_reaping._run_reclaim_command

    def timed_reclaim_command(argv: Any, *args: Any, **kwargs: Any) -> Any:
        # The merged-branch path in lanes.py deletes directly through this
        # helper. Observe that command without changing its authorization.
        if isinstance(argv, (list, tuple)) and len(argv) > 4 and argv[3] == "branch" and argv[4] in {"-d", "-D"}:
            return timed_call("branch_deletion_s", reclaim_command, argv, *args, **kwargs)
        return reclaim_command(argv, *args, **kwargs)

    def wrap_phase(key: str, function: Any) -> Any:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return timed_call(key, function, *args, **kwargs)

        return wrapped

    with ExitStack() as patches:
        patches.enter_context(patch.object(lane_reaping, "_bundle_before_reap", timed_bundle))
        patches.enter_context(patch.object(lane_reaping, "_remove_lane_worktree", timed_remove))
        patches.enter_context(patch.object(branch_reclaim_delete, "delete_authorized_branch", timed_authorized_delete))
        patches.enter_context(patch.object(lane_reaping, "_run_reclaim_command", timed_reclaim_command))
        for module, name, key in (
            (lane_disposition, "collect_snapshot", "disposition_s"),
            (lane_disposition, "decide", "disposition_s"),
            (lane_reaping, "_probe_worktree_process_owner", "owner_probe_s"),
            (lane_reaping, "_session_heartbeat_blocks_reclaim", "owner_probe_s"),
            (lane_reaping, "_shared_path_blocks_reclaim", "owner_probe_s"),
            (lane_reaping, "_branch_ledger_owner_blocks_reclaim", "owner_probe_s"),
            (lane_reclaim, "_session_live", "owner_probe_s"),
        ):
            patches.enter_context(patch.object(module, name, wrap_phase(key, getattr(module, name))))
        result = _retire_lane(
            lane_id=lane_id,
            task_ref=task_ref,
            apply=apply,
            delete_merged_branch=True,
        )
    switch_phase(None)
    if isinstance(result, dict) and "tombstone_foreign_content" in removal_details:
        result = {**result, "ok": False, "outcome": "tombstone_foreign_content"}
    if isinstance(result, dict):
        result = dict(result)
        result["phase_timings_s"] = _bounded_phase_timings(phase_timings)
    return result


def _bounded_phase_timings(value: object) -> dict[str, Any]:
    """Expose only the small, numeric phase allowlist in lane receipts."""
    raw = value if isinstance(value, dict) else {}
    bounded: dict[str, Any] = {}
    for key in _RETIRE_PHASE_TIMING_KEYS:
        item = raw.get(key, 0.0)
        if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(float(item)):
            item = 0.0
        bounded[key] = min(_RETIRE_PHASE_TIMING_MAX_S, max(0.0, float(item)))
    timed_out_in = raw.get("timed_out_in")
    if isinstance(timed_out_in, str) and (
        f"{timed_out_in}_s" in _RETIRE_PHASE_TIMING_KEYS or timed_out_in == "unknown"
    ):
        bounded["timed_out_in"] = timed_out_in
    return bounded


def _isolated_child_main(
    connection: Any,
    nonce: str,
    repo_root: str,
    operation: Any,
    operation_args: tuple[Any, ...],
    operation_kwargs: dict[str, Any],
) -> None:
    """Wait for the parent to verify this child's isolated session before work."""
    global _RUNTIME_CONFIGURATION_API, _RUNTIME_CONFIGURATION_API_ERROR
    global _RETIRE_PROGRESS_PATH, _RETIRE_PROGRESS_ERROR, _RETIRE_RUNTIME_ELAPSED_S

    _RUNTIME_CONFIGURATION_API = None
    _RUNTIME_CONFIGURATION_API_ERROR = None
    _RETIRE_PROGRESS_PATH = None
    _RETIRE_PROGRESS_ERROR = None
    _RETIRE_RUNTIME_ELAPSED_S = 0.0
    try:
        # Spawn-time module imports belong to the startup allowance. Keep their
        # cost out of the ready handshake, while reporting alive before any
        # repository-specific runtime configuration or destructive work.
        _RUNTIME_CONFIGURATION_API = _load_runtime_configuration_api()
    except BaseException as exc:  # noqa: BLE001 — report import failures after alive
        _RUNTIME_CONFIGURATION_API_ERROR = exc
    try:
        connection.send({"kind": "alive", "nonce": nonce})
    except (BrokenPipeError, EOFError, OSError):
        connection.close()
        return

    root = Path(repo_root).resolve()
    runtime_elapsed = 0.0
    try:
        os.setsid()
        os.chdir(root)
        runtime_started = _monotonic()
        _configure_runtime(root)
        runtime_elapsed = max(0.0, _monotonic() - runtime_started)
        state, identity = _read_process_identity(os.getpid())
        if (
            state != "available"
            or identity is None
            or not _identity_fields_match(identity, identity, repo_root=root, pid=os.getpid())
        ):
            connection.send({"kind": "abort", "outcome": "retire_session_unverified"})
            return
        connection.send({"kind": "ready", "nonce": nonce, "identity": identity})
        if not connection.poll(_CHILD_HANDSHAKE_TIMEOUT_S):
            return
        acknowledgement = connection.recv()
        if (
            not isinstance(acknowledgement, dict)
            or acknowledgement.get("kind") != "go"
            or acknowledgement.get("nonce") != nonce
        ):
            return
        _RETIRE_RUNTIME_ELAPSED_S = runtime_elapsed
        _RETIRE_PROGRESS_PATH = _retire_progress_path(root, nonce)
    except BaseException as exc:  # noqa: BLE001 — failed isolation must precede all destructive work
        try:
            connection.send(
                {"kind": "abort", "outcome": "retire_session_failed", "error": f"{type(exc).__name__}: {exc}"}
            )
        except (BrokenPipeError, EOFError, OSError):
            pass
        return

    try:
        result = operation(*operation_args, **operation_kwargs)
        if operation is _retire_child_operation and isinstance(result, dict):
            result = dict(result)
            phase_timings = result.get("phase_timings_s")
            phase_timings = phase_timings if isinstance(phase_timings, dict) else {}
            result["phase_timings_s"] = _bounded_phase_timings(
                {**phase_timings, "runtime_configuration_s": runtime_elapsed}
            )
        message = {"kind": "result", "result": result}
    except BaseException as exc:  # noqa: BLE001 — parent must retain a bounded result
        failed_result = {"ok": False, "outcome": "retire_raised", "error": f"{type(exc).__name__}: {exc}"}
        if operation is _retire_child_operation:
            failed_result["phase_timings_s"] = _bounded_phase_timings({"runtime_configuration_s": runtime_elapsed})
        message = {"kind": "result", "result": failed_result}
    if _RETIRE_PROGRESS_ERROR is not None:
        message.update(phase_instrument="missing", detail=_RETIRE_PROGRESS_ERROR)
        if isinstance(message.get("result"), dict):
            message["result"] = {**message["result"], "phase_instrument": "missing", "detail": _RETIRE_PROGRESS_ERROR}
    try:
        connection.send(message)
    except (BrokenPipeError, EOFError, OSError):
        pass
    finally:
        connection.close()
        _RETIRE_PROGRESS_PATH = None
        _RETIRE_PROGRESS_ERROR = None
        _RETIRE_RUNTIME_ELAPSED_S = 0.0


def _safe_child_context() -> Any:
    methods = multiprocessing.get_all_start_methods()
    for method in ("spawn", "forkserver"):
        if method in methods:
            return multiprocessing.get_context(method)
    raise RuntimeError("no_safe_multiprocessing_context")


def _canonical_spawn_callable(callback: Any) -> Any:
    """Resolve importable callables through the current module registry.

    Test runners and embedders can reload a module while retaining references
    to its old functions. Spawn pickles functions by module and qualified name,
    so the old reference then fails identity checks against ``sys.modules``.
    Resolve module-level callables to the registered version before passing
    them to a spawned process.
    """
    module_name = getattr(callback, "__module__", None)
    qualname = getattr(callback, "__qualname__", None)
    if not isinstance(module_name, str) or not isinstance(qualname, str):
        return callback
    parts = qualname.split(".")
    if "<locals>" in parts:
        return callback
    resolved: Any = sys.modules.get(module_name)
    if resolved is None:
        return callback
    for part in parts:
        resolved = getattr(resolved, part, None)
        if resolved is None:
            return callback
    return resolved if callable(resolved) else callback


def _retire_spawn_exception_detail(exc: Exception) -> str:
    detail = f"{type(exc).__name__}: {exc}"
    detail = " ".join(detail.split())

    def redact_secret(match: re.Match[str]) -> str:
        if match.group("scheme") is not None:
            return f"{match.group('scheme')}{match.group('scheme_separator')}[redacted]"
        value_quote = match.group("double_quote") or match.group("single_quote") or ""
        return (
            f"{match.group('label')}{match.group('key_quote')}{match.group('separator')}"
            f"{value_quote}[redacted]{value_quote}"
        )

    detail = _RETIRE_SPAWN_SECRET_RE.sub(redact_secret, detail)
    detail = _RETIRE_SPAWN_PATH_RE.sub("[path]", detail)
    if len(detail) > _RETIRE_SPAWN_DETAIL_CAP:
        detail = f"{detail[: _RETIRE_SPAWN_DETAIL_CAP - 1]}…"
    return detail


def _run_isolated_operation(
    *,
    repo_root: Path,
    operation: Any,
    operation_args: tuple[Any, ...],
    operation_kwargs: dict[str, Any] | None = None,
    timeout_s: float,
    startup_allowance_s: float | None = None,
) -> dict[str, Any]:
    startup_allowance = (
        _CHILD_STARTUP_ALLOWANCE_S
        if startup_allowance_s is None
        else min(_CHILD_STARTUP_ALLOWANCE_S, max(0.0, startup_allowance_s))
    )
    startup_budget_capped = startup_allowance < _CHILD_STARTUP_ALLOWANCE_S
    startup_budget_fields = {
        "startup_budget_capped": startup_budget_capped,
        "child_started": False,
        "phase_instrument": "missing",
    }
    if startup_budget_capped and startup_allowance <= 0.0:
        return {
            "ok": False,
            "outcome": "budget_exhausted",
            "startup_s": 0.0,
            "handshake_s": 0.0,
            **startup_budget_fields,
        }

    nonce = uuid.uuid4().hex
    spawn_started: float | None = None
    try:
        context = _safe_child_context()
        parent_connection, child_connection = context.Pipe(duplex=True)
        spawn_target = _canonical_spawn_callable(_isolated_child_main)
        spawn_operation = _canonical_spawn_callable(operation)
        process = context.Process(
            target=spawn_target,
            args=(
                child_connection,
                nonce,
                str(repo_root.resolve()),
                spawn_operation,
                operation_args,
                operation_kwargs or {},
            ),
            daemon=False,
        )
        spawn_started = _monotonic()
        process.start()
        startup_budget_fields["child_started"] = True
        child_connection.close()
    except Exception as exc:  # noqa: BLE001 — fail closed if a bounded child cannot start
        startup_s = 0.0 if spawn_started is None else max(0.0, _monotonic() - spawn_started)
        return {
            "ok": False,
            "outcome": "retire_spawn_failed",
            "detail": _retire_spawn_exception_detail(exc),
            "error": _retire_spawn_exception_detail(exc),
            "startup_s": startup_s,
            "handshake_s": 0.0,
            **startup_budget_fields,
        }

    assert spawn_started is not None
    startup_deadline = spawn_started + startup_allowance
    alive_message: Any = None
    alive_received_at: float | None = None
    startup_finished_at: float | None = None
    startup_deadline_expired = False
    child_exited_before_alive = False
    while True:
        now = _monotonic()
        remaining = max(0.0, startup_deadline - now)
        poll_timeout = min(_CHILD_STARTUP_POLL_INTERVAL_S, remaining)
        if parent_connection.poll(poll_timeout):
            try:
                alive_message = parent_connection.recv()
            except (EOFError, OSError):
                alive_message = None
            alive_received_at = _monotonic()
            startup_finished_at = alive_received_at
            if alive_message is None:
                startup_deadline_expired = alive_received_at >= startup_deadline
                child_exited_before_alive = not process.is_alive() and not startup_deadline_expired
            else:
                startup_deadline_expired = alive_received_at > startup_deadline
            break
        process_alive = process.is_alive()
        startup_finished_at = _monotonic()
        if not process_alive or startup_finished_at >= startup_deadline:
            startup_deadline_expired = startup_finished_at >= startup_deadline
            child_exited_before_alive = not process_alive and not startup_deadline_expired
            break

    assert startup_finished_at is not None
    startup_s = max(0.0, startup_finished_at - spawn_started)
    alive_kind = alive_message.get("kind") if isinstance(alive_message, dict) else None
    nonce_matches = isinstance(alive_message, dict) and alive_message.get("nonce") == nonce
    timely_alive = (
        alive_kind in ("alive", "ready")
        and nonce_matches
        and alive_received_at is not None
        and alive_received_at <= startup_deadline
    )
    if not timely_alive:
        parent_connection.close()
        _stop_unverified_child(process)
        outcome = "retire_session_startup_timeout"
        if process.is_alive():
            outcome = "retire_session_cleanup_unknown"
        elif child_exited_before_alive:
            outcome = "retire_session_child_exited"
        elif alive_message is None and not startup_deadline_expired:
            outcome = "retire_session_unverified"
        elif timely_alive is False and alive_kind in ("alive", "ready") and nonce_matches:
            outcome = "retire_session_startup_timeout"
        elif alive_message is not None:
            error = alive_message.get("error") if isinstance(alive_message, dict) else None
            reported_outcome = alive_message.get("outcome") if isinstance(alive_message, dict) else None
            if isinstance(reported_outcome, str) and reported_outcome:
                outcome = reported_outcome
            else:
                outcome = "retire_session_unverified"
            return {
                "ok": False,
                "outcome": outcome,
                **({"error": error} if error else {}),
                "startup_s": startup_s,
                "handshake_s": 0.0,
                "startup_deadline_expired": startup_deadline_expired,
                **startup_budget_fields,
            }
        return {
            "ok": False,
            "outcome": outcome,
            **({"exitcode": process.exitcode} if child_exited_before_alive else {}),
            "startup_s": startup_s,
            "handshake_s": 0.0,
            "startup_deadline_expired": startup_deadline_expired,
            **startup_budget_fields,
        }

    handshake_deadline = alive_received_at + _CHILD_HANDSHAKE_TIMEOUT_S
    legacy_ready = alive_message.get("kind") == "ready"
    handshake: Any = alive_message if legacy_ready else None
    ready_received_at: float | None = alive_received_at if legacy_ready else None
    if not legacy_ready:
        while True:
            now = _monotonic()
            remaining = max(0.0, handshake_deadline - now)
            poll_timeout = min(_CHILD_STARTUP_POLL_INTERVAL_S, remaining)
            if parent_connection.poll(poll_timeout):
                try:
                    handshake = parent_connection.recv()
                except (EOFError, OSError):
                    handshake = None
                ready_received_at = _monotonic()
                break
            if remaining <= 0.0:
                break

    handshake_finished_at = ready_received_at if ready_received_at is not None else _monotonic()
    handshake_s = max(0.0, handshake_finished_at - alive_received_at)
    if (
        not isinstance(handshake, dict)
        or handshake.get("kind") != "ready"
        or handshake.get("nonce") != nonce
        or ready_received_at is None
        or ready_received_at > handshake_deadline
    ):
        _stop_unverified_child(process)
        parent_connection.close()
        error = handshake.get("error") if isinstance(handshake, dict) else None
        handshake_outcome = handshake.get("outcome") if isinstance(handshake, dict) else None
        if not isinstance(handshake_outcome, str) or not handshake_outcome:
            handshake_outcome = (
                "retire_session_handshake_timeout"
                if handshake is None or not isinstance(handshake, dict)
                else "retire_session_unverified"
            )
        return {
            "ok": False,
            "outcome": handshake_outcome,
            **({"error": error} if error else {}),
            "startup_s": startup_s,
            "handshake_s": handshake_s,
            **startup_budget_fields,
        }

    child_identity = handshake.get("identity")
    state, current_identity = _read_process_identity(process.pid)
    if (
        state != "available"
        or not _identity_fields_match(child_identity, child_identity, repo_root=repo_root, pid=process.pid)
        or not _identity_fields_match(current_identity, child_identity, repo_root=repo_root, pid=process.pid)
    ):
        parent_connection.close()
        _stop_unverified_child(process)
        return {
            "ok": False,
            "outcome": "retire_session_unverified",
            "startup_s": startup_s,
            "handshake_s": max(0.0, _monotonic() - alive_received_at),
            **startup_budget_fields,
        }
    if _monotonic() >= handshake_deadline:
        handshake_s = max(0.0, _monotonic() - alive_received_at)
        parent_connection.close()
        _stop_unverified_child(process)
        return {
            "ok": False,
            "outcome": "retire_session_handshake_timeout",
            "startup_s": startup_s,
            "handshake_s": handshake_s,
            **startup_budget_fields,
        }
    handshake_s = max(0.0, _monotonic() - alive_received_at)
    progress_path = _retire_progress_path(repo_root.resolve(), nonce)
    progress_error: str | None = None
    try:
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        progress_path.touch(exist_ok=False)
    except OSError:
        progress_error = "retire_phase_progress_unavailable"
    try:
        parent_connection.send({"kind": "go", "nonce": nonce})
    except (BrokenPipeError, EOFError, OSError):
        parent_connection.close()
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        try:
            progress_path.unlink(missing_ok=True)
        except OSError:
            progress_error = "retire_phase_progress_cleanup_failed"
        return {
            "ok": False,
            "outcome": "retire_session_handshake_failed",
            **({"detail": progress_error} if progress_error else {}),
            "startup_s": startup_s,
            "handshake_s": handshake_s,
            **startup_budget_fields,
        }

    operation_started = _monotonic()
    deadline = operation_started + timeout_s
    message: Any = None
    result_messages: queue.Queue[Any] = queue.Queue(maxsize=1)
    stop_result_reader = threading.Event()

    def receive_result() -> None:
        # recv may wait for the rest of a partial frame. Keep it off the
        # deadline-enforcing thread, while draining large successful results.
        try:
            while not stop_result_reader.is_set():
                if parent_connection.poll(0.05):
                    result_messages.put_nowait(parent_connection.recv())
                    return
        except (EOFError, OSError):
            return
        finally:
            parent_connection.close()

    result_reader = threading.Thread(target=receive_result, daemon=True)
    result_reader.start()
    progress: dict[str, Any] | None = None
    first_phase_started: float | None = None
    progress_offset = 0
    progress_tail = b""

    def read_progress() -> bool:
        nonlocal progress, progress_error, first_phase_started, progress_offset, progress_tail
        try:
            with progress_path.open("rb") as progress_file:
                progress_file.seek(progress_offset)
                chunk = progress_file.read(65536)
        except OSError:
            progress_error = progress_error or "retire_phase_progress_unavailable"
            return False
        progress_offset += len(chunk)
        lines = (progress_tail + chunk).split(b"\n")
        progress_tail = lines.pop()
        # Bound memory even if a damaged writer never ends its line.
        if len(progress_tail) > 65536:
            progress_error = "retire_phase_progress_malformed"
            progress_tail = b""
        for line in lines:
            try:
                received = json.loads(line)
            except (ValueError, UnicodeError, RecursionError):
                progress_error = "retire_phase_progress_malformed"
                continue
            running = received.get("running") if isinstance(received, dict) else None
            started = received.get("started") if isinstance(received, dict) else None
            try:
                valid = (
                    isinstance(received, dict)
                    and received.get("kind") == "progress"
                    and (running is None or (isinstance(running, str) and running in _RETIRE_PHASE_TIMING_KEYS))
                    and not isinstance(started, bool)
                    and isinstance(started, (int, float))
                    and math.isfinite(started)
                    and operation_started - 0.5 <= started <= _monotonic()
                    and isinstance(received.get("phase_timings_s"), dict)
                    and all(
                        key in _RETIRE_PHASE_TIMING_KEYS
                        and not isinstance(value, bool)
                        and isinstance(value, (int, float))
                        and math.isfinite(value)
                        and value >= 0
                        for key, value in received["phase_timings_s"].items()
                    )
                )
            except (OverflowError, ValueError, TypeError):
                valid = False
            if valid:
                progress = received
                if first_phase_started is None:
                    first_phase_started = started
            else:
                progress_error = "retire_phase_progress_malformed"
        return len(chunk) == 65536

    while True:
        remaining = max(0.0, deadline - _monotonic())
        process.join(min(0.05, remaining))
        read_progress()
        if not process.is_alive() or _monotonic() >= deadline:
            break

    killed_at = _monotonic()
    timed_out = process.is_alive()
    cleanup_unknown = False
    if timed_out:
        signal_result = _signal_owned_group(child_identity, repo_root, signal.SIGKILL)
        process.join(_RETIRE_TIMEOUT_CLEANUP_S)
        cleanup_unknown = process.is_alive() or signal_result == "unknown"
    drain_deadline = _monotonic() + 0.05
    while read_progress() and _monotonic() < drain_deadline:
        pass
    if progress_tail:
        progress_error = "retire_phase_progress_malformed"
    try:
        progress_path.unlink(missing_ok=True)
    except OSError:
        progress_error = "retire_phase_progress_cleanup_failed"
    try:
        message = result_messages.get(timeout=0.05)
    except queue.Empty:
        message = None
    finally:
        stop_result_reader.set()
        # A surviving writer with a partial result cannot block this receipt.
        # The daemon owns the connection and closes it when recv finishes.
        result_reader.join(0.05)

    def with_phase_evidence(result: dict[str, Any], *, timeout: bool) -> dict[str, Any]:
        phases = _bounded_phase_timings(
            progress.get("phase_timings_s")
            if progress is not None and (timeout or "phase_timings_s" not in result)
            else result.get("phase_timings_s")
        )
        if timeout:
            running = progress.get("running") if progress is not None else None
            if isinstance(running, str) and running in _RETIRE_PHASE_TIMING_KEYS:
                phases[running] += max(0.0, killed_at - progress["started"])
                phases["timed_out_in"] = running.removesuffix("_s")
            elif progress is not None and running is None:
                phases["other_s"] += max(0.0, killed_at - progress["started"])
                phases["timed_out_in"] = "other"
            else:
                phases["timed_out_in"] = "unknown"
                progress_error_detail = progress_error or "retire_phase_progress_unavailable"
                result = {**result, "detail": progress_error_detail}
            if progress_error is not None:
                result = {**result, "detail": progress_error}
        elif progress is not None and progress.get("running") is None:
            phases["other_s"] += max(0.0, killed_at - progress["started"])
        if first_phase_started is not None:
            phases["other_s"] += max(0.0, first_phase_started - operation_started)
        # Account for measured startup, verification and teardown separately
        # from the exclusive intervals observed inside _retire_lane.
        phases["other_s"] += (
            startup_s + max(0.0, handshake_s - phases["runtime_configuration_s"]) + max(0.0, _monotonic() - killed_at)
        )
        instrument = (
            "malformed"
            if progress_error and progress_error != "retire_phase_progress_unavailable"
            else ("missing" if progress is None or progress_error else "ok")
        )
        child_progress_failed = (
            isinstance(message, dict)
            and message.get("phase_instrument") == "missing"
            and message.get("detail") == "retire_phase_progress_unavailable"
        )
        if child_progress_failed or (
            result.get("phase_instrument") == "missing" and result.get("detail") == "retire_phase_progress_unavailable"
        ):
            instrument = "missing" if instrument == "ok" else instrument
        if instrument != "ok":
            result = {**result, "detail": progress_error or "retire_phase_progress_unavailable"}
        return {**result, "phase_timings_s": _bounded_phase_timings(phases), "phase_instrument": instrument}

    fields = {"startup_s": startup_s, "handshake_s": handshake_s, **startup_budget_fields}
    fields.pop("phase_instrument")
    if cleanup_unknown:
        return with_phase_evidence({"ok": False, "outcome": "retire_timeout_cleanup_unknown", **fields}, timeout=True)
    # Retirement may complete before slow process teardown; preserve the
    # queued success receipt after verified cleanup instead of hiding it.
    if isinstance(message, dict) and message.get("kind") == "result" and isinstance(message.get("result"), dict):
        result = {**message["result"], **fields}
        return with_phase_evidence(result, timeout=False)
    if timed_out:
        return with_phase_evidence({"ok": False, "outcome": "retire_timeout", **fields}, timeout=True)
    return with_phase_evidence(
        {
            "ok": False,
            "outcome": "retire_malformed",
            "error": f"child_exit:{process.exitcode}",
            **fields,
        },
        timeout=False,
    )


def _stop_unverified_child(process: Any) -> None:
    """Bound and stop a direct child that failed before the verified go handshake."""
    if not process.is_alive():
        return
    try:
        process.terminate()
    except (OSError, ValueError):
        pass
    process.join(_RETIRE_TIMEOUT_CLEANUP_S)
    if process.is_alive():
        try:
            process.kill()
        except (AttributeError, OSError, ValueError):
            pass
        process.join(0)


def _retire_lane_bounded(
    *, lane_id: str, task_ref: str, apply: bool, remaining_s: float, repo_root: Path
) -> dict[str, Any]:
    if remaining_s <= 0:
        return {"ok": False, "outcome": "budget_exhausted", "child_started": False, "phase_instrument": "missing"}
    operation_timeout_s = min(
        _RETIRE_OPERATION_TIMEOUT_S,
        max(0.0, remaining_s - _CHILD_HANDSHAKE_TIMEOUT_S - _RETIRE_TIMEOUT_CLEANUP_S),
    )
    if operation_timeout_s <= 0:
        return {"ok": False, "outcome": "budget_exhausted", "child_started": False, "phase_instrument": "missing"}
    startup_allowance_s = min(
        _CHILD_STARTUP_ALLOWANCE_S,
        max(
            0.0,
            remaining_s - _CHILD_HANDSHAKE_TIMEOUT_S - operation_timeout_s - _RETIRE_TIMEOUT_CLEANUP_S,
        ),
    )
    if remaining_s <= _CHILD_HANDSHAKE_TIMEOUT_S + _RETIRE_OPERATION_TIMEOUT_S + _RETIRE_TIMEOUT_CLEANUP_S:
        # A below-cap first retire must leave time for a child to start.
        # Preserve the existing allocation when the full cap already fits.
        startup_allowance_s = min(_CHILD_STARTUP_ALLOWANCE_S, _RETIRE_EXPECTED_CHILD_STARTUP_S, operation_timeout_s / 2)
        operation_timeout_s -= startup_allowance_s
    result = _run_isolated_operation(
        repo_root=repo_root,
        operation=_retire_child_operation,
        operation_args=(str(repo_root.resolve()), lane_id, task_ref, apply, True),
        timeout_s=operation_timeout_s,
        startup_allowance_s=startup_allowance_s,
    )
    return {**result, "effective_timeout_s": operation_timeout_s}


def _retire_owner_lock(repo_root: Path):
    """Return lane_land's existing nonblocking mutation fence for a retire."""
    from . import orchestrator_lanes  # noqa: PLC0415

    return orchestrator_lanes._landing_mutation_lock(repo_root)


def _await_landing_quiescence(repo_root: Path, *, timeout_s: float) -> tuple[bool, float]:
    """Wait a bounded time for lane_land's fence, releasing it before the sweep."""
    wait_started = _monotonic()
    deadline = wait_started + max(0.0, float(timeout_s))
    while True:
        try:
            with _retire_owner_lock(repo_root) as acquired:
                if acquired is True:
                    return True, max(0.0, _monotonic() - wait_started)
        except Exception:  # noqa: BLE001 — an unverifiable fence must defer retirement
            pass
        remaining_s = deadline - _monotonic()
        if remaining_s <= 0:
            return False, max(0.0, _monotonic() - wait_started)
        time.sleep(min(_LANDING_QUIESCENCE_POLL_S, remaining_s))


def _continuation_decision(summary: dict[str, Any], *, apply: bool, continuation_count: int | None) -> str:
    """Explain whether durable progress or lock contention warrants a retry."""
    if not apply or continuation_count is None:
        return "none"
    if _budget_ceiling_reason(summary) is not None:
        return "none"
    if summary.get("outcome") == "retire_admission_starved":
        return "budget_exhausted" if continuation_count == 0 else "none"
    if summary.get("has_more") is not True and summary.get("outcome") != "budget_exhausted":
        return "none"

    outcome_counts = summary.get("outcome_counts")
    released_no_work = outcome_counts.get("released_no_work", 0) if isinstance(outcome_counts, dict) else 0
    released_no_work_progress = isinstance(released_no_work, int) and released_no_work > 0
    if continuation_count >= _MAX_BACKGROUND_SWEEP_CONTINUATIONS and not (released_no_work_progress):
        return "none"

    retired = summary.get("retired_lane_ids")
    closed_count = (
        sum(value for key, value in outcome_counts.items() if key.startswith("closed:") and isinstance(value, int))
        if isinstance(outcome_counts, dict)
        else 0
    )
    if (isinstance(retired, list) and bool(retired)) or closed_count > 0 or released_no_work_progress:
        return "progress"
    if summary.get("outcome") == "deferred_landing_active":
        return "contention"
    if summary.get("cursor_advanced") is True:
        return "cursor_advanced"

    skip_counts = summary.get("skip_counts")
    if isinstance(skip_counts, dict):
        active_count = skip_counts.get("keep:retire_owner_active", 0)
        total_kept = sum(value for value in skip_counts.values() if isinstance(value, int))
        if isinstance(active_count, int) and active_count > 0 and active_count == total_kept:
            return "contention"
        if skip_counts.get("budget_exhausted", 0) > 0:
            return "budget_exhausted"
    if summary.get("outcome") == "budget_exhausted":
        return "budget_exhausted"
    return "none"


def _retire_admission_allowance_s() -> float:
    """Recompute from the current operation cap, never the import-time allowance."""
    return (
        _RETIRE_EXPECTED_CHILD_STARTUP_S
        + _CHILD_HANDSHAKE_TIMEOUT_S
        + _clamp_retire_operation_timeout(_RETIRE_OPERATION_TIMEOUT_S)
        + _RETIRE_TIMEOUT_CLEANUP_S
    )


def __getattr__(name: str) -> Any:
    """Serve the retired allowance name as the live value, so no stale copy can govern a pass.

    Production code calls _retire_admission_allowance_s(); this read path exists only for
    callers that still name the old constant.
    """
    if name == "_RETIRE_ADMISSION_ALLOWANCE_S":
        return _retire_admission_allowance_s()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _resolve_pass_budget_s(requested_s: float, *, scan_estimate_s: float) -> tuple[float, str | None]:
    requested = float(requested_s)
    estimate = float(scan_estimate_s)
    if not math.isfinite(requested) or requested < 0 or not math.isfinite(estimate) or estimate < 0:
        raise ValueError("pass budget and scan estimate must be finite and non-negative")
    required = estimate + _retire_admission_allowance_s()
    reason = f"budget_ceiling_exceeded:{required:g}/{_MAX_PASS_BUDGET_S:g}" if required > _MAX_PASS_BUDGET_S else None
    return min(max(requested, required), _MAX_PASS_BUDGET_S), reason


def _budget_ceiling_refusal(*, task_ref: str | None, budget_s: float, reason: str) -> dict[str, Any]:
    """Typed no-launch verdict: a ceiling forbids the child, the wake and the continuation alike."""
    return {
        "ok": False,
        "outcome": "retire_admission_starved",
        "resolved_budget_s": budget_s,
        "could_not_act": [{"lane_id": None, "task_ref": task_ref, "status": None, "reason": reason}],
    }


def _budget_ceiling_reason(result: dict[str, Any]) -> str | None:
    could_not_act = result.get("could_not_act")
    if not isinstance(could_not_act, list):
        return None
    for item in could_not_act:
        if isinstance(item, dict) and str(item.get("reason", "")).startswith("budget_ceiling_exceeded:"):
            return str(item["reason"])
    return None


def _load_scan_cost(repo_root: Path) -> tuple[dict[str, list[dict[str, Any]]], str | None]:
    try:
        with (repo_root / _SCAN_COST_RELATIVE).open(encoding="utf-8") as stream:
            content = stream.read(_RECEIPT_LIMIT_BYTES + 1)
        if len(content) > _RECEIPT_LIMIT_BYTES:
            raise ValueError("scan history exceeds size bound")
        payload = json.loads(content)
        if not isinstance(payload, dict) or not isinstance(payload.get("scopes"), dict):
            raise ValueError("invalid scan history")
        scopes = payload["scopes"]
        for scope, samples in scopes.items():
            if not isinstance(scope, str) or not isinstance(samples, list):
                raise ValueError("invalid scan scope")
            for sample in samples:
                if not isinstance(sample, dict) or type(sample.get("truncated")) is not bool:
                    raise ValueError("invalid scan sample")
                for field in ("at", "scan_s", "filter_rows", "visited_rows"):
                    value = sample.get(field)
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                        or not math.isfinite(value)
                        or value < 0
                    ):
                        raise ValueError("invalid scan measurement")
                if (
                    type(sample["filter_rows"]) is not int
                    or type(sample["visited_rows"]) is not int
                    or sample["visited_rows"] > sample["filter_rows"]
                ):
                    raise ValueError("invalid scan row counts")
        return scopes, None
    except FileNotFoundError:
        return {}, None
    except (OSError, ValueError, OverflowError, RecursionError) as exc:
        return {}, type(exc).__name__


def _sample_scan_estimate(sample: dict[str, Any]) -> float:
    """Cost implied by one sample. A truncated scan proves only that its cost is at least scan_s.

    A truncated sample counts as twice what it measured, even above the cap. Clamping it to the
    admissible scan would admit a pass the evidence says cannot finish. The estimate then fails the
    ceiling check with a typed refusal instead.
    """
    if not sample["truncated"]:
        return sample["scan_s"]
    # Corrupt-but-finite huge measurements still lead to the typed cap,
    # rather than overflowing during truncated-cost extrapolation.
    return min(sample["scan_s"], sys.float_info.max / 2) * 2


def _scan_estimate_from_samples(samples: list[dict[str, Any]]) -> float:
    """The newest complete scan supersedes older evidence; only truncated samples at or after it count.

    A complete sample measured the scope's real cost, so an earlier truncated lower bound is obsolete
    and cannot override it. A truncated sample proves only that cost is at least scan_s, so it counts
    (doubled) when it is no older than the newest complete scan. A tie keeps the truncated sample.
    The recorder sets truncated for every stopped, failed or partial scan, so truncated is the sole
    completeness test. ceiling_limited alone is never evidence either way.
    """
    horizon = max((sample["at"] for sample in samples if not sample["truncated"]), default=None)
    relevant = [sample for sample in samples if horizon is None or sample["at"] >= horizon]
    return max(_sample_scan_estimate(sample) for sample in relevant)


def _scan_estimate(repo_root: Path, task_ref: str | None) -> tuple[float, str, str | None]:
    scopes, error = _load_scan_cost(repo_root)
    now = time.time()
    samples = [sample for sample in scopes.get(task_ref or "all", []) if 0 <= now - sample["at"] < _SCAN_COST_FRESH_S]
    if samples:
        return _scan_estimate_from_samples(samples), "measured", error
    return _SCAN_COST_BOOTSTRAP_S, "unreadable" if error else "bootstrap", error


def _record_scan_cost(
    repo_root: Path,
    task_ref: str | None,
    *,
    phase_timings: dict[str, float],
    truncated: bool,
    filter_rows: int,
    visited_rows: int,
    ceiling_limited: bool = False,
) -> str | None:
    """Called under the sweep lock; retain eight samples and reclaim old scopes."""
    scopes, error = _load_scan_cost(repo_root)
    now = time.time()
    scopes = {
        scope: samples[-_SCAN_COST_SAMPLE_LIMIT:]
        for scope, samples in scopes.items()
        if samples and now - samples[-1]["at"] <= _STATE_TTL_S
    }
    scope = task_ref or "all"
    scopes[scope] = (
        scopes.get(scope, [])
        + [
            {
                "at": now,
                "scan_s": sum(max(0.0, phase_timings.get(phase, 0.0)) for phase in _SCAN_COST_PHASES),
                "truncated": truncated,
                "filter_rows": filter_rows,
                "visited_rows": visited_rows,
                "ceiling_limited": ceiling_limited,
            }
        ]
    )[-_SCAN_COST_SAMPLE_LIMIT:]
    newest = sorted(scopes, key=lambda key: scopes[key][-1]["at"], reverse=True)[:_STATE_MAX_ENTRIES]
    _atomic_json_replace(
        repo_root / _SCAN_COST_RELATIVE, {"version": 1, "scopes": {key: scopes[key] for key in newest}}
    )
    return error


def _absorbed_landing(
    repo_root: Path,
    row: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    deadline: float,
    manifest_cache: dict[str, dict[str, Any] | None] | None = None,
    integration_refs_cache: dict[str, tuple[list[str], str | None]] | None = None,
) -> tuple[str | None, str | None]:
    """Prove succession and actual Git branch containment; unknowns fail closed."""
    task = str(row.get("task_ref") or "")
    lane = str(row.get("lane_id") or "")
    if not task or Path(task).name != task:
        return None, "manifest_missing"
    if not (repo_root / "config/lane-orchestration" / f"{task}.json").exists():
        return None, "manifest_missing"
    manifests = manifest_cache if manifest_cache is not None else {}
    if task not in manifests:
        manifests[task] = _read_task_manifest(repo_root, task)
    manifest = manifests[task]
    if not isinstance(manifest, dict) or not isinstance(manifest.get("depends_on", {}), dict):
        return None, "manifest_invalid"
    dependencies = manifest.get("depends_on", {})
    successors = {
        successor
        for successor, predecessors in dependencies.items()
        if successor != lane and isinstance(predecessors, list) and lane in predecessors
    }
    if not successors:
        return None, "no_successor"
    if not any(
        other.get("task_ref") == task
        and other.get("lane_id") in successors
        and str(other.get("status") or "").strip().lower() in {"merged", "closed"}
        for other in rows
    ):
        return None, "successor_not_terminal"

    def git(*args: str) -> subprocess.CompletedProcess[str]:
        remaining = deadline - _monotonic()
        if remaining <= 0:
            raise TimeoutError("budget exhausted")
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=min(_ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining),
        )

    branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    try:
        if not branch or git("show-ref", "--verify", "--quiet", f"refs/heads/{branch}").returncode != 0:
            return None, "branch_missing"
        refs, _source = _integration_refs_cached(
            repo_root,
            task,
            integration_refs_cache if integration_refs_cache is not None else {},
        )
        if not refs:
            return None, "integration_ref_missing"
        for ref in refs:
            tip = git("rev-parse", "--verify", f"{ref}^{{commit}}")
            if tip.returncode != 0:
                return None, "integration_ref_missing"
            landing = tip.stdout.strip()
            merged = git("branch", "--merged", landing, "--format=%(refname)")
            if merged.returncode != 0:
                return None, "merge_probe_failed"
            if f"refs/heads/{branch}" in merged.stdout.splitlines():
                return landing, None
        return None, "not_merged"
    except TimeoutError:
        return None, "budget_exhausted"
    except Exception:  # noqa: BLE001 — unknown evidence cannot authorize close
        return None, "merge_probe_failed"


# Successor-less rows that a tombstone may route to the disposition decision. Admission never authorizes removal.
_TOMBSTONE_ADMISSION_REASONS = frozenset({"no_successor", "successor_not_terminal"})


def _tombstone_admission(
    repo_root: Path,
    *,
    branch: str,
    integration_ref: str,
    deadline: float,
    shared_cache: dict[str, tuple[SharedReads | None, str | None, str | None]],
) -> tuple[bool, str | None, str | None]:
    """Admit a branch that the integration tombstone scan names; collect_snapshot/decide verify tip and evidence.

    Returns (admitted, probe_error, probe_detail); a probe error is never admitted.
    """
    from workbay_orchestrator_mcp.orchestration import lane_disposition  # noqa: PLC0415

    if integration_ref not in shared_cache:
        remaining_s = deadline - _monotonic()
        if remaining_s <= 0:
            return False, "budget_exhausted", None
        try:
            shared: SharedReads | None = lane_disposition.collect_shared(
                repo_root,
                integration_ref=integration_ref,
                timeout_s=min(_ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining_s),
            )
            shared_cache[integration_ref] = (shared, None, None)
        except Exception as exc:  # noqa: BLE001 — an unreadable tombstone scan cannot admit a branch
            shared_cache[integration_ref] = (
                None,
                f"tombstone_scan_failed:{type(exc).__name__}",
                f"{type(exc).__name__}: {exc}",
            )
    shared, error, detail = shared_cache[integration_ref]
    if error is not None:
        return False, error, detail
    scan = shared.scan if shared is not None else None
    if scan is None or not scan.complete:
        return False, "tombstone_scan_incomplete", None
    named = any(
        lane_disposition._short_branch_name(tombstone.branch) == branch for _commit, tombstone in scan.tombstones
    )
    return named, None, None


def _close_absorbed_lane(row: dict[str, Any], landing: str) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane

    return manage_worktree_lane(
        operation="close",
        task_ref=str(row["task_ref"]),
        lane_id=str(row["lane_id"]),
        landing_commit_sha=landing,
        notes="Superseded by a terminal dependent lane; branch absorbed into integration.",
    )


def _count_active_but_merged(
    repo_root: Path,
    rows: list[dict[str, Any]],
    *,
    deadline: float,
    integration_refs_cache: dict[str, tuple[list[str], str | None]] | None = None,
    branch_inventory: set[str] | None = None,
) -> tuple[int, list[str], bool]:
    """Count non-terminal lane branches already merged into their task target."""
    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        TERMINAL_STATUSES,
    )

    grouped: dict[str, set[str]] = {}
    pending: list[tuple[str, str, list[str]]] = []
    errors: list[str] = []
    complete = True
    refs_by_task: dict[str, list[str]] = {}
    shared_refs = integration_refs_cache if integration_refs_cache is not None else {}
    for row in rows:
        if str(row.get("status") or "").strip().lower() in TERMINAL_STATUSES:
            continue
        branch = str(row.get("branch") or "").strip()
        task = str(row.get("task_ref") or "").strip()
        if not branch or not task:
            continue
        normalized_branch = branch.removeprefix("refs/heads/")
        if branch_inventory is not None and normalized_branch not in branch_inventory:
            continue
        if _monotonic() >= deadline:
            complete = False
            break
        try:
            refs = refs_by_task.get(task)
            if refs is None:
                refs, _source = _integration_refs_cached(repo_root, task, shared_refs)
                refs_by_task[task] = refs
            if not refs:
                errors.append(f"active_merged_ref_missing:{task}")
                continue
            pending.append((task, normalized_branch, refs))
            for ref in refs:
                grouped.setdefault(ref, set()).add(normalized_branch)
        except Exception as exc:  # noqa: BLE001 — invalid manifests are visible
            errors.append(f"active_merged_ref_failed:{task}:{type(exc).__name__}:{exc}")

    merged_by_ref: dict[str, set[str]] = {}
    for ref in sorted(grouped):
        remaining = deadline - _monotonic()
        if remaining <= 0:
            complete = False
            break
        try:
            proc = subprocess.run(
                ["git", "-C", str(repo_root), "branch", "--merged", ref, "--format=%(refname:short)"],
                capture_output=True,
                text=True,
                check=False,
                timeout=min(_ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            errors.append(f"active_merged_probe_failed:{ref}:{type(exc).__name__}:{exc}")
            continue
        if proc.returncode != 0:
            errors.append(f"active_merged_probe_failed:{ref}:{(proc.stderr or proc.stdout).strip()}")
            continue
        merged_by_ref[ref] = {line.strip() for line in proc.stdout.splitlines() if line.strip()}

    count = sum(1 for _task, branch, refs in pending if any(branch in merged_by_ref.get(ref, set()) for ref in refs))
    return count, errors, complete


def _authoritative_lane_identities(
    repo_root: Path, *, deadline: float
) -> tuple[set[tuple[str, str]] | None, str | None]:
    """Every worktree_lanes identity across tasks, or None when that universe is not provably complete.

    The cross-task pager fails closed on drift and dropped rows, so any raised error means unknown.
    """
    try:
        rows = _load_lane_rows(repo_root, None, deadline=deadline)
    except Exception as exc:  # noqa: BLE001 — unknown evidence keeps other tasks' history, and says so
        return None, f"{type(exc).__name__}:{exc}"[:300]
    return {(str(row.get("task_ref") or ""), str(row.get("lane_id") or "")) for row in rows}, None


def _retained_other_task_keys(
    attempts: dict[tuple[str, str], float],
    *,
    now_epoch: float,
    global_identities: Callable[[], set[tuple[str, str]] | None] | None,
) -> set[tuple[str, str]]:
    """Other-task history that a scoped pass keeps: live identities, or all of it when evidence is unknown.

    A scoped scan cannot see other tasks' rows. Global evidence decides only when the TTL or the
    historical cap would act, so it is read lazily for that case. Unknown evidence keeps everything.
    An identity missing from a complete global universe is confirmed disappeared historical state.
    """
    if not attempts:
        return set()
    expired = any(now_epoch - attempted_at > _STATE_TTL_S for attempted_at in attempts.values())
    if not expired and len(attempts) <= _STATE_MAX_ENTRIES:
        return set(attempts)
    live = global_identities() if global_identities is not None else None
    if live is None:
        return set(attempts)
    return {key for key in attempts if key in live}


def _load_retry_state(
    repo_root: Path,
    *,
    now_epoch: float,
    candidate_states: dict[tuple[str, str], dict[str, Any]] | None = None,
    retained_keys: set[tuple[str, str]] | None = None,
    scope_task_ref: str | None = None,
    global_identities: Callable[[], set[tuple[str, str]] | None] | None = None,
) -> tuple[tuple[str, str] | None, int | None, dict[tuple[str, str], dict[str, Any]], int]:
    path = repo_root / _RETRY_STATE_RELATIVE
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None, {}, 0
    cursor_value = payload.get("cursor") if isinstance(payload, dict) else None
    cursor: tuple[str, str] | None = None
    cursor_priority: int | None = None
    if isinstance(cursor_value, dict):
        task = cursor_value.get("task_ref")
        lane = cursor_value.get("lane_id")
        if isinstance(task, str) and isinstance(lane, str):
            cursor = (task, lane)
        raw_priority = cursor_value.get("priority")
        if type(raw_priority) is int and raw_priority in {0, 1, 2}:
            cursor_priority = raw_priority
    entries: dict[tuple[str, str], dict[str, Any]] = {}
    retained = set(retained_keys if retained_keys is not None else candidate_states or {})
    pruned = 0
    raw_entries = payload.get("refusals", []) if isinstance(payload, dict) else []
    if isinstance(raw_entries, list):
        other_attempts: dict[tuple[str, str], float] = {}
        for item in raw_entries if scope_task_ref is not None else []:
            if (
                isinstance(item, dict)
                and isinstance(item.get("task_ref"), str)
                and isinstance(item.get("lane_id"), str)
                and item["task_ref"] != scope_task_ref
            ):
                try:
                    other_attempts[(item["task_ref"], item["lane_id"])] = float(item.get("attempted_at", 0))
                except (TypeError, ValueError, OverflowError):
                    continue
        retained.update(
            _retained_other_task_keys(other_attempts, now_epoch=now_epoch, global_identities=global_identities)
        )
        for item in raw_entries:
            if not isinstance(item, dict):
                continue
            task, lane = item.get("task_ref"), item.get("lane_id")
            try:
                count = max(0.0, float(item.get("count", 0)))
                attempted_at = float(item.get("attempted_at", 0))
                retry_after = float(item.get("retry_after", 0))
            except (TypeError, ValueError, OverflowError):
                continue
            if (
                not isinstance(task, str)
                or not isinstance(lane, str)
                or not all(math.isfinite(value) and value >= 0 for value in (count, attempted_at, retry_after))
                or ((task, lane) not in retained and now_epoch - attempted_at > _STATE_TTL_S)
            ):
                continue
            current_state = candidate_states.get((task, lane)) if candidate_states is not None else None
            if (
                isinstance(current_state, dict)
                and current_state.get("worktree_exists") is False
                and current_state.get("branch_exists") is False
            ):
                pruned += 1
                continue
            entry: dict[str, Any] = {
                "count": count,
                "attempted_at": attempted_at,
                "retry_after": retry_after,
            }
            if isinstance(item.get("reason"), str):
                entry["reason"] = item["reason"]
            candidate_state = item.get("candidate_state")
            branch_exists = candidate_state.get("branch_exists") if isinstance(candidate_state, dict) else None
            if (
                isinstance(candidate_state, dict)
                and isinstance(candidate_state.get("worktree_exists"), bool)
                and (branch_exists is None or isinstance(branch_exists, bool))
                and isinstance(candidate_state.get("status"), str)
            ):
                entry["candidate_state"] = {
                    "worktree_exists": candidate_state["worktree_exists"],
                    "branch_exists": branch_exists,
                    "status": candidate_state["status"],
                }
            entries[(task, lane)] = entry
    if len(entries) > _STATE_MAX_ENTRIES:
        newest = sorted(entries.items(), key=lambda item: item[1]["attempted_at"], reverse=True)
        # Active backlog history is essential to fairness. The old cap/TTL
        # bounds only historical entries, never a lane still in the row set.
        historical = [item for item in newest if item[0] not in retained][:_STATE_MAX_ENTRIES]
        entries = {**{key: value for key, value in newest if key in retained}, **dict(historical)}
    return cursor, cursor_priority, entries, pruned


def _save_retry_state(
    repo_root: Path,
    *,
    cursor: tuple[str, str] | None,
    cursor_priority: int | None = None,
    entries: dict[tuple[str, str], dict[str, Any]],
    now_epoch: float,
    candidate_states: dict[tuple[str, str], dict[str, Any]] | None = None,
    retained_keys: set[tuple[str, str]] | None = None,
    scope_task_ref: str | None = None,
    global_identities: Callable[[], set[tuple[str, str]] | None] | None = None,
) -> int:
    pruned = 0
    retained = set(retained_keys if retained_keys is not None else candidate_states or {})
    other_attempts = {
        key: float(value.get("attempted_at", 0.0))
        for key, value in entries.items()
        if scope_task_ref is not None and key[0] != scope_task_ref
    }
    retained.update(_retained_other_task_keys(other_attempts, now_epoch=now_epoch, global_identities=global_identities))
    live = []
    for key, value in entries.items():
        current_state = candidate_states.get(key) if candidate_states is not None else None
        if (
            isinstance(current_state, dict)
            and current_state.get("worktree_exists") is False
            and current_state.get("branch_exists") is False
        ):
            pruned += 1
            continue
        if key in retained or now_epoch - value.get("attempted_at", 0.0) <= _STATE_TTL_S:
            live.append((key, value))
    ordered = sorted(live, key=lambda item: item[1].get("attempted_at", 0.0), reverse=True)
    newest = [item for item in ordered if item[0] in retained]
    newest += [item for item in ordered if item[0] not in retained][:_STATE_MAX_ENTRIES]
    refusals = []
    for key, value in newest:
        refusal: dict[str, Any] = {
            "task_ref": key[0],
            "lane_id": key[1],
            "count": int(value.get("count", 0)),
            "attempted_at": value.get("attempted_at", 0.0),
            "retry_after": value.get("retry_after", 0.0),
        }
        candidate_state = value.get("candidate_state")
        if isinstance(candidate_state, dict):
            refusal["candidate_state"] = candidate_state
        if isinstance(value.get("reason"), str):
            refusal["reason"] = value["reason"]
        refusals.append(refusal)
    cursor_value: dict[str, Any] | None = None
    if cursor is not None:
        cursor_value = {"task_ref": cursor[0], "lane_id": cursor[1]}
        if type(cursor_priority) is int and cursor_priority in {0, 1, 2}:
            cursor_value["priority"] = cursor_priority
    payload = {
        "version": 1,
        "updated_at": now_epoch,
        "cursor": cursor_value,
        "refusals": refusals,
    }
    _atomic_json_replace(repo_root / _RETRY_STATE_RELATIVE, payload)
    return pruned


def _rotate_sweep_log(root: Path) -> None:
    path = root / _LOG_RELATIVE
    if path.exists() and path.stat().st_size >= _SWEEP_LOG_LIMIT_BYTES:
        backup = path.with_name(f"{path.name}.1")
        backup.unlink(missing_ok=True)
        os.replace(path, backup)


def _load_child_marker(root: Path) -> tuple[str, dict[str, Any] | None]:
    marker = root / _CHILD_MARKER_RELATIVE
    try:
        raw = marker.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "missing", None
    except OSError:
        return "unknown", None
    try:
        payload = json.loads(raw)
    except ValueError:
        return "unknown", None
    if not isinstance(payload, dict):
        return "unknown", None
    return "present", payload


def _sweep_child_state(root: Path) -> tuple[str, dict[str, Any] | None]:
    """Classify a marker only from durable PID, root, session, and start identity."""
    marker_state, payload = _load_child_marker(root)
    if marker_state != "present" or payload is None:
        return marker_state, payload
    pid = payload.get("pid")
    started_value = payload.get("started_at")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(started_value, bool)
        or not isinstance(started_value, (int, float))
    ):
        return "unknown", payload

    process_state, identity = _read_process_identity(pid)
    if process_state == "absent":
        return "stale", payload
    if process_state != "available" or identity is None:
        return "unknown", payload
    if not _identity_fields_match(identity, payload, repo_root=root, pid=pid, require_start_token=False):
        return "stale", payload
    start_token = payload.get("start_token")
    if not isinstance(start_token, str) or not start_token.strip():
        return "unknown", payload
    if not _identity_fields_match(identity, payload, repo_root=root, pid=pid):
        return "stale", payload
    lifetime_value = payload.get("lifetime_s")
    lifetime_s = (
        float(lifetime_value)
        if isinstance(lifetime_value, (int, float))
        and not isinstance(lifetime_value, bool)
        and math.isfinite(float(lifetime_value))
        and float(lifetime_value) >= 0
        else _SWEEP_CHILD_LIFETIME_S
    )
    if time.time() - float(started_value) > lifetime_s:
        return "expired", payload
    return "running", payload


def _sweep_child_lifetime_s(*, budget_s: float, await_landing_quiescence_s: float) -> float:
    """Bound a launched marker through its configured wait, sweep, and exit margin."""
    try:
        lifetime = (
            max(0.0, float(await_landing_quiescence_s)) + max(0.0, float(budget_s)) + _SWEEP_CHILD_LIFETIME_MARGIN_S
        )
    except (TypeError, ValueError, OverflowError):
        return _SWEEP_CHILD_LIFETIME_S
    return lifetime if math.isfinite(lifetime) else _SWEEP_CHILD_LIFETIME_S


def _clear_child_marker(
    root: Path,
    *,
    expected: dict[str, Any] | None = None,
    identity: dict[str, Any] | None = None,
    launch_lock: TextIO | None = None,
) -> bool:
    """Compare-and-remove a marker while holding the launch lock."""
    if expected is None and identity is None:
        return False
    owns_lock = launch_lock is None
    lock = launch_lock
    if lock is None:
        lock, _error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
        if lock is None:
            return False
    try:
        state, current = _load_child_marker(root)
        if state != "present" or current is None:
            return False
        if expected is not None and current != expected:
            return False
        if identity is not None:
            pid = identity.get("pid")
            if isinstance(pid, bool) or not isinstance(pid, int):
                return False
            if not _identity_fields_match(identity, current, repo_root=root, pid=pid):
                return False
        marker = root / _CHILD_MARKER_RELATIVE
        marker.unlink(missing_ok=True)
        return True
    finally:
        if owns_lock and lock is not None:
            _release_lock(lock)


def _expire_sweep_child(root: Path, expected: dict[str, Any]) -> str:
    """Recheck both marker and process immediately before an expiry signal."""
    marker_state, current = _load_child_marker(root)
    if marker_state != "present" or current != expected:
        return "changed"
    result = _signal_owned_group(expected, root, signal.SIGKILL)
    if result == "absent":
        _clear_child_marker(root, expected=expected)
    return result


def _write_receipt(repo_root: Path, summary: dict[str, Any]) -> None:
    if not isinstance(summary.get("started_at"), str) or not summary["started_at"]:
        summary["started_at"] = _utc_now_iso()
    summary["finished_at"] = _utc_now_iso()
    summary.setdefault("wake", {"outcome": "not_needed"})
    path = repo_root / _RECEIPT_RELATIVE
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.exists() and path.stat().st_size >= _RECEIPT_LIMIT_BYTES:
            backup = path.with_name(f"{path.name}.1")
            backup.unlink(missing_ok=True)
            os.replace(path, backup)
    except OSError:
        raise
    receipt = dict(summary)
    if receipt.get("journaled_events"):
        # These events already have a write-ahead receipt; keep references in
        # the final summary without publishing the same event twice.
        receipt["events"] = []
    line = json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n"
    with path.open("a+b") as handle:
        # Preserve a torn append as evidence, but isolate it from the next
        # receipt so a crash cannot swallow subsequent durable events.
        handle.seek(0, os.SEEK_END)
        if handle.tell():
            handle.seek(-1, os.SEEK_END)
            if handle.read(1) != b"\n":
                handle.write(b"\n")
        handle.write(line.encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())


def _journal_review_event(repo_root: Path, event: dict[str, Any]) -> None:
    """Write ahead under the sweep lock, deduplicating retries in retained receipts."""
    receipt = repo_root / _RECEIPT_RELATIVE
    for path in (receipt, receipt.with_name(f"{receipt.name}.1")):
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                start = max(0, handle.tell() - _RECEIPT_LIMIT_BYTES)
                handle.seek(start)
                lines = handle.read(_RECEIPT_LIMIT_BYTES).splitlines()
        except FileNotFoundError:
            continue
        if start and lines:
            lines = lines[1:]
        for line in lines:
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                # An incomplete receipt proves no publication. Retain it,
                # replay intact receipts, and durably publish on this retry.
                continue
            if not isinstance(record, dict):
                continue
            if event in record.get("events", []):
                return
    _write_receipt(repo_root, {"events": [event], "outcome": "review_event_write_ahead", "apply": True})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _result_detail(payload: dict[str, Any]) -> str | None:
    value = payload.get("detail") or payload.get("error")
    return str(value)[:300] if value is not None and str(value) else None


def _add_unactionable(
    *,
    lane_id: object,
    task_ref: object,
    status: object,
    reason: str,
    could_not_act: list[dict[str, Any]],
    skip_counts: Counter[str],
    detail: str | None = None,
) -> None:
    lane = str(lane_id or "")
    task = str(task_ref or "")
    row = {"lane_id": lane, "task_ref": task, "status": str(status or ""), "reason": reason}
    if detail:
        row["detail"] = detail[:300]
    if len(could_not_act) < _DETAIL_CAP:
        could_not_act.append(row)
    skip_counts[reason] += 1


def _budget_exhausted_summary(
    *,
    apply: bool,
    task_ref: str | None,
    phase: str,
    started: float,
    phase_timings: dict[str, float],
    lane_progress: dict[str, Any],
    outcome: str = "budget_exhausted",
    tombstones_drained: int = 0,
    tombstones_pending: int = 0,
    tombstone_could_not_act: list[dict[str, Any]] | None = None,
    tombstone_skip_counts: dict[str, int] | None = None,
    tombstone_fence_release: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return a typed budget stop with lane-discovery progress attached."""
    rows_fetched = int(lane_progress.get("rows_fetched") or 0)
    pages_fetched = int(lane_progress.get("pages_fetched") or 0)
    total_matching = lane_progress.get("total_matching")
    if type(total_matching) is not int:
        total_matching = None
    skip_counts = dict(tombstone_skip_counts or {})
    if outcome == "retire_admission_starved":
        skip_counts["retire_admission_starved"] = skip_counts.get("retire_admission_starved", 0) + 1
    summary: dict[str, Any] = {
        "ok": outcome != "retire_admission_starved",
        "outcome": outcome,
        "apply": bool(apply),
        "task_ref": task_ref,
        "candidates": 0,
        "attempted": 0,
        "outcome_counts": {},
        "skip_counts": skip_counts,
        "retired_lane_ids": [],
        "tombstones_drained": tombstones_drained,
        "tombstones_pending": tombstones_pending,
        "could_not_act": tombstone_could_not_act or [],
        "could_not_act_truncated": bool(tombstone_could_not_act and len(tombstone_could_not_act) >= _DETAIL_CAP),
        "missing_worktree_and_branch": 0,
        "lane_timings": [],
        "phase_timings_s": {**phase_timings, "total_s": max(0.0, _monotonic() - started)},
        "identity_filter_timing": _empty_identity_filter_timing(),
        "absorbed_retired": 0,
        "absorbed_skipped": 0,
        "active_but_merged": 0,
        "active_but_merged_complete": False,
        "has_more": True,
        "remaining": total_matching,
        "instrument_errors": [],
        "budget_progress": {
            "phase": phase,
            "lane_rows_fetched": rows_fetched,
            "lane_rows_pages_fetched": pages_fetched,
            "lane_rows_total": total_matching,
        },
    }
    summary.update(_fence_release_fields(tombstone_fence_release))
    return summary


def _fence_release_fields(records: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Summary fields for steps that ran but left a reclaim fence unreleased; ``{}`` when none.

    Additive only: a pass with no such step keeps its old summary shape. The records
    stay typed beside the pass outcome and are never folded into it [AGT-10].
    """
    if not records:
        return {}
    return {"fence_release": list(records), "cleanup_pending": "reclaim_fence_release"}


def _sweep_locked(
    *,
    repo_root: Path,
    apply: bool,
    max_lanes: int,
    deadline: float,
    grace_s: float,
    task_ref: str | None,
    started: float,
    budget_below_retire_allowance: bool,
    budget_ceiling_reason: str | None = None,
    scan_progress: dict[str, Any] | None = None,
    scan_ceiling_limited: bool = False,
) -> dict[str, Any]:
    """Coordinate one retire pass under the sweep lock.

    The pass contract (inputs, state ownership, dependency injection and
    side-effect order) is in ``lane_retire_sweep_context``; the units are in
    ``lane_retire_sweep_steps``. Units resolve collaborators through this
    module at call time, so its monkeypatch seams stay authoritative.
    """
    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        TERMINAL_STATUSES,
    )
    from workbay_orchestrator_mcp.orchestration.lane_reclaim import (  # noqa: PLC0415
        _session_live,
    )
    from workbay_orchestrator_mcp.orchestration.worker_lock import (  # noqa: PLC0415
        lock_is_held,
    )

    from . import lane_retire_sweep_steps as steps  # noqa: PLC0415
    from . import worktree_tombstone  # noqa: PLC0415
    from .lane_retire_sweep_context import BREAK, SweepInputs, SweepState  # noqa: PLC0415

    state = SweepState(timeout_receipt_keys=_retire_timeout_receipt_keys(repo_root))
    allowance_s = _retire_admission_allowance_s()
    scan_reservation_enabled = deadline - started >= allowance_s
    inputs = SweepInputs(
        deps=sys.modules[__name__],
        terminal_statuses=TERMINAL_STATUSES,
        session_live=_session_live,
        lock_is_held=lock_is_held,
        worktree_tombstone=worktree_tombstone,
        repo_root=repo_root,
        apply=apply,
        max_lanes=max_lanes,
        budget_deadline=deadline,
        grace_s=grace_s,
        task_ref=task_ref,
        started=started,
        budget_ceiling_reason=budget_ceiling_reason,
        scan_progress=scan_progress,
        scan_ceiling_limited=scan_ceiling_limited,
        allowance_s=allowance_s,
        scan_reservation_enabled=scan_reservation_enabled,
        scan_deadline=deadline - allowance_s if scan_reservation_enabled else deadline,
    )

    # 1. Scan: any budget stop returns its typed summary.
    budget_stop = steps.load_lane_rows(inputs, state)
    if budget_stop is not None:
        return budget_stop
    steps.drain_tombstones(inputs, state)
    budget_stop = steps.inventory_worktrees(inputs, state)
    if budget_stop is not None:
        return budget_stop
    budget_stop = steps.inventory_branches(inputs, state)
    if budget_stop is not None:
        return budget_stop

    # 2. Selection.
    identity_started = steps.prepare_selection(inputs, state)
    steps.filter_identities(inputs, state)
    steps.record_scan_cost(inputs, state, identity_started)
    steps.compute_next_grace_expiry(inputs, state)
    steps.order_candidates(inputs, state)

    # 3. Admission and disposition, one candidate at a time.
    for index, item in enumerate(state.candidates):
        if steps.process_candidate(inputs, state, index, item) == BREAK:
            break

    # 4. Reporting.
    return steps.report(inputs, state)


def sweep(
    *,
    repo_root: Path | str,
    apply: bool,
    max_lanes: int = 20,
    budget_s: float = 120.0,
    grace_s: float = 0.0,
    task_ref: str | None = None,
    await_landing_quiescence_s: float = 0.0,
    _continuation_count: int | None = None,
    _defer_receipt: bool = False,
) -> dict[str, Any]:
    """Retire a bounded set of finished, clean, idle lane worktrees."""
    invocation_started = _monotonic()
    receipt_started_at = _utc_now_iso()
    wait_limit_s = max(0.0, float(await_landing_quiescence_s))
    if not math.isfinite(wait_limit_s):
        return {
            "ok": False,
            "outcome": "instrument_error",
            "apply": bool(apply),
            "task_ref": task_ref,
            "has_more": True,
            "remaining": 0,
            "landing_quiescence_wait_s": 0.0,
            "continuation_reason": "none",
            "instrument_errors": ["invalid_landing_quiescence_wait_s"],
            "elapsed_s": max(0.0, _monotonic() - invocation_started),
            "started_at": receipt_started_at,
            "identity_filter_timing": _empty_identity_filter_timing(),
            "wake": {"outcome": "not_needed"},
        }
    try:
        requested_root = Path(repo_root).expanduser().resolve()
        root = _configure_runtime(requested_root)
    except Exception as exc:  # noqa: BLE001 — setup failures fail closed
        return {
            "ok": False,
            "outcome": "instrument_error",
            "apply": bool(apply),
            "has_more": True,
            "remaining": 0,
            "landing_quiescence_wait_s": 0.0,
            "continuation_reason": "none",
            "instrument_errors": [f"runtime_config_failed:{type(exc).__name__}:{exc}"],
            "elapsed_s": max(0.0, _monotonic() - invocation_started),
            "started_at": receipt_started_at,
            "identity_filter_timing": _empty_identity_filter_timing(),
            "wake": {"outcome": "not_needed"},
        }

    configured_budget_s = max(0.0, float(budget_s))
    budget_below_retire_allowance = configured_budget_s < _retire_admission_allowance_s()
    scan_estimate_s, scan_estimate_source, scan_cost_error = _scan_estimate(root, task_ref)
    # A dry run is the same pass without mutation: it resolves the same budget,
    # cap and ceiling verdict as apply, so both modes admit the same lanes.
    resolved_budget_s, budget_ceiling_reason = _resolve_pass_budget_s(
        configured_budget_s, scan_estimate_s=scan_estimate_s
    )
    budget_metadata = {
        "resolved_budget_s": resolved_budget_s,
        "scan_estimate_s": scan_estimate_s,
        "scan_estimate_source": scan_estimate_source,
    }
    if scan_cost_error is not None:
        budget_metadata["scan_cost_unreadable"] = scan_cost_error

    def annotate_budget(summary: dict[str, Any]) -> None:
        summary.update(budget_metadata)
        if budget_ceiling_reason is None:
            return
        # Every post-resolution return must retain the ceiling verdict,
        # including contention paths that never acquire the sweep lock.
        # Preserve instrument errors while forbidding continuations/wakes.
        summary.update(ok=False, outcome="retire_admission_starved", attempted=0, has_more=True)
        details = summary.setdefault("could_not_act", [])
        if not any(item.get("reason") == budget_ceiling_reason for item in details):
            ceiling_detail = {
                "lane_id": None,
                "task_ref": task_ref,
                "status": None,
                "reason": budget_ceiling_reason,
            }
            if len(details) >= _DETAIL_CAP:
                details[-1] = ceiling_detail
                summary["could_not_act_truncated"] = True
            else:
                details.append(ceiling_detail)
        summary.setdefault("skip_counts", {})["retire_admission_starved"] = 1

    landing_wait_s = 0.0
    if wait_limit_s > 0:
        quiescent, landing_wait_s = _await_landing_quiescence(root, timeout_s=wait_limit_s)
        if not quiescent:
            summary: dict[str, Any] = {
                "ok": True,
                "outcome": "deferred_landing_active",
                "apply": bool(apply),
                "task_ref": task_ref,
                "has_more": True,
                "remaining": None,
                "budget_below_retire_allowance": budget_below_retire_allowance,
                "landing_quiescence_wait_s": landing_wait_s,
                "continuation_reason": "none",
                "elapsed_s": max(0.0, _monotonic() - invocation_started),
                "started_at": receipt_started_at,
                "identity_filter_timing": _empty_identity_filter_timing(),
                "wake": {"outcome": "not_needed"},
            }
            annotate_budget(summary)
            summary["continuation_reason"] = _continuation_decision(
                summary, apply=bool(apply), continuation_count=_continuation_count
            )
            if not _defer_receipt:
                try:
                    _write_receipt(root, summary)
                except OSError as exc:
                    summary["ok"] = False
                    summary["outcome"] = "instrument_error"
                    summary["instrument_errors"] = [f"receipt_write_failed:{type(exc).__name__}:{exc}"]
            process_state, process_identity = _read_process_identity(os.getpid())
            if process_state == "available" and process_identity is not None:
                _clear_child_marker(root, identity=process_identity)
            return summary

    # Landing contention is outside the sweep budget. The successful probe is
    # released before this clock starts and before per-candidate fences run.
    started = _monotonic()
    budget_deadline = started + resolved_budget_s
    lock, lock_error = _try_acquire_lock(root)
    if lock is None:
        if lock_error == "already_running":
            summary = {
                "ok": True,
                "outcome": "already_running",
                "apply": bool(apply),
                "has_more": True,
                "remaining": None,
                "budget_below_retire_allowance": budget_below_retire_allowance,
                "landing_quiescence_wait_s": landing_wait_s,
                "continuation_reason": "none",
                "elapsed_s": max(0.0, _monotonic() - invocation_started),
                "started_at": receipt_started_at,
                "identity_filter_timing": _empty_identity_filter_timing(),
                "wake": {"outcome": "not_needed"},
            }
        else:
            summary = {
                "ok": False,
                "outcome": "instrument_error",
                "apply": bool(apply),
                "has_more": True,
                "remaining": 0,
                "budget_below_retire_allowance": budget_below_retire_allowance,
                "landing_quiescence_wait_s": landing_wait_s,
                "continuation_reason": "none",
                "instrument_errors": [str(lock_error or "sweep_lock_failed")],
                "elapsed_s": max(0.0, _monotonic() - invocation_started),
                "started_at": receipt_started_at,
                "identity_filter_timing": _empty_identity_filter_timing(),
                "wake": {"outcome": "not_needed"},
            }
        annotate_budget(summary)
        summary["continuation_reason"] = _continuation_decision(
            summary, apply=bool(apply), continuation_count=_continuation_count
        )
        if not _defer_receipt:
            try:
                _write_receipt(root, summary)
            except OSError as exc:
                summary["ok"] = False
                summary["outcome"] = "instrument_error"
                summary.setdefault("instrument_errors", []).append(f"receipt_write_failed:{type(exc).__name__}:{exc}")
        process_state, process_identity = _read_process_identity(os.getpid())
        if process_state == "available" and process_identity is not None:
            _clear_child_marker(root, identity=process_identity)
        return summary

    scan_progress: dict[str, Any] = {}
    # The pass's scan reservation was the admission cap; a truncated sample from it is a ceiling verdict.
    scan_ceiling_limited = resolved_budget_s >= _MAX_PASS_BUDGET_S
    try:
        try:
            summary = _sweep_locked(
                repo_root=root,
                apply=bool(apply) and budget_ceiling_reason is None,
                max_lanes=max(0, int(max_lanes)),
                deadline=budget_deadline,
                grace_s=max(0.0, float(grace_s)),
                task_ref=task_ref,
                started=started,
                budget_below_retire_allowance=budget_below_retire_allowance,
                budget_ceiling_reason=budget_ceiling_reason,
                scan_progress=scan_progress,
                scan_ceiling_limited=scan_ceiling_limited,
            )
        except Exception as exc:  # noqa: BLE001 — top-level instrument containment
            summary = {
                "ok": False,
                "outcome": "instrument_error",
                "apply": bool(apply),
                "task_ref": task_ref,
                "has_more": True,
                "remaining": 0,
                "budget_below_retire_allowance": budget_below_retire_allowance,
                "instrument_errors": [f"sweep_failed:{type(exc).__name__}:{exc}"],
            }
            if "identity_started" in scan_progress and not scan_progress.get("recorded"):
                timings = scan_progress["phase_timings"]
                timings["identity_and_linkage_filter_s"] = max(0.0, _monotonic() - scan_progress["identity_started"])
                summary["phase_timings_s"] = {**timings, "total_s": max(0.0, _monotonic() - started)}
                try:
                    error = _record_scan_cost(
                        root,
                        task_ref,
                        phase_timings=timings,
                        truncated=True,
                        filter_rows=int(scan_progress.get("filter_rows") or 0),
                        visited_rows=int((scan_progress.get("identity_filter_timing") or {}).get("rows_visited") or 0),
                        ceiling_limited=scan_ceiling_limited,
                    )
                    if error is not None:
                        summary["scan_cost_unreadable"] = error
                except OSError as scan_exc:
                    summary["instrument_errors"].append(f"scan_cost_write_failed:{type(scan_exc).__name__}:{scan_exc}")
        summary.update(budget_metadata)
        timings = summary.get("phase_timings_s")
        if (
            isinstance(timings, dict)
            and "identity_and_linkage_filter_s" not in timings
            and summary.get("budget_progress")
        ):
            # A scan may exhaust before identity filtering (pagination or
            # inventory). Persist that partial cost too, so retries learn
            # rather than replaying the same bootstrap scan reservation.
            try:
                scan_error = _record_scan_cost(
                    root,
                    task_ref,
                    phase_timings=timings,
                    truncated=True,
                    filter_rows=int((summary.get("budget_progress") or {}).get("lane_rows_fetched") or 0),
                    visited_rows=0,
                    ceiling_limited=scan_ceiling_limited,
                )
                if scan_error is not None:
                    summary["scan_cost_unreadable"] = scan_error
            except OSError as exc:
                summary["ok"] = False
                summary["outcome"] = "instrument_error"
                summary.setdefault("instrument_errors", []).append(f"scan_cost_write_failed:{type(exc).__name__}:{exc}")
        summary["apply"] = bool(apply)
        annotate_budget(summary)
        summary["budget_below_retire_allowance"] = budget_below_retire_allowance
        summary["landing_quiescence_wait_s"] = landing_wait_s
        summary["continuation_reason"] = _continuation_decision(
            summary, apply=bool(apply), continuation_count=_continuation_count
        )
        summary["elapsed_s"] = max(0.0, _monotonic() - invocation_started)
        summary.setdefault("started_at", receipt_started_at)
        summary.setdefault("identity_filter_timing", _empty_identity_filter_timing())
        summary.setdefault("wake", {"outcome": "not_needed"})
        if not _defer_receipt:
            try:
                _write_receipt(root, summary)
            except OSError as exc:
                summary["ok"] = False
                summary["outcome"] = "instrument_error"
                summary.setdefault("instrument_errors", []).append(f"receipt_write_failed:{type(exc).__name__}:{exc}")
    finally:
        _release_lock(lock)
        process_state, process_identity = _read_process_identity(os.getpid())
        if process_state == "available" and process_identity is not None:
            _clear_child_marker(root, identity=process_identity)
    return summary


def spawn_background_sweep(
    repo_root: Path | str,
    *,
    max_lanes: int = _DEFAULT_MAX_LANES,
    budget_s: float = _DEFAULT_SWEEP_BUDGET_S,
    respect_optout: bool = True,
    task_ref: str | None = None,
    _continuation_count: int = 0,
    await_landing_quiescence_s: float = _DEFAULT_LANDING_QUIESCENCE_WAIT_S,
    _wake_idle_chain: int | None = None,
    _wake_generation: int | None = None,
) -> dict[str, Any]:
    """Start an apply sweep detached; return immediately and contain failures."""
    if respect_optout and os.environ.get("WORKBAY_POST_MERGE_RETIRE_SWEEP") == "0":
        return {"ok": True, "outcome": "disabled"}
    scan_error: str | None = None
    try:
        requested_root = Path(repo_root).expanduser().resolve()
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        root = Path(RuntimeConfig.for_repo(requested_root).workspace_root).resolve()
        scan_estimate_s, _scan_source, scan_error = _scan_estimate(root, task_ref)
        budget_s, ceiling_reason = _resolve_pass_budget_s(budget_s, scan_estimate_s=scan_estimate_s)
        if ceiling_reason is not None:
            # A ceiling verdict forbids the child: a scan-only child cannot admit a retire.
            result = _budget_ceiling_refusal(task_ref=task_ref, budget_s=budget_s, reason=ceiling_reason)
        else:
            result = _spawn_detached_sweep(
                root,
                max_lanes=max_lanes,
                budget_s=budget_s,
                task_ref=task_ref,
                await_landing_quiescence_s=await_landing_quiescence_s,
                _continuation_count=_continuation_count,
                _wake_idle_chain=_wake_idle_chain,
                _wake_generation=_wake_generation,
            )
    except Exception as exc:  # noqa: BLE001 — a hook must never fail the merge
        result = {"ok": False, "outcome": "spawn_failed", "error": f"{type(exc).__name__}: {exc}"}
    if scan_error is not None:
        # Every exit after the estimate carries the diagnostic, so no future early return can drop it.
        result["scan_cost_unreadable"] = scan_error
    return result


def _spawn_detached_sweep(
    root: Path,
    *,
    max_lanes: int,
    budget_s: float,
    task_ref: str | None,
    await_landing_quiescence_s: float,
    _continuation_count: int,
    _wake_idle_chain: int | None,
    _wake_generation: int | None,
) -> dict[str, Any]:
    """Launch one detached child under the launch lock; the caller contains failures and adds diagnostics."""
    launch_lock, error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
    if launch_lock is None:
        if error == "already_running":
            return {"ok": True, "outcome": "already_running"}
        return {"ok": False, "outcome": "spawn_failed", "error": str(error)}
    try:
        if _wake_generation is not None:
            current_wake = _wake_marker(root)
            if current_wake is None or current_wake.get("generation") != _wake_generation:
                return {"ok": True, "outcome": "wake_superseded"}
        child_state, child_marker = _sweep_child_state(root)
        child_pid = child_marker.get("pid") if isinstance(child_marker, dict) else None
        if child_state in {"running", "unknown"}:
            return {"ok": True, "outcome": "already_running", "pid": child_pid}
        if child_state == "expired" and child_marker is not None:
            signal_result = _expire_sweep_child(root, child_marker)
            if signal_result == "signaled":
                return {"ok": True, "outcome": "already_running", "pid": child_pid, "recovered": True}
            if signal_result in {"unknown", "changed"}:
                return {"ok": True, "outcome": "already_running", "pid": child_pid}
            _clear_child_marker(root, expected=child_marker, launch_lock=launch_lock)
        elif child_state == "stale" and child_marker is not None:
            _clear_child_marker(root, expected=child_marker, launch_lock=launch_lock)

        lock, lock_error = _try_acquire_lock(root)
        if lock is None:
            if lock_error == "already_running":
                return {"ok": True, "outcome": "already_running"}
            return {"ok": False, "outcome": "spawn_failed", "error": str(lock_error)}
        _release_lock(lock)

        _rotate_sweep_log(root)
        log_path = root / _LOG_RELATIVE
        log_path.parent.mkdir(parents=True, exist_ok=True)
        output = log_path.open("ab")
        try:
            child_argv = [
                sys.executable,
                "-m",
                _MODULE,
                "--apply",
                "--max",
                str(max(0, int(max_lanes))),
                "--budget-s",
                str(max(0.0, float(budget_s))),
                "--retire-timeout-s",
                str(_RETIRE_OPERATION_TIMEOUT_S),
                "--await-landing-quiescence-s",
                str(max(0.0, float(await_landing_quiescence_s))),
            ]
            if task_ref is not None:
                child_argv.extend(["--task-ref", task_ref])
            child_argv.extend(["--continuation-count", str(max(0, int(_continuation_count)))])
            if _wake_idle_chain is not None:
                child_argv.extend(["--wake-idle-chain", str(max(0, int(_wake_idle_chain)))])
            proc = _Popen(
                child_argv,
                cwd=root,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        finally:
            output.close()
        pid = getattr(proc, "pid", None)
        if isinstance(pid, int) and pid > 0:
            identity_state, identity = _read_process_identity(pid)
            if (
                identity_state != "available"
                or identity is None
                or not _identity_fields_match(identity, identity, repo_root=root, pid=pid)
            ):
                # Keep an unknown-token marker while the process may be alive;
                # it prevents duplicate launches but never grants signal authority.
                identity = {
                    "pid": pid,
                    "start_token": None,
                    "repo_root": str(root),
                    "pgid": pid,
                    "sid": pid,
                }
            _atomic_json_replace(
                root / _CHILD_MARKER_RELATIVE,
                {
                    "version": 2,
                    **identity,
                    "scope": task_ref if task_ref is not None else "all",
                    "started_at": time.time(),
                    "lifetime_s": _sweep_child_lifetime_s(
                        budget_s=budget_s,
                        await_landing_quiescence_s=await_landing_quiescence_s,
                    ),
                },
            )
        return {"ok": True, "outcome": "started", "pid": pid, "resolved_budget_s": budget_s}
    finally:
        _release_lock(launch_lock)


def _sweep_wake_max_delay_s() -> float:
    raw = os.environ.get("WORKBAY_SWEEP_WAKE_MAX_DELAY_S")
    try:
        value = float(raw) if raw is not None else _DEFAULT_SWEEP_WAKE_MAX_DELAY_S
    except (TypeError, ValueError):
        value = _DEFAULT_SWEEP_WAKE_MAX_DELAY_S
    if not math.isfinite(value):
        value = _DEFAULT_SWEEP_WAKE_MAX_DELAY_S
    return max(_SWEEP_WAKE_MIN_DELAY_S, value)


def _sweep_wake_max_idle_chain() -> int:
    raw = os.environ.get("WORKBAY_SWEEP_WAKE_MAX_IDLE_CHAIN")
    try:
        value = int(raw) if raw is not None else _DEFAULT_SWEEP_WAKE_MAX_IDLE_CHAIN
    except (TypeError, ValueError):
        value = _DEFAULT_SWEEP_WAKE_MAX_IDLE_CHAIN
    return max(0, value)


def _wake_marker(root: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads((root / _WAKE_MARKER_RELATIVE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _wake_marker_scope(marker: dict[str, Any]) -> str:
    task_ref = marker.get("task_ref")
    legacy_task_ref = task_ref if isinstance(task_ref, str) and task_ref else None
    if "scope" not in marker:
        return legacy_task_ref or "all"

    scope = marker.get("scope")
    if not isinstance(scope, str) or not scope:
        return "all"
    # Preserve the round-five rule for contradictory legacy markers:
    # their task_ref remains the narrower effective scope.
    if scope == "all" and legacy_task_ref is not None:
        return legacy_task_ref
    return scope


def _wake_scope_union(first: str, second: str) -> str:
    """Combine two singleton wake scopes, broadening conflicts to all."""
    return first if first == second else "all"


def _clear_wake_marker(
    root: Path,
    *,
    generation: int,
    idle_chain: int = 0,
    scope: str = "all",
) -> None:
    """Leave an inactive, consumed generation tombstone."""
    task_ref = None if scope == "all" else scope
    _atomic_json_replace(
        root / _WAKE_MARKER_RELATIVE,
        {
            "version": 1,
            "generation": generation,
            "wake_at": None,
            "pid": None,
            "start_token": None,
            "task_ref": task_ref,
            "scope": scope,
            "scope_read": True,
            "scope_complete": False,
            "idle_chain": max(0, idle_chain),
        },
    )


def _close_finished_wake_marker(root: Path, marker: dict[str, Any], *, scope_complete: bool) -> None:
    """Persist a launched waker as consumed; the caller holds the launch lock."""
    marker["wake_at"] = None
    marker["scope_complete"] = bool(scope_complete)
    for field in (
        "pid",
        "start_token",
        "repo_root",
        "pgid",
        "sid",
        "state",
        "launch_successor_scope",
        "launch_successor_wake_at",
    ):
        marker.pop(field, None)
    marker["pid"] = None
    marker["start_token"] = None
    _atomic_json_replace(root / _WAKE_MARKER_RELATIVE, marker)


def _finish_wake_launch(
    root: Path,
    *,
    generation: int,
    scope_complete: bool,
    max_lanes: int,
    budget_s: float,
    await_landing_quiescence_s: float,
    idle_chain: int,
) -> dict[str, Any]:
    """Release a launching waker or replace it with its persisted successor."""
    deadline = _monotonic() + _CHILD_HANDSHAKE_TIMEOUT_S
    launch_lock: TextIO | None = None
    while launch_lock is None:
        launch_lock, error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
        if launch_lock is not None:
            break
        if error != "already_running" or _monotonic() >= deadline:
            return {"ok": False, "outcome": "wake_lock_failed", "error": str(error or "launch_lock_failed")[:300]}
        time.sleep(min(0.05, max(0.0, deadline - _monotonic())))

    try:
        marker = _wake_marker(root)
        if marker is None or marker.get("generation") != generation or marker.get("state") != "launching":
            return {"ok": True, "outcome": "wake_superseded"}

        successor_scope = marker.get("launch_successor_scope")
        if isinstance(successor_scope, str) and successor_scope:
            successor_wake_at = marker.get("launch_successor_wake_at")
            if (
                not isinstance(successor_wake_at, (int, float))
                or isinstance(successor_wake_at, bool)
                or not math.isfinite(float(successor_wake_at))
            ):
                successor_wake_at = time.time() + _SWEEP_WAKE_MIN_DELAY_S
            launch_release_error: str | None = None
            successor_diagnostics: dict[str, str] = {}
            try:
                successor = _schedule_sweep_wake_locked(
                    root,
                    requested_wake_at=float(successor_wake_at),
                    task_ref=None if successor_scope == "all" else successor_scope,
                    max_lanes=max_lanes,
                    budget_s=budget_s,
                    await_landing_quiescence_s=await_landing_quiescence_s,
                    idle_chain=idle_chain,
                    expected_generation=generation,
                    release_launching_generation=generation,
                    diagnostics=successor_diagnostics,
                )
            except Exception as exc:  # noqa: BLE001 — preserve the pending scope and report scheduling errors
                successor = {
                    "ok": False,
                    "outcome": "spawn_failed",
                    "error": f"{type(exc).__name__}: {exc}"[:300],
                }
                failed_marker = _wake_marker(root)
                marker_generation = failed_marker.get("generation") if failed_marker is not None else None
                if isinstance(marker_generation, int) and not isinstance(marker_generation, bool):
                    successor["generation"] = marker_generation
                if (
                    failed_marker is not None
                    and failed_marker.get("generation") == generation
                    and failed_marker.get("state") == "launching"
                ):
                    # A failed first marker write leaves this waker's live
                    # launching generation in place. Release it under the
                    # launch lock so the retry cannot deduplicate against the
                    # process that is about to exit.
                    try:
                        _clear_wake_marker(
                            root,
                            generation=generation,
                            idle_chain=idle_chain,
                            scope=successor_scope,
                        )
                    except Exception as release_exc:  # noqa: BLE001 — keep a failed release typed
                        launch_release_error = f"{type(release_exc).__name__}: {release_exc}"[:300]
            # Every successor outcome, including a failed marker write, keeps the scan diagnostic.
            successor.update(successor_diagnostics)
            if successor.get("outcome") == "retire_admission_starved":
                # A ceiling verdict forbids the successor wake, so this launch closes as if none was pending.
                _close_finished_wake_marker(root, marker, scope_complete=scope_complete)
                return {"ok": True, "outcome": "successor_budget_ceiling", "successor": successor}
            successor_outcome = successor.get("outcome")
            if successor.get("ok") is True and successor_outcome in {
                "scheduled",
                "wake_already_scheduled",
                "wake_scope_broadened",
            }:
                return {"ok": True, "outcome": "successor_scheduled", "successor": successor}
            if successor.get("ok") is True and successor_outcome == "wake_superseded":
                return {"ok": True, "outcome": "wake_superseded", "successor": successor}

            failed_generation = successor.get("generation")
            if not isinstance(failed_generation, int) or isinstance(failed_generation, bool):
                failed_marker = _wake_marker(root)
                marker_generation = failed_marker.get("generation") if failed_marker is not None else None
                failed_generation = (
                    marker_generation
                    if isinstance(marker_generation, int) and not isinstance(marker_generation, bool)
                    else generation
                )

            if launch_release_error is not None:
                retry = {
                    "ok": False,
                    "outcome": "wake_rearm_failed",
                    "rearm_reason": "successor_schedule_failed",
                    "error": launch_release_error,
                }
                return {
                    "ok": False,
                    "outcome": "wake_successor_schedule_failed",
                    "reason": "successor_schedule_failed",
                    "error": launch_release_error,
                    "successor": successor,
                    "retry": retry,
                }

            # The bounded rearm takes the same launch lock through the public
            # scheduler, so release this lock before asking it to preserve the
            # broadened scope in a new generation.
            _release_lock(launch_lock)
            launch_lock = None
            retry = _rearm_sweep_wake(
                root,
                generation=failed_generation,
                task_ref=None if successor_scope == "all" else successor_scope,
                max_lanes=max_lanes,
                budget_s=budget_s,
                await_landing_quiescence_s=await_landing_quiescence_s,
                idle_chain=idle_chain,
                reason="successor_schedule_failed",
            )
            return {
                "ok": False,
                "outcome": "wake_successor_schedule_failed",
                "reason": "successor_schedule_failed",
                "error": str(successor.get("error") or successor_outcome or "successor_schedule_failed")[:300],
                "successor": successor,
                "retry": retry,
            }

        _close_finished_wake_marker(root, marker, scope_complete=scope_complete)
        return {"ok": True, "outcome": "scope_complete" if scope_complete else "launch_finished"}
    finally:
        if launch_lock is not None:
            _release_lock(launch_lock)


def _schedule_sweep_wake_locked(
    root: Path,
    *,
    requested_wake_at: float,
    task_ref: str | None,
    max_lanes: int,
    budget_s: float,
    await_landing_quiescence_s: float,
    idle_chain: int,
    expected_generation: int | None = None,
    release_launching_generation: int | None = None,
    diagnostics: dict[str, str],
) -> dict[str, Any]:
    """Callers merge ``diagnostics`` into every result, including an exception they contain."""
    scan_estimate_s, _scan_source, scan_error = _scan_estimate(root, task_ref)
    if scan_error is not None:
        # Scheduling proceeds on the bootstrap estimate. Record the unreadable history before any
        # marker write, so a write that raises still leaves the caller able to report it.
        diagnostics["scan_cost_unreadable"] = scan_error
    budget_s, ceiling_reason = _resolve_pass_budget_s(budget_s, scan_estimate_s=scan_estimate_s)
    if ceiling_reason is not None:
        # A ceiling verdict forbids wakes. A launching waker's pending successor is dropped by its finish path.
        return _budget_ceiling_refusal(task_ref=task_ref, budget_s=budget_s, reason=ceiling_reason)
    return _schedule_sweep_wake_resolved(
        root,
        requested_wake_at=requested_wake_at,
        task_ref=task_ref,
        max_lanes=max_lanes,
        budget_s=budget_s,
        await_landing_quiescence_s=await_landing_quiescence_s,
        idle_chain=idle_chain,
        expected_generation=expected_generation,
        release_launching_generation=release_launching_generation,
    )


def _schedule_sweep_wake_resolved(
    root: Path,
    *,
    requested_wake_at: float,
    task_ref: str | None,
    max_lanes: int,
    budget_s: float,
    await_landing_quiescence_s: float,
    idle_chain: int,
    expected_generation: int | None = None,
    release_launching_generation: int | None = None,
) -> dict[str, Any]:
    previous = _wake_marker(root)
    previous_generation = previous.get("generation", 0) if previous is not None else 0
    if expected_generation is not None and (previous is None or previous.get("generation") != expected_generation):
        return {"ok": True, "outcome": "wake_superseded"}
    try:
        generation = max(0, int(previous_generation)) + 1
    except (TypeError, ValueError):
        generation = 1
    now = time.time()
    delay_s = min(max(requested_wake_at - now, _SWEEP_WAKE_MIN_DELAY_S), _sweep_wake_max_delay_s())
    wake_at = now + delay_s
    requested_scope = task_ref if isinstance(task_ref, str) and task_ref else "all"

    if previous is not None:
        previous_pid = previous.get("pid")
        previous_wake_at = previous.get("wake_at")
        if isinstance(previous_pid, int) and previous_pid > 0:
            process_state, process_identity = _read_process_identity(previous_pid)
            live = (
                process_state == "available"
                and process_identity is not None
                and _identity_fields_match(
                    process_identity,
                    previous,
                    repo_root=root,
                    pid=previous_pid,
                )
            )
            releasing_launch = (
                release_launching_generation is not None
                and previous.get("generation") == release_launching_generation
                and previous.get("state") == "launching"
            )
            if previous.get("state") == "launching" and not releasing_launch and (live or process_state == "unknown"):
                previous_scope = _wake_marker_scope(previous)
                successor_scope = previous.get("launch_successor_scope")
                effective_scope = previous_scope
                if isinstance(successor_scope, str) and successor_scope:
                    effective_scope = _wake_scope_union(effective_scope, successor_scope)
                if effective_scope == "all" or effective_scope == requested_scope:
                    return {
                        "ok": True,
                        "outcome": "wake_already_scheduled",
                        "wake_at": previous.get("launch_successor_wake_at") or requested_wake_at,
                        "generation": previous.get("generation"),
                    }

                previous["launch_successor_scope"] = _wake_scope_union(effective_scope, requested_scope)
                successor_wake_at = previous.get("launch_successor_wake_at")
                if (
                    not isinstance(successor_wake_at, (int, float))
                    or isinstance(successor_wake_at, bool)
                    or not math.isfinite(float(successor_wake_at))
                ):
                    successor_wake_at = requested_wake_at
                else:
                    successor_wake_at = min(float(successor_wake_at), requested_wake_at)
                previous["launch_successor_wake_at"] = successor_wake_at
                _atomic_json_replace(root / _WAKE_MARKER_RELATIVE, previous)
                return {
                    "ok": True,
                    "outcome": "wake_scope_broadened",
                    "wake_at": successor_wake_at,
                    "generation": previous.get("generation"),
                }
            if (
                live
                and isinstance(previous_wake_at, (int, float))
                and not isinstance(previous_wake_at, bool)
                and math.isfinite(float(previous_wake_at))
                and float(previous_wake_at) <= wake_at + 5.0
            ):
                previous_scope = _wake_marker_scope(previous)
                scope_covers_request = previous_scope == "all" or previous_scope == requested_scope
                malformed_scope = "scope" in previous and (
                    not isinstance(previous.get("scope"), str) or not previous.get("scope")
                )
                if scope_covers_request and not malformed_scope:
                    return {
                        "ok": True,
                        "outcome": "wake_already_scheduled",
                        "wake_at": float(previous_wake_at),
                        "generation": previous.get("generation"),
                    }

                # Before launching, the waker records scope_read under this same
                # lock. A live marker without that state is still pending, so
                # broadening it reaches the waker. A consumed marker takes the
                # launching path above or gets a new generation after completion.
                if previous.get("scope_read") is not True:
                    previous["scope"] = "all"
                    previous["task_ref"] = None
                    previous["scope_read"] = False
                    previous["scope_complete"] = False
                    _atomic_json_replace(root / _WAKE_MARKER_RELATIVE, previous)
                    return {
                        "ok": True,
                        "outcome": "wake_scope_broadened",
                        "wake_at": float(previous_wake_at),
                        "generation": previous.get("generation"),
                    }

    scheduled_scope = requested_scope
    if previous is not None and previous.get("scope_complete") is not True:
        previous_scope = _wake_marker_scope(previous)
        launch_successor_scope = previous.get("launch_successor_scope")
        if isinstance(launch_successor_scope, str) and launch_successor_scope:
            previous_scope = _wake_scope_union(previous_scope, launch_successor_scope)
        scheduled_scope = _wake_scope_union(previous_scope, requested_scope)
        previous_wake_at = previous.get("wake_at")
        if (
            isinstance(previous_wake_at, (int, float))
            and not isinstance(previous_wake_at, bool)
            and math.isfinite(float(previous_wake_at))
        ):
            wake_at = min(wake_at, float(previous_wake_at))
        launch_successor_wake_at = previous.get("launch_successor_wake_at")
        if (
            isinstance(launch_successor_wake_at, (int, float))
            and not isinstance(launch_successor_wake_at, bool)
            and math.isfinite(float(launch_successor_wake_at))
        ):
            wake_at = min(wake_at, float(launch_successor_wake_at))

    scope_broadened = scheduled_scope != requested_scope
    scheduled_task_ref = None if scheduled_scope == "all" else scheduled_scope
    marker: dict[str, Any] = {
        "version": 1,
        "generation": generation,
        "wake_at": wake_at,
        "pid": None,
        "start_token": None,
        "task_ref": None if scheduled_scope == "all" else scheduled_scope,
        "scope": scheduled_scope,
        "scope_read": False,
        "scope_complete": False,
        "idle_chain": max(0, idle_chain),
    }
    _atomic_json_replace(root / _WAKE_MARKER_RELATIVE, marker)

    try:
        _rotate_sweep_log(root)
        log_path = root / _LOG_RELATIVE
        log_path.parent.mkdir(parents=True, exist_ok=True)
        output = log_path.open("ab")
        try:
            child_argv = [
                sys.executable,
                "-m",
                _MODULE,
                "--wake-at",
                str(wake_at),
                "--wake-generation",
                str(generation),
                "--max",
                str(max(0, int(max_lanes))),
                "--budget-s",
                str(max(0.0, float(budget_s))),
                "--retire-timeout-s",
                str(_RETIRE_OPERATION_TIMEOUT_S),
                "--await-landing-quiescence-s",
                str(max(0.0, float(await_landing_quiescence_s))),
                "--wake-idle-chain",
                str(max(0, idle_chain)),
            ]
            if scheduled_task_ref is not None:
                child_argv.extend(["--task-ref", scheduled_task_ref])
            process = _Popen(
                child_argv,
                cwd=root,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        finally:
            output.close()
        pid = getattr(process, "pid", None)
        if not isinstance(pid, int) or pid <= 0:
            raise RuntimeError("waker_pid_unavailable")
        identity_state, identity = _read_process_identity(pid)
        if (
            identity_state != "available"
            or identity is None
            or not _identity_fields_match(identity, identity, repo_root=root, pid=pid)
        ):
            raise RuntimeError("waker_identity_unavailable")
        marker.update(identity)
        _atomic_json_replace(root / _WAKE_MARKER_RELATIVE, marker)
        outcome = "wake_scope_broadened" if scope_broadened else "scheduled"
        return {"ok": True, "outcome": outcome, "wake_at": wake_at, "generation": generation}
    except Exception as exc:  # noqa: BLE001 — wake scheduling must not fail a sweep
        current = _wake_marker(root)
        if current is not None and current.get("generation") == generation:
            _clear_wake_marker(
                root,
                generation=generation,
                idle_chain=idle_chain,
                scope=marker["scope"],
            )
        return {
            "ok": False,
            "outcome": "spawn_failed",
            "error": f"{type(exc).__name__}: {exc}"[:300],
        }


def schedule_sweep_wake(
    repo_root: Path | str,
    *,
    wake_at: float,
    task_ref: str | None = None,
    max_lanes: int = _DEFAULT_MAX_LANES,
    budget_s: float = _DEFAULT_SWEEP_BUDGET_S,
    await_landing_quiescence_s: float = _DEFAULT_LANDING_QUIESCENCE_WAIT_S,
    idle_chain: int = 0,
    _expected_generation: int | None = None,
) -> dict[str, Any]:
    """Schedule one fenced, deduplicated delayed sweep wake."""
    if os.environ.get("WORKBAY_POST_MERGE_RETIRE_SWEEP") == "0":
        return {"ok": True, "outcome": "not_needed", "reason": "opt_out"}
    diagnostics: dict[str, str] = {}
    try:
        requested_wake_at = float(wake_at)
        if not math.isfinite(requested_wake_at):
            raise ValueError("wake_at must be finite")
        root = _configure_runtime(Path(repo_root).expanduser().resolve())
        launch_lock, error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
        if launch_lock is None:
            return {"ok": False, "outcome": "spawn_failed", "error": str(error or "launch_lock_failed")[:300]}
        try:
            outcome = _schedule_sweep_wake_locked(
                root,
                requested_wake_at=requested_wake_at,
                task_ref=task_ref,
                max_lanes=max_lanes,
                budget_s=budget_s,
                await_landing_quiescence_s=await_landing_quiescence_s,
                idle_chain=idle_chain,
                expected_generation=_expected_generation,
                diagnostics=diagnostics,
            )
        finally:
            _release_lock(launch_lock)
        outcome.update(diagnostics)
        return outcome
    except Exception as exc:  # noqa: BLE001 — scheduling errors are typed in the receipt
        # The diagnostic is read before the failing write, so it survives this typed error.
        return {"ok": False, "outcome": "spawn_failed", "error": f"{type(exc).__name__}: {exc}"[:300], **diagnostics}


def _wake_finish_failure_result(
    finished: dict[str, Any],
    *,
    sweep_result: dict[str, Any],
    wake_context: dict[str, Any],
    launch_error: str | None = None,
) -> dict[str, Any]:
    outcome = finished.get("outcome")
    if not isinstance(outcome, str) or not outcome:
        outcome = "wake_finish_failed"
    result: dict[str, Any] = {"ok": False, "outcome": outcome}
    reason = finished.get("reason")
    if isinstance(reason, str) and reason:
        result["reason"] = reason
    elif outcome != "wake_finish_failed":
        result["reason"] = outcome
    for field in ("error", "successor", "retry"):
        if field in finished:
            result[field] = finished[field]
    if launch_error is not None:
        result["launch_error"] = launch_error
    result["sweep"] = sweep_result
    result.update(wake_context)
    return result


def _run_sweep_waker(
    repo_root: Path | str,
    *,
    wake_at: float,
    generation: int,
    task_ref: str | None,
    max_lanes: int,
    budget_s: float,
    await_landing_quiescence_s: float,
    idle_chain: int,
) -> dict[str, Any]:
    """Wait for a current wake generation, then launch a fresh apply sweep."""
    root = Path(repo_root).expanduser().resolve()
    if not math.isfinite(wake_at) or generation < 1:
        return {"ok": False, "outcome": "wake_invalid"}

    deadline = min(wake_at, time.time() + _sweep_wake_max_delay_s())
    while True:
        marker = _wake_marker(root)
        if marker is None or marker.get("generation") != generation:
            return {"ok": True, "outcome": "wake_superseded"}
        if os.environ.get("WORKBAY_POST_MERGE_RETIRE_SWEEP") == "0":
            break
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(_SWEEP_WAKE_POLL_S, remaining))

    lock_deadline = _monotonic() + _CHILD_HANDSHAKE_TIMEOUT_S
    launch_lock: TextIO | None = None
    while launch_lock is None:
        marker = _wake_marker(root)
        if marker is None or marker.get("generation") != generation:
            return {"ok": True, "outcome": "wake_superseded"}
        launch_lock, error = _try_acquire_file_lock(root, _LAUNCH_LOCK_RELATIVE)
        if launch_lock is not None:
            break
        if error != "already_running" or _monotonic() >= lock_deadline:
            return {"ok": False, "outcome": "wake_lock_failed", "error": str(error or "launch_lock_failed")[:300]}
        time.sleep(min(0.05, max(0.0, lock_deadline - _monotonic())))

    try:
        marker = _wake_marker(root)
        if marker is None or marker.get("generation") != generation:
            return {"ok": True, "outcome": "wake_superseded"}
        marker_scope = marker.get("scope")
        if not isinstance(marker_scope, str) or not marker_scope:
            marker_scope = None
        marker_task_ref = marker.get("task_ref")
        if not isinstance(marker_task_ref, str) or not marker_task_ref:
            marker_task_ref = None
        cli_task_ref = task_ref if isinstance(task_ref, str) and task_ref else None

        scope_conflict = marker_scope is not None and marker_task_ref is not None and marker_scope != marker_task_ref
        if marker_scope is not None:
            rearm_scope = marker_scope
            rearm_scope_source = "marker_scope"
            # A legacy task_ref is narrower than an explicit all-scope marker.
            if scope_conflict and marker_scope == "all":
                rearm_scope = marker_task_ref
                rearm_scope_source = "legacy_task_ref"
        elif marker_task_ref is not None:
            rearm_scope = marker_task_ref
            rearm_scope_source = "legacy_task_ref"
        elif cli_task_ref is not None:
            rearm_scope = cli_task_ref
            rearm_scope_source = "cli_task_ref"
        else:
            rearm_scope = "all"
            rearm_scope_source = "default_all"

        rearm_task_ref = None if rearm_scope == "all" else rearm_scope
        sweep_task_ref = rearm_task_ref
        wake_context = {
            "rearm_scope": rearm_scope,
            "rearm_scope_source": rearm_scope_source,
            "scope_conflict": scope_conflict,
        }
        if os.environ.get("WORKBAY_POST_MERGE_RETIRE_SWEEP") == "0":
            _clear_wake_marker(root, generation=generation, idle_chain=idle_chain, scope=rearm_scope)
            return {"ok": True, "outcome": "disabled", **wake_context}
        marker["wake_at"] = None
        marker["task_ref"] = rearm_task_ref
        marker["scope"] = rearm_scope
        marker["scope_read"] = True
        marker["scope_complete"] = False
        marker["state"] = "launching"
        _atomic_json_replace(root / _WAKE_MARKER_RELATIVE, marker)
    finally:
        _release_lock(launch_lock)

    try:
        result = spawn_background_sweep(
            root,
            max_lanes=max_lanes,
            budget_s=budget_s,
            respect_optout=True,
            task_ref=sweep_task_ref,
            _continuation_count=0,
            await_landing_quiescence_s=await_landing_quiescence_s,
            _wake_idle_chain=idle_chain,
            _wake_generation=generation,
        )
    except Exception as exc:  # noqa: BLE001 — a detached waker never performs retirement itself
        finished = _finish_wake_launch(
            root,
            generation=generation,
            scope_complete=False,
            max_lanes=max_lanes,
            budget_s=budget_s,
            await_landing_quiescence_s=await_landing_quiescence_s,
            idle_chain=idle_chain,
        )
        if finished.get("outcome") == "successor_scheduled" and finished.get("ok") is True:
            return {
                "ok": finished.get("ok") is True,
                "outcome": "wake_rearmed",
                "rearmed": "launch_scope_miss",
                "successor": finished.get("successor"),
                **wake_context,
            }
        if finished.get("outcome") != "launch_finished":
            return _wake_finish_failure_result(
                finished,
                sweep_result={"ok": False, "outcome": "spawn_failed"},
                wake_context=wake_context,
                launch_error=f"{type(exc).__name__}: {exc}"[:300],
            )
        rearm = _rearm_sweep_wake(
            root,
            generation=generation,
            task_ref=rearm_task_ref,
            max_lanes=max_lanes,
            budget_s=budget_s,
            await_landing_quiescence_s=await_landing_quiescence_s,
            idle_chain=idle_chain,
            reason="spawn_exception",
        )
        rearm.setdefault("error", f"{type(exc).__name__}: {exc}"[:300])
        rearm.update(wake_context)
        return rearm

    outcome = result.get("outcome")
    if outcome is None and result.get("ok") is True and set(result) == {"ok"}:
        # Keep compatibility with legacy launch adapters that reported only
        # success; the production launcher always returns an explicit outcome.
        outcome = "started"
    if outcome == "started":
        finished = _finish_wake_launch(
            root,
            generation=generation,
            scope_complete=True,
            max_lanes=max_lanes,
            budget_s=budget_s,
            await_landing_quiescence_s=await_landing_quiescence_s,
            idle_chain=idle_chain,
        )
        if finished.get("ok") is not True:
            return _wake_finish_failure_result(finished, sweep_result=result, wake_context=wake_context)
        return {
            "ok": result.get("ok") is True,
            "outcome": "wake_spawned",
            "consumed_by": "started",
            "sweep": result,
            "successor": finished.get("successor"),
            **wake_context,
        }
    if outcome == "already_running":
        owner = _live_sweep_scope_owner(root, task_ref=sweep_task_ref)
        if owner == "live_sweep_covering":
            finished = _finish_wake_launch(
                root,
                generation=generation,
                scope_complete=True,
                max_lanes=max_lanes,
                budget_s=budget_s,
                await_landing_quiescence_s=await_landing_quiescence_s,
                idle_chain=idle_chain,
            )
            if finished.get("ok") is not True:
                return _wake_finish_failure_result(finished, sweep_result=result, wake_context=wake_context)
            return {
                "ok": result.get("ok") is True,
                "outcome": "wake_spawned",
                "consumed_by": "live_sweep_covering",
                "sweep": result,
                "successor": finished.get("successor"),
                **wake_context,
            }
        reason = "already_running_other_scope" if owner == "live_sweep_other_scope" else "already_running_scope_unknown"
    elif outcome == "wake_superseded":
        return {"ok": True, "outcome": "wake_superseded", "sweep": result, **wake_context}
    elif outcome == "retire_admission_starved" and _budget_ceiling_reason(result) is not None:
        # A ceiling verdict forbids the rearm that the starved path below would schedule.
        finished = _finish_wake_launch(
            root,
            generation=generation,
            scope_complete=False,
            max_lanes=max_lanes,
            budget_s=budget_s,
            await_landing_quiescence_s=await_landing_quiescence_s,
            idle_chain=idle_chain,
        )
        if finished.get("ok") is not True:
            return _wake_finish_failure_result(finished, sweep_result=result, wake_context=wake_context)
        return {
            "ok": False,
            "outcome": "budget_ceiling_exceeded",
            "successor": finished.get("successor"),
            "sweep": result,
            **wake_context,
        }
    else:
        reason = "spawn_failed" if outcome == "spawn_failed" else str(outcome or "spawn_failed")

    finished = _finish_wake_launch(
        root,
        generation=generation,
        scope_complete=False,
        max_lanes=max_lanes,
        budget_s=budget_s,
        await_landing_quiescence_s=await_landing_quiescence_s,
        idle_chain=idle_chain,
    )
    if finished.get("outcome") == "successor_scheduled" and finished.get("ok") is True:
        return {
            "ok": finished.get("ok") is True,
            "outcome": "wake_rearmed",
            "rearmed": "launch_scope_miss",
            "successor": finished.get("successor"),
            "sweep": result,
            **wake_context,
        }
    if finished.get("outcome") != "launch_finished":
        return _wake_finish_failure_result(finished, sweep_result=result, wake_context=wake_context)

    rearm = _rearm_sweep_wake(
        root,
        generation=generation,
        task_ref=rearm_task_ref,
        max_lanes=max_lanes,
        budget_s=budget_s,
        await_landing_quiescence_s=await_landing_quiescence_s,
        idle_chain=idle_chain,
        reason=reason,
    )
    rearm["sweep"] = result
    rearm.update(wake_context)
    return rearm


def _summary_made_progress(summary: dict[str, Any]) -> bool:
    retired = summary.get("retired_lane_ids")
    if isinstance(retired, list) and retired:
        return True
    outcome_counts = summary.get("outcome_counts")
    return isinstance(outcome_counts, dict) and any(
        (key.startswith("closed:") or key == "released_no_work") and isinstance(value, int) and value > 0
        for key, value in outcome_counts.items()
    )


def _live_sweep_scope_owner(root: Path, *, task_ref: str | None) -> str:
    """Return whether the verified live child covers this sweep's requested scope."""
    child_state, child_marker = _sweep_child_state(root)
    if child_state != "running" or child_marker is None:
        return "none"
    live_scope = child_marker.get("scope")
    if live_scope == "all" or (task_ref is not None and live_scope == task_ref):
        return "live_sweep_covering"
    # Scope-unknown legacy markers remain live for single-flight purposes, but
    # they cannot prove ownership and must leave a fenced fallback wake.
    return "live_sweep_other_scope"


def _rearm_sweep_wake(
    root: Path,
    *,
    generation: int,
    task_ref: str | None,
    max_lanes: int,
    budget_s: float,
    await_landing_quiescence_s: float,
    idle_chain: int,
    reason: str,
) -> dict[str, Any]:
    """Queue a bounded retry for this wake unless its idle chain is exhausted."""
    next_idle_chain = max(0, idle_chain) + 1
    if next_idle_chain >= _sweep_wake_max_idle_chain():
        return {
            "ok": True,
            "outcome": "wake_chain_exhausted",
            "reason": reason,
            "idle_chain": next_idle_chain,
        }

    try:
        wake = schedule_sweep_wake(
            root,
            wake_at=time.time() + _SWEEP_WAKE_MIN_DELAY_S,
            task_ref=task_ref,
            max_lanes=max_lanes,
            budget_s=budget_s,
            await_landing_quiescence_s=await_landing_quiescence_s,
            idle_chain=next_idle_chain,
            _expected_generation=generation,
        )
    except Exception as exc:  # noqa: BLE001 — the detached waker must report and contain retry failures
        return {
            "ok": False,
            "outcome": "wake_rearm_failed",
            "rearm_reason": reason,
            "error": f"{type(exc).__name__}: {exc}"[:300],
            "idle_chain": next_idle_chain,
        }

    outcome = wake.get("outcome")
    if outcome == "wake_superseded":
        return {"ok": True, "outcome": "wake_superseded", "rearm_reason": reason}
    if outcome == "not_needed":
        return {"ok": True, "outcome": "disabled", "rearm_reason": reason}
    if outcome not in {"scheduled", "wake_already_scheduled", "wake_scope_broadened"}:
        return {
            "ok": False,
            "outcome": "wake_rearm_failed",
            "rearm_reason": reason,
            "error": str(wake.get("error") or outcome or "wake_rearm_failed")[:300],
            "wake": wake,
            "idle_chain": next_idle_chain,
        }
    return {
        "ok": True,
        "outcome": "wake_rearmed",
        "rearmed": reason,
        "wake": wake,
        "idle_chain": next_idle_chain,
    }


def _main(argv: list[str] | None = None) -> int:
    global _RETIRE_OPERATION_TIMEOUT_S

    parser = argparse.ArgumentParser(description="Bounded sweep of terminal lane worktrees")
    parser.add_argument("--apply", action="store_true", help="retire eligible lanes; default is dry-run")
    parser.add_argument("--max", type=int, default=_DEFAULT_MAX_LANES, dest="max_lanes")
    parser.add_argument(
        "--budget-s",
        type=float,
        default=_DEFAULT_SWEEP_BUDGET_S,
        help="requested pass budget; raised to scan estimate plus retire allowance and capped at 600 (see resolved_budget_s)",
    )
    parser.add_argument("--retire-timeout-s", type=float)
    parser.add_argument("--wake-at", type=float)
    parser.add_argument("--wake-generation", type=int)
    parser.add_argument("--wake-idle-chain", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--await-landing-quiescence-s",
        type=float,
        default=0.0,
        help="wait for landing lock quiescence before starting the sweep budget",
    )
    parser.add_argument("--task-ref")
    parser.add_argument("--continuation-count", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    _RETIRE_OPERATION_TIMEOUT_S = (
        _retire_operation_timeout_from_env()
        if args.retire_timeout_s is None
        else _clamp_retire_operation_timeout(args.retire_timeout_s)
    )
    if args.max_lanes < 0:
        parser.error("--max must be non-negative")
    if args.budget_s < 0:
        parser.error("--budget-s must be non-negative")
    if not math.isfinite(args.budget_s):
        parser.error("--budget-s must be finite")
    if args.await_landing_quiescence_s < 0 or not math.isfinite(args.await_landing_quiescence_s):
        parser.error("--await-landing-quiescence-s must be finite and non-negative")
    if args.continuation_count < 0:
        parser.error("--continuation-count must be non-negative")
    if args.wake_generation is not None and args.wake_generation < 1:
        parser.error("--wake-generation must be positive")
    if args.wake_at is not None and not math.isfinite(args.wake_at):
        parser.error("--wake-at must be finite")
    if (args.wake_at is None) != (args.wake_generation is None):
        parser.error("--wake-at and --wake-generation must be supplied together")
    if args.wake_idle_chain is not None and args.wake_idle_chain < 0:
        parser.error("--wake-idle-chain must be non-negative")
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_STATUS_TIMEOUT_S,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        summary = {
            "ok": False,
            "outcome": "instrument_error",
            "instrument_errors": [f"repo_root_failed:{type(exc).__name__}:{exc}"],
        }
        print(json.dumps(summary, sort_keys=True))
        return 2
    if proc.returncode != 0 or not proc.stdout.strip():
        summary = {
            "ok": False,
            "outcome": "instrument_error",
            "instrument_errors": [f"repo_root_failed:{(proc.stderr or proc.stdout).strip()}"],
        }
        print(json.dumps(summary, sort_keys=True))
        return 2
    root = Path(proc.stdout.strip())
    scan_estimate_s, _scan_source, launch_scan_error = _scan_estimate(root, args.task_ref)
    launch_budget_s, _launch_ceiling_reason = _resolve_pass_budget_s(args.budget_s, scan_estimate_s=scan_estimate_s)
    if args.wake_at is not None and args.wake_generation is not None:
        try:
            wake_result = _run_sweep_waker(
                root,
                wake_at=args.wake_at,
                generation=args.wake_generation,
                task_ref=args.task_ref,
                max_lanes=args.max_lanes,
                budget_s=args.budget_s,
                await_landing_quiescence_s=args.await_landing_quiescence_s,
                idle_chain=args.wake_idle_chain or 0,
            )
        except Exception as exc:  # noqa: BLE001 — wake failures stay typed at the entry point
            wake_result = {"ok": False, "outcome": "spawn_failed", "error": f"{type(exc).__name__}: {exc}"[:300]}
        if launch_scan_error is not None:
            wake_result.setdefault("scan_cost_unreadable", launch_scan_error)
        print(json.dumps(wake_result, sort_keys=True))
        return 0 if wake_result.get("ok") is True else 2

    summary = sweep(
        repo_root=root,
        apply=args.apply,
        max_lanes=args.max_lanes,
        budget_s=args.budget_s,
        task_ref=args.task_ref,
        await_landing_quiescence_s=args.await_landing_quiescence_s,
        _continuation_count=args.continuation_count,
        _defer_receipt=True,
    )
    if launch_scan_error is not None:
        summary.setdefault("scan_cost_unreadable", launch_scan_error)
    summary.setdefault("started_at", _utc_now_iso())
    summary["wake"] = {"outcome": "not_needed", "owner": "none"}
    summary["continuation_reason"] = _continuation_decision(
        summary, apply=args.apply, continuation_count=args.continuation_count
    )
    continuation_requested = summary["continuation_reason"] in {
        "progress",
        "contention",
        "cursor_advanced",
        "budget_exhausted",
        "retire_admission_starved",
    }
    continuation_owns_work = False
    wake_owner = "none"
    if continuation_requested:
        continuation_kwargs: dict[str, Any] = {
            "max_lanes": args.max_lanes,
            "budget_s": launch_budget_s,
            "respect_optout": True,
            "_continuation_count": args.continuation_count + 1,
            "await_landing_quiescence_s": args.await_landing_quiescence_s,
        }
        # Resolve again after the pass persisted its scan sample, preserving
        # the headroom already granted to this chain. A fast sample must not
        # shrink its budget to an allowance with no admission-probe headroom.
        # The spawn re-resolves and refuses on a ceiling, so its typed refusal
        # is the single carrier of a verdict found after the pass.
        scan_estimate_s, _scan_source, continuation_scan_error = _scan_estimate(root, args.task_ref)
        continuation_kwargs["budget_s"], _ = _resolve_pass_budget_s(
            launch_budget_s,
            scan_estimate_s=scan_estimate_s,
        )
        if args.task_ref is not None:
            continuation_kwargs["task_ref"] = args.task_ref
        if args.wake_idle_chain is not None:
            continuation_kwargs["_wake_idle_chain"] = 0 if _summary_made_progress(summary) else args.wake_idle_chain
        summary["continuation"] = spawn_background_sweep(root, **continuation_kwargs)
        if continuation_scan_error is not None:
            summary.setdefault("scan_cost_unreadable", continuation_scan_error)
        continuation_outcome = summary["continuation"].get("outcome")
        if continuation_outcome == "started":
            continuation_owns_work = True
            wake_owner = "live_sweep_covering"
        elif continuation_outcome == "already_running":
            wake_owner = _live_sweep_scope_owner(root, task_ref=args.task_ref)
            continuation_owns_work = wake_owner == "live_sweep_covering"
        summary["wake"]["owner"] = wake_owner

    if args.apply and not continuation_owns_work:
        ceiling_exceeded = (
            _budget_ceiling_reason(summary) is not None
            or _budget_ceiling_reason(summary.get("continuation") or {}) is not None
        )
        wake_deadlines = [
            (reason, float(value))
            for reason, value in (
                ("retry", summary.get("next_retry_at")),
                ("grace", summary.get("next_grace_expiry_at")),
            )
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
        ]
        if ceiling_exceeded:
            wake_deadlines.clear()
        elif (
            summary.get("outcome") == "retire_admission_starved"
            and summary.get("resolved_budget_s", launch_budget_s) < _MAX_PASS_BUDGET_S
        ):
            wake_deadlines.append(("starved", time.time() + _SWEEP_WAKE_MIN_DELAY_S))
        if (
            not wake_deadlines
            and not ceiling_exceeded
            and summary.get("outcome") != "retire_admission_starved"
            and (summary.get("has_more") is True or summary.get("outcome") == "budget_exhausted")
        ):
            wake_deadlines.append(("immediate", time.time() + _SWEEP_WAKE_MIN_DELAY_S))
        if wake_deadlines and wake_owner != "live_sweep_covering":
            wake_owner = _live_sweep_scope_owner(root, task_ref=args.task_ref)
            summary["wake"]["owner"] = wake_owner
            if wake_owner == "live_sweep_covering":
                wake_deadlines.clear()
        if wake_deadlines:
            wake_reason, wake_at = min(wake_deadlines, key=lambda item: item[1])
            idle_chain = 0
            if args.wake_idle_chain is not None:
                idle_chain = 0 if _summary_made_progress(summary) else args.wake_idle_chain + 1
                if idle_chain >= _sweep_wake_max_idle_chain():
                    summary["wake"] = {
                        "outcome": "wake_chain_exhausted",
                        "wake_at": wake_at,
                        "idle_chain": idle_chain,
                        "owner": wake_owner,
                    }
            if summary["wake"]["outcome"] != "wake_chain_exhausted":
                scheduler_scan_error: object = None
                try:
                    wake_result = schedule_sweep_wake(
                        root,
                        wake_at=wake_at,
                        task_ref=args.task_ref,
                        max_lanes=args.max_lanes,
                        budget_s=args.budget_s,
                        await_landing_quiescence_s=args.await_landing_quiescence_s,
                        idle_chain=idle_chain,
                    )
                    # The scheduler reads the scan history itself; keep its verdict beside the pass's own.
                    scheduler_scan_error = wake_result.get("scan_cost_unreadable")
                    wake_outcome = wake_result.get("outcome")
                    if wake_outcome == "scheduled":
                        summary["wake"] = {
                            "outcome": "scheduled",
                            "reason": wake_reason,
                            "wake_at": wake_result.get("wake_at"),
                            "generation": wake_result.get("generation"),
                            "idle_chain": idle_chain,
                            "owner": wake_owner,
                        }
                    elif wake_outcome == "wake_already_scheduled":
                        summary["wake"] = {
                            "outcome": "wake_already_scheduled",
                            "reason": wake_reason,
                            "wake_at": wake_result.get("wake_at"),
                            "generation": wake_result.get("generation"),
                            "owner": wake_owner,
                        }
                    elif wake_outcome == "wake_scope_broadened":
                        summary["wake"] = {
                            "outcome": "wake_scope_broadened",
                            "reason": wake_reason,
                            "wake_at": wake_result.get("wake_at"),
                            "generation": wake_result.get("generation"),
                            "owner": wake_owner,
                        }
                    elif wake_outcome == "retire_admission_starved":
                        # A late scheduler ceiling verdict stays typed: no waker is launched, and the
                        # resolved budget and the specific refusal reach the wake JSON and the receipt.
                        summary["wake"] = {
                            "outcome": "retire_admission_starved",
                            "reason": wake_reason,
                            "resolved_budget_s": wake_result.get("resolved_budget_s"),
                            "refusal": _budget_ceiling_reason(wake_result),
                            "could_not_act": wake_result.get("could_not_act"),
                            "owner": wake_owner,
                        }
                    elif wake_outcome == "not_needed":
                        summary["wake"] = {
                            "outcome": "not_needed",
                            **{k: v for k, v in wake_result.items() if k != "ok"},
                            "owner": wake_owner,
                        }
                    else:
                        summary["wake"] = {
                            "outcome": "spawn_failed",
                            "error": str(wake_result.get("error") or wake_outcome or "wake_spawn_failed")[:300],
                            "owner": wake_owner,
                        }
                except Exception as exc:  # noqa: BLE001 — wake scheduling never fails the sweep
                    summary["wake"] = {
                        "outcome": "spawn_failed",
                        "error": f"{type(exc).__name__}: {exc}"[:300],
                        "owner": wake_owner,
                    }
                if isinstance(scheduler_scan_error, str) and scheduler_scan_error:
                    summary["wake"]["scan_cost_unreadable"] = scheduler_scan_error
    try:
        _write_receipt(root, summary)
    except OSError as exc:
        summary["ok"] = False
        summary["outcome"] = "instrument_error"
        summary.setdefault("instrument_errors", []).append(f"receipt_write_failed:{type(exc).__name__}:{exc}")
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(_main())
