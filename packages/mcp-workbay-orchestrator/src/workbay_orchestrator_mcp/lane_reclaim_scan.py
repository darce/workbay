"""Hoisted pure core for terminal-lane reclaim scanning and optional nudging.

Both the legacy ``mcp-workbay-orchestrator lane-reclaim-scan`` command and the
``workbay lane reclaim-scan`` noun command delegate here so validation,
scanning, nudging, and never-raise fault handling have one source of truth
([ARCH-13], [DATA-14], [AGT-21]).
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from workbay_protocol.cli_contract import ExitCode

from .orchestration.lane_reclaim import (
    NudgeFailure,
    nudge_reclaim_candidate,
    scan_terminal_lanes,
)

ReclaimScanOutcome = Literal["ok", "degraded", "validation_error", "fault"]

# Legacy exit code for incomplete scans (OBS-08 / REVHARD-EXIT-CODE-NOT-PINNED-01).
LEGACY_DEGRADED_EXIT = 2

# Pinned equivalence table mapping each core outcome to (legacy_exit, noun_exit).
LEGACY_TO_NOUN_EXIT_CODES: Mapping[ReclaimScanOutcome, tuple[int, ExitCode]] = MappingProxyType(
    {
        "ok": (0, ExitCode.OK),
        "degraded": (LEGACY_DEGRADED_EXIT, ExitCode.DEGRADED),
        "validation_error": (1, ExitCode.USAGE),
        "fault": (1, ExitCode.INTERNAL),
    }
)


@dataclass(frozen=True, slots=True)
class ReclaimScanExecution:
    """Pure result of executing a terminal-lane reclaim scan + optional nudge."""

    outcome: ReclaimScanOutcome
    payload: Mapping[str, Any]
    legacy_lines: tuple[str, ...] = ()
    legacy_error: str | None = None
    legacy_exit_code: int = 0
    noun_exit_code: ExitCode = ExitCode.OK


def _default_progress(rows_done: int, rows_total: int | None = None) -> None:
    total = "?" if rows_total is None else str(rows_total)
    print(f"lane-reclaim-scan progress rows={rows_done}/{total}", file=sys.stderr)


def _default_log(level: str, event: str, **fields: Any) -> None:
    print(json.dumps({"level": level, "event": event, **fields}, default=str), file=sys.stderr)


def _validation_result(code: str, message: str) -> ReclaimScanExecution:
    legacy_exit, noun_exit = LEGACY_TO_NOUN_EXIT_CODES["validation_error"]
    return ReclaimScanExecution(
        outcome="validation_error",
        payload={
            "ok": False,
            "source": "workbay lane reclaim-scan",
            "error": {
                "kind": "usage",
                "code": code,
                "message": message,
            },
        },
        legacy_error=message,
        legacy_exit_code=legacy_exit,
        noun_exit_code=noun_exit,
    )


def run_lane_reclaim_scan(
    *,
    orchestrator_root: Path,
    task_ref: str | None,
    all_tasks: bool,
    lane_id: str | None,
    limit: int | None,
    offset: int,
    scan_fn: Callable[..., Any] = scan_terminal_lanes,
    nudge_fn: Callable[..., Any] = nudge_reclaim_candidate,
    progress: Callable[..., None] | None = _default_progress,
    log: Callable[..., None] | None = _default_log,
) -> ReclaimScanExecution:
    """Validate inputs, run ``scan_fn`` and optional ``nudge_fn``, and never raise.

    Writes nothing to stdout. Diagnostics (``progress`` and ``log``) go to
    stderr by default.
    """
    # Blank/whitespace --task-ref refused before any symbol call (mirror lane-reap).
    if task_ref is not None and task_ref.strip() == "":
        return _validation_result(
            "blank_task_ref",
            (
                "blank_task_ref: --task-ref was provided but empty/whitespace; "
                "an empty scope is not a valid reclaim scan. "
                "Pass a real task_ref."
            ),
        )

    if all_tasks and lane_id is not None:
        return _validation_result(
            "all_tasks_lane_id",
            "all_tasks_lane_id: --lane-id requires a scoped --task-ref scan.",
        )

    # Blank/whitespace --lane-id refused the same way when the flag is present.
    if lane_id is not None and lane_id.strip() == "":
        return _validation_result(
            "blank_lane_id",
            (
                "blank_lane_id: --lane-id was provided but empty/whitespace; "
                "an empty lane_id is not a valid reclaim nudge. "
                "Pass a real lane_id or omit the flag."
            ),
        )

    legacy_lines: list[str] = []
    try:
        scan = scan_fn(
            orchestrator_root=orchestrator_root,
            limit=limit,
            offset=offset,
            progress=progress,
            log=log,
            **({"all_tasks": True} if all_tasks else {"task_ref": task_ref}),
        )

        unchanged_skipped = scan.unchanged_skipped
        legacy_lines.append(f"unchanged_skipped={unchanged_skipped}")

        if not scan.complete:
            reason = scan.refusal_reason if scan.refusal_reason is not None else "scan_incomplete"
            legacy_lines.append(f"refusal_reason={reason}")
            legacy_exit, noun_exit = LEGACY_TO_NOUN_EXIT_CODES["degraded"]
            return ReclaimScanExecution(
                outcome="degraded",
                payload={
                    "ok": True,
                    "degraded": True,
                    "complete": False,
                    "unchanged_skipped": unchanged_skipped,
                    "refusal_reason": reason,
                },
                legacy_lines=tuple(legacy_lines),
                legacy_exit_code=legacy_exit,
                noun_exit_code=noun_exit,
            )

        candidates_count = len(scan)
        legacy_lines.append(f"candidates={candidates_count}")

        failed_lanes = tuple(scan.failed_lanes)
        unrecorded_lanes = tuple(scan.unrecorded_lanes)
        failed_n = len(failed_lanes)
        unrecorded_n = len(unrecorded_lanes)
        if failed_n or unrecorded_n:
            legacy_lines.append(f"failed_lanes={failed_n} unrecorded_lanes={unrecorded_n}")

        candidates_payload: list[dict[str, Any]] = []
        for cand in scan:
            verdict = cand.verdict
            legacy_lines.append(
                f"candidate lane_id={cand.lane_id} "
                f"reclaimable={verdict.reclaimable} "
                f"reason={verdict.reason} "
                f"recorded={cand.recorded}"
            )
            entry: dict[str, Any] = {
                "task_ref": cand.task_ref,
                "lane_id": cand.lane_id,
                "reclaimable": bool(verdict.reclaimable),
                "reason": verdict.reason,
                "recorded": bool(cand.recorded),
            }
            branch_verdict = cand.branch_verdict
            if branch_verdict is not None:
                legacy_lines.append(
                    f"branch_candidate task_ref={cand.task_ref} lane_id={cand.lane_id} "
                    f"reclaimable={branch_verdict.reclaimable} "
                    f"reason={branch_verdict.reason} "
                    f"recorded={cand.branch_recorded}"
                )
                entry["branch"] = {
                    "reclaimable": bool(branch_verdict.reclaimable),
                    "reason": branch_verdict.reason,
                    "recorded": bool(cand.branch_recorded),
                }
            candidates_payload.append(entry)

        nudge_payload: dict[str, Any] | None = None
        if lane_id is not None:
            nudged = nudge_fn(
                orchestrator_root=orchestrator_root,
                task_ref=task_ref,
                lane_id=lane_id,
            )
            if nudged is None:
                legacy_lines.append(f"nudge lane_id={lane_id} not-a-candidate")
                nudge_payload = {
                    "lane_id": lane_id,
                    "status": "not_a_candidate",
                }
            elif isinstance(nudged, NudgeFailure):
                legacy_lines.append(f"nudge lane_id={nudged.lane_id} failure reason={nudged.reason}")
                nudge_payload = {
                    "lane_id": nudged.lane_id,
                    "status": "failure",
                    "reason": nudged.reason,
                }
            else:
                verdict = nudged.verdict
                legacy_lines.append(
                    f"nudge lane_id={nudged.lane_id} "
                    f"reclaimable={verdict.reclaimable} "
                    f"reason={verdict.reason} "
                    f"recorded={nudged.recorded}"
                )
                nudge_payload = {
                    "lane_id": nudged.lane_id,
                    "status": "nudged",
                    "reclaimable": bool(verdict.reclaimable),
                    "reason": verdict.reason,
                    "recorded": bool(nudged.recorded),
                }

        legacy_exit, noun_exit = LEGACY_TO_NOUN_EXIT_CODES["ok"]
        return ReclaimScanExecution(
            outcome="ok",
            payload={
                "ok": True,
                "degraded": False,
                "complete": True,
                "unchanged_skipped": unchanged_skipped,
                "candidates_count": candidates_count,
                "failed_lanes": list(failed_lanes),
                "unrecorded_lanes": list(unrecorded_lanes),
                "candidates": candidates_payload,
                "nudge": nudge_payload,
            },
            legacy_lines=tuple(legacy_lines),
            legacy_exit_code=legacy_exit,
            noun_exit_code=noun_exit,
        )
    except Exception as exc:  # noqa: BLE001 — never-raise surface
        legacy_exit, noun_exit = LEGACY_TO_NOUN_EXIT_CODES["fault"]
        return ReclaimScanExecution(
            outcome="fault",
            payload={
                "ok": False,
                "source": "workbay lane reclaim-scan",
                "error": {
                    "kind": "internal",
                    "type": type(exc).__name__,
                },
            },
            legacy_lines=tuple(legacy_lines),
            legacy_error=f"lane-reclaim-scan error: {exc}",
            legacy_exit_code=legacy_exit,
            noun_exit_code=noun_exit,
        )


__all__ = [
    "LEGACY_DEGRADED_EXIT",
    "LEGACY_TO_NOUN_EXIT_CODES",
    "ReclaimScanExecution",
    "ReclaimScanOutcome",
    "run_lane_reclaim_scan",
]
