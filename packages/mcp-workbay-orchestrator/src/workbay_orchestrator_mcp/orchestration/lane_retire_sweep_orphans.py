"""Bounded orphan-branch arm; all reclaim authority stays in orphan_reclaim."""

from __future__ import annotations

import os
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from . import orphan_reclaim

# Admission floor, not a replacement for the reclaim producer's Git timeouts.
# Avoid starting repository inventory and durable reclaim work on the last tick.
_BUDGET_FLOOR_S = 2.0
_DEFAULT_MAX_BRANCHES = 20


def _result(outcome: str) -> dict[str, Any]:
    return {"outcome": outcome, "summary": {}, "applied": [], "would_apply": [], "deferred": [], "errors": []}


def run_orphan_branch_arm(
    root: Path | str,
    *,
    apply: bool,
    deadline_monotonic: float,
    max_branches: int | None = None,
    integration_ref: str = "main",
) -> dict[str, Any]:
    """Scan once and admit at most the cap's eligible rows to the existing actuator.

    The sweep contains exceptions and copies errors into its instrument channel.
    Neither task scope nor a task's integration branch grants orphan authority.
    """
    if os.environ.get("WORKBAY_RETIRE_SWEEP_ORPHAN_BRANCHES") == "0":
        return _result("skipped_disabled")
    if deadline_monotonic - time.monotonic() < _BUDGET_FLOOR_S:
        return _result("skipped_budget")
    limit = max(
        0,
        int(os.environ.get("WORKBAY_RETIRE_SWEEP_ORPHAN_MAX", str(_DEFAULT_MAX_BRANCHES)))
        if max_branches is None
        else int(max_branches),
    )
    scan = orphan_reclaim.scan_orphan_branches(root, integration_ref=integration_ref)
    result = _result("ran")
    result["summary"] = scan.summary()
    if scan.error is not None:
        result.update(outcome="failed", errors=[scan.error])
        return result

    eligible = [row for row in scan.rows if row.classification == "eligible"]
    # This independent arm owns branch-only cleanup. Worktree-bearing rows
    # stay with the sweep's worktree admission, including its holds and crash
    # recovery. Manual orphan reclaim can still explicitly admit those rows.
    branch_only = [row for row in eligible if row.worktree is None]
    admitted = {row.branch for row in branch_only[:limit]}
    result["deferred"] = [row.branch for row in eligible if row.branch not in admitted]
    # Inventory can use the remaining budget. Never begin a destructive batch
    # when it has left less than the same admission floor.
    if deadline_monotonic - time.monotonic() < _BUDGET_FLOOR_S:
        result.update(outcome="skipped_budget", deferred=[row.branch for row in eligible])
        result["errors"].extend(f"{row.classification}:{row.branch}" for row in scan.unknown_rows)
        return result
    bounded_scan = replace(
        scan,
        rows=tuple(row for row in scan.rows if row.classification != "eligible" or row.branch in admitted),
    )
    reclaim = orphan_reclaim.apply_orphan_reclaim(bounded_scan, apply=apply)
    result.update(
        applied=reclaim["applied"],
        would_apply=reclaim["would_apply"],
        errors=list(reclaim["errors"]),
    )
    if reclaim.get("fence_release"):
        result["errors"].append({"fence_release": reclaim["fence_release"]})
    if not reclaim["ok"] or result["errors"]:
        result["outcome"] = "failed"
    result["errors"].extend(f"{row.classification}:{row.branch}" for row in scan.unknown_rows)
    return result
