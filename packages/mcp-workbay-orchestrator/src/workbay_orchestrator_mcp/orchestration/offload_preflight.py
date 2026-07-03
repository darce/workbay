"""Fail-Fast pre-flight and lane-manifest materialization for grok offload lanes."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Callable

from workbay_orchestrator_mcp.orchestration.grok_lane_config import DEFAULT_GROK_MODEL

GROK_OFFLOAD_MODEL = DEFAULT_GROK_MODEL
GROK_OFFLOAD_BACKEND = "grok-cli"

# Single-cycle bounds (Release It! §5.1): sized from token_budget at the caller.
GROK_MAX_TURNS_CAP = 30
GROK_TIMEOUT_CAP = 900
ESTIMATED_TOKENS_PER_TURN = 4_000
MIN_TIMEOUT_SECONDS = 60
SECONDS_PER_TURN = 30


class OffloadPreflightError(RuntimeError):
    """Distinct Fail-Fast error for offload preconditions."""


def derive_grok_single_cycle_bounds(token_budget: int) -> dict[str, int]:
    """Derive per-invocation grok max_turns/timeout from the lane token budget."""
    if token_budget <= 0:
        raise ValueError("token_budget must be a positive integer")
    max_turns = min(GROK_MAX_TURNS_CAP, max(1, token_budget // ESTIMATED_TOKENS_PER_TURN))
    timeout = min(GROK_TIMEOUT_CAP, max(MIN_TIMEOUT_SECONDS, max_turns * SECONDS_PER_TURN))
    return {"max_turns": max_turns, "timeout": timeout}


def _worktree_is_clean(worktree_path: Path) -> bool:
    result = subprocess.run(
        ["git", "-C", str(worktree_path), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise OffloadPreflightError(f"worktree is not a git repository: {worktree_path}")
    return not (result.stdout or "").strip()


def materialize_offload_lane_manifest(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    branch: str,
    preferred_backend: str = GROK_OFFLOAD_BACKEND,
    preferred_model: str = GROK_OFFLOAD_MODEL,
) -> Path:
    """Write/patch the lane manifest so review_runner reads preferred_backend."""
    from workbay_orchestrator_mcp.orchestration.generate_lane_manifest import build_manifest
    from workbay_orchestrator_mcp.orchestration.lane_manifest import load_manifest, save_manifest

    root = orchestrator_root.expanduser().resolve()
    manifest_dir = root / "config" / "lane-orchestration"
    manifest_path = manifest_dir / f"{task_ref}.json"
    pin = {
        "preferred_backend": preferred_backend,
        "preferred_model": preferred_model,
        "branch": branch,
        "worktree_path": str(Path(worktree_path).expanduser().resolve()),
    }

    if manifest_path.exists():
        manifest = load_manifest(task_ref, orchestrator_root=str(root))
        lanes = manifest.setdefault("lanes", {})
        if not isinstance(lanes, dict):
            raise OffloadPreflightError(f"lane manifest lanes must be an object: {manifest_path}")
        lane = lanes.get(lane_id)
        if isinstance(lane, dict):
            lane.update(pin)
        else:
            scaffold = build_manifest(
                task_ref=task_ref,
                lane_ids=[lane_id],
                lane_overrides={lane_id: pin},
            )
            lanes[lane_id] = scaffold["lanes"][lane_id]
        if lane_id not in manifest.get("merge_order", []):
            merge_order = manifest.setdefault("merge_order", [])
            if isinstance(merge_order, list) and lane_id not in merge_order:
                merge_order.append(lane_id)
        downstream = manifest.setdefault("downstream", {})
        if isinstance(downstream, dict) and lane_id not in downstream:
            downstream[lane_id] = []
    else:
        manifest = build_manifest(
            task_ref=task_ref,
            lane_ids=[lane_id],
            lane_overrides={lane_id: pin},
        )

    return save_manifest(manifest, orchestrator_root=str(root))


def offload_preflight(
    *,
    orchestrator_root: Path,
    worktree_path: Path,
    model: str,
    token_budget: int | None,
    probe_availability: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    """Fail-Fast checks before spending on a grok offload dispatch."""
    if token_budget is None or token_budget <= 0:
        raise OffloadPreflightError("token_budget must be set to a positive integer for offload")

    availability = probe_availability(GROK_OFFLOAD_BACKEND)
    if not availability.get("is_available"):
        detail = availability.get("detail") or "unavailable"
        raise OffloadPreflightError(f"grok-cli backend unavailable: {detail}")

    normalized_model = str(model or "").strip()
    if normalized_model != GROK_OFFLOAD_MODEL:
        raise OffloadPreflightError(
            f"offload model must be {GROK_OFFLOAD_MODEL!r}, got {normalized_model!r}"
        )

    resolved_worktree = worktree_path.expanduser().resolve()
    if not resolved_worktree.exists():
        raise OffloadPreflightError(f"worktree does not exist: {resolved_worktree}")
    if not _worktree_is_clean(resolved_worktree):
        raise OffloadPreflightError(f"worktree must be clean before offload: {resolved_worktree}")

    single_cycle_bounds = derive_grok_single_cycle_bounds(token_budget)
    return {
        "ok": True,
        "backend": GROK_OFFLOAD_BACKEND,
        "model": GROK_OFFLOAD_MODEL,
        "token_budget": token_budget,
        "single_cycle_bounds": single_cycle_bounds,
        "orchestrator_root": str(orchestrator_root.expanduser().resolve()),
        "worktree_path": str(resolved_worktree),
    }