"""Fail-Fast pre-flight and lane-manifest materialization for offload lanes.

Profile-driven (see offload_profiles): resolves an explicit ``--agent`` to its
:class:`OffloadAgentProfile` and validates against it. Supports ``grok-cli`` and
``codex-subagent``; no fallback between backends.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Callable

# The grok single-cycle-bound constants, the OffloadPreflightError type, and
# derive_grok_single_cycle_bounds now live in offload_profiles (the profile seam).
# They are re-exported here so existing grok callers and tests keep importing them
# from offload_preflight unchanged.
from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: F401
    ESTIMATED_TOKENS_PER_TURN,
    GROK_MAX_TURNS_CAP,
    GROK_OFFLOAD_BACKEND,
    GROK_OFFLOAD_MODEL,
    GROK_TIMEOUT_CAP,
    MIN_TIMEOUT_SECONDS,
    SECONDS_PER_TURN,
    OffloadPreflightError,
    derive_grok_single_cycle_bounds,
)

GRANTS_MISSING_WARNING = (
    "lane manifest lacks a 'grants' block declaring its write surface; "
    "dispatch proceeds this release, but grants will become required in a later release"
)


def manifest_grants_warning(lane_config: dict[str, Any] | None) -> str | None:
    """Warn (never reject) when a lane config lacks a declared ``grants`` block.

    Rejection is deferred one release (expand -> migrate -> contract), so a
    grant-less manifest still dispatches this release with a single warning line.
    """
    if lane_config is None:
        return None
    if isinstance(lane_config.get("grants"), dict):
        return None
    return GRANTS_MISSING_WARNING


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
    preferred_model: str | None = None,
    preferred_reasoning_effort: str | None = None,
) -> Path:
    """Write/patch the lane manifest so review_runner reads the selected backend.

    Always pins ``preferred_backend``. Pins ``preferred_model`` only when known,
    and ``preferred_reasoning_effort`` only when it is a concrete effort
    (``low|medium|high|xhigh``): ``auto|inherit`` are resolved by
    ``_env.resolve_auto_reasoning_effort`` at execution and would be rejected by
    lane-manifest validation if pinned.
    """
    from workbay_orchestrator_mcp.orchestration._env import CODEX_REASONING_EFFORTS
    from workbay_orchestrator_mcp.orchestration.generate_lane_manifest import build_manifest
    from workbay_orchestrator_mcp.orchestration.lane_manifest import load_manifest, save_manifest

    root = orchestrator_root.expanduser().resolve()
    manifest_dir = root / "config" / "lane-orchestration"
    manifest_path = manifest_dir / f"{task_ref}.json"
    pin: dict[str, str] = {
        "preferred_backend": preferred_backend,
        "branch": branch,
        "worktree_path": str(Path(worktree_path).expanduser().resolve()),
    }
    selected_model = preferred_model
    if selected_model is None and preferred_backend == GROK_OFFLOAD_BACKEND:
        selected_model = GROK_OFFLOAD_MODEL
    if selected_model and str(selected_model).strip():
        pin["preferred_model"] = str(selected_model).strip()
    if preferred_reasoning_effort and preferred_reasoning_effort.strip().lower() in CODEX_REASONING_EFFORTS:
        pin["preferred_reasoning_effort"] = preferred_reasoning_effort.strip().lower()

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
    agent: str,
    token_budget: int | None,
    probe_availability: Callable[[str], dict[str, Any]],
    model: str | None = None,
    reasoning_effort: str | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> dict[str, Any]:
    """Fail-Fast checks before spending on an offload dispatch (no fallback).

    Resolves ``agent`` to a typed :class:`OffloadAgentProfile`, then validates
    availability, effort, model policy, worktree cleanliness, and budget. Grok
    lanes get derived ``max_turns``/``timeout`` bounds; codex-subagent is guarded
    by the bridge timeout, so it returns ``single_cycle_bounds=None``.
    """
    from workbay_orchestrator_mcp.orchestration._env import (
        CODEX_REASONING_EFFORTS,
        WORKER_REASONING_EFFORT_CHOICES,
    )
    from workbay_orchestrator_mcp.orchestration.offload_profiles import get_offload_profile

    if token_budget is None or token_budget <= 0:
        raise OffloadPreflightError("token_budget must be set to a positive integer for offload")

    # Resolve the explicit agent to its offload profile. Unknown backend ids raise
    # RuntimeError via validate_backend; surface them as the single offload error.
    try:
        profile = get_offload_profile(agent)
    except OffloadPreflightError:
        raise
    except RuntimeError as exc:
        raise OffloadPreflightError(str(exc)) from exc

    availability = probe_availability(profile.agent)
    if not availability.get("is_available"):
        detail = availability.get("detail") or "unavailable"
        raise OffloadPreflightError(f"{profile.agent} backend unavailable: {detail}")

    normalized_effort = str(reasoning_effort or "").strip().lower()
    if normalized_effort not in WORKER_REASONING_EFFORT_CHOICES:
        raise OffloadPreflightError(
            f"invalid reasoning effort {reasoning_effort!r}; valid values: {', '.join(WORKER_REASONING_EFFORT_CHOICES)}"
        )
    if normalized_effort not in profile.allowed_efforts:
        raise OffloadPreflightError(f"agent {profile.agent!r} does not support effort {normalized_effort!r}")
    # Concrete efforts are pinned into the manifest; auto|inherit are resolved by
    # _env.resolve_auto_reasoning_effort at execution and left unpinned.
    pinned_reasoning_effort = normalized_effort if normalized_effort in CODEX_REASONING_EFFORTS else None

    normalized_model = str(model or "").strip() or None
    if profile.pinned_model is not None:
        if normalized_model is not None and normalized_model != profile.pinned_model:
            raise OffloadPreflightError(f"offload model must be {profile.pinned_model!r}, got {normalized_model!r}")
        selected_model: str | None = profile.pinned_model
    else:
        selected_model = normalized_model

    resolved_worktree = worktree_path.expanduser().resolve()
    if not resolved_worktree.exists():
        raise OffloadPreflightError(f"worktree does not exist: {resolved_worktree}")
    if not _worktree_is_clean(resolved_worktree):
        raise OffloadPreflightError(f"worktree must be clean before offload: {resolved_worktree}")

    single_cycle_bounds = (
        derive_grok_single_cycle_bounds(token_budget) if profile.single_cycle_bound == "grok_derived" else None
    )

    warnings: list[str] = []

    # Token-governance decision, made HERE (fail-fast, before any dispatch/execute)
    # rather than mid-pass (internal / TB-002, TB-004).
    # A backend that emits token telemetry is governed by the token_budget; one
    # that does not (grok-cli) is governed by its derived turn/time bounds and the
    # pass deadline, and the downgrade is surfaced explicitly (no silent caps).
    from workbay_orchestrator_mcp.orchestration.backend_registry import backend_supports_token_telemetry

    if backend_supports_token_telemetry(profile.agent):
        token_governance: dict[str, Any] = {
            "mode": "token_budget",
            "enforced_by": "token_budget",
            "token_telemetry": True,
        }
    else:
        # no-silent-caps: a telemetry-free backend MUST carry derived turn/time
        # bounds; without them the pass would run ungoverned once the token check
        # is skipped. Fail fast instead of dispatching an unbounded pass.
        if not single_cycle_bounds:
            raise OffloadPreflightError(
                f"backend {profile.agent!r} emits no token telemetry and has no derived turn/time "
                "bounds; cannot govern a budgeted offload pass — refusing to dispatch ungoverned."
            )
        note = (
            f"token governance degraded: backend {profile.agent!r} emits no token telemetry; "
            f"token_budget={token_budget} is advisory, pass governed by turn/time bounds "
            f"{single_cycle_bounds}."
        )
        token_governance = {
            "mode": "degraded_turn_time",
            "enforced_by": "turn_time_bounds",
            "token_telemetry": False,
            "bounds": single_cycle_bounds,
            "note": note,
        }
        warnings.append(note)
    if task_ref and lane_id:
        from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config

        try:
            lane_config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
        except FileNotFoundError:
            lane_config = None
        except (json.JSONDecodeError, RuntimeError, OSError) as exc:
            lane_config = None
            warnings.append(f"lane manifest unreadable/invalid; grants could not be checked: {exc}")
        grants_warning = manifest_grants_warning(lane_config)
        if grants_warning:
            warnings.append(grants_warning)

    return {
        "ok": True,
        "agent": profile.agent,
        # Retain the legacy 'backend' key for existing readers.
        "backend": profile.agent,
        "model": selected_model,
        "reasoning_effort": normalized_effort,
        "pinned_reasoning_effort": pinned_reasoning_effort,
        "token_budget": token_budget,
        "single_cycle_bound": profile.single_cycle_bound,
        "single_cycle_bounds": single_cycle_bounds,
        "token_governance": token_governance,
        "orchestrator_root": str(orchestrator_root.expanduser().resolve()),
        "worktree_path": str(resolved_worktree),
        "warnings": warnings,
    }
