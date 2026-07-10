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

# Payload-rules roots compared for lane-branch freshness (internal).
# Verified in-repo: docs/workbay/rules is the consumer overlay (often a
# gitignored symlink onto the payload); packages/workbay-system/**/payload/docs/**
# is the shipped source of truth. Content-hash (git blob SHA), not ancestry.
PAYLOAD_RULES_DOCS_ROOT = "docs/workbay/rules"
PAYLOAD_RULES_PACKAGE_MARKER = "/payload/docs/"
PAYLOAD_RULES_PACKAGE_PREFIX = "packages/workbay-system/"
PAYLOAD_RULES_STALE_WARNING_PREFIX = "lane branch payload-rules content is stale vs primary main tip:"


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


def _git_run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def is_payload_rules_path(path: str) -> bool:
    """True when *path* falls under the payload-rules roots from implementation note S2."""
    normalized = path.replace("\\", "/").lstrip("./")
    if normalized == PAYLOAD_RULES_DOCS_ROOT or normalized.startswith(f"{PAYLOAD_RULES_DOCS_ROOT}/"):
        return True
    if normalized.startswith(PAYLOAD_RULES_PACKAGE_PREFIX) and PAYLOAD_RULES_PACKAGE_MARKER in normalized:
        return True
    return False


def resolve_primary_main_tip(repo: Path) -> str | None:
    """Resolve the primary ``main`` tip SHA from *repo* (linked worktrees share objects).

    Prefer local ``refs/heads/main``, then ``origin/main``. Returns ``None`` when
    no main tip is available (fresh init, shallow clone without main, etc.).
    """
    for candidate in ("refs/heads/main", "main", "refs/remotes/origin/main", "origin/main"):
        result = _git_run(repo, "rev-parse", "--verify", candidate)
        tip = (result.stdout or "").strip()
        if result.returncode == 0 and tip:
            return tip
    return None


def _parse_ls_tree_blobs(stdout: str) -> dict[str, str]:
    """Parse ``git ls-tree -r`` output into ``{path: blob_sha}`` (blobs only)."""
    blobs: dict[str, str] = {}
    for raw_line in (stdout or "").splitlines():
        line = raw_line.rstrip("\n")
        if not line or "\t" not in line:
            continue
        meta, path = line.split("\t", 1)
        parts = meta.split()
        if len(parts) < 3:
            continue
        obj_type, blob_sha = parts[1], parts[2]
        if obj_type != "blob":
            continue
        path = path.replace("\\", "/")
        if is_payload_rules_path(path):
            blobs[path] = blob_sha
    return blobs


def list_payload_rules_blobs(repo: Path, ref: str) -> dict[str, str] | None:
    """Return payload-rules ``{path: content-hash}`` at *ref*, or ``None`` on git IO failure.

    Uses git blob SHAs (content hashes): identical file bytes share a SHA even when
    commit ancestry diverges (duplicate-lineage safe).
    """
    # Bound the walk: first root + packages/workbay-system (filtered by marker).
    result = _git_run(
        repo,
        "ls-tree",
        "-r",
        ref,
        "--",
        PAYLOAD_RULES_DOCS_ROOT,
        "packages/workbay-system",
    )
    if result.returncode != 0:
        return None
    return _parse_ls_tree_blobs(result.stdout or "")


def find_stale_payload_rules_paths(
    *,
    main_blobs: dict[str, str],
    lane_blobs: dict[str, str],
) -> list[str]:
    """Paths on main whose content hash is missing or differs on the lane branch."""
    stale: list[str] = []
    for path, main_sha in main_blobs.items():
        if lane_blobs.get(path) != main_sha:
            stale.append(path)
    return sorted(stale)


def format_payload_rules_stale_warning(stale_paths: list[str]) -> str:
    """Single warnings[] entry that names every stale payload-rules path."""
    named = ", ".join(stale_paths)
    return f"{PAYLOAD_RULES_STALE_WARNING_PREFIX} {named}"


def check_payload_rules_freshness(
    worktree_path: Path,
    *,
    strict: bool = False,
    main_tip: str | None = None,
    lane_ref: str = "HEAD",
) -> str | None:
    """Compare lane payload-rules content hashes against primary main.

    Returns a structured non-fatal warning string when the lane is stale, ``None``
    when clean or when the check cannot run (degraded). When *strict* is True and
    the lane is stale, raises :class:`OffloadPreflightError` instead of warning.
    """
    resolved = worktree_path.expanduser().resolve()
    tip = main_tip if main_tip is not None else resolve_primary_main_tip(resolved)
    if not tip:
        # Degrade silently: empty fixture repos and clones without main must not
        # fail preflight or spam warnings (mirrors optional grants degrade).
        return None

    main_blobs = list_payload_rules_blobs(resolved, tip)
    if main_blobs is None:
        return "payload-rules freshness could not be checked: git ls-tree failed for main tip"
    if not main_blobs:
        return None

    lane_blobs = list_payload_rules_blobs(resolved, lane_ref)
    if lane_blobs is None:
        return f"payload-rules freshness could not be checked: git ls-tree failed for lane ref {lane_ref!r}"

    stale_paths = find_stale_payload_rules_paths(main_blobs=main_blobs, lane_blobs=lane_blobs)
    if not stale_paths:
        return None

    warning = format_payload_rules_stale_warning(stale_paths)
    if strict:
        raise OffloadPreflightError(warning)
    return warning


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
    strict: bool = False,
) -> dict[str, Any]:
    """Fail-Fast checks before spending on an offload dispatch (no fallback).

    Resolves ``agent`` to a typed :class:`OffloadAgentProfile`, then validates
    availability, effort, model policy, worktree cleanliness, and budget. Grok
    lanes get derived ``max_turns``/``timeout`` bounds; codex-subagent is guarded
    by the bridge timeout, so it returns ``single_cycle_bounds=None``.

    When the lane branch's payload-rules content hashes lag primary ``main``,
    appends a structured non-fatal warning naming the stale files. Pass
    ``strict=True`` to fail preflight instead of warning.
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

    # Lane-branch payload-rules freshness vs primary main (content-hash, not ancestry).
    # Non-fatal by default; strict=True raises OffloadPreflightError on stale files.
    freshness_warning = check_payload_rules_freshness(resolved_worktree, strict=strict)
    if freshness_warning:
        warnings.append(freshness_warning)

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
