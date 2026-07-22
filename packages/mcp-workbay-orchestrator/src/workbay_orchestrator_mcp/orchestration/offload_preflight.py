"""Fail-Fast pre-flight and lane-manifest materialization for offload lanes.

Profile-driven (see offload_profiles): resolves an explicit ``--agent`` to its
:class:`OffloadAgentProfile` and validates against it. Supports ``grok-cli`` and
``codex-subagent``; no fallback between backends.
"""

from __future__ import annotations

import json
import os
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
    REMOTE_OFFLOAD_BACKENDS,
    REMOTE_ONLY_OFFLOAD_BACKEND,
    SECONDS_PER_TURN,
    OffloadPreflightError,
    derive_adapter_timeout_bounds,
    derive_grok_single_cycle_bounds,
    derive_single_cycle_bounds,
    resolve_offload_backend_for_execution_mode,
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

# implementation note S1: worktree-env readiness. A lane whose provisioned ``.venv`` has
# stale/rotted editables (e.g. pointing at a non-suffixed worktree path) hands
# the offload backend a ``python`` that raises ``ModuleNotFoundError`` on
# self-verify → a false ``self_verify_failed`` and, if re-dispatched, a livelock.
# Probe it up-front (warn-default; strict→fail) so the failure names ``uv sync``
# rather than reading as "offload not applicable" ([OBS-08]).
WORKTREE_ENV_UNREADY_WARNING_PREFIX = "worktree env unready:"
WORKTREE_POINTER_DRIFT_WARNING_PREFIX = "worktree pointer drift:"
# The canonical in-tree sibling every workbay lane .venv must resolve. Probing
# ``workbay_protocol.version`` guards the exact failure the 0113 grok grind hit
# (``ModuleNotFoundError: workbay_protocol.version`` from a scrubbed editable).
DEFAULT_WORKTREE_ENV_PROBE_IMPORTS: tuple[str, ...] = ("workbay_protocol", "workbay_protocol.version")

# implementation note S12 / T25: codemap index-freshness gate (warn-only, never blocks).
# Named notes are single-sourced from lane_context_packet ([DATA-14]).


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


def check_worktree_env_readiness(
    worktree_path: Path,
    *,
    strict: bool = False,
    probe_imports: tuple[str, ...] | None = None,
) -> str | None:
    """Probe that the lane's ``.venv`` can import its declared workbay siblings.

    Returns a structured non-fatal warning string when the lane ``.venv`` exists
    but cannot import a probe module (a stale/rotted editable → the backend's
    self-verify would die with ``ModuleNotFoundError``); ``None`` when the import
    succeeds or when there is no lane ``.venv`` to probe (a package-less repo or
    a ``MODE=here`` lane that never provisioned one — degrade silently, mirroring
    the payload-rules freshness check). When *strict* is True and the ``.venv`` is
    unready, raises :class:`OffloadPreflightError` instead of warning.

    implementation note S1. With Plans 0114 (uniform uv env) and 0117 (branch plan-id
    invariant) landed this is an advisory **backstop** — the sibling-rot and
    pointer-drift it defends against are fixed at the root — so it warns by
    default and only fails under an explicit opt-in ``strict`` preflight.
    """
    resolved = worktree_path.expanduser().resolve()
    venv_python = resolved / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return None
    imports = tuple(probe_imports) if probe_imports else DEFAULT_WORKTREE_ENV_PROBE_IMPORTS
    code = "; ".join(f"import {name}" for name in imports)
    try:
        proc = subprocess.run(
            [str(venv_python), "-c", code],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        # Cannot even run the probe (unexecutable shim / timeout) — degrade to a
        # skip rather than block; a genuinely broken interpreter surfaces at the
        # availability probe, not here.
        return None
    if proc.returncode == 0:
        return None
    detail = ""
    if proc.stderr:
        lines = [line for line in proc.stderr.strip().splitlines() if line.strip()]
        detail = lines[-1] if lines else ""
    warning = (
        f"{WORKTREE_ENV_UNREADY_WARNING_PREFIX} lane .venv cannot import required "
        f"workbay siblings ({', '.join(imports)}): {detail or 'import failed'} — "
        f"run `uv sync` in {resolved} before dispatch"
    )
    if strict:
        raise OffloadPreflightError(warning)
    return warning


def check_worktree_pointer_drift(
    worktree_path: Path,
    manifest_worktree_path: str | None,
) -> str | None:
    """Return a warning when the lane manifest's pinned ``worktree_path`` diverges
    from the worktree preflight is actually operating on, else ``None``.

    A co-signal for the implementation note branch/handoff pointer drift: when the manifest
    still points at a stale (e.g. non-``-plan<NNNN>``-suffixed) worktree path, the
    lane ``.venv`` editables it provisioned resolve the wrong tree. Detection only
    — implementation note owns the root fix; here it is surfaced so a stale pointer does not
    read as health ([OBS-08]).
    """
    if not manifest_worktree_path:
        return None
    resolved = worktree_path.expanduser().resolve()
    pinned = Path(manifest_worktree_path).expanduser().resolve()
    if pinned == resolved:
        return None
    return (
        f"{WORKTREE_POINTER_DRIFT_WARNING_PREFIX} lane manifest worktree_path "
        f"{pinned} != preflight worktree {resolved} (co-signal implementation note branch "
        f"plan-id drift) — reconcile the lane pointer before dispatch"
    )


def build_lane_test_cmd(pkg: str, selector: str) -> str:
    """Return the canonical hermetic worktree-venv ``TEST_CMD`` for an offload lane.

    Emits::

        cd packages/<pkg> && HOME=$HOME TMPDIR=/tmp \
            WORKBAY_DISABLE_INVOKING_REPO_TRIPWIRE=1 \
            ../../.venv/bin/python -m pytest <selector> -q

    The ``../../.venv/bin/python`` resolves the *worktree's* provisioned env (not a
    pyenv shim), and the env prefix is attached to the ``python`` invocation — not
    the ``cd`` — because a POSIX leading env assignment binds to a single command.
    The env-key VALUES are single-sourced from the daemon's
    ``HERMETIC_SELF_VERIFY_*`` ([DATA-14]); the daemon idempotently skips any key
    the brief already carries at dispatch. implementation note S2 — the skill uses this to
    hand grok a scoped, hermetic self-verify command instead of relying on the
    operator to hand-assemble it.
    """
    tmpdir = HERMETIC_SELF_VERIFY_TMPDIR
    tripwire = HERMETIC_SELF_VERIFY_TRIPWIRE
    try:  # single-source the hermetic values from the daemon (DATA-14)
        from workbay_orchestrator_mcp.orchestration.worker_daemon import (  # noqa: PLC0415
            HERMETIC_SELF_VERIFY_TMPDIR as _TMPDIR,
        )
        from workbay_orchestrator_mcp.orchestration.worker_daemon import (
            HERMETIC_SELF_VERIFY_TRIPWIRE as _TRIPWIRE,
        )

        tmpdir, tripwire = _TMPDIR, _TRIPWIRE
    except Exception:  # noqa: BLE001 — fall back to the local mirror if the daemon
        pass  # module is not importable (partial checkout / unit isolation)
    env = f"HOME=$HOME TMPDIR={tmpdir} WORKBAY_DISABLE_INVOKING_REPO_TRIPWIRE={tripwire}"
    return f"cd packages/{pkg} && {env} ../../.venv/bin/python -m pytest {selector} -q"


# Local mirror of the daemon hermetic values so ``build_lane_test_cmd`` still
# emits a correct form when worker_daemon is not importable ([OBS-08] no silent
# wrong output). Kept in lockstep with worker_daemon.HERMETIC_SELF_VERIFY_*.
HERMETIC_SELF_VERIFY_TMPDIR = "/tmp"
HERMETIC_SELF_VERIFY_TRIPWIRE = "1"


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
    # harm3: pin preferred_model for both grok-cli and grok-remote (ledger
    # remote_only default) so consumers do not silently drift to adapter defaults.
    if selected_model is None and preferred_backend in (
        GROK_OFFLOAD_BACKEND,
        REMOTE_ONLY_OFFLOAD_BACKEND,
    ):
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
        # Scheduling relation (independent of downstream). Keep in lockstep so
        # save/load round-trips carry depends_on for every materialize path.
        depends_on = manifest.setdefault("depends_on", {})
        if isinstance(depends_on, dict) and lane_id not in depends_on:
            depends_on[lane_id] = []
    else:
        manifest = build_manifest(
            task_ref=task_ref,
            lane_ids=[lane_id],
            lane_overrides={lane_id: pin},
        )
        # build_manifest already emits depends_on={}; ensure key survives any
        # future scaffold change and that this arm matches the exists branch.
        depends_on = manifest.setdefault("depends_on", {})
        if isinstance(depends_on, dict) and lane_id not in depends_on:
            depends_on[lane_id] = []

    return save_manifest(manifest, orchestrator_root=str(root))


def ensure_lane_manifest_for_offload(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: str | Path,
    branch: str | None = None,
    preferred_backend: str | None = None,
    preferred_model: str | None = None,
    preferred_reasoning_effort: str | None = None,
    auto_materialize: bool = True,
) -> dict[str, Any]:
    """Validate lane manifest presence; optionally auto-materialize (implementation note S3 / T3).

    Returns a result dict:
      - ok: bool
      - lane_config: dict | None
      - materialized: bool
      - manifest_path: str | None
      - error: str | None (named cause mentioning materialize_offload_lane_manifest)
    """
    from workbay_orchestrator_mcp.orchestration.bootstrap_lane import (  # noqa: PLC0415
        format_missing_lane_manifest_error,
    )
    from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config  # noqa: PLC0415

    root = Path(orchestrator_root).expanduser().resolve()
    wt = str(Path(worktree_path).expanduser().resolve())
    manifest_file = root / "config" / "lane-orchestration" / f"{task_ref}.json"
    corrupt_reason: str | None = None
    try:
        lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(root))
    except FileNotFoundError:
        lane_cfg = None
    except (RuntimeError, ValueError, json.JSONDecodeError) as exc:
        # S3-A-02: corrupt-but-present manifest (bad JSON / schema rejection) is
        # recoverable like the missing case — auto-materialize instead of raising.
        lane_cfg = None
        corrupt_reason = str(exc)

    if lane_cfg is not None:
        # S2R-3: an already-materialized manifest must not grandfather a local
        # pin past a later remote_only flip (repair --with-remote). Re-check the
        # ledger against the stored pin; refuse typed, never silently rewrite.
        stored_pin = getattr(lane_cfg, "preferred_backend", None) or (
            lane_cfg.get("preferred_backend") if isinstance(lane_cfg, dict) else None
        )
        _, stale_pin_error = resolve_offload_backend_for_execution_mode(
            stored_pin,
            repo_root=wt,
        )
        if stale_pin_error is not None:
            return {
                "ok": False,
                "lane_config": None,
                "materialized": False,
                "manifest_path": str(root / "config" / "lane-orchestration" / f"{task_ref}.json"),
                "error": (
                    f"stale lane-manifest pin under remote_only: {stale_pin_error}; "
                    "re-materialize the lane manifest with preferred_backend=grok-remote"
                ),
                "outcome": "remote_required",
            }
        return {
            "ok": True,
            "lane_config": lane_cfg,
            "materialized": False,
            "manifest_path": str(root / "config" / "lane-orchestration" / f"{task_ref}.json"),
            "error": None,
        }

    named = format_missing_lane_manifest_error(task_ref, lane_id)
    if corrupt_reason:
        named = f"lane manifest for {task_ref} is corrupt ({corrupt_reason}); {named}"
    if not auto_materialize:
        return {
            "ok": False,
            "lane_config": None,
            "materialized": False,
            "manifest_path": None,
            "error": named,
        }

    if corrupt_reason and manifest_file.exists():
        # Quarantine, don't delete ([OBS-04] never information-destroying):
        # materialize would otherwise re-load the corrupt file and raise again.
        quarantine = manifest_file.with_name(f"{task_ref}.json.corrupt")
        try:
            manifest_file.replace(quarantine)
        except OSError as exc:
            return {
                "ok": False,
                "lane_config": None,
                "materialized": False,
                "manifest_path": str(manifest_file),
                "error": f"{named}; corrupt-manifest quarantine failed: {exc}",
            }

    resolved_branch = (branch or "").strip()
    if not resolved_branch:
        # Best-effort branch from the worktree HEAD; materialize requires a branch pin.
        probe = subprocess.run(
            ["git", "-C", wt, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        resolved_branch = (probe.stdout or "").strip()
    if not resolved_branch or resolved_branch == "HEAD":
        return {
            "ok": False,
            "lane_config": None,
            "materialized": False,
            "manifest_path": None,
            "error": named,
        }

    # implementation note S2: third defaulting path for dispatch_lane_work auto-materialize
    # inherits remote_only → grok-remote (and refuses explicit local pins).
    backend, remote_required_error = resolve_offload_backend_for_execution_mode(
        preferred_backend,
        repo_root=wt,
    )
    if remote_required_error is not None:
        return {
            "ok": False,
            "lane_config": None,
            "materialized": False,
            "manifest_path": None,
            "error": remote_required_error,
            "outcome": "remote_required",
        }
    try:
        manifest_path = materialize_offload_lane_manifest(
            orchestrator_root=root,
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=wt,
            branch=resolved_branch,
            preferred_backend=backend,
            preferred_model=preferred_model,
            preferred_reasoning_effort=preferred_reasoning_effort,
        )
    except Exception as exc:  # noqa: BLE001 — surface as named preflight failure
        return {
            "ok": False,
            "lane_config": None,
            "materialized": False,
            "manifest_path": None,
            "error": f"{named}; auto-materialize failed: {exc}",
        }

    try:
        lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(root))
    except FileNotFoundError:
        lane_cfg = None
    if lane_cfg is None:
        return {
            "ok": False,
            "lane_config": None,
            "materialized": True,
            "manifest_path": str(manifest_path),
            "error": named,
        }
    return {
        "ok": True,
        "lane_config": lane_cfg,
        "materialized": True,
        "manifest_path": str(manifest_path),
        "error": None,
    }


def _check_codemap_index_freshness(worktree_path: Path) -> dict[str, Any]:
    """Best-effort codemap index-freshness gate (implementation note S12 / T25).

    Imports :mod:`lane_context_packet` late so unit tests that load this module
    via ``spec_from_file_location`` still work when package imports are partial.
    Never raises: missing CLI or tool failure become typed notes.
    """
    try:
        from workbay_orchestrator_mcp.orchestration.lane_context_packet import (  # noqa: PLC0415
            check_codemap_index_freshness,
        )
    except Exception as exc:  # noqa: BLE001 — degrade typed, never crash preflight
        return {
            "available": False,
            "stale": False,
            "note": f"codemap_unavailable:import_error:{exc}",
            "status": None,
            "detect_changes": None,
            "project": None,
            "cli_path": None,
            "head_sha": None,
        }
    try:
        return check_codemap_index_freshness(worktree_path)
    except Exception as exc:  # noqa: BLE001
        return {
            "available": False,
            "stale": False,
            "note": f"codemap_unavailable:check_error:{exc}",
            "status": None,
            "detect_changes": None,
            "project": None,
            "cli_path": None,
            "head_sha": None,
        }


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

    When the optional codemap CLI is present, also queries ``index_status`` /
    ``detect_changes`` and attaches a non-fatal ``codemap_stale`` warning when
    the index lags the worktree; CLI absent → ``codemap_unavailable`` skip note
    ([OBS-08], implementation note S12 / T25). Never blocks on codemap state.
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

    # implementation note S1 residual: echo execution_mode + remote_probe_state so skills can
    # branch without a second tool call ([API-09] additive). repo_root is the lane
    # worktree — same seam as ensure_lane_manifest_for_offload / materialize.
    from workbay_protocol.bootstrap import load_execution_mode  # noqa: PLC0415

    resolved_worktree = worktree_path.expanduser().resolve()
    execution_mode = load_execution_mode(resolved_worktree)
    _, remote_required_error = resolve_offload_backend_for_execution_mode(
        profile.agent,
        repo_root=resolved_worktree,
    )
    is_remote_agent = profile.agent in REMOTE_OFFLOAD_BACKENDS
    if remote_required_error is not None:
        # Policy refusal before probe/spend; local agents never carry a remote probe.
        return {
            "ok": False,
            "outcome": "remote_required",
            "error": remote_required_error,
            "agent": profile.agent,
            "backend": profile.agent,
            "execution_mode": execution_mode,
            "remote_probe_state": "not_applicable",
            "orchestrator_root": str(orchestrator_root.expanduser().resolve()),
            "worktree_path": str(resolved_worktree),
        }

    # Remote lanes: the local pre-push guard (mirrored from
    # scripts/hooks/check_branch_naming.py) rejects branches outside the admitted
    # set after remote_agent.sh's git push — a non-conforming branch fails AFTER
    # dispatch spend with the cause buried in transport output (width2 dogfood).
    # Run BEFORE probe_availability so a mis-named branch does not pay an SSH
    # round-trip. Worktree existence is checked later; skip when the tree is
    # missing and let that later check raise.
    # Admittance mirrors scripts/hooks/check_branch_naming.py::_is_conforming_or_protected
    # (hook script is not importable from the installed package, hence the mirror):
    # protected names {main, master}, protected prefixes release/|hotfix/ with any
    # suffix, or TASK_REF_RE. Override: WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1.
    branch_gate_warning: str | None = None
    if is_remote_agent and resolved_worktree.exists():
        from workbay_protocol.branch_naming import TASK_REF_RE  # noqa: PLC0415

        # symbolic-ref resolves unborn branches too; detached HEAD fails → skip
        # (the guard targets a NAMED non-conforming branch, the observed miss).
        branch_proc = _git_run(resolved_worktree, "symbolic-ref", "--short", "-q", "HEAD")
        lane_branch = branch_proc.stdout.strip() if branch_proc.returncode == 0 else ""
        if lane_branch:
            admitted = (
                lane_branch in {"main", "master"}
                or lane_branch.startswith(("release/", "hotfix/"))
                or TASK_REF_RE.match(lane_branch) is not None
            )
            if not admitted:
                if os.environ.get("WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH") == "1":
                    branch_gate_warning = (
                        f"remote offload branch {lane_branch!r} is nonconforming under the local "
                        "pre-push guard; WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1 override accepted "
                        "(escape hatch — rename to feature/<slug>-NN when possible)"
                    )
                else:
                    raise OffloadPreflightError(
                        f"remote offload branch {lane_branch!r} does not match the admitted branch "
                        "set (feature/<task-ref>, main/master, release/*, hotfix/*). The local "
                        "pre-push guard (mirrored from scripts/hooks/check_branch_naming.py) would "
                        "reject this branch after dispatch spend — rename the branch "
                        "(git branch -m feature/<slug>-NN) before dispatch, or set "
                        "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1 as the escape hatch."
                    )

    availability = probe_availability(profile.agent)
    # Reuse the probe result already in this flow — no second SSH probe.
    if is_remote_agent:
        remote_probe_state = str(availability.get("state") or "unknown")
    else:
        remote_probe_state = "not_applicable"
    if not availability.get("is_available"):
        detail = availability.get("detail") or "unavailable"
        # implementation note residual R0152-1: attach already-computed capability echo so
        # api.py can surface structured fields on ok:false (no second probe).
        raise OffloadPreflightError(
            f"{profile.agent} backend unavailable: {detail}",
            execution_mode=execution_mode,
            remote_probe_state=remote_probe_state,
        )

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

    if not resolved_worktree.exists():
        raise OffloadPreflightError(f"worktree does not exist: {resolved_worktree}")
    if not _worktree_is_clean(resolved_worktree):
        raise OffloadPreflightError(f"worktree must be clean before offload: {resolved_worktree}")

    # Routed by the profile's declared bound KIND rather than a literal
    # comparison, so a backend bounded by wall-clock alone (cursor-cli: no
    # --max-turns) is recognised as governed instead of falling through to the
    # ungoverned-pass refusal below.
    single_cycle_bounds = derive_single_cycle_bounds(profile.single_cycle_bound, token_budget)

    warnings: list[str] = []
    if branch_gate_warning:
        warnings.append(branch_gate_warning)

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
        # implementation note S1: co-signal a stale lane pointer (manifest worktree_path
        # diverging from the tree preflight runs on) — a companion to the 0117
        # branch plan-id drift. Detection only; never blocks.
        pointer_drift_warning = check_worktree_pointer_drift(
            resolved_worktree,
            (lane_config or {}).get("worktree_path") if isinstance(lane_config, dict) else None,
        )
        if pointer_drift_warning:
            warnings.append(pointer_drift_warning)

    # Lane-branch payload-rules freshness vs primary main (content-hash, not ancestry).
    # Non-fatal by default; strict=True raises OffloadPreflightError on stale files.
    freshness_warning = check_payload_rules_freshness(resolved_worktree, strict=strict)
    if freshness_warning:
        warnings.append(freshness_warning)

    # implementation note S12 / T25: codemap index-freshness gate ([OBS-08] typed+loud).
    # CLI absent → codemap_unavailable skip note; stale → codemap_stale warning.
    # Never blocks dispatch.
    codemap_freshness = _check_codemap_index_freshness(resolved_worktree)
    codemap_note = codemap_freshness.get("note")
    if codemap_note:
        warnings.append(str(codemap_note))

    # implementation note S1: lane .venv sibling-import readiness (advisory backstop; warn
    # by default, strict→fail). No lane .venv → silent skip. Names `uv sync`.
    worktree_env_warning = check_worktree_env_readiness(resolved_worktree, strict=strict)
    if worktree_env_warning:
        warnings.append(worktree_env_warning)

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
        "codemap_freshness": codemap_freshness,
        # implementation note S1 residual: capability echo (same fields on remote_required).
        "execution_mode": execution_mode,
        "remote_probe_state": remote_probe_state,
    }
