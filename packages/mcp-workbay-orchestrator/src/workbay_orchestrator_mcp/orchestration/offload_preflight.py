"""Fail-Fast pre-flight and lane-manifest materialization for offload lanes.

Profile-driven (see offload_profiles): resolves an explicit ``--agent`` to its
:class:`OffloadAgentProfile` and validates against it. Supports ``grok-cli`` and
``codex-subagent``; no fallback between backends.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Callable

from workbay_orchestrator_mcp.orchestration import disk_probe
from workbay_orchestrator_mcp.orchestration.host_resources import (
    _GATED_COST_CLASSES,
    COST_HEAVY,
    COST_REMOTE,
    HostMemoryPolicy,
    load_host_memory_policy,
)

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
    resolve_adapter_timeout_cap,
    resolve_offload_backend_for_execution_mode,
)
from workbay_orchestrator_mcp.orchestration.probe_deadline import bounded_probe

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
    path_guard = HERMETIC_SELF_VERIFY_PATH_GUARD
    try:  # single-source the hermetic values from the daemon (DATA-14)
        from workbay_orchestrator_mcp.orchestration.worker_daemon import (  # noqa: PLC0415
            HERMETIC_SELF_VERIFY_PATH_GUARD as _PATH_GUARD,
        )
        from workbay_orchestrator_mcp.orchestration.worker_daemon import (
            HERMETIC_SELF_VERIFY_TMPDIR as _TMPDIR,
        )
        from workbay_orchestrator_mcp.orchestration.worker_daemon import (
            HERMETIC_SELF_VERIFY_TRIPWIRE as _TRIPWIRE,
        )

        tmpdir, tripwire, path_guard = _TMPDIR, _TRIPWIRE, _PATH_GUARD
    except Exception:  # noqa: BLE001 — fall back to the local mirror if the daemon
        pass  # module is not importable (partial checkout / unit isolation)
    # WORKBAY_DISABLE_PYTEST_PATH_GUARD rides the self-verify cmd unconditionally
    # (decision 4885): pytest runs from the linked worktree, so workbay-system's
    # path-guard would otherwise hard-fail the session on cross-worktree loads.
    env = (
        f"HOME=$HOME TMPDIR={tmpdir} "
        f"WORKBAY_DISABLE_INVOKING_REPO_TRIPWIRE={tripwire} "
        f"WORKBAY_DISABLE_PYTEST_PATH_GUARD={path_guard}"
    )
    return f"cd packages/{pkg} && {env} ../../.venv/bin/python -m pytest {selector} -q"


# Local mirror of the daemon hermetic values so ``build_lane_test_cmd`` still
# emits a correct form when worker_daemon is not importable ([OBS-08] no silent
# wrong output). Kept in lockstep with worker_daemon.HERMETIC_SELF_VERIFY_*.
HERMETIC_SELF_VERIFY_TMPDIR = "/tmp"
DISK_HEADROOM_ENV = "WORKBAY_DISK_HEADROOM_BYTES"


def resolve_disk_floor_bytes(
    path: Path | str,
    *,
    environ: Any = os.environ,
    policy: HostMemoryPolicy | None = None,
) -> int:
    """Return the shared staging disk floor in bytes (env override, else policy)."""
    configured_floor = environ.get(DISK_HEADROOM_ENV)
    if configured_floor is None:
        effective_policy = policy or load_host_memory_policy(Path(path))
        return int(effective_policy.local_staging_disk_floor_gib * 1024**3)
    try:
        floor_bytes = int(configured_floor)
    except (TypeError, ValueError) as exc:
        raise OffloadPreflightError(
            f"invalid {DISK_HEADROOM_ENV}={configured_floor!r}; expected a positive byte count",
            outcome="disk_headroom_refused",
        ) from exc
    if floor_bytes <= 0:
        raise OffloadPreflightError(
            f"invalid {DISK_HEADROOM_ENV}={configured_floor!r}; expected a positive byte count",
            outcome="disk_headroom_refused",
        )
    return floor_bytes


def _disk_headroom_probe_volumes(
    path: Path | str,
    cost_class: str,
    *,
    tmpdir: Path | str,
    staging_root: Path | str | None,
) -> tuple[Path, ...]:
    """Select volumes to probe: gated classes use path+tmpdir; COST_REMOTE uses the spool."""
    if cost_class in _GATED_COST_CLASSES:
        return (Path(path), Path(tmpdir))
    if cost_class == COST_REMOTE:
        spool = Path(path) if staging_root is None else Path(staging_root)
        return (spool,)
    return ()


def check_disk_headroom(
    path: Path | str,
    cost_class: str = COST_HEAVY,
    *,
    tmpdir: Path | str = HERMETIC_SELF_VERIFY_TMPDIR,
    environ: Any = os.environ,
    stat: Callable[[Path | str], Any] = os.stat,
    statvfs: Callable[[Path | str], Any] | None = None,
    policy: HostMemoryPolicy | None = None,
    staging_root: Path | str | None = None,
) -> None:
    """Refuse local dispatch when the cost-class volumes lack free disk space."""
    volumes = _disk_headroom_probe_volumes(path, cost_class, tmpdir=tmpdir, staging_root=staging_root)
    if not volumes:
        return

    statvfs_probe = disk_probe.DEFAULT_STATVFS if statvfs is None else statvfs
    floor_bytes = resolve_disk_floor_bytes(path, environ=environ, policy=policy)

    seen_devices: set[int] = set()
    for volume in volumes:
        try:
            device = stat(volume).st_dev
        except FileNotFoundError:
            # The existing worktree validation owns the clearer typed error for
            # a missing path. Keep this predicate early without masking it.
            continue
        if device in seen_devices:
            continue
        seen_devices.add(device)
        try:
            filesystem = statvfs_probe(volume)
        except OSError as exc:
            raise OffloadPreflightError(
                f"disk headroom probe failed for volume {volume}: {exc}",
                outcome="disk_headroom_refused",
            ) from exc
        free_bytes = filesystem.f_bavail * filesystem.f_frsize
        if free_bytes < floor_bytes:
            raise OffloadPreflightError(
                f"disk headroom refused: volume {volume} has {free_bytes} free bytes, "
                f"below floor {floor_bytes}; run "
                "remote_exec_staging_reaper.reap_remote_exec_staging to reclaim staging space",
                outcome="disk_headroom_refused",
            )


HERMETIC_SELF_VERIFY_TRIPWIRE = "1"
HERMETIC_SELF_VERIFY_PATH_GUARD = "1"

# Manifest materialization must not turn an unknown write set into a guessed
# path.  Keep this note stable so callers can distinguish unavailable ownership
# from an explicit path set (including intentional review-twin emptiness).
OWNED_PATHS_NOTE_TYPE = "write_set_unavailable"
OWNED_PATHS_NOTE_KEY = "owned_paths_note"
OWNED_PATHS_SCOPE_STATE_KEY = "scope_state"
ABSENT_SCOPE_STATE = "absent"
DECLARED_SCOPE_STATE = "declared"
UNKNOWN_SCOPE_REASON = "unknown_scope"
SEMANTIC_MANIFEST_INVALID_REASON = "manifest_semantic_invalid"
DISPATCH_REFUSED_OUTCOME = "dispatch_refused"
REVIEW_TWIN_MARKER = "__verify__"
_WRITE_SET_DIFF_TIMEOUT_SECONDS = 30


def _coerce_write_set(
    value: Any,
    *,
    source: str,
    allow_repo_root: bool = False,
) -> tuple[list[str] | None, str | None]:
    """Validate one manifest write-set source without inventing paths."""
    if not isinstance(value, list):
        return None, f"{source}_not_a_list"
    paths: list[str] = []
    for entry in value:
        if not isinstance(entry, str) or (not entry.strip() and not allow_repo_root):
            return None, f"{source}_contains_invalid_path"
        paths.append(entry.strip())
    return list(dict.fromkeys(paths)), None


def _validate_owned_paths(paths: list[str], *, lane_manifest: Any) -> tuple[list[str] | None, str | None]:
    """Validate path spelling through the manifest's collision contract."""
    normalized: list[str] = []
    for path in paths:
        try:
            lane_manifest._normalize_owned_path(path)
        except (TypeError, ValueError):
            return None, "path_is_not_repo_relative"
        normalized.append(path)
    return list(dict.fromkeys(normalized)), None


def _owned_paths_note(
    lane_id: str,
    reason: str,
    *,
    source: str | None = None,
    scope_state: str | None = None,
) -> dict[str, str]:
    """Build the stable note persisted when a lane's write set is unavailable."""
    note = {
        "type": OWNED_PATHS_NOTE_TYPE,
        "lane_id": lane_id,
        "reason": reason,
    }
    if source:
        note["source"] = source
    if scope_state:
        note[OWNED_PATHS_SCOPE_STATE_KEY] = scope_state
    return note


def _is_review_twin(lane_id: str, lane: dict[str, Any]) -> bool:
    """Return whether an empty scope is intentional for this lane.

    Compiler-emitted review twins carry the reserved ``__verify__`` namespace in
    their manifest id.  ``lane_kind=review`` is also accepted for callers that
    materialize a review lane without going through the compiler.
    """
    return REVIEW_TWIN_MARKER in lane_id or str(lane.get("lane_kind") or "").strip().lower() == "review"


def _validated_source_paths(
    paths: list[str],
    *,
    lane_id: str,
    source: str,
    lane_manifest: Any,
) -> tuple[list[str], dict[str, str] | None]:
    """Validate paths from one source and convert failures into a typed note."""
    valid, error = _validate_owned_paths(paths, lane_manifest=lane_manifest)
    if error is None and valid is not None:
        return valid, None
    return [], _owned_paths_note(
        lane_id,
        error or "write_set_unavailable",
        source=source,
        scope_state=DECLARED_SCOPE_STATE,
    )


def _structured_write_set(lane: dict[str, Any]) -> tuple[list[str] | None, str | None, str | None]:
    """Read an explicit structured write set, if a lane provides one.

    A prose objective is intentionally not parsed.  A mapping-valued objective
    is accepted only for compatibility with callers that carry a structured
    ``write_set`` alongside the human-readable objective.
    """
    for key in ("write_set", "write-set"):
        if key in lane:
            paths, error = _coerce_write_set(lane.get(key), source=key.replace("-", "_"))
            if error is not None:
                return None, error, key
            return paths, None, key

    objective = lane.get("objective")
    if isinstance(objective, dict):
        for key in ("write_set", "write-set", "owned_paths", "paths"):
            if key not in objective:
                continue
            paths, error = _coerce_write_set(objective.get(key), source=f"objective_{key.replace('-', '_')}")
            if error is not None:
                return None, error, f"objective.{key}"
            return paths, None, f"objective.{key}"
    return [], None, None


def _read_current_owned_paths(lane: dict[str, Any]) -> tuple[list[str] | None, str | None]:
    """Read the materialized ``owned_paths`` field when it is non-empty."""
    if "owned_paths" not in lane:
        return None, None
    current_owned = lane.get("owned_paths")
    if isinstance(current_owned, list) and not current_owned:
        return None, None
    return _coerce_write_set(current_owned, source="owned_paths", allow_repo_root=True)


def _read_declared_write_paths(lane: dict[str, Any]) -> tuple[list[str] | None, str | None, str | None]:
    """Read legacy commit/tooling path declarations in their precedence order."""
    declared: list[str] = []
    declared_sources: list[str] = []
    for key in ("commit_paths", "tooling_paths"):
        if key not in lane:
            continue
        paths, error = _coerce_write_set(lane.get(key), source=key)
        if error is not None:
            return None, error, key
        if paths:
            declared.extend(paths)
            declared_sources.append(key)
    if not declared:
        return None, None, None
    return list(dict.fromkeys(declared)), None, "+".join(declared_sources)


def _derive_explicit_owned_paths(
    lane_id: str,
    lane: dict[str, Any],
    *,
    lane_manifest: Any,
) -> tuple[list[str] | None, dict[str, str] | None]:
    """Resolve explicit scope sources before consulting Git."""
    current, current_error = _read_current_owned_paths(lane)
    if current_error is not None:
        return [], _owned_paths_note(
            lane_id,
            current_error,
            source="owned_paths",
            scope_state=DECLARED_SCOPE_STATE,
        )
    if current:
        return _validated_source_paths(
            current,
            lane_id=lane_id,
            source="owned_paths",
            lane_manifest=lane_manifest,
        )

    structured, structured_error, structured_source = _structured_write_set(lane)
    if structured_error is not None:
        return [], _owned_paths_note(
            lane_id,
            structured_error,
            source=structured_source or "structured_write_set",
            scope_state=DECLARED_SCOPE_STATE,
        )
    if structured_source is not None:
        if not structured:
            return [], None
        return _validated_source_paths(
            structured,
            lane_id=lane_id,
            source=structured_source or "structured_write_set",
            lane_manifest=lane_manifest,
        )

    declared, declared_error, declared_source = _read_declared_write_paths(lane)
    if declared_error is not None:
        return [], _owned_paths_note(
            lane_id,
            declared_error,
            source=declared_source,
            scope_state=DECLARED_SCOPE_STATE,
        )
    if declared:
        return _validated_source_paths(
            declared,
            lane_id=lane_id,
            source=declared_source or "declared_write_set",
            lane_manifest=lane_manifest,
        )
    return None, None


def _branch_ref_has_invalid_chars(branch: str) -> bool:
    """Return whether a branch contains Git-forbidden characters."""
    forbidden = set(" ~^:?*[\\")
    return any(char in forbidden or ord(char) < 0x20 or ord(char) == 0x7F for char in branch)


def _branch_ref_has_invalid_components(branch: str) -> bool:
    """Return whether slash-separated branch components violate ref rules."""
    if ".." in branch or "@{" in branch or "//" in branch or branch.endswith("/"):
        return True
    if branch.endswith(".") or branch.endswith(".lock"):
        return True
    return any(
        not component or component.startswith(".") or component.endswith(".lock") for component in branch.split("/")
    )


def _canonical_branch_ref(branch: Any) -> tuple[str | None, str | None]:
    """Validate a short branch name and return its unambiguous full ref.

    This mirrors Git's refname rules for the branch alphabet that can arrive in
    a manifest.  Keeping validation local means an option-like value is rejected
    before any subprocess is started; the returned ``refs/heads/`` prefix also
    prevents a valid-but-hostile branch component from becoming a Git option.
    """
    if not isinstance(branch, str) or not branch:
        return None, "branch_reference_missing"
    if branch != branch.strip() or branch.startswith("-"):
        return None, "branch_reference_invalid"
    if branch.startswith("refs/") or branch in {".", "..", "@"}:
        return None, "branch_reference_invalid"
    if _branch_ref_has_invalid_chars(branch) or _branch_ref_has_invalid_components(branch):
        return None, "branch_reference_invalid"
    return f"refs/heads/{branch}", None


def _probe_base_ancestor(*, repo: Path, base: str, tip: str) -> str | None:
    """Return a typed ancestry-probe failure, or ``None`` when ancestry holds."""
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "merge-base",
                "--is-ancestor",
                "--end-of-options",
                base,
                tip,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=_WRITE_SET_DIFF_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "base_sha_probe_failed"
    if result.returncode == 0:
        return None
    if result.returncode == 1:
        return "base_not_ancestor"
    return "base_sha_unavailable"


def _validated_base_sha(base_sha: Any) -> str | None:
    """Return a full lowercase commit SHA, or ``None`` when it is malformed."""
    if not isinstance(base_sha, str) or len(base_sha.strip()) != 40:
        return None
    base = base_sha.strip()
    return base if all(char in "0123456789abcdef" for char in base) else None


def _branch_diff_candidate(
    *,
    repo: Path,
    base: str,
    tip: str,
) -> tuple[list[str] | None, str | None]:
    """Probe ancestry and diff one repository/tip candidate."""
    if not repo.is_dir():
        return None, "branch_repository_missing"
    ancestry_error = _probe_base_ancestor(repo=repo, base=base, tip=tip)
    if ancestry_error is not None:
        return None, ancestry_error
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "diff",
                "--name-only",
                "--no-renames",
                "-z",
                "--end-of-options",
                base,
                tip,
                "--",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            check=False,
            timeout=_WRITE_SET_DIFF_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, "branch_diff_probe_failed"
    if result.returncode != 0:
        return None, "branch_diff_unavailable"
    return [path for path in (result.stdout or "").split("\0") if path], None


def _git_diff_write_set(
    *,
    base_sha: Any,
    worktree_path: Any,
    branch: Any,
    orchestrator_root: Path,
) -> tuple[list[str] | None, str | None]:
    """Return a branch's changed paths, or a typed reason when unavailable."""
    base = _validated_base_sha(base_sha)
    if base is None:
        return None, "base_sha_missing_or_invalid"

    canonical_branch, branch_error = _canonical_branch_ref(branch) if branch is not None else (None, None)
    if branch_error is not None and branch_error != "branch_reference_missing":
        return None, branch_error

    candidates: list[tuple[Path, str]] = []
    if isinstance(worktree_path, str) and worktree_path.strip():
        worktree = Path(worktree_path.strip().replace("{orchestrator_root}", str(orchestrator_root))).expanduser()
        if not worktree.is_absolute():
            worktree = orchestrator_root / worktree
        candidates.append((worktree.resolve(), "HEAD"))
    if canonical_branch is not None:
        candidates.append((orchestrator_root, canonical_branch))

    if not candidates:
        return None, "branch_reference_missing"

    last_reason = "branch_diff_unavailable"
    for repo, tip in candidates:
        paths, candidate_error = _branch_diff_candidate(repo=repo, base=base, tip=tip)
        if paths is not None:
            return paths, None
        last_reason = candidate_error or last_reason
    return None, last_reason


def _derive_lane_owned_paths(
    lane_id: str,
    lane: dict[str, Any],
    *,
    lane_manifest: Any,
    orchestrator_root: Path,
) -> tuple[list[str], dict[str, str] | None]:
    """Derive one lane's scope using only explicit, observable sources."""
    if _is_review_twin(lane_id, lane) and lane.get("owned_paths") == []:
        return [], None

    explicit, note = _derive_explicit_owned_paths(
        lane_id,
        lane,
        lane_manifest=lane_manifest,
    )
    if explicit is not None:
        return explicit, note

    branch_paths, branch_error = _git_diff_write_set(
        base_sha=lane.get("base_sha"),
        worktree_path=lane.get("worktree_path"),
        branch=lane.get("branch"),
        orchestrator_root=orchestrator_root,
    )
    if branch_paths is not None:
        if not branch_paths:
            branch_error = "branch_diff_empty"
        else:
            return _validated_source_paths(
                branch_paths,
                lane_id=lane_id,
                source="branch_diff",
                lane_manifest=lane_manifest,
            )
    return [], _owned_paths_note(
        lane_id,
        branch_error or "write_set_unavailable",
        source="branch_diff",
        scope_state=ABSENT_SCOPE_STATE,
    )


def _ordered_lane_ids(manifest: dict[str, Any], lanes: dict[str, Any]) -> list[str]:
    """Return merge-order lanes followed by deterministic stragglers."""
    ordered = [lane_id for lane_id in manifest.get("merge_order", []) if lane_id in lanes]
    ordered.extend(sorted(lane_id for lane_id in lanes if lane_id not in ordered))
    return ordered


def _materialize_lane_scope(
    lane_id: str,
    lane: dict[str, Any],
    *,
    lane_manifest: Any,
    orchestrator_root: Path,
) -> None:
    """Materialize one lane's paths and retain a typed unavailable note."""
    owned_paths, note = _derive_lane_owned_paths(
        lane_id,
        lane,
        lane_manifest=lane_manifest,
        orchestrator_root=orchestrator_root,
    )
    lane["owned_paths"] = owned_paths
    if note is None:
        lane.pop(OWNED_PATHS_NOTE_KEY, None)
    else:
        lane[OWNED_PATHS_NOTE_KEY] = note


def _ensure_dependency_entries(manifest: dict[str, Any], ordered_lanes: list[str]) -> dict[str, list[str]]:
    """Normalize the dependency map for every materialized lane."""
    depends_on = manifest.setdefault("depends_on", {})
    if not isinstance(depends_on, dict):
        depends_on = {}
        manifest["depends_on"] = depends_on
    for lane_id in ordered_lanes:
        raw_prereqs = depends_on.get(lane_id)
        depends_on[lane_id] = list(raw_prereqs) if isinstance(raw_prereqs, list) else []
    return depends_on


def _normalized_lane_roots(
    lanes: dict[str, Any],
    ordered_lanes: list[str],
    *,
    lane_manifest: Any,
) -> dict[str, list[str]]:
    """Build normalized roots for lanes with an established write set."""
    roots: dict[str, list[str]] = {}
    for lane_id in ordered_lanes:
        lane = lanes.get(lane_id)
        if not isinstance(lane, dict):
            continue
        lane_roots = [
            lane_manifest._normalize_owned_path(raw_path)
            for raw_path in lane.get("owned_paths", [])
            if isinstance(raw_path, str)
        ]
        roots[lane_id] = lane_roots
    return roots


def _roots_overlap(left_roots: list[str], right_roots: list[str], *, lane_manifest: Any) -> bool:
    """Return whether any root pair collides under the manifest contract."""
    return any(
        lane_manifest._owned_path_roots_overlap(left_root, right_root)
        for left_root in left_roots
        for right_root in right_roots
    )


def _derive_overlap_dependencies(
    ordered_lanes: list[str],
    roots: dict[str, list[str]],
    depends_on: dict[str, list[str]],
    *,
    lane_manifest: Any,
) -> None:
    """Append merge-order prerequisites for every overlapping lane pair."""
    for left_index, left_id in enumerate(ordered_lanes):
        for right_id in ordered_lanes[left_index + 1 :]:
            if not _roots_overlap(roots.get(left_id, []), roots.get(right_id, []), lane_manifest=lane_manifest):
                continue
            prereqs = depends_on[right_id]
            if left_id not in prereqs:
                prereqs.append(left_id)


def _materialize_owned_paths_and_dependencies(
    manifest: dict[str, Any],
    *,
    lane_manifest: Any,
    orchestrator_root: Path,
) -> None:
    """Populate missing lane scope and merge-order conflict prerequisites."""
    lanes = manifest.get("lanes")
    if not isinstance(lanes, dict):
        return

    ordered_lanes = _ordered_lane_ids(manifest, lanes)
    for lane_id in ordered_lanes:
        lane = lanes.get(lane_id)
        if not isinstance(lane, dict):
            continue
        _materialize_lane_scope(
            lane_id,
            lane,
            lane_manifest=lane_manifest,
            orchestrator_root=orchestrator_root,
        )
    depends_on = _ensure_dependency_entries(manifest, ordered_lanes)
    roots = _normalized_lane_roots(lanes, ordered_lanes, lane_manifest=lane_manifest)
    _derive_overlap_dependencies(
        ordered_lanes,
        roots,
        depends_on,
        lane_manifest=lane_manifest,
    )


def _scope_declaration_present(lane: dict[str, Any]) -> bool:
    """Return whether a lane carries an explicit write-set declaration."""
    for key in ("write_set", "write-set"):
        if key in lane:
            return True

    objective = lane.get("objective")
    if isinstance(objective, dict) and any(
        key in objective for key in ("write_set", "write-set", "owned_paths", "paths")
    ):
        return True

    for key in ("commit_paths", "tooling_paths"):
        if key not in lane:
            continue
        value = lane.get(key)
        if not isinstance(value, list) or value:
            return True

    note = lane.get(OWNED_PATHS_NOTE_KEY)
    if not isinstance(note, dict):
        return False
    if note.get(OWNED_PATHS_SCOPE_STATE_KEY) == ABSENT_SCOPE_STATE:
        return False
    # Notes from the first materializer release did not carry scope_state, but
    # branch_diff was only emitted for the legacy absent-source fallback.
    if note.get("source") == "branch_diff":
        return False
    return True


def _scope_is_legacy_absent(lane: dict[str, Any]) -> bool:
    """Return whether empty ownership represents an absent legacy scope."""
    if "owned_paths" not in lane:
        return True
    owned_paths = lane.get("owned_paths")
    return isinstance(owned_paths, list) and not owned_paths and not _scope_declaration_present(lane)


def _scope_needs_materialization(lane: dict[str, Any]) -> bool:
    """Return whether an absent scope still needs a derivation attempt."""
    if not _scope_is_legacy_absent(lane):
        return False
    note = lane.get(OWNED_PATHS_NOTE_KEY)
    return not (isinstance(note, dict) and note.get(OWNED_PATHS_SCOPE_STATE_KEY) == ABSENT_SCOPE_STATE)


def lane_scope_dispatch_error(lane_id: str, lane: dict[str, Any]) -> str | None:
    """Return a fail-closed error for an ordinary lane with unknown scope.

    A missing write-set declaration is a legacy manifest state.  It remains
    dispatchable after materialization records the typed derivation note, while
    a malformed declaration (or a legacy note without an absent-state marker)
    remains fail-closed.  Empty ``owned_paths`` alone is not enough to classify
    the state because generated legacy manifests use it as a placeholder.
    """
    if _is_review_twin(lane_id, lane):
        return None

    if _scope_is_legacy_absent(lane):
        return None

    owned_paths = lane.get("owned_paths")
    if isinstance(owned_paths, list) and owned_paths:
        return None
    note = lane.get(OWNED_PATHS_NOTE_KEY)
    note_reason = note.get("reason") if isinstance(note, dict) else "owned_paths_empty"
    return (
        f"lane {lane_id!r} has unknown write scope ({note_reason}); refusing dispatch until "
        "owned_paths is materialized. Empty ownership is reserved for intentional review twins."
    )


def _lane_dispatch_refusal(lane_id: str, lane: dict[str, Any]) -> tuple[str, str] | None:
    """Return a typed dispatch refusal for invalid branch or unknown scope."""
    _, branch_error = _canonical_branch_ref(lane.get("branch"))
    if branch_error is not None:
        return "branch_reference_invalid", f"lane {lane_id!r} has an invalid branch reference"
    scope_error = lane_scope_dispatch_error(lane_id, lane)
    if scope_error is not None:
        return UNKNOWN_SCOPE_REASON, scope_error
    return None


_SCOPE_DECLARATION_KEYS = ("owned_paths", "write_set", "commit_paths")


def _raw_lane_scope_declaration_error(manifest_file: Path, lane_id: str) -> str | None:
    """Return a typed error when a lane declares a scope key that is not a list.

    Read from the raw manifest so that a schema rejection is never mistaken for
    a corrupt file. A malformed declaration is an operator statement we cannot
    interpret, not missing data, so it fails closed instead of being recovered.
    """
    try:
        raw = json.loads(manifest_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    lanes = raw.get("lanes") if isinstance(raw, dict) else None
    lane = lanes.get(lane_id) if isinstance(lanes, dict) else None
    if not isinstance(lane, dict):
        return None
    malformed = [key for key in _SCOPE_DECLARATION_KEYS if key in lane and not isinstance(lane[key], list)]
    if not malformed:
        return None
    causes = ", ".join(f"{key}_not_a_list" for key in malformed)
    return (
        f"lane {lane_id!r} declares a malformed write scope ({causes}); refusing dispatch. "
        "Repair the declaration in the lane manifest; it is never replaced by an empty lane."
    )


def _dispatch_refusal_result(
    *,
    lane_id: str,
    manifest_path: Path | str | None,
    error: str,
    reason: str,
    materialized: bool,
) -> dict[str, Any]:
    """Shape one typed preflight refusal for all dispatch callers."""
    return {
        "ok": False,
        "lane_id": lane_id,
        "lane_config": None,
        "materialized": materialized,
        "manifest_path": str(manifest_path) if manifest_path is not None else None,
        "error": error,
        "outcome": DISPATCH_REFUSED_OUTCOME,
        "reason": reason,
    }


def _propagate_review_subject_to_existing_row(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    branch: str,
    row: dict[str, Any],
) -> None:
    """Persist a derived review_subject onto an existing remote review row (REF-26)."""
    if not row:
        return
    from workbay_orchestrator_mcp.orchestration.secure_sandbox import (  # noqa: PLC0415
        REVIEW_CONTEXT_PAYLOAD_BACKENDS,
    )

    if str(row.get("lane_kind") or "") != "review":
        return
    backend = str(row.get("backend") or "").strip()
    if backend not in REVIEW_CONTEXT_PAYLOAD_BACKENDS:
        return
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config  # noqa: PLC0415

    lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root)) or {}
    subject = lane_cfg.get("review_subject") if isinstance(lane_cfg, dict) else None
    kwargs: dict[str, Any] = {
        "operation": "upsert",
        "task_ref": task_ref,
        "lane_id": lane_id,
        "worktree_path": str(row.get("worktree_path") or worktree_path),
        "branch": str(row.get("branch") or branch),
        "backend": backend,
        "lane_kind": "review",
    }
    if isinstance(subject, dict):
        base_ref = subject.get("base_ref") or subject.get("base_sha")
        tip_ref = subject.get("tip_ref") or subject.get("tip_sha")
        if isinstance(base_ref, str) and base_ref.strip():
            kwargs["review_base_ref"] = base_ref.strip()
        if isinstance(tip_ref, str) and tip_ref.strip():
            kwargs["review_tip_ref"] = tip_ref.strip()
    result = manage_worktree_lane(**kwargs)
    if isinstance(result, dict) and result.get("ok") is False:
        raise OffloadPreflightError(str(result.get("error") or "review_subject propagation failed"))


def _materialize_legacy_lane_scope(
    *,
    lane_cfg: dict[str, Any],
    root: Path,
    worktree: str,
    task_ref: str,
    lane_id: str,
    branch: str | None,
    preferred_backend: str | None,
    preferred_model: str | None,
    preferred_reasoning_effort: str | None,
    preferred_speed: str | None,
) -> Path | None:
    """Refresh an existing legacy lane's absent scope when a branch is usable."""
    existing_branch = (branch or str(lane_cfg.get("branch") or "")).strip()
    if not existing_branch or existing_branch == "HEAD":
        return None
    stored_backend = lane_cfg.get("preferred_backend")
    existing_backend = preferred_backend or (
        stored_backend if isinstance(stored_backend, str) and stored_backend.strip() else None
    )
    return materialize_offload_lane_manifest(
        orchestrator_root=root,
        task_ref=task_ref,
        lane_id=lane_id,
        worktree_path=worktree,
        branch=existing_branch,
        preferred_backend=existing_backend or GROK_OFFLOAD_BACKEND,
        preferred_model=preferred_model,
        preferred_reasoning_effort=preferred_reasoning_effort,
        preferred_speed=preferred_speed,
    )


def materialize_offload_lane_manifest(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    branch: str,
    preferred_backend: str | None = None,
    preferred_model: str | None = None,
    preferred_reasoning_effort: str | None = None,
    preferred_speed: str | None = None,
    preferred_tier: str | None = None,
) -> Path:
    """Write/patch the lane manifest so review_runner reads the selected backend.

    Pins ``preferred_backend`` unless it was explicitly cleared. Pins
    ``preferred_model`` only when known,
    and ``preferred_reasoning_effort`` only when it is a concrete worker effort:
    ``auto|inherit`` are resolved by
    ``_env.resolve_auto_reasoning_effort`` at execution and would be rejected by
    lane-manifest validation if pinned.
    """
    import subprocess  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration import lane_manifest as lane_manifest_module
    from workbay_orchestrator_mcp.orchestration._env import WORKER_REASONING_EFFORT_CHOICES
    from workbay_orchestrator_mcp.orchestration.generate_lane_manifest import build_manifest
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (
        atomic_update_manifest,
        save_manifest,
    )

    root = orchestrator_root.expanduser().resolve()
    manifest_dir = root / "config" / "lane-orchestration"
    manifest_path = manifest_dir / f"{task_ref}.json"
    resolved_worktree = str(Path(worktree_path).expanduser().resolve())
    row_routing: dict[str, Any] = {}
    try:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        listed = manage_worktree_lane(operation="list", task_ref=task_ref, status="all", limit=10_000)
        rows = listed.get("lanes") if isinstance(listed, dict) else None
        row_routing = next(
            (row for row in rows or [] if isinstance(row, dict) and row.get("lane_id") == lane_id),
            {},
        )
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        # Materialization is also used before handoff state exists. In that
        # bootstrap shape, profile/default resolution remains the fallback.
        row_routing = {}
    backend_unsupplied = preferred_backend is None
    model_unsupplied = preferred_model is None
    effort_unsupplied = preferred_reasoning_effort is None
    speed_unsupplied = preferred_speed is None
    tier_unsupplied = preferred_tier is None
    clear_preferred_backend = preferred_backend == ""
    clear_preferred_model = preferred_model == ""
    clear_preferred_effort = preferred_reasoning_effort == ""
    clear_preferred_speed = preferred_speed == ""
    clear_preferred_tier = preferred_tier == ""
    effective_backend = (
        str(row_routing.get("backend") or GROK_OFFLOAD_BACKEND)
        if preferred_backend is None or clear_preferred_backend
        else preferred_backend
    )
    preferred_backend = effective_backend
    if preferred_model is None:
        preferred_model = row_routing.get("model")
    if preferred_reasoning_effort is None:
        preferred_reasoning_effort = row_routing.get("reasoning_effort")
    if preferred_speed is None:
        preferred_speed = row_routing.get("speed")
    if preferred_tier is None:
        preferred_tier = row_routing.get("tier")
    # main's branch guard runs after the routing quad is resolved but before the
    # pin is built, so an invalid ref still refuses instead of being recorded.
    _, branch_error = _canonical_branch_ref(branch)
    if branch_error is not None:
        raise OffloadPreflightError(
            f"lane {lane_id!r} branch reference is invalid: {branch!r}",
            outcome=DISPATCH_REFUSED_OUTCOME,
        )
    branch = branch.strip()
    pin: dict[str, str] = {"branch": branch, "worktree_path": resolved_worktree}
    if not clear_preferred_backend:
        pin["preferred_backend"] = preferred_backend
    selected_model = preferred_model
    normalized_tier = str(preferred_tier or "").strip().lower()
    if preferred_tier is not None and preferred_tier != "":
        if normalized_tier not in {"senior", "junior"}:
            raise OffloadPreflightError("preferred_tier must be one of ('senior', 'junior')")
        if effective_backend != "codex-remote":
            raise OffloadPreflightError("preferred_tier is codex-remote only")
        from workbay_orchestrator_mcp.orchestration.codex_lane_config import CODEX_MODEL_TIERS  # noqa: PLC0415

        tier_rows = [row for row in CODEX_MODEL_TIERS.values() if row.tier == normalized_tier and row.entitled]
        if not tier_rows:
            raise OffloadPreflightError(f"preferred_tier {normalized_tier!r} has no entitled model")
        selected_model = selected_model or tier_rows[0].slug
        preferred_reasoning_effort = preferred_reasoning_effort or tier_rows[0].default_effort
        preferred_speed = "standard" if preferred_speed is None else preferred_speed
        pin["preferred_tier"] = normalized_tier
    # Pin preferred_model for every offload profile that declares one (implementation note
    # M2/M3). Cursor used to materialize harness-only (backend, no model) —
    # that is the pin-21 RED baseline. The resolved pin now travels with the
    # backend identity.
    if selected_model is None:
        try:
            from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
                get_offload_profile,
            )

            profile = get_offload_profile(effective_backend)
        except (OffloadPreflightError, RuntimeError):
            profile = None
        if profile is not None:
            selected_model = profile.pinned_model or profile.default_model
        elif effective_backend in (
            GROK_OFFLOAD_BACKEND,
            REMOTE_ONLY_OFFLOAD_BACKEND,
        ):
            selected_model = GROK_OFFLOAD_MODEL
    if not clear_preferred_model and selected_model and str(selected_model).strip():
        pin["preferred_model"] = str(selected_model).strip()
    normalized_preferred_effort = str(preferred_reasoning_effort or "").strip().lower()
    if (
        not clear_preferred_effort
        and normalized_preferred_effort in WORKER_REASONING_EFFORT_CHOICES
        and normalized_preferred_effort
        not in {
            "auto",
            "inherit",
        }
    ):
        pin["preferred_reasoning_effort"] = normalized_preferred_effort
    # Three states are intentional: ``None`` means omitted/preserve, ``""``
    # explicitly clears an existing pin, and a concrete value sets it.
    if preferred_speed is not None and not clear_preferred_speed:
        from workbay_orchestrator_mcp.orchestration.backend_spec import normalize_operator_speed  # noqa: PLC0415
        from workbay_orchestrator_mcp.orchestration.offload_profiles import get_offload_profile  # noqa: PLC0415

        try:
            normalized_preferred_speed = normalize_operator_speed(preferred_speed)
            profile = get_offload_profile(effective_backend)
        except (ValueError, RuntimeError) as exc:
            raise OffloadPreflightError(str(exc)) from exc
        if normalized_preferred_speed not in profile.allowed_speeds:
            raise OffloadPreflightError(f"{effective_backend} does not advertise speed {normalized_preferred_speed!r}")
        pin["preferred_speed"] = normalized_preferred_speed

    # Existing lanes treat omission/null as preservation. Fresh lanes still
    # receive the resolved bootstrap/profile defaults assembled above.
    existing_lane_pin = dict(pin)
    for unsupplied, field in (
        (backend_unsupplied, "preferred_backend"),
        (model_unsupplied, "preferred_model"),
        (effort_unsupplied, "preferred_reasoning_effort"),
        (speed_unsupplied, "preferred_speed"),
        (tier_unsupplied, "preferred_tier"),
    ):
        if unsupplied:
            existing_lane_pin.pop(field, None)

    # implementation note S2: pin base_sha from the worktree HEAD at materialize time.
    base_sha: str | None = None
    try:
        head = subprocess.run(
            ["git", "-C", resolved_worktree, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        candidate = (head.stdout or "").strip()
        if head.returncode == 0 and len(candidate) == 40 and all(c in "0123456789abcdef" for c in candidate):
            base_sha = candidate
    except OSError:
        base_sha = None

    def _apply_pin(manifest: dict) -> None:
        lanes = manifest.setdefault("lanes", {})
        if not isinstance(lanes, dict):
            raise OffloadPreflightError(f"lane manifest lanes must be an object: {manifest_path}")
        lane = lanes.get(lane_id)
        if isinstance(lane, dict):
            if row_routing:
                from workbay_orchestrator_mcp.orchestration.codex_lane_config import (  # noqa: PLC0415
                    CODEX_MODEL_TIERS,
                )
                from workbay_orchestrator_mcp.orchestration.lane_routing import (  # noqa: PLC0415
                    RoutingDisagreement,
                    resolve_routing_quad,
                )

                try:
                    resolve_routing_quad(
                        row=row_routing,
                        manifest_entry=lane,
                        tier_table=CODEX_MODEL_TIERS,
                        profile=pin,
                    )
                except RoutingDisagreement as exc:
                    raise OffloadPreflightError(str(exc), outcome="routing_refused") from exc
            lane.update(existing_lane_pin)
            if clear_preferred_backend:
                lane.pop("preferred_backend", None)
            if clear_preferred_model:
                lane.pop("preferred_model", None)
            if clear_preferred_effort:
                lane.pop("preferred_reasoning_effort", None)
            if clear_preferred_speed:
                lane.pop("preferred_speed", None)
            if clear_preferred_tier:
                lane.pop("preferred_tier", None)
            # base_sha is deliberately NOT written here. The block below pins it
            # only after scope derivation, because the previously pinned base is
            # the only safe baseline for the branch-diff fallback -- overwriting
            # it with the current HEAD first makes ``base..HEAD`` empty and
            # silently yields ``owned_paths: []`` for a lane that did change files.
        else:
            scaffold = build_manifest(
                task_ref=task_ref,
                lane_ids=[lane_id],
                lane_overrides={lane_id: pin},
            )
            new_lane = scaffold["lanes"][lane_id]
            if base_sha is not None:
                new_lane["base_sha"] = base_sha
            lanes[lane_id] = new_lane
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

        # Derive scope before replacing an existing base_sha.  A previously
        # pinned base is the only safe baseline for the branch-diff fallback.
        _materialize_owned_paths_and_dependencies(
            manifest,
            lane_manifest=lane_manifest_module,
            orchestrator_root=root,
        )
        if base_sha is not None:
            target_lane = lanes.get(lane_id)
            if isinstance(target_lane, dict):
                target_lane["base_sha"] = base_sha

    if manifest_path.exists():
        # Existing-manifest RMW under flock (row 30 / three-writer discipline).
        try:
            atomic_update_manifest(manifest_path, _apply_pin)
        except RuntimeError as exc:
            raise OffloadPreflightError(str(exc)) from exc
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
        _materialize_owned_paths_and_dependencies(
            manifest,
            lane_manifest=lane_manifest_module,
            orchestrator_root=root,
        )
        if base_sha is not None:
            lanes = manifest.get("lanes")
            if isinstance(lanes, dict) and isinstance(lanes.get(lane_id), dict):
                lanes[lane_id]["base_sha"] = base_sha

        try:
            save_manifest(manifest, orchestrator_root=str(root))
        except RuntimeError as exc:
            raise OffloadPreflightError(str(exc)) from exc

    _propagate_review_subject_to_existing_row(
        orchestrator_root=root,
        task_ref=task_ref,
        lane_id=lane_id,
        worktree_path=resolved_worktree,
        branch=branch,
        row=row_routing,
    )
    return manifest_path


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
    preferred_speed: str | None = None,
    auto_materialize: bool = True,
) -> dict[str, Any]:
    """Validate lane manifest presence; optionally auto-materialize (implementation note S3 / T3).

    Returns a result dict:
      - ok: bool
      - lane_config: dict | None
      - materialized: bool
      - manifest_path: str | None
      - error: str | None (named cause mentioning materialize_offload_lane_manifest)
      - outcome/reason: typed dispatch refusal fields when scope or branch safety
        cannot be established
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
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # S3-A-02: only syntactically unreadable or undecodable JSON is
        # recoverable by quarantine + auto-materialization. Both parser errors
        # subclass ValueError, so this arm must stay before the semantic arm.
        lane_cfg = None
        corrupt_reason = str(exc)
    except (RuntimeError, ValueError) as exc:
        scope_declaration_error = _raw_lane_scope_declaration_error(manifest_file, lane_id)
        if scope_declaration_error is not None:
            return _dispatch_refusal_result(
                lane_id=lane_id,
                manifest_path=manifest_file,
                error=scope_declaration_error,
                reason=UNKNOWN_SCOPE_REASON,
                materialized=False,
            )
        # A parseable manifest that fails schema or semantic validation still
        # contains the sibling lanes. Refuse only this dispatch and leave the
        # bytes in place for operator repair; replacing the manifest with a
        # one-lane materialization would discard the active sibling schedule.
        return _dispatch_refusal_result(
            lane_id=lane_id,
            manifest_path=manifest_file,
            error=(
                f"lane manifest for {task_ref} failed semantic validation "
                "(semantically invalid; not corrupt JSON); "
                "repair the manifest before dispatch; "
                f"materialize_offload_lane_manifest is not attempted: {exc}"
            ),
            reason=SEMANTIC_MANIFEST_INVALID_REASON,
            materialized=False,
        )

    if lane_cfg is not None:
        materialized_existing = False
        if auto_materialize and _scope_needs_materialization(lane_cfg):
            try:
                refreshed_path = _materialize_legacy_lane_scope(
                    lane_cfg=lane_cfg,
                    root=root,
                    worktree=wt,
                    task_ref=task_ref,
                    lane_id=lane_id,
                    branch=branch,
                    preferred_backend=preferred_backend,
                    preferred_model=preferred_model,
                    preferred_reasoning_effort=preferred_reasoning_effort,
                    preferred_speed=preferred_speed,
                )
                if refreshed_path is not None:
                    manifest_path = refreshed_path
                    lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(root))
                    materialized_existing = lane_cfg is not None
            except OffloadPreflightError as exc:
                if exc.outcome == DISPATCH_REFUSED_OUTCOME:
                    return _dispatch_refusal_result(
                        lane_id=lane_id,
                        manifest_path=manifest_file,
                        error=str(exc),
                        reason="branch_reference_invalid",
                        materialized=False,
                    )
                return {
                    "ok": False,
                    "lane_config": None,
                    "materialized": False,
                    "manifest_path": str(manifest_file),
                    "error": f"legacy scope materialization failed: {exc}",
                    "outcome": exc.outcome or "error",
                }
            except Exception as exc:  # noqa: BLE001 — surface named preflight failure
                return {
                    "ok": False,
                    "lane_config": None,
                    "materialized": False,
                    "manifest_path": str(manifest_file),
                    "error": f"legacy scope materialization failed: {exc}",
                }
        refusal = _lane_dispatch_refusal(lane_id, lane_cfg)
        if refusal is not None:
            reason, error = refusal
            return _dispatch_refusal_result(
                lane_id=lane_id,
                manifest_path=manifest_file,
                error=error,
                reason=reason,
                materialized=materialized_existing,
            )
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
            "materialized": materialized_existing,
            "manifest_path": str(root / "config" / "lane-orchestration" / f"{task_ref}.json"),
            "error": None,
        }

    if corrupt_reason is not None:
        # A malformed scope declaration is not a corrupt manifest. Recovering it
        # by rebuilding would dispatch a lane that silently owns nothing, so it
        # fails closed on ambiguity and preserves the operator's declaration.
        scope_declaration_error = _raw_lane_scope_declaration_error(manifest_file, lane_id)
        if scope_declaration_error is not None:
            return _dispatch_refusal_result(
                lane_id=lane_id,
                manifest_path=manifest_file,
                error=scope_declaration_error,
                reason=UNKNOWN_SCOPE_REASON,
                materialized=False,
            )

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
            preferred_speed=preferred_speed,
        )
    except OffloadPreflightError as exc:
        if exc.outcome == DISPATCH_REFUSED_OUTCOME:
            return _dispatch_refusal_result(
                lane_id=lane_id,
                manifest_path=manifest_file,
                error=f"{named}; auto-materialize failed: {exc}",
                reason="branch_reference_invalid",
                materialized=False,
            )
        failure: dict[str, Any] = {
            "ok": False,
            "lane_config": None,
            "materialized": False,
            "manifest_path": None,
            "error": f"{named}; auto-materialize failed: {exc}",
        }
        if exc.outcome:
            failure["outcome"] = exc.outcome
        return failure
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
    refusal = _lane_dispatch_refusal(lane_id, lane_cfg)
    if refusal is not None:
        reason, error = refusal
        return _dispatch_refusal_result(
            lane_id=lane_id,
            manifest_path=manifest_path,
            error=error,
            reason=reason,
            materialized=True,
        )
    return {
        "ok": True,
        "lane_config": lane_cfg,
        "materialized": True,
        "manifest_path": str(manifest_path),
        "error": None,
    }


def _codemap_freshness_degrade(note: str) -> dict[str, Any]:
    """Typed degrade payload matching a real no-CLI skip key set.

    A degrade path that returns a *different* shape than the real check is worse
    than returning nothing: consumers that read ``indexed_sha_readable`` (or any
    other key) without a per-key guard treat a missing key as ``None`` — falsy,
    and therefore indistinguishable from "measured, and the payload carried no
    readable sha". That silent substitution of an unmeasured value for a measured
    one is exactly the defect class the freshness gate exists to close.
    """
    return {
        "available": False,
        "stale": False,
        "note": note,
        "status": None,
        "detect_changes": None,
        "project": None,
        "project_note": None,
        "cli_path": None,
        "head_sha": None,
        "primary_head_sha": None,
        "indexed_head_sha": None,
        "indexed_sha_readable": False,
        "lane_divergent": False,
    }


def _is_pre_discovery_default(
    caller_model: str | None,
    *,
    discovery: Any,
    profile_pin: str | None,
) -> bool:
    """True when *caller_model* is the tracked/manifest default, not an override."""
    if not caller_model or discovery is None:
        return False
    defaults = {discovery.tracked_pin, profile_pin}
    defaults.discard(None)
    return caller_model in defaults and caller_model != discovery.resolved_model


def _should_rewrite_lane_preferred_model(
    caller_model: str | None,
    *,
    discovery: Any,
    profile_pin: str | None,
) -> bool:
    """Rewrite only a missing, tracked, or already-resolved caller model.

    A refused override (e.g. ``grok-build``) must not upgrade the durable
    manifest pin — the next no-model call would then run the discovered slug
    even though this preflight returned an error ([AGT-10]).
    """
    if discovery is None or not getattr(discovery, "resolved_model", None):
        return False
    if not caller_model:
        return True
    if caller_model == discovery.resolved_model:
        return True
    return _is_pre_discovery_default(caller_model, discovery=discovery, profile_pin=profile_pin)


def rewrite_lane_preferred_model(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    resolved_model: str,
    allowed_from: set[str] | None = None,
) -> None:
    """Overwrite a pre-discovery tracked default with the pin that will run.

    When *allowed_from* is set, a lane whose current ``preferred_model`` is
    outside that set (and is not already the resolved pin) is left untouched.
    """
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        atomic_update_manifest,
    )

    pin = str(resolved_model).strip()
    manifest_path = Path(orchestrator_root).expanduser().resolve()
    manifest_path = manifest_path / "config" / "lane-orchestration" / f"{task_ref}.json"
    if not pin or not manifest_path.exists():
        return

    def _apply(manifest: dict) -> None:
        lanes = manifest.get("lanes")
        if not isinstance(lanes, dict):
            return
        lane = lanes.get(lane_id)
        if not isinstance(lane, dict):
            return
        current = lane.get("preferred_model")
        if allowed_from is not None and current and current not in allowed_from and current != pin:
            return
        lane["preferred_model"] = pin

    try:
        atomic_update_manifest(manifest_path, _apply)
    except (FileNotFoundError, RuntimeError, OSError):
        return


BUDGET_BLOCKER_SESSION = "offload-preflight-budget"
_logger = logging.getLogger(__name__)


def budget_blocker_prefix(backend: str) -> str:
    """Stable dedupe key for a backend's budget blocker (numbers excluded)."""
    return f"{backend.removesuffix('-remote')} budget below threshold"


def _record_budget_blocker(
    *,
    orchestrator_root: Path | str,
    task_ref: str | None,
    lane_id: str | None,
    description: str,
    backend: str,
) -> bool:
    """Best-effort task-scoped blocker (event_kind=blocker) for a budget trip.

    Same shape as ``host_resources._record_breaker_blocker``; never raises —
    the refusal that follows is the enforcement, the blocker is the alert
    (SEC-08 names alerts explicitly). ``task_ref=None`` resolves the
    workspace's active task through the handoff api itself (the same
    resolution ``handoff_close_check`` applies); an unresolvable task logs a
    warning naming the dropped alert instead of silently skipping it
    (S5-L-04). Dedupe is on the stable ``budget_blocker_prefix`` + backend, not
    the numbers: an already-open row has its description refreshed in place
    so a retry loop never stacks rows (S5-L-03).
    """
    try:
        from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(orchestrator_root)))
        prefix = budget_blocker_prefix(backend)
        backend_tag = f"backend {backend}"
        existing_id: int | None = None
        existing_text = ""
        resolved_task_ref: str | None = task_ref
        try:
            existing = handoff_api.handoff_close_check(task_ref=task_ref, enforce=False)
            data = existing.get("data") if isinstance(existing.get("data"), dict) else existing
            if not resolved_task_ref and isinstance(existing, dict):
                resolved_task_ref = existing.get("task_ref") or (data or {}).get("task_ref")
            items = (((data or {}).get("checks") or {}).get("open_blockers") or {}).get("items") or []
            for row in items:
                if not isinstance(row, dict):
                    continue
                text = str(row.get("description") or "")
                if text.startswith(prefix) and backend_tag in text and isinstance(row.get("id"), int):
                    existing_id = int(row["id"])
                    existing_text = text
                    break
        except Exception:  # noqa: BLE001 -- dedupe is advisory; still record
            existing_id = None
        if existing_id is not None:
            if existing_text == description:
                return True
            from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

            with _get_db_connection() as conn:
                conn.execute(
                    "UPDATE blockers SET description = ? WHERE id = ? AND status = 'open'",
                    (description, existing_id),
                )
            return True
        event: dict[str, Any] = {
            "event_kind": "blocker",
            "session": BUDGET_BLOCKER_SESSION,
            "operation": "add",
            "description": description,
        }
        if resolved_task_ref:
            event["task_ref"] = resolved_task_ref
        if lane_id:
            event["actor"] = {"lane_id": lane_id}
        result = handoff_api.record_event(event=event)  # type: ignore[arg-type]
        ok = bool(isinstance(result, dict) and result.get("ok"))
        if not ok:
            _logger.warning(
                "budget alert dropped for %s (task_ref=%s lane_id=%s): handoff refused the blocker: %s",
                backend,
                resolved_task_ref or "<unresolved>",
                lane_id,
                (result or {}).get("data") if isinstance(result, dict) else result,
            )
        return ok
    except Exception as exc:  # noqa: BLE001 -- best-effort by contract
        _logger.warning(
            "budget alert dropped for %s (task_ref=%s lane_id=%s): %s: %s",
            backend,
            task_ref or "<unresolved active task>",
            lane_id,
            type(exc).__name__,
            exc,
        )
        return False


def key_info_budget_trip(backend: str, availability: dict[str, Any]) -> str | None:
    """Blocker text when a key_info backend's probe reading is below its threshold.

    ``None`` for backends without a key_info port, or when the reading is at or
    above ``min_remaining_usd`` (strict ``<``, matching the VM probe). Trips on
    ``auth_state == budget_exhausted`` even without numbers (null limit), on a
    parsed ``remaining`` below threshold, and on a parsed ``credits_available``
    below threshold — the last is the account wallet a 402 names. When a
    consumer already surfaces ``limit=… usage=… remaining=…``, the credits
    reading is named alongside it so ``remaining=24.874`` on a dead account
    cannot stand alone.
    """
    from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
        BACKENDS,
        REMOTE_AUTH_BUDGET_EXHAUSTED,
    )

    spec = BACKENDS.get(backend)
    port = spec.auth if spec is not None else None
    if port is None or not port.key_info_url:
        return None
    key_info = availability.get("key_info") if isinstance(availability.get("key_info"), dict) else {}
    credits = availability.get("credits") if isinstance(availability.get("credits"), dict) else {}
    remaining = key_info.get("remaining")
    credits_available = credits.get("credits_available")
    numeric = isinstance(remaining, (int, float)) and not isinstance(remaining, bool)
    credits_numeric = isinstance(credits_available, (int, float)) and not isinstance(credits_available, bool)
    exhausted = str(availability.get("auth_state") or "") == REMOTE_AUTH_BUDGET_EXHAUSTED
    key_cap_trip = numeric and remaining < port.min_remaining_usd
    credits_trip = credits_numeric and credits_available < port.min_remaining_usd
    if not exhausted and not key_cap_trip and not credits_trip:
        return None
    label = backend.removesuffix("-remote")

    def _fmt(source: dict[str, Any], key: str) -> str:
        value = source.get(key)
        return str(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else "?"

    return (
        f"{label} budget below threshold: remaining={_fmt(key_info, 'remaining')} limit={_fmt(key_info, 'limit')} "
        f"usage={_fmt(key_info, 'usage')} credits={_fmt(credits, 'credits')} "
        f"credit_usage={_fmt(credits, 'credit_usage')} credits_available={_fmt(credits, 'credits_available')} "
        f"(threshold {port.min_remaining_usd} USD; backend {backend}); "
        "dispatch refused until the key is topped up or rotated"
    )


def enforce_key_info_budget(
    *,
    backend: str,
    availability: dict[str, Any],
    orchestrator_root: Path | str,
    task_ref: str | None,
    lane_id: str | None,
) -> None:
    """Budget alert + refusal for key_info backends (implementation note S5; SEC-08, AGT-10).

    Raises :class:`OffloadPreflightError` after recording a task blocker when
    the probe's ``remaining`` is below ``OPENROUTER_MIN_REMAINING_USD``; no-op
    otherwise. The probe reading is the source — no second ssh round-trip.
    """
    description = key_info_budget_trip(backend, availability)
    if description is None:
        return
    _record_budget_blocker(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        description=description,
        backend=backend,
    )
    raise OffloadPreflightError(description)


def key_info_admission_gate(
    *,
    backend: str,
    orchestrator_root: Path | str,
    task_ref: str | None,
    lane_id: str | None,
    surface: str,
    probe_availability: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Budget admission for the daemon spawn edge (implementation note S5 review, S5-M-01).

    ``None`` (no behaviour change, no probe) for every backend whose AuthPort
    has no ``key_info_url``. For key-info backends the TTL-cached availability
    probe is consulted (30s cache in the registry — not a new network call
    per dispatch) and a below-threshold reading records the budget blocker and
    returns an ``admission_refused`` payload shaped like the host-memory gate
    so worker_start / run_offload_pass surface it unchanged.

    up0824-ppd-r1-f01: a probe that never answers (deadline-expired) or that
    raised is NOT a below-threshold reading — ``enforce_key_info_budget`` reads
    ``availability["key_info"]``, which is absent on both an expired and an
    errored probe result, so it silently no-ops and admission would otherwise
    fall through exactly like a clean below-threshold pass. Refuse admission
    instead of guessing the budget is fine whenever the probe did not resolve
    a real reading.
    """
    from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
        BACKENDS,
    )

    spec = BACKENDS.get(backend)
    port = spec.auth if spec is not None else None
    if port is None or not port.key_info_url:
        return None
    if probe_availability is None:
        from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
            probe_availability as _probe,
        )

        probe_availability = _probe
    availability = bounded_probe(
        backend,
        probe=probe_availability,
        workspace_root=Path(orchestrator_root),
    )
    # ``state`` is the field bounded_probe always populates (a raw probe result
    # copies its own "state"; the expired/error synthetic branches set it to
    # "unknown"/"error"). A key absent from a legitimate probe's own payload
    # (e.g. a budget-focused stub that never sets it) must NOT be treated as
    # unresolved, so this reads the value with no "or unknown" fallback.
    probe_availability_state = availability.get("state")
    if availability.get("probe_expired") or probe_availability_state in {"unknown", "error"}:
        description = (
            f"{backend} key_info admission probe did not resolve a reading "
            f"(availability_state={probe_availability_state!r}, "
            f"probe_expired={bool(availability.get('probe_expired'))}); "
            "admission deferred rather than assumed pass"
        )
        _logger.warning(
            "key_info_admission_probe_unresolved",
            extra={
                "backend": backend,
                "surface": surface,
                "availability_state": probe_availability_state,
                "probe_expired": bool(availability.get("probe_expired")),
            },
        )
        payload: dict[str, Any] = {
            "ok": False,
            "outcome": "admission_deferred",
            "error": description,
            "error_kind": "admission_deferred",
            "admission": {
                "decision": "defer",
                "reason": description,
                "gate": "key_info_probe",
                "backend": backend,
                "surface": surface,
                "availability_state": probe_availability_state,
            },
            "backend": backend,
        }
        if task_ref:
            payload["task_ref"] = task_ref
        if lane_id:
            payload["lane_id"] = lane_id
        return payload
    try:
        enforce_key_info_budget(
            backend=backend,
            availability=availability,
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
        )
    except OffloadPreflightError as exc:
        description = str(exc)
        payload: dict[str, Any] = {
            "ok": False,
            "outcome": "admission_refused",
            "error": description,
            "error_kind": "admission_refused",
            "admission": {
                "decision": "refuse",
                "reason": description,
                "gate": "key_info_budget",
                "backend": backend,
                "surface": surface,
                "key_info": availability.get("key_info"),
                "auth_state": availability.get("auth_state"),
            },
            "backend": backend,
        }
        if task_ref:
            payload["task_ref"] = task_ref
        if lane_id:
            payload["lane_id"] = lane_id
        return payload
    return None


def record_lane_spend_bound(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    spend_bound: float | None,
) -> None:
    """implementation note S4: record the spend bound read at dispatch on the lane row.

    ``spend_bound`` is OpenRouter ``data.limit`` (USD) from the availability
    probe of a key-info backend. Written to the lane entry of the durable
    lane-orchestration manifest (no handoff-db schema change); a None bound
    writes nothing and a missing manifest/lane is left untouched.
    """
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        atomic_update_manifest,
    )

    if spend_bound is None:
        return
    manifest_path = Path(orchestrator_root).expanduser().resolve()
    manifest_path = manifest_path / "config" / "lane-orchestration" / f"{task_ref}.json"
    if not manifest_path.exists():
        return

    def _apply(manifest: dict) -> None:
        lanes = manifest.get("lanes")
        if not isinstance(lanes, dict):
            return
        lane = lanes.get(lane_id)
        if not isinstance(lane, dict):
            return
        lane["spend_bound"] = float(spend_bound)

    try:
        atomic_update_manifest(manifest_path, _apply)
    except (FileNotFoundError, RuntimeError, OSError):
        return


_pending_discovery_request: dict[str, str | None] = {"model": None, "effort": None}


def _discover_and_bind_offload_pin(profile: Any) -> tuple[Any, Any, str | None]:
    """Probe/publish the resolved pin. Does not rewrite the durable manifest."""
    from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
        PinHomeUndeclaredError,
        get_offload_profile,
        publish_resolved_model_pins,
        resolve_offbox_model,
    )

    pre_discovery_pin = profile.pinned_model or profile.default_model
    model_discovery = None
    try:
        model_discovery = resolve_offbox_model(
            profile.agent,
            probe=profile.agent in REMOTE_OFFLOAD_BACKENDS,
            request_model=_pending_discovery_request.get("model"),
            request_effort=_pending_discovery_request.get("effort"),
        )
        publish_resolved_model_pins({profile.agent: model_discovery})
        profile = get_offload_profile(profile.agent)
    except PinHomeUndeclaredError as exc:
        raise OffloadPreflightError(str(exc)) from exc
    except KeyError:
        model_discovery = None
    return profile, model_discovery, pre_discovery_pin


def _native_metadata_error_is_capability_refusal(exc: BaseException) -> bool:
    """True when native metadata named a capability miss, not an optional outage."""
    kind = getattr(exc, "kind", None)
    # timeout/malformed/mismatch: the producer ran but the protocol degraded.
    # unavailable/unsupported_*: no usable capability proof.
    return kind in {"unsupported_effort", "unsupported_model", "unavailable"}


def _catalogue_from_non_grok_discovery(
    backend_id: str,
    discovery: Any,
    *,
    captured_at: float,
) -> Any:
    """Build a snapshot from a probed non-Grok listing (live entitlement).

    Grok backends prove capability through native metadata. Names-only Grok
    listings are not admission. Codex/openrouter admission is the live
    entitlement / pin-home listing, not a Grok catalogue.
    """
    from workbay_orchestrator_mcp.orchestration import backend_registry  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.resolved_role_config import (  # noqa: PLC0415
        make_catalogue_snapshot,
        native_transport_for_backend,
    )

    spec = backend_registry.BACKENDS.get(backend_id)
    if spec is None or getattr(spec, "native_metadata_producer_path", None):
        return None
    if discovery is None or getattr(discovery, "catalogue_source", None) != "probed":
        return None
    names = tuple(str(item).strip() for item in getattr(discovery, "catalogue", ()) if str(item).strip())
    if not names:
        return None
    transport = native_transport_for_backend(backend_id)
    return make_catalogue_snapshot(
        version="live_entitlement",
        models=names,
        capabilities=tuple((name, transport) for name in names),
        captured_at=captured_at,
    )


def _augment_discovery_with_native_metadata(
    discovery: Any,
    *,
    backend_id: str,
    model: str | None,
    effort: str | None,
) -> Any:
    """Observe native ACP metadata on the public preflight path.

    Injected catalogues skip this function. Names-only listings are not
    capability proof; a declared producer must select the requested model.
    """
    from workbay_orchestrator_mcp.orchestration import backend_registry  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.adapters.grok_cli import (  # noqa: PLC0415
        NativeMetadataError,
    )
    from workbay_orchestrator_mcp.orchestration.offload_model_discovery import (  # noqa: PLC0415
        invoke_native_metadata_producer,
        merge_native_metadata,
    )

    spec = backend_registry.BACKENDS.get(backend_id)
    if spec is None or not getattr(spec, "native_metadata_producer_path", None):
        return discovery
    existing = getattr(discovery, "catalogue_capabilities", None) if discovery is not None else None
    existing_efforts = getattr(discovery, "catalogue_advertised_efforts", None) if discovery is not None else None
    if existing and (not effort or existing_efforts):
        return discovery
    selected = (model or "").strip()
    if not selected and discovery is not None:
        selected = str(getattr(discovery, "resolved_model", "") or getattr(discovery, "tracked_pin", "") or "").strip()
    if not selected:
        raise OffloadPreflightError("catalogue_unavailable: native_metadata_unavailable: no model selected")
    try:
        evidence = invoke_native_metadata_producer(backend_id, model=selected, effort=effort)
    except NativeMetadataError as exc:
        if exc.kind == "unsupported_effort" and effort is not None:
            try:
                evidence = invoke_native_metadata_producer(backend_id, model=selected, effort=None)
            except NativeMetadataError as nested:
                if _native_metadata_error_is_capability_refusal(nested):
                    raise OffloadPreflightError(
                        f"catalogue_unavailable: native_metadata_{nested.kind}: {nested}"
                    ) from nested
                return discovery
            except FileNotFoundError as nested:
                raise OffloadPreflightError(f"catalogue_unavailable: native_metadata_unavailable: {nested}") from nested
        elif _native_metadata_error_is_capability_refusal(exc):
            raise OffloadPreflightError(f"catalogue_unavailable: native_metadata_{exc.kind}: {exc}") from exc
        else:
            return discovery
    except FileNotFoundError as exc:
        raise OffloadPreflightError(f"catalogue_unavailable: native_metadata_unavailable: {exc}") from exc
    except (RuntimeError, OSError):
        return discovery
    if evidence is None:
        return discovery
    return merge_native_metadata(discovery, evidence)


def _maybe_rewrite_lane_preferred_model(
    *,
    orchestrator_root: Path,
    task_ref: str | None,
    lane_id: str | None,
    caller_model: str | None,
    discovery: Any,
    profile_pin: str | None,
) -> None:
    """Write the resolved pin only after fail-fast checks have succeeded."""
    if not task_ref or not lane_id:
        return
    if not _should_rewrite_lane_preferred_model(caller_model, discovery=discovery, profile_pin=profile_pin):
        return
    allowed = {getattr(discovery, "tracked_pin", None), profile_pin, None}
    allowed.discard(None)
    rewrite_lane_preferred_model(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        resolved_model=discovery.resolved_model,
        allowed_from={str(item) for item in allowed} or None,
    )


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
        return _codemap_freshness_degrade(f"codemap_unavailable:import_error:{exc}")
    try:
        return check_codemap_index_freshness(worktree_path)
    except Exception as exc:  # noqa: BLE001
        return _codemap_freshness_degrade(f"codemap_unavailable:check_error:{exc}")


TURN_METRICS_ADVICE_CAP = 2_000
TURN_METRICS_ADVICE_PAGE = 500
TOKEN_BUDGET_BELOW_FLOOR_PREFIX = "token_budget_below_floor: "
TOKEN_BUDGET_BELOW_RECOMMENDED_PREFIX = "token_budget_below_recommended: "


def _list_turn_metrics_for_advice(**kwargs: Any) -> Any:
    """Read-only turn_metrics fetch across tasks; isolated so tests can stub it.

    Deliberately does not call ``lanes.list_turn_metrics``, which hard-filters
    ``WHERE task_ref = ?`` (and resolves ``None`` to the active task). Percentile
    advice needs distinct ``(task_ref, lane_id)`` pairs, so this reader pages
    matching backend/model rows with no task_ref filter.
    """
    from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
        _decode_turn_metric_row_dict,
    )
    from workbay_orchestrator_mcp.lanes_support import (  # noqa: PLC0415
        _get_db_connection,
        _normalize_optional_text,
        _paginated_query,
    )

    backend = _normalize_optional_text(kwargs.get("backend"))
    model = _normalize_optional_text(kwargs.get("model"))
    limit = int(kwargs.get("limit") or TURN_METRICS_ADVICE_PAGE)
    offset = max(0, int(kwargs.get("offset") or 0))
    clauses = ["COALESCE(phase, '') != 'lane_prep'"]
    params: list[object] = []
    if backend is not None:
        clauses.append("backend = ?")
        params.append(backend)
    if model is not None:
        clauses.append("model = ?")
        params.append(model)
    with _get_db_connection() as conn:
        total, rows = _paginated_query(
            conn,
            "turn_metrics",
            " AND ".join(clauses),
            tuple(params),
            limit,
            offset,
            "created_at DESC, id DESC",
            _decode_turn_metric_row_dict,
        )
    return {
        "ok": True,
        "turn_metrics": rows,
        "has_more": offset + len(rows) < total,
        "total_matching": total,
        "returned": len(rows),
    }


def _page_turn_metrics_for_advice(
    *,
    backend: str,
    model: str | None,
) -> list[dict[str, Any]]:
    """Page turn_metrics up to the 2_000 most recent matching rows (all tasks)."""
    rows: list[dict[str, Any]] = []
    offset = 0
    while len(rows) < TURN_METRICS_ADVICE_CAP:
        remaining = TURN_METRICS_ADVICE_CAP - len(rows)
        payload = _list_turn_metrics_for_advice(
            backend=backend,
            model=model,
            limit=min(TURN_METRICS_ADVICE_PAGE, remaining),
            offset=offset,
        )
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            raise RuntimeError("turn_metrics list did not return ok")
        page = payload.get("turn_metrics")
        if not isinstance(page, list):
            raise RuntimeError("turn_metrics list missing rows")
        typed = [row for row in page if isinstance(row, dict)]
        rows.extend(typed)
        if not payload.get("has_more") or not typed:
            break
        offset += len(page)
    return rows[:TURN_METRICS_ADVICE_CAP]


_VALID_ADVICE_LANE_KINDS = frozenset({"implement", "review"})


def _resolve_advice_lane_kind(*candidates: object) -> str:
    """Return review vs implement from an explicit kind, never a lane_id substring."""
    for candidate in candidates:
        kind = str(candidate or "").strip().lower()
        if kind in _VALID_ADVICE_LANE_KINDS:
            return kind
    return "implement"


def _persisted_identity_unavailable(exc: BaseException) -> OffloadPreflightError:
    return OffloadPreflightError(f"conflicting_role_identity: persisted identity unavailable: {exc}")


def _load_persisted_lane_row(*, task_ref: str | None, lane_id: str | None) -> dict[str, Any] | None:
    """Persisted ``worktree_lanes`` row. None when absent; unreadable raises."""
    if not task_ref or not lane_id:
        return None
    try:
        from workbay_orchestrator_mcp.lanes import _get_lane_row  # noqa: PLC0415
        from workbay_orchestrator_mcp.lanes_support import _get_db_connection  # noqa: PLC0415

        with _get_db_connection() as conn:
            row = _get_lane_row(conn, task_ref, lane_id)
    except FileNotFoundError:
        return None
    except (OSError, ImportError, json.JSONDecodeError, sqlite3.Error) as exc:
        raise _persisted_identity_unavailable(exc) from exc
    except Exception:  # noqa: BLE001 — an unconfigured store is absent, not unreadable
        return None
    return row if isinstance(row, dict) else None


def _lane_kind_from_lane_row(*, task_ref: str | None, lane_id: str | None) -> str | None:
    """Best-effort ``worktree_lanes.lane_kind`` lookup; None when unavailable."""
    try:
        row = _load_persisted_lane_row(task_ref=task_ref, lane_id=lane_id)
    except OffloadPreflightError:
        return None
    if row is None:
        return None
    kind = str(row.get("lane_kind") or "").strip().lower()
    return kind if kind in _VALID_ADVICE_LANE_KINDS else None


def _token_budget_advice_payload(
    *,
    token_budget: int,
    backend: str,
    model: str | None,
    task_ref: str | None,
    lane_id: str | None,
    lane_kind: str | None = None,
    floor: int | None = None,
    bounds_fn: Callable[[int], dict[str, int]] | None = None,
) -> tuple[dict[str, Any], str | None, str | None]:
    """Build the preflight ``token_budget_advice`` block and gate strings.

    Returns ``(advice_dict, warn_message, refuse_reason)``. A broken
    turn_metrics reader yields ``source="unavailable"``; only a budget below
    the floor refuses dispatch. Equal-to-floor is ok; below recommended warns.
    Lane kind comes from an explicit dispatch/row kind, never from a lane_id
    substring. *bounds_fn* should be the profile's ``derive_single_cycle_bounds``.
    """
    from dataclasses import asdict  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.budget_floor import (  # noqa: PLC0415
        BudgetAdvice,
        check_token_budget,
        floor_for,
        recommend_token_budget,
    )

    resolved_kind = _resolve_advice_lane_kind(
        lane_kind,
        _lane_kind_from_lane_row(task_ref=task_ref, lane_id=lane_id),
    )
    if floor is None:
        try:
            floor = floor_for(resolved_kind)
        except ValueError as exc:
            raise OffloadPreflightError(f"invalid token budget floor: {exc}") from exc

    def _unavailable_advice(exc: Exception) -> BudgetAdvice:
        _logger.warning(
            "turn_metrics budget advice unavailable for backend=%s model=%s: %s",
            backend,
            model or "<unspecified>",
            exc,
            exc_info=True,
        )
        return BudgetAdvice(
            floor=floor,
            sample_lanes=0,
            p50=None,
            p90=None,
            recommended=floor,
            source="unavailable",
        )

    try:
        rows = _page_turn_metrics_for_advice(
            backend=backend,
            model=model,
        )
    except Exception as exc:  # noqa: BLE001 — metrics are advisory; never fail preflight
        advice = _unavailable_advice(exc)
    else:
        try:
            advice = recommend_token_budget(
                rows,
                backend=backend,
                model=model,
                lane_kind=resolved_kind,
            )
        except ValueError as exc:
            # A malformed floor override is a configuration error, not a
            # metrics outage. Keep it typed at the offload boundary instead of
            # degrading to source="unavailable".
            raise OffloadPreflightError(f"invalid token budget floor: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 — metrics are advisory; never fail preflight
            advice = _unavailable_advice(exc)
    verdict = check_token_budget(token_budget, advice, bounds_fn=bounds_fn)
    if verdict.severity == "refuse":
        return asdict(advice), None, f"{TOKEN_BUDGET_BELOW_FLOOR_PREFIX}{verdict.reason}"
    if verdict.severity == "warn":
        return asdict(advice), f"{TOKEN_BUDGET_BELOW_RECOMMENDED_PREFIX}{verdict.reason}", None
    return asdict(advice), None, None


def persist_transport_capability_receipt(
    orchestrator_root: Path,
    *,
    gate_host: str,
    sandbox_flags: Any,
    task_ref: str | None = None,
    lane_id: str | None,
    pass_id: str | None,
    dispatch_id: str | None,
    worktree_path: Path,
    config_digest: str | None,
    transport_digest: str,
) -> str:
    """Mint an identity-bound transport receipt before live probe evidence exists.

    Probe timeout/cleanup stay unobserved and write gates remain UNKNOWN
    (ecd32aecf9). Spawn-complete ``validate_capability_receipt`` still refuses
    until a live VM probe refreshes those fields; pre-spawn validation may
    accept the mint as not-yet-probed.
    """
    from workbay_orchestrator_mcp.orchestration.preflight_attestation import (  # noqa: PLC0415
        record_attestation,
    )

    try:
        capability_receipt_id = record_attestation(
            orchestrator_root,
            gate_host=gate_host,
            codex_version="",
            sandbox_flags=sandbox_flags,
            source="operator_file",
            task_ref=task_ref,
            lane_id=lane_id,
            pass_id=pass_id,
            dispatch_id=dispatch_id,
            worktree_path=worktree_path,
            config_digest=config_digest,
            transport_digest=transport_digest,
            transport_verdict="positive",
            probe_timeout=None,
            probe_cleanup_outcome="unknown",
        )
    except (OSError, RuntimeError) as exc:
        refusal = OffloadPreflightError(f"could not persist capability receipt: {exc}")
        setattr(refusal, "kind", "capability_receipt_unavailable")
        raise refusal from exc
    if not capability_receipt_id:
        refusal = OffloadPreflightError("capability receipt was not persisted")
        setattr(refusal, "kind", "capability_receipt_unavailable")
        raise refusal
    return capability_receipt_id


def offload_preflight(
    *,
    orchestrator_root: Path,
    worktree_path: Path,
    agent: str,
    token_budget: int | None,
    probe_availability: Callable[..., dict[str, Any]] | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    speed: str | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    lane_kind: str | None = None,
    pass_id: str | None = None,
    dispatch_id: str | None = None,
    capability_config_digest: str | None = None,
    strict: bool = False,
    catalogue: Any | None = None,
    catalogue_refresh: Callable[[], Any] | None = None,
    now: float | None = None,
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
    ``detect_changes`` and attaches a non-fatal note selected by cause:
    ``codemap_stale`` (reindex via ``index_repository``),
    ``codemap_divergence`` (index covers a different checkout — not cleared by
    reindex), ``codemap_incomparable`` (shas present but too short to compare —
    not cleared by reindex), or ``codemap_sha_unreadable`` (status carried no
    recognizable commit sha). CLI absent → ``codemap_unavailable`` skip note
    ([OBS-08], implementation note S12 / T25). Never blocks on codemap state.
    """
    from workbay_orchestrator_mcp.orchestration.backend_spec import require_canonical_operator_value
    from workbay_orchestrator_mcp.orchestration.offload_profiles import get_offload_profile

    if isinstance(token_budget, bool) or not isinstance(token_budget, int) or token_budget <= 0:
        raise OffloadPreflightError("token_budget must be set to a positive integer for offload")

    # These values are exact protocol pins at dispatch.  Refuse noncanonical
    # spellings here instead of silently rewriting them and claiming the lane is
    # safe to dispatch.
    try:
        agent = require_canonical_operator_value(agent, field_name="backend", lowercase=True)
        if model is not None:
            model = require_canonical_operator_value(model, field_name="model")
        if reasoning_effort is not None:
            reasoning_effort = require_canonical_operator_value(reasoning_effort, field_name="effort", lowercase=True)
    except ValueError as exc:
        raise OffloadPreflightError(str(exc)) from exc

    # Resolve the explicit agent to its offload profile. Unknown backend ids raise
    # RuntimeError via validate_backend; surface them as the single offload error.
    try:
        profile = get_offload_profile(agent)
    except OffloadPreflightError:
        raise
    except RuntimeError as exc:
        raise OffloadPreflightError(str(exc)) from exc

    # The floor is a local policy check. Resolve it before any disk, transport,
    # branch, availability, discovery, or worktree checks so a refusal cannot
    # spend time or mutate durable routing state first.
    from workbay_orchestrator_mcp.orchestration.budget_floor import (
        BudgetAdvice,
        check_token_budget,
        floor_for,
    )

    resolved_lane_kind = _resolve_advice_lane_kind(
        lane_kind,
        _lane_kind_from_lane_row(task_ref=task_ref, lane_id=lane_id),
    )
    try:
        budget_floor = floor_for(resolved_lane_kind)
    except ValueError as exc:
        raise OffloadPreflightError(f"invalid token budget floor: {exc}") from exc

    single_cycle_bounds = derive_single_cycle_bounds(
        profile.single_cycle_bound,
        token_budget,
        timeout_cap=profile.timeout_cap,
    )
    if token_budget < budget_floor:
        floor_advice = BudgetAdvice(
            floor=budget_floor,
            sample_lanes=0,
            p50=None,
            p90=None,
            recommended=budget_floor,
            source="floor",
        )
        floor_verdict = check_token_budget(
            token_budget,
            floor_advice,
            bounds_fn=lambda _value: dict(single_cycle_bounds) if single_cycle_bounds else {},
        )
        raise OffloadPreflightError(f"{TOKEN_BUDGET_BELOW_FLOOR_PREFIX}{floor_verdict.reason}")

    # implementation note S1 residual: echo execution_mode + remote_probe_state so skills can
    # branch without a second tool call ([API-09] additive). repo_root is the lane
    # worktree — same seam as ensure_lane_manifest_for_offload / materialize.
    from workbay_protocol.bootstrap import load_execution_mode  # noqa: PLC0415

    resolved_worktree = worktree_path.expanduser().resolve()
    from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
        cost_class_for_backend,
    )

    check_disk_headroom(
        resolved_worktree,
        cost_class=cost_class_for_backend(profile.agent),
        staging_root=orchestrator_root / ".task-state",
    )
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

    from workbay_orchestrator_mcp.orchestration.backend_registry import BACKENDS  # noqa: PLC0415

    backend_spec = BACKENDS.get(profile.agent)
    transport_resolution: Any | None = None
    transport_source: str | None = None
    transport_digest: str | None = None
    if backend_spec is not None and bool(getattr(backend_spec.capabilities, "dispatchable_off_box", False)):
        from workbay_orchestrator_mcp.orchestration.adapters import remote_exec as remote_exec_mod  # noqa: PLC0415

        try:
            # MUST be resolved_worktree, not the raw argument: every other check
            # in this function validates the expanduser-and-resolve form, and
            # api.offload_preflight forwards a bare Path with no expansion. A
            # caller-supplied tilde or relative path would otherwise make this
            # guard validate a DIFFERENT tree than the one preflight admits --
            # which reads as a pass (the packaged fallback exists on nearly every
            # install) while inspecting the wrong file [OBS-01] [AGT-10].
            transport_resolution = remote_exec_mod.resolve_transport(resolved_worktree)
        except remote_exec_mod.TransportMissingError as exc:
            refusal = OffloadPreflightError(str(exc))
            setattr(refusal, "kind", getattr(exc, "kind", "transport_missing"))
            raise refusal from exc
        transport_source = transport_resolution.source
        transport_digest = transport_resolution.transport_digest

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

    if probe_availability is None:
        from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
            probe_availability as _probe,
        )

        probe_availability = _probe
    availability = bounded_probe(
        profile.agent,
        probe=probe_availability,
        workspace_root=orchestrator_root,
    )
    # implementation note S5: a key-info backend below its spend threshold is an ALERT
    # (task blocker) plus a refusal, before the generic unavailability raise.
    enforce_key_info_budget(
        backend=profile.agent,
        availability=availability,
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
    )
    # implementation note S4: a key-info backend's probe carries the spend bound
    # (OpenRouter data.limit) read at dispatch; recorded on the lane row below.
    spend_bound = availability.get("spend_bound")
    if not isinstance(spend_bound, (int, float)) or isinstance(spend_bound, bool):
        spend_bound = None
    # Reuse the probe result already in this flow — no second SSH probe.
    if is_remote_agent:
        remote_probe_state = str(availability.get("state") or "unknown")
        # up0824-ppd-r2-f03: bounded_probe's own error state is "error", a
        # value outside the AVAIL_* vocabulary (available / reachable /
        # declared_not_installed / unavailable / unknown) that
        # list_available_backends already remaps to AVAIL_UNKNOWN for a
        # raising probe. Mirror that remap here so the preflight error surface
        # and the tool surface agree on the same closed vocabulary.
        if remote_probe_state == "error":
            remote_probe_state = "unknown"
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

    from workbay_protocol.reasoning_effort import validate_reasoning_effort

    try:
        normalized_effort = validate_reasoning_effort(reasoning_effort)
        if normalized_effort is None:
            from workbay_orchestrator_mcp.orchestration.effort_policy import (  # noqa: PLC0415
                MAX_EFFORT_MODELS,
                LunaEffortPolicyError,
                resolve_dispatch_effort,
            )

            candidate_model = str(model or profile.default_model or profile.pinned_model or "").strip()
            if candidate_model.lower() in {item.lower() for item in MAX_EFFORT_MODELS}:
                try:
                    normalized_effort = resolve_dispatch_effort(profile.agent, candidate_model, None)
                except LunaEffortPolicyError as exc:
                    raise OffloadPreflightError(str(exc)) from exc
            if normalized_effort is None:
                raise ValueError("reasoning effort must be explicit for offload")
    except (TypeError, ValueError) as exc:
        raise OffloadPreflightError(str(exc)) from exc
    if normalized_effort not in profile.allowed_efforts:
        raise OffloadPreflightError(f"agent {profile.agent!r} does not support effort {normalized_effort!r}")
    # Concrete efforts are pinned into the manifest; auto|inherit are resolved by
    # _env.resolve_auto_reasoning_effort at execution and left unpinned.
    pinned_reasoning_effort = normalized_effort if normalized_effort not in {"auto", "inherit"} else None

    from workbay_orchestrator_mcp.orchestration.backend_spec import normalize_operator_speed  # noqa: PLC0415

    try:
        normalized_speed = normalize_operator_speed(speed)
    except ValueError as exc:
        raise OffloadPreflightError(str(exc)) from exc
    if normalized_speed is not None and normalized_speed not in profile.allowed_speeds:
        advertised = ", ".join(profile.allowed_speeds) or "(none)"
        raise OffloadPreflightError(
            f"agent {profile.agent!r} does not advertise speed {normalized_speed!r}; advertised speeds: {advertised}"
        )

    from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
        build_offload_dispatch_receipt,
    )

    # Seed allowed_model on the probed backend only. Probe failure degrades
    # to the tracked pin and names MODEL_DISCOVERY_FAILED_WARNING (never silent).
    # The durable lane-manifest rewrite waits until fail-fast checks succeed.
    _pending_discovery_request["model"] = str(model or "").strip() or None
    _pending_discovery_request["effort"] = normalized_effort
    try:
        profile, model_discovery, pre_discovery_pin = _discover_and_bind_offload_pin(profile)
    finally:
        _pending_discovery_request["model"] = None
        _pending_discovery_request["effort"] = None

    caller_model = str(model or "").strip() or None
    normalized_model = caller_model
    if _is_pre_discovery_default(
        normalized_model,
        discovery=model_discovery,
        profile_pin=pre_discovery_pin,
    ):
        normalized_model = None
    if profile.pinned_model is not None:
        live_models: frozenset[str] | None = None
        if model_discovery is not None and getattr(model_discovery, "catalogue_source", None) == "probed":
            live_models = frozenset(str(slug) for slug in getattr(model_discovery, "catalogue", ()) if str(slug))
        admitted_models = live_models or frozenset({profile.pinned_model})
        if normalized_model is not None and normalized_model not in admitted_models:
            if live_models is not None:
                raise OffloadPreflightError(
                    f"offload model {normalized_model!r} is not in the live entitlement catalogue; "
                    f"entitled models: {', '.join(sorted(live_models)) or '(none)'}"
                )
            raise OffloadPreflightError(f"offload model must be {profile.pinned_model!r}, got {normalized_model!r}")
        selected_model: str | None = normalized_model or profile.pinned_model
    else:
        selected_model = normalized_model

    if profile.agent == "codex-remote":
        from workbay_orchestrator_mcp.orchestration.adapters.remote_exec import resolve_effective_model
        from workbay_orchestrator_mcp.orchestration.backend_registry import BACKENDS

        try:
            selected_model = resolve_effective_model(BACKENDS[profile.agent], profile.agent, selected_model)
        except RuntimeError as exc:
            raise OffloadPreflightError(str(exc)) from exc

    if normalized_speed is not None and profile.agent == "codex-remote":
        from workbay_orchestrator_mcp.orchestration.codex_lane_config import (  # noqa: PLC0415
            CODEX_MODEL_ALLOWED_SERVICE_TIERS,
            CODEX_SPEED_TO_SERVICE_TIER,
        )

        service_tier = CODEX_SPEED_TO_SERVICE_TIER[normalized_speed]
        advertised_tiers = CODEX_MODEL_ALLOWED_SERVICE_TIERS.get(str(selected_model), frozenset())
        if service_tier not in advertised_tiers:
            raise OffloadPreflightError(
                f"agent 'codex-remote' model {selected_model!r} does not advertise service tier "
                f"{service_tier!r}; advertised tiers: {', '.join(sorted(advertised_tiers)) or '(none)'}"
            )

    if not resolved_worktree.exists():
        raise OffloadPreflightError(f"worktree does not exist: {resolved_worktree}")
    if not _worktree_is_clean(resolved_worktree):
        raise OffloadPreflightError(f"worktree must be clean before offload: {resolved_worktree}")

    # Routed by the profile's declared bound KIND rather than a literal
    # comparison, so a backend bounded by wall-clock alone (cursor-cli: no
    # --max-turns) is recognised as governed instead of falling through to the
    # ungoverned-pass refusal below.
    warnings: list[str] = []
    if branch_gate_warning:
        warnings.append(branch_gate_warning)
    if model_discovery is not None and model_discovery.warning:
        warnings.append(model_discovery.warning)

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

    def _advice_bounds(token_budget_value: int) -> dict[str, int]:
        derived = derive_single_cycle_bounds(
            profile.single_cycle_bound,
            token_budget_value,
            timeout_cap=profile.timeout_cap,
        )
        return dict(derived) if derived else {}

    token_budget_advice, budget_warning, budget_refuse = _token_budget_advice_payload(
        token_budget=token_budget,
        backend=profile.agent,
        model=selected_model,
        task_ref=task_ref,
        lane_id=lane_id,
        lane_kind=resolved_lane_kind,
        floor=budget_floor,
        bounds_fn=_advice_bounds,
    )
    if budget_refuse:
        raise OffloadPreflightError(budget_refuse)
    if budget_warning:
        warnings.append(budget_warning)
    record_lane_spend_bound(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        spend_bound=spend_bound,
    )

    capability_receipt_id: str | None = None
    resolved_capability_config_digest: str | None = None
    if transport_resolution is not None and transport_digest is not None:
        # Build the same sandbox argv identity the remote adapter will ship. The
        # receipt is intentionally minted before live write evidence exists: its
        # gates remain UNKNOWN, while the exact transport provenance is already
        # durable and content-addressed. Probe timeout/cleanup stay unobserved
        # until a remote probe runs; inventing ``False``/``complete`` here would
        # make this local inspection indistinguishable from a completed probe.
        # ``validate_capability_receipt`` refuses until a positive VM probe
        # refreshes both the write gates and the observed probe fields.
        from workbay_orchestrator_mcp.orchestration.adapters import remote_exec as remote_exec_mod  # noqa: PLC0415
        from workbay_orchestrator_mcp.orchestration.backend_spec import (  # noqa: PLC0415
            build_agent_spec,
            default_effort_for_model,
        )
        from workbay_orchestrator_mcp.orchestration.preflight_attestation import (  # noqa: PLC0415
            STALE_WORKER_CONFIGURATION_REASON,
            build_capability_configuration,
            configuration_digest,
        )

        # Argv still ships a concrete default for auto|inherit (build_agent_spec
        # refuses effort modes). The receipt digest addresses the requested
        # policy: ``auto`` is hashed as the policy token so per-cycle resolver
        # output cannot look like configuration drift. inherit hashes the
        # model default, matching RemoteExecAdapter when resolve_auto returns
        # None. Explicit efforts hash the explicit value (T7IR2D-a7bd / HIFIX03RV-001).
        argv_effort = (
            default_effort_for_model(profile.agent, str(selected_model))
            if normalized_effort in {"auto", "inherit"}
            else normalized_effort
        )
        spec_effort = remote_exec_mod._capability_digest_effort_token(normalized_effort, argv_effort)
        bound_kwargs: dict[str, int] = {}
        if profile.single_cycle_bound == "grok_derived":
            bound_kwargs["max_turns"] = int(single_cycle_bounds["max_turns"])
        elif single_cycle_bounds:
            bound_kwargs["agent_turn_timeout_s"] = int(single_cycle_bounds["timeout"])
        from workbay_orchestrator_mcp.orchestration.lane_routing import InvalidSpeed  # noqa: PLC0415
        from workbay_orchestrator_mcp.orchestration.worker_daemon_ctl import (  # noqa: PLC0415
            canonical_capability_speed,
        )

        try:
            digest_speed = canonical_capability_speed(profile.agent, normalized_speed)
        except InvalidSpeed as exc:
            refusal = OffloadPreflightError(str(exc))
            setattr(refusal, "kind", "invalid_speed")
            raise refusal from exc
        try:
            preflight_spec = build_agent_spec(
                profile.agent,
                model=str(selected_model),
                effort=argv_effort,
                speed=digest_speed,
                prompt="capability receipt preflight",
                **bound_kwargs,
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            refusal = OffloadPreflightError(f"could not bind capability receipt to remote config: {exc}")
            setattr(refusal, "kind", "capability_receipt_unavailable")
            raise refusal from exc

        sandbox_flags = remote_exec_mod._sandbox_attestation_flags(preflight_spec.argv)
        capability_configuration = build_capability_configuration(
            agent=profile.agent,
            model=str(selected_model),
            reasoning_effort=spec_effort,
            speed=digest_speed,
            single_cycle_bounds=single_cycle_bounds,
            worktree_path=resolved_worktree,
            transport_digest=transport_digest,
        )
        recomputed_capability_config_digest = configuration_digest(capability_configuration)
        if capability_config_digest is not None and capability_config_digest != recomputed_capability_config_digest:
            refusal = OffloadPreflightError(
                "caller capability config digest does not match the recomputed worker configuration "
                f"({STALE_WORKER_CONFIGURATION_REASON})"
            )
            setattr(refusal, "kind", STALE_WORKER_CONFIGURATION_REASON)
            raise refusal
        resolved_capability_config_digest = recomputed_capability_config_digest
        gate_host = remote_exec_mod._transport_attestation_gate_host(resolved_worktree, os.environ)
        capability_receipt_id = persist_transport_capability_receipt(
            orchestrator_root,
            gate_host=gate_host,
            sandbox_flags=sandbox_flags,
            task_ref=task_ref,
            lane_id=lane_id,
            pass_id=pass_id,
            dispatch_id=dispatch_id,
            worktree_path=resolved_worktree,
            config_digest=resolved_capability_config_digest,
            transport_digest=transport_digest,
        )

    dispatch_receipt = build_offload_dispatch_receipt(
        profile.agent,
        served_model=None,
        discovery=model_discovery,
    )
    # Preflight resolves intent but does not execute a process.  Keep pin
    # provenance while refusing to mint observed/served routing telemetry.
    dispatch_receipt["resolved_model"] = None
    dispatch_receipt["served_model"] = None
    dispatch_receipt["gate_host"] = None

    import time as _time

    from workbay_orchestrator_mcp.orchestration.offload_model_discovery import (  # noqa: PLC0415
        authoritative_catalogue_snapshot,
    )
    from workbay_orchestrator_mcp.orchestration.resolved_role_config import (  # noqa: PLC0415
        CatalogueUnavailableError,
        EffortMismatchError,
        IncompatibleTransportError,
        RoleIdentity,
        RoleIdentityConflictError,
        UnsupportedEffortError,
        UnsupportedModelError,
        identity_from_persisted_lane,
        identity_from_role_manifest,
        resolve_role_config,
    )

    catalogue_snapshot = catalogue
    if catalogue_snapshot is None:
        model_discovery = _augment_discovery_with_native_metadata(
            model_discovery,
            backend_id=profile.agent,
            model=str(selected_model) if selected_model else None,
            effort=normalized_effort,
        )
        catalogue_snapshot = authoritative_catalogue_snapshot(model_discovery)
    clock = now if now is not None else _time.monotonic()
    if catalogue_snapshot is None:
        catalogue_snapshot = _catalogue_from_non_grok_discovery(profile.agent, model_discovery, captured_at=clock)
    role_name = "review" if str(resolved_lane_kind or "").lower() in {"review", "adjudicate"} else "execution"
    loaded_lane_config: dict[str, Any] | None = None
    if task_ref and lane_id:
        from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config as _get_lane_config

        try:
            loaded_lane_config = _get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
        except FileNotFoundError:
            loaded_lane_config = None
        except (json.JSONDecodeError, RuntimeError, OSError) as exc:
            raise _persisted_identity_unavailable(exc) from exc
    role_manifest = identity_from_role_manifest(loaded_lane_config)
    if role_manifest is not None and _is_pre_discovery_default(
        role_manifest.model,
        discovery=model_discovery,
        profile_pin=pre_discovery_pin,
    ):
        role_manifest = RoleIdentity(
            role=role_manifest.role,
            backend=role_manifest.backend,
            provider=role_manifest.provider,
            model=None,
            effort=role_manifest.effort,
            transport=role_manifest.transport,
        )
    try:
        resolved_role = resolve_role_config(
            role=role_name,
            request=RoleIdentity(
                role=role_name,
                backend=profile.agent,
                model=str(selected_model) if selected_model else None,
                effort=normalized_effort,
            ),
            role_manifest=role_manifest,
            persisted_lane=identity_from_persisted_lane(_load_persisted_lane_row(task_ref=task_ref, lane_id=lane_id)),
            catalogue=catalogue_snapshot,
            catalogue_refresh=catalogue_refresh,
            now=clock,
            catalogue_required=catalogue_snapshot is not None or catalogue_refresh is not None,
        )
    except (
        CatalogueUnavailableError,
        EffortMismatchError,
        IncompatibleTransportError,
        RoleIdentityConflictError,
        UnsupportedEffortError,
        UnsupportedModelError,
    ) as exc:
        raise OffloadPreflightError(str(exc)) from exc

    _maybe_rewrite_lane_preferred_model(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        caller_model=caller_model,
        discovery=model_discovery,
        profile_pin=pre_discovery_pin,
    )

    return {
        "ok": True,
        "agent": profile.agent,
        # Retain the legacy 'backend' key for existing readers.
        "backend": profile.agent,
        "model": selected_model,
        "resolved_model": selected_model,
        "resolved_role": resolved_role.as_public_dict(),
        "dispatch_receipt": dispatch_receipt,
        "reasoning_effort": normalized_effort,
        "pinned_reasoning_effort": pinned_reasoning_effort,
        "speed": normalized_speed,
        "token_budget": token_budget,
        "token_budget_advice": token_budget_advice,
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
        # implementation note S4: None for backends without a key-info spend bound.
        "spend_bound": spend_bound,
        # Which transport this lane would run ("worktree" | "primary" |
        # "package"); None for on-box backends. Anything but "worktree" means a
        # script from outside the lane checkout [OBS-08].
        "transport_source": transport_source,
        "transport_digest": transport_digest,
        "capability_receipt_id": capability_receipt_id,
        "capability_config_digest": resolved_capability_config_digest,
    }
