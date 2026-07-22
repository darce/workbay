#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import posixpath
import re
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[4]
DEFAULT_MANIFEST_DIR = REPO_ROOT / "config" / "lane-orchestration"
MANIFEST_DIR = DEFAULT_MANIFEST_DIR

logger = logging.getLogger(__name__)

REQUIRED_TOP_LEVEL_KEYS = ("task_ref", "merge_order", "lanes", "downstream")
REQUIRED_LANE_KEYS = ("branch", "worktree_path", "owned_paths", "test_commands")
# Known top-level keys (required + optional). Unknown keys are not refused
# (compat) but emit a warning at validate time — e.g. metrics keys merged
# into the dict before save must not silently re-serialize as schema.
KNOWN_TOP_LEVEL_KEYS = frozenset(
    {
        "task_ref",
        "merge_order",
        "lanes",
        "downstream",
        "depends_on",
        "task_plan_path",
        "heading_to_lane",
        "plan_routing_hints",
        "routing",
        "default_done_definition",
    }
)

# Declared per-lane permission surface (adoption A). The block is optional this
# release (grant-less manifests still validate); a later release flips absence to
# a hard rejection (expand -> migrate -> contract).
GRANT_KEYS = ("worktree", "primary_repo", "extra_write_paths")
GRANT_ACCESS_LEVELS = ("read_only", "read_write", "none")
# Breadth policy for ABSOLUTE extra_write_paths entries. Matching is purely
# lexical (no filesystem I/O) — a documented invariant that keeps manifest
# validation host-independent and deterministic — and case-insensitive because
# APFS is case-insensitive (``/Private/tmp`` must not bypass a check spelled
# ``/private/tmp``). The macOS ``/private`` symlink alias is folded onto its
# canonical spelling before matching (``/private/etc`` == ``/etc``).
#
# System subtrees are forbidden by CONTAINMENT: any path lexically inside one of
# these roots is rejected (``/usr/bin``, ``/etc/ssh``, ``/Library/LaunchDaemons``,
# ``/System/Library/Extensions``, ...), not just the root itself. Entries are
# stored casefolded.
_FORBIDDEN_EXTRA_PATH_SUBTREES = (
    "/applications",
    "/bin",
    "/dev",
    "/etc",
    "/lib",
    "/library",
    "/opt/homebrew",
    "/proc",
    "/root",
    "/sbin",
    "/sys",
    "/system",
    "/usr",
    "/var/root",
)
# Temp roots that are too broad granted bare, while their *deeper, user-scoped*
# subpaths remain legitimate grants (macOS TMPDIR lives at
# ``/private/var/folders/<xx>/<hash>/T``). Entries are stored casefolded.
_FORBIDDEN_EXACT_EXTRA_PATHS = frozenset({"/var/tmp", "/var/folders"})
# Bare user-home roots (``/Users/<u>``, ``/home/<u>``) are rejected by the depth
# rule below; deeper per-project paths validate. Entries are stored casefolded.
_HOME_ROOT_PREFIXES = ("/users", "/home")


def normalize_absolute_extra_path(entry: str) -> str:
    """Lexically normalize an absolute extra write path for the breadth check.

    Pure string work (no filesystem I/O): ``os.path.normpath`` collapses ``.``,
    ``//``, and trailing separators; the macOS ``/private`` alias is folded onto
    its canonical spelling (``/private/etc`` -> ``/etc``); the result is
    casefolded because APFS is case-insensitive.
    """
    normalized = os.path.normpath(str(entry).strip()).casefold()
    if normalized == "/private" or normalized.startswith("/private/"):
        normalized = normalized[len("/private") :] or "/"
    return normalized


def extra_write_path_too_broad(entry: str) -> bool:
    """True when an ABSOLUTE extra write path is too broad to grant.

    Purely lexical containment tier (no filesystem I/O — the manifest validator's
    documented invariant; ``lane_jail`` re-applies this check to realpath-resolved
    entries where I/O is already in scope):

    - the filesystem root and ANY bare top-level directory (depth <= 1);
    - anything lexically contained in a forbidden system subtree;
    - bare temp roots (``/var/tmp``, ``/var/folders``) — deeper user-scoped
      subpaths such as ``/var/folders/<xx>/<hash>/T/...`` stay grantable;
    - bare user-home roots (``/Users/<u>``, ``/home/<u>``).
    """
    normalized = normalize_absolute_extra_path(entry)
    parts = Path(normalized).parts
    depth = len(parts) - 1  # path segments below '/'
    if depth <= 1:
        return True
    if normalized in _FORBIDDEN_EXACT_EXTRA_PATHS:
        return True
    for root in _FORBIDDEN_EXTRA_PATH_SUBTREES:
        if normalized == root or normalized.startswith(f"{root}/"):
            return True
    # SWEEP-02: bare $HOME is a whole-user-profile grant WHEREVER HOME points
    # (including a /tmp-nested HOME in hermetic runs), which the depth and
    # home-root-prefix rules cannot see. env read only — still no filesystem I/O,
    # so the lexical/deterministic invariant of this check holds.
    home = normalize_absolute_extra_path(os.path.expanduser("~"))
    if home != "/" and normalized == home:
        return True
    return depth == 2 and f"/{parts[1]}" in _HOME_ROOT_PREFIXES


def _validate_grants(lane_id: str, grants: Any, path: Path) -> None:
    """Validate an optional per-lane ``grants`` block.

    Reject unknown keys, non-access-level values for ``worktree``/``primary_repo``,
    and any ``extra_write_paths`` entry that is not a non-empty string or that
    contains a ``..`` path-traversal segment (repo-relative or absolute allowed).
    """
    if not isinstance(grants, dict):
        raise RuntimeError(f"lane '{lane_id}' grants must be an object when present: {path}")
    unknown = sorted(key for key in grants if key not in GRANT_KEYS)
    if unknown:
        raise RuntimeError(f"lane '{lane_id}' grants has unknown key(s) {unknown}: {path}")
    for key in ("worktree", "primary_repo"):
        if key in grants:
            value = grants.get(key)
            if not isinstance(value, str) or value not in GRANT_ACCESS_LEVELS:
                raise RuntimeError(
                    f"lane '{lane_id}' grants.{key} must be one of {GRANT_ACCESS_LEVELS} when present: {path}"
                )
    if "extra_write_paths" in grants:
        extra = grants.get("extra_write_paths")
        if not isinstance(extra, list):
            raise RuntimeError(f"lane '{lane_id}' grants.extra_write_paths must be a list when present: {path}")
        for entry in extra:
            if not isinstance(entry, str) or not entry.strip():
                raise RuntimeError(
                    f"lane '{lane_id}' grants.extra_write_paths entries must be non-empty strings: {path}"
                )
            raw_path = Path(entry.strip())
            if ".." in raw_path.parts:
                raise RuntimeError(
                    f"lane '{lane_id}' grants.extra_write_paths entry must not contain '..' path traversal: {path}"
                )
            if raw_path.is_absolute():
                # Purely lexical containment check (no symlink/filesystem I/O), so
                # validation stays host-independent and deterministic.
                if extra_write_path_too_broad(entry):
                    raise RuntimeError(f"lane '{lane_id}' grants.extra_write_paths entry is too broad: {path}")


def _resolve_manifest_dir(*, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None) -> Path:
    if manifest_dir is not None:
        return Path(manifest_dir).expanduser().resolve()
    if MANIFEST_DIR != DEFAULT_MANIFEST_DIR:
        return MANIFEST_DIR
    if orchestrator_root:
        return (Path(orchestrator_root).expanduser().resolve() / "config" / "lane-orchestration").resolve()
    return MANIFEST_DIR


def manifest_dir(*, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None) -> Path:
    return _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)


def list_task_refs(*, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None) -> list[str]:
    resolved_dir = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    if not resolved_dir.exists():
        return []
    return sorted(path.stem for path in resolved_dir.glob("*.json"))


def _require_key(container: dict[str, Any], key: str, *, path: Path, context: str) -> None:
    if key not in container:
        raise RuntimeError(f"lane manifest missing required {context} key '{key}': {path}")


def _derive_commit_paths(owned_paths: list[str]) -> list[str]:
    derived: list[str] = []
    for path in owned_paths:
        value = _normalize_owned_path(path)
        if value:
            derived.append(value)
    return list(dict.fromkeys(derived))


def _normalize_owned_path(path: str) -> str:
    """Lexical root for collision checks: strip globs, collapse ``.``/``//``, no FS I/O.

    Trailing slashes are stripped *before* glob-suffix removal so that
    ``packages/shared/**/`` normalizes the same as ``packages/shared/**``.
    Backslashes become ``/`` before ``posixpath.normpath`` so Windows and POSIX
    spellings of the same tree compare equal. Absolute paths and paths that
    escape the repo root after normalization are refused — owned_paths are
    repo-relative by contract. Callers' ``owned_paths`` strings are never rewritten.
    """
    value = str(path).strip()
    if not value:
        return ""
    # Path alphabet first: Windows separators must not form a distinct root key.
    value = value.replace("\\", "/")
    # Slash-first then glob so residual ``/**`` after a trailing ``/`` is removed.
    while True:
        stripped = value.rstrip("/")
        if stripped.endswith("/**"):
            value = stripped[:-3]
            continue
        if stripped.endswith("/*"):
            value = stripped[:-2]
            continue
        value = stripped
        break
    if not value:
        return ""
    # Windows drive-absolute spellings survive backslash folding as ``C:/…``.
    if re.match(r"^[A-Za-z]:(/|$)", value):
        raise ValueError(
            f"owned_path must be repo-relative (drive-absolute path refused): {path!r}"
        )
    # Lexical only: collapse // and ., resolve .. segments; never touch the FS.
    normalized = posixpath.normpath(value)
    if normalized == ".":
        # Explicit repo-root spellings ("." / "./") keep the whole-tree
        # sentinel; a ``..`` parent-walk that merely COLLAPSES to root is a
        # mistyped path, not intent — silently granting whole-tree ownership
        # would fail open in collision checks.
        if ".." in value.split("/"):
            raise ValueError(
                f"owned_path collapses to the repo root via '..' (refused): {path!r}"
            )
        return ""
    # Repo-relative contract: refuse absolute roots and residual ``..`` escape.
    if normalized.startswith("/") or normalized == ".." or normalized.startswith("../"):
        raise ValueError(
            f"owned_path must be repo-relative (absolute or escaping path refused): {path!r}"
        )
    return normalized


def _owned_path_roots_overlap(left: str, right: str) -> bool:
    """True when two normalized owned-path roots collide on ``/`` boundaries.

    Equal roots overlap. A parent/child pair overlaps only when the longer path
    continues past a full ``/`` component of the shorter one — so ``packages/a``
    vs ``packages/a/sub`` overlaps, but ``packages/a`` vs ``packages/ab`` does not.

    The empty string is the repo-root sentinel produced by normalizing whole-tree
    globs such as ``/**`` or ``/*`` (``_normalize_owned_path`` is left unchanged);
    it overlaps every root, including another empty root.
    """
    if not left or not right:
        # Repo-root sentinel: whole-tree ownership overlaps everything.
        return True
    if left == right:
        return True
    return left.startswith(f"{right}/") or right.startswith(f"{left}/")


def _depends_on_reaches(depends_on: dict[str, list[str]], start: str, target: str) -> bool:
    """True when *target* is reachable from *start* following depends_on edges."""
    if start == target:
        return True
    seen: set[str] = set()
    stack = [start]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for prereq in depends_on.get(current, []):
            if prereq == target:
                return True
            if prereq not in seen:
                stack.append(prereq)
    return False


def _validate_depends_on(depends_on: Any, lane_ids: set[str], path: Path) -> dict[str, list[str]]:
    """Validate optional top-level ``depends_on`` (referential + acyclic).

    Returns a normalized ``{lane_id: [prereq, ...]}`` map used by later checks.
    Absent / empty means no declared scheduling edges. Non-string prereq entries
    are rejected (stricter than the historical ``downstream`` hole).
    """
    if depends_on is None:
        return {}
    if not isinstance(depends_on, dict):
        raise RuntimeError(f"lane manifest depends_on must be an object when present: {path}")

    normalized: dict[str, list[str]] = {}
    for lane_id, prereqs in depends_on.items():
        if not isinstance(lane_id, str) or lane_id not in lane_ids:
            raise RuntimeError(f"lane manifest depends_on references unknown source lane '{lane_id}': {path}")
        if not isinstance(prereqs, list):
            raise RuntimeError(f"lane manifest depends_on for lane '{lane_id}' must be a list: {path}")
        clean: list[str] = []
        for prereq in prereqs:
            if not isinstance(prereq, str):
                raise RuntimeError(
                    f"lane manifest depends_on for lane '{lane_id}' entries must be strings: {path}"
                )
            if prereq not in lane_ids:
                raise RuntimeError(
                    f"lane manifest depends_on for lane '{lane_id}' references unknown lane(s) {[prereq]}: {path}"
                )
            clean.append(prereq)
        normalized[lane_id] = clean

    # Acyclicity (depends_on only): iterative 3-colour DFS with named back-edge.
    # Explicit stack avoids RecursionError on deep (valid or cyclic) graphs.
    color: dict[str, int] = {lane_id: 0 for lane_id in lane_ids}  # 0 white, 1 gray, 2 black

    for start in list(normalized):
        if color.get(start, 0) != 0:
            continue
        # Stack frames: (node, next prereq index to process)
        stack: list[tuple[str, int]] = [(start, 0)]
        color[start] = 1
        while stack:
            node, idx = stack[-1]
            prereqs = normalized.get(node, [])
            if idx < len(prereqs):
                stack[-1] = (node, idx + 1)
                prereq = prereqs[idx]
                prereq_color = color.get(prereq, 0)
                if prereq_color == 1:
                    raise RuntimeError(
                        f"lane manifest depends_on cycle detected (back-edge '{node}' -> '{prereq}'): {path}"
                    )
                if prereq_color == 0:
                    color[prereq] = 1
                    stack.append((prereq, 0))
            else:
                color[node] = 2
                stack.pop()

    return normalized


def _validate_owned_path_collision_freedom(
    lanes: dict[str, Any],
    depends_on: dict[str, list[str]],
    path: Path,
) -> None:
    """Refuse incomparable lanes whose normalized owned_paths roots overlap."""
    lane_roots: dict[str, list[str]] = {}
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        owned = lane.get("owned_paths", [])
        if not isinstance(owned, list):
            continue
        roots: list[str] = []
        for raw in owned:
            # Non-string entries are rejected in validate_manifest; skip defensively.
            if not isinstance(raw, str):
                continue
            # Keep empty normalized roots: they are the repo-root sentinel (/**, /*).
            roots.append(_normalize_owned_path(raw))
        if roots:
            lane_roots[lane_id] = roots

    lane_ids = sorted(lane_roots)
    for i, left_id in enumerate(lane_ids):
        for right_id in lane_ids[i + 1 :]:
            for left_root in lane_roots[left_id]:
                for right_root in lane_roots[right_id]:
                    if not _owned_path_roots_overlap(left_root, right_root):
                        continue
                    comparable = _depends_on_reaches(depends_on, left_id, right_id) or _depends_on_reaches(
                        depends_on, right_id, left_id
                    )
                    if comparable:
                        continue
                    if left_root == right_root:
                        overlap_path = left_root if left_root else "/"
                    elif not left_root:
                        overlap_path = right_root if right_root else "/"
                    elif not right_root:
                        overlap_path = left_root
                    else:
                        overlap_path = (
                            left_root if left_root.startswith(f"{right_root}/") else right_root
                        )
                    raise RuntimeError(
                        f"lane manifest owned_paths collision between lanes '{left_id}' and '{right_id}' "
                        f"on path '{overlap_path}': declare depends_on so one transitively depends on the "
                        f"other, or split owned_paths so roots do not overlap: {path}"
                    )


def _validate_state_key_conflict_freedom(
    lanes: dict[str, Any],
    depends_on: dict[str, list[str]],
    path: Path,
) -> None:
    """Refuse incomparable lanes with write/write or write/read overlap on a state key.

    State keys are opaque strings compared for exact equality. Read/read overlap
    is always accepted. A lane never conflicts with itself.
    """
    lane_writes: dict[str, set[str]] = {}
    lane_reads: dict[str, set[str]] = {}
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        writes = lane.get("state_writes")
        if isinstance(writes, list):
            lane_writes[lane_id] = {key for key in writes if isinstance(key, str)}
        reads = lane.get("state_reads")
        if isinstance(reads, list):
            lane_reads[lane_id] = {key for key in reads if isinstance(key, str)}

    lane_ids = sorted(set(lane_writes) | set(lane_reads))
    for i, left_id in enumerate(lane_ids):
        for right_id in lane_ids[i + 1 :]:
            left_writes = lane_writes.get(left_id, set())
            right_writes = lane_writes.get(right_id, set())
            left_reads = lane_reads.get(left_id, set())
            right_reads = lane_reads.get(right_id, set())
            # Conflict when one lane writes K and the other reads or writes K.
            conflict_keys = (left_writes & right_writes) | (left_writes & right_reads) | (right_writes & left_reads)
            if not conflict_keys:
                continue
            comparable = _depends_on_reaches(depends_on, left_id, right_id) or _depends_on_reaches(
                depends_on, right_id, left_id
            )
            if comparable:
                continue
            key = sorted(conflict_keys)[0]
            raise RuntimeError(
                f"lane manifest state key conflict between lanes '{left_id}' and '{right_id}' "
                f"on key '{key}': declare depends_on so one transitively depends on the "
                f"other, or split state_reads/state_writes so keys do not conflict: {path}"
            )


def manifest_metrics(manifest: dict[str, Any]) -> dict[str, Any]:
    """Pure metrics over a validated (or raw) manifest. Never mutates *manifest*.

    Density orientation (lane->prereq edges vs merge-order-forward closure):

        edge_set  = {(lane, prereq) for lane, prereqs in depends_on.items() for prereq in prereqs}
        closure   = {(u, v) for u,v in merge_order if index(v) > index(u)}
        declared  = {(lane, prereq) in edge_set if (prereq, lane) not in closure}

    Edges that merely mirror merge-order precedence are excluded from the declared
    count; edges against merge order remain. Metrics are never written into the
    manifest dict and must not be called from ``validate_manifest``.
    """
    depends_on = manifest.get("depends_on")
    if not isinstance(depends_on, dict):
        depends_on = {}
    merge_order_raw = manifest.get("merge_order")
    merge_order = [lane for lane in merge_order_raw if isinstance(lane, str)] if isinstance(merge_order_raw, list) else []
    index = {lane: i for i, lane in enumerate(merge_order)}

    edge_set: set[tuple[str, str]] = set()
    for lane, prereqs in depends_on.items():
        if not isinstance(lane, str) or not isinstance(prereqs, list):
            continue
        for prereq in prereqs:
            if isinstance(prereq, str):
                edge_set.add((lane, prereq))

    closure = {
        (u, v)
        for u in merge_order
        for v in merge_order
        if index[v] > index[u]
    }
    declared = {(lane, prereq) for lane, prereq in edge_set if (prereq, lane) not in closure}

    return {
        "depends_on_edge_count": len(edge_set),
        "depends_on_declared_count": len(declared),
        "declared_edges": sorted(declared),
    }


def _candidate_runtime_roots(
    lane: dict[str, Any],
    *,
    orchestrator_root: str,
) -> list[Path]:
    root = Path(orchestrator_root).expanduser().resolve()
    candidates: list[Path] = []

    def add_candidate(relative_path: str) -> None:
        normalized = _normalize_owned_path(relative_path)
        if not normalized:
            return
        full_path = (root / normalized).resolve()
        probe = full_path if full_path.is_dir() else full_path.parent
        for candidate in [probe, *probe.parents]:
            if candidate == root.parent:
                break
            if candidate == root or root in candidate.parents:
                if (candidate / "composer.json").is_file() or (candidate / "package.json").is_file():
                    candidates.append(candidate)
                    break

    app_root = lane.get("app_root")
    if isinstance(app_root, str) and app_root.strip():
        add_candidate(app_root)

    owned_paths = lane.get("owned_paths", [])
    if isinstance(owned_paths, list):
        for owned_path in owned_paths:
            add_candidate(str(owned_path))

    tooling_paths = lane.get("tooling_paths", [])
    if isinstance(tooling_paths, list):
        for tooling_path in tooling_paths:
            tooling_str = str(tooling_path).strip()
            if tooling_str:
                add_candidate(str(Path(tooling_str).parent))

    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate not in seen:
            unique.append(candidate)
            seen.add(candidate)
    return unique


def _derive_runtime_preflight(
    lane: dict[str, Any],
    *,
    orchestrator_root: str,
) -> dict[str, Any]:
    roots = _candidate_runtime_roots(lane, orchestrator_root=orchestrator_root)
    if not roots:
        return {
            "capability_tags": [],
            "preflight_commands": [],
            "preflight_failure_summary": None,
            "preflight_failure_details": None,
        }

    root = Path(orchestrator_root).expanduser().resolve()
    capability_tags: list[str] = []
    commands: list[str] = []
    for runtime_root in roots:
        relative_root = str(runtime_root.relative_to(root))
        if (runtime_root / "composer.json").is_file():
            capability_tags.append("php-tooling-ready")
            commands.append(f"cd {relative_root} && test -f vendor/autoload.php")
        if (runtime_root / "package.json").is_file():
            capability_tags.append("node-tooling-ready")
            commands.append(f"cd {relative_root} && test -d node_modules")

    unique_tags = list(dict.fromkeys(tag for tag in capability_tags if tag))
    unique_commands = list(dict.fromkeys(command for command in commands if command))
    if not unique_commands:
        return {
            "capability_tags": unique_tags,
            "preflight_commands": [],
            "preflight_failure_summary": None,
            "preflight_failure_details": None,
        }

    return {
        "capability_tags": unique_tags,
        "preflight_commands": unique_commands,
        "preflight_failure_summary": "lane runtime preflight failed; lane-local dependencies are not ready.",
        "preflight_failure_details": (
            "This lane references package-managed application roots. Lane bootstrap should provision "
            "lane-local Composer vendor directories and any required Node dependencies before worker "
            "execution. If these prerequisites are still missing, fix bootstrap or install dependencies "
            "before dispatching a model turn."
        ),
    }


def _derive_routing_from_owned_paths(lanes: dict[str, Any]) -> list[tuple[str, str]]:
    derived: list[tuple[str, str]] = []
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        owned_paths = lane.get("owned_paths", [])
        if not isinstance(owned_paths, list):
            continue
        for raw in owned_paths:
            value = str(raw).strip()
            if not value:
                continue
            if value.endswith("/**"):
                value = value[:-3].rstrip("/") + "/"
            derived.append((value, lane_id))
    return list(dict.fromkeys(derived))


def validate_manifest(data: dict[str, Any], path: Path) -> dict[str, Any]:
    for key in REQUIRED_TOP_LEVEL_KEYS:
        _require_key(data, key, path=path, context="top-level")

    # Compat: unknown top-level keys are not refused, but warn so metrics keys
    # merged into the dict before save are visible (serialization hygiene).
    unknown_top_level = sorted(key for key in data if key not in KNOWN_TOP_LEVEL_KEYS)
    if unknown_top_level:
        logger.warning(
            "lane manifest has unknown top-level key(s) %s (passed through unchanged): %s",
            unknown_top_level,
            path,
        )

    merge_order = data.get("merge_order")
    lanes = data.get("lanes")
    downstream = data.get("downstream")
    if not isinstance(merge_order, list):
        raise RuntimeError(f"lane manifest merge_order must be a list: {path}")
    if not isinstance(lanes, dict):
        raise RuntimeError(f"lane manifest lanes must be an object: {path}")
    if not isinstance(downstream, dict):
        raise RuntimeError(f"lane manifest downstream must be an object: {path}")
    task_plan_path = data.get("task_plan_path")
    heading_to_lane = data.get("heading_to_lane")
    plan_routing_hints = data.get("plan_routing_hints")
    if task_plan_path is not None and (not isinstance(task_plan_path, str) or not task_plan_path.strip()):
        raise RuntimeError(f"lane manifest task_plan_path must be a non-empty string when present: {path}")
    if heading_to_lane is not None and not isinstance(heading_to_lane, dict):
        raise RuntimeError(f"lane manifest heading_to_lane must be an object when present: {path}")
    if plan_routing_hints is not None and not isinstance(plan_routing_hints, list):
        raise RuntimeError(f"lane manifest plan_routing_hints must be a list when present: {path}")

    lane_ids = set(lane_id for lane_id in lanes.keys() if isinstance(lane_id, str))
    unknown_merge_order = [lane_id for lane_id in merge_order if isinstance(lane_id, str) and lane_id not in lane_ids]
    if unknown_merge_order:
        raise RuntimeError(f"lane manifest merge_order references unknown lane(s) {unknown_merge_order}: {path}")

    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            raise RuntimeError(f"lane manifest lanes entries must be string->object pairs: {path}")
        for key in REQUIRED_LANE_KEYS:
            _require_key(lane, key, path=path, context=f"lane '{lane_id}'")
        if not isinstance(lane.get("branch"), str) or not str(lane.get("branch")).strip():
            raise RuntimeError(f"lane '{lane_id}' must define a non-empty branch: {path}")
        if not isinstance(lane.get("worktree_path"), str) or not str(lane.get("worktree_path")).strip():
            raise RuntimeError(f"lane '{lane_id}' must define a non-empty worktree_path: {path}")
        if not isinstance(lane.get("owned_paths"), list):
            raise RuntimeError(f"lane '{lane_id}' owned_paths must be a list: {path}")
        for owned_entry in lane.get("owned_paths", []):
            if not isinstance(owned_entry, str):
                raise RuntimeError(
                    f"lane '{lane_id}' owned_paths entries must be strings: {path}"
                )
            try:
                _normalize_owned_path(owned_entry)
            except ValueError as exc:
                raise RuntimeError(
                    f"lane '{lane_id}' owned_paths entry must be repo-relative "
                    f"(absolute or escaping path refused): {path}"
                ) from exc
        if "state_reads" in lane and not isinstance(lane.get("state_reads"), list):
            raise RuntimeError(f"lane '{lane_id}' state_reads must be a list when present: {path}")
        if "state_reads" in lane:
            for entry in lane.get("state_reads", []):
                if not isinstance(entry, str):
                    raise RuntimeError(f"lane '{lane_id}' state_reads entries must be strings: {path}")
        if "state_writes" in lane and not isinstance(lane.get("state_writes"), list):
            raise RuntimeError(f"lane '{lane_id}' state_writes must be a list when present: {path}")
        if "state_writes" in lane:
            for entry in lane.get("state_writes", []):
                if not isinstance(entry, str):
                    raise RuntimeError(f"lane '{lane_id}' state_writes entries must be strings: {path}")
        if not isinstance(lane.get("test_commands"), list):
            raise RuntimeError(f"lane '{lane_id}' test_commands must be a list: {path}")
        if "commit_paths" in lane and not isinstance(lane.get("commit_paths"), list):
            raise RuntimeError(f"lane '{lane_id}' commit_paths must be a list when present: {path}")
        if "tooling_paths" in lane and not isinstance(lane.get("tooling_paths"), list):
            raise RuntimeError(f"lane '{lane_id}' tooling_paths must be a list when present: {path}")
        if "capability_tags" in lane and not isinstance(lane.get("capability_tags"), list):
            raise RuntimeError(f"lane '{lane_id}' capability_tags must be a list when present: {path}")
        if "preflight_commands" in lane and not isinstance(lane.get("preflight_commands"), list):
            raise RuntimeError(f"lane '{lane_id}' preflight_commands must be a list when present: {path}")
        if "preferred_model" in lane and (
            not isinstance(lane.get("preferred_model"), str) or not str(lane.get("preferred_model")).strip()
        ):
            raise RuntimeError(f"lane '{lane_id}' preferred_model must be a non-empty string when present: {path}")
        if "preferred_backend" in lane and (
            not isinstance(lane.get("preferred_backend"), str) or not str(lane.get("preferred_backend")).strip()
        ):
            raise RuntimeError(f"lane '{lane_id}' preferred_backend must be a non-empty string when present: {path}")
        if "preferred_reasoning_effort" in lane:
            from _env import CODEX_REASONING_EFFORTS

            effort_val = lane.get("preferred_reasoning_effort")
            if not isinstance(effort_val, str) or effort_val.strip().lower() not in CODEX_REASONING_EFFORTS:
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_reasoning_effort must be one of {CODEX_REASONING_EFFORTS} when present: {path}"
                )
        if "preflight_failure_summary" in lane and (
            not isinstance(lane.get("preflight_failure_summary"), str)
            or not str(lane.get("preflight_failure_summary")).strip()
        ):
            raise RuntimeError(
                f"lane '{lane_id}' preflight_failure_summary must be a non-empty string when present: {path}"
            )
        if "preflight_failure_details" in lane and (
            not isinstance(lane.get("preflight_failure_details"), str)
            or not str(lane.get("preflight_failure_details")).strip()
        ):
            raise RuntimeError(
                f"lane '{lane_id}' preflight_failure_details must be a non-empty string when present: {path}"
            )
        if "guidance_fallbacks" in lane and not isinstance(lane.get("guidance_fallbacks"), list):
            raise RuntimeError(f"lane '{lane_id}' guidance_fallbacks must be a list when present: {path}")
        if "grants" in lane:
            _validate_grants(lane_id, lane.get("grants"), path)

    for lane_id, dependents in downstream.items():
        if lane_id not in lane_ids:
            raise RuntimeError(f"lane manifest downstream references unknown source lane '{lane_id}': {path}")
        if not isinstance(dependents, list):
            raise RuntimeError(f"lane manifest downstream for lane '{lane_id}' must be a list: {path}")
        unknown_dependents = [dep for dep in dependents if isinstance(dep, str) and dep not in lane_ids]
        if unknown_dependents:
            raise RuntimeError(
                f"lane manifest downstream for lane '{lane_id}' references unknown lane(s) {unknown_dependents}: {path}"
            )

    # Optional top-level depends_on (scheduling). Independent of downstream; not
    # required. Default absence means no declared prerequisites.
    depends_on_normalized = _validate_depends_on(data.get("depends_on"), lane_ids, path)
    _validate_owned_path_collision_freedom(lanes, depends_on_normalized, path)
    _validate_state_key_conflict_freedom(lanes, depends_on_normalized, path)

    if isinstance(heading_to_lane, dict):
        for heading, lane_id in heading_to_lane.items():
            if not isinstance(heading, str) or not heading.strip():
                raise RuntimeError(f"lane manifest heading_to_lane keys must be non-empty strings: {path}")
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(f"lane manifest heading_to_lane references unknown lane '{lane_id}': {path}")

    if isinstance(plan_routing_hints, list):
        for index, hint in enumerate(plan_routing_hints):
            if not isinstance(hint, dict):
                raise RuntimeError(f"lane manifest plan_routing_hints entries must be objects: {path}")
            lane_id = hint.get("lane")
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}] references unknown lane '{lane_id}': {path}"
                )
            heading = hint.get("heading")
            text_prefix = hint.get("text_prefix")
            contains = hint.get("contains")
            if heading is not None and (not isinstance(heading, str) or not heading.strip()):
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}].heading must be a non-empty string when present: {path}"
                )
            if text_prefix is not None and (not isinstance(text_prefix, str) or not text_prefix.strip()):
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}].text_prefix must be a non-empty string when present: {path}"
                )
            if contains is not None and (not isinstance(contains, str) or not contains.strip()):
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}].contains must be a non-empty string when present: {path}"
                )
            if heading is None and text_prefix is None and contains is None:
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}] must define at least one matcher field: {path}"
                )

    return data


def load_manifest(
    task_ref: str,
    *,
    orchestrator_root: str | None = None,
    manifest_dir: str | Path | None = None,
) -> dict[str, Any]:
    path = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir) / f"{task_ref}.json"
    if not path.exists():
        raise FileNotFoundError(f"lane manifest not found for task {task_ref}: {path}")
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise RuntimeError(f"lane manifest must be a JSON object: {path}")
    return validate_manifest(data, path)


def save_manifest(
    manifest: dict[str, Any],
    *,
    orchestrator_root: str | None = None,
    manifest_dir: str | Path | None = None,
) -> Path:
    """Validate and persist a lane manifest under config/lane-orchestration/."""
    resolved_dir = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    task_ref = str(manifest.get("task_ref") or "").strip()
    if not task_ref:
        raise RuntimeError("lane manifest must include task_ref before save")
    path = resolved_dir / f"{task_ref}.json"
    validated = validate_manifest(manifest, path)
    resolved_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(validated, indent=2) + "\n", encoding="utf-8")
    return path


def list_lanes(
    task_ref: str, *, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None
) -> list[str]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    lanes = manifest.get("lanes", {})
    if not isinstance(lanes, dict):
        return []
    return sorted(lanes.keys())


def _lane_manifest(
    task_ref: str,
    lane_id: str,
    *,
    orchestrator_root: str | None = None,
    manifest_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    lanes = load_manifest(task_ref, orchestrator_root=orchestrator_root, manifest_dir=manifest_dir).get("lanes", {})
    if not isinstance(lanes, dict):
        return None
    lane = lanes.get(lane_id)
    return lane if isinstance(lane, dict) else None


def expand_path_template(template: str, *, orchestrator_root: str) -> str:
    return template.replace("{orchestrator_root}", orchestrator_root)


def get_lane_config(task_ref: str, lane_id: str, *, orchestrator_root: str | None = None) -> dict[str, Any] | None:
    lane = _lane_manifest(task_ref, lane_id, orchestrator_root=orchestrator_root)
    if lane is None:
        return None
    result = dict(lane)
    owned_paths = [str(item) for item in result.get("owned_paths", []) if str(item).strip()]
    commit_paths = result.get("commit_paths")
    if not isinstance(commit_paths, list) or not commit_paths:
        result["commit_paths"] = _derive_commit_paths(owned_paths)
    result.setdefault("tooling_paths", [])
    result.setdefault("capability_tags", [])
    result.setdefault("preflight_commands", [])
    result.setdefault("guidance_fallbacks", [])
    result.setdefault("preferred_model", None)
    result.setdefault("preferred_backend", None)
    result.setdefault("preferred_reasoning_effort", None)
    result.setdefault("token_burn_threshold", 2_000_000)
    result.setdefault("model_context_window", 128_000)
    if orchestrator_root:
        derived_preflight = _derive_runtime_preflight(result, orchestrator_root=orchestrator_root)
        if not result.get("capability_tags"):
            result["capability_tags"] = derived_preflight["capability_tags"]
        if not result.get("preflight_commands"):
            result["preflight_commands"] = derived_preflight["preflight_commands"]
        if not result.get("preflight_failure_summary") and derived_preflight["preflight_failure_summary"]:
            result["preflight_failure_summary"] = derived_preflight["preflight_failure_summary"]
        if not result.get("preflight_failure_details") and derived_preflight["preflight_failure_details"]:
            result["preflight_failure_details"] = derived_preflight["preflight_failure_details"]
    if orchestrator_root and isinstance(result.get("worktree_path"), str):
        result["worktree_path"] = expand_path_template(result["worktree_path"], orchestrator_root=orchestrator_root)
    return result


def infer_lane_from_branch(branch: str, task_ref: str | None = None, *, orchestrator_root: str | None = None) -> str:
    if not branch:
        return ""

    task_refs = [task_ref] if task_ref else list_task_refs(orchestrator_root=orchestrator_root)
    matches: list[str] = []
    for candidate_task in task_refs:
        if not candidate_task:
            continue
        manifest = load_manifest(candidate_task, orchestrator_root=orchestrator_root)
        lanes = manifest.get("lanes", {})
        if not isinstance(lanes, dict):
            continue
        for lane_id, lane in lanes.items():
            if not isinstance(lane, dict):
                continue
            if lane.get("branch") == branch:
                matches.append(lane_id)
    unique = sorted(set(matches))
    if len(unique) == 1:
        return unique[0]
    return ""


def infer_task_from_branch_or_worktree(
    branch: str,
    *,
    worktree_path: str | None = None,
    orchestrator_root: str | None = None,
) -> str:
    candidates: list[str] = []
    normalized_worktree = str(Path(worktree_path).expanduser().resolve()) if worktree_path else ""

    for candidate_task in list_task_refs(orchestrator_root=orchestrator_root):
        manifest = load_manifest(candidate_task, orchestrator_root=orchestrator_root)
        lanes = manifest.get("lanes", {})
        if not isinstance(lanes, dict):
            continue
        for _lane_id, lane in lanes.items():
            if not isinstance(lane, dict):
                continue
            lane_branch = str(lane.get("branch", "")).strip()
            lane_worktree = str(lane.get("worktree_path", "")).strip()
            if orchestrator_root and lane_worktree:
                lane_worktree = expand_path_template(lane_worktree, orchestrator_root=orchestrator_root)
            lane_worktree_resolved = str(Path(lane_worktree).expanduser().resolve()) if lane_worktree else ""
            if branch and lane_branch == branch:
                candidates.append(candidate_task)
            elif normalized_worktree and lane_worktree_resolved == normalized_worktree:
                candidates.append(candidate_task)

    unique = sorted(set(candidates))
    if len(unique) == 1:
        return unique[0]
    return ""


def route_patterns(task_ref: str, *, orchestrator_root: str | None = None) -> list[tuple[str, str]]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    routes = manifest.get("routing", [])
    lanes = manifest.get("lanes", {})
    if not isinstance(routes, list) or not routes:
        return _derive_routing_from_owned_paths(lanes if isinstance(lanes, dict) else {})
    patterns: list[tuple[str, str]] = []
    for route in routes:
        if not isinstance(route, dict):
            continue
        prefix = route.get("prefix")
        lane = route.get("lane")
        if isinstance(prefix, str) and isinstance(lane, str):
            patterns.append((prefix, lane))
    return patterns


def lane_route_hints(task_ref: str, *, orchestrator_root: str | None = None) -> dict[str, tuple[str, ...]]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    lanes = manifest.get("lanes", {})
    if not isinstance(lanes, dict):
        return {}

    hints: dict[str, tuple[str, ...]] = {}
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        values: list[str] = [lane_id]
        title = lane.get("title")
        branch = lane.get("branch")
        worktree_path = lane.get("worktree_path")
        route_hints_value = lane.get("route_hints", [])
        if isinstance(title, str) and title.strip():
            values.append(title)
            values.append(title.lower())
        if isinstance(branch, str) and branch.strip():
            values.append(branch)
        if isinstance(worktree_path, str) and worktree_path.strip():
            values.append(worktree_path)
            values.append(Path(worktree_path).name)
        if isinstance(route_hints_value, list):
            values.extend(str(item) for item in route_hints_value if str(item).strip())
        normalized = tuple(dict.fromkeys(value for value in values if value and value.strip()))
        hints[lane_id] = normalized
    return hints


def merge_order(task_ref: str, *, orchestrator_root: str | None = None) -> list[str]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    order = manifest.get("merge_order", [])
    if not isinstance(order, list):
        return []
    return [lane for lane in order if isinstance(lane, str)]


def downstream_lanes(task_ref: str, lane_id: str, *, orchestrator_root: str | None = None) -> list[str]:
    """Return the declared downstream dependents for *lane_id*, or ``[]``."""
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    downstream = manifest.get("downstream", {})
    if not isinstance(downstream, dict):
        return []
    deps = downstream.get(lane_id, [])
    if not isinstance(deps, list):
        return []
    return [d for d in deps if isinstance(d, str)]


def guidance_fallbacks(task_ref: str, lane_id: str, *, orchestrator_root: str | None = None) -> list[dict[str, Any]]:
    lane = _lane_manifest(task_ref, lane_id, orchestrator_root=orchestrator_root)
    if lane is None:
        return []
    fallbacks = lane.get("guidance_fallbacks", [])
    if not isinstance(fallbacks, list):
        return []
    return [row for row in fallbacks if isinstance(row, dict)]


def task_plan_path(task_ref: str, *, orchestrator_root: str | None = None) -> str:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    raw = manifest.get("task_plan_path")
    if not isinstance(raw, str) or not raw.strip():
        return ""
    path = Path(raw)
    if path.is_absolute():
        return str(path)
    base = Path(orchestrator_root) if orchestrator_root else REPO_ROOT
    return str((base / path).expanduser())


def heading_to_lane(task_ref: str, *, orchestrator_root: str | None = None) -> dict[str, str]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    value = manifest.get("heading_to_lane")
    if not isinstance(value, dict):
        return {}
    result: dict[str, str] = {}
    for heading, lane_id in value.items():
        if isinstance(heading, str) and heading.strip() and isinstance(lane_id, str) and lane_id.strip():
            result[heading] = lane_id
    return result


def plan_routing_hints(task_ref: str, *, orchestrator_root: str | None = None) -> list[dict[str, str]]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    value = manifest.get("plan_routing_hints")
    if not isinstance(value, list):
        return []
    rows: list[dict[str, str]] = []
    for hint in value:
        if not isinstance(hint, dict):
            continue
        row: dict[str, str] = {}
        for key in ("heading", "text_prefix", "contains", "lane"):
            cell = hint.get(key)
            if isinstance(cell, str) and cell.strip():
                row[key] = cell
        if "lane" in row:
            rows.append(row)
    return rows
