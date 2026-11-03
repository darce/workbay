#!/usr/bin/env python3
from __future__ import annotations

import copy
import errno
import hashlib
import json
import logging
import os
import posixpath
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

from workbay_orchestrator_mcp.orchestration import worktree_root
from workbay_orchestrator_mcp.orchestration.lane_postmerge import TERMINAL_STATUSES
from workbay_orchestrator_mcp.orchestration.lane_routing import MANIFEST_ROUTING_KEYS

try:
    from workbay_orchestrator_mcp.orchestration._env import WORKER_REASONING_EFFORT_CHOICES
    from workbay_orchestrator_mcp.orchestration.codex_lane_config import CODEX_MODEL_TIERS
except ModuleNotFoundError:  # bare-script compatibility
    from _env import WORKER_REASONING_EFFORT_CHOICES
    from codex_lane_config import CODEX_MODEL_TIERS

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[4]
DEFAULT_MANIFEST_DIR = REPO_ROOT / "config" / "lane-orchestration"
MANIFEST_DIR = DEFAULT_MANIFEST_DIR

logger = logging.getLogger(__name__)

MANIFEST_BYTES_LIMIT = 1024 * 1024
MANIFEST_ADMISSION_HIGH_WATER_BYTES = MANIFEST_BYTES_LIMIT * 90 // 100

EVICTABLE_TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})

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
        "landing_policy",
        "default_done_definition",
        # implementation note S2 — conflict exclusion relation + colour certificate.
        "conflict_edges",
        "colour_classes",
        # Wave integration target: non-empty string or non-empty list of strings.
        "integration_branch",
    }
)

_FULL_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
# Deterministic multi-process RMW test seam (row 30): when set to a directory,
# atomic_update_manifest signals ready after load/mutate and waits for "go"
# before save.
_MANIFEST_RMW_BARRIER_ENV = "WORKBAY_TEST_MANIFEST_RMW_BARRIER"
# Named bound for the sidecar flock (RES-13). Create (``save_manifest``) and
# update (``atomic_update_manifest``) both wait at most this long unless the
# caller passes ``lock_timeout``. Expiry raises ``ManifestLockTimeoutError``
# and does not publish.
DEFAULT_MANIFEST_LOCK_TIMEOUT_SECONDS = 120.0
_manifest_lock_depth = threading.local()
_OWNED_PATH_COLLISION_PAIR_RE = re.compile(r"lane manifest owned_paths collision between lanes '([^']+)' and '([^']+)'")


class ManifestLockTimeoutError(TimeoutError):
    """The manifest sidecar lock was not acquired before the deadline.

    Machine-readable fields are ``lock_path`` and ``timeout_seconds`` (API-05).
    ``save_manifest`` and ``atomic_update_manifest`` raise this same type.
    Neither falls back to an unlocked write.
    """

    def __init__(self, lock_path: Path, timeout_seconds: float) -> None:
        self.lock_path = lock_path
        self.timeout_seconds = timeout_seconds
        super().__init__(f"lane manifest lock timed out after {timeout_seconds:.3f}s: {lock_path}")


class ManifestBudgetExhausted(RuntimeError):
    """A growth or lane-admission refusal; the existing file is untouched."""

    def __init__(self, path: Path, new_bytes: int, current_bytes: int, limit: int, *, reason: str) -> None:
        self.path = path
        self.new_bytes = new_bytes
        self.current_bytes = current_bytes
        self.limit = limit
        self.reason = reason
        super().__init__(
            f"{reason}: lane manifest {path}: new_bytes={new_bytes}, current_bytes={current_bytes}, "
            f"limit={limit}; use shrink_manifest(path, row_status_of_locked_lanes=...) to compact eligible "
            "terminal lanes, then retry. "
            "Shrinking may not free enough space for this write."
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "manifest_path": str(self.path),
            "new_bytes": self.new_bytes,
            "current_bytes": self.current_bytes,
            "limit": self.limit,
        }


def _manifest_lock_timeout_seconds(timeout: float | None) -> float:
    if timeout is None:
        return DEFAULT_MANIFEST_LOCK_TIMEOUT_SECONDS
    return max(0.0, float(timeout))


def _manifest_lock_key(path: Path | str) -> str:
    return str(Path(path).expanduser().resolve())


def _manifest_lock_depths() -> dict[str, int]:
    depths = getattr(_manifest_lock_depth, "depths", None)
    if depths is None:
        depths = {}
        _manifest_lock_depth.depths = depths
    return depths


@contextmanager
def manifest_write_lock(path: Path | str, *, timeout: float | None = None):
    """Hold ``<path>.lock`` for one manifest read-modify-write or create.

    A second ``flock`` on a new fd from this thread would deadlock on Linux,
    so same-thread re-entry reuses the held lock instead of locking again.
    """
    import fcntl

    manifest_path = Path(path)
    key = _manifest_lock_key(manifest_path)
    depths = _manifest_lock_depths()
    if depths.get(key, 0) > 0:
        depths[key] += 1
        try:
            yield
        finally:
            depths[key] -= 1
        return

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = Path(str(manifest_path) + ".lock")
    timeout_seconds = _manifest_lock_timeout_seconds(timeout)
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    deadline = time.monotonic() + timeout_seconds
    acquired = False
    try:
        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ManifestLockTimeoutError(lock_path, timeout_seconds) from None
                time.sleep(min(0.01, remaining))
        depths[key] = 1
        try:
            yield
        finally:
            depths.pop(key, None)
    finally:
        if acquired:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        os.close(lock_fd)


def _atomic_publish_manifest(manifest_path: Path, payload: bytes) -> None:
    """Publish ``payload`` via a same-dir temp file, fsync, and ``os.replace``.

    Shared by ``save_manifest`` and ``atomic_update_manifest``. A reader that
    does not hold the sidecar lock still observes only complete documents.
    """
    import tempfile

    parent = manifest_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{manifest_path.name}.",
        suffix=".tmp",
        dir=str(parent),
    )
    try:
        with os.fdopen(tmp_fd, "wb") as tmp_file:
            tmp_file.write(payload)
            tmp_file.flush()
            try:
                os.fsync(tmp_file.fileno())
            except OSError:
                pass
        os.replace(tmp_name, str(manifest_path))
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _manifest_lane_ids(manifest: dict[str, Any]) -> set[str]:
    lanes = manifest.get("lanes")
    return set(lanes) if isinstance(lanes, dict) else set()


def _encode_and_publish_manifest(
    path: Path, manifest: dict[str, Any], *, current_bytes: int, prior_lane_ids: set[str]
) -> tuple[dict[str, Any], bytes]:
    """Validate, encode and gate every publication while the sidecar lock is held.

    Validate before normalizing legacy markers, and work on a private candidate
    so a refused save cannot change the caller's document. Lifecycle writes
    that do not grow remain possible even for an already oversized manifest.
    """
    candidate = validate_manifest(copy.deepcopy(manifest), path)

    def encode() -> bytes:
        return (json.dumps(candidate, separators=(",", ":")) + "\n").encode("utf-8")

    def normalize_dropped() -> bool:
        changed = False
        for lane in candidate["lanes"].values():
            if "compacted" in lane:
                changed |= bool(lane["compacted"]["dropped"])
                lane["compacted"]["dropped"] = []
        return changed

    payload = encode()
    needs_recovery = len(payload) > MANIFEST_BYTES_LIMIT and len(payload) > current_bytes
    normalized = normalize_dropped()
    if needs_recovery or normalized:
        # One compact re-encoding recovery attempt. This removes legacy dropped
        # payloads, but deliberately grants no blanket lifecycle exception.
        payload = encode()
    new_bytes = len(payload)
    if new_bytes > MANIFEST_BYTES_LIMIT and new_bytes > current_bytes:
        raise ManifestBudgetExhausted(
            path, new_bytes, current_bytes, MANIFEST_BYTES_LIMIT, reason="manifest_budget_exhausted"
        )
    if new_bytes > MANIFEST_ADMISSION_HIGH_WATER_BYTES and _manifest_lane_ids(candidate) - prior_lane_ids:
        raise ManifestBudgetExhausted(
            path, new_bytes, current_bytes, MANIFEST_BYTES_LIMIT, reason="manifest_admission_high_water"
        )
    _atomic_publish_manifest(path, payload)
    if new_bytes > MANIFEST_ADMISSION_HIGH_WATER_BYTES:
        terminal = sum("terminal" in lane or "landed" in lane for lane in candidate["lanes"].values())
        logger.warning(
            "lane manifest above high-water: path=%s bytes=%s limit=%s live=%s terminal=%s",
            path,
            new_bytes,
            MANIFEST_BYTES_LIMIT,
            len(candidate["lanes"]) - terminal,
            terminal,
        )
    return candidate, payload


def _ensure_task_plan_required_docs(manifest: dict[str, Any]) -> None:
    """Derive each lane's task-plan document from the manifest-level path."""
    task_plan_path = manifest.get("task_plan_path")
    lanes = manifest.get("lanes")
    if not isinstance(task_plan_path, str) or not task_plan_path.strip() or not isinstance(lanes, dict):
        return

    for lane_id, lane in lanes.items():
        if not isinstance(lane, dict):
            continue
        # Compact terminal entries deliberately discard per-lane planning
        # detail. Do not inflate them again from the manifest-level fallback.
        if isinstance(lane.get("compacted"), dict):
            continue
        if "required_docs" not in lane:
            lane["required_docs"] = []
        required_docs = lane["required_docs"]
        if not isinstance(required_docs, list):
            raise RuntimeError(f"lane '{lane_id}' required_docs must be a list when task_plan_path is set")

        normalized_docs: list[Any] = []
        task_plan_seen = False
        for document in required_docs:
            if document == task_plan_path:
                if task_plan_seen:
                    continue
                task_plan_seen = True
            normalized_docs.append(document)
        if not task_plan_seen:
            normalized_docs.append(task_plan_path)
        lane["required_docs"] = normalized_docs


class TerminalLaneBlock(TypedDict):
    lane_id: str
    pair: list[str]


class TerminalLaneReconciliation(TypedDict):
    stamped: list[str]
    cleared: list[str]
    compacted: list[str]
    unknown: list[str]
    blocked: list[TerminalLaneBlock]


class ReclaimEvidence(TypedDict):
    worktree_absent: bool
    branch_absent: bool
    cleanup_intent_pending: bool


class LaneEvictionRefusal(TypedDict):
    lane_id: str
    reason: str


class LaneEvictionPlan(TypedDict):
    evict: list[str]
    refused: list[LaneEvictionRefusal]
    expected_lane_fingerprints: dict[str, str]


class LaneEvictionContraction(TypedDict):
    removed_ids: list[str]
    edges_dropped: dict[str, list[list[str]]]
    edges_added: dict[str, list[list[str]]]
    entries_dropped: dict[str, list[Any]]


class LaneEvictionResult(TypedDict):
    evicted: list[str]
    refused: list[LaneEvictionRefusal]
    bytes_before: int
    bytes_after: int
    contraction: LaneEvictionContraction


class LaneEvictionRefused(ValueError):
    """A pure terminal contraction refused one or more requested lane IDs."""

    def __init__(self, refusals: Sequence[LaneEvictionRefusal]) -> None:
        self.refusals = [dict(refusal) for refusal in refusals]
        super().__init__(f"lane eviction refused: {self.refusals}")


_COMPACTED_LANE_KEYS = (
    frozenset(
        {
            "branch",
            "worktree_path",
            "owned_paths",
            "commit_paths",
            "base_sha",
            "terminal",
            "landed",
            "released",
            "retired",
            "title",
            "review_subject",
            "test_commands",
        }
    )
    | MANIFEST_ROUTING_KEYS
)


def _needs_terminal_compaction(lane: dict[str, Any], status: str | None, no_work_released: bool) -> bool:
    """Return whether reconciliation has durable evidence to compact this lane."""
    if no_work_released or "compacted" in lane:
        return False
    if status is None:
        return "terminal" in lane or "landed" in lane
    return status in TERMINAL_STATUSES


def _compact_terminal_lane(lane: dict[str, Any], *, at: str) -> None:
    """Replace a terminal lane entry with the identity needed by later readers."""
    compacted = {
        key: value for key, value in lane.items() if key in _COMPACTED_LANE_KEYS or key.startswith("ownership_retired_")
    }
    compacted["test_commands"] = []
    compacted["compacted"] = {"at": at, "dropped": []}
    lane.clear()
    lane.update(compacted)


def _lane_row_state(row_state: Any) -> tuple[str | None, bool]:
    """Project one ``worktree_lanes`` reader value to ``(status, no_work_released)``."""
    if isinstance(row_state, str):
        return row_state, False
    if isinstance(row_state, dict):
        status = row_state.get("status")
        released = row_state.get("released") is True and status == "planned"
        return (status if isinstance(status, str) else None), released
    return None, False


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
    # Reject absolute roots before removing a trailing glob. In particular,
    # ``/**`` and ``/`` must not collapse into the empty repo-root sentinel.
    if re.match(r"^[A-Za-z]:(/|$)", value):
        raise ValueError(f"owned_path must be repo-relative (drive-absolute path refused): {path!r}")
    if value.startswith("/"):
        raise ValueError(f"owned_path must be repo-relative (absolute path refused): {path!r}")
    # Reject escapes in the original spelling as well as the glob-stripped
    # spelling below, so a suffix cannot hide a parent walk.
    normalized_before_glob = posixpath.normpath(value)
    if normalized_before_glob == ".." or normalized_before_glob.startswith("../"):
        raise ValueError(f"owned_path escapes the repo root (refused): {path!r}")
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
    # Lexical only: collapse // and ., resolve .. segments; never touch the FS.
    normalized = posixpath.normpath(value)
    if normalized == ".":
        # Explicit repo-root spellings ("." / "./") keep the whole-tree
        # sentinel; a ``..`` parent-walk that merely COLLAPSES to root is a
        # mistyped path, not intent — silently granting whole-tree ownership
        # would fail open in collision checks.
        if ".." in value.split("/"):
            raise ValueError(f"owned_path collapses to the repo root via '..' (refused): {path!r}")
        return ""
    # Repo-relative contract: refuse absolute roots and residual ``..`` escape.
    if normalized.startswith("/") or normalized == ".." or normalized.startswith("../"):
        raise ValueError(f"owned_path must be repo-relative (absolute or escaping path refused): {path!r}")
    return normalized


def _is_review_scratch_path(path: str) -> bool:
    """Return whether a repo-relative path is review scratch.

    Review diffs use the same ``.review/`` prefix exclusion as review_runner.
    The root itself is also scratch when it appears as an owned-path root.
    """
    return path == ".review" or path.startswith(".review/")


def _owned_path_roots_overlap(left: str, right: str) -> bool:
    """True when two normalized owned-path roots collide on ``/`` boundaries.

    Equal roots overlap. A parent/child pair overlaps only when the longer path
    continues past a full ``/`` component of the shorter one — so ``packages/a``
    vs ``packages/a/sub`` overlaps, but ``packages/a`` vs ``packages/ab`` does not.

    The empty string is the repo-root sentinel produced by explicit root
    spellings (``""``, ``"."`` or ``"./"``); it overlaps every root, including
    another empty root. Absolute root globs are rejected during normalization.
    """
    if _is_review_scratch_path(left) or _is_review_scratch_path(right):
        return False
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
                raise RuntimeError(f"lane manifest depends_on for lane '{lane_id}' entries must be strings: {path}")
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


def _conflict_edge_pair_set(conflict_edges: Any) -> set[frozenset[str]]:
    """Return undirected pairs from a validated-or-raw conflict_edges list."""
    pairs: set[frozenset[str]] = set()
    if not isinstance(conflict_edges, list):
        return pairs
    for entry in conflict_edges:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            continue
        a, b = entry[0], entry[1]
        if isinstance(a, str) and isinstance(b, str) and a and b and a != b:
            pairs.add(frozenset((a, b)))
    return pairs


def _validate_conflict_keys(
    data: dict[str, Any],
    lane_ids: set[str],
    path: Path,
) -> tuple[set[frozenset[str]], dict[str, int]]:
    """Structural checks for ``conflict_edges`` + ``colour_classes`` (implementation note S2).

    (a) every edge endpoint must be a known lane;
    (b) a monochrome conflict edge (equal colours) is refused.
    Returns the pair set and colour map for the scoped collision relaxation.
    """
    conflict_edges = data.get("conflict_edges")
    colour_classes = data.get("colour_classes")

    if conflict_edges is None and colour_classes is None:
        return set(), {}

    if conflict_edges is not None and not isinstance(conflict_edges, list):
        raise RuntimeError(f"lane manifest conflict_edges must be a list when present: {path}")
    if colour_classes is not None and not isinstance(colour_classes, dict):
        raise RuntimeError(f"lane manifest colour_classes must be an object when present: {path}")

    pairs: set[frozenset[str]] = set()
    if isinstance(conflict_edges, list):
        for index, entry in enumerate(conflict_edges):
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise RuntimeError(f"lane manifest conflict_edges[{index}] must be a 2-element pair: {path}")
            a, b = entry[0], entry[1]
            if not isinstance(a, str) or not isinstance(b, str) or not a or not b:
                raise RuntimeError(f"lane manifest conflict_edges[{index}] endpoints must be non-empty strings: {path}")
            if a == b:
                raise RuntimeError(
                    f"lane manifest conflict_edges[{index}] must be an undirected pair of distinct lanes: {path}"
                )
            if a not in lane_ids or b not in lane_ids:
                unknown = [x for x in (a, b) if x not in lane_ids]
                raise RuntimeError(f"lane manifest conflict_edges references unknown lane(s) {unknown}: {path}")
            pairs.add(frozenset((a, b)))

    colours: dict[str, int] = {}
    if isinstance(colour_classes, dict):
        for lane_id, colour in colour_classes.items():
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(f"lane manifest colour_classes references unknown lane '{lane_id}': {path}")
            if isinstance(colour, bool) or not isinstance(colour, int):
                raise RuntimeError(f"lane manifest colour_classes['{lane_id}'] must be an int: {path}")
            colours[lane_id] = colour

    # (b) monochrome conflict edge → refuse.
    for pair in pairs:
        left_id, right_id = sorted(pair)
        if left_id not in colours or right_id not in colours:
            raise RuntimeError(
                f"lane manifest conflict edge [{left_id!r}, {right_id!r}] lacks colour_classes entries: {path}"
            )
        if colours[left_id] == colours[right_id]:
            raise RuntimeError(
                f"lane manifest monochrome conflict edge between '{left_id}' and '{right_id}' "
                f"(colour={colours[left_id]}): {path}"
            )

    return pairs, colours


def _validate_owned_path_collision_freedom(
    lanes: dict[str, Any],
    depends_on: dict[str, list[str]],
    path: Path,
    *,
    conflict_pairs: set[frozenset[str]] | None = None,
    colour_classes: dict[str, int] | None = None,
) -> None:
    """Refuse incomparable lanes whose normalized owned_paths roots overlap.

    implementation note S2 relaxation: a depends_on-incomparable overlapping pair is
    accepted IFF it appears in ``conflict_edges`` with DISTINCT colours. The
    relaxation is scoped to exactly those persisted pairs — an overlapping
    pair absent from conflict_edges still refuses (RuntimeError naming owned_paths).
    """
    pairs = conflict_pairs or set()
    colours = colour_classes or {}

    lane_roots: dict[str, list[str]] = {}
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        if "landed" in lane:
            marker = lane["landed"]
            if (
                not isinstance(marker, dict)
                or not isinstance(marker.get("sha"), str)
                or re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", marker["sha"]) is None
            ):
                raise RuntimeError(f"lane '{lane_id}' landed must contain a 40/64 hex sha: {path}")
            try:
                stamp = datetime.fromisoformat(marker["at"])
                if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
                    raise ValueError("not UTC")
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(f"lane '{lane_id}' landed must contain a UTC ISO at: {path}") from exc
        released = lane.get("released")
        if "landed" in lane or "terminal" in lane or (isinstance(released, dict) and released.get("verified") is True):
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
                    # Scoped relaxation: declared conflict edge + distinct colours.
                    edge = frozenset((left_id, right_id))
                    if edge in pairs:
                        c_left = colours.get(left_id)
                        c_right = colours.get(right_id)
                        if isinstance(c_left, int) and isinstance(c_right, int) and c_left != c_right:
                            continue
                    if left_root == right_root:
                        overlap_path = left_root if left_root else "/"
                    elif not left_root:
                        overlap_path = right_root if right_root else "/"
                    elif not right_root:
                        overlap_path = left_root
                    else:
                        overlap_path = left_root if left_root.startswith(f"{right_root}/") else right_root
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
    merge_order = (
        [lane for lane in merge_order_raw if isinstance(lane, str)] if isinstance(merge_order_raw, list) else []
    )
    index = {lane: i for i, lane in enumerate(merge_order)}

    edge_set: set[tuple[str, str]] = set()
    for lane, prereqs in depends_on.items():
        if not isinstance(lane, str) or not isinstance(prereqs, list):
            continue
        for prereq in prereqs:
            if isinstance(prereq, str):
                edge_set.add((lane, prereq))

    closure = {(u, v) for u in merge_order for v in merge_order if index[v] > index[u]}
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


def _is_non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_integration_branch(value: Any, path: Path) -> None:
    """Refuse present integration_branch values that are not a string or string list."""
    if _is_non_empty_str(value):
        return
    if isinstance(value, list) and value and all(_is_non_empty_str(item) for item in value):
        return
    raise RuntimeError(
        f"lane manifest integration_branch must be a non-empty string or a non-empty list of non-empty strings: {path}"
    )


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
    landing_policy = data.get("landing_policy")
    if not isinstance(merge_order, list):
        raise RuntimeError(f"lane manifest merge_order must be a list: {path}")
    if not isinstance(lanes, dict):
        raise RuntimeError(f"lane manifest lanes must be an object: {path}")
    if not isinstance(downstream, dict):
        raise RuntimeError(f"lane manifest downstream must be an object: {path}")
    if landing_policy is not None:
        if not isinstance(landing_policy, dict):
            raise RuntimeError(f"lane manifest landing_policy must be an object when present: {path}")
        unknown_landing_policy = sorted(key for key in landing_policy if key != "no_ff_merge")
        if unknown_landing_policy:
            raise RuntimeError(f"lane manifest landing_policy has unknown key(s) {unknown_landing_policy}: {path}")
        if "no_ff_merge" in landing_policy and not isinstance(landing_policy["no_ff_merge"], bool):
            raise RuntimeError(f"lane manifest landing_policy.no_ff_merge must be a boolean: {path}")
    if "integration_branch" in data:
        _validate_integration_branch(data.get("integration_branch"), path)
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
        if "self_review" in lane and not isinstance(lane["self_review"], bool):
            raise RuntimeError(f"lane '{lane_id}' self_review must be a boolean: {path}")
        if "terminal" in lane:
            marker = lane["terminal"]
            status = marker.get("status") if isinstance(marker, dict) else None
            if (
                not isinstance(marker, dict)
                or not isinstance(status, str)
                or status not in TERMINAL_STATUSES
                or marker.get("source") != "worktree_lanes"
            ):
                raise RuntimeError(
                    f"lane '{lane_id}' terminal must contain a canonical status and source 'worktree_lanes': {path}"
                )
            try:
                stamp_value = marker.get("at")
                if not isinstance(stamp_value, str):
                    raise ValueError("at is not a string")
                stamp = datetime.fromisoformat(stamp_value)
                if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
                    raise ValueError("not UTC")
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"lane '{lane_id}' terminal must contain a UTC ISO at: {path}") from exc
        if "released" in lane:
            marker = lane["released"]
            try:
                stamp_value = marker.get("at") if isinstance(marker, dict) else None
                stamp = datetime.fromisoformat(stamp_value) if isinstance(stamp_value, str) else None
            except (TypeError, ValueError):
                stamp = None
            if (
                not isinstance(marker, dict)
                or marker.get("status") != "planned"
                or marker.get("source") != "worktree_lanes"
                or not isinstance(marker.get("verified"), bool)
                or stamp is None
                or stamp.utcoffset() is None
                or stamp.utcoffset().total_seconds() != 0
            ):
                raise RuntimeError(
                    f"lane '{lane_id}' released must contain planned status, a verification flag, "
                    f"and a UTC ISO at from 'worktree_lanes': {path}"
                )
            if "terminal" in lane:
                raise RuntimeError(f"lane '{lane_id}' cannot contain both terminal and released markers: {path}")
        if "compacted" in lane:
            marker = lane["compacted"]
            try:
                stamp_value = marker.get("at") if isinstance(marker, dict) else None
                stamp = datetime.fromisoformat(stamp_value) if isinstance(stamp_value, str) else None
            except (TypeError, ValueError):
                stamp = None
            dropped = marker.get("dropped") if isinstance(marker, dict) else None
            if (
                stamp is None
                or stamp.utcoffset() is None
                or stamp.utcoffset().total_seconds() != 0
                or not isinstance(dropped, list)
                or any(not isinstance(key, str) for key in dropped)
            ):
                raise RuntimeError(f"lane '{lane_id}' compacted must contain dropped keys and a UTC ISO at: {path}")
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
                raise RuntimeError(f"lane '{lane_id}' owned_paths entries must be strings: {path}")
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
            effort_val = lane.get("preferred_reasoning_effort")
            concrete_efforts = tuple(
                effort for effort in WORKER_REASONING_EFFORT_CHOICES if effort not in {"auto", "inherit"}
            )
            if not isinstance(effort_val, str) or effort_val.strip().lower() not in concrete_efforts:
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_reasoning_effort must be one of {concrete_efforts} when present: {path}"
                )
        if "preferred_speed" in lane:
            speed_val = lane.get("preferred_speed")
            if speed_val not in ("standard", "fast"):
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_speed must be one of ('standard', 'fast') when present: {path}"
                )
            if lane.get("preferred_backend") != "codex-remote":
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_speed is codex-remote only; "
                    f"preferred_backend is {lane.get('preferred_backend')!r}: {path}"
                )
        if "preferred_tier" in lane:
            tier_val = lane.get("preferred_tier")
            if tier_val not in ("senior", "junior"):
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_tier must be one of ('senior', 'junior') when present: {path}"
                )
            if lane.get("preferred_backend") != "codex-remote":
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_tier is codex-remote only; "
                    f"preferred_backend is {lane.get('preferred_backend')!r}: {path}"
                )
            model_val = lane.get("preferred_model")
            model_tier = CODEX_MODEL_TIERS.get(model_val) if isinstance(model_val, str) else None
            if model_tier is not None and model_tier.tier != tier_val:
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_model {model_val!r} belongs to tier "
                    f"{model_tier.tier!r}, not {tier_val!r}: {path}"
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
        # implementation note S2: optional per-lane base_sha (full 40-char hex commit).
        if "base_sha" in lane:
            base_sha = lane.get("base_sha")
            if not isinstance(base_sha, str) or not _FULL_COMMIT_SHA_RE.fullmatch(base_sha.strip()):
                raise RuntimeError(
                    f"lane '{lane_id}' base_sha must be a non-empty full 40-char hex commit sha when present: {path}"
                )

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
    # implementation note S2: conflict_edges / colour_classes structural validation, then
    # scoped owned-path collision relaxation for declared bicoloured pairs.
    conflict_pairs, colour_classes = _validate_conflict_keys(data, lane_ids, path)
    _validate_owned_path_collision_freedom(
        lanes,
        depends_on_normalized,
        path,
        conflict_pairs=conflict_pairs,
        colour_classes=colour_classes,
    )
    _validate_state_key_conflict_freedom(lanes, depends_on_normalized, path)

    if isinstance(heading_to_lane, dict):
        for heading, lane_id in heading_to_lane.items():
            if not isinstance(heading, str) or not heading.strip():
                raise RuntimeError(f"lane manifest heading_to_lane keys must be non-empty strings: {path}")
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(f"lane manifest heading_to_lane references unknown lane '{lane_id}': {path}")

    routes = data.get("routing")
    if isinstance(routes, list):
        for index, route in enumerate(routes):
            # Preserve the reader's tolerance of non-object routing entries.
            if not isinstance(route, dict):
                continue
            lane_id = route.get("lane")
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(f"lane manifest routing[{index}] references unknown lane '{lane_id}': {path}")

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


# STOR-10: retain tombstones until safe. Pruning waits for reclamation because
# lane_landing_drain.py:307 resumes cleanup intents by lane ID and
# lane_retire_sweep.py:2266 uses terminal-successor graph evidence. These APIs
# prepare contraction without wiring an eviction caller. All 20 audit
# must_change_before_prune readers must change first: _manifest_supersedes_lane
# treats absence as supersession, and offload_preflight can rematerialize lanes.
def _contract_adjacency(
    adjacency: dict[str, list[str]],
    removed_ids: set[str],
) -> tuple[dict[str, list[str]], list[list[str]], list[list[str]]]:
    """Contract paths through removed vertices, stopping at live vertices."""
    dropped_edges = {
        (source, target)
        for source, targets in adjacency.items()
        for target in targets
        if source in removed_ids or target in removed_ids
    }
    original_edges = {(source, target) for source, targets in adjacency.items() for target in targets}
    contracted: dict[str, list[str]] = {}
    added_edges: set[tuple[str, str]] = set()

    for source, targets in adjacency.items():
        if source in removed_ids:
            continue
        kept: list[str] = []
        for target in targets:
            if target not in removed_ids:
                if target != source and target not in kept:
                    kept.append(target)
                continue

            reached_removed: set[str] = set()
            stack = [target]
            while stack:
                node = stack.pop()
                if node in removed_ids:
                    if node in reached_removed:
                        continue
                    reached_removed.add(node)
                    stack.extend(adjacency.get(node, []))
                elif node != source:
                    if node not in kept:
                        kept.append(node)
                        if (source, node) not in original_edges:
                            added_edges.add((source, node))

        contracted[source] = kept

    return (
        contracted,
        [list(edge) for edge in sorted(dropped_edges)],
        [list(edge) for edge in sorted(added_edges)],
    )


def _empty_lane_eviction_contraction() -> LaneEvictionContraction:
    return {
        "removed_ids": [],
        "edges_dropped": {"depends_on": [], "downstream": [], "conflict_edges": []},
        "edges_added": {"depends_on": [], "downstream": [], "conflict_edges": []},
        "entries_dropped": {
            "merge_order": [],
            "colour_classes": [],
            "heading_to_lane": [],
            "plan_routing_hints": [],
            "routing": [],
        },
    }


def contract_terminal_lanes(
    manifest: dict[str, Any],
    lane_ids: Sequence[str],
) -> tuple[dict[str, Any], LaneEvictionContraction]:
    """Return a copied manifest with approved terminal lanes contracted away."""
    lanes = manifest.get("lanes")
    lane_map = lanes if isinstance(lanes, dict) else {}
    requested = sorted(set(lane_ids))
    refusals: list[LaneEvictionRefusal] = []
    for lane_id in requested:
        lane = lane_map.get(lane_id)
        if lane is None:
            refusals.append({"lane_id": lane_id, "reason": "unknown_lane"})
            continue
        terminal = lane.get("terminal") if isinstance(lane, dict) else None
        status = terminal.get("status") if isinstance(terminal, dict) else None
        if not isinstance(status, str) or status not in EVICTABLE_TERMINAL_STATUSES:
            refusals.append({"lane_id": lane_id, "reason": "status_not_evictable"})
    if refusals:
        raise LaneEvictionRefused(refusals)

    contracted = copy.deepcopy(manifest)
    removed_ids = set(requested)
    contracted_lanes = contracted.get("lanes")
    if isinstance(contracted_lanes, dict):
        for lane_id in requested:
            contracted_lanes.pop(lane_id, None)

    record = _empty_lane_eviction_contraction()
    record["removed_ids"] = requested

    # Audit graph_contraction.depends_on: "Drop u→T when T is a satisfied prerequisite; contract u→T→v into u→v when both live endpoints need the same reachability."
    depends_on = contracted.get("depends_on")
    if isinstance(depends_on, dict):
        contracted["depends_on"], dropped, added = _contract_adjacency(depends_on, removed_ids)
        record["edges_dropped"]["depends_on"] = dropped
        record["edges_added"]["depends_on"] = added

    # Audit graph_contraction.downstream: "Apply the reverse depends_on rule: drop links incident to satisfied T and contract u→T→v into u→v to preserve live reachability."
    downstream = contracted.get("downstream")
    if isinstance(downstream, dict):
        contracted["downstream"], dropped, added = _contract_adjacency(downstream, removed_ids)
        record["edges_dropped"]["downstream"] = dropped
        record["edges_added"]["downstream"] = added

    # Audit graph_contraction.merge_order: "Remove T while preserving the relative order of all remaining lane IDs; this list is ordering metadata, not a substitute path."
    merge_order = contracted.get("merge_order")
    if isinstance(merge_order, list):
        record["entries_dropped"]["merge_order"] = [lane_id for lane_id in merge_order if lane_id in removed_ids]
        contracted["merge_order"] = [lane_id for lane_id in merge_order if lane_id not in removed_ids]

    # Audit graph_contraction.conflict_edges: "Drop pairs incident to T; these are pairwise conflict facts, so contracting through T would invent a live conflict."
    conflict_edges = contracted.get("conflict_edges")
    if isinstance(conflict_edges, list):
        kept_conflicts: list[Any] = []
        dropped_conflicts: list[list[str]] = []
        for pair in conflict_edges:
            if isinstance(pair, (list, tuple)) and any(endpoint in removed_ids for endpoint in pair):
                dropped_conflicts.append(list(pair))
            else:
                kept_conflicts.append(pair)
        contracted["conflict_edges"] = kept_conflicts
        record["edges_dropped"]["conflict_edges"] = dropped_conflicts

    # Audit graph_contraction.colour_classes: "Drop T’s node label and preserve all other assignments; colours are node attributes, not reachability edges."
    colour_classes = contracted.get("colour_classes")
    if isinstance(colour_classes, dict):
        record["entries_dropped"]["colour_classes"] = [lane_id for lane_id in requested if lane_id in colour_classes]
        contracted["colour_classes"] = {
            lane_id: colour for lane_id, colour in colour_classes.items() if lane_id not in removed_ids
        }

    # Remove non-graph routing references while preserving surviving entries.
    heading_to_lane = contracted.get("heading_to_lane")
    if isinstance(heading_to_lane, dict):
        record["entries_dropped"]["heading_to_lane"] = [
            [heading, lane_id] for heading, lane_id in heading_to_lane.items() if lane_id in removed_ids
        ]
        contracted["heading_to_lane"] = {
            heading: lane_id for heading, lane_id in heading_to_lane.items() if lane_id not in removed_ids
        }

    for routing_key in ("plan_routing_hints", "routing"):
        routes = contracted.get(routing_key)
        if isinstance(routes, list):
            kept_routes: list[Any] = []
            for route in routes:
                if isinstance(route, dict) and route.get("lane") in removed_ids:
                    record["entries_dropped"][routing_key].append(copy.deepcopy(route))
                else:
                    kept_routes.append(route)
            contracted[routing_key] = kept_routes

    return contracted, record


def _lane_eviction_fingerprint(lane: dict[str, Any]) -> str:
    encoded = json.dumps(lane, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _lane_eviction_refusal_reason(
    manifest: dict[str, Any],
    lane_id: str,
    evidence: Mapping[str, ReclaimEvidence],
) -> str | None:
    """Share the ordered, fail-closed eligibility checks between plan and apply."""
    lanes_value = manifest.get("lanes")
    lanes = lanes_value if isinstance(lanes_value, dict) else {}
    lane = lanes.get(lane_id)
    if lane is None:
        return "unknown_lane"
    if not isinstance(lane, dict):
        return "not_terminal"
    terminal = lane.get("terminal")
    status = terminal.get("status") if isinstance(terminal, dict) else None
    if not isinstance(status, str) or status not in TERMINAL_STATUSES:
        return "not_terminal"
    if status not in EVICTABLE_TERMINAL_STATUSES:
        return "status_not_evictable"
    if not isinstance(lane.get("compacted"), dict):
        return "not_compacted"

    lane_evidence = evidence.get(lane_id)
    evidence_keys = ("worktree_absent", "branch_absent", "cleanup_intent_pending")
    if not isinstance(lane_evidence, Mapping) or any(
        not isinstance(lane_evidence.get(key), bool) for key in evidence_keys
    ):
        return "evidence_missing"
    if lane_evidence["worktree_absent"] is not True:
        return "worktree_present"
    if lane_evidence["branch_absent"] is not True:
        return "branch_present"
    if lane_evidence["cleanup_intent_pending"] is not False:
        return "cleanup_intent_pending"

    # lane_retire_sweep._absorbed_landing:2290-2295 discovers a predecessor's
    # terminal successor through depends_on, so preserve it until P is terminal.
    depends_on = manifest.get("depends_on", {})
    prerequisites = depends_on.get(lane_id, []) if isinstance(depends_on, dict) else None
    if not isinstance(prerequisites, list) or any(
        not isinstance(prerequisite, str)
        or not isinstance(lanes.get(prerequisite), dict)
        or not isinstance(lanes[prerequisite].get("terminal"), dict)
        or not isinstance(lanes[prerequisite]["terminal"].get("status"), str)
        or lanes[prerequisite]["terminal"].get("status") not in TERMINAL_STATUSES
        for prerequisite in prerequisites
    ):
        return "live_prerequisite"
    return None


def plan_reclaimed_lane_eviction(
    manifest: dict[str, Any],
    evidence: Mapping[str, ReclaimEvidence],
) -> LaneEvictionPlan:
    """Purely plan safe evictions from manifest state and caller-owned evidence."""
    lanes_value = manifest.get("lanes")
    lanes = lanes_value if isinstance(lanes_value, dict) else {}
    candidates = {
        lane_id
        for lane_id, lane in lanes.items()
        if isinstance(lane_id, str) and isinstance(lane, dict) and "terminal" in lane
    }
    candidates.update(lane_id for lane_id in evidence if isinstance(lane_id, str))

    evict: list[str] = []
    refused: list[LaneEvictionRefusal] = []
    expected_lane_fingerprints: dict[str, str] = {}
    for lane_id in sorted(candidates):
        reason = _lane_eviction_refusal_reason(manifest, lane_id, evidence)
        if reason is not None:
            refused.append({"lane_id": lane_id, "reason": reason})
            continue

        evict.append(lane_id)
        expected_lane_fingerprints[lane_id] = _lane_eviction_fingerprint(lanes[lane_id])

    return {
        "evict": evict,
        "refused": refused,
        "expected_lane_fingerprints": expected_lane_fingerprints,
    }


def apply_lane_eviction(
    task_ref: str,
    plan: LaneEvictionPlan,
    *,
    evidence: Mapping[str, ReclaimEvidence] | None = None,
    manifest_dir: str | Path | None = None,
) -> LaneEvictionResult:
    """Apply a plan with fresh evidence under the manifest's existing RMW lock.

    Evidence is checked again here, never reused from planning; absence refuses
    eviction. The caller must gather fresh evidence and hold the external owners'
    fences through this call so worktree, branch and cleanup-intent state stays
    stable. The manifest lock alone does not serialize those external writers.
    This module probes none of that state and adds no second lock or caller.
    """
    manifest_path = _resolve_manifest_dir(manifest_dir=manifest_dir) / f"{task_ref}.json"
    planned_refusals = [dict(refusal) for refusal in plan.get("refused", [])]
    new_refusals: list[LaneEvictionRefusal] = []
    evicted: list[str] = []

    with manifest_write_lock(manifest_path):
        if not manifest_path.exists():
            raise FileNotFoundError(f"lane manifest not found for atomic update: {manifest_path}")
        raw = manifest_path.read_bytes()
        try:
            current_data = json.loads(raw.decode("utf-8") if raw else "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"lane manifest is not valid JSON: {manifest_path}") from exc
        if not isinstance(current_data, dict):
            raise RuntimeError(f"lane manifest must be a JSON object: {manifest_path}")
        current = validate_manifest(current_data, manifest_path)
        current_lanes = current.get("lanes")
        current_lanes = current_lanes if isinstance(current_lanes, dict) else {}
        expected_fingerprints = plan.get("expected_lane_fingerprints", {})
        expected_fingerprints = expected_fingerprints if isinstance(expected_fingerprints, Mapping) else {}

        for lane_id in sorted(set(plan.get("evict", []))):
            lane = current_lanes.get(lane_id)
            if lane is None:
                new_refusals.append({"lane_id": lane_id, "reason": "unknown_lane"})
                continue
            expected_fingerprint = expected_fingerprints.get(lane_id)
            if not isinstance(lane, dict) or not isinstance(expected_fingerprint, str):
                new_refusals.append({"lane_id": lane_id, "reason": "changed_since_plan"})
                continue
            if _lane_eviction_fingerprint(lane) != expected_fingerprint:
                new_refusals.append({"lane_id": lane_id, "reason": "changed_since_plan"})
                continue
            reason = _lane_eviction_refusal_reason(current, lane_id, evidence if evidence is not None else {})
            if reason is not None:
                new_refusals.append({"lane_id": lane_id, "reason": reason})
                continue
            evicted.append(lane_id)

        refused_by_lane: dict[str, LaneEvictionRefusal] = {}
        for refusal in planned_refusals + new_refusals:
            refused_by_lane.setdefault(refusal["lane_id"], refusal)
        refused = [refused_by_lane[lane_id] for lane_id in sorted(refused_by_lane)]
        contraction = _empty_lane_eviction_contraction()
        bytes_before = len(raw)
        bytes_after = bytes_before

        if evicted:
            contracted, contraction = contract_terminal_lanes(current, evicted)
            _manifest_rmw_barrier()
            _, payload = _encode_and_publish_manifest(
                manifest_path, contracted, current_bytes=len(raw), prior_lane_ids=set(current_lanes)
            )
            bytes_after = len(payload)

    return {
        "evicted": evicted,
        "refused": refused,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "contraction": contraction,
    }


def reconcile_terminal_lanes(
    manifest_path: Path | str,
    row_status_of: Callable[[list[str]], dict[str, Any]] | None = None,
    *,
    row_status_of_locked_lanes: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> TerminalLaneReconciliation:
    """Project worktree_lanes row states into terminal and released markers.

    Unknown row state is fail-closed: it never retires a lane's ownership. A
    planned row retires ownership only with explicit no-work release evidence.
    A reactivated lane is cleared only when the full candidate document
    validates; otherwise its marker stays in place and the colliding pair is
    returned. New callers can use ``row_status_of_locked_lanes`` to inspect the
    exact lane mapping loaded for mutation under the manifest lock. The legacy
    ``row_status_of`` callback remains supported for existing callers.
    """
    path = Path(manifest_path)
    result: TerminalLaneReconciliation = {
        "stamped": [],
        "cleared": [],
        "compacted": [],
        "unknown": [],
        "blocked": [],
    }

    class _NoTerminalChanges(Exception):
        """Avoid writing or validating an unchanged, possibly poisoned file."""

    def mutate(data: dict[str, Any]) -> None:
        lanes = data.get("lanes")
        if not isinstance(lanes, dict):
            raise RuntimeError(f"lane manifest lanes must be an object: {path}")
        lane_ids = sorted(lane_id for lane_id in lanes if isinstance(lane_id, str))
        if row_status_of_locked_lanes is not None:
            row_states = row_status_of_locked_lanes(lanes)
        elif row_status_of is not None:
            row_states = row_status_of(lane_ids)
        else:
            raise RuntimeError("worktree_lanes status reader was not provided")
        if not isinstance(row_states, dict):
            raise RuntimeError("worktree_lanes status reader must return a lane-id mapping")

        states: dict[str, tuple[str | None, bool]] = {
            lane_id: _lane_row_state(row_states.get(lane_id)) for lane_id in lane_ids
        }

        actionable = False
        for lane_id in lane_ids:
            lane = lanes.get(lane_id)
            status, no_work_released = states[lane_id]
            if not isinstance(status, str):
                result["unknown"].append(lane_id)
                marker = lane.get("released") if isinstance(lane, dict) else None
                if isinstance(marker, dict) and marker.get("verified") is True:
                    actionable = True
                if isinstance(lane, dict) and _needs_terminal_compaction(lane, None, False):
                    actionable = True
                continue
            if (
                status in TERMINAL_STATUSES
                and isinstance(lane, dict)
                and ("terminal" not in lane or "released" in lane)
            ):
                actionable = True
            elif (
                status not in TERMINAL_STATUSES
                and isinstance(lane, dict)
                and ("terminal" in lane or ("released" in lane and not no_work_released))
            ):
                actionable = True
            elif (
                no_work_released
                and isinstance(lane, dict)
                and (
                    "released" not in lane
                    or not isinstance(lane.get("released"), dict)
                    or lane["released"].get("verified") is not True
                    or "terminal" in lane
                )
            ):
                actionable = True
            if isinstance(lane, dict) and _needs_terminal_compaction(lane, status, no_work_released):
                actionable = True

        if not actionable:
            raise _NoTerminalChanges

        now = datetime.now(timezone.utc).isoformat()
        for lane_id in lane_ids:
            lane = lanes.get(lane_id)
            status, no_work_released = states[lane_id]
            if not isinstance(status, str) or not isinstance(lane, dict):
                continue
            if status in TERMINAL_STATUSES:
                lane.pop("released", None)
            if status in TERMINAL_STATUSES and "terminal" not in lane:
                lane["terminal"] = {"status": status, "at": now, "source": "worktree_lanes"}
                result["stamped"].append(lane_id)
            if status not in TERMINAL_STATUSES and no_work_released:
                lane.pop("terminal", None)
                marker = lane.get("released")
                if not isinstance(marker, dict) or marker.get("verified") is not True:
                    lane["released"] = {
                        "status": "planned",
                        "at": now,
                        "source": "worktree_lanes",
                        "verified": True,
                    }
                    result["stamped"].append(lane_id)

        # Validate each reactivation candidate in isolation. A colliding clear
        # is rolled back while other safe lifecycle projections can commit.
        for lane_id in lane_ids:
            lane = lanes.get(lane_id)
            status, no_work_released = states[lane_id]
            if not isinstance(status, str):
                marker = lane.get("released") if isinstance(lane, dict) else None
                if not isinstance(marker, dict) or marker.get("verified") is not True:
                    continue
                candidate = copy.deepcopy(data)
                candidate["lanes"][lane_id]["released"]["verified"] = False
                try:
                    validate_manifest(candidate, path)
                except RuntimeError as exc:
                    match = _OWNED_PATH_COLLISION_PAIR_RE.search(str(exc))
                    if match is None:
                        raise
                    result["blocked"].append({"lane_id": lane_id, "pair": [match.group(1), match.group(2)]})
                    continue
                marker["verified"] = False
                continue
            if status in TERMINAL_STATUSES or no_work_released:
                continue
            if not isinstance(lane, dict) or ("terminal" not in lane and "released" not in lane):
                continue

            candidate = copy.deepcopy(data)
            candidate["lanes"][lane_id].pop("terminal", None)
            candidate["lanes"][lane_id].pop("released", None)
            try:
                validate_manifest(candidate, path)
            except RuntimeError as exc:
                match = _OWNED_PATH_COLLISION_PAIR_RE.search(str(exc))
                if match is None:
                    raise
                result["blocked"].append({"lane_id": lane_id, "pair": [match.group(1), match.group(2)]})
                continue

            lane.pop("terminal", None)
            lane.pop("released", None)
            result["cleared"].append(lane_id)

        # Keep terminal identity and ownership stable, but discard planning and
        # execution payloads that no terminal consumer needs. This phase follows
        # reactivation clearing so a known live row is never compacted.
        for lane_id in lane_ids:
            lane = lanes.get(lane_id)
            status, no_work_released = states[lane_id]
            if isinstance(lane, dict) and _needs_terminal_compaction(lane, status, no_work_released):
                _compact_terminal_lane(lane, at=now)
                result["compacted"].append(lane_id)

    try:
        atomic_update_manifest(path, mutate, pre_validate=False)
    except _NoTerminalChanges:
        # A no-op must not rewrite an invalid file: inspection owns the normal
        # semantic refusal, while this result still reports missing row IDs.
        pass
    return result


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
    lock_timeout: float | None = None,
) -> Path:
    """Validate and persist a lane manifest under the sidecar lock.

    The publish sequence is ``_atomic_publish_manifest`` — the same temp file,
    fsync, and ``os.replace`` ``atomic_update_manifest`` uses. A lock timeout
    raises ``ManifestLockTimeoutError`` and does not write.
    """
    resolved_dir = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    task_ref = str(manifest.get("task_ref") or "").strip()
    if not task_ref:
        raise RuntimeError("lane manifest must include task_ref before save")
    path = resolved_dir / f"{task_ref}.json"
    with manifest_write_lock(path, timeout=lock_timeout):
        _persist_manifest_unlocked(manifest, path)
    return path


def _persist_manifest_unlocked(manifest: dict[str, Any], path: Path) -> dict[str, Any]:
    """Validate and atomically replace ``path``. Caller holds the sidecar lock."""
    validated = copy.deepcopy(manifest)
    raw = path.read_bytes() if path.exists() else b""
    existing = None
    if raw:
        try:
            existing = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            existing = None
        if isinstance(existing, dict) and "task_plan_path" not in validated:
            existing_task_plan = existing.get("task_plan_path")
            if isinstance(existing_task_plan, str) and existing_task_plan.strip():
                validated = dict(validated)
                validated["task_plan_path"] = existing_task_plan
    if isinstance(validated.get("task_plan_path"), str) and validated["task_plan_path"].strip():
        validated = copy.deepcopy(validated)
        _ensure_task_plan_required_docs(validated)
    validated, _ = _encode_and_publish_manifest(
        path,
        validated,
        current_bytes=len(raw),
        prior_lane_ids=_manifest_lane_ids(existing if isinstance(existing, dict) else {}),
    )
    return validated


def _manifest_rmw_barrier() -> None:
    """Env-gated barrier between load/mutate and save (row 30 test seam)."""
    raw = os.environ.get(_MANIFEST_RMW_BARRIER_ENV)
    if not raw or not str(raw).strip():
        return
    barrier = Path(str(raw).strip())
    try:
        barrier.mkdir(parents=True, exist_ok=True)
        ready = barrier / f"ready.{os.getpid()}"
        ready.touch()
        go = barrier / "go"
        deadline = time.monotonic() + 30.0
        while not go.exists():
            if time.monotonic() >= deadline:
                break
            time.sleep(0.005)
    except OSError:
        return


def atomic_update_manifest(
    path: Path | str,
    mutate_fn: Callable[[dict[str, Any]], None],
    *,
    pre_validate: bool = True,
    lock_timeout: float | None = None,
) -> dict[str, Any]:
    """Load-modify-save an EXISTING manifest under a flock, writing atomically.

    Three live writers (compiler CLI recompile, server materialize, daemon
    base_sha re-record) must RMW through this helper so distinct-key updates
    cannot lose each other (row 30).

    **Invariant:** manifest writes take ``manifest_write_lock`` and publish
    through ``_atomic_publish_manifest``. ``save_manifest`` uses that same
    helper, so a create no longer races an update with ``path.write_text``.
    Readers that open the path without the flock never observe a partial
    (torn) JSON payload because the write is temp-file + ``os.replace``.

    Flock is held on a sidecar ``<path>.lock`` for the whole critical section
    (not the data inode), so the flock survives the atomic rename. An env-gated
    barrier between mutate and save makes two-process races deterministically
    testable. First-creation of the data file is not performed here (callers
    seed via ``save_manifest`` or ``create_or_update_manifest``); the
    exists-check runs under the sidecar lock so concurrent RMW callers never
    race a create/open TOCTOU on the data path.

    ``pre_validate`` (default True) runs ``validate_manifest`` before mutate so
    corrupt files fail closed. Repair callers pass ``pre_validate=False`` to
    skip only that check; post-mutation validate, the sidecar flock, the temp
    file and ``os.replace`` stay unconditional. A lock timeout raises
    ``ManifestLockTimeoutError`` — the same type ``save_manifest`` raises —
    and does not write.
    """
    manifest_path = Path(path)
    with manifest_write_lock(manifest_path, timeout=lock_timeout):
        return _atomic_update_unlocked(manifest_path, mutate_fn, pre_validate=pre_validate)


def _atomic_update_unlocked(
    manifest_path: Path,
    mutate_fn: Callable[[dict[str, Any]], None],
    *,
    pre_validate: bool,
) -> dict[str, Any]:
    """RMW ``manifest_path``. Caller holds the sidecar lock."""
    if not manifest_path.exists():
        raise FileNotFoundError(f"lane manifest not found for atomic update: {manifest_path}")

    raw = manifest_path.read_bytes()
    try:
        data = json.loads(raw.decode("utf-8") if raw else "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"lane manifest is not valid JSON: {manifest_path}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"lane manifest must be a JSON object: {manifest_path}")

    # Validate pre-mutation so corrupt files fail closed before mutate.
    # Repair paths skip only this check; post-mutation validate stays.
    if pre_validate:
        data = validate_manifest(data, manifest_path)
    prior_lane_ids = _manifest_lane_ids(data)
    had_task_plan_path = "task_plan_path" in data
    existing_task_plan_path = data.get("task_plan_path")
    mutate_fn(data)
    if had_task_plan_path and "task_plan_path" not in data:
        data["task_plan_path"] = existing_task_plan_path
    _ensure_task_plan_required_docs(data)
    _manifest_rmw_barrier()
    validated, _ = _encode_and_publish_manifest(
        manifest_path, data, current_bytes=len(raw), prior_lane_ids=prior_lane_ids
    )
    return validated


def create_or_update_manifest(
    path: Path | str,
    *,
    create_fn: Callable[[], dict[str, Any]],
    mutate_fn: Callable[[dict[str, Any]], None],
    pre_validate: bool = True,
    replace_existing: bool = False,
    lock_timeout: float | None = None,
) -> dict[str, Any]:
    """Create when absent, otherwise RMW, under one sidecar-lock section.

    ``create_fn`` runs only after the lock is held and the file is still
    absent, or when ``replace_existing=True`` explicitly requests replacement.
    It returns a full manifest and must not publish. ``mutate_fn`` is the
    ordinary existing-file update and still requires parseable object JSON,
    even with ``pre_validate=False``.

    Explicit replacement tolerates malformed previous JSON, while counting
    its actual bytes and treating unrecoverable lane IDs as an empty set for
    conservative admission. Candidate validation, budgets and atomic publish
    remain unconditional. A lock timeout raises ``ManifestLockTimeoutError``.
    """
    manifest_path = Path(path)
    with manifest_write_lock(manifest_path, timeout=lock_timeout):
        if replace_existing or not manifest_path.exists():
            return _persist_manifest_unlocked(create_fn(), manifest_path)
        return _atomic_update_unlocked(manifest_path, mutate_fn, pre_validate=pre_validate)


def repair_manifest(
    path: Path | str,
    *,
    retire: Sequence[str],
    reason: str,
) -> dict[str, Any]:
    """Retire named lanes without the pre-mutation validate so collisions can be repaired.

    Each named lane keeps its entry: ``owned_paths`` and ``commit_paths`` are
    emptied and ``retired: {reason, at}`` is stamped. ``depends_on`` references
    stay in place. Missing ids are reported, never raised. Returns
    ``{retired, missing, valid_after}``.
    """
    retired: list[str] = []
    missing: list[str] = []
    retire_ids = list(retire)
    stamp_at = datetime.now(timezone.utc).isoformat()

    def _mutate(manifest: dict[str, Any]) -> None:
        lanes = manifest.get("lanes")
        for lane_id in retire_ids:
            if not isinstance(lanes, dict) or lane_id not in lanes:
                missing.append(lane_id)
                continue
            lane = lanes[lane_id]
            if not isinstance(lane, dict):
                missing.append(lane_id)
                continue
            lane["owned_paths"] = []
            lane["commit_paths"] = []
            lane["retired"] = {"reason": reason, "at": stamp_at}
            retired.append(lane_id)

    atomic_update_manifest(path, _mutate, pre_validate=False)
    return {"retired": retired, "missing": missing, "valid_after": True}


def shrink_manifest(
    path: Path | str,
    *,
    row_status_of_locked_lanes: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compact eligible terminal lanes and re-encode under the write fence.

    This is the supported explicit shrink-and-retry recovery. Eligibility is the
    rule ``reconcile_terminal_lanes`` applies (``_needs_terminal_compaction``), so
    a row the reader reports live keeps its payload even when a stale terminal
    marker remains. Pass the same reader that reconcile uses. Without one, every
    row is unknown and only the lane's own marker decides. It never evicts a lane
    and cannot guarantee headroom.
    """

    def compact(data: dict[str, Any]) -> None:
        lanes = data["lanes"]
        row_states = row_status_of_locked_lanes(lanes) if row_status_of_locked_lanes is not None else {}
        if not isinstance(row_states, dict):
            raise RuntimeError("worktree_lanes status reader must return a lane-id mapping")
        now = datetime.now(timezone.utc).isoformat()
        for lane_id, lane in lanes.items():
            if not isinstance(lane, dict):
                continue
            status, no_work_released = _lane_row_state(row_states.get(lane_id))
            if _needs_terminal_compaction(lane, status, no_work_released):
                _compact_terminal_lane(lane, at=now)

    return atomic_update_manifest(path, compact)


def retire_lane_ownership(
    path: Path | str,
    lane_id: str,
    *,
    reason: str,
    missing_ok: bool = False,
) -> dict[str, Any]:
    """Empty a lane's owned_paths without popping the entry.

    ``depends_on`` references from other lanes keep resolving. A manifest that
    is already invalid is refused by ``atomic_update_manifest``'s pre-mutation
    validate (operator repair, not a silent rewrite). When ``missing_ok`` is
    True and the lane is absent, return ``{'retired': False, 'reason':
    'not_in_manifest'}`` instead of raising ``KeyError``.
    """

    def _mutate(manifest: dict[str, Any]) -> None:
        lanes = manifest.get("lanes")
        if not isinstance(lanes, dict) or lane_id not in lanes:
            raise KeyError(f"lane {lane_id!r} not in manifest")
        lane = lanes[lane_id]
        if not isinstance(lane, dict):
            raise ValueError(f"lane {lane_id!r} is not an object")
        lane["owned_paths"] = []
        if "commit_paths" in lane:
            lane["commit_paths"] = []
        lane["ownership_retired_at"] = datetime.now(timezone.utc).isoformat()
        lane["ownership_retired_reason"] = reason

    try:
        return atomic_update_manifest(path, _mutate)
    except KeyError:
        if missing_ok:
            return {"retired": False, "reason": "not_in_manifest"}
        raise


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
    return worktree_root.expand_lane_path(template, orchestrator_root=orchestrator_root)


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
    result.setdefault("preferred_speed", None)
    result.setdefault("preferred_tier", None)
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
