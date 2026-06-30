"""Write-context cluster for workbay_handoff_mcp.

Extracted from _shared.py (implementation note of internal). Contains:
  - WriteActor TypedDict
  - ResolvedWriteContext dataclass
  - _resolve_core_override()  — shared monkeypatch-fallback helper
  - build_write_actor()
  - _first_non_empty_env()
  - _run_cmd()
  - _detect_git_write_context()
  - _git_is_ancestor()
  - _classify_commit_relation()
  - _workspace_git_context()
  - _resolve_write_actor()

All symbols are re-exported from _shared.py for backward compatibility.

Imports from _shared are done at function level (late imports) to avoid a circular
module dependency: _shared.py re-exports from this module at its end, so module-level
imports in this file would create a deadlock when this module is loaded first.
"""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from typing import Any, TypedDict

from workbay_protocol import resolve_env_alias

from .enums import (
    normalize_model_identity,
    normalize_model_label,
    normalize_reasoning_level,
)
from .runtime import get_runtime_config
from .shared_primitives import _normalize_optional_text

# Mirror the constant from _shared; defined here so this module has no
# module-level dependency on _shared (avoids circular import).
_SUBPROCESS_TIMEOUT = 10

# Regex matching abbreviated and full git commit SHAs (4-40 hex chars).
# Anything shorter than 4 chars is too ambiguous to expand; anything
# non-hex is rejected outright.
_COMMIT_SHA_HEX_RE = re.compile(r"^[0-9a-f]{4,40}$")


class InvalidCommitShaError(ValueError):
    """Raised when a ``commit_sha`` value cannot be validated against git.

    The validator distinguishes three failure modes:

    1. The string is non-empty but not hex (typo, wrong field passed).
    2. The string is hex but does not resolve to any object in the
       active git repository (typically a fabricated SHA, e.g. one
       expanded from a 7-char abbreviation by typing the suffix from
       memory rather than via ``git rev-parse``).
    3. The string resolves to a non-commit object (tag, tree, blob).

    Validation is bypassed entirely when the
    ``WORKBAY_HANDOFF_SKIP_SHA_VALIDATION`` environment variable is set
    (used by both packages' test suites; see their ``conftest.py``).
    """


class BranchMismatchError(ValueError):
    """Raised when branch enforcement is enabled and a write targets the wrong branch."""

    def __init__(self, task_ref: str, expected_branch: str, actual_branch: str) -> None:
        self.task_ref = task_ref
        self.expected_branch = expected_branch
        self.actual_branch = actual_branch
        super().__init__(
            f"actor.branch {actual_branch!r} does not match active task {task_ref!r} target_branch {expected_branch!r}."
        )


class UnresolvedTaskContextError(ValueError):
    """Raised when a write path cannot resolve an active task_ref.

    The canonical resolution order is: (1) an explicit ``task_ref``
    parameter, (2) a workspace-path lookup via
    ``_resolve_workspace_handoff_row``. When neither resolves, callers
    must fail closed rather than silently falling back to a sentinel
    row. Callers that hit this error should either pass ``task_ref=``
    explicitly or run from the task's registered ``target_worktree_path``.
    """


class WorktreeNotFoundError(ValueError):
    """Raised when a task's canonical ``target_branch`` has no matching worktree on disk.

    internal: ``handoff_state.target_worktree_path`` is no
    longer read; the worktree path is derived via
    ``git worktree list --porcelain`` keyed by canonical
    ``target_branch``. When that lookup conclusively finds no match,
    derivation must fail loudly so operators see the missing worktree
    instead of getting silent wrong-cwd attribution. Callers that hit
    this error should either ``git worktree add`` the missing worktree,
    update the row's ``target_branch`` to the canonical form, archive the
    task, or — when the intent is to retire a task whose worktree is
    already gone — run ``make task-finish TASK=<task_ref>`` (the canonical
    close path now clears the stale pointer for them, internal).

    Fixture authors: initial ``set_handoff_state`` row creation is
    intentionally allowed before a worktree exists so task-start can
    bootstrap state, but later guarded writes such as ``record_*`` and
    ``review_findings`` resolve the canonical worktree and will raise
    this error. Hermetic tests that do not want production derivation
    should set ``WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION=1`` before importing
    the API; tests that exercise guarded writes should create a real
    feature-branch checkout or linked worktree matching
    ``target_branch``.
    """

    def __init__(self, target_branch: str) -> None:
        self.target_branch = target_branch
        super().__init__(
            "Peer branch-switch or missing worktree: no linked git worktree matches "
            f"canonical target_branch {target_branch!r} "
            "(a peer may have moved/renamed the worktree, or `git worktree add` was "
            "never run for this task). "
            "Re-run after resolving the worktree, pass an incoming target_branch to "
            "repoint the row, or — when retiring a task whose worktree is already gone — "
            "run `make task-finish TASK=<task_ref>`; the canonical close path clears "
            "the stale pointer for you instead of requiring a tombstone worktree."
        )


class WriteActorAttributionError(ValueError):
    """Raised when a write cannot be attributed without silent codex fallback.

    Callers must pass ``actor.model`` / ``actor.agent`` on each write, set
    ``WORKBAY_HANDOFF_DEFAULT_AGENT``, or have exactly one harness transcript
    env var active so the server can infer attribution.
    """


class AmbiguousWorkspaceContextError(UnresolvedTaskContextError):
    """Raised when the workspace resolver finds 2+ candidate tasks.

    Attaches a ``candidates`` list — each entry is a mapping with
    ``task_ref``, ``target_branch``, ``target_worktree_path``,
    ``objective``, ``status``, and ``updated_at``. ``get_handoff_state``
    surfaces these to the caller so the ambiguity can be resolved by
    passing ``task_ref`` explicitly instead of bailing with an opaque
    string (internal, closes COLDSTART-H-02). Extending this to
    ``search_handoff`` and other read entrypoints is tracked as a
    follow-up.
    """

    def __init__(self, message: str, candidates: list[dict] | None = None) -> None:
        super().__init__(message)
        self.candidates: list[dict] = list(candidates or [])


def _commit_sha_validation_enabled() -> bool:
    """Return ``False`` if the test bypass env var is set, else ``True``."""
    bypass = (
        resolve_env_alias(
            "WORKBAY_HANDOFF_SKIP_SHA_VALIDATION",
            default="",
        )
        .strip()
        .lower()
    )
    return bypass not in {"1", "true", "yes", "on"}


def _infer_harness_agent_from_env() -> str | None:
    """Best-effort harness slug from exactly one active transcript env var."""
    try:
        from .compaction_contract import detect_active_harness, load_compaction_contract
        from .runtime import get_runtime_config

        contract = load_compaction_contract(get_runtime_config().workspace_root)
        # ``detect_active_harness`` reads its source mapping from the ``env``
        # argument; with ``env`` omitted it defaults to an empty dict and never
        # matches a transcript var, so the harness slug is *always* None. Pass
        # the process environment so a present transcript var actually resolves
        # to its harness identity (the precedence at :902-903 then prefers it
        # over the harness-agent fallback).
        resolution = detect_active_harness(contract, env=dict(os.environ))
        if resolution.harness is None:
            return None
        return str(resolution.harness)
    except Exception:
        return None


def _worktree_derivation_enabled() -> bool:
    """Return ``False`` if the test bypass env var is set, else ``True``.

    internal: production reads of ``target_worktree_path`` are
    replaced by derivation through :func:`_canonical_worktree_for_task`.
    The test suite uses synthetic ``target_branch`` values that have no
    real worktree on disk; deriving would raise
    :class:`WorktreeNotFoundError` for hundreds of fixtures. Setting
    ``WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION=1`` keeps those tests reading
    the stored column. Dedicated implementation note derivation tests exercise the production
    path directly via ``tmp_path`` git repos and do not rely on the
    resolver wrapper, so they get the loud-failure behavior end-to-end.
    """
    bypass = (
        resolve_env_alias(
            "WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION",
            default="",
        )
        .strip()
        .lower()
    )
    return bypass not in {"1", "true", "yes", "on"}


def _branch_enforcement_enabled() -> bool:
    """Return ``True`` when env-gated branch enforcement should block writes."""
    bypass = (
        resolve_env_alias(
            "WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT",
            default="",
        )
        .strip()
        .lower()
    )
    if bypass in {"1", "true", "yes", "on"}:
        return False
    enabled = (
        resolve_env_alias(
            "WORKBAY_HANDOFF_ENFORCE_BRANCH",
            default="",
        )
        .strip()
        .lower()
    )
    return enabled in {"1", "true", "yes", "on"}


def _branch_target_is_enforceable(target_branch: str | None) -> bool:
    normalized = _normalize_optional_text(target_branch)
    if normalized is None:
        return False
    return normalized.lower() not in {"main", "master"}


def _git_repo_root() -> str | None:
    """Return the absolute path of the active git repo root, or None.

    Resolves the active task's ``target_worktree_path`` if present (so
    validation runs against the worktree the agent claims to be working
    in), falling back to the runtime workspace_root, falling back to
    cwd. Returns ``None`` if no git directory is reachable from any of
    those, which means SHA validation is silently skipped (the agent is
    not in a git context, e.g. running tests in a tmp_path fixture).
    """
    candidates: list[str] = []
    try:
        config = get_runtime_config()
        candidates.append(str(config.git_workspace_root))
    except RuntimeError:
        pass
    try:
        candidates.append(os.getcwd())
    except OSError:
        pass
    for candidate in candidates:
        try:
            proc = subprocess.run(
                ["git", "-C", candidate, "rev-parse", "--show-toplevel"],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=_SUBPROCESS_TIMEOUT,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        if proc.returncode == 0:
            root = proc.stdout.strip()
            if root:
                return root
    return None


def _validate_and_expand_commit_sha(value: str | None) -> str | None:
    """Validate ``value`` against the active git repo and return its full SHA.

    Behavior:

    - ``None`` or empty string -> return as-is (caller is not claiming
      provenance for a specific commit).
    - Non-hex string -> raise ``InvalidCommitShaError`` immediately.
    - Hex string of 4-40 chars -> shell out to
      ``git -C <repo> rev-parse --verify <sha>^{commit}`` to confirm
      the SHA resolves to a real commit. On success, return the
      full 40-char form so the audit trail always stores the
      canonical SHA. On failure (object not found, ambiguous abbrev,
      not a commit), raise ``InvalidCommitShaError``.
    - When git is not available or no repository is reachable from
      the runtime workspace_root, validation is silently skipped and
      the input is returned unchanged. This keeps tmp_path tests and
      non-git environments working. Validation is also bypassed entirely
      when the ``WORKBAY_HANDOFF_SKIP_SHA_VALIDATION`` env var is truthy.
      Both package test suites set this env var in their ``conftest.py``
      so synthetic test SHAs (``"abc123"``, ``"def456"``) pass
      through unchanged.

    The validator exists because the gate that previously accepted
    fabricated SHAs (``handoff_close_check`` comparing the passed
    ``current_commit_sha`` against the recorded slice decision's
    ``commit_sha`` as opaque strings) had no way to detect that the
    SHA didn't actually point at a real git object. Several
    internal/internal audit-trail rows ended up tagged with
    SHA suffixes that were typed from memory rather than read from
    ``git rev-parse``, and the gate passed because the fabricated
    string matched itself.
    """
    if not _commit_sha_validation_enabled():
        return value
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return value
    if not _COMMIT_SHA_HEX_RE.fullmatch(normalized):
        raise InvalidCommitShaError(
            f"commit_sha {value!r} is not a hex string of 4-40 characters. "
            "Pass the full SHA from `git rev-parse HEAD`, never a typed-from-memory expansion."
        )
    repo_root = _git_repo_root()
    if repo_root is None:
        # No git context available -- typical in tmp_path test fixtures.
        # Skip validation rather than failing the write.
        return value
    try:
        proc = subprocess.run(
            ["git", "-C", repo_root, "rev-parse", "--verify", f"{normalized}^{{commit}}"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
        )
    except Exception:
        # git binary not available or hung -- skip rather than fail.
        return value
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise InvalidCommitShaError(
            f"commit_sha {value!r} does not resolve to a real commit object in {repo_root}.\n"
            "Run `git rev-parse <abbrev>` to get the canonical SHA before recording it.\n"
            f"git rev-parse said: {stderr}"
        )
    full_sha = proc.stdout.strip()
    if not _COMMIT_SHA_HEX_RE.fullmatch(full_sha) or len(full_sha) != 40:
        # Defensive: git returned something unexpected.
        return value
    return full_sha


# ---------------------------------------------------------------------------
# TypedDicts / dataclasses
# ---------------------------------------------------------------------------


class WriteActor(TypedDict, total=False):
    agent: str
    model: str
    model_label: str
    reasoning_level: str
    branch: str
    commit_sha: str
    lane_id: str


@dataclass
class ResolvedWriteContext:
    agent: str | None
    branch: str | None
    commit_sha: str | None
    lane_id: str | None
    model: str | None
    model_label: str | None
    reasoning_level: str | None
    branch_explicit: bool = False
    commit_explicit: bool = False


# ---------------------------------------------------------------------------
# Core-override helper  (M1: eliminate repeated monkeypatch-fallback pattern)
# ---------------------------------------------------------------------------


def _resolve_core_override(attr_name: str, fallback: Any) -> Any:
    """Return the override of *attr_name* from the loaded core module, or *fallback*.

    The core module registers monkeypatched test doubles on itself, so callers
    that respect the monkeypatch contract must prefer the core-module version
    when it exists.  This helper centralises that lookup so every write-context
    function uses a single, tested code path.
    """
    import sys  # noqa: PLC0415

    _core_mod = sys.modules.get("workbay_handoff_mcp.core")
    fn = getattr(_core_mod, attr_name, None) if _core_mod is not None else None
    return fn if fn is not None else fallback


# ---------------------------------------------------------------------------
# Build write actor
# ---------------------------------------------------------------------------


def build_write_actor(
    agent: str | None = None,
    model: str | None = None,
    model_label: str | None = None,
    reasoning_level: str | None = None,
    branch: str | None = None,
    commit_sha: str | None = None,
    lane_id: str | None = None,
) -> WriteActor:
    actor: WriteActor = {}
    normalized_model = _normalize_optional_text(model)
    explicit_model_label = _normalize_optional_text(model_label)
    derived_model_label = normalize_model_label(normalized_model)
    if (
        explicit_model_label is not None
        and derived_model_label is not None
        and explicit_model_label != derived_model_label
    ):
        raise ValueError(
            "actor.model_label does not match the canonical label for actor.model: "
            f"{explicit_model_label!r} != {derived_model_label!r}"
        )
    normalized_model_label = explicit_model_label or derived_model_label
    normalized_reasoning_level = normalize_reasoning_level(reasoning_level)
    derived_agent = normalize_model_identity(normalized_model_label, normalized_reasoning_level)
    normalized_agent = _normalize_optional_text(agent)
    normalized_branch = _normalize_optional_text(branch)
    normalized_commit_sha = _normalize_optional_text(commit_sha)
    # Validate the SHA against the active git repo and auto-expand
    # abbreviated forms to the full 40-char canonical hash. Bypassed
    # by WORKBAY_HANDOFF_SKIP_SHA_VALIDATION (both packages' test conftests
    # set it). Raises InvalidCommitShaError if the SHA is
    # non-hex or does not resolve to a real commit object in a
    # reachable git repo. See _validate_and_expand_commit_sha for
    # the full contract.
    normalized_commit_sha = _validate_and_expand_commit_sha(normalized_commit_sha)
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_model is not None:
        actor["model"] = normalized_model
    if normalized_model_label is not None:
        actor["model_label"] = normalized_model_label
    if normalized_reasoning_level is not None:
        actor["reasoning_level"] = normalized_reasoning_level
    if derived_agent is not None:
        actor["agent"] = derived_agent
    elif normalized_agent is not None:
        actor["agent"] = normalized_agent
    if normalized_branch is not None:
        actor["branch"] = normalized_branch
    if normalized_commit_sha is not None:
        actor["commit_sha"] = normalized_commit_sha
    if normalized_lane_id is not None:
        actor["lane_id"] = normalized_lane_id
    return actor


# ---------------------------------------------------------------------------
# Git / env utilities
# ---------------------------------------------------------------------------


def _first_non_empty_env(*keys: str) -> str | None:
    for key in keys:
        candidate = _normalize_optional_text(os.environ.get(key))
        if candidate is not None:
            return candidate
    return None


def _run_cmd(cmd: list[str], timeout: int = _SUBPROCESS_TIMEOUT) -> subprocess.CompletedProcess:
    config = get_runtime_config()
    return subprocess.run(
        cmd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        cwd=str(config.git_workspace_root),
        timeout=timeout,
    )


def _detect_git_write_context() -> tuple[str | None, str | None]:
    branch = resolve_env_alias("WORKBAY_HANDOFF_DEFAULT_BRANCH") or _first_non_empty_env(
        "GITHUB_HEAD_REF",
        "GITHUB_REF_NAME",
        "CI_COMMIT_REF_NAME",
        "BRANCH_NAME",
    )
    commit_sha = resolve_env_alias("WORKBAY_HANDOFF_DEFAULT_COMMIT_SHA") or _first_non_empty_env(
        "GITHUB_SHA",
        "CI_COMMIT_SHA",
    )
    if branch is not None and commit_sha is not None:
        return branch, commit_sha
    try:
        branch_proc = _run_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        if branch_proc.returncode == 0:
            raw_branch = _normalize_optional_text(branch_proc.stdout)
            if raw_branch is not None and raw_branch != "HEAD":
                branch = raw_branch
            elif raw_branch == "HEAD" and branch is None:
                branch = "detached-head"
    except Exception:
        pass
    try:
        commit_proc = _run_cmd(["git", "rev-parse", "HEAD"])
        if commit_proc.returncode == 0:
            commit_sha = _normalize_optional_text(commit_proc.stdout)
    except Exception:
        pass
    if branch is None:
        branch = "unknown-branch"
    return branch, commit_sha


def _detect_git_write_context_at(worktree_path: str | None) -> tuple[str | None, str | None]:
    normalized_path = _normalize_optional_text(worktree_path)
    if normalized_path is None:
        return None, None
    branch: str | None = None
    commit_sha: str | None = None
    try:
        branch_proc = subprocess.run(
            ["git", "-C", normalized_path, "rev-parse", "--abbrev-ref", "HEAD"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
        )
        if branch_proc.returncode == 0:
            raw_branch = _normalize_optional_text(branch_proc.stdout)
            if raw_branch is not None and raw_branch != "HEAD":
                branch = raw_branch
            elif raw_branch == "HEAD":
                branch = "detached-head"
    except Exception:
        pass
    try:
        commit_proc = subprocess.run(
            ["git", "-C", normalized_path, "rev-parse", "HEAD"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
        )
        if commit_proc.returncode == 0:
            commit_sha = _normalize_optional_text(commit_proc.stdout)
    except Exception:
        pass
    return branch, commit_sha


def _canonical_worktree_for_task(
    target_branch: str | None,
    *,
    workspace_root: str | os.PathLike[str] | None = None,
) -> str | None:
    """Derive the worktree path for ``target_branch`` via ``git worktree list --porcelain``.

    internal: this is the single source of truth for "which
    worktree does this task live in?". The stored
    ``handoff_state.target_worktree_path`` column is no longer read.
    Callers that previously used the column must call this helper with
    the row's canonical ``target_branch`` instead.

    Returns the worktree's path (the literal string from ``git worktree
    list --porcelain``'s ``worktree`` line, which git emits in absolute
    form). Returns ``None`` when ``target_branch`` is empty or ``None``
    — the row has no branch identity yet, so derivation is impossible
    and there is no failure to report. Raises
    :class:`WorktreeNotFoundError` when ``target_branch`` is set but
    ``git worktree list --porcelain`` either fails or produces no
    matching entry; that path is the loud-failure gate the internal
    plan calls for, so internal's wrong-cwd attribution invariant
    cannot be re-broken by stale row data silently falling through.

    ``workspace_root`` controls which git context the porcelain query
    runs against. When omitted, falls back to
    ``get_runtime_config().workspace_root`` and finally to cwd, mirroring
    the resolution order used by :func:`_git_repo_root`.
    """
    branch = _normalize_optional_text(target_branch)
    if branch is None:
        return None

    cwd: str
    if workspace_root is not None:
        cwd = os.fspath(workspace_root)
    else:
        try:
            config = get_runtime_config()
            cwd = str(config.workspace_root)
        except RuntimeError:
            cwd = os.getcwd()

    try:
        proc = subprocess.run(
            ["git", "worktree", "list", "--porcelain"],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorktreeNotFoundError(branch) from exc
    if proc.returncode != 0:
        raise WorktreeNotFoundError(branch)

    current_path: str | None = None
    for line in proc.stdout.splitlines():
        if not line.strip():
            current_path = None
            continue
        if line.startswith("worktree "):
            current_path = line[len("worktree ") :].strip()
            continue
        if line.startswith("branch ") and current_path is not None:
            ref = line[len("branch ") :].strip()
            ref_branch = ref.removeprefix("refs/heads/")
            if ref_branch == branch:
                return current_path
            continue

    raise WorktreeNotFoundError(branch)


def clear_worktree_pointer_for_close(
    conn: sqlite3.Connection,
    task_ref: str,
) -> dict[str, str | None] | None:
    """Clear a stale ``target_branch`` / ``target_worktree_path`` before a close write.

    The write-side guard (:class:`WorktreeNotFoundError`) is correct on
    active writes but too strict on the *close* transition. When a task's
    linked worktree has already been deleted (off-canonical close), deriving
    the worktree from the row's canonical ``target_branch`` would abort the
    very archive / status-done write that retires the row. During close the
    row is being retired and the pointer has no defensible live reader, so
    clearing it lets actor resolution fall back to caller cwd.

    Clears only when the worktree is genuinely missing: a canonical close
    (worktree intact) is left untouched, so active-path strictness is
    preserved everywhere except the off-canonical recovery. Returns the
    pre-clear ``{"target_branch", "target_worktree_path"}`` mapping when a
    clear happened, else ``None``.

    No-op (returns ``None``) when worktree derivation is bypassed via
    ``WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION``, so the synthetic-branch test
    fixtures that never materialize worktrees keep their stored pointers.
    Does not commit; the caller owns the transaction.
    """
    if not _worktree_derivation_enabled():
        return None
    row = conn.execute(
        "SELECT target_branch, target_worktree_path FROM handoff_state WHERE task_ref = ?",
        (task_ref,),
    ).fetchone()
    if row is None:
        return None
    target_branch = _normalize_optional_text(row["target_branch"])
    if target_branch is None:
        return None
    canonical_fn = _resolve_core_override("_canonical_worktree_for_task", _canonical_worktree_for_task)
    try:
        canonical_fn(target_branch)
    except WorktreeNotFoundError:
        pre_clear: dict[str, str | None] = {
            "target_branch": target_branch,
            "target_worktree_path": _normalize_optional_text(row["target_worktree_path"]),
        }
        conn.execute(
            "UPDATE handoff_state SET target_branch = NULL, target_worktree_path = NULL WHERE task_ref = ?",
            (task_ref,),
        )
        return pre_clear
    return None


def _git_is_ancestor(ancestor_sha: str | None, descendant_sha: str | None) -> bool | None:
    normalized_ancestor = _normalize_optional_text(ancestor_sha)
    normalized_descendant = _normalize_optional_text(descendant_sha)
    if normalized_ancestor is None or normalized_descendant is None:
        return None
    if normalized_ancestor == normalized_descendant:
        return True
    try:
        proc = _run_cmd(["git", "merge-base", "--is-ancestor", normalized_ancestor, normalized_descendant])
    except Exception:
        return None
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def _classify_commit_relation(reference_sha: str | None, candidate_sha: str | None) -> str:
    normalized_reference = _normalize_optional_text(reference_sha)
    normalized_candidate = _normalize_optional_text(candidate_sha)
    if normalized_reference is None or normalized_candidate is None:
        return "unknown"
    if normalized_reference == normalized_candidate:
        return "same"
    if _git_is_ancestor(normalized_reference, normalized_candidate) is True:
        return "descendant"
    if _git_is_ancestor(normalized_candidate, normalized_reference) is True:
        return "ancestor"
    if _git_is_ancestor(normalized_reference, normalized_candidate) is False:
        return "diverged"
    return "unknown"


def _workspace_git_context() -> dict[str, str | None]:
    detect_fn = _resolve_core_override("_detect_git_write_context", _detect_git_write_context)
    branch, commit_sha = detect_fn()
    return {
        "branch": branch,
        "commit_sha": commit_sha,
    }


# ---------------------------------------------------------------------------
# Write actor resolution
# ---------------------------------------------------------------------------


def collect_target_context_warnings(
    conn: sqlite3.Connection,
    ctx: ResolvedWriteContext,
    *,
    target_branch: str | None = None,
    target_worktree_path: str | None = None,
    task_ref: str | None = None,
    enforce_branch: bool = True,
) -> list[str]:
    """Return human-readable warnings when the resolved write context drifts from the active task target.

    Reads the active handoff_state row and compares its `target_branch` and
    `target_worktree_path` against the resolved actor branch and the current
    process working directory. Mismatches are returned as warning strings that
    callers can pass through to `_envelope(warnings=...)`.

    By default the check enforces branch drift for callers that mutate content.
    Callers can pass ``enforce_branch=False`` when the operation itself exists
    to resolve pointer drift (for example ``switch_task``). In that mode branch
    mismatches stay warning-only even when WORKBAY_HANDOFF_ENFORCE_BRANCH
    is truthy, unless WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT already
    disabled the guard globally. Worktree-path drift remains warning-only.

    Task resolution itself is fail-closed. When no explicit ``task_ref`` is
    provided, the guard delegates to ``_resolve_workspace_handoff_row`` and
    raises UnresolvedTaskContextError if the workspace cannot be resolved to a
    single active row.
    """
    normalized_task_ref = _normalize_optional_text(task_ref)
    try:
        if normalized_task_ref is not None:
            active = conn.execute(
                # internal: ``target_worktree_path`` is selected only
                # for the test bypass fallback; production reads derive via
                # :func:`_canonical_worktree_for_task` and never look at the
                # stored value. The column itself stays for migration headroom
                # until the follow-up MAINT drops it.
                "SELECT task_ref, target_branch, target_worktree_path FROM handoff_state WHERE task_ref = ?",
                (normalized_task_ref,),
            ).fetchone()
        else:
            # internal: no sentinel `WHERE id = 1` fallback.
            # Delegate to workspace-path resolution and fail closed when
            # ambiguity or an unregistered cwd leaves the active row
            # unresolved.
            from .shared_primitives import _resolve_workspace_handoff_row

            try:
                active = _resolve_workspace_handoff_row(conn)
            except ValueError as exc:
                raise UnresolvedTaskContextError(str(exc)) from exc
    except sqlite3.OperationalError:
        # Schema is older than this build (missing column). Skip the check.
        return []
    if active is None and normalized_task_ref is None:
        raise UnresolvedTaskContextError(
            "No active task in handoff_state. Call set_handoff_state first or pass task_ref explicitly."
        )
    if active is None:
        # No matching row to compare against: either an explicit task_ref
        # pointed at a row that does not exist, or the handoff_state table
        # is empty (bootstrap / test-isolation). Nothing to check.
        # Ambiguous multi-row cases are handled above by the resolver
        # raising UnresolvedTaskContextError.
        return []
    warnings: list[str] = []
    resolved_task_ref = normalized_task_ref
    if resolved_task_ref is None and active["task_ref"]:
        resolved_task_ref = _normalize_optional_text(active["task_ref"])
    resolved_target_branch = _normalize_optional_text(target_branch)
    if resolved_target_branch is None and active["target_branch"]:
        resolved_target_branch = _normalize_optional_text(active["target_branch"])
    # internal: derive from canonical target_branch instead of
    # reading the stored column. ``target_worktree_path`` parameter still
    # honored when explicitly passed (callers that already know the path
    # do not pay for a `git worktree list` shell-out). Falls back to the
    # stored column only when the test bypass is set; production always
    # derives.
    resolved_target_worktree_path = _normalize_optional_text(target_worktree_path)
    if resolved_target_worktree_path is None and resolved_target_branch is not None:
        if _worktree_derivation_enabled():
            try:
                canonical_fn = _resolve_core_override("_canonical_worktree_for_task", _canonical_worktree_for_task)
                resolved_target_worktree_path = canonical_fn(resolved_target_branch)
            except WorktreeNotFoundError:
                # Surface the missing worktree as a context_drift warning
                # rather than failing the read-side observability path.
                # The write-side resolver still raises loudly.
                warnings.append(
                    "context_drift: target_branch={} has no matching worktree "
                    "(`git worktree list --porcelain` returned no entry). Run "
                    "`git worktree add` for the canonical branch or archive the task.".format(resolved_target_branch)
                )
        elif active is not None and "target_worktree_path" in active.keys() and active["target_worktree_path"]:
            resolved_target_worktree_path = _normalize_optional_text(active["target_worktree_path"])
    if resolved_target_branch and ctx.branch and ctx.branch != resolved_target_branch:
        if (
            enforce_branch
            and _branch_enforcement_enabled()
            and _branch_target_is_enforceable(resolved_target_branch)
            and resolved_task_ref is not None
        ):
            raise BranchMismatchError(
                task_ref=resolved_task_ref,
                expected_branch=resolved_target_branch,
                actual_branch=ctx.branch,
            )
        if not (ctx.branch_explicit and ctx.branch == resolved_target_branch):
            warnings.append(
                "context_drift: actor.branch={} but active task target_branch={}. "
                "Consider switching to the canonical worktree before recording further events.".format(
                    ctx.branch, resolved_target_branch
                )
            )
    if resolved_target_worktree_path:
        cwd = os.path.abspath(os.getcwd())
        canonical = os.path.abspath(resolved_target_worktree_path)
        if cwd != canonical:
            actor_authoritative = (
                ctx.branch_explicit
                and normalized_task_ref is not None
                and resolved_target_branch is not None
                and ctx.branch == resolved_target_branch
            )
            if not actor_authoritative:
                warnings.append(
                    "context_drift: cwd={} but active task target_worktree_path={}. "
                    "Run `make context` to confirm or switch directories.".format(cwd, canonical)
                )
    return warnings


def _resolve_write_actor(
    conn: sqlite3.Connection,
    actor: WriteActor | None,
    *,
    task_ref: str | None = None,
    allow_missing_worktree_fallback: bool = False,
    derive_worktree_for_branch: str | None = None,
) -> ResolvedWriteContext:
    explicit_agent = _normalize_optional_text(actor.get("agent")) if actor else None
    explicit_model = _normalize_optional_text(actor.get("model")) if actor else None
    explicit_model_label = _normalize_optional_text(actor.get("model_label")) if actor else None
    derived_model_label = normalize_model_label(explicit_model)
    if (
        explicit_model_label is not None
        and derived_model_label is not None
        and explicit_model_label != derived_model_label
    ):
        raise ValueError(
            "actor.model_label does not match the canonical label for actor.model: "
            f"{explicit_model_label!r} != {derived_model_label!r}"
        )
    explicit_model_label = explicit_model_label or derived_model_label
    explicit_reasoning_level = normalize_reasoning_level(actor.get("reasoning_level")) if actor else None
    explicit_identity = normalize_model_identity(explicit_model_label, explicit_reasoning_level)
    explicit_branch = _normalize_optional_text(actor.get("branch")) if actor else None
    explicit_commit = _normalize_optional_text(actor.get("commit_sha")) if actor else None
    explicit_lane = _normalize_optional_text(actor.get("lane_id")) if actor else None
    # internal: split the formerly-fused agent default into two rungs. The
    # caller's self-declared identity is the agent-dimension analogue of "caller
    # cwd HEAD" (which already outranks the row for branch/commit below), so it
    # must rank ABOVE the inherited last-writer; the hardcoded ``codex`` stays
    # the last-resort fallback BELOW it. Env-unset behavior is unchanged: with
    # no self-declaration, agent resolution falls through to active_agent, then
    # fallback_agent.
    self_declared_agent = _normalize_optional_text(resolve_env_alias("WORKBAY_HANDOFF_DEFAULT_AGENT"))
    harness_agent = _infer_harness_agent_from_env()
    from .shared_primitives import _resolve_workspace_handoff_row  # noqa: PLC0415

    normalized_task_ref = _normalize_optional_text(task_ref)
    try:
        if normalized_task_ref is not None:
            active = conn.execute(
                """
                SELECT updated_by, updated_branch, updated_commit_sha, target_worktree_path, target_branch
                FROM handoff_state
                WHERE task_ref = ?
                """,
                (normalized_task_ref,),
            ).fetchone()
        else:
            active = _resolve_workspace_handoff_row(conn)
    except ValueError:
        active = None
    active_agent = _normalize_optional_text(active["updated_by"]) if active is not None else None
    active_branch = _normalize_optional_text(active["updated_branch"]) if active is not None else None
    active_commit = _normalize_optional_text(active["updated_commit_sha"]) if active is not None else None
    active_target_branch = (
        _normalize_optional_text(active["target_branch"])
        if active is not None and "target_branch" in active.keys()
        else None
    )
    worktree_branch = (
        _normalize_optional_text(derive_worktree_for_branch)
        if derive_worktree_for_branch is not None
        else active_target_branch
    )
    # internal: derive target_worktree_path from `git worktree list
    # --porcelain` keyed by canonical target_branch instead of reading the
    # row's stored column. The column was the source of the internal
    # wrong-cwd failure mode (stale rows pointing the resolver at the
    # primary tree even when the task's worktree had moved). Deriving
    # makes git the source of truth; missing-worktree raises loudly.
    # The test bypass falls back to the stored column for fixtures that
    # don't materialize real worktrees — see _worktree_derivation_enabled.
    if _worktree_derivation_enabled():
        canonical_fn = _resolve_core_override("_canonical_worktree_for_task", _canonical_worktree_for_task)
        try:
            active_worktree_path = canonical_fn(worktree_branch)
        except WorktreeNotFoundError:
            if not allow_missing_worktree_fallback:
                raise
            active_worktree_path = None
    else:
        active_worktree_path = _normalize_optional_text(active["target_worktree_path"]) if active is not None else None
    detect_at_fn = _resolve_core_override("_detect_git_write_context_at", _detect_git_write_context_at)
    task_git_branch, task_git_commit = detect_at_fn(active_worktree_path)
    preferred_task_branch = task_git_branch if task_git_branch not in (None, "unknown-branch") else None
    detect_fn = _resolve_core_override("_detect_git_write_context", _detect_git_write_context)
    git_branch, git_commit = detect_fn()
    preferred_git_branch = git_branch if git_branch not in (None, "unknown-branch") else None

    # internal: resolver precedence collapse. Caller cwd HEAD is the
    # ground truth when valid (real git context, not "unknown-branch"). The
    # row's stored context (task_git probe + active_branch) only fills in when
    # the caller has no usable git context. Callers who need different
    # provenance (e.g., GC sweeps writing on behalf of an archived task) pass
    # an explicit ``WriteActor`` — explicit_* still wins above caller cwd.
    #
    # This replaces implementation note's cwd-inside-task-worktree probe. The probe was a
    # narrow gate that only let cwd win when cwd was provably inside the
    # task's worktree; it left category-(b) callers from main attributing to
    # the row's branch, which was the internal charter inversion gap. The new
    # rule is uniform: cwd > row context, always. Callers ARE the truth unless
    # they explicitly opt out via ``actor``.
    resolved_agent = explicit_identity or explicit_agent or self_declared_agent or active_agent or harness_agent
    if resolved_agent is None:
        raise WriteActorAttributionError(
            "Cannot attribute write: pass actor.model or actor.agent on each write, "
            "set WORKBAY_HANDOFF_DEFAULT_AGENT, or ensure exactly one harness "
            "transcript env var is set (for example CLAUDE_SESSION_TRANSCRIPT_PATH "
            "or CODEX_SESSION_TRANSCRIPT_PATH)."
        )
    return ResolvedWriteContext(
        agent=resolved_agent,
        branch=(explicit_branch or preferred_git_branch or preferred_task_branch or active_branch or git_branch),
        commit_sha=(
            explicit_commit
            or (git_commit if preferred_git_branch is not None else None)
            or task_git_commit
            or active_commit
            or git_commit
        ),
        lane_id=explicit_lane,
        model=explicit_model,
        model_label=explicit_model_label,
        reasoning_level=explicit_reasoning_level,
        branch_explicit=explicit_branch is not None,
        commit_explicit=explicit_commit is not None,
    )
