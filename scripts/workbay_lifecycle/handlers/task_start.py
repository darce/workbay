"""Mutating ``task-start`` subcommand (internal).

Creates a conforming feature branch from a supplied task ref. The first
sub-slice (3.2) covers ``MODE=here`` — the branch is created in the
current repo and HEAD is moved to it. ``MODE=worktree`` (implementation note.3) and
linked-worktree reuse (implementation note.4) extend this body.

Receipt schema follows the documented §JSON Receipt Schema for the
git-first lifecycle primitives, plus the task-start additive fields
``mode`` / ``created_branch`` / ``reused_worktree`` / ``plan_path``.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import json
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Literal

import projection
import resolver
import uv_provisioning

from . import _common
from .plan_baseline import build_acceptance_next_command, evaluate_plan_baseline

_VALID_MODES = ("worktree", "here", "auto", "claim")

# [REF-19]-style single-sourcing of the branch grammar: prefer the protocol
# module the commit guard loads; fall back to the lifecycle resolver mirror so
# a consumer bootstrap without workbay_protocol still refuses the same set.
try:
    from workbay_protocol.branch_naming import (  # noqa: PLC0415
        TASK_REF_RE as _BRANCH_GRAMMAR_RE,
    )
except Exception:  # pragma: no cover - bootstrap / plain-python path
    _BRANCH_GRAMMAR_RE = resolver._TASK_REF_RE  # type: ignore[attr-defined]


def _branch_matches_grammar(branch: str) -> bool:
    """True when ``branch`` matches the commit-guard feature grammar."""
    return bool(branch) and _BRANCH_GRAMMAR_RE.match(branch) is not None


def _auto_suffixed_task_ref(task_ref: str) -> str:
    """Return a digit-bearing auto-suffix form of ``task_ref`` (T12).

    The commit guard requires the feature branch's task_ref group to
    contain a digit. Refs like ``internal`` produce
    ``feature/maint-context-receipt-fixture`` (no digit) and fail at first
    commit. Appending ``-01`` yields a conforming branch while keeping the
    original prefix readable.
    """
    base = (task_ref or "").strip().upper()
    if not base:
        return base
    if any(ch.isdigit() for ch in base):
        # Already has a digit but still non-conforming (e.g. single segment
        # "X1"); append a second segment to satisfy the hyphen grammar.
        candidate = f"{base}-01"
        branch = resolver.format_branch_name(candidate)
        if branch and _branch_matches_grammar(branch):
            return candidate
        return f"{base}-task-01"
    return f"{base}-01"


def _record_draft_plan_baseline(
    repo: Path,
    *,
    task_ref: str,
    plan_path: str,
) -> tuple[bool, str | None]:
    """Commit a net-new untracked plan as the accepted baseline (T21).

    Mirrors plan-accept ``--local`` for the untracked-on-main case without
    requiring a planning-review pass: the draft *is* the baseline at start.
    Preconditions: canonical root, currently on main, dirty paths are empty
    or exactly the plan file. Returns ``(ok, error)``.
    """
    canonical = resolver.canonical_workspace_root(repo) or repo
    try:
        if repo.resolve() != canonical.resolve():
            return False, "draft_baseline_requires_canonical_root"
    except OSError:
        return False, "draft_baseline_requires_canonical_root"

    branch = _current_branch(repo)
    if branch != "main":
        return False, "draft_baseline_requires_main_checkout"

    # Only the plan path may be dirty/untracked ([CON-11]-safe: do not
    # silently stage unrelated operator work).
    porcelain = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=all"]
    )
    if porcelain.returncode != 0:
        return False, "draft_baseline_git_status_failed"
    dirty: list[str] = []
    for line in (porcelain.stdout or "").splitlines():
        if len(line) < 4:
            continue
        path_part = line[3:].strip()
        if " -> " in path_part:
            path_part = path_part.split(" -> ", 1)[1].strip()
        dirty.append(path_part)
    if dirty and set(dirty) != {plan_path}:
        return False, "draft_baseline_requires_clean_tree"

    plan_file = repo / plan_path
    if not plan_file.is_file():
        return False, "draft_baseline_plan_missing"

    msg = f"docs({task_ref.lower()}): draft task plan baseline"
    staged = False
    for argv in (
        ["git", "-C", str(repo), "add", "--", plan_path],
        ["git", "-C", str(repo), "commit", "-q", "-m", msg, "--", plan_path],
    ):
        proc = _common.run_subprocess(argv)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()[:300]
            if staged:
                # [RES-01] rollback: a successful ``git add`` followed by a
                # failed commit must not leave the plan staged on main — the
                # half-start would surprise the next status/commit on the
                # primary checkout. Best-effort unstage; the file itself is
                # untouched either way.
                _common.run_subprocess(
                    ["git", "-C", str(repo), "reset", "-q", "--", plan_path]
                )
            return False, f"draft_baseline_commit_failed: {err}"
        staged = True
    return True, None


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_workspace_summary_view(repo: Path) -> _common.WorkspaceSummaryView:
    """Derive the workspace summary view via ``render-handoff --no-write``.

    internal: the on-disk ``CURRENT_TASK.json`` is no longer
    the source of truth for the ambiguity guard. Each call derives the
    view from MCP's live state through the pure-read
    ``render_handoff(kind='current_task', write_file=False)`` path,
    eliminating the stale-projection failure mode that motivated the
    internal split.

    Fail-open semantics (CLI unavailable, malformed envelope, parse
    error → ``shape="none"``) match the prior file-based reader.
    """
    return _common.derive_workspace_summary_view(repo)


def _read_active_state(repo: Path) -> dict[str, Any]:
    """Best-effort read of the live ``active`` block.

    Returns the per-task payload only when the workspace summary
    resolves to ``shape="single"``; ``workspace_ambiguous`` and
    ``none`` both yield an empty dict so plan-path lookup degrades to
    "no active plan" rather than picking an arbitrary listed task.
    """
    view = _read_workspace_summary_view(repo)
    if view.shape == "single" and isinstance(view.active, dict):
        return view.active
    return {}


def _emit_error(
    reason: str,
    *,
    task_ref: str | None = None,
    branch: str = "",
    events: list[str] | None = None,
    handoff_projection: str = "error",
    conflict_kind: str | None = None,
    conflict_category: str | None = None,
    plan_path: str | None = None,
    plan_baseline: dict[str, Any] | None = None,
    recovery_kind: str | None = None,
    safe_next_commands: list[dict[str, str]] | None = None,
    worktree_path: str = "",
    head: str = "",
    claimed_plan_id: str | None = None,
) -> int:
    receipt: dict[str, Any] = {
        "ok": False,
        "command": "task-start",
        "task_ref": task_ref,
        "branch": branch,
        "worktree_path": worktree_path,
        "head": head,
        "handoff_projection": handoff_projection,
        "events": events if events is not None else [],
        "mode": "",
        "created_branch": False,
        "reused_worktree": False,
        "plan_path": plan_path,
        "claimed_plan_id": claimed_plan_id,
        "plan_baseline": plan_baseline,
        "recovery_kind": recovery_kind,
        "conflict_kind": conflict_kind,
        "conflict_category": conflict_category,
        "error": reason,
    }
    if safe_next_commands is not None:
        receipt["safe_next_commands"] = safe_next_commands
    _common.emit(receipt)
    return 2


def _checkout_branch_here(repo: Path, branch: str) -> bool:
    """Create+checkout ``branch`` in ``repo``. Returns True when created."""
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "checkout", "-q", "-b", branch]
    )
    return proc.returncode == 0


def _current_branch(repo: Path) -> str:
    """Return the currently checked-out branch in ``repo`` or ''."""
    proc = _common.run_subprocess(["git", "-C", str(repo), "branch", "--show-current"])
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def _rollback_branch_here(repo: Path, previous_branch: str, branch: str) -> None:
    """Best-effort teardown for a here-mode branch created earlier in this run.

    Mirrors ``_rollback_linked_worktree`` for ``MODE=here``: when uv sync
    fails after we created+checked-out the feature branch in the current
    repo, switch back to the prior branch (if known) and delete the new
    branch so the caller does not stay parked on a half-started lifecycle.
    """
    if previous_branch:
        _common.run_subprocess(
            ["git", "-C", str(repo), "checkout", "-q", previous_branch]
        )
    _common.run_subprocess(["git", "-C", str(repo), "branch", "-D", branch])


def _derive_worktree_path(
    primary: Path, task_ref: str, *, plan_id: str | None = None
) -> Path:
    """Sibling-of-primary convention: ``<primary>-<task-ref-lower>``.

    Mirrors the internal worktree layout already in use by this repo
    (``workbay`` + ``-internal-40``). Stays sibling-only
    so the linked worktree never nests inside the primary tree. When the
    task implements a numbered plan doc, a trailing ``-plan<NNNN>`` segment
    is appended so the worktree directory names the plan it implements
    (matching the branch suffix).
    """
    suffix = task_ref.lower()
    if plan_id:
        suffix = f"{suffix}-plan{plan_id.lower()}"
    return primary.parent / f"{primary.name}-{suffix}"


_PLAN_DOC_ID_RE = re.compile(r"(?:^|/)(\d{4})-[^/]+\.md$")


def _allocate_next_plan_id(repo: Path) -> str:
    """Return the next free four-digit plan id, ``max(<id>) + 1`` over **all
    refs and all linked worktrees** (and the working tree), per the canonical
    §Plan Id Allocation recipe (``docs/workbay/rules/planning-artifact-home.md``).

    A plan id is global across every branch: ``git log --all`` covers every ref
    (linked-worktree branches share the primary object store), and the
    working-tree glob additionally catches a plan present on disk but not yet
    committed. Picking ``max(main) + 1`` instead is how two concurrent branches
    collide (the 0053/0054 double-claim). Returns ``"0001"`` when the pipeline
    is empty.
    """
    ids: set[int] = set()
    proc = _common.run_subprocess(
        [
            "git",
            "-C",
            str(repo),
            "log",
            "--all",
            "--pretty=format:",
            "--name-only",
            "--",
            "docs/plans",
        ]
    )
    if proc.returncode == 0:
        for line in (proc.stdout or "").splitlines():
            match = _PLAN_DOC_ID_RE.search(line.strip())
            if match is not None:
                ids.add(int(match.group(1)))
    plans_dir = repo / "docs" / "plans"
    if plans_dir.is_dir():
        for entry in plans_dir.glob("*.md"):
            match = _PLAN_DOC_ID_RE.search(entry.name)
            if match is not None:
                ids.add(int(match.group(1)))
    next_id = (max(ids) + 1) if ids else 1
    return f"{next_id:04d}"


def _write_and_commit_plan_stub(
    worktree_path: Path,
    plan_id: str,
    task_ref: str,
    slug: str | None,
    objective: str,
) -> str | None:
    """Write + commit a ``# Plan <NNNN> — <title>`` stub onto the feature branch
    checked out at ``worktree_path``; return its repo-relative path.

    implementation note D1: the committed-but-unmerged doc *is* the id claim — it makes the
    freshly-allocated id visible to the all-refs allocation scan before any
    concurrent starter can re-pick it. Best-effort: any failure returns ``None``
    (the ``-plan<NNNN>`` suffix naming already happened, so a missing stub only
    means the claim is not yet durable, never that task-start failed). Only the
    stub path is staged, so gitignored provisioning artifacts never ride along.
    """
    doc_slug = (slug or task_ref).strip().lower()
    rel_path = f"docs/plans/{plan_id}-{doc_slug}.md"
    title = objective.strip() if objective and objective.strip() else task_ref
    target = worktree_path / "docs" / "plans" / f"{plan_id}-{doc_slug}.md"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            f"# Plan {plan_id} — {title}\n\n"
            f"<!-- Plan id {plan_id} claimed at worktree procurement for task "
            f"{task_ref} (`--plan-intent=yes`). The committed-but-unmerged doc "
            f"is the id claim; replace this stub with the real plan before "
            f"planning review. -->\n"
        )
    except OSError:
        return None
    add = _common.run_subprocess(
        ["git", "-C", str(worktree_path), "add", "--", rel_path]
    )
    if add.returncode != 0:
        return None
    commit = _common.run_subprocess(
        [
            "git",
            "-C",
            str(worktree_path),
            "commit",
            "-q",
            "-m",
            f"docs(plan): claim plan {plan_id} for {task_ref} at procurement",
            "--",
            rel_path,
        ]
    )
    if commit.returncode != 0:
        return None
    return rel_path


def _is_registered_linked_worktree(primary: Path, worktree_path: Path) -> bool:
    """Return True when ``worktree_path`` is a git-linked worktree (not primary)."""
    primary_str = str(primary)
    candidate = str(worktree_path)
    for entry in resolver.linked_worktrees(primary):
        if entry.get("path") == candidate and candidate != primary_str:
            return True
    return False


def _find_linked_worktree_for_branch(primary: Path, branch: str) -> Path | None:
    """Return the path of the linked worktree owning ``branch``, or None.

    Skips the primary worktree itself so a checked-out branch on the
    main repo never masks a real linked worktree pickup.
    """
    primary_str = str(primary)
    for entry in resolver.linked_worktrees(primary):
        if entry.get("branch") == branch and entry.get("path") != primary_str:
            path = entry.get("path")
            if isinstance(path, str) and path:
                return Path(path)
    return None


_ConflictKind = Literal[
    "same_task_elsewhere",
    "branch_collision",
    "worktree_path_collision",
    "mode_here_implementation_conflict",
    "claim_existing_worktree",
]


@dataclasses.dataclass(frozen=True)
class _RealConflict:
    """Structured task-start refusal cause (internal).

    ``category`` partitions the kinds: ``"collision"`` for name clashes
    detectable from git/filesystem state alone (branch / worktree path),
    ``"policy"`` for worktree-singleton-class rules (same task already
    live elsewhere / MODE=here against another implementation primary),
    and ``"recoverable"`` (internal) for the claim path — the requested
    task's own branch already has an unowned linked worktree, which the
    caller can adopt via ``MODE=claim`` rather than being told to delete
    it. See ``docs/workbay/rules/development-workflow.md`` §Task-Start
    Identity Resolution.
    """

    kind: _ConflictKind
    category: Literal["collision", "policy", "recoverable"]
    message: str
    conflicting_task_ref: str | None = None
    conflicting_branch: str | None = None
    conflicting_path: str | None = None


def _existing_worktree_owner(
    live: Iterable[Mapping[str, Any]],
    *,
    task_ref: str,
    existing_path: Path,
) -> str | None:
    """Return the task_ref of a *different* live row claiming ``existing_path``.

    internal: an existing linked worktree on the requested branch
    is only a hard ``branch_collision`` when some other live task already
    owns that exact worktree path. Otherwise it is an unowned worktree
    the caller may claim. Ownership is decided purely by
    ``target_worktree_path`` equality so a stale branch label on the
    worktree never masks a genuine cross-task claim.
    """
    existing_str = str(existing_path)
    for row in live:
        if not isinstance(row, Mapping):
            continue
        row_ref = row.get("task_ref")
        if not isinstance(row_ref, str) or not row_ref or row_ref == task_ref:
            continue
        row_path = row.get("target_worktree_path")
        if isinstance(row_path, str) and row_path == existing_str:
            return row_ref
    return None


def _detect_real_conflict(
    repo: Path,
    *,
    primary: Path,
    task_ref: str,
    target_branch: str,
    mode: str,
    live_tasks: Iterable[Mapping[str, Any]],
    plan_id: str | None = None,
) -> _RealConflict | None:
    """Return the first real conflict that blocks this task-start, or None.

    This is the pure replacement for the pre-internal
    ``workspace_ambiguous`` veto. ``live_tasks`` enumerates the live
    rows from the workspace summary; the helper consults git
    (``_find_linked_worktree_for_branch``, ``git branch --list``,
    ``Path.exists``) for resource collisions and the live-row payload
    for policy conflicts. Order of checks (deterministic):

    1. ``same_task_elsewhere`` — task already claims a different worktree.
    2. ``mode_here_implementation_conflict`` — MODE=here against a
       primary attached to a different implementation task.
    3. ``branch_collision`` — target branch attached to another
       worktree, or present as a local branch on the primary.
    4. ``worktree_path_collision`` — sibling-of-primary derived path is
       already attached to another task / already exists.
    """
    primary_str = str(primary)
    live = [t for t in live_tasks if isinstance(t, Mapping)]

    # internal: probe for an existing linked worktree on the
    # requested ``target_branch`` once up-front. When the live row for
    # ``task_ref`` claims that same worktree path, the request is the
    # canonical "resume in own worktree" case — neither
    # ``same_task_elsewhere`` nor ``branch_collision`` should fire, so
    # the caller's downstream ``_find_linked_worktree_for_branch`` reuse
    # path can run.
    existing_branch_worktree = _find_linked_worktree_for_branch(primary, target_branch)

    for row in live:
        row_ref = row.get("task_ref")
        if row_ref != task_ref:
            continue
        row_path = row.get("target_worktree_path")
        if not (isinstance(row_path, str) and row_path):
            continue
        if row_path == primary_str:
            continue
        if (
            existing_branch_worktree is not None
            and Path(row_path) == existing_branch_worktree
        ):
            return None
        # Plan-suffix recompute may change ``target_branch`` while the live
        # row still points at a legacy linked worktree on the old branch
        # name. Any registered linked worktree at ``row_path`` is still the
        # canonical resume case for this task (internal).
        if _is_registered_linked_worktree(primary, Path(row_path)):
            return None
        return _RealConflict(
            kind="same_task_elsewhere",
            category="policy",
            conflicting_task_ref=task_ref,
            conflicting_path=row_path,
            message=(
                f"task_ref={task_ref!r} is already live at "
                f"worktree {row_path!r}; resume there or finish that "
                f"task before re-starting"
            ),
        )

    if mode == "here":
        current = _current_branch(primary)
        if current and current != target_branch:
            for row in live:
                row_path = row.get("target_worktree_path")
                row_branch = row.get("target_branch")
                row_ref = row.get("task_ref")
                if (
                    isinstance(row_path, str)
                    and row_path == primary_str
                    and isinstance(row_branch, str)
                    and row_branch != "main"
                    and row_branch != target_branch
                    and isinstance(row_ref, str)
                    and row_ref != task_ref
                ):
                    return _RealConflict(
                        kind="mode_here_implementation_conflict",
                        category="policy",
                        conflicting_task_ref=row_ref,
                        conflicting_branch=row_branch,
                        conflicting_path=primary_str,
                        message=(
                            f"MODE=here would overwrite primary checkout "
                            f"currently attached to live implementation "
                            f"task {row_ref!r} (branch={row_branch!r}); "
                            f"switch task or use MODE=worktree"
                        ),
                    )

    if existing_branch_worktree is not None:
        owner = _existing_worktree_owner(
            live, task_ref=task_ref, existing_path=existing_branch_worktree
        )
        if owner is not None:
            return _RealConflict(
                kind="branch_collision",
                category="collision",
                conflicting_task_ref=owner,
                conflicting_branch=target_branch,
                conflicting_path=str(existing_branch_worktree),
                message=(
                    f"target_branch={target_branch!r} is attached to "
                    f"worktree {str(existing_branch_worktree)!r}, owned by "
                    f"live task {owner!r}; finish that task or choose a "
                    f"different branch"
                ),
            )
        # internal: unowned existing worktree on the requested
        # branch is recoverable — the caller can adopt it via MODE=claim
        # instead of being told to delete a valid worktree.
        return _RealConflict(
            kind="claim_existing_worktree",
            category="recoverable",
            conflicting_branch=target_branch,
            conflicting_path=str(existing_branch_worktree),
            message=(
                f"target_branch={target_branch!r} already has an unowned "
                f"worktree at {str(existing_branch_worktree)!r}; claim it "
                f"with MODE=claim"
            ),
        )
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "branch", "--list", target_branch]
    )
    if proc.returncode == 0 and (proc.stdout or "").strip():
        return _RealConflict(
            kind="branch_collision",
            category="collision",
            conflicting_branch=target_branch,
            message=(
                f"target_branch={target_branch!r} already exists locally; "
                f"choose a different branch or delete the existing one"
            ),
        )

    if mode == "worktree":
        derived = _derive_worktree_path(primary, task_ref, plan_id=plan_id)
        if derived.exists():
            return _RealConflict(
                kind="worktree_path_collision",
                category="collision",
                conflicting_path=str(derived),
                message=(
                    f"derived worktree path {str(derived)!r} already exists; "
                    f"remove it or choose a different task slug"
                ),
            )

    return None


def _create_linked_worktree(primary: Path, target: Path, branch: str) -> bool:
    """Run ``git worktree add -b <branch> <target>`` from ``primary``."""
    proc = _common.run_subprocess(
        [
            "git",
            "-C",
            str(primary),
            "worktree",
            "add",
            "-q",
            "-b",
            branch,
            str(target),
        ]
    )
    return proc.returncode == 0


_BOOTSTRAP_MARKER = ".workbay-bootstrap.json"
_DEFAULT_ADOPT_CMD = ("workbay-bootstrap", "adopt-worktree")


def _adopt_overlay_command(*, worktree_path: Path | None = None) -> list[str]:
    """Resolve the bootstrap adopt command, overridable via ``WORKBAY_ADOPT_CMD``.

    Set ``WORKBAY_ADOPT_CMD`` to override the command, or to an empty string to
    disable adoption. Without an override, prefer a freshly provisioned
    worktree-local ``.venv`` ``workbay-bootstrap`` script when present, then
    fall back to the installed ``workbay-bootstrap adopt-worktree`` console
    script on PATH (the symmetric call to the consumer's ``workbay-bootstrap
    install``) — git-only delivery has no per-session ``uvx`` PyPI resolve. This
    keeps source checkouts on the same bootstrap code under review instead of
    adopting with a mismatched install.
    """
    raw = os.environ.get("WORKBAY_ADOPT_CMD")
    if raw is None:
        if worktree_path is not None:
            local_script = _common._venv_console_script(worktree_path, "workbay-bootstrap")
            if local_script is not None:
                return [str(local_script), "adopt-worktree"]
        return list(_DEFAULT_ADOPT_CMD)
    return shlex.split(raw)


def _resolve_materialized_overlay_root(start: Path) -> tuple[Path | None, str | None]:
    """Walk upward from ``start`` for the nearest *materialized* overlay.

    Mirrors workbay-bootstrap ``worktree.primary_overlay_root`` (without
    importing that package — the inverse-dependency invariant): prefer the
    nearest directory that carries BOTH a ``.workbay-bootstrap.json`` marker
    and a ``.workbay/remote`` clone. Returns ``(overlay_root, None)`` on
    success, else ``(None, reason)`` where ``reason`` is ``"no_overlay_clone"``
    (a marker exists at/above ``start`` but none has a clone — e.g. the monorepo
    self-host) or ``"no_overlay_marker"`` (no marker found at all).
    """
    saw_marker = False
    current = start
    while True:
        if (current / _BOOTSTRAP_MARKER).is_file():
            saw_marker = True
            if (current / ".workbay" / "remote").exists():
                return current, None
        if current.parent == current:  # reached the filesystem root
            break
        current = current.parent
    return None, ("no_overlay_clone" if saw_marker else "no_overlay_marker")


def _adopt_overlay(primary: Path, worktree_path: Path) -> dict[str, Any]:
    """Best-effort: adopt the bootstrap overlay into a freshly created worktree.

    implementation note S3 — the durable, harness-agnostic self-heal trigger for the
    supported ``make task-start`` worktree flow. Cross-package via subprocess
    (workbay-system takes no hard dependency on workbay-bootstrap). NEVER
    fatal: a missing/older bootstrap or a non-overlay primary just leaves the
    worktree healable later via ``adopt-worktree`` / ``doctor --apply``.

    Gated on resolving a *materialized* overlay (marker + ``.workbay/remote``
    clone) at or above ``primary`` via the same upward walk workbay-bootstrap's
    ``primary_overlay_root`` uses — so a nested-source layout (the git repo lives
    inside the overlay dir, marker an ancestor) is healed rather than skipped
    (implementation note S4 / revC-nested-source-marker-gate-mismatch), while a tracked
    marker with no clone (the workbay monorepo self-host) still skips so the
    (potentially network-touching) bootstrap call never fires doomed. The walk is
    re-implemented locally because workbay-system must take no dependency on
    workbay-bootstrap; the bootstrap CLI then resolves the same root itself.
    """
    _overlay_root, skip_reason = _resolve_materialized_overlay_root(primary)
    if skip_reason is not None:
        return {"adopted": False, "skipped": skip_reason}
    cmd = _adopt_overlay_command(worktree_path=worktree_path)
    if not cmd:
        return {"adopted": False, "skipped": "disabled"}
    proc = _common.run_subprocess([*cmd, "--target", str(worktree_path)], timeout=120)
    if proc.returncode == 0:
        return {"adopted": True, "skipped": None}
    return {"adopted": False, "skipped": f"exit_{proc.returncode}"}


_DEFAULT_BOOTSTRAP_SURFACES_CMD = ("workbay-bootstrap", "bootstrap-surfaces")
_DEFAULT_SELFHOST_SURFACES_TIMEOUT = 300.0


def _selfhost_surfaces_timeout() -> float:
    """Resolve the self-host surface bootstrap timeout from env, mirroring
    :func:`_worktree_bootstrap_timeout` (``WORKBAY_BOOTSTRAP_SURFACES_TIMEOUT``;
    falls back to the default on unset/empty/non-int)."""
    raw = os.environ.get("WORKBAY_BOOTSTRAP_SURFACES_TIMEOUT")
    if raw is None or raw == "":
        return _DEFAULT_SELFHOST_SURFACES_TIMEOUT
    try:
        return float(int(raw))
    except ValueError:
        return _DEFAULT_SELFHOST_SURFACES_TIMEOUT


def _bootstrap_surfaces_command(*, worktree_path: Path | None = None) -> list[str]:
    """Resolve the ``bootstrap-surfaces`` command, overridable via
    ``WORKBAY_BOOTSTRAP_SURFACES_CMD`` (empty string disables it).

    Mirrors :func:`_adopt_overlay_command`: prefer a freshly provisioned
    worktree-local ``.venv`` ``workbay-bootstrap`` console script (keeps source
    checkouts on the bootstrap code under review), else fall back to the
    installed ``workbay-bootstrap bootstrap-surfaces`` console script on PATH
    (git-only delivery has no per-session ``uvx`` PyPI resolve).
    """
    raw = os.environ.get("WORKBAY_BOOTSTRAP_SURFACES_CMD")
    if raw is None:
        if worktree_path is not None:
            local_script = _common._venv_console_script(worktree_path, "workbay-bootstrap")
            if local_script is not None:
                return [str(local_script), "bootstrap-surfaces"]
        return list(_DEFAULT_BOOTSTRAP_SURFACES_CMD)
    return shlex.split(raw)


def _selfhost_surfaces_receipt(
    proc: subprocess.CompletedProcess[str],
) -> dict[str, Any]:
    """Fold a ``bootstrap-surfaces --json`` subprocess into the additive
    ``selfhost_surfaces`` receipt. The subcommand's own ``ok``/``steps`` win
    when its JSON receipt parses; otherwise the exit code decides ``ok``."""
    ok = proc.returncode == 0
    steps: list[Any] = []
    try:
        payload = json.loads(proc.stdout)
    except (json.JSONDecodeError, ValueError):
        payload = None
    if isinstance(payload, dict):
        if isinstance(payload.get("steps"), list):
            steps = payload["steps"]
        if "ok" in payload:
            ok = bool(payload["ok"])
    return {
        "ran": True,
        "ok": ok,
        "skipped": None,
        "exit": proc.returncode,
        "steps": steps,
    }


def _maybe_run_selfhost_surfaces(
    mode: str,
    overlay_adopt: dict[str, Any],
    primary_root: Path,
    worktree_path: Path,
) -> dict[str, Any]:
    """Best-effort: emit agent surfaces locally for a self-host worktree.

    implementation note — gated on ``_adopt_overlay`` returning ``no_overlay_clone`` (the
    workbay monorepo self-host: tracked marker, no ``.workbay/remote`` clone),
    so the consumer adopt path is skipped and the linked worktree would inherit
    no effective plugin tree / prompts / Cursor surfaces. Runs ``bootstrap-
    surfaces`` against the new worktree. NEVER fatal: a missing/older bootstrap
    leaves the worktree healable later via ``doctor --apply``.
    """
    if mode not in ("worktree", "claim") or worktree_path == primary_root:
        return {"ran": False, "ok": None, "skipped": "not_worktree", "exit": None, "steps": []}
    if overlay_adopt.get("skipped") != "no_overlay_clone":
        return {"ran": False, "ok": None, "skipped": "not_self_host", "exit": None, "steps": []}
    cmd = _bootstrap_surfaces_command(worktree_path=worktree_path)
    if not cmd:
        return {"ran": False, "ok": None, "skipped": "disabled", "exit": None, "steps": []}
    proc = _common.run_subprocess(
        [
            *cmd,
            "--target",
            str(worktree_path),
            "--primary",
            str(primary_root),
            "--json",
        ],
        timeout=_selfhost_surfaces_timeout(),
    )
    return _selfhost_surfaces_receipt(proc)


_DEFAULT_WORKTREE_BOOTSTRAP_TIMEOUT = 600.0


def _worktree_bootstrap_command() -> str | None:
    """Resolve the post-provision bootstrap shell command from env."""
    raw = os.environ.get("WORKBAY_WORKTREE_BOOTSTRAP_CMD")
    if raw is None or raw == "":
        return None
    return raw


def _worktree_bootstrap_timeout() -> float:
    raw = os.environ.get("WORKBAY_WORKTREE_BOOTSTRAP_TIMEOUT")
    if raw is None or raw == "":
        return _DEFAULT_WORKTREE_BOOTSTRAP_TIMEOUT
    try:
        return float(int(raw))
    except ValueError:
        return _DEFAULT_WORKTREE_BOOTSTRAP_TIMEOUT


def _stream_captured_subprocess_output(proc: subprocess.CompletedProcess[str]) -> None:
    if proc.stdout:
        sys.stderr.write(proc.stdout)
        if not proc.stdout.endswith("\n"):
            sys.stderr.write("\n")
    if proc.stderr:
        sys.stderr.write(proc.stderr)
        if not proc.stderr.endswith("\n"):
            sys.stderr.write("\n")


def _run_worktree_bootstrap(worktree_path: Path, cmd: str) -> dict[str, Any]:
    """Best-effort: run a shell bootstrap command rooted at the worktree."""
    proc = _common.run_subprocess(
        ["sh", "-c", cmd],
        cwd=str(worktree_path),
        timeout=_worktree_bootstrap_timeout(),
    )
    _stream_captured_subprocess_output(proc)
    return {
        "ran": True,
        "ok": proc.returncode == 0,
        "skipped": None,
        "exit": proc.returncode,
    }


def _maybe_run_worktree_bootstrap(
    mode: str,
    primary_root: Path,
    worktree_path: Path,
) -> dict[str, Any]:
    if mode not in ("worktree", "claim") or worktree_path == primary_root:
        return {"ran": False, "ok": None, "skipped": "not_worktree", "exit": None}
    cmd = _worktree_bootstrap_command()
    if cmd is None:
        return {"ran": False, "ok": None, "skipped": "unset", "exit": None}
    return _run_worktree_bootstrap(worktree_path, cmd)


def _rollback_linked_worktree(primary: Path, target: Path, branch: str) -> None:
    """Best-effort teardown for a worktree created earlier in this run.

    Used when a downstream provisioning step (uv sync) fails so we do not
    leave a half-provisioned worktree behind. Failure-paths are silenced
    because a rollback that itself fails is a strictly worse outcome
    than the already-reported sync error.
    """
    _common.run_subprocess(
        ["git", "-C", str(primary), "worktree", "remove", "--force", str(target)]
    )
    _common.run_subprocess(["git", "-C", str(primary), "branch", "-D", branch])


_PLAN_REVISION_SUFFIX_RE = re.compile(r"-r(\d+)\.md$")


def _plan_revision_rank(path: Path) -> tuple[int, int, str]:
    """Sort key: ``-rN.md`` suffix wins over un-suffixed; higher N wins."""
    match = _PLAN_REVISION_SUFFIX_RE.search(path.name)
    if match is not None:
        return (1, int(match.group(1)), path.name)
    return (0, 0, path.name)


def _resolve_plan_glob(
    repo: Path, plan_glob: str, plan_revision: str | None
) -> tuple[str | None, str | None]:
    """Resolve ``--plan`` glob to a single repo-relative plan path.

    Returns ``(plan_path, error)``. ``plan_path`` is repo-relative, e.g.
    ``docs/plans/0099-multi-plan-demo-r2.md``. ``error`` is non-None on
    failure: ``plan_glob_no_match`` when the glob matches nothing, and
    ``plan_revision_not_in_glob`` when an explicit pin is not among the
    matches. When multiple files match, ``-rN.md`` suffix variants
    outrank the un-suffixed file and higher N wins.
    """
    matches = sorted(repo.glob(plan_glob), key=_plan_revision_rank)
    if not matches:
        return None, "plan_glob_no_match"
    if plan_revision:
        for match in matches:
            if match.name == plan_revision:
                return str(match.relative_to(repo)), None
        return None, "plan_revision_not_in_glob"
    return str(matches[-1].relative_to(repo)), None


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle task-start", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--objective", dest="objective", default="")
    parser.add_argument("--slug", dest="slug", default=None)
    parser.add_argument(
        "--mode",
        dest="mode",
        default="worktree",
        choices=_VALID_MODES,
    )
    parser.add_argument("--plan", dest="plan", default=None)
    parser.add_argument("--plan-revision", dest="plan_revision", default=None)
    # implementation note D1: opt-in plan-id claim at procurement. ``yes`` is the only
    # path that burns a plan id without an existing ``--plan``/live-row plan;
    # ``no`` (the default for noninteractive callers) keeps the bare
    # ``feature/<task-ref>`` branch so genuinely standalone audits never spend
    # an id.
    parser.add_argument(
        "--plan-intent",
        dest="plan_intent",
        choices=("yes", "no"),
        default="no",
    )
    parser.add_argument(
        "--auto-suffix",
        dest="auto_suffix",
        action="store_true",
        default=False,
        help=(
            "When the task ref would produce a branch the commit guard "
            "rejects, apply the suggested digit-bearing auto-suffix "
            "instead of refusing (T12)."
        ),
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    task_ref = (args.task or "").strip().upper()
    if not task_ref:
        return _emit_error("task_ref_required")

    branch = resolver.format_branch_name(task_ref, slug=args.slug)
    if branch is None:
        return _emit_error("task_ref_required")

    repo = resolver.repo_root()
    if repo is None:
        return _emit_error("not_in_git_repo")

    # internal: ``--plan <glob>`` selects from possibly multiple
    # plan-revision files; lex-latest wins unless ``--plan-revision``
    # pins one. Resolution runs before any git/uv mutation so a failed
    # resolution never leaves a half-started worktree.
    plan_glob_path: str | None = None
    if args.plan:
        plan_glob_path, plan_error = _resolve_plan_glob(
            repo, args.plan, args.plan_revision
        )
        if plan_error is not None:
            return _emit_error(
                plan_error,
                task_ref=task_ref,
                branch=branch,
            )

    active_state = _read_active_state(repo)
    plan_path_for_gate = plan_glob_path
    if (
        plan_path_for_gate is None
        and _common.snapshot_is_live(active_state)
        and active_state.get("task_ref") == task_ref
    ):
        active_plan_path = active_state.get("task_plan_path")
        if isinstance(active_plan_path, str):
            plan_path_for_gate = active_plan_path

    # internal: embed the implementing plan id (``plan<NNNN>``) as a
    # trailing branch/worktree segment once the plan path is resolved.
    # Recomputed here (not at the early `format_branch_name`) so the
    # baseline-gate error receipts, the conflict detector, the git
    # mutation and the projection all carry the plan-suffixed branch.
    # ``plan<NNNN>`` parses like a slug segment, so the task-ref resolver
    # is unaffected. No plan -> no suffix (unchanged behavior).
    plan_id = resolver.extract_plan_id(plan_path_for_gate)
    if plan_id:
        branch = resolver.format_branch_name(
            task_ref, slug=args.slug, plan_id=plan_id
        )

    # implementation note D1: when no plan is resolvable but the operator marked the task
    # plan-bound (``--plan-intent=yes``), claim the next free id *now* and name
    # branch+worktree ``-plan<NNNN>``. There is no accepted baseline to gate
    # against — the claim precedes the plan — so ``plan_path_for_gate`` stays
    # None and the baseline gate below is intentionally skipped.
    claimed_plan_id: str | None = None
    if plan_id is None and args.plan_intent == "yes":
        claimed_plan_id = _allocate_next_plan_id(repo)
        plan_id = claimed_plan_id
        branch = resolver.format_branch_name(
            task_ref, slug=args.slug, plan_id=plan_id
        )

    # Track draft-baseline side-effect for the success receipt (T21).
    draft_baseline_recorded = False
    plan_baseline_receipt: dict[str, Any] | None = None
    events_prefix: list[str] = []

    # T12: validate the fully-resolved branch against the same grammar the
    # commit guard uses — refuse early with a suggested auto-suffix form
    # (or apply it when --auto-suffix is set) [REF-19]. Runs *before* the
    # draft-baseline commit so a grammar refusal never leaves a half-start
    # docs commit on main.
    if not _branch_matches_grammar(branch):
        suggested_ref = _auto_suffixed_task_ref(task_ref)
        suggested_branch = resolver.format_branch_name(
            suggested_ref, slug=args.slug, plan_id=plan_id
        )
        if args.auto_suffix and suggested_branch and _branch_matches_grammar(
            suggested_branch
        ):
            task_ref = suggested_ref
            branch = suggested_branch
            events_prefix.append("branch_auto_suffixed")
        else:
            return _emit_error(
                "branch_grammar_invalid",
                task_ref=task_ref,
                branch=branch,
                events=["branch_grammar_checked"],
                plan_path=plan_path_for_gate,
                recovery_kind="auto_suffix_task_ref",
                safe_next_commands=[
                    {
                        "command": (
                            f"make task-start TASK={suggested_ref}"
                            + (f" PLAN={args.plan}" if args.plan else "")
                            + ' OBJECTIVE="..."'
                        ),
                        "reason": "branch_grammar_invalid",
                    },
                    {
                        "command": (
                            f"make task-start TASK={task_ref}"
                            + " LIFECYCLE_ARGS=--auto-suffix"
                            + (f" PLAN={args.plan}" if args.plan else "")
                            + ' OBJECTIVE="..."'
                        ),
                        "reason": "branch_grammar_invalid_auto_suffix",
                    },
                ],
            )

    if plan_path_for_gate is not None:
        baseline = evaluate_plan_baseline(
            repo,
            task_ref=task_ref,
            task_plan_path=plan_path_for_gate,
            target_branch=_current_branch(repo) or "main",
        )
        baseline_receipt = baseline.to_dict()
        if plan_glob_path is not None:
            baseline_receipt["plan_path_source"] = "cli_plan_arg"
            if not (
                _common.snapshot_is_live(active_state)
                and active_state.get("task_ref") == task_ref
            ):
                baseline_receipt["identity_state"] = "task_row_missing"
        if baseline.baseline_status == "unknown":
            return _emit_error(
                baseline.reason or "plan_baseline_unknown",
                task_ref=task_ref,
                branch=branch,
                events=list(events_prefix) + ["plan_baseline_checked"],
                plan_path=plan_path_for_gate,
                plan_baseline=baseline_receipt,
            )
        if baseline.baseline_status != "accepted":
            # T21: net-new untracked draft — commit as draft baseline at
            # start instead of plan_baseline_missing deadlock.
            if (
                baseline.plan_untracked_on_main
                and baseline.detail_reason == "untracked_draft_on_main"
            ):
                draft_ok, draft_err = _record_draft_plan_baseline(
                    repo,
                    task_ref=task_ref,
                    plan_path=plan_path_for_gate,
                )
                if draft_ok:
                    draft_baseline_recorded = True
                    events_prefix.append("draft_baseline_recorded")
                    events_prefix.append("plan_baseline_checked")
                    baseline_receipt["baseline_status"] = "accepted"
                    baseline_receipt["reason"] = "draft_baseline_recorded"
                    baseline_receipt["baseline_exists_on_main"] = True
                    baseline_receipt["plan_untracked_on_main"] = False
                    baseline_receipt["detail_reason"] = "draft_baseline_recorded"
                    plan_baseline_receipt = baseline_receipt
                else:
                    baseline_receipt["reason"] = "plan_baseline_missing"
                    baseline_receipt["draft_baseline_error"] = draft_err
                    safe_next_commands = baseline_receipt.get("safe_next_commands") or []
                    if safe_next_commands:
                        baseline_receipt["next_command"] = safe_next_commands[0][
                            "command"
                        ]
                    return _emit_error(
                        "plan_baseline_missing",
                        task_ref=task_ref,
                        branch=branch,
                        events=list(events_prefix)
                        + ["plan_baseline_checked", "draft_baseline_failed"],
                        plan_path=plan_path_for_gate,
                        plan_baseline=baseline_receipt,
                    )
            elif baseline.acceptance_ready or baseline.plan_untracked_on_main:
                baseline_receipt["reason"] = "plan_baseline_missing"
                safe_next_commands = baseline_receipt.get("safe_next_commands") or []
                if safe_next_commands:
                    baseline_receipt["next_command"] = safe_next_commands[0]["command"]
                elif baseline.acceptance_ready:
                    baseline_receipt["next_command"] = build_acceptance_next_command(
                        task_ref
                    )
                error_reason = "plan_baseline_missing"
                return _emit_error(
                    error_reason,
                    task_ref=task_ref,
                    branch=branch,
                    events=list(events_prefix) + ["plan_baseline_checked"],
                    plan_path=plan_path_for_gate,
                    plan_baseline=baseline_receipt,
                )
            else:
                error_reason = baseline.reason or "plan_baseline_not_ready"
                return _emit_error(
                    error_reason,
                    task_ref=task_ref,
                    branch=branch,
                    events=list(events_prefix) + ["plan_baseline_checked"],
                    plan_path=plan_path_for_gate,
                    plan_baseline=baseline_receipt,
                )
        else:
            plan_baseline_receipt = baseline_receipt
            events_prefix.append("plan_baseline_checked")

    # internal: ``uv`` preflight runs before any state mutation
    # so an absent ``uv`` aborts cleanly without a half-created worktree.
    preflight = uv_provisioning.uv_preflight()
    if not preflight.ok:
        return _emit_error(
            f"uv_preflight_failed: {preflight.error}",
            task_ref=task_ref,
            branch=branch,
        )

    mode = args.mode
    if mode == "auto":
        # Auto resolves to worktree until a richer policy is needed.
        mode = "worktree"

    # Pre-mutation ambiguity guard (BR-internal + internal
    # rewrite for OQ2). Pinned discriminator (CTP-internal): a task is
    # planning/maintenance iff ``target_branch == "main"``; otherwise
    # it is implementation and subject to the worktree-singleton
    # invariant. ``task-start`` always derives ``feature/<task-ref>``,
    # so the *incoming* request is uniformly an implementation task.
    pre_view = _read_workspace_summary_view(repo)
    refusal_reason: str | None = None
    conflict: _RealConflict | None = None
    if pre_view.shape == "single" and isinstance(pre_view.active, dict):
        active = pre_view.active
        active_task_ref = active.get("task_ref")
        if (
            isinstance(active_task_ref, str)
            and active_task_ref
            and active_task_ref != task_ref
            and _common.snapshot_is_live(active)
            and active.get("target_branch") != "main"
        ):
            # OQ2 case 3: implementation active, different implementation
            # requested → refuse (preserve worktree-singleton). OQ2 case 4
            # (planning/maintenance active, ``target_branch == "main"``)
            # falls through and is allowed: the new feature-branch
            # worktree is a sibling and never displaces the on-main row.
            refusal_reason = (
                f"requested task_ref={task_ref!r} disagrees with active "
                f"handoff snapshot {active_task_ref!r}"
            )
    elif pre_view.shape == "workspace_ambiguous":
        # internal: the pre-internal "uniformly refuse on unlisted" veto
        # is replaced by claim-aware ``_detect_real_conflict`` against
        # the workspace summary plus on-disk git state. Returning None
        # means an explicit fresh task with unclaimed target branch +
        # worktree path is allowed even if the workspace already lists
        # other live siblings (the internal motivating case).
        primary = resolver.canonical_workspace_root(repo) or repo
        conflict = _detect_real_conflict(
            repo,
            primary=primary,
            task_ref=task_ref,
            target_branch=branch,
            mode=mode,
            live_tasks=pre_view.tasks,
            plan_id=plan_id,
        )
        # internal: ``claim_existing_worktree`` is recoverable, not
        # a refusal — it is handled below by the claim recovery surface /
        # MODE=claim binding rather than the generic ambiguity veto.
        if conflict is not None and conflict.kind != "claim_existing_worktree":
            refusal_reason = conflict.message

    if refusal_reason is not None:
        decision_id = f"claude_workflow_ambiguity_resolved_task_start_{task_ref.replace('-', '_').lower()}_{_utc_stamp()}"
        if conflict is not None:
            rationale = (
                f"task-start refused: {refusal_reason}; "
                f"conflict.kind={conflict.kind} "
                f"conflict.category={conflict.category}"
            )
            if conflict.conflicting_task_ref is not None:
                rationale += f" conflicting_task_ref={conflict.conflicting_task_ref!r}"
            if conflict.conflicting_branch is not None:
                rationale += f" conflicting_branch={conflict.conflicting_branch!r}"
            if conflict.conflicting_path is not None:
                rationale += f" conflicting_path={conflict.conflicting_path!r}"
            rationale += "; no git mutation performed"
        else:
            rationale = (
                f"task-start refused: {refusal_reason}; no git mutation performed"
            )
        projection.project_decision(
            repo,
            decision_id=decision_id,
            rationale=rationale,
            session=decision_id,
            task_ref=task_ref,
        )
        return _emit_error(
            "task_ref_ambiguous",
            task_ref=task_ref,
            branch=branch,
            events=["ambiguity_resolved"],
            conflict_kind=conflict.kind if conflict is not None else None,
            conflict_category=conflict.category if conflict is not None else None,
        )

    # internal: claim recovery surface. A claimable (unowned)
    # existing worktree for the requested branch is recoverable, not a
    # refusal — MODE=worktree/here returns a zero-mutation receipt naming
    # the supported MODE=claim follow-up; MODE=claim falls through to the
    # binding dispatch below.
    if (
        conflict is not None
        and conflict.kind == "claim_existing_worktree"
        and mode != "claim"
    ):
        claim_command = f"make task-start TASK={task_ref} MODE=claim"
        if args.slug:
            claim_command += f" SLUG={args.slug}"
        if args.plan:
            claim_command += f" PLAN={args.plan}"
        existing_path = conflict.conflicting_path or ""
        claim_head = (
            resolver.head_sha(Path(existing_path)) or "" if existing_path else ""
        )
        return _emit_error(
            "claimable_worktree_exists",
            task_ref=task_ref,
            branch=branch,
            events=["claim_recovery_offered"],
            handoff_projection="pending",
            conflict_kind=conflict.kind,
            conflict_category=conflict.category,
            recovery_kind="claim_existing_worktree",
            worktree_path=existing_path,
            head=claim_head,
            safe_next_commands=[
                {
                    "command": claim_command,
                    "reason": "claimable_worktree_exists",
                }
            ],
        )

    created_branch = True
    reused_worktree = False
    previous_branch = ""
    if mode == "here":
        previous_branch = _current_branch(repo)
        if not _checkout_branch_here(repo, branch):
            return _emit_error("branch_checkout_failed")
        worktree_path = repo
        head = resolver.head_sha(repo) or ""
    elif mode == "worktree":
        primary = resolver.canonical_workspace_root(repo) or repo
        existing = _find_linked_worktree_for_branch(primary, branch)
        if existing is not None:
            worktree_path = existing
            created_branch = False
            reused_worktree = True
        else:
            worktree_path = _derive_worktree_path(primary, task_ref, plan_id=plan_id)
            if not _create_linked_worktree(primary, worktree_path, branch):
                return _emit_error("worktree_create_failed")
        head = resolver.head_sha(worktree_path) or ""
    elif mode == "claim":
        # internal: bind a pre-existing unowned worktree for the
        # requested branch to the task row through the normal projection
        # path. No branch/worktree creation — the worktree is adopted
        # as-is. Owned-by-other is re-checked here so MODE=claim is safe
        # even outside the workspace_ambiguous shape that pre-filters it.
        primary = resolver.canonical_workspace_root(repo) or repo
        existing = _find_linked_worktree_for_branch(primary, branch)
        if existing is None:
            return _emit_error(
                "claim_no_existing_worktree",
                task_ref=task_ref,
                branch=branch,
            )
        owner = _existing_worktree_owner(
            [t for t in pre_view.tasks if isinstance(t, Mapping)],
            task_ref=task_ref,
            existing_path=existing,
        )
        if owner is not None:
            return _emit_error(
                "task_ref_ambiguous",
                task_ref=task_ref,
                branch=branch,
                events=["ambiguity_resolved"],
                conflict_kind="branch_collision",
                conflict_category="collision",
            )
        worktree_path = existing
        created_branch = False
        reused_worktree = True
        head = resolver.head_sha(worktree_path) or ""
    else:
        return _emit_error(f"mode_not_implemented:{mode}")

    # implementation note D3b: workspace repos provision via a single ``uv sync`` from
    # the worktree root; legacy repos keep per-package sync + root editable
    # installs. Failure aborts and rolls back the linked worktree (when we
    # created it ourselves) so the state row is never written.
    sync_root = worktree_path if worktree_path.is_dir() else repo
    root_venv = uv_provisioning.provision_worktree_env(
        sync_root,
        override=uv_provisioning.sync_packages_override(),
        stream=sys.stderr,
    )
    if not root_venv.ok:
        if mode == "worktree" and created_branch and not reused_worktree:
            primary = resolver.canonical_workspace_root(repo) or repo
            _rollback_linked_worktree(primary, worktree_path, branch)
        elif mode == "here" and created_branch:
            _rollback_branch_here(repo, previous_branch, branch)
        if "workspace uv sync" in root_venv.failure_reason or "per-package" in root_venv.failure_reason:
            reason = "uv_sync_failed"
        else:
            reason = f"root_venv_provisioning_failed: {root_venv.failure_reason}"
        return _emit_error(reason, task_ref=task_ref, branch=branch)

    # implementation note S3: heal the freshly created linked worktree by adopting the
    # bootstrap overlay (best-effort, non-fatal, marker-gated). This is the
    # durable, harness-agnostic self-heal trigger for the supported worktree flow.
    primary_root = resolver.canonical_workspace_root(repo) or repo
    if mode in ("worktree", "claim") and worktree_path != primary_root:
        overlay_adopt = _adopt_overlay(primary_root, worktree_path)
    else:
        overlay_adopt = {"adopted": False, "skipped": "not_worktree"}

    # implementation note: when adopt skipped because the primary self-hosts the overlay
    # (marker, no clone), emit the generated agent surfaces locally so the new
    # worktree carries /review-parallel et al. Runs before the optional consumer
    # bootstrap hook so surfaces exist before any npm-style post-provision step.
    selfhost_surfaces = _maybe_run_selfhost_surfaces(
        mode, overlay_adopt, primary_root, worktree_path
    )

    worktree_bootstrap = _maybe_run_worktree_bootstrap(
        mode, primary_root, worktree_path
    )

    # implementation note D1: when an id was claimed at procurement (``--plan-intent=yes``),
    # commit the ``# Plan <NNNN>`` stub onto the feature branch now so the claim
    # is durable. Runs after provisioning so only the stub is staged. HEAD moves,
    # so re-read it for the receipt + the state-sync commit_sha projection.
    claimed_plan_path: str | None = None
    if claimed_plan_id and mode in ("worktree", "here"):
        claimed_plan_path = _write_and_commit_plan_stub(
            worktree_path, claimed_plan_id, task_ref, args.slug, args.objective
        )
        if claimed_plan_path is not None:
            head = resolver.head_sha(worktree_path) or head

    plan_path = (
        active_state.get("task_plan_path")
        if _common.snapshot_is_live(active_state)
        else None
    )
    if not isinstance(plan_path, str):
        plan_path = None
    # implementation note: an explicit ``--plan`` glob always wins over the live
    # snapshot's task_plan_path so callers can re-anchor a task to a
    # specific plan revision without first mutating handoff state.
    if plan_glob_path is not None:
        plan_path = plan_glob_path
    # implementation note D1: the freshly committed claim stub is this task's plan path.
    if claimed_plan_path is not None:
        plan_path = claimed_plan_path

    # Forward the objective so the first ``set`` for a brand-new task_ref can
    # INSERT the handoff_state row. Without it ``set_handoff_state`` rejects the
    # insert (objective required) yet the CLI still exits 0, so the projection
    # would report ``synced`` while no row ever lands (internal-* silent
    # no-op). ``args.objective`` defaults to "" — still a valid (empty)
    # objective for the insert, which beats leaving the task unrecorded.
    status = projection.project_state_sync(
        repo,
        task_ref=task_ref,
        target_branch=branch,
        target_worktree_path=str(worktree_path),
        task_plan_path=plan_path,
        objective=args.objective,
    )

    is_claim = mode == "claim"
    success_events = list(events_prefix)
    success_events.append(
        "claimed_existing_worktree" if is_claim else "task_started"
    )
    receipt = {
        "ok": True,
        "command": "task-start",
        "task_ref": task_ref,
        "branch": branch,
        "worktree_path": str(worktree_path),
        "head": head,
        "handoff_projection": status,
        "events": success_events,
        "mode": mode,
        "created_branch": created_branch,
        "reused_worktree": reused_worktree,
        "plan_path": plan_path,
        # implementation note D1: additive — the four-digit plan id claimed at procurement
        # via ``--plan-intent=yes`` (``None`` when no id was claimed: a bare
        # task, or a plan resolved from ``--plan``/the live row).
        "claimed_plan_id": claimed_plan_id,
        "plan_baseline": plan_baseline_receipt,
        "draft_baseline_recorded": draft_baseline_recorded,
        "recovery_kind": "claim_existing_worktree" if is_claim else None,
        "conflict_kind": None,
        "conflict_category": None,
        # internal: additive — names the provisioned worktree-root
        # ``.venv`` when one was created (None when no packages required it).
        "root_venv_path": str(root_venv.venv_dir) if root_venv.created else None,
        # implementation note S3: additive — True when the bootstrap overlay was adopted
        # into a freshly created linked worktree (best-effort, marker-gated).
        "overlay_adopted": overlay_adopt["adopted"],
        # implementation note: additive — self-host local surface bootstrap receipt
        # {ran, ok, skipped, exit, steps}; ran only on no_overlay_clone worktrees.
        "selfhost_surfaces": selfhost_surfaces,
        # implementation note S1: additive — post-provision bootstrap hook receipt.
        "worktree_bootstrap": worktree_bootstrap,
    }

    if not args.emit_json:
        sys.stderr.write(
            f"task-start: task_ref={task_ref} branch={branch} mode={mode} head={head[:12]} projection={status}\n"
        )

    _common.emit(receipt)
    return 0
