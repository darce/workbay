"""``close-check`` subcommand (internal).

The local merge gate. Runs every ``review-ready`` check and adds the
close-only ones that prove a branch is safe to land — sub-implementation note.2
covers the mergeability check (``git merge-tree`` against the merge
base). Later sub-slices add the unresolved-blocker and
required-close-decision checks plus the ``handoff-close-check`` alias.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import resolver

from . import _common, review_ready


def _probe_dirty_main_protected_paths(repo: Path) -> list[str]:
    """Return dirty protected paths on the canonical workspace's main checkout.

    internal (finding internal): post-merge was retuned
    to warn-only in implementation note. The merge gate must therefore pick up the
    same dirty-protected-paths-on-main detection so close-check refuses
    to declare ``ready`` while the canonical workspace still carries
    uncommitted protected-path drift on main.

    Reuses the same hook helpers ``check_main_clean.py`` and the doctor
    facet rely on so all three surfaces agree on what counts as dirty.
    Returns an empty list when:

    - there is no canonical workspace (degraded git state),
    - the canonical workspace's branch is not main/master,
    - the helpers or the harness contract are unavailable.

    These degraded paths intentionally fall through to "no dirty paths"
    rather than blocking the gate — the operator gets the explicit
    doctor facet for diagnosis.
    """
    canonical = resolver.canonical_workspace_root(repo) or repo
    canonical_branch = resolver.current_branch(canonical)
    if canonical_branch not in {"main", "master"}:
        return []

    hooks_dir = _common.find_hooks_dir(canonical)
    if hooks_dir is None:
        return []

    sys.path.insert(0, str(hooks_dir))
    try:
        from _branch_isolation_guard import find_dirty_protected_paths  # noqa: PLC0415
        from _harness_protocol import (  # noqa: PLC0415
            HarnessContractMissingError,
            load_branch_isolation_policy,
        )
    except ImportError:
        return []

    try:
        policy = load_branch_isolation_policy(canonical)
    except HarnessContractMissingError:
        return []
    except Exception:
        return []

    result = find_dirty_protected_paths(
        branch=canonical_branch,
        repo_root=str(canonical),
        policy=policy,
        protected_branches={"main", "master"},
    )
    if result is None:
        return []
    _resolved_branch, dirty_paths = result
    return list(dirty_paths)


def _is_mergeable(repo: Path, base: str) -> bool:
    """Return True iff merging ``HEAD`` into ``base`` would not conflict.

    Uses ``git merge-tree --write-tree`` (porcelain v2): non-zero exit
    or a ``conflict`` marker in the output means the merge would
    require manual resolution.
    """
    merge_base = resolver.merge_base(repo, base)
    if merge_base is None:
        # No merge base means unrelated histories — treat as mergeable
        # only when there is no shared ancestor *and* no diff; the
        # review-ready foundation already rejects no-changes branches,
        # so this falls through to "treat as mergeable" for the rare
        # detached case.
        return True
    proc = resolver._run_git(repo, "merge-tree", "--write-tree", base, "HEAD")
    if proc is None:
        return False
    if proc.returncode != 0:
        return False
    # `git merge-tree --write-tree` prints the merged tree sha on the
    # first line, then any conflict markers. The presence of "<<<<"
    # blocks indicates conflicts.
    return "<<<<<<<" not in proc.stdout


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle close-check", add_help=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument("--base", dest="base", default="main")
    parser.add_argument(
        "--task-ref",
        dest="explicit_task_ref",
        default=None,
        help="Explicit task ref when branch-derived identity is ambiguous.",
    )
    parser.add_argument(
        "--allow-no-active-task",
        dest="allow_no_active_task",
        action="store_true",
        default=False,
        help="On protected bases only, do not block when no active row exists.",
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    branch = resolver.current_branch(repo) or ""
    head = resolver.head_sha(repo) or ""
    known_task_refs = _common._live_task_refs(repo)
    derived_task_ref = args.explicit_task_ref or resolver.derive_task_ref(
        branch, known_task_refs=known_task_refs
    )

    reasons = review_ready.evaluate(repo, args.base)
    reasons, findings_open, handoff_projection = review_ready.augment_with_handoff_state(
        repo, derived_task_ref, head, reasons
    )
    warnings = review_ready._orphan_planning_warnings(repo)

    if branch not in review_ready.PROTECTED_BASES:
        missing_row = False
        if derived_task_ref:
            local_row = _common.local_live_handoff_row_exists(repo, derived_task_ref)
            if local_row is False:
                missing_row = True
            elif local_row is None:
                identity, identity_ok = review_ready._query_active_task_identity(
                    repo, derived_task_ref
                )
                if identity_ok and identity is None:
                    missing_row = True
        else:
            missing_row = True
        if missing_row:
            if "no_active_task_row" not in reasons:
                reasons.append("no_active_task_row")
            warnings.append(
                "no_active_task_row: no resolvable active handoff row on a "
                "feature branch — run `make task-start` or pass explicit --task-ref"
            )

    if (
        args.explicit_task_ref is None
        and review_ready.probe_ambiguous_task_ref(
            repo, branch, derived_task_ref, known_task_refs
        )
    ):
        reasons.append("ambiguous_task_ref")
        warnings.append(
            "ambiguous_task_ref: pass explicit --task-ref (or TASK= on "
            "make targets that accept it) so handoff gates are evaluated"
        )

    if (
        branch not in review_ready.PROTECTED_BASES
        and derived_task_ref is None
        and "on_protected_base" not in reasons
    ):
        reasons.append("no_active_task")

    # Mergeability is close-only. Skip the check when review-ready
    # already rejected for being on a protected base — we cannot merge
    # main into itself, and the protected-base reason supersedes any
    # mergeability nuance.
    if "on_protected_base" in reasons:
        mergeable = True
    else:
        mergeable = _is_mergeable(repo, args.base)
        if not mergeable:
            reasons.append("unmergeable")

    # internal: close-only dirty-main probe.
    dirty_main_paths = _probe_dirty_main_protected_paths(repo)
    if dirty_main_paths:
        reasons.append("dirty_main_protected_paths")

    # internal: close-only checklist guardrail. Blocks merge
    # while the active task's plan still has `- [ ]` items whose
    # recorded handoff evidence would flip them. ``None`` means the
    # plan lookup failed (no task_ref, no stored plan, parse error,
    # etc.) — that is logged via the existing warnings surface and
    # never blocks the gate.
    pending_count, plan_path = review_ready._probe_checklist_sync_pending(
        repo, derived_task_ref
    )
    if pending_count is not None and pending_count > 0:
        reasons.append("checklist_sync_pending")
        warnings.append(
            f"checklist_sync_pending: {pending_count} evidence-backed unchecked "
            f"items in {plan_path} — run `make sync-task-plan-checklist "
            f"TASK={derived_task_ref or '<task-ref>'} APPLY=1`"
        )

    review_ready._emit(
        command="close-check",
        repo=repo,
        branch=branch,
        head=head,
        derived_task_ref=derived_task_ref,
        reasons=reasons,
        findings_open=findings_open,
        handoff_projection=handoff_projection,
        warnings=warnings,
        extras={"mergeable": mergeable},
        emit_json=args.emit_json,
    )
    if "ambiguous_task_ref" in reasons:
        return 2
    return 0
