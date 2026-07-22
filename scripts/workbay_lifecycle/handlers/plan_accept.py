"""``plan-accept`` subcommand (internal).

Acceptance gate that lands a planning-reviewed plan on ``main`` as a
docs-only commit. When readiness gates pass and the checkout is root
``main`` with a clean (or plan-only) worktree, the handler performs the
docs-only accept **inline** via :func:`_apply_local_accept` (implementation note
S1, facade-owned write). Otherwise the receipt keeps the operator
copy-paste ``next_command``. ``--local`` remains the strict path that
fails readiness when those preconditions do not hold.

Gates (per Plan internal §Acceptance criteria):

1. The task must have a ``task_plan_path`` registered.
2. The latest ``review_runs`` row for the plan ``subject_path`` with
   ``review_mode="planning"`` must have ``verdict="pass"`` — exactly.
   ``pass_with_findings``, ``conditional_pass``, ``fail``, or absent
   review runs all block.
3. Zero open planning findings for the plan subject (``file_path`` match).
   Findings on other plan subjects under the same task_ref are reported
   as ``open_planning_findings_other_subjects`` (warning, non-blocking).

When all gates pass the receipt's ``ready`` is True. On a successful
inline apply the receipt adds ``applied``, ``applied_commit_sha``, and
``applied_command``. On apply failure the typed ``apply_error`` is set
and ``next_command`` remains the manual fallback. When apply preconditions
do not hold, ``apply_skip_reason`` documents why the write was skipped.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

import resolver

from . import _common
from .plan_baseline import (
    build_acceptance_next_command,
    evaluate_plan_baseline,
    is_planning_path,
    query_open_planning_finding_count,
)


def _query_handoff_identity(repo: Path, task_ref: str) -> tuple[dict[str, Any] | None, str]:
    """Return ``(active_identity, state)`` for ``task_ref``.

    Mirrors :func:`review_ready._query_active_task_identity`: the real
    ``mcp-workbay-handoff`` CLI exposes ``state`` (positional ``task_ref``,
    ``--sections`` flag), not ``get-handoff-state`` — the legacy name
    silently degraded the gate to ``handoff_state_unavailable`` against
    a real CLI (regression guarded by
    ``test_plan_accept_uses_real_cli_state_subcommand``;
    finding internal).
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(workspace),
        "state",
        "--sections", "identity",
        task_ref,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None, "query_failed"
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None, "query_failed"
    data = payload.get("data") if isinstance(payload, dict) else None
    active = data.get("active") if isinstance(data, dict) else None
    if active is None:
        return None, "missing"
    if not isinstance(active, dict):
        return None, "malformed"
    return active, "active"


def _build_accept_command(*, task_ref: str, branch: str, plan_path: str) -> str:
    """Return the docs-only commit recipe operators can copy-paste."""
    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    if branch == "main":
        return (
            f"git switch main && "
            f"git add {shlex.quote(plan_path)} && "
            f"git commit -m {shlex.quote(msg)}"
        )
    return (
        f"git switch main && "
        f"git checkout {shlex.quote(branch)} -- {shlex.quote(plan_path)} && "
        f"git add {shlex.quote(plan_path)} && "
        f"git commit -m {shlex.quote(msg)}"
    )


def _build_explicit_recovery_command(task_ref: str) -> str:
    return (
        f'make plan-accept TASK={task_ref} '
        'LIFECYCLE_ARGS="--json --plan <task-plan-path> --source-branch <planning-branch>"'
    )


def _build_already_accepted_next_command(
    task_ref: str, plan_path: str | None = None
) -> str:
    """Forward command for a baseline that is already on ``main``.

    The plan is accepted, so the next lifecycle step is starting the task
    branch, not re-running acceptance. When the plan path is known, thread it
    through as ``PLAN=<plan_path>`` so ``task-start`` resolves the implementing
    plan id and appends the ``-plan<NNNN>`` branch/worktree segment plus the
    ``task_plan_path`` link. Without ``PLAN=`` on this order (accept-then-start,
    no live row yet) ``task-start`` has no way to discover the plan and the
    segment silently drops (implementation note).
    """
    plan = plan_path.strip() if isinstance(plan_path, str) else ""
    if plan:
        return (
            f"make task-start TASK={task_ref} "
            f'PLAN={shlex.quote(plan)} OBJECTIVE="..."'
        )
    return f'make task-start TASK={task_ref} OBJECTIVE="..."'


def _identity_recovery_kind(identity_state: str) -> str:
    if identity_state == "missing":
        return "handoff_identity_missing"
    if identity_state == "malformed":
        return "handoff_identity_malformed"
    return "handoff_identity_query_failed"


def _identity_state_label(identity_state: str) -> str:
    if identity_state == "missing":
        return "task_row_missing"
    if identity_state == "malformed":
        return "task_identity_malformed"
    if identity_state == "query_failed":
        return "identity_query_failed"
    return "active_task_identity"


def _identity_recovery_explanation(identity_state: str, task_ref: str) -> str:
    if identity_state == "missing":
        return (
            f"No active handoff identity was found for {task_ref}. "
            "Rerun plan-accept with explicit --plan and --source-branch values from the planning source branch."
        )
    if identity_state == "malformed":
        return (
            f"The active handoff identity for {task_ref} is malformed. "
            "Rerun plan-accept with explicit --plan and --source-branch values or repair the task row first."
        )
    return (
        f"The handoff identity query for {task_ref} failed. "
        "Retry the command or rerun with explicit --plan and --source-branch values if the planning source is known."
    )


def _is_worktree_clean(repo: Path) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain"],
    )
    return proc.returncode == 0 and not proc.stdout.strip()


def _dirty_paths(repo: Path) -> list[str] | None:
    # -z yields NUL-terminated records with verbatim (unquoted, un-escaped)
    # paths, so paths containing spaces or non-ASCII bytes compare correctly
    # against plan_path. Each record is "XY <path>"; rename/copy entries append
    # a second NUL-terminated field for the original path, which we skip.
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain=v1", "-z", "-uall"],
    )
    if proc.returncode != 0:
        return None
    paths: list[str] = []
    fields = proc.stdout.split("\0")
    index = 0
    while index < len(fields):
        record = fields[index]
        index += 1
        if len(record) < 4:
            continue
        status, path = record[:2], record[3:]
        paths.append(path)
        if "R" in status or "C" in status:
            # Consume the trailing original-path field for rename/copy records.
            index += 1
    return paths


def _is_worktree_clean_or_only_plan(repo: Path, plan_path: str) -> bool:
    """Return True when the worktree has no dirt that blocks plan-accept.

    The plan file itself is always allowed (tracked modification or untracked).
    Other untracked paths (``??``) are ignored so a batch of sibling draft
    plans can be accepted one-by-one. Any other porcelain status (modified,
    staged, deleted, renamed, etc.) on a non-plan path refuses.
    """
    # -z yields NUL-terminated records with verbatim paths; rename/copy
    # entries append a second field for the original path, which we skip.
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain=v1", "-z", "-uall"],
    )
    if proc.returncode != 0:
        return False
    # RG-0146-05: porcelain paths are repo-root-relative; normalize the
    # caller-supplied plan_path the same way (strip, drop a leading ./) so a
    # './docs/plans/p.md' identity form cannot false-refuse its own plan.
    plan_norm = plan_path.strip()
    plan_norm = plan_norm[2:] if plan_norm.startswith("./") else plan_norm
    fields = proc.stdout.split("\0")
    index = 0
    while index < len(fields):
        record = fields[index]
        index += 1
        if len(record) < 4:
            continue
        status, path = record[:2], record[3:]
        if "R" in status or "C" in status:
            # Consume the trailing original-path field for rename/copy records.
            index += 1
        path_norm = path.strip()
        path_norm = path_norm[2:] if path_norm.startswith("./") else path_norm
        if path_norm == plan_norm:
            continue
        if status == "??":
            # Untracked sibling drafts (and other untracked noise) are non-blocking.
            continue
        return False
    return True


def _current_branch(repo: Path) -> str:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _head_sha(repo: Path) -> str:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _plan_exists_on_branch(repo: Path, branch: str, plan_path: str) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "cat-file", "-e", f"{branch}:{plan_path}"],
    )
    return proc.returncode == 0


def _restore_path_from_head(repo: Path, plan_path: str) -> None:
    restore = _common.run_subprocess(
        ["git", "-C", str(repo), "restore", "--source=HEAD", "--staged", "--worktree", "--", plan_path],
    )
    if restore.returncode == 0:
        return
    _common.run_subprocess(["git", "-C", str(repo), "reset", "--", plan_path])
    if _plan_exists_on_branch(repo, "HEAD", plan_path):
        _common.run_subprocess(["git", "-C", str(repo), "checkout", "--", plan_path])
        return
    try:
        (repo / plan_path).unlink()
    except FileNotFoundError:
        pass


def _validate_apply_plan_path(plan_path: str) -> str | None:
    """Return a short rejection token if ``plan_path`` is unsafe for apply.

    Docs planning namespace only (``is_planning_path``), repo-relative, no
    ``..`` segments, and no leading dash (argv injection). Returns ``None``
    when the path is acceptable.
    """
    path = plan_path.strip() if isinstance(plan_path, str) else ""
    if not path:
        return "empty"
    if path.startswith("-"):
        return "dash_prefix"
    if Path(path).is_absolute() or path.startswith("/"):
        return "absolute"
    segments = [seg for seg in path.replace("\\", "/").split("/") if seg not in ("", ".")]
    if any(seg == ".." for seg in segments):
        return "path_traversal"
    # Rebuild without '.' segments for allowlist check; keep original relative form.
    normalized = "/".join(segments)
    if not is_planning_path(normalized) and not is_planning_path(path):
        return "not_planning_namespace"
    return None


def _apply_local_accept(
    repo: Path,
    *,
    task_ref: str,
    branch: str,
    plan_path: str,
) -> tuple[bool, str | None]:
    """Run the docs-only checkout+commit inline. Returns ``(ok, error)``."""
    if isinstance(branch, str) and branch.startswith("-"):
        return False, "branch_name_rejected: dash_prefix"
    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    if branch != "main":
        checkout = _common.run_subprocess(
            ["git", "-C", str(repo), "checkout", branch, "--", plan_path],
        )
        if checkout.returncode != 0:
            _restore_path_from_head(repo, plan_path)
            return False, f"checkout_failed: {checkout.stderr.strip() or checkout.stdout.strip()}"
    add = _common.run_subprocess(
        ["git", "-C", str(repo), "add", "--", plan_path],
    )
    if add.returncode != 0:
        _restore_path_from_head(repo, plan_path)
        return False, f"add_failed: {add.stderr.strip() or add.stdout.strip()}"
    # Path-scope cleanliness immediately before commit: abort+restore if the
    # tree gained unrelated staged/dirty paths since the outer precheck.
    if not _is_worktree_clean_or_only_plan(repo, plan_path):
        _restore_path_from_head(repo, plan_path)
        return False, "unrelated_staged_changes"
    commit = _common.run_subprocess(
        ["git", "-C", str(repo), "commit", "-m", msg, "--", plan_path],
    )
    if commit.returncode != 0:
        _restore_path_from_head(repo, plan_path)
        return False, f"commit_failed: {commit.stderr.strip() or commit.stdout.strip()}"
    return True, None


def _relink_plan_suffix(
    repo: Path,
    *,
    task_ref: str,
    target_branch: str,
    target_worktree_path: str | None,
    plan_id: str,
    revision: int | None,
) -> dict[str, Any]:
    """implementation note D2: rename branch + worktree + handoff row to carry the
    ``-plan<NNNN>`` suffix once ``plan-accept`` has made the id canonical.

    Guard-safe order (implementation note D8): ``git branch -m`` + ``git worktree move``
    run FIRST so the *incoming* branch/worktree already exists, THEN the
    ``set_handoff_state`` repoint — which derives its write-context worktree from
    the incoming ``target_branch`` (``shared_write_context`` resolves the new
    branch, not the stale stored one). Calling ``set`` before the move would
    reproduce the catch-22 (``WorktreeNotFoundError`` on the incoming branch).

    No-op when the suffix is already present. Pre-flight refuses a dirty or
    missing worktree and a pre-existing target path; a failed ``worktree move``
    rolls the branch rename back. Returns a structured result; ``relinked`` is
    True only when both git moves landed.
    """
    suffix = f"-plan{plan_id}"
    if target_branch.endswith(suffix):
        return {"relinked": False, "skipped": "suffix_present"}
    if not target_worktree_path:
        return {"relinked": False, "skipped": "no_worktree_path"}
    old_wt = Path(target_worktree_path)
    if not old_wt.is_dir():
        return {"relinked": False, "skipped": "worktree_missing"}
    new_branch = f"{target_branch}{suffix}"
    new_wt = old_wt.parent / f"{old_wt.name}{suffix}"
    if new_wt.exists():
        return {"relinked": False, "skipped": "target_path_exists"}
    status = _common.run_subprocess(
        ["git", "-C", str(old_wt), "status", "--porcelain"]
    )
    if status.returncode != 0 or status.stdout.strip():
        return {"relinked": False, "skipped": "worktree_dirty"}

    rename = _common.run_subprocess(
        ["git", "-C", str(repo), "branch", "-m", target_branch, new_branch]
    )
    if rename.returncode != 0:
        return {
            "relinked": False,
            "error": f"branch_rename_failed: {rename.stderr.strip()}",
        }
    move = _common.run_subprocess(
        ["git", "-C", str(repo), "worktree", "move", str(old_wt), str(new_wt)]
    )
    if move.returncode != 0:
        # Roll the branch rename back so the task is not stranded half-renamed.
        _common.run_subprocess(
            ["git", "-C", str(repo), "branch", "-m", new_branch, target_branch]
        )
        return {
            "relinked": False,
            "error": f"worktree_move_failed: {move.stderr.strip()}",
        }

    # Incoming-branch guard path (implementation note D8): the new worktree now exists, so
    # the repoint derives write-context from it and succeeds.
    set_argv = list(
        _common.handoff_command_argv(
            repo,
            "set",
            "--task-ref",
            task_ref,
            "--target-branch",
            new_branch,
            "--target-worktree-path",
            str(new_wt),
        )
    )
    if revision is not None:
        set_argv += ["--expected-revision", str(revision)]
    repoint = _common.run_subprocess(set_argv)
    return {
        "relinked": True,
        "relinked_from": {"branch": target_branch, "worktree_path": str(old_wt)},
        "relinked_to": {"branch": new_branch, "worktree_path": str(new_wt)},
        "row_repointed": repoint.returncode == 0,
    }


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle plan-accept", add_help=True)
    parser.add_argument("--task", dest="task_ref", required=True)
    parser.add_argument("--plan", dest="plan_path", default="")
    parser.add_argument("--source-branch", dest="source_branch", default="")
    parser.add_argument(
        "--review-task-ref",
        dest="review_task_ref",
        default="",
        help=(
            "Read planning verdict and open-finding evidence from this "
            "MAINT-row review task ref instead of the implementation task. "
            "Requires the row to have an exact-subject, passing planning "
            "review with zero open planning findings."
        ),
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument(
        "--local",
        dest="local",
        action="store_true",
        default=False,
        help=(
            "Run the docs-only checkout+commit inline (requires canonical "
            "root checkout on main with a clean tree)."
        ),
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    canonical = resolver.canonical_workspace_root(repo) or repo
    task_ref = args.task_ref

    reasons: list[str] = []
    extras: dict[str, Any] = {"task_ref": task_ref}
    explicit_plan_path = (args.plan_path or "").strip()
    explicit_source_branch = (args.source_branch or "").strip()
    explicit_review_task_ref = (args.review_task_ref or "").strip()
    self_invocation = _reconstruct_invocation(
        task_ref,
        plan_path=explicit_plan_path,
        source_branch=explicit_source_branch,
        review_task_ref=explicit_review_task_ref,
        local=bool(args.local),
    )

    identity, identity_state = _query_handoff_identity(repo, task_ref)
    if identity is None and not (explicit_plan_path and explicit_source_branch):
        recovery_command = _build_explicit_recovery_command(task_ref)
        recovery_kind = _identity_recovery_kind(identity_state)
        reasons.append("handoff_state_unavailable")
        extras.update(
            {
                "recovery_kind": recovery_kind,
                "recovery_explanation": _identity_recovery_explanation(identity_state, task_ref),
                "safe_next_commands": [
                    {
                        "command": recovery_command,
                        "reason": "explicit_plan_source_required",
                    }
                ],
                "next_command": recovery_command,
                "plan_path_source": "unknown",
                "identity_state": _identity_state_label(identity_state),
                "candidate_review_task_refs": [],
            }
        )
        recovery_command = _filter_self_echoing_safe_next(
            reasons=reasons,
            extras=extras,
            next_command=recovery_command,
            self_invocation=self_invocation,
            task_ref=task_ref,
            plan_path=explicit_plan_path,
            source_branch=explicit_source_branch,
        )
        extras["next_command"] = recovery_command
        return _emit(
            command="plan-accept",
            repo=repo,
            task_ref=task_ref,
            ready=False,
            reasons=reasons,
            next_command=recovery_command,
            extras=extras,
            emit_json=args.emit_json,
        )

    if explicit_plan_path and explicit_source_branch:
        plan_path = explicit_plan_path
        target_branch = explicit_source_branch
        extras["plan_path_source"] = "cli_plan_arg"
        extras["identity_state"] = _identity_state_label(identity_state)
    else:
        plan_path = identity.get("task_plan_path") if identity is not None else None
        target_branch = identity.get("target_branch") if identity is not None else None
    extras["target_branch"] = target_branch
    extras["task_plan_path"] = plan_path
    baseline = evaluate_plan_baseline(
        repo,
        task_ref=task_ref,
        task_plan_path=str(plan_path) if isinstance(plan_path, str) else None,
        target_branch=str(target_branch) if isinstance(target_branch, str) else None,
        review_task_ref=explicit_review_task_ref or None,
    )

    # Auto-adopt a single pass-verdict MAINT candidate when the operator did
    # not pass --review-task-ref. Candidates are known only after the first
    # baseline pass; re-evaluate with the adopted ref so evidence (verdict +
    # open findings) is bound exactly as if the flag had been explicit.
    adopted_review_task_ref: str | None = None
    if not explicit_review_task_ref and not (
        baseline.baseline_status == "accepted" and baseline.reason == "already_accepted"
    ):
        pass_candidates = _pass_candidate_refs(list(baseline.candidate_review_task_refs or []))
        if len(pass_candidates) == 1 and not baseline.acceptance_ready:
            adopted_review_task_ref = str(pass_candidates[0]["task_ref"]).strip()
            baseline = evaluate_plan_baseline(
                repo,
                task_ref=task_ref,
                task_plan_path=str(plan_path) if isinstance(plan_path, str) else None,
                target_branch=str(target_branch) if isinstance(target_branch, str) else None,
                review_task_ref=adopted_review_task_ref,
            )

    effective_review_task_ref = explicit_review_task_ref or adopted_review_task_ref or ""

    baseline_next_command = baseline.next_command
    baseline_fields = baseline.to_dict()
    baseline_fields.pop("next_command", None)
    if explicit_plan_path and explicit_source_branch:
        baseline_fields["plan_path_source"] = "cli_plan_arg"
        baseline_fields["identity_state"] = _identity_state_label(identity_state)
    extras.update(baseline_fields)
    if adopted_review_task_ref:
        extras["review_task_ref_adopted"] = adopted_review_task_ref
    noop_command: str | None = None
    already_accepted = (
        baseline.baseline_status == "accepted" and baseline.reason == "already_accepted"
    )
    if already_accepted:
        # internal: an accepted baseline is a satisfied lifecycle
        # state, not a failure. Exit zero and point at the next command.
        noop_command = _build_already_accepted_next_command(
            task_ref, plan_path if isinstance(plan_path, str) else None
        )
        extras["recovery_kind"] = "already_accepted"
        extras["safe_next_commands"] = [
            {"command": noop_command, "reason": "already_accepted"}
        ]
    elif not baseline.acceptance_ready and baseline.reason:
        reasons.append(baseline.reason)

    if (
        baseline.candidate_review_task_refs
        and not explicit_review_task_ref
        and not adopted_review_task_ref
    ):
        extras["recovery_kind"] = baseline.detail_reason or "wrong_ref_review_run"
        # Multi-candidate ambiguity: baseline leaves next_command empty. Surface
        # per-candidate --review-task-ref recoveries so the operator can choose.
        pass_candidates = _pass_candidate_refs(list(baseline.candidate_review_task_refs or []))
        if len(pass_candidates) > 1 and not baseline_next_command:
            recovery_entries: list[dict[str, str]] = []
            plan_for_cmd = str(plan_path) if isinstance(plan_path, str) else ""
            branch_for_cmd = str(target_branch) if isinstance(target_branch, str) else ""
            for candidate in pass_candidates:
                cmd = build_acceptance_next_command(
                    task_ref,
                    plan_path=plan_for_cmd or None,
                    source_branch=branch_for_cmd or None,
                    review_task_ref=str(candidate["task_ref"]),
                )
                recovery_entries.append(
                    {
                        "command": cmd,
                        "reason": "wrong_ref_review_run_recoverable",
                    }
                )
            if recovery_entries:
                extras["safe_next_commands"] = recovery_entries
                baseline_next_command = recovery_entries[0]["command"]

    plan_path_str = str(plan_path) if isinstance(plan_path, str) else ""
    plan_path_reject = (
        _validate_apply_plan_path(plan_path_str) if plan_path_str else "empty"
    )
    # Rejected paths must never be handed to a human as next_command either.
    suppress_next_command = False
    accept_command = (
        _build_accept_command(
            task_ref=task_ref,
            branch=str(target_branch),
            plan_path=plan_path_str,
        )
        if baseline.acceptance_ready and plan_path_reject is None
        else None
    )

    if baseline.acceptance_ready and not reasons and not already_accepted:
        # implementation note S1: facade-owned inline apply when ready. ``--local`` is the
        # strict path (precondition failures become readiness reasons);
        # without ``--local`` the same preconditions soft-skip with
        # ``apply_skip_reason`` and keep the operator ``next_command`` fallback.
        extras["applied_command"] = accept_command
        can_apply = True
        if plan_path_reject is not None:
            can_apply = False
            suppress_next_command = True
            reject_msg = f"plan_path_rejected: {plan_path_reject}"
            extras["applied"] = False
            extras["apply_error"] = reject_msg
            extras["apply_skip_reason"] = reject_msg
            if args.local:
                reasons.append(reject_msg)
        elif repo.resolve() != canonical.resolve():
            can_apply = False
            if args.local:
                reasons.append("local_requires_canonical_root")
            else:
                extras["applied"] = False
                extras["apply_skip_reason"] = "not_canonical_root"
        elif _current_branch(repo) != "main":
            can_apply = False
            if args.local:
                reasons.append("local_requires_main_checkout")
            else:
                extras["applied"] = False
                extras["apply_skip_reason"] = "not_on_main"
        elif not _is_worktree_clean_or_only_plan(repo, plan_path_str):
            can_apply = False
            if args.local:
                reasons.append("local_requires_clean_tree")
            else:
                extras["applied"] = False
                extras["apply_skip_reason"] = "worktree_dirty"
        if can_apply and not reasons:
            # r0153-5: re-query open subject findings at the apply boundary so a
            # finding recorded between readiness and write cannot land silently.
            evidence_task_ref = effective_review_task_ref or task_ref
            findings_result = query_open_planning_finding_count(
                repo,
                task_ref=evidence_task_ref,
                plan_path=plan_path_str,
            )
            subject_count, _other_count, findings_ok = findings_result  # type: ignore[misc]
            if findings_ok and subject_count > 0:
                extras["applied"] = False
                extras["apply_error"] = "readiness_stale_open_findings"
                extras["open_planning_findings"] = subject_count
                suppress_next_command = True
                accept_command = None
                extras["applied_command"] = None
                # Bind receipt to readiness evidence already at hand (no extra MCP).
                if baseline.latest_planning_verdict is not None:
                    extras["readiness_bound_verdict"] = baseline.latest_planning_verdict
                if baseline.review_task_ref is not None:
                    extras["readiness_bound_review_task_ref"] = baseline.review_task_ref
                if args.local:
                    reasons.append("readiness_stale_open_findings")
            else:
                ok, err = _apply_local_accept(
                    repo,
                    task_ref=task_ref,
                    branch=str(target_branch),
                    plan_path=plan_path_str,
                )
                if not ok:
                    extras["applied"] = False
                    extras["apply_error"] = err or "local_accept_failed"
                    if args.local:
                        # Strict --local: surface apply failure as a readiness block.
                        reasons.append(err or "local_accept_failed")
                    # Soft path keeps ready=True and next_command as the manual fallback.
                else:
                    extras["applied"] = True
                    extras["accepted"] = True
                    sha = _head_sha(repo)
                    if sha:
                        extras["applied_commit_sha"] = sha

    # implementation note D2: once the plan id is canonical (accepted on ``main`` — either
    # already, or just applied via ``--local`` / default-on ready apply), reconcile
    # a task whose branch/worktree still lack the ``-plan<NNNN>`` suffix in this
    # one invocation. No-op when the suffix is already correct.
    if (
        (already_accepted or extras.get("accepted"))
        and isinstance(target_branch, str)
        and target_branch
        and isinstance(plan_path, str)
    ):
        plan_id = resolver.extract_plan_id(plan_path)
        if plan_id:
            worktree_path = identity.get("target_worktree_path") if identity else None
            revision = identity.get("revision") if identity else None
            relink = _relink_plan_suffix(
                repo,
                task_ref=task_ref,
                target_branch=target_branch,
                target_worktree_path=(
                    worktree_path if isinstance(worktree_path, str) else None
                ),
                plan_id=plan_id,
                revision=revision if isinstance(revision, int) else None,
            )
            if relink.get("relinked"):
                extras["relinked"] = True
                extras["relinked_from"] = relink["relinked_from"]
                extras["relinked_to"] = relink["relinked_to"]
                extras["row_repointed"] = relink.get("row_repointed", False)
            elif relink.get("error"):
                extras["relink_error"] = relink["error"]

    if suppress_next_command:
        next_command: str | None = None
    else:
        next_command = noop_command or accept_command or baseline_next_command
    next_command = _filter_self_echoing_safe_next(
        reasons=reasons,
        extras=extras,
        next_command=next_command,
        self_invocation=self_invocation,
        task_ref=task_ref,
        plan_path=plan_path_str or explicit_plan_path,
        source_branch=(
            str(target_branch)
            if isinstance(target_branch, str) and target_branch
            else explicit_source_branch
        ),
    )
    return _emit(
        command="plan-accept",
        repo=repo,
        task_ref=task_ref,
        ready=not reasons,
        reasons=reasons,
        next_command=next_command,
        extras=extras,
        emit_json=args.emit_json,
    )


def _reconstruct_invocation(
    task_ref: str,
    *,
    plan_path: str = "",
    source_branch: str = "",
    review_task_ref: str = "",
    local: bool = False,
) -> str:
    """Rebuild the make-form invocation for this run from the parsed CLI args."""
    return build_acceptance_next_command(
        task_ref,
        plan_path=plan_path or None,
        source_branch=source_branch or None,
        local=local,
        review_task_ref=review_task_ref or None,
    )


def _pass_candidate_refs(
    candidates: list[dict[str, object | None]] | list[Any],
) -> list[dict[str, object | None]]:
    """Return candidate rows whose recorded verdict is exactly ``pass``."""
    out: list[dict[str, object | None]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if candidate.get("verdict") != "pass":
            continue
        ref = candidate.get("task_ref")
        if not isinstance(ref, str) or not ref.strip():
            continue
        out.append(candidate)
    return out


def _filter_self_echoing_safe_next(
    *,
    reasons: list[str],
    extras: dict[str, Any],
    next_command: str | None,
    self_invocation: str,
    task_ref: str,
    plan_path: str,
    source_branch: str,
) -> str | None:
    """On refusal, drop safe_next entries that merely restate this run's argv.

    A gate must never re-emit its own failing command as the recovery. When
    filtering empties the list, substitute a genuinely different remediation.
    """
    if not reasons:
        return next_command

    raw = extras.get("safe_next_commands") or []
    filtered: list[dict[str, str]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        command = entry.get("command")
        if not isinstance(command, str) or not command:
            continue
        if command == self_invocation:
            continue
        reason = entry.get("reason")
        filtered.append(
            {
                "command": command,
                "reason": reason if isinstance(reason, str) else "recovery",
            }
        )

    # Strip self-echo from next_command only when it literally restates this run.
    # Do not invent a next_command when baseline intentionally left it None
    # (e.g. non-pass verdicts); only promote a filtered recovery when we
    # ourselves removed a self-echoing next_command.
    stripped_self_next = bool(next_command) and next_command == self_invocation
    out_next = None if stripped_self_next else next_command

    if not filtered:
        if "local_requires_clean_tree" in reasons or any(
            r.startswith("unrelated_staged_changes") for r in reasons
        ):
            filtered = [
                {
                    "command": "git status --porcelain",
                    "reason": "inspect_worktree_dirt",
                }
            ]
        else:
            candidates = extras.get("candidate_review_task_refs") or []
            pass_candidates = _pass_candidate_refs(
                candidates if isinstance(candidates, list) else []
            )
            if pass_candidates:
                for candidate in pass_candidates:
                    ref = str(candidate["task_ref"])
                    cmd = build_acceptance_next_command(
                        task_ref,
                        plan_path=plan_path or None,
                        source_branch=source_branch or None,
                        review_task_ref=ref,
                    )
                    if cmd == self_invocation:
                        continue
                    filtered.append(
                        {
                            "command": cmd,
                            "reason": "wrong_ref_review_run_recoverable",
                        }
                    )
            if not filtered and out_next and out_next != self_invocation:
                filtered = [
                    {
                        "command": out_next,
                        "reason": str(extras.get("recovery_kind") or "recovery"),
                    }
                ]
            if not filtered:
                # Prefer a non-echoing inspection step over re-stating failure.
                # Only when we have nothing better — and do not force it into
                # next_command unless we stripped a self-echo above.
                filtered = [
                    {
                        "command": "git status --porcelain",
                        "reason": "inspect_before_retry",
                    }
                ]

    if stripped_self_next and out_next is None and filtered:
        out_next = filtered[0]["command"]
    extras["safe_next_commands"] = filtered
    return out_next


def _typed_make_vars_hint(task_ref: str) -> str:
    return (
        "  typed make vars: "
        f"TASK={task_ref} [REVIEW_TASK_REF=<ref>] [LOCAL=1] [PLAN=<path>] "
        "[SOURCE_BRANCH=<branch>]"
    )


def _make_n_probe_hint(task_ref: str) -> str:
    return (
        f"  probe: make -n plan-accept TASK={task_ref} "
        "(run before concluding the target is missing)"
    )


def _emit(
    *,
    command: str,
    repo: Path,
    task_ref: str,
    ready: bool,
    reasons: list[str],
    next_command: str | None,
    extras: dict[str, Any],
    emit_json: bool,
) -> int:
    receipt: dict[str, Any] = {
        "ok": ready,
        "command": command,
        "task_ref": task_ref,
        "worktree_path": str(repo),
        "ready": ready,
        "reasons": reasons,
        "events": ["plan_accept_evaluated"],
        "next_command": next_command,
    }
    receipt.update(extras)
    if not emit_json:
        if ready:
            sys.stderr.write("plan-accept: READY\n")
            if next_command:
                sys.stderr.write(f"  run: {next_command}\n")
        else:
            sys.stderr.write("plan-accept: NOT READY: " + ", ".join(reasons) + "\n")
            sys.stderr.write(_typed_make_vars_hint(task_ref) + "\n")
            sys.stderr.write(_make_n_probe_hint(task_ref) + "\n")
            # implementation note B2/B4: surface the recovery the --json receipt already
            # carries so the operator-facing (non-JSON) path names the matching
            # review task_ref and the exact re-run command, instead of leaving
            # the recovery legible only to a JSON consumer.
            candidates = extras.get("candidate_review_task_refs") or []
            refs = [
                c.get("task_ref", "") if isinstance(c, dict) else str(c)
                for c in candidates
            ]
            refs = [r for r in refs if r]
            if refs:
                sys.stderr.write(
                    "  passing planning review found under: "
                    + ", ".join(refs)
                    + "\n"
                )
            if next_command:
                sys.stderr.write(f"  recovery: {next_command}\n")
            recovery_kind = extras.get("recovery_kind")
            if isinstance(recovery_kind, str) and recovery_kind:
                sys.stderr.write(f"  recovery_kind: {recovery_kind}\n")
            detail_reason = extras.get("detail_reason")
            if isinstance(detail_reason, str) and detail_reason:
                sys.stderr.write(f"  detail_reason: {detail_reason}\n")
    _common.emit(receipt)
    return 0 if ready else 2
