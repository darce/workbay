"""``plan-accept`` subcommand (internal).

Acceptance gate that lands a planning-reviewed plan on ``main`` as a
docs-only commit. The handler is intentionally additive: it never
mutates anything by default. PR mode (the default) verifies the gates
and prints the exact git commands the operator should run; ``--local``
performs the commit in-process from a clean ``main`` checkout.

Gates (per Plan internal §Acceptance criteria):

1. The task must have a ``task_plan_path`` registered.
2. The latest ``review_runs`` row for the plan ``subject_path`` with
   ``review_mode="planning"`` must have ``verdict="pass"`` — exactly.
   ``pass_with_findings``, ``conditional_pass``, ``fail``, or absent
   review runs all block.
3. Zero open planning findings for the task. A non-zero count blocks
   regardless of severity (the gate is "clean planning surface", not
   "no high findings").

When all gates pass the receipt's ``ready`` is True and ``next_command``
carries a literal ``git switch main && git checkout <branch> -- <path>
&& git commit -m "docs(<task>): accept plan ..."`` command line the
operator can copy-paste. ``--local`` runs that command list in-process
after verifying the worktree is the canonical root on ``main`` with a
clean tree; any pre-flight failure aborts before touching the index.
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
from .plan_baseline import evaluate_plan_baseline


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


def _build_already_accepted_next_command(task_ref: str) -> str:
    """Forward command for a baseline that is already on ``main``.

    The plan is accepted, so the next lifecycle step is starting the task
    branch, not re-running acceptance.
    """
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
    paths = _dirty_paths(repo)
    if paths is None:
        return False
    return not paths or set(paths) == {plan_path}


def _current_branch(repo: Path) -> str:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
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


def _apply_local_accept(
    repo: Path,
    *,
    task_ref: str,
    branch: str,
    plan_path: str,
) -> tuple[bool, str | None]:
    """Run the docs-only checkout+commit inline. Returns ``(ok, error)``."""
    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    steps: list[list[str]] = []
    if branch != "main":
        steps.append(["git", "-C", str(repo), "checkout", branch, "--", plan_path])
    steps.extend(
        [
            ["git", "-C", str(repo), "add", plan_path],
            ["git", "-C", str(repo), "commit", "-m", msg],
        ]
    )
    for argv in steps:
        proc = _common.run_subprocess(argv)
        if proc.returncode != 0:
            _restore_path_from_head(repo, plan_path)
            return False, f"{argv[3]}_failed: {proc.stderr.strip() or proc.stdout.strip()}"
    return True, None


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
    baseline_next_command = baseline.next_command
    baseline_fields = baseline.to_dict()
    baseline_fields.pop("next_command", None)
    if explicit_plan_path and explicit_source_branch:
        baseline_fields["plan_path_source"] = "cli_plan_arg"
        baseline_fields["identity_state"] = _identity_state_label(identity_state)
    extras.update(baseline_fields)
    noop_command: str | None = None
    already_accepted = (
        baseline.baseline_status == "accepted" and baseline.reason == "already_accepted"
    )
    if already_accepted:
        # internal: an accepted baseline is a satisfied lifecycle
        # state, not a failure. Exit zero and point at the next command.
        noop_command = _build_already_accepted_next_command(task_ref)
        extras["recovery_kind"] = "already_accepted"
        extras["safe_next_commands"] = [
            {"command": noop_command, "reason": "already_accepted"}
        ]
    elif not baseline.acceptance_ready and baseline.reason:
        reasons.append(baseline.reason)

    if baseline.candidate_review_task_refs and not explicit_review_task_ref:
        extras["recovery_kind"] = baseline.detail_reason or "wrong_ref_review_run"

    accept_command = (
        _build_accept_command(
            task_ref=task_ref,
            branch=str(target_branch),
            plan_path=str(plan_path),
        )
        if baseline.acceptance_ready
        else None
    )

    if args.local and not reasons and not already_accepted:
        # Local mode preconditions: canonical root checkout, currently
        # on main, clean tree.
        if repo.resolve() != canonical.resolve():
            reasons.append("local_requires_canonical_root")
        elif _current_branch(repo) != "main":
            reasons.append("local_requires_main_checkout")
        elif not _is_worktree_clean_or_only_plan(repo, str(plan_path)):
            reasons.append("local_requires_clean_tree")
        else:
            ok, err = _apply_local_accept(
                repo,
                task_ref=task_ref,
                branch=str(target_branch),
                plan_path=str(plan_path),
            )
            if not ok:
                reasons.append(err or "local_accept_failed")
            else:
                extras["accepted"] = True

    return _emit(
        command="plan-accept",
        repo=repo,
        task_ref=task_ref,
        ready=not reasons,
        reasons=reasons,
        next_command=noop_command or accept_command or baseline_next_command,
        extras=extras,
        emit_json=args.emit_json,
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
    _common.emit(receipt)
    return 0 if ready else 2
