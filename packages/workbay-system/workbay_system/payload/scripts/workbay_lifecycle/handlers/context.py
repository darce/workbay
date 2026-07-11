"""Read-only ``context`` subcommand (internal).

Reports the lifecycle facts an operator or agent needs to decide what
to do next: current worktree path, branch, HEAD, branch-derived task
ref, dirty summary, plan path (derived on read from MCP via
``render-handoff --kind=current_task --no-write``), and a
next-recommended-command hint. For read-only ops the
cwd/branch is the source of truth; if the local handoff snapshot
disagrees with the branch-derived task ref, the branch wins and the
handler logs a ``workflow_ambiguity_resolved`` decision (or spools
the payload when MCP is offline).

Receipt schema follows the documented §JSON Receipt Schema for the
git-first lifecycle primitives.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path
from typing import Any

import resolver
import projection

from . import _common
from .plan_baseline import evaluate_plan_baseline


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _target_worktree_matches_repo(active: dict[str, Any], repo: Path) -> bool:
    target = active.get("target_worktree_path")
    if not isinstance(target, str) or not target:
        return True
    try:
        return Path(target).expanduser().resolve() == repo.resolve()
    except (OSError, RuntimeError, ValueError):
        return False


def _read_preferred_task_projection(repo: Path, task_ref: str | None) -> dict[str, Any]:
    if not task_ref or task_ref.startswith(".") or any(sep in task_ref for sep in ("/", "\\", "\x00")):
        return {}
    workspace = resolver.canonical_workspace_root(repo) or repo
    target = workspace / ".task-state" / "current" / f"{task_ref}.json"
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("task_ref") != task_ref:
        return {}
    if not _common.snapshot_is_live(payload):
        return {}
    if not _target_worktree_matches_repo(payload, repo):
        return {}
    return payload


def _read_active_state(
    repo: Path,
    *,
    preferred_task_ref: str | None = None,
) -> dict[str, Any]:
    """Derive the live ``active`` block via ``render-handoff --no-write``.

    internal: the on-disk ``CURRENT_TASK.json`` is no longer
    consulted; the workspace summary is derived from MCP's live state
    on each call. Under ``workspace_ambiguous``, prefer only the explicit
    branch-derived task for the current worktree; otherwise do not pick a
    global winner.
    """
    view = _common.derive_workspace_summary_view(repo)
    if view.shape == "single" and isinstance(view.active, dict):
        return view.active
    if view.shape == "workspace_ambiguous" and preferred_task_ref:
        for task in view.tasks:
            if task.get("task_ref") != preferred_task_ref:
                continue
            if not _common.snapshot_is_live(task):
                continue
            if not _target_worktree_matches_repo(task, repo):
                continue
            return task
    fallback = _read_preferred_task_projection(repo, preferred_task_ref)
    if fallback:
        return fallback
    return {}




def _flatten_handoff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") == 2:
        data = payload.get("data")
        if isinstance(data, dict):
            flat = {**payload, **data}
            scope = payload.get("scope")
            if isinstance(scope, dict) and "task_ref" not in flat and scope.get("task_ref"):
                flat["task_ref"] = scope["task_ref"]
            return flat
    return payload


def _reap_backstop(repo: Path) -> dict[str, Any] | None:
    """Dry-run reap sweep; report closeable rows without mutating state."""
    payload, _warning = _common.run_handoff_json(
        repo,
        argv=["archive", "--operation", "reap"],
        field="reap_backstop",
        timeout_seconds=2.0,
    )
    if payload is None or not isinstance(payload, dict):
        return None
    flat = _flatten_handoff_payload(payload)
    if not flat.get("ok", True):
        return None
    closeable = flat.get("closeable") or []
    if not closeable:
        return None
    return {
        "applied": bool(flat.get("applied", False)),
        "closeable_count": len(closeable),
        "closeable": closeable,
        "next_command": "make task-reap REAP_ARGS=--apply",
    }


def _retention_backstop(repo: Path) -> dict[str, Any] | None:
    """Dry-run task_archives retention; report prune candidates only."""
    payload, _warning = _common.run_handoff_json(
        repo,
        argv=["archive", "--operation", "retention"],
        field="retention_backstop",
        timeout_seconds=2.0,
    )
    if payload is None or not isinstance(payload, dict):
        return None
    flat = _flatten_handoff_payload(payload)
    if not flat.get("ok", True):
        return None
    would_prune = flat.get("would_prune") or []
    if not would_prune:
        return None
    return {
        "applied": bool(flat.get("applied", False)),
        "older_than_days": flat.get("older_than_days", 90),
        "would_prune_count": len(would_prune),
        "would_prune": would_prune,
        "next_command": "mcp-workbay-handoff archive --operation retention --apply",
    }


def _suggest_next_command(
    branch: str | None, derived_task_ref: str | None, dirty_total: int
) -> str:
    if branch in (None, "", "main", "master"):
        return "make tasks  # list active tasks, then `make task-start TASK=<ref>`"
    if derived_task_ref is None:
        return (
            "branch is non-conforming; rename to feature/<task-ref> "
            "or run `make task-start TASK=<ref>` to create a worktree"
        )
    if dirty_total > 0:
        return f"make slice-start TASK={derived_task_ref}  # working tree dirty"
    return f"make slice-start TASK={derived_task_ref}"


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle context", add_help=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = resolver.repo_root()
    if repo is None:
        receipt: dict[str, Any] = {
            "ok": False,
            "command": "context",
            "task_ref": None,
            "branch": "",
            "worktree_path": "",
            "head": "",
            "handoff_projection": "error",
            "events": [],
            "dirty_summary": {"staged": 0, "unstaged": 0, "untracked": 0, "total": 0},
            "plan_path": None,
            "next_command": "cd into a git repository and re-run `make context`",
            "error": "not_in_git_repo",
        }
        _common.emit(receipt)
        return 2

    git_facts = _common.gather_git_facts(repo)
    branch = git_facts.branch
    head = git_facts.head
    derived_task_ref = git_facts.derived_task_ref
    dirty = git_facts.dirty_summary
    active_state = _read_active_state(repo, preferred_task_ref=derived_task_ref)
    plan_path = active_state.get("task_plan_path") if _common.snapshot_is_live(active_state) else None
    if not isinstance(plan_path, str):
        plan_path = None

    events = ["context_loaded"]
    handoff_projection = "synced"

    snapshot_task_ref = active_state.get("task_ref")
    if (
        isinstance(snapshot_task_ref, str)
        and snapshot_task_ref
        and _common.snapshot_is_live(active_state)
        and derived_task_ref is not None
        and snapshot_task_ref != derived_task_ref
    ):
        decision_id = (
            f"claude_workflow_ambiguity_resolved_"
            f"{derived_task_ref.replace('-', '_').lower()}_{_utc_stamp()}"
        )
        rationale = (
            f"branch-derived task {derived_task_ref!r} beats handoff "
            f"snapshot {snapshot_task_ref!r} for read-only context"
        )
        status, _id = projection.project_decision(
            repo,
            decision_id=decision_id,
            rationale=rationale,
            session=decision_id,
            task_ref=derived_task_ref,
        )
        events.append("ambiguity_resolved")
        if status in ("spooled", "pending", "error", "dropped"):
            # internal: surface ``spooled`` (CLI rejection) and ``dropped``
            # (shed: spool + dead-letter both full) in addition to ``pending``
            # (CLI unreachable) so receipts route loud projection failures to the
            # operator instead of masquerading as durable queueing.
            handoff_projection = status

    next_command = _suggest_next_command(branch, derived_task_ref, dirty["total"])

    reap_backstop = None
    retention_backstop = None
    if branch in {"main", "master"}:
        reap_backstop = _reap_backstop(repo)
        if reap_backstop is not None:
            events.append("reap_backstop_reported")
        retention_backstop = _retention_backstop(repo)
        if retention_backstop is not None:
            events.append("retention_backstop_reported")

    plan_baseline_dict: dict[str, Any] | None = None
    active_task_ref = active_state.get("task_ref") if isinstance(active_state, dict) else None
    if isinstance(plan_path, str) and isinstance(active_task_ref, str) and active_task_ref:
        target_branch = active_state.get("target_branch") if isinstance(active_state, dict) else None
        baseline = evaluate_plan_baseline(
            repo,
            task_ref=active_task_ref,
            task_plan_path=plan_path,
            target_branch=str(target_branch) if isinstance(target_branch, str) else None,
        )
        plan_baseline_dict = {
            "status": baseline.baseline_status,
            "reason": baseline.reason,
            "task_plan_path": baseline.task_plan_path,
            "target_branch": (
                str(target_branch) if isinstance(target_branch, str) else None
            ),
            "next_command": baseline.next_command,
            "acceptance_ready": baseline.acceptance_ready,
            "plan_untracked_on_main": baseline.plan_untracked_on_main,
        }
        events.append("plan_baseline_evaluated")
        # internal: when the evaluator says the baseline is
        # ready for acceptance, hoist its recovery command above the
        # generic slice-start hint so root-main orientation immediately
        # points at `make plan-accept`.
        if baseline.acceptance_ready and baseline.next_command:
            next_command = baseline.next_command

    # internal+3: best-effort dual-spool reclaimer. Never raise /
    # never fail make context ([REF-20]/[AGT-10]); counts land in receipt + stderr.
    agent_errors_replay = _common.maybe_replay_agent_error_spool(repo)
    if agent_errors_replay is not None:
        events.append("agent_errors_replayed")
    terminal_guard_replay = _common.maybe_replay_terminal_guard_spool(repo)
    if terminal_guard_replay is not None:
        events.append("terminal_guard_replayed")
    # internal: belt-and-suspenders stale /tmp/workbay-* reclaimer.
    stale_dev_temp_reap = _common.maybe_reap_stale_dev_temp(repo)
    if stale_dev_temp_reap is not None and (stale_dev_temp_reap.get("removed") or []):
        events.append("stale_dev_temp_reaped")

    receipt = {
        "ok": True,
        "command": "context",
        "task_ref": derived_task_ref,
        "branch": branch,
        "worktree_path": str(repo),
        "head": head,
        "handoff_projection": handoff_projection,
        "events": events,
        "dirty_summary": dirty,
        "plan_path": plan_path,
        "next_command": next_command,
        "plan_baseline": plan_baseline_dict,
        "reap_backstop": reap_backstop,
        "retention_backstop": retention_backstop,
        "agent_errors_replay": agent_errors_replay,
        "terminal_guard_replay": terminal_guard_replay,
        "stale_dev_temp_reap": stale_dev_temp_reap,
    }

    if not args.emit_json:
        if plan_path:
            plan_line = f"plan: {branch}:{plan_path}"
        else:
            plan_line = "plan: <unset> — populate via make plan-register PLAN=..."
        if plan_baseline_dict is not None:
            baseline_line = (
                f"plan_baseline: status={plan_baseline_dict['status']} "
                f"reason={plan_baseline_dict.get('reason') or '-'}"
            )
        else:
            baseline_line = None
        sys.stderr.write(
            f"context: branch={branch} task_ref={derived_task_ref} "
            f"head={head[:12]} dirty={dirty['total']} "
            f"projection={handoff_projection}\n"
            f"{plan_line}\n"
        )
        if baseline_line is not None:
            sys.stderr.write(f"{baseline_line}\n")
        if reap_backstop is not None:
            refs = ", ".join(
                str(entry.get("task_ref", "?"))
                for entry in (reap_backstop.get("closeable") or [])
                if isinstance(entry, dict)
            )
            sys.stderr.write(
                "reap_backstop: closeable="
                f"{reap_backstop.get('closeable_count', 0)} ({refs}) — "
                f"run `{reap_backstop.get('next_command')}`\n"
            )
        if retention_backstop is not None:
            refs = ", ".join(
                str(entry.get("task_ref", "?"))
                for entry in (retention_backstop.get("would_prune") or [])
                if isinstance(entry, dict)
            )
            sys.stderr.write(
                "retention_backstop: would_prune="
                f"{retention_backstop.get('would_prune_count', 0)} ({refs}) — "
                f"run `{retention_backstop.get('next_command')}`\n"
            )
        sys.stderr.write(f"next: {next_command}\n")

    _common.emit(receipt)
    return 0
