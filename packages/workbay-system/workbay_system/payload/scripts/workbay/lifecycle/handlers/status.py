"""Read-only ``status`` subcommand (internal).

Emits a compact git-first receipt describing the current workspace,
branch/task alignment, merge-base availability, daemon posture, and
next-safe lifecycle command without starting any daemons.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import projection_queue
import resolver
from receipts import (
    DaemonStatus,
    DirtySummary,
    HandoffProjection,
    LastTestSummary,
    NextCommand,
    PlanBaselineSummary,
    PlanVisibility,
    ReceiptWarning,
    ReviewState,
    StatusReceipt,
    WorkflowFile,
)

from . import _common
from .plan_baseline import PlanBaselineStatus, evaluate_plan_baseline


PACKAGE_ROOT = Path(__file__).resolve().parents[4]
HARNESS_PROTOCOL = PACKAGE_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
DEFAULT_HANDOFF_TIMEOUT = _common.DEFAULT_HANDOFF_TIMEOUT


def _projection_queue_summary(repo: Path) -> dict[str, object]:
    auto_limit = projection_queue._int_env(
        "WORKBAY_PROJECTION_AUTO_DRAIN_MAX_BYTES",
        projection_queue.AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES,
    )
    hard_limit = projection_queue._int_env(
        "WORKBAY_PROJECTION_HARD_LIMIT_BYTES",
        projection_queue.SPOOL_HARD_LIMIT_BYTES,
    )
    health = projection_queue.spool_health(repo, auto_drain_max_bytes=auto_limit)
    breaker = projection_queue.breaker_state(repo)
    state_dir = repo / ".task-state"
    quarantine_count = 0
    claimed_size_bytes = 0
    if state_dir.is_dir():
        quarantine_dir = repo / projection_queue.QUARANTINE_REL
        if quarantine_dir.is_dir():
            # Count only manifests whose payload was actually moved; a crash between
            # the manifest-first write and the move can leave an incomplete orphan
            # manifest that must not inflate the quarantine count.
            quarantine_count = projection_queue.count_complete_quarantine_manifests(repo)
        for snapshot in state_dir.glob(projection_queue.DRAINING_GLOB):
            try:
                claimed_size_bytes += snapshot.stat().st_size
            except OSError:
                pass
    dead_letter_count, dead_letter_count_capped = projection_queue.dead_letter_count(repo)
    return {
        "live_size_bytes": health.live_size_bytes,
        "live_depth": health.live_depth,
        "live_depth_capped": health.live_depth_capped,
        "has_orphan_draining": health.has_orphan_draining,
        "has_drainable_spool": health.has_drainable_spool,
        "can_auto_drain": health.can_auto_drain,
        "auto_drain_skip_reason": health.auto_drain_skip_reason,
        "soft_limit_bytes": auto_limit,
        "hard_limit_bytes": hard_limit,
        "claimed_size_bytes": claimed_size_bytes,
        "quarantine_count": quarantine_count,
        "breaker_state": breaker.state,
        "consecutive_failures": breaker.consecutive_failures,
        "dead_letter_count": dead_letter_count,
        "dead_letter_count_capped": dead_letter_count_capped,
        "dead_letter_path": str(projection_queue.DEAD_LETTER_REL),
    }


def _run_handoff_json(
    repo: Path,
    *,
    argv: list[str],
    timeout_seconds: float,
) -> tuple[dict[str, Any] | None, ReceiptWarning | None]:
    payload, warning = _common.run_handoff_json(
        repo,
        argv=argv,
        timeout_seconds=timeout_seconds,
        field="handoff",
    )
    if payload is None:
        return None, warning
    if not isinstance(payload, dict):
        return None, ReceiptWarning(
            field="handoff",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    return payload, warning


def _read_handoff_projection(
    repo: Path,
    *,
    timeout_seconds: float,
) -> tuple[HandoffProjection | None, bool, ReceiptWarning | None]:
    payload, warning = _run_handoff_json(
        repo,
        argv=["state", "--sections", "identity", "--detail", "summary"],
        timeout_seconds=timeout_seconds,
    )
    if payload is None:
        return None, False, warning
    data = payload.get("data")
    if not isinstance(data, dict):
        return None, False, ReceiptWarning(
            field="handoff",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    active: Any = data.get("active")
    if active is None:
        # The live handoff CLI returns ``{"active": null, ...}`` to signal
        # "no active handoff state" — a valid empty result, not a payload
        # shape error. Treat it as a clean pending projection so the
        # root-main workflow-loop orientation path matches what ``make
        # tasks`` already sees.
        return None, False, None
    if not isinstance(active, dict):
        return None, False, ReceiptWarning(
            field="handoff",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    return (
        HandoffProjection(
            task_ref=active.get("task_ref") if isinstance(active.get("task_ref"), str) else None,
            status=active.get("status") if isinstance(active.get("status"), str) else None,
            target_branch=(
                active.get("target_branch")
                if isinstance(active.get("target_branch"), str)
                else None
            ),
            target_worktree_path=(
                active.get("target_worktree_path")
                if isinstance(active.get("target_worktree_path"), str)
                else None
            ),
            task_plan_path=(
                active.get("task_plan_path")
                if isinstance(active.get("task_plan_path"), str)
                else None
            ),
        ),
        True,
        None,
    )


def _warning_for_field(field: str, warning: ReceiptWarning | None) -> ReceiptWarning | None:
    if warning is None:
        return None
    return ReceiptWarning(
        field=field,
        reason=warning.reason,
        exception_type=warning.exception_type,
    )


def _read_open_findings_count(
    repo: Path,
    *,
    task_ref: str | None,
    timeout_seconds: float,
) -> tuple[int | None, ReceiptWarning | None]:
    argv = ["review-findings", "--operation", "list", "--status", "open"]
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    payload, warning = _run_handoff_json(repo, argv=argv, timeout_seconds=timeout_seconds)
    if payload is None:
        return None, _warning_for_field("review.open_findings_count", warning)
    severity = payload.get("data", {}).get("counts", {}).get("severity", {})
    if not isinstance(severity, dict):
        return None, ReceiptWarning(
            field="review.open_findings_count",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    return (
        int(severity.get("high", 0) or 0)
        + int(severity.get("medium", 0) or 0)
        + int(severity.get("low", 0) or 0),
        None,
    )


def _read_blockers_count(
    repo: Path,
    *,
    timeout_seconds: float,
) -> tuple[int | None, ReceiptWarning | None]:
    payload, warning = _run_handoff_json(
        repo,
        argv=["state", "--sections", "blockers_open", "--detail", "summary"],
        timeout_seconds=timeout_seconds,
    )
    if payload is None:
        return None, _warning_for_field("review.blockers_count", warning)
    data = payload.get("data")
    if not isinstance(data, dict):
        return None, ReceiptWarning(
            field="review.blockers_count",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    blockers = data.get("blockers_open")
    if not isinstance(blockers, list):
        return None, ReceiptWarning(
            field="review.blockers_count",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    return len(blockers), None


def _read_last_test_summary(
    repo: Path,
    *,
    task_ref: str | None,
    timeout_seconds: float,
) -> tuple[LastTestSummary | None, ReceiptWarning | None]:
    argv = [
        "get-verified-tests",
        "--passed",
        "true",
        "--exclude-never-passed",
        "--limit",
        "1",
    ]
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    payload, warning = _run_handoff_json(repo, argv=argv, timeout_seconds=timeout_seconds)
    if payload is None:
        return None, _warning_for_field("review.last_test_summary", warning)
    tests = payload.get("tests")
    if not isinstance(tests, list):
        tests = payload.get("data", {}).get("tests", {})
    if not isinstance(tests, list):
        return None, ReceiptWarning(
            field="review.last_test_summary",
            reason="malformed",
            exception_type="UnexpectedPayload",
        )
    if not tests:
        return None, None
    first = tests[0] if isinstance(tests[0], dict) else {}
    return (
        LastTestSummary(
            command=first.get("command") if isinstance(first.get("command"), str) else None,
            commit_sha=first.get("commit_sha") if isinstance(first.get("commit_sha"), str) else None,
            passed=first.get("passed") if isinstance(first.get("passed"), bool) else None,
            verified_at=(
                first.get("verified_at") if isinstance(first.get("verified_at"), str) else None
            ),
        ),
        None,
    )


def _review_state(
    repo: Path,
    *,
    task_ref: str | None,
    timeout_seconds: float,
) -> tuple[ReviewState, list[ReceiptWarning]]:
    open_findings_count: int | None = None
    blockers_count: int | None = None
    last_test_summary: LastTestSummary | None = None
    findings_warning: ReceiptWarning | None = None
    blockers_warning: ReceiptWarning | None = None
    tests_warning: ReceiptWarning | None = None

    try:
        open_findings_count, findings_warning = _read_open_findings_count(
            repo,
            task_ref=task_ref,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        findings_warning = ReceiptWarning(
            field="review.open_findings_count",
            reason="exception",
            exception_type=type(exc).__name__,
        )

    try:
        blockers_count, blockers_warning = _read_blockers_count(
            repo,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        blockers_warning = ReceiptWarning(
            field="review.blockers_count",
            reason="exception",
            exception_type=type(exc).__name__,
        )

    try:
        last_test_summary, tests_warning = _read_last_test_summary(
            repo,
            task_ref=task_ref,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        tests_warning = ReceiptWarning(
            field="review.last_test_summary",
            reason="exception",
            exception_type=type(exc).__name__,
        )

    warnings = [
        warning
        for warning in (findings_warning, blockers_warning, tests_warning)
        if warning is not None
    ]

    ready_state = "degraded"
    if blockers_count is not None and blockers_count > 0:
        ready_state = "blocked"
    elif open_findings_count is not None and open_findings_count > 0:
        ready_state = "review_required"
    elif open_findings_count == 0 and blockers_count == 0:
        ready_state = "ready"

    return (
        ReviewState(
            open_findings_count=open_findings_count,
            blockers_count=blockers_count,
            last_test_summary=last_test_summary,
            ready_state=ready_state,
        ),
        warnings,
    )


def _daemon_status() -> DaemonStatus:
    enabled = False
    if HARNESS_PROTOCOL.is_file():
        # Lazy import: keeps `make status`/`make context` working under default
        # python3 even when PyYAML is not installed.
        try:
            import yaml  # noqa: PLC0415
        except ImportError:
            return DaemonStatus(enabled=False, source="harness-protocol")
        try:
            payload = yaml.safe_load(HARNESS_PROTOCOL.read_text()) or {}
        except yaml.YAMLError:
            payload = {}
        orchestrator = payload.get("orchestrator", {}) if isinstance(payload, dict) else {}
        daemons = orchestrator.get("daemons", {}) if isinstance(orchestrator, dict) else {}
        enabled = bool(daemons.get("enabled", False)) if isinstance(daemons, dict) else False
    return DaemonStatus(enabled=enabled, source="harness-protocol")


def _workflow_file(worktree: Path) -> WorkflowFile:
    workflow = worktree / "WORKFLOW.md"
    return WorkflowFile(
        present=workflow.is_file(),
        path=str(workflow) if workflow.is_file() else None,
    )


def _classify_workspace_role(
    branch: str | None, derived_task_ref: str | None
) -> str:
    """Classify the current checkout for internal.

    Root `main` / `master` is the control plane (orientation surface).
    A conforming feature branch with a derivable task ref is the
    implementation plane. Anything else (non-conforming branch,
    detached HEAD, missing branch info) reports as ``unknown`` so
    downstream guidance does not over-promise.
    """
    if branch in ("main", "master"):
        return "control_plane"
    if branch and derived_task_ref:
        return "implementation_plane"
    return "unknown"


def _suggest_next_command(
    branch: str | None,
    derived_task_ref: str | None,
    dirty_total: int,
    *,
    handoff: HandoffProjection | None = None,
    handoff_available: bool = False,
) -> NextCommand:
    if branch in (None, "", "main", "master"):
        # internal: from root `main`, route into the workflow
        # loop. If the handoff projection already shows an active task,
        # the next safe step is `make tasks LIFECYCLE_ARGS=--json` so
        # the operator can pick a worktree to enter. Otherwise the
        # original `make task-start` recommendation stands.
        if handoff_available and handoff is not None and handoff.task_ref:
            return NextCommand(
                command="make tasks LIFECYCLE_ARGS=--json",
                reason="control_plane_with_active_tasks",
            )
        return NextCommand(
            command="make task-start TASK=<ref>",
            reason="main_branch_requires_task_start",
        )
    if derived_task_ref:
        reason = "working_tree_dirty" if dirty_total > 0 else "feature_branch_ready_for_slice"
        return NextCommand(
            command=f"make slice-start TASK={derived_task_ref}",
            reason=reason,
        )
    return NextCommand(
        command="make task-start TASK=<ref>",
        reason="non_conforming_branch_requires_task_start",
    )


def _read_plan_title(plan_path: Path) -> str | None:
    try:
        for line in plan_path.read_text().splitlines():
            if line.startswith("# "):
                return line[2:].strip() or None
    except OSError:
        return None
    return None


def _branch_has_path(repo: Path, branch: str, rel_path: str) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "cat-file", "-e", f"{branch}:{rel_path}"],
    )
    return proc.returncode == 0


def _plan_read_branch(worktree: Path, handoff: HandoffProjection | None, rel_path: str) -> str | None:
    if _branch_has_path(worktree, "main", rel_path):
        return "main"
    if (
        handoff is not None
        and handoff.target_branch
        and _branch_has_path(worktree, handoff.target_branch, rel_path)
    ):
        return handoff.target_branch
    return None


def _worktree_tasks_dir(worktree: Path) -> Path:
    """Re-root the lifecycle package's ``docs/tasks`` dir under ``worktree``.

    ``PACKAGE_ROOT`` is the *code's* location (``parents[4]`` of this file).
    In a normal ``make status`` run the runner code lives inside the operative
    worktree, so ``PACKAGE_ROOT`` and ``worktree`` coincide. But when the
    handler runs against a different tree — the installed code driven against a
    tmp repo in tests, or a status run pointed at a sibling worktree — anchoring
    on ``PACKAGE_ROOT`` leaks the code-bundled plans into a receipt that is
    meant to describe ``worktree``. Re-root the package-relative tasks subpath
    under ``worktree`` so plan visibility always reflects the inspected tree.
    Falls back to ``PACKAGE_ROOT`` when the package is not inside a git repo or
    sits outside its own repo root.
    """
    pkg_repo_root = resolver.repo_root(PACKAGE_ROOT)
    if pkg_repo_root is not None:
        try:
            pkg_rel = PACKAGE_ROOT.relative_to(pkg_repo_root)
        except ValueError:
            pkg_rel = None
        if pkg_rel is not None:
            return worktree / pkg_rel / "docs" / "tasks"
    return PACKAGE_ROOT / "docs" / "tasks"


def _plan_visibility(
    worktree: Path,
    derived_task_ref: str | None,
    handoff: HandoffProjection | None,
) -> PlanVisibility:
    effective_task_ref = derived_task_ref or (handoff.task_ref if handoff is not None else None)
    if not effective_task_ref:
        return PlanVisibility(
            path=None,
            exists=False,
            title=None,
            task_ref_matches_branch=None,
            stale_reason="branch_has_no_task_ref",
        )

    if handoff is not None and handoff.task_plan_path:
        rel_path = handoff.task_plan_path
        candidate = worktree / rel_path
        exists = candidate.is_file()
        read_branch = _plan_read_branch(worktree, handoff, rel_path)
        read_command = f"make plan-show TASK={effective_task_ref}" if read_branch else None
        read_receipt = (
            f"plan: {read_branch}:{rel_path} (read: {read_command})"
            if read_branch and read_command
            else None
        )
        task_ref_matches_branch = (
            handoff.task_ref == effective_task_ref
            if handoff.task_ref is not None
            else candidate.name.startswith(f"{effective_task_ref}-") or candidate.stem == effective_task_ref
        )
        return PlanVisibility(
            path=rel_path,
            exists=exists,
            title=_read_plan_title(candidate) if exists else None,
            task_ref_matches_branch=task_ref_matches_branch,
            stale_reason=None if exists else "missing_from_worktree",
            read_branch=read_branch,
            read_command=read_command,
            read_receipt=read_receipt,
        )

    plan_path: Path | None = None
    if plan_path is None:
        local_plan = worktree / "plans" / f"{effective_task_ref}.md"
        if local_plan.is_file():
            plan_path = local_plan
    if plan_path is None:
        tasks_dir = _worktree_tasks_dir(worktree)
        matches = sorted(tasks_dir.glob(f"{effective_task_ref}-*-task-plan.md")) if tasks_dir.is_dir() else []
        if matches:
            plan_path = matches[0]
    if plan_path is None:
        return PlanVisibility(
            path=None,
            exists=False,
            title=None,
            task_ref_matches_branch=None,
            stale_reason="missing_from_worktree",
        )

    relative_path = str(plan_path.relative_to(worktree)) if plan_path.is_relative_to(worktree) else str(plan_path)
    task_ref_matches_branch = (
        handoff.task_ref == effective_task_ref
        if handoff is not None and handoff.task_ref is not None
        else plan_path.name.startswith(f"{effective_task_ref}-") or plan_path.stem == effective_task_ref
    )
    return PlanVisibility(
        path=relative_path,
        exists=True,
        title=_read_plan_title(plan_path),
        task_ref_matches_branch=task_ref_matches_branch,
        stale_reason=None,
    )


def _plan_baseline_summary(status: PlanBaselineStatus) -> PlanBaselineSummary:
    return PlanBaselineSummary(
        status=status.baseline_status,
        reason=status.reason,
        task_plan_path=status.task_plan_path,
        target_branch=None,
        next_command=status.next_command,
        acceptance_ready=status.acceptance_ready,
        plan_untracked_on_main=status.plan_untracked_on_main,
    )


def _project_plan_baseline(
    repo: Path,
    derived_task_ref: str | None,
    handoff: HandoffProjection | None,
) -> PlanBaselineSummary | None:
    """Evaluate the active task's plan baseline for the status receipt.

    internal. Returns ``None`` when no task or plan is in scope
    so the caller does not surface ``baseline=unknown`` for genuinely
    planless workflows (e.g. fresh root-main orientation). When the
    handoff projection carries a ``task_plan_path``, hand the evaluator
    the projection's ``target_branch`` so missing/accepted/unknown
    derive from the same probes the lifecycle gate uses.
    """
    task_ref = derived_task_ref or (handoff.task_ref if handoff is not None else None)
    plan_path = handoff.task_plan_path if handoff is not None else None
    target_branch = handoff.target_branch if handoff is not None else None
    if not task_ref or not plan_path:
        return None
    status = evaluate_plan_baseline(
        repo,
        task_ref=task_ref,
        task_plan_path=plan_path,
        target_branch=target_branch,
    )
    summary = _plan_baseline_summary(status)
    return PlanBaselineSummary(
        status=summary.status,
        reason=summary.reason,
        task_plan_path=summary.task_plan_path,
        target_branch=target_branch,
        next_command=summary.next_command,
        acceptance_ready=summary.acceptance_ready,
        plan_untracked_on_main=summary.plan_untracked_on_main,
    )


def _error_receipt(error: str, *, worktree: Path | None = None, git_facts: _common.GitFacts | None = None) -> dict[str, object]:
    return {
        "ok": False,
        "command": "status",
        "task_ref": git_facts.derived_task_ref if git_facts is not None else None,
        "branch": git_facts.branch if git_facts is not None else "",
        "worktree_path": str(worktree) if worktree is not None else "",
        "head": git_facts.head if git_facts is not None else "",
        "handoff_projection": "error",
        "error": error,
    }


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle status", add_help=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument(
        "--handoff-timeout",
        dest="handoff_timeout",
        type=float,
        default=DEFAULT_HANDOFF_TIMEOUT,
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root()
    if repo is None:
        _common.emit(_error_receipt("not_in_git_repo"))
        return 2

    worktree = resolver.current_worktree(repo) or repo
    git_facts = _common.gather_git_facts(repo)
    if not (worktree / "Makefile").is_file():
        _common.emit(_error_receipt("missing_makefile", worktree=worktree, git_facts=git_facts))
        return 2

    cwd = Path.cwd()
    branch = git_facts.branch
    head = git_facts.head
    derived_task_ref = git_facts.derived_task_ref
    target_branch = branch or "main"
    target_worktree = worktree
    handoff, handoff_available, handoff_warning = _read_handoff_projection(
        repo,
        timeout_seconds=args.handoff_timeout,
    )
    warnings: list[ReceiptWarning] = []
    if handoff_warning is not None:
        warnings.append(handoff_warning)
    review = ReviewState(
        open_findings_count=None,
        blockers_count=None,
        last_test_summary=None,
        ready_state="pending_handoff",
    )
    if handoff_available:
        review, review_warnings = _review_state(
            repo,
            task_ref=handoff.task_ref if handoff is not None and handoff.task_ref else derived_task_ref,
            timeout_seconds=args.handoff_timeout,
        )
        warnings.extend(review_warnings)

    workspace_role = _classify_workspace_role(branch, derived_task_ref)
    canonical_worktree_path: str | None = None
    if handoff is not None and handoff.target_worktree_path:
        canonical_worktree_path = handoff.target_worktree_path

    plan_baseline = _project_plan_baseline(repo, derived_task_ref, handoff)

    from handlers import project_events_replay as replay_mod  # noqa: PLC0415

    projection_spool_depth, projection_spool_depth_capped = (
        replay_mod.spool_depth_summary(repo)
    )
    if projection_spool_depth > 0:
        reason = f"depth={projection_spool_depth}"
        if projection_spool_depth_capped:
            reason = f"depth>={projection_spool_depth}"
        warnings.append(
            ReceiptWarning(
                field="projection_spool",
                reason=reason,
            )
        )
    elif replay_mod.has_drainable_spool(repo):
        warnings.append(
            ReceiptWarning(
                field="projection_spool",
                reason="orphan_draining_snapshot",
            )
        )

    receipt = StatusReceipt(
        ok=True,
        command="status",
        task_ref=derived_task_ref,
        branch=branch,
        worktree_path=str(worktree),
        head=head,
        handoff_projection="synced" if handoff_available else "pending",
        repo_root=str(repo),
        cwd=str(cwd),
        cwd_matches_target=cwd == target_worktree,
        target_worktree_path=str(target_worktree),
        target_branch=target_branch,
        merge_base_available=resolver.merge_base(repo, target_branch) is not None,
        dirty_summary=DirtySummary(**git_facts.dirty_summary),
        daemon_status=_daemon_status(),
        workflow_file=_workflow_file(worktree),
        plan=_plan_visibility(worktree, derived_task_ref, handoff),
        handoff_available=handoff_available,
        handoff=handoff,
        review=review,
        warnings=warnings,
        next_command=_suggest_next_command(
            branch,
            derived_task_ref,
            git_facts.dirty_summary["total"],
            handoff=handoff,
            handoff_available=handoff_available,
        ),
        workspace_role=workspace_role,
        canonical_worktree_path=canonical_worktree_path,
        plan_baseline=plan_baseline,
        projection_spool_depth=projection_spool_depth,
        projection_queue=_projection_queue_summary(repo),
    )

    _common.emit(receipt.to_dict())
    return 0
