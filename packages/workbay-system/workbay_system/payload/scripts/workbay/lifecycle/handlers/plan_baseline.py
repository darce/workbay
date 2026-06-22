"""Shared plan-baseline evaluator for lifecycle gates."""

from __future__ import annotations

import json
import shlex
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import resolver

from . import _common


PLANNING_DIR_PREFIXES: tuple[str, ...] = (
    "docs/scopes/",
    "docs/plans/",
    "docs/assessments/",
    "docs/adrs/",
    "docs/reviews/",
    "docs/tech-debt/",
)


@dataclass(frozen=True)
class PlanBaselineStatus:
    task_ref: str
    task_plan_path: str | None
    baseline_exists_on_main: bool
    plan_exists_on_target_branch: bool
    plan_untracked_on_main: bool
    latest_planning_verdict: str | None
    open_planning_findings: int | None
    acceptance_ready: bool
    mcp_available: bool
    baseline_status: str
    reason: str | None
    next_command: str | None
    detail_reason: str | None = None
    plan_path_source: str = "task_plan_path"
    identity_state: str = "active_task_identity"
    source_branch_state: str = "unknown"
    review_task_ref: str | None = None
    candidate_review_task_refs: list[dict[str, object | None]] = field(default_factory=list)
    safe_next_commands: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def is_planning_path(path: str) -> bool:
    if any(path.startswith(prefix) for prefix in PLANNING_DIR_PREFIXES):
        return True
    if path.startswith("packages/"):
        parts = path.split("/")
        if len(parts) >= 5 and parts[2] == "docs" and parts[3] == "tasks":
            return True
    return False


def _workspace(repo: Path) -> Path:
    return resolver.canonical_workspace_root(repo) or repo


def _plan_exists_on_branch(repo: Path, branch: str, plan_path: str) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "cat-file", "-e", f"{branch}:{plan_path}"],
    )
    return proc.returncode == 0


def _current_branch(repo: Path) -> str:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _plan_untracked_on_main(repo: Path, plan_path: str) -> bool:
    if _current_branch(repo) != "main" or not is_planning_path(plan_path):
        return False
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=all"],
    )
    if proc.returncode != 0:
        return False
    return any(line == f"?? {plan_path}" for line in proc.stdout.splitlines())


def query_latest_planning_verdict(
    repo: Path,
    *,
    task_ref: str,
    subject_path: str,
) -> tuple[str | None, bool]:
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(_workspace(repo)),
        "review-runs",
        "--operation", "list",
        "--task-ref", task_ref,
        "--subject-path", subject_path,
        "--review-mode", "planning",
        "--limit", "1",
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None, False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None, False
    data = payload.get("data") if isinstance(payload, dict) else {}
    rows = data.get("runs") if isinstance(data, dict) else None
    if rows is None and isinstance(data, dict):
        rows = data.get("review_runs", [])
    if not isinstance(rows, list) or not rows:
        return None, True
    first = rows[0] if isinstance(rows[0], dict) else {}
    verdict = first.get("verdict")
    return (str(verdict) if verdict else None), True


def query_open_planning_finding_count(
    repo: Path,
    *,
    task_ref: str,
) -> tuple[int, bool]:
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(_workspace(repo)),
        "review-findings",
        "--operation", "list",
        "--status", "open",
        "--task-ref", task_ref,
        "--review-mode", "planning",
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return 0, False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return 0, False
    data = payload.get("data") if isinstance(payload, dict) else {}
    findings = data.get("findings") if isinstance(data, dict) else None
    if isinstance(findings, list):
        return len(findings), True
    counts = data.get("counts") if isinstance(data, dict) else {}
    status = counts.get("status") if isinstance(counts, dict) else {}
    if isinstance(status, dict):
        return int(status.get("open", 0) or 0), True
    return 0, True


def query_candidate_planning_runs(
    repo: Path,
    *,
    task_ref: str,
    subject_path: str,
) -> list[dict[str, object | None]]:
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(_workspace(repo)),
        "review-runs",
        "--operation", "list",
        "--subject-path", subject_path,
        "--review-mode", "planning",
        "--limit", "5",
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return []
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return []
    data = payload.get("data") if isinstance(payload, dict) else {}
    rows = data.get("runs") if isinstance(data, dict) else None
    if rows is None and isinstance(data, dict):
        rows = data.get("review_runs", [])
    if not isinstance(rows, list):
        return []
    candidates: list[dict[str, object | None]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidate_task_ref = row.get("task_ref")
        if not isinstance(candidate_task_ref, str) or candidate_task_ref == task_ref:
            continue
        review_run_id = row.get("review_run_id")
        if review_run_id is None:
            review_run_id = row.get("id")
        candidates.append(
            {
                "task_ref": candidate_task_ref,
                "review_run_id": review_run_id,
                "verdict": str(row.get("verdict")) if row.get("verdict") is not None else None,
                "reviewed_at": str(row.get("reviewed_at")) if row.get("reviewed_at") is not None else None,
                "session": str(row.get("session")) if row.get("session") is not None else None,
            }
        )
    return candidates


def build_acceptance_next_command(
    task_ref: str,
    *,
    plan_path: str | None = None,
    source_branch: str | None = None,
    local: bool = False,
    review_task_ref: str | None = None,
) -> str:
    args = ["--json"]
    if local:
        args.append("--local")
    if plan_path:
        args.extend(["--plan", shlex.quote(plan_path)])
    if source_branch:
        args.extend(["--source-branch", shlex.quote(source_branch)])
    if review_task_ref:
        args.extend(["--review-task-ref", shlex.quote(review_task_ref)])
    if len(args) == 1:
        return f"make plan-accept TASK={task_ref} LIFECYCLE_ARGS=--json"
    return f"make plan-accept TASK={task_ref} LIFECYCLE_ARGS=\"{' '.join(args)}\""


def build_planning_review_command(plan_path: str) -> str:
    return f"make plan-review DOC={shlex.quote(plan_path)}"


def _build_draft_commit_command(task_ref: str, plan_path: str) -> str:
    message = f"docs({task_ref.lower()}): draft task plan"
    return (
        f"git add {shlex.quote(plan_path)} && "
        f"git commit -m {shlex.quote(message)}"
    )


def _safe_next_commands(
    *,
    task_ref: str,
    plan_path: str | None,
    detail_reason: str | None,
    next_command: str | None,
) -> list[dict[str, str]]:
    if next_command and detail_reason:
        return [{"command": next_command, "reason": detail_reason}]
    if detail_reason == "untracked_draft_on_main" and plan_path:
        return [
            {
                "command": _build_draft_commit_command(task_ref, plan_path),
                "reason": detail_reason,
            }
        ]
    return []


def evaluate_plan_baseline(
    repo: Path,
    *,
    task_ref: str,
    task_plan_path: str | None,
    target_branch: str | None = None,
    review_task_ref: str | None = None,
) -> PlanBaselineStatus:
    plan_path = task_plan_path.strip() if isinstance(task_plan_path, str) else ""
    branch = target_branch.strip() if isinstance(target_branch, str) else ""
    # internal: when an explicit review task ref is supplied, planning verdict
    # and open-finding evidence is read from that MAINT-row review while the
    # baseline is still accepted for the implementation ``task_ref``. The
    # default (no override) keeps reading evidence from ``task_ref`` exactly.
    review_ref = review_task_ref.strip() if isinstance(review_task_ref, str) else ""
    review_ref = review_ref or None
    evidence_task_ref = review_ref or task_ref

    if not plan_path:
        detail_reason = "baseline_query_failed"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=task_plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=False,
            plan_untracked_on_main=False,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="unknown",
            reason="task_plan_path_unset",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state="not_checked",
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=task_plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )

    baseline_exists = _plan_exists_on_branch(repo, "main", plan_path)
    target_exists = bool(branch) and _plan_exists_on_branch(repo, branch, plan_path)
    untracked_on_main = _plan_untracked_on_main(repo, plan_path)

    if baseline_exists:
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=True,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="accepted",
            reason="already_accepted",
            next_command=None,
            source_branch_state="plan_present" if target_exists else "not_checked",
        )

    if not branch:
        detail_reason = "baseline_query_failed"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=False,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="missing",
            reason="target_branch_unset",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state="not_checked",
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )

    if not target_exists and not untracked_on_main:
        # The plan exists nowhere: not on the target branch and not untracked
        # on main. untracked_on_main is False throughout this block.
        detail_reason = "plan_missing_on_source_branch"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=False,
            plan_untracked_on_main=False,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="missing",
            reason="plan_missing_on_target_branch",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state="plan_missing",
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )

    source_branch_state = "plan_present" if target_exists else "plan_untracked_on_main"

    verdict, verdict_ok = query_latest_planning_verdict(
        repo, task_ref=evidence_task_ref, subject_path=plan_path,
    )
    if not verdict_ok:
        detail_reason = "baseline_query_failed"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=False,
            baseline_status="unknown",
            reason="planning_review_query_failed",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state=source_branch_state,
            review_task_ref=review_ref,
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )
    if verdict is None and review_ref is not None:
        # The operator named an explicit review task ref, but it has no
        # passing planning run for this exact plan subject (the
        # subject-filtered verdict query returned nothing). Block with an
        # auditable subject-mismatch reason; do not fall back to wrong-ref
        # candidate discovery — the operator already named the evidence row.
        detail_reason = "review_subject_mismatch"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="missing",
            reason="review_subject_mismatch",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state=source_branch_state,
            review_task_ref=review_ref,
        )
    if verdict is None:
        candidate_review_task_refs = query_candidate_planning_runs(
            repo,
            task_ref=task_ref,
            subject_path=plan_path,
        )
        # internal: only a passing review under another (MAINT) ref is
        # adoptable evidence. When exactly one such candidate exists, surface
        # the explicit plan-accept --review-task-ref recovery command so the
        # operator can adopt it auditably; more than one is ambiguous and we
        # refuse to guess; candidates that never passed fall back to a bare
        # re-review instruction.
        eligible = [
            candidate
            for candidate in candidate_review_task_refs
            if candidate.get("verdict") == "pass"
        ]
        if len(eligible) == 1:
            detail_reason = "wrong_ref_review_run"
            next_command = build_acceptance_next_command(
                task_ref,
                plan_path=plan_path,
                source_branch=(
                    "main"
                    if untracked_on_main and not target_exists
                    else (branch or "main")
                ),
                local=untracked_on_main and not target_exists,
                review_task_ref=str(eligible[0]["task_ref"]),
            )
            safe_next_commands = [
                {"command": next_command, "reason": "wrong_ref_review_run_recoverable"}
            ]
        elif len(eligible) > 1:
            detail_reason = "ambiguous_review_candidates"
            next_command = None
            safe_next_commands = []
        else:
            detail_reason = (
                "wrong_ref_review_run"
                if candidate_review_task_refs
                else "planning_verdict_not_pass"
            )
            next_command = (
                build_planning_review_command(plan_path)
                if candidate_review_task_refs
                else None
            )
            safe_next_commands = _safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=next_command,
            )
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="missing",
            reason="no_planning_review_recorded",
            next_command=next_command,
            detail_reason=detail_reason,
            source_branch_state=source_branch_state,
            candidate_review_task_refs=candidate_review_task_refs,
            safe_next_commands=safe_next_commands,
        )
    if verdict != "pass":
        detail_reason = "planning_verdict_not_pass"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=verdict,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="missing",
            reason=f"latest_planning_verdict_{verdict}",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state=source_branch_state,
            review_task_ref=review_ref,
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )

    findings_count, findings_ok = query_open_planning_finding_count(
        repo, task_ref=evidence_task_ref,
    )
    if not findings_ok:
        detail_reason = "baseline_query_failed"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=verdict,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=False,
            baseline_status="unknown",
            reason="planning_findings_query_failed",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state=source_branch_state,
            review_task_ref=review_ref,
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )
    if findings_count > 0:
        detail_reason = "planning_findings_open"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=False,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=verdict,
            open_planning_findings=findings_count,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="missing",
            reason="open_planning_findings",
            next_command=None,
            detail_reason=detail_reason,
            source_branch_state=source_branch_state,
            review_task_ref=review_ref,
            safe_next_commands=_safe_next_commands(
                task_ref=task_ref,
                plan_path=plan_path,
                detail_reason=detail_reason,
                next_command=None,
            ),
        )

    detail_reason = (
        "untracked_draft_on_main"
        if untracked_on_main and not target_exists
        else "accepted_baseline_missing"
    )
    next_command = (
        build_acceptance_next_command(
            task_ref,
            plan_path=plan_path,
            source_branch="main",
            local=True,
            review_task_ref=review_ref,
        )
        if detail_reason == "untracked_draft_on_main"
        else build_acceptance_next_command(task_ref, review_task_ref=review_ref)
    )
    return PlanBaselineStatus(
        task_ref=task_ref,
        task_plan_path=plan_path,
        baseline_exists_on_main=False,
        plan_exists_on_target_branch=target_exists,
        plan_untracked_on_main=untracked_on_main,
        latest_planning_verdict=verdict,
        open_planning_findings=findings_count,
        acceptance_ready=True,
        mcp_available=True,
        baseline_status="missing",
        reason="plan_baseline_missing",
        next_command=next_command,
        detail_reason=detail_reason,
        source_branch_state=source_branch_state,
        review_task_ref=review_ref,
        safe_next_commands=_safe_next_commands(
            task_ref=task_ref,
            plan_path=plan_path,
            detail_reason=detail_reason,
            next_command=next_command,
        ),
    )