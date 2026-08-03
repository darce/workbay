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
    # implementation note S1: open planning findings for other plan subjects under the
    # same task_ref. Informational only — does not block acceptance.
    open_planning_findings_other_subjects: int | None = None

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


def normalize_planning_file_path(path: str | None) -> str:
    """Normalize a finding or plan path for subject comparison.

    Strips whitespace and collapses a leading ``./`` segment so
    ``./docs/plans/x.md`` and ``docs/plans/x.md`` compare equal. Empty
    and ``None`` become ``""`` (caller treats that as unattributed).
    """
    if path is None:
        return ""
    text = str(path).strip()
    while text.startswith("./"):
        text = text[2:]
    return text


def _workspace(repo: Path) -> Path:
    return resolver.canonical_workspace_root(repo) or repo


def _plan_exists_on_branch(repo: Path, branch: str, plan_path: str) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "cat-file", "-e", f"{branch}:{plan_path}"],
    )
    return proc.returncode == 0


def _plan_blob_sha_on_branch(repo: Path, branch: str, plan_path: str) -> str | None:
    """Return the blob object name for ``branch:plan_path``, or None if absent.

    Uses ``git rev-parse <ref>:<path>`` so callers can compare content by
    object identity without materializing file bytes. Fail-closed: a probe
    error is indistinguishable from absence (returns None).
    """
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", f"{branch}:{plan_path}"],
    )
    if proc.returncode != 0:
        return None
    sha = (proc.stdout or "").strip()
    return sha or None


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
    plan_path: str | None = None,
) -> tuple[int, bool] | tuple[int, int, bool]:
    """Count open planning findings for ``task_ref``.

    Without ``plan_path``, returns ``(total_count, ok)`` — the historical
    contract used by any non-subject-scoped caller.

    With ``plan_path``, returns ``(subject_count, other_subjects_count, ok)``
    where ``subject_count`` is the number of open planning findings whose
    ``file_path`` equals the plan under acceptance and ``other_subjects_count``
    is the remainder (same task_ref, different plan subjects). The gate blocks
    only on ``subject_count``; the remainder is informational.
    """
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
        return (0, 0, False) if plan_path is not None else (0, False)
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return (0, 0, False) if plan_path is not None else (0, False)
    data = payload.get("data") if isinstance(payload, dict) else {}
    findings = data.get("findings") if isinstance(data, dict) else None
    if isinstance(findings, list):
        if plan_path is None:
            return len(findings), True
        # Fail closed on unattributed findings (NULL/empty file_path): they
        # count toward the SUBJECT (blocking) bucket, not other_subjects.
        # Normalize both sides (strip, collapse leading './') before compare.
        subject_norm = normalize_planning_file_path(plan_path)
        subject_count = 0
        other_count = 0
        for row in findings:
            if not isinstance(row, dict):
                other_count += 1
                continue
            raw_fp = row.get("file_path")
            finding_norm = normalize_planning_file_path(
                raw_fp if isinstance(raw_fp, str) else None
            )
            # None/empty → subject (blocking). Matching normalized path → subject.
            if not finding_norm or finding_norm == subject_norm:
                subject_count += 1
            else:
                other_count += 1
        return subject_count, other_count, True
    counts = data.get("counts") if isinstance(data, dict) else {}
    status = counts.get("status") if isinstance(counts, dict) else {}
    open_total = int(status.get("open", 0) or 0) if isinstance(status, dict) else 0
    if plan_path is not None:
        # Counts envelope has no per-row file_path; treat the total as
        # subject-scoped so the gate stays conservative without a list.
        return open_total, 0, True
    return open_total, True


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

    # Presence on main is not acceptance when the source branch still holds a
    # different (reviewed) blob. A false "already_accepted" is a false green
    # (eng [RLSE-05], [OBS-08]): compare content before short-circuiting.
    # Heuristic: eng [DATA-14] dual copies diverge silently without a single
    # content authority; eng [RLSE-02] gates are not suggestions.
    baseline_stale_vs_source = False
    if baseline_exists:
        if target_exists and branch:
            main_sha = _plan_blob_sha_on_branch(repo, "main", plan_path)
            source_sha = _plan_blob_sha_on_branch(repo, branch, plan_path)
            # Fail closed when either probe fails despite cat-file presence:
            # treat as stale so we never report already_accepted on unknown
            # content equality.
            if (
                main_sha is None
                or source_sha is None
                or main_sha != source_sha
            ):
                baseline_stale_vs_source = True
        if not baseline_stale_vs_source:
            # Identical blobs, or plan absent on the source branch: keep the
            # historical already_accepted no-op (content on main is the
            # accepted baseline, or there is no source copy to compare).
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
            baseline_exists_on_main=baseline_exists,
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
            baseline_exists_on_main=baseline_exists,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="stale" if baseline_stale_vs_source else "missing",
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
            baseline_exists_on_main=baseline_exists,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=None,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="stale" if baseline_stale_vs_source else "missing",
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
            baseline_exists_on_main=baseline_exists,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=verdict,
            open_planning_findings=None,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="stale" if baseline_stale_vs_source else "missing",
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

    findings_result = query_open_planning_finding_count(
        repo, task_ref=evidence_task_ref, plan_path=plan_path,
    )
    # plan_path is always non-empty here, so the subject-scoped triple is returned.
    findings_count, other_subjects_count, findings_ok = findings_result  # type: ignore[misc]
    if not findings_ok:
        detail_reason = "baseline_query_failed"
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=baseline_exists,
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
            baseline_exists_on_main=baseline_exists,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=verdict,
            open_planning_findings=findings_count,
            acceptance_ready=False,
            mcp_available=True,
            baseline_status="stale" if baseline_stale_vs_source else "missing",
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
            open_planning_findings_other_subjects=other_subjects_count,
        )

    if baseline_stale_vs_source:
        # Main has a plan blob, but it is not the reviewed source copy.
        # Gates passed: surface stale (not already_accepted) so plan-accept
        # routes through the normal readiness/apply path rather than the
        # already_accepted no-op success.
        detail_reason = "baseline_stale_vs_source"
        next_command = build_acceptance_next_command(
            task_ref, review_task_ref=review_ref
        )
        return PlanBaselineStatus(
            task_ref=task_ref,
            task_plan_path=plan_path,
            baseline_exists_on_main=True,
            plan_exists_on_target_branch=target_exists,
            plan_untracked_on_main=untracked_on_main,
            latest_planning_verdict=verdict,
            open_planning_findings=findings_count,
            acceptance_ready=True,
            mcp_available=True,
            baseline_status="stale",
            reason="baseline_stale_vs_source",
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
            open_planning_findings_other_subjects=other_subjects_count,
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
        open_planning_findings_other_subjects=other_subjects_count,
    )