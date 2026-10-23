"""Side-effect-free preflight validators (internal).

These helpers mirror :func:`validate_decision_id` exactly — synchronous,
no DB writes, no envelope mutations. Callers use them to learn the
exact corrective payload that would unblock a mutating call without
having to first attempt and bounce off the mutating path.

The validators live here rather than under ``api.py`` so a lint guard
can prove they never appear in the auto-regen dirty set.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .handoff_state import get_handoff_state
from .review_findings_queries import list_review_findings
from .shared_write_context import _classify_commit_relation

CANONICAL_BLOCKER_KINDS: frozenset[str] = frozenset(
    {
        "open_findings",
        "contract_co_change_missing",
        "dirty_protected_paths",
        "behind_main",
        "descendant_commit_required",
        "unknown_task",
    }
)


def _git_head_sha(workspace_root: Path | None = None) -> str | None:
    """Return current ``HEAD`` SHA, or ``None`` if git is unavailable."""

    cmd = ["git", "rev-parse", "HEAD"]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(workspace_root) if workspace_root else None,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    sha = proc.stdout.strip()
    return sha or None


def _task_known(task_ref: str) -> bool:
    """Read-only check that ``task_ref`` exists in handoff state.

    The v2 envelope from ``get_handoff_state(sections='identity')``
    surfaces the row under ``data.active`` (a dict when found, ``None``
    otherwise). The earlier ``data['identity']`` lookup was always
    absent so every known task was mis-reported as missing.
    """

    try:
        envelope = get_handoff_state(task_ref=task_ref, sections="identity")
    except Exception:
        return False
    data = envelope.get("data") if "data" in envelope else envelope
    if not isinstance(data, dict):
        return False
    active = data.get("active")
    return isinstance(active, dict) and bool(active.get("task_ref"))


def _open_findings_for(task_ref: str) -> list[dict[str, Any]]:
    """Read-only fetch of open findings; returns empty list on any error."""

    try:
        envelope = list_review_findings(task_ref=task_ref, status="open", limit=200, detail="summary")
    except Exception:
        return []
    data = envelope.get("data") if "data" in envelope else envelope
    if not isinstance(data, dict):
        return []
    findings = data.get("findings")
    if isinstance(findings, list):
        return [f for f in findings if isinstance(f, dict)]
    return []


def validate_review_ready(
    task_ref: str,
    branch: str | None = None,
) -> dict[str, Any]:
    """Preflight ``review-ready``: report what would block merge without writing.

    Returns ``{ok, blockers[{kind, detail, suggested}], boundary_state{...}}``.
    ``kind`` values come from :data:`CANONICAL_BLOCKER_KINDS`. ``suggested``
    names the next safe operation a caller should run.
    """

    blockers: list[dict[str, Any]] = []
    boundary_state: dict[str, Any] = {
        "task_ref": task_ref,
        "branch": branch,
        "head": _git_head_sha(),
    }

    task_known = _task_known(task_ref)
    if not task_known:
        blockers.append(
            {
                "kind": "unknown_task",
                "detail": (
                    f"task {task_ref!r} is not present in handoff state; "
                    "register it with set_handoff_state(...) before requesting review-ready."
                ),
                "suggested": "set_handoff_state(task_ref=..., status='in_progress', ...)",
            }
        )
        boundary_state["task_known"] = False
        return {"ok": False, "blockers": blockers, "boundary_state": boundary_state}
    boundary_state["task_known"] = True

    open_findings = _open_findings_for(task_ref)
    if open_findings:
        finding_ids = [f.get("finding_id") or f.get("id") for f in open_findings if isinstance(f, dict)]
        blockers.append(
            {
                "kind": "open_findings",
                "detail": (
                    f"task {task_ref!r} has {len(open_findings)} open review finding(s); "
                    "branch is not review-ready until each is resolved or deferred."
                ),
                "suggested": "review_findings(operation='update', status='fixed', ...)",
                "finding_ids": finding_ids,
            }
        )
    boundary_state["open_findings_count"] = len(open_findings)

    return {
        "ok": not blockers,
        "blockers": blockers,
        "boundary_state": boundary_state,
    }


def validate_finding_resolution(
    finding_id_or_db_id: str | int,
    fixed_commit_sha: str | None = None,
) -> dict[str, Any]:
    """Preflight finding resolution: name the commit SHA that would mark it fixed.

    Returns ``{ok, error, suggested{commit_sha, reason}}``. When
    ``fixed_commit_sha`` is omitted, suggests the current ``HEAD`` if it
    would satisfy the internal same-or-newer-descendant rule. Performs no
    DB writes.
    """

    finding_id: str | None = None
    finding_db_id: int | None = None
    if isinstance(finding_id_or_db_id, int):
        finding_db_id = finding_id_or_db_id
    else:
        text = str(finding_id_or_db_id).strip()
        if text.isdigit():
            finding_db_id = int(text)
        else:
            finding_id = text or None

    try:
        envelope = list_review_findings(
            finding_id=finding_id,
            finding_db_id=finding_db_id,
            limit=1,
            detail="summary",
        )
    except Exception as exc:  # pragma: no cover — defensive fallback
        return {
            "ok": False,
            "error": f"could not look up finding: {exc!r}",
            "suggested": {"commit_sha": None, "reason": "lookup_failed"},
        }

    data = envelope.get("data") if isinstance(envelope, dict) else None
    findings = data.get("findings") if isinstance(data, dict) else None
    if not findings:
        return {
            "ok": False,
            "error": (
                f"no review finding matches id={finding_id_or_db_id!r}; "
                "verify the id from review_findings(operation='list', ...)."
            ),
            "suggested": {"commit_sha": None, "reason": "finding_not_found"},
        }

    finding = findings[0] if isinstance(findings[0], dict) else {}
    finding_commit_sha = finding.get("commit_sha") if isinstance(finding.get("commit_sha"), str) else None

    head = _git_head_sha()
    if fixed_commit_sha is None and head is None:
        return {
            "ok": False,
            "error": "fixed_commit_sha omitted and HEAD could not be resolved.",
            "suggested": {"commit_sha": None, "reason": "head_unavailable"},
        }

    suggested_sha = fixed_commit_sha or head

    # internal same-or-newer-descendant rule: a finding can only be
    # marked fixed from the commit that recorded it or a descendant.
    # ``ancestor`` (workspace older) and ``diverged`` (no shared
    # history) are blocked at preflight so the caller learns the
    # corrective payload before attempting the mutating call.
    if finding_commit_sha is not None and suggested_sha is not None:
        relation = _classify_commit_relation(finding_commit_sha, suggested_sha)
        if relation in {"ancestor", "diverged"}:
            return {
                "ok": False,
                "error": (
                    f"commit {suggested_sha!r} is '{relation}' relative to the finding's "
                    f"recorded commit {finding_commit_sha!r}; a finding can only be marked "
                    "fixed from the same commit or a descendant."
                ),
                "suggested": {
                    "commit_sha": None,
                    "reason": "descendant_commit_required",
                    "finding_commit_sha": finding_commit_sha,
                    "candidate_commit_sha": suggested_sha,
                    "commit_relation": relation,
                },
            }

    return {
        "ok": True,
        "error": None,
        "suggested": {
            "commit_sha": suggested_sha,
            "reason": ("current_head" if fixed_commit_sha is None else "caller_provided"),
        },
    }
