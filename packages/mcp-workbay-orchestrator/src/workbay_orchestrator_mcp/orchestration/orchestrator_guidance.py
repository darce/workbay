"""Worker-guidance resolution: classify, apply, and cycle through worker guidance messages."""

from __future__ import annotations

import subprocess
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from orchestrator_helpers import (
    _combined_text,
    _json_list_text,
    _message_timestamp,
    _normalize_text,
    _require_dict_payload,
)

# ---------------------------------------------------------------------------
# Guidance markers
# ---------------------------------------------------------------------------

_RESOLVED_MARKERS = (
    "already resolved",
    "already covered",
    "already present",
    "already correct",
    "already appears",
    "already wired",
    "no code changes were warranted",
    "no lane-owned code changes were warranted",
    "no stale fallback wiring was found",
    "appears already resolved",
    "work appears present already",
    "existing coverage",
    "substantially covered",
    "no code changes were needed",
    "already fixed",
    "work already done",
    "verification passed",
)

_REMAINING_WORK_MARKERS = (
    "remaining domain implementation target",
    "highest-priority open lane-owned gap",
    "open domain work still appears",
    "remaining open frontend slice",
    "remaining slice",
    "next slice",
    "still appears to be",
)

_ENV_BLOCKER_MARKERS = (
    "read-only",
    "sandbox",
    "writable temp directory",
    "no usable temporary directory",
    "mypy is not available",
    "mypy was unavailable",
    "postgresql is not running",
    "permissionerror",
    "vendor is a symlink",
    "vendor/bin/phpunit",
    "vendor/bin/phpstan",
    "composer install",
    "npm install",
    "node_modules",
    "command not found",
    "exit with code 127",
)

GUIDANCE_STALL_THRESHOLD = 3

#: Token stamped on allow-with-warning review notes when git ancestry is
#: unknowable. Unknowable is not a miss: consumers must not take the blocked
#: arm just because ``merge-base --is-ancestor`` could not decide.
UNKNOWN_ANCESTRY_TOKEN = "unknown_ancestry"


# ---------------------------------------------------------------------------
# GuidanceResolution
# ---------------------------------------------------------------------------


class GuidanceResolutionKind(StrEnum):
    """Closed set of resolution kinds emitted by the guidance classifier.

    internal: replaces ad-hoc magic-string comparisons on
    ``GuidanceResolution.kind`` so new kinds fail fast at construction and at
    the (exhaustive) comparison sites.
    """

    MESSAGE = "message"
    REVIEW = "review"
    REDISPATCH = "redispatch"
    BLOCKED = "blocked"
    FATAL_ERROR = "fatal_error"


class GuidanceResolution:
    def __init__(
        self,
        *,
        kind: GuidanceResolutionKind | str,
        lane_id: str,
        worker_message_id: int,
        latest_report_id: int | None = None,
        decision: str = "",
        rationale: str | None = None,
        lane_status: str = "review",
        lane_notes: str | None = None,
        dispatch_subject: str | None = None,
        dispatch_message: str | None = None,
        close_dispatch_ids: tuple[int, ...] = (),
        error: str | None = None,
    ) -> None:
        # Coerce plain strings so downstream callers always see the StrEnum.
        self.kind: GuidanceResolutionKind = GuidanceResolutionKind(kind)
        self.lane_id = lane_id
        self.worker_message_id = worker_message_id
        self.latest_report_id = latest_report_id
        self.decision = decision
        self.rationale = rationale
        self.lane_status = lane_status
        self.lane_notes = lane_notes
        self.dispatch_subject = dispatch_subject
        self.dispatch_message = dispatch_message
        self.close_dispatch_ids = close_dispatch_ids
        self.error = error


# ---------------------------------------------------------------------------
# MCP query helpers
# ---------------------------------------------------------------------------


#: Page size for the paginated open-guidance scan below.
_GUIDANCE_QUEUE_PAGE_SIZE = 200

#: Hard stop on pagination so a backend that never reports exhaustion cannot
#: spin this helper forever.
_GUIDANCE_QUEUE_MAX_PAGES = 200


def _list_open_worker_guidance(task_ref: str) -> list[dict[str, Any]]:
    """Return every open worker-to-orchestrator message for ``task_ref``.

    This scan previously read a single capped page and treated it as the whole
    queue. The open-message queue grows without bound while the cap does not,
    so once the backlog passed one page the cycle could no longer even *see*
    the lanes it was supposed to answer: page one was re-read every cycle and
    everything behind it was unreachable regardless of how the router behaved.
    The direction filter compounds it — it is applied after the cap, so a page
    full of orchestrator-side rows yields no work at all.

    Page until the backend reports exhaustion instead. A queue drain must not
    be bounded by an arbitrary window.
    """
    from workbay_orchestrator_mcp.lanes import lane_communication  # noqa: PLC0415

    collected: list[dict[str, Any]] = []
    offset = 0
    for _ in range(_GUIDANCE_QUEUE_MAX_PAGES):
        payload = _require_dict_payload(
            lane_communication(
                kind="message",
                operation="list",
                task_ref=task_ref,
                status="open",
                limit=_GUIDANCE_QUEUE_PAGE_SIZE,
                offset=offset,
                fields="id,lane_id,session,direction,subject,message,status,created_at,updated_at",
            ),
            source=f"lane_communication(list worker guidance:offset={offset})",
        )
        if payload.get("ok") is not True:
            raise RuntimeError("Failed to list lane messages.")
        rows = payload.get("messages")
        if not isinstance(rows, list) or not rows:
            break
        collected.extend(
            row for row in rows if isinstance(row, dict) and row.get("direction") == "worker_to_orchestrator"
        )
        # Prefer the backend's own exhaustion signal; fall back to a short page
        # so a payload without ``has_more`` still terminates.
        has_more = payload.get("has_more")
        if has_more is False or (has_more is None and len(rows) < _GUIDANCE_QUEUE_PAGE_SIZE):
            break
        offset += len(rows)
    return collected


def _dedupe_worker_guidance_messages(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the newest open worker guidance message per lane."""
    latest_by_lane: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        lane_id = _normalize_text(row.get("lane_id"))
        if not lane_id:
            continue
        current = latest_by_lane.get(lane_id)
        candidate_key = (_message_timestamp(row), int(row.get("id") or 0))
        current_key = (
            (_message_timestamp(current), int(current.get("id") or 0)) if isinstance(current, dict) else ("", 0)
        )
        if current is None or candidate_key >= current_key:
            latest_by_lane[lane_id] = row
    return list(latest_by_lane.values())


def _list_open_dispatch_messages(task_ref: str, lane_id: str) -> list[dict[str, Any]]:
    from workbay_orchestrator_mcp.lanes import lane_communication  # noqa: PLC0415

    payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            status="open",
            limit=200,
            fields="id,direction",
        ),
        source=f"lane_communication(list dispatch messages:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lane messages for {lane_id}.")
    rows = payload.get("messages", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict) and row.get("direction") == "orchestrator_to_worker"]


def _latest_lane_report(task_ref: str, lane_id: str, *, session: str | None = None) -> dict[str, Any] | None:
    from workbay_orchestrator_mcp.lanes import worker_reports  # noqa: PLC0415

    payload = _require_dict_payload(
        worker_reports(
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            limit=20,
            # commit_sha is what makes this row decidable. The projection used
            # to select prose only (summary + blockers), which meant every
            # downstream classification was necessarily a guess about text: the
            # one field that says whether the lane actually produced work was
            # never fetched, so "did this land?" was unanswerable by
            # construction rather than merely unanswered.
            # error is required for the no-file pin-refusal backstop: classify
            # reads report error/summary/blockers_json only, never worker prose.
            # blockers_json is the live blockers column; in-memory tests may
            # still pass a list-valued blockers field directly.
            fields="id,session,summary,error,blockers_json,commit_sha,branch,merge_ready,status,outcome",
        ),
        source=f"worker_reports(list:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list worker reports for {lane_id}.")
    reports = payload.get("reports", [])
    if not isinstance(reports, list):
        return None
    if session:
        for report in reports:
            if isinstance(report, dict) and report.get("session") == session:
                return report
    for report in reports:
        if isinstance(report, dict):
            return report
    return None


#: Page size for the paginated lane lookup below. A task accumulates lanes
#: without bound, so a single capped page can never be assumed to contain the
#: lane being resolved.
_LANE_LOOKUP_PAGE_SIZE = 200

#: Hard stop on pagination so a backend that never reports exhaustion cannot
#: spin this helper forever.
_LANE_LOOKUP_MAX_PAGES = 100


def _lane_row(task_ref: str, lane_id: str) -> dict[str, Any]:
    """Return the lane row for ``lane_id``, paging until found or exhausted.

    This lookup previously scanned only the first page of ``list`` and raised
    "Lane not found" for anything past it. A task's lane count grows without
    bound, so that cap turned a routine lookup into a false absence for every
    lane beyond the window, and the resulting ``RuntimeError`` aborted the
    entire guidance cycle. Page instead: a lookup-by-id must not be bounded by
    an arbitrary window.
    """
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    offset = 0
    for _ in range(_LANE_LOOKUP_MAX_PAGES):
        payload = _require_dict_payload(
            manage_worktree_lane(
                operation="list",
                task_ref=task_ref,
                status="all",
                limit=_LANE_LOOKUP_PAGE_SIZE,
                offset=offset,
            ),
            source=f"manage_worktree_lane(list:{task_ref}:offset={offset})",
        )
        if payload.get("ok") is not True:
            raise RuntimeError(f"Failed to list lanes for {task_ref}.")
        lanes = payload.get("lanes") or []
        for lane in lanes:
            if isinstance(lane, dict) and lane.get("lane_id") == lane_id:
                return lane
        if not lanes:
            break
        # Prefer the backend's own exhaustion signal; fall back to a short page
        # so a payload without ``has_more`` still terminates.
        has_more = payload.get("has_more")
        if has_more is False or (has_more is None and len(lanes) < _LANE_LOOKUP_PAGE_SIZE):
            break
        offset += len(lanes)
    raise RuntimeError(f"Lane {lane_id} not found for task {task_ref}.")


def _lane_activity(task_ref: str, lane_id: str) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import get_lane_activity  # noqa: PLC0415

    payload = _require_dict_payload(
        get_lane_activity(lane_id=lane_id, task_ref=task_ref, limit_actions=50),
        source=f"get_lane_activity({lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to fetch lane activity for {lane_id}.")
    return payload


def _pending_lane_actions(activity: dict[str, Any]) -> list[dict[str, Any]]:
    rows = activity.get("actions", [])
    if not isinstance(rows, list):
        return []
    pending = [row for row in rows if isinstance(row, dict) and row.get("status") == "pending"]
    return sorted(pending, key=lambda row: (int(row.get("priority", 100)), int(row.get("id", 0))))


# ---------------------------------------------------------------------------
# Guidance classification and resolution
# ---------------------------------------------------------------------------


def _resolve_next_assignment(
    task_ref: str, lane_id: str, activity: dict[str, Any], text: str
) -> tuple[str, str] | None:
    pending_actions = _pending_lane_actions(activity)
    if pending_actions:
        action = pending_actions[0]
        return (
            f"{lane_id} next assignment",
            _normalize_text(action.get("action")),
        )

    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from orchestrator_guidance_policy import resolve_assignment as resolve_policy_assignment

    policy_assignment = resolve_policy_assignment(task_ref, lane_id, text, activity)
    if policy_assignment is not None:
        return policy_assignment

    lane = activity.get("lane", {})
    if any(marker in text for marker in _REMAINING_WORK_MARKERS):
        objective = _normalize_text(lane.get("objective"))
        if objective:
            return (f"{lane_id} next assignment", objective)
    return None


def _is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool | None:
    """Tri-state ancestry probe: True, False, or None when git cannot decide.

    ``git merge-base --is-ancestor`` exits 0 when ``ancestor`` is contained in
    ``descendant`` and 1 when it is not. Every other outcome — exit 128
    (unknown SHA / not a repo), OSError, TimeoutExpired, or any other
    non-0/1 status — is unknowable. Collapsing those onto False made
    "could not determine" look like "definitely not landed" and parked
    finished slices as blocked.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, descendant],
            capture_output=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    return None


def _lane_git_repo(orchestrator_root: Path, activity: dict[str, Any]) -> Path:
    """Prefer the lane worktree for git probes; fall back to orchestrator_root."""
    lane = activity.get("lane") or {}
    if isinstance(lane, dict):
        worktree_path = _normalize_text(lane.get("worktree_path"))
        if worktree_path:
            return Path(worktree_path)
    return orchestrator_root


def _git_text(repo: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if result.returncode != 0:
        return ""
    return (result.stdout or "").strip()


def _default_branch_ref(repo: Path) -> str:
    origin_head = _git_text(repo, "symbolic-ref", "--short", "-q", "refs/remotes/origin/HEAD")
    if origin_head:
        return origin_head
    for candidate in ("main", "master", "origin/main", "origin/master"):
        if _git_text(repo, "rev-parse", "--verify", "--quiet", candidate):
            return candidate
    return ""


def _derive_baseline_sha(repo: Path, lane_branch: str) -> str:
    """merge-base of the lane branch with the default branch in ``repo``."""
    default_ref = _default_branch_ref(repo)
    if not default_ref or not lane_branch:
        return ""
    return _git_text(repo, "merge-base", default_ref, lane_branch)


def _assignment_baseline(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    activity: dict[str, Any],
    *,
    repo: Path | None = None,
    lane_branch: str = "",
) -> str:
    """Return the assignment baseline SHA, deriving it when the row omits one.

    ``worktree_lanes`` has no ``base_sha`` column and the lane manifest leaves
    the field optional, so production rows almost never carry a baseline.
    Requiring one made the landed-commit gate a hard no. Derive
    ``merge-base(default-branch, lane-branch)`` inside the lane worktree
    instead of adding a schema column.
    """
    lane = activity.get("lane") or {}
    baseline = _normalize_text(lane.get("base_sha")) if isinstance(lane, dict) else ""
    if baseline:
        return baseline

    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    try:
        from lane_manifest import get_lane_config

        config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        config = None
    if isinstance(config, dict):
        baseline = _normalize_text(config.get("base_sha"))
        if baseline:
            return baseline

    git_repo = repo if repo is not None else orchestrator_root
    return _derive_baseline_sha(git_repo, lane_branch)


def _landed_commit_sha(
    latest_report: dict[str, Any] | None,
    *,
    orchestrator_root: Path | None = None,
    task_ref: str = "",
    lane_id: str = "",
    activity: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    """Return ``(sha, unknown_ancestry)`` for a report that may have landed.

    ``sha`` is empty only when the probe *proves* nothing landed (missing
    inputs, or git exit 1). ``unknown_ancestry`` is True when git could not
    decide; callers must degrade typed (allow-with-warning) instead of
    taking the blocked arm.

    A report SHA records write context, including for failed reports. It proves
    landed work only when git also proves the SHA is contained by the recorded
    lane branch and is at or ahead of the assignment baseline. Equal SHAs are
    ancestry-true: a plan's anchor commit can be its own lane SHA.

    ``merge_ready`` is deliberately *not* required. It is the worker's own
    self-assessment and is routinely 0 on submissions that did land a green
    commit, so gating on it re-parks exactly the reports this is meant to
    route.
    """
    if not isinstance(latest_report, dict) or orchestrator_root is None or not isinstance(activity, dict):
        return "", False
    candidate = _normalize_text(latest_report.get("commit_sha"))
    lane = activity.get("lane") or {}
    lane_branch = _normalize_text(lane.get("branch")) if isinstance(lane, dict) else ""
    if not candidate or not lane_branch:
        return "", False
    repo = _lane_git_repo(orchestrator_root, activity)
    baseline = _assignment_baseline(
        orchestrator_root,
        task_ref,
        lane_id,
        activity,
        repo=repo,
        lane_branch=lane_branch,
    )
    contained = _is_ancestor(repo, candidate, lane_branch)
    if contained is False:
        return "", False
    if baseline and candidate == baseline:
        # Identity is ancestry-true; do not short-circuit equal SHAs as a miss.
        return candidate, contained is None
    if not baseline:
        # Typed degrade: production rows often have no base_sha and derivation
        # can fail. That is unknown, not a miss — do not take the blocked arm.
        return candidate, True
    ahead = _is_ancestor(repo, baseline, candidate)
    if ahead is False:
        return "", False
    unknown = contained is None or ahead is None
    return candidate, unknown


def _lane_dispatch_identity(
    task_ref: str,
    lane_id: str,
    activity: dict[str, Any],
    orchestrator_root: Path | None,
) -> tuple[str, str | None]:
    """Return ``(backend_id, model)`` for a lane, preferring activity then manifest."""
    lane = activity.get("lane") if isinstance(activity.get("lane"), dict) else {}
    backend = _normalize_text(lane.get("preferred_backend")) or _normalize_text(lane.get("backend"))
    model = _normalize_text(lane.get("preferred_model")) or _normalize_text(lane.get("model")) or None
    if orchestrator_root is None:
        return backend, model
    if backend and model:
        return backend, model
    try:
        from lane_manifest import get_lane_config

        config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        config = None
    if isinstance(config, dict):
        backend = backend or _normalize_text(config.get("preferred_backend"))
        model = model or _normalize_text(config.get("preferred_model")) or None
    return backend, model or None


_SUCCESS_REPORT_MARKERS = frozenset({"success", "succeeded", "ok", "passed", "completed"})


def _report_indicates_success(latest_report: dict[str, Any]) -> bool:
    """Return True when the latest report's status or outcome is a success."""
    outcome = _normalize_text(latest_report.get("outcome")).lower()
    status = _normalize_text(latest_report.get("status")).lower()
    return outcome in _SUCCESS_REPORT_MARKERS or status in _SUCCESS_REPORT_MARKERS


def _latest_failure_result(latest_report: dict[str, Any] | None) -> dict[str, object] | None:
    """Build a ``classify_failure`` payload from the latest report only.

    Worker guidance prose is not part of the classifier haystack: a later
    successful report, or next-assignment text that quotes an old pin
    refusal, must not re-escalate REDISPATCH. Missing reports skip classify
    and leave the durable inspect path as the only backstop.
    """
    if not isinstance(latest_report, dict):
        return None
    if _report_indicates_success(latest_report):
        return {"ok": True}
    result: dict[str, object] = {"ok": False}
    outcome = latest_report.get("outcome")
    if outcome:
        result["outcome"] = outcome
    error = _normalize_text(latest_report.get("error"))
    summary = _normalize_text(latest_report.get("summary"))
    result["error"] = " ".join(part for part in (error, summary) if part)
    blockers = latest_report.get("blockers")
    if isinstance(blockers, list):
        result["blockers"] = blockers
    else:
        blockers_text = _json_list_text(latest_report.get("blockers_json"))
        if blockers_text:
            result["blockers"] = [blockers_text]
    return result


def _redispatch_configuration_block_reason(
    *,
    task_ref: str,
    lane_id: str,
    activity: dict[str, Any],
    latest_report: dict[str, Any] | None,
    orchestrator_root: Path | None,
) -> str | None:
    """Return a reason to refuse REDISPATCH when the configuration breaker applies."""
    from workbay_orchestrator_mcp.orchestration import dispatch_breaker

    payload = _latest_failure_result(latest_report)
    if payload is not None:
        failure_class = dispatch_breaker.classify_failure(payload)
        if failure_class == "configuration":
            return "latest failure classified as configuration; operator reset required"

    if orchestrator_root is None:
        return None
    backend_id, model = _lane_dispatch_identity(task_ref, lane_id, activity, orchestrator_root)
    admission = dispatch_breaker.inspect(orchestrator_root, backend_id or None, model=model)
    if admission.failure_class != "configuration":
        return None
    return admission.reason


def _classify_guidance(
    *,
    task_ref: str,
    worker_message: dict[str, Any],
    latest_report: dict[str, Any] | None,
    activity: dict[str, Any],
    open_dispatches: list[dict[str, Any]],
    orchestrator_root: Path | None = None,
) -> GuidanceResolution:
    lane_id = _normalize_text(worker_message.get("lane_id"))
    worker_message_id_raw = worker_message.get("id")
    if worker_message_id_raw is None:
        raise RuntimeError("Worker guidance message is missing an id.")
    worker_message_id = int(worker_message_id_raw)
    latest_report_id = (
        int(latest_report["id"]) if isinstance(latest_report, dict) and latest_report.get("id") is not None else None
    )
    combined = _combined_text(
        worker_message.get("subject"),
        worker_message.get("message"),
        latest_report.get("summary") if isinstance(latest_report, dict) else "",
        _json_list_text(latest_report.get("blockers_json")) if isinstance(latest_report, dict) else "",
    )
    close_dispatch_ids = tuple(
        int(row["id"]) for row in open_dispatches if isinstance(row, dict) and row.get("id") is not None
    )

    pending_actions = _pending_lane_actions(activity)
    has_resolved_marker = any(marker in combined for marker in _RESOLVED_MARKERS)
    has_env_blocker = any(marker in combined for marker in _ENV_BLOCKER_MARKERS)

    if has_resolved_marker and not pending_actions:
        return GuidanceResolution(
            kind=GuidanceResolutionKind.REVIEW,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Resolved worker guidance for {lane_id} by closing stale work and marking the lane ready for review.",
            rationale="Worker report indicates the assigned lane slice is already satisfied in the current branch state.",
            lane_status="review",
            lane_notes="Orchestrator confirmed the worker guidance reflected already-satisfied lane work.",
            close_dispatch_ids=close_dispatch_ids,
        )

    next_assignment = _resolve_next_assignment(task_ref, lane_id, activity, combined)

    if next_assignment is not None:
        block_reason = _redispatch_configuration_block_reason(
            task_ref=task_ref,
            lane_id=lane_id,
            activity=activity,
            latest_report=latest_report,
            orchestrator_root=orchestrator_root,
        )
        if block_reason is not None:
            return GuidanceResolution(
                kind=GuidanceResolutionKind.FATAL_ERROR,
                lane_id=lane_id,
                worker_message_id=worker_message_id,
                latest_report_id=latest_report_id,
                error=block_reason,
                decision=f"Failed to resolve worker guidance for {lane_id}.",
                rationale=block_reason,
                close_dispatch_ids=close_dispatch_ids,
            )
        subject, message = next_assignment
        return GuidanceResolution(
            kind=GuidanceResolutionKind.REDISPATCH,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Resolved worker guidance for {lane_id} by dispatching the next lane assignment.",
            rationale="Worker reported the prior slice as satisfied or blocked and identified a concrete remaining lane-owned target.",
            lane_status="active",
            lane_notes="Orchestrator resolved worker guidance and dispatched the next lane-owned slice.",
            dispatch_subject=subject,
            dispatch_message=message,
            close_dispatch_ids=close_dispatch_ids,
        )

    landed_commit, unknown_ancestry = _landed_commit_sha(
        latest_report,
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        activity=activity,
    )
    if landed_commit:
        # Landed work goes to the gate, never to the blocked bucket.
        #
        # Every branch below this point classifies by matching prose markers,
        # so a worker that finished its slice and described the result in words
        # no marker list anticipated fell through to the fallback and was parked
        # as blocked -- with its commit already sitting on the lane branch. The
        # lane was then dead: blocked is terminal for the router, so the work
        # never reached review and never merged.
        #
        # A landed commit is a fact; the prose is a guess. Prefer the fact.
        #
        # This is the guidance-layer form of the green-commit-despite-block
        # rule the offload contract already states: when a self-verified commit
        # landed and the block is a post-commit stage, route to the review gate
        # and do not re-dispatch, because re-dispatch re-runs the pass against
        # an already-green tree and deterministically re-hits the same block.
        #
        # Ordering: this sits *after* _resolve_next_assignment so a lane with a
        # concrete remaining lane-owned target still advances, and *before* the
        # environment-blocker and fallback branches so landed work outranks any
        # prose-derived block. Routing to review is not merging -- the
        # branch-complete review gate still adjudicates.
        #
        # unknown_ancestry is allow-with-warning, not a miss: git could not
        # prove containment, so we still route to review instead of blocked.
        decision = f"Routed {lane_id} to the review gate on its landed commit {landed_commit}."
        rationale = (
            "Worker report carries a landed commit on the lane branch, so the slice produced work "
            "regardless of how the submission text reads. Landed work is routed to the review gate "
            "rather than parked as blocked."
        )
        lane_notes = f"Orchestrator routed the lane to review on landed commit {landed_commit}."
        if unknown_ancestry:
            decision = (
                f"Routed {lane_id} to the review gate with {UNKNOWN_ANCESTRY_TOKEN} "
                f"warning on commit {landed_commit}."
            )
            rationale = (
                f"Worker report carries commit {landed_commit} but git ancestry was unknowable "
                f"({UNKNOWN_ANCESTRY_TOKEN}). Unknowable is not a miss; allow-with-warning "
                "routes to the review gate instead of parking the lane as blocked."
            )
            lane_notes = (
                f"Orchestrator routed the lane to review on commit {landed_commit} "
                f"with {UNKNOWN_ANCESTRY_TOKEN} warning; git could not prove containment."
            )
        return GuidanceResolution(
            kind=GuidanceResolutionKind.REVIEW,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=decision,
            rationale=rationale,
            lane_status="review",
            lane_notes=lane_notes,
            close_dispatch_ids=close_dispatch_ids,
        )

    if has_env_blocker:
        return GuidanceResolution(
            kind=GuidanceResolutionKind.BLOCKED,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Resolved worker guidance for {lane_id} by marking the lane blocked for operator/environment follow-up.",
            rationale="Worker report indicates an environment or sandbox blocker without a safe automatic redispatch target.",
            lane_status="blocked",
            lane_notes="Worker needs a writable or better-provisioned environment before the next lane step can continue.",
            close_dispatch_ids=close_dispatch_ids,
        )

    if combined.strip():
        return GuidanceResolution(
            kind=GuidanceResolutionKind.BLOCKED,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Classified unrecognized worker guidance for {lane_id} as blocked (fallback).",
            rationale="Guidance text present but did not match known resolved, redispatch, or environment-blocked patterns.",
            lane_status="blocked",
            lane_notes="Unclassifiable guidance; marked blocked for operator review.",
            dispatch_subject=None,
            dispatch_message=None,
            close_dispatch_ids=close_dispatch_ids,
            error=None,
        )

    return GuidanceResolution(
        kind=GuidanceResolutionKind.FATAL_ERROR,
        lane_id=lane_id,
        worker_message_id=worker_message_id,
        latest_report_id=latest_report_id,
        error=f"Unable to classify worker guidance for lane {lane_id}.",
        decision=f"Failed to resolve worker guidance for {lane_id}.",
        rationale="Guidance message did not match a known resolved, redispatchable, or environment-blocked pattern.",
        close_dispatch_ids=close_dispatch_ids,
    )


def _apply_guidance_resolution(
    *,
    task_ref: str,
    orchestrator_root: Path,
    resolution: GuidanceResolution,
    dry_run: bool = False,
) -> GuidanceResolution:
    from workbay_handoff_mcp import record_decision, update_next_actions  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication, manage_worktree_lane  # noqa: PLC0415

    lane = _lane_row(task_ref, resolution.lane_id)
    if dry_run:
        return resolution

    lane_communication(
        kind="message",
        operation="update",
        message_id=resolution.worker_message_id,
        status="closed",
        task_ref=task_ref,
    )
    for message_id in resolution.close_dispatch_ids:
        lane_communication(
            kind="message",
            operation="update",
            message_id=message_id,
            status="closed",
            task_ref=task_ref,
        )

    from lane_manifest import get_lane_config

    lane_cfg = get_lane_config(task_ref, resolution.lane_id) or {}

    # Derive owner_agent from backend if not already set in DB
    existing_owner = _normalize_text(lane.get("owner_agent"))
    backend = _normalize_text(lane_cfg.get("preferred_backend"))

    owner_agent = existing_owner
    if not owner_agent:
        if backend and "claude" in backend.lower():
            owner_agent = "claude"
        elif backend and "codex" in backend.lower():
            owner_agent = "codex"
        else:
            owner_agent = backend or "codex-subagent"

    manage_worktree_lane(
        operation="upsert",
        task_ref=task_ref,
        lane_id=resolution.lane_id,
        worktree_path=str(lane.get("worktree_path") or ""),
        branch=str(lane.get("branch") or ""),
        title=_normalize_text(lane.get("title")) or None,
        objective=_normalize_text(lane.get("objective")) or None,
        owner_agent=owner_agent,
        status=resolution.lane_status,
        notes=resolution.lane_notes,
    )

    if resolution.kind == GuidanceResolutionKind.REVIEW:
        for action in _pending_lane_actions(_lane_activity(task_ref, resolution.lane_id)):
            action_id = action.get("id")
            if action_id is None:
                continue
            update_next_actions(
                operation="update",
                action_id=int(action_id),
                status="done",
                task_ref=task_ref,
            )

    if resolution.kind == GuidanceResolutionKind.REDISPATCH and resolution.dispatch_message:
        _dispatch_payload: dict | None = None
        try:
            from workbay_handoff_mcp import artifact_index as _art_idx
            from workbay_handoff_mcp.config import RuntimeConfig as _ArtCfg

            _art_config = _ArtCfg.for_repo(orchestrator_root)
            _art_ref = _art_idx.maybe_record_artifact(
                task_ref=task_ref,
                lane_id=resolution.lane_id,
                app_root=None,
                source_kind="guidance-redispatch",
                source_label=f"{resolution.lane_id}-guidance",
                content_type="text/plain",
                summary=str(resolution.dispatch_subject or f"{resolution.lane_id} next assignment"),
                content=resolution.dispatch_message,
                artifact_db_path=_art_config.artifact_db_path,
                min_bytes=_art_config.artifact_index_min_bytes,
                min_lines=_art_config.artifact_index_min_lines,
            )
            if _art_ref is not None:
                _dispatch_payload = {"artifacts": [str(_art_ref["source_id"])]}
        except Exception:  # noqa: BLE001
            pass
        lane_communication(
            kind="message",
            operation="record",
            task_ref=task_ref,
            lane_id=resolution.lane_id,
            session=f"{task_ref}-orchestrator-guidance",
            direction="orchestrator_to_worker",
            subject=resolution.dispatch_subject,
            message=resolution.dispatch_message,
            status="open",
            payload=_dispatch_payload,
        )

    record_decision(
        session=f"{task_ref}-orchestrator-daemon",
        decision=resolution.decision,
        rationale=resolution.rationale,
        task_ref=task_ref,
    )
    return resolution


def _resolve_guidance_cycle(
    orchestrator_root: Path,
    task_ref: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> list[GuidanceResolution]:
    from workbay_handoff_mcp import record_decision

    results: list[GuidanceResolution] = []
    for worker_message in _dedupe_worker_guidance_messages(_list_open_worker_guidance(task_ref)):
        lane_id = _normalize_text(worker_message.get("lane_id"))
        if not lane_id:
            continue
        worker_message_id = int(worker_message.get("id") or 0)
        if callable(log):
            log(
                "INFO",
                "guidance_detected",
                lane=lane_id,
                worker_message_id=worker_message_id,
            )
        # Bulkhead: one lane's resolution must never strand every other lane's.
        # These helpers reach the DB and the lane store and can raise for
        # reasons entirely local to this lane (missing row, unreadable
        # activity, malformed report). Before this guard a single raise
        # propagated out of the cycle and aborted the whole phase, so one bad
        # lane silently held the entire guidance queue hostage. Contain the
        # failure as this lane's FATAL_ERROR and keep draining the queue; the
        # caller already counts consecutive FATAL_ERRORs per lane and stalls
        # the lane at GUIDANCE_STALL_THRESHOLD, so containment here cannot
        # become an unbounded retry.
        try:
            latest_report = _latest_lane_report(
                task_ref,
                lane_id,
                session=_normalize_text(worker_message.get("session")) or None,
            )
            activity = _lane_activity(task_ref, lane_id)
            open_dispatches = _list_open_dispatch_messages(task_ref, lane_id)
            resolution = _classify_guidance(
                task_ref=task_ref,
                worker_message=worker_message,
                latest_report=latest_report,
                activity=activity,
                open_dispatches=open_dispatches,
                orchestrator_root=orchestrator_root,
            )
            if resolution.kind == GuidanceResolutionKind.FATAL_ERROR:
                if not dry_run:
                    record_decision(
                        session=f"{task_ref}-orchestrator-daemon",
                        decision=resolution.decision,
                        rationale=resolution.rationale,
                        task_ref=task_ref,
                    )
                results.append(resolution)
                continue
            results.append(
                _apply_guidance_resolution(
                    task_ref=task_ref,
                    orchestrator_root=orchestrator_root,
                    resolution=resolution,
                    dry_run=dry_run,
                )
            )
        except Exception as exc:  # noqa: BLE001 - per-lane containment is the point
            if callable(log):
                log(
                    "ERROR",
                    "guidance_lane_failed",
                    lane=lane_id,
                    worker_message_id=worker_message_id,
                    error=str(exc),
                )
            results.append(
                GuidanceResolution(
                    kind=GuidanceResolutionKind.FATAL_ERROR,
                    lane_id=lane_id,
                    worker_message_id=worker_message_id,
                    decision=f"guidance_resolution_failed:{lane_id}",
                    rationale=f"Guidance resolution raised for lane {lane_id}: {exc}",
                    error=str(exc),
                )
            )
    return results
