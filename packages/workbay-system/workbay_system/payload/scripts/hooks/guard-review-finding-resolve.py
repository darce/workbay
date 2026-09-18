#!/usr/bin/env python3
"""PreToolUse hook: redirect review finding closures to the single working call.

Direct ``review_findings(operation="update", status="fixed")`` loses the
commit-backed reconciliation path. Block it once with a *case-exact* redirect
(T9): inspect the attempted write + finding state and name only the call that
works for that case.

Cases:
- open + live task row → ``operation=resolve``
- orphan / done / archived task ref → ``operation=disposition``
- already-closed finding → no-op message (do not update)

Structured rejection envelope from ``workbay_handoff_mcp.structured_rejections``
([REF-19] single source — no second phrasing to drift).
"""

from __future__ import annotations

import json
import sys
from typing import Any

# Stable fallbacks kept byte-aligned with structured_rejections constants so the
# hook still emits the structured shape when the package is not importable.
_FALLBACK_SANCTIONED_RESOLVE_CALL = (
    'review_findings(review={"operation":"resolve", "all_open":true, '
    '"verification_evidence":<targeted test or diff evidence>})'
)
_FALLBACK_ORPHAN_DISPOSITION_CALL = (
    'review_findings(review={"operation":"disposition", "status":"fixed", '
    '"task_ref":<orphan-or-done-ref>})'
)
_FALLBACK_RULE_ID = "review_findings.use_resolve_not_update_fixed"

# Terminal finding statuses: update(status=fixed) is a no-op / already done.
_CLOSED_STATUSES = frozenset(
    {
        "fixed",
        "resolved_on_branch",
        "integrated",
        "deferred",
        "wontfix",
        "superseded",
    }
)
# Task statuses that have no live worktree write path — disposition only.
_ORPHAN_TASK_STATUSES = frozenset({"done", "archived", "abandoned", "cancelled"})


def _review_payload(tool_input: dict) -> dict | None:
    review = tool_input.get("review")
    return review if isinstance(review, dict) else None


def _lookup_finding_case(
    *,
    finding_id: str | None,
    task_ref: str | None,
) -> str:
    """Return resolve | disposition | already_closed | unknown for the attempted write.

    Best-effort DB lookup. Returns ``unknown`` when the package/runtime is not
    available or the row cannot be located — callers fall back to resolve.
    """
    if not finding_id:
        return "unknown"
    try:
        from workbay_handoff_mcp.shared_schema import (  # type: ignore[import-not-found]
            _get_db_connection,
        )
    except ImportError:
        return "unknown"

    try:
        with _get_db_connection() as conn:
            if task_ref:
                row = conn.execute(
                    """
                    SELECT f.status AS finding_status, h.status AS task_status
                    FROM review_findings f
                    LEFT JOIN handoff_state h ON h.task_ref = f.task_ref
                    WHERE f.finding_id = ? AND f.task_ref = ?
                    """,
                    (finding_id, task_ref),
                ).fetchone()
            else:
                # Deterministic ordering (S4-A-02): a finding_id reused across
                # task_refs must not depend on sqlite scan order — take the
                # most recently updated row, and prefer open rows within that
                # ordering. Callers that know the task_ref should pass it
                # (the explicit-task_ref branch above is always preferred).
                rows = conn.execute(
                    """
                    SELECT f.status AS finding_status, h.status AS task_status, f.task_ref AS task_ref
                    FROM review_findings f
                    LEFT JOIN handoff_state h ON h.task_ref = f.task_ref
                    WHERE f.finding_id = ?
                    ORDER BY f.updated_at DESC, f.id DESC
                    """,
                    (finding_id,),
                ).fetchall()
                if not rows:
                    return "unknown"
                if len(rows) > 1:
                    # Ambiguous without task_ref — prefer the latest-updated
                    # open row; else the latest-updated row overall.
                    open_rows = [r for r in rows if str(r["finding_status"]) == "open"]
                    row = open_rows[0] if open_rows else rows[0]
                else:
                    row = rows[0]

            if row is None:
                return "unknown"
            finding_status = str(row["finding_status"] or "")
            task_status = str(row["task_status"] or "") if row["task_status"] is not None else ""
            if finding_status in _CLOSED_STATUSES:
                return "already_closed"
            if task_status in _ORPHAN_TASK_STATUSES or task_status == "":
                # Empty task_status: finding row with no live handoff_state → orphan.
                if task_status in _ORPHAN_TASK_STATUSES or row["task_status"] is None:
                    return "disposition"
            if finding_status == "open":
                return "resolve"
            return "unknown"
    except Exception:  # noqa: BLE001 — hook must never crash the agent session
        return "unknown"


def _resolve_example(finding_id: str | None) -> str:
    if isinstance(finding_id, str) and finding_id:
        return (
            f'review_findings(review={{"operation":"resolve", "finding_ids":["{finding_id}"], '
            f'"verification_evidence":<targeted test or diff evidence>}})'
        )
    return _FALLBACK_SANCTIONED_RESOLVE_CALL


def _disposition_example(finding_id: str | None, task_ref: str | None) -> str:
    fid = finding_id if isinstance(finding_id, str) and finding_id else "<finding_id>"
    tref = task_ref if isinstance(task_ref, str) and task_ref else "<orphan-or-done-ref>"
    return (
        f'review_findings(review={{"operation":"disposition", "status":"fixed", '
        f'"task_ref":"{tref}", "finding_id":"{fid}", '
        f'"resolution_notes":<why fixed>, "verified_commit_sha":<optional descendant sha>}})'
    )


def _structured_case_rejection(
    review: dict,
    *,
    case: str,
) -> dict[str, Any]:
    """Return the structured rejection envelope for the resolved case."""

    finding_id = review.get("finding_id")
    task_ref = review.get("task_ref")
    fid = str(finding_id) if isinstance(finding_id, str) else None
    tref = str(task_ref) if isinstance(task_ref, str) else None

    try:
        from workbay_handoff_mcp.structured_rejections import (  # type: ignore[import-not-found]
            ORPHAN_DISPOSITION_CALL,
            SANCTIONED_RESOLVE_CALL,
            build_structured_rejection,
            rejection_use_resolve_not_update_fixed,
        )

        if case == "already_closed":
            example = (
                f'no-op: finding {fid or "<finding_id>"} is already terminal; '
                f'do not call update(status=fixed). list/get to confirm status.'
            )
            return build_structured_rejection(
                rule_id="review_findings.already_closed_noop",
                violated=(
                    "Direct review_findings(operation=update, status=fixed) on an "
                    "already-closed finding is unnecessary"
                ),
                expected="No write needed — the finding is already terminal.",
                example=example,
                error=(
                    f"Finding {fid or '<finding_id>'} is already closed; update(status=fixed) "
                    "is a no-op. Do not re-close; use list/get if you need the current status."
                ),
                finding_id=fid,
                task_ref=tref,
                case="already_closed",
            )
        if case == "disposition":
            example = _disposition_example(fid, tref)
            return build_structured_rejection(
                rule_id="review_findings.use_disposition_for_orphan",
                violated=(
                    "Direct review_findings(operation=update, status=fixed) is blocked for "
                    "orphan/done/archived task refs"
                ),
                expected=f"Use admin disposition: {ORPHAN_DISPOSITION_CALL}",
                example=example,
                error=(
                    "Use disposition for orphan or done-task refs instead of update(status=fixed): "
                    f"{example}"
                ),
                finding_id=fid,
                task_ref=tref,
                case="disposition",
            )
        # open+live or unknown → resolve (single working call for live rows)
        return rejection_use_resolve_not_update_fixed(finding_id=fid, task_ref=tref)
    except ImportError:
        if case == "already_closed":
            example = (
                f'no-op: finding {fid or "<finding_id>"} is already terminal; '
                f'do not call update(status=fixed). list/get to confirm status.'
            )
            return {
                "rule_id": "review_findings.already_closed_noop",
                "violated": (
                    "Direct review_findings(operation=update, status=fixed) on an "
                    "already-closed finding is unnecessary"
                ),
                "expected": "No write needed — the finding is already terminal.",
                "example": example,
                "error": (
                    f"Finding {fid or '<finding_id>'} is already closed; update(status=fixed) "
                    "is a no-op. Do not re-close; use list/get if you need the current status."
                ),
            }
        if case == "disposition":
            example = _disposition_example(fid, tref)
            return {
                "rule_id": "review_findings.use_disposition_for_orphan",
                "violated": (
                    "Direct review_findings(operation=update, status=fixed) is blocked for "
                    "orphan/done/archived task refs"
                ),
                "expected": f"Use admin disposition: {_FALLBACK_ORPHAN_DISPOSITION_CALL}",
                "example": example,
                "error": (
                    "Use disposition for orphan or done-task refs instead of update(status=fixed): "
                    f"{example}"
                ),
            }
        example = _resolve_example(fid)
        return {
            "rule_id": _FALLBACK_RULE_ID,
            "violated": (
                "Direct review_findings(operation=update, status=fixed) loses the "
                "commit-backed reconciliation path"
            ),
            "expected": f"Use the commit-backed finding resolver: {_FALLBACK_SANCTIONED_RESOLVE_CALL}.",
            "example": example,
            "error": (
                "Use the commit-backed finding resolver instead of update(status=fixed): "
                f"{example}"
            ),
        }


def _format_message(envelope: dict) -> str:
    try:
        from workbay_handoff_mcp.structured_rejections import (  # type: ignore[import-not-found]
            format_rejection_message,
        )

        return format_rejection_message(envelope)
    except ImportError:
        slim = {
            key: envelope[key]
            for key in ("violated", "expected", "example", "rule_id", "error")
            if key in envelope
        }
        return json.dumps(slim, sort_keys=True)


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, OSError):
        sys.exit(0)

    if isinstance(payload, dict):
        try:
            from _protocol import validate_event  # type: ignore[import-not-found]

            validate_event(payload, expected="PreToolUse")
        except ImportError:
            pass

    tool_name = str(payload.get("tool_name") or "")
    if "review_findings" not in tool_name:
        sys.exit(0)

    review = _review_payload(payload.get("tool_input") or {})
    if not review:
        sys.exit(0)

    if review.get("operation") == "update" and review.get("status") == "fixed":
        finding_id = review.get("finding_id")
        task_ref = review.get("task_ref")
        case = _lookup_finding_case(
            finding_id=str(finding_id) if isinstance(finding_id, str) else None,
            task_ref=str(task_ref) if isinstance(task_ref, str) else None,
        )
        envelope = _structured_case_rejection(review, case=case)
        # JSON structured envelope + prose error line so both machine parsers
        # and legacy substring checks (sanctioned call shape) succeed.
        print(_format_message(envelope), file=sys.stderr)
        error = envelope.get("error")
        if isinstance(error, str) and error:
            print(error, file=sys.stderr)
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
