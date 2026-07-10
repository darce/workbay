#!/usr/bin/env python3
"""PreToolUse hook: redirect review finding closures to resolve.

Direct ``review_findings(operation="update", status="fixed")`` loses the
commit-backed reconciliation path. Block it once with the structured
rejection envelope from ``workbay_handoff_mcp.structured_rejections``
([REF-19] single source — no second phrasing to drift).
"""

from __future__ import annotations

import json
import sys

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


def _review_payload(tool_input: dict) -> dict | None:
    review = tool_input.get("review")
    return review if isinstance(review, dict) else None


def _structured_update_fixed_rejection(review: dict) -> dict:
    """Return the structured rejection envelope (package builder preferred)."""

    finding_id = review.get("finding_id")
    task_ref = review.get("task_ref")
    try:
        from workbay_handoff_mcp.structured_rejections import (  # type: ignore[import-not-found]
            rejection_use_resolve_not_update_fixed,
        )

        return rejection_use_resolve_not_update_fixed(
            finding_id=str(finding_id) if isinstance(finding_id, str) else None,
            task_ref=str(task_ref) if isinstance(task_ref, str) else None,
        )
    except ImportError:
        sanctioned = _FALLBACK_SANCTIONED_RESOLVE_CALL
        orphan = _FALLBACK_ORPHAN_DISPOSITION_CALL
        example = sanctioned
        if isinstance(finding_id, str) and finding_id:
            example = (
                f'review_findings(review={{"operation":"resolve", "finding_ids":["{finding_id}"], '
                f'"verification_evidence":<targeted test or diff evidence>}})'
            )
        return {
            "rule_id": _FALLBACK_RULE_ID,
            "violated": (
                "Direct review_findings(operation=update, status=fixed) loses the "
                "commit-backed reconciliation path"
            ),
            "expected": (
                f"Use the commit-backed finding resolver: {sanctioned}. "
                f"For orphan or done-task refs use {orphan}."
            ),
            "example": example,
            "error": (
                "Use the commit-backed finding resolver instead of update(status=fixed): "
                f"{sanctioned}. For orphan or done-task refs use {orphan}."
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
        envelope = _structured_update_fixed_rejection(review)
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
