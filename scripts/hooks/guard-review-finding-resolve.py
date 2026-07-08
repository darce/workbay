#!/usr/bin/env python3
"""PreToolUse hook: redirect review finding closures to resolve.

Direct ``review_findings(operation="update", status="fixed")`` loses the
commit-backed reconciliation path. Block it once with the sanctioned call.
"""

from __future__ import annotations

import json
import sys

SANCTIONED_RESOLVE_CALL = (
    'review_findings(review={"operation":"resolve", "all_open":true, '
    '"verification_evidence":<targeted test or diff evidence>})'
)
ORPHAN_DISPOSITION_CALL = (
    'review_findings(review={"operation":"disposition", "status":"fixed", '
    '"task_ref":<orphan-or-done-ref>})'
)


def _review_payload(tool_input: dict) -> dict | None:
    review = tool_input.get("review")
    return review if isinstance(review, dict) else None


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
        print(
            "Use the commit-backed finding resolver instead of update(status=fixed): "
            f"{SANCTIONED_RESOLVE_CALL}. For orphan or done-task refs use {ORPHAN_DISPOSITION_CALL}.",
            file=sys.stderr,
        )
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
