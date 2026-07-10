"""Structured write-rejection envelopes (internal).

Every sanctioned-write rejection emits a machine-actionable object::

    {violated, expected, example, rule_id}

- **violated** — what rule / guard fired (stable prose for humans + telemetry).
- **expected** — the grammar / precondition the caller must satisfy.
- **example** — a filled sanctioned call the caller can copy (caller values
  substituted when known).
- **rule_id** — stable telemetry id; must not drift across runs.

Named classes cover the live spool taxonomy; :func:`wrap_unclassified_rejection`
is the registry-wide default so residual rejections also get the shape.
The PreToolUse hook reuses the same builders ([REF-19] single source).
"""

from __future__ import annotations

from typing import Any, Mapping

from .shared_primitives import (
    BATCH_CLOSE_THRESHOLD,
    BATCH_CLOSE_WINDOW_SECONDS,
    MAX_RESOLUTION_NOTES_LENGTH,
)

# ---------------------------------------------------------------------------
# Stable rule ids (telemetry). Do not rename without a migration note.
# ---------------------------------------------------------------------------

RULE_CLOSE_SLICE_RATIONALE_XML = "close_slice.rationale_xml_anti_patterns"
RULE_BATCH_CLOSE_EVIDENCE = "review_findings.batch_close_evidence"
RULE_SUPERSEDED_MERGE_MANAGED = "review_findings.superseded_is_merge_managed"
RULE_RESOLUTION_NOTES_MAX_LENGTH = "review_findings.resolution_notes_max_length"
RULE_COMMIT_ANCESTRY = "review_findings.commit_ancestry"
RULE_USE_RESOLVE_NOT_UPDATE_FIXED = "review_findings.use_resolve_not_update_fixed"
RULE_UNCLASSIFIED_PREFIX = "write.unclassified"

STRUCTURED_REJECTION_KEYS: tuple[str, ...] = ("violated", "expected", "example", "rule_id")

# Shared sanctioned-call strings — single source for API guards + PreToolUse hook.
SANCTIONED_RESOLVE_CALL = (
    'review_findings(review={"operation":"resolve", "all_open":true, '
    '"verification_evidence":<targeted test or diff evidence>})'
)
ORPHAN_DISPOSITION_CALL = (
    'review_findings(review={"operation":"disposition", "status":"fixed", "task_ref":<orphan-or-done-ref>})'
)
# Open-preserving path rewrite (implementation note): when the finding is still live but
# its file moved (e.g. a rebrand rename), re-anchor the path instead of closing.
SANCTIONED_REANCHOR_CALL = (
    'review_findings(review={"operation":"reanchor", "task_ref":<task_ref>, '
    '"finding_id":<finding_id>, "file_path":<corrected live path>})'
)


def build_structured_rejection(
    *,
    rule_id: str,
    violated: str,
    expected: str,
    example: Any,
    error: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build the canonical structured rejection payload (plus optional extras).

    ``error`` is a backward-compatible prose summary (self-capture / older
    callers). When omitted it is derived from ``violated`` + ``expected``.
    Extra keys (``false_fix_guard``, ``rejected_tag``, …) are preserved.
    """

    summary = error if error is not None else f"{violated}. {expected}"
    payload: dict[str, Any] = {
        "rule_id": rule_id,
        "violated": violated,
        "expected": expected,
        "example": example,
        "error": summary,
    }
    for key, value in extra.items():
        if value is not None:
            payload[key] = value
    return payload


def has_structured_rejection(data: Mapping[str, Any] | None) -> bool:
    """True when ``data`` already carries the four structured fields."""

    if not isinstance(data, Mapping):
        return False
    return all(key in data for key in STRUCTURED_REJECTION_KEYS)


def wrap_unclassified_rejection(
    *,
    tool_name: str,
    error: str,
    example: Any | None = None,
    rule_id: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Default envelope for residual / unclassified write rejections.

    Registry-wide wrapper path: every ok:false write that is not one of the
    named classes still emits ``{violated, expected, example, rule_id}``.
    ``rule_id`` is stable per tool (``write.unclassified.<tool_name>``).
    """

    stable_id = rule_id or f"{RULE_UNCLASSIFIED_PREFIX}.{tool_name}"
    filled_example = (
        example
        if example is not None
        else {
            "tool": tool_name,
            "hint": "Consult limits.write / WriteContract.examples for a sanctioned payload shape.",
        }
    )
    return build_structured_rejection(
        rule_id=stable_id,
        violated=error,
        expected=f"Satisfy the write contract for tool {tool_name!r}",
        example=filled_example,
        error=error,
        **extra,
    )


def ensure_structured_rejection_data(
    tool_name: str,
    data: Mapping[str, Any] | None,
    *,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Ensure ``data`` has the structured envelope (registry-wide default).

    Named classes already populate the four fields; this path only wraps
    residual rejections that still carry bare ``error`` / ``errors`` prose.
    """

    base: dict[str, Any] = dict(data) if isinstance(data, Mapping) else {}
    if has_structured_rejection(base):
        return base

    summary: str | None = None
    raw_error = base.get("error")
    if isinstance(raw_error, str) and raw_error.strip():
        summary = raw_error.strip()
    elif isinstance(base.get("errors"), list) and base["errors"]:
        summary = "; ".join(str(item) for item in base["errors"][:5])
    elif isinstance(base.get("state_error"), str) and str(base["state_error"]).strip():
        summary = str(base["state_error"]).strip()
    if not summary:
        summary = f"{tool_name} write rejected"

    example: Any | None = None
    if payload is not None:
        # Prefer a minimal echo of the caller's own keys as a starting template.
        example = {
            "tool": tool_name,
            "received_keys": sorted(str(k) for k in payload.keys()),
            "hint": "Consult limits.write / WriteContract.examples for a sanctioned payload shape.",
        }

    wrapped = wrap_unclassified_rejection(
        tool_name=tool_name,
        error=summary,
        example=example,
    )
    # Preserve any pre-existing diagnostic fields (guards, tags, …).
    for key, value in base.items():
        if key not in wrapped:
            wrapped[key] = value
    return wrapped


# ---------------------------------------------------------------------------
# Named rejection classes (five live spool classes + hook redirect)
# ---------------------------------------------------------------------------


def rejection_close_slice_rationale_xml(
    *,
    rejected_tag: str,
    task_ref: str | None = None,
    rationale_preview: str | None = None,
) -> dict[str, Any]:
    """close_slice XML-tag prohibition (``<actor>`` / ``<changed_files>``)."""

    example_parts = {
        "tool": "close_slice",
        "task_ref": task_ref or "<task_ref>",
        "author_tag": "<author_tag>",
        "work_ref": "<work_ref>",
        "slug": "<slug>",
        "session": "<session>",
        "expected_revision": "<int>",
        "rationale": (
            "## Changes\n- …\n\n## Verification\n- …\n\n## Schema / Contract Changes\n- …\n\n## Open Threads\n- …"
        ),
        "actor": {"agent": "<agent>"},
        "changed_files": ["<path>"],
    }
    return build_structured_rejection(
        rule_id=RULE_CLOSE_SLICE_RATIONALE_XML,
        violated=(
            f"rationale contains the XML-like tag `{rejected_tag}` which indicates "
            f"the `actor` or `changed_files` parameters were accidentally embedded "
            f"inside the rationale string instead of being passed as separate "
            f"top-level JSON fields"
        ),
        expected=(
            "Remove XML tags from rationale; pass actor={...} and changed_files=[...] "
            "as separate top-level close_slice parameters"
        ),
        example=example_parts,
        error=(
            f"rationale contains the XML-like tag `{rejected_tag}` which indicates "
            f"the `actor` or `changed_files` parameters were accidentally "
            f"embedded inside the rationale string instead of being passed "
            f"as separate top-level JSON fields. Remove the XML tags from "
            f"the rationale and pass actor={{...}} and changed_files=[...] "
            f"as separate parameters to close_slice."
        ),
        rejected_tag=rejected_tag,
        rationale_preview=rationale_preview,
    )


def rejection_batch_close_evidence(
    *,
    finding_id: str | None = None,
    recent_fixes_in_window: int,
    window_seconds: int = BATCH_CLOSE_WINDOW_SECONDS,
    threshold: int = BATCH_CLOSE_THRESHOLD,
    additional_closing: int | None = None,
) -> dict[str, Any]:
    """Batch-close guard: verification_evidence required above threshold."""

    fid = finding_id or "<finding_id>"
    if additional_closing is not None:
        violated = (
            f"Batch-close guard would reject this resolve batch without verification_evidence: "
            f"{recent_fixes_in_window} other findings were marked fixed in the last "
            f"{window_seconds}s, and this request would close {additional_closing} more"
        )
        error = f"{violated}. Provide verification_evidence or resolve fewer findings via {SANCTIONED_RESOLVE_CALL}."
        example = SANCTIONED_RESOLVE_CALL
    else:
        violated = (
            f"Batch-close guard: {recent_fixes_in_window} other findings were marked fixed "
            f"in the last {window_seconds}s for this task"
        )
        error = (
            f"{violated}. Provide verification_evidence (code snippets, grep output, or "
            f"diff proving the fix exists) to confirm each closure is individually verified. "
            f"Sanctioned call: {SANCTIONED_RESOLVE_CALL}."
        )
        example = (
            f'review_findings(review={{"operation":"update", "finding_id":"{fid}", '
            f'"status":"fixed", "verification_evidence":'
            f'"<code snippet / grep / diff proving the fix>"}})'
        )
    return build_structured_rejection(
        rule_id=RULE_BATCH_CLOSE_EVIDENCE,
        violated=violated,
        expected=(
            f"Provide verification_evidence when >= {threshold} other findings were marked "
            f"fixed in the last {window_seconds}s for this task"
        ),
        example=example,
        error=error,
        false_fix_guard={
            "finding_id": fid,
            "recent_fixes_in_window": recent_fixes_in_window,
            "window_seconds": window_seconds,
            "threshold": threshold,
            "guard": "batch_close",
            **({"additional_closing": additional_closing} if additional_closing is not None else {}),
        },
    )


def rejection_superseded_merge_managed() -> dict[str, Any]:
    """Direct status='superseded' is merge-managed."""

    example = (
        'review_findings(review={"operation":"merge", "source_task_refs":["<src>"], '
        '"target_task_ref":"<target>", "retire_sources":true})'
    )
    return build_structured_rejection(
        rule_id=RULE_SUPERSEDED_MERGE_MANAGED,
        violated="status='superseded' is merge-managed and rejected on direct write",
        expected="use review_findings(operation='merge') with retire_sources",
        example=example,
        error=("status='superseded' is merge-managed; use review_findings(operation='merge') with retire_sources."),
    )


def rejection_resolution_notes_max_length(
    *,
    actual_length: int,
    max_length: int = MAX_RESOLUTION_NOTES_LENGTH,
    finding_id: str | None = None,
) -> dict[str, Any]:
    """resolution_notes exceeds MAX_RESOLUTION_NOTES_LENGTH."""

    fid = finding_id or "<finding_id>"
    example = (
        f'review_findings(review={{"operation":"update", "finding_id":"{fid}", '
        f'"status":"deferred", "resolution_notes":"<notes <= {max_length} chars>"}})'
    )
    return build_structured_rejection(
        rule_id=RULE_RESOLUTION_NOTES_MAX_LENGTH,
        violated=f"resolution_notes length {actual_length} exceeds limit {max_length}",
        expected=f"resolution_notes must be <= {max_length} characters",
        example=example,
        error=f"resolution_notes must be <= {max_length} characters.",
    )


def rejection_commit_ancestry(
    *,
    relation: str,
    finding_commit_sha: str | None = None,
    current_commit_sha: str | None = None,
    current_branch: str | None = None,
    verified_commit_sha: str | None = None,
    finding_id: str | None = None,
) -> dict[str, Any]:
    """Finding can only be fixed from same commit or a newer descendant."""

    fid = finding_id or "<finding_id>"
    example = (
        f'review_findings(review={{"operation":"resolve", "finding_ids":["{fid}"], '
        f'"verification_evidence":"<evidence after committing the fix on a descendant>"}})'
    )
    return build_structured_rejection(
        rule_id=RULE_COMMIT_ANCESTRY,
        violated=(
            f"Workspace commit relation is {relation!r}; a finding can only be marked "
            f"fixed from the same commit or a newer descendant commit"
        ),
        expected="same_or_descendant commit relation (workspace HEAD is finding commit or a descendant)",
        example=example,
        error="A finding can only be marked fixed from the same commit or a newer descendant commit.",
        commit_guard={
            "finding_commit_sha": finding_commit_sha,
            "current_commit_sha": current_commit_sha,
            "current_branch": current_branch,
            "verified_commit_sha": verified_commit_sha,
            "relation": relation,
        },
    )


def rejection_use_resolve_not_update_fixed(
    *,
    finding_id: str | None = None,
    task_ref: str | None = None,
) -> dict[str, Any]:
    """PreToolUse redirect: update(status=fixed) → resolve (hook + shared shape)."""

    if finding_id:
        example = (
            f'review_findings(review={{"operation":"resolve", "finding_ids":["{finding_id}"], '
            f'"verification_evidence":<targeted test or diff evidence>}})'
        )
    else:
        example = SANCTIONED_RESOLVE_CALL
    return build_structured_rejection(
        rule_id=RULE_USE_RESOLVE_NOT_UPDATE_FIXED,
        violated=("Direct review_findings(operation=update, status=fixed) loses the commit-backed reconciliation path"),
        expected=(
            f"Use the commit-backed finding resolver: {SANCTIONED_RESOLVE_CALL}. "
            f"For orphan or done-task refs use {ORPHAN_DISPOSITION_CALL}. "
            f"If the finding is still live but its file was renamed/moved, re-anchor "
            f"the path (open-preserving) instead of closing: {SANCTIONED_REANCHOR_CALL}."
        ),
        example=example,
        error=(
            "Use the commit-backed finding resolver instead of update(status=fixed): "
            f"{SANCTIONED_RESOLVE_CALL}. For orphan or done-task refs use {ORPHAN_DISPOSITION_CALL}. "
            f"If the finding is still live but its file was renamed/moved, re-anchor the "
            f"path (open-preserving) instead of closing: {SANCTIONED_REANCHOR_CALL}."
        ),
        finding_id=finding_id,
        task_ref=task_ref,
    )


def format_rejection_message(envelope: Mapping[str, Any]) -> str:
    """Serialize a structured rejection for hook stderr (JSON, single source)."""

    import json

    slim = {key: envelope[key] for key in STRUCTURED_REJECTION_KEYS if key in envelope}
    # Include error prose so legacy substring checks (sanctioned call shape) still match.
    if "error" in envelope:
        slim = {**slim, "error": envelope["error"]}
    return json.dumps(slim, sort_keys=True)
