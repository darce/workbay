"""Shared helpers for review-finding operations."""

from __future__ import annotations

import datetime as _dt
import json as _json
import re
import sqlite3
from pathlib import Path
from typing import cast

from .current_task_rendering import _write_current_task_md_for_task
from .shared_primitives import (
    ReviewFindingDetails,
    _envelope,
    _normalize_optional_text,
    _normalize_review_mode,
    _parse_sqlite_datetime,
    _resolve_workspace_handoff_row,
    _workspace_root,
)
from .shared_schema import _get_db_connection
from .shared_write_context import (
    _classify_commit_relation as _shared_classify_commit_relation,
)
from .shared_write_context import (
    _detect_git_write_context as _shared_detect_git_write_context,
)
from .shared_write_context import (
    _resolve_core_override,
)
from .slice_decision import is_canonical_decision

_AUTHOR_TAG_FALLBACK = "ahm"
_KNOWN_AUTHOR_TAGS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("claude", "code"), "cco"),
    (("claude", "opus"), "clo"),
    (("claude", "sonnet"), "cls"),
    (("claude", "haiku"), "clh"),
    (("codex",), "cdx"),
    (("gpt",), "gpt"),
)
_AUTHOR_TAG_RE = re.compile(r"^[a-z]{2,4}$")
_SLUG_NORMALIZE_RE = re.compile(r"[^a-z0-9_]+")


def _write_current_task_md_for_active_context(conn: sqlite3.Connection, fallback_task_ref: str) -> None:
    """Regenerate CURRENT_TASK.json for the active task when one exists."""

    try:
        active_row = _resolve_workspace_handoff_row(conn)
    except ValueError:
        active_row = None
    render_task_ref = (
        str(active_row["task_ref"]) if active_row is not None and active_row["task_ref"] else fallback_task_ref
    )
    _write_current_task_md_for_task(conn, render_task_ref)


def _current_task_revision(conn: sqlite3.Connection, task_ref: str) -> int | None:
    row = conn.execute(
        "SELECT revision FROM handoff_state WHERE task_ref = ?",
        (task_ref,),
    ).fetchone()
    return int(row["revision"]) if row is not None else None


def _current_task_revision_for(task_ref: str) -> int | None:
    with _get_db_connection() as conn:
        return _current_task_revision(conn, task_ref)


def _classify_commit_relation(reference_sha: str | None, candidate_sha: str | None) -> str:
    """Delegate to the shared implementation while honoring core monkeypatches."""

    classify_fn = _resolve_core_override("_classify_commit_relation", _shared_classify_commit_relation)
    result: str = classify_fn(reference_sha, candidate_sha)
    return result


def _detect_git_write_context() -> tuple[str | None, str | None]:
    """Delegate to the shared implementation while honoring core monkeypatches."""

    detect_fn = _resolve_core_override("_detect_git_write_context", _shared_detect_git_write_context)
    result: tuple[str | None, str | None] = detect_fn()
    return result


def _annotate_review_finding(
    row: dict[str, object],
    *,
    workspace_branch: str | None,
    workspace_commit_sha: str | None,
    include_closure_requirements: bool = False,
) -> dict[str, object]:
    finding = dict(row)
    finding_branch = _normalize_optional_text(finding.get("branch"))
    finding_commit_sha = _normalize_optional_text(finding.get("commit_sha"))
    branch_matches = None
    if finding_branch is not None and workspace_branch is not None:
        branch_matches = finding_branch == workspace_branch
    finding["workspace_branch"] = workspace_branch
    finding["workspace_commit_sha"] = workspace_commit_sha
    finding["workspace_branch_matches"] = branch_matches
    finding["workspace_commit_relation"] = _classify_commit_relation(finding_commit_sha, workspace_commit_sha)
    raw_merged_from = finding.pop("merged_from_json", None)
    if raw_merged_from:
        try:
            parsed = _json.loads(raw_merged_from) if isinstance(raw_merged_from, str) else None
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            finding["merged_from"] = parsed
    # internal: requirements-on-read — embed compact closure
    # preconditions so an agent reading a finding sees resolve/close needs
    # before the write (structured object, not prose). Embedded ONLY on the
    # single-finding read path ([RES-05]/[API-01]): it is actionable only when
    # acting on one finding, and per-row embedding on list/summary responses is
    # linear bloat.
    if include_closure_requirements:
        from .write_contracts import finding_closure_requirements  # noqa: PLC0415

        finding["closure_requirements"] = finding_closure_requirements(
            {
                "status": finding.get("status"),
                "workspace_commit_relation": finding.get("workspace_commit_relation"),
            }
        )
    return finding


def _validate_review_mode_or_envelope(
    review_mode: str | None, *, tool: str, entity: str = "finding"
) -> tuple[str | None, dict | None]:
    try:
        return _normalize_review_mode(review_mode), None
    except ValueError as exc:
        return None, _envelope(
            ok=False,
            tool=tool,
            data={"error": str(exc)},
            entity=entity,
        )


def _normalize_source_task_refs(raw: object) -> list[str]:
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        value = item.strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _auto_merge_session(target_task_ref: str) -> str:
    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"merge-{target_task_ref}-{ts}"


def cast_details(line_start: object, line_end: object, fix: object) -> ReviewFindingDetails:
    payload: dict[str, object] = {}
    if isinstance(line_start, int):
        payload["line_start"] = line_start
    if isinstance(line_end, int):
        payload["line_end"] = line_end
    if isinstance(fix, str) and fix:
        payload["fix"] = fix
    return cast("ReviewFindingDetails", payload)


def _short_author_tag(agent: str | None) -> str:
    normalized = (agent or "").strip().lower()
    if not normalized:
        return _AUTHOR_TAG_FALLBACK
    for needles, tag in _KNOWN_AUTHOR_TAGS:
        if all(needle in normalized for needle in needles):
            return tag
    first_token = next((token for token in re.split(r"\s+", normalized) if token), "")
    alpha_only = "".join(ch for ch in first_token if ch.isalpha())[:4]
    if _AUTHOR_TAG_RE.fullmatch(alpha_only):
        return alpha_only
    return _AUTHOR_TAG_FALLBACK


def _canonical_repair_provenance_decision_id(
    *,
    task_ref: str,
    finding_id: str,
    agent: str | None,
) -> str:
    author_tag = _short_author_tag(agent)
    slug = _SLUG_NORMALIZE_RE.sub("_", finding_id.lower()).strip("_")
    if not slug or not slug[0].isalnum():
        slug = f"f{slug}" if slug else "finding"
    candidate = f"{author_tag}_repair_provenance_{task_ref}_{slug}"
    if is_canonical_decision(candidate):
        return candidate
    safe_task_ref = re.sub(r"[^A-Za-z0-9_-]+", "-", task_ref).strip("-_") or "task"
    if not safe_task_ref[0].isalnum():
        safe_task_ref = f"t{safe_task_ref}"
    return f"{author_tag}_repair_provenance_{safe_task_ref}_{slug}"


def finding_file_modified_after(row: sqlite3.Row) -> dict[str, object] | None:
    raw_file_path = str(row["file_path"])
    path = Path(raw_file_path)
    if not path.is_absolute():
        path = _workspace_root() / raw_file_path
    if not path.exists():
        return None
    activity_dt = _parse_sqlite_datetime(row["updated_at"]) or _parse_sqlite_datetime(row["created_at"])
    if activity_dt is None:
        return None

    from datetime import UTC, datetime  # noqa: PLC0415

    file_modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    if file_modified_at <= activity_dt:
        return None
    return {
        "id": int(row["id"]),
        "finding_id": str(row["finding_id"]),
        "file_path": raw_file_path,
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]) if row["updated_at"] is not None else None,
        "file_modified_at": file_modified_at.strftime("%Y-%m-%d %H:%M:%S"),
    }
