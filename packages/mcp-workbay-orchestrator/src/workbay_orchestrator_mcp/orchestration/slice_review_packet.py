from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

from workbay_protocol import CONTRACTS_DIR

if TYPE_CHECKING:
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource

CONTRACT_PREFIXES = (
    f"{CONTRACTS_DIR}/",
    "packages/shared-contracts/",
)
PLANNING_ONLY_PREFIX = "docs/"
SLICE_DECISION_PREFIX = "slice_complete_"


class SliceReviewPacket(TypedDict):
    slice_label: str
    task_ref: str
    lane_id: str | None
    decision_id: int
    decision: str
    session: str
    branch: str | None
    commit_sha: str | None
    plan_item_id: str | None
    plan_cursor_id: int | None
    changed_files: list[str]
    external_changed_files: dict[str, list[str]]
    test_commands: list[str]
    contract_files: list[str]
    review_kind: ReviewKind
    review_guide_path: str
    scope_source: ReviewScopeSource
    rationale_excerpt: str | None
    created_at: str


_EXTERNAL_REPO_PREFIX_RE = re.compile(r"^([A-Za-z0-9._-]+):(.+)$")


def _partition_changed_files(
    raw: list[str],
) -> tuple[list[str], dict[str, list[str]]]:
    """Split decision ``changed_files`` into monorepo-relative and external.

    External entries use a ``<repo_alias>:<path>`` convention (e.g.
    ``mcp-workbay-bootstrap:src/foo.py``). The handoff packet contract
    requires ``changed_files`` to be monorepo-relative so reviewers running
    from the monorepo worktree can resolve them; external paths are
    surfaced separately under their alias with the prefix stripped.
    Order of paths within each bucket is preserved (internal).
    """
    monorepo: list[str] = []
    external: dict[str, list[str]] = {}
    for path in raw:
        match = _EXTERNAL_REPO_PREFIX_RE.match(path)
        if match is None:
            monorepo.append(path)
            continue
        alias, rel = match.group(1), match.group(2)
        # Defensive: a path that resembles "scheme:foo" without a "/" in the
        # tail is almost certainly a real prefixed external path; one with a
        # slash in the alias would have failed the regex above. Keep simple.
        external.setdefault(alias, []).append(rel)
    return monorepo, external


def _normalize_json_list(raw_value: Any) -> list[str]:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return []
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    result: list[str] = []
    for item in payload:
        if isinstance(item, str):
            normalized = item.strip()
            if normalized:
                result.append(normalized)
    return result


def _excerpt_text(value: str | None, *, limit: int = 240) -> str | None:
    if not isinstance(value, str):
        return None
    collapsed = " ".join(value.split())
    if not collapsed:
        return None
    if len(collapsed) <= limit:
        return collapsed
    return f"{collapsed[: limit - 3].rstrip()}..."


def _extract_changed_files_from_rationale(rationale: str | None) -> list[str]:
    if not isinstance(rationale, str) or not rationale.strip():
        return []

    in_changes = False
    changed_files: list[str] = []
    seen: set[str] = set()
    for raw_line in rationale.splitlines():
        line = raw_line.strip()
        if line == "## Changes":
            in_changes = True
            continue
        if in_changes and line.startswith("## "):
            break
        if not in_changes or not line.startswith("- "):
            continue

        candidate = line[2:].strip()
        for separator in (":", ";", " "):
            if separator in candidate:
                candidate = candidate.split(separator, 1)[0].strip()
                break
        if "/" not in candidate:
            continue
        if candidate not in seen:
            seen.add(candidate)
            changed_files.append(candidate)
    return changed_files


def derive_review_kind(changed_files: list[str]) -> ReviewKind:
    from workbay_handoff_mcp.enums import ReviewKind  # noqa: PLC0415

    if changed_files and all(path.startswith(PLANNING_ONLY_PREFIX) for path in changed_files):
        return ReviewKind.PLANNING
    return ReviewKind.BRANCH


def _review_guide_path(workspace_root: Path, review_kind: ReviewKind) -> str:
    from workbay_handoff_mcp.enums import ReviewKind  # noqa: PLC0415

    from workbay_orchestrator_mcp._assets import bundled_rules_dir  # noqa: PLC0415

    guide_name = "planning-review-guide.md" if review_kind == ReviewKind.PLANNING else "branch-review-guide.md"
    return str(bundled_rules_dir() / guide_name)


def _matching_plan_cursor(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str | None,
    decision_created_at: str,
) -> dict[str, Any] | None:
    params: list[Any] = [task_ref]
    where_sql = "task_ref = ?"
    if lane_id is not None:
        where_sql += " AND lane_id = ?"
        params.append(lane_id)
    rows = conn.execute(
        f"""
        SELECT *
        FROM plan_cursors
        WHERE {where_sql}
          AND datetime(COALESCE(completed_at, updated_at, created_at)) <= datetime(?)
        ORDER BY datetime(COALESCE(completed_at, updated_at, created_at)) DESC, id DESC
        LIMIT 1
        """,
        (*params, decision_created_at),
    ).fetchall()
    return dict(rows[0]) if rows else None


def _matching_worker_report(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str | None,
    branch: str | None,
    commit_sha: str | None,
    decision_created_at: str,
) -> dict[str, Any] | None:
    params: list[Any] = [task_ref]
    where_sql = "task_ref = ?"
    if lane_id is not None:
        where_sql += " AND lane_id = ?"
        params.append(lane_id)
    rows = conn.execute(
        f"""
        SELECT *
        FROM worker_reports
        WHERE {where_sql}
          AND datetime(created_at) <= datetime(?)
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT 20
        """,
        (*params, decision_created_at),
    ).fetchall()
    if not rows:
        return None

    best_row: sqlite3.Row | None = None
    best_score = -1
    for row in rows:
        score = 0
        if commit_sha and row["commit_sha"] == commit_sha:
            score += 4
        if branch and row["branch"] == branch:
            score += 2
        if score > best_score:
            best_row = row
            best_score = score
    return dict(best_row) if best_row is not None else None


def _matching_test_commands(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str | None,
    commit_sha: str | None,
    decision_created_at: str,
) -> list[str]:
    params: list[Any] = [task_ref]
    where_sql = "task_ref = ?"
    if lane_id is not None:
        where_sql += " AND lane_id = ?"
        params.append(lane_id)
    if commit_sha is not None:
        where_sql += " AND commit_sha = ?"
        params.append(commit_sha)
    rows = conn.execute(
        f"""
        SELECT command
        FROM verified_tests
        WHERE {where_sql}
          AND datetime(verified_at) <= datetime(?)
        ORDER BY datetime(verified_at) DESC, id DESC
        LIMIT 20
        """,
        (*params, decision_created_at),
    ).fetchall()
    commands: list[str] = []
    seen: set[str] = set()
    for row in rows:
        command = str(row["command"]).strip()
        if command and command not in seen:
            seen.add(command)
            commands.append(command)
    return commands


def _build_packet_for_decision(
    conn: sqlite3.Connection,
    *,
    workspace_root: Path,
    task_ref: str,
    decision_row: dict[str, Any],
) -> SliceReviewPacket:
    from workbay_handoff_mcp.enums import ReviewScopeSource  # noqa: PLC0415
    from workbay_handoff_mcp.slice_decision import extract_slice_label  # noqa: PLC0415

    matched_report = _matching_worker_report(
        conn,
        task_ref=task_ref,
        lane_id=decision_row.get("lane_id"),
        branch=decision_row.get("branch"),
        commit_sha=decision_row.get("commit_sha"),
        decision_created_at=str(decision_row["created_at"]),
    )
    changed_files = _normalize_json_list(decision_row.get("changed_files_json"))
    if not changed_files:
        changed_files = _normalize_json_list(matched_report.get("changed_files_json") if matched_report else None)
    if not changed_files:
        changed_files = _extract_changed_files_from_rationale(decision_row.get("rationale"))

    changed_files, external_changed_files = _partition_changed_files(changed_files)

    test_commands = _normalize_json_list(matched_report.get("test_commands_json") if matched_report else None)
    if not test_commands:
        test_commands = _matching_test_commands(
            conn,
            task_ref=task_ref,
            lane_id=decision_row.get("lane_id"),
            commit_sha=decision_row.get("commit_sha"),
            decision_created_at=str(decision_row["created_at"]),
        )
    plan_cursor = _matching_plan_cursor(
        conn,
        task_ref=task_ref,
        lane_id=decision_row.get("lane_id"),
        decision_created_at=str(decision_row["created_at"]),
    )
    review_kind = derive_review_kind(changed_files)

    return {
        "slice_label": extract_slice_label(str(decision_row["decision"])),
        "task_ref": task_ref,
        "lane_id": decision_row.get("lane_id"),
        "decision_id": int(decision_row["id"]),
        "decision": str(decision_row["decision"]),
        "session": str(decision_row["session"]),
        "branch": decision_row.get("branch"),
        "commit_sha": decision_row.get("commit_sha"),
        "plan_item_id": plan_cursor.get("plan_item_id") if plan_cursor else None,
        "plan_cursor_id": int(plan_cursor["id"]) if plan_cursor else None,
        "changed_files": changed_files,
        "external_changed_files": external_changed_files,
        "test_commands": test_commands,
        "contract_files": [path for path in changed_files if path.startswith(CONTRACT_PREFIXES)],
        "review_kind": review_kind,
        "review_guide_path": _review_guide_path(workspace_root, review_kind),
        "scope_source": ReviewScopeSource.SLICE_PACKET,
        "rationale_excerpt": _excerpt_text(decision_row.get("rationale")),
        "created_at": str(decision_row["created_at"]),
    }


def _slice_complete_decision_rows(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    where_sql = "task_ref = ? AND (decision LIKE ? OR decision LIKE ?)"
    params: list[Any] = [task_ref, f"{SLICE_DECISION_PREFIX}%", "%_slice_complete_%"]
    if lane_id is not None:
        where_sql += " AND lane_id = ?"
        params.append(lane_id)
    rows = conn.execute(
        f"""
        SELECT *
        FROM decisions
        WHERE {where_sql}
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _resolve_slice_complete_decision_row(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str | None,
    slice_decision_id: str | None,
    slice_label: str | None,
) -> dict[str, Any] | None:
    from workbay_handoff_mcp.slice_decision import extract_slice_label, is_slice_complete_decision  # noqa: PLC0415

    if slice_decision_id is not None:
        params: list[Any] = [task_ref, slice_decision_id]
        where_sql = "task_ref = ? AND decision = ?"
        if lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(lane_id)
        row = conn.execute(
            f"SELECT * FROM decisions WHERE {where_sql} LIMIT 1",
            tuple(params),
        ).fetchone()
        if row is None:
            return None
        decision_row = dict(row)
        if not is_slice_complete_decision(str(decision_row["decision"])):
            return None
        return decision_row

    if slice_label is not None:
        for decision_row in _slice_complete_decision_rows(conn, task_ref=task_ref, lane_id=lane_id):
            if not is_slice_complete_decision(str(decision_row["decision"])):
                continue
            if extract_slice_label(str(decision_row["decision"])) == slice_label:
                return decision_row
        return None

    return None


def get_latest_slice_review_packet_data(
    conn: sqlite3.Connection,
    *,
    workspace_root: Path,
    task_ref: str,
    lane_id: str | None = None,
    review_kind: ReviewKind | str | None = None,
    slice_decision_id: str | None = None,
    slice_label: str | None = None,
) -> SliceReviewPacket | None:
    """Resolve a single slice review packet, or ``None`` when nothing matches.

    ``slice_decision_id`` matches the ``decision`` id **string**, not the numeric
    ``decision_id`` exposed on the packet. When both ``slice_decision_id`` and
    ``slice_label`` are supplied, ``slice_decision_id`` takes precedence
    (``slice_label`` is ignored); the MCP wrapper rejects passing both up front,
    so direct callers should likewise supply only one.
    """
    from workbay_handoff_mcp.enums import ReviewKind  # noqa: PLC0415
    from workbay_handoff_mcp.slice_decision import is_slice_complete_decision  # noqa: PLC0415

    normalized_review_kind = ReviewKind(review_kind) if review_kind is not None else None

    selected_row = _resolve_slice_complete_decision_row(
        conn,
        task_ref=task_ref,
        lane_id=lane_id,
        slice_decision_id=slice_decision_id,
        slice_label=slice_label,
    )
    if selected_row is not None:
        packet = _build_packet_for_decision(
            conn,
            workspace_root=workspace_root,
            task_ref=task_ref,
            decision_row=selected_row,
        )
        if normalized_review_kind is not None and packet["review_kind"] != normalized_review_kind:
            return None
        return packet

    if slice_decision_id is not None or slice_label is not None:
        return None

    for decision_row in _slice_complete_decision_rows(conn, task_ref=task_ref, lane_id=lane_id):
        if not is_slice_complete_decision(str(decision_row["decision"])):
            continue
        packet = _build_packet_for_decision(
            conn,
            workspace_root=workspace_root,
            task_ref=task_ref,
            decision_row=decision_row,
        )
        if normalized_review_kind is not None and packet["review_kind"] != normalized_review_kind:
            continue
        return packet

    return None
