"""Verified test read surfaces."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import PurePosixPath

from .shared_primitives import _envelope, _resolve_task_ref, _row_to_dict
from .shared_schema import _get_db_connection


def _normalize_correlated_file(path: str | None) -> tuple[str | None, str | None]:
    if path is None:
        return None, None
    candidate = path.strip().replace("\\", "/")
    pure_path = PurePosixPath(candidate)
    if not candidate or pure_path.is_absolute() or not pure_path.parts or any(part == ".." for part in pure_path.parts):
        return None, "correlated_file must be a non-empty monorepo-relative path."
    normalized = "/".join(part for part in pure_path.parts if part not in ("", "."))
    return normalized, None


def _load_trace_map(conn: sqlite3.Connection, test_ids: list[int]) -> dict[int, list[str]]:
    if not test_ids:
        return {}
    placeholders = ",".join("?" for _ in test_ids)
    rows = conn.execute(
        f"""
        SELECT verified_test_id, trace
        FROM test_traces
        WHERE verified_test_id IN ({placeholders})
        ORDER BY verified_test_id ASC, trace_order ASC, id ASC
        """,
        tuple(test_ids),
    ).fetchall()
    trace_map: dict[int, list[str]] = {}
    for row in rows:
        trace_map.setdefault(int(row["verified_test_id"]), []).append(str(row["trace"]))
    return trace_map


def _load_trace_counts(conn: sqlite3.Connection, test_ids: list[int]) -> dict[int, int]:
    if not test_ids:
        return {}
    placeholders = ",".join("?" for _ in test_ids)
    rows = conn.execute(
        f"""
        SELECT verified_test_id, COUNT(*) AS trace_count
        FROM test_traces
        WHERE verified_test_id IN ({placeholders})
        GROUP BY verified_test_id
        """,
        tuple(test_ids),
    ).fetchall()
    return {int(row["verified_test_id"]): int(row["trace_count"]) for row in rows}


def _parse_changed_files_json(raw_value: object) -> set[str]:
    if not isinstance(raw_value, str) or raw_value.strip() == "":
        return set()
    try:
        decoded = json.loads(raw_value)
    except ValueError:
        return set()
    if not isinstance(decoded, list):
        return set()
    return {item for item in decoded if isinstance(item, str) and item}


def _parse_sqlite_timestamp(raw_value: object) -> datetime | None:
    if not isinstance(raw_value, str) or raw_value.strip() == "":
        return None
    normalized = raw_value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return datetime.strptime(raw_value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _correlated_test_ids_for_file(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    correlated_file: str,
    correlation_window_minutes: int,
) -> set[int]:
    decision_rows = conn.execute(
        """
        SELECT commit_sha, created_at, changed_files_json
        FROM decisions
        WHERE task_ref = ?
        ORDER BY created_at DESC, id DESC
        """,
        (task_ref,),
    ).fetchall()
    matching_commit_shas: set[str] = set()
    decision_times: list[datetime] = []
    for row in decision_rows:
        if correlated_file not in _parse_changed_files_json(row["changed_files_json"]):
            continue
        if isinstance(row["commit_sha"], str) and row["commit_sha"].strip():
            matching_commit_shas.add(str(row["commit_sha"]))
        parsed_time = _parse_sqlite_timestamp(row["created_at"])
        if parsed_time is not None:
            decision_times.append(parsed_time)

    if not matching_commit_shas and not decision_times:
        return set()

    matched_test_ids: set[int] = set()
    test_rows = conn.execute(
        """
        SELECT id, commit_sha, verified_at
        FROM verified_tests
        WHERE task_ref = ?
        """,
        (task_ref,),
    ).fetchall()
    correlation_window_seconds = max(0, int(correlation_window_minutes)) * 60
    for row in test_rows:
        commit_sha = str(row["commit_sha"]) if row["commit_sha"] is not None else None
        if matching_commit_shas:
            if commit_sha:
                if commit_sha in matching_commit_shas:
                    matched_test_ids.add(int(row["id"]))
                continue
        verified_at = _parse_sqlite_timestamp(row["verified_at"])
        if verified_at is None:
            continue
        if any(
            abs((verified_at - decision_time).total_seconds()) <= correlation_window_seconds
            for decision_time in decision_times
        ):
            matched_test_ids.add(int(row["id"]))
    return matched_test_ids


def get_verified_tests(
    task_ref: str | None = None,
    lane_id: str | None = None,
    branch: str | None = None,
    commit_sha: str | None = None,
    passed: bool | None = None,
    include_traces: bool = False,
    correlated_file: str | None = None,
    correlation_window_minutes: int = 120,
    exclude_never_passed: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """List verified test rows with bounded filters and deterministic ordering."""

    clamped_limit = max(1, min(int(limit), 200))
    clamped_offset = max(0, int(offset))
    clamped_window = max(0, int(correlation_window_minutes))
    normalized_correlated_file, path_error = _normalize_correlated_file(correlated_file)
    if path_error is not None:
        return _envelope(ok=False, tool="get_verified_tests", data={"error": path_error})

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        where_parts = ["task_ref = ?"]
        params: list[object] = [resolved_task_ref]

        if lane_id is not None:
            where_parts.append("lane_id = ?")
            params.append(lane_id)
        if branch is not None:
            where_parts.append("branch = ?")
            params.append(branch)
        if commit_sha is not None:
            where_parts.append("commit_sha = ?")
            params.append(commit_sha)
        if passed is not None:
            where_parts.append("passed = ?")
            params.append(1 if passed else 0)

        where_sql = " AND ".join(where_parts)
        rows = list(
            conn.execute(
                f"""
                SELECT *
                FROM verified_tests
                WHERE {where_sql}
                ORDER BY verified_at DESC, id DESC
                """,
                tuple(params),
            ).fetchall()
        )

        if normalized_correlated_file is not None:
            correlated_ids = _correlated_test_ids_for_file(
                conn,
                task_ref=resolved_task_ref,
                correlated_file=normalized_correlated_file,
                correlation_window_minutes=clamped_window,
            )
            rows = [row for row in rows if int(row["id"]) in correlated_ids]

        if exclude_never_passed:
            commands_with_pass = {str(row["command"]) for row in rows if bool(row["passed"])}
            rows = [row for row in rows if str(row["command"]) in commands_with_pass]

        total = len(rows)
        paged_rows = rows[clamped_offset : clamped_offset + clamped_limit]
        paged_ids = [int(row["id"]) for row in paged_rows]
        trace_map = _load_trace_map(conn, paged_ids) if include_traces else {}
        trace_counts = _load_trace_counts(conn, paged_ids) if not include_traces else {}

        payload_rows = []
        for row in paged_rows:
            payload = _row_to_dict(row)
            if payload is None:
                continue
            row_id = int(payload["id"])
            payload["passed"] = bool(payload["passed"])
            if include_traces:
                payload["traces"] = trace_map.get(row_id, [])
            else:
                payload["trace_count"] = trace_counts.get(row_id, 0)
            payload_rows.append(payload)

        return _envelope(
            ok=True,
            tool="get_verified_tests",
            data={
                "task_ref": resolved_task_ref,
                "lane_id": lane_id,
                "branch": branch,
                "commit_sha": commit_sha,
                "passed": passed,
                "include_traces": include_traces,
                "correlated_file": normalized_correlated_file,
                "correlation_window_minutes": clamped_window,
                "exclude_never_passed": exclude_never_passed,
                "total_matching": total,
                "returned": len(payload_rows),
                "has_more": clamped_offset + len(payload_rows) < total,
                "tests": payload_rows,
            },
            task_ref=resolved_task_ref,
            entity="verified_test",
        )
