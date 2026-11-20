"""Verified test reads and harness-owned base-result storage."""

from __future__ import annotations

import errno
import json
import os
import re
import sqlite3
import stat
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, TypeGuard

from .shared_primitives import _envelope, _resolve_task_ref, _row_to_dict
from .shared_schema import _get_db_connection

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

_BASE_RESULT_SCHEMA = "workbay/base-result-cache@2"
_BASE_OUTCOMES = frozenset({"passed", "failed", "skipped"})


@dataclass(frozen=True)
class BaseResultLookup:
    status: str
    reason: str | None = None
    outcomes: dict[str, str] | None = None


def _record_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(document, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii")


def lookup_base_result(root: Path, key: str) -> BaseResultLookup:
    """Read only. The harness pins its verification key outside the record store."""
    if not re.fullmatch(r"[0-9a-f]{64}", key):
        raise ValueError("expected a SHA-256 cache key")
    try:
        if not stat.S_ISDIR(root.stat().st_mode):
            raise ValueError("expected cache directory")
        try:
            fd = os.open(root / f"{key}.json", os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        except FileNotFoundError:
            return BaseResultLookup("miss")
        with os.fdopen(fd, "rb") as stream:
            record = os.fstat(stream.fileno())
            if not stat.S_ISREG(record.st_mode) or record.st_size > 8_000_000:
                raise ValueError("expected regular record")
            document = json.load(stream)
        if not isinstance(document, dict) or not _usable_base_outcomes(document.get("outcomes")):
            raise ValueError("invalid baseline")
    except (OSError, ValueError, RecursionError):
        return BaseResultLookup("unknown", "base_result_cache_unreadable")

    try:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except ImportError:
        return BaseResultLookup("unknown", "base_result_verifier_unavailable")
    try:
        if document.get("schema") != _BASE_RESULT_SCHEMA or document.get("key") != key:
            raise ValueError("unbound record")
        public = Ed25519PublicKey.from_public_bytes(bytes.fromhex(os.environ["WORKBAY_BASE_RESULT_VERIFY_KEY"]))
        signature = bytes.fromhex(document.pop("signature"))
        public.verify(signature, _record_bytes(document))
    except (InvalidSignature, KeyError, TypeError, ValueError, RecursionError):
        return BaseResultLookup("unknown", "base_result_writer_unverified")
    return BaseResultLookup("hit", outcomes=document["outcomes"])


def _usable_base_outcomes(outcomes: object) -> TypeGuard[dict[str, str]]:
    return (
        isinstance(outcomes, dict)
        and any(value in ("passed", "failed") for value in outcomes.values())
        and all(
            isinstance(node, str) and "::" in node and isinstance(result, str) and result in _BASE_OUTCOMES
            for node, result in outcomes.items()
        )
    )


def _publish_base_result(path: Path, key: str, outcomes: dict[str, str], signer: Ed25519PrivateKey) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".base-result-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            document: dict[str, object] = {"schema": _BASE_RESULT_SCHEMA, "key": key, "outcomes": outcomes}
            document["signature"] = signer.sign(_record_bytes(document)).hex()
            stream.write(_record_bytes(document))
            stream.flush()
            os.fchmod(stream.fileno(), 0o444)
            os.fsync(stream.fileno())
            os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def measure_harness_base_result(
    root: Path,
    key: str,
    measure: Callable[[], dict[str, str] | None],
    *,
    lock_timeout: float,
) -> BaseResultLookup:
    """Serialize a closure measurement, signing only with the held harness capability.

    The supervisor provides a seekable descriptor containing a raw Ed25519 seed
    to closure processes alone. Readers receive only the hex public key. The
    cache directory and the ordinary verified_tests ledger confer no authority.
    """
    lookup = lookup_base_result(root, key)
    if lookup.status != "miss":
        return lookup
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        fd = int(os.environ["WORKBAY_BASE_RESULT_SIGNING_FD"])
        if fd < 3:
            raise ValueError("expected a dedicated signing descriptor")
        signer = Ed25519PrivateKey.from_private_bytes(os.pread(fd, 33, 0))
        if signer.public_key().public_bytes_raw() != bytes.fromhex(os.environ["WORKBAY_BASE_RESULT_VERIFY_KEY"]):
            raise ValueError("signer differs from configured authority")
    except (AttributeError, ImportError, KeyError, OSError, OverflowError, ValueError):
        return BaseResultLookup("refused", "base_result_write_refused")
    path = root / f"{key}.json"
    try:
        import fcntl
    except ImportError:
        return BaseResultLookup("unknown", "base_result_cache_lock_unavailable")

    # Serialize misses for this key, including across worktrees. Never lock a
    # hit, so a read-only lane can use it without opening any writable handle.
    try:
        fd = os.open(root / f".{key}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "r+b") as lock:
            deadline = time.monotonic() + lock_timeout
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        return BaseResultLookup("unknown", "base_result_cache_lock_timeout")
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            lookup = lookup_base_result(root, key)
            if lookup.status != "miss":
                return lookup
            outcomes = measure()
            if not _usable_base_outcomes(outcomes):
                return BaseResultLookup("miss", "base_result_measurement_unknown")
            _publish_base_result(path, key, outcomes, signer)
            return BaseResultLookup("miss", outcomes=outcomes)
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EPERM, errno.EROFS}:
            return BaseResultLookup("refused", "base_result_write_refused")
        return BaseResultLookup("unknown", "base_result_cache_write_unavailable")


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


def get_verified_test_by_id(test_id: int) -> dict:
    """Read one verified test row by its task-independent primary key."""

    if isinstance(test_id, bool) or not isinstance(test_id, int) or test_id <= 0:
        return _envelope(
            ok=False,
            tool="get_verified_test_by_id",
            data={"error": "test_id must be a positive integer."},
            entity="verified_test",
        )

    with _get_db_connection() as conn:
        row = conn.execute("SELECT * FROM verified_tests WHERE id = ?", (test_id,)).fetchone()
        payload = _row_to_dict(row)
        if payload is not None:
            payload["passed"] = bool(payload["passed"])
        return _envelope(
            ok=True,
            tool="get_verified_test_by_id",
            data={"test_id": test_id, "found": payload is not None, "test": payload},
            entity="verified_test",
        )
