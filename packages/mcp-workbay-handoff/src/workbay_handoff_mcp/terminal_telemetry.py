"""Terminal-guard telemetry record/list helpers."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .runtime import get_runtime_config
from .shared_primitives import _envelope, _normalize_optional_text
from .shared_schema import _get_db_connection

_COMMAND_PREVIEW_LIMIT = 256
_GIT_TIMEOUT_SECONDS = 5
_TERMINAL_GUARD_DECISIONS = frozenset({"ask", "block"})
_SECRET_NAME_PATTERN = r"(?:token|password|passwd|secret|api[-_]?key)"
_AUTHORIZATION_RE = re.compile(r"(?i)\b(authorization:)\s+\S+")
_FLAG_EQUALS_RE = re.compile(rf"(?i)(--?(?:{_SECRET_NAME_PATTERN}))\s*=\s*([^\s]+)")
_FLAG_SPACE_RE = re.compile(rf"(?i)(--?(?:{_SECRET_NAME_PATTERN}))\s+([^\s]+)")
_ASSIGNMENT_RE = re.compile(rf"(?i)\b({_SECRET_NAME_PATTERN})\s*=\s*([^\s]+)")


def _normalize_command_preview(command_preview: str) -> str:
    preview = _normalize_optional_text(command_preview)
    if preview is None:
        return ""
    line = preview.splitlines()[0].strip()
    line = _AUTHORIZATION_RE.sub(r"\1 [REDACTED]", line)
    line = _FLAG_EQUALS_RE.sub(r"\1=[REDACTED]", line)
    line = _FLAG_SPACE_RE.sub(r"\1 [REDACTED]", line)
    line = _ASSIGNMENT_RE.sub(r"\1=[REDACTED]", line)
    line = " ".join(line.split())
    if len(line) <= _COMMAND_PREVIEW_LIMIT:
        return line
    return line[: _COMMAND_PREVIEW_LIMIT - 3].rstrip() + "..."


def _resolve_git_common_dir(workspace_root: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(workspace_root), "rev-parse", "--git-common-dir"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    raw = proc.stdout.strip()
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = (workspace_root / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return str(candidate)


def _resolve_repo_instance_id(conn, *, seen_at: str) -> str:
    runtime = get_runtime_config()
    workspace_root = str(runtime.workspace_root)
    git_common_dir = _resolve_git_common_dir(runtime.workspace_root)

    row = None
    if git_common_dir is not None:
        row = conn.execute(
            "SELECT repo_instance_id FROM repo_instances WHERE git_common_dir = ? ORDER BY created_at ASC, repo_instance_id ASC LIMIT 1",
            (git_common_dir,),
        ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT repo_instance_id FROM repo_instances WHERE git_common_dir IS NULL AND workspace_root = ? ORDER BY created_at ASC, repo_instance_id ASC LIMIT 1",
            (workspace_root,),
        ).fetchone()

    if row is None:
        repo_instance_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO repo_instances (
                repo_instance_id,
                workspace_root,
                git_common_dir,
                created_at,
                last_seen_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (repo_instance_id, workspace_root, git_common_dir, seen_at, seen_at),
        )
        return repo_instance_id

    repo_instance_id = str(row["repo_instance_id"])
    if git_common_dir is not None:
        conn.execute(
            """
            UPDATE repo_instances
            SET workspace_root = ?, git_common_dir = ?, last_seen_at = ?
            WHERE repo_instance_id = ?
            """,
            (workspace_root, git_common_dir, seen_at, repo_instance_id),
        )
    else:
        conn.execute(
            """
            UPDATE repo_instances
            SET workspace_root = ?, last_seen_at = ?
            WHERE repo_instance_id = ?
            """,
            (workspace_root, seen_at, repo_instance_id),
        )
    return repo_instance_id


def _compute_event_key(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _prepare_terminal_guard_event(
    conn,
    *,
    task_ref: str | None = None,
    worktree_path: str | None = None,
    harness: str,
    tool_name: str,
    decision: str,
    trigger: str | None = None,
    native_tool_hint: str | None = None,
    command_preview: str,
    policy_version: str,
    policy_source: str,
    fallback_source: str | None = None,
    created_at: str | None = None,
    event_key: str | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    normalized_harness = _normalize_optional_text(harness)
    normalized_tool_name = _normalize_optional_text(tool_name)
    normalized_decision = _normalize_optional_text(decision)
    normalized_command_preview = _normalize_command_preview(command_preview)
    normalized_policy_version = _normalize_optional_text(policy_version)
    normalized_policy_source = _normalize_optional_text(policy_source)
    normalized_task_ref = _normalize_optional_text(task_ref)
    normalized_worktree_path = _normalize_optional_text(worktree_path)
    normalized_trigger = _normalize_optional_text(trigger)
    normalized_native_tool_hint = _normalize_optional_text(native_tool_hint)
    normalized_fallback_source = _normalize_optional_text(fallback_source)
    normalized_event_key = _normalize_optional_text(event_key)

    if normalized_harness is None:
        return None, "harness is required."
    if normalized_tool_name is None:
        return None, "tool_name is required."
    if normalized_decision not in _TERMINAL_GUARD_DECISIONS:
        return None, "decision must be one of ask, block."
    if normalized_command_preview == "":
        return None, "command_preview is required."
    if normalized_policy_version is None:
        return None, "policy_version is required."
    if normalized_policy_source is None:
        return None, "policy_source is required."

    normalized_created_at = _normalize_optional_text(created_at)
    if normalized_created_at is None:
        normalized_created_at = str(conn.execute("SELECT datetime('now')").fetchone()[0])
    repo_instance_id = _resolve_repo_instance_id(conn, seen_at=normalized_created_at)
    if normalized_event_key is None:
        normalized_event_key = _compute_event_key(
            {
                "repo_instance_id": repo_instance_id,
                "task_ref": normalized_task_ref,
                "worktree_path": normalized_worktree_path,
                "harness": normalized_harness,
                "tool_name": normalized_tool_name,
                "decision": normalized_decision,
                "trigger": normalized_trigger,
                "native_tool_hint": normalized_native_tool_hint,
                "command_preview": normalized_command_preview,
                "policy_version": normalized_policy_version,
                "policy_source": normalized_policy_source,
                "fallback_source": normalized_fallback_source,
                "created_at": normalized_created_at,
            }
        )

    return {
        "event_key": normalized_event_key,
        "repo_instance_id": repo_instance_id,
        "task_ref": normalized_task_ref,
        "worktree_path": normalized_worktree_path,
        "harness": normalized_harness,
        "tool_name": normalized_tool_name,
        "decision": normalized_decision,
        "trigger": normalized_trigger,
        "native_tool_hint": normalized_native_tool_hint,
        "command_preview": normalized_command_preview,
        "policy_version": normalized_policy_version,
        "policy_source": normalized_policy_source,
        "fallback_source": normalized_fallback_source,
        "created_at": normalized_created_at,
    }, None


def _upsert_terminal_guard_event(conn, event_row: dict[str, object]) -> tuple[dict[str, object], bool]:
    existed = (
        conn.execute(
            "SELECT 1 FROM terminal_guard_events WHERE event_key = ?",
            (event_row["event_key"],),
        ).fetchone()
        is not None
    )
    conn.execute(
        """
        INSERT INTO terminal_guard_events (
            event_key,
            repo_instance_id,
            task_ref,
            worktree_path,
            harness,
            tool_name,
            decision,
            trigger,
            native_tool_hint,
            command_preview,
            policy_version,
            policy_source,
            fallback_source,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_key) DO UPDATE SET
            task_ref = COALESCE(excluded.task_ref, terminal_guard_events.task_ref),
            worktree_path = COALESCE(excluded.worktree_path, terminal_guard_events.worktree_path),
            trigger = COALESCE(excluded.trigger, terminal_guard_events.trigger),
            native_tool_hint = COALESCE(excluded.native_tool_hint, terminal_guard_events.native_tool_hint),
            policy_version = COALESCE(excluded.policy_version, terminal_guard_events.policy_version),
            policy_source = COALESCE(excluded.policy_source, terminal_guard_events.policy_source),
            fallback_source = COALESCE(excluded.fallback_source, terminal_guard_events.fallback_source)
        """,
        (
            event_row["event_key"],
            event_row["repo_instance_id"],
            event_row["task_ref"],
            event_row["worktree_path"],
            event_row["harness"],
            event_row["tool_name"],
            event_row["decision"],
            event_row["trigger"],
            event_row["native_tool_hint"],
            event_row["command_preview"],
            event_row["policy_version"],
            event_row["policy_source"],
            event_row["fallback_source"],
            event_row["created_at"],
        ),
    )
    row = conn.execute(
        "SELECT * FROM terminal_guard_events WHERE event_key = ?",
        (event_row["event_key"],),
    ).fetchone()
    return dict(row), not existed


def record_terminal_guard_event(
    *,
    task_ref: str | None = None,
    worktree_path: str | None = None,
    harness: str,
    tool_name: str,
    decision: str,
    trigger: str | None = None,
    native_tool_hint: str | None = None,
    command_preview: str,
    policy_version: str,
    policy_source: str,
    fallback_source: str | None = None,
    created_at: str | None = None,
) -> dict:
    normalized_task_ref = _normalize_optional_text(task_ref)

    try:
        with _get_db_connection() as conn:
            event_row, error = _prepare_terminal_guard_event(
                conn,
                task_ref=task_ref,
                worktree_path=worktree_path,
                harness=harness,
                tool_name=tool_name,
                decision=decision,
                trigger=trigger,
                native_tool_hint=native_tool_hint,
                command_preview=command_preview,
                policy_version=policy_version,
                policy_source=policy_source,
                fallback_source=fallback_source,
                created_at=created_at,
            )
            if error is not None or event_row is None:
                # Genuine bad input (validation) — not a DB failure; do not spool.
                return _envelope(
                    ok=False,
                    tool="terminal_guard_telemetry",
                    data={"error": error or "invalid telemetry payload."},
                    task_ref=normalized_task_ref,
                    entity="terminal_guard_event",
                )
            row, _ = _upsert_terminal_guard_event(conn, event_row)
    except Exception as exc:  # noqa: BLE001 — DB open/write failure: never lose a block
        # DB-missing / schema-mismatch / operational error: append to the
        # terminal_guard.jsonl spool so ``replay_terminal_guard_spool`` (its
        # only producer — REV-HARM-1) can ingest the block on a later drain,
        # mirroring the agent-error capture spool fallback.
        return _spool_terminal_guard_fallback(
            exc,
            task_ref=task_ref,
            worktree_path=worktree_path,
            harness=harness,
            tool_name=tool_name,
            decision=decision,
            trigger=trigger,
            native_tool_hint=native_tool_hint,
            command_preview=command_preview,
            policy_version=policy_version,
            policy_source=policy_source,
            fallback_source=fallback_source,
            created_at=created_at,
        )

    return _envelope(
        ok=True,
        tool="terminal_guard_telemetry",
        data={"event": row, "fallback_source": "db"},
        task_ref=normalized_task_ref,
        entity="terminal_guard_event",
    )


def _spool_terminal_guard_event(state_dir: Path, event: dict) -> dict:
    """Append one redacted terminal-guard event JSON line to the spool.

    Producer for ``replay_terminal_guard_spool`` (REV-HARM-1): before this
    the S3 lifecycle drain of ``terminal_guard.jsonl`` had no producer, so a
    guard block whose DB write failed was lost. Never raises — returns a small
    status dict.
    """
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        spool_path = state_dir / "terminal_guard.jsonl"
        with spool_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
    except Exception as exc:  # noqa: BLE001 — fallback must never raise
        return {"ok": False, "error": f"spool write failed: {type(exc).__name__}: {exc}"}
    return {"ok": True, "spool_path": str(spool_path)}


def _spool_terminal_guard_fallback(
    exc: Exception,
    *,
    task_ref: str | None,
    worktree_path: str | None,
    harness: str,
    tool_name: str,
    decision: str,
    trigger: str | None,
    native_tool_hint: str | None,
    command_preview: str,
    policy_version: str,
    policy_source: str,
    fallback_source: str | None,
    created_at: str | None,
) -> dict:
    """Spool a terminal-guard block when the DB write failed. Never raises."""
    normalized_task_ref = _normalize_optional_text(task_ref)
    spooled_at = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    # Redact command_preview at spool time so secrets never land on disk
    # unredacted (replay re-normalizes idempotently).
    event = {
        "task_ref": normalized_task_ref,
        "worktree_path": _normalize_optional_text(worktree_path),
        "harness": _normalize_optional_text(harness),
        "tool_name": _normalize_optional_text(tool_name),
        "decision": _normalize_optional_text(decision),
        "trigger": _normalize_optional_text(trigger),
        "native_tool_hint": _normalize_optional_text(native_tool_hint),
        "command_preview": _normalize_command_preview(command_preview),
        "policy_version": _normalize_optional_text(policy_version),
        "policy_source": _normalize_optional_text(policy_source),
        # Mark provenance so a harvested row is attributable to the spool path.
        "fallback_source": _normalize_optional_text(fallback_source) or "spool",
        # Preserve block-time provenance; replay stamps its own time if None.
        "created_at": _normalize_optional_text(created_at) or spooled_at,
        "spooled_at": spooled_at,
    }
    try:
        state_dir = get_runtime_config().state_dir
    except Exception as cfg_exc:  # noqa: BLE001 — fallback must never raise
        return _envelope(
            ok=False,
            tool="terminal_guard_telemetry",
            data={
                "error": f"db_write_failed_and_runtime_unresolved: {type(cfg_exc).__name__}",
                "fallback_source": "spool",
            },
            task_ref=normalized_task_ref,
            entity="terminal_guard_event",
        )
    spool_result = _spool_terminal_guard_event(state_dir, event)
    if not spool_result.get("ok"):
        return _envelope(
            ok=False,
            tool="terminal_guard_telemetry",
            data={
                "error": spool_result.get("error", "spool write failed"),
                "fallback_source": "spool",
                "db_error": f"{type(exc).__name__}: {exc}",
            },
            task_ref=normalized_task_ref,
            entity="terminal_guard_event",
        )
    return _envelope(
        ok=True,
        tool="terminal_guard_telemetry",
        data={
            "event": event,
            "fallback_source": "spool",
            "spool_path": spool_result.get("spool_path"),
            "db_error": f"{type(exc).__name__}: {exc}",
        },
        task_ref=normalized_task_ref,
        entity="terminal_guard_event",
    )


def replay_terminal_guard_spool(*, spool_path: str | None = None) -> dict:
    runtime = get_runtime_config()
    if spool_path is None:
        resolved_path = runtime.state_dir / "terminal_guard.jsonl"
    else:
        candidate = Path(spool_path).expanduser()
        if not candidate.is_absolute():
            candidate = (runtime.workspace_root / candidate).resolve()
        resolved_path = candidate

    if not resolved_path.exists():
        return _envelope(
            ok=True,
            tool="terminal_guard_telemetry",
            data={
                "spool_path": str(resolved_path),
                "processed": 0,
                "ingested": 0,
                "deduped": 0,
                "invalid": 0,
                "missing": True,
            },
            entity="terminal_guard_event",
        )

    processed = 0
    ingested = 0
    deduped = 0
    invalid = 0
    with _get_db_connection() as conn:
        for raw_line in resolved_path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            processed += 1
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                invalid += 1
                continue
            if not isinstance(payload, dict):
                invalid += 1
                continue

            event_row, error = _prepare_terminal_guard_event(
                conn,
                task_ref=payload.get("task_ref"),
                worktree_path=payload.get("worktree_path"),
                harness=payload.get("harness", ""),
                tool_name=payload.get("tool_name", ""),
                decision=payload.get("decision", ""),
                trigger=payload.get("trigger"),
                native_tool_hint=payload.get("native_tool_hint"),
                command_preview=payload.get("command_preview", ""),
                policy_version=payload.get("policy_version", ""),
                policy_source=payload.get("policy_source", ""),
                fallback_source=payload.get("fallback_source") or str(resolved_path),
                created_at=payload.get("created_at"),
                event_key=payload.get("event_key"),
            )
            if error is not None or event_row is None:
                invalid += 1
                continue
            _, inserted = _upsert_terminal_guard_event(conn, event_row)
            if inserted:
                ingested += 1
            else:
                deduped += 1

    return _envelope(
        ok=True,
        tool="terminal_guard_telemetry",
        data={
            "spool_path": str(resolved_path),
            "processed": processed,
            "ingested": ingested,
            "deduped": deduped,
            "invalid": invalid,
            "missing": False,
        },
        entity="terminal_guard_event",
    )


def list_terminal_guard_events(
    *,
    task_ref: str | None = None,
    decision: str | None = None,
    harness: str | None = None,
    tool_name: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    normalized_task_ref = _normalize_optional_text(task_ref)
    normalized_decision = _normalize_optional_text(decision)
    normalized_harness = _normalize_optional_text(harness)
    normalized_tool_name = _normalize_optional_text(tool_name)
    if normalized_decision is not None and normalized_decision not in _TERMINAL_GUARD_DECISIONS:
        return _envelope(
            ok=False,
            tool="terminal_guard_telemetry",
            data={"error": "decision must be one of ask, block."},
            task_ref=normalized_task_ref,
            entity="terminal_guard_event",
        )

    bounded_limit = max(1, min(limit, 500))
    bounded_offset = max(0, offset)
    where_parts: list[str] = []
    params: list[object] = []
    if normalized_task_ref is not None:
        where_parts.append("task_ref = ?")
        params.append(normalized_task_ref)
    if normalized_decision is not None:
        where_parts.append("decision = ?")
        params.append(normalized_decision)
    if normalized_harness is not None:
        where_parts.append("harness = ?")
        params.append(normalized_harness)
    if normalized_tool_name is not None:
        where_parts.append("tool_name = ?")
        params.append(normalized_tool_name)
    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    with _get_db_connection() as conn:
        total = int(
            conn.execute(
                f"SELECT COUNT(*) FROM terminal_guard_events WHERE {where_sql}",
                tuple(params),
            ).fetchone()[0]
        )
        rows = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT *
                FROM terminal_guard_events
                WHERE {where_sql}
                ORDER BY created_at DESC, event_key DESC
                LIMIT ? OFFSET ?
                """,
                (*params, bounded_limit, bounded_offset),
            ).fetchall()
        ]

    return _envelope(
        ok=True,
        tool="terminal_guard_telemetry",
        data={
            "filters": {
                "task_ref": normalized_task_ref,
                "decision": normalized_decision,
                "harness": normalized_harness,
                "tool_name": normalized_tool_name,
                "limit": bounded_limit,
                "offset": bounded_offset,
            },
            "total_matching": total,
            "returned": len(rows),
            "has_more": (bounded_offset + len(rows)) < total,
            "events": rows,
        },
        task_ref=normalized_task_ref,
        entity="terminal_guard_event",
    )
