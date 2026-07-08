"""Agent-error telemetry record helpers (internal / implementation note).

implementation note surface: the explicit write path behind
``record_event(event_kind='error')``. Inserts one redacted row per call
into ``agent_errors``.

implementation note surface: ``capture_write_rejection`` — the server's best-effort
self-capture of its own rejected writes (``error_class=
mcp_write_rejected``), with a 10-minute dedup window on
``(error_class, summary, task_ref)`` that increments
``occurrence_count`` instead of inserting, a thread-local re-entrancy
guard, and a never-raise guarantee so a failed capture can never fail
the operation it observes.

implementation note surface: ``record_agent_error_direct`` — the
``errors-record`` CLI path used by harness hooks. It bypasses the
configured runtime entirely: the primary DB is resolved via
``git rev-parse --path-format=absolute --git-common-dir`` (linked
worktrees write to the primary repo's ``.task-state``), the connection
uses WAL + busy_timeout in one transaction, and a
``PRAGMA user_version`` guard refuses to touch a DB whose schema
version differs from what this package expects — the redacted event is
appended to ``.task-state/agent-errors-spool.jsonl`` instead, for
later replay by a current install.

Redaction reuses the terminal-telemetry secret patterns; ``detail`` is
multi-line (tracebacks), so the same per-line rules are applied line by
line plus a whole-line Authorization rule (header values must not
survive into a harvestable bundle).
"""

from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path

from workbay_protocol import RUNTIME_ROOT_DIRNAME

from .shared_primitives import _envelope, _normalize_optional_text
from .shared_schema import HANDOFF_SCHEMA_VERSION, _get_db_connection
from .terminal_telemetry import (
    _ASSIGNMENT_RE,
    _AUTHORIZATION_RE,
    _FLAG_EQUALS_RE,
    _FLAG_SPACE_RE,
    _normalize_command_preview,
    _resolve_repo_instance_id,
)

_SUMMARY_LIMIT = 256
_DETAIL_LIMIT = 4096
_CAPTURE_DEDUP_WINDOW_MINUTES = 10
_ERROR_CLASS_RE = re.compile(r"^[a-z][a-z0-9_]*$")
# Header values are freeform; redact the rest of the line, not one token.
_AUTHORIZATION_LINE_RE = re.compile(r"(?i)\b(authorization:).*$")

# Initial taxonomy (implementation note). Append-only strings — unknown classes that
# match the grammar are accepted so consumers can extend without a schema
# migration; this set exists for docs/report grouping, not validation.
KNOWN_ERROR_CLASSES = frozenset(
    {
        "compaction_failed",
        "install_drift",
        "mcp_write_rejected",
        "mcp_unreachable",
        "cli_failure",
        "env_misconfig",
        "other",
    }
)


def _redact_line(line: str) -> str:
    line = _AUTHORIZATION_LINE_RE.sub(r"\1 [REDACTED]", line)
    line = _AUTHORIZATION_RE.sub(r"\1 [REDACTED]", line)
    line = _FLAG_EQUALS_RE.sub(r"\1=[REDACTED]", line)
    line = _FLAG_SPACE_RE.sub(r"\1 [REDACTED]", line)
    line = _ASSIGNMENT_RE.sub(r"\1=[REDACTED]", line)
    return line


def _redact_text(value: str | None, *, limit: int) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    redacted = "\n".join(_redact_line(line) for line in normalized.splitlines())
    if len(redacted) <= limit:
        return redacted
    return redacted[: limit - 3].rstrip() + "..."


def record_agent_error(
    *,
    error_class: str,
    summary: str,
    detail: str | None = None,
    tool_name: str | None = None,
    command_preview: str | None = None,
    package_name: str | None = None,
    package_version: str | None = None,
    workbay_release: str | None = None,
    harness: str = "mcp",
    task_ref: str | None = None,
) -> dict:
    normalized_task_ref = _normalize_optional_text(task_ref)
    normalized_class = _normalize_optional_text(error_class)
    normalized_summary = _redact_text(summary, limit=_SUMMARY_LIMIT)

    error: str | None = None
    if normalized_class is None or not _ERROR_CLASS_RE.match(normalized_class):
        error = "error_class must match ^[a-z][a-z0-9_]*$."
    elif normalized_summary is None:
        error = "summary is required."

    if error is not None:
        return _envelope(
            ok=False,
            tool="record_event",
            data={"error": error},
            task_ref=normalized_task_ref,
            entity="agent_error",
        )

    normalized_detail = _redact_text(detail, limit=_DETAIL_LIMIT)
    normalized_preview = None
    preview_text = _normalize_optional_text(command_preview)
    if preview_text is not None:
        normalized_preview = _normalize_command_preview(preview_text)

    with _get_db_connection() as conn:
        now = str(conn.execute("SELECT datetime('now')").fetchone()[0])
        repo_instance_id = _resolve_repo_instance_id(conn, seen_at=now)
        cursor = conn.execute(
            """
            INSERT INTO agent_errors (
                repo_instance_id, task_ref, harness, error_class, summary,
                detail, tool_name, command_preview, package_name,
                package_version, workbay_release, occurrence_count,
                created_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                repo_instance_id,
                normalized_task_ref,
                _normalize_optional_text(harness) or "mcp",
                normalized_class,
                normalized_summary,
                normalized_detail,
                _normalize_optional_text(tool_name),
                normalized_preview,
                _normalize_optional_text(package_name),
                _normalize_optional_text(package_version),
                _normalize_optional_text(workbay_release),
                now,
                now,
            ),
        )
        row = conn.execute("SELECT * FROM agent_errors WHERE id = ?", (cursor.lastrowid,)).fetchone()

    return _envelope(
        ok=True,
        tool="record_event",
        data={"agent_error": dict(row)},
        task_ref=normalized_task_ref,
        entity="agent_error",
    )


# ---------------------------------------------------------------------------
# implementation note — server self-capture of rejected writes
# ---------------------------------------------------------------------------

_capture_state = threading.local()


def capture_write_rejection(
    *,
    tool_name: str,
    summary: str,
    task_ref: str | None = None,
    detail: str | None = None,
    harness: str = "mcp",
) -> None:
    """Best-effort capture of a rejected MCP write as ``mcp_write_rejected``.

    Never raises: a failed capture is swallowed so it cannot fail the
    operation it observes, and a re-entrant call (a capture triggered
    while a capture is in flight on the same thread) is dropped by the
    thread-local guard.
    """
    if getattr(_capture_state, "active", False):
        return
    _capture_state.active = True
    try:
        _capture_write_rejection_unguarded(
            tool_name=tool_name,
            summary=summary,
            task_ref=task_ref,
            detail=detail,
            harness=harness,
        )
    except Exception as exc:  # noqa: BLE001 — never-fail guarantee (implementation note acceptance)
        _trace_capture_failure(
            exc,
            tool_name=tool_name,
            summary=summary,
            task_ref=task_ref,
            detail=detail,
            harness=harness,
        )
    finally:
        _capture_state.active = False


def _trace_capture_failure(
    exc: Exception,
    *,
    tool_name: str,
    summary: str,
    task_ref: str | None,
    detail: str | None,
    harness: str,
) -> None:
    """Make self-capture failure visible without failing the observed write."""
    sys.stderr.write(f"agent_errors self-capture failed: {type(exc).__name__}: {exc}\n")
    # Resolve the spool dir through the SAME primary-root resolution that
    # ``replay_agent_error_spool`` uses. Preferring ``get_runtime_config()``
    # here would spool into the nested ``.workbay`` overlay's state dir under a
    # nested install, while replay reads from the outer primary — orphaning the
    # trace. ``_resolve_primary_state_dir`` pins both to the outer primary DB.
    state_dir = _resolve_primary_state_dir(Path.cwd())
    if state_dir is None:
        try:
            from .runtime import get_runtime_config  # noqa: PLC0415

            state_dir = get_runtime_config().state_dir
        except Exception:  # noqa: BLE001 - best-effort fallback only
            state_dir = Path.cwd() / ".task-state"
    event = {
        "error_class": "mcp_write_rejected",
        "summary": _redact_text(summary, limit=_SUMMARY_LIMIT),
        "detail": _redact_text(detail, limit=_DETAIL_LIMIT),
        "tool_name": _normalize_optional_text(tool_name),
        "command_preview": None,
        "package_name": None,
        "package_version": None,
        "workbay_release": None,
        "harness": _normalize_optional_text(harness) or "mcp",
        "task_ref": _normalize_optional_text(task_ref),
        "spooled_at": datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
    }
    result = _spool_agent_error(state_dir, event, reason=f"self_capture_failed:{type(exc).__name__}")
    if not result.get("ok"):
        sys.stderr.write(f"agent_errors self-capture spool failed: {result.get('error')}\n")


def _capture_write_rejection_unguarded(
    *,
    tool_name: str,
    summary: str,
    task_ref: str | None,
    detail: str | None,
    harness: str,
) -> None:
    normalized_summary = _redact_text(summary, limit=_SUMMARY_LIMIT)
    if normalized_summary is None:
        return
    normalized_task_ref = _normalize_optional_text(task_ref)
    normalized_detail = _redact_text(detail, limit=_DETAIL_LIMIT)

    with _get_db_connection() as conn:
        now = str(conn.execute("SELECT datetime('now')").fetchone()[0])
        repo_instance_id = _resolve_repo_instance_id(conn, seen_at=now)
        _dedup_insert_or_bump(
            conn,
            now=now,
            repo_instance_id=repo_instance_id,
            error_class="mcp_write_rejected",
            summary=normalized_summary,
            task_ref=normalized_task_ref,
            detail=normalized_detail,
            tool_name=_normalize_optional_text(tool_name),
            harness=_normalize_optional_text(harness) or "mcp",
        )


def _dedup_insert_or_bump(
    conn,
    *,
    now: str,
    repo_instance_id: str,
    error_class: str,
    summary: str,
    task_ref: str | None,
    detail: str | None = None,
    tool_name: str | None = None,
    command_preview: str | None = None,
    package_name: str | None = None,
    package_version: str | None = None,
    workbay_release: str | None = None,
    harness: str = "mcp",
    created_at: str | None = None,
) -> str:
    """Insert an ``agent_errors`` row or bump an in-window duplicate.

    Dedup window (implementation note review decision 2): same
    ``(error_class, summary, task_ref)`` within 10 minutes updates the
    existing row's ``occurrence_count``/``last_seen_at`` instead of
    inserting. ``IS ?`` keeps NULL task_refs comparable. Served by the
    ``(error_class, summary, task_ref, last_seen_at)`` index from
    implementation note. Returns ``"inserted"`` or ``"deduped"``.

    ``created_at`` overrides the insert's first-seen timestamp (spool
    replay passes the original ``spooled_at`` so harvest first-seen
    provenance survives a delayed replay — REV-D-006); ``last_seen_at``
    and dedup bumps always use ``now``.
    """
    existing = conn.execute(
        """
        SELECT id FROM agent_errors
        WHERE error_class = ?
          AND summary = ?
          AND task_ref IS ?
          AND last_seen_at >= datetime('now', ?)
        ORDER BY last_seen_at DESC, id DESC
        LIMIT 1
        """,
        (
            error_class,
            summary,
            task_ref,
            f"-{_CAPTURE_DEDUP_WINDOW_MINUTES} minutes",
        ),
    ).fetchone()
    if existing is not None:
        conn.execute(
            "UPDATE agent_errors SET occurrence_count = occurrence_count + 1, last_seen_at = ? WHERE id = ?",
            (now, existing["id"]),
        )
        return "deduped"

    conn.execute(
        """
        INSERT INTO agent_errors (
            repo_instance_id, task_ref, harness, error_class, summary,
            detail, tool_name, command_preview, package_name,
            package_version, workbay_release, occurrence_count,
            created_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            repo_instance_id,
            task_ref,
            harness,
            error_class,
            summary,
            detail,
            tool_name,
            command_preview,
            package_name,
            package_version,
            workbay_release,
            created_at or now,
            now,
        ),
    )
    return "inserted"


# ---------------------------------------------------------------------------
# implementation note — errors-record direct path (harness hooks)
# ---------------------------------------------------------------------------


def _resolve_primary_state_dir(cwd: Path) -> Path | None:
    """Primary repo ``.task-state`` dir via the git common dir, or None.

    ``--git-common-dir`` points at the primary ``.git`` even from a
    linked worktree, so hook writes land in the primary repo state and
    never create cwd-local ``.task-state`` directories.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(cwd), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    common_dir = proc.stdout.strip()
    if not common_dir:
        return None
    repo_root = Path(common_dir).parent
    for parent in (repo_root, *repo_root.parents):
        if parent.name == RUNTIME_ROOT_DIRNAME:
            outer_root = parent.parent
            try:
                outer_proc = subprocess.run(
                    ["git", "-C", str(outer_root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
            except Exception:
                return outer_root / ".task-state"
            if outer_proc.returncode != 0:
                return outer_root / ".task-state"
            outer_common_dir = outer_proc.stdout.strip()
            if not outer_common_dir:
                return outer_root / ".task-state"
            return Path(outer_common_dir).parent / ".task-state"
    return repo_root / ".task-state"


def _spool_agent_error(state_dir: Path, event: dict, *, reason: str) -> dict:
    """Append a redacted event to the spool for later replay."""
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        spool_path = state_dir / "agent-errors-spool.jsonl"
        with spool_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
    except Exception as exc:
        return {"ok": False, "error": f"spool write failed: {type(exc).__name__}: {exc}"}
    return {"ok": True, "mode": "spool", "spool_path": str(spool_path), "reason": reason}


def _resolve_repo_instance_id_direct(conn, *, workspace_root: Path, seen_at: str) -> str:
    """Repo-instance resolution for the direct path (no runtime config).

    Mirrors ``terminal_telemetry._resolve_repo_instance_id`` but keys on
    the already-resolved primary repo root instead of the configured
    runtime workspace root.
    """
    git_common_dir = str((workspace_root / ".git").resolve())
    row = conn.execute(
        "SELECT repo_instance_id FROM repo_instances WHERE git_common_dir = ? "
        "ORDER BY created_at ASC, repo_instance_id ASC LIMIT 1",
        (git_common_dir,),
    ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT repo_instance_id FROM repo_instances WHERE workspace_root = ? "
            "ORDER BY created_at ASC, repo_instance_id ASC LIMIT 1",
            (str(workspace_root),),
        ).fetchone()
    if row is not None:
        repo_instance_id = str(row["repo_instance_id"])
        conn.execute(
            "UPDATE repo_instances SET last_seen_at = ? WHERE repo_instance_id = ?",
            (seen_at, repo_instance_id),
        )
        return repo_instance_id

    repo_instance_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO repo_instances (
            repo_instance_id, workspace_root, git_common_dir, created_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (repo_instance_id, str(workspace_root), git_common_dir, seen_at, seen_at),
    )
    return repo_instance_id


def record_agent_error_direct(
    *,
    error_class: str,
    summary: str,
    detail: str | None = None,
    tool_name: str | None = None,
    command_preview: str | None = None,
    package_name: str | None = None,
    package_version: str | None = None,
    workbay_release: str | None = None,
    harness: str = "hook",
    task_ref: str | None = None,
    cwd: Path | str | None = None,
) -> dict:
    """Direct-SQLite agent-error write for the ``errors-record`` CLI.

    Returns a small status dict (``{ok, mode: "db"|"spool", ...}``) —
    not a v2 envelope; this path runs outside the configured runtime.
    Spools instead of writing when the DB is missing, its
    ``user_version`` differs from ``HANDOFF_SCHEMA_VERSION`` (stale
    package vs newer DB, or stale DB vs newer package), or the write
    fails operationally.
    """
    normalized_class = _normalize_optional_text(error_class)
    normalized_summary = _redact_text(summary, limit=_SUMMARY_LIMIT)
    if normalized_class is None or not _ERROR_CLASS_RE.match(normalized_class):
        return {"ok": False, "error": "error_class must match ^[a-z][a-z0-9_]*$."}
    if normalized_summary is None:
        return {"ok": False, "error": "summary is required."}

    resolved_cwd = Path(cwd) if cwd is not None else Path.cwd()
    state_dir = _resolve_primary_state_dir(resolved_cwd)
    if state_dir is None:
        return {"ok": False, "error": "not inside a git repository; cannot resolve primary state dir."}

    normalized_preview = None
    preview_text = _normalize_optional_text(command_preview)
    if preview_text is not None:
        normalized_preview = _normalize_command_preview(preview_text)
    normalized_harness = _normalize_optional_text(harness) or "hook"
    event = {
        "error_class": normalized_class,
        "summary": normalized_summary,
        "detail": _redact_text(detail, limit=_DETAIL_LIMIT),
        "tool_name": _normalize_optional_text(tool_name),
        "command_preview": normalized_preview,
        "package_name": _normalize_optional_text(package_name),
        "package_version": _normalize_optional_text(package_version),
        "workbay_release": _normalize_optional_text(workbay_release),
        "harness": normalized_harness,
        "task_ref": _normalize_optional_text(task_ref),
        "spooled_at": datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
    }

    db_path = state_dir / "handoff.db"
    if not db_path.exists():
        return _spool_agent_error(state_dir, event, reason="db_missing")

    try:
        conn = sqlite3.connect(db_path, timeout=5)
    except sqlite3.Error as exc:
        return _spool_agent_error(state_dir, event, reason=f"connect_failed: {exc}")
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA journal_mode = WAL")
        db_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if db_version != HANDOFF_SCHEMA_VERSION:
            return _spool_agent_error(
                state_dir,
                event,
                reason=f"schema_version_mismatch: db={db_version} package={HANDOFF_SCHEMA_VERSION}",
            )
        with conn:  # single insert/update transaction
            now = str(conn.execute("SELECT datetime('now')").fetchone()[0])
            repo_instance_id = _resolve_repo_instance_id_direct(conn, workspace_root=state_dir.parent, seen_at=now)
            outcome = _dedup_insert_or_bump(
                conn,
                now=now,
                repo_instance_id=repo_instance_id,
                error_class=normalized_class,
                summary=normalized_summary,
                task_ref=event["task_ref"],
                detail=event["detail"],
                tool_name=event["tool_name"],
                command_preview=normalized_preview,
                package_name=event["package_name"],
                package_version=event["package_version"],
                workbay_release=event["workbay_release"],
                harness=normalized_harness,
            )
        return {"ok": True, "mode": "db", "outcome": outcome, "db_path": str(db_path)}
    except sqlite3.Error as exc:
        return _spool_agent_error(state_dir, event, reason=f"sqlite_error: {exc}")
    finally:
        conn.close()


def replay_agent_error_spool(
    *,
    cwd: Path | str | None = None,
    spool_path: Path | str | None = None,
) -> dict:
    """Drain ``agent-errors-spool.jsonl`` into the primary DB (REV-B-001).

    The replay half of the implementation note spool contract: events spooled by an
    older/newer ``errors-record`` install are written through the same
    schema-version-guarded direct path once a matching install runs.
    Successfully replayed lines are removed from the spool; malformed or
    failed lines are kept for a later attempt, as are lines appended by
    a concurrent hook while the replay ran. When the version guard still
    fails the spool is left untouched. Returns a status dict; never
    raises.

    Delivery is at-least-once (REV-D-004): rows commit before the spool
    rewrite, so a rewrite failure (reported as ``spool_rewrite_failed``)
    leaves replayed lines in the spool and a prompt re-run re-inserts
    them — the 10-minute dedup window absorbs the duplicates. The
    alternative (rewrite before commit) risks at-most-once data loss.

    ``spool_path`` overrides only the spool file location; the target DB
    is always the primary repo's resolved from ``cwd`` (REV-D-002 — a
    relocated spool file must not retarget the write to a sibling DB).
    """
    try:
        resolved_cwd = Path(cwd) if cwd is not None else Path.cwd()
        maybe_state_dir = _resolve_primary_state_dir(resolved_cwd)
        if maybe_state_dir is None:
            return {
                "ok": False,
                "error": "not inside a git repository; cannot resolve primary state dir.",
            }
        state_dir = maybe_state_dir
        spool = Path(spool_path) if spool_path is not None else state_dir / "agent-errors-spool.jsonl"

        if not spool.exists():
            return {"ok": True, "mode": "replay", "replayed": 0, "remaining": 0, "reason": "no_spool"}
        lines = spool.read_text(encoding="utf-8").splitlines()
        if not any(line.strip() for line in lines):
            spool.unlink(missing_ok=True)
            return {"ok": True, "mode": "replay", "replayed": 0, "remaining": 0, "reason": "empty_spool"}

        db_path = state_dir / "handoff.db"
        if not db_path.exists():
            return {"ok": False, "error": "db_missing", "remaining": len(lines)}
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute("PRAGMA journal_mode = WAL")
            db_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if db_version != HANDOFF_SCHEMA_VERSION:
                return {
                    "ok": False,
                    "error": f"schema_version_mismatch: db={db_version} package={HANDOFF_SCHEMA_VERSION}",
                    "remaining": len(lines),
                }
            kept: list[str] = []
            replayed = 0
            with conn:  # one transaction for the whole drain
                now = str(conn.execute("SELECT datetime('now')").fetchone()[0])
                repo_instance_id = _resolve_repo_instance_id_direct(conn, workspace_root=state_dir.parent, seen_at=now)
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        kept.append(line)
                        continue
                    if not isinstance(event, dict):
                        kept.append(line)
                        continue
                    error_class = _normalize_optional_text(event.get("error_class"))
                    summary = _redact_text(event.get("summary"), limit=_SUMMARY_LIMIT)
                    if error_class is None or not _ERROR_CLASS_RE.match(error_class) or summary is None:
                        kept.append(line)
                        continue
                    try:
                        _dedup_insert_or_bump(
                            conn,
                            now=now,
                            repo_instance_id=repo_instance_id,
                            error_class=error_class,
                            summary=summary,
                            task_ref=_normalize_optional_text(event.get("task_ref")),
                            detail=_redact_text(event.get("detail"), limit=_DETAIL_LIMIT),
                            tool_name=_normalize_optional_text(event.get("tool_name")),
                            command_preview=_normalize_optional_text(event.get("command_preview")),
                            package_name=_normalize_optional_text(event.get("package_name")),
                            package_version=_normalize_optional_text(event.get("package_version")),
                            workbay_release=_normalize_optional_text(event.get("workbay_release")),
                            harness=_normalize_optional_text(event.get("harness")) or "hook",
                            created_at=_normalize_optional_text(event.get("spooled_at")),
                        )
                        replayed += 1
                    except sqlite3.Error:
                        kept.append(line)
        finally:
            conn.close()

        # Keep any lines a concurrent hook appended while we replayed.
        # Rows are committed at this point: a failure below reports
        # spool_rewrite_failed instead of pretending nothing replayed
        # (at-least-once contract — see docstring).
        try:
            current = spool.read_text(encoding="utf-8").splitlines()
            tail = current[len(lines) :]
            remaining = [line for line in (*kept, *tail) if line.strip()]
            if remaining:
                tmp = spool.with_suffix(".jsonl.tmp")
                tmp.write_text("".join(line + "\n" for line in remaining), encoding="utf-8")
                tmp.replace(spool)
            else:
                spool.unlink(missing_ok=True)
        except Exception as exc:  # noqa: BLE001 — rows are already durable; report precisely
            return {
                "ok": False,
                "error": f"spool_rewrite_failed: {type(exc).__name__}: {exc}",
                "replayed": replayed,
                "spool_path": str(spool),
                "hint": "replayed rows are committed; re-running replay re-inserts them (deduped within 10 minutes)",
            }
        return {
            "ok": True,
            "mode": "replay",
            "replayed": replayed,
            "remaining": len(remaining),
            "db_path": str(db_path),
            "spool_path": str(spool),
        }
    except Exception as exc:  # noqa: BLE001 — replay must never crash a hook/CLI caller
        return {"ok": False, "error": f"replay failed: {type(exc).__name__}: {exc}"}
