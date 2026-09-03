"""Session-start handoff tool roster resolution."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, cast

from workbay_protocol.tool_serving_index import (
    HANDOFF_TOOL_NAMES,
    ORCHESTRATOR_TOOL_NAMES,
    SLUG_MCP_TOOLS,
)

from .shared_schema import _open_bounded_roster_read_connection, connect_handoff_db

ALWAYS_SERVE = frozenset(
    {
        "continuation",
        "semantic_reinjection_packet",
        "run_structured_turn",
        "list_available_backends",
        "get_verified_tests",
        "review_findings",
        "review_runs",
    }
)
DEFAULT_SKILLS_BY_STATUS: dict[str, tuple[str, ...]] = {
    "in_progress": ("handoff-lifecycle", "incremental-implementation", "tdd", "branch-lifecycle"),
    "review": ("handoff-lifecycle", "branch-review", "review-parallel", "planning-review"),
    "blocked": ("handoff-lifecycle", "investigate"),
    "done": ("handoff-lifecycle",),
}
CATALOG_TOOLS = frozenset({"activate_tool_domain", "list_tool_domains"})

_log = logging.getLogger("workbay_handoff_mcp.roster")


@dataclass(frozen=True, slots=True)
class BootRoster:
    effective_policy: str
    resolved_task_ref: str | None
    floor_taken: bool
    served: frozenset[str]


@dataclass(frozen=True, slots=True)
class _DecodedList:
    """Distinguish an absent roster cell from valid emptiness and corruption."""

    value: list[str] | None
    corrupt: bool = False


class RosterCorruptionError(ValueError):
    """A persisted roster cell is present but is not a JSON string list."""


@dataclass(frozen=True, slots=True)
class _AuditDocument:
    """Validated fields consumed from a non-authoritative skill document."""

    mcp_tools: tuple[str, ...]


class AuditDocumentShapeError(ValueError):
    """A non-authoritative skill document has an unsafe decoded shape."""

    def __init__(self, field: str) -> None:
        super().__init__(f"invalid audit document field: {field}")
        self.field = field


_BOOT_ROSTER = BootRoster("all", None, False, HANDOFF_TOOL_NAMES)


def current_boot_roster() -> BootRoster:
    return _BOOT_ROSTER


def record_boot_roster(roster: BootRoster) -> None:
    global _BOOT_ROSTER
    _BOOT_ROSTER = roster


def _floor_names() -> set[str]:
    return set().union(*(SLUG_MCP_TOOLS.get(slug, ()) for slug in DEFAULT_SKILLS_BY_STATUS["in_progress"]))


def _decode_list(value: object) -> _DecodedList:
    if value is None:
        return _DecodedList(None)
    if not isinstance(value, str):
        return _DecodedList(None, corrupt=True)
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return _DecodedList(None, corrupt=True)
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        return _DecodedList(None, corrupt=True)
    return _DecodedList(parsed)


def _decode_roster_row(row: sqlite3.Row, task_ref: str) -> tuple[str, list[str], list[str]]:
    skill_slugs = _decode_list(row["skill_slugs"])
    activated_domains = _decode_list(row["activated_domains"])
    corrupt_fields = [
        field
        for field, decoded in (
            ("skill_slugs", skill_slugs),
            ("activated_domains", activated_domains),
        )
        if decoded.corrupt
    ]
    if corrupt_fields:
        _receipt("roster_corrupt", task_ref=task_ref, fields=corrupt_fields)
        raise RosterCorruptionError(
            f"corrupt roster fields for {task_ref!r}: {', '.join(corrupt_fields)}"
        )
    return str(row["status"]), skill_slugs.value or [], activated_domains.value or []


def _read_live_roster(db_path: Path, task_ref: str) -> tuple[str, list[str], list[str]]:
    with connect_handoff_db(db_path, read_only=True) as conn:
        row = conn.execute(
            """SELECT h.status, r.skill_slugs, r.activated_domains
               FROM handoff_state AS h
               LEFT JOIN task_tool_roster AS r ON r.task_ref = h.task_ref
               WHERE h.task_ref = ?""",
            (task_ref,),
        ).fetchone()
    if row is None:
        raise ValueError(f"resolved task_ref {task_ref!r} has no handoff_state row")
    return _decode_roster_row(row, task_ref)


def _sole_done_roster(
    db_path: Path, *, remaining_ms: int
) -> tuple[str, str, list[str], list[str]] | None:
    """Read one terminal roster only when no live row exists, within the boot budget."""
    if remaining_ms <= 0 or os.environ.get("WORKBAY_HANDOFF_ACTIVE_TASK", "").strip():
        return None
    deadline = time.monotonic() + (remaining_ms / 1000)
    conn = _open_bounded_roster_read_connection(db_path, busy_timeout_ms=remaining_ms)
    try:
        if time.monotonic() >= deadline:
            return None
        # busy_timeout bounds lock contention only. The progress handler also
        # interrupts ordinary query execution when the shared boot budget ends.
        conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        row = conn.execute(
            """WITH counts AS (
                   SELECT
                       SUM(CASE WHEN status != 'done' THEN 1 ELSE 0 END) AS live_count,
                       SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_count
                   FROM handoff_state
               )
               SELECT h.task_ref, h.status, r.skill_slugs, r.activated_domains
               FROM counts
               JOIN handoff_state AS h ON h.status = 'done'
               LEFT JOIN task_tool_roster AS r ON r.task_ref = h.task_ref
               WHERE COALESCE(counts.live_count, 0) = 0 AND counts.done_count = 1"""
        ).fetchone()
    finally:
        conn.close()
    if row is None or time.monotonic() >= deadline:
        return None
    task_ref = str(row["task_ref"])
    status, slugs, domains = _decode_roster_row(row, task_ref)
    return task_ref, status, slugs, domains


def _resolution_fields(
    result: object,
) -> tuple[str | None, bool, int, int | None, int | None, tuple[str, ...], str | None]:
    if hasattr(result, "task_ref"):
        return (
            getattr(result, "task_ref", None),
            bool(getattr(result, "floor_taken", False)),
            int(getattr(result, "elapsed_ms", 0)),
            getattr(result, "open_ms", None),
            getattr(result, "query_ms", None),
            tuple(getattr(result, "tiebreak_candidates", ()) or ()),
            getattr(result, "exception_class", None),
        )
    values: tuple[object, ...] = tuple(cast(Iterable[object], result))
    task_ref = values[0]
    elapsed_ms = values[2]
    open_ms = values[3]
    query_ms = values[4]
    raw_candidates = values[5] if len(values) > 5 else None
    return (
        task_ref if isinstance(task_ref, str) else None,
        bool(values[1]),
        elapsed_ms if isinstance(elapsed_ms, int) else 0,
        open_ms if isinstance(open_ms, int) else None,
        query_ms if isinstance(query_ms, int) else None,
        tuple(str(item) for item in cast(Iterable[object], raw_candidates)) if raw_candidates else (),
        str(values[6]) if len(values) > 6 and values[6] else None,
    )


def _receipt(event: str, **fields: object) -> None:
    _log.warning("%s %s", event, json.dumps(fields, sort_keys=True, default=str))


def _validate_audit_document(document: object) -> _AuditDocument:
    """Validate every field before the non-authoritative document is consumed."""
    if not isinstance(document, dict):
        raise AuditDocumentShapeError("document")
    mcp_tools = document.get("mcp_tools")
    if mcp_tools is None:
        return _AuditDocument(())
    if not isinstance(mcp_tools, list) or not all(
        isinstance(tool_name, str) for tool_name in mcp_tools
    ):
        raise AuditDocumentShapeError("mcp_tools")
    return _AuditDocument(tuple(mcp_tools))


def _audit_source_classification(slugs: list[str]) -> None:
    """Emit drift receipts without using payload YAML as serving input."""
    try:
        import yaml  # noqa: PLC0415
    except ImportError:
        return
    skills_root = Path(__file__).resolve().parents[3] / "workbay-system/workbay_system/payload/skills"
    for slug in slugs:
        path = skills_root / slug / "skill.yaml"
        try:
            if not path.is_file():
                continue
            document = _validate_audit_document(
                yaml.safe_load(path.read_text(encoding="utf-8"))
            )
        except AuditDocumentShapeError as exc:
            _receipt(
                "roster_audit_failed",
                slug=slug,
                field=exc.field,
                exception_class=type(exc).__name__,
            )
            continue
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            _receipt("roster_audit_failed", slug=slug, exception_class=type(exc).__name__)
            continue
        for tool_name in document.mcp_tools:
            if tool_name in SLUG_MCP_TOOLS.get(slug, ()):
                continue
            reason = "other_server" if tool_name in ORCHESTRATOR_TOOL_NAMES else "unknown_name"
            _receipt("roster_tool_skipped", reason=reason, tool=tool_name, slug=slug)


def resolve_handoff_boot_roster(
    *,
    policy: str,
    db_path: Path,
    domain_tool_map: dict[str, list[str]],
    bounded_resolver: Callable[..., object],
) -> BootRoster:
    """Resolve one immutable served set before FastMCP construction."""
    if policy != "skill":
        return BootRoster("all", None, False, HANDOFF_TOOL_NAMES)

    resolved_task_ref: str | None = None
    floor_taken = False
    status = "in_progress"
    slugs: list[str] = []
    domains: list[str] = []
    try:
        result = bounded_resolver(db_path, deadline_s=2.0)
        (
            resolved_task_ref,
            floor_taken,
            elapsed_ms,
            open_ms,
            query_ms,
            candidates,
            resolver_exception_class,
        ) = _resolution_fields(result)
        timing = {"elapsed_ms": elapsed_ms, "open_ms": open_ms, "query_ms": query_ms}
        if floor_taken or resolved_task_ref is None:
            remaining_ms = max(0, 2000 - elapsed_ms)
            done_roster = _sole_done_roster(db_path, remaining_ms=remaining_ms)
            if done_roster is None:
                floor_taken = True
                resolved_task_ref = None
                fallback_fields: dict[str, object] = dict(timing)
                if resolver_exception_class is not None:
                    fallback_fields["exception_class"] = resolver_exception_class
                _receipt("roster_fallback", **fallback_fields)
            else:
                floor_taken = False
                resolved_task_ref, status, slugs, domains = done_roster
                _receipt("roster_resolved", task_ref=resolved_task_ref, **timing)
        else:
            status, slugs, domains = _read_live_roster(db_path, resolved_task_ref)
            _receipt("roster_resolved", task_ref=resolved_task_ref, **timing)
            if candidates:
                _receipt("tiebreak", candidates=candidates, task_ref=resolved_task_ref)
    except (ValueError, sqlite3.Error, OSError) as exc:
        floor_taken = True
        resolved_task_ref = None
        elapsed_ms = locals().get("elapsed_ms", 0)
        _receipt("roster_fallback", elapsed_ms=elapsed_ms, exception_class=type(exc).__name__)

    if not floor_taken and not slugs:
        default = DEFAULT_SKILLS_BY_STATUS.get(status)
        if default is None:
            floor_taken = True
            resolved_task_ref = None
            _receipt(
                "roster_fallback",
                elapsed_ms=locals().get("elapsed_ms", 0),
                reason="unknown_status",
            )
        else:
            slugs = list(default)
    if floor_taken:
        slugs = list(DEFAULT_SKILLS_BY_STATUS["in_progress"])
        domains = []

    declared: set[str] = set()
    _audit_source_classification(slugs)
    for slug in slugs:
        for tool_name in SLUG_MCP_TOOLS.get(slug, ()):
            if tool_name in HANDOFF_TOOL_NAMES:
                declared.add(tool_name)
            elif tool_name in ORCHESTRATOR_TOOL_NAMES:
                _receipt("roster_tool_skipped", reason="other_server", tool=tool_name, slug=slug)
            else:
                _receipt("roster_tool_skipped", reason="unknown_name", tool=tool_name, slug=slug)

    activated: set[str] = set()
    for domain in domains:
        for tool_name in domain_tool_map.get(domain, ()):
            if tool_name in HANDOFF_TOOL_NAMES:
                activated.add(tool_name)
            elif tool_name in ORCHESTRATOR_TOOL_NAMES:
                _receipt("roster_tool_skipped", reason="other_server", tool=tool_name, domain=domain)
            else:
                _receipt("roster_tool_skipped", reason="unknown_name", tool=tool_name, domain=domain)

    served = CATALOG_TOOLS | (ALWAYS_SERVE & HANDOFF_TOOL_NAMES) | declared | activated
    return BootRoster("skill", resolved_task_ref, floor_taken, frozenset(served))
