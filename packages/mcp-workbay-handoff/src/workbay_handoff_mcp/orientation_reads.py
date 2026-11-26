from __future__ import annotations

import os
import sqlite3
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

from .compaction_contract import normalize_compaction_harness
from .shared_schema import _get_db_connection

# internal: orientation-read instrumentation is opt-IN and gated on a
# process-local boundary flag rather than a public function parameter.
# The MCP tool dispatch wrapper (``_wrap_branch_mismatch_for_mcp``) enters
# this boundary for the four read tools, and the reinject hook enters it
# explicitly around its ``get_handoff_state`` call. Every other in-process
# caller (dashboard/close-check/hook-resolver/cross-package readers, the
# internal ``load_session`` -> ``get_handoff_state`` delegation, and the
# ``close_slice`` -> ``render_handoff`` current_task refresh) runs with the
# flag OFF and therefore records no telemetry. Driving the switch through a
# ``ContextVar`` keeps it off the public tool input schema, so a caller
# cannot pass a parameter to force-enable or force-disable recording.
_ORIENTATION_READ_BOUNDARY: ContextVar[bool] = ContextVar("workbay_orientation_read_boundary", default=False)


@contextmanager
def orientation_read_boundary() -> Iterator[None]:
    """Mark the enclosed call as a genuine orientation-read tool boundary.

    Entered by the MCP dispatch wrapper for the four read tools and by the
    reinject hook. Recording sites fire only while this boundary is active.
    """
    token = _ORIENTATION_READ_BOUNDARY.set(True)
    try:
        yield
    finally:
        _ORIENTATION_READ_BOUNDARY.reset(token)


@contextmanager
def suppress_orientation_read_boundary() -> Iterator[None]:
    """Temporarily clear the boundary around a delegated internal read.

    ``load_session`` uses this around its internal ``get_handoff_state``
    call so the compound read records exactly one ``load_session`` row
    instead of double-counting a nested ``get_handoff_state`` row.
    """
    token = _ORIENTATION_READ_BOUNDARY.set(False)
    try:
        yield
    finally:
        _ORIENTATION_READ_BOUNDARY.reset(token)


def orientation_read_boundary_active() -> bool:
    """Return True when the current call is inside an orientation-read boundary."""
    return _ORIENTATION_READ_BOUNDARY.get()


def _normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _normalize_harness_slug(value: str) -> str:
    slug = value.strip().lower().replace("-", "_").replace(" ", "_")
    return slug or "unknown"


# Presence-only harness markers that carry no canonical slug (e.g. Claude Code
# exports ``CLAUDECODE='1'``; Antigravity exports ``ANTIGRAVITY_AGENTAPI_EXE``
# / ``ANTIGRAVITY_SOURCE_METADATA``).
_HARNESS_FLAG_CANONICAL: tuple[tuple[str, str], ...] = (
    ("ANTIGRAVITY_AGENTAPI_EXE", "agy-cli"),
    ("ANTIGRAVITY_SOURCE_METADATA", "agy-cli"),
    ("CLAUDECODE", "claude-code"),
)


def _normalize_canonical_harness(value: str) -> str:
    try:
        return normalize_compaction_harness(value)
    except ValueError:
        slug = value.strip().lower().replace("_", "-").replace(" ", "-")
        return slug or "unknown"


def _infer_harness_from_env_contract(env: Mapping[str, str] | None = None) -> str | None:
    """Env-derived harness slug via the shared compaction-contract normalizer."""
    try:
        from .shared_write_context import _infer_harness_agent_from_env  # noqa: PLC0415

        return _infer_harness_agent_from_env(env=env)
    except Exception:  # noqa: BLE001 -- never fail a read on harness detection
        return None


def detect_caller_harness(env: Mapping[str, str] | None = None) -> str:
    """Detect the canonical caller harness identifier (hyphenated, e.g. ``agy-cli``).

    Precedence: explicit ``WORKBAY_HARNESS``/``CODEX_HARNESS`` override (normalized
    through the compaction harness alias table so ``agy``/``agy-app``/``agy-ide``
    resolve to ``agy-cli``) → transcript-var/identity-marker contract detection →
    presence-only runtime markers (``ANTIGRAVITY_AGENTAPI_EXE``,
    ``ANTIGRAVITY_SOURCE_METADATA``, ``CLAUDECODE``) → ``'unknown'``.
    """
    source = env if env is not None else os.environ
    explicit = _normalize_optional(source.get("WORKBAY_HARNESS")) or _normalize_optional(
        source.get("CODEX_HARNESS")
    )
    if explicit:
        return _normalize_canonical_harness(explicit)
    inferred = _infer_harness_from_env_contract(source)
    if inferred:
        return _normalize_canonical_harness(inferred)
    for env_var, canonical in _HARNESS_FLAG_CANONICAL:
        if _normalize_optional(source.get(env_var)):
            return canonical
    return "unknown"


def resolve_harness() -> str:
    """Resolve a harness *slug* for the telemetry row.

    Delegates to :func:`detect_caller_harness` and normalizes to a hyphen-free
    underscore slug so a boolean flag never leaks its literal value into the
    ``harness`` column.
    """
    return _normalize_harness_slug(detect_caller_harness())


def record_orientation_read(
    conn: sqlite3.Connection,
    *,
    tool: str,
    task_ref: str,
    resolution_outcome: str,
    harness: str | None = None,
    source: str | None = None,
    session: str | None = None,
    read_profile: str | None = None,
) -> None:
    """Persist one orientation read row. Caller owns commit; failures are fail-open.

    This low-level writer does not check the boundary flag — callers gate on
    :func:`orientation_read_boundary_active` before invoking it (or invoke it
    directly to seed rows in tests/fixtures).
    """
    try:
        conn.execute(
            """
            INSERT INTO orientation_reads (
                tool, task_ref, resolution_outcome, harness, source, session, read_profile
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tool,
                task_ref,
                resolution_outcome,
                _normalize_optional(harness) or resolve_harness(),
                _normalize_optional(source),
                _normalize_optional(session),
                _normalize_optional(read_profile),
            ),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"workbay orientation read telemetry failed: {exc}", file=sys.stderr)


def record_orientation_read_once(
    *,
    tool: str,
    task_ref: str | None,
    resolution_outcome: str,
    source: str | None = None,
    session: str | None = None,
    read_profile: str | None = None,
) -> None:
    """Open-a-connection convenience writer used by the compound read tools.

    No-ops unless the current call is inside an orientation-read boundary, so
    internal invocations of ``render_handoff``/``load_session``/
    ``semantic_reinjection_packet`` (e.g. the ``close_slice`` current_task
    refresh) never leave a telemetry row.
    """
    if not orientation_read_boundary_active():
        return
    if not task_ref:
        return
    try:
        with _get_db_connection() as conn:
            record_orientation_read(
                conn,
                tool=tool,
                task_ref=task_ref,
                resolution_outcome=resolution_outcome,
                source=source,
                session=session,
                read_profile=read_profile,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"workbay orientation read telemetry failed: {exc}", file=sys.stderr)
