"""Compaction helpers.

internal's compaction surfaces (Stop hook, advisory, ``session_compactions``
rows) run alongside the host harness's built-in context compaction — they
are not a replacement for it. The two mechanisms solve different problems
(in-conversation context pressure vs. durable cross-session retained
context). See
``packages/mcp-workbay-handoff/docs/explainers/compaction-vs-default-harness-compaction.md``
for the operator-facing comparison and the explicit note on what the
``test_compression_ratio.py`` benchmark actually measures.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel, Field, field_validator
from workbay_protocol import StructuredSummary, TurnRange

from .compaction_contract import (
    CompactionContract,
    CompactionContractHarness,
    TranscriptDiscoveryRule,
    build_contract_source_report,
    detect_active_harness,
    load_compaction_contract,
    normalize_compaction_harness,
    resolve_effective_thresholds,
    resolve_min_new_tokens_gate_with_fallback,
)
from .concept_embed_hook import embed_compaction_anchor_on_write, embed_concept_on_write
from .review_findings import list_review_findings
from .shared_primitives import _resolve_task_ref
from .shared_schema import _get_db_connection
from .touched_files import get_touched_files
from .verified_tests import get_verified_tests

_log = logging.getLogger("workbay_handoff_mcp")

CompactionHarness = Literal["claude-code", "codex", "grok", "vscode", "manual"]
CompactionHarnessInput = Literal["claude-code", "codex", "grok", "vscode", "manual", "cursor"]
COMPACTION_HARNESS_CHOICES: tuple[str, ...] = ("claude-code", "codex", "grok", "vscode", "manual")
COMPACTION_HARNESS_INPUT_CHOICES: tuple[str, ...] = (
    "claude-code",
    "codex",
    "grok",
    "vscode",
    "manual",
    "cursor",
)
PROSE_RESIDUAL_SOFT_LIMIT_CHARS = 4096
PROSE_RESIDUAL_HARD_LIMIT_CHARS = 16384
DEFAULT_MAX_TRANSCRIPT_BYTES = 52_428_800
DEFAULT_MAX_EXTRACT_CHARS = 400_000
TRANSCRIPT_CLIP_MARKER_PREFIX = "[transcript clipped:"
TURN_NUMBER_RE = re.compile(r"\bturn\s+(\d+)\b", re.IGNORECASE)
_JSONL_DROP_RECORD_TYPES = frozenset(
    {
        "attachment",
        "file-history-snapshot",
        "last-prompt",
        "mode",
        "permission-mode",
        "progress",
        "queue-operation",
        "system",
    }
)
_JSONL_TURN_RECORD_TYPES = frozenset({"user", "assistant"})

DEFAULT_MIN_NEW_TURNS = 0
# Emergency fallback for the Stop-hook token gate, used ONLY when the canonical
# compaction contract (harness-protocol.yaml `compaction.threshold_tokens`) cannot
# be loaded (missing/unreadable workspace). The contract is the single source of
# truth for the tunable gate default; this constant is intentionally decoupled and
# need NOT track the contract value — do not re-add an equality assertion against
# it. Resolution order: env > overlay > contract > this constant.
DEFAULT_MIN_NEW_TOKENS = 75_000

_COMPACTION_ENV_FIELDS: tuple[tuple[str, str], ...] = (
    (
        "disabled",
        "WORKBAY_HANDOFF_COMPACTION_DISABLED",
    ),
    (
        "compaction_notify",
        "WORKBAY_HANDOFF_COMPACTION_NOTIFY",
    ),
    (
        "min_new_tokens",
        "WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS",
    ),
    (
        "max_transcript_bytes",
        "WORKBAY_HANDOFF_COMPACTION_MAX_TRANSCRIPT_BYTES",
    ),
    (
        "max_extract_chars",
        "WORKBAY_HANDOFF_COMPACTION_MAX_EXTRACT_CHARS",
    ),
)

_FALSY_FLAG_VALUES: frozenset[str] = frozenset({"", "0", "false", "no", "off"})


def _coerce_truthy_flag(value: str) -> bool:
    """Coerce an env string to a boolean flag (truthy unless a falsy token).

    Polarity-neutral: shared by both the ``disabled`` gate and the
    enable-style ``compaction_notify`` knob, so the name must not imply a
    direction. Caller decides what True means for its field.
    """
    return value.strip().lower() not in _FALSY_FLAG_VALUES


def _env_disables_compaction(env: Mapping[str, str]) -> bool:
    raw = env.get("WORKBAY_HANDOFF_COMPACTION_DISABLED", "")
    return raw != "" and _coerce_truthy_flag(raw)


class CompactionSettings(BaseModel):
    """Typed config surface for the compaction hook and library.

    Reads ``WORKBAY_HANDOFF_COMPACTION_*`` env vars at the boundary;
    Bad values raise ``pydantic.ValidationError`` so typos become loud failures
    rather than silent default fallbacks.
    """

    disabled: bool = False
    compaction_notify: bool = True
    min_new_turns: int = Field(default=DEFAULT_MIN_NEW_TURNS, ge=0)
    min_new_tokens: int = Field(default=DEFAULT_MIN_NEW_TOKENS, ge=0)
    max_transcript_bytes: int = Field(default=DEFAULT_MAX_TRANSCRIPT_BYTES, ge=1)
    max_extract_chars: int = Field(default=DEFAULT_MAX_EXTRACT_CHARS, ge=1)

    @field_validator("min_new_turns", "min_new_tokens", "max_transcript_bytes", "max_extract_chars", mode="before")
    @classmethod
    def _strip_int_strings(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        workspace_root: str | Path | None = None,
    ) -> "CompactionSettings":
        source = env if env is not None else os.environ
        fields: dict[str, object] = {}
        for field_name, canonical in _COMPACTION_ENV_FIELDS:
            raw = source.get(canonical, "")
            if raw == "":
                continue
            if field_name in {"disabled", "compaction_notify"}:
                fields[field_name] = _coerce_truthy_flag(raw)
            else:
                fields[field_name] = raw

        if "min_new_tokens" not in fields:
            resolved_root: Path | None = None
            if workspace_root is not None:
                resolved_root = Path(workspace_root).expanduser().resolve()
            else:
                try:
                    from .runtime import get_runtime_config  # noqa: PLC0415

                    resolved_root = get_runtime_config().compaction_config_root
                except RuntimeError:
                    resolved_root = None
            gate = resolve_min_new_tokens_gate_with_fallback(
                env=source,
                workspace_root=resolved_root,
                constant_fallback=DEFAULT_MIN_NEW_TOKENS,
            )
            fields["min_new_tokens"] = gate.value

        return cls.model_validate(fields)


DisabledSource = Literal["env", "db"]


class CompactionDisabledRow(BaseModel):
    """Row from ``compaction_settings`` exposed to status callers."""

    scope_kind: Literal["task", "workspace"]
    task_ref: str | None
    enabled: bool
    updated_at: str
    updated_by: str | None


class CompactionStatusReceipt(BaseModel):
    """Return shape for ``compaction(operation='status', task_ref=...)``.

    ``env_override`` is ``True`` when the env var is forcing the disabled
    state regardless of any db row, mirroring internal's ``thresholds_source``
    disclosure pattern.
    """

    disabled: bool
    source: DisabledSource | None
    env_override: bool
    db_row: CompactionDisabledRow | None


class CompactionRecord(BaseModel):
    """Read model for a persisted compaction row."""

    summary: StructuredSummary
    tokens_saved_estimate: int | None = None


class CompactionRecordReceipt(BaseModel):
    """Return shape for ``compaction(operation='record', ...)`` (internal).

    The receipt **inlines** the canonical ``StructuredSummary`` rather than
    duplicating its counts (internal fix). Callers read counts as
    ``len(receipt.summary.decisions)``, ``len(receipt.summary.files_touched)``,
    etc.

    ``tokens_saved_estimate`` reuses the ``chars / 4`` divisor documented at
    ``packages/workbay-system/workbay_system/payload/docs/workbay/contracts/harness-protocol.yaml``
    lines 126-127 (internal fix) on slimmed ``input_chars`` only — ``raw_input_bytes``
    is informational for operator receipts and is not part of the divisor math.
    The estimate is clamped non-negative so a summary that ends up larger than the
    input transcript does not surface a negative number.

    ``db_row_id`` is the ``lastrowid`` captured from the ``INSERT`` cursor on
    the ``session_compactions`` table — useful for cross-referencing the
    receipt against the persisted row in tests and reviews.

    ``raw_input_bytes`` is the on-disk transcript size before JSONL slimming or
    extract-char clipping so ``tokens_saved_estimate`` stays interpretable when
    ``input_chars`` reflects the slimmed extractor input. Stop-hook/CLI receipt
    lines emit it immediately after ``input_chars``.
    """

    compaction_id: str
    summary: StructuredSummary
    input_chars: int
    raw_input_bytes: int
    summary_chars: int
    prose_residual_chars: int
    tokens_saved_estimate: int
    db_row_id: int


class CompactionAdvisoryThresholds(BaseModel):
    """Integer-valued threshold pair used for both ``thresholds`` and ``observed``."""

    tokens: int | None = None
    chars: int | None = None


class CompactionAdvisoryThresholdSources(BaseModel):
    """String-valued provenance pair for ``thresholds_source`` (enabled branch only)."""

    tokens: str | None = None
    chars: str | None = None


class CompactionAdvisoryTranscript(BaseModel):
    """Transcript locator pair returned under ``transcript``."""

    path: str | None = None
    source: str | None = None


class CompactionAdvisoryMetrics(BaseModel):
    """[OBS-08] measure-only advisory size/frequency telemetry (T19).

    Thresholds are not tuned from these fields; they exist so operators can
    observe advisory payload cost before changing contract thresholds.
    """

    advisory_bytes: int | None = None
    observed_chars: int | None = None
    observed_tokens: int | None = None
    recommended: bool = False


class CompactionAdvisory(BaseModel):
    """Return shape for ``compute_compaction_advisory`` (internal envelope).

    Callers historically consumed the dict form; ``compute_compaction_advisory``
    serialises with ``.model_dump(mode='json')`` to preserve that contract.
    """

    recommended: bool = False
    recommended_action: str | None = None
    thresholds: CompactionAdvisoryThresholds = Field(default_factory=CompactionAdvisoryThresholds)
    thresholds_source: CompactionAdvisoryThresholdSources | None = None
    observed: CompactionAdvisoryThresholds = Field(default_factory=CompactionAdvisoryThresholds)
    harness: str | None = None
    transcript: CompactionAdvisoryTranscript = Field(default_factory=CompactionAdvisoryTranscript)
    contract_source: dict | None = None
    latest_compaction_id: str | None = None
    disabled: bool = False
    disabled_source: DisabledSource | None = None
    warnings: list[str] = Field(default_factory=list)
    # [OBS-08] measure-only; do not drive threshold tuning from this field yet.
    metrics: CompactionAdvisoryMetrics | None = None


COMPACTION_RECORD_RECEIPT_OPERATOR_FIELDS: tuple[str, ...] = (
    "tokens_saved_estimate",
    "input_chars",
    "raw_input_bytes",
    "summary_chars",
    "prose_residual_chars",
)


def format_compaction_record_receipt_lines(receipt: CompactionRecordReceipt) -> list[str]:
    """Return the stable operator-facing receipt lines for a recorded compaction."""
    lines = [f"compaction_id={receipt.compaction_id}"]
    for field in COMPACTION_RECORD_RECEIPT_OPERATOR_FIELDS:
        lines.append(f"{field}={getattr(receipt, field)}")
    return lines


def format_compaction_notify_message(receipt: CompactionRecordReceipt) -> str:
    """Return the one-line user-visible compaction notification."""
    turn_range = receipt.summary.turn_range
    return (
        f"workbay: compacted {receipt.compaction_id} "
        f"(turns {turn_range.start_turn}-{turn_range.end_turn}), "
        f"~{receipt.tokens_saved_estimate} tokens saved"
    )


def format_compaction_stop_notify_stdout(message: str) -> str:
    """Return the Claude Stop-hook stdout JSON envelope for a notify line."""
    return json.dumps({"systemMessage": message})


def format_reinject_notify_message(
    *,
    task_ref: str,
    compaction_id: str | None,
    start_turn: int | None,
    end_turn: int | None,
    source: str | None = None,
    semantic_status: str | None = None,
    semantic_skip_reason: str | None = None,
    selected_count: int | None = None,
    selected_kinds: Sequence[str] | None = None,
    score_hi: float | None = None,
    score_lo: float | None = None,
    chars_used: int | None = None,
    chars_budget: int | None = None,
    max_chars: int | None = None,
) -> str:
    """Return the one-line user-visible reinject notification."""
    parts: list[str] = []
    if semantic_status or semantic_skip_reason or selected_count is not None:
        if source:
            parts.append(f"source={source}")
        parts.append(f"task={task_ref}")
        if compaction_id is not None and start_turn is not None and end_turn is not None:
            parts.append(f"compaction={compaction_id} turns {start_turn}-{end_turn}")
        if semantic_status:
            parts.append(f"semantic={semantic_status}")
        if semantic_skip_reason:
            parts.append(f"skip={semantic_skip_reason}")
        if selected_count is not None:
            kind_text = ",".join(sorted(set(selected_kinds or [])))
            if kind_text:
                parts.append(f"selected={selected_count} kinds={kind_text}")
            else:
                parts.append(f"selected={selected_count}")
        if score_hi is not None and score_lo is not None:
            parts.append(f"rel~{score_hi:.2f}-{score_lo:.2f}")
        if chars_used is not None and chars_budget is not None:
            parts.append(f"chars={chars_used}/{chars_budget}")
        message = "workbay: reinject " + " ".join(parts)
        if max_chars is not None and len(message) > max_chars:
            trimmed = message[: max(0, max_chars - 3)].rstrip()
            while trimmed and trimmed[-1] not in " )":
                trimmed = trimmed[:-1]
            message = trimmed + "..."
        return message
    if compaction_id is not None and start_turn is not None and end_turn is not None:
        return f"workbay: re-fed compaction {compaction_id} (turns {start_turn}-{end_turn}) for {task_ref}"
    return f"workbay: re-fed handoff context for {task_ref}"


def format_reinject_session_start_stdout(*, block: str, system_message: str) -> str:
    """Return the Claude SessionStart stdout JSON envelope for reinjection."""
    return json.dumps(
        {
            "hookSpecificOutput": {
                "hookEventName": "SessionStart",
                "additionalContext": block,
            },
            "systemMessage": system_message,
        }
    )


def reinject_json_envelope_overhead_chars(*, block: str, system_message: str) -> int:
    """Return JSON wrapper bytes beyond ``block`` for budget reservation."""
    wrapped = format_reinject_session_start_stdout(block=block, system_message=system_message)
    return len(wrapped) - len(block)


def _ensure_compaction_settings_table(conn: sqlite3.Connection) -> None:
    """Create ``compaction_settings`` if it does not exist.

    The HANDOFF_SCHEMA_SQL bootstrap and the warm-start migration both
    create this table, but disable/enable/status callers may be invoked
    before the warm-start migration when running against very old DBs.
    This helper makes the runtime write-path tolerant.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compaction_settings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_kind  TEXT NOT NULL CHECK (scope_kind IN ('task', 'workspace')),
            task_ref    TEXT,
            enabled     INTEGER NOT NULL CHECK (enabled IN (0, 1)),
            updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_by  TEXT
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_compaction_settings_scope "
        "ON compaction_settings(scope_kind, COALESCE(task_ref, ''))"
    )


def get_compaction_disabled_row(
    conn: sqlite3.Connection,
    *,
    scope_kind: Literal["task", "workspace"],
    task_ref: str | None,
) -> CompactionDisabledRow | None:
    """Return the matching compaction_settings row, or ``None`` if absent."""

    _ensure_compaction_settings_table(conn)
    if scope_kind == "workspace":
        row = conn.execute(
            "SELECT scope_kind, task_ref, enabled, updated_at, updated_by "
            "FROM compaction_settings WHERE scope_kind = 'workspace'"
        ).fetchone()
    else:
        if task_ref is None:
            return None
        row = conn.execute(
            "SELECT scope_kind, task_ref, enabled, updated_at, updated_by "
            "FROM compaction_settings WHERE scope_kind = 'task' AND task_ref = ?",
            (task_ref,),
        ).fetchone()
    if row is None:
        return None
    return CompactionDisabledRow(
        scope_kind=str(row["scope_kind"]),  # type: ignore[arg-type]
        task_ref=row["task_ref"] if row["task_ref"] is not None else None,
        enabled=bool(int(row["enabled"])),
        updated_at=str(row["updated_at"]),
        updated_by=str(row["updated_by"]) if row["updated_by"] is not None else None,
    )


def upsert_compaction_disabled(
    conn: sqlite3.Connection,
    *,
    scope_kind: Literal["task", "workspace"],
    task_ref: str | None,
    enabled: bool,
    actor: str | None = None,
) -> CompactionDisabledRow:
    """Upsert a compaction_settings row keyed by (scope_kind, task_ref).

    Uses ``ON CONFLICT (scope_kind, COALESCE(task_ref,'')) DO UPDATE``
    so the workspace-default row is a singleton and repeated task-scoped
    writes for the same task_ref refresh in place.
    """

    _ensure_compaction_settings_table(conn)
    if scope_kind == "task" and not task_ref:
        raise ValueError("task_ref is required when scope_kind='task'")
    if scope_kind == "workspace":
        task_ref = None
    now_iso = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO compaction_settings (scope_kind, task_ref, enabled, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (scope_kind, COALESCE(task_ref, '')) DO UPDATE SET
            enabled = excluded.enabled,
            updated_at = excluded.updated_at,
            updated_by = excluded.updated_by
        """,
        (scope_kind, task_ref, 1 if enabled else 0, now_iso, actor),
    )
    refreshed = get_compaction_disabled_row(conn, scope_kind=scope_kind, task_ref=task_ref)
    if refreshed is None:  # pragma: no cover -- defensive
        raise RuntimeError("compaction_settings upsert returned no row")
    return refreshed


def resolve_compaction_disabled(
    *,
    env: Mapping[str, str],
    conn: sqlite3.Connection,
    task_ref: str | None,
) -> tuple[bool, DisabledSource | None]:
    """Return ``(disabled, source)`` for the unified internal disable surface.

    Precedence (first match wins):
    1. ``WORKBAY_HANDOFF_COMPACTION_DISABLED`` env var.
      2. ``compaction_settings`` row with ``scope_kind='task'`` and a
         matching ``task_ref`` and ``enabled=0``.
      3. ``compaction_settings`` row with ``scope_kind='workspace'`` and
         ``enabled=0``.
      4. Otherwise ``(False, None)``.

    The caller is expected to pass its existing SQLite connection so
    that disable lookups do not double-open the DB (internal).
    """

    if _env_disables_compaction(env):
        return True, "env"

    _ensure_compaction_settings_table(conn)
    if task_ref:
        task_row = get_compaction_disabled_row(conn, scope_kind="task", task_ref=task_ref)
        if task_row is not None and not task_row.enabled:
            return True, "db"
    workspace_row = get_compaction_disabled_row(conn, scope_kind="workspace", task_ref=None)
    if workspace_row is not None and not workspace_row.enabled:
        return True, "db"
    return False, None


def compute_compaction_status(
    *,
    env: Mapping[str, str],
    conn: sqlite3.Connection,
    task_ref: str | None,
) -> CompactionStatusReceipt:
    """Return the structured ``CompactionStatusReceipt`` envelope."""

    env_override = _env_disables_compaction(env)

    db_row: CompactionDisabledRow | None = None
    if task_ref:
        db_row = get_compaction_disabled_row(conn, scope_kind="task", task_ref=task_ref)
    if db_row is None:
        db_row = get_compaction_disabled_row(conn, scope_kind="workspace", task_ref=None)

    disabled, source = resolve_compaction_disabled(env=env, conn=conn, task_ref=task_ref)
    return CompactionStatusReceipt(
        disabled=disabled,
        source=source,
        env_override=env_override,
        db_row=db_row,
    )


def set_compaction_enabled(
    *,
    enabled: bool,
    task_ref: str | None,
    actor: str | None = None,
) -> CompactionStatusReceipt:
    """Top-level helper for the MCP/CLI disable/enable ops.

    Opens its own short-lived connection. Returns the resolved status
    receipt so callers can echo the post-write state back to the user.
    """

    scope_kind: Literal["task", "workspace"] = "task" if task_ref else "workspace"
    with _get_db_connection() as conn:
        upsert_compaction_disabled(
            conn,
            scope_kind=scope_kind,
            task_ref=task_ref,
            enabled=enabled,
            actor=actor,
        )
        conn.commit()
        return compute_compaction_status(env=os.environ, conn=conn, task_ref=task_ref)


def get_compaction_status(task_ref: str | None) -> CompactionStatusReceipt:
    """Top-level helper for the ``status`` MCP op."""

    with _get_db_connection() as conn:
        return compute_compaction_status(env=os.environ, conn=conn, task_ref=task_ref)


def _parse_changed_files_json(raw_value: object) -> list[str]:
    if not isinstance(raw_value, str) or raw_value.strip() == "":
        return []
    try:
        decoded = json.loads(raw_value)
    except JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [item for item in decoded if isinstance(item, str) and item]


class _CompactionQueries:
    """Per-task query bundle binding ``(conn, task_ref)`` for compaction helpers.

    Fowler "Combine Functions into Class" (p. 144): the four primitive
    helpers below all carried the same ``(conn, task_ref)`` clump. Binding
    it once at construction removes the repeated parameters at every call
    site and groups related SQL by domain.
    """

    def __init__(self, conn: sqlite3.Connection, task_ref: str) -> None:
        self._conn = conn
        self._task_ref = task_ref

    def load_task_decisions(self) -> list[dict[str, object]]:
        rows = self._conn.execute(
            "SELECT decision, changed_files_json FROM decisions WHERE task_ref = ? ORDER BY created_at ASC, id ASC",
            (self._task_ref,),
        ).fetchall()
        decisions: list[dict[str, object]] = []
        for row in rows:
            decisions.append(
                {
                    "decision_id": str(row["decision"]),
                    "slug": str(row["decision"]),
                    "changed_files": _parse_changed_files_json(row["changed_files_json"]),
                }
            )
        return decisions

    def next_compaction_id(self) -> str:
        # MAX(compaction_id) is safe because the suffix is fixed-width zero-padded;
        # if that invariant changes, this query must change with it.
        row = self._conn.execute(
            "SELECT MAX(compaction_id) AS compaction_id FROM session_compactions WHERE task_ref = ?",
            (self._task_ref,),
        ).fetchone()
        if row is None or row["compaction_id"] is None:
            next_suffix = 1
        else:
            raw_compaction_id = str(row["compaction_id"])
            suffix_text = raw_compaction_id.rsplit("-", 1)[-1]
            try:
                next_suffix = int(suffix_text) + 1
            except ValueError as exc:
                raise ValueError(
                    f"Malformed compaction_id stored for task {self._task_ref}: {raw_compaction_id}"
                ) from exc
        if next_suffix > 9999:
            raise ValueError(f"compaction_id suffix overflow for task {self._task_ref}")
        return f"C-{self._task_ref}-{next_suffix:04d}"

    def count_decisions_after(self, after: datetime) -> int:
        # Stored compaction created_at is ISO with 'T'/'Z'; decisions.created_at is
        # the SQLite default 'YYYY-MM-DD HH:MM:SS'. Compare as ISO strings after
        # normalizing the compaction value to the same shape.
        cutoff = after.strftime("%Y-%m-%d %H:%M:%S")
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM decisions WHERE task_ref = ? AND created_at > ?",
            (self._task_ref, cutoff),
        ).fetchone()
        return int(row["n"]) if row is not None else 0

    def observed_token_total(self, since: datetime | None) -> int | None:
        # Exclude 'grok_context_delta' rows: they measure context-fill delta (a
        # different unit than observed input/output usage) and would inflate the
        # compaction advisory total. NULL usage_source = legacy observed rows.
        source_filter = "(usage_source IS NULL OR usage_source != 'grok_context_delta')"
        if since is None:
            row = self._conn.execute(
                f"SELECT COALESCE(SUM(total_tokens), 0) AS s FROM turn_metrics WHERE task_ref = ? AND {source_filter}",
                (self._task_ref,),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(total_tokens), 0) AS s FROM turn_metrics "
                f"WHERE task_ref = ? AND created_at > ? AND {source_filter}",
                (self._task_ref, since.strftime("%Y-%m-%d %H:%M:%S")),
            ).fetchone()
        if row is None:
            return None
        return int(row["s"] or 0)


class _ReinjectionQueries:
    def __init__(self, conn: sqlite3.Connection, task_ref: str) -> None:
        self._conn = conn
        self._task_ref = task_ref

    def next_reinjection_id(self) -> str:
        row = self._conn.execute(
            "SELECT MAX(reinjection_id) AS reinjection_id FROM session_reinjections WHERE task_ref = ?",
            (self._task_ref,),
        ).fetchone()
        if row is None or row["reinjection_id"] is None:
            next_suffix = 1
        else:
            raw_reinjection_id = str(row["reinjection_id"])
            suffix_text = raw_reinjection_id.rsplit("-", 1)[-1]
            try:
                next_suffix = int(suffix_text) + 1
            except ValueError as exc:
                raise ValueError(
                    f"Malformed reinjection_id stored for task {self._task_ref}: {raw_reinjection_id}"
                ) from exc
        if next_suffix > 9999:
            raise ValueError(f"reinjection_id suffix overflow for task {self._task_ref}")
        return f"R-{self._task_ref}-{next_suffix:04d}"


def _serialize_semantic_detail_json(semantic_detail: Mapping[str, object] | str) -> str:
    if isinstance(semantic_detail, str):
        raise TypeError("semantic_detail must be a structured mapping, not a pre-authored JSON string")
    if not isinstance(semantic_detail, Mapping):
        raise TypeError("semantic_detail must be a structured mapping")
    try:
        return json.dumps(semantic_detail, sort_keys=True, separators=(",", ":"))
    except TypeError as exc:
        raise TypeError("semantic_detail must be JSON-serializable") from exc


def session_reinjection_exists(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    compaction_id: str,
) -> bool:
    """Return whether successful semantic reinjection already ran.

    Sticky only for rows whose ``semantic_detail_json.status`` is
    ``selected`` **and** that carry emission evidence (non-empty
    ``selected`` array and/or ``chars_used`` > 0). Generic
    resume/startup/compact telemetry (null detail), non-selected attempts
    (``skipped`` / ``degraded``), and bare ``status=selected`` shells
    without payload must not suppress a later compact semantic arm for
    the same ``compaction_id``.
    """
    row = conn.execute(
        """
        SELECT 1 FROM session_reinjections
        WHERE task_ref = ?
          AND compaction_id = ?
          AND semantic_detail_json IS NOT NULL
          AND json_extract(semantic_detail_json, '$.status') = 'selected'
          AND (
            COALESCE(json_array_length(semantic_detail_json, '$.selected'), 0) > 0
            OR COALESCE(json_extract(semantic_detail_json, '$.chars_used'), 0) > 0
          )
        LIMIT 1
        """,
        (task_ref, compaction_id),
    ).fetchone()
    return row is not None


def record_session_reinjection(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    harness: str,
    task_ref: str,
    compaction_id: str | None,
    source: str,
    emitted_chars: int,
    arm: str | None = None,
    semantic_detail: Mapping[str, object] | None = None,
) -> str:
    """Persist one reinject emission row. Caller owns commit."""
    semantic_detail_json: str | None = None
    if semantic_detail is not None:
        semantic_detail_json = _serialize_semantic_detail_json(semantic_detail)
    conn.execute("BEGIN IMMEDIATE")
    reinjection_id = _ReinjectionQueries(conn, task_ref).next_reinjection_id()
    conn.execute(
        """
        INSERT INTO session_reinjections (
            reinjection_id, session_id, harness, task_ref, compaction_id,
            source, emitted_chars, arm, semantic_detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            reinjection_id,
            session_id,
            harness,
            task_ref,
            compaction_id,
            source,
            emitted_chars,
            arm,
            semantic_detail_json,
        ),
    )
    return reinjection_id


def _ordered_unique(items: list[str]) -> list[str]:
    return list(OrderedDict.fromkeys(items))


def _validate_harness(harness: str) -> CompactionHarness:
    return normalize_compaction_harness(harness)


def _iter_jsonl_turn_records(transcript: str):
    ordinal = 0
    for line in transcript.splitlines(keepends=True):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        if record.get("type") not in _JSONL_TURN_RECORD_TYPES:
            continue
        ordinal += 1
        yield ordinal, line


def _uses_jsonl_turn_accounting(transcript: str) -> bool:
    """True when turn math should use JSONL user/assistant ordinals."""
    if not _looks_like_jsonl(transcript):
        return False
    for _ordinal, _line in _iter_jsonl_turn_records(transcript):
        return True
    return False


def slice_new_turn_transcript(transcript: str, *, since_turn: int) -> str:
    """Return transcript text for turns strictly after ``since_turn``.

    JSONL transcripts slice on user/assistant record ordinals; prose
    transcripts keep the legacy ``turn N`` regex path.
    """
    if since_turn <= 0:
        return transcript
    if _uses_jsonl_turn_accounting(transcript):
        return "".join(line for ordinal, line in _iter_jsonl_turn_records(transcript) if ordinal > since_turn)
    out: list[str] = []
    keeping = False
    for line in transcript.splitlines(keepends=True):
        match = TURN_NUMBER_RE.search(line)
        if match and int(match.group(1)) > since_turn:
            keeping = True
        if keeping:
            out.append(line)
    return "".join(out)


def _derive_turn_range(transcript: str) -> TurnRange:
    if _uses_jsonl_turn_accounting(transcript):
        ordinals = [ordinal for ordinal, _ in _iter_jsonl_turn_records(transcript)]
        return TurnRange(start_turn=min(ordinals), end_turn=max(ordinals))
    turn_numbers = [int(match.group(1)) for match in TURN_NUMBER_RE.finditer(transcript)]
    if turn_numbers:
        return TurnRange(start_turn=min(turn_numbers), end_turn=max(turn_numbers))
    non_empty_lines = [line for line in transcript.splitlines() if line.strip()]
    line_count = max(1, len(non_empty_lines))
    return TurnRange(start_turn=1, end_turn=line_count)


def _prior_compaction_end_turn(conn: sqlite3.Connection, resolved_task_ref: str) -> int:
    """End turn of the latest stored compaction for the task (``0`` if none).

    Bounds the new-turn slice embedded as the transcript anchor (implementation note
    implementation note). Reads the persisted ``turn_range`` JSON directly; a malformed or
    absent value degrades to ``0`` (anchor over the full transcript).
    """
    row = conn.execute(
        """
        SELECT turn_range
        FROM session_compactions
        WHERE task_ref = ?
        ORDER BY created_at DESC, compaction_id DESC
        LIMIT 1
        """,
        (resolved_task_ref,),
    ).fetchone()
    if row is None or row[0] is None:
        return 0
    try:
        return int(json.loads(row[0]).get("end_turn", 0) or 0)
    except (JSONDecodeError, TypeError, ValueError, AttributeError):
        return 0


def _split_transcript_lines(transcript: str) -> list[str]:
    return [line.strip() for line in transcript.splitlines() if line.strip()]


def _truncate_residual(residual: str | None) -> str | None:
    if residual is None:
        return None
    if len(residual) <= PROSE_RESIDUAL_SOFT_LIMIT_CHARS:
        return residual
    omitted_guess = len(residual) - PROSE_RESIDUAL_SOFT_LIMIT_CHARS
    marker = f"[truncated; {omitted_guess} chars omitted] ... "
    tail_budget = max(0, PROSE_RESIDUAL_SOFT_LIMIT_CHARS - len(marker))
    omitted = len(residual) - tail_budget
    marker = f"[truncated; {omitted} chars omitted] ... "
    tail_budget = max(0, PROSE_RESIDUAL_SOFT_LIMIT_CHARS - len(marker))
    if len(residual) > PROSE_RESIDUAL_HARD_LIMIT_CHARS:
        _log.warning("Compaction prose_residual exceeded hard limit; clipping %s chars", omitted)
    else:
        _log.warning("Compaction prose_residual exceeded soft limit; truncating %s chars", omitted)
    clipped = marker + residual[-tail_budget:]
    if len(clipped) > PROSE_RESIDUAL_HARD_LIMIT_CHARS:
        clipped = clipped[:PROSE_RESIDUAL_HARD_LIMIT_CHARS]
    return clipped


_JSONL_DETECT_SAMPLE_LINES = 5


def _looks_like_jsonl(text: str) -> bool:
    sampled = 0
    object_lines = 0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        sampled += 1
        try:
            parsed = json.loads(stripped)
        except JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            object_lines += 1
        if sampled >= _JSONL_DETECT_SAMPLE_LINES:
            break
    if sampled == 0:
        return False
    return object_lines * 2 > sampled


_HARNESS_BOILERPLATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"<local-command-caveat>.*?</local-command-caveat>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<command-name>.*?</command-name>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<command-message>.*?</command-message>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<command-args>.*?</command-args>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<system-reminder>.*?</system-reminder>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<agent_skill\b[^>]*>.*?</agent_skill>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<skill_information>.*?</skill_information>", re.IGNORECASE | re.DOTALL),
)


def _strip_harness_boilerplate(text: str) -> str:
    cleaned = text
    for pattern in _HARNESS_BOILERPLATE_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def _extract_text_from_content(content: object) -> list[str]:
    parts: list[str] = []
    if isinstance(content, str):
        stripped = content.strip()
        if stripped:
            parts.append(stripped)
        return parts
    if not isinstance(content, list):
        return parts
    for block in content:
        if isinstance(block, str):
            stripped = block.strip()
            if stripped:
                parts.append(stripped)
            continue
        if not isinstance(block, dict):
            continue
        if block.get("type") != "text":
            continue
        text = block.get("text")
        if isinstance(text, str):
            stripped = text.strip()
            if stripped:
                parts.append(stripped)
    return parts


def _slim_jsonl_record(record: dict[str, object]) -> str | None:
    record_type = record.get("type")
    if record_type in _JSONL_DROP_RECORD_TYPES:
        return None
    if record_type in ("user", "assistant"):
        message = record.get("message")
        if not isinstance(message, dict):
            return None
        parts = [_strip_harness_boilerplate(part) for part in _extract_text_from_content(message.get("content"))]
        parts = [part for part in parts if part]
        if not parts:
            return None
        return f"{record_type}: {' '.join(parts)}"
    return None


def _slim_jsonl_transcript(text: str) -> str:
    lines_out: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except JSONDecodeError:
            lines_out.append(line)
            continue
        if not isinstance(record, dict):
            continue
        slimmed_line = _slim_jsonl_record(record)
        if slimmed_line:
            lines_out.append(slimmed_line)
    if not lines_out:
        return ""
    return "\n".join(lines_out) + "\n"


def _cap_extract_chars(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    omitted = len(text) - max_chars
    marker = f"{TRANSCRIPT_CLIP_MARKER_PREFIX} {omitted} chars omitted]\n"
    tail_budget = max_chars - len(marker)
    if tail_budget <= 0:
        return text[-max_chars:]
    return marker + text[-tail_budget:]


def _process_transcript_text(text: str, settings: CompactionSettings) -> str:
    processed = text
    if _looks_like_jsonl(text):
        slimmed = _slim_jsonl_transcript(text)
        if slimmed:
            processed = slimmed
        else:
            _log.warning("JSONL slimming produced no extractable text; falling back to raw transcript")
    return _cap_extract_chars(processed, settings.max_extract_chars)


def _read_raw_transcript(
    transcript_path: str | Path,
    settings: CompactionSettings | None = None,
) -> str:
    resolved_settings = settings or CompactionSettings.from_env()
    path = Path(transcript_path)
    raw_size = path.stat().st_size
    if raw_size > resolved_settings.max_transcript_bytes:
        raise ValueError(
            f"Transcript exceeds max_transcript_bytes limit of {resolved_settings.max_transcript_bytes} bytes"
        )
    return path.read_text(encoding="utf-8", errors="replace")


def _read_transcript(
    transcript_path: str | Path,
    settings: CompactionSettings | None = None,
) -> str:
    resolved_settings = settings or CompactionSettings.from_env()
    raw_text = _read_raw_transcript(transcript_path, settings=resolved_settings)
    return _process_transcript_text(raw_text, resolved_settings)


def _collect_finding_matches(
    transcript_lines: list[str],
    finding_rows: list[dict],
) -> tuple[list[str], list[str], set[int]]:
    findings_fixed: list[str] = []
    findings_opened: list[str] = []
    resolved: set[int] = set()
    for index, line in enumerate(transcript_lines):
        line_lower = line.lower()
        for finding in finding_rows:
            finding_id = str(finding.get("finding_id", ""))
            if not finding_id or finding_id.lower() not in line_lower:
                continue
            if any(word in line_lower for word in ("fixed", "fix", "resolved")):
                findings_fixed.append(finding_id)
                resolved.add(index)
            if any(word in line_lower for word in ("opened", "open", "reopened")):
                findings_opened.append(finding_id)
                resolved.add(index)
    return findings_fixed, findings_opened, resolved


def _collect_test_matches(
    transcript_lines: list[str],
    test_rows: list[dict],
) -> tuple[list[str], set[int]]:
    tests_verified: list[str] = []
    resolved: set[int] = set()
    for index, line in enumerate(transcript_lines):
        for test_row in test_rows:
            command = str(test_row.get("command", ""))
            if command and command in line:
                tests_verified.append(command)
                resolved.add(index)
    return tests_verified, resolved


def _collect_touched_file_matches(
    transcript_lines: list[str],
    touch_rows: list[dict],
) -> tuple[list[str], set[int]]:
    files_touched: list[str] = []
    resolved: set[int] = set()
    for index, line in enumerate(transcript_lines):
        for touch_row in touch_rows:
            file_path = str(touch_row.get("file_path", ""))
            if file_path and file_path in line:
                files_touched.append(file_path)
                resolved.add(index)
    return files_touched, resolved


def _collect_decision_matches(
    transcript_lines: list[str],
    decisions: list[dict[str, object]],
) -> tuple[list[dict[str, str]], list[str], set[int]]:
    resolved_decisions: list[dict[str, str]] = []
    extra_files_touched: list[str] = []
    resolved: set[int] = set()
    for index, line in enumerate(transcript_lines):
        for decision in decisions:
            decision_id = str(decision["decision_id"])
            if decision_id and decision_id in line:
                resolved_decisions.append({"decision_id": decision_id, "slug": str(decision["slug"])})
                resolved.add(index)
        for decision in decisions:
            changed_files = decision["changed_files"]
            if not isinstance(changed_files, list):
                continue
            for changed_file in changed_files:
                if changed_file in line:
                    extra_files_touched.append(changed_file)
                    resolved.add(index)
    return resolved_decisions, extra_files_touched, resolved


def _derive_prose_residual(
    raw_nonempty_lines: list[str],
    transcript: str,
    resolved_line_indexes: set[int],
) -> str | None:
    if not resolved_line_indexes:
        return transcript if transcript else None
    residual_lines = [line for index, line in enumerate(raw_nonempty_lines) if index not in resolved_line_indexes]
    return "".join(residual_lines).rstrip("\n") if residual_lines else None


def _extract_summary_fields(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    transcript: str,
    turn_range: TurnRange | None = None,
) -> dict[str, object]:
    raw_nonempty_lines = [line for line in transcript.splitlines(keepends=True) if line.strip()]
    transcript_lines = [line.strip() for line in raw_nonempty_lines]

    finding_rows = (
        list_review_findings(task_ref=task_ref, status="all", limit=500, detail="full")
        .get("data", {})
        .get("findings", [])
    )
    findings_fixed, findings_opened, finding_indexes = _collect_finding_matches(transcript_lines, finding_rows)

    test_rows = get_verified_tests(task_ref=task_ref, limit=500).get("data", {}).get("tests", [])
    tests_verified, test_indexes = _collect_test_matches(transcript_lines, test_rows)

    touch_rows = get_touched_files(task_ref=task_ref, limit=500).get("data", {}).get("touches", [])
    files_touched, touch_indexes = _collect_touched_file_matches(transcript_lines, touch_rows)

    decisions = _CompactionQueries(conn, task_ref).load_task_decisions()
    resolved_decisions, decision_files_touched, decision_indexes = _collect_decision_matches(
        transcript_lines, decisions
    )
    files_touched.extend(decision_files_touched)

    resolved_line_indexes = finding_indexes | test_indexes | touch_indexes | decision_indexes
    prose_residual = _derive_prose_residual(raw_nonempty_lines, transcript, resolved_line_indexes)

    return {
        "turn_range": turn_range or _derive_turn_range(transcript),
        "decisions": _ordered_unique([json.dumps(item, sort_keys=True) for item in resolved_decisions]),
        "findings_fixed": _ordered_unique(findings_fixed),
        "findings_opened": _ordered_unique(findings_opened),
        "tests_verified": _ordered_unique(tests_verified),
        "files_touched": _ordered_unique(files_touched),
        "prose_residual": _truncate_residual(prose_residual),
    }


def _build_structured_summary(
    *,
    compaction_id: str,
    session_id: str,
    normalized_harness: CompactionHarness,
    resolved_task_ref: str,
    extracted: dict[str, object],
    created_at: datetime,
) -> StructuredSummary:
    return StructuredSummary(
        compaction_id=compaction_id,
        session_id=session_id,
        harness=normalized_harness,
        task_ref=resolved_task_ref,
        turn_range=cast(TurnRange, extracted["turn_range"]),
        decisions=[json.loads(item) for item in cast(list[str], extracted["decisions"])],
        findings_fixed=cast(list[str], extracted["findings_fixed"]),
        findings_opened=cast(list[str], extracted["findings_opened"]),
        tests_verified=cast(list[str], extracted["tests_verified"]),
        files_touched=cast(list[str], extracted["files_touched"]),
        prose_residual=cast("str | None", extracted["prose_residual"]),
        created_at=created_at,
    )


def _compute_tokens_saved_estimate(
    *,
    input_chars: int,
    summary: StructuredSummary,
) -> int:
    summary_chars = len(summary.model_dump_json())
    prose_residual_chars = len(summary.prose_residual or "")
    return max(0, (input_chars - summary_chars - prose_residual_chars) // 4)


def _persist_session_compaction(
    conn: sqlite3.Connection,
    summary: StructuredSummary,
    *,
    created_at: datetime,
    tokens_saved_estimate: int,
) -> int | None:
    cursor = conn.execute(
        """
        INSERT INTO session_compactions (
            compaction_id, session_id, harness, task_ref, turn_range,
            structured_summary_json, prose_residual, tokens_saved_estimate,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            summary.compaction_id,
            summary.session_id,
            summary.harness,
            summary.task_ref,
            summary.turn_range.model_dump_json(),
            summary.model_dump_json(),
            summary.prose_residual,
            tokens_saved_estimate,
            created_at.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    return cursor.lastrowid


def _build_record_receipt(
    summary: StructuredSummary,
    *,
    input_chars: int,
    raw_input_bytes: int,
    db_row_id: int,
    tokens_saved_estimate: int,
) -> CompactionRecordReceipt:
    summary_chars = len(summary.model_dump_json())
    prose_residual_chars = len(summary.prose_residual or "")
    return CompactionRecordReceipt(
        compaction_id=summary.compaction_id,
        summary=summary,
        input_chars=input_chars,
        raw_input_bytes=raw_input_bytes,
        summary_chars=summary_chars,
        prose_residual_chars=prose_residual_chars,
        tokens_saved_estimate=tokens_saved_estimate,
        db_row_id=db_row_id,
    )


def compact_session(
    transcript_path: str | Path,
    task_ref: str,
    harness: CompactionHarnessInput,
    session_id: str,
    settings: CompactionSettings | None = None,
    *,
    transcript_text: str | None = None,
    turn_range: TurnRange | None = None,
) -> CompactionRecordReceipt:
    """Persist a ``session_compactions`` row and return the typed receipt.

    internal widened the return type from a bare ``compaction_id``
    string to ``CompactionRecordReceipt`` so callers can attribute the
    compression delta without a second round-trip. The receipt inlines the
    canonical ``StructuredSummary`` (internal); the chars/4 divisor used by
    ``tokens_saved_estimate`` is sourced from ``harness-protocol.yaml``
    lines 126-127 (internal). The legacy bare-string wrapper at the
    ``workbay_handoff_mcp.api`` layer was deleted alongside this widening
    after the internal caller audit (decision id 662) confirmed no external
    callers depended on it.
    """
    normalized_harness = _validate_harness(harness)
    resolved_settings = settings or CompactionSettings.from_env()
    transcript_path_obj = Path(transcript_path)
    raw_input_bytes = transcript_path_obj.stat().st_size
    # internal: when the caller supplies turn_range it has already sliced
    # transcript_text to the new turns (the Stop hook does this). Re-slicing here
    # would double-slice — and JSONL re-numbers ordinals from 1, so the second
    # slice against the absolute prior end_turn matches nothing and the anchor is
    # silently dropped. Track it so the anchor embeds the caller's text as-is.
    caller_supplied_turn_range = turn_range is not None
    if transcript_text is None:
        raw_text = _read_raw_transcript(transcript_path_obj, settings=resolved_settings)
        turn_source_text = raw_text
        transcript = _process_transcript_text(raw_text, resolved_settings)
        if turn_range is None:
            turn_range = _derive_turn_range(raw_text)
    else:
        turn_source_text = transcript_text
        if turn_range is None:
            turn_range = _derive_turn_range(transcript_text)
        transcript = _process_transcript_text(transcript_text, resolved_settings)
    now = datetime.now(UTC)

    with _get_db_connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        compaction_id = _CompactionQueries(conn, resolved_task_ref).next_compaction_id()
        # Bound the new-turn slice for the transcript anchor (internal).
        # Only needed when the caller did NOT pre-slice (turn_range omitted); the
        # Stop hook pre-slices + supplies turn_range, so skip the query there.
        prior_end_turn = 0 if caller_supplied_turn_range else _prior_compaction_end_turn(conn, resolved_task_ref)
        extracted = _extract_summary_fields(
            conn,
            task_ref=resolved_task_ref,
            transcript=transcript,
            turn_range=turn_range,
        )
        summary = _build_structured_summary(
            compaction_id=compaction_id,
            session_id=session_id,
            normalized_harness=normalized_harness,
            resolved_task_ref=resolved_task_ref,
            extracted=extracted,
            created_at=now,
        )
        tokens_saved_estimate = _compute_tokens_saved_estimate(
            input_chars=len(transcript),
            summary=summary,
        )
        db_row_id = _persist_session_compaction(
            conn,
            summary,
            created_at=now,
            tokens_saved_estimate=tokens_saved_estimate,
        )
        conn.commit()

    if db_row_id is None:
        raise RuntimeError("session_compactions INSERT returned no lastrowid; cannot build receipt")

    # Embed the prose residual after the compaction row committed (best-effort;
    # None residual is a no-op). Keyed by compaction_id, matching backfill.
    embed_concept_on_write("compaction.prose_residual", summary.compaction_id, summary.task_ref, summary.prose_residual)

    # Persist the transcript anchor for semantic reinjection (internal):
    # embed the turns NEW to this compaction so SessionStart can rank concepts by
    # similarity to it. Best-effort, post-commit; provider/extra absent leaves
    # anchor_vector NULL so reinjection degrades to today's selection. When the
    # caller pre-sliced (supplied turn_range) the source text IS the new turns.
    anchor_source_text = (
        turn_source_text
        if caller_supplied_turn_range
        else slice_new_turn_transcript(turn_source_text, since_turn=prior_end_turn)
    )
    embed_compaction_anchor_on_write(summary.compaction_id, summary.task_ref, anchor_source_text)

    return _build_record_receipt(
        summary,
        input_chars=len(transcript),
        raw_input_bytes=raw_input_bytes,
        db_row_id=db_row_id,
        tokens_saved_estimate=tokens_saved_estimate,
    )


def _row_to_compaction_record(row: sqlite3.Row) -> CompactionRecord:
    saved_raw = row["tokens_saved_estimate"]
    tokens_saved_estimate = None if saved_raw is None else int(saved_raw)
    return CompactionRecord(
        summary=StructuredSummary.model_validate_json(str(row["structured_summary_json"])),
        tokens_saved_estimate=tokens_saved_estimate,
    )


def get_compaction(compaction_id: str) -> CompactionRecord:
    normalized_compaction_id = compaction_id.strip()
    if not normalized_compaction_id:
        raise ValueError("compaction_id is required")

    with _get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT structured_summary_json, tokens_saved_estimate
            FROM session_compactions WHERE compaction_id = ?
            """,
            (normalized_compaction_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown compaction_id: {normalized_compaction_id}")
        return _row_to_compaction_record(row)


def render_cold_start_compaction(task_ref: str | None = None) -> str | None:
    """Render the latest compaction as an ID-only cold-start block.

    Returns ``None`` when no compaction row exists for the resolved task,
    so cold-start callers can fall back to the pre-internal baseline
    without emitting any extra bytes.
    """
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            """
            SELECT structured_summary_json, created_at
            FROM session_compactions
            WHERE task_ref = ?
            ORDER BY created_at DESC, compaction_id DESC
            LIMIT 1
            """,
            (resolved_task_ref,),
        ).fetchone()
        if row is None:
            return None
        latest = StructuredSummary.model_validate_json(str(row["structured_summary_json"]))
        # Use the row's stored created_at (string) so the comparison is
        # against the same column the row was written with.
        stored_created_at = datetime.strptime(str(row["created_at"]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        newer_decisions = _CompactionQueries(conn, resolved_task_ref).count_decisions_after(stored_created_at)

    lines: list[str] = []
    lines.append(f"Latest compaction: {latest.compaction_id}")
    lines.append(f"Turns {latest.turn_range.start_turn}–{latest.turn_range.end_turn}")
    decision_ids = [decision.decision_id for decision in latest.decisions]
    lines.append(f"Decisions: {', '.join(decision_ids) if decision_ids else '(none)'}")
    lines.append(f"Findings fixed: {', '.join(latest.findings_fixed) if latest.findings_fixed else '(none)'}")
    lines.append(f"Tests verified: {', '.join(latest.tests_verified) if latest.tests_verified else '(none)'}")
    lines.append(f"Files touched: {', '.join(latest.files_touched) if latest.files_touched else '(none)'}")
    if newer_decisions > 0:
        lines.append(f"(compaction stale; {newer_decisions} decisions newer)")
    return "\n".join(lines)


def get_latest_compaction(task_ref: str | None = None) -> CompactionRecord | None:
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            """
            SELECT structured_summary_json, tokens_saved_estimate
            FROM session_compactions
            WHERE task_ref = ?
            ORDER BY created_at DESC, compaction_id DESC
            LIMIT 1
            """,
            (resolved_task_ref,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_compaction_record(row)


def get_latest_compaction_for_session(
    task_ref: str | None,
    session_id: str,
) -> CompactionRecord | None:
    """Return the newest compaction row for one harness session."""
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            """
            SELECT structured_summary_json, tokens_saved_estimate
            FROM session_compactions
            WHERE task_ref = ? AND session_id = ?
            ORDER BY created_at DESC, compaction_id DESC
            LIMIT 1
            """,
            (resolved_task_ref, session_id),
        ).fetchone()
        if row is None:
            return None
        return _row_to_compaction_record(row)


def _load_latest_compaction_summary(
    conn: sqlite3.Connection,
    resolved_task_ref: str,
) -> tuple[StructuredSummary, int] | None:
    row = conn.execute(
        """
        SELECT structured_summary_json, created_at
        FROM session_compactions
        WHERE task_ref = ?
        ORDER BY created_at DESC, compaction_id DESC
        LIMIT 1
        """,
        (resolved_task_ref,),
    ).fetchone()
    if row is None:
        return None
    latest = StructuredSummary.model_validate_json(str(row["structured_summary_json"]))
    stored_created_at = datetime.strptime(str(row["created_at"]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    newer_decisions = _CompactionQueries(conn, resolved_task_ref).count_decisions_after(stored_created_at)
    return latest, newer_decisions


def _build_refresh_packet_body(
    latest: StructuredSummary,
    *,
    resolved_task_ref: str,
    dedupe_key: str,
    rendered: str | None,
    newer_decisions: int,
    advisory: dict[str, object] | None,
    semantic_reinjection: dict[str, object] | None = None,
) -> dict[str, object]:
    latest_json = latest.model_dump(mode="json")
    body: dict[str, object] = {
        "task_ref": resolved_task_ref,
        "compaction_id": latest.compaction_id,
        "created_at": latest_json.get("created_at"),
        "session_id": latest.session_id,
        "harness": latest.harness,
        "policy": "supersedes_prior_session_detail",
        "dedupe_key": dedupe_key,
        "rendered_cold_start": rendered,
        "stale": {
            "detected": newer_decisions > 0,
            "decisions_newer": newer_decisions,
        },
        "advisory": advisory,
    }
    if semantic_reinjection is not None:
        body["semantic_reinjection"] = semantic_reinjection
    return body


def extract_visible_texts_for_semantic_reinjection(
    state_data: Mapping[str, object],
) -> list[str]:
    """Collect nonblank objective, focus, and pending-action strings from handoff state."""
    active_raw = state_data.get("active")
    active = active_raw if isinstance(active_raw, dict) else {}
    texts: list[str] = []
    for key in ("objective", "focus"):
        value = str(active.get(key) or "").strip()
        if value:
            texts.append(value)
    pending_raw = state_data.get("actions_pending")
    if isinstance(pending_raw, list):
        for item in pending_raw:
            if not isinstance(item, dict):
                continue
            action = str(item.get("action") or "").strip()
            if action:
                texts.append(action)
    return texts


# Actionable remedies for embedding-provider degradation (implementation note S3 / AGT-08:
# a rejection must name its remedy). The runtime libs are a deliberate optional
# extra; when they are absent — or rows were written while the provider was
# broken — the skip payload names the exact repair instead of a bare ImportError.
EMBEDDINGS_INSTALL_HINT = (
    "embedding runtime libs absent: install the optional extra into the serving "
    "interpreter — `uv tool install 'mcp-workbay-handoff[embeddings]'` (uv-tool "
    "install) or `uv pip install numpy onnxruntime tokenizers` (venv install) — "
    "then re-embed historical rows via "
    "`python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings`."
)
EMBEDDINGS_BACKFILL_HINT = (
    "rows written while the embedding provider was unavailable stay unembedded; "
    "backfill them via `python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings` "
    "(doctor facet embedding_backfill_coverage reports the gap)."
)


def _build_semantic_reinjection_for_refresh(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    compaction_id: str,
    visible_texts: Sequence[str],
    semantic_content_budget_chars: int,
) -> dict[str, object]:
    """Build typed semantic reinjection telemetry for the pull-path refresh packet."""
    try:
        from .embeddings.provider import EmbeddingArtifactError, embedding_unavailable_reason
        from .embeddings.reinjection import (
            ReinjectionConfig,
            SkipReason,
            build_semantic_reinjection_packet,
        )
        from .embeddings.store import _resolve_provider, deserialize_vector
    except ImportError as exc:
        # The embeddings subpackage imports numpy et al.; an ImportError here means
        # the optional runtime is absent — a deps-missing condition, not an opaque error.
        return {
            "status": "degraded",
            "skip_reason": "deps_or_model_missing",
            "model_id": None,
            "chars_budget": max(0, semantic_content_budget_chars),
            "detail": str(exc),
            "remedy": EMBEDDINGS_INSTALL_HINT,
        }
    config = ReinjectionConfig.from_env()
    provider = _resolve_provider()
    # When no provider is constructible, distinguish env-off (disabled) from
    # unset artifact pins (unconfigured) instead of the generic provider_unavailable.
    provider_unavailable_reason = embedding_unavailable_reason() if provider is None else None
    row = conn.execute(
        "SELECT anchor_vector FROM session_compactions WHERE compaction_id = ?",
        (compaction_id,),
    ).fetchone()
    persisted = None
    if row is not None and row[0] is not None:
        persisted = deserialize_vector(row[0])
    try:
        result = build_semantic_reinjection_packet(
            conn,
            task_ref=task_ref,
            provider=provider,
            persisted_anchor=persisted,
            visible_texts=list(visible_texts),
            semantic_content_budget_chars=semantic_content_budget_chars,
            config=config,
            provider_unavailable_reason=cast("SkipReason | None", provider_unavailable_reason),
        )
        return result.to_dict()
    except EmbeddingArtifactError as exc:
        # The runtime/model is configured but the deps or artifact are missing at
        # embed time — surface it as a deps-missing condition, not opaque "error".
        return {
            "status": "degraded",
            "skip_reason": "deps_or_model_missing",
            "model_id": getattr(provider, "model_id", None),
            "chars_budget": max(0, semantic_content_budget_chars),
            "detail": str(exc),
            "remedy": EMBEDDINGS_INSTALL_HINT,
        }
    except Exception as exc:  # noqa: BLE001 - fail-open on pull path
        return {
            "status": "degraded",
            "skip_reason": "error",
            "model_id": getattr(provider, "model_id", None),
            "chars_budget": max(0, semantic_content_budget_chars),
            "detail": str(exc),
        }


def _relevant_lines_from_reinjection_payload(payload: dict[str, object]) -> list[str]:
    from .embeddings.reinjection import SelectedConcept, render_readable_relevant_lines

    selected_raw = payload.get("selected")
    if not isinstance(selected_raw, list) or not selected_raw:
        return []
    selected: list[SelectedConcept] = []
    for item in selected_raw:
        if not isinstance(item, dict):
            continue
        selected.append(
            SelectedConcept(
                kind=str(item.get("kind", "")),
                id=str(item.get("id", "")),
                label=str(item.get("label", "")),
                snippet=str(item.get("snippet", "")),
                score=float(item.get("score", 0.0)),
                emitted_chars=int(item.get("emitted_chars", 0)),
            )
        )
    return render_readable_relevant_lines(selected)


def semantic_reinjection_packet(
    task_ref: str | None = None,
    anchor_texts: list[str] | None = None,
    semantic_content_budget_chars: int | None = None,
) -> dict[str, object]:
    try:
        from .embeddings.reinjection import (
            REFRESH_BUDGET_CHARS_MAX,
            REFRESH_BUDGET_CHARS_MIN,
            ReinjectionConfig,
        )
    except ImportError as exc:
        # 0128 S3: this top-level import used to crash the whole tool with a bare
        # `No module named 'numpy'` when the optional embeddings extra is absent
        # even though embedding.env is configured. Degrade with the same typed
        # shape as the refresh path and name the remedy (AGT-08).
        return {
            "ok": True,
            "task_ref": task_ref,
            "status": "degraded",
            "skip_reason": "deps_or_model_missing",
            "relevant_lines": [],
            "semantic_reinjection": {
                "status": "degraded",
                "skip_reason": "deps_or_model_missing",
                "detail": str(exc),
                "remedy": EMBEDDINGS_INSTALL_HINT,
            },
        }

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        texts = [text.strip() for text in (anchor_texts or []) if isinstance(text, str) and text.strip()]
        loaded = _load_latest_compaction_summary(conn, resolved_task_ref)
        compaction_id = loaded[0].compaction_id if loaded is not None else ""
        # Clamp a caller-supplied override to the same bounds from_env enforces so
        # an explicit budget cannot bypass the contract ceiling.
        budget = (
            max(REFRESH_BUDGET_CHARS_MIN, min(REFRESH_BUDGET_CHARS_MAX, int(semantic_content_budget_chars)))
            if semantic_content_budget_chars is not None
            else ReinjectionConfig.from_env().refresh_budget_chars
        )
        reinjection = _build_semantic_reinjection_for_refresh(
            conn,
            task_ref=resolved_task_ref,
            compaction_id=compaction_id,
            visible_texts=texts,
            semantic_content_budget_chars=budget,
        )
        # 0128 S3: unembedded-backlog skips name the backfill repair (OBS-08 —
        # the skip must be distinguishable and actionable, not just labeled).
        if reinjection.get("skip_reason") == "no_embeddings" and "remedy" not in reinjection:
            reinjection["remedy"] = EMBEDDINGS_BACKFILL_HINT
        return {
            "ok": True,
            "task_ref": resolved_task_ref,
            "status": reinjection.get("status"),
            "skip_reason": reinjection.get("skip_reason"),
            "relevant_lines": _relevant_lines_from_reinjection_payload(reinjection),
            "semantic_reinjection": reinjection,
        }


def build_context_refresh_packet(
    task_ref: str | None = None,
    *,
    last_injected_compaction_id: str | None = None,
    advisory: dict[str, object] | None = None,
    visible_texts: Sequence[str] | None = None,
    semantic_content_budget_chars: int | None = None,
) -> dict[str, object]:
    """Build an opt-in same-session context refresh packet from the latest compaction."""
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        loaded = _load_latest_compaction_summary(conn, resolved_task_ref)
        if loaded is None:
            return {
                "available": False,
                "reason": "no_compaction",
                "dedupe_key": None,
                "packet": None,
            }
        latest, newer_decisions = loaded

        dedupe_key = latest.compaction_id
        if last_injected_compaction_id is not None and last_injected_compaction_id.strip() == latest.compaction_id:
            return {
                "available": False,
                "reason": "already_injected",
                "dedupe_key": dedupe_key,
                "packet": None,
            }

        semantic_reinjection: dict[str, object] | None = None
        if visible_texts is not None:
            try:
                from .embeddings.reinjection import ReinjectionConfig

                budget = (
                    semantic_content_budget_chars
                    if semantic_content_budget_chars is not None
                    else ReinjectionConfig.from_env().refresh_budget_chars
                )
                semantic_reinjection = _build_semantic_reinjection_for_refresh(
                    conn,
                    task_ref=resolved_task_ref,
                    compaction_id=latest.compaction_id,
                    visible_texts=visible_texts,
                    semantic_content_budget_chars=int(budget),
                )
            except ImportError:
                budget = max(0, int(semantic_content_budget_chars or 0))
                semantic_reinjection = {
                    "status": "degraded",
                    "skip_reason": "error",
                    "model_id": None,
                    "chars_budget": budget,
                }

        rendered = render_cold_start_compaction(task_ref=resolved_task_ref)
        packet = _build_refresh_packet_body(
            latest,
            resolved_task_ref=resolved_task_ref,
            dedupe_key=dedupe_key,
            rendered=rendered,
            newer_decisions=newer_decisions,
            advisory=advisory,
            semantic_reinjection=semantic_reinjection,
        )
        # [OBS-08] measure-only packet size + frequency row. Do not tune
        # reinjection thresholds from these counters yet (T19).
        packet_bytes = len(json.dumps(packet, separators=(",", ":")))
        try:
            record_session_reinjection(
                conn,
                session_id=str(latest.session_id or "context_refresh"),
                harness=str(latest.harness),
                task_ref=resolved_task_ref,
                compaction_id=latest.compaction_id,
                source="context_refresh_packet",
                emitted_chars=packet_bytes,
                arm="observability",
                semantic_detail={
                    "kind": "packet_size_metric",
                    "packet_bytes": packet_bytes,
                    "available": True,
                },
            )
            conn.commit()
        except Exception:  # noqa: BLE001 — observability must never fail the read path
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
        return {
            "available": True,
            "reason": "new_packet",
            "dedupe_key": dedupe_key,
            "packet": packet,
            "metrics": {
                "packet_bytes": packet_bytes,
                "source": "context_refresh_packet",
            },
        }


def _skip_advisory(warnings: list[str]) -> dict:
    return CompactionAdvisory(warnings=warnings).model_dump(mode="json")


def _disabled_advisory(
    source: DisabledSource,
    *,
    warnings: list[str],
    harness: str | None,
    transcript_path: str | None,
    transcript_source: str | None,
    contract_source: dict | None = None,
) -> dict:
    """Build the advisory envelope returned when the resolver short-circuits.

    Threshold/observed math is skipped entirely (the resolver short-circuits
    *before* any comparison). The harness/transcript fields remain present
    for traceability so callers can still see what the advisory would have
    measured if it had run.
    """
    return CompactionAdvisory(
        harness=harness,
        transcript=CompactionAdvisoryTranscript(path=transcript_path, source=transcript_source),
        contract_source=contract_source,
        disabled=True,
        disabled_source=source,
        warnings=warnings,
    ).model_dump(mode="json")


def _glob_matches_for_harness(rule: TranscriptDiscoveryRule) -> list[Path]:
    expanded = Path(rule.fallback_glob).expanduser()
    if expanded.is_absolute():
        candidates = list(Path(expanded.anchor or "/").glob(str(expanded.relative_to(expanded.anchor))))
    else:
        candidates = list(Path.cwd().glob(str(expanded)))
    return sorted(candidates, key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)


def _detect_harness_from_fallback_glob(
    contract: CompactionContract,
) -> CompactionContractHarness | None:
    """Resolve harness via fallback_glob when env vars are unset.

    internal follow-up: internal documents transcript discovery as env-var
    first, fallback_glob second. When exactly one harness produces matches on
    disk, use it; ambiguous or empty results remain unresolved.
    """

    matches: list[CompactionContractHarness] = []
    for harness_name, rule in contract.transcript_discovery.items():
        if _glob_matches_for_harness(rule):
            matches.append(normalize_compaction_harness(harness_name))
    if len(matches) == 1:
        return matches[0]
    return None


def _resolve_transcript(
    contract: CompactionContract,
    harness: str | None,
    env: Mapping[str, str],
) -> tuple[str | None, str | None, str | None]:
    """Return (path, source, warning) for the resolved transcript."""

    if harness is None or harness not in contract.transcript_discovery:
        return None, None, None
    rule = contract.transcript_discovery[harness]
    env_value = env.get(rule.env_var, "").strip()
    if env_value:
        candidate = Path(env_value).expanduser()
        if candidate.exists():
            return str(candidate), "env_var", None
        return (
            None,
            None,
            f"transcript path from {rule.env_var} does not exist: {candidate}",
        )

    matches = _glob_matches_for_harness(rule)
    if matches:
        return str(matches[0]), "fallback_glob", None
    return (
        None,
        None,
        f"no transcript found for harness {harness!r} (env {rule.env_var} unset, fallback_glob had no matches)",
    )


def _build_drift_warning(contract_source: Mapping[str, object]) -> str | None:
    drift = contract_source.get("drift")
    if not (isinstance(drift, dict) and drift.get("detected")):
        return None
    drift_thresholds = drift.get("thresholds") or {}
    parts: list[str] = []
    for key in ("tokens", "chars"):
        entry = drift_thresholds.get(key)
        if isinstance(entry, dict):
            parts.append(f"{key} resolved={entry.get('resolved')} package_reference={entry.get('package_reference')}")
    summary = "; ".join(parts) if parts else "thresholds differ"
    return f"compaction_contract_drift: resolved contract differs from package reference ({summary})"


def _resolve_harness_with_fallback(
    contract: CompactionContract,
    env: Mapping[str, str],
) -> tuple[CompactionContractHarness | None, list[str]]:
    resolution = detect_active_harness(contract, env=dict(env))
    harness = resolution.harness
    if harness is None:
        harness = _detect_harness_from_fallback_glob(contract)
    extra_warnings: list[str] = [] if harness is not None else list(resolution.warnings)
    return harness, extra_warnings


def _measure_transcript_chars(transcript_path: str | None) -> tuple[int | None, str | None]:
    if transcript_path is None:
        return None, None
    try:
        return len(Path(transcript_path).read_text(encoding="utf-8", errors="replace")), None
    except OSError as exc:
        return None, f"unreadable transcript at {transcript_path}: {exc}"


def _latest_compaction_cursor(task_ref: str) -> tuple[str | None, datetime | None]:
    """Return (latest_id, since) for the latest compaction of task_ref."""

    latest = get_latest_compaction(task_ref=task_ref)
    if latest is None:
        return None, None
    ts_raw = getattr(latest.summary, "created_at", None)
    since = ts_raw if isinstance(ts_raw, datetime) else None
    return latest.summary.compaction_id, since


def compute_compaction_advisory(
    *,
    workspace_root: str | Path,
    task_ref: str,
    env: Mapping[str, str] | None = None,
) -> dict:
    """internal — contract-driven compaction advisory evaluator.

    Returns the canonical advisory envelope documented in the internal
    task plan (`recommended`, `thresholds`, `observed`, `harness`,
    `transcript`, `latest_compaction_id`, `warnings`).
    """

    source_env: Mapping[str, str] = env if env is not None else os.environ
    try:
        contract = load_compaction_contract(workspace_root)
    except FileNotFoundError as exc:
        return _skip_advisory([f"missing compaction contract: {exc}"])
    except (ValueError, OSError) as exc:
        return _skip_advisory([f"unreadable compaction contract: {exc}"])

    warnings: list[str] = []
    contract_source = build_contract_source_report(contract, workspace_root=workspace_root).model_dump(mode="json")
    drift_warning = _build_drift_warning(contract_source)
    if drift_warning is not None:
        warnings.append(drift_warning)
    harness, harness_warnings = _resolve_harness_with_fallback(contract, source_env)
    warnings.extend(harness_warnings)

    transcript_path, transcript_source, transcript_warning = _resolve_transcript(contract, harness, source_env)
    if transcript_warning:
        warnings.append(transcript_warning)

    observed_chars, transcript_read_warning = _measure_transcript_chars(transcript_path)
    if transcript_read_warning is not None:
        warnings.append(transcript_read_warning)

    latest_id, since = _latest_compaction_cursor(task_ref)

    with _get_db_connection() as conn:
        disabled, disabled_source = resolve_compaction_disabled(env=source_env, conn=conn, task_ref=task_ref)
        if disabled and disabled_source is not None:
            return _disabled_advisory(
                disabled_source,
                warnings=warnings,
                harness=harness,
                transcript_path=transcript_path,
                transcript_source=transcript_source,
                contract_source=contract_source,
            )
        observed_tokens = _CompactionQueries(conn, task_ref).observed_token_total(since)

    effective = resolve_effective_thresholds(
        contract,
        env=source_env,
        workspace_root=Path(workspace_root) if not isinstance(workspace_root, Path) else workspace_root,
    )
    warnings.extend(effective.warnings)

    recommended = False
    if observed_tokens is not None and observed_tokens >= effective.tokens:
        recommended = True
    char_gate_eligible = latest_id is None or (observed_tokens is not None and observed_tokens > 0)
    if char_gate_eligible and observed_chars is not None and observed_chars >= effective.chars:
        recommended = True

    advisory = CompactionAdvisory(
        recommended=recommended,
        recommended_action="compaction(operation=record)" if recommended else None,
        thresholds=CompactionAdvisoryThresholds(tokens=effective.tokens, chars=effective.chars),
        thresholds_source=CompactionAdvisoryThresholdSources(
            tokens=effective.tokens_source,
            chars=effective.chars_source,
        ),
        observed=CompactionAdvisoryThresholds(tokens=observed_tokens, chars=observed_chars),
        harness=harness,
        transcript=CompactionAdvisoryTranscript(path=transcript_path, source=transcript_source),
        contract_source=contract_source,
        latest_compaction_id=latest_id,
        warnings=warnings,
    )
    payload = advisory.model_dump(mode="json")
    # [OBS-08] record advisory size after serialisation so the metric is real-shape.
    payload["metrics"] = CompactionAdvisoryMetrics(
        advisory_bytes=len(json.dumps(payload, separators=(",", ":"))),
        observed_chars=observed_chars,
        observed_tokens=observed_tokens,
        recommended=recommended,
    ).model_dump(mode="json")
    return payload
