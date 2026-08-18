"""Doctor facet: embedded-vs-eligible concept coverage.

Extracted from ``subcommands`` so the god-module only registers the call.
``doctor()`` imports ``_doctor_embedding_backfill_gap`` and extends findings.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

Finding = dict[str, str]

_EMBEDDING_ARTIFACT_ENV_KEYS: tuple[str, ...] = (
    "WORKBAY_HANDOFF_EMBEDDING_MODEL",
    "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER",
    "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256",
    "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256",
)


class BackfillGapStatus(str, Enum):
    """Typed measurement result — never collapse a failed probe to "no gap"."""

    MEASURED_GAP = "measured_gap"
    MEASURED_CLEAN = "measured_clean"
    UNMEASURABLE = "unmeasurable"


@dataclass(frozen=True)
class BackfillGapOutcome:
    status: BackfillGapStatus
    findings: tuple[Finding, ...] = ()
    reason: str | None = None


@dataclass(frozen=True)
class BackfillModelPin:
    """Pinned model id, or a typed miss (unconfigured vs import failure)."""

    model_id: str | None
    import_error: str | None = None


@dataclass(frozen=True)
class StoreBackfillContract:
    classify: Callable[..., tuple[str, str | None]]
    kinds: tuple[str, ...]
    sources: tuple[tuple[str, str, str, str], ...]


def _doctor_backfill_model_id() -> BackfillModelPin:
    """Pinned write-path model identity.

    ``import_error`` is set when the pin module cannot be imported — that is
    *not* "no pin configured" and must not fall back to hash-only matching.
    ``model_id is None`` with no error means hash-only is the intended gate.
    """
    try:
        from workbay_handoff_mcp.embeddings.model_pin import MODEL_ID
    except Exception as exc:  # noqa: BLE001 — distinguish fail from unconfigured
        return BackfillModelPin(
            None, f"MODEL_ID import failed: {type(exc).__name__}: {exc}"
        )
    if MODEL_ID is None or not str(MODEL_ID).strip():
        return BackfillModelPin(None)
    return BackfillModelPin(str(MODEL_ID))


def _load_store_backfill_contract() -> tuple[StoreBackfillContract | None, str | None]:
    """Lazy store contract (kinds, skip gate, source projection).

    store.py imports numpy at module level. Loading it here — only after the
    embeddings extra is declared — keeps default ``doctor()`` import-light.
    That is why subcommands historically said "do not import that module".
    """
    try:
        from workbay_handoff_mcp.embeddings.store import (
            CONCEPT_BACKFILL_SOURCES,
            CONCEPT_ENTITY_KINDS,
            classify_concept_for_backfill,
        )
    except Exception as exc:  # noqa: BLE001 — failed contract is unmeasurable
        return None, f"store backfill contract import failed: {type(exc).__name__}: {exc}"
    return (
        StoreBackfillContract(
            classify=classify_concept_for_backfill,
            kinds=tuple(CONCEPT_ENTITY_KINDS),
            sources=tuple(CONCEPT_BACKFILL_SOURCES),
        ),
        None,
    )


def _source_kind_drift_reason(contract: StoreBackfillContract) -> str | None:
    source_kinds = {kind for kind, _table, _id_col, _text in contract.sources}
    if source_kinds != set(contract.kinds):
        return "doctor source-kind map drifted from store.CONCEPT_ENTITY_KINDS"
    return None


def _aligned_store_contract() -> StoreBackfillContract | BackfillGapOutcome:
    contract, contract_error = _load_store_backfill_contract()
    if contract is None:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=contract_error or "store backfill contract unavailable",
        )
    drift = _source_kind_drift_reason(contract)
    if drift:
        return BackfillGapOutcome(BackfillGapStatus.UNMEASURABLE, reason=drift)
    return contract


def _doctor_embedding_env_map(target: Path) -> dict[str, str]:
    """Process env overlays ``.workbay/embedding.env`` (process wins)."""
    from workbay_bootstrap.embedding_provision import parse_embedding_env_file

    merged = dict(parse_embedding_env_file(target))
    for key in _EMBEDDING_ARTIFACT_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            merged[key] = value
    disabled = os.environ.get("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED")
    if disabled is not None:
        merged["WORKBAY_HANDOFF_EMBEDDINGS_DISABLED"] = disabled
    return merged


def _backfill_action_note(target: Path) -> str | None:
    """Override the backfill remedy when a provider cannot actually be built.

    ``make backfill-handoff-embeddings`` writes zero rows when artifacts are
    unconfigured. Name the missing env vars so the finding is actionable.
    """
    try:
        from workbay_handoff_mcp.embeddings.provider import (
            ENV_MODEL,
            ENV_MODEL_SHA256,
            ENV_TOKENIZER,
            ENV_TOKENIZER_SHA256,
            embedding_unavailable_reason,
        )
    except Exception as exc:  # noqa: BLE001 — still measure; don't claim backfill works
        return (
            f"EmbeddingProvider import failed ({type(exc).__name__}: {exc}); "
            "install mcp-workbay-handoff[embeddings] before "
            "`make backfill-handoff-embeddings`"
        )
    reason = embedding_unavailable_reason(_doctor_embedding_env_map(target))
    if reason != "unconfigured":
        return None
    keys = (ENV_MODEL, ENV_TOKENIZER, ENV_MODEL_SHA256, ENV_TOKENIZER_SHA256)
    env = _doctor_embedding_env_map(target)
    missing = [key for key in keys if not str(env.get(key) or "").strip()]
    missing_label = ", ".join(missing) if missing else "WORKBAY_HANDOFF_EMBEDDING_*"
    return (
        f"embeddings artifacts unconfigured (missing {missing_label}); "
        "set them via `.workbay/embedding.env` or "
        "`workbay-bootstrap provision-embeddings` before "
        "`make backfill-handoff-embeddings`"
    )


def _count_kind_backfill_coverage(
    conn: object,
    kind: str,
    table: str,
    id_column: str,
    text_column: str,
    model_id: str | None,
    classify: Callable[..., tuple[str, str | None]],
) -> tuple[int, int] | None:
    """Eligible/embedded counts using the store skip gate (Python ``str.strip()``)."""
    import sqlite3

    try:
        rows = conn.execute(  # type: ignore[attr-defined]
            f"SELECT CAST({id_column} AS TEXT), {text_column} FROM {table}"
        ).fetchall()
    except sqlite3.Error:
        return None
    eligible = 0
    embedded = 0
    for entity_id, text in rows:
        try:
            outcome, _new_hash = classify(
                conn, kind, str(entity_id), text, model_id=model_id
            )
        except sqlite3.Error:
            return None
        if outcome == "empty":
            continue
        eligible += 1
        if outcome == "skipped":
            embedded += 1
    return eligible, embedded


def _backfill_gap_finding(
    kind: str, eligible: int, embedded: int, *, action_note: str | None = None
) -> Finding:
    action = action_note or "run `make backfill-handoff-embeddings`"
    return {
        "kind": "embedding_backfill_gap",
        "path": ".task-state/handoff.db",
        "severity": "warning",
        "message": f"{kind}: {eligible} eligible, {embedded} embedded — {action}",
    }


def _unmeasurable_backfill_finding(reason: str) -> Finding:
    return {
        "kind": "embedding_backfill_unmeasurable",
        "path": ".task-state/handoff.db",
        "severity": "error",
        "message": f"embedding backfill coverage unmeasurable: {reason}",
    }


def _sqlite_table_names(conn: object) -> set[str] | BackfillGapOutcome:
    import sqlite3

    try:
        return {
            str(row[0])
            for row in conn.execute(  # type: ignore[attr-defined]
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=f"sqlite_master unreadable: {type(exc).__name__}: {exc}",
        )


def _gap_findings_for_sources(
    conn: object,
    tables: set[str],
    *,
    sources: tuple[tuple[str, str, str, str], ...],
    model_id: str | None,
    classify: Callable[..., tuple[str, str | None]],
    action_note: str | None,
) -> BackfillGapOutcome:
    findings: list[Finding] = []
    for kind, table, id_column, text_column in sources:
        if table not in tables:
            continue
        counts = _count_kind_backfill_coverage(
            conn, kind, table, id_column, text_column, model_id, classify
        )
        if counts is None:
            return BackfillGapOutcome(
                BackfillGapStatus.UNMEASURABLE,
                reason=f"failed probing {kind} on {table}.{text_column}",
            )
        eligible, embedded = counts
        if eligible - embedded <= 0:
            continue
        findings.append(
            _backfill_gap_finding(kind, eligible, embedded, action_note=action_note)
        )
    if findings:
        return BackfillGapOutcome(
            BackfillGapStatus.MEASURED_GAP, findings=tuple(findings)
        )
    return BackfillGapOutcome(BackfillGapStatus.MEASURED_CLEAN)


def _backfill_gap_findings_from_conn(
    conn: object,
    *,
    sources: tuple[tuple[str, str, str, str], ...],
    model_id: str | None,
    classify: Callable[..., tuple[str, str | None]],
    action_note: str | None,
) -> BackfillGapOutcome:
    tables = _sqlite_table_names(conn)
    if isinstance(tables, BackfillGapOutcome):
        return tables
    if "concept_embeddings" not in tables:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason="concept_embeddings table missing",
        )
    return _gap_findings_for_sources(
        conn,
        tables,
        sources=sources,
        model_id=model_id,
        classify=classify,
        action_note=action_note,
    )


def _measure_embedding_backfill_gap(target: Path) -> BackfillGapOutcome:
    """Typed coverage measurement. Failed probes are ``unmeasurable``, not clean."""
    import sqlite3

    from workbay_bootstrap.subcommands import (
        _connect_sqlite_readonly,
        _embeddings_facet_preamble,
    )

    resolved = _embeddings_facet_preamble(target)
    if resolved is None:
        return BackfillGapOutcome(BackfillGapStatus.MEASURED_CLEAN)
    db_path = resolved / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return BackfillGapOutcome(BackfillGapStatus.MEASURED_CLEAN)
    pin = _doctor_backfill_model_id()
    if pin.import_error:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE, reason=pin.import_error
        )
    loaded = _aligned_store_contract()
    if isinstance(loaded, BackfillGapOutcome):
        return loaded
    try:
        conn = _connect_sqlite_readonly(db_path)
        try:
            return _backfill_gap_findings_from_conn(
                conn,
                sources=loaded.sources,
                model_id=pin.model_id,
                classify=loaded.classify,
                action_note=_backfill_action_note(resolved),
            )
        finally:
            conn.close()
    except (sqlite3.Error, OSError) as exc:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=f"handoff.db unreadable: {type(exc).__name__}: {exc}",
        )


def _doctor_embedding_backfill_gap(target: Path) -> list[Finding]:
    """Assert embedded-vs-eligible concept counts per embeddable entity_kind.

    Eligible = source rows whose text is non-empty after Python ``str.strip()``
    (tabs/newlines/other Unicode whitespace are empty, matching the store).
    Embedded = eligible ``(entity_kind, entity_id)`` pairs whose stored
    ``(text_hash, model_id)`` still matches the current source hash and pinned
    model. Orphan embedding rows do not cancel missing current source rows.

    Silent when the embeddings gate is disabled or the target does not declare
    ``mcp-workbay-handoff[embeddings]`` — a backfill warning is a no-op
    without a provider. Absent DB is silent (nothing to measure). Unreadable
    DB, missing ``concept_embeddings``, or a failed probe emits
    ``embedding_backfill_unmeasurable`` (severity error) rather than
    impersonating a clean corpus.
    """
    outcome = _measure_embedding_backfill_gap(target)
    if outcome.status is BackfillGapStatus.UNMEASURABLE:
        return [_unmeasurable_backfill_finding(outcome.reason or "unknown probe failure")]
    return list(outcome.findings)
