"""Doctor facet: embedded-vs-eligible concept coverage.

Extracted from ``subcommands`` so the god-module only registers the call.
``doctor()`` imports ``_doctor_embedding_backfill_gap`` and extends findings.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

Finding = dict[str, str]


class BackfillGapStatus(str, Enum):
    """Typed measurement result — never collapse a failed probe to "no gap"."""

    MEASURED_GAP = "measured_gap"
    MEASURED_CLEAN = "measured_clean"
    UNMEASURABLE = "unmeasurable"
    SKIPPED = "skipped"


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
    kinds: tuple[str, ...]
    sources: tuple[tuple[str, str, str, str], ...]
    gather: Callable[..., list[tuple[str, str, str, str | None]]]
    classify: Callable[
        [object, tuple[str, str] | None, str | None], tuple[str, str | None]
    ]


# Extra runtimes whose absence is "instrument unavailable", not a broken store.
# Keep in lockstep with subcommands._EMBEDDINGS_RUNTIME_MODULES.
_EMBEDDINGS_EXTRA_RUNTIMES = frozenset({"numpy", "onnxruntime", "tokenizers"})


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
            classify_coverage_row,
            gather_concepts,
        )
    except ModuleNotFoundError as exc:
        missing = (getattr(exc, "name", None) or "").split(".", 1)[0]
        if missing in _EMBEDDINGS_EXTRA_RUNTIMES:
            # Probe instrument missing in *this* interpreter (numpy / extra).
            # Not an unreadable DB — same class as an undeclared extra.
            return None, f"instrument_unavailable: {type(exc).__name__}: {exc}"
        return None, f"store backfill contract import failed: {type(exc).__name__}: {exc}"
    except ImportError as exc:
        # The store module IS present but one of the expected names is not
        # (API drift between this doctor and the store contract it pins).
        # This is a real contract failure, not an absent instrument — do
        # not downgrade it to the same warning-severity
        # ``instrument_unavailable`` reason (CL0816-SEMRET-R3REV-claude-07b).
        return None, f"store backfill contract API drift: {type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001 — failed contract is unmeasurable
        return None, f"store backfill contract import failed: {type(exc).__name__}: {exc}"
    return (
        StoreBackfillContract(
            kinds=tuple(CONCEPT_ENTITY_KINDS),
            sources=tuple(CONCEPT_BACKFILL_SOURCES),
            gather=gather_concepts,
            classify=classify_coverage_row,
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
        if contract_error and contract_error.startswith("instrument_unavailable:"):
            return BackfillGapOutcome(
                BackfillGapStatus.SKIPPED,
                reason=contract_error,
            )
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=contract_error or "store backfill contract unavailable",
        )
    drift = _source_kind_drift_reason(contract)
    if drift:
        return BackfillGapOutcome(BackfillGapStatus.UNMEASURABLE, reason=drift)
    return contract


class _EmbeddingEnvUnavailable(Exception):
    """``workbay_handoff_mcp.embedding_env`` could not be imported.

    Distinct from a disabled/absent gate so the caller routes to
    ``instrument_unavailable`` (UNMEASURABLE, warning) instead of silently
    treating the corpus as clean (CL0816-SEMRET-R3REV-claude-02).
    """


def _doctor_embedding_env_map(target: Path) -> dict[str, str]:
    """File pins with nonempty-process-overlay (empty process values lose).

    Delegates to ``workbay_handoff_mcp.embedding_env.merge_embedding_env`` —
    the same helper the backfill CLI applies via ``apply_embedding_env`` —
    so file-only vs process-only cannot drift. This is not the hook
    presence-wins set-if-unset rule.

    ``mcp-workbay-handoff`` is only a ``dev`` dependency of
    workbay-bootstrap: guard the import so an interpreter that lacks the
    package raises a typed, catchable ``_EmbeddingEnvUnavailable`` instead
    of an uncaught ``ModuleNotFoundError`` (every other
    ``workbay_handoff_mcp`` import in this package is guarded the same way).
    """
    try:
        from workbay_handoff_mcp.embedding_env import merge_embedding_env
    except ImportError as exc:
        raise _EmbeddingEnvUnavailable(
            f"instrument_unavailable: workbay_handoff_mcp.embedding_env "
            f"unimportable: {type(exc).__name__}: {exc}"
        ) from exc

    try:
        return merge_embedding_env(target)
    except (OSError, UnicodeDecodeError):
        # Shared loader does not catch I/O; do not crash the facet or
        # mislabel an unreadable env file as an unreadable handoff.db.
        return {}


def _doctor_embeddings_disabled(target: Path) -> bool:
    """True when the overlaid env explicitly disables the embeddings gate."""
    raw = _doctor_embedding_env_map(target).get("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED")
    if raw is None:
        return False
    return str(raw).strip().lower() in {"1", "true", "yes"}


def _backfill_action_note(target: Path) -> str | None:
    """Override the backfill remedy when a provider cannot actually be built.

    ``make backfill-handoff-embeddings`` fails closed when artifacts are
    unconfigured, missing on disk, hash-mismatched, or the gate is disabled.
    Name the gap so the finding is actionable (AGT-21 / OBS-01). Operator
    strings live here; the probe is ``from_env`` → ``verify_artifacts``.
    """
    try:
        from workbay_handoff_mcp.embeddings.provider import probe_embedding_provider

        probe = probe_embedding_provider(_doctor_embedding_env_map(target))
    except Exception as exc:  # noqa: BLE001 — still measure; don't claim backfill works
        return (
            f"EmbeddingProvider import failed ({type(exc).__name__}: {exc}); "
            "install mcp-workbay-handoff[embeddings] before "
            "`make backfill-handoff-embeddings`"
        )
    reason = getattr(probe, "reason", None)
    if reason == "unconfigured":
        missing = tuple(getattr(probe, "missing_keys", ()) or ())
        missing_label = ", ".join(missing) if missing else "WORKBAY_HANDOFF_EMBEDDING_*"
        return (
            f"embeddings artifacts unconfigured (missing {missing_label}); "
            "set them via `.workbay/embedding.env` or "
            "`workbay-bootstrap provision-embeddings` before "
            "`make backfill-handoff-embeddings`"
        )
    if reason == "disabled":
        return (
            "embeddings gate is disabled; "
            "enable with `workbay embeddings --enable` "
            "(clears WORKBAY_HANDOFF_EMBEDDINGS_DISABLED) before "
            "`make backfill-handoff-embeddings`"
        )
    if reason == "artifact_missing":
        path = getattr(probe, "path", None) or "unknown path"
        return (
            f"embeddings artifact missing ({path}); "
            "repair `.workbay/embedding.env` before "
            "`make backfill-handoff-embeddings`"
        )
    if reason == "artifact_sha_mismatch":
        path = getattr(probe, "path", None) or "unknown path"
        return (
            f"embeddings artifact sha mismatch ({path}); "
            "repair the pin or the file before "
            "`make backfill-handoff-embeddings`"
        )
    if reason is not None:
        detail = getattr(probe, "error", None) or reason
        return (
            f"embeddings artifacts unreadable ({detail}); "
            "repair `.workbay/embedding.env` before "
            "`make backfill-handoff-embeddings`"
        )
    return None


def _load_embedding_index(
    conn: object,
) -> dict[tuple[str, str], tuple[str, str]] | BackfillGapOutcome:
    """One full-table read of concept_embeddings — no per-row SELECTs."""
    import sqlite3

    try:
        rows = conn.execute(  # type: ignore[attr-defined]
            "SELECT entity_kind, entity_id, text_hash, model_id FROM concept_embeddings"
        ).fetchall()
    except sqlite3.Error as exc:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=f"concept_embeddings unreadable: {type(exc).__name__}: {exc}",
        )
    return {
        (str(kind), str(entity_id)): (str(stored_hash), str(stored_model))
        for kind, entity_id, stored_hash, stored_model in rows
    }


def _iter_present_concepts(
    conn: object,
    tables: set[str],
    sources: tuple[tuple[str, str, str, str], ...],
) -> list[tuple[str, str, object]] | BackfillGapOutcome:
    """One SELECT per present source table (review_findings read once)."""
    import sqlite3

    grouped: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for kind, table, id_col, text_col in sources:
        if table not in tables:
            continue
        grouped.setdefault((table, id_col), []).append((kind, text_col))
    out: list[tuple[str, str, object]] = []
    try:
        for (table, id_col), kind_cols in grouped.items():
            text_cols = [text_col for _kind, text_col in kind_cols]
            sql = (
                f"SELECT CAST({id_col} AS TEXT), "
                + ", ".join(text_cols)
                + f" FROM {table}"
            )
            for row in conn.execute(sql).fetchall():  # type: ignore[attr-defined]
                entity_id = str(row[0])
                for (kind, _text_col), text in zip(kind_cols, row[1:], strict=True):
                    out.append((kind, entity_id, text))
    except sqlite3.Error as exc:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=f"source table unreadable: {type(exc).__name__}: {exc}",
        )
    return out


def _source_tables_nonempty(
    conn: object,
    tables: set[str],
    sources: tuple[tuple[str, str, str, str], ...],
) -> bool | BackfillGapOutcome:
    """True when any CONCEPT_BACKFILL_SOURCES table present here has a row."""
    import sqlite3

    seen: set[str] = set()
    try:
        for _kind, table, _id_col, _text in sources:
            if table not in tables or table in seen:
                continue
            seen.add(table)
            row = conn.execute(  # type: ignore[attr-defined]
                f"SELECT 1 FROM {table} LIMIT 1"
            ).fetchone()
            if row is not None:
                return True
    except sqlite3.Error as exc:
        return BackfillGapOutcome(
            BackfillGapStatus.UNMEASURABLE,
            reason=f"source table unreadable: {type(exc).__name__}: {exc}",
        )
    return False


def _collect_concepts(
    conn: object,
    tables: set[str],
    contract: StoreBackfillContract,
) -> list[tuple[str, str, object]] | BackfillGapOutcome:
    """Prefer gather_concepts when every source table exists; else present tables."""
    import sqlite3

    needed = {table for _kind, table, _id_col, _text in contract.sources}
    if needed <= tables:
        try:
            gathered = contract.gather(conn, None)
        except sqlite3.Error as exc:
            return BackfillGapOutcome(
                BackfillGapStatus.UNMEASURABLE,
                reason=f"gather_concepts failed: {type(exc).__name__}: {exc}",
            )
        collected = [
            (kind, str(entity_id), text)
            for kind, entity_id, _tref, text in gathered
        ]
        if not collected:
            nonempty = _source_tables_nonempty(conn, tables, contract.sources)
            if isinstance(nonempty, BackfillGapOutcome):
                return nonempty
            if nonempty:
                return BackfillGapOutcome(
                    BackfillGapStatus.UNMEASURABLE,
                    reason=(
                        "gather_concepts returned no rows while source tables "
                        "are non-empty"
                    ),
                )
        return collected
    return _iter_present_concepts(conn, tables, contract.sources)


def _gap_findings_for_concepts(
    concepts: list[tuple[str, str, object]],
    index: dict[tuple[str, str], tuple[str, str]],
    *,
    kinds: tuple[str, ...],
    model_id: str | None,
    action_note: str | None,
    contract: StoreBackfillContract,
) -> BackfillGapOutcome:
    eligible = {kind: 0 for kind in kinds}
    embedded = {kind: 0 for kind in kinds}
    for kind, entity_id, text in concepts:
        if kind not in eligible:
            continue
        outcome, _new_hash = contract.classify(
            text, index.get((kind, entity_id)), model_id
        )
        if outcome == "empty":
            continue
        eligible[kind] += 1
        if outcome == "skipped":
            embedded[kind] += 1
    findings: list[Finding] = []
    for kind in kinds:
        if eligible[kind] - embedded[kind] <= 0:
            continue
        findings.append(
            _backfill_gap_finding(
                kind, eligible[kind], embedded[kind], action_note=action_note
            )
        )
    if findings:
        return BackfillGapOutcome(
            BackfillGapStatus.MEASURED_GAP, findings=tuple(findings)
        )
    return BackfillGapOutcome(BackfillGapStatus.MEASURED_CLEAN)


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
    # ImportError / missing numpy is UNMEASURABLE (AGT-21) but not a broken
    # DB probe — keep it distinct from error-severity unreadability.
    severity = "warning" if reason.startswith("instrument_unavailable:") else "error"
    return {
        "kind": "embedding_backfill_unmeasurable",
        "path": ".task-state/handoff.db",
        "severity": severity,
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


def _backfill_gap_findings_from_conn(
    conn: object,
    *,
    contract: StoreBackfillContract,
    model_id: str | None,
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
    index = _load_embedding_index(conn)
    if isinstance(index, BackfillGapOutcome):
        return index
    concepts = _collect_concepts(conn, tables, contract)
    if isinstance(concepts, BackfillGapOutcome):
        return concepts
    return _gap_findings_for_concepts(
        concepts,
        index,
        kinds=contract.kinds,
        model_id=model_id,
        action_note=action_note,
        contract=contract,
    )


def _measure_embedding_backfill_gap(target: Path) -> BackfillGapOutcome:
    """Typed coverage measurement. Failed probes are ``unmeasurable``, not clean."""
    import sqlite3

    from workbay_bootstrap.subcommands import (
        EmbeddingsFacetMiss,
        _connect_sqlite_readonly,
        _embeddings_facet_preamble,
    )

    resolved = _embeddings_facet_preamble(target)
    if resolved.target is None:
        if resolved.miss in {
            EmbeddingsFacetMiss.UNREADABLE,
            EmbeddingsFacetMiss.IMPORT_FAILED,
        }:
            return BackfillGapOutcome(
                BackfillGapStatus.UNMEASURABLE,
                reason=resolved.detail
                or (
                    "embedding.env unreadable"
                    if resolved.miss is EmbeddingsFacetMiss.UNREADABLE
                    else "embedding provision import failed"
                ),
            )
        return BackfillGapOutcome(
            BackfillGapStatus.SKIPPED,
            reason=resolved.miss.value if resolved.miss else "preamble skipped",
        )
    target = resolved.target
    db_path = target / ".task-state" / "handoff.db"
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
                contract=loaded,
                model_id=pin.model_id,
                action_note=_backfill_action_note(target),
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

    Silent when the embeddings gate is disabled, the target does not declare
    ``mcp-workbay-handoff[embeddings]``, or this interpreter cannot import
    an embeddings-extra runtime (numpy / onnxruntime / tokenizers —
    ``BackfillGapStatus.SKIPPED``, not ``MEASURED_CLEAN``). Absent DB is
    silent (nothing to measure). Unreadable ``.workbay/embedding.env``, a
    failed embedding-provision import, an unreadable DB, a missing
    non-optional store import, a partial store import
    (``ImportError: cannot import name`` — reported as ``store backfill
    contract API drift``, distinct from an absent instrument), or missing
    ``concept_embeddings`` emits ``embedding_backfill_unmeasurable``
    (severity error) rather than
    impersonating a clean corpus.
    """
    outcome = _measure_embedding_backfill_gap(target)
    if outcome.status is BackfillGapStatus.UNMEASURABLE:
        return [_unmeasurable_backfill_finding(outcome.reason or "unknown probe failure")]
    return list(outcome.findings)
