"""concept_embeddings store: embed-on-write + backfill over handoff concepts.

internal. Canonical storage is a little-endian float32 vector BLOB
keyed by ``(entity_kind, entity_id)``; re-embed is gated on ``(text_hash,
model_id)`` so an unchanged concept embedded by the same model is never
re-embedded. Embedding runs best-effort *after* the concept row's own write has
committed (see :func:`embed_concept_best_effort`) — never inside the write
transaction — so provider absence or inference failure leaves the write path
byte-identical to today, and any gap is reconciled by the resumable backfill.

Prose-only corpus (implementation note S2 / [OBS-08]): reinjection embeds **task-memory
prose only** — findings, decisions, blockers, objectives/focus, and compaction
residuals. Code and path search belong to codemap, not embeddings.
``handoff_state.task_plan_path`` is dropped: it never resolves in reinjection
(``_REF_KINDS`` / ``_RESOLVER_SPECS``) and a filesystem path encodes nothing
cosine can use. Existing vectors of that kind are purged at DB open (idempotent
DELETE; no schema-version bump).

This module imports numpy and therefore belongs to the optional ``embeddings``
subpackage; the core server's write paths import it lazily and treat an
``ImportError`` (numpy/extra absent) as a no-op.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from typing import Literal, Protocol

import numpy as np

from workbay_handoff_mcp.embeddings.provider import truncate_embed_text
from workbay_handoff_mcp.shared_schema import _get_db_connection

_log = logging.getLogger("workbay_handoff_mcp")

# Observable outcomes for store_compaction_anchor_best_effort (AXI-5).
# Counters distinguish "no anchor needed" from "anchor failed N times".
AnchorStoreOutcome = Literal["stored", "skipped_no_provider", "skipped_empty", "failed"]

_ANCHOR_STORE_COUNTS: dict[str, int] = {
    "stored": 0,
    "skipped_no_provider": 0,
    "skipped_empty": 0,
    "failed": 0,
}

# The fixed enumeration of embeddable concept kinds (single source of truth for
# what gets embedded). Each value is "<entity>.<field>". Prose only — paths and
# code belong to codemap (implementation note S2).
CONCEPT_ENTITY_KINDS: tuple[str, ...] = (
    "decision.rationale",
    "finding.description",
    "finding.fix",
    "finding.resolution_notes",
    "blocker.description",
    "handoff_state.objective",
    "handoff_state.focus",
    "compaction.prose_residual",
)


class SupportsEmbed(Protocol):
    """The slice-1 EmbeddingProvider surface the store depends on."""

    @property
    def dim(self) -> int: ...

    @property
    def model_id(self) -> str: ...

    def embed(self, texts: list[str]) -> np.ndarray: ...


def text_hash(text: str) -> str:
    """Stable SHA-256 hex of the concept text — the re-embed idempotency key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def serialize_vector(vector: np.ndarray) -> bytes:
    """Canonical little-endian float32 bytes (``dim * 4``), host-byte-order independent."""
    return np.asarray(vector, dtype="<f4").reshape(-1).tobytes()


def deserialize_vector(blob: bytes) -> np.ndarray:
    """Inverse of :func:`serialize_vector` (returns an owned, writable copy)."""
    return np.frombuffer(blob, dtype="<f4").copy()


def _existing_hash_and_model(conn: sqlite3.Connection, entity_kind: str, entity_id: str) -> tuple[str, str] | None:
    row = conn.execute(
        "SELECT text_hash, model_id FROM concept_embeddings WHERE entity_kind = ? AND entity_id = ?",
        (entity_kind, entity_id),
    ).fetchone()
    if row is None:
        return None
    return (str(row[0]), str(row[1]))


def store_concept_embedding(
    conn: sqlite3.Connection,
    provider: SupportsEmbed,
    entity_kind: str,
    entity_id: object,
    task_ref: str,
    text: str | None,
) -> str:
    """Embed and upsert one concept within ``conn`` (caller owns the transaction).

    Idempotent: returns ``"skipped"`` without calling ``provider.embed`` when a
    row for ``(entity_kind, entity_id)`` already carries the same text_hash and
    model_id. Returns ``"empty"`` for missing/blank text (no row written),
    ``"stored"`` when a vector was (re-)written.
    """
    entity_id = str(entity_id)
    if text is None or not text.strip():
        return "empty"
    new_hash = text_hash(text)
    existing = _existing_hash_and_model(conn, entity_kind, entity_id)
    if existing == (new_hash, provider.model_id):
        return "skipped"
    # Cap before the provider so stub providers and the real path see bounded text.
    vectors = provider.embed([truncate_embed_text(text)])
    if vectors.shape[0] != 1:
        # Non-blank text that fails to yield a singleton batch is a real embed
        # failure — do not alias it to the empty-text no-op (which never records
        # embedding_failed via best-effort's except path).
        raise RuntimeError(
            f"provider returned unexpected batch size {vectors.shape[0]} "
            f"for {entity_kind}/{entity_id}"
        )
    blob = serialize_vector(vectors[0])
    dim = int(vectors.shape[1])
    conn.execute(
        """
        INSERT INTO concept_embeddings
            (entity_kind, entity_id, task_ref, text_hash, dim, vector, model_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(entity_kind, entity_id) DO UPDATE SET
            task_ref   = excluded.task_ref,
            text_hash  = excluded.text_hash,
            dim        = excluded.dim,
            vector     = excluded.vector,
            model_id   = excluded.model_id,
            created_at = excluded.created_at
        """,
        (entity_kind, entity_id, task_ref, new_hash, dim, blob, provider.model_id),
    )
    return "stored"


# --- provider resolution + after-commit hook -------------------------------

# Cache sentinel: empty list = unresolved; [provider] / [None] = resolved.
_PROVIDER_CACHE: list[SupportsEmbed | None] = []


def _resolve_provider() -> SupportsEmbed | None:
    """Opt-in provider from the env config, cached. ``None`` => clean degrade."""
    if not _PROVIDER_CACHE:
        from workbay_handoff_mcp.embeddings.provider import EmbeddingProvider

        _PROVIDER_CACHE.append(EmbeddingProvider.from_env())
    return _PROVIDER_CACHE[0]


def set_provider_for_testing(provider: SupportsEmbed | None) -> None:
    """Override the cached provider (tests only)."""
    _PROVIDER_CACHE[:] = [provider]


def reset_provider_cache() -> None:
    """Drop the cached provider so the next resolve re-reads the env."""
    _PROVIDER_CACHE.clear()


def embed_concept_best_effort(
    entity_kind: str,
    entity_id: object,
    task_ref: str,
    text: str | None,
    provider: SupportsEmbed | None = None,
) -> None:
    """Embed+store one concept AFTER its row committed. Best-effort; never raises.

    A ``None`` provider (artifact/extra absent) is a silent no-op so the write
    path is unchanged. Opens its own short-lived connection — the embedding is a
    derived artifact and must not hold the caller's write transaction across
    inference. Any failure is swallowed; the resumable backfill reconciles gaps.
    """
    try:
        prov = provider if provider is not None else _resolve_provider()
        if prov is None or text is None or not str(text).strip():
            return
        with _get_db_connection() as conn:
            store_concept_embedding(conn, prov, entity_kind, entity_id, task_ref, text)
    except Exception as exc:  # noqa: BLE001 - derived artifact; best-effort
        try:
            # Runtime-aware writer: lands in get_runtime_config().db_path (the DB
            # doctor reads). record_agent_error_direct resolves cwd and can miss
            # under custom WORKBAY_HANDOFF_STATE_DIR / MCP cwd outside workspace.
            from workbay_handoff_mcp.agent_errors import record_agent_error

            record_agent_error(
                error_class="embedding_failed",
                summary=f"embed-on-write failed: {entity_kind}",
                detail=f"{entity_kind}/{entity_id}: {exc}",
                task_ref=task_ref,
            )
        except Exception:  # noqa: BLE001 - recording must not raise out of best-effort
            pass
        _log.debug("embed-on-write skipped for %s/%s: %s", entity_kind, entity_id, exc)


def get_anchor_store_counts() -> dict[str, int]:
    """Snapshot of compaction-anchor store outcomes (process-local counters)."""
    return dict(_ANCHOR_STORE_COUNTS)


def reset_anchor_store_counts() -> None:
    """Zero the compaction-anchor outcome counters (tests only)."""
    for key in _ANCHOR_STORE_COUNTS:
        _ANCHOR_STORE_COUNTS[key] = 0


def _record_anchor_outcome(outcome: AnchorStoreOutcome) -> AnchorStoreOutcome:
    _ANCHOR_STORE_COUNTS[outcome] = _ANCHOR_STORE_COUNTS.get(outcome, 0) + 1
    return outcome


def store_compaction_anchor_best_effort(
    compaction_id: object,
    task_ref: str,
    text: str | None,
    provider: SupportsEmbed | None = None,
) -> AnchorStoreOutcome:
    """Embed transcript text + persist it as ``session_compactions.anchor_vector``.

    internal. Best-effort, post-commit (own short-lived connection): a
    ``None`` provider (artifact/extra absent) or blank text is a no-op,
    leaving ``anchor_vector`` NULL so the reinjection read path degrades to
    today's selection. Failures never raise — but they are **observable**:
    returns a typed outcome, increments :func:`get_anchor_store_counts`, and
    logs a WARNING with the exception class so a silent permanent failure
    (e.g. 87/87 anchors NULL) is no longer invisible (AXI-5).
    """
    try:
        prov = provider if provider is not None else _resolve_provider()
        if prov is None:
            return _record_anchor_outcome("skipped_no_provider")
        if text is None or not str(text).strip():
            return _record_anchor_outcome("skipped_empty")
        # Cap before the provider so stub providers receive bounded text.
        capped = truncate_embed_text(str(text))
        vectors = prov.embed([capped])
        if vectors.shape[0] != 1:
            _log.warning(
                "anchor embed-on-write failed for compaction_id=%s task_ref=%s "
                "error_type=UnexpectedVectorCount vector_rows=%s",
                compaction_id,
                task_ref,
                vectors.shape[0],
            )
            return _record_anchor_outcome("failed")
        blob = serialize_vector(vectors[0])
        with _get_db_connection() as conn:
            conn.execute(
                "UPDATE session_compactions SET anchor_vector = ? WHERE compaction_id = ? AND task_ref = ?",
                (blob, str(compaction_id), task_ref),
            )
            conn.commit()
        return _record_anchor_outcome("stored")
    except Exception as exc:  # noqa: BLE001 - derived artifact; best-effort
        _log.warning(
            "anchor embed-on-write failed for compaction_id=%s task_ref=%s error_type=%s: %s",
            compaction_id,
            task_ref,
            type(exc).__name__,
            exc,
        )
        return _record_anchor_outcome("failed")


# --- backfill --------------------------------------------------------------


def _gather_concepts(conn: sqlite3.Connection, task_ref: str | None) -> list[tuple[str, str, str, str | None]]:
    """Collect every embeddable concept as ``(entity_kind, entity_id, task_ref, text)``.

    Each source table is fully materialized before the backfill issues any
    INSERT, so iterating the work list never races with writes on ``conn``. The
    entity_id matches embed-on-write: row id for decisions/findings/blockers,
    task_ref for handoff_state, compaction_id for compactions.
    """
    clause = " WHERE task_ref = ?" if task_ref else ""
    params: tuple[object, ...] = (task_ref,) if task_ref else ()
    out: list[tuple[str, str, str, str | None]] = []
    for row in conn.execute("SELECT id, task_ref, rationale FROM decisions" + clause, params).fetchall():
        out.append(("decision.rationale", str(row[0]), str(row[1]), row[2]))
    for row in conn.execute(
        "SELECT id, task_ref, description, fix, resolution_notes FROM review_findings" + clause, params
    ).fetchall():
        out.append(("finding.description", str(row[0]), str(row[1]), row[2]))
        out.append(("finding.fix", str(row[0]), str(row[1]), row[3]))
        out.append(("finding.resolution_notes", str(row[0]), str(row[1]), row[4]))
    for row in conn.execute("SELECT id, task_ref, description FROM blockers" + clause, params).fetchall():
        out.append(("blocker.description", str(row[0]), str(row[1]), row[2]))
    for row in conn.execute(
        "SELECT task_ref, objective, focus FROM handoff_state" + clause, params
    ).fetchall():
        out.append(("handoff_state.objective", str(row[0]), str(row[0]), row[1]))
        out.append(("handoff_state.focus", str(row[0]), str(row[0]), row[2]))
    for row in conn.execute(
        "SELECT compaction_id, task_ref, prose_residual FROM session_compactions" + clause, params
    ).fetchall():
        out.append(("compaction.prose_residual", str(row[0]), str(row[1]), row[2]))
    return out


def backfill_concept_embeddings(
    conn: sqlite3.Connection,
    provider: SupportsEmbed,
    *,
    task_ref: str | None = None,
    commit_every: int = 200,
) -> dict[str, int]:
    """Embed every concept missing or stale in ``concept_embeddings``.

    Idempotent + resumable: the ``(text_hash, model_id)`` gate makes a re-run a
    no-op for already-embedded concepts, so an interrupted run resumes by simply
    running again. Commits every ``commit_every`` writes so progress survives
    interruption. Returns ``{stored, skipped, empty}`` counts.
    """
    counts = {"stored": 0, "skipped": 0, "empty": 0}
    processed = 0
    for entity_kind, entity_id, tref, text in _gather_concepts(conn, task_ref):
        counts[store_concept_embedding(conn, provider, entity_kind, entity_id, tref, text)] += 1
        processed += 1
        if processed % commit_every == 0:
            conn.commit()
    conn.commit()
    return counts
