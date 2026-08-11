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

# Process-local counters for embed_concept_best_effort outcomes ([ARCH-13],
# [OBS-08] F-HB-05). Mirror anchor counters so provider-absent vs empty-text
# vs failed is distinguishable without re-reading agent_errors.
_CONCEPT_EMBED_COUNTS: dict[str, int] = {
    "stored": 0,
    "skipped": 0,
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


def _upsert_concept_embedding_row(
    conn: sqlite3.Connection,
    *,
    entity_kind: str,
    entity_id: str,
    task_ref: str,
    new_hash: str,
    vector: np.ndarray,
    model_id: str,
) -> None:
    """Write one embedding row. No provider work ([ARCH-13], [RES-02]).

    Single copy of the concept_embeddings upsert SQL: both on-write
    :func:`store_concept_embedding` and the backfill flush path route here
    so schema changes cannot silently skew the two writers ([CON-18], [DRY]).
    """
    blob = serialize_vector(vector)
    dim = int(vector.shape[0])
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
        (entity_kind, entity_id, task_ref, new_hash, dim, blob, model_id),
    )


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
        raise RuntimeError(f"provider returned unexpected batch size {vectors.shape[0]} for {entity_kind}/{entity_id}")
    _upsert_concept_embedding_row(
        conn,
        entity_kind=entity_kind,
        entity_id=entity_id,
        task_ref=task_ref,
        new_hash=new_hash,
        vector=vectors[0],
        model_id=provider.model_id,
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


# Observable outcomes for embed_concept_best_effort (AXI-5 / RES-02).
# Callers that surface degrade (e.g. close_slice) must treat ``failed`` as a
# typed partial-failure signal — never infer success from a silent return.
ConceptEmbedOutcome = Literal[
    "stored",
    "skipped",
    "skipped_no_provider",
    "skipped_empty",
    "failed",
]


def _record_concept_outcome(outcome: ConceptEmbedOutcome) -> ConceptEmbedOutcome:
    _CONCEPT_EMBED_COUNTS[outcome] = _CONCEPT_EMBED_COUNTS.get(outcome, 0) + 1
    return outcome


def get_concept_embed_counts() -> dict[str, int]:
    """Snapshot of concept embed-on-write outcomes (process-local counters)."""
    return dict(_CONCEPT_EMBED_COUNTS)


def reset_concept_embed_counts() -> None:
    """Zero the concept embed outcome counters (tests only)."""
    for key in _CONCEPT_EMBED_COUNTS:
        _CONCEPT_EMBED_COUNTS[key] = 0


def embed_concept_best_effort(
    entity_kind: str,
    entity_id: object,
    task_ref: str,
    text: str | None,
    provider: SupportsEmbed | None = None,
) -> ConceptEmbedOutcome:
    """Embed+store one concept AFTER its row committed. Best-effort; never raises.

    A ``None`` provider (artifact/extra absent) is a silent no-op so the write
    path is unchanged. Opens its own short-lived connection — the embedding is a
    derived artifact and must not hold the caller's write transaction across
    inference. Failures never raise, but they return ``\"failed\"`` so callers
    can attach a typed partial-failure signal; the resumable backfill also
    reconciles gaps. Outcomes increment :func:`get_concept_embed_counts`.
    """
    try:
        prov = provider if provider is not None else _resolve_provider()
        if prov is None:
            return _record_concept_outcome("skipped_no_provider")
        if text is None or not str(text).strip():
            return _record_concept_outcome("skipped_empty")
        with _get_db_connection() as conn:
            result = store_concept_embedding(conn, prov, entity_kind, entity_id, task_ref, text)
        if result == "empty":
            return _record_concept_outcome("skipped_empty")
        if result == "skipped":
            return _record_concept_outcome("skipped")
        return _record_concept_outcome("stored")
    except Exception as exc:  # noqa: BLE001 - derived artifact; best-effort
        # F-HB-04: under SQLITE lock/busy contention, do not re-enter handoff.db
        # for failure recording — that path adds load on the contended resource
        # and can stall the degrade arm. Log-only then return failed. Non-lock
        # failures still record agent_errors.embedding_failed for the doctor.
        try:
            from workbay_handoff_mcp.write_retry import is_sqlite_lock_error

            if is_sqlite_lock_error(exc):
                _log.warning(
                    "embed-on-write failed under sqlite lock for %s/%s task_ref=%s: %s",
                    entity_kind,
                    entity_id,
                    task_ref,
                    exc,
                )
                return _record_concept_outcome("failed")
        except Exception:  # noqa: BLE001 - classifier import/use must not raise
            pass
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
        return _record_concept_outcome("failed")


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
    for row in conn.execute("SELECT task_ref, objective, focus FROM handoff_state" + clause, params).fetchall():
        out.append(("handoff_state.objective", str(row[0]), str(row[0]), row[1]))
        out.append(("handoff_state.focus", str(row[0]), str(row[0]), row[2]))
    for row in conn.execute(
        "SELECT compaction_id, task_ref, prose_residual FROM session_compactions" + clause, params
    ).fetchall():
        out.append(("compaction.prose_residual", str(row[0]), str(row[1]), row[2]))
    return out


# Default bound on provider.embed during backfill. Aligns with the writer
# heartbeat stale window so a hung provider fails closed and exits the
# registered writer rather than advertising a live writer indefinitely
# while blocked on inference ([RES-02] / S5-A-04).
DEFAULT_BACKFILL_EMBED_TIMEOUT_SECONDS = 300.0


def _end_deferred_read_txn(conn: sqlite3.Connection) -> None:
    """End any deferred read transaction opened by classify SELECTs.

    Backfill refuses an open caller transaction at entry, so any open txn here
    is our own read snapshot — rollback (not commit) drops it without implying
    durability of foreign work ([RES-01], [DATA-20] / S5-A-05).
    """
    if conn.in_transaction:
        conn.rollback()


def _provider_embed_batch(
    provider: SupportsEmbed,
    texts: list[str],
    *,
    timeout_seconds: float | None,
) -> np.ndarray:
    """Call ``provider.embed`` with an optional wall-clock timeout.

    When ``timeout_seconds`` is set, a hung provider raises ``TimeoutError`` so
    the whole-run ``db_writer_heartbeat`` context can exit rather than pulse
    forever while inference is stuck ([RES-02]).
    """
    if timeout_seconds is None:
        return provider.embed(texts)
    # Lazy: concurrent.futures is stdlib but keep the always-on import surface thin.
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FuturesTimeout

    # wait=False on shutdown so a timed-out hung embed does not block the
    # backfill (and its writer-heartbeat exit) forever on the orphaned worker.
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(provider.embed, texts)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeout as exc:
            raise TimeoutError(
                f"provider.embed timed out after {timeout_seconds}s "
                f"(batch size {len(texts)})"
            ) from exc
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def backfill_concept_embeddings(
    conn: sqlite3.Connection,
    provider: SupportsEmbed,
    *,
    task_ref: str | None = None,
    commit_every: int = 200,
    embed_timeout_seconds: float | None = DEFAULT_BACKFILL_EMBED_TIMEOUT_SECONDS,
) -> dict[str, int]:
    """Embed every concept missing or stale in ``concept_embeddings``.

    Idempotent + resumable: the ``(text_hash, model_id)`` gate makes a re-run a
    no-op for already-embedded concepts, so an interrupted run resumes by simply
    running again. Returns ``{stored, skipped, empty}`` counts.

    Provider work never runs under the SQLite write lock (internal /
    [RES-02]): each chunk of at most ``commit_every`` concepts is classified
    with short-lived deferred reads, embedded in **one** ``provider.embed``
    call outside any write transaction, then written under ``BEGIN IMMEDIATE``
    and committed. Crash resume is durable at chunk boundaries.

    Memory honesty ([DATA-16]): ``_gather_concepts`` still materializes the full
    candidate corpus (including text) before the loop; ``commit_every`` bounds
    only the per-chunk provider batch and write transaction — not gather-phase
    resident memory.

    Refuses a connection that already has an open transaction: a callee must
    not commit work it did not begin ([RES-01], [RES-06]). Callers must commit
    or rollback before entry.

    Registers as a long-lived DB writer for the whole run (internal) so the
    sidecar reaper can observe this process while a write phase holds the lock.
    Heartbeats are pulsed by the context manager's side thread (default interval
    30s, well under the 300s stale window). ``embed_timeout_seconds`` (default
    300s; pass ``None`` to disable) bounds each provider batch so a hung embed
    fails with ``TimeoutError`` instead of advertising a live writer forever.
    """
    if commit_every < 1:
        raise ValueError(f"commit_every must be >= 1, got {commit_every}")

    # Lazy imports: keep the optional embeddings import graph free of the
    # always-on liveness module at module load; reaper/runtime are cheap.
    from workbay_handoff_mcp.db_writer_liveness import db_writer_heartbeat
    from workbay_handoff_mcp.runtime import get_runtime_config

    # RES-01 / RES-06: never silently end a transaction the caller began.
    # Guard sits above every connection touch (including gather).
    if conn.in_transaction:
        raise RuntimeError(
            "backfill_concept_embeddings refused an open transaction on the "
            "caller connection; commit or rollback first"
        )

    label = (
        f"concept_embedding_backfill task_ref={task_ref}"
        if task_ref
        else "concept_embedding_backfill"
    )
    db_path = get_runtime_config().db_path
    counts = {"stored": 0, "skipped": 0, "empty": 0}
    with db_writer_heartbeat(db_path, label=label):
        concepts = _gather_concepts(conn, task_ref)
        for offset in range(0, len(concepts), commit_every):
            chunk = concepts[offset : offset + commit_every]
            # --- classify (reads only; no write transaction) ----------------
            to_store: list[tuple[str, str, str, str, str]] = []  # kind, id, tref, text, hash
            for entity_kind, entity_id, tref, text in chunk:
                if text is None or not str(text).strip():
                    counts["empty"] += 1
                    continue
                new_hash = text_hash(str(text))
                existing = _existing_hash_and_model(conn, entity_kind, entity_id)
                if existing == (new_hash, provider.model_id):
                    counts["skipped"] += 1
                    continue
                to_store.append((entity_kind, entity_id, tref, str(text), new_hash))

            if not to_store:
                continue

            # Drop deferred read snapshot before the provider wait so we never
            # hold a transaction across embed ([DATA-20], [RES-02]).
            _end_deferred_read_txn(conn)

            # --- embed (one provider call for the whole remaining chunk) ----
            capped = [truncate_embed_text(text) for *_, text, _hash in to_store]
            vectors = _provider_embed_batch(
                provider, capped, timeout_seconds=embed_timeout_seconds
            )
            if vectors.shape[0] != len(capped):
                raise RuntimeError(
                    f"provider returned unexpected batch size {vectors.shape[0]} "
                    f"for backfill chunk of {len(capped)}"
                )

            # --- write + commit under BEGIN IMMEDIATE ([RES-02], HARM-A-06) --
            conn.execute("BEGIN IMMEDIATE")
            try:
                for i, (entity_kind, entity_id, tref, _text, new_hash) in enumerate(to_store):
                    _upsert_concept_embedding_row(
                        conn,
                        entity_kind=entity_kind,
                        entity_id=entity_id,
                        task_ref=tref,
                        new_hash=new_hash,
                        vector=vectors[i],
                        model_id=provider.model_id,
                    )
                    counts["stored"] += 1
                conn.commit()
            except BaseException:
                if conn.in_transaction:
                    conn.rollback()
                raise
    return counts
