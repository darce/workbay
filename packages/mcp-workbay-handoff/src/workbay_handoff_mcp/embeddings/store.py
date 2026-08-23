"""concept_embeddings store: embed-on-write + backfill over handoff concepts.

internal / storage-format GREEN. Canonical storage is a little-endian
int8 vector BLOB (1 byte/dim, dequant ``q / 127``) keyed by ``(entity_kind,
entity_id)``; re-embed is gated on ``(text_hash, model_id)`` so an unchanged
concept embedded by the same model is never re-embedded. Duplicate
``(text_hash, model_id)`` payloads are interned — one physical BLOB, empty
placeholders on sharer rows. Legacy float32 blobs (``dim * 4`` bytes) remain
readable and are rewritten in place without calling the provider. Embedding
runs best-effort *after* the concept row's own write has committed (see
:func:`embed_concept_best_effort`) — never inside the write transaction — so
provider absence or inference failure leaves the write path byte-identical to
today, and any gap is reconciled by the resumable backfill.

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
import threading
from collections.abc import Iterable
from typing import Literal, Protocol

import numpy as np

from workbay_handoff_mcp.embeddings.provider import EMBEDDING_DIM, truncate_embed_text
from workbay_handoff_mcp.shared_schema import (
    _get_db_connection,
    _transfer_interned_embedding_payload,
)

# Int8 dequant scale: pre-norm is q/127. Serialize stretches by max(|x|) so
# 768-d random unit vectors use the full int8 range (naive x*127 clips cosine
# below 0.999); deserialize L2-renormalizes so ranking dots stay comparable.
_INT8_SCALE = 127.0
_EMPTY_VECTOR_BLOB = b""

_log = logging.getLogger("workbay_handoff_mcp")

# Interned sharer whose owner BLOB is gone ([OBS-08]). Ranking used to skip
# silently; the counter is the census that skip hid.
interned_payload_missing_owner_count = 0
_interned_payload_lock = threading.Lock()

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
    "decision.title",
    "decision.rationale",
    "finding.description",
    "finding.fix",
    "finding.resolution_notes",
    "blocker.description",
    "handoff_state.objective",
    "handoff_state.focus",
    "compaction.prose_residual",
)

# Row-identity half of the backfill contract: (entity_kind, table, id_column,
# text_column). Doctor and other read-only counters must use this instead of a
# local SQL projection so a column/table rename cannot silently diverge from
# _gather_concepts. Additive export only — _gather_concepts is not rewritten
# to iterate this tuple (write-path SQL stays as-is).
CONCEPT_BACKFILL_SOURCES: tuple[tuple[str, str, str, str], ...] = (
    ("decision.title", "decisions", "id", "decision"),
    ("decision.rationale", "decisions", "id", "rationale"),
    ("finding.description", "review_findings", "id", "description"),
    ("finding.fix", "review_findings", "id", "fix"),
    ("finding.resolution_notes", "review_findings", "id", "resolution_notes"),
    ("blocker.description", "blockers", "id", "description"),
    ("handoff_state.objective", "handoff_state", "task_ref", "objective"),
    ("handoff_state.focus", "handoff_state", "task_ref", "focus"),
    ("compaction.prose_residual", "session_compactions", "compaction_id", "prose_residual"),
)


class SupportsEmbed(Protocol):
    """The slice-1 EmbeddingProvider surface the store depends on.

    Test fakes implement this protocol. Artifact verification is a separate
    ``VerifyArtifacts`` surface on the production ``EmbeddingProvider``.
    """

    @property
    def dim(self) -> int: ...

    @property
    def model_id(self) -> str: ...

    def embed(self, texts: list[str]) -> np.ndarray: ...


class VerifyArtifacts(Protocol):
    """Cheap hash-only artifact check used by the backfill CLI and doctor."""

    def verify_artifacts(self) -> None: ...


def text_hash(text: str) -> str:
    """Stable SHA-256 hex of the concept text — the re-embed idempotency key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def serialize_vector(vector: np.ndarray) -> bytes:
    """Canonical little-endian int8 bytes (``dim``), decoded as ``q / 127``.

    Quantization is ``q = clip(round(x * 127 / peak), -127, 127)`` where
    ``peak = max(|x|)`` (or 1 when the vector is empty). Dequant ``q / 127``
    recovers the *direction*; :func:`deserialize_vector` L2-renormalizes so
    ranking dots stay comparable across rows. A naive ``x * 127`` on a 768-d
    random unit vector only uses ~16 of 127 levels and drops cosine below
    the 0.999 rewrite bar; stretching to the int8 range keeps the on-disk
    dtype as dim signed int8 with no appended per-row scale float.
    """
    vec = np.asarray(vector, dtype=np.float64).reshape(-1)
    peak = float(np.max(np.abs(vec))) if vec.size else 0.0
    if peak <= 0.0:
        return np.zeros(vec.shape[0], dtype="<i1").tobytes()
    quantized = np.clip(np.rint(vec * (_INT8_SCALE / peak)), -_INT8_SCALE, _INT8_SCALE)
    return np.asarray(quantized, dtype="<i1").tobytes()


def deserialize_vector(blob: bytes, dim: int | None = None) -> np.ndarray:
    """Decode an int8 or legacy float32 payload.

    ``dim is None`` is the legacy-compatible default: a payload of
    ``EMBEDDING_DIM`` bytes decodes as signed int8 (``q / 127`` then L2
    renormalize); any other length decodes as little-endian float32 and
    never raises.

    When ``dim`` is given, format is discriminated strictly by payload
    length relative to that dimension: ``dim`` bytes is signed-int8 and
    ``dim * 4`` bytes is little-endian float32. Int8 dequant is ``q / 127``
    then L2-renormalized so stored vectors stay unit for ranking. Returns
    an owned, writable copy and rejects payloads of any other length.
    """
    data = bytes(blob)
    n = len(data)
    if dim is None:
        if n == EMBEDDING_DIM:
            quantized = np.frombuffer(data, dtype="<i1")
            vec = quantized.astype(np.float64) / _INT8_SCALE
            norm = float(np.linalg.norm(vec))
            if norm > 0.0:
                vec = vec / norm
            return np.asarray(vec, dtype=np.float32)
        return np.frombuffer(data, dtype="<f4").copy()
    if n == dim:
        quantized = np.frombuffer(data, dtype="<i1")
        vec = quantized.astype(np.float64) / _INT8_SCALE
        norm = float(np.linalg.norm(vec))
        if norm > 0.0:
            vec = vec / norm
        return np.asarray(vec, dtype=np.float32)
    if n == dim * 4:
        return np.frombuffer(data, dtype="<f4").copy()
    raise ValueError(
        f"embedding payload length {n} does not match int8 ({dim}) or float32 ({dim * 4}) encoding for dimension {dim}"
    )


def _note_missing_interned_owner(text_hash: str, model_id: str) -> None:
    global interned_payload_missing_owner_count
    with _interned_payload_lock:
        interned_payload_missing_owner_count += 1
    _log.warning(
        "interned embedding payload owner missing for text_hash=%s model_id=%s; dropping row from ranking [OBS-08]",
        text_hash,
        model_id,
    )


def prefetch_owned_vector_blobs(
    conn: sqlite3.Connection,
    keys: Iterable[tuple[str, str]],
) -> dict[tuple[str, str], bytes]:
    """Resolve interned payloads in one lookup keyed by ``(text_hash, model_id)``.

    Ranking used to call :func:`resolve_vector_blob` per empty row (N+1).
    """
    unique = {(str(text_hash), str(model_id)) for text_hash, model_id in keys}
    if not unique:
        return {}
    hashes = tuple({text_hash for text_hash, _model_id in unique})
    placeholders = ",".join("?" * len(hashes))
    rows = conn.execute(
        f"""
        SELECT text_hash, model_id, vector
        FROM concept_embeddings
        WHERE LENGTH(vector) > 0 AND text_hash IN ({placeholders})
        """,
        hashes,
    ).fetchall()
    owned: dict[tuple[str, str], bytes] = {}
    for text_hash, model_id, vector in rows:
        key = (str(text_hash), str(model_id))
        if key in unique and key not in owned and vector is not None:
            owned[key] = bytes(vector)
    return owned


def resolve_vector_blob(
    conn: sqlite3.Connection,
    blob: bytes | None,
    text_hash: str,
    model_id: str,
    *,
    owned_cache: dict[tuple[str, str], bytes] | None = None,
) -> bytes:
    """Return the physical payload for a row, following interned empty blobs."""
    data = bytes(blob or _EMPTY_VECTOR_BLOB)
    if data:
        return data
    key = (str(text_hash), str(model_id))
    if owned_cache is not None:
        cached = owned_cache.get(key, _EMPTY_VECTOR_BLOB)
        if not cached:
            _note_missing_interned_owner(text_hash, model_id)
        return cached
    row = conn.execute(
        """
        SELECT vector FROM concept_embeddings
        WHERE text_hash = ? AND model_id = ? AND LENGTH(vector) > 0
        LIMIT 1
        """,
        (text_hash, model_id),
    ).fetchone()
    if row is None or row[0] is None:
        _note_missing_interned_owner(text_hash, model_id)
        return _EMPTY_VECTOR_BLOB
    return bytes(row[0])


def _existing_hash_and_model(conn: sqlite3.Connection, entity_kind: str, entity_id: str) -> tuple[str, str] | None:
    row = conn.execute(
        "SELECT text_hash, model_id FROM concept_embeddings WHERE entity_kind = ? AND entity_id = ?",
        (entity_kind, entity_id),
    ).fetchone()
    if row is None:
        return None
    return (str(row[0]), str(row[1]))


def _interned_payload_dim(
    conn: sqlite3.Connection,
    new_hash: str,
    model_id: str,
    *,
    entity_kind: str,
    entity_id: str,
) -> int | None:
    """Return dim of an already-stored payload for ``(text_hash, model_id)``."""
    row = conn.execute(
        """
        SELECT dim FROM concept_embeddings
        WHERE text_hash = ? AND model_id = ? AND LENGTH(vector) > 0
          AND NOT (entity_kind = ? AND entity_id = ?)
        LIMIT 1
        """,
        (new_hash, model_id, entity_kind, entity_id),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def _transfer_owned_payload(conn: sqlite3.Connection, entity_kind: str, entity_id: str) -> None:
    """If this row owns an interned payload, move it to a remaining sharer."""
    _transfer_interned_embedding_payload(conn, entity_kind, entity_id)


def _intern_duplicate_payloads(conn: sqlite3.Connection) -> None:
    """Keep one physical BLOB per ``(text_hash, model_id)``; empty the rest."""
    dupes = conn.execute(
        """
        SELECT text_hash, model_id
        FROM concept_embeddings
        GROUP BY text_hash, model_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for text_hash_value, model_id in dupes:
        rows = conn.execute(
            """
            SELECT entity_kind, entity_id, vector
            FROM concept_embeddings
            WHERE text_hash = ? AND model_id = ?
            ORDER BY LENGTH(vector) DESC, entity_kind, entity_id
            """,
            (str(text_hash_value), str(model_id)),
        ).fetchall()
        for index, (entity_kind, entity_id, blob) in enumerate(rows):
            if index == 0:
                continue
            if blob and len(blob) > 0:
                conn.execute(
                    """
                    UPDATE concept_embeddings
                    SET vector = ?
                    WHERE entity_kind = ? AND entity_id = ?
                    """,
                    (_EMPTY_VECTOR_BLOB, entity_kind, entity_id),
                )


def rewrite_legacy_float32_payloads(conn: sqlite3.Connection) -> int:
    """Requantize leftover ``dim * 4`` float32 blobs to int8 without a provider.

    Live rows were written as little-endian float32. Rewriting them from the
    stored bytes (never ``provider.embed``) is what reclaims the ~80 MB; a
    codec-only change on new writes would leave the existing corpus untouched.
    """
    rows = conn.execute(
        """
        SELECT entity_kind, entity_id, vector
        FROM concept_embeddings
        WHERE LENGTH(vector) = dim * 4
        """
    ).fetchall()
    rewritten = 0
    for entity_kind, entity_id, blob in rows:
        vec = deserialize_vector(bytes(blob))
        conn.execute(
            """
            UPDATE concept_embeddings
            SET vector = ?
            WHERE entity_kind = ? AND entity_id = ?
            """,
            (serialize_vector(vec), entity_kind, entity_id),
        )
        rewritten += 1
    _intern_duplicate_payloads(conn)
    return rewritten


def _upsert_concept_embedding_row(
    conn: sqlite3.Connection,
    *,
    entity_kind: str,
    entity_id: str,
    task_ref: str,
    new_hash: str,
    vector: np.ndarray | None,
    model_id: str,
    blob: bytes | None = None,
    dim: int | None = None,
) -> None:
    """Write one embedding row. No provider work ([ARCH-13], [RES-02]).

    Single copy of the concept_embeddings upsert SQL: both on-write
    :func:`store_concept_embedding` and the backfill flush path route here
    so schema changes cannot silently skew the two writers ([CON-18], [DRY]).

    When another row already holds a payload for ``(text_hash, model_id)`` the
    vector BLOB is interned as empty bytes so entity identity stays while the
    payload is stored once.
    """
    _transfer_owned_payload(conn, entity_kind, entity_id)
    if blob is None:
        if vector is None:
            raise ValueError("vector is required when blob is not provided")
        interned = _interned_payload_dim(conn, new_hash, model_id, entity_kind=entity_kind, entity_id=entity_id)
        blob = _EMPTY_VECTOR_BLOB if interned is not None else serialize_vector(vector)
        dim = int(vector.shape[0]) if dim is None else dim
    elif dim is None:
        dim = int(vector.shape[0]) if vector is not None else EMBEDDING_DIM
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
    ``"stored"`` when a vector was (re-)written. Duplicate text under a new
    entity key stores the payload once (intern) and does not re-embed.
    """
    entity_id = str(entity_id)
    if text is None or not text.strip():
        return "empty"
    new_hash = text_hash(text)
    existing = _existing_hash_and_model(conn, entity_kind, entity_id)
    if existing == (new_hash, provider.model_id):
        return "skipped"
    intern_dim = _interned_payload_dim(conn, new_hash, provider.model_id, entity_kind=entity_kind, entity_id=entity_id)
    if intern_dim is not None:
        _upsert_concept_embedding_row(
            conn,
            entity_kind=entity_kind,
            entity_id=entity_id,
            task_ref=task_ref,
            new_hash=new_hash,
            vector=None,
            model_id=provider.model_id,
            blob=_EMPTY_VECTOR_BLOB,
            dim=intern_dim,
        )
        return "stored"
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

    A ``decision.rationale`` hook also reads and stores the owning row's
    additive ``decision.title`` concept. This keeps the existing numpy-free
    write-hook surface stable while giving the semantic arm both fields that
    the decision FTS body indexes; the rationale concept itself is unchanged.

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
        with _get_db_connection() as conn:
            results: list[str] = []
            if entity_kind == "decision.rationale":
                decision_row = conn.execute(
                    "SELECT decision FROM decisions WHERE id = ? AND task_ref = ?",
                    (entity_id, task_ref),
                ).fetchone()
                if decision_row is not None:
                    results.append(
                        store_concept_embedding(
                            conn,
                            prov,
                            "decision.title",
                            entity_id,
                            task_ref,
                            decision_row[0],
                        )
                    )
            results.append(store_concept_embedding(conn, prov, entity_kind, entity_id, task_ref, text))
        if "stored" in results:
            return _record_concept_outcome("stored")
        if "skipped" in results:
            return _record_concept_outcome("skipped")
        return _record_concept_outcome("skipped_empty")
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
    for row in conn.execute("SELECT id, task_ref, decision, rationale FROM decisions" + clause, params).fetchall():
        out.append(("decision.title", str(row[0]), str(row[1]), row[2]))
        out.append(("decision.rationale", str(row[0]), str(row[1]), row[3]))
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


# Public alias so read-only callers share the write-path gather without
# reaching for the private name. Implementation is unchanged.
gather_concepts = _gather_concepts


# Default bound on provider.embed during backfill. Aligns with the writer
# heartbeat stale window so a hung provider fails closed and exits the
# registered writer rather than advertising a live writer indefinitely
# while blocked on inference ([RES-02] / S5-A-04).
DEFAULT_BACKFILL_EMBED_TIMEOUT_SECONDS = 300.0

BackfillClassifyOutcome = Literal["empty", "skipped", "pending"]


def classify_coverage_row(
    text: object,
    existing: tuple[str, str] | None,
    model_id: str | None,
) -> tuple[BackfillClassifyOutcome, str | None]:
    """Pure skip-gate shared by write-path classify and the doctor facet.

    Blank text is ``empty``. When ``model_id`` is set, a row is ``skipped``
    only on a matching ``(text_hash, model_id)`` pair — the production resume
    gate. When ``model_id`` is ``None`` (preview with no resolved provider),
    skip is hash-only so operators can still count remaining work without
    constructing a provider.
    """
    if text is None or not str(text).strip():
        return "empty", None
    new_hash = text_hash(str(text))
    if model_id is None:
        if existing is not None and existing[0] == new_hash:
            return "skipped", new_hash
        return "pending", new_hash
    if existing == (new_hash, model_id):
        return "skipped", new_hash
    return "pending", new_hash


def _classify_concept_for_backfill(
    conn: sqlite3.Connection,
    entity_kind: str,
    entity_id: str,
    text: str | None,
    *,
    model_id: str | None,
) -> tuple[BackfillClassifyOutcome, str | None]:
    """Classify one concept with the same skip gate as the write path.

    Shared by :func:`backfill_concept_embeddings` and the dry-run CLI so the
    two cannot drift. Looks up the stored ``(text_hash, model_id)`` then
    delegates the comparison to :func:`classify_coverage_row`.
    """
    existing = None
    if text is not None and str(text).strip():
        existing = _existing_hash_and_model(conn, entity_kind, entity_id)
    return classify_coverage_row(text, existing, model_id)


# Public alias for the conn-backed write-path classify. Doctor and other
# read-only counters must use classify_coverage_row with a preloaded index
# so they share the skip gate without per-row SELECTs.
classify_concept_for_backfill = _classify_concept_for_backfill


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
            raise TimeoutError(f"provider.embed timed out after {timeout_seconds}s (batch size {len(texts)})") from exc
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def backfill_concept_embeddings(
    conn: sqlite3.Connection,
    provider: SupportsEmbed,
    *,
    task_ref: str | None = None,
    commit_every: int = 200,
    embed_timeout_seconds: float | None = DEFAULT_BACKFILL_EMBED_TIMEOUT_SECONDS,
    kinds: tuple[str, ...] | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    """Embed every concept missing or stale in ``concept_embeddings``.

    Idempotent + resumable: the ``(text_hash, model_id)`` gate makes a re-run a
    no-op for already-embedded concepts, so an interrupted run resumes by simply
    running again. Returns ``{stored, skipped, empty}`` counts.

    ``kinds`` restricts work to that subset of :data:`CONCEPT_ENTITY_KINDS`
    (default: all). ``limit`` is a cap on rows **stored in this run** — already
    embedded / empty rows are classified but do not consume the budget, so
    ``--limit N`` then ``--limit N`` again continues through the remaining
    corpus instead of stalling on the already-written prefix.

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
    if limit is not None and limit < 0:
        raise ValueError(f"limit must be >= 0, got {limit}")

    # Lazy imports: keep the optional embeddings import graph free of the
    # always-on liveness module at module load; reaper/runtime are cheap.
    from workbay_handoff_mcp.db_writer_liveness import db_writer_heartbeat
    from workbay_handoff_mcp.runtime import get_runtime_config

    # RES-01 / RES-06: never silently end a transaction the caller began.
    # Guard sits above every connection touch (including gather).
    if conn.in_transaction:
        raise RuntimeError(
            "backfill_concept_embeddings refused an open transaction on the caller connection; commit or rollback first"
        )

    # Requantize leftover float32 rows from stored bytes (no provider) so a
    # resumed backfill reclaims the live corpus instead of skipping it as
    # hash-fresh. Caller owns the connection; commit the rewrite before
    # classify so skip-gate reads see the compact payload.
    rewrite_legacy_float32_payloads(conn)
    if conn.in_transaction:
        conn.commit()

    label = f"concept_embedding_backfill task_ref={task_ref}" if task_ref else "concept_embedding_backfill"
    db_path = get_runtime_config().db_path
    counts = {"stored": 0, "skipped": 0, "empty": 0}
    remaining = limit
    kind_set = set(kinds) if kinds is not None else None
    with db_writer_heartbeat(db_path, label=label):
        concepts = _gather_concepts(conn, task_ref)
        if kind_set is not None:
            concepts = [row for row in concepts if row[0] in kind_set]
        for offset in range(0, len(concepts), commit_every):
            if remaining is not None and remaining <= 0:
                break
            chunk = concepts[offset : offset + commit_every]
            # --- classify (reads only; no write transaction) ----------------
            to_store: list[tuple[str, str, str, str, str]] = []  # kind, id, tref, text, hash
            for entity_kind, entity_id, tref, text in chunk:
                if remaining is not None and remaining <= 0:
                    break
                outcome, new_hash = _classify_concept_for_backfill(
                    conn,
                    entity_kind,
                    entity_id,
                    text,
                    model_id=provider.model_id,
                )
                if outcome == "empty":
                    counts["empty"] += 1
                    continue
                if outcome == "skipped" or new_hash is None:
                    counts["skipped"] += 1
                    continue
                to_store.append((entity_kind, entity_id, tref, str(text), new_hash))
                remaining = remaining - 1 if remaining is not None else None

            if not to_store:
                continue

            # Drop deferred read snapshot before the provider wait so we never
            # hold a transaction across embed ([DATA-20], [RES-02]).
            _end_deferred_read_txn(conn)

            # --- embed (one provider call for the whole remaining chunk) ----
            capped = [truncate_embed_text(text) for *_, text, _hash in to_store]
            vectors = _provider_embed_batch(provider, capped, timeout_seconds=embed_timeout_seconds)
            if vectors.shape[0] != len(capped):
                raise RuntimeError(
                    f"provider returned unexpected batch size {vectors.shape[0]} for backfill chunk of {len(capped)}"
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
