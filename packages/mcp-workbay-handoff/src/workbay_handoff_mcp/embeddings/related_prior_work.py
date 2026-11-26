"""Read-only semantic retrieval of related prior findings/decisions (advisory only).

Finds concept rows in ``concept_embeddings`` that resemble a query, for a human
or agent to **read**. Similarity may change list **order**; it must never change
the **membership** of any set the machine acts on (CAL-03). Retrieved hits must
not gate, filter, auto-close, or auto-link anything.

Two query modes:

* **by ``finding_id``** — uses the row's already-stored vector. No embedding
  model is required (the normal operator state).
* **by free ``text``** — requires a provider; degrades to the typed empty state
  ``provider_unavailable`` when absent.

Empty outcomes are **typed** (AXI-5): a bare empty list is never returned, so a
caller can distinguish "nothing similar above the floor" from "model missing"
from "store empty" from "incompatible model_id".
"""

from __future__ import annotations

import contextlib
import contextvars
import heapq
import logging
import math
import os
import sqlite3
import threading
import time
from collections.abc import Collection, Iterator
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal

import numpy as np

from .scan import (
    check_cancelled,
    concept_rows_exist,
    iter_concept_vector_chunks,
    reclaim_exclusion_sql,
    score_chunk,
)
from .store import (
    SupportsEmbed,
    concept_embedding_coverage,
    deserialize_vector,
    prefetch_owned_vector_blobs,
    resolve_vector_blob,
)

# Calibrated abstention floor for ranking-only prior-art retrieval.
# Matches ReinjectionConfig.min_score (0.35): cosine on L2-normalized unit
# vectors is in [-1, 1]; identical ≈ 1.0, orthogonal ≈ 0.0. A floor of 0.35
# drops weak noise while keeping near-paraphrase hits. This is **not** an
# absence proof — "nothing above the floor" is a statement about the floor,
# never about the corpus (CAL-03 ranking-only; skip formal calibration).
DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE: float = 0.35

DEFAULT_RELATED_PRIOR_WORK_LIMIT: int = 5

# Compaction residuals above this stored-text length are not cosine-ranked
# (implementation note). Snippets stay separately capped by _resolve_snippet.
COMPACTION_RESIDUAL_RANK_MAX_CHARS: int = 4000
_COMPACTION_RESIDUAL_KIND = "compaction.prose_residual"

# Preferred stored vectors when resolving a finding_id query (most representative first).
_FINDING_QUERY_KINDS: tuple[str, ...] = (
    "finding.description",
    "finding.fix",
    "finding.resolution_notes",
)

# Source-text join specs: entity_kind -> (table, id_col, text_col).
# concept_embeddings stores only text_hash; snippets must come from the source row.
_SNIPPET_SPECS: dict[str, tuple[str, str, str]] = {
    "decision.rationale": ("decisions", "id", "rationale"),
    "finding.description": ("review_findings", "id", "description"),
    "finding.fix": ("review_findings", "id", "fix"),
    "finding.resolution_notes": ("review_findings", "id", "resolution_notes"),
    "blocker.description": ("blockers", "id", "description"),
    "handoff_state.objective": ("handoff_state", "task_ref", "objective"),
    "handoff_state.focus": ("handoff_state", "task_ref", "focus"),
    "compaction.prose_residual": ("session_compactions", "compaction_id", "prose_residual"),
}

RelatedPriorWorkStatus = Literal[
    "ok_with_results",
    "no_results_above_threshold",
    "provider_unavailable",
    "model_mismatch",
    "store_empty",
    "busy",
]

SemanticMode = Literal["warm_only", "allow_cold"]
_VALID_SEMANTIC_MODES: frozenset[str] = frozenset({"warm_only", "allow_cold"})

# Rows per fetchmany chunk of the bounded scan: peak resident memory is one
# chunk (rows + an (n, dim) float32 matrix) plus the top-k, never the store.
RELATED_PRIOR_WORK_SCAN_CHUNK_ROWS: int = 1024

# Single-flight ([RES-04], [CAL-02]): one prior-work query scans at a time per
# process; later callers queue (same result shape, same status vocabulary)
# for at most this long. Only exhausting the wait degrades, to the typed
# status "busy" with note RELATED_PRIOR_WORK_BUSY_NOTE.
DEFAULT_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS: float = 30.0
ENV_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS = "WORKBAY_HANDOFF_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS"
RELATED_PRIOR_WORK_BUSY_NOTE = "single_flight_wait_exhausted"
# Cancel-token poll interval while queued.
_QUEUE_POLL_SECONDS = 0.05

_SINGLE_FLIGHT = threading.Lock()

# Cancel token of the request this thread is serving (set by the MCP tool
# bridge in api.py; ``None`` for in-process callers). A ContextVar so it
# reaches find_related_prior_work_public through core's pass-through without
# widening every intermediate signature.
_ACTIVE_CANCEL: contextvars.ContextVar[threading.Event | None] = contextvars.ContextVar(
    "related_prior_work_cancel", default=None
)


@contextlib.contextmanager
def cancellation_scope(cancel: threading.Event | None) -> Iterator[None]:
    """Make ``cancel`` the token checked by prior-work queries in this context."""
    token = _ACTIVE_CANCEL.set(cancel)
    try:
        yield
    finally:
        _ACTIVE_CANCEL.reset(token)


def resolve_queue_timeout_seconds() -> float:
    """Bounded single-flight wait, rejecting unsafe environment values."""
    raw = os.environ.get(ENV_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS)
    if raw is None:
        return DEFAULT_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{ENV_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS} must be a finite positive number, got {raw!r}"
        ) from exc
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(
            f"{ENV_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS} must be a finite positive number, got {raw!r}"
        )
    return value


def _acquire_single_flight(timeout_seconds: float, cancel: threading.Event | None) -> bool:
    """Queue for the scan slot; ``False`` once the bounded wait is exhausted.

    Raises :class:`ScanCancelled` if the caller's token is set while queued.
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        check_cancelled(cancel)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        wait = remaining if cancel is None else min(remaining, _QUEUE_POLL_SECONDS)
        if _SINGLE_FLIGHT.acquire(timeout=wait):
            if cancel is not None and cancel.is_set():
                _SINGLE_FLIGHT.release()
                check_cancelled(cancel)
            return True


@dataclass(frozen=True)
class RelatedPriorWorkHit:
    """One advisory prior-art hit (never a gate input)."""

    entity_kind: str
    entity_id: str
    task_ref: str
    score: float
    snippet: str | None
    model_id: str


@dataclass(frozen=True)
class RelatedPriorWorkResult:
    """Typed retrieval outcome — always inspect ``status`` before ``results``."""

    status: RelatedPriorWorkStatus
    results: tuple[RelatedPriorWorkHit, ...]
    min_score: float
    query_mode: Literal["finding_id", "text"] | None = None
    query_model_id: str | None = None
    note: str | None = None
    coverage: dict[str, object] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "results": [asdict(h) for h in self.results],
            "min_score": self.min_score,
            "query_mode": self.query_mode,
            "query_model_id": self.query_model_id,
            "note": self.note,
            "coverage": self.coverage,
        }


def _empty(
    status: RelatedPriorWorkStatus,
    *,
    min_score: float,
    query_mode: Literal["finding_id", "text"] | None = None,
    query_model_id: str | None = None,
    note: str | None = None,
) -> RelatedPriorWorkResult:
    return RelatedPriorWorkResult(
        status=status,
        results=(),
        min_score=min_score,
        query_mode=query_mode,
        query_model_id=query_model_id,
        note=note,
    )


def _store_row_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM concept_embeddings").fetchone()
    return int(row[0]) if row is not None else 0


def _resolve_embedding_entity_id(conn: sqlite3.Connection, finding_id: str) -> str:
    """Map a caller finding key to the ``concept_embeddings.entity_id``.

    Embeddings are keyed by ``str(review_findings.id)`` (the integer PK). Callers
    may pass that PK string, or the stable ``review_findings.finding_id`` text;
    both resolve to the same embedding entity_id when present.
    """
    placeholders = ",".join("?" for _ in _FINDING_QUERY_KINDS)
    hit = conn.execute(
        f"""
        SELECT 1 FROM concept_embeddings
        WHERE entity_id = ? AND entity_kind IN ({placeholders})
        LIMIT 1
        """,
        (finding_id, *_FINDING_QUERY_KINDS),
    ).fetchone()
    if hit is not None:
        return finding_id
    row = conn.execute(
        "SELECT id FROM review_findings WHERE finding_id = ? ORDER BY id ASC LIMIT 1",
        (finding_id,),
    ).fetchone()
    if row is not None:
        return str(row[0])
    return finding_id


def _stored_vector_dim(value: object) -> int | None:
    """Coerce the stored ``dim`` column; ``None`` keeps legacy length heuristics."""
    if value is None:
        return None
    return int(value)


def _load_finding_query_vector(
    conn: sqlite3.Connection,
    finding_id: str,
) -> tuple[np.ndarray, str, str, str] | None:
    """Return ``(vector, model_id, entity_kind, entity_id)`` for a finding's stored embed."""
    entity_id = _resolve_embedding_entity_id(conn, finding_id)
    placeholders = ",".join("?" for _ in _FINDING_QUERY_KINDS)
    rows = conn.execute(
        f"""
        SELECT entity_kind, entity_id, vector, model_id, text_hash, dim
        FROM concept_embeddings
        WHERE entity_id = ? AND entity_kind IN ({placeholders})
        """,
        (entity_id, *_FINDING_QUERY_KINDS),
    ).fetchall()
    if not rows:
        return None
    intern_keys = [(str(r[4]), str(r[3])) for r in rows if not bytes(r[2] or b"")]
    owned_cache = prefetch_owned_vector_blobs(conn, intern_keys)
    by_kind = {str(r[0]): r for r in rows}
    for kind in _FINDING_QUERY_KINDS:
        row = by_kind.get(kind)
        if row is None:
            continue
        blob = resolve_vector_blob(conn, row[2], str(row[4]), str(row[3]), owned_cache=owned_cache)
        if not blob:
            continue
        vec = deserialize_vector(blob, dim=_stored_vector_dim(row[5]))
        return vec, str(row[3]), str(row[0]), str(row[1])
    return None


def _resolve_snippet(
    conn: sqlite3.Connection,
    entity_kind: str,
    entity_id: str,
    task_ref: str,
    *,
    max_chars: int = 240,
) -> str | None:
    spec = _SNIPPET_SPECS.get(entity_kind)
    if spec is None:
        return None
    table, id_col, text_col = spec
    # Prefer task_ref-scoped join (embeddings always carry task_ref); fall back to id alone.
    row = conn.execute(
        f"SELECT {text_col} FROM {table} WHERE {id_col} = ? AND task_ref = ?",
        (entity_id, task_ref),
    ).fetchone()
    if row is None:
        row = conn.execute(
            f"SELECT {text_col} FROM {table} WHERE {id_col} = ?",
            (entity_id,),
        ).fetchone()
    if row is None or row[0] is None:
        return None
    text = str(row[0]).strip()
    if not text:
        return None
    if len(text) > max_chars:
        cut = text[:max_chars]
        if " " in cut:
            cut = cut.rsplit(" ", 1)[0]
        text = cut.rstrip() + "…"
    return text


def _rank_key(hit: tuple[float, str, str, str, str]) -> tuple[float, str, str]:
    """Result order: score descending, then entity_kind, then entity_id."""
    return (-hit[0], hit[1], hit[2])


def _include_token(value: object) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return str(value)


def _normalize_include(value: Collection[str] | None) -> frozenset[str] | None:
    """None = no membership filter; empty collection = include nothing.

    A lone ``str`` / ``bytes`` / ``bytearray`` is one token (BR-I6-01,
    BR-I7-02). ``str`` is a ``Collection[str]`` of characters, so iterating
    it would silently change membership. ``bytes`` must decode as utf-8,
    not ``str(b'OLD')`` → ``\"b'OLD'\"``.
    """
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        return frozenset({_include_token(value)})
    return frozenset(_include_token(item) for item in value)


def _rank_against_store(
    conn: sqlite3.Connection,
    *,
    anchor: np.ndarray,
    model_id: str,
    limit: int,
    min_score: float,
    exclude_entity_ids: frozenset[str],
    exclude_task_ref: str | None,
    query_mode: Literal["finding_id", "text"],
    include_task_refs: frozenset[str] | None = None,
    include_plan_paths: frozenset[str] | None = None,
    include_statuses: frozenset[str] | None = None,
    cancel: threading.Event | None = None,
) -> RelatedPriorWorkResult:
    check_cancelled(cancel)
    total = _store_row_count(conn)
    if total == 0:
        return _empty("store_empty", min_score=min_score, query_mode=query_mode, query_model_id=model_id)

    # Empty include set is a membership filter, not "treat as omitted".
    if (
        (include_task_refs is not None and len(include_task_refs) == 0)
        or (include_plan_paths is not None and len(include_plan_paths) == 0)
        or (include_statuses is not None and len(include_statuses) == 0)
    ):
        return _empty(
            "no_results_above_threshold",
            min_score=min_score,
            query_mode=query_mode,
            query_model_id=model_id,
            note="no_candidates_after_exclusions",
        )

    anchor = np.asarray(anchor, dtype="<f4").reshape(-1)
    norm = float(np.linalg.norm(anchor))
    if norm == 0.0:
        return _empty(
            "no_results_above_threshold",
            min_score=min_score,
            query_mode=query_mode,
            query_model_id=model_id,
            note="zero_query_vector",
        )
    anchor = anchor / norm

    # Space identity (EMB-01 / EMB-05): never silently rank across model_id geometries.
    # Every predicate is SQL; the scan returns only same-model rows that
    # survive the reclaim exclusion, the caller's exclusions and the include
    # filters, chunk by chunk ([RES-04]). Foreign-model rows are only ever
    # probed with EXISTS, never fetched.
    needs_join = include_plan_paths is not None or include_statuses is not None
    joins_sql = ""
    if needs_join:
        # Live handoff_state wins via COALESCE. Join archives on task_ref
        # alone so a restored live row with NULL task_plan_path can still
        # fall back to $.active.task_plan_path (BR-I8-01 / GRPH-36).
        joins_sql = (
            "LEFT JOIN handoff_state h ON h.task_ref = c.task_ref LEFT JOIN task_archives a ON a.task_ref = c.task_ref"
        )
    where: list[str] = []
    params: list[object] = []
    if include_task_refs is not None:
        placeholders = ",".join("?" for _ in include_task_refs)
        where.append(f"c.task_ref IN ({placeholders})")
        params.extend(sorted(include_task_refs))
    if include_plan_paths is not None:
        placeholders = ",".join("?" for _ in include_plan_paths)
        where.append(
            f"COALESCE(h.task_plan_path, json_extract(a.snapshot_json, '$.active.task_plan_path')) IN ({placeholders})"
        )
        params.extend(sorted(include_plan_paths))
    if include_statuses is not None:
        placeholders = ",".join("?" for _ in include_statuses)
        where.append(f"COALESCE(h.status, json_extract(a.snapshot_json, '$.active.status')) IN ({placeholders})")
        params.extend(sorted(include_statuses))
    exclusions: list[str] = []
    exclusion_params: list[object] = []
    if exclude_entity_ids:
        exclusions.append("c.entity_id NOT IN (" + ",".join("?" for _ in exclude_entity_ids) + ")")
        exclusion_params.extend(sorted(exclude_entity_ids))
    if exclude_task_ref is not None:
        exclusions.append("c.task_ref != ?")
        exclusion_params.append(exclude_task_ref)

    # Same-model rows are counted before the residual skip so an oversized
    # compaction.prose_residual is not reported as model_mismatch
    # (BR-MEM2-01 / CARD-05).
    model_match_count = 0
    any_scored = False
    any_above = False
    keep = max(0, limit)
    top: list[tuple[float, str, str, str, str]] = []  # score, kind, eid, task_ref, model_id
    for chunk in iter_concept_vector_chunks(
        conn,
        model_id=model_id,
        dim=int(anchor.shape[0]),
        where_sql=[*where, *exclusions],
        params=[*params, *exclusion_params],
        joins_sql=joins_sql,
        chunk=RELATED_PRIOR_WORK_SCAN_CHUNK_ROWS,
        cancel=cancel,
        residual_max_chars=COMPACTION_RESIDUAL_RANK_MAX_CHARS,
        decode=lambda blob, stored_dim: deserialize_vector(blob, dim=_stored_vector_dim(stored_dim)),
    ):
        model_match_count += chunk.rows_seen
        if not chunk.keys:
            continue
        any_scored = True
        candidates = [
            (score, *key)
            for score, key in zip(score_chunk(chunk.matrix, anchor), chunk.keys, strict=True)
            if score >= min_score
        ]
        if not candidates:
            continue
        any_above = True
        # Bounded top-k: O(limit + chunk) entries, same (-score, kind, id) order.
        top = heapq.nsmallest(keep, [*top, *candidates], key=_rank_key)

    if model_match_count == 0:
        if where and not concept_rows_exist(conn, predicates=where, params=params, joins_sql=joins_sql, cancel=cancel):
            return _empty(
                "no_results_above_threshold",
                min_score=min_score,
                query_mode=query_mode,
                query_model_id=model_id,
                note="no_candidates_after_include_filters",
            )
        # Store has rows but none share the query model_id: foreign-model rows
        # that survive the same filters are the mismatch evidence.
        if concept_rows_exist(
            conn,
            predicates=["c.model_id != ?", reclaim_exclusion_sql(conn), *where, *exclusions],
            params=[model_id, *params, *exclusion_params],
            joins_sql=joins_sql,
            cancel=cancel,
        ):
            return _empty(
                "model_mismatch",
                min_score=min_score,
                query_mode=query_mode,
                query_model_id=model_id,
                note="no_candidates_share_query_model_id",
            )
        # Everything was self/task-excluded; treat as empty-of-peers, not mismatch.
        return _empty(
            "no_results_above_threshold",
            min_score=min_score,
            query_mode=query_mode,
            query_model_id=model_id,
            note="no_candidates_after_exclusions",
        )

    if not any_above:
        return _empty(
            "no_results_above_threshold",
            min_score=min_score,
            query_mode=query_mode,
            query_model_id=model_id,
            note="no_candidates_after_exclusions" if not any_scored else None,
        )

    check_cancelled(cancel)
    hits: list[RelatedPriorWorkHit] = []
    for score, entity_kind, entity_id, row_task_ref, row_model_id in top:
        snippet = _resolve_snippet(conn, entity_kind, entity_id, row_task_ref)
        hits.append(
            RelatedPriorWorkHit(
                entity_kind=entity_kind,
                entity_id=entity_id,
                task_ref=row_task_ref,
                score=score,
                snippet=snippet,
                model_id=row_model_id,
            )
        )
    return RelatedPriorWorkResult(
        status="ok_with_results",
        results=tuple(hits),
        min_score=min_score,
        query_mode=query_mode,
        query_model_id=model_id,
    )


def _find_related_prior_work(
    conn: sqlite3.Connection,
    *,
    text: str | None = None,
    finding_id: str | None = None,
    task_ref: str | None = None,
    include_task_refs: Collection[str] | None = None,
    include_plan_paths: Collection[str] | None = None,
    include_statuses: Collection[str] | None = None,
    limit: int = DEFAULT_RELATED_PRIOR_WORK_LIMIT,
    min_score: float = DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE,
    provider: SupportsEmbed | None = None,
    cancel: threading.Event | None = None,
    semantic_mode: SemanticMode = "allow_cold",
) -> RelatedPriorWorkResult:
    """Return prior concept rows that resemble the query (advisory context only).

    Keyword-only (except ``conn``). Exactly one of ``text`` or ``finding_id``
    must be provided.

    Parameters
    ----------
    conn:
        Open handoff DB connection (caller owns the transaction).
    text:
        Free-text query. Requires ``provider`` (or a resolvable env provider).
        When no provider is available, returns ``provider_unavailable`` — distinct
        from empty results.
    finding_id:
        Use the already-stored embedding for this review finding. No model load.
    task_ref:
        When set, exclude rows from this task so the caller sees **prior** art
        rather than sibling concepts on the active task.
    include_task_refs / include_plan_paths / include_statuses:
        Optional candidate *membership* filters applied before cosine (GRPH-36
        rank-only). ``None`` keeps today's full-table scan. An empty collection
        includes nothing (typed ``no_candidates_after_exclusions``). Plan-path
        and status join ``handoff_state``. Does **not** invert ``task_ref``
        exclude semantics.
    limit:
        Maximum hits to return when status is ``ok_with_results``.
    min_score:
        Minimum cosine similarity (inclusive). Scores below this yield
        ``no_results_above_threshold``, not low-ranked results. Default
        :data:`DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE`.

        A threshold produces *calibrated abstention*, **not** an absence proof:
        "nothing above the floor" is a statement about the floor, never about
        the corpus.
    provider:
        Embedding provider for free-text mode. Ignored for ``finding_id`` mode.
        Tests inject a stub; production may pass ``None`` to resolve from env.
    cancel:
        Optional token. Once set, the scan stops at its next chunk boundary
        and raises :class:`~workbay_handoff_mcp.embeddings.scan.ScanCancelled`
        (the caller has abandoned the request; nothing is returned).

    Returns
    -------
    RelatedPriorWorkResult
        Always has a machine-readable ``status`` in
        ``ok_with_results | no_results_above_threshold | provider_unavailable |
        model_mismatch | store_empty``. Never a bare empty list alone.

    Notes
    -----
    * Ranking refuses cross-``model_id`` comparison (EMB-01 / EMB-05) and
      returns ``model_mismatch`` when the store has vectors but none share the
      query's model identity.
    * Self-exclusion: a ``finding_id`` query never returns that finding.
    * Bounded: rows are scanned in chunks of
      :data:`RELATED_PRIOR_WORK_SCAN_CHUNK_ROWS` with every filter in SQL and a
      top-``limit`` heap, so resident memory is O(chunk + limit), not O(store).
    """
    has_text = text is not None and str(text).strip() != ""
    has_finding = finding_id is not None and str(finding_id).strip() != ""
    if has_text == has_finding:
        raise ValueError("exactly one of text= or finding_id= is required")

    if limit < 0:
        raise ValueError("limit must be >= 0")

    if semantic_mode not in _VALID_SEMANTIC_MODES:
        raise ValueError(f"semantic_mode must be 'warm_only' or 'allow_cold', got {semantic_mode!r}")

    exclude_task = task_ref.strip() if task_ref is not None and task_ref.strip() else None
    inc_tasks = _normalize_include(include_task_refs)
    inc_plans = _normalize_include(include_plan_paths)
    inc_statuses = _normalize_include(include_statuses)

    if has_finding:
        fid = str(finding_id).strip()
        loaded = _load_finding_query_vector(conn, fid)
        if loaded is None:
            if _store_row_count(conn) == 0:
                return _empty(
                    "store_empty",
                    min_score=min_score,
                    query_mode="finding_id",
                    note="query_finding_not_embedded_and_store_empty",
                )
            return _empty(
                "no_results_above_threshold",
                min_score=min_score,
                query_mode="finding_id",
                note="query_finding_not_embedded",
            )
        anchor, model_id, _q_kind, query_entity_id = loaded
        # Self-exclusion uses the embedding entity_id (PK string), not the
        # caller's possibly-external finding_id label.
        return _rank_against_store(
            conn,
            anchor=anchor,
            model_id=model_id,
            limit=limit,
            min_score=min_score,
            exclude_entity_ids=frozenset({query_entity_id}),
            exclude_task_ref=exclude_task,
            query_mode="finding_id",
            include_task_refs=inc_tasks,
            include_plan_paths=inc_plans,
            include_statuses=inc_statuses,
            cancel=cancel,
        )

    # Free-text mode — provider required.
    prov = provider
    if prov is None:
        from .store import _resolve_provider

        prov = _resolve_provider()
    if prov is None:
        return _empty(
            "provider_unavailable",
            min_score=min_score,
            query_mode="text",
            note="no_embedding_provider_for_free_text_query",
        )

    from .provider import EmbeddingArtifactError, EmbeddingProvider
    from .sidecar_adapter import SidecarEmbeddingAdapter, SidecarEmbeddingError

    if semantic_mode == "warm_only":
        if isinstance(prov, SidecarEmbeddingAdapter):
            if not prov.is_loaded:
                return _empty(
                    "provider_unavailable",
                    min_score=min_score,
                    query_mode="text",
                    note="sidecar_cold",
                )
        elif isinstance(prov, EmbeddingProvider):
            if not prov.is_loaded:
                return _empty(
                    "provider_unavailable",
                    min_score=min_score,
                    query_mode="text",
                    note="sidecar_disabled_cold",
                )
        elif not bool(getattr(prov, "is_loaded", True)):
            return _empty(
                "provider_unavailable",
                min_score=min_score,
                query_mode="text",
                note="provider_cold",
            )

    check_cancelled(cancel)
    try:
        if semantic_mode == "warm_only" and isinstance(prov, SidecarEmbeddingAdapter):
            vectors = prov.embed([str(text).strip()], allow_cold=False)
        else:
            vectors = prov.embed([str(text).strip()])
    except EmbeddingArtifactError as exc:
        if (
            semantic_mode == "warm_only"
            and isinstance(exc, SidecarEmbeddingError)
            and exc.reason in ("socket_missing", "connection_refused", "unavailable")
        ):
            return _empty(
                "provider_unavailable",
                min_score=min_score,
                query_mode="text",
                note="sidecar_cold",
            )
        logging.getLogger("workbay_handoff_mcp").warning(
            "related_prior_work_degraded embedding_artifact_%s path=%s error=%s",
            exc.kind or "error",
            exc.path,
            exc,
        )
        return _empty(
            "provider_unavailable",
            min_score=min_score,
            query_mode="text",
            note=f"embedding_artifact_{exc.kind or 'error'}",
        )
    if vectors.shape[0] != 1:
        return _empty(
            "no_results_above_threshold",
            min_score=min_score,
            query_mode="text",
            query_model_id=prov.model_id,
            note="provider_returned_empty_batch",
        )
    return _rank_against_store(
        conn,
        anchor=vectors[0],
        model_id=prov.model_id,
        limit=limit,
        min_score=min_score,
        exclude_entity_ids=frozenset(),
        exclude_task_ref=exclude_task,
        query_mode="text",
        include_task_refs=inc_tasks,
        include_plan_paths=inc_plans,
        include_statuses=inc_statuses,
        cancel=cancel,
    )


def find_related_prior_work(
    conn: sqlite3.Connection,
    *,
    text: str | None = None,
    finding_id: str | None = None,
    task_ref: str | None = None,
    include_task_refs: Collection[str] | None = None,
    include_plan_paths: Collection[str] | None = None,
    include_statuses: Collection[str] | None = None,
    limit: int = DEFAULT_RELATED_PRIOR_WORK_LIMIT,
    min_score: float = DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE,
    provider: SupportsEmbed | None = None,
    cancel: threading.Event | None = None,
    semantic_mode: SemanticMode = "allow_cold",
) -> RelatedPriorWorkResult:
    """Retrieve advisory prior art and report source-derived global coverage.

    Coverage scope is all source concepts, regardless of result membership
    filters. A successful ranking may still have pending coverage. Provider
    absence or missing query model identity yields unknown coverage. The
    census is streamed in pages, like the ranking scan. ``cancel`` stops
    either between chunks (:class:`~workbay_handoff_mcp.embeddings.scan.ScanCancelled`).
    """
    result = _find_related_prior_work(
        conn,
        text=text,
        finding_id=finding_id,
        task_ref=task_ref,
        include_task_refs=include_task_refs,
        include_plan_paths=include_plan_paths,
        include_statuses=include_statuses,
        limit=limit,
        min_score=min_score,
        provider=provider,
        cancel=cancel,
        semantic_mode=semantic_mode,
    )
    return replace(result, coverage=concept_embedding_coverage(conn, result.query_model_id, cancel=cancel))


def _busy_payload(
    *,
    text: str | None,
    finding_id: str | None,
    min_score: float,
) -> dict[str, Any]:
    """Typed degrade when the bounded single-flight wait is exhausted.

    Nothing was ranked or counted, so coverage is ``unknown`` (not a claim).
    """
    has_text = text is not None and str(text).strip() != ""
    has_finding = finding_id is not None and str(finding_id).strip() != ""
    result = RelatedPriorWorkResult(
        status="busy",
        results=(),
        min_score=min_score,
        query_mode="finding_id" if has_finding else "text" if has_text else None,
        note=RELATED_PRIOR_WORK_BUSY_NOTE,
        coverage={
            "status": "unknown",
            "model_id": None,
            "total": None,
            "current": None,
            "pending": None,
            "scope": "all_source_concepts",
        },
    )
    return result.as_dict()


def find_related_prior_work_public(
    *,
    text: str | None = None,
    finding_id: str | None = None,
    task_ref: str | None = None,
    include_task_refs: Collection[str] | None = None,
    include_plan_paths: Collection[str] | None = None,
    include_statuses: Collection[str] | None = None,
    limit: int = DEFAULT_RELATED_PRIOR_WORK_LIMIT,
    min_score: float = DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE,
    provider: SupportsEmbed | None = None,
    cancel: threading.Event | None = None,
    semantic_mode: SemanticMode = "allow_cold",
) -> dict[str, Any]:
    """Package-level entry: open the handoff DB and return a JSON-friendly dict.

    Keyword-only. See :func:`find_related_prior_work` for semantics. Public
    callers receive ``empty_membership_filter`` for an explicitly empty include
    collection so it cannot be confused with exclusions removing candidates.

    Single-flight ([RES-04], [CAL-02]): embed + rank + coverage run under one
    process-wide lock, so N concurrent callers hold one scan working set, not
    N. A second caller is queued, not refused: it waits, then gets the same
    ranking. The wait is bounded by :func:`resolve_queue_timeout_seconds`
    (``WORKBAY_HANDOFF_RELATED_PRIOR_WORK_QUEUE_TIMEOUT_SECONDS``, default
    30 s); only exhausting it degrades, to the typed status ``busy`` with note
    ``single_flight_wait_exhausted`` and ``unknown`` coverage.

    ``cancel`` (default: the token installed by :func:`cancellation_scope`)
    is checked while queued and between scan chunks; once set the call raises
    :class:`~workbay_handoff_mcp.embeddings.scan.ScanCancelled`.
    """
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    token = cancel if cancel is not None else _ACTIVE_CANCEL.get()
    if not _acquire_single_flight(resolve_queue_timeout_seconds(), token):
        return _busy_payload(text=text, finding_id=finding_id, min_score=min_score)
    try:
        with _get_db_connection() as conn:
            result = find_related_prior_work(
                conn,
                text=text,
                finding_id=finding_id,
                task_ref=task_ref,
                include_task_refs=include_task_refs,
                include_plan_paths=include_plan_paths,
                include_statuses=include_statuses,
                limit=limit,
                min_score=min_score,
                provider=provider,
                cancel=token,
                semantic_mode=semantic_mode,
            )
        payload = result.as_dict()
        if result.note == "no_candidates_after_exclusions" and any(
            (normalized := _normalize_include(value)) is not None and not normalized
            for value in (include_task_refs, include_plan_paths, include_statuses)
        ):
            payload["note"] = "empty_membership_filter"
        return payload
    finally:
        _SINGLE_FLIGHT.release()
