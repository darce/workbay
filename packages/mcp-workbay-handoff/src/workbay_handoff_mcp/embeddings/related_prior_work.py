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

import sqlite3
from dataclasses import asdict, dataclass
from typing import Any, Literal

import numpy as np

from .store import SupportsEmbed, deserialize_vector

# Calibrated abstention floor for ranking-only prior-art retrieval.
# Matches ReinjectionConfig.min_score (0.35): cosine on L2-normalized unit
# vectors is in [-1, 1]; identical ≈ 1.0, orthogonal ≈ 0.0. A floor of 0.35
# drops weak noise while keeping near-paraphrase hits. This is **not** an
# absence proof — "nothing above the floor" is a statement about the floor,
# never about the corpus (CAL-03 ranking-only; skip formal calibration).
DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE: float = 0.35

DEFAULT_RELATED_PRIOR_WORK_LIMIT: int = 5

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
]


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

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "results": [asdict(h) for h in self.results],
            "min_score": self.min_score,
            "query_mode": self.query_mode,
            "query_model_id": self.query_model_id,
            "note": self.note,
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


def _load_finding_query_vector(
    conn: sqlite3.Connection,
    finding_id: str,
) -> tuple[np.ndarray, str, str, str] | None:
    """Return ``(vector, model_id, entity_kind, entity_id)`` for a finding's stored embed."""
    entity_id = _resolve_embedding_entity_id(conn, finding_id)
    placeholders = ",".join("?" for _ in _FINDING_QUERY_KINDS)
    rows = conn.execute(
        f"""
        SELECT entity_kind, entity_id, vector, model_id
        FROM concept_embeddings
        WHERE entity_id = ? AND entity_kind IN ({placeholders})
        """,
        (entity_id, *_FINDING_QUERY_KINDS),
    ).fetchall()
    if not rows:
        return None
    by_kind = {str(r[0]): r for r in rows}
    for kind in _FINDING_QUERY_KINDS:
        row = by_kind.get(kind)
        if row is None:
            continue
        vec = deserialize_vector(row[2])
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
) -> RelatedPriorWorkResult:
    total = _store_row_count(conn)
    if total == 0:
        return _empty("store_empty", min_score=min_score, query_mode=query_mode, query_model_id=model_id)

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
    model_match_count = 0
    other_model_count = 0
    scored: list[tuple[float, str, str, str, str]] = []  # score, kind, eid, task_ref, model_id

    for row in conn.execute(
        "SELECT entity_kind, entity_id, task_ref, vector, model_id FROM concept_embeddings"
    ).fetchall():
        entity_kind = str(row[0])
        entity_id = str(row[1])
        row_task_ref = str(row[2])
        row_model_id = str(row[4])

        if entity_id in exclude_entity_ids:
            continue
        if exclude_task_ref is not None and row_task_ref == exclude_task_ref:
            continue

        if row_model_id != model_id:
            other_model_count += 1
            continue

        model_match_count += 1
        vec = deserialize_vector(row[3])
        if vec.shape[0] != anchor.shape[0]:
            continue
        score = float(np.dot(anchor, vec))
        scored.append((score, entity_kind, entity_id, row_task_ref, row_model_id))

    if model_match_count == 0:
        if other_model_count > 0 or total > 0:
            # Store has rows but none share the query model_id (or all were excluded
            # and only foreign-model rows remain visible as mismatch evidence).
            if other_model_count > 0:
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

    above = [s for s in scored if s[0] >= min_score]
    if not above:
        return _empty(
            "no_results_above_threshold",
            min_score=min_score,
            query_mode=query_mode,
            query_model_id=model_id,
        )

    above.sort(key=lambda t: (-t[0], t[1], t[2]))
    top = above[: max(0, limit)]
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


def find_related_prior_work(
    conn: sqlite3.Connection,
    *,
    text: str | None = None,
    finding_id: str | None = None,
    task_ref: str | None = None,
    limit: int = DEFAULT_RELATED_PRIOR_WORK_LIMIT,
    min_score: float = DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE,
    provider: SupportsEmbed | None = None,
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
    """
    has_text = text is not None and str(text).strip() != ""
    has_finding = finding_id is not None and str(finding_id).strip() != ""
    if has_text == has_finding:
        raise ValueError("exactly one of text= or finding_id= is required")

    if limit < 0:
        raise ValueError("limit must be >= 0")

    exclude_task = task_ref.strip() if task_ref is not None and task_ref.strip() else None

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

    vectors = prov.embed([str(text).strip()])
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
    )


def find_related_prior_work_public(
    *,
    text: str | None = None,
    finding_id: str | None = None,
    task_ref: str | None = None,
    limit: int = DEFAULT_RELATED_PRIOR_WORK_LIMIT,
    min_score: float = DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE,
    provider: SupportsEmbed | None = None,
) -> dict[str, Any]:
    """Package-level entry: open the handoff DB and return a JSON-friendly dict.

    Keyword-only. See :func:`find_related_prior_work` for semantics.
    """
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        result = find_related_prior_work(
            conn,
            text=text,
            finding_id=finding_id,
            task_ref=task_ref,
            limit=limit,
            min_score=min_score,
            provider=provider,
        )
    return result.as_dict()
