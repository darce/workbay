"""Cosine top-K ranking over ``concept_embeddings`` (internal).

The reinjection consumer ranks stored handoff concepts by semantic similarity to
a composed anchor and re-surfaces the top-K. Stored vectors are L2-normalized at
write time (the provider normalizes), so cosine similarity is a plain dot
product. This module imports numpy and therefore belongs to the optional
``embeddings`` subpackage; callers import it lazily and treat ``ImportError``
(extra absent) as "semantic ranking off".
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast

import numpy as np

from .store import SupportsEmbed, deserialize_vector


def compose_anchor(
    provider: SupportsEmbed,
    *,
    persisted_anchor: np.ndarray | None,
    texts: list[str],
) -> np.ndarray | None:
    """Mean of the persisted transcript anchor + freshly embedded component texts.

    internal. ``texts`` are the live components composed at SessionStart
    (the shipped hook passes objective, focus, and pending-action texts); blanks
    are dropped. The function itself embeds whatever texts the caller supplies.
    ``persisted_anchor`` is the transcript-derived vector stored at compaction
    time (implementation note) and is included only when its dimensionality matches the
    provider (a model swap drops it rather than crashing). Returns ``None`` when
    there is nothing to compose, so the caller degrades to today's selection. The
    mean is returned un-normalized; :func:`rank_concepts_by_anchor` normalizes it
    (ranking order is scale-invariant). Component weighting is uniform here and
    is a implementation note tuning surface.
    """
    rows: list[np.ndarray] = []
    clean = [t for t in texts if t and t.strip()]
    if clean:
        rows.append(np.asarray(provider.embed(clean), dtype="<f4"))
    if persisted_anchor is not None:
        pa = np.asarray(persisted_anchor, dtype="<f4").reshape(1, -1)
        if pa.shape[1] == provider.dim:
            rows.append(pa)
    if not rows:
        return None
    return cast(np.ndarray, np.concatenate(rows, axis=0).mean(axis=0))


@dataclass(frozen=True)
class RankedConcept:
    """One ranked concept: its store key and cosine score against the anchor."""

    entity_kind: str
    entity_id: str
    score: float


@dataclass(frozen=True)
class ScoredConceptVector:
    """Ranked concept carrying its stored embedding vector for MMR selection."""

    entity_kind: str
    entity_id: str
    score: float
    vector: np.ndarray


def rank_candidate_vectors_by_anchor(
    conn: sqlite3.Connection,
    anchor: np.ndarray,
    task_ref: str,
    *,
    candidate_pool: int | None,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> list[ScoredConceptVector]:
    """Return up to ``candidate_pool`` concepts with vectors, ranked by cosine (desc)."""
    if candidate_pool is not None and candidate_pool <= 0:
        return []
    anchor = np.asarray(anchor, dtype="<f4").reshape(-1)
    norm = float(np.linalg.norm(anchor))
    if norm == 0.0:
        return []
    anchor = anchor / norm

    sql = "SELECT entity_kind, entity_id, vector, model_id FROM concept_embeddings WHERE task_ref = ?"
    params: list[object] = [task_ref]
    if entity_kinds:
        placeholders = ",".join("?" for _ in entity_kinds)
        sql += f" AND entity_kind IN ({placeholders})"
        params.extend(entity_kinds)

    scored: list[ScoredConceptVector] = []
    for row in conn.execute(sql, params).fetchall():
        entity_kind, entity_id, vector_blob, row_model_id = row[0], row[1], row[2], row[3]
        if model_id is not None and str(row_model_id) != model_id:
            continue
        vec = deserialize_vector(vector_blob)
        if vec.shape[0] != anchor.shape[0]:
            continue
        score = float(np.dot(anchor, vec))
        scored.append(ScoredConceptVector(str(entity_kind), str(entity_id), score, vec))

    scored.sort(key=lambda r: (-r.score, r.entity_kind, r.entity_id))
    return scored if candidate_pool is None else scored[:candidate_pool]


def rank_concepts_by_anchor(
    conn: sqlite3.Connection,
    anchor: np.ndarray,
    task_ref: str,
    *,
    top_k: int,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> list[RankedConcept]:
    """Return up to ``top_k`` concepts for ``task_ref`` ranked by cosine to ``anchor`` (desc).

    Compatibility projection over :func:`rank_candidate_vectors_by_anchor`.
    """
    if top_k <= 0:
        return []
    candidates = rank_candidate_vectors_by_anchor(
        conn,
        anchor,
        task_ref,
        candidate_pool=top_k,
        entity_kinds=entity_kinds,
        model_id=model_id,
    )
    return [RankedConcept(c.entity_kind, c.entity_id, c.score) for c in candidates]
