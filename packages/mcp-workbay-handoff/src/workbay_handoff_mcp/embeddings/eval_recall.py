"""Recall@K efficacy eval for semantic reinjection (internal).

Compares the two reinjection *selection* arms on a labeled relevance set:

  arm A ("current")  = recency-ordered top-K over ``concept_embeddings``
                       (today's recency/ID selection signal)
  arm B ("semantic") = cosine top-K to the composed anchor (implementation note ranking)

and reports, per arm, recall@K against the labeled-relevant ref set plus the
on-wire token cost of that arm's ``relevant:`` line (the same line the
SessionStart hook emits). :func:`apply_recall_gate` is the pre-registered
decision rule: adopt arm B iff its recall improves at equal-or-lower token cost.

Both arms draw from the *same* candidate pool (the task's stored concepts), so
the comparison isolates the ranking signal (recency vs cosine) — an apples-to-
apples controlled measurement, not a re-implementation of the full cold-start
selector. This module imports numpy and so belongs to the optional
``embeddings`` subpackage; callers import it lazily.

This is eval-only / operator-facing scaffolding: it is intentionally *not*
referenced by the reinjection hot path (the SessionStart hook), only by the
implementation note eval test and any operator-run efficacy harness. It lives in ``src``
(beside ``ranking.py``) so such a harness can import it, not because the
runtime needs it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import numpy as np

from .ranking import rank_concepts_by_anchor

ConceptRef = tuple[str, str]  # (entity_kind, entity_id)


@dataclass(frozen=True)
class ArmRecall:
    """One arm's eval outcome: its selection, recall@K, and on-wire token cost."""

    arm: str
    selected: tuple[ConceptRef, ...]
    recall_at_k: float
    token_cost: int


def recall_at_k(selected: list[ConceptRef], relevant: list[ConceptRef]) -> float:
    """Fraction of the labeled-relevant set present in ``selected``.

    Returns 0.0 when ``relevant`` is empty (never divides by zero). ``selected``
    is de-duplicated so a repeated ref cannot inflate the hit count.
    """
    rel = set(relevant)
    if not rel:
        return 0.0
    hits = len(rel & set(selected))
    return hits / len(rel)


def _relevant_line_cost(selected: list[ConceptRef]) -> int:
    """Char length of a ``relevant: <kind>:<id>, ...`` line (0 if empty).

    Approximates the SessionStart hook's reinjected line for like-for-like refs.
    It is a controlled proxy, not byte-exact: the hook additionally runs the line
    through ``_sanitize_field`` and excludes ``handoff_state.objective``/``focus``
    from its rank kinds, neither of which this cost function models. For the
    plain ``kind:id`` refs the eval compares, the proxy is exact.
    """
    if not selected:
        return 0
    refs = ", ".join(f"{kind}:{entity_id}" for kind, entity_id in selected)
    return len(f"relevant: {refs}")


def select_current_recency(
    conn: sqlite3.Connection,
    task_ref: str,
    *,
    top_k: int,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> list[ConceptRef]:
    """Arm A baseline: the ``top_k`` most-recent concepts (today's recency signal).

    Ordered by ``created_at`` then ``(entity_kind, entity_id)`` descending so the
    result is deterministic when timestamps tie. Filters mirror the semantic arm
    (``entity_kinds`` / ``model_id``) so both arms rank the identical pool.
    """
    if top_k <= 0:
        return []
    sql = "SELECT entity_kind, entity_id FROM concept_embeddings WHERE task_ref = ?"
    params: list[object] = [task_ref]
    if model_id is not None:
        sql += " AND model_id = ?"
        params.append(model_id)
    if entity_kinds:
        placeholders = ",".join("?" for _ in entity_kinds)
        sql += f" AND entity_kind IN ({placeholders})"
        params.extend(entity_kinds)
    sql += " ORDER BY created_at DESC, entity_kind DESC, entity_id DESC LIMIT ?"
    params.append(top_k)
    return [(str(row[0]), str(row[1])) for row in conn.execute(sql, params).fetchall()]


def select_semantic_topk(
    conn: sqlite3.Connection,
    anchor: np.ndarray,
    task_ref: str,
    *,
    top_k: int,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> list[ConceptRef]:
    """Arm B: the implementation note cosine top-K, projected to ``(entity_kind, entity_id)`` refs."""
    ranked = rank_concepts_by_anchor(conn, anchor, task_ref, top_k=top_k, entity_kinds=entity_kinds, model_id=model_id)
    return [(r.entity_kind, r.entity_id) for r in ranked]


def evaluate_recall_arms(
    conn: sqlite3.Connection,
    *,
    anchor: np.ndarray,
    task_ref: str,
    relevant: list[ConceptRef],
    top_k: int,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> dict[str, ArmRecall]:
    """Run both arms over the same pool and return their recall@K + token cost."""
    rel = [(str(k), str(i)) for k, i in relevant]
    current = select_current_recency(conn, task_ref, top_k=top_k, entity_kinds=entity_kinds, model_id=model_id)
    semantic = select_semantic_topk(conn, anchor, task_ref, top_k=top_k, entity_kinds=entity_kinds, model_id=model_id)
    return {
        "current": ArmRecall("current", tuple(current), recall_at_k(current, rel), _relevant_line_cost(current)),
        "semantic": ArmRecall("semantic", tuple(semantic), recall_at_k(semantic, rel), _relevant_line_cost(semantic)),
    }


def apply_recall_gate(arms: dict[str, ArmRecall]) -> dict[str, object]:
    """Pre-registered gate: adopt arm B iff recall improves at equal-or-lower tokens.

    ``arms`` must carry ``"current"`` (arm A) and ``"semantic"`` (arm B). Returns
    a JSON-able verdict (``recommendation`` is ``"adopt"`` or ``"hold"``) carrying
    the per-arm recall and token figures the decision turned on.
    """
    current = arms["current"]
    semantic = arms["semantic"]
    recall_improved = semantic.recall_at_k > current.recall_at_k
    tokens_equal_or_lower = semantic.token_cost <= current.token_cost
    adopt = recall_improved and tokens_equal_or_lower
    return {
        "recommendation": "adopt" if adopt else "hold",
        "rule": "adopt arm B (semantic) iff recall_semantic > recall_current AND tokens_semantic <= tokens_current",
        "recall_current": current.recall_at_k,
        "recall_semantic": semantic.recall_at_k,
        "tokens_current": current.token_cost,
        "tokens_semantic": semantic.token_cost,
        "recall_improved": recall_improved,
        "tokens_equal_or_lower": tokens_equal_or_lower,
    }
