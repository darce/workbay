"""Recall@K efficacy eval for semantic reinjection (internal).

Compares the two reinjection *selection* arms on a labeled relevance set:

  arm A ("current")  = recency-ordered top-K over ``concept_embeddings``
                       (today's recency/ID selection signal)
  arm B ("semantic") = cosine top-K to the composed anchor (implementation note ranking)

and reports, per arm, recall@K against the labeled-relevant ref set plus the
on-wire token cost of that arm's ``relevant:`` line (the same line the
SessionStart hook emits). :func:`apply_recall_gate` is the pre-registered
decision rule: adopt arm B iff its recall improves at equal-or-lower token cost.

implementation note extends the same primitives for **record-level** retrieval eval
(semantic vs BM25 vs simplified RRF): :func:`recall_at_k` and :func:`mrr` are
ref-agnostic; :func:`apply_recall_gate` / :func:`apply_recall_gate_values` keep
the pre-registered cost-sensitive adopt rule without re-registering a new
decision surface ([EVAL-07]).

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
from typing import Hashable, Sequence, TypeVar

import numpy as np

from .ranking import rank_concepts_by_anchor

ConceptRef = tuple[str, str]  # (entity_kind, entity_id)
# Record-level unit for implementation note: (record_type, record_id). record_id is the
# string form of the source-table PK so set membership matches FTS + concept
# projection without int/str skew.
RecordRef = tuple[str, str]

RefT = TypeVar("RefT", bound=Hashable)


@dataclass(frozen=True)
class ArmRecall:
    """One arm's eval outcome: its selection, recall@K, and on-wire token cost."""

    arm: str
    selected: tuple[ConceptRef, ...]
    recall_at_k: float
    token_cost: int


def recall_at_k(selected: Sequence[RefT], relevant: Sequence[RefT]) -> float:
    """Fraction of the labeled-relevant set present in ``selected``.

    Returns 0.0 when ``relevant`` is empty (never divides by zero). ``selected``
    is de-duplicated so a repeated ref cannot inflate the hit count.
    Works for concept-level :data:`ConceptRef` and record-level :data:`RecordRef`.
    """
    rel = set(relevant)
    if not rel:
        return 0.0
    hits = len(rel & set(selected))
    return hits / len(rel)


def mrr(ranked: Sequence[RefT], relevant: Sequence[RefT]) -> float:
    """Mean reciprocal rank of the first relevant hit (0.0 if none).

    Single-query form: ``1/rank`` of the first relevant item in ``ranked``
    (1-based). Empty relevant set yields 0.0. Rank is taken over the ordered
    list as given (duplicates after the first occurrence of a ref are ignored
    for hit detection only via first-pass scan).
    """
    rel = set(relevant)
    if not rel:
        return 0.0
    for index, ref in enumerate(ranked, start=1):
        if ref in rel:
            return 1.0 / float(index)
    return 0.0


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


def apply_recall_gate_values(
    *,
    baseline_name: str,
    challenger_name: str,
    baseline_recall: float,
    challenger_recall: float,
    baseline_cost: float,
    challenger_cost: float,
    cost_unit: str = "tokens",
) -> dict[str, object]:
    """Pre-registered gate core ([EVAL-07]): adopt challenger iff recall↑ and cost≤.

    Shared by the implementation note reinjection arms (``current`` vs ``semantic``, cost =
    on-wire token proxy) and implementation note retrieval arms (e.g. BM25 baseline vs
    semantic/hybrid challenger, cost = artifact-MB proxy). The decision *rule*
    is fixed; only the arm labels and cost units change — no re-registered
    selection surface.

    ``cost_unit`` labels the cost proxy. Default ``"tokens"`` keeps Plan-0046
    backward-compat aliases (``tokens_*`` / ``recall_current`` /
    ``recall_semantic``). Non-token units (e.g. ``"artifact_mb"``) omit those
    aliases so 0141 artifact-MB gates are not mislabeled as token costs.
    """
    recall_improved = challenger_recall > baseline_recall
    cost_equal_or_lower = challenger_cost <= baseline_cost
    adopt = recall_improved and cost_equal_or_lower
    out: dict[str, object] = {
        "recommendation": "adopt" if adopt else "hold",
        "rule": (
            f"adopt {challenger_name} iff recall_{challenger_name} > recall_{baseline_name} "
            f"AND cost_{challenger_name} <= cost_{baseline_name}"
        ),
        "baseline_name": baseline_name,
        "challenger_name": challenger_name,
        "recall_baseline": baseline_recall,
        "recall_challenger": challenger_recall,
        "cost_baseline": baseline_cost,
        "cost_challenger": challenger_cost,
        "cost_unit": cost_unit,
        "recall_improved": recall_improved,
        "tokens_equal_or_lower": cost_equal_or_lower,
        "cost_equal_or_lower": cost_equal_or_lower,
    }
    # Plan-0046 token-cost aliases only when cost_unit is tokens ([EVAL-07]).
    if cost_unit == "tokens":
        out["recall_current"] = baseline_recall
        out["recall_semantic"] = challenger_recall
        out["tokens_current"] = baseline_cost
        out["tokens_semantic"] = challenger_cost
    return out


def apply_recall_gate(
    arms: dict[str, ArmRecall],
    *,
    baseline_key: str = "current",
    challenger_key: str = "semantic",
) -> dict[str, object]:
    """Pre-registered gate: adopt challenger iff recall improves at equal-or-lower cost.

    Default keys match implementation note (``"current"`` baseline, ``"semantic"`` challenger).
    implementation note callers may pass e.g. ``baseline_key="lexical"``,
    ``challenger_key="semantic"`` over :class:`ArmRecall` values whose
    ``token_cost`` holds the comparable cost proxy for that eval.
    """
    baseline = arms[baseline_key]
    challenger = arms[challenger_key]
    return apply_recall_gate_values(
        baseline_name=baseline.arm,
        challenger_name=challenger.arm,
        baseline_recall=baseline.recall_at_k,
        challenger_recall=challenger.recall_at_k,
        baseline_cost=float(baseline.token_cost),
        challenger_cost=float(challenger.token_cost),
    )
