"""implementation note retrieval-eval harness: S0 corpus intersection + S2 arms/metrics.

Offline, deterministic, batch-only. Compares three arms over one query set on
the **record-level corpus intersection** (decisions + findings + blockers):

  A semantic  — ``rank_candidate_vectors_by_anchor`` + ``compose_anchor``,
                projected concept → owning record before scoring
  B lexical   — FTS5/BM25 over the intersection tables (``search_handoff`` SQL shape)
  C hybrid    — simplified RRF (k=60, original-query 2×) fusing A+B

Metrics: Recall@5/@10 (:func:`recall_at_k`), MRR (:func:`mrr`), and a cost
column (artifact MB / native deps / failure modes). Arm D (Model2Vec) is out
of scope (concept_embeddings PK collision). Ground truth is operator-authored
(S1); this module only consumes the fixture interface.

Heuristics: [EVAL-01] B is the offline baseline; [EVAL-07] gate inherited from
``apply_recall_gate``; [EMB-01]/[EMB-05] one pinned embedding space; [DBG-05]
do not mine anchors as labels; [EVAL-15] private-corpus eval stays private.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from .eval_recall import RecordRef, apply_recall_gate_values, mrr, recall_at_k
from .eval_rrf import (
    ORIGINAL_QUERY_WEIGHT,
    RRF_K,
    SIMPLIFIED_RRF_DISCLAIMER,
    rrf_fuse_semantic_lexical,
)
from .model_pin import MODEL_ID, MODEL_PIN
from .ranking import compose_anchor, rank_candidate_vectors_by_anchor
from .store import SupportsEmbed

# ---------------------------------------------------------------------------
# S0 — corpus intersection + concept→record projection
# ---------------------------------------------------------------------------

# Record types present in both concept_embeddings (via field-level kinds) and
# FTS virtual tables. actions / verified_tests are FTS-only; handoff_state /
# compaction kinds are embedding-only — both excluded from the shared unit.
INTERSECTION_RECORD_TYPES: tuple[str, ...] = ("decision", "finding", "blocker")

# entity_kind values that project into the intersection (store.CONCEPT_ENTITY_KINDS
# minus handoff_state.* and compaction.prose_residual).
INTERSECTION_ENTITY_KINDS: tuple[str, ...] = (
    "decision.rationale",
    "finding.description",
    "finding.fix",
    "finding.resolution_notes",
    "blocker.description",
)

# entity_kind → record_type for downward projection (concept id → owning record).
_ENTITY_KIND_TO_RECORD_TYPE: dict[str, str] = {
    "decision.rationale": "decision",
    "finding.description": "finding",
    "finding.fix": "finding",
    "finding.resolution_notes": "finding",
    "blocker.description": "blocker",
}

# Mirrors core._RECORD_TYPE_FTS_MAP for the intersection only (core.py:167).
_INTERSECTION_FTS_MAP: dict[str, tuple[str, bool]] = {
    "decision": ("decisions_fts", False),
    "finding": ("findings_fts", True),
    "blocker": ("blockers_fts", True),
}

_FTS5_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")

# Default candidate pool over-fetch so multi-field findings still yield top_k records.
_SEMANTIC_POOL_MULTIPLIER = 8

# Hybrid RRF fuses A/B lists deeper than metric top_k so a record ranked just
# outside metric depth on both arms remains fusion-reachable. Standalone A/B
# metrics still use metric top_k only.
_RRF_POOL_MULTIPLIER = 5
_RRF_POOL_MIN = 50


def rrf_pool_depth(top_k: int) -> int:
    """Candidate-list depth for hybrid RRF before cutting to metric ``top_k``.

    ``max(top_k * 5, 50)`` — large enough that ranks 11..N on both arms can
    still surface in the fused top_k after RRF. Returns 0 when ``top_k <= 0``.
    """
    if top_k <= 0:
        return 0
    return max(top_k * _RRF_POOL_MULTIPLIER, _RRF_POOL_MIN)


@dataclass(frozen=True)
class ArmCost:
    """Cost / fragility column for quality-per-fragility decisions."""

    arm: str
    artifact_mb: float
    native_deps: str
    failure_modes: str
    notes: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


# Pinned cost table — not measured per-run; documents fragility of each arm.
# Semantic artifact size is the published ~147 MB int8 ONNX (model_pin / guide).
ARM_COST_TABLE: dict[str, ArmCost] = {
    "semantic": ArmCost(
        arm="semantic",
        artifact_mb=147.0,
        native_deps="numpy, onnxruntime, tokenizers",
        failure_modes=(
            "ONNX artifact missing; digest mismatch; OOM / provisioning failure on "
            "constrained hosts; model_id pin skew vs stored vectors"
        ),
        notes=f"model_id={MODEL_ID}; revision pin={MODEL_PIN.source_revision}",
    ),
    "lexical": ArmCost(
        arm="lexical",
        artifact_mb=0.0,
        native_deps="sqlite3 FTS5 (stdlib / system SQLite)",
        failure_modes="FTS5 virtual tables unavailable; MATCH syntax error after sanitize",
        notes="BM25 baseline; no embedding provider required ([EVAL-01])",
    ),
    "hybrid_rrf": ArmCost(
        arm="hybrid_rrf",
        artifact_mb=147.0,
        native_deps="numpy, onnxruntime, tokenizers + sqlite3 FTS5",
        failure_modes="Union of semantic + lexical failure modes; RRF is pure CPU fusion",
        notes=SIMPLIFIED_RRF_DISCLAIMER,
    ),
}


def project_concept_to_record(entity_kind: str, entity_id: str) -> RecordRef | None:
    """Project a field-level concept key down to its owning record, or None.

    Intersection kinds map to ``(record_type, str(entity_id))``. Kinds outside
    the intersection (handoff_state.*, compaction.*) return None and must not
    enter record-level scoring.
    """
    record_type = _ENTITY_KIND_TO_RECORD_TYPE.get(entity_kind)
    if record_type is None:
        return None
    return (record_type, str(entity_id))


def project_scored_concepts_to_records(
    concepts: Sequence[tuple[str, str, float]],
    *,
    top_k: int | None = None,
) -> list[RecordRef]:
    """Collapse scored concepts to unique records, keeping the best score per record.

    ``concepts`` items are ``(entity_kind, entity_id, score)`` with higher score
    better. Records outside the intersection are dropped. Output is ordered by
    score desc, then (record_type, record_id) for determinism.
    """
    best: dict[RecordRef, float] = {}
    for entity_kind, entity_id, score in concepts:
        ref = project_concept_to_record(entity_kind, entity_id)
        if ref is None:
            continue
        prev = best.get(ref)
        if prev is None or score > prev:
            best[ref] = float(score)
    ordered = sorted(best.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
    refs = [ref for ref, _ in ordered]
    if top_k is None:
        return refs
    if top_k <= 0:
        return []
    return refs[:top_k]


# ---------------------------------------------------------------------------
# S2 — arms
# ---------------------------------------------------------------------------


def run_semantic_arm(
    conn: sqlite3.Connection,
    provider: SupportsEmbed,
    *,
    query: str,
    task_ref: str,
    top_k: int,
    model_id: str | None = None,
    entity_kinds: Sequence[str] = INTERSECTION_ENTITY_KINDS,
) -> list[RecordRef]:
    """Arm A: compose anchor from query, cosine-rank concepts, project to records.

    Uses :func:`compose_anchor` with ``texts=[query]`` and
    :func:`rank_candidate_vectors_by_anchor` (task_ref-scoped). No hot path.
    """
    if top_k <= 0:
        return []
    anchor = compose_anchor(provider, persisted_anchor=None, texts=[query])
    if anchor is None:
        return []
    pool = max(top_k * _SEMANTIC_POOL_MULTIPLIER, top_k)
    kinds = tuple(entity_kinds)
    scored = rank_candidate_vectors_by_anchor(
        conn,
        anchor,
        task_ref,
        candidate_pool=pool,
        entity_kinds=kinds,
        model_id=model_id if model_id is not None else provider.model_id,
    )
    concepts = [(c.entity_kind, c.entity_id, c.score) for c in scored]
    return project_scored_concepts_to_records(concepts, top_k=top_k)


def _fts_match_query(query: str) -> str | None:
    """Build a token-OR FTS5 MATCH expression for the lexical eval arm.

    INTENTIONALLY diverges from ``search_handoff``'s whole-query phrase-quoting;
    the eval needs a fair token baseline, and production-FTS-on-NL behavior is
    recorded separately (decision 4705).

    Control chars are stripped (same as production sanitize), then whitespace
    tokens of length >= 3 are individually double-quoted and joined with OR.
    """
    stripped = _FTS5_CONTROL_RE.sub(" ", query).strip()
    if not stripped:
        return None
    terms: list[str] = []
    for raw in stripped.split():
        if len(raw) <= 2:
            continue
        terms.append('"' + raw.replace('"', '""') + '"')
    if not terms:
        return None
    return " OR ".join(terms)


def run_lexical_arm(
    conn: sqlite3.Connection,
    *,
    query: str,
    task_ref: str,
    top_k: int,
    record_types: Sequence[str] = INTERSECTION_RECORD_TYPES,
) -> list[RecordRef]:
    """Arm B: FTS5/BM25 over intersection tables (token-OR MATCH, SQL shape of search_handoff).

    Task-scoped. Rank is SQLite FTS5 ``rank`` (lower/more-negative is better).
    Does not require an embedding provider ([EVAL-01] offline baseline).
    Query construction is a fair token baseline via :func:`_fts_match_query`, not
    production whole-query phrase quoting.
    """
    if top_k <= 0:
        return []
    fts_query = _fts_match_query(query)
    if fts_query is None:
        return []

    hits: list[tuple[float, RecordRef]] = []
    for rtype in record_types:
        if rtype not in _INTERSECTION_FTS_MAP:
            continue
        fts_table, has_status = _INTERSECTION_FTS_MAP[rtype]
        status_col = f"{fts_table}.status" if has_status else "NULL AS status"
        if rtype == "decision":
            # Mirror core.search_handoff decision branch (include_system=False).
            sql = f"""
                SELECT {fts_table}.record_id AS record_id,
                       rank
                FROM {fts_table}
                JOIN decisions ON decisions.id = {fts_table}.rowid
                  AND (0 OR COALESCE(decisions.decision_origin, 'agent') != 'system')
                WHERE {fts_table} MATCH ?
                  AND {fts_table}.task_ref = ?
                ORDER BY rank
                LIMIT ?
            """
            params: tuple[object, ...] = (fts_query, task_ref, top_k)
        else:
            sql = f"""
                SELECT record_id, rank
                FROM {fts_table}
                WHERE {fts_table} MATCH ?
                  AND task_ref = ?
                ORDER BY rank
                LIMIT ?
            """
            params = (fts_query, task_ref, top_k)
            # silence unused status_col for non-decision (kept for shape parity)
            _ = status_col
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            continue
        for row in rows:
            record_id = str(row[0])
            rank = float(row[1] if row[1] is not None else 0.0)
            hits.append((rank, (rtype, record_id)))

    # Lower FTS rank is better; break ties on (record_type, record_id).
    hits.sort(key=lambda item: (item[0], item[1][0], item[1][1]))
    seen: set[RecordRef] = set()
    out: list[RecordRef] = []
    for _, ref in hits:
        if ref in seen:
            continue
        seen.add(ref)
        out.append(ref)
        if len(out) >= top_k:
            break
    return out


def run_hybrid_arm(
    semantic_ranked: Sequence[RecordRef],
    lexical_ranked: Sequence[RecordRef],
    *,
    top_k: int,
    rrf_k: int = RRF_K,
    original_query_weight: float = ORIGINAL_QUERY_WEIGHT,
) -> list[RecordRef]:
    """Arm C: simplified RRF fuse of A+B (k=60, original-query 2×)."""
    if top_k <= 0:
        return []
    return rrf_fuse_semantic_lexical(
        semantic_ranked,
        lexical_ranked,
        k=rrf_k,
        original_query_weight=original_query_weight,
        top_k=top_k,
    )


# ---------------------------------------------------------------------------
# Metrics + per-query / pooled evaluation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArmResult:
    """One arm's record-level ranking + metrics for a single query."""

    arm: str
    selected: tuple[RecordRef, ...]
    recall_at_5: float
    recall_at_10: float
    mrr: float
    cost: ArmCost

    def as_dict(self) -> dict[str, object]:
        return {
            "arm": self.arm,
            "selected": list(self.selected),
            "recall_at_5": self.recall_at_5,
            "recall_at_10": self.recall_at_10,
            "mrr": self.mrr,
            "cost": self.cost.as_dict(),
        }


@dataclass(frozen=True)
class QueryEvalResult:
    """Per-query three-arm evaluation (task-scoped)."""

    query_id: str
    query: str
    task_ref: str
    relevant: tuple[RecordRef, ...]
    arms: dict[str, ArmResult]
    gate_semantic_vs_lexical: dict[str, object]
    gate_hybrid_vs_lexical: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "query_id": self.query_id,
            "query": self.query,
            "task_ref": self.task_ref,
            "relevant": list(self.relevant),
            "arms": {name: arm.as_dict() for name, arm in self.arms.items()},
            "gate_semantic_vs_lexical": self.gate_semantic_vs_lexical,
            "gate_hybrid_vs_lexical": self.gate_hybrid_vs_lexical,
        }


def _score_arm(
    arm_name: str,
    ranked: Sequence[RecordRef],
    relevant: Sequence[RecordRef],
    *,
    list_top_k: int = 10,
) -> ArmResult:
    selected = tuple(ranked[:list_top_k])
    cost = ARM_COST_TABLE[arm_name]
    return ArmResult(
        arm=arm_name,
        selected=selected,
        recall_at_5=recall_at_k(list(ranked[:5]), list(relevant)),
        recall_at_10=recall_at_k(list(ranked[:10]), list(relevant)),
        mrr=mrr(list(ranked), list(relevant)),
        cost=cost,
    )


def evaluate_retrieval_query(
    conn: sqlite3.Connection,
    provider: SupportsEmbed | None,
    *,
    query_id: str,
    query: str,
    task_ref: str,
    relevant: Sequence[RecordRef],
    top_k: int = 10,
    model_id: str | None = None,
) -> QueryEvalResult:
    """Run arms A/B/C for one query and attach inherited gate comparisons.

    ``provider`` may be None only when the caller supplies precomputed semantic
    rankings via a later extension; today a provider is required for arm A/C.
    Arm B still runs if the provider is absent (lexical isolation).
    """
    rel = [(str(rt), str(rid)) for rt, rid in relevant]

    # Standalone A/B metrics must fetch at least depth-10 so Recall@10 is a
    # genuine measurement even when the caller passes top_k < 10 ([EVAL-01]).
    # _score_arm still slices ranked[:5]/[:10] for honesty at each @k.
    metric_depth = max(10, top_k)
    if provider is not None:
        semantic = run_semantic_arm(
            conn,
            provider,
            query=query,
            task_ref=task_ref,
            top_k=metric_depth,
            model_id=model_id,
        )
    else:
        semantic = []

    lexical = run_lexical_arm(conn, query=query, task_ref=task_ref, top_k=metric_depth)

    # Hybrid path only: fuse deeper A/B candidate pools, then cut to top_k.
    # Metric-depth lists above are intentionally *not* reused — RRF over
    # already-truncated top_k lists cannot surface a record ranked 11..N on
    # both arms (PLAN0141-REVIEW-M1).
    pool = rrf_pool_depth(top_k)
    if provider is not None:
        semantic_pool = run_semantic_arm(
            conn,
            provider,
            query=query,
            task_ref=task_ref,
            top_k=pool,
            model_id=model_id,
        )
    else:
        semantic_pool = []
    lexical_pool = run_lexical_arm(conn, query=query, task_ref=task_ref, top_k=pool)
    hybrid = run_hybrid_arm(semantic_pool, lexical_pool, top_k=top_k)

    arms = {
        "semantic": _score_arm("semantic", semantic, rel),
        "lexical": _score_arm("lexical", lexical, rel),
        "hybrid_rrf": _score_arm("hybrid_rrf", hybrid, rel),
    }

    # Inherit pre-registered gate: BM25 is the baseline challengers must beat
    # ([EVAL-01], [EVAL-07]). Cost proxy = artifact_mb from the cost column.
    # cost_unit="artifact_mb" omits Plan-0046 token aliases (finding 7683).
    gate_sem = apply_recall_gate_values(
        baseline_name="lexical",
        challenger_name="semantic",
        baseline_recall=arms["lexical"].recall_at_10,
        challenger_recall=arms["semantic"].recall_at_10,
        baseline_cost=arms["lexical"].cost.artifact_mb,
        challenger_cost=arms["semantic"].cost.artifact_mb,
        cost_unit="artifact_mb",
    )
    gate_hyb = apply_recall_gate_values(
        baseline_name="lexical",
        challenger_name="hybrid_rrf",
        baseline_recall=arms["lexical"].recall_at_10,
        challenger_recall=arms["hybrid_rrf"].recall_at_10,
        baseline_cost=arms["lexical"].cost.artifact_mb,
        challenger_cost=arms["hybrid_rrf"].cost.artifact_mb,
        cost_unit="artifact_mb",
    )

    return QueryEvalResult(
        query_id=query_id,
        query=query,
        task_ref=task_ref,
        relevant=tuple(rel),
        arms=arms,
        gate_semantic_vs_lexical=gate_sem,
        gate_hybrid_vs_lexical=gate_hyb,
    )


@dataclass
class PooledArmMetrics:
    arm: str
    mean_recall_at_5: float
    mean_recall_at_10: float
    mean_mrr: float
    cost: ArmCost


@dataclass
class RetrievalEvalReport:
    """Full harness output: per-query rows + pooled metrics + cost table."""

    cases: list[QueryEvalResult] = field(default_factory=list)
    pooled: dict[str, PooledArmMetrics] = field(default_factory=dict)
    task_refs: list[str] = field(default_factory=list)
    fixture_label: str = "unspecified"
    notes: list[str] = field(default_factory=list)
    # S3 verdict is intentionally not auto-filled until real S1 ground truth exists.
    # Operator snapshot path fills via compute_verdict / run_snapshot_eval.
    verdict: str | None = None
    verdict_detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fixture_label": self.fixture_label,
            "task_refs": list(self.task_refs),
            "notes": list(self.notes),
            "verdict": self.verdict,
            "verdict_detail": self.verdict_detail,
            "pooled": {
                name: {
                    "arm": m.arm,
                    "mean_recall_at_5": m.mean_recall_at_5,
                    "mean_recall_at_10": m.mean_recall_at_10,
                    "mean_mrr": m.mean_mrr,
                    "cost": m.cost.as_dict(),
                }
                for name, m in self.pooled.items()
            },
            "cases": [c.as_dict() for c in self.cases],
            "simplified_rrf_disclaimer": SIMPLIFIED_RRF_DISCLAIMER,
        }


def pool_arm_metrics(cases: Sequence[QueryEvalResult]) -> dict[str, PooledArmMetrics]:
    """Mean Recall@5/@10 and MRR across queries, per arm."""
    if not cases:
        return {}
    arm_names = ("semantic", "lexical", "hybrid_rrf")
    out: dict[str, PooledArmMetrics] = {}
    n = float(len(cases))
    for name in arm_names:
        r5 = sum(c.arms[name].recall_at_5 for c in cases) / n
        r10 = sum(c.arms[name].recall_at_10 for c in cases) / n
        mm = sum(c.arms[name].mrr for c in cases) / n
        out[name] = PooledArmMetrics(
            arm=name,
            mean_recall_at_5=r5,
            mean_recall_at_10=r10,
            mean_mrr=mm,
            cost=ARM_COST_TABLE[name],
        )
    return out


def sample_task_refs_stratified(
    task_sizes: Mapping[str, int],
    *,
    n: int = 10,
    seed: int = 0,
) -> list[str]:
    """Sample ~n task_refs stratified by corpus size (large/medium/small).

    ``task_sizes`` maps task_ref → intersection record count. Deterministic
    given ``seed``. Returns fewer than n when the corpus is small. Used by
    operator harnesses over a pinned snapshot; smoke tests pass an explicit
    task_ref list instead.
    """
    if n <= 0 or not task_sizes:
        return []
    items = sorted(task_sizes.items(), key=lambda kv: (-kv[1], kv[0]))
    if len(items) <= n:
        return [t for t, _ in items]

    # Tertile buckets by rank order (already sorted large→small).
    third = max(1, len(items) // 3)
    buckets = [
        items[:third],  # large
        items[third : 2 * third],  # medium
        items[2 * third :],  # small
    ]
    # Round-robin pick with seed offset for stability.
    per_bucket = max(1, n // 3)
    chosen: list[str] = []
    for bucket in buckets:
        start = seed % max(1, len(bucket))
        rotated = bucket[start:] + bucket[:start]
        for task_ref, _ in rotated[:per_bucket]:
            if task_ref not in chosen:
                chosen.append(task_ref)
    # Fill remainder from overall order if under-sampled.
    if len(chosen) < n:
        for task_ref, _ in items:
            if task_ref not in chosen:
                chosen.append(task_ref)
            if len(chosen) >= n:
                break
    return chosen[:n]


def run_retrieval_eval(
    conn: sqlite3.Connection,
    provider: SupportsEmbed | None,
    cases: Sequence[Mapping[str, Any]],
    *,
    top_k: int = 10,
    model_id: str | None = None,
    fixture_label: str = "unspecified",
) -> RetrievalEvalReport:
    """Evaluate a list of query-case mappings (fixture interface).

    Each case mapping must provide: ``query_id``, ``query``, ``task_ref``,
    ``relevant`` (sequence of ``(record_type, record_id)``).
    """
    results: list[QueryEvalResult] = []
    task_refs: list[str] = []
    for case in cases:
        qid = str(case["query_id"])
        query = str(case["query"])
        task_ref = str(case["task_ref"])
        relevant = [(str(rt), str(rid)) for rt, rid in case["relevant"]]
        if task_ref not in task_refs:
            task_refs.append(task_ref)
        results.append(
            evaluate_retrieval_query(
                conn,
                provider,
                query_id=qid,
                query=query,
                task_ref=task_ref,
                relevant=relevant,
                top_k=top_k,
                model_id=model_id,
            )
        )
    notes = [
        "Offline batch harness (implementation note S0+S2). No hot path, no daemon.",
        SIMPLIFIED_RRF_DISCLAIMER,
        "Arm D (Model2Vec) is out of scope (concept_embeddings PK has no model_id).",
        "S3 verdict requires operator S1 ground truth — not produced by this run.",
        f"Embedding model pin: {MODEL_ID} @ {MODEL_PIN.source_revision} ([EMB-05]).",
    ]
    if "smoke" in fixture_label.lower() or "synthetic" in fixture_label.lower():
        notes.insert(
            0,
            "FIXTURE IS SYNTHETIC / non-authoritative / machinery-test-only — not real ground truth ([DBG-05]).",
        )
    return RetrievalEvalReport(
        cases=results,
        pooled=pool_arm_metrics(results),
        task_refs=task_refs,
        fixture_label=fixture_label,
        notes=notes,
        verdict=None,
    )


def compute_verdict(report: RetrievalEvalReport) -> str | None:
    """Compute a cost-sensitive verdict from pooled arm metrics ([EVAL-01], [EVAL-07]).

    Baseline = lexical (BM25); challengers = semantic and hybrid_rrf.
    Recall = mean_recall_at_10; cost = cost.artifact_mb. Mutates
    ``report.verdict_detail`` with the gate dicts and veto surface, then
    returns the short decision string (``"hold"`` / ``"retain_lexical"`` /
    ``"adopt"``). Idempotent over a fixed ``report.pooled``.

    Recall and cost are surfaced **independently**: under the live
    :data:`ARM_COST_TABLE` (semantic/hybrid 147 MB vs lexical 0 MB) the
    cost gate is structurally a veto for any recall, so operators always
    see the cost signal even when recall did not improve.
    """
    pooled = report.pooled
    if not pooled or "lexical" not in pooled:
        report.verdict_detail = {
            "decision": None,
            "recall_improved": False,
            "cost_veto": False,
            "veto": "",
        }
        return None

    baseline = pooled["lexical"]
    gate_sem = apply_recall_gate_values(
        baseline_name="lexical",
        challenger_name="semantic",
        baseline_recall=baseline.mean_recall_at_10,
        challenger_recall=pooled["semantic"].mean_recall_at_10 if "semantic" in pooled else 0.0,
        baseline_cost=baseline.cost.artifact_mb,
        challenger_cost=pooled["semantic"].cost.artifact_mb if "semantic" in pooled else 0.0,
        cost_unit="artifact_mb",
    )
    gate_hyb = apply_recall_gate_values(
        baseline_name="lexical",
        challenger_name="hybrid_rrf",
        baseline_recall=baseline.mean_recall_at_10,
        challenger_recall=pooled["hybrid_rrf"].mean_recall_at_10 if "hybrid_rrf" in pooled else 0.0,
        baseline_cost=baseline.cost.artifact_mb,
        challenger_cost=pooled["hybrid_rrf"].cost.artifact_mb if "hybrid_rrf" in pooled else 0.0,
        cost_unit="artifact_mb",
    )

    any_recall_improved = bool(gate_sem["recall_improved"]) or bool(gate_hyb["recall_improved"])
    any_adopt = gate_sem["recommendation"] == "adopt" or gate_hyb["recommendation"] == "adopt"
    # Cost surface is independent of recall ([EVAL-07]): true when a
    # challenger's artifact_mb exceeds the lexical baseline and nothing
    # was adopted. Under ARM_COST_TABLE this is the structural hold path.
    any_cost_higher = (not bool(gate_sem["cost_equal_or_lower"])) or (not bool(gate_hyb["cost_equal_or_lower"]))
    cost_veto = any_cost_higher and not any_adopt

    if any_adopt:
        decision = "adopt"
        veto = ""
    elif cost_veto and any_recall_improved:
        decision = "hold"
        veto = "cost veto: challenger recall improved but artifact_mb exceeds lexical baseline"
    elif cost_veto:
        # Structural cost hold — recall did not improve, but cost still
        # independently blocks free adoption of semantic/hybrid.
        decision = "hold"
        veto = "cost veto: semantic/hybrid artifact_mb exceeds lexical baseline (0 MB)"
    else:
        # No recall gain and no cost overhang — retain the BM25 baseline.
        decision = "retain_lexical"
        veto = ""

    report.verdict_detail = {
        "decision": decision,
        "recall_improved": any_recall_improved,
        "cost_veto": cost_veto,
        "veto": veto,
        "gate_semantic_vs_lexical": gate_sem,
        "gate_hybrid_vs_lexical": gate_hyb,
    }
    return decision


def run_snapshot_eval(
    snapshot_path: str,
    *,
    provider: SupportsEmbed | None,
    cases: Sequence[Mapping[str, Any]] | None = None,
    top_k: int = 10,
) -> RetrievalEvalReport:
    """Operator path: evaluate an out-of-tree handoff.db snapshot with a real verdict.

    Opens ``snapshot_path`` directly (does not use the process-global DB).
    Requires operator ground-truth ``cases`` (caller supplies them — CLI loads
    from the tests fixture; tests pass ``bound_smoke_cases()``). Does not
    import the tests package from src.
    """
    if cases is None:
        raise ValueError("run_snapshot_eval requires operator ground-truth cases")

    conn = sqlite3.connect(snapshot_path)
    try:
        report = run_retrieval_eval(
            conn,
            provider,
            cases,
            top_k=top_k,
            fixture_label="operator-snapshot",
        )
        report.verdict = compute_verdict(report)
        return report
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Markdown report (new convention; shape borrowed from ace_metrics renderers)
# ---------------------------------------------------------------------------


def render_retrieval_eval_markdown(report: RetrievalEvalReport) -> str:
    """Render a markdown report for ``make retrieval-eval``.

    Shape mirrors ``ace_metrics._render_retrieval_activity``: section headers
    and bullet lines, not a hot-path UI.
    """
    lines: list[str] = [
        "# Retrieval Eval Report (implementation note)",
        "",
        f"- Fixture: `{report.fixture_label}`",
        f"- Task refs: {', '.join(f'`{t}`' for t in report.task_refs) or '_none_'}",
        f"- Queries: {len(report.cases)}",
        f"- Verdict: {report.verdict or '_deferred (S1 ground truth + S3 decision record)_'}",
        "",
        "## Notes",
    ]
    for note in report.notes:
        lines.append(f"- {note}")

    lines.extend(["", "## Cost Column (quality-per-fragility)"])
    for name in ("lexical", "semantic", "hybrid_rrf"):
        cost = ARM_COST_TABLE[name]
        lines.append(
            f"- **{name}**: artifact_mb={cost.artifact_mb}  "
            f"native_deps=`{cost.native_deps}`  "
            f"failure_modes={cost.failure_modes}"
        )

    lines.extend(["", "## Pooled Metrics"])
    if not report.pooled:
        lines.append("_No cases evaluated._")
    else:
        lines.append("| arm | mean Recall@5 | mean Recall@10 | mean MRR | artifact_mb |")
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for name in ("lexical", "semantic", "hybrid_rrf"):
            m = report.pooled[name]
            lines.append(
                f"| {name} | {m.mean_recall_at_5:.3f} | {m.mean_recall_at_10:.3f} | "
                f"{m.mean_mrr:.3f} | {m.cost.artifact_mb:.1f} |"
            )

    lines.extend(["", "## Per-Query Results"])
    if not report.cases:
        lines.append("_No per-query rows._")
    for case in report.cases:
        lines.append("")
        lines.append(f"### `{case.query_id}` (task `{case.task_ref}`)")
        lines.append(f"- Query: {case.query}")
        lines.append(f"- Relevant: `{list(case.relevant)}`")
        for name in ("lexical", "semantic", "hybrid_rrf"):
            arm = case.arms[name]
            lines.append(
                f"- **{name}**: R@5={arm.recall_at_5:.3f}  R@10={arm.recall_at_10:.3f}  "
                f"MRR={arm.mrr:.3f}  selected=`{list(arm.selected[:5])}`"
            )
        lines.append(f"- Gate semantic vs lexical: `{case.gate_semantic_vs_lexical.get('recommendation')}`")
        lines.append(f"- Gate hybrid vs lexical: `{case.gate_hybrid_vs_lexical.get('recommendation')}`")

    lines.extend(
        [
            "",
            "## Inherited Gate ([EVAL-07])",
            "- Rule: adopt challenger iff recall improves at equal-or-lower cost "
            "(from `apply_recall_gate` / `apply_recall_gate_values`).",
            "- Baseline arm for 0141: **lexical (BM25)** ([EVAL-01]).",
            "- Cost proxy: artifact_mb from the cost column.",
            "",
            "## Out of Scope",
            "- Arm D Model2Vec (PK collision on concept_embeddings).",
            "- S1 operator ground truth authoring.",
            "- S3 recorded verdict on a real snapshot.",
        ]
    )
    return "\n".join(lines) + "\n"
