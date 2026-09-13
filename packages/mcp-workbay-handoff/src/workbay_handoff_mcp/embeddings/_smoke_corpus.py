"""Synthetic smoke corpus for implementation note harness machinery tests.

**NOT real ground truth.** Marked synthetic / non-authoritative / machinery-
test-only. Auto-mining handoff objectives as labels is forbidden ([DBG-05]).
Operator-authored S1 cases replace this fixture for real evaluation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from workbay_handoff_mcp import api as mcp_server
from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.embeddings.provider import EMBEDDING_DIM
from workbay_handoff_mcp.embeddings.store import serialize_vector, text_hash
from workbay_handoff_mcp.shared_schema import _get_db_connection

SMOKE_FIXTURE_LABEL = "smoke-synthetic-non-authoritative"
SMOKE_FIXTURE_BANNER = "SYNTHETIC / non-authoritative / for-machinery-test-only — never present as real ground truth"
SMOKE_TASK_REF = "SMOKE-RETRIEVAL-01"
_MODEL_ID = "gte-base-en-v1.5"

# Distinct marker phrases so BM25 and one-hot semantic both have a clear target.
_DECISION_MARKER = "alpha-unique-backoff-policy-marker"
_FINDING_MARKER = "beta-unique-webhook-validation-marker"
_BLOCKER_MARKER = "gamma-unique-schema-migration-marker"
_NOISE_PREFIX = "unrelated-noise-concept"


class FakeOneHotProvider:
    """Deterministic one-hot provider: identical text → identical unit vector."""

    def __init__(self, dim: int = EMBEDDING_DIM, model_id: str = _MODEL_ID) -> None:
        self._dim = dim
        self._model_id = model_id

    @property
    def dim(self) -> int:
        return self._dim

    @property
    def model_id(self) -> str:
        return self._model_id

    def embed(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), self._dim), dtype=np.float32)
        for i, text in enumerate(texts):
            out[i, int(text_hash(text), 16) % self._dim] = 1.0
        return out


def _insert_concept(
    conn,
    *,
    kind: str,
    entity_id: str,
    task_ref: str,
    text: str,
    provider: FakeOneHotProvider,
    created_at: str,
) -> None:
    blob = serialize_vector(provider.embed([text])[0])
    conn.execute(
        "INSERT INTO concept_embeddings (entity_kind, entity_id, task_ref, text_hash, dim, vector, "
        "model_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (kind, entity_id, task_ref, text_hash(text), EMBEDDING_DIM, blob, provider.model_id, created_at),
    )


def build_smoke_runtime_corpus(workspace_root: Path) -> tuple[FakeOneHotProvider, str]:
    """Configure an isolated runtime, seed intersection rows + embeddings + FTS.

    Returns ``(provider, task_ref)``. Side effect: configures the global handoff
    runtime so :func:`_get_db_connection` points at this workspace.
    """
    state_dir = workspace_root / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    runtime = RuntimeConfig.for_workspace(
        workspace_root,
        state_dir=state_dir,
        current_task_path=workspace_root / "CURRENT_TASK.json",
    )
    mcp_server.configure_runtime(runtime)
    provider = FakeOneHotProvider()
    task_ref = SMOKE_TASK_REF

    with _get_db_connection() as conn:
        # Target decision (relevant for smoke-q-decision).
        cur = conn.execute(
            "INSERT INTO decisions (task_ref, session, decision, rationale) VALUES (?, ?, ?, ?)",
            (task_ref, "s", "adopt backoff", f"Use {_DECISION_MARKER} for retries"),
        )
        decision_id = int(cur.lastrowid)

        # Noise decisions (distinct text).
        for i in range(3):
            conn.execute(
                "INSERT INTO decisions (task_ref, session, decision, rationale) VALUES (?, ?, ?, ?)",
                (task_ref, "s", f"noise-d-{i}", f"{_NOISE_PREFIX} decision {i}"),
            )

        # Target finding.
        cur = conn.execute(
            "INSERT INTO review_findings "
            "(task_ref, finding_id, severity, file_path, description, session, fix, resolution_notes) "
            "VALUES (?, ?, 'high', 'a.py', ?, 's', ?, ?)",
            (
                task_ref,
                "F-SMOKE-1",
                f"Missing check: {_FINDING_MARKER}",
                "validate inputs",
                "open",
            ),
        )
        finding_id = int(cur.lastrowid)

        for i in range(3):
            conn.execute(
                "INSERT INTO review_findings "
                "(task_ref, finding_id, severity, file_path, description, session, fix, resolution_notes) "
                "VALUES (?, ?, 'low', 'b.py', ?, 's', ?, ?)",
                (task_ref, f"F-NOISE-{i}", f"{_NOISE_PREFIX} finding {i}", "n/a", "open"),
            )

        # Target blocker.
        cur = conn.execute(
            "INSERT INTO blockers (task_ref, description) VALUES (?, ?)",
            (task_ref, f"Blocked on {_BLOCKER_MARKER}"),
        )
        blocker_id = int(cur.lastrowid)

        for i in range(2):
            conn.execute(
                "INSERT INTO blockers (task_ref, description) VALUES (?, ?)",
                (task_ref, f"{_NOISE_PREFIX} blocker {i}"),
            )

        # Concept embeddings for intersection kinds only (S0).
        # Target vectors use the *exact* marker string so the deterministic
        # one-hot FakeOneHotProvider yields cosine 1.0 against the smoke query
        # (identical text → identical unit vector). Source-row FTS bodies still
        # carry the marker via the surrounding prose for BM25 arm B.
        _insert_concept(
            conn,
            kind="decision.rationale",
            entity_id=str(decision_id),
            task_ref=task_ref,
            text=_DECISION_MARKER,
            provider=provider,
            created_at="2026-01-01 00:00:00",
        )
        _insert_concept(
            conn,
            kind="finding.description",
            entity_id=str(finding_id),
            task_ref=task_ref,
            text=_FINDING_MARKER,
            provider=provider,
            created_at="2026-01-01 00:00:01",
        )
        _insert_concept(
            conn,
            kind="finding.fix",
            entity_id=str(finding_id),
            task_ref=task_ref,
            text="validate inputs",
            provider=provider,
            created_at="2026-01-01 00:00:02",
        )
        _insert_concept(
            conn,
            kind="blocker.description",
            entity_id=str(blocker_id),
            task_ref=task_ref,
            text=_BLOCKER_MARKER,
            provider=provider,
            created_at="2026-01-01 00:00:03",
        )

        # Noise concepts (distinct one-hot dims via distinct text).
        noise_decisions = conn.execute(
            "SELECT id, rationale FROM decisions WHERE task_ref = ? AND id != ?",
            (task_ref, decision_id),
        ).fetchall()
        for row in noise_decisions:
            _insert_concept(
                conn,
                kind="decision.rationale",
                entity_id=str(row[0]),
                task_ref=task_ref,
                text=str(row[1]),
                provider=provider,
                created_at="2026-06-01 00:00:00",
            )

        conn.commit()

        # Bind smoke cases to real PKs (module-level cache for CLI/tests).
        global _BOUND_CASES  # noqa: PLW0603
        _BOUND_CASES = [
            {
                "query_id": "smoke-q-decision",
                "query": _DECISION_MARKER,
                "task_ref": task_ref,
                "relevant": [("decision", str(decision_id))],
            },
            {
                "query_id": "smoke-q-finding",
                "query": _FINDING_MARKER,
                "task_ref": task_ref,
                "relevant": [("finding", str(finding_id))],
            },
            {
                "query_id": "smoke-q-blocker",
                "query": _BLOCKER_MARKER,
                "task_ref": task_ref,
                "relevant": [("blocker", str(blocker_id))],
            },
        ]
        # Stash IDs for tests that need projection checks.
        global _BOUND_IDS  # noqa: PLW0603
        _BOUND_IDS = {
            "decision": str(decision_id),
            "finding": str(finding_id),
            "blocker": str(blocker_id),
        }

    return provider, task_ref


_BOUND_CASES: list[dict[str, Any]] = []
_BOUND_IDS: dict[str, str] = {}


def bound_smoke_cases() -> list[dict[str, Any]]:
    """Return cases bound by the last :func:`build_smoke_runtime_corpus` call."""
    if not _BOUND_CASES:
        raise RuntimeError("build_smoke_runtime_corpus must run before bound_smoke_cases()")
    # Strip private keys; return deep-ish copy of mappings.
    return [
        {
            "query_id": c["query_id"],
            "query": c["query"],
            "task_ref": c["task_ref"],
            "relevant": list(c["relevant"]),
        }
        for c in _BOUND_CASES
    ]


def bound_smoke_ids() -> dict[str, str]:
    if not _BOUND_IDS:
        raise RuntimeError("build_smoke_runtime_corpus must run before bound_smoke_ids()")
    return dict(_BOUND_IDS)


def smoke_case_mappings() -> list[dict[str, Any]]:
    """Public API: bound cases if seeded, else unbound template."""
    if _BOUND_CASES:
        return bound_smoke_cases()
    return [
        {
            "query_id": "smoke-q-decision",
            "query": _DECISION_MARKER,
            "task_ref": SMOKE_TASK_REF,
            "relevant": [],
        },
        {
            "query_id": "smoke-q-finding",
            "query": _FINDING_MARKER,
            "task_ref": SMOKE_TASK_REF,
            "relevant": [],
        },
        {
            "query_id": "smoke-q-blocker",
            "query": _BLOCKER_MARKER,
            "task_ref": SMOKE_TASK_REF,
            "relevant": [],
        },
    ]
