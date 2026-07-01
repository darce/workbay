"""Numpy-free post-commit embed-on-write hook (internal).

The concept-embedding store lives in the optional ``embeddings`` subpackage
(numpy / onnxruntime / tokenizers). Core write paths import THIS shim — which has
no heavy imports at module load — and call :func:`embed_concept_on_write` AFTER
their row has committed. The store is imported lazily; when the optional extra is
absent the call is a silent no-op, so the default server path stays numpy-free
and byte-identical to today.
"""

from __future__ import annotations


def embed_concept_on_write(entity_kind: str, entity_id: object, task_ref: str, text: str | None) -> None:
    """Best-effort: embed + store one concept after its row committed. Never raises.

    Must be called *after* the concept row's own write transaction has committed
    — the store opens its own connection, so calling it mid-transaction would
    contend with the open write lock. A missing embeddings extra (numpy) makes
    this a no-op; gaps are reconciled by the resumable backfill.
    """
    try:
        from .embeddings.store import embed_concept_best_effort
    except ImportError:
        return  # optional embeddings extra absent -> semantic feature off
    embed_concept_best_effort(entity_kind, entity_id, task_ref, text)


def embed_compaction_anchor_on_write(compaction_id: object, task_ref: str, text: str | None) -> None:
    """Best-effort: embed transcript text + persist it as the compaction's anchor_vector.

    internal. Called *after* the ``session_compactions`` row committed;
    the store opens its own connection. A missing embeddings extra (numpy) makes
    this a no-op (``anchor_vector`` stays NULL -> reinjection degrades to today's
    selection); gaps are reconciled by the resumable backfill.
    """
    try:
        from .embeddings.store import store_compaction_anchor_best_effort
    except ImportError:
        return  # optional embeddings extra absent -> semantic feature off
    store_compaction_anchor_best_effort(compaction_id, task_ref, text)


def _as_text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def embed_finding_concepts(finding: dict[str, object] | None) -> None:
    """Embed a finding row's description/fix/resolution_notes after it committed.

    Best-effort; ``None``/blank fields are no-ops. ``finding`` is a row dict as
    returned in a review-finding envelope (``data["finding"]``).
    """
    if not finding:
        return
    entity_id = finding.get("id")
    task_ref = str(finding.get("task_ref"))
    embed_concept_on_write("finding.description", entity_id, task_ref, _as_text(finding.get("description")))
    embed_concept_on_write("finding.fix", entity_id, task_ref, _as_text(finding.get("fix")))
    embed_concept_on_write("finding.resolution_notes", entity_id, task_ref, _as_text(finding.get("resolution_notes")))


def embed_finding_from_envelope(result: dict[str, object]) -> None:
    """Embed the single finding carried in a review-finding envelope's ``data["finding"]``."""
    if not result.get("ok"):
        return
    data = result.get("data")
    finding = data.get("finding") if isinstance(data, dict) else None
    embed_finding_concepts(finding if isinstance(finding, dict) else None)
