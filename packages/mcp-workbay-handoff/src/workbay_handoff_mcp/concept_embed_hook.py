"""Numpy-free post-commit embed-on-write hook (internal).

The concept-embedding store lives in the optional ``embeddings`` subpackage
(numpy / onnxruntime / tokenizers). Core write paths import THIS shim — which has
no heavy imports at module load — and call :func:`embed_concept_on_write` AFTER
their row has committed. The store is imported lazily; when the optional extra is
absent the call is a silent no-op, so the default server path stays numpy-free
and byte-identical to today.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from workbay_handoff_mcp.embeddings.store import AnchorStoreOutcome, ConceptEmbedOutcome

# Re-export the outcome Literals for callers that type-check against the hook
# surface without importing the optional embeddings subpackage at runtime.
# (Runtime values are plain strings; the Literal types live in store.py.)
try:
    from workbay_handoff_mcp.embeddings.store import (  # noqa: F401
        AnchorStoreOutcome,
        ConceptEmbedOutcome,
    )
except ImportError:  # optional embeddings extra absent
    from typing import Literal

    ConceptEmbedOutcome = Literal[  # type: ignore[misc,assignment]
        "stored",
        "skipped",
        "skipped_no_provider",
        "skipped_empty",
        "failed",
    ]
    AnchorStoreOutcome = Literal[  # type: ignore[misc,assignment]
        "stored",
        "skipped_no_provider",
        "skipped_empty",
        "failed",
    ]

_log = logging.getLogger("workbay_handoff_mcp")


def embed_concept_on_write(
    entity_kind: str, entity_id: object, task_ref: str, text: str | None
) -> ConceptEmbedOutcome:
    """Best-effort: embed + store one concept after its row committed. Never raises.

    Must be called *after* the concept row's own write transaction has committed
    — the store opens its own connection, so calling it mid-transaction would
    contend with the open write lock. A missing embeddings extra (numpy) makes
    this a no-op; gaps are reconciled by the resumable backfill.

    Returns a :data:`~workbay_handoff_mcp.embeddings.store.ConceptEmbedOutcome`
    (``stored`` / ``skipped`` / ``skipped_no_provider`` / ``skipped_empty`` /
    ``failed``). Callers that surface partial failure (e.g. ``close_slice``)
    must treat ``failed`` as a typed signal — production failures are swallowed
    inside the store and never re-raised. Callers that cannot act on the
    outcome still get a WARNING log here plus the store's
    ``agent_errors.embedding_failed`` telemetry so a provider outage is not
    silent ([DATA-13], [OBS-08]).
    """
    try:
        from .embeddings.store import embed_concept_best_effort
    except ImportError:
        return "skipped_no_provider"  # optional embeddings extra absent -> semantic feature off
    outcome = embed_concept_best_effort(entity_kind, entity_id, task_ref, text)
    if outcome == "failed":
        # Surface for every consumer of this public hook so discard-style
        # call sites still leave an observable trace ([DATA-13], [OBS-08]).
        # close_slice additionally promotes failed into partial + side_effect_failures.
        _log.warning(
            "embed-on-write returned failed for %s/%s task_ref=%s",
            entity_kind,
            entity_id,
            task_ref,
        )
    return outcome


def embed_compaction_anchor_on_write(
    compaction_id: object, task_ref: str, text: str | None
) -> AnchorStoreOutcome:
    """Best-effort: embed transcript text + persist it as the compaction's anchor_vector.

    internal. Called *after* the ``session_compactions`` row committed;
    the store opens its own connection. A missing embeddings extra (numpy) makes
    this a no-op (``anchor_vector`` stays NULL -> reinjection degrades to today's
    selection); gaps are reconciled by the resumable backfill.

    Returns an :data:`~workbay_handoff_mcp.embeddings.store.AnchorStoreOutcome`
    so callers can observe store/skip/fail rather than discarding the typed
    result ([ARCH-13], [OBS-08]). The store already logs WARNING + increments
    process-local counters on failure.
    """
    try:
        from .embeddings.store import store_compaction_anchor_best_effort
    except ImportError:
        return "skipped_no_provider"  # optional embeddings extra absent -> semantic feature off
    return store_compaction_anchor_best_effort(compaction_id, task_ref, text)


def _as_text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def embed_finding_concepts(finding: dict[str, object] | None) -> list[ConceptEmbedOutcome]:
    """Embed a finding row's description/fix/resolution_notes after it committed.

    Best-effort; ``None``/blank fields are no-ops. ``finding`` is a row dict as
    returned in a review-finding envelope (``data["finding"]``).

    Returns the per-field :data:`ConceptEmbedOutcome` list (empty when
    ``finding`` is missing) so callers can inspect ``failed`` rather than
    discarding the typed contract ([DATA-13]). Each ``failed`` is also surfaced
    via :func:`embed_concept_on_write`'s WARNING + agent_errors telemetry.
    """
    if not finding:
        return []
    entity_id = finding.get("id")
    task_ref = str(finding.get("task_ref"))
    return [
        embed_concept_on_write(
            "finding.description", entity_id, task_ref, _as_text(finding.get("description"))
        ),
        embed_concept_on_write("finding.fix", entity_id, task_ref, _as_text(finding.get("fix"))),
        embed_concept_on_write(
            "finding.resolution_notes",
            entity_id,
            task_ref,
            _as_text(finding.get("resolution_notes")),
        ),
    ]


def embed_finding_from_envelope(result: dict[str, object]) -> list[ConceptEmbedOutcome]:
    """Embed the single finding carried in a review-finding envelope's ``data["finding"]``.

    Returns the outcomes from :func:`embed_finding_concepts` (empty list when
    the envelope is not ok or has no finding) so discard-style callers still
    leave the typed path observed at the hook layer ([DATA-13]).
    """
    if not result.get("ok"):
        return []
    data = result.get("data")
    finding = data.get("finding") if isinstance(data, dict) else None
    return embed_finding_concepts(finding if isinstance(finding, dict) else None)
