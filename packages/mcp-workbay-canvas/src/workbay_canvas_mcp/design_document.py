"""Design-document models and pure render/patch helpers (internal).

The design document is the durable, per-``design_ref`` design record that
gives successive canvas iterations memory: goals, audience, design
tokens, links into the component roster, layout decisions, scaffold
targets, and open questions. It is the artifact a future step renders to
Markdown for docs or reads to scaffold static content — without turning
the workbay canvas into a code generator.

This module is pure: :func:`apply_patch`, :func:`append_decision`,
:func:`render_markdown`, and :func:`render_json` never touch I/O.
:class:`~workbay_canvas_mcp.store.CanvasStore` owns persistence
(full-replace ``upsert_design_document`` / ``get_design_document``) and
the ``manage_design_document`` MCP tool owns the operation surface,
orchestrating read -> pure transform -> write.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class DesignDecision(BaseModel):
    """One recorded layout/design decision within a design document.

    ``decision_id`` and ``created_at`` are assigned by
    :func:`append_decision` when a caller does not supply them, so the
    append surface stays a single call.
    """

    decision_id: str
    summary: str
    rationale: str | None = None
    created_at: str


class DesignDocument(BaseModel):
    """The durable design record for a single ``design_ref``.

    Every collection defaults empty so an unseeded document is a valid
    starting point. ``layout_decisions`` is append-only via
    :func:`append_decision`; the other fields are replaced per-field by
    :func:`apply_patch`.
    """

    design_ref: str
    title: str | None = None
    goals: list[str] = Field(default_factory=list)
    audience: list[str] = Field(default_factory=list)
    tokens: dict[str, Any] = Field(default_factory=dict)
    component_refs: list[str] = Field(default_factory=list)
    layout_decisions: list[DesignDecision] = Field(default_factory=list)
    scaffold_targets: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    not_doing: list[str] = Field(default_factory=list)


class DesignDocumentReceipt(BaseModel):
    """Result of a ``manage_design_document`` call.

    ``document`` always reflects the post-operation state. ``rendered``
    and ``format`` are populated only for ``operation="render"``.
    """

    operation: str
    design_ref: str
    document: DesignDocument
    rendered: str | None = None
    format: str | None = None


# Fields a caller may set through ``apply_patch``. ``design_ref`` is the
# key and ``layout_decisions`` is owned by :func:`append_decision`, so
# neither is patchable here.
_PATCHABLE_FIELDS = frozenset(
    {
        "title",
        "goals",
        "audience",
        "tokens",
        "component_refs",
        "scaffold_targets",
        "open_questions",
        "not_doing",
    }
)


def apply_patch(document: DesignDocument, patch: dict[str, Any]) -> DesignDocument:
    """Return a copy of ``document`` with ``patch`` fields replaced.

    Last-write-wins per field; absent fields are left untouched. A patch
    key outside :data:`_PATCHABLE_FIELDS` (including ``design_ref`` and
    ``layout_decisions``) raises ``ValueError`` so typos and
    wrong-surface writes fail loudly instead of silently no-op'ing.
    """

    unknown = set(patch) - _PATCHABLE_FIELDS
    if unknown:
        raise ValueError(f"unpatchable design-document field(s): {sorted(unknown)}")
    return DesignDocument.model_validate({**document.model_dump(), **patch})


def append_decision(
    document: DesignDocument,
    *,
    summary: str,
    rationale: str | None = None,
    decision_id: str | None = None,
    created_at: str | None = None,
) -> DesignDocument:
    """Return a copy of ``document`` with one decision appended in order."""

    decision = DesignDecision(
        decision_id=decision_id or str(uuid.uuid4()),
        summary=summary,
        rationale=rationale,
        created_at=created_at or datetime.now(UTC).isoformat(),
    )
    return document.model_copy(update={"layout_decisions": [*document.layout_decisions, decision]})


def render_json(document: DesignDocument) -> str:
    """Render the document as indented, stable JSON text."""

    return document.model_dump_json(indent=2)


def render_markdown(document: DesignDocument) -> str:
    """Render the document as deterministic Markdown.

    Sections render in a fixed order; empty sections are omitted so the
    output stays readable, but the title line is always present (falling
    back to the ``design_ref`` when untitled).
    """

    lines: list[str] = [f"# {document.title or document.design_ref}", ""]

    def _bullet_section(heading: str, items: list[str]) -> None:
        if not items:
            return
        lines.append(f"## {heading}")
        lines.extend(f"- {item}" for item in items)
        lines.append("")

    _bullet_section("Goals", document.goals)
    _bullet_section("Audience", document.audience)

    if document.tokens:
        lines.append("## Design Tokens")
        lines.extend(f"- {key}: {value}" for key, value in document.tokens.items())
        lines.append("")

    _bullet_section("Components", document.component_refs)

    if document.layout_decisions:
        lines.append("## Layout Decisions")
        for decision in document.layout_decisions:
            if decision.rationale:
                lines.append(f"- {decision.summary} — {decision.rationale}")
            else:
                lines.append(f"- {decision.summary}")
        lines.append("")

    _bullet_section("Scaffold Targets", document.scaffold_targets)
    _bullet_section("Open Questions", document.open_questions)
    _bullet_section("Not Doing", document.not_doing)

    return "\n".join(lines).rstrip() + "\n"
