"""Skill manifest schema (Schema #4 from founding implementation note).

Structured contract for the canonical neutral skill layout
``skills/<slug>/skill.yaml`` (implementation note step 1). The pre-Plan-0002
``.claude/skills/<slug>/SKILL.md`` frontmatter is now a *generated*
artifact in target repos and is no longer a source-of-truth surface.
Enforces the Tier 1 / Tier 2 / Tier 3 portability boundary by
requiring an explicit ``scope`` declaration on every skill —
preventing project-specific skills from being accidentally hoisted
into the harness overlay.

``extra='allow'`` so existing harness fields (``mode``, ``context_budget``,
``mcp_tools``, ``tdd_gate``, ``makefile_target``, etc.) round-trip
unchanged. The schema only enforces the cross-repo portability
contract; consumer-specific validators continue to enforce their own
fields locally.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class SkillScope(str, Enum):
    """Portability scope of a skill.

    ``harness``: project-agnostic. Lives in ``workbay-system`` source of
                 truth and is installed/symlinked into target repos by
                 ``workbay-bootstrap``. Must not reference project-specific
                 paths, package names, or domain terminology.
    ``project``: project-specific. Stays in the target repo's
                 ``skills/`` (or its generated ``.claude/skills/``
                 mirror) and is never hoisted by bootstrap. May freely
                 reference project-local apps, packages, and
                 conventions.
    """

    harness = "harness"
    project = "project"


class SkillManifest(BaseModel):
    """Required cross-repo fields on a skill's frontmatter."""

    model_config = ConfigDict(extra="allow")

    name: str = Field(min_length=1, description="Unique skill identifier (kebab-case).")
    description: str = Field(min_length=1, description="One-line description used for skill matching.")
    scope: SkillScope = Field(
        description="Portability scope. 'harness' = installed into target repos by bootstrap; 'project' = stays local.",
    )
    triggers: list[str] | None = Field(
        default=None,
        description="Optional list of trigger phrases or patterns the skill responds to.",
    )
    model: str | None = Field(
        default=None,
        description="Optional preferred model identifier (e.g. 'claude-opus-4-7').",
    )
