"""Pydantic SSOT for .uxmap.json (internal).

Hard vs soft invariants
-----------------------
**Hard (model ValidationError):**
- required fields (e.g. ``map_ref``)
- enum membership for kinds/roles/states/hierarchy
- unique ids within ``jobs``, ``screens``, ``flows``, ``actions``
- unique zone ids within each screen
- ``extra="forbid"`` (typos on S3 fields are rejected, not dropped)

**Soft (critique / S3, not model-enforced):**
- dangling ``FlowStep.screen_id`` / ``Action.screen_id`` / ``primary_action_id``
- ``Action.costly|irreversible`` without ``preview_required`` ([INT-07] critique)
- zone role rules (``evidence_linked``, ``max_candidates``) for [HAI-01]/[HAI-16]
- screen state inventory completeness ([RLSE-04])
"""

from __future__ import annotations

from enum import Enum
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)


class MapState(str, Enum):
    """Per-screen/zone states aligned to engineering [RLSE-04]."""

    DEFAULT = "default"
    LOADING = "loading"
    EMPTY = "empty"
    ERROR = "error"
    OFFLINE = "offline"
    FIRST_TIME = "first_time"
    EDGE_INPUT = "edge_input"
    DEGRADED = "degraded"


class ScreenKind(str, Enum):
    SCREEN = "screen"
    OVERLAY = "overlay"
    EXIT = "exit"


class ZoneRole(str, Enum):
    CONTENT = "content"
    NAV = "nav"
    STATUS = "status"
    QUEUE = "queue"
    JOB = "job"
    AI_REVIEW = "ai_review"
    FORCED_CHOICE = "forced_choice"
    FORM = "form"
    OTHER = "other"


class ActionHierarchy(str, Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    TERTIARY = "tertiary"
    DESTRUCTIVE = "destructive"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Job(_StrictModel):
    id: str
    label: str


# TODO(internal): Regenerate ux_map/schema/uxmap.schema.json when that
# derived artifact is admitted to the lane write-set.
class Zone(_StrictModel):
    id: str
    label: str
    role: ZoneRole
    code_ref: str | None = None
    evidence_linked: bool | None = None
    max_candidates: int | None = None
    states: list[MapState] = Field(default_factory=list)


class Screen(_StrictModel):
    id: str
    kind: ScreenKind
    title: str
    purpose: str
    route: str
    wp_page: str | None = None
    job_entry: bool = False
    deep_link: bool = True
    url_params: list[str] = Field(default_factory=list)
    zones: list[Zone] = Field(default_factory=list)
    states: list[MapState] = Field(default_factory=list)
    primary_action_id: str | None = None
    code_ref: str | None = None

    @model_validator(mode="after")
    def _unique_zone_ids(self) -> Self:
        ids = [z.id for z in self.zones]
        if len(ids) != len(set(ids)):
            raise ValueError(f"duplicate zone ids on screen {self.id!r}: {ids}")
        return self


class FlowStep(_StrictModel):
    screen_id: str
    branch_label: str | None = None


class Flow(_StrictModel):
    id: str
    job: str
    steps: list[FlowStep] = Field(default_factory=list)
    label: str | None = None


class Action(_StrictModel):
    """Action row.

    ``costly`` / ``irreversible`` without ``preview_required`` is **allowed** here;
    [INT-07] is enforced by the S3 critique pack, not this model.
    """

    id: str
    verb: str
    target: str
    hierarchy: ActionHierarchy
    costly: bool = False
    irreversible: bool = False
    preview_required: bool = False
    screen_id: str | None = None
    code_ref: str | None = None


def _unique(ids: list[str], label: str) -> None:
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate {label} ids: {ids}")


class UxMap(_StrictModel):
    """Structured UX map — jobs, screens, zones, states, flows, actions."""

    schema_version: Literal[1] = 1
    map_ref: str
    product: str
    source_fixture: str | None = None
    goals: list[str] = Field(default_factory=list)
    jobs: list[Job] = Field(default_factory=list)
    screens: list[Screen] = Field(default_factory=list)
    flows: list[Flow] = Field(default_factory=list)
    actions: list[Action] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    not_doing: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_collection_ids(self) -> Self:
        _unique([j.id for j in self.jobs], "job")
        _unique([s.id for s in self.screens], "screen")
        _unique([f.id for f in self.flows], "flow")
        _unique([a.id for a in self.actions], "action")
        return self

    def screen_by_id(self, screen_id: str) -> Screen | None:
        for s in self.screens:
            if s.id == screen_id:
                return s
        return None

    def action_by_id(self, action_id: str) -> Action | None:
        for a in self.actions:
            if a.id == action_id:
                return a
        return None
