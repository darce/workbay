"""Compaction contract schemas for cross-harness session summaries."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .handoff import TaskRef


class DecisionRef(BaseModel):
    """Stable decision pointer captured in a compaction summary."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str = Field(description="Stable decision identifier recorded in handoff.")
    slug: str = Field(description="Human-readable decision slug used for quick inspection.")


class TurnRange(BaseModel):
    """Inclusive turn range covered by the compaction summary."""

    model_config = ConfigDict(extra="forbid")

    start_turn: int = Field(ge=1, description="Inclusive 1-indexed first turn covered by the summary.")
    end_turn: int = Field(ge=1, description="Inclusive 1-indexed last turn covered by the summary.")

    @model_validator(mode="after")
    def _validate_bounds(self) -> "TurnRange":
        if self.end_turn < self.start_turn:
            raise ValueError("end_turn must be greater than or equal to start_turn")
        return self


class StructuredSummary(BaseModel):
    """Durable structured summary stored for cross-harness compaction."""

    model_config = ConfigDict(extra="forbid")

    compaction_id: str = Field(description="Server-generated stable compaction handle.")
    session_id: str = Field(description="Harness session identifier for the compacted transcript span.")
    harness: Literal["claude-code", "codex", "grok", "vscode", "manual"] = Field(
        description="Harness that produced the compaction record."
    )
    task_ref: TaskRef
    turn_range: TurnRange
    decisions: list[DecisionRef] = Field(default_factory=list)
    findings_fixed: list[str] = Field(default_factory=list)
    findings_opened: list[str] = Field(default_factory=list)
    tests_verified: list[str] = Field(default_factory=list)
    files_touched: list[str] = Field(default_factory=list)
    prose_residual: str | None = Field(
        default=None,
        description="Narrative spans that the extractor could not resolve to structured IDs.",
    )
    created_at: datetime = Field(description="Timestamp when the compaction row was created.")

    @field_validator("harness", mode="before")
    @classmethod
    def _normalize_harness(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        normalized = value.strip().lower()
        if normalized == "cursor":
            return "vscode"
        return normalized
