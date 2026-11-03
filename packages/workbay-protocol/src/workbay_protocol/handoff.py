"""Handoff state schemas (Schema #1 from founding implementation note).

These are the cross-repo contract types for handoff state. They model
the wire shape that ``mcp-workbay-handoff`` exposes to MCP clients and
that ``mcp-workbay-orchestrator`` consumes — not the on-disk SQLite row
shape, which is an implementation detail.

Compatibility note: ``ActiveTask`` is permissive on extra keys
(``extra='allow'``) because the underlying handoff DB row carries
columns that this schema does not yet model (revision metadata, lane
metadata, etc.). As subsequent schema slices land, those fields
graduate from passthrough to typed.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

# ---------------------------------------------------------------------------
# Primitive identifiers and enums
# ---------------------------------------------------------------------------

# A task_ref is a short identifier like "internal" or "internal" or "AAA-1".
# We require non-empty string with a permissive character class so domain
# tools can introduce new prefixes without re-releasing the protocol.
TaskRef = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=128),
    Field(description="Short task identifier, e.g. 'internal' or 'internal'."),
]


class HandoffStatus(str, Enum):
    """Lifecycle status of a handoff row.

    Mirrors the CHECK constraint on ``handoff_state.status`` in
    ``mcp-workbay-handoff`` (``shared_schema.py``).
    """

    in_progress = "in_progress"
    blocked = "blocked"
    review = "review"
    done = "done"


class TaskPlanResolution(str, Enum):
    """How an absolute task-plan path was resolved at read time.

    ``absolute``: the configured ``task_plan_path`` was already absolute.
    ``worktree``: resolved against ``target_worktree_path``.
    ``workspace``: resolved against the configured workspace root because
                   no ``target_worktree_path`` was set.
    ``unresolved``: resolution failed (no worktree, no workspace).
    """

    absolute = "absolute"
    worktree = "worktree"
    workspace = "workspace"
    unresolved = "unresolved"


# ---------------------------------------------------------------------------
# Composite models
# ---------------------------------------------------------------------------


class TargetWorktree(BaseModel):
    """The branch + filesystem location where a task's work lives."""

    model_config = ConfigDict(extra="forbid")

    target_branch: str | None = Field(
        default=None,
        description="Git branch the task targets (e.g. 'feature/e17-14').",
    )
    target_worktree_path: str | None = Field(
        default=None,
        description="Absolute path to the worktree where the task's branch is checked out.",
    )


class TaskPlanRef(BaseModel):
    """Structured pointer to a task's planning artifact.

    Replaces inferring plan paths from the freeform ``focus`` field.
    The triple (path, abs_path, exists, resolution) is computed by the
    handoff runtime against ``target_worktree_path`` and surfaced on
    every active row so the root workspace can discover and open plans
    living in sibling worktrees without switching the root checkout.
    """

    model_config = ConfigDict(extra="forbid")

    task_plan_path: str = Field(
        description="Repo-relative path (or absolute path) to the planning artifact."
    )
    task_plan_abs_path: str | None = Field(
        default=None,
        description="Absolute path resolved against the target worktree.",
    )
    task_plan_exists: bool | None = Field(
        default=None,
        description="Whether the resolved absolute path is a regular file at read time.",
    )
    task_plan_resolution: TaskPlanResolution | None = Field(
        default=None,
        description="How task_plan_abs_path was derived.",
    )


class ActiveTask(BaseModel):
    """Wire shape for a single active handoff row.

    Carries identifiers, status, optional target worktree, optional
    structured task-plan reference, and a revision counter used for
    optimistic concurrency control. ``extra='allow'`` lets older or
    newer columns (lane metadata, audit fields) round-trip without
    requiring a coordinated bump of every consumer.
    """

    model_config = ConfigDict(extra="allow")

    task_ref: TaskRef
    objective: str = Field(description="Human-readable task objective.")
    focus: str | None = Field(
        default=None,
        description="Freeform context for the current iteration. Not a plan-path source — see task_plan_path.",
    )
    status: HandoffStatus = HandoffStatus.in_progress
    revision: int = Field(default=0, ge=0, description="Optimistic-concurrency counter.")

    target_branch: str | None = None
    target_worktree_path: str | None = None

    # Flat task-plan fields rather than a nested object: matches the wire
    # shape produced by _enrich_handoff_active in the handoff runtime,
    # so existing dict consumers don't have to change keys.
    task_plan_path: str | None = None
    task_plan_abs_path: str | None = None
    task_plan_exists: bool | None = None
    task_plan_resolution: TaskPlanResolution | None = None

    def target_worktree(self) -> TargetWorktree:
        return TargetWorktree(
            target_branch=self.target_branch,
            target_worktree_path=self.target_worktree_path,
        )

    def task_plan(self) -> TaskPlanRef | None:
        if self.task_plan_path is None:
            return None
        return TaskPlanRef(
            task_plan_path=self.task_plan_path,
            task_plan_abs_path=self.task_plan_abs_path,
            task_plan_exists=self.task_plan_exists,
            task_plan_resolution=self.task_plan_resolution,
        )


class HandoffState(BaseModel):
    """Top-level handoff state surface.

    A list of active tasks plus a pointer to which one resolves to the
    current workspace context (``active_task_ref``). The single-active
    fallback (``active``) is kept for callers that still expect the
    legacy single-task envelope; it equals the entry whose ``task_ref``
    matches ``active_task_ref`` when present.
    """

    model_config = ConfigDict(extra="allow")

    active_tasks: list[ActiveTask] = Field(default_factory=list)
    active_task_ref: TaskRef | None = None
    active: ActiveTask | None = None

    @classmethod
    def from_identity_envelope(cls, envelope: dict) -> "HandoffState":
        """Adapter from the legacy ``get_handoff_state`` MCP envelope.

        Accepts the shape ``{"ok": True, "tool": ..., "task_ref": ...,
        "data": {"active": {...}, ...}}`` and reshapes it to a
        single-active ``HandoffState``. Multi-active variants will be
        added when the runtime exposes them.

        Identity consistency: when both the outer ``envelope['task_ref']``
        and the inner ``data.active.task_ref`` are present and differ,
        this raises ``ValueError`` rather than silently picking one.
        That prevents the adapter from constructing a logically
        inconsistent ``HandoffState`` whose ``active_task_ref`` does
        not point at the only active task.

        Use this in tests and consumer-side validation where you have
        an envelope and want to enforce the cross-repo top-level shape
        without reshaping the runtime first.
        """
        if not isinstance(envelope, dict):
            raise TypeError(f"envelope must be a dict, got {type(envelope).__name__}")
        data = envelope.get("data") or {}
        if not isinstance(data, dict):
            raise TypeError("envelope['data'] must be a dict")
        active_raw = data.get("active")
        active = ActiveTask.model_validate(active_raw) if isinstance(active_raw, dict) else None

        outer_ref = envelope.get("task_ref")
        inner_ref = active.task_ref if active is not None else None
        if outer_ref and inner_ref and outer_ref != inner_ref:
            raise ValueError(
                f"envelope identity mismatch: envelope['task_ref']={outer_ref!r} "
                f"but data.active.task_ref={inner_ref!r}. The handoff envelope "
                "must be self-consistent before adapting to HandoffState."
            )
        active_task_ref = inner_ref or outer_ref
        return cls(
            active=active,
            active_task_ref=active_task_ref,
            active_tasks=[active] if active is not None else [],
        )
