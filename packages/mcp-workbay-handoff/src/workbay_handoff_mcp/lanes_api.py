"""Public worktree lane port surface."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field

from .api_contract_shared import ActorParam, TaskRefParam, dump_actor
from .lanes_recording import close_lane as _close_lane
from .lanes_recording import get_lane as _get_lane
from .lanes_recording import latest_lane_landing as _latest_lane_landing
from .lanes_recording import list_lanes as _list_lanes
from .lanes_recording import open_lane as _open_lane
from .lanes_recording import update_lane as _update_lane

LaneStatus = Literal["planned", "active", "blocked", "review", "merged", "closed"]
CloseLaneStatus = Literal["closed", "merged"]


class LanesOpenOp(BaseModel):
    operation: Literal["open"]
    lane_id: Annotated[str, Field(description="Stable lane identifier within the task.")]
    worktree_path: Annotated[str, Field(description="Absolute path to the lane worktree.")]
    branch: Annotated[str, Field(description="Git branch checked out in the lane worktree.")]
    title: Annotated[str | None, Field(description="Optional lane title.")] = None
    objective: Annotated[str | None, Field(description="Optional lane objective.")] = None
    owner_agent: Annotated[str | None, Field(description="Optional owning agent label.")] = None
    model: Annotated[str | None, Field(description="Optional model slug.")] = None
    backend: Annotated[str | None, Field(description="Optional backend label.")] = None
    reasoning_effort: Annotated[str | None, Field(description="Optional reasoning effort.")] = None
    status: LaneStatus = "planned"
    notes: Annotated[str | None, Field(description="Optional free-form notes.")] = None
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class LanesUpdateOp(BaseModel):
    operation: Literal["update"]
    lane_id: Annotated[str, Field(description="Lane identifier to update.")]
    title: Annotated[str | None, Field(description="Optional lane title.")] = None
    objective: Annotated[str | None, Field(description="Optional lane objective.")] = None
    worktree_path: Annotated[str | None, Field(description="Optional worktree path.")] = None
    branch: Annotated[str | None, Field(description="Optional branch name.")] = None
    owner_agent: Annotated[str | None, Field(description="Optional owning agent label.")] = None
    model: Annotated[str | None, Field(description="Optional model slug.")] = None
    backend: Annotated[str | None, Field(description="Optional backend label.")] = None
    reasoning_effort: Annotated[str | None, Field(description="Optional reasoning effort.")] = None
    status: LaneStatus | None = None
    notes: Annotated[str | None, Field(description="Optional free-form notes.")] = None
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class LanesCloseOp(BaseModel):
    operation: Literal["close"]
    lane_id: Annotated[str, Field(description="Lane identifier to close.")]
    status: CloseLaneStatus = "closed"
    notes: Annotated[str | None, Field(description="Optional close notes.")] = None
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class LanesGetOp(BaseModel):
    operation: Literal["get"]
    lane_id: Annotated[str, Field(description="Lane identifier to fetch.")]
    task_ref: TaskRefParam = None


class LanesListOp(BaseModel):
    operation: Literal["list"]
    task_ref: TaskRefParam = None
    status: Annotated[str, Field(description="Lane status filter or 'all'.")] = "all"
    limit: int = 100
    offset: int = 0


LanesOp = LanesOpenOp | LanesUpdateOp | LanesCloseOp | LanesGetOp | LanesListOp


def dispatch_lanes(payload: LanesOp) -> dict:
    actor = dump_actor(payload.actor) if hasattr(payload, "actor") else None
    if isinstance(payload, LanesOpenOp):
        return _open_lane(
            lane_id=payload.lane_id,
            worktree_path=payload.worktree_path,
            branch=payload.branch,
            title=payload.title,
            objective=payload.objective,
            owner_agent=payload.owner_agent,
            model=payload.model,
            backend=payload.backend,
            reasoning_effort=payload.reasoning_effort,
            status=payload.status,
            notes=payload.notes,
            task_ref=payload.task_ref,
            actor=actor,
        )
    if isinstance(payload, LanesUpdateOp):
        return _update_lane(
            lane_id=payload.lane_id,
            title=payload.title,
            objective=payload.objective,
            worktree_path=payload.worktree_path,
            branch=payload.branch,
            owner_agent=payload.owner_agent,
            model=payload.model,
            backend=payload.backend,
            reasoning_effort=payload.reasoning_effort,
            status=payload.status,
            notes=payload.notes,
            task_ref=payload.task_ref,
            actor=actor,
        )
    if isinstance(payload, LanesCloseOp):
        return _close_lane(
            lane_id=payload.lane_id,
            status=payload.status,
            notes=payload.notes,
            task_ref=payload.task_ref,
            actor=actor,
        )
    if isinstance(payload, LanesGetOp):
        return _get_lane(lane_id=payload.lane_id, task_ref=payload.task_ref)
    return _list_lanes(
        task_ref=payload.task_ref,
        status=payload.status,
        limit=payload.limit,
        offset=payload.offset,
    )


# Direct public callables for cross-package contract tests.
open_lane = _open_lane
update_lane = _update_lane
close_lane = _close_lane
get_lane = _get_lane
list_lanes = _list_lanes
latest_lane_landing = _latest_lane_landing
