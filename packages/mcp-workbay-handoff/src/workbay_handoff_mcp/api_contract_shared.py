from __future__ import annotations

from typing import Annotated, Any, cast

from pydantic import BaseModel, Field

from .shared_write_context import WriteActor


class WriteActorInput(BaseModel):
    agent: Annotated[
        str | None,
        Field(
            description="Optional stable agent identity override. Omit to derive it from model metadata when available."
        ),
    ] = None
    model: Annotated[
        str | None,
        Field(description="Canonical model slug for the writing agent, for example 'gpt-5.4'."),
    ] = None
    model_label: Annotated[
        str | None,
        Field(description="Canonical human-readable label for model; must match the normalized label for actor.model."),
    ] = None
    reasoning_level: Annotated[
        str | None,
        Field(description="Reasoning effort label used to derive agent identity and provenance."),
    ] = None
    branch: Annotated[
        str | None,
        Field(description="Git branch override for the write provenance."),
    ] = None
    commit_sha: Annotated[
        str | None,
        Field(description="Git commit SHA override for the write provenance."),
    ] = None
    lane_id: Annotated[
        str | None,
        Field(description="Optional worktree lane identifier for the write provenance."),
    ] = None


TaskRefParam = Annotated[
    str | None,
    Field(description="Optional task reference override. When omitted, the active task is used."),
]

ActorParam = Annotated[
    WriteActorInput | None,
    Field(description="Optional structured provenance override for the write operation."),
]

DecisionChangedFilesParam = Annotated[
    list[str] | None,
    Field(description="Optional monorepo-relative paths touched by this slice."),
]


def dump_actor(actor: WriteActorInput | dict[str, Any] | None) -> WriteActor | None:
    if actor is None:
        return None
    actor_model = WriteActorInput.model_validate(actor) if isinstance(actor, dict) else actor
    return cast(
        WriteActor,
        {key: value for key, value in actor_model.model_dump(exclude_none=True).items() if isinstance(value, str)},
    )
