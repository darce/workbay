"""Hook event payload schemas (Schema #3 from founding implementation note).

Models the JSON Claude Code delivers to hook scripts on stdin. Each
event type is modeled separately rather than as a discriminated union
so individual hook scripts can validate against the precise event they
expect without parsing every variant.

``extra='allow'`` everywhere because Claude Code may add fields in
future releases and we don't want hooks to break on benign additions —
the contract guarantees the named fields, not exhaustive coverage.

Tag scheme: ``hook_event_name`` matches the literal string Claude Code
emits, so a hook script can do:

    raw = json.loads(sys.stdin.read())
    match raw["hook_event_name"]:
        case "PreToolUse":
            event = PreToolUseEvent.model_validate(raw)
        case ...
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _HookEventBase(BaseModel):
    model_config = ConfigDict(extra="allow")

    session_id: str = Field(description="Stable per-session identifier from Claude Code.")
    transcript_path: str | None = Field(
        default=None,
        description="Filesystem path to the active session transcript, when emitted by the harness.",
    )
    cwd: str | None = Field(
        default=None,
        description="Working directory of the Claude Code process at event time.",
    )


class SessionStartEvent(_HookEventBase):
    hook_event_name: Literal["SessionStart"] = "SessionStart"
    source: str | None = Field(
        default=None,
        description="Why the session started (e.g. 'startup', 'resume', 'compact').",
    )


class UserPromptSubmitEvent(_HookEventBase):
    hook_event_name: Literal["UserPromptSubmit"] = "UserPromptSubmit"
    prompt: str = Field(description="Raw text of the user's submitted prompt.")


class PreToolUseEvent(_HookEventBase):
    hook_event_name: Literal["PreToolUse"] = "PreToolUse"
    tool_name: str = Field(description="Tool the model is about to invoke.")
    tool_input: dict[str, Any] = Field(default_factory=dict, description="Arguments the model passed to the tool.")


class PostToolUseEvent(_HookEventBase):
    hook_event_name: Literal["PostToolUse"] = "PostToolUse"
    tool_name: str
    tool_input: dict[str, Any] = Field(default_factory=dict)
    tool_response: Any | None = Field(
        default=None,
        description="Tool result (shape varies by tool; passthrough so hooks can inspect freely).",
    )


class StopEvent(_HookEventBase):
    hook_event_name: Literal["Stop"] = "Stop"
    stop_hook_active: bool | None = Field(
        default=None,
        description="True when a previous Stop hook already ran for this turn.",
    )


_EVENT_TYPES: dict[str, type[_HookEventBase]] = {
    "SessionStart": SessionStartEvent,
    "UserPromptSubmit": UserPromptSubmitEvent,
    "PreToolUse": PreToolUseEvent,
    "PostToolUse": PostToolUseEvent,
    "Stop": StopEvent,
}


def parse_hook_event(payload: dict[str, Any]) -> _HookEventBase:
    """Validate ``payload`` against the matching hook-event schema.

    Hook scripts call this on the dict they get from
    ``json.loads(sys.stdin.read())`` to enforce the wire contract at
    the entrypoint. Raises ``ValueError`` for unknown event names and
    re-raises ``pydantic.ValidationError`` for shape failures.
    """
    name = payload.get("hook_event_name")
    if name not in _EVENT_TYPES:
        raise ValueError(f"unknown hook_event_name: {name!r}")
    return _EVENT_TYPES[name].model_validate(payload)
