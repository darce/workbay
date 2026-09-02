from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Protocol

# Adapter-owned evidence. Worker documents enter through from_dict and cannot
# mint these keys; adapters stamp them after from_dict or construct
# BackendResult directly with their own raw_payload.
_ADAPTER_OWNED_RAW_PAYLOAD_KEYS = frozenset(
    {
        "receiver_num_turns",
        "phase_timing",
        "transport_failure",
        "rate_limited",
        "admission_deferred",
    }
)


_HANDOFF_ACTION_SOURCES = frozenset({"defaulted", "explicit"})
_VALID_HANDOFF_ACTIONS = frozenset({"merge_ready", "needs_guidance"})


def _handoff_action_is_blank(raw_action: Any) -> bool:
    """True when the action is absent-equivalent (None, empty, or whitespace)."""
    return raw_action is None or not str(raw_action).strip()


def _handoff_action_is_valid_enum(raw_action: Any) -> bool:
    return isinstance(raw_action, str) and raw_action in _VALID_HANDOFF_ACTIONS


def handoff_action_needs_clamp(payload: dict[str, Any]) -> bool:
    """Return True when the caller must fail-closed-clamp ``handoff_action``.

    Absent, null, empty, and whitespace-only values are defaulted by
    ``BackendResult.from_dict`` and must not grow an
    ``invalid_handoff_action`` blocker. An unshaped recovery stamp is judged
    the same way as a raw payload: only a present non-enum value is invalid.
    """
    if "handoff_action" not in payload:
        return False
    action = payload.get("handoff_action")
    if _handoff_action_is_blank(action):
        return False
    return not _handoff_action_is_valid_enum(action)


@dataclass(frozen=True)
class BackendResult:
    """Standardized result from an execution backend."""

    handoff_action: str
    summary: str
    details: str
    tests_run: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    changed_files: list[str] = field(default_factory=list)
    merge_ready: bool = False
    token_usage: dict[str, Any] | None = None
    response_model: str | None = None
    reasoning_effort: str | None = None
    # Non-None when a requested reasoning effort could not be applied (implementation note
    # S5 / [AGT-10] [RLSE-05]). Surfaced on the audit trail so a lane never
    # silently believes an unhonored effort was applied.
    downgrade_reason: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    # Sandbox provision outcome when the backend ran under a secure sandbox
    # (e.g. "provisioned", "provision_skipped: no_python_project"). None when
    # the sandbox path did not attempt provisioning (flag off / sandbox off).
    sandbox_provision: str | None = None
    # Off-box self-verify result captured by a backend that runs the lane's
    # TEST_CMD on the same remote host as the agent (grok-remote today,
    # codex-remote next). Shape: {command, exit_code, passed, output_tail}.
    # None when the backend runs on-box (the worker self-verifies locally).
    # Consumed backend-neutrally by worker_daemon._self_verify_phase so a
    # venv-less linked worktree is never re-run locally (REF-20 / OBS-08).
    off_box_self_verify: dict[str, Any] | None = None
    # Which transport the remote turn actually ran: "worktree" | "primary" |
    # "package". Anything other than "worktree" means the lane ran a script from
    # OUTSIDE its own checkout, which can be version-skewed from the code under
    # test. Carried on the result (not just a log line) so a substituted transport
    # is identifiable from the pass record alone [OBS-08]. None for on-box
    # backends, which resolve no remote transport.
    transport_source: str | None = None
    # True when the payload supplied a non-empty handoff_action. False when
    # from_dict defaulted an absent/blank field to needs_guidance so callers can
    # tell an omitted signal from an explicit ask without widening the vocabulary.
    handoff_action_explicit: bool = True

    def __post_init__(self) -> None:
        """Recover defaulted provenance when a rebuild omitted the first-class flag.

        Adapter copies reconstruct ``BackendResult(...)`` field-by-field and
        historically drop ``handoff_action_explicit``. If the copied
        ``raw_payload`` still says the action was defaulted, keep that stamp
        instead of silently promoting the rebuild to an explicit ask.
        """
        if self.handoff_action_explicit is False:
            return
        source = _provenance_source_from_mapping(self.raw_payload)
        if source == "defaulted":
            object.__setattr__(self, "handoff_action_explicit", False)

    @property
    def handoff_action_source(self) -> str:
        return "explicit" if self.handoff_action_explicit else "defaulted"

    def with_fields(self, **changes: Any) -> BackendResult:
        """Copy this result while preserving provenance fields adapters often omit."""
        return replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a standardized dictionary for serialization."""
        source = self.handoff_action_source
        if isinstance(self.raw_payload, dict):
            raw_out = dict(self.raw_payload)
            raw_out["handoff_action_source"] = source
        else:
            raw_out = {"handoff_action_source": source}
        d: dict[str, Any] = {
            "handoff_action": self.handoff_action,
            "summary": self.summary,
            "details": self.details,
            "tests_run": self.tests_run,
            "blockers": self.blockers,
            "changed_files": self.changed_files,
            "merge_ready": self.merge_ready,
            "raw_payload": raw_out,
            "handoff_action_explicit": self.handoff_action_explicit,
            "handoff_action_source": source,
        }
        if self.token_usage is not None:
            d["token_usage"] = self.token_usage
        if self.response_model is not None:
            d["response_model"] = self.response_model
        if self.reasoning_effort is not None:
            d["reasoning_effort"] = self.reasoning_effort
        if self.downgrade_reason is not None:
            d["downgrade_reason"] = self.downgrade_reason
        if self.sandbox_provision is not None:
            d["sandbox_provision"] = self.sandbox_provision
        if self.off_box_self_verify is not None:
            d["off_box_self_verify"] = self.off_box_self_verify
        if self.transport_source is not None:
            d["transport_source"] = self.transport_source
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackendResult:
        """Create a result from a dictionary, typically from a JSON response.

        Provenance keys are not trusted input on first parse. Missing, blank,
        or non-enum ``handoff_action`` is always ``source=defaulted``. A
        defaulted/explicit stamp is honoured only when the action is already a
        valid enum member (adapter-written round-trip).
        """
        payload = {
            key: value
            for key, value in data.items()
            if key not in _ADAPTER_OWNED_RAW_PAYLOAD_KEYS
        }
        raw_action = payload.get("handoff_action")
        action_missing = _handoff_action_is_blank(raw_action)
        valid_enum = _handoff_action_is_valid_enum(raw_action)
        source_hint = _provenance_source_from_mapping(payload)
        explicit_hint = payload.get("handoff_action_explicit")
        if not isinstance(explicit_hint, bool):
            explicit_hint = None

        # Model-controlled provenance cannot forge an explicit ask when the
        # action itself is missing, blank, or off-enum. Round-trip stamps are
        # honoured only beside a valid enum action. Non-bool explicit values
        # are ignored (a string "false" must not become True).
        if not valid_enum:
            handoff_action_explicit = False
        elif source_hint == "defaulted" or explicit_hint is False:
            handoff_action_explicit = False
        else:
            handoff_action_explicit = True

        if action_missing or not valid_enum:
            handoff_action = "needs_guidance"
        else:
            handoff_action = raw_action

        source = "explicit" if handoff_action_explicit else "defaulted"
        payload["handoff_action_source"] = source
        return cls(
            handoff_action=handoff_action,
            summary=payload.get("summary", ""),
            details=payload.get("details", ""),
            tests_run=payload.get("tests_run") or [],
            blockers=payload.get("blockers") or [],
            changed_files=payload.get("changed_files") or [],
            merge_ready=bool(payload.get("merge_ready", False)),
            token_usage=payload.get("token_usage"),
            response_model=payload.get("response_model") or payload.get("model"),
            reasoning_effort=payload.get("reasoning_effort"),
            downgrade_reason=payload.get("downgrade_reason"),
            raw_payload=payload,
            sandbox_provision=payload.get("sandbox_provision"),
            off_box_self_verify=payload.get("off_box_self_verify"),
            transport_source=payload.get("transport_source"),
            handoff_action_explicit=handoff_action_explicit,
        )


def _provenance_source_from_mapping(data: Any) -> str | None:
    """Return defaulted/explicit from a mapping, including nested raw_payload."""
    if not isinstance(data, dict):
        return None
    source = data.get("handoff_action_source")
    if source in _HANDOFF_ACTION_SOURCES:
        return source
    nested = data.get("raw_payload")
    if isinstance(nested, dict):
        nested_source = nested.get("handoff_action_source")
        if nested_source in _HANDOFF_ACTION_SOURCES:
            return nested_source
    return None


class BackendAdapter(Protocol):
    """Execution contract for all orchestration backends."""

    def resolve_reasoning_effort(
        self,
        *,
        orchestrator_root: Path,
        task_ref: str,
        lane_id: str,
        requested: str,
        cycle: int,
        prompt_override: str | None,
        previous_run_exhausted: bool = False,
    ) -> tuple[str | None, list[str]]:
        """Resolve the effective reasoning effort for this cycle.

        Args:
            orchestrator_root: Path to the orchestrator root.
            task_ref: Task reference ID.
            lane_id: Lane ID.
            requested: The requested effort strategy (e.g. 'auto', 'high', 'inherit').
            cycle: The current execution cycle number.
            prompt_override: The fix prompt if this is a fix cycle, else None.
            previous_run_exhausted: If True, escalate effort one level above auto-selected.

        Returns:
            A tuple of (effective_effort, list_of_reasons_for_decision).
        """
        ...

    def execute(
        self,
        prompt: str,
        schema: dict[str, Any],
        worktree_path: Path,
        model: str | None = None,
        reasoning_effort: str | None = None,
        session_mode: str | None = None,
        env: dict[str, str] | None = None,
        progress_callback: Callable[..., None] | None = None,
        **kwargs: Any,
    ) -> BackendResult:
        """Execute a turn with the given prompt and schema.

        Args:
            prompt: The full prompt text to send to the backend.
            schema: The output JSON schema for the turn.
            worktree_path: Path to the worktree where execution should happen.
            model: Explicit model override (e.g. 'gpt-5.4-mini').
            reasoning_effort: Effective reasoning effort (e.g. 'high').
            session_mode: Session mode strategy ('fresh_turn' or 'shared_lane').
            env: Optional environment variables for the execution context.
            progress_callback: Optional callback for telemetry/heartbeats.
            **kwargs: Extra backend-specific parameters.

        Returns:
            A BackendResult object containing the structured output and metadata.
        """
        ...
