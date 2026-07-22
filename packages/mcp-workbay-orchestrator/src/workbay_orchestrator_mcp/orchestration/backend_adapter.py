from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol


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

    def to_dict(self) -> dict[str, Any]:
        """Convert to a standardized dictionary for serialization."""
        d: dict[str, Any] = {
            "handoff_action": self.handoff_action,
            "summary": self.summary,
            "details": self.details,
            "tests_run": self.tests_run,
            "blockers": self.blockers,
            "changed_files": self.changed_files,
            "merge_ready": self.merge_ready,
            "raw_payload": self.raw_payload,
        }
        if self.token_usage is not None:
            d["token_usage"] = self.token_usage
        if self.response_model is not None:
            d["response_model"] = self.response_model
        if self.reasoning_effort is not None:
            d["reasoning_effort"] = self.reasoning_effort
        if self.sandbox_provision is not None:
            d["sandbox_provision"] = self.sandbox_provision
        if self.off_box_self_verify is not None:
            d["off_box_self_verify"] = self.off_box_self_verify
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackendResult:
        """Create a result from a dictionary, typically from a JSON response."""
        return cls(
            handoff_action=data.get("handoff_action", "needs_guidance"),
            summary=data.get("summary", ""),
            details=data.get("details", ""),
            tests_run=data.get("tests_run") or [],
            blockers=data.get("blockers") or [],
            changed_files=data.get("changed_files") or [],
            merge_ready=bool(data.get("merge_ready", False)),
            token_usage=data.get("token_usage"),
            response_model=data.get("response_model") or data.get("model"),
            reasoning_effort=data.get("reasoning_effort"),
            raw_payload=data,
            sandbox_provision=data.get("sandbox_provision"),
            off_box_self_verify=data.get("off_box_self_verify"),
        )


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
