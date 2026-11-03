from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult
from ._result_text import normalize_cli_usage

# Cap ``claude -p`` actually passes as ``--max-turns``. Keep this on the
# adapter so WorkerConfig.grok_max_turns is not the Claude handoff source.
DEFAULT_CLAUDE_MAX_TURNS = 60

# Values ``claude --effort`` accepts; anything else is left to the CLI default.
_CLAUDE_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})


class ClaudeCodeAdapter(BackendAdapter):
    """Execution adapter for the `claude` CLI (Anthropic)."""

    supports_jail = True

    def __init__(self, claude_bin: str = "claude", *, max_turns: int = DEFAULT_CLAUDE_MAX_TURNS):
        self.claude_bin = claude_bin
        self.max_turns = (
            max_turns
            if isinstance(max_turns, int) and not isinstance(max_turns, bool) and max_turns >= 1
            else DEFAULT_CLAUDE_MAX_TURNS
        )

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
        """Resolve reasoning effort via the shared auto-resolver."""
        from .._env import resolve_auto_reasoning_effort  # noqa: PLC0415

        return resolve_auto_reasoning_effort(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            requested=requested,
            cycle=cycle,
            prompt_override=prompt_override,
            previous_run_exhausted=previous_run_exhausted,
        )

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
        """Execute turn via `claude` CLI."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="claude-code")

        # Lane write-jail prefix (implementation note / adoption C). Empty unless gated in.
        jail_prefix = list(kwargs.get("jail_argv_prefix") or [])
        from .remote_exec import (  # noqa: PLC0415
            count_observed_tool_events,
            host_session_num_turns,
            positive_max_turns,
            stamp_attempt_evidence,
        )

        enforced_max_turns = positive_max_turns(kwargs.get("max_turns")) or self.max_turns
        # The prompt goes on stdin. --json-schema makes the CLI validate the
        # final answer and return it as the envelope's ``structured_output``.
        cmd = [
            *jail_prefix,
            self.claude_bin,
            "-p",
            "--output-format",
            "json",
            "--json-schema",
            json.dumps(schema),
            "--max-turns",
            str(enforced_max_turns),
        ]
        # Model priority: explicit parameter > ANTHROPIC_MODEL env var
        effective_model = model or (env or {}).get("ANTHROPIC_MODEL")
        if effective_model:
            cmd.extend(["--model", effective_model])
        if reasoning_effort in _CLAUDE_EFFORTS:
            cmd.extend(["--effort", reasoning_effort])

        try:
            completed = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                check=False,
                cwd=str(worktree_path),
                env=env or os.environ.copy(),
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("Claude Code execution timed out after 10 minutes.")
        except FileNotFoundError:
            raise RuntimeError(f"Claude CLI '{self.claude_bin}' not found in PATH.")

        if completed.returncode != 0:
            stderr_text = (completed.stderr or "").strip()
            stdout_text = (completed.stdout or "").strip()
            stderr_tail = stderr_text[-500:] if stderr_text else ""
            stdout_tail = stdout_text[-500:] if stdout_text else ""
            raise RuntimeError(
                f"Claude Code failed (exit {completed.returncode}).\nSTDOUT: {stdout_tail}\nSTDERR: {stderr_tail}"
            )

        try:
            response = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse Claude Code JSON envelope: {exc}")
        payload = response.get("structured_output") if isinstance(response, dict) else None
        if not isinstance(payload, dict):
            subtype = response.get("subtype") if isinstance(response, dict) else None
            raise RuntimeError(f"Claude Code returned no structured_output (subtype={subtype!r}).")
        # Evidence comes from the host envelope only; the worker-authored
        # structured_output must not mint tool calls or turns.
        host_envelope = {key: value for key, value in response.items() if key != "structured_output"}

        # Extract usage data from the structured response
        token_usage = normalize_cli_usage(response)
        response_model = response.get("model") or effective_model

        if progress_callback and token_usage:
            progress_callback(
                WorkerEventName.SUBAGENT_TURN_COMPLETE,
                backend="claude-code",
                phase="execution",
                token_usage=token_usage,
                response_model=response_model,
                reasoning_effort=reasoning_effort,
            )

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_COMPLETE, backend="claude-code")

        result = BackendResult.from_dict(payload)
        if token_usage:
            # Attach usage to the result via a new instance (frozen dataclass)
            result = BackendResult(
                handoff_action=result.handoff_action,
                summary=result.summary,
                details=result.details,
                tests_run=result.tests_run,
                blockers=result.blockers,
                changed_files=result.changed_files,
                merge_ready=result.merge_ready,
                token_usage=token_usage,
                response_model=response_model,
                reasoning_effort=reasoning_effort,
                raw_payload=result.raw_payload,
            )
        elif response_model is not None or reasoning_effort is not None:
            result = BackendResult(
                handoff_action=result.handoff_action,
                summary=result.summary,
                details=result.details,
                tests_run=result.tests_run,
                blockers=result.blockers,
                changed_files=result.changed_files,
                merge_ready=result.merge_ready,
                token_usage=result.token_usage,
                response_model=response_model,
                reasoning_effort=reasoning_effort,
                raw_payload=result.raw_payload,
            )
        return stamp_attempt_evidence(
            result,
            tool_call_count=count_observed_tool_events(host_envelope),
            max_turns=enforced_max_turns,
            receiver_num_turns=host_session_num_turns(host_envelope),
        )
