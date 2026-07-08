from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult
from ._result_text import extract_result_payload, find_embedded_json_object, normalize_cli_usage


class ClaudeCodeAdapter(BackendAdapter):
    """Execution adapter for the `claude` CLI (Anthropic)."""

    supports_jail = True

    def __init__(self, claude_bin: str = "claude"):
        self.claude_bin = claude_bin

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

        with tempfile.TemporaryDirectory(prefix="claude-code-") as tmpdir:
            tmp = Path(tmpdir)
            prompt_file = tmp / "prompt.md"

            # Claude Code expects a natural language prompt.
            # We append the schema requirements to the prompt.
            full_prompt = (
                f"{prompt}\n\n"
                f"IMPORTANT: Your final output must be a single JSON object matching this schema:\n"
                f"{json.dumps(schema, indent=2)}\n"
            )
            prompt_file.write_text(full_prompt)

            # Lane write-jail prefix (implementation note / adoption C). Empty unless gated in.
            jail_prefix = list(kwargs.get("jail_argv_prefix") or [])
            cmd = [
                *jail_prefix,
                self.claude_bin,
                "execute",
                "--cwd",
                str(worktree_path),
                "--file",
                str(prompt_file),
                "--output-format",
                "json",
            ]
            # Model priority: explicit parameter > ANTHROPIC_MODEL env var
            effective_model = model or (env or {}).get("ANTHROPIC_MODEL")
            if effective_model:
                cmd.extend(["--model", effective_model])

            try:
                completed = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
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

            output = completed.stdout
            try:
                response = json.loads(output)
            except json.JSONDecodeError:
                # Fallback: try regex extraction for older CLI versions
                json_str = find_embedded_json_object(output)
                if not json_str:
                    raise RuntimeError("Claude Code completed but no JSON found in output.")
                try:
                    response = json.loads(json_str)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"Failed to parse JSON from Claude Code output: {exc}")

            # Extract usage data from the structured response
            token_usage = normalize_cli_usage(response)

            # The structured response wraps the result; extract the inner content
            payload = extract_result_payload(response)
            response_model = response.get("model") or payload.get("model") or effective_model

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
            return result
