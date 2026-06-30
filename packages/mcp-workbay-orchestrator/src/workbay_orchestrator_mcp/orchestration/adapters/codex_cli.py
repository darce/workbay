from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult

_CODEX_SEARCH_PATHS = (
    "/Applications/Codex.app/Contents/Resources/codex",
    "{home}/.local/bin/codex",
)


def find_codex(explicit_path: str | None = None) -> str:
    """Find the codex CLI executable."""
    if explicit_path:
        return explicit_path
    for path in _CODEX_SEARCH_PATHS:
        expanded = Path(path.format(home=str(Path.home()))).expanduser()
        if expanded.exists():
            return str(expanded)
    import subprocess

    res = subprocess.run(["which", "codex"], capture_output=True, text=True)
    if res.returncode == 0:
        return res.stdout.strip()
    raise RuntimeError("codex CLI not found in SEARCH_PATHS or PATH. Install it or provide --codex-bin.")


class CodexCliAdapter(BackendAdapter):
    """Execution adapter for the `@openai/codex` CLI."""

    def __init__(self, codex_bin: str | None = None, codex_args: list[str] | None = None):
        self.codex_bin = find_codex(codex_bin)
        self.codex_args = codex_args or []

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
        heartbeat_interval: int = 20,
        **kwargs: Any,
    ) -> BackendResult:
        """Execute turn via `codex exec` subprocess."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        del reasoning_effort, session_mode
        # Extra codex-specific args from kwargs
        extra_args = kwargs.get("codex_args") or self.codex_args

        with tempfile.TemporaryDirectory(prefix="codex-cli-") as tmpdir:
            tmp = Path(tmpdir)
            prompt_file = tmp / "prompt.md"
            schema_file = tmp / "schema.json"
            result_file = tmp / "result.json"

            prompt_file.write_text(prompt)
            schema_file.write_text(json.dumps(schema))

            cmd = [
                self.codex_bin,
                "exec",
                "-C",
                str(worktree_path),
                *extra_args,
            ]
            if model:
                cmd.extend(["--model", model])
            cmd.extend(
                [
                    "--output-schema",
                    str(schema_file),
                    "-o",
                    str(result_file),
                    "-",
                ]
            )

            with prompt_file.open("r") as stdin_fh:
                completed = self._run_codex_process(
                    cmd=cmd,
                    stdin_fh=stdin_fh,
                    env=env or os.environ.copy(),
                    heartbeat_interval=heartbeat_interval,
                    progress_callback=progress_callback,
                )

            if completed.returncode != 0:
                stderr_tail = self._tail_text(completed.stderr)
                raise RuntimeError(f"codex exec failed (exit {completed.returncode}):\n{stderr_tail}")

            if not result_file.is_file():
                raise RuntimeError("codex exec completed but no result file was produced.")

            payload = json.loads(result_file.read_text())
            response_model = payload.get("response_model") or payload.get("model") or model
            reasoning_effort = kwargs.get("reasoning_effort")

            # Extract token usage from the result JSON or stdout
            token_usage = _extract_codex_usage(payload, completed.stdout)
            if progress_callback and token_usage:
                progress_callback(
                    WorkerEventName.SUBAGENT_TURN_COMPLETE,
                    backend="codex-cli",
                    phase="execution",
                    token_usage=token_usage,
                    response_model=response_model,
                    reasoning_effort=reasoning_effort,
                )

            result = BackendResult.from_dict(payload)
            if token_usage:
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

    def _run_codex_process(
        self,
        *,
        cmd: list[str],
        stdin_fh: Any,
        env: dict[str, str],
        heartbeat_interval: int,
        progress_callback: Callable[..., None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Subprocess runner with heartbeats."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        proc = subprocess.Popen(
            cmd,
            stdin=stdin_fh,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        started = time.monotonic()
        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, pid=proc.pid, backend="codex-cli")

        while True:
            try:
                stdout, stderr = proc.communicate(timeout=heartbeat_interval)
                if progress_callback:
                    progress_callback(WorkerEventName.EXEC_COMPLETE, pid=proc.pid, backend="codex-cli")
                return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
            except subprocess.TimeoutExpired:
                if progress_callback:
                    elapsed = int(time.monotonic() - started)
                    progress_callback(
                        WorkerEventName.EXEC_HEARTBEAT,
                        pid=proc.pid,
                        elapsed_seconds=elapsed,
                        backend="codex-cli",
                    )

    def _tail_text(self, text: str | bytes, limit: int = 500) -> str:
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="replace")
        return text.strip()[-limit:]


def _extract_codex_usage(payload: dict[str, Any], stdout: str | None) -> dict[str, Any] | None:
    """Extract token usage from a Codex CLI result JSON or stdout.

    The Codex CLI may embed usage data in the result JSON under ``usage`` or
    ``token_usage``, or print a usage summary to stdout.  We normalize into
    the ``{last: {...}, total: {...}}`` shape expected by the observability
    pipeline.
    """
    # Check the result JSON first
    usage = payload.get("token_usage") or payload.get("usage")
    if isinstance(usage, dict):
        # Already in normalized shape?
        if "last" in usage and "total" in usage:
            return usage
        # Flat shape from the CLI
        return _normalize_flat_usage(usage)

    # Try parsing a usage summary line from stdout
    if stdout:
        match = re.search(
            r"tokens?\s*(?:used|usage)[:\s]*(\d+)\s*input[,\s]+(\d+)\s*output",
            stdout,
            re.IGNORECASE,
        )
        if match:
            input_tokens = int(match.group(1))
            output_tokens = int(match.group(2))
            return _normalize_flat_usage(
                {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                }
            )
    return None


def _normalize_flat_usage(usage: dict[str, Any]) -> dict[str, Any]:
    """Normalize a flat usage dict into the standard nested shape."""
    input_tokens = int(usage.get("input_tokens") or usage.get("inputTokens") or 0)
    output_tokens = int(usage.get("output_tokens") or usage.get("outputTokens") or 0)
    cached = int(usage.get("cached_input_tokens") or usage.get("cachedInputTokens") or 0)
    reasoning = int(usage.get("reasoning_output_tokens") or usage.get("reasoningOutputTokens") or 0)
    total_tokens = int(usage.get("total_tokens") or usage.get("totalTokens") or 0)
    if total_tokens == 0:
        total_tokens = input_tokens + output_tokens
    breakdown = {
        "cached_input_tokens": cached,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "reasoning_output_tokens": reasoning,
        "total_tokens": total_tokens,
    }
    return {
        "last": breakdown,
        "total": breakdown,
        "model_context_window": usage.get("model_context_window") or usage.get("modelContextWindow"),
        "usage_source": "observed",
    }
