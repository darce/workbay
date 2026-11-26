"""Execution/shaping adapter for the Antigravity CLI (`agy`)."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from ..agy_lane_config import AGY_EFFORTS
from ..backend_adapter import BackendAdapter, BackendResult


def _agy_exec_argv(
    *,
    agy_bin: str,
    schema_file: str,
    prompt: str,
    model: str | None,
    reasoning_effort: str | None,
) -> list[str]:
    cmd = [
        agy_bin,
        "--output-format",
        "json",
        "--dangerously-skip-permissions",
        "--disable-slash-commands",
        "--json-schema",
        schema_file,
    ]
    if model:
        cmd.extend(["--model", model])
    if reasoning_effort in AGY_EFFORTS:
        cmd.extend(["--effort", reasoning_effort])
    cmd.extend(["-p", prompt])
    return cmd


def _parse_agy_output(stdout: str) -> dict[str, Any]:
    """Return the structured JSON payload from `agy -p --output-format json`."""
    stripped = stdout.strip()
    if not stripped:
        raise RuntimeError("agy returned empty stdout.")
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"agy returned non-JSON stdout: {stripped[:200]!r}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"agy returned non-object JSON payload: {type(parsed).__name__}")
    # Unwrap standard envelope if present (e.g., {"result": {...}} or {"structured_output": {...}})
    for key in ("structured_output", "result"):
        nested = parsed.get(key)
        if isinstance(nested, dict):
            return nested
        if isinstance(nested, str):
            try:
                decoded = json.loads(nested)
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, dict):
                return decoded
    return parsed


class AgyCliAdapter(BackendAdapter):
    """Execution/shaping adapter for the `agy` CLI (Antigravity CLI v1.5+)."""

    supports_jail = True

    def __init__(self, agy_bin: str = "agy", *, timeout: int = 1800):
        self.agy_bin = agy_bin
        self.timeout = timeout if isinstance(timeout, int) and timeout >= 1 else 1800

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
        """Execute one turn via `agy -p` (brief on stdin, schema via file)."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="agy-remote")

        jail_prefix = list(kwargs.get("jail_argv_prefix") or [])
        with tempfile.TemporaryDirectory(prefix="agy-exec-") as tmp:
            schema_file = Path(tmp) / "schema.json"
            schema_file.write_text(json.dumps(schema), encoding="utf-8")
            cmd = [
                *jail_prefix,
                *_agy_exec_argv(
                    agy_bin=self.agy_bin,
                    schema_file=str(schema_file),
                    prompt=prompt,
                    model=model,
                    reasoning_effort=reasoning_effort,
                ),
            ]
            run_env = dict(env) if env is not None else os.environ.copy()
            run_env.setdefault("WORKBAY_HARNESS", "agy-cli")
            try:
                completed = subprocess.run(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    check=False,
                    cwd=str(worktree_path),
                    env=run_env,
                    timeout=self.timeout,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"agy timed out after {self.timeout} seconds.")
            except FileNotFoundError:
                raise RuntimeError(f"agy CLI '{self.agy_bin}' not found in PATH.")

        if completed.returncode != 0:
            stderr_tail = (completed.stderr or "").strip()[-500:]
            stdout_tail = (completed.stdout or "").strip()[-500:]
            raise RuntimeError(
                f"agy failed (exit {completed.returncode}).\nSTDOUT: {stdout_tail}\nSTDERR: {stderr_tail}"
            )

        payload = _parse_agy_output(completed.stdout or "")
        if progress_callback:
            progress_callback(WorkerEventName.EXEC_COMPLETE, backend="agy-remote")
        return BackendResult.from_dict(payload)
