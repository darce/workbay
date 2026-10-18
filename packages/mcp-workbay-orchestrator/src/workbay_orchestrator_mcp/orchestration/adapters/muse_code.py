from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult

# Values the shared operator-effort grammar admits for muse lanes; the muse
# CLI also accepts none/minimal/ultra, which stay refused at that seam.
_MUSE_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})


def _muse_exec_argv(
    *,
    muse_bin: str,
    prompt_file: str,
    schema_file: str,
    model: str | None,
    reasoning_effort: str | None,
) -> list[str]:
    cmd = [
        muse_bin,
        "exec",
        "--prompt-file",
        prompt_file,
        "--output-schema",
        schema_file,
        "--yolo",
    ]
    if model:
        cmd.extend(["--model", model])
    if reasoning_effort in _MUSE_EFFORTS:
        cmd.extend(["--reasoning-effort", reasoning_effort])
    return cmd


def _parse_muse_jsonl(stdout: str) -> dict[str, Any]:
    """Return the final structured payload from muse exec output.

    Plain mode prints the schema-shaped payload alone on stdout; --json mode
    wraps it in a JSONL run_terminal event. Accept both.
    """
    stripped = stdout.strip()
    if stripped.startswith("{"):
        try:
            direct = json.loads(stripped)
        except json.JSONDecodeError:
            direct = None
        if isinstance(direct, dict) and "payload" not in direct:
            return direct
    terminal_text: str | None = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = record.get("payload") if isinstance(record, dict) else None
        if isinstance(payload, dict) and payload.get("kind") == "run_terminal":
            text = payload.get("text")
            if isinstance(text, str):
                terminal_text = text
    if terminal_text is None:
        raise RuntimeError("Muse exec returned no run_terminal event.")
    try:
        parsed = json.loads(terminal_text)
    except json.JSONDecodeError:
        return {"handoff_action": "completed", "summary": terminal_text}
    if not isinstance(parsed, dict):
        return {"handoff_action": "completed", "summary": terminal_text}
    return parsed


class MuseCodeAdapter(BackendAdapter):
    """Execution/shaping adapter for the `muse` CLI (Meta Muse Code)."""

    supports_jail = True

    def __init__(self, muse_bin: str = "muse", *, timeout: int = 600):
        self.muse_bin = muse_bin
        self.timeout = timeout if isinstance(timeout, int) and timeout >= 1 else 600

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
        """Execute one turn via `muse exec` (brief and schema via files)."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="muse-remote")

        jail_prefix = list(kwargs.get("jail_argv_prefix") or [])
        with tempfile.TemporaryDirectory(prefix="muse-exec-") as tmp:
            prompt_file = Path(tmp) / "brief.md"
            schema_file = Path(tmp) / "schema.json"
            prompt_file.write_text(prompt, encoding="utf-8")
            schema_file.write_text(json.dumps(schema), encoding="utf-8")
            cmd = [
                *jail_prefix,
                *_muse_exec_argv(
                    muse_bin=self.muse_bin,
                    prompt_file=str(prompt_file),
                    schema_file=str(schema_file),
                    model=model,
                    reasoning_effort=reasoning_effort,
                ),
            ]
            try:
                completed = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    cwd=str(worktree_path),
                    env=env or os.environ.copy(),
                    timeout=self.timeout,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"Muse exec timed out after {self.timeout} seconds.")
            except FileNotFoundError:
                raise RuntimeError(f"Muse CLI '{self.muse_bin}' not found in PATH.")

        if completed.returncode != 0:
            stderr_tail = (completed.stderr or "").strip()[-500:]
            stdout_tail = (completed.stdout or "").strip()[-500:]
            raise RuntimeError(
                f"Muse exec failed (exit {completed.returncode}).\nSTDOUT: {stdout_tail}\nSTDERR: {stderr_tail}"
            )

        payload = _parse_muse_jsonl(completed.stdout or "")
        if progress_callback:
            progress_callback(WorkerEventName.EXEC_COMPLETE, backend="muse-remote")
        return BackendResult.from_dict(payload)
