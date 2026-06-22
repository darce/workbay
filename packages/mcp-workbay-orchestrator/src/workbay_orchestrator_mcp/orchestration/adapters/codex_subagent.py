from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult


class CodexSubagentAdapter(BackendAdapter):
    """Execution adapter for backends that use a `run_subagent` bridge."""

    def __init__(self, runner: Callable[..., dict[str, Any] | str], name: str = "subagent"):
        self.runner = runner
        self.name = name

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
        **kwargs: Any,
    ) -> BackendResult:
        """Execute turn via the provided bridge runner."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend=self.name)

        runner_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "schema": schema,
            "cwd": str(worktree_path),
        }

        # Inject model into env so the bridge picks it up via CODEX_MODEL.
        if model and env is not None:
            env.setdefault("CODEX_MODEL", model)

        # Handle optional parameters based on bridge support
        if env is not None:
            runner_kwargs["env"] = env

        if progress_callback is not None:
            runner_kwargs["telemetry_callback"] = lambda telemetry: progress_callback(
                WorkerEventName.SUBAGENT_TURN_COMPLETE,
                backend=self.name,
                phase="execution",
                **telemetry,
            )

        try:
            payload = self._call_runner(runner_kwargs)
        except TypeError as exc:
            # Fallback for bridges that don't support telemetry_callback or env
            if "telemetry_callback" in str(exc):
                runner_kwargs.pop("telemetry_callback", None)
                payload = self._call_runner(runner_kwargs)
            elif "env" in str(exc):
                runner_kwargs.pop("env", None)
                payload = self._call_runner(runner_kwargs)
            else:
                raise

        if isinstance(payload, str):
            payload = json.loads(payload)

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_COMPLETE, backend=self.name)

        result = BackendResult.from_dict(payload)
        token_usage = self._normalize_token_usage(payload.get("token_usage") or payload.get("usage"))
        response_model = payload.get("response_model") or payload.get("model") or model
        if token_usage is None:
            if response_model is None and reasoning_effort is None:
                return result
            return BackendResult(
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
        return BackendResult(
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

    def _call_runner(self, kwargs: dict[str, Any]) -> dict[str, Any] | str:
        return self.runner(**kwargs)

    def _normalize_token_usage(self, payload: object) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        if "last" in payload and "total" in payload:
            usage = dict(payload)
            usage.setdefault("usage_source", "observed")
            return usage
        input_tokens = int(payload.get("input_tokens") or payload.get("prompt_tokens") or 0)
        output_tokens = int(payload.get("output_tokens") or payload.get("completion_tokens") or 0)
        cached_input_tokens = int(payload.get("cached_input_tokens") or payload.get("cached_tokens") or 0)
        reasoning_output_tokens = int(payload.get("reasoning_output_tokens") or payload.get("reasoning_tokens") or 0)
        total_tokens = int(payload.get("total_tokens") or (input_tokens + output_tokens))
        if total_tokens <= 0 and input_tokens <= 0 and output_tokens <= 0:
            return None
        breakdown = {
            "cached_input_tokens": cached_input_tokens,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "reasoning_output_tokens": reasoning_output_tokens,
            "total_tokens": total_tokens,
        }
        return {
            "last": breakdown,
            "total": breakdown,
            "model_context_window": payload.get("model_context_window"),
            "usage_source": "observed",
        }
