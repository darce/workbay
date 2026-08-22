from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any, Callable

from .._env import resolve_auto_reasoning_effort
from ..backend_adapter import BackendAdapter, BackendResult


class LocalModelAdapter(BackendAdapter):
    """Execution adapter for local OpenAI-compatible models (Ollama, vLLM, etc)."""

    def __init__(self, base_url: str = "http://localhost:11434/v1", api_key: str = "ollama"):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def available(self) -> bool:
        """Check if the local model endpoint is reachable."""
        try:
            req = urllib.request.Request(f"{self.base_url}/models", method="GET")
            with urllib.request.urlopen(req, timeout=2) as resp:
                status: int = resp.status
                return status == 200
        except Exception:
            return False

    def resolve_reasoning_effort(
        self,
        orchestrator_root: Path,
        task_ref: str,
        lane_id: str,
        requested: str,
        cycle: int,
        prompt_override: str | None = None,
        previous_run_exhausted: bool = False,
    ) -> tuple[str | None, list[str]]:
        """Resolve reasoning effort using shared auto-scoring logic."""
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
        """Execute turn via OpenAI-compatible completion API."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        del session_mode, schema, worktree_path, env, kwargs
        if not model:
            raise ValueError("model is required for LocalModelAdapter.")

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="local-model-openai", model=model)

        # Build message payload for instruction-following model
        messages = [
            {"role": "system", "content": "You are a helpful coding assistant. Follow the output schema strictly."},
            {"role": "user", "content": prompt},
        ]

        # Note: In a real implementation with structured output, we would use
        # tools or json-mode if supported by the local backend.
        payload = {"model": model, "messages": messages, "temperature": 0.0}

        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                content = data["choices"][0]["message"]["content"]
                token_usage = _extract_openai_usage(data)
                response_model = data.get("model") or model

                if progress_callback and token_usage is not None:
                    progress_callback(
                        WorkerEventName.SUBAGENT_TURN_COMPLETE,
                        backend="local-model-openai",
                        phase="execution",
                        token_usage=token_usage,
                        response_model=response_model,
                        reasoning_effort=reasoning_effort,
                    )

                if progress_callback:
                    progress_callback(WorkerEventName.EXEC_COMPLETE, backend="local-model-openai")

                # We expect the model's content to be valid JSON matching the schema
                # for the purposes of this adapter.
                result_data = json.loads(content)
                result = BackendResult.from_dict(result_data)
                if token_usage is not None:
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
                if response_model is not None or reasoning_effort is not None:
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
                return result
        except Exception as exc:
            raise RuntimeError(f"Local model call failed: {exc}")


def _extract_openai_usage(payload: dict[str, Any]) -> dict[str, Any] | None:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
    reasoning_tokens = int(usage.get("reasoning_tokens") or usage.get("reasoning_output_tokens") or 0)
    cached_input_tokens = int(usage.get("cached_tokens") or usage.get("cached_input_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or (input_tokens + output_tokens))
    breakdown = {
        "cached_input_tokens": cached_input_tokens,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "reasoning_output_tokens": reasoning_tokens,
        "total_tokens": total_tokens,
    }
    return {
        "last": breakdown,
        "total": breakdown,
        "model_context_window": None,
        "usage_source": "observed",
    }
