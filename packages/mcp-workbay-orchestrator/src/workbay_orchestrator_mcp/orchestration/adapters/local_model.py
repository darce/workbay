from __future__ import annotations

import json
import math
import os
import urllib.request
from collections.abc import Mapping
from dataclasses import replace
from http.client import HTTPException
from pathlib import Path
from typing import Any, Callable

from .._env import resolve_auto_reasoning_effort
from ..backend_adapter import BackendAdapter, BackendResult

_DEFAULT_BASE_URL = "http://localhost:11434/v1"
_DEFAULT_API_KEY = "ollama"
_DEFAULT_TIMEOUT_S = 120


class LocalModelResponseError(RuntimeError):
    """The endpoint did not return a usable JSON completion."""


class LocalModelAdapter(BackendAdapter):
    """Execution adapter for local OpenAI-compatible models (Ollama, vLLM, etc)."""

    def __init__(
        self, base_url: str | None = None, api_key: str | None = None, *, timeout: float | None = None
    ) -> None:
        self.base_url = base_url.rstrip("/") if base_url is not None else None
        self.api_key = api_key
        self.timeout = timeout

    def _settings(self, env: Mapping[str, str] | None, timeout: float | None = None) -> tuple[str, str, float]:
        settings = os.environ if env is None else env
        base_url = self.base_url or settings.get("WORKBAY_LOCAL_MODEL_BASE_URL", "").strip() or _DEFAULT_BASE_URL
        api_key = self.api_key
        if api_key is None:
            api_key = settings.get("WORKBAY_LOCAL_MODEL_API_KEY", "") or _DEFAULT_API_KEY
        effective_timeout = timeout if timeout is not None else self.timeout
        if effective_timeout is None:
            configured_timeout = settings.get("WORKBAY_LOCAL_MODEL_TIMEOUT_S", "").strip()
            try:
                effective_timeout = float(configured_timeout or _DEFAULT_TIMEOUT_S)
            except ValueError:
                effective_timeout = _DEFAULT_TIMEOUT_S
            if not math.isfinite(effective_timeout) or effective_timeout <= 0:
                effective_timeout = _DEFAULT_TIMEOUT_S
        elif not math.isfinite(effective_timeout) or effective_timeout <= 0:
            raise ValueError("Local model timeout must be a finite positive number.")
        return base_url.rstrip("/"), api_key, effective_timeout

    def available(self, env: Mapping[str, str] | None = None) -> bool:
        """Check if the local model endpoint is reachable."""
        try:
            base_url, api_key, timeout = self._settings(env)
            req = urllib.request.Request(
                f"{base_url}/models", headers={"Authorization": f"Bearer {api_key}"}, method="GET"
            )
            with urllib.request.urlopen(req, timeout=min(2, timeout)) as resp:
                status: int = resp.status
                return status == 200
        except (OSError, HTTPException, ValueError):
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
        schema: dict[str, Any] | None,
        worktree_path: Path,
        model: str | None = None,
        reasoning_effort: str | None = None,
        session_mode: str | None = None,
        env: dict[str, str] | None = None,
        progress_callback: Callable[..., None] | None = None,
        *,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> BackendResult:
        """Execute turn via OpenAI-compatible completion API."""
        from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

        max_turns = kwargs.get("max_turns")
        if not model:
            raise ValueError("model is required for LocalModelAdapter.")
        base_url, api_key, request_timeout = self._settings(env, timeout)

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="local-model-openai", model=model)

        # Build message payload for instruction-following model
        messages = [
            {"role": "system", "content": "You are a helpful coding assistant. Follow the output schema strictly."},
            {"role": "user", "content": prompt},
        ]

        payload = {"model": model, "messages": messages, "temperature": 0.0}
        if schema is not None:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "workbay_result", "schema": schema},
            }

        req = urllib.request.Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=request_timeout) as resp:
                body = resp.read()
        except (OSError, HTTPException) as exc:
            raise RuntimeError(f"Local model call failed: {exc}") from exc

        try:
            data = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LocalModelResponseError("Local model endpoint returned an invalid JSON response.") from exc
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LocalModelResponseError("Local model response is missing completion message content.") from exc
        try:
            result_data = json.loads(content)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LocalModelResponseError("Local model completion content must be a JSON object.") from exc
        if not isinstance(result_data, dict):
            raise LocalModelResponseError("Local model completion content must be a JSON object.")

        result = BackendResult.from_dict(result_data)
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

        result = replace(
            result,
            token_usage=token_usage if token_usage is not None else result.token_usage,
            response_model=response_model,
            reasoning_effort=reasoning_effort,
        )
        from .remote_exec import (  # noqa: PLC0415
            _RECEIVER_NUM_TURNS_KEY,
            count_observed_tool_events,
            positive_max_turns,
            stamp_attempt_evidence,
        )

        # Count chat-level tool_calls only. Never pass the worker
        # result object, and never walk message content (the worker JSON).
        observed = count_observed_tool_events(_chat_transport_source(data))
        raw = dict(result.raw_payload or {})
        raw.pop("phase_timing", None)
        raw.pop("tool_call_count", None)
        raw.pop(_RECEIVER_NUM_TURNS_KEY, None)
        stamped = stamp_attempt_evidence(
            replace(result, raw_payload=raw),
            tool_call_count=observed,
            max_turns=positive_max_turns(max_turns),
        )
        envelope = dict(stamped.raw_payload or {})
        envelope[_RECEIVER_NUM_TURNS_KEY] = 1
        return replace(stamped, raw_payload=envelope)


def _chat_transport_source(data: dict[str, Any]) -> dict[str, Any]:
    """Chat-level tool_calls and message streams, excluding worker content JSON."""
    transport: dict[str, Any] = {}
    choices = data.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message")
            if not isinstance(message, dict):
                continue
            for key in ("tool_calls", "tool_uses", "toolUses"):
                if key in message and key not in transport:
                    transport[key] = message[key]
    for key in ("messages", "events"):
        if key in data:
            transport[key] = data[key]
    return transport


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
