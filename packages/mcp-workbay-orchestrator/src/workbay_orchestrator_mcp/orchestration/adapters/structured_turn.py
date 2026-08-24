"""Always-available in-repo BackendAdapter that composes run_structured_turn.

``StructuredTurnAdapter`` is the anchor of the cross-vendor equivalence
matrix (internal). It exercises the same MCP primitive
(``workbay_orchestrator_mcp.run_structured_turn``) that Codex and Copilot
coordinators invoke at runtime, but composes it in-process so the
adapter is always importable and instantiable — without depending on
vendor-supplied bridge modules that do not ship in this repo.

The default runner proxies to ``api.run_structured_turn`` with a
configurable downstream backend; tests inject a fake runner to drive
the adapter deterministically without a real subprocess or bridge.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult


class StructuredTurnAdapter(BackendAdapter):
    """BackendAdapter that composes run_structured_turn in-process.

    The ``runner`` parameter is the unit the adapter actually calls. It
    receives ``prompt``, ``schema``, ``cwd``, ``env`` kwargs and returns
    either a dict or a JSON string. Production callers leave ``runner``
    unset and the adapter falls back to ``api.run_structured_turn``
    against ``downstream_backend`` (default ``codex-subagent``). Tests
    inject their own runner to avoid the bridge requirement.
    """

    def __init__(
        self,
        runner: Callable[..., dict[str, Any] | str] | None = None,
        name: str = "structured-turn",
        downstream_backend: str = "codex-subagent",
        timeout_seconds: float = 120.0,
    ) -> None:
        self.runner = runner
        self.name = name
        self._downstream_backend = downstream_backend
        self._timeout_seconds = timeout_seconds

    @property
    def downstream_backend(self) -> str:
        """Name of the backend this adapter composes (probe surface, internal)."""
        return self._downstream_backend

    @property
    def runner_emits_envelope(self) -> bool:
        """True when the resolved runner is the composed default (internal).

        The default runner proxies ``api.run_structured_turn`` and therefore
        always returns the downstream ``{"ok", "backend", "result"|"error"}``
        envelope. Injected runners own their payload shape; dispatch must pass
        it through verbatim without envelope sniffing — a caller schema that
        merely *looks* like the envelope must never be unwrapped.
        """
        return self.runner is None

    def resolve_runner(self) -> Callable[..., dict[str, Any] | str]:
        """Return the synchronous runner seam for MCP dispatch (internal).

        ``api.run_structured_turn`` dispatches in-process backends through this
        seam instead of ``execute()`` so arbitrary caller schemas pass through
        verbatim — ``BackendResult`` coercion is a worker-lane concern.

        Structural recursion guard: the default runner composes another
        ``run_structured_turn`` call against ``downstream_backend``, so a
        downstream that is itself ``in-process`` would recurse. Refuse it at
        resolution time with a configuration error. Injected runners bypass
        the guard deliberately (tests own their composition).
        """
        if self.runner is not None:
            return self.runner
        from ..backend_registry import get_backend_spec  # noqa: PLC0415

        downstream_spec = get_backend_spec(self._downstream_backend)
        if downstream_spec.kind == "in-process":
            raise RuntimeError(
                f"StructuredTurnAdapter downstream backend '{self._downstream_backend}' is in-process; "
                "refusing recursive composition. Configure a bridge backend downstream."
            )
        return self._default_runner

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
        runner = self.runner or self._default_runner

        runner_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "schema": schema,
            "cwd": str(worktree_path),
        }
        if env is not None:
            runner_kwargs["env"] = env

        payload = runner(**runner_kwargs)
        if isinstance(payload, str):
            payload = json.loads(payload)

        if isinstance(payload, dict) and payload.get("ok") is False:
            error_msg = payload.get("error") or "unknown backend error"
            backend_name = payload.get("backend") or self._downstream_backend
            raise RuntimeError(f"StructuredTurnAdapter downstream backend '{backend_name}' failed: {error_msg}")

        if isinstance(payload, dict) and "result" in payload and isinstance(payload["result"], dict):
            # api.run_structured_turn envelope: {"ok": bool, "backend": str, "result": {...}}
            payload = payload["result"]

        result = BackendResult.from_dict(payload)
        response_model = payload.get("response_model") or payload.get("model") or model
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

    def _default_runner(
        self, *, prompt: str, schema: dict[str, Any], cwd: str, env: dict[str, str] | None = None
    ) -> dict[str, Any]:
        from workbay_orchestrator_mcp import api  # noqa: PLC0415

        response = api.run_structured_turn(
            prompt=prompt,
            schema=schema,
            cwd=cwd,
            backend=self._downstream_backend,
            env=env,
            timeout_seconds=self._timeout_seconds,
        )
        return response
