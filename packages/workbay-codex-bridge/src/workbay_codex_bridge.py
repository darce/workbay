from __future__ import annotations

import atexit
import json
import os
import select
import shutil
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

_CLIENT_NAME = "workbay-codex-bridge"
_CLIENT_VERSION = "0.1.0"
_DEFAULT_TIMEOUT_SECONDS = 120.0
_SHUTDOWN_GRACE_SECONDS = 1.0
_REASONING_EFFORT_KEYS = ("CODEX_REASONING_EFFORT", "REASONING_EFFORT")
_MODEL_KEYS = ("CODEX_MODEL", "MODEL")
_CODEX_BIN_KEYS = ("CODEX_BIN", "CODEX_PATH")
_VALID_REASONING_EFFORTS = {"low", "medium", "high", "xhigh"}
_SESSION_MODE_KEYS = ("CODEX_SUBAGENT_BRIDGE_SESSION_MODE",)
_SHARED_SESSION_VALUES = {"shared", "long-lived", "long_lived"}
_CODEX_SEARCH_PATHS = (
    "/Applications/Codex.app/Contents/Resources/codex",
    "{home}/.local/bin/codex",
)
_shared_clients_lock = threading.Lock()
_shared_clients: dict[tuple[str, tuple[tuple[str, str], ...]], "_SharedClientEntry"] = {}


def run_subagent(
    prompt: str,
    schema: dict[str, Any],
    cwd: str,
    env: dict[str, str] | None = None,
    telemetry_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one structured Codex turn in ``cwd`` and return the final JSON object."""
    if _shared_session_requested(env):
        return _run_subagent_shared(
            prompt=prompt,
            schema=schema,
            cwd=cwd,
            env=env,
            telemetry_callback=telemetry_callback,
        )

    client = AppServerClient(cwd=cwd, env=env)
    try:
        client.start()
        client.initialize()
        thread_id = client.start_thread()
        return client.run_structured_turn(
            thread_id=thread_id,
            prompt=prompt,
            output_schema=schema,
            telemetry_callback=telemetry_callback,
        )
    finally:
        client.close()


@dataclass
class _SharedClientEntry:
    client: "AppServerClient"
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class AppServerClient:
    cwd: str
    env: Mapping[str, str] | None = None
    codex_bin: str = "codex"
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    popen_factory: Any = subprocess.Popen
    proc: subprocess.Popen[str] | None = field(init=False, default=None)
    _request_id: int = field(init=False, default=0)
    _pending_notifications: deque[dict[str, Any]] = field(init=False, default_factory=deque)

    def start(self) -> None:
        launch_env = os.environ.copy()
        if self.env:
            launch_env.update({key: str(value) for key, value in self.env.items()})
        codex_bin = _resolve_codex_bin(self.codex_bin, self.env)

        proc = self.popen_factory(
            [codex_bin, "app-server", "--listen", "stdio://"],
            cwd=self.cwd,
            env=launch_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if proc.stdin is None or proc.stdout is None or proc.stderr is None:
            raise RuntimeError("codex app-server did not expose stdio pipes.")
        self.proc = proc

    def close(self) -> None:
        proc = self.proc
        self.proc = None
        if proc is None:
            return

        for stream_name in ("stdin", "stdout", "stderr"):
            stream = getattr(proc, stream_name, None)
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass

        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=_SHUTDOWN_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=_SHUTDOWN_GRACE_SECONDS)

    def initialize(self) -> dict[str, Any]:
        result = self._request(
            "initialize",
            {
                "clientInfo": {
                    "name": _CLIENT_NAME,
                    "version": _CLIENT_VERSION,
                },
                "capabilities": {"experimentalApi": True},
            },
        )
        if not isinstance(result, dict):
            raise RuntimeError("codex app-server initialize returned a non-object response.")
        return result

    def start_thread(self) -> str:
        params: dict[str, Any] = {
            "approvalPolicy": "never",
            "cwd": self.cwd,
            "ephemeral": True,
            "sandbox": "danger-full-access",
        }
        model = _env_lookup(self.env, _MODEL_KEYS)
        if model:
            params["model"] = model
        result = self._request("thread/start", params)
        thread_id = _extract_nested_string(result, ("thread", "id"))
        if thread_id is None:
            raise RuntimeError("codex app-server thread/start response did not include thread.id.")
        return thread_id

    def start_turn(self, *, thread_id: str, prompt: str, output_schema: dict[str, Any]) -> str:
        params: dict[str, Any] = {
            "cwd": self.cwd,
            "input": [{"type": "text", "text": prompt}],
            "outputSchema": output_schema,
            "threadId": thread_id,
        }
        effort = _normalize_reasoning_effort(_env_lookup(self.env, _REASONING_EFFORT_KEYS))
        if effort is not None:
            params["effort"] = effort

        result = self._request("turn/start", params)
        turn_id = _extract_nested_string(result, ("turn", "id"))
        if turn_id is None:
            raise RuntimeError("codex app-server turn/start response did not include turn.id.")
        return turn_id

    def run_structured_turn(
        self,
        *,
        thread_id: str,
        prompt: str,
        output_schema: dict[str, Any],
        telemetry_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        turn_id = self.start_turn(thread_id=thread_id, prompt=prompt, output_schema=output_schema)
        structured_payload: Any | None = None
        latest_token_usage: dict[str, Any] | None = None
        requested_effort = _normalize_reasoning_effort(_env_lookup(self.env, _REASONING_EFFORT_KEYS))

        for notification in self.stream_until_completed(turn_id):
            method = notification.get("method")
            params = notification.get("params")
            if method == "item/completed" and isinstance(params, dict) and params.get("turnId") == turn_id:
                candidate = _find_structured_content(params.get("item"))
                if candidate is not None:
                    structured_payload = candidate
            elif method == "thread/tokenUsage/updated" and isinstance(params, dict):
                if params.get("threadId") == thread_id and params.get("turnId") == turn_id:
                    latest_token_usage = _normalize_thread_token_usage(params.get("tokenUsage"))
            elif method == "turn/completed" and isinstance(params, dict):
                turn = params.get("turn")
                if not isinstance(turn, dict):
                    raise RuntimeError("codex app-server turn/completed notification was missing turn data.")
                if turn.get("status") != "completed":
                    message = _extract_turn_error(turn)
                    raise RuntimeError(f"codex app-server turn failed before structured output was produced: {message}")
                if structured_payload is None:
                    structured_payload = _find_structured_content(turn)

        if structured_payload is None:
            raise RuntimeError("codex app-server completed the turn without emitting structured output.")

        if telemetry_callback is not None:
            telemetry_callback(
                {
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "requested_reasoning_effort": requested_effort or "inherit",
                    "token_usage": latest_token_usage,
                }
            )

        return _normalize_structured_payload(structured_payload)

    def stream_until_completed(self, turn_id: str) -> list[dict[str, Any]]:
        notifications: list[dict[str, Any]] = []
        while True:
            message = self._next_notification()
            notifications.append(message)
            if (
                message.get("method") == "turn/completed"
                and isinstance(message.get("params"), dict)
                and message["params"].get("turn", {}).get("id") == turn_id
            ):
                return notifications

    def _next_notification(self) -> dict[str, Any]:
        if self._pending_notifications:
            return self._pending_notifications.popleft()

        deadline = time.monotonic() + self.timeout_seconds
        while True:
            message = self._read_message(deadline)
            if "method" in message:
                return message
            raise RuntimeError(f"Unexpected JSON-RPC response while waiting for a notification: {message!r}")

    def _request(self, method: str, params: dict[str, Any]) -> Any:
        proc = self._require_proc()
        self._request_id += 1
        request_id = self._request_id
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        assert proc.stdin is not None
        proc.stdin.write(json.dumps(payload) + "\n")
        proc.stdin.flush()

        deadline = time.monotonic() + self.timeout_seconds
        while True:
            message = self._read_message(deadline)
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"codex app-server {method} failed: {message['error']!r}")
                return message.get("result")
            if "method" in message:
                self._pending_notifications.append(message)
                continue
            raise RuntimeError(f"Unexpected JSON-RPC response while waiting for {method}: {message!r}")

    def _read_message(self, deadline: float) -> dict[str, Any]:
        proc = self._require_proc()
        assert proc.stdout is not None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Timed out waiting for codex app-server output.")

            self._wait_for_stdout(proc.stdout, remaining, deadline)

            line = proc.stdout.readline()
            if line == "":
                stderr_tail = self._stderr_tail()
                exited = proc.poll()
                detail = f" (exit={exited})" if exited is not None else ""
                if stderr_tail:
                    detail += f": {stderr_tail}"
                raise RuntimeError(f"codex app-server stream ended unexpectedly{detail}")

            text = line.strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"codex app-server emitted invalid JSON: {exc}") from exc
            if not isinstance(message, dict):
                raise RuntimeError(f"codex app-server emitted a non-object JSON-RPC message: {type(message).__name__}")
            return message

    def _wait_for_stdout(self, stdout: Any, remaining: float, deadline: float) -> None:
        """Block until ``stdout`` has data or the deadline passes.

        ``select`` is the only way to bound a blocking ``readline()`` against a
        process that stays alive but stops emitting. Test fakes don't have a
        real fileno, so we fall back to letting readline() return — they
        either return data immediately or signal EOF.
        """
        try:
            fd = stdout.fileno()
        except (AttributeError, OSError, ValueError):
            return
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            raise RuntimeError("Timed out waiting for codex app-server output.")

    def _stderr_tail(self) -> str:
        proc = self._require_proc()
        if proc.poll() is None or proc.stderr is None:
            return ""
        try:
            return proc.stderr.read().strip()
        except OSError:
            return ""

    def _require_proc(self) -> subprocess.Popen[str]:
        if self.proc is None:
            raise RuntimeError("codex app-server process has not been started.")
        return self.proc


def close_shared_clients() -> None:
    with _shared_clients_lock:
        entries = list(_shared_clients.values())
        _shared_clients.clear()

    for entry in entries:
        entry.client.close()


def _normalize_structured_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Structured output was not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Structured output must be a JSON object, got {type(payload).__name__}.")
    return payload


def _find_structured_content(value: Any) -> Any | None:
    if isinstance(value, dict):
        if "structuredContent" in value and value["structuredContent"] is not None:
            return value["structuredContent"]
        if value.get("type") == "agentMessage" and isinstance(value.get("text"), str) and value["text"].strip():
            return value["text"]
        for nested in value.values():
            found = _find_structured_content(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_structured_content(nested)
            if found is not None:
                return found
    return None


def _normalize_token_usage_breakdown(value: Any) -> dict[str, int] | None:
    if not isinstance(value, dict):
        return None
    normalized: dict[str, int] = {}
    mapping = {
        "cached_input_tokens": "cachedInputTokens",
        "input_tokens": "inputTokens",
        "output_tokens": "outputTokens",
        "reasoning_output_tokens": "reasoningOutputTokens",
        "total_tokens": "totalTokens",
    }
    for target, source in mapping.items():
        candidate = value.get(source)
        if not isinstance(candidate, int):
            return None
        normalized[target] = candidate
    return normalized


def _normalize_thread_token_usage(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    last = _normalize_token_usage_breakdown(value.get("last"))
    total = _normalize_token_usage_breakdown(value.get("total"))
    if last is None or total is None:
        return None
    model_context_window = value.get("modelContextWindow")
    if model_context_window is not None and not isinstance(model_context_window, int):
        model_context_window = None
    return {
        "last": last,
        "total": total,
        "model_context_window": model_context_window,
    }


def _extract_nested_string(value: Any, path: tuple[str, ...]) -> str | None:
    current = value
    for segment in path:
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    if isinstance(current, str) and current.strip():
        return current
    return None


def _extract_turn_error(turn: dict[str, Any]) -> str:
    error = turn.get("error")
    if not isinstance(error, dict):
        return "unknown turn failure"
    message = error.get("message")
    if isinstance(message, str) and message.strip():
        return message
    details = error.get("additionalDetails")
    if isinstance(details, str) and details.strip():
        return details
    return "unknown turn failure"


def _env_lookup(env: Mapping[str, str] | None, keys: tuple[str, ...]) -> str | None:
    if env is None:
        return None
    for key in keys:
        value = env.get(key)
        if value:
            return str(value)
    return None


def _resolve_codex_bin(explicit: str | None, env: Mapping[str, str] | None) -> str:
    explicit_text = str(explicit or "").strip()
    if explicit_text and explicit_text != "codex":
        return explicit_text

    env_override = _env_lookup(env, _CODEX_BIN_KEYS)
    if env_override is not None and env_override.strip():
        return env_override.strip()

    search_path = None
    if env and env.get("PATH"):
        search_path = str(env["PATH"])
    discovered = shutil.which("codex", path=search_path)
    if discovered:
        return discovered

    home = Path.home()
    for template in _CODEX_SEARCH_PATHS:
        candidate = Path(template.format(home=home))
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    raise RuntimeError(
        "codex binary not found for codex-subagent bridge. Install Codex, add it to PATH, or set CODEX_BIN."
    )


def _normalize_reasoning_effort(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized not in _VALID_REASONING_EFFORTS:
        raise RuntimeError(f"Unsupported reasoning effort '{value}'. Valid values: low, medium, high, xhigh.")
    return normalized


def _shared_session_requested(env: Mapping[str, str] | None) -> bool:
    value = _env_lookup(env, _SESSION_MODE_KEYS)
    if value is None:
        return False
    return value.strip().lower() in _SHARED_SESSION_VALUES


def _shared_session_key(cwd: str, env: Mapping[str, str] | None) -> tuple[str, tuple[tuple[str, str], ...]]:
    filtered: dict[str, str] = {}
    if env:
        for key in (*_MODEL_KEYS, *_REASONING_EFFORT_KEYS, *_SESSION_MODE_KEYS, *_CODEX_BIN_KEYS):
            value = env.get(key)
            if value:
                filtered[key] = str(value)
    return (cwd, tuple(sorted(filtered.items())))


def _run_subagent_shared(
    prompt: str,
    schema: dict[str, Any],
    cwd: str,
    env: dict[str, str] | None,
    telemetry_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    entry = _get_or_create_shared_client(cwd=cwd, env=env)
    try:
        with entry.lock:
            thread_id = entry.client.start_thread()
            return entry.client.run_structured_turn(
                thread_id=thread_id,
                prompt=prompt,
                output_schema=schema,
                telemetry_callback=telemetry_callback,
            )
    except Exception:
        _discard_shared_client(cwd=cwd, env=env, entry=entry)
        raise


def _get_or_create_shared_client(cwd: str, env: Mapping[str, str] | None) -> _SharedClientEntry:
    key = _shared_session_key(cwd, env)
    with _shared_clients_lock:
        entry = _shared_clients.get(key)
        if entry is not None:
            return entry

        client = AppServerClient(cwd=cwd, env=env)
        client.start()
        client.initialize()
        entry = _SharedClientEntry(client=client)
        _shared_clients[key] = entry
        return entry


def _discard_shared_client(cwd: str, env: Mapping[str, str] | None, entry: _SharedClientEntry) -> None:
    key = _shared_session_key(cwd, env)
    with _shared_clients_lock:
        existing = _shared_clients.get(key)
        if existing is entry:
            _shared_clients.pop(key, None)
    entry.client.close()


atexit.register(close_shared_clients)
