"""Tool invocation adapters for workbay_handoff_mcp.

Extracted from _shared.py (implementation note of internal). Contains:
  - _resolve_awaitable: run an awaitable from a sync context
  - _normalize_tool_result: coerce MCP tool results to str
  - Protocol definitions: _FnWrappedTool, _FunctionWrappedTool, _FuncWrappedTool, _RunnableTool
  - _unwrap_tool_candidate: peel wrapper attributes off wrapped tool objects
  - _invoke_tool: dispatch a tool call regardless of its concrete type

All symbols are re-exported from _shared.py for backward compatibility.
"""

from __future__ import annotations

import asyncio
import inspect
from threading import Thread
from typing import Any, Awaitable, Protocol, runtime_checkable


def _resolve_awaitable(value: object) -> object:
    if not inspect.isawaitable(value):
        return value

    async def _await_value(awaitable: Awaitable[Any]) -> Any:
        return await awaitable

    awaitable: Awaitable[Any] = value
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_await_value(awaitable))
    box: dict[str, object] = {}

    def _runner() -> None:
        try:
            box["value"] = asyncio.run(_await_value(awaitable))
        except Exception as exc:
            box["error"] = exc
            box["traceback"] = exc.__traceback__

    thread = Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in box:
        error = box["error"]
        if isinstance(error, Exception) and box.get("traceback") is not None:
            error = error.with_traceback(box["traceback"])  # type: ignore[arg-type]
        raise error  # type: ignore[misc]
    return box.get("value")


def _normalize_tool_result(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    structured_content = getattr(value, "structured_content", None)
    if isinstance(structured_content, dict):
        structured_result = structured_content.get("result")
        if isinstance(structured_result, str):
            return structured_result
    text = getattr(value, "text", None)
    if isinstance(text, str):
        return text
    content = getattr(value, "content", None)
    if isinstance(content, list):
        parts = [item.text for item in content if isinstance(getattr(item, "text", None), str)]
        if parts:
            return "\n".join(parts)
    return str(value)


@runtime_checkable
class _FnWrappedTool(Protocol):
    fn: object


@runtime_checkable
class _FunctionWrappedTool(Protocol):
    function: object


@runtime_checkable
class _FuncWrappedTool(Protocol):
    func: object


@runtime_checkable
class _RunnableTool(Protocol):
    def run(self, arguments: dict[str, object]) -> object: ...


def _unwrap_tool_candidate(candidate: object) -> object | None:
    if isinstance(candidate, _FnWrappedTool) and candidate.fn is not candidate:
        return candidate.fn
    if isinstance(candidate, _FunctionWrappedTool) and candidate.function is not candidate:
        return candidate.function
    if isinstance(candidate, _FuncWrappedTool) and candidate.func is not candidate:
        return candidate.func
    return None


def _invoke_tool(tool: object, **kwargs: object) -> str:
    candidate: object = tool
    visited: set[int] = set()
    for _ in range(10):
        if inspect.isawaitable(candidate):
            candidate = _resolve_awaitable(candidate)
            continue
        if callable(candidate):
            return _normalize_tool_result(_resolve_awaitable(candidate(**kwargs)))
        marker = id(candidate)
        if marker in visited:
            break
        visited.add(marker)
        unwrapped = _unwrap_tool_candidate(candidate)
        if unwrapped is not None:
            candidate = unwrapped
            continue
        if isinstance(candidate, _RunnableTool):
            return _normalize_tool_result(_resolve_awaitable(candidate.run(kwargs)))
        break
    raise TypeError(f"Unable to invoke tool of type {type(tool).__name__}")
