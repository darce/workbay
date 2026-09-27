"""Deterministic token estimation (implementation note S2).

Pure local computation — no LLM calls. Prefers tiktoken ``o200k_base``;
falls back to ``len(text) / 4``. Method strings map to provenance labels
already valid for ``usage_source`` / ``prompt_token_source``:

    tiktoken:o200k_base → tokenizer_estimate
    chars_div_4         → char_estimate

Also extracts model-output text from session ``updates.jsonl`` for
usage-less backends: ``agent_message_chunk`` + ``agent_thought_chunk``
text plus ``tool_call`` arguments. ``tool_call_update`` results are
excluded (tool output, not model output).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

#: Estimator method strings returned by :func:`estimate_token_count`.
METHOD_TIKTOKEN_O200K = "tiktoken:o200k_base"
METHOD_CHARS_DIV_4 = "chars_div_4"

#: Method → provenance label (valid ``usage_source`` / ``prompt_token_source``).
METHOD_TO_PROVENANCE: Mapping[str, str] = {
    METHOD_TIKTOKEN_O200K: "tokenizer_estimate",
    METHOD_CHARS_DIV_4: "char_estimate",
}

#: ``sessionUpdate`` kinds counted as model output (implementation note S2).
_MODEL_OUTPUT_UPDATE_KINDS = frozenset(
    {
        "agent_message_chunk",
        "agent_thought_chunk",
        "tool_call",
    }
)

#: Explicitly excluded even if nesting drifts (tool results, not model output).
_EXCLUDED_UPDATE_KINDS = frozenset({"tool_call_update"})


def estimate_token_count(text: str) -> tuple[int, str]:
    """Return ``(count, method)`` for *text*.

    Method is ``tiktoken:o200k_base`` when tiktoken is available, else
    ``chars_div_4``. Matches the historical tools-snapshot estimator.
    """
    try:
        import tiktoken  # type: ignore

        encoding = tiktoken.get_encoding("o200k_base")
        return len(encoding.encode(text)), METHOD_TIKTOKEN_O200K
    except Exception:
        return max(1, round(len(text) / 4)), METHOD_CHARS_DIV_4


def method_to_provenance(method: str) -> str:
    """Map an estimator method string to a provenance label.

    Raises ``KeyError`` for unknown methods so the mapping stays total over
    the known method set (tested).
    """
    try:
        return METHOD_TO_PROVENANCE[method]
    except KeyError as exc:
        raise KeyError(f"unknown token-estimate method: {method!r}") from exc


def estimate_with_provenance(text: str) -> tuple[int, str]:
    """Return ``(count, provenance)`` where provenance is tokenizer/char estimate."""
    count, method = estimate_token_count(text)
    return count, method_to_provenance(method)


def estimate_prompt_metrics(text: str) -> dict[str, Any]:
    """Estimate prompt tokens/chars from dispatched brief (or full prompt) text.

    Returns a dict with ``prompt_tokens``, ``prompt_chars``,
    ``prompt_token_source`` (provenance label).
    """
    tokens, provenance = estimate_with_provenance(text)
    return {
        "prompt_tokens": tokens,
        "prompt_chars": len(text),
        "prompt_token_source": provenance,
    }


def extract_model_output_text_from_updates(updates_path: Path | str) -> str:
    """Concatenate model-output text from a session ``updates.jsonl``.

    Includes:
    - ``agent_message_chunk`` / ``agent_thought_chunk`` text content
    - ``tool_call`` arguments (``rawInput`` / ``arguments`` / …)

    Excludes ``tool_call_update`` (and any other non-listed kind). Never
    raises — missing/malformed files yield ``""``.
    """
    path = Path(updates_path)
    if not path.is_file():
        return ""
    parts: list[str] = []
    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
                if not isinstance(obj, dict):
                    continue
                update = _update_object(obj)
                if update is None:
                    continue
                kind = update.get("sessionUpdate")
                if not isinstance(kind, str) or kind in _EXCLUDED_UPDATE_KINDS:
                    continue
                if kind not in _MODEL_OUTPUT_UPDATE_KINDS:
                    continue
                if kind in ("agent_message_chunk", "agent_thought_chunk"):
                    text = _chunk_text(update)
                    if text:
                        parts.append(text)
                elif kind == "tool_call":
                    args_text = _tool_call_args_text(update)
                    if args_text:
                        parts.append(args_text)
    except OSError:
        return ""
    return "".join(parts)


def estimate_output_tokens_from_updates(
    updates_path: Path | str,
) -> tuple[int | None, str | None]:
    """Estimate output tokens from ``updates.jsonl``.

    Returns ``(tokens, provenance)`` or ``(None, None)`` when no model-output
    text is available.
    """
    text = extract_model_output_text_from_updates(updates_path)
    if not text:
        return None, None
    tokens, provenance = estimate_with_provenance(text)
    return tokens, provenance


def estimate_output_tokens_from_session_dir(
    session_dir: Path | str,
) -> tuple[int | None, str | None]:
    """Estimate output tokens from a session directory's ``updates.jsonl``."""
    return estimate_output_tokens_from_updates(Path(session_dir) / "updates.jsonl")


def build_token_estimates(
    *,
    prompt_text: str | None = None,
    updates_path: Path | str | None = None,
    session_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Build a ``token_estimates`` payload for adapter/raw_usage attachment.

    Prompt fields come from *prompt_text* when provided. Output fields come
    from *updates_path* or ``session_dir/updates.jsonl``.
    """
    out: dict[str, Any] = {
        "prompt_tokens": None,
        "prompt_chars": None,
        "prompt_token_source": None,
        "output_tokens": None,
        "output_token_source": None,
    }
    if prompt_text is not None:
        metrics = estimate_prompt_metrics(prompt_text)
        out.update(metrics)
    path: Path | None = None
    if updates_path is not None:
        path = Path(updates_path)
    elif session_dir is not None:
        path = Path(session_dir) / "updates.jsonl"
    if path is not None:
        tokens, provenance = estimate_output_tokens_from_updates(path)
        out["output_tokens"] = tokens
        out["output_token_source"] = provenance
    return out


def _update_object(obj: dict[str, Any]) -> dict[str, Any] | None:
    """Pull the nested ``params.update`` (or top-level ``update``) object."""
    params = obj.get("params")
    if isinstance(params, dict):
        update = params.get("update")
        if isinstance(update, dict):
            return update
    update = obj.get("update")
    return update if isinstance(update, dict) else None


def _chunk_text(update: dict[str, Any]) -> str:
    content = update.get("content")
    if isinstance(content, dict):
        text = content.get("text")
        if isinstance(text, str):
            return text
    if isinstance(content, str):
        return content
    return ""


def _tool_call_args_text(update: dict[str, Any]) -> str:
    """Serialize tool_call arguments for token estimation."""
    for key in ("rawInput", "arguments", "input", "args"):
        if key not in update:
            continue
        val = update.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            return val
        try:
            return json.dumps(val, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(val)
    return ""
