"""Best-effort main-agent turn token reader (internal / PR-0094-06).

Harness-neutral: resolve the main-agent transcript from whichever transcript
env var is set (``*_SESSION_TRANSCRIPT_PATH`` pattern + known aliases mirrored
from the handoff attribution / compaction contract layer). Never hard-codes
vendor branching in the usage-extraction logic.

Returns the **per-turn aggregate**: usage summed across every token-bearing
API call since the current turn's user prompt (multi-tool-round turns make
many API calls; the last record alone badly understates the turn). Records
sharing one ``message.id`` are one API call (Claude Code writes one JSONL
record per content block, each repeating the same usage) and are counted
once. Otherwise ``None`` → callers render ``main-agent: unavailable``.
Never raises, never blocks.

Production gap + fallback (REV-S3-03): the ``*_SESSION_TRANSCRIPT_PATH`` env
vars are exported only into hook subprocess envs — the MCP server process the
offload pass runs in never sees them. When no env var resolves, a bounded
best-effort fallback picks the newest ``*.jsonl`` in the Claude Code project
transcript dir for the current cwd (``~/.claude/projects/<munged-cwd>/``,
the ``fallback_glob`` posture from harness-protocol.yaml). Non-recursive,
mtime-newest, tail-bounded read; may attribute a concurrent session's
transcript — acceptable for a best-effort display figure.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

# Mirror handoff / session_heartbeat / harness-protocol transcript discovery
# (docs/workbay/contracts/harness-protocol.yaml). Listed for first-pass order;
# any additional ``*_SESSION_TRANSCRIPT_PATH`` env is also accepted.
_KNOWN_TRANSCRIPT_ENV_VARS: tuple[str, ...] = (
    "CLAUDE_SESSION_TRANSCRIPT_PATH",
    "CODEX_SESSION_TRANSCRIPT_PATH",
    "GROK_SESSION_TRANSCRIPT_PATH",
    "VSCODE_TARGET_SESSION_LOG",
)

# Cap how much of a large transcript we scan from the end (best-effort, non-blocking).
_MAX_SCAN_BYTES = 2 * 1024 * 1024
_MAX_SCAN_LINES = 4000


def resolve_main_agent_transcript_path(
    env: Mapping[str, str] | None = None,
) -> Path | None:
    """Return the first set harness transcript path, or ``None``.

    Resolution order:
    1. Known transcript env vars (handoff posture).
    2. Any other ``*_SESSION_TRANSCRIPT_PATH`` key present in the env.
    """
    source = env if env is not None else os.environ
    for key in _KNOWN_TRANSCRIPT_ENV_VARS:
        raw = str(source.get(key, "") or "").strip()
        if raw:
            return Path(raw)
    try:
        for key, value in source.items():
            if key in _KNOWN_TRANSCRIPT_ENV_VARS:
                continue
            if key.endswith("_SESSION_TRANSCRIPT_PATH"):
                raw = str(value or "").strip()
                if raw:
                    return Path(raw)
    except Exception:  # noqa: BLE001 — never block
        return None
    return None


def resolve_fallback_transcript_path(
    *,
    cwd: str | Path | None = None,
    projects_root: str | Path | None = None,
) -> Path | None:
    """Bounded fallback when no transcript env var is set (MCP server process).

    Newest ``*.jsonl`` in ``<projects_root>/<munged-cwd>/`` where the munged
    cwd replaces every non-alphanumeric character with ``-`` (Claude Code's
    project-dir naming). Non-recursive; returns ``None`` when the directory or
    candidates are absent. Never raises.
    """
    try:
        root = Path(projects_root) if projects_root is not None else Path.home() / ".claude" / "projects"
        base = Path(cwd) if cwd is not None else Path.cwd()
        project_dir = root / re.sub(r"[^A-Za-z0-9-]", "-", str(base))
        if not project_dir.is_dir():
            return None
        candidates = [p for p in project_dir.iterdir() if p.suffix == ".jsonl" and p.is_file()]
        if not candidates:
            return None
        return max(candidates, key=lambda p: p.stat().st_mtime)
    except Exception:  # noqa: BLE001 — best-effort contract: never raise
        return None


def _as_nonneg_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value >= 0 and value == int(value):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _normalize_usage_dict(usage: Mapping[str, Any]) -> dict[str, Any] | None:
    """Normalize a token-bearing mapping into a stable shape, or None."""
    input_tokens = _as_nonneg_int(usage.get("input_tokens") if "input_tokens" in usage else usage.get("prompt_tokens"))
    output_tokens = _as_nonneg_int(
        usage.get("output_tokens") if "output_tokens" in usage else usage.get("completion_tokens")
    )
    total_tokens = _as_nonneg_int(usage.get("total_tokens"))
    cached = _as_nonneg_int(
        usage.get("cached_input_tokens") if "cached_input_tokens" in usage else usage.get("cache_read_input_tokens")
    )
    if total_tokens is None and (input_tokens is not None or output_tokens is not None):
        total_tokens = (input_tokens or 0) + (output_tokens or 0)
    if input_tokens is None and output_tokens is None and total_tokens is None:
        return None
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "cached_input_tokens": cached,
        "usage_source": "observed",
    }


def _extract_usage_from_record(obj: Any) -> dict[str, Any] | None:
    """Pull a token-bearing field from one transcript JSON object.

    Real Claude Code JSONL shape (discovered 2026-07-09)::

        {"type": "assistant", "message": {"usage": {
            "input_tokens": N, "output_tokens": M,
            "cache_read_input_tokens": C, ...
        }}}

    Also accepts flat ``usage`` objects and top-level token fields (codex-style
    / synthetic fixtures) without vendor-specific branches.
    """
    if not isinstance(obj, dict):
        return None
    message = obj.get("message")
    if isinstance(message, dict):
        nested = message.get("usage")
        if isinstance(nested, dict):
            normalized = _normalize_usage_dict(nested)
            if normalized is not None:
                return normalized
    top_usage = obj.get("usage")
    if isinstance(top_usage, dict):
        normalized = _normalize_usage_dict(top_usage)
        if normalized is not None:
            return normalized
    # Flat token fields on the record itself.
    if any(k in obj for k in ("input_tokens", "output_tokens", "total_tokens", "prompt_tokens")):
        return _normalize_usage_dict(obj)
    return None


def _is_turn_boundary_record(obj: Any) -> bool:
    """True when ``obj`` is a genuine user prompt (starts a turn).

    Real Claude Code shape (inspected 2026-07-09): tool results also arrive as
    ``type == "user"`` records, but their ``message.content`` is exclusively
    ``tool_result`` blocks — those are mid-turn, not boundaries. ``isMeta``
    records are harness injections, not prompts.
    """
    if not isinstance(obj, dict) or obj.get("type") != "user" or obj.get("isMeta"):
        return False
    message = obj.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if isinstance(content, str):
        return bool(content.strip())
    if isinstance(content, list):
        return any(not (isinstance(block, dict) and block.get("type") == "tool_result") for block in content)
    return False


def _read_tail_lines(path: Path) -> list[str]:
    """Read up to ``_MAX_SCAN_LINES`` from the end of ``path`` without loading all of it."""
    size = path.stat().st_size
    if size <= 0:
        return []
    with path.open("rb") as fh:
        if size > _MAX_SCAN_BYTES:
            fh.seek(max(0, size - _MAX_SCAN_BYTES))
            # Drop partial first line after seek.
            fh.readline()
        raw = fh.read()
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) > _MAX_SCAN_LINES:
        lines = lines[-_MAX_SCAN_LINES:]
    return lines


def _sum_field(calls: list[dict[str, Any]], key: str) -> int | None:
    values = [v for c in calls if (v := c.get(key)) is not None]
    return sum(values) if values else None


def read_main_agent_turn_tokens(
    *,
    env: Mapping[str, str] | None = None,
    transcript_path: str | Path | None = None,
    fallback_projects_root: str | Path | None = None,
    cwd: str | Path | None = None,
) -> dict[str, Any] | None:
    """Best-effort per-turn main-agent usage from the harness transcript.

    Aggregates every token-bearing API call since the current turn's user
    prompt (turn boundary: genuine ``type=="user"`` prompt record — see
    :func:`_is_turn_boundary_record`; records sharing a ``message.id`` count
    once). Returns ``{input_tokens, output_tokens, total_tokens,
    cached_input_tokens, usage_source, api_calls}`` or ``None`` when the
    current turn has no token-bearing record.

    Resolution order: explicit ``transcript_path`` → transcript env vars →
    bounded ``~/.claude/projects/<munged-cwd>/`` fallback (see module
    docstring; ``fallback_projects_root``/``cwd`` exist for tests).

    Never raises (PR-0094-06).
    """
    try:
        if transcript_path is not None:
            path: Path | None = Path(transcript_path)
        else:
            path = resolve_main_agent_transcript_path(env)
            if path is None:
                path = resolve_fallback_transcript_path(cwd=cwd, projects_root=fallback_projects_root)
        if path is None or not path.is_file():
            return None
        lines = _read_tail_lines(path)
        # Walk newest → oldest, collecting API-call usage until the turn
        # boundary (the user prompt that started the current turn).
        calls_by_id: dict[str, dict[str, Any]] = {}
        for idx, line in enumerate(reversed(lines)):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if _is_turn_boundary_record(obj):
                break
            usage = _extract_usage_from_record(obj)
            if usage is None:
                continue
            message = obj.get("message") if isinstance(obj, dict) else None
            message_id = message.get("id") if isinstance(message, dict) else None
            key = str(message_id) if message_id else f"__record_{idx}"
            calls_by_id.setdefault(key, usage)
        if not calls_by_id:
            return None
        calls = list(calls_by_id.values())
        return {
            "input_tokens": _sum_field(calls, "input_tokens"),
            "output_tokens": _sum_field(calls, "output_tokens"),
            "total_tokens": _sum_field(calls, "total_tokens"),
            "cached_input_tokens": _sum_field(calls, "cached_input_tokens"),
            "usage_source": "observed",
            "api_calls": len(calls),
        }
    except Exception:  # noqa: BLE001 — best-effort contract: never raise
        return None
