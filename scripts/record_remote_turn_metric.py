#!/usr/bin/env python3
"""Record observed usage from a remote gate on the WorkBay host."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

_TOKEN_FIELDS = {
    "input_tokens": ("input_tokens", "inputTokens"),
    "output_tokens": ("output_tokens", "outputTokens"),
    "cached_input_tokens": (
        "cached_input_tokens",
        "cache_read_tokens",
        "cachedInputTokens",
        "cachedReadTokens",
    ),
    "reasoning_output_tokens": (
        "reasoning_output_tokens",
        "reasoning_tokens",
        "thinking_tokens",
        "reasoningOutputTokens",
        "reasoningTokens",
    ),
    "total_tokens": ("total_tokens", "totalTokens"),
}


def _token_count(value: object) -> int:
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if type(value) is not int or value < 0:
        raise ValueError("expected a non-negative integer")  # noqa: TRY003
    return value


def _model_name(value: object) -> str | None:
    return (value.strip() or None) if isinstance(value, str) else None


def _derived_total(counts: dict[str, int], *, codex: bool) -> int | None:
    if "input_tokens" not in counts or "output_tokens" not in counts:
        return None
    reasoning = counts.get("reasoning_output_tokens", 0) if codex else 0
    return counts["input_tokens"] + counts["output_tokens"] + reasoning


def parse_usage(payload: str) -> tuple[dict[str, int], str | None]:
    """Read an agy JSON object or the cumulative usage in a Codex JSONL stream."""
    try:
        events = [json.loads(payload)]
    except json.JSONDecodeError:
        events = [json.loads(line) for line in payload.splitlines() if line.strip()]
    if not events or not all(isinstance(event, dict) for event in events):
        raise ValueError("expected JSON objects")  # noqa: TRY003

    model = None
    fallback: dict[str, int] = {}
    completed: dict[str, int] = {}
    for event in events:
        model = _model_name(event.get("model")) or model
        usage = event.get("usage")
        if not isinstance(usage, dict):
            continue
        model = _model_name(usage.get("model")) or model
        counts: dict[str, int] = {}
        for field, aliases in _TOKEN_FIELDS.items():
            value = next((usage[key] for key in aliases if usage.get(key) is not None), None)
            if value is not None:
                counts[field] = _token_count(value)
        if not counts:
            continue
        if event.get("type") == "turn.completed":
            # The latest completion contains Codex's cumulative counters.
            completed = counts
        else:
            fallback = counts
    return completed or fallback, model


def _record_metric(args: argparse.Namespace, counts: dict[str, int], model: str | None) -> dict:
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime

    config = RuntimeConfig.for_repo(args.workspace_root)
    configure_runtime(config)
    try:
        from workbay_orchestrator_mcp.lanes import TokenUsage, record_turn_metric
    except ModuleNotFoundError as exc:
        if exc.name not in {"workbay_orchestrator_mcp", "workbay_orchestrator_mcp.lanes"}:
            raise
        db_uri = config.db_path.as_uri() + "?mode=rw"
        with closing(sqlite3.connect(db_uri, uri=True)) as conn, conn:
            cursor = conn.execute(
                """
                INSERT INTO turn_metrics (
                    task_ref, lane_id, session, phase, backend, model,
                    input_tokens, output_tokens, cached_input_tokens, reasoning_output_tokens,
                    total_tokens, usage_source, duration_seconds, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'observed', ?, datetime('now'))
                """,
                (
                    args.task_ref,
                    args.lane_id,
                    args.session,
                    args.phase,
                    args.backend,
                    model,
                    counts.get("input_tokens"),
                    counts.get("output_tokens"),
                    counts.get("cached_input_tokens"),
                    counts.get("reasoning_output_tokens"),
                    counts["total_tokens"],
                    args.duration_seconds,
                ),
            )
            return {
                "id": cursor.lastrowid,
                "task_ref": args.task_ref,
                "backend": args.backend,
                "model": model,
                "total_tokens": counts["total_tokens"],
                "usage_source": "observed",
            }

    result = record_turn_metric(
        session=args.session,
        phase=args.phase,
        backend=args.backend,
        lane_id=args.lane_id,
        model=model,
        task_ref=args.task_ref,
        token_usage=TokenUsage(**counts, usage_source="observed"),
        duration_seconds=args.duration_seconds,
    )
    if not result["ok"]:
        raise ValueError(result["error"])
    return result["turn_metric"]


def _nonempty(value: str) -> str:
    if not value.strip():
        raise argparse.ArgumentTypeError("empty value")  # noqa: TRY003
    return value.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-ref", required=True, type=_nonempty)
    parser.add_argument("--lane-id", type=_nonempty)
    parser.add_argument("--session", default="remote-gate", type=_nonempty)
    parser.add_argument("--phase", default="merge_gate", type=_nonempty)
    parser.add_argument("--backend", required=True, type=_nonempty)
    parser.add_argument("--model", type=_nonempty)
    parser.add_argument("--from-json", metavar="PATH", help="agy JSON or Codex JSONL, or - for stdin")
    for field in _TOKEN_FIELDS:
        parser.add_argument("--" + field.replace("_", "-"), type=_token_count)
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--workspace-root", default=Path.cwd(), type=Path)
    parser.add_argument("--json", action="store_true", help="Emit a compact JSON receipt (the default)")
    args = parser.parse_args(argv)
    duration = args.duration_seconds
    if duration is not None and (not math.isfinite(duration) or duration < 0):
        parser.error("--duration-seconds must be finite and non-negative")

    try:
        counts, model = {}, None
        if args.from_json is not None:
            payload = sys.stdin.read() if args.from_json == "-" else Path(args.from_json).read_text(encoding="utf-8")
            counts, model = parse_usage(payload)
        counts.update({field: value for field in _TOKEN_FIELDS if (value := getattr(args, field)) is not None})
        if "total_tokens" not in counts:
            total = _derived_total(counts, codex=args.backend.startswith("codex"))
            if total is None:
                parser.error("supply an observed total or both input and output token counts")
            counts["total_tokens"] = total
        row = _record_metric(args, counts, args.model or model)
    except (OSError, ValueError, ImportError, OverflowError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")

    fields = ("id", "task_ref", "backend", "model", "total_tokens", "usage_source")
    print(json.dumps({"ok": True, **{field: row[field] for field in fields}}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
