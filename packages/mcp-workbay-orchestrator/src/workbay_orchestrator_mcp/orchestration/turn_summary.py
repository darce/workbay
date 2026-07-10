"""Turn-end main-agent + subagent token presentation (internal).

Buckets subagent usage by ``usage_source``. ``grok_context_delta`` is a
different unit (cumulative context fill) and is rendered on its own labeled
line (``grok context (approx)``); it is **never** summed into observed
input/output totals (PR-0094-05). Missing sources render an explicit
``unavailable`` line.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (
    USAGE_SOURCE_GROK_CONTEXT_DELTA,
)

#: Human label for grok context-delta lines (never mixed into observed totals).
GROK_CONTEXT_APPROX_LABEL = "grok context (approx)"

# Sources that contribute to the observed-style (input/output) total.
_OBSERVED_STYLE_SOURCES = frozenset({"observed", "tokenizer_estimate", "char_estimate"})


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == int(value):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _format_observed_counts(entry: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key, label in (
        ("input_tokens", "input"),
        ("output_tokens", "output"),
        ("total_tokens", "total"),
    ):
        val = _as_int(entry.get(key))
        if val is not None:
            parts.append(f"{label}={val}")
    return " ".join(parts) if parts else "total=0"


def _is_pending_reason(reason: Any) -> bool:
    text = str(reason or "").lower()
    return "pending" in text


def render_turn_token_summary(
    main: Mapping[str, Any] | None,
    subagents: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Format main-agent + per-lane subagent token lines for turn end.

    Parameters
    ----------
    main:
        Main-agent usage dict (``input_tokens`` / ``output_tokens`` /
        ``total_tokens``) or ``None`` → ``main-agent: unavailable``.
    subagents:
        Sequence of per-lane entries::

            {
              "lane_id": str,
              "usage_source": "observed" | "grok_context_delta" | ...,
              "total_tokens": int | None,
              "input_tokens": int | None,   # optional
              "output_tokens": int | None,  # optional
              "reason": str | None,         # e.g. "unavailable (pending flush)"
            }

        Missing / null source or a null ``total_tokens`` (with or without a
        reason) → explicit per-entry ``unavailable`` line; token fields are
        never coerced from ``None``. ``grok_context_delta`` is never folded into
        ``observed_total``.

    Returns a structured dict with ``text``, ``lines``, bucket totals, and
    flags suitable for the offload pass-end payload.
    """
    lines: list[str] = []

    if main is None:
        lines.append("main-agent: unavailable")
        main_available = False
    else:
        lines.append(f"main-agent: {_format_observed_counts(main)}")
        main_available = True

    tokens_by_usage_source: dict[str, int] = {
        "observed": 0,
        "tokenizer_estimate": 0,
        "char_estimate": 0,
        USAGE_SOURCE_GROK_CONTEXT_DELTA: 0,
    }
    observed_total = 0
    grok_approx_total = 0
    unavailable_lanes: list[str] = []

    for raw in subagents or ():
        if not isinstance(raw, Mapping):
            continue
        lane = str(raw.get("lane_id") or "unknown").strip() or "unknown"
        source_raw = raw.get("usage_source")
        source = str(source_raw).strip() if source_raw is not None else None
        if source == "":
            source = None
        total = _as_int(raw.get("total_tokens"))
        reason = raw.get("reason")

        # Unavailable per-entry: no source, or no usable total (regardless of
        # whether a reason was attached — a null total must never reach
        # ``int(None)`` below; REV-S3-01). Grok with no total yet is the
        # pending-flush / missing-session-dir case.
        if source is None or total is None:
            if _is_pending_reason(reason) or (source == USAGE_SOURCE_GROK_CONTEXT_DELTA and total is None):
                lines.append(f"subagent {lane}: unavailable (pending flush)")
            else:
                lines.append(f"subagent {lane}: unavailable")
            unavailable_lanes.append(lane)
            continue

        if source == USAGE_SOURCE_GROK_CONTEXT_DELTA:
            # Different unit — own labeled line; never add to observed_total.
            n = int(total)
            grok_approx_total += n
            tokens_by_usage_source[USAGE_SOURCE_GROK_CONTEXT_DELTA] = (
                tokens_by_usage_source.get(USAGE_SOURCE_GROK_CONTEXT_DELTA, 0) + n
            )
            lines.append(f"subagent {lane}: {GROK_CONTEXT_APPROX_LABEL}={n}")
            continue

        # Observed-style (or estimate) sources: format counts; bucket by source.
        n = int(total)
        if source in tokens_by_usage_source:
            tokens_by_usage_source[source] = tokens_by_usage_source.get(source, 0) + n
        else:
            tokens_by_usage_source[source] = n
        if source in _OBSERVED_STYLE_SOURCES or source == "observed":
            # Only pure "observed" contributes to the "observed total" line;
            # estimates stay in their by-source bucket (lanes.py sums estimates
            # into total_tokens_total but labels them separately — we keep
            # observed total = observed only so the label stays honest).
            if source == "observed":
                observed_total += n
        lines.append(f"subagent {lane}: {_format_observed_counts(raw)} source={source}")

    lines.append(f"observed total: {observed_total}")
    if grok_approx_total:
        lines.append(f"{GROK_CONTEXT_APPROX_LABEL} total: {grok_approx_total}")

    return {
        "text": "\n".join(lines),
        "lines": lines,
        "main_agent_available": main_available,
        "observed_total": observed_total,
        "grok_context_approx_total": grok_approx_total,
        "total_tokens_by_usage_source": tokens_by_usage_source,
        "unavailable_lanes": unavailable_lanes,
    }
