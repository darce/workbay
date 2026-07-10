"""Hybrid slice brief packet: semantic (handoff) + structural (codemap) channels.

internal. Structural questions are answered by codebase-graph MCP
queries only — code is **never** embedded into the semantic store
(internal / plan Not-Doing).

Channel contracts:

* **semantic** — ``semantic_reinjection_packet`` (in-process handoff import),
  same degrade shape as :mod:`lane_prompt` (try/except → labeled unavailable).
* **structural** — injectable ``_mcp_codemap_*`` wrappers (tests mock these).
  Production has no in-process codemap client (separate MCP process); wrappers
  raise :class:`CodemapUnavailableError` so the structural channel is omitted
  and the semantic-only brief is returned unchanged.

Normalized wrapper return shapes are **ours** — thin adapters must map any
upstream tool payload into these TypedDicts; production paths do not hardcode
unverified codemap tool keys.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping, Sequence, TypedDict

logger = logging.getLogger(__name__)

# Surfaced on every structural channel so workers treat graph hits as local aid.
LOCAL_AID_CAVEAT = "structural channel is local aid, not ground truth (codemap index may lag the worktree)"


class CodemapUnavailableError(RuntimeError):
    """Raised by production codemap wrappers when the graph MCP is absent."""


# ---------------------------------------------------------------------------
# Public packet shapes (SliceReviewPacket style)
# ---------------------------------------------------------------------------


class SliceAnchor(TypedDict, total=False):
    """One plan-cited path (optionally with a symbol)."""

    path: str
    symbol: str | None


class StructuralAnchorHit(TypedDict):
    path: str
    symbol: str | None
    callers: list[str]
    callees: list[str]
    snippet: str | None
    resolved: bool
    warning: str | None


class StructuralChannel(TypedDict):
    index_timestamp: str | None
    caveat: str
    anchors: list[StructuralAnchorHit]
    warnings: list[str]


class SemanticChannel(TypedDict):
    status: str
    skip_reason: str | None
    relevant_lines: list[str]
    fallback_scope: str | None
    raw: dict[str, Any] | None


class SliceBriefPacket(TypedDict):
    task_ref: str
    semantic: SemanticChannel
    structural: StructuralChannel | None
    warnings: list[str]


# ---------------------------------------------------------------------------
# Normalized codemap wrapper shapes (mock boundary)
# ---------------------------------------------------------------------------


class CodemapIndexStatus(TypedDict, total=False):
    available: bool
    index_timestamp: str | None
    stale: bool
    reason: str | None


class CodemapTraceResult(TypedDict, total=False):
    found: bool
    path: str
    symbol: str | None
    callers: list[str]
    callees: list[str]
    warning: str | None


class CodemapSnippetResult(TypedDict, total=False):
    found: bool
    path: str
    symbol: str | None
    snippet: str | None
    warning: str | None


# ---------------------------------------------------------------------------
# Injectable MCP wrappers (tests patch these names)
# ---------------------------------------------------------------------------


def _mcp_semantic_reinjection_packet(*args: Any, **kwargs: Any) -> Any:
    """In-process late-binding wrapper — mirrors lane_prompt.py."""
    from workbay_handoff_mcp import semantic_reinjection_packet as _packet

    return _packet(*args, **kwargs)


def _mcp_codemap_index_status() -> CodemapIndexStatus:
    """Best-effort codemap index_status.

    No in-process codebase-graph client exists in the orchestrator yet (the MCP
    is a separate process). Production degrades via
    :class:`CodemapUnavailableError`; tests inject a normalized status.
    """
    raise CodemapUnavailableError("codebase-graph MCP not available in-process")


def _mcp_codemap_trace_path(
    *,
    path: str,
    symbol: str | None = None,
    direction: str = "both",
) -> CodemapTraceResult:
    """Best-effort ``trace_path`` (callers/callees blast radius)."""
    del path, symbol, direction  # production stub — no transport yet
    raise CodemapUnavailableError("codebase-graph MCP not available in-process")


def _mcp_codemap_get_code_snippet(
    *,
    path: str,
    symbol: str | None = None,
) -> CodemapSnippetResult:
    """Best-effort ``get_code_snippet`` for a path:symbol anchor."""
    del path, symbol
    raise CodemapUnavailableError("codebase-graph MCP not available in-process")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_anchor(raw: str | Mapping[str, Any]) -> SliceAnchor:
    """Accept ``{path, symbol}`` maps or ``path:symbol`` / bare-path strings."""
    if isinstance(raw, Mapping):
        path = str(raw.get("path") or "").strip()
        symbol_raw = raw.get("symbol")
        symbol: str | None
        if symbol_raw is None:
            symbol = None
        else:
            symbol_s = str(symbol_raw).strip()
            symbol = symbol_s or None
        return {"path": path, "symbol": symbol}

    text = str(raw).strip()
    if not text:
        return {"path": "", "symbol": None}
    # Split on the last colon when the RHS looks like a bare identifier
    # (not a path segment). Windows drive letters (C:\...) keep a single letter
    # before the colon and are left unsplit via the length check on ``left``.
    if ":" in text:
        left, right = text.rsplit(":", 1)
        right = right.strip()
        left = left.strip()
        if left and right and "/" not in right and "\\" not in right and len(left) > 1:
            return {"path": left, "symbol": right}
    return {"path": text, "symbol": None}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str):
            cleaned = item.strip()
            if cleaned:
                out.append(cleaned)
        elif item is not None:
            cleaned = str(item).strip()
            if cleaned:
                out.append(cleaned)
    return out


def clean_semantic_relevant_lines(raw_lines: Any) -> list[str]:
    """Single source (REV-HARM-A-001) for cleaning semantic_reinjection
    ``relevant_lines``: drop the packet's ``relevant:`` header and any leading
    ``- `` bullet so callers can re-wrap the concept bodies. Consumed here and by
    lane_prompt.py (which imports this to avoid a clone that must change together).
    """
    if not isinstance(raw_lines, list):
        return []
    cleaned: list[str] = []
    for raw in raw_lines:
        item = str(raw).strip()
        if not item or item.lower() == "relevant:":
            continue
        if item.startswith("- "):
            item = item[2:].strip()
        if item:
            cleaned.append(item)
    return cleaned


def _semantic_channel_from_payload(payload: Mapping[str, Any] | None, *, error: str | None = None) -> SemanticChannel:
    if error is not None or payload is None:
        reason = error or "unavailable"
        return {
            "status": "unavailable",
            "skip_reason": reason,
            "relevant_lines": [f"relevant concepts: (unavailable: {reason})"],
            "fallback_scope": None,
            "raw": None,
        }

    status = str(payload.get("status") or "").strip() or "unavailable"
    skip_reason_raw = payload.get("skip_reason")
    skip_reason = str(skip_reason_raw).strip() if skip_reason_raw is not None else None
    if skip_reason == "":
        skip_reason = None

    reinjection = payload.get("semantic_reinjection")
    fallback_scope: str | None = None
    if isinstance(reinjection, Mapping):
        fb = reinjection.get("fallback_scope")
        if fb is not None and str(fb).strip():
            fallback_scope = str(fb).strip()
    if fallback_scope is None:
        fb_top = payload.get("fallback_scope")
        if fb_top is not None and str(fb_top).strip():
            fallback_scope = str(fb_top).strip()

    relevant_lines = clean_semantic_relevant_lines(payload.get("relevant_lines"))
    if status == "selected" and relevant_lines:
        lines = relevant_lines
    elif status == "selected" and not relevant_lines:
        lines = ["relevant concepts: (unavailable: empty)"]
        status = "unavailable"
        skip_reason = skip_reason or "empty"
    else:
        reason = skip_reason or status or "unavailable"
        lines = [f"relevant concepts: (unavailable: {reason})"]

    raw: dict[str, Any] | None
    try:
        raw = dict(payload)
    except Exception:  # noqa: BLE001
        raw = None

    return {
        "status": status,
        "skip_reason": skip_reason,
        "relevant_lines": lines,
        "fallback_scope": fallback_scope,
        "raw": raw,
    }


def _build_semantic_channel(
    task_ref: str,
    *,
    anchor_texts: Sequence[str] | None,
    semantic_content_budget_chars: int | None,
) -> SemanticChannel:
    kwargs: dict[str, Any] = {"task_ref": task_ref}
    if anchor_texts is not None:
        kwargs["anchor_texts"] = list(anchor_texts)
    if semantic_content_budget_chars is not None:
        kwargs["semantic_content_budget_chars"] = semantic_content_budget_chars
    try:
        payload = _mcp_semantic_reinjection_packet(**kwargs)
    except Exception as exc:  # noqa: BLE001 — mirror lane_prompt degrade
        logger.warning("slice_brief semantic unavailable: %s", exc)
        return _semantic_channel_from_payload(None, error="error")
    if not isinstance(payload, Mapping):
        return _semantic_channel_from_payload(None, error="invalid_payload")
    return _semantic_channel_from_payload(payload)


def _query_structural_anchor(anchor: SliceAnchor) -> StructuralAnchorHit:
    path = str(anchor.get("path") or "").strip()
    symbol = anchor.get("symbol")
    symbol_s = str(symbol).strip() if symbol else None

    if not path:
        return {
            "path": "",
            "symbol": symbol_s,
            "callers": [],
            "callees": [],
            "snippet": None,
            "resolved": False,
            "warning": "unresolved anchor: empty path",
        }

    try:
        trace = _mcp_codemap_trace_path(path=path, symbol=symbol_s, direction="both")
    except Exception as exc:  # noqa: BLE001
        # Per-anchor transport failure: surface warning, do not fabricate.
        return {
            "path": path,
            "symbol": symbol_s,
            "callers": [],
            "callees": [],
            "snippet": None,
            "resolved": False,
            "warning": f"unresolved anchor: trace failed ({exc})",
        }

    if not isinstance(trace, Mapping):
        return {
            "path": path,
            "symbol": symbol_s,
            "callers": [],
            "callees": [],
            "snippet": None,
            "resolved": False,
            "warning": "unresolved anchor: invalid trace payload",
        }

    found = bool(trace.get("found", False))
    callers = _string_list(trace.get("callers"))
    callees = _string_list(trace.get("callees"))
    trace_warning = trace.get("warning")
    warning: str | None = str(trace_warning).strip() if trace_warning else None

    snippet: str | None = None
    try:
        snip = _mcp_codemap_get_code_snippet(path=path, symbol=symbol_s)
    except Exception as exc:  # noqa: BLE001
        if found:
            warning = warning or f"snippet unavailable: {exc}"
        snip = None

    if isinstance(snip, Mapping):
        if snip.get("found") and snip.get("snippet") is not None:
            snippet_text = str(snip.get("snippet") or "").strip()
            snippet = snippet_text or None
            if snippet is not None:
                found = True
        snip_warning = snip.get("warning")
        if snip_warning and not warning:
            warning = str(snip_warning).strip() or None
        if not snip.get("found", False) and not found:
            label = f"{path}:{symbol_s}" if symbol_s else path
            warning = warning or f"unresolved anchor: symbol not found ({label})"

    if not found:
        label = f"{path}:{symbol_s}" if symbol_s else path
        warning = warning or f"unresolved anchor: symbol not found ({label})"
        return {
            "path": path,
            "symbol": symbol_s,
            "callers": [],
            "callees": [],
            "snippet": None,
            "resolved": False,
            "warning": warning,
        }

    return {
        "path": path,
        "symbol": symbol_s,
        "callers": callers,
        "callees": callees,
        "snippet": snippet,
        "resolved": True,
        "warning": warning,
    }


def _build_structural_channel(
    anchors: Sequence[SliceAnchor],
) -> tuple[StructuralChannel | None, list[str]]:
    """Assemble structural channel or omit it (codemap absent/stale).

    Returns ``(channel_or_None, packet_level_warnings)``.
    """
    packet_warnings: list[str] = []
    try:
        status = _mcp_codemap_index_status()
    except Exception as exc:  # noqa: BLE001
        msg = f"structural channel omitted: codemap unavailable ({exc})"
        logger.warning("%s", msg)
        packet_warnings.append(msg)
        return None, packet_warnings

    if not isinstance(status, Mapping):
        msg = "structural channel omitted: invalid index_status payload"
        logger.warning("%s", msg)
        packet_warnings.append(msg)
        return None, packet_warnings

    available = bool(status.get("available", True))
    stale = bool(status.get("stale", False))
    if not available or stale:
        reason = status.get("reason")
        if reason is None or not str(reason).strip():
            reason = "stale" if stale else "unavailable"
        msg = f"structural channel omitted: codemap {reason}"
        logger.warning("%s", msg)
        packet_warnings.append(msg)
        return None, packet_warnings

    ts_raw = status.get("index_timestamp")
    index_timestamp = str(ts_raw).strip() if ts_raw is not None and str(ts_raw).strip() else None

    hits: list[StructuralAnchorHit] = []
    channel_warnings: list[str] = []
    for anchor in anchors:
        hit = _query_structural_anchor(anchor)
        hits.append(hit)
        if hit.get("warning"):
            channel_warnings.append(str(hit["warning"]))

    channel: StructuralChannel = {
        "index_timestamp": index_timestamp,
        "caveat": LOCAL_AID_CAVEAT,
        "anchors": hits,
        "warnings": channel_warnings,
    }
    return channel, packet_warnings


def build_slice_brief_packet(
    task_ref: str,
    slice_anchors: Sequence[str | Mapping[str, Any]] | None = None,
    *,
    anchor_texts: Sequence[str] | None = None,
    semantic_content_budget_chars: int | None = None,
) -> SliceBriefPacket:
    """Build a two-channel slice brief packet for ``task_ref``.

    Parameters
    ----------
    task_ref:
        Active task reference for the semantic channel.
    slice_anchors:
        Plan-cited anchors as ``path:symbol`` strings or ``{path, symbol}`` maps.
        Used for the structural channel only.
    anchor_texts:
        Optional texts forwarded to ``semantic_reinjection_packet`` (changed-file
        paths / rationale excerpts). When omitted, empty anchors are used.
    semantic_content_budget_chars:
        Optional budget override for the semantic channel.
    """
    resolved_ref = str(task_ref or "").strip()
    normalized: list[SliceAnchor] = []
    for raw in slice_anchors or ():
        anchor = _normalize_anchor(raw)
        if anchor.get("path"):
            normalized.append(anchor)

    semantic = _build_semantic_channel(
        resolved_ref,
        anchor_texts=anchor_texts,
        semantic_content_budget_chars=semantic_content_budget_chars,
    )
    structural, structural_omit_warnings = _build_structural_channel(normalized)

    warnings: list[str] = list(structural_omit_warnings)
    if structural is not None:
        warnings.extend(structural.get("warnings") or [])

    return {
        "task_ref": resolved_ref,
        "semantic": semantic,
        "structural": structural,
        "warnings": warnings,
    }
