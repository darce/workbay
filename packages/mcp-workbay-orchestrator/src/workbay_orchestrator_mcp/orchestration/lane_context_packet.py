"""Deterministic lane context packet from the codemap CLI (internal / T25).

Zero-LLM packet builder: pure subprocess calls to ``codebase-memory-mcp cli``
plus JSON parsing. Optional integration — when the CLI is absent every path
degrades typed+loud ([OBS-08]) and never crashes.

Bounded precursor to full codemap auto-wiring (deferred). Hard size cap is a
single-sourced constant ([DATA-14]).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

logger = logging.getLogger(__name__)

# [DATA-14] single-source constants for packet + CLI policy.
PACKET_MAX_BYTES = 8192
PACKET_TRUNCATION_MARKER = "\n...[truncated: lane context packet exceeded PACKET_MAX_BYTES]...\n"
# S12-A-03: bound the fan-out — each target costs up to 3 CLI calls, so an
# unbounded targets list would multiply subprocess latency into the dispatch path.
PACKET_MAX_TARGETS = 8
# S12-A-03: aggregate wall-clock budget across ALL CLI calls for one packet
# (per-call timeout alone still allows targets x tools x 8s worst case).
CODEMAP_TOTAL_BUDGET_SECONDS = 30.0
CODEMAP_CLI_NAME = "codebase-memory-mcp"
CODEMAP_CLI_TIMEOUT_SECONDS = 8.0
CODEMAP_STALE_WARNING = "codemap_stale: refresh via index_repository"
CODEMAP_UNAVAILABLE_NOTE = "codemap_unavailable"
CODEMAP_SECTION_OMITTED_PREFIX = "section_omitted:"
CODEMAP_BUDGET_EXHAUSTED_NOTE = f"{CODEMAP_SECTION_OMITTED_PREFIX}cli_budget_exhausted"

# Tools used by the packet builder (subset of the CLI surface).
_TOOL_INDEX_STATUS = "index_status"
_TOOL_DETECT_CHANGES = "detect_changes"
_TOOL_SEARCH_GRAPH = "search_graph"
_TOOL_TRACE_PATH = "trace_path"
_TOOL_GET_CODE_SNIPPET = "get_code_snippet"


def project_key_for_worktree(worktree_path: str | Path) -> str:
    """Map an absolute worktree path to the CLI project name used by list_projects.

    Convention observed in codebase-memory-mcp: absolute path with the leading
    slash stripped and remaining ``/`` replaced by ``-``.
    """
    resolved = str(Path(worktree_path).expanduser().resolve())
    if resolved.startswith("/"):
        resolved = resolved[1:]
    return resolved.replace("/", "-").replace("\\", "-")


def primary_repo_root(worktree_path: str | Path) -> Path:
    """Primary repository root for a (possibly linked) git worktree.

    S12-A-01: codemap indexes the PRIMARY checkout, so the project key must be
    derived from it — a lane worktree path would name a project that was never
    indexed. ``git rev-parse --git-common-dir`` points at the primary ``.git``
    even from a linked worktree; its parent is the primary root. Falls back to
    the worktree path itself on any git failure (bare/non-repo paths).
    """
    resolved = Path(worktree_path).expanduser().resolve()
    try:
        completed = subprocess.run(
            ["git", "-C", str(resolved), "rev-parse", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return resolved
    common = (completed.stdout or "").strip()
    if completed.returncode != 0 or not common:
        return resolved
    common_path = Path(common)
    if not common_path.is_absolute():
        common_path = resolved / common_path
    common_path = common_path.resolve()
    if common_path.name == ".git":
        return common_path.parent
    return resolved


def project_key_for_primary_repo(worktree_path: str | Path) -> str:
    """Project key for the indexed primary root of *worktree_path* (S12-A-01)."""
    return project_key_for_worktree(primary_repo_root(worktree_path))


def resolve_codemap_cli(*, path_env: str | None = None) -> str | None:
    """Return an executable path for the codemap CLI, or None when absent.

    Search order: ``CODEBASE_MEMORY_MCP`` env override, then PATH, then
    ``~/.local/bin/codebase-memory-mcp``.
    """
    override = (path_env if path_env is not None else os.environ.get("CODEBASE_MEMORY_MCP") or "").strip()
    if override:
        candidate = Path(override).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate.resolve())
        return None

    found = shutil.which(CODEMAP_CLI_NAME)
    if found:
        return found

    local = Path.home() / ".local" / "bin" / CODEMAP_CLI_NAME
    if local.is_file() and os.access(local, os.X_OK):
        return str(local.resolve())
    return None


def run_codemap_cli(
    tool: str,
    payload: Mapping[str, Any],
    *,
    cli_path: str | None = None,
    timeout_seconds: float = CODEMAP_CLI_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Invoke ``cli <tool> '<json>'`` and parse the first JSON object from stdout.

    Returns a normalized result::

        {"ok": bool, "data": dict|None, "error": str|None, "cli_path": str|None}

    Never raises for missing CLI / timeout / bad JSON — those become
    ``ok=False`` with a typed error string ([OBS-08]).
    """
    resolved = cli_path if cli_path is not None else resolve_codemap_cli()
    if not resolved:
        return {
            "ok": False,
            "data": None,
            "error": CODEMAP_UNAVAILABLE_NOTE,
            "cli_path": None,
        }

    try:
        body = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        return {
            "ok": False,
            "data": None,
            "error": f"invalid payload for {tool}: {exc}",
            "cli_path": resolved,
        }

    try:
        completed = subprocess.run(
            [resolved, "cli", tool, body],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError:
        return {
            "ok": False,
            "data": None,
            "error": CODEMAP_UNAVAILABLE_NOTE,
            "cli_path": resolved,
        }
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "data": None,
            "error": f"{CODEMAP_SECTION_OMITTED_PREFIX}{tool}:timeout",
            "cli_path": resolved,
        }
    except OSError as exc:
        return {
            "ok": False,
            "data": None,
            "error": f"{CODEMAP_SECTION_OMITTED_PREFIX}{tool}:os_error:{exc}",
            "cli_path": resolved,
        }

    stdout = completed.stdout or ""
    data = _extract_json_object(stdout)
    if data is None:
        # Some CLIs print log lines to stdout; fall back to stderr tail for error text.
        err_tail = (completed.stderr or stdout or "").strip().splitlines()
        hint = err_tail[-1] if err_tail else f"exit={completed.returncode}"
        return {
            "ok": False,
            "data": None,
            "error": f"{CODEMAP_SECTION_OMITTED_PREFIX}{tool}:bad_json:{hint[:200]}",
            "cli_path": resolved,
        }

    if completed.returncode != 0 and _payload_looks_like_error(data):
        return {
            "ok": False,
            "data": data,
            "error": f"{CODEMAP_SECTION_OMITTED_PREFIX}{tool}:{_error_message(data)}",
            "cli_path": resolved,
        }

    return {"ok": True, "data": data, "error": None, "cli_path": resolved}


def _extract_json_object(text: str) -> dict[str, Any] | None:
    """Parse the first JSON object from CLI stdout (skip log lines)."""
    stripped = (text or "").strip()
    if not stripped:
        return None
    # Fast path: whole stdout is JSON.
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    # Scan lines for a JSON object.
    for line in stripped.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    # Last resort: first {...} span.
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _payload_looks_like_error(data: Mapping[str, Any]) -> bool:
    if data.get("error") is not None:
        return True
    status = str(data.get("status") or "").strip().lower()
    return status in {"error", "failed", "not_found"}


def _error_message(data: Mapping[str, Any]) -> str:
    err = data.get("error")
    if err is not None and str(err).strip():
        return str(err).strip()[:200]
    msg = data.get("message") or data.get("hint") or data.get("status") or "error"
    return str(msg).strip()[:200]


def _worktree_head_sha(worktree_path: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(worktree_path), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    sha = (completed.stdout or "").strip()
    if completed.returncode == 0 and sha:
        return sha
    return None


def _indexed_sha_from_status(status: Mapping[str, Any] | None) -> str | None:
    """Indexed commit sha from an index_status payload, when it carries one."""
    if not isinstance(status, Mapping):
        return None
    for key in ("head_sha", "commit_sha", "git_sha", "indexed_commit", "revision"):
        raw = status.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return None


def check_codemap_index_freshness(
    worktree_path: str | Path,
    *,
    cli_path: str | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """Index-freshness gate for offload_preflight (warn-only, never blocks).

    Returns::

        {
          "available": bool,
          "stale": bool,
          "note": str | None,   # codemap_unavailable | codemap_stale: ...
          "status": dict | None,
          "detect_changes": dict | None,
          "project": str | None,
          "cli_path": str | None,
          "head_sha": str | None,
        }

    Stale when index_status reports non-ready / explicit stale flag, when
    detect_changes reports changed files, or when an indexed commit disagrees
    with worktree HEAD (when the status payload carries one).
    """
    resolved_wt = Path(worktree_path).expanduser().resolve()
    resolved_cli = cli_path if cli_path is not None else resolve_codemap_cli()
    # S12-A-01: default project key comes from the indexed PRIMARY root, not the
    # lane worktree path; an explicit ``project`` override always wins.
    proj = project or project_key_for_primary_repo(resolved_wt)
    head_sha = _worktree_head_sha(resolved_wt)

    if not resolved_cli:
        return {
            "available": False,
            "stale": False,
            "note": CODEMAP_UNAVAILABLE_NOTE,
            "status": None,
            "detect_changes": None,
            "project": proj,
            "cli_path": None,
            "head_sha": head_sha,
            "indexed_head_sha": None,
        }

    status_result = run_codemap_cli(
        _TOOL_INDEX_STATUS,
        {"project": proj},
        cli_path=resolved_cli,
    )
    if not status_result["ok"] or not isinstance(status_result.get("data"), dict):
        # CLI present but tool failed — still typed unavailable-style skip.
        err = status_result.get("error") or CODEMAP_UNAVAILABLE_NOTE
        # Prefer the named unavailable note when the CLI itself is gone mid-call.
        note: str | None = (
            CODEMAP_UNAVAILABLE_NOTE if err == CODEMAP_UNAVAILABLE_NOTE else f"{CODEMAP_UNAVAILABLE_NOTE}:{err}"
        )
        return {
            "available": False,
            "stale": False,
            "note": note,
            "status": status_result.get("data"),
            "detect_changes": None,
            "project": proj,
            "cli_path": resolved_cli,
            "head_sha": head_sha,
            "indexed_head_sha": _indexed_sha_from_status(status_result.get("data")),
        }

    status = status_result["data"]
    assert isinstance(status, dict)

    # Explicit stale / non-ready status fields.
    status_flag = str(status.get("status") or "").strip().lower()
    explicit_stale = bool(status.get("stale")) or status_flag in {
        "stale",
        "outdated",
        "dirty",
        "needs_refresh",
    }
    ready = status_flag in {"", "ready", "ok", "fresh", "indexed"} and not explicit_stale

    # Indexed commit vs worktree HEAD when the status payload carries a sha.
    indexed_sha = _indexed_sha_from_status(status)
    head_mismatch = bool(
        head_sha and indexed_sha and not (head_sha.startswith(indexed_sha) or indexed_sha.startswith(head_sha))
    )

    detect_data: dict[str, Any] | None = None
    detect_stale = False
    detect_result = run_codemap_cli(
        _TOOL_DETECT_CHANGES,
        {"project": proj},
        cli_path=resolved_cli,
    )
    if detect_result["ok"] and isinstance(detect_result.get("data"), dict):
        detect_data = detect_result["data"]
        changed = detect_data.get("changed_files") or detect_data.get("changes") or []
        changed_count = detect_data.get("changed_count")
        if isinstance(changed_count, int) and changed_count > 0:
            detect_stale = True
        elif isinstance(changed, list) and len(changed) > 0:
            detect_stale = True
        elif bool(detect_data.get("stale")):
            detect_stale = True

    stale = (not ready) or explicit_stale or head_mismatch or detect_stale
    note = CODEMAP_STALE_WARNING if stale else None
    return {
        "available": True,
        "stale": stale,
        "note": note,
        "status": status,
        "detect_changes": detect_data,
        "project": proj,
        "cli_path": resolved_cli,
        "head_sha": head_sha,
        "indexed_head_sha": indexed_sha,
    }


def _normalize_target(raw: str | Mapping[str, Any]) -> dict[str, str | None]:
    """Accept ``path:symbol``, bare path/symbol, or ``{path, symbol}`` maps."""
    if isinstance(raw, Mapping):
        path = str(raw.get("path") or "").strip() or None
        symbol_raw = raw.get("symbol")
        symbol = str(symbol_raw).strip() if symbol_raw is not None and str(symbol_raw).strip() else None
        if symbol is None and path is None:
            text = str(raw.get("name") or raw.get("query") or "").strip()
            return {"path": None, "symbol": text or None, "query": text or None}
        query = f"{path}:{symbol}" if path and symbol else (symbol or path)
        return {"path": path, "symbol": symbol, "query": query}

    text = str(raw).strip()
    if not text:
        return {"path": None, "symbol": None, "query": None}
    if ":" in text:
        left, right = text.rsplit(":", 1)
        left, right = left.strip(), right.strip()
        if left and right and "/" not in right and "\\" not in right and len(left) > 1:
            return {"path": left, "symbol": right, "query": text}
    # Bare identifier vs path heuristic.
    if "/" in text or text.endswith((".py", ".ts", ".js", ".go", ".rs", ".md")):
        return {"path": text, "symbol": None, "query": text}
    return {"path": None, "symbol": text, "query": text}


def _pick_search_hits(data: Mapping[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    results = data.get("results") or data.get("nodes") or data.get("matches") or []
    if not isinstance(results, list):
        return []
    hits: list[dict[str, Any]] = []
    for item in results[:limit]:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or item.get("symbol") or "").strip()
        path = str(item.get("file_path") or item.get("path") or item.get("file") or "").strip()
        qn = str(item.get("qualified_name") or "").strip() or None
        label = str(item.get("label") or item.get("kind") or "").strip() or None
        if not name and not path:
            continue
        hits.append(
            {
                "path": path or None,
                "symbol": name or None,
                "qualified_name": qn,
                "label": label,
                "anchor": f"{path}:{name}" if path and name else (name or path),
            }
        )
    return hits


def _format_blast(data: Mapping[str, Any], *, max_each: int = 8) -> dict[str, Any]:
    callers_raw = data.get("callers") or []
    callees_raw = data.get("callees") or []
    callers: list[str] = []
    callees: list[str] = []
    if isinstance(callers_raw, list):
        for item in callers_raw[:max_each]:
            if isinstance(item, Mapping):
                name = str(item.get("name") or item.get("qualified_name") or "").strip()
            else:
                name = str(item).strip()
            if name:
                callers.append(name)
    if isinstance(callees_raw, list):
        for item in callees_raw[:max_each]:
            if isinstance(item, Mapping):
                name = str(item.get("name") or item.get("qualified_name") or "").strip()
            else:
                name = str(item).strip()
            if name:
                callees.append(name)
    return {
        "function": data.get("function") or data.get("name"),
        "callers": callers,
        "callees": callees,
    }


def _snippet_text(data: Mapping[str, Any], *, max_chars: int = 600) -> str | None:
    for key in ("source", "snippet", "code", "text"):
        raw = data.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            if len(text) > max_chars:
                return text[: max_chars - 20] + "\n...[snippet truncated]...\n"
            return text
    return None


def build_lane_context_packet(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: str | Path,
    targets: Sequence[str | Mapping[str, Any]] | None,
    cli_path: str | None = None,
    project: str | None = None,
    max_bytes: int = PACKET_MAX_BYTES,
) -> dict[str, Any]:
    """Build a bounded lane context packet for a dispatch brief.

    Parameters
    ----------
    task_ref / lane_id / worktree_path:
        Identity of the dispatch target.
    targets:
        Slice target files/symbols (``path:symbol``, bare path, or maps).
    cli_path:
        Optional CLI override (tests inject a fake executable).
    max_bytes:
        Hard size cap; truncated with a marker when exceeded ([DATA-14]).

    Returns a dict with ``packet`` (str|None), ``packet_bytes``, ``sections``,
    ``notes``, ``available``, and metadata. Never raises for CLI absence.
    """
    resolved_wt = Path(worktree_path).expanduser().resolve()
    resolved_cli = cli_path if cli_path is not None else resolve_codemap_cli()
    # S12-A-01: project key from the indexed PRIMARY root, not the lane worktree
    # path (which names a never-indexed project); explicit override wins.
    proj = project or project_key_for_primary_repo(resolved_wt)
    normalized_targets = [_normalize_target(t) for t in (targets or ()) if t is not None]
    normalized_targets = [t for t in normalized_targets if t.get("query")]

    sections: dict[str, Any] = {
        "meta": {
            "task_ref": task_ref,
            "lane_id": lane_id,
            "worktree_path": str(resolved_wt),
            "project": proj,
        },
        "anchors": [],
        "blast_radius": [],
        "snippets": [],
    }
    notes: list[str] = []

    # S12-A-03: cap the target fan-out (each target costs up to 3 CLI calls).
    if len(normalized_targets) > PACKET_MAX_TARGETS:
        dropped_targets = len(normalized_targets) - PACKET_MAX_TARGETS
        normalized_targets = normalized_targets[:PACKET_MAX_TARGETS]
        notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}targets:capped:{dropped_targets}_over_{PACKET_MAX_TARGETS}")

    if not resolved_cli:
        notes.append(CODEMAP_UNAVAILABLE_NOTE)
        return {
            "available": False,
            "packet": None,
            "packet_bytes": 0,
            "sections": sections,
            "notes": notes,
            "truncated": False,
            "cli_path": None,
            "project": proj,
        }

    # S12-A-03: aggregate wall-clock budget across every CLI call this packet
    # makes (freshness probe included) — per-call timeouts alone still allow
    # targets x tools x CODEMAP_CLI_TIMEOUT_SECONDS in the dispatch path.
    deadline = time.monotonic() + CODEMAP_TOTAL_BUDGET_SECONDS
    budget_exhausted = False

    def _budgeted_call(tool: str, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        nonlocal budget_exhausted
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            budget_exhausted = True
            return None
        return run_codemap_cli(
            tool,
            payload,
            cli_path=resolved_cli,
            timeout_seconds=min(CODEMAP_CLI_TIMEOUT_SECONDS, remaining),
        )

    # S12-A-02: stamp REAL index freshness onto packet meta (stale flag +
    # indexed head sha) so the consumer sees measured state, not a disclaimer.
    freshness = check_codemap_index_freshness(resolved_wt, cli_path=resolved_cli, project=proj)
    sections["meta"]["index_stale"] = bool(freshness.get("stale")) if freshness.get("available") else None
    sections["meta"]["indexed_head_sha"] = freshness.get("indexed_head_sha")
    sections["meta"]["worktree_head_sha"] = freshness.get("head_sha")
    freshness_note = freshness.get("note")
    if freshness_note:
        notes.append(str(freshness_note))

    if not normalized_targets:
        notes.append("section_omitted:targets:none_provided")
        packet = _render_packet(sections, notes=notes)
        packet, truncated = _cap_packet(packet, max_bytes=max_bytes)
        return {
            "available": True,
            "packet": packet,
            "packet_bytes": len(packet.encode("utf-8")),
            "sections": sections,
            "notes": notes,
            "truncated": truncated,
            "cli_path": resolved_cli,
            "project": proj,
        }

    # --- anchors via search_graph ---
    for target in normalized_targets:
        query = str(target.get("query") or "")
        search = _budgeted_call(
            _TOOL_SEARCH_GRAPH,
            {"project": proj, "query": query, "limit": 5},
        )
        if search is None:
            break
        if not search["ok"] or not isinstance(search.get("data"), dict):
            notes.append(search.get("error") or f"{CODEMAP_SECTION_OMITTED_PREFIX}search_graph:{query}")
            continue
        hits = _pick_search_hits(search["data"], limit=5)
        if not hits:
            # Retry with name_pattern for bare symbols.
            symbol = target.get("symbol")
            if symbol:
                search2 = _budgeted_call(
                    _TOOL_SEARCH_GRAPH,
                    {"project": proj, "name_pattern": str(symbol), "limit": 5},
                )
                if search2 is not None and search2["ok"] and isinstance(search2.get("data"), dict):
                    hits = _pick_search_hits(search2["data"], limit=5)
        if hits:
            sections["anchors"].extend(hits)
        else:
            notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}search_graph:no_hits:{query}")

    # Deduplicate anchors by (path, symbol).
    seen_anchors: set[tuple[str | None, str | None]] = set()
    unique_anchors: list[dict[str, Any]] = []
    for hit in sections["anchors"]:
        key = (hit.get("path"), hit.get("symbol"))
        if key in seen_anchors:
            continue
        seen_anchors.add(key)
        unique_anchors.append(hit)
    sections["anchors"] = unique_anchors

    # --- blast radius via trace_path for each target symbol ---
    traced_symbols: set[str] = set()
    for target in normalized_targets:
        symbol = target.get("symbol")
        if not symbol:
            # Fall back to first search hit name for path-only targets.
            for hit in sections["anchors"]:
                if hit.get("path") == target.get("path") and hit.get("symbol"):
                    symbol = hit["symbol"]
                    break
        if not symbol or symbol in traced_symbols:
            continue
        traced_symbols.add(str(symbol))
        trace = _budgeted_call(
            _TOOL_TRACE_PATH,
            {
                "project": proj,
                "function_name": str(symbol),
                "direction": "both",
                "depth": 1,
            },
        )
        if trace is None:
            break
        if not trace["ok"] or not isinstance(trace.get("data"), dict):
            notes.append(trace.get("error") or f"{CODEMAP_SECTION_OMITTED_PREFIX}trace_path:{symbol}")
            continue
        sections["blast_radius"].append(_format_blast(trace["data"]))

    # --- snippets via get_code_snippet (prefer qualified_name from anchors) ---
    snipped: set[str] = set()
    collected_snippet_bytes = 0
    for hit in sections["anchors"][:6]:
        # S12-A-03: enforce the size cap while COLLECTING — once accumulated
        # snippet text alone exceeds max_bytes, further CLI calls only feed the
        # truncator, so stop before assembling more large strings.
        if collected_snippet_bytes >= max_bytes:
            notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}snippets:size_cap")
            break
        qn = hit.get("qualified_name")
        symbol = hit.get("symbol")
        snippet_key = str(qn or symbol or "")
        if not snippet_key or snippet_key in snipped:
            continue
        snipped.add(snippet_key)
        payload: dict[str, Any] = {"project": proj}
        if qn:
            payload["qualified_name"] = qn
        elif symbol:
            payload["qualified_name"] = str(symbol)
        else:
            continue
        snip = _budgeted_call(
            _TOOL_GET_CODE_SNIPPET,
            payload,
        )
        if snip is None:
            break
        if not snip["ok"] or not isinstance(snip.get("data"), dict):
            notes.append(snip.get("error") or f"{CODEMAP_SECTION_OMITTED_PREFIX}get_code_snippet:{key}")
            continue
        data = snip["data"]
        # Ambiguous response: skip body, keep a note.
        if str(data.get("status") or "").lower() == "ambiguous":
            notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}get_code_snippet:ambiguous:{key}")
            continue
        text = _snippet_text(data)
        if not text:
            notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}get_code_snippet:empty:{key}")
            continue
        collected_snippet_bytes += len(text.encode("utf-8"))
        sections["snippets"].append(
            {
                "path": hit.get("path") or data.get("file_path"),
                "symbol": hit.get("symbol") or data.get("name"),
                "qualified_name": qn or data.get("qualified_name"),
                "snippet": text,
            }
        )

    if budget_exhausted:
        notes.append(CODEMAP_BUDGET_EXHAUSTED_NOTE)

    packet = _render_packet(sections, notes=notes)
    packet, truncated = _cap_packet(packet, max_bytes=max_bytes)
    if truncated:
        notes.append("packet_truncated")

    return {
        "available": True,
        "packet": packet,
        "packet_bytes": len(packet.encode("utf-8")),
        "sections": sections,
        "notes": notes,
        "truncated": truncated,
        "cli_path": resolved_cli,
        "project": proj,
    }


def _render_packet(sections: Mapping[str, Any], *, notes: Sequence[str]) -> str:
    lines: list[str] = [
        "## Lane context packet (codemap, deterministic)",
        f"task_ref: {sections.get('meta', {}).get('task_ref')}",
        f"lane_id: {sections.get('meta', {}).get('lane_id')}",
        f"project: {sections.get('meta', {}).get('project')}",
        "",
        "### Anchors (path:symbol)",
    ]
    anchors = sections.get("anchors") or []
    if anchors:
        for hit in anchors:
            anchor = hit.get("anchor") or hit.get("symbol") or hit.get("path") or "?"
            label = hit.get("label")
            extra = f" ({label})" if label else ""
            lines.append(f"- {anchor}{extra}")
    else:
        lines.append("- (none)")

    lines.extend(["", "### Blast radius (callers/callees)"])
    blasts = sections.get("blast_radius") or []
    if blasts:
        for blast in blasts:
            fn = blast.get("function") or "?"
            callers = ", ".join(blast.get("callers") or []) or "(none)"
            callees = ", ".join(blast.get("callees") or []) or "(none)"
            lines.append(f"- {fn}")
            lines.append(f"  callers: {callers}")
            lines.append(f"  callees: {callees}")
    else:
        lines.append("- (none)")

    lines.extend(["", "### Code excerpts"])
    snippets = sections.get("snippets") or []
    if snippets:
        for snip in snippets:
            label = snip.get("symbol") or snip.get("path") or "?"
            path = snip.get("path") or ""
            lines.append(f"- {label} @ {path}")
            body = str(snip.get("snippet") or "").rstrip()
            for body_line in body.splitlines():
                lines.append(f"  | {body_line}")
    else:
        lines.append("- (none)")

    if notes:
        lines.extend(["", "### Notes"])
        for note in notes:
            lines.append(f"- {note}")

    # S12-A-02: measured index freshness, not a static disclaimer.
    meta = sections.get("meta", {}) if isinstance(sections.get("meta"), Mapping) else {}
    stale = meta.get("index_stale")
    indexed_sha = meta.get("indexed_head_sha")
    worktree_sha = meta.get("worktree_head_sha")
    lines.append("")
    if stale is True:
        lines.append(
            "Index freshness: STALE — indexed "
            f"{indexed_sha or 'unknown'} vs worktree {worktree_sha or 'unknown'}; "
            "structural channel may lag the worktree (refresh via index_repository)."
        )
    elif stale is False:
        lines.append(f"Index freshness: fresh (indexed {indexed_sha or worktree_sha or 'unknown'}).")
    else:
        lines.append(
            "Index freshness: unknown (index_status unavailable); treat the "
            "structural channel as local aid, not ground truth."
        )
    return "\n".join(lines) + "\n"


def _cap_packet(packet: str, *, max_bytes: int) -> tuple[str, bool]:
    raw = packet.encode("utf-8")
    if len(raw) <= max_bytes:
        return packet, False
    # Leave room for the truncation marker.
    marker = PACKET_TRUNCATION_MARKER
    marker_bytes = marker.encode("utf-8")
    budget = max(0, max_bytes - len(marker_bytes))
    truncated = raw[:budget]
    # Avoid splitting a multi-byte UTF-8 sequence.
    while truncated and (truncated[-1] & 0xC0) == 0x80:
        truncated = truncated[:-1]
    text = truncated.decode("utf-8", errors="ignore") + marker
    # Final guard if marker pushed us over (shouldn't with budget math).
    encoded = text.encode("utf-8")
    if len(encoded) > max_bytes:
        text = encoded[:max_bytes].decode("utf-8", errors="ignore")
    return text, True


def append_packet_to_brief(brief: str | None, packet: str | None) -> str | None:
    """Append a non-empty packet to an existing brief (or stand alone)."""
    packet_s = (packet or "").strip()
    if not packet_s:
        return brief
    brief_s = (brief or "").strip()
    if not brief_s:
        return packet_s
    return f"{brief_s}\n\n{packet_s}"


def should_include_context_packet(
    *,
    include_context_packet: bool | None,
    targets: Sequence[Any] | None,
    cli_path: str | None = None,
) -> bool:
    """Opt-in flag wins; else auto when CLI present AND targets provided."""
    if include_context_packet is False:
        return False
    if include_context_packet is True:
        return True
    # Auto: CLI present AND targets provided.
    if not targets:
        return False
    return (cli_path if cli_path is not None else resolve_codemap_cli()) is not None
