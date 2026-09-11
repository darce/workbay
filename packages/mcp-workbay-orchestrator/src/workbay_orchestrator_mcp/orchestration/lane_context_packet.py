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
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

# Comparability floor: single package owner is codemap_adapter._sha_equal.
# Re-exported under this name for packet note-taxonomy and stable imports.
# Probe copies cannot import the package (stdlib-only); parity is enforced by
# tests/test_codemap_compare_policy_single_source_pin.py — not by comment.
from workbay_orchestrator_mcp.orchestration.codemap_adapter import (  # noqa: F401
    _MIN_SHA_COMPARE_LEN as _MIN_SHA_COMPARE_LEN,
)

logger = logging.getLogger(__name__)

# [DATA-14] single-source constants for packet + CLI policy.
PACKET_MAX_BYTES = 8192
PACKET_TRUNCATION_MARKER = "\n...[truncated: lane context packet exceeded PACKET_MAX_BYTES]...\n"
# S12-A-03: bound the fan-out — each target costs up to 3 CLI calls, so an
# unbounded targets list would multiply subprocess latency into the dispatch path.
PACKET_MAX_TARGETS = 8
# S12-A-03: aggregate wall-clock budget across ALL CLI calls for one packet
# (per-call timeout alone still allows targets x tools x 8s worst case).
CODEMAP_TOTAL_BUDGET_SECONDS = 60.0
CODEMAP_CLI_NAME = "codebase-memory-mcp"
CODEMAP_CLI_TIMEOUT_SECONDS = 20.0
CODEMAP_STALE_WARNING = "codemap_stale: refresh via index_repository"
# head_mismatch-only: two different checkouts (indexed root vs primary root HEAD).
# A reindex of the primary root cannot clear this — do not prescribe index_repository.
CODEMAP_DIVERGENCE_NOTE = (
    "codemap_divergence: index covers a different checkout than the primary root (not cleared by reindex)"
)
# Present but shorter than _MIN_SHA_COMPARE_LEN on either side: fail-closed without
# claiming a measured disagreement (reindex cannot make a truncated value comparable).
CODEMAP_INCOMPARABLE_NOTE = (
    "codemap_incomparable: indexed/primary commit shas present but too short to compare (not cleared by reindex)"
)
# index_status parsed but no recognizable commit sha — "agrees" vs "unreadable"
# must not be silent-indistinguishable (the defect that hid nested-sha for 13 days).
CODEMAP_SHA_UNREADABLE_NOTE = "codemap_sha_unreadable: index_status carried no recognizable commit sha"
CODEMAP_UNAVAILABLE_NOTE = "codemap_unavailable"
CODEMAP_SECTION_OMITTED_PREFIX = "section_omitted:"
CODEMAP_BUDGET_EXHAUSTED_NOTE = f"{CODEMAP_SECTION_OMITTED_PREFIX}cli_budget_exhausted"

# Tools used by the packet builder (subset of the CLI surface).
_TOOL_INDEX_STATUS = "index_status"
_TOOL_DETECT_CHANGES = "detect_changes"
_TOOL_SEARCH_GRAPH = "search_graph"
_TOOL_TRACE_PATH = "trace_path"
_TOOL_GET_CODE_SNIPPET = "get_code_snippet"

# Advisory related-prior (CAL-03): cosine neighbors, never a gate. Caps keep
# the packet inside PACKET_MAX_BYTES ([DATA-14]).
RELATED_PRIOR_LIMIT = 5
RELATED_PRIOR_SNIPPET_MAX_CHARS = 240
RELATED_PRIOR_NOTE_PREFIX = "related_prior:"
SEMANTIC_PRIOR_LIMIT = 5
SEMANTIC_PRIOR_KEYWORD_LIMIT = 6


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
    """Pipe a JSON request to ``cli <tool>`` and parse its JSON response.

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

    request = dict(payload)
    if tool in {_TOOL_SEARCH_GRAPH, _TOOL_TRACE_PATH, _TOOL_DETECT_CHANGES}:
        request.setdefault("format", "json")

    try:
        body = json.dumps(request, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        return {
            "ok": False,
            "data": None,
            "error": f"invalid payload for {tool}: {exc}",
            "cli_path": resolved,
        }

    try:
        completed = subprocess.run(
            [resolved, "cli", tool],
            input=body,
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
    # A successful-looking payload cannot establish freshness after a failed
    # status probe. Preserve stderr even when stdout is valid JSON.
    if tool == _TOOL_INDEX_STATUS and completed.returncode != 0:
        hint = (completed.stderr or "").strip() or f"exit={completed.returncode}"
        return {
            "ok": False,
            "data": data,
            "error": f"index_status_failed:{hint[:200]}",
            "cli_path": resolved,
        }
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

    if completed.returncode != 0 and (tool == _TOOL_DETECT_CHANGES or _payload_looks_like_error(data)):
        return {
            "ok": False,
            "data": data,
            "error": f"{CODEMAP_SECTION_OMITTED_PREFIX}{tool}:exit={completed.returncode}:{_error_message(data)}",
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


def _result_is_project_not_indexed(result: Mapping[str, Any]) -> bool:
    """Whether an index_status result specifically names an absent index."""
    fragments = [str(result.get("error") or "")]
    data = result.get("data")
    if isinstance(data, Mapping):
        status = str(data.get("status") or "").strip().lower()
        if status in {"not_indexed", "not_found"}:
            return True
        fragments.extend(str(data.get(key) or "") for key in ("error", "message", "detail"))
    message = " ".join(fragments).lower().replace("-", "_")
    return "project not found" in message or "not indexed" in message or "not_indexed" in message


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
    """Best-effort sha from an index_status payload, when it carries one.

    Measured production shape (``codebase-memory-mcp cli index_status``,
    2026-07-23): top-level keys are ``project/nodes/edges/status/root_path/git``
    — none of the sha names below. Prefer nested ``git`` first; fall back to
    top-level for synthetic/legacy test payloads only (no producer version has
    ever emitted a top-level sha; binary strings for
    ``commit_sha``/``git_sha``/``indexed_commit`` = 0 occurrences).

    ``git.head_sha`` is a LIVE probe of ``root_path`` HEAD at query time, NOT
    the commit the index was built at. Measured: it moved C1→C2 with no reindex
    while ``nodes`` and ``status`` held. The CLI exposes the index-build commit
    nowhere; this reader still surfaces the nested live sha because it is the
    only sha-shaped field real payloads carry (and the head-mismatch branch
    needs a comparable value).

    ``git.base_sha`` is deliberately excluded: it is the compare/merge base,
    not a HEAD/index commit. Returning it would false-confirm freshness after
    HEAD advanced past that base.
    """
    if not isinstance(status, Mapping):
        return None
    # Two explicit allowlists (not one shared tuple). Nested ``git`` accepts
    # only the production key the real CLI emits (``head_sha``). The broader
    # top-level list covers synthetic/legacy test payloads
    # (``commit_sha``/``git_sha``/``indexed_commit``/``revision``) and must not
    # be copy-pasted into the nested path — a key added for legacy top-level
    # must not be silently granted to the production nested object.
    # Exclusion of ``base_sha`` is load-bearing on BOTH allowlists: base_sha is
    # the compare/merge base, not a HEAD/index commit; returning it would
    # false-confirm freshness after HEAD advanced past that base.
    nested_git_sha_keys = ("head_sha",)
    top_level_sha_keys = ("head_sha", "commit_sha", "git_sha", "indexed_commit", "revision")

    git = status.get("git")
    if isinstance(git, Mapping):
        for key in nested_git_sha_keys:
            raw = git.get(key)
            # Require a real string: str(int)/str(dict) would launder schema drift
            # into a "readable" sha and then mislabel it as divergence.
            if isinstance(raw, str) and raw.strip():
                return raw.strip()

    for key in top_level_sha_keys:
        raw = status.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None


def check_codemap_index_freshness(
    worktree_path: str | Path,
    *,
    cli_path: str | None = None,
    project: str | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Index-freshness gate for offload_preflight (warn-only, never blocks).

    Returns::

        {
          "available": bool,
          "stale": bool,
          "note": str | None,   # cause-selected; see below
          "status": dict | None,
          "detect_changes": dict | None,
          "project": str | None,
          "cli_path": str | None,
          "head_sha": str | None,           # LANE worktree HEAD (display only)
          "primary_head_sha": str | None,   # PRIMARY root HEAD (compare operand)
          "indexed_head_sha": str | None,
          "indexed_sha_readable": bool,
        }

    Stale when index_status reports non-ready / explicit stale flag, when
    detect_changes reports changed files, or when a readable status sha
    disagrees with the PRIMARY root HEAD (the checkout codemap indexes).

    ``head_sha`` in the return value is still the LANE worktree HEAD (display
    context). ``primary_head_sha`` is the compare operand (primary root HEAD)
    and is present on every return path so consumers never need a fallback.
    The head-mismatch comparison uses the primary root HEAD so a diverged
    lane does not false-stale a fresh primary index.

    Note selection is by cause (honest about what was measured):

    - not-ready / explicit_stale / detect_stale → ``CODEMAP_STALE_WARNING``
      (reindex is an actionable remediation).
    - head_mismatch alone → ``CODEMAP_DIVERGENCE_NOTE`` (two different
      checkouts; reindex cannot clear it — must not prescribe
      ``index_repository``).
    - present but incomparable shas alone → ``CODEMAP_INCOMPARABLE_NOTE``
      (length guard refused the comparison; reindex cannot clear it).
    - available + parsed status but no readable sha → append
      ``CODEMAP_SHA_UNREADABLE_NOTE`` and log the observed top-level keys.
    """
    resolved_wt = Path(worktree_path).expanduser().resolve()
    resolved_cli = cli_path if cli_path is not None else resolve_codemap_cli()
    # Prefer an explicitly configured project. Otherwise optimistically try the
    # lane key, then fall back to the indexed primary checkout only when the CLI
    # specifically reports that the lane project is absent.
    proj = project or project_key_for_worktree(resolved_wt)
    primary_root = primary_repo_root(resolved_wt)
    primary_proj = project_key_for_worktree(primary_root)
    project_note: str | None = None
    # Lane worktree HEAD: reported for display context only (not the compare target).
    head_sha = _worktree_head_sha(resolved_wt)
    # Compare index against *primary* HEAD — codemap indexes the primary checkout.
    primary_head_sha = _worktree_head_sha(primary_root)

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
            "primary_head_sha": primary_head_sha,
            "indexed_head_sha": None,
            "indexed_sha_readable": False,
            "project_note": project_note,
        }

    def freshness_call(tool: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        timeout = CODEMAP_CLI_TIMEOUT_SECONDS
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"ok": False, "data": None, "error": "freshness_budget_exhausted"}
            timeout = min(timeout, remaining)
        result = run_codemap_cli(tool, payload, cli_path=resolved_cli, timeout_seconds=timeout)
        data = result.get("data")
        if tool == _TOOL_INDEX_STATUS and result["ok"] and isinstance(data, dict):
            if "error" in data or _payload_looks_like_error(data):
                return {**result, "ok": False, "error": f"index_status_failed:{_error_message(data)}"}
            if not str(data.get("status") or "").strip():
                return {**result, "ok": False, "error": "index_status_missing_status"}
        return result

    status_result = freshness_call(
        _TOOL_INDEX_STATUS,
        {"project": proj, "verbose": True},
    )
    if project is None and primary_proj != proj and _result_is_project_not_indexed(status_result):
        fallback_result = freshness_call(
            _TOOL_INDEX_STATUS,
            {"project": primary_proj, "verbose": True},
        )
        # Preserve a failed primary probe's diagnostic as well as successes.
        status_result = fallback_result
        if fallback_result["ok"] and isinstance(fallback_result.get("data"), dict):
            proj = primary_proj
            project_note = f"codemap_project_fallback:{primary_proj}"
            status_result = fallback_result
    if not status_result["ok"] or not isinstance(status_result.get("data"), dict):
        # CLI present but tool failed — still typed unavailable-style skip.
        err = status_result.get("error") or CODEMAP_UNAVAILABLE_NOTE
        # Prefer the named unavailable note when the CLI itself is gone mid-call.
        note: str | None = (
            CODEMAP_UNAVAILABLE_NOTE if err == CODEMAP_UNAVAILABLE_NOTE else f"{CODEMAP_UNAVAILABLE_NOTE}:{err}"
        )
        indexed_from_partial = _indexed_sha_from_status(status_result.get("data"))
        return {
            "available": False,
            "stale": None,
            "note": note,
            "status": status_result.get("data"),
            "detect_changes": None,
            "project": proj,
            "cli_path": resolved_cli,
            "head_sha": head_sha,
            "primary_head_sha": primary_head_sha,
            "indexed_head_sha": indexed_from_partial,
            "indexed_sha_readable": indexed_from_partial is not None,
            "project_note": project_note,
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
    ready = status_flag in {"ready", "ok", "fresh", "indexed"} and not explicit_stale

    # Status-carried sha vs PRIMARY root HEAD when the payload yields a readable
    # one. Note: nested git.head_sha is a LIVE root_path probe, not the
    # index-build commit — head_mismatch therefore measures checkout divergence
    # against the indexed primary checkout, not "index lag behind lane HEAD"
    # (see CODEMAP_DIVERGENCE_NOTE). Lane worktree HEAD stays in the return
    # dict as display context only.
    # Comparison: codemap_adapter._sha_equal owns agreement (case-fold + prefix
    # + min length). Length is still classified here so note taxonomy can
    # distinguish incomparable (no comparison ran) from measured divergence.
    from workbay_orchestrator_mcp.orchestration.codemap_adapter import (  # noqa: PLC0415
        _sha_equal,
    )

    indexed_sha = _indexed_sha_from_status(status)
    indexed_sha_readable = indexed_sha is not None
    # Three outcomes, three names: missing side (no mismatch claim), present but
    # too short to compare (incomparable), or a real comparable disagree/agree.
    head_mismatch = False
    sha_incomparable = False
    if not primary_head_sha or not indexed_sha:
        pass
    elif (
        len(str(primary_head_sha).strip()) < _MIN_SHA_COMPARE_LEN
        or len(str(indexed_sha).strip()) < _MIN_SHA_COMPARE_LEN
    ):
        # Present but not comparable → not agreement (silent FRESH is the
        # failure direction that matters for truncated/placeholder values).
        # Not the same as measured divergence: no comparison ran.
        sha_incomparable = True
    else:
        head_mismatch = not _sha_equal(str(primary_head_sha), str(indexed_sha))

    detect_data: dict[str, Any] | None = None
    detect_stale = False
    detect_determined = False
    detect_result = freshness_call(
        _TOOL_DETECT_CHANGES,
        {"project": proj},
    )
    detect_status = "missing"
    if isinstance(detect_result.get("data"), dict):
        detect_data = detect_result["data"]
        detect_status = str(detect_data.get("status") or "missing").strip().lower()
        # Empty or error envelopes cannot establish content freshness, even
        # when the CLI exits successfully. Keep positive stale evidence below.
        detect_determined = (
            detect_result["ok"]
            and detect_status in {"ok", "ready", "success"}
            and "error" not in detect_data
            and isinstance(detect_data.get("changed_files"), list)
            and type(detect_data.get("changed_count")) is int
            and detect_data["changed_count"] >= 0
        )
        changed = detect_data.get("changed_files") or detect_data.get("changes") or []
        changed_count = detect_data.get("changed_count")
        if isinstance(changed_count, int) and changed_count > 0:
            detect_stale = True
        elif isinstance(changed, list) and len(changed) > 0:
            detect_stale = True
        elif bool(detect_data.get("stale")):
            detect_stale = True

    real_stale_cause = (not ready) or explicit_stale or detect_stale
    stale = real_stale_cause or head_mismatch or sha_incomparable
    # Note by cause — do not claim a reindex will clear divergence or incomparable.
    if real_stale_cause:
        note = CODEMAP_STALE_WARNING
    elif head_mismatch:
        note = CODEMAP_DIVERGENCE_NOTE
    elif sha_incomparable:
        note = CODEMAP_INCOMPARABLE_NOTE
    else:
        note = None

    if not detect_determined:
        error = str(detect_result.get("error") or "")
        cause = ("freshness_budget_exhausted" if error == "freshness_budget_exhausted"
                 else "detect_changes_timeout" if error.endswith(":timeout")
                 else "detect_changes_failed")
        if cause == "detect_changes_failed":
            cause += f"; detect_changes status={detect_status}"
            raw_error = detect_data.get("error") if detect_data else None
            if raw_error is not None:
                cause += f"; error={raw_error}"
            if error:
                cause += f"; {error}"
        # Matching live root SHAs cannot establish index-content freshness.
        if not stale:
            stale = None
        note = f"{note}; {cause}" if note else cause

    if not indexed_sha_readable:
        # Available + parsed status but no recognizable sha: make it loud.
        # Silent None made "agrees" and "unreadable" indistinguishable.
        note = f"{note}; {CODEMAP_SHA_UNREADABLE_NOTE}" if note else CODEMAP_SHA_UNREADABLE_NOTE
        logger.warning(
            "codemap index_status carried no readable commit sha; top-level keys=%s",
            sorted(str(k) for k in status.keys()),
        )

    return {
        "available": True,
        "stale": stale,
        "note": note,
        "status": status,
        "detect_changes": detect_data,
        "project": proj,
        "cli_path": resolved_cli,
        "head_sha": head_sha,
        "primary_head_sha": primary_head_sha,
        "indexed_head_sha": indexed_sha,
        "indexed_sha_readable": indexed_sha_readable,
        "project_note": project_note,
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


def _semantic_keywords_for_targets(targets: Sequence[Mapping[str, Any]]) -> list[str]:
    """Derive a small, stable keyword set for the advisory vector lookup."""
    keywords: list[str] = []
    seen: set[str] = set()
    for target in targets:
        for field in ("query", "symbol"):
            raw = str(target.get(field) or "")
            # Split acronym/camel boundaries before treating punctuation and
            # underscores as separators.
            expanded = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", raw)
            expanded = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", expanded)
            for token in re.split(r"[^A-Za-z0-9]+", expanded):
                token = token.lower()
                if len(token) < 3 or token.isdigit() or token in seen:
                    continue
                seen.add(token)
                keywords.append(token)
                if len(keywords) >= SEMANTIC_PRIOR_KEYWORD_LIMIT:
                    return keywords
    return keywords


def _is_test_path(path: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    if not normalized:
        return False
    parts = normalized.lower().split("/")
    if any(part in {"test", "tests"} for part in parts[:-1]):
        return True
    basename = parts[-1]
    return (
        basename == "conftest.py"
        or (basename.startswith("test_") and basename.endswith(".py"))
        or basename.endswith("_test.py")
    )


def _semantic_rows(data: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    """Normalize semantic hits; return None for malformed blocks, [] for empty ones."""
    semantic = data.get("semantic")
    raw_rows: list[Any]
    if isinstance(semantic, list):
        raw_rows = semantic
    elif isinstance(semantic, Mapping):
        rows = semantic.get("rows")
        cols = semantic.get("cols")
        if (
            not isinstance(rows, list)
            or not isinstance(cols, list)
            or not all(isinstance(col, str) for col in cols)
        ):
            return None
        raw_rows = []
        for row in rows:
            if isinstance(row, Mapping):
                raw_rows.append(row)
            elif isinstance(row, (list, tuple)) and cols and len(row) == len(cols):
                raw_rows.append(dict(zip(cols, row, strict=True)))
            else:
                return None
    else:
        return None

    # Validate the entire page before filtering or the display limit can hide
    # malformed rows and turn an invalid response into a successful retrieval.
    if not all(isinstance(raw, Mapping) for raw in raw_rows):
        return None

    results: list[dict[str, Any]] = []
    for raw in raw_rows:
        file_path = str(raw.get("file") or "").strip()
        if not file_path or _is_test_path(file_path):
            continue
        results.append(
            {
                "qn": str(raw.get("qn") or "").strip(),
                "label": str(raw.get("label") or "").strip(),
                "file": file_path,
                "score": raw.get("score"),
            }
        )
        if len(results) >= SEMANTIC_PRIOR_LIMIT:
            break
    return results


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


def _normalize_query_text(query_text: str | None) -> str | None:
    if query_text is None:
        return None
    stripped = str(query_text).strip()
    return stripped or None


def _related_prior_status_note(result: Mapping[str, Any]) -> str:
    status = str(result.get("status") or "error").strip() or "error"
    return f"{RELATED_PRIOR_NOTE_PREFIX}{status}"


def _lookup_related_prior(*, query_text: str, task_ref: str) -> dict[str, Any]:
    """Advisory cosine lookup. Never raises — typed degrade on any failure."""
    try:
        from workbay_handoff_mcp import core as handoff_core

        result = handoff_core.find_related_prior_work(
            text=query_text,
            task_ref=task_ref,
            limit=RELATED_PRIOR_LIMIT,
        )
    except Exception as exc:  # noqa: BLE001 — never raise to dispatch
        logger.warning("related-prior lookup failed: %s", exc)
        return {"status": "error", "results": [], "note": "related_prior:error"}
    if not isinstance(result, dict):
        return {"status": "error", "results": [], "note": "related_prior:error"}
    return result


def lookup_related_prior(*, query_text: str, task_ref: str) -> dict[str, Any]:
    """Shared advisory lookup for dispatch packets and failure results."""
    return _lookup_related_prior(query_text=query_text, task_ref=task_ref)


def _attach_related_prior(
    sections: dict[str, Any],
    notes: list[str],
    *,
    query_text: str,
    task_ref: str,
) -> None:
    result = _lookup_related_prior(query_text=query_text, task_ref=task_ref)
    sections["related_prior"] = result
    notes.append(_related_prior_status_note(result))


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
    query_text: str | None = None,
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
    query_text:
        Optional like-this phrase for advisory related-prior. Blank/None
        skips the cosine lookup. Typed degrade never fails the packet.

    Returns a dict with ``packet`` (str|None), ``packet_bytes``, ``sections``,
    ``notes``, ``available``, and metadata. Never raises for CLI absence.
    """
    resolved_wt = Path(worktree_path).expanduser().resolve()
    resolved_cli = cli_path if cli_path is not None else resolve_codemap_cli()
    # An explicit override wins. Otherwise freshness probes the lane-derived
    # project first and may select the indexed primary project as a fallback.
    proj = project or project_key_for_worktree(resolved_wt)
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
        "semantic_prior": None,
    }
    notes: list[str] = []

    # S12-A-03: cap the target fan-out (each target costs up to 3 CLI calls).
    if len(normalized_targets) > PACKET_MAX_TARGETS:
        dropped_targets = len(normalized_targets) - PACKET_MAX_TARGETS
        normalized_targets = normalized_targets[:PACKET_MAX_TARGETS]
        notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}targets:capped:{dropped_targets}_over_{PACKET_MAX_TARGETS}")

    related_query = _normalize_query_text(query_text)
    if related_query:
        _attach_related_prior(sections, notes, query_text=related_query, task_ref=task_ref)

    if not resolved_cli:
        notes.append(CODEMAP_UNAVAILABLE_NOTE)
        if related_query:
            packet = _render_packet(sections, notes=notes)
            packet, truncated = _cap_packet(packet, max_bytes=max_bytes)
            return {
                "available": False,
                "packet": packet,
                "packet_bytes": len(packet.encode("utf-8")),
                "sections": sections,
                "notes": notes,
                "truncated": truncated,
                "cli_path": None,
                "project": proj,
            }
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
    # indexed head sha + cause) so the consumer sees measured state, not a
    # disclaimer, and _render_packet can branch by cause rather than boolean alone.
    freshness = check_codemap_index_freshness(
        resolved_wt,
        cli_path=resolved_cli,
        project=project,
        deadline=deadline,
    )
    proj = str(freshness.get("project") or proj)
    sections["meta"]["project"] = proj
    project_note = freshness.get("project_note")
    sections["meta"]["project_note"] = str(project_note) if project_note else None
    if project_note:
        notes.append(str(project_note))
    sections["meta"]["index_stale"] = freshness.get("stale") if freshness.get("available") else None
    sections["meta"]["indexed_head_sha"] = freshness.get("indexed_head_sha")
    sections["meta"]["worktree_head_sha"] = freshness.get("head_sha")
    sections["meta"]["primary_head_sha"] = freshness.get("primary_head_sha")
    sections["meta"]["indexed_sha_readable"] = bool(freshness.get("indexed_sha_readable"))
    freshness_note = freshness.get("note")
    sections["meta"]["freshness_note"] = str(freshness_note) if freshness_note else None
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
    for target_index, target in enumerate(normalized_targets):
        query = str(target.get("query") or "")
        search = _budgeted_call(
            _TOOL_SEARCH_GRAPH,
            {"project": proj, "query": query, "limit": 5},
        )
        if search is None:
            remaining_targets = len(normalized_targets) - target_index
            notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}anchors:budget_exhausted:{remaining_targets}")
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
                if search2 is None:
                    remaining_targets = len(normalized_targets) - target_index
                    notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}anchors:budget_exhausted:{remaining_targets}")
                    break
                if search2["ok"] and isinstance(search2.get("data"), dict):
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

    # --- semantic prior via a separate vector-only search_graph call ---
    # Keep this last so deterministic structural sections retain budget priority.
    semantic_keywords = _semantic_keywords_for_targets(normalized_targets)
    if not semantic_keywords:
        notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}semantic_prior:no_keywords")
        sections["semantic_prior"] = {"status": "omitted", "results": []}
    else:
        semantic = _budgeted_call(
            _TOOL_SEARCH_GRAPH,
            {
                "project": proj,
                "semantic_query": semantic_keywords,
                "format": "json",
                "limit": SEMANTIC_PRIOR_LIMIT,
            },
        )
        omission_reason: str | None = None
        semantic_rows: list[dict[str, Any]] | None = None
        if semantic is None:
            omission_reason = "cli_budget_exhausted"
        elif not semantic["ok"]:
            omission_reason = str(semantic.get("error") or "search_graph_failed")
            search_prefix = f"{CODEMAP_SECTION_OMITTED_PREFIX}{_TOOL_SEARCH_GRAPH}:"
            if omission_reason.startswith(search_prefix):
                omission_reason = omission_reason[len(search_prefix) :]
        elif not isinstance(semantic.get("data"), Mapping):
            omission_reason = "invalid_response"
        elif "semantic" not in semantic["data"]:
            omission_reason = "missing_semantic"
        else:
            semantic_rows = _semantic_rows(semantic["data"])
            if semantic_rows is None:
                omission_reason = "invalid_result_block"

        if omission_reason:
            notes.append(f"{CODEMAP_SECTION_OMITTED_PREFIX}semantic_prior:{omission_reason}")
            sections["semantic_prior"] = {"status": "omitted", "results": []}
        else:
            sections["semantic_prior"] = {
                "status": "ok",
                "keywords": semantic_keywords,
                "results": semantic_rows,
            }

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
    meta_hdr = sections.get("meta", {}) if isinstance(sections.get("meta"), Mapping) else {}
    lane_head = meta_hdr.get("worktree_head_sha")
    lines: list[str] = [
        "## Lane context packet (codemap, deterministic)",
        f"task_ref: {meta_hdr.get('task_ref')}",
        f"lane_id: {meta_hdr.get('lane_id')}",
        f"project: {meta_hdr.get('project')}",
        # Lane worktree HEAD is display context (not the freshness compare operand).
        f"lane_worktree_head: {lane_head or 'unknown'}",
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

    semantic_prior = sections.get("semantic_prior")
    if isinstance(semantic_prior, Mapping):
        lines.extend(
            [
                "",
                "### Semantic prior art (codemap, advisory)",
                "note: static per-token vectors; ignores filters; never licenses absence",
            ]
        )
        semantic_hits = semantic_prior.get("results") or []
        rendered = 0
        if isinstance(semantic_hits, list):
            for hit in semantic_hits[:SEMANTIC_PRIOR_LIMIT]:
                if not isinstance(hit, Mapping):
                    continue
                qn = str(hit.get("qn") or "?").strip() or "?"
                label = str(hit.get("label") or "?").strip() or "?"
                file_path = str(hit.get("file") or "?").strip() or "?"
                score = hit.get("score")
                score_text = f"{float(score):.2f}" if isinstance(score, (int, float)) else str(score or "?")
                lines.append(f"- {qn} ({label}) {file_path} score={score_text}")
                rendered += 1
        if rendered == 0:
            lines.append("- (none)")

    related = sections.get("related_prior")
    if isinstance(related, Mapping):
        lines.extend(["", "### Related prior work (advisory)"])
        lines.append(f"status: {related.get('status') or 'unknown'}")
        hits = related.get("results") or []
        rendered = 0
        if isinstance(hits, list):
            for hit in hits[:RELATED_PRIOR_LIMIT]:
                if not isinstance(hit, Mapping):
                    continue
                score = hit.get("score")
                if isinstance(score, (int, float)):
                    score_s = f"{float(score):.3f}"
                else:
                    score_s = str(score or "?")
                kind = str(hit.get("entity_kind") or "?").strip() or "?"
                eid = str(hit.get("entity_id") or "?").strip() or "?"
                snippet = str(hit.get("snippet") or "").replace("\n", " ").strip()
                if len(snippet) > RELATED_PRIOR_SNIPPET_MAX_CHARS:
                    snippet = snippet[:RELATED_PRIOR_SNIPPET_MAX_CHARS]
                lines.append(f"- {score_s} {kind} {eid} {snippet}")
                rendered += 1
        if rendered == 0:
            lines.append("- (none)")

    if notes:
        lines.extend(["", "### Notes"])
        for note in notes:
            lines.append(f"- {note}")

    # S12-A-02: measured index freshness, not a static disclaimer.
    # Branch by cause (meta.freshness_note / indexed_sha_readable), not only
    # the index_stale boolean — reindex remediation is reserved for reindexable
    # causes, and "fresh" is reserved for a real agreeing comparison.
    meta = sections.get("meta", {}) if isinstance(sections.get("meta"), Mapping) else {}
    stale = meta.get("index_stale")
    indexed_sha = meta.get("indexed_head_sha")
    # Compare operand is PRIMARY root HEAD (S6); not the lane worktree HEAD.
    primary_sha = meta.get("primary_head_sha")
    freshness_note = str(meta.get("freshness_note") or "")
    indexed_sha_readable = meta.get("indexed_sha_readable")
    # When meta was hand-built without the flag, treat a missing sha as unreadable
    # so we do not claim a verified agreement over an unmeasured check.
    if indexed_sha_readable is None:
        indexed_sha_readable = indexed_sha is not None and str(indexed_sha).strip() != ""
    lines.append("")
    if stale is True:
        # Divergence-only: reindex cannot clear a different-checkout mismatch.
        divergence_only = CODEMAP_DIVERGENCE_NOTE in freshness_note and CODEMAP_STALE_WARNING not in freshness_note
        # Incomparable-only: length guard refused the comparison; reindex cannot
        # make a truncated/placeholder value comparable.
        incomparable_only = CODEMAP_INCOMPARABLE_NOTE in freshness_note and CODEMAP_STALE_WARNING not in freshness_note
        # Honest phrasing: git.head_sha is a live root probe, not an index-build
        # commit — same wording as the FRESH line ("index root is at"). RHS is
        # the primary HEAD that was actually compared (not the lane worktree).
        if divergence_only:
            lines.append(
                "Index freshness: STALE — index root is at "
                f"{indexed_sha or 'unknown'} vs primary {primary_sha or 'unknown'}; "
                "structural channel covers a different checkout (not cleared by reindex)."
            )
        elif incomparable_only:
            lines.append(
                "Index freshness: STALE — index root is at "
                f"{indexed_sha or 'unknown'} vs primary {primary_sha or 'unknown'}; "
                "commit shas present but not comparable (not cleared by reindex)."
            )
        else:
            lines.append(
                "Index freshness: STALE — index root is at "
                f"{indexed_sha or 'unknown'} vs primary {primary_sha or 'unknown'}; "
                "structural channel may lag the primary root (refresh via index_repository)."
            )
    elif stale is False:
        if not indexed_sha_readable:
            # No sha was readable → no comparison ran. Absence of evidence is
            # not verified agreement; withhold the word "fresh".
            lines.append(
                "Index freshness: undetermined — no readable commit sha (unknown); head comparison was not performed."
            )
        else:
            # Measured agreement: name the live root probe honestly (not "indexed").
            lines.append(f"Index freshness: fresh (index root is at {indexed_sha or 'unknown'}).")
    elif "detect_changes status=" in freshness_note:
        detection_status = freshness_note.split("detect_changes status=", 1)[1].split(";", 1)[0]
        lines.append(f"Index freshness: unknown (detect_changes status={detection_status}) — {freshness_note}.")
    elif any(cause in freshness_note for cause in (
        "freshness_budget_exhausted", "detect_changes_timeout", "detect_changes_failed"
    )):
        lines.append(f"Index freshness: undetermined — {freshness_note}.")
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
