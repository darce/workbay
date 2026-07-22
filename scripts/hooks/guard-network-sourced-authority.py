#!/usr/bin/env python3
"""PreToolUse guard (SEC-07): network-fetched content must not land in
agent-authoritative surfaces.

Invariant
---------
Content obtained from the network never lands in an agent-authoritative
surface. Agent-authoritative surfaces are files agents read as instructions:

* ``docs/workbay/rules/`` (payload SSOT and root mirror)
* skill bodies (``body.md`` / ``SKILL.md`` under a ``skills/`` tree)

Fetched content is fine in a *data* role (e.g. ``docs/notes/``). Writing it
into rules or skill bodies converts untrusted text into agent instructions —
the prompt-injection path this guard closes (SEC-03 / SEC-07).

Detection
---------
On Edit|Write, if the target path is agent-authoritative, the guard scans the
session transcript (``transcript_path``) for network tool results (WebFetch,
WebSearch, curl/wget/gh-api fetches, etc.). A write is **network-sourced** when
the content being written shares a substantial contiguous span with a prior
network tool result.

This is intentionally *not* a filename allowlist of the calling script. A
caller that renames itself does not evade detection; the signal is content
overlap with retrieved bytes, not process identity.

Fail-closed (deliberate inversion of advisory hooks)
----------------------------------------------------
Several existing hooks in this repo ``sys.exit(0)`` on parse failure — correct
for advisory / best-effort instrumentation. This is a **security** guard, so
ambiguity refuses:

* unparseable stdin JSON
* non-object payload
* missing / non-string file path on an Edit|Write to a classifiable surface
* unreadable transcript when the path is agent-authoritative
* unreadable or malformed provenance when a permit is claimed

Exit code (distinct from the rest of the repo)
----------------------------------------------
Every other guard in this repo blocks with exit ``2`` (the Claude Code
PreToolUse "block with stderr reason" convention). This guard blocks SEC-07
violations with exit ``2`` so automated scanners can distinguish a
network→authority refusal from a generic block.

Blocking uses the documented PreToolUse block code ``2``. An earlier draft
used ``3`` for machine-distinctness, but a security guard must not depend on
how a given harness version interprets a nonstandard code — if ``3`` surfaces
as a hook *error* rather than a structured block, the guard fails OPEN, which
is the one outcome it exists to prevent. Machine distinctness is preserved by
the ``SEC-07`` token this guard writes to stderr; scanners match on that.

The one permitted crossing (principled, not by filename)
--------------------------------------------------------
``sync_heuristics_canon.py`` is the legitimate network→rules path. It is
permitted only when *all* of the following hold — none of which is the
script's basename:

1. A provenance sidecar is readable at the well-known path
   ``packages/workbay-system/scripts/.heuristics_canon_state.json``
   (relative to the project root).
2. The sidecar records a ``canon_url``, a **pinned** ``canon_ref`` (not a
   floating branch name like ``main`` / ``master`` / ``HEAD``), and a
   non-empty ``lexicons`` map of ``{"sha": "<hex>"}`` per blob.
3. The content being written hashes (SHA-256 over UTF-8 bytes, matching the
   sync script's ``_content_sha``) to one of those recorded blob shas —
   proving the bytes are the verified fetched blob, not arbitrary text.

Any network-sourced write that fails that check is refused, regardless of
what the caller calls itself.

Hook contract
-------------
  stdin:  Claude Code PreToolUse JSON (tool_name, tool_input, transcript_path)
  stderr: human-readable reason including the concrete offending path
  exit 0 allow; exit 2 SEC-07 block; exit 2 also on fail-closed ambiguity
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

# Distinct from every other hook's block code (2). See module docstring.
EXIT_ALLOW = 0
EXIT_SEC07 = 2

# Contiguous overlap length required to treat write content as network-sourced.
# Short enough to catch a pasted paragraph; long enough to avoid trivial
# common-word false positives.
_OVERLAP_MIN = 80

# Floating refs are not pins — permit requires an immutable commit-ish ref.
_FLOATING_REFS = frozenset({"main", "master", "head", "trunk", "develop", "development"})

# Network-ish tool names (Claude Code + common MCP / shell wrappers).
_NETWORK_TOOL_RE = re.compile(
    r"(?i)^(WebFetch|WebSearch|web_fetch|web_search|Browser|"
    r"mcp__.*__(web_fetch|web_search|fetch|browse).*|"
    r"mcp__.*fetch.*)$"
)
_NETWORK_BASH_RE = re.compile(
    r"(?i)\b(curl|wget|httpie|http\s|fetch\s|gh\s+api\b|npm\s+view\b|"
    r"pip\s+index\b|git\s+clone\b|git\s+fetch\b)\b"
)

_SKILL_BODY_NAMES = frozenset({"body.md", "skill.md", "skill.mdx"})

_PROVENANCE_REL = Path("packages/workbay-system/scripts/.heuristics_canon_state.json")

_HEX_SHA_RE = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)


def content_sha(text: str) -> str:
    """SHA-256 of UTF-8 bytes — same digest family as sync_heuristics_canon._content_sha."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _norm_path(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")


def is_agent_authoritative_path(path: str) -> bool:
    """True when *path* is a rules tree file or a skill body."""
    if not path or not isinstance(path, str):
        return False
    norm = _norm_path(path)
    # Rules SSOT + root mirror (any nesting: monorepo payload path included).
    if "docs/workbay/rules/" in norm or norm.startswith("docs/workbay/rules/"):
        return True
    # Skill bodies under any skills/ tree (payload, root, plugins).
    parts = norm.split("/")
    if "skills" not in parts:
        return False
    name = Path(norm).name
    if name in _SKILL_BODY_NAMES:
        return True
    # Claude / Codex convention: SKILL.md (case-sensitive on disk; compare lower).
    if name.lower() == "skill.md":
        return True
    return False


def is_rules_path(path: str) -> bool:
    norm = _norm_path(path)
    return "docs/workbay/rules/" in norm or norm.startswith("docs/workbay/rules/")


def _payload_value(payload: dict, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return default


def extract_write_target(payload: dict) -> tuple[str | None, str | None, str | None]:
    """Return (file_path, content, error_reason).

    error_reason is set when the payload shape is unexpected for Edit|Write.
    """
    tool_name = str(_payload_value(payload, "tool_name", "toolName", default="") or "")
    tool_input = payload.get("tool_input")
    if tool_input is None:
        tool_input = payload.get("toolInput")
    if tool_input is None:
        # Non-Edit tools may appear under a broad matcher in future; allow only
        # when we can positively say this is not an Edit|Write authority write.
        if tool_name and tool_name not in (
            "Edit",
            "Write",
            "write",
            "edit",
            "create_file",
            "apply_patch",
            "replace_string_in_file",
            "multi_replace_string_in_file",
        ):
            return None, None, None
        return None, None, "missing tool_input"
    if not isinstance(tool_input, dict):
        return None, None, "tool_input is not an object"

    file_path = _payload_value(tool_input, "file_path", "filePath", "path", default="")
    if not isinstance(file_path, str) or not file_path.strip():
        # apply_patch may use a different shape — treat as unclassifiable if we
        # cannot find a path (fail closed only when we cannot rule out authority).
        return None, None, "missing file_path"

    content: str | None = None
    if isinstance(tool_input.get("content"), str):
        content = tool_input["content"]
    elif isinstance(tool_input.get("new_string"), str):
        content = tool_input["new_string"]
    elif isinstance(tool_input.get("newString"), str):
        content = tool_input["newString"]
    else:
        # Edit without new_string / Write without content — unclassifiable.
        return file_path, None, "missing write content (content/new_string)"

    return file_path, content, None


def _iter_jsonl_records(text: str) -> Iterable[dict]:
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def _walk_content_blocks(node: Any) -> Iterable[dict]:
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk_content_blocks(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_content_blocks(item)


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
                else:
                    try:
                        parts.append(json.dumps(item, sort_keys=True))
                    except (TypeError, ValueError):
                        parts.append(str(item))
            else:
                parts.append(str(item))
        return "\n".join(parts)
    if isinstance(value, dict):
        if isinstance(value.get("text"), str):
            return value["text"]
        try:
            return json.dumps(value, sort_keys=True)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def extract_network_result_texts(transcript_text: str) -> list[str]:
    """Pull result/input texts associated with network tools from a transcript.

    Supports Claude Code JSONL (tool_use / tool_result blocks) and a flat
    diagnostic form used by tests: ``{"role":"tool","name":"WebFetch","content":"..."}}``.
    """
    results: list[str] = []
    # Map tool_use id -> whether it was a network tool (for matching tool_result).
    network_ids: set[str] = set()
    pending_network = False

    for rec in _iter_jsonl_records(transcript_text):
        # Flat / test form.
        name = rec.get("name") or rec.get("tool_name") or rec.get("toolName") or ""
        role = str(rec.get("role") or rec.get("type") or "")
        if name and _is_network_tool_name(str(name)):
            blob = _coerce_text(rec.get("content") or rec.get("result") or rec.get("output"))
            if blob.strip():
                results.append(blob)
            pending_network = True
            continue
        if pending_network and role in {"tool", "tool_result", "user"}:
            blob = _coerce_text(rec.get("content") or rec.get("result") or rec.get("output"))
            if blob.strip():
                results.append(blob)
            pending_network = False

        for block in _walk_content_blocks(rec):
            btype = str(block.get("type") or "")
            if btype == "tool_use":
                tname = str(block.get("name") or "")
                tid = str(block.get("id") or "")
                if _is_network_tool_name(tname) or (
                    tname in {"Bash", "bash", "Shell", "shell"}
                    and _NETWORK_BASH_RE.search(_coerce_text(block.get("input")))
                ):
                    if tid:
                        network_ids.add(tid)
                    # Capture URL/query inputs too (weak signal; overlap still required).
                    inp = _coerce_text(block.get("input"))
                    if len(inp) >= _OVERLAP_MIN:
                        results.append(inp)
            elif btype == "tool_result":
                tid = str(block.get("tool_use_id") or block.get("toolUseId") or "")
                if tid in network_ids or not network_ids:
                    # If we cannot correlate ids, still collect large tool_result
                    # only when we already saw a network tool in this transcript.
                    if tid in network_ids or (network_ids and tid == ""):
                        blob = _coerce_text(block.get("content") or block.get("output"))
                        if blob.strip():
                            results.append(blob)
                if tid in network_ids:
                    blob = _coerce_text(block.get("content") or block.get("output"))
                    if blob.strip():
                        results.append(blob)

    # Second pass: any tool_result whose sibling tool_use was network — already
    # handled. Also accept explicit network markers in records.
    return _dedupe_keep_order(results)


def _is_network_tool_name(name: str) -> bool:
    if not name:
        return False
    if _NETWORK_TOOL_RE.match(name):
        return True
    lower = name.lower()
    if "webfetch" in lower or "web_fetch" in lower or "websearch" in lower:
        return True
    if "web_search" in lower:
        return True
    return False


def _dedupe_keep_order(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def has_substantial_overlap(write_content: str, network_blobs: Sequence[str]) -> bool:
    """True if write_content shares a contiguous span ≥ _OVERLAP_MIN with any blob."""
    if not write_content or not network_blobs:
        return False
    write = write_content
    if len(write) < _OVERLAP_MIN:
        # Short writes: require the entire write to appear inside a network blob
        # (and write must be non-trivial).
        if len(write.strip()) < 24:
            return False
        needle = write.strip()
        return any(needle in blob for blob in network_blobs)

    # Sliding windows of _OVERLAP_MIN over the write content.
    # Step by _OVERLAP_MIN // 2 for speed while remaining hard to evade by
    # tiny shifts (evasion would need to fragment every 80-char span).
    step = max(1, _OVERLAP_MIN // 2)
    limit = len(write) - _OVERLAP_MIN + 1
    # Bound work on huge files.
    max_windows = 400
    windows_checked = 0
    for start in range(0, limit, step):
        chunk = write[start : start + _OVERLAP_MIN]
        for blob in network_blobs:
            if chunk in blob:
                return True
        windows_checked += 1
        if windows_checked >= max_windows:
            break
    # Also: entire network blob embedded in the write (fetch dumped into file).
    for blob in network_blobs:
        if len(blob) >= _OVERLAP_MIN and blob[: min(len(blob), 2000)] in write:
            return True
        if _OVERLAP_MIN <= len(blob) < 2000 and blob in write:
            return True
    return False


def is_pinned_ref(canon_ref: str) -> bool:
    if not canon_ref or not isinstance(canon_ref, str):
        return False
    ref = canon_ref.strip()
    if not ref:
        return False
    if ref.lower() in _FLOATING_REFS:
        return False
    # Reject obvious branch-like names with slashes that are not tags? Allow
    # tags like v2.0.0 and full SHAs. Reject empty and floating only.
    return True


def load_provenance(state_path: Path) -> dict | None:
    """Load provenance sidecar. Returns None if missing; raises ValueError if corrupt."""
    if not state_path.is_file():
        return None
    try:
        raw = state_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"unreadable provenance: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"unparseable provenance JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("provenance root is not an object")
    return data


def provenance_permits_content(state: dict, content: str) -> bool:
    """True when state is a pinned, sha-verified record for *content*."""
    canon_url = state.get("canon_url")
    canon_ref = state.get("canon_ref")
    lexicons = state.get("lexicons")
    if not isinstance(canon_url, str) or not canon_url.strip():
        return False
    if not isinstance(canon_ref, str) or not is_pinned_ref(canon_ref):
        return False
    if not isinstance(lexicons, dict) or not lexicons:
        return False
    digest = content_sha(content)
    for _name, entry in lexicons.items():
        if not isinstance(entry, dict):
            return False  # malformed entry → fail closed on permit
        sha = entry.get("sha")
        if not isinstance(sha, str) or not _HEX_SHA_RE.match(sha):
            return False
        if sha.lower() == digest.lower():
            return True
    return False


def resolve_project_root(payload: dict) -> Path:
    env = os.environ.get("CLAUDE_PROJECT_DIR") or os.environ.get("WORKBAY_PROJECT_DIR")
    if env:
        return Path(env).resolve()
    cwd = Path.cwd().resolve()
    # Prefer git root when available without adding dependencies — walk up for .git.
    for candidate in (cwd, *cwd.parents):
        if (candidate / ".git").exists():
            return candidate
        if (candidate / "packages" / "workbay-system").is_dir() and (
            candidate / "scripts" / "hooks"
        ).is_dir():
            return candidate
    return cwd


def refuse(path: str, detail: str) -> int:
    msg = (
        f"SEC-07 network-sourced authority guard: refusing write to agent-authoritative "
        f"path {path!r}: {detail} "
        f"Network-fetched content belongs in a data role (e.g. docs/notes/), not in "
        f"rules or skill bodies. The only permitted rules crossing is a pinned-ref, "
        f"per-blob-sha-verified heuristics-canon sync provenance record."
    )
    print(msg, file=sys.stderr)
    return EXIT_SEC07


def evaluate(
    payload: dict,
    *,
    project_root: Path | None = None,
    transcript_text: str | None = None,
) -> int:
    """Core decision function. Returns exit code. Used by main and tests."""
    if not isinstance(payload, dict):
        return refuse("<unknown>", "payload is not a JSON object (fail-closed)")

    file_path, content, shape_err = extract_write_target(payload)
    if shape_err and file_path is None and content is None:
        # Not an Edit|Write we understand and no path — if tool is clearly
        # non-write, allow; otherwise fail closed.
        tool_name = str(_payload_value(payload, "tool_name", "toolName", default="") or "")
        if tool_name and tool_name not in (
            "Edit",
            "Write",
            "write",
            "edit",
            "create_file",
            "apply_patch",
            "replace_string_in_file",
            "multi_replace_string_in_file",
        ):
            return EXIT_ALLOW
        return refuse("<unknown>", f"unexpected payload shape: {shape_err} (fail-closed)")

    if file_path is None:
        return refuse("<unknown>", f"unexpected payload shape: {shape_err or 'missing path'} (fail-closed)")

    if not is_agent_authoritative_path(file_path):
        return EXIT_ALLOW

    # Authoritative surface — fail closed on missing content / shape.
    if shape_err or content is None:
        return refuse(file_path, f"{shape_err or 'missing content'} (fail-closed)")

    root = project_root or resolve_project_root(payload)

    # Load transcript for network-source detection.
    if transcript_text is None:
        tpath = _payload_value(payload, "transcript_path", "transcriptPath", default="")
        if not tpath or not isinstance(tpath, str):
            return refuse(
                file_path,
                "missing transcript_path; cannot classify network provenance (fail-closed)",
            )
        try:
            transcript_text = Path(tpath).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            return refuse(
                file_path,
                f"unreadable transcript_path {tpath!r}: {exc} (fail-closed)",
            )

    network_blobs = extract_network_result_texts(transcript_text)
    if not has_substantial_overlap(content, network_blobs):
        # Ordinary author edit — no network-sourced content detected.
        return EXIT_ALLOW

    # Network-sourced write into authority surface.
    # Canon permit applies only to rules paths (not skill bodies).
    if is_rules_path(file_path):
        state_path = root / _PROVENANCE_REL
        try:
            state = load_provenance(state_path)
        except ValueError as exc:
            return refuse(
                file_path,
                f"network-sourced content and unreadable provenance record at "
                f"{state_path}: {exc} (fail-closed)",
            )
        if state is not None and provenance_permits_content(state, content):
            return EXIT_ALLOW
        if state is None:
            detail = (
                "network-sourced content without pinned/sha-verified canon provenance "
                f"(missing {state_path})"
            )
        else:
            detail = (
                "network-sourced content does not match any per-blob sha in the pinned "
                f"canon provenance at {state_path}"
            )
        return refuse(file_path, detail)

    return refuse(
        file_path,
        "network-sourced content must not be written into skill bodies",
    )


def main(argv: list[str] | None = None) -> int:
    del argv  # hook protocol is stdin-only
    try:
        raw = sys.stdin.read()
    except OSError as exc:
        print(
            f"SEC-07 network-sourced authority guard: unreadable stdin: {exc} (fail-closed)",
            file=sys.stderr,
        )
        return EXIT_SEC07
    try:
        payload = json.loads(raw) if raw.strip() else None
    except json.JSONDecodeError as exc:
        print(
            f"SEC-07 network-sourced authority guard: unparseable stdin JSON: {exc} "
            f"(fail-closed)",
            file=sys.stderr,
        )
        return EXIT_SEC07
    if payload is None:
        print(
            "SEC-07 network-sourced authority guard: empty stdin (fail-closed)",
            file=sys.stderr,
        )
        return EXIT_SEC07
    if not isinstance(payload, dict):
        print(
            "SEC-07 network-sourced authority guard: stdin JSON is not an object (fail-closed)",
            file=sys.stderr,
        )
        return EXIT_SEC07

    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(payload, expected="PreToolUse")
    except ImportError:
        pass
    except Exception:
        # Protocol validation must not open the guard; ignore contract helper errors.
        pass

    return evaluate(payload)


if __name__ == "__main__":
    raise SystemExit(main())
