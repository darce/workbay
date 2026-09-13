#!/usr/bin/env python3
"""PreToolUse guard: refuse Edit/Write to managed generated renders.

Closes NO-WRITE-TIME-GUARD-ON-MANAGED-GENERATED-RENDERS-01 (ARCH-13).

Invariant
---------
A managed generated render is never hand-edited. The contract at
``docs/workbay/contracts/harness-protocol.yaml`` is the single source of
truth; renders are regenerated from it. Hand-edits silently drift the
render, break byte-exact goldens, and only surface when a human remembers
to run the hooks suite — which is how the original defect survived a
merge-readiness pass.

Detection (both arms required; Arm A alone is insufficient)
-----------------------------------------------------------
**Arm A — content marker.** The target file's *current on-disk content*
carries a managed marker:

* the literal ``_managed_by`` key (JSON / YAML style), or
* an HTML comment fence of the form ``BEGIN GENERATED`` / ``END GENERATED``.

For a fenced file such as ``CLAUDE.md``, refuse **only** when the edit
lands inside a fenced region. Edits outside the fence are legitimate and
must pass (partial-file case is mandatory).

**Arm B — contract-derived path set.** Read
``docs/workbay/contracts/harness-protocol.yaml`` at runtime and derive
the set of render paths from fields that name them:

* ``harness_capabilities.plugin_activation.rows[*].config_path``
  (file paths ending in ``settings.json``)
* ``branch_isolation.permitted_main_surfaces`` exact (non-glob) patterns
  under harness config dirs that name settings/hooks JSON
* ``bootstrap_overlay.tracked_vs_overlay_boundary.tracked_overlay_source_paths``
  entries that end in ``hooks.json``

Do **not** hardcode a second list of render paths (DATA-14 / REF-09).
If a contract-declared render path is absent from disk, Arm B still
matches by path and does not crash (missing files are expected for
gitignored surfaces such as ``.codex/hooks.json``).

Arm B is required because only ``.claude/settings.json`` carries
``_managed_by``; ``.claude/settings.hooks.json`` carries no marker at all.

Fail-closed (OBS-08)
--------------------
Exit 2 with an actionable reason on stderr naming:

(a) the source file to edit instead
    (``docs/workbay/contracts/harness-protocol.yaml``)
(b) the exact regenerate command for that render

No environment-variable escape hatch. None of the sibling PreToolUse
guards has one; an escape hatch would restore the silent-bypass this
finding exists to close.

Operating modes
---------------
1. **Claude Code hook** (default): reads the PreToolUse JSON payload from
   stdin and inspects ``tool_input.file_path`` plus Edit/Write content
   fields. Exits 2 with an actionable reason on stderr to block the tool
   call; exits 0 to allow (silent on allow).

2. ``--scan-paths <path> [...]``: evaluates each path as if written
   (full-file), useful for ad-hoc sweeps.

Hook contract
-------------
  stdin:  Claude Code PreToolUse JSON (tool_name, tool_input)
  stderr: human-readable reason including source path + regenerate command
  exit 0 allow; exit 2 refuse (and on fail-closed ambiguity for Edit|Write
  to a classifiable managed surface when the path cannot be resolved)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

EXIT_ALLOW = 0
EXIT_BLOCK = 2

CONTRACT_RELATIVE = Path("docs/workbay/contracts/harness-protocol.yaml")
SOURCE_HINT = "docs/workbay/contracts/harness-protocol.yaml"

# Payload twin of every monorepo-root render lives under the shipped payload
# tree (DATA-14). Arm B must cover both anchors (SECD-03 / WEB-33).
_PAYLOAD_TWIN_PREFIX = "packages/workbay-system/workbay_system/payload/"

# Package-internal standalone router (generator default without --target).
_PACKAGE_CODEX_ROUTER = (
    "packages/workbay-system/docs/workbay/generated/codex-command-router.md"
)
_ROOT_CODEX_ROUTER = "docs/workbay/generated/codex-command-router.md"

# Last-known managed render roots used when the contract is unreadable
# (SECD-05 fail-closed). Expanded with payload twins at load time.
_LAST_KNOWN_ROOT_RENDERS = frozenset(
    {
        ".claude/settings.json",
        ".claude/settings.hooks.json",
        ".codex/hooks.json",
        ".cursor/hooks.json",
        ".github/hooks/terminal-guard.json",
        _ROOT_CODEX_ROUTER,
        _PACKAGE_CODEX_ROUTER,
    }
)

# Marker arm (Arm A).
_MANAGED_BY_RE = re.compile(r"""["']?_managed_by["']?\s*:""")
_BEGIN_FENCE_RE = re.compile(
    r"<!--\s*BEGIN GENERATED(?:\s*:[^>]*)?\s*-->", re.IGNORECASE
)
_END_FENCE_RE = re.compile(
    r"<!--\s*END GENERATED(?:\s*:[^>]*)?\s*-->", re.IGNORECASE
)

# Verified regenerate commands (CLM — each was run and observed to rewrite
# the named path class). Bare generate heals the payload twin only; root
# renders require WORKFLOW_TARGET_ROOT=. (generator --target).
_REGEN_GENERATE_PAYLOAD = "make generate-agent-workflows"
_REGEN_GENERATE_ROOT = "make generate-agent-workflows WORKFLOW_TARGET_ROOT=."

# Root-relative paths that ``generate_agent_workflows._expected_hooks_outputs``
# (and the codex-router emission) actually rewrite when pointed at a root.
_GENERATOR_REWRITTEN_ROOTS = frozenset(
    {
        ".claude/settings.hooks.json",
        ".codex/hooks.json",
        ".github/hooks/terminal-guard.json",
        _ROOT_CODEX_ROUTER,
    }
)

_EDIT_WRITE_TOOLS = frozenset(
    {
        "Edit",
        "Write",
        "write",
        "edit",
        "create_file",
        "apply_patch",
        "replace_string_in_file",
        "multi_replace_string_in_file",
    }
)


def _norm_path(path: str) -> str:
    # Strip surrounding whitespace (WEB-13 — trailing-space path bypass),
    # then a leading ``./`` prefix only. Do NOT use ``lstrip("./")`` —
    # that treats the characters as a set and would turn ``.claude/x``
    # into ``claude/x``.
    # Then collapse ``.`` / ``..`` *lexically* (WEB-13) without touching the
    # filesystem. ``Path.resolve()`` does not collapse through a missing
    # intermediate segment, so ``nosuchdir/../.claude/settings.hooks.json``
    # would otherwise defeat both Arm B membership and Arm A ``read_on_disk``.
    norm = path.strip().replace("\\", "/")
    while norm.startswith("./"):
        norm = norm[2:]
    if not norm:
        return ""
    norm = os.path.normpath(norm).replace("\\", "/")
    # ``normpath(".")`` is ``"."``; treat that as empty relative.
    if norm == ".":
        return ""
    return norm


def _payload_value(payload: dict, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return default


def _path_in_render_set(norm: str, render_paths: frozenset[str]) -> bool:
    """Arm B membership with case-insensitive fallback (WEB-13).

    ``Path.resolve()`` does not rewrite case on case-insensitive
    filesystems, so a variant like ``.CLAUDE/settings.hooks.json`` can
    resolve to the managed file while missing a case-sensitive set
    compare. Exact match is preferred; casefold is the safety net.

    Absolute forms are also matched by path suffix against relative Arm B
    entries (D3 / SECD-05): when ``repo_root`` is empty, classify may leave
    a resolved absolute path that is not a member of the relative last-known
    set even though it names a managed render.
    """
    if not norm:
        return False
    if norm in render_paths:
        return True
    folded = norm.casefold()
    if any(p.casefold() == folded for p in render_paths):
        return True
    # Suffix match: absolute (or otherwise prefixed) path ending in a known
    # relative managed render. Prefer longer candidates so payload twins win
    # over root-anchored names when both would match.
    norm_slash = norm.replace("\\", "/")
    for p in sorted(render_paths, key=len, reverse=True):
        if not p:
            continue
        suffix = "/" + p.replace("\\", "/")
        if norm_slash.endswith(suffix) or norm_slash.casefold().endswith(
            suffix.casefold()
        ):
            return True
    return False


# apply_patch blob headers (Codex / OpenAI apply_patch style).
_APPLY_PATCH_PATH_RE = re.compile(
    r"^\*\*\*\s+(?:Update|Add|Delete)\s+File:\s*(.+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)


def extract_apply_patch_paths(tool_input: dict) -> tuple[list[str], str | None]:
    """Return ``(paths, error)`` extracted from an apply_patch tool_input.

    Paths come from ``*** Update File:`` / ``*** Add File:`` /
    ``*** Delete File:`` headers. When no path is classifiable the
    caller must fail closed (SECD-03).
    """
    patch = _payload_value(
        tool_input,
        "patch",
        "input",
        "content",
        "diff",
        default="",
    )
    if not isinstance(patch, str) or not patch.strip():
        # Some hosts put a single path alongside an empty patch field.
        single = _payload_value(
            tool_input, "file_path", "filePath", "path", default=""
        )
        if isinstance(single, str) and single.strip():
            return [_norm_path(single)], None
        return [], "apply_patch payload has no patch blob or file_path"

    paths: list[str] = []
    seen: set[str] = set()
    for match in _APPLY_PATCH_PATH_RE.finditer(patch):
        raw = match.group(1).strip().strip('"').strip("'")
        if not raw:
            continue
        norm = _norm_path(raw)
        if norm not in seen:
            seen.add(norm)
            paths.append(norm)
    if not paths:
        return [], "apply_patch paths are not classifiable"
    return paths, None


def _git_repo_root() -> str:
    try:
        from resolve_handoff_src import resolve_harness_workspace_root

        return resolve_harness_workspace_root()
    except Exception:  # noqa: BLE001 — keep guard self-contained
        pass
    import os
    import subprocess

    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    for var in ("CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT"):
        value = os.environ.get(var)
        if value and value.strip():
            return value.strip()
    return ""


def _to_repo_relative(path: str, repo_root: str) -> str:
    if not path:
        return path
    norm = path.strip().replace("\\", "/")
    if not repo_root:
        return _norm_path(norm)
    root = repo_root.replace("\\", "/").rstrip("/")
    if norm.startswith(root + "/"):
        return _norm_path(norm[len(root) + 1 :])
    # Absolute path outside repo — keep basename-normalized form.
    try:
        resolved = Path(path.strip()).resolve()
        root_path = Path(repo_root).resolve()
        return str(resolved.relative_to(root_path)).replace("\\", "/")
    except (OSError, ValueError):
        return _norm_path(norm)


def classify_write_path(path: str, repo_root: str) -> tuple[str, str]:
    """Shared path classification for hook and scan modes (SECD-03).

    Returns ``(abs_path, rel_path)``. When *path* exists (including as a
    symlink), resolve to the real path so both modes agree on the managed
    target. Missing paths are still normalized so Arm B can match by
    declared path without requiring the file on disk.

    Lexical ``..`` collapse (WEB-13) runs *before* existence checks, Arm B
    membership, and ``read_on_disk`` so a missing intermediate segment
    cannot leave a literal ``nosuchdir/../managed`` form that bypasses both
    arms.
    """
    raw = (path or "").strip()
    if not raw:
        return "", ""
    # WEB-13: canonicalize lexically first (os.path.normpath / PurePath
    # semantics — not Path.resolve(), which fails open on missing segments).
    collapsed = _norm_path(raw)
    if not collapsed:
        return "", ""
    p = Path(collapsed)
    try:
        if p.exists():
            abs_path = str(p.resolve())
        elif repo_root and not p.is_absolute():
            candidate = Path(repo_root) / collapsed
            abs_path = str(candidate.resolve()) if candidate.exists() else str(candidate)
        elif p.is_absolute():
            abs_path = str(p)
        elif repo_root:
            abs_path = str(Path(repo_root) / collapsed)
        else:
            abs_path = collapsed
    except OSError:
        abs_path = collapsed if p.is_absolute() else (
            str(Path(repo_root) / collapsed) if repo_root else collapsed
        )
    rel_path = _to_repo_relative(abs_path, repo_root)
    return abs_path, rel_path


# ---------------------------------------------------------------------------
# Arm A — content markers
# ---------------------------------------------------------------------------


def has_managed_by_marker(text: str) -> bool:
    """True when *text* carries a ``_managed_by`` key."""
    if not text:
        return False
    return _MANAGED_BY_RE.search(text) is not None


def find_generated_fences(text: str) -> list[tuple[int, int]]:
    """Return ``(start, end)`` character spans for each BEGIN/END GENERATED fence.

    Spans are half-open ``[start, end)`` over the full fence including markers.
    Unmatched BEGIN markers are treated as running to end-of-file (fail closed
    on the remainder of the file).
    """
    if not text:
        return []
    begins = list(_BEGIN_FENCE_RE.finditer(text))
    ends = list(_END_FENCE_RE.finditer(text))
    if not begins:
        return []
    spans: list[tuple[int, int]] = []
    end_iter = iter(ends)
    next_end = next(end_iter, None)
    for begin in begins:
        # Advance end markers that end before this begin.
        while next_end is not None and next_end.start() < begin.start():
            next_end = next(end_iter, None)
        if next_end is None:
            spans.append((begin.start(), len(text)))
            break
        spans.append((begin.start(), next_end.end()))
        next_end = next(end_iter, None)
    return spans


def has_generated_fence(text: str) -> bool:
    return bool(find_generated_fences(text))


def span_overlaps_fences(start: int, end: int, fences: list[tuple[int, int]]) -> bool:
    for f_start, f_end in fences:
        if start < f_end and end > f_start:
            return True
    return False


def edit_touches_generated_fence(existing: str, old_string: str) -> bool:
    """True when an Edit's ``old_string`` overlaps a generated fence region.

    Fail closed (SECD-05) when ``old_string`` is empty or cannot be located
    in a fenced file — unlocatable edits must not slip past the guard.
    Every non-overlapping occurrence is checked (WEB-24); a first-match
    only scan would miss a later hit inside a fence.
    """
    fences = find_generated_fences(existing)
    if not fences:
        return False
    if not old_string:
        # Empty old_string is unlocatable — fail closed on fenced content.
        return True
    step = max(len(old_string), 1)
    start = 0
    found = False
    while True:
        idx = existing.find(old_string, start)
        if idx < 0:
            break
        found = True
        if span_overlaps_fences(idx, idx + len(old_string), fences):
            return True
        start = idx + step
    if not found:
        # Unlocatable edit on a fenced file — fail closed.
        return True
    return False


def new_string_introduces_generated_markers(new_string: str | None) -> bool:
    """True when *new_string* would inject BEGIN/END GENERATED markers."""
    if not new_string:
        return False
    return (
        _BEGIN_FENCE_RE.search(new_string) is not None
        or _END_FENCE_RE.search(new_string) is not None
    )


# ---------------------------------------------------------------------------
# Arm B — contract-derived render paths
# ---------------------------------------------------------------------------


def _parse_yaml_mapping(contract_path: Path) -> dict[str, Any] | None:
    """Load the contract as a mapping. Prefer PyYAML; salvage via line-scan.

    On ``ImportError`` (no PyYAML) or ``yaml.YAMLError`` (malformed document),
    attempt ``_parse_contract_paths_fallback`` so Arm B can still recover path
    fields (SECD-05). Returns None only when the file is unreadable or the
    document is not a mapping and salvage found nothing usable.
    """
    try:
        text = contract_path.read_text(encoding="utf-8")
    except OSError:
        return None
    if not text.strip():
        return None
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError:
        # D2 / SECD-05: same empty-derive promotion as YAMLError salvage —
        # an ImportError line-scan that yields zero managed paths must not
        # return a sparse dict that empties Arm B with no last-known fallback.
        fb = _parse_contract_paths_fallback(text)
        if not derive_render_paths(fb):
            return None
        return fb
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        # Defect 6 / SECD-05: do not discard the line-scan fallback when the
        # installed PyYAML rejects the document. If salvage recovers no managed
        # paths, return None so load_contract_render_paths fail-closes on the
        # last-known set rather than degrading open.
        fb = _parse_contract_paths_fallback(text)
        if not derive_render_paths(fb):
            return None
        return fb
    if not isinstance(data, dict):
        return None
    return data


def _parse_contract_paths_fallback(text: str) -> dict[str, Any]:
    """Minimal line scan when PyYAML is unavailable.

    Reconstructs only the fields Arm B needs so the guard still derives
    paths without a third-party dependency.
    """
    # Synthetic structure matching the keys derive_render_paths reads.
    plugin_rows: list[dict[str, str]] = []
    permitted: list[dict[str, str]] = []
    tracked: list[str] = []

    in_plugin_activation = False
    in_permitted = False
    in_tracked = False

    for raw in text.splitlines():
        stripped = raw.split("#", 1)[0].rstrip()
        if not stripped.strip():
            continue
        indent = len(raw) - len(raw.lstrip(" "))

        if indent == 0:
            in_plugin_activation = False
            in_permitted = False
            in_tracked = False
            continue

        body = stripped.strip()

        if body == "plugin_activation:":
            in_plugin_activation = True
            in_permitted = False
            in_tracked = False
            continue
        if body == "permitted_main_surfaces:":
            in_permitted = True
            in_plugin_activation = False
            in_tracked = False
            continue
        if body == "tracked_overlay_source_paths:":
            in_tracked = True
            in_plugin_activation = False
            in_permitted = False
            continue
        if body.endswith(":") and indent <= 4 and body not in (
            "rows:",
            "fields:",
            "notes:",
        ):
            # Leaving a sibling section under a parent block.
            if indent <= 2:
                in_plugin_activation = False
                in_permitted = False
                in_tracked = False

        if in_plugin_activation and body.startswith("config_path:"):
            value = body.split(":", 1)[1].strip().strip("\"'")
            if value:
                plugin_rows.append({"config_path": value})
            continue

        if in_permitted and body.startswith("- pattern:"):
            value = body.split(":", 1)[1].strip().strip("\"'")
            if value:
                permitted.append({"pattern": value})
            continue

        if in_tracked and body.startswith("- "):
            value = body[2:].strip().strip("\"'")
            if value:
                tracked.append(value)
            continue

    return {
        "harness_capabilities": {
            "plugin_activation": {"rows": plugin_rows},
        },
        "branch_isolation": {"permitted_main_surfaces": permitted},
        "bootstrap_overlay": {
            "tracked_vs_overlay_boundary": {
                "tracked_overlay_source_paths": tracked,
            }
        },
    }


def _is_exact_path(pattern: str) -> bool:
    return bool(pattern) and not any(ch in pattern for ch in "*?[")


def _is_harness_settings_or_hooks_path(path: str) -> bool:
    """True for full-file managed harness config / generated-router renders.

    CLAUDE.md is intentionally excluded — it is partial-file fenced (Arm A).
    """
    norm = _norm_path(path)
    # Strip a leading payload-twin prefix so the same predicate covers both
    # monorepo-root and payload-anchored forms.
    if norm.startswith(_PAYLOAD_TWIN_PREFIX):
        norm = norm[len(_PAYLOAD_TWIN_PREFIX) :]
    name = Path(norm).name
    if name in {"settings.json", "settings.hooks.json"}:
        return True
    if name == "hooks.json":
        return True
    if name == "terminal-guard.json" and (
        norm.startswith(".github/hooks/") or "/.github/hooks/" in f"/{norm}"
    ):
        return True
    if name == "codex-command-router.md" and "docs/workbay/generated" in norm:
        return True
    return False


def _expand_with_payload_twins(paths: set[str]) -> frozenset[str]:
    """Add payload twins of root-anchored renders (SECD-03 / WEB-33 / DATA-14)."""
    out = set(paths)
    for p in list(paths):
        if p.startswith(_PAYLOAD_TWIN_PREFIX):
            continue
        # Package-internal paths are not root-anchored; do not twin them.
        if p.startswith("packages/"):
            continue
        out.add(_PAYLOAD_TWIN_PREFIX + p)
    # Generator default (no --target) also writes the package-internal router.
    if _ROOT_CODEX_ROUTER in out or any(
        p.endswith("/" + _ROOT_CODEX_ROUTER) or p == _ROOT_CODEX_ROUTER for p in out
    ):
        out.add(_PACKAGE_CODEX_ROUTER)
    return frozenset(out)


def _last_known_render_paths() -> frozenset[str]:
    """Fail-closed Arm B set when the contract is absent/empty/unreadable."""
    return _expand_with_payload_twins(set(_LAST_KNOWN_ROOT_RENDERS))


def derive_render_paths(contract: dict[str, Any]) -> frozenset[str]:
    """Derive managed render paths from contract fields (Arm B).

    Paths come from the contract itself so this set cannot drift from the
    fields that already name harness config surfaces (DATA-14). Every
    root-anchored render is expanded with its payload twin so tracked
    payload goldens are authorized at the same reference monitor (SECD-03).
    """
    paths: set[str] = set()

    rows = (
        (contract.get("harness_capabilities") or {})
        .get("plugin_activation", {})
        .get("rows")
        or []
    )
    for row in rows:
        if not isinstance(row, dict):
            continue
        cp = row.get("config_path")
        if isinstance(cp, str) and cp and not cp.endswith("/"):
            if _is_harness_settings_or_hooks_path(cp):
                paths.add(_norm_path(cp))

    permitted = (
        (contract.get("branch_isolation") or {}).get("permitted_main_surfaces") or []
    )
    for surface in permitted:
        if not isinstance(surface, dict):
            continue
        pattern = surface.get("pattern")
        if not isinstance(pattern, str) or not _is_exact_path(pattern):
            continue
        if _is_harness_settings_or_hooks_path(pattern):
            paths.add(_norm_path(pattern))

    tracked = (
        (contract.get("bootstrap_overlay") or {})
        .get("tracked_vs_overlay_boundary", {})
        .get("tracked_overlay_source_paths")
        or []
    )
    for entry in tracked:
        if isinstance(entry, str) and _is_harness_settings_or_hooks_path(entry):
            paths.add(_norm_path(entry))

    return _expand_with_payload_twins(paths)


def load_contract_render_paths(repo_root: str) -> frozenset[str]:
    """Load the contract and return Arm B path set.

    Fail closed (SECD-05): when the contract is absent, empty, unreadable,
    or parses to zero managed paths (sparse/wrong-shape mapping), return the
    last-known managed render set rather than an empty set that would
    degrade Arm B to allow (D2).
    """
    last_known = _last_known_render_paths()
    if not repo_root:
        return last_known
    contract_path = Path(repo_root) / CONTRACT_RELATIVE
    if not contract_path.is_file():
        return last_known
    data = _parse_yaml_mapping(contract_path)
    if data is None:
        return last_known
    derived = derive_render_paths(data)
    # D2 / SECD-05: a successfully parsed but sparse contract must not empty
    # Arm B. YAMLError salvage already promotes empty derive to None→last
    # known; apply the same empty-check on the happy path.
    if not derived:
        return last_known
    return derived


# ---------------------------------------------------------------------------
# Refusal messaging
# ---------------------------------------------------------------------------


def regenerate_command_for(rel_path: str) -> str | None:
    """Return a verified rewrite command for *rel_path*, or None if none.

    CLM: only name a command after observing it rewrite the same
    repo-relative path. Verified (worktree root):

    * Root ``.claude/settings.hooks.json``, ``.codex/hooks.json``,
      ``.github/hooks/terminal-guard.json``,
      ``docs/workbay/generated/codex-command-router.md``, and the
      ``CLAUDE.md`` codex-command-router fence — rewritten by
      ``make generate-agent-workflows WORKFLOW_TARGET_ROOT=.``
      (bare generate does **not** rewrite root renders).
    * Payload twins under
      ``packages/workbay-system/workbay_system/payload/`` and the
      package-internal router
      ``packages/workbay-system/docs/workbay/generated/codex-command-router.md``
      — rewritten by bare ``make generate-agent-workflows``.
    * ``.claude/settings.json`` — no verified wholesale writer; deferred
      composition was never finished. Returns None.
    * ``.cursor/hooks.json`` — listed in the contract but not emitted by
      ``_expected_hooks_outputs``; no verified writer. Returns None.
    """
    norm = _norm_path(rel_path)
    is_payload = norm.startswith(_PAYLOAD_TWIN_PREFIX)
    base = norm[len(_PAYLOAD_TWIN_PREFIX) :] if is_payload else norm

    if base == ".claude/settings.json":
        return None
    if base == ".cursor/hooks.json":
        return None

    if base in _GENERATOR_REWRITTEN_ROOTS or base == ".claude/settings.hooks.json":
        return _REGEN_GENERATE_PAYLOAD if is_payload else _REGEN_GENERATE_ROOT

    if norm == _PACKAGE_CODEX_ROUTER:
        return _REGEN_GENERATE_PAYLOAD

    if norm == "CLAUDE.md" or norm.endswith("/CLAUDE.md"):
        return _REGEN_GENERATE_ROOT

    # Unknown managed path: do not invent a command (CLM).
    return None


def format_refusal(rel_path: str, *, reason: str) -> str:
    regen = regenerate_command_for(rel_path)
    header = (
        f"Refusing Edit/Write to managed generated render: `{rel_path}`\n"
        f"\n"
        f"Reason: {reason}\n"
        f"\n"
        f"Do not hand-edit this file. Edit the contract source instead:\n"
        f"  {SOURCE_HINT}\n"
        f"\n"
    )
    if regen:
        return header + f"Then regenerate with:\n  {regen}\n"
    # Honest no-writer text. Mentions check-harness-sync only as verify-only
    # so operators are not sent to a no-op regenerator (CLM / defect 5).
    return (
        header
        + "No verified regenerate command rewrites this path "
        + "(make check-harness-sync is verify-only and does not rewrite it).\n"
    )


# ---------------------------------------------------------------------------
# Decision
# ---------------------------------------------------------------------------


def read_on_disk(path: str) -> str | None:
    """Return file text, or None if missing/unreadable (do not crash)."""
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return None


def should_refuse(
    *,
    rel_path: str,
    abs_path: str,
    tool_name: str,
    old_string: str | None,
    render_paths: frozenset[str],
    new_string: str | None = None,
) -> tuple[bool, str]:
    """Return ``(refuse, reason)`` for a candidate write.

    Arm B matches on path alone (file need not exist). Arm A inspects
    on-disk content when the file is present; missing files skip Arm A.
    """
    norm = _norm_path(rel_path)
    on_disk = read_on_disk(abs_path)

    # --- Arm B: contract-derived path set (full-file refuse) ---
    if _path_in_render_set(norm, render_paths):
        return True, (
            "path is a contract-derived managed render "
            f"(declared in {SOURCE_HINT})"
        )

    # D3 / SECD-05 / WEB-33: when repo root was missing, classify may leave an
    # absolute path that is not a member of the relative last-known set.
    # Suffix matching in _path_in_render_set covers last-known names; any
    # remaining classifiable managed harness config still fails closed rather
    # than degrading Arm B open for markerless renders.
    try:
        norm_is_absolute = Path(norm).is_absolute()
    except (OSError, ValueError):
        norm_is_absolute = norm.startswith("/")
    if norm_is_absolute and _is_harness_settings_or_hooks_path(norm):
        return True, (
            "path names a managed harness config under an unresolved repo "
            "root; fail closed (SECD-05)"
        )

    # --- Arm A: content markers on current on-disk content ---
    if on_disk is None:
        return False, ""

    if has_managed_by_marker(on_disk):
        return True, "on-disk content carries `_managed_by` managed marker"

    if has_generated_fence(on_disk):
        is_edit = tool_name in {
            "Edit",
            "edit",
            "replace_string_in_file",
        }
        if is_edit:
            if edit_touches_generated_fence(on_disk, old_string or ""):
                return True, (
                    "edit lands inside a BEGIN GENERATED / END GENERATED fence"
                )
            # new_string must not inject fence markers into a file already
            # in the generated-render regime (WEB-33).
            if new_string_introduces_generated_markers(new_string):
                return True, (
                    "edit new_string introduces BEGIN GENERATED / END GENERATED "
                    "markers into a generated-render regime file"
                )
            # Edit outside the fence is legitimate.
            return False, ""
        # Write / full-file replacement of a fenced file would overwrite
        # the generated region — refuse.
        return True, (
            "file carries a BEGIN GENERATED / END GENERATED fence; "
            "full-file Write would overwrite the managed region"
        )

    return False, ""


def extract_write_target(
    payload: dict,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Return ``(tool_name, file_path, old_string, new_string, error_reason)``.

    ``error_reason`` is set for unparseable Edit|Write shapes that still
    look like write tools — callers must fail closed (SECD-05). Non-write
    tools return all-None with no error so the hook allows them silently.

    For ``apply_patch``, ``file_path`` may be None when multiple paths are
    present; use :func:`extract_apply_patch_paths` in that case. When the
    patch yields exactly one path it is returned here for convenience.
    """
    tool_name = str(_payload_value(payload, "tool_name", "toolName", default="") or "")
    tool_input = payload.get("tool_input")
    if tool_input is None:
        tool_input = payload.get("toolInput")
    if tool_input is None:
        if tool_name in _EDIT_WRITE_TOOLS:
            return tool_name, None, None, None, "missing tool_input"
        return None, None, None, None, None
    if not isinstance(tool_input, dict):
        if tool_name in _EDIT_WRITE_TOOLS:
            return tool_name, None, None, None, "tool_input is not an object"
        return None, None, None, None, None

    # apply_patch carries paths inside the patch blob, not file_path.
    if tool_name == "apply_patch":
        paths, patch_err = extract_apply_patch_paths(tool_input)
        if patch_err:
            return tool_name, None, None, None, patch_err
        if not paths:
            return tool_name, None, None, None, "apply_patch paths are not classifiable"
        # Single-path convenience; multi-path handled by the hook loop.
        return tool_name, paths[0] if len(paths) == 1 else None, None, None, None

    file_path = _payload_value(tool_input, "file_path", "filePath", "path", default="")
    if not isinstance(file_path, str) or not file_path.strip():
        if tool_name in _EDIT_WRITE_TOOLS:
            return tool_name, None, None, None, "missing file_path"
        return None, None, None, None, None

    # Always return the stripped path so Arm B membership cannot miss a
    # trailing-whitespace variant (WEB-13).
    file_path = file_path.strip()

    old_string: str | None = None
    raw_old = tool_input.get("old_string")
    if raw_old is None:
        raw_old = tool_input.get("oldString")
    if isinstance(raw_old, str):
        old_string = raw_old

    new_string: str | None = None
    raw_new = tool_input.get("new_string")
    if raw_new is None:
        raw_new = tool_input.get("newString")
    if isinstance(raw_new, str):
        new_string = raw_new

    return tool_name or "Write", file_path, old_string, new_string, None


# ---------------------------------------------------------------------------
# Hook entry points
# ---------------------------------------------------------------------------


def evaluate_path(
    path: str,
    *,
    repo_root: str,
    tool_name: str = "Write",
    old_string: str | None = None,
    new_string: str | None = None,
    render_paths: frozenset[str] | None = None,
) -> tuple[bool, str, str]:
    """Evaluate a single path. Returns ``(refuse, rel_path, reason)``.

    Both hook and scan modes route through :func:`classify_write_path` so
    symlink resolution and path normalization cannot diverge (SECD-03).
    """
    abs_path, rel_path = classify_write_path(path, repo_root)
    if render_paths is None:
        render_paths = load_contract_render_paths(repo_root)
    refuse, reason = should_refuse(
        rel_path=rel_path,
        abs_path=abs_path,
        tool_name=tool_name,
        old_string=old_string,
        new_string=new_string,
        render_paths=render_paths,
    )
    return refuse, rel_path, reason


def _format_parse_refusal(tool_name: str | None, error: str) -> str:
    name = tool_name or "Edit|Write"
    return (
        f"Refusing {name}: unparseable payload ({error}).\n"
        f"For a tool in the Edit|Write family the guard fails closed when "
        f"the payload cannot be classified (SECD-05).\n"
    )


def _run_claude_hook() -> int:
    # D4 judgement (SECD-05): fail *closed* on unparseable PreToolUse
    # envelopes. Rationale — this process is a reference monitor for
    # Edit|Write; silent allow on invalid JSON or a non-object root lets an
    # adversary bypass every arm by sending garbage. Non-tool traffic is not
    # a supported stdin shape for this hook (scan mode uses --scan-paths).
    # Chosen deliberately over fail-open; do not revert without a documented
    # alternate traffic contract.
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        sys.stderr.write(
            "Refusing: PreToolUse stdin is not valid JSON "
            "(SECD-05 fail-closed on unparseable envelope; D4).\n"
        )
        return EXIT_BLOCK
    if not isinstance(data, dict):
        sys.stderr.write(
            "Refusing: PreToolUse stdin JSON must be an object "
            "(SECD-05 fail-closed on unparseable envelope; D4).\n"
        )
        return EXIT_BLOCK

    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(data, expected="PreToolUse")
    except ImportError:
        pass
    except Exception:  # noqa: BLE001 — protocol mismatch must not fail open for writes
        pass

    tool_name, file_path, old_string, new_string, error = extract_write_target(data)
    if error:
        # Unclassifiable Edit|Write — fail closed (SECD-05).
        sys.stderr.write(_format_parse_refusal(tool_name, error))
        return EXIT_BLOCK

    repo_root = _git_repo_root()
    render_paths = load_contract_render_paths(repo_root)

    # apply_patch may name multiple files; classify every path (SECD-03).
    if tool_name == "apply_patch":
        tool_input = data.get("tool_input")
        if tool_input is None:
            tool_input = data.get("toolInput")
        if not isinstance(tool_input, dict):
            sys.stderr.write(
                _format_parse_refusal(tool_name, "tool_input is not an object")
            )
            return EXIT_BLOCK
        paths, patch_err = extract_apply_patch_paths(tool_input)
        if patch_err or not paths:
            sys.stderr.write(
                _format_parse_refusal(
                    tool_name, patch_err or "apply_patch paths are not classifiable"
                )
            )
            return EXIT_BLOCK
        for p in paths:
            refuse, rel_path, reason = evaluate_path(
                p,
                repo_root=repo_root,
                tool_name=tool_name or "apply_patch",
                old_string=None,
                new_string=None,
                render_paths=render_paths,
            )
            if refuse:
                sys.stderr.write(format_refusal(rel_path, reason=reason))
                return EXIT_BLOCK
        return EXIT_ALLOW

    if not file_path:
        return EXIT_ALLOW

    refuse, rel_path, reason = evaluate_path(
        file_path,
        repo_root=repo_root,
        tool_name=tool_name or "Write",
        old_string=old_string,
        new_string=new_string,
        render_paths=render_paths,
    )
    if not refuse:
        return EXIT_ALLOW

    sys.stderr.write(format_refusal(rel_path, reason=reason))
    return EXIT_BLOCK


def _run_scan_paths(targets: list[str]) -> int:
    repo_root = _git_repo_root() or "."
    render_paths = load_contract_render_paths(repo_root)
    failures: list[str] = []
    for raw in targets:
        # Shared classification with hook mode (SECD-03) — no separate
        # resolve-then-evaluate path that could diverge on symlinks.
        refuse, rel_path, reason = evaluate_path(
            raw,
            repo_root=repo_root,
            tool_name="Write",
            old_string=None,
            render_paths=render_paths,
        )
        if refuse:
            failures.append(format_refusal(rel_path, reason=reason))
    if failures:
        sys.stderr.write("\n".join(failures) + "\n")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scan-paths",
        nargs="+",
        metavar="PATH",
        help="Evaluate the given paths as full-file Write targets.",
    )
    args = parser.parse_args(argv)
    if args.scan_paths:
        return _run_scan_paths(args.scan_paths)
    return _run_claude_hook()


if __name__ == "__main__":
    raise SystemExit(main())
