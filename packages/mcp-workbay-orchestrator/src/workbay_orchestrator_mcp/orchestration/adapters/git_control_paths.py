"""Git-control-path classifier for remote-exec patch quarantine.

Extracted from ``remote_exec.py`` so fold / hunk-budget / header parsing have a
dedicated seam (the adapter stays the caller of ``patch_touches_git_control_paths``).
"""

from __future__ import annotations

import re

#: Quoted or unquoted path token in a hunk / rename header (git C-style quotes).
#: Unquoted tokens stop at whitespace so a path cannot swallow a timestamp.
_DIFF_PATH_TOKEN = r'(?:"(?:\\.|[^"\\])*"|[^\s]+)'
#: ``diff --git`` with optional C-style quoting around each side. Surrounding
#: / trailing whitespace is tolerated; an unquoted path still cannot contain
#: an unescaped space (``_DIFF_PATH_TOKEN``).
_DIFF_GIT_HEADER_RE = re.compile(
    rf"^[ \t]*diff --git ({_DIFF_PATH_TOKEN}) ({_DIFF_PATH_TOKEN})[ \t]*$",
    re.MULTILINE,
)
#: ``---`` / ``+++`` unified headers, including custom / missing ``b/`` prefixes.
#: Trailing metadata may be space- or tab-delimited (traditional timestamps);
#: one-or-more spaces after ``+++``/``---`` covers doubled whitespace.
_DIFF_UNIFIED_HEADER_RE = re.compile(
    rf"^[ \t]*(?:\+\+\+|---)[ \t]+({_DIFF_PATH_TOKEN})(?:[ \t].*)?$",
    re.MULTILINE,
)
#: Rename/copy detection headers (git emits these without a ``+++`` line).
_DIFF_RENAME_COPY_RE = re.compile(
    r"^(?:rename|copy) (?:from|to) (.+)$",
    re.MULTILINE,
)
#: Unified hunk header. Counts default to 1 when omitted. A line that starts
#: with ``@@`` but does not match is fail-closed (treat as control-touching).
_HUNK_HEADER_RE = re.compile(r"^@@\s+-(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@")

#: Paths a remote lane can poison now that ``.git`` is writable. A patch that
#: touches any of these is quarantined on the host before ``git apply``.
#:
#: Host ``core.hooksPath`` is ``scripts/hooks/git``, but those entrypoints
#: resolve GUARD_DIR to the parent and exec sibling helpers
#: (``scripts/hooks/check_branch_naming.py`` and friends). The tracked payload
#: twin under ``packages/workbay-system/.../payload/scripts/hooks/`` is the
#: dogfood-link symlink target — rewriting it is the same hook-exec surface.
_GIT_CONTROL_EXACT = frozenset(
    {
        ".gitattributes",
        ".gitmodules",
        ".git",
        "scripts/hooks",
        "scripts/hooks/git",
        "packages/workbay-system/workbay_system/payload/scripts/hooks",
    }
)
_GIT_CONTROL_PREFIXES = (
    ".git/",
    "scripts/hooks/",
    "packages/workbay-system/workbay_system/payload/scripts/hooks/",
)
_GIT_CONTROL_BASENAMES = frozenset({".gitattributes", ".gitmodules"})
_GIT_CONTROL_EXACT_FOLD = frozenset(p.casefold() for p in _GIT_CONTROL_EXACT)
_GIT_CONTROL_PREFIXES_FOLD = tuple(p.casefold() for p in _GIT_CONTROL_PREFIXES)
_GIT_CONTROL_BASENAMES_FOLD = frozenset(p.casefold() for p in _GIT_CONTROL_BASENAMES)

#: Git C-style simple escapes (``quote.c`` / ``unquote_c_style``).
_C_STYLE_SIMPLE_ESCAPES = {
    "a": "\a",
    "b": "\b",
    "t": "\t",
    "n": "\n",
    "v": "\v",
    "f": "\f",
    "r": "\r",
    '"': '"',
    "\\": "\\",
}


def _dequote_c_style(token: str) -> str:
    """Undo git's C-style path quoting (``quote.c`` / ``unquote_c_style``)."""
    if len(token) < 2 or token[0] != '"' or token[-1] != '"':
        return token
    inner = token[1:-1]
    out: list[str] = []
    i = 0
    n = len(inner)
    while i < n:
        ch = inner[i]
        if ch != "\\" or i + 1 >= n:
            out.append(ch)
            i += 1
            continue
        nxt = inner[i + 1]
        simple = _C_STYLE_SIMPLE_ESCAPES.get(nxt)
        if simple is not None:
            out.append(simple)
            i += 2
            continue
        if nxt in "01234567":
            j = i + 1
            while j < n and j < i + 4 and inner[j] in "01234567":
                j += 1
            out.append(chr(int(inner[i + 1 : j], 8) & 0xFF))
            i = j
            continue
        out.append(nxt)
        i += 2
    return "".join(out)


def _normalize_diff_path(path: str) -> str:
    """Dequote C-style headers and normalize slashes on a hunk-header path."""
    cleaned = _dequote_c_style(path.strip()).replace("\\", "/")
    if cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned


def _strip_p1_prefix(path: str) -> str:
    """Strip one leading path component, matching host ``git apply -p1``."""
    slash = path.find("/")
    if slash <= 0:
        return path
    return path[slash + 1 :]


def _collapse_dot_slash(path: str) -> str:
    """Fold ``./`` segments and collapse duplicate slashes.

    ``+++ b/./.git/config`` must become ``.git/config`` after p1-strip so the
    ``.git/`` prefix match (and the ``.git`` path-segment reject) can fire.
    ``..`` segments are dropped with their parent so ``foo/../.git/config``
    cannot hide a control path. Fail-closed: an empty result stays empty.
    """
    parts: list[str] = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    collapsed = "/".join(parts)
    if path.startswith("/"):
        return "/" + collapsed if collapsed else "/"
    return collapsed


def _path_has_dot_git_segment(path: str) -> bool:
    """True when any path component is exactly ``.git`` (case-folded)."""
    return any(seg == ".git" for seg in path.split("/"))


def _parse_hunk_header(line: str) -> tuple[int, int] | None:
    """Return ``(oldcount, newcount)`` or ``None`` if *line* is not a hunk header.

    Omitted counts default to 1, matching unified-diff / ``git apply``.
    """
    match = _HUNK_HEADER_RE.match(line)
    if match is None:
        return None
    old_count = int(match.group(2)) if match.group(2) is not None else 1
    new_count = int(match.group(4)) if match.group(4) is not None else 1
    return old_count, new_count


def _folded_is_git_control_path(folded: str) -> bool:
    """Case-insensitive match against the known control-path tokens."""
    if folded in _GIT_CONTROL_EXACT_FOLD:
        return True
    if folded.startswith(_GIT_CONTROL_PREFIXES_FOLD):
        return True
    basename = folded.rsplit("/", 1)[-1]
    return basename in _GIT_CONTROL_BASENAMES_FOLD


def _is_git_control_path(path: str) -> bool:
    """Return True when *path* is a git control path the host must not apply.

    Tests both the normalized path and the ``-p1``-stripped form so a custom
    first component (or a literal ``a/`` / ``b/``) cannot hide a control path.
    Control-token comparison is case-insensitive (macOS default APFS).

    Git apply treats header paths as C strings and truncates at the first NUL.
    Any token that contains a NUL (raw, or after C-style dequote of ``\\000``)
    is fail-closed as a control path so Python matches that C-string view
    instead of classifying the suffix-padded token as benign.
    """
    if "\x00" in path:
        return True
    cleaned = _normalize_diff_path(path.split("\x00", 1)[0])
    if "\x00" in cleaned:
        return True
    cleaned = cleaned.split("\x00", 1)[0]
    if not cleaned or cleaned == "/dev/null":
        return False
    candidates = [cleaned]
    stripped = _strip_p1_prefix(cleaned)
    if stripped and stripped != cleaned and stripped != "/dev/null":
        candidates.append(stripped)
    for cand in candidates:
        folded = _collapse_dot_slash(cand).casefold()
        if not folded or folded == "/dev/null":
            continue
        if _folded_is_git_control_path(folded):
            return True
        if _path_has_dot_git_segment(folded):
            return True
    return False


def _normalize_patch_text_for_headers(patch_text: str) -> str:
    """Normalize line endings so header anchors see LF-only lines.

    A trailing CR between the path token and LF (CRLF patches, or a lone CR)
    would otherwise sit under ``$`` and fail-open the control-path classifier.
    All header regexes consume this normalized view so the same CR cannot
    dodge ``diff --git``, unified, or rename/copy matchers independently.
    """
    return patch_text.replace("\r\n", "\n").replace("\r", "\n")


def _is_hunk_body_line(line: str) -> bool:
    """True when *line* is a unified-diff hunk body prefix (``+``/``-``/`` ``/``\\``)."""
    return bool(line) and line[0] in "+- \\"


def _apply_hunk_body_counts(line: str, old_remaining: int, new_remaining: int) -> tuple[int, int]:
    """Consume one unified-diff body line against leftover old/new counts.

    Context (leading space) counts toward both sides. A delete counts toward
    old only; an add toward new only. ``\\ No newline`` does not count. This
    is *not* ``max(old, new)`` and not ``old+new``: a mixed add+delete hunk
    has more body lines than ``max`` and fewer than ``sum``.
    """
    prefix = line[0]
    if prefix == "\\":
        return old_remaining, new_remaining
    if prefix == " ":
        if old_remaining > 0:
            old_remaining -= 1
        if new_remaining > 0:
            new_remaining -= 1
        return old_remaining, new_remaining
    if prefix == "-":
        if old_remaining > 0:
            old_remaining -= 1
        return old_remaining, new_remaining
    if prefix == "+":
        if new_remaining > 0:
            new_remaining -= 1
    return old_remaining, new_remaining


def patch_touches_git_control_paths(patch_text: str) -> bool:
    """Return True when any file-header path targets a git control path.

    Control paths: anything under ``scripts/hooks/`` (git entrypoints plus
    sibling helpers the entrypoints exec), the payload twin under
    ``packages/workbay-system/workbay_system/payload/scripts/hooks/``,
    anything under ``.git/``, ``.gitattributes`` (textconv driver
    injection), and ``.gitmodules``.
    Headers inspected: ``diff --git`` (quoted or unquoted), ``---`` / ``+++``
    (any ``-p1`` prefix, including a missing prefix), and ``rename`` / ``copy``
    ``from`` / ``to`` lines.

    Parsing is structural: a well-formed ``@@ -<old>[,<oldcount>]
    +<new>[,<newcount>] @@`` header consumes body lines until both the old
    and new counts are exhausted (leading ``+`` counts toward new, ``-``
    toward old, space toward both; omitted counts default to 1). On the
    line *after* that budget is exhausted, ``in_hunk`` is cleared so a
    following traditional ``---`` / ``+++`` / ``diff --git`` / rename / copy
    header is re-classified. A line that starts with ``@@`` but does not
    parse as a hunk header is fail-closed (treated as control-touching). A
    hunk body that merely *renders* like ``--- .gitattributes`` or
    ``+++ b/.gitattributes`` is not a file header.
    """
    if not patch_text:
        return False
    patch_text = _normalize_patch_text_for_headers(patch_text)
    in_hunk = False
    old_remaining = 0
    new_remaining = 0
    for line in patch_text.split("\n"):
        if in_hunk:
            if _is_hunk_body_line(line):
                old_remaining, new_remaining = _apply_hunk_body_counts(line, old_remaining, new_remaining)
                if old_remaining <= 0 and new_remaining <= 0:
                    in_hunk = False
                continue
            # Budget exhausted or a non-body line: leave the hunk and
            # re-classify this line (traditional second-file headers live here).
            in_hunk = False
            old_remaining = 0
            new_remaining = 0
        git_match = _DIFF_GIT_HEADER_RE.match(line)
        if git_match:
            if _is_git_control_path(git_match.group(1)) or _is_git_control_path(git_match.group(2)):
                return True
            continue
        if line.startswith("@@"):
            parsed = _parse_hunk_header(line)
            if parsed is None:
                return True
            old_remaining, new_remaining = parsed
            in_hunk = old_remaining > 0 or new_remaining > 0
            continue
        uni_match = _DIFF_UNIFIED_HEADER_RE.match(line)
        if uni_match and _is_git_control_path(uni_match.group(1)):
            return True
        rename_match = _DIFF_RENAME_COPY_RE.match(line)
        if rename_match and _is_git_control_path(rename_match.group(1)):
            return True
    return False
