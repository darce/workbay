"""Resolve the runtime ``uv tool`` interpreter for a managed MCP server.

Git-only installs place each MCP server as a ``uv tool install`` console script
(NOT a workspace ``.venv``). The verify / doctor / reinject seams must probe
*this* interpreter — the one ``mcp_launch.py`` execs at runtime — never the
front-door ``workbay`` tool venv (which carries different extras) nor a
``<target>/.venv`` that git-only installs never create.

The uv-tool bin-dir resolution mirrors the shim
(``packages/workbay-system/.../payload/scripts/hooks/mcp_launch.py``:
``_uv_tool_bin_dirs`` / ``_tool_console_path``). The shim is a payload script,
not an importable module, so the small, stable logic is duplicated here rather
than imported; keep the two in sync by contract. Stdlib-only on purpose.

POSIX (macOS/Linux) is the supported surface for the runtime-interpreter probe:
uv-tool console scripts are shebang-pinned to their own venv python, so the
interpreter is read from the console's ``#!`` line. On Windows the console is a
compiled ``.exe`` launcher with no readable shebang; ``resolve_tool_python``
degrades to ``""`` there (callers already treat ``""`` as "not resolvable").
"""

from __future__ import annotations

import os
import shutil


def _uv_tool_bin_dirs() -> list[str]:
    """uv's default ``uv tool install`` console bin-dir resolution order.

    ``UV_TOOL_BIN_DIR`` → ``$XDG_BIN_HOME`` → ``$XDG_DATA_HOME/../bin`` →
    ``~/.local/bin``, de-duplicated preserving first-seen order.
    """
    candidates: list[str] = []
    for env in ("UV_TOOL_BIN_DIR", "XDG_BIN_HOME"):
        value = os.environ.get(env)
        if value:
            candidates.append(value)
    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        candidates.append(os.path.join(os.path.dirname(xdg_data_home), "bin"))
    candidates.append(os.path.join(os.path.expanduser("~"), ".local", "bin"))
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def resolve_tool_console(console: str) -> str:
    """Path to the ``uv tool`` console script for ``console``, or ``""``.

    Probes ``PATH`` (``shutil.which``) first, then uv's default tool bin-dir
    order under POSIX (``<dir>/<console>`` or ``<dir>/bin/<console>``) and
    Windows (``<dir>/Scripts/<console>.exe``) layouts.
    """
    which = shutil.which(console)
    if which and os.path.exists(which):
        return which
    for bin_dir in _uv_tool_bin_dirs():
        for candidate in (
            os.path.join(bin_dir, console),
            os.path.join(bin_dir, "bin", console),
            os.path.join(bin_dir, "Scripts", f"{console}.exe"),
        ):
            if os.path.exists(candidate):
                return candidate
    return ""


def resolve_tool_python(console: str) -> str:
    """The venv python behind the ``uv tool`` ``console`` script, or ``""``.

    Reads the console script's shebang (``#!<venv>/bin/python``) — uv-tool
    console scripts are shebang-pinned to their own venv interpreter, so this is
    the interpreter the runtime server actually runs under. Returns ``""`` when
    the console is unresolved, unreadable, has no shebang (e.g. a Windows
    ``.exe`` launcher), or the shebang interpreter path does not exist.
    """
    script = resolve_tool_console(console)
    if not script:
        return ""
    try:
        with open(script, "rb") as handle:
            first_line = handle.readline()
    except OSError:
        return ""
    if not first_line.startswith(b"#!"):
        return ""
    # A shebang may carry an interpreter arg (``#!/usr/bin/env python``); the
    # interpreter is the first whitespace-delimited token after ``#!``.
    interpreter = first_line[2:].strip().split(None, 1)[0].decode("utf-8", "replace")
    if interpreter and os.path.exists(interpreter):
        return interpreter
    return ""
