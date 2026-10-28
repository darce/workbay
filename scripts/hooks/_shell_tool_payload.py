#!/usr/bin/env python3
"""Shared shell-tool payload adapter for every Bash-matched hook (stdlib only).

One parser decides whether a hook payload is a shell call and, if so, which
command it runs and in which directory (SECD-03: one reference monitor). Guard
policy stays in each guard. A payload shape that only some guards accepted
before this adapter existed is a keyword-only switch that defaults to the
narrow behaviour, so moving a guard onto the adapter never drops a payload it
already checked.

Observed Codex hook payloads (ur-hk-07 probe, codex-cli 0.159.2) carry
``tool_name`` ``"Bash"``, ``tool_input.command`` and a top-level ``cwd``. The
``exec_command`` / ``functions.exec_command`` names, ``tool_input.cmd`` and
``tool_input.workdir`` come from the consumer report and are supported, but no
hook payload has shown them yet. The nested ``functions.exec`` shape is
unobserved, so it is not unwrapped and returns ``None`` (CARD-03, REF-12).

Hooks load this file by path rather than through ``sys.path``, because root and
payload twins share basenames. Python 3.9 syntax: the remote-gate guard runs
under the consumer's ``python3``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SHELL_TOOL_NAMES = (
    "Bash",
    "Shell",
    "run_in_terminal",
    "exec_command",
    "functions.exec_command",
)
EXEC_COMMAND_TOOL_NAMES = ("exec_command", "functions.exec_command")
TOOL_NAME_KEYS = ("tool_name", "toolName", "tool")
TOOL_INPUT_KEYS = ("tool_input", "toolInput", "input")
COMMAND_KEYS = ("command", "cmd")

_SHELL_NAMES = frozenset(name.lower() for name in SHELL_TOOL_NAMES)
_EXEC_NAMES = frozenset(name.lower() for name in EXEC_COMMAND_TOOL_NAMES)


@dataclass(frozen=True)
class ShellCall:
    """A shell call found in a hook payload.

    ``tool_name`` is the name as sent (``""`` for an untyped payload).
    ``workdir`` is ``None`` when the payload names no directory; the caller then
    keeps its process cwd. ``transport`` is ``"exec_command"`` for the exec
    names and ``"bash"`` otherwise.
    """

    tool_name: str
    command: str
    workdir: Path | None
    transport: str


def _tool_name(payload: dict, name_keys: tuple[str, ...]) -> str:
    for key in name_keys:
        value = payload.get(key)
        if value:
            return str(value)
    return ""


def shell_tool_name(payload: Any, *, name_keys: tuple[str, ...] = TOOL_NAME_KEYS) -> str | None:
    """The payload's tool name when it names a shell tool (any case), else ``None``."""
    if not isinstance(payload, dict):
        return None
    name = _tool_name(payload, name_keys)
    return name if name.lower() in _SHELL_NAMES else None


def _tool_input(payload: dict) -> dict | None:
    for key in TOOL_INPUT_KEYS:
        value = payload.get(key)
        if isinstance(value, dict) and value:
            return value
    args = payload.get("toolArgs")
    if isinstance(args, str):
        try:
            args = json.loads(args)
        except (ValueError, RecursionError):
            return None
    return args if isinstance(args, dict) and args else None


def _command(sources: list[dict], *, allow_empty: bool, coerce: bool) -> str | None:
    present: list[Any] = []
    for source in sources:
        for key in COMMAND_KEYS:
            if key not in source:
                continue
            value = source[key]
            if isinstance(value, str) and value:
                return value
            present.append(value)
    if coerce and present:
        return str(present[0] or "")
    if allow_empty and any(isinstance(value, str) for value in present):
        return ""
    return None


def _directory(value: Any) -> Path | None:
    # A NUL byte would make every later syscall raise ValueError; treat it as absent.
    if isinstance(value, str) and value and "\x00" not in value:
        return Path(value)
    return None


def _workdir(payload: dict, tool_input: dict | None) -> Path | None:
    base = _directory(payload.get("cwd"))
    path = _directory(tool_input.get("workdir")) if tool_input else None
    if path is not None:
        # A relative workdir without a cwd stays relative: it resolves against
        # the process cwd, as the caller's own default would.
        return path if path.is_absolute() or base is None else base / path
    return base


def extract_shell_call(
    payload: Any,
    *,
    name_keys: tuple[str, ...] = TOOL_NAME_KEYS,
    allow_untyped: bool = False,
    top_level_command: bool = False,
    allow_empty_command: bool = False,
    coerce_command: bool = False,
) -> ShellCall | None:
    """Return the payload's shell call, or ``None`` when it is not one.

    Name: the first non-empty value among ``name_keys``, matched against
    ``SHELL_TOOL_NAMES`` case-insensitively. Tool input: the first non-empty
    dict among ``tool_input`` / ``toolInput`` / ``input``, else the
    ``toolArgs`` dict or JSON string. Command: the first non-empty string among
    ``command`` / ``cmd``. Working directory: ``tool_input.workdir`` (relative
    to ``cwd`` when relative), else top-level ``cwd``, else ``None``.

    Per-guard switches, each reproducing a shape some guard accepted before:

    - ``name_keys``: the keys holding the tool name, first non-empty wins.
      ``()`` ignores tool names, so every payload counts as untyped.
    - ``allow_untyped``: a payload with no tool name is a shell call when it
      carries a command.
    - ``top_level_command``: also read top-level ``command`` / ``cmd`` when
      the tool input carries none.
    - ``allow_empty_command``: an empty-string command yields ``""`` rather
      than ``None``.
    - ``coerce_command``: a command key holding a non-string or empty value
      yields ``str(value or "")``, and a shell-named payload with no command
      key yields ``""``.

    Never raises on a malformed payload: anything it cannot parse is ``None``,
    and the caller keeps its non-shell path (SECD-05).
    """
    if not isinstance(payload, dict):
        return None
    name = _tool_name(payload, name_keys)
    key = name.lower()
    tool_input = _tool_input(payload)
    sources = [tool_input] if tool_input else []
    if top_level_command:
        sources.append(payload)
    command = _command(sources, allow_empty=allow_empty_command, coerce=coerce_command)
    if key in _SHELL_NAMES:
        if command is None and coerce_command:
            command = ""
    elif name or not allow_untyped:
        return None
    if command is None:
        return None
    transport = "exec_command" if key in _EXEC_NAMES else "bash"
    return ShellCall(name, command, _workdir(payload, tool_input), transport)
