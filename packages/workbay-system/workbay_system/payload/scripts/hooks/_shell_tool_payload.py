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

import ast
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


@dataclass(frozen=True)
class ShellWriteEffect:
    """A write and its execution directory before repository policy is applied.

    A missing target means an unresolved write, or an implicit tree-wide
    write when `formatter` is true.
    """

    target: str | None
    cwd: Path | None
    formatter: bool = False


_PATH_MUTATORS = frozenset({
    "chmod", "hardlink_to", "lchmod", "mkdir", "rename", "replace", "rmdir",
    "symlink_to", "touch", "unlink", "write_bytes", "write_text",
})
_PYTHON_MUTATORS = {
    "os.remove": ("path",), "os.unlink": ("path",), "os.rmdir": ("path",),
    "os.mkdir": ("path",), "os.makedirs": ("name",), "os.truncate": ("path",),
    "os.chmod": ("path",), "os.rename": ("src", "dst"), "os.replace": ("src", "dst"),
    "shutil.move": ("src", "dst"), "shutil.rmtree": ("path",),
    "shutil.copy": (None, "dst"), "shutil.copy2": (None, "dst"),
    "shutil.copyfile": (None, "dst"), "shutil.copytree": (None, "dst"),
}
_MAX_PYTHON_SOURCE = 64 * 1024
# Only these flags open a file for writing; r, b and t select read and encoding.
_OPEN_WRITE_FLAGS = "wax+"
_PATH_TYPES = frozenset({"Path", "pathlib.Path"})
_BUILTIN_OPENERS = frozenset({"open", "io.open", "builtins.open"})
# Module openers whose argument 0 is the path and argument 1 the mode (default read).
_STREAM_OPENERS = {
    "gzip.open": "filename", "bz2.open": "filename", "lzma.open": "filename",
    "codecs.open": "filename", "tarfile.open": "name",
}
# tarfile modes are "r", "r:xz", "w|gz" and so on: only the first letter selects writing.
_TAR_WRITE_MODES = "wax"
# os.open flags that open nothing for writing. O_RDONLY must be present.
_OS_READ_FLAGS = frozenset({"O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK", "O_NOCTTY", "O_BINARY"})
# Archive extraction (tarfile, zipfile) writes the archive's member names, which are
# unknown here and may leave the destination ("../x", absolute names). The receiver's
# type is not tracked, so any receiver counts (fail closed).
_EXTRACTORS = frozenset({"extract", "extractall"})
# Methods that return text when their receiver is text. Path.replace renames a
# file, while str.replace and bytes.replace only build a new value.
_TEXT_READERS = frozenset({"read_text", "read_bytes"})
_TEXT_METHODS = frozenset({
    "capitalize", "casefold", "center", "decode", "encode", "expandtabs", "format",
    "join", "ljust", "lower", "lstrip", "removeprefix", "removesuffix", "replace",
    "rjust", "rstrip", "strip", "swapcase", "title", "translate", "upper", "zfill",
})


def _python_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return f"{node.value.id}.{node.attr}"
    return ""


def _argument(call: ast.Call, index: int, name: str = "") -> ast.expr | None:
    if index < len(call.args):
        return call.args[index]
    return next((kw.value for kw in call.keywords if name and kw.arg == name), None)


def _hidden(call: ast.Call, index: int) -> bool:
    """True when a ``*args`` at or before ``index`` hides which value sits there."""
    return any(isinstance(arg, ast.Starred) for arg in call.args[:index + 1])


def _is_none(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


class _PythonWrites(ast.NodeVisitor):
    """Resolve constant path values in straight-line code without running it."""

    def __init__(self) -> None:
        self.bindings: dict[str, str] = {}
        # Names proven to hold str or bytes values, never a Path.
        self.texts: set[str] = set()
        # Names proven to hold a pathlib.Path value.
        self.paths: set[str] = set()
        self.targets: list[str | None] = []
        self.cwd_changes = False

    def _is_text(self, node: ast.expr | None) -> bool:
        """True only for an expression proven to be str or bytes in straight-line code."""
        if isinstance(node, ast.Constant):
            return isinstance(node.value, (str, bytes))
        if isinstance(node, ast.JoinedStr):
            return True
        if isinstance(node, ast.Name):
            return node.id in self.texts
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in _TEXT_READERS:
                    return True
                return node.func.attr in _TEXT_METHODS and self._is_text(node.func.value)
        return False

    def _is_path(self, node: ast.expr | None) -> bool:
        """True only for an expression proven to be a pathlib.Path in straight-line code."""
        if isinstance(node, ast.Call):
            return _python_name(node.func) in _PATH_TYPES
        if isinstance(node, ast.Name):
            return node.id in self.paths
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            return self._is_path(node.left) or self._is_path(node.right)
        return False

    def _value(self, node: ast.expr | None) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return self.bindings.get(node.id)
        if isinstance(node, ast.Call) and _python_name(node.func) in {"Path", "pathlib.Path"}:
            parts = [self._value(arg) for arg in node.args]
            if None not in parts and not node.keywords:
                return str(Path(*(part for part in parts if part is not None)))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            left, right = self._value(node.left), self._value(node.right)
            if left is not None and right is not None:
                return str(Path(left) / right)
        return None

    def scan(self, tree: ast.Module) -> None:
        for statement in tree.body:
            if isinstance(statement, (ast.Assign, ast.AnnAssign)):
                self.visit(statement)
                value = self._value(statement.value)
                targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                text = statement.value is not None and self._is_text(statement.value)
                path = statement.value is not None and self._is_path(statement.value)
                for target in targets:
                    if isinstance(target, ast.Name) and value is not None:
                        self.bindings[target.id] = value
                    else:
                        self._forget(target)
                    if isinstance(target, ast.Name) and text:
                        self.texts.add(target.id)
                    elif isinstance(target, ast.Name):
                        self.texts.discard(target.id)
                    if isinstance(target, ast.Name) and path:
                        self.paths.add(target.id)
                    elif isinstance(target, ast.Name):
                        self.paths.discard(target.id)
            elif isinstance(statement, (ast.Expr, ast.Import, ast.ImportFrom, ast.Delete, ast.AugAssign)):
                self.visit(statement)
                self._forget(statement)
            else:
                # Branches and nested scopes can rebind a receiver. Scan them
                # for effects, then discard facts we cannot prove at the join.
                self.bindings.clear()
                self.texts.clear()
                self.paths.clear()
                self.visit(statement)
                self.bindings.clear()
                self.texts.clear()
                self.paths.clear()

    def _forget(self, node: ast.AST) -> None:
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
                self.bindings.pop(child.id, None)
                self.texts.discard(child.id)
                self.paths.discard(child.id)
            if isinstance(child, ast.alias):
                self.bindings.pop(child.asname or child.name.partition(".")[0], None)
                self.texts.discard(child.asname or child.name.partition(".")[0])
                self.paths.discard(child.asname or child.name.partition(".")[0])

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self._forget(node.target)

    def visit_Call(self, call: ast.Call) -> None:
        self.generic_visit(call)
        name = _python_name(call.func)
        if name in {"os.chdir", "os.fchdir"}:
            self.cwd_changes = True
        if name in _PYTHON_MUTATORS:
            for index, keyword in enumerate(_PYTHON_MUTATORS[name]):
                if keyword is not None:
                    self.targets.append(self._value(_argument(call, index, keyword)))
            return

        receiver = call.func.value if isinstance(call.func, ast.Attribute) else None
        method = call.func.attr if isinstance(call.func, ast.Attribute) else ""
        if method == "replace" and self._is_text(receiver):
            return  # str.replace or bytes.replace builds a value; it touches no file.
        if method in _PATH_MUTATORS:
            self.targets.append(self._value(receiver))
            if method in {"rename", "replace"}:
                self.targets.append(self._value(_argument(call, 0, "target")))
        elif method in _EXTRACTORS:
            # A destination directory does not bound the files written, so the target is unprovable.
            self.targets.append(None)
        elif name in _BUILTIN_OPENERS or method == "open":
            self._open(call, name, receiver)

    def _open(self, call: ast.Call, name: str, receiver: ast.expr | None) -> None:
        """Record a write when the resolved opener's mode argument can write."""
        # Resolve the opener first: only it says which argument is the mode.
        tar = name == "tarfile.open"
        if name in _BUILTIN_OPENERS:
            target, index = _argument(call, 0, "file"), 1
        elif self._is_path(receiver):
            target, index = receiver, 0
        elif name in _STREAM_OPENERS:
            target, index = _argument(call, 0, _STREAM_OPENERS[name]), 1
        elif name == "os.open":
            # A *args at or before the flags slot hides the real flags.
            if _hidden(call, 1) or not self._os_read_flags(call):
                # dir_fd (or a **kwargs that may carry it) anchors the path elsewhere.
                anchored = any(
                    kw.arg is None or (kw.arg == "dir_fd" and not _is_none(kw.value))
                    for kw in call.keywords
                )
                self.targets.append(None if anchored else self._value(_argument(call, 0, "path")))
            return
        else:
            if call.args or call.keywords:
                self.targets.append(None)  # unknown opener: its mode is unprovable
            return
        hidden = _hidden(call, index)
        mode = None if hidden else _argument(call, index, "mode")
        if mode is None and not hidden and not any(kw.arg is None for kw in call.keywords):
            return  # default read mode
        value = self._value(mode)
        if value is None:
            self.targets.append(None)
        elif (value[:1] in _TAR_WRITE_MODES) if tar else any(flag in value for flag in _OPEN_WRITE_FLAGS):
            self.targets.append(self._value(target))

    def _os_read_flags(self, call: ast.Call) -> bool:
        """True only for os.O_RDONLY, optionally OR'd with flags that open nothing for writing."""
        flags = _argument(call, 1, "flags")
        if flags is None:
            return False
        names: set[str] = set()
        pending = [flags]
        while pending:
            node = pending.pop()
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
                pending.extend((node.left, node.right))
                continue
            name = _python_name(node)
            if not name.startswith("os.") or name[3:] not in _OS_READ_FLAGS:
                return False
            names.add(name[3:])
        return "O_RDONLY" in names


def python_write_targets(source: str | None, *, shell_expands: bool = False) -> list[str | None]:
    """Return recognized Python writes, using None for an unprovable target."""
    if source is None or len(source) > _MAX_PYTHON_SOURCE:
        return [None]
    if shell_expands and ("$" in source or "`" in source or "\\\n" in source):
        return [None]
    scanner = _PythonWrites()
    try:
        scanner.scan(ast.parse(source))
    except (SyntaxError, ValueError, RecursionError):
        return [None]
    return [
        target if target and "\x00" not in target
        and (not scanner.cwd_changes or Path(target).is_absolute()) else None
        for target in scanner.targets
    ]


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
