#!/usr/bin/env python3
"""Refuse host Python test execution when the repository requires remote gates.

Under execution_mode=remote_only, this guard stops habitual and accidental
workstation runs of the enumerated runners from command text. Test runs go
through make check-remote or /wb-offload. Deliberate evasion (built names,
renamed copies, non-shell readers of produced files, or _pytest imports) is
out of scope. The audited env override is the only sanctioned exception for
a workstation run. Dispatcher-proven remote lane sandboxes may run tests.

This PreToolUse(Bash/Shell/run_in_terminal) hook is deliberately stdlib-only: harness hooks
run under the harness interpreter, which may not have the WorkBay packages
installed. The manifest lookup mirrors the literal ``remote_only`` policy
reader in ``guard-agent-remote-only-implementation.py``.

It enforces pytest, py.test, ``run_named_tests.py`` and deny-by-default make
routing. Make needs a parsed argv, implicit unproduced makefile and explicit
goals with inert recipe-closure evidence. Parsed dry runs retain their
allowance when no alternate source, fed input or test-runner text is present.
JS runners, unittest, tox, nox, hatch and scripts invoked by path are not
inspected, so a host run of those is not refused.

A shell (``_SHELLS``, ``source`` or ``.``) whose script comes from stdin, a file
operand or the ``-c`` text of a file an earlier segment wrote is scanned as
running that text; runtime-built names and non-shell readers are out of scope.
The remote command of an ssh that may run on this host (a loopback destination,
or one not provably remote) is scanned like ``-c`` text. The hook tracks no
``cd``, so a script path whose basename an earlier segment produced under another
path is treated as that produced file. Launchers that hand a string to a shell
(``su -c``, ``flock -c``, ``watch``) are not modeled.

The hook classifies shell command text only. It never executes a command or
contacts the remote gate. Every command segment is ``test`` (a proven host
test run), ``inert`` (a command proven not to run a test) or ``unknown``
(anything else). ``unknown`` segments are refused when their text names a
test runner, so a shape the parser does not recognize is routed to the
remote gate instead of running on the host. A recorded bypass is the only
way to run a refused command in ``remote_only`` mode.
"""

from __future__ import annotations

import datetime
import fnmatch
import ipaddress
import json
import os
import re
import shlex
import socket
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable
from urllib.parse import urlsplit

_BYPASS_ENV = "WORKBAY_ALLOW_LOCAL_GATE_UNDER_REMOTE_ONLY"
_AUDIT_FILE = "remote_gate_routing_bypass.jsonl"
_MANIFEST_NAME_PRECEDENCE = (".workbay-bootstrap.json", ".workbay-overlay.json")
_MAX_SHELL_DEPTH = 3
_MAX_SUBSTITUTION_NESTING = 16
_MAX_COMMAND_BYTES = 64 * 1024
_UNPARSED_SHAPE = "unparsed_shape"

_TEST_TARGETS = frozenset(
    {
        "check-all",
        "check-bootstrap",
        "check-bridge",
        "check-canvas",
        "check-git-overlay-install",
        "check-handoff",
        "check-hook-twin-parity",
        "check-hooks",
        "check-legacy-overlay-guard",
        "check-orchestrator",
        "check-overlay-gates",
        "check-protocol",
        "check-review-pipeline",
        "check-system",
        "check-system-integration",
        "check-workbay",
        "crap-coverage",
        "test",
        "test-bridge",
        "test-canvas",
        "test-contract",
        "test-handoff",
        "test-handoff-acceptance",
        "test-handoff-embeddings-combo",
        "test-orchestrator",
        "test-preflight",
        "test-rehearsal",
        "test-tier-fast",
        "test-tier-e2e",
        "lane-check",
        "test-files",
    }
)

# Evidence: root help prints an awk listing; package help recipes only echo
# their command lists. Root check-remote invokes remote_gate.sh, with no local
# test prerequisite. Decision 34038 also admits the five documented lifecycle
# facades below: each has tracked recipe-closure evidence and an allowance
# control. The recipe-closure guard tests every tracked definition.
# A missing goal is an availability cost; adding an unproven goal is a bypass.
_INERT_MAKE_GOALS = frozenset({
    "help", "check-remote", "lane-land", "finalize-plan", "handoff-write",
    "offload-flock", "lane-reap",
})

# Drift-scan exemptions: operator flows with their own preflight, and a help
# listing whose echoed text names pytest but whose recipe runs nothing.
_EXEMPT_TARGETS = MappingProxyType(
    {
        "release-monorepo": "Operator release flow runs its own preflight.",
        "overlay-install-venv": "Operator install flow provisions the overlay environment.",
        "help": "Prints the target list; its echoed text names pytest, but no recipe line runs it.",
    }
)

# Commands that never execute their arguments. Conditional members (rg, git,
# gh, find, sort, command) are checked by _is_inert for their executing options.
_INERT_COMMANDS = frozenset(
    {
        "[",
        "cat",
        "cd",
        "cut",
        "echo",
        "false",
        "grep",
        "head",
        "jq",
        "ls",
        "printf",
        "pwd",
        "tail",
        "tee",
        "test",
        "tr",
        "true",
        "type",
        "uniq",
        "wc",
        "which",
    }
)
# git subcommands with no option that runs a program named on the command line.
_GIT_INERT_SUBCOMMANDS = frozenset(
    {
        "add",
        "blame",
        "branch",
        "checkout",
        "commit",
        "diff",
        "grep",
        "log",
        "ls-files",
        "mv",
        "restore",
        "rev-parse",
        "rm",
        "show",
        "stash",
        "status",
        "switch",
        "tag",
    }
)
_FIND_EXEC_ACTIONS = frozenset({"-exec", "-execdir", "-ok", "-okdir"})
_WRAPPERS = frozenset(
    {"caffeinate", "command", "doas", "exec", "nice", "nohup", "stdbuf", "sudo", "time", "timeout", "xargs"}
)
_SHELLS = frozenset({"bash", "sh", "zsh", "dash", "ksh"})
# Shell options that consume the next word, so it is not a script operand.
_SHELL_VALUE_OPTIONS = frozenset({"-o", "+o", "-O", "+O", "--rcfile", "--init-file"})
# Startup files a shell reads before its script: these options and variables name or locate them.
_STARTUP_FILE_OPTIONS = frozenset({"--rcfile", "--init-file"})
# The name alone counts: printf -v, read, declare or a name-only export sets it without NAME=.
_STARTUP_ENV = re.compile(r"(?<![\w${])(?:BASH_ENV|ENV|ZDOTDIR)(?!\w)")
# Wrappers that pass the word after them to the program they launch.
_LAUNCHERS = _WRAPPERS | {"env"}
# Make calls the make path models, so their expansion is not refused as unparsed.
_MAKE_CALL_WORDS = frozenset({"$(MAKE)", "${MAKE}"})
# Program names a glob in a command word may expand into.
_RUNNER_NAMES = ("make", "gmake", "pytest", "py.test", "run_named_tests.py")
# Builtins that run text built from expansions: eval, trap and source run it, alias defines it.
_TEXT_EXECUTORS = frozenset({"alias", "builtin", "eval", "source", "trap", "."})
# Syntax that can lead a simple command before its command word: reserved words and grouping.
_RESERVED_WORDS = frozenset({"(", "{", "!", "if", "then", "elif", "else", "do", "while", "until", "coproc"})
# A function header is NAME() or NAME(){, or `function NAME` with an optional () or (){ token after it.
_FUNCTION_HEADER = re.compile(r"^[A-Za-z_][\w.-]*\(\)\{?$")
_FUNCTION_NAME = re.compile(r"^[A-Za-z_][\w.-]*(?:\(\)\{?)?$")
_FUNCTION_PARENS = frozenset({"()", "(){"})
# A case pattern such as x) or (x). A bare pattern that opens a later arm is followed by a word, not a redirect.
_CASE_PATTERN = re.compile(r"^\(?[^()$`]+\)$")
_REDIRECT_START = re.compile(r"^\d*[<>&]")
# A redirect leading a segment: its target is the rest of the word, or the next word when it is detached.
_LEADING_REDIRECT = re.compile(r"^(\d*(?:>>|>\||>&|&>>|&>|>|<&|<>|<))([^<>()|&;]*)$")
# An output redirect and its target. The shell opens the target before any command in the segment runs.
_OUTPUT_REDIRECT = re.compile(r"""(?<![<>&\w])\d*(?:<>|>>|>\||&>>|&>|>)\s*([^\s;&|<>()'"`$]+)""")
# The words of text an earlier segment printed or wrote, split where the shell splits words.
_PRIOR_WORD_SPLIT = re.compile(r"""[\s;&|<>()'"`=]+""")
# An input redirect word, < s or 0<s; not a heredoc, here-string, <> or process substitution.
_INPUT_REDIRECT = re.compile(r"^\d*<(?![<>&(])(.*)$")

_IMPLICIT_MAKEFILES = frozenset({"GNUmakefile", "makefile", "Makefile"})
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$", re.ASCII)
_ENV_ASSIGNMENT = re.compile(rf"^{re.escape(_BYPASS_ENV)}=(.*)$", re.ASCII)
_RUNNER_WORD = re.compile(
    r"(?<![\w.-])(?:pytest|py\.test|run_named_tests\.py)(?![\w.-])"
    # A runner attached to a short option: python -mpytest, git grep -Opytest.
    r"|(?<![\w-])-[A-Za-z]+(?:pytest|py\.test)(?![\w.-])"
)
# Quotes and backslashes the shell removes from a word before it runs, so ma""ke
# names make. The goals after it are unquoted in _raw_test_word.
_SHELL_GAP = r"[\"'\\]*"
_MAKE_WORD = re.compile(
    r"(?<![\w.-])(?:(?:g" + _SHELL_GAP + r")?" + _SHELL_GAP.join("make") + r"|\$\(MAKE\)|\$\{MAKE\})(?![\w.-])"
)
_SHELL_WORD = re.compile(r"""(?:[^\s"'()<>`]|"[^"]*"|'[^']*')+""")
_CASE_WORD = re.compile(r"(?<![\w-])case(?![\w-])")
# Stands in for a command or process substitution inside segment text, so
# shlex never parses substitution source; the substitution is scanned itself.
_SUBSTITUTION = "\x00"
_MAKE_VALUE_OPTIONS = frozenset(
    {
        "-C",
        "-f",
        "-I",
        "-o",
        "-W",
        "-E",
        "--directory",
        "--file",
        "--makefile",
        "--include-dir",
        "--old-file",
        "--what-if",
        "--assume-new",
        "--eval",
    }
)
_MAKE_SHORT_FLAGS = frozenset("bBdehikmnprRqsStvw")
_MAKE_LONG_FLAGS = frozenset({
    "--always-make", "--environment-overrides", "--help", "--ignore-errors",
    "--keep-going", "--no-keep-going", "--stop", "--just-print", "--dry-run",
    "--recon", "--print-data-base", "--question", "--no-builtin-rules",
    "--no-builtin-variables", "--silent", "--quiet", "--print-directory",
    "--no-print-directory", "--touch", "--version", "--warn-undefined-variables", "--trace",
})
_MAKE_NUMERIC_OPTIONS = frozenset({"--jobs", "--load-average", "--max-load"})
# GNU make's optional numeric values: -j/--jobs take digits; -l/--load-average/--max-load take a decimal.
_MAKE_JOBS_VALUE = re.compile(r"[0-9]+")
_MAKE_LOAD_VALUE = re.compile(r"[0-9]+(?:\.[0-9]+)?")
_MAKE_SOURCE_ENV = re.compile(r"(?<![\w${])(?:MAKEFILES|MAKEFLAGS|GNUMAKEFLAGS|MFLAGS)(?!\w)")
_ENV_VALUE_OPTIONS = frozenset({"-u", "--unset", "-C", "--chdir", "-S", "--split-string"})
_UV_VALUE_OPTIONS = frozenset(
    {
        "--allow-insecure-host",
        "--build-constraints",
        "--color",
        "--config-setting",
        "--config-settings-package",
        "--project",
        "-p",
        "--directory",
        "-C",
        "-w",
        "--python",
        "--python-preference",
        "--python-platform",
        "--python-version",
        "--default-index",
        "--extra-index-url",
        "--find-links",
        "--index",
        "--index-strategy",
        "--index-url",
        "--keyring-provider",
        "--link-mode",
        "--with",
        "--with-editable",
        "--with-requirements",
        "--env-file",
        "--config-file",
        "--cache-dir",
        "--script-shell",
        "--package",
        "--extra",
        "--no-extra",
        "--group",
        "--no-group",
        "--only-group",
        "--no-install-package",
        "--no-build-isolation-package",
        "--prerelease",
        "--reinstall-package",
        "--resolution",
        "--exclude-newer",
        "--exclude-newer-package",
        "--fork-strategy",
        "--no-binary-package",
        "--no-build-package",
        "--no-editable-package",
        "--no-sources-package",
        "--refresh-package",
        "--upgrade-group",
        "--upgrade-package",
        "-P",
        "-f",
        "-i",
    }
)


def _repo_root() -> Path:
    return Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())


def _execution_policy(root: Path) -> tuple[str, str | None]:
    """An absent ledger opts out; a faulty present ledger never opts out."""
    for name in _MANIFEST_NAME_PRECEDENCE:
        candidate = root / name
        try:
            raw = candidate.read_bytes()
        except FileNotFoundError as exc:
            if not candidate.is_symlink():
                continue
            return "ledger_unreadable", f"{candidate}: {exc}"
        except OSError as exc:
            return "ledger_unreadable", f"{candidate}: {exc}"
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, RecursionError) as exc:
            return "ledger_unreadable", f"{candidate}: {exc}"
        if not isinstance(data, dict):
            return "ledger_unreadable", f"{candidate}: expected a JSON object"
        if data.get("execution_mode") == "remote_only":
            return "remote_only", None
        return "local_ok", None
    return "local_ok", None


def _execution_mode(root: Path) -> str:
    return _execution_policy(root)[0]


def _is_remote_gate_context(root: Path) -> bool:
    """Bind a lane marker to the dispatcher's provenance and shallow base.

    These are plain file reads, not a remote-host switch. The dispatching
    workstation's remote-gate.env rules out a sandbox allowance. The gate
    clone runs make directly with invoking hooks disabled, so its marker is
    not evidence of a harness running remotely.
    """
    root = root.resolve()
    if (root / ".workbay/remote-gate.env").exists():
        return False
    for parent in (root, *root.parents):
        try:
            if (parent / ".workbay-lane-sandbox").read_text(encoding="utf-8") != f"lane_key={parent.name}\n":
                continue
            provenance = (parent / ".workbay-source-provenance").read_text(encoding="utf-8")
            bases = [line.partition("=")[2] for line in provenance.splitlines() if line.startswith("sandbox_base_commit=")]
            if len(bases) != 1 or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", bases[0]) is None:
                continue
            if (parent / ".git/shallow").read_text(encoding="utf-8") == bases[0] + "\n":
                return True
        except (OSError, ValueError):
            continue
    return False


class _Segment:
    """One simple command: shlex text, verbatim source, heredocs and substitutions."""

    __slots__ = ("bodies", "from_prior", "make_stdin_unprovable", "opaque", "raw", "substitutions", "text", "upstream")

    def __init__(self) -> None:
        self.text: list[str] = []
        self.raw: list[str] = []
        self.bodies: list[str] = []
        self.substitutions: list[list[_Segment]] = []
        # Earlier stages of the same pipeline: their output is this stdin.
        self.upstream: list[_Segment] = []
        # Set for expansions the lexer does not split into commands:
        # arithmetic, expanding heredoc bodies, and substitutions holding case.
        self.opaque = False
        self.make_stdin_unprovable = False
        # Set on the stand-in for a stdin that is the earlier text itself; see _PRIOR_STDIN.
        self.from_prior = False

    def add(self, text: str, raw: str | None = None) -> None:
        self.text.append(text)
        self.raw.append(text if raw is None else raw)


_Heredoc = tuple[_Segment, str, bool, bool]
# Stands in for the earlier text where a shell reads a produced file on stdin. The scan merges the earlier
# text itself, so copying it into every reader's text would make a run of readers quadratic.
_PRIOR_STDIN = "\x01prior-stdin\x01"


def _arithmetic_end(command: str, index: int) -> int:
    """Return the index after the ``))`` closing an arithmetic form."""
    depth = 2
    while index < len(command):
        if command[index] == "(":
            depth += 1
        elif command[index] == ")":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    raise ValueError("unterminated arithmetic expansion")


def _lex_heredoc_operator(command: str, index: int, segment: _Segment, pending: list[_Heredoc]) -> int:
    """Record a ``<<`` here-document; its body is read after the next newline."""
    start = index
    index += 2
    strip_tabs = command.startswith("-", index)
    if strip_tabs:
        index += 1
    while index < len(command) and command[index] in " \t":
        index += 1
    delimiter: list[str] = []
    quoted = False
    quote: str | None = None
    while index < len(command):
        char = command[index]
        if quote is not None:
            if char == quote:
                quote = None
            else:
                delimiter.append(char)
            index += 1
            continue
        if char in "'\"":
            quote = char
            quoted = True
            index += 1
            continue
        if char == "\\" and index + 1 < len(command):
            quoted = True
            delimiter.append(command[index + 1])
            index += 2
            continue
        if char in " \t\n;|&<>()":
            break
        delimiter.append(char)
        index += 1
    # Keep the redirect in raw text, but it is not a command argument.
    segment.add(" ", command[start:index])
    if delimiter:
        pending.append((segment, "".join(delimiter), strip_tabs, not quoted))
    return index


def _read_heredoc_bodies(command: str, index: int, pending: list[_Heredoc]) -> int:
    """Attach here-document bodies to the segments that declared them."""
    for segment, delimiter, strip_tabs, expands in pending:
        lines: list[str] = []
        while index < len(command):
            newline = command.find("\n", index)
            end = len(command) if newline < 0 else newline
            line = command[index:end]
            index = end + 1 if newline >= 0 else len(command)
            if (line.lstrip("\t") if strip_tabs else line) == delimiter:
                break
            lines.append(line)
        body = "\n".join(lines)
        segment.bodies.append(body)
        if expands and ("$(" in body or "`" in body):
            segment.opaque = True
    pending.clear()
    return index


def _lex_substitution(command: str, index: int, segment: _Segment, nesting: int) -> int:
    """Lex ``$(...)``, ``<(...)``, ``>(...)``, backquotes or ``$((...))``."""
    if command.startswith("$((", index):
        end = _arithmetic_end(command, index + 3)
        segment.add(command[index:end])
        segment.opaque = True
        return end
    if command[index] == "`":
        end = index + 1
        while end < len(command) and command[end] != "`":
            end += 2 if command[end] == "\\" else 1
        if end >= len(command):
            raise ValueError("unterminated backquote substitution")
        inner, _ = _lex(re.sub(r"\\([\\`$])", r"\1", command[index + 1 : end]), nesting=nesting + 1)
        end += 1
    else:
        inner, end = _lex(command, index + 2, closer=True, nesting=nesting + 1)
        if command[index] == "$" and command[index + 2 : end - 1] == "MAKE":
            segment.add(command[index:end])
            return end
        if command[index] == ">":
            # An output process substitution reads what the outer command writes.
            for item in inner:
                item.upstream = [segment, *item.upstream]
    segment.substitutions.append(inner)
    # The closing-paren search does not model case patterns; keep such a
    # substitution away from the inert path rather than trust its bounds.
    if any(_CASE_WORD.search("".join(item.text)) for item in inner):
        segment.opaque = True
    segment.add(_SUBSTITUTION, command[index:end])
    return end


class _Lexer:
    """Scan state for one ``_lex`` call: the cursor, the open segment and quoting."""

    def __init__(self, command: str, index: int, *, closer: bool, nesting: int) -> None:
        self.command = command
        self.index = index
        self.closer = closer
        self.nesting = nesting
        self.segments: list[_Segment] = []
        self.current = _Segment()
        self.pipeline: list[_Segment] = []
        self.pending: list[_Heredoc] = []
        self.quote: str | None = None
        self.word_started = False
        self.depth = 0

    def finish(self) -> _Segment | None:
        """Close a non-empty segment; an empty one keeps its pipeline input."""
        if not "".join(self.current.raw).strip():
            return None
        finished = self.current
        self.segments.append(finished)
        self.current = _Segment()
        return finished

    def step(self) -> bool:
        """Consume one unit of input; True when a substitution's closing parenthesis ends the scan."""
        if self._quoting():
            return False
        return self._operator()

    def _add_quoted(self, char: str) -> None:
        """Add one character of quoted text; the matching quote character closes the quote."""
        self.current.add(char)
        if char == self.quote:
            self.quote = None
        self.index += 1

    def _escape(self) -> None:
        """Keep a backslash pair verbatim; a backslash-newline continuation adds nothing."""
        following = self.command[self.index + 1 : self.index + 2]
        if following != "\n":
            self.current.add(self.command[self.index] + following)
            self.word_started = True
        self.index += 2

    def _quoting(self) -> bool:
        """Consume quoted text, escapes, substitutions and comments; False for unquoted syntax."""
        command, char = self.command, self.command[self.index]
        if self.quote == "'":
            self._add_quoted(char)
            return True
        if char == "\\":
            self._escape()
            return True
        if char == "`" or command.startswith("$(", self.index):
            self.index = _lex_substitution(command, self.index, self.current, self.nesting)
            self.word_started = True
            return True
        if self.quote == '"':
            self._add_quoted(char)
            return True
        if char == "#" and not self.word_started:
            newline = command.find("\n", self.index)
            self.index = len(command) if newline < 0 else newline
            return True
        if char in "'\"":
            self.quote = char
            self.current.add(char)
            self.word_started = True
            self.index += 1
            return True
        return False

    def _operator(self) -> bool:
        """Handle unquoted here-documents, substitutions and arithmetic; else the plain path."""
        command, index, char = self.command, self.index, self.command[self.index]
        if command.startswith("<<<", index):
            self.current.add("<<<")
            self.word_started = True
            self.index += 3
        elif command.startswith("<<", index):
            self.index = _lex_heredoc_operator(command, index, self.current, self.pending)
            self.word_started = True
        elif command.startswith(("<(", ">("), index):
            self.index = _lex_substitution(command, index, self.current, self.nesting)
            self.word_started = True
        elif command.startswith("((", index) and not self.word_started:
            end = _arithmetic_end(command, index + 2)
            self.current.add(command[index:end])
            self.current.opaque = True
            self.word_started = True
            self.index = end
        else:
            return self._plain(char)
        return False

    def _plain(self, char: str) -> bool:
        """Handle parentheses, blanks, redirections, separators and ordinary characters."""
        if self.closer and char == "(":
            self.depth += 1
        elif self.closer and char == ")":
            if self.depth == 0:
                self.index += 1
                return True
            self.depth -= 1
        if char in " \t\r\f\v":
            self.current.add(char)
            self.word_started = False
        elif char == "&" and (
            self.command[self.index - 1 : self.index] in {"<", ">"} or self.command.startswith(">", self.index + 1)
        ):
            self.current.add(char)
            self.word_started = True
        elif char in ";\n|&":
            self._separator(char)
            return False
        else:
            self.current.add(char)
            self.word_started = True
        self.index += 1
        return False

    def _separator(self, char: str) -> None:
        """Close the open segment at ``;``, a newline, ``|`` or ``&`` and link pipelines."""
        command, index = self.command, self.index
        pipe = char == "|" and not command.startswith("|", index + 1)
        if pipe and command.startswith("&", index + 1):
            self.index += 1
        elif char in "|&" and command.startswith(char, index + 1):
            self.index += 1
        finished = self.finish()
        if pipe:
            if finished is not None:
                self.pipeline.append(finished)
            self.current.upstream = list(self.pipeline)
        elif finished is not None or char != "\n":
            # A newline right after a pipe continues that pipeline.
            self.pipeline = []
            self.current.upstream = []
        self.word_started = False
        self.index += 1
        if char == "\n" and self.pending:
            self.index = _read_heredoc_bodies(command, self.index, self.pending)


def _lex(command: str, index: int = 0, *, closer: bool = False, nesting: int = 0) -> tuple[list[_Segment], int]:
    """Split shell text into simple-command segments.

    Separators are ``;``, ``&&``, ``||``, ``|``, ``&`` and newlines outside
    quotes. Here-document bodies are attached to their segment as data, and
    substitutions are lexed recursively. With ``closer`` the scan stops after
    the unmatched ``)`` that ends a substitution. Raises ValueError for a
    substitution that is unterminated or nested too deeply.
    """
    if nesting > _MAX_SUBSTITUTION_NESTING:
        raise ValueError("command substitution nested too deeply")
    lexer = _Lexer(command, index, closer=closer, nesting=nesting)
    while lexer.index < len(command):
        if lexer.step():
            lexer.finish()
            return lexer.segments, lexer.index
    if closer:
        raise ValueError("unterminated command substitution")
    lexer.finish()
    return lexer.segments, len(command)


def _basename(token: str) -> str:
    return token.rsplit("/", 1)[-1]


def _command_name(token: str) -> str | None:
    """Return the name of a PATH-resolved or absolute command, else None."""
    if "/" not in token:
        return token
    if token.startswith("/"):
        return _basename(token)
    return None


def _split_option_value(tokens: list[str], index: int, options: frozenset[str]) -> int:
    """Advance over an option and its separate value where required."""
    token = tokens[index]
    if token in options:
        return min(index + 2, len(tokens))
    if any(token.startswith(option + "=") for option in options if option.startswith("--")):
        return index + 1
    return index + 1


def _strip_env(tokens: list[str], index: int) -> tuple[list[str], dict[str, str]]:
    """Remove env options and assignments, returning the launched argv."""
    assignments: dict[str, str] = {}
    working = list(tokens)
    cursor = index + 1
    while cursor < len(working):
        token = working[cursor]
        if token == "--":
            return working[cursor + 1 :], assignments
        split_string: str | None = None
        suffix_start = cursor + 1
        if token.startswith("-S") and token != "-S":
            split_string = token[2:]
        elif token in {"-S", "--split-string"}:
            if cursor + 1 >= len(working):
                return [], assignments
            split_string = working[cursor + 1]
            suffix_start = cursor + 2
        elif token.startswith("--split-string="):
            split_string = token.split("=", 1)[1]
        if split_string is not None:
            split_tokens = shlex.split(split_string, comments=False, posix=True)
            working = ["env", *split_tokens, *working[suffix_start:]]
            cursor = 1
            continue
        if token in _ENV_VALUE_OPTIONS:
            cursor = min(cursor + 2, len(working))
            continue
        if token.startswith("--unset=") or token.startswith("--chdir="):
            cursor += 1
            continue
        if token.startswith("-"):
            cursor += 1
            continue
        match = _ASSIGNMENT.fullmatch(token)
        if match:
            key, value = token.split("=", 1)
            assignments[key] = value
            cursor += 1
            continue
        return working[cursor:], assignments
    return [], assignments


def _strip_uv_run(tokens: list[str], index: int) -> list[str] | None:
    """Remove ``uv run`` and its options; None for any other uv subcommand."""
    index += 1
    while index < len(tokens) and tokens[index] != "run":
        if tokens[index].startswith("-"):
            index = _split_option_value(tokens, index, _UV_VALUE_OPTIONS)
            continue
        return None
    if index >= len(tokens):
        return None
    index += 1
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return tokens[index + 1 :]
        if token == "--script":
            index += 1
            continue
        if token.startswith("--script="):
            return [token.split("=", 1)[1], *tokens[index + 1 :]]
        if not token.startswith("-"):
            return tokens[index:]
        index = _split_option_value(tokens, index, _UV_VALUE_OPTIONS)
    return []


def _skip_wrapper_options(
    tokens: list[str],
    index: int,
    *,
    value_options: frozenset[str] = frozenset(),
    short_flags: str = "",
    flag_options: frozenset[str] = frozenset(),
) -> int | None:
    """Consume known wrapper options; None for lookups and unknown forms.

    The exec/nice/timeout option rules mirror permission_audit's standalone
    wrapper scanner, including separate and attached option arguments.
    """
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in value_options:
            index += 2
            if index > len(tokens):
                return None
            continue
        if any(
            token.startswith(option + "=") if option.startswith("--") else token.startswith(option) and len(token) > 2
            for option in value_options
        ):
            index += 1
            continue
        if token in flag_options or (
            token.startswith("-") and len(token) > 1 and all(char in short_flags for char in token[1:])
        ):
            index += 1
            continue
        if token.startswith("-") and token != "-":
            return None
        return index
    return None


def _skip_exec_options(tokens: list[str], index: int) -> int | None:
    return _skip_wrapper_options(tokens, index, value_options=frozenset({"-a"}), short_flags="cl")


def _skip_nice_options(tokens: list[str], index: int) -> int | None:
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in {"-n", "--adjustment"}:
            index += 2
            if index > len(tokens):
                return None
            continue
        if token.startswith("--adjustment=") or re.fullmatch(r"-n-?\d+", token) or re.fullmatch(r"-\d+", token):
            index += 1
            continue
        if token.startswith("-") and token != "-":
            return None
        return index
    return None


def _skip_timeout_options(tokens: list[str], index: int) -> int | None:
    duration_index = _skip_wrapper_options(
        tokens,
        index,
        value_options=frozenset({"-k", "--kill-after", "-s", "--signal"}),
        short_flags="vfp",
        flag_options=frozenset({"--preserve-status", "--foreground", "--verbose"}),
    )
    if duration_index is None or duration_index + 1 >= len(tokens):
        return None
    return duration_index + 1


def _skip_wrapper(tokens: list[str], index: int, wrapper: str) -> int | None:
    """Return the launched argv index for a transparent execution wrapper."""
    index += 1
    if wrapper == "timeout":
        return _skip_timeout_options(tokens, index)
    if wrapper == "nice":
        return _skip_nice_options(tokens, index)
    if wrapper == "exec":
        return _skip_exec_options(tokens, index)
    if wrapper == "nohup":
        return _skip_wrapper_options(tokens, index)
    if wrapper in {"time", "command"}:
        return _skip_wrapper_options(tokens, index, short_flags="p")
    if wrapper == "caffeinate":
        return _skip_wrapper_options(tokens, index, value_options=frozenset({"-t", "-w"}), short_flags="dimsu")
    if wrapper == "stdbuf":
        return _skip_wrapper_options(
            tokens, index, value_options=frozenset({"-i", "-o", "-e", "--input", "--output", "--error"})
        )
    if wrapper == "sudo":
        # --preserve-env is a flag; as a value option it would swallow the program word.
        return _skip_wrapper_options(
            tokens,
            index,
            value_options=frozenset(
                {
                    "-u",
                    "-g",
                    "-p",
                    "-C",
                    "-D",
                    "-r",
                    "-t",
                    "-T",
                    "-U",
                    "-R",
                    "--user",
                    "--group",
                    "--prompt",
                    "--close-from",
                    "--chdir",
                    "--role",
                    "--type",
                    "--command-timeout",
                    "--other-user",
                    "--chroot",
                }
            ),
            short_flags="AbEHiKknPSs",
            flag_options=frozenset(
                {
                    "--askpass",
                    "--background",
                    "--preserve-env",
                    "--set-home",
                    "--login",
                    "--remove-timestamp",
                    "--reset-timestamp",
                    "--non-interactive",
                    "--preserve-groups",
                    "--stdin",
                    "--shell",
                }
            ),
        )
    if wrapper == "doas":
        return _skip_wrapper_options(tokens, index, value_options=frozenset({"-u", "-a"}), short_flags="ns")
    if wrapper == "xargs":
        return _skip_wrapper_options(
            tokens,
            index,
            value_options=frozenset(
                {
                    "-n",
                    "-P",
                    "-I",
                    "-L",
                    "-s",
                    "-E",
                    "-d",
                    "--max-args",
                    "--max-procs",
                    "--replace",
                    "--max-lines",
                    "--max-chars",
                    "--eof",
                    "--delimiter",
                }
            ),
            short_flags="0rtpx",
            flag_options=frozenset({"--null", "--no-run-if-empty", "--verbose", "--interactive", "--exit"}),
        )
    return None


def _is_command_lookup(tokens: list[str]) -> bool:
    """``command -v``/``-V`` only resolve names; they never launch them."""
    for token in tokens[1:]:
        if not token.startswith("-") or token == "--":
            return False
        if "v" in token or "V" in token:
            return True
    return False


def _strip_prefixes(tokens: list[str]) -> tuple[list[str], dict[str, str], bool, bool]:
    """Strip assignments and execution wrappers.

    Returns the launched argv, the prefix assignments, whether every prefix
    was understood, and whether xargs appends stdin words to the argv. A
    known wrapper with an option form the parser does not model is not
    understood, so its segment is ``unknown``.
    """
    remaining = list(tokens)
    assignments: dict[str, str] = {}
    stdin_args = False
    while remaining:
        while remaining and _ASSIGNMENT.fullmatch(remaining[0]):
            key, value = remaining.pop(0).split("=", 1)
            assignments[key] = value
        if not remaining:
            break
        name = _command_name(remaining[0])
        if name == "env":
            remaining, env_assignments = _strip_env(remaining, 0)
            assignments.update(env_assignments)
            continue
        if name == "uv":
            launched = _strip_uv_run(remaining, 0)
            if launched is None:
                return remaining, assignments, _package_is_inert(remaining), stdin_args
            remaining = launched
            continue
        if name == "command" and _is_command_lookup(remaining):
            break
        if name in _WRAPPERS:
            wrapped_index = _skip_wrapper(remaining, 0, name)
            if wrapped_index is None:
                return remaining, assignments, False, stdin_args
            stdin_args = stdin_args or name == "xargs"
            remaining = remaining[wrapped_index:]
            continue
        break
    return remaining, assignments, True, stdin_args


def _export_bypass_value(tokens: list[str]) -> bool | None:
    """Return an updated export state for a standalone export segment."""
    if not tokens or tokens[0] != "export":
        return None
    state: bool | None = None
    for token in tokens[1:]:
        match = _ENV_ASSIGNMENT.fullmatch(token)
        if match:
            state = match.group(1) == "1"
    return state


class _MakeArgv:
    """One whole-argv parse shared by structural and bounded raw classification."""

    def __init__(self, command: list[str]) -> None:
        self.goals: list[str] = []
        self.directories: list[str] = []
        self.makefiles: list[str] = []
        self.eval = False
        self.source_env = False
        self.dry_run = False
        self.unparsed: str | None = None
        index = 1
        options_ended = False
        while index < len(command):
            word = command[index]
            index += 1
            if not options_ended and word == "--":
                options_ended = True
                continue
            options: list[tuple[str, str | None]] = []
            if not options_ended and word.startswith("--"):
                option, equals, value = word.partition("=")
                if option in _MAKE_VALUE_OPTIONS:
                    if not equals:
                        if index == len(command):
                            self.unparsed = word
                            return
                        value = command[index]
                        index += 1
                    options.append((option, value))
                elif option in _MAKE_NUMERIC_OPTIONS:
                    pattern = _MAKE_JOBS_VALUE if option == "--jobs" else _MAKE_LOAD_VALUE
                    if not equals and index < len(command) and pattern.fullmatch(command[index]):
                        value = command[index]
                        index += 1
                    if (equals or value) and not pattern.fullmatch(value):
                        self.unparsed = word
                        return
                elif option in _MAKE_LONG_FLAGS and not equals:
                    options.append((option, None))
                else:
                    self.unparsed = word
                    return
            elif not options_ended and word.startswith("-") and word != "-":
                # Scan a single-dash cluster left to right. A required value
                # terminates it: the suffix is data, so -sCn never selects -n.
                cursor = 1
                while cursor < len(word):
                    letter = word[cursor]
                    cursor += 1
                    option = "-" + letter
                    if option in _MAKE_VALUE_OPTIONS:
                        value = word[cursor:]
                        if not value:
                            if index == len(command):
                                self.unparsed = word
                                return
                            value = command[index]
                            index += 1
                        options.append((option, value))
                        break
                    if letter in "jl":
                        # GNU make's optional value is the rest of the cluster, so an attached
                        # value ends it: -ln is load "n", never a dry run. A separated value is
                        # consumed only when numeric, and -j takes digits only.
                        pattern = _MAKE_JOBS_VALUE if letter == "j" else _MAKE_LOAD_VALUE
                        remainder = word[cursor:]
                        if remainder and letter == "j" and not pattern.fullmatch(remainder):
                            self.unparsed = word
                            return
                        if not remainder and index < len(command) and pattern.fullmatch(command[index]):
                            index += 1
                        break
                    elif letter in _MAKE_SHORT_FLAGS:
                        options.append((option, None))
                    else:
                        self.unparsed = word
                        return
            elif _ASSIGNMENT.fullmatch(word):
                self.source_env = self.source_env or word.partition("=")[0] in {"MAKEFILES", "MAKEFLAGS", "GNUMAKEFLAGS", "MFLAGS"}
            else:
                self.goals.append(word)
            for option, value in options:
                if option in {"-C", "--directory"}:
                    self.directories.append(value or "")
                elif option in {"-f", "--file", "--makefile"}:
                    self.makefiles.append(value or "")
                elif option in {"-E", "--eval"}:
                    self.eval = True
                elif option in {"-n", "--just-print", "--dry-run", "--recon"}:
                    self.dry_run = True


def _package_is_inert(command: list[str]) -> bool:
    """Package operands name dependencies rather than Python test runners."""
    name = _command_name(command[0])
    args = command[1:]
    pip_actions = {"show", "list", "install", "download", "uninstall"}
    if name and re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", name):
        index = 1
        while index < len(command):
            token = command[index]
            if token == "-m" and command[index + 1 : index + 2] == ["pip"]:
                args = command[index + 2 :]
                name = "pip"
                break
            if token == "-mpip":
                args = command[index + 1 :]
                name = "pip"
                break
            if token in {"-W", "-X"}:
                index += 2
            elif token.startswith("-") and token not in {"-c", "-m", "--"}:
                index += 1
            else:
                return False
    if name in {"pip", "pip3"}:
        # Only known global flags precede the action; unknown options stay fail-safe.
        while args and args[0] in {"-q", "-v", "--quiet", "--verbose", "--isolated", "--disable-pip-version-check"}:
            args = args[1:]
        return bool(args) and args[0] in pip_actions
    if name == "uv":
        index = 1
        while index < len(command) and command[index].startswith("-"):
            index = _split_option_value(command, index, _UV_VALUE_OPTIONS)
        args = command[index:]
        return bool(args) and (
            args[0] in {"add", "remove"}
            or (len(args) > 1 and args[0] == "pip" and args[1] in pip_actions - {"download"})
        )
    return False


def _is_loopback_destination(destination: str) -> bool:
    if destination.startswith("ssh://"):
        destination = urlsplit(destination).hostname or ""
    else:
        destination = destination.rsplit("@", 1)[-1]
        if destination.startswith("[") and destination.endswith("]"):
            # Bracketed IPv6 addresses are literal SSH hosts, not pathname globs.
            try:
                ipaddress.IPv6Address(destination[1:-1])
            except ValueError:
                return True
            destination = destination[1:-1]
    if _may_expand(destination) or _may_glob(destination) or "%" in destination:
        return True  # cannot prove the destination is remote
    hostname = socket.gethostname().lower().rstrip(".")
    host = destination.lower().rstrip(".")
    if not host or host in {"localhost", hostname, hostname.split(".", 1)[0]}:
        return True
    try:
        address = ipaddress.ip_address(host)
        mapped = address.ipv4_mapped if isinstance(address, ipaddress.IPv6Address) else None
        return address.is_loopback or address.is_unspecified or (
            mapped is not None and (mapped.is_loopback or mapped.is_unspecified)
        )
    except ValueError:
        # inet_aton is a local numeric parser: SSH also accepts shortened,
        # decimal and hexadecimal IPv4 forms such as 127.1 and 0x7f000001.
        try:
            address = ipaddress.IPv4Address(socket.inet_aton(host))
            return address.is_loopback or address.is_unspecified
        except OSError:
            return False


def _ssh_options(command: list[str]) -> tuple[str | None, list[tuple[str, str]], bool, list[str]]:
    """Parse local options, stopping at the remote command (including after --).

    Returns the destination, the options, whether every option was understood and the remote command words.
    """
    index = 1
    destination: str | None = None
    options: list[tuple[str, str]] = []
    value_options = frozenset({"-b", "-c", "-D", "-E", "-e", "-F", "-i", "-J", "-L", "-l", "-m", "-O", "-o", "-p", "-Q", "-R", "-S", "-W", "-w"})
    while index < len(command):
        token = command[index]
        if token == "--":
            index += 1
            if destination is None and index < len(command):
                destination = command[index]
                index += 1
            break
        if not token.startswith("-"):
            if destination is not None:
                break  # The remote command and everything after it are remote argv.
            destination = token
            index += 1
            continue  # OpenSSH also parses options immediately after the destination.
        option = token[:2]
        if option in value_options:
            if token == option:
                if index + 1 >= len(command):
                    return destination, options, False, []
                value = command[index + 1]
                index += 2
            else:
                value = token[2:]
                index += 1
            options.append((option, value))
            continue
        if len(token) > 1 and all(flag in "1246AaCfGgKkMNnqsTtVvXxYy" for flag in token[1:]):
            index += 1
            continue
        return destination, options, False, []
    return destination, options, True, command[index:]


def _ssh_is_remote(command: list[str], scan: _Scan | None = None) -> bool:
    destination, options, valid, _remote = _ssh_options(command)
    if not valid or destination is None:
        return False
    hostname_override: str | None = None
    for option, value in options:
        if option == "-F" and (_may_expand(value) or (scan is not None and _earlier_file(value, scan))):
            return False
        if option == "-o":
            key, _, setting = value.replace("=", " ", 1).partition(" ")
            # Auth paths and user names are data. These SSH options can run
            # command text on the workstation.
            if key.lower() in {"proxycommand", "localcommand", "knownhostscommand"} and _raw_test_word(setting):
                return False
            # OpenSSH keeps the first obtained value, including repeated -o.
            if key.lower() == "hostname" and hostname_override is None:
                hostname_override = setting.strip()
    return destination is not None and not _is_loopback_destination(hostname_override or destination)


def _python_runs_pytest(tokens: list[str]) -> bool:
    """Recognize python interpreter ``-m pytest`` with ordinary options."""
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token == "-m":
            return index + 1 < len(tokens) and tokens[index + 1] == "pytest"
        if not token.startswith("-"):
            return False
        if token in {"-W", "-X"}:
            index += 2
        else:
            index += 1
    return False


def _python_runs_named_runner(tokens: list[str]) -> bool:
    """Find run_named_tests.py as the Python script after interpreter options."""
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            index += 1
            break
        if token in {"-c", "-m"}:
            return False
        if token in {"-W", "-X"}:
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        return token.endswith("run_named_tests.py")
    return index < len(tokens) and tokens[index].endswith("run_named_tests.py")


def _runs_host_tests(command: list[str]) -> bool:
    word = _basename(command[0])
    if word in {"pytest", "py.test"} or command[0].endswith("run_named_tests.py"):
        return True
    return word.startswith("python") and (_python_runs_pytest(command) or _python_runs_named_runner(command))


def _shell_command(tokens: list[str]) -> tuple[str, list[str]] | None:
    if not tokens or _command_name(tokens[0]) not in _SHELLS:
        return None
    # Only leading options select -c: after "--", "-" or the first script operand, -c is a script argument.
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token == "-c" or token == "--command":
            return (tokens[index + 1], tokens[index + 2 :]) if index + 1 < len(tokens) else None
        if token.startswith("-") and "c" in token[1:] and not token.startswith("--"):
            return (tokens[index + 1], tokens[index + 2 :]) if index + 1 < len(tokens) else None
        if token in {"--", "-"} or not token.startswith(("-", "+")):
            return None
        index += 2 if token in _SHELL_VALUE_OPTIONS else 1
    return None


def _git_is_inert(command: list[str]) -> bool:
    index = 1
    while index < len(command):
        token = command[index]
        if token in {"-C", "--git-dir", "--work-tree"}:
            index += 2
            continue
        if token in {"--no-pager", "-P", "--no-optional-locks"} or token.startswith(("--git-dir=", "--work-tree=")):
            index += 1
            continue
        if token.startswith("-"):
            return False
        break
    if index >= len(command) or command[index] not in _GIT_INERT_SUBCOMMANDS:
        return False
    # git grep -O/--open-files-in-pager runs the named pager.
    return command[index] != "grep" or not any(
        token.startswith(("-O", "--open-files-in-pager")) for token in command[index + 1 :]
    )


def _is_inert(command: list[str], scan: _Scan | None = None) -> bool:
    """Return True when the command word never executes its arguments."""
    name = _command_name(command[0])
    if _package_is_inert(command):
        return True
    if name == "ssh":
        return _ssh_is_remote(command, scan)
    if name in _INERT_COMMANDS:
        # printf -v NAME stores its text in a shell variable that a later segment can eval.
        return not (name == "printf" and any(token.startswith("-v") for token in command[1:]))
    if name == "rg":
        return not any(token.startswith("--pre") for token in command[1:])
    if name == "git":
        return _git_is_inert(command)
    if name == "gh":
        return len(command) > 1 and command[1] in {"issue", "pr"}
    if name == "find":
        return not any(token in _FIND_EXEC_ACTIONS for token in command[1:])
    if name == "sort":
        return not any(token.startswith("--compress-program") for token in command[1:])
    if name == "command":
        return _is_command_lookup(command)
    return False


def _strip_shell_group_tokens(tokens: list[str]) -> list[str]:
    """Remove the shell syntax that leads a simple command, up to its command word."""
    result = list(tokens)
    if result[:1] == ["for"]:
        # The for header binds a variable; its body begins in a later do segment.
        return []
    while result:
        head = result[0]
        redirect = _LEADING_REDIRECT.match(head)
        if head in _RESERVED_WORDS | {"fi", "done", "esac"} or head in _FUNCTION_PARENS or _FUNCTION_HEADER.match(head):
            width = 1
        elif head == "case" and len(result) >= 4 and result[2] == "in" and _CASE_PATTERN.match(result[3]):
            width = 4
        elif (
            _CASE_PATTERN.match(head)
            and len(result) > 1
            and result[1] not in {")", "}", "fi", "done", "esac"}
            and not _REDIRECT_START.match(result[1])
        ):
            # A subshell's closing paren is followed by redirects at most, so a word after it opens a case arm.
            width = 1
        elif head == "function" and len(result) >= 2 and _FUNCTION_NAME.match(result[1]):
            width = 2
        elif redirect is not None:
            # A detached target is the next word, so that redirect takes two tokens.
            width = 1 if redirect.group(2) else 2
        else:
            break
        del result[:width]
    if result and result[0].startswith("(") and not result[0].startswith("$("):
        result[0] = result[0].lstrip("(")
    while result and result[-1] in {")", "}", ";"}:
        result.pop()
    if result and _basename(result[0]) not in _SHELLS and not result[-1].startswith("$("):
        last = result[-1]
        # Strip only closers this word leaves unbalanced: "x.py)" but not "{}".
        while last and last[-1] in ")}" and last.count(last[-1]) > last.count("(" if last[-1] == ")" else "{"):
            last = last[:-1]
        result[-1] = last
        if not result[-1]:
            result.pop()
    return result


_Route = tuple[str, str]
# What one reading of a text holds: a runner word, a runner word once quotes are removed, the first
# make goal that names a test target, and the first other make finding. Earlier words win a merge.
_Words = tuple["str | None", "str | None", "_Route | None", "_Route | None"]
_Readings = tuple[_Words, _Words]
_NO_READINGS: _Readings = ((None, None, None, None), (None, None, None, None))


def _merge_readings(first: _Readings, second: _Readings) -> _Readings:
    """The words of the text ``first`` followed by the text ``second``."""
    (joined_a, printed_a), (joined_b, printed_b) = first, second
    return (
        tuple(a if a is not None else b for a, b in zip(joined_a, joined_b)),  # type: ignore[return-value]
        tuple(a if a is not None else b for a, b in zip(printed_a, printed_b)),  # type: ignore[return-value]
    )


def _raw_test_word(
    command: str, *, include_default_make: bool = False, deny_unlisted_make: bool = True,
    prior: _PriorText | None = None,
) -> tuple[str, str] | None:
    """Find a test-run word in text whose shape the parser could not prove.

    This scan never treats text as proof of an audited bypass. It returns only
    the matched word and route, never the surrounding text. With ``prior`` the
    text is read as following the earlier text the scan has collected, without
    reading that text again.
    """
    readings = _text_words(command, include_default_make=include_default_make, deny_unlisted_make=deny_unlisted_make)
    if prior is not None:
        readings = _merge_readings(prior.words(include_default_make, deny_unlisted_make), readings)
    found: _Route | None = None
    for runner, stripped, target, other in readings:
        word = runner or stripped
        candidate = (word, "test-files") if word is not None else (target if target is not None else other)
        if candidate is not None:
            # Both readings refuse. Prefer a named route for the message,
            # instead of a partial/printed-escape word's generic route.
            if candidate[1] != "test-files":
                return candidate
            if found is None:
                found = candidate
    return found


def _text_words(command: str, *, include_default_make: bool, deny_unlisted_make: bool) -> _Readings:
    """The words of the two readings of a text: with continuations joined, and with whitespace escapes printed."""
    # The shell deletes each backslash-newline pair before it splits words, as it
    # does quotes and backslashes, so a continuation can split a make or runner
    # word. Parse normalized text: an unfinished pre-continuation token is not
    # a make goal and must not mask the joined goal or refuse inert help.
    joined = re.sub(r"\\\r?\n", "", command)
    # printf/echo can print whitespace escapes before a literal runner word.
    # Unknown readers of that produced text must see the same word boundary.
    printed_whitespace = re.sub(r"\\[nrt]", " ", joined)
    first, second = (
        _test_word_in(text, include_default_make=include_default_make, deny_unlisted_make=deny_unlisted_make)
        for text in (joined, printed_whitespace)
    )
    return first, second


def _test_word_in(command: str, *, include_default_make: bool = False, deny_unlisted_make: bool = True) -> _Words:
    """Scan one reading of the text for a runner word or a make goal."""
    # Quote concatenation and backslashes split a runner name (py""test), so the
    # text without them is matched as well.
    runner = _RUNNER_WORD.search(command)
    stripped = None if runner is not None else _RUNNER_WORD.search(re.sub(r"[\"'\\]", "", command))
    if runner is not None or stripped is not None:
        # A runner word decides the reading, so the make goals after it cannot change the result.
        return (runner.group() if runner else None, stripped.group() if stripped else None, None, None)
    target, other = _make_words(command, include_default_make=include_default_make, deny_unlisted_make=deny_unlisted_make)
    return None, None, target, other


def _make_words(command: str, *, include_default_make: bool, deny_unlisted_make: bool) -> tuple["_Route | None", "_Route | None"]:
    """The first test-target goal and the first other finding among the make words of one reading."""
    found: _Route | None = None
    for make in _MAKE_WORD.finditer(command):
        # A quote can close outside this make word (printf '%s ' 'make' 'test'), so
        # the tail is read two ways. Quote-grouped words keep a quoted argv word
        # whole, so make options and assignments still consume their values
        # instead of turning a directory named test into a goal. Plain words
        # cannot merge a goal into a quote that closes elsewhere. A goal from
        # either reading counts. Parentheses and backquotes delimit substitution
        # words, and each goal loses its quotes and backslashes, as the shell does.
        tail = re.split(r"[;|&\n]", command[make.end() :], maxsplit=1)[0]
        if "<" in tail or ">" in tail:
            # Redirect targets are shell data, but goals after the redirect
            # still belong to make. Reuse the redirect parser for quoted
            # targets and adjacent fd numbers. Unbalanced raw quotes retain
            # the conservative two readings below.
            try:
                redirect_segment = _Segment()
                redirect_segment.add(tail)
                redirects = _RedirectCommand(redirect_segment)
                if not redirects.malformed:
                    tail = " ".join(shlex.quote(word) for word in redirects.argv)
            except ValueError:
                pass
        readings = (
            [re.sub(r"[\"'\\]", "", word) for word in _SHELL_WORD.findall(tail)],
            [re.sub(r"[\"'\\]", "", word) for word in re.findall(r"[^\s()<>`]+", tail)],
        )
        for words in readings:
            argv = _MakeArgv([make.group(), *words])
            if argv.unparsed is not None or argv.makefiles or argv.eval or argv.source_env:
                # A known goal can improve the suggested route on a refusal;
                # it never authorizes an unparsed argv or alternate source.
                target = next((goal for goal in argv.goals if goal in _TEST_TARGETS), None)
                if target is not None:
                    return (target, target), found
                if found is None:
                    found = (make.group(), "test-files")
                continue
            if include_default_make and not argv.goals and not argv.dry_run:
                if found is None:
                    found = (make.group(), "test-files")
                continue
            for goal in argv.goals:
                # A goal the shell expands or globs may select a test target at run time.
                if found is None and (_may_expand(goal) or _may_glob(goal)):
                    found = (goal, "test-files")
                elif not argv.dry_run and (
                    goal in _TEST_TARGETS or (deny_unlisted_make and goal not in _INERT_MAKE_GOALS)
                ):
                    # The test table chooses message wording, never permission.
                    if goal in _TEST_TARGETS:
                        return (goal, goal), found
                    if found is None:
                        found = (goal, "test-files")
    return None, found


_Finding = tuple[str, str, "str | None", str]


class _Names:
    """Basenames of produced paths.

    The hook tracks no cd, so a path whose basename was produced under another spelling may name the same
    file. Two absolute paths with different directories cannot, so an absolute path matches only a name
    that was produced under a relative path.
    """

    def __init__(self, parent: _Names | None = None) -> None:
        self.every = set(parent.every) if parent is not None else set()
        self.relative = set(parent.relative) if parent is not None else set()
        self.makefile_fragment = parent.makefile_fragment if parent is not None else False

    def add(self, path: str) -> None:
        name = _basename(path)
        self.every.add(name)
        if not path.startswith("/"):
            self.relative.add(name)
        if path.endswith(".mk") or path.startswith("Makefile.d/") or "/Makefile.d/" in path:
            self.makefile_fragment = True

    def may_hold_makefile(self) -> bool:
        """True when a produced path may be a makefile make reads implicitly or includes."""
        return self.makefile_fragment or not _IMPLICIT_MAKEFILES.isdisjoint(self.every)

    def may_alias(self, path: str) -> bool:
        """True when the path's basename was produced under another path; "", "." and ".." name no file."""
        name = _basename(path)
        return name not in {"", ".", ".."} and name in (self.relative if path.startswith("/") else self.every)


class _PriorText:
    """The text earlier segments printed or wrote, and facts about it that later segments reuse.

    Segments only append parts, and every scan rewinds to its starting mark when it ends. Each fact is
    folded once per part, so a long run of segments that read the earlier text costs time linear in the
    command, instead of a rescan of the whole earlier text for every reader.
    """

    def __init__(self) -> None:
        self.parts: list[str] = []
        self._unquoted: list[str] = []
        self._joined: str | None = None
        self._unquoted_joined: str | None = None
        self._folds: dict[object, list] = {}

    def append(self, text: str) -> None:
        self.parts.append(text)
        self._joined = self._unquoted_joined = None

    def mark(self) -> int:
        return len(self.parts)

    def rewind(self, mark: int) -> None:
        """Drop the parts appended after ``mark`` and every fact folded from them."""
        if len(self.parts) > mark:
            del self.parts[mark:]
            del self._unquoted[mark:]
            for folded in self._folds.values():
                del folded[mark:]
            self._joined = self._unquoted_joined = None

    def text(self) -> str:
        if self._joined is None:
            self._joined = "\n".join(self.parts)
        return self._joined

    def unquoted(self) -> str:
        """The text with the quotes and backslashes the shell removes taken out."""
        if self._unquoted_joined is None:
            while len(self._unquoted) < len(self.parts):
                self._unquoted.append(re.sub(r"[\"'\\]", "", self.parts[len(self._unquoted)]))
            self._unquoted_joined = "\n".join(self._unquoted)
        return self._unquoted_joined

    def fold(self, key: object, summarize: Callable[[str], Any], merge: Callable[[Any, Any], Any], empty: Any) -> Any:
        """``summarize`` each part once and ``merge`` the results in order; a part is never summarized twice."""
        folded = self._folds.setdefault(key, [])
        while len(folded) < len(self.parts):
            summary = summarize(self.parts[len(folded)])
            folded.append(merge(folded[-1], summary) if folded else summary)
        return folded[-1] if folded else empty

    def matches(self, pattern: re.Pattern) -> bool:
        """True when the pattern matches inside any part. Patterns used here cannot match across the newline between parts."""
        return self.fold(pattern, lambda part: pattern.search(part) is not None, lambda a, b: a or b, False)

    def words(self, include_default_make: bool, deny_unlisted_make: bool) -> _Readings:
        return self.fold(
            ("words", include_default_make, deny_unlisted_make),
            lambda part: _text_words(part, include_default_make=include_default_make, deny_unlisted_make=deny_unlisted_make),
            _merge_readings,
            _NO_READINGS,
        )


class _Scan:
    """Findings ``(reason, target, via, word)`` plus unparseable-text errors."""

    def __init__(self) -> None:
        self.findings: list[_Finding] = []
        self.parse_errors: list[str] = []
        self.text_executor = False
        self.text_executor_via: str | None = None
        # Set by any unbypassed text executor, even after a bypassed one marked first.
        self.text_executor_unbypassed = False
        # Normalized output-redirect targets of the whole command, and the text earlier segments produced.
        self.written_files: frozenset[str] = frozenset()
        self.produced_words: set[str] = set()
        # The basenames of the written files and produced words.
        self.produced_names = _Names()
        self.prior = _PriorText()
        self.written_aliases: set[int] = set()
        self.file_production = _FileProduction()

    @property
    def prior_text(self) -> str:
        return self.prior.text()

    def add_produced_text(self, text: str) -> None:
        """Index each produced segment once; join its text only if a reader needs it."""
        self.prior.append(text)
        for item in _PRIOR_WORD_SPLIT.split(text):
            if item:
                word = _canonical_path(item)
                self.produced_words.add(word)
                self.produced_names.add(word)


def _mark_text_executor(scan: _Scan, via: str | None) -> None:
    """Record a segment that runs stored text; the first mark sets the via, an unbypassed mark always counts."""
    if not scan.text_executor:
        scan.text_executor = True
        scan.text_executor_via = via
    if via is None:
        scan.text_executor_unbypassed = True


def _stored_runner_word(text: str) -> str | None:
    """Return the first runner or make word in the text, read as the shell would join it."""
    joined = re.sub(r"\\\r?\n", "", text)
    for reading in (text, joined, re.sub(r"[\"'\\]", "", joined)):
        word = _RUNNER_WORD.search(reading) or _MAKE_WORD.search(reading)
        if word is not None:
            return word.group()
    return None


def _record_unknown(
    scan: _Scan, text: str, via: str | None, *, parse_error: str | None = None,
    include_default_make: bool = False, deny_unlisted_make: bool = True, after_prior: bool = False
) -> None:
    """Refuse an ``unknown`` segment that names a test runner; allow the rest.

    With ``after_prior`` the segment runs text an earlier segment produced, so that text is scanned too.
    """
    match = _raw_test_word(
        text, include_default_make=include_default_make, deny_unlisted_make=deny_unlisted_make,
        prior=scan.prior if after_prior else None,
    )
    if match is not None:
        word, target = match
        scan.findings.append(("unparseable" if parse_error is not None else _UNPARSED_SHAPE, target, via, word))
    elif parse_error is not None:
        scan.parse_errors.append(parse_error)


def _stdin_text(segment: _Segment) -> str:
    """Data a segment receives on stdin: pipe input, heredocs, here-strings."""
    parts: list[str] = []
    for item in segment.upstream:
        parts.append("".join(item.raw))
        parts.extend(item.bodies)
    parts.extend(segment.bodies)
    raw = "".join(segment.raw)
    if "<<<" in raw:
        parts.append(raw)
    return "\n".join(parts)


def _reads_prior(segment: _Segment) -> bool:
    """True when the segment's stdin is the earlier text, so its scan must merge that text."""
    return any(item.from_prior for item in segment.upstream)


def _segment_scan_text(segment: _Segment, tokens: list[str] | None) -> str:
    """Text an ``unknown`` segment may execute, including piped-in stdin."""
    parts: list[str] = []
    for item in (*segment.upstream, segment):
        parts.append("".join(item.raw))
        parts.extend(item.bodies)
    if tokens:
        parts.append(" ".join(tokens))
    return "\n".join(parts)


def _redirect_targets(text: str) -> frozenset[str]:
    """Normalized output-redirect targets: files the command may write before a later segment runs them."""
    return frozenset(_canonical_path(target) for target in _OUTPUT_REDIRECT.findall(text))


def _earlier_file(word: str, scan: _Scan) -> bool:
    """True when the word may name a file an earlier segment produced, or may expand to one."""
    if _may_expand(word):
        return True
    literal = word.lstrip("<")
    path = _canonical_path(literal)
    # A bare redirect operator names no file, and "." names the directory, not a file an earlier segment made.
    if not literal or path == ".":
        return False
    if path in scan.written_files:
        return True
    if _PRIOR_WORD_SPLIT.search(literal):
        # A quoted or escaped name holds a separator that the word split would cut, so match the whole name,
        # normalized like the produced side, and as a pattern when it may glob.
        unquoted = scan.prior.unquoted()
        if _may_glob(path):
            return re.search(_glob_pattern(path), unquoted) is not None
        return path in unquoted or literal in unquoted
    if _may_glob(path):
        return any(fnmatch.fnmatchcase(item, path) for item in scan.produced_words | scan.written_files)
    return path in scan.produced_words


def _may_alias_earlier_file(path: str, scan: _Scan) -> bool:
    """True when a path shares its basename with a produced name, so an earlier cd could make them one file.

    The hook cannot prove that sub/s and s are different files; only a name that was never produced stays
    outside the earlier text.
    """
    return scan.produced_names.may_alias(path)


def _glob_pattern(word: str) -> str:
    """Unanchored regex for a pathname pattern: * and ? do not cross a separator, and a bracket matches one character."""
    pattern: list[str] = []
    index = 0
    while index < len(word):
        char = word[index]
        if char == "*":
            pattern.append("[^/]*")
        elif char == "?":
            pattern.append("[^/]")
        elif char == "[" and "]" in word[index + 1 :]:
            index = word.index("]", index + 1)
            pattern.append("[^/]")
        else:
            pattern.append(re.escape(char))
        index += 1
    return "".join(pattern)


# Redirects retain their fd, operator and target independently of command argv.
_Redirect = tuple[int, str, str]
_FdSource = tuple[str, str]
_DESCRIPTOR_PATH = re.compile(r"/(?:dev/fd|proc/(?:self|thread-self)/fd)/([0-9]+)")
_STANDARD_DESCRIPTOR_PATHS = {"/dev/stdin": 0, "/dev/stdout": 1, "/dev/stderr": 2}
_RELATIVE_DESCRIPTOR_PATH = re.compile(r"(?:^|/)fd/[0-9]+$")
_REDIRECT_OPERATOR = re.compile(r"<<<|<<-|<<|<>|<&|>>|>\||>&|&>>|&>|[<>]")
_INPUT_OPERATORS = frozenset({"<", "<>", "<&"})
_PRODUCING_OPERATORS = frozenset({"<>", ">", ">>", ">|", "&>", "&>>"})


def _canonical_path(word: str) -> str:
    """Normalize a pathname, including POSIX's otherwise-preserved pair of leading slashes."""
    path = os.path.normpath(word)
    return "/" + path.lstrip("/") if path.startswith("/") else path


def _descriptor_fd(path: str) -> int | None:
    """Map a canonical, process-local descriptor operand to its fd; all other aliases are unproven."""
    if path in _STANDARD_DESCRIPTOR_PATHS:
        return _STANDARD_DESCRIPTOR_PATHS[path]
    match = _DESCRIPTOR_PATH.fullmatch(path)
    try:
        return int(match[1]) if match is not None else None
    except ValueError:
        # An unbounded fd number remains an unproven /dev/ or /proc/ operand.
        return None


def _unproven_device_path(path: str) -> bool:
    """Unknown device/proc operands and relative descriptor spellings cannot prove file provenance."""
    return path.startswith(("/dev/", "/proc/")) or _RELATIVE_DESCRIPTOR_PATH.search(path) is not None


class _FileProduction:
    """Incremental evidence from parsed earlier segments, excluding input-only redirect operands."""

    def __init__(self, parent: _FileProduction | None = None) -> None:
        self.paths = set(parent.paths) if parent is not None else set()
        self.words = set(parent.words) if parent is not None else set()
        self.names = _Names(parent.names if parent is not None else None)
        self.protected_writer = parent.protected_writer if parent is not None else False

    def add_path(self, path: str) -> None:
        self.paths.add(path)
        self.names.add(path)

    def add_word(self, word: str) -> None:
        self.words.add(word)
        self.names.add(word)

    def matches(self, path: str) -> bool:
        if _may_expand(path) or path in self.paths or path in self.words:
            return True
        return _may_glob(path) and any(fnmatch.fnmatchcase(item, path) for item in self.words | self.paths)

    def may_alias(self, path: str) -> bool:
        return self.names.may_alias(path)


def _redirect_word_end(text: str, start: int) -> int:
    """Find a Bash word boundary without interpreting quoted or escaped metacharacters."""
    index, quote = start, ""
    while index < len(text):
        char = text[index]
        if char == "\\" and quote != "'":
            index += 2
            continue
        if quote:
            if char == quote:
                quote = ""
        elif char in "'\"":
            quote = char
        elif text.startswith(("$(MAKE)", "${MAKE}"), index):
            index += 7
            continue
        elif char.isspace() or char in "|&;()<>":
            break
        index += 1
    return index


def _redirect_words(text: str) -> list[tuple[str, str, int, int]]:
    """Return (value, spelling, start, end), preserving fd/operator adjacency and quoting."""
    words: list[tuple[str, str, int, int]] = []
    index = 0
    while index < len(text):
        if text[index].isspace():
            index += 1
            continue
        start = index
        operator = _REDIRECT_OPERATOR.match(text, index)
        if operator:
            index = operator.end()
        elif text[index] in "|&;()":
            index += 1
        else:
            index = _redirect_word_end(text, index)
        raw = text[start:index]
        value = raw if operator or raw in "|&;()" else shlex.split(raw, comments=False, posix=True)[0]
        words.append((value, raw, start, index))
    return words


class _RedirectCommand:
    """The original segment, argv, redirect records and compound membership."""

    def __init__(self, segment: _Segment) -> None:
        self.segment = segment
        self.text = "".join(segment.text)
        self.legacy = shlex.split(self.text, comments=False, posix=True)
        self.words = _redirect_words(self.text)
        self.redirects: list[_Redirect] = []
        self.offsets: list[int] = []
        self.malformed = False
        self.groups: list[_RedirectGroup] = []
        self.argv = self._extract()

    def _extract(self) -> list[str]:
        """Extract all redirects before leading syntax is stripped; only adjacent bare digits name an fd."""
        spans: list[tuple[int, int]] = []
        index = 0
        while index < len(self.words):
            value, raw, start, end = self.words[index]
            if raw != value or _REDIRECT_OPERATOR.fullmatch(raw) is None:
                index += 1
                continue
            fd = 0 if raw.startswith("<") else 1
            if index and self.words[index - 1][1].isdigit() and self.words[index - 1][3] == start:
                fd, start = int(self.words[index - 1][1]), self.words[index - 1][2]
            if index + 1 >= len(self.words) or self.words[index + 1][1] in {"<", ">", "(", ")", "<&", "<>"}:
                self.malformed = True
                target = ""
            else:
                index += 1
                target, _spelling, _start, end = self.words[index]
            self.redirects.append((fd, raw, target))
            self.offsets.append(start)
            # Here-strings still use _stdin_text; heredocs and process substitutions are handled by _lex.
            if raw != "<<<":
                spans.append((start, end))
            index += 1
        chars = list(self.text)
        for start, end in spans:
            chars[start:end] = " " * (end - start)
        return shlex.split("".join(chars), comments=False, posix=True)

    def tokens(self) -> list[str]:
        """Input argv has no redirects; preserve existing output-only classification and reason codes."""
        records = [*self.redirects, *(record for group in self.groups for record in group.redirects)]
        return self.argv if any(op in _INPUT_OPERATORS for _fd, op, _target in records) else self.legacy


class _RedirectGroup:
    """A compound's range and redirects, plus exec state scoped to that compound."""

    def __init__(self, closer: str, first: int) -> None:
        self.closer = closer
        self.first = first
        self.last: int | None = None
        self.redirects: list[_Redirect] = []
        self.exec_fds: dict[int, _FdSource] = {}


def _compound_redirects(commands: list[_RedirectCommand | None]) -> None:
    """Attribute closer redirects to every command in balanced braces, subshells, conditionals and loops."""
    stack: list[_RedirectGroup] = []
    openers = {"{": "}", "(": ")", "if": "fi", "for": "done", "while": "done", "until": "done", "case": "esac"}
    for index, entry in enumerate(commands):
        if entry is None:
            continue
        words = entry.words
        for position, (word, raw, _start, _end) in enumerate(words):
            if word != raw:
                continue
            # Parentheses in a function header and case patterns do not close a subshell.
            if word == "(" and position and position + 1 < len(words) and words[position + 1][0] == ")":
                continue
            if word == ")" and position and words[position - 1][0] == "(":
                continue
            if word == ")" and stack and stack[-1].closer == "esac":
                continue
            keyword = position == 0 or words[position - 1][0] in {"{", "(", "then", "do", "else"}
            if word in openers and (keyword or word == "("):
                group = _RedirectGroup(openers[word], index)
                stack.append(group)
            elif word in {"}", ")", "fi", "done", "esac"} and (keyword or word in {"}", ")"}):
                if not stack or stack[-1].closer != word:
                    entry.malformed = bool(entry.redirects)
                    continue
                group = stack.pop()
                group.last = index
                # The redirect following a compound closer belongs to the compound, not to its last argv.
                # Stop at the next closer: nested compounds on one segment can have distinct stdin sources.
                limit = next(
                    (
                        start
                        for _word, spelling, start, _stop in words[position + 1 :]
                        if spelling in {"}", ")", "fi", "done", "esac"}
                    ),
                    len(entry.text),
                )
                group.redirects = [
                    record
                    for offset, record in zip(entry.offsets, entry.redirects)
                    if _end <= offset < limit
                ]
                for member in commands[group.first : index + 1]:
                    if member is not None:
                        member.groups.insert(0, group)
                kept = [
                    (offset, record)
                    for offset, record in zip(entry.offsets, entry.redirects)
                    if not _end <= offset < limit
                ]
                entry.offsets = [offset for offset, _record in kept]
                entry.redirects = [record for _offset, record in kept]
    for group in stack:
        for member in commands[group.first :]:
            if member is not None:
                member.groups.insert(0, group)


def _resolve_redirects(fds: dict[int, _FdSource], records: list[_Redirect]) -> bool:
    """Apply records left to right; return whether any fd source cannot be resolved."""
    unknown = False
    for fd, operator, target in records:
        if operator in {"<", "<>"}:
            path = _canonical_path(target) if target else ""
            alias = _descriptor_fd(path)
            source = fds.get(alias, ("unknown", "")) if alias is not None else ("file", path)
            fds[fd] = ("file", source[1]) if source[0] == "output" else source
            if not target:
                fds[fd] = ("unknown", "")
        elif operator in {"<&", ">&"}:
            if target == "-":
                fds[fd] = ("closed", "")
            elif target.isdigit():
                fds[fd] = fds.get(int(target), ("unknown", ""))
            else:
                fds[fd] = ("unknown", "")
        elif operator == "<<<":
            fds[fd] = ("inline", target)
        elif operator.startswith(">") or operator.startswith("&>"):
            path = _canonical_path(target)
            alias = _descriptor_fd(path)
            source = fds.get(alias, ("unknown", "")) if alias is not None else ("file", path)
            fds[fd] = ("output", source[1]) if source[0] in {"file", "output"} else source
            if operator.startswith("&>"):
                fds[2] = fds[fd]
        unknown = unknown or fds.get(fd, ("unknown", ""))[0] == "unknown"
    return unknown


def _segment_fds(entry: _RedirectCommand | None, persistent: dict[int, _FdSource]) -> tuple[dict[int, _FdSource], bool]:
    """Descriptors in force during expansion, before the segment's own redirects apply."""
    fds = {fd: ("inherited", "") for fd in (0, 1, 2)} | persistent
    if entry is None:
        return fds, False
    unknown = entry.malformed
    for group in entry.groups:
        unknown = _resolve_redirects(fds, group.redirects) or unknown
        fds.update(group.exec_fds)
        unknown = unknown or (group.last is None and bool(entry.redirects or group.exec_fds))
    return fds, unknown


def _command_fds(entry: _RedirectCommand, persistent: dict[int, _FdSource]) -> tuple[dict[int, _FdSource], bool]:
    """Combine outer exec state, outer-to-inner compound records, and this command's records."""
    fds, unknown = _segment_fds(entry, persistent)
    unknown = _resolve_redirects(fds, entry.redirects) or unknown
    tokens = _strip_shell_group_tokens(entry.argv)
    command, _assignments, _understood, _stdin_args = _strip_prefixes(tokens)
    commandless_exec = not command or (
        command[:1] == ["exec"] and _skip_exec_options([*command, _SUBSTITUTION], 1) == len(command)
    )
    if commandless_exec and "exec" in tokens and entry.redirects:
        # exec without argv changes this list's descriptors; a subshell's changes stay inside its group.
        scope = entry.groups[-1].exec_fds if entry.groups else persistent
        scope.update(fds)
    return fds, unknown


def _has_shell_reader(entry: _RedirectCommand | None) -> bool:
    """Only a launched shell, source or dot command is in the produced-text protection model."""
    if entry is None:
        return False
    try:
        tokens = _strip_shell_group_tokens(entry.argv)
        command, _assignments, _understood, _stdin_args = _strip_prefixes(tokens)
    except ValueError:
        return False
    if command[:1] == ["builtin"]:
        command = command[2:] if command[1:2] == ["--"] else command[1:]
    return bool(command) and _command_name(command[0]) in _SHELLS | {"source", "."}


def _remember_file_production(scan: _Scan, entry: _RedirectCommand | None, fds: dict[int, _FdSource]) -> None:
    """Index one earlier command once; reads do not turn trusted input into a produced file."""
    if entry is None:
        return
    records = [*entry.redirects, *(record for group in entry.groups for record in group.redirects)]
    outputs = [target for _fd, op, target in records if op in _PRODUCING_OPERATORS and target]
    for fd, op, target in records:
        if op == ">&" and target.isdigit():
            kind, backing = fds.get(fd, ("unknown", ""))
            if kind in {"file", "output"} and backing:
                outputs.append(backing)
    try:
        command, _assignments, _clean, _args = _strip_prefixes(_strip_shell_group_tokens(entry.argv))
    except ValueError:
        command = []
    if command and _command_name(command[0]) in {"tee", "cp", "mv", "install", "touch"}:
        outputs.extend(word for word in command[1:] if not word.startswith("-"))
    produced = scan.file_production
    for target in outputs:
        path = _canonical_path(target)
        produced.add_path(path)
        alias = _descriptor_fd(path)
        if alias is not None:
            scan.written_aliases.add(alias)
            kind, backing = fds.get(alias, ("unknown", ""))
            if kind in {"file", "output"} and backing:
                produced.add_path(_canonical_path(backing))
    # Preserve ordinary argv/tee/body evidence, but never index input-only redirect targets.
    for value in [*entry.argv, *outputs, *entry.segment.bodies]:
        produced.add_word(_canonical_path(value))
        for word in _PRIOR_WORD_SPLIT.split(value):
            if word:
                produced.add_word(_canonical_path(word))
    if outputs and _raw_test_word(_segment_scan_text(entry.segment, None)) is not None:
        produced.protected_writer = True


def _fail_closed_redirect(scan: _Scan, via: str | None) -> bool:
    """An unattributable redirect refuses only when an earlier writer produced protected text."""
    if not scan.file_production.protected_writer:
        return False
    match = _raw_test_word("", prior=scan.prior)
    if match is None:
        return False
    word, target = match
    finding = (_UNPARSED_SHAPE, target, via, word)
    if finding not in scan.findings:
        scan.findings.append(finding)
    return True


def _shell_reads_stdin(tokens: list[str]) -> bool:
    """A shell reads stdin as a script only without a file operand, or with -s or a lone -."""
    command, _assignments, _clean, _args = _strip_prefixes(tokens)
    if not command or _command_name(command[0]) not in _SHELLS:
        return False
    if _shell_command(command) is not None:
        # The -c classifier decides whether the script consumes its supplied stdin.
        return True
    index = 1
    while index < len(command):
        word = command[index]
        redirect = re.fullmatch(r"\d*(<<<|<<-|<<|<>|<&|>>|>\||>&|&>>|&>|[<>])(.*)", word)
        if redirect is not None:
            index += 1 if redirect.group(2) else 2
            continue
        if word == "-" or (word.startswith("-") and not word.startswith("--") and "s" in word[1:]):
            return True
        if word == "--":
            return index + 1 == len(command)
        if not word.startswith(("-", "+")):
            return False
        index += 2 if word in _SHELL_VALUE_OPTIONS else 1
    return True


def _shell_script_index(command: list[str]) -> int | None:
    """Locate the script operand; later words are arguments to that script."""
    index = 1
    options_ended = False
    while index < len(command):
        word = command[index]
        redirect = re.fullmatch(r"\d*(<<<|<<-|<<|<>|<&|>>|>\||>&|&>>|&>|[<>])(.*)", word)
        if redirect is not None:
            index += 1 if redirect.group(2) else 2
            continue
        if options_ended:
            return index
        if word == "--":
            options_ended = True
            index += 1
            continue
        if word == "-":
            return None
        if not word.startswith(("-", "+")):
            return index
        if word.startswith("-") and not word.startswith("--") and "s" in word[1:]:
            return None
        index += 2 if word in _SHELL_VALUE_OPTIONS else 1
    return None


def _earlier_script_file(word: str, scan: _Scan, fds: dict[int, _FdSource]) -> bool:
    """Use one provenance decision for script operands and descriptor-fed stdin."""
    path = _canonical_path(word)
    if path == "/dev/null":
        return False
    fd = _descriptor_fd(path)
    produced = scan.file_production
    if fd is None:
        if _unproven_device_path(path):
            return bool(produced.paths)
        return _earlier_file(path, scan) or _may_alias_earlier_file(path, scan)
    if path in scan.written_files or fd in scan.written_aliases:
        return True
    kind, target = fds.get(fd, ("unknown", ""))
    if kind == "closed":
        return False
    if kind == "inline":
        return _may_expand(target)
    if kind not in {"file", "output"}:
        return bool(produced.paths)
    backing = _canonical_path(target)
    if backing == "/dev/null":
        return False
    if _unproven_device_path(backing):
        return bool(produced.paths)
    return backing in scan.written_files or produced.matches(backing) or produced.may_alias(backing)


def _runs_earlier_text(command: list[str], segment: _Segment, scan: _Scan, fds: dict[int, _FdSource]) -> bool:
    """True when the segment runs a file an earlier segment produced, or input piped from one."""
    if not command:
        return False
    if _canonical_path(command[0]) in scan.written_files:
        return True
    if "/" in command[0] and (_earlier_file(command[0], scan) or _may_alias_earlier_file(_canonical_path(command[0]), scan)):
        return True
    name = _command_name(command[0])
    if name == "ssh":
        _destination, options, _valid, _remote = _ssh_options(command)
        return any(option == "-F" and _earlier_file(value, scan) for option, value in options)
    if name == "builtin" and len(command) > 1:
        # builtin source s runs s like source s; in builtin -- source s, "--" ends builtin's options.
        command = command[2:] if command[1] == "--" else command[1:]
        if not command:
            return False
        name = _command_name(command[0])
    if name in _SHELLS and _shell_command(command) is None:
        if segment.upstream:
            return True
        if _may_expand(_stdin_text(segment)):
            # A here-string such as "$(cat s)" is stdin, not a script argument.
            return True
        index = _shell_script_index(command)
        return index is not None and _earlier_script_file(command[index], scan, fds)
    if name in {"source", "."} and len(command) > 1:
        # In source -- s and . -- s, "--" ends the options, so the operand is the word after it.
        operands = command[2:] if command[1:2] == ["--"] else command[1:]
        return bool(operands) and _earlier_script_file(operands[0], scan, fds)
    return False


def _may_expand(word: str) -> bool:
    """True when the shell may expand text in this word, so it is not provably literal.

    Quotes are not tracked, so a single-quoted ``$`` is refused as well. That
    fails safe, and the audited override covers a literal command name.
    """
    return "$" in word or "`" in word or _SUBSTITUTION in word


def _may_glob(word: str) -> bool:
    """True when pathname expansion may rewrite this word into another name."""
    return any(char in word for char in "*?[")


def _may_glob_into_runner(word: str) -> bool:
    """True when pathname expansion may rewrite this program name into a runner name.

    Only the basename is tested: a literal basename keeps its own rule, and a glob
    in a directory part leaves that rule in place.
    """
    name = _basename(word)
    return _may_glob(name) and any(fnmatch.fnmatchcase(runner, name) for runner in _RUNNER_NAMES)


def _unproven_command_word(command: list[str], understood: bool) -> str | None:
    """Return a command word that may expand or glob into a runner at run time, or None.

    Once every prefix is understood only the first word names the program. A
    launcher the parser does not model may pass any later word to a program,
    so each of its words is checked.
    """
    if not command or command[0] in _MAKE_CALL_WORDS:
        return None
    launched = command if not understood and _command_name(command[0]) in _LAUNCHERS else command[:1]
    word = next((item for item in launched if _may_expand(item) or _may_glob_into_runner(item)), None)
    return None if word is None else word.replace(_SUBSTITUTION, "$(...)")


def _is_make_word(command: list[str]) -> bool:
    return _basename(command[0]) in {"make", "gmake"} or command[0] in _MAKE_CALL_WORDS


def _is_proven_inert(command: list[str], stdin_args: bool, clean: bool, scan: _Scan | None = None) -> bool:
    """True when the command never executes its arguments."""
    # After xargs only unconditionally inert commands stay inert: stdin words
    # could add an executing option the argv does not show.
    return clean and _is_inert(command, scan) and (not stdin_args or _command_name(command[0]) in _INERT_COMMANDS)


def _remote_dispatch_text(command: list[str], scan: _Scan) -> str | None:
    """The tracked dispatcher's build/submit test command is data for the VM."""
    script_index = 0
    if _command_name(command[0]) == "bash":
        operand = _shell_script_index(command)
        if operand is None:
            return None
        script_index = operand
    script = command[script_index]
    if script != "scripts/remote_agent.sh" or _earlier_file(script, scan):
        return None
    if command[script_index + 1 : script_index + 2] not in (["build"], ["submit"]):
        return None
    filtered = command[: script_index + 2]
    index = script_index + 2
    while index < len(command):
        word = command[index]
        if word == "--test-cmd":
            if index + 1 >= len(command):
                return None
            index += 2
        elif word.startswith("--test-cmd="):
            index += 1
        else:
            filtered.append(word)
            index += 1
    return " ".join(filtered)


def _reads_startup_file(command: list[str], assignments: dict[str, str], scan: _Scan) -> bool:
    """True when a shell reads a startup file, which runs before its script and may hold produced text."""
    if not command or _command_name(command[0]) not in _SHELLS:
        return False
    return (
        any(key in {"BASH_ENV", "ENV", "ZDOTDIR"} for key in assignments)
        # An export or earlier assignment can name the file the shell reads, as in export BASH_ENV=s; bash -c ...
        or scan.prior.matches(_STARTUP_ENV)
        or any(word in _STARTUP_FILE_OPTIONS for word in command[1:])
    )


def _local_ssh_command(command: list[str], scan: _Scan) -> str | None:
    """The remote command text of an ssh that may run on this host, else None.

    A loopback destination, or one the hook cannot prove remote, runs the command in a local shell. A
    non-loopback ssh stays inert by design, and options the parser does not model leave no command to scan.
    """
    if _command_name(command[0]) != "ssh" or _ssh_is_remote(command, scan):
        return None
    _destination, _options, valid, remote = _ssh_options(command)
    return " ".join(remote) if valid and remote else None


def _classify_shell_text(
    segment: _Segment,
    tokens: list[str],
    command: list[str],
    assignments: dict[str, str],
    depth: int,
    via: str | None,
    scan: _Scan,
    *,
    fds: dict[int, _FdSource],
) -> bool:
    """Scan the script of a ``bash -c`` style command; False when it is not one.

    The remote command of an ssh that may run on this host is such a script: sshd hands it to a shell. It
    is handled only when its scan finds something. Otherwise the ssh line is still read as unknown text.
    """
    shell_command = _shell_command(command)
    ssh_text = _local_ssh_command(command, scan) if shell_command is None else None
    local_ssh = ssh_text is not None
    if local_ssh:
        shell_command = (ssh_text or "", [])
    if shell_command is None:
        return False
    shell_text, shell_args = shell_command
    if depth >= _MAX_SHELL_DEPTH or _SUBSTITUTION in shell_text:
        # The script is not scanned, so it may run stored text. Do not recurse further.
        _mark_text_executor(scan, via)
        return False
    stdin = _stdin_text(segment).replace(_PRIOR_STDIN, scan.prior_text)
    nested = _scan_command(
        shell_text, depth + 1, stdin=stdin, parent=scan,
        stdin_unprovable=segment.make_stdin_unprovable or bool(segment.upstream or segment.bodies),
        inherited_fds=dict(fds),
    )
    scan.findings.extend(
        (reason, target, nested_via or via, word) for reason, target, nested_via, word in nested.findings
    )
    scan.parse_errors.extend(nested.parse_errors)
    if nested.text_executor_unbypassed:
        _mark_text_executor(scan, via)
    elif nested.text_executor:
        _mark_text_executor(scan, nested.text_executor_via or via)
    if local_ssh:
        return bool(nested.findings)
    if shell_args or assignments:
        # A script can execute positional arguments through eval,
        # shift or set even without a visible parameter expansion. A prefix
        # assignment can likewise hold the text that eval runs.
        _record_unknown(scan, _segment_scan_text(segment, tokens), via, after_prior=_reads_prior(segment))
    return True


def _classify_make_call(
    segment: _Segment, command: list[str], stdin_args: bool, clean: bool, via: str | None, scan: _Scan
) -> bool:
    """Deny every make call unless one parse proves its explicit goals inert."""
    argv = _MakeArgv(command)

    def refuse(reason: str, word: str) -> bool:
        target = word if word in _TEST_TARGETS else "test-files"
        scan.findings.append((reason, target, via, word.replace(_SUBSTITUTION, "$(...)")))
        return True

    if argv.unparsed is not None or stdin_args or not clean:
        return refuse("make_argv_unparsed", argv.unparsed or "make")
    directory = ""
    for value in argv.directories:
        directory = os.path.normpath(os.path.join(directory, value))
    implicit_files = [os.path.join(directory, name) for name in ("GNUmakefile", "makefile", "Makefile")]
    if (
        argv.makefiles or argv.eval or argv.source_env
        or scan.prior.matches(_MAKE_SOURCE_ENV)
        or segment.make_stdin_unprovable or segment.upstream or segment.bodies
        or "<<<" in "".join(segment.raw)
        or any(_earlier_file(value, scan) for value in implicit_files)
        or (_command_name(command[0]) is None and _earlier_file(command[0], scan))
        or _canonical_path(command[0]) in scan.written_files
        # An earlier cd can change how a produced relative path resolves. If
        # any implicit-source name was produced, its provenance is unproven.
        or scan.produced_names.may_hold_makefile()
    ):
        return refuse("make_makefile_unprovable", "make")
    expanding = next((goal for goal in argv.goals if _may_expand(goal) or _may_glob(goal)), None)
    if expanding is not None:
        return refuse("make_goal_not_allowlisted", expanding)
    # An allowlisted goal's argument text can still carry a make call, and make runs the default goal
    # of a call that names none.
    test_word = _raw_test_word(" ".join(command[1:]), include_default_make=True)
    if test_word is not None:
        return refuse("make_makefile_unprovable", test_word[0])
    if argv.dry_run:
        # Preserves the existing dry-run controls, including check-hooks/test.
        # No alternate source, fed input or parse-time runner text is allowed.
        return True
    if not argv.goals:
        return refuse("make_goal_missing", "make")
    blocked = [goal for goal in argv.goals if goal not in _INERT_MAKE_GOALS]
    for goal in blocked:
        refuse("make_goal_not_allowlisted", goal)
    return True


def _classify_segment(
    segment: _Segment, tokens: list[str], depth: int, exported: bool, scan: _Scan,
    *, redirect_argv: list[str] | None = None, fds: dict[int, _FdSource] | None = None
) -> None:
    """Decide ``test`` / ``inert`` / ``unknown`` for one parsed segment."""
    if fds is None:
        fds = {}
    try:
        command, assignments, understood, stdin_args = _strip_prefixes(tokens)
    except ValueError as exc:  # an env -S string shlex cannot split
        # No assignments were parsed, so only the export state can apply.
        via = "export" if exported else None
        _record_unknown(
            scan, _segment_scan_text(segment, tokens), via, parse_error=f"env -S: {exc}", include_default_make=True,
            after_prior=_reads_prior(segment),
        )
        return
    via = "inline" if assignments.get(_BYPASS_ENV) == "1" else ("export" if exported else None)
    if understood and command and _runs_host_tests(command):
        scan.findings.append(("test", "test-files", via, _basename(command[0])))
        return
    expanding = _unproven_command_word(command, understood)
    if expanding is not None:
        scan.findings.append((_UNPARSED_SHAPE, "test-files", via, expanding))
        return
    if understood and command:
        if _reads_startup_file(command, assignments, scan):
            _record_unknown(
                scan, _segment_scan_text(segment, tokens), via, include_default_make=True, after_prior=True
            )
        if _classify_shell_text(segment, tokens, command, assignments, depth, via, scan, fds=fds):
            return
        # A prefix assignment reaches make's recipes like a command-line one: its text can carry a make call.
        make_call = _is_make_word(command)
        clean = not segment.opaque and not any(
            _raw_test_word(value, include_default_make=make_call) for value in assignments.values()
        )
        dispatch_text = _remote_dispatch_text(command, scan) if clean and not stdin_args else None
        if dispatch_text is not None:
            # Substitutions were scanned independently; all non-test-cmd argv
            # and any stdin remain on the ordinary fail-safe raw-text path.
            _record_unknown(scan, "\n".join((dispatch_text, _stdin_text(segment))), via, after_prior=_reads_prior(segment))
            return
        if make_call:
            if _MAKE_SOURCE_ENV.search(" ".join(assignments)) is not None:
                scan.findings.append(("make_makefile_unprovable", "test-files", via, "make"))
                return
            if _classify_make_call(segment, command, stdin_args, clean, via, scan):
                return
        elif _is_proven_inert(command, stdin_args, clean, scan):
            return
    name = _command_name(command[0]) if command else None
    if name in _TEXT_EXECUTORS and (name == "alias" or _may_expand(_segment_scan_text(segment, tokens))):
        _mark_text_executor(scan, via)
    text = _segment_scan_text(segment, tokens)
    runs_earlier_text = _runs_earlier_text(command, segment, scan, fds)
    # A stored make name is data until a text executor runs it. Keep the
    # default-goal fallback for actual/unknown launches, not name-only stores.
    include_default_make = bool(command) and name not in {"export", "declare", "typeset", "local", "readonly", "printf", "read"}
    deny_unlisted_make = not understood or runs_earlier_text or name in _SHELLS | _TEXT_EXECUTORS
    if not deny_unlisted_make and include_default_make:
        # Unknown launchers can run a literal make argv. Prose in other argv
        # words is data; it cannot authorize an actual make call. Redirects
        # have already been retained as stdin evidence and are not goals.
        # Name-only stores do not launch their make word; an eventual text
        # executor receives the existing single stored-word finding instead.
        make_command = command
        make_clean = understood and not segment.opaque and not any(_raw_test_word(value) for value in assignments.values())
        if redirect_argv is not None:
            make_command, _assignments, _understood, _stdin_args = _strip_prefixes(
                _strip_shell_group_tokens(redirect_argv)
            )
        index = next((i for i in range(1, len(make_command)) if _is_make_word([make_command[i]])), None)
        if index is not None:
            # Words before make include the launcher's own assignments, as in flock l env MAKEFILES=x make.
            if _MAKE_SOURCE_ENV.search(" ".join([*assignments, *make_command[:index]])) is not None:
                scan.findings.append(("make_makefile_unprovable", "test-files", via, "make"))
            else:
                _classify_make_call(segment, make_command[index:], stdin_args, make_clean, via, scan)
    _record_unknown(
        scan, text, via, include_default_make=include_default_make, deny_unlisted_make=deny_unlisted_make,
        after_prior=runs_earlier_text or _reads_prior(segment),
    )


def _scan_segments(
    segments: list[_Segment], depth: int, scan: _Scan, exported: bool = False,
    *, inherited_fds: dict[int, _FdSource] | None = None,
) -> None:
    """Classify segments with redirect data preserved before stripping syntax and prefixes."""
    entries: list[_RedirectCommand | None] = []
    errors: dict[int, str] = {}
    for index, segment in enumerate(segments):
        try:
            entries.append(_RedirectCommand(segment))
        except ValueError as exc:
            entries.append(None)
            errors[index] = str(exc)
    _compound_redirects(entries)
    has_reader = any(_has_shell_reader(entry) for entry in entries)
    persistent: dict[int, _FdSource] = dict(inherited_fds or {})
    outer_prior = scan.prior.mark()
    outer_words = scan.produced_words.copy()
    outer_names = _Names(scan.produced_names)
    outer_files = scan.file_production
    outer_aliases = scan.written_aliases.copy()
    scan.file_production = _FileProduction(outer_files)
    previous_fds: dict[int, _FdSource] = {}
    for index, (segment, entry) in enumerate(zip(segments, entries)):
        if index:
            scan.add_produced_text(_segment_scan_text(segments[index - 1], None))
            _remember_file_production(scan, entries[index - 1], previous_fds)
        sub_fds, _unknown = _segment_fds(entry, persistent)
        for inner in segment.substitutions:
            _scan_segments(inner, depth, scan, exported, inherited_fds=dict(sub_fds))
        previous_fds = {}
        if entry is None:
            via = "export" if exported else None
            _record_unknown(scan, _segment_scan_text(segment, None), via, parse_error=errors[index], include_default_make=True)
            continue
        tokens = _strip_shell_group_tokens(entry.tokens())
        export_state = _export_bypass_value(tokens)
        if export_state is not None:
            exported = export_state
        try:
            fds, unknown = _command_fds(entry, persistent)
            previous_fds = fds
            _command, assignments, _clean, _args = _strip_prefixes(tokens)
        except ValueError:
            # The existing segment classifier reports env -S errors and refuses protected text.
            _classify_segment(segment, tokens, depth, exported, scan, redirect_argv=entry.argv)
            continue
        via = "inline" if assignments.get(_BYPASS_ENV) == "1" else ("export" if exported else None)
        if unknown and has_reader and _fail_closed_redirect(scan, via):
            continue
        if not tokens:
            _record_unknown(scan, _segment_scan_text(segment, None), via)
            continue
        upstream = segment.upstream
        segment.make_stdin_unprovable = segment.make_stdin_unprovable or fds.get(0, ("inherited", ""))[0] != "inherited"
        if _command and _is_make_word(_command):
            # Output redirections are shell syntax, never make goals. Input
            # redirections have already been retained in the fd evidence.
            tokens = _strip_shell_group_tokens(entry.argv)
        if fds.get(0, ("inherited", ""))[0] == "file":
            # An explicit file redirect replaces pipeline or inherited input.
            segment.upstream = []
        if (
            _shell_reads_stdin(tokens)
            and fds.get(0, ("inherited", ""))[0] != "inherited"
            and _earlier_script_file("/dev/stdin", scan, fds)
        ):
            # Feed produced stdin through the existing shell-text classifier, including nested -c scripts.
            inherited = _Segment()
            inherited.from_prior = True
            inherited.raw.append(_PRIOR_STDIN)
            segment.upstream = [inherited]
        try:
            _classify_segment(segment, tokens, depth, exported, scan, redirect_argv=entry.argv, fds=fds)
        finally:
            segment.upstream = upstream
    scan.prior.rewind(outer_prior)
    scan.produced_words = outer_words
    scan.produced_names = outer_names
    scan.file_production = outer_files
    scan.written_aliases = outer_aliases


def _scan_command(
    command: str, depth: int = 0, *, stdin: str = "", parent: _Scan | None = None, stdin_unprovable: bool = False,
    inherited_fds: dict[int, _FdSource] | None = None,
) -> _Scan:
    """Scan shell text; ``stdin`` is data a ``-c`` script may read and run."""
    scan = _Scan()
    if len(command.encode("utf-8")) + len(stdin.encode("utf-8")) > _MAX_COMMAND_BYTES:
        # The bounded fallback deliberately proves no command-text override.
        # Ambient overrides still use the mandatory receipt path.
        # Default make goals must remain refused when structural parsing is skipped.
        word = _raw_test_word(command + "\n" + stdin if stdin else command, include_default_make=True)
        if word is not None:
            scan.findings.append((_UNPARSED_SHAPE, word[1], None, word[0]))
        return scan
    scan.written_files = _redirect_targets(command)
    scan.written_aliases = {fd for path in scan.written_files if (fd := _descriptor_fd(path)) is not None}
    if parent is not None:
        # A nested script can run what the outer command wrote or printed before it.
        scan.written_files |= parent.written_files
        # Shared, not copied: the nested scan rewinds it to this point when it ends, and the facts it folds
        # stay available to the outer scan.
        scan.prior = parent.prior
        scan.produced_words = parent.produced_words.copy()
        scan.produced_names = _Names(parent.produced_names)
        scan.written_aliases |= parent.written_aliases
        scan.file_production = parent.file_production
    for path in scan.written_files:
        scan.produced_names.add(path)
    try:
        segments, _ = _lex(command)
    except ValueError as exc:
        _record_unknown(scan, "\n".join((command, stdin)), None, parse_error=str(exc), include_default_make=True)
        return scan
    if stdin:
        inherited = _Segment()
        inherited.raw.append(stdin)
        for segment in segments:
            segment.upstream = [inherited, *segment.upstream]
    if stdin_unprovable:
        for segment in segments:
            segment.make_stdin_unprovable = True
    _scan_segments(segments, depth, scan, inherited_fds=inherited_fds)
    # An unbypassed executor refuses even after a bypassed finding, but never beside an unbypassed refusal.
    refusal_missing = not scan.findings or (
        scan.text_executor_unbypassed and all(_via is not None for _reason, _target, _via, _word in scan.findings)
    )
    if depth == 0 and scan.text_executor and refusal_missing:
        word = _stored_runner_word(command)
        if word is not None:
            via = None if scan.text_executor_unbypassed else scan.text_executor_via
            scan.findings.append((_UNPARSED_SHAPE, "test-files", via, word))
    return scan


def _log_bypass(*, command: str, via: str) -> str | None:
    """Append the required receipt; return an error instead of bypassing silently."""
    record = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "cwd": os.getcwd(),
        "command": command,
        "via": via,
    }
    try:
        state_dir = _repo_root() / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / _AUDIT_FILE).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    except Exception as exc:  # a failed receipt must never authorize the command
        return f"could not append bypass audit receipt: {exc}"
    return None


def _blocked_message(targets: set[str]) -> str:
    refused_targets = " ".join(sorted(targets)) if targets else "test-files"
    return (
        "BLOCKED: host Python test execution is refused under execution_mode=remote_only.\n"
        "Enforced: pytest, py.test, run_named_tests.py; make is deny-by-default.\n"
        "Not inspected: JS runners, unittest, tox, nox, hatch, and scripts invoked by path.\n\n"
        f"Allowlisted make goals: {', '.join(sorted(_INERT_MAKE_GOALS))}.\n\n"
        "WorkBay monorepo only (from its root, with a configured remote gate):\n"
        f'  make check-remote TARGETS="{refused_targets}"\n'
        '  WORKBAY_REMOTE_GATE_ENV="WORKBAY_TEST_FILES=<files>" '
        "make check-remote TARGETS=test-files\n"
        "\nInstalled consumer repositories:\n"
        "Bootstrap provides no standalone consumer remote test-gate command and does not install "
        "the monorepo targets or their runner scripts.\n"
        "Ask the repository operator to configure a repository-specific remote test runner and suite command "
        "for this repository's checkout on the remote host; then use that command.\n"
        "Setting WORKBAY_REMOTE_GATE_HOST alone does not install that runner.\n"
        "For lane-owned remote work, /wb-offload requires a configured supported remote backend "
        "and declared lane verification commands.\n\n"
        f"Operator override: set or export {_BYPASS_ENV}=1 for this command. "
        f"Every override is recorded in .task-state/{_AUDIT_FILE}."
    )


def _refusal_message(findings: list[_Finding]) -> str:
    targets = {target for _reason, target, _via, _word in findings}
    for reason, prefix in (
        ("make_argv_unparsed", "the hook could not parse the whole make argv"),
        ("make_makefile_unprovable", "makefile source or parse-time text is unprovable"),
        ("make_goal_missing", "make requires an explicit inert goal; the default goal may run tests"),
        ("make_goal_not_allowlisted", "make goal has no inert recipe-closure evidence"),
        ("unparseable", "command could not be parsed"),
        (_UNPARSED_SHAPE, "the hook could not prove this command shape is not a host test run"),
    ):
        word = next((word for kind, _target, _via, word in findings if kind == reason), None)
        if word is not None:
            if reason.startswith("make_"):
                return (
                    f"BLOCKED ({reason}): {prefix}; refused word '{word}'. "
                    f"Use an explicit allowlisted goal ({', '.join(sorted(_INERT_MAKE_GOALS))}) or the audited override.\n\n"
                    f"{_blocked_message(targets)}"
                )
            return (
                f"BLOCKED ({_UNPARSED_SHAPE}): {prefix}; found test-run word '{word}'. "
                f"If this is not a test run, retry once with the audited override {_BYPASS_ENV}=1.\n\n"
                "For selector/spec data, cat > file <<'EOF' writes data without running it.\n\n"
                f"{_blocked_message(targets)}"
            )
    return _blocked_message(targets)


def _diagnostic(message: str) -> int:
    print(message, file=sys.stderr)
    return 0


def _policy_refusal(findings: list[_Finding], ledger_error: str | None) -> str:
    message = _refusal_message(findings)
    if ledger_error is not None:
        return f"BLOCKED (ledger_unreadable): {ledger_error}. Test routing cannot opt out on a faulty ledger.\n\n{message}"
    return message


def _apply_findings(command: str, findings: list[_Finding], ledger_error: str | None) -> int:
    """Both normal and exception paths require a receipt before any bypass."""
    if _is_remote_gate_context(_repo_root()):
        # A real remote lane suppresses a finding only after scanning it.
        # Its receipt is best-effort; the workstation override remains strict.
        audit_error = _log_bypass(command=command, via="lane_sandbox")
        if audit_error is not None:
            print(f"ALLOW: lane_sandbox; {audit_error}", file=sys.stderr)
        return 0
    ambient_bypass = os.environ.get(_BYPASS_ENV) == "1"
    unbypassed = [finding for finding in findings if finding[2] is None]
    message = _policy_refusal(findings, ledger_error)
    if unbypassed and not ambient_bypass:
        print(message, file=sys.stderr)
        return 2
    via = "env" if ambient_bypass else (findings[0][2] or "")
    if not via:
        print(message, file=sys.stderr)
        return 2
    audit_error = _log_bypass(command=command, via=via)
    if audit_error is not None:
        print(f"BLOCKED: {audit_error}.\n\n{message}", file=sys.stderr)
        return 2
    return 0


def _payload_command(payload: dict) -> tuple[bool, str | None]:
    """Normalize the tool aliases and argument envelopes used by harnesses."""
    tool = str(payload.get("tool_name") or payload.get("toolName") or payload.get("tool") or "")
    tool_input = payload.get("tool_input") or payload.get("toolInput") or payload.get("input")
    if not tool_input:
        tool_input = payload.get("toolArgs")
        if isinstance(tool_input, str):
            try:
                tool_input = json.loads(tool_input)
            except (ValueError, RecursionError):
                tool_input = None
    command = tool_input.get("command") if isinstance(tool_input, dict) else payload.get("command")
    command = command if isinstance(command, str) else None
    return tool.lower() in {"bash", "shell", "run_in_terminal"} or (not tool and command is not None), command


def main(raw_input: str | None = None) -> int:
    try:
        payload = json.load(sys.stdin) if raw_input is None else json.loads(raw_input)
    except (json.JSONDecodeError, ValueError, RecursionError) as exc:
        return _diagnostic(f"ALLOW: malformed hook JSON: {exc}")
    if not isinstance(payload, dict):
        return _diagnostic("ALLOW: malformed hook payload: expected a JSON object")
    mediated, command = _payload_command(payload)
    if not mediated:
        return 0
    if command is None:
        return _diagnostic("ALLOW: malformed Bash hook payload: command must be a string")
    root = _repo_root()
    mode, ledger_error = _execution_policy(root)
    if mode not in {"remote_only", "ledger_unreadable"}:
        return 0

    scan = _scan_command(command)
    if not scan.findings:
        if ledger_error is not None:
            return _diagnostic(f"ALLOW: ledger_unreadable: {ledger_error}; no host test finding")
        if scan.parse_errors:
            return _diagnostic(f"ALLOW: unparseable command, no test-run word found: {scan.parse_errors[0]}")
        return 0

    return _apply_findings(command, scan.findings, ledger_error)


def _exception_bypass(command: str) -> str | None:
    """Prove only simple prefix overrides when the full scanner has failed.

    Complex shell shapes stay refused; a later or revoked override must not
    authorize another segment. Ambient overrides use the shared audit path.
    """
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    tokens = list(lexer)
    if tokens[:2] == ["export", f"{_BYPASS_ENV}=1"] and tokens[2:3] == [";"]:
        tokens = tokens[3:]
        via = "export"
    else:
        via = "inline"
    if any(token and all(char in "();<>|&" for char in token) for token in tokens):
        return None
    _command, assignments, understood, _stdin_args = _strip_prefixes(tokens)
    if not understood or assignments.get(_BYPASS_ENV) not in {None, "1"}:
        return None
    return via if via == "export" or assignments.get(_BYPASS_ENV) == "1" else None


def _run() -> int:
    """Degrade loudly, with one bounded, independent look at a failed command."""
    raw_input = ""
    try:
        raw_input = sys.stdin.read()
        return main(raw_input)
    except Exception as exc:  # noqa: BLE001 - one hook bug must not wedge all shell commands.
        error = f"hook error ({type(exc).__name__}): {exc}"
        command: str | None = None
        word: tuple[str, str] | None = None
        try:
            payload = json.loads(raw_input)
            mediated, command = _payload_command(payload) if isinstance(payload, dict) else (False, None)
            if mediated and command is not None:
                word = _raw_test_word(command, include_default_make=True)
                try:
                    mode, ledger_error = _execution_policy(_repo_root())
                except Exception as policy_exc:
                    mode = "ledger_unreadable"
                    ledger_error = f"policy read failed ({type(policy_exc).__name__}): {policy_exc}"
                if mode in {"remote_only", "ledger_unreadable"} and word is not None:
                    print(f"ERROR: {error}; checking raw test-run text", file=sys.stderr)
                    # Failure to prove an inline override refuses that override,
                    # rather than dropping into the second-look fail-open arm.
                    try:
                        via = _exception_bypass(command)
                    except Exception:
                        via = None
                    return _apply_findings(command, [(_UNPARSED_SHAPE, word[1], via, word[0])], ledger_error)
            return _diagnostic(f"ALLOW: {error}")
        except Exception as second_exc:  # noqa: BLE001 - the recovery path is bounded too.
            if command is not None and word is not None:
                print(
                    f"BLOCKED (ledger_unreadable): {error}; second look failed "
                    f"({type(second_exc).__name__}): {second_exc}\n\n"
                    + _refusal_message([(_UNPARSED_SHAPE, word[1], None, word[0])]),
                    file=sys.stderr,
                )
                return 2
            return _diagnostic(f"ALLOW: {error}; second look failed ({type(second_exc).__name__}): {second_exc}")


if __name__ == "__main__":
    sys.exit(_run())
