"""Single-owner arbitration and representation for the lane test command.

The lane row is the owner. Callers classify and resolve through this module
so the daemon apply site, the adapter dispatch, and the offload construction
site cannot drift. This module only arbitrates already-supplied values; it does
not derive test coverage from a worktree diff or prove that a command collects
changed tests.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import sqlite3
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

LANE_ROW_TEST_CMD_READ_FAILED = "lane_row_test_cmd_read_failed"
TEST_CMD_DISCARDED_WARNING = "test_cmd_discarded"
TEST_CMD_NOT_SUPPLIED: object = object()

_TEST_CMD_HELD = "held"
_TEST_CMD_CLEARED = "cleared"
_TEST_CMD_ABSENT = "not_supplied"
_TEST_CMD_DIGEST_LENGTH = 12


def _command_fingerprint(text: str) -> tuple[int, str]:
    """Return non-reversible diagnostics for command arbitration warnings."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:_TEST_CMD_DIGEST_LENGTH]
    return len(text), digest


def lane_row_test_cmd_read_failure(
    *, task_ref: str, lane_id: str, error: object, cause: str | None = None
) -> dict[str, Any]:
    """Build the typed, non-executable result of a failed lane-row read."""
    if cause is None:
        if isinstance(error, json.JSONDecodeError):
            cause = "malformed_json"
        elif isinstance(error, ImportError):
            cause = "import_error"
        elif isinstance(error, sqlite3.OperationalError):
            lowered = str(error).lower()
            if "no such table" in lowered:
                cause = "missing_table"
            elif "locked" in lowered or "busy" in lowered:
                cause = "database_locked"
            else:
                cause = "operational_error"
        else:
            cause = "row_read_error"
    return {
        "type": LANE_ROW_TEST_CMD_READ_FAILED,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "cause": cause,
        "error": str(error),
    }


def classify_test_cmd(value: Any) -> tuple[str, str | None]:
    """Return ``(state, text)`` for a test-command carrier or row value.

    States:
    - ``not_supplied``: absent / NULL / the explicit sentinel
    - ``cleared``: an empty or whitespace-only string
    - ``held``: a non-empty command string

    Carriers are a string protocol. Reject other values instead of coercing
    booleans, numbers, byte strings, or containers into executable text.
    """
    if value is TEST_CMD_NOT_SUPPLIED or value is None:
        return _TEST_CMD_ABSENT, None
    if isinstance(value, str):
        text = value.strip()
        return (_TEST_CMD_HELD, text) if text else (_TEST_CMD_CLEARED, "")
    raise TypeError(f"test_cmd must be a string or None, got {type(value).__name__}")


_REMOTE_TEST_CMD_NOOP_BASENAMES = frozenset({"true", ":", "echo", "false"})
_REMOTE_TEST_CMD_CONNECTORS = frozenset({"&&", "||", ";", "|", "&"})
_REMOTE_TEST_CMD_SHELL_BASENAMES = frozenset(
    {"bash", "csh", "dash", "fish", "ksh", "powershell", "pwsh", "sh", "shell", "tcsh", "zsh"}
)
_REMOTE_TEST_CMD_PREFIX_WRAPPER_BASENAMES = frozenset(
    {
        "env",
        "command",
        "exec",
        "eval",
        "builtin",
        "nice",
        "nohup",
        "timeout",
        "sudo",
        "xargs",
    }
)
_DEFAULT_REMOTE_TEST_CMD_RUNNER_COMMANDS = frozenset({"pytest"})
_DEFAULT_REMOTE_TEST_CMD_RUNNER_MODULES = frozenset({"pytest"})
_REMOTE_TEST_CMD_INTERPRETER_BASENAMES = frozenset({"python", "python3"})
_REMOTE_TEST_CMD_RESERVED_INTERPRETER_BASENAMES = _REMOTE_TEST_CMD_INTERPRETER_BASENAMES | frozenset(
    {
        "bun",
        "composer",
        "deno",
        "java",
        "node",
        "nodejs",
        "perl",
        "php",
        "pnpm",
        "ruby",
        "yarn",
    }
)
_REMOTE_TEST_CAPABILITY_FILE = "pyproject.toml"
_REMOTE_TEST_CAPABILITY_TABLE = "[tool.workbay.remote_test]"
_REMOTE_TEST_RUNNER_COMMANDS_KEY = "runner_commands"
_REMOTE_TEST_RUNNER_SELECTING_WRAPPERS_KEY = "runner_selecting_wrappers"
# ``composer`` and ``php`` are deliberately not runner-selecting wrappers:
# both can launch arbitrary repository scripts, unlike the reviewed ``npx``
# and ``uv run`` forms below. PHP lanes should declare ``phpunit`` directly.
_SUPPORTED_RUNNER_SELECTING_WRAPPERS = frozenset({"npx", "uv"})
_RUNNER_COMMAND_BASENAME_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._+-]*\Z")
_MAX_PATH_CANDIDATE_WALK_DEPTH = 8
_MAX_WRAPPER_PEEL_DEPTH = 8
_TIMEOUT_DURATION_RE = re.compile(r"\A\d+(?:\.\d+)?[smhd]?\Z")
_ENV_VALUE_OPTIONS = frozenset(
    {
        "-C",
        "--chdir",
        "-u",
        "--unset",
        "-a",
        "--argv0",
    }
)
_SUDO_VALUE_OPTIONS = frozenset(
    {
        "-u",
        "--user",
        "-g",
        "--group",
        "-h",
        "--host",
        "-p",
        "--prompt",
        "-C",
        "--close-from",
        "-D",
        "--chdir",
        "-R",
        "--chroot",
        "-T",
        "--command-timeout",
        "-U",
        "--other-user",
    }
)
_XARGS_VALUE_OPTIONS = frozenset(
    {
        "-a",
        "--arg-file",
        "-d",
        "--delimiter",
        "-E",
        "-e",
        "-I",
        "-i",
        "-L",
        "-l",
        "-n",
        "--max-args",
        "-P",
        "--max-procs",
        "-s",
        "--max-chars",
    }
)
_UV_RUN_VALUE_OPTIONS = frozenset(
    {
        "--directory",
        "--env-file",
        "--extra",
        "--find-links",
        "--index",
        "--index-url",
        "--python",
        "--with",
        "--with-editable",
        "--with-requirements",
    }
)
_UV_RUN_SOURCE_REDIRECT_VALUE_OPTIONS = frozenset({"--find-links", "--index", "--index-url"})
_UV_RUN_PACKAGE_VALUE_OPTIONS = frozenset({"--with", "--with-editable"})
_UV_RUN_CONTAINED_PATH_VALUE_OPTIONS = frozenset({"--directory", "--env-file", "--python", "--with-requirements"})
_UV_RUN_BOOLEAN_OPTIONS = frozenset(
    {
        "--all-extras",
        "--exact",
        "--frozen",
        "--inexact",
        "--isolated",
        "--locked",
        "--native-tls",
        "--no-cache",
        "--no-default-groups",
        "--no-dev",
        "--no-editable",
        "--no-project",
        "--no-sources",
        "--no-sync",
        "--offline",
        "--preview",
        "--quiet",
        "--verbose",
        "-q",
        "-v",
    }
)
_NPX_VALUE_OPTIONS = frozenset(
    {
        "--package",
        "-p",
        "--registry",
        "--userconfig",
    }
)
_NPX_SOURCE_REDIRECT_VALUE_OPTIONS = frozenset({"--registry", "--userconfig"})
_NPX_PACKAGE_VALUE_OPTIONS = frozenset({"--package", "-p"})
_NPX_BOOLEAN_OPTIONS = frozenset({"--no", "--quiet", "--yes", "-q", "-y"})
_NPM_VALUE_OPTIONS = frozenset(
    {
        "--loglevel",
        "--prefix",
        "--workspace",
        "-w",
    }
)
_NPM_BOOLEAN_OPTIONS = frozenset(
    {
        "--include-workspace-root",
        "--silent",
        "--workspaces",
        "-s",
    }
)
_UV_RUN_TERMINAL_OPTIONS = frozenset({"--help", "--version", "-V", "-h"})
_NPX_TERMINAL_OPTIONS = frozenset({"--help", "--version", "-?", "-h", "-v"})
_NPM_TERMINAL_OPTIONS = frozenset({"--help", "--version", "--versions", "-?", "-h", "-v"})
# Sandbox-relative allowlist: tokens that do not start with '/' or '~'.
# Absolute interpreters such as /usr/bin/python3 are refused unless added
# here; remote commands must use a PATH-resolved interpreter (python3, pytest).
_ALLOWED_ABSOLUTE_INTERPRETERS: frozenset[str] = frozenset()


@dataclass(frozen=True)
class _RemoteTestRunnerCapability:
    """Repository-declared executable names admitted by the remote gate.

    Direct runner commands and runner-selecting wrappers are deliberately
    separate. Declaring ``vitest`` must not silently admit ``node`` (or a
    shell), while declaring ``uv``/``npx`` as a selector still requires the
    nested command to be an approved runner.
    """

    runner_commands: frozenset[str] = _DEFAULT_REMOTE_TEST_CMD_RUNNER_COMMANDS
    runner_selecting_wrappers: frozenset[str] = frozenset()


_DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY = _RemoteTestRunnerCapability()


class _RemoteTestCapabilityInvalid(ValueError):
    """A capability table shape is unusable and must fall back with a warning."""


def _capability_remedy() -> str:
    return (
        f"declare its basename in {_REMOTE_TEST_CAPABILITY_FILE} "
        f"{_REMOTE_TEST_CAPABILITY_TABLE}.{_REMOTE_TEST_RUNNER_COMMANDS_KEY}; "
        "for uv/npx selectors, declare the selector separately in "
        f"{_REMOTE_TEST_CAPABILITY_TABLE}.{_REMOTE_TEST_RUNNER_SELECTING_WRAPPERS_KEY}"
    )


def _declared_basenames(value: object, *, key: str) -> tuple[frozenset[str], list[str]]:
    """Return valid basenames and diagnostics for rejected declaration entries."""
    if not isinstance(value, list):
        return frozenset(), [f"{_REMOTE_TEST_CAPABILITY_TABLE}.{key} must be an array of basenames"]
    declared: set[str] = set()
    rejected: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item or _RUNNER_COMMAND_BASENAME_RE.fullmatch(item) is None:
            rejected.append(f"{_REMOTE_TEST_CAPABILITY_TABLE}.{key}[{index}] must be a non-empty command basename")
            continue
        if _posix_basename(item) != item:
            rejected.append(f"{_REMOTE_TEST_CAPABILITY_TABLE}.{key}[{index}] must not contain a path")
            continue
        declared.add(item)
    return frozenset(declared), rejected


def _capability_warning(config_path: Path, problems: list[str]) -> str | None:
    if not problems:
        return None
    return (
        f"remote test runner capability warning in {config_path}: "
        f"{'; '.join(problems)}; rejected declarations were ignored and built-in "
        "pytest defaults were retained"
    )


def _read_remote_test_capability_document(
    config_path: Path,
) -> tuple[dict[str, object] | None, str | None]:
    """Read and parse the capability document; ``None`` means no file."""
    try:
        raw = config_path.read_bytes()
    except FileNotFoundError:
        return None, None
    except OSError as exc:
        return None, f"cannot read {config_path}: {exc}"
    try:
        return tomllib.loads(raw.decode("utf-8")), None
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        return None, f"cannot parse {config_path}: {exc}"


def _remote_test_capability_table(document: dict[str, object], *, config_path: Path) -> dict[str, object] | None:
    """Return the declared table, distinguishing absence from malformed parents."""
    table: object = document
    for key in ("tool", "workbay", "remote_test"):
        if not isinstance(table, dict):
            raise _RemoteTestCapabilityInvalid(f"parent of {key!r} in {config_path} must be a table")
        if key not in table:
            return None
        table = table[key]
    if not isinstance(table, dict):
        raise _RemoteTestCapabilityInvalid(f"{_REMOTE_TEST_CAPABILITY_TABLE} in {config_path} must be a table")
    return table


def _build_remote_test_runner_capability(
    table: dict[str, object], *, config_path: Path
) -> tuple[_RemoteTestRunnerCapability, str | None]:
    """Construct an additive capability, retaining valid declaration entries."""
    problems: list[str] = []
    unknown = set(table) - {
        _REMOTE_TEST_RUNNER_COMMANDS_KEY,
        _REMOTE_TEST_RUNNER_SELECTING_WRAPPERS_KEY,
    }
    if unknown:
        problems.append(
            f"{_REMOTE_TEST_CAPABILITY_TABLE} has unknown keys: {', '.join(sorted(str(key) for key in unknown))}"
        )
    declared_runners, runner_problems = _declared_basenames(
        table.get(_REMOTE_TEST_RUNNER_COMMANDS_KEY, []),
        key=_REMOTE_TEST_RUNNER_COMMANDS_KEY,
    )
    declared_wrappers, wrapper_problems = _declared_basenames(
        table.get(_REMOTE_TEST_RUNNER_SELECTING_WRAPPERS_KEY, []),
        key=_REMOTE_TEST_RUNNER_SELECTING_WRAPPERS_KEY,
    )
    problems.extend(runner_problems)
    problems.extend(wrapper_problems)

    reserved_runner_names = (
        _REMOTE_TEST_CMD_NOOP_BASENAMES
        | _REMOTE_TEST_CMD_SHELL_BASENAMES
        | _REMOTE_TEST_CMD_PREFIX_WRAPPER_BASENAMES
        | _REMOTE_TEST_CMD_RESERVED_INTERPRETER_BASENAMES
        | _SUPPORTED_RUNNER_SELECTING_WRAPPERS
        | frozenset({"cd", "exit"})
    )
    collisions = declared_runners & reserved_runner_names
    if collisions:
        problems.append(
            f"runner_commands rejected shells, interpreters, no-ops, or wrappers ({', '.join(sorted(collisions))})"
        )
        declared_runners -= collisions
    unsupported_wrappers = declared_wrappers - _SUPPORTED_RUNNER_SELECTING_WRAPPERS
    if unsupported_wrappers:
        problems.append(
            "runner_selecting_wrappers rejected unsupported entries "
            f"({', '.join(sorted(unsupported_wrappers))}); supported: "
            f"{', '.join(sorted(_SUPPORTED_RUNNER_SELECTING_WRAPPERS))}"
        )
        declared_wrappers -= unsupported_wrappers
    capability = _RemoteTestRunnerCapability(
        runner_commands=_DEFAULT_REMOTE_TEST_CMD_RUNNER_COMMANDS | declared_runners,
        runner_selecting_wrappers=declared_wrappers,
    )
    return capability, _capability_warning(config_path, problems)


def _load_remote_test_runner_capability(
    repository_root: str | Path | None,
) -> tuple[_RemoteTestRunnerCapability, str | None]:
    """Load the tracked repository capability, preserving legacy defaults.

    A missing file or table is the zero-declaration case and reproduces the
    original pytest/python-only behaviour. Invalid declaration parts are
    ignored and warned about; valid siblings remain additive. A wholly
    malformed declaration falls back to the defaults, so widening policy can
    never remove the built-in pytest path.
    """
    if repository_root is None:
        return _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY, None
    config_path = Path(repository_root) / _REMOTE_TEST_CAPABILITY_FILE
    document, read_error = _read_remote_test_capability_document(config_path)
    if read_error is not None:
        warning = _capability_warning(config_path, [read_error])
        return _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY, warning
    if document is None:
        return _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY, None
    try:
        table = _remote_test_capability_table(document, config_path=config_path)
        if table is None:
            return _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY, None
        return _build_remote_test_runner_capability(table, config_path=config_path)
    except _RemoteTestCapabilityInvalid as exc:
        warning = _capability_warning(config_path, [str(exc)])
        return _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY, warning


def _posix_basename(token: str) -> str:
    if token in {":", ".", ".."}:
        return token
    return PurePosixPath(token).name or token


def _is_absolute_path_like(token: str) -> bool:
    return token.startswith("/") or token.startswith("~")


def _is_contained_repository_relative_path(value: str) -> bool:
    """Return whether a path stays below its checkout-relative starting point."""
    if value.startswith("file:"):
        value = value.removeprefix("file:")
    return bool(value) and not _is_absolute_path_like(value) and ".." not in PurePosixPath(value).parts


def _is_allowed_home_forward(text: str, index: int) -> bool:
    """True only for the constructor's whole-token ``HOME=$HOME`` form."""
    prefix_start = index - len("HOME=")
    after = index + len("$HOME")
    return (
        prefix_start >= 0
        and text[prefix_start:index] == "HOME="
        and (prefix_start == 0 or text[prefix_start - 1].isspace())
        and (after == len(text) or text[after].isspace())
    )


def _unquoted_shell_character(text: str, index: int, *, held: str) -> tuple[int, str | None]:
    """Judge one unquoted character and return the next scan position."""
    char = text[index]
    if char == "$":
        if _is_allowed_home_forward(text, index):
            return index + len("$HOME"), None
        return index + 1, f"test_cmd contains shell metacharacter '$': {held!r}"
    if char in {"\n", "\r", "`", ";", "|", "<", ">", "(", ")", "{", "}"}:
        return index + 1, f"test_cmd contains shell metacharacter {char!r}: {held!r}"
    if char != "&":
        return index + 1, None
    if index + 1 < len(text) and text[index + 1] == "&":
        if index + 2 < len(text) and text[index + 2] == "&":
            return index + 1, f"test_cmd contains unapproved ampersand sequence: {held!r}"
        return index + 2, None
    return index + 1, f"test_cmd contains shell metacharacter '&': {held!r}"


def _double_quoted_shell_character(char: str, *, held: str) -> str | None:
    if char in {"$", "`"}:
        return f"test_cmd contains shell expansion metacharacter {char!r}: {held!r}"
    return None


def _quoted_shell_character(quote: str, char: str, *, held: str) -> tuple[str | None, str | None]:
    """Advance one quoted character and report expansion syntax."""
    if quote == "'":
        return (None if char == "'" else quote), None
    if char == '"':
        return None, None
    return quote, _double_quoted_shell_character(char, held=held)


def _shell_metacharacter_refusal(text: str, *, held: str) -> str | None:
    """Reject shell syntax except the reviewed ``cd ... && runner`` join.

    Quote and backslash handling mirrors the shell closely enough to distinguish
    literal punctuation in test arguments from syntax that can append a second
    command, redirect output, expand a command, or introduce a multiline body.
    The structural gate later restricts the only admitted connector (``&&``)
    to a single relative ``cd`` prefix.
    """
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(text):
        char = text[index]
        if escaped:
            escaped = False
        elif char == "\\" and quote != "'":
            escaped = True
        elif quote is not None:
            quote, refusal = _quoted_shell_character(quote, char, held=held)
            if refusal is not None:
                return refusal
        elif char in {"'", '"'}:
            quote = char
        else:
            index, refusal = _unquoted_shell_character(text, index, held=held)
            if refusal is not None:
                return refusal
            continue
        index += 1
    return None


# Hermetic constructor (build_lane_test_cmd) emits TMPDIR=/tmp. Allow the POSIX
# temp root only as an env-assignment VALUE, never as a bare token or attached
# flag, and never a child of /tmp.
_ALLOWED_POSIX_TEMP_ROOTS: frozenset[str] = frozenset({"/tmp", "/tmp/"})


def _split_env_assignment(token: str) -> tuple[str, str] | None:
    if "=" not in token or token.startswith("="):
        return None
    name, value = token.split("=", 1)
    if not name or not (name[0].isascii() and (name[0].isalpha() or name[0] == "_")):
        return None
    if not all(c.isascii() and (c.isalnum() or c == "_") for c in name):
        return None
    return name, value


def _nested_path_candidate_parts(value: str) -> list[str]:
    """Return unique non-empty pieces exposed by nested list/assignment syntax."""
    parts: list[str] = []
    for delimiter in (":", "="):
        if delimiter not in value:
            continue
        for part in value.split(delimiter):
            if part and part != value and part not in parts:
                parts.append(part)
    return parts


def _path_candidates_from_env_value(value: str, *, _depth: int = 0) -> list[str]:
    """Collect path-like fragments from colon lists and nested KEY=value segments.

    After the first ``=`` split, walk every ``KEY=value`` segment and every
    colon-separated part recursively so ``--override-ini=cache_dir=/Users/x``
    and ``PYTEST_ADDOPTS=--basetemp=/Users/x`` cannot skip the host-absolute
    gate. Depth is capped (bounded walk; no unbounded buffer).
    """
    if not value or _depth > _MAX_PATH_CANDIDATE_WALK_DEPTH:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(part: str) -> None:
        if part and part not in seen:
            seen.add(part)
            out.append(part)

    add(value)
    if _depth >= _MAX_PATH_CANDIDATE_WALK_DEPTH:
        return out
    for part in _nested_path_candidate_parts(value):
        for nested in _path_candidates_from_env_value(part, _depth=_depth + 1):
            add(nested)
    return out


class _RemoteTestCmdRefuse(Exception):
    """Fail-closed remote test-cmd classification (wrapper peel)."""


def _normalized_argv(tokens: list[str]) -> list[str]:
    argv = [token.rstrip(";") for token in tokens]
    return [token for token in argv if token]


def _posix_shell_argv(text: str, *, held: str) -> list[str]:
    """SEC-01 / TEST-15: expose attached connectors as structural tokens."""
    try:
        lexer = shlex.shlex(text, posix=True, punctuation_chars="&|;")
        lexer.whitespace_split = True
        lexer.commenters = ""
        argv = _normalized_argv(list(lexer))
    except ValueError:
        raise _RemoteTestCmdRefuse(f"test_cmd is unparseable: {held!r}") from None
    if not argv:
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    return argv


def _wrapper_unresolved_reason(held: str) -> str:
    return f"test_cmd wrapper payload cannot be resolved: {held!r}"


def _wrapper_noop_reason(held: str) -> str:
    return f"test_cmd is a no-op: {held!r}"


def _require_option_value(tail: list[str], index: int, *, held: str) -> None:
    if index + 1 >= len(tail):
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))


def _attached_or_concatenated_option_value(token: str, names: frozenset[str]) -> tuple[str, str] | None:
    """Return the option and value of ``--name=value`` or concatenated ``-Xvalue``."""
    if "=" in token:
        name, value = token.split("=", 1)
        if name in names:
            return name, value
        return None
    if token.startswith("--"):
        return None
    for name in names:
        if len(name) == 2 and name[0] == "-" and token.startswith(name) and token != name:
            return name, token[len(name) :]
    return None


def _refuse_host_absolute_option_value(
    value: str,
    *,
    held: str,
    require_contained_path: bool = False,
) -> None:
    """Judge a consumed option value according to that option's semantics."""
    if require_contained_path and not _is_contained_repository_relative_path(value):
        raise _RemoteTestCmdRefuse(
            f"test_cmd selector path must be non-empty, repository-relative, and stay inside the checkout: {held!r}"
        )
    for candidate in _path_candidates_from_env_value(value):
        if _is_absolute_path_like(candidate):
            _, reason = _host_absolute_reason(candidate, held)
            raise _RemoteTestCmdRefuse(reason)


def _selector_package_identity(value: str, *, held: str, allow_at_version: bool) -> str:
    """Extract a registry package identity without mistaking it for a path."""
    lowered = value.lower()
    if (
        not value
        or _is_absolute_path_like(value)
        or value.startswith((".", "file:"))
        or "/" in value
        or "\\" in value
        or ":" in value
        or "://" in lowered
        or lowered.startswith(("git+", "git@"))
    ):
        raise _RemoteTestCmdRefuse(
            "test_cmd package source redirect is forbidden (package paths outside the declared identity are not "
            f"repository-relative selector paths) in value {value!r}: {held!r}"
        )
    match = re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", value)
    if match is None:
        raise _RemoteTestCmdRefuse(f"test_cmd contains invalid selector package value {value!r}: {held!r}")
    identity = match.group(0)
    remainder = value[len(identity) :]
    npm_version = allow_at_version and re.fullmatch(r"@[A-Za-z0-9][A-Za-z0-9._*+^-]*", remainder)
    pep_version = re.fullmatch(r"(?:===|==|~=|!=|<=|>=|<|>)[A-Za-z0-9][A-Za-z0-9._*+!-]*", remainder)
    if remainder and npm_version is None and pep_version is None:
        if remainder.startswith("@"):
            raise _RemoteTestCmdRefuse(
                f"test_cmd package source redirect is forbidden in selector package value {value!r}: {held!r}"
            )
        raise _RemoteTestCmdRefuse(f"test_cmd contains invalid selector package value {value!r}: {held!r}")
    return identity


def _judge_selector_package_value(
    value: str,
    *,
    held: str,
    declared_package_names: frozenset[str],
    allow_at_version: bool,
) -> None:
    identity = _selector_package_identity(value, held=held, allow_at_version=allow_at_version)
    if identity not in declared_package_names:
        raise _RemoteTestCmdRefuse(
            f"test_cmd uses undeclared selector package {identity!r}: {held!r}; remedy: {_capability_remedy()}"
        )


def _advance_past_value_option(
    tail: list[str],
    index: int,
    names: frozenset[str],
    *,
    held: str,
    contained_path_names: frozenset[str] = frozenset(),
    package_names: frozenset[str] = frozenset(),
    source_redirect_names: frozenset[str] = frozenset(),
    declared_package_names: frozenset[str] = frozenset(),
    at_version_package_names: frozenset[str] = frozenset(),
) -> int | None:
    """Consume a value option after judging its dest; return the next index."""
    token = tail[index]
    if token in names:
        _require_option_value(tail, index, held=held)
        value = tail[index + 1]
        option = token
        next_index = index + 2
    else:
        attached = _attached_or_concatenated_option_value(token, names)
        if attached is None:
            return None
        option, value = attached
        next_index = index + 1
    if option in source_redirect_names:
        raise _RemoteTestCmdRefuse(f"test_cmd package source redirect option {option!r} is forbidden: {held!r}")
    if option in package_names:
        _judge_selector_package_value(
            value,
            held=held,
            declared_package_names=declared_package_names,
            allow_at_version=option in at_version_package_names,
        )
    else:
        _refuse_host_absolute_option_value(
            value,
            held=held,
            require_contained_path=option in contained_path_names,
        )
    return next_index


def _peel_env_options(tail: list[str], *, held: str) -> list[str]:
    kept: list[str] = []
    index = 0
    length = len(tail)
    while index < length:
        token = tail[index]
        if _split_env_assignment(token) is not None:
            kept.append(token)
            index += 1
            continue
        if token in {"-S", "--split-string"} or token.startswith("--split-string="):
            raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
        consumed = _advance_past_value_option(tail, index, _ENV_VALUE_OPTIONS, held=held)
        if consumed is not None:
            index = consumed
            continue
        if token == "--":
            return kept + tail[index + 1 :]
        if token.startswith("-"):
            index += 1
            continue
        return kept + tail[index:]
    return kept


def _peel_timeout_options(tail: list[str], *, held: str) -> list[str]:
    index = 0
    length = len(tail)
    saw_duration = False
    while index < length:
        token = tail[index]
        if token == "--":
            index += 1
            continue
        if token in {"-k", "--kill-after", "-s", "--signal"}:
            _require_option_value(tail, index, held=held)
            index += 2
            continue
        if token.startswith(("--kill-after=", "--signal=")):
            index += 1
            continue
        if token in {"-v", "--verbose", "--preserve-status", "--foreground"}:
            index += 1
            continue
        if token.startswith("-") and _TIMEOUT_DURATION_RE.fullmatch(token) is None:
            index += 1
            continue
        if not saw_duration:
            if _TIMEOUT_DURATION_RE.fullmatch(token) is None:
                raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
            saw_duration = True
            index += 1
            continue
        return tail[index:]
    return []


def _peel_sudo_options(tail: list[str], *, held: str) -> list[str]:
    index = 0
    length = len(tail)
    while index < length:
        token = tail[index]
        if token == "--":
            return tail[index + 1 :]
        consumed = _advance_past_value_option(tail, index, _SUDO_VALUE_OPTIONS, held=held)
        if consumed is not None:
            index = consumed
            continue
        if token.startswith("-"):
            index += 1
            continue
        return tail[index:]
    return []


def _peel_eval_payload(tail: list[str], *, held: str) -> list[str]:
    if not tail:
        return []
    if len(tail) == 1:
        if refusal := _shell_metacharacter_refusal(tail[0], held=held):
            raise _RemoteTestCmdRefuse(refusal)
        return _posix_shell_argv(tail[0], held=held)
    return tail


def _peel_nice_options(tail: list[str], *, held: str) -> list[str]:
    if not tail:
        return []
    token = tail[0]
    if token in {"-n", "--adjustment"}:
        _require_option_value(tail, 0, held=held)
        return tail[2:]
    if token.startswith("--adjustment=") or (token.startswith("-n") and token != "-n"):
        return tail[1:]
    if len(token) >= 2 and token[0] == "-" and token[1:].isdigit():
        return tail[1:]
    return tail


def _peel_command_options(tail: list[str]) -> list[str]:
    index = 0
    length = len(tail)
    while index < length:
        token = tail[index]
        if token == "--":
            return tail[index + 1 :]
        if token in {"-p", "-v", "-V"}:
            index += 1
            continue
        if token.startswith("-"):
            index += 1
            continue
        return tail[index:]
    return []


def _peel_exec_options(tail: list[str], *, held: str) -> list[str]:
    index = 0
    length = len(tail)
    while index < length:
        token = tail[index]
        if token == "--":
            return tail[index + 1 :]
        if token in {"-l", "-c"}:
            index += 1
            continue
        if token == "-a":
            _require_option_value(tail, index, held=held)
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        return tail[index:]
    return []


def _peel_xargs_options(tail: list[str], *, held: str) -> list[str]:
    index = 0
    length = len(tail)
    while index < length:
        token = tail[index]
        if token == "--":
            return tail[index + 1 :]
        consumed = _advance_past_value_option(tail, index, _XARGS_VALUE_OPTIONS, held=held)
        if consumed is not None:
            index = consumed
            continue
        if token.startswith("-"):
            index += 1
            continue
        return tail[index:]
    return []


def _peel_supported_runner_options(
    tail: list[str],
    value_options: frozenset[str],
    boolean_options: frozenset[str],
    terminal_options: frozenset[str],
    *,
    held: str,
    command_name: str,
    contained_path_options: frozenset[str] = frozenset(),
    package_options: frozenset[str] = frozenset(),
    source_redirect_options: frozenset[str] = frozenset(),
    declared_package_names: frozenset[str] = frozenset(),
    at_version_package_options: frozenset[str] = frozenset(),
) -> list[str]:
    """Peel reviewed options and refuse terminal or unknown option shapes."""
    index = 0
    while index < len(tail):
        token = tail[index]
        if token == "--":
            return tail[index + 1 :]
        if token in terminal_options:
            raise _RemoteTestCmdRefuse(
                f"test_cmd {command_name} option {token!r} exits without running tests: {held!r}"
            )
        consumed = _advance_past_value_option(
            tail,
            index,
            value_options,
            held=held,
            contained_path_names=contained_path_options,
            package_names=package_options,
            source_redirect_names=source_redirect_options,
            declared_package_names=declared_package_names,
            at_version_package_names=at_version_package_options,
        )
        if consumed is not None:
            index = consumed
            continue
        if token in boolean_options:
            index += 1
            continue
        if token.startswith("-"):
            raise _RemoteTestCmdRefuse(f"test_cmd uses unsupported {command_name} option {token!r}: {held!r}")
        return tail[index:]
    return []


def _peel_runner_selecting_wrapper(
    base: str,
    rest: list[str],
    *,
    held: str,
    capability: _RemoteTestRunnerCapability,
) -> list[str]:
    """Resolve a declared uv/npx selector to the runner it names."""
    tail = rest[1:]
    if base == "uv":
        # ``uv`` has many subcommands. Only ``uv run`` selects a test runner;
        # admitting the basename alone would also admit arbitrary uv actions.
        if not tail or tail[0] != "run":
            raise _RemoteTestCmdRefuse(f"declared runner-selecting wrapper 'uv' must use 'uv run': {held!r}")
        inner = _peel_supported_runner_options(
            tail[1:],
            _UV_RUN_VALUE_OPTIONS,
            _UV_RUN_BOOLEAN_OPTIONS,
            _UV_RUN_TERMINAL_OPTIONS,
            held=held,
            command_name="uv run",
            contained_path_options=_UV_RUN_CONTAINED_PATH_VALUE_OPTIONS,
            package_options=_UV_RUN_PACKAGE_VALUE_OPTIONS,
            source_redirect_options=_UV_RUN_SOURCE_REDIRECT_VALUE_OPTIONS,
            declared_package_names=capability.runner_commands,
        )
    elif base == "npx":
        inner = _peel_supported_runner_options(
            tail,
            _NPX_VALUE_OPTIONS,
            _NPX_BOOLEAN_OPTIONS,
            _NPX_TERMINAL_OPTIONS,
            held=held,
            command_name="npx",
            package_options=_NPX_PACKAGE_VALUE_OPTIONS,
            source_redirect_options=_NPX_SOURCE_REDIRECT_VALUE_OPTIONS,
            declared_package_names=capability.runner_commands,
            at_version_package_options=_NPX_PACKAGE_VALUE_OPTIONS,
        )
    else:  # guarded by capability validation; retain a fail-closed local invariant
        raise _RemoteTestCmdRefuse(f"unsupported runner-selecting wrapper {base!r}: {held!r}")
    if not inner:
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    return inner


def _peel_leading_flags(tail: list[str]) -> list[str]:
    index = 0
    length = len(tail)
    while index < length:
        token = tail[index]
        if token == "--":
            return tail[index + 1 :]
        if token.startswith("-"):
            index += 1
            continue
        return tail[index:]
    return []


def _peel_shell_segment(rest: list[str], *, held: str) -> list[str]:
    index = 1
    length = len(rest)
    payload: str | None = None
    while index < length:
        token = rest[index]
        if token == "-c":
            _require_option_value(rest, index, held=held)
            payload = rest[index + 1]
            break
        if token in {"-o", "-O"}:
            _require_option_value(rest, index, held=held)
            index += 2
            continue
        if token == "--":
            index += 1
            continue
        if token.startswith("-"):
            index += 1
            continue
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
    if payload is None:
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
    if refusal := _shell_metacharacter_refusal(payload, held=held):
        raise _RemoteTestCmdRefuse(refusal)
    inner = _posix_shell_argv(payload, held=held)
    if not inner:
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    return inner


def _peel_prefix_segment(base: str, rest: list[str], *, held: str) -> list[str]:
    tail = rest[1:]
    if base == "env":
        inner = _peel_env_options(tail, held=held)
    elif base == "timeout":
        inner = _peel_timeout_options(tail, held=held)
    elif base == "sudo":
        inner = _peel_sudo_options(tail, held=held)
    elif base == "eval":
        inner = _peel_eval_payload(tail, held=held)
    elif base == "nice":
        inner = _peel_nice_options(tail, held=held)
    elif base == "command":
        inner = _peel_command_options(tail)
    elif base == "exec":
        inner = _peel_exec_options(tail, held=held)
    elif base == "xargs":
        inner = _peel_xargs_options(tail, held=held)
    else:
        inner = _peel_leading_flags(tail)
    if not inner:
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    return inner


def _flatten_segment(
    segment: list[str],
    *,
    held: str,
    depth: int,
    capability: _RemoteTestRunnerCapability,
) -> list[str]:
    if depth >= _MAX_WRAPPER_PEEL_DEPTH:
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
    kept: list[str] = []
    index = 0
    length = len(segment)
    while index < length and _split_env_assignment(segment[index]) is not None:
        kept.append(segment[index])
        index += 1
    rest = segment[index:]
    if not rest:
        return kept
    base = _posix_basename(rest[0])
    if base == "cd":
        return kept + rest
    if base in _REMOTE_TEST_CMD_SHELL_BASENAMES:
        inner = _peel_shell_segment(rest, held=held)
        return kept + _flatten_wrappers(inner, held=held, depth=depth + 1, capability=capability)
    if base in _REMOTE_TEST_CMD_PREFIX_WRAPPER_BASENAMES:
        inner = _peel_prefix_segment(base, rest, held=held)
        return kept + _flatten_wrappers(inner, held=held, depth=depth + 1, capability=capability)
    if base in capability.runner_selecting_wrappers:
        inner = _peel_runner_selecting_wrapper(base, rest, held=held, capability=capability)
        return kept + _flatten_wrappers(inner, held=held, depth=depth + 1, capability=capability)
    return kept + rest


def _flatten_wrappers(
    argv: list[str],
    *,
    held: str,
    depth: int = 0,
    capability: _RemoteTestRunnerCapability = _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY,
) -> list[str]:
    """Peel prefix wrappers and quoted ``-c`` payloads into a flat argv.

    Fail closed when a wrapper has no payload, the payload is unparseable, or
    the peeler cannot resolve the nested command. Depth is capped (bounded
    walk; no unbounded buffer).
    """
    if depth >= _MAX_WRAPPER_PEEL_DEPTH:
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
    out: list[str] = []
    index = 0
    length = len(argv)
    while index < length:
        if argv[index] in _REMOTE_TEST_CMD_CONNECTORS:
            out.append(argv[index])
            index += 1
            continue
        end = index + 1
        while end < length and argv[end] not in _REMOTE_TEST_CMD_CONNECTORS:
            end += 1
        out.extend(
            _flatten_segment(
                argv[index:end],
                held=held,
                depth=depth,
                capability=capability,
            )
        )
        index = end
    return out


def _executable_payload_tokens(cmd_argv: list[str]) -> list[str]:
    """Return command tokens after ignoring cd, dest, connectors, and env.

    A remote test command is a no-op when every remaining executable token is a
    no-op basename (including ``false`` and ``exit`` with a non-zero status).
    ``cd`` plus its relative destination, shell connectors, ``NAME=value``
    assignments, and flag tokens are not payload. Prefix wrappers and quoted
    ``-c`` payloads are peeled before this walk so they cannot hide a no-op.
    """
    payload: list[str] = []
    index = 0
    length = len(cmd_argv)
    while index < length:
        token = cmd_argv[index]
        if token in _REMOTE_TEST_CMD_CONNECTORS:
            index += 1
            continue
        if _split_env_assignment(token) is not None:
            index += 1
            continue
        base = _posix_basename(token)
        if base == "cd":
            index += 1
            if index < length:
                dest = cmd_argv[index]
                if dest not in _REMOTE_TEST_CMD_CONNECTORS and _split_env_assignment(dest) is None:
                    index += 1
            continue
        if token.startswith("-"):
            index += 1
            continue
        payload.append(token)
        index += 1
    return payload


def _split_command_segments(cmd_argv: list[str]) -> list[list[str]]:
    segments: list[list[str]] = [[]]
    for token in cmd_argv:
        if token in _REMOTE_TEST_CMD_CONNECTORS:
            segments.append([])
        else:
            segments[-1].append(token)
    return segments


def _is_approved_version_query(
    segment: list[str],
    capability: _RemoteTestRunnerCapability,
    *,
    held: str,
) -> bool:
    """Return whether a segment is only an approved runner version query."""
    if not segment or segment[-1] != "--version":
        return False
    if len(segment) == 2:
        payload = [segment[0]]
    elif (
        len(segment) == 4
        and _posix_basename(segment[0]) in _REMOTE_TEST_CMD_INTERPRETER_BASENAMES
        and segment[1] == "-m"
        and _posix_basename(segment[2]) in _DEFAULT_REMOTE_TEST_CMD_RUNNER_MODULES
    ):
        payload = [segment[0], segment[2]]
    else:
        return False
    return _is_approved_remote_test_runner(payload, segment, capability, held=held)


def _trailing_verification_path_refusal(path: str, *, held: str) -> str | None:
    if _is_contained_repository_relative_path(path):
        return None
    return (
        "test_cmd trailing verification path must be non-empty, repository-relative, "
        f"and stay inside the checkout: {held!r}"
    )


def _trailing_verification_refusal(
    segments: list[list[str]],
    capability: _RemoteTestRunnerCapability,
    *,
    held: str,
) -> str | None:
    """Validate the closed read-only gate after a runner version query."""
    if len(segments) not in {2, 3} or not _is_approved_version_query(segments[0], capability, held=held):
        return f"test_cmd trailing verification allowlist rejects this '&&' command shape: {held!r}"
    size_check = segments[1]
    if len(size_check) != 3 or _posix_basename(size_check[0]) != "test" or size_check[1] != "-s":
        return f"test_cmd trailing verification allowlist permits only 'test -s PATH': {held!r}"
    if refusal := _trailing_verification_path_refusal(size_check[2], held=held):
        return refusal
    if len(segments) == 2:
        return None
    content_check = segments[2]
    if (
        len(content_check) != 4
        or _posix_basename(content_check[0]) != "grep"
        or content_check[1] != "-Fq"
        or not content_check[2]
    ):
        return (
            "test_cmd trailing verification allowlist permits only "
            f"'grep -Fq FIXED_STRING PATH' after the size check: {held!r}"
        )
    if refusal := _trailing_verification_path_refusal(content_check[3], held=held):
        return refusal
    if content_check[3] != size_check[2]:
        return f"test_cmd trailing verification allowlist requires both checks to use the same path: {held!r}"
    return None


def _command_structure_refusal(
    cmd_argv: list[str],
    capability: _RemoteTestRunnerCapability,
    *,
    held: str,
) -> str | None:
    """Permit a relative cd prefix or a bounded read-only artifact gate."""
    connector_indexes = [index for index, token in enumerate(cmd_argv) if token in _REMOTE_TEST_CMD_CONNECTORS]
    if not connector_indexes:
        if any(_posix_basename(token) == "cd" for token in cmd_argv):
            return f"test_cmd cd prefix must be followed by '&&' and an approved runner: {held!r}"
        return None
    if any(cmd_argv[index] != "&&" for index in connector_indexes):
        return f"test_cmd contains an unapproved shell command connector: {held!r}"
    segments = _split_command_segments(cmd_argv)
    if segments and len(segments[0]) == 2 and _posix_basename(segments[0][0]) == "cd":
        cd_dest = segments[0][1]
        if not cd_dest or cd_dest.startswith("-"):
            return f"test_cmd cd prefix requires a repository-relative destination: {held!r}"
        if _is_absolute_path_like(cd_dest):
            return _host_absolute_reason(cd_dest, held)[1]
        if not _is_contained_repository_relative_path(cd_dest):
            return f"test_cmd cd prefix must stay inside the checkout: {held!r}"
        if len(segments) == 2:
            if not segments[1]:
                return _wrapper_noop_reason(held)
            return None
        return _trailing_verification_refusal(segments[1:], capability, held=held)
    return _trailing_verification_refusal(segments, capability, held=held)


def _is_noop_payload_token(token: str) -> bool:
    base = _posix_basename(token)
    if base in _REMOTE_TEST_CMD_NOOP_BASENAMES or base == "exit":
        return True
    return token.isdigit()


def _unapproved_runner_reason(basename: str, held: str) -> tuple[str, str]:
    return (
        "refuse",
        f"test_cmd first payload is not an approved test runner ({basename}): {held!r}; remedy: {_capability_remedy()}",
    )


def _concatenated_code_string_payload(token: str) -> str | None:
    """Return the payload of concatenated ``-c…`` / ``-c=…``, else None."""
    if token.startswith("--") or not token.startswith("-c") or token == "-c":
        return None
    payload = token[2:]
    if payload.startswith("="):
        payload = payload[1:]
    return payload


def _interpreter_selects_runner_module(
    cmd_argv: list[str],
    interpreter_token: str,
) -> bool:
    """True when Python selects a built-in approved module with ``-m``.

    Executable declarations are intentionally not module declarations: adding
    ``vitest`` or ``phpunit`` must not also approve ``python -m vitest`` or
    ``python -m phpunit``, which invoke a different runtime boundary.
    """
    try:
        start = cmd_argv.index(interpreter_token)
    except ValueError:
        return False
    rest = cmd_argv[start + 1 :]
    index = 0
    length = len(rest)
    while index < length:
        token = rest[index]
        if token in _REMOTE_TEST_CMD_CONNECTORS:
            break
        if token == "-c" or _concatenated_code_string_payload(token) is not None:
            return False
        if token == "--":
            return False
        if token == "-m":
            if index + 1 >= length:
                return False
            return _posix_basename(rest[index + 1]) in _DEFAULT_REMOTE_TEST_CMD_RUNNER_MODULES
        if token.startswith("-m") and token != "-m":
            module = token[2:]
            if module.startswith("="):
                module = module[1:]
            return bool(module) and _posix_basename(module) in _DEFAULT_REMOTE_TEST_CMD_RUNNER_MODULES
        index += 1
    return False


def _npm_selects_test_script(cmd_argv: list[str], npm_token: str, *, held: str) -> bool:
    """Admit npm's test script only, never npm exec or an arbitrary script."""
    try:
        start = cmd_argv.index(npm_token)
    except ValueError:
        return False
    rest = _peel_supported_runner_options(
        cmd_argv[start + 1 :],
        _NPM_VALUE_OPTIONS,
        _NPM_BOOLEAN_OPTIONS,
        _NPM_TERMINAL_OPTIONS,
        held=held,
        command_name="npm",
    )
    if not rest:
        return False
    command = rest[0]
    if command in {"test", "t", "tst"}:
        return True
    return command == "run" and len(rest) > 1 and rest[1] == "test"


def _is_approved_remote_test_runner(
    payload: list[str],
    cmd_argv: list[str],
    capability: _RemoteTestRunnerCapability = _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY,
    *,
    held: str = "",
) -> bool:
    """True when the first payload selects a repository-approved test runner."""
    first = payload[0]
    first_base = _posix_basename(first)
    if first_base in capability.runner_commands:
        if first_base == "npm":
            return _npm_selects_test_script(cmd_argv, first, held=held)
        return True
    if first_base in _REMOTE_TEST_CMD_INTERPRETER_BASENAMES:
        return _interpreter_selects_runner_module(cmd_argv, first)
    return False


def _host_absolute_reason(candidate: str, held: str) -> tuple[str, str]:
    return "refuse", f"test_cmd contains host-absolute path {candidate!r}: {held!r}"


def _token_path_candidates(token: str) -> tuple[list[str], bool]:
    """Return path candidates and whether env-value ``/tmp`` rules apply."""
    assignment = _split_env_assignment(token)
    if assignment is not None:
        return _path_candidates_from_env_value(assignment[1]), True
    if token.startswith("-") and "=" in token:
        _, value = token.split("=", 1)
        return _path_candidates_from_env_value(value), False
    return [token], False


def _host_absolute_refusal(cmd_argv: list[str], held: str) -> tuple[str, str] | None:
    """Return a host-absolute refuse pair when any argv token is path-like."""
    for token in cmd_argv:
        candidates, allow_temp_root = _token_path_candidates(token)
        for candidate in candidates:
            if allow_temp_root and candidate in _ALLOWED_POSIX_TEMP_ROOTS:
                continue
            if candidate in _ALLOWED_ABSOLUTE_INTERPRETERS:
                continue
            if _is_absolute_path_like(candidate):
                return _host_absolute_reason(candidate, held)
    return None


def _payload_is_noop(payload: list[str]) -> bool:
    if not payload:
        return True
    first_payload_base = _posix_basename(payload[0])
    if first_payload_base in _REMOTE_TEST_CMD_NOOP_BASENAMES or first_payload_base == "exit":
        return True
    return all(_is_noop_payload_token(token) for token in payload)


def _find_code_string_payload(segment: list[str], *, held: str) -> str | None:
    """Return the first ``-c`` payload in ``segment``, on any basename."""
    index = 0
    length = len(segment)
    while index < length:
        token = segment[index]
        if token in _REMOTE_TEST_CMD_CONNECTORS:
            break
        if token == "--":
            break
        if token == "-c":
            _require_option_value(segment, index, held=held)
            return segment[index + 1]
        concatenated = _concatenated_code_string_payload(token)
        if concatenated is not None:
            return concatenated
        index += 1
    return None


def _interpreter_code_string_mode(segment: list[str], interpreter_token: str) -> bool:
    """True when ``-c`` is this interpreter's mode (before ``-m``, ``--``, or a script)."""
    try:
        start = segment.index(interpreter_token)
    except ValueError:
        return False
    for token in segment[start + 1 :]:
        if token in _REMOTE_TEST_CMD_CONNECTORS:
            return False
        if token == "-c" or _concatenated_code_string_payload(token) is not None:
            return True
        if token == "-m" or (token.startswith("-m") and token != "-m"):
            return False
        if token == "--":
            return False
        if token.startswith("-"):
            continue
        return False
    return False


def _tokenize_code_string_payload(
    payload: str,
    *,
    held: str,
    depth: int,
    capability: _RemoteTestRunnerCapability,
) -> list[str]:
    """Split a code-string payload and peel nested wrappers (bounded)."""
    if refusal := _shell_metacharacter_refusal(payload, held=held):
        raise _RemoteTestCmdRefuse(refusal)
    inner = _posix_shell_argv(payload, held=held)
    if not inner:
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    inner = _flatten_wrappers(inner, held=held, depth=depth + 1, capability=capability)
    return _apply_code_string_payloads(
        inner,
        held=held,
        depth=depth + 1,
        capability=capability,
    )


def _keep_outer_after_code_string(
    segment: list[str],
    payload: list[str],
    capability: _RemoteTestRunnerCapability,
    *,
    held: str,
) -> bool:
    """True when ``-c`` is a runner config flag, not a quoting layer."""
    if not payload or not _is_approved_remote_test_runner(payload, segment, capability, held=held):
        return False
    first = payload[0]
    if _posix_basename(first) in _REMOTE_TEST_CMD_INTERPRETER_BASENAMES:
        return not _interpreter_code_string_mode(segment, first)
    return True


def _apply_code_string_segment(
    segment: list[str],
    *,
    held: str,
    depth: int,
    capability: _RemoteTestRunnerCapability,
) -> list[str]:
    """Open a ``-c`` payload on any basename and judge its contents."""
    raw = _find_code_string_payload(segment, held=held)
    if raw is None:
        return segment
    inner = _tokenize_code_string_payload(
        raw,
        held=held,
        depth=depth,
        capability=capability,
    )
    if _payload_is_noop(_executable_payload_tokens(inner)):
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    refusal = _host_absolute_refusal(inner, held)
    if refusal is not None:
        raise _RemoteTestCmdRefuse(refusal[1])
    env_kept: list[str] = []
    index = 0
    length = len(segment)
    while index < length and _split_env_assignment(segment[index]) is not None:
        env_kept.append(segment[index])
        index += 1
    rest = segment[index:]
    rest_payload = _executable_payload_tokens(rest)
    if _keep_outer_after_code_string(rest, rest_payload, capability, held=held):
        return segment
    return env_kept + inner


def _apply_code_string_payloads(
    argv: list[str],
    *,
    held: str,
    depth: int = 0,
    capability: _RemoteTestRunnerCapability = _DEFAULT_REMOTE_TEST_RUNNER_CAPABILITY,
) -> list[str]:
    """Tokenize every code-string payload and reuse host-absolute / runner checks.

    Structural: any basename with ``-c`` / concatenated ``-c…`` has that payload
    split and re-walked. A payload that is a no-op or host-absolute refuses.
    An already-approved runner form whose ``-c`` is config (``pytest -c``,
    ``python3 -m pytest -c``) is left in place; otherwise the payload becomes
    the command so the terminal runner decision runs over its contents.
    """
    if depth >= _MAX_WRAPPER_PEEL_DEPTH:
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))
    out: list[str] = []
    index = 0
    length = len(argv)
    while index < length:
        if argv[index] in _REMOTE_TEST_CMD_CONNECTORS:
            out.append(argv[index])
            index += 1
            continue
        end = index + 1
        while end < length and argv[end] not in _REMOTE_TEST_CMD_CONNECTORS:
            end += 1
        out.extend(
            _apply_code_string_segment(
                argv[index:end],
                held=held,
                depth=depth,
                capability=capability,
            )
        )
        index = end
    return out


@dataclass(frozen=True)
class _NormalizedRemoteTestCommand:
    """Contained command after tokenization and wrapper normalization."""

    held: str
    capability: _RemoteTestRunnerCapability
    prefix_env_assignments: tuple[tuple[str, str], ...]
    argv: tuple[str, ...]
    payload: tuple[str, ...]


def _required_remote_test_cmd(text: str | None) -> str:
    """Return the held command or raise the distinct missing/empty refusal."""
    state, held = classify_test_cmd(text)
    if state == _TEST_CMD_ABSENT:
        raise _RemoteTestCmdRefuse(
            "test_cmd is not held because it is missing; remote implement lanes require "
            f"a non-empty approved test runner ({_capability_remedy()})"
        )
    if state == _TEST_CMD_CLEARED or not held:
        raise _RemoteTestCmdRefuse(
            "test_cmd is not held because it is empty; remote implement lanes cannot skip "
            f"verification ({_capability_remedy()})"
        )
    return held


def _required_remote_test_capability(
    repository_root: str | Path | None,
) -> tuple[_RemoteTestRunnerCapability, str | None]:
    return _load_remote_test_runner_capability(repository_root)


def _tokenize_remote_test_command(held: str) -> list[str]:
    """Apply the shell-syntax boundary, then produce a normalized POSIX argv."""
    if refusal := _shell_metacharacter_refusal(held, held=held):
        raise _RemoteTestCmdRefuse(refusal)
    return _posix_shell_argv(held, held=held)


def _split_segment_env_assignments(
    argv: list[str],
) -> tuple[tuple[tuple[str, str], ...], list[str]]:
    """Extract executable-prefix assignments from every shell segment."""
    assignments: list[tuple[str, str]] = []
    command_argv: list[str] = []
    at_segment_start = True
    for token in argv:
        if token in _REMOTE_TEST_CMD_CONNECTORS:
            command_argv.append(token)
            at_segment_start = True
            continue
        assignment = _split_env_assignment(token) if at_segment_start else None
        if assignment is not None:
            assignments.append(assignment)
            continue
        command_argv.append(token)
        at_segment_start = False
    return tuple(assignments), command_argv


def _normalize_remote_test_command(
    held: str,
    capability: _RemoteTestRunnerCapability,
    argv: list[str],
) -> _NormalizedRemoteTestCommand:
    """Peel reviewed wrappers and expose the executable payload as typed data."""
    command_argv = _flatten_wrappers(argv, held=held, capability=capability)
    command_argv = _apply_code_string_payloads(
        command_argv,
        held=held,
        capability=capability,
    )
    prefix_env_assignments, command_argv = _split_segment_env_assignments(command_argv)
    return _NormalizedRemoteTestCommand(
        held=held,
        capability=capability,
        prefix_env_assignments=prefix_env_assignments,
        argv=tuple(command_argv),
        payload=tuple(_executable_payload_tokens(command_argv)),
    )


def _prefix_env_refusal(command: _NormalizedRemoteTestCommand) -> str | None:
    """SEC-01: keep environment prefixes from rebinding the implementation."""
    for name, value in command.prefix_env_assignments:
        if name in {"PATH", "PYTHONHOME"}:
            return f"test_cmd environment assignment {name!r} can rebind the approved runner: {command.held!r}"
        if name == "PYTHONPATH":
            entries = value.split(":")
            for entry in entries:
                if _is_absolute_path_like(entry):
                    return _host_absolute_reason(entry, command.held)[1]
            if any(not _is_contained_repository_relative_path(entry) for entry in entries):
                return (
                    "test_cmd PYTHONPATH entries must be non-empty, repository-relative, "
                    f"and stay inside the checkout: {command.held!r}"
                )
            continue
        for candidate in _path_candidates_from_env_value(value):
            if candidate not in _ALLOWED_POSIX_TEMP_ROOTS and _is_absolute_path_like(candidate):
                return _host_absolute_reason(candidate, command.held)[1]
    return None


def _validate_remote_test_command_structure(command: _NormalizedRemoteTestCommand) -> None:
    """Enforce no-op, host-path, and connector containment on normalized argv."""
    if refusal := _prefix_env_refusal(command):
        raise _RemoteTestCmdRefuse(refusal)
    payload = list(command.payload)
    if _payload_is_noop(payload):
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(command.held))
    argv = list(command.argv)
    if refusal := _command_structure_refusal(argv, command.capability, held=command.held):
        raise _RemoteTestCmdRefuse(refusal)
    if refusal_pair := _host_absolute_refusal(argv, command.held):
        raise _RemoteTestCmdRefuse(refusal_pair[1])


def _validate_remote_test_command_runner(command: _NormalizedRemoteTestCommand) -> None:
    """Require the first normalized payload to select an approved test runner."""
    payload = list(command.payload)
    argv = list(command.argv)
    if _is_approved_remote_test_runner(
        payload,
        argv,
        command.capability,
        held=command.held,
    ):
        return
    first_payload_base = _posix_basename(payload[0])
    raise _RemoteTestCmdRefuse(_unapproved_runner_reason(first_payload_base, command.held)[1])


def classify_remote_test_cmd(
    text: str | None,
    *,
    repository_root: str | Path | None = None,
    capability_warnings: list[str] | None = None,
) -> tuple[str, str]:
    """Classify a resolved test command for remote implement dispatch.

    Reads only the repository-root ``pyproject.toml`` capability when
    ``repository_root`` is supplied, then tokenizes with posix ``shlex``
    (``ValueError`` → refuse).
    Returns ``("refuse", reason)`` when the command is not held, is a no-op,
    contains a host-absolute path, or the first remaining payload is not an
    approved test runner. Otherwise ``("ok", "")``.

    No-op: every executable content token is a no-op basename in
    ``{true, :, echo, false}`` or a wrapper around one (``env``, ``command``,
    ``exec``, ``timeout``, ``sudo``, ``sh``/``bash`` ``-c``, …), including
    ``exit`` with any status. Prefix wrappers are peeled recursively and
    quoted ``-c`` payloads are ``shlex``-split and re-walked, including
    ``&&`` / ``;`` / ``||`` chains inside the payload, so a quoting layer
    cannot hide a no-op. ``cd``, its relative destination, connectors, and
    ``NAME=value`` assignments are ignored when collecting payload tokens, so
    a constructor-shaped ``cd packages/<pkg> && true`` refuses. A wrapper with
    no payload, an unparseable payload, or a payload the peeler cannot resolve
    refuses (fail closed).

    Approved runner (fail closed): after peeling, the first remaining payload
    basename must be ``pytest`` or a direct runner command enumerated in
    ``pyproject.toml [tool.workbay.remote_test].runner_commands``. An
    interpreter (``python`` / ``python3``, including the relative
    ``../../.venv/bin/python`` form producers emit) is admitted only when its
    argument vector selects an approved runner as a module (``-m pytest``).
    ``uv`` and ``npx`` must be declared separately as
    ``runner_selecting_wrappers`` and are peeled to an approved nested runner;
    this never turns an approved runner declaration into approval for a shell
    or arbitrary interpreter. Runner declarations name executables only and
    never widen Python ``-m`` module approval beyond the built-in ``pytest``.
    With no declaration, the behaviour remains the legacy pytest/python-only
    allowlist. Invalid declaration entries are
    ignored with an optional ``capability_warnings`` diagnostic while valid
    siblings and the built-in defaults remain active.
    An unlisted wrapper (``time``, ``stdbuf``, ``setsid``, ``busybox``) or
    any other unknown binary is refused with a reason that names that
    basename.

    Host-absolute: any path-like token starting with ``/`` or ``~`` that is
    not sandbox-relative (relative tokens only) and not on the explicit
    interpreter allowlist ``_ALLOWED_ABSOLUTE_INTERPRETERS`` (currently empty
    — ``/usr/bin/python3`` is refused; use a PATH-resolved interpreter).
    ``NAME=value`` env-assignment prefixes have their names and values checked
    after wrapper peel, including prefixes on the runner after ``cd ... &&``.
    ``PATH`` and ``PYTHONHOME`` always refuse because they rebind the approved
    implementation. Every ``PYTHONPATH`` entry must be non-empty,
    repository-relative, and free of parent-directory escapes. Other values
    are checked as path candidates (colon-split, then nested ``KEY=value``).
    The POSIX temp root
    (``/tmp``, ``/tmp/``) is allowed only as an env-assignment VALUE
    (``TMPDIR=/tmp``) so the hermetic constructor is not refused; children
    such as ``/tmp/pytest-of-daniel``, bare ``/tmp``, and attached flags
    (``--basetemp=/tmp``) stay refused. GNU attached flags
    (``--rootdir=/Users/x``, ``--override-ini=cache_dir=/Users/x``) are not
    env assignments; after the first ``=`` every nested ``KEY=value`` and
    colon-separated part is checked with ``_is_absolute_path_like``. The same
    walk runs on every resolved payload segment after wrapper peel, so
    ``bash -c 'pytest /Users/x'`` and ``sh -c 'pytest --rootdir=/Users/x'``
    refuse. The same walk and the terminal runner decision also run over
    the tokenized payload of a code-string flag (``-c`` / concatenated
    ``-c…``) on any basename, so ``python3 -c 'pytest /Users/x'``,
    ``busybox sh -c 'pytest /Users/x'``, and ``ash -c true`` refuse.
    Wrapper peel also judges every option value it consumes
    (``env -C``/``--chdir``, ``sudo -D``/``--chdir``/``-R``/``--chroot``,
    ``xargs -a``/``--arg-file``, attached ``=`` forms, and concatenated short
    forms) with that path-candidate walk and never POSIX-temp allowlists
    those dests; the allowlist is for env-assignment VALUES only. Reviewed
    Selector options are partitioned by semantics. Filesystem-valued ``uv``
    options require contained repository-relative paths. ``npx --package`` /
    ``-p`` and ``uv run --with`` / ``--with-editable`` require a package
    identity present in the effective runner capability; a declared identity
    may carry an ordinary version constraint but not a path or direct source.
    Registry, index, find-links, and user-config source redirects always
    refuse, including otherwise well-formed HTTP(S) values.

    Shell containment: expansion, redirection, multiline commands, and command
    separators are refused even when the first command is approved. The sole
    connector forms are one relative ``cd`` prefix joined to the runner by
    ``&&``, or an approved runner's ``--version`` query followed by exactly
    ``test -s PATH`` and optionally ``grep -Fq FIXED_STRING PATH``. The latter
    checks one contained repository-relative artifact. Punctuation-aware
    tokenization recognizes connectors even when attached to an adjacent
    token, and every other later command remains refused.
    """
    try:
        held = _required_remote_test_cmd(text)
        capability, capability_warning = _required_remote_test_capability(repository_root)
        if capability_warning is not None and capability_warnings is not None:
            capability_warnings.append(capability_warning)
        argv = _tokenize_remote_test_command(held)
        command = _normalize_remote_test_command(held, capability, argv)
        _validate_remote_test_command_structure(command)
        _validate_remote_test_command_runner(command)
    except _RemoteTestCmdRefuse as exc:
        return "refuse", str(exc)
    return "ok", ""


def resolve_dispatch_test_cmd(
    downstream: Any,
    *,
    lane_id: str = "",
    lane_row_value: Any = None,
) -> tuple[str | None, dict[str, Any] | None]:
    """Arbitrate supplied commands with the lane row as the single owner.

    This function does not inspect changed files or establish test coverage.

    - Row held: use the row. If the carrier is also held and differs,
      return a typed warning fingerprinting both values without exposing them.
    - Row cleared: use the empty string so a deliberate clear is expressible.
    - Row not supplied: use the carrier (held, cleared, or absent).
    - Carrier not supplied + row held: silent recovery (no false discard alarm).
    """
    row_state, row_text = classify_test_cmd(lane_row_value)
    carrier_state, carrier_text = classify_test_cmd(downstream)
    if row_state == _TEST_CMD_HELD:
        warning = None
        if carrier_state == _TEST_CMD_HELD and carrier_text != row_text:
            held_len, held_digest = _command_fingerprint(row_text)
            discarded_len, discarded_digest = _command_fingerprint(carrier_text)
            warning = {
                "type": TEST_CMD_DISCARDED_WARNING,
                "lane_id": lane_id,
                "held_len": held_len,
                "held_digest": held_digest,
                "discarded_len": discarded_len,
                "discarded_digest": discarded_digest,
            }
        return row_text, warning
    if row_state == _TEST_CMD_CLEARED:
        return "", None
    if carrier_state == _TEST_CMD_HELD:
        return carrier_text, None
    if carrier_state == _TEST_CMD_CLEARED:
        return "", None
    return None, None
