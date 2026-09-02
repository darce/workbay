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
from pathlib import PurePosixPath
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
    {"sh", "bash", "dash", "ksh", "zsh", "shell"}
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
_REMOTE_TEST_CMD_RUNNER_BASENAMES = frozenset({"pytest"})
_REMOTE_TEST_CMD_INTERPRETER_BASENAMES = frozenset({"python", "python3"})
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
# Sandbox-relative allowlist: tokens that do not start with '/' or '~'.
# Absolute interpreters such as /usr/bin/python3 are refused unless added
# here; remote commands must use a PATH-resolved interpreter (python3, pytest).
_ALLOWED_ABSOLUTE_INTERPRETERS: frozenset[str] = frozenset()


def _posix_basename(token: str) -> str:
    if token in {":", ".", ".."}:
        return token
    return PurePosixPath(token).name or token


def _is_absolute_path_like(token: str) -> bool:
    return token.startswith("/") or token.startswith("~")


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
    if ":" in value:
        for part in value.split(":"):
            if part and part != value:
                for nested in _path_candidates_from_env_value(part, _depth=_depth + 1):
                    add(nested)
    if "=" in value:
        for part in value.split("="):
            if part and part != value:
                for nested in _path_candidates_from_env_value(part, _depth=_depth + 1):
                    add(nested)
    return out


class _RemoteTestCmdRefuse(Exception):
    """Fail-closed remote test-cmd classification (wrapper peel)."""


def _normalized_argv(tokens: list[str]) -> list[str]:
    argv = [token.rstrip(";") for token in tokens]
    return [token for token in argv if token]


def _wrapper_unresolved_reason(held: str) -> str:
    return f"test_cmd wrapper payload cannot be resolved: {held!r}"


def _wrapper_noop_reason(held: str) -> str:
    return f"test_cmd is a no-op: {held!r}"


def _require_option_value(tail: list[str], index: int, *, held: str) -> None:
    if index + 1 >= len(tail):
        raise _RemoteTestCmdRefuse(_wrapper_unresolved_reason(held))


def _attached_or_concatenated_option_value(token: str, names: frozenset[str]) -> str | None:
    """Return the value of ``--name=value`` or concatenated ``-Xvalue``."""
    if "=" in token:
        name, value = token.split("=", 1)
        if name in names:
            return value
        return None
    if token.startswith("--"):
        return None
    for name in names:
        if len(name) == 2 and name[0] == "-" and token.startswith(name) and token != name:
            return token[len(name) :]
    return None


def _refuse_host_absolute_option_value(value: str, *, held: str) -> None:
    """Judge peeler-consumed option values without the POSIX temp-root allowlist."""
    for candidate in _path_candidates_from_env_value(value):
        if _is_absolute_path_like(candidate):
            _, reason = _host_absolute_reason(candidate, held)
            raise _RemoteTestCmdRefuse(reason)


def _advance_past_value_option(
    tail: list[str], index: int, names: frozenset[str], *, held: str
) -> int | None:
    """Consume a value option after judging its dest; return the next index."""
    token = tail[index]
    if token in names:
        _require_option_value(tail, index, held=held)
        _refuse_host_absolute_option_value(tail[index + 1], held=held)
        return index + 2
    attached = _attached_or_concatenated_option_value(token, names)
    if attached is not None:
        _refuse_host_absolute_option_value(attached, held=held)
        return index + 1
    return None


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
        if token in {"-k", "--kill-after"}:
            _require_option_value(tail, index, held=held)
            index += 2
            continue
        if token.startswith("--kill-after="):
            index += 1
            continue
        if token in {"-s", "--signal"}:
            _require_option_value(tail, index, held=held)
            index += 2
            continue
        if token.startswith("--signal="):
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
        try:
            inner = shlex.split(tail[0], posix=True)
        except ValueError:
            raise _RemoteTestCmdRefuse(f"test_cmd is unparseable: {held!r}") from None
        return _normalized_argv(inner)
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
    try:
        inner = shlex.split(payload, posix=True)
    except ValueError:
        raise _RemoteTestCmdRefuse(f"test_cmd is unparseable: {held!r}") from None
    inner = _normalized_argv(inner)
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


def _flatten_segment(segment: list[str], *, held: str, depth: int) -> list[str]:
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
        return kept + _flatten_wrappers(inner, held=held, depth=depth + 1)
    if base in _REMOTE_TEST_CMD_PREFIX_WRAPPER_BASENAMES:
        inner = _peel_prefix_segment(base, rest, held=held)
        return kept + _flatten_wrappers(inner, held=held, depth=depth + 1)
    return kept + rest


def _flatten_wrappers(argv: list[str], *, held: str, depth: int = 0) -> list[str]:
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
        out.extend(_flatten_segment(argv[index:end], held=held, depth=depth))
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


def _is_noop_payload_token(token: str) -> bool:
    base = _posix_basename(token)
    if base in _REMOTE_TEST_CMD_NOOP_BASENAMES or base == "exit":
        return True
    return token.isdigit()


def _unapproved_runner_reason(basename: str, held: str) -> tuple[str, str]:
    return (
        "refuse",
        f"test_cmd first payload is not an approved test runner ({basename}): {held!r}",
    )


def _concatenated_code_string_payload(token: str) -> str | None:
    """Return the payload of concatenated ``-c…`` / ``-c=…``, else None."""
    if token.startswith("--") or not token.startswith("-c") or token == "-c":
        return None
    payload = token[2:]
    if payload.startswith("="):
        payload = payload[1:]
    return payload


def _interpreter_selects_runner_module(cmd_argv: list[str], interpreter_token: str) -> bool:
    """True when the interpreter argv selects the approved runner as ``-m``."""
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
            return _posix_basename(rest[index + 1]) in _REMOTE_TEST_CMD_RUNNER_BASENAMES
        if token.startswith("-m") and token != "-m":
            module = token[2:]
            if module.startswith("="):
                module = module[1:]
            return bool(module) and _posix_basename(module) in _REMOTE_TEST_CMD_RUNNER_BASENAMES
        index += 1
    return False


def _is_approved_remote_test_runner(payload: list[str], cmd_argv: list[str]) -> bool:
    """True when the first remaining payload is pytest or python -m pytest."""
    first = payload[0]
    first_base = _posix_basename(first)
    if first_base in _REMOTE_TEST_CMD_RUNNER_BASENAMES:
        return True
    if first_base in _REMOTE_TEST_CMD_INTERPRETER_BASENAMES:
        return _interpreter_selects_runner_module(cmd_argv, first)
    return False


def _host_absolute_reason(candidate: str, held: str) -> tuple[str, str]:
    return "refuse", f"test_cmd contains host-absolute path {candidate!r}: {held!r}"


def _host_absolute_refusal(cmd_argv: list[str], held: str) -> tuple[str, str] | None:
    """Return a host-absolute refuse pair when any argv token is path-like."""
    for token in cmd_argv:
        assignment = _split_env_assignment(token)
        if assignment is not None:
            for candidate in _path_candidates_from_env_value(assignment[1]):
                if candidate in _ALLOWED_POSIX_TEMP_ROOTS:
                    continue
                if _is_absolute_path_like(candidate):
                    return _host_absolute_reason(candidate, held)
            continue
        # Flags such as --rootdir=/Users/x start with '-' so they are not
        # env assignments; after the first '=' walk nested KEY=value and
        # colon-separated parts (attached flags never get the /tmp allowlist).
        if token.startswith("-") and "=" in token:
            _, value = token.split("=", 1)
            for candidate in _path_candidates_from_env_value(value):
                if _is_absolute_path_like(candidate):
                    return _host_absolute_reason(candidate, held)
            continue
        if not _is_absolute_path_like(token):
            continue
        if token in _ALLOWED_ABSOLUTE_INTERPRETERS:
            continue
        return _host_absolute_reason(token, held)
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


def _tokenize_code_string_payload(payload: str, *, held: str, depth: int) -> list[str]:
    """Split a code-string payload and peel nested wrappers (bounded)."""
    try:
        inner = shlex.split(payload, posix=True)
    except ValueError:
        raise _RemoteTestCmdRefuse(f"test_cmd is unparseable: {held!r}") from None
    inner = _normalized_argv(inner)
    if not inner:
        raise _RemoteTestCmdRefuse(_wrapper_noop_reason(held))
    inner = _flatten_wrappers(inner, held=held, depth=depth + 1)
    return _apply_code_string_payloads(inner, held=held, depth=depth + 1)


def _keep_outer_after_code_string(segment: list[str], payload: list[str]) -> bool:
    """True when ``-c`` is a runner config flag, not a quoting layer."""
    if not payload or not _is_approved_remote_test_runner(payload, segment):
        return False
    first = payload[0]
    if _posix_basename(first) in _REMOTE_TEST_CMD_INTERPRETER_BASENAMES:
        return not _interpreter_code_string_mode(segment, first)
    return True


def _apply_code_string_segment(segment: list[str], *, held: str, depth: int) -> list[str]:
    """Open a ``-c`` payload on any basename and judge its contents."""
    raw = _find_code_string_payload(segment, held=held)
    if raw is None:
        return segment
    inner = _tokenize_code_string_payload(raw, held=held, depth=depth)
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
    if _keep_outer_after_code_string(rest, rest_payload):
        return segment
    return env_kept + inner


def _apply_code_string_payloads(argv: list[str], *, held: str, depth: int = 0) -> list[str]:
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
        out.extend(_apply_code_string_segment(argv[index:end], held=held, depth=depth))
        index = end
    return out


def classify_remote_test_cmd(text: str | None) -> tuple[str, str]:
    """Classify a resolved test command for remote implement dispatch.

    Pure: no I/O. Tokenizes with posix ``shlex`` (``ValueError`` → refuse).
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
    basename must be ``pytest``, or an interpreter (``python`` / ``python3``,
    including the relative ``../../.venv/bin/python`` form producers emit)
    whose argument vector selects that runner as a module (``-m pytest``).
    An unlisted wrapper (``time``, ``stdbuf``, ``setsid``, ``busybox``) or
    any other unknown binary is refused with a reason that names that
    basename.

    Host-absolute: any path-like token starting with ``/`` or ``~`` that is
    not sandbox-relative (relative tokens only) and not on the explicit
    interpreter allowlist ``_ALLOWED_ABSOLUTE_INTERPRETERS`` (currently empty
    — ``/usr/bin/python3`` is refused; use a PATH-resolved interpreter).
    ``NAME=value`` env-assignment prefixes have their values checked as path
    candidates (colon-split, then nested ``KEY=value``). The POSIX temp root
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
    those dests; the allowlist is for env-assignment VALUES only.
    """
    state, held = classify_test_cmd(text)
    if state != _TEST_CMD_HELD or not held:
        return "refuse", f"test_cmd is not held (state={state}, value={text!r})"
    try:
        argv = shlex.split(held, posix=True)
    except ValueError:
        return "refuse", f"test_cmd is unparseable: {held!r}"
    argv = _normalized_argv(argv)
    if not argv:
        return "refuse", f"test_cmd is a no-op: {held!r}"

    env_values: list[str] = []
    cmd_index = 0
    for cmd_index, token in enumerate(argv):
        assignment = _split_env_assignment(token)
        if assignment is None:
            break
        env_values.append(assignment[1])
    else:
        cmd_index = len(argv)
    cmd_argv = argv[cmd_index:]

    for value in env_values:
        for candidate in _path_candidates_from_env_value(value):
            if candidate in _ALLOWED_POSIX_TEMP_ROOTS:
                continue
            if _is_absolute_path_like(candidate):
                return _host_absolute_reason(candidate, held)

    if not cmd_argv:
        return "refuse", f"test_cmd is a no-op: {held!r}"

    try:
        cmd_argv = _flatten_wrappers(cmd_argv, held=held)
        cmd_argv = _apply_code_string_payloads(cmd_argv, held=held)
    except _RemoteTestCmdRefuse as exc:
        return "refuse", str(exc)

    if not cmd_argv:
        return "refuse", f"test_cmd is a no-op: {held!r}"

    payload = _executable_payload_tokens(cmd_argv)
    if _payload_is_noop(payload):
        return "refuse", f"test_cmd is a no-op: {held!r}"
    first_payload_base = _posix_basename(payload[0])

    host_absolute = _host_absolute_refusal(cmd_argv, held)
    if host_absolute is not None:
        return host_absolute
    if not _is_approved_remote_test_runner(payload, cmd_argv):
        return _unapproved_runner_reason(first_payload_base, held)
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
