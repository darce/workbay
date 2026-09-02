#!/usr/bin/env python3
"""PreToolUse(Bash) hook: refuse raw git lifecycle commands, name the primitive.

Branch and worktree lifecycle in this repo runs through ``make`` primitives
(``Makefile.d/lifecycle.mk``). They enforce the branch-naming grammar, provision
the worktree ``.venv`` with editable installs, project handoff state, and keep
the lane row and the git ref reconciled. Raw ``git commit`` / ``git branch`` /
``git worktree add`` skip all of it, and nothing complains at the time -- the
damage surfaces later as a non-conforming branch the remote push guard refuses,
a worktree with no interpreter, or a lane row stranded from its ref.

A rule that depends on an agent remembering it is a buffer, not a drain: this
hook is the drain. It refuses the raw command before it runs and names the
primitive that does the same job correctly.

Contract (Claude Code + VS Code harnesses):
    stdin  : JSON payload with tool_name and tool_input.command
    args   : none
    stderr : BLOCKED message naming the primitive when a raw intent is detected
             (exit-2 PreToolUse feedback is read from stderr; stdout is dropped)
    exit 0 : allow
    exit 2 : block

Escape hatch for the rare case a primitive genuinely cannot express the
operation (salvage, conflict surgery): prepend
``export WORKBAY_ALLOW_RAW_GIT_LIFECYCLE=1 ; `` to the command text — the hook
reads the command, not the shell env of the tool call. Every bypassed lifecycle
segment is audit-logged to ``.task-state/lifecycle_guard_bypass.jsonl``.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import shlex
import sys
from pathlib import Path

_BYPASS_ENV = "WORKBAY_ALLOW_RAW_GIT_LIFECYCLE"
_BYPASS_TOKEN = f"{_BYPASS_ENV}=1"

# Shell operators that separate one command from the next. Splitting must run
# on the RAW string before shlex: shlex glues `;` to the preceding word and
# swallows newlines as whitespace, so token-level splitting silently fused
# `export X=y; git commit` into one unscanned export-led segment. Same stage
# grammar as _bash_isolation_guard._iter_stages, plus `\n`.
_STAGE_SPLIT = re.compile(r"(\|\||&&|\||;|&(?!>)|\n)")
_STAGE_SEPARATORS = frozenset({"||", "&&", "|", ";", "&"})

# git global options that consume the following token as their value. Missing
# one of these would make the scanner read the value as the subcommand.
_GIT_GLOBAL_OPTS_WITH_VALUE = frozenset(
    {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path", "--super-prefix"}
)

# `git branch` flags that make the invocation a query, never a creation.
_BRANCH_QUERY_FLAGS = frozenset(
    {
        "-l",
        "--list",
        "-a",
        "--all",
        "-r",
        "--remotes",
        "-v",
        "-vv",
        "--verbose",
        "--show-current",
        "--contains",
        "--no-contains",
        "--merged",
        "--no-merged",
        "--points-at",
        "--format",
        "--sort",
        "-i",
        "--ignore-case",
        "--column",
        "--no-column",
        "--color",
        "--no-color",
        "--abbrev",
        "--no-abbrev",
    }
)

# `git branch` flags that consume the next token as a value.
_BRANCH_OPTS_WITH_VALUE = frozenset(
    {
        "--contains",
        "--no-contains",
        "--merged",
        "--no-merged",
        "--points-at",
        "--format",
        "--sort",
        "-u",
        "--set-upstream-to",
        "--abbrev",
    }
)

_BRANCH_DELETE_FLAGS = frozenset({"-d", "-D", "--delete"})
_BRANCH_MOVE_FLAGS = frozenset({"-m", "-M", "--move", "-c", "-C", "--copy"})


def _remedy(primitive: str, why: str) -> str:
    return f"{why}\n  Use instead:  {primitive}"


_COMMIT_REMEDY = _remedy(
    'make slice-commit TASK=<task-ref> MSG="<message>"',
    "Raw `git commit` skips slice bookkeeping and the handoff projection.",
)
_WORKTREE_ADD_REMEDY = _remedy(
    'make task-start TASK=<task-ref> OBJECTIVE="<objective>"',
    "Raw `git worktree add` skips branch-grammar validation and leaves the "
    "worktree with no provisioned .venv (no editable installs).",
)
_BRANCH_CREATE_REMEDY = _remedy(
    'make task-start TASK=<task-ref> OBJECTIVE="<objective>"',
    "Raw branch creation skips the branch-naming grammar. A non-conforming "
    "branch is only refused later, on the remote push, with no artifacts.",
)
_BRANCH_DELETE_REMEDY = _remedy(
    'make task-finish TASK=<task-ref>   (or: make lane-reap REAP_ARGS="--apply")',
    "Raw branch deletion strands the lane row from its ref; they must be "
    "reconciled together.",
)
_WORKTREE_REMOVE_REMEDY = _remedy(
    'make task-finish TASK=<task-ref>   (or: make lane-reap REAP_ARGS="--apply --reclaim-worktrees")',
    "Raw worktree removal strands the lane row and skips archival.",
)
_BRANCH_MOVE_REMEDY = _remedy(
    'make task-start TASK=<task-ref> OBJECTIVE="<objective>"',
    "Renaming a branch cannot repair a name: the grammar is enforced at "
    "creation. Start the correctly-named task and move the work onto it.",
)


def _split_segments(command: str) -> list[list[str]]:
    """Split the raw command into per-command token lists on shell operators."""
    segments: list[list[str]] = []
    for raw_stage in _STAGE_SPLIT.split(command):
        stage = raw_stage.strip()
        if not stage or stage in _STAGE_SEPARATORS:
            continue
        try:
            tokens = shlex.split(stage, comments=False, posix=True)
        except ValueError:
            # Unbalanced quotes: fall back to whitespace tokens so an
            # unparseable stage cannot silently bypass the guard.
            tokens = stage.split()
        if tokens:
            segments.append(tokens)
    return segments


def _strip_env_prefix(tokens: list[str]) -> list[str]:
    """Drop leading VAR=value assignments and `env`/`command` wrappers."""
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in {"env", "command", "builtin", "exec", "time", "nohup"}:
            index += 1
            continue
        if "=" in token and not token.startswith("-") and "/" not in token.split("=", 1)[0]:
            index += 1
            continue
        break
    return tokens[index:]


def _is_git(token: str) -> bool:
    return token == "git" or token.endswith("/git")


def _parse_git_subcommand(tokens: list[str]) -> tuple[str | None, list[str]]:
    """Return (subcommand, remaining args) skipping git's global options."""
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("-"):
            return token, tokens[index + 1 :]
        if token in _GIT_GLOBAL_OPTS_WITH_VALUE:
            index += 2
            continue
        index += 1
    return None, []


def _branch_intent(args: list[str]) -> str | None:
    """Classify a `git branch` invocation as create / delete / move / query."""
    if any(flag in args for flag in _BRANCH_DELETE_FLAGS):
        return "delete"
    if any(flag in args for flag in _BRANCH_MOVE_FLAGS):
        return "move"
    if any(flag in args for flag in _BRANCH_QUERY_FLAGS):
        return None
    # A positional operand with no query flag means creation.
    index = 0
    while index < len(args):
        token = args[index]
        if token == "--":
            index += 1
            continue
        if token.startswith("-"):
            if token in _BRANCH_OPTS_WITH_VALUE:
                index += 2
                continue
            index += 1
            continue
        return "create"
    return None


def _violation(segment: list[str]) -> str | None:
    """Return a remedy message when this command is a raw lifecycle intent."""
    tokens = _strip_env_prefix(segment)
    if not tokens or not _is_git(tokens[0]):
        return None
    subcommand, args = _parse_git_subcommand(tokens)
    if subcommand is None:
        return None

    if subcommand == "commit":
        if "--dry-run" in args:
            return None
        return _COMMIT_REMEDY

    if subcommand == "worktree":
        action = next((a for a in args if not a.startswith("-")), None)
        if action == "add":
            return _WORKTREE_ADD_REMEDY
        if action in {"remove", "prune"}:
            return _WORKTREE_REMOVE_REMEDY
        return None

    if subcommand == "branch":
        intent = _branch_intent(args)
        if intent == "create":
            return _BRANCH_CREATE_REMEDY
        if intent == "delete":
            return _BRANCH_DELETE_REMEDY
        if intent == "move":
            return _BRANCH_MOVE_REMEDY
        return None

    if subcommand == "checkout" and ("-b" in args or "-B" in args):
        return _BRANCH_CREATE_REMEDY

    if subcommand == "switch" and ("-c" in args or "-C" in args or "--create" in args):
        return _BRANCH_CREATE_REMEDY

    return None


def scan_command(command: str) -> str | None:
    """Return a remedy message for the first raw lifecycle intent found."""
    remedy, _bypassed = scan_command_with_bypass(command)
    return remedy


def scan_command_with_bypass(command: str) -> tuple[str | None, list[str]]:
    """Return (remedy, bypassed lifecycle segments) for the command.

    A standalone ``export WORKBAY_ALLOW_RAW_GIT_LIFECYCLE=1`` segment opts the
    REST of the command out of enforcement; segments before it are still
    scanned, so the export cannot rescue a violation retroactively. Bypassed
    lifecycle segments are returned for audit logging, not silence.
    """
    if not command or not command.strip():
        return None, []
    bypass_active = False
    bypassed: list[str] = []
    for segment in _split_segments(command):
        if segment[0] == "export" and _BYPASS_TOKEN in segment[1:]:
            bypass_active = True
            continue
        if bypass_active:
            if _violation(segment) is not None:
                bypassed.append(" ".join(segment))
            continue
        remedy = _violation(segment)
        if remedy is not None:
            return remedy, bypassed
    return None, bypassed


def _log_bypass(command: str, bypassed: list[str]) -> None:
    """Append a bypass audit record; best-effort, never breaks the bypass."""
    record = {
        "event": "bash_lifecycle_bypass",
        "bypass_var": _BYPASS_ENV,
        "command": command,
        "bypassed_segments": bypassed,
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    root = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
    try:
        state_dir = Path(root) / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / "lifecycle_guard_bypass.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError:
        pass


def main() -> int:
    if os.environ.get(_BYPASS_ENV) == "1":
        return 0
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(payload, dict):
        return 0
    # Cursor registers this guard under its Shell tool (harness-protocol.yaml
    # cursor_matcher) with no payload-rewriting adapter in between.
    if payload.get("tool_name", payload.get("toolName")) not in {"Bash", "Shell"}:
        return 0
    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        return 0
    command = tool_input.get("command")
    if not isinstance(command, str):
        return 0

    remedy, bypassed = scan_command_with_bypass(command)
    if bypassed:
        _log_bypass(command, bypassed)
    if remedy is None:
        return 0

    print(
        "BLOCKED: raw git lifecycle command. This repo drives branch and "
        "worktree lifecycle through make primitives.\n\n"
        f"{remedy}\n\n"
        "See Makefile.d/lifecycle.mk. If a primitive genuinely cannot express "
        f"this operation, re-run with `export {_BYPASS_ENV}=1 ; ` prepended to "
        "the command text (the hook reads the command, not your shell env).",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
