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

Contract (Claude Code + VS Code + Cursor + Codex harnesses):
    stdin  : JSON hook payload; the shared adapter ``_shell_tool_payload.py``
             finds the shell call (Bash, Shell, exec_command, ...), its
             command and its working directory
    args   : none
    stderr : BLOCKED message naming the primitive when a raw intent is detected
             (exit-2 PreToolUse feedback is read from stderr; stdout is dropped)
    exit 0 : allow
    exit 2 : block

Operator guidance: in the rare case a primitive cannot express an operation,
the operator may authorize the command-text ``export
WORKBAY_ALLOW_RAW_GIT_LIFECYCLE=1`` segment or set
``WORKBAY_ALLOW_RAW_GIT_LIFECYCLE=1`` in the hook process environment. Both
opt-ins are audit-logged to ``.task-state/lifecycle_guard_bypass.jsonl`` with a
``bypass_source`` field identifying ``command`` or ``env``.
"""

from __future__ import annotations

import datetime
from collections import namedtuple
import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
import time
import zlib
from pathlib import Path

_BYPASS_ENV = "WORKBAY_ALLOW_RAW_GIT_LIFECYCLE"
_BYPASS_TOKEN = f"{_BYPASS_ENV}=1"

_REMOTE_REAP_REMEDY = (
    "BLOCKED: raw remote_agent.sh reap cannot delete sandboxes (standalone delete unsupported "
    "since e0fd0ca33c6). Use: make vm-reap   (dry run)  /  make vm-reap APPLY=1"
)
# Stable refusal reasons of the reap pass [AGT-10].
_REAP_REASONS = {
    "raw_reap_direct": "the command runs remote_agent.sh reap",
    "raw_reap_unmodelled_form": (
        "the reap pass does not model this wrapper, substitution, eval, variable, stdin or compound "
        "form, and its words name remote_agent.sh reap"
    ),
}


def _reap_remedy(reason: str) -> str:
    return (
        f"{_REMOTE_REAP_REMEDY}\nreason: {reason} ({_REAP_REASONS[reason]})\n"
        f"Operator override (audit-logged to .task-state/lifecycle_guard_bypass.jsonl): "
        f"export {_BYPASS_TOKEN}; <command>"
    )

# Working directory of the shell call, from the payload (``workdir`` / ``cwd``).
# Set by ``main()``; ``None`` keeps the process cwd [AGT-08].
_payload_workdir: Path | None = None

_SHELL_TOOL_PAYLOAD_PATH = Path(__file__).resolve().parent / "_shell_tool_payload.py"


def _shell_tool_payload():
    """Sibling shell-tool payload adapter loaded by path, or None when absent.

    Root and payload twins share basenames, so the module is keyed by its file
    location. A standalone copy of this guard (pinned by
    ``test_guard_loads_policy_lazily_from_sibling_path``) has no sibling
    adapter and keeps the pre-adapter parse in ``_shell_command``.
    """
    if not _SHELL_TOOL_PAYLOAD_PATH.is_file():
        return None
    name = f"_shell_tool_payload_{zlib.crc32(str(_SHELL_TOOL_PAYLOAD_PATH).encode()):08x}"
    module = sys.modules.get(name)
    if module is None:
        spec = importlib.util.spec_from_file_location(name, _SHELL_TOOL_PAYLOAD_PATH)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {_SHELL_TOOL_PAYLOAD_PATH}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(name, None)
            raise
    return module


def _shell_command(payload: dict) -> tuple[str, Path | None] | None:
    """``(command, workdir)`` of the payload's shell call, or None [SECD-03]."""
    adapter = _shell_tool_payload()
    if adapter is not None:
        call = adapter.extract_shell_call(payload)
        return None if call is None else (call.command, call.workdir)
    # Standalone copy without the adapter: the pre-adapter Bash/Shell parse.
    if payload.get("tool_name", payload.get("toolName")) not in {"Bash", "Shell"}:
        return None
    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        return None
    command = tool_input.get("command")
    if not isinstance(command, str):
        return None
    return command, None


def _workspace_root() -> Path:
    """Project root: ``CLAUDE_PROJECT_DIR``, else the shell call's working
    directory, else the process cwd."""
    return Path(os.environ.get("CLAUDE_PROJECT_DIR") or _payload_workdir or os.getcwd())

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

# `git merge` options that consume the next token as a value.
_MERGE_OPTS_WITH_VALUE = frozenset(
    {
        "-m",
        "--message",
        "-F",
        "--file",
        "-s",
        "--strategy",
        "-X",
        "--strategy-option",
        "--cleanup",
        "--into-name",
    }
)
# Merge invocations that manage an in-progress merge, never start one.
_MERGE_STATE_FLAGS = frozenset({"--abort", "--continue", "--quit"})
# Branch prefixes whose merge requires an adjudication artifact
# (docs/workbay/rules/branch-pipeline.md section 7): the merge-gate primitive
# fences the artifact to the subject tip, so a stale or absent adjudication
# refuses here, before the merge exists, not after.
_GATED_MERGE_PREFIXES = ("feature/",)
_ADJUDICATION_DIR = Path(".task-state") / "review-adjudication"
_ARTIFACT_BRANCH_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _branch_file_slug(branch: str) -> str:
    return _ARTIFACT_BRANCH_SLUG_RE.sub("-", branch).strip("-")


def _remedy(primitive: str, why: str) -> str:
    return f"{why}\n  Use instead:  {primitive}"


_COMMIT_REMEDY = _remedy(
    'For on-main maintenance with no registered task, first run '
    'make maint-start TASK=MAINT-... OBJECTIVE="<objective>".\n'
    '  With a registered task, run make slice-commit TASK=<task-ref> MSG="<message>" PATHS="<owned paths>"',
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
    "make task-finish TASK=<task-ref>\n"
    "For one lane: land it if it has not landed (land retires it in the same call): "
    'manage_worktree_lane(operation="land", task_ref=<task-ref>, lane_id=<lane-id>, '
    "expected_tip=<40-hex lane tip>) or, once it has landed, retire it (a dry run unless "
    'apply=True): manage_worktree_lane(operation="retire", task_ref=<task-ref>, '
    "lane_id=<lane-id>, apply=True, delete_merged_branch=True)\n"
    "Repo-wide sweeps are operator-only batch actions. Hand the operator this message "
    "instead of running them: mcp-workbay-orchestrator --workspace-root . lane-reap --apply",
    "Raw branch deletion strands the lane row from its ref; they must be reconciled together.",
)
_WORKTREE_REMOVE_REMEDY = _remedy(
    "make task-finish TASK=<task-ref>\n"
    "For one lane: land it if it has not landed (land retires it in the same call): "
    'manage_worktree_lane(operation="land", task_ref=<task-ref>, lane_id=<lane-id>, '
    "expected_tip=<40-hex lane tip>) or, once it has landed, retire it (a dry run unless "
    'apply=True): manage_worktree_lane(operation="retire", task_ref=<task-ref>, '
    "lane_id=<lane-id>, apply=True, delete_merged_branch=True)\n"
    "Repo-wide sweeps are operator-only batch actions. Hand the operator this message "
    "instead of running them: mcp-workbay-orchestrator --workspace-root . lane-reap --apply "
    "(worktrees: add --reclaim-worktrees)",
    "Raw worktree removal strands the lane row and skips archival.",
)
_BRANCH_MOVE_REMEDY = _remedy(
    'make task-start TASK=<task-ref> OBJECTIVE="<objective>"',
    "Renaming a branch cannot repair a name: the grammar is enforced at "
    "creation. Start the correctly-named task and move the work onto it.",
)

_LAND_REMEDY = (
    'If this is a lane branch (manage_worktree_lane(operation="list", task_ref=<task-ref>) '
    "shows its row), land it instead. land merges, writes the landing receipt and retires "
    "the lane in one call:\n"
    'MCP: manage_worktree_lane(operation="land", task_ref=<task-ref>, lane_id=<lane-id>, '
    "expected_tip=<40-hex lane tip>)\n"
    "CLI: python -m workbay_orchestrator_mcp.orchestration.lane_land --workspace-root . "
    "--task <task-ref> --lane <lane-id> --expected-tip <40-hex lane tip>"
)

_LANDING_PROTOCOL_MODULE = "_lifecycle_guard_harness_protocol"


def _landing_requirements(root: Path) -> str:
    """Render landing requirements from the contract without loading on allow."""
    try:
        module = sys.modules.get(_LANDING_PROTOCOL_MODULE)
        if module is None:
            helper = Path(__file__).resolve().parent / "_harness_protocol.py"
            spec = importlib.util.spec_from_file_location(_LANDING_PROTOCOL_MODULE, helper)
            if spec is None or spec.loader is None:
                raise ImportError(f"cannot load {helper}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[_LANDING_PROTOCOL_MODULE] = module
            try:
                spec.loader.exec_module(module)
            except Exception:
                sys.modules.pop(_LANDING_PROTOCOL_MODULE, None)
                raise
        policy = module.load_landing_policy(root)
    except Exception:  # noqa: BLE001 -- missing policy is surfaced as unavailable
        policy = None

    if policy is None:
        return (
            "Landing requirements are unavailable: the landing: block of "
            "docs/workbay/contracts/harness-protocol.yaml is missing or unreadable, "
            "so land refuses with merge_refused (landing_policy_unavailable) until it is restored."
        )

    bullets: list[str] = []
    if policy.require_gate_receipt:
        bullets.append("- a passing gate receipt (a verified test_result) at the lane tip;")
    else:
        bullets.append("- land refuses with merge_refused (policy_unsupported:require_gate_receipt) under this policy.")
    if policy.require_review_verdict:
        bullets.append(
            "- land refuses with merge_refused (policy_unsupported:require_review_verdict) under this policy."
        )
    else:
        bullets.append("- a review verdict is not a precondition (reviews run after integration);")
    bullets.append(f"- review findings at {', '.join(policy.blocking_severities)} block a MERGE verdict.")
    heading = (
        "Landing requirements (the landing: block of "
        "docs/workbay/contracts/harness-protocol.yaml, "
        f"receipt_version {policy.receipt_version}):"
    )
    return "\n".join((heading, *bullets))


def _merge_targets(args: list[str]) -> list[str]:
    """All positional refs of a `git merge`; empty for state management."""
    if any(flag in args for flag in _MERGE_STATE_FLAGS):
        return []
    refs: list[str] = []
    index = 0
    while index < len(args):
        token = args[index]
        if token == "--":
            index += 1
            continue
        if token.startswith("-"):
            if token in _MERGE_OPTS_WITH_VALUE:
                index += 2
                continue
            index += 1
            continue
        refs.append(token)
        index += 1
    return refs


def _rev_parse_commit(ref: str) -> str | None:
    """Resolve any ref spelling to its commit sha, or None."""
    root = _workspace_root()
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def _is_ancestor_of_head(sha: str) -> bool | None:
    """True/False for a decided ancestry probe, None when the probe failed."""
    root = _workspace_root()
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", sha, "HEAD"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def _resolved_feature_tip(ref: str) -> str | None:
    """Map an opaque merge target (sha, FETCH_HEAD, tag) onto a feature tip."""
    root = _workspace_root()
    sha = _rev_parse_commit(ref)
    if sha is None:
        return None
    try:
        tips = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "for-each-ref",
                "--format=%(objectname) %(refname:lstrip=2)",
                "refs/heads/feature/",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if tips.returncode != 0:
        return None
    for line in tips.stdout.splitlines():
        tip, _, name = line.partition(" ")
        if tip == sha and name:
            return name
    return None


# Target spellings that carry no branch identity: raw shas and the *_HEAD
# pseudo-refs (FETCH_HEAD, MERGE_HEAD, ...). A named ref reveals what it is;
# these hide it, so they are the fail-closed class when they would land
# commits that are not already on HEAD.
_ANONYMOUS_TARGET = re.compile(r"^(?:[0-9a-f]{7,40}|[A-Z_]*HEAD)$")


def _anonymous_target_violation(ref: str) -> str | None:
    """Refuse an anonymous target that lands unreviewed, non-ancestor commits.

    Reached only after `_gated_subject` found no subject: the target matched
    no current feature tip. An ancestor of HEAD is a no-op merge and passes;
    an unresolvable ref passes (the merge itself will fail). Anything else is
    new work smuggled past the gate under a spelling with no name, including
    a failed ancestry probe — fail closed.
    """
    if not _ANONYMOUS_TARGET.match(ref):
        return None
    sha = _rev_parse_commit(ref)
    if sha is None:
        return None
    if _is_ancestor_of_head(sha) is True:
        return None
    return _remedy(
        "git merge <branch-name>  (after make merge-gate SUBJECT=<branch>)",
        f"Merging anonymous target `{ref}` would land commits that are not "
        "ancestors of HEAD and match no reviewed subject tip. Spell the merge "
        "as its branch name so the gate can check the adjudication artifact.\n\n"
        f"{_LAND_REMEDY}",
    )


def _gated_subject(ref: str) -> str | None:
    """Resolve a merge target to the gated subject branch it lands, or None.

    Spelling variants (`refs/heads/feature/x`, `origin/feature/x`,
    `refs/remotes/origin/feature/x`) name the subject directly; an opaque
    target (raw sha, FETCH_HEAD, tag) is gated when it resolves to the tip of
    a local feature branch — the merge lands that subject regardless of how
    the ref was spelled.
    """
    norm = ref
    for prefix in ("refs/heads/", "refs/remotes/"):
        if norm.startswith(prefix):
            norm = norm[len(prefix) :]
    if norm.startswith(_GATED_MERGE_PREFIXES):
        return norm
    marker = norm.find("/feature/")
    if marker != -1:
        return norm[marker + 1 :]
    return _resolved_feature_tip(ref)


def _adjudication_allows_merge(subject: str, target_sha: str | None) -> tuple[bool, str]:
    """Fail-closed check of the merge-gate artifact for a gated branch ref.

    The tip fence compares the artifact against ``target_sha`` — the commit
    the merge target actually resolves to — never against the local branch of
    the same name. `origin/feature/x` at an unreviewed tip must refuse even
    while a current MERGE artifact exists for local `feature/x`.
    """
    root = _workspace_root()
    artifact = root / _ADJUDICATION_DIR / (_branch_file_slug(subject) + ".json")
    try:
        record = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return False, "no readable adjudication artifact"
    if not isinstance(record, dict):
        return False, "adjudication artifact is not an object"
    if record.get("verdict") != "MERGE":
        return False, f"adjudicated verdict is {record.get('verdict')!r}, not MERGE"
    if record.get("subject") != subject:
        return False, f"adjudication is for {record.get('subject')!r}, not this subject"
    if target_sha is None:
        return False, "merge target did not resolve to a commit"
    if record.get("subject_tip") != target_sha:
        return False, "adjudication does not fence the commit this merge lands"
    return True, "ok"


def _merge_violation(args: list[str]) -> str | None:
    for ref in _merge_targets(args):
        subject = _gated_subject(ref)
        if subject is None:
            anonymous = _anonymous_target_violation(ref)
            if anonymous is not None:
                return anonymous
            continue
        allowed, why = _adjudication_allows_merge(subject, _rev_parse_commit(ref))
        if allowed:
            continue
        return _remedy(
            f'make review-adjudicate SUBJECT={subject} PATCH=<input patch> DOCS="<review docs>"'
            f"  then  make merge-gate SUBJECT={subject}",
            f"Merging `{ref}` (subject `{subject}`) without a current MERGE "
            f"adjudication ({why}). The pipeline requires the verdict "
            "artifact, fenced to the subject tip, before the merge exists.\n\n"
            f"{_LAND_REMEDY}",
        )
    return None


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
            tokens = [word.replace("'", "").replace('"', "") for word in stage.split()]
        if tokens:
            segments.append(tokens)
    return segments


def _reap_stages(command: str) -> list[str]:
    """Keep quoted operators for the shell that actually interprets them."""
    stages: list[str] = []
    start = 0
    quote = ""
    escaped = False
    for index, char in enumerate(command):
        if escaped:
            escaped = False
        elif char == "\\" and quote != "'":
            escaped = True
        elif quote:
            if char == quote:
                quote = ""
        elif char in "\"'":
            quote = char
        elif char in ";|&\n" and not (char == "&" and command[index + 1:index + 2] == ">"):
            stages.append(command[start:index])
            start = index + 1
    stages.append(command[start:])
    return [stage for stage in stages if stage.strip()]


def _reap_tokens(stage: str) -> list[str]:
    try:
        lexer = shlex.shlex(stage, posix=True, punctuation_chars="<>")
        lexer.whitespace_split = True
        lexer.commenters = ""
        return list(lexer)
    except ValueError:
        # Broken quoting must not turn reap into a different word (reap').
        return [word.replace("'", "").replace('"', "") for word in stage.split()]


def _scan_segments(command: str) -> list[list[str]]:
    """Segments for the git detections, with the legacy quoted-string pass.

    SSH remote text belongs only to the reap pass (``_reap_units``); never
    expose its quoted stages to the local git scanner.
    """
    segments: list[list[str]] = []
    for stage in _reap_stages(command):
        tokens = _reap_tokens(stage)
        if not tokens:
            continue
        segments.append(tokens)
        words = _strip_env_prefix(tokens)
        base = words[0].rsplit("/", 1)[-1] if words else ""
        flags = words[1:words.index("--")] if "--" in words else words[1:]
        shell_text = base in _SHELL_WRAPPERS and any(
            token.startswith("-") and not token.startswith("--") and "c" in token[1:]
            for token in flags
        )
        if base in {"ssh", "timeout"} or shell_text:
            # Shell/timeout wrappers are scanned recursively below. Splitting
            # their quoted bodies here would expose remote git as local git.
            continue
        # The legacy pass serves only git detections.
        segments.extend(part for part in _split_segments(stage) if part != tokens)
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


_SHELL_WRAPPERS = frozenset({"sh", "bash", "zsh", "dash", "ksh"})
_TIMEOUT_OPTS_WITH_VALUE = frozenset({"-k", "--kill-after", "-s", "--signal"})


def _unwrap_violation(tokens: list[str]) -> str | None:
    """Scan through `sh -c '...'` / `timeout N git ...` wrapper shells."""
    head = tokens[0]
    base = head.rsplit("/", 1)[-1]
    if base in _SHELL_WRAPPERS:
        for index, token in enumerate(tokens[1:], start=1):
            if token == "--":
                break
            if token.startswith("--"):
                continue
            if token.startswith("-") and "c" in token[1:]:
                if index + 1 < len(tokens):
                    for inner in _scan_segments(tokens[index + 1]):
                        remedy = _violation(inner)
                        if remedy is not None:
                            return remedy
                return None
        return None
    if base == "timeout":
        rest = tokens[1:]
        index = 0
        while index < len(rest) and rest[index].startswith("-"):
            if rest[index] in _TIMEOUT_OPTS_WITH_VALUE:
                index += 2
                continue
            index += 1
        # rest[index] is the duration; the wrapped command follows it.
        if index + 1 < len(rest):
            for inner in _scan_segments(shlex.join(rest[index + 1 :])):
                remedy = _violation(inner)
                if remedy is not None:
                    return remedy
        return None
    return None


def _ssh_remote_words(words: list[str]) -> list[str]:
    """Copy the routing guard's option/destination boundary, without imports."""
    value_options = {"-B", "-b", "-c", "-D", "-E", "-e", "-F", "-I", "-i", "-J", "-L",
                     "-l", "-m", "-O", "-o", "-P", "-p", "-Q", "-R", "-S", "-W", "-w"}
    index = 1
    destination = None
    while index < len(words):
        token = words[index]
        if token == "--":
            index += 1
            if destination is None:
                index += 1
            break
        if not token.startswith("-"):
            if destination is not None:
                break
            destination = token
            index += 1
            continue
        option = token[:2]
        index += 2 if option in value_options and token == option else 1
    return words[index:]


# The reap pass is deny-by-default [WEB-33]: a command that could run
# remote_agent.sh reap is admitted only when every executor in it is modelled
# and none of them runs the script with the reap subcommand. Anything it does
# not model -- an unknown program, a wrapper option form, eval, a substitution,
# a stdin script, a compound header -- is refused when the reap words appear.
_REAP_SCRIPT = "remote_agent.sh"
_REAP_DIRECT = "raw_reap_direct"
_REAP_UNMODELLED = "raw_reap_unmodelled_form"
_REAP_DEPTH_LIMIT = 4
# Reserved words and grouping that lead the simple command they open.
_REAP_RESERVED = frozenset({"!", "{", "}", "if", "then", "elif", "else", "fi", "do", "done", "while", "until", "esac"})
# Compound headers whose words the pass does not model as a command.
_REAP_OPAQUE_SYNTAX = frozenset({"for", "case", "select", "function", "coproc", "in", "[[", "(("})
# Builtins that run text built from their arguments (copied from the routing guard).
_REAP_TEXT_EXECUTORS = frozenset({"alias", "builtin", "eval", "source", "trap", "."})
# Execution wrappers (copied from the routing guard's _WRAPPERS).
_REAP_WRAPPERS = frozenset(
    {"caffeinate", "command", "doas", "exec", "nice", "nohup", "stdbuf", "sudo", "time", "timeout", "xargs"}
)
_REAP_ENV_VALUE_OPTIONS = frozenset({"-u", "--unset", "-C", "--chdir"})
_REAP_SHELL_VALUE_OPTIONS = frozenset({"-o", "+o", "-O", "+O", "--rcfile", "--init-file"})
# Programs that read or print their operands and never execute them; the
# sed, awk, rg and git forms that can execute are excluded in _reap_reader_inert.
_REAP_READERS = frozenset(
    {"grep", "egrep", "fgrep", "rg", "cat", "less", "more", "head", "tail", "wc", "echo", "printf",
     "sed", "awk", "gawk", "mawk", "nawk", "git", "ls", "true", ":", "test", "[", "sort", "uniq",
     "cut", "tr", "cd", "pwd"}
)
_REAP_GIT_VERBS = frozenset(
    {"blame", "cat-file", "commit", "describe", "diff", "grep", "log", "ls-files", "ls-tree", "reflog",
     "rev-list", "rev-parse", "shortlog", "show", "status"}
)
_REAP_DECLARERS = frozenset({"export", "declare", "local", "readonly", "typeset"})
_REAP_ASSIGNMENT = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)(?:\[[^\]]*\])?(\+?)=", re.ASCII)
_REAP_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*", re.ASCII)
# A GNU sed `e` command or `s///e` flag runs text as a shell command.
_REAP_SED_EXEC = re.compile(r"(?<![A-Za-z_])e(?![A-Za-z_])")
_REAP_WORD_SPLIT = re.compile(r"[\s;|&()<>`{}]+")


def _reap_flat_words(text: str) -> list[str]:
    """Words of text with every quoting level removed, split where a shell may split."""
    flat = text.replace("'", " ").replace('"', " ").replace("\\", "")
    return [word for word in _REAP_WORD_SPLIT.split(flat) if word]


def _has_reap_words(text: str) -> bool:
    """A remote_agent.sh word followed by a reap word, at any quoting depth."""
    words = _reap_flat_words(text)
    for index, word in enumerate(words):
        if word.rsplit("/", 1)[-1].lstrip("$") == _REAP_SCRIPT and "reap" in words[index + 1:]:
            return True
    return False


def _reap_evidence(argv: list[str], dynamic: list[bool], stdin: list[str], stdin_file: str | None) -> bool:
    """Whether an unmodelled executor could run remote_agent.sh reap from these words."""
    text = " ".join(argv)
    if _has_reap_words(text) or any(_has_reap_words(part) for part in stdin):
        return True
    if "reap" not in _reap_flat_words(text):
        return False
    # The script name may be hidden in an expansion or arrive as the stdin script.
    return any(dynamic) or (stdin_file or "").rsplit("/", 1)[-1] == _REAP_SCRIPT


# Immutable, per-root evidence and budget; recursive shell passes share both.
_ReapContext = namedtuple("_ReapContext", "evidence deadline")
_REAP_PARAMETER_DEFAULT = re.compile(r"\$\{(?:[A-Za-z_][A-Za-z0-9_]*|[0-9]+)(?::[-=+]|[-=+])([^{}]*)\}")


class _ReapLimit(ValueError):
    """The reap pass exhausted its lexical nesting or work budget."""


class _ReapDeadline(_ReapLimit):
    """The reap pass exhausted its monotonic work budget."""


def _reap_root_evidence(text: str) -> bool:
    if "reap" in _reap_flat_words(text):
        return True
    return any("reap" in re.split(r"[\s\"']+", match.group(1))
               for match in _REAP_PARAMETER_DEFAULT.finditer(text))


def _reap_check(context: _ReapContext | None) -> None:
    if context is not None and time.monotonic() >= context.deadline:
        raise _ReapDeadline("reap work deadline")


def _reap_match(text: str, index: int, opener: str = "(", closer: str = ")",
                nesting: int = 0, context: _ReapContext | None = None) -> int:
    """Index of the closer that balances an opener just before ``index``."""
    if nesting >= 64:
        raise _ReapLimit("reap lexical nesting")
    depth = 1
    while index < len(text):
        if index % 256 == 0:
            _reap_check(context)
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == "'":
            end = text.find("'", index + 1)
            if end < 0:
                raise ValueError("unbalanced quote")
            index = end + 1
            continue
        if char == '"':
            index = _reap_dquote_end(text, index + 1, nesting + depth, context)
            continue
        if char == "`":
            index = _reap_backtick_end(text, index + 1) + 1
            continue
        if char == "$" and text[index + 1:index + 2] == "(" and opener != "(":
            index = _reap_match(text, index + 2, nesting=nesting + 1, context=context) + 1
            continue
        if char == opener:
            depth += 1
            if nesting + depth >= 64:
                raise _ReapLimit("reap lexical nesting")
        elif char == closer:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError("unbalanced group")


def _reap_dquote_end(text: str, index: int, nesting: int = 0,
                      context: _ReapContext | None = None) -> int:
    if nesting >= 64:
        raise _ReapLimit("reap lexical nesting")
    while index < len(text):
        if index % 256 == 0:
            _reap_check(context)
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == '"':
            return index + 1
        if char == "$" and text[index + 1:index + 2] == "(":
            index = _reap_match(text, index + 2, nesting=nesting + 1, context=context) + 1
            continue
        if char == "`":
            index = _reap_backtick_end(text, index + 1) + 1
            continue
        index += 1
    raise ValueError("unbalanced quote")


def _reap_backtick_end(text: str, index: int) -> int:
    while index < len(text):
        if text[index] == "\\":
            index += 2
            continue
        if text[index] == "`":
            return index
        index += 1
    raise ValueError("unbalanced backtick")


class _ReapStage:
    """One simple command: argv, which words came from unknown expansions, and its inputs."""

    def __init__(self, start: int, piped: bool) -> None:
        self.start = start
        self.piped = piped
        self.words: list[str] = []
        self.dynamic: list[bool] = []
        self.stdin: list[str] = []
        self.stdin_file: str | None = None
        self.bodies: list[str] = []
        self.opaque: list[str] = []
        self.assigns: dict[str, tuple[str, bool]] = {}
        self.assignment_unsafe = False
        self.assignment_reap = False


class _ReapLexer:
    """Split shell text into simple commands, keeping substitutions, heredocs and here-strings.

    Variables assigned earlier in the same text are expanded from ``assigns``;
    any other expansion or substitution marks its word dynamic. Unbalanced
    quoting or nesting raises ValueError so the caller can fail closed.
    """

    def __init__(self, text: str, assigns: dict[str, tuple[str, bool]],
                 context: _ReapContext | None = None) -> None:
        self.context = context
        self.text = text
        self.assigns = assigns
        self.index = 0
        self.pending: list[tuple[_ReapStage, str, bool, bool]] = []
        self.stage = _ReapStage(0, False)
        self.word: list[str] | None = None
        self.dynamic = False
        self.quoted = False
        self.expect: str | None = None
        self.compounds: list[str] = []
        self.structure_invalid = False
        self.command_start = True

    def stages(self):
        piped = conditional = False
        while self.index < len(self.text):
            self.stage = _ReapStage(self.index, piped)
            self.stage.assignment_unsafe = bool(self.compounds) or conditional
            self.command_start = True
            self.expect = None
            separator = self._read_stage()
            self._separator_context(separator)
            piped = separator in {"|", "|&"}
            if self.stage.words or separator in {"&&", "||"}:
                conditional = separator in {"&&", "||"} or (conditional and piped)
            yield self.stage

    def _word_context(self, value: str) -> None:
        """Track only compound boundaries in the existing unquoted token stream."""
        function_body = value == "{" and self.compounds[-1:] == ["function"]
        if (not self.command_start and not function_body) or self.quoted or self.dynamic:
            self.command_start = False
            return
        if value in {"case", "if", "for", "while", "until", "{", "function"}:
            if function_body:
                self.compounds.pop()
            self.compounds.append(value)
            self.stage.assignment_unsafe = True
        elif value in {"esac", "fi", "done", "}"}:
            expected = {"esac": {"case"}, "fi": {"if"},
                        "done": {"for", "while", "until"}, "}": {"{"}}[value]
            self.stage.assignment_unsafe = True
            if self.compounds and self.compounds[-1] in expected:
                self.compounds.pop()
            else:
                self.structure_invalid = True
        elif value in {"then", "elif", "else", "do"}:
            expected = {"for", "while", "until"} if value == "do" else {"if"}
            self.stage.assignment_unsafe = True
            if not self.compounds or self.compounds[-1] not in expected:
                self.structure_invalid = True
        elif value != "!":
            self.command_start = False

    def _separator_context(self, separator: str) -> None:
        if separator == "(":
            self.compounds.append("(")
            self.stage.assignment_unsafe = True
        elif separator == ")":
            self.stage.assignment_unsafe = True
            if self.compounds and self.compounds[-1] == "(":
                self.compounds.pop()
            elif not self.compounds or self.compounds[-1] != "case":
                self.structure_invalid = True

    def _add(self, chars: str) -> None:
        if self.word is None:
            self.word = []
        self.word.append(chars)

    def _add_dynamic(self, chars: str) -> None:
        self._add(chars)
        self.dynamic = True

    def _finish(self) -> None:
        if self.word is None:
            return
        value = "".join(self.word)
        stage = self.stage
        if self.expect in {"<", "<<<"}:
            if self.expect == "<":
                stage.stdin_file = value
            else:
                stage.stdin.append(value)
        elif self.expect in {"<<", "<<-"}:
            self.pending.append((stage, value, self.expect == "<<-", not self.quoted))
        elif self.expect is None:
            self._word_context(value)
            stage.words.append(value)
            stage.dynamic.append(self.dynamic)
        self.word, self.dynamic, self.quoted, self.expect = None, False, False, None

    def _expand(self, value: str, dynamic: bool, quoted: bool) -> None:
        if quoted:
            self._add(value)
        else:
            parts = value.split()
            for number, part in enumerate(parts):
                if number:
                    self._finish()
                self._add(part)
        self.dynamic = self.dynamic or dynamic

    def _dollar(self, index: int, quoted: bool) -> int:
        text = self.text
        following = text[index + 1:index + 2]
        if following == "(":
            end = _reap_match(text, index + 2, context=self.context)
            self.stage.bodies.append(text[index + 2:end])
            self._add_dynamic(text[index:end + 1])
            return end + 1
        if following == "{":
            end = _reap_match(text, index + 2, "{", "}", context=self.context)
            inner = text[index + 2:end]
            if _REAP_NAME.fullmatch(inner) and inner in self.assigns:
                self._expand(*self.assigns[inner], quoted)
            else:
                self._add_dynamic(text[index:end + 1])
                self.stage.opaque.append(inner)
            return end + 1
        if following == "'" and not quoted:
            end = index + 2
            while end < len(text) and text[end] != "'":
                end += 2 if text[end] == "\\" else 1
            if end >= len(text):
                raise ValueError("unbalanced quote")
            try:
                self._add(text[index + 2:end].encode("latin-1", "backslashreplace").decode("unicode_escape"))
            except (UnicodeDecodeError, UnicodeEncodeError):
                self._add(text[index + 2:end])
            self.quoted = True
            return end + 1
        if following == '"' and not quoted:
            self.quoted = True
            return self._dquote(index + 2)
        name = _REAP_NAME.match(text, index + 1)
        if name:
            if name.group() in self.assigns:
                self._expand(*self.assigns[name.group()], quoted)
            else:
                self._add_dynamic("$" + name.group())
            return name.end()
        if following and following in "@*#?$!-0123456789":
            self._add_dynamic(text[index:index + 2])
            return index + 2
        self._add("$")
        return index + 1

    def _backtick(self, index: int) -> int:
        end = _reap_backtick_end(self.text, index + 1)
        body = self.text[index + 1:end]
        self.stage.bodies.append(re.sub(r"\\([`$\\])", r"\1", body))
        self._add_dynamic(self.text[index:end + 1])
        return end + 1

    def _dquote(self, index: int, closing: bool = True) -> int:
        text = self.text
        while index < len(text):
            if index % 256 == 0:
                _reap_check(self.context)
            char = text[index]
            if char == '"' and closing:
                return index + 1
            if char == "\\" and text[index + 1:index + 2] in ('$', '`', '"', "\\", "\n"):
                if text[index + 1] != "\n":
                    self._add(text[index + 1])
                index += 2
                continue
            if char == "$":
                index = self._dollar(index, True)
                continue
            if char == "`":
                index = self._backtick(index)
                continue
            self._add(char)
            index += 1
        if closing:
            raise ValueError("unbalanced quote")
        return index

    def _heredocs(self) -> None:
        text = self.text
        for stage, delimiter, strip_tabs, expands in self.pending:
            lines: list[str] = []
            while self.index < len(text):
                end = text.find("\n", self.index)
                end = len(text) if end < 0 else end
                line = text[self.index:end]
                self.index = end + 1
                if (line.lstrip("\t") if strip_tabs else line) == delimiter:
                    break
                lines.append(line)
            body = "\n".join(lines)
            stage.stdin.append(body)
            if expands:
                # An unquoted delimiter expands $(...) and `...` in the body.
                inner = _ReapLexer(body, self.assigns, self.context)
                inner.stage = stage
                try:
                    inner._dquote(0, closing=False)
                except _ReapLimit:
                    raise
                except ValueError:
                    stage.opaque.append(body)
        self.pending = []

    def _read_stage(self) -> str:
        text = self.text
        while self.index < len(text):
            if self.index % 256 == 0:
                _reap_check(self.context)
            index = self.index
            char = text[index]
            following = text[index + 1:index + 2]
            if char in " \t":
                self._finish()
                self.index += 1
            elif char == "\\":
                if following != "\n":
                    self._add(following)
                    self.quoted = True
                self.index += 2
            elif char == "'":
                end = text.find("'", index + 1)
                if end < 0:
                    raise ValueError("unbalanced quote")
                self._add(text[index + 1:end])
                self.quoted = True
                self.index = end + 1
            elif char == '"':
                self.quoted = True
                self._add("")
                self.index = self._dquote(index + 1)
            elif char == "`":
                self.index = self._backtick(index)
            elif char == "$":
                self.index = self._dollar(index, False)
            elif char == "#" and self.word is None:
                end = text.find("\n", index)
                self.index = len(text) if end < 0 else end
            elif char in "<>" and following == "(":
                end = _reap_match(text, index + 2, context=self.context)
                self.stage.bodies.append(text[index + 2:end])
                self._add_dynamic(text[index:end + 1])
                self.index = end + 1
            elif char in "<>" or (char == "&" and following == ">"):
                if self.word is not None and not self.quoted and "".join(self.word).isdigit():
                    self.word = None
                else:
                    self._finish()
                for operator in ("<<<", "<<-", "&>>", "<<", "<>", "<&", ">>", ">|", ">&", "&>", "<", ">"):
                    if text.startswith(operator, index):
                        break
                self.index = index + len(operator)
                self.expect = operator if operator in {"<", "<<<", "<<", "<<-"} else ">"
            elif char in ";&|\n()":
                if char == "(" and self.word is not None and "".join(self.word).endswith("="):
                    end = _reap_match(text, index + 1, context=self.context)
                    self._add(text[index:end + 1])
                    self.index = end + 1
                    continue
                self._finish()
                for separator in (";;&", ";;", ";&", "&&", "||", "|&"):
                    if text.startswith(separator, index):
                        break
                else:
                    separator = char
                self.index = index + len(separator)
                if char == "\n" and self.pending:
                    self._heredocs()
                return separator
            else:
                self._add(char)
                self.index += 1
        self._finish()
        return ""


def _reap_record(stage: _ReapStage, assigns: dict[str, tuple[str, bool]],
                 assignment_unsafe: bool) -> None:
    """Know only a single, unconditional, top-level literal assignment before use."""
    words, dynamic = stage.words, stage.dynamic
    if words[:1] == ["function"] and "{" in words:
        body = words.index("{") + 1
        words, dynamic = words[body:], dynamic[body:]
    while words and words[0] in _REAP_RESERVED and not dynamic[0]:
        words, dynamic = words[1:], dynamic[1:]
    if not words:
        return
    if words[0] in _REAP_DECLARERS:
        pairs = [(word, flag) for word, flag in zip(words[1:], dynamic[1:]) if not word.startswith("-")]
    else:
        pairs = []
        for word, flag in zip(words, dynamic):
            if not _REAP_ASSIGNMENT.match(word):
                break
            pairs.append((word, flag))
        # Prefix assignments belong to the launched command's environment;
        # they are writes, but cannot prove a persistent shell value.
        assignment_unsafe = assignment_unsafe or len(pairs) != len(words)
    for word, flag in pairs:
        match = _REAP_ASSIGNMENT.match(word)
        if match is None:
            continue
        value = word[match.end():]
        stage.assignment_reap |= "reap" in _reap_flat_words(value)
        name = match.group(1)
        flag = flag or assignment_unsafe or name in assigns or bool(match.group(2))
        # Preserve an expansion across ssh's argv-to-text boundary, so an
        # uncertain assignment cannot become a known literal in the next shell.
        assigns[name] = ("$" + name if flag else value, flag)


def _skip_wrapper_options(
    tokens: list[str],
    index: int,
    *,
    value_options: frozenset[str] = frozenset(),
    short_flags: str = "",
    flag_options: frozenset[str] = frozenset(),
) -> int | None:
    """Consume known wrapper options; None for unknown forms (routing guard copy)."""
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
    """Return the launched argv index for a transparent execution wrapper (routing guard copy)."""
    index += 1
    if wrapper == "timeout":
        return _skip_timeout_options(tokens, index)
    if wrapper == "nice":
        return _skip_nice_options(tokens, index)
    if wrapper == "exec":
        return _skip_wrapper_options(tokens, index, value_options=frozenset({"-a"}), short_flags="cl")
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
                {"-u", "-g", "-p", "-C", "-D", "-r", "-t", "-T", "-U", "-R", "--user", "--group", "--prompt",
                 "--close-from", "--chdir", "--role", "--type", "--command-timeout", "--other-user", "--chroot"}
            ),
            short_flags="AbEHiKknPSs",
            flag_options=frozenset(
                {"--askpass", "--background", "--preserve-env", "--set-home", "--login", "--remove-timestamp",
                 "--reset-timestamp", "--non-interactive", "--preserve-groups", "--stdin", "--shell"}
            ),
        )
    if wrapper == "doas":
        return _skip_wrapper_options(tokens, index, value_options=frozenset({"-u", "-a"}), short_flags="ns")
    if wrapper == "xargs":
        return _skip_wrapper_options(
            tokens,
            index,
            value_options=frozenset(
                {"-n", "-P", "-I", "-L", "-s", "-E", "-d", "--max-args", "--max-procs", "--replace",
                 "--max-lines", "--max-chars", "--eof", "--delimiter"}
            ),
            short_flags="0rtpx",
            flag_options=frozenset({"--null", "--no-run-if-empty", "--verbose", "--interactive", "--exit"}),
        )
    return None


def _skip_env(tokens: list[str]) -> int | None:
    """Index of the program env launches; None for an option form not modelled."""
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in _REAP_ENV_VALUE_OPTIONS:
            index += 2
        elif token in {"-i", "--ignore-environment", "-0", "--null", "-"} or token.startswith(("--unset=", "--chdir=")):
            index += 1
        elif token.startswith("-"):
            return None
        elif _REAP_ASSIGNMENT.match(token):
            index += 1
        else:
            return index
    return index


def _reap_reader_inert(name: str, args: list[str]) -> bool:
    """Whether a reading program leaves its operands unexecuted."""
    operands = [arg for arg in args if not arg.startswith("-")]
    if name == "rg":
        return not any(arg == "--pre" or arg.startswith("--pre=") for arg in args)
    if name == "sed":
        quiet = any(arg in {"--quiet", "--silent"} or (arg[:1] == "-" and arg[1:2] != "-" and "n" in arg[1:])
                    for arg in args)
        return quiet and not any(_REAP_SED_EXEC.search(arg) for arg in operands)
    if name in {"awk", "gawk", "mawk", "nawk"}:
        return not any("system" in arg or "|" in arg or "getline" in arg for arg in args)
    if name == "git":
        subcommand, rest = _parse_git_subcommand(["git", *args])
        global_options = args[:len(args) - len(rest) - 1] if subcommand is not None else args
        if any(option.startswith(("-c", "--config-env", "--exec-path")) for option in global_options):
            return False
        if subcommand == "grep" and any(arg.startswith(("-O", "--open-files-in-pager")) for arg in rest):
            return False
        return subcommand in _REAP_GIT_VERBS
    return True


_REAP_MAKE_METADATA = frozenset({"MSG", "OBJECTIVE", "TITLE", "BODY", "NOTE", "NOTES",
                                  "REASON", "SUMMARY", "DESCRIPTION"})
_REAP_GH_METADATA = frozenset({"--body", "-b", "--title", "-t", "--notes"})


def _reap_metadata_inert(name: str | None, argv: list[str], stdin: list[str],
                         stdin_file: str | None) -> bool:
    """Exempt only reap mentions contained in the specified inert operands."""
    if name in {"make", "gmake"}:
        if any(arg.startswith("-") for arg in argv[1:]):
            return False
        for arg in argv[1:]:
            if "reap" not in _reap_flat_words(arg):
                continue
            key, sep, value = arg.partition("=")
            if not sep or key not in _REAP_MAKE_METADATA or any(c in value for c in "'\"`$\\\n"):
                return False
        return True
    if name == "gh" and argv[1:2] in (["pr"], ["issue"], ["release"]) and argv[2:3] in (
        ["create"], ["edit"], ["comment"], ["review"]
    ):
        remaining = argv[3:]
        index = 0
        while index < len(remaining):
            arg = remaining[index]
            option, sep, _value = arg.partition("=")
            if option in _REAP_GH_METADATA:
                if not sep:
                    if index + 1 >= len(remaining):
                        return False
                    index += 1
            elif "reap" in _reap_flat_words(arg):
                return False
            index += 1
        return True
    if name in {"python", "python3"}:
        import io
        import tokenize

        if len(argv) == 3 and argv[1] == "-c":
            scripts = [argv[2]]
        elif argv[1:] in ([], ["-"]) and stdin and stdin_file is None:
            scripts = stdin
        else:
            return False
        try:
            for script in scripts:
                tokens = tokenize.generate_tokens(io.StringIO(script).readline)
                uncommented = tokenize.untokenize(token for token in tokens if token.type != tokenize.COMMENT)
                if _has_reap_words(uncommented):
                    return False
        except (tokenize.TokenError, IndentationError, SyntaxError):
            return False
        return True
    return False


def _reap_shell(argv: list[str], dynamic: list[bool], stdin: list[str], stdin_file: str | None,
                depth: int, assigns: dict[str, tuple[str, bool]], context: _ReapContext) -> str | None:
    """Classify sh/bash/zsh/dash/ksh: -c text, a script operand or a stdin script."""
    index = 1
    streamed = False
    while index < len(argv):
        token = argv[index]
        if token == "--":
            index += 1
            break
        if not token.startswith(("-", "+")) or token in {"-", "+"}:
            break
        if token[:1] == "-" and not token.startswith("--") and "c" in token[1:]:
            if index + 1 < len(argv):
                return _reap_text(argv[index + 1], depth + 1, stdin, stdin_file, assigns, context)
            return None
        if token[:1] == "-" and not token.startswith("--") and "s" in token[1:]:
            streamed = True
        index += 2 if token in _REAP_SHELL_VALUE_OPTIONS else 1
    rest = argv[index:]
    if streamed or not rest:
        # The shell reads its script from stdin; rest are its positional words.
        if (stdin_file or "").rsplit("/", 1)[-1] == _REAP_SCRIPT:
            return _REAP_DIRECT if rest[:1] == ["reap"] else None
        if _reap_evidence(rest, dynamic[index:], stdin, None):
            return _REAP_UNMODELLED
        return None
    if rest[0].rsplit("/", 1)[-1] == _REAP_SCRIPT and not dynamic[index]:
        if context.evidence and (len(rest) < 2 or dynamic[index + 1]):
            return _REAP_UNMODELLED
        return _REAP_DIRECT if rest[1:2] == ["reap"] else None
    # Any other script is opaque: it may run the words it is given.
    return _REAP_UNMODELLED if _reap_evidence(rest, dynamic[index:], [], None) else None


def _reap_command(argv: list[str], dynamic: list[bool], stdin: list[str], stdin_file: str | None,
                  depth: int, assigns: dict[str, tuple[str, bool]], context: _ReapContext) -> str | None:
    """Reason a simple command could run remote_agent.sh reap, or None when proven not to."""
    argv, dynamic = list(argv), list(dynamic)
    while argv and argv[0] in _REAP_RESERVED and not dynamic[0]:
        argv, dynamic = argv[1:], dynamic[1:]
    if not argv:
        return None

    def unmodelled() -> str | None:
        return _REAP_UNMODELLED if _reap_evidence(argv, dynamic, stdin, stdin_file) else None

    if argv[0] in _REAP_OPAQUE_SYNTAX:
        return unmodelled()
    xargs = False
    while argv:
        while argv and _REAP_ASSIGNMENT.match(argv[0]):
            key, _sep, value = argv[0].partition("=")
            if key in _REAP_MAKE_METADATA and _has_reap_words(value) and not _reap_metadata_inert(
                "make", ["make", argv[0]], [], None
            ):
                return _REAP_UNMODELLED
            argv, dynamic = argv[1:], dynamic[1:]
        if not argv:
            return None
        if dynamic[0]:
            # The program is an expansion the pass cannot resolve.
            return unmodelled()
        name = argv[0].rsplit("/", 1)[-1] if "/" not in argv[0] or argv[0].startswith("/") else None
        if name == "env":
            launched = _skip_env(argv)
        elif name == "command" and any(arg.startswith("-") and ("v" in arg or "V" in arg)
                                       for arg in argv[1:2]):
            return None
        elif name in _REAP_WRAPPERS:
            launched = _skip_wrapper(argv, 0, name)
            xargs = xargs or name == "xargs"
        else:
            break
        if launched is None:
            # A wrapper option form the pass does not model is not understood.
            return unmodelled()
        argv, dynamic = argv[launched:], dynamic[launched:]
    if not argv:
        return None
    base = argv[0].rsplit("/", 1)[-1]
    if xargs and base not in _REAP_READERS and _has_reap_words(" ".join([*argv, *stdin])):
        # xargs appends words read from stdin to the program it launches.
        return _REAP_UNMODELLED
    if base == _REAP_SCRIPT:
        if context.evidence and (len(argv) < 2 or dynamic[1]):
            return _REAP_UNMODELLED
        return _REAP_DIRECT if argv[1:2] == ["reap"] else None
    if name in _REAP_TEXT_EXECUTORS:
        return unmodelled()
    if name == "ssh":
        remote = _ssh_remote_words(argv)
        if not remote:
            # The remote login shell reads its commands from ssh's stdin.
            return _REAP_UNMODELLED if any(_has_reap_words(part) for part in stdin) else None
        # ssh joins argv with spaces, not shell quoting, before sshd's shell.
        return _reap_text(" ".join(remote), depth + 1, stdin, stdin_file, context=context)
    if name in _SHELL_WRAPPERS:
        return _reap_shell(argv, dynamic, stdin, stdin_file, depth, assigns, context)
    if name in _REAP_READERS and _reap_reader_inert(name, argv[1:]):
        return None
    if _reap_metadata_inert(name, argv, stdin, stdin_file):
        return None
    if name in {"make", "gmake", "gh"} and any(
        _has_reap_words(arg.partition("=")[2]) for arg in argv[1:] if "=" in arg
    ):
        # NAME=value and --option=value must not hide evidence when the narrow
        # operand exemption failed (options, executable values, unknown fields).
        return _REAP_UNMODELLED
    return unmodelled()


def _reap_fallback_command(stage: str, stdin: list[str], stdin_file: str | None, depth: int,
                           context: _ReapContext) -> str | None:
    """Classify a stage whose quoting does not balance; reap words refuse."""
    tokens = _reap_tokens(stage)
    words: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "<" and index + 1 < len(tokens):
            stdin_file = tokens[index + 1]
            index += 2
            continue
        if token.startswith("<") and not token.startswith("<<"):
            stdin_file = token[1:]
        else:
            words.append(token)
        index += 1
    reason = _reap_command(words, [False] * len(words), stdin, stdin_file, depth, {}, context) if words else None
    if reason is None and _has_reap_words(stage):
        reason = _REAP_UNMODELLED
    return reason


def _reap_units(text: str, depth: int = 0, stdin: list[str] | tuple[str, ...] = (),
                stdin_file: str | None = None,
                assigns: dict[str, tuple[str, bool]] | None = None,
                context: _ReapContext | None = None) -> list[tuple[list[str], str | None]]:
    """``(words, reason)`` for each simple command of shell text; a None reason admits it."""
    plain = " ".join([text.replace("'", "").replace('"', "").replace("\\", ""), *stdin, stdin_file or ""])
    if "reap" not in plain and "remote_agent" not in plain:
        return []
    if context is None:
        deadline = time.monotonic() + 2.0
        context = _ReapContext(_reap_root_evidence(text), deadline)
    _reap_check(context)
    if depth > _REAP_DEPTH_LIMIT:
        # An uninspected shell is not proof that no raw reap will execute.
        return [(_reap_tokens(text), _REAP_DIRECT)]
    assigns = dict(assigns or {})
    stages: list[_ReapStage] = []
    try:
        initial_assigns = dict(assigns)
        for all_dynamic in (False, True):
            assigns = dict(initial_assigns)
            stages = []
            lexer = _ReapLexer(text, assigns, context)
            for stage in lexer.stages():
                stage.assigns = dict(assigns)
                _reap_record(stage, assigns, all_dynamic or stage.assignment_unsafe)
                stages.append(stage)
            if all_dynamic or not (lexer.structure_invalid or lexer.compounds):
                break
            # Unmatched structure invalidates even earlier assignment facts.
            # Replay the same lexer with every assignment marked uncertain.
    except _ReapDeadline:
        raise
    except _ReapLimit:
        if context.evidence:
            raise
        return []
    except ValueError:
        return [(_reap_tokens(stage), _reap_fallback_command(stage, list(stdin), stdin_file, depth, context))
                for stage in _reap_stages(text)]
    # Assignment RHS evidence must survive invalidating its literal value.
    if any(stage.assignment_reap for stage in stages):
        context = context._replace(evidence=True)
    units: list[tuple[list[str], str | None]] = []
    pipeline_start = 0
    for stage in stages:
        _reap_check(context)
        if not stage.piped:
            pipeline_start = stage.start
        upstream = text[pipeline_start:stage.start] if stage.piped else ""
        units.append((stage.words, _reap_stage(stage, upstream, depth, list(stdin), stdin_file, context)))
    return units


def _reap_stage(stage: _ReapStage, upstream: str, depth: int, stdin: list[str], stdin_file: str | None,
                context: _ReapContext) -> str | None:
    for body in stage.bodies:
        # A substitution runs in the same shell, with the variables set so far.
        reason = _reap_text(body, depth + 1, (), None, stage.assigns, context)
        if reason is not None:
            return reason
    if any(_has_reap_words(part) for part in stage.opaque):
        return _REAP_UNMODELLED
    own = list(stage.stdin)
    if stage.piped:
        # The program reads what earlier commands print; their text bounds it.
        own.append(upstream)
    if own or stage.stdin_file is not None:
        stdin, stdin_file = own, stage.stdin_file
    return _reap_command(stage.words, stage.dynamic, stdin, stdin_file, depth, stage.assigns, context)


def _reap_text(text: str, depth: int = 0, stdin: list[str] | tuple[str, ...] = (),
               stdin_file: str | None = None, assigns: dict[str, tuple[str, bool]] | None = None,
               context: _ReapContext | None = None) -> str | None:
    """First reason shell text could run remote_agent.sh reap, or None."""
    for _words, reason in _reap_units(text, depth, stdin, stdin_file, assigns, context):
        if reason is not None:
            return reason
    return None


def _reap_crash_units(command: str) -> list[tuple[list[str], str | None]]:
    units = []
    for stage in _reap_stages(command):
        words = _reap_tokens(stage)
        override = words[:1] == ["export"] and _BYPASS_TOKEN in words[1:]
        units.append((words, None if override else _REAP_UNMODELLED))
    return units


def _violation(segment: list[str]) -> str | None:
    """Return a remedy message when this command is a raw lifecycle intent."""
    tokens = _strip_env_prefix(segment)
    if not tokens:
        return None
    if not _is_git(tokens[0]):
        return _unwrap_violation(tokens)
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

    if subcommand == "merge":
        return _merge_violation(args)

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
    try:
        reap_units = _reap_units(command)
    except Exception:
        reap_units = _reap_crash_units(command)
    for words, reason in reap_units:
        if words[:1] == ["export"] and _BYPASS_TOKEN in words[1:]:
            bypass_active = True
            continue
        if reason is None:
            continue
        if not bypass_active:
            return _reap_remedy(reason), bypassed
        bypassed.append(" ".join(words))
    bypass_active = False
    for segment in _scan_segments(command):
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


def _log_bypass(command: str, bypassed: list[str], source: str = "command") -> None:
    """Append a bypass audit record; best-effort, never breaks the bypass."""
    record = {
        "event": "bash_lifecycle_bypass",
        "bypass_var": _BYPASS_ENV,
        "bypass_source": source,
        "command": command,
        "bypassed_segments": bypassed,
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    root = _workspace_root()
    try:
        state_dir = root / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / "lifecycle_guard_bypass.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError:
        pass


def main() -> int:
    global _payload_workdir
    _payload_workdir = None
    env_bypass = os.environ.get(_BYPASS_ENV) == "1"
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(payload, dict):
        return 0
    # Cursor registers this guard under its Shell tool (harness-protocol.yaml
    # cursor_matcher); the shared adapter also covers Codex exec_command.
    shell = _shell_command(payload)
    if shell is None:
        return 0
    command, _payload_workdir = shell

    if env_bypass:
        try:
            reap_units = _reap_units(command)
        except Exception:
            reap_units = _reap_crash_units(command)
        bypassed = [" ".join(words) for words, reason in reap_units if reason is not None]
        bypassed += [" ".join(segment) for segment in _scan_segments(command) if _violation(segment) is not None]
        if bypassed:
            _log_bypass(command, bypassed, source="env")
            print(
                f"lifecycle guard bypassed by the process env ({_BYPASS_ENV}=1), "
                "audit-logged to .task-state/lifecycle_guard_bypass.jsonl",
                file=sys.stderr,
            )
        return 0

    remedy, bypassed = scan_command_with_bypass(command)
    if bypassed:
        _log_bypass(command, bypassed)
    if remedy is None:
        return 0

    if remedy.startswith(_REMOTE_REAP_REMEDY):
        print(remedy, file=sys.stderr)
        return 2

    if 'operation="land"' in remedy:
        root = _workspace_root().expanduser().resolve()
        remedy = f"{remedy}\n\n{_landing_requirements(root)}"

    print(
        "BLOCKED: raw git lifecycle command. This repo drives branch and "
        "worktree lifecycle through make primitives.\n\n"
        f"{remedy}\n\n"
        "See Makefile.d/lifecycle.mk. Do not bypass this guard: run the named "
        "primitive, or stop and hand the operator this command and this message. "
        "The operator's audit-logged escape hatch is documented in the module "
        "docstring of scripts/hooks/guard-bash-lifecycle-primitives.py.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
