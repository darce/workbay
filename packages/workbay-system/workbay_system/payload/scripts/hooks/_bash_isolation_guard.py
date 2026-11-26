#!/usr/bin/env python3
"""Shell-command scanner for Bash tool main-branch edit enforcement (BR-17).

Problem: the Edit/Write PreToolUse hook only gates the editor tool surface.
Commands like `sed -i`, `tee`, `echo > file`, `python -c "open('x','w')..."` go
through Bash, which was excluded from every hook matcher, creating a silent
bypass of the branch-isolation policy.

Scope (conservative): identify shell commands that WRITE to or DELETE protected
paths. Inspection is best-effort; when ambiguous we bias toward blocking so the
user uses the Edit/Write tool (which has proper path semantics and already goes
through the main-branch guard).

Public API:
    scan_bash_command(command, repo_root, policy) -> list[str]
        Return a list of repo-relative protected paths the command appears to
        write to (or delete). Empty list means the command is safe.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

from _branch_isolation_guard import (
    PROTECTED_EDIT_BRANCHES,
    collect_protected_inodes,
    normalize_path_token,
    path_identifies_repo_root,
    resolve_path_branch,
)
from _harness_protocol import BranchIsolationPolicy, is_branch_isolation_protected_path
from _shell_tool_payload import ShellWriteEffect, python_write_targets

# Unexpanded shell cwd tokens: expand from the effective stage cwd before
# identity / protected-path checks so `$PWD/...` cannot land in the allow
# arm. [SEC-01][SEC-07]
_PWD_ENV_RE = re.compile(r"^\$(?:\{PWD\}|PWD)(?=/|$|\\)")


_WRITE_REDIRECTS = {">", ">>", "|&>", "&>", "&>>"}

_DESTRUCTIVE_VERBS = {
    "rm": "all_nonflag",
    "unlink": "all_nonflag",
    "truncate": "all_nonflag",
    "shred": "all_nonflag",
    "tee": "all_nonflag",
    "cp": "last_nonflag",
    "mv": "last_nonflag",
    "install": "last_nonflag",
    "rsync": "last_nonflag",
    "dd": "after_flag:of=",
}

_PYTHON_VERB = re.compile(r"python(?:\d+(?:\.\d+)*)?")

_SED_IN_PLACE_FLAGS = {"-i", "--in-place"}


# Formatter / linter-fix command families (FU-01). These rewrite files in place
# across one or more code roots without naming individual targets in the shell
# command, so the per-token scanners above miss them. When detected on main
# without a MAINT-* task, the guard synthesises the configured code_roots as
# blocked paths so the caller surfaces the contract-backed violation. The
# registry is deliberately conservative: we match on the first "real" verb
# (after sudo/env stripping via _verb_of) and a discriminating second token so
# unrelated invocations (e.g. `make test-handoff`, `ruff check` without --fix)
# do not trigger false positives.
_FORMATTER_SUBCOMMAND_RULES: tuple[tuple[str, tuple[str, ...] | None], ...] = (
    # Makefile-driven formatter / lint-fix targets.
    (
        "make",
        (
            "format-all",
            "format-handoff",
            "format-orchestrator",
            "format-mcp",
            "fix-lint-handoff",
            "fix-lint-orchestrator",
            "fix-lint-mcp",
            "fix-php-style",
        ),
    ),
    # ruff format <paths...>  /  ruff check --fix
    ("ruff", ("format",)),
    # black <paths...>  — any invocation rewrites in place.
    ("black", None),
    # prettier --write / -w
    ("prettier", None),
    # npm / pnpm / yarn scripts typically aliased to formatters.
    ("npm", ("format", "fmt", "lint:fix", "fix")),
    ("pnpm", ("format", "fmt", "lint:fix", "fix")),
    ("yarn", ("format", "fmt", "lint:fix", "fix")),
    # Composer aliases used in the PHP plugin workspace.
    ("composer", ("format", "fix-style", "run-format", "run-fix-style")),
)

# A formatter-family command can be explicitly read-only. Keep this list
# narrow and tool-agnostic: an unrecognised or ambiguous flag remains a write.
# Write flags take precedence when a command contains both kinds. In
# particular, ``-l`` is intentionally absent because Black uses it for line
# length while still rewriting files. [OBS-04][SEC]
_FORMATTER_READONLY_FLAGS = frozenset(
    {
        "--check",
        "--diff",
        "--dry-run",
        "--list-different",
        "--help",
        "--version",
    }
)
_FORMATTER_WRITE_FLAGS = frozenset(
    {"--fix", "--fix-only", "--unsafe-fixes", "--write", "-w"}
)

# Secondary fix-flag scan: commands whose baseline behaviour is read-only but
# which write to disk when a specific flag is present.
_FORMATTER_FIX_FLAG_RULES: tuple[tuple[str, frozenset[str]], ...] = (
    ("ruff", frozenset({"--fix", "--fix-only", "--unsafe-fixes"})),
    ("eslint", frozenset({"--fix"})),
    ("stylelint", frozenset({"--fix"})),
)


def _detect_formatter(verb: str, args: list[str]) -> bool:
    """Return True when the verb+args look like an in-place formatter run."""
    if not verb:
        return False
    write_requested = any(token in _FORMATTER_WRITE_FLAGS for token in args)
    read_only_requested = any(token in _FORMATTER_READONLY_FLAGS for token in args)
    for rule_verb, subcommands in _FORMATTER_SUBCOMMAND_RULES:
        if verb != rule_verb:
            continue
        if subcommands is None:
            matched = True
        else:
            # npm/yarn/pnpm often require a literal "run" token before the script.
            candidate_tokens = []
            for token in args:
                if token in {"run", "run-script", "exec", "--"}:
                    continue
                if token.startswith("-"):
                    continue
                candidate_tokens.append(token)
                break  # Only the first positional matters for script selection.
            matched = bool(candidate_tokens and candidate_tokens[0] in subcommands)
        if not matched:
            continue
        if write_requested:
            return True
        if read_only_requested:
            return False
        return True
    for rule_verb, fix_flags in _FORMATTER_FIX_FLAG_RULES:
        if verb != rule_verb:
            continue
        if any(token in fix_flags for token in args):
            return True
    return False


def _is_flag(token: str) -> bool:
    return token.startswith("-") and token != "-"


_SHELL_QUOTED = r"""'[^']*'|"(?:\\[\s\S]|[^"\\])*"|\\[\s\S]"""
_SHELL_SYNTAX = re.compile(
    rf"""{_SHELL_QUOTED}|\|&>|\|>|&>>?|<<<"""
    rf"""|(?P<heredoc><<-?(?![<-])[ \t]*(?:{_SHELL_QUOTED}|[^\s;|&<>'"\\])+)"""
    r"|(?P<comment>(?<!\S)#[^\n]*)|(?P<joiner>\|\||&&|[|;&\n])"
)


def _parse_shell_stages(
    command: str,
) -> list[tuple[str | None, str, list[str], list[tuple[str | None, bool]]]]:
    """Split shell stages while keeping quoted code and heredoc data intact."""
    stages = []
    pending = []
    bodies: list[tuple[str | None, bool]] = []
    start = cursor = 0
    joiner: str | None = None
    comment_start: int | None = None
    while True:
        match = _SHELL_SYNTAX.search(command, cursor)
        end = match.start() if match else len(command)
        cursor = match.end() if match else end
        separator = match.group("joiner") if match else None
        if match and match.group("heredoc"):
            spec = match.group()
            strip_tabs = spec.startswith("<<-")
            spelling = spec[3 if strip_tabs else 2:].lstrip(" \t")
            delimiter = shlex.split(spelling)[0]
            index = len(bodies)
            bodies.append((None, False))
            pending.append((delimiter, strip_tabs, spelling == delimiter, bodies, index))
            continue
        if match and match.group("comment"):
            comment_start = end
            continue
        if match and separator is None:
            continue

        stage = command[start:comment_start if comment_start is not None else end].strip()
        if stage:
            try:
                tokens = shlex.split(stage, comments=False, posix=True)
            except ValueError:
                tokens = stage.split()
            stages.append((joiner, stage, tokens, bodies))
            bodies = []
        if separator is None:
            break
        if separator == "\n":
            for delimiter, strip_tabs, shell_expands, stage_bodies, index in pending:
                lines = []
                while cursor < len(command):
                    newline = command.find("\n", cursor)
                    line = command[cursor:] if newline < 0 else command[cursor:newline]
                    cursor = len(command) if newline < 0 else newline + 1
                    if strip_tabs:
                        line = line.lstrip("\t")
                    if line == delimiter:
                        stage_bodies[index] = ("\n".join(lines), shell_expands)
                        break
                    lines.append(line)
            pending.clear()
        if stage or separator != "\n":
            joiner = ";" if separator == "\n" else separator
        start = cursor
        comment_start = None
    return stages


def _iter_stages(command: str) -> list[tuple[str | None, str, list[str]]]:
    """Project shell stages for the main-branch hook's branch-switch checks."""
    return [(joiner, raw, tokens) for joiner, raw, tokens, _bodies in _parse_shell_stages(command)]


def _iter_words(command: str) -> list[tuple[str | None, list[str]]]:
    """Project the same shell stages for the main-branch hook's prefix checks."""
    return [(joiner, tokens) for joiner, _raw, tokens in _iter_stages(command)]


# implementation note: separators through which a preceding `cd` stage propagates its
# directory change to the next stage. `|` runs the cd in a pipeline subshell
# and `||` only reaches the next stage when the cd FAILED, so both degrade to
# unknown-cwd (fail-closed) instead of adopting the cd target.
_CD_PROPAGATING_JOINERS = frozenset({"&&", ";"})


def _resolve_cd_target(args: list[str], current: Path | None) -> Path | None:
    """Resolve a ``cd`` stage's target directory, or ``None`` when unknown.

    Conservative: flags (``-P``/``-L``/anything dash-prefixed), ``cd -``,
    bare ``cd``, multi-token forms, and unexpandable targets (``$VAR``,
    backticks) all yield ``None`` so downstream relative targets stay
    fail-closed against the repo root.
    """
    if len(args) != 1:
        return None
    target = args[0]
    if not target or target == "-" or target.startswith("-"):
        return None
    if "$" in target or "`" in target:
        return None
    candidate = Path(target).expanduser()
    try:
        if candidate.is_absolute():
            return candidate.resolve(strict=False)
        if current is None:
            return None
        return (current / candidate).resolve(strict=False)
    except OSError:
        return None


def _expand_pwd_token(raw: str, stage_cwd: Path | None) -> str:
    """Expand a leading unexpanded ``$PWD`` / ``${PWD}`` against ``stage_cwd``.

    Shell does not expand env vars before the guard sees the command text.
    Leaving ``$PWD/...`` as a relative token lands it in the allow arm
    (literal ``$PWD/packages/...`` is not a protected path). Expand against
    the effective stage cwd when known; when cwd is unknown, leave the
    token so the None/identity arm can fail closed. [SEC-01][SEC-07]
    """
    stripped = normalize_path_token(raw) if raw else ""
    if not stripped:
        return raw
    match = _PWD_ENV_RE.match(stripped)
    if not match:
        return raw
    if stage_cwd is None:
        # Cannot expand — keep the unexpanded form for fail-closed handling.
        return stripped
    rest = stripped[match.end() :]
    # rest is "" or starts with / or \
    joined = str(stage_cwd) + rest if rest else str(stage_cwd)
    return joined


def _absolutize_target(
    raw: str, stage_cwd: Path | None, fallback_base: Path | None
) -> str:
    """Absolutize a relative write target against its stage's effective cwd.

    Absolute targets pass through untouched. Relative targets resolve against
    ``stage_cwd`` when a tracked ``cd`` established one, else against
    ``fallback_base`` (the repo root in ``scan_bash_command``, keeping
    unknown-cwd relative paths fail-closed and independent of the hook
    process cwd). When both are ``None`` (``extract_raw_write_targets``
    without any tracked cd) the raw token is preserved for the caller's own
    resolution.

    Unexpanded ``$PWD`` / ``${PWD}`` tokens expand against the stage cwd
    (or fallback) before absolutization. [SEC-01]
    """
    expanded = _expand_pwd_token(
        raw, stage_cwd if stage_cwd is not None else fallback_base
    )
    stripped = expanded.strip().strip("'\"")
    if not stripped:
        return raw
    candidate = Path(stripped).expanduser()
    if candidate.is_absolute():
        return stripped
    base = stage_cwd if stage_cwd is not None else fallback_base
    if base is None:
        return expanded
    try:
        return str((base / candidate).resolve(strict=False))
    except OSError:
        return expanded


def _scan_redirects(tokens: list[str]) -> list[str]:
    targets: list[str] = []
    idx = 0
    while idx < len(tokens):
        tok = tokens[idx]
        matched = False
        for op in sorted(_WRITE_REDIRECTS, key=len, reverse=True):
            if tok == op and idx + 1 < len(tokens):
                targets.append(tokens[idx + 1])
                idx += 2
                matched = True
                break
            if tok.startswith(op) and len(tok) > len(op):
                targets.append(tok[len(op) :])
                idx += 1
                matched = True
                break
        if not matched:
            for op in sorted(_WRITE_REDIRECTS, key=len, reverse=True):
                if op in tok and not tok.startswith("-"):
                    before, _, after = tok.partition(op)
                    if before and after:
                        targets.append(after)
                        break
            idx += 1
    return targets


def _strip_command_prefix(tokens: list[str]) -> list[str]:
    i = 0
    while i < len(tokens) and "=" in tokens[i] and not tokens[i].startswith("-"):
        name, _, _ = tokens[i].partition("=")
        if name and name.isidentifier():
            i += 1
        else:
            break
    return tokens[i:] if i < len(tokens) else tokens


def _verb_of(tokens: list[str]) -> tuple[str, list[str]]:
    rest = _strip_command_prefix(tokens)
    if not rest:
        return "", []
    skip = {"sudo", "env", "exec", "time", "nice", "command", "builtin"}
    while rest and rest[0] in skip:
        rest = rest[1:]
    if not rest:
        return "", []
    verb = Path(rest[0]).name if "/" in rest[0] else rest[0]
    return verb, rest[1:]


def _scan_verb_targets(verb: str, args: list[str]) -> list[str]:
    if verb not in _DESTRUCTIVE_VERBS:
        return []
    rule = _DESTRUCTIVE_VERBS[verb]
    positional = [a for a in args if not _is_flag(a) and not a.startswith("+")]
    if rule == "all_nonflag":
        return positional
    if rule == "last_nonflag":
        return positional[-1:] if positional else []
    if rule.startswith("after_flag:"):
        needle = rule.split(":", 1)[1]
        matched: list[str] = []
        for token in args:
            if token.startswith(needle):
                matched.append(token[len(needle) :])
        return matched
    return []


def _scan_sed_in_place(args: list[str]) -> list[str]:
    has_in_place = any(a in _SED_IN_PLACE_FLAGS or a.startswith("-i") for a in args)
    if not has_in_place:
        return []
    return [a for a in args if not _is_flag(a) and not a.startswith("+")]


_PYTHON_REDIRECT = re.compile(r"(?:\d*|&)(?:>>?|<(?!<))")


def _python_source(
    args: list[str], bodies: list[tuple[str | None, bool]],
) -> tuple[str | None, bool, int | None] | None:
    """Extract Python's executable source and the argv index of inline code."""
    index = 0
    while index < len(args):
        arg = args[index]
        if arg.startswith("-c"):
            if arg != "-c":
                source, source_index = arg[2:], index
            elif index + 1 < len(args):
                source_index = index + 1
                source = args[source_index]
            else:
                return None, False, None
            # shlex has removed the quotes. Treat any potential interpolation
            # in the resulting source as unprovable.
            return source, True, source_index
        if arg == "-" or arg.startswith(("<<", "0<<")):
            break
        redirect = _PYTHON_REDIRECT.match(arg)
        if redirect:
            index += 2 if redirect.end() == len(arg) else 1
            continue
        if not arg.startswith("-") or arg == "--" or arg.startswith("-m"):
            return None
        index += 2 if arg in {"-W", "-X"} else 1
    if bodies:
        source, expands = bodies[-1]
        return source, expands, None
    return None


def _split_git_global_opts(args: list[str]) -> tuple[str | None, list[str]]:
    """Consume git global options preceding the subcommand.

    internal: pre-fix, `git -C <dir> checkout -- <path>` treated
    ``-C`` as the subcommand and the whole stage was invisible to the scanner
    — for benign worktree targets and primary-repo writes alike. Returns the
    composed ``-C`` directory (git applies multiple ``-C`` left-to-right) and
    the remaining tokens starting at the real subcommand. Only ``-C <dir>``
    and ``-c <key=val>`` pairs are consumed; any other leading token stops the
    scan (conservative: unknown global flags fall through to the existing
    subcommand mismatch, i.e. no targets).
    """
    git_dir: str | None = None
    i = 0
    while i < len(args):
        tok = args[i]
        if tok == "-C" and i + 1 < len(args):
            nxt = args[i + 1]
            git_dir = nxt if git_dir is None else str(Path(git_dir) / nxt)
            i += 2
            continue
        if tok == "-c" and i + 1 < len(args):
            i += 2
            continue
        break
    return git_dir, args[i:]


def _scan_git_writeback(verb: str, args: list[str]) -> tuple[str | None, list[str]]:
    """Return ``(-C dir or None, write-back targets)`` for a git stage."""
    if verb != "git" or not args:
        return None, []
    git_dir, args = _split_git_global_opts(args)
    subcmd = args[0] if args else ""
    rest = args[1:]
    targets: list[str] = []
    if subcmd == "checkout":
        if "--" in rest:
            dash_idx = rest.index("--")
            targets = [a for a in rest[dash_idx + 1 :] if not _is_flag(a)]
        else:
            targets = [a for a in rest if not _is_flag(a)]
    elif subcmd == "restore":
        targets = [a for a in rest if not _is_flag(a) and a != "--"]
    elif subcmd == "reset":
        if "--" in rest:
            dash_idx = rest.index("--")
            targets = [a for a in rest[dash_idx + 1 :] if not _is_flag(a)]
    elif subcmd == "clean":
        targets = [a for a in rest if not _is_flag(a)]
    return git_dir, targets


def _git_stage_targets(
    verb: str, args: list[str], effective_cwd: Path | None
) -> tuple[list[str], Path | None]:
    """Resolve git write-back targets plus the base dir they resolve against.

    The base is the composed ``-C`` directory (resolved against the stage's
    effective cwd via the same conservative rules as ``cd`` targets) when one
    was given, else the stage cwd itself. An unresolvable ``-C`` dir yields
    ``None`` so callers fall back fail-closed.
    """
    git_dir, targets = _scan_git_writeback(verb, args)
    if not targets:
        return [], effective_cwd
    if git_dir is None:
        return targets, effective_cwd
    return targets, _resolve_cd_target([git_dir], effective_cwd)


def _to_repo_relative(path: str, repo_root: Path) -> str | None:
    """Map a write target to a repo-relative posix path.

    Returns:
        - repo-relative posix string on success
        - ``""`` when the input token is empty/whitespace (no target)
        - ``""`` when ``repo_root`` is empty (no basis → skip; shared contract
          with ``to_repo_relative`` and ``guard-task-plan-findings``)
        - ``None`` when the path cannot be placed under ``repo_root``

    ``None`` is the unrelativizable signal: callers apply ancestor-identity
    refusal (``path_identifies_repo_root``) so spoofed in-repo spellings refuse
    while genuine outsiders stay available. [SECD-05][ARCH-13][WEB-13][WEB-33]
    """
    stripped = normalize_path_token(path)
    if not stripped:
        return ""
    # Shared contract with to_repo_relative / guard-task-plan-findings: empty
    # repo_root means no basis → skip. Path("") has no parts (str is ".") so
    # check .parts rather than str/bool.
    if not getattr(repo_root, "parts", True) or not str(repo_root).strip():
        return ""
    try:
        root_abs = repo_root.expanduser().resolve(strict=False)
    except OSError:
        root_abs = repo_root
    candidate = Path(stripped).expanduser()
    if not candidate.is_absolute():
        try:
            candidate = (root_abs / candidate).resolve(strict=False)
        except OSError:
            return stripped.replace("\\", "/").lstrip("/")
    else:
        try:
            candidate = candidate.resolve(strict=False)
        except OSError:
            pass
    try:
        # Normalize the relativization result (posix form) before policy match.
        return candidate.relative_to(root_abs).as_posix()
    except ValueError:
        # Unrelativizable after resolve — caller decides via ancestor identity.
        return None


def _shell_write_effects(command: str, initial_cwd: Path | None) -> list[ShellWriteEffect]:
    """Classify each stage once for isolation and worktree-drift checks."""
    effects = []
    cwd = initial_cwd
    pending_cd: tuple[Path | None] | None = None
    for joiner, _raw, tokens, bodies in _parse_shell_stages(command):
        if pending_cd is not None:
            cwd = pending_cd[0] if joiner in _CD_PROPAGATING_JOINERS else None
            pending_cd = None
        verb, args = _verb_of(tokens)
        redirect_tokens = tokens
        if _PYTHON_VERB.fullmatch(verb):
            script = _python_source(args, bodies)
            if script is not None:
                source, expands, source_index = script
                if source_index is not None:
                    token_index = len(tokens) - len(args) + source_index
                    redirect_tokens = tokens[:token_index] + tokens[token_index + 1:]
                effects.extend(
                    ShellWriteEffect(target, cwd)
                    for target in python_write_targets(source, shell_expands=expands)
                )

        effects.extend(ShellWriteEffect(target, cwd) for target in _scan_redirects(redirect_tokens))
        if verb == "cd":
            pending_cd = (_resolve_cd_target(args, cwd),)
            continue
        if verb == "sed":
            effects.extend(ShellWriteEffect(target, cwd) for target in _scan_sed_in_place(args))
        effects.extend(ShellWriteEffect(target, cwd) for target in _scan_verb_targets(verb, args))
        git_targets, git_base = _git_stage_targets(verb, args, cwd)
        effects.extend(ShellWriteEffect(target, git_base) for target in git_targets)
        if _detect_formatter(verb, args):
            effects.append(ShellWriteEffect(None, cwd, formatter=True))
    return effects


def extract_raw_write_targets(command: str) -> list[str]:
    """Return the write-target tokens parsed out of `command`.

    Mirrors the per-stage scanners used by scan_bash_command but skips the
    repo-root filter applied by `_to_repo_relative`, so absolute paths that
    resolve *outside* the current workspace are preserved. The caller (worktree
    drift guard) needs these so cross-worktree writes — e.g. `sed -i` against
    an absolute path pointing into the primary worktree from a linked feature
    worktree — can still be compared against the active task's target_worktree.

    implementation note contract: a relative target that follows a tracked `cd` (joined
    by `&&`/`;`) is returned ABSOLUTIZED against the cd-target directory, so
    the drift comparison sees the directory the shell would actually write
    into. Relative targets with no tracked cd are returned raw (the caller
    resolves them against its own workspace root, unchanged behavior).
    """
    return [
        _absolutize_target(effect.target, effect.cwd, None)
        for effect in _shell_write_effects(command, None)
        if effect.target is not None
    ]


def scan_bash_command(
    command: str,
    repo_root: Path,
    policy: BranchIsolationPolicy,
) -> list[str]:
    if not command or not command.strip():
        return []

    try:
        root_abs = repo_root.expanduser().resolve(strict=False)
    except OSError:
        root_abs = repo_root

    effects = _shell_write_effects(command, root_abs)
    formatter_cwds = [effect.cwd for effect in effects if effect.formatter]

    # Single shared set with the inline/branch guards so the protected set
    # cannot drift. Policy dataclass does not yet model protected_branches.
    # [ARCH-13]
    protected_branches = PROTECTED_EDIT_BRANCHES

    blocked: list[str] = []
    seen: set[str] = set()
    protected_inodes: frozenset[tuple[int, int]] | None = None
    for effect in effects:
        if effect.formatter:
            continue
        if effect.target is None:
            label = "(unresolved Python write target)"
            if label not in seen and (policy.code_roots or policy.root_protected_files):
                blocked.append(label)
                seen.add(label)
            continue
        raw = _absolutize_target(effect.target, effect.cwd, root_abs)
        relative = _to_repo_relative(raw, repo_root)
        if relative is None:
            # Unrelativizable after resolve: refuse when an existing ancestor
            # is samefile with repo_root (spoofed in-repo spelling) OR when
            # the candidate's inode matches a protected-surface file
            # (hardlink alias). Genuine outsiders continue unblocked.
            # Unexpanded `$PWD` tokens that could not be expanded also refuse
            # under the identity arm (fail-closed). [SECD-05][SEC-07]
            # [ARCH-13][WEB-13][WEB-33]
            token = normalize_path_token(raw) or raw
            if token in seen:
                continue
            seen.add(token)
            if _PWD_ENV_RE.match(token):
                # Still-unexpanded cwd token — cannot place it; refuse. [SEC-07]
                blocked.append(token)
                continue
            if protected_inodes is None:
                protected_inodes = collect_protected_inodes(repo_root, policy)
            if path_identifies_repo_root(
                token, repo_root, protected_inodes=protected_inodes
            ):
                blocked.append(token)
            continue
        if not relative or relative in seen:
            continue
        seen.add(relative)
        if not is_branch_isolation_protected_path(relative, policy):
            continue
        # Per-path worktree resolution (parity with check_file_edit): a path
        # that physically lives inside a linked worktree on a feature branch
        # is not a main-branch write even when the harness cwd reports main.
        # Falls back to the harness branch when the path does not resolve to
        # any git working tree (e.g. paths outside any repo). Normalize so
        # padded/quoted spellings do not re-anchor to the hook cwd. [SEC-01]
        per_path_branch = resolve_path_branch(normalize_path_token(raw) or raw)
        if per_path_branch is None or per_path_branch in protected_branches:
            blocked.append(relative)

    # FU-01: a formatter invocation implicitly writes across every code_root in
    # the contract. Emit the configured roots directly (with a `<root> (formatter)`
    # label) so the caller surfaces a clear, contract-backed violation without
    # fabricating file paths.
    # Formatter stages are allowed only when every effective cwd belongs to a
    # known, non-protected worktree branch. Unknown cwd, non-git directories,
    # and main/master all retain the conservative block. [OBS-04][SEC]
    formatter_isolated = formatter_cwds and all(
        cwd is not None
        and (branch := resolve_path_branch(str(cwd))) is not None
        and branch not in protected_branches
        for cwd in formatter_cwds
    )
    if formatter_cwds and not formatter_isolated:
        for root in policy.code_roots:
            normalized = root.strip("/")
            if not normalized:
                continue
            label = f"{normalized}/ (formatter)"
            if label not in seen:
                seen.add(label)
                blocked.append(label)
        for root_file in policy.root_protected_files:
            label = f"{root_file} (formatter)"
            if label not in seen:
                seen.add(label)
                blocked.append(label)
    return blocked
