#!/usr/bin/env python3
"""PreToolUse(Bash) hook: block destructive shell edits to protected paths on main.

Covers the BR-17 bypass where `sed -i`, `echo > file`, `tee`, `rm`, `python -c
"open(..., 'w')"`, `git restore`, etc. ran via the Bash tool and were never
scanned by the editor-tool-only main-branch guard.

Contract (Claude Code + VS Code harnesses):
    stdin  : JSON payload with tool_name and tool_input.command
    args   : none
    stdout : BLOCKED message when a write to a protected path is detected
    exit 0 : allow
    exit 2 : block
"""

from __future__ import annotations

import datetime as _datetime
import json
import os
import shlex
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

# internal: WORKBAY_* is the primary bypass name per the Tier-4
# env-var convention; the ALT_* form remains a deprecated legacy fallback.
_BYPASS_ENV_PRIMARY = "WORKBAY_ALLOW_BASH_MAIN_WRITE"
_BYPASS_ENV_LEGACY = "ALT_ALLOW_BASH_MAIN_WRITE"

# internal: durable terminal_guard_events write at block time
# ([OBS-01]). Test seam overrides the CLI argv (mirrors _run_guard.py).
_TERMINAL_GUARD_RECORD_ENV = "WORKBAY_TERMINAL_GUARD_RECORD"
_POLICY_VERSION = "branch-isolation-v1"
_POLICY_SOURCE = "guard-bash-main-branch"
_HARNESS_CHOICES = ("claude-code", "codex", "grok", "cursor", "manual")
_COMMAND_PREVIEW_LIMIT = 256

# PreToolUse harness timeout is 5s (.claude/settings.json). Reserve 1.5s for
# interpreter start and non-subprocess work; the rest is the shared path budget
# for every git probe this hook waits on [RES-03][ARCH-13].
_HOOK_SUBPROCESS_BUDGET_SECONDS = 3.5
_REPO_ROOT_TIMEOUT_CAP = 1.5
_CURRENT_BRANCH_TIMEOUT_CAP = 1.5
# Caps for the branch-switch path after _repo_root has drawn from the budget.
# Kept low enough that repo + primary + one toplevel fit inside the budget even
# when each grant is charged in full (worst-case sum).
_PRIMARY_ROOT_TIMEOUT_CAP = 1.0
_WORKTREE_TOPLEVEL_TIMEOUT_CAP = 1.0

# Sentinel from ``_worktree_toplevel`` when the shared deadline left a zero
# grant. Distinct from ``None`` (path unresolvable) so the scan cannot treat
# "could not check" as "checked and clean" [OBS-08][RES-03].
_BUDGET_EXHAUSTED = object()
# Probe could not determine an answer for a non-budget reason (e.g. missing
# git binary, deleted cwd, hung rev-parse). Distinct from ``None`` determined
# negatives and from ``_BUDGET_EXHAUSTED`` [OBS-08][AGT-10].
_COULD_NOT_DETERMINE = object()


class _SubprocessTimeoutBudget:
    """Shared wall-clock deadline for every git probe this hook waits on.

    Each ``take(cap)`` returns ``min(cap, wall_left)`` and charges nothing: the
    monotonic deadline alone enforces the budget, so unused patience is
    available to later probes when git returns quickly. A zero grant means the
    caller must degrade without starting a subprocess [RES-03]. Every probe
    still clamps to the same deadline, so N branch-switch intents cannot run
    past the budget even if each receives a full cap [ARCH-13].
    """

    def __init__(self, seconds: float) -> None:
        seconds = float(seconds)
        self._deadline = time.monotonic() + seconds

    def take(self, cap: float) -> float:
        wall_left = max(0.0, self._deadline - time.monotonic())
        granted = min(float(cap), wall_left)
        if granted <= 0.0:
            return 0.0
        return granted


# Set for the duration of ``main()``; helpers outside main keep their caps.
_timeout_budget: _SubprocessTimeoutBudget | None = None


def _probe_timeout(cap: float) -> float:
    """Derive a per-call timeout from the invocation budget when active."""
    if _timeout_budget is None:
        return float(cap)
    return _timeout_budget.take(cap)


def _env_bypass_set(var_name: str) -> bool:
    """True when ``var_name`` requests a bypass via the environment.

    The ``WORKBAY_*`` primary resolves through the shared ``_interp`` alias
    (mirroring the sibling ``_guard_main_branch_inline.py``); ``ALT_*`` stays a
    raw legacy read. Falls back to a raw read when ``_interp`` is unavailable (it
    is a sibling on the hooks-dir ``sys.path`` inserted in :func:`main` before
    this runs).
    """
    if var_name.startswith("WORKBAY_"):
        try:
            from _interp import resolve_env_alias
        except ImportError:
            return os.environ.get(var_name) == "1"
        return resolve_env_alias(var_name) == "1"
    return os.environ.get(var_name) == "1"


def _bypass_request(command: str) -> tuple[str, str] | None:
    """Return ``(source, var_name)`` when a bypass is requested, else None.

    ``source`` is ``"env"`` (variable set in the environment that launched
    the harness) or ``"inline"`` (a leading ``VAR=1`` assignment on the FIRST
    stage of the command). Pre-fix the printed advice suggested an inline
    assignment, but the check only read ``os.environ`` — which the hook
    process evaluates *before* the user's command runs, so the inline form
    could never work. Only a first-stage leading assignment counts: a
    mid-command ``&& VAR=1 cmd`` does not bypass earlier stages.
    """
    for var_name in (_BYPASS_ENV_PRIMARY, _BYPASS_ENV_LEGACY):
        if _env_bypass_set(var_name):
            return "env", var_name
    try:
        from _bash_isolation_guard import _iter_words
    except ImportError:
        return None
    stages = _iter_words(command)
    if not stages:
        return None
    first_joiner, first_tokens = stages[0]
    if first_joiner is not None:
        return None
    for token in first_tokens:
        name, sep, value = token.partition("=")
        if not sep or not name.isidentifier():
            break  # past the leading-assignment prefix
        if name in (_BYPASS_ENV_PRIMARY, _BYPASS_ENV_LEGACY) and value == "1":
            return "inline", name
    return None


def _log_bypass(
    repo_root: Path,
    command: str,
    blocked: list[str],
    *,
    source: str,
    var_name: str,
) -> None:
    """Append a bypass audit record to .task-state/branch_isolation_guard.jsonl.

    Best-effort: an unwritable state dir must never break the bypass itself.
    """
    record = {
        "event": "bash_main_write_bypass",
        "bypass_source": source,
        "bypass_var": var_name,
        "command": command,
        "blocked_paths": blocked,
        "ts": _datetime.datetime.now(_datetime.timezone.utc).isoformat(),
    }
    try:
        state_dir = repo_root / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / "branch_isolation_guard.jsonl").open(
            "a", encoding="utf-8"
        ) as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError:
        pass


def _primary_checkout_root() -> Path | None:
    """Primary checkout root via filesystem only — never ``Path.cwd()``.

    Linked worktrees often lack their own ``.venv``; the primary checkout
    usually has one. Walk from this file's tree for a ``.git`` dir, or parse a
    worktree ``.git`` file to reach the shared primary. No git subprocess: the
    block-path telemetry launcher must not burn the hook budget [RES-03].
    """
    here = Path(__file__).resolve()
    # scripts/hooks/<this> -> parents[2] is the scripts-tree checkout root.
    start = here.parents[2]
    for base in (start, *here.parents[:8]):
        git_entry = base / ".git"
        if git_entry.is_dir():
            return base
        if not git_entry.is_file():
            continue
        try:
            text = git_entry.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not text.startswith("gitdir:"):
            return None
        raw = text.split(":", 1)[1].strip()
        gitdir = Path(raw) if Path(raw).is_absolute() else (base / raw)
        try:
            gitdir = gitdir.resolve(strict=False)
        except OSError:
            return None
        # worktree gitdir: <primary>/.git/worktrees/<name>
        for parent in gitdir.parents:
            if parent.name == ".git":
                return parent.parent
        return None
    return None


def _deps_python() -> str:
    """Interpreter carrying the workbay stack deps (mirrors _run_guard.py).

    Prefer a repo-local ``.venv`` discovered from this file's location, then the
    primary checkout (so a worktree without its own venv still heals). Never
    select an interpreter from the agent's ``Path.cwd()`` — that may belong to
    an unrelated project [OBS-01]. ``_interp.resolve_deps_python`` shells out
    twice at ``timeout=5`` each, which alone exceeds the PreToolUse budget on
    the BLOCK path that launches telemetry. Telemetry is best-effort; a missing
    module must not deny the tool call by burning the hook's budget [RES-03].
    """
    here = Path(__file__).resolve()
    # scripts/hooks/<this> -> parents[2] is the checkout root for the scripts/
    # tree. For the packaged payload twin (…/payload/scripts/hooks) parents[2]
    # is …/payload, so that look-up misses and we fall back to the primary
    # checkout (never cwd).
    bases: list[Path] = []
    for base in (here.parents[2], _primary_checkout_root()):
        if base is None:
            continue
        if base not in bases:
            bases.append(base)
    for base in bases:
        for parts in (("bin", "python"), ("Scripts", "python.exe")):
            candidate = base / ".venv" / Path(*parts)
            if candidate.is_file():
                return str(candidate)
    return sys.executable


def _resolve_harness() -> str:
    """Derive harness label from WORKBAY_HANDOFF_HARNESS (mirrors capture-agent-errors)."""
    try:
        from _interp import resolve_env_alias

        raw = (resolve_env_alias("WORKBAY_HANDOFF_HARNESS") or "").strip()
    except Exception:  # noqa: BLE001
        raw = (os.environ.get("WORKBAY_HANDOFF_HARNESS") or "").strip()
    if not raw:
        if os.environ.get("GROK_WORKSPACE_ROOT", "").strip():
            return "grok"
        return "claude-code"
    if raw in _HARNESS_CHOICES:
        return raw
    return "manual"


def _terminal_guard_record_argv() -> list[str]:
    """Resolve terminal-guard-record invocation (CLI only — never import handoff)."""
    override = os.environ.get(_TERMINAL_GUARD_RECORD_ENV)
    if override:
        return shlex.split(override)
    console_script = shutil.which("mcp-workbay-handoff")
    if console_script:
        return [console_script, "terminal-guard-record"]
    return [_deps_python(), "-m", "workbay_handoff_mcp", "terminal-guard-record"]


def _redact_command_preview(command: str) -> str:
    """Bounded single-line preview for telemetry (schema limit 256)."""
    line = " ".join((command or "").splitlines()[0].split()) if command else ""
    if len(line) <= _COMMAND_PREVIEW_LIMIT:
        return line
    return line[: _COMMAND_PREVIEW_LIMIT - 3].rstrip() + "..."


def _record_terminal_guard_block(
    *,
    command: str,
    blocked: list[str],
    decision: str = "block",
) -> None:
    """Best-effort terminal_guard_events write; never raises, never blocks exit path.

    Fire-and-forget detached Popen (same posture as ``_run_guard._record_infra_failure``):
    a slow DB write must not delay the exit-2 block (hook timeouts become denies).

    The ENTIRE body (argv build + Popen) is guarded (REV-S3-1): a malformed
    ``WORKBAY_TERMINAL_GUARD_RECORD`` override makes ``shlex.split`` raise
    ``ValueError`` — if that escapes, ``main`` exits 1 with a traceback instead
    of ``return 2``, inverting the BLOCK into an ALLOW. Nothing may escape.
    """
    try:
        preview = _redact_command_preview(command)
        if not preview:
            preview = "(empty-command)"
        trigger = ",".join(blocked) if blocked else "protected-path"
        if len(trigger) > _COMMAND_PREVIEW_LIMIT:
            trigger = trigger[: _COMMAND_PREVIEW_LIMIT - 3].rstrip() + "..."
        argv = _terminal_guard_record_argv() + [
            "--decision",
            decision,
            "--tool-name",
            "Bash",
            "--harness",
            _resolve_harness(),
            "--command-preview",
            preview,
            "--policy-version",
            _POLICY_VERSION,
            "--policy-source",
            _POLICY_SOURCE,
            "--trigger",
            trigger,
        ]
        subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        pass


def _safe_cwd() -> Path | None:
    """Process cwd, or ``None`` when it is unreadable/deleted (non-raising)."""
    try:
        return Path.cwd()
    except OSError:
        return None


def _repo_root() -> Path | object:
    """Resolve the git toplevel, or ``_COULD_NOT_DETERMINE`` when probing fails.

    ``Path.cwd()`` is never allowed to raise: a deleted agent cwd (common after
    ``make task-finish`` removes the worktree the session sits in) must fail
    closed, not crash the hook with exit 1 (non-blocking) [AGT-10][OBS-08].
    ``TimeoutExpired`` is could-not-determine, never a silent cwd fallback that
    keys later checks off the wrong root [RES-03].
    """
    timeout = _probe_timeout(_REPO_ROOT_TIMEOUT_CAP)
    if timeout <= 0:
        cwd = _safe_cwd()
        return cwd if cwd is not None else _COULD_NOT_DETERMINE
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            # Drawn from the shared path budget so the sum of grants cannot
            # exceed the harness allowance [RES-03][ARCH-13].
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        # Hung rev-parse must not fall through to cwd (wrong root) [RES-03].
        return _COULD_NOT_DETERMINE
    except (OSError, subprocess.SubprocessError):
        cwd = _safe_cwd()
        return cwd if cwd is not None else _COULD_NOT_DETERMINE
    if proc.returncode != 0:
        cwd = _safe_cwd()
        return cwd if cwd is not None else _COULD_NOT_DETERMINE
    return Path(proc.stdout.strip() or ".")


def _current_branch(repo_root: Path) -> str | None:
    """Return the current branch name, ``""`` when detached, or ``None`` when
    the probe could not determine an answer.

    ``git branch --show-current`` exits 0 with empty stdout on detached HEAD
    (rebase/bisect/checkout ``<sha>``) — that is DETERMINED "no branch", not
    could-not-determine [OBS-08]. Non-zero rc, timeout, and OSError remain
    ``None`` (could-not-determine).
    """
    timeout = _probe_timeout(_CURRENT_BRANCH_TIMEOUT_CAP)
    if timeout <= 0:
        return None
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def _load_payload() -> dict:
    try:
        return json.load(sys.stdin)
    except Exception:
        return {}


def _extract_command(payload: dict) -> str:
    tool_input = payload.get("toolInput") or payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        return ""
    command = tool_input.get("command")
    if not isinstance(command, str):
        return ""
    return command


# Dedicated bypass for the root-worktree branch-switch guard (PLAN-3). Kept
# separate from WORKBAY_ALLOW_BASH_MAIN_WRITE (write semantics) so a switch
# bypass is not logged as a write bypass and the two cannot be conflated.
_BRANCH_SWITCH_BYPASS_PRIMARY = "WORKBAY_ALLOW_ROOT_BRANCH_SWITCH"
_BRANCH_SWITCH_BYPASS_LEGACY = "ALT_ALLOW_ROOT_BRANCH_SWITCH"
_PROTECTED_BRANCHES = frozenset({"main", "master"})


def _branch_switch_bypass(command: str) -> tuple[str, str] | None:
    """Return ``(source, var_name)`` when a root-branch-switch bypass is set."""
    for var_name in (_BRANCH_SWITCH_BYPASS_PRIMARY, _BRANCH_SWITCH_BYPASS_LEGACY):
        if _env_bypass_set(var_name):
            return "env", var_name
    try:
        from _bash_isolation_guard import _iter_words
    except ImportError:
        return None
    stages = _iter_words(command)
    if not stages:
        return None
    first_joiner, first_tokens = stages[0]
    if first_joiner is not None:
        return None
    for token in first_tokens:
        name, sep, value = token.partition("=")
        if not sep or not name.isidentifier():
            break
        if name in (_BRANCH_SWITCH_BYPASS_PRIMARY, _BRANCH_SWITCH_BYPASS_LEGACY) and value == "1":
            return "inline", name
    return None


def _switch_target_branch(subcmd: str, subargs: list[str], is_flag) -> str | None:
    """Branch a `git checkout`/`switch` stage creates or switches to, else None.

    Q2 scope: only the *unambiguous* branch forms are reported —
    ``checkout -b/-B <X>``, ``switch -c/-C <X>`` (creation),
    ``checkout --orphan <X>``, and plain ``switch <X>`` (switch never
    restores files). Plain ``git checkout <X>`` is EXCLUDED (ambiguous
    with file restore) to avoid false positives.

    Short creation flags are recognised as a separate token (``-b X``),
    value-attached (``-bX``), or inside a general short-flag cluster
    (``-qb X``, ``-qbX``) [SEC-01].
    """
    _CREATION_LETTERS = {
        "checkout": frozenset({"b", "B"}),
        "switch": frozenset({"c", "C"}),
    }

    def creation_from_short() -> str | None:
        letters = _CREATION_LETTERS.get(subcmd)
        if not letters:
            return None
        for i, tok in enumerate(subargs):
            # Only short-flag clusters: leading single dash, not "--" / "-".
            if not tok.startswith("-") or tok.startswith("--") or tok == "-":
                continue
            body = tok[1:]
            for j, ch in enumerate(body):
                if ch not in letters:
                    # Non-creation short letter: keep scanning the cluster.
                    continue
                attached = body[j + 1 :]
                if attached:
                    return attached
                if i + 1 < len(subargs):
                    cand = subargs[i + 1]
                    return cand if not is_flag(cand) else None
                return None
        return None

    def creation_from_orphan() -> str | None:
        if subcmd != "checkout":
            return None
        for i, tok in enumerate(subargs):
            if tok == "--orphan":
                if i + 1 < len(subargs):
                    cand = subargs[i + 1]
                    return cand if not is_flag(cand) else None
                return None
            if tok.startswith("--orphan="):
                val = tok[len("--orphan=") :]
                return val or None
        return None

    def first_positional() -> str | None:
        for tok in subargs:
            if tok != "--" and not is_flag(tok):
                return tok
        return None

    if subcmd == "checkout":
        orphan = creation_from_orphan()
        if orphan is not None:
            return orphan
        return creation_from_short()
    if subcmd == "switch":
        created = creation_from_short()
        return created if created is not None else first_positional()
    return None


# Nested-shell carriers that hide a real git invocation inside a command string.
# Bound recursion so a self-referential ``bash -c 'bash -c ...'`` cannot spin.
_SHELL_C_VERBS = frozenset({"bash", "sh"})
_SHELL_NEST_MAX_DEPTH = 3


def _command_string_from_shell_stage(verb: str, args: list[str]) -> str | None:
    """Return the command-string payload for bash/sh -c or eval, else None."""
    if verb in _SHELL_C_VERBS:
        for i, tok in enumerate(args):
            if tok == "-c":
                return args[i + 1] if i + 1 < len(args) else None
            if not tok.startswith("-") or tok.startswith("--") or tok == "-":
                continue
            # General short-flag cluster may embed ``c`` (e.g. ``-lc``).
            body = tok[1:]
            for j, ch in enumerate(body):
                if ch != "c":
                    continue
                attached = body[j + 1 :]
                if attached:
                    return attached
                return args[i + 1] if i + 1 < len(args) else None
        return None
    if verb == "eval":
        if not args:
            return None
        return " ".join(args)
    return None


def _git_switch_intents(
    command: str,
    repo_root: Path,
    *,
    _depth: int = 0,
) -> list[tuple[Path | None, str]] | object:
    """Parse ``(effective_target_dir, branch)`` for each branch creation/switch.

    Mirrors ``scan_bash_command``'s joiner-aware effective-cwd tracking so a
    ``cd <dir> && git switch -c X`` and ``git -C <dir> checkout -b X`` resolve
    against the worktree the command actually targets — not the harness cwd.
    Pure parsing (no git calls); ``target_dir is None`` means "the cwd worktree".

    Returns ``_COULD_NOT_DETERMINE`` when parser helpers cannot be imported —
    distinct from an empty list (determined "no switch intent") [OBS-08][AGT-10].
    """
    try:
        from _bash_isolation_guard import (
            _CD_PROPAGATING_JOINERS,
            _is_flag,
            _iter_stages,
            _resolve_cd_target,
            _split_git_global_opts,
            _verb_of,
        )
    except ImportError:
        return _COULD_NOT_DETERMINE
    try:
        root_abs = repo_root.expanduser().resolve(strict=False)
    except OSError:
        root_abs = repo_root
    intents: list[tuple[Path | None, str]] = []
    effective_cwd: Path | None = root_abs
    pending_cd: tuple[Path | None] | None = None
    for joiner, _stage, tokens in _iter_stages(command):
        if pending_cd is not None:
            effective_cwd = pending_cd[0] if joiner in _CD_PROPAGATING_JOINERS else None
            pending_cd = None
        verb, args = _verb_of(tokens)
        if verb == "cd":
            pending_cd = (_resolve_cd_target(args, effective_cwd),)
            continue
        if verb != "git":
            # Nested shell/eval may hide a real git stage inside a -c/eval string.
            if _depth < _SHELL_NEST_MAX_DEPTH:
                nested_cmd = _command_string_from_shell_stage(verb, args)
                if nested_cmd:
                    nested_root = effective_cwd if effective_cwd is not None else root_abs
                    inner = _git_switch_intents(
                        nested_cmd, nested_root, _depth=_depth + 1
                    )
                    if inner is _COULD_NOT_DETERMINE:
                        return _COULD_NOT_DETERMINE
                    intents.extend(inner)  # type: ignore[arg-type]
            continue
        git_dir, rest = _split_git_global_opts(args)
        if not rest:
            continue
        branch = _switch_target_branch(rest[0], rest[1:], _is_flag)
        if not branch:
            continue
        target_dir = _resolve_cd_target([git_dir], effective_cwd) if git_dir is not None else effective_cwd
        intents.append((target_dir, branch))
    return intents


def _worktree_toplevel(directory: Path) -> str | None | object:
    timeout = _probe_timeout(_WORKTREE_TOPLEVEL_TIMEOUT_CAP)
    if timeout <= 0:
        # Budget exhausted: could-not-determine for this probe [RES-03].
        return _BUDGET_EXHAUSTED
    try:
        proc = subprocess.run(
            ["git", "-C", str(directory), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        # Timeout is budget exhaustion, not "not a git repo". Returning None
        # would silently skip this intent (fail-open) [OBS-08][RES-03].
        return _BUDGET_EXHAUSTED
    except OSError:
        # Missing/non-executable git on PATH, etc. — could-not-determine, not
        # a determined "not a git directory" skip [OBS-08][AGT-10].
        return _COULD_NOT_DETERMINE
    except subprocess.SubprocessError:
        return _COULD_NOT_DETERMINE
    if proc.returncode != 0 or not proc.stdout.strip():
        # Determined negative: this path is not a git toplevel — skip intent.
        return None
    return proc.stdout.strip()


# ---------------------------------------------------------------------------
# Git directory validation (same contract as scripts/dev_install.py —
# _validate_headref / _is_git_directory). Duplicated here so the hook stays
# free of package imports while refusing litter ``.git`` shapes git rejects.
# ---------------------------------------------------------------------------

_GIT_HEADREF_WS = " \t\n\r"


def _validate_headref(head: Path) -> bool:
    """Port of git's ``validate_headref`` (refs.c): is *head* a well-formed HEAD.

    Accepts a symlink whose link text begins ``refs/`` (unresolved — a dangling
    symlink to ``refs/heads/main`` is a valid unborn branch), a regular file
    whose first 40 characters are hexadecimal (detached HEAD), or a regular
    file beginning ``ref:`` whose remainder, after leading space/tab/LF/CR only,
    begins ``refs/``. ``PermissionError`` is treated as success (unreadable entry
    pins as a repository; deliberate divergence from git, which walks past —
    this helper has no fatal error channel); other ``OSError`` fails.
    """
    try:
        st = os.lstat(head)
    except PermissionError:
        return True
    except OSError:
        return False

    mode = st.st_mode
    if stat.S_ISLNK(mode):
        try:
            link = os.readlink(head)
        except PermissionError:
            return True
        except OSError:
            return False
        return link.startswith("refs/")

    if not stat.S_ISREG(mode):
        return False

    try:
        with open(head, "rb") as fh:
            data = fh.read(255)
    except PermissionError:
        return True
    except OSError:
        return False

    text = data.decode("utf-8", errors="replace")
    if len(text) >= 40 and all(c in "0123456789abcdefABCDEF" for c in text[:40]):
        return True
    if text.startswith("ref:"):
        return text[4:].lstrip(_GIT_HEADREF_WS).startswith("refs/")
    return False


def _is_git_directory(suspect: Path) -> bool:
    """Port of git's ``is_git_directory`` (setup.c): three filesystem checks.

    Requires a well-formed ``HEAD`` via :func:`_validate_headref`, then that
    ``objects`` and ``refs`` are accessible under the *common* directory with
    ``os.access(..., X_OK)`` (git's probe — executable bit, not is-dir). The
    common directory defaults to *suspect* only when ``suspect/commondir`` is
    absent. When the entry is present it must be a readable regular file
    (after following a symlink): a zero-length file is refused (git dies with
    ``failed to read commondir``); otherwise the whole body is read and a
    trailing run of CR/LF only is stripped. A non-empty result names the
    common directory, resolved against *suspect* when relative; a body that
    is only trailing CR/LF leaves the default (*suspect*) in place (git
    accepts that shape). A present but unusable entry rejects the candidate
    (returns False): non-regular target (directory, FIFO, whether named
    directly or reached through a link — so ``open`` never blocks), broken
    symlink, undecodable body, or any other read/probe failure. Only a missing
    ``commondir`` entry keeps the default common directory. Linked worktrees
    hold ``HEAD`` + ``commondir`` without local ``objects``/``refs``; submodule
    gitdirs under ``.git/modules`` hold all three locally with no
    ``commondir``. ``PermissionError`` while reading ``commondir`` is the sole
    deliberate pin (returns True; git walks past — this helper has no fatal
    error channel); every other present-but-unusable probe failure rejects.
    """
    if not _validate_headref(suspect / "HEAD"):
        return False

    common = suspect
    try:
        commondir = suspect / "commondir"
        try:
            # Existence without following: a missing entry keeps the default
            # common dir. A present entry that cannot be opened as a regular
            # file must reject — not fall through as if the entry were absent.
            os.lstat(commondir)
        except FileNotFoundError:
            pass
        else:
            # Dereference deliberately: git's open() follows a symlink to a
            # regular file. S_ISREG still refuses a FIFO target (stat never
            # blocks on a FIFO; only open does), so the long-lived-server
            # hang guard survives for both a named FIFO and a link to one.
            try:
                cd_st = os.stat(commondir)
            except FileNotFoundError:
                # Broken symlink: entry exists, target does not.
                return False
            if not stat.S_ISREG(cd_st.st_mode):
                return False
            # Zero-length file: git refuses with "failed to read commondir".
            # Newlines-only (non-zero size, empty after CR/LF strip) keeps the
            # default common dir and must not take this arm.
            if cd_st.st_size == 0:
                return False
            with open(commondir, "rb") as fh:
                raw = fh.read().decode("utf-8")
            # Whole file, trailing CR/LF run only (not a first-line read; not
            # spaces/tabs). Matches git's commondir parse.
            body = raw.rstrip("\r\n")
            if body:
                common_path = Path(body)
                common = common_path if common_path.is_absolute() else suspect / common_path
    except PermissionError:
        return True
    except (OSError, UnicodeDecodeError, ValueError):
        # ValueError: NUL in the path must not abort the walk (not an OSError).
        # Present-but-unusable commondir rejects the candidate (git: failed to
        # read commondir); only PermissionError above is the deliberate pin.
        return False

    for name in ("objects", "refs"):
        try:
            # git probes with access(X_OK): accepts an executable regular file,
            # rejects mode-644 files and unsearchable (mode-000) directories.
            # os.access returns False on permission failure rather than raising.
            if not os.access(common / name, os.X_OK):
                return False
        except (OSError, ValueError):
            return False
    return True


def _git_worktrees_dir_reachable(repo_root: Path) -> bool:
    """True when a ``.git/worktrees`` directory is reachable from ``repo_root``.

    Scopes the primary-unresolvable fail-closed arm so bare-backed worktrees,
    submodules, and non-git cwds never inherit multi-worktree enforcement
    [OBS-08]. A bare repo's ``worktrees/`` parent is not named ``.git``, so
    those layouts correctly report out-of-scope.

    Litter ``.git`` directories (no validated HEAD/objects/refs) and gitfiles
    that lack the exact byte-zero ``gitdir: `` prefix do not short-circuit the
    walk — they are skipped so a genuine multi-worktree layout higher up still
    scopes the guard.
    """
    try:
        cursor = Path(repo_root).resolve(strict=False)
    except OSError:
        cursor = Path(repo_root)
    for _ in range(128):
        git_entry = cursor / ".git"
        try:
            if git_entry.is_dir():
                # Require a real git directory; empty/litter .git must not
                # return False (caller treats False as out-of-scope allow).
                if not _is_git_directory(git_entry):
                    pass
                else:
                    return (git_entry / "worktrees").is_dir()
            elif git_entry.is_file():
                try:
                    # Binary read so universal-newlines cannot swallow CR as
                    # a newline; we only rstrip an explicit CR/LF run below.
                    text = git_entry.read_bytes().decode("utf-8")
                except (OSError, UnicodeDecodeError):
                    return False
                # Exact gitfile prefix at byte zero (git rejects all variants).
                text = text.rstrip("\r\n")
                if not text.startswith("gitdir: "):
                    # Invalid gitfile: keep walking rather than fail-open.
                    pass
                else:
                    payload = text[len("gitdir: ") :]
                    if not payload:
                        # Empty payload after the exact prefix: same class as
                        # other malformed gitfiles — keep walking rather than
                        # return False (caller treats False as out-of-scope allow).
                        pass
                    else:
                        gd = Path(payload)
                        if not gd.is_absolute():
                            gd = cursor / gd
                        try:
                            gd = gd.resolve(strict=False)
                        except OSError:
                            return False
                        # .../<primary>/.git/worktrees/<name>
                        if (
                            gd.parent.name == "worktrees"
                            and gd.parent.parent.name == ".git"
                        ):
                            return True
                        return False
        except OSError:
            return False
        parent = cursor.parent
        if parent == cursor:
            return False
        cursor = parent
    return False


def _build_switch_block_message(branch: str) -> str:
    return (
        "BLOCKED: refusing to switch the PRIMARY (root) worktree to non-main "
        f"branch '{branch}'.\n\n"
        "The root worktree must stay on main so a concurrent session's "
        "main-integration commit never lands on a feature branch. Use a LINKED "
        "worktree for feature work:\n"
        '  make task-start TASK=<task-ref> OBJECTIVE="..."\n'
        "  # or, ad hoc:\n"
        "  git worktree add ../<repo>-<task-id> -b <branch>\n\n"
        "If this is an intentional root operation (e.g. release/rebase), prefix "
        "the bypass token to the WHOLE command:\n"
        f"  {_BRANCH_SWITCH_BYPASS_PRIMARY}=1 <your full command>\n"
        "Every bypass is logged to .task-state/branch_isolation_guard.jsonl.\n\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory"
    )


def _build_switch_could_not_determine_message(reason: str) -> str:
    """Fail-closed message when the switch scan cannot finish [AGT-10][OBS-08]."""
    return (
        "BLOCKED: could not complete the root-worktree branch-switch check "
        f"({reason}). Refusing rather than reporting a silent clean result.\n\n"
        "Re-run the command, or if this is an intentional root operation, "
        "prefix the bypass token to the WHOLE command:\n"
        f"  {_BRANCH_SWITCH_BYPASS_PRIMARY}=1 <your full command>\n"
        "Every bypass is logged to .task-state/branch_isolation_guard.jsonl.\n"
    )


def _detect_root_branch_switch(command: str, *, repo_root: Path) -> str | None:
    """Block message when a stage switches the PRIMARY worktree to non-main.

    Runs regardless of the cwd's current branch (GPR-1): a
    ``git -C <primary> checkout -b X`` issued from a linked worktree is the real
    misroute and must be caught. Only enforced when linked worktrees exist
    (multi-worktree workflow). A zero grant mid-loop is could-not-determine for
    the whole scan (fail closed), never a per-intent skip [OBS-08][AGT-10].
    """
    # Switch-intent parse sits outside the identity-probe try below; wrap it
    # alone so expanduser RuntimeError (e.g. cd ~nosuchuser) fails closed with
    # could-not-determine rather than raising out of the hook [AGT-10][OBS-08].
    try:
        intents = _git_switch_intents(command, repo_root)
    except Exception:
        return _build_switch_could_not_determine_message(
            "could not complete switch-intent parse for root branch-switch guard"
        )
    if intents is _COULD_NOT_DETERMINE:
        # Missing parser deps is not the same fact as "no switch intent" [OBS-08].
        return _build_switch_could_not_determine_message(
            "could not import switch-intent parser dependencies"
        )
    if not intents:
        return None
    try:
        from _worktree_identity import has_linked_worktrees, primary_workspace_root
    except ImportError:
        # Non-empty intents with no identity helper must fail closed [AGT-10].
        return _build_switch_could_not_determine_message(
            "could not import worktree identity helper for root branch-switch guard"
        )
    primary_timeout = _probe_timeout(_PRIMARY_ROOT_TIMEOUT_CAP)
    if primary_timeout <= 0:
        # Deadline exhausted before identity probes: whole scan undetermined.
        return _build_switch_could_not_determine_message(
            "subprocess deadline exhausted before primary-worktree identity probe"
        )
    # Broad fail-closed handler: any probe exception (ValueError, UnicodeDecodeError,
    # OSError, TypeError, ...) must return could-not-determine rather than raise or
    # return None. A PreToolUse crash exits 1 (non-blocking) and skips the write
    # scan, converting a guard into no guard [AGT-10][OBS-08].
    # primary_timeout still gates entry when the shared deadline is exhausted;
    # primary_workspace_root itself is pure FS (no timeout=) [ARCH-13][RES-03].
    _ = primary_timeout
    try:
        primary_raw = primary_workspace_root(repo_root)
        if primary_raw is None:
            # Only fail closed when multi-worktree enforcement is in scope.
            # primary_workspace_root returns None for bare-backed worktrees,
            # submodules, broken gitdir targets, and non-git cwds — single-
            # worktree consumers must never be blocked by this arm [OBS-08].
            if not _git_worktrees_dir_reachable(repo_root):
                print(
                    "guard-bash-main-branch: could not determine primary "
                    "worktree; root branch-switch guard not in multi-worktree "
                    "scope [could-not-determine]",
                    file=sys.stderr,
                )
                return None
            return _build_switch_could_not_determine_message(
                "could not determine primary worktree from on-disk git metadata"
            )
        primary = str(Path(primary_raw).resolve(strict=False))
        if not has_linked_worktrees(primary):
            return None
    except Exception:
        return _build_switch_could_not_determine_message(
            "could not determine primary worktree identity for root branch-switch guard"
        )
    try:
        cwd_top = str(repo_root.expanduser().resolve(strict=False))
    except OSError:
        cwd_top = str(repo_root)
    for target_dir, branch in intents:
        if branch in _PROTECTED_BRANCHES:
            continue
        # Each iteration draws from the shared budget via _worktree_toplevel;
        # a zero grant aborts the whole scan rather than skipping the intent
        # that may target the primary [ARCH-13][OBS-08].
        if target_dir is None:
            top: str | None | object = cwd_top
        else:
            top = _worktree_toplevel(target_dir)
        if top is _BUDGET_EXHAUSTED:
            return _build_switch_could_not_determine_message(
                "subprocess deadline exhausted mid switch-intent scan"
            )
        if top is _COULD_NOT_DETERMINE:
            # Any intent that could not be probed fails closed for the whole
            # command — a later clean intent must not recover to allow [OBS-08].
            return _build_switch_could_not_determine_message(
                "could not resolve worktree toplevel for a switch intent"
            )
        if not top:
            continue  # determined negative: path is not a git toplevel
        if str(Path(str(top)).resolve(strict=False)) == primary:
            return _build_switch_block_message(branch)
    return None


def main() -> int:
    global _timeout_budget
    _timeout_budget = _SubprocessTimeoutBudget(_HOOK_SUBPROCESS_BUDGET_SECONDS)
    try:
        return _main()
    finally:
        _timeout_budget = None


def _main() -> int:
    repo_root = _repo_root()
    if repo_root is _COULD_NOT_DETERMINE:
        # Deleted cwd / hung rev-parse / unreadable cwd: fail closed rather
        # than crash (exit 1 non-blocking) or key checks off a wrong root.
        print(
            "BLOCKED: could not determine repository root for Bash guard "
            "(fail closed) [could-not-determine].\n\n"
            "Re-run from a live worktree, or if this is intentional, prefix "
            "the appropriate bypass token to the WHOLE command.",
            file=sys.stderr,
        )
        return 2
    assert isinstance(repo_root, Path)

    payload = _load_payload()
    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(payload, expected="PreToolUse")
    except ImportError:
        pass
    tool_name = payload.get("toolName") or payload.get("tool_name") or ""
    if tool_name != "Bash":
        return 0

    command = _extract_command(payload)
    if not command:
        return 0

    sys.path.insert(0, str(repo_root / "scripts" / "hooks"))

    # Probe branch BEFORE the switch path can spend the entire deadline, so a
    # single hung checkout intent cannot starve the write-scan gate into a
    # silent skip [RES-03][D1].
    branch = _current_branch(repo_root)

    # Root-worktree branch-switch guard — runs independent of the cwd branch
    # (GPR-1) so cross-worktree `git -C <primary>` switches are caught too.
    switch_block = _detect_root_branch_switch(command, repo_root=repo_root)
    if switch_block is not None:
        bypass = _branch_switch_bypass(command)
        if bypass is None:
            print(switch_block, file=sys.stderr)
            _record_terminal_guard_block(
                command=command,
                blocked=["root-branch-switch"],
                decision="block",
            )
            return 2
        source, var_name = bypass
        if var_name == _BRANCH_SWITCH_BYPASS_LEGACY:
            print(
                f"(deprecated) {_BRANCH_SWITCH_BYPASS_LEGACY} is the legacy name; "
                f"use {_BRANCH_SWITCH_BYPASS_PRIMARY}=1 instead.",
                file=sys.stderr,
            )
        print(
            f"(bypass) {var_name}=1 ({source}) — allowing root branch switch but logging",
            file=sys.stderr,
        )
        _log_bypass(repo_root, command, [f"root-branch-switch:{command}"], source=source, var_name=var_name)

    # Write-scan path: only skip when the branch is a *known* non-main.
    # None is could-not-determine; empty string is detached HEAD (determined).
    # Both still run the write scan [RES-03][OBS-08][AGT-10].
    if branch is not None and branch and branch not in {"main", "master"}:
        return 0
    if branch is None:
        branch_label = "(unknown)"
        print(
            "guard-bash-main-branch: could not determine current branch; "
            "running protected-path write scan (fail-safe) [could-not-determine]",
            file=sys.stderr,
        )
    elif branch == "":
        branch_label = "(detached HEAD)"
        print(
            "guard-bash-main-branch: detached HEAD (no current branch); "
            "running protected-path write scan",
            file=sys.stderr,
        )
    else:
        branch_label = branch
    try:
        from _bash_isolation_guard import scan_bash_command
        from _harness_protocol import (
            HarnessContractMissingError,
            HarnessContractMissingPolicy,
            handle_missing_contract,
            load_branch_isolation_policy,
        )
    except ImportError as exc:
        print(f"guard-bash-main-branch: import failed — {exc}", file=sys.stderr)
        return 0

    # internal: this is an end-user PreToolUse hook; a missing
    # contract YAML must warn and exit 0 instead of blocking the user's
    # Bash command. Hard-fail enforcement lives in the internal
    # verification suite (``check_main_clean.py --mode block``).
    try:
        policy = load_branch_isolation_policy(repo_root)
    except HarnessContractMissingError as exc:
        return handle_missing_contract(exc, policy=HarnessContractMissingPolicy.WARN)

    blocked = scan_bash_command(command, repo_root, policy)
    if not blocked:
        return 0

    bypass = _bypass_request(command)
    if bypass is not None:
        source, var_name = bypass
        if var_name == _BYPASS_ENV_LEGACY:
            print(
                f"(deprecated) {_BYPASS_ENV_LEGACY} is the legacy bypass name; "
                f"use {_BYPASS_ENV_PRIMARY}=1 instead.",
                file=sys.stderr,
            )
        print(
            f"(bypass) {var_name}=1 ({source}) — allowing but logging",
            file=sys.stderr,
        )
        _log_bypass(repo_root, command, blocked, source=source, var_name=var_name)
        return 0

    rendered = "\n".join(f"  - {path}" for path in blocked)
    print(
        "BLOCKED: Bash command appears to write to or delete protected paths on main.\n\n"
        f"Branch: {branch_label}\n"
        f"Protected paths touched by this command:\n{rendered}\n\n"
        "Use the Edit/Write tool (which has proper path semantics) or move the change\n"
        "into a LINKED feature worktree first (do NOT `git checkout -b` in root):\n"
        '  make task-start TASK=<task-ref> OBJECTIVE="..."\n\n'
        "If the detection is a false positive (e.g. scanning, not writing), re-run\n"
        "with the bypass token prefixed to the WHOLE command:\n"
        f"  {_BYPASS_ENV_PRIMARY}=1 <your full command>\n"
        "(a mid-command assignment after && or ; does not bypass). Every bypass is\n"
        "logged to .task-state/branch_isolation_guard.jsonl.\n\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory",
        file=sys.stderr,
    )
    # internal [OBS-01]: instrument-at-write-time. Best-effort,
    # never-raise, detached — must not change exit-2 block behavior.
    _record_terminal_guard_block(command=command, blocked=blocked, decision="block")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
