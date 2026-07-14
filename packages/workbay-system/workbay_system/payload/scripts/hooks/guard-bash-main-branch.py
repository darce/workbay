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
import subprocess
import sys
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


def _deps_python() -> str:
    """Interpreter carrying the workbay stack deps (mirrors _run_guard.py)."""
    try:
        from _interp import resolve_deps_python

        return resolve_deps_python() or sys.executable
    except Exception:  # noqa: BLE001 -- fail open to the launch interpreter
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


def _repo_root() -> Path:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return Path.cwd()
    if proc.returncode != 0:
        return Path.cwd()
    return Path(proc.stdout.strip() or ".")


def _current_branch(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
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
    ``checkout -b/-B <X>``, ``switch -c/-C <X>`` (creation) and plain
    ``switch <X>`` (switch never restores files). Plain ``git checkout <X>``
    is EXCLUDED (ambiguous with file restore) to avoid false positives.
    """

    def after_flag(flags: set[str]) -> str | None:
        for i, tok in enumerate(subargs):
            if tok in flags and i + 1 < len(subargs):
                cand = subargs[i + 1]
                return cand if not is_flag(cand) else None
        return None

    def first_positional() -> str | None:
        for tok in subargs:
            if tok != "--" and not is_flag(tok):
                return tok
        return None

    if subcmd == "checkout":
        return after_flag({"-b", "-B"})
    if subcmd == "switch":
        created = after_flag({"-c", "-C"})
        return created if created is not None else first_positional()
    return None


def _git_switch_intents(command: str, repo_root: Path) -> list[tuple[Path | None, str]]:
    """Parse ``(effective_target_dir, branch)`` for each branch creation/switch.

    Mirrors ``scan_bash_command``'s joiner-aware effective-cwd tracking so a
    ``cd <dir> && git switch -c X`` and ``git -C <dir> checkout -b X`` resolve
    against the worktree the command actually targets — not the harness cwd.
    Pure parsing (no git calls); ``target_dir is None`` means "the cwd worktree".
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
        return []
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


def _worktree_toplevel(directory: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(directory), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return proc.stdout.strip()


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


def _detect_root_branch_switch(command: str, *, repo_root: Path) -> str | None:
    """Block message when a stage switches the PRIMARY worktree to non-main.

    Runs regardless of the cwd's current branch (GPR-1): a
    ``git -C <primary> checkout -b X`` issued from a linked worktree is the real
    misroute and must be caught. Only enforced when linked worktrees exist
    (multi-worktree workflow); fail-open on any ambiguity / unresolvable path.
    """
    intents = _git_switch_intents(command, repo_root)
    if not intents:
        return None
    try:
        from _worktree_identity import has_linked_worktrees, primary_workspace_root
    except ImportError:
        return None
    try:
        primary = str(Path(primary_workspace_root(repo_root)).resolve(strict=False))
    except Exception:
        return None
    if not has_linked_worktrees(primary):
        return None
    try:
        cwd_top = str(repo_root.expanduser().resolve(strict=False))
    except OSError:
        cwd_top = str(repo_root)
    for target_dir, branch in intents:
        if branch in _PROTECTED_BRANCHES:
            continue
        top = cwd_top if target_dir is None else _worktree_toplevel(target_dir)
        if not top:
            continue  # fail-open: cannot resolve the target worktree
        if str(Path(top).resolve(strict=False)) == primary:
            return _build_switch_block_message(branch)
    return None


def main() -> int:
    repo_root = _repo_root()

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

    # Write-scan path below only matters when the cwd worktree is on main.
    branch = _current_branch(repo_root)
    if branch not in {"main", "master"}:
        return 0
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
        f"Branch: {branch}\n"
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
