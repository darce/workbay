#!/usr/bin/env python3
"""Branch-naming gate helper used by post-checkout / pre-commit / pre-push.

This single delegate is invoked by all three git-side gates. The
``--trigger`` argument selects gate-specific exit semantics per implementation note:

- ``post-checkout``  : warn-only — always exits 0, even on violations.
- ``pre-commit``     : implementation note — hard block + bounded override.
- ``pre-push``       : implementation note — hard block + distinct override env var.

implementation note ships only the ``post-checkout`` branch; ``pre-commit`` and
``pre-push`` raise ``SystemExit(2)`` so a stale invocation is loud,
not silent. They will be filled in by Slices 4 and 4b.

The helper loads ``TASK_REF_RE`` from the canonical lightweight
``workbay_protocol.branch_naming`` module when available, with a literal
fallback so git hooks keep working under plain ``python3`` before MCP package
imports are on ``sys.path``. The protected-class carve-out
(``main``/``master``/``release/*``/``hotfix/*``) lives next to the
validator in ``_branch_isolation_guard`` to keep the taxonomy in one
place.
"""

from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Callable

# Mirror of the validator's protected-class set. Kept inline (rather
# than imported from ``_branch_isolation_guard``) because this helper
# runs in the consumer-bootstrapped ``scripts/hooks/`` surface where
# the import path is the consumer repo, not the monorepo. A tiny
# duplication beats a fragile sys.path manipulation in a hook script.
_PROTECTED_BRANCH_NAMES = frozenset({"main", "master"})
_PROTECTED_BRANCH_PREFIXES: tuple[str, ...] = ("release/", "hotfix/")

# implementation note — bounded override audit. The 2 s wall-clock timeout is the
# load-bearing invariant: the override path NEVER blocks the commit.
# A hung handoff DB or stuck mcp client must fall through to the
# fallback log, not stall the operator.
_OVERRIDE_LOG_RELPATH = ".task-state/branch_naming_overrides.log"
_OVERRIDE_TIMEOUT_S = 2.0
_OVERRIDE_ENV_VAR = "WORKBAY_ALLOW_NONCONFORMING_BRANCH"
_OVERRIDE_REASON_ENV_VAR = "WORKBAY_ALLOW_NONCONFORMING_BRANCH_REASON"

# implementation note — distinct pre-push override env var. Operators who set the
# commit-side override (often after a series of WIP commits) MUST NOT have
# that leniency silently leak across the publish boundary, so the push
# gate reads its own variable.
_PUSH_OVERRIDE_LOG_RELPATH = ".task-state/branch_naming_push_overrides.log"
_PUSH_OVERRIDE_ENV_VAR = "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH"
_PUSH_OVERRIDE_REASON_ENV_VAR = "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON"


def _resolve_override_env(env: dict, canonical: str) -> str | None:
    """Resolve a ``WORKBAY_*`` override from the injected ``env`` mapping.

    Unlike the shared ``_interp`` resolver this honors the injected ``env``
    mapping (the gate's dependency-injection seam) rather than ``os.environ``
    directly. A blank/whitespace-only value is treated as unset; the returned
    value is otherwise verbatim, preserving the exact ``!= "1"`` and
    reason-string semantics.
    """
    value = env.get(canonical)
    if value is not None and value.strip():
        return value
    return None


def _fallback_task_ref_re() -> re.Pattern[str]:
    return re.compile(
        r"^feature/"
        r"(?=[a-z])"
        r"(?=[a-z0-9-]*\d)"
        r"(?P<task_ref>[a-z0-9]+(?:-[a-z0-9]+)+)"
        r"$"
    )


def _load_task_ref_re() -> re.Pattern[str]:
    """Load the branch grammar without requiring full MCP runtime imports."""
    hook_path = Path(__file__).resolve()
    for parent in hook_path.parents:
        candidate = (
            parent
            / "packages"
            / "workbay-protocol"
            / "src"
            / "workbay_protocol"
            / "branch_naming.py"
        )
        if not candidate.is_file():
            continue
        spec = importlib.util.spec_from_file_location(
            "_workbay_branch_naming_for_hook",
            candidate,
        )
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        task_ref_re = getattr(module, "TASK_REF_RE", None)
        if isinstance(task_ref_re, re.Pattern):
            return task_ref_re

    try:
        from workbay_protocol.branch_naming import TASK_REF_RE as task_ref_re  # noqa: PLC0415

        return task_ref_re
    except Exception:
        pass

    try:
        from workbay_handoff_mcp import TASK_REF_RE as task_ref_re  # noqa: PLC0415

        return task_ref_re
    except Exception:
        return _fallback_task_ref_re()


TASK_REF_RE = _load_task_ref_re()


def _current_branch() -> str | None:
    """Return the current git branch, or None on detached HEAD / error.

    Bounded by a 2 s timeout so a wedged git process never wedges the
    post-checkout hook (warn-only contract demands fast exit on any
    error).
    """
    try:
        proc = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _is_conforming_or_protected(branch: str) -> bool:
    """Classify a branch using the canonical branch grammar."""
    if branch in _PROTECTED_BRANCH_NAMES:
        return True
    for prefix in _PROTECTED_BRANCH_PREFIXES:
        if branch.startswith(prefix):
            return True
    return TASK_REF_RE.match(branch) is not None


def _suggest_branch_for_active_task() -> str | None:
    """Render a "did you mean ..." suggestion from the active task ref.

    Returns ``None`` on cold-start (no active task) or any failure —
    callers fall back to a generic register-a-task message. The
    warn-only contract requires this never to raise.
    """
    try:
        from workbay_handoff_mcp import (  # noqa: PLC0415
            format_suggested_branch_name,
            get_handoff_state,
        )
    except Exception:
        return None
    try:
        state = get_handoff_state(sections="identity")
    except Exception:
        return None
    if not isinstance(state, dict):
        return None
    data = state.get("data") if "data" in state else state
    if not isinstance(data, dict):
        return None
    active = data.get("active")
    if not isinstance(active, dict):
        return None
    task_ref = active.get("task_ref")
    if not task_ref:
        return None
    try:
        return format_suggested_branch_name(task_ref)
    except Exception:
        return None


def _format_warning(branch: str, suggested: str | None) -> str:
    lines = [
        f"WARNING: Branch '{branch}' does not match the canonical "
        "feature-branch grammar.",
        "  Rule: workbay_protocol.branch_naming.TASK_REF_RE",
    ]
    if suggested:
        lines.append(f"  Did you mean: {suggested}")
    else:
        lines.append(
            "  Register a task (e.g. `make task-start TASK=<task-ref>`) "
            "then rename to feature/<task-ref>."
        )
    lines.append(
        "  This is a warn-only gate. pre-commit will hard-block; set "
        "WORKBAY_ALLOW_NONCONFORMING_BRANCH=1 to override (audited)."
    )
    return "\n".join(lines) + "\n"


def run_post_checkout(
    branch: str | None,
    *,
    suggester=_suggest_branch_for_active_task,
    stream=sys.stderr,
) -> int:
    """Warn-only post-checkout entry point.

    Exits 0 unconditionally — operators may still ``git branch -m``
    before any commit, so this layer never blocks. The pre-commit /
    pre-push gates own the hard-block contract.
    """
    if not branch:
        return 0
    if _is_conforming_or_protected(branch):
        return 0
    suggested = suggester()
    stream.write(_format_warning(branch, suggested))
    return 0


def _commit_author() -> str:
    try:
        proc = subprocess.run(
            ["git", "config", "user.name"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "<unknown>"
    return proc.stdout.strip() or "<unknown>"


def _repo_root() -> Path | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    raw = proc.stdout.strip()
    return Path(raw) if raw else None


def _format_pre_commit_block(branch: str) -> str:
    return (
        f"BLOCKED: Cannot commit on '{branch}' — branch name does not "
        "match the canonical feature-branch grammar.\n"
        "  Rule: workbay_protocol.branch_naming.TASK_REF_RE\n"
        "  Allowed classes: protected (main/master/release/*/hotfix/*)\n"
        "                 | conforming feature/<task-ref> (lowercase, must contain a digit)\n"
        "  Override (audited):\n"
        f"    {_OVERRIDE_ENV_VAR}=1            [required]\n"
        f"    {_OVERRIDE_REASON_ENV_VAR}=\"<why>\"  [optional]\n"
        "  Pre-push enforces a separate gate; this override does NOT carry over.\n"
        "  See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory\n"
    )


def _sanitize_branch_for_id(branch: str) -> str:
    """Replace characters disallowed in decision ids with underscores."""
    return re.sub(r"[^A-Za-z0-9-]", "_", branch)


def _record_override_decision_blocking(branch: str, reason: str | None) -> None:
    """Synchronously record an audit decision via record_event.

    Run inside a 2 s timeout-bounded executor so a stalled handoff DB
    cannot block the commit. Any exception propagates to the caller,
    which routes the failure into the fallback log path.
    """
    from workbay_handoff_mcp import record_event  # noqa: PLC0415

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    decision_id = f"branch_naming_override_{_sanitize_branch_for_id(branch)}_{timestamp}"
    record_event(
        event={
            "event_kind": "decision",
            "session": "branch_naming_override",
            "decision": decision_id,
            "rationale": (
                f"WORKBAY_ALLOW_NONCONFORMING_BRANCH override accepted for "
                f"branch '{branch}'.\n"
                f"Reason: {reason or '(not provided)'}\n"
                f"Author: {_commit_author()}\n"
            ),
        }
    )


def _record_override_with_timeout(branch: str, reason: str | None) -> str | None:
    """Return ``None`` on success, an error string on failure/timeout.

    Uses a *daemon* ``threading.Thread`` rather than
    ``ThreadPoolExecutor`` because the executor's ``__exit__`` blocks
    on ``shutdown(wait=True)`` — a hung worker would defeat the 2 s
    wall-clock budget. ``signal.alarm`` is unsuitable here because the
    helper may import third-party code that installs its own signal
    handlers. The daemon thread leaks gracefully when the helper
    subprocess exits (the entire interpreter dies a few ms later
    regardless of whether the worker has unwound).
    """
    state: dict = {"err": None, "done": False}

    def _target() -> None:
        try:
            _record_override_decision_blocking(branch, reason)
            state["done"] = True
        except Exception as exc:  # noqa: BLE001
            state["err"] = f"{type(exc).__name__}: {exc}"
            state["done"] = True

    worker = threading.Thread(target=_target, daemon=True, name="branch-naming-override-recorder")
    worker.start()
    worker.join(timeout=_OVERRIDE_TIMEOUT_S)
    if not state["done"]:
        return "timeout"
    return state["err"]


def _append_fallback_log(
    repo_root: Path,
    branch: str,
    *,
    reason: str | None,
    error: str | None,
) -> None:
    """Append a JSONL audit row when the decision-event write fails.

    The fallback log itself is best-effort — write failures here are
    swallowed so the commit still completes (the override-never-blocks
    invariant out-ranks the audit-coverage invariant).
    """
    log_path = repo_root / _OVERRIDE_LOG_RELPATH
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "branch": branch,
            "commit_author": _commit_author(),
            "reason": reason,
            "error": error,
        }
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
    except OSError:
        pass


def run_pre_commit(
    branch: str | None,
    env: dict | None = None,
    *,
    recorder: Callable[[str, str | None], str | None] | None = None,
    log_writer: Callable[..., None] | None = None,
    stream=sys.stderr,
    repo_root: Path | None = None,
) -> int:
    """Hard-block pre-commit entry point with bounded audit override.

    - Conforming or protected branch → exit 0.
    - Non-conforming + no override env var → exit 1 with cited rule.
    - Non-conforming + ``WORKBAY_ALLOW_NONCONFORMING_BRANCH=1`` →
      attempt to record a decision event; on success exit 0; on
      timeout / error / DB contention, append to
      ``.task-state/branch_naming_overrides.log`` and exit 0. The
      override path NEVER blocks the commit.
    """
    env = env if env is not None else os.environ
    if not branch or _is_conforming_or_protected(branch):
        return 0
    if _resolve_override_env(env, _OVERRIDE_ENV_VAR) != "1":
        stream.write(_format_pre_commit_block(branch))
        return 1

    reason = _resolve_override_env(env, _OVERRIDE_REASON_ENV_VAR)
    rec = recorder if recorder is not None else _record_override_with_timeout
    err = rec(branch, reason)
    if err is None:
        stream.write(
            f"branch-naming override accepted for '{branch}' "
            "(decision event recorded).\n"
        )
        return 0

    root = repo_root if repo_root is not None else (_repo_root() or Path.cwd())
    writer = log_writer if log_writer is not None else _append_fallback_log
    writer(root, branch, reason=reason, error=err)
    stream.write(
        f"branch-naming override accepted for '{branch}' (audit "
        f"fallback: {err}; logged to {_OVERRIDE_LOG_RELPATH}).\n"
    )
    return 0


def _format_pre_push_block(branch: str) -> str:
    return (
        f"BLOCKED: Cannot push '{branch}' — branch name does not match the "
        "canonical feature-branch grammar.\n"
        "  Rule: workbay_protocol.branch_naming.TASK_REF_RE\n"
        "  Allowed classes: protected (main/master/release/*/hotfix/*)\n"
        "                 | conforming feature/<task-ref> (lowercase, must contain a digit)\n"
        "  Override (audited):\n"
        f"    {_PUSH_OVERRIDE_ENV_VAR}=1            [required]\n"
        f"    {_PUSH_OVERRIDE_REASON_ENV_VAR}=\"<why>\"  [optional]\n"
        "  Note: the commit-side WORKBAY_ALLOW_NONCONFORMING_BRANCH override\n"
        "        does NOT carry over to push — leniency must be re-asserted here.\n"
        "  See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory\n"
    )


def _record_push_override_decision_blocking(branch: str, reason: str | None) -> None:
    """Synchronously record a push-override audit decision.

    Uses ``session='branch_naming_push_override'`` (distinct from the
    commit-side session) so the two override populations are separable
    in audit queries.
    """
    from workbay_handoff_mcp import record_event  # noqa: PLC0415

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    decision_id = (
        f"branch_naming_push_override_{_sanitize_branch_for_id(branch)}_{timestamp}"
    )
    record_event(
        event={
            "event_kind": "decision",
            "session": "branch_naming_push_override",
            "decision": decision_id,
            "rationale": (
                f"WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH override accepted for "
                f"branch '{branch}'.\n"
                f"Reason: {reason or '(not provided)'}\n"
                f"Author: {_commit_author()}\n"
            ),
        }
    )


def _record_push_override_with_timeout(branch: str, reason: str | None) -> str | None:
    """Mirror of ``_record_override_with_timeout`` for the push gate.

    Same daemon-thread + 2 s wall-clock contract as the commit-side
    recorder so a hung handoff DB cannot wedge ``git push``.
    """
    state: dict = {"err": None, "done": False}

    def _target() -> None:
        try:
            _record_push_override_decision_blocking(branch, reason)
            state["done"] = True
        except Exception as exc:  # noqa: BLE001
            state["err"] = f"{type(exc).__name__}: {exc}"
            state["done"] = True

    worker = threading.Thread(
        target=_target, daemon=True, name="branch-naming-push-override-recorder"
    )
    worker.start()
    worker.join(timeout=_OVERRIDE_TIMEOUT_S)
    if not state["done"]:
        return "timeout"
    return state["err"]


def _append_push_fallback_log(
    repo_root: Path,
    branch: str,
    *,
    reason: str | None,
    error: str | None,
) -> None:
    """Push-side mirror of ``_append_fallback_log``.

    Writes to a distinct file (``branch_naming_push_overrides.log``) so
    operators can audit publish-time leniency separately from commit-time.
    Best-effort — write failures are swallowed (override-never-blocks).
    """
    log_path = repo_root / _PUSH_OVERRIDE_LOG_RELPATH
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "branch": branch,
            "commit_author": _commit_author(),
            "reason": reason,
            "error": error,
        }
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
    except OSError:
        pass


def run_pre_push(
    branch: str | None,
    env: dict | None = None,
    *,
    recorder: Callable[[str, str | None], str | None] | None = None,
    log_writer: Callable[..., None] | None = None,
    stream=sys.stderr,
    repo_root: Path | None = None,
) -> int:
    """Hard-block pre-push entry point with a distinct bounded override.

    Mirror of :func:`run_pre_commit` with two key differences:

    - reads ``WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH`` (not the commit
      env var) so commit-side leniency does not leak across the publish
      boundary;
    - records to ``session='branch_naming_push_override'`` and falls back
      to ``.task-state/branch_naming_push_overrides.log``.
    """
    env = env if env is not None else os.environ
    if not branch or _is_conforming_or_protected(branch):
        return 0
    if _resolve_override_env(env, _PUSH_OVERRIDE_ENV_VAR) != "1":
        stream.write(_format_pre_push_block(branch))
        return 1

    reason = _resolve_override_env(env, _PUSH_OVERRIDE_REASON_ENV_VAR)
    rec = recorder if recorder is not None else _record_push_override_with_timeout
    err = rec(branch, reason)
    if err is None:
        stream.write(
            f"branch-naming push override accepted for '{branch}' "
            "(decision event recorded).\n"
        )
        return 0

    root = repo_root if repo_root is not None else (_repo_root() or Path.cwd())
    writer = log_writer if log_writer is not None else _append_push_fallback_log
    writer(root, branch, reason=reason, error=err)
    stream.write(
        f"branch-naming push override accepted for '{branch}' (audit "
        f"fallback: {err}; logged to {_PUSH_OVERRIDE_LOG_RELPATH}).\n"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="implementation note branch-naming gate helper.")
    parser.add_argument(
        "--trigger",
        required=True,
        choices=("post-checkout", "pre-commit", "pre-push"),
        help="Which git gate is calling this helper.",
    )
    args = parser.parse_args(argv)
    if args.trigger == "post-checkout":
        return run_post_checkout(_current_branch())
    if args.trigger == "pre-commit":
        return run_pre_commit(_current_branch())
    return run_pre_push(_current_branch())


if __name__ == "__main__":
    raise SystemExit(main())
