#!/usr/bin/env python3
"""Fail-open guard runner (internal, D2 graft).

Rendered tool-guard hook commands are prefixed with this wrapper:
``python3 scripts/hooks/_run_guard.py [--fail-mode=closed] <handler> [args...]``.

Semantics:

- Handler resolves + spawns -> **byte-transparent passthrough** of stdout,
  stderr, AND exit code. Both verified block mechanisms survive untouched:
  the stdout-JSON ``permissionDecision=block`` + exit-0 shape and the
  exit-2 shape. The wrapper never parses or rewrites handler output, and
  stdin (the hook payload) is inherited by the handler process.
- Handler missing/unspawnable (errno 2 et al.) -> exit 0, no block, and a
  best-effort ``hook_infra_failure`` record through the implementation note
  errors-record channel. Fail-open applies to *infra-absence of workflow
  guards* only.
- ``--fail-mode=closed`` (rendered from a contract entry's
  ``fail_mode: closed``) opts a security-classified guard out of
  fail-open: a missing handler exits 2 with an explanatory stderr line.

Workspace-root resolution (per-harness anchor, verified 2026-06-06):

- Claude Code substitutes ``$CLAUDE_PROJECT_DIR`` in rendered commands and
  exports it to hook processes — both the anchored-absolute and the
  relative handler form resolve through it.
- Grok exports ``${GROK_WORKSPACE_ROOT}`` the same way.
- VS Code (Copilot) and Codex spawn hook processes with
  ``cwd = workspace root``, so relative paths resolve via ``os.getcwd()``.

Handlers live on BOTH hook surfaces (``scripts/hooks`` and
``.github/hooks``); when the rendered relpath does not resolve, the same
basename is tried on each surface before declaring the handler missing.

The implementation note ``resolve-every-script`` coherence gate resolves this wrapper's
own path in rendered configs (it IS the command path), so a dangling
wrapper is caught by the same detection layer as a dangling handler.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys

_HOOK_SURFACES = ("scripts/hooks", ".github/hooks")
# Test seam: a command line that replaces the errors-record invocation.
_ERRORS_RECORD_ENV = "WORKBAY_RUN_GUARD_ERRORS_RECORD"


def _workspace_root() -> str:
    # Fail open (mirrors _deps_python): if the sibling helper is unimportable
    # (e.g. a partial overlay materialized _run_guard without it) fall back to
    # os.getcwd() rather than raising out of main() and inverting the guard's
    # fail-mode. Per-harness anchors (CLAUDE_PROJECT_DIR/GROK_WORKSPACE_ROOT)
    # come from the shared helper; VS Code / Codex use cwd = workspace root.
    try:
        from resolve_handoff_src import workspace_env_anchor

        return workspace_env_anchor() or os.getcwd()
    except Exception:  # noqa: BLE001 -- fail open if the helper is absent
        return os.getcwd()


def _resolve_handler(root: str, handler: str) -> str | None:
    candidate = handler if os.path.isabs(handler) else os.path.join(root, handler)
    if os.path.exists(candidate):
        return candidate
    # Dual-surface fallback: the same basename on the sibling hook surface.
    basename = os.path.basename(handler)
    for surface in _HOOK_SURFACES:
        sibling = os.path.join(root, surface, basename)
        if os.path.exists(sibling):
            return sibling
    return None


def _deps_python() -> str:
    """Interpreter carrying the workbay stack deps, for Python subprocesses.

    Python hook handlers and the errors-record module fallback import the
    workbay stack; the launch ``python3`` may lack those deps after a system
    Python upgrade, silently turning guarded hooks / telemetry into no-ops
    (MAINT-postmerge-review REV-B-HOOK-INTERP-01). Route them through the shared
    interpreter resolver. Fail open to the launch interpreter if the helper
    cannot be imported (same hook surface -> sys.path[0] = hooks dir).
    """
    try:
        from _interp import resolve_deps_python

        return resolve_deps_python() or sys.executable
    except Exception:  # noqa: BLE001 -- fail open to the launch interpreter
        return sys.executable


def _errors_record_argv() -> list[str]:
    """Resolve the errors-record invocation (mirrors capture-agent-errors.py)."""
    override = os.environ.get(_ERRORS_RECORD_ENV)
    if override:
        return shlex.split(override)
    console_script = shutil.which("mcp-workbay-handoff")
    if console_script:
        return [console_script, "errors-record"]
    return [_deps_python(), "-m", "workbay_handoff_mcp", "errors-record"]


def _record_infra_failure(handler: str, detail: str) -> None:
    """Best-effort hook_infra_failure telemetry; never raises, never blocks.

    Fire-and-forget: the wrapper must return well inside the smallest hook
    timeout (5s) or the harness kills it and Copilot/Codex treat the timeout
    as a deny — re-creating the incident class fail-open exists to prevent.
    A synchronous errors-record write (cold-MCP/DB contention can exceed 10s)
    is therefore detached and never awaited (REV-A-001).
    """
    argv = _errors_record_argv() + [
        "--error-class",
        "hook_infra_failure",
        "--summary",
        f"hook handler missing/unspawnable: {handler}",
        "--detail",
        detail,
        "--tool-name",
        "hook",
    ]
    try:
        subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        pass


def main(argv: list[str]) -> int:
    args = list(argv)
    fail_mode = "open"
    while args and args[0].startswith("--"):
        flag = args.pop(0)
        if flag.startswith("--fail-mode="):
            fail_mode = flag.split("=", 1)[1]

    if not args:
        # Malformed rendered command: nothing to run. Workflow guards fail
        # open; there is no handler identity to fail closed on.
        _record_infra_failure("<missing>", "wrapper invoked without a handler argument")
        return 0

    handler, *handler_args = args
    root = _workspace_root()
    resolved = _resolve_handler(root, handler)

    if resolved is None:
        _record_infra_failure(
            handler,
            f"workspace_root={root}; tried direct path and surfaces {_HOOK_SURFACES}",
        )
        if fail_mode == "closed":
            sys.stderr.write(
                f"_run_guard: fail-closed handler missing: {handler} "
                f"(workspace root {root}); blocking.\n"
            )
            return 2
        return 0

    if resolved.endswith(".sh"):
        cmd = ["bash", resolved, *handler_args]
    else:
        # Python handlers may import the workbay stack; spawn them under a
        # deps-bearing interpreter, not the (possibly deps-less) launch python3
        # (MAINT-postmerge-review REV-B-HOOK-INTERP-01).
        cmd = [_deps_python(), resolved, *handler_args]

    try:
        # stdin/stdout/stderr inherited: byte-transparent passthrough.
        completed = subprocess.run(cmd)
    except OSError as exc:
        _record_infra_failure(handler, f"spawn failed: {exc}")
        if fail_mode == "closed":
            sys.stderr.write(
                f"_run_guard: fail-closed handler unspawnable: {handler}: {exc}\n"
            )
            return 2
        return 0
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
