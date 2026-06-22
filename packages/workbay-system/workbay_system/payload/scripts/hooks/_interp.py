"""Shared hook helper: resolve / re-exec under a deps-bearing interpreter.

Harness-agnostic. Every harness (Claude Code, VS Code, Codex, Cursor, Grok)
launches these hooks via a bare ``python3`` command -- see the per-harness
``*_command`` fields rendered from ``harness-protocol.yaml``. The hook
*scripts* are the single shared artifact all of those commands invoke, so the
interpreter hardening lives here (one place -> every harness) rather than in
the ~60 per-harness command strings.

A system Python upgrade can leave the launch ``python3`` without the workbay
runtime deps (``pydantic`` and friends), which silently turns stack-importing
hooks into no-ops. These helpers route hook work to the project ``.venv``
python, which the bootstrap install keeps synced. Repo root is resolved via
``git`` -- no vendor-specific env var.

Two entry points by hook shape:

- ``ensure_deps_interpreter()`` re-execs the *current* process under the venv.
  Use it for hooks that import the stack in-process (compaction, reinjection);
  they fire infrequently (Stop / SessionStart) so the re-exec cost is fine.
- ``resolve_deps_python()`` returns a deps-bearing interpreter path *without*
  re-execing. Use it for hooks that run the stack in a subprocess and fire on
  every event (e.g. record-file-touch on PostToolUse), where re-execing the
  whole hook per event would add latency.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys


def _git_repo_root() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:  # noqa: BLE001 -- best-effort discovery
        pass
    return ""


def _primary_checkout_root() -> str:
    """Root of the *primary* checkout (parent of the shared git dir).

    A linked worktree (feature / review / harness-session) often has no
    ``.venv`` of its own, but the primary checkout does. ``git rev-parse
    --git-common-dir`` resolves to the shared ``.git`` regardless of which
    worktree we are in; its parent is the primary checkout root. Empty on
    failure (graceful no-op).
    """
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            common = proc.stdout.strip()
            if common:
                return os.path.dirname(common)
    except Exception:  # noqa: BLE001 -- best-effort discovery
        pass
    return ""


def _venv_python(repo_root: str | None = None) -> str:
    """First existing project-venv interpreter, or "" if none found.

    Probes, in order: the git toplevel's ``.venv`` then the primary
    checkout's ``.venv`` (so a worktree lacking its own venv still heals),
    and within each both POSIX (``bin/python``) and Windows
    (``Scripts/python.exe``) layouts.
    """
    roots: list[str] = []
    root = repo_root if repo_root is not None else _git_repo_root()
    if root:
        roots.append(root)
    primary = _primary_checkout_root()
    if primary and primary not in roots:
        roots.append(primary)
    for base in roots:
        for rel in (("bin", "python"), ("Scripts", "python.exe")):
            candidate = os.path.join(base, ".venv", *rel)
            if os.path.exists(candidate):
                return candidate
    return ""


# Leaf deps the stack-importing hooks load (via ``workbay_handoff_mcp``) at
# module import time. Probing only ``pydantic`` under-detects: a system Python
# can carry pydantic yet lack ``fastmcp`` / ``workbay_protocol``, so the hook
# would wrongly stay on it and silently no-op instead of healing to the venv
# (MAINT-postmerge-review REV-A-HOOK-DEPS-01).
_REQUIRED_DEPS = ("pydantic", "fastmcp", "workbay_protocol")


def _deps_present() -> bool:
    """True only when every workbay-stack dep the hooks import is importable.

    Short-circuits on the first missing dep. ``find_spec`` can raise
    (``ModuleNotFoundError`` for a missing parent, ``ValueError`` for a
    half-initialised module) -- treat any failure as "not present" so the
    caller heals to the venv rather than crashing the hook.
    """
    for name in _REQUIRED_DEPS:
        try:
            if importlib.util.find_spec(name) is None:
                return False
        except (ImportError, ValueError):
            return False
    return True


def resolve_deps_python(repo_root: str | None = None) -> str:
    """Return an interpreter that carries the workbay deps.

    For hooks that run the stack in a *subprocess* (no re-exec -- safe for
    high-frequency / per-event hooks). Prefer the project venv; fall back to
    the current interpreter when the full stack is already importable or no
    venv exists.
    """
    if _deps_present():
        return sys.executable
    return _venv_python(repo_root) or sys.executable


def resolve_env_alias(canonical: str, default: str | None = None) -> str | None:
    """Resolve a canonical ``WORKBAY_*`` env var.

    Delegates to ``workbay_protocol.resolve_env_alias`` when the stack is
    importable (the re-exec hooks), and falls back to a stdlib-only equivalent so
    a bare ``python3`` hook (pre-re-exec, no stack) still resolves the variable.
    Blank / whitespace-only values are treated as unset, matching the canonical
    resolver.
    """
    try:
        from workbay_protocol import resolve_env_alias as _canonical
    except Exception:  # noqa: BLE001 -- bare python3 / partial stack: stdlib fallback
        value = os.environ.get(canonical)
        if value is not None and value.strip():
            return value.strip()
        return default
    return _canonical(canonical, default=default)


def ensure_deps_interpreter() -> None:
    """Re-exec the current hook under the venv when the stack can't import.

    No-op when the full stack (``pydantic`` + ``fastmcp`` +
    ``workbay_protocol``) already imports. The ``WORKBAY_HOOK_REEXEC``
    sentinel bars a second re-exec so a broken venv cannot loop. Compares
    abspath, not realpath: a venv ``python`` usually symlinks to the same base
    binary as the system interpreter yet activates a different site-packages
    (where the deps live), so realpath equality would wrongly skip the
    re-exec. Call as the first statement of ``main()`` -- before stdin is read
    -- so ``os.execv`` preserves the unread event payload.
    """
    if _deps_present():
        return
    if os.environ.get("WORKBAY_HOOK_REEXEC") == "1":
        return
    candidate = _venv_python()
    if candidate and os.path.abspath(candidate) != os.path.abspath(sys.executable):
        os.environ["WORKBAY_HOOK_REEXEC"] = "1"
        os.execv(candidate, [candidate, *sys.argv])
