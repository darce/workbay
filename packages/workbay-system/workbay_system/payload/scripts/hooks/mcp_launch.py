#!/usr/bin/env python3
"""Stdlib-only MCP launcher shim (implementation note).

Harness-agnostic. Every harness launches a workbay MCP server as a stdio
subprocess; a statically generated ``.mcp.json`` command string cannot branch at
launch time on which environment actually exists, so this shim does the branch.
Invoked as ``python3 scripts/hooks/mcp_launch.py <server-id>`` it:

1. resolves the repo root via ``git`` (no vendor env var), reusing the
   ``_interp.py`` idiom;
2. probes the deps-bearing console script in the canonical workspace
   ``.venv/{bin/<console>, Scripts/<console>.exe}`` (POSIX / Windows) --
   checked in the worktree first and then the *primary* checkout, so a linked
   worktree without its own venv still heals (the ``_interp.py`` idiom);
3. if found, ``os.execvp``\\ s it directly — the fast path that skips ``uv run``'s
   per-invocation project resolution (the boot-miss cause this plan removes);
4. else ``os.execvp``\\ s ``uv run --no-sync --project <pkg> <console> …`` — the
   provisioning fallback, so no environment regresses to "won't start".

Stdlib only on purpose: a bare ``python3`` must run it even when the workbay
stack deps are absent; it merely *selects* the deps-bearing interpreter for the
server — the fail-open pattern of ``_run_guard.py`` / ``_interp.py``.

The ``SERVERS`` registry mirrors ``config/agent-workflows/mcp_servers.yaml`` and
``_build_local_default_mcp_servers``; keep them in sync (implementation note wiring).
"""

from __future__ import annotations

import os
import subprocess
import sys

# server-id -> (project dir, console script, forwarded args). Mirrors
# mcp_servers.yaml / _build_local_default_mcp_servers.
SERVERS: dict[str, dict] = {
    "workbay-handoff-mcp": {
        "project": "packages/mcp-workbay-handoff",
        "console": "mcp-workbay-handoff",
        "args": ["--workspace-root", ".", "serve-stdio"],
    },
    "workbay-orchestrator-mcp": {
        "project": "packages/mcp-workbay-orchestrator",
        "console": "mcp-workbay-orchestrator",
        "args": ["--workspace-root", ".", "serve-stdio"],
    },
}


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


def _primary_checkout_root(repo_root: str) -> str:
    """Root of the *primary* checkout (parent of the shared git dir).

    A linked worktree (feature / review / harness-session) often has no
    ``.venv`` of its own, but the primary checkout does. ``git rev-parse
    --git-common-dir`` resolves to the shared ``.git`` regardless of which
    worktree we are in; its parent is the primary checkout root. Mirrors the
    ``_interp.py`` idiom so the shim heals the same multi-worktree case. Empty
    on failure (graceful no-op).
    """
    try:
        proc = subprocess.run(
            ["git", "-C", repo_root, "rev-parse",
             "--path-format=absolute", "--git-common-dir"],
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


def _console_path(repo_root: str, project: str, console: str) -> str:
    """First existing deps-bearing console script in the workspace ``.venv``, or "".

    Probes the canonical root ``.venv`` only (implementation note D3): the git toplevel
    (the worktree) before the *primary* checkout, so a linked worktree lacking
    its own venv still heals to the primary's console script instead of silently
    dropping to the slow ``uv run`` fallback (mirrors ``_interp.py``). Per-package
    ``packages/<pkg>/.venv`` paths are not consulted. Within a venv, both POSIX
    (``bin/<console>``) and Windows (``Scripts/<console>.exe``) layouts. A
    candidate must exist *and* be executable (POSIX).
    """
    roots = [repo_root]
    primary = _primary_checkout_root(repo_root)
    if primary and primary not in roots:
        roots.append(primary)
    venvs = [os.path.join(r, ".venv") for r in roots]
    for venv in venvs:
        for rel in (("bin", console), ("Scripts", f"{console}.exe")):
            candidate = os.path.join(venv, *rel)
            if os.path.exists(candidate) and (
                os.name == "nt" or os.access(candidate, os.X_OK)
            ):
                return candidate
    return ""


def resolve_launch(
    server_id: str, repo_root: str, extra_args: list[str] | None = None
) -> list[str]:
    """Return the argv to exec for ``server_id`` (fast path or uv-run fallback).

    ``extra_args`` (the launcher's own ``argv[2:]``) override the default
    ``spec["args"]`` when present, so non-serve invocations forward through.
    The ``.mcp.json`` server-launch form passes only the id, so it keeps the
    default ``--workspace-root . serve-stdio`` args; the bootstrap provisioning
    call ``mcp_launch.py <id> --workspace-root <p> --state-dir <p>/.task-state
    init-state`` forwards its args so the console runs ``init-state`` instead of
    silently booting the stdio server (which has no stdin here and hangs the
    caller until its timeout).
    """
    spec = SERVERS.get(server_id)
    if spec is None:
        raise ValueError(f"unknown MCP server id: {server_id!r}")
    forwarded = list(extra_args) if extra_args else list(spec["args"])
    script = _console_path(repo_root, spec["project"], spec["console"])
    if script:
        return [script, *forwarded]
    # Provisioning fallback: uv builds/uses the env, --no-sync skips re-resolution.
    return [
        "uv",
        "run",
        "--no-sync",
        "--project",
        spec["project"],
        spec["console"],
        *forwarded,
    ]


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        sys.stderr.write("mcp_launch: missing <server-id> argument\n")
        return 2
    server_id = argv[1]
    extra_args = argv[2:]
    repo_root = _git_repo_root() or os.getcwd()
    try:
        cmd = resolve_launch(server_id, repo_root, extra_args)
    except ValueError as exc:
        sys.stderr.write(f"mcp_launch: {exc}\n")
        return 2
    # chdir so the forwarded ``--workspace-root .`` and a relative ``--project``
    # resolve against the repo root regardless of the harness's launch cwd.
    try:
        os.chdir(repo_root)
    except OSError:
        pass
    try:
        os.execvp(cmd[0], cmd)
    except OSError as exc:  # exec failed: nothing left to launch
        sys.stderr.write(f"mcp_launch: failed to exec {cmd[0]!r}: {exc}\n")
        return 127
    return 127  # unreachable on a successful exec


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
