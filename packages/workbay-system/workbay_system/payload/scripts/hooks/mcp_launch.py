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
3. if a ``.venv`` console was found, ``os.execvp``\\ s it directly — the fast
   path that skips ``uv run``'s per-invocation project resolution (the boot-miss
   cause this plan removes);
4. else probes the ``uv tool`` console installed once by ``workbay-bootstrap
   install`` — uv's default bin-dir order (``UV_TOOL_BIN_DIR``,
   ``$XDG_BIN_HOME``, ``$XDG_DATA_HOME/../bin``, ``~/.local/bin``) — and execs
   it when present;
5. else, for a **session serve** (the default ``serve-stdio`` args) with nothing
   installed, raises ``ValueError`` carrying the ``workbay-bootstrap install``
   setup one-liner — there is **no** per-session PyPI/``uvx``/``uv run`` resolve;
6. else, **only** when non-default args are forwarded (e.g. ``init-state``
   provisioning), conditionally falls back to ``uv run --no-sync --project
   <clone>/<pkg> …`` — and only when an overlay clone (``.workbay/remote`` |
   ``.workbay/clone`` | the primary checkout) actually carries the package.

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


def _uv_tool_bin_dirs() -> list[str]:
    """uv's default ``uv tool install`` bin-dir resolution order.

    ``uv tool install`` places console scripts in the first available of:
    ``UV_TOOL_BIN_DIR``, then ``$XDG_BIN_HOME``, then ``$XDG_DATA_HOME/../bin``,
    then ``~/.local/bin``. The shim must probe the same candidates in the same
    order: the harness MCP-launch env frequently has ``UV_TOOL_BIN_DIR`` unset
    while ``XDG_BIN_HOME`` / ``XDG_DATA_HOME`` are set, so a correctly-installed
    console lives under an XDG path the single-probe form missed (→ a false
    "MCP server binaries not installed"). Returns absolute candidate dirs,
    order-preserving and de-duplicated, with ``~/.local/bin`` as the tail.
    """
    candidates: list[str] = []
    explicit = os.environ.get("UV_TOOL_BIN_DIR")
    if explicit:
        candidates.append(explicit)
    xdg_bin_home = os.environ.get("XDG_BIN_HOME")
    if xdg_bin_home:
        candidates.append(xdg_bin_home)
    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        candidates.append(os.path.join(os.path.dirname(xdg_data_home), "bin"))
    candidates.append(os.path.join(os.path.expanduser("~"), ".local", "bin"))
    dirs: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in dirs:
            dirs.append(candidate)
    return dirs


def _tool_console_path(console: str) -> str:
    """Console script installed by ``uv tool install`` (Q5 one-time setup).

    Probes uv's default tool bin-dir order (see ``_uv_tool_bin_dirs``) for the
    console under POSIX (``<dir>/<console>`` or ``<dir>/bin/<console>``) or
    Windows (``<dir>/Scripts/<console>.exe``) layouts. A candidate must exist
    *and* be executable (POSIX).
    """
    candidates: list[str] = []
    for bin_dir in _uv_tool_bin_dirs():
        candidates.extend(
            (
                os.path.join(bin_dir, console),
                os.path.join(bin_dir, "bin", console),
                os.path.join(bin_dir, "Scripts", f"{console}.exe"),
            )
        )
    for candidate in candidates:
        if os.path.exists(candidate) and (
            os.name == "nt" or os.access(candidate, os.X_OK)
        ):
            return candidate
    return ""


def resolve_launch(
    server_id: str, repo_root: str, extra_args: list[str] | None = None
) -> list[str]:
    """Return the argv to exec for ``server_id``.

    ``extra_args`` (the launcher's own ``argv[2:]``) override the default
    ``spec["args"]`` when present, so non-serve invocations forward through.
    The ``.mcp.json`` server-launch form passes only the id, so it keeps the
    default ``--workspace-root . serve-stdio`` args; the bootstrap provisioning
    call ``mcp_launch.py <id> --workspace-root <p> --state-dir <p>/.task-state
    init-state`` forwards its args so the console runs ``init-state`` instead of
    silently booting the stdio server (which has no stdin here and hangs the
    caller until its timeout).

    Q5 (Dist-1): no per-session ``uv run`` fallback — workspace ``.venv`` or
  ``uv tool`` install only; otherwise fail loud with the setup one-liner.
    """
    spec = SERVERS.get(server_id)
    if spec is None:
        raise ValueError(f"unknown MCP server id: {server_id!r}")
    forwarded = list(extra_args) if extra_args else list(spec["args"])
    script = _console_path(repo_root, spec["project"], spec["console"])
    if script:
        return [script, *forwarded]
    tool_script = _tool_console_path(spec["console"])
    if tool_script:
        return [tool_script, *forwarded]
    # Q5: session serve must not per-session resolve from PyPI/index.
    default_forwarded = list(spec["args"])
    if forwarded != default_forwarded:
        for root in _project_roots(repo_root):
            project = os.path.join(root, spec["project"])
            if os.path.isfile(os.path.join(project, "pyproject.toml")):
                return [
                    "uv",
                    "run",
                    "--no-sync",
                    "--project",
                    project,
                    # The git_overlay clone retains the workspace's
                    # `[tool.uv.sources] { workspace = true }` pins, which `uv
                    # run` rejects outside the workspace ("references a
                    # workspace ... but is not a workspace member"). Ignore them
                    # so the cloned package resolves from its plain
                    # [project.dependencies] (internal missed this
                    # clone-launch site).
                    "--no-sources",
                    spec["console"],
                    *forwarded,
                ]
    raise ValueError(
        "MCP server binaries not installed — run: workbay-bootstrap install "
        "(or `workbay install`) to provision the git-sourced tool closure once."
    )


def _project_roots(repo_root: str) -> list[str]:
    """Repo root plus overlay clone roots that may carry in-tree packages."""
    roots = [repo_root]
    for sub in ((".workbay", "remote"), (".workbay", "clone")):
        candidate = os.path.join(repo_root, *sub)
        if os.path.isdir(candidate):
            roots.append(candidate)
    primary = _primary_checkout_root(repo_root)
    if primary and primary not in roots:
        roots.append(primary)
    return roots



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
    from _envfile import load_embedding_env

    load_embedding_env(repo_root)
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
