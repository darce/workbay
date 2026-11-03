#!/usr/bin/env python3
"""Stdlib MCP launcher: qualified native workspace, same-Git primary, then tools.

The WorkBay consoles must own and import the required public core contracts in
one interpreter environment. Executability, name and version are insufficient.
Missing capabilities fail with a typed setup-recovery message, including for
init-state; resolution never runs uv or accesses the network. Explicit argv and
the child launch cwd are preserved. The external-binary router is independent.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys

from _native_runtime import NativeRuntimeUnavailable, git_roots, select_console, uv_tool_bin_dirs

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
    "codebase-graph-mcp": {
        "kind": "external-binary",
        "binary": "codebase-memory-mcp",
        "args": [],
    },
}


def _git_repo_root() -> str:
    roots = git_roots(os.getcwd())
    return roots[0] if roots else ""


def _primary_checkout_root(repo_root: str) -> str:
    """Root of the *primary* checkout (parent of the shared git dir).

    A linked worktree (feature / review / harness-session) often has no
    ``.venv`` of its own, but the primary checkout does. ``git rev-parse
    --git-common-dir`` resolves to the shared ``.git`` regardless of which
    worktree we are in; its parent is the primary checkout root. Mirrors the
    ``_interp.py`` idiom so the shim heals the same multi-worktree case. Empty
    on failure (graceful no-op).
    """
    roots = git_roots(repo_root)
    return roots[-1] if roots else ""


def _console_path(repo_root: str, project: str, console: str) -> str:
    """Qualified native console; keep implementation note's Git-only venv anchoring."""
    roots = git_roots(repo_root)
    tools = [os.path.join(directory, *rel)
             for directory in _uv_tool_bin_dirs()
             for rel in ((console,), ("bin", console), ("Scripts", console + ".exe"))]
    return select_console(console, roots, tools=tools, workspace=repo_root)


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
    return uv_tool_bin_dirs()


def _external_binary_path(binary: str) -> str:
    """Resolve a prebuilt external binary from PATH and uv-tool bin dirs.

    Probes, in order: ``shutil.which`` on PATH, then the uv-tool default bin-dir
    order (``_tool_console_path``). Stdlib-only on purpose.
    """
    which = shutil.which(binary)
    if which and os.path.exists(which) and (
        os.name == "nt" or os.access(which, os.X_OK)
    ):
        return which
    return _tool_console_path(binary)


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


def _console_python(console_path: str) -> str:
    """Return the interpreter embedded in an installed console script.

    ``uv tool`` and virtualenv POSIX entrypoints carry an absolute Python
    shebang.  Reading that shebang lets diagnostics ask the selected
    environment for its own distribution metadata without running the console
    itself (which may start a stdio server).  Windows launchers use the sibling
    ``python.exe``.
    """
    if not console_path:
        return ""
    parent = os.path.dirname(console_path)
    if os.name == "nt" or console_path.lower().endswith(".exe"):
        candidate = os.path.join(parent, "python.exe")
        return candidate if os.path.exists(candidate) else ""
    try:
        with open(console_path, "rb") as stream:
            first_line = stream.readline(4096).decode("utf-8", errors="replace")
    except OSError:
        return ""
    if not first_line.startswith("#!"):
        return ""
    try:
        command = shlex.split(first_line[2:].strip())
    except ValueError:
        return ""
    if not command:
        return ""
    interpreter = command[0]
    if os.path.basename(interpreter) == "env":
        args = command[1:]
        if args[:1] == ["-S"]:
            args = args[1:]
        if not args:
            return ""
        interpreter = shutil.which(args[0]) or ""
    return interpreter if interpreter and os.path.exists(interpreter) else ""


def _console_distribution_version(console_path: str, distribution: str) -> str:
    """Return ``distribution``'s version from ``console_path``'s environment."""
    interpreter = _console_python(console_path)
    if not interpreter:
        return ""
    code = (
        "from importlib.metadata import version; import sys; "
        "print(version(sys.argv[1]))"
    )
    try:
        proc = subprocess.run(
            [interpreter, "-c", code, distribution],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip().splitlines()[0] if proc.stdout.strip() else ""


def resolved_builds(server_id: str, repo_root: str) -> dict[str, str]:
    """Describe the launcher-selected and uv-tool builds for ``server_id``.

    This is the shared authority used by update and doctor.  Crucially it
    resolves ``spec['console']`` -- the executable the launcher actually execs
    -- rather than probing the differently named server id.
    """
    spec = SERVERS.get(server_id)
    if spec is None:
        raise ValueError(f"unknown MCP server id: {server_id!r}")
    if spec.get("kind") == "external-binary":
        return {}
    console = str(spec["console"])
    distribution = os.path.basename(str(spec["project"]))
    try:
        repo_console = _console_path(repo_root, str(spec["project"]), console)
        error = ""
    except NativeRuntimeUnavailable as exc:
        repo_console, error = "", str(exc)
    tool_console = _tool_console_path(console)
    launcher_console = repo_console
    return {
        "server_id": server_id,
        "console": console,
        "launcher_path": launcher_console,
        "launcher_error": error,
        "launcher_version": _console_distribution_version(
            launcher_console, distribution
        ),
        "uv_tool_path": tool_console,
        "uv_tool_version": _console_distribution_version(
            tool_console, distribution
        ),
    }


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
    if spec.get("kind") == "external-binary":
        binary = spec.get("binary")
        if not binary:
            raise ValueError(
                f"external-binary server {server_id!r} missing 'binary' key"
            )
        script = _external_binary_path(binary)
        if script:
            return [script, *forwarded]
        raise ValueError(
            f"MCP server {server_id!r} not installed — install "
            f"{binary!r} to PATH (e.g. ~/.local/bin) and retry"
        )
    script = _console_path(repo_root, spec["project"], spec["console"])
    return [script, *forwarded]


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
    if server_id == "--check":
        # Resolve-only availability probe (no exec): the single authority for
        # "is this server launchable in this environment?", reused by the
        # install/mcp-sync opt-in advisory. Exit 0 = available, 2 = absent
        # (with the same actionable message the launch path emits on stderr).
        if len(argv) < 3:
            sys.stderr.write("mcp_launch: --check requires a <server-id>\n")
            return 2
        repo_root = _git_repo_root() or os.getcwd()
        try:
            resolve_launch(argv[2], repo_root)
        except ValueError as exc:
            sys.stderr.write(f"mcp_launch: {exc}\n")
            return 2
        return 0
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
        if SERVERS[server_id].get("kind") != "external-binary":
            select_console(SERVERS[server_id]["console"], git_roots(repo_root),
                           override=cmd[0], workspace=repo_root)
    except NativeRuntimeUnavailable as exc:
        sys.stderr.write(f"mcp_launch: {exc}\n")
        return 2
    try:
        os.execvp(cmd[0], cmd)
    except OSError as exc:  # exec failed: nothing left to launch
        sys.stderr.write(f"mcp_launch: failed to exec {cmd[0]!r}: {exc}\n")
        return 127
    return 127  # unreachable on a successful exec


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
