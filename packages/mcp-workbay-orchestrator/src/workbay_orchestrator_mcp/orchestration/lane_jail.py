#!/usr/bin/env python3
"""macOS Seatbelt write-jail for lane agent processes (adoption C, implementation note).

The lane agent CLI (codex/grok/claude) is wrapped with ``sandbox-exec`` so it may
only *write* to a small allow-list compiled from the Slice-1 manifest ``grants``
block: the lane worktree, the primary repo ``.git/`` plumbing (so ``git commit``
from the lane still works), ``TMPDIR``, the uv/npm tool caches, and any
``extra_write_paths`` the lane declared. Everything else is EPERM. Reads, exec,
and network are left unrestricted (non-goal: no network/exec jail).

Fail-open by contract: the jail must NEVER fail dispatch. When it is unsupported
(non-darwin, no ``sandbox-exec`` on PATH), not opted in (the jail is opt-in:
``WORKBAY_LANE_JAIL`` unset or != ``1``), grant-less, or if
compiling/materializing the profile raises for any reason, the lane runs
UNJAILED after a single warn line.

``wrap_argv`` is pure (no I/O). ``compile_profile`` is deterministic string
assembly with one exception: absolute ``extra_write_paths`` entries are
realpath-resolved (filesystem I/O) and re-checked against the lane_manifest
breadth rule so a symlink cannot widen the allow-list — too-broad resolutions
are dropped. Golden tests therefore use relative extras or absolute extras that
do not exist on the host (realpath passes those through unchanged); the
gate/materialize helpers do the environment probing and the temp-file write.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

try:  # package import (installed / src layout)
    from workbay_orchestrator_mcp.orchestration.lane_manifest import extra_write_path_too_broad
except ImportError:  # script mode: the orchestration dir itself is on sys.path
    from lane_manifest import extra_write_path_too_broad  # type: ignore[no-redef]

_logger = logging.getLogger("workbay.lane_jail")

#: Env rollout switch: set to ``1`` to opt a lane into the write jail.
JAIL_ENV_VAR = "WORKBAY_LANE_JAIL"


@dataclass(frozen=True)
class JailPaths:
    """Absolute, canonical write-allow anchors for a lane jail profile.

    All fields are expected to be absolute paths (``compile_profile`` does not
    touch the filesystem; the caller resolves symlinks via the gate helper).
    Empty strings / ``None`` are skipped.
    """

    worktree_root: str
    primary_git_dir: str = ""
    tmpdir: str = ""
    uv_cache: str = ""
    npm_cache: str = ""
    codex_home: str = ""
    claude_home: str = ""
    claude_config: str = ""
    grok_home: str = ""
    primary_repo_root: str | None = None
    #: Extra writable git plumbing dirs for a linked worktree (the resolved
    #: per-worktree gitdir + shared ``commondir`` objects) when ``.git`` is a file.
    extra_git_dirs: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Pure profile compilation
# ---------------------------------------------------------------------------


def _norm(path: str) -> str:
    if not path:
        return ""
    return os.path.normpath(str(path))


def _escape(path: str) -> str:
    """Escape a path for an SBPL double-quoted string literal."""
    return path.replace("\\", "\\\\").replace('"', '\\"')


def _resolve_extra(entry: str, worktree_root: str) -> str:
    """Resolve one ``extra_write_paths`` entry.

    Repo-relative entries are joined onto the lane worktree root (pure string
    work, no I/O). Absolute entries are realpath-resolved (filesystem I/O —
    mirror the sibling anchors in ``_resolve_jail_paths``) so symlinked prefixes
    such as ``/var/tmp -> /private/var/tmp`` match the sandbox allow-list, then
    RE-CHECKED against the manifest breadth rule: manifest validation is
    lexical-only, so a validated deep path that is a symlink INTO a forbidden
    root (e.g. ``-> /etc``) would otherwise realpath-expand into the allow-list
    unchecked. Too-broad resolutions are dropped (returns ``""``) with a warning
    rather than silently widening the jail. Other validation (``..`` rejection,
    non-empty) is the manifest validator's job (implementation note); this is defensive only.
    """
    text = str(entry).strip()
    if not text:
        return ""
    candidate = Path(text)
    if candidate.is_absolute():
        resolved = os.path.realpath(text)
        if extra_write_path_too_broad(resolved):
            _logger.warning(
                "lane jail: extra_write_paths entry %r resolves to %r, which is too broad; "
                "dropping it from the write allow-list.",
                text,
                resolved,
            )
            return ""
        return resolved
    return os.path.normpath(os.path.join(str(worktree_root), text))


def compile_profile(grants: dict | None, paths: JailPaths) -> str:
    """Compile a Seatbelt SBPL profile from a lane ``grants`` block.

    Deterministic / byte-stable: the write-allow subpaths are de-duplicated and
    sorted, and the header/baseline lines are fixed, so the output is
    golden-testable. The only filesystem I/O is the realpath + breadth re-check
    applied to absolute ``extra_write_paths`` entries (see ``_resolve_extra``);
    everything else is pure string assembly.
    """
    grants = grants or {}
    allow: set[str] = set()

    # Baseline: character/pseudo devices toolchains routinely write (/dev/null,
    # /dev/tty, ...). Without this, deny-file-write* EPERMs benign tool output.
    allow.add("/dev")

    # TMPDIR + tool caches + agent CLI state dirs.
    for candidate in (
        paths.tmpdir,
        paths.uv_cache,
        paths.npm_cache,
        paths.codex_home,
        paths.claude_home,
        paths.claude_config,
        paths.grok_home,
    ):
        normalized = _norm(candidate)
        if normalized:
            allow.add(normalized)

    # Lane worktree (default read_write; read_only/none withhold write access).
    if grants.get("worktree", "read_write") == "read_write":
        worktree = _norm(paths.worktree_root)
        if worktree:
            allow.add(worktree)

    # Primary repo .git/ plumbing is ALWAYS allowed so `git commit` from a lane
    # (linked) worktree — which writes objects/refs/worktrees under the primary
    # .git — still succeeds. This is independent of the primary_repo grant level,
    # which governs the primary WORKING tree (denied by default).
    primary_git = _norm(paths.primary_git_dir)
    if primary_git:
        allow.add(primary_git)
    # Linked-worktree ``.git`` file: the real per-worktree gitdir + shared
    # commondir object store must also be writable or ``git commit`` EPERMs.
    for git_dir in paths.extra_git_dirs:
        normalized = _norm(git_dir)
        if normalized:
            allow.add(normalized)

    # Only an explicit read_write grant opens the primary working tree.
    if grants.get("primary_repo") == "read_write" and paths.primary_repo_root:
        primary_root = _norm(paths.primary_repo_root)
        if primary_root:
            allow.add(primary_root)

    # Declared extra write paths.
    extras = grants.get("extra_write_paths") or []
    if isinstance(extras, (list, tuple)):
        for entry in extras:
            resolved = _resolve_extra(entry, paths.worktree_root)
            if resolved:
                allow.add(resolved)

    lines = [
        "(version 1)",
        ";; WorkBay lane write-jail (adoption C, implementation note). Deny all writes by",
        ";; default, then re-allow the lane worktree, primary .git plumbing,",
        ";; TMPDIR, tool caches, and granted extra paths. Reads/exec/network are",
        ";; deliberately unrestricted (no network/exec jail).",
        "(allow default)",
        "(deny file-write*)",
        "(allow file-read*)",
        "(allow file-write*",
    ]
    for path in sorted(allow):
        lines.append(f'    (subpath "{_escape(path)}")')
    lines.append(")")
    return "\n".join(lines) + "\n"


def wrap_argv(argv: Sequence[str], profile_path: str | os.PathLike[str]) -> list[str]:
    """Wrap ``argv`` under ``sandbox-exec -f <profile>`` (PURE)."""
    return ["sandbox-exec", "-f", str(profile_path), *argv]


# ---------------------------------------------------------------------------
# Environment-probing helpers (I/O)
# ---------------------------------------------------------------------------


def jail_supported(env: "os._Environ[str] | dict[str, str] | None" = None) -> bool:
    """True when this host can run the Seatbelt jail (darwin + sandbox-exec)."""
    del env  # platform + PATH only; kept for symmetry with the gate signature
    return sys.platform == "darwin" and shutil.which("sandbox-exec") is not None


def _resolve_primary_git_dirs(primary_repo_root: str | None) -> list[str]:
    """Resolve the primary repo's writable git plumbing dirs (best-effort).

    In a normal repo ``.git`` is a directory and this returns ``[realpath(.git)]``.
    In a *linked worktree* ``.git`` is a FILE containing ``gitdir: <path>`` that
    points at the per-worktree git dir under the common ``.git/worktrees/<name>``;
    the shared object store lives at that dir's ``commondir``. All of these must
    be write-allowed or a jailed ``git commit`` EPERMs. Tolerates a
    malformed/absent ``.git`` by returning what it could resolve.
    """
    if not primary_repo_root:
        return []
    real = os.path.realpath
    dot_git = os.path.join(str(primary_repo_root), ".git")
    if not os.path.exists(dot_git):
        return []
    if os.path.isdir(dot_git):
        return [real(dot_git)]
    try:
        with open(dot_git, "r", encoding="utf-8") as fh:
            content = fh.read()
    except OSError:
        return []
    gitdir = ""
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("gitdir:"):
            gitdir = stripped[len("gitdir:") :].strip()
            break
    if not gitdir:
        return []
    if not os.path.isabs(gitdir):
        gitdir = os.path.join(os.path.dirname(dot_git), gitdir)
    gitdir = real(gitdir)
    dirs = [gitdir]
    try:
        with open(os.path.join(gitdir, "commondir"), "r", encoding="utf-8") as fh:
            commondir = fh.read().strip()
    except OSError:
        commondir = ""
    if commondir:
        if not os.path.isabs(commondir):
            commondir = os.path.join(gitdir, commondir)
        dirs.append(real(commondir))
    return dirs


def _resolve_jail_paths(
    worktree_root: str | os.PathLike[str],
    primary_repo_root: str | os.PathLike[str] | None,
    env: "os._Environ[str] | dict[str, str]",
) -> JailPaths:
    """Build a :class:`JailPaths` with symlinks resolved (macOS /var -> /private/var).

    ``os.path.realpath`` is applied so the profile's subpaths match the *real*
    paths the sandboxed process sees; otherwise TMPDIR under ``/var/folders`` would
    never match a write to ``/private/var/folders`` and every temp write EPERMs.
    """
    real = os.path.realpath
    home = env.get("HOME") or str(Path.home())
    tmpdir = env.get("TMPDIR") or tempfile.gettempdir()
    uv_cache = env.get("UV_CACHE_DIR") or os.path.join(home, ".cache", "uv")
    npm_cache = env.get("npm_config_cache") or os.path.join(home, ".npm")
    codex_home = env.get("CODEX_HOME") or os.path.join(home, ".codex")
    claude_home = env.get("CLAUDE_HOME") or os.path.join(home, ".claude")
    # KNOWN LIMITATION: claude_config is a FILE anchor inside the deny'd $HOME.
    # Direct in-place writes to ~/.claude.json succeed under the jail, but
    # atomic sibling-temp+rename writers (~/.claude.json.tmp -> rename) still
    # EPERM because the temp file lands in $HOME itself. If that breaks the
    # claude CLI in practice, the follow-up is a narrow ~/.claude.json.*
    # allowance — not a $HOME-wide one.
    claude_config = env.get("CLAUDE_CONFIG_PATH") or os.path.join(home, ".claude.json")
    grok_home = env.get("GROK_CONFIG_DIR") or os.path.join(home, ".grok")
    git_dirs = _resolve_primary_git_dirs(str(primary_repo_root) if primary_repo_root else None)
    primary_git = git_dirs[0] if git_dirs else ""
    return JailPaths(
        worktree_root=real(str(worktree_root)),
        primary_git_dir=primary_git,
        extra_git_dirs=tuple(git_dirs[1:]),
        tmpdir=real(tmpdir),
        uv_cache=real(uv_cache),
        npm_cache=real(npm_cache),
        codex_home=real(codex_home),
        claude_home=real(claude_home),
        claude_config=real(claude_config),
        grok_home=real(grok_home),
        primary_repo_root=real(str(primary_repo_root)) if primary_repo_root else None,
    )


def materialize_profile(profile_text: str) -> str:
    """Write ``profile_text`` to a temp ``.sb`` file and return its path.

    The file is left in TMPDIR (the OS reaps it): it must outlive the spawned
    subprocess, and threading a cleanup handle through the adapter call chain is
    not worth the coupling for a tiny profile.
    """
    fd, name = tempfile.mkstemp(prefix="workbay-lane-jail-", suffix=".sb")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(profile_text)
    return name


def _default_warn(message: str) -> None:
    _logger.warning(message)


def maybe_jail_argv(
    argv: Sequence[str],
    *,
    grants: dict | None,
    worktree_root: str | os.PathLike[str],
    primary_repo_root: str | os.PathLike[str] | None = None,
    env: "os._Environ[str] | dict[str, str] | None" = None,
    warn: Callable[[str], None] | None = None,
) -> list[str]:
    """Gate + wrap ``argv`` under the lane jail, or return it unchanged.

    Wraps ONLY when ALL hold: grants present, ``sys.platform == 'darwin'``,
    ``WORKBAY_LANE_JAIL == '1'``, and ``sandbox-exec`` on PATH. Otherwise emits a
    single warn line and returns the argv unjailed. NEVER raises — any setup
    failure degrades to unjailed.
    """
    resolved_env = os.environ if env is None else env
    emit = _default_warn if warn is None else warn
    argv_list = list(argv)
    try:
        if not grants:
            emit("lane jail: no grants declared on lane manifest; running unjailed.")
            return argv_list
        if sys.platform != "darwin":
            emit(f"lane jail: unsupported platform {sys.platform!r}; running unjailed.")
            return argv_list
        if resolved_env.get(JAIL_ENV_VAR) != "1":
            emit(f"lane jail: opt-in env {JAIL_ENV_VAR}=1 not set; running unjailed.")
            return argv_list
        if shutil.which("sandbox-exec") is None:
            emit("lane jail: sandbox-exec not found on PATH; running unjailed.")
            return argv_list
        paths = _resolve_jail_paths(worktree_root, primary_repo_root, resolved_env)
        profile_path = materialize_profile(compile_profile(grants, paths))
        return wrap_argv(argv_list, profile_path)
    except Exception as exc:  # noqa: BLE001 - jail must never fail dispatch
        emit(f"lane jail: setup failed ({type(exc).__name__}: {exc}); running unjailed.")
        return argv_list


def build_jail_prefix(
    *,
    grants: dict | None,
    worktree_root: str | os.PathLike[str],
    primary_repo_root: str | os.PathLike[str] | None = None,
    env: "os._Environ[str] | dict[str, str] | None" = None,
    warn: Callable[[str], None] | None = None,
) -> list[str]:
    """Return a ``sandbox-exec`` argv prefix to prepend to the agent command.

    ``["sandbox-exec", "-f", <profile>]`` when jailed, or ``[]`` when the gate
    degrades to unjailed. Adapters splat this ahead of their CLI binary.
    """
    return maybe_jail_argv(
        [],
        grants=grants,
        worktree_root=worktree_root,
        primary_repo_root=primary_repo_root,
        env=env,
        warn=warn,
    )


# ---------------------------------------------------------------------------
# Denial telemetry signature (pure)
# ---------------------------------------------------------------------------


def is_sandbox_denial(text: str | None) -> bool:
    """Heuristic: does agent stderr look like a Seatbelt write denial?

    Matches the strings a ``sandbox-exec`` file-write* denial surfaces so a jailed
    nonzero exit can be classified as ``lane_jail_denial`` telemetry.
    """
    if not text:
        return False
    lowered = text.lower()
    if "sandbox-exec" in lowered or "operation not permitted" in lowered:
        return True
    if "deny" in lowered and "sandbox" in lowered:
        return True
    if "sandbox policy" in lowered:
        return True
    return False
