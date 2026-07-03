"""Shared handoff source resolution for payload hook scripts."""

from __future__ import annotations

import os

# Per-harness workspace-root env anchors. Claude Code exports
# ``CLAUDE_PROJECT_DIR`` to hook processes; Grok exports ``GROK_WORKSPACE_ROOT``
# the same way. VS Code (Copilot) and Codex export no anchor (they spawn hooks
# with ``cwd = workspace root``): the one direct env->root caller
# (``capture-agent-errors``) composes ``or os.getcwd()`` for that case, while the
# ``_git_repo_root`` callers return the bare anchor — ``git rev-parse`` already
# consulted cwd, so an empty result preserves their ``''``=unresolved contract.
# This single-sources the anchor SET shared with ``_run_guard._workspace_root``
# and ``coherence-self-check._workspace_root`` (both delegate here); it does not
# own their ``os.getcwd()`` tail.
_WORKSPACE_ROOT_ENV_VARS: tuple[str, ...] = ("CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT")


def resolve_harness_workspace_root(default: str = "") -> str:
    """Resolve the harness workspace root: git toplevel, then env anchors.

    Used by hooks that need a workspace root for path normalization (not
    git-toplevel-for-venv/console discovery — those stay git-only).
    """
    import subprocess

    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if proc.returncode == 0:
            root = proc.stdout.strip()
            if root:
                return root
    except Exception:  # noqa: BLE001
        pass
    return workspace_env_anchor(default)


def workspace_env_anchor(default: str = "") -> str:
    """Resolve the workspace root from per-harness env anchors.

    Returns the first non-blank value among :data:`_WORKSPACE_ROOT_ENV_VARS`,
    else ``default``. A whitespace-only value is treated as unset (mirroring the
    ``.strip()`` guard on the sibling ``GROK_WORKSPACE_ROOT`` harness probes) so a
    blank anchor never becomes a garbage root. Hooks use this as the
    ``git rev-parse`` fallback so a non-Claude harness (Grok) still resolves a
    root instead of dropping to a Claude-only ``CLAUDE_PROJECT_DIR`` lookup.
    """
    for var in _WORKSPACE_ROOT_ENV_VARS:
        value = os.environ.get(var)
        if value and value.strip():
            return value
    return default


def is_package_source_repo(repo_root: str) -> bool:
    """Return True when ``repo_root`` ships the handoff package in-tree."""
    return os.path.isdir(
        os.path.join(repo_root, "packages", "mcp-workbay-handoff", "src")
    )


_HANDOFF_PACKAGE_MODULE = "workbay_handoff_mcp"


def _overlay_src_candidates(repo_root: str) -> list[str]:
    """Overlay-clone ``src`` paths under ``.workbay/remote``.

    :func:`resolve_agent_handoff_src` accepts a candidate only when it actually
    exposes ``workbay_handoff_mcp`` (see :func:`_overlay_src_exposes_module`).
    """
    return [
        os.path.join(repo_root, ".workbay", "remote", "packages", "mcp-workbay-handoff", "src"),
    ]


def _overlay_src_exposes_module(overlay_src: str) -> bool:
    """True when ``overlay_src`` actually contains the ``workbay_handoff_mcp``
    package — not merely an existing ``src`` directory. Mirrors the
    installed-distribution probe so a module-less or pre-rename clone is never
    returned as a PYTHONPATH entry that cannot satisfy ``import workbay_handoff_mcp``.
    """
    return os.path.isdir(os.path.join(overlay_src, _HANDOFF_PACKAGE_MODULE))


def resolve_agent_handoff_src(repo_root: str) -> str:
    """Resolve a PYTHONPATH entry exposing ``workbay_handoff_mcp``.

    Installed distributions win first. In the package-source repo, in-tree
    ``packages/mcp-workbay-handoff/src`` wins over the managed overlay
    clone; consumer repos keep overlay-preferred order. The overlay probe is
    module-aware: a candidate resolves only when its ``src`` actually exposes
    ``workbay_handoff_mcp``, so an empty/partial clone falls through rather than
    resolving a path the hook import cannot satisfy.
    """
    try:
        from importlib import metadata as importlib_metadata

        dist = importlib_metadata.distribution("mcp-workbay-handoff")
        located = dist.locate_file("workbay_handoff_mcp")
        if located is not None and os.path.isdir(str(located)):
            return os.path.dirname(str(located))
    except Exception:  # noqa: BLE001
        pass

    in_tree = os.path.join(repo_root, "packages", "mcp-workbay-handoff", "src")
    if is_package_source_repo(repo_root) and os.path.isdir(in_tree):
        return in_tree
    for overlay_src in _overlay_src_candidates(repo_root):
        if _overlay_src_exposes_module(overlay_src):
            return overlay_src
    return in_tree
