"""Bundled runtime assets shipped with the orchestrator package.

These resources (lane management scripts, review prompt guides, the
contract-change checklist) used to be looked up at consumer-repo paths
such as ``<orchestrator_root>/scripts/worktree-lane`` or
``<orchestrator_root>/docs/workbay/rules/branch-review-guide.md``.
That coupling forced every consumer repo to vendor the package's own
files, which drifted across consumers and broke installs that did not
ship the orchestrator from a sibling source checkout.

The fix: the orchestrator now owns these files inside its own package
tree and resolves them via ``importlib.resources``. Consumers may still
override the rules directory or the script path explicitly, but the
default lookup never leaves the package.
"""

from __future__ import annotations

from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path

_PACKAGE = "workbay_orchestrator_mcp._assets"


def assets_root() -> Traversable:
    """Return the package-local ``_assets`` Traversable."""
    return resources.files(_PACKAGE)


def bundled_script_path(name: str) -> Path:
    """Resolve a bundled executable script to a real filesystem path.

    The orchestrator shells out to these scripts (``worktree-lane``), so a
    Traversable is not enough — subprocess needs an on-disk path. Package
    data files always live on disk for ``setuptools`` installs, but we go
    through ``as_file`` so it works under future zip-imports too.
    """
    traversable = assets_root() / "scripts" / name
    with resources.as_file(traversable) as fs_path:
        return Path(fs_path)


def bundled_rules_dir() -> Path:
    """Resolve the bundled review-rules directory to a filesystem path."""
    traversable = assets_root() / "rules"
    with resources.as_file(traversable) as fs_path:
        return Path(fs_path)
