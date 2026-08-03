"""Per-package pytest path-guard helper (internal).

Hard-fails the pytest session at ``pytest_sessionstart`` if any in-repo
agentic package was imported from outside the active worktree root.
Per-package ``conftest.py`` shims call ``check_path_guard`` so
``cd packages/<pkg> && uv run pytest`` catches the case where an
environment-wide editable install points at a different worktree.

Opt-out via ``WORKBAY_DISABLE_PYTEST_PATH_GUARD=1`` for cross-worktree
fixture work where loading from outside is intentional.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable, Sequence

GUARDED_TOP_LEVEL_NAMES = (
    "workbay_handoff_mcp",
    "workbay_orchestrator_mcp",
)
GUARDED_TOP_LEVEL_PREFIXES = ("workbay_",)
OPT_OUT_ENV = "WORKBAY_DISABLE_PYTEST_PATH_GUARD"


def _is_guarded_top_level(name: str) -> bool:
    if "." in name:
        return False
    if name in GUARDED_TOP_LEVEL_NAMES:
        return True
    return any(name.startswith(prefix) for prefix in GUARDED_TOP_LEVEL_PREFIXES)


def collect_violations(
    worktree_root: Path,
    modules: Iterable[tuple[str, object]] | None = None,
) -> list[tuple[str, Path, Path]]:
    """Return ``(name, actual_path, worktree_root)`` for guarded modules outside ``worktree_root``."""
    root = worktree_root.resolve()
    iterable = list(modules) if modules is not None else list(sys.modules.items())
    violations: list[tuple[str, Path, Path]] = []
    for name, module in iterable:
        if module is None or not _is_guarded_top_level(name):
            continue
        file_attr = getattr(module, "__file__", None)
        if not isinstance(file_attr, str) or not file_attr:
            continue
        try:
            actual = Path(file_attr).resolve()
        except OSError:
            continue
        try:
            actual.relative_to(root)
        except ValueError:
            violations.append((name, actual, root))
    return violations


def remediation_message(
    violations: Sequence[tuple[str, Path, Path]],
    cwd: Path | None = None,
) -> str:
    here = (cwd or Path.cwd()).resolve()
    lines: list[str] = []
    for name, actual, _root in violations:
        lines.append(
            f"{name} loaded from {actual}, but cwd is {here}. "
            "Run uv sync --extra dev in this worktree's package directory and retry."
        )
    return "\n".join(lines)


def check_path_guard(worktree_root: Path) -> None:
    """Raise ``pytest.UsageError`` if any guarded module loaded from outside ``worktree_root``.

    Honors the ``WORKBAY_DISABLE_PYTEST_PATH_GUARD=1`` opt-out.
    """
    if os.environ.get(OPT_OUT_ENV) == "1":
        return
    violations = collect_violations(worktree_root)
    if not violations:
        return
    import pytest  # local import — pytest is only present in test sessions.

    raise pytest.UsageError(remediation_message(violations))
