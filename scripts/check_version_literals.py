#!/usr/bin/env python3
"""version-literal-check (audit PPSSOT-VER-*): fail if any package ``__init__``
reintroduces a hand-copied ``__version__`` string literal.

``pyproject.toml [project].version`` is the single version authority; runtime
``__version__`` must derive from it via ``workbay_protocol.version.version_of``.
A literal ``__version__ = "x.y.z"`` is a second copy that silently drifts (the
workbay 0.2.1-vs-0.3.6 / bootstrap 0.2.1-vs-0.3.5 drift this gate exists to
prevent). Mirrors the fail-closed posture of ``scripts/check_brand.py``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# A literal assignment is ``__version__`` — with an optional type annotation
# (``__version__: str``) — then ``=`` then a quote (optionally wrapped in an
# opening paren, ``__version__ = ("x.y.z")``). ``__version__ = version_of(...)``
# has no quote after ``=`` and is allowed. Without the annotation / paren
# tolerance a literal could re-enter as ``__version__: str = "x.y.z"`` and slip
# the gate (PPSSOT-VER / S2 regex-evasion).
PATTERN = r"__version__[[:space:]]*(:[^=]*)?=[[:space:]]*[(]?[[:space:]]*[\"']"
# Scan every package ``src`` module, not just ``__init__.py``: a hand-copied
# literal in ``_version.py`` / ``version.py`` / any submodule drifts just the
# same. ``*.py`` covers top-level single-file modules directly under ``src/``
# that ``**`` skips (mirrors check_distribution_urls.py's pathspec pair).
SCAN_GLOBS = ("packages/*/src/**/*.py", "packages/*/src/*.py")


def scan(repo_root: Path = REPO_ROOT) -> list[str]:
    """Return ``path:line:text`` hits of ``__version__`` string literals."""
    proc = subprocess.run(
        ["git", "grep", "-nIE", PATTERN, "--", *SCAN_GLOBS],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    # git grep: rc 0 = matches found, rc 1 = no matches (clean), rc >1 = error.
    if proc.returncode > 1:
        raise RuntimeError(f"git grep failed (rc={proc.returncode}): {proc.stderr}")
    return [line for line in proc.stdout.splitlines() if line.strip()]


def main() -> int:
    hits = scan()
    if hits:
        sys.stderr.write(
            "version-literal-check: hand-copied __version__ literal(s) found — "
            "derive from pyproject via workbay_protocol.version.version_of instead "
            "(pyproject [project].version is the single version authority):\n"
        )
        for hit in hits:
            sys.stderr.write(f"  {hit}\n")
        return 1
    print("version-literal-check: ok (no hand-copied __version__ literals)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
