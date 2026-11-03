#!/usr/bin/env python3
"""Detect ``version_of`` metadata drift for the interpreter this runs under.

Drift = an *installed* WorkBay workspace distribution whose
``importlib.metadata`` version differs from its ``pyproject [project].version``.
``version_of`` (``workbay_protocol.version``) prefers installed metadata and
only falls back to pyproject on ``PackageNotFoundError`` — so a stale editable
build-cache or a leftover pyenv install makes ``__version__`` silently wrong.
This checker fails fast on that, with the exact remediation.

Stdlib-only. The dist set **and** each package dir come from the single member
registry ``scripts/workspace_members.iter_workspace_members`` — never a second
hand-authored list. The declared version is read from
``<repo_root>/<package_relpath>/pyproject.toml``: ``package_relpath`` is ALREADY
repo-relative (e.g. ``"packages/workbay"``), so it is joined to ``repo_root``
directly and **never** re-prefixed with ``packages/`` (that would double-prefix
to ``packages/packages/<name>`` — a path that does not exist, silently dropping
every dist and producing the exact false-green this guard prevents).

Feeds:
* ``--check`` — the ``check-version-drift-pyenv`` make prereq + the post-sync
  in-suite assertion in ``packages/workbay/tests`` (both import this comparison)
  + doctor ``_doctor_version_of_skew`` (implementation note).
* ``--print-installed-dirs`` — historical pyenv reinstall helper (``sync-version-drift-pyenv``
  retired; implementation note Resolution #1).
* ``--print-drifted-names`` — ``uv sync --reinstall-package <names>`` (venv sync).
"""

from __future__ import annotations

import sys
import tomllib
import ast
from collections.abc import Callable
from importlib import metadata
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from workspace_members import iter_workspace_members, repo_root  # noqa: E402

# name -> installed version, or None when the dist is not installed here.
InstalledResolver = Callable[[str], "str | None"]


def _installed_version(dist_name: str) -> str | None:
    """Installed distribution metadata version, or ``None`` when not installed.

    Mirrors ``version_of``'s primary source; ``PackageNotFoundError`` (absent)
    maps to ``None`` so callers can honour the pyproject-fallback contract
    (absent is not drift)."""
    try:
        return metadata.version(dist_name)
    except metadata.PackageNotFoundError:
        return None


def _calls_version_of(path: Path) -> bool:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id == "version_of":
            return True
        if isinstance(func, ast.Attribute) and func.attr == "version_of":
            return True
    return False


def _member_uses_version_of(repo: Path, src_relpath: str) -> bool:
    root = repo / src_relpath
    if root.is_file():
        return _calls_version_of(root)
    if not root.is_dir():
        return False
    return any(_calls_version_of(path) for path in root.rglob("*.py"))


def dist_table(repo: Path | None = None) -> dict[str, tuple[str, str]]:
    """Map ``version_of`` workspace ``dist_name -> (declared_version, package_relpath)``.

    ``declared_version`` is ``<repo>/<package_relpath>/pyproject.toml``
    ``[project].version``; ``package_relpath`` is already repo-relative and is
    joined to ``repo`` without any ``packages/`` re-prefix (RV09-01). Membership
    is derived from source code that actually calls ``version_of(...)`` so
    unrelated workspace distributions cannot false-red this guard."""
    repo = (repo or repo_root()).resolve()
    table: dict[str, tuple[str, str]] = {}
    for member in iter_workspace_members(repo):
        if not _member_uses_version_of(repo, member.src_relpath):
            continue
        pyproject = repo / member.package_relpath / "pyproject.toml"
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        version = data.get("project", {}).get("version")
        if isinstance(version, str):
            table[member.dist_name] = (version, member.package_relpath)
    return table


def installed_drift(
    resolver: InstalledResolver = _installed_version,
    repo: Path | None = None,
) -> dict[str, tuple[str, str]]:
    """Installed dists whose metadata != declared.

    Returns ``dist_name -> (installed_version, declared_version)``. Absent dists
    (resolver returns ``None``) are skipped — not installed == fallback == not
    drift, mirroring ``version_of``."""
    drift: dict[str, tuple[str, str]] = {}
    for dist_name, (declared, _pkg_dir) in dist_table(repo).items():
        installed = resolver(dist_name)
        if installed is None:
            continue
        if installed != declared:
            drift[dist_name] = (installed, declared)
    return drift


def installed_dirs(
    resolver: InstalledResolver = _installed_version,
    repo: Path | None = None,
) -> list[str]:
    """Package dirs of every *installed* workspace dist.

    Historical helper for the retired pyenv reinstall band-aid; retained for
    ``--print-installed-dirs`` / diagnostics only."""
    return [
        pkg_dir
        for dist_name, (_declared, pkg_dir) in dist_table(repo).items()
        if resolver(dist_name) is not None
    ]


def drifted_names(
    resolver: InstalledResolver = _installed_version,
    repo: Path | None = None,
) -> list[str]:
    """Sorted dist names with drift, for ``uv sync --reinstall-package <names>``
    (venv sync target)."""
    return sorted(installed_drift(resolver, repo))


def _format_drift(drift: dict[str, tuple[str, str]]) -> str:
    return "\n".join(
        f"  {name}: installed {installed!r} != declared {declared!r}"
        for name, (installed, declared) in sorted(drift.items())
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--print-installed-dirs" in argv:
        print("\n".join(installed_dirs()))
        return 0
    if "--print-drifted-names" in argv:
        print("\n".join(drifted_names()))
        return 0
    if "--check" in argv:
        drift = installed_drift()
        if drift:
            print(
                "version_of metadata drift (installed distribution metadata != "
                "pyproject [project].version):\n" + _format_drift(drift),
                file=sys.stderr,
            )
            return 1
        return 0
    print(__doc__)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
