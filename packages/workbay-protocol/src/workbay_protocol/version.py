"""Single-source version resolution for WorkBay packages.

``__version__`` is **not** a hand-copied literal — it derives from the one
version authority, ``pyproject.toml [project].version``, surfaced at runtime via
installed distribution metadata. In an installed / wheel context
``importlib.metadata`` is authoritative; in the repo's PYTHONPATH-only test and
gate runs (the package is not installed) it falls back to parsing the adjacent
``pyproject.toml``, so pyproject stays the single source and no second literal
can drift (audit internal, findings PPSSOT-VER-*).

Callers pass their own ``__file__`` as ``anchor`` so the fallback can locate the
owning package's ``pyproject.toml``::

    from workbay_protocol.version import version_of

    __version__ = version_of("mcp-workbay-handoff", anchor=__file__)
"""

from __future__ import annotations

import re
from importlib import metadata
from pathlib import Path

__all__ = ["version_of"]


def _canonical(dist_name: str) -> str:
    """PEP 503 canonical distribution name (case/`-`/`_`/`.` insensitive)."""
    return re.sub(r"[-_.]+", "-", dist_name).lower()


def version_of(dist_name: str, *, anchor: str | Path | None = None) -> str:
    """Return ``dist_name``'s version from installed metadata, else pyproject.

    Never a hand-authored literal: the installed distribution metadata is the
    production source; the adjacent ``pyproject.toml`` is the dev/test fallback.
    Returns ``"0+unknown"`` only when neither is resolvable.
    """
    try:
        return metadata.version(dist_name)
    except metadata.PackageNotFoundError:
        if anchor is not None:
            found = _adjacent_pyproject_version(Path(anchor), dist_name)
            if found is not None:
                return found
        return "0+unknown"


def _adjacent_pyproject_version(anchor: Path, dist_name: str) -> str | None:
    """Walk up from ``anchor``'s directory to the OWNING package's
    ``pyproject.toml`` — the first whose ``[project].name`` matches
    ``dist_name`` — and return its ``[project].version``. Stdlib-only.

    The dist-name guard bounds the walk to the package that actually owns the
    anchor. Without it the walk stopped at the *first* pyproject declaring any
    ``[project].version`` and could leak a parent's version (e.g. the monorepo
    root ``[project].version``) when the package's own pyproject was unreadable
    (PPSSOT-VER / S2 fallback-boundary). No matching pyproject -> ``None`` ->
    caller returns ``"0+unknown"``, never a wrong sibling/parent version.
    """
    target = _canonical(dist_name)
    start = anchor.resolve().parent
    for parent in (start, *start.parents):
        candidate = parent / "pyproject.toml"
        if candidate.is_file():
            name, version = _read_project_name_version(candidate)
            if name is not None and _canonical(name) == target and version is not None:
                return version
    return None


def _read_project_name_version(pyproject: Path) -> tuple[str | None, str | None]:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover -- py<3.11; all members are >=3.11
        return None, None
    try:
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None, None
    project = data.get("project", {})
    name = project.get("name")
    version = project.get("version")
    return (
        name if isinstance(name, str) else None,
        version if isinstance(version, str) else None,
    )
