"""Single-source version resolution for WorkBay packages.

``__version__`` is **not** a hand-copied literal — it derives from the one
version authority, ``pyproject.toml [project].version``. That authority is
resolved in one of two ways, in priority order:

1. **The owning package's in-tree ``pyproject.toml``**, located by walking up
   from the caller's ``anchor`` (``__file__``). Whenever it is adjacent — a
   source checkout, a PYTHONPATH=src gate run, or an *editable* install (whose
   ``.pth`` puts ``src`` on ``sys.path``, so ``anchor`` still resolves into the
   source tree) — it is authoritative. This is the fix for internal-*:
   an editable build-cache or a leftover pyenv install can leave stale
   ``importlib.metadata`` dist-info, but the *code actually running* is the
   in-tree source, so its pyproject is the truth. Preferring it makes
   ``__version__`` immune to metadata drift (previously ``version_of`` trusted
   the stale dist-info and reported a wrong version; that drift is now cosmetic).
2. **Installed distribution metadata** (``importlib.metadata``) — the shipped
   wheel case, where no pyproject is adjacent to the importable module.

Returns ``"0+unknown"`` only when neither resolves (audit
internal, findings PPSSOT-VER-*).

Callers pass their own ``__file__`` as ``anchor`` so the owning package's
``pyproject.toml`` can be located::

    from workbay_protocol.version import version_of

    __version__ = version_of("mcp-workbay-handoff", anchor=__file__)
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache
from importlib import metadata
from pathlib import Path

__all__ = ["version_of"]

_log = logging.getLogger(__name__)


def _canonical(dist_name: str) -> str:
    """PEP 503 canonical distribution name (case/`-`/`_`/`.` insensitive)."""
    return re.sub(r"[-_.]+", "-", dist_name).lower()


@lru_cache(maxsize=None)
def version_of(dist_name: str, *, anchor: str | Path | None = None) -> str:
    """Return ``dist_name``'s version from the owning in-tree pyproject, else
    installed metadata.

    Never a hand-authored literal. The owning package's adjacent
    ``pyproject.toml`` (located from ``anchor``, matched on ``[project].name``) is
    authoritative when present — a **src-layout** editable install / PYTHONPATH-src
    / source checkout runs that source, so its pyproject is the truth even when
    ``importlib.metadata`` dist-info has drifted (stale editable build-cache or
    leftover non-editable install; internal). Installed metadata is the
    shipped-wheel source, used when no owning pyproject is adjacent to the
    importable module. Returns ``"0+unknown"`` only when neither is resolvable.

    Observability ([OBS-08], implementation note): when BOTH sources resolve and disagree,
    the in-tree value is returned **and** a one-line skew warning is logged —
    silent healing would hide the install rot. Memoized (`lru_cache`), so the
    walk and the warning happen once per ``(dist_name, anchor)`` per process.

    Caveats: the src-truth path assumes the editable scheme maps the module to
    its real source path (hatchling/`editables` `.pth`); an import-hook / PEP 660
    finder whose ``__file__`` points into a build cache has no adjacent pyproject
    and falls back to metadata. A pyproject with a **dynamic** ``[project.version]``
    yields no static version, so metadata wins for that dist.
    """
    pyproject_version: str | None = None
    if anchor is not None:
        try:
            pyproject_version = _adjacent_pyproject_version(Path(anchor), dist_name)
        except OSError:
            # A broken/looping symlinked anchor must never turn a version lookup
            # into an import-time crash; fall through to installed metadata.
            pyproject_version = None

    try:
        installed_version: str | None = metadata.version(dist_name)
    except metadata.PackageNotFoundError:
        installed_version = None

    if pyproject_version is not None:
        if installed_version is not None and installed_version != pyproject_version:
            _log.warning(
                "version_of: %s installed dist-info %r disagrees with in-tree "
                "pyproject %r; using in-tree (source of record). Stale install — "
                "re-provision this interpreter's dist-info from src.",
                dist_name,
                installed_version,
                pyproject_version,
            )
        return pyproject_version
    if installed_version is not None:
        return installed_version
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
