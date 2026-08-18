"""Per-package pytest path-guard helper (internal).

Hard-fails the pytest session at ``pytest_sessionstart`` if any in-repo
agentic package was imported from outside the active worktree root.
Per-package ``conftest.py`` shims call ``check_path_guard`` so
``cd packages/<pkg> && uv run pytest`` catches the case where an
environment-wide editable install points at a different worktree.

It is fine for the interpreter / site-packages to live outside the
worktree (the standard remote-sandbox lane venv topology). What is not
fine is for a guarded module's *source origin* to resolve outside the
worktree — e.g. a stale editable install shadowing the tree under test.

Opt-out via ``WORKBAY_DISABLE_PYTEST_PATH_GUARD=1`` for cross-worktree
fixture work where loading from outside is intentional.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Callable, Iterable, Sequence
from urllib.parse import unquote, urlparse

GUARDED_TOP_LEVEL_NAMES = (
    "workbay_handoff_mcp",
    "workbay_orchestrator_mcp",
)
GUARDED_TOP_LEVEL_PREFIXES = ("workbay_",)
OPT_OUT_ENV = "WORKBAY_DISABLE_PYTEST_PATH_GUARD"

OriginResolver = Callable[[str], Path | None]


def _is_guarded_top_level(name: str) -> bool:
    if "." in name:
        return False
    if name in GUARDED_TOP_LEVEL_NAMES:
        return True
    return any(name.startswith(prefix) for prefix in GUARDED_TOP_LEVEL_PREFIXES)


def _file_url_to_path(url: str) -> Path | None:
    """Convert a ``file://`` URL from direct_url.json into a filesystem path."""
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return None
    # urlparse gives path as /path on POSIX; unquote handles %20 etc.
    path = unquote(parsed.path)
    if not path:
        return None
    return Path(path)


def _editable_origin_from_direct_url(direct_url_text: str) -> Path | None:
    """Parse PEP 610 direct_url.json; return editable source dir if present."""
    try:
        data = json.loads(direct_url_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    dir_info = data.get("dir_info")
    if not isinstance(dir_info, dict) or not dir_info.get("editable"):
        return None
    url = data.get("url")
    if not isinstance(url, str) or not url:
        return None
    origin = _file_url_to_path(url)
    if origin is None:
        return None
    try:
        return origin.resolve()
    except OSError:
        return None


def _default_editable_origin(module_name: str) -> Path | None:
    """Look up the editable install origin for ``module_name`` via importlib.metadata.

    Remote sandbox lane venvs sit outside the worktree; uv still records
    ``direct_url.json`` with ``dir_info.editable: true`` pointing at the
    worktree package directory. Prefer that origin over a site-packages
    ``__file__`` so the guard does not abort the standard topology.
    """
    try:
        import importlib.metadata as metadata
    except ImportError:  # pragma: no cover - stdlib on supported Pythons
        return None

    try:
        dist_names = metadata.packages_distributions().get(module_name) or []
    except Exception:  # pragma: no cover - defensive; metadata can raise
        return None

    for dist_name in dist_names:
        try:
            dist = metadata.distribution(dist_name)
        except metadata.PackageNotFoundError:
            continue
        try:
            direct_url_text = dist.read_text("direct_url.json")
        except Exception:  # pragma: no cover - dist-info edge cases
            continue
        if not direct_url_text:
            continue
        origin = _editable_origin_from_direct_url(direct_url_text)
        if origin is not None:
            return origin
    return None


def _path_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolve_origin(origin: Path | None) -> Path | None:
    if origin is None:
        return None
    try:
        return origin.resolve()
    except OSError:
        return None


def _effective_source_path(
    name: str,
    module: object,
    worktree_root: Path,
    *,
    origin_resolver: OriginResolver | None = None,
    consult_metadata: bool = True,
) -> tuple[Path | None, Path | None]:
    """Return ``(effective_path, claimed_origin_if_shadow)`` for membership.

    Preference / correlation order:
    1. Resolved ``__file__`` when it already lives under ``worktree_root``
       (definitive — the module was loaded from in-tree source; B11).
    2. Editable install origin (PEP 610 direct_url or injected resolver) only
       when it is *consistent* with the loaded file:
       - If ``__file__`` resolves under the origin tree, accept the origin
         (redirect / ``.pth`` editable layout).
       - If the origin itself is outside the worktree, surface the origin so
         stale-editable remediation names the wrong install target.
       - If the origin claims in-worktree source but ``__file__`` is a
         disjoint outside path (physical site-packages shadow), **reject**
         the origin and use ``__file__`` — the code that is executing.
    3. Otherwise the resolved ``__file__`` (outside → violation).

    True remote-sandbox editable installs resolve ``__file__`` into the
    worktree via ``.pth`` / import hooks (step 1). A physical copy under
    site-packages with editable metadata still pointing in-tree is a
    false-green if origin is trusted blindly — that is the PG-002 case.
    """
    file_attr = getattr(module, "__file__", None)
    if not isinstance(file_attr, str) or not file_attr:
        return None, None
    try:
        file_path = Path(file_attr).resolve()
    except OSError:
        return None, None

    root = worktree_root.resolve()
    if _path_under(file_path, root):
        return file_path, None

    origin: Path | None = None
    if origin_resolver is not None:
        try:
            origin = _resolve_origin(origin_resolver(name))
        except Exception:  # pragma: no cover - never let resolver crash the guard
            origin = None
    elif consult_metadata:
        try:
            origin = _resolve_origin(_default_editable_origin(name))
        except Exception:  # pragma: no cover - never let metadata crash the guard
            origin = None

    if origin is None:
        return file_path, None

    # Consistent redirect: loaded file actually lives under the origin tree.
    if _path_under(file_path, origin):
        return origin, None

    # Stale origin outside the worktree — name the wrong install target.
    if not _path_under(origin, root):
        return origin, None

    # In-worktree origin + disjoint outside __file__ = shadow copy.
    # Flag the path that is actually loaded; keep origin for remediation.
    return file_path, origin


def collect_violations(
    worktree_root: Path,
    modules: Iterable[tuple[str, object]] | None = None,
    *,
    origin_resolver: OriginResolver | None = None,
) -> list[tuple[str, Path, Path] | tuple[str, Path, Path, Path]]:
    """Return violation tuples for guarded modules outside ``worktree_root``.

    Each entry is ``(name, actual_path, worktree_root)`` or, when a shadow
    copy was detected, ``(name, actual_path, worktree_root, claimed_origin)``
    so remediation can name both the loaded file and the editable claim.

    ``actual_path`` is the *effective* source path used for membership:
    in-worktree ``__file__`` when the module loaded from the tree; correlated
    editable origin when consistent with the load; otherwise the resolved
    ``__file__`` (including physical shadows of an in-tree origin).

    When ``modules`` is an explicit injected list (unit tests), live install
    metadata is not consulted unless the caller supplies ``origin_resolver``.
    Production callers (``modules is None``) use importlib.metadata.
    """
    root = worktree_root.resolve()
    if modules is not None:
        iterable = list(modules)
        # Hermetic for synthetic module lists: only the injected origin_resolver
        # may supply origin; do not read the process-global package install map.
        consult_metadata = False
    else:
        iterable = list(sys.modules.items())
        consult_metadata = True

    violations: list[tuple[str, Path, Path] | tuple[str, Path, Path, Path]] = []
    for name, module in iterable:
        if module is None or not _is_guarded_top_level(name):
            continue
        actual, shadow_origin = _effective_source_path(
            name,
            module,
            root,
            origin_resolver=origin_resolver,
            consult_metadata=consult_metadata,
        )
        if actual is None:
            continue
        if not _path_under(actual, root):
            if shadow_origin is not None:
                violations.append((name, actual, root, shadow_origin))
            else:
                violations.append((name, actual, root))
    return violations


def remediation_message(
    violations: Sequence[tuple[str, Path, Path] | tuple[str, Path, Path, Path]],
    cwd: Path | None = None,
) -> str:
    here = (cwd or Path.cwd()).resolve()
    lines: list[str] = []
    for entry in violations:
        name = entry[0]
        actual = entry[1]
        claimed = entry[3] if len(entry) > 3 else None
        if claimed is not None:
            loc = f"{actual} (editable origin claims {claimed})"
        else:
            loc = str(actual)
        lines.append(
            f"{name} loaded from {loc}, but cwd is {here}. "
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
