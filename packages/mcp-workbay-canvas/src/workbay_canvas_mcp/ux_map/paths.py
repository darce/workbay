"""Maps-dir and map file path resolution (internal path conventions)."""

from __future__ import annotations

from pathlib import Path


class MapPathError(ValueError):
    """Raised when map path / map_ref / maps-dir cannot be resolved."""


def resolve_maps_dir(
    *,
    maps_dir: str | Path | None = None,
    workspace_root: str | Path | None = None,
) -> Path:
    """Resolve the maps directory.

    Precedence:
    1. absolute ``maps_dir`` if provided
    2. ``maps_dir`` resolved relative to ``workspace_root`` (or cwd)
    3. default ``<workspace-root>/docs/ux-maps``
    """

    root = Path(workspace_root) if workspace_root is not None else Path.cwd()
    if maps_dir is None:
        return (root / "docs" / "ux-maps").resolve()
    p = Path(maps_dir)
    if p.is_absolute():
        return p.resolve()
    return (root / p).resolve()


def resolve_map_path(
    *,
    path: str | Path | None = None,
    map_ref: str | None = None,
    maps_dir: str | Path | None = None,
    workspace_root: str | Path | None = None,
) -> Path:
    """Resolve a filesystem path to a ``.uxmap.json`` file.

    Prefer explicit ``path``. Otherwise require ``map_ref`` and look up
    ``{map_ref}.uxmap.json`` under the resolved maps directory.
    """

    if path is not None:
        p = Path(path).expanduser()
        if p.is_absolute():
            return p.resolve()
        # Relative --path resolves under workspace_root (or cwd) so dogfood
        # paths stay portable across worktrees.
        root = Path(workspace_root) if workspace_root is not None else Path.cwd()
        return (root / p).resolve()
    if not map_ref:
        raise MapPathError("provide path or map_ref")
    directory = resolve_maps_dir(maps_dir=maps_dir, workspace_root=workspace_root)
    # Allow map_ref with or without .uxmap.json suffix
    name = map_ref if map_ref.endswith(".uxmap.json") else f"{map_ref}.uxmap.json"
    resolved = (directory / name).resolve()
    if not resolved.is_relative_to(directory):
        raise MapPathError(f"map_ref escapes maps dir: {map_ref!r}")
    return resolved
