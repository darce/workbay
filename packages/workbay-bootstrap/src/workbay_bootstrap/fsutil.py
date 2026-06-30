"""Shared filesystem/JSON helpers (internal, RF29-S3-01).

Public home for helpers that grew up as ``install.py`` privates but are
consumed across modules (``harnesses.py``). ``install.py`` re-imports them
under the legacy private aliases for its internal call sites.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Mapping

from workbay_bootstrap.surfaces import overlay_clone_homes, path_resolves_under

STALE_PACKAGE_DIR_PREFIXES = ("workstate-", "mcp-workstate-")


def deep_merge(dst: dict[str, Any], src: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge ``src`` into ``dst`` and return ``dst``.

    Dict-into-dict merges recurse. Any non-dict value in ``src`` (including
    lists) replaces the corresponding key in ``dst`` outright — list-concat
    semantics would silently grow user config across reruns.
    """
    for key, value in src.items():
        existing = dst.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            deep_merge(existing, value)
        elif isinstance(value, Mapping):
            new_dict: dict[str, Any] = {}
            deep_merge(new_dict, value)
            dst[key] = new_dict
        else:
            dst[key] = value
    return dst


def write_json_file(
    path: Path, payload: dict[str, Any], *, manifest_path: str | None = None
) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2) + "\n"
    manifest_entry_path = manifest_path or path.as_posix()
    if path.exists():
        previous = path.read_text()
        if previous == content:
            return {"path": manifest_entry_path, "action": "unchanged"}
        path.write_text(content)
        return {"path": manifest_entry_path, "action": "updated"}
    path.write_text(content)
    return {"path": manifest_entry_path, "action": "created"}

def _entry_path(target: Path, path: Path) -> str:
    try:
        return path.relative_to(target).as_posix()
    except ValueError:
        return path.as_posix()


def _symlink_resolves_into(target: Path, candidate: Path) -> bool:
    if not candidate.exists():
        return False
    for path in target.rglob("*"):
        if ".git" in path.parts:
            continue
        if not path.is_symlink():
            continue
        raw = os.readlink(path)
        link_target = Path(raw) if os.path.isabs(raw) else path.parent / raw
        if path_resolves_under(link_target, candidate):
            return True
    return False


def _stale_package_dirs(target: Path) -> list[Path]:
    packages = target / "packages"
    if not packages.is_dir():
        return []
    stale: list[Path] = []
    for child in packages.iterdir():
        if not child.is_dir():
            continue
        if child.name.startswith(STALE_PACKAGE_DIR_PREFIXES):
            stale.append(child)
    return stale


def _dir_has_tracked_files(target: Path, path: Path) -> bool | None:
    """Whether any git-tracked file lives under ``path``.

    A reclaim candidate that holds git-tracked content is real source — a
    legitimate workspace member or operator file — NOT untracked overlay debris,
    and must never be removed (the S4/B5 reclaimer is scoped to *untracked* stale
    trees). Returns ``True`` when tracked content exists, ``False`` when the dir
    is genuinely untracked, and ``None`` when git cannot answer (target is not a
    repo, git is unavailable) so the caller can fail safe and preserve it.
    """
    try:
        rel = path.relative_to(target).as_posix()
    except ValueError:
        rel = str(path)
    try:
        result = subprocess.run(
            ["git", "-C", str(target), "ls-files", "--", rel],
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, ValueError):
        return None
    if result.returncode != 0:
        # Not a git repo / git error -> unknowable -> caller preserves.
        return None
    return bool(result.stdout.strip())


def plan_overlay_reclaim(target: Path) -> list[dict[str, object]]:
    """Plan reclaimable overlay debris under ``target``."""
    from workbay_bootstrap.install import still_resolves_through_clone

    target = target.resolve()
    planned: list[dict[str, object]] = []

    for clone_home in overlay_clone_homes(target):
        if not clone_home.is_dir():
            continue
        load_bearing = still_resolves_through_clone(target, clone_home)
        reason = (
            "hooks or surfaces still resolve through clone"
            if load_bearing
            else "orphaned overlay clone home"
        )
        planned.append(
            {
                "path": _entry_path(target, clone_home),
                "kind": "overlay_clone",
                "load_bearing": load_bearing,
                "reason": reason,
            }
        )

    for package_dir in _stale_package_dirs(target):
        symlinked = _symlink_resolves_into(target, package_dir)
        tracked = _dir_has_tracked_files(target, package_dir)
        # Preserve (refuse to reclaim) when ANY of: a live symlink resolves into
        # it, it holds git-tracked source, or git status is unknowable. Only a
        # provably-untracked, non-resolved dir is debris safe to remove (BB-1).
        load_bearing = symlinked or tracked is True or tracked is None
        if symlinked:
            reason = "symlink under target still resolves into stale package dir"
        elif tracked is True:
            reason = "git-tracked source — preserved, not overlay debris"
        elif tracked is None:
            reason = "git status unknowable — preserved (fail-safe)"
        else:
            reason = "stale workspace package dir"
        planned.append(
            {
                "path": _entry_path(target, package_dir),
                "kind": "stale_package",
                "load_bearing": load_bearing,
                "reason": reason,
            }
        )

    return planned


def execute_overlay_reclaim(
    target: Path, *, dry_run: bool, apply: bool
) -> dict[str, list]:
    """Plan and optionally remove non-load-bearing overlay reclaim candidates."""
    if not dry_run and not apply:
        raise ValueError("overlay reclaim requires --dry-run or --yes")

    target = target.resolve()
    planned = plan_overlay_reclaim(target)
    refused = [entry for entry in planned if entry["load_bearing"]]
    reclaimable = [entry for entry in planned if not entry["load_bearing"]]
    reclaimed: list[str] = []
    failed: list[dict[str, str]] = []

    if apply:
        from workbay_bootstrap.install import _prune_empty_parent_dirs

        for entry in reclaimable:
            rel = str(entry["path"])
            path = target / rel
            try:
                if path.is_symlink():
                    # A symlinked candidate: remove only the link, never its
                    # target — and rmtree() raises on a symlink (BB-3).
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)
                elif path.exists():
                    path.unlink()
                else:
                    continue  # already gone
                _prune_empty_parent_dirs(path.parent, target)
            except OSError as exc:
                # Isolate per-entry failures so one bad path cannot abort the run
                # mid-way, leaving a partial non-atomic teardown (BB-3).
                failed.append({"path": rel, "error": str(exc)})
                continue
            reclaimed.append(rel)

    return {
        "planned": planned,
        "reclaimed": reclaimed,
        "refused": refused,
        "failed": failed,
    }
