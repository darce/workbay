"""Path value objects for plugin surfaces (implementation note S4)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from workbay_protocol import RUNTIME_ROOT_DIRNAME

CLONE_SUBDIR = (RUNTIME_ROOT_DIRNAME, "remote")


def path_resolves_under(path: Path, root: Path) -> bool:
    """True when ``path`` resolves to a location under ``root``."""
    try:
        resolved = path.resolve()
        root_resolved = root.resolve()
    except OSError:
        return False
    try:
        resolved.relative_to(root_resolved)
    except ValueError:
        return False
    return True


def overlay_clone_homes(target: Path) -> tuple[Path, ...]:
    """Canonical managed clone root under ``target`` (single-element for call-site compatibility)."""
    return (target.joinpath(*CLONE_SUBDIR),)


from workbay_bootstrap.harnesses import PLUGIN_GENERATED_ROOT, PLUGIN_NAME


@dataclass(frozen=True)
class PluginTreeRef:
    """Reference to a generated plugin tree (base or effective variant)."""

    root: Path
    harness: str
    variant: str  # ``base`` | ``effective``

    def path(self) -> Path:
        return self.root.joinpath(*PLUGIN_GENERATED_ROOT, self.variant, self.harness)

    def relative(self) -> str:
        return f"./{Path(*PLUGIN_GENERATED_ROOT, self.variant, self.harness).as_posix()}"


@dataclass(frozen=True)
class SurfaceLink:
    """A materialized surface link between source and target."""

    source: Path
    target: Path
    kind: str  # ``symlink`` | ``copy``


@dataclass(frozen=True)
class MarketplacePin:
    """A harness marketplace pointer surface."""

    harness: str
    path: Path
    payload: Mapping[str, Any]


def plugin_tree_out(target: Path, kind: str) -> Path:
    """Return the generated plugin tree root for ``kind`` (``base``/``effective``)."""
    return target.joinpath(*PLUGIN_GENERATED_ROOT, kind)


def relative_plugin_tree_path(kind: str, harness: str) -> str:
    return PluginTreeRef(root=Path("."), harness=harness, variant=kind).relative()


def clone_layout_probe_roots(clone: Path) -> tuple[Path, Path, Path]:
    """Named fallback roots for :func:`resolve_in_clone` (payload, subdir, root)."""
    payload_subdir = "packages/workbay-system/workbay_system/payload"
    system_subdir = "packages/workbay-system"
    return (
        clone / payload_subdir,
        clone / system_subdir,
        clone,
    )