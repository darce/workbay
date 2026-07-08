"""Path value objects for plugin surfaces (implementation note S4)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, get_args

from workbay_protocol import (
    CONTRACTS_DIR,
    RULES_DIR,
    RUNTIME_ROOT_DIRNAME,
    TEMPLATES_DIR,
)

SurfaceKind = Literal["shared", "generated", "generator_input", "lifecycle"]

# Single declaration of bootstrap surface provenance (implementation note). Keys are
# literal path strings; install.py re-exports the legacy tuple constants as
# thin views over this table (option b — no install import here).
SURFACE_PROVENANCE: dict[str, SurfaceKind] = {
    # shared — materialized as symlinks into ``.workbay/remote``
    ".github/hooks": "shared",
    "scripts/hooks": "shared",
    CONTRACTS_DIR: "shared",
    RULES_DIR: "shared",
    TEMPLATES_DIR: "shared",
    "Makefile.d": "shared",
    "scripts/workbay": "shared",
    ".codex/hooks.json": "shared",
    # generated — real dirs the generator writes into the target
    ".github/prompts": "generated",
    # generator_input — ledger-resolved against clone/payload, not materialized
    "scripts/generate_agent_workflows.py": "generator_input",
    "config/agent-workflows/portable_commands.json": "generator_input",
    "skills": "generator_input",
}

KNOWN_SURFACE_KINDS: frozenset[SurfaceKind] = frozenset(get_args(SurfaceKind))


def surfaces_for_kind(kind: SurfaceKind) -> tuple[str, ...]:
    """Return paths classified as ``kind``, in stable table insertion order."""
    return tuple(
        path for path, provenance_kind in SURFACE_PROVENANCE.items() if provenance_kind == kind
    )


def is_known_surface_kind(kind: str) -> bool:
    """True when ``kind`` is a recognized :class:`SurfaceKind` value."""
    return kind in KNOWN_SURFACE_KINDS


def should_leave_surface_untouched(kind: str) -> bool:
    """Forward-compat gate: consumers must not act on unknown kinds."""
    return not is_known_surface_kind(kind)


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
