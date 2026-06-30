from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


SurfaceKind = Literal["skills", "hooks", "commands", "prompts", "contracts"]
ResolvedSource = Literal["shared", "local", "overlapping"]
OverlayMode = Literal["source_tree", "canonical", "legacy"]

DEFAULT_SURFACE_ROOTS: dict[SurfaceKind, Path] = {
    "skills": Path(".claude/skills"),
    "hooks": Path(".github/hooks"),
    "commands": Path(".claude/commands"),
    "prompts": Path(".github/prompts"),
    "contracts": Path("docs/workbay/contracts"),
}

# Manifest filenames. Duplicated here (rather than imported from
# workbay-bootstrap) so workbay-system stays decoupled from the bootstrap
# package — the same way the validator scripts probe filenames directly today.
BOOTSTRAP_MANIFEST_NAME = ".workbay-bootstrap.json"
LEGACY_OVERLAY_MANIFEST_NAME = ".workbay-overlay.json"

# internal: the canonical bootstrap ledger keys surfaces by
# filesystem ``path`` (not by resolver ``kind``), so the resolver maps each
# kind to the ledger path(s) it owns. ``skills`` / ``commands`` are absent on
# purpose — they moved to the generated plugin tree under
# ``.workbay/generated/plugins/...`` and are no longer ledger surfaces; a
# kind missing from this map falls through to source-tree/default resolution
# and is never treated as broken-overlay drift.
CANONICAL_KIND_LEDGER_PATHS: dict[SurfaceKind, tuple[str, ...]] = {
    "contracts": ("docs/workbay/contracts",),
    "hooks": (".github/hooks", "scripts/hooks"),
    "prompts": (".github/prompts",),
}

# Bootstrap clone root that ``source="shared"`` surfaces symlink into.
_CLONE_SUBDIR = (".workbay", "remote")
_LEGACY_CLONE_SUBDIR = (".workstate", "remote")


class OverlayResolverError(RuntimeError):
    """Base class for overlay resolution errors."""


class BrokenOverlayError(OverlayResolverError):
    """Raised when an overlay entry points to a missing target."""


class HalfMaterializedOverlayError(BrokenOverlayError):
    """Raised when ledger mode and on-disk overlay materialization disagree."""


@dataclass(frozen=True)
class ResolvedPath:
    source: ResolvedSource
    effective_path: Path
    shared_path: Path | None = None
    local_path: Path | None = None


def _load_overlay_manifest(project_root: Path) -> dict | None:
    manifest_path = project_root / ".workbay-overlay.json"
    if not manifest_path.is_file():
        return None

    try:
        payload = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        raise OverlayResolverError(f"overlay manifest is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise OverlayResolverError("overlay manifest must parse to a mapping")
    return payload


def _surface_roots(project_root: Path, kind: SurfaceKind) -> tuple[Path, Path] | None:
    manifest = _load_overlay_manifest(project_root)
    if manifest is None:
        return None

    surfaces = manifest.get("surfaces")
    if not isinstance(surfaces, dict):
        raise OverlayResolverError("overlay manifest must define a `surfaces` mapping")

    surface = surfaces.get(kind)
    if surface is None:
        return None
    if not isinstance(surface, dict):
        raise OverlayResolverError(f"overlay manifest `surfaces.{kind}` must be a mapping")

    shared_root = surface.get("shared_root")
    local_root = surface.get("local_root")
    if not isinstance(shared_root, str) or not shared_root.strip():
        raise OverlayResolverError(f"overlay manifest `surfaces.{kind}.shared_root` must be a non-empty string")
    if not isinstance(local_root, str) or not local_root.strip():
        raise OverlayResolverError(f"overlay manifest `surfaces.{kind}.local_root` must be a non-empty string")

    return project_root / shared_root, project_root / local_root


def _iter_surface_entries(root: Path) -> dict[str, Path]:
    if not root.exists():
        return {}

    entries: dict[str, Path] = {}
    for entry in sorted(root.iterdir(), key=lambda path: path.name):
        if entry.name.startswith("."):
            continue
        if entry.is_file() or entry.is_dir() or entry.is_symlink():
            entries[entry.name] = entry
    return entries


def _iter_hook_entries(anchor: Path) -> dict[str, Path]:
    entries: dict[str, Path] = {}
    hook_roots = (
        anchor / ".github" / "hooks",
        anchor / "scripts" / "hooks",
    )

    for hook_root in hook_roots:
        if not hook_root.exists():
            continue
        for entry in sorted(hook_root.rglob("*"), key=lambda path: path.as_posix()):
            if not (entry.is_file() or entry.is_symlink()):
                continue
            relative_from_root = entry.relative_to(hook_root)
            if any(part.startswith(".") or part == "__pycache__" for part in relative_from_root.parts):
                continue
            entries[entry.relative_to(anchor).as_posix()] = entry
    return entries


def _hook_anchor_from_surface_root(surface_root: Path) -> Path:
    suffix = Path(".github/hooks")
    suffix_parts = suffix.parts
    if surface_root.parts[-len(suffix_parts) :] == suffix_parts:
        return surface_root.parents[1]
    return surface_root


def _iter_hook_files(hook_root: Path) -> dict[str, Path]:
    """Recursively enumerate hook files under a *single* concrete hook root.

    Unlike :func:`_iter_hook_entries` (which scans both ``.github/hooks`` and
    ``scripts/hooks`` under one anchor), this scans exactly the directory it is
    given, so a canonical-ledger caller can apply each ledger entry's own
    ``source`` to its own files. Keyed by path relative to ``hook_root``.
    """
    entries: dict[str, Path] = {}
    if not hook_root.exists():
        return entries
    for entry in sorted(hook_root.rglob("*"), key=lambda path: path.as_posix()):
        if not (entry.is_file() or entry.is_symlink()):
            continue
        relative = entry.relative_to(hook_root)
        if any(part.startswith(".") or part == "__pycache__" for part in relative.parts):
            continue
        entries[relative.as_posix()] = entry
    return entries


def _validate_entry(path: Path, *, project_root: Path, label: str) -> None:
    if path.is_symlink() and not path.exists():
        raise BrokenOverlayError(
            f"{path.relative_to(project_root)} points to a missing {label} overlay target. "
            "Run workbay-bootstrap repair to restore the overlay."
        )


def _legacy_is_bootstrap_owned(project_root: Path) -> bool:
    """Return True when the legacy file is a stale *bootstrap-owned* ledger.

    Bootstrap-owned ledgers carry ``surfaces`` as a *list* (the shape
    ``workbay-bootstrap``'s ``_migrate_legacy_manifest`` migrates), whereas a
    user-owned legacy overlay keys ``surfaces`` as a *mapping*. Only the latter
    makes a dual-manifest state genuinely ambiguous.
    """
    try:
        payload = json.loads((project_root / LEGACY_OVERLAY_MANIFEST_NAME).read_text())
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and isinstance(payload.get("surfaces"), list)


def detect_overlay_mode(project_root: Path) -> OverlayMode:
    """Classify the consumer overlay state.

    Raises ``OverlayResolverError`` for an ambiguous dual-manifest state where a
    *user-owned* legacy mapping overlay coexists with the canonical bootstrap
    ledger — the resolver must not silently pick one authority.
    """
    project_root = Path(project_root).expanduser().resolve()
    has_canonical = (project_root / BOOTSTRAP_MANIFEST_NAME).is_file()
    has_legacy = (project_root / LEGACY_OVERLAY_MANIFEST_NAME).is_file()

    if has_canonical and has_legacy:
        if _legacy_is_bootstrap_owned(project_root):
            # Stale bootstrap-owned file: canonical wins; operator should let
            # `workbay-bootstrap` migrate/remove the legacy copy.
            return "canonical"
        raise OverlayResolverError(
            f"both {BOOTSTRAP_MANIFEST_NAME} and a user-owned {LEGACY_OVERLAY_MANIFEST_NAME} "
            "exist; refusing to choose a manifest authority. Migrate or remove the legacy "
            f"overlay, or run `workbay-bootstrap doctor --target {project_root}` followed by "
            f"`workbay-bootstrap repair --target {project_root}`."
        )
    if has_canonical:
        return "canonical"
    if has_legacy:
        return "legacy"
    return "source_tree"




def _manifest_source_kind(manifest: dict) -> str:
    kind = manifest.get("source_kind")
    if isinstance(kind, str) and kind.strip():
        return kind.strip()
    return "git_overlay"


def _overlay_remediation(project_root: Path) -> str:
    return (
        f"Run `workbay-bootstrap doctor --target {project_root}` then "
        f"`workbay-bootstrap repair --target {project_root}`."
    )


def _validate_ledger_materialization(project_root: Path, manifest: dict) -> None:
    """Fail loud when bootstrap ledger mode disagrees with clone/symlink residue."""
    source_kind = _manifest_source_kind(manifest)
    remediation = _overlay_remediation(project_root)
    if source_kind != "package":
        return
    if project_root.joinpath(*_LEGACY_CLONE_SUBDIR).exists():
        raise HalfMaterializedOverlayError(
            "bootstrap ledger records source_kind=package but legacy clone "
            f"`.workstate/remote` is still present. {remediation}"
        )
    if project_root.joinpath(*_CLONE_SUBDIR, ".git").exists():
        raise HalfMaterializedOverlayError(
            "bootstrap ledger records source_kind=package but clone "
            f"`.workbay/remote/.git` still exists. {remediation}"
        )

def _load_bootstrap_manifest(project_root: Path) -> dict:
    """Parse and shape-check the canonical ``.workbay-bootstrap.json`` ledger."""
    manifest_path = project_root / BOOTSTRAP_MANIFEST_NAME
    try:
        payload = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        raise OverlayResolverError(
            f"canonical bootstrap ledger {BOOTSTRAP_MANIFEST_NAME} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise OverlayResolverError(
            f"canonical bootstrap ledger {BOOTSTRAP_MANIFEST_NAME} must parse to a mapping"
        )
    if not isinstance(payload.get("schema_version"), int):
        raise OverlayResolverError(
            f"canonical bootstrap ledger {BOOTSTRAP_MANIFEST_NAME} is missing an integer "
            "`schema_version`"
        )
    source_kind = _manifest_source_kind(payload)
    if source_kind == "package":
        package_version = payload.get("package_version")
        if not isinstance(package_version, str) or not package_version.strip():
            raise OverlayResolverError(
                f"canonical bootstrap ledger {BOOTSTRAP_MANIFEST_NAME} is missing package "
                "provenance (`package_version`) for source_kind=package"
            )
    else:
        remote_sha = payload.get("remote_sha")
        if not isinstance(remote_sha, str) or not remote_sha.strip():
            raise OverlayResolverError(
                f"canonical bootstrap ledger {BOOTSTRAP_MANIFEST_NAME} is missing clone metadata "
                "(`remote_sha`)"
            )
    if not isinstance(payload.get("surfaces"), list):
        raise OverlayResolverError(
            f"canonical bootstrap ledger {BOOTSTRAP_MANIFEST_NAME} `surfaces` must be a list "
            "of {path, source} entries"
        )
    return payload


def _ledger_entry_by_path(manifest: dict) -> dict[str, dict]:
    entries: dict[str, dict] = {}
    for entry in manifest["surfaces"]:
        if isinstance(entry, dict) and isinstance(entry.get("path"), str):
            entries[entry["path"].rstrip("/")] = entry
    return entries


def _validate_shared_surface(
    surface_root: Path, *, project_root: Path, rel: str, manifest: dict
) -> None:
    """Fail closed when a ``source="shared"`` ledger surface is broken."""
    remediation = _overlay_remediation(project_root)
    source_kind = _manifest_source_kind(manifest)

    if source_kind == "package":
        if surface_root.is_symlink():
            try:
                resolved = surface_root.resolve().as_posix()
            except OSError:
                resolved = "<unresolvable>"
            if ".workstate/remote" in resolved:
                raise HalfMaterializedOverlayError(
                    f"shared overlay surface `{rel}` symlink resolves through legacy "
                    f"`.workstate/remote` on a package-mode ledger (`{resolved}`). "
                    f"{remediation}"
                )
            raise HalfMaterializedOverlayError(
                f"shared overlay surface `{rel}` is symlink-backed on a package-mode "
                f"ledger (expected a materialized directory). {remediation}"
            )
        if not surface_root.exists():
            raise BrokenOverlayError(
                f"shared overlay surface `{rel}` is missing on a package-mode ledger. "
                f"{remediation}"
            )
        return

    if not surface_root.is_symlink():
        raise BrokenOverlayError(
            f"bootstrap ledger records `{rel}` as a shared overlay surface but it is no longer a "
            f"bootstrap-managed symlink into .workbay/remote. {remediation}"
        )
    if not surface_root.exists():
        raise BrokenOverlayError(
            f"shared overlay surface `{rel}` points to a missing bootstrap-owned target. "
            f"{remediation}"
        )
    clone_root = project_root.joinpath(*_CLONE_SUBDIR).resolve()
    resolved = surface_root.resolve()
    resolved_posix = resolved.as_posix()
    if ".workstate/remote" in resolved_posix:
        raise HalfMaterializedOverlayError(
            f"shared overlay surface `{rel}` resolves through legacy `.workstate/remote` "
            f"(`{resolved}`). {remediation}"
        )
    if resolved != clone_root and not str(resolved).startswith(str(clone_root) + os.sep):
        raise BrokenOverlayError(
            f"shared overlay surface `{rel}` resolves outside the bootstrap clone "
            f"({'/'.join(_CLONE_SUBDIR)}); it points at `{resolved}`. {remediation}"
        )


def _resolve_canonical_surface(kind: SurfaceKind, project_root: Path) -> list[ResolvedPath] | None:
    """Resolve a surface from the canonical bootstrap ledger.

    Returns ``None`` when the ledger records no surface for ``kind`` (e.g.
    ``skills`` / ``commands``), signalling the caller to fall through to
    source-tree/default resolution rather than failing closed.
    """
    ledger_paths = CANONICAL_KIND_LEDGER_PATHS.get(kind)
    if ledger_paths is None:
        return None

    manifest = _load_bootstrap_manifest(project_root)
    _validate_ledger_materialization(project_root, manifest)
    entry_by_path = _ledger_entry_by_path(manifest)

    resolved: list[ResolvedPath] = []
    matched_any = False
    for rel in ledger_paths:
        entry = entry_by_path.get(rel.rstrip("/"))
        if entry is None:
            continue
        matched_any = True
        source = entry.get("source", "shared")
        surface_root = project_root / rel

        if source == "shared":
            _validate_shared_surface(surface_root, project_root=project_root, rel=rel, manifest=manifest)
        # `generated` / `lifecycle` / `local` surfaces are real paths, not clone
        # symlinks: validate by existence only and never raise BrokenOverlayError.

        if kind == "hooks":
            # Scan THIS ledger path's concrete hook root once so its own source
            # is applied to its own files. Routing through _iter_hook_entries
            # would scan both hook roots under one anchor and mis-tag / drop
            # entries when the two ledger paths differ.
            entries = _iter_hook_files(surface_root)
        else:
            entries = _iter_surface_entries(surface_root)

        rp_source: ResolvedSource = "shared" if source == "shared" else "local"
        for path in entries.values():
            resolved.append(
                ResolvedPath(
                    source=rp_source,
                    effective_path=path,
                    shared_path=path if rp_source == "shared" else None,
                    local_path=path if rp_source == "local" else None,
                )
            )

    if not matched_any:
        return None
    return resolved


def _resolve_source_tree(kind: SurfaceKind, project_root: Path) -> list[ResolvedPath]:
    if kind == "hooks":
        return [
            ResolvedPath(source="shared", effective_path=path, shared_path=path)
            for path in _iter_hook_entries(project_root).values()
        ]

    default_root = project_root / DEFAULT_SURFACE_ROOTS[kind]
    if not default_root.exists():
        return []
    return [
        ResolvedPath(source="shared", effective_path=path, shared_path=path)
        for path in _iter_surface_entries(default_root).values()
    ]


def resolve_surface(kind: SurfaceKind, project_root: Path) -> list[ResolvedPath]:
    project_root = project_root.expanduser().resolve()

    mode = detect_overlay_mode(project_root)
    if mode == "canonical":
        canonical = _resolve_canonical_surface(kind, project_root)
        if canonical is not None:
            return canonical
        # kind has no ledger surface (skills/commands) → source-tree fallthrough.
        return _resolve_source_tree(kind, project_root)

    roots = _surface_roots(project_root, kind) if mode == "legacy" else None
    if roots is None:
        return _resolve_source_tree(kind, project_root)

    shared_root, local_root = roots
    if kind == "hooks":
        shared_entries = _iter_hook_entries(_hook_anchor_from_surface_root(shared_root))
        local_entries = _iter_hook_entries(_hook_anchor_from_surface_root(local_root))
    else:
        shared_entries = _iter_surface_entries(shared_root)
        local_entries = _iter_surface_entries(local_root)
    resolved: list[ResolvedPath] = []

    for name in sorted(set(shared_entries) | set(local_entries)):
        local_entry = local_entries.get(name)
        shared_entry = shared_entries.get(name)

        if local_entry is not None:
            _validate_entry(local_entry, project_root=project_root, label="local")
        if shared_entry is not None:
            _validate_entry(shared_entry, project_root=project_root, label="shared")

        if local_entry is not None and shared_entry is not None:
            resolved.append(
                ResolvedPath(
                    source="overlapping",
                    effective_path=local_entry,
                    shared_path=shared_entry,
                    local_path=local_entry,
                )
            )
            continue
        if local_entry is not None:
            resolved.append(ResolvedPath(source="local", effective_path=local_entry, local_path=local_entry))
            continue
        if shared_entry is not None:
            resolved.append(ResolvedPath(source="shared", effective_path=shared_entry, shared_path=shared_entry))

    return resolved
