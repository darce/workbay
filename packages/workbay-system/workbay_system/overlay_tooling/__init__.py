"""Importable overlay validator port (internal)."""

from workbay_system.overlay_tooling.overlay_resolver import (
    BOOTSTRAP_MANIFEST_NAME,
    BrokenOverlayError,
    HalfMaterializedOverlayError,
    LEGACY_OVERLAY_MANIFEST_NAME,
    OverlayMode,
    OverlayResolverError,
    ResolvedPath,
    ResolvedSource,
    SurfaceKind,
    detect_overlay_mode,
    resolve_surface,
)

__all__ = [
    "BOOTSTRAP_MANIFEST_NAME",
    "BrokenOverlayError",
    "HalfMaterializedOverlayError",
    "LEGACY_OVERLAY_MANIFEST_NAME",
    "OverlayMode",
    "OverlayResolverError",
    "ResolvedPath",
    "ResolvedSource",
    "SurfaceKind",
    "detect_overlay_mode",
    "resolve_surface",
]
