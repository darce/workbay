"""Per-harness plugin activation dispatcher (internal).

Public API delegates to :mod:`workbay_bootstrap.harnesses` (implementation note S3).
Grok CLI helpers are re-exported here so existing tests can patch this module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from workbay_bootstrap.harnesses import (
    GROK_BARE_SELECTOR,
    GROK_PLUGIN_DEST,
    detect_stale_grok_discovery_selectors,
    grok_bare_selector_enabled,
    grok_dest_is_unmanaged_dir,
    grok_plugin_surface_problems,
    grok_surface_is_foreign_local,
    load_harness_protocol,
    materialize_grok_plugin_symlink,
    plugin_activation_row,
    write_plugin_activation as _write_plugin_activation,
)
from workbay_bootstrap.harnesses import (
    _grok_cli_available,
    _run_grok_cli,
)
from workbay_bootstrap.harnesses import activate_grok_plugin as _activate_grok_plugin

__all__ = [
    "GROK_BARE_SELECTOR",
    "GROK_PLUGIN_DEST",
    "activate_grok_plugin",
    "detect_stale_grok_discovery_selectors",
    "grok_bare_selector_enabled",
    "grok_dest_is_unmanaged_dir",
    "grok_plugin_surface_problems",
    "grok_surface_is_foreign_local",
    "load_harness_protocol",
    "materialize_grok_plugin_symlink",
    "plugin_activation_row",
    "write_plugin_activation",
]


def activate_grok_plugin(target: Path) -> dict[str, str]:
    """Shim: tests patch ``activation._grok_cli_available`` / ``_run_grok_cli``."""
    # Re-bind harness helpers to this module's names so unittest.mock patches apply.
    import workbay_bootstrap.harnesses as harnesses

    harnesses._grok_cli_available = _grok_cli_available
    harnesses._run_grok_cli = _run_grok_cli
    return _activate_grok_plugin(target)


def write_plugin_activation(
    harness: str,
    target: Path,
    *,
    clone: Path,
) -> dict[str, str]:
    return _write_plugin_activation(harness, target, clone=clone)