"""Resolve the managed MCP server map from required + optional catalog entries.

Optional servers are opt-in via an explicit flag ONLY. Config resolution is a
pure function of the manifest-derived pins plus the caller's flag set — it never
probes the host, so ``install`` and ``mcp-sync`` render byte-identical surfaces
regardless of what is installed on any particular machine (the render seam stays
deterministic so the drift audit is meaningful). The launch-time capability
probe lives solely in the ``mcp_launch.py`` shim, which soft-degrades when the
binary is absent.
"""

from __future__ import annotations

from typing import Any


def resolve_managed_mcp_servers(
    active_flags: frozenset[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Merge required DEFAULT_MCP_SERVERS with flag-opted-in optional entries."""
    from workbay_bootstrap._mcp_pins import DEFAULT_MCP_SERVERS, OPTIONAL_MCP_SERVERS

    resolved: dict[str, dict[str, Any]] = {
        name: dict(spec) for name, spec in DEFAULT_MCP_SERVERS.items()
    }
    flags = active_flags or frozenset()
    for name, spec in OPTIONAL_MCP_SERVERS.items():
        meta = spec.get("_meta") or {}
        opt_in_flag = meta.get("opt_in_flag")
        if opt_in_flag and opt_in_flag in flags:
            resolved[name] = {k: v for k, v in spec.items() if k != "_meta"}
    return resolved
