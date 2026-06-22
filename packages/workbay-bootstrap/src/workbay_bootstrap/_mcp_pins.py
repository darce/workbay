"""GENERATED MODULE — DO NOT EDIT.

Rendered by ``scripts/mcp_pins.py sync`` from the canonical MCP
registration manifest:

    packages/workbay-system/workbay_system/payload/config/
    agent-workflows/mcp_servers.yaml

``make mcp-pins-check`` fails the build when this file drifts from a
fresh render. Edit the manifest, not this module.
"""

from __future__ import annotations

from typing import Any


# Per-harness MCP registration ownership (harness -> 'root' | 'plugin').
# 'root' harnesses get their servers from the bootstrap-written root
# surfaces; 'plugin' harnesses get them from the emitted plugin tree.
MCP_REGISTRATION: dict[str, str] = {
    "claude": "root",
    "codex": "root",
    "cursor": "root",
    "vscode": "root",
    "grok": "root",
}

# Managed-server launch specs (bootstrap `type: stdio` shape).
DEFAULT_MCP_SERVERS: dict[str, dict[str, Any]] = {
    "workbay-handoff-mcp": {
        "type": "stdio",
        "command": "uvx",
        "args": [
            "mcp-workbay-handoff@0.1.0",
            "--workspace-root",
            ".",
            "serve-stdio",
        ],
    },
    "workbay-orchestrator-mcp": {
        "type": "stdio",
        "command": "uvx",
        "args": [
            "mcp-workbay-orchestrator[bridge]@0.1.0",
            "--workspace-root",
            ".",
            "serve-stdio",
        ],
    },
}
