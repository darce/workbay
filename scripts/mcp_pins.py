#!/usr/bin/env python3
"""Generate workbay-bootstrap's MCP pins module from mcp_servers.yaml.

``packages/workbay-system/workbay_system/payload/config/agent-workflows/
mcp_servers.yaml`` is the single canonical MCP registration manifest. The
bootstrap-side launch specs (``DEFAULT_MCP_SERVERS``) and the per-harness
registration ownership table (``MCP_REGISTRATION``) are **generated** into
``packages/workbay-bootstrap/src/workbay_bootstrap/_mcp_pins.py`` so the
wheel stays standalone-installable with no runtime dependency on the
manifest, while the data can no longer drift from it by hand-editing.

Subcommands (mirrors scripts/stack_pins.py):

* ``sync``  — regenerate ``_mcp_pins.py`` from the manifest.
* ``check`` — exit non-zero when ``_mcp_pins.py`` differs from a fresh
  render (CI / preflight gate; wired as ``make mcp-pins-check``).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = (
    REPO_ROOT
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "config"
    / "agent-workflows"
    / "mcp_servers.yaml"
)
PINS_MODULE = (
    REPO_ROOT
    / "packages"
    / "workbay-bootstrap"
    / "src"
    / "workbay_bootstrap"
    / "_mcp_pins.py"
)

_HEADER = '''"""GENERATED MODULE — DO NOT EDIT.

Rendered by ``scripts/mcp_pins.py sync`` from the canonical MCP
registration manifest:

    packages/workbay-system/workbay_system/payload/config/
    agent-workflows/mcp_servers.yaml

``make mcp-pins-check`` fails the build when this file drifts from a
fresh render. Edit the manifest, not this module.
"""

from __future__ import annotations

from typing import Any

'''


def _literal(value: object, indent: int = 0) -> str:
    """Deterministic, insertion-ordered Python literal rendering."""
    pad = " " * indent
    inner = " " * (indent + 4)
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = ",\n".join(
            f"{inner}{_literal(key)}: {_literal(val, indent + 4)}"
            for key, val in value.items()
        )
        return "{\n" + items + ",\n" + pad + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        items = ",\n".join(f"{inner}{_literal(item, indent + 4)}" for item in value)
        return "[\n" + items + ",\n" + pad + "]"
    if isinstance(value, str):
        # json.dumps gives double-quoted strings, keeping the rendered
        # module byte-stable under `ruff format`.
        return json.dumps(value)
    return repr(value)


def render(manifest_path: Path = MANIFEST) -> str:
    payload = yaml.safe_load(manifest_path.read_text())
    if payload.get("version") != 2:
        raise SystemExit(
            f"{manifest_path}: expected version=2; found {payload.get('version')!r}"
        )
    registration = payload["registration"]
    servers: dict[str, dict[str, object]] = {}
    for server in payload["mcp_servers"]:
        entry: dict[str, object] = {
            "type": "stdio",
            "command": server["command"],
            "args": list(server.get("args", [])),
        }
        env = server.get("env")
        if isinstance(env, dict) and env:
            entry["env"] = dict(env)
        servers[server["name"]] = entry

    lines = [
        _HEADER,
        "# Per-harness MCP registration ownership (harness -> 'root' | 'plugin').",
        "# 'root' harnesses get their servers from the bootstrap-written root",
        "# surfaces; 'plugin' harnesses get them from the emitted plugin tree.",
        f"MCP_REGISTRATION: dict[str, str] = {_literal(dict(registration))}",
        "",
        "# Managed-server launch specs (bootstrap `type: stdio` shape).",
        f"DEFAULT_MCP_SERVERS: dict[str, dict[str, Any]] = {_literal(servers)}",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("sync", "check"))
    args = parser.parse_args(argv)

    rendered = render()
    current = PINS_MODULE.read_text() if PINS_MODULE.exists() else None
    if args.command == "sync":
        if current == rendered:
            print(f"mcp-pins: {PINS_MODULE.relative_to(REPO_ROOT)} already in sync")
            return 0
        PINS_MODULE.write_text(rendered)
        print(f"mcp-pins: wrote {PINS_MODULE.relative_to(REPO_ROOT)}")
        return 0
    if current != rendered:
        print(
            f"mcp-pins: {PINS_MODULE.relative_to(REPO_ROOT)} drifts from "
            f"{MANIFEST.relative_to(REPO_ROOT)}; run `make mcp-pins-sync`",
            file=sys.stderr,
        )
        return 1
    print("mcp-pins: in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
