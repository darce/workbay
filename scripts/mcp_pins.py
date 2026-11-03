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


def _launch_entry(server: dict) -> dict[str, object]:
    """Bootstrap ``type: stdio`` launch spec for one manifest server entry."""
    entry: dict[str, object] = {
        "type": "stdio",
        "command": server["command"],
        "args": list(server.get("args", [])),
    }
    env = server.get("env")
    if isinstance(env, dict) and env:
        entry["env"] = dict(env)
    return entry


def render(manifest_path: Path = MANIFEST) -> str:
    payload = yaml.safe_load(manifest_path.read_text())
    if payload.get("version") != 2:
        raise SystemExit(
            f"{manifest_path}: expected version=2; found {payload.get('version')!r}"
        )
    registration = payload["registration"]
    servers: dict[str, dict[str, object]] = {}
    for server in payload["mcp_servers"]:
        servers[server["name"]] = _launch_entry(server)
    # Optional servers live under a distinct manifest key so pre-opt-in
    # consumers exclude them by default (forward-compatible). They carry only
    # flag metadata — the capability probe + binary name stay in the launch
    # shim, never in the manifest or the generated pins (§6).
    optional_servers: dict[str, dict[str, object]] = {}
    for server in payload.get("optional_mcp_servers", []):
        entry = _launch_entry(server)
        meta: dict[str, object] = {"optional": True}
        opt_in_flag = server.get("opt_in_flag")
        if isinstance(opt_in_flag, str) and opt_in_flag:
            meta["opt_in_flag"] = opt_in_flag
        entry["_meta"] = meta
        optional_servers[server["name"]] = entry

    lines = [
        _HEADER,
        "# Per-harness MCP registration ownership (harness -> 'root' | 'plugin').",
        "# 'root' harnesses get their servers from the bootstrap-written root",
        "# surfaces; 'plugin' harnesses get them from the emitted plugin tree.",
        f"MCP_REGISTRATION: dict[str, str] = {_literal(dict(registration))}",
        "",
        "# Managed-server launch specs (bootstrap `type: stdio` shape).",
        "# Required servers only — optional entries live in OPTIONAL_MCP_SERVERS.",
        f"DEFAULT_MCP_SERVERS: dict[str, dict[str, Any]] = {_literal(servers)}",
        "",
        "# Opt-in managed servers (materialized when flag or probe passes).",
        f"OPTIONAL_MCP_SERVERS: dict[str, dict[str, Any]] = {_literal(optional_servers)}",
        "",
    ]
    return "\n".join(lines)


def _validate_optional_metadata(manifest_path: Path = MANIFEST) -> list[str]:
    """Return human-readable errors when optional servers lack required fields.

    Optional servers are opt-in via ``opt_in_flag`` only; the launch-time
    capability probe and binary name live in the shim, never the manifest (§6),
    so they are intentionally not required (or permitted) here.
    """
    payload = yaml.safe_load(manifest_path.read_text())
    errors: list[str] = []
    for server in payload.get("optional_mcp_servers", []):
        name = server.get("name", "<unnamed>")
        if not server.get("opt_in_flag"):
            errors.append(f"{name}: optional server missing opt_in_flag")
        if server.get("capability_probe"):
            errors.append(
                f"{name}: capability_probe must not appear in the manifest; "
                "the probe + binary name belong in the launch shim only (§6)"
            )
    return errors


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
    optional_errors = _validate_optional_metadata()
    if optional_errors:
        for err in optional_errors:
            print(f"mcp-pins: {err}", file=sys.stderr)
        return 1
    print("mcp-pins: in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
