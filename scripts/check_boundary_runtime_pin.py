#!/usr/bin/env python3
"""Check the stored consumer boundary pins against the orchestrator release.

Consumers lack the source pyproject, so release_prepare regenerates these stored
values and release_public verifies them before exporting a release.
"""
from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path

PIN_PATHS = (
    "scripts/remote_agent.sh",
    "packages/workbay-system/workbay_system/payload/scripts/remote_agent.sh",
)
# Only a single, top-level literal assignment is authoritative. Quotes must pair.
PIN_LINE = re.compile(r"^_BOUNDARY_RUNTIME_VERSION=(?:([0-9][A-Za-z0-9.+-]*)|'([0-9][A-Za-z0-9.+-]*)'|\"([0-9][A-Za-z0-9.+-]*)\")$")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    repo = parser.parse_args().repo
    expected = None
    try:
        with (repo / "packages/mcp-workbay-orchestrator/pyproject.toml").open("rb") as stream:
            expected = tomllib.load(stream)["project"]["version"]
    except (OSError, ValueError, KeyError, TypeError):
        pass
    pins = {}
    for name in PIN_PATHS:
        try:
            lines = [line for line in (repo / name).read_text(encoding="utf-8").splitlines()
                     if PIN_LINE.fullmatch(line)]
        except (OSError, UnicodeError):
            lines = []
        match = PIN_LINE.fullmatch(lines[0]) if len(lines) == 1 else None
        pins[name] = next((value for value in match.groups() if value is not None), None) if match else None
    if not isinstance(expected, str) or not expected or any(pin != expected for pin in pins.values()):
        print(json.dumps({"refused": "boundary_runtime_pin_stale", "expected": expected, "pins": pins}))
        return 1
    print(json.dumps({"ok": True, "version": expected}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
