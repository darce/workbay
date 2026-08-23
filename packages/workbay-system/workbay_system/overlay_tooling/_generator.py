"""Delegate generate-agent-workflows to the payload script."""

from __future__ import annotations

import sys
from importlib.util import module_from_spec, spec_from_file_location

from workbay_system.overlay_tooling._paths import PACKAGE_ROOT


def _generator_script():
    return (
        PACKAGE_ROOT
        / "workbay_system"
        / "payload"
        / "scripts"
        / "generate_agent_workflows.py"
    )


def main(argv: list[str] | None = None) -> int:
    path = _generator_script()
    if not path.is_file():
        print(f"generate-agent-workflows: missing script at {path}", file=sys.stderr)
        return 1
    spec = spec_from_file_location("workbay_generate_agent_workflows", path)
    if spec is None or spec.loader is None:
        print("generate-agent-workflows: failed to load module spec", file=sys.stderr)
        return 1
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    old_argv = sys.argv
    sys.argv = [str(path), *(argv or [])]
    try:
        return int(module.main())
    finally:
        sys.argv = old_argv
