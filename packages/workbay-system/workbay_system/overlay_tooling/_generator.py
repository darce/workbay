"""Delegate generate-agent-workflows to the payload script."""

from __future__ import annotations

import argparse
import shutil
import subprocess
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
    # Bootstrap depends on system; keep validation across that boundary in a
    # subprocess rather than introducing the reverse package dependency.
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--plugin-overrides")
    options, _ = parser.parse_known_args(argv or [])
    if options.plugin_overrides is not None:
        validator = shutil.which("workbay-bootstrap")
        if validator is None:
            print(
                "generate-agent-workflows: override validation skipped (workbay-bootstrap executable not found).",
                file=sys.stderr,
            )
        else:
            try:
                result = subprocess.run(
                    [validator, "validate-overrides", "--override-root", options.plugin_overrides],
                    check=False,
                    timeout=30,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                print(f"generate-agent-workflows: override validation failed: {exc}", file=sys.stderr)
                return 1
            if result.returncode != 0:
                return result.returncode

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
