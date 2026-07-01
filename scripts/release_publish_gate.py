#!/usr/bin/env python3
"""Publish-gate state check for ``release-publish.yml`` (implementation note S4b).

Extracted from an inline workflow heredoc so the accepted-state set is
unit-testable and cannot silently drift. A package version may proceed to CI
Trusted-Publishing only when its release-plan state means "this version is not
on PyPI and is safe to publish":

  - ``pending_upload``          — the tag-after-publish flow, and
  - ``remote_tag_without_pypi`` — the natural state when the tag-driven
    ``release-public`` flow force-syncs the package tag BEFORE CI publishes
    (the 155989a hot-fix taught the gate to accept this).

``released`` (already on PyPI) and every other state fail-close.

Usage (from the workflow):
    python scripts/release_publish_gate.py --plan release-plan.json --package <name>

On success it writes ``version=<v>`` to ``$GITHUB_OUTPUT`` (when set) and stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Both states mean "this version is absent from PyPI and is safe to publish".
# Keep this set in lockstep with scripts/release.sh's release_state machine;
# tests/release/test_release_publish_gate.py locks it against drift.
PUBLISHABLE_STATES = frozenset({"pending_upload", "remote_tag_without_pypi"})


def select_version(plan: dict, package_name: str) -> str:
    """Return the package's plan version, or raise SystemExit if not publishable.

    Raises when the package is absent from the plan or its state is not in
    ``PUBLISHABLE_STATES`` (e.g. ``released`` — already on PyPI).
    """
    package = next(
        (entry for entry in plan["packages"] if entry["name"] == package_name),
        None,
    )
    if package is None:
        raise SystemExit(f"{package_name} is not present in release-plan.json")
    state = package["state"]
    if state not in PUBLISHABLE_STATES:
        raise SystemExit(
            f"{package_name} is in state {state}; expected one of "
            f"{sorted(PUBLISHABLE_STATES)} (version absent from PyPI) before CI publish"
        )
    return str(package["version"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="release-publish-gate")
    parser.add_argument("--plan", required=True, help="path to release-plan.json")
    parser.add_argument("--package", required=True, help="package name to validate")
    args = parser.parse_args(argv)

    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    version = select_version(plan, args.package)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(f"version={version}\n")
    print(f"version={version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
