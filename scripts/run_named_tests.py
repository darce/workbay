"""Run explicitly named pytest files for the remote gate.

Handoff-package tests keep their own ``test-handoff`` target and are not routed
through this entrypoint.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

_ENTRY_CHARACTERS = re.compile(r"[A-Za-z0-9_./:-]+")


def _refuse(reason: str) -> int:
    print(f"test-files: refused {reason}", file=sys.stderr)
    return 64


def _validate_entry(entry: str, repo_root: Path) -> str | None:
    if not _ENTRY_CHARACTERS.fullmatch(entry):
        return "invalid entry"

    parts = entry.split("::")
    if any(not part for part in parts):
        return "invalid entry"

    relative_path = Path(parts[0])
    if relative_path.is_absolute() or ".." in relative_path.parts:
        return "invalid entry"

    try:
        resolved_path = (repo_root / relative_path).resolve(strict=True)
    except (OSError, RuntimeError):
        return "file does not exist"

    try:
        resolved_path.relative_to(repo_root)
    except ValueError:
        return "invalid entry"

    if not resolved_path.is_file():
        return "file does not exist"

    return None


def main(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    """Run the validated file list in WORKBAY_TEST_FILES and return pytest's status."""
    del argv  # The entrypoint has no command-line options; files come from its environment.
    environment = os.environ if environ is None else environ
    requested = environment.get("WORKBAY_TEST_FILES")
    if not isinstance(requested, str):
        return _refuse("missing WORKBAY_TEST_FILES")

    entries = [entry.strip() for entry in requested.split(",") if entry.strip()]
    if not entries:
        return _refuse("missing WORKBAY_TEST_FILES")

    repo_root = Path(__file__).resolve().parent.parent
    for entry in entries:
        reason = _validate_entry(entry, repo_root)
        if reason is not None:
            return _refuse(reason)

    pytest_argv = [sys.executable, "-m", "pytest", *entries, "-q", "-p", "no:cacheprovider"]
    try:
        __import__("xdist")
    except ImportError:
        pass
    else:
        pytest_argv.extend(["-n", "0"])

    result = subprocess.run(pytest_argv, cwd=repo_root, shell=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
