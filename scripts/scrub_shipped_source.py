#!/usr/bin/env python3
"""Report shipped-source files that would change under the shared scrub transform.

The in-place ``--apply`` mutator was removed in implementation note. Scrubbing now
happens at wheel/sdist build time via ``packages/workbay-system/hatch_build.py``.
This entrypoint remains as a report-only helper for operators.

Usage:
  python scripts/scrub_shipped_source.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from _scrub_core import scrub_text
from check_shipped_privacy import (
    REPO_ROOT,
    _iter_shipped_files,
    _publishable_packages,
    _shipped_roots,
)


def main() -> int:
    if "--apply" in sys.argv[1:]:
        print(
            "scrub_shipped_source.py --apply was removed; scrubbing happens at "
            "wheel/sdist build time via hatch_build.py",
            file=sys.stderr,
        )
        return 2

    changed: list[str] = []
    for pkg in _publishable_packages():
        package_dir = REPO_ROOT / str(pkg["path"])
        for root in _shipped_roots(package_dir):
            for path in _iter_shipped_files(root):
                try:
                    text = path.read_text(encoding="utf-8")
                except (UnicodeDecodeError, OSError):
                    continue
                if scrub_text(text) != text:
                    changed.append(path.relative_to(REPO_ROOT).as_posix())

    print(f"would scrub {len(changed)} shipped source file(s)")
    for rel in changed:
        print(f"  {rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())