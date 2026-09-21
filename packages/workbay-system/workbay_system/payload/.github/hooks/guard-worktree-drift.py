#!/usr/bin/env python3
"""VS Code PreToolUse wrapper for the shared worktree-drift hook."""

from __future__ import annotations

import sys
from pathlib import Path


HELPER_DIR = Path(__file__).resolve().parents[2] / "scripts" / "hooks"
if str(HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(HELPER_DIR))

from _worktree_drift import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
