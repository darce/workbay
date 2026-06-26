#!/usr/bin/env bash
# PreToolUse hook: ask before editing in a worktree other than the active task target.

set -euo pipefail

python3 "$CLAUDE_PROJECT_DIR/scripts/hooks/_worktree_drift.py"
