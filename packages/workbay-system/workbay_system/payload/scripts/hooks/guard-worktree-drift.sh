#!/usr/bin/env bash
# PreToolUse hook: ask before editing in a worktree other than the active task target.

set -euo pipefail

HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/hooks/_resolve_repo_root.sh
if [ -f "${HOOK_DIR}/_resolve_repo_root.sh" ]; then
  . "${HOOK_DIR}/_resolve_repo_root.sh"
else
  # SSOT helper absent (partial/stale overlay materialization): fall back to env
  # roots so a missing file fails OPEN under `set -e` instead of aborting the
  # source and exiting non-zero (a spurious PreToolUse block). No inline git
  # toplevel here — resolving the git root is the SSOT's job, and the drift gate
  # forbids inlining it.
  REPO_ROOT="${CLAUDE_PROJECT_DIR:-${GROK_WORKSPACE_ROOT:-$(pwd)}}"
fi
DRIFT_HANDLER="${REPO_ROOT}/scripts/hooks/_worktree_drift.py"
# Fail OPEN (allow the edit) when the root or handler can't be resolved: a drift
# advisory must never hard-block edits just because git/cwd is unusual. Without
# this, an empty REPO_ROOT would run a bad path and `set -e` would exit non-zero
# (= PreToolUse block).
[ -n "$REPO_ROOT" ] && [ -f "$DRIFT_HANDLER" ] || exit 0
python3 "$DRIFT_HANDLER"
