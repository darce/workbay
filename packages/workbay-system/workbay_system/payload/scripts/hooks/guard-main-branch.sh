#!/usr/bin/env bash
# PreToolUse hook: blocks protected edits on the main branch.
#
# Receives tool invocation as JSON on stdin (Claude Code hook protocol).
# Exit 0 = allow, Exit 2 = block (stderr shown to agent as reason).
#
# Policy: only explicitly permitted operator docs/config surfaces may be edited
#         on main. Planning docs and implementation files require a feature branch.

set -euo pipefail

INPUT=$(cat)

# Determine current branch.
BRANCH=$(git branch --show-current 2>/dev/null || echo "")

if [ "$BRANCH" != "main" ] && [ "$BRANCH" != "master" ]; then
  exit 0
fi

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
# internal pattern: the inline Python that used to live here as a
# `python -c '...'` heredoc now lives at scripts/hooks/_guard_main_branch_inline.py.
# Bash quoting (especially apostrophes inside Python comments) cannot break
# the script because there is no heredoc to quote.
if ! BLOCK_REASON=$(printf '%s' "$INPUT" | python3 \
    "${REPO_ROOT}/scripts/hooks/_guard_main_branch_inline.py" \
    "$REPO_ROOT" "$BRANCH"); then
  exit 2
fi

if [ -n "$BLOCK_REASON" ]; then
  printf '%s\n' "$BLOCK_REASON" >&2
  exit 2
fi

# Warning-only rollout: permitted main-branch edits still require a handoff task.
# If none is active, print a maintenance-task reminder but do not block the edit.
# Only query (and warn) when the CLI is actually installed — a missing CLI must
# not masquerade as "no active task" (internal regression).
if [ "${WORKBAY_SKIP_ACTIVE_TASK_PROBE:-0}" = "1" ]; then
  exit 0
fi

if ! command -v mcp-workbay-handoff >/dev/null 2>&1; then
  exit 0
fi

ACTIVE_TASK=$(
  mcp-workbay-handoff --workspace-root "$REPO_ROOT" state --sections identity 2>/dev/null | python3 -c "
import sys, json
try:
    payload = json.load(sys.stdin)
except Exception:
    print('')
    raise SystemExit(0)
data = payload.get('data') if isinstance(payload, dict) else None
active = data.get('active') if isinstance(data, dict) else None
task_ref = active.get('task_ref') if isinstance(active, dict) else ''
print(task_ref or '')
" 2>/dev/null || true
)

if [ -z "$ACTIVE_TASK" ]; then
  cat >&2 <<EOF
WARNING: Editing on $BRANCH without an active handoff task.
  Register a MAINT-* task before continuing.

Register a maintenance task before continuing:
  set_handoff_state(task_ref='MAINT-<slug>', objective='Describe the main-branch patch', status='in_progress', target_branch='main', target_worktree_path='<repo-root>')

Note: for MAINT-* tasks on main/master, target_worktree_path defaults to
the current repo root when omitted. Passing it explicitly is still fine
and wins over the default.

This rollout is warning-only for permitted main-branch edits.
EOF
fi

exit 0
