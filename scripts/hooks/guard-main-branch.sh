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

HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/hooks/_resolve_repo_root.sh
if [ -f "${HOOK_DIR}/_resolve_repo_root.sh" ]; then
  . "${HOOK_DIR}/_resolve_repo_root.sh"
else
  # SSOT helper absent (partial/stale overlay materialization): fall back to env
  # roots. Without this guard, a missing source under `set -e` aborts with a
  # non-zero exit BEFORE the protected-path check below — and a non-2 exit is a
  # non-blocking hook error, so the protected main edit would proceed (guard
  # BYPASS). The env fallback keeps the inline guard running so it still blocks.
  REPO_ROOT="${CLAUDE_PROJECT_DIR:-${GROK_WORKSPACE_ROOT:-$(pwd)}}"
fi
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
# Resolve in-process via the in-tree shared module (same pattern as
# guard-worktree-drift) — never via a PATH-installed console script, whose
# failure was previously swallowed and reported as "no active task".
if [ "${WORKBAY_SKIP_ACTIVE_TASK_PROBE:-0}" = "1" ]; then
  exit 0
fi

# Capture stdout and exit status separately. A non-zero probe exit is
# could-not-determine, never a clean negative [OBS-08][AGT-10]. Under
# set -e, assign via `|| PROBE_STATUS=$?` so a raising probe does not
# abort the advisory hook; do not collapse failure into empty success.
# Failure reasons go to stderr (one line) so operators can see why [OBS-01][RES-03].
PROBE_STATUS=0
PROBE_OUT=$(
  python3 - "$HOOK_DIR" "$REPO_ROOT" <<'PY'
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
try:
    from _active_task_context import _load_active_task

    ctx = _load_active_task(Path(sys.argv[2]))
except Exception as e:
    # Bounded one-line reason; no stack trace on the hot path [OBS-01].
    sys.stderr.write("active-task probe failed: %s\n" % (e,))
    sys.exit(1)

if ctx.probe_error:
    sys.stderr.write("active-task probe failed: %s\n" % (ctx.probe_error,))
    print("PROBE_ERROR")
elif ctx.task_ref:
    print(ctx.task_ref)
else:
    print("")
PY
) || PROBE_STATUS=$?

# A failed probe must not be phrased as "without an active handoff task".
if [ "$PROBE_STATUS" -ne 0 ] || [ "$PROBE_OUT" = "PROBE_ERROR" ]; then
  exit 0
fi

ACTIVE_TASK="$PROBE_OUT"

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
