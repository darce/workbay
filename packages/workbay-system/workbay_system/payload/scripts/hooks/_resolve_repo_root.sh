#!/usr/bin/env bash
# Harness-neutral workspace-root resolution (fail-open).
# Precedence: git toplevel -> CLAUDE_PROJECT_DIR -> GROK_WORKSPACE_ROOT -> pwd.
# Source from sibling hook scripts:
#   HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   # shellcheck source=scripts/hooks/_resolve_repo_root.sh
#   . "${HOOK_DIR}/_resolve_repo_root.sh"

REPO_ROOT=""
if root="$(git rev-parse --show-toplevel 2>/dev/null)" && [ -n "$root" ]; then
  REPO_ROOT="$root"
elif [ -n "${CLAUDE_PROJECT_DIR:-}" ]; then
  REPO_ROOT="$CLAUDE_PROJECT_DIR"
elif [ -n "${GROK_WORKSPACE_ROOT:-}" ]; then
  REPO_ROOT="$GROK_WORKSPACE_ROOT"
else
  REPO_ROOT="$(pwd)"
fi
