#!/usr/bin/env bash
set +e

HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/hooks/_resolve_repo_root.sh
. "${HOOK_DIR}/_resolve_repo_root.sh"
WORKSPACE_ROOT="$REPO_ROOT"
HOOK_PAYLOAD="$(cat)"

if ! SHOULD_RUN="$(
  HOOK_PAYLOAD="$HOOK_PAYLOAD" python3 - <<'PY'
import json
import os

payload_raw = os.environ.get("HOOK_PAYLOAD", "")
try:
    payload = json.loads(payload_raw) if payload_raw else {}
except json.JSONDecodeError:
    print("true")
    raise SystemExit(0)

tool_name = payload.get("tool_name") or payload.get("toolName") or ""
tool_input = payload.get("tool_input") or payload.get("toolInput") or {}


def _review_operation(ti):
    review = ti.get("review")
    if isinstance(review, str):
        try:
            review = json.loads(review)
        except json.JSONDecodeError:
            return None
    if not isinstance(review, dict):
        return None
    return review.get("operation")


if "record_event" in tool_name:
    print("true")
elif "set_handoff_state" in tool_name:
    print("true")
elif "review_findings" in tool_name:
    operation = _review_operation(tool_input)
    print("true" if operation not in {"list", "get"} else "false")
elif "review_runs" in tool_name:
    operation = _review_operation(tool_input)
    print("true" if operation not in {"list", "coverage"} else "false")
else:
    print("false")
PY
)"; then
  SHOULD_RUN="true"
fi

if [ "$SHOULD_RUN" != "true" ]; then
  exit 0
fi

# Refresh the dashboard via the stdlib MCP launcher shim (implementation note / Dist-1
# git-only). The shim execs the git-installed `uv tool` console or the workspace
# `.venv` console — never a per-session `uvx` PyPI resolve — and forwards these
# non-default args so the console runs `render-handoff` instead of booting the
# stdio server. The old manifest-pin grep is gone: Dist-1 stripped the `@<pin>`
# from mcp_servers.yaml, so the pin is no longer resolvable there.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM="$SCRIPT_DIR/mcp_launch.py"

if [ ! -f "$SHIM" ]; then
  # A consumer overlay that materialized this helper without the shim:
  # this hook is a best-effort dashboard refresher, so degrade to a no-op.
  exit 0
fi

python3 "$SHIM" workbay-handoff-mcp --workspace-root "$WORKSPACE_ROOT" \
  render-handoff --kind dashboard >/dev/null 2>&1 || true

exit 0
