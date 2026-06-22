#!/usr/bin/env bash
set +e

WORKSPACE_ROOT="${CLAUDE_PROJECT_DIR:-$(pwd)}"
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

# Resolve the pinned handoff CLI spec from the single-source MCP manifest
# (config/agent-workflows/mcp_servers.yaml) and launch via `uvx`, exactly
# like the manifest's server entry. Reading the pin at runtime means this
# hook can never drift from the version the live MCP server runs, and it
# avoids calling a bare PATH binary that would resolve to whatever pyenv
# shim happens to be installed (the source of the version drift this hook
# was carrying, plus the pre-v0.2.0 CLI name this hook used to invoke,
# which was renamed and no longer exists on PATH).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MCP_MANIFEST="$SCRIPT_DIR/../../config/agent-workflows/mcp_servers.yaml"
HANDOFF_PIN="$(grep -oE 'mcp-workbay-handoff@[0-9][0-9A-Za-z.-]*' "$MCP_MANIFEST" 2>/dev/null | head -n1)"

if [ -z "$HANDOFF_PIN" ]; then
  # No manifest pin resolvable (e.g. a consumer overlay that materialized
  # this fallback helper without the manifest). This hook is a best-effort
  # dashboard refresher, so degrade to a no-op rather than guessing an
  # unpinned binary that could silently drift.
  exit 0
fi

uvx "$HANDOFF_PIN" --workspace-root "$WORKSPACE_ROOT" render-handoff --kind dashboard >/dev/null 2>&1 || true

exit 0
