#!/usr/bin/env bash
# handoff_write.sh — envelope-safe wrapper for handoff CLI writes.
#
# Why this exists (observed 2026-07-26):
#   1. The handoff CLI exits 0 even when the JSON envelope says "ok": false.
#      Shell callers that check $? therefore treat a refused write as success.
#      The envelope is the only truth; this wrapper turns ok:false into a
#      nonzero exit so $? matches the write outcome.
#   2. Operators often filter stdout after merging streams (2>&1), which
#      mangles the only evidence the write produced. The real CLI already
#      puts warnings on stderr and clean JSON on stdout — this wrapper keeps
#      those streams separate and never filters or reformats stdout.
#
# Usage: scripts/handoff_write.sh <cli-subcommand> [args...]
# Override the CLI with HANDOFF_CLI (a single executable; used by the hermetic
# test suite). Default: .venv/bin/python -m workbay_handoff_mcp.cli

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

stdout_file="$(mktemp)"
stderr_file="$(mktemp)"
cleanup() {
  rm -f "$stdout_file" "$stderr_file"
}
trap cleanup EXIT

# Capture streams so we can re-emit them verbatim and still adjudicate the
# envelope. Capture the CLI exit code without aborting under set -e.
cli_rc=0
if [[ -n "${HANDOFF_CLI:-}" ]]; then
  # HANDOFF_CLI is a single executable (argv[0]); do not word-split it.
  "$HANDOFF_CLI" "$@" >"$stdout_file" 2>"$stderr_file" || cli_rc=$?
else
  "${REPO_ROOT}/.venv/bin/python" -m workbay_handoff_mcp.cli "$@" \
    >"$stdout_file" 2>"$stderr_file" || cli_rc=$?
fi

# Forward streams unchanged — never merge (no 2>&1), never filter stdout.
cat "$stdout_file"
cat "$stderr_file" >&2

# A crashing CLI (nonzero, often no envelope) stays a failure.
if [[ "$cli_rc" -ne 0 ]]; then
  exit "$cli_rc"
fi

# Adjudicate the top-level "ok" key with python3. Regex/string matching on
# JSON is unsafe (nested "ok": false strings must not drive the verdict).
python3 -c '
import json
import sys

raw = sys.stdin.read()
try:
    data = json.loads(raw)
except json.JSONDecodeError:
    sys.exit(1)
if not isinstance(data, dict):
    sys.exit(1)
if "ok" not in data:
    sys.exit(1)
if data["ok"] is True:
    sys.exit(0)
sys.exit(1)
' <"$stdout_file"
