#!/usr/bin/env bash
# FXGATE-VENVREAP-01 — assert a package-scoped gate interpreter is live.
#
# When a worktree .venv is reaped mid-gate, bare ``python -m pytest`` yields
# exit 127 (or a cascade of false test failures under a half-dead env). That
# looks like a code regression. This helper fails first with a greppable named
# condition so operators can distinguish "interpreter vanished" from "tests
# failed" without decoding exit codes.
#
# Usage: assert_gate_interpreter.sh <interpreter-path> <package-or-target>
# Exit:  0 when the path is a regular file present and executable as an
#        interpreter; 78 (EX_CONFIG-class harness fault) with
#        GATE_INTERPRETER_VANISHED on stderr otherwise.
set -euo pipefail

INTERPRETER="${1:-}"
PACKAGE="${2:-}"

if [ -z "$INTERPRETER" ] || [ -z "$PACKAGE" ]; then
  echo "assert_gate_interpreter: usage: $0 <interpreter-path> <package-or-target>" >&2
  exit 78
fi

# Require a regular file: an executable directory satisfies -x but is not a
# runnable interpreter (FXGATE-VENVREAP-01).
if [ ! -f "$INTERPRETER" ] || [ ! -x "$INTERPRETER" ]; then
  # Named condition — greppable; must not be confusable with a pytest failure.
  echo "GATE_INTERPRETER_VANISHED: package=${PACKAGE} interpreter=${INTERPRETER} — not present or not executable (FXGATE-VENVREAP-01)" >&2
  exit 78
fi

exit 0
