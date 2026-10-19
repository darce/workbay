#!/usr/bin/env bash
# ci_local.sh — single-driver CI (assessment 2026-08-14, Minimal GitHub Actions shape).
# Runs every gate that .github/workflows/test.yml previously ran as 11 parallel
# jobs, sequentially in one process: one checkout, one Python setup, one install.
# GitHub bills per job-minute rounded up, so 11 jobs pay 11x setup + 11x 1-min
# floor; one job pays it once. Fail-fast by default (a red suite stops the run
# and stops the meter); CI_LOCAL_KEEP_GOING=1 runs all suites and reports all.
# Select a subset with CI_LOCAL_SUITES="brand format protocol" (smoke/local use).
set -uo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-python3}"
KEEP_GOING="${CI_LOCAL_KEEP_GOING:-0}"
SUITES="${CI_LOCAL_SUITES:-brand format protocol bootstrap handoff orchestrator bridge system workbay contract scripts}"

ran=0 failed=0
run_suite() {
  local name="$1"; shift
  case " $SUITES " in *" $name "*) ;; *) return 0 ;; esac
  ran=$((ran+1))
  echo "=== ci-local: $name ==="
  if "$@"; then echo "--- ci-local: $name OK"; else
    echo "--- ci-local: $name FAILED" >&2; failed=$((failed+1))
    [ "$KEEP_GOING" = "1" ] || exit 1
  fi
}

if [ "${CI_LOCAL_INSTALL:-1}" = "1" ]; then
  echo "=== ci-local: install ==="
  # uv-managed venvs (the local workspace .venv) have no pip module; hosted
  # runners do. Use whichever installer the interpreter actually has.
  if "$PY" -m pip --version >/dev/null 2>&1; then
    PIP_INSTALL=("$PY" -m pip install)
    "${PIP_INSTALL[@]}" --upgrade pip || exit 1
  elif command -v uv >/dev/null 2>&1; then
    PIP_INSTALL=(uv pip install --python "$PY")
  else
    "$PY" -m ensurepip --upgrade >/dev/null 2>&1 || { echo "ci-local: no pip or uv for $PY" >&2; exit 1; }
    PIP_INSTALL=("$PY" -m pip install)
  fi
  # Sibling-first --no-deps posture from the old per-job installs: no sibling
  # may be re-resolved from an index into a shadowing non-editable copy.
  pip_install() { "${PIP_INSTALL[@]}" "$@" || exit 1; }
  pip_install --no-deps -e ./packages/workbay-protocol
  pip_install --no-deps -e ./packages/mcp-workbay-handoff
  pip_install -e ./packages/workbay-system
  pip_install -e ./packages/workbay-protocol
  pip_install pytest fastmcp pyyaml 'pydantic>=2.6' tiktoken 'numpy>=2,<3' \
    'pytest-timeout>=2.3' 'hypothesis>=6' build tomlkit packaging 'ruff==0.15.17'
  pip_install --no-deps -e ./packages/mcp-workbay-orchestrator
  pip_install --no-deps -e ./packages/workbay-bootstrap
  pip_install --no-deps -e ./packages/workbay-codex-bridge
  pip_install --no-deps -e ./packages/workbay
fi

run_suite brand  "$PY" scripts/check_brand.py
run_suite format make format-check
run_suite protocol "$PY" -m pytest packages/workbay-protocol/tests -q
run_suite bootstrap "$PY" -m pytest packages/workbay-bootstrap/tests -q
run_suite handoff "$PY" -m pytest packages/mcp-workbay-handoff/tests -q
run_suite orchestrator "$PY" -m pytest packages/mcp-workbay-orchestrator/tests -q
run_suite bridge "$PY" -m pytest packages/workbay-codex-bridge/tests -q
run_suite system "$PY" -m pytest packages/workbay-system/tests -q
# Advisory only, as in the old workbay-system job: the sole remediation is a
# manual destructive prune that cannot run in CI, so it must never red the run.
if [ "$failed" -eq 0 ] || [ "$KEEP_GOING" = "1" ]; then
  case " $SUITES " in *" system "*) "$PY" scripts/release_public.py audit-tags || echo "ci-local: stale-tag audit advisory failure (non-blocking)" ;; esac
fi
run_suite workbay "$PY" -m pytest packages/workbay/tests -q
run_suite contract "$PY" -m pytest packages/mcp-workbay-orchestrator/tests/test_protocol_contract.py -q
run_suite scripts "$PY" -m pytest scripts/tests -m "not integration" -q

echo "ci-local: ran=$ran failed=$failed"
[ "$ran" -gt 0 ] || { echo "ci-local: no suites selected" >&2; exit 2; }
[ "$failed" -eq 0 ]
