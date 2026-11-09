#!/usr/bin/env bash
# ci_local.sh — local/hosted transport of the ONE gate manifest (scripts/gate_manifest.txt).
# CI_LOCAL_STAGE selects local (default) or hosted rows; no stage list means both.
# Canon v0.26.2: [REF-45] one authored source for the gate list (this file does
# NOT author suites; it consumes the manifest like ci_report.sh and test.yml);
# [TEST-24] commit-stage checks stay local/short — select a fast subset with
# CI_LOCAL_SUITES="brand-check format-check". Each transport gates its selected
# manifest column. Fail-fast by default; CI_LOCAL_KEEP_GOING=1 runs all.
set -uo pipefail
cd "$(dirname "$0")/.."
MANIFEST="scripts/gate_manifest.txt"
[ -f "$MANIFEST" ] || { echo "ci-local: missing $MANIFEST" >&2; exit 2; }

STAGE="${CI_LOCAL_STAGE:-local}"
case "$STAGE" in
  local|hosted) ;;
  *) echo "ci-local: CI_LOCAL_STAGE must be local or hosted (got '$STAGE')" >&2; exit 2 ;;
esac

PY="${PYTHON:-.venv/bin/python}"
# A bare name (python3) resolves on PATH; only path-like values are
# absolutized (sub-makes via make -C need an absolute interpreter).
case "$PY" in
  */*) case "$PY" in /*) ;; *) PY="$(pwd)/$PY" ;; esac ;;
  *) _resolved="$(command -v "$PY" || true)"; [ -n "$_resolved" ] || { echo "ci-local: interpreter '$PY' not found on PATH" >&2; exit 1; }; case "$_resolved" in /*) PY="$_resolved" ;; *) PY="$(pwd)/$_resolved" ;; esac ;;
esac
if [ -n "${PYTHON:-}" ] && [ ! -x "$PY" ]; then
  echo "ci-local: explicit PYTHON=$PYTHON is not executable" >&2; exit 1
fi
if [ ! -x "$PY" ]; then
  if command -v uv >/dev/null 2>&1; then echo "ci-local: provisioning .venv via uv sync"; uv sync || exit 1;
  else echo "ci-local: $PY missing and no uv — run 'make sync' first." >&2; exit 1; fi
fi
export PYTHON="$PY"

SELECTED="${CI_LOCAL_SUITES:-}"
ran=0 failed=0
while read -r target stages; do
  case "$target" in ''|'#'*) continue ;; esac
  if [ -n "$stages" ]; then case " $stages " in *" $STAGE "*) ;; *) continue ;; esac; fi
  if [ -n "$SELECTED" ]; then case " $SELECTED " in *" $target "*) ;; *) continue ;; esac; fi
  ran=$((ran+1))
  echo "=== ci-local: $target ==="
  if make "$target"; then echo "--- ci-local: $target OK"; else
    echo "--- ci-local: $target FAILED" >&2; failed=$((failed+1))
    [ "${CI_LOCAL_KEEP_GOING:-0}" = "1" ] || exit 1
  fi
done < "$MANIFEST"

echo "ci-local: ran=$ran failed=$failed"
[ "$ran" -gt 0 ] || { echo "ci-local: no targets selected" >&2; exit 2; }
[ "$failed" -eq 0 ]
