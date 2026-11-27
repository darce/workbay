#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
check_name="${1-workbay-manifest-gate}"

case "$check_name" in
  workbay-manifest-gate)
    CI_LOCAL_STAGE=vm-ci exec bash scripts/ci_local.sh
    ;;
  workbay-canon-lint)
    workbay_cli=workbay
    if [[ -x .venv/bin/workbay ]]; then
      workbay_cli=.venv/bin/workbay
    fi
    "$workbay_cli" canon check-drift --json
    exec "$workbay_cli" canon lint --json
    ;;
  *)
    printf 'workbay-gate: unknown check %s\n' "$check_name" >&2
    exit 2
    ;;
esac
