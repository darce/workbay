#!/usr/bin/env bash
# Install the WorkBay front-door CLI (`workbay`) from a tagged GitHub ref.
#
# Why this exists: member packages declare [tool.uv.sources] workspace pins for
# monorepo development. A consumer `uv tool install --from git+…#subdirectory=…`
# has no workspace root, so uv requires `--no-sources` plus an explicit `--with`
# for every runtime sibling. Operators should not have to type that by hand.
#
# Usage:
#   ./scripts/install-workbay-cli.sh [REF]
#   curl -fsSL https://raw.githubusercontent.com/darce/workbay/REF/scripts/install-workbay-cli.sh \
#     | bash -s -- REF
#
# Env:
#   WORKBAY_GIT_URL   override repo URL (default: https://github.com/darce/workbay.git)
#   WORKBAY_FORCE=0   skip `uv tool install --force` (default: force reinstall)
set -euo pipefail

REF="${1:-}"
if [[ -z "${REF}" ]]; then
  echo "usage: $0 <git-ref-or-tag>   e.g.  $0 v0.1.54" >&2
  exit 2
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "install-workbay-cli: uv not found on PATH (https://docs.astral.sh/uv/)" >&2
  exit 127
fi

REPO="${WORKBAY_GIT_URL:-https://github.com/darce/workbay.git}"
R="git+${REPO}@${REF}"
FORCE_FLAG=(--force)
if [[ "${WORKBAY_FORCE:-1}" == "0" ]]; then
  FORCE_FLAG=()
fi

# Runtime closure members (must stay aligned with
# packages/workbay-bootstrap/.../gitonly_closure.py GITONLY_RUNTIME_MEMBERS).
MEMBERS=(
  workbay-protocol
  mcp-workbay-handoff
  mcp-workbay-orchestrator
  workbay-bootstrap
  workbay-system
)

WITH_ARGS=()
for member in "${MEMBERS[@]}"; do
  WITH_ARGS+=(--with "${R}#subdirectory=packages/${member}")
done

echo "install-workbay-cli: installing workbay from ${REPO}@${REF}"
uv tool install --no-sources \
  "${FORCE_FLAG[@]}" \
  "${WITH_ARGS[@]}" \
  --from "${R}#subdirectory=packages/workbay" \
  workbay

echo "install-workbay-cli: ok — next: workbay install --target /path/to/your/repo --remote-ref ${REF}"
