#!/usr/bin/env bash
# land_main.sh — canon landing gate for main (heuristics-canon v0.26.2).
# Pattern: work on feature branches (no CI trigger by design), run the local
# gate, then land to main. The remote-gate rollup green is the release
# evidence: [RLSE-02] gates are not suggestions (B), [RLSE-12] green must mean
# deployable. [skip ci] is a named exception, never the routine path:
# [TEST-30] do not silence a red suite. [TEST-24]: keep the commit-stage gate
# local and short — set LAND_SUITES to a fast subset for iteration, but the
# default is the full ci-local driver.
# Usage: bash scripts/land_main.sh [--push]
#   LAND_SKIP_CI_REASON="docs-only, ci-local brand ran"  allow [skip ci] (docs/release-metadata only)
set -euo pipefail
cd "$(dirname "$0")/.."

branch="$(git branch --show-current)"
[ "$branch" = "main" ] || { echo "land: refuse — on '$branch', not main. Land via merge to main first." >&2; exit 1; }
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "land: refuse — tracked working tree is dirty. Commit or stash your files first." >&2; exit 1
fi

PUSH=0; [ "${1:-}" = "--push" ] && PUSH=1

# [TEST-29] stop for a broken mainline: the remote VM gate on committed
# HEAD is that check here (publishes remote-gate/* statuses; zero Actions
# minutes). Run `make ci-local` first locally for the fast commit stage
# [TEST-24]. LAND_LOCAL=1 substitutes the local full run (no statuses).
if [ "${LAND_LOCAL:-0}" = "1" ]; then
  echo "land: running local gate (make ci-local)…"
  if [ -n "${LAND_SUITES:-}" ]; then export CI_LOCAL_SUITES="$LAND_SUITES"; fi
  PYTHON="${PYTHON:-.venv/bin/python}" make ci-local
else
  echo "land: running remote VM gate (make ci-report) on HEAD $(git rev-parse --short HEAD)…"
  make ci-report TARGETS="${LAND_TARGETS:-}"
fi

if [ -n "${LAND_SKIP_CI_REASON:-}" ]; then
  case "$LAND_SKIP_CI_REASON" in
    *"ci-local"*) echo "land: [skip ci] exception recorded: $LAND_SKIP_CI_REASON" ;;
    *) echo "land: refuse — LAND_SKIP_CI_REASON must name the local gate evidence (must contain 'ci-local')." >&2; exit 1 ;;
  esac
  echo "land: NOTE — [skip ci] is for docs/release-metadata only [TEST-30]; the last commit must carry [skip ci] in its message to take effect."
fi

if [ "$PUSH" = "1" ]; then
  echo "land: pushing main (remote-gate rollup above is the release evidence [RLSE-12]; hosted CI is dispatch-only)…"
  git push origin main
  echo "land: pushed. Sibling agents: git fetch && git rebase origin/main (or merge) in your worktree to pick this up."
else
  echo "land: gate passed. Re-run with --push to push main."
fi
