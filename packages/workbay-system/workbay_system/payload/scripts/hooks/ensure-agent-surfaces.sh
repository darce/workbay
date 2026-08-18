#!/usr/bin/env bash
# SessionStart hook — ensure this clone's GENERATED agent surfaces exist.
#
# Three agent surfaces are generated into GITIGNORED paths, so a fresh clone
# carries the pins/sources but not the generated output — and the surfaces
# silently fail to load until they are built:
#   - Claude plugin tree  .workbay/generated/plugins/workbay-system/base/claude  (make plugins-build)
#   - Codex  plugin tree  .workbay/generated/plugins/workbay-system/base/codex   (make plugins-build)
#   - VS Code Copilot      .github/prompts/                                       (make generate-agent-workflows)
#
# This hook regenerates all of them when missing. It can only run on CLAUDE CODE
# session start — Codex and VS Code Copilot have no equivalent trigger (see the
# README "Developing in this repo" section) — but because `plugins-build` emits
# BOTH plugin trees and `generate-agent-workflows` emits the Copilot prompts,
# opening Claude once bootstraps every surface. Plugins load only at session
# start, so a one-time restart is needed after the first build. The hook is a
# no-op (fast, silent) once the surfaces exist and never blocks session start.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
claude_manifest="$repo_root/.workbay/generated/plugins/workbay-system/base/claude/.claude-plugin/plugin.json"
codex_manifest="$repo_root/.workbay/generated/plugins/workbay-system/base/codex/.codex-plugin/plugin.json"
copilot_prompts="$repo_root/.github/prompts"

# All three surfaces must be present to skip; checking only Claude + Copilot
# would leave a partial-drift clone (e.g. only the codex tree deleted) unhealed.
if [ -f "$claude_manifest" ] && [ -f "$codex_manifest" ] && [ -d "$copilot_prompts" ]; then
  exit 0
fi

echo "workbay-system agent surfaces missing (fresh clone?) — generating them…"
ok=1
make -C "$repo_root" plugins-build >/dev/null 2>&1 || ok=0
# Pass WORKFLOW_TARGET_ROOT so the Copilot prompts land in this repo's root
# .github/prompts/ (the surface VS Code reads); without --target the generator
# only rewrites the tracked package-level source copy.
make -C "$repo_root" generate-agent-workflows WORKFLOW_TARGET_ROOT="$repo_root" >/dev/null 2>&1 || ok=0
if [ "$ok" -eq 1 ]; then
  echo "Generated the Claude + Codex plugin trees and the VS Code Copilot prompts."
  echo "RESTART Claude Code (/exit, then reopen) to load the /branch-review, /plan-analyze, … slash commands."
else
  echo "Could not auto-generate every surface. Run 'cd \"$repo_root\" && make plugins-build && make generate-agent-workflows WORKFLOW_TARGET_ROOT=\"\$PWD\"' (or 'workbay-bootstrap install --target \"$repo_root\"') manually, then restart Claude Code."
fi
exit 0
