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
# This hook regenerates all of them when missing in a self-host source clone.
# Installed external consumers receive an advisory without running generators.
# It can only run on CLAUDE CODE
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

# Match the in-tree source anchor used by install.self_host_payload_root and
# bootstrap_surfaces. Also require the generator inputs and root Makefile:
# copied hooks or a partial payload directory do not make a consumer self-host.
# Resolve against this hook's repo root, never the session's working directory.
payload_root="$repo_root/packages/workbay-system/workbay_system/payload"
self_host=1
if [ ! -f "$repo_root/Makefile" ] || \
   [ ! -f "$payload_root/scripts/generate_agent_workflows.py" ] || \
   [ ! -f "$payload_root/config/agent-workflows/portable_commands.json" ] || \
   [ ! -d "$payload_root/skills" ]; then
  self_host=0
fi

# Existence is not provenance: an installed (wheel) payload copied or linked
# into an application root beside its own Makefile must never run that
# application's make targets. Bind generation to an actual self-host checkout
# with bounded read-only file tests and Git index data only:
#   - no symlink on the payload path or the provenance anchors;
#   - the canonical build input packages/workbay-system/pyproject.toml (never
#     shipped in a wheel) and the payload Make fragments defining both targets,
#     included by the root Makefile;
#   - this hook's repo root is its Git top-level and every anchor is tracked.
system_rel="packages/workbay-system"
payload_rel="$system_rel/workbay_system/payload"
anchors="Makefile $system_rel/pyproject.toml $payload_rel/Makefile.d/plugins.mk $payload_rel/Makefile.d/workflows.mk $payload_rel/config/agent-workflows/portable_commands.json"
git_data() (
  unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_NAMESPACE
  GIT_OPTIONAL_LOCKS=0 git -c core.fsmonitor=false -C "$repo_root" "$@"
)
for path in packages "$system_rel" "$system_rel/workbay_system" "$payload_rel" \
            "$payload_rel/scripts" "$payload_rel/config" "$payload_rel/config/agent-workflows" \
            "$payload_rel/Makefile.d" "$payload_rel/skills" $anchors; do
  if [ -L "$repo_root/$path" ] || [ ! -e "$repo_root/$path" ]; then self_host=0; fi
done
if [ "$self_host" -eq 1 ]; then
  if ! grep -Fqx 'name = "workbay-system"' "$repo_root/$system_rel/pyproject.toml" || \
     ! grep -Fq "/$payload_rel/Makefile.d/" "$repo_root/Makefile"; then
    self_host=0
  fi
fi
if [ "$self_host" -eq 1 ]; then
  top="$(git_data rev-parse --show-toplevel 2>/dev/null)" || top=""
  # anchors is a fixed list of relative, space-free paths: split it on purpose.
  # shellcheck disable=SC2086
  if [ -z "$top" ] || [ "$top" != "$(cd "$repo_root" && pwd -P)" ] || \
     ! git_data ls-files --error-unmatch -- $anchors "$payload_rel/skills" >/dev/null 2>&1; then
    self_host=0
  fi
fi
if [ "$self_host" -ne 1 ]; then
  echo "workbay-system agent surfaces missing in an external consumer; automatic generation requires a tracked in-tree WorkBay source checkout. Review this consumer's workbay-bootstrap installation to restore the surfaces."
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
  echo "RESTART Claude Code (/exit, then reopen) to load the /wb-review-code, /wb-draft, … slash commands."
else
  echo "Could not auto-generate every surface. Run 'cd \"$repo_root\" && make plugins-build && make generate-agent-workflows WORKFLOW_TARGET_ROOT=\"\$PWD\"' (or 'workbay-bootstrap install --target \"$repo_root\"') manually, then restart Claude Code."
fi
exit 0
