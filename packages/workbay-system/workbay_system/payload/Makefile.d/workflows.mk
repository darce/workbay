# Makefile.d/workflows.mk — workflow generation + facade lint targets.
#
# Path resolution mirrors lifecycle.mk so the fragment works both in the
# monorepo source layout and when hoisted into a consumer repo.

WORKFLOWS_MK_DIR      := $(dir $(lastword $(MAKEFILE_LIST)))
WORKFLOWS_ROOT        := $(abspath $(WORKFLOWS_MK_DIR)..)
# implementation note S3: order-safe consumer/repo root. Defined here (before any
# immediate := consumes it) because the root -include glob loads plugins.mk
# before workflows.mk, so a root shared only by workflows.mk would expand empty.
# Anchor to git toplevel; fall back to the explicit target, then the fragment
# root (the hoisted consumer layout where Makefile.d/, scripts/, .claude/ are
# siblings even without git). Depth-independent — survives the payload move.
WORKFLOW_TARGET_ROOT  ?=
WORKBAY_REPO_ROOT   ?= $(shell git -C "$(WORKFLOWS_ROOT)" rev-parse --show-toplevel 2>/dev/null)
WORKBAY_TARGET_ROOT := $(if $(WORKFLOW_TARGET_ROOT),$(WORKFLOW_TARGET_ROOT),$(if $(WORKBAY_REPO_ROOT),$(WORKBAY_REPO_ROOT),$(WORKFLOWS_ROOT)))
# The generator + facade lint scripts depend on PyYAML, which the bare system
# `python3` typically lacks. Probe the pyenv-managed project interpreter first
# and verify it actually imports `yaml`; fall back to `python3` only if it does
# not. This keeps `make check-agent-workflows` working when pyenv is installed
# but the project Python has not been built/synced (and therefore lacks PyYAML).
# Operators can still override explicitly:
#   make check-agent-workflows WORKFLOWS_PYTHON=/path/to/python
# `|| true` on the probe assignment: GNU make >= 4.0 runs $(shell) through
# SHELL/.SHELLFLAGS, and the including root Makefile sets bash -eu -o pipefail
# — without it a missing pyenv aborts the snippet and the variable expands
# empty, so recipes exec the non-executable .py script directly.
WORKFLOWS_PYTHON      ?= $(shell \
  cand=$$(pyenv which python 2>/dev/null || true); \
  if [ -n "$$cand" ] && "$$cand" -c 'import yaml' >/dev/null 2>&1; then \
    echo "$$cand"; \
  else \
    command -v python3; \
  fi)

WORKBAY_TOOL_PYTHON ?= $(shell \
  workbay_bin=`command -v workbay 2>/dev/null || true`; \
  if [ -n "$$workbay_bin" ]; then \
    first_line=`head -n 1 "$$workbay_bin" | sed 's/^..//'`; \
    if printf '%s\n' "$$first_line" | grep -q python; then echo "$$first_line"; fi; \
  fi)
_WORKBAY_INSTALLED_TOOL_PYTHON := $(shell \
  cand="$(WORKBAY_TOOL_PYTHON)"; \
  if [ -n "$$cand" ] && $$cand -c 'import workbay_system, workbay_protocol' >/dev/null 2>&1; then \
    echo "$$cand"; \
  fi)
_WORKBAY_SYSTEM_PYPROJECT := $(firstword $(wildcard $(abspath $(WORKFLOWS_MK_DIR)../../..)/pyproject.toml) $(wildcard $(abspath $(WORKFLOWS_MK_DIR)../..)/pyproject.toml))
_WORKFLOW_GENERATOR_SCRIPT := $(WORKFLOWS_ROOT)/scripts/generate_agent_workflows.py
ifneq ($(_WORKBAY_SYSTEM_PYPROJECT),)
WORKFLOW_GENERATOR := uv run --project "$(_WORKBAY_SYSTEM_PYPROJECT:%/pyproject.toml=%)" workbay-overlay-tooling generate-agent-workflows
else ifneq ($(_WORKBAY_INSTALLED_TOOL_PYTHON),)
WORKFLOW_GENERATOR := $(_WORKBAY_INSTALLED_TOOL_PYTHON) -m workbay_system.overlay_tooling.cli generate-agent-workflows
else
WORKFLOW_GENERATOR := $(WORKFLOWS_PYTHON) $(_WORKFLOW_GENERATOR_SCRIPT)
endif
WORKFLOW_FACADE_CHECK := $(WORKFLOWS_ROOT)/scripts/check_workflow_facade.py
SETTINGS_PIN_CHECK    := $(WORKFLOWS_ROOT)/scripts/validate_claude_settings_pin.py
SETTINGS_PIN_FILE     := $(WORKBAY_TARGET_ROOT)/.claude/settings.json
WORKFLOW_TARGET_ARG   := $(if $(WORKFLOW_TARGET_ROOT),--target "$(WORKFLOW_TARGET_ROOT)")
# Codex router consumers (CLAUDE.md, docs/workbay/instructions.md) live at the
# git/consumer root, which in a nested-source layout (the monorepo) sits *above*
# WORKFLOWS_ROOT. Resolve to the explicit target if set, else the git top-level
# (= consumer root for consumers, repo root for the monorepo source) so the
# codex-router-block check finds the marker-bearing docs in both layouts.
CODEX_ROUTER_ROOT     := $(if $(WORKFLOW_TARGET_ROOT),$(WORKFLOW_TARGET_ROOT),$(WORKBAY_REPO_ROOT))

.PHONY: generate generate-agent-workflows check-agent-workflows check-claude-settings-pin

generate: generate-agent-workflows ## implementation note S2: regenerate all agent-workflow adapters from source (entry-point alias)

generate-agent-workflows: ## Regenerate Claude, VS Code, and Codex workflow adapters
	@$(WORKFLOW_GENERATOR) $(WORKFLOW_TARGET_ARG)

check-claude-settings-pin: ## Validate .claude/settings.json source discriminator + path + enabledPlugins
	@$(WORKFLOWS_PYTHON) $(SETTINGS_PIN_CHECK) "$(SETTINGS_PIN_FILE)"

check-agent-workflows: check-claude-settings-pin ## Regenerate adapters from source, then verify router blocks + facade (no committed drift possible)
	# implementation note S2: regenerate (write) rather than `--check` against committed
	# copies — the adapters are gitignored since S2.2, so a fresh clone has none
	# to compare against. Regeneration produces them deterministically from
	# source; the tracked source-embedded router blocks are still validated for
	# drift by `--check-codex-router-blocks`.
	@$(WORKFLOW_GENERATOR) $(WORKFLOW_TARGET_ARG)
	# implementation note S3 (resolves revA-standalone-router-doc-no-drift-gate +
	# revA-check-target-writes-tracked-files): the regenerate above HEALS the
	# tracked generator outputs (the standalone codex-command-router.md + the
	# CLAUDE.md / docs/workbay/instructions.md router blocks) IN PLACE, then the
	# block check below runs against the already-healed tree — so neither step can
	# catch drift on its own. Fail loud if the regenerate mutated any tracked
	# generator output. Skipped outside a git repo (installed-consumer make), where
	# there is nothing tracked to diff against.
	$(if $(WORKBAY_REPO_ROOT),@git -C "$(WORKBAY_REPO_ROOT)" diff --exit-code HEAD -- ':(glob)**/docs/workbay/generated/codex-command-router.md' ':(glob)**/CLAUDE.md' ':(glob)**/docs/workbay/instructions.md' || { echo "ERROR: check-agent-workflows regenerate changed a tracked codex router output (drift): the standalone codex-command-router.md or a CLAUDE.md/instructions.md router block is out of sync with the manifest. Run 'make generate' and commit the result."; exit 1; },@echo "skip router drift gate: not a git repo")
	$(if $(CODEX_ROUTER_ROOT),@$(WORKFLOW_GENERATOR) --check-codex-router-blocks --target "$(CODEX_ROUTER_ROOT)",@echo "skip codex-router-block check: no git repo and no WORKFLOW_TARGET_ROOT to locate consumer docs")
	$(if $(MCP_PIN_SOURCE),@$(WORKFLOW_GENERATOR) --check-mcp-pins --pin-source '$(MCP_PIN_SOURCE)')
	@$(WORKFLOWS_PYTHON) $(WORKFLOW_FACADE_CHECK) --root "$(WORKFLOWS_ROOT)"
