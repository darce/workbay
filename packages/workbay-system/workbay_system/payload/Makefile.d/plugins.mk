# Makefile.d/plugins.mk — internal plugin-tree emission targets.
#
# `plugins-build`  emits Claude + Codex + Grok plugin trees under the same
#                  bootstrap-generated base root used by marketplace pins:
#                  .workbay/generated/plugins/workbay-system/base/
#                  from the canonical inputs
#                  (skills/, config/agent-workflows/portable_commands.json,
#                  config/agent-workflows/mcp_servers.yaml).
# `plugins-check`  re-runs the generator with --check against the same
#                  destination and exits non-zero on drift.
#
# Path resolution mirrors workflows.mk so the fragment works both in the
# monorepo source layout and when hoisted into a consumer repo.

PLUGINS_MK_DIR        := $(dir $(lastword $(MAKEFILE_LIST)))
PLUGINS_ROOT          := $(abspath $(PLUGINS_MK_DIR)..)
# implementation note S3: order-safe consumer/repo root, computed locally (plugins.mk
# loads before workflows.mk under the root -include glob, so it cannot borrow
# workflows.mk's root). git-anchored + depth-independent — replaces the fragile
# `notdir == workbay-system` sentinel that silently flipped to the wrong
# branch once the fragment moved under workbay_system/payload/.
WORKFLOW_TARGET_ROOT  ?=
WORKBAY_REPO_ROOT   ?= $(shell git -C "$(PLUGINS_ROOT)" rev-parse --show-toplevel 2>/dev/null)
PLUGINS_TARGET_ROOT   := $(if $(WORKFLOW_TARGET_ROOT),$(WORKFLOW_TARGET_ROOT),$(if $(WORKBAY_REPO_ROOT),$(WORKBAY_REPO_ROOT),$(PLUGINS_ROOT)))
# Reuse the PyYAML-aware interpreter probe from workflows.mk by sharing
# the WORKFLOWS_PYTHON variable name; if workflows.mk has not been
# included yet, repeat the same probe locally so plugins-* targets can be
# invoked standalone.
WORKFLOWS_PYTHON      ?= $(shell \
  cand=$$(pyenv which python 2>/dev/null); \
  if [ -n "$$cand" ] && "$$cand" -c 'import yaml' >/dev/null 2>&1; then \
    echo "$$cand"; \
  else \
    command -v python3; \
  fi)

_WORKBAY_SYSTEM_PYPROJECT := $(wildcard $(abspath $(PLUGINS_MK_DIR)../..)/pyproject.toml)
_PLUGINS_GENERATOR_SCRIPT := $(PLUGINS_ROOT)/scripts/generate_agent_workflows.py
ifneq ($(_WORKBAY_SYSTEM_PYPROJECT),)
PLUGINS_GENERATOR := uv run --project "$(abspath $(PLUGINS_MK_DIR)../..)" workbay-overlay-tooling generate-agent-workflows
else
PLUGINS_GENERATOR := $(WORKFLOWS_PYTHON) $(_PLUGINS_GENERATOR_SCRIPT)
endif
PLUGINS_DIST_ROOT     ?= $(PLUGINS_TARGET_ROOT)/.workbay/generated/plugins/workbay-system/base
# internal always-effective: every build also composes the effective tree the
# marketplace pins target. With a consumer override root present the generator
# composes base + overrides; otherwise it emits the base tree unchanged plus a
# passthrough plugin-lock.json receipt.
PLUGINS_EFFECTIVE_ROOT ?= $(PLUGINS_TARGET_ROOT)/.workbay/generated/plugins/workbay-system/effective
PLUGINS_OVERRIDE_ROOT  ?= $(PLUGINS_TARGET_ROOT)/workbay-overrides/workbay-system
PLUGINS_BASE_SHA       ?= $(shell git -C "$(PLUGINS_ROOT)" rev-parse HEAD 2>/dev/null)
PLUGINS_EFFECTIVE_ARGS  = $(if $(wildcard $(PLUGINS_OVERRIDE_ROOT)/overrides.yaml),--plugin-overrides "$(PLUGINS_OVERRIDE_ROOT)",--plugin-passthrough-lock) --plugin-base-remote-sha "$(PLUGINS_BASE_SHA)"

.PHONY: plugins-build plugins-check

plugins-build: ## Emit Claude + Codex + Grok base plugin trees and compose the effective trees
	@$(PLUGINS_GENERATOR) --mode=plugin --plugin-out "$(PLUGINS_DIST_ROOT)"
	@$(PLUGINS_GENERATOR) --mode=plugin --plugin-out "$(PLUGINS_EFFECTIVE_ROOT)" $(PLUGINS_EFFECTIVE_ARGS)

plugins-check: ## Verify base and effective plugin trees match the canonical inputs
	@$(PLUGINS_GENERATOR) --mode=plugin --plugin-out "$(PLUGINS_DIST_ROOT)" --check
	@$(PLUGINS_GENERATOR) --mode=plugin --plugin-out "$(PLUGINS_EFFECTIVE_ROOT)" $(PLUGINS_EFFECTIVE_ARGS) --check
