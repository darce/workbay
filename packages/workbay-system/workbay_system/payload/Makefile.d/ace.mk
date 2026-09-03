# Makefile.d/ace.mk — ACE reflection/apply operator surface (implementation note).
#
# Hoisted into consumer repos by workbay-bootstrap and included by the root
# Makefile (`-include Makefile.d/*.mk`). Every recipe delegates to the
# orchestrator CLI pinned by bootstrap; override ``WORKBAY_ACE_CLI`` for
# source-tree tests or offline installs.
#
# Consumers declare playbook content once via ``WORKBAY_ACE_PLAYBOOK_FILES``
# (whitespace-separated paths). Reflect, curation-report, and metrics targets
# require a non-empty declaration; metrics and trends require ``TASK=<ref>``.
#
# ``WORKBAY_ACE_PLAYBOOK_FILES`` is the canonical declaration: these recipes
# expand it into explicit ``--playbook-file`` flags, and the orchestrator CLI
# also resolves the same env var through ``workbay_protocol.resolve_env_alias``
# (the single seam that renames in lockstep under the WorkBay rebrand, implementation note).

# Default resolves the git-installed `mcp-workbay-orchestrator` console script
# from PATH (the closure bootstrap installs) — never a per-session `uvx` PyPI
# resolve. Override for a source-tree `python -m ...` invocation.
WORKBAY_ACE_CLI ?= mcp-workbay-orchestrator --workspace-root $(CURDIR)

_ACE_PLAYBOOK_ARGS = $(foreach _ace_pf,$(WORKBAY_ACE_PLAYBOOK_FILES),--playbook-file $(_ace_pf))

.PHONY: ace-reflect ace-curation-report ace-metrics ace-metrics-json ace-trends

ace-reflect: ## Apply pending ACE counter updates: WORKBAY_ACE_PLAYBOOK_FILES=<paths> [ACE_ARGS=--dry-run]
	@test -n "$(strip $(WORKBAY_ACE_PLAYBOOK_FILES))" || { echo "ace-reflect: WORKBAY_ACE_PLAYBOOK_FILES must name at least one playbook file" >&2; exit 2; }
	@$(WORKBAY_ACE_CLI) ace-reflect $(_ACE_PLAYBOOK_ARGS) $(ACE_ARGS)

ace-curation-report: ## Print ACE curation report: WORKBAY_ACE_PLAYBOOK_FILES=<paths>
	@test -n "$(strip $(WORKBAY_ACE_PLAYBOOK_FILES))" || { echo "ace-curation-report: WORKBAY_ACE_PLAYBOOK_FILES must name at least one playbook file" >&2; exit 2; }
	@$(WORKBAY_ACE_CLI) ace-curation-report $(_ACE_PLAYBOOK_ARGS)

ace-metrics: ## Build ACE metrics snapshot: TASK=<ref> WORKBAY_ACE_PLAYBOOK_FILES=<paths> [ACE_ARGS=--format json]
	@test -n "$(TASK)" || { echo "ace-metrics: TASK=<task-ref> is required" >&2; exit 2; }
	@test -n "$(strip $(WORKBAY_ACE_PLAYBOOK_FILES))" || { echo "ace-metrics: WORKBAY_ACE_PLAYBOOK_FILES must name at least one playbook file" >&2; exit 2; }
	@$(WORKBAY_ACE_CLI) ace-metrics --task-ref $(TASK) $(_ACE_PLAYBOOK_ARGS) $(ACE_ARGS)

ace-metrics-json: ## Alias for ace-metrics with JSON output: TASK=<ref> WORKBAY_ACE_PLAYBOOK_FILES=<paths>
	@test -n "$(TASK)" || { echo "ace-metrics-json: TASK=<task-ref> is required" >&2; exit 2; }
	@test -n "$(strip $(WORKBAY_ACE_PLAYBOOK_FILES))" || { echo "ace-metrics-json: WORKBAY_ACE_PLAYBOOK_FILES must name at least one playbook file" >&2; exit 2; }
	@$(WORKBAY_ACE_CLI) ace-metrics --task-ref $(TASK) $(_ACE_PLAYBOOK_ARGS) $(ACE_ARGS) --format json

ace-trends: ## Print ACE metrics sparklines: TASK=<ref>
	@test -n "$(TASK)" || { echo "ace-trends: TASK=<task-ref> is required" >&2; exit 2; }
	@$(WORKBAY_ACE_CLI) ace-trends --task-ref $(TASK)
