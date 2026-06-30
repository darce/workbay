# Makefile.d/plans.mk — plan-targets surface (internal stub).
#
# This file is hoisted into consumer repos by workbay-bootstrap and
# included by their root Makefile (`include Makefile.d/*.mk`).
#
# In implementation note only the launcher contract is locked in; the recipe bodies
# are stubs that exit non-zero with a "implemented in Slice N" message.
# Slices 2-4 replace the stub bodies with real recipes; consumers
# update by re-running `workbay-bootstrap update --remote-ref <pin>`.
#
# The launcher token reuses the same distribution and isolated
# environment that bootstrap pins for the MCP server
# (`uvx mcp-workbay-handoff serve-stdio` in .mcp.json), so a freshly
# bootstrapped consumer can run the plan targets without any
# `pip install mcp-workbay-handoff` step. Override
# `WORKBAY_HANDOFF_PLAN_CLI` to a bare `python -m ...` invocation if the
# consumer manages its own venv.
#
# Every recipe forwards `--workspace-root $(CURDIR)`
# so a coordinator on `main` does not need to preflight
# `WORKBAY_HANDOFF_WORKSPACE_ROOT=$PWD` in their shell. `plan-show` also
# forwards `PLAN_MODE=baseline|working-copy|auto` and raw
# `PLAN_ARGS='--working-copy'` so operators can choose which snapshot
# (main baseline vs feature-branch working copy) the resolver returns,
# without needing to drop down to `plan_cli` directly. PLAN_MODE maps
# to the matching CLI flag (`--<mode>`); when both PLAN_MODE and
# PLAN_ARGS are set, PLAN_MODE wins on conflict simply because it is
# emitted first on the argv.

WORKBAY_HANDOFF_PLAN_CLI ?= uvx --from mcp-workbay-handoff python -m workbay_handoff_mcp.plan_cli

.PHONY: plan-show plan-edit plans-list plan-register plan-accept plan-accept-backfill

plan-show: ## Print the active task's plan via `git show <branch>:<path>` [TASK=<ref>] [PLAN_MODE=baseline|working-copy|auto] [PLAN_ARGS='...']
	@$(WORKBAY_HANDOFF_PLAN_CLI) --workspace-root $(CURDIR) show $(if $(TASK),--task $(TASK)) $(if $(PLAN_MODE),--$(PLAN_MODE)) $(PLAN_ARGS)

plan-edit: ## Open the active task's plan in $EDITOR against target_worktree_path
	@$(WORKBAY_HANDOFF_PLAN_CLI) --workspace-root $(CURDIR) edit $(if $(TASK),--task $(TASK))

plans-list: ## Print every active task's plan location, one block per task
	@$(WORKBAY_HANDOFF_PLAN_CLI) --workspace-root $(CURDIR) list --include-unset-path

plan-register: ## Persist task_plan_path: TASK=<ref> [PLAN=<docs/plans/...>]
	@test -n "$(TASK)" || { echo "plan-register: TASK=<ref> is required" >&2; exit 2; }
	@$(WORKBAY_HANDOFF_PLAN_CLI) --workspace-root $(CURDIR) register --task $(TASK) $(if $(PLAN),--plan $(PLAN))

# Docs-only acceptance of a planning-reviewed plan.
# Forwards to the workbay-system lifecycle runner (NOT the workbay-handoff
# plan_cli), because git/PR orchestration is owned by the lifecycle
# layer. Default mode prints the docs-only commit command; --local in
# LIFECYCLE_ARGS runs it inline on a clean canonical-root main checkout.
plan-accept: ## Land a clean planning-review `pass` plan on main: TASK=<ref> [REVIEW_TASK_REF=<ref>] [LOCAL=1] [PLAN=<path>] [SOURCE_BRANCH=<branch>] [LIFECYCLE_ARGS=--json]
	@test -n "$(TASK)" || { echo "plan-accept: TASK=<ref> is required" >&2; exit 2; }
	@$(LIFECYCLE) plan-accept --task $(TASK) \
		$(if $(REVIEW_TASK_REF),--review-task-ref $(REVIEW_TASK_REF)) \
		$(if $(LOCAL),--local) \
		$(if $(PLAN),--plan $(PLAN)) \
		$(if $(SOURCE_BRANCH),--source-branch $(SOURCE_BRANCH)) \
		$(LIFECYCLE_ARGS)

# One-shot backfill walk over every live handoff row
# whose plan baseline is missing from `main`. Same review/finding gate
# as `plan-accept`; idempotent (rows whose plan already lives on main
# are reported as `already_accepted`).
plan-accept-backfill: ## Walk live tasks and emit/apply acceptance for clean plans absent from main [TASK=<ref>] [LIFECYCLE_ARGS=--json|--local]
	@$(LIFECYCLE) plan-accept-backfill $(if $(TASK),--task $(TASK)) $(LIFECYCLE_ARGS)
