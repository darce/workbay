# Makefile.d/compaction.mk — manual compact-now surface (internal).
#
# Hoisted into consumer repos by workbay-bootstrap (lifecycle profile)
# and included by their root Makefile (`-include Makefile.d/*.mk`).
#
# `make compact-now TASK=<ref>` writes a session_compactions row for
# the named task and prints `compaction_id=<id>`. The launcher token
# defaults to the `python3 -m workbay_handoff_mcp.compaction_cli` module entry,
# resolved from the handoff package that bootstrap installs alongside the MCP
# server (git-only delivery launches that server via the `mcp_launch.py` shim
# in .mcp.json — never a per-session `uvx` PyPI resolve). Override
# `WORKBAY_HANDOFF_COMPACTION_CLI` when the consumer manages its own venv.

WORKBAY_HANDOFF_COMPACTION_CLI ?= python3 -m workbay_handoff_mcp.compaction_cli

# `make compaction-{disable,enable,status}` are flagless operator wrappers for
# the unified runtime disable surface. They shell out to the
# `mcp-workbay-handoff compaction --operation <op>` CLI dispatch which writes /
# reads the `compaction_settings` table. Pass `TASK=<ref>` to target a single
# task; omit `TASK` to write the workspace-default row.
WORKBAY_HANDOFF_COMPACTION_OP_CLI ?= mcp-workbay-handoff compaction

.PHONY: compact-now compaction-disable compaction-enable compaction-status

compact-now: ## Manually compact a session: TASK=<ref> [TRANSCRIPT=<path>] [HARNESS=manual]
	@test -n "$(TASK)" || { echo "compact-now: TASK=<ref> is required" >&2; exit 2; }
	@$(WORKBAY_HANDOFF_COMPACTION_CLI) \
		--task-ref $(TASK) \
		$(if $(TRANSCRIPT),--transcript $(TRANSCRIPT)) \
		$(if $(HARNESS),--harness $(HARNESS))

compaction-disable: ## Disable WorkBay compaction runtime: [TASK=<ref>] for task scope, else workspace default
	@$(WORKBAY_HANDOFF_COMPACTION_OP_CLI) --operation disable $(if $(TASK),--task-ref $(TASK))

compaction-enable: ## Re-enable WorkBay compaction runtime: [TASK=<ref>] for task scope, else workspace default
	@$(WORKBAY_HANDOFF_COMPACTION_OP_CLI) --operation enable $(if $(TASK),--task-ref $(TASK))

compaction-status: ## Show WorkBay compaction runtime disable status: [TASK=<ref>] to resolve for that task
	@$(WORKBAY_HANDOFF_COMPACTION_OP_CLI) --operation status $(if $(TASK),--task-ref $(TASK))
