# lifecycle.mk — git-first lifecycle Make targets (internal).
#
# Each target is a one-liner that forwards to the lifecycle Python
# package at ``scripts/workbay_lifecycle/``. Path resolution is relative
# to this fragment's own location so the same file works two ways:
#
#   * Source layout (monorepo): included from the root Makefile via
#     ``-include packages/workbay-system/Makefile.d/*.mk``; the runner
#     resolves to ``packages/workbay-system/scripts/workbay_lifecycle``.
#   * Consumer layout (post-bootstrap ``--profile lifecycle``): hoisted
#     to ``Makefile.d/lifecycle.mk`` at consumer root with a sibling
#     ``scripts/workbay_lifecycle/`` directory; the runner resolves to
#     ``scripts/workbay_lifecycle``.
#
# Stub targets (slice-start/review-ready/close-check/status/tasks/
# project-events-replay) currently print a ``not_implemented`` JSON
# receipt and exit 2 — explicit failure rather than fake-green — until
# their owning slices (4-6) replace the runner handlers. The
# review/plan-side targets (plan-review/plan-analyze skill-broadcasters
# and review-run/handoff-review-run/handoff-close-check shell-outs) ship
# real bodies in implementation note; ``task-start`` lands in implementation note and ``context``
# in implementation note.
#
# task-start arg bridge: TASK= and OBJECTIVE= (plus optional
# SLUG=/MODE=/PLAN=/PLAN_REVISION=) Make variables translate into the
# corresponding lifecycle CLI flags. PLAN= accepts a glob; lex-latest
# `-rN.md` wins unless PLAN_REVISION= pins (internal).
# Values are wrapped in single quotes for shell safety; values
# containing single quotes are unsupported.

LIFECYCLE_MK_DIR    := $(dir $(lastword $(MAKEFILE_LIST)))
LIFECYCLE_RUNNER    := $(abspath $(LIFECYCLE_MK_DIR)../scripts/workbay_lifecycle)
LIFECYCLE_PYTHON    ?= python3
LIFECYCLE           := $(LIFECYCLE_PYTHON) $(LIFECYCLE_RUNNER)
# T24 / implementation note: mnemonic one-shot dispatcher over lifecycle handlers.
WB_RUNNER           := $(abspath $(LIFECYCLE_MK_DIR)../scripts/workbay/wb)
WB                  := $(LIFECYCLE_PYTHON) $(WB_RUNNER)

.PHONY: task-start task-finish task-reap finalize-plan plan-done maint-start context slice-start slice-commit review-ready close-check \
        handoff-close-check plan-review plan-analyze review-run \
        handoff-review-run status tasks doctor errors-report provision-env project-events-replay tasks-gc \
        dashboard format sync-task-plan-checklist task-plan-checklist-audit plan-status \
        task-plan-checklist-backfill wb

# internal: every operator-facing target carries a `## …` doc
# string so the root Makefile's awk-based `make help` walker surfaces it
# alongside release / workflows / plans / compaction targets. The
# two-line ``target: ## description\n\t@body`` shape is required — the
# inline ``target: ## description ; @body`` form parses as a comment
# that silently strips the recipe (`Nothing to be done for 'target'`).
# Targets intentionally hidden from `make help` (project-events-replay,
# tasks-gc, dashboard) keep the bare-recipe form and are justified
# inline below so the omission is auditable.
# Canonical workflow step order lives in docs/workbay/rules/development-workflow.md;
# help strings stay number-free so make help cannot drift from the rule doc.

# wb (implementation note / T24): mnemonic lifecycle one-shots. See
# docs/workbay/wb-lifecycle-runbook.md for the verb table + refusal map.
# VERB= is required; remaining vars mirror the collapsed make targets.
wb: ## Mnemonic lifecycle one-shot: VERB=<start|status|slice|close|gate|ship|stop|accept|doctor> [TASK=] [TEST_CMD=] [DOC=] [WB_ARGS=]
	@test -n "$(VERB)" || { echo "VERB is required, e.g. VERB=status" >&2; exit 2; }
	@$(WB) '$(VERB)' \
		$(if $(filter start,$(VERB)),$(if $(TASK),$(TASK)) $(if $(OBJECTIVE),--objective '$(OBJECTIVE)') $(if $(PLAN),--plan '$(PLAN)') $(if $(MODE),--mode '$(MODE)') $(if $(SLUG),--slug '$(SLUG)')) \
		$(if $(filter slice,$(VERB)),$(if $(N),$(N),$(if $(SLICE),$(SLICE))) $(if $(TASK),--task '$(TASK)') $(if $(TEST_CMD),--test-cmd '$(TEST_CMD)') $(if $(SLUG),--slug '$(SLUG)')) \
		$(if $(filter close,$(VERB)),$(if $(N),$(N),$(if $(SLICE),$(SLICE))) $(if $(TASK),--task '$(TASK)')) \
		$(if $(filter ship stop doctor status gate,$(VERB)),$(if $(TASK),--task '$(TASK)')) \
		$(if $(filter accept,$(VERB)),$(if $(DOC),$(DOC),$(if $(PLAN),$(PLAN))) $(if $(TASK),--task '$(TASK)')) \
		$(WB_ARGS)

task-start: ## Create the task feature branch + linked worktree: TASK=<ref> OBJECTIVE="..." [PLAN=<glob>] [MODE=worktree|here] [LIFECYCLE_ARGS=--json]
	@WORKBAY_WORKTREE_BOOTSTRAP_CMD='$(LIFECYCLE_WORKTREE_BOOTSTRAP)' $(LIFECYCLE) task-start $(if $(TASK),--task '$(TASK)') $(if $(OBJECTIVE),--objective '$(OBJECTIVE)') $(if $(SLUG),--slug '$(SLUG)') $(if $(MODE),--mode '$(MODE)') $(if $(PLAN),--plan '$(PLAN)') $(if $(PLAN_REVISION),--plan-revision '$(PLAN_REVISION)') $(LIFECYCLE_ARGS)
task-finish: ## Close task: status=done -> archive -> dashboard -> remove linked worktree -> delete merged branch: TASK=<ref> [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) task-finish $(if $(TASK),--task '$(TASK)') $(LIFECYCLE_ARGS)
task-reap: ## Classify live handoff rows; REAP_ARGS=--apply closes closeable rows: [TASK=<ref>] [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) task-reap $(if $(TASK),--task '$(TASK)') $(REAP_ARGS) $(LIFECYCLE_ARGS)
finalize-plan: ## Persist final task-plan checklist ticks onto the feature branch BEFORE merge (so they ride into main): TASK=<ref> [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) finalize-plan $(if $(TASK),--task '$(TASK)') $(LIFECYCLE_ARGS)
plan-done: ## Close an on-main MAINT planning/audit pass: status=done + tasks_gc sweep: TASK=<ref> [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) plan-done $(if $(TASK),--task '$(TASK)') $(LIFECYCLE_ARGS)
maint-start: ## Register a MAINT-* maintenance task on current branch (no worktree created): TASK=<ref> OBJECTIVE="..."
	@test -n "$(TASK)" || { echo "TASK is required, e.g. TASK=internal" >&2; exit 2; }
	@test -n "$(OBJECTIVE)" || { echo "OBJECTIVE is required" >&2; exit 2; }
	@WORKBAY_HANDOFF_DEFAULT_AGENT=$${WORKBAY_HANDOFF_DEFAULT_AGENT:-operator} \
		mcp-workbay-handoff set \
		--task-ref "$(TASK)" \
		--objective "$(OBJECTIVE)" \
		--target-branch "$$(git rev-parse --abbrev-ref HEAD)"
context: ## Print branch/task/projection state for the active task (cold-start orientation)
	@$(LIFECYCLE) context $(LIFECYCLE_ARGS)
slice-start: ## Begin a TDD slice and pin red-evidence: TASK=<ref> TEST_CMD="<command>"
	@$(LIFECYCLE) slice-start $(if $(TASK),--task '$(TASK)') $(if $(TEST_CMD),--test-cmd '$(TEST_CMD)') $(if $(SLUG),--slug '$(SLUG)') $(LIFECYCLE_ARGS)
slice-commit: ## Stage tracked changes and commit a slice: TASK=<ref> MSG="..."
	@$(LIFECYCLE) slice-commit $(if $(TASK),--task '$(TASK)') $(if $(MSG),--msg '$(MSG)') $(LIFECYCLE_ARGS)
review-ready: ## Check the active task is ready for review (tests, findings, contract drift) [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) review-ready $(LIFECYCLE_ARGS)
close-check: ## Check the active task is ready to close (gate before merge) [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) close-check $(LIFECYCLE_ARGS)
handoff-close-check: ## Enforced merge-readiness gate on the final branch HEAD
	@$(LIFECYCLE) handoff-close-check $(LIFECYCLE_ARGS)
plan-review: ## Broadcast a planning-review skill intent: DOC=<plan path>
	@$(LIFECYCLE) plan-review $(if $(DOC),--doc '$(DOC)') $(LIFECYCLE_ARGS)
plan-analyze: ## Broadcast a plan-analyze skill intent: DOC=<plan path>
	@$(LIFECYCLE) plan-analyze $(if $(DOC),--doc '$(DOC)') $(LIFECYCLE_ARGS)
review-run: ## Run the branch-review workflow against the active task
	@$(LIFECYCLE) review-run $(LIFECYCLE_ARGS)
handoff-review-run: ## Run the cross-package handoff review workflow
	@$(LIFECYCLE) handoff-review-run $(LIFECYCLE_ARGS)
status: ## Orient on the active task / control plane [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) status $(LIFECYCLE_ARGS)
tasks: ## List active tasks and recommend the next safe step [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) tasks $(LIFECYCLE_ARGS)
# doctor (internal): cold-start aggregator. Returns one
# DoctorReceipt with env / mcp / branch / lifecycle / dashboard / hooks
# facets so first-turn agents replace the make-context + DASHBOARD.txt
# + raw-MCP three-call pattern with one structured payload.
doctor: ## One-shot cold-start diagnostic: env / mcp / branch / lifecycle / dashboard / hooks
	@$(LIFECYCLE) doctor $(LIFECYCLE_ARGS)
# errors-report (internal): harvest the agent_errors ledger into
# class-level clusters. No SOURCES reads the primary repo handoff.db;
# SOURCES="<db-or-jsonl> ..." merges N consumer DBs / export bundles.
errors-report: ## Cluster captured agent errors: [SOURCES="<db|jsonl> ..."] [SINCE="<ts>"] [ERRORS_ARGS=...]
	@mcp-workbay-handoff errors-report $(foreach s,$(SOURCES),--source '$(s)') $(if $(SINCE),--since '$(SINCE)') $(ERRORS_ARGS)
# provision-env (internal follow-up): public convenience wrapper over the
# lifecycle ``provision-env`` subcommand. task-start and the orchestrator
# fresh-lane path provision the worktree-root ``.venv`` inline; this is the
# memorable manual-recovery command for an operator who needs to (re)provision
# an existing worktree's root ``.venv`` (pytest + editable packages) outside
# those flows — e.g. after the pyenv-shim red flag in the branch-lifecycle
# skill. Defaults the target worktree to ``$(CURDIR)`` (the directory ``make``
# was invoked from); override with ``WORKTREE=<path>``.
provision-env: ## Provision the worktree-root .venv (pytest + editable packages) for manual recovery: [WORKTREE=<path>] [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) provision-env --worktree '$(if $(WORKTREE),$(WORKTREE),$(CURDIR))' $(LIFECYCLE_ARGS)
# project-events-replay: internal replay tool used by maintenance scripts;
# intentionally hidden from `make help` so it does not surface as a
# routine operator target.
project-events-replay: ; @$(LIFECYCLE) project-events-replay $(LIFECYCLE_ARGS)
# tasks-gc janitor (internal): bulk-archive status=done internal-*
# rows whose internal parent is archived. Dry-run by default; APPLY=1 mutates.
# Intentionally hidden from `make help` — janitor target, not an
# operator entry point.
tasks-gc:              ; @mcp-workbay-handoff archive --operation gc $(if $(APPLY),--apply)
# dashboard (internal): deprecation alias. The dashboard
# auto-regenerates on close_slice, explicit render_handoff(kind='dashboard'),
# and resolve_review_findings when findings are fixed (internal) — not on
# every handoff write.
# Intentionally hidden from `make help` — no-op alias kept only for
# muscle-memory; surfacing it would mislead operators into thinking
# manual regen is required.
dashboard:             ; @echo "dashboard auto-regenerates on close_slice, render_handoff(kind='dashboard'), and resolve_review_findings (when findings are fixed); this command is a no-op"

# format: hoist-safe formatter entry point. The branch-lifecycle skill
# tells operators to run "make format" after each slice (step 4); in
# the monorepo source layout `make format-all` is defined at the root
# Makefile and walks every package, but bootstrap consumers do not
# inherit that target — they only get this fragment. Consumers point
# `LIFECYCLE_FORMATTER` at whatever formatter command their repo ships
# (e.g. `LIFECYCLE_FORMATTER = $(MAKE) format-all` for the monorepo
# itself, or `LIFECYCLE_FORMATTER = ruff format .` for a single-package
# consumer). Default: a loud no-op that tells the operator to set the
# variable rather than silently doing nothing.
LIFECYCLE_FORMATTER ?= echo "make format: no LIFECYCLE_FORMATTER configured. Set it in your Makefile to your repo formatter (e.g. 'ruff format .' or '\$$(MAKE) format-all') so the branch-lifecycle skill's post-slice format step runs your tooling." >&2; exit 0
# implementation note: consumer-authored post-provision command for linked worktrees.
# Forwarded to the runner as WORKBAY_WORKTREE_BOOTSTRAP_CMD on task-start
# (shell-eval'd, worktree-rooted, best-effort). Empty default = feature off.
# The value is single-quote-wrapped in the task-start recipe (same trusted-
# operator limitation as TASK=/OBJECTIVE= above): a value containing a single
# quote is unsupported.
LIFECYCLE_WORKTREE_BOOTSTRAP ?=
format: ## Run the consumer-defined formatter; configure via LIFECYCLE_FORMATTER
	@$(LIFECYCLE_FORMATTER)

# sync-task-plan-checklist (internal): evidence-driven projection of
# handoff DB state onto the `- [ ]` / `- [x]` checkboxes in a task
# plan markdown file. Dry-run by default; APPLY=1 mutates. PLAN=
# overrides the task's stored ``task_plan_path``. Granular: every
# flipped box must trace back to a specific DB record (a close_slice
# decision's changed_files, a record_event(test_result) command, or
# an explicit decision id reference). One-way ratchet (never unticks),
# Stretch never auto-ticks.
attest: ## Record plan attestation criterion: TASK=<ref> CRITERION=<id>
	@$(LIFECYCLE) attest $(if $(TASK),--task '$(TASK)') $(if $(CRITERION),--criterion '$(CRITERION)') $(LIFECYCLE_ARGS)

plan-status: ## Project task-plan checklist from handoff evidence: TASK=<ref> [PLAN=<path>]
	@$(LIFECYCLE) plan-status $(if $(TASK),--task '$(TASK)') $(if $(PLAN),--plan '$(PLAN)') $(LIFECYCLE_ARGS)

sync-task-plan-checklist: ## Sync task-plan `- [ ]` boxes from handoff DB evidence: TASK=<ref> [APPLY=1] [PLAN=<path>]
	@$(LIFECYCLE) sync-task-plan-checklist $(if $(TASK),--task '$(TASK)') $(if $(PLAN),--plan '$(PLAN)') $(if $(APPLY),--apply) $(LIFECYCLE_ARGS)

# task-plan-checklist-audit (internal): read-only inspection
# of `- [ ]` / `- [x]` state for one or more task refs / plan files.
# Never writes. Per-plan rows with already_ticked, tick_candidates,
# unresolved, stretch_skipped, and warnings. Duplicate task IDs across
# packages (e.g. the internal collision) surface as separate rows.
task-plan-checklist-audit: ## Audit task-plan checklist state read-only: TASKS='internal ...' [PLANS='path1 path2'] [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) task-plan-checklist-audit $(if $(TASKS),--tasks '$(TASKS)') $(if $(PLANS),--plans '$(PLANS)') $(LIFECYCLE_ARGS)

# task-plan-checklist-backfill (internal): historical reconciler.
# Runs sync_task_plan_checklist's parse/resolve/apply across the supplied
# refs/plans. Dry-run by default; APPLY=1 mutates. Detects task_ref
# collisions (internal shape) and suppresses bare ``Slice N`` matching
# for those refs so a slice-close decision recorded for one plan cannot
# tick a bare ``Slice N`` box in the other plan.
task-plan-checklist-backfill: ## Backfill historical task-plan checks: TASKS='internal ...' [PLANS='path1 path2'] [APPLY=1] [LIFECYCLE_ARGS=--json]
	@$(LIFECYCLE) task-plan-checklist-backfill $(if $(TASKS),--tasks '$(TASKS)') $(if $(PLANS),--plans '$(PLANS)') $(if $(APPLY),--apply) $(LIFECYCLE_ARGS)
