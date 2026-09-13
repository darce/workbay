# review-pipeline.mk — deterministic primitives for the branch-pipeline
# transitions that were previously prose-only (docs/workbay/rules/
# branch-pipeline.md sections 2, 5 and 7).
#
#   make review-stage      SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>] [REV=<n>] [INTEGRATION=main]
#   make review-adjudicate SUBJECT=<branch> PATCH=<patch> DOCS="<doc> [<doc>...]"
#   make merge-gate        SUBJECT=<branch>
#
# stage commits the review input patch and holds the proof-of-reading keys in
# a gitignored sidecar; adjudicate re-derives the keys at gate time, parses
# the verdict line and counts MEDIUM+ findings; merge-gate refuses a merge
# whose adjudication artifact is missing, non-MERGE, or fenced to a stale
# subject tip. The merge guard hook consults the same artifact, so passing
# merge-gate is the only path to a raw `git merge feature/...` on main.

REVIEW_PIPELINE_MK_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
REVIEW_PIPELINE_SCRIPT := $(abspath $(REVIEW_PIPELINE_MK_DIR)../scripts/review_pipeline.py)
REVIEW_PIPELINE_JUDGE_ROOT := $(abspath $(if $(JUDGE_ROOT),$(JUDGE_ROOT),$(REVIEW_PIPELINE_MK_DIR)../../../../..))
REVIEW_PIPELINE_PYTHON := $(REVIEW_PIPELINE_JUDGE_ROOT)/.venv/bin/python
REVIEW_PIPELINE_PYTHONPATH := $(REVIEW_PIPELINE_JUDGE_ROOT)/packages/workbay-protocol/src:$(REVIEW_PIPELINE_JUDGE_ROOT)/packages/mcp-workbay-handoff/src:$(REVIEW_PIPELINE_JUDGE_ROOT)/packages/mcp-workbay-orchestrator/src
REVIEW_PIPELINE        = "$(REVIEW_PIPELINE_PYTHON)" "$(REVIEW_PIPELINE_SCRIPT)"
INTEGRATION ?= main
REV ?= 1

.PHONY: review-stage review-adjudicate merge-gate

review-stage: ## Stage a subject branch for review: SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>] [REV=<n>] (no REV_WORKTREE -> stage cuts one)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@test -n "$(SLUG)" || { echo "SLUG=<slug> is required" >&2; exit 2; }
	@$(REVIEW_PIPELINE_JUDGE_ROOT)/scripts/assert_gate_interpreter.sh "$(REVIEW_PIPELINE_PYTHON)" "review-stage"
	@JUDGE_ROOT="$(REVIEW_PIPELINE_JUDGE_ROOT)" SYSTEM_PYTHON="$(REVIEW_PIPELINE_PYTHON)" PYTHONPATH="$(REVIEW_PIPELINE_PYTHONPATH)" $(REVIEW_PIPELINE) stage --subject '$(SUBJECT)' --slug '$(SLUG)' --integration '$(INTEGRATION)' --rev '$(REV)' $(if $(REV_WORKTREE),--worktree '$(REV_WORKTREE)')

review-adjudicate: ## Adjudicate committed review docs: SUBJECT=<branch> PATCH=<path> DOCS="<doc>..."
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@test -n "$(PATCH)" || { echo "PATCH=<input patch path> is required" >&2; exit 2; }
	@test -n "$(DOCS)" || { echo "DOCS=\"<review doc> ...\" is required" >&2; exit 2; }
	@$(REVIEW_PIPELINE_JUDGE_ROOT)/scripts/assert_gate_interpreter.sh "$(REVIEW_PIPELINE_PYTHON)" "review-adjudicate"
	@JUDGE_ROOT="$(REVIEW_PIPELINE_JUDGE_ROOT)" SYSTEM_PYTHON="$(REVIEW_PIPELINE_PYTHON)" PYTHONPATH="$(REVIEW_PIPELINE_PYTHONPATH)" $(REVIEW_PIPELINE) adjudicate --subject '$(SUBJECT)' --patch '$(PATCH)' --docs $(DOCS)

merge-gate: ## Verify the adjudication artifact authorizes merging SUBJECT=<branch> at its current tip
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@$(REVIEW_PIPELINE_JUDGE_ROOT)/scripts/assert_gate_interpreter.sh "$(REVIEW_PIPELINE_PYTHON)" "merge-gate"
	@JUDGE_ROOT="$(REVIEW_PIPELINE_JUDGE_ROOT)" SYSTEM_PYTHON="$(REVIEW_PIPELINE_PYTHON)" PYTHONPATH="$(REVIEW_PIPELINE_PYTHONPATH)" $(REVIEW_PIPELINE) merge-gate --subject '$(SUBJECT)'
