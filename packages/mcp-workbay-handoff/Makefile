PACKAGE_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(PACKAGE_ROOT)/../..)

# implementation note (internal): dedicated uv-managed env for the handoff suite,
# re-derived from src on every gated run by `ensure-handoff-env` (sync-on-run).
# Preferred over the standing description-service pyenv (retired in implementation note) and
# the shared root `.venv` so the suite never trusts a drifted standing install.
# `test-handoff` invokes $(HANDOFF_PYTHON) directly (its `ensure-handoff-env`
# prerequisite guarantees the env exists on first run, before parse-time
# $(wildcard) would see it); the cascade preference below keeps the ancillary
# lint/mypy targets on the same env once it has been provisioned.
HANDOFF_ENV := $(PACKAGE_ROOT)/.venv-handoff
HANDOFF_PYTHON := $(HANDOFF_ENV)/bin/python
ifneq ($(wildcard $(HANDOFF_PYTHON)),)
PYTHON ?= $(HANDOFF_PYTHON)
endif
# implementation note/4 (internal): the description-service pyenv is fully retired
# as a handoff interpreter — the dedicated .venv-handoff env (re-derived from src
# on every gated run) supersedes it. Fallback to the shared root .venv, then a
# bare python3, for ancillary targets when .venv-handoff is not yet provisioned.
ifneq ($(wildcard $(REPO_ROOT)/.venv/bin/python),)
PYTHON ?= $(REPO_ROOT)/.venv/bin/python
PREFLIGHT_PYTHON ?= $(REPO_ROOT)/.venv/bin/python
endif
PYTHON ?= python3
PREFLIGHT_PYTHON ?= $(PYTHON)
RUFF_TARGETS := src tests
MYPY_TARGETS := src
PYTEST_TARGETS := tests
PREFLIGHT_TMPDIR ?= /tmp/workbay-preflight-$(notdir $(PACKAGE_ROOT))

LOCAL_PYTHONPATH := $(PACKAGE_ROOT)/src
ifneq ($(wildcard $(PACKAGE_ROOT)/../workbay-protocol/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-protocol/src
endif
ifneq ($(wildcard $(PACKAGE_ROOT)/../workbay-codex-bridge/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-codex-bridge/src
endif
# test_enums.py imports workbay_orchestrator_mcp to assert cross-package enum
# parity; expose the sibling src so `make test-handoff` resolves it on a fresh
# worktree (the package is not a runtime dependency, so it is path-only).
ifneq ($(wildcard $(PACKAGE_ROOT)/../mcp-workbay-orchestrator/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-orchestrator/src
endif
ifneq ($(PYTHONPATH),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PYTHONPATH)
endif

PREFLIGHT_PYTHONPATH := $(PACKAGE_ROOT)/src:$(PACKAGE_ROOT):$(PACKAGE_ROOT)/tests
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-protocol/src
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-system/workbay_system/payload/scripts
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(REPO_ROOT)

.PHONY: help lint-handoff fix-lint-handoff format-handoff mypy-handoff ensure-handoff-env test-handoff test-preflight test-handoff-acceptance test-handoff-embeddings-combo check-handoff sync-handoff-embeddings

sync-handoff-embeddings: ## Install [embeddings] extra into repo root .venv (implementation note)
	$(MAKE) -C $(REPO_ROOT) sync-handoff-embeddings

help:
	@echo "mcp-workbay-handoff Commands"
	@echo "=========================="
	@echo ""
	@echo "  make lint-handoff     - Run Ruff checks (includes format check)"
	@echo "  make fix-lint-handoff - Auto-fix Ruff violations (unsafe fixes)"
	@echo "  make format-handoff   - Fix lint + format code with Ruff"
	@echo "  make mypy-handoff     - Run mypy"
	@echo "  make test-handoff     - Run pytest (PYTEST_TARGETS= overrides default tests/)"
	@echo "  make test-handoff-acceptance - implementation note opt-in nested-suite gate (WORKBAY_ACCEPTANCE_PYTHON, default .venv-handoff)"
	@echo "  make check-handoff    - Run lint, mypy, and tests"

lint-handoff:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check $(RUFF_TARGETS)
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff format --check $(RUFF_TARGETS)

fix-lint-handoff:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check --fix --unsafe-fixes $(RUFF_TARGETS)

format-handoff:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check --fix --unsafe-fixes $(RUFF_TARGETS)
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff format $(RUFF_TARGETS)

mypy-handoff:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m mypy $(MYPY_TARGETS)

ensure-handoff-env: ## Provision/refresh the dedicated uv-managed handoff test env from src (sync-on-run; no-op fast when warm)
	@UV_PROJECT_ENVIRONMENT=$(HANDOFF_ENV) uv sync --package mcp-workbay-handoff --extra test --extra dev --frozen

test-handoff: ensure-handoff-env
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(HANDOFF_PYTHON) -m pytest $(PYTEST_TARGETS) -q --durations=25

test-preflight:
	@status=0; \
	rm -rf "$(PREFLIGHT_TMPDIR)"; \
	mkdir -p "$(PREFLIGHT_TMPDIR)"; \
	trap 'rm -rf "$(PREFLIGHT_TMPDIR)"' EXIT INT TERM HUP; \
	TMPDIR="$(PREFLIGHT_TMPDIR)" PYTHONPATH="$(PREFLIGHT_PYTHONPATH)" $(PREFLIGHT_PYTHON) -m pytest $(PYTEST_TARGETS) -q || status=$$?; \
	if [ -n "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE" ]; then printf '%s\n' "$$status" > "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE"; fi; \
	exit $$status

# implementation note (internal): check-version-drift-pyenv retired by
# prove-by-removal. The gate probed the description-service pyenv for version_of
# skew, but the handoff suite no longer runs under that interpreter — it runs
# under the dedicated .venv-handoff env, re-derived from src on every gated run
# by ensure-handoff-env (sync-on-run), which cannot rot between runs. The gate's
# cross-interpreter coverage is therefore vacuous by construction. This
# consciously supersedes implementation note Resolution #1 (which kept the gate fatal while
# the pyenv persisted as a standing env). Standing detectors remain: the doctor
# version_of_skew facet and the in-suite check-workbay assertion over root .venv.

test-handoff-acceptance:
	@WORKBAY_RUN_ACCEPTANCE_SUITE=1 $(MAKE) test-handoff PYTEST_TARGETS=tests/test_plan0040_acceptance.py

# implementation note R6: combo regression for embeddings/semantic test hermeticity. Runs
# the env-leak-sensitive tests TOGETHER (the concept_embeddings UNIQUE +
# unconfigured/no_embeddings pairs that pass solo but flipped skip_reason when a
# process-global provider cache leaked from a prior test) so a regression in the
# _hermetic_embedding_provider_env conftest fixture (provider-env scrub + cache
# reset) is caught combined, not only per-file.
test-handoff-embeddings-combo: ensure-handoff-env
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(HANDOFF_PYTHON) -m pytest tests/ -q -p no:cacheprovider \
		-k "embedding or concept_embed or no_embeddings or unconfigured or semantic"

check-handoff: lint-handoff mypy-handoff test-handoff
