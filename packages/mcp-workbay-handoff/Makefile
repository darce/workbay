PYENV_ROOT ?= $(HOME)/.pyenv
DESCRIPTION_SERVICE_PYTHON := $(PYENV_ROOT)/versions/description-service/bin/python
ifneq ($(wildcard $(DESCRIPTION_SERVICE_PYTHON)),)
PYTHON ?= $(DESCRIPTION_SERVICE_PYTHON)
endif
PACKAGE_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(PACKAGE_ROOT)/../..)
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

.PHONY: help lint-handoff fix-lint-handoff format-handoff mypy-handoff test-handoff test-preflight test-handoff-description-service-acceptance check-handoff check-version-drift-pyenv sync-version-drift-pyenv sync-handoff-embeddings

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
	@echo "  make test-handoff-description-service-acceptance - implementation note opt-in nested-suite gate"
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

test-handoff: check-version-drift-pyenv
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m pytest $(PYTEST_TARGETS) -q --durations=25

test-preflight:
	@status=0; \
	mkdir -p "$(PREFLIGHT_TMPDIR)"; \
	TMPDIR="$(PREFLIGHT_TMPDIR)" PYTHONPATH="$(PREFLIGHT_PYTHONPATH)" $(PREFLIGHT_PYTHON) -m pytest $(PYTEST_TARGETS) -q || status=$$?; \
	if [ -n "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE" ]; then printf '%s\n' "$$status" > "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE"; fi; \
	exit $$status

# Fail-fast (Release It! Sec 5.5, "check resources at the very start of the
# transaction") BEFORE the ~137s suite if the description-service pyenv has a
# stale version_of install (installed metadata != pyproject) — that makes
# version_of report a wrong __version__. No sync precedes test-handoff, so this
# make prereq IS its transaction-start check. Recipe-local + guarded: a no-op
# when the pyenv is absent (fresh clone / CI) or SKIP_DSENV_CHECK is set.
check-version-drift-pyenv:
	@if [ -n "$(SKIP_DSENV_CHECK)" ]; then \
		echo "check-version-drift-pyenv: skipped (SKIP_DSENV_CHECK)"; \
	elif [ ! -x "$(DESCRIPTION_SERVICE_PYTHON)" ]; then \
		echo "check-version-drift-pyenv: skipped (description-service pyenv absent)"; \
	else \
		"$(DESCRIPTION_SERVICE_PYTHON)" "$(REPO_ROOT)/scripts/version_of_drift.py" --check \
			|| { echo "  Re-derive with: make sync-version-drift-pyenv" >&2; exit 1; }; \
	fi

# Idempotent (DDIA) re-derive: reinstall every installed version_of dist into the
# description-service pyenv from its in-tree source. Recipe-local, empty=no-op.
sync-version-drift-pyenv:
	@if [ ! -x "$(DESCRIPTION_SERVICE_PYTHON)" ]; then \
		echo "sync-version-drift-pyenv: skipped (description-service pyenv absent)"; \
	else \
		dirs=$$("$(DESCRIPTION_SERVICE_PYTHON)" "$(REPO_ROOT)/scripts/version_of_drift.py" --print-installed-dirs); \
		if [ -z "$$dirs" ]; then \
			echo "sync-version-drift-pyenv: no installed version_of dists"; \
		else \
			cd "$(REPO_ROOT)" && uv pip install --python "$(DESCRIPTION_SERVICE_PYTHON)" --no-deps --reinstall $$dirs; \
		fi; \
	fi

test-handoff-description-service-acceptance:
	@WORKBAY_RUN_DESCRIPTION_SERVICE_ACCEPTANCE=1 $(MAKE) test-handoff PYTEST_TARGETS=tests/test_plan0040_description_service_acceptance.py

check-handoff: lint-handoff mypy-handoff test-handoff
