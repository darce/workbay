PYENV_ROOT ?= $(HOME)/.pyenv
DESCRIPTION_SERVICE_PYTHON := $(PYENV_ROOT)/versions/description-service/bin/python
ifneq ($(wildcard $(DESCRIPTION_SERVICE_PYTHON)),)
PYTHON ?= $(DESCRIPTION_SERVICE_PYTHON)
endif
PYTHON ?= python3
PACKAGE_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
RUFF_TARGETS := src tests
MYPY_TARGETS := src
PYTEST_TARGETS := tests

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

.PHONY: help lint-handoff fix-lint-handoff format-handoff mypy-handoff test-handoff test-handoff-description-service-acceptance check-handoff

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

test-handoff:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m pytest $(PYTEST_TARGETS) -q

test-handoff-description-service-acceptance:
	@WORKBAY_RUN_DESCRIPTION_SERVICE_ACCEPTANCE=1 $(MAKE) test-handoff PYTEST_TARGETS=tests/test_plan0040_description_service_acceptance.py

check-handoff: lint-handoff mypy-handoff test-handoff
