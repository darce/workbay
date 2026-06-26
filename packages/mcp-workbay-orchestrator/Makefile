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
ifneq ($(wildcard $(PACKAGE_ROOT)/../mcp-workbay-handoff/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-handoff/src
endif
ifneq ($(wildcard $(PACKAGE_ROOT)/../workbay-codex-bridge/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-codex-bridge/src
endif
ifneq ($(PYTHONPATH),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PYTHONPATH)
endif

.PHONY: help lint-orchestrator fix-lint-orchestrator format-orchestrator mypy-orchestrator test-orchestrator check-orchestrator tools-list-snapshot

help:
	@echo "mcp-workbay-orchestrator Commands"
	@echo "==============================="
	@echo ""
	@echo "  make lint-orchestrator     - Run Ruff checks (includes format check)"
	@echo "  make fix-lint-orchestrator - Auto-fix Ruff violations (unsafe fixes)"
	@echo "  make format-orchestrator   - Fix lint + format code with Ruff"
	@echo "  make mypy-orchestrator     - Run mypy"
	@echo "  make test-orchestrator     - Run pytest"
	@echo "  make check-orchestrator    - Run lint, mypy, and tests"
	@echo "  make tools-list-snapshot   - Capture a normalized tools/list snapshot"

lint-orchestrator:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check $(RUFF_TARGETS)
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff format --check $(RUFF_TARGETS)

fix-lint-orchestrator:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check --fix --unsafe-fixes $(RUFF_TARGETS)

format-orchestrator:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check --fix --unsafe-fixes $(RUFF_TARGETS)
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff format $(RUFF_TARGETS)

mypy-orchestrator:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m mypy $(MYPY_TARGETS)

test-orchestrator:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m pytest $(PYTEST_TARGETS) -q

check-orchestrator: lint-orchestrator mypy-orchestrator test-orchestrator

tools-list-snapshot:
	@PHASE="$${PHASE:-current}"; \
	if [ -n "$$OUT" ]; then :; \
	elif [ "$$PHASE" = "current" ]; then OUT="$(PACKAGE_ROOT)/.task-state/tools-list-snapshot.json"; \
	else OUT="$(PACKAGE_ROOT)/.task-state/tools-list-snapshot-$$PHASE.json"; fi; \
	PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m workbay_orchestrator_mcp \
		--workspace-root "$(PACKAGE_ROOT)" \
		tools-snapshot --phase "$$PHASE" --output "$$OUT"
