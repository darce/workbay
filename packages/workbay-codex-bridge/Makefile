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
ifneq ($(wildcard $(PACKAGE_ROOT)/../mcp-workbay-orchestrator/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-orchestrator/src
endif
ifneq ($(wildcard $(PACKAGE_ROOT)/../mcp-workbay-handoff/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-handoff/src
endif
ifneq ($(wildcard $(PACKAGE_ROOT)/../workbay-protocol/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-protocol/src
endif
ifneq ($(PYTHONPATH),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PYTHONPATH)
endif

PREFLIGHT_PYTHONPATH := $(PACKAGE_ROOT)/src:$(PACKAGE_ROOT):$(PACKAGE_ROOT)/tests
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(REPO_ROOT)

.PHONY: help lint-bridge fix-lint-bridge format-bridge mypy-bridge test-bridge test-preflight check-bridge

help:
	@echo "workbay-codex-bridge Commands"
	@echo "==============================="
	@echo ""
	@echo "  make lint-bridge     - Run Ruff checks (includes format check)"
	@echo "  make fix-lint-bridge - Auto-fix Ruff violations (unsafe fixes)"
	@echo "  make format-bridge   - Fix lint + format code with Ruff"
	@echo "  make mypy-bridge     - Run mypy"
	@echo "  make test-bridge     - Run pytest"
	@echo "  make check-bridge    - Run lint, mypy, and tests"

lint-bridge:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check $(RUFF_TARGETS)
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff format --check $(RUFF_TARGETS)

fix-lint-bridge:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check --fix --unsafe-fixes $(RUFF_TARGETS)

format-bridge:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff check --fix --unsafe-fixes $(RUFF_TARGETS)
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m ruff format $(RUFF_TARGETS)

mypy-bridge:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m mypy $(MYPY_TARGETS)

test-bridge:
	@PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m pytest $(PYTEST_TARGETS) -q

test-preflight:
	@status=0; \
	mkdir -p "$(PREFLIGHT_TMPDIR)"; \
	TMPDIR="$(PREFLIGHT_TMPDIR)" PYTHONPATH="$(PREFLIGHT_PYTHONPATH)" $(PREFLIGHT_PYTHON) -m pytest $(PYTEST_TARGETS) -q || status=$$?; \
	if [ -n "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE" ]; then printf '%s\n' "$$status" > "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE"; fi; \
	exit $$status

check-bridge: lint-bridge mypy-bridge test-bridge
