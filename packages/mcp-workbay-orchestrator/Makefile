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
ifneq ($(wildcard $(PACKAGE_ROOT)/../mcp-workbay-handoff/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-handoff/src
endif
ifneq ($(wildcard $(PACKAGE_ROOT)/../workbay-codex-bridge/src),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-codex-bridge/src
endif
ifneq ($(PYTHONPATH),)
LOCAL_PYTHONPATH := $(LOCAL_PYTHONPATH):$(PYTHONPATH)
endif

PREFLIGHT_PYTHONPATH := $(PACKAGE_ROOT)/src:$(PACKAGE_ROOT):$(PACKAGE_ROOT)/tests
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-handoff/src
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-protocol/src
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(PACKAGE_ROOT)/../workbay-system/workbay_system/payload/scripts
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(REPO_ROOT)

.PHONY: help lint-orchestrator fix-lint-orchestrator format-orchestrator mypy-orchestrator test-orchestrator test-preflight check-orchestrator tools-list-snapshot

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

test-preflight:
	@status=0; \
	rm -rf "$(PREFLIGHT_TMPDIR)"; \
	mkdir -p "$(PREFLIGHT_TMPDIR)"; \
	trap 'rm -rf "$(PREFLIGHT_TMPDIR)"' EXIT INT TERM HUP; \
	TMPDIR="$(PREFLIGHT_TMPDIR)" PYTHONPATH="$(PREFLIGHT_PYTHONPATH)" $(PREFLIGHT_PYTHON) -m pytest $(PYTEST_TARGETS) -q || status=$$?; \
	if [ -n "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE" ]; then printf '%s\n' "$$status" > "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE"; fi; \
	exit $$status

check-orchestrator: lint-orchestrator mypy-orchestrator test-orchestrator

tools-list-snapshot:
	@PHASE="$${PHASE:-current}"; \
	if [ -n "$$OUT" ]; then :; \
	elif [ "$$PHASE" = "current" ]; then OUT="$(PACKAGE_ROOT)/.task-state/tools-list-snapshot.json"; \
	else OUT="$(PACKAGE_ROOT)/.task-state/tools-list-snapshot-$$PHASE.json"; fi; \
	PYTHONPATH="$(LOCAL_PYTHONPATH)" $(PYTHON) -m workbay_orchestrator_mcp \
		--workspace-root "$(PACKAGE_ROOT)" \
		tools-snapshot --phase "$$PHASE" --output "$$OUT"
