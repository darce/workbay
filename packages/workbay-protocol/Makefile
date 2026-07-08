PACKAGE_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(PACKAGE_ROOT)/../..)
ifneq ($(wildcard $(REPO_ROOT)/.venv/bin/python),)
PYTHON ?= $(REPO_ROOT)/.venv/bin/python
PREFLIGHT_PYTHON ?= $(REPO_ROOT)/.venv/bin/python
endif
PYTHON ?= python3
PREFLIGHT_PYTHON ?= $(PYTHON)
PYTEST_TARGETS ?= tests
PREFLIGHT_TMPDIR ?= /tmp/workbay-preflight-$(notdir $(PACKAGE_ROOT))

PREFLIGHT_PYTHONPATH := $(PACKAGE_ROOT)/src:$(PACKAGE_ROOT):$(PACKAGE_ROOT)/tests
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(PACKAGE_ROOT)/../mcp-workbay-handoff/src
PREFLIGHT_PYTHONPATH := $(PREFLIGHT_PYTHONPATH):$(REPO_ROOT)

.PHONY: test-preflight

test-preflight:
	@status=0; \
	mkdir -p "$(PREFLIGHT_TMPDIR)"; \
	TMPDIR="$(PREFLIGHT_TMPDIR)" PYTHONPATH="$(PREFLIGHT_PYTHONPATH)" $(PREFLIGHT_PYTHON) -m pytest $(PYTEST_TARGETS) -q || status=$$?; \
	if [ -n "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE" ]; then printf '%s\n' "$$status" > "$$WORKBAY_PREFLIGHT_EXIT_CODE_FILE"; fi; \
	exit $$status
