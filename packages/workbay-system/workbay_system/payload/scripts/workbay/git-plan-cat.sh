#!/usr/bin/env bash
# git-plan-cat.sh — `git plan-cat [task-ref]` shell wrapper.
#
# Hoisted into consumer repos by workbay-bootstrap. Backs the optional
# `[alias] plan-cat = !git-plan-cat.sh` entry described in
# docs/CONSUMER.md; users opt in by adding the alias to their
# `.gitconfig`. The Make targets remain the canonical entrypoint, so
# this wrapper exists purely as a `git`-native convenience that resolves
# through the same `workbay_handoff_mcp.plan_cli show` path the Makefile
# drives — guaranteeing byte-for-byte parity with `make plan-show`.
#
# Override `WORKBAY_HANDOFF_PLAN_CLI` to bypass the default `uvx` launcher
# (e.g. when the consumer manages its own venv). Any
# extra positional arg is forwarded as the task ref; with zero args the
# resolver falls back to the active task, matching `make plan-show`.

set -euo pipefail

PLAN_CLI=${WORKBAY_HANDOFF_PLAN_CLI:-uvx --from mcp-workbay-handoff python -m workbay_handoff_mcp.plan_cli}

# Intentional unquoted expansion: the launcher variable carries multiple shell
# tokens (e.g. `uvx --from mcp-workbay-handoff python -m
# workbay_handoff_mcp.plan_cli`) that must word-split before exec.
if [ "$#" -eq 0 ]; then
    exec ${PLAN_CLI} show
fi
exec ${PLAN_CLI} show --task "$1"
