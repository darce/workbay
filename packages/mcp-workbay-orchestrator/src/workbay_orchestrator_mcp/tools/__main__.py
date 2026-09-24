"""``python -m workbay_orchestrator_mcp.tools`` is not a single tool entrypoint.

Use an explicit submodule, e.g.::

    python -m workbay_orchestrator_mcp.tools.backfill_grok_token_splits --db PATH
"""

from __future__ import annotations

import sys

print(
    "workbay_orchestrator_mcp.tools: specify a tool module "
    "(e.g. python -m workbay_orchestrator_mcp.tools.backfill_grok_token_splits --db PATH)",
    file=sys.stderr,
)
raise SystemExit(2)
