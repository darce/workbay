"""Named cross-transport invariants for the mcp-workbay-handoff surface.

Every transport (stdio, HTTP, CLI fallback) MUST expose exactly the same
number of handoff tools. This count is referenced from transport tests so
that adding or removing a tool requires an explicit, reviewed change to a
single named constant rather than scattered magic numbers.
"""

from __future__ import annotations

EXPECTED_HANDOFF_TOOL_COUNT = 25
