"""Operator-facing one-shot scripts shipped with ``workbay_handoff_mcp``.

Submodules here are designed to be invoked via
``uvx --from mcp-workbay-handoff python -m workbay_handoff_mcp.scripts.<name>``
so consumer repos do not need an editable install. Each script owns
its own ``main`` and exits with a small set of well-defined return
codes.
"""
