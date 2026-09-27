"""Operator-facing one-shot scripts shipped with ``workbay_handoff_mcp``.

Submodules here are designed to be invoked via
``python3 -m workbay_handoff_mcp.scripts.<name>`` against the handoff
package that ``workbay-bootstrap`` installs (git-only delivery — no PyPI
``uvx`` / ``pip install`` step). Each script owns
its own ``main`` and exits with a small set of well-defined return
codes.
"""
