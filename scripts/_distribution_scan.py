"""Shared scan surface for the distribution drift-gates.

The consumer-facing install/upgrade docs are policed by several sibling gates
(``check_distribution_prose`` — no retired PyPI launch grammar; and
``check_distribution_tag`` — a single agreed monorepo pin). Listing that surface
once, here, keeps them coherent: a new consumer doc is picked up by every gate
at once instead of being remembered for one and silently skipped by another —
the Shotgun-Surgery hazard the PPSSOT-HARM cross-slice review flagged when the
four gates each hand-maintained a slightly different glob tuple.

Stdlib-only and dependency-free so each gate can ``from _distribution_scan
import CONSUMER_INSTALL_DOCS`` when run directly as ``python scripts/<gate>.py``
(the script's own directory is ``sys.path[0]``).
"""

from __future__ import annotations

# Every doc that teaches a consumer how to install, launch, or pin WorkBay.
# The hoisted payload docs are shipped into consumers too; omitting them let
# retired install snippets survive there past the git-only cutover.
CONSUMER_INSTALL_DOCS = (
    "README.md",
    "docs/CONSUMER.md",
    "docs/UPGRADING.md",
    "docs/RELEASING.md",
    "packages/*/README.md",
    "packages/workbay-system/workbay_system/payload/docs/workbay/**/*.md",
)
