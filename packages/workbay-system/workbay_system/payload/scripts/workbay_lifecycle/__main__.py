"""Entry point: ``python <abs-path>/scripts/workbay_lifecycle ...``.

Delegates to :func:`lifecycle.cli.main`. Kept intentionally thin so the
dispatch table stays in :mod:`cli` and is testable without the
``-m`` / package-as-script invocation indirection.
"""

from __future__ import annotations

import sys

from cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
