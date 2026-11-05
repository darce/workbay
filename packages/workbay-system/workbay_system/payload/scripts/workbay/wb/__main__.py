"""Entry: ``python <abs>/scripts/workbay/wb <verb> [args]``.

Matches the ``workbay_lifecycle`` runner shape: the package directory is
put on ``sys.path`` so ``import dispatcher`` resolves without an install.
"""

from __future__ import annotations

import sys

from dispatcher import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
