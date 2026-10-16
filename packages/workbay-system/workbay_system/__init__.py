"""workbay-system overlay payload package.

Ships the canonical overlay surfaces — skills, the workflow generator,
agent-workflows config, and the shared hook/contract surfaces — as package
data so ``workbay-bootstrap`` can materialize them from an installed
distribution (the package delivery source) instead of a git clone.

The payload is co-located under ``workbay_system/payload/`` and ships by
"ship the package" (``only-include = ["workbay_system"]`` in
``pyproject.toml`` — no force-include map), so :func:`data_root` resolves to the
``payload/`` directory that contains ``skills/``, ``scripts/``, ``config/``,
``docs/``, ``Makefile.d/``, and ``.github/`` once installed.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

__all__ = ["data_root"]


def data_root() -> Path:
    """Return the filesystem root of the installed overlay payload."""
    return Path(str(resources.files(__name__) / "payload"))
