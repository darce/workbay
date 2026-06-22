"""Canonical WorkBay runtime + doc-mirror path roots — single source of truth.

The runtime install directory is ``.workbay/`` and the mirrored docs/contracts
path is ``docs/workbay/``. Every package resolves these through this module so
the names live in exactly one place: a future path change is a one-line flip
here, not a repo-wide sweep.
"""

from __future__ import annotations

from pathlib import Path

# Runtime install root — bootstrap materializes overlay surfaces and the remote
# clone under ``<target>/.workbay/``.
RUNTIME_ROOT_DIRNAME = ".workbay"

# Mirrored docs / contracts path — the SHARED_SURFACES consumed at install time
# (rules, contracts, templates) live under ``docs/workbay/``.
DOCS_MIRROR_DIR = "docs/workbay"

# Common derived locations under the canonical docs mirror.
CONTRACTS_DIR = f"{DOCS_MIRROR_DIR}/contracts"
RULES_DIR = f"{DOCS_MIRROR_DIR}/rules"
HARNESS_CONTRACT_RELPATH = Path(CONTRACTS_DIR) / "harness-protocol.yaml"
INSTRUCTIONS_RELPATH = Path(DOCS_MIRROR_DIR) / "instructions.md"

__all__ = [
    "CONTRACTS_DIR",
    "DOCS_MIRROR_DIR",
    "HARNESS_CONTRACT_RELPATH",
    "INSTRUCTIONS_RELPATH",
    "RULES_DIR",
    "RUNTIME_ROOT_DIRNAME",
    "docs_mirror_path",
    "runtime_root_path",
]


def docs_mirror_path(*parts: str) -> Path:
    """Return a path under the canonical docs mirror (``docs/workbay/...``)."""

    return Path(DOCS_MIRROR_DIR, *parts)


def runtime_root_path(base: Path, *parts: str) -> Path:
    """Return a path under ``<base>/.workbay/...``."""

    return base.joinpath(RUNTIME_ROOT_DIRNAME, *parts)
