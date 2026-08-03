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

# Bootstrap/overlay manifest filenames — the single source of truth shared by the
# bootstrap installer (workbay-bootstrap) and the handoff init-state reader
# (mcp-workbay-handoff). The canonical marker install writes is
# ``.workbay-bootstrap.json``; ``.workbay-overlay.json`` is the legacy WorkBay
# overlay name the installer migrates forward. ``.agentic-overlay.json`` is the
# older pre-WorkBay overlay name, handled as a distinct legacy-origin guard (NOT a
# readable manifest) and therefore excluded from MANIFEST_NAME_PRECEDENCE.
BOOTSTRAP_MANIFEST_NAME = ".workbay-bootstrap.json"
LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME = ".workbay-overlay.json"
LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME = ".agentic-overlay.json"
# Canonical lookup order for reading a valid bootstrap/overlay manifest: the
# current name first, then the migrated-forward legacy WorkBay overlay name.
MANIFEST_NAME_PRECEDENCE = (
    BOOTSTRAP_MANIFEST_NAME,
    LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME,
)

# Mirrored docs / contracts path — the SHARED_SURFACES consumed at install time
# (rules, contracts, templates) live under ``docs/workbay/``.
DOCS_MIRROR_DIR = "docs/workbay"

# Common derived locations under the canonical docs mirror.
CONTRACTS_DIR = f"{DOCS_MIRROR_DIR}/contracts"
RULES_DIR = f"{DOCS_MIRROR_DIR}/rules"
TEMPLATES_DIR = f"{DOCS_MIRROR_DIR}/templates"
HARNESS_CONTRACT_RELPATH = Path(CONTRACTS_DIR) / "harness-protocol.yaml"
INSTRUCTIONS_RELPATH = Path(DOCS_MIRROR_DIR) / "instructions.md"

__all__ = [
    "BOOTSTRAP_MANIFEST_NAME",
    "CONTRACTS_DIR",
    "DOCS_MIRROR_DIR",
    "HARNESS_CONTRACT_RELPATH",
    "INSTRUCTIONS_RELPATH",
    "LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME",
    "LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME",
    "MANIFEST_NAME_PRECEDENCE",
    "RULES_DIR",
    "TEMPLATES_DIR",
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
