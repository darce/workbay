"""Canonical WorkBay brand / project-identity constants — single source of truth.

The *display* brand (name, slug, org, repo, URLs) derives from here so the
consolidate-able identity lives in one place: a future brand rename of these
surfaces is a one-line edit + regen, not a repo-wide sweep (implementation note §3 D2).

This is deliberately distinct from the **structural** package / dist / module /
dir names (``workbay_protocol``, ``mcp-workbay-handoff``, ``packages/workbay-*``):
those are import paths and packaging metadata that cannot read a runtime constant
(implementation note §2), so they stay literal and are covered by the brand-token map +
``make brand-check`` gate (D1), not by this module.
"""

from __future__ import annotations

BRAND_NAME = "WorkBay"
"""Human-facing display name."""

BRAND_SLUG = "workbay"
"""Lowercase brand token (dist prefix, on-disk path roots, URLs)."""

ORG = "darce"
"""GitHub org / owner of the public repo."""

REPO = "workbay"
"""Public GitHub repo name."""

REPO_URL = f"git@github.com:{ORG}/{REPO}.git"
"""Canonical SSH clone URL for the public repo (bootstrap ``DEFAULT_REMOTE_URL``)."""

REPO_HTTPS_URL = f"https://github.com/{ORG}/{REPO}"
"""Canonical HTTPS URL for the public repo."""

PYPI_URL = f"https://pypi.org/project/{BRAND_SLUG}"
"""Canonical PyPI project URL for the front-door distribution."""

__all__ = [
    "BRAND_NAME",
    "BRAND_SLUG",
    "ORG",
    "PYPI_URL",
    "REPO",
    "REPO_HTTPS_URL",
    "REPO_URL",
]
