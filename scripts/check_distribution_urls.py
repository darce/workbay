#!/usr/bin/env python3
"""distribution-url-check (audit PPSSOT-URL-01/02): the public repo URL is
single-sourced in ``workbay_protocol.brand``.

Fails when:
  1. a package ``src`` module hardcodes the canonical GitHub URL as a string
     literal instead of importing it from ``brand`` (docstring/comment mentions
     of the URL are fine — only quoted string literals are flagged), or
  2. a release script's URL literal drifts from ``brand.REPO_URL`` (release
     scripts intentionally keep the literal to stay import-free, so this gate is
     their SSOT enforcement instead of an import).

Mirrors the fail-closed posture of ``scripts/check_brand.py`` / ``stack_pins.py``.
"""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BRAND_PY = REPO_ROOT / "packages" / "workbay-protocol" / "src" / "workbay_protocol" / "brand.py"

# A URL string literal = an opening quote immediately followed by the URL.
# Docstring/comment mentions embed the URL mid-text, so they are not matched.
# The https branch MUST consume the ``/`` before ``org/repo`` (the ssh branch's
# ``[:/]`` already does): without it the combined pattern demands
# ``github.com<org>`` and can never match an ``https://github.com/<org>/<repo>``
# literal — the exact form the git-only refactor removed from src (PPSSOT-URL).
SRC_LITERAL_PATTERN = r"[\"'](https://github\.com/|git@github\.com[:/])"


def _load_brand():
    """Import brand.py standalone (bypassing the workbay_protocol package
    __init__ + its transitive deps) — brand.py is pure stdlib."""
    spec = importlib.util.spec_from_file_location("_brand_ssot", BRAND_PY)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def main() -> int:
    brand = _load_brand()
    org_repo = re.escape(f"{brand.ORG}/{brand.REPO}")
    errors: list[str] = []

    # (1) No canonical-URL string literal in package src (import from brand instead).
    proc = subprocess.run(
        # Two pathspecs: ``**/*.py`` covers nested modules; ``*.py`` covers the
        # top-level single-file modules directly under ``src/`` (the two MCP
        # launchers, ``workbay_codex_bridge.py``) that ``**`` skips.
        [
            "git",
            "grep",
            "-nIE",
            SRC_LITERAL_PATTERN + org_repo,
            "--",
            "packages/*/src/**/*.py",
            "packages/*/src/*.py",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode > 1:
        raise RuntimeError(f"git grep failed (rc={proc.returncode}): {proc.stderr}")
    for line in proc.stdout.splitlines():
        if line.strip():
            errors.append(
                f"hardcoded repo-URL literal (import from workbay_protocol.brand instead): {line}"
            )

    # (2) release_public.py's intentional literal must equal brand.REPO_URL.
    release_public = REPO_ROOT / "scripts" / "release_public.py"
    text = release_public.read_text(encoding="utf-8")
    match = re.search(r'PUBLIC_GIT_REMOTE\s*=\s*"([^"]+)"', text)
    if match is None:
        errors.append("scripts/release_public.py: PUBLIC_GIT_REMOTE literal not found")
    elif match.group(1) != brand.REPO_URL:
        errors.append(
            f"scripts/release_public.py PUBLIC_GIT_REMOTE={match.group(1)!r} "
            f"drifted from brand.REPO_URL={brand.REPO_URL!r}"
        )

    if errors:
        sys.stderr.write("distribution-url-check: repo-URL SSOT violations:\n")
        for err in errors:
            sys.stderr.write(f"  {err}\n")
        return 1
    # Scope-honest: this gate enforces the SSOT for package ``src`` string
    # literals and release_public.py's intentional literal. Canonical-URL
    # mentions in docs/pyproject/Makefiles are out of scope (not claimed clean).
    print(
        "distribution-url-check: ok (no hardcoded repo-URL literal in packages/*/src; "
        "release_public.py matches brand.REPO_URL)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
