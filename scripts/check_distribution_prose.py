#!/usr/bin/env python3
"""distribution-prose-check (audit PPSSOT-DISTGATE-01 / PYPI-PROSE-*): fail if a
consumer-facing install doc reintroduces the retired PyPI launch grammar.

WorkBay is git-only (Dist-1: one channel, the public git mirror; no PyPI upload;
``docs/RELEASING.md``). Consumer install docs must teach the git-sourced form,
never ``From PyPI (recommended)`` / ``pip install <ourdist>`` / a PyPI-pinned
``uvx <ourdist>@`` launcher. This is the machine token that replaces the manual
scrub sweeps (release-it Steady State).

Deliberately narrow: scans only the consumer-facing install surfaces (root
README, CONSUMER, package READMEs). Historical planning/assessment docs keep
their PyPI references as a record and are out of scope. The local editable dev
step ``pip install -e ...`` is NOT matched (it is a local install, not PyPI).
Mirrors the fail-closed posture of ``scripts/check_brand.py``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from _distribution_scan import CONSUMER_INSTALL_DOCS

REPO_ROOT = Path(__file__).resolve().parents[1]

# ERE alternation. Each targets a retired *PyPI launch* imperative, not prose
# that merely mentions PyPI (e.g. "PyPI is retired", "no PyPI" stay legal).
FORBIDDEN = [
    r"From PyPI \(recommended\)",
    r"pip install (mcp-)?workbay[a-z-]*",  # install our dist FROM PyPI (not `pip install -e`)
    # A bare ``uvx <ourdist>`` launch resolves from PyPI. The legal git form is
    # ``uvx --from "git+…" <ourdist>`` (``uvx`` is followed by ``--from``, never
    # by the dist name), so this only fires on the PyPI form — pinned (``@``) or
    # not. Dropping the old required trailing ``@`` closes the un-pinned
    # ``uvx workbay-bootstrap …`` false-negative.
    r"uvx (mcp-)?workbay[a-z-]*",
    r"pip resolves it from PyPI",
]
# Consumer-facing install surfaces — the shared set every distribution gate
# scans (README, CONSUMER, UPGRADING, RELEASING, package READMEs). UPGRADING /
# RELEASING are consumer-facing too; omitting them let retired PyPI launch
# grammar survive there past the git-only cutover (PPSSOT-DISTGATE / HARM).
SCAN_GLOBS = CONSUMER_INSTALL_DOCS


def scan(repo_root: Path = REPO_ROOT) -> list[str]:
    proc = subprocess.run(
        ["git", "grep", "-nIE", "|".join(FORBIDDEN), "--", *SCAN_GLOBS],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    # git grep: rc 0 = matches found, rc 1 = no matches (clean), rc >1 = error.
    if proc.returncode > 1:
        raise RuntimeError(f"git grep failed (rc={proc.returncode}): {proc.stderr}")
    return [line for line in proc.stdout.splitlines() if line.strip()]


def main() -> int:
    hits = scan()
    if hits:
        sys.stderr.write(
            "distribution-prose-check: retired PyPI launch grammar in a consumer "
            "install doc — WorkBay is git-only; teach the git-sourced install "
            '(`uv tool install --no-sources "git+https://github.com/darce/workbay'
            '.git@<tag>#subdirectory=packages/<pkg>" <pkg>`) instead:\n'
        )
        for hit in hits:
            sys.stderr.write(f"  {hit}\n")
        return 1
    print("distribution-prose-check: ok (no retired PyPI launch grammar in install docs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
