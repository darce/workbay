#!/usr/bin/env python3
"""Reject retired distribution-name launch grammar in consumer install docs.

WorkBay is released git-only (Dist-1): the public darce/workbay mirror and its
release tags are the delivery source (``docs/RELEASING.md``). Consumer install
docs must teach the git-sourced form with the runtime closure pinned to one tag.
This gate replaces manual scrub sweeps (release-it Steady State).

Deliberately narrow: scans only consumer-facing install surfaces. Historical
planning/assessment docs retain their references as a record and are out of
scope. The local editable dev step ``pip install -e ...`` is not matched.
Mirrors the fail-closed posture of ``scripts/check_brand.py``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from _distribution_scan import CONSUMER_INSTALL_DOCS

REPO_ROOT = Path(__file__).resolve().parents[1]

# ERE alternation. Each targets a retired distribution-name launch imperative,
# rather than descriptive prose about released git-only delivery.
FORBIDDEN = [
    r"From PyPI \(recommended\)",
    r"pip install (mcp-)?workbay[a-z-]*",  # bare dist install (not `pip install -e`)
    # A bare ``uvx <ourdist>`` launch omits the git source. The legal git form is
    # ``uvx --from "git+…" <ourdist>`` (``uvx`` is followed by ``--from``, never
    # by the dist name), so this only fires on the bare-name form — pinned (``@``) or
    # not. Dropping the old required trailing ``@`` closes the un-pinned
    # ``uvx workbay-bootstrap …`` false-negative.
    r"uvx (mcp-)?workbay[a-z-]*",
    r"pip resolves it from PyPI",
]
# Consumer-facing install surfaces — the shared set every distribution gate
# scans (README, CONSUMER, UPGRADING, RELEASING, package READMEs). UPGRADING /
# RELEASING are consumer-facing too; omitting them let retired distribution-name launch
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
            "distribution-prose-check: retired distribution-name launch grammar in a consumer "
            "install doc — WorkBay is released git-only from darce/workbay release tags; teach the git-sourced install "
            '(`uv tool install --no-sources "git+https://github.com/darce/workbay'
            '.git@<tag>#subdirectory=packages/<pkg>" <pkg>`) instead:\n'
        )
        for hit in hits:
            sys.stderr.write(f"  {hit}\n")
        return 1
    print(
        "distribution-prose-check: ok (released git-only from darce/workbay tags; no retired launch grammar in install docs)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
