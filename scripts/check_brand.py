#!/usr/bin/env python3
"""brand-check (implementation note D1): fail if a forbidden prior-brand token reappears in
tracked production source outside the allowlist.

Driven by ``config/brand/brand-tokens.toml`` so a future brand rename is a codemod
over one map and stray legacy tokens are caught in CI. Scans the consolidate-able
production source only; docs / tests / the workbay-system payload that
intentionally carry historical brand names are out of scope or allowlisted
(implementation note §2/§5).
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from fnmatch import fnmatch
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TOKENS_PATH = REPO_ROOT / "config" / "brand" / "brand-tokens.toml"


def _load_config() -> dict:
    return tomllib.loads(TOKENS_PATH.read_text(encoding="utf-8"))


def scan(repo_root: Path = REPO_ROOT) -> list[str]:
    """Return ``path:line:text`` hits of forbidden tokens in scanned source."""
    cfg = _load_config()
    forbidden: list[str] = cfg["forbidden"]["tokens"]
    scan_globs: list[str] = cfg["scan"]["globs"]
    allow_globs: list[str] = cfg.get("allowlist", {}).get("globs", [])

    proc = subprocess.run(
        ["git", "grep", "-nIE", "|".join(forbidden), "--", *scan_globs],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    # git grep: rc 0 = matches found, rc 1 = no matches (clean), rc >1 = error.
    if proc.returncode > 1:
        raise RuntimeError(f"git grep failed (rc={proc.returncode}): {proc.stderr}")

    hits: list[str] = []
    for line in proc.stdout.splitlines():
        path = line.split(":", 1)[0]
        if any(fnmatch(path, glob) for glob in allow_globs):
            continue
        hits.append(line)
    return hits


def main() -> int:
    cfg = _load_config()
    pattern = "|".join(cfg["forbidden"]["tokens"])
    hits = scan()
    if hits:
        sys.stderr.write(
            "brand-check: forbidden prior-brand token(s) in tracked source — rename "
            "via config/brand/brand-tokens.toml + a codemod, or allowlist it if the "
            "reference is intentional history:\n" + "\n".join(hits) + "\n"
        )
        return 1
    print(f"brand-check: OK — no forbidden tokens [{pattern}] in scanned source")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
