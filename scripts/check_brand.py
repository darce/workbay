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


class BrandCheckConfigError(RuntimeError):
    """Raised when brand-tokens.toml is missing or malformed.

    Carries a clean, actionable message so the close gate fails closed with a
    brand-check diagnostic rather than a raw traceback (implementation note L1).
    """


def _load_config() -> dict:
    try:
        raw = TOKENS_PATH.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise BrandCheckConfigError(
            f"brand-check: config not found at {TOKENS_PATH} — restore "
            "config/brand/brand-tokens.toml (it ships alongside this script)."
        ) from exc
    try:
        cfg = tomllib.loads(raw)
    except tomllib.TOMLDecodeError as exc:
        raise BrandCheckConfigError(
            f"brand-check: config at {TOKENS_PATH} is malformed TOML: {exc}"
        ) from exc
    try:
        forbidden = cfg["forbidden"]["tokens"]
        scan_globs = cfg["scan"]["globs"]
        allowlist = cfg.get("allowlist", {})
        allow_globs = allowlist.get("globs", [])
        inline_marker = allowlist.get("inline_marker", "")
    except (KeyError, TypeError, AttributeError) as exc:
        raise BrandCheckConfigError(
            "brand-check: config schema invalid; expected [forbidden].tokens and [scan].globs lists"
        ) from exc
    if (
        not isinstance(forbidden, list)
        or not all(isinstance(item, str) and item for item in forbidden)
        or not isinstance(scan_globs, list)
        or not all(isinstance(item, str) and item for item in scan_globs)
        or not isinstance(allow_globs, list)
        or not all(isinstance(item, str) and item for item in allow_globs)
        or not isinstance(inline_marker, str)
    ):
        raise BrandCheckConfigError(
            "brand-check: config schema invalid; token, scan, and allowlist entries must be strings"
        )
    return cfg


def scan(repo_root: Path = REPO_ROOT) -> list[str]:
    """Return ``path:line:text`` hits of forbidden tokens in scanned source."""
    cfg = _load_config()
    forbidden: list[str] = cfg["forbidden"]["tokens"]
    scan_globs: list[str] = cfg["scan"]["globs"]
    allow_globs: list[str] = cfg.get("allowlist", {}).get("globs", [])
    inline_marker: str = cfg.get("allowlist", {}).get("inline_marker", "")

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
        # Line-level escape hatch for a load-bearing literal that must stay verbatim
        # (e.g. a legacy on-disk name a migration guard has to string-match). The
        # marker is carried in the source line itself, so the exemption is visible at
        # the hit site rather than hidden in a path glob that would exempt the whole
        # file and mask future accidental drift.
        if inline_marker and inline_marker in line:
            continue
        hits.append(line)
    return hits


def main() -> int:
    try:
        cfg = _load_config()
        pattern = "|".join(cfg["forbidden"]["tokens"])
        hits = scan()
    except BrandCheckConfigError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    except RuntimeError as exc:
        sys.stderr.write(f"brand-check: scan failed: {exc}\n")
        return 1
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
