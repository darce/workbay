"""Resolve every `docs/workbay/rules/*.md` path cited from a hook script.

Hook scripts print these citations at the moment of operator frustration
(blocked commit / blocked push / blocked PreToolUse). A 404 there is the
worst possible time for the link to be wrong. implementation note BR-R2-03 sweep:
every cited rules-doc path must resolve to a file we own, and any
in-doc anchor cited as `#fragment` must exist as a heading in the file.

Resolution mirrors `workbay_bootstrap.install._resolve_in_clone`: the
canonical home is `packages/workbay-system/docs/workbay/rules/`; a
monorepo-root fallback is allowed for legacy hoisted layouts. The
package-tree location is what consumer repos see after bootstrap
materializes the symlink under `<consumer>/docs/workbay/rules/`.

Hoisting note: `docs/workbay/rules` is NOT in `workbay-bootstrap`'s
`SHARED_SURFACES` yet, so consumer repos installing via bootstrap do
not receive this file today. Wider hoisting is a separate scope.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
HOOKS_DIRS = (
    REPO_ROOT / "packages" / "workbay-system" / "scripts" / "hooks",
    REPO_ROOT / "packages" / "workbay-system" / ".github" / "hooks",
)
# Mirror of workbay_bootstrap.install._resolve_in_clone: probe the
# package tree first (the canonical hoist source), fall back to the
# clone root for legacy hoisted overlays.
RULES_SEARCH_ROOTS = (
    REPO_ROOT / "packages" / "workbay-system" / "docs" / "workbay" / "rules",
    REPO_ROOT / "docs" / "workbay" / "rules",
)


def _resolve_rules_path(rel: str) -> Path | None:
    for base in RULES_SEARCH_ROOTS:
        candidate = base / rel
        if candidate.exists():
            return candidate
    return None

# Match: docs/workbay/rules/<name>.md   or   docs/workbay/rules/<name>.md#anchor
_CITATION_RE = re.compile(
    r"docs/workbay/rules/(?P<rel>[A-Za-z0-9_\-./]+\.md)(?:#(?P<anchor>[A-Za-z0-9_\-]+))?"
)


def _slugify_heading(heading: str) -> str:
    """GitHub-style heading -> anchor slug (lowercase, spaces -> dashes, drop punctuation)."""
    text = heading.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text


def _collect_anchors(md_path: Path) -> set[str]:
    anchors: set[str] = set()
    for line in md_path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^#{1,6}\s+(.+?)\s*$", line)
        if m:
            anchors.add(_slugify_heading(m.group(1)))
    return anchors


def _collect_citations() -> list[tuple[Path, int, str, str | None]]:
    """Return list of (source_file, line_number, rel_path, anchor_or_none)."""
    found: list[tuple[Path, int, str, str | None]] = []
    for hooks_dir in HOOKS_DIRS:
        if not hooks_dir.exists():
            continue
        for path in sorted(hooks_dir.rglob("*.py")):
            if path.name.startswith("test_"):
                continue
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                for m in _CITATION_RE.finditer(line):
                    found.append((path, lineno, m.group("rel"), m.group("anchor")))
    return found


def test_hook_citations_exist() -> None:
    citations = _collect_citations()
    assert citations, (
        "expected at least one docs/workbay/rules/*.md citation in hook "
        "scripts; the regex or hook layout may have moved"
    )
    missing: list[str] = []
    for source, lineno, rel, _anchor in citations:
        if _resolve_rules_path(rel) is None:
            missing.append(f"{source.relative_to(REPO_ROOT)}:{lineno} -> docs/workbay/rules/{rel}")
    assert not missing, (
        "hook scripts cite docs/workbay/rules/*.md paths that do not "
        "resolve under either the package tree "
        "(packages/workbay-system/docs/workbay/rules/) or the clone root "
        "(docs/workbay/rules/). Operators see these citations on a "
        "blocked commit/push/PreToolUse — a 404 here is the worst time "
        "for a broken link.\n\nMissing:\n  " + "\n  ".join(missing)
    )


def test_hook_citation_anchors_exist() -> None:
    citations = [c for c in _collect_citations() if c[3] is not None]
    if not citations:
        pytest.skip("no anchored citations to check")
    by_file: dict[Path, set[str]] = {}
    bad: list[str] = []
    for source, lineno, rel, anchor in citations:
        assert anchor is not None
        target = _resolve_rules_path(rel)
        if target is None:
            continue  # the path-existence test owns this failure mode
        if target not in by_file:
            by_file[target] = _collect_anchors(target)
        if anchor not in by_file[target]:
            bad.append(
                f"{source.relative_to(REPO_ROOT)}:{lineno} -> "
                f"docs/workbay/rules/{rel}#{anchor} (anchor not found)"
            )
    assert not bad, (
        "hook scripts cite anchors that no heading in the target rules "
        "doc resolves to. Add the heading or fix the citation:\n  "
        + "\n  ".join(bad)
    )
