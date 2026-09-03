"""Drift guard: the git-hook literal branch-grammar fallback must stay byte-for-byte
equal to the canonical ``workbay_protocol.branch_naming.TASK_REF_RE``.

``check_branch_naming._load_task_ref_re`` resolves the canonical grammar via a
file-walk / import chain so the post-checkout, pre-commit and pre-push gates keep
working under plain ``python3`` before the MCP packages are importable. Its final
leg is ``_fallback_task_ref_re()`` — a *literal copy* of the canonical regex. The
canonical module documents itself as the SOLE owner of the grammar, so a silent
drift between the literal copy and canonical would make hooks classify branches
differently from every other gate without any test noticing (the existing
classifier tests always resolve the real canonical module).

This pins ``_fallback_task_ref_re()`` to canonical and exercises the fallback
behaviorally. NOTE: the sibling ``_branch_isolation_guard.py`` carries the same
literal copy inline (not via a named function); pinning it too would require
exposing a ``_fallback_task_ref_re`` there — tracked as a follow-up.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

_HOOKS_DIR = Path(__file__).resolve().parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register before exec so dataclass/annotation machinery in the loaded
    # module can resolve ``cls.__module__`` via sys.modules (mirrors the
    # production hook loader).
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _canonical_branch_naming():
    """Load the canonical ``branch_naming`` module by file path (the same walk
    the hook uses), skipping if the source tree is not reachable."""
    for parent in _HOOKS_DIR.parents:
        candidate = (
            parent
            / "packages"
            / "workbay-protocol"
            / "src"
            / "workbay_protocol"
            / "branch_naming.py"
        )
        if candidate.is_file():
            return _load_module("_canonical_branch_naming_for_drift_test", candidate)
    pytest.skip("canonical workbay_protocol.branch_naming source not reachable")


def _check_branch_naming():
    return _load_module(
        "_check_branch_naming_for_drift_test",
        _HOOKS_DIR / "check_branch_naming.py",
    )


def test_fallback_regex_matches_canonical_task_ref_re() -> None:
    """The literal hook fallback must equal canonical TASK_REF_RE pattern + flags."""
    canonical = _canonical_branch_naming().TASK_REF_RE
    fallback = _check_branch_naming()._fallback_task_ref_re()

    assert isinstance(canonical, re.Pattern)
    assert fallback.pattern == canonical.pattern
    assert fallback.flags == canonical.flags


@pytest.mark.parametrize(
    ("branch", "conforming"),
    [
        ("feature/internal-37", True),
        ("feature/internal-37-slice3-post-checkout", True),
        ("feature/maint-dirty-br-01", True),
        ("feature/no-digits-here", False),  # no digit anywhere
        ("feature/foo", False),  # single segment
        ("feature/123-foo", False),  # leading digit
        ("main", False),  # not a feature/ branch
    ],
)
def test_fallback_regex_classifies_known_branches(
    branch: str, conforming: bool
) -> None:
    """The fallback (used when canonical is unreachable) must classify branches
    identically to canonical — exercised directly so the fallback leg is not
    dead code behaviorally."""
    fallback = _check_branch_naming()._fallback_task_ref_re()
    assert (fallback.match(branch) is not None) is conforming
