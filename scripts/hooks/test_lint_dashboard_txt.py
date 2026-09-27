"""Tests for scripts/hooks/lint-dashboard-txt.py (internal).

The guard forbids `DASHBOARD.md` from re-appearing in tracked non-archive
paths after the internal `DASHBOARD.md → DASHBOARD.txt` rename. It is
wired into `make check-all`. These tests exercise the in-process scan
function directly so they run fast in the `test-hooks` pytest target.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "hooks" / "lint-dashboard-txt.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("lint_dashboard_txt", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["lint_dashboard_txt"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_script_exists() -> None:
    assert SCRIPT_PATH.is_file(), f"missing guard script: {SCRIPT_PATH}"


def test_scan_paths_flags_non_archive_markdown_reference(tmp_path: Path) -> None:
    module = _load_module()
    offending = tmp_path / "docs" / "some-plan.md"
    offending.parent.mkdir(parents=True)
    offending.write_text("See DASHBOARD.md for the operator view.\n")

    violations = module.scan_paths([offending])
    assert violations, "guard must flag DASHBOARD.md in a non-archive tracked path"
    assert any("DASHBOARD.md" in v for v in violations)


def test_scan_paths_excludes_archive_plans(tmp_path: Path) -> None:
    module = _load_module()
    archived = tmp_path / "docs" / "tasks" / "archive" / "old-plan.md"
    archived.parent.mkdir(parents=True)
    archived.write_text("legacy DASHBOARD.md reference\n")

    violations = module.scan_paths([archived])
    assert not violations, "archived task plans must be excluded"


def test_scan_paths_excludes_test_fixtures(tmp_path: Path) -> None:
    module = _load_module()
    fixture = tmp_path / "packages" / "foo" / "tests" / "fixtures" / "sample.md"
    fixture.parent.mkdir(parents=True)
    fixture.write_text("DASHBOARD.md in a fixture does not count\n")

    violations = module.scan_paths([fixture])
    assert not violations, "test fixtures must be excluded"


def test_scan_paths_clean_file_returns_no_violations(tmp_path: Path) -> None:
    module = _load_module()
    clean = tmp_path / "docs" / "clean.md"
    clean.parent.mkdir(parents=True)
    clean.write_text("Operator view: see DASHBOARD.txt.\n")

    violations = module.scan_paths([clean])
    assert not violations


def test_is_excluded_recognizes_archive_and_fixture_patterns() -> None:
    module = _load_module()
    assert module.is_excluded(Path("docs/tasks/archive/something.md"))
    assert module.is_excluded(Path("packages/foo/tests/fixtures/x.md"))
    assert module.is_excluded(Path("packages/foo/test_fixtures/x.md"))
    assert module.is_excluded(Path("packages/foo/tests/test_rendering.py"))
    assert module.is_excluded(Path("docs/assessments/dashboard-md-vs-txt-drift.md"))
    assert module.is_excluded(Path(".gitignore"))
    # Guards that scan for the retired name must contain the literal verbatim.
    assert module.is_excluded(Path("scripts/check_harness_sync.py"))
    assert not module.is_excluded(Path("docs/tasks/17.0/internal-future-plan.md"))
    assert not module.is_excluded(Path("README.md"))
    assert not module.is_excluded(Path("docs/workbay/rules/development-workflow.md"))


@pytest.mark.parametrize(
    "filename",
    [
        "README.md",
        "docs/workbay/rules/branch-review-guide.md",
        "packages/mcp-workbay-handoff/README.md",
    ],
)
def test_current_repo_passes(filename: str) -> None:
    """The currently-tracked repo must pass the guard — regression anchor for CI."""
    module = _load_module()
    exit_code = module.main(["--repo-root", str(REPO_ROOT)])
    assert exit_code == 0, (
        f"repo state violates the DASHBOARD.md guard; a tracked non-archive file "
        f"re-introduced the old name (check near {filename} and similar)."
    )
