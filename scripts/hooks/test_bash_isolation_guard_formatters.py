"""Regression tests for FU-01 formatter detection in _bash_isolation_guard.

Contract: when the Bash command matches a known in-place formatter pattern,
`scan_bash_command` reports every configured `code_roots` entry (and
`root_protected_files`) as blocked with a `<root>/ (formatter)` label, so the
caller (guard-bash-main-branch.py) surfaces the contract-backed violation.
Unrelated commands (read-only linters, `make test-*`, plain `ruff check`)
must NOT trigger the formatter branch.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

from _bash_isolation_guard import scan_bash_command  # noqa: E402
from _harness_protocol import BranchIsolationPolicy  # noqa: E402


def _policy() -> BranchIsolationPolicy:
    return BranchIsolationPolicy(
        code_roots=("apps/", "packages/", "scripts/", ".github/hooks/", ".claude/", "mk/"),
        protected_extensions=(".py", ".ts", ".tsx", ".js", ".jsx", ".php", ".sql", ".sh", ".css", ".scss", ".mk"),
        root_protected_files=("Makefile",),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
    )


def _assert_formatter_blocked(command: str) -> None:
    blocked = scan_bash_command(command, REPO_ROOT, _policy())
    labels = [b for b in blocked if b.endswith("(formatter)")]
    assert labels, f"expected formatter-labelled blocked entries for {command!r}, got {blocked!r}"
    # Every configured code_root surfaces in the output.
    for root in ("apps", "packages", "scripts", ".github/hooks", ".claude", "mk"):
        assert any(root in entry for entry in labels), (
            f"missing root `{root}` in formatter-blocked set for {command!r}: {labels!r}"
        )


def _assert_not_formatter(command: str) -> None:
    blocked = scan_bash_command(command, REPO_ROOT, _policy())
    labels = [b for b in blocked if b.endswith("(formatter)")]
    assert not labels, f"unexpected formatter flag for {command!r}: {labels!r}"


def test_make_format_all_blocked() -> None:
    _assert_formatter_blocked("make format-all")


def test_make_format_handoff_blocked() -> None:
    _assert_formatter_blocked("make format-handoff")


def test_make_fix_lint_handoff_blocked() -> None:
    _assert_formatter_blocked("make fix-lint-handoff")


def test_make_fix_php_style_blocked() -> None:
    _assert_formatter_blocked("make fix-php-style")


def test_ruff_format_blocked() -> None:
    _assert_formatter_blocked("ruff format packages/")


def test_ruff_check_fix_blocked() -> None:
    _assert_formatter_blocked("ruff check --fix packages/")


def test_ruff_check_readonly_not_blocked() -> None:
    _assert_not_formatter("ruff check packages/")


def test_black_blocked() -> None:
    _assert_formatter_blocked("black packages/")


def test_prettier_write_blocked() -> None:
    _assert_formatter_blocked("prettier --write apps/")


def test_prettier_short_w_blocked() -> None:
    _assert_formatter_blocked("prettier -w apps/**/*.ts")


def test_prettier_plain_blocked() -> None:
    # Bare `prettier` without --write is write-by-default in some setups; the
    # conservative registry still flags `prettier` because `None` matches verb
    # alone. Guarding bias: prefer false-positive over silent drift.
    _assert_formatter_blocked("prettier apps/")


def test_npm_run_format_blocked() -> None:
    _assert_formatter_blocked("npm run format")


def test_npm_run_lint_fix_blocked() -> None:
    _assert_formatter_blocked("npm run lint:fix")


def test_pnpm_run_fix_blocked() -> None:
    _assert_formatter_blocked("pnpm run fix")


def test_yarn_format_blocked() -> None:
    _assert_formatter_blocked("yarn format")


def test_composer_run_format_blocked() -> None:
    _assert_formatter_blocked("composer run format")


def test_composer_fix_style_blocked() -> None:
    _assert_formatter_blocked("composer fix-style")


def test_eslint_fix_blocked() -> None:
    _assert_formatter_blocked("eslint --fix apps/")


def test_eslint_readonly_not_blocked() -> None:
    _assert_not_formatter("eslint apps/")


def test_stylelint_fix_blocked() -> None:
    _assert_formatter_blocked("stylelint --fix apps/")


def test_make_test_not_formatter() -> None:
    _assert_not_formatter("make test-handoff")


def test_make_check_all_not_formatter() -> None:
    _assert_not_formatter("make check-all")


def test_npm_run_test_not_formatter() -> None:
    _assert_not_formatter("npm run test")


def test_pytest_not_formatter() -> None:
    _assert_not_formatter("pytest packages/mcp-workbay-handoff/tests/")


def test_env_prefix_does_not_confuse_detector() -> None:
    _assert_formatter_blocked("FOO=bar make format-all")


def test_sudo_prefix_does_not_confuse_detector() -> None:
    _assert_formatter_blocked("sudo ruff format .")


def test_chained_commands_trigger_on_formatter_stage() -> None:
    _assert_formatter_blocked("git status && ruff format packages/")


def test_chained_commands_no_trigger_when_no_formatter() -> None:
    _assert_not_formatter("git status && ruff check packages/")
