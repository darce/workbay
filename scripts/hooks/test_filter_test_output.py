"""Tests for the filter-test-output PostToolUse hook."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

HOOK = Path(__file__).parent / "filter-test-output.py"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_hook_raw(payload: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=5,
    )


def run_hook(payload: dict) -> dict:
    """Run the hook with a schema-valid *payload* and return parsed JSON stdout.

    Schema-clean payloads must not trip the workbay-protocol drift
    warning; if they do, either the payload is missing a required
    protocol field or the helper started rejecting a previously
    accepted shape — both are regressions worth seeing in CI.
    """
    result = _run_hook_raw(payload)
    assert result.returncode == 0, f"Hook exited {result.returncode}: {result.stderr}"
    assert "[hook-protocol]" not in result.stderr, (
        f"protocol drift on schema-valid payload: {result.stderr!r}"
    )
    return json.loads(result.stdout)


def run_hook_lenient(payload: dict) -> dict:
    """Run the hook with an intentionally malformed *payload*.

    Some edge cases pass payloads that violate the hook event schema
    (string tool_input, missing fields, empty dict). The validate_event
    helper logs a stderr warning but does not block; tests use this
    runner to exercise those tolerance paths without false-positive
    drift assertions.
    """
    result = _run_hook_raw(payload)
    assert result.returncode == 0, f"Hook exited {result.returncode}: {result.stderr}"
    return json.loads(result.stdout)


def make_bash_payload(command: str, stdout: str, exit_code: int = 0) -> dict:
    return {
        "hook_event_name": "PostToolUse",
        "session_id": "test-session",
        "tool_name": "Bash",
        "tool_input": {"command": command},
        "tool_response": {
            "stdout": stdout,
            "stderr": "",
            "exitCode": exit_code,
        },
    }


def get_context(response: dict) -> str | None:
    hso = response.get("hookSpecificOutput")
    if hso:
        return hso.get("additionalContext")
    return None


# ---------------------------------------------------------------------------
# Non-test commands should no-op
# ---------------------------------------------------------------------------


class TestNonTestCommands:
    def test_ls_command(self):
        resp = run_hook(make_bash_payload("ls -la", "total 42\ndrwxr-xr-x ..."))
        assert get_context(resp) is None

    def test_git_status(self):
        resp = run_hook(make_bash_payload("git status", "On branch main"))
        assert get_context(resp) is None

    def test_empty_payload(self):
        # Empty payload violates the hook event schema; the protocol
        # helper logs but does not block.
        resp = run_hook_lenient({})
        assert get_context(resp) is None


# ---------------------------------------------------------------------------
# pytest: all passing
# ---------------------------------------------------------------------------


PYTEST_ALL_PASS = textwrap.dedent("""\
    ============================= test session starts ==============================
    platform darwin -- Python 3.12.4, pytest-8.3.4
    rootdir: /repo
    collected 47 items

    tests/test_core.py ..............................                         [63%]
    tests/test_api.py .................                                       [100%]

    ============================== 47 passed in 3.21s ==============================
""")


class TestPytestAllPassing:
    def test_summary_injected(self):
        resp = run_hook(make_bash_payload("make test-handoff", PYTEST_ALL_PASS))
        ctx = get_context(resp)
        assert ctx is not None
        assert "47 passed" in ctx
        assert "3.21" in ctx
        assert "all green" in ctx

    def test_no_failure_details(self):
        resp = run_hook(make_bash_payload("pytest tests/", PYTEST_ALL_PASS))
        ctx = get_context(resp)
        assert "FAILED" not in ctx


# ---------------------------------------------------------------------------
# pytest: with failures
# ---------------------------------------------------------------------------


PYTEST_WITH_FAILURES = textwrap.dedent("""\
    ============================= test session starts ==============================
    platform darwin -- Python 3.12.4, pytest-8.3.4
    rootdir: /repo
    collected 50 items

    tests/test_core.py ..............................                         [60%]
    tests/test_api.py .............F..F                                       [94%]
    tests/test_cli.py ...                                                    [100%]

    ================================== FAILURES ===================================
    _____________________________ test_create_widget ______________________________

        def test_create_widget():
            result = api.create_widget(name="test")
    >       assert result.status == "created"
    E       AssertionError: assert 'pending' == 'created'

    tests/test_api.py:42: AssertionError
    _____________________________ test_delete_widget ______________________________

        def test_delete_widget():
            result = api.delete_widget(id=99)
    >       assert result is not None
    E       AssertionError: assert None is not None

    tests/test_api.py:58: AssertionError
    =========================== short test summary info ============================
    FAILED tests/test_api.py::test_create_widget - AssertionError: assert 'pending'
    FAILED tests/test_api.py::test_delete_widget - AssertionError: assert None
    ========================= 2 failed, 48 passed in 4.56s ========================
""")


class TestPytestWithFailures:
    def test_summary_shows_failure_count(self):
        resp = run_hook(make_bash_payload("make test-handoff", PYTEST_WITH_FAILURES))
        ctx = get_context(resp)
        assert ctx is not None
        assert "2 FAILED" in ctx
        assert "48 passed" in ctx

    def test_failure_names_included(self):
        resp = run_hook(make_bash_payload("pytest", PYTEST_WITH_FAILURES))
        ctx = get_context(resp)
        assert "test_create_widget" in ctx
        assert "test_delete_widget" in ctx

    def test_no_all_green_message(self):
        resp = run_hook(make_bash_payload("pytest", PYTEST_WITH_FAILURES))
        ctx = get_context(resp)
        assert "all green" not in ctx


# ---------------------------------------------------------------------------
# pytest: with errors
# ---------------------------------------------------------------------------


PYTEST_WITH_ERRORS = textwrap.dedent("""\
    ============================= test session starts ==============================
    collected 10 items

    ================================== ERRORS =====================================
    _____________________ ERROR collecting tests/test_broken.py ____________________
    ImportError: cannot import name 'missing_func' from 'mymodule'
    =========================== short test summary info ============================
    ========================= 1 error in 0.45s ====================================
""")


class TestPytestWithErrors:
    def test_error_detected(self):
        resp = run_hook(make_bash_payload("pytest tests/", PYTEST_WITH_ERRORS))
        ctx = get_context(resp)
        assert ctx is not None
        assert "1 errors" in ctx or "1 error" in ctx
        assert "all green" not in ctx


# ---------------------------------------------------------------------------
# pytest: with warnings and skips
# ---------------------------------------------------------------------------


PYTEST_MIXED = textwrap.dedent("""\
    ============================= test session starts ==============================
    collected 100 items

    tests/test_all.py ....s...s..........s........................................ [50%]
    tests/test_more.py ...................................................       [100%]

    ================= 97 passed, 3 skipped, 12 warnings in 8.92s =================
""")


class TestPytestMixed:
    def test_skipped_and_warnings_shown(self):
        resp = run_hook(make_bash_payload("make test-handoff", PYTEST_MIXED))
        ctx = get_context(resp)
        assert "97 passed" in ctx
        assert "3 skipped" in ctx
        assert "12 warnings" in ctx
        assert "all green" in ctx  # no failures = all green


# ---------------------------------------------------------------------------
# phpunit: passing
# ---------------------------------------------------------------------------


PHPUNIT_PASS = textwrap.dedent("""\
    PHPUnit 10.5.2 by Sebastian Bergmann and contributors.

    ..............................................                        47 / 47 (100%)

    Time: 00:02.341, Memory: 24.00 MB

    OK (47 tests, 132 assertions)
""")


class TestPhpunitPassing:
    def test_summary_injected(self):
        resp = run_hook(make_bash_payload("make test-php", PHPUNIT_PASS))
        ctx = get_context(resp)
        assert ctx is not None
        assert "47 passed" in ctx
        assert "all green" in ctx


# ---------------------------------------------------------------------------
# phpunit: with failures
# ---------------------------------------------------------------------------


PHPUNIT_FAIL = textwrap.dedent("""\
    PHPUnit 10.5.2 by Sebastian Bergmann and contributors.

    .........F...............................F....                       47 / 47 (100%)

    FAILURES!

    Tests: 47, Assertions: 130, Failures: 2.
""")


class TestPhpunitFailing:
    def test_failure_count(self):
        resp = run_hook(make_bash_payload("phpunit tests/", PHPUNIT_FAIL))
        ctx = get_context(resp)
        assert ctx is not None
        assert "2 FAILED" in ctx
        assert "all green" not in ctx


# ---------------------------------------------------------------------------
# Command pattern detection
# ---------------------------------------------------------------------------


class TestCommandDetection:
    @pytest.mark.parametrize("cmd", [
        "make test-handoff",
        "make test-orchestrator",
        "pytest tests/test_core.py",
        "PYENV_VERSION=description-service pytest tests/",
        "cd packages/mcp-workbay-handoff && make test-handoff",
        "npm test",
        "npm run test",
        "npx vitest run",
        "phpunit tests/",
    ])
    def test_recognized_test_commands(self, cmd):
        from importlib.util import module_from_spec, spec_from_file_location
        spec = spec_from_file_location("hook", str(HOOK))
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.is_test_command(cmd), f"Should recognize: {cmd}"

    @pytest.mark.parametrize("cmd", [
        "ls -la",
        "git status",
        "cat foo.py",
        "make build",
        "python3 scripts/lint.py",
    ])
    def test_non_test_commands(self, cmd):
        from importlib.util import module_from_spec, spec_from_file_location
        spec = spec_from_file_location("hook", str(HOOK))
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert not mod.is_test_command(cmd), f"Should not recognize: {cmd}"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_malformed_json_stdin(self):
        result = subprocess.run(
            [sys.executable, str(HOOK)],
            input="not json",
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0
        assert json.loads(result.stdout) == {}

    def test_short_output_no_crash(self):
        resp = run_hook(make_bash_payload("pytest", "ok"))
        assert get_context(resp) is None

    def test_empty_stdout(self):
        resp = run_hook(make_bash_payload("pytest", ""))
        assert get_context(resp) is None

    def test_string_tool_response_is_safely_ignored(self):
        """Some harnesses emit a bare string for tool_response when the Bash
        invocation itself fails (timeout, process error). The hook must not
        crash — `(payload.get("tool_response") or {}).get(...)` was buggy
        because a non-empty string is truthy and fell through to .get(),
        raising AttributeError: 'str' object has no attribute 'get'.

        Uses ``run_hook_lenient`` because a bare-string tool_response
        violates the PostToolUse schema (Any | None still types as
        passthrough, but this case predates the helper)."""
        resp = run_hook_lenient({
            "hook_event_name": "PostToolUse",
            "session_id": "test-session",
            "tool_name": "Bash",
            "tool_input": {"command": "pytest -q"},
            "tool_response": "Bash command failed: timeout after 120s",
        })
        assert resp == {}

    def test_string_tool_input_is_safely_ignored(self):
        """Symmetric guard: tool_input must also be a dict."""
        resp = run_hook_lenient({
            "hook_event_name": "PostToolUse",
            "session_id": "test-session",
            "tool_name": "Bash",
            "tool_input": "pytest -q",
            "tool_response": {"stdout": "x" * 100, "stderr": "", "exitCode": 0},
        })
        assert resp == {}

    def test_non_string_command_is_safely_ignored(self):
        # tool_input is dict-typed at the schema level so this payload
        # IS schema-valid; only the inner command type is wrong.
        resp = run_hook({
            "hook_event_name": "PostToolUse",
            "session_id": "test-session",
            "tool_name": "Bash",
            "tool_input": {"command": 42},
            "tool_response": {"stdout": "x" * 100, "stderr": "", "exitCode": 0},
        })
        assert resp == {}

    def test_null_stdout_is_safely_ignored(self):
        resp = run_hook({
            "hook_event_name": "PostToolUse",
            "session_id": "test-session",
            "tool_name": "Bash",
            "tool_input": {"command": "pytest -q"},
            "tool_response": {"stdout": None, "stderr": "", "exitCode": 0},
        })
        assert resp == {}


# ---------------------------------------------------------------------------
# The rendered failure list must be truthful: one entry per real failure,
# and anything the caps drop has to be announced.
# ---------------------------------------------------------------------------


ELISION_MARKER = "not shown"


def pytest_failures_fixture(count: int) -> str:
    """Render pytest output carrying exactly *count* distinct failures."""
    blocks = []
    for i in range(count):
        name = f"test_generated_{i:03d}"
        blocks.append(
            f"_________________________ {name} _________________________\n"
            "\n"
            f"    def {name}():\n"
            ">       assert compute() == 1\n"
            "E       AssertionError: assert 0 == 1\n"
            "\n"
            f"tests/test_generated.py:{i + 1}: AssertionError"
        )
    return (
        "===================== test session starts ======================\n"
        f"collected {count + 36} items\n"
        "\n"
        "========================== FAILURES ============================\n"
        + "\n".join(blocks)
        + "\n"
        "==================== short test summary info ===================\n"
        f"=============== {count} failed, 36 passed in 783.12s ==========\n"
    )


def vitest_failures_fixture(count: int) -> str:
    lines = [f"FAIL  src/gen_{i:03d}.test.ts > case {i}" for i in range(count)]
    lines.append("")
    lines.append(f" Tests  {count} failed | 47 passed ({count + 47})")
    lines.append(" Duration  4.20s")
    return "\n".join(lines) + "\n"


def rendered_names(ctx: str, label: str = "FAILED") -> list[str]:
    marker = f"{label}:"
    return [
        line.split(marker, 1)[1].strip()
        for line in ctx.split("\n")
        if marker in line
    ]


class TestRenderedFailureListIsTruthful:
    def test_entry_count_matches_the_real_failure_count(self):
        ctx = get_context(run_hook(make_bash_payload("pytest", pytest_failures_fixture(2))))
        assert rendered_names(ctx) == ["test_generated_000", "test_generated_001"]

    def test_body_text_is_never_reported_as_a_failure_name(self):
        ctx = get_context(run_hook(make_bash_payload("pytest", pytest_failures_fixture(3))))
        names = rendered_names(ctx)
        assert not [n for n in names if n.startswith("def ")], names

    def test_every_name_survives_past_the_old_eight_entry_cap(self):
        ctx = get_context(run_hook(make_bash_payload("pytest", pytest_failures_fixture(25))))
        names = rendered_names(ctx)
        missing = [f"test_generated_{i:03d}" for i in range(25)
                   if f"test_generated_{i:03d}" not in names]
        assert not missing, f"names silently dropped: {missing}"

    def test_elision_marker_states_how_many_were_hidden(self):
        total = 80
        ctx = get_context(run_hook(make_bash_payload("pytest", pytest_failures_fixture(total))))
        shown = rendered_names(ctx)
        hidden = total - len(shown)
        assert hidden > 0, "fixture no longer exceeds the cap; raise total"
        assert ELISION_MARKER in ctx, "truncated the list without announcing it"
        assert f"{hidden} more" in ctx, f"marker miscounts; expected {hidden}, got: {ctx[-300:]}"
        assert str(total) in ctx

    def test_no_elision_marker_when_nothing_is_hidden(self):
        # Control: the marker must not be unconditional, or it says nothing.
        ctx = get_context(run_hook(make_bash_payload("pytest", pytest_failures_fixture(3))))
        assert ELISION_MARKER not in ctx

    def test_vitest_names_are_not_capped_before_rendering(self):
        ctx = get_context(run_hook(make_bash_payload("vitest run", vitest_failures_fixture(12))))
        names = rendered_names(ctx)
        missing = [f"gen_{i:03d}" for i in range(12)
                   if not any(f"gen_{i:03d}" in n for n in names)]
        assert not missing, f"vitest names dropped at parse time: {missing}"
