"""Tests for the task-plan finding-list guard hook.

Run with: ``python3 -m pytest scripts/hooks/test_guard_task_plan_findings.py``
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOK_SCRIPT = Path(__file__).parent / "guard-task-plan-findings.py"

_spec = importlib.util.spec_from_file_location("guard_task_plan_findings", HOOK_SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

_detect_finding_runs = _mod._detect_finding_runs
_path_should_be_scanned = _mod._path_should_be_scanned
_detect_revision_history_blocks = _mod._detect_revision_history_blocks
_git_repo_root = _mod._git_repo_root



# ---------------------------------------------------------------------------
# _git_repo_root — harness workspace fallback (implementation note / REV2-C-001)
# ---------------------------------------------------------------------------


def test_git_repo_root_falls_back_to_grok_when_git_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git_fail(command, *args, **kwargs):
        return subprocess.CompletedProcess(command, 128, "", "fatal: not a git repo")

    monkeypatch.setattr(_mod.subprocess, "run", fake_git_fail)
    monkeypatch.delenv("CLAUDE_PROJECT_DIR", raising=False)
    monkeypatch.setenv("GROK_WORKSPACE_ROOT", "/tmp/grok-ws")

    assert _git_repo_root() == "/tmp/grok-ws"


def test_hook_scans_repo_relative_paths_when_git_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: Grok exports GROK_WORKSPACE_ROOT but git may be absent."""
    ws = tmp_path / "workspace"
    target = ws / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)

    monkeypatch.delenv("CLAUDE_PROJECT_DIR", raising=False)
    monkeypatch.setenv("GROK_WORKSPACE_ROOT", str(ws))

    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "## Findings\n\n"
                "- **H-1**: severity high finding\n"
                "- **M-2**: severity medium finding\n"
                "- **L-3**: severity low finding\n"
            ),
        }
    }
    # Run from a non-git cwd so the hook subprocess cannot git-rev-parse.
    rc, stderr = _run_hook(payload, cwd=str(tmp_path))
    assert rc == 2, f"expected hook to block, got rc={rc}, stderr={stderr!r}"
    assert "H-1" in stderr


# ---------------------------------------------------------------------------
# _detect_finding_runs — heuristic correctness
# ---------------------------------------------------------------------------


def test_detects_three_consecutive_task_prefixed_findings() -> None:
    text = (
        "## Findings\n"
        "\n"
        "- internal: Description here.\n"
        "- internal: Another finding.\n"
        "- internal: Yet another.\n"
    )
    runs = _detect_finding_runs(text)
    assert len(runs) == 1
    start_line, ids = runs[0]
    assert start_line == 3
    assert ids == ["internal", "internal", "internal"]


def test_detects_three_consecutive_severity_shorthand_findings() -> None:
    text = (
        "- **H-1**: severity high finding\n"
        "- **M-2**: severity medium finding\n"
        "- **L-3**: severity low finding\n"
    )
    runs = _detect_finding_runs(text)
    assert len(runs) == 1
    assert runs[0][1] == ["H-1", "M-2", "L-3"]


def test_two_findings_do_not_trigger() -> None:
    text = (
        "- H-1: only two\n"
        "- M-2: of these\n"
        "- not a finding bullet\n"
    )
    assert _detect_finding_runs(text) == []


def test_inline_mention_does_not_trigger() -> None:
    text = (
        "We fixed internal in this commit.\n"
        "\n"
        "The reviewer flagged H-1 and L-3 as related, but only H-1 blocks merge.\n"
    )
    assert _detect_finding_runs(text) == []


def test_continuation_lines_do_not_break_run() -> None:
    text = (
        "- internal: Description that wraps\n"
        "  onto a continuation line.\n"
        "- internal: Another finding\n"
        "  with more detail.\n"
        "- internal: Final one.\n"
    )
    runs = _detect_finding_runs(text)
    assert len(runs) == 1
    assert runs[0][1] == ["internal", "internal", "internal"]


def test_blank_line_resets_run() -> None:
    text = (
        "- internal: First.\n"
        "- internal: Second.\n"
        "\n"
        "- internal: Third (separate run).\n"
    )
    assert _detect_finding_runs(text) == []


def test_non_finding_bullet_resets_run() -> None:
    text = (
        "- internal: First.\n"
        "- internal: Second.\n"
        "- Some other note.\n"
        "- internal: Third.\n"
    )
    assert _detect_finding_runs(text) == []


def test_em_dash_separator_matches() -> None:
    text = (
        "- internal \u2014 dash separator\n"
        "- internal \u2014 dash separator\n"
        "- internal \u2014 dash separator\n"
    )
    runs = _detect_finding_runs(text)
    assert len(runs) == 1


def test_e_prefixed_epic_id_matches() -> None:
    text = (
        "- DEMO-7-BR-01: epic-prefixed task finding\n"
        "- DEMO-7-BR-02: another\n"
        "- DEMO-7-BR-03: a third\n"
    )
    runs = _detect_finding_runs(text)
    assert len(runs) == 1


def test_finds_multiple_independent_runs() -> None:
    text = (
        "- internal: first run a\n"
        "- internal: first run b\n"
        "- internal: first run c\n"
        "\n"
        "Some prose between.\n"
        "\n"
        "- H-1: second run a\n"
        "- H-2: second run b\n"
        "- H-3: second run c\n"
    )
    runs = _detect_finding_runs(text)
    assert len(runs) == 2


# ---------------------------------------------------------------------------
# _path_should_be_scanned — path filter
# ---------------------------------------------------------------------------


def test_path_filter_includes_docs_tasks() -> None:
    assert _path_should_be_scanned("docs/tasks/12.0/foo.md")


def test_path_filter_includes_packages_docs_tasks() -> None:
    assert _path_should_be_scanned(
        "packages/mcp-workbay-orchestrator/docs/tasks/internal-tool-surface-consolidation-task-plan.md"
    )


def test_path_filter_includes_docs_epics() -> None:
    assert _path_should_be_scanned("docs/epics/v0.4.0/public-demo.md")


def test_path_filter_includes_task_plan_filename_anywhere() -> None:
    assert _path_should_be_scanned("packages/foo/some-task-plan.md")


def test_path_filter_excludes_claude_md() -> None:
    assert not _path_should_be_scanned("CLAUDE.md")


def test_path_filter_excludes_instructions_md() -> None:
    assert not _path_should_be_scanned("docs/workbay/instructions.md")


def test_path_filter_excludes_changelog() -> None:
    assert not _path_should_be_scanned("packages/mcp-workbay-handoff/CHANGELOG.md")


def test_path_filter_excludes_non_markdown() -> None:
    assert not _path_should_be_scanned("foo.py")
    assert not _path_should_be_scanned("docs/tasks/12.0/foo.txt")


def test_path_filter_excludes_docs_plans_even_when_glob_would_match() -> None:
    # internal: legacy numbered plans under docs/plans/** are historical
    # planning drafts and out of scope for both detectors, even when the
    # filename happens to end in -plan.md.
    assert not _path_should_be_scanned("docs/plans/0009-historical-plan.md")
    assert not _path_should_be_scanned("docs/plans/0010-frictionless-receipts.md")


# ---------------------------------------------------------------------------
# end-to-end stdin invocation (Claude Code hook protocol)
# ---------------------------------------------------------------------------


def _run_hook(payload: dict, cwd: str | None = None) -> tuple[int, str]:
    tool_input = payload.get("tool_input") if isinstance(payload.get("tool_input"), dict) else {}
    default_tool_name = "Edit" if "new_string" in tool_input else "Write"
    hook_payload = dict(payload)
    hook_payload.setdefault("hook_event_name", "PreToolUse")
    hook_payload.setdefault("session_id", "test-session")
    hook_payload.setdefault("tool_name", default_tool_name)

    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(hook_payload),
        capture_output=True,
        text=True,
        timeout=5,
        cwd=cwd,
    )
    return proc.returncode, proc.stderr


def test_hook_blocks_write_with_finding_list(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "## Findings\n\n"
                "- internal: First.\n"
                "- internal: Second.\n"
                "- internal: Third.\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "Pasted review-finding list detected" in stderr
    assert "internal" in stderr
    assert "review_findings" in stderr  # actionable hint


def test_hook_allows_write_without_finding_list(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# Task Plan\n\n"
                "## implementation note\n"
                "- Implement foo\n"
                "- Test bar\n"
                "- Document baz\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 0
    assert stderr == ""


def test_hook_allows_write_to_unscoped_path(tmp_path: Path) -> None:
    target = tmp_path / "CLAUDE.md"
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "## Findings\n\n"
                "- internal: First.\n"
                "- internal: Second.\n"
                "- internal: Third.\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 0


def test_hook_blocks_edit_with_new_string(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "old_string": "placeholder",
            "new_string": (
                "- **H-1**: severity high finding\n"
                "- **M-2**: severity medium finding\n"
                "- **L-3**: severity low finding\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "H-1" in stderr


def test_hook_blocks_incremental_edit_completing_three_bullet_run(tmp_path: Path) -> None:
    """Regression for internal.

    A task plan that already contains two finding bullets must be unable to
    grow a third via an Edit whose ``new_string`` adds a single bullet. The
    hook must scan the post-edit document, not just the inserted fragment.
    """
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    target.write_text(
        "## Findings\n"
        "\n"
        "- internal: First.\n"
        "- internal: Second.\n"
        "- placeholder\n",
        encoding="utf-8",
    )
    payload = {
        "tool_input": {
            "file_path": str(target),
            "old_string": "- placeholder\n",
            "new_string": "- internal: Third.\n",
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2, f"expected hook to block, got rc={rc}, stderr={stderr!r}"
    assert "internal" in stderr
    assert "Pasted review-finding list detected" in stderr


def test_hook_allows_edit_that_does_not_form_finding_run(tmp_path: Path) -> None:
    """An Edit on a clean task plan that adds prose must still be allowed."""
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    target.write_text(
        "# Task Plan\n\n## implementation note\n- Implement foo\n- Test bar\n",
        encoding="utf-8",
    )
    payload = {
        "tool_input": {
            "file_path": str(target),
            "old_string": "- Test bar\n",
            "new_string": "- Test bar\n- Document baz\n",
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 0, f"expected hook to allow, got rc={rc}, stderr={stderr!r}"


def test_hook_falls_back_to_new_string_when_file_missing(tmp_path: Path) -> None:
    """When the target file doesn't exist, the hook still scans new_string."""
    target = tmp_path / "docs" / "tasks" / "12.0" / "does-not-exist-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "old_string": "placeholder",
            "new_string": (
                "- internal: a\n"
                "- internal: b\n"
                "- internal: c\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "internal" in stderr


def test_hook_replace_all_simulates_global_replacement(tmp_path: Path) -> None:
    """replace_all=True must apply to every occurrence in the simulated scan."""
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    target.write_text(
        "## A\n- TOK\n\n## B\n- TOK\n\n## C\n- TOK\n",
        encoding="utf-8",
    )
    payload = {
        "tool_input": {
            "file_path": str(target),
            "old_string": "- TOK",
            "new_string": "- internal: filled in",
            "replace_all": True,
        }
    }
    # Each TOK becomes a single finding bullet, but they sit in separate
    # sections so blank lines reset the run. None of the runs reach three
    # consecutive bullets, so the hook must allow.
    rc, stderr = _run_hook(payload)
    assert rc == 0, f"expected allow, got rc={rc}, stderr={stderr!r}"


# ---------------------------------------------------------------------------
# --scan-repo mode (regression for internal)
# ---------------------------------------------------------------------------


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )


def _init_fake_repo(root: Path) -> None:
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "Test")
    _git(root, "commit", "--allow-empty", "-m", "init", "-q")


def _run_hook_in(cwd: Path, *args: str) -> tuple[int, str, str]:
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT), *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=10,
    )
    return proc.returncode, proc.stdout, proc.stderr


def test_scan_repo_catches_task_plan_outside_hard_coded_directories(tmp_path: Path) -> None:
    """Regression for internal.

    A file matching the ``*task-plan*.md`` filename glob but living outside
    the four directories the old Makefile target hard-coded must still be
    swept by ``make lint-task-plans``. ``--scan-repo`` enumerates every
    tracked .md file via ``git ls-files`` and applies the same path filter
    the Claude Code hook uses.
    """
    _init_fake_repo(tmp_path)
    offending = (
        tmp_path
        / "packages"
        / "mcp-workbay-orchestrator"
        / "docs"
        / "tech-debt"
        / "orchestrator-chat-tui-task-plan.md"
    )
    offending.parent.mkdir(parents=True)
    offending.write_text(
        "# Plan\n\n## Findings\n\n"
        "- internal: First.\n"
        "- internal: Second.\n"
        "- internal: Third.\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "add bad plan", "-q")

    rc, _stdout, stderr = _run_hook_in(tmp_path, "--scan-repo")
    assert rc == 1, f"expected scan-repo to fail, got rc={rc}, stderr={stderr!r}"
    assert "orchestrator-chat-tui-task-plan.md" in stderr
    assert "internal" in stderr


def test_scan_repo_passes_on_clean_repo(tmp_path: Path) -> None:
    _init_fake_repo(tmp_path)
    plan = tmp_path / "docs" / "tasks" / "12.0" / "clean-task-plan.md"
    plan.parent.mkdir(parents=True)
    plan.write_text(
        "# Plan\n\n## implementation note\n- Implement foo\n- Test bar\n- Document baz\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "add clean plan", "-q")

    rc, _stdout, stderr = _run_hook_in(tmp_path, "--scan-repo")
    assert rc == 0, f"expected scan-repo to pass, got rc={rc}, stderr={stderr!r}"
    assert stderr == ""


def test_scan_repo_ignores_unscoped_markdown(tmp_path: Path) -> None:
    """README.md and CLAUDE.md must remain exempt even with --scan-repo."""
    _init_fake_repo(tmp_path)
    readme = tmp_path / "README.md"
    readme.write_text(
        "# Repo\n\n"
        "- internal: noted\n"
        "- internal: noted\n"
        "- internal: noted\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "add readme", "-q")

    rc, _stdout, stderr = _run_hook_in(tmp_path, "--scan-repo")
    assert rc == 0, f"expected scan-repo to pass, got rc={rc}, stderr={stderr!r}"


def test_scan_repo_and_scan_paths_are_mutually_exclusive(tmp_path: Path) -> None:
    rc, _stdout, stderr = _run_hook_in(
        tmp_path, "--scan-repo", "--scan-paths", str(tmp_path)
    )
    assert rc != 0
    assert "mutually exclusive" in stderr


def test_hook_no_op_on_invalid_json() -> None:
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="not json at all",
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert proc.returncode == 0


def test_hook_no_op_on_missing_file_path() -> None:
    payload = {"tool_input": {"content": "irrelevant"}}
    rc, _ = _run_hook(payload)
    assert rc == 0


# ---------------------------------------------------------------------------
# _detect_revision_history_blocks — heuristic correctness
# ---------------------------------------------------------------------------


def test_revision_history_label_is_detected() -> None:
    text = "Some prose.\n\nRevision history:\n- entry one\n"
    assert _detect_revision_history_blocks(text) == [3]


def test_revision_history_heading_is_detected() -> None:
    text = "# Plan\n\n## Revision history\n\n- 2025-01-01 wrote thing\n"
    assert _detect_revision_history_blocks(text) == [3]


def test_revision_history_heading_levels_one_through_four_match() -> None:
    for prefix in ("#", "##", "###", "####"):
        text = f"{prefix} Revision history\n"
        assert _detect_revision_history_blocks(text) == [1], prefix


def test_revision_history_is_case_insensitive() -> None:
    text = "## REVISION HISTORY\n"
    assert _detect_revision_history_blocks(text) == [1]


def test_revision_history_inline_mention_does_not_match() -> None:
    text = "We keep the revision history in handoff, not in the plan.\n"
    assert _detect_revision_history_blocks(text) == []


def test_revision_history_label_inside_paragraph_does_not_match() -> None:
    text = "See the project's revision history: it lives in the DB.\n"
    assert _detect_revision_history_blocks(text) == []


def test_revision_history_returns_each_match() -> None:
    text = (
        "## Revision history\n"
        "\n"
        "Some prose.\n"
        "\n"
        "Revision history:\n"
    )
    assert _detect_revision_history_blocks(text) == [1, 5]


def test_revision_history_blockquoted_label_is_detected() -> None:
    """Regression for internal.

    The legacy shape the task plan cites as motivation lives inside a
    Markdown blockquote (the metadata block at the top of older task plans
    quotes the rule with a leading ``>``). Without blockquote-aware matching
    the guard misses exactly the case it was created to catch.
    """
    text = "> Some metadata\n>\n> Revision history:\n>\n> - 2025-01-01 r1\n"
    assert _detect_revision_history_blocks(text) == [3]


def test_revision_history_blockquoted_heading_is_detected() -> None:
    text = "> ## Revision history\n>\n> - entry\n"
    assert _detect_revision_history_blocks(text) == [1]


def test_revision_history_nested_blockquote_is_detected() -> None:
    """Markdown allows nested blockquotes; the detector should still fire."""
    text = "> > Revision history:\n"
    assert _detect_revision_history_blocks(text) == [1]


def test_revision_history_blockquoted_inline_mention_does_not_match() -> None:
    """A blockquote that merely mentions the phrase mid-sentence is allowed."""
    text = "> See the revision history: it lives in handoff.\n"
    assert _detect_revision_history_blocks(text) == []


# ---------------------------------------------------------------------------
# Hook end-to-end: revision-history rejection in task-plan paths
# ---------------------------------------------------------------------------


def test_hook_blocks_write_with_revision_history_heading_in_docs_tasks(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# Plan\n\n"
                "## Revision history\n\n"
                "- 2025-01-01 first revision\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "Revision-history block detected" in stderr
    # Actionable MCP-oriented message names the canonical handoff surfaces.
    assert "set_handoff_state" in stderr
    assert "record_event" in stderr
    assert "close_slice" in stderr
    assert "review_findings" in stderr
    assert "render_handoff" in stderr


def test_hook_blocks_write_with_revision_history_label_in_packages_docs_tasks(
    tmp_path: Path,
) -> None:
    target = (
        tmp_path
        / "packages"
        / "mcp-workbay-handoff"
        / "docs"
        / "tasks"
        / "internal-task-plan.md"
    )
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# Plan\n\n"
                "## Objective\n\n"
                "Stuff.\n\n"
                "Revision history:\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "Revision-history block detected" in stderr


def test_hook_blocks_edit_inserting_revision_history_heading(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    target.write_text("# Plan\n\n<placeholder>\n", encoding="utf-8")
    payload = {
        "tool_input": {
            "file_path": str(target),
            "old_string": "<placeholder>",
            "new_string": "## Revision history\n\n- entry",
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "Revision-history block detected" in stderr


def test_hook_allows_revision_history_in_docs_plans(tmp_path: Path) -> None:
    """Top-level numbered plans under docs/plans/** are out of scope."""
    target = tmp_path / "docs" / "plans" / "0009-some-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# implementation note\n\n"
                "## Revision history\n\n"
                "- 2025-01-01 r1\n"
                "- 2025-02-01 r2\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 0, f"expected allow on docs/plans/, got rc={rc}, stderr={stderr!r}"


def test_hook_allows_task_plan_without_revision_history(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# Plan\n\n"
                "## Objective\n\nDo a thing.\n\n"
                "## implementation note\n- step\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 0, f"expected allow on clean task plan, got rc={rc}, stderr={stderr!r}"


def test_hook_blocks_write_with_blockquoted_revision_history_in_docs_tasks(
    tmp_path: Path,
) -> None:
    """Regression for internal.

    The legacy shape that motivated the guard lives inside a Markdown
    blockquote. The end-to-end hook must reject task-plan writes whose
    revision-history block sits behind one or more ``>`` markers.
    """
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# Plan\n\n"
                "> **Metadata** -- fill in when creating a new doc.\n"
                ">\n"
                "> Revision history:\n"
                ">\n"
                "> - 2025-01-01 r1: initial draft\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "Revision-history block detected" in stderr


def test_hook_allows_blockquoted_revision_history_in_docs_plans(tmp_path: Path) -> None:
    """Blockquoted revision history in docs/plans/** stays out of scope."""
    target = tmp_path / "docs" / "plans" / "0009-historical.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# implementation note\n\n"
                "> **Metadata**\n"
                ">\n"
                "> Revision history:\n"
                ">\n"
                "> - 2024-12-01 r1\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 0, f"expected allow, got rc={rc}, stderr={stderr!r}"


def test_hook_reports_both_violations_simultaneously(tmp_path: Path) -> None:
    """A single write can trip both detectors; both must be reported."""
    target = tmp_path / "docs" / "tasks" / "12.0" / "fake-task-plan.md"
    target.parent.mkdir(parents=True)
    payload = {
        "tool_input": {
            "file_path": str(target),
            "content": (
                "# Plan\n\n"
                "## Findings\n\n"
                "- internal: a\n"
                "- internal: b\n"
                "- internal: c\n\n"
                "## Revision history\n\n"
                "- entry\n"
            ),
        }
    }
    rc, stderr = _run_hook(payload)
    assert rc == 2
    assert "Pasted review-finding list detected" in stderr
    assert "Revision-history block detected" in stderr


# ---------------------------------------------------------------------------
# --scan-repo regression: revision-history scope
# ---------------------------------------------------------------------------


def test_scan_repo_catches_revision_history_in_task_plan(tmp_path: Path) -> None:
    _init_fake_repo(tmp_path)
    plan = tmp_path / "docs" / "tasks" / "12.0" / "bad-task-plan.md"
    plan.parent.mkdir(parents=True)
    plan.write_text(
        "# Plan\n\n## Revision history\n\n- entry\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "add bad plan", "-q")

    rc, _stdout, stderr = _run_hook_in(tmp_path, "--scan-repo")
    assert rc == 1, f"expected fail, got rc={rc}, stderr={stderr!r}"
    assert "bad-task-plan.md" in stderr
    assert "Revision-history block detected" in stderr


def test_scan_repo_allows_revision_history_in_docs_plans(tmp_path: Path) -> None:
    _init_fake_repo(tmp_path)
    plan = tmp_path / "docs" / "plans" / "0009-historical.md"
    plan.parent.mkdir(parents=True)
    plan.write_text(
        "# implementation note\n\n## Revision history\n\n- 2024-12-01 r1\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "add historical plan", "-q")

    rc, _stdout, stderr = _run_hook_in(tmp_path, "--scan-repo")
    assert rc == 0, f"expected allow, got rc={rc}, stderr={stderr!r}"
    assert stderr == ""
