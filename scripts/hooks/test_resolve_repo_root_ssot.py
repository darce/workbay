"""SSOT guard: shell hooks source _resolve_repo_root.sh (internal)."""
from __future__ import annotations

from pathlib import Path

HOOKS = Path(__file__).parent
ADOPTERS = (
    HOOKS / "guard-worktree-drift.sh",
    HOOKS / "guard-main-branch.sh",
    HOOKS / "regenerate-task-views.sh",
)


def test_shell_hooks_source_resolve_repo_root_snippet() -> None:
    for path in ADOPTERS:
        text = path.read_text()
        assert "_resolve_repo_root.sh" in text, path.name
        assert "git rev-parse --show-toplevel" not in text, path.name


def test_resolve_repo_root_snippet_exists() -> None:
    assert (HOOKS / "_resolve_repo_root.sh").is_file()


def test_regenerate_task_views_uses_repo_root_not_inline_env() -> None:
    text = (HOOKS / "regenerate-task-views.sh").read_text()
    assert "${CLAUDE_PROJECT_DIR:-${GROK_WORKSPACE_ROOT" not in text
