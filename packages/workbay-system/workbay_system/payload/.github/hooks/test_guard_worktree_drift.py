"""Tests for the worktree-drift PreToolUse hook."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


HELPER_SCRIPT = Path(__file__).parents[2] / "scripts" / "hooks" / "_worktree_drift.py"
sys.path.insert(0, str(HELPER_SCRIPT.parent))

_spec = importlib.util.spec_from_file_location("worktree_drift_helper", HELPER_SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules[_spec.name] = _mod  # type: ignore[index]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

evaluate_payload = _mod.evaluate_payload
_log_trace = _mod._log_trace
DriftDecision = _mod.DriftDecision

from _harness_protocol import (  # noqa: E402
    BranchIsolationPolicy,
    HarnessContractMissingError,
    MainSurfacePattern,
    find_permitted_main_surface,
)


def test_evaluate_payload_returns_none_when_no_active_task(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(repo / "README.md")}}
    result = evaluate_payload(payload, workspace_root=repo, active_task=(None, None))
    assert result is None


def test_evaluate_payload_returns_none_when_target_matches_candidate(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    scripts_dir = repo / "scripts"
    scripts_dir.mkdir(parents=True)
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(scripts_dir / "check.py")}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(repo.resolve()))
    result = evaluate_payload(payload, workspace_root=repo, active_task=("internal", str(repo), "feature/e17-8"))
    assert result is None


def test_evaluate_payload_returns_finding_when_candidate_worktree_differs(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    (main_repo / "docs").mkdir(parents=True)
    _write_contract(main_repo)
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(main_repo / "docs" / "note.md")}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(payload, workspace_root=main_repo, active_task=("internal", str(feature_repo), "feature/e17-8"))
    assert result is not None
    assert result.outcome == "block"
    assert result.task_ref == "internal"
    assert result.candidate_worktree == str(main_repo.resolve())
    assert result.target_worktree == str(feature_repo.resolve())


def test_evaluate_payload_handles_apply_patch_paths(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    (main_repo / "scripts").mkdir(parents=True)
    _write_contract(main_repo)
    patch = f"""*** Begin Patch
*** Update File: {main_repo / 'scripts' / 'check.py'}
@@
-old
+new
*** End Patch"""
    payload = {"toolName": "apply_patch", "toolInput": {"input": patch}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(payload, workspace_root=main_repo, active_task=("internal", str(feature_repo), "feature/e17-8"))
    assert result is not None
    assert result.outcome == "block"
    assert result.path.endswith("scripts/check.py")


def test_evaluate_payload_returns_allowlisted_main_surface_for_task_plan(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    contract_dir = main_repo / "docs" / "agentic" / "contracts"
    task_dir = main_repo / "docs" / "tasks" / "17.0"
    task_dir.mkdir(parents=True)
    contract_dir.mkdir(parents=True)
    (contract_dir / "harness-protocol.yaml").write_text(
        """version: 1

branch_isolation:
  code_roots:
    - scripts/
  protected_extensions:
    - .py
  root_protected_files:
    - Makefile
  permitted_main_surfaces:
    - pattern: "docs/tasks/**/*.md"
      reason: "Task plans"
""",
        encoding="utf-8",
    )
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(task_dir / "plan.md")}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(payload, workspace_root=main_repo, active_task=("internal", str(feature_repo), "feature/e17-8"))
    assert result is not None
    assert result.outcome == "allowlisted_main_surface"
    assert result.matched_pattern == "docs/tasks/**/*.md"


def test_find_permitted_main_surface_matches_trailing_double_star_patterns() -> None:
    policy = BranchIsolationPolicy(
        code_roots=("scripts/",),
        protected_extensions=(".py",),
        root_protected_files=("Makefile",),
        protected_main_surfaces=(),
        permitted_main_surfaces=(
            MainSurfacePattern(pattern="docs/assessments/**", reason="Assessments"),
        ),
    )

    immediate = find_permitted_main_surface("docs/assessments/note.md", policy)
    nested = find_permitted_main_surface("docs/assessments/2026/summary.md", policy)

    assert immediate is not None
    assert immediate.reason == "Assessments"
    assert nested is not None
    assert nested.reason == "Assessments"


def test_evaluate_payload_blocks_mixed_patch_when_later_path_is_not_allowlisted(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    contract_dir = main_repo / "docs" / "agentic" / "contracts"
    contract_dir.mkdir(parents=True)
    (contract_dir / "harness-protocol.yaml").write_text(
        """version: 1

branch_isolation:
  code_roots:
    - scripts/
  protected_extensions:
    - .py
  root_protected_files:
    - Makefile
  permitted_main_surfaces:
    - pattern: "docs/tasks/**/*.md"
      reason: "Task plans"
""",
        encoding="utf-8",
    )
    (main_repo / "docs" / "tasks" / "17.0").mkdir(parents=True)
    (main_repo / "scripts").mkdir(parents=True)
    patch = f"""*** Begin Patch
*** Update File: {main_repo / 'docs' / 'tasks' / '17.0' / 'plan.md'}
@@
-old
+new
*** Update File: {main_repo / 'scripts' / 'check.py'}
@@
-old
+new
*** End Patch"""
    payload = {"toolName": "apply_patch", "toolInput": {"input": patch}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(payload, workspace_root=main_repo, active_task=("internal", str(feature_repo), "feature/e17-8"))
    assert result is not None
    assert result.outcome == "block"
    assert result.path.endswith("scripts/check.py")


def test_evaluate_payload_allowlist_uses_primary_worktree_when_running_from_feature_root(
    tmp_path: Path, monkeypatch
) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    contract_dir = main_repo / "docs" / "agentic" / "contracts"
    task_dir = main_repo / "docs" / "tasks" / "17.0"
    task_dir.mkdir(parents=True)
    contract_dir.mkdir(parents=True)
    (contract_dir / "harness-protocol.yaml").write_text(
        """version: 1

branch_isolation:
  code_roots:
    - scripts/
  protected_extensions:
    - .py
  root_protected_files:
    - Makefile
  permitted_main_surfaces:
    - pattern: "docs/tasks/**/*.md"
      reason: "Task plans"
""",
        encoding="utf-8",
    )
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(task_dir / "plan.md")}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    monkeypatch.setattr(_mod, "_primary_workspace_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=feature_repo,
        active_task=("internal", str(feature_repo), "feature/e17-8"),
    )
    assert result is not None
    assert result.outcome == "allowlisted_main_surface"
    assert result.candidate_worktree == str(main_repo.resolve())


def test_log_trace_includes_timestamp(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    decision = DriftDecision(
        outcome="allowlisted_main_surface",
        reason="allow-listed main surface: Task plans",
        primary_worktree=str(repo),
        path=str(repo / "docs" / "tasks" / "17.0" / "plan.md"),
        candidate_worktree=str(repo),
        target_worktree=str(repo / "repo-feature"),
        task_ref="internal",
        repo_relative_path="docs/tasks/17.0/plan.md",
        matched_pattern="docs/tasks/**/*.md",
        matched_reason="Task plans",
    )

    _log_trace(decision)

    records = (repo / ".task-state" / "branch_isolation_guard.jsonl").read_text(encoding="utf-8").splitlines()
    payload = json.loads(records[0])
    assert payload["timestamp"].endswith("Z")
    assert payload["outcome"] == "allowlisted_main_surface"


def test_evaluate_payload_missing_contract_does_not_block(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """internal regression: when the branch-isolation contract is
    absent (a legitimate state on fresh ``--profile minimal`` consumer
    installs), the drift guard must route through
    ``HarnessContractMissingPolicy.WARN`` and return ``None`` instead
    of emitting a blocking ``DriftDecision``. The block-on-missing-
    contract behaviour from implementation note contradicted implementation note's policy
    framework — implementation note explicitly classifies the drift guard as a
    background helper with a non-blocking fallback path."""
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    (main_repo / "docs" / "tasks" / "17.0").mkdir(parents=True)
    target_file = main_repo / "docs" / "tasks" / "17.0" / "plan.md"
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(target_file)}}

    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    monkeypatch.setattr(_mod, "_primary_workspace_root", lambda _path: str(main_repo.resolve()))

    def fake_loader(_path: Path):
        raise HarnessContractMissingError(
            "HarnessContractMissingError: unable to load branch-isolation "
            "policy from docs/workbay/contracts/harness-protocol.yaml. "
            "missing on disk."
        )

    monkeypatch.setattr(_mod, "load_branch_isolation_policy", fake_loader)

    result = evaluate_payload(
        payload,
        workspace_root=feature_repo,
        active_task=("internal", str(feature_repo), "feature/e17-8"),
    )

    assert result is None, (
        "missing-contract path must return None (non-blocking), got "
        f"{result!r}"
    )

    captured = capsys.readouterr()
    assert "warning:" in captured.err
    assert "HarnessContractMissingError" in captured.err


def test_main_exits_zero_when_contract_missing(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """End-to-end: ``main()`` must exit 0 (no block emitted to stdout)
    when the contract is missing and the candidate worktree would
    otherwise have hit the allow-list path."""
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    (main_repo / "docs" / "tasks" / "17.0").mkdir(parents=True)
    target_file = main_repo / "docs" / "tasks" / "17.0" / "plan.md"
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(target_file)}}

    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    monkeypatch.setattr(_mod, "_primary_workspace_root", lambda _path: str(main_repo.resolve()))
    monkeypatch.setattr(_mod, "_workspace_root", lambda: feature_repo)
    monkeypatch.setattr(
        _mod,
        "_load_active_task",
        lambda _root: _mod.ActiveTaskContext(
            "internal", str(feature_repo), "feature/e17-8", str(main_repo.resolve())
        ),
    )

    def fake_loader(_path: Path):
        raise HarnessContractMissingError(
            "HarnessContractMissingError: contract missing."
        )

    monkeypatch.setattr(_mod, "load_branch_isolation_policy", fake_loader)
    monkeypatch.setattr(
        _mod.sys,
        "stdin",
        __import__("io").StringIO(json.dumps(payload)),
    )

    exit_code = _mod.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == "", (
        "main() must not emit a hookSpecificOutput block when the "
        f"contract is missing; got stdout={captured.out!r}"
    )


def test_evaluate_payload_returns_maintenance_bypass(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    payload = {"toolName": "create_file", "toolInput": {"filePath": str(repo / "README.md")}}
    result = evaluate_payload(payload, workspace_root=repo, active_task=("MAINT-test", str(repo / "feature"), "feature/test"))
    assert result is not None
    assert result.outcome == "maintenance_bypass"


def _write_contract(repo: Path) -> None:
    contract_dir = repo / "docs" / "agentic" / "contracts"
    contract_dir.mkdir(parents=True, exist_ok=True)
    (contract_dir / "harness-protocol.yaml").write_text(
        """version: 1

branch_isolation:
  code_roots:
    - apps/
    - packages/
    - scripts/
  protected_extensions:
    - .py
    - .ts
  root_protected_files:
    - Makefile
""",
        encoding="utf-8",
    )


def test_evaluate_payload_bash_formatter_drift_blocked(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    main_repo.mkdir()
    _write_contract(main_repo)
    payload = {"toolName": "Bash", "toolInput": {"command": "make format-all"}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=main_repo,
        active_task=("internal", str(feature_repo), "feature/e17-9"),
    )
    assert result is not None
    assert result.outcome == "block"
    assert result.candidate_worktree == str(main_repo.resolve())
    assert result.target_worktree == str(feature_repo.resolve())


def test_evaluate_payload_bash_explicit_path_drift_blocked(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    main_repo.mkdir()
    _write_contract(main_repo)
    payload = {"toolName": "Bash", "toolInput": {"command": "sed -i 's/x/y/' packages/foo.py"}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=main_repo,
        active_task=("internal", str(feature_repo), "feature/e17-9"),
    )
    assert result is not None
    assert result.outcome == "block"
    assert result.path.endswith("packages/foo.py")


def test_evaluate_payload_bash_readonly_command_ignored(tmp_path: Path, monkeypatch) -> None:
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    main_repo.mkdir()
    _write_contract(main_repo)
    payload = {"toolName": "Bash", "toolInput": {"command": "git status"}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=main_repo,
        active_task=("internal", str(feature_repo), "feature/e17-9"),
    )
    # git status does not touch protected paths — drift check emits nothing.
    assert result is None


def test_evaluate_payload_bash_formatter_in_target_worktree_allowed(tmp_path: Path, monkeypatch) -> None:
    feature_repo = tmp_path / "repo-feature"
    feature_repo.mkdir()
    _write_contract(feature_repo)
    payload = {"toolName": "Bash", "toolInput": {"command": "ruff format packages/"}}
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(feature_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=feature_repo,
        active_task=("internal", str(feature_repo), "feature/e17-9"),
    )
    assert result is None


def test_evaluate_payload_bash_absolute_path_cross_worktree_blocked(
    tmp_path: Path, monkeypatch
) -> None:
    """BR-01: absolute-path Bash write into another worktree must block.

    From a linked feature worktree, `sed -i '' 's/x/y/' /<primary>/packages/foo.py`
    previously slipped through because _to_repo_relative dropped paths outside
    the feature worktree. The drift guard now preserves absolute tokens and
    compares their hosting worktree against target_worktree.
    """
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    (main_repo / "packages").mkdir(parents=True)
    (main_repo / "packages" / "foo.py").write_text("x=1\n")
    _write_contract(feature_repo)
    abs_target = main_repo.resolve() / "packages" / "foo.py"
    payload = {
        "toolName": "Bash",
        "toolInput": {"command": f"sed -i '' 's/x/y/' {abs_target}"},
    }
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=feature_repo,
        active_task=("internal", str(feature_repo.resolve()), "feature/e17-9"),
    )
    assert result is not None
    assert result.outcome == "block"
    assert result.path is not None and result.path.endswith("packages/foo.py")


def test_evaluate_payload_bash_relative_path_escapes_workspace_blocked(
    tmp_path: Path, monkeypatch
) -> None:
    """BR-01 part 2: `../`-style relative paths that escape the feature worktree
    and land inside another worktree must also block. scan_bash_command drops
    these via _to_repo_relative; the extractor must preserve them.
    """
    main_repo = tmp_path / "repo-main"
    feature_repo = tmp_path / "repo-feature"
    (main_repo / "packages").mkdir(parents=True)
    (main_repo / "packages" / "foo.py").write_text("x=1\n")
    feature_repo.mkdir()
    _write_contract(feature_repo)
    # Craft a relative path from feature_repo that resolves into main_repo.
    rel = f"../{main_repo.name}/packages/foo.py"
    payload = {
        "toolName": "Bash",
        "toolInput": {"command": f"sed -i '' 's/x/y/' {rel}"},
    }
    monkeypatch.setattr(_mod, "_candidate_worktree_root", lambda _path: str(main_repo.resolve()))
    result = evaluate_payload(
        payload,
        workspace_root=feature_repo,
        active_task=("internal", str(feature_repo.resolve()), "feature/e17-9"),
    )
    assert result is not None
    assert result.outcome == "block"
    assert result.path is not None and result.path.endswith("packages/foo.py")


# ---------- Root-worktree-on-non-main guard ----------


def test_evaluate_payload_blocks_root_worktree_on_non_main_branch(
    tmp_path: Path, monkeypatch
) -> None:
    """Root worktree on a feature branch must be blocked."""
    root_repo = tmp_path / "repo-root"
    (root_repo / "scripts").mkdir(parents=True)
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(root_repo / "scripts" / "check.py")},
    }
    monkeypatch.setattr(
        _mod, "_candidate_worktree_root", lambda _path: str(root_repo.resolve())
    )
    monkeypatch.setattr(
        _mod, "_detect_current_branch", lambda _ws: "feature/e17-10"
    )
    result = evaluate_payload(
        payload,
        workspace_root=root_repo,
        active_task=("internal", str(root_repo), "feature/e17-10"),
    )
    assert result is not None
    assert result.outcome == "block"
    assert "RootWorktreeNotOnMainError" in (result.reason or "")


def test_evaluate_payload_allows_root_worktree_on_main_branch(
    tmp_path: Path, monkeypatch
) -> None:
    """Root worktree on main should not trigger the root-worktree guard."""
    root_repo = tmp_path / "repo-root"
    feature_repo = tmp_path / "repo-feature"
    (root_repo / "scripts").mkdir(parents=True)
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(root_repo / "scripts" / "check.py")},
    }
    monkeypatch.setattr(
        _mod, "_candidate_worktree_root", lambda _path: str(root_repo.resolve())
    )
    monkeypatch.setattr(_mod, "_detect_current_branch", lambda _ws: "main")
    result = evaluate_payload(
        payload,
        workspace_root=root_repo,
        active_task=("internal", str(feature_repo), "feature/e17-10"),
    )
    # Should NOT be a RootWorktreeNotOnMainError
    if result is not None:
        assert "RootWorktreeNotOnMainError" not in (result.reason or "")


def test_evaluate_payload_root_worktree_guard_skipped_for_maint_tasks(
    tmp_path: Path, monkeypatch
) -> None:
    """MAINT tasks bypass the root-worktree guard entirely."""
    root_repo = tmp_path / "repo-root"
    (root_repo / "scripts").mkdir(parents=True)
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(root_repo / "scripts" / "check.py")},
    }
    monkeypatch.setattr(
        _mod, "_candidate_worktree_root", lambda _path: str(root_repo.resolve())
    )
    monkeypatch.setattr(
        _mod, "_detect_current_branch", lambda _ws: "feature/maint-fix"
    )
    result = evaluate_payload(
        payload,
        workspace_root=root_repo,
        active_task=("internal", str(root_repo), "main"),
    )
    assert result is not None
    assert result.outcome == "maintenance_bypass"
