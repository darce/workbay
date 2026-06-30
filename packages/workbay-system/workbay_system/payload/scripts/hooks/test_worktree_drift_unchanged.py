"""internal: pin that ``_worktree_drift.py`` remains the
hard contract for PreToolUse file-mutation edits to protected paths
from the wrong worktree.

The parent scope explicitly requires this: ``branch-lifecycle/body.md``
documents ``_worktree_drift.py`` as the Cold-Start runbook's
file-mutation hard contract. internal relaxes the post-merge
``check-main-clean`` surface (implementation note) but **must not** relax the
file-mutation surface — that would be a silent regression of the
runbook.

These tests use ``evaluate_payload`` directly so the assertions cover
the policy core, not just the subprocess shim. The hook script itself
remains untouched in internal; this module exists to fail loudly
if a future refactor reroutes the file-mutation contract.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

from _active_task_context import ActiveTaskContext  # noqa: E402
from _worktree_drift import evaluate_payload  # noqa: E402

CONTRACT_SOURCE = REPO_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _seed_contract(repo: Path) -> None:
    """Copy the live harness-protocol contract into the fixture repo so
    ``load_branch_isolation_policy`` can resolve the protected surfaces
    when ``_worktree_drift`` evaluates a payload.
    """
    target = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(CONTRACT_SOURCE, target)


def _make_two_worktrees(tmp_path: Path) -> tuple[Path, Path]:
    """Build a primary worktree on ``main`` plus a linked worktree on
    ``feature/internal-03-drift`` so the drift guard has a concrete target
    worktree distinct from the primary.
    """
    primary = tmp_path / "primary"
    primary.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(primary)], check=True)
    _seed_contract(primary)
    (primary / "README.md").write_text("seed\n", encoding="utf-8")
    _git("add", "-A", cwd=primary)
    _git("commit", "-q", "-m", "init", cwd=primary)

    feature_root = tmp_path / "primary-feature"
    _git(
        "worktree",
        "add",
        "-b",
        "feature/internal-03-drift",
        str(feature_root),
        cwd=primary,
    )
    return primary, feature_root


def test_worktree_drift_blocks_edit_into_primary_when_task_targets_feature(
    tmp_path: Path,
) -> None:
    """A `_worktree_drift.py` Edit payload targeting a file in the
    primary worktree must hard-block when the active task's
    ``target_worktree`` is a linked feature worktree.

    This is the Cold-Start runbook's load-bearing contract: the
    file-mutation hook is what stops an agent in the wrong shell from
    silently writing into the primary worktree on ``main``.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    # Materialise the parent directory so `_candidate_worktree_root` can
    # resolve the path's owning worktree via `git rev-parse --show-toplevel`
    # (which probes the parent directory when the file itself does not yet
    # exist — the common case for Edit/Write target paths).
    (primary / "docs" / "scopes").mkdir(parents=True)

    # Active task lives on the feature worktree.
    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/internal-03-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
    )

    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "would-be-drift.md"),
        },
    }

    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is not None, "drift guard must return a decision for cross-worktree edits"
    assert decision.outcome == "block", (
        f"file-mutation edit into wrong worktree must hard-block; got outcome={decision.outcome!r} "
        f"reason={decision.reason!r}"
    )


def test_worktree_drift_allows_edit_into_target_worktree(tmp_path: Path) -> None:
    """The symmetric pass case: an Edit targeting the active task's
    own ``target_worktree`` must return ``None`` (allow). Without this
    pass-case coverage, the block test alone could mask a regression
    that blocks ALL edits.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)

    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/internal-03-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
    )

    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(feature_root / "docs" / "scopes" / "in-target.md"),
        },
    }

    decision = evaluate_payload(
        payload,
        workspace_root=feature_root,
        active_task=context,
    )
    assert decision is None, (
        "file-mutation edits inside the task's target_worktree must pass through; "
        f"got decision={decision!r}"
    )


def test_worktree_drift_module_source_preserves_file_mutation_surface() -> None:
    """Structural test: ``_worktree_drift.py`` must continue to import the
    file-mutation helpers from ``_harness_protocol`` so the Cold-Start
    runbook's references remain accurate. A future refactor that ripped
    out ``find_permitted_main_surface`` or ``load_branch_isolation_policy``
    would silently weaken the contract — this test fails loudly first.
    """
    source_path = REPO_ROOT / "scripts" / "hooks" / "_worktree_drift.py"
    source = source_path.read_text(encoding="utf-8")
    assert "find_permitted_main_surface" in source, (
        "_worktree_drift.py must consume the permitted-surface carve-out for "
        "main-branch allowlisting; this is part of the Cold-Start runbook contract."
    )
    assert "load_branch_isolation_policy" in source, (
        "_worktree_drift.py must continue to load the branch-isolation policy."
    )
    # implementation note explicitly preserves the file-mutation surface. If a future
    # refactor demotes this hook to advisory-only, update the Cold-Start
    # runbook (branch-lifecycle/body.md:73-88) in the same change.
    assert 'outcome="block"' in source, (
        "_worktree_drift.py must still emit `outcome=\"block\"` decisions for "
        "wrong-worktree edits — this is the load-bearing Cold-Start contract."
    )
