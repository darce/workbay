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

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _active_task_context as atc  # noqa: E402
from _active_task_context import ActiveTaskContext  # noqa: E402
from _worktree_drift import evaluate_payload  # noqa: E402

CONTRACT_SOURCE = REPO_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"


def _dir_with_no_git_ancestor() -> Path:
    """Temp dir whose ancestors have no ``.git`` (true walk-to-root None).

    pytest's default root is often under ``/tmp``, and some hosts keep a real
    ``/tmp/.git``; walk-up correctly treats those paths as inside that repo.
    """
    for base in (Path("/var/tmp"), Path.home(), Path("/tmp")):
        if not base.is_dir():
            continue
        cursor = base.resolve(strict=False)
        blocked = False
        for _ in range(128):
        # Over-approximation is deliberate: bare .git existence is the SAFE
        # direction for this OPPOSITE contract (find a temp base with no repo
        # above it). Rejecting doubtful .git entries is more conservative about
        # isolation; do not harden this probe to the production ascent predicate.
            if (cursor / ".git").exists():
                blocked = True
                break
            parent = cursor.parent
            if parent == cursor:
                break
            cursor = parent
        if blocked:
            continue
        return Path(tempfile.mkdtemp(prefix="drift-none-", dir=str(base)))
    raise RuntimeError("no base directory free of .git ancestors for negative walk-up")


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


def _seed_older_authority(root: Path) -> Path:
    """Create ``.task-state/handoff.db`` older than a soon-to-be-written snapshot.

    Freshness requires a defined authority (finding 9943): a fully absent
    ``.task-state/`` answers not-fresh, so a snapshot arm that needs the cache
    path reachable must seed an older main DB before writing the snapshot.
    """
    import os

    state_dir = root / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    if not db_path.exists():
        db_path.write_bytes(b"")
    older = db_path.stat().st_mtime - 120.0
    os.utime(db_path, (older, older))
    return db_path


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


def test_worktree_drift_blocks_on_ambiguous_probe_error(tmp_path: Path) -> None:
    """D3: snapshot_worktree_ambiguous with no target must BLOCK, not ALLOW."""
    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)

    context = ActiveTaskContext(
        task_ref=None,
        target_branch=None,
        target_worktree=None,
        primary_worktree=str(primary),
        probe_error="snapshot_worktree_ambiguous",
    )
    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "protected.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is not None
    assert decision.outcome == "block"
    assert decision.reason is not None
    assert "snapshot_worktree_ambiguous" in decision.reason


def test_worktree_drift_allows_handoff_unavailable(tmp_path: Path) -> None:
    """D3 control: handoff_unavailable is a clean allow, not a blanket block."""
    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)

    context = ActiveTaskContext(
        task_ref=None,
        target_branch=None,
        target_worktree=None,
        primary_worktree=str(primary),
        probe_error="handoff_unavailable",
    )
    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "ok.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is None


def test_worktree_drift_over_budget_keeps_resolved_identity(tmp_path: Path) -> None:
    """Slow-but-resolved context must not become a repo-wide edit lockout.

    ``handoff_probe_over_budget`` names a complete identity answer that was
    merely late [RES-03]. An Edit inside the resolved target_worktree must
    pass; a cross-worktree Edit under the same label must still block. The
    pair prevents a pin that is satisfied by a guard that stopped guarding.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)
    (feature_root / "docs" / "scopes").mkdir(parents=True)

    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/wb-hook-latency-01",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
        probe_error="handoff_probe_over_budget",
    )

    in_target = evaluate_payload(
        {
            "toolName": "Edit",
            "toolInput": {
                "file_path": str(feature_root / "docs" / "scopes" / "in-target.md"),
            },
        },
        workspace_root=feature_root,
        active_task=context,
    )
    assert in_target is None or getattr(in_target, "outcome", None) != "block", (
        "handoff_probe_over_budget with a resolved target must allow edits "
        f"inside that target; got decision={in_target!r}"
    )

    cross = evaluate_payload(
        {
            "toolName": "Edit",
            "toolInput": {
                "file_path": str(primary / "docs" / "scopes" / "would-be-drift.md"),
            },
        },
        workspace_root=feature_root,
        active_task=context,
    )
    assert cross is not None, (
        "handoff_probe_over_budget must not disable cross-worktree isolation"
    )
    assert cross.outcome == "block", (
        "cross-worktree Edit under handoff_probe_over_budget must still block; "
        f"got outcome={cross.outcome!r} reason={cross.reason!r}"
    )


def test_multi_task_snapshot_at_primary_allows_edit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Primary-checkout edit with a multi-task snapshot must ALLOW, not block.

    Live repro wedge: CURRENT_TASK.json lists several live tasks (none matching
    the primary root as a target worktree). The snapshot path must fall through
    so the post-database probe_error is the allowlisted handoff_unavailable
    token, and the drift evaluator permits the edit.
    """
    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)
    monkeypatch.delenv("ALT_ALLOW_WORKTREE_DRIFT", raising=False)

    snapshot = {
        "schema_version": 2,
        "shape": "workspace_ambiguous",
        "staleness_note": "May lag; authoritative state via load_session.",
        "generated_at": "2026-07-29T03:23:00Z",
        "tasks": [
            {
                "task_ref": "internal",
                "status": "in_progress",
                "target_branch": "feature/a",
                "target_worktree_path": "/elsewhere/repo-a",
            },
            {
                "task_ref": "internal",
                "status": "in_progress",
                "target_branch": "feature/b",
                "target_worktree_path": "/elsewhere/repo-b",
            },
            {
                "task_ref": "internal",
                "status": "in_progress",
                "target_branch": "feature/c",
                "target_worktree_path": "/elsewhere/repo-c",
            },
        ],
    }
    (primary / "CURRENT_TASK.json").write_text(json.dumps(snapshot), encoding="utf-8")

    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda _root, *, timeout=5.0: str(primary)
    )
    monkeypatch.setattr(atc, "_load_handoff_exports", lambda: None)

    snap = atc._try_load_active_task_from_snapshot(primary)
    assert snap is None, (
        f"multi-task no-match snapshot must fall through (None); got {snap!r}"
    )

    context = atc._load_active_task(primary)
    assert context.probe_error == "handoff_unavailable", (
        f"expected handoff_unavailable after fallthrough, got {context.probe_error!r}"
    )

    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "primary-edit.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is None or (
        getattr(decision, "outcome", None) not in {"block"}
    ), (
        "primary-checkout edit under multi-task unusable snapshot must allow, "
        f"not block; got decision={decision!r}"
    )
    if decision is not None:
        assert decision.outcome != "block"
        assert "snapshot_no_worktree_match" not in (decision.reason or "")
        assert "snapshot_worktree_ambiguous" not in (decision.reason or "")


def test_resolved_snapshot_context_still_blocks_mismatched_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Isolation pin: a confident snapshot match still enforces drift.

    When CURRENT_TASK.json resolves a task for the feature worktree, an Edit
    into the primary worktree must hard-block. Fallthrough must not silently
    drop isolation for resolvable snapshots.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)
    monkeypatch.delenv("ALT_ALLOW_WORKTREE_DRIFT", raising=False)

    # This arm needs the snapshot to be USED, so the authority it names must
    # exist and be older than it; an absent .task-state/ answers not-fresh
    # [finding 9943] and would exercise the fall-through instead.
    _seed_older_authority(primary)

    # shape=single: confident match without multi-row ambiguity fallback notes.
    snapshot = {
        "schema_version": 2,
        "shape": "single",
        "staleness_note": "May lag; authoritative state via load_session.",
        "generated_at": "2026-07-29T03:23:00Z",
        "authority_db_path": str(primary / ".task-state" / "handoff.db"),
        "authority_projection_dir": str(primary / ".task-state" / "current"),
        "active": {
            "task_ref": "internal",
            "status": "in_progress",
            "target_branch": "feature/wb-hook-latency-01",
            "target_worktree_path": str(feature_root),
        },
    }
    (primary / "CURRENT_TASK.json").write_text(json.dumps(snapshot), encoding="utf-8")

    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda _root, *, timeout=5.0: str(primary)
    )

    context = atc._try_load_active_task_from_snapshot(feature_root)
    assert context is not None
    assert context.task_ref == "internal"
    assert context.probe_error is None
    assert context.target_worktree == str(feature_root)

    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "would-be-drift.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=feature_root,
        active_task=context,
    )
    assert decision is not None
    assert decision.outcome == "block", (
        "resolved snapshot task must still enforce cross-worktree isolation; "
        f"got outcome={decision.outcome!r} reason={decision.reason!r}"
    )


def test_legacy_snapshot_without_authority_keys_falls_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshots missing or empty authority keys must fall through [DATA-01].

    The freshness helper rejects a cache answer when authority_db_path or
    authority_projection_dir is absent, empty, or non-string. That is not a
    usable context — the loader returns None so the authoritative probe runs.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    monkeypatch.delenv("ALT_ALLOW_WORKTREE_DRIFT", raising=False)

    snapshot = {
        "schema_version": 2,
        "shape": "single",
        "staleness_note": "May lag; authoritative state via load_session.",
        "generated_at": "2026-07-29T03:23:00Z",
        "active": {
            "task_ref": "internal",
            "status": "in_progress",
            "target_branch": "feature/wb-hook-latency-01",
            "target_worktree_path": str(feature_root),
        },
    }
    (primary / "CURRENT_TASK.json").write_text(json.dumps(snapshot), encoding="utf-8")

    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda _root, *, timeout=5.0: str(primary)
    )

    context = atc._try_load_active_task_from_snapshot(feature_root)
    assert context is None, (
        "legacy snapshot without authority keys must fall through (None); "
        f"got {context!r}"
    )

    snapshot_empty_keys = {
        **snapshot,
        "authority_db_path": "",
        "authority_projection_dir": "",
    }
    (primary / "CURRENT_TASK.json").write_text(
        json.dumps(snapshot_empty_keys), encoding="utf-8"
    )
    context_empty = atc._try_load_active_task_from_snapshot(feature_root)
    assert context_empty is None, (
        "snapshot with empty-string authority keys must fall through (None); "
        f"got {context_empty!r}"
    )


def test_worktree_drift_blocks_unrecognised_probe_error(tmp_path: Path) -> None:
    """D3: unknown probe_error tokens fail closed [AGT-10]."""
    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)

    context = ActiveTaskContext(
        task_ref=None,
        target_branch=None,
        target_worktree=None,
        primary_worktree=str(primary),
        probe_error="some_future_token",
    )
    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "protected.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is not None
    assert decision.outcome == "block"
    assert decision.reason is not None
    assert "some_future_token" in decision.reason


def test_worktree_drift_maint_bypass_survives_non_allowlisted_probe_error(
    tmp_path: Path,
) -> None:
    """Non-allowlisted probe_error must not outrank the MAINT task-ref hatch."""
    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)

    context = ActiveTaskContext(
        task_ref="internal",
        target_branch=None,
        target_worktree=None,
        primary_worktree=str(primary),
        probe_error="snapshot_worktree_ambiguous",
    )
    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "maint.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is not None
    assert decision.outcome == "maintenance_bypass", (
        "MAINT task ref must bypass even when probe_error is not allow-listed; "
        f"got outcome={decision.outcome!r} reason={decision.reason!r}"
    )


def test_worktree_drift_env_bypass_survives_non_allowlisted_probe_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-allowlisted probe_error must not outrank ALT_ALLOW_WORKTREE_DRIFT=1."""
    import os

    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)
    monkeypatch.setenv("ALT_ALLOW_WORKTREE_DRIFT", "1")

    context = ActiveTaskContext(
        task_ref=None,
        target_branch=None,
        target_worktree=None,
        primary_worktree=str(primary),
        probe_error="snapshot_worktree_ambiguous",
    )
    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "env.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is not None
    assert decision.outcome == "env_bypass", (
        "ALT_ALLOW_WORKTREE_DRIFT=1 must bypass even when probe_error is not "
        f"allow-listed; got outcome={decision.outcome!r} reason={decision.reason!r}"
    )
    # sanity: env was actually set for this pin
    assert os.environ.get("ALT_ALLOW_WORKTREE_DRIFT") == "1"


def test_worktree_drift_non_allowlisted_probe_error_still_blocks_without_hatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without either hatch, non-allowlisted probe_error remains a hard block."""
    primary, _feature = _make_two_worktrees(tmp_path)
    (primary / "docs" / "scopes").mkdir(parents=True)
    monkeypatch.delenv("ALT_ALLOW_WORKTREE_DRIFT", raising=False)

    context = ActiveTaskContext(
        task_ref="internal",
        target_branch=None,
        target_worktree=None,
        primary_worktree=str(primary),
        probe_error="snapshot_worktree_ambiguous",
    )
    payload = {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "blocked.md"),
        },
    }
    decision = evaluate_payload(
        payload,
        workspace_root=primary,
        active_task=context,
    )
    assert decision is not None
    assert decision.outcome == "block", (
        "non-allowlisted probe_error with neither hatch must still block; "
        f"got outcome={decision.outcome!r} reason={decision.reason!r}"
    )
    assert decision.reason is not None
    assert "snapshot_worktree_ambiguous" in decision.reason
    # Escape hatches must be named so the operator can proceed (via block helper).
    assert "MAINT-*" in decision.reason or "MAINT-" in decision.reason
    assert "ALT_ALLOW_WORKTREE_DRIFT" in decision.reason


def test_worktree_drift_blocks_when_primary_unresolvable() -> None:
    """Primary root could-not-determine must fail closed [OBS-08][ARCH-13].

    The tuple active_task path resolves primary via ``_primary_workspace_root``.
    Outside any git repo that helper returns None, so the guard's
    ``git_primary_unresolvable`` arm must block rather than invent a primary
    and permit the edit.
    """
    isolated = _dir_with_no_git_ancestor()
    try:
        outside = isolated / "not-a-repo"
        outside.mkdir()
        (outside / "docs" / "scopes").mkdir(parents=True)
        feature_target = isolated / "feature-target"
        feature_target.mkdir()

        payload = {
            "toolName": "Edit",
            "toolInput": {
                "file_path": str(outside / "docs" / "scopes" / "would-be-drift.md"),
            },
        }
        decision = evaluate_payload(
            payload,
            workspace_root=outside,
            active_task=("internal", str(feature_target)),
        )
        assert decision is not None
        assert decision.outcome == "block"
        assert decision.reason is not None
        assert "git_primary_unresolvable" in decision.reason
    finally:
        shutil.rmtree(isolated, ignore_errors=True)


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
