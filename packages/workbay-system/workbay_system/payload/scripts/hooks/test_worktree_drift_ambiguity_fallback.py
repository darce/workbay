"""The drift guard's ambiguity-fallback valve, and the ordering that makes it safe.

``scripts/hooks/`` is a materialized mirror of the payload tree with nothing
gating it. The payload copies of ``_active_task_context.py`` and
``_worktree_drift.py`` carry a paired feature the root copies lack entirely:

* ``ActiveTaskContext.resolution_note`` records that the active task row was
  *guessed* from several live candidates rather than resolved unambiguously.
* ``evaluate_payload`` reads that note and returns ``outcome="fallback"``
  instead of evaluating candidate-path drift against a guessed row — a
  mis-picked row would block edits legitimately belonging to another task.
* ``_ambiguity_fallback_disabled()`` reads ``WORKBAY_GUARD_AMBIGUITY_FALLBACK``
  so an operator can switch the valve off and force the fail-closed behaviour.

The root copies have no ``resolution_note`` field at all, so the valve is not
merely off there — it is unreachable, and an ambiguous workspace takes whatever
the unguessed probe happens to do.

**The ordering is the load-bearing part.** The fallback must sit AFTER the
MAINT bypass, the ``ALT_ALLOW_WORKTREE_DRIFT`` hatch, the non-allowlisted
``probe_error`` gate, and the root-worktree-on-non-main guard. Those checks key
on the live branch and primary-worktree identity, not on the guessed task row,
so an ambiguity rationale must not exempt them. Emitting the fallback earlier
lets an ambiguous multi-row database slip a root-worktree feature-branch edit
through. Tests 3 and 4 pin that ordering; without them a "fix" that returns the
fallback at the top of the function would satisfy test 2 and reintroduce the
regression.

Controls: with no note, drift must still block, and an edit into the task's own
target worktree must still be allowed. A valve that blanket-allows would pass
the fallback assertions while retiring the guard.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _active_task_context as atc  # noqa: E402
from _active_task_context import ActiveTaskContext  # noqa: E402
from _worktree_drift import evaluate_payload  # noqa: E402

CONTRACT_SOURCE = REPO_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"

NOTE = "snapshot_workspace_ambiguous: selected by target_worktree_path match among multiple live tasks"


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _seed_contract(repo: Path) -> None:
    """Copy the live contract in so ``load_branch_isolation_policy`` resolves.

    Only the non-fallback arms reach the allow-list loop, but seeding
    unconditionally keeps the control and the fallback arms on one fixture.
    """
    target = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(CONTRACT_SOURCE.read_bytes())


def _make_two_worktrees(tmp_path: Path) -> tuple[Path, Path]:
    """A primary worktree on ``main`` plus a linked worktree on a feature branch."""
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
        "feature/ambiguity-drift",
        str(feature_root),
        cwd=primary,
    )
    return primary, feature_root


def _drifting_payload(primary: Path) -> dict[str, object]:
    """An Edit into the primary worktree while the task targets elsewhere.

    ``_candidate_worktree_root`` probes the parent directory when the file does
    not exist yet (the common Edit/Write case), so the parent must be real.
    """
    (primary / "docs" / "scopes").mkdir(parents=True, exist_ok=True)
    return {
        "toolName": "Edit",
        "toolInput": {
            "file_path": str(primary / "docs" / "scopes" / "would-be-drift.md")
        },
    }


def test_context_carries_a_resolution_note() -> None:
    """The field the whole valve hangs on. Absent here, every arm below is moot."""
    context = ActiveTaskContext(
        task_ref="internal",
        target_worktree="/tmp/somewhere",
        target_branch="feature/example",
        primary_worktree="/tmp/primary",
        resolution_note=NOTE,
    )
    assert context.resolution_note == NOTE


def test_ambiguous_resolution_falls_back_instead_of_blocking(tmp_path: Path) -> None:
    """A guessed task row must not be used to adjudicate candidate-path drift."""
    primary, feature_root = _make_two_worktrees(tmp_path)
    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/ambiguity-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
        resolution_note=NOTE,
    )

    decision = evaluate_payload(
        _drifting_payload(primary), workspace_root=primary, active_task=context
    )

    assert decision is not None, (
        "the ambiguity valve must report a decision, not fall silent"
    )
    assert decision.outcome == "fallback", (
        "drift was adjudicated against a guessed task row; a mis-picked row "
        f"blocks edits belonging to another task. got outcome={decision.outcome!r}"
    )
    assert decision.reason == NOTE, (
        "the fallback must surface the tiebreak note as its reason so an "
        f"operator can see why the guard stood down; got {decision.reason!r}"
    )


def test_ambiguity_note_does_not_exempt_the_root_worktree_guard(tmp_path: Path) -> None:
    """Ordering pin: the root worktree must stay on main even under ambiguity.

    This guard keys on the *live* branch and primary-worktree identity, not on
    the guessed task row, so the ambiguity rationale does not apply to it.
    A fallback emitted before this check lets an ambiguous multi-row database
    slip a root-worktree feature-branch edit through.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    _git("checkout", "-q", "-b", "feature/root-is-drifted", cwd=primary)

    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/ambiguity-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
        resolution_note=NOTE,
    )

    decision = evaluate_payload(
        _drifting_payload(primary), workspace_root=primary, active_task=context
    )

    assert decision is not None and decision.outcome == "block", (
        "an ambiguity note exempted the root-worktree-on-non-main guard; got "
        f"{decision.outcome if decision else None!r}"
    )
    assert "RootWorktreeNotOnMainError" in (decision.reason or ""), (
        f"blocked for the wrong reason: {decision.reason!r}"
    )


def test_ambiguity_note_does_not_exempt_the_probe_error_gate(tmp_path: Path) -> None:
    """Ordering pin: could-not-determine stays fail-closed under ambiguity.

    ``probe_error`` values outside the allow-list mean the probe could not
    answer at all. That is a different failure from "answered, but ambiguously",
    and the valve must not launder one into the other.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/ambiguity-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
        resolution_note=NOTE,
        probe_error="handoff_probe_exploded",
    )

    decision = evaluate_payload(
        _drifting_payload(primary), workspace_root=primary, active_task=context
    )

    assert decision is not None and decision.outcome == "block", (
        "an ambiguity note downgraded a non-allowlisted probe_error from block; "
        f"got {decision.outcome if decision else None!r}"
    )


@pytest.mark.parametrize("value", ["0", "false", "off", "no", "FALSE", " Off "])
def test_kill_switch_disables_the_valve(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """The operator switch that forces fail-closed resolution."""
    monkeypatch.setenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", value)
    assert atc._ambiguity_fallback_disabled() is True


@pytest.mark.parametrize("value", ["1", "true", "on", ""])
def test_kill_switch_left_on_keeps_the_valve(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """Control: only the explicit off-tokens disable it, so the default stands."""
    monkeypatch.setenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", value)
    assert atc._ambiguity_fallback_disabled() is False


def test_without_a_resolution_note_drift_still_blocks(tmp_path: Path) -> None:
    """Control: the valve must not leak into the unambiguous path.

    Same fixture as the fallback arm with the note removed. If this turns green
    only because everything now falls back, the guard has been retired rather
    than refined.
    """
    primary, feature_root = _make_two_worktrees(tmp_path)
    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/ambiguity-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
    )

    decision = evaluate_payload(
        _drifting_payload(primary), workspace_root=primary, active_task=context
    )

    assert decision is not None and decision.outcome == "block", (
        f"unambiguous drift stopped blocking; got {decision.outcome if decision else None!r}"
    )


def test_edit_into_the_target_worktree_is_still_allowed(tmp_path: Path) -> None:
    """Control: the guard still permits work in the task's own worktree."""
    primary, feature_root = _make_two_worktrees(tmp_path)
    (feature_root / "docs").mkdir(parents=True, exist_ok=True)
    context = ActiveTaskContext(
        task_ref="internal",
        target_branch="feature/ambiguity-drift",
        target_worktree=str(feature_root),
        primary_worktree=str(primary),
    )

    decision = evaluate_payload(
        {
            "toolName": "Edit",
            "toolInput": {"file_path": str(feature_root / "docs" / "ok.md")},
        },
        workspace_root=primary,
        active_task=context,
    )

    assert decision is None, f"an in-target edit was not allowed: {decision!r}"
