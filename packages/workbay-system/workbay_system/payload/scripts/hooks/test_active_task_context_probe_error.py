"""A failed active-task probe must not be indistinguishable from "no task".

internal. ``_load_active_task`` currently collapses three different
situations into the same value -- ``ActiveTaskContext(task_ref=None, ...)``:

* the handoff package cannot be imported at all,
* ``get_handoff_state`` raised,
* the handoff DB answered cleanly and there genuinely is no active task.

Callers therefore cannot tell "I could not determine this" from "the answer is
no", and every one of them that phrases the negative as a fact is asserting
something it never established. ``guard-main-branch.sh`` is the live repro: its
probe has been failing against a schema-mismatched CLI, and the hook has been
printing ``WARNING: Editing on main without an active handoff task`` on every
edit while an active task existed the whole time.

Silence from a dead probe must not read as health [OBS-08], and a swallowed
error must still leave a durable trace rather than vanish into a default
[AGT-10]. The repo already holds this line for slow git scans -- see
``test_branch_isolation_guard_timeout.py``, where a timeout is
could-not-determine and never a crash. This pins the same distinction for the
active-task probe.

The mechanism: ``ActiveTaskContext`` grows a defaulted ``probe_error`` field
(the dataclass already documents that convention for ``task_plan_path``), set to
a short reason string when the probe could not answer and left ``None`` when it
did. Defaulted, so the existing 4-positional constructions keep working.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent
if str(HOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(HOOKS_DIR))

import _active_task_context as atc  # noqa: E402


def _snapshot_dict(root: Path, *, shape: str = "none", **fields) -> dict:
    """Build CURRENT_TASK.json payload with production authority fields.

    Defaults point at conventional ``.task-state`` paths under ``root``;
    callers override for relocated-authority or absent-path carve-outs.
    """
    return {
        "schema_version": 2,
        "shape": shape,
        "authority_db_path": str(root / ".task-state" / "handoff.db"),
        "authority_projection_dir": str(root / ".task-state" / "current"),
        **fields,
    }


def test_import_failure_is_reported_as_a_probe_error(tmp_path, monkeypatch):
    """No handoff package -> could-not-determine, not "no active task"."""
    monkeypatch.setattr(atc, "_load_handoff_exports", lambda: None)
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error, (
        "the handoff package could not be imported, so the probe never ran -- "
        "returning task_ref=None with no probe_error tells every caller the "
        "answer is 'no active task', which was never established"
    )


def test_raising_probe_is_reported_as_a_probe_error(tmp_path, monkeypatch):
    """The live repro: the probe blew up and the failure was swallowed.

    A distinct arm from the import failure because they take different branches
    and each owns its own early return -- one can be fixed while the other
    silently keeps conflating.
    """

    class _Runtime:
        workspace_root = str(tmp_path)

        @staticmethod
        def for_repo(root):  # noqa: ANN001, ANN205 - test stub
            return _Runtime()

    def _boom(*args, **kwargs):  # noqa: ANN002, ANN003 - test stub
        raise RuntimeError("schema_version_mismatch: db user_version=33 package 32")

    monkeypatch.setattr(
        atc,
        "_load_handoff_exports",
        lambda: (_Runtime, lambda runtime: None, _boom, ValueError),
    )
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error, (
        "get_handoff_state raised and the exception was swallowed into a "
        "task_ref=None default -- the failure must survive as a reason string"
    )


def test_a_clean_negative_answer_carries_no_probe_error(tmp_path, monkeypatch):
    """Control arm: a probe that ran and found nothing is NOT an error.

    Without this, "always set probe_error" satisfies both arms above and the
    field becomes a constant that tells callers nothing. The two states must be
    distinguishable in both directions.
    """

    class _Runtime:
        workspace_root = str(tmp_path)

        @staticmethod
        def for_repo(root):  # noqa: ANN001, ANN205 - test stub
            return _Runtime()

    monkeypatch.setattr(
        atc,
        "_load_handoff_exports",
        lambda: (
            _Runtime,
            lambda runtime: None,
            lambda **kwargs: {"ok": True, "data": {"active": {}}},
            ValueError,
        ),
    )
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error is None, (
        f"the probe answered cleanly with no active task, so this is a real "
        f"negative -- probe_error must stay None, got {ctx.probe_error!r}"
    )


def test_a_resolved_task_carries_no_probe_error(tmp_path, monkeypatch):
    """Second control arm: the happy path must stay clean.

    Guards against a fix that sets probe_error unconditionally on any path that
    does not raise.
    """

    class _Runtime:
        workspace_root = str(tmp_path)

        @staticmethod
        def for_repo(root):  # noqa: ANN001, ANN205 - test stub
            return _Runtime()

    payload = {
        "ok": True,
        "data": {"active": {"task_ref": "internal", "target_branch": "feature/x"}},
    }
    monkeypatch.setattr(
        atc,
        "_load_handoff_exports",
        lambda: (_Runtime, lambda runtime: None, lambda **kwargs: payload, ValueError),
    )
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.probe_error is None


def test_probe_error_field_is_defaulted(tmp_path):
    """Existing positional constructions must keep working [CON-17].

    ``_load_active_task`` builds ``ActiveTaskContext(None, None, None, primary)``
    in several places and other hooks construct it too; a required field would
    break every one of them at import time rather than at the call site.
    """
    ctx = atc.ActiveTaskContext(None, None, None, str(tmp_path))

    assert ctx.probe_error is None


def test_invalid_json_from_package_is_probe_error(tmp_path, monkeypatch):
    """D3: package JSONDecodeError must set probe_error, not look like absence."""

    class _Runtime:
        workspace_root = str(tmp_path)

        @staticmethod
        def for_repo(root):  # noqa: ANN001, ANN205 - test stub
            return _Runtime()

    monkeypatch.setattr(
        atc,
        "_load_handoff_exports",
        lambda: (_Runtime, lambda runtime: None, lambda **kwargs: "not-json", ValueError),
    )
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )
    monkeypatch.setattr(atc, "_try_load_active_task_from_snapshot", lambda _root: None)

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_json_invalid"


def test_missing_active_object_is_probe_error(tmp_path, monkeypatch):
    """D3: package answer without an active object is could-not-determine."""

    class _Runtime:
        workspace_root = str(tmp_path)

        @staticmethod
        def for_repo(root):  # noqa: ANN001, ANN205 - test stub
            return _Runtime()

    monkeypatch.setattr(
        atc,
        "_load_handoff_exports",
        lambda: (
            _Runtime,
            lambda runtime: None,
            lambda **kwargs: {"ok": True, "data": {}},
            ValueError,
        ),
    )
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )
    monkeypatch.setattr(atc, "_try_load_active_task_from_snapshot", lambda _root: None)

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_active_missing"


def test_ambiguous_paused_status_falls_through_not_clean_negative(tmp_path, monkeypatch):
    """workspace_ambiguous entry with status=paused must not clean-negative.

    An unrecognised status is not a confident snapshot answer: the loader must
    return None so the authoritative probe runs rather than affirming absence.
    """
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )
    wt = str(tmp_path.resolve(strict=False))
    # Self-describing authority present but absent on disk → fresh carve-out,
    # so the scan (not freshness) is what decides fallthrough.
    auth_db = tmp_path / "authority" / "handoff.db"
    auth_proj = tmp_path / "authority" / "projections"
    (tmp_path / "CURRENT_TASK.json").write_text(
        json.dumps(
            _snapshot_dict(
                tmp_path,
                shape="workspace_ambiguous",
                authority_db_path=str(auth_db),
                authority_projection_dir=str(auth_proj),
                tasks=[
                    {
                        "task_ref": "internal",
                        "status": "paused",
                        "target_worktree_path": wt,
                        "target_branch": "feature/paused",
                    }
                ],
            )
        ),
        encoding="utf-8",
    )

    result = atc._try_load_active_task_from_snapshot(tmp_path)

    assert result is None, (
        "status=paused is neither live nor terminal-done; snapshot loader must "
        f"return None (fall through), not a clean negative; got {result!r} "
        f"probe_error={getattr(result, 'probe_error', None)!r}"
    )


def test_snapshot_authority_paths_drive_freshness(tmp_path, monkeypatch):
    """Freshness must use embedded authority paths, never a hardcoded guess.

    A snapshot missing authority_db_path is not fresh. A snapshot whose
    authority_db_path names a relocated dir with a newer db is not fresh.
    """
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    # Older snapshot without authority fields → not fresh → fall through.
    snap_path = tmp_path / "CURRENT_TASK.json"
    snap_path.write_text(
        json.dumps({"schema_version": 2, "shape": "none"}),
        encoding="utf-8",
    )
    missing = atc._try_load_active_task_from_snapshot(tmp_path)
    assert missing is None, (
        "snapshot missing authority_db_path must be judged not-fresh and fall "
        f"through (None); got {missing!r}"
    )
    assert (
        atc._snapshot_fresh_enough(
            snap_path, {"schema_version": 2, "shape": "none"}
        )
        is False
    ), "missing authority_db_path field must make _snapshot_fresh_enough False"

    # Relocated authority with a newer db → not fresh.
    relocated = tmp_path / "relocated-state"
    relocated.mkdir()
    db_path = relocated / "handoff.db"
    proj_dir = tmp_path / "projections"
    proj_dir.mkdir()
    snap_mtime = 1_700_000_000.0
    newer = snap_mtime + 120.0
    payload = _snapshot_dict(
        tmp_path,
        shape="none",
        authority_db_path=str(db_path),
        authority_projection_dir=str(proj_dir),
    )
    snap_path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(snap_path, (snap_mtime, snap_mtime))
    db_path.write_bytes(b"")
    os.utime(db_path, (newer, newer))

    relocated_result = atc._try_load_active_task_from_snapshot(tmp_path)
    assert relocated_result is None, (
        "authority_db_path pointing at a newer relocated db must be not-fresh; "
        f"got {relocated_result!r}"
    )
    assert atc._snapshot_fresh_enough(snap_path, payload) is False, (
        "relocated authority_db_path with newer mtime must make "
        "_snapshot_fresh_enough return False"
    )


def test_unresolvable_workspace_root_returns_none_not_probe_error(
    tmp_path, monkeypatch
):
    """Unusable ambiguous-scan outcomes must not emit terminal probe_error.

    Returning a probe_error context short-circuits the authoritative probe and
    wedges the drift guard; fall through with None instead [OBS-08].
    """
    def _boom_expand(self):  # noqa: ANN001
        raise RuntimeError("cannot expanduser")

    monkeypatch.setattr(Path, "expanduser", _boom_expand)
    tasks = [
        {
            "task_ref": "internal",
            "status": "done",
            "target_worktree_path": str(tmp_path),
        }
    ]

    result = atc._scan_workspace_ambiguous_tasks(tasks, tmp_path, str(tmp_path))

    assert result is None, (
        "unresolvable workspace_root must return None, not a context carrying "
        f"probe_error; got {result!r} probe_error="
        f"{getattr(result, 'probe_error', None)!r}"
    )


def _resolved_handoff_exports(tmp_path: Path, get_state):
    class _Runtime:
        workspace_root = str(tmp_path)

        @staticmethod
        def for_repo(root):  # noqa: ANN001, ANN205 - test stub
            return _Runtime()

    return (_Runtime, lambda runtime: None, get_state, ValueError)


def test_handoff_fallback_over_budget_sets_probe_error(tmp_path, monkeypatch):
    """Budget control arm 1: slow fallback is labeled, not silent [OBS-08].

    The answer is still returned (slow is better than none); probe_error
    carries ``handoff_probe_over_budget`` so the cold path is visible.
    """
    import time

    monkeypatch.setattr(atc, "_HANDOFF_FALLBACK_BUDGET_S", 0.01)
    monkeypatch.delenv(atc._HANDOFF_FALLBACK_BUDGET_ENV, raising=False)
    monkeypatch.setattr(atc, "_try_load_active_task_from_snapshot", lambda _root: None)
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    payload = {
        "ok": True,
        "data": {
            "active": {
                "task_ref": "internal",
                "target_branch": "feature/slow",
            }
        },
    }

    def _slow_get_state(*, sections: str = "identity"):
        time.sleep(0.05)
        return payload

    monkeypatch.setattr(
        atc, "_load_handoff_exports", lambda: _resolved_handoff_exports(tmp_path, _slow_get_state)
    )

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref == "internal", (
        "over-budget must still return the probe answer; "
        f"got task_ref={ctx.task_ref!r}"
    )
    assert ctx.probe_error == "handoff_probe_over_budget", (
        "elapsed over budget on the handoff fallback must set "
        f"handoff_probe_over_budget, got {ctx.probe_error!r}"
    )


def test_handoff_fallback_within_budget_leaves_probe_error_none(tmp_path, monkeypatch):
    """Budget control arm 2: same sleep under a high budget stays clean.

    Without this arm the fix degenerates into "always report over budget".
    """
    import time

    monkeypatch.setattr(atc, "_HANDOFF_FALLBACK_BUDGET_S", 5.0)
    monkeypatch.delenv(atc._HANDOFF_FALLBACK_BUDGET_ENV, raising=False)
    monkeypatch.setattr(atc, "_try_load_active_task_from_snapshot", lambda _root: None)
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    payload = {
        "ok": True,
        "data": {
            "active": {
                "task_ref": "internal",
                "target_branch": "feature/fast",
            }
        },
    }

    def _brief_get_state(*, sections: str = "identity"):
        time.sleep(0.05)
        return payload

    monkeypatch.setattr(
        atc,
        "_load_handoff_exports",
        lambda: _resolved_handoff_exports(tmp_path, _brief_get_state),
    )

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.probe_error is None, (
        f"sleep within budget must not set a timing token; got {ctx.probe_error!r}"
    )


def test_over_budget_relabel_preserves_every_other_field():
    """The over-budget relabel adds probe_error and changes nothing else.

    This twin's ``ActiveTaskContext`` carries a ``resolution_note`` the root
    copy does not have, so ``_with_over_budget_probe_error`` has one more field
    to forward than its root counterpart. Rebuilding it positionally — the
    natural thing to do when porting from root — silently shifts every value
    one slot and still returns a plausible-looking context.

    Asserting field-by-field rather than on ``resolution_note`` alone: the
    mis-assignment failure mode is about *all* the fields, and a pin that only
    watches one of them still passes while the rest are scrambled.
    """
    original = atc.ActiveTaskContext(
        task_ref="internal",
        target_worktree="/wt/feature",
        target_branch="feature/port",
        primary_worktree="/primary",
        task_plan_path="docs/tasks/internal.md",
        resolution_note="ambiguous active task; picked most recent",
        probe_error=None,
    )

    relabeled = atc._with_over_budget_probe_error(original)

    assert relabeled.probe_error == "handoff_probe_over_budget"
    for field in (
        "task_ref",
        "target_worktree",
        "target_branch",
        "primary_worktree",
        "task_plan_path",
        "resolution_note",
    ):
        assert getattr(relabeled, field) == getattr(original, field), (
            f"{field} must survive the over-budget relabel unchanged; "
            f"got {getattr(relabeled, field)!r}, expected "
            f"{getattr(original, field)!r}"
        )


def test_over_budget_relabel_keeps_a_more_specific_probe_error():
    """A context that already failed for a named reason keeps that reason.

    Over-budget is the weakest diagnosis available; it must not overwrite a
    caller's more specific one [OBS-08].
    """
    original = atc.ActiveTaskContext(
        task_ref=None,
        target_worktree=None,
        target_branch=None,
        primary_worktree="/primary",
        task_plan_path=None,
        resolution_note=None,
        probe_error="handoff_import_failed",
    )

    assert atc._with_over_budget_probe_error(original) is original


def test_live_snapshot_skips_fallback_and_timing_token(tmp_path, monkeypatch):
    """Fast-path control: a live snapshot must not run the handoff fallback.

    Regression arm for the whole point of CURRENT_TASK.json — the cold path
    is not taken, so no over-budget token can appear.
    """
    monkeypatch.setattr(
        atc, "_primary_workspace_root", lambda root, *, timeout=5.0: str(root)
    )

    # Defined older authority so freshness is true under fail-closed rules
    # (finding 9943: absent authority is no longer a freshness carve-out).
    import os

    auth_db = tmp_path / "authority" / "handoff.db"
    auth_proj = tmp_path / "authority" / "projections"
    auth_db.parent.mkdir(parents=True, exist_ok=True)
    auth_proj.mkdir(parents=True, exist_ok=True)
    auth_db.write_bytes(b"")
    target = tmp_path / "wt-feature"
    snap_path = tmp_path / "CURRENT_TASK.json"
    snap_path.write_text(
        json.dumps(
            _snapshot_dict(
                tmp_path,
                shape="single",
                authority_db_path=str(auth_db),
                authority_projection_dir=str(auth_proj),
                active={
                    "task_ref": "internal",
                    "status": "in_progress",
                    "target_worktree_path": str(target),
                    "target_branch": "feature/snap",
                },
            )
        ),
        encoding="utf-8",
    )
    older = snap_path.stat().st_mtime - 120.0
    os.utime(auth_db, (older, older))

    def _must_not_run() -> None:
        raise AssertionError("handoff fallback must not run when snapshot answered")

    monkeypatch.setattr(atc, "_load_handoff_exports", _must_not_run)

    ctx = atc._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.probe_error is None, (
        f"live snapshot answer must not carry a timing token; got {ctx.probe_error!r}"
    )


def test_git_primary_timeout_absent_from_source() -> None:
    """Dead-token control: git_primary_timeout appears nowhere in the module."""
    source_path = Path(atc.__file__).resolve()
    source = source_path.read_text(encoding="utf-8")
    assert "git_primary_timeout" not in source, (
        f"git_primary_timeout must be deleted from {source_path.name}; "
        "no code path can raise subprocess.TimeoutExpired on primary resolution"
    )
