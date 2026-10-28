"""Hook-facing task resolution must not depend on the handoff package.

internal. ``_load_active_task`` answers by importing
``workbay_handoff_mcp`` and calling ``get_handoff_state``. Both assumptions
fail where it matters most -- inside a ``PreToolUse`` hook:

1. **The interpreter cannot import it.** Hooks are spawned with bare
   ``python3``, which on the primary host is a pyenv shim. Measured: the shim
   yields ``probe_error='handoff_unavailable'`` while ``.venv/bin/python``
   resolves the task. So every hook invocation takes the could-not-determine
   path, and a guard that can only ever say "I don't know" is a guard that never
   speaks [OBS-08]. Replacing a false warning with permanent silence is not a
   fix; it is a harder-to-notice version of the same defect.

2. **Where it does import, it is far too slow.** Measured on the working
   interpreter: 40.7s and 44.0s wall against a 5s harness budget, of which only
   ~1.5s is CPU. The cost is ``get_handoff_state`` against a 154MB SQLite file,
   and the *second* call is slower than the first, so it is not cold-cache
   warmup. The console script this replaced cost 2118ms.

This matters beyond the advisory main-branch guard: ``_worktree_drift.py`` --
the *blocking* ``PreToolUse`` guard matched on ``Edit|Write`` -- calls the same
resolver, as does ``advise-worktree-cd.py``. The originally reported symptom
(``PreToolUse:Edit`` timing out 82 times in 2227 runs, ``Write`` 59 in 1375) is
this resolver on the blocking path, where a killed hook denies the edit.

The invariant these arms pin: **hook-facing resolution answers from the
maintained snapshot, using only the standard library, or it says so** -- it
never reports "could not determine" as "no active task", and never reports a
task that is not there.

A snapshot read was measured at 1.6ms against the same question.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent


def _find_repo_root(start: Path) -> Path:
    """Resolve monorepo root from either scripts/hooks or the payload twin.

    The payload copy lives five levels deeper than ``scripts/hooks``, so a
    fixed ``parents[1]`` only works for the root twin. Walk upward for the
    directory that holds both hook trees.
    """
    for candidate in (start, *start.parents):
        root_hooks = candidate / "scripts" / "hooks" / "_active_task_context.py"
        payload_hooks = (
            candidate
            / "packages"
            / "workbay-system"
            / "workbay_system"
            / "payload"
            / "scripts"
            / "hooks"
            / "_active_task_context.py"
        )
        if root_hooks.is_file() and payload_hooks.is_file():
            return candidate
    return start.parents[1]


REPO_ROOT = _find_repo_root(HOOKS_DIR)
PAYLOAD_HOOKS_DIR = (
    REPO_ROOT
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "scripts"
    / "hooks"
)

# Freshness contract must hold on both hand-maintained twins.
FRESHNESS_TWINS = [
    pytest.param(HOOKS_DIR, id="root"),
    pytest.param(PAYLOAD_HOOKS_DIR, id="payload"),
]


def _load_module(hooks_dir: Path | None = None):
    hooks_dir = hooks_dir or HOOKS_DIR
    name = f"_active_task_context_under_test_{hooks_dir.resolve().as_posix().replace('/', '_')}"
    spec = importlib.util.spec_from_file_location(
        name, hooks_dir / "_active_task_context.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Register before exec: the module defines a @dataclass, and dataclasses
    # resolves string annotations through sys.modules[cls.__module__]. Without
    # this the import itself raises, which would redden every arm below for a
    # reason unrelated to the property each one names.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _snapshot_dict(root: Path, *, shape: str = "workspace_ambiguous", **fields) -> dict:
    """Build CURRENT_TASK.json in the shape production emits (schema 2).

    Always embeds authority_db_path / authority_projection_dir at the
    conventional ``.task-state`` locations under ``root`` so freshness uses
    the same paths the tests seed for handoff.db and projections. Callers may
    override either field via ``**fields``.
    """
    return {
        "schema_version": 2,
        "shape": shape,
        "staleness_note": "May lag; authoritative state via load_session.",
        "generated_at": "2026-07-29T03:23:00Z",
        "authority_db_path": str(root / ".task-state" / "handoff.db"),
        "authority_projection_dir": str(root / ".task-state" / "current"),
        **fields,
    }


def _seed_older_authority(root: Path) -> Path:
    """Create ``.task-state/handoff.db`` older than a soon-to-be-written snapshot.

    Freshness requires a defined authority (finding 9943). Non-freshness arms
    need the fast path reachable, so seed an older main DB before writing the
    snapshot. Callers that deliberately test absent authority must remove it.
    """
    import os

    state_dir = root / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    if not db_path.exists():
        db_path.write_bytes(b"")
    # Stamp an older mtime now; the snapshot write that follows is newer.
    older = db_path.stat().st_mtime - 120.0
    os.utime(db_path, (older, older))
    return db_path


def _write_snapshot(root: Path, *, shape: str = "workspace_ambiguous", **fields) -> None:
    """Write the maintained snapshot in the shape production emits (schema 2)."""
    import os

    root.mkdir(parents=True, exist_ok=True)
    # Defined authority so non-freshness arms stay on the fast path [9943].
    db_path = _seed_older_authority(root)
    payload = _snapshot_dict(root, shape=shape, **fields)
    snap_path = root / "CURRENT_TASK.json"
    snap_path.write_text(json.dumps(payload), encoding="utf-8")
    # Guarantee snap >= authority even on coarse FS clocks.
    snap_mtime = snap_path.stat().st_mtime
    older = snap_mtime - 120.0
    os.utime(db_path, (older, older))


def _write_ambiguous(root: Path, tasks: list[dict]) -> None:
    _write_snapshot(root, shape="workspace_ambiguous", tasks=tasks)


def _without_handoff_package(monkeypatch, module):
    """Simulate the production interpreter, which cannot import the package.

    This is not a hypothetical: the pyenv shim that hooks are spawned with
    fails exactly here, which is why this whole path is dead in production
    while the existing suite -- which stubs the resolver outright -- stays
    green.
    """
    real_import = importlib.import_module

    def _blocked(name, *args, **kwargs):
        if name == "workbay_handoff_mcp":
            raise ImportError("No module named 'workbay_handoff_mcp'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(module.importlib, "import_module", _blocked)


def _pin_primary(monkeypatch, module, root: Path):
    monkeypatch.setattr(
        module,
        "_primary_workspace_root",
        lambda _root, *, timeout=5.0: str(root),
    )


ACTIVE = {
    "task_ref": "internal",
    "status": "in_progress",
    "target_branch": "feature/wb-hook-latency-01",
    "target_worktree_path": "/somewhere/repo-wb-hook-latency-01",
    "objective": "irrelevant to resolution",
}


def _active_here(root: Path, **overrides) -> dict:
    """The active task, named against the worktree the hook is resolving.

    The snapshot is workspace-wide (``shape: workspace_ambiguous``), so a task
    entry only identifies *this* workspace when its ``target_worktree_path``
    points at it. Arms that seed a task without doing so are describing a task
    that belongs to some other worktree.
    """
    return {**ACTIVE, "target_worktree_path": str(root), **overrides}


def test_resolution_survives_an_unimportable_handoff_package(monkeypatch, tmp_path):
    """The production case: no handoff package, but a snapshot is right there.

    This is the arm that is red today. The resolver gives up the moment the
    import fails, even though the answer sits in a 109KB JSON file that parses
    in ~1.6ms and needs nothing outside the standard library.
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "internal", (
        "the resolver could not answer without the handoff package, even with "
        "the maintained snapshot present. In production every hook invocation "
        "takes this path, so the guard is permanently silent."
    )
    assert ctx.probe_error is None, (
        f"a snapshot answer is an answer, not a degraded one: {ctx.probe_error!r}"
    )
    assert ctx.target_branch == "feature/wb-hook-latency-01"


def test_a_genuine_absence_is_still_reported_as_absence(monkeypatch, tmp_path):
    """Terminal-only projection status must fall through, not clean-negative.

    Finding 9984: projection ``status`` can lag the DB (spool fail-open). A
    ``done`` entry is therefore not proof of absence — the snapshot path must
    return None so the authoritative probe answers. With no package that is
    handoff_unavailable (could-not-determine), not probe_error=None ALLOW.
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(tmp_path, [_active_here(tmp_path, status="done")])
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None, (
        f"terminal-only projection must fall through (None), not clean negative; "
        f"got {snap!r}"
    )

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None, "a completed task must not count as active"
    assert ctx.probe_error == "handoff_unavailable", (
        "terminal-only snapshot is not a clean negative; after fallthrough with "
        f"no package expected handoff_unavailable, got {ctx.probe_error!r}"
    )


def test_a_missing_snapshot_is_could_not_determine_not_absence(monkeypatch, tmp_path):
    """Control: with neither package nor snapshot, say so.

    This is the distinction the whole branch exists to preserve. Losing it here
    would reintroduce the original defect -- a probe that cannot answer,
    phrased to the operator as 'without an active handoff task' [OBS-08].
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_unavailable", (
        f"no package and no snapshot means the probe could not determine "
        f"anything; expected handoff_unavailable, got {ctx.probe_error!r}"
    )


def test_a_corrupt_snapshot_falls_through_to_handoff_probe(monkeypatch, tmp_path):
    """Malformed snapshot is not evidence about the task — fall through [OBS-08].

    An unusable CURRENT_TASK.json must return None from the snapshot loader so
    the authoritative probe runs. With no handoff package that probe ends as
    handoff_unavailable (allowlisted by the drift guard), not a terminal
    snapshot_* token that hard-blocks primary-checkout edits.
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "CURRENT_TASK.json").write_text(
        '{"schema_version": 2, "shape": "workspace_ambiguous", "tasks": [{"task_',
        encoding="utf-8",
    )
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None, (
        f"unusable snapshot must fall through (None), not a probe_error context; "
        f"got {snap!r}"
    )

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_unavailable", (
        f"after fallthrough with no package, expected handoff_unavailable, "
        f"got {ctx.probe_error!r}"
    )


def test_corrupt_snapshot_with_importable_package_falls_through(
    monkeypatch, tmp_path
):
    """Unusable snapshot must consult the package path, not terminalise.

    A present-but-unusable snapshot is not a final answer. The loader must
    fall through so the authoritative probe can answer.
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "CURRENT_TASK.json").write_text("{not-json", encoding="utf-8")

    called: list[str] = []

    def _exports():
        called.append("exports")
        return None

    monkeypatch.setattr(module, "_load_handoff_exports", _exports)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert called == ["exports"], "package path must run after an unusable snapshot"
    assert ctx.probe_error == "handoff_unavailable"


def test_the_matching_worktree_wins_among_many_active_tasks(monkeypatch, tmp_path):
    """The production shape: 46 tasks, 44 of them in_progress.

    The snapshot's own ``shape`` is ``workspace_ambiguous`` -- it is a
    workspace-wide list, not a pointer to one task. So ``in_progress`` alone
    never identifies the active task, and a resolver that returns the first
    in_progress entry would answer confidently and wrongly on every real
    invocation while passing an arm that seeds a single-element list.

    What disambiguates is the worktree the hook is running in:
    ``target_worktree_path`` must match the workspace being resolved.
    """
    module = _load_module()
    here = tmp_path / "repo-wb-hook-latency-01"
    here.mkdir()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(
        tmp_path,
        [
            {**ACTIVE, "task_ref": "internal", "target_worktree_path": "/elsewhere/repo-a"},
            {**ACTIVE, "task_ref": "internal", "target_worktree_path": "/elsewhere/repo-b"},
            {**ACTIVE, "task_ref": "internal", "target_worktree_path": str(here)},
            {**ACTIVE, "task_ref": "internal", "target_worktree_path": "/elsewhere/repo-c"},
        ],
    )
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(here)

    assert ctx.task_ref == "internal", (
        "four tasks are in_progress and only one names this worktree; the "
        "resolver must match on target_worktree_path rather than picking the "
        "first active entry it sees"
    )
    assert ctx.probe_error is None


def test_many_active_tasks_and_no_match_falls_through(monkeypatch, tmp_path):
    """No worktree match is not a confident snapshot answer — fall through.

    When several tasks are active and none names this worktree, the snapshot
    loader returns None so the authoritative probe runs. Guessing a candidate
    or emitting a terminal snapshot_* probe_error both hard-wedge the primary
    checkout under the drift guard.
    """
    module = _load_module()
    here = tmp_path / "unknown-worktree"
    here.mkdir()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(
        tmp_path,
        [
            {**ACTIVE, "task_ref": "internal", "target_worktree_path": "/elsewhere/repo-a"},
            {**ACTIVE, "task_ref": "internal", "target_worktree_path": "/elsewhere/repo-b"},
        ],
    )
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(here)
    assert snap is None, (
        f"no worktree match must fall through (None); got {snap!r}"
    )

    ctx = module._load_active_task(here)

    assert ctx.task_ref is None, (
        "no task in the snapshot names this worktree, so naming one of them "
        "is a fabricated answer"
    )
    assert ctx.probe_error == "handoff_unavailable", (
        f"after fallthrough with no package, expected handoff_unavailable, "
        f"got {ctx.probe_error!r}"
    )


def test_the_snapshot_path_does_not_import_the_handoff_package(monkeypatch, tmp_path):
    """The cost pin, asserted structurally rather than as a wall-clock bound.

    A timing assertion here would be flaky and machine-specific. What actually
    has to hold is that the fast path never reaches the 154MB database at all:
    if it imports the package, it will pay the 14-20s query on the blocking
    edit path no matter how the timing happens to land on one host.
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])

    imported: list[str] = []
    real_import = importlib.import_module

    def _record(name, *args, **kwargs):
        imported.append(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(module.importlib, "import_module", _record)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert "workbay_handoff_mcp" not in imported, (
        "the snapshot already answered, but the resolver still imported the "
        "handoff package -- which is where the 14-20s query lives. This path "
        "runs on the blocking Edit|Write guard."
    )


# --- D1: all three snapshot shapes -------------------------------------------------


def test_shape_single_answers_from_active(monkeypatch, tmp_path):
    """D1: shape=single must answer without falling through to the package."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(
        tmp_path,
        shape="single",
        task_ref="internal",
        active=_active_here(tmp_path),
    )
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.probe_error is None
    assert ctx.target_branch == "feature/wb-hook-latency-01"


def test_shape_none_is_clean_negative(monkeypatch, tmp_path):
    """D1: shape=none is an authoritative clean negative."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error is None


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_stale_snapshot_when_handoff_db_newer_returns_none(
    hooks_dir, monkeypatch, tmp_path
):
    """REVHOO-RED-01: stale CURRENT_TASK.json must not replace the handoff probe.

    Measured live: CURRENT_TASK.json was 11 minutes older than
    ``.task-state/handoff.db`` while DASHBOARD.txt had been regenerated.
    ``_load_active_task`` returned the snapshot whenever it was non-None
    (including shape==\"none\", a clean negative), so the main-branch guard
    failed OPEN on a stale cache.

    Contract: snapshot is a freshness-checked cache. When handoff.db is newer
    (or unstattable), ``_try_load_active_task_from_snapshot`` returns None so
    the caller falls through to the probe. Stat-only — no subprocess, no
    package import. Both root and payload twins.
    """
    import os
    import time

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")

    state_dir = tmp_path / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    db_path.write_bytes(b"")
    # Ensure DB mtime is strictly newer than the snapshot (the live defect).
    snap_path = tmp_path / "CURRENT_TASK.json"
    snap_mtime = snap_path.stat().st_mtime
    newer = snap_mtime + 120.0
    os.utime(db_path, (newer, newer))
    # Guard against coarse filesystem clocks collapsing the delta.
    if db_path.stat().st_mtime <= snap_path.stat().st_mtime:
        time.sleep(0.05)
        db_path.write_bytes(b"")
        newer = snap_path.stat().st_mtime + 120.0
        os.utime(db_path, (newer, newer))

    result = module._try_load_active_task_from_snapshot(tmp_path)

    assert result is None, (
        f"{hooks_dir.name} twin: CURRENT_TASK.json shape=none with a newer "
        ".task-state/handoff.db must return None from the snapshot path so "
        f"_load_active_task falls through to the handoff probe; got {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_missing_handoff_db_still_uses_snapshot(hooks_dir, monkeypatch, tmp_path):
    """Fully absent authority is not freshness — fall through [finding 9943].

    Formerly encoded fail-open: missing handoff.db treated as "no newer
    authority" and served the snapshot. ``.task-state/`` is disposable, so an
    arbitrarily old CURRENT_TASK.json must not answer when authority is gone.
    Both twins.
    """
    import shutil

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    # Remove the fixture-seeded authority so none remains on disk.
    shutil.rmtree(tmp_path / ".task-state", ignore_errors=True)
    assert not (tmp_path / ".task-state").exists()

    result = module._try_load_active_task_from_snapshot(tmp_path)

    assert result is None, (
        f"{hooks_dir.name} twin: CURRENT_TASK.json with fully absent authority "
        f"must return None so the probe runs; got {result!r}"
    )
    snap_path = tmp_path / "CURRENT_TASK.json"
    assert module._snapshot_fresh_enough(
        snap_path, _snapshot_dict(tmp_path, shape="none")
    ) is False


# --- Authority timestamp (WAL-aware freshness) ------------------------------------


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_wal_sidecar_newer_than_snapshot_is_not_fresh(
    hooks_dir, monkeypatch, tmp_path
):
    """Main DB older than snapshot but -wal newer must answer not-fresh.

    WAL commits leave the main file mtime unchanged while the sidecar moves.
    Comparing only the main file would serve a stale snapshot as authoritative.
    """
    import os

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    snap_path = tmp_path / "CURRENT_TASK.json"
    snap_mtime = snap_path.stat().st_mtime

    state_dir = tmp_path / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    wal_path = Path(str(db_path) + "-wal")
    db_path.write_bytes(b"")
    wal_path.write_bytes(b"wal")
    older = snap_mtime - 120.0
    newer = snap_mtime + 120.0
    os.utime(db_path, (older, older))
    os.utime(wal_path, (newer, newer))

    result = module._try_load_active_task_from_snapshot(tmp_path)

    assert result is None, (
        f"{hooks_dir.name} twin: main handoff.db older than CURRENT_TASK.json "
        "but a newer -wal sidecar must return None so the probe runs; "
        f"got {result!r}"
    )
    assert module._snapshot_fresh_enough(
        snap_path, _snapshot_dict(tmp_path, shape="none")
    ) is False


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_main_db_absent_with_wal_sidecar_is_not_fresh(
    hooks_dir, monkeypatch, tmp_path
):
    """Main database absent with -wal present is partially initialised → not-fresh."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    snap_path = tmp_path / "CURRENT_TASK.json"

    state_dir = tmp_path / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    wal_path = Path(str(db_path) + "-wal")
    # Fixture seeds an older main DB for non-freshness arms; remove it so this
    # arm exercises partial-init (main missing, -wal present).
    if db_path.exists():
        db_path.unlink()
    assert not db_path.exists()
    wal_path.write_bytes(b"wal")

    result = module._try_load_active_task_from_snapshot(tmp_path)

    assert result is None, (
        f"{hooks_dir.name} twin: handoff.db missing with -wal present must "
        f"return None (partially-initialised, not-fresh); got {result!r}"
    )
    assert module._snapshot_fresh_enough(
        snap_path, _snapshot_dict(tmp_path, shape="none")
    ) is False


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_named_db_missing_with_older_projection_is_not_fresh(
    hooks_dir, monkeypatch, tmp_path
):
    """Named authority DB gone must not fail open via surviving projections.

    Reproduce: projection older than the database, snapshot stamped to the
    database mtime, then delete the database (no -wal/-shm). Both present
    must stay fresh; DB deleted with projections surviving must answer
    not-fresh so partial authority absence cannot fail open [OBS-08].
    """
    import os

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)

    state_dir = tmp_path / ".task-state"
    proj_dir = state_dir / "current"
    proj_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    db_path.write_bytes(b"")
    proj_path = proj_dir / "active.json"
    proj_path.write_text("{}", encoding="utf-8")

    db_time = 1_700_000_100.0
    proj_time = 1_700_000_000.0  # older than the database
    os.utime(db_path, (db_time, db_time))
    os.utime(proj_path, (proj_time, proj_time))

    payload = _snapshot_dict(tmp_path, shape="none")
    snap_path = tmp_path / "CURRENT_TASK.json"
    snap_path.write_text(json.dumps(payload), encoding="utf-8")
    # Writer stamps snapshot to the previous authority maximum (the DB).
    os.utime(snap_path, (db_time, db_time))

    assert module._snapshot_fresh_enough(snap_path, payload) is True, (
        f"{hooks_dir.name} twin: db+projection present with snap at db mtime "
        "must be fresh (control arm); got False"
    )
    both_present = module._try_load_active_task_from_snapshot(tmp_path)
    assert both_present is not None, (
        f"{hooks_dir.name} twin: both-present control must keep the fast path; "
        f"got {both_present!r}"
    )

    db_path.unlink()
    assert not db_path.exists()
    assert not Path(str(db_path) + "-wal").exists()
    assert not Path(str(db_path) + "-shm").exists()
    assert proj_path.exists()

    assert module._snapshot_fresh_enough(snap_path, payload) is False, (
        f"{hooks_dir.name} twin: named authority db deleted with older "
        "projection surviving must answer not-fresh; got True"
    )
    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is None, (
        f"{hooks_dir.name} twin: named db missing with surviving projection "
        f"must fall through (None); got {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_all_authority_files_absent_preserves_snapshot_use(
    hooks_dir, monkeypatch, tmp_path
):
    """No main DB, no -wal, no -shm, no projection JSON → not fresh [9943].

    Formerly encoded fail-open (served snapshot when authority was gone).
    Fully absent authority is not evidence of freshness.
    """
    import shutil

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    snap_path = tmp_path / "CURRENT_TASK.json"
    shutil.rmtree(tmp_path / ".task-state", ignore_errors=True)
    db_path = tmp_path / ".task-state" / "handoff.db"
    assert not db_path.exists()
    assert not Path(str(db_path) + "-wal").exists()
    assert not Path(str(db_path) + "-shm").exists()
    assert not (tmp_path / ".task-state").exists()

    result = module._try_load_active_task_from_snapshot(tmp_path)

    assert result is None, (
        f"{hooks_dir.name} twin: completely absent authority must fall through "
        f"(None); got {result!r}"
    )
    assert module._snapshot_fresh_enough(
        snap_path, _snapshot_dict(tmp_path, shape="none")
    ) is False


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_quiescent_snapshot_equals_authority_is_fresh(
    hooks_dir, monkeypatch, tmp_path
):
    """Snapshot mtime equal to authority keeps the fast path reachable."""
    import os

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    snap_path = tmp_path / "CURRENT_TASK.json"

    state_dir = tmp_path / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "handoff.db"
    db_path.write_bytes(b"")
    authority = 1_700_000_000.0
    os.utime(db_path, (authority, authority))
    os.utime(snap_path, (authority, authority))

    result = module._try_load_active_task_from_snapshot(tmp_path)

    assert result is not None, (
        f"{hooks_dir.name} twin: snapshot mtime equal to authority must stay "
        "fresh so the fast path remains reachable; got None"
    )
    assert result.task_ref is None
    assert result.probe_error is None, (
        f"{hooks_dir.name} twin: expected probe_error=None, "
        f"got probe_error={result.probe_error!r}"
    )
    assert module._snapshot_fresh_enough(
        snap_path, _snapshot_dict(tmp_path, shape="none")
    ) is True
    assert module._handoff_db_authority_mtime(db_path) == authority


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_identity_degrade_missing_db_does_not_clean_negative(
    hooks_dir, monkeypatch, tmp_path
):
    """Identity-degrade + missing-DB must not fail OPEN as a clean negative.

    When ``_primary_workspace_root`` cannot resolve (identity helper unimportable)
    and ``.task-state/handoff.db`` is absent, a present shape=none snapshot under
    a fabricated workspace_root anchor used to return probe_error=None. The
    main-branch guard then treats "no active task" as fact. Fall through to the
    handoff probe so the composed path carries could-not-determine. Both twins.
    """
    module = _load_module(hooks_dir)
    # Do not pin primary: force the real resolver, then make identity unimportable.
    monkeypatch.setitem(sys.modules, "_worktree_identity", None)
    _write_snapshot(tmp_path, shape="none")
    # Identity-degrade path returns before freshness; drop seeded authority so
    # the composed missing-DB story remains explicit.
    db_path = tmp_path / ".task-state" / "handoff.db"
    if db_path.exists():
        db_path.unlink()
    assert not db_path.exists()
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None, (
        f"{hooks_dir.name} twin: unresolvable primary must not answer from the "
        f"snapshot path; got {snap!r}"
    )

    ctx = module._load_active_task(tmp_path)
    assert ctx.task_ref is None
    assert ctx.probe_error is not None, (
        f"{hooks_dir.name} twin: identity-degrade + missing-DB + shape=none must "
        f"be could-not-determine, not a clean negative; probe_error={ctx.probe_error!r}"
    )


def test_shape_workspace_ambiguous_scans_tasks(monkeypatch, tmp_path):
    """D1: shape=workspace_ambiguous keeps the existing worktree scan."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.probe_error is None


def test_unrecognised_shape_falls_through(monkeypatch, tmp_path):
    """D1: unknown shape is not a confident snapshot answer — fall through."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="future_shape_v9")
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_unavailable"


def test_unrecognised_schema_version_falls_through(monkeypatch, tmp_path):
    """D1: unknown schema_version must fall through, not terminalise."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "CURRENT_TASK.json").write_text(
        json.dumps(
            {
                "schema_version": 99,
                "shape": "single",
                "task_ref": "internal",
                "active": _active_here(tmp_path),
            }
        ),
        encoding="utf-8",
    )
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_unavailable"


# --- D2: live entry preferred over terminal; order-independent --------------------


def test_live_entry_preferred_over_terminal_same_worktree(monkeypatch, tmp_path):
    """D2: a done entry must not mask a live one for the same worktree."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    done = _active_here(tmp_path, task_ref="AAA-FINISHED", status="done")
    live = _active_here(tmp_path, task_ref="ZZZ-LIVE", status="in_progress")
    _write_ambiguous(tmp_path, [done, live])
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "ZZZ-LIVE"
    assert ctx.probe_error is None


def test_scan_is_order_independent_for_live_over_terminal(monkeypatch, tmp_path):
    """D2: both list orders must yield the same live answer."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    done = _active_here(tmp_path, task_ref="AAA-FINISHED", status="done")
    live = _active_here(tmp_path, task_ref="ZZZ-LIVE", status="in_progress")
    _without_handoff_package(monkeypatch, module)

    _write_ambiguous(tmp_path, [done, live])
    first = module._load_active_task(tmp_path)
    _write_ambiguous(tmp_path, [live, done])
    second = module._load_active_task(tmp_path)

    assert first.task_ref == second.task_ref == "ZZZ-LIVE"
    assert first.probe_error is second.probe_error is None


def test_two_live_entries_same_worktree_falls_through(monkeypatch, tmp_path):
    """D2: two live matches for one worktree is not a confident snapshot answer."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    a = _active_here(tmp_path, task_ref="TASK-A", status="in_progress")
    b = _active_here(tmp_path, task_ref="TASK-B", status="in_progress")
    _write_ambiguous(tmp_path, [a, b])
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None, (
        f"two live matches must fall through (None), not guess or terminalise; "
        f"got {snap!r}"
    )

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_unavailable"


# --- D4: dead git_primary_timeout instrumentation removed -------------------------


def test_git_primary_timeout_token_absent_from_module_source() -> None:
    """Dead-token control: git_primary_timeout must not reappear [OBS-08].

    Primary-root resolution is pure filesystem work and spawns no child
    process, so subprocess.TimeoutExpired / git_primary_timeout were
    unreachable instrumentation. Absence is the honest pin — regressions
    restore the token.
    """
    source = (HOOKS_DIR / "_active_task_context.py").read_text(encoding="utf-8")
    assert "git_primary_timeout" not in source, (
        "git_primary_timeout must not appear in _active_task_context.py; "
        "the token could never be emitted once primary resolution became pure FS"
    )
    assert "_SNAPSHOT_PRIMARY_TIMEOUT_S" not in source, (
        "_SNAPSHOT_PRIMARY_TIMEOUT_S was discarded-timeout decoration; "
        "it must not return alongside a pure-FS primary probe"
    )


# --- D5: payload twin carries resolution_note for workspace_ambiguous -------------


def test_payload_snapshot_sets_resolution_note_on_ambiguous_workspace(
    monkeypatch, tmp_path
):
    """D5: payload twin must keep the ambiguity valve reachable from snapshot."""
    payload_path = PAYLOAD_HOOKS_DIR / "_active_task_context.py"
    assert payload_path.is_file(), f"payload twin missing at {payload_path}"

    spec = importlib.util.spec_from_file_location(
        "_active_task_context_payload_under_test", payload_path
    )
    assert spec and spec.loader
    payload_mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = payload_mod
    spec.loader.exec_module(payload_mod)

    monkeypatch.setattr(
        payload_mod,
        "_primary_workspace_root",
        lambda _root, *, timeout=5.0: str(tmp_path),
    )
    monkeypatch.delenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", raising=False)

    # workspace_ambiguous with one match for this worktree + one elsewhere.
    other = {
        **ACTIVE,
        "task_ref": "internal",
        "target_worktree_path": "/elsewhere/other",
    }
    _write_ambiguous(tmp_path, [_active_here(tmp_path), other])

    real_import = importlib.import_module

    def _blocked(name, *args, **kwargs):
        if name == "workbay_handoff_mcp" or name.startswith("workbay_handoff_mcp."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(payload_mod.importlib, "import_module", _blocked)

    ctx = payload_mod._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.resolution_note, (
        "workspace_ambiguous snapshot answers must carry resolution_note so "
        "the drift guard's ambiguity fallback valve is not silently dead"
    )
    assert ctx.probe_error is None


def test_payload_strict_ambiguity_fallback_suppresses_note(monkeypatch, tmp_path):
    """D5: WORKBAY_GUARD_AMBIGUITY_FALLBACK=0 must keep the valve fail-closed."""
    payload_path = PAYLOAD_HOOKS_DIR / "_active_task_context.py"
    spec = importlib.util.spec_from_file_location(
        "_active_task_context_payload_strict", payload_path
    )
    assert spec and spec.loader
    payload_mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = payload_mod
    spec.loader.exec_module(payload_mod)

    monkeypatch.setattr(
        payload_mod,
        "_primary_workspace_root",
        lambda _root, *, timeout=5.0: str(tmp_path),
    )
    monkeypatch.setenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", "0")

    other = {
        **ACTIVE,
        "task_ref": "internal",
        "target_worktree_path": "/elsewhere/other",
    }
    _write_ambiguous(tmp_path, [_active_here(tmp_path), other])

    real_import = importlib.import_module

    def _blocked(name, *args, **kwargs):
        if name == "workbay_handoff_mcp" or name.startswith("workbay_handoff_mcp."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(payload_mod.importlib, "import_module", _blocked)

    ctx = payload_mod._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.resolution_note is None, (
        "strict FALLBACK=0 must not emit resolution_note (fail-closed for drift)"
    )


# --- D6: expanduser RuntimeError must not crash the scan --------------------------


def test_malformed_expanduser_entry_is_skipped_not_fatal(monkeypatch, tmp_path):
    """D6: ~nosuchuser expanduser RuntimeError must not abort the scan."""
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    bad = {
        **ACTIVE,
        "task_ref": "internal",
        "target_worktree_path": "~nosuchuser1234/x",
    }
    good = _active_here(tmp_path)
    _write_ambiguous(tmp_path, [bad, good])
    _without_handoff_package(monkeypatch, module)

    # Force expanduser to raise RuntimeError for the bad prefix even if the
    # host happens to resolve unknown users.
    real_expanduser = Path.expanduser

    def _expanduser(self):  # noqa: ANN001
        s = str(self)
        if s.startswith("~nosuchuser"):
            raise RuntimeError("Could not determine home directory")
        return real_expanduser(self)

    monkeypatch.setattr(Path, "expanduser", _expanduser)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "internal"
    assert ctx.probe_error is None


def test_canonical_target_worktree_expanduser_runtime_error_returns_none(
    monkeypatch,
) -> None:
    """W3: expanduser RuntimeError in _canonical_target_worktree returns None.

    Distinct from the snapshot-scan skip arm above: this pins the guard on the
    canonicalizer itself so a bare ``except ():`` mutation cannot survive.
    """
    module = _load_module()

    def _raise_expanduser(self):  # noqa: ANN001
        raise RuntimeError("Could not determine home directory")

    monkeypatch.setattr(Path, "expanduser", _raise_expanduser)
    assert module._canonical_target_worktree("~/anything") is None
    assert module._canonical_target_worktree("~nosuchuser/path") is None


# --- Unusable snapshot fallthrough (primary-checkout wedge fix) -------------------
#
# Tokens that previously terminalised the snapshot path as probe_error contexts.
# Each must now return None so the loader falls through to the authoritative
# database. Assert exact stable token strings never appear as a populated
# probe_error from the snapshot helper.


_UNUSABLE_SNAPSHOT_TOKENS = (
    "snapshot_unreadable",
    "snapshot_schema_unsupported",
    "snapshot_shape_unknown",
    "snapshot_single_invalid",
    "snapshot_tasks_invalid",
    "snapshot_worktree_ambiguous",
    "snapshot_no_worktree_match",
)


def _seed_unusable_snapshot(token: str, root: Path, workspace: Path) -> None:
    """Materialise a CURRENT_TASK.json that triggers the named unusable branch."""
    root.mkdir(parents=True, exist_ok=True)
    if token == "snapshot_unreadable":
        (root / "CURRENT_TASK.json").write_text("{not-json", encoding="utf-8")
        return
    if token == "snapshot_schema_unsupported":
        (root / "CURRENT_TASK.json").write_text(
            json.dumps(
                {
                    "schema_version": 99,
                    "shape": "single",
                    "active": _active_here(workspace),
                }
            ),
            encoding="utf-8",
        )
        return
    if token == "snapshot_shape_unknown":
        _write_snapshot(root, shape="future_shape_v9")
        return
    if token == "snapshot_single_invalid":
        _write_snapshot(root, shape="single", active="not-a-dict")
        return
    if token == "snapshot_tasks_invalid":
        _write_snapshot(root, shape="workspace_ambiguous", tasks="not-a-list")
        return
    if token == "snapshot_worktree_ambiguous":
        a = _active_here(workspace, task_ref="TASK-A", status="in_progress")
        b = _active_here(workspace, task_ref="TASK-B", status="in_progress")
        _write_ambiguous(root, [a, b])
        return
    if token == "snapshot_no_worktree_match":
        _write_ambiguous(
            root,
            [
                {
                    **ACTIVE,
                    "task_ref": "internal",
                    "target_worktree_path": "/elsewhere/repo-a",
                },
                {
                    **ACTIVE,
                    "task_ref": "internal",
                    "target_worktree_path": "/elsewhere/repo-b",
                },
            ],
        )
        return
    raise AssertionError(f"unknown unusable token fixture: {token!r}")


@pytest.mark.parametrize("token", _UNUSABLE_SNAPSHOT_TOKENS)
def test_unusable_snapshot_loader_returns_none_not_probe_error(
    token, monkeypatch, tmp_path
):
    """Each unusable-snapshot branch returns None, not a probe_error context.

    A truthiness-only assertion cannot catch a wrong-token regression: assert
    the snapshot helper is exactly None and never carries the named token.
    """
    module = _load_module()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _pin_primary(monkeypatch, module, tmp_path)
    _seed_unusable_snapshot(token, tmp_path, workspace)

    result = module._try_load_active_task_from_snapshot(workspace)

    assert result is None, (
        f"token={token!r}: unusable snapshot must return None for fallthrough, "
        f"got {result!r} with probe_error="
        f"{getattr(result, 'probe_error', None)!r}"
    )


def test_snapshot_resolves_task_for_matching_worktree(monkeypatch, tmp_path):
    """Confident snapshot match still returns a populated context (isolation pin)."""
    module = _load_module()
    here = tmp_path / "repo-feature"
    here.mkdir()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_ambiguous(
        tmp_path,
        [
            {
                **ACTIVE,
                "task_ref": "internal",
                "target_worktree_path": "/elsewhere/repo-a",
            },
            {
                **ACTIVE,
                "task_ref": "internal",
                "target_worktree_path": str(here),
                "target_branch": "feature/wb-hook-latency-01",
            },
        ],
    )

    result = module._try_load_active_task_from_snapshot(here)

    assert result is not None
    assert result.task_ref == "internal"
    assert result.probe_error is None
    assert result.target_worktree == str(here)
    assert result.target_branch == "feature/wb-hook-latency-01"


# --- Live-status set: review/blocked are active; unrecognised falls through -------
#
# The snapshot only lists tasks whose status is in LIVE_ACTIVE_STATUSES
# (in_progress, review, blocked). The fast path must treat that full set as
# live. A clean negative for review/blocked drops target_worktree and silently
# disables branch isolation. Unrecognised statuses must fall through to the
# authoritative probe, not claim "known and not active".


@pytest.mark.parametrize("status", ["review", "blocked"])
def test_live_status_resolves_active_and_enforces_drift_target(
    status, monkeypatch, tmp_path
):
    """review/blocked are live; target_worktree stays set so drift can engage.

    Resolves from a mismatched worktree against a shape=single snapshot whose
    target is the real task worktree. The answer must be active (task_ref set,
    probe_error is None) and keep target_worktree pointing at the task path —
    not None — so a drift guard can still deny the mismatched location.
    """
    module = _load_module()
    task_worktree = tmp_path / "task-worktree"
    task_worktree.mkdir()
    mismatched = tmp_path / "mismatched-worktree"
    mismatched.mkdir()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(
        tmp_path,
        shape="single",
        active=_active_here(
            task_worktree,
            status=status,
            task_ref="internal",
            target_branch="feature/wb-live-status",
        ),
    )
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(mismatched)

    assert ctx.task_ref == "internal", (
        f"status={status!r} is live and must resolve as active, not a clean "
        f"negative; got task_ref={ctx.task_ref!r}"
    )
    assert ctx.probe_error is None, (
        f"a live snapshot answer must not set probe_error; got {ctx.probe_error!r}"
    )
    assert ctx.target_worktree == str(task_worktree), (
        f"drift needs a concrete target_worktree; got {ctx.target_worktree!r}"
    )
    assert ctx.target_branch == "feature/wb-live-status"
    # Guard engagement pin: the resolved target must differ from the mismatched
    # worktree so isolation is not silently dropped to "no target".
    want_target = module._canonical_target_worktree(ctx.target_worktree)
    mismatched_canonical = str(mismatched.resolve(strict=False))
    assert want_target == str(task_worktree.resolve(strict=False))
    assert want_target != mismatched_canonical, (
        "target_worktree must remain distinct from the mismatched worktree so "
        "the drift guard has something to deny"
    )


def test_non_live_status_falls_through_not_clean_negative(monkeypatch, tmp_path):
    """A status outside the live set must fall through, not claim absence.

    Returning a clean negative (probe_error=None, task_ref=None) from an
    unrecognised status is the same class of bug as treating review/blocked as
    inactive: it is an affirmative "no active task" answer that drops isolation.
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(
        tmp_path,
        shape="single",
        active=_active_here(tmp_path, status="paused", task_ref="internal"),
    )
    _without_handoff_package(monkeypatch, module)

    snap = module._try_load_active_task_from_snapshot(tmp_path)
    assert snap is None, (
        f"non-live status must fall through (None), not a clean negative or "
        f"populated context; got {snap!r} with probe_error="
        f"{getattr(snap, 'probe_error', None)!r} task_ref="
        f"{getattr(snap, 'task_ref', None)!r}"
    )

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref is None
    assert ctx.probe_error == "handoff_unavailable", (
        f"after fallthrough with no package, expected handoff_unavailable, "
        f"got {ctx.probe_error!r}"
    )


def test_multi_task_scan_review_lands_in_live_not_terminal(monkeypatch, tmp_path):
    """workspace_ambiguous: review is a live match, not a terminal clean-negative.

    A done entry for the same worktree must not win over a review entry, and
    the review entry must not be filed as terminal (which would drop isolation).
    """
    module = _load_module()
    _pin_primary(monkeypatch, module, tmp_path)
    done = _active_here(tmp_path, task_ref="AAA-FINISHED", status="done")
    review = _active_here(
        tmp_path,
        task_ref="ZZZ-REVIEW",
        status="review",
        target_branch="feature/zz-review",
    )
    _write_ambiguous(tmp_path, [done, review])
    _without_handoff_package(monkeypatch, module)

    ctx = module._load_active_task(tmp_path)

    assert ctx.task_ref == "ZZZ-REVIEW", (
        f"review must land in live matches and be preferred over terminal done; "
        f"got task_ref={ctx.task_ref!r}"
    )
    assert ctx.probe_error is None, (
        f"live review match must not set probe_error; got {ctx.probe_error!r}"
    )
    assert ctx.target_worktree == str(tmp_path)
    assert ctx.target_branch == "feature/zz-review"


# --- Fail-open closures: findings 9943 / 9944 / 9984 ---------------------------------


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_absent_task_state_authority_is_not_fresh(hooks_dir, monkeypatch, tmp_path):
    """Finding 9943: fully absent .task-state is not freshness evidence.

    Build a real temp tree with CURRENT_TASK.json and no .task-state directory
    at all. Do not mock freshness helpers — the real path must fall through.
    """
    import shutil

    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    snap_path = tmp_path / "CURRENT_TASK.json"
    assert snap_path.is_file()
    shutil.rmtree(tmp_path / ".task-state", ignore_errors=True)
    assert not (tmp_path / ".task-state").exists()

    payload = _snapshot_dict(tmp_path, shape="none")
    assert module._handoff_db_authority_mtime(
        Path(payload["authority_db_path"]),
        Path(payload["authority_projection_dir"]),
    ) is None
    assert module._snapshot_fresh_enough(snap_path, payload) is False

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is None, (
        f"{hooks_dir.name} twin: absent .task-state must fall through; got {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_authority_present_and_not_newer_still_serves_snapshot(
    hooks_dir, monkeypatch, tmp_path
):
    """Finding 9943 negative control: defined older authority keeps fast path."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    _write_snapshot(tmp_path, shape="none")
    assert (tmp_path / ".task-state" / "handoff.db").is_file()

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is not None, (
        f"{hooks_dir.name} twin: older handoff.db must keep shape=none reachable; "
        f"got None"
    )
    assert result.task_ref is None
    assert result.probe_error is None


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_lane_id_env_forces_fast_path_fallthrough(hooks_dir, monkeypatch, tmp_path):
    """Finding 9944: WORKBAY_LANE_ID set → snapshot cannot prove agreement."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    monkeypatch.delenv("WORKBAY_HANDOFF_ACTIVE_TASK", raising=False)
    monkeypatch.setenv("WORKBAY_LANE_ID", "lane-fix-hooklat")
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is None, (
        f"{hooks_dir.name} twin: WORKBAY_LANE_ID set must fall through; got {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_active_task_pin_mismatch_forces_fast_path_fallthrough(
    hooks_dir, monkeypatch, tmp_path
):
    """Finding 9944: pin set to a different task_ref → fall through."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    monkeypatch.delenv("WORKBAY_LANE_ID", raising=False)
    monkeypatch.setenv("WORKBAY_HANDOFF_ACTIVE_TASK", "internal")
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])  # task_ref internal

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is None, (
        f"{hooks_dir.name} twin: pin mismatch must fall through; got {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_active_task_pin_match_still_serves_snapshot(hooks_dir, monkeypatch, tmp_path):
    """Finding 9944 negative: pin equals selected task_ref → still serves."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    monkeypatch.delenv("WORKBAY_LANE_ID", raising=False)
    monkeypatch.setenv("WORKBAY_HANDOFF_ACTIVE_TASK", "internal")
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is not None, (
        f"{hooks_dir.name} twin: matching pin must still serve; got None"
    )
    assert result.task_ref == "internal"
    assert result.probe_error is None


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_no_env_identity_still_serves_worktree_match(hooks_dir, monkeypatch, tmp_path):
    """Finding 9944 negative: neither env set → worktree match still answers."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    monkeypatch.delenv("WORKBAY_LANE_ID", raising=False)
    monkeypatch.delenv("WORKBAY_HANDOFF_ACTIVE_TASK", raising=False)
    _write_ambiguous(tmp_path, [_active_here(tmp_path)])

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is not None
    assert result.task_ref == "internal"
    assert result.probe_error is None


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_terminal_done_projection_falls_through_not_clean_negative(
    hooks_dir, monkeypatch, tmp_path
):
    """Finding 9984: status 'done' must not produce affirmative absence."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    monkeypatch.delenv("WORKBAY_LANE_ID", raising=False)
    monkeypatch.delenv("WORKBAY_HANDOFF_ACTIVE_TASK", raising=False)
    _write_ambiguous(tmp_path, [_active_here(tmp_path, status="done")])

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is None, (
        f"{hooks_dir.name} twin: terminal-only done must fall through; got {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", FRESHNESS_TWINS)
def test_live_in_progress_still_serves_after_terminal_fallthrough_fix(
    hooks_dir, monkeypatch, tmp_path
):
    """Finding 9984 negative: live status still serves from the fast path."""
    module = _load_module(hooks_dir)
    _pin_primary(monkeypatch, module, tmp_path)
    monkeypatch.delenv("WORKBAY_LANE_ID", raising=False)
    monkeypatch.delenv("WORKBAY_HANDOFF_ACTIVE_TASK", raising=False)
    _write_ambiguous(tmp_path, [_active_here(tmp_path, status="in_progress")])

    result = module._try_load_active_task_from_snapshot(tmp_path)
    assert result is not None
    assert result.task_ref == "internal"
    assert result.probe_error is None
