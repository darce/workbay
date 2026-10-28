"""Snapshot freshness must key on DB *content*, not on sidecar mtimes.

internal. ``_snapshot_fresh_enough`` decides whether the
maintained ``CURRENT_TASK.json`` may answer hook-facing task resolution. Today
it answers by comparing the snapshot's mtime against the newest mtime among
``handoff.db``, ``handoff.db-wal``, ``handoff.db-shm`` and the projection JSON
files.

Under SQLite WAL, ``-shm`` and ``-wal`` are touched by **any** connection --
including a read-only probe from an unrelated session. So on a host running
concurrent sessions against one repo, the authority mtime advances constantly
while the snapshot's *content* stays perfectly current, and the fast path is
rejected essentially every time. Measured on the primary root: snapshot
``generated_at`` 79s **newer** than the DB's newest ``updated_at``, yet
``-shm``/``-wal`` newer still, so the cache missed and each hook paid the cold
path (~2.1s CPU / 3.5-6.6s wall against a 5s harness budget). A cache whose
invalidation key is advanced by readers is not a cache [RES-08].

The naive repair -- drop the sidecars and compare the main DB file mtime -- is
worse than the defect: under WAL a committed write leaves the main file's mtime
untouched until checkpoint, so it would serve a genuinely stale identity. That
is exactly the failure the sidecar rule was added to prevent
[REVHOO-RED-01][finding 9213], and ``test_control_*`` below keeps it fixed.

The invariant these arms pin: **freshness is decided by whether the DB's
content changed since the snapshot was rendered, and an unanswerable question
is answered "not fresh"** -- never by whether some process opened the file.

Both hook trees are parametrized: the root copy and the export-scrubbed payload
twin must implement one contract, so a fix cannot land on one side only.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent

# Far-past stamp used to force "main DB file looks old" while its *content* is
# current -- the WAL condition a mtime-only rule cannot see.
_OLD_MTIME = 1_650_000_000.0

_HANDOFF_STATE_DDL = """
CREATE TABLE handoff_state (
    id INTEGER PRIMARY KEY,
    task_ref TEXT NOT NULL,
    objective TEXT,
    focus TEXT,
    status TEXT,
    target_branch TEXT,
    target_worktree_path TEXT,
    task_plan_path TEXT,
    last_observed_integration_sha TEXT,
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT,
    updated_by TEXT,
    updated_branch TEXT,
    updated_commit_sha TEXT
)
"""


def _find_repo_root(start: Path) -> Path:
    """Resolve monorepo root from either scripts/hooks or the payload twin."""
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
    raise AssertionError(f"could not locate monorepo root from {start}")


REPO_ROOT = _find_repo_root(HOOKS_DIR)

TREES = {
    "root": REPO_ROOT / "scripts" / "hooks",
    "payload": (
        REPO_ROOT
        / "packages"
        / "workbay-system"
        / "workbay_system"
        / "payload"
        / "scripts"
        / "hooks"
    ),
}


@pytest.fixture(params=sorted(TREES), ids=sorted(TREES))
def ctx(request: pytest.FixtureRequest):
    """Load ``_active_task_context`` from one hook tree under a unique name.

    Each tree is loaded under a distinct module name so the two twins cannot
    collide in ``sys.modules`` and silently test the same file twice.
    """
    tree = TREES[request.param]
    path = tree / "_active_task_context.py"
    mod_name = f"_active_task_context__{request.param}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(mod_name, None)


def _content_revision(ctx, db_path: Path) -> str:
    """Call the module's content-revision helper, failing loudly if absent.

    The helper is the shared definition both halves of the freshness contract
    must use (reader here, writer in ``current_task_rendering``). Naming it in
    one place is what keeps the two halves from drifting apart.
    """
    helper = getattr(ctx, "_handoff_content_revision", None)
    assert helper is not None, (
        "_active_task_context must expose _handoff_content_revision(db_path) -- "
        "the stdlib-only content fingerprint that replaces sidecar mtime as the "
        "freshness key. Hooks cannot import the handoff package, so this helper "
        "is intentionally duplicated from its writer counterpart."
    )
    return helper(db_path)


def _seed_db(db_path: Path, rows: int = 2) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.execute(_HANDOFF_STATE_DDL)
        for i in range(rows):
            con.execute(
                "INSERT INTO handoff_state (task_ref, status, revision, updated_at) "
                "VALUES (?, 'in_progress', ?, ?)",
                (f"SEED-{i}", i, f"2026-08-07 10:0{i}:00"),
            )
        con.commit()
    finally:
        con.close()


def _write_snapshot(
    root: Path, db_path: Path, *, content_revision: str | None
) -> Path:
    """Write CURRENT_TASK.json in the shape production emits (schema 2)."""
    payload = {
        "schema_version": 2,
        "shape": "none",
        "staleness_note": "May lag; authoritative state via load_session.",
        "generated_at": "2026-08-07T19:09:43Z",
        "authority_db_path": str(db_path),
        "authority_projection_dir": str(root / ".task-state" / "current"),
    }
    if content_revision is not None:
        payload["authority_content_revision"] = content_revision
    (root / ".task-state" / "current").mkdir(parents=True, exist_ok=True)
    snapshot = root / "CURRENT_TASK.json"
    snapshot.write_text(json.dumps(payload), encoding="utf-8")
    return snapshot


def _age_the_snapshot(snapshot: Path, db_path: Path) -> None:
    """Make every authority path *look* newer than the snapshot.

    This is the state a peer session produces just by opening the DB: the
    snapshot's content is current, but a mtime-only rule sees stale.

    The snapshot is aged by seconds, not years. A peer touching ``-shm``
    moments after the snapshot was written is the real scenario, and the
    arms below still fail under the mtime rule at that spacing -- while a
    far-past stamp would additionally trip the bounded staleness window
    (``_SNAPSHOT_MAX_STALENESS_SECONDS``) and stop isolating the defect under
    test. See ``test_active_task_context_revision_scope.py`` for the bound.
    """
    now = time.time()
    os.utime(snapshot, (now - 5, now - 5))
    for path in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
        if path.exists():
            os.utime(path, (now, now))


# --------------------------------------------------------------------------
# RED: the defect
# --------------------------------------------------------------------------


def test_peer_touching_sidecars_must_not_invalidate_a_current_snapshot(
    ctx, tmp_path: Path
) -> None:
    """Content unchanged + sidecars newer => fresh.

    The snapshot records the exact content revision the DB still has. Another
    session merely opening that DB advances ``-shm``/``-wal``. Rejecting here
    is the defect: it makes every hook invocation pay the cold path on a host
    with concurrent sessions, for a snapshot that is byte-accurate [RES-08].
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)

    # A peer session opens the DB read-only: no content change, but -shm moves.
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        con.execute("SELECT COUNT(*) FROM handoff_state").fetchone()
    finally:
        con.close()
    _age_the_snapshot(snapshot, db_path)

    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    assert ctx._snapshot_fresh_enough(snapshot, payload) is True, (
        "a snapshot whose recorded content revision still matches the DB must "
        "be usable even though a peer session touched -wal/-shm; keying "
        "invalidation on reader-advanced mtimes is why the cache never hits"
    )


# --------------------------------------------------------------------------
# CONTROLS: these must stay green, and they are what a naive fix breaks
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate,label",
    [
        (
            lambda con: con.execute(
                "INSERT INTO handoff_state (task_ref, status, revision, updated_at) "
                "VALUES ('NEW-ROW', 'in_progress', 0, '2026-08-07 20:00:00')"
            ),
            "insert",
        ),
        (
            lambda con: con.execute(
                "UPDATE handoff_state SET revision = revision + 1, "
                "updated_at = '2026-08-07 20:00:00' WHERE task_ref = 'SEED-0'"
            ),
            "update",
        ),
        (
            lambda con: con.execute(
                "DELETE FROM handoff_state WHERE task_ref = 'SEED-1'"
            ),
            "delete",
        ),
    ],
)
def test_real_content_change_after_snapshot_is_not_fresh(
    ctx, tmp_path: Path, mutate, label: str
) -> None:
    """A genuine write invalidates, even when no file mtime betrays it.

    The main DB file is stamped old *after* the write to emulate WAL, where a
    committed change leaves the main file untouched until checkpoint. Any fix
    that swaps sidecar mtimes for main-file mtime serves a stale identity here
    -- the regression [REVHOO-RED-01][finding 9213] guards against.

    Three mutation kinds run because a fingerprint sensitive to only one of
    them (e.g. row count) would pass the arm above while missing real drift.
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision_before = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision_before)

    con = sqlite3.connect(db_path)
    try:
        mutate(con)
        con.commit()
    finally:
        con.close()

    assert _content_revision(ctx, db_path) != revision_before, (
        f"the content fingerprint must discriminate a {label}; a fingerprint "
        "blind to it cannot serve as an invalidation key"
    )

    # Emulate WAL: nothing on disk looks newer than the snapshot.
    os.utime(snapshot, (time.time(), time.time()))
    for path in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
        if path.exists():
            os.utime(path, (_OLD_MTIME, _OLD_MTIME))

    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    assert ctx._snapshot_fresh_enough(snapshot, payload) is False, (
        f"content changed by {label} since the snapshot was rendered, so the "
        "snapshot must not answer -- regardless of how new its mtime looks"
    )


def test_snapshot_without_revision_stamp_keeps_todays_behaviour(
    ctx, tmp_path: Path
) -> None:
    """No stamp => no content evidence => fall through, exactly as today.

    Snapshots written before the writer stamps a revision must not be granted
    freshness by the absence of a field. Silence is not evidence [OBS-08].
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=None)
    _age_the_snapshot(snapshot, db_path)

    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    assert ctx._snapshot_fresh_enough(snapshot, payload) is False, (
        "a legacy snapshot carrying no authority_content_revision must fall "
        "through to the authoritative probe, not be accepted by default"
    )


def test_unreadable_authority_does_not_grant_freshness(ctx, tmp_path: Path) -> None:
    """An unanswerable question answers "not fresh" [OBS-08].

    A corrupt or locked DB must fail closed. Fail-open here would serve an
    arbitrarily old identity precisely when state is damaged.
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)
    db_path.write_bytes(b"this is not a sqlite database")
    _age_the_snapshot(snapshot, db_path)

    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    assert ctx._snapshot_fresh_enough(snapshot, payload) is False, (
        "an unreadable authority must fall through to the authoritative probe"
    )


def test_freshness_check_stays_far_below_the_hook_budget(ctx, tmp_path: Path) -> None:
    """The confirmation must cost a query, not a package import.

    Measured against the live 154MB DB: ~1.0ms for this read versus ~2440ms to
    import the handoff package. The ceiling here is deliberately loose so it
    cannot flake under host load -- it exists to catch a repair that reaches
    for the heavy path, not to benchmark [RES-07].
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path, rows=200)
    revision = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)
    _age_the_snapshot(snapshot, db_path)
    payload = json.loads(snapshot.read_text(encoding="utf-8"))

    started = time.perf_counter()
    ctx._snapshot_fresh_enough(snapshot, payload)
    elapsed = time.perf_counter() - started
    assert elapsed < 0.5, (
        f"freshness confirmation took {elapsed:.3f}s; it must be a stdlib "
        "sqlite read, never a package import"
    )
