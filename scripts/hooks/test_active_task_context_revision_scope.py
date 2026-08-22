"""Content freshness must cover every payload input, and must be bounded.

internal replaced the sidecar-mtime freshness key with a
``handoff_state`` content fingerprint. That fixed the false-invalidation defect
-- peers merely *opening* the DB touched ``-wal``/``-shm``, so the cache missed
essentially forever -- but it narrowed the authority to a single table, while
``CURRENT_TASK.json`` is rendered from strictly more than that table:

  * the per-task projection JSON files, whose contents are read verbatim into
    the payload, and
  * ``compaction_advisory``, which ``_overlay_compaction_advisory`` computes
    from live runtime state and *deliberately never persists* into the
    projection file.

So a projection rewrite, or a new compaction record, changes the payload while
``COUNT(*)`` / ``SUM(revision)`` / ``MAX(updated_at)`` over ``handoff_state``
stay byte-identical. The fingerprint matches, the snapshot is declared fresh,
and the stale payload is served **indefinitely** -- nothing caps it. The legacy
rule caught this case via the db/``-wal`` mtime term that the change removed,
so the narrowing traded a false-invalidation bug for a missed-invalidation bug.

[RES-08] asks for both halves of a cache: an invalidation key *and* a bound.
These arms pin the two the content branch is missing -- projection coverage,
and a bounded staleness window as the backstop for the inputs no fingerprint
over ``handoff_state`` can enumerate.

Sidecar mtimes stay excluded: they are what peers advance, and re-admitting
them re-opens the original defect. Projection mtimes are *not* peer-touched --
a projection file changes only when something rewrites it -- so keeping them in
the authority costs nothing and restores the guard that existed before.

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

_HANDOFF_STATE_DDL = """
CREATE TABLE handoff_state (
    id INTEGER PRIMARY KEY,
    task_ref TEXT NOT NULL,
    status TEXT,
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT
)
"""


def _find_repo_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "scripts" / "hooks" / "_active_task_context.py").is_file():
            return candidate
    raise AssertionError("could not locate monorepo root from %s" % start)


_ROOT = _find_repo_root(HOOKS_DIR)

TREES = {
    "root": _ROOT / "scripts" / "hooks",
    "payload": (
        _ROOT
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
    """Load ``_active_task_context`` from one hook tree under a unique name."""
    tree = TREES[request.param]
    path = tree / "_active_task_context.py"
    mod_name = f"_active_task_context__scope__{request.param}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(mod_name, None)


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


def _content_revision(ctx, db_path: Path) -> str:
    helper = getattr(ctx, "_handoff_content_revision", None)
    assert helper is not None, "_active_task_context must expose _handoff_content_revision"
    value = helper(db_path)
    assert isinstance(value, str) and value, f"expected a fingerprint, got {value!r}"
    return value


def _write_snapshot(root: Path, db_path: Path, *, content_revision: str | None) -> Path:
    payload: dict[str, object] = {
        "schema_version": 2,
        "shape": "none",
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


def _fresh(ctx, snapshot: Path) -> bool:
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    return ctx._snapshot_fresh_enough(snapshot, payload)


def _stamp(path: Path, when: float) -> None:
    os.utime(path, (when, when))


def _staleness_bound(ctx) -> float:
    """The module's bounded staleness window, in seconds.

    Pinned as a *property*, not a magic number: a cache with no bound cannot
    survive an input its key does not cover [RES-08], and a bound so long it
    never fires is the same as no bound at all.

    The upper end is deliberately generous, because the bound is defence in
    depth and not the invalidation key. Everything the hook actually reads is
    already covered by an exact term -- identity fields by the ``handoff_state``
    fingerprint, payload fields by the projection mtime -- so a snapshot that
    passes both terms is correct for every consumed field no matter its age.
    What the window bounds is only the *unenumerated* input.

    Two clocks are easy to conflate here, and only the second one governs
    staleness: the hook fires many times per minute, but the snapshot is
    rewritten solely by handoff writes. A short window therefore does not
    ride the hook's firing rate -- it expires during any lull in write
    traffic and forces the cold path (measured 2.44s CPU, 6.60s wall under
    load, against a 5s harness ceiling), which is the very failure this
    module's cache exists to prevent.
    """
    bound = getattr(ctx, "_SNAPSHOT_MAX_STALENESS_SECONDS", None)
    assert isinstance(bound, (int, float)) and not isinstance(bound, bool), (
        "_active_task_context must expose _SNAPSHOT_MAX_STALENESS_SECONDS -- the "
        "bound that caps how long a content-fresh snapshot may be trusted when "
        "an input outside handoff_state (compaction_advisory, projection "
        "contents) changed without moving the fingerprint [RES-08]."
    )
    assert 0 < float(bound) <= 900, (
        f"staleness bound {bound!r}s is outside the useful range: it must be "
        "positive, and short enough to actually cap staleness (<= 15 min)."
    )
    return float(bound)


# --------------------------------------------------------------------------
# RED: authority scope excludes the projection files the payload is built from
# --------------------------------------------------------------------------


def test_projection_rewrite_invalidates_even_when_db_fingerprint_matches(
    ctx, tmp_path: Path
) -> None:
    """A projection rewritten after the snapshot must not read as fresh.

    ``handoff_state`` is untouched throughout, so the fingerprint is identical
    before and after -- which is exactly the blind spot: the payload is
    rendered *from the projection files*, so their content is authority too.
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision = _content_revision(ctx, db_path)

    projection = tmp_path / ".task-state" / "current" / "SEED-0.json"
    projection.parent.mkdir(parents=True, exist_ok=True)
    projection.write_text(json.dumps({"task_ref": "SEED-0", "focus": "before"}))

    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)

    now = time.time()
    _stamp(db_path, now - 60)
    _stamp(projection, now - 30)
    _stamp(snapshot, now - 10)
    assert _fresh(ctx, snapshot) is True, (
        "precondition: with nothing changed since it was written, the snapshot "
        "must be usable -- otherwise this arm proves nothing"
    )

    # Something rewrites the projection. The DB never moves.
    projection.write_text(json.dumps({"task_ref": "SEED-0", "focus": "AFTER"}))
    _stamp(projection, now - 5)

    assert _content_revision(ctx, db_path) == revision, (
        "guard: the DB fingerprint must be unchanged, or this arm is testing "
        "the fingerprint rather than the projection blind spot"
    )
    assert _fresh(ctx, snapshot) is False, (
        "a projection rewritten after the snapshot changes the rendered payload "
        "while leaving handoff_state identical; keying freshness on that table "
        "alone serves the stale payload indefinitely"
    )


# --------------------------------------------------------------------------
# RED: no bound caps inputs the fingerprint cannot enumerate
# --------------------------------------------------------------------------


def test_snapshot_beyond_the_staleness_bound_is_not_fresh(ctx, tmp_path: Path) -> None:
    """Content-fresh is necessary but not sufficient past the bound.

    ``compaction_advisory`` is computed from live runtime state and never
    persisted, so no fingerprint over ``handoff_state`` can detect its change.
    A bound is the only thing that keeps such an input from going stale forever.
    """
    bound = _staleness_bound(ctx)

    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)

    long_ago = time.time() - (bound * 10)
    _stamp(db_path, long_ago)
    _stamp(snapshot, long_ago)

    assert _content_revision(ctx, db_path) == revision, "guard: content unchanged"
    assert _fresh(ctx, snapshot) is False, (
        "the fingerprint still matches, but the snapshot is far past the "
        "staleness bound; without a bound an input outside handoff_state stays "
        "stale forever [RES-08]"
    )


def test_snapshot_inside_the_staleness_bound_is_still_fresh(
    ctx, tmp_path: Path
) -> None:
    """Control for the arm above: the bound must not eat the cache.

    If this fails the bound is too tight and the whole point of the change --
    letting the hook answer from cache -- is lost.
    """
    _staleness_bound(ctx)

    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)

    now = time.time()
    _stamp(db_path, now - 2)
    _stamp(snapshot, now - 1)
    # Peers touching the sidecars must still not matter.
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        sidecar.write_bytes(b"")
        _stamp(sidecar, now)

    assert _fresh(ctx, snapshot) is True, (
        "a just-written snapshot whose content matches must be usable even "
        "while peers are touching -wal/-shm"
    )


# --------------------------------------------------------------------------
# Characterization: cold start, and the read-only probe's own side effect
# --------------------------------------------------------------------------


def test_checkpointed_wal_database_without_sidecars_is_still_readable(
    ctx, tmp_path: Path
) -> None:
    """Cold start -- no sidecars on disk -- must still get a fingerprint.

    A cleanly closed WAL database is fully checkpointed and SQLite removes
    ``-wal``/``-shm``. Opening it ``mode=ro`` from a writable directory then
    *recreates* them, so the fingerprint is readable and the fast path is
    available before any MCP server has connected. This arm exists because the
    opposite was asserted during review and measurement refuted it: the
    failures seen there came from hand-deleting sidecars that still held
    uncheckpointed WAL content, which manufactures an incomplete database
    rather than reproducing any real cold-start state.

    The recreation is also a side effect worth keeping visible: this hook,
    which is otherwise a pure reader, does touch the filesystem. That is
    exactly why sidecar mtimes must never be the freshness key -- the reader
    advances them itself.
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(_HANDOFF_STATE_DDL)
        con.execute(
            "INSERT INTO handoff_state (task_ref, status, revision, updated_at) "
            "VALUES ('SEED-0', 'in_progress', 0, '2026-08-07 10:00:00')"
        )
        con.commit()
    finally:
        con.close()

    assert not Path(str(db_path) + "-wal").exists(), (
        "precondition: a cleanly closed WAL db checkpoints and removes its "
        "sidecars; if this fails the arm is not testing cold start"
    )

    assert ctx._handoff_content_revision(db_path) == "1:0:2026-08-07 10:00:00", (
        "a checkpointed WAL database with no sidecars must still yield a "
        "fingerprint -- otherwise the fast path is dead at cold start, which "
        "is precisely the case this change exists to speed up"
    )


def test_unreadable_authority_answers_not_fresh(ctx, tmp_path: Path) -> None:
    """When the fingerprint genuinely cannot be read, answer not-fresh [OBS-08].

    Directory permissions are the honest way to make the read fail: SQLite
    needs to create the ``-shm`` for a WAL database, so a non-writable parent
    denies it. An unanswerable question must never fall open.
    """
    db_path = tmp_path / ".task-state" / "handoff.db"
    _seed_db(db_path)
    revision = _content_revision(ctx, db_path)
    snapshot = _write_snapshot(tmp_path, db_path, content_revision=revision)

    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.commit()
    finally:
        con.close()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.exists():
            sidecar.unlink()

    os.chmod(db_path.parent, 0o500)
    try:
        assert ctx._handoff_content_revision(db_path) is None, (
            "a read that cannot complete must report None, not a partial answer"
        )
        assert _fresh(ctx, snapshot) is False, (
            "an unreadable authority must answer not-fresh, never fall open "
            "[OBS-08]"
        )
    finally:
        os.chmod(db_path.parent, 0o700)
