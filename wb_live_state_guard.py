"""Repo-root live-``.task-state`` hermeticity guard (pytest plugin).

Shipped as a *plugin module* rather than only a repo-root ``conftest.py`` so it
fires regardless of pytest ``rootdir``. Package-scoped runs -- ``cd
packages/<pkg> && pytest`` (``make test``), ``make test-handoff``, and
``check-all`` -- set ``rootdir`` to the package directory because every package
``pyproject.toml`` carries ``[tool.pytest.ini_options]``. pytest's default
``confcutdir`` is the ``rootdir``, so a repo-root ``conftest.py`` sitting *above*
it is never collected and the guard would be silently absent. Each package
``tests/conftest.py`` and the repo-root ``conftest.py`` register this module via
``pytest_plugins``; pytest imports it once (deduped by name) and its hooks run
for every session.

The guard resolves the **git worktree root** itself (walking up for ``.git``,
which is a file in linked worktrees) so it watches the real repo-root
``.task-state`` even when ``rootdir`` is a package directory.

Mutations are attributed **per test** (``pytest_runtest_teardown``), not once per
session, so an unmarked leak still fails even when a legitimately-marked test
coexists in the same run. A test opts in to mutating live state with
``@pytest.mark.live_state``. Unmarked mutations fail the session -- but only when
no *other* live agent session is active (this repo routinely runs several
concurrently); otherwise the mutation is reported as a non-failing warning.

Set ``WORKBAY_DISABLE_LIVE_STATE_GUARD=1`` to disable the guard entirely.

The guard is observational: it never forces env or state-dir overrides, so
package conftests keep working unchanged. Fixtures that need an alternate
handoff state dir should pass explicit paths -- ``WORKBAY_HANDOFF_STATE_DIR`` is
only honored by the handoff runtime's ``config.from_args`` path, not
``RuntimeConfig.for_workspace`` (pass ``state_dir=`` there).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TypeAlias

import pytest

_Snapshot: TypeAlias = dict[str, tuple[int, int]]

_DISABLE_ENV = "WORKBAY_DISABLE_LIVE_STATE_GUARD"

_WATCHED_ROOT_FILES = (
    ".task-state/checklist_sync.json",
    ".task-state/CURRENT_TASK.json",
    ".task-state/DASHBOARD.txt",
)
_WATCHED_STATE_DIRS = (
    ".task-state/DASHBOARD.d",
    ".task-state/current",
    ".task-state/exports",
)


def _resolve_repo_root(start: Path) -> Path:
    """Return the git worktree root at/above ``start`` (``.git`` file or dir)."""

    start = start.resolve()
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _wb_snapshot_live_state(repo: Path) -> _Snapshot:
    """Snapshot only pytest-sensitive live state under ``repo/.task-state``.

    Excludes ``handoff.db``/``-wal``/``-shm``, ``.heartbeat/``, and guard
    JSONLs -- concurrent live agent sessions mutate those continuously and
    would false-positive the guard.
    """

    repo = repo.resolve()
    snapshot: _Snapshot = {}

    def add_file(path: Path) -> None:
        if not path.is_file():
            return
        stat = path.stat()
        snapshot[str(path.relative_to(repo))] = (stat.st_size, stat.st_mtime_ns)

    for rel in _WATCHED_ROOT_FILES:
        add_file(repo / rel)

    for rel in _WATCHED_STATE_DIRS:
        root = repo / rel
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            add_file(path)

    return snapshot


def _wb_diff_live_state(before: _Snapshot, after: _Snapshot) -> list[str]:
    changes: list[str] = []
    before_keys = set(before)
    after_keys = set(after)
    for rel in sorted(after_keys - before_keys):
        changes.append(f"added {rel}")
    for rel in sorted(before_keys - after_keys):
        changes.append(f"removed {rel}")
    for rel in sorted(before_keys & after_keys):
        if before[rel] != after[rel]:
            changes.append(f"modified {rel}")
    return changes


def _wb_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wb_has_other_live_sessions(repo: Path) -> bool:
    heartbeat_dir = repo / ".task-state" / ".heartbeat"
    if not heartbeat_dir.is_dir():
        return False
    current_pid = os.getpid()
    for path in heartbeat_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        pid = payload.get("session_pid")
        if isinstance(pid, str) and pid.isdigit():
            pid = int(pid)
        if isinstance(pid, int) and pid != current_pid and _wb_pid_alive(pid):
            return True
    return False


def _wb_should_fail_for_live_state_mutation(
    diff: list[str], *, saw_live_state_marker: bool, other_live_sessions: bool
) -> bool:
    """Decide whether a per-test watchlist mutation should fail the session."""

    if not diff:
        return False
    if saw_live_state_marker:
        return False
    return not other_live_sessions


def _wb_guard_active(config: pytest.Config) -> bool:
    return bool(getattr(config, "_wb_repo_root", None)) and not getattr(config, "_wb_guard_disabled", False)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "live_state: test intentionally mutates the repository's live .task-state watchlist",
    )
    if os.environ.get(_DISABLE_ENV):
        config._wb_guard_disabled = True  # type: ignore[attr-defined]
        return
    config._wb_guard_disabled = False  # type: ignore[attr-defined]
    config._wb_repo_root = _resolve_repo_root(Path(config.rootpath))  # type: ignore[attr-defined]
    config._wb_last_snapshot = {}  # type: ignore[attr-defined]
    config._wb_leaks = []  # type: ignore[attr-defined]
    config._wb_warns = []  # type: ignore[attr-defined]


def pytest_sessionstart(session: pytest.Session) -> None:
    config = session.config
    if not _wb_guard_active(config):
        return
    config._wb_last_snapshot = _wb_snapshot_live_state(config._wb_repo_root)  # type: ignore[attr-defined]


def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:
    config = item.config
    if not _wb_guard_active(config):
        return
    repo = config._wb_repo_root  # type: ignore[attr-defined]
    before = getattr(config, "_wb_last_snapshot", {})
    after = _wb_snapshot_live_state(repo)
    config._wb_last_snapshot = after  # type: ignore[attr-defined]
    diff = _wb_diff_live_state(before, after)
    if not diff:
        return
    marked = bool(item.get_closest_marker("live_state"))
    if marked:
        # Intentional mutation; the baseline has already advanced above.
        return
    other_live_sessions = _wb_has_other_live_sessions(repo)
    if _wb_should_fail_for_live_state_mutation(
        diff, saw_live_state_marker=marked, other_live_sessions=other_live_sessions
    ):
        config._wb_leaks.append((item.nodeid, diff))  # type: ignore[attr-defined]
    else:
        config._wb_warns.append((item.nodeid, diff))  # type: ignore[attr-defined]


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    config = session.config
    if not _wb_guard_active(config):
        return
    leaks = getattr(config, "_wb_leaks", [])
    warns = getattr(config, "_wb_warns", [])
    terminal = config.pluginmanager.get_plugin("terminalreporter")
    if terminal is not None and warns:
        terminal.write_sep("=", "live .task-state mutation observed (not failing)")
        terminal.write_line("Another live session was active during these mutations; not failing:")
        for nodeid, diff in warns:
            terminal.write_line(f"  {nodeid}: {', '.join(diff)}")
    if not leaks:
        return
    if terminal is not None:
        terminal.write_sep("=", "live .task-state mutation detected")
        for nodeid, diff in leaks:
            terminal.write_line(f"  {nodeid}: {', '.join(diff)}")
        terminal.write_line(
            "Mark intentional live-state tests with @pytest.mark.live_state, or route writes through a tmp state_dir."
        )
    session.exitstatus = pytest.ExitCode.TESTS_FAILED
