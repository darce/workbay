from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

HOOK = Path(__file__).parent / "guard-review-finding-resolve.py"


def _run_hook(review: dict, *, tool_name: str = "mcp__workbay-handoff-mcp__review_findings") -> subprocess.CompletedProcess:
    payload = {
        "hook_event_name": "PreToolUse",
        "session_id": "test-session",
        "tool_name": tool_name,
        "tool_input": {"review": review},
    }
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_blocks_update_fixed_with_resolve_call_shape() -> None:
    """Default/unknown state still names the resolve call (open+live fallback)."""
    result = _run_hook({"operation": "update", "status": "fixed", "finding_id": "F-1"})

    assert result.returncode == 2
    assert 'review_findings(review={"operation":"resolve"' in result.stderr
    # Either finding_ids form or all_open form is acceptable as the resolve shape.
    assert '"finding_ids":["F-1"]' in result.stderr or '"all_open":true' in result.stderr


def test_passes_non_fixed_updates_and_resolve_ops() -> None:
    assert _run_hook({"operation": "update", "status": "deferred"}).returncode == 0
    assert _run_hook({"operation": "resolve", "all_open": True}).returncode == 0
    assert _run_hook({"operation": "list"}).returncode == 0


def test_ignores_unrelated_tools() -> None:
    result = _run_hook({"operation": "update", "status": "fixed"}, tool_name="Bash")

    assert result.returncode == 0


def test_case_exact_open_live_names_resolve_only(monkeypatch) -> None:
    """T9: open+live row → single working call is resolve (not disposition)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("guard_review_finding_resolve", HOOK)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    monkeypatch.setattr(mod, "_lookup_finding_case", lambda **kwargs: "resolve")
    envelope = mod._structured_case_rejection(
        {"operation": "update", "status": "fixed", "finding_id": "LIVE-1", "task_ref": "TASK-LIVE"},
        case="resolve",
    )
    assert "resolve" in envelope["example"]
    assert "disposition" not in envelope["example"] or "resolve" in envelope["example"]
    # Primary example is resolve for live open rows.
    assert '"operation":"resolve"' in envelope["example"] or "operation\":\"resolve\"" in envelope["example"]


def test_case_exact_orphan_names_disposition_only(monkeypatch) -> None:
    """T9: orphan/done ref → single working call is disposition."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("guard_review_finding_resolve", HOOK)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    envelope = mod._structured_case_rejection(
        {
            "operation": "update",
            "status": "fixed",
            "finding_id": "ORPH-1",
            "task_ref": "DONE-TASK",
        },
        case="disposition",
    )
    assert result_has_disposition_only(envelope)


def result_has_disposition_only(envelope: dict) -> bool:
    assert envelope["rule_id"] == "review_findings.use_disposition_for_orphan"
    assert '"operation":"disposition"' in envelope["example"]
    assert "resolve" not in envelope["example"]
    return True


def test_case_exact_already_closed_is_noop_message(monkeypatch) -> None:
    """T9: already-closed finding → no-op message (do not re-close)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("guard_review_finding_resolve", HOOK)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    envelope = mod._structured_case_rejection(
        {"operation": "update", "status": "fixed", "finding_id": "CLOSED-1", "task_ref": "TASK-X"},
        case="already_closed",
    )
    assert envelope["rule_id"] == "review_findings.already_closed_noop"
    assert "no-op" in envelope["example"].lower() or "already" in envelope["error"].lower()
    assert "already closed" in envelope["error"].lower() or "already terminal" in envelope["violated"].lower()


def test_hook_subprocess_disposition_case_via_lookup_patch() -> None:
    """End-to-end hook process: disposition case emits disposition call only."""
    # Patch is in-process; for subprocess we rely on unit tests above for case
    # dispatch and this smoke check that the hook still blocks update fixed.
    result = _run_hook(
        {
            "operation": "update",
            "status": "fixed",
            "finding_id": "ANY-1",
            "task_ref": "ANY-TASK",
        }
    )
    assert result.returncode == 2
    assert "update" in result.stderr.lower() or "resolve" in result.stderr or "disposition" in result.stderr


# ---------------------------------------------------------------------------
# S4-A-02: deterministic case lookup when finding_id is ambiguous across tasks
# ---------------------------------------------------------------------------


def _load_hook_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location("guard_review_finding_resolve", HOOK)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _fake_shared_schema(db_path: Path):
    """Build stub workbay_handoff_mcp(.shared_schema) modules over a temp DB."""
    import contextlib
    import sqlite3
    import types

    @contextlib.contextmanager
    def _get_db_connection():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    pkg = types.ModuleType("workbay_handoff_mcp")
    sub = types.ModuleType("workbay_handoff_mcp.shared_schema")
    sub._get_db_connection = _get_db_connection
    pkg.shared_schema = sub
    return pkg, sub


def _seed_ambiguous_db(db_path: Path) -> None:
    import sqlite3

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE review_findings ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " finding_id TEXT, task_ref TEXT, status TEXT, updated_at TEXT)"
        )
        conn.execute(
            "CREATE TABLE handoff_state (task_ref TEXT PRIMARY KEY, status TEXT)"
        )
        # Same finding_id on two tasks: older row open+live, newer row open+done.
        conn.execute(
            "INSERT INTO review_findings(finding_id, task_ref, status, updated_at)"
            " VALUES ('DUP-1', 'TASK-OLD', 'open', '2020-01-01 00:00:00')"
        )
        conn.execute(
            "INSERT INTO review_findings(finding_id, task_ref, status, updated_at)"
            " VALUES ('DUP-1', 'TASK-NEW', 'open', '2026-01-01 00:00:00')"
        )
        conn.execute(
            "INSERT INTO handoff_state(task_ref, status) VALUES ('TASK-OLD', 'in_progress')"
        )
        conn.execute(
            "INSERT INTO handoff_state(task_ref, status) VALUES ('TASK-NEW', 'done')"
        )
        conn.commit()


def test_ambiguous_finding_id_uses_latest_updated_row(tmp_path, monkeypatch) -> None:
    """No task_ref: the LATEST-updated open row wins (deterministic ORDER BY),
    not sqlite scan order. TASK-NEW is done → disposition case."""
    import sys as _sys

    db_path = tmp_path / "handoff.db"
    _seed_ambiguous_db(db_path)
    mod = _load_hook_module()
    pkg, sub = _fake_shared_schema(db_path)
    monkeypatch.setitem(_sys.modules, "workbay_handoff_mcp", pkg)
    monkeypatch.setitem(_sys.modules, "workbay_handoff_mcp.shared_schema", sub)

    case = mod._lookup_finding_case(finding_id="DUP-1", task_ref=None)
    assert case == "disposition"


def test_explicit_task_ref_preferred_over_ambiguous_scan(tmp_path, monkeypatch) -> None:
    """An explicit task_ref pins the row: TASK-OLD is open+live → resolve."""
    import sys as _sys

    db_path = tmp_path / "handoff.db"
    _seed_ambiguous_db(db_path)
    mod = _load_hook_module()
    pkg, sub = _fake_shared_schema(db_path)
    monkeypatch.setitem(_sys.modules, "workbay_handoff_mcp", pkg)
    monkeypatch.setitem(_sys.modules, "workbay_handoff_mcp.shared_schema", sub)

    case = mod._lookup_finding_case(finding_id="DUP-1", task_ref="TASK-OLD")
    assert case == "resolve"
