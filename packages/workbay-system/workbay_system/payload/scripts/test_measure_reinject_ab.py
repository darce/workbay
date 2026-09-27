"""Unit tests for measure_reinject_ab offline analyzer (internal)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from measure_reinject_ab import (
    SessionResult,
    apply_decision_rule,
    compute_m1,
    compute_m2,
    derive_measurement_window,
    iter_turn_actions,
    load_ab_sessions,
    analyze_workspace,
    max_transcript_turn,
    summarize_by_arm,
    _resolve_post_compaction_end_turn,
)


def _tool_use_line(turn_user_text: str, tool_name: str, **input_fields: object) -> list[str]:
    return [
        json.dumps(
            {
                "type": "user",
                "message": {"role": "user", "content": [{"type": "text", "text": turn_user_text}]},
            }
        ),
        json.dumps(
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "name": tool_name,
                            "input": dict(input_fields),
                        }
                    ],
                },
            }
        ),
    ]


def test_iter_turn_actions_counts_tool_uses_per_turn() -> None:
    transcript = "\n".join(
        _tool_use_line("t1", "load_session")
        + _tool_use_line("t2", "mcp__workbay-handoff-mcp__search_handoff")
        + _tool_use_line("t3", "Read", path="src/a.py")
    ) + "\n"

    actions = iter_turn_actions(transcript)

    assert [(a.turn, a.tool_name, a.file_path) for a in actions] == [
        (2, "load_session", None),
        (4, "mcp__workbay-handoff-mcp__search_handoff", None),
        (6, "Read", "src/a.py"),
    ]


def test_compute_m1_counts_handoff_calls_and_preread_rereads() -> None:
    transcript = "\n".join(
        _tool_use_line("t1", "load_session")
        + _tool_use_line("t2", "search_handoff")
        + _tool_use_line("t3", "Read", path="src/old.py")
        + _tool_use_line("t4", "Read", path="src/new.py")
    ) + "\n"
    actions = iter_turn_actions(transcript)
    pre = {"src/old.py"}
    window = derive_measurement_window(post_compaction_end_turn=None)

    assert compute_m1(actions, pre_compaction_files=pre, window=window) == 3


def test_compute_m2_turns_to_first_write() -> None:
    transcript = "\n".join(
        _tool_use_line("t1", "load_session")
        + _tool_use_line("t2", "Read", path="src/a.py")
        + _tool_use_line("t3", "Write", path="src/b.py")
    ) + "\n"
    actions = iter_turn_actions(transcript)
    window = derive_measurement_window(post_compaction_end_turn=None)

    assert compute_m2(actions, window=window) == 6


def test_compute_m2_counts_edit_tool() -> None:
    transcript = "\n".join(
        _tool_use_line("t1", "load_session")
        + _tool_use_line("t2", "Edit", path="src/a.py")
    ) + "\n"
    actions = iter_turn_actions(transcript)
    window = derive_measurement_window(post_compaction_end_turn=None)

    assert compute_m2(actions, window=window) == 4


def test_resolve_post_compaction_end_turn_resets_for_resume_source() -> None:
    assert (
        _resolve_post_compaction_end_turn(
            source="resume",
            session_id="sess-new",
            compaction_end_turn=40,
            compaction_session_id="sess-old",
        )
        is None
    )


def test_resolve_post_compaction_end_turn_resets_on_session_mismatch() -> None:
    assert (
        _resolve_post_compaction_end_turn(
            source="compact",
            session_id="sess-b",
            compaction_end_turn=40,
            compaction_session_id="sess-a",
        )
        is None
    )


def test_resolve_post_compaction_end_turn_keeps_same_session_compact() -> None:
    assert (
        _resolve_post_compaction_end_turn(
            source="compact",
            session_id="sess-a",
            compaction_end_turn=40,
            compaction_session_id="sess-a",
        )
        == 40
    )


def test_post_compaction_window_anchors_after_compaction_end_turn() -> None:
    transcript = "\n".join(
        _tool_use_line("t1", "load_session")
        + _tool_use_line("t2", "Write", path="src/pre.py")
        + _tool_use_line("t3", "load_session")
        + _tool_use_line("t4", "search_handoff")
    ) + "\n"
    actions = iter_turn_actions(transcript)
    window = derive_measurement_window(post_compaction_end_turn=4)

    assert window.start_turn == 5
    assert compute_m1(actions, pre_compaction_files=set(), window=window) == 2
    assert compute_m2(actions, window=window) is None


def test_compute_m1_normalizes_absolute_read_paths(tmp_path: Path) -> None:
    abs_path = tmp_path / "src" / "old.py"
    abs_path.parent.mkdir(parents=True)
    transcript = "\n".join(_tool_use_line("t1", "Read", path=str(abs_path))) + "\n"
    actions = iter_turn_actions(transcript)
    window = derive_measurement_window(post_compaction_end_turn=None)

    assert compute_m1(
        actions,
        pre_compaction_files={"src/old.py"},
        window=window,
        workspace_root=tmp_path,
    ) == 1


def test_analyze_workspace_writes_results_jsonl(tmp_path: Path) -> None:
    state_dir = tmp_path / ".task-state"
    state_dir.mkdir()
    transcripts_dir = tmp_path / "transcripts"
    transcripts_dir.mkdir()

    db_path = state_dir / "handoff.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE session_reinjections (
            reinjection_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            harness TEXT NOT NULL,
            task_ref TEXT NOT NULL,
            compaction_id TEXT,
            source TEXT NOT NULL,
            emitted_chars INTEGER NOT NULL,
            arm TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE session_compactions (
            compaction_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            harness TEXT NOT NULL,
            task_ref TEXT NOT NULL,
            turn_range TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE touched_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref TEXT NOT NULL,
            file_path TEXT NOT NULL,
            change_kind TEXT NOT NULL,
            touched_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO session_compactions (
            compaction_id, session_id, harness, task_ref, turn_range, created_at
        ) VALUES ('C-1', 'sess-treat', 'claude-code', 'TASK-1', '{"start_turn":1,"end_turn":2}', '2026-06-01 00:00:00');
        INSERT INTO session_reinjections (
            reinjection_id, session_id, harness, task_ref, compaction_id,
            source, emitted_chars, arm, created_at
        ) VALUES
            ('R-1', 'sess-treat', 'claude-code', 'TASK-1', 'C-1', 'compact', 120, 'treatment', '2026-06-02 00:00:00'),
            ('R-2', 'sess-ctrl', 'claude-code', 'TASK-1', 'C-1', 'compact', 0, 'control', '2026-06-02 00:00:00');
        INSERT INTO touched_files (task_ref, file_path, change_kind, touched_at)
        VALUES ('TASK-1', 'src/old.py', 'edit', '2026-06-01 00:00:00');
        """
    )
    conn.commit()
    conn.close()

    (transcripts_dir / "sess-treat.jsonl").write_text(
        "\n".join(
            _tool_use_line("t1", "load_session")
            + _tool_use_line("t2", "Write", path="src/x.py")
        )
        + "\n",
        encoding="utf-8",
    )
    (transcripts_dir / "sess-ctrl.jsonl").write_text(
        "\n".join(
            _tool_use_line("t1", "load_session")
            + _tool_use_line("t2", "search_handoff")
            + _tool_use_line("t3", "load_session")
            + _tool_use_line("t4", "Write", path="src/y.py")
        )
        + "\n",
        encoding="utf-8",
    )

    out_path = tmp_path / "results.jsonl"
    results = analyze_workspace(
        workspace_root=tmp_path,
        transcripts_dir=transcripts_dir,
        output_path=out_path,
        state_dir=state_dir,
    )

    assert len(results) == 2
    by_session = {row.session_id: row for row in results}
    assert by_session["sess-treat"].window_start_turn == 3
    assert by_session["sess-treat"].m1_reorientation_cost == 0
    assert by_session["sess-treat"].m2_turns_to_first_write == 2
    assert by_session["sess-ctrl"].window_start_turn == 1
    assert by_session["sess-ctrl"].m1_reorientation_cost == 3
    assert by_session["sess-ctrl"].m2_turns_to_first_write == 8

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert {row["session_id"] for row in rows} == {"sess-treat", "sess-ctrl"}


def test_apply_decision_rule_invest_on_20pct_m1_reduction() -> None:
    def _result(session_id: str, arm: str, m1: int, m2: int) -> SessionResult:
        return SessionResult(
            session_id=session_id,
            arm=arm,
            task_ref="TASK",
            compaction_id=None,
            m1_reorientation_cost=m1,
            m2_turns_to_first_write=m2,
            turns_analyzed=10,
            window_start_turn=1,
        )

    treatment = [_result("sess-t1", "treatment", 2, 3), _result("sess-t2", "treatment", 2, 4)]
    control = [_result("sess-c1", "control", 5, 6), _result("sess-c2", "control", 4, 7)]
    summary = summarize_by_arm(treatment + control)
    decision = apply_decision_rule(summary, min_sessions_per_arm=2)

    assert decision["recommendation"] == "invest"
    assert decision["m1_median_delta_pct"] <= -0.20


def test_apply_decision_rule_freeze_when_m2_sign_inconsistent() -> None:
    treatment = [
        SessionResult("t1", "treatment", "TASK", None, 2, 5, 10, 1),
        SessionResult("t2", "treatment", "TASK", None, 2, 6, 10, 1),
    ]
    control = [
        SessionResult("c1", "control", "TASK", None, 5, 3, 10, 1),
        SessionResult("c2", "control", "TASK", None, 4, 4, 10, 1),
    ]
    decision = apply_decision_rule(
        summarize_by_arm(treatment + control),
        min_sessions_per_arm=2,
    )

    assert decision["recommendation"] == "freeze"
    assert decision["m2_consistent_sign"] is False


def test_apply_decision_rule_insufficient_data_without_control_median() -> None:
    treatment = [SessionResult("t1", "treatment", "TASK", None, 2, 3, 10, 1)]
    decision = apply_decision_rule(summarize_by_arm(treatment))

    assert decision["recommendation"] == "insufficient_data"


def _library_jsonl_end_turn(transcript: str) -> int:
    """Mirror compaction._iter_jsonl_turn_records ordinal accounting."""
    ordinal = 0
    for raw_line in transcript.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        if record.get("type") in ("user", "assistant"):
            ordinal += 1
    return ordinal


def test_analyzer_turn_ordinals_match_library_fixture() -> None:
    """Analyzer max turn must equal compaction library end_turn on shared JSONL."""
    transcript = "\n".join(
        _tool_use_line("t1", "load_session")
        + [
            json.dumps({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result"}]}}),
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": []}}),
        ]
        + _tool_use_line("t2", "Write", path="src/a.py")
    ) + "\n"

    assert max_transcript_turn(transcript) == _library_jsonl_end_turn(transcript)


def test_load_ab_sessions_dedupes_by_session_id(tmp_path: Path) -> None:
    db_path = tmp_path / "handoff.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE session_reinjections (
            reinjection_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            harness TEXT NOT NULL,
            task_ref TEXT NOT NULL,
            compaction_id TEXT,
            source TEXT NOT NULL,
            emitted_chars INTEGER NOT NULL,
            arm TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO session_reinjections (
            reinjection_id, session_id, harness, task_ref, compaction_id,
            source, emitted_chars, arm, created_at
        ) VALUES
            ('R-1', 'sess-a', 'claude-code', 'TASK', 'C-1', 'compact', 1, 'treatment', '2026-06-01 00:00:00'),
            ('R-2', 'sess-a', 'claude-code', 'TASK', 'C-2', 'resume', 1, 'treatment', '2026-06-02 00:00:00');
        """
    )
    sessions = load_ab_sessions(conn)
    assert len(sessions) == 1
    assert sessions[0].compaction_id == "C-2"


def test_apply_decision_rule_insufficient_data_below_session_floor() -> None:
    treatment = [SessionResult("t1", "treatment", "TASK", None, 2, 3, 10, 1)]
    control = [SessionResult("c1", "control", "TASK", None, 5, 6, 10, 1)]
    decision = apply_decision_rule(summarize_by_arm(treatment + control))

    assert decision["recommendation"] == "insufficient_data"
    assert "min_sessions_per_arm" in str(decision.get("reason"))


def test_apply_decision_rule_freeze_when_control_m1_zero() -> None:
    treatment = [
        SessionResult("t1", "treatment", "TASK", None, 1, 3, 10, 1),
        SessionResult("t2", "treatment", "TASK", None, 1, 4, 10, 1),
    ]
    control = [
        SessionResult("c1", "control", "TASK", None, 0, 3, 10, 1),
        SessionResult("c2", "control", "TASK", None, 0, 4, 10, 1),
    ]
    decision = apply_decision_rule(
        summarize_by_arm(treatment + control),
        min_sessions_per_arm=2,
    )

    assert decision["recommendation"] == "freeze"
    assert "zero" in str(decision.get("reason"))


def test_apply_decision_rule_invest_on_m2_tie_with_m1_reduction() -> None:
    treatment = [
        SessionResult("t1", "treatment", "TASK", None, 2, 4, 10, 1),
        SessionResult("t2", "treatment", "TASK", None, 2, 4, 10, 1),
    ]
    control = [
        SessionResult("c1", "control", "TASK", None, 5, 4, 10, 1),
        SessionResult("c2", "control", "TASK", None, 4, 4, 10, 1),
    ]
    decision = apply_decision_rule(
        summarize_by_arm(treatment + control),
        min_sessions_per_arm=2,
    )

    assert decision["recommendation"] == "invest"
    assert decision["m2_consistent_sign"] is True


def test_load_ab_sessions_ignores_null_arm(tmp_path: Path) -> None:
    db_path = tmp_path / "handoff.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE session_reinjections (
            reinjection_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            harness TEXT NOT NULL,
            task_ref TEXT NOT NULL,
            compaction_id TEXT,
            source TEXT NOT NULL,
            emitted_chars INTEGER NOT NULL,
            arm TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO session_reinjections (
            reinjection_id, session_id, harness, task_ref, compaction_id,
            source, emitted_chars, arm
        ) VALUES
            ('R-1', 'sess-a', 'claude-code', 'TASK', NULL, 'compact', 1, 'treatment'),
            ('R-2', 'sess-b', 'claude-code', 'TASK', NULL, 'compact', 1, NULL);
        """
    )
    sessions = load_ab_sessions(conn)
    assert [s.session_id for s in sessions] == ["sess-a"]