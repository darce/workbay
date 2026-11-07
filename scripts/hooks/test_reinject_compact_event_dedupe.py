"""Compact-event dedupe for the semantic reinjection arm of reinject-context.

The sticky semantic dedupe key includes the SessionStart compact event that
triggered the hook. Keyed on
``(task_ref, compaction_id)`` alone, one delivery suppresses every later
harness compaction until the Stop hook writes a new ``session_compactions``
row. The harness compacts every 10-60 minutes and the Stop hook writes a row
every few hours, so most compactions got no ``relevant:`` lines.

The event key is ``"<session_id>:<transcript size in bytes>"``. Each harness
compaction appends to the session transcript, so the size changes between
events and stays the same when one event fires the hook twice. A payload
without a readable transcript keeps the unkeyed ``(task_ref, compaction_id)``
behaviour.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator

import pytest

HOOK_SCRIPT = Path(__file__).parent / "reinject-context.py"

_HOOK_SURFACE_RELATIVE_PATHS = (
    Path("scripts/hooks"),
    Path("packages/workbay-system/workbay_system/payload/scripts/hooks"),
)
_PACKAGE_SOURCE_MARKERS = (
    Path("mcp-workbay-handoff/src"),
    Path("workbay-protocol/src"),
)


def _resolve_packages_dir(file_path: Path | None = None) -> Path | None:
    """Locate package sources only inside this checkout's two hook surfaces.

    Same contract as ``test_reinject_context_hook._resolve_packages_dir``: a
    consumer export without monorepo packages skips this module.
    """
    resolved_file = (file_path or Path(__file__)).resolve()
    for checkout_root in (resolved_file.parent, *resolved_file.parents):
        git_marker = checkout_root / ".git"
        if not (git_marker.is_dir() or git_marker.is_file()):
            continue
        if not any(
            resolved_file.is_relative_to(checkout_root / relative_path)
            for relative_path in _HOOK_SURFACE_RELATIVE_PATHS
        ):
            return None
        packages_dir = checkout_root / "packages"
        try:
            packages_root = packages_dir.resolve()
        except (OSError, RuntimeError):
            return None
        if packages_root.parent != checkout_root:
            return None
        if not all(
            (source_dir := packages_root / relative_path).is_dir()
            and source_dir.resolve().is_relative_to(packages_root)
            for relative_path in _PACKAGE_SOURCE_MARKERS
        ):
            return None
        return packages_root
    return None


PACKAGES_DIR = _resolve_packages_dir()
pytestmark = pytest.mark.skipif(
    PACKAGES_DIR is None,
    reason="monorepo package sources are unavailable in this consumer export",
)

_SOURCE_ROOT = PACKAGES_DIR or Path(__file__).resolve().parent
HANDOFF_SRC = _SOURCE_ROOT / "mcp-workbay-handoff" / "src"
PROTOCOL_SRC = _SOURCE_ROOT / "workbay-protocol" / "src"
WORKBAY_PACKAGE_PREFIXES = ("workbay_protocol", "workbay_handoff_mcp")

TASK_REF = "internal"


def _is_workbay_module(module_name: str) -> bool:
    return any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in WORKBAY_PACKAGE_PREFIXES)


def _prepare_source_imports() -> tuple[list[str], dict[str, ModuleType]]:
    saved_path = list(sys.path)
    saved_modules = {name: module for name, module in sys.modules.items() if _is_workbay_module(name)}
    for src in (PROTOCOL_SRC, HANDOFF_SRC):
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
    for mod_name in list(sys.modules):
        if _is_workbay_module(mod_name):
            del sys.modules[mod_name]
    return saved_path, saved_modules


def _restore_source_imports(saved_path: list[str], saved_modules: dict[str, ModuleType]) -> None:
    sys.path[:] = saved_path
    for mod_name in list(sys.modules):
        if _is_workbay_module(mod_name):
            del sys.modules[mod_name]
    sys.modules.update(saved_modules)


@pytest.fixture()
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Isolated handoff workspace with an active task seeded."""
    saved_path, saved_modules = _prepare_source_imports()
    try:
        state_dir = tmp_path / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", str(state_dir))
        monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_SHA_VALIDATION", "1")
        monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT", "1")

        from workbay_handoff_mcp import (
            RuntimeConfig,
            configure_runtime,
            set_handoff_state,
        )

        runtime = RuntimeConfig.for_workspace(
            tmp_path,
            state_dir=state_dir,
            current_task_path=tmp_path / "CURRENT_TASK.json",
        )
        configure_runtime(runtime)
        set_handoff_state(
            task_ref=TASK_REF,
            objective="Test compact-event semantic reinjection dedupe.",
            status="in_progress",
            target_branch="feature/reinjkey-01",
        )
        yield tmp_path
    finally:
        _restore_source_imports(saved_path, saved_modules)


def _seed_compaction_row(workspace: Path) -> str:
    """Persist one session_compactions row for TASK_REF; return its id."""
    transcript = workspace / "transcript.jsonl"
    transcript.write_text("turn 1 user: design the hook\nturn 2 assistant: shipped\nturn 3 user: probe\n")
    from workbay_handoff_mcp import compact_session

    receipt = compact_session(
        transcript_path=str(transcript),
        task_ref=TASK_REF,
        harness="claude-code",
        session_id="seed-session",
    )
    return receipt.summary.compaction_id


def _load_reinject_module():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("reinject_context_hook", str(HOOK_SCRIPT))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _mock_readable_semantic_lines(**kwargs):
    from workbay_handoff_mcp.embeddings.reinjection import (
        SelectedConcept,
        SemanticReinjectionResult,
    )

    concept = SelectedConcept(
        kind="decision",
        id="mock-1",
        label="decision",
        snippet="mock semantic packet marker",
        score=0.9,
        emitted_chars=32,
    )
    return (
        ["relevant: [decision:mock-1] mock semantic packet marker"],
        SemanticReinjectionResult(
            status="selected",
            skip_reason=None,
            model_id="mock-model",
            selected=[concept],
            chars_used=32,
            chars_budget=999,
            score_hi=0.9,
            score_lo=0.9,
        ),
    )


def _semantic_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    return mod


def _compact(mod, workspace: Path, *, session_id: str, compact_event: str | None) -> None:
    from workbay_handoff_mcp.compaction import CompactionSettings

    event_kwargs = {} if compact_event is None else {"compact_event": compact_event}
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id=session_id,
        source="compact",
        **event_kwargs,
    )


def _record_selected(compaction_id: str, *, session_id: str, compact_event: str | None = None) -> None:
    from workbay_handoff_mcp.compaction import record_session_reinjection
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    detail: dict[str, object] = {
        "status": "selected",
        "skip_reason": None,
        "selected": [{"kind": "decision", "id": "prior-1"}],
        "chars_used": 100,
    }
    if compact_event is not None:
        detail["compact_event"] = compact_event
    with _get_db_connection() as conn:
        record_session_reinjection(
            conn,
            session_id=session_id,
            harness="codex",
            task_ref=TASK_REF,
            compaction_id=compaction_id,
            source="compact",
            emitted_chars=100,
            semantic_detail=detail,
        )
        conn.commit()


def _reinjection_rows() -> list[tuple[str | None, dict[str, object] | None]]:
    """Return ``(compaction_id, semantic_detail)`` per telemetry row, oldest first."""
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        rows = conn.execute(
            "SELECT compaction_id, semantic_detail_json FROM session_reinjections ORDER BY rowid"
        ).fetchall()
    return [
        (
            row["compaction_id"],
            json.loads(row["semantic_detail_json"]) if row["semantic_detail_json"] else None,
        )
        for row in rows
    ]


def _run_main(mod, monkeypatch: pytest.MonkeyPatch, workspace: Path, payload: dict) -> list[dict]:
    """Drive ``main()`` in-process and record the ``_reinject`` keyword arguments."""
    import _envfile
    import _interp

    calls: list[dict] = []

    def _record(**kwargs) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(_interp, "ensure_deps_interpreter", lambda *args, **kwargs: None)
    monkeypatch.setattr(_envfile, "load_embedding_env", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "_git_repo_root", lambda: str(workspace))
    monkeypatch.setattr(mod, "_ensure_in_repo_sources_on_path", lambda repo_root: None)
    monkeypatch.setattr(mod, "_reinject", _record)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    monkeypatch.setenv("WORKBAY_REINJECT_SOURCES", "compact,resume")
    assert mod.main() == 0
    return calls


def test_compact_event_key_is_session_and_transcript_size(workspace: Path) -> None:
    """The key changes when the transcript grows and repeats while it does not."""
    mod = _load_reinject_module()
    transcript = workspace / "session.jsonl"
    transcript.write_text('{"type": "user"}\n')
    payload = {"session_id": "s1", "transcript_path": str(transcript)}

    first = mod._compact_event_key(payload)
    assert first == f"s1:{transcript.stat().st_size}"
    assert mod._compact_event_key(payload) == first

    with transcript.open("a") as handle:
        handle.write('{"type": "assistant"}\n')
    second = mod._compact_event_key(payload)
    assert second == f"s1:{transcript.stat().st_size}"
    assert second != first


@pytest.mark.parametrize("transcript_path", [None, "", "absent"], ids=["unset", "empty", "missing-file"])
def test_compact_event_key_is_none_without_a_readable_transcript(workspace: Path, transcript_path: str | None) -> None:
    mod = _load_reinject_module()
    payload: dict[str, object] = {"session_id": "s1"}
    if transcript_path == "absent":
        payload["transcript_path"] = str(workspace / "absent.jsonl")
    elif transcript_path is not None:
        payload["transcript_path"] = transcript_path

    assert mod._compact_event_key(payload) is None


def test_main_passes_compact_event_for_compact_source(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_reinject_module()
    transcript = workspace / "session.jsonl"
    transcript.write_text('{"type": "user"}\n{"type": "assistant"}\n')
    payload = {
        "hook_event_name": "SessionStart",
        "session_id": "sess-main",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
        "source": "compact",
    }

    calls = _run_main(mod, monkeypatch, workspace, payload)

    assert len(calls) == 1
    assert calls[0]["source"] == "compact"
    assert calls[0]["compact_event"] == f"sess-main:{transcript.stat().st_size}"


def test_main_passes_no_compact_event_for_resume_source(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_reinject_module()
    transcript = workspace / "session.jsonl"
    transcript.write_text('{"type": "user"}\n')
    payload = {
        "hook_event_name": "SessionStart",
        "session_id": "sess-main",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
        "source": "resume",
    }

    calls = _run_main(mod, monkeypatch, workspace, payload)

    assert len(calls) == 1
    assert calls[0]["source"] == "resume"
    assert calls[0].get("compact_event") is None


def test_distinct_compact_events_each_deliver_semantic(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """One workbay compaction record, three harness compactions: three deliveries.

    Fails on base: the second event is suppressed by the first event's row.
    """
    compaction_id = _seed_compaction_row(workspace)
    mod = _semantic_module(monkeypatch)

    events = (("s1", "s1:100"), ("s1", "s1:200"), ("s2", "s2:200"))
    for session_id, event in events:
        _compact(mod, workspace, session_id=session_id, compact_event=event)
        captured = capsys.readouterr()
        assert "relevant:" in captured.out, event
        assert "mock semantic packet marker" in captured.out, event
        assert "already reinjected" not in captured.err, event

    rows = _reinjection_rows()
    assert [row_compaction_id for row_compaction_id, _ in rows] == [compaction_id] * len(events)
    details = [detail for _, detail in rows]
    assert all(detail is not None for detail in details)
    assert [detail["status"] for detail in details] == ["selected"] * len(events)
    assert [detail["compact_event"] for detail in details] == [event for _, event in events]


def test_repeat_compact_event_is_skipped_with_typed_telemetry(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The same event firing twice delivers once and records a typed skip."""
    _seed_compaction_row(workspace)
    mod = _semantic_module(monkeypatch)

    _compact(mod, workspace, session_id="s1", compact_event="s1:100")
    assert "relevant:" in capsys.readouterr().out

    _compact(mod, workspace, session_id="s1", compact_event="s1:100")
    captured = capsys.readouterr()
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert "reinject semantic skipped: already reinjected for compact_event=s1:100" in captured.err

    _, detail = _reinjection_rows()[-1]
    assert detail is not None
    assert detail["status"] == "skipped"
    assert detail["skip_reason"] == "already_reinjected"
    assert detail["compact_event"] == "s1:100"

    # A skipped row never sticks: the session's next compaction delivers.
    _compact(mod, workspace, session_id="s1", compact_event="s1:300")
    assert "relevant:" in capsys.readouterr().out


def test_unkeyed_selected_row_does_not_suppress_a_keyed_event(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Rows written before the key existed carry no compact_event and never match one."""
    compaction_id = _seed_compaction_row(workspace)
    _record_selected(compaction_id, session_id="prior-compact")
    mod = _semantic_module(monkeypatch)

    _compact(mod, workspace, session_id="s1", compact_event="s1:100")
    captured = capsys.readouterr()
    assert "relevant:" in captured.out
    assert "already reinjected" not in captured.err


def test_unkeyed_sticky_skip_keeps_message_and_records_typed_telemetry(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Without an event key the (task_ref, compaction_id) rule still applies, now typed.

    Fails on base: the skip reaches stderr only and the telemetry row has no
    semantic detail, which reads the same as a disabled arm.
    """
    compaction_id = _seed_compaction_row(workspace)
    _record_selected(compaction_id, session_id="prior-compact")
    mod = _semantic_module(monkeypatch)

    _compact(mod, workspace, session_id="compact-repeat", compact_event=None)
    captured = capsys.readouterr()
    assert "relevant:" not in captured.out
    assert f"reinject semantic skipped: already reinjected for compaction_id={compaction_id}" in captured.err

    _, detail = _reinjection_rows()[-1]
    assert detail is not None
    assert detail["status"] == "skipped"
    assert detail["skip_reason"] == "already_reinjected"
    assert detail.get("compact_event") is None


def test_session_reinjection_exists_matches_compact_event(workspace: Path) -> None:
    from workbay_handoff_mcp.compaction import record_session_reinjection, session_reinjection_exists
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    compaction_id = _seed_compaction_row(workspace)
    _record_selected(compaction_id, session_id="s1", compact_event="s1:100")
    _record_selected(compaction_id, session_id="legacy")
    with _get_db_connection() as conn:
        record_session_reinjection(
            conn,
            session_id="s1",
            harness="codex",
            task_ref=TASK_REF,
            compaction_id=compaction_id,
            source="compact",
            emitted_chars=40,
            semantic_detail={
                "status": "skipped",
                "skip_reason": "already_reinjected",
                "selected": [],
                "chars_used": 0,
                "compact_event": "s1:300",
            },
        )
        conn.commit()

        def exists(**kwargs) -> bool:
            return session_reinjection_exists(conn, task_ref=TASK_REF, compaction_id=compaction_id, **kwargs)

        assert exists(compact_event="s1:100")
        assert not exists(compact_event="s1:200")
        assert not exists(compact_event="legacy:100")
        assert not exists(compact_event="s1:300")
        assert exists()
        assert exists(compact_event=None)
        assert not session_reinjection_exists(
            conn, task_ref="other-task", compaction_id=compaction_id, compact_event="s1:100"
        )


def test_skip_reason_literal_includes_already_reinjected(workspace: Path) -> None:
    from typing import get_args

    from workbay_handoff_mcp.embeddings.reinjection import SkipReason

    assert "already_reinjected" in get_args(SkipReason)
