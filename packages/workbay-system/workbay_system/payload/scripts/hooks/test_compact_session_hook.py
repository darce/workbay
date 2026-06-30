"""Tests for the compact-session Stop hook (implementation note of internal).

The hook fires on the harness Stop event, derives the active task from
the workspace, and writes a structured ``session_compactions`` row via
``workbay_handoff_mcp.compact_session``. Per the failure-mode contract
(see internal task plan, implementation note), the hook MUST exit 0 in every
operational outcome and surface its disposition on stderr:

- success          -> ``compaction_id=C-...``
- nothing-to-do    -> ``compaction skipped: <reason>``
- internal failure -> ``compaction failed: <reason>``

Strict-mode protocol violations (``WORKBAY_HOOK_PROTOCOL_STRICT=1``
plus a malformed event payload) remain the one exception and propagate
``SystemExit(2)`` via the shared ``_protocol.validate_event`` helper.
That contract is locked in by ``test_protocol_validation_wiring.py``
once ``compact-session.py`` is added to ``WIRED_HOOKS``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import Iterator

import pytest
from workbay_protocol import TurnRange

HOOK_SCRIPT = Path(__file__).parent / "compact-session.py"

PACKAGES_DIR = Path(__file__).resolve().parents[5]
HANDOFF_SRC = PACKAGES_DIR / "mcp-workbay-handoff" / "src"
PROTOCOL_SRC = PACKAGES_DIR / "workbay-protocol" / "src"
WORKBAY_PACKAGE_PREFIXES = ("workbay_protocol", "workbay_handoff_mcp")

# implementation note default gate is 50k new tokens; tests that expect a row write
# must lower the threshold explicitly. ``=0`` disables the gate entirely.
_COMPACTION_LOW_TOKEN_GATE = {"WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS": "1"}


def _is_workbay_module(module_name: str) -> bool:
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in WORKBAY_PACKAGE_PREFIXES
    )


def _prepare_source_imports() -> tuple[list[str], dict[str, ModuleType]]:
    saved_path = list(sys.path)
    saved_modules = {
        name: module
        for name, module in sys.modules.items()
        if _is_workbay_module(name)
    }
    for src in (PROTOCOL_SRC, HANDOFF_SRC):
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
    # Some sibling tests may have already imported a stale workbay package
    # from the parent monorepo's venv; drop those modules so the worktree
    # source wins on the next import, then restore the prior graph at teardown.
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


def _run_hook(
    payload: dict,
    *,
    workspace: Path,
    extra_env: dict[str, str] | None = None,
    scrub_compaction_env: bool = False,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if scrub_compaction_env:
        for key in list(env):
            if key.startswith("WORKBAY_HANDOFF_COMPACTION_"):
                del env[key]
    env["CLAUDE_PROJECT_DIR"] = str(workspace)
    env["WORKBAY_HANDOFF_STATE_DIR"] = str(workspace / ".task-state")
    # Pin PYTHONPATH at the in-repo sources so the hook subprocess
    # imports the worktree's workbay_handoff_mcp + workbay_protocol
    # rather than whichever copies the parent monorepo's venv has
    # editable-installed.
    existing_pp = env.get("PYTHONPATH", "")
    parts = [str(HANDOFF_SRC), str(PROTOCOL_SRC)]
    if existing_pp:
        parts.append(existing_pp)
    env["PYTHONPATH"] = os.pathsep.join(parts)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        cwd=str(workspace),
    )


@pytest.fixture()
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Isolated handoff workspace with an active task seeded.

    Drives the same configure_runtime path the hook will reach so the
    seeded row is visible to the subprocess.
    """
    saved_path, saved_modules = _prepare_source_imports()
    try:
        state_dir = tmp_path / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", str(state_dir))
        monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_SHA_VALIDATION", "1")
        monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT", "1")

        from workbay_handoff_mcp import RuntimeConfig, configure_runtime, set_handoff_state

        runtime = RuntimeConfig.for_workspace(
            tmp_path,
            state_dir=state_dir,
            current_task_path=tmp_path / "CURRENT_TASK.json",
        )
        configure_runtime(runtime)
        set_handoff_state(
            task_ref="internal",
            objective="Test the compact-session Stop hook end-to-end.",
            status="in_progress",
            target_branch="feature/internal-34",
        )
        yield tmp_path
    finally:
        _restore_source_imports(saved_path, saved_modules)


def _write_transcript(workspace: Path, body: str) -> Path:
    transcript = workspace / "transcript.jsonl"
    transcript.write_text(body)
    return transcript


def test_compact_session_hook_writes_row(workspace: Path) -> None:
    """A real Stop event with a transcript persists one compaction row
    and prints ``compaction_id=...`` on stderr.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: world\nturn 3 user: bye\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-write-row",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
        "stop_hook_active": False,
    }

    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction_id=" in result.stderr, (
        f"expected compaction_id= on stderr; got stderr={result.stderr!r}"
    )

    from workbay_handoff_mcp import get_latest_compaction

    latest = get_latest_compaction("internal")
    assert latest is not None
    assert latest.summary.session_id == "session-write-row"
    assert latest.summary.harness == "claude-code"
    # Stable handle should round-trip from stderr to the persisted row.
    stderr_id = next(
        line.split("=", 1)[1].strip()
        for line in result.stderr.splitlines()
        if line.startswith("compaction_id=")
    )
    assert latest.summary.compaction_id == stderr_id


def test_compact_session_hook_success_emits_receipt_fields(workspace: Path) -> None:
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: world\nturn 3 user: receipt\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-receipt-lines",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
        "stop_hook_active": False,
    }

    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )

    assert result.returncode == 0, f"hook exited {result.returncode}; stderr={result.stderr!r}"
    lines = [line.strip() for line in result.stderr.splitlines() if line.strip()]
    assert lines[0].startswith("compaction_id=C-internal-")
    assert [line.split("=", 1)[0] for line in lines[:6]] == [
        "compaction_id",
        "tokens_saved_estimate",
        "input_chars",
        "raw_input_bytes",
        "summary_chars",
        "prose_residual_chars",
    ]
    for line in lines[1:6]:
        key, raw_value = line.split("=", 1)
        assert raw_value.isdigit(), f"{key} must be an integer receipt value; got {line!r}"


def test_compact_session_notify_claude_emits_system_message_envelope(
    workspace: Path,
) -> None:
    """internal: Claude Stop hook prints a stdout JSON envelope."""
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: notify\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-notify-claude",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={
            **_COMPACTION_LOW_TOKEN_GATE,
            "WORKBAY_HANDOFF_HARNESS": "claude-code",
        },
    )
    assert result.returncode == 0, result.stderr
    assert "compaction_id=" in result.stderr
    assert result.stdout.strip(), "Claude notify path must emit stdout JSON"
    envelope = json.loads(result.stdout)
    assert "systemMessage" in envelope
    assert envelope["systemMessage"].startswith("workbay: compacted C-internal-")
    assert "tokens saved" in envelope["systemMessage"]


def test_compact_session_notify_codex_keeps_stderr_receipt_only(workspace: Path) -> None:
    """Non-Claude harnesses keep receipt lines on stderr; no stdout notify."""
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: codex\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-notify-codex",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={
            **_COMPACTION_LOW_TOKEN_GATE,
            "WORKBAY_HANDOFF_HARNESS": "codex",
        },
    )
    assert result.returncode == 0, result.stderr
    assert "compaction_id=" in result.stderr
    assert result.stdout.strip() == ""


def test_compact_session_notify_off_skips_stdout_envelope(workspace: Path) -> None:
    """``WORKBAY_HANDOFF_COMPACTION_NOTIFY=0`` suppresses stdout notify."""
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: quiet\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-notify-off",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={
            **_COMPACTION_LOW_TOKEN_GATE,
            "WORKBAY_HANDOFF_HARNESS": "claude-code",
            "WORKBAY_HANDOFF_COMPACTION_NOTIFY": "0",
        },
    )
    assert result.returncode == 0, result.stderr
    assert "compaction_id=" in result.stderr
    assert result.stdout.strip() == ""


def test_compact_session_hook_failure_is_non_fatal(
    workspace: Path, tmp_path: Path
) -> None:
    """A bogus state-dir surfaces as ``compaction failed:`` on stderr
    and exit 0 — never blocks the harness turn.
    """
    transcript = _write_transcript(workspace, "turn 1 user: hello\n")
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-fail",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }

    # Plant a regular file at the path the DB would want to create its
    # parent directory under. RuntimeConfig.for_repo will try to mkdir
    # the parent and fail, surfacing the documented failure path.
    bogus_parent = tmp_path / "blocker-file"
    bogus_parent.write_text("not a directory")
    bogus_state = bogus_parent / ".task-state"
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_STATE_DIR": str(bogus_state)},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction failed:" in result.stderr, (
        f"expected 'compaction failed:' on stderr; got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr, (
        "failure path must not emit a compaction_id= envelope; "
        f"stderr={result.stderr!r}"
    )


def test_compact_session_hook_round_trips_into_cold_start_render(workspace: Path) -> None:
    """End-to-end synthetic-transcript fixture from the internal plan
    Verification section: harness fires the Stop event -> hook writes a
    session_compactions row -> next process rendering the same task
    sees the structured cold-start block, dereferenced through
    compaction_id (never rowid).

    This locks in the cross-package contract that the hook (in
    packages/workbay-system) and the renderer (in
    packages/mcp-workbay-handoff) actually compose without an integration
    gap. Component tests cover each layer in isolation; this guards
    against the wiring drifting silently between releases.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: design the renderer\n"
        "turn 2 assistant: shipped renderer\n"
        "turn 3 user: end-to-end probe\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-e2e-cold-start",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }

    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    compaction_id = next(
        line.split("=", 1)[1].strip()
        for line in result.stderr.splitlines()
        if line.startswith("compaction_id=")
    )

    from workbay_handoff_mcp import api as mcp_server

    rendered = mcp_server.render_handoff(
        kind="current_task",
        task_ref="internal",
        write_file=True,
    )
    assert rendered["ok"] is True, rendered

    # internal: the v2 slim CURRENT_TASK.json projection does not
    # carry `cold_start_compaction` at the top level — that block is served
    # on demand by `render_cold_start_compaction(task_ref=...)`. Cold-start
    # consumers dereference compaction_id via that renderer (or via
    # `compaction(operation="get", compaction_id=...)`), not via a slim
    # projection field. Assert against the renderer instead.
    block = mcp_server.render_cold_start_compaction(
        task_ref="internal",
    )
    assert block is not None, (
        "render_cold_start_compaction must return a block after the "
        "Stop hook persists a row"
    )
    assert compaction_id in block, (
        f"cold-start block must dereference {compaction_id} written by "
        f"the hook; block={block!r}"
    )


def test_compact_session_hook_uses_env_harness(workspace: Path) -> None:
    """The hook must derive the harness from WORKBAY_HANDOFF_HARNESS so
    Codex / Cursor / manual callers don't get mislabeled as claude-code
    in session_compactions and StructuredSummary.

    Reproduces internal.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: world\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-codex-harness",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }

    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={
            **_COMPACTION_LOW_TOKEN_GATE,
            "WORKBAY_HANDOFF_HARNESS": "codex",
        },
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction_id=" in result.stderr

    from workbay_handoff_mcp import get_latest_compaction

    latest = get_latest_compaction("internal")
    assert latest is not None
    assert latest.summary.harness == "codex", (
        f"expected harness=codex from WORKBAY_HANDOFF_HARNESS env; "
        f"got harness={latest.summary.harness!r}"
    )


def test_compact_session_hook_does_not_skip_on_new_session(workspace: Path) -> None:
    """A fresh session whose transcript restarts turn numbering at 1 must
    NOT be skipped just because the previous session reached a higher
    end_turn. The skip decision must be session-scoped.

    Reproduces internal: prior session compaction had
    turn_range end=5, new session ships a 2-turn transcript -> the hook
    used to emit `compaction skipped: no new turns since ...` and never
    write a new row.
    """
    long_transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\nturn 3 user: how\n"
        "turn 4 assistant: fine\nturn 5 user: bye\n",
    )
    first_payload = {
        "hook_event_name": "Stop",
        "session_id": "session-long-prior",
        "transcript_path": str(long_transcript),
        "cwd": str(workspace),
    }
    first = _run_hook(
        first_payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )
    assert first.returncode == 0
    assert "compaction_id=" in first.stderr

    short_transcript = _write_transcript(
        workspace.parent / "short_transcript_dir" if False else workspace,
        # Distinct file so reading the prior compaction's turn-range
        # doesn't accidentally cover this content.
        "turn 1 user: ping\nturn 2 assistant: pong\n",
    )
    # New, different session_id with restarted turn numbering.
    second_payload = {
        "hook_event_name": "Stop",
        "session_id": "session-new-resumed",
        "transcript_path": str(short_transcript),
        "cwd": str(workspace),
    }
    second = _run_hook(
        second_payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )

    assert second.returncode == 0, (
        f"hook exited {second.returncode}; stderr={second.stderr!r}"
    )
    assert "compaction_id=" in second.stderr, (
        "new session must not be skipped just because turn numbering "
        f"restarted; stderr={second.stderr!r}"
    )
    assert "compaction skipped" not in second.stderr


def test_disabled_env_var_skips(workspace: Path) -> None:
    """``WORKBAY_HANDOFF_COMPACTION_DISABLED=1`` makes the hook emit a skip line
    and exit 0 without writing any session_compactions row.

    internal.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-disabled",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_DISABLED": "1"},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: disabled" in result.stderr, (
        f"expected disabled-skip line; got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()
    assert row_count["n"] == 0, "disabled gate must not insert any row"


def test_min_new_turns_env_no_longer_gates_compaction(workspace: Path) -> None:
    """implementation note D1: min_new_turns is retired as an independent trigger gate."""
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-min-turns-retired",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TURNS": "100"},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "new turn" not in result.stderr
    assert "compaction skipped: only" in result.stderr
    assert "new tokens" in result.stderr


def test_min_new_tokens_threshold_skips(workspace: Path) -> None:
    """``WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS`` larger than the encoded
    transcript token count must short-circuit with a skip line.

    internal.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-min-tokens",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS": "100000"},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: only" in result.stderr, (
        f"expected min-tokens skip line; got stderr={result.stderr!r}"
    )
    assert "threshold 100000" in result.stderr
    assert "compaction_id=" not in result.stderr


def test_workbay_compaction_disabled_skips(workspace: Path) -> None:
    """``WORKBAY_HANDOFF_COMPACTION_DISABLED=1`` is the canonical
    consolidated env-var name. The hook must short-circuit on it and emit
    no deprecation noise (the legacy alias shim has been retired).
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-disabled-new",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_DISABLED": "1"},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: disabled" in result.stderr, (
        f"expected disabled-skip line; got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr
    assert "deprecated" not in result.stderr.lower(), (
        "no deprecation warning expected when only the canonical name "
        f"is set; stderr={result.stderr!r}"
    )


def test_compaction_settings_db_disable_skips_hook(workspace: Path) -> None:
    """internal: a workspace-default ``compaction_settings`` row with
    ``enabled=0`` (the row that ``compaction(operation='disable')``
    writes) silences the Stop hook through the same unified resolver as
    the env var. Source label is ``db``.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )

    # Seed the workspace-default disable row by reusing the upsert helper
    # against the same workspace state dir the hook subprocess will hit.
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.compaction import upsert_compaction_disabled
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    configure_runtime(
        RuntimeConfig.for_repo(workspace, state_dir=workspace / ".task-state")
    )
    with _get_db_connection() as conn:
        upsert_compaction_disabled(
            conn,
            scope_kind="workspace",
            task_ref=None,
            enabled=False,
            actor="test",
        )
        conn.commit()

    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-db-disabled",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(payload, workspace=workspace)

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: disabled (source=db)" in result.stderr, (
        f"expected db-disabled-skip line; got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr


def test_compaction_settings_task_scoped_disable_skips_hook(workspace: Path) -> None:
    """internal: task-scoped DB disables must silence the Stop hook
    for the active task, not only the advisory path.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )

    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.compaction import upsert_compaction_disabled
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    configure_runtime(
        RuntimeConfig.for_repo(workspace, state_dir=workspace / ".task-state")
    )
    with _get_db_connection() as conn:
        upsert_compaction_disabled(
            conn,
            scope_kind="task",
            task_ref="internal",
            enabled=False,
            actor="test",
        )
        conn.commit()

    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-task-db-disabled",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(payload, workspace=workspace)

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: disabled (source=db)" in result.stderr, (
        f"expected task-scoped db-disabled-skip line; got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()["n"]
    assert row_count == 0


def test_invalid_int_env_var_emits_compaction_failed(workspace: Path) -> None:
    """Bad int values for ``WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS`` must
    surface as a ``compaction failed: invalid compaction settings: ...``
    line via the typed ``CompactionSettings.from_env()`` boundary, not
    silently fall back to the default and write a compaction row.

    Reproduces BR-internal: implementation note added the typed
    surface but the live Stop hook kept its inline ``int(...)``
    fallback, so invalid env values still wrote a compaction row.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-bad-int",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS": "abc"},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction failed: invalid compaction settings:" in result.stderr, (
        "expected loud failure on invalid int env var; "
        f"got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr, (
        "invalid env value must not silently fall back to default and "
        f"persist a row; stderr={result.stderr!r}"
    )

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()
    assert row_count["n"] == 0, (
        "invalid-settings path must not insert any row; "
        f"saw {row_count['n']} rows"
    )


def test_thresholds_default_match_plan_0037_50k_gate(workspace: Path) -> None:
    """With no WORKBAY_HANDOFF_COMPACTION_* env vars set, the hook must
    apply the plan-0037 install default: skip until new_tokens >= 50_000.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-defaults",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(payload, workspace=workspace, scrub_compaction_env=True)

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: only" in result.stderr
    assert "threshold 50000" in result.stderr
    assert "compaction_id=" not in result.stderr

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()["n"]
    assert row_count == 0


def test_jsonl_default_gate_writes_row_above_50k_tokens(workspace: Path) -> None:
    """implementation note: real-shape JSONL above the contract default gate writes a row.

    Mutation check (by hand): deleting the 50k gate or breaking JSONL ordinal
    slicing must turn this case red alongside
    ``test_jsonl_default_gate_skips_below_50k_tokens``.
    """
    transcript = _write_transcript(
        workspace,
        _jsonl_transcript_at_least_tokens(50_001),
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-above-gate",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        scrub_compaction_env=True,
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction_id=" in result.stderr, (
        "default 50k gate must allow compaction on large JSONL slice; "
        f"stderr={result.stderr!r}"
    )

    from workbay_handoff_mcp import get_latest_compaction
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    latest = get_latest_compaction("internal")
    assert latest is not None
    assert latest.summary.session_id == "session-jsonl-above-gate"
    stderr_id = next(
        line.split("=", 1)[1].strip()
        for line in result.stderr.splitlines()
        if line.startswith("compaction_id=")
    )
    assert latest.summary.compaction_id == stderr_id

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()["n"]
    assert row_count == 1


def test_jsonl_default_gate_skips_below_50k_tokens(workspace: Path) -> None:
    """implementation note: JSONL twin of the prose below-threshold default-gate skip."""
    transcript = _write_transcript(workspace, _jsonl_turn_transcript("hi", "there"))
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-below-gate",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        scrub_compaction_env=True,
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: only" in result.stderr
    assert "threshold 50000" in result.stderr
    assert "compaction_id=" not in result.stderr

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()["n"]
    assert row_count == 0


def test_compact_session_hook_skips_when_no_new_turns(workspace: Path) -> None:
    """A second invocation against an unchanged transcript head must
    short-circuit with ``compaction skipped:`` instead of writing a
    duplicate row.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hello\nturn 2 assistant: world\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-skip",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }

    first = _run_hook(
        payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )
    assert first.returncode == 0
    assert "compaction_id=" in first.stderr

    second = _run_hook(payload, workspace=workspace)
    assert second.returncode == 0, (
        f"second invocation exited {second.returncode}; "
        f"stderr={second.stderr!r}"
    )
    assert "compaction skipped:" in second.stderr, (
        f"expected 'compaction skipped:' on second invocation; "
        f"stderr={second.stderr!r}"
    )
    assert "compaction_id=" not in second.stderr

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()
    assert row_count["n"] == 1, (
        "skip path must not insert a second row; "
        f"saw {row_count['n']} rows"
    )


# ---------------------------------------------------------------------------
# internal.5e — Resolution-Rule shape coverage
#
# The hook's task-ref derivation routes through the server-side four-step
# Resolution Rule (``shared_primitives._resolve_task_ref`` ->
# ``resolve_active_task_ref``). The existing tests above all run under the
# ``single`` projection shape. The two tests below pin the hook's
# failure-mode contract under the other two shapes from the internal
# workspace summary contract: ``none`` (no live task) and
# ``workspace_ambiguous`` (multiple live tasks with no
# target_worktree_path / cwd disambiguator). The ``none`` shape still
# surfaces ``compaction failed: active task unresolved``; the ambiguous
# shape tiebreaks to the most-recent row and emits a stderr note (implementation note).
# ---------------------------------------------------------------------------


def _seed_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Configure the in-process MCP runtime against ``tmp_path`` without
    seeding any handoff_state row. Mirrors the prefix of the ``workspace``
    fixture so the two ``shape`` fixtures below stay structurally aligned
    with the single-shape baseline.
    """
    state_dir = tmp_path / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", str(state_dir))
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_SHA_VALIDATION", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")

    from workbay_handoff_mcp import RuntimeConfig, configure_runtime

    runtime = RuntimeConfig.for_workspace(
        tmp_path,
        state_dir=state_dir,
        current_task_path=tmp_path / "CURRENT_TASK.json",
    )
    configure_runtime(runtime)
    return tmp_path


@pytest.fixture()
def workspace_no_active_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Path]:
    """Workspace with the runtime configured but zero handoff_state rows.

    Exercises the ``none`` projection shape: the four-step resolver
    finds no live tasks and raises ``ValueError("No active task ...")``.
    """
    saved_path, saved_modules = _prepare_source_imports()
    try:
        yield _seed_runtime(tmp_path, monkeypatch)
    finally:
        _restore_source_imports(saved_path, saved_modules)


@pytest.fixture()
def workspace_ambiguous_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Path]:
    """Workspace with two active tasks and no target_worktree_path on
    either row — hooks tiebreak to the most-recent ``updated_at`` row.
    Pins the ``workspace_ambiguous`` projection shape.
    """
    import time

    saved_path, saved_modules = _prepare_source_imports()
    try:
        workspace = _seed_runtime(tmp_path, monkeypatch)
        from workbay_handoff_mcp import set_handoff_state

        set_handoff_state(
            task_ref="AMBIG-A",
            objective="Ambiguous-shape fixture row A.",
            status="in_progress",
            target_branch="feature/ambig-a",
        )
        time.sleep(1.05)
        set_handoff_state(
            task_ref="AMBIG-B",
            objective="Ambiguous-shape fixture row B.",
            status="in_progress",
            target_branch="feature/ambig-b",
        )
        yield workspace
    finally:
        _restore_source_imports(saved_path, saved_modules)


def _assert_unresolved_failure(result: subprocess.CompletedProcess) -> None:
    """Shared assertion bundle for both shape failure cases.

    Pins the failure-mode contract: returncode 0, the documented stderr
    envelope, and no ``compaction_id=`` leak (which would imply a row
    was written despite the resolver failure).
    """
    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction failed: active task unresolved" in result.stderr, (
        "expected 'compaction failed: active task unresolved' on stderr; "
        f"got stderr={result.stderr!r}"
    )
    assert "compaction_id=" not in result.stderr, (
        "unresolved-task path must not emit a compaction_id= envelope; "
        f"stderr={result.stderr!r}"
    )


def test_compact_session_hook_none_shape_exits_clean(
    workspace_no_active_task: Path,
) -> None:
    """``none`` shape: the four-step resolver finds zero live tasks and
    raises. The hook MUST surface ``compaction failed: active task
    unresolved`` and exit 0 so the harness turn is not blocked. Covers
    internal.5e proof for the ``none`` projection shape.
    """
    transcript = _write_transcript(
        workspace_no_active_task,
        "turn 1 user: hello\nturn 2 assistant: world\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-none-shape",
        "transcript_path": str(transcript),
        "cwd": str(workspace_no_active_task),
    }
    _assert_unresolved_failure(
        _run_hook(payload, workspace=workspace_no_active_task)
    )


def test_compact_session_hook_workspace_ambiguous_shape_tiebreaks(
    workspace_ambiguous_tasks: Path,
) -> None:
    """``workspace_ambiguous`` shape: two live tasks with no cwd match.

    implementation note: the hook tiebreaks to the most-recent ``updated_at`` row,
    emits the stderr note, and compacts against that task_ref.
    """
    transcript = _write_transcript(
        workspace_ambiguous_tasks,
        "turn 1 user: hello\nturn 2 assistant: world\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-ambiguous-shape",
        "transcript_path": str(transcript),
        "cwd": str(workspace_ambiguous_tasks),
    }
    result = _run_hook(
        payload,
        workspace=workspace_ambiguous_tasks,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )
    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "ambiguous active task: chose AMBIG-B (most recent)" in result.stderr
    assert "AMBIG-A" in result.stderr
    assert "WORKBAY_HANDOFF_ACTIVE_TASK" in result.stderr
    assert "compaction_id=C-AMBIG-B-" in result.stderr


def test_compact_session_hook_ambiguous_tiebreak_honors_task_scoped_disable(
    workspace_ambiguous_tasks: Path,
) -> None:
    """Disable preflight must use the same tiebroken task_ref as ``_compact``."""
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.compaction import upsert_compaction_disabled
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    configure_runtime(
        RuntimeConfig.for_repo(
            workspace_ambiguous_tasks,
            state_dir=workspace_ambiguous_tasks / ".task-state",
        )
    )
    with _get_db_connection() as conn:
        upsert_compaction_disabled(
            conn,
            scope_kind="task",
            task_ref="AMBIG-B",
            enabled=False,
            actor="test",
        )
        conn.commit()

    transcript = _write_transcript(
        workspace_ambiguous_tasks,
        "turn 1 user: hello\nturn 2 assistant: world\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-ambiguous-disabled",
        "transcript_path": str(transcript),
        "cwd": str(workspace_ambiguous_tasks),
    }
    result = _run_hook(payload, workspace=workspace_ambiguous_tasks)

    assert result.returncode == 0, result.stderr
    assert "compaction skipped: disabled (source=db)" in result.stderr
    assert "compaction_id=" not in result.stderr


def test_compact_session_hook_workspace_ambiguous_pinned_task_ref(
    workspace_ambiguous_tasks: Path,
) -> None:
    """``WORKBAY_HANDOFF_ACTIVE_TASK`` overrides tiebreak when set."""
    transcript = _write_transcript(
        workspace_ambiguous_tasks,
        "turn 1 user: hello\nturn 2 assistant: world\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-ambiguous-pinned",
        "transcript_path": str(transcript),
        "cwd": str(workspace_ambiguous_tasks),
    }
    result = _run_hook(
        payload,
        workspace=workspace_ambiguous_tasks,
        extra_env={
            **_COMPACTION_LOW_TOKEN_GATE,
            "WORKBAY_HANDOFF_ACTIVE_TASK": "AMBIG-A",
        },
    )
    assert result.returncode == 0, result.stderr
    assert "ambiguous active task:" not in result.stderr
    assert "compaction_id=C-AMBIG-A-" in result.stderr


def test_min_new_tokens_zero_disables_gate(workspace: Path) -> None:
    """implementation note D5: ``WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS=0`` means
    *never fire* the token gate — every turn is skipped and no row is written.

    This is the load-bearing inversion of the old ``0 == off`` (write every
    turn) semantics, so it gets an explicit guard against silent regression.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-tokens-zero",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS": "0"},
    )

    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert (
        "compaction skipped: min_new_tokens gate disabled (override is 0)"
        in result.stderr
    ), f"expected gate-disabled skip line; got stderr={result.stderr!r}"
    assert "compaction_id=" not in result.stderr

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row_count = conn.execute(
            "SELECT COUNT(*) AS n FROM session_compactions WHERE task_ref = ?",
            ("internal",),
        ).fetchone()["n"]
    assert row_count == 0, "disabled token gate must not insert any row"


def test_compact_session_skip_keeps_empty_stdout_under_claude(workspace: Path) -> None:
    """implementation note D3: a SKIP must keep stdout empty even under
    ``WORKBAY_HANDOFF_HARNESS=claude-code`` with notify on (default).

    The stdout ``systemMessage`` envelope is reachable only after a row is
    written; every skip path returns before the notify block. Here the default
    50k gate skips the tiny transcript, so stdout stays empty and no envelope
    leaks into the model's context.
    """
    transcript = _write_transcript(
        workspace,
        "turn 1 user: hi\nturn 2 assistant: hello\n",
    )
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-skip-claude-empty-stdout",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_HARNESS": "claude-code"},
    )
    assert result.returncode == 0, (
        f"hook exited {result.returncode}; stderr={result.stderr!r}"
    )
    assert "compaction skipped: only" in result.stderr
    assert "compaction_id=" not in result.stderr
    assert result.stdout == "", (
        "skip path must keep stdout empty under claude-code+notify-on; "
        f"got stdout={result.stdout!r}"
    )


def _jsonl_user_message(text: str) -> str:
    return json.dumps(
        {
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        }
    )


def _jsonl_assistant_message(text: str) -> str:
    return json.dumps(
        {
            "type": "assistant",
            "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
        }
    )


def _jsonl_turn_transcript(*messages: str) -> str:
    lines: list[str] = []
    for index, text in enumerate(messages):
        if index % 2 == 0:
            lines.append(_jsonl_user_message(text))
        else:
            lines.append(_jsonl_assistant_message(text))
    return "\n".join(lines) + "\n"


def _load_compact_session_module():
    spec = spec_from_file_location("compact_session_hook_tokens", str(HOOK_SCRIPT))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _count_new_turn_tokens(text: str) -> int:
    mod = _load_compact_session_module()
    return mod._count_new_turn_tokens(text)


def _jsonl_gate_token_count(transcript: str) -> int:
    """Match the Stop hook gate: slice new turns, slim, then count tokens."""
    from workbay_handoff_mcp.compaction import (
        CompactionSettings,
        _process_transcript_text,
        slice_new_turn_transcript,
    )

    settings = CompactionSettings()
    new_text = slice_new_turn_transcript(transcript, since_turn=0)
    gate_text = _process_transcript_text(new_text, settings)
    return _count_new_turn_tokens(gate_text)


def _jsonl_transcript_at_least_tokens(min_tokens: int) -> str:
    """Real-shape alternating JSONL whose gated new-turn text meets ``min_tokens``."""
    chunk = "token "
    low, high = 1, 1
    while _jsonl_gate_token_count(_jsonl_turn_transcript(chunk * high, "ack")) < min_tokens:
        high *= 2
    while low < high:
        mid = (low + high) // 2
        if _jsonl_gate_token_count(_jsonl_turn_transcript(chunk * mid, "ack")) < min_tokens:
            low = mid + 1
        else:
            high = mid
    return _jsonl_turn_transcript(chunk * low, "ack")


def test_jsonl_second_stop_compacts_disjoint_turn_range(workspace: Path) -> None:
    """Growing real-shape JSONL: second Stop compacts only the new turns."""
    transcript = _write_transcript(workspace, _jsonl_turn_transcript("a", "b"))
    first_payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-growth",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    first = _run_hook(
        first_payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )
    assert first.returncode == 0
    assert "compaction_id=" in first.stderr

    transcript.write_text(_jsonl_turn_transcript("a", "b", "c", "d"), encoding="utf-8")
    second_payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-growth",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    second = _run_hook(
        second_payload,
        workspace=workspace,
        extra_env=_COMPACTION_LOW_TOKEN_GATE,
    )
    assert second.returncode == 0, (
        f"hook exited {second.returncode}; stderr={second.stderr!r}"
    )
    assert "compaction_id=" in second.stderr, (
        "second Stop over growing JSONL must compact new turns; "
        f"stderr={second.stderr!r}"
    )

    from workbay_handoff_mcp import get_latest_compaction
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        rows = conn.execute(
            "SELECT structured_summary_json FROM session_compactions ORDER BY created_at"
        ).fetchall()
    assert len(rows) == 2
    first_summary = json.loads(rows[0]["structured_summary_json"])
    second_summary = json.loads(rows[1]["structured_summary_json"])
    assert first_summary["turn_range"] == {"start_turn": 1, "end_turn": 2}
    assert second_summary["turn_range"] == {"start_turn": 3, "end_turn": 4}

    latest = get_latest_compaction("internal")
    assert latest is not None
    assert latest.summary.turn_range.model_dump() == {"start_turn": 3, "end_turn": 4}


def test_jsonl_second_stop_unchanged_transcript_skips(workspace: Path) -> None:
    """Second Stop with unchanged JSONL must skip instead of writing an empty slice."""
    transcript = _write_transcript(workspace, _jsonl_turn_transcript("a", "b"))
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-unchanged",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    first = _run_hook(payload, workspace=workspace, extra_env=_COMPACTION_LOW_TOKEN_GATE)
    assert first.returncode == 0
    assert "compaction_id=" in first.stderr

    second = _run_hook(payload, workspace=workspace, extra_env=_COMPACTION_LOW_TOKEN_GATE)
    assert second.returncode == 0
    assert "compaction skipped: no new turns since" in second.stderr
    assert "compaction_id=" not in second.stderr


def test_jsonl_turn_derivation_hook_matches_library(workspace: Path) -> None:
    """Hook path persists the same sliced turn_range the library derives."""
    transcript = _write_transcript(workspace, _jsonl_turn_transcript("x", "y", "z"))
    first_payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-cross",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    first = _run_hook(first_payload, workspace=workspace, extra_env=_COMPACTION_LOW_TOKEN_GATE)
    assert first.returncode == 0
    assert "compaction_id=" in first.stderr

    from workbay_handoff_mcp.compaction import _derive_turn_range, slice_new_turn_transcript

    full_transcript = _jsonl_turn_transcript("x", "y", "z", "w")
    transcript.write_text(full_transcript, encoding="utf-8")
    expected_slice = slice_new_turn_transcript(full_transcript, since_turn=3)
    expected_range = TurnRange(start_turn=4, end_turn=4)
    assert _derive_turn_range(full_transcript).model_dump() == {"start_turn": 1, "end_turn": 4}

    second_payload = {
        "hook_event_name": "Stop",
        "session_id": "session-jsonl-cross",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    second = _run_hook(second_payload, workspace=workspace, extra_env=_COMPACTION_LOW_TOKEN_GATE)
    assert second.returncode == 0
    assert "compaction_id=" in second.stderr

    from workbay_handoff_mcp import CompactionSettings, get_latest_compaction
    from workbay_handoff_mcp.compaction import _process_transcript_text

    latest = get_latest_compaction("internal")
    assert latest is not None
    assert latest.summary.turn_range.model_dump() == expected_range.model_dump()
    settings = CompactionSettings.from_env(_COMPACTION_LOW_TOKEN_GATE)
    processed_slice = _process_transcript_text(expected_slice, settings)
    assert "w" in processed_slice
    assert "x" not in processed_slice
    assert latest.summary.prose_residual is not None
    assert "w" in latest.summary.prose_residual
