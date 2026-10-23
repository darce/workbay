"""Tests for the reinject-context SessionStart hook (implementation note of internal).

The hook fires on the harness ``SessionStart`` event, gates on the event
``source`` (default: ``compact`` / ``resume``), resolves the active task
from the workspace, and emits ONE budgeted fenced block of handoff.db
references to **stdout** — the surface Claude Code injects into model
context. Per the failure-mode contract (implementation note, implementation note) the hook MUST
exit 0 in every operational outcome, emit nothing on stdout unless a block
is produced, and surface its disposition on stderr:

- success     -> fenced ```workbay-reinject block on stdout
- gated/noop  -> ``reinject skipped: <reason>`` on stderr, empty stdout
- any failure -> ``reinject skipped: <reason>`` on stderr, empty stdout

Successful emissions best-effort write one ``session_reinjections`` row; skip
paths write nothing.

Strict-mode protocol violations (``WORKBAY_HOOK_PROTOCOL_STRICT=1`` plus
a malformed event payload) remain the one exception and propagate
``SystemExit(2)`` via the shared ``_protocol.validate_event`` helper.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator

import pytest

HOOK_SCRIPT = Path(__file__).parent / "reinject-context.py"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"

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

    Public consumers may ship only ``scripts/hooks`` and no monorepo
    packages.  Return ``None`` for that supported shape so collection can
    skip this module; never borrow a similarly named packages directory from
    an enclosing checkout.
    """
    resolved_file = (file_path or Path(__file__)).resolve()
    for checkout_root in (resolved_file.parent, *resolved_file.parents):
        git_marker = checkout_root / ".git"
        if not (git_marker.is_dir() or git_marker.is_file()):
            continue

        # The nearest checkout boundary owns the result. A path nested under
        # an unrelated packages tree is not a shipped hook surface, even when
        # an ancestor happens to contain both source markers.
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


@pytest.mark.parametrize(
    "hook_relative_path",
    _HOOK_SURFACE_RELATIVE_PATHS,
    ids=["root", "payload"],
)
def test_resolve_packages_dir_accepts_exact_hook_surfaces(tmp_path: Path, hook_relative_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    for marker in _PACKAGE_SOURCE_MARKERS:
        (checkout / "packages" / marker).mkdir(parents=True)
    hook_file = checkout / hook_relative_path / Path(__file__).name
    hook_file.parent.mkdir(parents=True)

    assert _resolve_packages_dir(hook_file) == checkout / "packages"


@pytest.mark.parametrize(
    ("hook_relative_path", "boundary_relative_path"),
    [
        (Path("consumer/scripts/hooks"), Path("consumer")),
        (
            Path("packages/foreign/workbay_system/payload/scripts/hooks"),
            Path("packages/foreign"),
        ),
    ],
    ids=["unrelated-consumer", "foreign-payload"],
)
@pytest.mark.parametrize("inner_checkout", [False, True], ids=["ancestor", "boundary"])
def test_resolve_packages_dir_does_not_cross_unrelated_checkout(
    tmp_path: Path,
    hook_relative_path: Path,
    boundary_relative_path: Path,
    inner_checkout: bool,
) -> None:
    """A nested or foreign hook path must not inherit ancestor sources."""
    outer = tmp_path / "outer"
    (outer / ".git").mkdir(parents=True)
    for marker in _PACKAGE_SOURCE_MARKERS:
        (outer / "packages" / marker).mkdir(parents=True)
    if inner_checkout:
        (outer / boundary_relative_path / ".git").mkdir(parents=True)
    hook_file = outer / hook_relative_path / Path(__file__).name
    hook_file.parent.mkdir(parents=True)

    assert _resolve_packages_dir(hook_file) is None


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


def _run_hook(
    payload: dict,
    *,
    workspace: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["CLAUDE_PROJECT_DIR"] = str(workspace)
    env["WORKBAY_HANDOFF_STATE_DIR"] = str(workspace / ".task-state")
    # Pin PYTHONPATH at the in-repo sources so the hook subprocess imports
    # the worktree's workbay_handoff_mcp + workbay_protocol rather than
    # whichever copies the parent monorepo's venv has editable-installed.
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
            objective="Test the reinject-context SessionStart hook end-to-end.",
            status="in_progress",
            target_branch="feature/ws-reinj-01",
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


def _stdout_injection(result: subprocess.CompletedProcess) -> str:
    """Return injected context from stdout (raw block or Claude JSON envelope)."""
    stdout = result.stdout
    if not stdout.strip():
        return ""
    if stdout.lstrip().startswith("{"):
        envelope = json.loads(stdout)
        return str(envelope["hookSpecificOutput"]["additionalContext"])
    return stdout


def _payload(source: str | None, session_id: str = "session-reinject") -> dict:
    payload = {
        "hook_event_name": "SessionStart",
        "session_id": session_id,
        "cwd": "",
    }
    if source is not None:
        payload["source"] = source
    return payload


def _db_write_snapshot() -> dict[str, int]:
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        return {
            table: conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            for table in (
                "session_compactions",
                "session_reinjections",
                "decisions",
                "handoff_state",
            )
        }


def _latest_reinjection_row() -> dict[str, object] | None:
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT reinjection_id, session_id, task_ref, compaction_id, source, "
            "emitted_chars, arm "
            "FROM session_reinjections ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row is not None else None


def _load_reinject_module():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("reinject_context_hook", str(HOOK_SCRIPT))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_reinject_emits_block_on_compact_source(workspace: Path) -> None:
    """source=compact emits ONE fenced block on stdout carrying task_ref,
    the latest compaction_id, and the literal deep-recovery command hints —
    and best-effort writes one session_reinjections telemetry row.
    """
    compaction_id = _seed_compaction_row(workspace)
    before = _db_write_snapshot()

    result = _run_hook(_payload("compact"), workspace=workspace)

    assert result.returncode == 0, f"hook exited {result.returncode}; stderr={result.stderr!r}"
    assert "reinject skipped" not in result.stderr, result.stderr
    block = _stdout_injection(result)
    assert block.startswith("```workbay-reinject"), f"stdout={result.stdout!r}"
    assert block.rstrip().endswith("```"), f"stdout={block!r}"
    assert TASK_REF in block
    assert compaction_id in block, f"block must dereference latest compaction row; stdout={block!r}"
    assert "in_progress" in block
    # Literal command hints for deeper agent-initiated recovery.
    assert "compaction(get_latest)" in block
    assert 'get_handoff_state(read_profile="hot_summary")' in block

    after = _db_write_snapshot()
    assert after["session_reinjections"] == before["session_reinjections"] + 1
    assert after["decisions"] == before["decisions"]
    assert after["handoff_state"] == before["handoff_state"]
    telemetry = _latest_reinjection_row()
    assert telemetry is not None
    assert telemetry["task_ref"] == TASK_REF
    assert telemetry["compaction_id"] == compaction_id
    assert telemetry["source"] == "compact"
    assert int(telemetry["emitted_chars"]) == len(result.stdout)


def test_reinject_emits_block_on_resume_source(workspace: Path) -> None:
    before = _db_write_snapshot()
    result = _run_hook(_payload("resume"), workspace=workspace)

    assert result.returncode == 0
    block = _stdout_injection(result)
    assert block.startswith("```workbay-reinject")
    assert TASK_REF in block
    after = _db_write_snapshot()
    assert after["session_reinjections"] == before["session_reinjections"] + 1
    telemetry = _latest_reinjection_row()
    assert telemetry is not None
    assert telemetry["task_ref"] == TASK_REF
    assert telemetry["source"] == "resume"
    assert int(telemetry["emitted_chars"]) == len(result.stdout)


def test_reinject_notify_claude_emits_json_envelope(workspace: Path) -> None:
    """internal: Claude + notify-on wraps block in SessionStart JSON."""
    compaction_id = _seed_compaction_row(workspace)
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_HARNESS": "claude-code"},
    )
    assert result.returncode == 0, result.stderr
    envelope = json.loads(result.stdout)
    assert envelope["hookSpecificOutput"]["hookEventName"] == "SessionStart"
    block = envelope["hookSpecificOutput"]["additionalContext"]
    assert block.startswith("```workbay-reinject")
    assert compaction_id in block
    # The one-result floor keeps the semantic arm from being starved here, so
    # the notify line carries semantic telemetry rather than the legacy
    # "re-fed compaction" fallback that a starved arm produced.
    message = envelope["systemMessage"]
    assert message.startswith("workbay: reinject ")
    assert "semantic=selected" in message
    assert compaction_id in message
    assert TASK_REF in message


def test_reinject_notify_off_emits_raw_block_on_claude(workspace: Path) -> None:
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={
            "WORKBAY_HANDOFF_HARNESS": "claude-code",
            "WORKBAY_HANDOFF_COMPACTION_NOTIFY": "0",
        },
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("```workbay-reinject")
    assert "systemMessage" not in result.stdout


def test_reinject_notify_codex_emits_raw_block(workspace: Path) -> None:
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_HARNESS": "codex"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("```workbay-reinject")
    assert not result.stdout.lstrip().startswith("{")


def test_reinject_notify_grok_env_emits_raw_block(workspace: Path) -> None:
    """implementation note R1 / REV-E-010: a grok launcher sets GROK_WORKSPACE_ROOT but
    no WORKBAY_HANDOFF_HARNESS export (the compat-loaded .claude entry must
    not carry one). _resolve_harness must classify this as grok — NOT fall
    through to the claude-code default and emit the Claude-only JSON envelope.
    Mirrors compact-session.py's grok fallback so both hooks agree.
    """
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        # Force the harness override empty so the GROK_WORKSPACE_ROOT
        # fallback is exercised deterministically regardless of ambient env.
        extra_env={
            "WORKBAY_HANDOFF_HARNESS": "",
            "GROK_WORKSPACE_ROOT": str(workspace),
        },
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("```workbay-reinject"), (
        f"grok must receive the raw fenced block, not the Claude JSON envelope; got stdout={result.stdout[:80]!r}"
    )
    assert not result.stdout.lstrip().startswith("{")
    assert "systemMessage" not in result.stdout


def test_reinject_notify_claude_context_parity_with_raw_block(workspace: Path) -> None:
    """Envelope additionalContext must match the raw fenced block byte-for-byte."""
    _seed_compaction_row(workspace)
    raw = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={
            "WORKBAY_HANDOFF_HARNESS": "codex",
            "WORKBAY_HANDOFF_COMPACTION_NOTIFY": "1",
        },
    )
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        conn.execute("DELETE FROM session_reinjections")
        conn.commit()
    wrapped = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={
            "WORKBAY_HANDOFF_HARNESS": "claude-code",
            "WORKBAY_HANDOFF_COMPACTION_NOTIFY": "1",
        },
    )
    assert raw.returncode == 0 and wrapped.returncode == 0
    assert _stdout_injection(wrapped).rstrip("\n") == raw.stdout.rstrip("\n")


def test_reinject_skip_path_writes_no_telemetry_row(workspace: Path) -> None:
    before = _db_write_snapshot()
    result = _run_hook(_payload("startup"), workspace=workspace)
    assert result.returncode == 0
    assert result.stdout == ""
    after = _db_write_snapshot()
    assert after["session_reinjections"] == before["session_reinjections"]


def test_reinject_block_without_compaction_row_still_emits_state(
    workspace: Path,
) -> None:
    """No session_compactions row yet: the block still carries task identity
    but no compaction line.
    """
    before = _db_write_snapshot()
    result = _run_hook(_payload("compact"), workspace=workspace)

    assert result.returncode == 0
    block = _stdout_injection(result)
    assert block.startswith("```workbay-reinject")
    assert TASK_REF in block
    assert "latest_compaction" not in block
    after = _db_write_snapshot()
    assert after["session_reinjections"] == before["session_reinjections"] + 1
    telemetry = _latest_reinjection_row()
    assert telemetry is not None
    assert telemetry["compaction_id"] is None
    assert telemetry["source"] == "compact"


def test_reinject_telemetry_write_failure_still_emits(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Telemetry write failure must not block stdout injection (internal)."""
    from workbay_handoff_mcp import compaction as compaction_mod
    from workbay_handoff_mcp.compaction import CompactionSettings

    mod = _load_reinject_module()

    def _boom(*_args, **_kwargs) -> str:
        raise RuntimeError("simulated telemetry write failure")

    monkeypatch.setattr(compaction_mod, "record_session_reinjection", _boom)

    rc = mod._reinject(
        repo_root=str(workspace),
        budget_chars=1500,
        settings=CompactionSettings(),
        session_id="sess-telemetry-fail",
        source="compact",
    )
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() != ""
    assert "reinject telemetry write failed" in captured.err


def test_reinject_skips_on_startup_source_by_default(workspace: Path) -> None:
    """Default source gate excludes startup so ordinary session starts are
    not taxed next to load_session guidance.
    """
    result = _run_hook(_payload("startup"), workspace=workspace)

    assert result.returncode == 0
    assert result.stdout == "", f"gated source must emit nothing; stdout={result.stdout!r}"
    assert "reinject skipped: source" in result.stderr, result.stderr


def test_reinject_sources_env_override(workspace: Path) -> None:
    """WORKBAY_REINJECT_SOURCES extends the gate (comma list)."""
    result = _run_hook(
        _payload("startup"),
        workspace=workspace,
        extra_env={"WORKBAY_REINJECT_SOURCES": "startup,compact"},
    )

    assert result.returncode == 0, result.stderr
    block = _stdout_injection(result)
    assert block.startswith("```workbay-reinject"), f"startup must emit once allowlisted; stderr={result.stderr!r}"


def test_reinject_budget_truncation(workspace: Path) -> None:
    """WORKBAY_REINJECT_BUDGET_CHARS caps total stdout chars while keeping
    the fence closed.
    """
    _seed_compaction_row(workspace)
    budget = 200
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={
            "WORKBAY_REINJECT_BUDGET_CHARS": str(budget),
            "WORKBAY_HANDOFF_COMPACTION_NOTIFY": "0",
        },
    )

    assert result.returncode == 0
    assert result.stdout, "block must still be emitted under a small budget"
    assert len(result.stdout) <= budget, f"stdout must fit the {budget}-char budget; got {len(result.stdout)}"
    assert result.stdout.startswith("```workbay-reinject")
    assert result.stdout.rstrip().endswith("```"), f"truncation must keep the fence closed; stdout={result.stdout!r}"


def test_reinject_budget_below_task_ref_floor_skips(workspace: Path) -> None:
    """A budget too small to fit fences + the mandatory task_ref line emits
    NOTHING (no contentless fence pair) and exits 0.
    """
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={"WORKBAY_REINJECT_BUDGET_CHARS": "10"},
    )

    assert result.returncode == 0
    assert result.stdout == "", f"sub-floor budget must emit nothing; stdout={result.stdout!r}"
    assert "reinject skipped: budget" in result.stderr, result.stderr


def test_reinject_minimal_budget_always_carries_task_ref(workspace: Path) -> None:
    """The smallest emitting budget still carries the task_ref line — the
    block is never an empty fence pair.
    """
    floor = len("\n".join(["```workbay-reinject", f"task_ref: {TASK_REF}", "```"])) + 1
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={
            "WORKBAY_REINJECT_BUDGET_CHARS": str(floor),
            "WORKBAY_HANDOFF_COMPACTION_NOTIFY": "0",
        },
    )

    assert result.returncode == 0, result.stderr
    assert len(result.stdout) <= floor
    content = [line for line in result.stdout.splitlines() if line not in ("```workbay-reinject", "```")]
    assert content == [f"task_ref: {TASK_REF}"], (
        f"floor-budget block must carry exactly the task_ref line; stdout={result.stdout!r}"
    )


def test_reinject_sanitizes_fence_tokens_in_field_values(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Agent-authored field values containing ``` or newlines must not close
    the injected fence early: exactly one closing fence, as the final line.
    """
    from workbay_handoff_mcp import get_handoff_state, set_handoff_state

    # The tmp workspace has no real worktree for the seeded target_branch;
    # skip derivation (standard test bypass) so the focus update lands.
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
    revision = identity["data"]["active"]["revision"]
    update = set_handoff_state(
        task_ref=TASK_REF,
        focus="evil\n```\ninjected fence line",
        status="in_progress",
        expected_revision=revision,
    )
    assert update.get("ok"), f"focus update must land: {update!r}"

    result = _run_hook(_payload("compact"), workspace=workspace)

    assert result.returncode == 0, result.stderr
    block_lines = _stdout_injection(result).rstrip("\n").splitlines()
    assert block_lines[0] == "```workbay-reinject"
    assert block_lines[-1] == "```"
    interior = block_lines[1:-1]
    assert all(not line.startswith("```") for line in interior), (
        f"sanitized block must not contain an interior fence; stdout={result.stdout!r}"
    )
    focus_lines = [line for line in interior if line.startswith("focus: ")]
    assert focus_lines == ["focus: evil `` injected fence line"], (
        f"focus must be flattened + fence-token-stripped; interior={interior!r}"
    )


def test_reinject_invalid_budget_skips(workspace: Path) -> None:
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={"WORKBAY_REINJECT_BUDGET_CHARS": "abc"},
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert "reinject skipped: invalid budget" in result.stderr, result.stderr


def test_reinject_no_active_task_skips(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero handoff_state rows: hook must skip cleanly, never block the
    session start.
    """
    saved_path, saved_modules = _prepare_source_imports()
    try:
        state_dir = tmp_path / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", str(state_dir))

        from workbay_handoff_mcp import RuntimeConfig, configure_runtime

        configure_runtime(
            RuntimeConfig.for_workspace(
                tmp_path,
                state_dir=state_dir,
                current_task_path=tmp_path / "CURRENT_TASK.json",
            )
        )

        result = _run_hook(_payload("compact"), workspace=tmp_path)

        assert result.returncode == 0
        assert result.stdout == ""
        assert "reinject skipped: active task unresolved" in result.stderr, result.stderr
    finally:
        _restore_source_imports(saved_path, saved_modules)


def test_reinject_disable_resolver_silences(workspace: Path) -> None:
    """A disabled compaction surface (internal unified resolver) also
    silences re-injection.
    """
    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_COMPACTION_DISABLED": "1"},
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert "reinject skipped: disabled" in result.stderr, result.stderr


def test_reinject_db_unreachable_is_non_fatal(workspace: Path, tmp_path: Path) -> None:
    """A bogus state-dir surfaces as ``reinject skipped:`` on stderr and
    exit 0 — never blocks the session start.
    """
    bogus_parent = tmp_path / "blocker-file"
    bogus_parent.write_text("not a directory")
    bogus_state = bogus_parent / ".task-state"

    result = _run_hook(
        _payload("compact"),
        workspace=workspace,
        extra_env={"WORKBAY_HANDOFF_STATE_DIR": str(bogus_state)},
    )

    assert result.returncode == 0, f"hook exited {result.returncode}; stderr={result.stderr!r}"
    assert result.stdout == ""
    assert "reinject skipped:" in result.stderr, result.stderr


def test_reinject_strict_mode_protocol_drift_exits_2(workspace: Path) -> None:
    """WORKBAY_HOOK_PROTOCOL_STRICT=1 plus a wrong-event payload propagates
    SystemExit(2), matching every other wired hook.
    """
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-strict",
        "source": "compact",
    }
    result = _run_hook(
        payload,
        workspace=workspace,
        extra_env={"WORKBAY_HOOK_PROTOCOL_STRICT": "1"},
    )

    assert result.returncode == 2, f"strict protocol drift must exit 2; rc={result.returncode} stderr={result.stderr!r}"
    assert result.stdout == ""


def test_reinject_malformed_stdin_skips(workspace: Path) -> None:
    env = os.environ.copy()
    env["CLAUDE_PROJECT_DIR"] = str(workspace)
    env["WORKBAY_HANDOFF_STATE_DIR"] = str(workspace / ".task-state")
    parts = [str(HANDOFF_SRC), str(PROTOCOL_SRC)]
    if env.get("PYTHONPATH"):
        parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(parts)
    result = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="not json {",
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        cwd=str(workspace),
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert "reinject skipped: malformed stdin payload" in result.stderr


# ---------------------------------------------------------------------------
# implementation note — ambiguous workspace tiebreak + env pin (mirrors compact-session)
# ---------------------------------------------------------------------------


@pytest.fixture()
def workspace_ambiguous_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
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
        import time

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
        yield tmp_path
    finally:
        _restore_source_imports(saved_path, saved_modules)


def test_reinject_ambiguous_shape_tiebreaks_to_most_recent(
    workspace_ambiguous_tasks: Path,
) -> None:
    result = _run_hook(_payload("compact"), workspace=workspace_ambiguous_tasks)

    assert result.returncode == 0, result.stderr
    assert "ambiguous active task: chose AMBIG-B (most recent)" in result.stderr
    assert "AMBIG-A" in result.stderr
    block = _stdout_injection(result)
    assert block.startswith("```workbay-reinject")
    assert "AMBIG-B" in block
    assert "reinject skipped: active task unresolved" not in result.stderr


def test_reinject_ambiguous_shape_pinned_task_ref(
    workspace_ambiguous_tasks: Path,
) -> None:
    result = _run_hook(
        _payload("compact"),
        workspace=workspace_ambiguous_tasks,
        extra_env={"WORKBAY_HANDOFF_ACTIVE_TASK": "AMBIG-A"},
    )

    assert result.returncode == 0, result.stderr
    assert "ambiguous active task:" not in result.stderr
    block = _stdout_injection(result)
    assert block.startswith("```workbay-reinject")
    assert "AMBIG-A" in block


# ---------------------------------------------------------------------------
# Contract block — harness-protocol.yaml `reinjection:` section (implementation note
# implementation note). Text-level assertions, matching the doc-test pattern in
# test_dev_workflow_compaction_docs.py (the payload test venv does not
# declare a YAML parser dependency).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# internal — semantic top-K concept reinjection (WORKBAY_REINJECT_SEMANTIC, default off)
# ---------------------------------------------------------------------------


class _FakeProvider:
    """Deterministic one-hot provider (no ONNX artifact); in-process injection."""

    def __init__(self, dim: int = 768, model_id: str = "gte-base-en-v1.5") -> None:
        self._dim = dim
        self._model_id = model_id

    @property
    def dim(self) -> int:
        return self._dim

    @property
    def model_id(self) -> str:
        return self._model_id

    def embed(self, texts):
        import numpy as np
        from workbay_handoff_mcp.embeddings.store import text_hash

        out = np.zeros((len(texts), self._dim), dtype=np.float32)
        for i, text in enumerate(texts):
            out[i, int(text_hash(text), 16) % self._dim] = 1.0
        return out


def _block_content_lines(stdout: str) -> list[str]:
    return [line for line in stdout.splitlines() if line not in ("```workbay-reinject", "```")]


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


def test_reinject_semantic_emits_on_compact_with_new_compaction_id(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """compact + unseen compaction_id: semantic block emitted (mocked packet)."""
    from workbay_handoff_mcp.compaction import CompactionSettings

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="compact-new",
        source="compact",
    )
    captured = capsys.readouterr()
    assert "relevant:" in captured.out
    assert "mock semantic packet marker" in captured.out
    assert "reinject semantic skipped:" not in captured.err


def test_reinject_semantic_skips_repeat_compaction_id(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """compact + existing (task_ref, compaction_id) row: semantic skipped, generic emitted."""
    from workbay_handoff_mcp.compaction import (
        CompactionSettings,
        record_session_reinjection,
    )
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    compaction_id = _seed_compaction_row(workspace)
    with _get_db_connection() as conn:
        record_session_reinjection(
            conn,
            session_id="prior-compact",
            harness="codex",
            task_ref=TASK_REF,
            compaction_id=compaction_id,
            source="compact",
            emitted_chars=100,
            semantic_detail={
                "status": "selected",
                "skip_reason": None,
                "selected": [{"kind": "decision", "id": "prior-1"}],
                "chars_used": 100,
            },
        )
        conn.commit()

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="compact-repeat",
        source="compact",
    )
    captured = capsys.readouterr()
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert f"reinject semantic skipped: already reinjected for compaction_id={compaction_id}" in captured.err


def test_reinject_semantic_skipped_on_resume_generic_still_emitted(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """resume: semantic skipped by inner compact gate; generic block still emitted."""
    from workbay_handoff_mcp.compaction import CompactionSettings

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="resume-sem-skip",
        source="resume",
    )
    captured = capsys.readouterr()
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert "reinject semantic skipped: requires source=compact (got source=resume)" in captured.err


def test_reinject_semantic_skipped_on_startup(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """startup (allowlisted): semantic skipped; generic block still emitted."""
    from workbay_handoff_mcp.compaction import CompactionSettings

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="startup-sem-skip",
        source="startup",
    )
    captured = capsys.readouterr()
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert "reinject semantic skipped: requires source=compact (got source=startup)" in captured.err


def test_reinject_semantic_skipped_without_compaction_id(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """compact + semantic on but no compaction_id: skip semantic (no dedupe key)."""
    from workbay_handoff_mcp.compaction import CompactionSettings

    # Intentionally do NOT seed a compaction row.
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="compact-no-id",
        source="compact",
    )
    captured = capsys.readouterr()
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert "reinject semantic skipped: requires compaction_id on compact source" in captured.err


def test_reinject_semantic_not_suppressed_by_prior_resume_row(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Generic resume telemetry for compaction_id must not suppress later compact semantic."""
    from workbay_handoff_mcp.compaction import (
        CompactionSettings,
        record_session_reinjection,
    )
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    compaction_id = _seed_compaction_row(workspace)
    with _get_db_connection() as conn:
        record_session_reinjection(
            conn,
            session_id="prior-resume",
            harness="codex",
            task_ref=TASK_REF,
            compaction_id=compaction_id,
            source="resume",
            emitted_chars=40,
            # intentionally no semantic_detail
        )
        conn.commit()

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="compact-after-resume",
        source="compact",
    )
    captured = capsys.readouterr()
    assert "relevant:" in captured.out
    assert "mock semantic packet marker" in captured.out
    assert "reinject semantic skipped: already reinjected" not in captured.err


def test_reinject_semantic_off_is_byte_identical(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Flag off, and flag-on-without-a-provider, both emit the baseline block."""
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    mod = _load_reinject_module()
    embedding_store.set_provider_for_testing(None)
    try:
        monkeypatch.delenv("WORKBAY_REINJECT_SEMANTIC", raising=False)
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=2000,
            settings=CompactionSettings(),
            session_id="sem-off-a",
            source="compact",
        )
        baseline = capsys.readouterr().out
        monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=2000,
            settings=CompactionSettings(),
            session_id="sem-off-b",
            source="compact",
        )
        degraded = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    assert "relevant:" not in baseline
    assert "relevant:" not in degraded  # flag on but provider absent -> degrade
    assert _block_content_lines(baseline) == _block_content_lines(degraded)


def _seed_semantic_ranking_fixture(
    workspace: Path,
    provider: _FakeProvider,
    *,
    concept_text: str,
    concept_id: str = "777",
    noise_id: str = "999",
    decision_label: str | None = None,
) -> None:
    """Seed compaction anchor + decision source row + ranked/noise embeddings."""
    from workbay_handoff_mcp.embeddings.store import (
        serialize_vector,
        store_concept_embedding,
    )
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    compaction_id = _seed_compaction_row(workspace)
    anchor = provider.embed([concept_text])[0]
    label = decision_label if decision_label is not None else f"dec-{concept_id}"
    with _get_db_connection() as conn:
        conn.execute(
            "UPDATE session_compactions SET anchor_vector = ? WHERE compaction_id = ?",
            (serialize_vector(anchor), compaction_id),
        )
        conn.execute(
            """
            INSERT INTO decisions (id, task_ref, session, decision, rationale, agent, changed_files_json, created_at)
            VALUES (?, ?, 'sess', ?, ?, 'agent', '[]', datetime('now'))
            """,
            (concept_id, TASK_REF, label, concept_text),
        )
        store_concept_embedding(conn, provider, "decision.rationale", concept_id, TASK_REF, concept_text)
        store_concept_embedding(
            conn,
            provider,
            "finding.description",
            noise_id,
            TASK_REF,
            "utterly unrelated noise",
        )
        conn.commit()


def test_reinject_semantic_emits_ranked_concepts(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Flag on + provider + seeded embeddings: block carries readable ``relevant:`` snippets."""
    from workbay_handoff_mcp import get_handoff_state, set_handoff_state
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        concept_text = "semantic target focus marker xyzzy"
        identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
        revision = identity["data"]["active"]["revision"]
        assert set_handoff_state(
            task_ref=TASK_REF,
            focus="visible operator focus",
            status="in_progress",
            expected_revision=revision,
        ).get("ok")
        _seed_semantic_ranking_fixture(workspace, provider, concept_text=concept_text)

        mod._reinject(
            repo_root=str(workspace),
            budget_chars=4000,
            settings=CompactionSettings(),
            session_id="sem-emit",
            source="compact",
        )
        out = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    assert "relevant:" in out, f"expected readable semantic block; stdout={out!r}"
    assert concept_text in out
    assert "[decision:777]" in out
    assert "decision.rationale:777" not in out
    assert "utterly unrelated noise" not in out


def test_reinject_semantic_provider_present_no_embeddings_degrades(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Flag on + provider present but ZERO matching embeddings (cold start) ⇒
    ranked == [] ⇒ no `relevant:` line ⇒ block byte-identical to baseline."""
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    mod = _load_reinject_module()
    try:
        embedding_store.set_provider_for_testing(None)
        monkeypatch.delenv("WORKBAY_REINJECT_SEMANTIC", raising=False)
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=2000,
            settings=CompactionSettings(),
            session_id="ne-off",
            source="compact",
        )
        baseline = capsys.readouterr().out
        monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
        embedding_store.set_provider_for_testing(_FakeProvider())  # provider present, no embeddings seeded
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=2000,
            settings=CompactionSettings(),
            session_id="ne-on",
            source="compact",
        )
        degraded = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    assert "relevant:" not in degraded
    assert _block_content_lines(baseline) == _block_content_lines(degraded)


def test_reinject_semantic_line_respects_byte_budget(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The `relevant:` line is budget-bounded: present under a generous budget,
    dropped (fence still closed, budget honored) under a tight one."""
    from workbay_handoff_mcp import get_handoff_state, set_handoff_state
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        focus_text = "budget focus marker"
        identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
        set_handoff_state(
            task_ref=TASK_REF,
            focus="visible operator focus",
            status="in_progress",
            expected_revision=identity["data"]["active"]["revision"],
        )
        _seed_semantic_ranking_fixture(
            workspace,
            provider,
            concept_text=focus_text,
        )
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=4000,
            settings=CompactionSettings(),
            session_id="bud-big",
            source="compact",
        )
        big = capsys.readouterr().out
        floor = len("\n".join(["```workbay-reinject", f"task_ref: {TASK_REF}", "```"])) + 1
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=floor + 15,
            settings=CompactionSettings(),
            session_id="bud-small",
            source="compact",
        )
        small = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    assert "relevant:" in big
    assert "relevant:" not in small
    assert small.startswith("```workbay-reinject") and small.rstrip().endswith("```")
    assert len(small) <= floor + 15


def test_reinject_semantic_enabled_selects_top_k_with_null_arm(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """WORKBAY_REINJECT_SEMANTIC=1 on compact selects semantic top-K directly
    (no A/B arm branch) and persists telemetry with arm=NULL."""
    from workbay_handoff_mcp import get_handoff_state, set_handoff_state
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        concept_text = "semantic unconditional focus marker abcxyz"
        identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
        set_handoff_state(
            task_ref=TASK_REF,
            focus="visible operator focus",
            status="in_progress",
            expected_revision=identity["data"]["active"]["revision"],
        )
        _seed_semantic_ranking_fixture(workspace, provider, concept_text=concept_text)
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=4000,
            settings=CompactionSettings(),
            session_id="semantic-null-arm",
            source="compact",
        )
        out = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    assert "relevant:" in out
    assert concept_text in out
    telemetry = _latest_reinjection_row()
    assert telemetry is not None
    assert telemetry["session_id"] == "semantic-null-arm"
    assert telemetry["arm"] is None


def test_reinject_semantic_fail_open_preserves_base_block(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    embedding_store.set_provider_for_testing(_FakeProvider())
    mod = _load_reinject_module()
    try:

        def _boom(**kwargs):
            raise RuntimeError("forced semantic failure")

        monkeypatch.setattr(mod, "_readable_semantic_lines", _boom)
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=2000,
            settings=CompactionSettings(),
            session_id="fail-open",
            source="compact",
        )
        out = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    assert out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in out

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT semantic_detail_json FROM session_reinjections WHERE session_id = ?",
            ("fail-open",),
        ).fetchone()
    assert row is not None
    payload = json.loads(row["semantic_detail_json"])
    assert payload["status"] == "degraded"
    assert payload["skip_reason"] == "error"


def test_reinject_semantic_embeddings_import_error_degrades_with_telemetry(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """implementation note S1 [OBS-08]: embeddings ImportError must emit + land degraded row.

    Predict: stderr names degrade + provision-embeddings remedy; exit path still
    emits generic block; semantic_detail status=degraded skip=deps_or_model_missing.
    """
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")

    class _BlockEmbeddings:
        def find_spec(self, fullname, path, target=None):  # noqa: ANN001
            if fullname == "workbay_handoff_mcp.embeddings" or fullname.startswith("workbay_handoff_mcp.embeddings."):
                raise ImportError("forced: embeddings extra missing")
            return None

    finder = _BlockEmbeddings()
    purged = [
        key
        for key in list(sys.modules)
        if key == "workbay_handoff_mcp.embeddings" or key.startswith("workbay_handoff_mcp.embeddings.")
    ]
    for key in purged:
        del sys.modules[key]
    sys.meta_path.insert(0, finder)
    try:
        mod = _load_reinject_module()
        exit_code = mod._reinject(
            repo_root=str(workspace),
            budget_chars=4000,
            settings=CompactionSettings(),
            session_id="deps-missing-degrade",
            source="compact",
        )
        captured = capsys.readouterr()
    finally:
        if finder in sys.meta_path:
            sys.meta_path.remove(finder)

    assert exit_code == 0
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert "reinject semantic degraded: embeddings unavailable" in captured.err
    assert "workbay-bootstrap provision-embeddings" in captured.err

    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT semantic_detail_json FROM session_reinjections WHERE session_id = ?",
            ("deps-missing-degrade",),
        ).fetchone()
    assert row is not None
    assert row["semantic_detail_json"] is not None
    detail = json.loads(row["semantic_detail_json"])
    assert detail["status"] == "degraded"
    assert detail["skip_reason"] == "deps_or_model_missing"


def test_reinject_semantic_dedupe_helper_import_error_suppresses(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """implementation note S4 [REF-20]: sticky helper ImportError suppresses semantic (no dup).

    Predict: no relevant: block; stderr names suppress + dedupe_unavailable;
    telemetry skip_reason=dedupe_unavailable.
    """
    import workbay_handoff_mcp.compaction as compaction_mod
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    # from X import session_reinjection_exists → ImportError when attr missing
    monkeypatch.delattr(compaction_mod, "session_reinjection_exists")

    mod = _load_reinject_module()
    semantic_calls: list[int] = []

    def _should_not_run(**kwargs):  # noqa: ANN003
        semantic_calls.append(1)
        return _mock_readable_semantic_lines(**kwargs)

    monkeypatch.setattr(mod, "_readable_semantic_lines", _should_not_run)
    exit_code = mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="dedupe-unavailable",
        source="compact",
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out.startswith("```workbay-reinject")
    assert f"task_ref: {TASK_REF}" in captured.out
    assert "relevant:" not in captured.out
    assert semantic_calls == []
    assert "reinject semantic suppressed: sticky-check helper unavailable" in captured.err
    assert "skip=dedupe_unavailable" in captured.err
    assert "sticky-check failed (fail-open)" not in captured.err

    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT semantic_detail_json FROM session_reinjections WHERE session_id = ?",
            ("dedupe-unavailable",),
        ).fetchone()
    assert row is not None
    assert row["semantic_detail_json"] is not None
    detail = json.loads(row["semantic_detail_json"])
    assert detail["status"] == "skipped"
    assert detail["skip_reason"] == "dedupe_unavailable"


@pytest.mark.parametrize("outcome", ["unconfigured", "no_embeddings", "error", "raised_error"])
def test_sembudget_r2_hook_replaces_base_context_before_semantic_retrieval_succeeds_01(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    outcome: str,
) -> None:
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings.reinjection import SemanticReinjectionResult

    _seed_compaction_row(workspace)
    focus = "f" * 1300
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    _set_focus(focus)
    mod = _load_reinject_module()
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "0")
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=1500,
        settings=CompactionSettings(),
        session_id="generic-baseline",
        source="compact",
    )
    baseline = capsys.readouterr().out
    assert f"focus: {focus}" in baseline

    calls = []

    def empty_retrieval(**kwargs):
        calls.append(kwargs)
        if outcome == "raised_error":
            raise RuntimeError("forced retrieval failure")
        return [], SemanticReinjectionResult(
            status="degraded" if outcome == "error" else "skipped",
            skip_reason=outcome,
            model_id=None,
        )

    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setattr(mod, "_readable_semantic_lines", empty_retrieval)
    mod._reinject(
        repo_root=str(workspace),
        budget_chars=1500,
        settings=CompactionSettings(),
        session_id=f"empty-retrieval-{outcome}",
        source="compact",
    )
    captured = capsys.readouterr()
    assert len(calls) == 1
    assert captured.out == baseline
    assert len(captured.out) <= 1500


@pytest.mark.parametrize("focus_chars", [1273, 1274, 1275])
def test_f1_exact_boundary_reservation_delivers_selected_bullet(
    focus_chars: int,
) -> None:
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings.reinjection import (
        SelectedConcept,
        one_result_floor_chars,
        render_readable_relevant_lines,
    )

    mod = _load_reinject_module()
    base_lines = ["task_ref: T", "focus: " + "f" * focus_chars]
    allocation = mod._compute_semantic_content_budget(
        budget_chars=1500,
        harness="codex",
        settings=CompactionSettings(),
        base_lines=base_lines,
        recover_hint=mod._RECOVER_HINT,
        notify_allowance_chars=220,
        provisional_notify="",
    )
    relevant_lines = render_readable_relevant_lines([SelectedConcept("decision", "777", "L" * 48, "x" * 120, 1.0, 182)])
    assert len("\n".join(relevant_lines)) == one_result_floor_chars(120)
    assert allocation.skip_reason is None
    assert allocation.granted >= len("\n".join(relevant_lines))
    kept = allocation.kept_base_lines
    block = mod._render_block(
        [*(base_lines if kept is None else kept), *relevant_lines, mod._RECOVER_HINT],
        budget_chars=1500,
    )
    assert block is not None
    assert relevant_lines[1] in block
    assert len(block) + 1 <= 1500


def test_f2_long_reference_fits_advertised_one_result_floor() -> None:
    import numpy as np
    from workbay_handoff_mcp.embeddings.ranking import ScoredConceptVector
    from workbay_handoff_mcp.embeddings.reinjection import (
        ReinjectionConfig,
        ResolvedConceptSource,
        one_result_floor_chars,
        render_readable_relevant_lines,
        select_concepts,
    )

    key = ("compaction.prose_residual", "c" * 180)
    budget = one_result_floor_chars(40)
    selected = select_concepts(
        [ScoredConceptVector(*key, 1.0, np.array([1.0]))],
        resolved={key: ResolvedConceptSource(*key, "source prose", "L" * 48)},
        seed_vectors=[],
        config=ReinjectionConfig(snippet_chars=40),
        chars_budget=budget,
        provider=_FakeProvider(),
    )
    assert len(selected) == 1
    assert selected[0].id == key[1]
    assert len(selected[0].snippet) <= 40
    assert selected[0].snippet.endswith("…]")
    assert len("\n".join(render_readable_relevant_lines(selected))) <= budget


def test_f3_oversized_label_keeps_serialized_packet_bounded() -> None:
    import numpy as np
    from workbay_handoff_mcp.embeddings.ranking import ScoredConceptVector
    from workbay_handoff_mcp.embeddings.reinjection import (
        ReinjectionConfig,
        ResolvedConceptSource,
        SemanticReinjectionResult,
        render_readable_relevant_lines,
        select_concepts,
    )

    key = ("decision.rationale", "777")
    budget = 1000
    selected = select_concepts(
        [ScoredConceptVector(*key, 1.0, np.array([1.0]))],
        resolved={key: ResolvedConceptSource(*key, "source prose", "L" * 1_000_000)},
        seed_vectors=[],
        config=ReinjectionConfig(),
        chars_budget=budget,
        provider=_FakeProvider(),
    )
    assert len(selected) == 1
    result = SemanticReinjectionResult(
        status="selected",
        skip_reason=None,
        model_id="test",
        selected=selected,
        chars_used=sum(item.emitted_chars for item in selected),
        chars_budget=budget,
    )
    assert len(json.dumps(result.to_dict())) < 4 * budget
    assert len(selected[0].label) == 48
    assert selected[0].id == "777"
    assert "[decision:777]" in selected[0].snippet
    assert result.chars_used == len("\n".join(render_readable_relevant_lines(selected)))


def test_reinject_semantic_dedupe_transient_error_fail_open(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """implementation note S4: non-ImportError sticky failure keeps fail-open (semantic runs)."""
    import workbay_handoff_mcp.compaction as compaction_mod
    from workbay_handoff_mcp.compaction import CompactionSettings

    _seed_compaction_row(workspace)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")

    def _boom_sticky(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("forced sticky db hiccup")

    monkeypatch.setattr(compaction_mod, "session_reinjection_exists", _boom_sticky)
    mod = _load_reinject_module()
    monkeypatch.setattr(mod, "_readable_semantic_lines", _mock_readable_semantic_lines)
    exit_code = mod._reinject(
        repo_root=str(workspace),
        budget_chars=4000,
        settings=CompactionSettings(),
        session_id="dedupe-fail-open",
        source="compact",
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "relevant:" in captured.out
    assert "mock semantic packet marker" in captured.out
    assert "reinject sticky-check failed (fail-open): forced sticky db hiccup" in captured.err
    assert "dedupe_unavailable" not in captured.err
    assert "reinject semantic suppressed:" not in captured.err


def test_reinject_notify_renders_semantic_band_and_budget(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from workbay_handoff_mcp import get_handoff_state, set_handoff_state
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "claude-code")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        concept_text = "notify semantic marker text"
        identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
        set_handoff_state(
            task_ref=TASK_REF,
            focus="visible operator focus",
            status="in_progress",
            expected_revision=identity["data"]["active"]["revision"],
        )
        _seed_semantic_ranking_fixture(workspace, provider, concept_text=concept_text)
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=1500,
            settings=CompactionSettings(compaction_notify=True),
            session_id="notify-sem",
            source="compact",
        )
        out = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    envelope = json.loads(out)
    notify = envelope["systemMessage"]
    assert "rel~" in notify
    assert "chars=" in notify
    assert len(notify) <= 220


def test_reinject_semantic_persists_semantic_detail_json(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import json as json_mod

    from workbay_handoff_mcp import get_handoff_state, set_handoff_state
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        concept_text = "telemetry semantic marker"
        identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
        set_handoff_state(
            task_ref=TASK_REF,
            focus="visible operator focus",
            status="in_progress",
            expected_revision=identity["data"]["active"]["revision"],
        )
        _seed_semantic_ranking_fixture(workspace, provider, concept_text=concept_text)
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=4000,
            settings=CompactionSettings(),
            session_id="telemetry-sem",
            source="compact",
        )
        capsys.readouterr()
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT semantic_detail_json FROM session_reinjections WHERE session_id = ?",
                ("telemetry-sem",),
            ).fetchone()
    finally:
        embedding_store.reset_provider_cache()

    assert row is not None
    assert row["semantic_detail_json"] is not None
    payload = json_mod.loads(row["semantic_detail_json"])
    assert payload["status"] in {"selected", "skipped", "degraded"}


@pytest.fixture(scope="module")
def contract_text() -> str:
    return CONTRACT_PATH.read_text(encoding="utf-8")


def test_contract_has_reinjection_block(contract_text: str) -> None:
    assert "\nreinjection:" in contract_text, (
        "harness-protocol.yaml must declare a top-level `reinjection:` block "
        "as the single documented source for the hook's tunables (implementation note)."
    )


@pytest.mark.parametrize(
    "marker",
    [
        "WORKBAY_REINJECT_SOURCES",
        "WORKBAY_REINJECT_BUDGET_CHARS",
        "WORKBAY_HANDOFF_ACTIVE_TASK",
        "session_reinjections",
        "WORKBAY_HANDOFF_COMPACTION_NOTIFY",
        "budget_chars: 1500",
        "- compact",
        "- resume",
        "reinject-context.py",
        "reservation_capped",
        "SkipReason",
    ],
)
def test_contract_documents_reinjection_tunables(contract_text: str, marker: str) -> None:
    # Bound the slice at the next top-level key so markers that only appear
    # in later sections (e.g. `orchestrator:`) cannot satisfy the assertion.
    tail = contract_text.split("\nreinjection:", 1)[-1]
    boundary = re.search(r"\n[A-Za-z_][A-Za-z0-9_-]*:", tail)
    reinjection_block = tail[: boundary.start()] if boundary else tail
    assert marker in reinjection_block, f"`reinjection:` contract block must mention {marker!r}"


def test_resolve_harness_honors_legacy_workbay_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A5 §8 proof (implementation note bridge #1 / REVD-2): after the WORKBAY_* rename
    sweep a pre-rename operator's ``WORKBAY_HANDOFF_HARNESS`` export is still
    honored mid-session via the ``_interp`` resolve_env_alias bridge, and the
    canonical ``WORKBAY_*`` still wins when both are set."""
    mod = _load_reinject_module()
    monkeypatch.delenv("WORKBAY_HANDOFF_HARNESS", raising=False)
    monkeypatch.delenv("GROK_WORKSPACE_ROOT", raising=False)
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    assert mod._resolve_harness() == "codex"

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "grok")
    assert mod._resolve_harness() == "grok"


def test_semantic_activation_advisory_when_model_set(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """implementation note S4: advisory when embedding model is set but semantic mode is off."""
    mod = _load_reinject_module()
    mod._SEMANTIC_ADVISORY_EMITTED = False
    monkeypatch.delenv("WORKBAY_REINJECT_SEMANTIC", raising=False)
    monkeypatch.setenv("WORKBAY_HANDOFF_EMBEDDING_MODEL", "/models/test.onnx")
    mod._maybe_emit_semantic_activation_advisory()
    captured = capsys.readouterr()
    assert "[reinject] semantic mode available but inactive" in captured.err


def test_semantic_activation_advisory_suppressed_when_enabled(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    mod = _load_reinject_module()
    mod._SEMANTIC_ADVISORY_EMITTED = False
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_EMBEDDING_MODEL", "/models/test.onnx")
    mod._maybe_emit_semantic_activation_advisory()
    captured = capsys.readouterr()
    assert captured.err == ""


def test_selected_semantic_delivered_rejects_numeric_id_false_positive():
    """Numeric decision ids must not match latest_compaction turn ranges (h3 S2-BR-01)."""
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path
    from types import SimpleNamespace

    path = Path(__file__).resolve().parent / "reinject-context.py"
    spec = spec_from_file_location("reinject_context_under_test_delivery", path)
    assert spec and spec.loader
    mod = module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception:
        raise
    block = "task_ref: T\nlatest_compaction: C-1 (turns 1-12)\nrelevant:\n"
    result = SimpleNamespace(
        status="selected",
        selected=[SimpleNamespace(id="1", kind="decision", label="foo", snippet="bar")],
    )
    assert mod._selected_semantic_delivered(block, result) is False

    block_ok = "task_ref: T\nlatest_compaction: C-1 (turns 1-12)\nrelevant:\n- foo: body [decision:1]\n"
    assert mod._selected_semantic_delivered(block_ok, result) is True


def test_selected_semantic_delivered_accepts_bullet_label():
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path
    from types import SimpleNamespace

    path = Path(__file__).resolve().parent / "reinject-context.py"
    spec = spec_from_file_location("reinject_context_under_test_delivery2", path)
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    result = SimpleNamespace(
        status="selected",
        selected=[SimpleNamespace(id="4956", kind="finding", label="h2-S4-A-01", snippet="x")],
    )
    block = "relevant:\n- h2-S4-A-01: Add WORKBAY_REINJECT_SEMANTIC\n"
    assert mod._selected_semantic_delivered(block, result) is True


def _semantic_section_text(block: str) -> str:
    """Return the ``relevant:`` section, excluding recover/fence lines."""
    if "relevant:" not in block:
        return ""
    start = block.index("relevant:")
    kept: list[str] = []
    for line in block[start:].splitlines():
        if line.startswith("recover:") or line == "```":
            break
        kept.append(line)
    return "\n".join(kept)


def _set_focus(focus: str) -> None:
    from workbay_handoff_mcp import get_handoff_state, set_handoff_state

    identity = get_handoff_state(task_ref=TASK_REF, sections="identity")
    assert set_handoff_state(
        task_ref=TASK_REF,
        focus=focus,
        status="in_progress",
        expected_revision=identity["data"]["active"]["revision"],
    ).get("ok")


def _clear_session_reinjections() -> None:
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        conn.execute("DELETE FROM session_reinjections")
        conn.commit()


# Measured on the unpatched hook with budget_chars=1500, harness=codex, the
# short-label fixture (`dec-777` + 29-char body). Healthy focus lengths emit
# this relevant section; the former dead band emits nothing.
_HEALTHY_SHOULDER_SECTION = "relevant:\n- dec-777: healthy-shoulder-marker-xyzzy [decision:777]"
_BASE_SEMANTIC_SECTION_CHARS = {
    0: len(_HEALTHY_SHOULDER_SECTION),
    300: len(_HEALTHY_SHOULDER_SECTION),
    900: len(_HEALTHY_SHOULDER_SECTION),
    1000: len(_HEALTHY_SHOULDER_SECTION),
    1500: len(_HEALTHY_SHOULDER_SECTION),
}
_DEAD_BAND_FOCUS_LENS = (1250, 1400)
_LONG_LABEL = "L" * 155
_FULL_SNIPPET_BODY = ("word " * 40).strip()


def test_skip_reason_literal_includes_reservation_capped() -> None:
    from typing import get_args

    from workbay_handoff_mcp.embeddings.reinjection import SkipReason

    assert "reservation_capped" in get_args(SkipReason)


def test_readable_relevant_lines_caps_155_char_label_keeps_entity_ref() -> None:
    """A 155-char label is shortened; the snippet's [kind:id] stays intact.

    Fails on base because the renderer emits the unbounded label in full.
    """
    from workbay_handoff_mcp.embeddings.reinjection import (
        ELLIPSIS_MARKER,
        RENDERED_LABEL_CAP,
        SelectedConcept,
        render_readable_relevant_lines,
    )

    snippet = f"{_FULL_SNIPPET_BODY} [finding:19962]"
    lines = render_readable_relevant_lines(
        [
            SelectedConcept(
                kind="finding",
                id="19962",
                label=_LONG_LABEL,
                snippet=snippet,
                score=0.9,
                emitted_chars=1,
            )
        ]
    )
    assert lines[0] == "relevant:"
    bullet = lines[1]
    assert bullet.startswith("- ")
    label, _, rest = bullet[2:].partition(": ")
    assert len(label) == RENDERED_LABEL_CAP
    assert label.endswith(ELLIPSIS_MARKER)
    assert _LONG_LABEL not in bullet
    assert "[finding:19962]" in rest
    assert snippet in bullet


def test_155_char_label_result_renders_in_former_dead_band(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A 155-char label + full snippet must appear under a focus length that
    previously starved the arm. Fails on base: leftover cannot hold the
    unbounded line, so the block has no relevant: section.
    """
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store
    from workbay_handoff_mcp.embeddings.reinjection import (
        ELLIPSIS_MARKER,
        RENDERED_LABEL_CAP,
        render_concept_label,
    )

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        _set_focus("f" * 1200)
        _seed_semantic_ranking_fixture(
            workspace,
            provider,
            concept_text=_FULL_SNIPPET_BODY,
            decision_label=_LONG_LABEL,
        )
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=1500,
            settings=CompactionSettings(),
            session_id="long-label-dead-band",
            source="compact",
        )
        out = capsys.readouterr().out
    finally:
        embedding_store.reset_provider_cache()

    section = _semantic_section_text(out)
    assert "relevant:" in section, f"155-char-label result missing; stdout={out!r}"
    expected_label = render_concept_label(_LONG_LABEL)
    assert len(expected_label) == RENDERED_LABEL_CAP
    assert expected_label.endswith(ELLIPSIS_MARKER)
    assert f"- {expected_label}: " in section
    assert _LONG_LABEL not in section
    assert "[decision:777]" in section
    assert ELLIPSIS_MARKER in section


def test_healthy_focus_lengths_keep_recorded_semantic_section(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Sweep fixed-content lengths: healthy shoulders must not shrink below
    recorded base sizes, and the former dead band must now emit a result.
    """
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        _seed_semantic_ranking_fixture(
            workspace,
            provider,
            concept_text="healthy-shoulder-marker-xyzzy",
        )
        for focus_len in (*_BASE_SEMANTIC_SECTION_CHARS, *_DEAD_BAND_FOCUS_LENS):
            _clear_session_reinjections()
            _set_focus("f" * focus_len)
            mod._reinject(
                repo_root=str(workspace),
                budget_chars=1500,
                settings=CompactionSettings(),
                session_id=f"shoulder-{focus_len}",
                source="compact",
            )
            out = capsys.readouterr().out
            section = _semantic_section_text(out)
            measured = len(section)
            if focus_len in _BASE_SEMANTIC_SECTION_CHARS:
                assert measured >= _BASE_SEMANTIC_SECTION_CHARS[focus_len], (
                    f"focus={focus_len} semantic section shrank: "
                    f"{measured} < {_BASE_SEMANTIC_SECTION_CHARS[focus_len]}; "
                    f"section={section!r}"
                )
                assert _HEALTHY_SHOULDER_SECTION in out
            else:
                assert "relevant:" in section, f"dead-band focus={focus_len} still starved; stdout={out!r}"
                assert "[decision:777]" in section
    finally:
        embedding_store.reset_provider_cache()


def test_reservation_capped_skip_reason_when_floor_exceeds_cap(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A 500-char snippet floor exceeds 1/3 of a 1500-char block. The hook
    must name ``reservation_capped`` instead of silently clamping. Fails on
    base: the skip reason is ``budget_exhausted`` or absent.
    """
    from workbay_handoff_mcp.compaction import CompactionSettings
    from workbay_handoff_mcp.embeddings import store as embedding_store
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION", "1")
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC_SNIPPET_CHARS", "500")
    provider = _FakeProvider()
    embedding_store.set_provider_for_testing(provider)
    mod = _load_reinject_module()
    try:
        _set_focus("f" * 1200)
        _seed_semantic_ranking_fixture(
            workspace,
            provider,
            concept_text=_FULL_SNIPPET_BODY,
            decision_label=_LONG_LABEL,
        )
        mod._reinject(
            repo_root=str(workspace),
            budget_chars=1500,
            settings=CompactionSettings(),
            session_id="reservation-capped",
            source="compact",
        )
        captured = capsys.readouterr()
    finally:
        embedding_store.reset_provider_cache()

    assert "relevant:" not in captured.out
    assert "reservation_capped" in captured.err
    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT semantic_detail_json FROM session_reinjections WHERE session_id = ?",
            ("reservation-capped",),
        ).fetchone()
    assert row is not None
    detail = json.loads(row["semantic_detail_json"])
    assert detail["skip_reason"] == "reservation_capped"
    assert detail["status"] == "skipped"
