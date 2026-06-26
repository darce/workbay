"""Tests for the branch-naming gate helper (implementation note Slices 3 + 4).

The helper is a single delegate invoked by every git-side gate. This
file pins the ``--trigger post-checkout`` (warn-only) and
``--trigger pre-commit`` (hard-block + bounded override) semantics.
implementation note adds tests for ``--trigger pre-push``.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

import importlib.util


HOOK_SCRIPT = Path(__file__).parent / "check_branch_naming.py"

_spec = importlib.util.spec_from_file_location("check_branch_naming", HOOK_SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
assert _spec is not None and _spec.loader is not None
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

run_post_checkout = _mod.run_post_checkout
run_pre_commit = _mod.run_pre_commit
run_pre_push = _mod.run_pre_push
_format_warning = _mod._format_warning
_format_pre_commit_block = _mod._format_pre_commit_block
_format_pre_push_block = _mod._format_pre_push_block
_is_conforming_or_protected = _mod._is_conforming_or_protected
_record_override_with_timeout = _mod._record_override_with_timeout
_record_push_override_with_timeout = _mod._record_push_override_with_timeout
_OVERRIDE_LOG_RELPATH = _mod._OVERRIDE_LOG_RELPATH
_PUSH_OVERRIDE_LOG_RELPATH = _mod._PUSH_OVERRIDE_LOG_RELPATH
main = _mod.main


# ---------------------------------------------------------------------------
# Unit: classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "branch",
    [
        "main",
        "master",
        "release/v1",
        "release/2026-q2",
        "hotfix/foo",
        "feature/internal-37",
        "feature/internal-37-slice3-post-checkout",
        "feature/plan-0006",
        "feature/maint-dirty-br-01",
    ],
)
def test_is_conforming_or_protected_true(branch: str) -> None:
    assert _is_conforming_or_protected(branch) is True


@pytest.mark.parametrize(
    "branch",
    [
        "feature/bad",
        "feature/bad-name",
        "feature/internal",
        "fix/foo",
        "chore/bar",
        "wip-thing",
        "random-branch",
    ],
)
def test_is_conforming_or_protected_false(branch: str) -> None:
    assert _is_conforming_or_protected(branch) is False


# ---------------------------------------------------------------------------
# Unit: run_post_checkout
# ---------------------------------------------------------------------------


def _capture(branch: str | None, suggested: str | None = None) -> tuple[int, str]:
    stream = io.StringIO()
    code = run_post_checkout(branch, suggester=lambda: suggested, stream=stream)
    return code, stream.getvalue()


def test_post_checkout_silent_on_conforming_branch() -> None:
    code, output = _capture("feature/internal-37-foo")
    assert code == 0
    assert output == ""


def test_post_checkout_silent_on_protected_branch() -> None:
    for branch in ("main", "master", "release/v1", "hotfix/foo"):
        code, output = _capture(branch)
        assert code == 0, branch
        assert output == "", branch


def test_post_checkout_silent_on_empty_branch() -> None:
    """Detached HEAD must never trigger output (warn-only contract still
    applies, but there is no name to warn about)."""
    code, output = _capture("")
    assert code == 0
    assert output == ""

    code, output = _capture(None)
    assert code == 0
    assert output == ""


def test_post_checkout_warns_on_non_conforming_with_suggestion() -> None:
    """The suggestion path must produce output even when the *current*
    branch is unparseable — ``derive_task_ref_candidates`` returns an
    empty list, so the formatter (which consults the active task)
    is what carries the "did you mean" line."""
    code, output = _capture("feature/bad-name", suggested="feature/internal-37")
    assert code == 0
    assert "WARNING" in output
    assert "feature/bad-name" in output
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in output
    assert "Did you mean: feature/internal-37" in output


def test_post_checkout_warns_on_non_conforming_without_active_task() -> None:
    """Cold-start: no active task registered, suggester returns None.
    The fallback message instructs the operator to register a task."""
    code, output = _capture("feature/bad-name", suggested=None)
    assert code == 0
    assert "WARNING" in output
    assert "feature/bad-name" in output
    assert "make task-start" in output
    # Must not render an empty "Did you mean:" line.
    assert "Did you mean" not in output


def test_post_checkout_warns_on_other_prefix() -> None:
    """implementation note r3 widened the rejection class beyond ``feature/<bad>``;
    ``fix/foo``, ``chore/bar``, ``wip-thing`` all warn."""
    for branch in ("fix/foo", "chore/bar", "wip-thing"):
        code, output = _capture(branch)
        assert code == 0, branch
        assert "WARNING" in output, branch
        assert branch in output, branch


def test_warning_names_canonical_rule_and_override_path() -> None:
    """The warn line MUST cite the canonical module path and the
    override env var so the operator can find both without grep."""
    text = _format_warning("feature/bad", suggested="feature/internal-37")
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in text
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH" in text
    assert "warn-only" in text


# ---------------------------------------------------------------------------
# Integration: subprocess invocation in a temp git repo
# ---------------------------------------------------------------------------


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )


def _init_repo(tmp_path: Path, branch: str) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    (repo / "README").write_text("hi\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    if branch != "main":
        _git(repo, "checkout", "-q", "-b", branch)
    return repo


def _run_helper(repo: Path, trigger: str = "post-checkout") -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT), "--trigger", trigger],
        cwd=str(repo),
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_helper_subprocess_silent_on_conforming_branch(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "feature/internal-37-foo")
    proc = _run_helper(repo)
    assert proc.returncode == 0
    assert proc.stderr == ""
    assert proc.stdout == ""


def test_helper_subprocess_warns_on_non_conforming_branch(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "feature/bad-name")
    proc = _run_helper(repo)
    assert proc.returncode == 0  # warn-only
    assert "WARNING" in proc.stderr
    assert "feature/bad-name" in proc.stderr
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in proc.stderr


def test_helper_subprocess_silent_on_main(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "main")
    proc = _run_helper(repo)
    assert proc.returncode == 0
    assert proc.stderr == ""


def test_helper_subprocess_pre_push_blocks_non_conforming(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "feature/bad-name")
    proc = _run_helper(repo, trigger="pre-push")
    assert proc.returncode == 1
    assert "BLOCKED" in proc.stderr
    assert "feature/bad-name" in proc.stderr


def test_helper_subprocess_pre_push_allows_conforming(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "feature/internal-37-foo")
    proc = _run_helper(repo, trigger="pre-push")
    assert proc.returncode == 0
    assert proc.stderr == ""


def test_helper_subprocess_pre_push_silent_on_main(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "main")
    proc = _run_helper(repo, trigger="pre-push")
    assert proc.returncode == 0
    assert proc.stderr == ""


# ---------------------------------------------------------------------------
# Integration: the hook line in scripts/hooks/git/post-checkout fires
# ---------------------------------------------------------------------------


def test_post_checkout_hook_invokes_helper(tmp_path: Path) -> None:
    """End-to-end: a checkout in a fixture repo whose ``core.hooksPath``
    points at the Slice-0 layout fires the helper and surfaces the
    warning. This is the regression test for "the dormant hooks now
    actually fire" claim from implementation note §0."""
    repo = tmp_path / "consumer"
    repo.mkdir()
    # Mirror the consumer layout: scripts/hooks/check_branch_naming.py
    # plus scripts/hooks/git/post-checkout. We do not copy
    # check_main_clean.py / check_root_on_main.py — the post-checkout
    # script tolerates their absence via ``|| true``.
    hooks_dir = repo / "scripts" / "hooks"
    hooks_dir.mkdir(parents=True)
    (hooks_dir / "check_branch_naming.py").write_bytes(HOOK_SCRIPT.read_bytes())
    (hooks_dir / "check_branch_naming.py").chmod(0o755)
    src_post_checkout = HOOK_SCRIPT.parent / "git" / "post-checkout"
    git_hooks_dir = hooks_dir / "git"
    git_hooks_dir.mkdir()
    (git_hooks_dir / "post-checkout").write_bytes(src_post_checkout.read_bytes())
    (git_hooks_dir / "post-checkout").chmod(0o755)

    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    _git(repo, "config", "core.hooksPath", "scripts/hooks/git")
    (repo / "x").write_text("hi\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")

    # Branch checkout — should fire the helper and emit the warning.
    proc = subprocess.run(
        ["git", "-C", str(repo), "checkout", "-b", "feature/bad-name"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    # Git relays hook stderr; the warn line we care about is in
    # combined output. The other helpers (check_main_clean,
    # check_root_on_main) may print their own lines; we only check
    # that ours appears.
    combined = proc.stdout + proc.stderr
    assert "feature/bad-name" in combined, combined
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in combined, combined


# ---------------------------------------------------------------------------
# implementation note: pre-commit hard gate + bounded override
# ---------------------------------------------------------------------------


def _capture_pre_commit(
    branch: str | None,
    env: dict | None = None,
    *,
    recorder=None,
    log_writer=None,
    repo_root: Path | None = None,
) -> tuple[int, str, list]:
    """Run ``run_pre_commit`` capturing exit code, stderr, and any
    fallback-log writer invocations as a list of (root, branch, kwargs)
    tuples."""
    stream = io.StringIO()
    calls: list = []

    def _capturing_log_writer(root, branch_arg, *, reason, error):
        calls.append((root, branch_arg, {"reason": reason, "error": error}))

    code = run_pre_commit(
        branch,
        env=env,
        recorder=recorder,
        log_writer=log_writer if log_writer is not None else _capturing_log_writer,
        stream=stream,
        repo_root=repo_root,
    )
    return code, stream.getvalue(), calls


def test_pre_commit_allows_conforming_branch() -> None:
    code, output, calls = _capture_pre_commit("feature/internal-37-foo", env={})
    assert code == 0
    assert output == ""
    assert calls == []


def test_pre_commit_allows_protected_branches() -> None:
    for branch in ("main", "master", "release/v1", "hotfix/foo"):
        code, output, calls = _capture_pre_commit(branch, env={})
        assert code == 0, branch
        assert output == "", branch
        assert calls == [], branch


def test_pre_commit_allows_empty_branch() -> None:
    """Detached HEAD must not block — the pre-commit gate cannot afford
    to wedge a branchless commit (e.g. interactive rebase). The naming
    contract has nothing to enforce when there is no branch name."""
    code, output, _ = _capture_pre_commit("", env={})
    assert code == 0
    assert output == ""


def test_pre_commit_blocks_non_conforming_branch() -> None:
    code, output, calls = _capture_pre_commit("feature/bad-name", env={})
    assert code == 1
    assert "BLOCKED" in output
    assert "feature/bad-name" in output
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in output
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH" in output
    assert calls == []  # No fallback log entry on a clean block.


def test_pre_commit_blocks_other_prefixes() -> None:
    """implementation note r3 widened the rejection class — fix/foo, chore/bar,
    wip-thing all hard-block at pre-commit."""
    for branch in ("fix/foo", "chore/bar", "wip-thing"):
        code, output, _ = _capture_pre_commit(branch, env={})
        assert code == 1, branch
        assert branch in output, branch


def test_pre_commit_override_records_decision_and_allows(tmp_path: Path) -> None:
    """``WORKBAY_ALLOW_NONCONFORMING_BRANCH=1`` allows the commit and
    invokes the recorder. Successful recording → no fallback-log row."""
    recorded: list = []

    def _fake_recorder(branch, reason):
        recorded.append((branch, reason))
        return None  # success

    code, output, calls = _capture_pre_commit(
        "feature/bad-name",
        env={
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH": "1",
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_REASON": "release-cut",
        },
        recorder=_fake_recorder,
        repo_root=tmp_path,
    )
    assert code == 0
    assert "override accepted" in output
    assert "decision event recorded" in output
    assert recorded == [("feature/bad-name", "release-cut")]
    assert calls == []
    # No fallback log file should have been touched.
    assert not (tmp_path / _OVERRIDE_LOG_RELPATH).exists()


def test_pre_commit_override_falls_back_on_recorder_error(tmp_path: Path) -> None:
    """Handoff DB unavailable / locked — recorder returns an error
    string. The commit MUST still complete (override-never-blocks
    invariant) and a JSONL fallback row is appended."""
    def _failing_recorder(branch, reason):
        return "OperationalError: database is locked"

    code, output, calls = _capture_pre_commit(
        "feature/bad-name",
        env={"WORKBAY_ALLOW_NONCONFORMING_BRANCH": "1"},
        recorder=_failing_recorder,
        repo_root=tmp_path,
    )
    assert code == 0
    assert "override accepted" in output
    assert "audit fallback" in output
    assert "OperationalError" in output
    assert len(calls) == 1
    assert calls[0][1] == "feature/bad-name"
    assert calls[0][2]["error"].startswith("OperationalError")


def test_pre_commit_override_writes_fallback_log_to_disk(tmp_path: Path) -> None:
    """End-to-end: when the recorder fails, the *real* log writer
    appends a JSONL row at ``.task-state/branch_naming_overrides.log``
    relative to the repo root."""
    def _failing_recorder(branch, reason):
        return "timeout"

    stream = io.StringIO()
    code = run_pre_commit(
        "feature/bad-name",
        env={
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH": "1",
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_REASON": "hotfix",
        },
        recorder=_failing_recorder,
        # Use the real log writer (no injection).
        stream=stream,
        repo_root=tmp_path,
    )
    assert code == 0
    log_path = tmp_path / _OVERRIDE_LOG_RELPATH
    assert log_path.exists()
    rows = [json.loads(line) for line in log_path.read_text().splitlines() if line]
    assert len(rows) == 1
    row = rows[0]
    assert row["branch"] == "feature/bad-name"
    assert row["reason"] == "hotfix"
    assert row["error"] == "timeout"
    assert "timestamp" in row
    assert "commit_author" in row


def test_pre_commit_override_completes_within_three_seconds_when_recorder_hangs(
    tmp_path: Path,
) -> None:
    """**Load-bearing test** for implementation note acceptance: the override
    path must NEVER block the commit. Mock ``record_event`` to hang
    (sleep 10 s); the 2 s wall-clock timeout in
    ``_record_override_with_timeout`` must trip and route the failure
    into the fallback log. Total elapsed wall-clock must stay under
    3 s (1 s headroom over the 2 s timeout)."""
    def _hanging_record_event(event):
        time.sleep(10.0)

    # Inject the hanging record_event into the import that
    # `_record_override_with_timeout` performs lazily.
    import workbay_handoff_mcp as _ahm
    original = _ahm.record_event
    _ahm.record_event = _hanging_record_event
    try:
        start = time.monotonic()
        stream = io.StringIO()
        code = run_pre_commit(
            "feature/bad-name",
            env={"WORKBAY_ALLOW_NONCONFORMING_BRANCH": "1"},
            # Use the real recorder so the timeout path is exercised.
            stream=stream,
            repo_root=tmp_path,
        )
        elapsed = time.monotonic() - start
    finally:
        _ahm.record_event = original

    assert code == 0, "override path must never block the commit"
    assert elapsed < 3.0, f"override path took {elapsed:.2f}s, expected < 3.0s"
    output = stream.getvalue()
    assert "override accepted" in output
    assert "audit fallback: timeout" in output
    log_path = tmp_path / _OVERRIDE_LOG_RELPATH
    assert log_path.exists()
    rows = [json.loads(line) for line in log_path.read_text().splitlines() if line]
    assert len(rows) == 1
    assert rows[0]["error"] == "timeout"
    assert rows[0]["branch"] == "feature/bad-name"


def test_pre_commit_block_message_cites_canonical_rule_and_separate_pre_push() -> None:
    text = _format_pre_commit_block("feature/bad")
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in text
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH" in text
    # The block message MUST warn that pre-push enforces a separate
    # gate — operators who set the override only on the commit side
    # would otherwise discover the pre-push rejection mid-push.
    assert "Pre-push" in text or "pre-push" in text


def test_pre_commit_block_override_lines_are_one_var_per_line() -> None:
    """BR-R2-05: avoid the wrapped 'set X=1 (optionally\\n  set Y=...)' format.

    The old format crammed both env vars onto one logical line with a
    hard-coded continuation indent. Under terminal wrapping the second
    var visually merged with the first. Each var should now stand on
    its own line so operators can copy-paste either independently.
    """
    text = _format_pre_commit_block("feature/bad")
    lines = text.splitlines()

    required_lines = [ln for ln in lines if "WORKBAY_ALLOW_NONCONFORMING_BRANCH=1" in ln]
    reason_lines = [ln for ln in lines if "WORKBAY_ALLOW_NONCONFORMING_BRANCH_REASON" in ln]
    assert len(required_lines) == 1, (
        f"expected exactly one line carrying the required override env var, "
        f"got {len(required_lines)}: {required_lines!r}"
    )
    assert len(reason_lines) == 1, (
        f"expected exactly one line carrying the optional reason env var, "
        f"got {len(reason_lines)}: {reason_lines!r}"
    )
    # The two env vars must NOT share a line.
    assert required_lines[0] != reason_lines[0], (
        "required and optional override env vars must each have their own line"
    )
    # The optional reason line must mark itself optional so operators
    # don't assume both are required.
    assert "optional" in reason_lines[0].lower(), (
        f"reason var line must declare itself optional, got: {reason_lines[0]!r}"
    )
    # No awkward "(optionally\n  set" continuation pattern.
    assert "(optionally" not in text, (
        "the wrapped '(optionally\\n  set ...' continuation has been "
        "replaced by per-line override lines"
    )


# ---------------------------------------------------------------------------
# implementation note: subprocess-level pre-commit invocation
# ---------------------------------------------------------------------------


def test_helper_subprocess_pre_commit_blocks_non_conforming(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "feature/bad-name")
    proc = _run_helper(repo, trigger="pre-commit")
    assert proc.returncode == 1
    assert "BLOCKED" in proc.stderr
    assert "feature/bad-name" in proc.stderr


def test_helper_subprocess_pre_commit_allows_conforming(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path, "feature/internal-37-foo")
    proc = _run_helper(repo, trigger="pre-commit")
    assert proc.returncode == 0
    assert proc.stderr == ""


# ---------------------------------------------------------------------------
# implementation note: end-to-end git commit blocked / unblocked
# ---------------------------------------------------------------------------


def _init_repo_with_pre_commit_hook(tmp_path: Path, branch: str) -> Path:
    """Mirror the consumer-installed layout: scripts/hooks/{git,...}
    plus core.hooksPath=scripts/hooks/git."""
    repo = tmp_path / "consumer"
    repo.mkdir()
    hooks_dir = repo / "scripts" / "hooks"
    hooks_dir.mkdir(parents=True)
    (hooks_dir / "check_branch_naming.py").write_bytes(HOOK_SCRIPT.read_bytes())
    (hooks_dir / "check_branch_naming.py").chmod(0o755)

    git_hooks_dir = hooks_dir / "git"
    git_hooks_dir.mkdir()
    src_pre_commit = HOOK_SCRIPT.parent / "git" / "pre-commit"
    (git_hooks_dir / "pre-commit").write_bytes(src_pre_commit.read_bytes())
    (git_hooks_dir / "pre-commit").chmod(0o755)

    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    _git(repo, "config", "core.hooksPath", "scripts/hooks/git")
    (repo / "x").write_text("hi\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    if branch != "main":
        _git(repo, "checkout", "-q", "-b", branch)
    return repo


def test_git_commit_blocked_on_non_conforming_branch(tmp_path: Path) -> None:
    repo = _init_repo_with_pre_commit_hook(tmp_path, "feature/bad-name")
    (repo / "y").write_text("hello\n")
    proc = subprocess.run(
        ["git", "-C", str(repo), "add", "y"], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0
    proc = subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", "should fail"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    combined = proc.stdout + proc.stderr
    assert "BLOCKED" in combined
    assert "feature/bad-name" in combined


def test_git_commit_succeeds_on_conforming_branch(tmp_path: Path) -> None:
    repo = _init_repo_with_pre_commit_hook(tmp_path, "feature/internal-37-foo")
    (repo / "y").write_text("hello\n")
    _git(repo, "add", "y")
    proc = subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", "ok"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


# ---------------------------------------------------------------------------
# implementation note: pre-push hard gate + distinct bounded override
# ---------------------------------------------------------------------------


def _capture_pre_push(
    branch: str | None,
    env: dict | None = None,
    *,
    recorder=None,
    log_writer=None,
    repo_root: Path | None = None,
) -> tuple[int, str, list]:
    stream = io.StringIO()
    calls: list = []

    def _capturing_log_writer(root, branch_arg, *, reason, error):
        calls.append((root, branch_arg, {"reason": reason, "error": error}))

    code = run_pre_push(
        branch,
        env=env,
        recorder=recorder,
        log_writer=log_writer if log_writer is not None else _capturing_log_writer,
        stream=stream,
        repo_root=repo_root,
    )
    return code, stream.getvalue(), calls


def test_pre_push_allows_conforming_branch() -> None:
    code, output, calls = _capture_pre_push("feature/internal-37-foo", env={})
    assert code == 0
    assert output == ""
    assert calls == []


def test_pre_push_allows_protected_branches() -> None:
    for branch in ("main", "master", "release/v1", "hotfix/foo"):
        code, output, calls = _capture_pre_push(branch, env={})
        assert code == 0, branch
        assert output == "", branch
        assert calls == [], branch


def test_pre_push_allows_empty_branch() -> None:
    code, output, _ = _capture_pre_push("", env={})
    assert code == 0
    assert output == ""


def test_pre_push_blocks_non_conforming_branch() -> None:
    code, output, calls = _capture_pre_push("feature/bad-name", env={})
    assert code == 1
    assert "BLOCKED" in output
    assert "feature/bad-name" in output
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in output
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH" in output
    assert calls == []


def test_pre_push_does_not_honor_commit_side_override_env() -> None:
    """Critical isolation invariant: WORKBAY_ALLOW_NONCONFORMING_BRANCH
    (the commit-side override) MUST NOT bypass the push gate. Operators
    must re-assert leniency at publish time via the distinct env var."""
    code, output, calls = _capture_pre_push(
        "feature/bad-name",
        env={
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH": "1",
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_REASON": "set during commit",
        },
    )
    assert code == 1, "commit-side override leaked into push gate"
    assert "BLOCKED" in output
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH" in output
    assert calls == []


def test_pre_push_override_records_decision_and_allows(tmp_path: Path) -> None:
    """Distinct push override env var allows the push and invokes the
    push-side recorder. Successful recording → no fallback row."""
    recorded: list = []

    def _fake_recorder(branch, reason):
        recorded.append((branch, reason))
        return None

    code, output, calls = _capture_pre_push(
        "feature/bad-name",
        env={
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH": "1",
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON": "publish-cut",
        },
        recorder=_fake_recorder,
        repo_root=tmp_path,
    )
    assert code == 0
    assert "push override accepted" in output
    assert "decision event recorded" in output
    assert recorded == [("feature/bad-name", "publish-cut")]
    assert calls == []
    assert not (tmp_path / _PUSH_OVERRIDE_LOG_RELPATH).exists()


def test_pre_push_override_falls_back_on_recorder_error(tmp_path: Path) -> None:
    def _failing_recorder(branch, reason):
        return "OperationalError: database is locked"

    code, output, calls = _capture_pre_push(
        "feature/bad-name",
        env={"WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH": "1"},
        recorder=_failing_recorder,
        repo_root=tmp_path,
    )
    assert code == 0
    assert "push override accepted" in output
    assert "audit fallback" in output
    assert "OperationalError" in output
    assert len(calls) == 1
    assert calls[0][1] == "feature/bad-name"
    assert calls[0][2]["error"].startswith("OperationalError")


def test_pre_push_override_writes_fallback_log_to_disk(tmp_path: Path) -> None:
    def _failing_recorder(branch, reason):
        return "timeout"

    stream = io.StringIO()
    code = run_pre_push(
        "feature/bad-name",
        env={
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH": "1",
            "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON": "publish-fallback",
        },
        recorder=_failing_recorder,
        stream=stream,
        repo_root=tmp_path,
    )
    assert code == 0
    log_path = tmp_path / _PUSH_OVERRIDE_LOG_RELPATH
    assert log_path.exists()
    rows = [json.loads(line) for line in log_path.read_text().splitlines() if line]
    assert len(rows) == 1
    row = rows[0]
    assert row["branch"] == "feature/bad-name"
    assert row["reason"] == "publish-fallback"
    assert row["error"] == "timeout"
    # Pre-push fallback log MUST be a distinct file from the commit-side log.
    assert not (tmp_path / _OVERRIDE_LOG_RELPATH).exists()


def test_pre_push_override_completes_within_three_seconds_when_recorder_hangs(
    tmp_path: Path,
) -> None:
    """**Load-bearing test**: the push override path must NEVER block
    the push. Mock ``record_event`` to sleep 10 s; the 2 s daemon-thread
    timeout in ``_record_push_override_with_timeout`` must trip and
    route the failure into the push-side fallback log."""
    def _hanging_record_event(event):
        time.sleep(10.0)

    import workbay_handoff_mcp as _ahm
    original = _ahm.record_event
    _ahm.record_event = _hanging_record_event
    try:
        start = time.monotonic()
        stream = io.StringIO()
        code = run_pre_push(
            "feature/bad-name",
            env={"WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH": "1"},
            stream=stream,
            repo_root=tmp_path,
        )
        elapsed = time.monotonic() - start
    finally:
        _ahm.record_event = original

    assert code == 0, "push override path must never block the push"
    assert elapsed < 3.0, f"push override path took {elapsed:.2f}s, expected < 3.0s"
    output = stream.getvalue()
    assert "push override accepted" in output
    assert "audit fallback: timeout" in output
    log_path = tmp_path / _PUSH_OVERRIDE_LOG_RELPATH
    assert log_path.exists()
    rows = [json.loads(line) for line in log_path.read_text().splitlines() if line]
    assert len(rows) == 1
    assert rows[0]["error"] == "timeout"
    assert rows[0]["branch"] == "feature/bad-name"


def test_pre_push_block_message_cites_canonical_rule_and_distinct_env_var() -> None:
    text = _format_pre_push_block("feature/bad")
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in text
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH" in text
    # Must explicitly call out that the commit-side override does not carry over.
    assert "does NOT carry over" in text or "does not carry over" in text


def test_pre_push_block_override_lines_are_one_var_per_line() -> None:
    """BR-R2-05 mirror of the pre-commit override layout fix."""
    text = _format_pre_push_block("feature/bad")
    lines = text.splitlines()

    required_lines = [ln for ln in lines if "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1" in ln]
    reason_lines = [ln for ln in lines if "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON" in ln]
    assert len(required_lines) == 1, (
        f"expected exactly one line carrying the push override env var, "
        f"got {len(required_lines)}: {required_lines!r}"
    )
    assert len(reason_lines) == 1, (
        f"expected exactly one line carrying the push reason env var, "
        f"got {len(reason_lines)}: {reason_lines!r}"
    )
    assert required_lines[0] != reason_lines[0]
    assert "optional" in reason_lines[0].lower()
    assert "(optionally" not in text


# ---------------------------------------------------------------------------
# implementation note: end-to-end git push blocked / unblocked
# ---------------------------------------------------------------------------


def _init_repo_with_pre_push_hook(tmp_path: Path, branch: str) -> tuple[Path, Path]:
    """Mirror the consumer-installed layout for pre-push gate testing.

    Returns (consumer_repo, bare_remote) so callers can drive a real
    ``git push`` end-to-end. Stages only the branch-naming check from
    the pre-push hook by stubbing ``check_main_clean.py`` to a no-op
    (the dirty-main concern is tested elsewhere)."""
    repo = tmp_path / "consumer"
    repo.mkdir()
    hooks_dir = repo / "scripts" / "hooks"
    hooks_dir.mkdir(parents=True)
    (hooks_dir / "check_branch_naming.py").write_bytes(HOOK_SCRIPT.read_bytes())
    (hooks_dir / "check_branch_naming.py").chmod(0o755)
    # Stub the dirty-main scanner — we only want to exercise the
    # branch-naming branch of the pre-push hook here.
    (hooks_dir / "check_main_clean.py").write_text(
        "#!/usr/bin/env python3\nimport sys; sys.exit(0)\n"
    )
    (hooks_dir / "check_main_clean.py").chmod(0o755)

    git_hooks_dir = hooks_dir / "git"
    git_hooks_dir.mkdir()
    src_pre_push = HOOK_SCRIPT.parent / "git" / "pre-push"
    (git_hooks_dir / "pre-push").write_bytes(src_pre_push.read_bytes())
    (git_hooks_dir / "pre-push").chmod(0o755)

    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    _git(repo, "config", "core.hooksPath", "scripts/hooks/git")
    (repo / "x").write_text("hi\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")

    bare = tmp_path / "remote.git"
    subprocess.run(
        ["git", "init", "--bare", "-q", str(bare)], check=True, capture_output=True
    )
    _git(repo, "remote", "add", "origin", str(bare))

    if branch != "main":
        # Use --no-verify on commit so we can land an offending commit
        # without the commit-side gate intercepting; the test then
        # asserts the push gate still blocks publication.
        _git(repo, "checkout", "-q", "-b", branch)
        (repo / "y").write_text("hello\n")
        _git(repo, "add", "y")
        subprocess.run(
            ["git", "-C", str(repo), "commit", "--no-verify", "-q", "-m", "wip"],
            check=True,
            capture_output=True,
        )
    return repo, bare


def test_git_push_blocked_on_non_conforming_branch_even_after_no_verify_commit(
    tmp_path: Path,
) -> None:
    """End-to-end isolation: ``git commit --no-verify`` lands a commit
    on a non-conforming branch, but ``git push`` MUST still be blocked
    by the pre-push gate."""
    repo, _bare = _init_repo_with_pre_push_hook(tmp_path, "feature/bad-name")
    proc = subprocess.run(
        ["git", "-C", str(repo), "push", "-u", "origin", "feature/bad-name"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    combined = proc.stdout + proc.stderr
    assert "BLOCKED" in combined
    assert "feature/bad-name" in combined
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH" in combined


def test_git_push_succeeds_on_conforming_branch(tmp_path: Path) -> None:
    repo, _bare = _init_repo_with_pre_push_hook(tmp_path, "feature/internal-37-foo")
    proc = subprocess.run(
        ["git", "-C", str(repo), "push", "-u", "origin", "feature/internal-37-foo"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
