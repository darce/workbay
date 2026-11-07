"""P0-1: tracked-and-clean state files must not trip find_dirty_state_files.

Consuming repos on a clean main were blocked by check-main-clean because
pass 2 of ``find_dirty_state_files`` treated existing tracked
``CURRENT_TASK.json`` / ``DASHBOARD.txt`` as dirty purely because they
existed on disk — without checking whether Git reported any modification.

Canon MEAS-11/MEAS-09: the decision needs "is this file dirty", not "does
this file exist". Fail-closed (RLSE-02): non-zero git exits, missing index
tags, and porcelain-suppressing index flags (skip-worktree / assume-unchanged)
still count as dirty; only the tracked + plain ``H`` + porcelain-clean case
is subtracted.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _branch_isolation_guard as guard  # noqa: E402
from _branch_isolation_guard import find_dirty_state_files  # noqa: E402
from _harness_protocol import (  # noqa: E402
    BranchIsolationPolicy,
    MainSurfacePattern,
    find_permitted_main_surface,
    is_branch_isolation_protected_path,
)


def _git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )
    return proc.stdout.strip()


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    (repo / "seed").write_text("seed\n", encoding="utf-8")
    _git("add", "seed", cwd=repo)
    _git("commit", "-q", "-m", "init", cwd=repo)
    return repo


def _state_policy() -> BranchIsolationPolicy:
    return BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=(),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
        state_dirty_surfaces=(
            MainSurfacePattern(pattern="CURRENT_TASK.json", reason="active-task snapshot"),
            MainSurfacePattern(pattern="DASHBOARD.txt", reason="dashboard render"),
        ),
        first_edit_protected_surfaces=(),
    )


def _commit_tracked_state(repo: Path) -> None:
    (repo / "CURRENT_TASK.json").write_text('{"task":"demo"}\n', encoding="utf-8")
    (repo / "DASHBOARD.txt").write_text("dashboard\n", encoding="utf-8")
    _git("add", "CURRENT_TASK.json", "DASHBOARD.txt", cwd=repo)
    _git("commit", "-q", "-m", "track state files", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", f"fixture must be clean after commit, got {porcelain!r}"


def test_tracked_and_clean_state_files_are_not_dirty(tmp_path: Path) -> None:
    """Reproduction: tracked, committed, unmodified state must return nothing."""
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert dirty == [], (
        "tracked-and-clean CURRENT_TASK.json/DASHBOARD.txt must not be dirty; "
        f"got {dirty!r}"
    )


def test_modified_tracked_state_file_still_reported_dirty(tmp_path: Path) -> None:
    """Contract smoke (not regression-proof for subtraction alone).

    A real modification must still trip the guard. Pre-fix code that flags
    every on-disk state file also passes this assertion.
    """
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)
    (repo / "CURRENT_TASK.json").write_text('{"task":"modified"}\n', encoding="utf-8")

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert "CURRENT_TASK.json" in dirty, dirty


def test_modified_state_with_subdir_repo_root_still_reported_dirty(tmp_path: Path) -> None:
    """Contract smoke: single-file subdir dirt still reported.

    Does not pin path-base normalization alone (pre-fix "everything dirty"
    also passes). Primary path-base regression is
    ``test_multi_candidate_subdir_repo_root_mixed_dirt``; mid-state proof is
    ``test_identity_path_mapping_under_reports_subdir_mixed_dirt``.
    """
    repo = _init_repo(tmp_path)
    sub = repo / "sub"
    sub.mkdir()
    (sub / "CURRENT_TASK.json").write_text('{"task":"demo"}\n', encoding="utf-8")
    _git("add", "sub/CURRENT_TASK.json", cwd=repo)
    _git("commit", "-q", "-m", "track nested state", cwd=repo)
    (sub / "CURRENT_TASK.json").write_text('{"task":"modified-in-sub"}\n', encoding="utf-8")

    dirty = find_dirty_state_files(repo_root=str(sub), policy=_state_policy())
    assert "CURRENT_TASK.json" in dirty, dirty


def test_skip_worktree_divergent_content_still_reported_dirty(tmp_path: Path) -> None:
    """Contract smoke: skip-worktree + disk!=HEAD must stay dirty.

    Porcelain is blind to skip-worktree divergence; fail-closed on non-H
    tags is the real contract. Pre-subtraction "everything dirty" also passes.
    """
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)
    _git("update-index", "--skip-worktree", "CURRENT_TASK.json", cwd=repo)
    (repo / "CURRENT_TASK.json").write_text('{"task":"divergent-skip"}\n', encoding="utf-8")

    # Control: porcelain is empty for this path even though content diverged.
    scoped = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all", "--", "CURRENT_TASK.json"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert scoped.returncode == 0
    assert scoped.stdout.strip() == "", scoped.stdout
    tag = subprocess.run(
        ["git", "ls-files", "-v", "--", "CURRENT_TASK.json"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout.strip()
    assert tag.startswith("S "), tag

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert "CURRENT_TASK.json" in dirty, dirty


def test_assume_unchanged_divergent_content_still_reported_dirty(tmp_path: Path) -> None:
    """Contract smoke: assume-unchanged + disk!=HEAD must stay dirty.

    Porcelain is blind to assume-unchanged divergence. Pre-subtraction
    "everything dirty" also passes this assertion.
    """
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)
    _git("update-index", "--assume-unchanged", "CURRENT_TASK.json", cwd=repo)
    (repo / "CURRENT_TASK.json").write_text('{"task":"divergent-assume"}\n', encoding="utf-8")

    scoped = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all", "--", "CURRENT_TASK.json"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert scoped.returncode == 0
    assert scoped.stdout.strip() == "", scoped.stdout
    tag = subprocess.run(
        ["git", "ls-files", "-v", "--", "CURRENT_TASK.json"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout.strip()
    # assume-unchanged uses a lowercase tag (commonly ``h``).
    assert tag and tag[0].islower(), tag

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert "CURRENT_TASK.json" in dirty, dirty


def test_run_git_degraded_returns_none_on_oserror(monkeypatch) -> None:
    """OSError (e.g. E2BIG argv) must degrade to None, not crash the hook."""
    def _boom(*_args, **_kwargs):  # noqa: ANN001, ANN003 - test stub
        raise OSError(7, "Argument list too long")

    monkeypatch.setattr(guard.subprocess, "run", _boom)
    assert guard._run_git_degraded(["-C", ".", "status", "--porcelain=v1"]) is None


def test_batch_ls_files_v_tags_huge_path_list_does_not_raise(tmp_path: Path) -> None:
    """Huge candidate sets must not traceback; mapping is dict or None.

    Chunking may succeed (empty dict for non-existent paths) or degrade to
    None on a tight ARG_MAX — both are correct fail-closed-or-empty outcomes.
    """
    repo = _init_repo(tmp_path)
    # Boundary on this host is far below 100k pathspecs; keep the list large
    # enough that a single splat would raise OSError without chunking.
    paths = [f"docs/tasks/archive/f{i:05d}.md" for i in range(100_000)]
    tags = guard._batch_ls_files_v_tags(repo, paths)
    assert tags is None or isinstance(tags, dict)
    if tags is not None:
        assert tags == {}


def test_batch_ls_files_v_tags_oserror_returns_none(tmp_path: Path, monkeypatch) -> None:
    """Contract smoke (not a round-4 production-delta pin).

    Forced OSError on the ls-files spawn must degrade to None (fail closed).
    This assertion was already true of the pre-chunk fail-closed path; it
    documents the contract rather than discriminating the round-4 chunker.
    """
    repo = _init_repo(tmp_path)

    def _boom(*_args, **_kwargs):  # noqa: ANN001, ANN003 - test stub
        raise OSError(7, "Argument list too long")

    monkeypatch.setattr(guard.subprocess, "run", _boom)
    assert guard._batch_ls_files_v_tags(repo, ["CURRENT_TASK.json"]) is None


def test_tracked_clean_non_ascii_state_path_not_dirty(tmp_path: Path) -> None:
    """Non-ASCII tracked+clean paths must not be re-flagged dirty by ls-files quoting.

    ``git ls-files -v`` without ``-z`` C-quotes non-ASCII paths, so tag lookup
    misses the raw path and fail-closed keeps a clean file dirty. ``-z`` returns
    the unquoted path and restores the consumer P0 fix.
    """
    repo = _init_repo(tmp_path)
    rel = "docs/tasks/archive/tâche-café.md"
    target = repo / rel
    target.parent.mkdir(parents=True)
    target.write_text("archive note\n", encoding="utf-8")
    _git("add", rel, cwd=repo)
    _git("commit", "-q", "-m", "track accented archive", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", porcelain
    # Control: non-z output is C-quoted on this host.
    quoted = subprocess.run(
        ["git", "ls-files", "-v", "--", rel],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout.strip()
    assert quoted.startswith('H "') or "\\" in quoted, quoted

    policy = BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=(),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
        state_dirty_surfaces=(
            MainSurfacePattern(pattern="docs/tasks/archive/**", reason="archive"),
        ),
        first_edit_protected_surfaces=(),
    )
    dirty = find_dirty_state_files(repo_root=str(repo), policy=policy)
    assert dirty == [], f"tracked-and-clean non-ASCII path must not be dirty; got {dirty!r}"


def test_tracked_clean_carriage_return_in_path_not_dirty(tmp_path: Path) -> None:
    """Regression: CR in a tracked+clean path must not be re-flagged dirty.

    ``git … -z`` returns raw CR in path bytes. ``text=True`` universal-newlines
    rewrites CR→LF so index-tag keys become newline-paths while filesystem
    candidates keep CR-paths; fail-closed then keeps a CLEAN file dirty.
    Bytes-mode -z parsing + ``os.fsdecode`` restores the match.
    """
    repo = _init_repo(tmp_path)
    rel = "docs/tasks/archive/weird\rname.json"
    target = repo / rel
    target.parent.mkdir(parents=True)
    target.write_text('{"task":"cr-path"}\n', encoding="utf-8")
    # Add via pathspec file to avoid shell CR handling issues.
    _git("add", "--", rel, cwd=repo)
    _git("commit", "-q", "-m", "track cr path", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", porcelain
    # Control: bytes -z keeps CR; text=True rewrites it to LF.
    z_bytes = subprocess.run(
        ["git", "ls-files", "-v", "-z", "--", rel],
        cwd=repo,
        capture_output=True,
        text=False,
        timeout=5,
        check=True,
    ).stdout
    assert b"\r" in z_bytes, z_bytes
    z_text = subprocess.run(
        ["git", "ls-files", "-v", "-z", "--", rel],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout
    assert "\r" not in z_text and "\n" in z_text, repr(z_text)

    policy = BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=(),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
        state_dirty_surfaces=(
            MainSurfacePattern(pattern="docs/tasks/archive/**", reason="archive"),
        ),
        first_edit_protected_surfaces=(),
    )
    dirty = find_dirty_state_files(repo_root=str(repo), policy=policy)
    assert dirty == [], (
        "tracked-and-clean path with CR must not be dirty after bytes -z parse; "
        f"got {dirty!r}"
    )


def test_multi_candidate_only_modified_state_reported_dirty(tmp_path: Path) -> None:
    """Batched pass-2 must not treat sibling clean state files as dirty.

    Round-2 regressions survived because every prior fixture used a single
    candidate. Mixed multi-candidate trees exercise the shared dirty set and
    the batched index-tag map together.
    """
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)
    (repo / "CURRENT_TASK.json").write_text('{"task":"only-this-dirty"}\n', encoding="utf-8")

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert dirty == ["CURRENT_TASK.json"], dirty


def test_multi_candidate_both_clean_not_dirty(tmp_path: Path) -> None:
    """Two tracked+clean state surfaces must both be subtracted."""
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert dirty == [], dirty


def test_multi_candidate_subdir_repo_root_mixed_dirt(tmp_path: Path) -> None:
    """Primary path-base regression: subdir repo_root + mixed multi-candidate dirt.

    When pass-1 porcelain stays top-level-relative while candidates are
    repo_root-relative, subtraction is vacuous and real dirt is under-reported.
    Correct mapping yields only the modified file.
    """
    repo = _init_repo(tmp_path)
    sub = repo / "pkg"
    sub.mkdir()
    (sub / "CURRENT_TASK.json").write_text('{"task":"demo"}\n', encoding="utf-8")
    (sub / "DASHBOARD.txt").write_text("dashboard\n", encoding="utf-8")
    _git("add", "pkg/CURRENT_TASK.json", "pkg/DASHBOARD.txt", cwd=repo)
    _git("commit", "-q", "-m", "nested multi state", cwd=repo)
    (sub / "CURRENT_TASK.json").write_text('{"task":"dirty"}\n', encoding="utf-8")

    dirty = find_dirty_state_files(repo_root=str(sub), policy=_state_policy())
    assert dirty == ["CURRENT_TASK.json"], dirty


def test_identity_path_mapping_under_reports_subdir_mixed_dirt(
    tmp_path: Path, monkeypatch
) -> None:
    """Contract smoke + mid-state discrimination for path-base mapping.

    Not advertised as a round-4-only regression pin: the identity-mapping
    arm documents that path-base normalization is load-bearing. With correct
    mapping, mixed dirt under a subdir repo_root reports
    ``['CURRENT_TASK.json']``. Forcing the mapping to identity leaves porcelain
    keys top-level (``pkg/CURRENT_TASK.json``) while candidates are
    repo-relative (``CURRENT_TASK.json``), so membership misses and
    subtraction wrongly clears the modified file → ``dirty == []``.
    """
    repo = _init_repo(tmp_path)
    sub = repo / "pkg"
    sub.mkdir()
    (sub / "CURRENT_TASK.json").write_text('{"task":"demo"}\n', encoding="utf-8")
    (sub / "DASHBOARD.txt").write_text("dashboard\n", encoding="utf-8")
    _git("add", "pkg/CURRENT_TASK.json", "pkg/DASHBOARD.txt", cwd=repo)
    _git("commit", "-q", "-m", "nested multi state", cwd=repo)
    (sub / "CURRENT_TASK.json").write_text('{"task":"dirty"}\n', encoding="utf-8")

    # Control arm: correct mapping reports only the modified file.
    correct = find_dirty_state_files(repo_root=str(sub), policy=_state_policy())
    assert correct == ["CURRENT_TASK.json"], correct

    # Identity mapping: do not strip the repo_prefix — pass-1 keys stay
    # top-level-relative and never match repo-relative candidates.
    monkeypatch.setattr(
        guard,
        "_toplevel_path_to_repo_relative",
        lambda toplevel_rel, repo_prefix: toplevel_rel.replace("\\", "/").lstrip("/").rstrip("/"),
    )
    under = find_dirty_state_files(repo_root=str(sub), policy=_state_policy())
    assert under == [], (
        "identity path mapping must under-report mixed subdir dirt "
        f"(proves mapping is load-bearing); got {under!r}"
    )


def test_fail_closed_when_pass1_status_degraded(tmp_path: Path, monkeypatch) -> None:
    """Contract smoke: pass-1 failure must not treat empty dirty set as clean.

    A genuinely modified tracked state file is porcelain-visible on pass 1.
    If pass 1 degrades and the implementation still subtracts any plain ``H``
    path not present in an empty dirty set, the modification is under-reported.
    Fail-closed requires the path to remain dirty (via fallback or no subtract).

    Also green against pre-batch per-path status (round 1). Pins the
    empty-dirty-set footgun for the batched design; multi-candidate tests
    cover batch interactions.
    """
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)
    (repo / "CURRENT_TASK.json").write_text('{"task":"hidden-by-degraded-pass1"}\n', encoding="utf-8")

    real_run = guard.subprocess.run
    # Fail only the pass-1 whole-repo status (has --ignored). Path-scoped
    # fallback status (no --ignored) is left working so a correct
    # fail-closed fallback still sees the dirt; an incorrect "empty dirty
    # set means clean" path would subtract and fail this assertion.
    def _pass1_nonzero(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        if (
            isinstance(cmd, (list, tuple))
            and "status" in cmd
            and "--porcelain=v1" in cmd
            and "--ignored" in cmd
        ):
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="status failed")
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(guard.subprocess, "run", _pass1_nonzero)

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert "CURRENT_TASK.json" in dirty, dirty


def test_fail_closed_when_run_git_degraded_returns_none_on_fallback(
    tmp_path: Path, monkeypatch
) -> None:
    """Contract smoke: ``_run_git_degraded`` → ``None`` must keep the path dirty.

    Also green against round-1 per-path status, because any ``None`` cleanliness
    probe must fail closed. Pins the contract for the batched pass-1 + fallback
    design rather than discriminating pre-batch code.
    """
    repo = _init_repo(tmp_path)
    _commit_tracked_state(repo)

    real_degraded = guard._run_git_degraded

    def _degrade_fallback(args, **kwargs):  # noqa: ANN001, ANN003 - test stub
        # Pass-1 whole-repo status (has --ignored): force degrade so the
        # per-path fallback is exercised.
        if "status" in args and "--ignored" in args:
            return None
        # Path-scoped porcelain fallback (status without --ignored): None.
        if "status" in args and "--porcelain=v1" in args and "--ignored" not in args:
            return None
        return real_degraded(args, **kwargs)

    monkeypatch.setattr(guard, "_run_git_degraded", _degrade_fallback)

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_state_policy())
    assert "CURRENT_TASK.json" in dirty, dirty
    assert "DASHBOARD.txt" in dirty, dirty


# ---------------------------------------------------------------------------
# ROUND 5 — protected-path gate: fail-closed degradation + CR dirty-path pins
# ---------------------------------------------------------------------------


def _protected_code_policy() -> BranchIsolationPolicy:
    return BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py", ".json"),
        root_protected_files=(),
        protected_main_surfaces=(
            MainSurfacePattern(pattern="apps/**", reason="application code"),
            MainSurfacePattern(pattern="docs/specs/**", reason="planning specs"),
        ),
        permitted_main_surfaces=(
            MainSurfacePattern(
                pattern="docs/specs/charter.md",
                reason="charter carve-out stays editable on main",
            ),
        ),
        state_dirty_surfaces=(),
        first_edit_protected_surfaces=(),
    )


def test_git_dirty_paths_returns_none_when_status_times_out(
    tmp_path: Path, monkeypatch
) -> None:
    """D1 pin: degraded status must not look like a clean tree ([]).

    Pre-fix returned ``[]`` for timeout / non-zero, so callers could not
    distinguish could-not-determine from genuinely clean.
    """
    repo = _init_repo(tmp_path)

    def _timeout(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=kwargs.get("timeout", 5))

    monkeypatch.setattr(guard.subprocess, "run", _timeout)
    assert guard._git_dirty_paths(repo) is None


def test_git_dirty_paths_returns_none_when_status_nonzero(
    tmp_path: Path, monkeypatch
) -> None:
    """D1 pin: non-zero git status is could-not-determine, not clean."""
    repo = _init_repo(tmp_path)
    real_run = guard.subprocess.run

    def _status_fail(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        if isinstance(cmd, (list, tuple)) and "status" in cmd and "--porcelain=v1" in cmd:
            return subprocess.CompletedProcess(cmd, 128, stdout=b"", stderr=b"fail")
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(guard.subprocess, "run", _status_fail)
    assert guard._git_dirty_paths(repo) is None


def test_git_dirty_paths_empty_stdout_on_success_is_clean(
    tmp_path: Path, monkeypatch
) -> None:
    """D1 pin: successful probe with empty porcelain remains genuinely clean."""
    repo = _init_repo(tmp_path)
    real_run = guard.subprocess.run

    def _empty_ok(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        if isinstance(cmd, (list, tuple)) and "status" in cmd and "--porcelain=v1" in cmd:
            return subprocess.CompletedProcess(cmd, 0, stdout=b"", stderr=b"")
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(guard.subprocess, "run", _empty_ok)
    assert guard._git_dirty_paths(repo) == []


def test_find_dirty_protected_paths_fail_closed_when_git_degraded(
    tmp_path: Path, monkeypatch
) -> None:
    """D1 pin: degraded probe must BLOCK, not return None (allow).

    Pre-fix ``_git_dirty_paths`` returned ``[]`` on timeout, so
    ``find_dirty_protected_paths`` returned ``None`` and the inline guard
    allowed the write. Must return the degraded third state with a distinct
    actionable block message.
    """
    repo = _init_repo(tmp_path)

    def _timeout(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=kwargs.get("timeout", 5))

    monkeypatch.setattr(guard.subprocess, "run", _timeout)
    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "degraded probe must not allow (None)"
    assert isinstance(result, guard.DirtyProtectedDegraded), type(result)
    assert result.branch == "main"
    msg = result.block_message
    assert "could not determine" in msg.lower(), msg
    assert "degrad" in msg.lower() or "timeout" in msg.lower(), msg
    assert "git status" in msg.lower(), msg
    # Distinct from ordinary dirty-paths wording.
    assert "already dirty" not in msg.lower(), msg


def test_dirty_protected_degraded_is_not_tuple_unpackable() -> None:
    """D3 pin: no ``__iter__`` shim — forgotten callers must TypeError.

    The round-5 compat iterator let three of four callers keep the old
    ``branch, paths = result`` pattern and silently mishandle degrade.
    Unpacking must fail loudly so future call sites cannot regress.
    """
    degraded = guard.DirtyProtectedDegraded(branch="main")
    with pytest.raises(TypeError):
        _branch, _paths = degraded  # type: ignore[misc]


def test_find_dirty_protected_paths_clean_tree_still_allows(tmp_path: Path) -> None:
    """D1 regression: healthy clean main must still allow (return None)."""
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/ok.py", cwd=repo)
    _git("commit", "-q", "-m", "track protected clean", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", porcelain

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, f"clean tree must allow; got {result!r}"


def test_find_dirty_protected_paths_dirty_still_blocks_with_ordinary_result(
    tmp_path: Path,
) -> None:
    """D1 regression: ordinary dirty protected path still returns (branch, paths)."""
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "mod.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/mod.py", cwd=repo)
    _git("commit", "-q", "-m", "track", cwd=repo)
    (repo / "apps" / "mod.py").write_text("x = 2\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/mod.py"]


def test_find_dirty_protected_paths_permitted_carveout_still_applies(
    tmp_path: Path,
) -> None:
    """R4a: dirty path that is BOTH protected AND permitted must not block.

    The pre-fix fixture used an untracked ``docs/specs/charter.md`` that
    ``is_branch_isolation_protected_path`` already excludes, so the
    ``find_permitted_main_surface`` continue-arm was never reached. Rebuild
    so the candidate is apps/*.py (protected) and listed in
    ``permitted_main_surfaces`` (carve-out). Removing the carve-out must flip
    the result to a dirty-path block.
    """
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    target = repo / "apps" / "allowed_on_main.py"
    target.write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/allowed_on_main.py", cwd=repo)
    _git("commit", "-q", "-m", "track permitted protected path", cwd=repo)
    target.write_text("x = 2\n", encoding="utf-8")

    policy = BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=(),
        protected_main_surfaces=(),
        permitted_main_surfaces=(
            MainSurfacePattern(
                pattern="apps/allowed_on_main.py",
                reason="carve-out keeps this protected path editable on main",
            ),
        ),
        state_dirty_surfaces=(),
        first_edit_protected_surfaces=(),
    )
    # Fixture must exercise both arms: protected AND permitted under the same
    # policy evaluation (otherwise deleting the carve-out leaves the suite green).
    assert is_branch_isolation_protected_path("apps/allowed_on_main.py", policy)
    assert find_permitted_main_surface("apps/allowed_on_main.py", policy) is not None

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=policy,
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, f"permitted carve-out must not block; got {result!r}"


def test_find_dirty_protected_paths_skip_worktree_divergent_blocks(
    tmp_path: Path,
) -> None:
    """R1: skip-worktree + disk!=HEAD on a protected path must BLOCK.

    Porcelain is empty under ``--skip-worktree``; pre-fix trusted porcelain
    alone and returned None (ALLOW). Index-tag compensation must fail closed.
    """
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    target = repo / "apps" / "secret.py"
    target.write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/secret.py", cwd=repo)
    _git("commit", "-q", "-m", "track secret", cwd=repo)
    _git("update-index", "--skip-worktree", "apps/secret.py", cwd=repo)
    target.write_text("x = divergent-skip\n", encoding="utf-8")

    scoped = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all", "--", "apps/secret.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert scoped.returncode == 0
    assert scoped.stdout.strip() == "", scoped.stdout
    tag = subprocess.run(
        ["git", "ls-files", "-v", "--", "apps/secret.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout.strip()
    assert tag.startswith("S "), tag

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "skip-worktree divergent protected path must not allow"
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/secret.py"], paths


def test_find_dirty_protected_paths_assume_unchanged_divergent_blocks(
    tmp_path: Path,
) -> None:
    """R1: assume-unchanged + disk!=HEAD on a protected path must BLOCK.

    Porcelain is empty under ``--assume-unchanged``; pre-fix returned None.
    """
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    target = repo / "apps" / "secret.py"
    target.write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/secret.py", cwd=repo)
    _git("commit", "-q", "-m", "track secret", cwd=repo)
    _git("update-index", "--assume-unchanged", "apps/secret.py", cwd=repo)
    target.write_text("x = divergent-assume\n", encoding="utf-8")

    scoped = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all", "--", "apps/secret.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert scoped.returncode == 0
    assert scoped.stdout.strip() == "", scoped.stdout
    tag = subprocess.run(
        ["git", "ls-files", "-v", "--", "apps/secret.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout.strip()
    assert tag and tag[0].islower(), tag

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "assume-unchanged divergent protected path must not allow"
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/secret.py"], paths


def test_find_dirty_protected_paths_tag_map_unavailable_degrades(
    tmp_path: Path, monkeypatch
) -> None:
    """R1: when the index-tag map cannot be obtained, degrade (never allow)."""
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/ok.py", cwd=repo)
    _git("commit", "-q", "-m", "track", cwd=repo)

    monkeypatch.setattr(guard, "_batch_ls_files_v_tags", lambda *_a, **_k: None)

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert isinstance(result, guard.DirtyProtectedDegraded), result
    assert result.branch == "main"


def test_find_dirty_protected_paths_subdir_repo_root_maps_porcelain(
    tmp_path: Path,
) -> None:
    """R2: porcelain is top-level-relative; subdir repo_root must still block.

    Pre-fix passed toplevel-prefixed paths to the policy matcher, which
    missed ``apps/`` prefixes and returned None (ALLOW).
    """
    repo = _init_repo(tmp_path)
    sub = repo / "pkg"
    (sub / "apps").mkdir(parents=True)
    target = sub / "apps" / "mod.py"
    target.write_text("x = 1\n", encoding="utf-8")
    _git("add", "pkg/apps/mod.py", cwd=repo)
    _git("commit", "-q", "-m", "track nested protected", cwd=repo)
    target.write_text("x = 2\n", encoding="utf-8")

    # Control: porcelain under -C sub is still top-level-relative.
    porcelain = _git("status", "--porcelain=v1", cwd=sub)
    assert "pkg/apps/mod.py" in porcelain or "apps/mod.py" in porcelain, porcelain

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(sub),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "subdir repo_root must not drop toplevel porcelain paths"
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/mod.py"], paths


def test_find_dirty_protected_paths_unreconciled_bases_degrade(
    tmp_path: Path, monkeypatch
) -> None:
    """R2: when path bases cannot be reconciled, degrade — never return []."""
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "mod.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/mod.py", cwd=repo)
    _git("commit", "-q", "-m", "track", cwd=repo)
    (repo / "apps" / "mod.py").write_text("x = 2\n", encoding="utf-8")

    monkeypatch.setattr(guard, "_repo_prefix_under_toplevel", lambda *_a, **_k: None)

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert isinstance(result, guard.DirtyProtectedDegraded), result
    assert result.branch == "main"


def test_find_dirty_protected_paths_empty_repo_root_degrades_on_protected_branch() -> None:
    """R3: falsy repo_root on a protected branch is degrade, not allow."""
    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root="",
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert isinstance(result, guard.DirtyProtectedDegraded), result
    assert result.branch == "main"


def test_find_dirty_protected_paths_mixed_dirty_only_protected_reported(
    tmp_path: Path,
) -> None:
    """R4b: dirty non-protected sibling must not be reported; only protected path.

    Pins the ``is_branch_isolation_protected_path`` filter: treating every
    dirty path as protected would still leave older single-path tests green.
    """
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "secret.py").write_text("x = 1\n", encoding="utf-8")
    (repo / "notes.txt").write_text("notes\n", encoding="utf-8")
    _git("add", "apps/secret.py", "notes.txt", cwd=repo)
    _git("commit", "-q", "-m", "track mixed", cwd=repo)
    (repo / "apps" / "secret.py").write_text("x = 2\n", encoding="utf-8")
    (repo / "notes.txt").write_text("notes dirty\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/secret.py"], paths
    assert "notes.txt" not in paths


def test_find_dirty_protected_paths_non_protected_branch_allows(
    tmp_path: Path,
) -> None:
    """R4c: non-protected branch short-circuits to None even with dirty protected code."""
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "x.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", "apps/x.py", cwd=repo)
    _git("commit", "-q", "-m", "track", cwd=repo)
    (repo / "apps" / "x.py").write_text("x = 2\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="feature/x",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None


def test_git_dirty_paths_reports_modified_cr_path(tmp_path: Path) -> None:
    """D2 pin: bytes-mode -z must surface a tracked MODIFIED path containing CR.

    A regression that reverts ``text=False`` only on ``_git_dirty_paths`` would
    rewrite CR→LF and miss the filesystem path (or report a LF-mutated form).
    """
    repo = _init_repo(tmp_path)
    rel = "apps/weird\rname.py"
    target = repo / rel
    target.parent.mkdir(parents=True)
    target.write_text("x = 1\n", encoding="utf-8")
    _git("add", "--", rel, cwd=repo)
    _git("commit", "-q", "-m", "track cr path", cwd=repo)
    target.write_text("x = 2\n", encoding="utf-8")

    dirty = guard._git_dirty_paths(repo)
    assert dirty is not None
    assert rel in dirty, f"modified CR path must appear in dirty set; got {dirty!r}"


def test_git_dirty_paths_omits_clean_cr_path(tmp_path: Path) -> None:
    """D2 pin: tracked CLEAN path containing CR must not appear as dirty."""
    repo = _init_repo(tmp_path)
    rel = "apps/clean\rname.py"
    target = repo / rel
    target.parent.mkdir(parents=True)
    target.write_text("x = 1\n", encoding="utf-8")
    _git("add", "--", rel, cwd=repo)
    _git("commit", "-q", "-m", "track clean cr path", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", porcelain

    dirty = guard._git_dirty_paths(repo)
    assert dirty is not None
    assert dirty == [], f"clean CR path must not be dirty; got {dirty!r}"


def test_find_dirty_protected_paths_discriminates_cr_modified_vs_clean(
    tmp_path: Path,
) -> None:
    """D2 pin: protected-path gate must list modified CR path and omit clean CR path."""
    repo = _init_repo(tmp_path)
    dirty_rel = "apps/dirty\rfile.py"
    clean_rel = "apps/clean\rfile.py"
    for rel, body in ((dirty_rel, "a\n"), (clean_rel, "b\n")):
        target = repo / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
        _git("add", "--", rel, cwd=repo)
    _git("commit", "-q", "-m", "track cr pair", cwd=repo)
    (repo / dirty_rel).write_text("a-modified\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    _branch, paths = result
    assert dirty_rel in paths, paths
    assert clean_rel not in paths, paths


# ---------------------------------------------------------------------------
# ROUND 8 — on-disk walk must not flag untracked gitignored protected paths
# ---------------------------------------------------------------------------


def test_find_dirty_protected_paths_gitignored_on_disk_allows(tmp_path: Path) -> None:
    """R8 regression: clean main + gitignored venv under code_roots must ALLOW.

    Round 7's ``_collect_on_disk_protected_relpaths`` walk unions every
    on-disk protected-extension file into candidates without filtering
    untracked/ignored paths. An untracked file has no ``H`` index tag, so
    ``_is_tracked_clean_no_suppress`` cannot prove it clean and the path
    falls through into dirty_paths — blocking a completely clean main.

    Fixture: committed ``.gitignore`` with ``.venv/``, untracked
    ``apps/.venv/lib/x.py``, plus a tracked clean ``apps/ok.py``. Worktree
    porcelain is empty. Must return None (ALLOW).
    """
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (repo / "apps" / ".venv" / "lib").mkdir(parents=True)
    (repo / "apps" / ".venv" / "lib" / "x.py").write_text("x = 1\n", encoding="utf-8")
    (repo / "apps" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", ".gitignore", "apps/ok.py", cwd=repo)
    _git("commit", "-q", "-m", "track gitignore + clean protected", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", f"fixture must be clean; got {porcelain!r}"
    # Control: path is ignored and untracked (not a tracked clean file).
    ignored = subprocess.run(
        ["git", "check-ignore", "-v", "--", "apps/.venv/lib/x.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert ignored.returncode == 0, ignored.stdout + ignored.stderr
    untracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", "apps/.venv/lib/x.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert untracked.returncode != 0

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, (
        "clean main with only gitignored untracked protected-extension files "
        f"must ALLOW; got {result!r}"
    )


def test_find_dirty_protected_paths_tracked_modified_still_blocks_control(
    tmp_path: Path,
) -> None:
    """R8 control: tracked + genuinely modified protected path must still BLOCK.

    Same fixture shape as the gitignored ALLOW regression (gitignore + clean
    tree setup), but a tracked ``apps/mod.py`` is modified. Proves the
    untracked/ignored filter did not disable ``find_dirty_protected_paths``.
    """
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (repo / "apps" / ".venv" / "lib").mkdir(parents=True)
    (repo / "apps" / ".venv" / "lib" / "x.py").write_text("x = 1\n", encoding="utf-8")
    (repo / "apps" / "mod.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", ".gitignore", "apps/mod.py", cwd=repo)
    _git("commit", "-q", "-m", "track for control", cwd=repo)
    (repo / "apps" / "mod.py").write_text("x = 2\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "tracked modified protected path must BLOCK"
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/mod.py"], paths
    assert "apps/.venv/lib/x.py" not in paths, paths


# ---------------------------------------------------------------------------
# ROUND 9 — ignored inventory via ls-files; never walk symlink directories
# ---------------------------------------------------------------------------


def _r9_symlink_and_ignored_fixture(repo: Path) -> None:
    """Build the R9 synthetic tree: tracked clean code, gitignored venv, symlink dir.

    Layout (all under a real git repo in tmp_path — not the checkout layout):

    * tracked ``apps/ok.py`` (protected, clean)
    * ``.gitignore`` → ``.venv-x/`` with untracked ``apps/.venv-x/lib/foo.py``
    * real payload dir + symlinked ``apps/linked_hooks/`` containing a ``.py``
      file — the path shape that makes bare ``check-ignore --stdin`` exit 128
      and that a symlink-following walk would false-nominate
    """
    (repo / ".gitignore").write_text(".venv-x/\n", encoding="utf-8")
    (repo / "apps" / ".venv-x" / "lib").mkdir(parents=True)
    (repo / "apps" / ".venv-x" / "lib" / "foo.py").write_text(
        "foo = 1\n", encoding="utf-8"
    )
    (repo / "apps" / "ok.py").write_text("ok = 1\n", encoding="utf-8")
    payload = repo / "payload" / "hooks"
    payload.mkdir(parents=True)
    (payload / "token_budget.py").write_text("token = 1\n", encoding="utf-8")
    link = repo / "apps" / "linked_hooks"
    # Relative target so the tracked symlink stays valid under tmp_path moves.
    os.symlink(os.path.relpath(payload, start=link.parent), link)
    _git(
        "add",
        ".gitignore",
        "apps/ok.py",
        "apps/linked_hooks",
        "payload/hooks/token_budget.py",
        cwd=repo,
    )
    _git("commit", "-q", "-m", "r9 fixture: clean + ignore + payload", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", f"r9 fixture must be clean; got {porcelain!r}"


def test_find_dirty_protected_paths_r9_clean_with_symlink_and_ignored_allows(
    tmp_path: Path,
) -> None:
    """R9: clean main + gitignored + symlink-dir file must ALLOW.

    Pins both round-9 defects together:

    1. Ignored untracked paths (``.venv-x``) must drop via
       ``ls-files --others --ignored`` inventory, not ``check-ignore --stdin``
       (which exits 128 when any pathspec traverses a symlink dir).
    2. Files only reachable through a symlinked directory under a code root
       must not be nominated by the on-disk walk.
    """
    repo = _init_repo(tmp_path)
    _r9_symlink_and_ignored_fixture(repo)

    # Control: single-path ignore still works for the venv file.
    assert guard._is_untracked_ignored_path(repo, "apps/.venv-x/lib/foo.py") is True
    # Control: batch helper must also classify it ignored (defect-1 instrument).
    batch = guard._batch_untracked_ignored_paths(
        repo,
        [
            "apps/ok.py",
            "apps/.venv-x/lib/foo.py",
            "apps/linked_hooks/token_budget.py",
        ],
    )
    assert "apps/.venv-x/lib/foo.py" in batch, batch

    # Walk must not emit the symlink-dir path (defect-2 instrument).
    on_disk = guard._collect_on_disk_protected_relpaths(repo, _protected_code_policy())
    assert "apps/linked_hooks/token_budget.py" not in on_disk, on_disk
    assert "apps/.venv-x/lib/foo.py" in on_disk, on_disk  # present before ignore filter

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, (
        "clean main with gitignored + symlink-dir artifacts must ALLOW; "
        f"got {result!r}"
    )


def test_find_dirty_protected_paths_r9_tracked_modified_still_blocks_inverse(
    tmp_path: Path,
) -> None:
    """R9 inverse control: genuine tracked dirt must still BLOCK and name the file.

    Same symlink + gitignore fixture as the ALLOW case; additionally modify a
    tracked protected file. A fix that returns ALLOW unconditionally fails here.
    The symlinked-dir path and the gitignored path must not appear in the block
    set.
    """
    repo = _init_repo(tmp_path)
    _r9_symlink_and_ignored_fixture(repo)
    (repo / "apps" / "ok.py").write_text("ok = 2\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "tracked modified protected path must BLOCK"
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    branch, paths = result
    assert branch == "main"
    assert "apps/ok.py" in paths, paths
    assert "apps/.venv-x/lib/foo.py" not in paths, paths
    assert "apps/linked_hooks/token_budget.py" not in paths, paths


def test_find_dirty_protected_paths_r9_symlink_code_root_not_walked(
    tmp_path: Path,
) -> None:
    """R9: when a code_root *is* a symlink, the walk must not enter it."""
    repo = _init_repo(tmp_path)
    real = repo / "payload" / "hooks"
    real.mkdir(parents=True)
    (real / "token_budget.py").write_text("token = 1\n", encoding="utf-8")
    (repo / "apps").mkdir(exist_ok=True)
    (repo / "apps" / "ok.py").write_text("ok = 1\n", encoding="utf-8")
    os.symlink(
        os.path.relpath(real, start=(repo / "apps").as_posix()),
        repo / "apps" / "hooks_link",
    )
    _git(
        "add",
        "apps/ok.py",
        "apps/hooks_link",
        "payload/hooks/token_budget.py",
        cwd=repo,
    )
    _git("commit", "-q", "-m", "symlink code root fixture", cwd=repo)

    policy = BranchIsolationPolicy(
        code_roots=("apps/", "apps/hooks_link/"),
        protected_extensions=(".py",),
        root_protected_files=(),
        protected_main_surfaces=(
            MainSurfacePattern(pattern="apps/**", reason="application code"),
        ),
        permitted_main_surfaces=(),
        state_dirty_surfaces=(),
        first_edit_protected_surfaces=(),
    )
    on_disk = guard._collect_on_disk_protected_relpaths(repo, policy)
    assert "apps/ok.py" in on_disk, on_disk
    assert "apps/hooks_link/token_budget.py" not in on_disk, on_disk

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=policy,
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, result


# ---------------------------------------------------------------------------
# ROUND 10 — ignored inventory degrade is fail-closed (not empty-set allow)
# ---------------------------------------------------------------------------


def test_find_dirty_protected_paths_inventory_degrade_returns_degraded(
    tmp_path: Path, monkeypatch
) -> None:
    """R10 pin: ignored-inventory degrade -> DirtyProtectedDegraded, not paths.

    Pre-fix returned an empty set on inventory timeout/non-zero, which the
    caller treated as "nothing ignored" and false-blocked clean trees with
    thousands of gitignored candidates. Only the inventory call degrades;
    toplevel / dirty-paths / tag-map probes stay healthy so this gate is
    reached.
    """
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (repo / "apps" / ".venv" / "lib").mkdir(parents=True)
    (repo / "apps" / ".venv" / "lib" / "x.py").write_text("x = 1\n", encoding="utf-8")
    (repo / "apps" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", ".gitignore", "apps/ok.py", cwd=repo)
    _git("commit", "-q", "-m", "r10 inventory degrade fixture", cwd=repo)

    real_degraded = guard._run_git_degraded

    def _degrade_inventory_only(args, **kwargs):  # noqa: ANN001, ANN003
        # Inventory is the argv-only ls-files --others --ignored batch.
        if (
            "ls-files" in args
            and "--others" in args
            and "--ignored" in args
            and "--exclude-standard" in args
        ):
            return None
        return real_degraded(args, **kwargs)

    monkeypatch.setattr(guard, "_run_git_degraded", _degrade_inventory_only)

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "inventory degrade must not allow (None)"
    assert isinstance(result, guard.DirtyProtectedDegraded), (
        f"inventory degrade must be DirtyProtectedDegraded, not ordinary "
        f"dirty-paths tuple; got {result!r}"
    )
    assert not isinstance(result, tuple), result
    assert result.branch == "main"


def test_find_dirty_protected_paths_inventory_success_gitignored_allows(
    tmp_path: Path,
) -> None:
    """R10: successful inventory + gitignored on-disk paths must ALLOW (None)."""
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (repo / "apps" / ".venv" / "lib").mkdir(parents=True)
    (repo / "apps" / ".venv" / "lib" / "x.py").write_text("x = 1\n", encoding="utf-8")
    (repo / "apps" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", ".gitignore", "apps/ok.py", cwd=repo)
    _git("commit", "-q", "-m", "r10 allow fixture", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", porcelain

    batch = guard._batch_untracked_ignored_paths(
        repo, ["apps/ok.py", "apps/.venv/lib/x.py"]
    )
    assert batch is not None, "healthy inventory must not degrade"
    assert "apps/.venv/lib/x.py" in batch, batch

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, (
        f"clean main with only gitignored on-disk protected files must ALLOW; "
        f"got {result!r}"
    )


def test_find_dirty_protected_paths_inventory_fix_tracked_modified_still_blocks(
    tmp_path: Path,
) -> None:
    """R10 inverse control: tracked modified protected path still ordinary BLOCK.

    Proves the inventory fail-closed fix did not disable the guard.
    """
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (repo / "apps" / ".venv" / "lib").mkdir(parents=True)
    (repo / "apps" / ".venv" / "lib" / "x.py").write_text("x = 1\n", encoding="utf-8")
    (repo / "apps" / "mod.py").write_text("x = 1\n", encoding="utf-8")
    _git("add", ".gitignore", "apps/mod.py", cwd=repo)
    _git("commit", "-q", "-m", "r10 inverse fixture", cwd=repo)
    (repo / "apps" / "mod.py").write_text("x = 2\n", encoding="utf-8")

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "tracked modified protected path must BLOCK"
    assert not isinstance(result, guard.DirtyProtectedDegraded), result
    assert isinstance(result, tuple), result
    branch, paths = result
    assert branch == "main"
    assert paths == ["apps/mod.py"], paths
    assert "apps/.venv/lib/x.py" not in paths, paths


def test_find_dirty_protected_paths_symlink_dir_ancestor_filter_is_the_gate(
    tmp_path: Path, monkeypatch
) -> None:
    """R10: ``_path_has_symlink_ancestor`` is the only gate for symlink-dir paths.

    Existing R9 symlink tests stayed green when this helper was neutered to
    ``return False``. Fixture: a tracked symlink *directory* under a real
    code root, with a root-protected path that exists only through that
    link. Real helper -> ALLOW; neutered helper -> ordinary BLOCK.
    """
    repo = _init_repo(tmp_path)
    real = repo / "payload" / "hooks"
    real.mkdir(parents=True)
    (real / "evil.py").write_text("evil = 1\n", encoding="utf-8")
    (repo / "apps").mkdir(exist_ok=True)
    (repo / "apps" / "ok.py").write_text("ok = 1\n", encoding="utf-8")
    link = repo / "apps" / "linked"
    os.symlink(os.path.relpath(real, start=link.parent), link)
    _git(
        "add",
        "apps/ok.py",
        "apps/linked",
        "payload/hooks/evil.py",
        cwd=repo,
    )
    _git("commit", "-q", "-m", "r10 symlink-dir ancestor fixture", cwd=repo)
    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", porcelain

    # Path only exists through the symlinked directory under code_root.
    through_link = "apps/linked/evil.py"
    assert (repo / through_link).is_file()
    assert guard._path_has_symlink_ancestor(repo, repo / through_link) is True

    policy = BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=(through_link,),
        protected_main_surfaces=(
            MainSurfacePattern(pattern="apps/**", reason="application code"),
        ),
        permitted_main_surfaces=(),
        state_dirty_surfaces=(),
        first_edit_protected_surfaces=(),
    )

    on_disk = guard._collect_on_disk_protected_relpaths(repo, policy)
    assert through_link not in on_disk, on_disk
    assert "apps/ok.py" in on_disk, on_disk

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=policy,
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is None, (
        f"symlink-dir path must not false-block; got {result!r}"
    )

    # Mutation pin: neutering the helper must re-nominate the path and BLOCK.
    monkeypatch.setattr(guard, "_path_has_symlink_ancestor", lambda *_a, **_k: False)
    on_disk_neutered = guard._collect_on_disk_protected_relpaths(repo, policy)
    assert through_link in on_disk_neutered, on_disk_neutered
    neutered = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=policy,
        protected_branches=frozenset({"main", "master"}),
    )
    assert neutered is not None, "neutered symlink filter must not ALLOW"
    assert not isinstance(neutered, guard.DirtyProtectedDegraded), neutered
    _branch, paths = neutered
    assert through_link in paths, paths


def test_find_dirty_protected_paths_raw_dirty_none_fail_closed(
    tmp_path: Path, monkeypatch
) -> None:
    """R10 pin: ``raw_dirty is None`` branch must return DirtyProtectedDegraded.

    The older degrade test stubs ``subprocess.run`` globally so
    ``_git_toplevel`` fails first and the ``raw_dirty`` gate is never
    reached. Degrade only ``_git_dirty_paths``; leave toplevel + prefix
    healthy.
    """
    repo = _init_repo(tmp_path)
    (repo / "apps").mkdir()
    (repo / "apps" / "ok.py").write_text("ok = 1\n", encoding="utf-8")
    _git("add", "apps/ok.py", cwd=repo)
    _git("commit", "-q", "-m", "r10 raw_dirty fixture", cwd=repo)

    # Prove the earlier gates succeed with the real helpers.
    assert guard._git_toplevel(repo) is not None
    toplevel = guard._git_toplevel(repo)
    assert guard._repo_prefix_under_toplevel(repo, toplevel) is not None

    monkeypatch.setattr(guard, "_git_dirty_paths", lambda _repo: None)

    result = guard.find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_protected_code_policy(),
        protected_branches=frozenset({"main", "master"}),
    )
    assert result is not None, "raw_dirty None must not allow"
    assert isinstance(result, guard.DirtyProtectedDegraded), (
        f"raw_dirty None must hit DirtyProtectedDegraded; got {result!r}"
    )
    assert result.branch == "main"
