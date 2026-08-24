"""Root-worktree branch-switch guard (internal / A1).

Drives ``guard-bash-main-branch.py`` as a subprocess against tmp repos that
mirror the misroute configuration: a PRIMARY checkout on main with at least one
LINKED worktree. In that configuration a branch *creation/switch* that targets
the primary worktree must hard-block (exit 2) so a concurrent session's
main-integration commit can never land on a feature branch. Single-worktree
consumers (the normal `git checkout -b` workflow) must stay unaffected.

HOOK_SCRIPT uses the sibling path (not the materialized-only parents[] form the
lightweight test uses) so these run from the payload source tree too.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


HOOK_SCRIPT = Path(__file__).resolve().parent / "guard-bash-main-branch.py"


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    (path / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "-A", cwd=path)
    _git("commit", "-q", "-m", "init", cwd=path)


@pytest.fixture
def primary_with_linked(tmp_path: Path) -> tuple[Path, Path]:
    """A primary repo on main plus one linked worktree (multi-worktree config)."""
    primary = tmp_path / "primary"
    primary.mkdir()
    _init_repo(primary)
    linked = tmp_path / "primary-linked"
    _git("worktree", "add", "-q", str(linked), "-b", "feature/seed", cwd=primary)
    return primary, linked


@pytest.fixture
def single_worktree(tmp_path: Path) -> Path:
    repo = tmp_path / "solo"
    repo.mkdir()
    _init_repo(repo)
    return repo


def _invoke(cwd: Path, command: str, *, extra_env: dict | None = None) -> subprocess.CompletedProcess[str]:
    payload = {"toolName": "Bash", "toolInput": {"command": command}}
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        cwd=cwd,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


# --- BLOCK: branch creation / switch that targets the primary worktree --------


def test_checkout_b_in_primary_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -b feature/x")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr and "PRIMARY" in proc.stderr


def test_switch_c_in_primary_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch -c feature/x")
    assert proc.returncode == 2, proc.stderr


def test_plain_switch_to_nonmain_in_primary_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch feature/seed")
    assert proc.returncode == 2, proc.stderr


def test_cross_worktree_dashC_from_linked_blocked(primary_with_linked) -> None:
    # GPR-1: `git -C <primary> checkout -b` issued from a LINKED worktree cwd
    # (on a feature branch) is the real misroute and must still be caught.
    primary, linked = primary_with_linked
    proc = _invoke(linked, f"git -C {primary} checkout -b feature/x")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_cd_to_primary_from_linked_blocked(primary_with_linked) -> None:
    primary, linked = primary_with_linked
    proc = _invoke(linked, f"cd {primary} && git checkout -b feature/x")
    assert proc.returncode == 2, proc.stderr


# --- ALLOW: switch-to-main, worktree add, bypass, ambiguous checkout ----------


def test_checkout_main_allowed(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout main")
    assert proc.returncode == 0, proc.stderr


def test_switch_main_allowed(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch main")
    assert proc.returncode == 0, proc.stderr


def test_worktree_add_allowed(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git worktree add ../wt2 -b feature/y")
    assert proc.returncode == 0, proc.stderr


def test_inline_bypass_token_allows(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "WORKBAY_ALLOW_ROOT_BRANCH_SWITCH=1 git checkout -b feature/x")
    assert proc.returncode == 0, proc.stderr
    assert "bypass" in proc.stderr.lower()


def test_ambiguous_checkout_path_not_treated_as_switch(primary_with_linked) -> None:
    # `git checkout -- <file>` (no -b) is a restore, not a branch switch; the
    # switch-guard must not fire (README.md is unprotected so write-scan passes).
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -- README.md")
    assert proc.returncode == 0, proc.stderr


# --- CONSUMER-SAFE: single-worktree repos keep the normal branch workflow -----


def test_single_worktree_checkout_b_allowed(single_worktree: Path) -> None:
    # No linked worktrees ⇒ no concurrent session to strand ⇒ normal workflow.
    proc = _invoke(single_worktree, "git checkout -b feature/x")
    assert proc.returncode == 0, proc.stderr
    assert "BLOCKED" not in proc.stderr


def test_detect_root_branch_switch_valueerror_fails_closed(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AGT-10: identity helper ValueError must fail closed, not propagate.

    A PreToolUse hook that crashes exits 1 (non-blocking error), which lets the
    command proceed and skips the protected-path write scan. Probe failure must
    return the could-not-determine message instead of raising.
    """
    import importlib.util

    primary, _ = primary_with_linked
    hooks_dir = str(HOOK_SCRIPT.parent)
    if hooks_dir not in sys.path:
        sys.path.insert(0, hooks_dir)

    import _worktree_identity

    def _boom(_repo_root: Path) -> Path | None:
        raise ValueError("identity probe exploded")

    monkeypatch.setattr(_worktree_identity, "primary_workspace_root", _boom)

    spec = importlib.util.spec_from_file_location(
        "guard_bash_main_branch_under_test", HOOK_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    msg = mod._detect_root_branch_switch(
        "git checkout -b feature/x",
        repo_root=primary,
    )
    assert msg is not None, (
        "ValueError from identity helper must fail closed with a message, "
        "not return None (silent permit) or raise"
    )
    assert "could not complete the root-worktree branch-switch check" in msg


def test_detect_root_branch_switch_tilde_user_fails_closed(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """AGT-10: switch-intent parse RuntimeError must fail closed, not raise.

    ``cd ~nosuchuser`` makes Path.expanduser() raise RuntimeError. That call
    previously sat outside the fail-closed try, so the hook crashed (exit 1,
    non-blocking) and skipped the write scan. Must return could-not-determine.
    """
    import importlib.util

    primary, _ = primary_with_linked
    hooks_dir = str(HOOK_SCRIPT.parent)
    if hooks_dir not in sys.path:
        sys.path.insert(0, hooks_dir)

    spec = importlib.util.spec_from_file_location(
        "guard_bash_main_branch_tilde_user", HOOK_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    msg = mod._detect_root_branch_switch(
        "cd ~nosuchuser && git checkout -b feature/x",
        repo_root=primary,
    )
    assert msg is not None, (
        "tilde-user expanduser RuntimeError must fail closed with a message, "
        "not return None (silent permit) or raise"
    )
    assert "could not complete the root-worktree branch-switch check" in msg
    assert "switch-intent parse" in msg


def test_detect_root_branch_switch_toplevel_timeout_fails_closed(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-08: _worktree_toplevel TimeoutExpired must fail closed, not skip.

    TimeoutExpired is a SubprocessError subclass; treating it like a non-git
    path returns None and silently skips the intent that may target primary.
    Must surface could-not-determine for the whole scan via _BUDGET_EXHAUSTED.
    """
    import importlib.util

    primary, linked = primary_with_linked
    hooks_dir = str(HOOK_SCRIPT.parent)
    if hooks_dir not in sys.path:
        sys.path.insert(0, hooks_dir)

    spec = importlib.util.spec_from_file_location(
        "guard_bash_main_branch_toplevel_timeout", HOOK_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    def _timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="git", timeout=1.0)

    monkeypatch.setattr(mod.subprocess, "run", _timeout)

    # Cross-worktree form forces _worktree_toplevel(target_dir) rather than cwd.
    msg = mod._detect_root_branch_switch(
        f"git -C {primary} checkout -b feature/x",
        repo_root=linked,
    )
    assert msg is not None, (
        "TimeoutExpired from _worktree_toplevel must fail closed with a message, "
        "not return None (silent skip of primary-targeting intent) or raise"
    )
    assert "could not complete the root-worktree branch-switch check" in msg
    assert "subprocess deadline exhausted" in msg


def _load_guard_module(name: str):
    import importlib.util

    hooks_dir = str(HOOK_SCRIPT.parent)
    if hooks_dir not in sys.path:
        sys.path.insert(0, hooks_dir)
    spec = importlib.util.spec_from_file_location(name, HOOK_SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_primary_none_single_worktree_allows_switch(
    single_worktree: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D1 (9932): unresolvable primary must not block single-worktree consumers.

    primary_workspace_root returns None for bare-backed worktrees, submodules,
    and other layouts. That used to fail closed before the has_linked_worktrees
    scope gate, permanently blocking `git switch -c` until an env bypass.
    """
    import _worktree_identity

    mod = _load_guard_module("guard_bash_d1_single")
    monkeypatch.setattr(
        _worktree_identity, "primary_workspace_root", lambda _r: None
    )

    msg = mod._detect_root_branch_switch(
        "git checkout -b feature/x",
        repo_root=single_worktree,
    )
    assert msg is None, (
        "primary_raw is None outside multi-worktree scope must allow, not block; "
        f"got message={msg!r}"
    )


def test_primary_none_with_worktrees_layout_fails_closed(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D1 (9932): unresolvable primary still fails closed when in multi-worktree scope."""
    import _worktree_identity

    primary, _ = primary_with_linked
    mod = _load_guard_module("guard_bash_d1_multi")
    monkeypatch.setattr(
        _worktree_identity, "primary_workspace_root", lambda _r: None
    )

    msg = mod._detect_root_branch_switch(
        "git checkout -b feature/x",
        repo_root=primary,
    )
    assert msg is not None, (
        "primary_raw is None inside multi-worktree scope must fail closed"
    )
    assert "could not determine primary worktree" in msg


def test_empty_git_dir_litter_does_not_fail_open_multi_worktree_scope(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty ``.git`` between probe cwd and multi-worktree root must not allow.

    ``_git_worktrees_dir_reachable`` previously accepted any directory named
    ``.git`` and returned False when it lacked ``worktrees/``. The caller
    treats False as out-of-scope allow, so a stray empty ``.git`` directory
    silently disabled the primary-worktree branch-switch guard on a genuine
    multi-worktree checkout. Litter must not short-circuit the walk.
    """
    import _worktree_identity

    primary, _ = primary_with_linked
    nested = primary / "nested" / "probe"
    nested.mkdir(parents=True)
    # Empty directory named .git — no HEAD, objects, or refs.
    (primary / "nested" / ".git").mkdir()

    mod = _load_guard_module("guard_bash_empty_git_litter")
    assert mod._git_worktrees_dir_reachable(nested) is True, (
        "empty .git litter must not make multi-worktree layout unreachable"
    )

    monkeypatch.setattr(
        _worktree_identity, "primary_workspace_root", lambda _r: None
    )
    msg = mod._detect_root_branch_switch(
        "git checkout -b feature/x",
        repo_root=nested,
    )
    assert msg is not None, (
        "primary_raw is None with multi-worktree layout beyond empty .git litter "
        "must fail closed, not take the out-of-scope allow path"
    )
    assert "could not determine primary worktree" in msg


# --- DEFECT PINS: _git_worktrees_dir_reachable litter must not fail-open ------


def _probe_under_primary(primary: Path) -> Path:
    """Nested probe cwd below *primary*; caller plants litter at nested/.git."""
    nested = primary / "nested" / "probe"
    nested.mkdir(parents=True, exist_ok=True)
    return nested


def _litter_git_path(primary: Path) -> Path:
    return primary / "nested" / ".git"


def _plant_valid_head(git_dir: Path, content: str = "ref: refs/heads/main\n") -> None:
    git_dir.mkdir(parents=True, exist_ok=True)
    (git_dir / "HEAD").write_text(content, encoding="utf-8")


def _plant_objects_refs(common: Path) -> None:
    (common / "objects").mkdir(parents=True, exist_ok=True)
    (common / "refs").mkdir(parents=True, exist_ok=True)


def test_worktrees_reachable_positive_control_no_litter(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Harness control: multi-worktree primary with no litter must be reachable."""
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    mod = _load_guard_module("guard_bash_wt_control")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "positive control with no litter must find primary .git/worktrees; "
        "if this fails the harness is broken and litter pins prove nothing"
    )


# --- GROUP A: linked-worktree gitfile layout (positive + near-miss) ------------


def test_linked_worktree_gitfile_layout_is_reachable(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Real ``git worktree add`` gitfile must return True via the layout check.

    Pins the exact ``…/.git/worktrees/<name>`` comparison on the gitfile arm.
    The directory-``.git`` positive control above never exercises this path;
    inverting ``parent.name == \"worktrees\"`` must fail this test.
    """
    _primary, linked = primary_with_linked
    gitfile = linked / ".git"
    assert gitfile.is_file(), "linked worktree must use a gitfile, not a git dir"
    raw = gitfile.read_bytes().decode("utf-8").rstrip("\r\n")
    assert raw.startswith("gitdir: "), raw
    payload = Path(raw[len("gitdir: ") :])
    assert payload.parent.name == "worktrees", payload
    assert payload.parent.parent.name == ".git", payload

    mod = _load_guard_module("guard_bash_linked_gitfile_true")
    assert mod._git_worktrees_dir_reachable(linked) is True, (
        "genuine linked-worktree gitfile must be in multi-worktree scope"
    )
    nested = linked / "nested" / "probe"
    nested.mkdir(parents=True)
    assert mod._git_worktrees_dir_reachable(nested) is True, (
        "ascent from nested cwd under a linked worktree must still hit the gitfile"
    )


def _plant_gitfile_pointing_at(probe_root: Path, gitdir_target: Path) -> None:
    """Write *probe_root*/.git as a gitfile whose payload is *gitdir_target*."""
    probe_root.mkdir(parents=True, exist_ok=True)
    (probe_root / ".git").write_text(
        f"gitdir: {gitdir_target}\n", encoding="utf-8"
    )


def test_gitfile_layout_rejects_one_level_too_shallow(tmp_path: Path) -> None:
    """``…/.git/<name>`` (missing ``worktrees/``) must not count as linked."""
    probe = tmp_path / "probe"
    # One level too shallow: parent is ``.git``, not ``worktrees``.
    target = tmp_path / "common" / ".git" / "wtname"
    target.mkdir(parents=True)
    _plant_gitfile_pointing_at(probe, target)
    mod = _load_guard_module("guard_bash_gitfile_shallow")
    assert mod._git_worktrees_dir_reachable(probe) is False, (
        "gitdir one level too shallow (…/.git/<name>) must not match linked layout"
    )


def test_gitfile_layout_rejects_one_level_too_deep(tmp_path: Path) -> None:
    """``…/.git/worktrees/extra/<name>`` must not count as linked."""
    probe = tmp_path / "probe"
    # One level too deep: parent is ``extra``, not ``worktrees``.
    target = tmp_path / "common" / ".git" / "worktrees" / "extra" / "wtname"
    target.mkdir(parents=True)
    _plant_gitfile_pointing_at(probe, target)
    mod = _load_guard_module("guard_bash_gitfile_deep")
    assert mod._git_worktrees_dir_reachable(probe) is False, (
        "gitdir one level too deep must not match linked layout"
    )


def test_gitfile_layout_rejects_worktrees_parent_not_dot_git(tmp_path: Path) -> None:
    """Directory named ``worktrees`` whose parent is not ``.git`` must not match."""
    probe = tmp_path / "probe"
    target = tmp_path / "common" / "not-dot-git" / "worktrees" / "wtname"
    target.mkdir(parents=True)
    _plant_gitfile_pointing_at(probe, target)
    mod = _load_guard_module("guard_bash_gitfile_not_dot_git")
    assert mod._git_worktrees_dir_reachable(probe) is False, (
        "…/not-dot-git/worktrees/<name> must not match linked layout"
    )


def test_gitfile_layout_rejects_inverted_dot_git_under_worktrees(
    tmp_path: Path,
) -> None:
    """``…/worktrees/.git/<name>`` resembles the real shape inverted — reject.

    Parent is ``.git`` (not ``worktrees``); grandparent is ``worktrees``.
    A loosened comparison that only checks for those names anywhere in the
    chain would wrongly accept this.
    """
    probe = tmp_path / "probe"
    target = tmp_path / "common" / "worktrees" / ".git" / "wtname"
    target.mkdir(parents=True)
    _plant_gitfile_pointing_at(probe, target)
    mod = _load_guard_module("guard_bash_gitfile_inverted")
    assert mod._git_worktrees_dir_reachable(probe) is False, (
        "inverted …/worktrees/.git/<name> must not match linked layout"
    )


# --- GROUP B: _validate_headref accept/reject arms ----------------------------


def test_validate_headref_symlink_arm_accepts_refs_prefix(
    tmp_path: Path,
) -> None:
    """Symlink whose link text begins ``refs/`` is a valid unborn-branch HEAD."""
    head = tmp_path / "HEAD"
    head.symlink_to("refs/heads/main")
    mod = _load_guard_module("guard_bash_headref_symlink_ok")
    assert mod._validate_headref(head) is True, (
        "symlink HEAD beginning refs/ must be accepted"
    )


def test_validate_headref_symlink_arm_rejects_non_refs_prefix(
    tmp_path: Path,
) -> None:
    """Sibling of the symlink accept: link text not beginning ``refs/``."""
    head = tmp_path / "HEAD"
    head.symlink_to("heads/main")
    mod = _load_guard_module("guard_bash_headref_symlink_bad")
    assert mod._validate_headref(head) is False, (
        "symlink HEAD not beginning refs/ must be rejected"
    )


def test_validate_headref_textual_ref_arm_accepts_ref_refs(
    tmp_path: Path,
) -> None:
    """Regular file ``ref:`` + optional headref WS + ``refs/`` is accepted."""
    head = tmp_path / "HEAD"
    head.write_text("ref: refs/heads/main\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_textual_ok")
    assert mod._validate_headref(head) is True, (
        "textual ref: refs/... HEAD must be accepted"
    )


def test_validate_headref_textual_ref_arm_accepts_headref_whitespace(
    tmp_path: Path,
) -> None:
    """``ref:`` remainder may strip only space/tab/LF/CR before ``refs/``."""
    head = tmp_path / "HEAD"
    # Leading tab after ``ref:`` is in _GIT_HEADREF_WS and must be stripped.
    head.write_bytes(b"ref:\trefs/heads/main\n")
    mod = _load_guard_module("guard_bash_headref_textual_ws")
    assert mod._validate_headref(head) is True, (
        "ref: + headref whitespace + refs/ must be accepted"
    )


def test_validate_headref_textual_ref_arm_rejects_non_refs_remainder(
    tmp_path: Path,
) -> None:
    """Sibling of the textual accept: ``ref:`` remainder does not begin ``refs/``."""
    head = tmp_path / "HEAD"
    head.write_text("ref: heads/main\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_textual_bad")
    assert mod._validate_headref(head) is False, (
        "ref: without refs/ remainder must be rejected"
    )


def test_validate_headref_textual_ref_arm_rejects_non_headref_whitespace(
    tmp_path: Path,
) -> None:
    """Only space/tab/LF/CR are stripped; a form-feed before ``refs/`` rejects."""
    head = tmp_path / "HEAD"
    head.write_bytes(b"ref:\x0crefs/heads/main\n")
    mod = _load_guard_module("guard_bash_headref_textual_ff")
    assert mod._validate_headref(head) is False, (
        "ref: + non-headref whitespace before refs/ must be rejected"
    )


def test_validate_headref_hex_arm_accepts_detached_sha(tmp_path: Path) -> None:
    """First 40 characters hexadecimal is a detached HEAD."""
    head = tmp_path / "HEAD"
    head.write_text("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_hex_ok")
    assert mod._validate_headref(head) is True, (
        "40-char hex detached HEAD must be accepted"
    )


def test_validate_headref_hex_arm_rejects_short_hex(tmp_path: Path) -> None:
    """Sibling of the hex accept: fewer than 40 hex characters is not detached."""
    head = tmp_path / "HEAD"
    head.write_text("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_hex_short")
    assert mod._validate_headref(head) is False, (
        "39-char hex must not be accepted as detached HEAD"
    )


def test_validate_headref_non_regular_arm_rejects_directory(
    tmp_path: Path,
) -> None:
    """Neither symlink nor regular file: directory HEAD is rejected.

    This arm has no accept path of its own (only ``return False``).
    """
    head = tmp_path / "HEAD"
    head.mkdir()
    mod = _load_guard_module("guard_bash_headref_dir")
    assert mod._validate_headref(head) is False, (
        "directory HEAD must be rejected"
    )


def test_validate_headref_neither_hex_nor_ref_rejects(tmp_path: Path) -> None:
    """Regular file that is neither hex nor ``ref:`` is rejected."""
    head = tmp_path / "HEAD"
    head.write_text("not-a-valid-head\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_junk")
    assert mod._validate_headref(head) is False, (
        "junk HEAD content must be rejected"
    )


def test_validate_headref_lstat_permission_error_accepts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PermissionError on lstat is treated as success (deliberate pin)."""
    head = tmp_path / "HEAD"
    mod = _load_guard_module("guard_bash_headref_lstat_perm")

    def _perm(_path, *args, **kwargs):  # noqa: ANN001, ANN003
        raise PermissionError(13, "Permission denied", str(head))

    monkeypatch.setattr(mod.os, "lstat", _perm)
    assert mod._validate_headref(head) is True, (
        "lstat PermissionError must accept"
    )


def test_validate_headref_lstat_oserror_rejects(
    tmp_path: Path,
) -> None:
    """Sibling of the lstat PermissionError arm: other OSError rejects."""
    head = tmp_path / "missing-HEAD"
    mod = _load_guard_module("guard_bash_headref_lstat_missing")
    assert mod._validate_headref(head) is False, (
        "missing HEAD (OSError from lstat) must reject"
    )


def test_validate_headref_readlink_permission_error_accepts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PermissionError on readlink of a symlink HEAD is treated as success."""
    head = tmp_path / "HEAD"
    head.symlink_to("refs/heads/main")
    mod = _load_guard_module("guard_bash_headref_readlink_perm")

    def _perm(_path):  # noqa: ANN001
        raise PermissionError(13, "Permission denied", str(head))

    monkeypatch.setattr(mod.os, "readlink", _perm)
    assert mod._validate_headref(head) is True, (
        "readlink PermissionError must accept"
    )


def test_validate_headref_readlink_oserror_rejects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling of the readlink PermissionError arm: other OSError rejects."""
    head = tmp_path / "HEAD"
    head.symlink_to("refs/heads/main")
    mod = _load_guard_module("guard_bash_headref_readlink_oserr")

    def _err(_path):  # noqa: ANN001
        raise OSError(5, "I/O error", str(head))

    monkeypatch.setattr(mod.os, "readlink", _err)
    assert mod._validate_headref(head) is False, (
        "readlink OSError must reject"
    )


def test_validate_headref_open_permission_error_accepts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PermissionError opening a regular HEAD is treated as success."""
    import builtins

    head = tmp_path / "HEAD"
    head.write_text("ref: refs/heads/main\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_open_perm")
    real_open = builtins.open

    def _open(path, *args, **kwargs):  # noqa: ANN001, ANN003
        if Path(path) == head:
            raise PermissionError(13, "Permission denied", str(head))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _open)
    assert mod._validate_headref(head) is True, (
        "open PermissionError must accept"
    )


def test_validate_headref_open_oserror_rejects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling of the open PermissionError arm: other OSError rejects."""
    import builtins

    head = tmp_path / "HEAD"
    head.write_text("ref: refs/heads/main\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_headref_open_oserr")
    real_open = builtins.open

    def _open(path, *args, **kwargs):  # noqa: ANN001, ANN003
        if Path(path) == head:
            raise OSError(5, "I/O error", str(head))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _open)
    assert mod._validate_headref(head) is False, (
        "open OSError must reject"
    )


# --- GROUP C: gitfile payload normalisation steps -----------------------------


def test_gitfile_rstrip_crlf_empty_payload_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Trailing CR/LF after ``gitdir: `` must rstrip to empty and keep walking.

    Without ``rstrip(\"\\r\\n\")``, the payload is a non-empty newline/CR and the
    gitfile arm returns False (fail-open) instead of continuing ascent to the
    real multi-worktree root.
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    _litter_git_path(primary).write_bytes(b"gitdir: \r\n")
    mod = _load_guard_module("guard_bash_gitfile_rstrip_empty")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "gitdir: + CR/LF-only payload must rstrip empty and keep ascent"
    )


def test_gitfile_relative_payload_joined_to_cursor(tmp_path: Path) -> None:
    """Relative gitdir payload is resolved against the probe cursor, not cwd.

    Without ``gd = cursor / gd``, ``Path(payload).resolve()`` uses process cwd
    and the layout comparison misses the planted ``…/.git/worktrees/<name>``.
    """
    probe = tmp_path / "probe"
    probe.mkdir()
    target = probe / "nested" / ".git" / "worktrees" / "wt"
    target.mkdir(parents=True)
    (probe / ".git").write_text(
        "gitdir: nested/.git/worktrees/wt\n", encoding="utf-8"
    )
    mod = _load_guard_module("guard_bash_gitfile_relative")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "relative gitdir payload must join against the cursor directory"
    )


def test_gitfile_resolve_follows_symlink_to_real_layout(tmp_path: Path) -> None:
    """``resolve(strict=False)`` is required when the lexical path is a decoy.

    Lexical parent is ``decoy`` (not ``worktrees``); only after resolve does the
    path become ``…/.git/worktrees/<name>``. Removing resolve must flip this to
    False.
    """
    real = tmp_path / "real" / ".git" / "worktrees" / "wt"
    real.mkdir(parents=True)
    decoy_dir = tmp_path / "decoy"
    decoy_dir.mkdir()
    decoy = decoy_dir / "entry"
    decoy.symlink_to(real)
    probe = tmp_path / "probe"
    probe.mkdir()
    (probe / ".git").write_text(f"gitdir: {decoy}\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_gitfile_resolve")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "symlink decoy gitdir must resolve to …/.git/worktrees/<name>"
    )


def test_gitfile_utf8_decode_accepts_non_ascii_path(tmp_path: Path) -> None:
    """``read_bytes().decode(\"utf-8\")`` must accept a non-ASCII path segment."""
    probe = tmp_path / "probe"
    probe.mkdir()
    target = tmp_path / "common" / ".git" / "worktrees" / "café-wt"
    target.mkdir(parents=True)
    (probe / ".git").write_text(f"gitdir: {target}\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_gitfile_utf8_ok")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "utf-8 gitdir path with non-ASCII segment must match layout"
    )


def test_gitfile_utf8_decode_rejects_invalid_bytes(tmp_path: Path) -> None:
    """Invalid UTF-8 in the gitfile body must return False (decode failure)."""
    probe = tmp_path / "probe"
    probe.mkdir()
    # Valid-looking prefix then an invalid UTF-8 byte in the payload.
    (probe / ".git").write_bytes(b"gitdir: /tmp/x/.git/worktrees/y\xff\n")
    mod = _load_guard_module("guard_bash_gitfile_utf8_bad")
    assert mod._git_worktrees_dir_reachable(probe) is False, (
        "invalid utf-8 gitfile must not be treated as reachable"
    )


def test_gitfile_exact_prefix_requires_space_after_colon(tmp_path: Path) -> None:
    """Exact ``gitdir: `` (with space) is required; ``gitdir:`` alone is not.

    Without the space in the prefix check, ``gitdir:/abs/.git/worktrees/x``
    would be accepted. With it, this near-miss must return False.
    """
    probe = tmp_path / "probe"
    probe.mkdir()
    target = tmp_path / "common" / ".git" / "worktrees" / "wt"
    target.mkdir(parents=True)
    # Missing space after colon — git rejects this shape.
    (probe / ".git").write_text(f"gitdir:{target}\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_gitfile_prefix_space")
    assert mod._git_worktrees_dir_reachable(probe) is False, (
        "gitdir: without space after colon must not match"
    )


@pytest.mark.parametrize(
    "label,plant",
    [
        (
            "junk_gitfile",
            lambda p: _litter_git_path(p).write_text("not-a-gitdir-file\n", encoding="utf-8"),
        ),
        (
            "upper_prefix",
            lambda p: _litter_git_path(p).write_text(
                "GITDIR: /nonexistent/worktrees/x\n", encoding="utf-8"
            ),
        ),
        (
            "lead_space",
            lambda p: _litter_git_path(p).write_text(
                " gitdir: /nonexistent/worktrees/x\n", encoding="utf-8"
            ),
        ),
        (
            "no_space_prefix",
            lambda p: _litter_git_path(p).write_text(
                "gitdir:/nonexistent/worktrees/x\n", encoding="utf-8"
            ),
        ),
        (
            "empty_payload_gitfile",
            lambda p: _litter_git_path(p).write_text("gitdir: ", encoding="utf-8"),
        ),
    ],
    ids=[
        "junk_gitfile",
        "upper_prefix",
        "lead_space",
        "no_space_prefix",
        "empty_payload_gitfile",
    ],
)
def test_malformed_gitfile_litter_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
    label: str,
    plant,
) -> None:
    """Malformed ``.git`` FILE litter must not short-circuit the ascent.

    Pins the exact ``gitdir: `` prefix check (junk / case / spacing variants)
    and the empty-payload arm that previously returned False (fail-open).
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    plant(primary)
    mod = _load_guard_module(f"guard_bash_gitfile_{label}")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        f"{label} litter must not make multi-worktree layout unreachable"
    )


def test_head_only_git_dir_litter_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """``.git`` with well-formed HEAD but no objects/refs must keep walking.

    Pins the ``os.access(..., X_OK)`` loop: without it, HEAD-only litter is
    accepted as a real git directory and ``worktrees/`` absence returns False.
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    mod = _load_guard_module("guard_bash_head_only_litter")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "HEAD-only .git litter (no objects/refs) must not fail-open the walk"
    )


def test_malformed_head_with_objects_refs_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """``.git`` with objects/refs but malformed HEAD must keep walking.

    Pins ``_validate_headref``: without it, objects+refs alone short-circuit
    the walk as a real git directory lacking ``worktrees/``.
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    # Not hex, not ``ref: refs/...`` — git rejects this HEAD shape.
    _plant_valid_head(git_dir, content="not-a-valid-head\n")
    _plant_objects_refs(git_dir)
    mod = _load_guard_module("guard_bash_bad_head_litter")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "malformed HEAD with objects/refs litter must not fail-open the walk"
    )


def test_commondir_zero_length_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Zero-length ``commondir`` with local objects/refs must keep walking.

    Pins the zero-length ``commondir`` refusal: without it, empty body keeps
    the default common dir and objects/refs make litter look like a real repo.
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    _plant_objects_refs(git_dir)
    (git_dir / "commondir").write_bytes(b"")
    mod = _load_guard_module("guard_bash_commondir_zero")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "zero-length commondir litter must not fail-open the walk"
    )


def test_commondir_non_regular_directory_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """``commondir`` as a directory (non-regular) must keep walking.

    Pins ``stat.S_ISREG``: a directory entry must not be treated as a usable
    commondir file even when local objects/refs exist beside it.
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    _plant_objects_refs(git_dir)
    (git_dir / "commondir").mkdir()
    mod = _load_guard_module("guard_bash_commondir_dir")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "directory commondir litter must not fail-open the walk"
    )


def test_commondir_non_regular_dev_null_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """``commondir`` symlink to a non-regular device must keep walking.

    Pins ``stat.S_ISREG`` for a readable non-regular target (``/dev/null``):
    without the regular-file check, open/read can succeed and litter looks real.
    """
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    _plant_objects_refs(git_dir)
    (git_dir / "commondir").symlink_to("/dev/null")
    mod = _load_guard_module("guard_bash_commondir_devnull")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "non-regular commondir (symlink to /dev/null) must not fail-open the walk"
    )


def test_commondir_s_isreg_required_when_body_would_otherwise_accept(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pins ``stat.S_ISREG`` as uniquely load-bearing (M2).

    Real FS non-regular shapes either have size 0 (caught by the zero-length
    arm) or fail ``open`` (caught by OSError). To pin S_ISREG alone, plant a
    *readable* newline-only ``commondir`` plus local objects/refs (a shape
    that would be accepted if treated as a regular file), then make
    ``os.stat`` report a non-regular mode with non-zero size for that entry.
    Without the S_ISREG guard the walk fail-opens; with it, ascent continues.
    """
    import os
    import stat as stat_mod

    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    _plant_objects_refs(git_dir)
    # Newline-only body: if treated as a regular commondir file, keeps default
    # common dir and objects/refs make this look like a real repository.
    (git_dir / "commondir").write_bytes(b"\n")

    mod = _load_guard_module("guard_bash_commondir_s_isreg")
    real_stat = os.stat
    # Absolute string form; avoid Path.resolve under the patched os.stat.
    commondir_key = os.path.abspath(str(git_dir / "commondir"))

    def _stat_non_regular(path, *args, **kwargs):  # noqa: ANN001, ANN003
        st = real_stat(path, *args, **kwargs)
        try:
            key = os.path.abspath(str(path))
        except (OSError, TypeError, ValueError):
            return st
        if key == commondir_key:
            # Directory mode, non-zero size — skips zero-length arm; open of the
            # *real* file still succeeds when S_ISREG is neutralised.
            return os.stat_result(
                (
                    stat_mod.S_IFDIR | 0o755,
                    st.st_ino,
                    st.st_dev,
                    st.st_nlink,
                    st.st_uid,
                    st.st_gid,
                    4096,
                    st.st_atime,
                    st.st_mtime,
                    st.st_ctime,
                )
            )
        return st

    monkeypatch.setattr(mod.os, "stat", _stat_non_regular)
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "non-regular commondir stat shape must not fail-open when body+objects "
        "would otherwise accept the litter as a real git directory"
    )


def test_commondir_broken_symlink_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Broken ``commondir`` symlink must keep walking."""
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    _plant_objects_refs(git_dir)
    (git_dir / "commondir").symlink_to("missing-commondir-target")
    mod = _load_guard_module("guard_bash_commondir_broken")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "broken commondir symlink litter must not fail-open the walk"
    )


def test_commondir_absent_head_only_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Absent ``commondir`` with HEAD only (no objects/refs) must keep walking."""
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    # No commondir entry, no objects/refs.
    mod = _load_guard_module("guard_bash_commondir_absent")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "absent commondir with HEAD-only litter must not fail-open the walk"
    )


def test_commondir_newline_only_without_objects_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Newline-only ``commondir`` without objects/refs must keep walking."""
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    (git_dir / "commondir").write_bytes(b"\n")
    mod = _load_guard_module("guard_bash_commondir_newline")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "newline-only commondir without objects/refs must not fail-open the walk"
    )


def test_commondir_relative_missing_target_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Relative ``commondir`` body pointing at a missing common dir keeps walking."""
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    (git_dir / "commondir").write_text("not-a-real-common\n", encoding="utf-8")
    mod = _load_guard_module("guard_bash_commondir_rel")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "relative missing commondir target must not fail-open the walk"
    )


def test_commondir_absolute_missing_target_keeps_ascent(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Absolute ``commondir`` body pointing at a missing common dir keeps walking."""
    primary, _ = primary_with_linked
    probe = _probe_under_primary(primary)
    git_dir = _litter_git_path(primary)
    _plant_valid_head(git_dir)
    (git_dir / "commondir").write_text(
        "/nonexistent/git-common-dir-for-litter\n", encoding="utf-8"
    )
    mod = _load_guard_module("guard_bash_commondir_abs")
    assert mod._git_worktrees_dir_reachable(probe) is True, (
        "absolute missing commondir target must not fail-open the walk"
    )


def test_worktree_toplevel_oserror_is_could_not_determine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """D4 (9540): OSError from git probe is could-not-determine, not None skip."""
    mod = _load_guard_module("guard_bash_d4_oserror")

    def _oserror(*_a, **_k):
        raise FileNotFoundError(2, "No such file or directory", "git")

    monkeypatch.setattr(mod.subprocess, "run", _oserror)
    monkeypatch.setattr(mod, "_probe_timeout", lambda _cap: 1.0)

    result = mod._worktree_toplevel(tmp_path)
    assert result is mod._COULD_NOT_DETERMINE, (
        f"OSError must return could-not-determine sentinel, got {result!r}"
    )


def test_worktree_toplevel_nonzero_rc_remains_determined_skip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """D4 residual: returncode != 0 stays a determined None skip (not a block)."""
    mod = _load_guard_module("guard_bash_d4_rc")

    def _fail(cmd, **kwargs):  # noqa: ANN001, ANN003
        return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="not a git repo")

    monkeypatch.setattr(mod.subprocess, "run", _fail)
    monkeypatch.setattr(mod, "_probe_timeout", lambda _cap: 1.0)

    result = mod._worktree_toplevel(tmp_path)
    assert result is None, f"nonzero rc must remain determined None skip, got {result!r}"


def test_toplevel_oserror_on_any_intent_fails_closed(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D5 (9980): could-not-determine on any intent fails closed for the command.

    Multi-intent: if the first (primary-targeting) intent hits OSError and is
    skipped, a later clean intent must not make the whole scan return None.
    """
    primary, linked = primary_with_linked
    mod = _load_guard_module("guard_bash_d5_multi_intent")

    calls = {"n": 0}

    def _flaky(cmd, **kwargs):  # noqa: ANN001, ANN003
        calls["n"] += 1
        cmd_text = " ".join(str(p) for p in cmd)
        # First toplevel probe (primary-targeting intent) blows up.
        if "--show-toplevel" in cmd_text and calls["n"] == 1:
            raise FileNotFoundError(2, "No such file or directory", "git")
        # Later probes would resolve cleanly to the linked worktree.
        return subprocess.CompletedProcess(cmd, 0, stdout=str(linked), stderr="")

    monkeypatch.setattr(mod.subprocess, "run", _flaky)

    msg = mod._detect_root_branch_switch(
        f"git -C {primary} checkout -b feature/x && "
        f"git -C {linked} switch -c feature/y",
        repo_root=linked,
    )
    assert msg is not None, (
        "OSError on first intent must fail closed for the whole command, "
        "not be recovered by a later clean intent"
    )
    assert "could not complete the root-worktree branch-switch check" in msg
    assert "could not resolve worktree toplevel" in msg



# --- DEFECT PINS: switch-parser bypasses + _current_branch channels (r0802) ---


def test_checkout_clustered_short_flags_blocked(primary_with_linked) -> None:
    """DEFECT ONE: clustered -qb must be recognised as checkout -b creation."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -qb feature/clustered")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_checkout_attached_short_flag_blocked(primary_with_linked) -> None:
    """DEFECT ONE: attached -bBRANCH must be recognised as checkout -b creation."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -bfeature/attached")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_checkout_clustered_with_attached_value_blocked(primary_with_linked) -> None:
    """DEFECT ONE: -qbBRANCH (cluster + attached value) must create-switch-block."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -qbfeature/cluster-attached")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_switch_clustered_short_flags_blocked(primary_with_linked) -> None:
    """DEFECT ONE twin: clustered switch -qc must also be recognised."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch -qc feature/sw-clustered")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_bash_c_nested_checkout_blocked(primary_with_linked) -> None:
    """DEFECT TWO: bash -c hides git checkout -b; must still block on primary."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, 'bash -c "git checkout -b feature/bash-c"')
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_sh_c_nested_checkout_blocked(primary_with_linked) -> None:
    """DEFECT TWO: sh -c hides git checkout -b; must still block on primary."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, 'sh -c "git checkout -b feature/sh-c"')
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_eval_nested_checkout_blocked(primary_with_linked) -> None:
    """DEFECT TWO: eval hides git checkout -b; must still block on primary."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, 'eval "git checkout -b feature/eval-c"')
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_nested_shell_depth_bound_no_spin(primary_with_linked) -> None:
    """DEFECT TWO: self-referential nested shells must not spin forever."""
    primary, _ = primary_with_linked
    # Four nested bash -c layers exceed the depth bound of 3 unwraps; must not hang.
    cmd = "bash -c " + "bash -c " * 8 + "true"
    proc = _invoke(primary, cmd)
    # No switch intent after bound → allow (exit 0), and must finish quickly.
    assert proc.returncode == 0, proc.stderr


def test_checkout_orphan_in_primary_blocked(primary_with_linked) -> None:
    """DEFECT THREE: checkout --orphan is a real switch and must block on primary."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout --orphan feature/orphan-x")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_git_switch_intents_import_error_is_could_not_determine(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DEFECT FOUR: missing parser deps must not read as empty intent list."""
    primary, _ = primary_with_linked
    mod = _load_guard_module("guard_bash_d4_import")

    real_import = __import__

    def _block_isolation(name, *args, **kwargs):  # noqa: ANN001, ANN003
        if name == "_bash_isolation_guard" or name.endswith("._bash_isolation_guard"):
            raise ImportError("simulated missing isolation guard")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _block_isolation)

    result = mod._git_switch_intents("git checkout -b feature/x", primary)
    assert result is mod._COULD_NOT_DETERMINE, (
        "ImportError from parser helpers must be could-not-determine, "
        f"not empty-list clean; got {result!r}"
    )


def test_detect_import_error_emits_could_not_determine_block(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DEFECT FOUR: caller must block with existing could-not-determine message."""
    primary, _ = primary_with_linked
    mod = _load_guard_module("guard_bash_d4_detect_import")

    monkeypatch.setattr(
        mod, "_git_switch_intents", lambda *_a, **_k: mod._COULD_NOT_DETERMINE
    )

    msg = mod._detect_root_branch_switch(
        "git checkout -b feature/x",
        repo_root=primary,
    )
    assert msg is not None, (
        "could-not-determine intents must fail closed, not return None (allow)"
    )
    assert "could not complete the root-worktree branch-switch check" in msg


def test_identity_helper_import_error_fails_closed(
    primary_with_linked: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DEFECT FIVE: missing identity helper must block, not silent-allow None."""
    primary, _ = primary_with_linked
    mod = _load_guard_module("guard_bash_d5_ident_import")

    # Ensure intents are non-empty so we reach the identity import arm.
    monkeypatch.setattr(
        mod,
        "_git_switch_intents",
        lambda *_a, **_k: [(primary, "feature/x")],
    )

    real_import = __import__

    def _block_identity(name, *args, **kwargs):  # noqa: ANN001, ANN003
        if name == "_worktree_identity" or name.endswith("._worktree_identity"):
            raise ImportError("simulated missing identity helper")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _block_identity)

    msg = mod._detect_root_branch_switch(
        "git checkout -b feature/x",
        repo_root=primary,
    )
    assert msg is not None, (
        "ImportError from identity helper must fail closed with a message, "
        "not return None (silent permit)"
    )
    assert "could not complete the root-worktree branch-switch check" in msg


def test_current_branch_nonzero_rc_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """DEFECT SIX: non-zero branch probe is could-not-determine (None), not ''."""
    mod = _load_guard_module("guard_bash_d6_nonzero")
    monkeypatch.setattr(mod, "_probe_timeout", lambda _cap: 1.0)

    def _fail(cmd, **kwargs):  # noqa: ANN001, ANN003
        return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="boom")

    monkeypatch.setattr(mod.subprocess, "run", _fail)

    result = mod._current_branch(tmp_path)
    assert result is None, (
        f"non-zero exit from branch probe must return None, got {result!r}"
    )


def test_current_branch_zero_empty_stdout_returns_empty_string(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """DEFECT SIX: zero + empty stdout is determined detached HEAD ('')."""
    mod = _load_guard_module("guard_bash_d6_empty")
    monkeypatch.setattr(mod, "_probe_timeout", lambda _cap: 1.0)

    def _empty(cmd, **kwargs):  # noqa: ANN001, ANN003
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(mod.subprocess, "run", _empty)

    result = mod._current_branch(tmp_path)
    assert result == "", (
        f"zero exit with empty stdout must return empty string, got {result!r}"
    )


# --- OPPOSING ALLOW ARMS: neighbouring benign forms must still pass ------------


def test_no_git_stage_still_allowed(primary_with_linked) -> None:
    """Benign: command with no git stage must still be allowed."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "echo hello-from-allow-arm")
    assert proc.returncode == 0, proc.stderr
    assert "BLOCKED" not in proc.stderr


def test_non_switch_git_still_allowed(primary_with_linked) -> None:
    """Benign: git command that is not checkout/switch must still be allowed."""
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git status")
    assert proc.returncode == 0, proc.stderr
    assert "BLOCKED" not in proc.stderr


def test_checkout_b_in_linked_worktree_still_allowed(primary_with_linked) -> None:
    """Benign: checkout -b inside a linked worktree (not primary) must allow."""
    _, linked = primary_with_linked
    proc = _invoke(linked, "git checkout -b feature/linked-ok")
    assert proc.returncode == 0, proc.stderr
    assert "BLOCKED" not in proc.stderr


def test_plain_checkout_existing_still_not_treated_as_switch(primary_with_linked) -> None:
    """Out of scope: plain checkout of a branch remains deliberately excluded."""
    primary, _ = primary_with_linked
    # feature/seed exists via the linked worktree branch; plain checkout is
    # ambiguous with path restore and must NOT be classified as switch-intent.
    mod = _load_guard_module("guard_bash_plain_checkout_excluded")
    intents = mod._git_switch_intents("git checkout feature/seed", primary)
    assert intents == [], (
        "plain checkout without creation flag must stay excluded from switch "
        f"detection; got {intents!r}"
    )


def test_parser_results_for_concrete_bypass_commands(
    primary_with_linked: tuple[Path, Path],
) -> None:
    """Unit-level pins: each named bypass command yields a non-empty intent."""
    primary, _ = primary_with_linked
    mod = _load_guard_module("guard_bash_bypass_unit")
    cases = [
        "git checkout -qb feature/clustered",
        "git checkout -bfeature/attached",
        "bash -c \"git checkout -b feature/bash-c\"",
        "sh -c \"git checkout -b feature/sh-c\"",
        "eval \"git checkout -b feature/eval-c\"",
        "git checkout --orphan feature/orphan-x",
    ]
    for cmd in cases:
        intents = mod._git_switch_intents(cmd, primary)
        assert intents, f"expected non-empty intents for {cmd!r}, got {intents!r}"
        assert intents is not mod._COULD_NOT_DETERMINE
        branches = [b for _t, b in intents]
        assert any(b.startswith("feature/") for b in branches), (
            f"expected feature/* branch in intents for {cmd!r}, got {intents!r}"
        )
