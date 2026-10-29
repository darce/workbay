"""Regression tests for effective-cwd tracking in _bash_isolation_guard (implementation note).

Contract under test (implementation note):

* ``scan_bash_command`` threads an effective cwd through ``&&``/``;``-joined
  stages, so ``cd <feature-worktree> && git checkout -- Makefile`` resolves the
  relative target against the worktree (allow) instead of the primary repo
  root (the BR-17 false positive that blocked the prescribed worktree-fallback
  pattern).
* ``cd`` joined onward by ``|``, ``&``, or ``||`` does NOT propagate (subshell
  / failure-path semantics) — the scanner degrades to unknown-cwd fail-closed.
* ``cd`` with flags, ``-``, vars, or no args → unknown cwd, fail-closed.
* ``extract_raw_write_targets`` mirrors the absolutization so the worktree
  drift guard sees the same targets.

Fixtures build a real primary repo (on ``main``) with a linked feature-branch
worktree so ``resolve_path_branch`` exercises true per-path git resolution.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bash_isolation_guard import (  # noqa: E402
    extract_raw_write_targets,
    scan_bash_command,
)
from _harness_protocol import BranchIsolationPolicy  # noqa: E402


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _policy() -> BranchIsolationPolicy:
    return BranchIsolationPolicy(
        code_roots=("packages/", "scripts/"),
        protected_extensions=(".py", ".sh", ".mk"),
        root_protected_files=("Makefile",),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
    )


@pytest.fixture()
def repo_pair(tmp_path: Path) -> tuple[Path, Path]:
    """(primary repo on main, linked feature-branch worktree)."""
    primary = tmp_path / "primary"
    primary.mkdir()
    _git(primary, "init", "-b", "main")
    _git(primary, "config", "user.email", "t@example.invalid")
    _git(primary, "config", "user.name", "t")
    (primary / "Makefile").write_text("all:\n\ttrue\n")
    pkg = primary / "packages" / "pkg"
    pkg.mkdir(parents=True)
    (pkg / "mod.py").write_text("x = 1\n")
    _git(primary, "add", "-A")
    _git(primary, "commit", "-m", "init")
    worktree = tmp_path / "wt"
    _git(primary, "worktree", "add", "-b", "feature/task", str(worktree))
    return primary, worktree


def _scan(command: str, primary: Path) -> list[str]:
    return scan_bash_command(command, primary, _policy())


# --- cd propagation across && / ; -----------------------------------------


def test_cd_worktree_then_relative_git_checkout_allowed(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"cd {wt} && git checkout -- Makefile", primary)
    assert blocked == [], f"cd-worktree relative checkout must not block: {blocked!r}"


def test_plain_relative_git_checkout_on_main_blocked(repo_pair) -> None:
    primary, _wt = repo_pair
    blocked = _scan("git checkout -- Makefile", primary)
    assert "Makefile" in blocked


def test_cd_subdir_relative_parent_write_blocked(repo_pair) -> None:
    # `cd packages && rm ../Makefile` writes primary/Makefile. Pre-fix the
    # naive root-join produced a path *outside* the root (fail-open); correct
    # cwd-tracking must block it.
    primary, _wt = repo_pair
    blocked = _scan("cd packages && rm ../Makefile", primary)
    assert "Makefile" in blocked


def test_cd_redirect_into_worktree_allowed(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"cd {wt} && echo x > packages/pkg/mod.py", primary)
    assert blocked == [], f"redirect into worktree must not block: {blocked!r}"


def test_cd_worktree_then_absolute_primary_tee_blocked(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"cd {wt} && tee {primary}/Makefile", primary)
    assert "Makefile" in blocked


# --- unknown-cwd fail-closed ------------------------------------------------


def test_cd_unset_var_then_relative_rm_blocked(repo_pair) -> None:
    primary, _wt = repo_pair
    blocked = _scan('cd "$SOME_DIR" && rm Makefile', primary)
    assert "Makefile" in blocked


def test_cd_with_flag_then_relative_rm_blocked(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"cd -P {wt} && rm Makefile", primary)
    assert "Makefile" in blocked


# --- pipe / || joins must NOT propagate cd (fail-open guard) ----------------


def test_cd_piped_does_not_propagate(repo_pair) -> None:
    # In a real shell the piped `cd` runs in a subshell; `rm Makefile` still
    # deletes primary/Makefile. The scanner must not resolve it via the wt.
    primary, wt = repo_pair
    blocked = _scan(f"cd {wt} | true; rm Makefile", primary)
    assert "Makefile" in blocked


def test_cd_or_join_does_not_propagate(repo_pair) -> None:
    # `cd <wt> || rm Makefile` runs rm only when the cd FAILED (cwd unchanged).
    primary, wt = repo_pair
    blocked = _scan(f"cd {wt} || rm Makefile", primary)
    assert "Makefile" in blocked


# --- git -C global-option parsing (implementation note) ----------------------------------


def test_git_dash_c_worktree_checkout_allowed(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"git -C {wt} checkout -- Makefile", primary)
    assert blocked == [], f"git -C worktree checkout must not block: {blocked!r}"


def test_git_dash_c_primary_checkout_blocked(repo_pair) -> None:
    # Pre-fix `-C` was mistaken for the subcommand and the stage was invisible
    # to the scanner — a real bypass hole for writes aimed at the primary repo.
    primary, _wt = repo_pair
    blocked = _scan(f"git -C {primary} checkout -- Makefile", primary)
    assert "Makefile" in blocked


def test_git_dash_c_relative_composes_with_cd(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"cd {wt} && git -C . checkout -- Makefile", primary)
    assert blocked == []


def test_git_dash_c_unresolvable_dir_fail_closed(repo_pair) -> None:
    # Pipe-joined cd leaves the cwd unknown; a relative -C dir cannot compose,
    # so the relative target falls back to the repo root (blocked).
    primary, _wt = repo_pair
    pkgdir = primary / "packages"
    blocked = _scan(f"cd {pkgdir} | true; git -C .. checkout -- Makefile", primary)
    assert "Makefile" in blocked


def test_git_dash_c_config_pair_skipped(repo_pair) -> None:
    primary, _wt = repo_pair
    blocked = _scan("git -c core.autocrlf=false checkout -- Makefile", primary)
    assert "Makefile" in blocked


# --- absolute-path regression guards ----------------------------------------


def test_absolute_worktree_target_still_allowed(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(f"rm {wt}/packages/pkg/mod.py", primary)
    assert blocked == []


def test_absolute_primary_target_still_blocked(repo_pair) -> None:
    primary, _wt = repo_pair
    blocked = _scan(f"rm {primary}/packages/pkg/mod.py", primary)
    assert "packages/pkg/mod.py" in blocked


def test_cd_worktree_then_relative_python_write_allowed(repo_pair) -> None:
    primary, wt = repo_pair
    blocked = _scan(
        f"cd {wt} && python -c \"open('packages/pkg/mod.py','w')\"",
        primary,
    )
    assert blocked == [], f"python write into worktree must not block: {blocked!r}"


def test_cd_subdir_then_relative_python_parent_write_blocked(repo_pair) -> None:
    primary, _wt = repo_pair
    blocked = _scan("cd packages && python -c \"open('../Makefile','w')\"", primary)
    assert "Makefile" in blocked


# --- extract_raw_write_targets parity ----------------------------------------


def test_extract_raw_targets_absolutized_under_cd(repo_pair) -> None:
    primary, wt = repo_pair
    targets = extract_raw_write_targets(f"cd {wt} && rm packages/pkg/mod.py")
    assert str(wt / "packages" / "pkg" / "mod.py") in targets


def test_extract_raw_targets_absolute_passthrough(repo_pair) -> None:
    primary, wt = repo_pair
    targets = extract_raw_write_targets(f"sed -i s/a/b/ {wt}/packages/pkg/mod.py")
    assert str(wt / "packages" / "pkg" / "mod.py") in targets


def test_extract_raw_python_targets_absolutized_under_cd(repo_pair) -> None:
    primary, wt = repo_pair
    targets = extract_raw_write_targets(
        f"cd {wt} && python -c \"open('packages/pkg/mod.py','w')\""
    )
    assert str(wt / "packages" / "pkg" / "mod.py") in targets


# --- guard-bash-main-branch.py bypass behavior (implementation note, subprocess-level) ---

HOOKS_DIR = Path(__file__).resolve().parent
PAYLOAD_ROOT = HOOKS_DIR.parents[1]
CONTRACT_PATH = (
    PAYLOAD_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
)

BYPASS_PRIMARY = "WORKBAY_ALLOW_BASH_MAIN_WRITE"
BYPASS_LEGACY = "ALT_ALLOW_BASH_MAIN_WRITE"


@pytest.fixture()
def hook_repo(tmp_path: Path) -> Path:
    """A tmp git repo on main carrying the hooks + contract, hook-runnable."""
    repo = tmp_path / "hookrepo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "t@example.invalid")
    _git(repo, "config", "user.name", "t")
    (repo / "Makefile").write_text("all:\n\ttrue\n")
    hooks_dst = repo / "scripts" / "hooks"
    hooks_dst.parent.mkdir(parents=True)
    shutil.copytree(
        HOOKS_DIR,
        hooks_dst,
        ignore=shutil.ignore_patterns("__pycache__", "test_*"),
    )
    contract_dst = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    contract_dst.parent.mkdir(parents=True)
    shutil.copy(CONTRACT_PATH, contract_dst)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "init")
    return repo


def _run_hook(repo: Path, command: str, extra_env: dict[str, str] | None = None):
    payload = {
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": command},
    }
    env = {
        k: v for k, v in os.environ.items() if k not in {BYPASS_PRIMARY, BYPASS_LEGACY}
    }
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, str(repo / "scripts" / "hooks" / "guard-bash-main-branch.py")],
        cwd=str(repo),
        input=json.dumps(payload),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _bypass_log_lines(repo: Path) -> list[dict]:
    log = repo / ".task-state" / "branch_isolation_guard.jsonl"
    if not log.is_file():
        return []
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


def test_hook_blocks_and_advertises_new_env_name(hook_repo) -> None:
    proc = _run_hook(hook_repo, "git checkout -- Makefile")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr
    assert BYPASS_PRIMARY in proc.stderr
    # Legacy name must no longer be the advertised advice.
    assert f"set {BYPASS_LEGACY}=1" not in proc.stderr


def test_hook_inline_primary_bypass_allows_and_logs(hook_repo) -> None:
    proc = _run_hook(hook_repo, f"{BYPASS_PRIMARY}=1 git checkout -- Makefile")
    assert proc.returncode == 0, proc.stderr
    assert "bypass" in proc.stderr.lower()
    records = _bypass_log_lines(hook_repo)
    assert records, "bypass must append a jsonl audit record"
    assert records[-1].get("bypass_source") == "inline"


def test_hook_inline_legacy_bypass_allows_with_deprecation(hook_repo) -> None:
    proc = _run_hook(hook_repo, f"{BYPASS_LEGACY}=1 git checkout -- Makefile")
    assert proc.returncode == 0, proc.stderr
    assert "deprecat" in proc.stderr.lower()
    records = _bypass_log_lines(hook_repo)
    assert records and records[-1].get("bypass_var") == BYPASS_LEGACY


def test_hook_mid_command_assignment_does_not_bypass(hook_repo) -> None:
    proc = _run_hook(
        hook_repo, f"git status && {BYPASS_PRIMARY}=1 git checkout -- Makefile"
    )
    assert proc.returncode == 2, proc.stderr


def test_hook_env_var_bypass_allows_and_logs(hook_repo) -> None:
    proc = _run_hook(
        hook_repo, "git checkout -- Makefile", extra_env={BYPASS_PRIMARY: "1"}
    )
    assert proc.returncode == 0, proc.stderr
    records = _bypass_log_lines(hook_repo)
    assert records and records[-1].get("bypass_source") == "env"
