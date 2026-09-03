"""Regression pin for F-HARM-S2-01 / finding 13253 (cross-guard path spelling).

Finding: with the hook process cwd inside a linked feature worktree, a
whitespace-padded or quote-wrapped *absolute* path to a protected main-branch
file was ALLOWED by ``check_file_edit`` while ``scan_bash_command`` BLOCKED the
same file. The padded/quoted token normalized cleanly inside ``to_repo_relative``
(so it took the relativizable arm), but ``resolve_path_branch`` received the
still-raw token — ``Path("  /repo/x.py  ")`` is not absolute, so ``.resolve()``
re-anchored to the hook cwd and reported the *feature* branch.

Production fix normalizes before ``resolve_path_branch`` at the public entry
points. This module pins that contract: both guards must agree, the main-file
verdict must be BLOCK under adversarial spellings, a genuine feature-worktree
path must ALLOW under those same spellings (control arm), and genuine outsiders
must stay unblocked.

Fixtures build a real primary repo on ``main`` with a linked feature-branch
worktree and run every assertion with process cwd inside that worktree — the
cwd is the entire precondition for the defect.

GUARDPIN-REV-01: the feature control arm must pass ``repo_root=worktree`` (the
production shape inside a linked worktree). Passing ``repo_root=primary`` makes
the feature absolute path unrelativizable, so the outsider arm fires and the
control never reaches ``resolve_path_branch`` — a vacuous green under a
constant-``"main"`` mutation of the load-bearing call site.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bash_isolation_guard import scan_bash_command  # noqa: E402
from _branch_isolation_guard import check_file_edit  # noqa: E402
from _harness_protocol import BranchIsolationPolicy  # noqa: E402


_PROTECTED = frozenset({"main", "master"})


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


def _shell_quote_preserve(token: str) -> str:
    """Embed ``token`` so ``shlex.split`` yields the exact raw spelling.

    Adversarial spellings must reach the guard as the same characters the Edit
    tool would hand ``file_path`` — including leading whitespace and surrounding
    quote characters — not as a shell-decoded clean path.
    """
    return "'" + token.replace("'", "'\"'\"'") + "'"


@pytest.fixture()
def repo_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """(primary on main, linked feature worktree); process cwd = worktree.

    Mirrors ``test_bash_isolation_guard_cwd.repo_pair`` and chdirs into the
    feature worktree so padded absolute paths re-anchor there if normalization
    is skipped — the load-bearing precondition for F-HARM-S2-01.
    """
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
    # cwd inside the feature worktree is the defect precondition.
    monkeypatch.chdir(worktree)
    return primary, worktree


def _main_abs(primary: Path) -> str:
    return str((primary / "packages" / "pkg" / "mod.py").resolve())


def _feature_abs(worktree: Path) -> str:
    return str((worktree / "packages" / "pkg" / "mod.py").resolve())


def _adversarial_spellings(abs_path: str) -> list[tuple[str, str]]:
    """Named adversarial spellings of one absolute path.

    Each spelling either (a) makes a naive ``Path(token).is_absolute()`` return
    False so ``resolve()`` re-anchors to the hook cwd, or (b) is a quote/padding
    variant that ``normalize_path_token`` is required to collapse before
    resolution. Trailing-only whitespace stays absolute under ``Path`` but is
    still a normalize-required spelling named by the finding.
    """
    return [
        ("leading_ws", f"  {abs_path}"),
        ("trailing_ws", f"{abs_path}  "),
        ("both_ws", f"  {abs_path}  "),
        ("double_quote", f'"{abs_path}"'),
        ("single_quote", f"'{abs_path}'"),
        ("pad_double_quote", f'  "{abs_path}"  '),
        ("leading_tab", f"\t{abs_path}"),
        ("pad_single_quote", f"  '{abs_path}'  "),
    ]


def _edit_is_blocked(raw_path: str, repo_root: Path) -> bool:
    result = check_file_edit(
        "Edit",
        {"file_path": raw_path},
        branch="main",
        repo_root=str(repo_root),
        policy=_policy(),
        protected_branches=_PROTECTED,
    )
    return result is not None


def _bash_is_blocked(raw_path: str, repo_root: Path) -> bool:
    command = f"rm {_shell_quote_preserve(raw_path)}"
    blocked = scan_bash_command(command, repo_root, _policy())
    real = [b for b in blocked if not str(b).endswith("(formatter)")]
    return bool(real)


def assert_cross_guard_verdict(
    raw_path: str,
    repo_root: Path,
    *,
    expect_block: bool,
) -> None:
    """Invariant: both public guards return the same block/allow verdict.

    ``repo_root`` is the root handed to both guards for this arm. Inside a
    linked feature worktree the production shape is ``repo_root=worktree``
    (``git rev-parse --show-toplevel``); on the primary checkout it is the
    primary. Both guards must receive the same root for a given arm.
    """
    edit_blocked = _edit_is_blocked(raw_path, repo_root)
    bash_blocked = _bash_is_blocked(raw_path, repo_root)
    assert edit_blocked == bash_blocked, (
        f"cross-guard inversion on {raw_path!r}: "
        f"check_file_edit blocked={edit_blocked}, "
        f"scan_bash_command blocked={bash_blocked}"
    )
    assert edit_blocked is expect_block, (
        f"expected expect_block={expect_block} for {raw_path!r}, "
        f"got edit_blocked={edit_blocked} bash_blocked={bash_blocked}"
    )


# --- A: protected main file must BLOCK under every adversarial spelling -------


@pytest.mark.parametrize(
    "spelling_name",
    [
        "leading_ws",
        "trailing_ws",
        "both_ws",
        "double_quote",
        "single_quote",
        "pad_double_quote",
        "leading_tab",
        "pad_single_quote",
    ],
)
def test_main_protected_file_blocked_under_adversarial_spelling(
    repo_pair: tuple[Path, Path],
    spelling_name: str,
) -> None:
    primary, _wt = repo_pair
    spellings = dict(_adversarial_spellings(_main_abs(primary)))
    raw = spellings[spelling_name]
    # Main-checkout file: production root is the primary worktree.
    assert_cross_guard_verdict(raw, primary, expect_block=True)


# --- B: control arm — feature-worktree path ALLOWED under same spellings ------
# GUARDPIN-REV-01: repo_root must be the feature worktree so the path is
# relativizable and resolve_path_branch is actually reached. With
# repo_root=primary the absolute feature path is an outsider and this arm
# cannot go red under a constant-"main" mutation of line 477.


@pytest.mark.parametrize(
    "spelling_name",
    [
        "leading_ws",
        "trailing_ws",
        "both_ws",
        "double_quote",
        "single_quote",
        "pad_double_quote",
        "leading_tab",
        "pad_single_quote",
    ],
)
def test_feature_worktree_file_allowed_under_adversarial_spelling(
    repo_pair: tuple[Path, Path],
    spelling_name: str,
) -> None:
    _primary, wt = repo_pair
    spellings = dict(_adversarial_spellings(_feature_abs(wt)))
    raw = spellings[spelling_name]
    # Production shape inside a linked worktree: show-toplevel == worktree.
    assert_cross_guard_verdict(raw, wt, expect_block=False)


# --- B': feature path under primary root is an *outsider* from that root ------
# The pre-REV-01 control arm accidentally tested this shape. Keep it, labeled
# for what it actually is: unrelativizable absolute path outside repo_root.


def test_feature_path_is_outsider_when_repo_root_is_primary(
    repo_pair: tuple[Path, Path],
) -> None:
    primary, wt = repo_pair
    # Absolute path into the linked worktree is not under primary; both guards
    # must treat it as a genuine outsider (ALLOW), not as a main protected edit.
    assert_cross_guard_verdict(_feature_abs(wt), primary, expect_block=False)
    assert_cross_guard_verdict(
        f"  {_feature_abs(wt)}  ", primary, expect_block=False
    )


# --- C: genuine outsider is not treated as protected --------------------------


def test_outsider_paths_unblocked(repo_pair: tuple[Path, Path], tmp_path: Path) -> None:
    primary, _wt = repo_pair
    outsider_file = tmp_path / "genuine-outsider.txt"
    outsider_file.write_text("outside\n", encoding="utf-8")
    for raw in (
        "/dev/null",
        "  /dev/null  ",
        '"/dev/null"',
        str(outsider_file.resolve()),
        f"  {outsider_file.resolve()}  ",
        f'"{outsider_file.resolve()}"',
    ):
        assert_cross_guard_verdict(raw, primary, expect_block=False)


# --- plain absolute baseline (sanity; not a spelling attack) ------------------


def test_plain_absolute_main_blocked_and_feature_allowed(
    repo_pair: tuple[Path, Path],
) -> None:
    primary, wt = repo_pair
    assert_cross_guard_verdict(_main_abs(primary), primary, expect_block=True)
    # Feature control uses worktree root (same production shape as arm B).
    assert_cross_guard_verdict(_feature_abs(wt), wt, expect_block=False)
