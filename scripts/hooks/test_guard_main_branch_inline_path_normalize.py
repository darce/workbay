"""Pin normalize-before-resolve at ``_guard_main_branch_inline.py:149``.

GUARDPIN-REV-02 / F-HARM-S2-01 follow-up: the dirty-main short-circuit in the
inline guard must feed ``normalize_path_token`` into ``resolve_path_branch``.
Without that normalize, a padded/quoted absolute token re-anchors through
``Path(...).resolve()`` to the hook cwd (a linked feature worktree) and the
short-circuit treats the path as a non-protected branch — silent ALLOW when
main has dirty protected files.

``check_file_edit`` (line 477) already blocks padded *main protected file*
edits before the dirty arm runs, so those spellings never reach line 149 while
477 is intact. The load-bearing path that *does* reach line 149 is: main has
dirty protected state, the current edit is an unrelativizable outsider (or
any path that does not identify the repo root), and process cwd is inside a
feature worktree. Under that shape, normalize is what keeps
``resolve_path_branch`` from reporting the feature branch and short-circuiting
past the dirty-main WARNING.

Harness shape follows ``test_guard_main_branch_active_task_probe.py``: drive the
real ``_guard_main_branch_inline.py`` entry point as a subprocess with
``REPO_ROOT`` + ``BRANCH`` argv and a JSON tool payload on stdin. Assertions are
on observable stdout/stderr verdicts only — never on line numbers or source text.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent
INLINE_SCRIPT = HOOKS_DIR / "_guard_main_branch_inline.py"
REPO_ROOT = HOOKS_DIR.parent.parent

sys.path.insert(0, str(HOOKS_DIR))
from _harness_protocol import CONTRACT_RELATIVE_PATH  # noqa: E402


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture()
def dirty_main_with_feature_wt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, str]:
    """Primary on main with dirty protected file + linked feature worktree.

    Process cwd is the feature worktree (defect precondition for re-anchor).
    Returns ``(primary, worktree, main_protected_abs)``.
    """
    if not INLINE_SCRIPT.is_file():
        pytest.skip("_guard_main_branch_inline.py is not present in this tree")

    contract_src = REPO_ROOT / CONTRACT_RELATIVE_PATH
    if not contract_src.is_file():
        pytest.skip(f"harness contract missing at {contract_src}")

    primary = tmp_path / "primary"
    primary.mkdir()
    _git(primary, "init", "-b", "main")
    _git(primary, "config", "user.email", "t@example.invalid")
    _git(primary, "config", "user.name", "t")
    (primary / "Makefile").write_text("all:\n\ttrue\n")
    pkg = primary / "packages" / "pkg"
    pkg.mkdir(parents=True)
    (pkg / "mod.py").write_text("x = 1\n")
    # Policy contract must load from the synthetic repo root.
    dest = primary / CONTRACT_RELATIVE_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(contract_src, dest)
    _git(primary, "add", "-A")
    _git(primary, "commit", "-m", "init")
    # Dirty protected surface on main — forces the dirty-path arm.
    (pkg / "mod.py").write_text("x = 2 dirty\n")
    worktree = tmp_path / "wt"
    _git(primary, "worktree", "add", "-b", "feature/task", str(worktree))
    monkeypatch.chdir(worktree)
    main_abs = str((primary / "packages" / "pkg" / "mod.py").resolve())
    return primary, worktree, main_abs


def _run_inline(
    primary: Path,
    *,
    file_path: str,
    branch: str = "main",
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    payload = {
        "tool_name": "Edit",
        "tool_input": {
            "file_path": file_path,
            "old_string": "a",
            "new_string": "b",
        },
    }
    return subprocess.run(
        [sys.executable, str(INLINE_SCRIPT), str(primary), branch],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=str(cwd) if cwd is not None else None,
        timeout=30,
    )


def _verdict(proc: subprocess.CompletedProcess[str]) -> str:
    out = proc.stdout or ""
    err = proc.stderr or ""
    if out.strip().startswith("BLOCKED"):
        return "BLOCK"
    if "WARNING" in err or "WARNING" in out:
        return "WARN"
    return "ALLOW"


# --- main-file adversarial spellings (blocked by check_file_edit before 149) -


@pytest.mark.parametrize(
    "spell",
    [
        lambda p: f"  {p}",
        lambda p: f"{p}  ",
        lambda p: f"  {p}  ",
        lambda p: f'"{p}"',
        lambda p: f"'{p}'",
        lambda p: f'  "{p}"  ',
        lambda p: f"\t{p}",
        lambda p: f"  '{p}'  ",
    ],
    ids=[
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
def test_inline_blocks_padded_main_protected_file(
    dirty_main_with_feature_wt: tuple[Path, Path, str],
    spell,
) -> None:
    """Padded/quoted absolute main-file paths must BLOCK via the inline entry.

    These hit ``check_file_edit`` first (normalize at line 477). They document
    the brief's requested main-file spelling drive of the inline script; they
    are not the mutation killer for line 149 alone (see dirty-outsider arm).
    """
    primary, wt, main_abs = dirty_main_with_feature_wt
    raw = spell(main_abs)
    proc = _run_inline(primary, file_path=raw, cwd=wt)
    assert proc.returncode == 0, (
        f"inline script hard-failed rc={proc.returncode}\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert _verdict(proc) == "BLOCK", (
        f"expected BLOCK for padded main path {raw!r}, got {_verdict(proc)}\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )


# --- load-bearing pin for line 149: dirty main + adversarial outsider ---------


@pytest.mark.parametrize(
    "raw_path",
    [
        "  /dev/null  ",
        '"/dev/null"',
        "  '/dev/null'  ",
    ],
    ids=["pad_ws_devnull", "double_quote_devnull", "pad_single_quote_devnull"],
)
def test_inline_dirty_main_outsider_not_silent_short_circuit(
    dirty_main_with_feature_wt: tuple[Path, Path, str],
    raw_path: str,
) -> None:
    """Dirty-main + padded outsider must WARN, not silent-ALLOW via re-anchor.

    When main has dirty protected files and the edit target is a genuine
    outsider, production falls through the per-path short-circuit (path does
    not identify the repo root; normalized resolve yields no branch → harness
    branch ``main`` stays protected) into the BR-21 WARN+allow carve-out.

    Reverting line 149 to ``resolve_path_branch(p)`` makes the padded token
    re-anchor to the feature-worktree cwd, report a non-protected branch, and
    silent-ALLOW — suppressing the dirty-main warning. That flip is the
    acceptance criterion for GUARDPIN-REV-02.
    """
    primary, wt, _main_abs = dirty_main_with_feature_wt
    proc = _run_inline(primary, file_path=raw_path, cwd=wt)
    assert proc.returncode == 0, (
        f"inline script hard-failed rc={proc.returncode}\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert _verdict(proc) == "WARN", (
        f"expected WARN (dirty-main + outsider) for {raw_path!r}, "
        f"got {_verdict(proc)}. Silent ALLOW means the dirty short-circuit "
        f"re-anchored an un-normalized token to the feature cwd (line 149 "
        f"normalize missing).\nstdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert "dirty on main" in (proc.stderr or "").lower() or "WARNING" in (
        proc.stderr or ""
    ), (
        f"expected dirty-main warning text on stderr for {raw_path!r}\n"
        f"stderr={proc.stderr!r}"
    )


def test_inline_feature_worktree_path_allowed_despite_dirty_main(
    dirty_main_with_feature_wt: tuple[Path, Path, str],
) -> None:
    """Control: a real feature-worktree edit must still short-circuit ALLOW.

    Without this arm, a hook that always WARNs/BLOCKs on dirty main would
    satisfy the outsider pin while breaking the sibling-worktree unblock that
    line 149 exists to provide.
    """
    primary, wt, _main_abs = dirty_main_with_feature_wt
    feat_abs = str((wt / "packages" / "pkg" / "mod.py").resolve())
    # Adversarial spelling of the feature path — normalize must still allow.
    raw = f"  {feat_abs}  "
    proc = _run_inline(primary, file_path=raw, cwd=wt)
    assert proc.returncode == 0, (
        f"inline script hard-failed rc={proc.returncode}\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert _verdict(proc) == "ALLOW", (
        f"feature-worktree edit under dirty main must silent-ALLOW via "
        f"per-path short-circuit; got {_verdict(proc)} for {raw!r}\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
