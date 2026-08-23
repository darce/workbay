"""An unimportable ``_worktree_identity`` must not take the guard down.

``_primary_workspace_root`` resolves the primary worktree through a bare
``from _worktree_identity import primary_workspace_root`` executed at call
time. That import depends on the hooks directory being on ``sys.path``, which
is true only when a hook is invoked as a top-level script from that directory.
Every other consumer shape — a nested hook whose ``sys.path[0]`` is a
subdirectory, an overlay copy, a caller that imports the resolver as a module —
gets ``ModuleNotFoundError``.

The two call sites that wrap the resolver guard only ``subprocess.TimeoutExpired``
(a leftover from when resolution shelled out to git), so the import error escapes
both and kills the hook process outright. A guard that crashes is strictly worse
than one that cannot determine an answer: the crash is what the resolver's own
docstring promises never happens ("returns ``None`` when the layout cannot be
determined").

Pinned here: the import failure degrades to ``None`` — the resolver's documented
could-not-determine value — and neither call site raises. The controls exist so
the degrade cannot be bought by making the resolver useless: with the helper
importable it must still return the true primary for a linked worktree, and must
still return ``None`` outside any repository.

Both hook twins are covered. They already drift in other respects, but they
share this defect verbatim (root tree line 98, payload tree line 100), so a fix
to one is not a fix.
"""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[6]
ROOT_HOOKS = REPO_ROOT / "scripts" / "hooks"
PAYLOAD_HOOKS = (
    REPO_ROOT
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "scripts"
    / "hooks"
)

# The helper the resolver imports by bare name. Written out literally rather
# than read off the module under test, so the oracle cannot drift with it.
IDENTITY_MODULE_NAME = "_worktree_identity"

TWINS = [
    pytest.param(ROOT_HOOKS, id="root"),
    pytest.param(PAYLOAD_HOOKS, id="payload"),
]


def _load_twin(hooks_dir: Path) -> ModuleType:
    """Load one twin under a unique name so both can coexist in one session."""
    path = hooks_dir / "_active_task_context.py"
    name = f"_atc_twin_{hooks_dir.parent.parent.name}_{hooks_dir.name}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _identity_importable() -> None:
    """Every case starts with the helper reachable; the REDs remove it."""
    if str(ROOT_HOOKS) not in sys.path:
        sys.path.insert(0, str(ROOT_HOOKS))


def _make_repo_with_linked_worktree(tmp_path: Path) -> tuple[Path, Path]:
    """A real primary repo plus one linked worktree, no test-double anywhere."""
    primary = tmp_path / "primary"
    primary.mkdir()
    env = {
        "GIT_AUTHOR_NAME": "pin",
        "GIT_AUTHOR_EMAIL": "pin@example.invalid",
        "GIT_COMMITTER_NAME": "pin",
        "GIT_COMMITTER_EMAIL": "pin@example.invalid",
        "PATH": "/usr/bin:/bin:/usr/local/bin",
        "HOME": str(tmp_path),
    }

    def _git(*args: str) -> None:
        subprocess.run(
            ["git", *args], cwd=primary, env=env, check=True, capture_output=True
        )

    _git("init", "-q", "-b", "main")
    (primary / "seed.txt").write_text("seed\n", encoding="utf-8")
    _git("add", "seed.txt")
    _git("commit", "-qm", "seed")
    linked = tmp_path / "linked"
    _git("worktree", "add", "-q", "-b", "feature/pin", str(linked))
    return primary, linked


def _dir_outside_any_repo() -> Path:
    """A temp dir with no ``.git`` in any ancestor (a true negative)."""
    for base in (Path("/var/tmp"), Path.home(), Path("/tmp")):
        if not base.is_dir():
            continue
        cursor = base.resolve(strict=False)
        blocked = False
        for _ in range(128):
        # Over-approximation is deliberate: bare .git existence is the SAFE
        # direction for this OPPOSITE contract (find a temp base with no repo
        # above it). Rejecting doubtful .git entries is more conservative about
        # isolation; do not harden this probe to the production ascent predicate.
            if (cursor / ".git").exists():
                blocked = True
                break
            parent = cursor.parent
            if parent == cursor:
                break
            cursor = parent
        if not blocked:
            return Path(tempfile.mkdtemp(prefix="atc-identity-", dir=str(base)))
    pytest.skip("no base directory free of .git ancestors on this host")


# --------------------------------------------------------------------------
# RED: the import failure must degrade, not escape.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("hooks_dir", TWINS)
def test_unimportable_identity_helper_resolves_to_none(
    hooks_dir: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``None`` is the resolver's own documented could-not-determine value."""
    module = _load_twin(hooks_dir)
    # ``None`` in sys.modules is the stdlib's own "this module is unimportable"
    # sentinel: the import statement raises ImportError without touching disk.
    monkeypatch.setitem(sys.modules, IDENTITY_MODULE_NAME, None)

    try:
        result = module._primary_workspace_root(tmp_path)
    except ImportError as exc:  # ModuleNotFoundError is a subclass
        pytest.fail(
            f"{hooks_dir.name} twin: the import escaped the resolver and would "
            f"kill the hook process: {type(exc).__name__}: {exc}"
        )
    assert result is None, (
        "an unimportable identity helper must read as could-not-determine, "
        f"not as the fabricated answer {result!r}"
    )


@pytest.mark.parametrize("hooks_dir", TWINS)
@pytest.mark.parametrize(
    "call_site", ["_try_load_active_task_from_snapshot", "_load_active_task"]
)
def test_neither_guarded_call_site_lets_the_import_escape(
    hooks_dir: Path,
    call_site: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Both wrappers catch only TimeoutExpired today, so the error escapes."""
    module = _load_twin(hooks_dir)
    resolver = getattr(module, call_site, None)
    assert resolver is not None, f"{hooks_dir.name} twin has no {call_site}"
    monkeypatch.setitem(sys.modules, IDENTITY_MODULE_NAME, None)

    try:
        resolver(tmp_path)
    except ImportError as exc:
        pytest.fail(
            f"{hooks_dir.name} twin: {call_site} let {type(exc).__name__} escape; "
            "the hook dies instead of degrading"
        )


# --------------------------------------------------------------------------
# Controls: the degrade must not be bought by making the resolver useless.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("hooks_dir", TWINS)
def test_control_importable_helper_still_resolves_the_true_primary(
    hooks_dir: Path, tmp_path: Path
) -> None:
    """Red if the fix returns ``None`` unconditionally to dodge the import."""
    module = _load_twin(hooks_dir)
    primary, linked = _make_repo_with_linked_worktree(tmp_path)
    expected = str(primary.resolve(strict=False))

    assert module._primary_workspace_root(linked) == expected, (
        "resolution from a linked worktree regressed; the fix must keep "
        "answering, not stop answering"
    )
    assert module._primary_workspace_root(primary) == expected


@pytest.mark.parametrize("hooks_dir", TWINS)
def test_control_outside_any_repo_is_still_none(hooks_dir: Path) -> None:
    """Red if the fix starts fabricating the caller's own directory."""
    module = _load_twin(hooks_dir)
    isolated = _dir_outside_any_repo()
    try:
        outside = isolated / "not-a-repo"
        outside.mkdir()
        assert module._primary_workspace_root(outside) is None, (
            "outside a repository the resolver must return None, never the "
            "caller's own path"
        )
    finally:
        shutil.rmtree(isolated, ignore_errors=True)
