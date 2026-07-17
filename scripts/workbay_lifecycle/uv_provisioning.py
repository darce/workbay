"""``uv`` preflight + ``uv sync --extra dev`` provisioning helpers.

Used by ``task-start`` (mandatory, before lifecycle state mutation) and
``slice-start`` (defensive, cheap on no-op). Honors the ``UV_BIN`` env
override so tests can stub the binary deterministically without
PATH manipulation.

``SYNC_PACKAGES=<csv>`` narrows the synced set to the named package
directories under ``<worktree>/packages/`` instead of every package
that ships a ``pyproject.toml``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path

try:  # py311+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for older runtimes
    tomllib = None  # type: ignore[assignment]

UV_BIN_ENV = "WORKBAY_LIFECYCLE_UV_BIN"
DEFAULT_UV_BIN = "uv"
SYNC_PACKAGES_ENV = "SYNC_PACKAGES"
ROOT_VENV_DIRNAME = ".venv"


def uv_bin() -> str:
    return os.environ.get(UV_BIN_ENV) or DEFAULT_UV_BIN


@dataclass(frozen=True)
class PreflightResult:
    ok: bool
    uv_path: str
    version_output: str
    error: str = ""


@dataclass(frozen=True)
class SyncResult:
    package: str
    package_dir: Path
    ok: bool
    stdout: str
    stderr: str
    returncode: int


def uv_preflight() -> PreflightResult:
    """Probe ``uv --version``. Returns success/error without raising."""
    binary = uv_bin()
    try:
        proc = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return PreflightResult(
            ok=False,
            uv_path=binary,
            version_output="",
            error=(
                f"uv preflight failed: {binary!r} not found on PATH. "
                "Install uv (https://docs.astral.sh/uv/) and retry."
            ),
        )
    if proc.returncode != 0:
        return PreflightResult(
            ok=False,
            uv_path=binary,
            version_output=proc.stdout,
            error=(
                f"uv preflight failed: {binary!r} exited {proc.returncode}. "
                f"stderr={proc.stderr.strip()!r}"
            ),
        )
    return PreflightResult(ok=True, uv_path=binary, version_output=proc.stdout.strip())


def discover_packages(worktree_root: Path, override: str | None = None) -> list[Path]:
    """Return package directories whose ``pyproject.toml`` should be synced.

    ``override`` is the raw ``SYNC_PACKAGES`` env value: a comma-separated
    list of package directory names under ``<worktree>/packages/``. When
    ``None`` (or empty), every package directory with a ``pyproject.toml``
    is returned in sorted order.
    """
    packages_dir = worktree_root / "packages"
    if not packages_dir.is_dir():
        return []
    if override:
        names = [name.strip() for name in override.split(",") if name.strip()]
        return [packages_dir / name for name in names]
    return sorted(
        entry
        for entry in packages_dir.iterdir()
        if entry.is_dir() and (entry / "pyproject.toml").is_file()
    )


def uv_sync_package(package_dir: Path) -> SyncResult:
    """Run ``uv sync --extra dev`` in ``package_dir``."""
    binary = uv_bin()
    try:
        proc = subprocess.run(
            [binary, "sync", "--extra", "dev"],
            cwd=str(package_dir),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return SyncResult(
            package=package_dir.name,
            package_dir=package_dir,
            ok=False,
            stdout="",
            stderr=f"command not found: {binary}",
            returncode=127,
        )
    return SyncResult(
        package=package_dir.name,
        package_dir=package_dir,
        ok=proc.returncode == 0,
        stdout=proc.stdout,
        stderr=proc.stderr,
        returncode=proc.returncode,
    )


def uv_sync_packages(
    worktree_root: Path,
    *,
    override: str | None = None,
    stream: object = sys.stderr,
) -> tuple[bool, list[SyncResult]]:
    """Sync each discovered package. Streams a one-line header per package.

    Returns ``(all_ok, results)``. Stops at the first failure so the
    operator sees the failing package's output without scrolling past
    later (still-pending) packages.
    """
    results: list[SyncResult] = []
    targets = discover_packages(worktree_root, override=override)
    for pkg in targets:
        if not (pkg / "pyproject.toml").is_file():
            stream.write(
                f"uv sync: skipping {pkg.name} (no pyproject.toml in {pkg})\n"
            )
            results.append(
                SyncResult(
                    package=pkg.name,
                    package_dir=pkg,
                    ok=False,
                    stdout="",
                    stderr=f"missing pyproject.toml in {pkg}",
                    returncode=2,
                )
            )
            return False, results
        stream.write(f"uv sync: {pkg.name}\n")
        result = uv_sync_package(pkg)
        if result.stdout:
            stream.write(result.stdout)
            if not result.stdout.endswith("\n"):
                stream.write("\n")
        if result.stderr:
            stream.write(result.stderr)
            if not result.stderr.endswith("\n"):
                stream.write("\n")
        results.append(result)
        if not result.ok:
            return False, results
    return True, results


def sync_packages_override() -> str | None:
    raw = os.environ.get(SYNC_PACKAGES_ENV)
    return raw if raw else None


# ---------------------------------------------------------------------------
# internal: root ``.venv`` provisioning
# ---------------------------------------------------------------------------
#
# internal syncs each *package* ``.venv`` but never creates a
# worktree-root ``.venv``. Without one, a bare ``pytest`` from the worktree
# root resolves through the pyenv shim and can import the *primary*
# checkout's source. This helper provisions ``<worktree>/.venv`` so root
# command resolution stays local to the worktree.
#
# Failure semantics are split by cause (see the task plan Constraints):
#   * venv creation / ``pytest`` install failure -> HARD fail (feeds the
#     task-start rollback path in implementation note).
#   * per-package editable install failure -> best-effort SKIP + warning,
#     because divergent per-package pins cannot always co-resolve in one
#     shared environment. The contract is "root ``pytest`` resolves locally
#     and the worktree source is importable", not full dependency coherence.


@dataclass(frozen=True)
class EditableInstall:
    package: str
    package_dir: Path
    installed: bool
    skipped: bool
    reason: str = ""
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


@dataclass(frozen=True)
class RootVenvResult:
    ok: bool
    venv_dir: Path
    python_path: Path
    pytest_path: Path
    created: bool = False
    installs: list[EditableInstall] = field(default_factory=list)
    commands: list[list[str]] = field(default_factory=list)
    failure_reason: str = ""
    stdout: str = ""
    stderr: str = ""
    # Structured heal subprocess receipt when ``_heal_dev_editables`` ran
    # ``scripts/dev_install.py``: ``{"returncode", "stdout", "stderr"}``.
    dev_install: dict[str, object] | None = None


def root_venv_dir(worktree_root: Path) -> Path:
    return worktree_root / ROOT_VENV_DIRNAME


def declares_dev_extra(package_dir: Path) -> bool:
    """Return True when ``package_dir`` declares a ``dev`` optional-dependency.

    Drives whether the root editable install requests the ``[dev]`` extra.
    Parses ``pyproject.toml`` with ``tomllib`` when available, falling back
    to a conservative text scan on older runtimes so the helper stays
    stdlib-only.
    """
    pyproject = package_dir / "pyproject.toml"
    if not pyproject.is_file():
        return False
    text = pyproject.read_text()
    if tomllib is not None:
        try:
            data = tomllib.loads(text)
        except tomllib.TOMLDecodeError:
            return False
        extras = data.get("project", {}).get("optional-dependencies", {})
        return isinstance(extras, dict) and "dev" in extras
    in_extras = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_extras = stripped == "[project.optional-dependencies]"
            continue
        if in_extras and stripped.split("=", 1)[0].strip() == "dev":
            return True
    return False


@dataclass(frozen=True)
class WorkspaceSyncResult:
    ok: bool
    stdout: str
    stderr: str
    returncode: int
    failure_reason: str = ""


def workspace_enabled(worktree_root: Path) -> bool:
    """True when ``worktree_root`` ships a uv workspace manifest (implementation note D1)."""
    pyproject = worktree_root / "pyproject.toml"
    if not pyproject.is_file():
        return False
    if tomllib is None:
        return "[tool.uv.workspace]" in pyproject.read_text()
    try:
        data = tomllib.loads(pyproject.read_text())
    except tomllib.TOMLDecodeError:
        return False
    return "workspace" in data.get("tool", {}).get("uv", {})


def uv_workspace_sync(
    worktree_root: Path,
    *,
    clear: bool = False,
    stream: object = sys.stderr,
) -> WorkspaceSyncResult:
    """Run ``uv sync`` from a workspace root (implementation note D3b)."""
    if not workspace_enabled(worktree_root):
        return WorkspaceSyncResult(
            ok=True,
            stdout="",
            stderr="",
            returncode=0,
            failure_reason="",
        )
    venv_dir = root_venv_dir(worktree_root)
    if clear and venv_dir.exists():
        import shutil

        shutil.rmtree(venv_dir)
    stream.write("uv sync: workspace\n")  # type: ignore[attr-defined]
    proc = _run_uv(["sync"], cwd=str(worktree_root))
    if proc.returncode != 0:
        return WorkspaceSyncResult(
            ok=False,
            stdout=proc.stdout,
            stderr=proc.stderr,
            returncode=proc.returncode,
            failure_reason=(
                f"workspace uv sync failed (exit {proc.returncode}): "
                f"{proc.stderr.strip()!r}"
            ),
        )
    if proc.stdout:
        stream.write(proc.stdout)  # type: ignore[attr-defined]
        if not proc.stdout.endswith("\n"):
            stream.write("\n")  # type: ignore[attr-defined]
    if proc.stderr:
        stream.write(proc.stderr)  # type: ignore[attr-defined]
        if not proc.stderr.endswith("\n"):
            stream.write("\n")  # type: ignore[attr-defined]
    return WorkspaceSyncResult(
        ok=True,
        stdout=proc.stdout,
        stderr=proc.stderr,
        returncode=0,
    )


def _run_uv(args: list[str], *, cwd: str | None = None) -> subprocess.CompletedProcess[str]:
    binary = uv_bin()
    try:
        return subprocess.run(
            [binary, *args],
            capture_output=True,
            text=True,
            check=False,
            cwd=cwd,
        )
    except FileNotFoundError:
        return subprocess.CompletedProcess(
            [binary, *args], 127, "", f"command not found: {binary}"
        )


def provision_root_venv(
    worktree_root: Path,
    *,
    override: str | None = None,
    clear: bool = False,
    stream: object = sys.stderr,
) -> RootVenvResult:
    """Create and validate ``<worktree>/.venv`` for local test resolution.

    Returns a structured result. ``ok`` is False only on a hard failure
    (venv creation or ``pytest`` install); per-package editable-install
    conflicts are recorded as ``skipped`` outcomes and do not flip ``ok``.
    When no package targets are discovered the step is a no-op success and
    the venv is not created (compatible with package-less repos).

    ``clear`` re-provisions an existing ``.venv`` in place by passing
    ``uv venv --clear``. The inline ``task-start`` / fresh-lane callers leave
    it False so an unexpected pre-existing venv still fails loudly; the manual
    ``provision-env`` recovery path opts in so an operator can (re)provision an
    existing worktree's root venv without first deleting it by hand. ``uv venv``
    otherwise aborts with "A virtual environment already exists ... Use --clear
    to replace it" — see the internal-followups branch review.
    """
    venv_dir = root_venv_dir(worktree_root)
    python_path = venv_dir / "bin" / "python"
    pytest_path = venv_dir / "bin" / "pytest"
    commands: list[list[str]] = []

    targets = discover_packages(worktree_root, override=override)
    if not targets:
        return RootVenvResult(
            ok=True,
            venv_dir=venv_dir,
            python_path=python_path,
            pytest_path=pytest_path,
            created=False,
            commands=commands,
        )

    # 1) Create the venv (HARD requirement). ``--clear`` (opt-in) replaces an
    # existing ``.venv`` so the manual recovery path is idempotent.
    venv_args = ["venv", str(venv_dir), "--seed"]
    if clear:
        venv_args.append("--clear")
    commands.append(venv_args)
    proc = _run_uv(venv_args)
    if proc.returncode != 0:
        return RootVenvResult(
            ok=False,
            venv_dir=venv_dir,
            python_path=python_path,
            pytest_path=pytest_path,
            commands=commands,
            failure_reason=(
                f"root venv creation failed (exit {proc.returncode}): "
                f"{proc.stderr.strip()!r}"
            ),
            stdout=proc.stdout,
            stderr=proc.stderr,
        )
    if not python_path.exists():
        return RootVenvResult(
            ok=False,
            venv_dir=venv_dir,
            python_path=python_path,
            pytest_path=pytest_path,
            commands=commands,
            failure_reason=(
                f"root venv created but {python_path} is missing"
            ),
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    # 2) Install pytest (HARD requirement).
    pytest_args = ["pip", "install", "--python", str(python_path), "pytest"]
    commands.append(pytest_args)
    proc = _run_uv(pytest_args)
    if proc.returncode != 0:
        return RootVenvResult(
            ok=False,
            venv_dir=venv_dir,
            python_path=python_path,
            pytest_path=pytest_path,
            created=True,
            commands=commands,
            failure_reason=(
                f"pytest install into root venv failed (exit {proc.returncode}): "
                f"{proc.stderr.strip()!r}"
            ),
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    # 3) Editable installs of discovered packages (BEST-EFFORT per package).
    installs: list[EditableInstall] = []
    for pkg in targets:
        spec = f"{pkg}[dev]" if declares_dev_extra(pkg) else str(pkg)
        pkg_args = ["pip", "install", "--python", str(python_path), "-e", spec]
        commands.append(pkg_args)
        proc = _run_uv(pkg_args)
        if proc.returncode == 0:
            installs.append(
                EditableInstall(
                    package=pkg.name,
                    package_dir=pkg,
                    installed=True,
                    skipped=False,
                    stdout=proc.stdout,
                    stderr=proc.stderr,
                    returncode=0,
                )
            )
            continue
        reason = (
            f"editable install of {pkg.name} failed (exit {proc.returncode}); "
            f"skipped to preserve best-effort provisioning: {proc.stderr.strip()!r}"
        )
        stream.write(f"provision-env: skipping {pkg.name} — {reason}\n")  # type: ignore[attr-defined]
        installs.append(
            EditableInstall(
                package=pkg.name,
                package_dir=pkg,
                installed=False,
                skipped=True,
                reason=reason,
                stdout=proc.stdout,
                stderr=proc.stderr,
                returncode=proc.returncode,
            )
        )

    return RootVenvResult(
        ok=True,
        venv_dir=venv_dir,
        python_path=python_path,
        pytest_path=pytest_path,
        created=True,
        installs=installs,
        commands=commands,
    )


def _heal_dev_editables(
    worktree_root: Path, result: RootVenvResult, *, stream: object = sys.stderr
) -> RootVenvResult:
    """Replace copy-editables with src redirects after a successful sync.

    ``uv sync`` reinstalls workspace members as copies (no PEP 660), which
    reverts the dev-install src redirects. Without this heal a freshly
    provisioned worktree serves stale copies and fails ``make check-dev-editables``
    — and, for the handoff package, a stale copy can serve a schema older than
    the DB (internal). Idempotent; a heal failure fails the
    provision so the caller surfaces it rather than silently proceeding on copies.
    """
    if not (result.ok and result.created):
        return result
    dev_install = worktree_root / "scripts" / "dev_install.py"
    if not dev_install.is_file():
        # Monorepo roots expect dev editables: missing script is fail-closed.
        # Non-monorepo trees (no packages/) keep the silent skip.
        if (worktree_root / "packages").is_dir():
            msg = f"dev_install_missing: {dev_install}"
            stream.write(  # type: ignore[attr-defined]
                f"dev-install (redirect heal) failed: {msg}\n"
            )
            return replace(
                result,
                ok=False,
                failure_reason=msg,
                stderr=(result.stderr or "") + msg + "\n",
            )
        return result
    proc = subprocess.run(
        [
            str(result.python_path),
            str(dev_install),
            "--repo",
            str(worktree_root),
            "--venv",
            str(result.venv_dir),
            "--emit-json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    receipt: dict[str, object] = {
        "returncode": proc.returncode,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
    }
    if proc.returncode != 0:
        stream.write(  # type: ignore[attr-defined]
            f"dev-install (redirect heal) failed (exit {proc.returncode}): "
            f"{proc.stderr.strip()}\n"
        )
        return replace(
            result,
            ok=False,
            failure_reason="dev_install_failed",
            # Keep stderr-append for backwards compatibility; also surface
            # heal stdout on the result so callers are not stuck with only
            # the nested receipt for diagnosis.
            stdout=(result.stdout or "") + (proc.stdout or ""),
            stderr=(result.stderr or "") + proc.stderr,
            dev_install=receipt,
        )
    stream.write("dev-install: healed src redirects in worktree .venv\n")  # type: ignore[attr-defined]
    return replace(result, dev_install=receipt)


def provision_worktree_env(
    worktree_root: Path,
    *,
    override: str | None = None,
    clear: bool = False,
    stream: object = sys.stderr,
) -> RootVenvResult:
    """Provision a worktree environment for local test/hook resolution.

    When a uv workspace manifest is present (implementation note), run a single
    ``uv sync`` from the worktree root. Otherwise fall back to per-package
    ``uv sync --extra dev`` plus the legacy root-venv editable-install path.
    After a successful sync the copy-editables are healed back to src redirects
    (``_heal_dev_editables``) so the worktree serves live src, not stale copies.
    """
    if workspace_enabled(worktree_root):
        sync = uv_workspace_sync(worktree_root, clear=clear, stream=stream)
        venv_dir = root_venv_dir(worktree_root)
        python_path = venv_dir / "bin" / "python"
        pytest_path = venv_dir / "bin" / "pytest"
        if not sync.ok:
            return RootVenvResult(
                ok=False,
                venv_dir=venv_dir,
                python_path=python_path,
                pytest_path=pytest_path,
                commands=[["sync"]],
                failure_reason=sync.failure_reason,
                stdout=sync.stdout,
                stderr=sync.stderr,
            )
        created = python_path.is_file()
        if not created:
            return RootVenvResult(
                ok=False,
                venv_dir=venv_dir,
                python_path=python_path,
                pytest_path=pytest_path,
                commands=[["sync"]],
                failure_reason=(
                    f"workspace uv sync succeeded but {python_path} is missing"
                ),
                stdout=sync.stdout,
                stderr=sync.stderr,
            )
        return _heal_dev_editables(
            worktree_root,
            RootVenvResult(
                ok=True,
                venv_dir=venv_dir,
                python_path=python_path,
                pytest_path=pytest_path,
                created=created,
                commands=[["sync"]],
                stdout=sync.stdout,
                stderr=sync.stderr,
            ),
            stream=stream,
        )

    sync_ok, _sync_results = uv_sync_packages(
        worktree_root, override=override, stream=stream
    )
    if not sync_ok:
        venv_dir = root_venv_dir(worktree_root)
        return RootVenvResult(
            ok=False,
            venv_dir=venv_dir,
            python_path=venv_dir / "bin" / "python",
            pytest_path=venv_dir / "bin" / "pytest",
            failure_reason="per-package uv sync failed before root venv provisioning",
        )
    return _heal_dev_editables(
        worktree_root,
        provision_root_venv(
            worktree_root, override=override, clear=clear, stream=stream
        ),
        stream=stream,
    )


def root_venv_env(
    worktree_root: Path, base_env: dict[str, str] | None = None
) -> dict[str, str]:
    """Return an env mapping with ``<worktree>/.venv/bin`` prepended to PATH.

    No-op (returns a copy of ``base_env``) when the root venv ``bin``
    directory does not exist, so callers can apply it unconditionally.
    """
    env = dict(base_env if base_env is not None else os.environ)
    venv_bin = root_venv_dir(worktree_root) / "bin"
    if venv_bin.is_dir():
        existing = env.get("PATH", "")
        env["PATH"] = str(venv_bin) + (os.pathsep + existing if existing else "")
        env["VIRTUAL_ENV"] = str(root_venv_dir(worktree_root))
    return env
