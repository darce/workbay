"""Provision one shipped advisory helper closure without installing an overlay.

The catalogue is intentionally small. AST validation includes imports nested in
functions and try/except blocks; no payload module is imported or executed.
Directory descriptors and exclusive creates bound filesystem races. Rollback
removes only files whose inode and bytes still match this call's writes. This is
best effort against concurrent writers, not a filesystem-wide transaction.
"""

from __future__ import annotations

import ast
import hashlib
import os
import stat
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path

# Every local dependency is explicit, including optional imports. A payload
# change must update this catalogue deliberately rather than copy a directory.
_CLOSURE = {
    "advise-worktree-cd.py": frozenset({"_active_task_context", "_protocol"}),
    "_active_task_context.py": frozenset({"_worktree_identity"}),
    "_worktree_identity.py": frozenset(),
    "_protocol.py": frozenset({"_interp"}),
    "_interp.py": frozenset(),
}
_EXTERNAL = frozenset({"workbay_handoff_mcp", "workbay_protocol"})
_MAX_SOURCE_BYTES = 1024 * 1024
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


class _Refused(Exception):
    pass


def _payload_root() -> Path:
    # Use the existing installed-package resolver with no source override.
    from .install import _package_source_root

    return _package_source_root(None)


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _keep(stack: ExitStack, descriptor: int) -> int:
    stack.callback(os.close, descriptor)
    return descriptor


def _absolute_directory(path: Path) -> int:
    """Open every component without following symlinks; caller owns the fd."""
    if not path.is_absolute() or ".." in path.parts:
        raise _Refused(f"unsafe_path: absolute path without '..' required: {path}")
    descriptor = os.open(path.anchor, _DIR_FLAGS)
    try:
        for part in path.parts[1:]:
            child = os.open(part, _DIR_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_file(directory: int, name: str, *, limit: int = _MAX_SOURCE_BYTES) -> tuple[bytes, os.stat_result]:
    info = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise _Refused(f"unsafe_file: regular non-symlink file required: {name}")
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or _identity(opened) != _identity(info):
            raise _Refused(f"file_changed: {name}")
        chunks = bytearray()
        while len(chunks) <= limit:
            chunk = os.read(descriptor, min(65536, limit + 1 - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
        after = os.fstat(descriptor)
        named = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if len(chunks) > limit:
            raise _Refused(f"file_too_large: {name}")
        if (
            _identity(after) != _identity(named)
            or not stat.S_ISREG(named.st_mode)
            or (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
            != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        ):
            raise _Refused(f"file_changed: {name}")
        return bytes(chunks), after
    finally:
        os.close(descriptor)


def _validate_imports(name: str, data: bytes) -> None:
    tree = ast.parse(data, filename=name)
    local = set()
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                raise _Refused(f"source_import: unsupported relative import in {name}")
            modules.append(node.module or "")
        elif isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.Call) and (
            isinstance(node.func, ast.Name)
            and node.func.id == "__import__"
            or isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
        ):
            if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
                raise _Refused(f"source_import: unbounded dynamic import in {name}")
            modules.append(node.args[0].value)
    for module in modules:
        root = module.split(".")[0]
        if f"{root}.py" in _CLOSURE:
            if module != root:
                raise _Refused(f"source_import: unsupported local package in {name}: {module}")
            local.add(root)
        elif root not in sys.stdlib_module_names and root not in _EXTERNAL:
            raise _Refused(f"source_import: uncatalogued dependency in {name}: {module}")
    if local != _CLOSURE[name]:
        raise _Refused(f"source_import: local dependency catalogue differs for {name}")


def _load_sources(source: dict[str, object]) -> dict[str, bytes]:
    try:
        root = _payload_root()
        source["root"] = str(root)
        with ExitStack() as stack:
            root_fd = _keep(stack, _absolute_directory(root))
            scripts = _keep(stack, os.open("scripts", _DIR_FLAGS, dir_fd=root_fd))
            hooks = _keep(stack, os.open("hooks", _DIR_FLAGS, dir_fd=scripts))
            result = {}
            digests = {}
            for name in sorted(_CLOSURE):
                data, _ = _read_file(hooks, name)
                _validate_imports(name, data)
                result[name] = data
                digests[f"scripts/hooks/{name}"] = hashlib.sha256(data).hexdigest()
            source["files"] = digests
            return result
    except (OSError, ValueError, SyntaxError, ImportError, _Refused) as exc:
        raise _Refused(f"source_unavailable: verify installed workbay-system helper payload: {exc}") from exc


def _git_root(target: Path) -> None:
    # Ignore ambient Git location overrides, including a candidate-commit
    # GIT_DIR. rev-parse reads metadata only; it cannot execute repository hooks.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", "-C", str(target), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            env=env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise _Refused(f"target_git: cannot verify repository root: {exc}") from exc
    if result.returncode or Path(result.stdout.strip()) != target:
        raise _Refused("target_git: target must be the existing actual Git worktree root")


def _bound(target: Path, root: int, scripts: int | None = None, hooks: int | None = None) -> None:
    """Confirm held directories are still reachable through the explicit root."""
    with ExitStack() as stack:
        current = _keep(stack, _absolute_directory(target))
        if _identity(os.fstat(current)) != _identity(os.fstat(root)):
            raise _Refused("target_changed: repository directory was replaced")
        for name, expected in (("scripts", scripts), ("hooks", hooks)):
            if expected is None:
                break
            current = _keep(stack, os.open(name, _DIR_FLAGS, dir_fd=current))
            if _identity(os.fstat(current)) != _identity(os.fstat(expected)):
                raise _Refused(f"target_changed: {name} directory was replaced")


def _existing_directory(parent: int, name: str, stack: ExitStack) -> int | None:
    try:
        return _keep(stack, os.open(name, _DIR_FLAGS, dir_fd=parent))
    except FileNotFoundError:
        return None


def _actions(hooks: int | None, sources: dict[str, bytes]) -> list[dict[str, str]]:
    files = []
    for name, data in sources.items():
        action = "missing"
        if hooks is not None:
            try:
                current, _ = _read_file(hooks, name)
            except FileNotFoundError:
                pass
            else:
                if current != data:
                    raise _Refused(f"target_conflict: preserve existing different file scripts/hooks/{name}")
                action = "identical"
        files.append({"path": f"scripts/hooks/{name}", "sha256": hashlib.sha256(data).hexdigest(), "action": action})
    return files


def _rollback(hooks: int | None, created: list, directories: list) -> list[str]:
    retained = []
    if hooks is not None:
        for name, identity, written in reversed(created):
            try:
                current, info = _read_file(hooks, name)
                if _identity(info) != identity or current != bytes(written):
                    retained.append(f"scripts/hooks/{name}")
                    continue
                os.unlink(name, dir_fd=hooks)
            except FileNotFoundError:
                continue
            except (OSError, _Refused):
                retained.append(f"scripts/hooks/{name}")
    for parent, name, identity in reversed(directories):
        try:
            if _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) == identity:
                os.rmdir(name, dir_fd=parent)
        except OSError:
            pass  # Nonempty or replaced directories belong to another writer.
    return retained


def provision_hook_helpers(
    *, target: Path, helper: str = "advise-worktree-cd", check: bool = False
) -> dict[str, object]:
    """Return a ready/missing/refused receipt for this exact helper closure.

    Check mode never creates anything. Apply accepts identical files and creates
    missing regular files only, after full source and destination preflight.
    Conflicts, unsafe paths, missing payloads and bounded I/O failures refuse.
    No hooks, configuration, permissions, package bindings or refs are updated.
    """
    target = Path(target)
    source: dict[str, object] = {"kind": "package", "package": "workbay-system", "root": None, "files": {}}
    receipt: dict[str, object] = {
        "ok": False,
        "status": "refused",
        "target": str(target.absolute()),
        "helper": helper,
        "source": source,
        "files": [],
        "reason": None,
    }
    created: list = []
    directories: list = []
    hooks = None
    with ExitStack() as stack:
        try:
            if helper != "advise-worktree-cd":
                raise _Refused("helper_not_allowed: only advise-worktree-cd is shipped by this route")
            root = _keep(stack, _absolute_directory(target))
            _git_root(target)
            sources = _load_sources(source)
            scripts = _existing_directory(root, "scripts", stack)
            hooks = _existing_directory(scripts, "hooks", stack) if scripts is not None else None
            files = _actions(hooks, sources)
            receipt["files"] = files
            _bound(target, root, scripts, hooks)
            if check:
                missing = any(entry["action"] == "missing" for entry in files)
                receipt.update(
                    ok=not missing,
                    status="missing" if missing else "ready",
                    reason="helpers_missing: apply provision-hook-helpers to materialize missing files"
                    if missing
                    else None,
                )
                return receipt

            if scripts is None:
                os.mkdir("scripts", dir_fd=root)
                scripts = _keep(stack, os.open("scripts", _DIR_FLAGS, dir_fd=root))
                directories.append((root, "scripts", _identity(os.fstat(scripts))))
            if hooks is None:
                _bound(target, root, scripts)
                os.mkdir("hooks", dir_fd=scripts)
                hooks = _keep(stack, os.open("hooks", _DIR_FLAGS, dir_fd=scripts))
                directories.append((scripts, "hooks", _identity(os.fstat(hooks))))
            _bound(target, root, scripts, hooks)
            # A concurrent file appearing after preflight must be refused even
            # when identical: it was not one of the accepted existing files.
            for entry in files:
                name = Path(entry["path"]).name
                _bound(target, root, scripts, hooks)
                if entry["action"] == "identical":
                    current, _ = _read_file(hooks, name)
                    if current != sources[name]:
                        raise _Refused(f"target_changed: {entry['path']}")
                    continue
                descriptor = os.open(
                    name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o644, dir_fd=hooks
                )
                written = bytearray()
                created.append((name, _identity(os.fstat(descriptor)), written))
                try:
                    data = sources[name]
                    while len(written) < len(data):
                        count = os.write(descriptor, data[len(written) :])
                        if count <= 0:
                            raise OSError("short helper write")
                        written.extend(data[len(written) : len(written) + count])
                finally:
                    os.close(descriptor)
                entry["action"] = "created"
            _bound(target, root, scripts, hooks)
            if any(entry["action"] != "identical" for entry in _actions(hooks, sources)):
                raise _Refused("target_changed: helper disappeared after creation")
            receipt.update(ok=True, status="ready", reason=None)
        except (OSError, ValueError, _Refused) as exc:
            retained = _rollback(hooks, created, directories)
            receipt.update(
                ok=False,
                status="refused",
                reason=str(exc) if isinstance(exc, _Refused) else f"filesystem_refused: {exc}",
            )
            if retained:
                receipt["reason"] += "; rollback_retained: concurrent or unreadable changes at " + ", ".join(retained)
            # Concurrent replacement can make the planned actions obsolete.
            # A refusal reports no successful file actions; source digests and
            # the typed reason still describe the attempted operation.
            receipt["files"] = []
    return receipt
