"""Refresh the registered lifecycle copy from the installed wheel only.

The bootstrap manifest grants surface ownership, never per-file freshness.
The wheel RECORD supplies new bytes; a durable pending journal and private
backups retain old bytes before any replacement. Replay checks both sides.
All consumer I/O uses held, no-follow directory descriptors. The writer lock
uses the existing bootstrap writer's lock file and flock protocol.
"""

from __future__ import annotations

import base64
import csv
import email.parser
import fcntl
import hashlib
import importlib.metadata as metadata
from importlib.util import source_from_cache
import io
import json
import os
import stat
import uuid
from contextlib import ExitStack
from pathlib import Path, PurePosixPath

from .hook_helpers import (
    _DIR_FLAGS,
    _absolute_directory,
    _git_root,
    _identity,
    _read_file,
    _Refused,
)

_SURFACE = "scripts/workbay_lifecycle"
_MANIFEST = ".workbay-bootstrap.json"
_JOURNAL = ".workbay/lifecycle-refresh.json"
_PREFIX = "workbay_system/payload/"
_MAX_SOURCE_ENTRIES = 4096


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _record_entry(data: bytes) -> list[str]:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return ["sha256=" + digest, str(len(data))]


def _signature(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode)


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise _Refused(f"duplicate_key: {key}")
        result[key] = value
    return result


def _json(data: bytes):
    return json.loads(data, object_pairs_hook=_pairs)


def _relative(raw: str) -> str:
    path = PurePosixPath(raw)
    if (
        not raw
        or path.is_absolute()
        or any(x in {".", ".."} for x in raw.split("/"))
        or path.as_posix() != raw
        or "\\" in raw
    ):
        raise _Refused(f"unsafe_path: {raw}")
    return raw


def _safe(info: os.stat_result, *, directory=False, source=False) -> None:
    if info.st_uid not in ({0, os.geteuid()} if source else {os.geteuid()}):
        raise _Refused("unsafe_owner: path is not owned by the expected principal")
    if info.st_mode & 0o022:
        raise _Refused("unsafe_permissions: group/world writable path")
    if not directory and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
        raise _Refused("unsafe_file: single-link regular file required")


class _Tree:
    def __init__(self, stack: ExitStack, root: Path, *, source=False):
        self.stack, self.root, self.source = stack, root, source
        self.dirs: dict[str, int] = {}
        fd = _absolute_directory(root)
        stack.callback(os.close, fd)
        _safe(os.fstat(fd), directory=True, source=source)
        self.dirs[""] = fd

    def directory(self, rel: str = "", *, create=False) -> int:
        if rel in self.dirs:
            return self.dirs[rel]
        _relative(rel)
        parent, _, name = rel.rpartition("/")
        fd = self.directory(parent, create=create)
        try:
            child = os.open(name, _DIR_FLAGS, dir_fd=fd)
        except FileNotFoundError:
            if not create:
                raise
            os.mkdir(name, 0o700, dir_fd=fd)
            os.fsync(fd)
            child = os.open(name, _DIR_FLAGS, dir_fd=fd)
        self.stack.callback(os.close, child)
        _safe(os.fstat(child), directory=True, source=self.source)
        self.dirs[rel] = child
        return child

    def read(self, rel: str):
        _relative(rel)
        parent, _, name = rel.rpartition("/")
        data, info = _read_file(self.directory(parent), name, limit=16 * 1024 * 1024)
        _safe(info, source=self.source)
        return data, _signature(info)

    def optional(self, rel: str):
        try:
            return self.read(rel)
        except FileNotFoundError:
            return None

    def bound(self):
        with ExitStack() as stack:
            now = _Tree(stack, self.root, source=self.source)
            for rel, fd in list(self.dirs.items()):
                current = now.directory(rel)
                if _identity(os.fstat(current)) != _identity(os.fstat(fd)):
                    raise _Refused(f"directory_changed: {rel or self.root}")

    def write_new(self, rel: str, data: bytes, *, mode=0o600):
        parent, _, name = _relative(rel).rpartition("/")
        fd = self.directory(parent, create=True)
        self.bound()
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=fd)
        try:
            # Only this call's newly created inode; never chmod an existing file.
            os.fchmod(descriptor, mode)
            with os.fdopen(os.dup(descriptor), "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
        os.fsync(fd)

    def replace(self, rel: str, data: bytes, expected, *, mode=0o600):
        parent, _, name = _relative(rel).rpartition("/")
        fd = self.directory(parent)
        temporary = f".lifecycle-{uuid.uuid4().hex}"
        temp_rel = f"{parent}/{temporary}" if parent else temporary
        self.write_new(temp_rel, data, mode=mode)
        try:
            self.bound()
            if self.optional(rel) != expected:
                raise _Refused(f"old_cas_changed: {rel}")
            os.replace(temporary, name, src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
            self.bound()
        finally:
            try:
                os.unlink(temporary, dir_fd=fd)
            except FileNotFoundError:
                pass


def _installed_tree(site: _Tree, rel: str, recorded: set[str]) -> set[str]:
    """Packaged inventory, excluding only safe caches of RECORD-bound source.

    Cache contents and interpreter format versions are derived state, never
    source authority. Walk and mediate their paths without reading their bytes.
    """
    found, pending, entries = set(), [rel], 0
    while pending:
        current = pending.pop()
        fd = site.directory(current)
        for name in sorted(os.listdir(fd)):
            entries += 1
            if entries > _MAX_SOURCE_ENTRIES:
                raise _Refused("source_record: installed lifecycle tree exceeds capacity bound")
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                pending.append(f"{current}/{name}")
            elif stat.S_ISREG(info.st_mode):
                path = f"{current}/{name}"
                if path not in recorded and name.endswith(".pyc") and PurePosixPath(current).name == "__pycache__":
                    try:
                        source = source_from_cache(path)
                    except ValueError:
                        source = None
                    if source in recorded:
                        _safe(info, source=True)
                        continue
                found.add(path)
            else:
                raise _Refused(f"source_record: unsafe installed lifecycle entry: {current}/{name}")
    return found


def _validated_metadata(metabytes: bytes) -> str:
    headers = email.parser.HeaderParser().parsestr(metabytes.decode("utf-8"))
    names, versions = headers.get_all("Name") or [], headers.get_all("Version") or []
    if (
        len(names) != 1
        or names[0].strip().lower().replace("_", "-") != "workbay-system"
        or len(versions) != 1
        or not versions[0].strip()
    ):
        raise _Refused("source_metadata: installed METADATA lacks one workbay-system Name and Version")
    return versions[0].strip()


def _load_source():
    """Bind resolver, installed RECORD, METADATA and every payload blob.

    RECORD is the authority only when it is internally consistent: its own
    METADATA row matches the bytes read, and its lifecycle rows cover exactly
    the installed source inventory. Safe interpreter caches derived from those
    bound sources are inert and excluded. Nothing outside the bound bytes is trusted.
    """
    from .install import _package_source_root

    root = _package_source_root(None)
    dist = metadata.distribution("workbay-system")
    dist_path = Path(dist._path)
    if root != Path(dist.locate_file("workbay_system/payload")) or dist_path.parent != root.parent.parent:
        raise _Refused("source_identity: resolver is not the installed RECORD owner")
    # Multiple installations elsewhere do not own this payload. Multiple
    # metadata owners in this site directory are ambiguous and must be held.
    owners = [
        d
        for d in metadata.distributions(path=[str(dist_path.parent)])
        if (d.metadata.get("Name") or "").lower().replace("_", "-") == "workbay-system"
    ]
    if len(owners) != 1:
        raise _Refused("source_identity: ambiguous installed workbay-system ownership")
    with ExitStack() as stack:
        site = _Tree(stack, dist_path.parent, source=True)
        record, record_id = site.read(f"{dist_path.name}/RECORD")
        metabytes, meta_id = site.read(f"{dist_path.name}/METADATA")
        direct = site.optional(f"{dist_path.name}/direct_url.json")
        if direct and _json(direct[0]).get("dir_info", {}).get("editable"):
            raise _Refused("source_identity: editable provider has no installed payload RECORD")
        sources, identities = {}, {}
        rows = {}
        for row in csv.reader(io.StringIO(record.decode("utf-8"))):
            if len(row) != 3 or row[0] in rows:
                raise _Refused("source_record: malformed or duplicate RECORD path")
            rows[row[0]] = row[1:]
        if rows.get(f"{dist_path.name}/METADATA") != _record_entry(metabytes):
            raise _Refused("source_metadata: installed METADATA differs from its RECORD digest/size")
        version = _validated_metadata(metabytes)
        recorded = {_relative(name) for name in rows if name.startswith(_PREFIX + _SURFACE + "/")}
        if f"{_PREFIX}{_SURFACE}/__init__.py" not in recorded:
            raise _Refused("source_record: lifecycle payload missing from installed RECORD")
        # Partial RECORD coverage would silently drop a managed module from
        # every comparison; require exact coverage of the installed source,
        # while leaving safe interpreter-derived artifacts outside the copy set.
        uncovered = recorded ^ _installed_tree(site, _PREFIX + _SURFACE, recorded)
        if uncovered:
            raise _Refused(
                "source_record: installed lifecycle tree and RECORD coverage differ: "
                + ", ".join(sorted(uncovered)[:5])
            )
        for name in sorted(recorded):
            rel = name.removeprefix(_PREFIX)
            data, identity = site.read(name)
            if rows[name] != _record_entry(data):
                raise _Refused(f"source_record: payload digest differs: {rel}")
            sources[rel], identities[rel] = data, identity
        site.bound()
        source = {
            "origin": "installed workbay-system RECORD",
            "root": str(root),
            "version": version,
            "record_sha256": _sha(record),
            "metadata_sha256": _sha(metabytes),
            "record_identity": list(record_id),
            "metadata_identity": list(meta_id),
            "files": {rel: _sha(data) for rel, data in sorted(sources.items())},
            "file_identities": {rel: list(value) for rel, value in sorted(identities.items())},
        }
        return dict(sorted(sources.items())), source


def _manifest(tree: _Tree):
    observed = tree.read(_MANIFEST)
    manifest = _json(observed[0])
    if not isinstance(manifest, dict) or manifest.get("source_kind") != "package":
        raise _Refused("ownership: only registered package-source managed copies may refresh")
    surfaces = manifest.get("surfaces")
    if not isinstance(surfaces, list):
        raise _Refused("ownership: surface ledger is absent")
    owned, seen = [], set()
    for entry in surfaces:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise _Refused("ownership: malformed surface registration")
        path = _relative(entry["path"])
        if path in seen:
            raise _Refused(f"ownership: duplicate surface registration: {path}")
        seen.add(path)
        if path == _SURFACE or path.startswith(_SURFACE + "/") or _SURFACE.startswith(path + "/"):
            owned.append(entry)
    if owned != [{"path": _SURFACE, "source": "lifecycle"}]:
        # Fields unrelated to ownership are allowed on the one exact record.
        if len(owned) != 1 or owned[0].get("path") != _SURFACE or owned[0].get("source") != "lifecycle":
            raise _Refused("ownership: lifecycle must have one exact managed registration without overlapping owners")
    return observed


def _consumer_identity(tree: _Tree):
    """Pin Git registration as well as the worktree and ledger identities."""
    try:
        descriptor = tree.directory(".git")
    except NotADirectoryError:
        return tree.read(".git")  # linked worktree registration, no-follow
    return _identity(os.fstat(descriptor)), tree.read(".git/HEAD")


def _before_replace(path: str) -> None:
    """Fault-injection seam; production has no callback or override argument."""


def _audit(tree: _Tree, journal: dict, source: dict, manifest):
    if (
        not isinstance(journal, dict)
        or journal.get("schema") != "workbay/lifecycle-refresh@1"
        or journal.get("status") not in {"pending", "complete"}
    ):
        raise _Refused("journal_invalid: unknown refresh receipt")
    transaction = journal.get("transaction")
    if (
        not isinstance(transaction, str)
        or len(transaction) != 32
        or any(c not in "0123456789abcdef" for c in transaction)
    ):
        raise _Refused("journal_invalid: invalid transaction identity")
    changes = journal.get("changes")
    if not isinstance(changes, list) or not changes:
        raise _Refused("journal_invalid: missing change proof")
    seen = set()
    for item in changes:
        if not isinstance(item, dict):
            raise _Refused("journal_invalid: invalid change")
        rel = _relative(item["path"])
        backup = _relative(item["backup"])
        if (
            rel in seen
            or not rel.startswith(_SURFACE + "/")
            or backup != f".workbay/lifecycle-refresh-backups/{journal['transaction']}/{rel}"
        ):
            raise _Refused("journal_invalid: invalid backup ownership")
        seen.add(rel)
        if _sha(tree.read(backup)[0]) != item["old_sha256"]:
            raise _Refused(f"backup_changed: {backup}")
    if journal["status"] == "pending":
        if journal.get("source") != source or journal.get("manifest_sha256") != _sha(manifest[0]):
            raise _Refused("pending_identity_changed: provider or consumer ledger changed; retain custody hold")
        for item in changes:
            current = _sha(tree.read(item["path"])[0])
            if (
                current not in {item["old_sha256"], item["new_sha256"]}
                or source["files"].get(item["path"]) != item["new_sha256"]
            ):
                raise _Refused(f"old_cas_changed: interrupted refresh: {item['path']}")


def refresh_lifecycle(*, target: Path, check: bool = False) -> dict[str, object]:
    """Check or refresh only the ledger-owned lifecycle surface. No source override.

    Status: ready (0), stale/interrupted (1), refused (2). A failed apply leaves
    durable pending proof for explicit replay, never a false fresh receipt.
    """
    receipt = {"ok": False, "status": "refused", "target": str(target), "source": {}, "files": [], "reason": None}
    try:
        target = Path(target)
        with ExitStack() as stack:
            tree = _Tree(stack, target)
            _git_root(target)
            git_identity = _consumer_identity(tree)
            manifest = _manifest(tree)
            sources, source = _load_source()
            receipt["source"] = source
            initial = {rel: tree.read(rel) for rel in sources}
            # Validate optional writer state even in check mode, without creating it.
            try:
                tree.directory(".workbay")
            except FileNotFoundError:
                pass
            lock_state = tree.optional(".workbay/bootstrap-writer.lock")
            journal_state = tree.optional(_JOURNAL)
            journal = _json(journal_state[0]) if journal_state else None
            if journal is not None:
                _audit(tree, journal, source, manifest)
            files = [
                {
                    "path": rel,
                    "sha256": _sha(data),
                    "old_sha256": _sha(initial[rel][0]),
                    "action": "identical" if initial[rel][0] == data else "different",
                }
                for rel, data in sources.items()
            ]
            receipt["files"] = files
            pending = journal is not None and journal["status"] == "pending"
            different = [f for f in files if f["action"] == "different"]
            tree.bound()
            if (
                _consumer_identity(tree) != git_identity
                or _manifest(tree) != manifest
                or _load_source()[1] != source
                or any(tree.read(path) != state for path, state in initial.items())
                or tree.optional(_JOURNAL) != journal_state
            ):
                raise _Refused("check_cas_changed: provider or consumer changed during inspection")
            if check or (not different and not pending):
                receipt.update(
                    ok=not different and not pending,
                    status="interrupted" if pending else "stale" if different else "ready",
                )
                return receipt
            workbay = tree.directory(".workbay", create=True)
            # The same inode/flock protocol as embedding_provision's bootstrap
            # writer lock, with no-follow safety and prompt competition refusal.
            if lock_state is None:
                tree.write_new(".workbay/bootstrap-writer.lock", b"")
            lock_state = tree.read(".workbay/bootstrap-writer.lock")
            lock = os.open("bootstrap-writer.lock", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=workbay)
            stack.callback(os.close, lock)
            if _signature(os.fstat(lock)) != lock_state[1]:
                raise _Refused("lock_changed: bootstrap writer lock replaced")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise _Refused("writer_lock_busy: another bootstrap writer is active") from exc
            stack.callback(fcntl.flock, lock, fcntl.LOCK_UN)

            def validate():
                tree.bound()
                _git_root(target)
                if (
                    _consumer_identity(tree) != git_identity
                    or _manifest(tree) != manifest
                    or tree.optional(_JOURNAL) != journal_state
                ):
                    raise _Refused("consumer_changed: ownership ledger or refresh journal changed")
                if tree.read(".workbay/bootstrap-writer.lock") != lock_state:
                    raise _Refused("lock_changed: bootstrap writer lock replaced")
                if _load_source()[1] != source:
                    raise _Refused("source_changed: installed provider changed")
                if journal_state:
                    _audit(tree, _json(journal_state[0]), source, manifest)

            validate()
            if not pending:
                transaction = uuid.uuid4().hex
                changes = []
                for item in different:
                    rel = item["path"]
                    backup = f".workbay/lifecycle-refresh-backups/{transaction}/{rel}"
                    tree.write_new(backup, initial[rel][0])
                    changes.append(
                        {"path": rel, "old_sha256": item["old_sha256"], "new_sha256": item["sha256"], "backup": backup}
                    )
                journal = {
                    "schema": "workbay/lifecycle-refresh@1",
                    "transaction": transaction,
                    "status": "pending",
                    "source": source,
                    "manifest_sha256": _sha(manifest[0]),
                    "changes": changes,
                }
                validate()
                # Preserve each prior completed receipt with its backups.
                if journal_state:
                    prior = _json(journal_state[0])
                    archived = f".workbay/lifecycle-refresh-backups/{prior['transaction']}/receipt.json"
                    if tree.optional(archived) is None:
                        tree.write_new(archived, journal_state[0])
                    elif tree.read(archived)[0] != journal_state[0]:
                        raise _Refused("backup_changed: archived receipt")
                tree.replace(_JOURNAL, (json.dumps(journal, sort_keys=True) + "\n").encode(), journal_state)
                journal_state = tree.read(_JOURNAL)
            _audit(tree, journal, source, manifest)
            for item in journal["changes"]:
                rel = item["path"]
                if initial[rel][0] == sources[rel]:
                    continue
                _before_replace(rel)
                validate()
                # All not-yet-written files must still match the preflight,
                # including equal files; never overwrite a concurrent edit.
                if any(tree.read(path) != state for path, state in initial.items()):
                    raise _Refused("old_cas_changed: lifecycle copy changed during refresh")
                tree.replace(rel, sources[rel], initial[rel], mode=stat.S_IMODE(initial[rel][1][-1]))
                initial[rel] = tree.read(rel)
                for entry in files:
                    if entry["path"] == rel:
                        entry.update(action="refreshed", backup=item["backup"])
            validate()
            if any(tree.read(rel)[0] != data for rel, data in sources.items()):
                raise _Refused("final_cas_changed: lifecycle content changed")
            _audit(tree, journal, source, manifest)
            journal["status"] = "complete"
            tree.replace(_JOURNAL, (json.dumps(journal, sort_keys=True) + "\n").encode(), journal_state)
            journal_state = tree.read(_JOURNAL)
            validate()
            if any(tree.read(rel) != state for rel, state in initial.items()):
                raise _Refused("final_cas_changed: lifecycle copy changed before completion")
            receipt.update(ok=True, status="ready")
    except (OSError, ValueError, KeyError, TypeError, AttributeError, metadata.PackageNotFoundError, _Refused) as exc:
        receipt.update(ok=False, status="refused", reason=str(exc))
    return receipt


def doctor_lifecycle(target: Path, manifest: dict) -> list[dict[str, str]]:
    """Current-provider content finding for the registered package copy only."""
    entries = manifest.get("surfaces")
    if (
        manifest.get("source_kind") != "package"
        or not isinstance(entries, list)
        or not any(
            isinstance(e, dict) and e.get("path") == _SURFACE and e.get("source") == "lifecycle" for e in entries
        )
    ):
        return []
    receipt = refresh_lifecycle(target=target, check=True)
    if receipt["ok"]:
        return []
    if receipt["status"] == "stale":
        return [
            {
                "kind": "lifecycle_content_drift",
                "path": f["path"],
                "severity": "error",
                "message": f"managed copy differs from installed workbay-system RECORD ({f['sha256']}); check `workbay-bootstrap refresh-lifecycle --target <target> --check --json`, then apply explicitly",
            }
            for f in receipt["files"]
            if f["action"] == "different"
        ]
    return [
        {
            "kind": "lifecycle_refresh_hold",
            "path": _SURFACE,
            "severity": "error",
            "message": str(receipt["reason"] or "interrupted refresh requires explicit replay and custody audit"),
        }
    ]
