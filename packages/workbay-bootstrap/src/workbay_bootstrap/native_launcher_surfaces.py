"""Deliver the finite native launcher pair into a registered copied hooks surface.

Installed RECORD-bound bytes are authority. The third local dependency,
_envfile.py, must already match that provider and is never managed here.
File readiness and native consumer qualification are separate receipts.
"""

from __future__ import annotations

import ast
import csv
import fcntl
import importlib.metadata as metadata
import io
import json
import os
import stat
import uuid
from contextlib import ExitStack
from pathlib import Path

from .configured_hooks import _executable_ast
from .lifecycle_surfaces import (
    _consumer_identity,
    _git_root,
    _json,
    _record_entry,
    _Refused,
    _relative,
    _sha,
    _signature,
    _Tree,
    _validated_metadata,
)

_SURFACE = "scripts/hooks"
_MANIFEST = ".workbay-bootstrap.json"
_JOURNAL = ".workbay/native-launcher-refresh.json"
_BACKUPS = ".workbay/native-launcher-refresh-backups"
_LOCK = ".workbay/bootstrap-writer.lock"
_PREFIX = "workbay_system/payload/"
_MANAGED = (f"{_SURFACE}/_native_runtime.py", f"{_SURFACE}/mcp_launch.py")
_ENVFILE = f"{_SURFACE}/_envfile.py"
# Reviewed executable-ast@2 catalogue, excluding only docstring text as the
# wheel scrubber does. Arbitrary future imports/loaders cannot enlarge scope.
_AST = {
    _MANAGED[0]: "1a5d2da7f07cac594d5e1d6eec07621e1004c1a6ae372f5fa430f0223d26d8d8",
    _MANAGED[1]: "e81dc426ded2a1cbc00c197bb3b7436742e3a3057fc7edd6a1d2ed92055aca7c",
    _ENVFILE: "aa763ba0ebad2006ae01be0e370695ff1c4b1f2583ddf536c1058b0bcc44bb67",
}


def _load_source():
    from .install import _package_source_root

    root = _package_source_root(None)
    dist = metadata.distribution("workbay-system")
    directory = Path(dist._path)
    if root != Path(dist.locate_file("workbay_system/payload")) or directory.parent != root.parent.parent:
        raise _Refused("source_identity: resolver is not the installed RECORD owner")
    owners = [
        d
        for d in metadata.distributions(path=[str(directory.parent)])
        if (d.metadata.get("Name") or "").lower().replace("_", "-") == "workbay-system"
    ]
    if len(owners) != 1:
        raise _Refused("source_identity: ambiguous installed workbay-system ownership")
    with ExitStack() as stack:
        site = _Tree(stack, directory.parent, source=True)
        record = site.read(f"{directory.name}/RECORD")
        metabytes = site.read(f"{directory.name}/METADATA")
        direct = site.optional(f"{directory.name}/direct_url.json")
        if direct and _json(direct[0]).get("dir_info", {}).get("editable"):
            raise _Refused("source_identity: editable provider")
        rows = {}
        for row in csv.reader(io.StringIO(record[0].decode("utf-8"))):
            if len(row) != 3 or row[0] in rows:
                raise _Refused("source_record: malformed or duplicate RECORD path")
            rows[row[0]] = row[1:]
        if rows.get(f"{directory.name}/METADATA") != _record_entry(metabytes[0]):
            raise _Refused("source_metadata: METADATA differs from RECORD digest/size")
        version = _validated_metadata(metabytes[0])
        blobs, identities = {}, {}
        for rel, fingerprint in _AST.items():
            data, identity = site.read(_PREFIX + rel)
            if rows.get(_PREFIX + rel) != _record_entry(data):
                raise _Refused(f"source_record: digest/size binding missing or different: {rel}")
            if _executable_ast(ast.parse(data, filename=rel)) != fingerprint:
                raise _Refused(f"source_catalog: unqualified native closure: {rel}")
            blobs[rel], identities[rel] = data, list(identity)
        site.bound()
        return blobs, {
            "origin": "installed workbay-system RECORD",
            "root": str(root),
            "version": version,
            "record_sha256": _sha(record[0]),
            "record_identity": list(record[1]),
            "metadata_sha256": _sha(metabytes[0]),
            "metadata_identity": list(metabytes[1]),
            "direct_url": None if direct is None else [_sha(direct[0]), list(direct[1])],
            "files": {rel: _sha(data) for rel, data in blobs.items()},
            "file_identities": identities,
            "managed": list(_MANAGED),
            "readonly_preconditions": [_ENVFILE],
            "native_external": ["workbay-protocol"],
        }


def _manifest(tree):
    observed = tree.read(_MANIFEST)
    value = _json(observed[0])
    if not isinstance(value, dict) or value.get("source_kind") != "package":
        raise _Refused("ownership: registered package-source copy required")
    entries = value.get("surfaces")
    if not isinstance(entries, list):
        raise _Refused("ownership: surface registration absent")
    seen, owners = set(), []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise _Refused("ownership: malformed registration")
        path = _relative(entry["path"])
        if path in seen:
            raise _Refused("ownership: duplicate registration")
        seen.add(path)
        if path == _SURFACE or path.startswith(_SURFACE + "/") or _SURFACE.startswith(path + "/"):
            owners.append(entry)
    if len(owners) != 1 or owners[0].get("path") != _SURFACE or owners[0].get("source") != "shared":
        raise _Refused("ownership: one exact shared hooks registration without overlapping owners required")
    return observed


def _mode(state):
    return stat.S_IMODE(state[1][-1]) if state else None


def _audit(tree, journal, source, manifest):
    if (
        not isinstance(journal, dict)
        or journal.get("schema") != "workbay/native-launcher-refresh@1"
        or journal.get("status") not in {"pending", "complete"}
    ):
        raise _Refused("journal_invalid: unknown native launcher receipt")
    transaction = journal.get("transaction")
    if (
        not isinstance(transaction, str)
        or len(transaction) != 32
        or any(c not in "0123456789abcdef" for c in transaction)
    ):
        raise _Refused("journal_invalid: transaction")
    changes = journal.get("changes")
    if not isinstance(changes, list) or not 1 <= len(changes) <= 2:
        raise _Refused("journal_invalid: finite change proof required")
    seen = set()
    for item in changes:
        rel = item["path"]
        if rel not in _MANAGED or rel in seen:
            raise _Refused("journal_invalid: change ownership")
        seen.add(rel)
        old, backup, mode = item["old_sha256"], item["backup"], item["mode"]
        if type(mode) is not int or mode < 0 or mode > 0o7777 or mode & 0o022:
            raise _Refused("journal_invalid: file mode")
        if old is None:
            if rel != _MANAGED[0] or backup is not None or mode != 0o644:
                raise _Refused("journal_invalid: missing helper proof")
        else:
            if backup != f"{_BACKUPS}/{transaction}/{rel}":
                raise _Refused("journal_invalid: backup ownership")
            state = tree.read(backup)
            if _sha(state[0]) != old or _mode(state) != mode:
                raise _Refused(f"backup_changed: {backup}")
        if journal["status"] == "pending":
            current = tree.optional(rel)
            digest = _sha(current[0]) if current else None
            if (
                digest not in {old, item["new_sha256"]}
                or (current and _mode(current) != mode)
                or source["files"].get(rel) != item["new_sha256"]
            ):
                raise _Refused(f"old_cas_changed: interrupted refresh: {rel}")
    if journal["status"] == "pending" and (
        journal.get("source") != source or journal.get("manifest_sha256") != _sha(manifest[0])
    ):
        raise _Refused("pending_identity_changed: retain provider/consumer custody hold")


def _before_write(rel):
    """Fault-injection seam; no public callbacks or source overrides."""


def _create_helper(tree, rel, data, mode):
    # Publish a fully fsynced new file without replacing a concurrent creator.
    parent, _, name = rel.rpartition("/")
    fd = tree.directory(parent)
    temporary = f".native-{uuid.uuid4().hex}"
    tree.write_new(f"{parent}/{temporary}", data, mode=mode)
    try:
        tree.bound()
        try:
            os.link(temporary, name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
        except FileExistsError as exc:
            raise _Refused(f"old_cas_changed: {rel}") from exc
        os.fsync(fd)
    finally:
        os.unlink(temporary, dir_fd=fd)
        os.fsync(fd)
    tree.bound()


def refresh_native_launchers(*, target: Path, check: bool = False) -> dict[str, object]:
    """Read-only plan or explicit two-file apply; 0 ready, 1 stale, 2 hold."""
    receipt = {
        "ok": False,
        "status": "hold",
        "target": str(target),
        "source": {},
        "files": [],
        "reason": None,
        "native_qualification": "separate",
    }
    try:
        target = Path(target)
        with ExitStack() as stack:
            tree = _Tree(stack, target)
            _git_root(target)
            git_identity, manifest = _consumer_identity(tree), _manifest(tree)
            blobs, source = _load_source()
            receipt["source"] = source
            envfile = tree.read(_ENVFILE)
            if envfile[0] != blobs[_ENVFILE]:
                raise _Refused("readonly_dependency: existing _envfile.py must match qualified provider")
            initial = {rel: tree.optional(rel) for rel in _MANAGED}
            if initial[_MANAGED[1]] is None:
                raise _Refused("ownership: existing regular managed MCP shim required")
            lock_state, journal_state = tree.optional(_LOCK), tree.optional(_JOURNAL)
            journal = _json(journal_state[0]) if journal_state else None
            if journal is not None:
                _audit(tree, journal, source, manifest)
            pending = journal is not None and journal["status"] == "pending"
            files = [
                {
                    "path": rel,
                    "sha256": _sha(blobs[rel]),
                    "old_sha256": _sha(initial[rel][0]) if initial[rel] else None,
                    "action": "missing"
                    if initial[rel] is None
                    else "identical"
                    if initial[rel][0] == blobs[rel]
                    else "different",
                }
                for rel in _MANAGED
            ]
            receipt["files"] = files

            def validate():
                tree.bound()
                _git_root(target)
                if (
                    _consumer_identity(tree) != git_identity
                    or _manifest(tree) != manifest
                    or tree.optional(_JOURNAL) != journal_state
                    or tree.read(_ENVFILE) != envfile
                ):
                    raise _Refused("consumer_cas_changed: ledger, dependency or journal changed")
                if tree.optional(_LOCK) != lock_state:
                    raise _Refused("lock_changed: bootstrap writer lock replaced")
                if _load_source()[1] != source:
                    raise _Refused("source_cas_changed: installed provider changed")
                if any(tree.optional(rel) != state for rel, state in initial.items()):
                    raise _Refused("old_cas_changed: native launcher copy changed")
                if journal_state:
                    _audit(tree, _json(journal_state[0]), source, manifest)

            validate()
            different = [f for f in files if f["action"] != "identical"]
            if check or (not different and not pending):
                receipt.update(
                    ok=not different and not pending,
                    status="interrupted" if pending else "stale" if different else "ready",
                )
                return receipt
            workbay = tree.directory(".workbay", create=True)
            if lock_state is None:
                tree.write_new(_LOCK, b"")
            lock_state = tree.read(_LOCK)
            lock = os.open("bootstrap-writer.lock", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=workbay)
            stack.callback(os.close, lock)
            if _signature(os.fstat(lock)) != lock_state[1]:
                raise _Refused("lock_changed: bootstrap writer lock replaced")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise _Refused("writer_lock_busy: another bootstrap writer is active") from exc
            stack.callback(fcntl.flock, lock, fcntl.LOCK_UN)
            validate()
            if not pending:
                transaction, changes = uuid.uuid4().hex, []
                for entry in different:
                    rel, old = entry["path"], initial[entry["path"]]
                    backup = f"{_BACKUPS}/{transaction}/{rel}" if old else None
                    mode = _mode(old) if old else 0o644
                    if backup:
                        tree.write_new(backup, old[0], mode=mode)
                    changes.append(
                        {
                            "path": rel,
                            "backup": backup,
                            "mode": mode,
                            "old_sha256": entry["old_sha256"],
                            "new_sha256": entry["sha256"],
                        }
                    )
                validate()
                if journal_state:
                    prior = _json(journal_state[0])
                    archived = f"{_BACKUPS}/{prior['transaction']}/receipt.json"
                    if tree.optional(archived) is None:
                        tree.write_new(archived, journal_state[0])
                    elif tree.read(archived)[0] != journal_state[0]:
                        raise _Refused("backup_changed: archived receipt")
                journal = {
                    "schema": "workbay/native-launcher-refresh@1",
                    "transaction": transaction,
                    "status": "pending",
                    "source": source,
                    "manifest_sha256": _sha(manifest[0]),
                    "changes": changes,
                }
                tree.replace(_JOURNAL, (json.dumps(journal, sort_keys=True) + "\n").encode(), journal_state)
                journal_state = tree.read(_JOURNAL)
            for item in journal["changes"]:
                rel = item["path"]
                if initial[rel] is None or initial[rel][0] != blobs[rel]:
                    _before_write(rel)
                    validate()
                    if initial[rel] is None:
                        _create_helper(tree, rel, blobs[rel], item["mode"])
                    else:
                        tree.replace(rel, blobs[rel], initial[rel], mode=item["mode"])
                    initial[rel] = tree.read(rel)
                next(f for f in files if f["path"] == rel).update(action="refreshed", backup=item["backup"])
            validate()
            if any(tree.read(rel)[0] != blobs[rel] for rel in _MANAGED):
                raise _Refused("final_cas_changed: incomplete native closure")
            journal["status"] = "complete"
            tree.replace(_JOURNAL, (json.dumps(journal, sort_keys=True) + "\n").encode(), journal_state)
            journal_state = tree.read(_JOURNAL)
            validate()
            receipt.update(ok=True, status="refreshed")
    except (
        OSError,
        ValueError,
        SyntaxError,
        KeyError,
        TypeError,
        AttributeError,
        metadata.PackageNotFoundError,
        _Refused,
    ) as exc:
        receipt.update(ok=False, status="hold", reason=str(exc))
    return receipt
