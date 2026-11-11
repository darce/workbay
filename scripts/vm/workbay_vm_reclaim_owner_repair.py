#!/usr/bin/env python3
"""Root-only, bounded owner repair of gate-owned reclaim tombstones.

The CLI has no path arguments. Requests supply only a tombstone basename; all
filesystem access is anchored by no-follow directory fds. A complete preflight
pins the bounded tree before any ownership or directory-mode change.
"""
from __future__ import annotations

import grp
import json
import os
import pwd
import re
import stat
import time
from pathlib import Path

NAME = re.compile(r'\.reclaiming-workbay-[A-Za-z0-9][A-Za-z0-9_.-]*\Z')
QUEUE = '.local/state/workbay-reclaim/owner-repair'
MAX_ENTRIES = 1024
MAX_REQUESTS = 16
TIMEOUT = 30
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


class Refusal(Exception):
    pass


def lstat(name, *, dir_fd):
    return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)


def identity(info):
    return info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)


def emit(receipt, path, action, info=None):
    receipt.write(json.dumps(dict(path=path, old_uid=info.st_uid if info else None,
                                 old_gid=info.st_gid if info else None,
                                 old_mode=info.st_mode if info else None, action=action), sort_keys=True) + '\n')
    receipt.flush()
    os.fsync(receipt.fileno())


def repair(home_fd, name, uid, gid, receipt, *, max_entries=MAX_ENTRIES,
           deadline=None, lstat=lstat, chown=os.fchown):
    if not NAME.fullmatch(name):
        raise Refusal('invalid tombstone name')
    deadline = time.monotonic() + TIMEOUT if deadline is None else deadline
    entries = []

    def check_bound():
        if time.monotonic() >= deadline:
            raise Refusal('deadline exceeded')
        if len(entries) >= max_entries:
            raise Refusal('entry limit exceeded')

    def pin(parent_fd, child, relative, root=False):
        check_bound()
        info = lstat(child, dir_fd=parent_fd)
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise Refusal('symlink or special entry')
        if info.st_dev != os.fstat(home_fd).st_dev:
            raise Refusal('cross-device entry')
        if root and (not stat.S_ISDIR(info.st_mode) or info.st_uid != uid):
            raise Refusal('tombstone root must be a directory owned by gate')
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise Refusal('hard-linked file')
        flags = DIR_FLAGS if stat.S_ISDIR(info.st_mode) else os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        fd = os.open(child, flags, dir_fd=parent_fd)
        entries.append((fd, parent_fd, child, relative, info))
        if identity(os.fstat(fd)) != identity(info):
            raise Refusal('entry changed while opening')
        return fd

    try:
        root_fd = pin(home_fd, name, name, root=True)
        pending = [(root_fd, name)]
        while pending:
            parent, relative = pending.pop()
            with os.scandir(parent) as children:
                for child in children:
                    fd = pin(parent, child.name, f'{relative}/{child.name}')
                    if stat.S_ISDIR(entries[-1][4].st_mode):
                        pending.append((fd, f'{relative}/{child.name}'))
        # Check the full pinned snapshot again before the first mutation.
        for fd, parent, child, relative, info in entries:
            if time.monotonic() >= deadline:
                raise Refusal('deadline exceeded')
            current = lstat(child, dir_fd=parent)
            if (identity(current), current.st_uid, current.st_gid, current.st_mode, current.st_nlink) != (
                    identity(info), info.st_uid, info.st_gid, info.st_mode, info.st_nlink):
                raise Refusal('entry changed after preflight')
        for fd, parent, child, relative, info in entries:
            if time.monotonic() >= deadline:
                raise Refusal('deadline exceeded')
            foreign = info.st_uid != uid
            directory = stat.S_ISDIR(info.st_mode)
            mode = stat.S_IMODE(info.st_mode) | stat.S_IRWXU
            chmod = directory and (stat.S_IMODE(info.st_mode) != mode or foreign)
            if foreign:
                chown(fd, uid, gid)
            if chmod:
                os.fchmod(fd, mode)
            if foreign or chmod:
                emit(receipt, relative, 'chown_chmod' if foreign and chmod else 'chown' if foreign else 'chmod', info)
        return len(entries)
    finally:
        for fd, *_ in reversed(entries):
            os.close(fd)


def open_directory(path):
    """Reject symlinks at every component, including gate's mutable ancestors."""
    fd = os.open('/', DIR_FLAGS)
    try:
        for part in Path(path).parts[1:]:
            child = os.open(part, DIR_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def consume(home, uid, gid, receipt, *, lstat=lstat):
    deadline = time.monotonic() + TIMEOUT
    home_fd = open_directory(home)
    queue_fd = None
    try:
        # Open relative to the pinned home, refusing every queue symlink.
        queue_fd = os.dup(home_fd)
        for part in QUEUE.split('/'):
            child = os.open(part, DIR_FLAGS, dir_fd=queue_fd)
            os.close(queue_fd)
            queue_fd = child
        count = 0
        with os.scandir(queue_fd) as requests:
            for entry in requests:
                if not entry.name.endswith('.json'):
                    continue
                if count >= MAX_REQUESTS or time.monotonic() >= deadline:
                    break
                count += 1
                name = entry.name[:-5]
                try:
                    if not NAME.fullmatch(name):
                        raise Refusal('invalid request name')
                    info = lstat(entry.name, dir_fd=queue_fd)
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_nlink != 1 or info.st_size > 4096:
                        raise Refusal('invalid request file')
                    fd = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=queue_fd)
                    with os.fdopen(fd, 'rb') as stream:
                        if identity(os.fstat(stream.fileno())) != identity(info):
                            raise Refusal('request changed')
                        raw = stream.read(4097)
                    value = json.loads(raw)
                    if (len(raw) > 4096 or not isinstance(value, dict) or type(value.get('schema')) is not int
                            or value != {'schema': 1, 'tombstone': name}):
                        raise Refusal('invalid request payload')
                    repair(home_fd, name, uid, gid, receipt, deadline=deadline, lstat=lstat)
                    emit(receipt, name, 'complete')
                except (OSError, ValueError, Refusal):
                    emit(receipt, name, 'refused')
                # Consume refusals too: PathExistsGlob must not spin on unsafe input.
                os.unlink(entry.name, dir_fd=queue_fd)
    finally:
        if queue_fd is not None:
            os.close(queue_fd)
        os.close(home_fd)


def main():
    if os.geteuid() != 0:
        raise Refusal('root required')
    gate = pwd.getpwnam('gate')
    group = grp.getgrnam('gate')
    fd = os.open('/var/log/workbay-vm-reclaim-owner-repair.jsonl',
                 os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, 'a') as receipt:
        info = os.fstat(receipt.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise Refusal('unsafe receipt file')
        consume(Path('/home/gate'), gate.pw_uid, group.gr_gid, receipt)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (Refusal, OSError, ValueError) as exc:
        print(json.dumps({'outcome': 'refused', 'error': str(exc)}))
        raise SystemExit(2)
