#!/usr/bin/env python3
"""Scheduled, user-owned macOS pressure observation and released-directory reclaim.

Standard library only, including under /usr/bin/python3 -I (Python 3.9).
Pressure is a signal; it never participates in admission.
"""
import argparse
import errno
import fcntl
import json
import os
import plistlib
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from xml.parsers.expat import ExpatError

GIB = 1024 ** 3
TOMBSTONE = '.workbay-host-reclaim-'
CLASSES = ('scratch', 'lane', 'gate', 'evidence')
PRIVACY_EXCLUDES = ('Desktop', 'Documents', 'Downloads', 'Library/Safari', 'Library/Calendars',
                    'Library/Reminders', 'Library/Application Support/AddressBook',
                    'Library/Application Support/CallHistoryDB', 'Library/Application Support/MobileSync',
                    'Library/Cookies', 'Library/HomeKit', 'Library/Suggestions')
EXCLUDES = ('Library/Mail', 'Library/Messages', 'Library/Mobile Documents',
            'Library/Containers', 'Library/Group Containers', 'Pictures/Photos Library.photoslibrary') + PRIVACY_EXCLUDES


class Refusal(Exception):
    def __init__(self, kind, message=''):
        super().__init__(message or kind)
        self.kind = kind


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace('+00:00', 'Z')


def epoch(value):
    if not isinstance(value, str):
        raise ValueError('timestamp must be a string')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamp must include a timezone')
    return parsed.timestamp()


def command(argv, timeout):
    return subprocess.run(argv, capture_output=True, timeout=timeout, check=False)


def atomic_json(path, value):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '-', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.lexists(temporary):
            os.unlink(temporary)


def regular_json(path, secure_uid=None):
    """Bounded read; neither marker nor registration final symlinks are followed."""
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if secure_uid is not None and (info.st_uid != secure_uid or info.st_mode & 0o022):
            raise Refusal('registration_insecure')
        if not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024 ** 2:
            raise ValueError('requires a regular JSON file of at most 2 MiB')
        value = json.loads(stream.read(2 * 1024 ** 2 + 1))
        if not isinstance(value, dict):
            raise ValueError('requires a JSON object')
        return value


def saved_json(path, default):
    try:
        return regular_json(path)
    except (OSError, ValueError):
        return default


def within(path, prefix):
    return path == prefix or prefix in path.parents


def real_directory(path):
    """Reject symlinks in every component, including ancestors of a root."""
    return path.is_absolute() and str(path) == os.path.realpath(str(path)) and stat.S_ISDIR(path.lstat().st_mode)


def marker(path, release=False):
    value = regular_json(path)
    if value.get('schema') != 1 or type(value.get('schema')) is not int:
        raise ValueError('marker schema must be 1')
    if not isinstance(value.get('owner'), str) or not value['owner'].strip():
        raise ValueError('owner must be nonempty')
    if release:
        if not isinstance(value.get('reason'), str) or not value['reason'].strip():
            raise ValueError('release reason must be nonempty')
        epoch(value.get('released_at'))
    else:
        if value.get('retention_class') not in CLASSES:
            raise ValueError('invalid retention_class')
        if 'no_reap' in value and type(value['no_reap']) is not bool:
            raise ValueError('no_reap must be boolean')
    return value


def applescript_string(value):
    return value.replace('\\', '\\\\').replace('"', '\\"').replace('\r', '\\r').replace('\n', '\\n')


class HostReclaimer:
    def __init__(self, home=None, diskutil=command, lsof=command, osascript=command,
                 clock=time.time, monotonic=time.monotonic, statvfs=os.statvfs,
                 scandir=os.scandir, lstat=os.lstat, rename=os.rename, delete=shutil.rmtree):
        self.home = Path(home) if home is not None else Path.home()
        self.home = self.home.absolute()
        self.state = self.home / '.local/state/workbay/host-reclaim'
        self.registration = self.home / '.config/workbay/host-reclaim.json'
        self.uid = os.getuid()
        self.diskutil, self.lsof, self.osascript = diskutil, lsof, osascript
        self.clock, self.monotonic, self.statvfs = clock, monotonic, statvfs
        self.scandir, self.lstat, self.rename, self.delete = scandir, lstat, rename, delete

    def defaults(self):
        return dict(schema=1, roots=[], inventory_roots=[str(self.home)],
                    inventory_exclude=[str(self.home / p) for p in EXCLUDES],
                    warn_free_bytes=15 * GIB, critical_free_bytes=5 * GIB,
                    per_run_byte_cap=50 * GIB, release_grace_seconds=600, notify=True)

    def config(self):
        defaults = self.defaults()
        try:
            data = regular_json(self.registration, secure_uid=self.uid)
        except FileNotFoundError:
            return defaults, 'registration_missing'
        except Refusal as exc:
            return defaults, exc.kind
        except (OSError, ValueError):
            return defaults, 'registration_invalid'
        try:
            if type(data.get('schema')) is not int or data['schema'] != 1:
                raise ValueError('schema must be 1')
            config = dict(defaults, **data)
            if 'roots' not in data:
                raise ValueError('roots required')
            for key in ('roots', 'inventory_roots', 'inventory_exclude'):
                if not isinstance(config[key], list) or not all(isinstance(p, str) and p and Path(p).is_absolute()
                                                               for p in config[key]):
                    raise ValueError('absolute path lists required')
            for item in config['roots']:
                root = Path(item)
                if within(self.home.resolve(), root.resolve()):
                    raise Refusal('root_too_broad')
                if not real_directory(root):
                    raise ValueError('root must be an existing non-symlink directory')
            for key in ('warn_free_bytes', 'critical_free_bytes', 'per_run_byte_cap', 'release_grace_seconds'):
                if type(config[key]) is not int or config[key] < 0:
                    raise ValueError('nonnegative integer required')
            if config['warn_free_bytes'] <= config['critical_free_bytes'] or config['per_run_byte_cap'] == 0:
                raise ValueError('invalid thresholds or cap')
            if type(config['notify']) is not bool:
                raise ValueError('notify must be boolean')
            return config, None
        except Refusal as exc:
            return defaults, exc.kind
        except (OSError, ValueError, TypeError):
            return defaults, 'registration_invalid'

    def pressure(self, config=None):
        config = config or self.config()[0]
        free, size, source, reference = None, None, None, None
        errors, volumes, advisories = [], [], []
        try:
            response = self.diskutil(['diskutil', 'info', '-plist', '/System/Volumes/Data'], timeout=10)
            if response.returncode != 0:
                raise ValueError('diskutil info failed')
            data = plistlib.loads(response.stdout)
            free, size = data['APFSContainerFree'], data['APFSContainerSize']
            reference = data['APFSContainerReference']
            if type(free) is not int or type(size) is not int or not 0 <= free <= size or not reference:
                raise ValueError('invalid container facts')
            source = 'diskutil'
        except (OSError, ValueError, TypeError, KeyError, plistlib.InvalidFileException, ExpatError, subprocess.SubprocessError) as exc:
            errors.append('diskutil: ' + str(exc))
            free, size = None, None
            try:
                info = self.statvfs('/System/Volumes/Data')
                free, size = info.f_bavail * info.f_frsize, info.f_blocks * info.f_frsize
                if not 0 <= free <= size:
                    raise ValueError('invalid statvfs facts')
                source = 'statvfs'
            except (OSError, ValueError) as fallback:
                errors.append('statvfs: ' + str(fallback))
                free, size, source = None, None, None
        if reference is not None:
            try:
                response = self.diskutil(['diskutil', 'apfs', 'list', '-plist'], timeout=10)
                if response.returncode != 0:
                    raise ValueError('diskutil apfs list failed')
                listing = plistlib.loads(response.stdout)
                for container in listing['Containers']:
                    if container.get('ContainerReference') != reference:
                        continue
                    for volume in container.get('Volumes', []):
                        roles = volume.get('Roles', [])
                        if isinstance(roles, str):
                            roles = [roles]
                        used = volume.get('CapacityInUse')
                        if type(used) is not int or used < 0:
                            raise ValueError('unreadable volume use')
                        name = volume.get('Name', '')
                        volumes.append(dict(role=roles, name=name, capacity_in_use_bytes=used))
                        if (('Preboot' in roles or name == 'Preboot') and used > 8 * GIB or
                                ('Update' in roles or name == 'Update') and used > GIB):
                            if 'staged_os_update' not in advisories:
                                advisories.append('staged_os_update')
            except (OSError, ValueError, TypeError, KeyError, plistlib.InvalidFileException, ExpatError, subprocess.SubprocessError) as exc:
                errors.append('volumes: ' + str(exc))
        level = ('unknown' if free is None else 'critical' if free < config['critical_free_bytes'] else
                 'warn' if free < config['warn_free_bytes'] else 'ok')
        return dict(schema='workbay/host-disk-pressure@1', observed_at=timestamp(self.clock()), level=level,
                    container_free_bytes=free, container_size_bytes=size, source=source, volumes=volumes,
                    advisories=advisories, last_inventory=None, last_reclaim=None, errors=errors)

    def inventory(self, config=None, deadline_seconds=120, entry_cap=2000000):
        config = config or self.config()[0]
        started, now = self.monotonic(), self.clock()
        excluded = [Path(os.path.abspath(p)) for p in config['inventory_exclude']]
        files, growth, totals, reasons = [], [], {}, []
        unreadable = dict(count=0, samples=[])
        entries = 0

        def unread(path):
            unreadable['count'] += 1
            if len(unreadable['samples']) < 20:
                unreadable['samples'].append(str(path))

        not_inventoried = []
        for name in PRIVACY_EXCLUDES:
            path = self.home / name
            if not any(within(path, p) for p in excluded):
                continue
            try:
                self.lstat(path)
            except FileNotFoundError:
                continue
            except OSError:
                unread(path)
                continue
            not_inventoried.append(dict(path=str(path), reason='privacy_protected'))

        def bounded():
            if self.monotonic() - started >= deadline_seconds:
                if 'deadline' not in reasons:
                    reasons.append('deadline')
                return True
            if entries >= entry_cap:
                if 'entry_cap' not in reasons:
                    reasons.append('entry_cap')
                return True
            return False

        for registered in config['inventory_roots']:
            root = Path(registered)
            if any(within(root, p) for p in excluded):
                continue
            try:
                root_info = self.lstat(root)
                if not stat.S_ISDIR(root_info.st_mode) or not real_directory(root):
                    unread(root)
                    continue
            except OSError:
                unread(root)
                continue
            stack = [root]
            while stack and not bounded():
                directory = stack.pop()
                try:
                    with self.scandir(directory) as iterator:
                        for entry in iterator:
                            if bounded():
                                break
                            entries += 1
                            path = Path(entry.path)
                            if any(within(path, p) for p in excluded):
                                continue
                            try:
                                info = entry.stat(follow_symlinks=False)
                            except OSError:
                                unread(path)
                                continue
                            if stat.S_ISLNK(info.st_mode) or info.st_dev != root_info.st_dev:
                                continue
                            parts = path.relative_to(root).parts
                            allocated = info.st_blocks * 512
                            for depth in (1, 2):
                                if len(parts) > depth or len(parts) == depth and stat.S_ISDIR(info.st_mode):
                                    key = (str(root), str(root.joinpath(*parts[:depth])))
                                    totals[key] = totals.get(key, 0) + allocated
                            if stat.S_ISDIR(info.st_mode):
                                stack.append(path)
                            elif stat.S_ISREG(info.st_mode) and info.st_size >= 256 * 1024 ** 2:
                                row = dict(path=str(path), size_bytes=info.st_size, allocated_bytes=allocated,
                                           modified_at=timestamp(info.st_mtime))
                                files.append(row)
                                files.sort(key=lambda r: (-r['size_bytes'], r['path']))
                                del files[25:]
                                if now - 72 * 3600 <= info.st_mtime <= now:
                                    growth.append(row)
                                    growth.sort(key=lambda r: (-r['size_bytes'], r['path']))
                                    del growth[25:]
                except OSError:
                    unread(directory)
            if reasons:
                break
        directories = []
        for root in config['inventory_roots']:
            rows = [dict(root=r, path=p, allocated_bytes=b) for (r, p), b in totals.items() if r == root]
            directories.extend(sorted(rows, key=lambda r: (-r['allocated_bytes'], r['path']))[:15])
        return dict(schema='workbay/host-consumer-inventory@1', observed_at=timestamp(now),
                    files=files, growth=growth, directories=directories, unreadable=unreadable,
                    partial=bool(reasons), reasons=reasons, entries=entries, advisories=[],
                    not_inventoried=not_inventoried)

    def target(self, directory, config):
        directory = Path(os.path.abspath(os.path.expanduser(str(directory))))
        if directory.parent not in [Path(p) for p in config['roots']] or directory.name.startswith(TOMBSTONE):
            raise Refusal('not_under_registered_root')
        if not real_directory(directory.parent):
            raise Refusal('not_under_registered_root')
        return directory

    def allocate(self, directory, owner, retention_class, task_ref=None, note=None, adopt=False):
        config, hold = self.config()
        if hold:
            raise Refusal(hold)
        directory = self.target(directory, config)
        if not isinstance(owner, str) or not owner.strip():
            raise Refusal('owner_missing')
        if retention_class not in CLASSES:
            raise Refusal('retention_invalid')
        parent_fd = os.open(str(directory.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_EX)
            if adopt:
                try:
                    info = self.lstat(directory)
                    if not stat.S_ISDIR(info.st_mode) or info.st_dev != self.lstat(directory.parent).st_dev:
                        raise Refusal('not_a_dir')
                except OSError as exc:
                    raise Refusal('not_a_dir') from exc
                if os.path.lexists(str(directory / 'RETENTION.json')):
                    raise Refusal('already_marked')
            else:
                try:
                    directory.mkdir(mode=0o755)
                except FileExistsError as exc:
                    raise Refusal('exists') from exc
            data = dict(schema=1, owner=owner, retention_class=retention_class, task_ref=task_ref,
                        note=note, created_at=timestamp(self.clock()), host=socket.gethostname(), adopted=adopt)
            atomic_json(directory / 'RETENTION.json', data)
        finally:
            os.close(parent_fd)
        return dict(outcome='adopted' if adopt else 'allocated', basename=directory.name, path=str(directory), **data)

    def release(self, directory, owner, reason, evidence_copied_to=None):
        config, hold = self.config()
        if hold:
            raise Refusal(hold)
        directory = self.target(directory, config)
        if not stat.S_ISDIR(self.lstat(directory).st_mode):
            raise Refusal('not_a_dir')
        try:
            retention = marker(directory / 'RETENTION.json')
        except FileNotFoundError as exc:
            raise Refusal('retention_missing') from exc
        except (OSError, ValueError) as exc:
            raise Refusal('retention_invalid') from exc
        if retention.get('no_reap') is True:
            raise Refusal('retention_pinned')
        if retention['owner'] != owner:
            raise Refusal('owner_mismatch')
        if not isinstance(reason, str) or not reason.strip():
            raise Refusal('reason_missing')
        data = dict(schema=1, owner=owner, reason=reason, released_at=timestamp(self.clock()),
                    evidence_copied_to=evidence_copied_to, host=socket.gethostname())
        atomic_json(directory / 'RELEASE.json', data)
        return dict(outcome='released', basename=directory.name, **data)

    def activity(self, path):
        try:
            response = self.lsof(['lsof', '-nP', '-F', 'pn', '+D', str(path)], timeout=60)
            output = response.stdout
            if isinstance(output, bytes):
                output = output.decode('utf-8', errors='replace')
            if any(line.startswith('p') and line[1:].isdigit() for line in output.splitlines()):
                return 'busy'
            if response.returncode == 1 and not output.strip() and not response.stderr:
                return None
            return 'activity_unknown'
        except (OSError, subprocess.SubprocessError):
            return 'activity_unknown'

    def verdict(self, path, root, config):
        try:
            info = self.lstat(path)
            if not stat.S_ISDIR(info.st_mode):
                return 'hold_not_directory'
            if info.st_dev != self.lstat(root).st_dev:
                return 'hold_other_device'
            try:
                retention = marker(path / 'RETENTION.json')
            except FileNotFoundError:
                return 'hold_unmarked'
            except (OSError, ValueError):
                return 'retention_invalid'
            if retention.get('no_reap') is True:
                return 'retention_pinned'
            try:
                release = marker(path / 'RELEASE.json', release=True)
            except FileNotFoundError:
                return 'hold_not_released'
            except (OSError, ValueError):
                return 'release_invalid'
            if release['owner'] != retention['owner']:
                return 'hold_owner_mismatch'
            if self.clock() - epoch(release['released_at']) < config['release_grace_seconds']:
                return 'hold_grace'
            return self.activity(path) or 'reclaimable'
        except OSError:
            return 'activity_unknown'

    def candidates(self, config):
        entries = []
        for item in config['roots']:
            root = Path(item)
            try:
                if not real_directory(root):
                    entries.append(dict(path=str(root), verdict='registration_invalid'))
                    continue
                with self.scandir(root) as iterator:
                    paths = sorted((Path(e.path) for e in iterator), key=lambda p: p.name)
                for path in paths:
                    entries.append(dict(path=str(path), verdict=self.verdict(path, root, config)))
            except OSError:
                entries.append(dict(path=str(root), verdict='activity_unknown'))
        return entries

    def scan(self):
        config, hold = self.config()
        return dict(hold=hold, entries=[] if hold else self.candidates(config))

    def status(self):
        return dict(pressure=self.pressure(), reclaim=self.scan(),
                    last_run=saved_json(self.state / 'pressure.json', {}))

    def measure(self, path):
        """Allocated bytes, on one device, bounded independently of tree depth."""
        root_info = self.lstat(path)
        stack, total, count = [path], 0, 0
        deadline = self.monotonic() + 120
        while stack:
            current = stack.pop()
            if self.monotonic() >= deadline or count >= 2000000:
                raise Refusal('measurement_unknown')
            info = self.lstat(current)
            count += 1
            if info.st_dev != root_info.st_dev:
                raise Refusal('hold_other_device')
            total += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                with self.scandir(current) as iterator:
                    for entry in iterator:
                        if count + len(stack) >= 2000000 or self.monotonic() >= deadline:
                            raise Refusal('measurement_unknown')
                        stack.append(Path(entry.path))
        return total

    def free_bytes(self, path):
        try:
            info = self.statvfs(str(path))
            return info.f_bavail * info.f_frsize
        except OSError:
            return None

    def reclaim(self, config, apply):
        report = dict(entries=[], reclaimed_bytes=0, admitted_bytes=0, free_before_bytes=None, free_after_bytes=None)
        if not apply:
            report['entries'] = self.candidates(config)
            return report
        journal_path = self.state / 'pending.json'
        try:
            journal = regular_json(journal_path)
            if journal.get('schema') != 1 or not isinstance(journal.get('pending'), list):
                raise ValueError('invalid recovery journal')
        except FileNotFoundError:
            journal = dict(schema=1, pending=[])
        except (OSError, ValueError):
            report.update(hold='recovery_invalid', recover_failed=True)
            return report
        report['free_before_bytes'] = self.free_bytes(config['roots'][0]) if config['roots'] else None
        pending = journal['pending']
        # A journal intent precedes rename; both rename failures and interrupted
        # deletions are retried before fresh candidates, with authority rechecked.
        queued = [(record, True) for record in list(pending)]
        old_paths = {r.get('original') for r in pending} | {r.get('tombstone') for r in pending}
        queued.extend((dict(original=row['path']), False) for row in self.candidates(config)
                      if row['path'] not in old_paths)
        for record, recovery in queued:
            original = Path(record.get('original', ''))
            root = original.parent
            if root not in [Path(p) for p in config['roots']] or not real_directory(root):
                if recovery:
                    report['entries'].append(dict(path=str(original), verdict='recover_failed', reason='root_unregistered'))
                continue
            tomb = Path(record.get('tombstone', str(root / (TOMBSTONE + original.name + '-' + secrets.token_hex(8)))))
            if tomb.parent != root or not tomb.name.startswith(TOMBSTONE):
                report['entries'].append(dict(path=str(original), verdict='recover_failed', reason='recovery_invalid'))
                continue
            path = tomb if recovery and os.path.lexists(str(tomb)) else original
            if recovery and not os.path.lexists(str(path)):
                pending.remove(record)
                atomic_json(journal_path, journal)
                continue
            row = dict(path=str(path), verdict=self.verdict(path, root, config))
            report['entries'].append(row)
            if row['verdict'] != 'reclaimable':
                if recovery:
                    row.update(reason=row['verdict'], verdict='recover_failed')
                continue
            if path.name.startswith(TOMBSTONE) and not recovery:
                row['verdict'] = 'hold_unjournaled_tombstone'
                continue
            try:
                info = self.lstat(path)
                identity = [info.st_dev, info.st_ino]
                if recovery and identity != record.get('identity'):
                    raise Refusal('identity_changed')
                measured = self.measure(path)
                if report['admitted_bytes'] + measured > config['per_run_byte_cap']:
                    row['verdict'] = 'recover_failed' if recovery else 'hold_byte_cap'
                    if recovery:
                        row['reason'] = 'hold_byte_cap'
                    continue
                # Recheck markers and activity after walking a potentially large
                # tree; no stale scan result authorizes the rename.
                check = self.verdict(path, root, config)
                if check != 'reclaimable':
                    raise Refusal(check)
                if not recovery:
                    record.update(tombstone=str(tomb), identity=identity, measured_bytes=measured)
                    pending.append(record)
                    atomic_json(journal_path, journal)
                row['measured_bytes'] = measured
                if path != tomb:
                    self.rename(path, tomb)
                if [self.lstat(tomb).st_dev, self.lstat(tomb).st_ino] != identity:
                    raise Refusal('identity_changed')
                # shutil.rmtree uses fd-based, symlink-resistant traversal on
                # macOS/POSIX. The final directory is checked again after rename.
                check = self.verdict(tomb, root, config)
                if check != 'reclaimable':
                    raise Refusal(check)
                retention_copy = marker(tomb / 'RETENTION.json')
                release_copy = marker(tomb / 'RELEASE.json', release=True)
                report['admitted_bytes'] += measured
                row['free_before_bytes'] = self.free_bytes(root)
                try:
                    self.delete(tomb)
                except OSError:
                    # A partial rmtree can remove a marker before encountering a
                    # locked file. Restore only our missing markers on the same
                    # directory inode, so the next pass can revalidate authority.
                    try:
                        remaining = self.lstat(tomb)
                        if stat.S_ISDIR(remaining.st_mode) and [remaining.st_dev, remaining.st_ino] == identity:
                            for name, value in (('RETENTION.json', retention_copy), ('RELEASE.json', release_copy)):
                                if not os.path.lexists(str(tomb / name)):
                                    atomic_json(tomb / name, value)
                    except OSError:
                        pass  # Journal stays pending and missing authority holds.
                    raise
                row['free_after_bytes'] = self.free_bytes(root)
                report['reclaimed_bytes'] += measured
                pending.remove(record)
                atomic_json(journal_path, journal)
                row.update(verdict='reclaimed', tombstone=str(tomb))
            except (OSError, Refusal) as exc:
                row.update(verdict='recover_failed' if record in pending else getattr(exc, 'kind', 'measurement_unknown'),
                           reason=str(exc), tombstone=str(tomb))
        report['free_after_bytes'] = self.free_bytes(config['roots'][0]) if config['roots'] else None
        report['recover_failed'] = bool(pending)
        return report

    def notification(self, receipt, config):
        path = self.state / 'notify-state.json'
        previous = saved_json(path, {})
        now, level = self.clock(), receipt['level']
        last = previous.get('notified_at', 0)
        due = (level in ('warn', 'critical') and
               (previous.get('level') != level or level == 'critical' and now - last >= 3600))
        outcome = dict(status='silent')
        if due and config['notify']:
            consumers = []
            pointer = receipt['last_inventory']
            if pointer:
                inventory = saved_json(self.state / Path(pointer['path']).name, {})
                consumers = [r['path'] for r in inventory.get('files', [])[:3]]
            text = '{:.1f} GiB free, {}. Top consumers: {}'.format(
                receipt['container_free_bytes'] / GIB, level, ', '.join(consumers) or 'inventory unavailable')
            script = 'display notification "{}" with title "WorkBay disk"'.format(applescript_string(text))
            try:
                response = self.osascript(['osascript', '-e', script], timeout=10)
                outcome = dict(status='sent' if response.returncode == 0 else 'failed', returncode=response.returncode)
            except (OSError, subprocess.SubprocessError) as exc:
                outcome = dict(status='failed', error=str(exc))
            previous['notified_at'] = now
        previous['level'] = level
        atomic_json(path, previous)
        return outcome

    def run(self, apply=False, trigger='manual'):
        self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        config, hold = self.config()
        receipt = self.pressure(config)
        previous = saved_json(self.state / 'pressure.json', {})
        receipt['last_inventory'] = previous.get('last_inventory')
        receipt['last_reclaim'] = previous.get('last_reclaim')
        report = dict(observed_at=receipt['observed_at'], trigger=trigger, apply=apply,
                      status='ok', exit_code=0, level=receipt['level'])
        lock_fd = None
        try:
            if apply:
                lock_fd = os.open(str(self.state / 'lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    if exc.errno not in (errno.EAGAIN, errno.EACCES):
                        raise
                    report['status'] = 'busy'
            if report['status'] != 'busy':
                interval = {'warn': 21600, 'critical': 3600}.get(receipt['level'])
                last = receipt['last_inventory']
                if interval is not None and (not last or self.clock() - epoch(last['observed_at']) >= interval):
                    inventory = self.inventory(config)
                    inventory['advisories'] = receipt['advisories']
                    compact = datetime.fromtimestamp(self.clock(), timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
                    name = 'inventory-' + compact + '.json'
                    atomic_json(self.state / name, inventory)
                    receipt['last_inventory'] = dict(path=name, observed_at=inventory['observed_at'])
                    for old in sorted(self.state.glob('inventory-*.json'), reverse=True)[10:]:
                        old.unlink()
                report['reclaim'] = (dict(hold=hold, entries=[], reclaimed_bytes=0) if hold else
                                     self.reclaim(config, apply))
                if apply:
                    receipt['last_reclaim'] = dict(observed_at=receipt['observed_at'], **report['reclaim'])
                if report['reclaim'].get('recover_failed'):
                    report['exit_code'] = 3
                report['notification'] = self.notification(receipt, config)
        except (OSError, ValueError, Refusal) as exc:
            report.update(status='error', exit_code=2, error=str(exc))
        finally:
            try:
                atomic_json(self.state / 'pressure.json', receipt)
                log = self.state / 'runs.jsonl'
                if log.exists() and log.stat().st_size > 5 * 1024 ** 2:
                    os.replace(log, self.state / 'runs.jsonl.1')
                with log.open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps(report, sort_keys=True) + '\n')
                    stream.flush()
                    os.fsync(stream.fileno())
            finally:
                if lock_fd is not None:
                    os.close(lock_fd)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    run_parser = commands.add_parser('run', help='observe pressure; reclaim only with --apply')
    run_parser.add_argument('--apply', action='store_true')
    run_parser.add_argument('--trigger', choices=('timer', 'headroom', 'manual'), default='manual')
    commands.add_parser('scan', help='read-only candidate scan')
    commands.add_parser('status', help='read-only pressure and candidate status')
    allocate_parser = commands.add_parser('allocate')
    allocate_parser.add_argument('directory')
    allocate_parser.add_argument('--owner', required=True)
    allocate_parser.add_argument('--retention-class', required=True, choices=CLASSES)
    allocate_parser.add_argument('--task-ref')
    allocate_parser.add_argument('--note')
    allocate_parser.add_argument('--adopt', action='store_true')
    release_parser = commands.add_parser('release')
    release_parser.add_argument('directory')
    release_parser.add_argument('--owner', required=True)
    release_parser.add_argument('--reason', required=True)
    release_parser.add_argument('--evidence-copied-to')
    args = parser.parse_args(argv)
    engine = HostReclaimer()
    try:
        if args.command == 'run':
            output = engine.run(apply=args.apply, trigger=args.trigger)
        elif args.command == 'scan':
            output = engine.scan()
        elif args.command == 'status':
            output = engine.status()
        elif args.command == 'allocate':
            output = engine.allocate(args.directory, args.owner, args.retention_class,
                                     args.task_ref, args.note, args.adopt)
        else:
            output = engine.release(args.directory, args.owner, args.reason, args.evidence_copied_to)
    except (Refusal, OSError, ValueError) as exc:
        output = dict(exit_code=2, error=dict(type=getattr(exc, 'kind', 'error'), message=str(exc)))
    print(json.dumps(output, sort_keys=True))
    return output.get('exit_code', 0)


if __name__ == '__main__':
    sys.exit(main())
