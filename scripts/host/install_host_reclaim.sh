#!/bin/bash
# User LaunchAgent installer. No sudo; compatible with the macOS Bash 3.2.
set -euo pipefail
script_directory="${BASH_SOURCE[0]%/*}"
if [ "$script_directory" = "${BASH_SOURCE[0]}" ]; then
    script_directory=.
fi
exec /usr/bin/python3 -I - "$script_directory/workbay_host_reclaim.py" "$@" <<'PY'
import argparse
import json
import os
import plistlib
import subprocess
import sys
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser(description='Install the WorkBay host reclaimer LaunchAgent')
mode = parser.add_mutually_exclusive_group()
mode.add_argument('--dry-run', action='store_true')
mode.add_argument('--uninstall', action='store_true')
parser.add_argument('--root', action='append', default=[])
args = parser.parse_args(sys.argv[2:])
if any(not Path(p).is_absolute() for p in args.root):
    parser.error('--root must be absolute')
home = Path.home()
source = Path(sys.argv[1])
destination = home / '.local/lib/workbay/workbay_host_reclaim.py'
state = home / '.local/state/workbay/host-reclaim'
plist_path = home / 'Library/LaunchAgents/com.workbay.host-reclaim.plist'
registration = home / '.config/workbay/host-reclaim.json'


def run(argv, check=True):
    return subprocess.run(argv, timeout=30, check=check, capture_output=True)


def bootout(uid):
    # launchctl returns nonzero for an absent job.
    run(['launchctl', 'bootout', 'gui/' + uid + '/com.workbay.host-reclaim'], check=False)


def atomic_write(path, content, mode, lint=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '-', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if lint:
            run(['plutil', '-lint', temporary])
        os.replace(temporary, path)
    finally:
        if os.path.lexists(temporary):
            os.unlink(temporary)


try:
    if args.uninstall:
        uid = run(['id', '-u']).stdout.decode().strip()
        bootout(uid)
        if os.path.lexists(str(plist_path)):
            plist_path.unlink()
        print('uninstalled; state and registration retained')
        sys.exit(0)
    plist = dict(Label='com.workbay.host-reclaim',
                 ProgramArguments=['/usr/bin/python3', '-I', str(destination), 'run', '--apply', '--trigger', 'timer'],
                 StartInterval=600, RunAtLoad=True, ProcessType='Background', LowPriorityIO=True, Nice=10,
                 StandardOutPath=str(state / 'stdout.log'), StandardErrorPath=str(state / 'stderr.log'))
    script_data = source.read_bytes()
    plist_data = plistlib.dumps(plist, sort_keys=True)
    changed = False
    for path, content, permissions, lint in ((destination, script_data, 0o755, False),
                                              (plist_path, plist_data, 0o644, True)):
        if (not path.is_symlink() and path.is_file() and path.read_bytes() == content and
                path.stat().st_mode & 0o777 == permissions):
            continue
        changed = True
        if args.dry_run:
            print('would write: ' + str(path))
        else:
            atomic_write(path, content, permissions, lint=lint)
    if not os.path.lexists(str(registration)):
        if args.dry_run:
            print('would write: ' + str(registration) + ' (0600, only if absent)')
        else:
            registration.parent.mkdir(parents=True, exist_ok=True)
            try:
                fd = os.open(str(registration), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass  # A concurrent creator's registration wins, byte for byte.
            else:
                with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                    json.dump(dict(schema=1, roots=args.root), stream, sort_keys=True)
                    stream.write('\n')
                    stream.flush()
                    os.fsync(stream.fileno())
    if args.dry_run:
        if changed:
            print('would write: ' + str(state) + ' (log directory)')
            print('would run: plutil -lint ' + str(plist_path))
            print('would run: launchctl bootout gui/<uid>/com.workbay.host-reclaim')
            print('would run: launchctl bootstrap gui/<uid> ' + str(plist_path))
        else:
            print('unchanged')
    else:
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        if changed:
            uid = run(['id', '-u']).stdout.decode().strip()
            bootout(uid)
            run(['launchctl', 'bootstrap', 'gui/' + uid, str(plist_path)])
            print('installed')
        else:
            print('unchanged')
except (OSError, subprocess.SubprocessError) as exc:
    print(str(exc), file=sys.stderr)
    sys.exit(2)
PY
