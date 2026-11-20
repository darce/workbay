#!/bin/bash
# User LaunchAgent installer. No sudo; compatible with the macOS Bash 3.2.
set -euo pipefail
script_directory="${BASH_SOURCE[0]%/*}"
if [ "$script_directory" = "${BASH_SOURCE[0]}" ]; then
    script_directory=.
fi
exec /usr/bin/python3 -I - "$script_directory/workbay_host_reclaim.py" "$@" <<'PY'
import argparse
import importlib.util
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
if args.uninstall and args.root:
    parser.error('--root cannot be used with --uninstall')
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
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location('workbay_host_reclaim', source)
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    engine = native.HostReclaimer(home=home)
    # Validate the entire batch/config before code, plist, logs or service writes.
    # No explicit roots preserves an existing config byte for byte, even if invalid.
    registration_result = None
    if args.root or not os.path.lexists(str(registration)):
        try:
            registration_result = engine._register_roots(args.root, dry_run=args.dry_run)
        except native.Refusal as exc:
            raise ValueError(str(exc)) from exc
        if registration_result['outcome'] != 'unchanged':
            print(('would register: ' if args.dry_run else 'registered: ') + str(registration))
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
    if args.dry_run:
        if changed:
            print('would write: ' + str(state) + ' (log directory)')
            print('would run: plutil -lint ' + str(plist_path))
            print('would run: launchctl bootout gui/<uid>/com.workbay.host-reclaim')
            print('would run: launchctl bootstrap gui/<uid> ' + str(plist_path))
        elif registration_result is None or registration_result['outcome'] == 'unchanged':
            print('unchanged')
    else:
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        if changed:
            uid = run(['id', '-u']).stdout.decode().strip()
            bootout(uid)
            run(['launchctl', 'bootstrap', 'gui/' + uid, str(plist_path)])
            print('installed')
        elif registration_result is None or registration_result['outcome'] == 'unchanged':
            print('unchanged')
except (OSError, ValueError, subprocess.SubprocessError) as exc:
    print(str(exc), file=sys.stderr)
    sys.exit(2)
PY
