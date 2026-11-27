#!/bin/bash -p
# Sandbox teardown verifier called by scripts/ci/run-vm-check.sh before a retry:
#   vm-check-sandbox-verifier.sh --repository OWNER/REPO --attempt PREVIOUS_ATTEMPT
# It first fences the attempt so vm-check-launcher.sh refuses to provision it
# from then on, then exits 0 only when the attempt's launcher is gone, its
# checkout is gone, no mirror worktree is registered for it, and no process
# from its recorded process group (or working inside its checkout) is alive.
# Configuration matches the launcher: <ci_env_prefix>MIRROR and
# <ci_env_prefix>SANDBOX_ROOT (ACX_VM_CI_MIRROR, ACX_VM_CI_SANDBOX_ROOT for this
# repository). Those names and the fence record schema come from
# config/vm-ci/site.json through the scripts/ci/vm_ci_site.py beside this script
# (symlinks resolved; $VMCI_SITE_CONFIG names another file), resolved once below.
# A missing or invalid loader or config leaves the attempt unconfirmed (exit 1)
# with one line, and nothing is fenced.
# Exit status: 0 confirmed, 1 unconfirmed, 2 usage.
# bash -p ignores BASH_ENV, ENV, SHELLOPTS and exported functions, so nothing
# from the caller runs before the exec below.
set -euo pipefail
site_fail() {
    printf 'vm-check-sandbox-verifier: site configuration: %s\n' "$*" >&2
    exit 1
}
site_self=$(/usr/bin/readlink -e -- "${BASH_SOURCE[0]}") || site_fail "cannot resolve the path of this script"
site_loader=${site_self%/*}/vm_ci_site.py
[[ -f $site_loader ]] || site_fail "loader $site_loader is missing"
# Only the loader's own output may define VMCI_SITE_* (VMCI_SITE_CONFIG chooses the file).
for site_name in ${!VMCI_SITE_@}; do [[ $site_name == VMCI_SITE_CONFIG ]] || unset -v "$site_name"; done
if ! site_env=$(/usr/bin/python3 -I -B "$site_loader" shell 2>&1); then
    site_env=${site_env##*$'\n'}
    site_fail "${site_env#vm-ci-site: }"
fi
# Only the loader's sorted, quoted export lines are evaluated.
while IFS= read -r site_line; do
    [[ $site_line =~ ^export\ VMCI_SITE_[A-Z0-9_]+= ]] || site_fail "unexpected loader output from $site_loader"
done <<<"$site_env"
eval "$site_env"
unset -v site_self site_loader site_env site_line site_name
exec /usr/bin/python3 -I -B - "$@" <<'PY'
import fcntl
import json
import os
import re
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path

ATTEMPT = r"[0-9a-f]{32}"
REPOSITORY = r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"
GIT_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": "/nonexistent",
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0"}
BUDGET = 8          # The router abandons the verifier after 10 s.
LOCK_TIMEOUT = 3
GIT_TIMEOUT = 3

# The site, as the bash prologue resolved it through the loader's shell mode.
SITE_PREFIX, SITE_CONFIG_VARIABLE = "VMCI_SITE_", "VMCI_SITE_CONFIG"
SITE = {name[len(SITE_PREFIX):].lower(): os.environ.pop(name) for name in sorted(os.environ)
        if name.startswith(SITE_PREFIX) and name != SITE_CONFIG_VARIABLE}
SITE_KEYS = ("ci_env_prefix", "schema_vm_check_fence")
if not all(SITE.get(key) for key in SITE_KEYS):
    print("vm-check-sandbox-verifier: site configuration: the site loader did not resolve "
          f"{next(key for key in SITE_KEYS if not SITE.get(key))}", file=sys.stderr)
    sys.exit(1)
MIRROR_ENV, SANDBOX_ROOT_ENV = (SITE["ci_env_prefix"] + name for name in ("MIRROR", "SANDBOX_ROOT"))
FENCE_SCHEMA = SITE["schema_vm_check_fence"]


class Unconfirmed(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Unconfirmed(reason)


def parse(argv):
    require(len(argv) == 4 and argv[0] == "--repository" and argv[2] == "--attempt", "usage")
    require(re.fullmatch(REPOSITORY, argv[1]) is not None, "invalid_repository")
    require(re.fullmatch(ATTEMPT, argv[3]) is not None, "invalid_attempt")
    return argv[1], argv[3]


def sandbox_root():
    raw = os.environ.get(SANDBOX_ROOT_ENV, "")
    require(raw and os.path.isabs(raw), "sandbox_root_unset")
    path = Path(raw)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o077
            and Path(os.path.realpath(path)) == path, "sandbox_root_not_private")
    return path


def sync_directory(path):
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def lock(path, blocking):
    """Return a descriptor holding an exclusive lock on path, or None if busy."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    deadline = time.monotonic() + (LOCK_TIMEOUT if blocking else 0)
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(fd)
                return None
            time.sleep(0.02)


def fence(root, repository, attempt):
    # Same lock as the launcher's fence check and attempt creation.
    held = lock(root / ".provision.lock", blocking=True)
    require(held is not None, "provision_lock_busy")
    try:
        fenced = root / "fenced"
        try:
            fenced.mkdir(mode=0o700)
        except FileExistsError:
            pass
        info = fenced.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid(), "fence_directory_invalid")
        try:
            fd = os.open(fenced / attempt, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                         0o600)
        except FileExistsError:
            return  # Already fenced; fencing is idempotent.
        with os.fdopen(fd, "w") as record:
            json.dump(dict(schema=FENCE_SCHEMA, repository=repository, attempt=attempt,
                           fenced_at=int(time.time())), record, sort_keys=True)
            record.write("\n")
            record.flush()
            os.fsync(record.fileno())
        sync_directory(fenced)
    finally:
        os.close(held)


def registered_worktrees(mirror):
    result = subprocess.run(["/usr/bin/git", f"--git-dir={mirror}", "worktree", "list", "--porcelain", "-z"],
                            env=GIT_ENV, cwd="/", stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=GIT_TIMEOUT)
    require(result.returncode == 0, "mirror_unavailable")
    return {field[len(b"worktree "):] for field in result.stdout.split(b"\0") if field.startswith(b"worktree ")}


def group_alive(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def processes_inside(checkout):
    prefix = str(checkout)
    found = []
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        for link in ("cwd", "root"):
            try:
                target = os.readlink(f"/proc/{entry.name}/{link}").removesuffix(" (deleted)")
            except OSError:
                continue
            if target == prefix or target.startswith(prefix + "/"):
                found.append(int(entry.name))
    return found


def verify(root, attempt):
    directory = root / attempt
    checkout = directory / "checkout"
    if os.path.lexists(directory):
        info = directory.lstat()
        require(stat.S_ISDIR(info.st_mode), "attempt_not_directory")
        if os.path.lexists(directory / "lock"):
            held = lock(directory / "lock", blocking=False)
            require(held is not None, "launcher_active")
            os.close(held)
    require(not os.path.lexists(checkout), "checkout_present")
    raw = os.environ.get(MIRROR_ENV, "")
    require(raw and os.path.isabs(raw) and os.path.isdir(raw), "mirror_unavailable")
    require(os.fsencode(checkout) not in registered_worktrees(os.path.realpath(raw)), "worktree_registered")
    state_path = directory / "state.json"
    if os.path.lexists(state_path):
        state = json.loads(state_path.read_bytes())
        require(isinstance(state, dict) and state.get("attempt") == attempt, "state_invalid")
        pgid = state.get("pgid")
        require(pgid is None or type(pgid) is int and pgid > 1, "state_invalid")
        if pgid is not None:
            require(not group_alive(pgid), "process_group_alive")
    require(not processes_inside(checkout), "process_inside_checkout")


def expired(signum, frame):
    raise Unconfirmed("verifier_budget_exceeded")


def main(argv):
    signal.signal(signal.SIGALRM, expired)
    signal.alarm(BUDGET)
    try:
        repository, attempt = parse(argv)
        root = sandbox_root()
        fence(root, repository, attempt)
        verify(root, attempt)
    except Unconfirmed as error:
        print(f"vm-check-sandbox-verifier: {error}", file=sys.stderr)
        return 2 if str(error) == "usage" else 1
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"vm-check-sandbox-verifier: unconfirmed: {type(error).__name__}", file=sys.stderr)
        return 1
    finally:
        signal.alarm(0)
    return 0


sys.exit(main(sys.argv[1:]))
PY
