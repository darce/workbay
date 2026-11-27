#!/bin/bash -p
# VM check launcher called by scripts/ci/run-vm-check.sh:
#   vm-check-launcher.sh --repository OWNER/REPO --commit SHA --script RELATIVE_ROUTE \
#       --check ID --attempt ATTEMPT
# Configuration (the router scrubs its environment, so the operator-installed
# wrapper must export these):
#   ACX_VM_CI_MIRROR        absolute path of a bare mirror of the repository
#   ACX_VM_CI_SANDBOX_ROOT  absolute, private (0700) directory owned by this user
#   ACX_VM_CI_TOOLCHAIN     optional absolute directory provisioned by
#                           scripts/ci/vm-ci-toolchain.sh. When set, the check's
#                           PATH is TOOLCHAIN/bin:/usr/local/bin:/usr/bin:/bin with
#                           TOOLCHAIN/bin resolved (and recorded) once per attempt;
#                           a missing or writable-by-others TOOLCHAIN/bin is
#                           unavailable (3). Unset or empty keeps the minimal PATH.
#   ACX_VM_CI_USERNS_CHECKS optional comma-separated check ids (for example
#                           snapshot-checker) run in a new user namespace where
#                           UID/GID 0 map to this user and the subordinate ranges
#                           from /etc/subuid and /etc/subgid are mapped from 1 up
#                           (util-linux `unshare --map-root-user --map-auto`, which
#                           needs the setuid newuidmap/newgidmap helpers, so the
#                           launcher itself must not run with no_new_privs).
#   ACX_VM_CI_HANDOFF_PYTHON optional absolute path of the WorkBay virtualenv
#                           Python 3.12 (scripts/ci/vm-ci-install.sh
#                           --handoff-python). Passed, unresolved so the venv is
#                           still detected, to the handoff-integrity check only, as
#                           ACX_CI_HANDOFF_PYTHON; other checks never see it. For
#                           that check a relative, ':'-bearing, missing,
#                           non-executable or writable-by-others interpreter (or
#                           bin directory) is unavailable (3). Unset or empty
#                           passes nothing, and the adapter reports unavailable.
# The production stack shares this host, so checks are admitted and bounded
# (RES-31); unset or empty values take the defaults:
#   ACX_VM_CI_MAX_USED_PCT  integer 50..89, default 85. A check is refused
#                           (disk_floor, exit 75) when the filesystem holding
#                           the sandbox root or / is at or above it, both before
#                           the sandbox is created and again once the checkout is
#                           materialized (the sandbox is then removed). It stays
#                           below the postgres healthcheck disk guard
#                           (ACX_PG_DISK_MAX_USED_PCT, default 90), so CI refuses
#                           work before the database stacks turn unhealthy.
#   ACX_VM_CI_CPU_QUOTA     CPUQuota percentage, default 200%, below the host's
#                           CPU count x 100%.
#   ACX_VM_CI_MEMORY_MAX    MemoryMax (digits plus K, M, G or T), default 6G, at
#                           least 256M and below the host's physical memory.
#   ACX_VM_CI_ALLOW_UNBOUNDED "1" runs checks without the scope (logging a
#                           warning) when systemd-run --user is unavailable;
#                           otherwise that is unavailable (3). "", "0" or "1".
# Each check runs in a transient systemd user scope (systemd-run --user --scope
# with CPUQuota, MemoryMax, IOWeight=50 and nice 10), found on the launcher's
# PATH with df and reached through XDG_RUNTIME_DIR (default /run/user/UID).
# The scope reports having started before the check runs, so a scope that
# cannot be created is unavailable (3), never the check's own status.
# Every check process runs with no_new_privs (set after the namespace mapping),
# so candidate code cannot gain privileges through setuid executables.
# The launcher is the check's child subreaper (no PID namespace): orphaned
# descendants are reparented to it, and it reaps each one as it exits (on
# SIGCHLD) while the check runs, so a process a check killed is gone rather
# than a zombie that still answers kill -0; teardown kills whatever remains.
# Teardown removes files left by mapped subordinate UIDs by repeating the
# removal inside the same kind of namespace when a plain removal fails. After
# every attempt, passed or not, only the evidence (state.json, output.log and
# the attempt lock) is left in the attempt directory.
# Exit status: the check's own status, 2 for invalid arguments, 3 when the
# mirror, commit, adapter, toolchain, user namespace, resource scope or sandbox
# is unavailable or the attempt is reused or fenced, 75 when the disk floor
# refuses the check, 130/143 when cancelled by SIGINT/SIGTERM (SIGHUP reports
# 143).
# Site configuration: the variable names above (ACX_VM_CI_* is this repository's
# ci_env_prefix; the check sees ACX_VM_CHECK_* from vm_env_prefix and
# ACX_CI_HANDOFF_PYTHON from env_prefix), the state.json schema and the scope unit
# prefix come from config/vm-ci/site.json through scripts/ci/vm_ci_site.py,
# resolved once below before anything else runs. The loader is the one beside
# this script (symlinks resolved) and it reads the config of the tree that holds
# it, so an installed release carries both; $VMCI_SITE_CONFIG names another file.
# A missing or invalid loader or config is unavailable (3) with one line; there
# are no built-in defaults.
# bash -p ignores BASH_ENV, ENV, SHELLOPTS and exported functions, so nothing
# from the caller runs before the exec below.
set -euo pipefail
site_fail() {
    printf 'vm-check-launcher: site configuration: %s\n' "$*" >&2
    exit 3
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
import ctypes
import fcntl
import json
import os
import re
import select
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

USAGE, UNAVAILABLE, DISK_FLOOR = 2, 3, 75   # 75: EX_TEMPFAIL, retry once disk is freed.
FLAGS = ("repository", "commit", "script", "check", "attempt")
PATTERNS = {
    "repository": r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+",
    "commit": r"(?!0{40})[0-9a-f]{40}",
    "script": r"scripts/ci/checks/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*",
    "check": r"[a-z0-9][a-z0-9-]{0,63}",
    "attempt": r"[0-9a-f]{32}",
}
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
GIT_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": "/nonexistent",
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0"}
GIT = ("/usr/bin/git", "-c", "core.hooksPath=/dev/null", "-c", "protocol.allow=never")
GIT_TIMEOUT = 300
LOCK_TIMEOUT = 60
CHECK_TERM_GRACE = 10   # The router allows 30 s for the whole teardown.
SWEEP_TIMEOUT = 5
REMOVE_TIMEOUT = 10     # Bounded namespaced removal of subordinate-UID files.
PR_SET_CHILD_SUBREAPER = 36
SYSTEM_PATH = "/usr/bin:/bin"
TOOLCHAIN_PATH_TAIL = "/usr/local/bin:/usr/bin:/bin"
SETPRIV = "/usr/bin/setpriv"
# --map-root-user maps 0 to this user; --map-auto maps the first /etc/subuid and
# /etc/subgid blocks from 1 up through the setuid newuidmap/newgidmap helpers.
UNSHARE = ("/usr/bin/unshare", "--user", "--map-root-user", "--map-auto", "--")
# unshare finds newuidmap through PATH; never through the check's toolchain.
UNSHARE_ENV = {"PATH": SYSTEM_PATH, "LANG": "C.UTF-8"}
# The only check given the WorkBay interpreter (scripts/ci/checks/backend.sh).
HANDOFF_CHECK = "handoff-integrity"
# Host protection (RES-31). The disk floor must stay below the postgres
# healthcheck guard (ACX_PG_DISK_MAX_USED_PCT, default 90).
DEFAULT_MAX_USED_PCT, MAX_USED_PCT_RANGE = 85, (50, 89)
DEFAULT_CPU_QUOTA, DEFAULT_MEMORY_MAX = "200%", "6G"
MIN_MEMORY_MAX = 256 << 20
IO_WEIGHT, NICE = 50, 10
DF_TIMEOUT = 10
TOOL_ENV = {"PATH": SYSTEM_PATH, "LANG": "C"}
# Teardown leaves only these in the attempt directory; the verifier reads them.
EVIDENCE = frozenset({"state.json", "output.log", "lock"})

# The site, as the bash prologue resolved it through the loader's shell mode. The
# variables are consumed here, so no check process inherits them.
SITE_PREFIX, SITE_CONFIG_VARIABLE = "VMCI_SITE_", "VMCI_SITE_CONFIG"
SITE = {name[len(SITE_PREFIX):].lower(): os.environ.pop(name) for name in sorted(os.environ)
        if name.startswith(SITE_PREFIX) and name != SITE_CONFIG_VARIABLE}
SITE_KEYS = ("ci_env_prefix", "vm_env_prefix", "env_prefix", "schema_vm_check_sandbox", "check_scope_prefix")
if not all(SITE.get(key) for key in SITE_KEYS):
    print("vm-check-launcher: site configuration: the site loader did not resolve "
          f"{next(key for key in SITE_KEYS if not SITE.get(key))}", file=sys.stderr, flush=True)
    sys.exit(3)
# Launcher configuration, exported by the installed wrapper (<ci_env_prefix>NAME).
MIRROR_ENV, SANDBOX_ROOT_ENV, TOOLCHAIN_ENV, USERNS_CHECKS_ENV, HANDOFF_PYTHON_ENV = (
    SITE["ci_env_prefix"] + name for name in ("MIRROR", "SANDBOX_ROOT", "TOOLCHAIN", "USERNS_CHECKS",
                                              "HANDOFF_PYTHON"))
MAX_USED_PCT_ENV, CPU_QUOTA_ENV, MEMORY_MAX_ENV, ALLOW_UNBOUNDED_ENV = (
    SITE["ci_env_prefix"] + name for name in ("MAX_USED_PCT", "CPU_QUOTA", "MEMORY_MAX", "ALLOW_UNBOUNDED"))
# What the check itself sees: its id and commit, and the WorkBay interpreter.
CHECK_ID_ENV, CHECK_COMMIT_ENV = (SITE["vm_env_prefix"] + name for name in ("CHECK_ID", "CHECK_COMMIT"))
CHECK_HANDOFF_ENV = f"{SITE['env_prefix']}CI_HANDOFF_PYTHON"
SANDBOX_SCHEMA = SITE["schema_vm_check_sandbox"]
SCOPE_PREFIX = SITE["check_scope_prefix"]


class Exit(Exception):
    def __init__(self, code, reason):
        super().__init__(reason)
        self.code, self.reason = code, reason


class Cancelled(Exception):
    def __init__(self, signum):
        super().__init__(signal.Signals(signum).name)
        self.code = 130 if signum == signal.SIGINT else 143


class Cancellation:
    """Record cancellation signals without raising; poll() wakes on the pipe.

    SIGCHLD also wakes the pipe (without cancelling), so the launcher, as the
    check's child subreaper, reaps orphans as soon as they exit."""

    def __init__(self):
        self.signals = []
        self.read, self.write = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        signal.set_wakeup_fd(self.write, warn_on_full_buffer=False)
        for sig in SIGNALS:
            signal.signal(sig, lambda signum, frame: self.signals.append(signum))
        signal.signal(signal.SIGCHLD, lambda signum, frame: None)

    def pending(self):
        try:
            while os.read(self.read, 512):
                pass
        except BlockingIOError:
            pass
        return self.signals[0] if self.signals else None

    def check(self):
        if self.pending() is not None:
            raise Cancelled(self.signals[0])


def log(message):
    print(f"vm-check-launcher: {message}", file=sys.stderr, flush=True)


def parse(argv):
    values = {}
    if len(argv) != 2 * len(FLAGS):
        raise Exit(USAGE, "usage")
    for flag, value in zip(argv[::2], argv[1::2]):
        name = flag.removeprefix("--")
        if not flag.startswith("--") or name not in FLAGS or name in values:
            raise Exit(USAGE, "usage")
        if re.fullmatch(PATTERNS[name], value) is None:
            raise Exit(USAGE, f"invalid_{name}")
        values[name] = value
    parts = values["script"].split("/")
    if any(part in ("", ".", "..") for part in parts) or not parts[-1].endswith(".sh"):
        raise Exit(USAGE, "invalid_script")
    return values


def private_root(name):
    raw = os.environ.get(name, "")
    path = Path(raw)
    if not raw or not path.is_absolute():
        raise Exit(UNAVAILABLE, f"{name.lower()}_unset")
    try:
        info = path.lstat()
    except OSError:
        raise Exit(UNAVAILABLE, f"{name.lower()}_missing") from None
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077
            or Path(os.path.realpath(path)) != path):
        raise Exit(UNAVAILABLE, f"{name.lower()}_not_private")
    return path


def toolchain_bin():
    """Return the resolved TOOLCHAIN/bin, or None when no toolchain is configured."""
    raw = os.environ.get(TOOLCHAIN_ENV, "")
    if not raw:
        return None
    # ':' would inject PATH entries; a non-normalized path hides what is trusted.
    if not os.path.isabs(raw) or ":" in raw or os.path.normpath(raw) != raw.rstrip("/"):
        raise Exit(UNAVAILABLE, "toolchain_invalid")
    # Resolve once so a provisioner switching a symlink cannot change the
    # toolchain under a running attempt; state.json records what was used.
    resolved = os.path.realpath(os.path.join(raw, "bin"))
    try:
        info = os.stat(resolved)
    except OSError:
        raise Exit(UNAVAILABLE, "toolchain_unavailable") from None
    if ":" in resolved or not stat.S_ISDIR(info.st_mode):
        raise Exit(UNAVAILABLE, "toolchain_unavailable")
    if not trusted(info):
        raise Exit(UNAVAILABLE, "toolchain_not_trusted")
    return resolved


def trusted(info):
    return info.st_uid in (0, os.geteuid()) and not info.st_mode & 0o022


def handoff_python(check):
    """Return the WorkBay interpreter for the handoff-integrity check, else None."""
    raw = os.environ.get(HANDOFF_PYTHON_ENV, "")
    if check != HANDOFF_CHECK or not raw:
        return None
    # The adapter puts the interpreter's directory first on its PATH, so ':'
    # would inject PATH entries. It is passed unresolved: a virtualenv is
    # recognized from the path it is invoked through.
    if not os.path.isabs(raw) or ":" in raw or os.path.normpath(raw) != raw:
        raise Exit(UNAVAILABLE, "handoff_python_invalid")
    try:
        interpreter, directory = os.stat(raw), os.stat(os.path.dirname(raw))
    except OSError:
        raise Exit(UNAVAILABLE, "handoff_python_unavailable") from None
    if not stat.S_ISREG(interpreter.st_mode) or not os.access(raw, os.X_OK):
        raise Exit(UNAVAILABLE, "handoff_python_unavailable")
    if not trusted(interpreter) or not trusted(directory):
        raise Exit(UNAVAILABLE, "handoff_python_not_trusted")
    return raw


def userns_checks():
    raw = os.environ.get(USERNS_CHECKS_ENV, "")
    checks = raw.split(",") if raw else []
    if any(re.fullmatch(PATTERNS["check"], check) is None for check in checks):
        raise Exit(UNAVAILABLE, "userns_checks_invalid")
    return frozenset(checks)


def find_tool(name):
    """Resolve a host tool on the launcher's PATH (absolute entries only).

    The router runs the launcher with PATH=/usr/bin:/bin. A tool someone else
    could replace is refused rather than skipped."""
    raw = os.environ.get("PATH", "")
    entries = [entry for entry in raw.split(":") if entry.startswith("/")] or SYSTEM_PATH.split(":")
    for entry in entries:
        candidate = os.path.join(entry, name)
        if not os.path.isfile(candidate) or not os.access(candidate, os.X_OK):
            continue
        resolved = os.path.realpath(candidate)
        if not trusted(os.stat(resolved)) or not trusted(os.stat(os.path.dirname(resolved))):
            raise Exit(UNAVAILABLE, f"{name.replace('-', '_')}_not_trusted")
        return resolved
    return None


def disk_limit():
    raw = os.environ.get(MAX_USED_PCT_ENV, "")
    if not raw:
        return DEFAULT_MAX_USED_PCT
    low, high = MAX_USED_PCT_RANGE
    if re.fullmatch(r"[1-9][0-9]", raw) is None or not low <= int(raw) <= high:
        raise Exit(UNAVAILABLE, "max_used_pct_invalid")
    return int(raw)


def disk_usage(df, paths):
    """Return the used percentage (as df and the postgres guard report it) per path."""
    try:
        result = subprocess.run((df, "--output=pcent", "--", *map(str, paths)), env=TOOL_ENV, cwd="/",
                                stdin=subprocess.DEVNULL, capture_output=True, timeout=DF_TIMEOUT, check=False)
    except (OSError, subprocess.SubprocessError):
        raise Exit(UNAVAILABLE, "disk_usage_unavailable") from None
    lines = result.stdout.decode("ascii", "replace").splitlines()[1:]
    matches = [re.fullmatch(r"\s*([0-9]{1,3})%\s*", line) for line in lines]
    if result.returncode != 0 or len(matches) != len(paths) or not all(matches):
        raise Exit(UNAVAILABLE, "disk_usage_unavailable")
    return {str(path): int(match.group(1)) for path, match in zip(paths, matches)}


class DiskFloor:
    """Admission against the filesystems holding the sandbox root and /."""

    def __init__(self, root):
        self.root, self.limit = root, disk_limit()
        self.df = find_tool("df")
        if self.df is None:
            raise Exit(UNAVAILABLE, "disk_usage_unavailable")

    def admit(self, stage):
        usage = disk_usage(self.df, [self.root, Path("/")])
        for path, used in usage.items():
            if used >= self.limit:
                log(f"disk_floor {stage}: {path} is {used}% used, at or above {self.limit}%")
                raise Exit(DISK_FLOOR, "disk_floor")
        return usage


def cpu_quota():
    raw = os.environ.get(CPU_QUOTA_ENV, "") or DEFAULT_CPU_QUOTA
    match = re.fullmatch(r"([1-9][0-9]{0,5})%", raw)
    # A quota covering every CPU bounds nothing.
    if match is None or int(match.group(1)) >= 100 * len(os.sched_getaffinity(0)):
        raise Exit(UNAVAILABLE, "cpu_quota_invalid")
    return raw


def memory_max():
    raw = os.environ.get(MEMORY_MAX_ENV, "") or DEFAULT_MEMORY_MAX
    match = re.fullmatch(r"([1-9][0-9]{0,9})([KMGT])", raw)
    if match is None:
        raise Exit(UNAVAILABLE, "memory_max_invalid")
    size = int(match.group(1)) << (10 * "KMGT".index(match.group(2)) + 10)
    physical = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    if not MIN_MEMORY_MAX <= size < physical:
        raise Exit(UNAVAILABLE, "memory_max_invalid")
    return raw


def user_runtime_dir():
    """The user manager's runtime directory, or None when it is unusable."""
    raw = os.environ.get("XDG_RUNTIME_DIR", "") or f"/run/user/{os.geteuid()}"
    if not os.path.isabs(raw) or os.path.normpath(raw) != raw:
        return None
    try:
        info = os.stat(raw)
    except OSError:
        return None
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        return None
    return raw


class Scope:
    def __init__(self, argv, env, bounds):
        self.argv, self.env, self.bounds = argv, env, bounds


def resource_scope(attempt):
    """Return the systemd-run prefix bounding the check, or None when explicitly unbounded."""
    allow = os.environ.get(ALLOW_UNBOUNDED_ENV, "")
    if allow not in ("", "0", "1"):
        raise Exit(UNAVAILABLE, "allow_unbounded_invalid")
    cpu, memory = cpu_quota(), memory_max()
    systemd_run = find_tool("systemd-run")
    runtime = user_runtime_dir()
    if systemd_run is None or runtime is None:
        reason = "systemd_run_unavailable" if systemd_run is None else "user_runtime_dir_unavailable"
        if allow != "1":
            raise Exit(UNAVAILABLE, reason)
        log(f"warning: {reason}; running the check without resource bounds ({ALLOW_UNBOUNDED_ENV}=1)")
        return None
    bounds = dict(cpu_quota=cpu, memory_max=memory, io_weight=IO_WEIGHT, nice=NICE)
    argv = [systemd_run, "--user", "--scope", "--quiet", "--collect", f"--unit={SCOPE_PREFIX}{attempt}",
            "-p", f"CPUQuota={cpu}", "-p", f"MemoryMax={memory}", "-p", f"IOWeight={IO_WEIGHT}",
            "--nice", str(NICE), "--"]
    return Scope(argv, {"XDG_RUNTIME_DIR": runtime}, bounds)


def wait_exit(pid, cancel, timeout, cancellable, reap_orphans=False):
    """Wait for a child via its pidfd. Return the pending signal or None on exit.

    With reap_orphans, every wakeup (SIGCHLD included) also reaps the other
    exited children: while the check runs those are its orphaned descendants,
    reparented to this subreaper. pid itself is left for Popen.wait()."""
    deadline = None if timeout is None else time.monotonic() + timeout
    child = os.pidfd_open(pid)
    try:
        poller = select.poll()
        poller.register(child, select.POLLIN)
        poller.register(cancel.read, select.POLLIN)
        while True:
            # Drain the wakeups before reaping: a SIGCHLD arriving after the
            # drain either is reaped below or leaves a byte that wakes poll().
            signum = cancel.pending()
            if reap_orphans:
                reap_other_children(pid)
            if cancellable and signum is not None:
                return signum
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise subprocess.TimeoutExpired(str(pid), timeout)
            ready = {fd for fd, _ in poller.poll(None if remaining is None else max(1, int(remaining * 1000)))}
            if child in ready:
                return cancel.pending() if cancellable else None
    finally:
        os.close(child)


def git(cancel, *args, cancellable=True, check=True, timeout=GIT_TIMEOUT):
    with tempfile.TemporaryFile() as output, subprocess.Popen(
            (*GIT, *args), env=GIT_ENV, cwd="/", stdin=subprocess.DEVNULL, stdout=output,
            stderr=subprocess.DEVNULL) as process:
        try:
            interrupted = wait_exit(process.pid, cancel, timeout, cancellable)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise Exit(UNAVAILABLE, "git_timeout") from None
        if interrupted is not None:
            process.kill()
            process.wait()
            raise Cancelled(interrupted)
        status = process.wait()
        output.seek(0)
        content = output.read()
    if check and status != 0:
        raise Exit(UNAVAILABLE, "git_failed")
    return status, content


class Lock:
    def __init__(self, path, cancel, cancellable=True, timeout=LOCK_TIMEOUT):
        self.path, self.cancel, self.cancellable, self.timeout = path, cancel, cancellable, timeout
        self.fd = None

    def __enter__(self):
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.fd = fd
                return self
            except BlockingIOError:
                if self.cancellable and self.cancel.pending() is not None:
                    os.close(fd)
                    raise Cancelled(self.cancel.signals[0]) from None
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise Exit(UNAVAILABLE, "lock_timeout") from None
                time.sleep(0.02)

    def __exit__(self, *exc):
        os.close(self.fd)
        self.fd = None


def sync_directory(path):
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json(path, data):
    with tempfile.NamedTemporaryFile("w", dir=path.parent, prefix=".state-", delete=False) as staged:
        try:
            json.dump(data, staged, sort_keys=True)
            staged.write("\n")
            staged.flush()
            os.fsync(staged.fileno())
            os.replace(staged.name, path)
        except BaseException:
            os.unlink(staged.name)
            raise
    sync_directory(path.parent)


def children_by_parent():
    children = {}
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        try:
            data = Path("/proc", entry.name, "stat").read_bytes()
        except OSError:
            continue
        ppid = int(data[data.rindex(b")") + 2:].split()[1])
        children.setdefault(ppid, []).append(int(entry.name))
    return children


def reap_other_children(keep):
    """Reap every exited child except keep, without blocking.

    As the child subreaper the launcher is the parent of every orphaned
    descendant of the check. Unreaped, a killed orphan stays a zombie that
    os.kill(pid, 0) still reports, so a check could not observe that a process
    it killed is gone; reaping here gives checks normal process semantics.
    Only this process reaps its children, so a listed pid cannot be reused
    before waitpid() collects it."""
    for pid in children_by_parent().get(os.getpid(), ()):
        if pid == keep:
            continue
        try:
            os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            pass


def descendants():
    children = children_by_parent()
    found, stack = [], [os.getpid()]
    while stack:
        for pid in children.get(stack.pop(), ()):
            found.append(pid)
            stack.append(pid)
    return found


def reap():
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def sweep():
    """Kill every remaining descendant. As a child subreaper the launcher
    inherits orphaned workers, including those that left the check's session."""
    deadline = time.monotonic() + SWEEP_TIMEOUT
    while True:
        reap()
        remaining = descendants()
        if not remaining:
            return True
        if time.monotonic() >= deadline:
            return False
        for pid in remaining:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        time.sleep(0.02)


def remove_tree(path):
    def retry(function, name, error):
        # The check may have removed write permission from its own directories.
        os.chmod(os.path.dirname(name), 0o700)
        if os.path.isdir(name) and not os.path.islink(name):
            os.chmod(name, 0o700)
        function(name)

    if os.path.lexists(path):
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path, onexc=retry)
        else:
            os.unlink(path)


def remove_paths(paths):
    """Remove each path; return True when none is left.

    A user-namespaced check can leave files owned by mapped subordinate UIDs,
    which this user can neither unlink nor chmod. Those are removed by `rm`
    running as root of a new namespace with the same mapping: its privilege
    covers exactly this user's own and subordinate IDs (RES-31)."""
    leftover = []
    for path in paths:
        try:
            remove_tree(path)
        except OSError:
            leftover.append(path)
    if leftover:
        log(f"removing {len(leftover)} sandbox path(s) inside a user namespace")
        try:
            subprocess.run((*UNSHARE, "/bin/rm", "-rf", "--one-file-system", "--", *map(str, leftover)),
                           env=UNSHARE_ENV, cwd="/", stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=REMOVE_TIMEOUT, check=False)
        except (OSError, subprocess.SubprocessError):
            pass
    return not any(os.path.lexists(path) for path in paths)


def registered(cancel, mirror, checkout, cancellable):
    _, listing = git(cancel, f"--git-dir={mirror}", "worktree", "list", "--porcelain", "-z",
                     cancellable=cancellable)
    paths = [field[len(b"worktree "):] for field in listing.split(b"\0") if field.startswith(b"worktree ")]
    return os.fsencode(checkout) in paths


def check_environment(args, home, scratch, toolchain, handoff=None):
    if toolchain is not None:
        path = f"{toolchain}:{TOOLCHAIN_PATH_TAIL}"
    else:
        path = os.environ.get("PATH", "")
        if not path or not all(part.startswith("/") for part in path.split(":")):
            path = SYSTEM_PATH
    env = {"PATH": path, "HOME": str(home), "LANG": "C.UTF-8", "TMPDIR": str(scratch),
           CHECK_ID_ENV: args["check"], CHECK_COMMIT_ENV: args["commit"]}
    if args["check"] == "i18n":
        # The runbook gives i18n the tested commit and an attempt-specific RUNNER_TEMP.
        env.update(GITHUB_SHA=args["commit"], RUNNER_TEMP=str(scratch))
    if handoff is not None:
        # The adapter runs the contract with `env -i`, its own HOME, TMPDIR and
        # PATH, so the interpreter is all it takes from the launcher.
        env[CHECK_HANDOFF_ENV] = handoff
    return env


def regular_file_in(root, relative):
    current = root
    for part in relative.split("/"):
        current = current / part
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode):
            return False
    return stat.S_ISREG(info.st_mode)


def check_command(args, env, userns, scope, started_fd):
    """Return (argv, env) for the check process.

    The process keeps one PID from start to finish (systemd-run --scope,
    unshare, setpriv, env and bash all exec), so it stays the session and
    process-group leader. Once inside the resource scope it writes "s" to
    started_fd, and immediately before the adapter it writes ".", so a failure
    of systemd-run, unshare or setpriv is reported as unavailable instead of as
    the check's own status. env -i leaves the check only its own environment."""
    adapter = ("/bin/bash", args["script"], args["check"])
    marker = f'printf . >&{started_fd} && exec {started_fd}>&- && exec "$0" "$@"'
    argv = [SETPRIV, "--no-new-privs", "--", "/usr/bin/env", "-i",
            *(f"{key}={value}" for key, value in env.items()), "/bin/bash", "-c", marker, *adapter]
    if userns:
        argv = [*UNSHARE, *argv]
    if scope is None:
        return argv, UNSHARE_ENV
    entered = f'printf s >&{started_fd} && exec "$0" "$@"'
    return [*scope.argv, "/bin/bash", "-c", entered, *argv], UNSHARE_ENV | scope.env


class Sandbox:
    def __init__(self, args, mirror, root, cancel, disk, scope, toolchain=None, userns=False, handoff=None):
        self.args, self.mirror, self.root, self.cancel = args, mirror, root, cancel
        self.disk, self.scope = disk, scope
        self.toolchain, self.userns, self.handoff = toolchain, userns, handoff
        self.directory = root / args["attempt"]
        self.checkout = self.directory / "checkout"
        self.home = self.directory / "home"
        self.scratch = self.directory / "tmp"
        self.state_path = self.directory / "state.json"
        self.process = None
        self.state = dict(schema=SANDBOX_SCHEMA, repository=args["repository"],
                          commit=args["commit"], check=args["check"], script=args["script"],
                          attempt=args["attempt"], launcher_pid=os.getpid(), pid=None, pgid=None,
                          toolchain=toolchain, userns=userns, handoff_python=handoff,
                          resource_bounds=None if scope is None else scope.bounds,
                          disk_max_used_pct=disk.limit, disk_used_pct=None,
                          started_at=int(time.time()), status="provisioning", finished_at=None,
                          exit_status=None, reason=None, teardown=None)

    def save(self):
        write_json(self.state_path, self.state)

    def provision(self):
        self.save()
        self.home.mkdir(mode=0o700)
        self.scratch.mkdir(mode=0o700)
        with Lock(self.root / ".mirror.lock", self.cancel):
            git(self.cancel, f"--git-dir={self.mirror}", "worktree", "add", "--detach", "--quiet",
                str(self.checkout), self.args["commit"])
        _, head = git(self.cancel, "-C", str(self.checkout), "rev-parse", "--verify", "HEAD^{commit}")
        if head.decode().strip() != self.args["commit"]:
            raise Exit(UNAVAILABLE, "checkout_head_mismatch")
        if not regular_file_in(self.checkout, self.args["script"]):
            raise Exit(UNAVAILABLE, "script_unavailable")
        self.cancel.check()
        # The materialized checkout may itself have crossed the floor; teardown
        # then removes the sandbox before anything runs.
        self.state["disk_used_pct"] = self.disk.admit("after provisioning")
        self.cancel.check()

    def run(self):
        self.provision()
        started_read, started_write = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        try:
            env = check_environment(self.args, self.home, self.scratch, self.toolchain, self.handoff)
            argv, env = check_command(self.args, env, self.userns, self.scope, started_write)
            with open(self.directory / "output.log", "wb") as output:
                self.process = subprocess.Popen(
                    argv, cwd=self.checkout, env=env, stdin=subprocess.DEVNULL, stdout=output,
                    stderr=subprocess.STDOUT, start_new_session=True, close_fds=True,
                    pass_fds=(started_write,))
            os.close(started_write)
            started_write = None
            pgid = self.process.pid  # A new session leader leads its own process group.
            self.state.update(pid=self.process.pid, pgid=pgid, status="running")
            self.save()
            interrupted = wait_exit(self.process.pid, self.cancel, None, True, reap_orphans=True)
            if interrupted is not None:
                self.signal_group(signal.SIGTERM)
                try:
                    wait_exit(self.process.pid, self.cancel, CHECK_TERM_GRACE, False, reap_orphans=True)
                except subprocess.TimeoutExpired:
                    pass
                raise Cancelled(interrupted)
            status = self.process.wait()
            try:
                markers = os.read(started_read, 8)
            except BlockingIOError:
                markers = b""
            if self.scope is not None and not markers.startswith(b"s"):
                raise Exit(UNAVAILABLE, "resource_scope_unavailable")
            if not markers.endswith(b"."):
                raise Exit(UNAVAILABLE, "userns_unavailable" if self.userns else "check_start_unavailable")
            return status if status >= 0 else 128 - status
        finally:
            os.close(started_read)
            if started_write is not None:
                os.close(started_write)

    def signal_group(self, sig):
        try:
            os.killpg(self.process.pid, sig)
        except ProcessLookupError:
            pass

    def teardown(self):
        if self.process is not None:
            self.signal_group(signal.SIGKILL)
            self.process.wait()
        workers_gone = sweep()
        guard = Lock(self.root / ".mirror.lock", self.cancel, cancellable=False)
        try:
            guard.__enter__()
        except Exit:
            # Teardown still proceeds; git marks worktrees being added as locked,
            # so an unserialized prune cannot remove another attempt's checkout.
            guard = None
        try:
            if os.path.lexists(self.checkout):
                git(self.cancel, f"--git-dir={self.mirror}", "worktree", "remove", "--force", "--force",
                    str(self.checkout), cancellable=False, check=False)
            checkout_gone = remove_paths([self.checkout])
            git(self.cancel, f"--git-dir={self.mirror}", "worktree", "prune", cancellable=False, check=False)
            still_registered = registered(self.cancel, self.mirror, self.checkout, False)
        finally:
            if guard is not None:
                guard.__exit__()
        private_gone = remove_paths([self.home, self.scratch])
        # The check can write beside its checkout; keep only the evidence.
        strays = [self.directory / name for name in sorted(os.listdir(self.directory)) if name not in EVIDENCE]
        strays_gone = remove_paths(strays)
        return workers_gone and checkout_gone and private_gone and strays_gone and not still_registered


def classify(status):
    return {0: "passed", 1: "failed", 3: "unavailable", DISK_FLOOR: "unavailable", 124: "deadline",
            130: "cancelled", 143: "cancelled"}.get(status, "unavailable")


def verify_source(args, mirror, cancel):
    status, bare = git(cancel, f"--git-dir={mirror}", "rev-parse", "--is-bare-repository", check=False)
    if status != 0 or bare.strip() != b"true":
        raise Exit(UNAVAILABLE, "mirror_unavailable")
    status, kind = git(cancel, f"--git-dir={mirror}", "cat-file", "-t", args["commit"], check=False)
    if status != 0 or kind.strip() != b"commit":
        raise Exit(UNAVAILABLE, "commit_unavailable")
    _, listing = git(cancel, f"--git-dir={mirror}", "ls-tree", "-z", args["commit"], "--", args["script"])
    entries = [entry for entry in listing.split(b"\0") if entry]
    if len(entries) != 1:
        raise Exit(UNAVAILABLE, "script_unavailable")
    meta, _, name = entries[0].partition(b"\t")
    mode, kind, _ = meta.split(b" ")
    if mode not in (b"100644", b"100755") or kind != b"blob" or name != args["script"].encode():
        raise Exit(UNAVAILABLE, "script_unavailable")


def launch(argv, cancel):
    args = parse(argv)
    raw_mirror = os.environ.get(MIRROR_ENV, "")
    if not raw_mirror or not os.path.isabs(raw_mirror) or not os.path.isdir(raw_mirror):
        raise Exit(UNAVAILABLE, "mirror_unavailable")
    mirror = os.path.realpath(raw_mirror)
    root = private_root(SANDBOX_ROOT_ENV)
    toolchain = toolchain_bin()
    userns = args["check"] in userns_checks()
    handoff = handoff_python(args["check"])
    disk = DiskFloor(root)
    scope = resource_scope(args["attempt"])
    if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise Exit(UNAVAILABLE, "subreaper_unavailable")
    verify_source(args, mirror, cancel)
    # Admission before any sandbox exists: a refused check leaves nothing behind.
    disk_used = disk.admit("before provisioning")
    cancel.check()
    sandbox = Sandbox(args, mirror, root, cancel, disk, scope, toolchain, userns, handoff)
    sandbox.state["disk_used_pct"] = disk_used
    # The verifier fences under the same lock, so after it fences an attempt either
    # this attempt directory (and its held lock) already exists or it never will.
    with Lock(root / ".provision.lock", cancel):
        if os.path.lexists(root / "fenced" / args["attempt"]):
            raise Exit(UNAVAILABLE, "attempt_fenced")
        try:
            sandbox.directory.mkdir(mode=0o700)
        except FileExistsError:
            raise Exit(UNAVAILABLE, "attempt_reused") from None
        # Held (close-on-exec, so never by the check) until this process exits.
        attempt_lock = os.open(sandbox.directory / "lock", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
        fcntl.flock(attempt_lock, fcntl.LOCK_EX)
        sync_directory(root)
    status, reason = None, None
    try:
        status = sandbox.run()
        reason = "check_exited"
    except Exit as error:
        status, reason = error.code, error.reason
    except Cancelled as error:
        status, reason = error.code, f"cancelled_{error}"
    except (OSError, subprocess.SubprocessError):
        status, reason = UNAVAILABLE, "launch_error"
    finally:
        complete = False
        try:
            complete = sandbox.teardown()
        except (Exit, OSError, subprocess.SubprocessError) as error:
            log(f"teardown error: {getattr(error, 'reason', type(error).__name__)}")
        finally:
            # A failure before a status was decided (for example in teardown
            # itself) reports the attempt as unavailable.
            if status is None:
                status, reason = UNAVAILABLE, "teardown_error"
            if cancel.pending() is not None and status not in (130, 143):
                # A cancellation that arrived before the result was recorded wins.
                status, reason = Cancelled(cancel.signals[0]).code, f"cancelled_{signal.Signals(cancel.signals[0]).name}"
            sandbox.state.update(status=classify(status), exit_status=status, reason=reason,
                                 finished_at=int(time.time()), teardown="complete" if complete else "incomplete")
            sandbox.save()
    if not complete:
        log("sandbox teardown incomplete; reconcile before retrying this attempt")
    return status


libc = ctypes.CDLL(None, use_errno=True)


def main(argv):
    os.umask(0o077)
    cancel = Cancellation()
    try:
        return launch(argv, cancel)
    except Exit as error:
        log(error.reason)
        return error.code
    except Cancelled as error:
        log(f"cancelled before provisioning ({error})")
        return error.code
    except (OSError, subprocess.SubprocessError) as error:
        log(f"unavailable: {type(error).__name__}")
        return UNAVAILABLE


sys.exit(main(sys.argv[1:]))
PY
