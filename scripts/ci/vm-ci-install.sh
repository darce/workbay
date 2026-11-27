#!/bin/bash -p
# Install a trusted, immutable copy of the VM CI controller from an exact commit:
#   vm-ci-install.sh --mirror PATH --commit SHA --prefix DIR --sandbox-root DIR \
#       --store DIR --state-dir DIR --repository OWNER/REPO \
#       [--toolchain DIR] [--userns-checks LIST] [--handoff-python PATH] \
#       [--interval-seconds N] [--no-systemd]
#
# Site values (unit names, variable prefix, display name) come from the site
# configuration: scripts/ci/vm_ci_site.py beside this script reads
# config/vm-ci/site.json (or $VMCI_SITE_CONFIG). They are resolved once, below,
# before anything else runs; a missing loader or a missing or invalid config is
# refused with one line (exit 3). <P> below is the site's ci_env_prefix
# (ACX_VM_CI_ for this repository's deployment), <UNIT> its poll_unit
# (acx-vm-ci-poll here).
#
# Layout under --prefix (all paths are canonical and absolute):
#   releases/<sha>/scripts/ci/...  read-only `git archive` export of the router,
#                                  routing policy, launcher, sandbox verifier,
#                                  poller and site loader at <sha>, verified
#                                  against the tree
#   releases/<sha>/config/vm-ci/site.json
#                                  the site config at <sha>, where the installed
#                                  loader resolves it; it must resolve to the
#                                  same values as this installer's (else exit 3)
#   current -> releases/<sha>      switched atomically by a symlink rename
#   bin/launcher                   exports <P>MIRROR and <P>SANDBOX_ROOT
#   bin/sandbox-verifier           and execs the installed copy (the router scrubs
#                                  its environment, so configuration lives here);
#                                  bin/launcher also exports <P>TOOLCHAIN
#                                  (--toolchain, else unset) and
#                                  <P>USERNS_CHECKS (--userns-checks) and
#                                  <P>HANDOFF_PYTHON (--handoff-python,
#                                  else unset)
#   bin/poll                       runs the installed poller with installed paths
#
# The wrappers and units hold values rendered at install time; the installed
# scripts read the installed loader and config, never built-in defaults.
# The sandbox root, evidence store and state dir are created with mode 0700.
# Unless --no-systemd is given, a systemd user service and timer (<UNIT>.service,
# <UNIT>.timer) are rendered into ${XDG_CONFIG_HOME:-~/.config}/systemd/user; the
# enable command is printed, never run. The switch waits for no poll: if a poll
# holds state-dir/poll.lock the release is staged and verified but `current` is
# not switched (exit 3; rerun).
# Reinstalling the same commit is idempotent and re-verifies the installed bytes.
#
# --toolchain DIR is the directory scripts/ci/vm-ci-toolchain.sh provisions; the
# launcher puts DIR/bin first on the check PATH and reports unavailable until it
# exists. It is exported as given (normalized), not resolved, so a provisioner
# that switches a symlink is picked up; the launcher resolves it per attempt.
# --userns-checks LIST (default snapshot-checker; '' for none) names the checks
# run in a user namespace with this account's subordinate IDs mapped. That needs
# the setuid newuidmap/newgidmap helpers, so when LIST is non-empty the service
# unit omits NoNewPrivileges=yes; the launcher sets no_new_privs on every check
# process itself, after the namespace mapping.
# --handoff-python PATH is the WorkBay virtualenv interpreter for the
# handoff-integrity check, for example ~/.local/share/uv/tools/workbay/bin/python.
# It must be an absolute executable whose `-c 'import sys;
# print(sys.version_info[:2])'` reports (3, 12). It is exported as given
# (normalized), not resolved, because a virtualenv is recognized from the path
# its interpreter is invoked through. The launcher passes it to that check only
# (as ACX_CI_HANDOFF_PYTHON in this repository's launcher). Without it the check
# stays unavailable.
# Never uses sudo.
#
# Exit status: 0 installed, 2 usage, 3 refused (missing or invalid site config,
# unknown commit, unsafe or tampered layout, unusable --handoff-python,
# concurrent install or running poll).
# bash -p ignores BASH_ENV, ENV, SHELLOPTS and exported functions, so nothing
# from the caller runs before the exec below.
set -euo pipefail
refuse() { printf 'vm-ci-install: refused: %s\n' "$1" >&2; exit 3; }
self=$(/usr/bin/realpath -e -- "${BASH_SOURCE[0]}" 2>/dev/null) || refuse "cannot resolve the installer's own path"
loader=${self%/*}/vm_ci_site.py
[[ -f $loader ]] || refuse "site loader $loader not found; it must sit beside this installer"
# Only the loader's own output may define VMCI_SITE_* (VMCI_SITE_CONFIG chooses the file).
for name in ${!VMCI_SITE_@}; do [[ $name == VMCI_SITE_CONFIG ]] || unset -v "$name"; done
site=$(/usr/bin/python3 -I -B "$loader" shell 2>&1) || refuse "${site//$'\n'/ }"
while IFS= read -r line; do
    [[ $line =~ ^export\ VMCI_SITE_[A-Z0-9_]+= ]] || refuse "site loader $loader printed an unexpected line"
done <<<"$site"
eval "$site"
unset -v self name site line
exec /usr/bin/python3 -I -B - "$loader" "$@" <<'PY'
import argparse
import fcntl
import hashlib
import io
import json
import os
import re
import secrets
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

USAGE, REFUSED = 2, 3
GIT_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": "/nonexistent",
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0"}
GIT = ("/usr/bin/git", "-c", "core.hooksPath=/dev/null", "-c", "protocol.allow=never")
GIT_TIMEOUT = 120
ROUTER = "scripts/ci/run-vm-check.sh"
POLICY = "scripts/ci/vm-check-policy.json"
LAUNCHER = "scripts/ci/vm-check-launcher.sh"
VERIFIER = "scripts/ci/vm-check-sandbox-verifier.sh"
POLLER = "scripts/ci/vm-ci-poll.py"
# The installed scripts load the site from the release itself: the loader beside
# them resolves config/vm-ci/site.json two levels above its own directory.
SITE_LOADER = "scripts/ci/vm_ci_site.py"
SITE_CONFIG = "config/vm-ci/site.json"
# Repository path -> tree modes accepted for it. Anything exec'd directly must be
# executable in the tree; the policy, the poller (run via python3), the site loader
# and its config may be either.
FILES = {ROUTER: ("100755",), POLICY: ("100644", "100755"), LAUNCHER: ("100755",),
         VERIFIER: ("100755",), POLLER: ("100644", "100755"), SITE_LOADER: ("100644", "100755"),
         SITE_CONFIG: ("100644", "100755")}
DIRECTORIES = ("scripts", "scripts/ci", "config", "config/vm-ci")
SHA = r"(?!0{40})[0-9a-f]{40}"
CHECK = r"[a-z0-9][a-z0-9-]{0,63}"
USERNS_CHECKS = rf"(?:{CHECK}(?:,{CHECK})*)?"
DEFAULT_USERNS_CHECKS = "snapshot-checker"
REPOSITORY = r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"
# Paths end up in shell wrappers and systemd units; keep them to a plain charset.
SAFE_PATH = r"/(?:[A-Za-z0-9._+-]+/?)*"
HANDOFF_VERSION_TIMEOUT = 10
HANDOFF_VERSION = b"(3, 12)\n"
SITE_TIMEOUT = 60

# The site, as the bash prologue resolved it through the loader's shell mode. The
# variables are consumed here, so no child process inherits them.
LOADER = sys.argv[1]
SITE_PREFIX, SITE_CONFIG_VARIABLE = "VMCI_SITE_", "VMCI_SITE_CONFIG"
SITE = {name[len(SITE_PREFIX):].lower(): os.environ.pop(name) for name in sorted(os.environ)
        if name.startswith(SITE_PREFIX) and name != SITE_CONFIG_VARIABLE}
SITE_KEYS = ("display_name", "ci_env_prefix", "poll_service", "poll_timer")
if not all(SITE.get(key) for key in SITE_KEYS):
    print(f"vm-ci-install: refused: site loader {LOADER} did not resolve "
          f"{next(key for key in SITE_KEYS if not SITE.get(key))}", file=sys.stderr)
    sys.exit(REFUSED)
DISPLAY_NAME = SITE["display_name"]
ENV_PREFIX = SITE["ci_env_prefix"]
SERVICE, TIMER = SITE["poll_service"], SITE["poll_timer"]
ENABLE = f"systemctl --user daemon-reload && systemctl --user enable --now {TIMER}"


class Refused(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Refused(reason)


def say(message):
    print(f"vm-ci-install: {message}", flush=True)


def git(mirror, *args, check=True):
    try:
        result = subprocess.run((*GIT, f"--git-dir={mirror}", *args), env=GIT_ENV, cwd="/",
                                stdin=subprocess.DEVNULL, capture_output=True, timeout=GIT_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise Refused(f"git {args[0]} timed out") from None
    if check and result.returncode != 0:
        raise Refused(f"git {args[0]} failed: {result.stderr.decode(errors='replace').strip()[-500:]}")
    return result


def parse(argv):
    parser = argparse.ArgumentParser(prog="vm-ci-install.sh", description="Install the VM CI controller.")
    for flag in ("mirror", "prefix", "sandbox-root", "store", "state-dir"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--commit", required=True, help="full 40-digit commit ID")
    parser.add_argument("--repository", required=True, help="OWNER/REPO")
    parser.add_argument("--toolchain", help="provisioned toolchain directory (DIR/bin leads the check PATH)")
    parser.add_argument("--userns-checks", default=DEFAULT_USERNS_CHECKS,
                        help="comma-separated check ids run in a user namespace ('' for none)")
    parser.add_argument("--handoff-python",
                        help="WorkBay virtualenv Python 3.12 for the handoff-integrity check")
    parser.add_argument("--interval-seconds", type=int, default=120)
    parser.add_argument("--no-systemd", action="store_true")
    args = parser.parse_args(argv)
    if re.fullmatch(SHA, args.commit) is None:
        parser.error("--commit must be a full lowercase 40-digit commit ID")
    if re.fullmatch(REPOSITORY, args.repository) is None:
        parser.error("--repository must be OWNER/REPO")
    if not 30 <= args.interval_seconds <= 86400:
        parser.error("--interval-seconds must be between 30 and 86400")
    for flag in ("mirror", "prefix", "sandbox_root", "store", "state_dir"):
        value = getattr(args, flag)
        if re.fullmatch(SAFE_PATH, value) is None or "/../" in value + "/" or "/./" in value + "/":
            parser.error(f"--{flag.replace('_', '-')} must be an absolute path of [A-Za-z0-9._+-] components")
        setattr(args, flag, Path(os.path.realpath(value)))
    for flag in ("toolchain", "handoff_python"):
        value = getattr(args, flag)
        if value is None:
            continue
        option = f"--{flag.replace('_', '-')}"
        if re.fullmatch(SAFE_PATH, value) is None or "/../" in value + "/" or "/./" in value + "/":
            parser.error(f"{option} must be an absolute path of [A-Za-z0-9._+-] components")
        setattr(args, flag, Path(os.path.normpath(value)))
        if getattr(args, flag) == Path("/"):
            parser.error(f"{option} must not be /")
    if re.fullmatch(USERNS_CHECKS, args.userns_checks) is None:
        parser.error("--userns-checks must be comma-separated check ids such as snapshot-checker")
    args.userns_checks = ",".join(dict.fromkeys(filter(None, args.userns_checks.split(","))))
    return args


def check_layout(args):
    """The five locations must be distinct and must not nest inside each other."""
    places = {"--mirror": args.mirror, "--prefix": args.prefix, "--sandbox-root": args.sandbox_root,
              "--store": args.store, "--state-dir": args.state_dir}
    for name, path in places.items():
        for other, other_path in places.items():
            require(name == other or not path.is_relative_to(other_path),
                    f"{name} {path} must not be inside or equal to {other} {other_path}")
    # The toolchain and the WorkBay interpreter may live in their own directory
    # under the prefix, but never in the installer-managed entries, the candidate
    # sandbox or controller state. Both the given and the resolved path count.
    for option, value in (("--toolchain", args.toolchain), ("--handoff-python", args.handoff_python)):
        if value is None:
            continue
        for trusted in dict.fromkeys((value, Path(os.path.realpath(value)))):
            for name, path in places.items():
                require(not path.is_relative_to(trusted),
                        f"{name} {path} must not be inside or equal to {option} {trusted}")
                if name == "--prefix" and trusted.is_relative_to(path):
                    top = trusted.relative_to(path).parts[0]
                    require(top not in ("releases", "bin", "current") and not top.startswith("."),
                            f"{option} {trusted} must not be inside {path / top}")
                else:
                    require(not trusted.is_relative_to(path),
                            f"{option} {trusted} must not be inside {name} {path}")


def check_handoff_python(args):
    """The interpreter must be an executable file that reports Python 3.12."""
    python = args.handoff_python
    if python is None:
        return
    require(python.is_file() and os.access(python, os.X_OK),
            f"--handoff-python {python} must be an executable file")
    # The launcher refuses an interpreter or bin directory others can replace.
    for path in (python, python.parent):
        info = path.stat()
        require(info.st_uid in (0, os.geteuid()) and not info.st_mode & 0o022,
                f"--handoff-python {path} must be owned by root or this user and not group- or world-writable")
    try:
        result = subprocess.run((str(python), "-I", "-c", "import sys; print(sys.version_info[:2])"),
                                env=GIT_ENV, cwd="/", stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=HANDOFF_VERSION_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise Refused(f"--handoff-python {python} did not report its version in time") from None
    require(result.returncode == 0 and result.stdout == HANDOFF_VERSION,
            f"--handoff-python {python} must report (3, 12), got "
            f"{result.stdout.decode(errors='replace').strip()[:80]!r} (exit {result.returncode})")


def owned_directory(path, private):
    """Create path (and parents) if needed and return its verified lstat."""
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid(),
            f"{path} must be a directory owned by this user")
    if private and info.st_mode & 0o077:
        os.chmod(path, 0o700)
    elif not private:
        require(not info.st_mode & 0o022, f"{path} must not be group- or world-writable")
    return path


def sync_directory(path):
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def make_writable(root):
    for directory, _, _ in os.walk(root):
        os.chmod(directory, 0o700)


def remove_tree(path):
    make_writable(path)
    shutil.rmtree(path)


def verify_source(args):
    bare = git(args.mirror, "rev-parse", "--is-bare-repository", check=False)
    require(bare.returncode == 0 and bare.stdout.strip() == b"true", f"--mirror is not a bare repository: {args.mirror}")
    kind = git(args.mirror, "cat-file", "-t", args.commit, check=False)
    require(kind.returncode == 0 and kind.stdout.strip() == b"commit",
            f"commit {args.commit} is not in the mirror")
    listing = git(args.mirror, "ls-tree", "-z", "--full-tree", args.commit, "--", *FILES).stdout
    blobs = {}
    for entry in filter(None, listing.split(b"\0")):
        meta, _, name = entry.decode().partition("\t")
        mode, kind, object_id = meta.split(" ")
        require(name in FILES and name not in blobs and kind == "blob" and mode in FILES[name],
                f"{name} at {args.commit} must be a regular file with mode {'/'.join(FILES.get(name, ()))}")
        blobs[name] = (mode, object_id)
    missing = sorted(set(FILES) - set(blobs))
    require(not missing, f"commit {args.commit} lacks {', '.join(missing)}")
    return blobs


def export(args, blobs):
    """Return {path: (mode, bytes)} from `git archive`, verified against the tree."""
    archive = git(args.mirror, "archive", "--format=tar", args.commit, "--", *FILES).stdout
    files = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
        for member in tar.getmembers():
            name = member.name.rstrip("/")
            if member.isdir() and name in DIRECTORIES:
                continue
            require(member.isreg() and name in FILES and name not in files, f"unexpected archive entry {name}")
            content = tar.extractfile(member).read()
            object_id = hashlib.sha1(b"blob %d\0" % len(content) + content).hexdigest()
            require(object_id == blobs[name][1], f"archive bytes of {name} do not match the tree")
            files[name] = (0o555 if blobs[name][0] == "100755" else 0o444, content)
    require(set(files) == set(FILES), "archive is incomplete")
    return files


def check_release_site(args, files):
    """The release's site config must resolve to this installer's site.

    The wrappers and units rendered here and the scripts installed from the commit
    must name one site; the config is resolved by this installer's loader (fed on
    stdin, so nothing is written) before anything changes."""
    try:
        result = subprocess.run(("/usr/bin/python3", "-I", "-B", LOADER, "--config", "/dev/stdin", "json"),
                                input=files[SITE_CONFIG][1], env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
                                cwd="/", capture_output=True, timeout=SITE_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise Refused(f"the site loader did not check {SITE_CONFIG} at {args.commit} in time") from None
    detail = result.stderr.decode(errors="replace").strip().removeprefix("vm-ci-site: ")
    require(result.returncode == 0, f"{SITE_CONFIG} at {args.commit} is invalid: "
            + detail.replace("'/dev/stdin'", repr(SITE_CONFIG))[-500:])
    release = json.loads(result.stdout)
    for key in sorted(release.keys() | SITE.keys()):
        require(release.get(key) == SITE.get(key),
                f"{SITE_CONFIG} at {args.commit} resolves {key} to {release.get(key)!r}, not this "
                f"installer's {SITE.get(key)!r}; install with the site config of that commit")


def expected_snapshot(files):
    snapshot = {directory: ("dir", 0o555, None) for directory in DIRECTORIES}
    snapshot.update({name: ("file", mode, content) for name, (mode, content) in files.items()})
    return snapshot


def snapshot(root):
    found = {}
    for directory, subdirectories, names in os.walk(root):
        for name in subdirectories + names:
            path = Path(directory, name)
            info = path.lstat()
            relative = str(path.relative_to(root))
            if stat.S_ISDIR(info.st_mode):
                found[relative] = ("dir", stat.S_IMODE(info.st_mode), None)
            elif stat.S_ISREG(info.st_mode):
                found[relative] = ("file", stat.S_IMODE(info.st_mode), path.read_bytes())
            else:
                found[relative] = ("other", stat.S_IMODE(info.st_mode), None)
    return found


def stage(releases, files):
    staging = releases / f".staging-{secrets.token_hex(8)}"
    staging.mkdir(mode=0o700)
    for directory in DIRECTORIES:
        (staging / directory).mkdir(mode=0o700)
    for name, (mode, content) in files.items():
        fd = os.open(staging / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        with os.fdopen(fd, "wb") as target:
            target.write(content)
            target.flush()
            os.fsync(target.fileno())
        os.chmod(staging / name, mode)
    for directory in sorted(DIRECTORIES, reverse=True):
        sync_directory(staging / directory)
        os.chmod(staging / directory, 0o555)
    sync_directory(staging)
    os.chmod(staging, 0o555)
    return staging


def install_release(args, files):
    releases = owned_directory(args.prefix / "releases", private=False)
    for leftover in releases.glob(".staging-*"):
        remove_tree(leftover)  # An install interrupted before its rename.
    release = releases / args.commit
    expected = expected_snapshot(files)
    if os.path.lexists(release):
        info = release.lstat()
        require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o555
                and snapshot(release) == expected,
                f"{release} differs from {args.commit}; move it aside before reinstalling")
        say(f"release {args.commit} already installed and verified")
        return release, False
    staging = stage(releases, files)
    try:
        require(snapshot(staging) == expected, "staged release does not match the export")
        os.rename(staging, release)
    except BaseException:
        if staging.exists():
            remove_tree(staging)
        raise
    sync_directory(releases)
    say(f"release {args.commit} installed read-only at {release}")
    return release, True


def write_file(path, content, mode):
    """Atomically replace path with content and mode; return whether it changed."""
    try:
        info = path.lstat()
        if stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == mode and path.read_bytes() == content:
            return False
    except FileNotFoundError:
        pass
    temp = path.with_name(f".{path.name}.{secrets.token_hex(8)}")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        with os.fdopen(fd, "wb") as target:
            target.write(content)
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temp, mode)
        os.replace(temp, path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    sync_directory(path.parent)
    return True


def wrappers(args):
    q = lambda path: shlex.quote(str(path))
    current, bin_dir = args.prefix / "current", args.prefix / "bin"
    header = "#!/bin/bash -p\n# Generated by scripts/ci/vm-ci-install.sh; reinstall instead of editing.\n"
    p = ENV_PREFIX
    env = (f"export {p}MIRROR={q(args.mirror)}\n"
           f"export {p}SANDBOX_ROOT={q(args.sandbox_root)}\n")
    toolchain = (f"unset {p}TOOLCHAIN\n" if args.toolchain is None
                 else f"export {p}TOOLCHAIN={q(args.toolchain)}\n")
    handoff = (f"unset {p}HANDOFF_PYTHON\n" if args.handoff_python is None
               else f"export {p}HANDOFF_PYTHON={q(args.handoff_python)}\n")
    launcher_env = f"{env}{toolchain}export {p}USERNS_CHECKS={q(args.userns_checks)}\n{handoff}"
    poll = [
        "/usr/bin/python3", "-I", "-B", current / POLLER,
        "--mirror", args.mirror, "--repository", args.repository,
        "--state-dir", args.state_dir, "--store", args.store,
        "--router", current / ROUTER, "--launcher", bin_dir / "launcher",
        "--sandbox-verifier", bin_dir / "sandbox-verifier",
    ]
    return {
        "launcher": f"{header}{launcher_env}exec {q(current / LAUNCHER)} \"$@\"\n",
        "sandbox-verifier": f"{header}{env}exec {q(current / VERIFIER)} \"$@\"\n",
        "poll": f"{header}umask 077\nexec {' '.join(q(part) for part in poll)} \"$@\"\n",
    }


def switch_current(args):
    current = args.prefix / "current"
    target = f"releases/{args.commit}"
    if os.path.lexists(current):
        require(current.is_symlink(), f"{current} exists and is not a symlink")
        if os.readlink(current) == target:
            return False
    temp = args.prefix / f".current-{secrets.token_hex(8)}"
    os.symlink(target, temp)
    try:
        os.replace(temp, current)  # rename(2) swaps the link in one step.
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    sync_directory(args.prefix)
    return True


def units(args):
    poll = args.prefix / "bin" / "poll"
    if args.userns_checks:
        privileges = (f"# No NoNewPrivileges=: user-namespaced checks ({args.userns_checks}) need the setuid\n"
                      "# newuidmap/newgidmap helpers. The launcher sets no_new_privs on every check itself.\n")
    else:
        privileges = "NoNewPrivileges=yes\n"
    service = f"""# Generated by scripts/ci/vm-ci-install.sh for {args.repository}; reinstall instead of editing.
[Unit]
Description={DISPLAY_NAME} VM CI poller for {args.repository}

[Service]
Type=oneshot
ExecStart={poll}
UMask=0077
{privileges}# Stop only the poller first: it forwards SIGTERM to the router, which gives the
# launcher 30 s to tear the sandbox down. The rest of the group is killed after.
KillMode=mixed
TimeoutStopSec=120
"""
    timer = f"""# Generated by scripts/ci/vm-ci-install.sh for {args.repository}; reinstall instead of editing.
[Unit]
Description=Run the {DISPLAY_NAME} VM CI poller every {args.interval_seconds} s

[Timer]
OnActiveSec=30
OnUnitActiveSec={args.interval_seconds}
AccuracySec=5
Unit={SERVICE}

[Install]
WantedBy=timers.target
"""
    return {SERVICE: service, TIMER: timer}


def unit_directory():
    config = os.environ.get("XDG_CONFIG_HOME", "")
    if not config.startswith("/"):
        home = os.environ.get("HOME", "")
        require(home.startswith("/"), "HOME must be absolute to render systemd user units")
        config = os.path.join(home, ".config")
    return Path(config, "systemd", "user")


def acquire(path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


def install(args):
    check_layout(args)
    check_handoff_python(args)
    blobs = verify_source(args)
    files = export(args, blobs)
    check_release_site(args, files)
    owned_directory(args.prefix, private=False)
    for path in (args.sandbox_root, args.store, args.state_dir):
        owned_directory(path, private=True)
    install_lock = acquire(args.prefix / ".install.lock")
    require(install_lock is not None, "another install is running for this prefix")
    try:
        release, _ = install_release(args, files)
        # Polls run the router and launcher under state-dir/poll.lock; switching
        # only while holding it keeps a poll on one release from start to end.
        poll_lock = acquire(args.state_dir / "poll.lock")
        require(poll_lock is not None,
                f"a poll is running; release {args.commit} is staged, rerun to switch current")
        try:
            bin_dir = owned_directory(args.prefix / "bin", private=False)
            for name, content in wrappers(args).items():
                if write_file(bin_dir / name, content.encode(), 0o555):
                    say(f"wrote {bin_dir / name}")
            if switch_current(args):
                say(f"current -> {release}")
            else:
                say(f"current already -> {release}")
        finally:
            os.close(poll_lock)
    finally:
        os.close(install_lock)
    if args.no_systemd:
        say(f"skipped systemd units; run {args.prefix / 'bin' / 'poll'} from your own scheduler")
        return 0
    directory = unit_directory()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name, content in units(args).items():
        if write_file(directory / name, content.encode(), 0o644):
            say(f"wrote {directory / name}")
    say("enable the poller (not run by this installer):")
    print(ENABLE, flush=True)
    return 0


def main(argv):
    os.umask(0o077)
    args = parse(argv)
    try:
        return install(args)
    except Refused as error:
        print(f"vm-ci-install: refused: {error}", file=sys.stderr)
    except (OSError, ValueError, tarfile.TarError) as error:
        print(f"vm-ci-install: refused: {type(error).__name__}: {error}", file=sys.stderr)
    return REFUSED


sys.exit(main(sys.argv[2:]))
PY
