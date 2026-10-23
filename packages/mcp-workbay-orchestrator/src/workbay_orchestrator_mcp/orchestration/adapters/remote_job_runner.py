"""Submit, monitor, and collect detached remote-agent jobs."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import sleep as _sleep
from typing import Any

from workbay_orchestrator_mcp.orchestration.lane_lifecycle_contracts import (
    ContractError,
    RemoteJobIdentity,
)

_HOST_ONLY_ENV_KEYS = (
    "WORKBAY_REMOTE_JOB_TASK_REF",
    "WORKBAY_REMOTE_JOB_LANE_ID",
    "WORKBAY_REMOTE_JOB_PASS_ID",
    "WORKBAY_REMOTE_JOB_ATTEMPT",
)
_POLL_INITIAL_SECONDS = 5.0
_POLL_MAX_SECONDS = 30.0
_CANCEL_RESERVE_MAX_SECONDS = 10.0
_CANCEL_RESERVE_FRACTION = 0.25
_TRANSPORT_MAX_BYTES = 8 * 1024 * 1024
_SNAPSHOT_EXCLUDE_RULE = ".workbay-transport-*.sh"
_GIT_PROBE_TIMEOUT_SECONDS = 5.0


def _identity(env: dict[str, str] | None) -> RemoteJobIdentity | ContractError:
    try:
        return RemoteJobIdentity.from_env(env)
    except ContractError as exc:
        return exc


def _refusal(cmd: list[str], reason: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(cmd, 2, "", reason)


def _child_env(env: dict[str, str] | None) -> dict[str, str]:
    result = dict(os.environ if env is None else env)
    for key in _HOST_ONLY_ENV_KEYS:
        result.pop(key, None)
    return result


class _SelectedTransportReadError(OSError):
    """A selected source cannot provide bounded, executable regular bytes."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class _TransportBindingRefused(Exception):
    """Authenticated source could not be privately owned before submission."""


class _BufferedTransportSource(type(Path())):
    """Selected-path inspection view backed only by an opened, bounded buffer.

    Keep the Path text/digest inspection consumers and original path identity,
    but their reads cannot reopen the mutable selected name.
    """

    def __new__(cls, path, data):
        return super().__new__(cls, path)

    def __init__(self, path, data):
        super().__init__(path)
        self.data = data

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if mode not in {"r", "rb"}:
            raise ValueError("selected transport buffer is read-only")
        stream = io.BytesIO(self.data)
        if mode == "rb":
            return stream
        return io.TextIOWrapper(stream, encoding=encoding, errors=errors, newline=newline)


def _read_selected_transport(path):
    """Reject special files/redirects before reading the same finite FD buffer.

    [RES-02] **Timeout on every blocking call** (Release It!, ch. 5): a
    FIFO open must never wait for a writer before we can check the deadline.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    except OSError as exc:
        raise _SelectedTransportReadError(f"selected_source_unreadable:{type(exc).__name__}") from exc
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o111:
            raise _SelectedTransportReadError("selected_source_type_or_mode")
        if info.st_size > _TRANSPORT_MAX_BYTES:
            raise _SelectedTransportReadError("selected_source_oversized")
        data = stream.read(_TRANSPORT_MAX_BYTES + 1)
    if len(data) > _TRANSPORT_MAX_BYTES:
        raise _SelectedTransportReadError("selected_source_oversized")
    return _BufferedTransportSource(path, data), info


class _AuthenticatedTransportCommand(list[str]):
    """Original argv plus a single-owner, read-only native executable copy.

    All native launch routes consume the copy. The durable intent retains the original
    argv, so random snapshot names never change the idempotent job identity.
    """

    def __init__(self, command, snapshot, receipt, *, transport_owner=None):
        super().__init__(command)
        self.snapshot = snapshot
        self.receipt = receipt
        self.consumed = False
        self.cleanup_diagnostic = None
        self.deadline = None
        self.closed = False
        self.transport_owner = transport_owner if transport_owner is not None else self
        # None preserves the legacy, unreported context-exit contract. Upgraded
        # runners must report clean/unknown explicitly; an unknown is sticky.
        self.consumer_lifetime = None
        self.consumer_reports = []
        self.consumers_dropped = 0
        self.open_borrowers = 0
        self.snapshot_released = False
        self.snapshot_exclusion = None


class _SnapshotConsumerArgv(list[str]):
    """Native argv retaining the carrier that owns its consumer-lifetime receipt."""

    def __init__(self, command, carrier, *, deadline=None):
        super().__init__(command)
        self.carrier = carrier
        self.deadline = deadline


def _consumer_carrier(command):
    if isinstance(command, _AuthenticatedTransportCommand):
        return command
    if isinstance(command, _SnapshotConsumerArgv):
        return command.carrier
    return None


def _record_consumer_lifetime(command, state, evidence=None):
    """Report ``clean`` or sticky ``unknown`` to this carrier and its root owner.

    EXEC must call this on every cleanup-receipt path, including before a
    refusal is raised. Plain argv has no owned snapshot and is a no-op. Clean
    means the caller has positively confirmed *all* of its native consumers
    gone, regardless of exit status. A later clean never erases an earlier
    unknown on the same snapshot; reconciliation uses the retained evidence.
    No wait, registry, lease or database transaction is involved [CON-18].
    """
    if state not in ("clean", "unknown"):
        raise ValueError("consumer lifetime must be clean or unknown")
    carrier = _consumer_carrier(command)
    if carrier is None:
        return
    owner = carrier.transport_owner
    if owner.snapshot_released:
        raise ValueError("consumer lifetime report arrived after snapshot release")
    for target in (carrier,) if carrier is owner else (carrier, owner):
        if target.consumer_lifetime != "unknown":
            target.consumer_lifetime = state
        if state == "unknown":
            # Fixed keys and primitive, copied values keep refusal evidence
            # bounded and immune to later mutation by its caller [FM-02].
            report = {"consumer_lifetime": state}
            for key in ("source", "reason", "detail", "operation", "pid", "pgid", "owner_pid"):
                value = evidence.get(key) if isinstance(evidence, dict) else None
                if type(value) is bool or (type(value) is int and value.bit_length() <= 64):
                    report[key] = value
                elif isinstance(value, str):
                    report[key] = value[:240]
            if len(target.consumer_reports) < 8:
                target.consumer_reports.append(report)
            else:
                target.consumers_dropped += 1
    if owner.closed:
        _finish_transport_owner(owner)


def _finish_transport_owner(owner):
    """Release only a closed owner with no open borrower or unknown consumer."""
    if owner.consumer_lifetime == "unknown" or owner.open_borrowers:
        diagnostic = {
            "reason": "transport_snapshot_retained",
            "consumer_lifetime": "unknown",
            **owner.receipt,
            "open_borrowers": owner.open_borrowers,
            "consumers": [dict(report) for report in owner.consumer_reports],
            "consumers_dropped": owner.consumers_dropped,
            "retryable": True,
        }
        if owner.snapshot_exclusion is not None:
            diagnostic["snapshot_exclusion"] = owner.snapshot_exclusion
        owner.cleanup_diagnostic = diagnostic
        return
    if not owner.closed or owner.snapshot_released:
        return
    try:
        info = owner.snapshot.lstat()
        if (info.st_dev, info.st_ino) != (owner.receipt["device"], owner.receipt["inode"]):
            raise OSError("snapshot identity changed")
        owner.snapshot.unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        diagnostic = {
            "reason": "transport_snapshot_cleanup_failed",
            "error_type": type(exc).__name__,
            "transport_path": str(owner.snapshot),
            "retryable": True,
        }
        if owner.snapshot_exclusion is not None:
            diagnostic["snapshot_exclusion"] = owner.snapshot_exclusion
        owner.cleanup_diagnostic = diagnostic
        return
    owner.snapshot_released = True
    owner.cleanup_diagnostic = owner.snapshot_exclusion


def _execution_command(command):
    """Keep public argv stable while the native child opens the owned inode."""
    if isinstance(command, _AuthenticatedTransportCommand):
        command.consumed = True
        return _SnapshotConsumerArgv([str(command.snapshot), *command[1:]], command)
    return command


def _exclude_diagnostic(directory: Path, error_type: str) -> dict[str, Any]:
    return {
        "reason": "transport_snapshot_exclude_unavailable",
        "error_type": error_type,
        "source_dir": str(directory),
        "retryable": True,
    }


def _ignore_private_snapshots(directory: Path) -> dict[str, Any] | None:
    """Keep same-directory snapshots out of ``git status`` and ``git add -A``.

    A directory outside every work tree needs nothing. A failed git probe is
    returned as a loud diagnostic and never refuses the run [AGT-10][RES-02].
    """
    try:
        probe = subprocess.run(
            ["git", "-C", str(directory), "rev-parse", "--is-inside-work-tree", "--git-path", "info/exclude"],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return _exclude_diagnostic(directory, type(exc).__name__)
    if probe.returncode != 0:
        if "not a git repository" in probe.stderr.lower():
            return None
        return _exclude_diagnostic(directory, f"git_rc_{probe.returncode}")
    inside, _, relative = probe.stdout.partition("\n")
    if inside != "true":
        return None
    exclude = directory / relative.strip()
    try:
        try:
            existing = exclude.read_text(encoding="utf-8")
        except FileNotFoundError:
            existing = ""
        if _SNAPSHOT_EXCLUDE_RULE not in (line.strip() for line in existing.splitlines()):
            with exclude.open("a", encoding="utf-8") as stream:
                if existing and not existing.endswith("\n"):
                    stream.write("\n")
                stream.write(f"{_SNAPSHOT_EXCLUDE_RULE}\n")
    except OSError as exc:
        return _exclude_diagnostic(directory, type(exc).__name__)
    return None


_PS_PROBE_TIMEOUT_SECONDS = 5.0
_PROC_MOUNTINFO = Path("/proc/self/mountinfo")
_ZOMBIE_STATES = ("Z", "X")
# Owner record in a snapshot name: pid, process generation, boot and PID namespace. Any other
# record version matches nothing here, so a future or unparsable name is always kept [DATA-21].
_SNAPSHOT_RECORD = (
    r"-v2-p(?P<pid>[0-9]{1,7})-t(?P<start>[0-9A-Za-z]{1,64})-b(?P<boot>[0-9a-f]{0,32})-n(?P<ns>[0-9A-Za-z]{1,20})\.sh"
)


def _is_zombie(state: str) -> bool:
    return state.startswith(_ZOMBIE_STATES)


def _ps_lines(spec: str, pid: int | None = None, *, deadline: float | None = None) -> list[str] | None:
    """Rows of ``ps -o spec`` for one pid or the whole table; None when ps cannot answer [RES-02]."""
    command = ["ps", "-ww", "-o", spec, "-p", str(pid)] if pid is not None else ["ps", "-A", "-ww", "-o", spec]
    probe_deadline = time.monotonic() + _PS_PROBE_TIMEOUT_SECONDS
    deadline = min(deadline, probe_deadline) if deadline is not None else probe_deadline
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None
    probe = None
    try:
        probe = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        stdout, _ = probe.communicate(timeout=max(0.0, deadline - time.monotonic()))
    except BaseException as exc:
        if probe is not None:
            # subprocess.run's timeout path does a final unbounded communicate.
            # A slow ps wrapper may have descendants holding its pipes, so use
            # our owned group and WNOHANG instead, with no fresh wait [RES-02].
            _signal_group(probe.pid, signal.SIGKILL)
            probe.poll()
        if isinstance(exc, (OSError, subprocess.SubprocessError)):
            return None
        raise
    finally:
        if probe is not None:
            for stream in (probe.stdout, probe.stderr):
                if stream is not None:
                    stream.close()
    if probe.returncode != 0:
        return None
    return stdout.splitlines()


def _complete_process_view(spec: str, *, deadline: float | None = None) -> list[str] | None:
    """Require a self-visible, unrestricted process table before proving absence.

    A successful empty ps, or Linux hidepid/subset view, is not absence
    authority. Namespace/boot/generation checks remain separate [SECD-05].
    """
    rows = _ps_lines("pid=," + spec, deadline=deadline)
    if rows is None or not any(row.split()[:1] == [str(os.getpid())] for row in rows):
        return None
    if sys.platform.startswith("linux"):
        try:
            if os.readlink("/proc/self") != str(os.getpid()):
                return None  # this proc mount exposes a different PID namespace
            mounts = []
            for line in _PROC_MOUNTINFO.read_text().splitlines():
                before, after = line.split(" - ", 1)
                fields, filesystem = before.split(), after.split()
                if fields[4] == "/proc" and filesystem[0] == "proc":
                    mounts.append(set(fields[5].split(",") + filesystem[2].split(",")))
            if not mounts or any(
                option == "subset=pid" or (option.startswith("hidepid=") and option not in {"hidepid=0", "hidepid=off"})
                for options in mounts
                for option in options
            ):
                return None
        except (OSError, ValueError, IndexError):
            return None
    # Remove only the visibility sentinel column, retaining the requested data.
    fields = [row.split(None, 1) for row in rows]
    if any(len(row) != 2 or not row[0].isdigit() for row in fields):
        return None
    return [row[1] for row in fields]


def _process_state(pid: int) -> tuple[str, bool] | None:
    """(generation, is_zombie) of a live pid; the generation changes whenever the pid is reused."""
    if sys.platform.startswith("linux"):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        except OSError:
            return None
        return fields[19], _is_zombie(fields[0])
    rows = _ps_lines("lstart=,stat=", pid=pid)
    if not rows or len(rows[0].split()) < 2:
        return None
    *started, state = rows[0].split()
    return re.sub(r"[^0-9A-Za-z]", "", " ".join(started)), _is_zombie(state)


def _boot_generation() -> str:
    """Identity of this boot; a pid recorded under another boot names no process here."""
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip().replace("-", "")
    except OSError:
        return ""


def _pid_namespace() -> str:
    """PID namespace of this process; a pid number means something only inside its own namespace."""
    if not sys.platform.startswith("linux"):
        return "host"
    try:
        return os.readlink("/proc/self/ns/pid").split("[", 1)[1].rstrip("]")
    except (OSError, IndexError):
        return ""


def _snapshot_record() -> str:
    """Owner record for a new snapshot name; an unreadable generation leaves the name unparsable, so kept."""
    state = _process_state(os.getpid())
    start = state[0] if state is not None else ""
    return f"-v2-p{os.getpid()}-t{start}-b{_boot_generation()}-n{_pid_namespace()}"


def _owner_verdict(pid: int, start: str, boot: str, namespace: str) -> str:
    """ "dead" only when the recorded owner is provably gone; every other answer keeps the snapshot."""
    if namespace != _pid_namespace() or boot != _boot_generation():
        return "unverifiable"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "dead"
    except PermissionError:
        pass  # exists under another uid, so its generation below decides
    except OSError:
        return "unverifiable"
    state = _process_state(pid)
    if state is None:
        return "unverifiable"
    generation, zombie = state
    if generation != start:
        return "dead"  # the pid number was reused, so the recorded owner has exited
    return "dead" if zombie else "alive"


def _consumer_may_run(snapshot: Path) -> bool:
    """A live process whose command line names the snapshot still runs from it [SECD-05]."""
    rows = _complete_process_view("args=")
    if rows is None:
        return True  # the process table is unreadable, so no consumer can be ruled out
    return any(str(snapshot) in row for row in rows)


def _unlink_orphaned_snapshots(directory: Path, prefix: str) -> None:
    """Remove a snapshot only when its owner is provably dead and no live consumer runs it [SECD-05].

    Legacy bare-pid names, other record versions and unparsable names are kept.
    """
    pattern = re.compile(re.escape(prefix) + r"[a-z0-9_]+" + _SNAPSHOT_RECORD)
    for candidate in directory.glob(f"{prefix}*.sh"):
        match = pattern.fullmatch(candidate.name)
        if match is None:
            continue
        if _owner_verdict(int(match["pid"]), match["start"], match["boot"], match["ns"]) != "dead":
            continue
        if _consumer_may_run(candidate):
            continue
        try:
            candidate.unlink(missing_ok=True)
        except OSError:
            continue


@contextmanager
def _authenticated_transport(
    command, selected_source, *, admitted_source=None, snapshot_prefix=".workbay-transport-", owned_transport=None
):
    if selected_source is None:
        if owned_transport is not None:
            raise _TransportBindingRefused("owned_source_mismatch")
        yield command
        return
    if owned_transport is not None:
        # Borrow the admitted inode, never its cleanup responsibility. Each
        # consumer tracks its own launch so preflight is not dispatch evidence.
        if (
            owned_transport.closed
            or owned_transport.transport_owner.closed
            or not command
            or command[0] != owned_transport[0]
            or command[0] != selected_source["transport_path"]
            or owned_transport.receipt["transport_digest"] != selected_source["transport_digest"]
        ):
            raise _TransportBindingRefused("owned_source_mismatch")
        owner = owned_transport.transport_owner
        borrowed = _AuthenticatedTransportCommand(
            command,
            owner.snapshot,
            dict(owner.receipt),
            transport_owner=owner,
        )
        borrowed.deadline = owned_transport.deadline
        owner.open_borrowers += 1
        try:
            yield borrowed
        finally:
            borrowed.closed = True
            owner.open_borrowers -= 1
            if owner.closed or owner.consumer_lifetime == "unknown":
                _finish_transport_owner(owner)
                borrowed.cleanup_diagnostic = owner.cleanup_diagnostic
        return
    snapshot = None
    carrier = None
    binding_error = None
    exclude_diagnostic = None
    try:
        source = Path(selected_source["transport_path"])
        if not command or str(source) != command[0]:
            raise _TransportBindingRefused("selected_path_mismatch")
        # Nonblocking/no-follow open refuses redirects and special files before
        # reading. Authenticate the SAME bounded bytes that we copy, never a
        # separate path sample [CON-11]. No selected-source writer owns the new
        # inode or its unpredictable name; no advisory writer fence is needed.
        if admitted_source is None:
            admitted_source = _read_selected_transport(source)
        buffered_source, info = admitted_source
        if str(buffered_source) != str(source):
            raise _TransportBindingRefused("selected_path_mismatch")
        data = buffered_source.data
        digest = hashlib.sha256(data).hexdigest()
        if digest != selected_source["transport_digest"]:
            raise _TransportBindingRefused("selected_source_changed")
        # The git rule and orphan sweep run first, so a crash after this point
        # leaves an ignored file whose owner record is recorded in its suffix.
        exclude_diagnostic = _ignore_private_snapshots(source.parent)
        _unlink_orphaned_snapshots(source.parent, snapshot_prefix)
        # Same directory preserves native shebang, dirname($0), BASH_SOURCE and
        # adjacent VM assets; no interpreter wrapper or alternate source tree.
        fd, name = tempfile.mkstemp(prefix=snapshot_prefix, suffix=f"{_snapshot_record()}.sh", dir=source.parent)
        snapshot = Path(name)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fchmod(stream.fileno(), 0o500)
            snapshot_info = os.fstat(stream.fileno())
        # Close the sole writable handle before exposing the owned carrier.
        receipt = {
            "transport_path": str(snapshot),
            "transport_digest": digest,
            "device": snapshot_info.st_dev,
            "inode": snapshot_info.st_ino,
            "selected_device": info.st_dev,
            "selected_inode": info.st_ino,
        }
        carrier = _AuthenticatedTransportCommand(command, snapshot, receipt)
        carrier.cleanup_diagnostic = exclude_diagnostic
        carrier.snapshot_exclusion = exclude_diagnostic
    except _TransportBindingRefused as exc:
        binding_error = exc
    except (OSError, KeyError, ValueError) as exc:
        binding_error = _TransportBindingRefused(f"snapshot_unavailable:{type(exc).__name__}")
    try:
        if binding_error is not None:
            raise binding_error
        yield carrier
    finally:
        if carrier is not None:
            carrier.closed = True
            _finish_transport_owner(carrier)
        elif snapshot is not None:
            try:
                snapshot.unlink(missing_ok=True)
            except OSError as exc:
                # Cleanup cannot replace acknowledged or uncertain submission
                # evidence [RES-03][GRPH-14]. Retain one private inode and report
                # one bounded unlink attempt for later owner reconciliation.
                diagnostic = {
                    "reason": "transport_snapshot_cleanup_failed",
                    "error_type": type(exc).__name__,
                    "transport_path": str(snapshot),
                    "retryable": True,
                }
                if exclude_diagnostic is not None:
                    diagnostic["snapshot_exclusion"] = exclude_diagnostic
                target = carrier if carrier is not None else binding_error
                if target is not None:
                    target.cleanup_diagnostic = diagnostic


_PROCESS_GROUP_GRACE_SECONDS = 2.0
_PROCESS_GROUP_POLL_ATTEMPTS = 40
_PROCESS_GROUP_POLL_SECONDS = 0.05
_PROCESS_GROUP_SURVIVED = "remote_job_process_group_survived"
_OWNED_CONSUMER_UNSETTLED = "remote_job_owned_consumer_unsettled"

# Linux snapshot-consumer owner [PH-02]. A private subreaper adopts every
# orphaned descendant of the native child, including setsid-escaped ones that
# dropped the inherited pipes, which neither the process group nor pipe EOF can
# contain. It leads and shares the child's process group, so the runner's group
# signals, pgid evidence and absolute deadline are unchanged. Its receipt
# reports zero only after a nonblocking kernel no-children proof, not a filtered
# /proc snapshot. After a group SIGTERM it SIGKILLs adopted consumers,
# each first proven its child by waitid(WNOWAIT), so a reused PID is never hit.
_SNAPSHOT_CONSUMER_OWNER = r"""
import ctypes, json, os, resource, signal, subprocess, sys, time
receipt = int(sys.argv[1])
os.set_inheritable(receipt, False)
stopping = False
def stop(*args):
    global stopping
    stopping = True
signal.signal(signal.SIGTERM, stop)
owned = ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0  # PR_SET_CHILD_SUBREAPER
children_path = "/proc/self/task/%d/children" % os.getpid()
def report(**values):
    os.write(receipt, json.dumps({"schema": "workbay.runner-consumer-owner.v1", "owned": owned, **values}).encode())
try:
    root = subprocess.Popen(sys.argv[2:])
except OSError as exc:
    report(spawn_error=type(exc).__name__, live=0)
    sys.exit(2)
null = os.open(os.devnull, os.O_RDWR)
for target in (0, 1, 2):
    os.dup2(null, target)  # pipe EOF then tracks only the native consumers
returncode = None
def reap_children():
    global returncode
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return True  # ECHILD: no child remains that could fork or hand us descendants
        if not pid:
            return False  # a child still exists; WNOHANG zero is not settlement
        if pid == root.pid:
            returncode = os.waitstatus_to_exitcode(status)
while True:
    reap_children()
    try:
        with open(children_path) as source:
            children = [int(value) for value in source.read().split()]
    except (OSError, ValueError):
        owned, children = False, []
    live = []
    for child in children:
        try:
            with open("/proc/%d/stat" % child) as source:
                state = source.read().rsplit(")", 1)[1].split()[0]
        except (OSError, IndexError):
            state = "?"
        if state not in ("Z", "X"):  # a zombie has already handed us its children
            live.append(child)
    if stopping:
        for child in live:
            if child == root.pid:
                continue  # the runner's group TERM/KILL keeps the child's grace
            try:
                os.waitid(os.P_PID, child, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                os.kill(child, signal.SIGKILL)
            except (ChildProcessError, ProcessLookupError):
                pass
    remaining = len(live)
    if not remaining and not reap_children():
        # A listed child may have forked then become Z after the list was read.
        # Only ECHILD closes that adoption race; otherwise at least one child
        # is unsettled. Keep its ownership for the next stop/signal iteration.
        remaining = 1
    if returncode is not None and not (stopping and remaining):
        report(returncode=returncode, live=remaining)
        break
    time.sleep(0.01)
if returncode < 0:
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    signal.signal(-returncode, signal.SIG_DFL)
    os.kill(os.getpid(), -returncode)
os._exit(returncode & 0xFF)
"""


def _owner_receipt(fd: int) -> dict[str, Any] | None:
    """The owner's final receipt; None while it still runs, after a kill, or when malformed."""
    chunks = []
    os.set_blocking(fd, False)
    try:
        while data := os.read(fd, 65536):
            chunks.append(data)
    except BlockingIOError:
        return None  # the owner holds the write end, so it has not finished
    try:
        receipt = json.loads(b"".join(chunks))
    except ValueError:
        return None
    if not isinstance(receipt, dict) or receipt.get("schema") != "workbay.runner-consumer-owner.v1":
        return None
    return receipt


def _signal_group(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError):
        # Gone already, or members we may not signal; the liveness poll reports the latter.
        return


def _group_absent_or_zombie(pgid: int, *, deadline: float | None = None) -> bool:
    """True only when ps lists no member of the group, or every listed member is a zombie."""
    rows = _complete_process_view("pgid=,stat=", deadline=deadline)
    if rows is None:
        return False
    states = []
    for row in rows:
        fields = row.split()
        if len(fields) != 2 or not fields[0].isdigit():
            return False
        if fields[0] == str(pgid):
            states.append(fields[1])
    return all(_is_zombie(state) for state in states)


def _group_alive(pgid: int, *, deadline: float | None = None) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # EPERM is not proof of death. Darwin answers it for an all-zombie group, but also for members
        # we may not signal, so only a process table showing no members or only zombies counts as gone.
        return not _group_absent_or_zombie(pgid, deadline=deadline)
    # Linux also answers success for an all-zombie group. A complete view can
    # confirm those consumers gone, even when their adopter has yet to reap.
    return not _group_absent_or_zombie(pgid, deadline=deadline)


def _drain(proc: subprocess.Popen[str], deadline: float) -> tuple[str | bytes | None, str | bytes | None, bool]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None, None, False
    try:
        stdout, stderr = proc.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        return exc.output, exc.stderr, False
    return stdout, stderr, True


def _stop_process_group(
    proc: subprocess.Popen[str], deadline: float
) -> tuple[str | bytes | None, str | bytes | None, bool]:
    """TERM, KILL, drain and probe within one absolute deadline; True means unconfirmed."""
    pgid = proc.pid
    _signal_group(pgid, signal.SIGTERM)
    now = time.monotonic()
    term_deadline = min(deadline, now + max(0.0, deadline - now) / 2)
    stdout, stderr, drained = _drain(proc, term_deadline)
    _signal_group(pgid, signal.SIGKILL)
    proc.poll()  # Popen uses waitpid(WNOHANG), never a blocking reap.
    # Consult visibility before a stuck drain can consume the last remainder.
    alive = _group_alive(pgid, deadline=deadline)
    if not drained:
        final_stdout, final_stderr, _ = _drain(proc, deadline)
        stdout = final_stdout if final_stdout is not None else stdout
        stderr = final_stderr if final_stderr is not None else stderr
    proc.poll()
    if not alive:
        return stdout, stderr, False
    for _ in range(_PROCESS_GROUP_POLL_ATTEMPTS):
        if not _group_alive(pgid, deadline=deadline):
            return stdout, stderr, False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        _sleep(min(_PROCESS_GROUP_POLL_SECONDS, remaining))
        proc.poll()
    return stdout, stderr, True


def _as_text(value: str | bytes | None) -> str | None:
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def _run_child(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    deadline = time.monotonic() + max(0.0, timeout)
    carrier = _consumer_carrier(cmd)
    limits = [getattr(cmd, "deadline", None)]
    if carrier is not None:
        limits.extend((carrier.deadline, carrier.transport_owner.deadline))
    for limit in limits:
        if limit is not None:
            deadline = min(deadline, limit)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(cmd, timeout)
    reserve = min(_PROCESS_GROUP_GRACE_SECONDS, remaining * _CANCEL_RESERVE_FRACTION)
    work_deadline = deadline - reserve
    proc = None
    lifetime_recorded = False
    # Only a snapshot consumer's release depends on settlement. Off Linux there
    # is no subreaper, so group absence remains the (weaker) legacy evidence.
    owned = carrier is not None and sys.platform.startswith("linux")
    receipt_read = receipt_write = -1
    receipt = None

    def record(survived):
        nonlocal lifetime_recorded, receipt
        reason = _PROCESS_GROUP_SURVIVED if survived else "process_group_clean"
        detail = ""
        if owned and not survived:
            receipt = _owner_receipt(receipt_read)
            if receipt is None or receipt.get("owned") is not True or receipt.get("live") != 0:
                # Group absence and pipe EOF never prove an escaped consumer gone.
                survived, reason = True, _OWNED_CONSUMER_UNSETTLED
                detail = (
                    "no owner receipt"
                    if receipt is None
                    else f"owned={receipt.get('owned')} live={receipt.get('live')}"
                )
        _record_consumer_lifetime(
            cmd,
            "unknown" if survived else "clean",
            {
                "source": "remote_job_runner._run_child",
                "pgid": proc.pid,
                "operation": cmd[1] if len(cmd) > 1 else "",
                "reason": reason,
                "detail": detail,
            },
        )
        lifetime_recorded = True

    try:
        argv, owner_fds = cmd, {}
        if owned:
            receipt_read, receipt_write = os.pipe()
            argv = [sys.executable, "-I", "-c", _SNAPSHOT_CONSUMER_OWNER, str(receipt_write), *cmd]
            owner_fds = {"pass_fds": (receipt_write,)}
        # A session of its own makes the child's pid its process-group id, so a timeout reaches its descendants.
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=_child_env(env),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
            **owner_fds,
        )
        if owned:
            os.close(receipt_write)
            receipt_write = -1
        try:
            stdout, stderr = proc.communicate(timeout=max(0.0, work_deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            partial_stdout, partial_stderr, survived = _stop_process_group(proc, deadline)
            record(survived)
            partial_stdout = partial_stdout if partial_stdout is not None else exc.output
            partial_stderr = partial_stderr if partial_stderr is not None else exc.stderr
            detail = _as_text(partial_stderr)
            if survived:
                marker = f"{_PROCESS_GROUP_SURVIVED} pgid={proc.pid}"
                detail = f"{detail}\n{marker}" if detail else marker
            raise subprocess.TimeoutExpired(
                cmd,
                timeout,
                output=_as_text(partial_stdout),
                stderr=detail,
            ) from exc
        except BaseException:
            _, _, survived = _stop_process_group(proc, min(deadline, time.monotonic() + _PROCESS_GROUP_GRACE_SECONDS))
            record(survived)
            raise
        # Pipe EOF and direct-child exit alone do not prove descendant exit.
        if carrier is not None:
            record(_group_alive(proc.pid, deadline=deadline))
        if receipt is not None and isinstance(receipt.get("spawn_error"), str):
            # The owner could not start the native child: the legacy spawn refusal.
            return subprocess.CompletedProcess(
                cmd,
                2,
                "",
                f"remote_job_subprocess_failed reason={receipt['spawn_error'][:64]}",
            )
        return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
    except (OSError, ValueError) as exc:
        return subprocess.CompletedProcess(
            cmd,
            2,
            "",
            f"remote_job_subprocess_failed reason={type(exc).__name__}",
        )
    finally:
        if proc is not None:
            # Even a failure *inside* cleanup cannot become implicit permission
            # to unlink an authenticated consumer's snapshot [SECD-05].
            if carrier is not None and not lifetime_recorded:
                record(True)
            if proc.returncode is None:
                proc.poll()
            for stream in (getattr(proc, "stdout", None), getattr(proc, "stderr", None)):
                if stream is not None:
                    stream.close()
        for fd in (receipt_read, receipt_write):
            if fd >= 0:
                os.close(fd)


def run_status(
    cmd: list[str],
    *,
    cwd: str,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Run one status command with the scrubbed host environment passed explicitly."""
    return _run_child(cmd, cwd=cwd, env=_child_env(None), timeout=timeout)


def _build_command(cmd: list[str], identity: RemoteJobIdentity) -> list[str] | None:
    if len(cmd) < 2 or cmd[1] != "build":
        return None
    return [cmd[0], "submit", "--job-id", identity.job_id, *cmd[2:]]


def _operation_command(cmd: list[str], operation: str, identity: RemoteJobIdentity) -> list[str]:
    result = [_execution_command(cmd)[0], operation, "--job-id", identity.job_id]
    if operation == "collect":
        flags = {
            "--out",
            "--result-out",
            "--debug-out",
            "--stream-out",
            "--selfverify-out",
            "--uncommitted-out",
            "--phases-out",
            "--provenance-out",
        }
        for index in range(2, len(cmd) - 1, 2):
            if cmd[index] in flags:
                result.extend(cmd[index : index + 2])
    return _SnapshotConsumerArgv(result, cmd) if isinstance(cmd, _AuthenticatedTransportCommand) else result


def _as_completed(
    original_cmd: list[str], completed: subprocess.CompletedProcess[str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        original_cmd,
        completed.returncode,
        completed.stdout or "",
        completed.stderr or "",
    )


def _identity_refusal(
    cmd: list[str], env: dict[str, str] | None
) -> tuple[RemoteJobIdentity | None, subprocess.CompletedProcess[str] | None]:
    identity = _identity(env)
    if isinstance(identity, ContractError):
        return None, _refusal(cmd, f"remote_job_identity_missing {identity.field}")
    return identity, None


def submit_only(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Submit ``build`` as an idempotent remote job without waiting for it."""
    identity, refusal = _identity_refusal(cmd, env)
    if refusal is not None:
        return refusal
    assert identity is not None
    submit_cmd = _build_command(cmd, identity)
    if submit_cmd is None:
        return _refusal(cmd, "remote_job_invalid_command expected=build")
    if isinstance(cmd, _AuthenticatedTransportCommand):
        if timeout <= 0:
            return _refusal(cmd, "transport_source_binding_refused deadline_expired")
        submit_cmd[0] = _execution_command(cmd)[0]
        submit_cmd = _SnapshotConsumerArgv(submit_cmd, cmd)
    completed = _run_child(submit_cmd, cwd=cwd, env=env, timeout=timeout)
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip()
        suffix = f" {detail}" if detail else ""
        completed.stderr = f"remote_job_submit_failed rc={completed.returncode}{suffix}"
    return _as_completed(cmd, completed)


def collect_only(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Collect the completed job associated with a ``build`` command."""
    identity, refusal = _identity_refusal(cmd, env)
    if refusal is not None:
        return refusal
    assert identity is not None
    if _build_command(cmd, identity) is None:
        return _refusal(cmd, "remote_job_invalid_command expected=build")
    completed = _run_child(
        _operation_command(cmd, "collect", identity),
        cwd=cwd,
        env=env,
        timeout=timeout,
    )
    return _as_completed(cmd, completed)


def _cancel_reserve(timeout: float) -> float:
    """Seconds of the authorized total held back for a cancel after a phase times out."""
    # A full 10 s reserve would consume a short budget whole, so short budgets
    # reserve a quarter of it; budgets of 40 s or more keep the full allowance.
    return max(0.0, min(_CANCEL_RESERVE_MAX_SECONDS, _CANCEL_RESERVE_FRACTION * timeout))


@dataclass(frozen=True)
class _TimeoutContext:
    cmd: list[str]
    timeout: float
    identity: RemoteJobIdentity
    cwd: str
    env: dict[str, str] | None
    work_deadline: float  # submit, poll and collect stop here, leaving the cancel reserve
    total_deadline: float  # the authorized total; a cancel may not run past it


@dataclass(frozen=True)
class _JobStatus:
    completed: subprocess.CompletedProcess[str]
    payload: dict[str, Any]


def _status_payload(completed: subprocess.CompletedProcess[str], identity: RemoteJobIdentity) -> dict[str, Any] | None:
    try:
        payload: Any = json.loads(completed.stdout)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("job_id") != identity.job_id:
        return None
    if not isinstance(payload.get("state"), str):
        return None
    return payload


def _timed_out(
    context: _TimeoutContext,
    stderr_parts: list[str],
    *,
    stdout: str = "",
    cause: subprocess.TimeoutExpired | None = None,
    include_cause_output: bool = False,
) -> None:
    cancel_cmd = _operation_command(context.cmd, "cancel", context.identity)
    cancel_timeout = max(0.0, context.total_deadline - time.monotonic())
    if cancel_timeout <= 0:
        # Once the total is spent, the VM unit's RuntimeMaxSec retires an uncancelled job, so no cancel is started.
        stderr_parts.append("remote_job_cancel_failed reason=deadline")
    else:
        try:
            cancelled = _run_child(
                _child_command(cancel_cmd, context.total_deadline),
                cwd=context.cwd,
                env=context.env,
                timeout=cancel_timeout,
            )
            job_was_absent = (
                cancelled.returncode == 2 and (cancelled.stderr or "").strip() == "remote_agent: job_not_found"
            )
            if cancelled.returncode != 0 and not job_was_absent:
                stderr_parts.append(f"remote_job_cancel_failed rc={cancelled.returncode}")
        except subprocess.TimeoutExpired as exc:
            stderr_parts.append("remote_job_cancel_failed reason=timeout")
            if exc.stderr:
                stderr_parts.append(_as_text(exc.stderr))
    if cause is not None:
        if include_cause_output and cause.output:
            stdout = cause.output.decode(errors="replace") if isinstance(cause.output, bytes) else cause.output
        if cause.stderr:
            detail = cause.stderr.decode(errors="replace") if isinstance(cause.stderr, bytes) else cause.stderr
            stderr_parts.append(detail)
    stderr = "\n".join(part for part in stderr_parts if part)
    raise subprocess.TimeoutExpired(context.cmd, context.timeout, output=stdout, stderr=stderr) from None


def _remaining(context: _TimeoutContext) -> float:
    return context.work_deadline - time.monotonic()


def _child_command(command, deadline):
    """Carry the phase's absolute bound through the unchanged _run_child seam."""
    return _SnapshotConsumerArgv(command, _consumer_carrier(command), deadline=deadline)


def _submit_job(
    context: _TimeoutContext,
    submit_cmd: list[str],
    stderr_parts: list[str],
) -> subprocess.CompletedProcess[str] | None:
    try:
        submitted = _run_child(
            _child_command(submit_cmd, context.work_deadline),
            cwd=context.cwd,
            env=context.env,
            timeout=max(0.0, _remaining(context)),
        )
    except subprocess.TimeoutExpired as exc:
        _timed_out(context, stderr_parts, cause=exc)
    if submitted.stderr:
        stderr_parts.append(submitted.stderr)
    if submitted.returncode == 0:
        return None
    detail = (submitted.stderr or "").strip()
    suffix = f" {detail}" if detail else ""
    return subprocess.CompletedProcess(
        context.cmd,
        submitted.returncode,
        submitted.stdout or "",
        f"remote_job_submit_failed rc={submitted.returncode}{suffix}",
    )


def _read_status(
    context: _TimeoutContext,
    status_cmd: list[str],
    stderr_parts: list[str],
) -> _JobStatus | subprocess.CompletedProcess[str]:
    try:
        status = _run_child(
            _child_command(status_cmd, context.work_deadline),
            cwd=context.cwd,
            env=context.env,
            timeout=max(0.0, _remaining(context)),
        )
    except subprocess.TimeoutExpired as exc:
        _timed_out(context, stderr_parts, cause=exc)
    if status.stderr:
        stderr_parts.append(status.stderr)
    if status.returncode != 0:
        detail = (status.stderr or "").strip()
        suffix = f" {detail}" if detail else ""
        return subprocess.CompletedProcess(
            context.cmd,
            status.returncode,
            "",
            f"remote_job_status_failed rc={status.returncode}{suffix}",
        )
    payload = _status_payload(status, context.identity)
    if payload is None:
        return _refusal(context.cmd, f"remote_job_status_invalid job_id={context.identity.job_id}")
    return _JobStatus(status, payload)


def _poll_job(
    context: _TimeoutContext,
    stderr_parts: list[str],
) -> subprocess.CompletedProcess[str] | None:
    interval = _POLL_INITIAL_SECONDS
    status_cmd = _operation_command(context.cmd, "status", context.identity)
    while True:
        status = _read_status(context, status_cmd, stderr_parts)
        if isinstance(status, subprocess.CompletedProcess):
            return status
        payload = status.payload
        state = payload["state"]
        if state == "lost":
            return subprocess.CompletedProcess(context.cmd, 1, "", f"remote_job_lost job_id={context.identity.job_id}")
        if state == "done":
            return None
        if state == "unknown" and payload.get("observer_error") is None:
            return subprocess.CompletedProcess(
                context.cmd,
                1,
                status.completed.stdout or "",
                f"remote_job_unknown job_id={context.identity.job_id}",
            )
        remaining = _remaining(context)
        if remaining <= 0:
            _timed_out(context, stderr_parts)
        _sleep(min(interval, remaining))
        interval = min(interval * 2, _POLL_MAX_SECONDS)


def _collect_job(
    context: _TimeoutContext,
    stderr_parts: list[str],
) -> subprocess.CompletedProcess[str]:
    collect_cmd = _operation_command(context.cmd, "collect", context.identity)
    try:
        collected = _run_child(
            _child_command(collect_cmd, context.work_deadline),
            cwd=context.cwd,
            env=context.env,
            timeout=max(0.0, _remaining(context)),
        )
    except subprocess.TimeoutExpired as exc:
        _timed_out(context, stderr_parts, cause=exc, include_cause_output=True)
    if collected.stderr:
        stderr_parts.append(collected.stderr)
    return subprocess.CompletedProcess(
        context.cmd,
        collected.returncode,
        collected.stdout or "",
        "".join(stderr_parts),
    )


def run_detached(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Run a remote build through submit, bounded status polling, and collect."""
    identity, refusal = _identity_refusal(cmd, env)
    if refusal is not None:
        return refusal
    assert identity is not None
    submit_cmd = _build_command(cmd, identity)
    if submit_cmd is None:
        return _refusal(cmd, "remote_job_invalid_command expected=build")
    total_deadline = time.monotonic() + timeout
    if isinstance(cmd, _AuthenticatedTransportCommand):
        if cmd.deadline is not None:
            total_deadline = min(total_deadline, cmd.deadline)
        if total_deadline <= time.monotonic():
            return _refusal(cmd, "transport_source_binding_refused deadline_expired")
        submit_cmd[0] = _execution_command(cmd)[0]
        submit_cmd = _SnapshotConsumerArgv(submit_cmd, cmd)
    context = _TimeoutContext(
        cmd,
        timeout,
        identity,
        cwd,
        env,
        work_deadline=total_deadline - _cancel_reserve(timeout),
        total_deadline=total_deadline,
    )
    stderr_parts: list[str] = []
    submit_error = _submit_job(context, submit_cmd, stderr_parts)
    if submit_error is not None:
        return submit_error
    poll_result = _poll_job(context, stderr_parts)
    if poll_result is not None:
        return poll_result
    return _collect_job(context, stderr_parts)
