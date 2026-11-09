"""Non-model provisioning handshake at the remote model process boundary.

No persisted PASS is reused here: volatile environment, directory and policy
inputs are observed again immediately before exec. Receipts are provenance,
not an isolation boundary against another process with the same uid.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
import math
import os
import pwd
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
from pathlib import Path

from .codex_lane_config import EXECUTION_PREFLIGHT_TIMEOUT_S as DEFAULT_TIMEOUT

SCHEMA = "workbay/execution-preflight@1"
MIN_FREE_BYTES = 6 * 1024**3
WRITE_CAPABILITY = "workspace-write"
ANALYSIS_CAPABILITY = "analysis-read-only"
ANALYSIS_PERMISSION_PROFILE = "workbay_analysis_read_only"
ANALYSIS_PERMISSION_CONFIG = 'permissions={workbay_analysis_read_only={extends=":read-only"}}'
ANALYSIS_PERMISSION_TEMPLATE = 'permissions.workbay_analysis_read_only.extends=":read-only"'


def capability_policy(roots, capability):
    """The closed set of native contracts, shared by producer and consumer."""
    from .codex_lane_config import EXECUTION_PERMISSION_PROFILE

    if capability == ANALYSIS_CAPABILITY:
        return ANALYSIS_PERMISSION_PROFILE, ANALYSIS_PERMISSION_CONFIG
    if capability == WRITE_CAPABILITY:
        return EXECUTION_PERMISSION_PROFILE, permission_config(roots)
    raise Refused("execution_capability_unknown")


def analysis_argv(*, model, effort, service_tier):
    from .backend_spec import _codex_cli_argv

    argv = _codex_cli_argv(model=model, effort=effort, service_tier=service_tier)
    legacy = argv.index("-s")
    del argv[legacy - 2 : legacy + 2]
    insert = argv.index("--output-schema")
    argv[insert:insert] = [
        "-c", f'default_permissions="{ANALYSIS_PERMISSION_PROFILE}"',
        "-c", ANALYSIS_PERMISSION_TEMPLATE,
    ]
    return argv

# The child is run by the SAME interpreter, environment and cwd as future work.
# It writes only fresh random files and unlinks only files it created.
WRITE_CHILD = """import json, os, secrets, sys
from pathlib import Path
results = []
for raw in sys.argv[1:]:
    root = Path(raw)
    name = root / (".workbay-preflight-" + secrets.token_hex(16))
    created = False
    try:
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
        with os.fdopen(fd, "w+b") as f:
            f.write(b"workbay-provisioning"); f.flush(); f.seek(0)
            if f.read() != b"workbay-provisioning": raise OSError("readback mismatch")
        name.unlink(); created = False
        results.append({"root": str(root), "create_read_unlink": True})
    finally:
        if created: name.unlink()
print(json.dumps(results))
"""


class Refused(Exception):
    def __init__(
        self,
        reason,
        *,
        status="UNKNOWN",
        retryable=False,
        repair="Repair provisioning and retry before charging source work.",
    ):
        self.reason, self.status, self.retryable, self.repair = reason, status, retryable, repair


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _file_identity(path):
    p = Path(path).resolve()
    s = p.stat()
    return {
        "path": str(p),
        "device": s.st_dev,
        "inode": s.st_ino,
        "mode": s.st_mode,
        "uid": s.st_uid,
        "gid": s.st_gid,
        "mtime_ns": s.st_mtime_ns,
        "size": s.st_size,
    }


def _executable_identity(path):
    identity = _file_identity(path)
    with Path(path).open("rb") as stream:
        identity["sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
    return identity


def _cli_identity(path):
    identity = _executable_identity(path)
    resolved = Path(path).resolve()
    # npm's entry JS is not the runtime binary. Bind both the wrapper's
    # interpreter and the selected platform executable when provisioned.
    if resolved.name == "codex.js":
        node = shutil.which("node")
        if not node:
            raise Refused("executable_missing")
        identity["node"] = _executable_identity(node)
        arch = {"aarch64": "arm64", "x86_64": "x64"}.get(os.uname().machine)
        target = {"arm64": "aarch64", "x64": "x86_64"}.get(arch)
        if target is None:
            raise Refused("sandbox_backend_unsupported")
        runtime = (
            resolved.parents[1]
            / "node_modules/@openai"
            / ("codex-linux-" + arch)
            / "vendor"
            / (target + "-unknown-linux-musl/bin/codex")
        )
        if not runtime.is_file():
            raise Refused("executable_missing")
        identity["runtime"] = _executable_identity(runtime)
    return identity


def _option(argv, *names):
    for i, arg in enumerate(argv):
        for name in names:
            if arg == name and i + 1 < len(argv):
                return argv[i + 1]
            if arg.startswith(name + "="):
                return arg.split("=", 1)[1]
    return None


def _child(argv, *, deadline, env, cwd, evidence=None, evidence_dir=None):
    """Bound startup, imports, descendants and output drain with one deadline.

    Output is file backed, so an inherited pipe cannot extend the deadline.
    Linux descendants are identified by a private inherited nonce, including
    children which start another session. Unknown cleanup prevents PASS.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0.4:
        raise Refused("deadline_expired", retryable=True)
    nonce = "WORKBAY_EXECUTION_PROBE_" + uuid.uuid4().hex
    child_env = dict(env)
    # Keep ancestor ownership tags, including across nested session leaders.
    child_env[nonce] = "1"
    proc = None
    timed_out = False

    def output_file(suffix):
        if evidence is not None and evidence_dir is not None:
            return tempfile.NamedTemporaryFile(
                prefix="native-preflight-", suffix=suffix, dir=evidence_dir, delete=False
            )
        return tempfile.TemporaryFile()

    with output_file(".stdout") as stdout, output_file(".stderr") as stderr:
        try:
            if sys.platform != "linux" or not hasattr(os, "waitid"):
                raise Refused("descendant_supervision_unavailable")
            # This is an isolated boundary/worker process, never a shared
            # daemon. Adopt orphaned grandchildren so cleanup can await them.
            import ctypes

            if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
                raise Refused("descendant_supervision_unavailable")
            proc = subprocess.Popen(
                argv,
                cwd=cwd,
                env=child_env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            # WNOWAIT pins the group leader until cleanup has signalled its
            # private group, avoiding PID/group reuse after wait().
            while os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
                if time.monotonic() >= deadline - 0.35:
                    timed_out = True
                    break
                time.sleep(0.005)
        finally:
            if proc is not None:
                # The group is private to this invocation. The nonce scan also
                # covers setsid descendants; never signal unrelated processes.
                descendants = set()
                if sys.platform == "linux":
                    for entry in Path("/proc").iterdir():
                        if entry.name.isdigit():
                            try:
                                if (nonce + "=1").encode() in (entry / "environ").read_bytes().split(b"\0"):
                                    descendants.add(int(entry.name))
                            except (OSError, PermissionError):
                                continue
                for pid in descendants:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait(timeout=max(0.001, deadline - time.monotonic()))
                for pid in descendants - {proc.pid}:
                    while True:
                        try:
                            ended, _ = os.waitpid(pid, os.WNOHANG)
                        except ChildProcessError:
                            # It may still be parented to an owned descendant.
                            if not Path(f"/proc/{pid}").exists():
                                break
                            ended = 0
                        if ended:
                            break
                        if time.monotonic() >= deadline:
                            raise Refused("descendant_cleanup_unknown", retryable=True)
                        time.sleep(0.005)
        stdout.seek(0)
        stderr.seek(0)
        out = stdout.read(65537)
        err = stderr.read(65537)
        if evidence is not None:
            evidence.append(
                {
                    "argv": list(map(str, argv)),
                    "returncode": proc.returncode,
                    "stdout_path": str(stdout.name),
                    "stderr_path": str(stderr.name),
                    "stdout_bytes": os.fstat(stdout.fileno()).st_size,
                    "stderr_bytes": os.fstat(stderr.fileno()).st_size,
                    "stdout_base64": base64.b64encode(out).decode() if len(out) <= 65536 else None,
                    "stderr_base64": base64.b64encode(err).decode() if len(err) <= 65536 else None,
                }
            )
        if timed_out or time.monotonic() >= deadline:
            raise Refused("deadline_expired", retryable=True)
        if len(out) > 65536 or len(err) > 65536:
            raise Refused("probe_output_overflow")
        # Never publish credential-bearing stdout/stderr from arbitrary imports.
        return proc.returncode, out.decode(errors="replace")


def _runtime_observation(source_root):
    """Observe imports, not just installed distribution metadata."""
    from .codex_lane_config import CODEX_CLI_VERSION, EXECUTION_PREFLIGHT_IMPORTS
    from .preflight_attestation import ATTESTATION_SCHEMA_VERSION

    if ATTESTATION_SCHEMA_VERSION != 2:
        raise Refused("runtime_schema_unsupported")
    imports = []
    for name in EXECUTION_PREFLIGHT_IMPORTS:
        try:
            mod = importlib.import_module(name)
        except ImportError as exc:
            raise Refused(
                "companion_missing", repair=f"Provision the locked runtime companion {name} in the lane interpreter."
            ) from exc
        origin = Path(mod.__file__).resolve()
        # Repository imports must come from this checkout. Installed releases
        # use their provisioned origin, and retain that identity in the receipt.
        expected = (
            source_root
            / "packages"
            / {
                "workbay_orchestrator_mcp": "mcp-workbay-orchestrator/src",
                "workbay_handoff_mcp": "mcp-workbay-handoff/src",
                "workbay_protocol": "workbay-protocol/src",
            }[name]
            / name
        )
        if expected.exists() and not origin.is_relative_to(expected.resolve()):
            raise Refused(
                "runtime_origin_mismatch",
                repair=f"Reprovision {name} from the exact lane source, then restart the interpreter.",
            )
        imports.append(
            {
                "module": name,
                "origin": _file_identity(origin),
                "sha256": hashlib.sha256(origin.read_bytes()).hexdigest(),
            }
        )
    return {
        "imports": imports,
        "attestation_schema": ATTESTATION_SCHEMA_VERSION,
        "expected_executable_version": CODEX_CLI_VERSION,
    }


def _roots(cwd, output_dir, env):
    cwd = Path(cwd).resolve(strict=True)
    tmp = env.get("TMPDIR")
    if not tmp or not Path(tmp).is_dir():
        raise Refused("tmpdir_missing")
    if not Path(tmp).is_absolute():
        raise Refused("tmpdir_not_absolute")
    rc, git = _child(["git", "rev-parse", "--absolute-git-dir"], deadline=time.monotonic() + 2, env=env, cwd=cwd)
    if rc:
        raise Refused("git_root_unavailable")
    git = git.strip()
    return list(
        map(
            str,
            [
                cwd,
                Path(output_dir).resolve(strict=True),
                Path(tmp).resolve(strict=True),
                Path(git).resolve(strict=True),
            ],
        )
    )


def permission_config(roots):
    from .codex_lane_config import EXECUTION_PERMISSION_PROFILE

    # TOML inline tables as WHOLE values: dotted CLI keys cannot quote paths.
    def table(values):
        return "{" + ",".join(json.dumps(k) + "=" + v for k, v in values.items()) + "}"

    workspace = table({p: "true" for p in roots[:3]})
    filesystem = table({roots[-1]: '"write"'})
    return (
        "permissions={"
        + EXECUTION_PERMISSION_PROFILE
        + '={extends=":workspace",workspace_roots='
        + workspace
        + ",filesystem="
        + filesystem
        + "}}"
    )


def materialize_analysis_argv(argv):
    argv = list(argv)
    if ANALYSIS_PERMISSION_TEMPLATE in argv:
        if argv.count(ANALYSIS_PERMISSION_TEMPLATE) != 1:
            raise Refused("sandbox_policy_ambiguous")
        argv[argv.index(ANALYSIS_PERMISSION_TEMPLATE)] = ANALYSIS_PERMISSION_CONFIG
    return argv


def materialize_execution_argv(argv, *, cwd, output_dir, env):
    """Resolve only the shipped template, never repair an arbitrary policy."""
    from .codex_lane_config import EXECUTION_PERMISSION_TEMPLATE

    argv = materialize_analysis_argv(argv)
    if EXECUTION_PERMISSION_TEMPLATE in argv:
        if argv.count(EXECUTION_PERMISSION_TEMPLATE) != 1:
            raise Refused("sandbox_policy_ambiguous")
        roots = _roots(cwd, output_dir, env)
        argv[argv.index(EXECUTION_PERMISSION_TEMPLATE)] = permission_config(roots)
    return argv


def _policy(argv, roots, *, capability=WRITE_CAPABILITY):
    profile, config = capability_policy(roots, capability)

    if (
        "--ignore-user-config" not in argv
        or _option(argv, "-p", "--profile", "-P", "--permission-profile")
        or _option(argv, "-s", "--sandbox")
        or any(
            a.startswith(("--add-dir", "--dangerously-", "--approve-for-me", "--sandbox", "--enable", "--disable"))
            or (a.startswith(("-s", "-p", "-P")) and not a.startswith("--"))
            for a in argv
        )
    ):
        raise Refused("sandbox_policy_unavailable")
    # A future/ambient CLI selector must not change cwd or policy after the
    # probe. Only the existing remote recipe's execution surface is mirrored.
    valued = {"-c", "--config", "-C", "--cd", "-m", "--model", "--output-schema", "-o", "--output-last-message"}
    if not argv or argv[0] != "exec" or argv.count("--ignore-user-config") != 1:
        raise Refused("sandbox_policy_unavailable")
    index = 1
    while index < len(argv):
        arg = argv[index]
        if arg in valued:
            if index + 1 >= len(argv):
                raise Refused("sandbox_config_unmirrored")
            index += 2
        elif arg in {"--json", "--ignore-user-config", "-"}:
            if arg == "-" and index != len(argv) - 1:
                raise Refused("sandbox_config_unmirrored")
            index += 1
        elif any(arg.startswith(option + "=") for option in valued):
            index += 1
        else:
            raise Refused("sandbox_config_unmirrored")
    overrides = []
    for i, arg in enumerate(argv):
        if arg in {"-c", "--config"}:
            if i + 1 == len(argv):
                raise Refused("sandbox_config_unmirrored")
            overrides.append(argv[i + 1])
        elif arg.startswith(("-c=", "--config=")):
            overrides.append(arg.split("=", 1)[1])
        elif arg.startswith("-c") and arg not in {"-c", "--config"} and not arg.startswith("--"):
            raise Refused("sandbox_config_unmirrored")
    policy = [v for v in overrides if v.split("=", 1)[0] in {"permissions", "default_permissions"}]
    if sorted(policy) != sorted(
        [config, 'default_permissions="' + profile + '"']
    ):
        raise Refused("sandbox_policy_unavailable")
    for value in overrides:
        key = value.split("=", 1)[0]
        if key in {"permissions", "default_permissions", "model_reasoning_effort", "service_tier"}:
            continue
        if capability == WRITE_CAPABILITY and key.startswith("mcp_servers.") and key.endswith((".command", ".args")):
            continue
        raise Refused("sandbox_config_unmirrored")
    # Parse every value too, so malformed TOML never reaches a model boundary.
    try:
        for value in overrides:
            key, _, raw = value.partition("=")
            # CLI -c explicitly falls back to raw strings for bare routing
            # literals. Preserve the real recipe's high/default spelling in
            # BOTH commands; permission definitions must remain valid TOML.
            if key in {"model_reasoning_effort", "service_tier"} and re.fullmatch(r"[a-z][a-z0-9_-]*", raw):
                continue
            tomllib.loads(value)
    except tomllib.TOMLDecodeError as exc:
        raise Refused("sandbox_config_unmirrored") from exc
    return profile, [item for value in overrides for item in ("-c", value)]


READ_ONLY_CHILD = """import errno,json,os,secrets,sys
from pathlib import Path
results=[]
for raw in sys.argv[1:]:
    root=Path(raw)
    os.listdir(root)
    name=root / (".workbay-readonly-"+secrets.token_hex(16))
    created=False
    try:
        fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); created=True; os.close(fd)
    except OSError as e:
        if e.errno not in (errno.EPERM,errno.EACCES,errno.EROFS): raise
        results.append({"root":raw,"read_allowed":True,"write_denied":True,"errno":e.errno})
    else:
        raise RuntimeError("analysis write allowed")
    finally:
        if created: name.unlink()
print(json.dumps(results))
"""


def valid_readonly_denials(rows, roots):
    return (
        isinstance(rows, list)
        and len(rows) == len(roots)
        and all(
            isinstance(row, dict)
            and set(row) == {"root", "read_allowed", "write_denied", "errno"}
            and row["root"] == root
            and row["read_allowed"] is True
            and row["write_denied"] is True
            and type(row["errno"]) is int and row["errno"] in (1, 13, 30)
            for row, root in zip(rows, roots)
        )
    )


NEGATIVE_CHILD = """import errno,json,os,sys
p=sys.argv[1]
created=False
try:
    fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); created=True; os.close(fd)
except OSError as e:
    if e.errno not in (errno.EPERM,errno.EACCES,errno.EROFS): raise
    print(json.dumps({"outside_write_denied":True,"errno":e.errno}))
else:
    raise RuntimeError("outside write allowed")
finally:
    if created: os.unlink(p)
"""


def _config_observation(cwd, env):
    home = Path(env.get("CODEX_HOME", str(Path(env.get("HOME", "")) / ".codex")))
    paths = {home / "config.toml", Path("/etc/codex/requirements.toml"), Path("/etc/codex/config.toml")}
    paths.update(p / ".codex/config.toml" for p in (Path(cwd), *Path(cwd).parents))
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None for p in sorted(paths)}


def _effective_environment(env, logical_keys=None):
    if logical_keys is None:
        return dict(env)
    # Omit only this call's supervisor-added tags. Caller-provided variables,
    # including ones with the same prefix, remain bound and invalidate on drift.
    extras = set(env) - set(logical_keys)
    if any(not key.startswith("WORKBAY_EXECUTION_PROBE_") or env[key] != "1" for key in extras):
        raise Refused("execution_context_changed")
    return {key: env[key] for key in logical_keys}


def _live_context(binary, argv, cwd, output_dir, env, logical_keys=None):
    roots = _roots(cwd, output_dir, env)
    rc, source = _child(["git", "rev-parse", "HEAD", "HEAD^{tree}"], deadline=time.monotonic() + 2, env=env, cwd=cwd)
    if rc:
        raise Refused("source_identity_unavailable")
    source = source.splitlines()
    return {
        "source": source,
        "roots": [{k: v for k, v in _file_identity(p).items() if k not in {"mtime_ns", "size"}} for p in roots],
        "environment_digest": _digest(_effective_environment(env, logical_keys)),
        "binary": _cli_identity(binary),
        "interpreter": _executable_identity(sys.executable),
        "argv_digest": _digest(argv),
        "config_files": _config_observation(cwd, env),
        "implementation": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (
                Path(__file__),
                Path(__file__).with_name("codex_lane_config.py"),
                Path(__file__).with_name("preflight_attestation.py"),
                Path(__file__).with_name("backend_spec.py"),
            )
        },
        "owner_uid": os.getuid(),
    }


def check_execution_context(proof, binary, argv, *, cwd, output_dir, env):
    # Directory mtime changes when our own fresh probe files are unlinked;
    # compare stable ownership/inode/mode, not directory modification clocks.
    current = _live_context(binary, argv, cwd, output_dir, env)
    owner = proof.get("owner", {})
    if owner != {"pid": os.getpid(), "uid": os.getuid(), "start": Path("/proc/self/stat").read_text().split()[21]}:
        return False
    return proof.get("live_context") == current


def _observe(binary, argv, cwd, output_dir, env, deadline, receipt):
    checks = receipt.setdefault("native_checks", [])
    cwd = Path(cwd).resolve(strict=True)
    if Path.cwd().resolve() != cwd:
        raise Refused("cwd_mismatch")
    # exec's explicit -C must resolve to the process cwd used by this probe.
    if sum(a in {"-C", "--cd"} or a.startswith(("-C=", "--cd=")) for a in argv) > 1:
        raise Refused("cwd_mismatch")
    if any(a.startswith("-C") and a != "-C" and not a.startswith("-C=") for a in argv):
        raise Refused("cwd_mismatch")
    requested_cwd = _option(argv, "-C", "--cd")
    if requested_cwd and (cwd / requested_cwd).resolve() != cwd:
        raise Refused("cwd_mismatch")
    tmp = env.get("TMPDIR")
    if not tmp or not Path(tmp).is_dir():
        raise Refused(
            "tmpdir_missing",
            repair="Create the lane TMPDIR and export it in the final agent environment before preflight.",
        )
    if not Path(tmp).is_absolute():
        raise Refused("tmpdir_not_absolute")
    homes = {Path(env.get("HOME", "/")), Path(pwd.getpwuid(os.getuid()).pw_dir)}
    preliminary = [cwd, Path(output_dir).resolve(strict=True), Path(tmp).resolve(strict=True)]
    if any(home.is_relative_to(root) or root == Path("/") for root in preliminary for home in homes):
        raise Refused("sandbox_roots_overbroad")
    rc, git_dir = _child(
        ["git", "rev-parse", "--absolute-git-dir"],
        deadline=deadline,
        env=env,
        cwd=cwd,
        evidence=checks,
        evidence_dir=output_dir,
    )
    if rc or not Path(git_dir.strip()).is_dir():
        raise Refused("git_root_unavailable")
    roots = list(
        map(
            str,
            [
                cwd,
                Path(output_dir).resolve(strict=True),
                Path(tmp).resolve(strict=True),
                Path(git_dir.strip()).resolve(strict=True),
            ],
        )
    )
    homes = {Path(env.get("HOME", "/")), Path(pwd.getpwuid(os.getuid()).pw_dir)}
    if any(home.is_relative_to(Path(p)) or p == "/" for p in roots for home in homes):
        raise Refused("sandbox_roots_overbroad")
    receipt["roots"] = [_file_identity(p) for p in roots]
    receipt["environment_digest"] = _digest(
        _effective_environment(env, receipt["environment_keys"])
    )  # values never printed
    receipt["interpreter"] = _executable_identity(sys.executable)
    receipt["interpreter"].update(
        executable=sys.executable, prefix=sys.prefix, base_prefix=sys.base_prefix, version=sys.version
    )
    receipt["cwd"] = str(cwd)
    receipt["runtime"] = _runtime_observation(cwd)
    receipt["source"] = {}
    for field, command in [("commit", ["git", "rev-parse", "HEAD"]), ("tree", ["git", "rev-parse", "HEAD^{tree}"])]:
        rc, text = _child(command, deadline=deadline, env=env, cwd=cwd, evidence=checks, evidence_dir=output_dir)
        if rc:
            raise Refused("source_identity_unavailable")
        receipt["source"][field] = text.strip()
    observed_binary = shutil.which(binary, path=env.get("PATH"))
    if not observed_binary:
        raise Refused("executable_missing")
    receipt["binary"] = _cli_identity(observed_binary)
    rc, version = _child(
        [observed_binary, "--version"], deadline=deadline, env=env, cwd=cwd, evidence=checks, evidence_dir=output_dir
    )
    receipt["executable_version"] = version.strip()
    if rc or version.strip() != "codex-cli " + receipt["runtime"]["expected_executable_version"]:
        raise Refused(
            "executable_version_mismatch",
            repair="Reprovision the pinned Codex executable and restart the lane runtime; do not refresh metadata alone.",
        )
    receipt["capacity_context"] = {"load_average": list(os.getloadavg()), "cpu_count": os.cpu_count()}
    try:
        available = next(
            line for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemAvailable:")
        )
        receipt["capacity_context"]["memory_available_bytes"] = int(available.split()[1]) * 1024
    except (OSError, StopIteration, ValueError):
        receipt["capacity_context"]["memory_available_bytes"] = "UNKNOWN"
    receipt["free_bytes"] = {p: shutil.disk_usage(p).free for p in roots}
    if min(receipt["free_bytes"].values()) < MIN_FREE_BYTES:
        raise Refused(
            "capacity_disk_low",
            retryable=True,
            repair="Wait for coordinator-owned capacity recovery; retain all custody and liveness holds.",
        )
    # Host checks are diagnostics ONLY. They never constitute sandbox PASS.
    rc, observed = _child(
        [sys.executable, "-c", WRITE_CHILD, *roots],
        deadline=deadline,
        env=env,
        cwd=cwd,
        evidence=checks,
        evidence_dir=output_dir,
    )
    if rc:
        raise Refused(
            "host_write_denied",
            status="FAIL",
            repair="Provision write access to the exact cwd, OUTPUT, TMPDIR and Git directory without widening sandbox policy.",
        )
    receipt["host_writes"] = json.loads(observed)
    live_before = _live_context(observed_binary, argv, cwd, output_dir, env, receipt["environment_keys"])
    if live_before["environment_digest"] != receipt["requested_environment_digest"]:
        raise Refused("execution_context_changed")
    profile, selectors = _policy(argv, roots, capability=receipt["capability"])
    checks = receipt.setdefault("native_checks", [])
    for command, required in [
        ("sandbox", ("--permission-profile", "--include-managed-config", "--cd")),
        ("exec", ("--ignore-user-config", "--config", "--cd")),
    ]:
        rc, help_text = _child(
            [observed_binary, command, "--help"],
            deadline=deadline,
            env=env,
            cwd=cwd,
            evidence=checks,
            evidence_dir=output_dir,
        )
        if rc or not all(flag in help_text for flag in required):
            raise Refused("sandbox_backend_unsupported")
    base = [
        observed_binary,
        "sandbox",
        "--permission-profile",
        profile,
        *selectors,
        "--include-managed-config",
        "--cd",
        str(cwd),
        "--",
        sys.executable,
        "-c",
    ]
    receipt["policy"] = {"profile": profile, "argv_digest": _digest(base), "config": selectors}
    rc, observed = _child(
        [*base, READ_ONLY_CHILD if receipt["capability"] == ANALYSIS_CAPABILITY else WRITE_CHILD, *roots],
        deadline=deadline, env=env, cwd=cwd, evidence=checks, evidence_dir=output_dir
    )
    if rc:
        raise Refused("sandbox_probe_failed")
    if receipt["capability"] == ANALYSIS_CAPABILITY:
        denials = json.loads(observed)
        if not valid_readonly_denials(denials, roots):
            raise Refused("sandbox_probe_malformed")
        receipt["sandbox_readonly"] = denials
        receipt["sandbox_writes"] = []
    else:
        writes = json.loads(observed)
        if writes != [{"root": p, "create_read_unlink": True} for p in roots] or any(
            row.get("create_read_unlink") is not True for row in writes
        ):
            raise Refused("sandbox_probe_malformed")
        receipt["sandbox_writes"] = writes
    # Choose an existing outside parent, use exclusive create, never touch a
    # pre-existing file. This check MUST fail inside the real native sandbox.
    outside = cwd.parent / (".workbay-outside-" + uuid.uuid4().hex)
    if any(outside.is_relative_to(Path(p)) for p in roots) or str(cwd) == env.get("HOME"):
        raise Refused("sandbox_outside_control_unavailable")
    rc, control = _child(
        [sys.executable, "-c", WRITE_CHILD, str(cwd.parent)],
        deadline=deadline,
        env=env,
        cwd=cwd,
        evidence=checks,
        evidence_dir=output_dir,
    )
    expected_control = [{"root": str(cwd.parent), "create_read_unlink": True}]
    if rc or json.loads(control) != expected_control:
        raise Refused("sandbox_outside_control_unavailable")
    receipt["outside_host_control"] = expected_control
    rc, observed = _child(
        [*base, NEGATIVE_CHILD, str(outside)],
        deadline=deadline,
        env=env,
        cwd=cwd,
        evidence=checks,
        evidence_dir=output_dir,
    )
    if rc:
        raise Refused("sandbox_outside_write_allowed")
    negative = json.loads(observed)
    if (
        negative.get("outside_write_denied") is not True
        or type(negative.get("errno")) is not int
        or negative.get("errno") not in (1, 13, 30)
    ):
        raise Refused("sandbox_probe_malformed")
    receipt["sandbox_negative"] = negative
    receipt["live_context"] = _live_context(observed_binary, argv, cwd, output_dir, env, receipt["environment_keys"])
    for field, suffix in (("commit", "HEAD"), ("tree", "HEAD^{tree}")):
        rc, source_now = _child(
            ["git", "rev-parse", suffix], deadline=deadline, env=env, cwd=cwd, evidence=checks, evidence_dir=output_dir
        )
        if rc or source_now.strip() != receipt["source"][field]:
            raise Refused("source_identity_changed")
    if receipt["live_context"] != live_before:
        raise Refused("execution_context_changed")


def refusal_receipt(reason, *, binding=None, error_type=None):
    stamp = time.time()
    proof = {
        "schema": SCHEMA,
        "status": "UNKNOWN",
        "reason": reason,
        "retryable": False,
        "cache_reused": False,
        "binding": binding or {},
        "started_at": stamp,
        "finished_at": stamp,
        "expires_at": stamp + 20,
        "deadline_seconds": 20,
        "error_type": error_type,
    }
    proof["evidence_digest"] = _digest(proof)
    return proof


def refusal_effect(proof):
    return {"schema": "workbay/native-execution-effect@1", "effect": "pre_model_refusal", "rc": 7,
            "proof_sha256": _digest(proof), "binding": proof.get("binding", {})}


def refusal_marker(proof):
    return {
        "schema": "workbay/execution-preflight-refusal@1",
        "phase": "execution_preflight",
        "exit_code": 7,
        "model_boundary_reached": False,
        "reason": proof["reason"],
        "evidence_digest": _digest(proof),
    }


def _worker(request):
    receipt = {
        "schema": SCHEMA,
        "capability": request["capability"],
        "status": "UNKNOWN",
        "reason": "probe_incomplete",
        "retryable": False,
        "config_digest": _digest(request),
        "binding": request.get("binding", {}),
        "started_at": time.time(),
        "deadline_seconds": request["timeout"],
        "cache_reused": False,
        "owner": request["owner"],
        "environment_keys": request["environment_keys"],
        "requested_environment_digest": request["environment_digest"],
        "scope": "native sandbox provisioning; no model call; same-uid supervision only",
    }
    deadline = time.monotonic() + request["timeout"]
    try:
        _observe(
            request["binary"], request["argv"], request["cwd"], request["output_dir"], os.environ, deadline, receipt
        )
        receipt.update(status="PASS", reason=("native_read_only" if request["capability"] == ANALYSIS_CAPABILITY
                                              else "native_create_read_unlink"), retryable=False)
    except Refused as exc:
        receipt.update(status=exc.status, reason=exc.reason, retryable=exc.retryable, repair_hint=exc.repair)
    except Exception as exc:  # typed unknown, never infer a grant after an error
        receipt.update(
            reason="probe_exception",
            error_type=type(exc).__name__,
            retryable=True,
            repair_hint="Inspect exact runtime and directory provisioning; no model was called.",
        )
    receipt["finished_at"] = time.time()
    from .codex_lane_config import PREFLIGHT_ATTESTATION_TTL_S

    receipt["expires_at"] = receipt["started_at"] + min(PREFLIGHT_ATTESTATION_TTL_S, request["timeout"])
    receipt["evidence_digest"] = _digest(receipt)
    return receipt


def run_execution_preflight(binary, argv, *, cwd, output_dir, env=None, timeout=DEFAULT_TIMEOUT, binding=None,
                            capability=WRITE_CAPABILITY):
    """Observe in a bounded child; no author-supplied receipt is accepted."""
    if not math.isfinite(timeout) or timeout <= 0 or timeout > 60:
        raise ValueError("preflight timeout must be finite in (0,60]")
    effective_env = dict(os.environ if env is None else env)
    request = dict(
        capability=capability,
        environment_keys=sorted(effective_env),
        environment_digest=_digest(effective_env),
        binary=str(binary),
        argv=list(argv),
        cwd=str(cwd),
        output_dir=str(output_dir),
        timeout=timeout,
        binding=binding or {},
        owner={"pid": os.getpid(), "uid": os.getuid(), "start": Path("/proc/self/stat").read_text().split()[21]},
    )
    started = time.time()
    deadline = time.monotonic() + timeout
    try:
        # Import of the runtime and its companions happens inside this clock.
        code = "import json,sys; from workbay_orchestrator_mcp.orchestration.execution_preflight import _worker; print(json.dumps(_worker(json.loads(sys.argv[1]))))"
        rc, data = _child(
            [sys.executable, "-c", code, json.dumps(request)], deadline=deadline, env=effective_env, cwd=cwd
        )
        result = json.loads(data) if rc == 0 else None
        if not isinstance(result, dict) or result.get("schema") != SCHEMA:
            raise Refused(
                "runtime_handshake_failed",
                repair="Reprovision the lane interpreter with the exact source and all locked companion imports.",
            )
        return result
    except (Refused, OSError, ValueError, subprocess.SubprocessError) as exc:
        result = refusal_receipt(
            getattr(exc, "reason", "runtime_handshake_failed"), binding=binding, error_type=type(exc).__name__
        )
        result.update(
            expires_at=started + timeout,
            started_at=started,
            finished_at=time.time(),
            deadline_seconds=timeout,
            retryable=getattr(exc, "retryable", True),
            repair_hint=getattr(exc, "repair", "Repair the exact lane runtime before model dispatch."),
            config_digest=_digest(request),
        )
        result.pop("evidence_digest", None)
        result["evidence_digest"] = _digest(result)
        return result
