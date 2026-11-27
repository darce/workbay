#!/bin/bash -p
set -euo pipefail
exec /usr/bin/python3 -I -B - "${BASH_SOURCE[0]}" "$@" <<'PY'
import argparse
import hashlib
import json
import math
import os
import re
import select as kernel_select  # `select` names the routing function below.
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SYSTEM_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": "/nonexistent"}
SYSTEM_ENV.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null")
HEX_SHA = r"(?!0{40})[0-9a-f]{40}"
ID = r"[a-z0-9][a-z0-9-]{0,63}"
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
LAUNCHER_TEARDOWN_GRACE = 30
SANDBOX_VERIFICATION_TIMEOUT = 10
ROUTES = {f"scripts/ci/checks/{name}.sh" for name in ("frontend", "backend", "gpu-snapshot", "deploy-contract", "workbay-gate", "workbay-release-check")}
# Only the demo deploy still verifies a signed release-train receipt;
# deploy-recognition-gate runs scripts/ci/checks/deploy-contract.sh on the VM.
RELEASE_CHECKS = {"deploy-demo-gate": ("deploy-demo", None)}
# Release-only checks (the policy's release_only section) run only on the release path:
# scripts/deploy/vm-release-ship.sh runs each at its ship stage. They are never planned or
# run here, so per-push VM CI never starts one; asking to run one is refused.
RELEASE_ONLY_STAGES = ("doctor", "rehearsal")
RELEASE_ONLY_SCRIPT = r"scripts/(ci/checks|deploy)/[a-z0-9][a-z0-9-]*\.sh"
MAX_JSON = 2 * 1024 * 1024

# Site facts (schema ids) come from the VM CI site configuration through the loader
# beside this router (its tree's config/vm-ci/site.json, or $VMCI_SITE_CONFIG); both
# travel with the router into an installed release.
sys.path.insert(0, str(Path(sys.argv[1]).resolve().parent))
try:
    import vm_ci_site
    SITE = vm_ci_site.load()
except (ImportError, ValueError, OSError) as error:
    print(f"vm-check: site_unavailable: {str(error).splitlines()[0] if str(error) else type(error).__name__}",
          file=sys.stderr)
    sys.exit(3)
SCHEMA = {name: SITE[f"schema_vm_check_{name.replace('-', '_')}"]
          for name in ("routing", "event", "plan", "claim", "result", "release-policy")}


class Refused(Exception):
    pass


class Cancelled(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Refused(reason)


def matches(pattern, value):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def fields(value, names):
    return isinstance(value, dict) and set(value) == set(names.split())


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def read_bytes(path, limit=MAX_JSON):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        require(stat.S_ISREG(os.fstat(fd).st_mode), "input_not_regular")
        content = source.read(limit + 1)
    require(len(content) <= limit, "input_too_large")
    return content


def decode(content):
    return json.loads(content, object_pairs_hook=unique_keys)


def encode(data):
    return (json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(content):
    return hashlib.sha256(content).hexdigest()


def path_pattern(glob):
    # The retained workflows use only *, ** and literal characters.
    return "".join({"*": "[^/]*", "**": ".*"}.get(part, re.escape(part))
                   for part in re.findall(r"\*\*|\*|[^*]+", glob))


def read_policy(path):
    content = read_bytes(path)
    policy = decode(content)
    require(isinstance(policy, dict) and set(policy) in ({"schema", "workflows"}, {"schema", "workflows", "release_only"})
            and policy["schema"] == SCHEMA["routing"]
            and isinstance(policy["workflows"], dict) and policy["workflows"], "routing_schema")
    checks = set()
    for name, workflow in policy["workflows"].items():
        require(matches(ID, name) and fields(workflow, "events checks"), "workflow_schema")
        events, jobs = workflow["events"], workflow["checks"]
        require(isinstance(events, dict) and events and isinstance(jobs, dict) and jobs, "workflow_schema")
        for event, filters in events.items():
            if event == "workflow_dispatch":
                require(filters == {}, "dispatch_filters")
                continue
            require(event in ("pull_request", "push") and isinstance(filters, dict)
                    and set(filters) <= {"branches", "paths"} and "paths" in filters, "event_filters")
            require(all(isinstance(values, list) and values and all(
                matches(r"[A-Za-z0-9_./*-]+", value)
                and ("**" not in value or value.endswith("/**") and "**" not in value[:-3])
                for value in values
            ) for values in filters.values()), "event_filters")
            require(all("*" not in branch for branch in filters.get("branches", [])), "branch_filters")
        for check, job in jobs.items():
            require(matches(ID, check) and check not in checks
                    and fields(job, "hosted_name script timeout_seconds")
                    and (job["script"] is None or job["script"] in ROUTES)
                    and (isinstance(job["hosted_name"], str) and job["hosted_name"].strip()
                         or job["hosted_name"] is None and job["script"] is None)
                    and type(job["timeout_seconds"]) is int
                    and 0 < job["timeout_seconds"] <= 21600, "check_schema")
            if check in RELEASE_CHECKS or job["script"] is None:
                require(job["script"] is None and RELEASE_CHECKS.get(check) == (name, job["hosted_name"]),
                        "release_route_required")
            checks.add(check)
    release_only = read_release_only(policy.get("release_only", {}), checks)
    return policy["workflows"], digest(content), release_only


def read_release_only(section, routed):
    """The release-only checks: never planned, never run here (see RELEASE_ONLY_STAGES)."""
    require(isinstance(section, dict), "release_only_schema")
    for check, job in section.items():
        require(matches(ID, check) and check not in routed and check not in RELEASE_CHECKS
                and fields(job, "stage command train_args timeout_seconds reason")
                and job["stage"] in RELEASE_ONLY_STAGES
                and isinstance(job["command"], list) and job["command"]
                and all(isinstance(arg, str) and matches(r"[A-Za-z0-9_.=-]+", arg) for arg in job["command"][1:])
                and matches(RELEASE_ONLY_SCRIPT, job["command"][0])
                and type(job["train_args"]) is bool
                and type(job["timeout_seconds"]) is int and 0 < job["timeout_seconds"] <= 21600
                and isinstance(job["reason"], str) and job["reason"].strip(), "release_only_schema")
    return set(section)


def read_event(path):
    event = decode(read_bytes(path))
    require(isinstance(event, dict), "event_schema")
    common = "schema repository delivery_id event ref"
    kind = event.get("event")
    extra = {
        "pull_request": "action number base_ref base_sha head_sha merge_sha diff",
        "push": "before_sha after_sha diff",
        "workflow_dispatch": "workflow sha",
    }
    require(kind in extra and fields(event, common + " " + extra[kind])
            and event["schema"] == SCHEMA["event"]
            and matches(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", event["repository"])
            and matches(ID, event["delivery_id"])
            and matches(r"refs/[^\x00-\x20\x7f]+", event["ref"]), "event_schema")
    if kind == "pull_request":
        require(type(event["number"]) is int and event["number"] > 0
                and event["ref"] == f"refs/pull/{event['number']}/merge"
                and matches(r"[a-z_]+", event["action"])
                and matches(r"refs/heads/[^\x00-\x20\x7f]+", event["base_ref"])
                and matches(HEX_SHA, event["base_sha"]) and matches(HEX_SHA, event["head_sha"])
                and (event["merge_sha"] is None or matches(HEX_SHA, event["merge_sha"])
                     and event["merge_sha"] not in (event["base_sha"], event["head_sha"])), "pr_identity")
    elif kind == "push":
        require(matches(r"[0-9a-f]{40}", event["before_sha"])
                and matches(r"[0-9a-f]{40}", event["after_sha"]), "push_identity")
    else:
        require(matches(ID, event["workflow"]) and matches(HEX_SHA, event["sha"]), "dispatch_identity")
        return event
    diff = event["diff"]
    require(fields(diff, "mode paths") and diff["mode"] in ("paths", "force", "unknown")
            and isinstance(diff["paths"], list) and len(diff["paths"]) <= 300
            and (diff["mode"] == "paths" or not diff["paths"])
            and all(isinstance(path, str) and "\0" not in path
                    and all(part not in ("", ".", "..") for part in path.split("/"))
                    for path in diff["paths"]), "event_diff")
    return event


def select(event, workflow, filters):
    kind = event["event"]
    if filters is None:
        return "event_filtered"
    if kind == "workflow_dispatch":
        return "selected" if event["workflow"] == workflow else "workflow_filtered"
    if kind == "pull_request":
        if event["action"] not in ("opened", "synchronize", "reopened"):
            return "action_filtered"
        branch = event["base_ref"].removeprefix("refs/heads/")
    else:
        if event["after_sha"] == "0" * 40 or not event["ref"].startswith("refs/heads/"):
            return "ref_filtered"
        branch = event["ref"].removeprefix("refs/heads/")
    if "branches" in filters and branch not in filters["branches"]:
        return "branch_filtered"
    if kind == "pull_request" and event["merge_sha"] is None:
        return "merge_unavailable"
    diff = event["diff"]
    if diff["mode"] == "unknown":
        return "diff_unavailable"
    patterns = [re.compile(path_pattern(glob), re.DOTALL) for glob in filters["paths"]]
    if diff["mode"] == "force" or any(pattern.fullmatch(path) for path in diff["paths"] for pattern in patterns):
        return "selected"
    return "path_filtered"


def plan(workflows, event):
    sha = event.get("merge_sha", event.get("after_sha", event.get("sha")))
    if sha == "0" * 40:
        sha = None
    jobs = {}
    for workflow, policy in workflows.items():
        reason = select(event, workflow, policy["events"].get(event["event"]))
        for check, job in policy["checks"].items():
            jobs[check] = dict(job, workflow=workflow, selection=reason,
                               tested_sha=sha, pr_head_sha=event.get("head_sha"))
    return jobs


def private_directory(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid()
            and not info.st_mode & 0o077, "store_must_be_private")


def sync_directory(path):
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_record(directory, name, record):
    with tempfile.NamedTemporaryFile(dir=directory, delete=False) as staged:
        staged_path = Path(staged.name)
        try:
            staged.write(encode(record))
            staged.flush()
            os.fsync(staged.fileno())
            os.link(staged_path, directory / name)
        finally:
            staged_path.unlink()
    sync_directory(directory)


def cancel(signum, frame):
    raise Cancelled


def wait_ready(poller, deadline):
    # One kernel wait returns whichever registered source fires first. An empty
    # result means the deadline passed with no exit or cancellation pending.
    while True:
        remaining = deadline - time.monotonic()
        ready = {fd for fd, _ in poller.poll(max(0, math.ceil(remaining * 1000)))}
        if ready or remaining <= 0:
            return ready


def cancellation_pending(cancelled, wake_read):
    # The wakeup descriptor carries one byte per caught signal number. Drain it so
    # only a cancellation signal, not some other caught signal, counts.
    pending = bool(cancelled)
    while True:
        try:
            received = os.read(wake_read, 512)
        except BlockingIOError:
            return pending
        if not received:
            return pending
        pending = pending or any(number in SIGNALS for number in received)


def launch(argv, cwd, seconds, termination_grace, cancelled, wake_read):
    # Return the launcher status, or the interruption to raise after cleanup.
    with subprocess.Popen(argv, cwd=cwd, env=SYSTEM_ENV, stdin=subprocess.DEVNULL,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          start_new_session=True) as process:
        interruption = None
        try:
            deadline = time.monotonic() + seconds
            # The session leader stays unreaped until its process group is
            # killed, so the group ID cannot be reused before cleanup.
            child = os.pidfd_open(process.pid)
            try:
                poller = kernel_select.poll()
                poller.register(child, kernel_select.POLLIN)
                poller.register(wake_read, kernel_select.POLLIN)
                while True:
                    ready = wait_ready(poller, deadline)
                    # Decide by the monotonic clock read right after the wake. A
                    # supervisor paused across the deadline may wake to a ready
                    # exit it never observed in time; that exit is overdue.
                    overdue = time.monotonic() >= deadline
                    if cancellation_pending(cancelled, wake_read):
                        interruption = Cancelled()
                    elif overdue:
                        interruption = subprocess.TimeoutExpired(argv, seconds)
                    elif child in ready:
                        break
                    else:
                        continue  # Another caught signal woke the wait.
                    break
                if interruption is not None and termination_grace:
                    os.kill(process.pid, signal.SIGTERM)
                    exit_only = kernel_select.poll()
                    exit_only.register(child, kernel_select.POLLIN)
                    wait_ready(exit_only, time.monotonic() + termination_grace)
            finally:
                os.close(child)
        finally:
            # Every outcome removes the launcher's remaining process group.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            status = process.wait()
        return status if interruption is None else interruption


def supervise(argv, cwd, seconds, *, termination_grace=0):
    # Install a non-raising handler and a wakeup descriptor before spawning so a
    # launch cannot lose its child handle and a signal cannot be missed by poll.
    cancelled = []
    previous = {}
    previous_wakeup = None
    result = None
    wake_read, wake_write = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        previous_wakeup = signal.set_wakeup_fd(wake_write, warn_on_full_buffer=False)
        for sig in SIGNALS:
            previous[sig] = signal.signal(sig, lambda signum, frame: cancelled.append(signum))
        if not cancelled:
            result = launch(argv, cwd, seconds, termination_grace, cancelled, wake_read)
    finally:
        # Block cancellation while deciding and restoring. A cancellation received
        # at any point before this decision, including after the launcher exited,
        # is reported rather than lost to the non-raising handler; one arriving
        # later reaches the restored raising handler after the unblock.
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            if cancellation_pending(cancelled, wake_read):
                result = Cancelled()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            if previous_wakeup is not None:
                signal.set_wakeup_fd(previous_wakeup)
            os.close(wake_read)
            os.close(wake_write)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
    if isinstance(result, BaseException):
        raise result
    return result


def absolute_file(path):
    require(path is not None and path.is_absolute() and path.is_file(), "trusted_input_missing")
    return path


def verify_release(args, claim, scratch, budget):
    started = time.monotonic()
    policy_bytes = read_bytes(absolute_file(args.release_policy))
    signature = read_bytes(absolute_file(args.policy_signature), 64)
    require(len(signature) == 64, "invalid_policy_signature")
    policy_file = scratch / "signed-policy.json"
    signature_file = scratch / "policy.sig"
    policy_file.write_bytes(policy_bytes)
    signature_file.write_bytes(signature)
    status = supervise([
        "/usr/bin/openssl", "pkeyutl", "-verify", "-pubin", "-rawin",
        "-inkey", str(absolute_file(args.policy_key)), "-in", str(policy_file),
        "-sigfile", str(signature_file),
    ], scratch, min(10, budget))
    require(status == 0, "invalid_policy_signature")
    signed = decode(policy_bytes)
    require(fields(signed, "schema repository check execution_policy")
            and signed["schema"] == SCHEMA["release-policy"]
            and signed["repository"] == claim["repository"] and signed["check"] == claim["check"]
            and isinstance(signed["execution_policy"], dict), "wrong_release_policy")
    policy = signed["execution_policy"]
    require(isinstance(policy.get("version"), str) and policy["version"].startswith(claim["check"] + "."),
            "wrong_release_policy_version")
    require(type(policy.get("timeout_seconds")) is int and 0 < policy["timeout_seconds"] <= budget,
            "wrong_release_policy_budget")
    request_bytes = read_bytes(absolute_file(args.release_request))
    receipt_bytes = read_bytes(absolute_file(args.release_receipt))
    request = decode(request_bytes)
    require(isinstance(request, dict) and request.get("run_id") == claim["attempt_id"], "wrong_release_attempt")
    request_file, execution_file, receipt_file = (scratch / name for name in ("request", "policy", "receipt"))
    request_file.write_bytes(request_bytes)
    execution_file.write_bytes(encode(policy))
    receipt_file.write_bytes(receipt_bytes)
    argv = [str(absolute_file(args.receipt_verifier)), "release-receipt", "verify",
            "--request", str(request_file), "--policy", str(execution_file),
            "--receipt", str(receipt_file), "--public-key", str(absolute_file(args.receipt_key)),
            "--commit", claim["tested_sha"]]
    status = supervise(argv, scratch, budget - (time.monotonic() - started))
    require(status == 0, "release_receipt_refused")
    return dict(signed_policy_sha256=digest(policy_bytes), request_sha256=digest(request_bytes),
                receipt_sha256=digest(receipt_bytes))


def execute(args, claim, job, scratch):
    selection = job["selection"]
    if selection != "selected":
        outcome = "unavailable" if selection.endswith("_unavailable") else "filtered"
        return outcome, selection, None, None
    if job["script"] is None:
        evidence = verify_release(args, claim, scratch, job["timeout_seconds"])
        return "passed", "verified_release_receipt", 0, evidence
    launcher = absolute_file(args.launcher)
    status = supervise([str(launcher), "--repository", claim["repository"],
                        "--commit", claim["tested_sha"], "--script", job["script"],
                        "--check", claim["check"], "--attempt", claim["attempt_id"]],
                       scratch, job["timeout_seconds"], termination_grace=LAUNCHER_TEARDOWN_GRACE)
    outcome, reason = {
        0: ("passed", "check_passed"),
        1: ("failed", "check_failed"),
        3: ("unavailable", "launcher_unavailable"),
        # EX_TEMPFAIL: the launcher's disk floor refused the check. Unavailable,
        # never failed, so the poller holds the cursor and retries later.
        75: ("unavailable", "disk_floor"),
        124: ("deadline", "launcher_deadline"),
        130: ("cancelled", "launcher_cancelled"),
        143: ("cancelled", "launcher_cancelled"),
    }.get(status, ("unavailable", "launcher_status_unknown"))
    return outcome, reason, status, None


def confirm_sandbox_teardown(args, repository):
    verifier = absolute_file(args.sandbox_verifier)
    try:
        status = supervise([str(verifier), "--repository", repository, "--attempt", args.retry_of],
                           args.store, SANDBOX_VERIFICATION_TIMEOUT)
    except (OSError, subprocess.TimeoutExpired):
        raise Refused("sandbox_teardown_unconfirmed") from None
    require(status == 0, "sandbox_teardown_unconfirmed")


def run(args, event, jobs, policy_hash, release_only):
    # A release-only check is refused before anything is claimed: only the release path runs it.
    require(args.check not in release_only, "release_only_check")
    require(args.check in jobs and matches(r"[0-9a-f]{32}", args.attempt), "invalid_attempt")
    require(args.store.is_absolute(), "store_must_be_absolute")
    private_directory(args.store)
    job = jobs[args.check]
    identity = dict(repository=event["repository"], delivery_id=event["delivery_id"],
                    event_sha256=digest(encode(event)), routing_sha256=policy_hash,
                    workflow=job["workflow"], check=args.check, hosted_name=job["hosted_name"],
                    tested_sha=job["tested_sha"], pr_head_sha=job["pr_head_sha"])
    if args.retry_of is not None:
        require(matches(r"[0-9a-f]{32}", args.retry_of) and args.attempt != args.retry_of, "invalid_retry")
        previous = decode(read_bytes(args.store / args.retry_of / "claim.json"))
        require(all(previous.get(key) == value for key, value in identity.items()), "retry_identity_mismatch")
        if job["selection"] == "selected" and job["script"] is not None:
            confirm_sandbox_teardown(args, event["repository"])
    directory = args.store / args.attempt
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        raise Refused("attempt_already_claimed") from None
    sync_directory(args.store)
    claim = dict(identity, schema=SCHEMA["claim"], attempt_id=args.attempt, retry_of=args.retry_of,
                 event=event["event"], ref=event["ref"], pr_number=event.get("number"),
                 pr_base_sha=event.get("base_sha"), selection=job["selection"], started_at=int(time.time()))
    write_record(directory, "claim.json", claim)
    status = evidence = None
    try:
        with tempfile.TemporaryDirectory(prefix="work-", dir=directory) as temp:
            outcome, reason, status, evidence = execute(args, claim, job, Path(temp))
    except Cancelled:
        outcome, reason = "cancelled", "supervisor_cancelled"
    except subprocess.TimeoutExpired:
        outcome, reason = "deadline", "supervisor_deadline"
    except Refused as error:
        outcome, reason = "unavailable", str(error)
    except (OSError, ValueError, TypeError, RecursionError):
        outcome, reason = "unavailable", "input_or_execution_error"
    result = dict(claim, schema=SCHEMA["result"], finished_at=int(time.time()), outcome=outcome,
                  reason=reason, exit_status=status, receipt_evidence=evidence)
    write_record(directory, "result.json", result)
    print(outcome)
    return {"passed": 0, "failed": 1}.get(outcome, 3)


def main():
    installed = Path(sys.argv[1]).resolve().parent
    parser = argparse.ArgumentParser(prog="run-vm-check.sh")
    parser.add_argument("--event", required=True, type=Path)
    parser.add_argument("--policy", type=Path, default=installed / "vm-check-policy.json")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    runner = commands.add_parser("run")
    for flag in ("check", "attempt"):
        runner.add_argument("--" + flag, required=True)
    runner.add_argument("--retry-of")
    runner.add_argument("--store", required=True, type=Path)
    for flag in ("launcher", "sandbox-verifier", "release-policy", "policy-signature", "policy-key",
                 "release-request", "release-receipt", "receipt-key"):
        runner.add_argument("--" + flag, type=Path)
    runner.set_defaults(receipt_verifier=installed.parent / "remote_gate.sh")
    args = parser.parse_args(sys.argv[2:])
    for sig in SIGNALS:
        signal.signal(sig, cancel)
    try:
        event = read_event(args.event)
        workflows, policy_hash, release_only = read_policy(args.policy)
        jobs = plan(workflows, event)
        if args.command == "run":
            return run(args, event, jobs, policy_hash, release_only)
        print(encode(dict(schema=SCHEMA["plan"], event=event, routing_sha256=policy_hash,
                          checks=jobs)).decode(), end="")
        return 0
    except Refused as error:
        print(f"vm-check: {error}", file=sys.stderr)
    except Cancelled:
        print("vm-check: cancelled", file=sys.stderr)
    except (OSError, ValueError, TypeError, RecursionError):
        print("vm-check: input_or_storage_error", file=sys.stderr)
    return 3


sys.exit(main())
PY
