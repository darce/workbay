#!/usr/bin/python3 -IB
"""Freeze pushes from a VM-side bare mirror and drive the trusted VM check router.

One poll handles at most one frozen push event. It holds state-dir/poll.lock for
the whole poll, so a concurrent poll exits 0 without work. State files:

- cursor.json: the last processed ``after_sha`` and the delivery ID of the frozen
  event still in progress (``pending``). The cursor advances only after every
  selected check of that event has a terminal router result. A retry keeps
  ``after_sha`` fixed and records the start of each fresh budget in ``retry_from``.
- events/<delivery_id>.json: the immutable frozen ``<slug>.vm-check-event/1`` push.
- runs/<delivery_id>.json: the frozen routing and every attempt with its router
  exit. The attempts are the durable retry count: a check whose router exit is
  neither 0 (passed) nor 1 (failed) is retried on the next poll, and after
  VM_CI_MAX_UNAVAILABLE_ATTEMPTS (default 3, 1-20) such outcomes it is failed
  with a recorded reason, so an environment that stays unavailable never holds
  the cursor. --retry-unavailable grants a fresh budget to exhausted checks on
  the last processed push while preserving their earlier attempts.
- deploys/<sha>.intent.json, deploys/<sha>.outcome.json: the dev auto-deploy of a
  main sha. The intent is written once before vm-ci-dev-deploy.sh runs and the
  outcome once after, so a sha is deployed at most once. An intent without an
  outcome (a poll that died mid-deploy) is reported as unknown and never retried.

Dev auto-deploy (--dev-deploy; by default on only when the operator env file
exists) runs once every check of a main push is terminal, deploy-recognition-gate
was routed and passed, and every other check passed. It runs before the cursor
advances, so a restart never skips it; its outcome never holds the cursor.

Routing change: when the router's current routing for a pending delivery differs
from the routing frozen in its run record, the delivery is re-frozen under the
current routing (old and new identity and the reason are recorded, its checks
restart as pending with no attempts) and this poll ends; the next poll runs it.
A delivery that would repeat a re-freeze for the same (old, new) routing pair, or
exceed MAX_REFREEZES, is failed with a recorded reason instead. A completed or
failed delivery is never re-frozen, and a dev deploy intent is never re-run.

Release hold: while the release driver (scripts/deploy/vm-release-ship.sh) has a
release in flight it keeps a hold file at the site's release_hold_path
(state_dir/release-hold.json: {"schema": "<slug>.vm-release-hold/1", "commit":
<40-hex sha>, "placed_at": <epoch>}). While that file exists the poller starts no
dev deploy: checks keep running and the cursor keeps moving, but a deploy that
would start is deferred instead (deploys/deferred.json names it; a newer deferred
deploy replaces an older one) and the poll exits 3. A hold file that is not a
plain readable regular file with that exact content is malformed and still holds
(fail closed), with a message naming the file and the reason. Every report while
a hold exists carries "release_hold". Once the file is gone the next poll runs
the deferred deploy, through the same intent/outcome records (at most once per
sha), as its only work, and removes deploys/deferred.json after the outcome.

Site: the schema ids, the delivery ID namespace, the release hold file and the
dev deploy env file variable and default path come from the VM CI site
configuration, resolved by vm_ci_site.py next to this poller (its
config/vm-ci/site.json, or $VMCI_SITE_CONFIG). Both travel with the poller into
an installed release; a missing or invalid site configuration refuses the poll
before any effect.

Exit status: 0 nothing to do or all passed, 1 a check, the delivery or the dev
deploy failed, 3 a check is still pending, the delivery was just re-frozen, the
comparison is unavailable, a dev deploy outcome is unknown or was deferred by a
release hold, or the poll could not proceed, 2 usage (including an invalid
VM_CI_MAX_UNAVAILABLE_ATTEMPTS or site configuration).
"""

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import pwd
import re
import secrets
import signal
import stat
import subprocess
import sys
import tempfile
import time
import types
import uuid
from pathlib import Path

# The site loader travels next to this poller, in a checkout and in an installed
# release; its config is the release's own config/vm-ci/site.json.
SITE_LOADER = "vm_ci_site.py"
SITE_CONFIG_VARIABLE = "VMCI_SITE_CONFIG"
# Release gates run through scripts/deploy/release-train.sh with signed receipts.
RELEASE_GATES = frozenset({"deploy-demo-gate"})
# A normal check: the deploy contract. Its pass on a main push is the dev deploy gate.
DEV_DEPLOY_GATE = "deploy-recognition-gate"
DEPLOY_TIMEOUT = 2700  # Matches the hosted deploy job's 45 minute budget.
# vm-ci-dev-deploy.sh adds worktree setup and removal (git, 120 s per call at
# most) and a 60 s SIGTERM grace to the deploy's own wall-clock timeout.
DEPLOY_MARGIN = 300
DEPLOY_TERM_GRACE = 60 + 30
# vm-ci-dev-deploy.sh exit status -> recorded outcome.
DEPLOY_OUTCOMES = {0: "deployed", 1: "failed", 2: "refused", 69: "busy", 70: "error", 73: "evidence_exists",
                   75: "superseded", 77: "refused_config", 78: "not_configured", 124: "timed_out",
                   130: "interrupted"}
# Outcomes that leave nothing to report: the sha is done, or a newer push will deploy.
DEPLOY_SETTLED = frozenset({"deployed", "superseded", "not_configured", "not_installed"})
# Outcomes where whether a deploy happened is not known to this poller.
DEPLOY_UNKNOWN = frozenset({"unknown", "busy", "evidence_exists"})
PATH_CAP = 300
FORCE_COMMITS = 1000
HEX_SHA = re.compile(r"[0-9a-f]{40}")
ATTEMPT = re.compile(r"[0-9a-f]{32}")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
DELIVERY = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
SYSTEM_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": "/nonexistent",
              "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
GIT_TIMEOUT = 120
FETCH_TIMEOUT = 600
PLAN_TIMEOUT = 60
# The router adds a 30 s launcher teardown grace and a 10 s sandbox verifier
# budget to the routed job deadline; leave margin for claim and result writes.
ROUTER_MARGIN = 30 + 10 + 60
ROUTER_TERM_GRACE = 45
MAX_JSON = 2 * 1024 * 1024
DIAGNOSTIC_LIMIT = 2000
MAX_UNAVAILABLE_ENV = "VM_CI_MAX_UNAVAILABLE_ATTEMPTS"
DEFAULT_MAX_UNAVAILABLE = 3
MAX_UNAVAILABLE_RANGE = range(1, 21)
# Total re-freezes of one delivery, whatever the routing pairs (GRPH-26).
MAX_REFREEZES = 5
# The release driver's hold file is a few hundred bytes; anything larger is malformed.
HOLD_LIMIT = 64 * 1024
# The dev deploy a release hold deferred, under state-dir/deploys.
DEFERRED = "deferred.json"


class PollError(Exception):
    """A poll that cannot proceed safely; reported as exit 3."""


class Interrupted(Exception):
    pass


class SiteUnavailable(Exception):
    """The site loader or configuration is missing or invalid; reported as exit 2 before any effect."""


def site_names(site):
    """The poller's site-specific names, all from the resolved site configuration."""
    return types.SimpleNamespace(
        event_schema=site["schema_vm_check_event"],
        plan_schema=site["schema_vm_check_plan"],
        cursor_schema=site["schema_vm_ci_poll_cursor"],
        runs_schema=site["schema_vm_ci_poll_runs"],
        deploy_schema=site["schema_vm_ci_dev_deploy_record"],
        # Stable namespace: the same repository, before and after always freeze to
        # the same delivery ID, so a retried poll never creates a second delivery.
        delivery_namespace=uuid.uuid5(uuid.NAMESPACE_URL, site["delivery_namespace"]),
        deploy_env_variable=f"{site['ci_env_prefix']}DEPLOY_ENV",
        default_deploy_env=site["deploy_env_relpath"],
        # Written by the release driver while a release is in flight.
        release_hold_path=site["release_hold_path"],
        hold_schema=site["schema_vm_release_hold"],
    )


def load_site():
    """Resolve the site names through the loader next to this poller; never fall back to defaults."""
    loader = Path(__file__).resolve().with_name(SITE_LOADER)
    write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True  # A release is read-only; a checkout stays clean.
    try:
        spec = importlib.util.spec_from_file_location("vm_ci_site", loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as error:  # Any failure to load the loader refuses the poll.
        raise SiteUnavailable(f"site loader {loader} cannot be loaded: {type(error).__name__}: {error}") from None
    finally:
        sys.dont_write_bytecode = write_bytecode
    try:
        return site_names(module.load())
    except module.SiteError as error:
        raise SiteUnavailable(str(error)) from None
    except KeyError as error:
        raise SiteUnavailable(f"site configuration lacks {error}") from None


def encode(data):
    # Same canonical encoding as the router, so event_sha256 matches its records.
    return (json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n").encode()


def now():
    return int(time.time())


def read_json(path, limit=MAX_JSON):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise PollError(f"not a regular file: {path}")
        content = source.read(limit + 1)
    if len(content) > limit:
        raise PollError(f"file too large: {path}")
    return json.loads(content)


def sync_directory(path):
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def staged(directory, content):
    handle = tempfile.NamedTemporaryFile(dir=directory, prefix=".staged-", delete=False)
    with handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    return Path(handle.name)


def atomic_write(path, content):
    temp = staged(path.parent, content)
    try:
        os.replace(temp, path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    sync_directory(path.parent)


def write_once(path, content):
    """Write an immutable file; an existing file must already hold these bytes."""
    temp = staged(path.parent, content)
    try:
        os.link(temp, path)
    except FileExistsError:
        if path.read_bytes() != content:
            raise PollError(f"immutable record differs: {path}") from None
    finally:
        temp.unlink()
    sync_directory(path.parent)


def private_directory(path, create):
    if create:
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    info = path.lstat()
    if not (stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o077):
        raise PollError(f"state directory must be a private directory owned by this user: {path}")


def git(mirror, *args, timeout=GIT_TIMEOUT, env=None, check=True):
    argv = ["/usr/bin/git", f"--git-dir={mirror}", *args]
    try:
        result = subprocess.run(argv, env=env or SYSTEM_ENV, stdin=subprocess.DEVNULL,
                                capture_output=True, timeout=timeout, cwd="/")
    except subprocess.TimeoutExpired:
        raise PollError(f"git {args[0]} timed out") from None
    if check and result.returncode != 0:
        message = result.stderr.decode(errors="replace").strip()[-DIAGNOSTIC_LIMIT:]
        raise PollError(f"git {args[0]} failed: {message}")
    return result


def resolve_commit(mirror, revision):
    result = git(mirror, "rev-parse", "--verify", "--quiet", "--end-of-options",
                 f"{revision}^{{commit}}", check=False)
    sha = result.stdout.decode().strip()
    return sha if result.returncode == 0 and HEX_SHA.fullmatch(sha) else None


def fetch(args):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    spec = f"+refs/heads/{args.branch}:refs/heads/{args.branch}"
    git(args.mirror, "fetch", "--quiet", "--no-tags", "--no-recurse-submodules",
        "--no-write-fetch-head", "--end-of-options", args.remote, spec, timeout=FETCH_TIMEOUT, env=env)


def delivery_id(site, repository, before, after):
    return str(uuid.uuid5(site.delivery_namespace, f"{repository}\n{before}\n{after}"))


def compare(mirror, before, after):
    """Return the event diff for the two-dot comparison before..after."""
    unknown = {"mode": "unknown", "paths": []}
    if resolve_commit(mirror, before) != before:
        return unknown  # The before commit is no longer present in the mirror.
    if git(mirror, "merge-base", "--is-ancestor", before, after, check=False).returncode != 0:
        return unknown  # Force push: no established effective comparison.
    count = git(mirror, "rev-list", "--count", f"{before}..{after}").stdout.decode().strip()
    if int(count) > FORCE_COMMITS:
        return {"mode": "force", "paths": []}
    # Default git order: no order file, renames, relative paths or external tools.
    names = git(mirror, "diff", "--name-only", "-z", "--no-renames", "--no-ext-diff", "--no-textconv",
                "--no-relative", "-O/dev/null", before, after, "--").stdout
    paths = []
    for raw in names.split(b"\0"):
        if not raw:
            continue
        try:
            paths.append(raw.decode("utf-8"))
        except UnicodeDecodeError:
            return unknown  # The path list cannot be represented faithfully.
        if len(paths) == PATH_CAP:
            break
    return {"mode": "paths", "paths": paths}


def freeze(args, before, after):
    return {
        "schema": args.site.event_schema,
        "repository": args.repository,
        "delivery_id": delivery_id(args.site, args.repository, before, after),
        "event": "push",
        "ref": f"refs/heads/{args.branch}",
        "before_sha": before,
        "after_sha": after,
        "diff": compare(args.mirror, before, after),
    }


def read_cursor(args):
    path = args.state_dir / "cursor.json"
    if not path.exists():
        return None
    cursor = read_json(path)
    if not (isinstance(cursor, dict) and cursor.get("schema") == args.site.cursor_schema
            and cursor.get("repository") == args.repository and cursor.get("branch") == args.branch
            and isinstance(cursor.get("after_sha"), str) and HEX_SHA.fullmatch(cursor["after_sha"])
            and (cursor.get("pending") is None
                 or isinstance(cursor["pending"], str) and DELIVERY.fullmatch(cursor["pending"]))):
        raise PollError("cursor.json does not belong to this repository and branch")
    retry_from = cursor.get("retry_from")
    if retry_from is not None and not (
            cursor["pending"] and isinstance(retry_from, dict) and retry_from
            and all(type(offset) is int and offset > 0 for offset in retry_from.values())):
        raise PollError("cursor.json has an invalid retry budget")
    return cursor


def write_cursor(args, after, pending, **fields):
    atomic_write(args.state_dir / "cursor.json", encode({
        "schema": args.site.cursor_schema, "repository": args.repository, "branch": args.branch,
        "after_sha": after, "pending": pending, "updated_at": now(), **fields}))


def load_event(args, delivery, cursor):
    event = read_json(args.state_dir / "events" / f"{delivery}.json")
    cursor_edge = "after_sha" if cursor.get("retry_from") else "before_sha"
    if not (isinstance(event, dict) and event.get("schema") == args.site.event_schema and event.get("event") == "push"
            and event.get("delivery_id") == delivery and event.get("repository") == args.repository
            and event.get("ref") == f"refs/heads/{args.branch}"
            and event.get(cursor_edge) == cursor["after_sha"]):
        raise PollError(f"pending frozen event {delivery} does not continue the cursor")
    return event


def retry_delivery(args, cursor):
    """Locate the current cursor's completed delivery, including legacy cursors."""
    if not args.retry_unavailable or cursor is None or cursor["pending"] is not None:
        return None, None
    directory = args.state_dir / "runs"
    latest = cursor.get("last_delivery")
    if latest is not None:
        if not isinstance(latest, str) or not DELIVERY.fullmatch(latest):
            raise PollError("cursor.json has an invalid last delivery")
        paths = [directory / f"{latest}.json"]
    elif directory.is_dir():
        paths = sorted(directory.glob("*.json"), key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)
    else:
        return None, None
    for path in paths:
        runs = read_json(path)
        if not (isinstance(runs, dict) and runs.get("schema") == args.site.runs_schema
                and runs.get("repository") == args.repository and runs.get("ref") == f"refs/heads/{args.branch}"
                and runs.get("after_sha") == cursor["after_sha"] and runs.get("complete") is True
                and runs.get("delivery_id") == path.stem and DELIVERY.fullmatch(path.stem)):
            continue
        if runs.get("failure") is not None:
            break
        offsets = {
            name: len(state["attempts"]) for name, state in runs["checks"].items()
            if state["status"] == "failed" and (state.get("reason") or "").startswith("unavailable after ")
            and state["attempts"] and all(entry["finished_at"] is not None
                                         and status_for(effective_exit(entry)) == "pending"
                                         for entry in state["attempts"])
        }
        if offsets:
            return load_event(args, path.stem, dict(cursor, retry_from=offsets)), offsets
        break
    return None, None


def next_event(args, cursor):
    """Return (event, is_new), or (None, message) when there is nothing to run."""
    if cursor is not None and cursor["pending"] is not None:
        return load_event(args, cursor["pending"], cursor), False
    tip = resolve_commit(args.mirror, f"refs/heads/{args.branch}")
    if tip is None:
        raise PollError(f"branch {args.branch} is missing from the mirror")
    if cursor is None:
        if args.since is None:
            return None, {"action": "seed", "after_sha": tip}
        before = resolve_commit(args.mirror, args.since)
        if before is None:
            raise PollError(f"--since {args.since} is not a commit in the mirror")
    else:
        if args.since is not None:
            print("vm-ci-poll: --since ignored; a cursor already exists", file=sys.stderr)
        before = cursor["after_sha"]
    if before == tip:
        return None, {"action": "up_to_date", "after_sha": tip}
    return freeze(args, before, tip), True


def interrupt(signum, frame):
    raise Interrupted


def invoke(argv, timeout, env=None, term_grace=ROUTER_TERM_GRACE):
    """Run a trusted router (or dev deploy) command; return (status, stdout, stderr).

    On a poller deadline or cancellation the router receives SIGTERM so it can
    record its own cancellation and tear down the launcher before it is killed.
    """
    process = subprocess.Popen(argv, env=env or SYSTEM_ENV, cwd="/", stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    interrupted = None
    try:
        try:
            out, err = process.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, Interrupted) as error:
            interrupted = error
            process.send_signal(signal.SIGTERM)
            try:
                out, err = process.communicate(timeout=term_grace)
            except subprocess.TimeoutExpired:
                process.kill()
                out, err = process.communicate()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    if isinstance(interrupted, Interrupted):
        raise interrupted
    status = None if interrupted is not None else process.returncode
    return status, out.decode(errors="replace"), err.decode(errors="replace")[-DIAGNOSTIC_LIMIT:]


def router_environment():
    """The router resolves the same site as this poller (its loader reads this variable, else
    the config of its own tree); nothing else of the poller's environment reaches it."""
    env = dict(SYSTEM_ENV)
    if SITE_CONFIG_VARIABLE in os.environ:
        env[SITE_CONFIG_VARIABLE] = os.environ[SITE_CONFIG_VARIABLE]
    return env


def router_plan(args, event_path, event):
    status, out, err = invoke([str(args.router), "--event", str(event_path), "plan"], PLAN_TIMEOUT,
                              env=router_environment())
    if status != 0:
        raise PollError(f"router plan exited {status}: {err.strip()}")
    try:
        plan = json.loads(out)
    except ValueError:
        raise PollError("router plan output is not JSON") from None
    checks = plan.get("checks") if isinstance(plan, dict) else None
    if not (isinstance(checks, dict) and plan.get("schema") == args.site.plan_schema and plan.get("event") == event
            and isinstance(plan.get("routing_sha256"), str) and isinstance(checks, dict)
            and all(isinstance(job, dict) and isinstance(job.get("selection"), str)
                    for job in checks.values())):
        raise PollError("router plan does not describe this frozen event")
    return plan


def summarize(plan):
    checks = plan["checks"]
    chosen = sorted(name for name, job in checks.items() if job["selection"] == "selected")
    runnable = [name for name in chosen if name not in RELEASE_GATES and checks[name].get("script") is not None]
    return {
        "routing_sha256": plan["routing_sha256"],
        "selected": runnable,
        "excluded_release_gates": [name for name in chosen if name not in runnable],
        "unavailable": {name: job["selection"] for name, job in sorted(checks.items())
                        if job["selection"].endswith("_unavailable")},
    }


def claimed(args, attempt):
    return (args.store / attempt / "claim.json").is_file()


def recover(args, delivery, entry):
    """Adopt the router's own terminal record for an attempt the poller lost."""
    try:
        result = read_json(args.store / entry["attempt"] / "result.json")
    except (OSError, ValueError, PollError):
        return None
    if (isinstance(result, dict) and result.get("attempt_id") == entry["attempt"]
            and result.get("delivery_id") == delivery):
        return {"passed": 0, "failed": 1}.get(result.get("outcome"), 3)
    return None


def status_for(router_exit):
    return {0: "passed", 1: "failed"}.get(router_exit, "pending")


def effective_exit(entry):
    """The router exit of a finished attempt, or the exit recovered from the store."""
    return entry["recovered_exit"] if "recovered_exit" in entry else entry["router_exit"]


def describe(entry):
    if entry.get("interrupted") and effective_exit(entry) is None:
        return "interrupted"
    code = effective_exit(entry)
    if code is None:
        return "poller_deadline"
    return f"router exit {code}" + (f" ({entry['outcome']})" if entry.get("outcome") else "")


def settle(state, limit):
    """Derive a check's status from its durable attempts (the retry count's source of truth)."""
    attempts = state["attempts"][state.get("retry_from", 0):]
    last = attempts[-1] if attempts else None
    if last is not None and last["finished_at"] is not None and status_for(effective_exit(last)) != "pending":
        state["status"] = status_for(effective_exit(last))
        return
    # Finished attempts that neither passed nor failed: unavailable, deadline or cancelled.
    unavailable = [entry for entry in attempts
                   if entry["finished_at"] is not None and status_for(effective_exit(entry)) == "pending"]
    if len(unavailable) >= limit:
        state["status"] = "failed"
        state["reason"] = f"unavailable after {len(unavailable)} attempts: {describe(unavailable[-1])}"
    else:
        state["status"] = "pending"


def run_check(args, event_path, runs, check, timeout, save):
    state = runs["checks"][check]
    attempts = state["attempts"]
    if attempts and attempts[-1]["router_exit"] is None and attempts[-1]["finished_at"] is None:
        # A previous poll stopped while this attempt was running.
        attempts[-1].update(finished_at=now(), interrupted=True,
                            recovered_exit=recover(args, runs["delivery_id"], attempts[-1]))
    settle(state, args.max_unavailable)
    save()
    if state["status"] != "pending":
        return
    previous = next((entry["attempt"] for entry in reversed(attempts) if claimed(args, entry["attempt"])), None)
    attempt = secrets.token_hex(16)
    entry = {"attempt": attempt, "retry_of": previous, "started_at": now(), "finished_at": None,
             "router_exit": None, "outcome": None, "diagnostic": None}
    attempts.append(entry)
    state["status"] = "pending"
    save()
    argv = [str(args.router), "--event", str(event_path), "run", "--check", check, "--store", str(args.store),
            "--attempt", attempt, "--launcher", str(args.launcher), "--sandbox-verifier", str(args.sandbox_verifier)]
    if previous is not None:
        argv += ["--retry-of", previous]
    try:
        router_exit, out, err = invoke(argv, timeout, env=router_environment())
    except Interrupted:
        entry.update(finished_at=now(), interrupted=True)
        save()
        raise
    entry.update(finished_at=now(), router_exit=router_exit, outcome=out.strip()[:64] or None,
                 diagnostic=err.strip() or (None if router_exit is not None else "poller_deadline"))
    settle(state, args.max_unavailable)
    save()


def routing_identity(summary):
    return hashlib.sha256(encode(summary)).hexdigest()


def refreeze(runs, frozen, current):
    """Re-freeze a pending delivery under the current routing; return False when it was failed instead."""
    old, new = routing_identity(frozen), routing_identity(current)
    history = runs.setdefault("refreezes", [])
    change = (f"routing changed for pending delivery {runs['delivery_id']}: "
              f"{frozen['routing_sha256']} -> {current['routing_sha256']}")
    if any(entry["from"] == old and entry["to"] == new for entry in history):
        reason = f"{change}; already re-frozen once for this routing pair"
    elif len(history) >= MAX_REFREEZES:
        reason = f"{change}; already re-frozen {len(history)} times"
    else:
        history.append({"from": old, "to": new, "from_routing": frozen, "to_routing": current,
                        "reason": change, "at": now(),
                        # Compact: the attempts' evidence stays in the router store.
                        "previous_checks": {name: {"status": state["status"], "reason": state.get("reason"),
                                                   "attempts": [entry["attempt"] for entry in state["attempts"]]}
                                            for name, state in runs["checks"].items()}})
        runs.update(current)
        runs["checks"] = {name: {"status": "pending", "attempts": []} for name in current["selected"]}
        return True
    runs["failure"] = {"reason": reason, "from": old, "to": new, "to_routing": current, "at": now()}
    for state in runs["checks"].values():
        if state["status"] == "pending":
            state.update(status="failed", reason=reason)
    return False


def release_hold(site):
    """None when no release hold exists, else what holds (fail closed: a malformed file holds).

    Only a missing file (or a missing directory above it) is "no hold"; anything
    else at that path, including a symlink, an unreadable file or content that is
    not exactly the driver's record, is a hold with the reason it is malformed."""
    path = Path(site.release_hold_path)

    def malformed(reason):
        return {"path": str(path), "valid": False, "commit": None, "reason": reason}
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except FileNotFoundError:
        return None
    except OSError as error:
        return malformed(f"cannot be opened: {error.strerror or type(error).__name__}")
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            return malformed("is not a regular file")
        with os.fdopen(fd, "rb") as source:
            content = source.read(HOLD_LIMIT + 1)
    except OSError as error:
        return malformed(f"cannot be read: {error.strerror or type(error).__name__}")
    if len(content) > HOLD_LIMIT:
        return malformed(f"is larger than {HOLD_LIMIT} bytes")
    try:
        record = json.loads(content)
    except (ValueError, RecursionError):
        return malformed("is not valid JSON")
    if not isinstance(record, dict):
        return malformed("is not a JSON object")
    if record.get("schema") != site.hold_schema:
        return malformed(f"has schema {record.get('schema')!r}, not {site.hold_schema!r}")
    commit = record.get("commit")
    if not (isinstance(commit, str) and HEX_SHA.fullmatch(commit)):
        return malformed("does not name a 40-digit commit")
    placed = record.get("placed_at")
    return {"path": str(path), "valid": True, "commit": commit,
            "placed_at": placed if type(placed) is int else None}


def describe_hold(hold):
    if hold["valid"]:
        return f"release hold {hold['path']} (commit {hold['commit']}): automatic dev deploys are deferred"
    return (f"release hold {hold['path']} is malformed ({hold['reason']}); treated as a hold: automatic dev "
            "deploys are deferred until it is repaired or removed")


def deploy_blocker(args, summary, statuses, failure):
    """Return why this terminal push must not deploy dev, or None to deploy."""
    if failure is not None:
        return "delivery_failed"
    if not args.dev_deploy:
        return "disabled"
    if args.branch != "main":
        return "not_main"
    if DEV_DEPLOY_GATE not in summary["selected"]:
        return "gate_not_routed"
    if statuses.get(DEV_DEPLOY_GATE) != "passed":
        return "gate_not_passed"
    if any(status != "passed" for status in statuses.values()):
        return "check_failed"
    if summary["unavailable"]:
        return "check_unavailable"
    return None


def deploy_environment():
    # The deploy needs this user's home (ssh known_hosts, an optional agent); the
    # deploy script builds the environment of recognition-service.sh itself.
    env = dict(SYSTEM_ENV, HOME=pwd.getpwuid(os.geteuid()).pw_dir)
    env.pop("GIT_CONFIG_NOSYSTEM")
    env.pop("GIT_CONFIG_GLOBAL")
    if os.environ.get("SSH_AUTH_SOCK", "").startswith("/"):
        env["SSH_AUTH_SOCK"] = os.environ["SSH_AUTH_SOCK"]
    # The deploy resolves the same site as this poller (its loader reads this
    # variable, else the config of its own tree).
    if SITE_CONFIG_VARIABLE in os.environ:
        env[SITE_CONFIG_VARIABLE] = os.environ[SITE_CONFIG_VARIABLE]
    return env


def deploy_record(site, event, **fields):
    return encode({"schema": site.deploy_schema, "repository": event["repository"], "ref": event["ref"],
                   "sha": event["after_sha"], "delivery_id": event["delivery_id"], "env": "dev", **fields})


def dev_deploy(args, event, summary, statuses, failure=None):
    """Deploy the push's after_sha to dev at most once; return the report for this poll."""
    sha = event["after_sha"]
    directory = args.state_dir / "deploys"
    intent_path, outcome_path = directory / f"{sha}.intent.json", directory / f"{sha}.outcome.json"
    if intent_path.exists() and not outcome_path.exists():
        # An intent without an outcome stays unknown whatever the checks now say,
        # e.g. after a re-freeze re-ran them (RES-31).
        return {"decision": "unknown", "outcome": "unknown", "intent": read_json(intent_path).get("delivery_id")}
    blocker = deploy_blocker(args, summary, statuses, failure)
    if blocker is not None:
        return {"decision": blocker}
    private_directory(directory, create=True)
    if outcome_path.exists():
        recorded = read_json(outcome_path)
        return {"decision": "already_recorded", "outcome": recorded.get("outcome"),
                "delivery_id": recorded.get("delivery_id")}
    if intent_path.exists():
        # A poll stopped after the intent and before the outcome: the deploy may
        # or may not have happened. Never retry it automatically (RES-31).
        return {"decision": "unknown", "outcome": "unknown",
                "intent": read_json(intent_path).get("delivery_id")}
    # Immediately before anything is recorded or started: a release in flight
    # defers the deploy instead of racing it (re-read here, not at poll start).
    hold = release_hold(args.site)
    if hold is not None:
        atomic_write(directory / DEFERRED, deploy_record(args.site, event, deferred_at=now(), release_hold=hold))
        return {"decision": "held", "release_hold": hold}
    script = args.deploy_script
    if not (script.is_file() and os.access(script, os.X_OK)):
        write_once(outcome_path, deploy_record(args.site, event, outcome="not_installed", exit=None,
                                               started_at=None, finished_at=now(),
                                               diagnostic=f"{script} is not installed"))
        return {"decision": "deploy", "outcome": "not_installed"}
    evidence_root = args.store / "dev-deploy"
    private_directory(evidence_root, create=True)
    evidence = evidence_root / sha
    started = now()
    write_once(intent_path, deploy_record(args.site, event, started_at=started, evidence=str(evidence),
                                          deploy_script=str(script)))
    argv = [str(script), "--mirror", str(args.mirror), "--evidence-dir", str(evidence),
            "--work-root", str(args.state_dir / "deploy-work"), "--deploy-env", str(args.deploy_env),
            "--env", "dev", "--timeout", str(args.deploy_timeout), "--", sha]
    try:
        status, out, err = invoke(argv, args.deploy_timeout + DEPLOY_MARGIN, env=deploy_environment(),
                                  term_grace=DEPLOY_TERM_GRACE)
    except Interrupted:
        write_once(outcome_path, deploy_record(args.site, event, outcome="interrupted", exit=None,
                                               started_at=started, finished_at=now(), evidence=str(evidence),
                                               diagnostic="poller interrupted; the deploy was sent SIGTERM"))
        raise
    outcome = "timed_out" if status is None else DEPLOY_OUTCOMES.get(status, "error")
    result = out.strip().splitlines()[-1][:DIAGNOSTIC_LIMIT] if out.strip() else None
    write_once(outcome_path, deploy_record(args.site, event, outcome=outcome, exit=status,
                                           started_at=started, finished_at=now(), evidence=str(evidence),
                                           result=result,
                                           diagnostic=err.strip() or (None if status is not None
                                                                      else "poller_deadline")))
    return {"decision": "deploy", "outcome": outcome, "evidence": str(evidence)}


def deferred_deploy(args):
    """The deferred deploy record, or None; a record that does not verify stops the poll."""
    path = args.state_dir / "deploys" / DEFERRED
    if not path.exists():
        return None
    record = read_json(path)
    if not (isinstance(record, dict) and record.get("schema") == args.site.deploy_schema
            and record.get("repository") == args.repository and record.get("ref") == f"refs/heads/{args.branch}"
            and isinstance(record.get("sha"), str) and HEX_SHA.fullmatch(record["sha"])
            and isinstance(record.get("delivery_id"), str) and DELIVERY.fullmatch(record["delivery_id"])):
        raise PollError(f"{path} does not belong to this repository and branch")
    return record


def drop_deferred(args):
    (args.state_dir / "deploys" / DEFERRED).unlink()
    sync_directory(args.state_dir / "deploys")


def resume_deferred(args, record):
    """Run the deploy a release hold deferred, now that the hold is gone.

    Returns the exit status, or None when there is nothing to resume: a poll that
    stopped after deferring but before completing the delivery left it pending in
    the cursor, so the normal poll re-evaluates its deploy instead."""
    delivery = record["delivery_id"]
    event = read_json(args.state_dir / "events" / f"{delivery}.json")
    runs_path = args.state_dir / "runs" / f"{delivery}.json"
    runs = read_json(runs_path)
    if not (isinstance(event, dict) and event.get("schema") == args.site.event_schema
            and event.get("delivery_id") == delivery and event.get("after_sha") == record["sha"]
            and isinstance(runs, dict) and runs.get("delivery_id") == delivery
            and runs.get("event_sha256") == hashlib.sha256(encode(event)).hexdigest()):
        raise PollError(f"deferred dev deploy {delivery} does not match its frozen event and run record")
    if runs.get("complete") is not True:
        drop_deferred(args)
        return None
    summary = {key: runs.get(key) for key in ("routing_sha256", "selected", "excluded_release_gates",
                                              "unavailable")}
    statuses = {name: state["status"] for name, state in runs["checks"].items()}
    deploy = dev_deploy(args, event, summary, statuses, runs.get("failure"))
    runs["dev_deploy"], runs["updated_at"] = deploy, now()
    atomic_write(runs_path, encode(runs))
    if deploy["decision"] != "held":
        # Only after the outcome is durable (or the deploy was found settled): a
        # poll that dies before this line re-runs the same idempotent decision.
        drop_deferred(args)
    report = {"action": "resumed_dev_deploy", "delivery_id": delivery, "after_sha": record["sha"],
              "dev_deploy": deploy}
    if deploy.get("release_hold"):
        report["release_hold"] = deploy["release_hold"]
    print(json.dumps(report, sort_keys=True))
    outcome = deploy.get("outcome") if deploy["decision"] != "already_recorded" else None
    if deploy["decision"] == "held" or outcome in DEPLOY_UNKNOWN:
        return 3
    if outcome not in (None, *DEPLOY_SETTLED):
        return 1
    return 0


def poll(args):
    hold = release_hold(args.site)
    if hold is not None:
        print(f"vm-ci-poll: {describe_hold(hold)}", file=sys.stderr)
    deferred = deferred_deploy(args)
    if deferred is not None and hold is None:
        # The hold is gone: the deferred deploy is this poll's only work.
        resumed = resume_deferred(args, deferred)
        if resumed is not None:
            return resumed
        deferred = None
    status = {} if hold is None else {"release_hold": hold}
    if deferred is not None:
        status["deferred_dev_deploy"] = {"sha": deferred["sha"], "delivery_id": deferred["delivery_id"]}
    cursor = read_cursor(args)
    event, new_retry = retry_delivery(args, cursor)
    retry_from = new_retry or (cursor.get("retry_from") if cursor else None)
    detail = False
    if event is None:
        event, detail = next_event(args, cursor)
    if event is None:
        if detail["action"] == "seed":
            write_cursor(args, detail["after_sha"], None)
        print(json.dumps(dict(detail, **status), sort_keys=True))
        return 0
    delivery = event["delivery_id"]
    for name in ("events", "runs"):
        private_directory(args.state_dir / name, create=True)
    event_path = args.state_dir / "events" / f"{delivery}.json"
    if detail:  # A new frozen event: persist it before the cursor names it.
        write_once(event_path, encode(event))
        write_cursor(args, event["before_sha"], delivery)
    plan = router_plan(args, event_path, event)
    summary = summarize(plan)
    runs_path = args.state_dir / "runs" / f"{delivery}.json"
    if runs_path.exists():
        runs = read_json(runs_path)
        if runs.get("delivery_id") != delivery or runs.get("event_sha256") != hashlib.sha256(encode(event)).hexdigest():
            raise PollError(f"run record {delivery} does not match the frozen event")
        frozen = {key: runs.get(key) for key in summary}
        if retry_from:
            if new_retry and frozen != summary:
                raise PollError(f"cannot retry unavailable checks for {delivery}: routing changed")
            for name, offset in retry_from.items():
                state = runs["checks"][name]
                if state.get("retry_from") != offset:
                    if offset != len(state["attempts"]) or state["status"] != "failed":
                        raise PollError(f"retry budget for {delivery}/{name} does not match its attempts")
                    state.update(status="pending", retry_from=offset)
                    state.pop("reason", None)
                    runs["complete"] = False
            if not runs["complete"] and frozen != summary:
                reason = f"routing changed while retrying unavailable checks for {delivery}"
                runs["failure"] = {"reason": reason, "at": now()}
                for state in runs["checks"].values():
                    if state["status"] == "pending":
                        state.update(status="failed", reason=reason)
        if runs.get("complete") or runs.get("failure") is not None:
            # A finished delivery keeps its frozen routing; only the cursor write remains.
            summary = frozen
        elif frozen != summary:
            # A retry must keep the frozen event and routing policy, so a changed
            # policy re-freezes the delivery once instead of wedging the cursor.
            if refreeze(runs, frozen, summary):
                runs["updated_at"] = now()
                atomic_write(runs_path, encode(runs))
                print(f"vm-ci-poll: routing changed for pending delivery {delivery}; re-frozen under "
                      f"routing {summary['routing_sha256']}", file=sys.stderr)
                print(json.dumps({"action": "refrozen", "delivery_id": delivery, "after_sha": event["after_sha"],
                                  "refreeze": {key: runs["refreezes"][-1][key] for key in ("from", "to", "reason")},
                                  "cursor_advanced": False, **status}, sort_keys=True))
                return 3
            summary = frozen
    else:
        if retry_from:
            raise PollError(f"missing run record for retry of {delivery}")
        runs = dict(summary, schema=args.site.runs_schema, delivery_id=delivery,
                    event_sha256=hashlib.sha256(encode(event)).hexdigest(),
                    repository=event["repository"], ref=event["ref"], before_sha=event["before_sha"],
                    after_sha=event["after_sha"], diff_mode=event["diff"]["mode"], created_at=now(),
                    updated_at=now(), complete=False,
                    checks={name: {"status": "pending", "attempts": []} for name in summary["selected"]})

    def save():
        runs["updated_at"] = now()
        atomic_write(runs_path, encode(runs))

    if new_retry:
        # Persist the intent first so an interrupted poll can apply each offset once.
        write_cursor(args, event["after_sha"], delivery, retry_from=new_retry)
    save()
    for check in summary["selected"]:
        if runs["checks"][check]["status"] == "pending" and runs.get("failure") is None:
            timeout = plan["checks"][check]["timeout_seconds"] + ROUTER_MARGIN
            run_check(args, event_path, runs, check, timeout, save)
    statuses = {name: state["status"] for name, state in runs["checks"].items()}
    pending = sorted(name for name, status in statuses.items() if status == "pending")
    failed = sorted(name for name, status in statuses.items() if status == "failed")
    failure = runs.get("failure")
    deploy = {"decision": "checks_pending"}
    if not pending:
        # Before the cursor moves, so a restart re-evaluates instead of skipping.
        deploy = dev_deploy(args, event, summary, statuses, failure)
        runs["dev_deploy"] = deploy
        runs["complete"] = True
        save()
        write_cursor(args, event["after_sha"], None, last_delivery=delivery)
    report = {"action": "ran", "delivery_id": delivery, "after_sha": event["after_sha"],
              "checks": statuses, "unavailable": summary["unavailable"],
              "cursor_advanced": not pending and not retry_from, "dev_deploy": deploy, **status}
    # The checks may have run for a long time: report the hold as it is now.
    current = deploy["release_hold"] if deploy["decision"] == "held" else release_hold(args.site)
    report.pop("release_hold", None)
    if current is not None:
        report["release_hold"] = current
        if hold is None:
            print(f"vm-ci-poll: {describe_hold(current)}", file=sys.stderr)
    if deploy["decision"] == "held":
        report["deferred_dev_deploy"] = {"sha": event["after_sha"], "delivery_id": delivery}
    reasons = {name: state["reason"] for name, state in runs["checks"].items() if state.get("reason")}
    if reasons:
        report["reasons"] = reasons
    if failure is not None:
        report["failure"] = failure["reason"]
    print(json.dumps(report, sort_keys=True))
    outcome = deploy.get("outcome") if deploy["decision"] != "already_recorded" else None
    if failed or failure is not None or outcome not in (None, *DEPLOY_SETTLED, *DEPLOY_UNKNOWN):
        return 1
    if pending or summary["unavailable"] or outcome in DEPLOY_UNKNOWN or deploy["decision"] == "held":
        return 3
    return 0


def dry_run(args):
    hold = release_hold(args.site)
    status = {} if hold is None else {"release_hold": hold}
    if hold is not None:
        print(f"vm-ci-poll: {describe_hold(hold)}", file=sys.stderr)
    cursor = read_cursor(args) if args.state_dir.is_dir() else None
    event, _ = retry_delivery(args, cursor)
    if event is None:
        event, detail = next_event(args, cursor)
    if event is None:
        print(json.dumps(dict(detail, dry_run=True, **status), sort_keys=True))
        return 0
    with tempfile.TemporaryDirectory(prefix="vm-ci-poll-") as scratch:
        event_path = Path(scratch) / "event.json"
        event_path.write_bytes(encode(event))
        plan = router_plan(args, event_path, event)
    print(json.dumps({"dry_run": True, "event": event, "plan": plan, **summarize(plan), **status}, sort_keys=True))
    return 0


def trusted_executable(parser, path, name):
    if not (path.is_absolute() and path.is_file() and os.access(path, os.X_OK)):
        parser.error(f"{name} must be an absolute path to an installed executable")
    return path


def parse(argv, site):
    parser = argparse.ArgumentParser(prog="vm-ci-poll.py", description=__doc__.splitlines()[0])
    parser.add_argument("--mirror", required=True, type=Path, help="bare source mirror")
    parser.add_argument("--remote", default="origin", help="remote to fetch, or - to skip the fetch")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--repository", required=True, help="OWNER/REPO")
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--router", required=True, type=Path, help="trusted installed run-vm-check.sh")
    parser.add_argument("--launcher", required=True, type=Path)
    parser.add_argument("--sandbox-verifier", required=True, type=Path)
    parser.add_argument("--store", required=True, type=Path, help="private evidence store")
    parser.add_argument("--since", help="first-run base commit instead of seeding the cursor")
    parser.add_argument("--retry-unavailable", action="store_true",
                        help="give exhausted unavailable checks on the last processed push a fresh retry budget")
    parser.add_argument("--dry-run", action="store_true", help="print the frozen event and plan; change no state")
    parser.add_argument("--dev-deploy", action=argparse.BooleanOptionalAction, default=None,
                        help="deploy dev after a main push passes deploy-recognition-gate "
                             "(default: on only when the --deploy-env file exists)")
    parser.add_argument("--deploy-env", type=Path,
                        help="private operator env file for the dev deploy (default "
                             f"${site.deploy_env_variable}, else ~/{site.default_deploy_env})")
    parser.add_argument("--deploy-script", type=Path, default=Path(__file__).resolve().with_name("vm-ci-dev-deploy.sh"),
                        help="trusted installed vm-ci-dev-deploy.sh (default: next to this poller)")
    parser.add_argument("--deploy-timeout", type=int, default=DEPLOY_TIMEOUT,
                        help="dev deploy wall-clock seconds (60-7200)")
    args = parser.parse_args(argv)
    bound = os.environ.get(MAX_UNAVAILABLE_ENV)
    if bound is None:
        args.max_unavailable = DEFAULT_MAX_UNAVAILABLE
    elif re.fullmatch(r"[0-9]{1,2}", bound) and int(bound) in MAX_UNAVAILABLE_RANGE:
        args.max_unavailable = int(bound)
    else:
        parser.error(f"{MAX_UNAVAILABLE_ENV} must be an integer from {MAX_UNAVAILABLE_RANGE.start} "
                     f"to {MAX_UNAVAILABLE_RANGE.stop - 1}")
    if not REPOSITORY.fullmatch(args.repository):
        parser.error("--repository must be OWNER/REPO")
    if (not re.fullmatch(r"[A-Za-z0-9._/-]+", args.branch) or subprocess.run(
            ["/usr/bin/git", "check-ref-format", f"refs/heads/{args.branch}"], env=SYSTEM_ENV,
            capture_output=True).returncode != 0):
        parser.error("--branch is not a valid branch name")
    if args.remote != "-" and (args.remote.startswith("-") or not args.remote):
        parser.error("--remote must be a remote name or -")
    for flag in ("mirror", "state_dir", "store"):
        if not getattr(args, flag).is_absolute():
            parser.error(f"--{flag.replace('_', '-')} must be absolute")
    for flag in ("router", "launcher", "sandbox_verifier"):
        trusted_executable(parser, getattr(args, flag), "--" + flag.replace("_", "-"))
    router = args.router.resolve()
    for root in (args.mirror, args.state_dir):
        if router.is_relative_to(root.resolve()):
            parser.error("--router must be the trusted installation, not a mirror or state copy")
    if args.deploy_env is None:
        configured = os.environ.get(site.deploy_env_variable)
        home = os.environ.get("HOME", "")
        args.deploy_env = (Path(configured) if configured
                           else Path(home) / site.default_deploy_env if home.startswith("/") else None)
    if args.deploy_env is not None and not args.deploy_env.is_absolute():
        parser.error("--deploy-env must be absolute")
    explicit = args.dev_deploy is True
    if args.dev_deploy is None:
        # Installs without the operator env file keep running checks only.
        args.dev_deploy = args.deploy_env is not None and os.path.lexists(args.deploy_env)
    if args.dev_deploy:
        if args.deploy_env is None:
            parser.error(f"--dev-deploy needs --deploy-env or {site.deploy_env_variable}")
        if explicit:
            # Implicit enablement records a missing script per sha instead, so
            # an env file placed ahead of the install never stops the checks.
            trusted_executable(parser, args.deploy_script, "--deploy-script")
        if not 60 <= args.deploy_timeout <= 7200:
            parser.error("--deploy-timeout must be between 60 and 7200 seconds")
        if not args.deploy_script.is_absolute():
            parser.error("--deploy-script must be absolute")
        script = args.deploy_script.resolve()
        for root in (args.mirror, args.state_dir, args.store):
            if script.is_relative_to(root.resolve()):
                parser.error("--deploy-script must be the trusted installation, not a mirror or state copy")
    args.site = site
    return args


def main(argv=None):
    try:
        # Before parsing and before any effect: an unresolvable site refuses the poll.
        site = load_site()
    except SiteUnavailable as error:
        print(f"vm-ci-poll: site configuration: {' '.join(str(error).split())}", file=sys.stderr)
        return 2
    args = parse(sys.argv[1:] if argv is None else argv, site)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, interrupt)
    try:
        bare = git(args.mirror, "rev-parse", "--is-bare-repository", check=False)
        if bare.returncode != 0 or bare.stdout.strip() != b"true":
            raise PollError(f"--mirror is not a bare repository: {args.mirror}")
        if args.dry_run:
            return dry_run(args)
        private_directory(args.state_dir, create=True)
        lock_fd = os.open(args.state_dir / "poll.lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print(json.dumps({"action": "busy"}), file=sys.stderr)
                return 0
            if args.remote != "-":
                fetch(args)
            return poll(args)
        finally:
            os.close(lock_fd)
    except PollError as error:
        print(f"vm-ci-poll: {error}", file=sys.stderr)
    except Interrupted:
        print("vm-ci-poll: interrupted", file=sys.stderr)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"vm-ci-poll: state_or_storage_error: {type(error).__name__}: {error}", file=sys.stderr)
    return 3


if __name__ == "__main__":
    sys.exit(main())
