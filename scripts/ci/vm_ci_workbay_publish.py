#!/usr/bin/env python3
"""Run WorkBay's public release pipeline with the VM CI publish hold and journal."""

import argparse
import fcntl
import json
import os
import re
import runpy
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK_TIMEOUT = 120
SHA = re.compile(r"[0-9a-fA-F]{40}")


class Refused(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--site-config", help="Override the VM CI site configuration path.")
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--commit", help="40-hex source commit (default: current HEAD).")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="Attempt live publish with CONFIRM=PUBLISH.")
    mode.add_argument("--plan-only", action="store_true", help="Print the dry-run command without starting it.")
    parser.add_argument("--skip-canon-lint", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=900, metavar="30..7200")
    parser.add_argument(
        "--release-arg",
        action="append",
        default=[],
        help="Forward a pipeline option, e.g. --release-arg=--prepared=PLAN_ID.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if not 30 <= args.timeout_seconds <= 7200:
        parser.error("--timeout-seconds must be between 30 and 7200")
    return args


def load_site(config):
    path = Path(__file__).with_name("vm_ci_site.py")
    try:
        loader = runpy.run_path(str(path))
    except OSError as exc:
        raise Refused(3, f"site loader unavailable: {exc}") from exc

    try:
        return loader["load"](config)
    except loader["SiteError"] as exc:
        unavailable = isinstance(exc.__context__, OSError) and not isinstance(exc.__context__, IsADirectoryError)
        raise Refused(3 if unavailable else 2, str(exc)) from exc


def release_args(values):
    # Mode and operator subcommands belong to the adapter and publisher
    # respectively. Only forward options for the release pipeline.
    remaining = iter(values)
    for value in remaining:
        if value in ("--allow-higher-existing-tags", "--json"):
            continue
        if value.startswith("--prepared=") and value != "--prepared=":
            continue
        if value == "--prepared":
            plan = next(remaining, "")
            if plan and not plan.startswith("-"):
                continue
        raise Refused(2, f"unsupported --release-arg: {value!r}")
    return values


def run_check(command, root, timeout):
    # Callers select the Git and Canon commands and inspect their exit status.
    return subprocess.run(  # noqa: S603
        command,
        cwd=root,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        errors="replace",
        check=False,
    )


def git(root, *args):
    result = run_check(["git", "--no-optional-locks", *args], root, CHECK_TIMEOUT)
    if result.returncode:
        raise Refused(1, result.stderr.strip() or "git check failed")
    return result.stdout


def verify_head(root, expected):
    head = git(root, "rev-parse", "--verify", "HEAD").strip()
    if not SHA.fullmatch(head) or (expected is not None and head.lower() != expected.lower()):
        raise Refused(3, f"HEAD {head!r} does not match the requested commit {expected!r}")
    return head.lower()


def verify_clean(root):
    if git(root, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none"):
        raise Refused(3, "live publish requires a clean working tree")


def canon_preflight(root, receipt):
    local_workbay = root / ".venv" / "bin" / "workbay"
    workbay = (
        str(local_workbay) if local_workbay.is_file() and os.access(local_workbay, os.X_OK) else shutil.which("workbay")
    )
    if workbay is None:
        receipt.update(canon_preflight="skipped", canon_preflight_reason="workbay_not_found")
        return
    receipt["canon_preflight"] = "ran"
    for verb in ("check-drift", "lint"):
        result = run_check([workbay, "canon", verb, "--json"], root, CHECK_TIMEOUT)
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            raise Refused(1, f"workbay canon {verb} failed: {detail}")
        try:
            report = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise Refused(1, f"workbay canon {verb} did not return JSON") from exc
        if not isinstance(report, dict) or report.get("ok") is not True:
            raise Refused(1, f"workbay canon {verb} did not report success: {result.stdout.strip()}")


def json_bytes(data):
    return (json.dumps(data, separators=(",", ":")) + "\n").encode("utf-8")


@contextmanager
def staged_file(path, content):
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            yield temporary, os.fstat(handle.fileno())
        finally:
            temporary.unlink(missing_ok=True)


def write_json(path, data):
    with staged_file(path, json_bytes(data)) as (temporary, _):
        os.replace(temporary, path)


def remove_owned_hold(path, identity, content):
    try:
        if not os.path.samestat(path.lstat(), identity):
            return
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as handle:
            if not os.path.samestat(os.fstat(handle.fileno()), identity):
                return
            if handle.read(len(content) + 1) != content:
                return
            if os.path.samestat(path.lstat(), identity):
                path.unlink(missing_ok=True)
    except FileNotFoundError:
        return


def stop_on_sigterm(_signum, _frame):
    raise KeyboardInterrupt


def run_release(receipt, root, timeout):
    # A separate process group lets timeout or interruption stop the publisher
    # and its helpers before the live hold is removed.
    try:
        with subprocess.Popen(  # noqa: S603
            receipt["command"],
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=sys.stderr,
            start_new_session=True,
        ) as child:
            try:
                returncode = child.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError as exc:
                    print(f"Publisher process group already exited: {exc}", file=sys.stderr)
                raise
    except subprocess.TimeoutExpired:
        returncode = 124
        receipt["error"] = f"publisher timed out after {timeout} seconds"
    except KeyboardInterrupt:
        returncode = 130
        receipt["error"] = "publisher interrupted"
    except OSError as exc:
        returncode = 1
        receipt["error"] = f"could not start publisher: {exc}"
    else:
        if returncode:
            receipt["error"] = f"publisher exited with status {returncode}"
    receipt.update(returncode=returncode, ok=returncode == 0, finished_at=int(time.time()))


def publish_live(receipt, root, state_dir, hold_path, hold_schema, timeout):
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_fd = os.open(state_dir / "publish.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Refused(3, "publish lock is busy") from exc
        journal_dir = state_dir / "publishes"
        journal_dir.mkdir(mode=0o700, exist_ok=True)
        hold_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        receipt["started_at"] = int(time.time())
        hold_content = json_bytes(
            {"schema": hold_schema, "commit": receipt["commit"], "placed_at": receipt["started_at"]}
        )
        hold_identity = None
        try:
            with staged_file(hold_path, hold_content) as (temporary, identity):
                hold_identity = identity
                try:
                    os.link(temporary, hold_path)
                except FileExistsError as exc:
                    hold_identity = None
                    raise Refused(3, f"release hold already exists at {hold_path}") from exc
            verify_head(root, receipt["commit"])
            run_release(receipt, root, timeout)
        finally:
            try:
                if hold_identity is not None:
                    remove_owned_hold(hold_path, hold_identity, hold_content)
            except OSError as exc:
                receipt.update(ok=False, error=f"could not remove release hold: {exc}")
            if "returncode" in receipt:
                write_json(journal_dir / f"{receipt['run_id']}.json", receipt)
    finally:
        os.close(lock_fd)


def run(args, receipt):
    site = load_site(args.site_config)
    if args.execute and os.environ.get("CONFIRM") != "PUBLISH":
        raise Refused(2, "--execute requires CONFIRM=PUBLISH")
    if args.commit is not None and not SHA.fullmatch(args.commit):
        raise Refused(2, "--commit must be a 40-hex commit SHA")
    extra_args = release_args(args.release_arg)
    root = args.repo_root.resolve()
    commit = verify_head(root, args.commit)
    if args.execute:
        verify_clean(root)
    if args.skip_canon_lint:
        receipt.update(canon_preflight="skipped", canon_preflight_reason="skip_flag")
    else:
        canon_preflight(root, receipt)

    state_dir = (args.state_dir or Path(site["state_dir"])).resolve()
    hold_path = Path(site["release_hold_path"])
    command = [sys.executable, str(root / "scripts" / "release_public.py"), *extra_args]
    if args.execute:
        command.extend(("--execute", "--assume-yes"))
    receipt.update(
        schema=f"{site['slug']}.vm-ci-workbay-publish/1",
        run_id=uuid.uuid4().hex,
        commit=commit,
        command=command,
        started_at=int(time.time()),
    )

    if args.plan_only:
        receipt.update(
            ok=True,
            returncode=None,
            finished_at=int(time.time()),
            site={
                "slug": site["slug"],
                "state_dir": str(state_dir),
                "release_hold_path": str(hold_path),
                "schema_vm_release_hold": site["schema_vm_release_hold"],
            },
        )
    elif args.execute:
        verify_clean(root)
        verify_head(root, commit)
        publish_live(receipt, root, state_dir, hold_path, site["schema_vm_release_hold"], args.timeout_seconds)
    else:
        verify_head(root, commit)
        run_release(receipt, root, args.timeout_seconds)


def main():
    args = parse_args()
    receipt = {"mode": "execute" if args.execute else "dry_run", "ok": False}
    try:
        run(args, receipt)
        code = 0 if receipt["ok"] else 1
    except Refused as exc:
        code = exc.code
        receipt.update(ok=False, error=str(exc))
    except (OSError, subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        code = 1
        receipt.update(ok=False, error=str(exc) or "publish interrupted")

    if args.json:
        print(json.dumps(receipt, separators=(",", ":")))
    elif receipt["ok"]:
        print(shlex.join(receipt["command"]) if args.plan_only else f"{receipt['mode']}: {receipt['commit']}")
        if args.plan_only:
            print(f"Commit: {receipt['commit']}")
            print(json.dumps(receipt["site"], separators=(",", ":")))
    else:
        print(f"vm-ci-workbay-publish: {receipt['error']}", file=sys.stderr)
    return code


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_on_sigterm)
    raise SystemExit(main())
