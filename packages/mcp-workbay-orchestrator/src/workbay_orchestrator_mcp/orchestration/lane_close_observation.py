"""Read-only lane close probes over native publications and canonical Git.

The combined witness protects review charges from automated loss or
misattribution of dispatch work. A principal with host write access can
rewrite the inputs or carry work onto an unrelated lineage.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import sqlite3
import stat
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path
from urllib.parse import unquote, urlsplit

from workbay_handoff_mcp.enums import TERMINAL_LANE_STATUSES
from workbay_handoff_mcp.lane_close_evidence import OPENINGS, ClosureIdentity, opening_coverage
from workbay_handoff_mcp.shared_write_context import run_subprocess
from workbay_protocol.remote_probe import resolve_remote_gate_host

from . import review_cycle_dispatch as cycle
from .invocation_disposition import DispositionHold, _json, regular_bytes
from .lane_lifecycle_contracts import JOB_ID_RE, job_id_for
from .offload_pass_index import _parse_ledger_line, _valid_pass_id
from .review_ratchet import _scope_path

OID = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
COMBINED_WITNESS = "combined_no_contribution_v1"
_MAX_SOURCE = 32 * 1024 * 1024


class CloseObservationHold(ValueError):
    def __init__(self, reason: str, *, disposition: str = "unknown", **details):
        super().__init__(reason)
        self.diagnostic = dict(reason=reason, disposition=disposition, **details)


def require(condition, reason: str, **details) -> None:
    if not condition:
        raise CloseObservationHold(reason, **details)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def unique_json(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_field", field=key, disposition="denied")
        result[key] = value
    return result


def _matches(value: object, pattern: re.Pattern) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _same(value: object, subset: dict) -> bool:
    return isinstance(value, dict) and all(
        key in value and type(value[key]) is type(item) and value[key] == item for key, item in subset.items()
    )


def _read_bytes(
    path: Path, root: Path, reason: str, *, private: bool = True, unaliased: bool = False, sealed_parent: bool = False,
) -> bytes:
    """Check the opened inode and cap the read; path components may not be links."""
    try:
        require(path.is_relative_to(root) and path.resolve(strict=True) == path, reason, locator=str(path))
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(
                stat.S_ISREG(info.st_mode)
                and info.st_uid == os.geteuid()
                and not info.st_mode & (0o077 if private else 0o002 if sealed_parent else 0o022)
                and (not unaliased or info.st_nlink == 1)
                and info.st_size <= _MAX_SOURCE,
                reason,
                locator=str(path),
            )
            raw = stream.read(_MAX_SOURCE + 1)
            after = path.stat(follow_symlinks=False)
            require(
                len(raw) <= _MAX_SOURCE
                and (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                reason,
                locator=str(path),
            )
            return raw
    except OSError as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc


def _entry_exists(path: Path, reason: str) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc
    return True


def read_native_ledger(ledger, authority: Path, *, sources: list | None = None) -> dict:
    """Decode protected bytes under the caller's existing native ledger lock."""
    raw = _read_bytes(ledger.path, authority, "native_ledger_unavailable")
    if sources is not None:
        sources.append(_file_source(ledger.path, raw, "review-cycle-ledger"))
    return ledger._decode(raw, project_corrections=False)


def protected_record(path: Path, root: Path, reason: str) -> tuple[dict, str]:
    raw = _read_bytes(path, root, reason)
    try:
        value = json.loads(raw, object_pairs_hook=unique_json)
        require(isinstance(value, dict), reason, locator=str(path))
    except (UnicodeError, ValueError) as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc
    return value, hashlib.sha256(raw).hexdigest()


def _probe(repo: Path, *args: str):
    try:
        result = cycle._subject_probe(repo, "--no-replace-objects", *args)
    except ValueError as exc:
        raise CloseObservationHold("git_probe_unavailable") from exc
    require(not result.stderr, "git_probe_unavailable")
    return result


def _git(repo: Path, *args: str) -> str:
    result = _probe(repo, *args)
    require(result.returncode == 0, "git_probe_unavailable")
    return result.stdout


def _lane_key(branch: str) -> str:
    name = re.sub(r"[^A-Za-z0-9-]", "-", branch.encode().decode("latin1"))[:40].lstrip("-") or "lane"
    return f"{name}-{hashlib.sha256(branch.encode()).hexdigest()[:8]}"


def _lane_prefix(branch: str) -> str:
    return f"refs/workbay/remote-agent/{_lane_key(branch)}-"


def _lane_prefixes(branch: str) -> tuple[str, str]:
    key = _lane_key(branch)
    return (f"refs/workbay/remote-agent/{key}-", f"refs/heads/{key}-")


def _ref_rows(output: str, ref: str, prefixes: str | tuple[str, ...], exact: set[str]) -> dict[str, str]:
    prefix_tuple = (prefixes,) if isinstance(prefixes, str) else prefixes
    result = {}
    for line in output.splitlines():
        parts = line.split("\t")
        require(len(parts) == 2 and _matches(parts[0], OID), "ref_inventory_unavailable")
        oid, name = parts
        if name == ref or any(name.startswith(prefix) for prefix in prefix_tuple) or name in exact:
            result[name] = oid
    return result


@dataclass(frozen=True)
class _GitObjectStore:
    locator: str
    directory: str
    host: str | None = None
    port: int | None = None

    def read(self, *args: str, input: str | None = None) -> str:
        reason = "git_history_unavailable"
        try:
            if self.host is None:
                result = cycle._subject_probe(
                    Path(self.directory), "--no-replace-objects", *args, input=input,
                )
            else:
                argv = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                        "-o", "ConnectTimeout=5"]
                if self.port is not None:
                    argv.extend(("-p", str(self.port)))
                command = shlex.join([
                    "env", "GIT_NO_LAZY_FETCH=1", "git", "--no-replace-objects", "-C", self.directory, *args,
                ])
                result = run_subprocess(
                    [*argv, "--", self.host, command], input=input or "", text=True,
                    timeout=10, capture_output=True,
                )
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise CloseObservationHold(reason, locator=self.locator) from exc
        require(result.returncode == 0 and not result.stderr, reason, locator=self.locator)
        return result.stdout


def _transport_object_store(repo: Path, url: str, locators: set[str]) -> _GitObjectStore:
    reason = "git_history_unavailable"
    label = ", ".join(sorted(locators))
    parsed = urlsplit(url)
    if parsed.scheme == "file" and parsed.netloc in ("", "localhost"):
        path = (repo / unquote(parsed.path)).resolve(strict=True)
        return _GitObjectStore(str(path), str(path))
    if not parsed.scheme and ":" not in url:
        path = (repo / url).resolve(strict=True)
        return _GitObjectStore(str(path), str(path))
    if parsed.scheme == "ssh":
        host = parsed.hostname
        if host and parsed.username:
            host = unquote(parsed.username) + "@" + host
        directory, port = unquote(parsed.path), parsed.port
    elif (scp := re.fullmatch(r"([^:/]+):([^:].*)", url)) is not None:
        host, directory = scp.groups()
        port = None
    else:
        raise CloseObservationHold(reason, locator=label)
    require(host and not host.startswith("-") and not any(char.isspace() for char in host)
            and directory and "\0" not in directory, reason, locator=label)
    return _GitObjectStore(label, directory, host, port)


def collect_git_inventory(
    repo: Path, branch: str, dispatch_refs: set[str], *, remote_required: bool,
    object_stores: list[_GitObjectStore] | None = None,
    sources: list | None = None,
) -> dict:
    """Census issuer and dispatch refs in the canonical store and configured destinations."""
    from .adapters.remote_exec import _parse_remote_gate_env_line

    ref = "refs/heads/" + branch
    prefixes = _lane_prefixes(branch)
    prefix = prefixes[0]
    prefix_globs = tuple(item + "*" for item in prefixes)
    _git(repo, "check-ref-format", ref)
    local_args = ("for-each-ref", "--format=%(objectname)%09%(refname)", ref, *prefix_globs, *sorted(dispatch_refs))
    local_output = _git(repo, *local_args)
    local = _ref_rows(local_output, ref, prefixes, dispatch_refs)
    if sources is not None:
        sources.append(_git_source(str(repo), local_args, local_output))
    checked = [{"store": str(repo), "refs": local}]
    targets: dict[str, set[str]] = {}
    host = resolve_remote_gate_host(repo)
    env_file = repo / ".workbay" / "remote-gate.env"
    settings = {}
    if _entry_exists(env_file, "dispatch_destination_unavailable"):
        raw = _read_bytes(env_file, repo, "dispatch_destination_unavailable", private=False)
        if sources is not None:
            sources.append(_file_source(env_file, raw, "remote-gate.env"))
        for line in raw.decode().splitlines():
            pair = _parse_remote_gate_env_line(line)
            if isinstance(pair, tuple):
                settings[pair[0]] = pair[1]
    if host:
        directory = os.environ.get("WORKBAY_REMOTE_GATE_DIR") or settings.get("REMOTE_GATE_DIR") or "src/" + repo.name
        require(
            not host.startswith("-") and not any(char.isspace() for char in host + directory),
            "dispatch_destination_unavailable",
        )
        targets.setdefault(f"{host}:{directory}", set()).add("configured_remote_gate")
    require(not remote_required or targets, "dispatch_destination_unavailable")
    for remote in _git(repo, "remote").splitlines():
        urls = _git(repo, "remote", "get-url", "--push", "--all", "--", remote).splitlines()
        require(urls, "dispatch_destination_unavailable")
        for url in urls:
            targets.setdefault(url, set()).add("git-remote:" + remote)
    for target, locators in sorted(targets.items()):
        patterns = (ref, *prefix_globs, *sorted(dispatch_refs))
        output = _git(repo, "ls-remote", "--refs", "--", target, *patterns)
        if sources is not None:
            sources.append(_git_source(", ".join(sorted(locators)), ("ls-remote", "--refs", "--", *patterns), output))
        refs = _ref_rows(output, ref, prefixes, dispatch_refs)
        # Only the native worker receives attempt objects outside published refs.
        if object_stores is not None and "configured_remote_gate" in locators and not refs and not local:
            url = _git(repo, "ls-remote", "--get-url", "--", target).strip()
            try:
                store = _transport_object_store(repo, url, locators)
            except (OSError, ValueError) as exc:
                raise CloseObservationHold("git_history_unavailable", stores=sorted(locators)) from exc
            if store not in object_stores:
                object_stores.append(store)
        # A configured Git URL may contain credentials. Retain its exact
        # identity by digest and a walkable config locator.
        checked.append(
            {
                "store": ", ".join(sorted(locators)),
                "endpoint_sha256": hashlib.sha256(target.encode()).hexdigest(),
                "refs": refs,
            }
        )
    present = {name for entry in checked for name in entry["refs"]}
    return dict(
        dispatch_refs_present=sorted(present - {ref}),
        issuer_ref_present=ref in present,
        dispatch_ref_observations=checked,
        dispatch_ref_names=sorted({ref, *dispatch_refs}),
        dispatch_ref_prefix=prefix,
        dispatch_ref_prefixes=list(prefixes),
    )


def collect_git_cut(
    repo: Path, base: str, tip: str, tree: str, integration: str, *, sources: list | None = None,
) -> dict:
    """Resolve pinned commits and measure the diff and integration ancestry."""
    def resolve(revision: str) -> str:
        args = ("rev-parse", "--verify", revision)
        output = _git(repo, *args)
        if sources is not None:
            sources.append(_git_source(str(repo), args, output))
        return output.strip()

    require(all(_matches(value, OID) for value in (base, tip, tree)), "terminal_tip_unproven", terminal_tip_sha=None)
    require(
        resolve(tip + "^{commit}") == tip
        and resolve(base + "^{commit}") == base
        and resolve(tip + "^{tree}") == tree,
        "terminal_tip_unproven",
        terminal_tip_sha=None,
    )
    ref = "refs/heads/" + integration
    _git(repo, "check-ref-format", ref)
    head = resolve(ref + "^{commit}")
    require(_matches(head, OID), "integration_tip_unproven")
    args = ("merge-base", "--is-ancestor", tip, head)
    ancestry = _probe(repo, *args)
    if ancestry.returncode == 1:
        require(_git(repo, "rev-parse", "--is-shallow-repository").strip() == "false", "integration_ancestry_unknown")
        raise CloseObservationHold("tip_not_on_integration", disposition="contradiction")
    require(ancestry.returncode == 0, "integration_ancestry_unknown")
    if sources is not None:
        sources.append(_git_source(str(repo), args, ancestry.stdout))
    # Git emits raw filenames with -z, including bytes outside UTF-8.
    git_env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}
    }
    git_env["GIT_NO_LAZY_FETCH"] = "1"
    try:
        diff = run_subprocess(
            [
                "git",
                "--no-replace-objects",
                "-C",
                str(repo),
                "diff",
                "--no-renames",
                "--name-only",
                "-z",
                base,
                tip,
                "--",
            ],
            env=git_env,
            check=True,
            timeout=5,
            capture_output=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CloseObservationHold("git_diff_unavailable") from exc
    paths = diff.stdout
    require(not diff.stderr and (not paths or paths.endswith(b"\0")), "git_diff_unavailable")
    return dict(
        base_sha=base,
        terminal_tip_sha=tip,
        terminal_tree=tree,
        changed_file_count=paths.count(b"\0"),
        integration_branch=integration,
        integration_ref=ref,
        integration_head=head,
        tip_on_integration=True,
    )


def receipt_stem(dispatch_id: str) -> str:
    return hashlib.sha256(dispatch_id.encode()).hexdigest()


def _registration(closure: ClosureIdentity) -> dict:
    return dict(
        task_ref=closure.task_ref,
        lane_id=closure.lane_id,
        branch=closure.branch,
        worktree_path=closure.worktree_path,
        id=closure.row_id,
        identity_seq=closure.identity_seq,
        cycle_epoch=closure.closed_epoch,
        status=closure.status,
    )


def read_dispatch_receipt(path: Path, authority: Path) -> tuple[dict, dict]:
    """Read native dispatch input under its canonical owner-controlled directory."""
    reason = "receipt_inventory_incomplete"
    require(path.parent == authority / "review_cycle_dispatch" and path.suffix == ".json"
            and _matches(path.stem, DIGEST), reason, locator=str(path))
    _owned_directory_mode(authority, reason)
    _owned_directory_mode(path.parent, reason)
    return _read_json_source(path, authority, reason, "review-cycle-dispatch", private=False)


def receipt_names_issuer(receipt: dict, closure: ClosureIdentity) -> bool:
    """Select candidates inclusively. Conflicting stamps must reach the full join."""
    issuer = receipt.get("source_issuer")
    pair = dict(task_ref=closure.task_ref, lane_id=closure.lane_id)
    return (_same(issuer, {"row_id": closure.row_id}) or _same(issuer, pair)
            or _same(receipt.get("managed_identity"), pair))


def _receipt(path: Path, authority: Path, closure: ClosureIdentity) -> tuple[dict, str]:
    receipt, source = read_dispatch_receipt(path, authority)
    pair = dict(task_ref=closure.task_ref, lane_id=closure.lane_id)
    # Older managed dispatches predate the registration stamp. Their pass
    # binds the row and epoch in collect_close_inventory's ledger/Git join.
    if "source_issuer" in receipt:
        matches = _same(receipt["source_issuer"], dict(row_id=closure.row_id, **pair))
    else:
        matches = _same(receipt.get("managed_identity"), pair)
    require(matches, "receipt_issuer_mismatch", disposition="contradiction", locator=str(path))
    return receipt, source["sha256"]


def _pass_files(state_dir: Path, closure: ClosureIdentity, complete: dict) -> dict:
    reason = "pass_inventory_incomplete"
    require(
        complete
        and all(
            isinstance(key, str) and _valid_pass_id(key) and _matches(digest, DIGEST)
            for key, digest in complete.items()
        ),
        reason,
    )
    records, sources, _ = _combined_pass_inventory(state_dir, closure, [], retained_ids=set(complete))
    for key, source in sources.items():
        require(source["sha256"] == complete[key], reason, locator=source["locator"])
    return records


def _require_no_pass_contribution(result: dict) -> None:
    checkpoints = result.get("checkpoint_commits")
    require(
        isinstance(checkpoints, list) and all(_matches(item, OID) for item in checkpoints),
        "checkpoint_inventory_unknown",
        checkpoint_commits=None,
    )
    require(
        not checkpoints,
        "issuer_checkpoint_present",
        disposition="contradiction",
        checkpoint_commits=sorted(set(checkpoints)),
    )
    landed = result.get("commit_landed")
    require(landed is not True, "issuer_landing_present", disposition="contradiction", commit_landed=True)
    require(landed is False and result.get("work_status") == "not_landed", "pass_landing_unknown", commit_landed=None)
    require(
        not any(
            result.get(field)
            for field in (
                "landing_commit_sha",
                "commit_sha",
                "harvest",
                "harvest_receipt",
                "retained_products",
            )
        ),
        "issuer_product_present",
        disposition="contradiction",
    )


def _check_bound_jobs(
    state_dir: Path, closure: ClosureIdentity, passes: dict, *, sources: list | None = None,
) -> tuple[str, dict]:
    """Check current native jobs and join retained completions to their passes."""
    directory = state_dir / "remote-jobs" / "ledger"
    jobs = {}
    if not _entry_exists(directory.parent, "job_inventory_unavailable"):
        return str(directory), jobs
    require(directory.resolve(strict=True) == directory, "job_inventory_unavailable", locator=str(directory))
    for path in sorted(directory.iterdir()):
        if path.suffix != ".json" or not path.name.startswith("j"):
            continue
        if sources is None:
            record, _ = protected_record(path, state_dir, "job_inventory_unavailable")
        else:
            record, source = _read_json_source(path, state_dir, "job_inventory_unavailable", "remote-job-ledger")
            sources.append(source)
        spec = record.get("spec")
        require(
            isinstance(spec, dict) and JOB_ID_RE.fullmatch(path.stem) and spec.get("job_id") == path.stem,
            "job_inventory_unavailable", locator=str(path),
        )
        pass_id = spec.get("pass_id")
        pair = spec.get("task_ref"), spec.get("lane_id")
        if pass_id not in passes and pair != (closure.task_ref, closure.lane_id):
            continue
        require(record.get("stage") == "completed", "issuer_job_not_quiescent", locator=str(path))
        state = passes.get(pass_id) or {}
        result = state.get("result") or {}
        attempt = spec.get("attempt")
        require(
            sources is not None and state
            and pair == (closure.task_ref, closure.lane_id) and spec.get("branch") == closure.branch
            and type(attempt) is int and attempt >= 0 and path.stem == job_id_for(*pair, pass_id, attempt)
            and path.stem in (state.get("job_id"), result.get("job_id"))
            and type(record.get("rc")) is int and isinstance(record.get("completed_at"), str)
            and not record.get("remote_state") and not record.get("loss_cause"),
            "job_terminal_evidence_unavailable", locator=str(path),
        )
        jobs[path.stem] = dict(
            pass_id=pass_id, attempt=attempt, stage=record["stage"], rc=record["rc"],
            completed_at=record["completed_at"], sha256=source["sha256"],
        )
    return str(directory), jobs


def _check_completion_events(conn: sqlite3.Connection, closure: ClosureIdentity) -> None:
    event = conn.execute(
        "SELECT event_id FROM lane_completion_events "
        "WHERE lane_row_id=? OR (task_ref=? AND lane_id=?) LIMIT 1",
        (closure.row_id, closure.task_ref, closure.lane_id),
    ).fetchone()
    require(event is None, "lane_completion_event_present", locator="sqlite:lane_completion_events")


def check_measurement_generation(conn: sqlite3.Connection, closure: ClosureIdentity) -> None:
    """Reject a reclosed issuer using retained native cuts where available."""
    earlier = conn.execute(
        "SELECT 1 FROM worktree_lane_close_evidence WHERE task_ref=? AND lane_id=? "
        "AND kind='measurement' AND lane_row_id=? AND identity_seq=? AND closed_epoch!=? LIMIT 1",
        (closure.task_ref, closure.lane_id, *closure.key),
    ).fetchone()
    require(earlier is None, "issuer_reopened", locator="sqlite:worktree_lane_close_evidence")
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lane_resource_producer_evidence'"
    ).fetchone() is None:
        return
    for raw, digest, locator in conn.execute(
        "SELECT payload_json, digest, ref FROM lane_resource_producer_evidence "
        "WHERE json_extract(payload_json, '$.task_ref')=? AND json_extract(payload_json, '$.lane_id')=?",
        (closure.task_ref, closure.lane_id),
    ):
        require(digest == "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
                "lane_history_unavailable", locator=locator)
        lane = json.loads(raw)["lane"]
        if (lane["id"] == closure.row_id and lane["identity_seq"] == closure.identity_seq
                and lane["status"] in TERMINAL_LANE_STATUSES):
            require(lane["cycle_epoch"] == closure.closed_epoch, "issuer_reopened", locator=locator)


def _sibling_pass_epochs(conn: sqlite3.Connection, closure: ClosureIdentity, facts: dict, issued: int):
    """Read first-publication bindings from indexed, owner-controlled pass files."""
    # These locators are built by collect_close_inventory from the native state directory.
    sources = facts.get("pass_sources", [])
    if not sources:
        return
    state_dir = Path(sources[0]["locator"]).parent
    indexed, doubts, index_source = _native_pass_index(state_dir)
    doubtful = {key for key, _ in doubts}
    digests = facts.get("file_sha256s")
    if digests is not None:
        require(index_source["sha256"] == digests.get(index_source["locator"]), "close_observation_changed")
    reason = "issuer_opening_unproven"
    for key, pair in indexed.items():
        if pair == (closure.task_ref, closure.lane_id):
            continue
        path = state_dir / f"offload-pass-{key}.json"
        try:
            state, source = _read_json_source(path, state_dir, reason, "offload-pass", private=False)
        except CloseObservationHold:
            # An unavailable neighbor supplies no allocation evidence.
            continue
        binding = state.get("lane_cycle_binding")
        epoch = binding.get("cycle_epoch") if isinstance(binding, dict) else None
        if type(epoch) is not int or not issued < epoch < closure.closed_epoch:
            continue
        row_id = binding.get("row_id")
        base = binding.get("source_base_sha")
        if key in doubtful or type(row_id) is not int or row_id <= 0 or not _matches(base, OID):
            continue
        owner = dict(task_ref=pair[0], lane_id=pair[1])
        # The host freezes the registration at first publication. A later
        # invocation of this pass can execute in a different checkout.
        require(
            _same(binding, dict(v=1, **owner))
            and all(isinstance(binding.get(k), str) and binding[k] for k in ("branch", "worktree_path"))
            and _same(state, owner)
            and state.get("pass_id", key) == key,
            reason, locator=str(path),
        )
        if digests is not None:
            require(source["sha256"] == digests.get(str(path)), "close_observation_changed", locator=str(path))
        # A pruned registration can survive in its first-publication binding.
        # Any live registration under either key must corroborate the same row.
        for current in conn.execute(
            "SELECT id, task_ref, lane_id, cycle_epoch FROM worktree_lanes WHERE id=? OR (task_ref=? AND lane_id=?)",
            (row_id, *pair),
        ):
            require(tuple(current[:3]) == (row_id, *pair) and current[3] >= epoch, reason, locator=str(path))
        yield epoch, row_id, *pair


def _opening_snapshots(conn: sqlite3.Connection, issued: int, closed: int):
    yield from conn.execute(
        "SELECT cycle_epoch,id,task_ref,lane_id FROM worktree_lanes WHERE cycle_epoch > ? AND cycle_epoch < ?",
        (issued, closed),
    )
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lane_resource_producer_evidence'"
    ).fetchone():
        return
    for raw, digest, ref in conn.execute(
        "SELECT payload_json,digest,ref FROM lane_resource_producer_evidence "
        "WHERE json_extract(payload_json, '$.lane.cycle_epoch') BETWEEN ? AND ?", (issued + 1, closed - 1),
    ):
        reason = "issuer_opening_unproven"
        require(digest == "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), reason, locator=ref)
        snapshot = json.loads(raw, object_pairs_hook=unique_json)
        lane = snapshot["lane"]
        require(_same(lane, dict(task_ref=snapshot.get("task_ref"), lane_id=snapshot.get("lane_id"))),
                reason, locator=ref)
        yield lane["cycle_epoch"], lane["id"], lane["task_ref"], lane["lane_id"]


def check_pass_opening(conn: sqlite3.Connection, closure: ClosureIdentity, facts: dict) -> None:
    """Prove the bound epoch belongs to the opening that reached this close."""
    issued = min(facts["issuing_epochs"])
    row = conn.execute(f"SELECT first_epoch FROM {OPENINGS} WHERE lane_row_id=?", (closure.row_id,)).fetchone()
    if row is not None:
        require(row[0] is not None, "issuer_opening_unproven", locator=f"sqlite:{OPENINGS}")
        require(row[0] <= issued < closure.closed_epoch, "issuer_reopened", locator=f"sqlite:{OPENINGS}")
        return
    # Every reopen since installation (or repair) leaves a marker. Without
    # one, a pass issued strictly after that cut belongs to this opening.
    covered = opening_coverage(conn)
    require(covered is not None, "issuer_opening_unproven", locator="sqlite:schema_meta")
    if issued > covered:
        return
    # The allocator is global. Each intervening epoch must belong to exactly
    # one other registration, even if a later transition overwrote its live epoch.
    reason = "issuer_opening_unproven"
    require(len(set(facts["issuing_epochs"])) == 1, reason)
    if closure.closed_epoch == issued + 1:
        return
    owners = {}
    for epoch, row_id, task, lane in chain(
        _opening_snapshots(conn, issued, closure.closed_epoch),
        _sibling_pass_epochs(conn, closure, facts, issued),
    ):
        owner = row_id, task, lane
        require(
            type(epoch) is int and type(row_id) is int and row_id > 0 and row_id != closure.row_id
            and all(isinstance(value, str) and value for value in (task, lane))
            and (task, lane) != (closure.task_ref, closure.lane_id),
            reason,
        )
        require(owners.setdefault(epoch, owner) == owner, reason, epoch=epoch)
    require(len(owners) == closure.closed_epoch - issued - 1, reason, locator="sqlite:worktree_lanes")


def check_production_history(conn: sqlite3.Connection, closure: ClosureIdentity, base: str) -> None:
    """Refuse retained production until its products and final operation are bound."""
    _check_completion_events(conn, closure)
    rows = conn.execute(
        """SELECT evidence_id, candidate_sha, landing_sha
        FROM lane_completion_evidence
        WHERE task_ref=? AND lane_id=? AND lane_row_id=? AND (identity_seq=? OR identity_seq IS NULL)""",
        (closure.task_ref, closure.lane_id, closure.row_id, closure.identity_seq),
    )
    for record_id, candidate, landing in rows:
        locator = "sqlite:lane_completion_evidence/" + record_id
        require(
            landing is None and candidate in (None, base),
            "issuer_product_present",
            disposition="contradiction",
            locator=locator,
        )
        raise CloseObservationHold("production_inventory_unproven", locator=locator)


def collect_close_inventory(
    repo: Path,
    authority: Path,
    state_dir: Path,
    closure: ClosureIdentity,
    requested: list[str],
    ledger: dict,
    inventory: dict,
    *,
    pre_close_epoch: int | None = None,
    combined: dict | None = None,
    measurement: bool = False,
    sources: list | None = None,
) -> tuple[dict, set[str], bool]:
    """Join a trusted complete inventory to issued receipts, passes and native scope.

    The caller supplies the authority-controlled inventory and holds the lane,
    receipt and ledger locks. A close observer can require its exact observed
    pre-close epoch. Historical callers must prove the terminal cut separately.
    """
    registration = _registration(closure)
    receipts_dir = authority / "review_cycle_dispatch"
    receipts = {stem: _receipt(receipts_dir / (stem + ".json"), authority, closure) for stem in requested}
    receipt_hashes = {stem: record[1] for stem, record in receipts.items()}
    require(receipt_hashes == inventory.get("receipt_sha256s"), "receipt_inventory_incomplete")
    pass_hashes = inventory.get("pass_sha256s")
    require(isinstance(pass_hashes, dict), "pass_inventory_incomplete")
    passes = combined if combined is not None else _pass_files(state_dir, closure, pass_hashes)
    jobs_locator, jobs = _check_bound_jobs(state_dir, closure, passes, sources=sources)

    rows = ledger["rows"]
    receipt_files = set()
    known_receipts = set()
    for path in sorted(receipts_dir.iterdir()):
        if path.suffix != ".json":
            continue
        raw, source = read_dispatch_receipt(path, authority)
        if path.stem in receipts:
            require(receipt_hashes[path.stem] == source["sha256"], "close_observation_changed", locator=str(path))
        elif sources is not None:
            sources.append(source)
        receipt_files.add(path.stem)
        if receipt_names_issuer(raw, closure):
            known_receipts.add(path.stem)
    require(
        known_receipts == receipt_hashes.keys()
        and all(
            receipt_stem(dispatch_id) in receipt_files
            for dispatch_id, row in rows.items()
            if row.get("subject_branch") == closure.branch and row.get("decision") == "reserved"
        ),
        "receipt_inventory_incomplete",
    )

    base = None
    issuing_epochs = set()
    pass_sources = []
    for pass_id, record in passes.items():
        identity = {key: registration[key] for key in ("task_ref", "lane_id", "branch", "worktree_path")}
        binding = record.get("lane_cycle_binding")
        require(
            _same(record, {**identity, "orchestrator_root": str(repo)})
            and _same(binding, {**identity, "v": 1, "row_id": closure.row_id}),
            "pass_binding_mismatch" if combined else "pass_issuer_mismatch",
            disposition="unknown" if combined else "contradiction",
        )
        epoch = binding.get("cycle_epoch")
        source = binding.get("source_base_sha")
        config = record.get("completion_config")
        result = record.get("result")
        require(
            type(epoch) is int
            and (0 < epoch < closure.closed_epoch if pre_close_epoch is None else epoch == pre_close_epoch)
            and _matches(source, OID)
            and base in (None, source)
            and _same(config, dict(pass_id=pass_id, start_head_ref=source)),
            "pass_binding_mismatch" if combined else "pass_binding_unproven",
        )
        issuing_epochs.add(epoch)
        base = source
        if combined:
            from .offload_pass import _terminal_pass_terminal_at

            require(
                isinstance(result, dict) and isinstance(result.get("outcome"), str)
                and _terminal_pass_terminal_at(record) is not None,
                "pass_not_terminal", locator=str(state_dir / f"offload-pass-{pass_id}.json"),
            )
            require(record.get("pass_id", pass_id) == pass_id, "pass_binding_mismatch",
                    locator=str(state_dir / f"offload-pass-{pass_id}.json"))
        else:
            require(
                record.get("status") == "done"
                and isinstance(record.get("terminal_at"), str)
                and bool(record["terminal_at"])
                and isinstance(result, dict),
                "pass_not_quiescent",
            )
            _require_no_pass_contribution(result)
            work, _ = _collect_work_fields(state_dir, _pass_activity(record))
            _require_empty_work(work)
            require(result.get("base_sha") in (None, source) and result.get("tip_sha") in (None, source),
                    "pass_tip_mismatch")
        # A job identifier alone cannot certify completed collection or harvest.
        for job_id in (record.get("job_id"), result.get("job_id")):
            if job_id is not None:
                require(isinstance(job_id, str) and jobs.get(job_id, {}).get("pass_id") == pass_id,
                        "job_terminal_evidence_unavailable", pass_id=pass_id)
        dispatch_id = record.get("review_cycle_dispatch_id")
        if dispatch_id is None and not combined:
            require(
                result.get("outcome") in {"admission_deferred", "dispatch_refused"}
                and result.get("model_started") is False,
                "pass_reservation_unproven",
            )
        elif dispatch_id is not None:
            require(dispatch_id == pass_id and receipt_stem(pass_id) in receipts, "receipt_inventory_incomplete")
        pass_sources.append(
            dict(
                locator=str(state_dir / f"offload-pass-{pass_id}.json"),
                sha256=pass_hashes[pass_id],
                issuing_epoch=epoch,
            )
        )

    dispatch_refs, scope_sources, receipt_sources = set(), {}, []
    integration = None
    remote_required = False
    require(base is not None, "pass_inventory_incomplete")
    base_tree = _git(repo, "rev-parse", "--verify", base + "^{tree}").strip()
    if combined is None:
        # A protected clean-removal witness supplies the terminal Git cut.
        # It cannot resolve contradictory native activity in a deferred pass.
        for pass_id, record in passes.items():
            result = record["result"]
            if result.get("outcome") in _PRE_MODEL or result.get("model_started") is False:
                receipt, _ = receipts.get(receipt_stem(pass_id), ({}, None))
                observation = _collect_pass_observation(
                    state_dir, closure, record, receipt, base, base_tree, None, measurement=False,
                )
                _unstarted_pass(observation, legacy_abandoned=False)
    for stem, (receipt, digest) in receipts.items():
        identity = receipt.get("identity")
        require(isinstance(identity, dict) and isinstance(identity.get("dispatch_id"), str), "receipt_binding_unproven")
        dispatch_id = identity["dispatch_id"]
        row, state = rows.get(dispatch_id), passes.get(dispatch_id)
        decision = receipt.get("decision")
        managed = dict(
            task_ref=closure.task_ref, lane_id=closure.lane_id, pass_id=dispatch_id, worktree_path=closure.worktree_path
        )
        source = dict(branch=closure.branch, head_sha=base, head_tree=base_tree)
        require(
            receipt_stem(dispatch_id) == stem
            and isinstance(row, dict)
            and isinstance(state, dict)
            and _same(row, identity)
            and row.get("decision") == "reserved"
            and not row.get("legacy_source")
            and _same(decision, dict(status="reserved"))
            and isinstance(decision.get("row"), dict)
            and all(
                decision["row"].get(k) == row.get(k)
                for k in (
                    "dispatch_id",
                    "kind",
                    "subject_branch",
                    "round",
                    "issued_sequence",
                )
            )
            and receipt.get("managed_identity") == managed
            and receipt.get("managed_source") == source
            and (
                receipt.get("state") == "prepared"
                and not any(key in receipt for key in ("source_head", "source_tree", "source_branch"))
                or _same(receipt, dict(source_head=base, source_tree=base_tree, source_branch=closure.branch))
            )
            and state.get("review_cycle_dispatch_id") == dispatch_id
            and state.get("review_cycle") == decision
            and state.get("review_cycle_subject_branch") == row.get("subject_branch"),
            "receipt_binding_unproven",
            locator=str(receipts_dir / (stem + ".json")),
        )
        result = state["result"]
        if row.get("completion") is None:
            if measurement:
                require(receipt.get("native_completion") is None and receipt.get("settlement") is None,
                        "pass_settlement_unproven")
            else:
                require(
                    receipt.get("state") == "prepared"
                    and receipt.get("native_dispatch") is None
                    and (result.get("outcome") in _PRE_MODEL
                         or combined is not None and state.get("status") == result.get("outcome") == "abandoned"
                         and _prepared_engine_receipt(receipt))
                    and (combined is not None or result.get("model_started") is False),
                    "pass_not_quiescent",
                )
        else:
            # The review charge can settle as an error without a native lane
            # completion event. Preserve it and verify the engine's join.
            settlement = (result.get("review_cycle") or {}).get("settlement") or {}
            require(settlement.get("row") == row, "pass_settlement_unproven")
        native = receipt.get("native_dispatch")
        if native is not None:
            prefixes = _lane_prefixes(closure.branch)
            nonce = native.get("dispatch_nonce")
            dispatch_ref = native.get("dispatch_ref")
            require(
                isinstance(nonce, str)
                and re.fullmatch(r"[0-9]+-[0-9a-f]{16}", nonce)
                and isinstance(dispatch_ref, str)
                and any(dispatch_ref == prefix + nonce for prefix in prefixes),
                "dispatch_ref_unproven",
            )
            dispatch_refs.add(dispatch_ref)
            remote_required = True
        proof = receipt.get("source_task_subject")
        if proof is not None:
            require(
                _same(
                    proof,
                    dict(
                        task_ref=closure.task_ref,
                        lane_id=closure.lane_id,
                        base_sha=base,
                        tip_sha=base,
                        changed_files=[],
                    ),
                ),
                "native_scope_unproven",
            )
            name = proof.get("integration_branch")
            require(isinstance(name, str) and name and integration in (None, name), "native_scope_unproven")
            integration = name
            scope_path = _scope_path(repo, proof)
            original, scope_hash = protected_record(scope_path, authority, "native_scope_unproven")
            require(
                _same(
                    original,
                    dict(schema="workbay/task-subject-scope@1", original=proof, worktree_path=closure.worktree_path),
                ),
                "native_scope_unproven",
            )
            scope_sources[str(scope_path)] = scope_hash
        receipt_sources.append(
            dict(
                locator=str(receipts_dir / (stem + ".json")),
                sha256=digest,
                receipt_id=stem,
                dispatch_id=dispatch_id,
                subject_branch=row["subject_branch"],
                kind=row["kind"],
                round=row["round"],
                ledger_row_sha256=hashlib.sha256(
                    json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
                issuing_epoch=state["lane_cycle_binding"]["cycle_epoch"],
                source_head=base,
                source_tree=base_tree,
            )
        )
    require(integration is not None, "native_scope_unproven")
    return (
        dict(
            base_sha=base,
            integration_branch=integration,
            receipt_ids=sorted(requested),
            receipt_sha256s=receipt_hashes,
            pass_sha256s=pass_hashes,
            pass_sources=pass_sources,
            receipt_sources=receipt_sources,
            issuing_epochs=sorted(issuing_epochs),
            scope_sources=scope_sources,
            job_inventory_locator=jobs_locator,
            **({"jobs": jobs} if combined else {}),
        ),
        dispatch_refs,
        remote_required,
    )


def _git_source(store: str, args: tuple, output: str) -> dict:
    return dict(store=store, args=list(args), exit=0, output_sha256=hashlib.sha256(output.encode()).hexdigest())


def _file_source(path: Path, raw: bytes, schema: str) -> dict:
    return dict(locator=str(path), schema=schema, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def _owned_directory_mode(path: Path, reason: str) -> int:
    try:
        info = path.lstat()
    except OSError as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc
    require(
        stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o022,
        reason, locator=str(path),
    )
    return info.st_mode


def _native_pass_index(state_dir: Path) -> tuple[dict, set, dict]:
    reason = "pass_inventory_incomplete"
    path = state_dir / "offload-pass-index" / "ledger.jsonl"
    sealed = not _owned_directory_mode(state_dir, reason) & 0o077
    if not sealed:
        _owned_directory_mode(path.parent, reason)
    raw = _read_bytes(path, state_dir, reason, private=False, unaliased=True, sealed_parent=sealed)
    require(not raw or raw.endswith(b"\n"), reason, locator=str(path))
    indexed = {}
    doubts = set()
    try:
        for line in raw.splitlines():
            row = _parse_ledger_line(line)
            key, owner = row["p"], (row["t"], row["l"])
            require(_valid_pass_id(key) and all(owner), reason, locator=str(path))
            if "d" in row:
                doubts.add((key, owner))
            else:
                require(indexed.setdefault(key, owner) == owner, reason, locator=str(path))
    except (KeyError, UnicodeError, ValueError) as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc
    return indexed, doubts, _file_source(path, raw, "offload-pass-index")


def _combined_pass_inventory(
    state_dir: Path, closure: ClosureIdentity, scanned_sources: list, *, retained_ids: set[str] | None = None,
) -> tuple[dict, dict, dict]:
    reason = "pass_inventory_incomplete"
    pair = closure.task_ref, closure.lane_id
    indexed, doubts, index_source = _native_pass_index(state_dir)
    path = index_source["locator"]
    ids = {key for key, owner in indexed.items() if owner == pair}
    # A retained inventory can also bind a pre-index private file by digest.
    # Readable legacy files must always have native target membership.
    expected = ids if retained_ids is None else retained_ids
    require(expected and ids <= expected and not any(key in expected or owner == pair for key, owner in doubts),
            reason, locator=str(path))
    records, sources = {}, {}
    for record_path in sorted(state_dir.glob("offload-pass-*.json")):
        key = record_path.name.removeprefix("offload-pass-").removesuffix(".json")
        if key in indexed and key not in ids:
            # The index owns this ID. A readable contrary record can veto
            # exclusion, but an unreadable neighbor is not a target source.
            try:
                inspected = regular_bytes(record_path, limit=_MAX_SOURCE)
                scanned_sources.append(_file_source(record_path, inspected, "offload-pass"))
                neighbor = _json(inspected)
            except OSError:
                continue
            except DispositionHold as exc:
                require(str(exc) not in {"file_changed", "file_replaced"}, reason, locator=str(record_path))
                continue
            require((neighbor.get("task_ref"), neighbor.get("lane_id")) != pair, reason, locator=str(record_path))
            continue
        # Membership authorizes reading the host-owned native publication.
        # Old producers allow other readers. Only the owner may write.
        record, source = _read_json_source(record_path, state_dir, reason, "offload-pass", private=key not in ids)
        scanned_sources.append(source)
        owner = record.get("task_ref"), record.get("lane_id")
        if key not in expected:
            require(all(isinstance(item, str) and item for item in owner) and owner != pair,
                    reason, locator=str(record_path))
            continue
        require(owner == pair, "pass_binding_mismatch", locator=str(record_path))
        records[key], sources[key] = record, source
    require(records.keys() == expected, reason, locator=str(path), missing=sorted(expected - records.keys()))
    return records, sources, index_source


def _read_json_source(path: Path, root: Path, reason: str, schema: str, *, private: bool = True) -> tuple[dict, dict]:
    raw = _read_bytes(path, root, reason, private=private, unaliased=True)
    try:
        record = json.loads(raw, object_pairs_hook=unique_json)
        require(isinstance(record, dict), reason)
    except (UnicodeError, ValueError) as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc
    return record, _file_source(path, raw, schema)


def _stream_activity(raw: bytes, path: Path) -> list[str]:
    reason = "model_activity_unproven"
    require(raw.endswith(b"\n"), reason, locator=str(path))
    markers = set()
    try:
        for line in raw.splitlines():
            event = json.loads(line, object_pairs_hook=unique_json)
            require(isinstance(event, dict), reason, locator=str(path))
            for key in ("event", "type"):
                value = event.get(key)
                if value in ("exec_spawned", "turn.started", "model_process_started"):
                    markers.add(value)
            if event.get("model_process_started") is True:
                markers.add("model_process_started")
    except (UnicodeError, ValueError) as exc:
        raise CloseObservationHold(reason, locator=str(path)) from exc
    return sorted(markers)


def _attempt_directory(state_dir: Path, lane_id: str, stream: Path) -> Path:
    from .adapters.remote_exec import _spool_prefix

    directory = stream.parent
    require(
        directory.parent == state_dir
        and directory.name.startswith(_spool_prefix("remote-exec", lane_id))
        and stream.name == "agent-stream.jsonl",
        "attempt_identity_mismatch", locator=str(stream),
    )
    return directory


def _started_attempt(
    state_dir: Path, lane_id: str, branch: str, base: str, tree: str, stream: Path,
    native: dict, saved: dict | None,
) -> dict:
    directory = _attempt_directory(state_dir, lane_id, stream)
    reason = "attempt_directory_missing"
    if not _entry_exists(directory, reason):
        if saved is not None:
            # The staging reaper removes the entire spool. Reuse the original
            # projection only when its exact pass/receipt binding still agrees.
            require(
                _same(saved, dict(
                    attempt_dir=str(directory),
                    dispatch_nonce=native["dispatch_nonce"], dispatch_ref=native["dispatch_ref"],
                    remote_source_commit=base, remote_source_tree=tree, model_started=True,
                )),
                "attempt_identity_mismatch", locator=str(directory),
            )
            return saved
        raise CloseObservationHold(reason, locator=str(directory))
    _owned_directory_mode(directory, "attempt_identity_mismatch")
    raw = _read_bytes(stream, state_dir, "model_activity_unproven", private=False, unaliased=True)
    markers = _stream_activity(raw, stream)
    require(markers, "model_activity_unproven", locator=str(stream))

    patch = directory / "turn.patch"
    patch_raw = _read_bytes(patch, state_dir, "attempt_patch_missing", private=False, unaliased=True)
    sources = [_file_source(stream, raw, "agent-stream"), _file_source(patch, patch_raw, "turn.patch")]
    remainder = directory / "uncommitted-remainder.patch"
    remainder_bytes = 0
    remainder_error = False
    try:
        if _entry_exists(remainder, "pass_remainder_unavailable"):
            dirty = _read_bytes(remainder, state_dir, "pass_remainder_unavailable",
                                private=False, unaliased=True)
            remainder_bytes = len(dirty)
            sources.append(_file_source(remainder, dirty, "uncommitted-remainder.patch"))
    except CloseObservationHold:
        remainder_error = True

    provenance_path = directory / "source-provenance.json"
    provenance, source = _read_json_source(
        provenance_path, state_dir, "attempt_provenance_missing", "remote-source-provenance@1", private=False,
    )
    sources.append(source)
    require(
        _same(provenance, dict(
            schema_version=1, history_stripped=True, **native,
            requested_branch=branch, requested_ref="refs/heads/" + branch,
            agent_spec_head_sha=base, remote_source_commit=base,
        ))
        and _matches(provenance.get("sandbox_base_commit"), OID),
        "attempt_identity_mismatch", locator=str(provenance_path),
    )
    require(
        _matches(provenance.get("sandbox_base_tree"), OID)
        and provenance.get("remote_source_tree") == tree,
        "attempt_tree_mismatch", locator=str(provenance_path),
    )
    return dict(
        model_started=True,
        attempt_dir=str(directory),
        activity_markers=markers,
        patch_bytes=len(patch_raw),
        **{
            key: provenance[key]
            for key in (
                "dispatch_nonce",
                "dispatch_ref",
                "requested_branch",
                "requested_ref",
                "agent_spec_head_sha",
                "remote_source_commit",
                "remote_source_tree",
                "history_stripped",
                "sandbox_base_commit",
                "sandbox_base_tree",
            )
        },
        remainder_bytes=remainder_bytes,
        remainder_read_error=remainder_error,
        attempt_sources=sources,
    )


# Native work fields have typed empty values. In particular a corrupt zero,
# empty string or null is not interchangeable with False or an empty list.
_EMPTY_WORK = {
    "checkpoint_commits": ([],),
    "commit_landed": (False,),
    "work_status": ("not_landed",),
    "detached_checkpoint": (None,),
    "landing_commit_sha": (None,),
    "commit_sha": (None,),
    "harvest_receipt": (None,),
    "harvest": (None,),
    "retained_products": (None, []),
    "terminal_attempt_evidence_changed": (False,),
    "changed_files": ([],),
    "merge_ready": (False,),
    "salvage_candidate": (None,),
    "salvage_patch_cause": (None, "patch_empty"),
    "superseded_invocations": ([],),
    "uncommitted_remainder_present": (False,),
    "uncommitted_remainder_read_error": (False,),
    "uncommitted_remainder_bytes": (0,),
    "findings": ([],),
    "patch": (None, ""),
}
_COUNTS = (
    "observed_tokens",
    "observed_tokens_lower_bound",
    "cumulative_tokens",
    "cumulative_total",
    "turns",
    "rounds",
    "context_delta_total",
    "cost_per_accepted_output",
)
_PRE_MODEL = ("admission_deferred", "dispatch_refused")
# These terminal outcomes can describe a model attempt with no product.
# The outcome only selects a shape. The attempt and work evidence must still match.
_EMPTY_ATTEMPT_OUTCOMES = frozenset(
    {
        "abandoned",
        "error",
        "transport_failure",
        "timeout",
        "rate_limited",
        "needs_guidance",
        "no_actionable_work",
        "token_budget_exceeded",
        "self_verify_failed",
        "self_verify_inconclusive",
        "verification_failed",
        "verification_harness_error",
        "verification_skipped",
        "review_incomplete",
        "guidance_contract_invalid",
        "escalated",
        "server_stale_restart_required",
    }
)


def _values(records, fields):
    return tuple(record[key] for record in records for key in fields if key in record)


@dataclass
class _PassObservation:
    """Authenticated native inputs for one decision. Collection never infers an empty pass."""

    state: dict
    result: dict
    receipt: dict
    activity: list[dict]
    flags: tuple
    counts: tuple
    execution: tuple
    histories: tuple
    work: list[tuple]
    attempt: dict | None
    attempt_error: CloseObservationHold | None
    stream: str | None
    sources: list
    setup_sources: list


def _collect_work_fields(state_dir: Path, activity: list[dict]) -> tuple[list[tuple], list[dict]]:
    work = [(key, item[key]) for item in activity for key in _EMPTY_WORK if key in item]
    sources = []
    for locator in _values(activity, ("uncommitted_remainder_path",)):
        if locator is None:
            continue
        require(isinstance(locator, str) and locator, "pass_remainder_unavailable")
        path = Path(locator)
        try:
            raw = _read_bytes(path, state_dir, "pass_remainder_unavailable", private=False, unaliased=True)
        except CloseObservationHold:
            work.append(("uncommitted_remainder_read_error", True))
            continue
        sources.append(_file_source(path, raw, "declared-remainder"))
        work.append(("uncommitted_remainder_bytes", len(raw)))
    return work, sources


def _require_empty_work(work: list[tuple]) -> None:
    for key, value in work:
        require(
            any(type(value) is type(empty) and value == empty for empty in _EMPTY_WORK[key]),
            "pass_work_contradiction",
            field=key,
            observed=value if isinstance(value, (int, bool, str)) else None,
        )


def _pass_activity(state: dict) -> list[dict]:
    result = state["result"]
    activity = [state, result, state.get("progress", {}), result.get("progress", {})]
    require(all(isinstance(item, dict) for item in activity), "model_activity_unproven")
    return activity


def _prepared_engine_receipt(receipt: dict) -> bool:
    # shell_reserve consumes the preparation and publishes the source cut
    # before launching transport. A prepared engine reservation has neither.
    return (_same(receipt, dict(state="prepared", settlement_owner="engine"))
            and isinstance(receipt.get("token"), str) and bool(receipt["token"])
            and set(receipt) <= {"state", "settlement_owner", "token", "identity", "decision", "managed_identity",
                                 "managed_source", "source_issuer", "source_review_scope"})


def _prepared_abandonment(state: dict, receipt: dict) -> bool:
    """Confirm the v51 write-ahead execute marker preceded any shell transfer."""
    if not _prepared_engine_receipt(receipt):
        return False
    progress = state.get("progress")
    result = state["result"]
    if not isinstance(progress, dict) or result.get("progress") != progress:
        return False
    marker = progress.get("model_call_in_flight")
    backend = state.get("backend")
    return (
        isinstance(backend, str) and backend.endswith("-remote")
        and set(progress) == {"backend", "model_call_in_flight", "observed_tokens", "producer_question_count",
                              "token_budget", "token_telemetry_supported", "tokens_observed"}
        and _same(progress, dict(backend=backend, observed_tokens=None, producer_question_count=0,
                                 tokens_observed=False))
        and _same(marker, dict(cycle=1, phase="execute"))
        and set(marker) == {"cycle", "phase", "started_at"} and isinstance(marker["started_at"], str)
        and type(progress["token_telemetry_supported"]) is bool
        and _same(result.get("tokens"), dict(token_budget=progress["token_budget"],
                                             token_telemetry=progress["token_telemetry_supported"]))
        and _same(result, dict(producer_question_count=0, abandoned_at=state.get("terminal_at")))
        and state.get("model_call_in_flight") is None and result.get("model_call_in_flight") is None
    )


def _deferred_setup(
    state_dir: Path, closure: ClosureIdentity, state: dict, receipt: dict, stream: Path, base: str,
) -> list[dict]:
    """Verify an allocated stream locator for a ledger-settled, pre-model VM deferral."""
    directory = _attempt_directory(state_dir, closure.lane_id, stream)
    _owned_directory_mode(directory, "attempt_directory_missing")
    if _entry_exists(stream, "model_activity_unproven"):
        return []

    result = state["result"]
    # collect_close_inventory compares this settlement to the native ledger.
    settlement = (result.get("review_cycle") or {}).get("settlement") or {}
    row = settlement.get("row") or {}
    require(
        _same(receipt, dict(state="transport_started", settlement_owner="engine"))
        and isinstance(result.get("backend"), str) and result["backend"].endswith("-remote")
        and state.get("backend") == result["backend"]
        and result.get("defer_reason") == "vm_measured_saturation"
        and settlement.get("status") == "settled"
        and _same(row, dict(outcome_class="infra_stop", settlement_status="settled"))
        and row.get("round_charged", False) is False
        and _same(row.get("completion"), dict(outcome="admission_deferred", refusal_reason=None,
                                              blocking_ids=[], blocking_fingerprints=[]))
        and _same(result.get("dispatch_receipt"), dict(turns=0, rounds=0, observed_tokens=0)),
        "model_activity_unproven", locator=str(stream),
    )
    setup = {"brief.md", "schema.json", "spec.json", "spec.argv"}
    require({entry.name for entry in directory.iterdir()} == setup, "attempt_evidence_unavailable", locator=str(stream))
    spec, source = _read_json_source(directory / "spec.json", state_dir, "attempt_identity_mismatch",
                                     "remote-agent-spec", private=False)
    require(_same(spec, dict(branch=closure.branch, head_sha=base, backend_id=result.get("backend"))),
            "attempt_identity_mismatch", locator=str(directory / "spec.json"))
    sources = [source]
    for name in sorted(setup - {"spec.json"}):
        path = directory / name
        raw = _read_bytes(path, state_dir, "attempt_evidence_unavailable", private=False, unaliased=True)
        sources.append(_file_source(path, raw, name))
    return sources


def _collect_pass_observation(
    state_dir: Path,
    closure: ClosureIdentity,
    state: dict,
    receipt: dict,
    base: str,
    tree: str,
    saved: dict | None,
    *,
    measurement: bool,
) -> _PassObservation:
    result = state["result"]
    activity = _pass_activity(state)
    counters = [item["dispatch_receipt"] for item in activity if "dispatch_receipt" in item]
    tokens = [item["tokens"] for item in activity if "tokens" in item]
    require(all(isinstance(item, dict) for item in (*tokens, *counters)), "model_activity_unproven")
    history = [item["pass_history"] for item in counters if "pass_history" in item]
    require(all(isinstance(item, dict) for item in history), "model_activity_unproven")
    telemetry = [*activity, *counters, *tokens, *history]
    flags = _values(telemetry, ("model_started", "model_process_started", "process_ran"))
    counts = tuple(
        (key, item[key])
        for item in telemetry
        for key in _COUNTS
        if key in item
        # The native token envelope explicitly marks an unavailable total,
        # and an unaccepted output has no cost. Neither field asserts usage.
        and not (
            item[key] is None
            and (
                key == "cumulative_total"
                and item.get("token_telemetry") is False
                or key == "cost_per_accepted_output"
                or key == "observed_tokens"
                and item.get("tokens_observed") is False
            )
        )
    )
    execution = (
        *_values(activity, ("job_id", "attempt")),
        *_values(
            [*counters, *history],
            ("served_model", "observed_backend", "observed_model", "resolved_model", "routing_effective"),
        ),
    )
    histories = _values([*counters, *history], ("routing_history",))
    work, sources = _collect_work_fields(state_dir, activity)

    paths = _values(activity, ("agent_stream_path",))
    stream = next((path for path in paths if path is not None), None)
    require(
        all(path is None or isinstance(path, str) and path == stream for path in paths), "attempt_identity_mismatch"
    )
    native = receipt.get("native_dispatch")
    attempt = error = None
    setup_sources = []
    if (not measurement and stream is not None and isinstance(native, dict)
            and result.get("outcome") == "admission_deferred"):
        setup_sources = _deferred_setup(state_dir, closure, state, receipt, Path(stream), base)
    if stream is not None and isinstance(native, dict) and not setup_sources:
        try:
            attempt = _started_attempt(
                state_dir, closure.lane_id, closure.branch, base, tree, Path(stream), native, saved
            )
        except CloseObservationHold as exc:
            error = exc
            if measurement and str(exc) == "attempt_directory_missing":
                persisted = result.get("terminal_attempt_facts")
                if isinstance(persisted, dict):
                    attempt = dict(
                        patch_bytes=persisted.get("patch_bytes"),
                        sandbox_base_tree=persisted.get("sandbox_base_tree"),
                        model_started=True,
                        attempt_dir=str(Path(stream).parent),
                        dispatch_nonce=native["dispatch_nonce"],
                        dispatch_ref=native["dispatch_ref"],
                        remote_source_commit=base,
                        remote_source_tree=tree,
                    )
                else:
                    error = CloseObservationHold("attempt_evidence_unavailable", locator=stream)
    return _PassObservation(
        state=state,
        result=result,
        receipt=receipt,
        activity=activity,
        flags=flags,
        counts=counts,
        execution=execution,
        histories=histories,
        work=work,
        attempt=attempt,
        attempt_error=error,
        stream=stream,
        sources=sources,
        setup_sources=setup_sources,
    )


def _observed_counts(facts: _PassObservation, *, legacy_abandoned: bool) -> list[int]:
    # Only the v51 unstarted abandonment shape carries this unobserved total.
    counts = [n for key, n in facts.counts if not (legacy_abandoned and key == "cumulative_total" and n is None)]
    require(all(type(n) is int and n >= 0 for n in counts), "model_activity_unproven")
    return counts


def _unstarted_pass(facts: _PassObservation, *, legacy_abandoned: bool) -> dict:
    result, state = facts.result, facts.state
    outcome = result.get("outcome")
    abandoned = state["status"] == outcome == "abandoned"
    receipt = result.get("dispatch_receipt", {})
    turns, rounds = receipt.get("turns"), receipt.get("rounds")
    counts = _observed_counts(facts, legacy_abandoned=legacy_abandoned)
    require(all(type(flag) is bool for flag in facts.flags), "model_activity_unproven")
    if (facts.stream is not None and not facts.setup_sources) or any(facts.flags):
        # A conflicting positive start cannot turn a missing spool into
        # a deferred pass, even if the result reports zero turns.
        raise CloseObservationHold("attempt_evidence_unavailable")
    require(
        not any(counts)
        and (legacy_abandoned and _prepared_abandonment(state, facts.receipt)
             or all(item.get("model_call_in_flight") is None for item in facts.activity))
        and all(value is None for value in facts.execution)
        and all(value == [] for value in facts.histories)
        and "terminal_attempt_facts" not in result,
        "model_activity_unproven",
    )
    require(
        all(
            item.get("tokens_observed") is not True or type(item.get("observed_tokens")) is int
            for item in facts.activity
        ),
        "model_activity_unproven",
    )
    if abandoned:
        if not legacy_abandoned:
            require(
                result.get("token_accounting") == "observed"
                and _same(receipt, dict(tokens_complete=True, observed_tokens=0))
                and _same(result.get("tokens"), dict(token_telemetry=True, cumulative_total=0)),
                "model_activity_unproven",
            )
        require(
            isinstance(result.get("abandoned_at"), str)
            and type(turns) is int
            and turns == rounds == 0
            and (not facts.receipt or legacy_abandoned and _prepared_engine_receipt(facts.receipt)),
            "model_activity_unproven",
        )
    else:
        if "dispatch_receipt" in result:
            require(_same(receipt, dict(turns=0, rounds=0, observed_tokens=0))
                    and (receipt.get("tokens_complete") is True
                         or facts.setup_sources and receipt.get("tokens_complete") is False),
                    "model_activity_unproven")
        require(
            state["status"] == "done"
            and outcome in _PRE_MODEL
            and (
                facts.flags or facts.setup_sources
                or outcome == "admission_deferred"
                and isinstance(result.get("defer_reason"), str)
                and result["defer_reason"]
                and facts.receipt.get("state") == "prepared"
                and facts.receipt.get("native_dispatch") is None
                and all(cycle._model_or_unknown_evidence(item) is None for item in facts.activity)
            ),
            "model_activity_unproven",
        )
    projection = dict(model_started=False, activity_source="abandoned" if abandoned else "admission_deferred")
    if facts.setup_sources:
        projection.update(attempt_dir=str(Path(facts.stream).parent), attempt_sources=facts.setup_sources)
    return projection


def _empty_pass_projection(facts: _PassObservation, base: str, tree: str, *, measurement: bool) -> dict:
    """Accept a complete executed, deferred, or native unstarted abandonment shape."""
    state, result = facts.state, facts.result
    outcome = result["outcome"]
    require(all(type(flag) is bool for flag in facts.flags), "model_activity_unproven")
    _require_empty_work(facts.work)
    receipt = result.get("dispatch_receipt", {})
    turns, rounds = receipt.get("turns"), receipt.get("rounds")
    started = facts.stream is not None and not facts.setup_sources
    abandoned = state["status"] == outcome == "abandoned"
    # The abandoned-before-launch constructor emits no terminal Git fields.
    # On demand, the prepared engine receipt must also prove there was no transfer.
    legacy_abandoned = (
        abandoned
        and not started
        and isinstance(result.get("abandoned_at"), str)
        and _same(receipt, {"turns": 0, "rounds": 0, "tokens_complete": False})
        and set(receipt) == {"turns", "rounds", "tokens_complete"}
        and result.get("token_accounting") == "unknown"
        and set(result)
        <= {
            "outcome",
            "abandoned_at",
            "progress",
            "producer_question_count",
            "tokens",
            "dispatch_receipt",
            "token_accounting",
            "question_count_escalation_emitted",
        }
        and (
            _prepared_abandonment(state, facts.receipt)
            or (
                not measurement and not facts.receipt
                and state.get("progress", {}) == result.get("progress") == {}
                and result.get("producer_question_count") is None
            )
        )
        and isinstance(result.get("tokens"), dict)
        and set(result["tokens"]) == {"token_budget", "token_telemetry", "cumulative_total"}
        and result["tokens"]["cumulative_total"] is None
    )
    expected = dict(base_sha=base, tip_sha=base, checkpoint_commits=[], commit_landed=False, work_status="not_landed")
    require(legacy_abandoned or all(item.get("model_call_in_flight") is None for item in facts.activity),
            "model_activity_unproven")
    if not legacy_abandoned:
        require(all(result.get(key) == base for key in ("base_sha", "tip_sha")), "pass_tip_mismatch")
        require(_same(result, expected), "pass_work_contradiction")
    for item in facts.activity:
        require(all(item[key] == base for key in ("base_sha", "tip_sha") if key in item), "pass_tip_mismatch")

    if started:
        _observed_counts(facts, legacy_abandoned=False)
        require(False not in facts.flags and outcome in _EMPTY_ATTEMPT_OUTCOMES, "pass_work_contradiction")
        if facts.attempt is None and facts.attempt_error is not None:
            raise facts.attempt_error
        attempt = facts.attempt
        require(attempt is not None, "model_activity_unproven")
        require(not attempt.get("remainder_read_error"), "pass_remainder_unavailable")
        require(
            type(attempt.get("patch_bytes")) is int
            and attempt["patch_bytes"] == 0
            and attempt.get("remainder_bytes", 0) == 0,
            "attempt_patch_nonempty",
        )
        require(attempt.get("sandbox_base_tree") == tree, "attempt_tree_mismatch")
        if "terminal_attempt_facts" in result:
            require(
                _same(result["terminal_attempt_facts"], dict(patch_bytes=0, sandbox_base_tree=tree)),
                "attempt_evidence_changed",
            )
    else:
        attempt = _unstarted_pass(facts, legacy_abandoned=legacy_abandoned)
    return dict(
        attempt,
        status=state["status"],
        terminal_at=state["terminal_at"],
        outcome=outcome,
        binding=state["lane_cycle_binding"],
        turns=turns,
        rounds=rounds,
        **{key: result.get(key) for key in expected},
    )


def _combined_pass_facts(
    state_dir: Path,
    closure: ClosureIdentity,
    pass_id: str,
    state: dict,
    receipt: dict | None,
    base: str,
    tree: str,
    saved: dict | None,
    *,
    measurement: bool = False,
) -> dict:
    try:
        facts = _collect_pass_observation(
            state_dir, closure, state, receipt or {}, base, tree, saved, measurement=measurement
        )
        projection = _empty_pass_projection(facts, base, tree, measurement=measurement)
        if facts.sources:
            projection["remainder_sources"] = facts.sources
        return projection
    except CloseObservationHold as exc:
        exc.diagnostic.setdefault("locator", str(state_dir / f"offload-pass-{pass_id}.json"))
        raise


def check_combined_history(conn: sqlite3.Connection, closure: ClosureIdentity, base: str) -> dict:
    """Check the issuer projection on the caller's native SQLite snapshot."""
    pair = (closure.task_ref, closure.lane_id)
    params = (closure.row_id, *pair)
    queries = {
        "registration": (
            "SELECT id, task_ref, lane_id, identity_seq, cycle_epoch, status, branch_tip_sha, landing_commit_sha "
            "FROM worktree_lanes WHERE id=? OR (task_ref=? AND lane_id=?) ORDER BY id", params,
        ),
        "completion_events": (
            "SELECT event_id, identity_revision FROM lane_completion_events "
            "WHERE lane_row_id=? OR (task_ref=? AND lane_id=?) ORDER BY event_id", params,
        ),
        "completion_evidence": (
            "SELECT evidence_id, lane_row_id, identity_seq, evidence_kind, operation_phase, candidate_sha, "
            "landing_sha, operation_id, payload_json FROM lane_completion_evidence "
            "WHERE lane_row_id=? OR (task_ref=? AND lane_id=?) ORDER BY evidence_id", params,
        ),
        "landing_decisions": (
            "SELECT id, commit_sha FROM decisions WHERE task_ref=? AND decision=? ORDER BY id",
            (closure.task_ref, f"lane_landed_{closure.task_ref}_{closure.lane_id}"),
        ),
    }
    sources = {}
    try:
        for key, (sql, bind) in queries.items():
            rows = [dict(row) for row in conn.execute(sql, bind)]
            sources[key] = dict(sql=sql, parameters=list(bind), rows=rows)
    except sqlite3.Error as exc:
        raise CloseObservationHold("lane_history_unavailable", locator="sqlite:handoff") from exc
    require(
        not sources["completion_events"]["rows"],
        "lane_completion_event_present", locator="sqlite:lane_completion_events",
    )
    require(not sources["landing_decisions"]["rows"], "landing_present", locator="sqlite:decisions")
    for row in sources["registration"]["rows"]:
        require(
            _same(row, {key: _registration(closure)[key] for key in (
                "id", "task_ref", "lane_id", "identity_seq", "cycle_epoch", "status",
            )}),
            "issuer_reopened", locator="sqlite:worktree_lanes",
        )
        require(row["landing_commit_sha"] is None, "landing_present", locator="sqlite:worktree_lanes")
        require(row["branch_tip_sha"] in (None, base), "pass_tip_mismatch", locator="sqlite:worktree_lanes")
    for row in sources["completion_evidence"]["rows"]:
        locator = "sqlite:lane_completion_evidence/" + row["evidence_id"]
        require(
            row["lane_row_id"] == closure.row_id and row["identity_seq"] == closure.identity_seq,
            "lane_history_unavailable", locator=locator,
        )
        require(row["landing_sha"] is None, "landing_present", locator=locator)
        require(row["candidate_sha"] in (None, base), "checkpoint_present", locator=locator)
        # An open/admission or contract link can precede any production.
        # Other native evidence may describe retained work or a history gap.
        require(
            row["evidence_kind"] in {"production_admission", "production_contract_link"},
            "lane_history_unavailable", locator=locator,
        )
        row["payload_sha256"] = hashlib.sha256(row.pop("payload_json").encode()).hexdigest()
    db = next(row["file"] for row in conn.execute("PRAGMA database_list") if row["name"] == "main")
    return dict(
        locator=db, closure=_registration(closure), queries=sources,
        projection_sha256=_projection_sha256(sources),
    )


def _projection_sha256(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _attributable_commits(
    stores: list[_GitObjectStore], base: str, closure: ClosureIdentity, dispatch_refs: set[str],
) -> list[dict]:
    from .commit_subject import sanitize_lane_id_for_commit_message
    from .landing_log import parse_landing_receipt
    from .offload_pass import build_checkpoint_commit_message

    reason = "git_history_unavailable"
    scope = []
    marker = f"Offload turn for lane {sanitize_lane_id_for_commit_message(closure.lane_id)}."
    landing = f"Workbay-Lane: {closure.task_ref}/{closure.lane_id}"
    for store in stores:
        census = ("cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)")
        log = ("log", "-z", "--no-walk=unsorted", "--format=%H%x00%P%x00%B", "--stdin")
        reflog = ("reflog", "show", "--all", "-z", "--format=%H%x00%gD")
        shallow = ("rev-parse", "--is-shallow-repository")
        probes = []

        def probe(args: tuple, *, input: str | None = None) -> str:
            output = store.read(*args, input=input)
            probes.append(_git_source(store.locator, args, output))
            return output

        listing = probe(census)
        objects = [line.split(" ") for line in listing.splitlines()]
        require(all(len(row) == 2 and _matches(row[0], OID) for row in objects), reason, locator=store.locator)
        commits = sorted(oid for oid, kind in objects if kind == "commit")
        require(probe(shallow).strip() == "false", reason, locator=store.locator)
        output = probe(log, input="\n".join(commits) + "\n") if commits else ""
        reflogs = probe(reflog).split("\0")
        require(reflogs[-1] == "" and (len(reflogs) - 1) % 2 == 0, reason, locator=store.locator)
        lane_ref = "refs/heads/" + closure.branch
        prefixes = _lane_prefixes(closure.branch)
        for oid, selector in zip(reflogs[0:-1:2], reflogs[1:-1:2], strict=True):
            ref = selector.rpartition("@{")[0]
            if oid != base and (
                ref == lane_ref
                or ref in dispatch_refs
                or any(ref.startswith(prefix) for prefix in prefixes)
            ):
                raise CloseObservationHold("attributable_commit_present", locator=store.locator, commit=oid)
        fields = output.split("\0")
        require(fields[-1] == "" and (len(fields) - 1) % 3 == 0, reason, locator=store.locator)
        messages = {
            oid: (parents.split(), message) for oid, parents, message in zip(
                fields[0:-1:3], fields[1:-1:3], fields[2:-1:3], strict=True,
            )
        }
        require(
            messages.keys() == set(commits) and all(
                parent in messages for parents, _ in messages.values() for parent in parents
            ),
            reason, locator=store.locator,
        )
        # Propagate from B through every surviving parent edge, including
        # unreachable objects. A reference disappearing cannot hide a commit.
        children = {}
        for oid, (parents, _) in messages.items():
            for parent in parents:
                children.setdefault(parent, []).append(oid)
        pending = list(children.get(base, ()))
        descendants = set(pending)
        while pending:
            for child in children.get(pending.pop(), ()):
                if child not in descendants:
                    descendants.add(child)
                    pending.append(child)
        for oid, (parents, message) in messages.items():
            lines = message.splitlines()
            subject = lines[0] if lines else ""
            checkpoint = re.fullmatch(r"wip\(offload\): .* checkpoint ([0-9]{1,9})", subject)
            if landing in lines:
                receipt = parse_landing_receipt(message)
                require(
                    not isinstance(receipt, dict) and receipt is not None
                    and (receipt.task_ref, receipt.lane_id) == (closure.task_ref, closure.lane_id)
                    and len(parents) == 2 and receipt.tip in parents,
                    "attribution_unresolved", locator=store.locator, commit=oid,
                )
                raise CloseObservationHold("attributable_commit_present", locator=store.locator, commit=oid)
            attributed = (
                subject.startswith("offload: ") and marker in lines[1:]
                or checkpoint is not None and subject == build_checkpoint_commit_message(
                    lane_id=closure.lane_id, checkpoint_number=int(checkpoint[1]),
                )
            )
            if attributed:
                require(oid in descendants, "attribution_unresolved", locator=store.locator, commit=oid)
                raise CloseObservationHold("attributable_commit_present", locator=store.locator, commit=oid)
        scope.append(dict(
            store=store.locator, commit_count=len(commits), descendant_count=len(descendants), probes=probes,
        ))
    return scope


def _worktree_absence(repo: Path, closure: ClosureIdentity, sources: list) -> None:
    args = ("worktree", "list", "--porcelain", "-z")
    output = _git(repo, *args)
    sources.append(_git_source(str(repo), args, output))
    worktrees = output.split("\0")
    require(
        "branch refs/heads/" + closure.branch not in worktrees
        and "worktree " + closure.worktree_path not in worktrees
        and not _entry_exists(Path(closure.worktree_path), "issuer_worktree_unavailable"),
        "issuer_worktree_present", locator=closure.worktree_path,
    )


def _collect_combined_close(
    repo: Path, authority: Path, state_dir: Path, closure: ClosureIdentity, requested: list[str],
    ledger: dict, *, ledger_source: dict | None, previous: dict | None, measurement: bool = False,
) -> dict:
    scanned_sources = []
    git_sources = []
    passes, pass_sources, index_source = _combined_pass_inventory(state_dir, closure, scanned_sources)
    receipt_sources = {}
    for stem in requested:
        path = authority / "review_cycle_dispatch" / (stem + ".json")
        receipt, source = read_dispatch_receipt(path, authority)
        receipt_sources[stem] = receipt, source
    inventory = dict(
        receipt_sha256s={key: source["sha256"] for key, (_, source) in receipt_sources.items()},
        pass_sha256s={key: source["sha256"] for key, source in pass_sources.items()},
    )
    facts, dispatch_refs, remote_required = collect_close_inventory(
        repo, authority, state_dir, closure, requested, ledger, inventory,
        combined=passes, sources=scanned_sources, measurement=measurement,
    )
    if previous is not None:
        for key in ("receipt_sha256s", "pass_sha256s", "scope_sources", "receipt_sources"):
            require(previous.get(key) == facts[key], "close_observation_changed", source=key)
    stores = [_GitObjectStore(str(repo), str(repo))]
    git_inventory = collect_git_inventory(
        repo, closure.branch, dispatch_refs, remote_required=remote_required, object_stores=stores, sources=git_sources,
    )
    require(
        not git_inventory["issuer_ref_present"] and not git_inventory["dispatch_refs_present"],
        "issuer_ref_present", refs=git_inventory["dispatch_ref_names"],
    )
    if not measurement:
        _worktree_absence(repo, closure, git_sources)
    base = facts["base_sha"]
    tree = _git(repo, "rev-parse", "--verify", base + "^{tree}").strip()
    try:
        cut = collect_git_cut(repo, base, base, tree, facts["integration_branch"], sources=git_sources)
    except CloseObservationHold as exc:
        raise CloseObservationHold(str(exc), locator=f"refs/heads/{facts['integration_branch']}") from exc
    projection = {}
    for pass_id, state in passes.items():
        saved = (previous or {}).get("passes", {}).get(pass_id)
        abandoned = state["result"].get("abandoned_pass_ids", [])
        require(
            isinstance(abandoned, list) and all(isinstance(item, str) and item in passes for item in abandoned),
            "pass_inventory_incomplete", locator=pass_sources[pass_id]["locator"],
        )
        receipt = receipt_sources.get(receipt_stem(pass_id))
        projection[pass_id] = _combined_pass_facts(
            state_dir, closure, pass_id, state, receipt[0] if receipt else None, base, tree, saved,
            measurement=measurement,
        )
    source_manifest = [
        index_source,
        *(source for _, source in receipt_sources.values()),
        *scanned_sources,
        *(source for record in projection.values() for field in ("attempt_sources", "remainder_sources")
          for source in record.get(field, [])),
    ]
    if ledger_source is not None:
        source_manifest.append(ledger_source)
    for locator, digest in facts["scope_sources"].items():
        path = Path(locator)
        raw = _read_bytes(path, authority, "native_scope_unproven", unaliased=True)
        source = _file_source(path, raw, "workbay/task-subject-scope@1")
        require(source["sha256"] == digest, "close_observation_changed", locator=locator)
        source_manifest.append(source)
    object_scope = _attributable_commits(stores, base, closure, dispatch_refs)
    source_manifest.extend(source for source in git_sources if "bytes" in source)
    native_rows = []
    for source in facts["receipt_sources"]:
        row = ledger["rows"][source["dispatch_id"]]
        native_rows.append({key: row.get(key) for key in (
            "dispatch_id", "subject_branch", "kind", "round", "issued_sequence", "round_charged",
        )} | {"completion_outcome": (row.get("completion") or {}).get("outcome")})
    return dict(
        facts, **git_inventory, repo=str(repo), witness_kind=COMBINED_WITNESS, no_contribution=True,
        disposition="closed_without_contribution", reasons=[], passes=projection, source_manifest=source_manifest,
        file_sha256s={source["locator"]: source["sha256"] for source in source_manifest},
        terminal_sources=[{key: index_source[key] for key in ("locator", "sha256")}],
        terminal_source_kind="combined_no_contribution", terminal_tip_sha=None, terminal_tree=None, clean=None,
        base_tree=tree, changed_file_count=None, tip_on_integration=True,
        integration_ref=cut["integration_ref"], integration_head=cut["integration_head"],
        object_scope=object_scope, object_stores=[store.locator for store in stores],
        git_probes=[source for source in git_sources if "args" in source], ledger_projection=native_rows,
        **({} if measurement else {"issuer_worktree_present": False}), active_jobs=[], checkpoint_commits=[],
        landing_commit_sha=None, commit_landed=False,
    )


def collect_close_measurement(
    repo: Path, authority: Path, state_dir: Path, closure: ClosureIdentity, requested: list[str],
    ledger: dict, *, previous: dict | None = None,
) -> dict:
    """Measure a post-upgrade terminal issuer from its native publications."""
    return _collect_combined_close(
        repo, authority, state_dir, closure, requested, ledger,
        ledger_source=None, previous=previous, measurement=True,
    )


def historical_close_matches(saved: dict, current: dict) -> bool:
    """Compare decisive projections after remeasuring current contrary facts."""
    mutable = {"integration_head", "object_scope"}
    if current.get("witness_kind") == COMBINED_WITNESS:
        # Full ledger and object digests describe the original cut. Fresh
        # collection verifies the issuer projection even after unrelated appends.
        mutable.update({"file_sha256s", "source_manifest", "terminal_sources", "git_probes"})
    return all(saved.get(key) == value for key, value in current.items() if key not in mutable)


def collect_historical_close(
    repo: Path,
    authority: Path,
    state_dir: Path,
    closure: ClosureIdentity,
    requested: list[str],
    ledger: dict,
    *,
    ledger_source: dict | None = None,
    previous: dict | None = None,
) -> dict:
    """Reconstruct one cut from complete protected originals under the issuer fence.

    Callers hold the lane publication lease, receipt locks and native ledger
    lock. Repeat before publishing, and on any later use of retained evidence.
    """
    archive = authority / "retained-teardowns"
    key = "-".join(map(str, closure.key))
    inventory_path = archive / (key + ".inventory.json")
    witness_path = archive / (key + ".json")
    combined = (previous or {}).get("witness_kind") == COMBINED_WITNESS or (
        previous is None and not _entry_exists(witness_path, "terminal_tip_unproven")
    )
    ref = "refs/heads/" + closure.branch
    _git(repo, "check-ref-format", ref)
    require(
        ref not in _git(repo, "for-each-ref", "--format=%(refname)", ref).splitlines(),
        "issuer_ref_present",
        disposition="unknown" if combined else "issuer_work_ref_present",
        ref=ref,
    )
    if combined:
        try:
            return _collect_combined_close(
                repo, authority, state_dir, closure, requested, ledger,
                ledger_source=ledger_source, previous=previous,
            )
        except CloseObservationHold as exc:
            if previous is None and _entry_exists(inventory_path, "terminal_tip_unproven") and str(exc) in {
                "model_activity_unproven", "pass_tip_mismatch",
            }:
                raise CloseObservationHold(
                    "terminal_tip_unproven", terminal_tip_sha=None,
                    missing_witnesses={
                        "clean_worktree_removed": str(witness_path),
                        COMBINED_WITNESS: exc.diagnostic,
                    },
                ) from exc
            if exc.diagnostic["disposition"] != "unknown":
                facts = {key: value for key, value in exc.diagnostic.items() if key not in {"reason", "disposition"}}
                raise CloseObservationHold(str(exc), **facts) from exc
            raise
    directory = archive.stat(follow_symlinks=False) if os.path.lexists(archive) else None
    require(
        directory is not None
        and stat.S_ISDIR(directory.st_mode)
        and directory.st_uid == os.getuid()
        and not directory.st_mode & 0o077,
        "terminal_tip_unproven",
        terminal_tip_sha=None,
    )
    registration = _registration(closure)
    inventory, inventory_hash = protected_record(inventory_path, archive, "pass_inventory_incomplete")
    require(
        _same(inventory, dict(version=1, kind="terminal_inventory", repo=str(repo), registration=registration)),
        "terminal_inventory_mismatch",
        disposition="contradiction",
    )
    inventory_facts, dispatch_refs, remote_required = collect_close_inventory(
        repo,
        authority,
        state_dir,
        closure,
        requested,
        ledger,
        inventory,
    )
    stores = [_GitObjectStore(str(repo), str(repo))]
    git_inventory = collect_git_inventory(
        repo, closure.branch, dispatch_refs, remote_required=remote_required, object_stores=stores,
    )
    require(
        not git_inventory["issuer_ref_present"] and not git_inventory["dispatch_refs_present"],
        "issuer_ref_present",
        disposition="issuer_work_ref_present",
    )
    worktrees = _git(repo, "worktree", "list", "--porcelain", "-z").split("\0")
    require(
        "branch " + ref not in worktrees
        and "worktree " + closure.worktree_path not in worktrees
        and not _entry_exists(Path(closure.worktree_path), "issuer_worktree_unavailable"),
        "issuer_worktree_present",
        disposition="contradiction",
    )

    try:
        witness, witness_hash = protected_record(witness_path, archive, "terminal_tip_unproven")
        fields = {key: value for key, value in closure.fields().items() if key != "status"}
        require(
            _same(
                witness,
                dict(
                    **fields,
                    version=1,
                    kind="clean_worktree_removed",
                    repo=str(repo),
                    ref=ref,
                    clean=True,
                    ref_deleted_at_observed_tip=True,
                ),
            )
            and datetime.fromisoformat(witness["observed_at"]).utcoffset() is not None,
            "terminal_tip_unproven",
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CloseObservationHold("terminal_tip_unproven", terminal_tip_sha=None, locator=str(witness_path)) from exc
    base = inventory_facts["base_sha"]
    cut = collect_git_cut(
        repo, base, witness.get("terminal_tip_sha"), witness.get("terminal_tree"), inventory_facts["integration_branch"]
    )
    require(
        cut["changed_file_count"] == 0 and cut["terminal_tip_sha"] == base,
        "issuer_contribution_observed",
        disposition="contradiction",
    )
    return dict(
        inventory_facts,
        **cut,
        **git_inventory,
        repo=str(repo),
        disposition="closed_without_contribution",
        reasons=[],
        clean=True,
        issuer_worktree_present=False,
        active_jobs=[],
        checkpoint_commits=[],
        landing_commit_sha=None,
        commit_landed=False,
        terminal_observed_at=witness["observed_at"],
        terminal_source_kind="clean_worktree_removed",
        object_scope=_attributable_commits(stores, base, closure, dispatch_refs),
        terminal_sources=[
            dict(locator=str(witness_path), sha256=witness_hash),
            dict(locator=str(inventory_path), sha256=inventory_hash),
        ],
    )
