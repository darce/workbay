"""Bounded, derived host and VM fleet observations. See fleet-vm-inventory-v1.md.

No dispatch/reclaim APIs belong here. The cache functions are the only writers.
"""

from __future__ import annotations

import errno
import fcntl
import json
import math
import os
import signal
import stat
import subprocess
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.shared_schema import bounded_handoff_read

from . import host_resources as host
from .fleet_lineage import build_lineage_tree as build_lineage_tree

MAX_RECORD_BYTES = 128 * 1024**2
MAX_TOTAL_BYTES = 512 * 1024**2
MAX_FILES = 200_000
REFRESH_SECONDS = 30.0
CACHE_FRESH_SECONDS = 60.0
VM_FRESH_SECONDS = 120.0
_TERMINAL_LANES = frozenset({"merged", "closed", "closed_stale"})
_OBSERVER_SLOT = threading.Lock()
_VM_OBSERVER_SLOT = threading.Lock()
_NATIVE_VM_RUNNER = subprocess.run


def _iso(epoch: float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if epoch is None else epoch, timezone.utc).isoformat()


def _timestamp(value) -> float | None:
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp()
        except (ValueError, OverflowError):
            return None
    return None


def _state(observed_at, *, now=None, age=CACHE_FRESH_SECONDS):
    stamp = _timestamp(observed_at)
    if stamp is None:
        return "unknown"
    return "fresh" if 0 <= (time.time() if now is None else now) - stamp <= age else "stale"


def _unknown(reason="not_observed"):
    return {"state": "unknown", "observed_at": None, "reason": reason}


@dataclass
class _Budget:
    deadline: float
    bytes_read: int = 0
    issues: set[str] = field(default_factory=set)

    def check(self):
        if time.monotonic() >= self.deadline:
            self.issues.add("deadline")
            raise TimeoutError("deadline")

    def read(self, path: Path):
        self.check()
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("not_regular_file")
        if info.st_size > MAX_RECORD_BYTES:
            self.issues.add("oversize")
            raise ValueError("oversize")
        if info.st_size > MAX_TOTAL_BYTES - self.bytes_read:
            self.issues.add("total_bytes")
            raise ValueError("total_bytes")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            # Recheck after open: a producer may have replaced/grown the file.
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise ValueError("not_regular_file")
            if opened.st_size > MAX_RECORD_BYTES:
                self.issues.add("oversize")
                raise ValueError("oversize")
            chunks = []
            length = 0
            while True:
                self.check()
                remaining = min(MAX_RECORD_BYTES - length, MAX_TOTAL_BYTES - self.bytes_read)
                if remaining <= 0:
                    # EOF is established by size for regular files at the cap;
                    # growth during observation must never turn into an overread.
                    if os.fstat(fd).st_size > length:
                        reason = "oversize" if length >= MAX_RECORD_BYTES else "total_bytes"
                        self.issues.add(reason)
                        raise ValueError(reason)
                    break
                chunk = os.read(fd, min(1024**2, remaining))
                self.bytes_read += len(chunk)
                length += len(chunk)
                if not chunk:
                    break
                chunks.append(chunk)
            self.check()
            payload = json.loads(b"".join(chunks))
            self.check()
            if not isinstance(payload, dict):
                raise ValueError("not_an_object")
            return payload, info
        finally:
            os.close(fd)


def _read(path, budget):
    try:
        payload, info = budget.read(path)
        return payload, {"state": _state(info.st_mtime), "observed_at": _iso(info.st_mtime), "path": str(path)}
    except (OSError, ValueError, UnicodeError, TimeoutError) as exc:
        reason = str(exc) if isinstance(exc, (ValueError, TimeoutError)) else type(exc).__name__
        if not isinstance(exc, FileNotFoundError):
            budget.issues.add(reason)
        return None, dict(_unknown(reason), path=str(path))


def _locks_root_readonly(root: Path, deadline: float) -> Path:
    # host.locks_root creates its namespace, so observers resolve the same
    # git-common-root without invoking it. Never allow ambient GIT_* redirection.
    env = {k: v for k, v in os.environ.items() if k not in {"GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE"}}
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("deadline")
    result = subprocess.run(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=min(5.0, remaining),
    )
    common = Path(result.stdout.strip())
    if not common.is_absolute():
        raise ValueError("git_common_dir_not_absolute")
    return common.parent / ".workbay" / "locks"


def _held_slots(root: Path, width: int, budget: _Budget) -> int:
    # Same non-blocking flock observation as count_held_heavy_slots, but opening
    # read-only and propagating unreadable/error instead of treating it as free.
    held = 0
    for number in range(max(0, width)):
        budget.check()
        path = root / "admission" / f"slot-{number}"
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            continue
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("slot_not_regular_file")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
                held += 1
            else:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
    return held


def _admission(root: Path, budget: _Budget) -> dict:
    answer = dict(_unknown(), decision="unknown", held_slots=None, width=None, stock="not_evaluated")
    try:
        budget.check()
        resources = replace(
            host.probe_host(workspace_root=root), repo_volume_free_gib=host._probe_repo_volume_free_gib(root)
        )
        answer["snapshot"] = asdict(resources)
        answer["observed_at"] = _iso(resources.probed_at) if resources.probed_at else None
        if resources.probe_error or resources.available_ram is None or resources.pressure == "unknown":
            raise ValueError(resources.probe_error or "host_resources_unknown")
        if resources.repo_volume_free_gib is None:
            raise ValueError("repo_volume_unknown")
        budget.check()
        policy = host.load_host_memory_policy(root)
        width, _ = host.reported_width(resources, policy, host.COST_HEAVY)
        answer["width"] = width
        held = _held_slots(_locks_root_readonly(root, budget.deadline), width, budget) if width is not None else 0
        budget.check()
        verdict = host.evaluate_admission(resources, host.COST_HEAVY, policy, held, stock=None)
        answer.update(verdict.to_dict())
        answer.update(state=_state(resources.probed_at), observed_at=_iso(resources.probed_at), stock="not_evaluated")
        return answer
    except Exception as exc:  # Probe failure is evidence uncertainty, never zero occupancy.
        answer.update(state="unknown", decision="unknown", held_slots=None, reason=f"{type(exc).__name__}: {exc}")
        return answer


def _bounded(seed, collect, seconds):
    """One daemon observer limits caller latency even for a stalled filesystem/probe.

    No unbounded thread accumulation: a still-retiring worker holds the slot.
    Published sections are detached and never mutated after publication.
    """
    started = time.monotonic()
    budget = _Budget(started + seconds)
    guard = threading.Lock()
    done = threading.Event()
    result = dict(seed)
    if not _OBSERVER_SLOT.acquire(blocking=False):
        result.update(partial=True, limits_hit=["observer_busy"], state="unknown")
        if "passes" in result:
            result["passes_quantity"] = "lower_bound"
        return result

    def publish(**parts):
        with guard:
            result.update(parts)

    def run():
        try:
            collect(budget, publish)
        except Exception as exc:
            budget.issues.add(f"{type(exc).__name__}: {exc}")
        finally:
            publish(partial=bool(budget.issues), limits_hit=sorted(budget.issues), bytes_parsed=budget.bytes_read)
            _OBSERVER_SLOT.release()
            done.set()

    threading.Thread(target=run, name="workbay-fleet-observer", daemon=True).start()
    done.wait(max(0, budget.deadline - time.monotonic()))
    with guard:
        detached = dict(result)
    if not done.is_set() or time.monotonic() >= budget.deadline:
        detached.update(partial=True, limits_hit=sorted(set(detached.get("limits_hit", [])) | {"deadline"}))
    if detached.get("partial"):
        detached["state"] = "unknown"
        if "passes" in detached:
            detached["passes_quantity"] = "lower_bound"
    detached["elapsed_s"] = time.monotonic() - started
    return detached


def probe_admission(root) -> dict:
    """Observe heavy host admission; unknown stock is explicitly not evaluated."""
    seed = dict(_unknown(), decision="unknown", held_slots=None, width=None, stock="not_evaluated")
    return _bounded(seed, lambda b, p: p(**_admission(Path(root), b)), REFRESH_SECONDS)


def _entries(state: Path, budget: _Budget):
    entries = []
    inspected = 0
    try:
        with os.scandir(state) as scan:
            for entry in scan:
                budget.check()
                if inspected >= MAX_FILES:
                    budget.issues.add("files")
                    break
                inspected += 1
                try:
                    entries.append((Path(entry.path), entry.stat(follow_symlinks=False)))
                except OSError as exc:
                    budget.issues.add(type(exc).__name__)
    except OSError as exc:
        budget.issues.add(type(exc).__name__)
    return entries


def _family(name):
    for prefix in ("remote-exec-", "offload-pass-", "worker-", "review-"):
        if name.startswith(prefix):
            return prefix.rstrip("-")
    return name if name in {"fleet", "handoff.db", "handoff.db-wal", "handoff.db-shm"} else "other"


def _census(state: Path, entries, budget: _Budget, days: float, owners=None):
    families = {}
    scanned = 0
    partial = bool(budget.issues)
    for path, info in entries:
        try:
            budget.check()
            if scanned >= MAX_FILES:
                budget.issues.add("files")
                partial = True
                break
            family = families.setdefault(
                _family(path.name),
                dict(entries=0, bytes=0, aged_entries=0, no_nonterminal_owner=0, ownership_unknown=0, quantity="exact"),
            )
            family["entries"] += 1
            aged = time.time() - info.st_mtime > days * 86400
            family["aged_entries"] += int(aged)
            ownership = (owners or {}).get(str(path), "unknown")
            family["ownership_unknown"] += int(aged and ownership == "unknown")
            family["no_nonterminal_owner"] += int(aged and ownership == "terminal")
            stack = [(path, info)]
            while stack:
                budget.check()
                current, item = stack.pop()
                if scanned >= MAX_FILES:
                    budget.issues.add("files")
                    partial = True
                    break
                scanned += 1
                if stat.S_ISDIR(item.st_mode):
                    # Retain only bounded entries; never follow directory symlinks.
                    with os.scandir(current) as children:
                        for child in children:
                            budget.check()
                            if scanned + len(stack) >= MAX_FILES:
                                budget.issues.add("files")
                                partial = True
                                break
                            stack.append((Path(child.path), child.stat(follow_symlinks=False)))
                else:
                    family["bytes"] += item.st_size
            if partial:
                family["quantity"] = "lower_bound"
        except (OSError, TimeoutError) as exc:
            partial = True
            budget.issues.add(type(exc).__name__)
            if families:
                family["quantity"] = "lower_bound"
        if "deadline" in budget.issues or "files" in budget.issues:
            break
    partial = partial or bool(budget.issues)
    # Any incomplete listing can omit additional entries in any family.
    if partial:
        families = {key: dict(value, quantity="lower_bound") for key, value in families.items()}
    return dict(
        state="unknown" if partial else "fresh",
        observed_at=_iso(),
        partial=partial,
        quantity="lower_bound" if partial else "exact",
        families=families,
        files_scanned=scanned,
        census_days=days,
        limits_hit=sorted(budget.issues),
        reaper={
            "command": "python -m workbay_orchestrator_mcp.orchestration.remote_exec_staging_reaper "
            f"--staging-root {state} --git-repo {state.parent}",
            "mode": "dry_run_default_no_apply",
            "scope": "remote-exec",
            "receipt_path": None,
            "state": "not_run",
        },
    )


def census_spool(root, *, census_days=7, deadline_s=30) -> dict:
    """Host spool bytes/entries by family, with global census and deadline bounds."""
    _positive(census_days, "census_days")
    _positive(deadline_s, "deadline_s")
    seed = dict(_unknown(), partial=True, families={}, files_scanned=0, quantity="lower_bound")

    def collect(budget, publish):
        runtime = RuntimeConfig.for_repo(Path(root))
        publish(**_census(runtime.state_dir, _entries(runtime.state_dir, budget), budget, census_days))

    return _bounded(seed, collect, min(deadline_s, REFRESH_SECONDS))


def _phases(directories, budget):
    rows = []
    keys = set()
    for directory in directories:
        data, source = _read(Path(directory) / "phases.json", budget)
        values = {}
        if data and isinstance(data.get("phases"), dict):
            for name, phase in data["phases"].items():
                duration = phase.get("duration_s") if isinstance(phase, dict) else None
                if (
                    isinstance(duration, (int, float))
                    and not isinstance(duration, bool)
                    and math.isfinite(duration)
                    and duration >= 0
                ):
                    values[name] = duration
        keys.update(values)
        rows.append(dict(source, directory=directory, durations=values, partial=bool(data and data.get("partial"))))
    totals = {}
    observed = {}
    for key in sorted(keys):
        observed[key] = sum(row["durations"].get(key, 0) for row in rows)
        totals[key] = observed[key] if all(key in row["durations"] and not row["partial"] for row in rows) else None
    complete = bool(rows) and all(row["durations"] and row["state"] != "unknown" and not row["partial"] for row in rows)
    complete = complete and all(value is not None for value in totals.values())
    return dict(
        state=("stale" if any(r["state"] == "stale" for r in rows) else "fresh") if complete else "unknown",
        observed_at=_iso() if rows else None,
        totals=totals if rows else None,
        observed_totals=observed,
        cycles=rows,
        quantity="exact" if complete else "lower_bound",
    )


def _safe_directory(value, state):
    try:
        path = Path(value).resolve()
        if (
            path.parent == state.resolve()
            and path.name.startswith("remote-exec-")
            and path.is_dir()
            and not Path(value).is_symlink()
        ):
            return str(path)
    except (OSError, ValueError, TypeError):
        pass
    return None


def _passes(state, entries, budget, hours):
    cutoff = time.time() - hours * 3600
    markers = {}
    ownership = {}
    for path, info in entries:
        budget.check()
        if not path.name.startswith("remote-exec-") or not stat.S_ISDIR(info.st_mode):
            continue
        marker = path / "pass-identity.json"
        try:
            if marker.lstat().st_mtime < cutoff:
                continue
        except FileNotFoundError:
            continue
        except OSError as exc:
            budget.issues.add(type(exc).__name__)
            continue
        data, _ = _read(marker, budget)
        if data and data.get("schema_version") == 1 and isinstance(data.get("pass_id"), str) and data["pass_id"]:
            markers.setdefault(data["pass_id"], []).append((_timestamp(data.get("written_at")), str(path)))
    markers_complete = not budget.issues
    passes = []
    for path, info in entries:
        budget.check()
        if not (path.name.startswith("offload-pass-") and path.suffix == ".json") or info.st_mtime < cutoff:
            continue
        payload, source = _read(path, budget)
        if payload is None:
            passes.append(
                dict(
                    source,
                    pass_id=path.name[len("offload-pass-") : -5],
                    status="unknown",
                    remote_exec_dirs=[],
                    remote_exec_dir=None,
                    link="unknown",
                    phases=_unknown(),
                    tokens=_unknown(),
                )
            )
            continue
        config = payload.get("completion_config")
        config = config if isinstance(config, dict) else {}
        pass_id = payload.get("pass_id") or config.get("pass_id")
        # Filenames are producer pass-state identities, never lane-prefix guesses.
        pass_id = pass_id if isinstance(pass_id, str) and pass_id else path.name[len("offload-pass-") : -5]
        row = {
            key: payload.get(key)
            for key in (
                "task_ref",
                "lane_id",
                "session",
                "backend",
                "model",
                "reasoning_effort",
                "worktree_path",
                "branch",
                "status",
                "started_at",
                "terminal_at",
                "job_id",
                "dispatch_nonce",
            )
        }
        row.update(
            source,
            pass_id=pass_id,
            test_cmd=config.get("test_cmd"),
            dispatch_id=payload.get("dispatch_id") or config.get("dispatch_id"),
        )
        matches = markers.get(pass_id, [])
        matches.sort(key=lambda pair: (pair[0] is None, pair[0] or 0, pair[1]))
        ordered = all(stamp is not None for stamp, _ in matches)
        directories = [directory for _, directory in matches]
        link = "pass_identity" if directories else "unknown"
        if not directories and isinstance(payload.get("agent_stream_path"), str):
            stream = Path(payload["agent_stream_path"])
            if not stream.is_absolute():
                stream = state.parent / stream
            fallback = _safe_directory(stream.parent, state)
            if fallback:
                directories, link = [fallback], "agent_stream_path"
        row.update(
            remote_exec_dirs=directories,
            remote_exec_dir=directories[-1] if directories and ordered else None,
            link=link,
            link_order="known" if ordered else "unknown",
        )
        row["phases"] = _phases(directories, budget)
        row["link_complete"] = markers_complete
        if not markers_complete:
            # The scan may have omitted further cycles for this very pass.
            row["phases"].update(state="unknown", quantity="lower_bound")
            if row["phases"]["totals"] is not None:
                row["phases"]["totals"] = dict.fromkeys(row["phases"]["totals"])
        row["outcome"] = payload.get("result") if isinstance(payload.get("result"), dict) else _unknown()
        row["tokens"] = _pass_tokens(row["outcome"], source)
        row["routing"] = _unknown("no_linked_spec")
        if row["remote_exec_dir"]:
            spec, spec_source = _read(Path(row["remote_exec_dir"]) / "spec.json", budget)
            if spec and isinstance(spec.get("routing_effective"), dict):
                row["routing"] = dict(spec_source, routing_effective=spec["routing_effective"])
        passes.append(row)
        # Reuse the pass producer's conservative terminality predicate. Merely
        # seeing a done label is insufficient (pending remote jobs also use it).
        from .offload_pass import _terminal_pass_terminal_at

        row["terminality"] = "terminal" if _terminal_pass_terminal_at(payload) is not None else "unknown"
        # This is advisory ownership, never deletion eligibility. Unknown states hold.
        # Do not infer terminality from arbitrary status/outcome strings. This
        # is only a positive owner hold; absent proof remains ownership_unknown.
        for directory in directories:
            ownership[directory] = (
                "nonterminal" if row["status"] in {"running", "pending", "dispatching"} else "unknown"
            )
    return passes, ownership


def _pass_tokens(result, source):
    # offload_pass._build_token_telemetry_payload writes result.tokens.
    # Never substitute a token budget, lane aggregate, or cost estimate.
    tokens = result.get("tokens")
    if not isinstance(tokens, dict):
        return dict(_unknown("no_exact_pass_token_attribution"), total_tokens=None)
    value = tokens.get("cumulative_total")
    known = isinstance(value, int) and not isinstance(value, bool) and value >= 0
    complete = known and tokens.get("usage_source") == "observed" and tokens.get("tokens_complete") is not False
    observed = tokens.get("observed_tokens_lower_bound", value if known else None)
    return dict(
        state=source["state"] if complete else "unknown",
        observed_at=source["observed_at"],
        total_tokens=value if complete else None,
        observed_total_tokens=observed,
        usage_source=tokens.get("usage_source"),
        quantity="exact" if complete else "lower_bound",
        source="pass_state.result.tokens",
        telemetry=tokens,
    )


def _ledger(runtime, budget, hours):
    cutoff = _iso(time.time() - hours * 3600)

    def read(conn):
        # Columns and terminal states verified in shared_schema.HANDOFF_SCHEMA_SQL.
        # No bootstrap/migration or process-global configure_runtime call.
        lanes = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM worktree_lanes WHERE status NOT IN ('merged','closed','closed_stale') ORDER BY id LIMIT ?",
                (MAX_FILES + 1,),
            )
        ]
        metrics = [
            dict(row)
            for row in conn.execute(
                "SELECT id, task_ref, lane_id, session, cycle, phase, backend, model, input_tokens, output_tokens, "
                "cached_input_tokens, reasoning_output_tokens, total_tokens, usage_source, duration_seconds, created_at "
                "FROM turn_metrics WHERE julianday(created_at) >= julianday(?) AND COALESCE(phase,'') != 'lane_prep' ORDER BY id LIMIT ?",
                (cutoff, MAX_FILES + 1),
            )
        ]
        return lanes, metrics

    try:
        lanes, metrics = bounded_handoff_read(runtime.db_path, read, deadline=budget.deadline)
        if len(lanes) > MAX_FILES or len(metrics) > MAX_FILES:
            budget.issues.add("db_rows")
        return (
            lanes[:MAX_FILES],
            metrics[:MAX_FILES],
            dict(state="fresh", observed_at=_iso(), path=str(runtime.db_path)),
        )
    except Exception as exc:
        budget.issues.add(f"ledger:{type(exc).__name__}")
        return [], [], dict(_unknown(f"{type(exc).__name__}: {exc}"), path=str(runtime.db_path))


def _positive(value, name):
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def _vm_unknown(reason="not_observed"):
    return dict(
        _unknown(reason),
        sandboxes=[],
        processes=[],
        rows_emitted=None,
        sandboxes_state="unknown",
        processes_state="unknown",
        truncated=None,
        quantity="unknown",
        processes_quantity="unknown",
        mem_available_bytes=None,
        psi={axis: None for axis in ("cpu", "memory", "io")},
        load=None,
        df=None,
        axes={
            axis: dict(_unknown(reason), source=source)
            for axis, source in (
                ("cpu", "/proc/pressure/cpu"),
                ("memory", "/proc/pressure/memory"),
                ("io", "/proc/pressure/io"),
                ("disk", "df -P -B1"),
            )
        },
    )


def _run_vm_command(argv, *, timeout, **kwargs):
    """Bound the entire local bash/SSH process group, including inherited pipes.

    subprocess.run kills only its direct child on timeout and can then wait
    indefinitely on a descendant's pipe. The native runner owns a new session;
    only that session's process group is killed, never a remote job or sandbox.
    """
    reserve = min(0.1, timeout / 5)
    started = time.monotonic()
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=max(0, timeout - reserve - (time.monotonic() - started)))
    except BaseException:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        # Do not communicate again: an escaped child could retain a pipe.
        proc.stdout.close()
        proc.stderr.close()
        proc.wait(timeout=reserve)
        raise
    return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def _vm_document(doc, rc, *, cached=False):
    """Normalize live and cached B1 evidence before any consumer sees its rows."""
    # Failed/disabled observations have no B1 envelope. Never revive any cached
    # measurements attached to such an unknown observation.
    if cached and isinstance(doc, dict) and doc.get("state") == "unknown" and doc.get("observed_at") is None:
        reason = doc.get("reason")
        return _vm_unknown(reason if isinstance(reason, str) else "bad_json")
    source_state = doc.get("state") if cached and isinstance(doc, dict) else None
    if (
        not isinstance(doc, dict)
        or type(doc.get("schema_version")) is not int
        or doc["schema_version"] != 1
        or _timestamp(doc.get("observed_at")) is None
        or _timestamp(doc["observed_at"]) > time.time()
        or not isinstance(doc.get("psi"), dict)
        or type(doc.get("truncated")) is not bool
        or not isinstance(doc.get("unknowns"), list)
        or not all(isinstance(v, str) for v in doc["unknowns"])
    ):
        raise ValueError("invalid inventory envelope")
    source_axes = doc.get("axes", {}) if cached else {}
    if not isinstance(source_axes, dict) or not all(isinstance(axis, dict) for axis in source_axes.values()):
        raise ValueError("invalid inventory axes")
    for key in ("sandboxes", "processes"):
        rows = doc.get(key)
        if (
            not isinstance(rows, list)
            or not all(isinstance(row, dict) for row in rows)
            or doc.get(key + "_state") not in ("known", "unknown")
        ):
            raise ValueError("invalid inventory rows")
    if (
        type(doc.get("rows_emitted")) is not int
        or doc["rows_emitted"] != len(doc["sandboxes"])
        or any(not isinstance(row.get("path"), str) or not row["path"] for row in doc["sandboxes"])
        or any(type(row.get("pid")) is not int or row["pid"] <= 0 for row in doc["processes"])
    ):
        raise ValueError("invalid inventory row identity/count")
    unknowns = set(doc["unknowns"])
    for row in doc["processes"]:
        if not _number(row.get("rss_kib")):
            row["rss_kib"] = None
            doc["processes_state"] = "unknown"
            unknowns.add("processes")
    for axis in ("cpu", "memory", "io"):
        psi = doc["psi"].get(axis)
        if (
            not isinstance(psi, dict)
            or "some" not in psi
            or any(
                not isinstance(values, dict)
                or not all(_number(values.get(k)) for k in ("avg10", "avg60", "avg300", "total"))
                for values in psi.values()
            )
        ):
            doc["psi"][axis] = None
            unknowns.add("psi_" + axis)
    if not _number(doc.get("mem_available_bytes")):
        doc["mem_available_bytes"] = None
        unknowns.add("mem_available_bytes")
    load = doc.get("load")
    if not isinstance(load, list) or len(load) != 3 or not all(_number(v) for v in load):
        doc["load"] = None
        unknowns.add("load")
    mounts = doc.get("df")
    if (
        not isinstance(mounts, list)
        or not mounts
        or any(not isinstance(row, dict) or not isinstance(row.get("mount"), str) for row in mounts)
    ):
        doc["df"] = None
        unknowns.add("df")
    else:
        for row in mounts:
            for key in ("bytes", "used_bytes", "available_bytes"):
                if not _number(row.get(key)):
                    row[key] = None
                    unknowns.add("df")
    for row in doc["sandboxes"]:
        if row.get("bytes_state") != "known" or not _number(row.get("bytes")):
            row.update(bytes=None, bytes_state="unknown")
        for key in ("marker_age_seconds", "lock_holder_pid"):
            if not _number(row.get(key)):
                row[key] = None
        for key in ("git_dirty", "lock_holder_alive"):
            if row.get(key) not in ("yes", "no", "unknown"):
                row[key] = "unknown"

    # Nonfinite values in additive producer metadata cannot escape into JSON/cache.
    def finite(value):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        if isinstance(value, dict):
            return {k: finite(v) for k, v in value.items()}
        if isinstance(value, list):
            return [finite(v) for v in value]
        return value

    doc = finite(doc)
    truncated = rc == 3 or doc["truncated"]
    doc.update(
        state=source_state if source_state in ("unknown", "partial", "stale") else "partial" if rc == 3 else "fresh",
        truncated=truncated,
        quantity="lower_bound" if truncated or doc["sandboxes_state"] == "unknown" else "exact",
        processes_quantity="lower_bound" if truncated or doc["processes_state"] == "unknown" else "exact",
        unknowns=sorted(unknowns),
    )
    axes = {}
    for axis in ("cpu", "memory", "io", "disk"):
        missing = {
            "cpu": {"psi_cpu", "load"},
            "memory": {"psi_memory", "mem_available_bytes"},
            "io": {"psi_io"},
            "disk": {"df"},
        }[axis]
        axes[axis] = dict(
            state="unknown"
            if unknowns & missing or source_state == "unknown" or source_axes.get(axis, {}).get("state") == "unknown"
            else "fresh",
            observed_at=doc["observed_at"],
            source="df -P -B1" if axis == "disk" else "/proc/pressure/" + axis,
        )
    axes["cpu"].update(load=doc["load"], load_source="/proc/loadavg")
    axes["memory"].update(
        mem_available_bytes=doc["mem_available_bytes"], mem_available_source="/proc/meminfo:MemAvailable"
    )
    doc["axes"] = axes
    return _vm_freshness(doc)


def _vm_freshness(vm):
    vm = dict(vm)
    axes = vm.get("axes", {})
    if not isinstance(axes, dict) or not all(isinstance(source, dict) for source in axes.values()):
        return _vm_unknown("bad_json")
    age_state = _state(vm.get("observed_at"), age=VM_FRESH_SECONDS)
    if vm.get("state") in ("fresh", "partial", "stale") and age_state != "fresh":
        vm.update(state=age_state)
    vm["axes"] = {
        axis: dict(source, state=age_state if source.get("state") != "unknown" else "unknown")
        for axis, source in axes.items()
    }
    return vm


def collect_vm_inventory(root, *, runner=subprocess.run, timeout_s=30) -> dict:
    """Observe B1's read-only, one-SSH inventory arm. Never infer empty health."""
    _positive(timeout_s, "timeout_s")
    argv = [
        "bash",
        str(Path(root).absolute() / "scripts" / "remote_agent.sh"),
        "inventory",
        "--json",
        "--deadline-seconds",
        "20",
    ]
    try:
        run = _run_vm_command if runner is _NATIVE_VM_RUNNER else runner
        result = run(argv, capture_output=True, text=True, check=False, timeout=timeout_s)
        if result.returncode not in (0, 3):
            return _vm_unknown(f"ssh_rc_{result.returncode}")
        if len(result.stdout) > MAX_RECORD_BYTES:
            return _vm_unknown("bad_json")
        return _vm_document(json.loads(result.stdout), result.returncode)
    except (subprocess.TimeoutExpired, TimeoutError):
        return _vm_unknown("timeout")
    except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
        return _vm_unknown("bad_json")
    except OSError as exc:
        return _vm_unknown(type(exc).__name__)


def _start_vm_observer(root, runner, deadline):
    """At most one VM worker, retained until even a broken injected runner retires."""
    done, result = threading.Event(), {}
    if not _VM_OBSERVER_SLOT.acquire(blocking=False):
        result["vm"] = _vm_unknown("observer_busy")
        done.set()
        return done, result

    def run():
        try:
            remaining = deadline - time.monotonic()
            result["vm"] = (
                collect_vm_inventory(root, runner=runner, timeout_s=remaining)
                if remaining > 0
                else _vm_unknown("timeout")
            )
        except Exception as exc:
            result["vm"] = _vm_unknown(type(exc).__name__)
        finally:
            _VM_OBSERVER_SLOT.release()
            done.set()

    threading.Thread(target=run, name="workbay-fleet-vm-observer", daemon=True).start()
    return done, result


def build_snapshot(root, *, hours=24, census_days=7, vm=True, runner=subprocess.run) -> dict:
    """Join the configured handoff ledger and bounded pass/spool observations.

    Returns a v1 dict even when sources fail. VM inventory is opt-out; no writes.
    """
    _positive(hours, "hours")
    _positive(census_days, "census_days")
    root = Path(root).absolute()
    started = time.monotonic()
    deadline = started + REFRESH_SECONDS
    vm_observer = _start_vm_observer(root, runner, deadline) if vm else None
    seed = dict(
        schema_version=1,
        observed_at=_iso(),
        state="fresh",
        host_only=not vm,
        root=str(root),
        hours=hours,
        census_days=census_days,
        partial=False,
        limits_hit=[],
        lanes=[],
        passes=[],
        passes_quantity="exact",
        admission=dict(_unknown(), decision="unknown", held_slots=None, stock="not_evaluated"),
        spool=dict(_unknown(), families={}, quantity="lower_bound"),
        ledger=_unknown(),
        turn_metrics=[],
    )

    def collect(budget, publish):
        runtime = RuntimeConfig.for_repo(root)
        publish(state_dir=str(runtime.state_dir))
        publish(admission=_admission(root, budget))
        entries = _entries(runtime.state_dir, budget)
        passes, owners = _passes(runtime.state_dir, entries, budget, hours)
        publish(passes=passes)
        lanes, metrics, source = _ledger(runtime, budget, hours)
        by_lane = {}
        for row in passes:
            by_lane.setdefault((row.get("task_ref"), row.get("lane_id")), []).append(row["pass_id"])
        metrics_by_lane = {}
        for metric in metrics:
            metrics_by_lane.setdefault((metric.get("task_ref"), metric.get("lane_id")), []).append(metric)
        for lane in lanes:
            budget.check()
            key = (lane["task_ref"], lane["lane_id"])
            lane.update(
                pass_ids=by_lane.get(key, []), observed_at=lane.get("updated_at"), state=_state(lane.get("updated_at"))
            )
            lane_metrics = metrics_by_lane.get(key, [])
            values = [m.get("total_tokens") for m in lane_metrics]
            known = [v for v in values if isinstance(v, int) and v >= 0]
            lane["tokens"] = dict(
                scope="lane_window",
                total_tokens=sum(known) if known else None,
                state="fresh" if values and len(known) == len(values) else "unknown",
                quantity="exact" if values and len(known) == len(values) else "lower_bound",
                observed_at=source["observed_at"],
            )
        publish(lanes=lanes, ledger=source, turn_metrics=metrics)
        active_lane_keys = {(row["task_ref"], row["lane_id"]) for row in lanes}
        for row in passes:
            key = (row.get("task_ref"), row.get("lane_id"))
            for directory in row["remote_exec_dirs"]:
                if key in active_lane_keys:
                    owners[directory] = "nonterminal"
                elif (
                    row.get("terminality") == "terminal"
                    and source["state"] == "fresh"
                    and "db_rows" not in budget.issues
                ):
                    owners[directory] = "terminal"
        publish(spool=_census(runtime.state_dir, entries, budget, census_days, owners))

    snapshot = _bounded(seed, collect, max(0, deadline - time.monotonic()))
    if vm_observer:
        done, result = vm_observer
        done.wait(max(0, deadline - time.monotonic()))
        snapshot["vm"] = result["vm"] if done.is_set() else _vm_unknown("timeout")
        if snapshot["vm"]["state"] != "fresh" or snapshot["vm"].get("unknowns"):
            snapshot.update(partial=True, state="unknown")
            snapshot["limits_hit"] = sorted(
                set(snapshot["limits_hit"]) | {"vm:" + snapshot["vm"].get("reason", snapshot["vm"]["state"])}
            )
    else:
        snapshot["vm"] = _vm_unknown()
    snapshot["elapsed_s"] = time.monotonic() - started
    return snapshot


def read_snapshot_cache(root):
    """Return None for missing cache; malformed/unreadable caches are explicit unknown."""
    runtime = RuntimeConfig.for_repo(Path(root))
    path = runtime.state_dir / "fleet" / "snapshot.json"
    if not path.exists():
        return None
    budget = _Budget(time.monotonic() + REFRESH_SECONDS)
    payload, source = _read(path, budget)
    if payload is None or payload.get("schema_version") != 1:
        return dict(_unknown(source.get("reason", "unsupported_schema")), partial=True)
    cache_state = _state(payload.get("observed_at"))
    state = cache_state if cache_state != "fresh" else payload.get("state", "unknown")
    if "vm" in payload:
        vm = payload["vm"]
        try:
            rc = 3 if isinstance(vm, dict) and vm.get("state") == "partial" else 0
            payload["vm"] = _vm_document(vm, rc, cached=True)
        except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
            payload["vm"] = _vm_unknown("bad_json")
    else:
        payload["vm"] = _vm_unknown()
    return dict(payload, state=state, cache_state=cache_state, cache_observed_at=_iso())


def write_snapshot_cache(root, snapshot):
    """Atomically replace only fleet/snapshot.json. Source and ledger stay authoritative."""
    runtime = RuntimeConfig.for_repo(Path(root))
    directory = runtime.state_dir / "fleet"
    directory.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".snapshot-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(snapshot, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(name, directory / "snapshot.json")
    finally:
        try:
            os.unlink(name)
        except FileNotFoundError:
            pass


# Offline summary jobs are seeded by the coordinator. This module never calls a model.
MAX_BRIEF_BYTES = 64 * 1024
MAX_SUMMARIES = 50
MAX_SUMMARY_CHARS = 600
SUMMARY_MODEL = "gpt-6.1-terra"  # Declared coordinator route; import is not provider telemetry.


def _open_directory_no_symlinks(path):
    """Open every absolute component with O_NOFOLLOW, including workspace ancestors.

    dir_fd traversal keeps the check and read on the same inode under rename races.
    The caller owns the returned descriptor.
    """
    path = Path(os.path.abspath(path))
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd
    except OSError as exc:
        os.close(fd)
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise PermissionError("symlink component refused") from exc
        raise


def _read_no_symlinks(path, *, limit):
    fd = _open_directory_no_symlinks(Path(path).parent)
    try:
        try:
            file_fd = os.open(Path(path).name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise PermissionError("symlink file refused") from exc
            raise
        with os.fdopen(file_fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise PermissionError("regular files only")
            data = stream.read(limit + 1)
            if len(data) > limit:
                raise ValueError("file too large")
            return data
    finally:
        os.close(fd)


def read_fleet_brief(root, snapshot, pass_id, *, limit=MAX_RECORD_BYTES):
    """Read only a cached pass's linked brief; unknown identity/link raises KeyError."""
    rows = [p for p in snapshot.get("passes", []) if isinstance(p, dict) and p.get("pass_id") == pass_id]
    if len(rows) != 1 or not rows[0].get("remote_exec_dir"):
        raise KeyError("unknown pass or link")
    workspace = Path(os.path.abspath(root))
    state = workspace / ".task-state"
    directory = Path(rows[0]["remote_exec_dir"])
    if not directory.is_absolute():
        directory = workspace / directory
    candidate = directory / "brief.md"
    # Refuse '..' rather than normalize away a symlink component before traversal.
    if ".." in candidate.parts:
        raise PermissionError("brief path escape")
    try:
        candidate.relative_to(state)
    except ValueError as exc:
        raise PermissionError("brief path escape") from exc
    return _read_no_symlinks(candidate, limit=limit)


def _current_briefs(root, snapshot):
    import hashlib

    found = {}
    for row in snapshot.get("passes", [])[:10_000]:
        if not isinstance(row, dict) or not isinstance(row.get("pass_id"), str):
            continue
        try:
            raw = read_fleet_brief(root, snapshot, row["pass_id"], limit=MAX_BRIEF_BYTES)
            brief = raw.decode("utf-8")
        except (OSError, ValueError, KeyError, UnicodeError):
            continue
        sha = hashlib.sha256(raw).hexdigest()
        found[sha] = {"brief_sha256": sha, "brief": brief}
    return found


def read_prompt_summary(root, sha):
    """Bounded cache read; malformed, missing or unsafe caches are no summary."""
    import re

    if not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{64}", sha) is None:
        return None
    path = Path(root) / ".task-state/fleet/summaries" / (sha + ".json")
    try:
        row = json.loads(_read_no_symlinks(path, limit=8192))
        if (
            isinstance(row, dict)
            and row.get("brief_sha256") == sha
            and row.get("source") == "ai"
            and isinstance(row.get("summary"), str)
            and len(row["summary"]) <= MAX_SUMMARY_CHARS
            and isinstance(row.get("model"), str)
            and isinstance(row.get("generated_at"), str)
        ):
            return row
    except (OSError, ValueError, UnicodeError):
        pass
    return None


def prompt_summaries(root, snapshot):
    """Read-only pass-id -> cached AI evidence; raw briefs remain the authority."""
    import hashlib

    summaries = {}
    for row in snapshot.get("passes", [])[:10_000]:
        try:
            pass_id = row["pass_id"]
            raw = read_fleet_brief(root, snapshot, pass_id, limit=MAX_BRIEF_BYTES)
            cached = read_prompt_summary(root, hashlib.sha256(raw).hexdigest())
            if cached is not None:
                summaries[pass_id] = cached
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return summaries


def emit_summary_batch(root, snapshot, output):
    """Emit at most fifty unique missing summaries with exact-byte SHA and inline text."""
    missing = [row for sha, row in _current_briefs(root, snapshot).items() if read_prompt_summary(root, sha) is None]
    selected = missing[:MAX_SUMMARIES]
    with Path(output).open("w", encoding="utf-8", newline="\n") as stream:
        for row in selected:
            stream.write(json.dumps(row, ensure_ascii=True) + "\n")
    return len(selected)


def import_summary_results(root, snapshot, results):
    """Validate the whole bounded result batch before writing its derived cache.

    Input is coordinator-provided output; sha must still identify a CURRENT brief.
    No source path is accepted from the results. Cache writes use no-follow dir fds.
    """
    current = _current_briefs(root, snapshot)
    accepted = {}
    with Path(results).open("rb") as stream:
        for index in range(MAX_SUMMARIES + 1):
            raw = stream.readline(8193)
            if not raw:
                break
            if index == MAX_SUMMARIES or len(raw) > 8192:
                raise ValueError("summary batch exceeds bounds")
            row = json.loads(raw)
            if not isinstance(row, dict):
                raise ValueError("invalid summary row")
            sha, summary = row.get("brief_sha256"), row.get("summary")
            if not isinstance(sha, str) or sha not in current:
                raise ValueError("summary SHA is not in current snapshot")
            if not isinstance(summary, str) or len(summary) > MAX_SUMMARY_CHARS:
                raise ValueError("summary must be at most 600 characters")
            if sha in accepted:
                raise ValueError("duplicate summary SHA")
            accepted[sha] = dict(
                brief_sha256=sha, summary=summary, model=SUMMARY_MODEL, generated_at=_iso(), source="ai"
            )
    parent = Path(root) / ".task-state/fleet"
    fd = _open_directory_no_symlinks(parent)
    try:
        try:
            os.mkdir("summaries", dir_fd=fd)
        except FileExistsError:
            pass
        try:
            cache_fd = os.open("summaries", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        except OSError as exc:
            raise PermissionError("summary cache symlink refused") from exc
        try:
            for sha, row in accepted.items():
                name = "." + sha + "." + os.urandom(8).hex() + ".tmp"
                file_fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=cache_fd)
                try:
                    with os.fdopen(file_fd, "w", encoding="utf-8") as stream:
                        json.dump(row, stream, ensure_ascii=True)
                        stream.write("\n")
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(name, sha + ".json", src_dir_fd=cache_fd, dst_dir_fd=cache_fd)
                    os.fsync(cache_fd)
                finally:
                    try:
                        os.unlink(name, dir_fd=cache_fd)
                    except FileNotFoundError:
                        pass
        finally:
            os.close(cache_fd)
    finally:
        os.close(fd)
    return len(accepted)
