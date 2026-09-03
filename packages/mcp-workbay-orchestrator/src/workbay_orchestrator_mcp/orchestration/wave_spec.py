"""Deterministic dispatch_wave lane_spec builder.

Every wave-member spec is derived from the system of record (lane manifest
plus worktree_lanes rows). Callers cannot hand-type a spec: ``build_wave_specs``
has no parameter that accepts one. Failures are returned as typed per-lane
refusals, never raised and never swallowed into an empty frontier.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Margin subtracted from a backend's declared wall-clock cap so the join still
# has slack after the worker's own deadline. 15s matches the wave-join slack
# used elsewhere; the constant is the only place the margin is spelled.
TIMEOUT_MARGIN_SECONDS = 15
DEFAULT_TIMEOUT_SECONDS = 1785
DEFAULT_TOKEN_BUDGET = 120_000
TOKEN_BUDGET_ENV = "WORKBAY_WAVE_TOKEN_BUDGET"
TIMEOUT_DEFAULT_ENV = "WORKBAY_WAVE_TIMEOUT_DEFAULT"

# Terminal worktree_lanes statuses that must never be re-dispatched.
# Matches wave_dispatch._COMPLETED_LANE_STATUSES / LANE_STATUSES: closed_stale
# is the reaper terminal production writes. superseded is a worker_reports
# token (REPORT_STATUSES), not a lane status.
_TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})
_FULL_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_GIT_TIMEOUT_SECONDS = 30
# list_lane_briefs default page. A single unpaged call is a false absence.
_BRIEF_PAGE_SIZE = 20

LoadManifest = Callable[..., Mapping[str, Any]]
ListRows = Callable[..., object]
ListBriefs = Callable[..., object]
RunGit = Callable[..., object]


@dataclass(frozen=True, slots=True)
class WaveLaneSpec:
    """One derived wave member. Fields are filled from row + manifest + env."""

    lane_id: str
    backend: str
    model: str
    effort: str | None
    token_budget: int
    timeout_seconds: int
    brief_id: int | None


@dataclass(frozen=True, slots=True)
class WaveSpecRefusal:
    """Typed per-lane refusal; ``kind`` is the stable machine token."""

    lane_id: str
    kind: str
    detail: str


@dataclass(frozen=True, slots=True)
class WaveSpecResult:
    """Specs that passed plus refusals that did not; never a mixed silent drop."""

    specs: list[WaveLaneSpec]
    refusals: list[WaveSpecRefusal]
    task_ref: str
    manifest_path: str

    def to_lane_specs(self) -> list[dict[str, Any]]:
        """Exactly the item shape ``dispatch_wave`` / ``coordinate_wave`` accepts."""
        return [
            {
                "lane_id": spec.lane_id,
                "backend": spec.backend,
                "token_budget": spec.token_budget,
                "timeout_seconds": spec.timeout_seconds,
                "model": spec.model,
                "effort": spec.effort,
            }
            for spec in self.specs
        ]


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _manifest_path_for(root: Path, task_ref: str) -> str:
    return str((Path(root) / "config" / "lane-orchestration" / f"{task_ref}.json"))


def _coerce_dict_list(payload: object, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, Mapping):
        return []
    data = payload.get("data") if isinstance(payload.get("data"), Mapping) else payload
    if not isinstance(data, Mapping):
        return []
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _invoke_load_manifest(load_fn: LoadManifest, task_ref: str, root: Path) -> Mapping[str, Any]:
    try:
        loaded = load_fn(task_ref, orchestrator_root=str(root))
    except TypeError:
        loaded = load_fn(task_ref)
    if not isinstance(loaded, Mapping):
        raise RuntimeError(f"lane manifest must be a JSON object, got {type(loaded).__name__}")
    return loaded


def _invoke_list(fn: Callable[..., object], task_ref: str) -> object:
    try:
        return fn(task_ref)
    except TypeError:
        return fn()


def _git_returncode(result: object) -> int:
    if isinstance(result, bool) or not isinstance(result, int):
        rc = getattr(result, "returncode", None)
        if isinstance(rc, bool) or not isinstance(rc, int):
            return 1
        return rc
    return result


def _default_run_git(args: Sequence[str], *, cwd: Path) -> int:
    try:
        completed = subprocess.run(
            ["git", "-C", str(cwd), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 1
    return int(completed.returncode)


def _default_list_rows(task_ref: str) -> object:
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    return manage_worktree_lane(operation="list", task_ref=task_ref, status="all", limit=10_000)


def _payload_mapping(payload: object) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    nested = payload.get("data")
    if isinstance(nested, Mapping):
        return nested
    return payload


def _page_has_more(payload: object, *, page_len: int, page_size: int) -> bool:
    flag = _payload_mapping(payload).get("has_more")
    if flag is True:
        return True
    if flag is False:
        return False
    # Missing or non-bool: a full page may still have more. Absence of the
    # flag is not exhaustion — that is the capped-lookup false-absence trap.
    return page_len >= page_size


def _call_brief_page(
    fetch_page: Callable[..., object],
    task_ref: str,
    *,
    limit: int,
    offset: int,
) -> object:
    try:
        return fetch_page(task_ref=task_ref, status="open", limit=limit, offset=offset)
    except TypeError:
        try:
            return fetch_page(task_ref, status="open", limit=limit, offset=offset)
        except TypeError:
            try:
                return fetch_page(task_ref)
            except TypeError:
                return fetch_page()


def _collect_open_briefs(
    fetch_page: Callable[..., object],
    task_ref: str,
    *,
    page_size: int = _BRIEF_PAGE_SIZE,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    offset = 0
    while True:
        payload = _call_brief_page(fetch_page, task_ref, limit=page_size, offset=offset)
        page = _coerce_dict_list(payload, "briefs")
        collected.extend(page)
        if not _page_has_more(payload, page_len=len(page), page_size=page_size):
            break
        if not page:
            break
        offset += len(page)
    return collected


def _default_list_briefs(
    task_ref: str,
    *,
    fetch_page: Callable[..., object] | None = None,
) -> object:
    fetch = fetch_page
    if fetch is None:
        from workbay_orchestrator_mcp.lanes import list_lane_briefs  # noqa: PLC0415

        fetch = list_lane_briefs
    return _collect_open_briefs(fetch, task_ref)


def _rows_by_lane(payload: object) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _coerce_dict_list(payload, "lanes"):
        lane_id = _text(row.get("lane_id"))
        if lane_id and lane_id not in out:
            out[lane_id] = row
    return out


def _newest_open_brief_id(payload: object, lane_id: str) -> int | None:
    best: int | None = None
    for brief in _coerce_dict_list(payload, "briefs"):
        if _text(brief.get("lane_id")) != lane_id:
            continue
        status = _text(brief.get("status")).lower() or "open"
        if status != "open":
            continue
        raw_id = brief.get("id")
        if isinstance(raw_id, bool) or not isinstance(raw_id, int):
            if isinstance(raw_id, str) and raw_id.strip().isdigit():
                brief_id = int(raw_id.strip())
            else:
                continue
        else:
            brief_id = raw_id
        if best is None or brief_id > best:
            best = brief_id
    return best


def _parse_positive_int(env: Mapping[str, str], key: str, default: int) -> tuple[int | None, str | None]:
    raw = env.get(key) if key in env else None
    if raw is None or _text(raw) == "":
        return default, None
    try:
        if isinstance(raw, bool) or isinstance(raw, float):
            raise ValueError
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None, f"{key} must be a positive int, got {raw!r}"
    if value <= 0:
        return None, f"{key} must be a positive int, got {value}"
    return value, None


def _derive_backend(row: Mapping[str, Any], entry: Mapping[str, Any]) -> tuple[str, str | None]:
    row_backend = _text(row.get("backend"))
    manifest_backend = _text(entry.get("preferred_backend"))
    if row_backend and manifest_backend and row_backend != manifest_backend:
        return (
            "",
            f"row backend {row_backend!r} disagrees with manifest preferred_backend {manifest_backend!r}",
        )
    return row_backend or manifest_backend, None


def _derive_model(row: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    return _text(row.get("model")) or _text(entry.get("preferred_model"))


def _derive_effort(row: Mapping[str, Any], entry: Mapping[str, Any], backend: str, model: str) -> str | None:
    effort = _text(row.get("reasoning_effort")) or _text(entry.get("preferred_reasoning_effort"))
    if effort:
        return effort
    from workbay_orchestrator_mcp.orchestration.backend_spec import default_effort_for_model  # noqa: PLC0415

    return default_effort_for_model(backend, model)


def _derive_timeout(backend: str, env: Mapping[str, str]) -> tuple[int | None, WaveSpecRefusal | None]:
    from workbay_orchestrator_mcp.orchestration.wave_dispatch import (  # noqa: PLC0415
        describe_lane_spec_error,
        lane_spec_refusal_clauses,
        lane_spec_timeout_cap,
    )

    cap = lane_spec_timeout_cap(backend)
    env_timeout, env_error = _parse_positive_int(env, TIMEOUT_DEFAULT_ENV, DEFAULT_TIMEOUT_SECONDS)
    if cap is None:
        if env_error is not None:
            return None, WaveSpecRefusal(lane_id="", kind="bad_env", detail=env_error)
        timeout = env_timeout if env_timeout is not None else DEFAULT_TIMEOUT_SECONDS
    else:
        timeout = int(cap) - int(TIMEOUT_MARGIN_SECONDS)
    if cap is not None and (timeout <= 0 or timeout > cap):
        return None, WaveSpecRefusal(
            lane_id="",
            kind="timeout_over_cap",
            detail=f"derived timeout {timeout} exceeds cap {cap} for {backend}",
        )
    probe: dict[str, object] = {
        "lane_id": "_",
        "backend": backend,
        "token_budget": DEFAULT_TOKEN_BUDGET,
        "timeout_seconds": timeout,
    }
    clauses = lane_spec_refusal_clauses(probe, enforce_cap=True)
    if any("exceeds cap" in clause for clause in clauses):
        return None, WaveSpecRefusal(
            lane_id="",
            kind="timeout_over_cap",
            detail=describe_lane_spec_error(probe, enforce_cap=True),
        )
    return timeout, None


def _run_git_check(run_git: RunGit, args: Sequence[str], root: Path) -> int:
    try:
        result = run_git(args, cwd=root)
    except TypeError:
        try:
            result = run_git(args)
        except TypeError:
            result = run_git(*args)
    return _git_returncode(result)


def _spec_for_lane(
    *,
    lane_id: str,
    entry: Mapping[str, Any] | None,
    row: Mapping[str, Any] | None,
    briefs: object,
    run_git: RunGit,
    env: Mapping[str, str],
    root: Path,
    token_budget: int | None,
    budget_error: str | None,
) -> WaveLaneSpec | WaveSpecRefusal:
    if entry is None:
        return WaveSpecRefusal(lane_id, "no_manifest_entry", f"lane {lane_id!r} is not in the manifest")
    if row is None:
        return WaveSpecRefusal(lane_id, "no_row", f"lane {lane_id!r} has no worktree_lanes row")

    status = _text(row.get("status")).lower()
    if status in _TERMINAL_STATUSES:
        return WaveSpecRefusal(
            lane_id,
            "status_terminal",
            f"lane {lane_id!r} status {status!r} is terminal",
        )

    backend, mismatch = _derive_backend(row, entry)
    if mismatch is not None:
        return WaveSpecRefusal(lane_id, "backend_mismatch", mismatch)
    if not backend:
        return WaveSpecRefusal(
            lane_id,
            "backend_mismatch",
            "backend missing from worktree_lanes row and manifest preferred_backend",
        )

    base_sha = _text(entry.get("base_sha"))
    if not _FULL_COMMIT_SHA_RE.fullmatch(base_sha):
        return WaveSpecRefusal(
            lane_id,
            "short_base_sha",
            f"base_sha must be a 40-char lowercase hex commit, got {base_sha!r}",
        )

    branch = _text(row.get("branch")) or _text(entry.get("branch"))
    if not branch:
        return WaveSpecRefusal(lane_id, "branch_missing", f"lane {lane_id!r} has no branch")
    show_rc = _run_git_check(run_git, ["show-ref", "--verify", f"refs/heads/{branch}"], root)
    if show_rc != 0:
        return WaveSpecRefusal(
            lane_id,
            "branch_missing",
            f"git show-ref --verify refs/heads/{branch} failed (rc={show_rc})",
        )
    ancestor_rc = _run_git_check(run_git, ["merge-base", "--is-ancestor", base_sha, branch], root)
    if ancestor_rc != 0:
        return WaveSpecRefusal(
            lane_id,
            "base_not_ancestor",
            f"git merge-base --is-ancestor {base_sha} {branch} failed (rc={ancestor_rc})",
        )

    brief_id = _newest_open_brief_id(briefs, lane_id)
    if brief_id is None:
        return WaveSpecRefusal(lane_id, "no_brief", f"lane {lane_id!r} has no open brief")

    if budget_error is not None or token_budget is None:
        return WaveSpecRefusal(lane_id, "bad_env", budget_error or f"{TOKEN_BUDGET_ENV} must be a positive int")

    timeout, timeout_refusal = _derive_timeout(backend, env)
    if timeout_refusal is not None:
        return WaveSpecRefusal(lane_id, timeout_refusal.kind, timeout_refusal.detail)
    assert timeout is not None

    model = _derive_model(row, entry)
    effort = _derive_effort(row, entry, backend, model)
    return WaveLaneSpec(
        lane_id=lane_id,
        backend=backend,
        model=model,
        effort=effort,
        token_budget=token_budget,
        timeout_seconds=timeout,
        brief_id=brief_id,
    )


def build_wave_specs(
    task_ref: str,
    lane_ids: Sequence[str],
    *,
    root: Path | str,
    load_manifest: LoadManifest | None = None,
    list_rows: ListRows | None = None,
    list_briefs: ListBriefs | None = None,
    run_git: RunGit | None = None,
    env: Mapping[str, str] | None = None,
) -> WaveSpecResult:
    """Derive every ``dispatch_wave`` lane_spec from manifest + worktree_lanes.

    Collaborators default to the real loaders, resolved lazily so importing this
    module has no I/O. Inject fakes in tests. There is no parameter through
    which a caller can supply a spec.
    """
    root_path = Path(root)
    ref = _text(task_ref)
    manifest_path = _manifest_path_for(root_path, ref)
    requested = [lid for lid in (_text(item) for item in lane_ids) if lid]
    # Preserve caller order, drop duplicates.
    seen: set[str] = set()
    ordered: list[str] = []
    for lane_id in requested:
        if lane_id not in seen:
            seen.add(lane_id)
            ordered.append(lane_id)

    load_fn = load_manifest
    if load_fn is None:
        from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
            load_manifest as load_fn,
        )
    rows_fn = list_rows if list_rows is not None else _default_list_rows
    briefs_fn = list_briefs if list_briefs is not None else _default_list_briefs
    git_fn = run_git if run_git is not None else _default_run_git
    environ: Mapping[str, str] = env if env is not None else os.environ

    try:
        manifest = _invoke_load_manifest(load_fn, ref, root_path)
    except (FileNotFoundError, OSError, RuntimeError, ValueError, TypeError) as exc:
        detail = str(exc)
        return WaveSpecResult(
            specs=[],
            refusals=[WaveSpecRefusal(lane_id, "manifest_invalid", detail) for lane_id in ordered],
            task_ref=ref,
            manifest_path=manifest_path,
        )

    token_budget, budget_error = _parse_positive_int(environ, TOKEN_BUDGET_ENV, DEFAULT_TOKEN_BUDGET)

    lanes_raw = manifest.get("lanes") if isinstance(manifest, Mapping) else None
    entries = lanes_raw if isinstance(lanes_raw, Mapping) else {}
    rows = _rows_by_lane(_invoke_list(rows_fn, ref))
    briefs = _invoke_list(briefs_fn, ref)

    specs: list[WaveLaneSpec] = []
    refusals: list[WaveSpecRefusal] = []
    for lane_id in ordered:
        entry = entries.get(lane_id)
        entry_map = entry if isinstance(entry, Mapping) else None
        outcome = _spec_for_lane(
            lane_id=lane_id,
            entry=entry_map,
            row=rows.get(lane_id),
            briefs=briefs,
            run_git=git_fn,
            env=environ,
            root=root_path,
            token_budget=token_budget,
            budget_error=budget_error,
        )
        if isinstance(outcome, WaveSpecRefusal):
            refusals.append(outcome)
        else:
            specs.append(outcome)
    return WaveSpecResult(specs=specs, refusals=refusals, task_ref=ref, manifest_path=manifest_path)
