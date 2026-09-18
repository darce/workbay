"""``plan-accept-backfill`` subcommand (internal).

One-shot acceptance walk over every live ``handoff_state`` row whose
plan baseline is missing from ``main``. For each row, the handler
applies the same gate as :mod:`plan_accept` -- latest planning verdict
exactly ``pass``, zero open planning findings -- and emits a per-task
docs-only commit recipe when the gate clears. Rows that already have
their plan on ``main`` are reported as ``already_accepted`` so re-runs
are no-ops.

Per-task ``action`` vocabulary (three values):

- ``accept`` — gate cleared; recipe emitted (or applied under ``--local``).
- ``skip`` — not acceptance-ready (baseline gate) or a local apply failed
  after restore left a clean tree.
- ``blocked`` — ``--local`` pre-flight refused the whole batch (wrong
  root / not on main / dirty tree) before any per-task mutation.

``skipped_count`` counts only ``action == "skip"`` rows;
``blocked_count`` counts only ``action == "blocked"`` rows.
The three counters partition the batch:
``accepted_count + skipped_count + blocked_count == len(entries)``.

The handler is intentionally additive in receipt-only mode (default):
it prints what *would* be accepted but never touches the index.
``--local`` performs the docs-only checkout+commit cycle inline once
per ready task; pre-flight enforces canonical-root, ``main`` checkout,
and clean tree just like the single-task handler.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

import resolver

from . import _common
from .plan_baseline import evaluate_plan_baseline


_LIVE_STATUSES = ("in_progress", "review", "blocked")


def _query_handoff_rows(repo: Path) -> list[dict[str, Any]]:
    """Return live handoff rows from the MCP CLI.

    Returns an empty list on any failure so the backfill degrades to a
    no-op receipt rather than crashing.
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(workspace),
        "handoff-rows",
        "--status", *_LIVE_STATUSES,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return []
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _plan_exists_on_branch(repo: Path, branch: str, plan_path: str) -> bool:
    """Return True when ``<branch>:<plan_path>`` resolves in the local repo.

    Uses ``git cat-file -e <branch>:<path>`` so a missing branch, missing
    blob, or unreadable repo all collapse to False. For target branches,
    that means the row is not safe to accept yet because ``plan-show``
    would not be able to read the registered plan.
    """
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "cat-file", "-e", f"{branch}:{plan_path}"],
    )
    return proc.returncode == 0


def _plan_exists_on_main(repo: Path, plan_path: str) -> bool:
    return _plan_exists_on_branch(repo, "main", plan_path)


def _build_accept_command(*, task_ref: str, branch: str, plan_path: str) -> str:
    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    return (
        f"git switch main && "
        f"git checkout {shlex.quote(branch)} -- {shlex.quote(plan_path)} && "
        f"git add {shlex.quote(plan_path)} && "
        f"git commit -m {shlex.quote(msg)}"
    )


def _is_worktree_clean(repo: Path) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain"],
    )
    return proc.returncode == 0 and not proc.stdout.strip()


def _current_branch(repo: Path) -> str:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _is_preexisting_untracked_draft(repo: Path, plan_path: str) -> bool:
    """True when ``plan_path`` is on disk, untracked, and absent from HEAD.

    Captured *before* any mutating step so restore can preserve operator
    drafts that this call did not create.
    """
    if _plan_exists_on_branch(repo, "HEAD", plan_path):
        return False
    try:
        target = repo / plan_path
        if not (target.exists() or target.is_symlink()):
            return False
    except OSError:
        return False
    idx = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", f":0:{plan_path}"],
    )
    return idx.returncode != 0


def _path_scoped_porcelain(repo: Path, plan_path: str) -> str:
    """Path-scoped porcelain used to detect residual mutation vs pre-call state."""
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain", "-uall", "--", plan_path],
    )
    return proc.stdout if proc.returncode == 0 else (proc.stdout or "")


def _restore_path_from_head(
    repo: Path, plan_path: str, *, preserve_untracked: bool = False
) -> None:
    # Pre-existing untracked draft: only unstage. Never restore worktree or
    # unlink — those destroy operator bytes this call did not put there.
    if preserve_untracked:
        _common.run_subprocess(["git", "-C", str(repo), "reset", "--", plan_path])
        return
    restore = _common.run_subprocess(
        ["git", "-C", str(repo), "restore", "--source=HEAD", "--staged", "--worktree", "--", plan_path],
    )
    if restore.returncode == 0:
        return
    _common.run_subprocess(["git", "-C", str(repo), "reset", "--", plan_path])
    if _plan_exists_on_branch(repo, "HEAD", plan_path):
        _common.run_subprocess(["git", "-C", str(repo), "checkout", "--", plan_path])
        return
    try:
        (repo / plan_path).unlink()
    except OSError:
        pass


def _has_residual_plan_mutation(repo: Path, plan_path: str) -> bool:
    """True when ``plan_path`` still shows mutation after a restore attempt.

    Residual is True when path-scoped porcelain is non-empty, or when the
    plan file still exists on disk but HEAD does not carry it. Callers use
    this together with :func:`_is_post_checkout_apply_error` — the error
    token alone is not sufficient because restore may have erased the
    mutation.
    """
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain", "--", plan_path],
    )
    if proc.returncode == 0 and proc.stdout.strip():
        return True
    try:
        on_disk = (repo / plan_path).exists() or (repo / plan_path).is_symlink()
    except OSError:
        return True
    if on_disk and not _plan_exists_on_branch(repo, "HEAD", plan_path):
        return True
    return False


def _is_post_checkout_apply_error(err: str | None) -> bool:
    """Token half of recipe retention: True for post-checkout failure tokens.

    Pre-mutation failures (``checkout_failed:``) must not re-offer the
    destructive accept recipe. Post-checkout tokens (``add_failed:``,
    ``commit_failed:``) are necessary but **not sufficient** to keep the
    recipe: :func:`_apply_local_accept` restores on every failure path, so
    callers must also require residual mutation
    (:func:`_has_residual_plan_mutation`). Token alone may describe a tree
    that restore already returned to pre-mutation equivalence.
    Mirrors the taxonomy in :mod:`plan_accept`.
    """
    if not err:
        return False
    return err.startswith("commit_failed:") or err.startswith("add_failed:")


_SAFE_REASON_TOKENS = frozenset(
    {
        "checkout_failed",
        "add_failed",
        "commit_failed",
        "local_accept_failed",
    }
)


def _safe_reason_token(reason: str | None) -> str:
    """Extract a stable allowlisted token from an apply-failure reason.

    Never resurrects raw git stderr (paths, shell metacharacters) into
    receipt fields. Unrecognised, empty, or missing reasons degrade to
    ``local_accept_failed``.
    """
    if not reason:
        return "local_accept_failed"
    token = str(reason).split(":", 1)[0]
    if token in _SAFE_REASON_TOKENS:
        return token
    return "local_accept_failed"


class _LocalApplyOutcome(tuple):
    """``(ok, err)`` plus ``residual`` mutation flag after restore.

    Mirrors :class:`plan_accept._LocalApplyOutcome`. Unpacks as the legacy
    2-tuple ``(ok, err)``; callers that need residual read ``.residual``
    (or use :meth:`as_triple`). Iteration also yields the residual flag so
    legacy three-value unpacks remain valid during convergence.
    """

    residual: bool

    def __new__(
        cls,
        ok: bool,
        err: str | None,
        residual: bool = False,
    ) -> "_LocalApplyOutcome":
        inst = super().__new__(cls, (bool(ok), err))
        inst.residual = bool(residual)
        return inst

    def __iter__(self):
        yield bool(self[0])
        yield self[1]
        yield self.residual

    @property
    def ok(self) -> bool:
        return bool(self[0])

    @property
    def err(self) -> str | None:
        return self[1]  # type: ignore[return-value]

    def as_triple(self) -> tuple[bool, str | None, bool]:
        return self.ok, self.err, self.residual


def _apply_local_accept(
    repo: Path, *, task_ref: str, branch: str, plan_path: str
) -> _LocalApplyOutcome:
    """Run docs-only checkout+commit. Returns :class:`_LocalApplyOutcome`.

    On every failure path, restore is attempted first, then residual
    mutation is probed. ``residual`` is False on success.
    """
    # Capture before any mutating step: restore must not destroy a draft
    # that was already on disk (untracked, absent from HEAD).
    preserve_untracked = _is_preexisting_untracked_draft(repo, plan_path)
    pre_porcelain = (
        _path_scoped_porcelain(repo, plan_path) if preserve_untracked else ""
    )

    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    steps: list[list[str]] = [
        ["git", "-C", str(repo), "checkout", branch, "--", plan_path],
        ["git", "-C", str(repo), "add", plan_path],
        ["git", "-C", str(repo), "commit", "-m", msg],
    ]
    for argv in steps:
        proc = _common.run_subprocess(argv)
        if proc.returncode != 0:
            _restore_path_from_head(
                repo, plan_path, preserve_untracked=preserve_untracked
            )
            err = f"{argv[3]}_failed: {proc.stderr.strip() or proc.stdout.strip()}"
            if preserve_untracked:
                residual = (
                    _path_scoped_porcelain(repo, plan_path) != pre_porcelain
                )
            else:
                residual = _has_residual_plan_mutation(repo, plan_path)
            return _LocalApplyOutcome(False, err, residual)
    return _LocalApplyOutcome(True, None, False)


def _evaluate_row(
    repo: Path,
    row: dict[str, Any],
) -> dict[str, Any]:
    """Return a per-task receipt entry for one handoff row."""
    task_ref = row.get("task_ref") or ""
    plan_path = row.get("task_plan_path")
    target_branch = row.get("target_branch")

    entry: dict[str, Any] = {
        "task_ref": task_ref,
        "target_branch": target_branch,
        "task_plan_path": plan_path,
    }
    baseline = evaluate_plan_baseline(
        repo,
        task_ref=str(task_ref),
        task_plan_path=str(plan_path) if isinstance(plan_path, str) else None,
        target_branch=str(target_branch) if isinstance(target_branch, str) else None,
    )
    entry.update(baseline.to_dict())

    if not baseline.acceptance_ready:
        entry["action"] = "skip"
        entry["reason"] = baseline.reason
        return entry

    entry["action"] = "accept"
    entry["next_command"] = _build_accept_command(
        task_ref=task_ref, branch=target_branch, plan_path=plan_path,
    )
    return entry


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle plan-accept-backfill", add_help=True
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument("--task", dest="task_ref", default="")
    parser.add_argument(
        "--local",
        dest="local",
        action="store_true",
        default=False,
        help=(
            "Apply each ready task's docs-only checkout+commit inline "
            "(requires canonical-root checkout on main with a clean tree)."
        ),
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    canonical = resolver.canonical_workspace_root(repo) or repo

    task_ref_filter = args.task_ref.strip().upper()
    rows = _query_handoff_rows(repo)
    if task_ref_filter:
        rows = [row for row in rows if str(row.get("task_ref") or "").upper() == task_ref_filter]
    entries = [_evaluate_row(repo, row) for row in rows]

    accepted_count = sum(1 for e in entries if e.get("action") == "accept")
    skipped_count = sum(1 for e in entries if e.get("action") == "skip")

    local_errors: list[str] = []
    if args.local and accepted_count > 0:
        if repo.resolve() != canonical.resolve():
            local_errors.append("local_requires_canonical_root")
        elif _current_branch(repo) != "main":
            local_errors.append("local_requires_main_checkout")
        elif not _is_worktree_clean(repo):
            local_errors.append("local_requires_clean_tree")
        else:
            for entry in entries:
                if entry.get("action") != "accept":
                    continue
                apply_out = _apply_local_accept(
                    repo,
                    task_ref=entry["task_ref"],
                    branch=entry["target_branch"],
                    plan_path=entry["task_plan_path"],
                )
                ok, err = apply_out[0], apply_out[1]
                residual = apply_out.residual
                if ok:
                    entry["accepted"] = True
                else:
                    local_errors.append(f"{entry['task_ref']}:{err or 'local_accept_failed'}")
                    entry["action"] = "skip"
                    entry["reason"] = err or "local_accept_failed"
                    accepted_count -= 1
                    skipped_count += 1
                    # R13-BF-02: every --local apply failure clears readiness
                    # and must not leave a plan-accept re-attempt command.
                    entry["acceptance_ready"] = False
                    reason_token = _safe_reason_token(entry["reason"])
                    # R13-BF-01: retain the destructive recipe only when the
                    # error token is post-checkout AND residual mutation
                    # survived restore. Token alone is not enough.
                    if not (
                        _is_post_checkout_apply_error(err) and residual
                    ):
                        root_q = shlex.quote(str(canonical))
                        hint = f"git -C {root_q} status --porcelain"
                        entry["next_command"] = hint
                        # R13-BF-03: re-stamp baseline envelope so readiness
                        # fields cannot be composed back into "accept warranted".
                        entry["baseline_status"] = "blocked_local"
                        entry["detail_reason"] = entry["reason"]
                        # R13-BF-05: stable token in safe_next, raw stays on reason.
                        entry["safe_next_commands"] = [
                            {"command": hint, "reason": reason_token},
                        ]
                    else:
                        # Residual finish-the-job path: keep next_command recipe
                        # but never advertise plan-accept or readiness.
                        # R14B-02: re-stamp envelope so baseline fields cannot
                        # compose with next_command into "accept warranted".
                        safe = [
                            e
                            for e in (entry.get("safe_next_commands") or [])
                            if "plan-accept" not in (e.get("command") or "")
                        ]
                        entry["safe_next_commands"] = safe
                        entry["baseline_status"] = "local_residual"
                        entry["detail_reason"] = entry["reason"]

        # R11-internal: a pre-mutation --local refusal must not leave rows carrying
        # the destructive accept recipe (or action=accept). Apply failures use
        # "TASK:err" codes and never match these three rungs.
        _preflight = (
            "local_requires_canonical_root",
            "local_requires_main_checkout",
            "local_requires_clean_tree",
        )
        if local_errors and local_errors[0] in _preflight:
            refusal = local_errors[0]
            root_q = shlex.quote(str(canonical))
            if refusal == "local_requires_canonical_root":
                hint = "git rev-parse --git-common-dir"
            elif refusal == "local_requires_main_checkout":
                hint = f"git -C {root_q} switch main"
            else:
                hint = f"git -C {root_q} status --porcelain"
            for entry in entries:
                if entry.get("action") == "accept":
                    entry["action"] = "blocked"
                    entry["next_command"] = hint
                    entry["reason"] = refusal
                    # R12-BF-03: scrub must not leave split-brain readiness.
                    entry["acceptance_ready"] = False
                    # R13-BF-03: re-stamp baseline envelope on preflight refusal.
                    entry["baseline_status"] = "blocked_local"
                    entry["detail_reason"] = entry["reason"]
                    entry["safe_next_commands"] = [
                        {"command": hint, "reason": refusal},
                    ]
            accepted_count = 0

    # R12-BF-02 / R14B-03: after --local may relabel accept→blocked/skip,
    # re-derive a partition: accepted + skipped + blocked == len(entries).
    accepted_count = sum(1 for e in entries if e.get("action") == "accept")
    skipped_count = sum(1 for e in entries if e.get("action") == "skip")
    blocked_count = sum(1 for e in entries if e.get("action") == "blocked")

    receipt: dict[str, Any] = {
        "ok": not local_errors,
        "command": "plan-accept-backfill",
        "worktree_path": str(repo),
        "accepted_count": accepted_count,
        "skipped_count": skipped_count,
        "blocked_count": blocked_count,
        "tasks": entries,
        "events": ["plan_accept_backfill_evaluated"],
    }
    if local_errors:
        receipt["local_errors"] = local_errors

    if not args.emit_json:
        sys.stderr.write(
            f"plan-accept-backfill: accepted={accepted_count} skipped={skipped_count}\n"
        )
        for entry in entries:
            action = entry.get("action")
            if action == "accept":
                sys.stderr.write(
                    f"  accept {entry['task_ref']}: {entry.get('next_command', '')}\n"
                )
            elif action == "blocked":
                sys.stderr.write(
                    f"  blocked {entry['task_ref']}: {entry.get('reason', '')}\n"
                )
            else:
                sys.stderr.write(
                    f"  skip   {entry['task_ref']}: {entry.get('reason', '')}\n"
                )

    _common.emit(receipt)
    return 0 if not local_errors else 2
