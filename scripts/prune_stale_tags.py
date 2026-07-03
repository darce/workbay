#!/usr/bin/env python3
"""Operator-gated prune of pre-rebrand stale-higher tags (dry-run by default).

implementation note, implementation note. The public mirror (``darce/workbay``) and the local repo
carry pre-rebrand ``<pkg>-vX.Y.Z`` tags whose versions sit ABOVE the current
line, poisoning ``git tag -l '<pkg>-v*' | sort -V | tail -1`` resolution. This
tool enumerates those offenders via the SAME detector as
``release_public.py audit-tags`` (``release_public.audit_stale_tags``) and emits
the exact deletion commands for the mirror and the local repo.

Deleting tags on the public mirror is destructive AND public, so — mirroring
``release_reconcile_tag.py``'s ``--execute`` gating (implementation note-S2 precedent) —
this defaults to a dry-run that only prints the commands. ``--execute`` runs
them. It is intentionally NOT wired to any auto-running make target.

Usage:
    python scripts/prune_stale_tags.py                 # dry-run (print commands)
    python scripts/prune_stale_tags.py --execute       # operator-run deletion
    python scripts/prune_stale_tags.py --execute --assume-yes  # skip the prompt
    python scripts/prune_stale_tags.py --remote <url>  # override the mirror

Enumeration is LOCAL (``git tag -l`` via ``audit_stale_tags``): a tag present on
the mirror but absent locally is not pruned. Ensure the local clone's tags are a
superset of the mirror's before pruning (they are, right after a release).

Consumer note: a consumer that hand-runs ``git fetch`` must add ``--prune`` to
drop the deleted refs (overlay installs already fetch ``--tags --prune --force``,
so they self-heal on the next run).
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

# Both scripts live in scripts/; reuse the audit detector rather than
# re-implementing enumeration. Ensure scripts/ is importable whether this runs
# as a script (scripts/ already on sys.path[0]) or is loaded via importlib.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from release_public import (  # noqa: E402
    PUBLIC_GIT_REMOTE,
    REPO_ROOT,
    _GIT_TIMEOUT_S,
    _audit_declared,
    _local_tags,
    _ls_remote_tags,
    _memoizing_commit_date,
    _stale_higher_tags,
    audit_stale_tags,
)


def _deletion_commands(remote: str, tags: list[str]) -> list[list[str]]:
    """Dry-run commands: per-tag mirror deletes then one local batch."""
    mirror_cmds = [
        ["git", "-C", str(REPO_ROOT), "push", remote, "--delete", tag]
        for tag in tags
    ]
    local_cmd = ["git", "-C", str(REPO_ROOT), "tag", "-d", *tags]
    return [*mirror_cmds, local_cmd]


def _confirm(remote: str) -> bool:
    """Prompt before deleting public tags."""
    sys.stderr.write(
        f"About to DELETE the stale tags above from the PUBLIC mirror ({remote}) "
        "and the local repo. This is destructive and irreversible. Type 'prune' "
        "to proceed: "
    )
    sys.stderr.flush()
    try:
        answer = input()
    except EOFError:
        return False
    return answer.strip() == "prune"


def _mirror_only_offenders(
    remote: str, commit_date, baseline, local_offender_tags: set[str]
) -> list[str]:
    """Mirror tags that poison semver-latest but are absent from local enumeration."""
    try:
        mirror_tags = _ls_remote_tags(remote)
    except RuntimeError as exc:
        sys.stderr.write(
            f"[prune-stale-tags] NOTE: could not read mirror tags ({exc}); "
            "mirror-only offenders not checked.\n"
        )
        return []
    mirror_only = [tag for tag in mirror_tags if tag not in set(_local_tags())]
    if not mirror_only:
        return []
    declared = _audit_declared(_local_tags(), commit_date, baseline)
    offenders = _stale_higher_tags(declared, mirror_only)
    return [
        offender.tag
        for offender in offenders
        if offender.tag not in local_offender_tags
    ]


def _print_consumer_note() -> None:
    print(
        "[prune-stale-tags] NOTE: consumers that hand-run `git fetch` must add "
        "`--prune` to drop the deleted refs (overlay installs already do)."
    )


def _resolve_remote(args: argparse.Namespace) -> str:
    return args.remote or os.environ.get("RELEASE_PUBLIC_REMOTE", PUBLIC_GIT_REMOTE)


def _validate_remote(remote: str, *, force_remote: bool, execute: bool) -> int | None:
    """Refuse destructive runs against a non-canonical remote unless overridden."""
    if remote == PUBLIC_GIT_REMOTE or force_remote:
        return None
    sys.stderr.write(
        f"[prune-stale-tags] remote {remote!r} differs from canonical "
        f"{PUBLIC_GIT_REMOTE!r}.\n"
    )
    if execute:
        sys.stderr.write(
            "[prune-stale-tags] pass --force-remote to delete against this remote.\n"
        )
        return 1
    sys.stderr.write(
        "[prune-stale-tags] dry-run only — pass --force-remote with --execute "
        "to override.\n"
    )
    return None


def _run_git(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=_GIT_TIMEOUT_S,
    )


def _mirror_tag_set(remote: str) -> set[str]:
    return set(_ls_remote_tags(remote))


def _delete_mirror_tags(remote: str, tags: list[str]) -> int:
    """Delete ``tags`` on ``remote`` one at a time; tolerate already-absent refs."""
    present = [tag for tag in tags if tag in _mirror_tag_set(remote)]
    if not present:
        return 0
    for tag in present:
        cmd = ["git", "-C", str(REPO_ROOT), "push", remote, "--delete", tag]
        print(f"[prune-stale-tags] running: {shlex.join(cmd)}")
        proc = _run_git(cmd)
        if proc.returncode == 0:
            continue
        if tag not in _mirror_tag_set(remote):
            continue
        detail = (proc.stderr or proc.stdout or "").strip()
        sys.stderr.write(
            f"[prune-stale-tags] WARNING: mirror delete for {tag!r} exited "
            f"{proc.returncode}"
            + (f": {detail}" if detail else "")
            + "\n"
        )
        return proc.returncode
    return 0


def _delete_local_tags(tags: list[str]) -> int:
    """Delete local tags; tolerate refs that are already gone."""
    if not tags:
        return 0
    cmd = ["git", "-C", str(REPO_ROOT), "tag", "-d", *tags]
    print(f"[prune-stale-tags] running: {shlex.join(cmd)}")
    proc = _run_git(cmd)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        sys.stderr.write(
            f"[prune-stale-tags] WARNING: local delete exited {proc.returncode}"
            + (f": {detail}" if detail else "")
            + " (mirror may already be clean — re-run to finish local cleanup).\n"
        )
        return proc.returncode
    return 0


def _execute_prune(remote: str, tags: list[str]) -> int:
    """Mirror-first, then local. Local delete runs when mirror is already clean."""
    mirror_rc = _delete_mirror_tags(remote, tags)
    if mirror_rc != 0:
        remaining = [tag for tag in tags if tag in _mirror_tag_set(remote)]
        if remaining:
            sys.stderr.write(
                "[prune-stale-tags] mirror still carries offender tag(s); "
                "leaving local tags as the re-detection anchor.\n"
            )
            _print_consumer_note()
            return mirror_rc

    local_rc = _delete_local_tags(tags)
    if local_rc == 0:
        print("[prune-stale-tags] done: mirror + local stale tags pruned.")
    _print_consumer_note()
    return local_rc


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    remote = _resolve_remote(args)

    remote_err = _validate_remote(
        remote, force_remote=bool(args.force_remote), execute=bool(args.execute)
    )
    if remote_err is not None:
        return remote_err

    commit_date = _memoizing_commit_date()
    baseline, offenders = audit_stale_tags(commit_date)
    tags = [offender.tag for offender in offenders]
    local_offender_tags = set(tags)
    mirror_only = _mirror_only_offenders(
        remote, commit_date, baseline, local_offender_tags
    )

    print(
        f"[prune-stale-tags] rebrand baseline date: {baseline.isoformat()}; "
        f"mirror: {remote}"
    )
    if not tags and not mirror_only:
        print("[prune-stale-tags] no stale-higher tags — nothing to prune.")
        return 0

    if mirror_only:
        print(
            f"[prune-stale-tags] {len(mirror_only)} mirror-only offender(s) "
            "(absent locally — delete on remote by hand or fetch tags first):"
        )
        for tag in mirror_only:
            print(f"    {tag}")

    print(f"[prune-stale-tags] {len(tags)} offender tag(s):")
    for offender in offenders:
        print(
            f"    {offender.tag} "
            f"({offender.package or '(monorepo)'} declared {offender.declared})"
        )
    commands = _deletion_commands(remote, tags)

    if not args.execute:
        print("[prune-stale-tags] dry-run — would run (pass --execute to run):")
        for command in commands:
            print(f"    {shlex.join(command)}")
        _print_consumer_note()
        return 0

    if not args.assume_yes and not _confirm(remote):
        print("[prune-stale-tags] not confirmed — no tags deleted.")
        return 0

    return _execute_prune(remote, tags)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Delete the offender tags from the mirror and locally (default: "
        "dry-run, print the commands only). Prompts for confirmation.",
    )
    parser.add_argument(
        "--assume-yes",
        action="store_true",
        help="Skip the interactive confirmation prompt (operator automation).",
    )
    parser.add_argument(
        "--remote",
        default=None,
        help="Override the mirror remote (default: RELEASE_PUBLIC_REMOTE env or "
        "the public git remote).",
    )
    parser.add_argument(
        "--force-remote",
        action="store_true",
        help="Allow --execute against a remote that differs from the canonical "
        "public mirror URL.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())