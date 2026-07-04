#!/usr/bin/env python3
"""Orchestrate the git-only public WorkBay release flow (dry-run by default).

Pipeline: export -> push -> tag-sync -> status. PyPI publish retired.

Authoritative playbook: docs/RELEASING.md.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import tomllib
from datetime import date, datetime
from pathlib import Path
from typing import Callable, NamedTuple

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_HELPER = REPO_ROOT / "scripts" / "release_manifest.py"
EXPORT_PUBLIC = REPO_ROOT / "scripts" / "export_public.py"
RELEASE_SCRIPT = REPO_ROOT / "scripts" / "release.sh"

# Reuse the canonical semver parser from check_mcp_pin_drift rather than
# re-implementing version parsing (implementation note). Both scripts live in scripts/;
# ensure that directory is importable whether release_public.py runs as a script
# (scripts/ is already on sys.path[0]) or is loaded via importlib in a test.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from check_mcp_pin_drift import _version_triple  # noqa: E402

# The public consumer-facing repository the export is force-pushed to. Git-only
# delivery (Dist-1): there is no PyPI upload, so no Trusted-Publisher binding.
# Kept literal so this release script stays free of a workbay_protocol import;
# `make distribution-url-check` asserts it equals workbay_protocol.brand.REPO_URL
# so it cannot drift from the SSOT (PPSSOT-URL-02).
PUBLIC_GIT_REMOTE = "git@github.com:darce/workbay.git"
# Ordered list of pipeline steps surfaced in the dry-run plan. ``export`` and
# ``status`` never mutate remote state; ``push`` and ``tag-sync`` are the
# network-mutating steps guarded behind --execute + confirmation (MUTATING_STEPS).
PIPELINE_STEPS = ("export", "push", "tag-sync", "status")
MUTATING_STEPS = ("push", "tag-sync")
_GIT_TIMEOUT_S = 120

# --- Pre-rebrand stale-higher-tag detection (implementation note) ---------------------
# A published ``<pkg>-vX.Y.Z`` tag poisons semver-latest resolution
# (``git tag -l '<pkg>-v*' | sort -V | tail -1``) when its version sits ABOVE the
# package's current declared line yet points at a pre-rebrand publish commit. The
# functional discriminator is version > declared; the safety gate against an
# intentionally-regressed line is the tagged commit predating the rebrand
# baseline. Reachability from ``main`` is NOT used — every package release tag
# anchors to a scrubbed publish commit, so stale and current-line tags are equally
# unreachable (verified non-discriminating; see docs/plans/0079-...).
# Full 40-char SHA (not an 8-hex abbreviation) so resolution can never become
# ambiguous as history grows.
REBRAND_BASELINE_COMMIT = "53222a7fe594a49750c16d9bb44da9d968044c15"
# Retired distributions whose ``packages/<name>`` tree is gone: the entire tag
# family is stale. Enumerated explicitly so other renamed pre-rebrand families
# (the ``agentic-*`` tag line and the prior-brand families that succeeded it)
# stay OUT of scope — they do not poison current ``<pkg>-v*`` resolution and
# are not this plan's concern.
RETIRED_FAMILIES = ("workbay-stack",)
# Sentinel declared-version for a retired family: any real tag outranks it.
REMOVED_DECLARED = "removed"
# Declared-map key for the bare monorepo ``vX.Y.Z`` family (no package prefix).
MONOREPO_FAMILY = ""

_PKG_TAG_RE = re.compile(r"^(?P<pkg>.+)-v(?P<ver>\d+\.\d+\.\d+)$")
_MONOREPO_TAG_RE = re.compile(r"^v(?P<ver>\d+\.\d+\.\d+)$")


class StaleTag(NamedTuple):
    """A tag flagged as a pre-rebrand higher-than-current-line offender."""

    tag: str
    package: str  # family name; MONOREPO_FAMILY ('') for the bare v* line
    version: str
    declared: str  # declared version compared against, or REMOVED_DECLARED
    committed: date | None  # None when the date gate is skipped (release guard)


def _parse_tag(tag: str) -> tuple[str, str] | None:
    """Split a tag into ``(family, version)``.

    ``family`` is ``MONOREPO_FAMILY`` for the bare ``vX.Y.Z`` line. Returns
    ``None`` for anything that is not a three-component release tag
    (pre-release / rc / non-version tags are ignored).
    """
    mono = _MONOREPO_TAG_RE.match(tag)
    if mono:
        return (MONOREPO_FAMILY, mono.group("ver"))
    pkg = _PKG_TAG_RE.match(tag)
    if pkg:
        return (pkg.group("pkg"), pkg.group("ver"))
    return None


def _stale_higher_tags(
    declared: dict[str, str],
    tags: list[str],
    commit_date: Callable[[str], date] | None = None,
    baseline: date | None = None,
) -> list[StaleTag]:
    """Return the tags that are higher-than-declared offenders.

    A tag is an offender when its parsed version exceeds the declared version for
    its family AND (in dated mode) its tagged commit's committer date precedes
    ``baseline``. ``declared`` maps a family (``MONOREPO_FAMILY`` for the bare
    ``v*`` line) to its current declared version, or ``REMOVED_DECLARED`` for a
    retired family whose whole tag family is stale. Families absent from
    ``declared`` are out of scope (renamed pre-rebrand names) and skipped.

    When ``commit_date``/``baseline`` are omitted the pre-baseline date gate is
    skipped and the detector reduces to pure version-higher detection — used by
    the release-time guard, which compares against the mirror's tag NAMES
    (``git ls-remote``) whose commit objects are not present locally. ``commit_date``
    is injected so the dated mode stays hermetically testable.
    """
    if (commit_date is None) != (baseline is None):
        # Fail loud, not open: partial date args would silently disable the
        # pre-rebrand gate on a detector that gates destructive prune/release.
        raise ValueError(
            "commit_date and baseline must be provided together (dated mode) or "
            "both omitted (version-only mode)."
        )
    check_date = commit_date is not None
    offenders: list[StaleTag] = []
    for tag in tags:
        parsed = _parse_tag(tag)
        if parsed is None:
            continue
        family, version_str = parsed
        if family not in declared:
            continue
        version = _version_triple(version_str)
        if version is None:
            continue
        declared_str = declared[family]
        if declared_str == REMOVED_DECLARED:
            # Retired family: no current line exists, so the whole family is
            # stale by definition. Flag unconditionally, bypassing the date gate
            # (which only guards a live CURRENT line against an intentional
            # regression — inapplicable when the package is gone).
            committed = commit_date(tag) if check_date else None  # type: ignore[misc]
            offenders.append(
                StaleTag(tag, family, version_str, declared_str, committed)
            )
            continue
        declared_triple = _version_triple(declared_str)
        if declared_triple is None:
            # Fail closed: a safety detector must never silently drop a family
            # because its declared version is unparseable (e.g. a dev/rc string).
            raise ValueError(
                f"declared version for {family or '(monorepo)'} is not a "
                f"three-part semver: {declared_str!r} — cannot audit safely."
            )
        if version <= declared_triple:
            continue
        committed = commit_date(tag) if check_date else None  # type: ignore[misc]
        if check_date and committed >= baseline:  # type: ignore[operator]
            # Higher, but published at/after the rebrand — a legitimate current
            # or future line, never a pre-rebrand offender.
            continue
        offenders.append(
            StaleTag(tag, family, version_str, declared_str, committed)
        )
    offenders.sort(key=lambda s: (s.package, _version_triple(s.version) or (0, 0, 0)))
    return offenders


def _advance_local_monorepo_tag(repo_root: Path, monorepo_tag: str) -> None:
    """Create/advance the monorepo v-tag in the LOCAL repo after a successful
    mirror push.

    release.sh's ``suggested_next_tag`` is ``max(git tag -l 'v*') + 1``. Because
    tag-sync creates the consumer tag only in the ephemeral export tree and
    pushes it to the mirror, the local repo never learned about it — so the next
    release re-suggested the already-published tag and force-clobbered it on the
    mirror. Advancing the local tag here keeps ``git tag -l`` in step with what
    was published. The local tag marks the local source release commit (HEAD);
    the mirror tag marks the scrubbed export commit — both legitimate.
    """
    subprocess.run(
        ["git", "-C", str(repo_root), "tag", "-f", monorepo_tag],
        check=True,
        timeout=_GIT_TIMEOUT_S,
    )


def _run_text(args: list[str]) -> str:
    proc = subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=_GIT_TIMEOUT_S,
    )
    return proc.stdout


def git_mirror_packages() -> list[str]:
    """Git-mirror package names from the release manifest.

    Reuses release_manifest.py's --release-only filter so the public-release
    release pipeline operates on (git_mirror=true members).
    """
    out = _run_text(
        [
            sys.executable,
            str(MANIFEST_HELPER),
            "list",
            "--release-only",
            "--field",
            "name",
        ]
    )
    return [line for line in out.splitlines() if line.strip()]


def release_plan() -> dict[str, object]:
    """Canonical machine-readable release plan from scripts/release.sh."""
    out = _run_text(["bash", str(RELEASE_SCRIPT), "plan", "--json"])
    return json.loads(out)



def build_report(
    *,
    execute: bool,
    confirmed: bool,
    plan: dict[str, object],
    packages: list[str],
    allow_higher_existing_tags: bool = False,
) -> dict[str, object]:
    """Assemble the full release-public report (the --json payload)."""
    will_mutate = bool(execute and confirmed)

    steps = []
    for name in PIPELINE_STEPS:
        mutating = name in MUTATING_STEPS
        steps.append(
            {
                "step": name,
                "mutating": mutating,
                "action": ("execute" if (mutating and will_mutate) else "plan"),
            }
        )

    package_status = []
    plan_packages = {
        entry["name"]: entry
        for entry in plan.get("packages", [])
        if isinstance(entry, dict)
    }
    for package in packages:
        entry = plan_packages.get(package, {})
        package_status.append(
            {
                "name": package,
                "private_source_tag": entry.get("state", "unknown"),
                "public_export_branch": "pending_export",
                "public_tag": "pending_tag_sync",
            }
        )

    return {
        "mode": "execute" if execute else "dry-run",
        "confirmed": confirmed,
        "will_mutate": will_mutate,
        "public_remote": PUBLIC_GIT_REMOTE,
        "monorepo_tag": (plan.get("monorepo", {}) or {}).get("suggested_next_tag"),
        "allow_higher_existing_tags": allow_higher_existing_tags,
        "steps": steps,
        "git_mirror_packages": list(packages),
        "status": package_status,
    }


def render_text(report: dict[str, object]) -> str:
    lines: list[str] = []
    mode = report["mode"]
    lines.append(f"[release-public] mode: {mode}")
    lines.append(f"[release-public] public remote: {report['public_remote']}")
    lines.append(f"[release-public] monorepo tag (suggested): {report['monorepo_tag']}")
    lines.append(
        "[release-public] git-mirror packages: "
        + ", ".join(report.get("git_mirror_packages", []))
    )
    lines.append("")
    lines.append("Pipeline steps (in order):")
    for index, step in enumerate(report["steps"], start=1):
        marker = "MUTATING" if step["mutating"] else "read-only"
        lines.append(f"  {index}. {step['step']:<9} [{marker}] -> {step['action']}")
    lines.append("")

    lines.append("Release status:")
    header = f"  {'PACKAGE':<28} {'SRC-TAG':<16} {'PUB-BRANCH':<16} {'PUB-TAG':<16}"
    lines.append(header)
    for entry in report["status"]:
        lines.append(
            f"  {entry['name']:<28} {entry['private_source_tag']:<16} "
            f"{entry['public_export_branch']:<16} {entry['public_tag']:<16}"
        )
    lines.append("")

    if report["mode"] == "execute" and not report["confirmed"]:
        lines.append(
            "[release-public] --execute requested but not confirmed — "
            "no network-mutating step ran (git push / tag push skipped)."
        )
    elif report["mode"] == "dry-run":
        lines.append(
            "[release-public] dry-run only — no network mutation performed. "
            "Re-run with --execute to push/tag after confirmation."
        )
    return "\n".join(lines) + "\n"


def confirm_interactively() -> bool:
    """Prompt the operator before any network-mutating step.

    Reads from stdin; returns False on EOF / non-tty so a non-interactive
    --execute invocation never mutates. ``--assume-yes`` bypasses this.
    """
    # Write the prompt to stderr so a --json run keeps stdout machine-clean.
    sys.stderr.write(
        "About to force-push the exported tree to the public repo and "
        "force-push its tag family (no PyPI upload).\n"
        "This mutates remote state. Type 'release' to proceed: "
    )
    sys.stderr.flush()
    try:
        answer = input()
    except EOFError:
        return False
    return answer.strip() == "release"


def run(argv: list[str]) -> int:
    args = parse_args(argv)

    # ``audit-tags`` is a read-only subcommand: it never mutates local or remote
    # state and short-circuits the export/push/tag-sync pipeline entirely.
    if args.command == "audit-tags":
        return _audit_tags(as_json=bool(args.json))

    execute = bool(args.execute)
    confirmed = False
    if execute:
        confirmed = True if args.assume_yes else confirm_interactively()

    packages = git_mirror_packages()
    plan = release_plan()
    report = build_report(
        execute=execute,
        confirmed=confirmed,
        plan=plan,
        packages=packages,
        allow_higher_existing_tags=bool(args.allow_higher_existing_tags),
    )

    if args.json:
        sys.stdout.write(json.dumps(report, indent=2) + "\n")
    else:
        sys.stdout.write(render_text(report))

    if execute and not confirmed:
        return 0

    if execute and confirmed:
        return _execute(report)

    return 0


def _package_paths() -> dict[str, str]:
    """Map each publishable package name to its repo-relative directory.

    Sourced from the release manifest's ``path`` field rather than assuming a
    ``packages/<name>`` layout. The name and directory happen to coincide for
    every package today, but the manifest is the authoritative mapping, so a
    future package whose distribution name diverges from its directory still
    resolves correctly.
    """
    out = _run_text(
        [
            sys.executable,
            str(MANIFEST_HELPER),
            "list",
            "--release-only",
            "--format",
            "json",
        ]
    )
    data = json.loads(out)
    return {entry["name"]: entry["path"] for entry in data if entry.get("name")}


def _package_version(package_path: str) -> str:
    """Return the declared version from ``package_path``'s pyproject.toml."""
    pyproject = REPO_ROOT / package_path / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def _local_tags() -> list[str]:
    """All tag names in the local repo (``git tag -l``)."""
    out = _run_text(["git", "-C", str(REPO_ROOT), "tag", "-l"])
    return [line for line in out.splitlines() if line.strip()]


def _ls_remote_tags(remote: str) -> list[str]:
    """Tag names on ``remote`` via ``git ls-remote --tags --refs``.

    ``--refs`` drops peeled ``^{}`` lines. Each line is ``<sha>\\trefs/tags/<tag>``;
    the tag name is the ref with its exact ``refs/tags/`` prefix stripped (not a
    last-``/``-segment split, so a slash-namespaced tag survives intact and is
    rejected by ``_parse_tag`` rather than truncated into a false family match).
    This is the sole read in the tag-sync guard and runs only under ``--execute``
    — no round-trip in ``status``/dry-run. Raises ``RuntimeError`` (not a
    stderr-swallowed ``CalledProcessError``) when the mirror cannot be read.
    """
    try:
        out = _run_text(["git", "ls-remote", "--tags", "--refs", remote])
    except subprocess.TimeoutExpired as exc:
        # A hung mirror must surface as the guard-catchable RuntimeError, not an
        # uncaught TimeoutExpired traceback (implementation note review C176-S2-A2).
        raise RuntimeError(
            f"timed out reading tags from mirror {remote!r} after "
            f"{_GIT_TIMEOUT_S}s"
        ) from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip()
        raise RuntimeError(
            f"could not read tags from mirror {remote!r}"
            + (f": {detail}" if detail else "")
        ) from exc
    tags: list[str] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        ref = line.split("\t", 1)[-1]
        tags.append(ref.removeprefix("refs/tags/"))
    return tags


def _commit_date(ref: str) -> datetime:
    """Committer timestamp of the commit ``ref`` resolves to (tz-aware datetime).

    Returns the FULL ISO-8601 committer timestamp, not a day-truncated calendar
    date: truncating to a date makes the baseline gate fail-open for a pre-rebrand
    tag committed EARLIER on the same calendar day as the rebrand-baseline commit
    — it would compare equal to the baseline and be misclassified as a legitimate
    post-baseline line (implementation note review C176-S1-A1). The audit/guard compares
    timestamps, so second precision is retained here; human/JSON output renders
    the date component only.

    Raises a clear ``RuntimeError`` (rather than leaking git's stderr-swallowed
    ``CalledProcessError`` or an uncaught ``TimeoutExpired``) when the commit
    object is absent — e.g. a shallow clone or the scrubbed public mirror where
    the rebrand-baseline commit is not present — or when the read times out.
    ``audit-tags`` is meant to run against the full source monorepo.
    """
    try:
        out = _run_text(
            [
                "git",
                "-C",
                str(REPO_ROOT),
                "show",
                "-s",
                "--format=%cI",
                f"{ref}^{{commit}}",
            ]
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"timed out resolving commit for {ref!r} after {_GIT_TIMEOUT_S}s."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"could not resolve commit for {ref!r} — run audit-tags against the "
            "full source monorepo, not a shallow clone or a scrubbed mirror."
        ) from exc
    # ``%cI`` is strict ISO-8601 with offset, e.g. 2026-06-22T16:59:54-04:00.
    # Parse the whole timestamp so the baseline comparison is second-precise.
    return datetime.fromisoformat(out.strip())


def _monorepo_current_ceiling(
    tags: list[str], commit_date: Callable[[str], date], baseline: date
) -> str | None:
    """Highest monorepo ``vX.Y.Z`` published at/after the rebrand baseline, or None.

    Serves as the declared ceiling for the bare ``v*`` family. Pre-baseline tags
    are excluded from the ceiling so a stale high ``v`` tag cannot mask itself by
    being the numeric maximum. Returns ``None`` when no current-line monorepo tag
    exists — the caller then omits the ``v*`` family from the audit rather than
    defaulting to a ``0.0.0`` ceiling that would flag every pre-baseline ``v``
    tag as an offender (and, via the reused detector, drive Slice-3 prune to
    delete a legitimate historical ``v`` line).
    """
    best: tuple[int, int, int] | None = None
    for tag in tags:
        mono = _MONOREPO_TAG_RE.match(tag)
        if mono is None or commit_date(tag) < baseline:
            continue
        triple = _version_triple(mono.group("ver"))
        if triple is not None and (best is None or triple > best):
            best = triple
    if best is None:
        return None
    return "%d.%d.%d" % best


def _audit_declared(
    tags: list[str], commit_date: Callable[[str], date], baseline: date
) -> dict[str, str]:
    """Build the family -> declared-version map audited by ``audit-tags``.

    Git-mirrored release-manifest packages (``release_manifest.py list
    --release-only``) contribute their pyproject version; retired families
    contribute the ``REMOVED_DECLARED`` sentinel; the monorepo ``v*`` family
    contributes its current-line ceiling when one exists. Non-mirrored manifest
    packages (``git_mirror=false``, e.g. the private canvas member) are
    intentionally out of scope — they cannot poison the mirror's
    ``<pkg>-v*`` resolution.
    """
    declared = {name: _package_version(path) for name, path in _package_paths().items()}
    for family in RETIRED_FAMILIES:
        declared[family] = REMOVED_DECLARED
    ceiling = _monorepo_current_ceiling(tags, commit_date, baseline)
    if ceiling is not None:
        declared[MONOREPO_FAMILY] = ceiling
    return declared


def audit_stale_tags(commit_date: Callable[[str], date]) -> tuple[date, list[StaleTag]]:
    """Resolve the baseline + local tags and return the stale-higher offenders.

    Shared by the ``audit-tags`` subcommand and the operator prune tool so both
    key on the identical detection logic. ``commit_date`` is threaded through (a
    memoizing wrapper in the caller) to bound repeated ``git show`` calls.
    """
    baseline = commit_date(REBRAND_BASELINE_COMMIT)
    tags = _local_tags()
    declared = _audit_declared(tags, commit_date, baseline)
    return baseline, _stale_higher_tags(declared, tags, commit_date, baseline)


def _memoizing_commit_date() -> Callable[[str], date]:
    """A ``_commit_date`` wrapper that caches per-ref lookups within one run."""
    cache: dict[str, date] = {}

    def lookup(ref: str) -> date:
        if ref not in cache:
            cache[ref] = _commit_date(ref)
        return cache[ref]

    return lookup


def _audit_tags(*, as_json: bool = False) -> int:
    """Read-only audit: print pre-rebrand stale-higher tags; nonzero if any exist."""
    baseline, offenders = audit_stale_tags(_memoizing_commit_date())

    if as_json:
        sys.stdout.write(
            json.dumps(
                {
                    "rebrand_baseline_commit": REBRAND_BASELINE_COMMIT,
                    "rebrand_baseline_date": baseline.isoformat()[:10],
                    "offenders": [
                        {
                            "tag": o.tag,
                            "package": o.package or "(monorepo)",
                            "version": o.version,
                            "declared": o.declared,
                            "committed": (
                                o.committed.isoformat()[:10]
                                if o.committed is not None
                                else None
                            ),
                        }
                        for o in offenders
                    ],
                },
                indent=2,
            )
            + "\n"
        )
        return 1 if offenders else 0

    sys.stdout.write(
        f"[audit-tags] rebrand baseline: {REBRAND_BASELINE_COMMIT} "
        f"({baseline.isoformat()[:10]})\n"
    )
    if not offenders:
        sys.stdout.write("[audit-tags] no stale-higher tags — clean.\n")
        return 0
    sys.stdout.write(
        f"  {'FAMILY':<28} {'TAG':<34} {'VERSION':<9} {'DECLARED':<9} COMMITTED\n"
    )
    for o in offenders:
        committed = o.committed.isoformat()[:10] if o.committed is not None else "-"
        sys.stdout.write(
            f"  {(o.package or '(monorepo)'):<28} {o.tag:<34} "
            f"{o.version:<9} {o.declared:<9} {committed}\n"
        )
    families = sorted({o.package for o in offenders})
    sys.stdout.write(
        f"[audit-tags] {len(offenders)} stale-higher tag(s) across "
        f"{len(families)} famil(ies). "
        "Remediation: python scripts/prune_stale_tags.py [--execute]\n"
    )
    return 1


def _export_command() -> list[str]:
    """The export invocation, overridable for hermetic tests.

    Defaults to ``<python> scripts/export_public.py``. The
    ``RELEASE_PUBLIC_EXPORT_CMD`` env seam lets the test suite point the export
    at a stub that materializes an output tree without touching git or the
    network.
    """
    override = os.environ.get("RELEASE_PUBLIC_EXPORT_CMD")
    if override:
        return shlex.split(override)
    return [sys.executable, str(EXPORT_PUBLIC)]


def _execute(report: dict[str, object]) -> int:
    """Perform the network-mutating public-release steps.

    Reached only under ``--execute`` + confirmation. Three idempotent steps
    (export, push, tag-sync), each shelled out so the orchestration is
    hermetically testable (stub ``git`` on PATH + the ``RELEASE_PUBLIC_*`` env
    seams):

      1. export    — build the fresh-history public tree (single scrubbed
                     commit) via ``scripts/export_public.py --out <tmp> --force``.
      2. push      — force-push that commit to the public ``main``. The export
                     rewrites history every run, so a force replacement is the
                     intended, convergent behavior.
      3. tag-sync  — create the per-package tag family (``<pkg>-vX.Y.Z``) plus
                     the consumer-facing ``vX.Y.Z`` monorepo tag on the exported
                     commit and force-push them.

    PyPI publish is retired; git mirror push + tag-sync is the sole release action.
    """
    monorepo_tag = report.get("monorepo_tag")
    if not monorepo_tag:
        sys.stderr.write(
            "[release-public] aborting: no consumer-facing monorepo tag in the "
            "release plan; nothing to tag-sync.\n"
        )
        return 1

    remote = os.environ.get("RELEASE_PUBLIC_REMOTE", PUBLIC_GIT_REMOTE)
    package_paths = _package_paths()
    packages = [
        entry["name"]
        for entry in report.get("status", [])
        if isinstance(entry, dict) and entry.get("name")
    ]

    # Tag-sync preflight guard (implementation note, implementation note). Refuse to release while the
    # mirror already carries a HIGHER tag than the version being released for any
    # family — a force-push of our lower tag would not remove it, leaving
    # ``sort -V | tail -1`` poisoned. Runs BEFORE export/push so an abort leaves
    # the mirror untouched (no partial release). Version-only comparison against
    # the mirror's tag NAMES (its commit objects are not local, so no date gate).
    # ``git ls-remote`` is the only read and this path is ``--execute`` only, so
    # there is no new round-trip in status/dry-run.
    allow_higher = bool(report.get("allow_higher_existing_tags"))
    # Scope: active release families + monorepo v* + retired families (version-only
    # on mirror tag names). Dated audit/prune still flags retired families locally.
    guard_declared = {
        package: _package_version(package_paths.get(package, f"packages/{package}"))
        for package in packages
    }
    for family in RETIRED_FAMILIES:
        guard_declared[family] = REMOVED_DECLARED
    mono_parsed = _parse_tag(str(monorepo_tag))
    if mono_parsed is not None:
        guard_declared[MONOREPO_FAMILY] = mono_parsed[1]
    else:
        sys.stderr.write(
            "[release-public] WARNING: monorepo tag "
            f"{monorepo_tag!r} is not a three-part vX.Y.Z; the bare v* family is "
            "not covered by the tag-sync guard this run.\n"
        )
    try:
        mirror_tags = _ls_remote_tags(remote)
    except RuntimeError as exc:
        sys.stderr.write(f"[release-public] ABORT: {exc}\n")
        return 1
    higher_existing = _stale_higher_tags(guard_declared, mirror_tags)
    if higher_existing:
        listing = ", ".join(
            f"{st.tag} > {st.package or '(monorepo)'} v{st.declared}"
            for st in higher_existing
        )
        if not allow_higher:
            sys.stderr.write(
                "[release-public] ABORT: the mirror already carries higher tags "
                "than the versions being released:\n"
            )
            for st in higher_existing:
                sys.stderr.write(
                    f"    {st.tag} (on mirror) outranks "
                    f"{st.package or '(monorepo)'} v{st.declared} being released\n"
                )
            sys.stderr.write(
                "[release-public] Prune the stale tags first: "
                "python scripts/prune_stale_tags.py --execute\n"
                "[release-public] If the offender exists on the mirror but not "
                "locally, fetch tags or delete on the remote by hand (prune "
                "enumerates local tags only).\n"
                "[release-public] or re-run with --allow-higher-existing-tags to "
                "override (recorded in the release log).\n"
            )
            return 1
        sys.stderr.write(
            "[release-public] --allow-higher-existing-tags: proceeding despite "
            f"higher mirror tags: {listing}\n"
        )

    export_dir = Path(tempfile.mkdtemp(prefix="workbay-public-export-"))

    def _git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(export_dir), *args],
            check=True,
            cwd=REPO_ROOT,
            timeout=_GIT_TIMEOUT_S,
        )

    try:
        # 1. export
        sys.stderr.write(f"[release-public] export -> {export_dir}\n")
        subprocess.run(
            [*_export_command(), "--out", str(export_dir), "--force"],
            check=True,
            cwd=REPO_ROOT,
            timeout=_GIT_TIMEOUT_S,
        )

        # 2. push the exported fresh history to the public main.
        sys.stderr.write(f"[release-public] push -> {remote} (force, main)\n")
        _git("push", "--force", remote, "HEAD:refs/heads/main")

        # 3. tag-sync: per-package family + the monorepo consumer tag, on the
        #    exported commit, force-pushed (the public commit changes every export).
        tags = [
            f"{package}-v{_package_version(package_paths.get(package, f'packages/{package}'))}"
            for package in packages
        ]
        tags.append(str(monorepo_tag))
        for tag in tags:
            _git("tag", "-f", tag)
        _git("push", "--force", remote, *tags)
        # Advance the LOCAL monorepo v-tag so release.sh's suggested_next_tag
        # (computed from `git tag -l`) tracks what was just published instead of
        # lagging the mirror and re-suggesting an already-published tag (which
        # would force-clobber it on the next release). The local tag marks the
        # local source release commit; the mirror tag marks the scrubbed export.
        _advance_local_monorepo_tag(REPO_ROOT, str(monorepo_tag))

        sys.stderr.write(
            "[release-public] tag-sync -> "
            + ", ".join(tags)
            + "\n[release-public] done: export -> push -> tag-sync (git-only; no PyPI publish).\n"
        )
        return 0
    finally:
        # Always remove the throwaway export tree, even on a partial failure,
        # so repeated --execute runs do not accumulate /tmp export dirs.
        shutil.rmtree(export_dir, ignore_errors=True)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        nargs="?",
        choices=("audit-tags",),
        default=None,
        help=(
            "Optional read-only subcommand. 'audit-tags' lists pre-rebrand "
            "stale-higher tags (version above the declared line on a pre-rebrand "
            "commit) and exits nonzero when any exist. Omit to run the release "
            "pipeline plan."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Plan and report only; perform no network mutation (default).",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Perform network-mutating steps after interactive confirmation.",
    )
    parser.add_argument(
        "--assume-yes",
        action="store_true",
        help="Skip the interactive confirmation prompt (operator automation).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the machine-readable report instead of the text report.",
    )
    parser.add_argument(
        "--allow-higher-existing-tags",
        action="store_true",
        help=(
            "Override the tag-sync preflight guard: proceed with --execute even "
            "when the mirror already carries a higher tag than the version being "
            "released (the override is logged). Without it, --execute aborts and "
            "names the offenders."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    raise SystemExit(main())
