#!/usr/bin/env python3
"""Orchestrate the git-only public WorkBay release flow (dry-run by default).

Pipeline: export -> push -> tag-sync -> status. PyPI publish retired.

Authoritative playbook: docs/RELEASING.md.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_HELPER = REPO_ROOT / "scripts" / "release_manifest.py"
EXPORT_PUBLIC = REPO_ROOT / "scripts" / "export_public.py"
RELEASE_SCRIPT = REPO_ROOT / "scripts" / "release.sh"

# The public consumer-facing repository the export is force-pushed to. Git-only
# delivery (Dist-1): there is no PyPI upload, so no Trusted-Publisher binding.
PUBLIC_GIT_REMOTE = "git@github.com:darce/workbay.git"
# Ordered list of pipeline steps surfaced in the dry-run plan. ``export`` and
# ``status`` never mutate remote state; ``push`` and ``tag-sync`` are the
# network-mutating steps guarded behind --execute + confirmation (MUTATING_STEPS).
PIPELINE_STEPS = ("export", "push", "tag-sync", "status")
MUTATING_STEPS = ("push", "tag-sync")



def _run_text(args: list[str]) -> str:
    proc = subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
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

    export_dir = Path(tempfile.mkdtemp(prefix="workbay-public-export-"))

    def _git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(export_dir), *args],
            check=True,
            cwd=REPO_ROOT,
        )

    try:
        # 1. export
        sys.stderr.write(f"[release-public] export -> {export_dir}\n")
        subprocess.run(
            [*_export_command(), "--out", str(export_dir), "--force"],
            check=True,
            cwd=REPO_ROOT,
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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    raise SystemExit(main())
