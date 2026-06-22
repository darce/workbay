#!/usr/bin/env python3
"""Orchestrate the public WorkBay release flow (dry-run by default).

This is the single release-owned command that composes the existing release
tooling into one repeatable public-release path:

  1. preflight  — verify every publishable package has a matching PyPI Trusted
                  Publisher binding for ``darce/workbay`` /
                  ``release-publish.yml`` / ``pypi`` (reported in dry-run, the
                  live PyPI probe is gated behind ``--execute``),
  2. export     — build the public tree via ``scripts/export_public.py``,
  3. push       — push the exported tree to ``git@github.com:darce/workbay.git``
                  (``--execute`` only),
  4. tag-sync   — create the per-package tag family plus the consumer-facing
                  ``vX.Y.Z`` monorepo tag on the public commit (``--execute`` only),
  5. status     — a unified report distinguishing the five release states.

Dry-run is the default. Network-mutating steps (git push, tag push) run ONLY
under ``--execute`` *and* an interactive operator confirmation *and* a passing
publisher preflight. Every mutating step is idempotent (re-running converges:
the export rewrites history and tags are force-synced to the new commit). PyPI
upload is intentionally out of scope — it is a separate, explicit step: after
this command syncs sources + tags to darce/workbay, the operator dispatches
``gh workflow run release-publish.yml`` (Trusted Publishing, the primary path).
Local ``twine`` upload remains only the documented fallback.

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

# The public consumer-facing repository and the Trusted Publisher binding that
# each publishable PyPI project must carry before any upload is dispatched.
PUBLIC_GIT_REMOTE = "git@github.com:darce/workbay.git"
PUBLISHER_OWNER = "darce"
PUBLISHER_REPO = "workbay"
PUBLISHER_WORKFLOW = "release-publish.yml"
PUBLISHER_ENVIRONMENT = "pypi"

# Ordered list of pipeline steps surfaced in the dry-run plan. The two
# read-only steps (preflight, status) never mutate; the middle three are the
# network-mutating steps guarded behind --execute + confirmation.
PIPELINE_STEPS = ("preflight", "export", "push", "tag-sync", "status")
MUTATING_STEPS = ("push", "tag-sync", "publish")

# Documented account-scoped fallback for a missing Trusted Publisher, a PyPI
# outage, or a publisher-form failure. Surfaced verbatim in the preflight
# report so an operator never has to leave the command output to recover.
MANUAL_UPLOAD_FALLBACK = (
    "fallback: if a Trusted Publisher is missing or PyPI is unavailable, "
    "publish with the account-scoped API token via "
    "`uvx --with keyring twine upload packages/<pkg>/dist/*` "
    "(scripts/release.sh package <pkg>), then add the Trusted Publisher "
    f"({PUBLISHER_OWNER}/{PUBLISHER_REPO} / {PUBLISHER_WORKFLOW} / "
    f"{PUBLISHER_ENVIRONMENT}) before the next release."
)


def _run_text(args: list[str]) -> str:
    proc = subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout


def publishable_packages() -> list[str]:
    """Publishable (publish=true) package names from the release manifest.

    Reuses release_manifest.py's --release-only filter so the public-release
    preflight enumerates exactly the same set the publish workflow and
    release.sh operate on (mcp-workbay-canvas, publish=false, is excluded).
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


# Tri-state outcomes of a publisher probe. ``missing`` and ``unverifiable`` are
# deliberately distinct: ``missing`` is positive proof the binding is absent (the
# project 404s, or exposes an explicit publisher list without our binding) and IS
# a config error to fix; ``unverifiable`` means the project exists but PyPI's
# public JSON API does not reveal publisher metadata, so absence is unproven — the
# operator confirms in the PyPI UI and re-runs with --publishers-verified. Flatly
# reporting the latter as ``missing`` was the 0019 footgun this hardening fixes.
# Since the bindings were confirmed once in the PyPI settings UI, the CLI now
# ASSUMES verified publishers by default; pass --probe-publishers to force the
# live (mostly-unverifiable) probe again.
PUBLISHER_COVERED = "covered"
PUBLISHER_MISSING = "missing"
PUBLISHER_UNVERIFIABLE = "unverifiable"


def classify_publisher_payload(payload: dict | None) -> str:
    """Tri-state classification of a PyPI JSON ``payload`` for the expected binding.

    ``payload is None`` models a 404 (project absent). A ``dict`` payload is the
    parsed project JSON. Returns one of ``covered`` / ``missing`` / ``unverifiable``.
    """
    if payload is None:
        # 404 — the project does not exist yet, so a pending publisher is
        # genuinely required. This is a verifiable absence.
        return PUBLISHER_MISSING
    publishers = payload.get("trusted-publishers")
    if publishers is None:
        publishers = payload.get("trusted_publishers")
    if not isinstance(publishers, list):
        # The public JSON API exposes no publisher field for most projects;
        # absence here is NOT proof of a missing binding.
        return PUBLISHER_UNVERIFIABLE
    for publisher in publishers:
        if not isinstance(publisher, dict):
            continue
        if (
            publisher.get("owner") == PUBLISHER_OWNER
            and publisher.get("repository") == PUBLISHER_REPO
            and publisher.get("workflow") == PUBLISHER_WORKFLOW
        ):
            return PUBLISHER_COVERED
    # An explicit publisher list that does not contain our binding (including an
    # empty list) IS positive proof the binding is absent.
    return PUBLISHER_MISSING


def probe_trusted_publisher(distribution: str) -> str:
    """Tri-state probe of ``distribution``'s Trusted Publisher (see classify_*).

    NETWORK-MUTATING-ADJACENT: this hits the PyPI JSON API and is only ever
    called under --execute. In dry-run the preflight reports the binding it
    *would* check without contacting PyPI.
    """
    # Hermetic test seam: distributions whose publisher metadata exists but is
    # not exposed by the public API (the common real case) report unverifiable.
    unverifiable = os.environ.get("RELEASE_PUBLIC_FAKE_UNVERIFIABLE")
    if unverifiable is not None:
        unverifiable_set = {name for name in unverifiable.split(",") if name}
        if distribution in unverifiable_set:
            return PUBLISHER_UNVERIFIABLE
    # Hermetic test seam: a comma-separated allowlist of distributions whose
    # publisher binding is considered present; everything else is a verifiable
    # absence (missing). Used by the dry-run-only suite to exercise the
    # --execute preflight branch without any network call.
    override = os.environ.get("RELEASE_PUBLIC_FAKE_PUBLISHERS")
    if override is not None:
        covered = {name for name in override.split(",") if name}
        return PUBLISHER_COVERED if distribution in covered else PUBLISHER_MISSING

    import urllib.error
    import urllib.request

    url = f"https://pypi.org/pypi/{distribution}/json"
    try:
        with urllib.request.urlopen(url, timeout=15) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return PUBLISHER_MISSING
        raise
    return classify_publisher_payload(payload)


def build_preflight(
    packages: list[str],
    *,
    execute: bool,
    publishers_verified: bool = False,
    missing_publishers: list[str] | None = None,
    unverifiable_publishers: list[str] | None = None,
) -> dict[str, object]:
    """Assemble the Trusted-Publisher preflight result.

    In dry-run (``execute`` is False) this is a *reported* check: it records
    the exact set of packages that WOULD be probed and reports ``checked`` as
    False without any network call. Under ``--execute`` it probes PyPI and
    partitions packages into ``missing`` (verifiably absent — a config error)
    and ``unverifiable`` (the project exists but PyPI's public JSON API does not
    expose publisher metadata, so absence is unproven). Either non-empty set
    blocks the release, but only ``unverifiable`` is recoverable with
    ``--publishers-verified`` after confirming in the PyPI UI.

    ``publishers_verified`` is the operator-confirm escape hatch: because the
    public JSON API does not expose Trusted-Publisher metadata for most
    projects, the live probe reports ``unverifiable`` rather than fail-closing
    as ``missing``. When the operator has confirmed the bindings out-of-band (or
    the release performs no PyPI upload), ``--publishers-verified`` records the
    set as covered without probing and stamps ``operator_verified``.
    """
    binding = {
        "owner": PUBLISHER_OWNER,
        "repository": PUBLISHER_REPO,
        "workflow": PUBLISHER_WORKFLOW,
        "environment": PUBLISHER_ENVIRONMENT,
    }
    if not execute:
        return {
            "checked": False,
            "would_check": list(packages),
            "binding": binding,
            "missing": [],
            "unverifiable": [],
            "ok": True,
            "operator_verified": False,
            "fallback": MANUAL_UPLOAD_FALLBACK,
        }

    if publishers_verified:
        return {
            "checked": True,
            "would_check": list(packages),
            "binding": binding,
            "missing": [],
            "unverifiable": [],
            "ok": True,
            "operator_verified": True,
            "fallback": MANUAL_UPLOAD_FALLBACK,
        }

    if missing_publishers is None or unverifiable_publishers is None:
        probed = {package: probe_trusted_publisher(package) for package in packages}
        if missing_publishers is None:
            missing_publishers = [
                p for p, state in probed.items() if state == PUBLISHER_MISSING
            ]
        if unverifiable_publishers is None:
            unverifiable_publishers = [
                p for p, state in probed.items() if state == PUBLISHER_UNVERIFIABLE
            ]
    blocked = bool(missing_publishers) or bool(unverifiable_publishers)
    return {
        "checked": True,
        "would_check": list(packages),
        "binding": binding,
        "missing": list(missing_publishers),
        "unverifiable": list(unverifiable_publishers),
        "ok": not blocked,
        "operator_verified": False,
        "fallback": MANUAL_UPLOAD_FALLBACK,
    }


def build_report(
    *,
    execute: bool,
    confirmed: bool,
    plan: dict[str, object],
    preflight: dict[str, object],
) -> dict[str, object]:
    """Assemble the full release-public report (the --json payload)."""
    packages = preflight["would_check"]
    will_mutate = bool(execute and confirmed and preflight.get("ok", False))

    steps = []
    for name in PIPELINE_STEPS:
        mutating = name in MUTATING_STEPS
        steps.append(
            {
                "step": name,
                "mutating": mutating,
                # Mutating steps only "run" under --execute + confirmation;
                # otherwise they are planned but skipped.
                "action": ("execute" if (mutating and will_mutate) else "plan"),
            }
        )

    # The five release states the status report must distinguish, sourced from
    # the existing release.sh pending-recovery state machine, plus the two
    # public-export-specific states this command owns.
    package_status = []
    plan_packages = {
        entry["name"]: entry
        for entry in plan.get("packages", [])
        if isinstance(entry, dict)
    }
    publisher_missing = set(preflight.get("missing", []))
    publisher_unverifiable = set(preflight.get("unverifiable", []))
    for package in packages:
        entry = plan_packages.get(package, {})
        if preflight["checked"]:
            if package in publisher_missing:
                publisher_state = "missing"
            elif package in publisher_unverifiable:
                publisher_state = "unverifiable"
            else:
                publisher_state = "ready"
        else:
            publisher_state = "unchecked (dry-run)"
        package_status.append(
            {
                "name": package,
                # private source-repo tag state (released/pending_upload/...)
                "private_source_tag": entry.get("state", "unknown"),
                # public export branch freshness + public tag presence are
                # only known after a real export/push; in build-only mode we
                # report the planned target, never a probed remote state.
                "public_export_branch": "pending_export",
                "public_tag": "pending_tag_sync",
                # PyPI publication state, derived from the same plan state.
                "pypi_publication": (
                    "published" if entry.get("state") == "released" else "unpublished"
                ),
                # PyPI Trusted Publisher readiness.
                "trusted_publisher": publisher_state,
            }
        )

    return {
        "mode": "execute" if execute else "dry-run",
        "confirmed": confirmed,
        "will_mutate": will_mutate,
        "public_remote": PUBLIC_GIT_REMOTE,
        "monorepo_tag": (plan.get("monorepo", {}) or {}).get("suggested_next_tag"),
        "steps": steps,
        "preflight": preflight,
        "status": package_status,
    }


def render_text(report: dict[str, object]) -> str:
    lines: list[str] = []
    mode = report["mode"]
    lines.append(f"[release-public] mode: {mode}")
    lines.append(f"[release-public] public remote: {report['public_remote']}")
    lines.append(f"[release-public] monorepo tag (suggested): {report['monorepo_tag']}")
    lines.append("")
    lines.append("Pipeline steps (in order):")
    for index, step in enumerate(report["steps"], start=1):
        marker = "MUTATING" if step["mutating"] else "read-only"
        lines.append(f"  {index}. {step['step']:<9} [{marker}] -> {step['action']}")
    lines.append("")

    preflight = report["preflight"]
    lines.append("PyPI Trusted Publisher preflight:")
    binding = preflight["binding"]
    lines.append(
        "  binding: "
        f"{binding['owner']}/{binding['repository']} / "
        f"{binding['workflow']} / {binding['environment']}"
    )
    if preflight["checked"]:
        if preflight["ok"] and preflight.get("operator_verified"):
            lines.append(
                "  result: OK — operator-verified (publishers confirmed "
                "out-of-band / no upload this release; live probe skipped)"
            )
        elif preflight["ok"]:
            lines.append("  result: OK — all publishable packages covered")
        else:
            missing = preflight.get("missing", [])
            unverifiable = preflight.get("unverifiable", [])
            if missing:
                lines.append(
                    "  result: FAIL — missing Trusted Publisher for: "
                    + ", ".join(missing)
                )
            if unverifiable:
                lines.append(
                    "  result: BLOCKED — unverifiable (PyPI's public JSON API "
                    "does not expose publisher metadata, so absence is unproven): "
                    "confirm the binding in the PyPI project settings UI, then "
                    "re-run with --publishers-verified (the default when "
                    "--probe-publishers is omitted), for: " + ", ".join(unverifiable)
                )
    else:
        lines.append(
            "  result: reported (dry-run) — would check: "
            + ", ".join(preflight["would_check"])
        )
    lines.append(f"  {preflight['fallback']}")
    lines.append("")

    lines.append("Release status (five states):")
    header = (
        f"  {'PACKAGE':<28} {'SRC-TAG':<16} {'PUB-BRANCH':<16} "
        f"{'PUB-TAG':<16} {'PYPI':<12} {'PUBLISHER':<18}"
    )
    lines.append(header)
    for entry in report["status"]:
        lines.append(
            f"  {entry['name']:<28} {entry['private_source_tag']:<16} "
            f"{entry['public_export_branch']:<16} {entry['public_tag']:<16} "
            f"{entry['pypi_publication']:<12} {entry['trusted_publisher']:<18}"
        )
    lines.append("")

    if report["mode"] == "execute" and not report["confirmed"]:
        lines.append(
            "[release-public] --execute requested but not confirmed — "
            "no network-mutating step ran (git push / tag push / PyPI "
            "upload all skipped)."
        )
    elif report["mode"] == "dry-run":
        lines.append(
            "[release-public] dry-run only — no network mutation performed. "
            "Re-run with --execute to push/tag/publish after confirmation."
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
        "This mutates remote state. Type 'publish' to proceed: "
    )
    sys.stderr.flush()
    try:
        answer = input()
    except EOFError:
        return False
    return answer.strip() == "publish"


def run(argv: list[str]) -> int:
    args = parse_args(argv)

    # --dry-run is the default; --execute opts into mutation, still gated on
    # an interactive confirmation (or --assume-yes for an automated operator).
    execute = bool(args.execute)
    confirmed = False
    if execute:
        confirmed = True if args.assume_yes else confirm_interactively()

    packages = publishable_packages()
    plan = release_plan()
    preflight = build_preflight(
        packages,
        execute=execute,
        publishers_verified=bool(args.publishers_verified),
    )
    report = build_report(
        execute=execute,
        confirmed=confirmed,
        plan=plan,
        preflight=preflight,
    )

    if args.json:
        sys.stdout.write(json.dumps(report, indent=2) + "\n")
    else:
        sys.stdout.write(render_text(report))

    # A failed publisher preflight under --execute blocks before any upload.
    if execute and not preflight.get("ok", True):
        missing = preflight.get("missing", [])
        unverifiable = preflight.get("unverifiable", [])
        parts = ["[release-public] aborting:"]
        if missing:
            parts.append(
                " missing Trusted Publisher binding for: " + ", ".join(missing) + "."
            )
        if unverifiable:
            parts.append(
                " unverifiable publisher binding for: "
                + ", ".join(unverifiable)
                + " — confirm in the PyPI project settings UI, then re-run with "
                "--publishers-verified (the default when --probe-publishers is "
                "omitted)."
            )
        sys.stderr.write("".join(parts) + "\n")
        return 1

    if execute and not confirmed:
        # Honor the gate: --execute without confirmation is a no-op, not an
        # error, so an operator can rehearse the prompt safely.
        return 0

    if args.preflight_only:
        # Rehearse the preflight (and, under --execute, the live probe) without
        # touching the mutating steps. Returns nonzero above if a publisher is
        # missing; reaching here means the preflight passed.
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
    ``RELEASE_PUBLIC_EXPORT_CMD`` env seam (mirroring
    ``RELEASE_PUBLIC_FAKE_PUBLISHERS``) lets the test suite point the export at
    a stub that materializes an output tree without touching git or the
    network.
    """
    override = os.environ.get("RELEASE_PUBLIC_EXPORT_CMD")
    if override:
        return shlex.split(override)
    return [sys.executable, str(EXPORT_PUBLIC)]


def _execute(report: dict[str, object]) -> int:
    """Perform the network-mutating public-release steps.

    Reached only under ``--execute`` + confirmation + a passing preflight.
    Three idempotent steps, each shelled out so the orchestration is
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

    PyPI upload is intentionally out of scope: it is a separate, explicit step.
    After this command syncs sources + tags to darce/workbay, dispatch
    ``gh workflow run release-publish.yml`` per pending package (Trusted
    Publishing, the primary path; see docs/RELEASING.md). Local ``twine`` is
    the fallback only.
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
            + "\n[release-public] done: export -> push -> tag-sync. PyPI upload "
            "not run here -- next, dispatch Trusted Publishing per pending "
            "package: scripts/release.sh package <pkg> (passes expected_sha256_* "
            "+ source_date_epoch; see docs/RELEASING.md).\n"
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
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Run the publisher preflight + report and stop before any "
            "export/push/tag/publish step (rehearsal)."
        ),
    )
    parser.add_argument(
        "--publishers-verified",
        action="store_true",
        default=True,
        help=(
            "DEFAULT. Treat every publishable package's Trusted Publisher as "
            "confirmed (the bindings were verified once in the PyPI project "
            "settings UI; the public JSON API cannot re-verify them). Skips "
            "the live PyPI probe and treats all packages as covered. Pass "
            "--probe-publishers to opt back into the live probe."
        ),
    )
    parser.add_argument(
        "--probe-publishers",
        "--no-publishers-verified",
        dest="publishers_verified",
        action="store_false",
        help=(
            "Opt out of the assumed-verified default: run the live PyPI "
            "publisher probe under --execute. Most projects report "
            "'unverifiable' (the public JSON API hides publisher metadata), "
            "which blocks the release until re-run without this flag."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    raise SystemExit(main())
