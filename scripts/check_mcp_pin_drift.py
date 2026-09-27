#!/usr/bin/env python3
"""Guard that managed MCP-server ``uvx`` pins do not silently drift.

``mcp_servers.yaml`` is the **single canonical** pin site. The bootstrap
launch specs (``DEFAULT_MCP_SERVERS``) live in the GENERATED
``_mcp_pins.py`` module rendered from it by ``scripts/mcp_pins.py sync``
(byte-equality gated by ``make mcp-pins-check``), so this guard verifies
the generated copy and the operator-facing docs against the canonical
site instead of reconciling two hand-maintained primaries. The canonical
site is not touched by ``scripts/release_prepare.py``'s dependency-floor
rewriter, so bumping a server package does not move its pin.

That gap shipped a real bug in the ``v0.1.22`` release: ``mcp-workbay-orchestrator``
was bumped ``0.5.0 -> 0.5.1`` but the pin sites kept saying ``@0.5.0``, so
package-source / default-server installs launched the superseded ``0.5.0``
wheel. A follow-up ``v0.1.23`` release was needed to correct the pins.

This module makes that drift impossible to ship silently. It verifies that
for any *managed* server (one that is pinned in the canonical site or the
generated copy) every surface agrees with the expected version:

Canonical pin site (authoritative):
  1. ``packages/workbay-system/workbay_system/payload/config/agent-workflows/mcp_servers.yaml``
     — ``mcp_servers[].args[0] == "<distribution>@<version>"``

Generated copy (must match; regenerate via ``make mcp-pins-sync``):
  2. ``packages/workbay-bootstrap/src/workbay_bootstrap/_mcp_pins.py``
     — ``DEFAULT_MCP_SERVERS[...]["args"][0] == "<distribution>@<version>"``

Coupled surfaces (also verified so a stale doc cannot ship):
  3. ``packages/workbay-system/docs/plugin-distribution.md``
     — the operator-copyable ``<distribution>@<version>`` JSON snippets.
  4. ``docs/CONSUMER.md``
     — the consumer-facing MCP-server registration table (internal).

Two entry points:

* ``check_release_bump(repo_root, package_name, new_version)`` — used by
  ``scripts/release_prepare.py`` to FAIL a managed-server bump when the new
  version does not already match every pin surface.
* ``main()`` / ``make check-mcp-pins`` — standalone steady-state check
  (usable in CI / preflight). With no ``--package`` it checks every managed
  server against its own published (``pyproject.toml``) version.

A mismatch is **reported, never auto-rewritten**: updating the canonical
pin is coupled to regenerating the copy (``make mcp-pins-sync``) and
updating the drift-guard tests and ``docs/plugin-distribution.md``, and a
blind rewrite would leave those out of sync. The operator changes all of
them deliberately.

Detection is **deliberately conservative**: each surface is scanned for *every*
``<distribution>@<version>`` occurrence (not just the load-bearing
``args[0]``), and a surface is OK only when every occurrence agrees with the
expected version. So a stale version mentioned anywhere in a primary site —
even in a comment or an unrelated snippet — is treated as drift. This is
fail-closed by design: for a release guard, loudly flagging a stray version
reference (the operator removes or updates it) is safer than parsing only one
position and missing a real second pin.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_VERSION_RE = re.compile(r'(?m)^version\s*=\s*"([^"]+)"\s*$')

# Bridge extra floor coherence (round-3 review finding
# bridge_extra_floor_coherence_unguarded): the orchestrator's ``[bridge]``
# extra is what the managed ``mcp-workbay-orchestrator[bridge]@<ver>`` uvx
# pins resolve at env-build time, but workbay-codex-bridge is deliberately
# outside stack_pins and has no ``<dist>@<ver>`` pin of its own — so nothing
# else couples the extra's floor to the bridge's published version. Without
# this check a bridge bump past the floor's upper bound would make uvx
# silently resolve the previous bridge wheel (the same silent-skew class the
# rest of this module guards for the server pins themselves).
ORCHESTRATOR_DISTRIBUTION = "mcp-workbay-orchestrator"
BRIDGE_DISTRIBUTION = "workbay-codex-bridge"
_SPECIFIER_RE = re.compile(r"^(>=|<=|==|!=|>|<)\s*(\d+\.\d+\.\d+)$")

# Coupled surfaces an operator must update in lockstep with any pin change.
# Surfaced in the failure message so the change is made deliberately rather
# than silently rewritten.
COUPLED_DRIFT_GUARDS: tuple[str, ...] = (
    "packages/workbay-bootstrap/tests/test_install.py",
    "packages/workbay-bootstrap/tests/test_bootstrap_install_rehearsal.py",
    "packages/workbay-system/tests/test_plugin_emission.py",
    "packages/workbay-system/tests/test_plugin_distribution_doc.py",
)


@dataclass(frozen=True)
class PinSurface:
    """A file that carries a ``<distribution>@<version>`` pin for a server."""

    label: str
    relpath: str
    # The canonical pin site is ``primary``; the generated bootstrap copy is
    # ``generated`` (implementation note: rendered by `make mcp-pins-sync`, never
    # hand-edited). A server is classified as "managed" by its presence in
    # the primary site or the generated copy — so deleting its canonical pin
    # while the copy still carries it is reported as drift, not silently
    # unmanaged. Doc surfaces are coupled: checked, absent-tolerated.
    primary: bool
    generated: bool = False


PIN_SURFACES: tuple[PinSurface, ...] = (
    PinSurface(
        label="mcp_servers.yaml (canonical)",
        relpath="packages/workbay-system/workbay_system/payload/config/agent-workflows/mcp_servers.yaml",
        primary=True,
    ),
    PinSurface(
        label="_mcp_pins.py (generated DEFAULT_MCP_SERVERS)",
        relpath="packages/workbay-bootstrap/src/workbay_bootstrap/_mcp_pins.py",
        primary=False,
        generated=True,
    ),
    PinSurface(
        label="plugin-distribution.md (coupled doc)",
        relpath="packages/workbay-system/docs/plugin-distribution.md",
        primary=False,
    ),
    PinSurface(
        label="CONSUMER.md (coupled doc)",
        relpath="docs/CONSUMER.md",
        primary=False,
    ),
)


@dataclass(frozen=True)
class SurfaceFinding:
    surface: PinSurface
    found_versions: tuple[str, ...]
    ok: bool
    note: str


def _pin_re(distribution: str) -> re.Pattern[str]:
    # Anchor on the literal ``<distribution>`` plus an optional PEP 508 extras
    # bracket (``[bridge]`` on the orchestrator pin) then ``@`` and a semver
    # triple. Requiring ``[`` or ``@`` immediately after the name prevents one
    # distribution matching a longer sibling, and the trailing ``(?![\w.])``
    # boundary stops a pre-release / 4-component pin (``@0.5.2rc1``,
    # ``@0.5.2.1``) from silently truncating to the base triple and passing the
    # guard — such a pin is a non-match, which check_distribution then reports
    # as "no pin" (drift) rather than a false OK.
    return re.compile(
        re.escape(distribution) + r"(?:\[[A-Za-z0-9_,.-]+\])?@(\d+\.\d+\.\d+)(?![\w.])"
    )


def versions_in_text(text: str, distribution: str) -> list[str]:
    """All distinct ``<distribution>@<version>`` versions found, in order."""
    seen: list[str] = []
    for version in _pin_re(distribution).findall(text):
        if version not in seen:
            seen.append(version)
    return seen


def _read_surface(repo_root: Path, surface: PinSurface) -> str | None:
    path = repo_root / surface.relpath
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def load_packages(repo_root: Path) -> list[dict[str, object]]:
    manifest_path = repo_root / "config" / "release" / "packages.json"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    packages = data.get("packages")
    if not isinstance(packages, list):
        raise SystemExit(f"manifest must contain a 'packages' list: {manifest_path}")
    return packages


def distribution_for_package(repo_root: Path, package_name: str) -> str | None:
    for package in load_packages(repo_root):
        if package.get("name") == package_name:
            distribution = package.get("distribution")
            return str(distribution) if distribution is not None else None
    return None


def pyproject_version_for_distribution(
    repo_root: Path, distribution: str
) -> str | None:
    for package in load_packages(repo_root):
        if package.get("distribution") != distribution:
            continue
        pyproject_path = repo_root / str(package["path"]) / "pyproject.toml"
        if not pyproject_path.is_file():
            return None
        match = PYPROJECT_VERSION_RE.search(pyproject_path.read_text(encoding="utf-8"))
        return match.group(1) if match else None
    return None


def managed_distributions(repo_root: Path) -> dict[str, str]:
    """Map ``distribution -> package name`` for every server pinned in the
    canonical site or the generated copy. Classifying via both means a pin
    deleted from one while the other still carries it stays "managed" and is
    reported as drift rather than silently dropping out of the guard."""
    dist_to_pkg = {
        str(p["distribution"]): str(p["name"])
        for p in load_packages(repo_root)
        if p.get("distribution") and p.get("name")
    }
    texts = {
        surface.label: _read_surface(repo_root, surface) for surface in PIN_SURFACES
    }
    managed: dict[str, str] = {}
    for distribution, package_name in dist_to_pkg.items():
        for surface in PIN_SURFACES:
            if not (surface.primary or surface.generated):
                continue
            text = texts[surface.label]
            if text is not None and versions_in_text(text, distribution):
                managed[distribution] = package_name
                break
    return managed


def check_distribution(
    repo_root: Path, distribution: str, expected_version: str
) -> list[SurfaceFinding]:
    findings: list[SurfaceFinding] = []
    for surface in PIN_SURFACES:
        text = _read_surface(repo_root, surface)
        if text is None:
            # The canonical site or the generated copy being absent is
            # reported (an absent generated copy means `make mcp-pins-sync`
            # never ran); an absent coupled doc is tolerated (some checkouts
            # may not ship it).
            required = surface.primary or surface.generated
            findings.append(
                SurfaceFinding(
                    surface=surface,
                    found_versions=(),
                    ok=not required,
                    note="file not found" if required else "absent (skipped)",
                )
            )
            continue
        versions = versions_in_text(text, distribution)
        if not versions:
            findings.append(
                SurfaceFinding(
                    surface=surface,
                    found_versions=(),
                    ok=False,
                    note=f"no pin for {distribution} (expected {expected_version})",
                )
            )
        elif set(versions) != {expected_version}:
            findings.append(
                SurfaceFinding(
                    surface=surface,
                    found_versions=tuple(versions),
                    ok=False,
                    note=f"pins {', '.join(versions)} (expected {expected_version}) [STALE]",
                )
            )
        else:
            findings.append(
                SurfaceFinding(
                    surface=surface,
                    found_versions=tuple(versions),
                    ok=True,
                    note=f"pins {expected_version}",
                )
            )
    return findings


def _format_drift(
    distribution: str, expected_version: str, findings: list[SurfaceFinding]
) -> list[str]:
    lines = [f"MCP server pin drift for {distribution} (expected {expected_version}):"]
    for finding in findings:
        marker = "ok " if finding.ok else "DRIFT"
        lines.append(
            f"  [{marker}] {finding.surface.label} "
            f"({finding.surface.relpath}): {finding.note}"
        )
    return lines


def _coupling_reminder(expected_version: str) -> list[str]:
    lines = [
        "",
        "Pins are intentionally NOT auto-rewritten by this guard. Update the "
        f"canonical mcp_servers.yaml pin to {expected_version}, regenerate the "
        "bootstrap copy with `make mcp-pins-sync`, then update the coupled "
        "drift-guard tests + docs in the same change so they do not fall out "
        "of sync:",
    ]
    lines.extend(f"  - {path}" for path in COUPLED_DRIFT_GUARDS)
    return lines


def check_release_bump(
    repo_root: Path, package_name: str, new_version: str
) -> tuple[bool, list[str]]:
    """Gate a release bump: if ``package_name`` is a managed MCP server, every
    pin surface must already agree with ``new_version``.

    Returns ``(ok, messages)``. Non-server packages are a no-op (``True, []``)
    so this can be called unconditionally for any release bump.
    """
    distribution = distribution_for_package(repo_root, package_name)
    if distribution is None:
        return True, []
    # A bridge bump must stay inside the orchestrator's [bridge] extra floor,
    # or the managed `...[bridge]@<ver>` uvx pins silently resolve the
    # previous bridge wheel. The bridge has no @<ver> pin of its own, so this
    # is its only release gate here.
    if distribution == BRIDGE_DISTRIBUTION:
        bridge_ok, bridge_messages = check_bridge_extra_floor(
            repo_root, bridge_version=new_version
        )
        if bridge_ok:
            return True, []
        return False, [
            f"Refusing to bump {package_name} to {new_version}: ",
            *bridge_messages,
        ]
    if distribution not in managed_distributions(repo_root):
        return True, []

    findings = check_distribution(repo_root, distribution, new_version)
    if all(finding.ok for finding in findings):
        return True, []

    messages = [
        f"Refusing to bump {package_name} to {new_version}: managed MCP-server "
        "pins are stale.",
        *_format_drift(distribution, new_version, findings),
        *_coupling_reminder(new_version),
    ]
    return False, messages


def _bridge_floor_requirement(repo_root: Path) -> str | None:
    """The orchestrator's ``[bridge]`` extra requirement on the bridge, or
    ``None`` when the orchestrator (or the extra) is absent — synthetic repos
    without a bridge simply skip the coherence check."""
    for package in load_packages(repo_root):
        if package.get("distribution") != ORCHESTRATOR_DISTRIBUTION:
            continue
        pyproject_path = repo_root / str(package["path"]) / "pyproject.toml"
        if not pyproject_path.is_file():
            return None
        try:
            payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError:
            return None
        extras = (payload.get("project") or {}).get("optional-dependencies") or {}
        bridge_reqs = extras.get("bridge") if isinstance(extras, dict) else None
        if not isinstance(bridge_reqs, list):
            return None
        for requirement in bridge_reqs:
            if isinstance(requirement, str) and requirement.startswith(
                BRIDGE_DISTRIBUTION
            ):
                return requirement
        return None
    return None


def _version_triple(version: str) -> tuple[int, int, int] | None:
    parts = version.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        return None
    major, minor, patch = (int(part) for part in parts)
    return (major, minor, patch)


def _floor_admits(specifiers: str, version: str) -> bool | None:
    """Evaluate a comma-joined specifier set against a semver triple.

    Deliberately supports only the simple operator set the repo actually uses
    (``>=``, ``<=``, ``==``, ``!=``, ``>``, ``<`` with three-component
    versions); anything else returns ``None`` so the caller fails closed with
    an actionable message instead of guessing PEP 440 semantics."""
    candidate = _version_triple(version)
    if candidate is None:
        return None
    for clause in specifiers.split(","):
        clause = clause.strip()
        if not clause:
            continue
        match = _SPECIFIER_RE.match(clause)
        if match is None:
            return None
        operator, bound_text = match.groups()
        bound = _version_triple(bound_text)
        if bound is None:
            return None
        outcome = {
            ">=": candidate >= bound,
            "<=": candidate <= bound,
            "==": candidate == bound,
            "!=": candidate != bound,
            ">": candidate > bound,
            "<": candidate < bound,
        }[operator]
        if not outcome:
            return False
    return True


def check_bridge_extra_floor(
    repo_root: Path, bridge_version: str | None = None
) -> tuple[bool, list[str]]:
    """Verify the orchestrator's ``[bridge]`` extra floor brackets the bridge
    version (the repo's ``pyproject.toml`` version, or an explicit
    release-candidate version). Repos without the extra skip cleanly."""
    requirement = _bridge_floor_requirement(repo_root)
    if requirement is None:
        return True, [
            "no orchestrator [bridge] extra found; bridge floor check skipped"
        ]
    specifiers = requirement[len(BRIDGE_DISTRIBUTION) :].strip()
    if bridge_version is None:
        bridge_version = pyproject_version_for_distribution(
            repo_root, BRIDGE_DISTRIBUTION
        )
    if bridge_version is None:
        return False, [
            f"orchestrator [bridge] extra declares '{requirement}' but the "
            f"{BRIDGE_DISTRIBUTION} version could not be resolved from the "
            "release manifest / pyproject.toml"
        ]
    admitted = _floor_admits(specifiers, bridge_version)
    if admitted is None:
        return False, [
            f"orchestrator [bridge] extra floor '{specifiers}' (or version "
            f"'{bridge_version}') is not in the supported simple-specifier "
            "form; update check_mcp_pin_drift._floor_admits alongside the "
            "requirement"
        ]
    if not admitted:
        return False, [
            f"bridge floor drift: orchestrator [bridge] extra "
            f"'{requirement}' excludes {BRIDGE_DISTRIBUTION} {bridge_version} "
            "— uvx would silently resolve a stale bridge wheel. Update the "
            "extra floor in packages/mcp-workbay-orchestrator/pyproject.toml "
            "in the same change as the bridge bump."
        ]
    return True, [
        f"ok: orchestrator [bridge] extra '{requirement}' admits "
        f"{BRIDGE_DISTRIBUTION} {bridge_version}"
    ]


def check_all(
    repo_root: Path, package_name: str | None = None
) -> tuple[bool, list[str]]:
    """Steady-state check: every managed server's pins must agree with its own
    published (``pyproject.toml``) version. Optionally scope to one package.

    Returns ``(ok, messages)``; ``messages`` always carries a one-line status
    summary even on success.
    """
    managed = managed_distributions(repo_root)

    if package_name is not None:
        distribution = distribution_for_package(repo_root, package_name)
        if distribution is None:
            return False, [f"unknown package in release manifest: {package_name}"]
        if distribution not in managed:
            return True, [
                f"{package_name} ({distribution}) is not a managed MCP server; "
                "nothing to check."
            ]
        managed = {distribution: package_name}

    if not managed:
        return True, ["no managed MCP servers found to check"]

    ok = True
    messages: list[str] = []
    for distribution in sorted(managed):
        expected = pyproject_version_for_distribution(repo_root, distribution)
        if expected is None:
            ok = False
            messages.append(
                f"could not resolve published version for {distribution} "
                "(missing pyproject.toml or version field)"
            )
            continue
        findings = check_distribution(repo_root, distribution, expected)
        if all(finding.ok for finding in findings):
            messages.append(
                f"ok: {distribution} pinned at {expected} across all surfaces"
            )
            continue
        ok = False
        messages.extend(_format_drift(distribution, expected, findings))
        messages.extend(_coupling_reminder(expected))

    # Bridge extra floor coherence rides the steady-state check (and any run
    # scoped to the orchestrator): the floor lives in the orchestrator's
    # pyproject, not in a <dist>@<ver> pin site, so the surface scan above
    # cannot see it.
    if package_name is None or ORCHESTRATOR_DISTRIBUTION in managed:
        bridge_ok, bridge_messages = check_bridge_extra_floor(repo_root)
        ok = ok and bridge_ok
        messages.extend(bridge_messages)

    return ok, messages


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify managed MCP-server uvx pins agree across both pin sites "
            "and the published version."
        )
    )
    parser.add_argument(
        "--package",
        default=None,
        help="Limit the check to a single managed-server package name.",
    )
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root (defaults to the monorepo containing this script).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    ok, messages = check_all(repo_root, package_name=args.package)
    stream = sys.stdout if ok else sys.stderr
    for line in messages:
        print(line, file=stream)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
