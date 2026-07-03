"""Installed hook-surface coherence assessment (internal).

Pure/offline module: ``assess_hook_coherence(target)`` inspects an installed
overlay target and returns findings for the four gate families:

- **resolve-every-script** — every ``command`` path named by a rendered hook
  config must stat through the live symlink chain exactly the way the harness
  resolves it. A config naming a deleted/moved guard script is the incident
  class (internal fallout: stale clone config invoking the
  payload-deleted ``terminal-guard.py`` fail-closed an entire Copilot
  session). ERROR.
- **same-snapshot provenance** — the config-bearing and script-bearing
  surfaces (and every other ``SHARED_SURFACES`` mount) must resolve to the
  same snapshot. Mixed mounts (one surface → stale ``.workbay/remote``
  clone, another → live payload) are how the incident substrate formed.
  ERROR for the two hook surfaces, WARN for non-hook shared surfaces (they
  degrade behavior — e.g. a stale ``Makefile.d`` recipe — but cannot
  fail-close a harness).
- **stale-clone** — a surface symlinks into ``.workbay/remote`` whose HEAD
  no longer matches the receipt ``remote_sha``. WARN (offline-only by
  design: a long-lagging clone whose configs still resolve only warns).
- **orphan / hybrid-receipt** — an on-disk hook config absent from BOTH the
  receipt (``surfaces[]``/``configs[]``) and the renderer's expected-output
  map is unmanaged: update()/doctor will never refresh or flag it (the
  consumer committed-``.codex/hooks.json`` class). WARN. A
  ``source_kind=package`` receipt whose surfaces nevertheless symlink into a
  ``.workbay/remote`` clone is the hybrid incident substrate. WARN.

No network access anywhere; every probe is local stat/readlink/git-plumbing.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .install import BOOTSTRAP_MANIFEST_NAME, CLONE_SUBDIR, SHARED_SURFACES

# The two surfaces whose skew can fail-close a harness: hook configs live in
# one, the scripts they invoke in the other.
HOOK_SURFACES: tuple[str, ...] = (".github/hooks", "scripts/hooks")

# Where rendered hook configs are looked for on disk when the renderer seam
# is unavailable. This is a *search* list (enumeration for the resolve gate),
# NOT a managed-set assertion — orphan classification only consults the
# receipt and the renderer's expected-output map.
HOOK_CONFIG_CANDIDATE_GLOBS: tuple[str, ...] = (
    ".github/hooks/*.json",
    ".codex/hooks.json",
    # Claude plugin-tree guard channel (implementation note plan-review pin): every
    # hooks-bearing config the generator materializes under the generated
    # plugin tree.
    ".workbay/generated/plugins/**/hooks*.json",
)

# Tokens we strip before treating command words as candidate paths.
_INTERPRETERS = frozenset({"python", "python3", "bash", "sh", "uv", "uvx"})
# Workspace-root env anchors: $CLAUDE_PROJECT_DIR/, ${GROK_WORKSPACE_ROOT}/,
# and any sibling a future harness introduces — all substitute to the
# workspace root, i.e. the assessment target.
_ENV_ANCHOR_RE = re.compile(r"^\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/")


def _clone_dir(target: Path) -> Path:
    """``<target>/.workbay/remote`` (single-sourced from install.CLONE_SUBDIR)."""
    return target.joinpath(*CLONE_SUBDIR)


_CLONE_RELPATH = "/".join(CLONE_SUBDIR)


@dataclass(frozen=True)
class CoherenceFinding:
    """One coherence gate outcome.

    ``kind`` is one of ``hook_command_unresolved`` / ``hook_surface_skew`` /
    ``clone_stale`` / ``orphan_hook_config`` / ``hybrid_receipt``.
    ``severity`` is ``error`` or ``warning``. ``path`` is target-relative
    where possible. ``detail`` is the human remediation line.
    """

    kind: str
    severity: str
    path: str
    detail: str

    def as_doctor_finding(self) -> dict[str, str]:
        """Project onto the ``subcommands.Finding`` dict shape (implementation note)."""
        return {
            "kind": self.kind,
            "severity": self.severity,
            "path": self.path,
            "detail": self.detail,
        }


def assess_hook_coherence(
    target: Path,
    *,
    package_root: Path | None = None,
    receipt: dict[str, object] | None = None,
) -> list[CoherenceFinding]:
    """Assess installed hook-surface coherence at ``target``.

    ``package_root`` overrides the workbay-system payload root used for
    package-mode content bucketing (tests / pinned installs), mirroring
    ``install._package_source_root``.
    """
    target = Path(target).resolve()
    receipt = receipt if receipt is not None else _load_receipt(target)
    findings: list[CoherenceFinding] = []

    configs = _enumerate_hook_configs(target)
    known = _receipt_known_paths(target, receipt)

    for config in configs:
        rel = config.relative_to(target).as_posix()
        findings.extend(_resolve_every_script(target, config, rel))
        if not _is_receipt_known(rel, known):
            findings.append(
                CoherenceFinding(
                    kind="orphan_hook_config",
                    severity="warning",
                    path=rel,
                    detail=(
                        "hook config exists on disk but is unknown to the "
                        "install receipt and the renderer's expected outputs; "
                        "update()/doctor will never refresh or flag it once "
                        "it goes stale. Adopt it into a managed surface or "
                        "delete it."
                    ),
                )
            )

    findings.extend(_same_snapshot_gate(target, receipt, package_root))
    findings.extend(_stale_clone_gate(target, receipt))
    findings.extend(_hybrid_receipt_gate(target, receipt))
    return findings


# --- enumeration -----------------------------------------------------------


def _enumerate_hook_configs(target: Path) -> list[Path]:
    """Rendered hook configs to assess, discovered on disk.

    Primary source SHOULD be the internal ``_expected_hooks_outputs``
    renderer seam so the list stays single-sourced with generation; that seam
    is consulted by ``_expected_renderer_outputs`` below when the installed
    payload carries it. Discovery must additionally sweep the candidate
    globs regardless: coherence runs against already-installed targets whose
    payload may predate the seam, and the orphan gate exists precisely to
    catch configs NO source claims.
    """
    found: dict[Path, None] = {}
    for pattern in HOOK_CONFIG_CANDIDATE_GLOBS:
        for hit in sorted(target.glob(pattern)):
            if hit.is_file():
                found[hit] = None
    for rel in _expected_renderer_outputs(target):
        candidate = target / rel
        if candidate.is_file():
            found[candidate] = None
    return list(found)


def _expected_renderer_outputs(target: Path) -> list[str]:
    """Target-relative hook-config paths the installed renderer expects.

    Probes the internal ``_expected_hooks_outputs`` seam in the
    target's installed ``scripts/generate_agent_workflows.py``. Returns
    ``[]`` when the script or seam is absent (pre-seam payloads) or the
    probe fails for any reason — enumeration then rests on the candidate
    globs alone. Best-effort by design; never raises.
    """
    script = target / "scripts" / "generate_agent_workflows.py"
    if not script.is_file():
        return []
    probe = (
        "import importlib.util, json, sys\n"
        f"spec = importlib.util.spec_from_file_location('gaw', {str(script)!r})\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
        "fn = getattr(mod, '_expected_hooks_outputs', None)\n"
        "if fn is None:\n"
        "    print('[]')\n"
        "else:\n"
        f"    out = fn({str(target)!r})\n"
        "    print(json.dumps(sorted(str(k) for k in out)))\n"
    )
    from workbay_bootstrap.external import run_external

    try:
        proc = run_external(
            ["python3", "-c", probe],
            call_class="probe",
            check=True,
            capture_output=True,
            text=True,
        )
        return [str(p) for p in json.loads(proc.stdout or "[]")]
    except Exception:  # noqa: BLE001 — best-effort seam probe
        return []


def _load_receipt(target: Path) -> dict[str, object]:
    manifest_path = target / BOOTSTRAP_MANIFEST_NAME
    try:
        return json.loads(manifest_path.read_text())
    except (OSError, ValueError):
        return {}


def _receipt_known_paths(target: Path, receipt: dict[str, object]) -> set[str]:
    known: set[str] = set()
    for key in ("surfaces", "configs"):
        entries = receipt.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict) and entry.get("path"):
                known.add(str(entry["path"]))
    known.update(_expected_renderer_outputs(target))
    return known


def _is_receipt_known(rel: str, known: set[str]) -> bool:
    if rel in known:
        return True
    # A config under a receipt-managed surface directory is managed by that
    # surface (e.g. .github/hooks/terminal-guard.json under .github/hooks).
    return any(rel.startswith(prefix + "/") for prefix in known)


# --- resolve-every-script gate ---------------------------------------------


def _resolve_every_script(
    target: Path, config: Path, rel: str
) -> list[CoherenceFinding]:
    try:
        data = json.loads(config.read_text())
    except (OSError, ValueError) as exc:
        return [
            CoherenceFinding(
                kind="hook_command_unresolved",
                severity="error",
                path=rel,
                detail=f"hook config unreadable as JSON: {exc}",
            )
        ]
    findings: list[CoherenceFinding] = []
    for command in _iter_command_strings(data):
        for candidate in _command_path_tokens(command):
            resolved = (target / candidate).resolve()
            if not resolved.exists():
                findings.append(
                    CoherenceFinding(
                        kind="hook_command_unresolved",
                        severity="error",
                        path=rel,
                        detail=(
                            f"command {command!r} names {candidate!r} which "
                            f"does not resolve from the target "
                            f"(checked {resolved}); the harness will "
                            f"errno-2 this hook. Regenerate the config or "
                            f"restore the script."
                        ),
                    )
                )
    return findings


def _iter_command_strings(node: object) -> list[str]:
    """Every ``"command": "<str>"`` value anywhere in the config (flat VS
    Code shape and nested Claude ``hooks[]`` shape alike)."""
    commands: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "command" and isinstance(value, str):
                commands.append(value)
            else:
                commands.extend(_iter_command_strings(value))
    elif isinstance(node, list):
        for item in node:
            commands.extend(_iter_command_strings(item))
    return commands


def _command_path_tokens(command: str) -> list[str]:
    """Path-like tokens of ``command`` to resolve against the target.

    Strips interpreter words and workspace-root env anchors
    (``$CLAUDE_PROJECT_DIR``, ``${GROK_WORKSPACE_ROOT}``, …) — every harness
    substitutes such anchors with the workspace root, which IS the target we
    resolve against. Every remaining non-flag token that looks like a path is
    resolved — including arguments after the first script, so the implementation note
    two-path wrapper form (``_run_guard.py <handler-relpath>``) gets BOTH its
    wrapper and its handler checked.
    """
    try:
        words = shlex.split(command)
    except ValueError:
        return []
    tokens: list[str] = []
    for word in words:
        word = _ENV_ANCHOR_RE.sub("", word)
        if word in _INTERPRETERS or word.startswith("-"):
            continue
        if "/" in word or word.endswith((".py", ".sh")):
            tokens.append(word)
    return tokens


# --- same-snapshot provenance gate ------------------------------------------


def _comparable(a: str, b: str) -> bool:
    """Whether two provenance keys carry the same KIND of evidence.

    Mount identities (``clone:``/``link:`` — where a symlink actually
    resolves) are comparable with each other: differing mounts are the
    incident substrate. Content buckets (``package:`` — a copied surface
    matching the installed payload) are comparable with each other. A mount
    key vs a content bucket is inconclusive — a git_overlay install
    legitimately mixes clone symlinks with carved per-file copies of the
    SAME snapshot, and content cannot prove mount divergence.
    """
    mount = ("clone:", "link:")
    a_mount = a.startswith(mount)
    b_mount = b.startswith(mount)
    return a_mount == b_mount


def _receipt_local_surfaces(receipt: dict[str, object]) -> set[str]:
    """Surface paths the receipt classifies as operator-owned (``source='local'``).

    Install deliberately preserves foreign symlinks (raw target outside
    ``.workbay/remote``) and records them as ``local`` — the operator owns
    that mount, so cross-surface snapshot divergence is a WARN, never an
    install-aborting ERROR.
    """
    local: set[str] = set()
    entries = receipt.get("surfaces")
    if not isinstance(entries, list):
        return local
    for entry in entries:
        if (
            isinstance(entry, dict)
            and entry.get("source") == "local"
            and entry.get("path")
        ):
            local.add(str(entry["path"]))
    return local


def _same_snapshot_gate(
    target: Path, receipt: dict[str, object], package_root: Path | None
) -> list[CoherenceFinding]:
    keys: dict[str, str | None] = {}
    for surface in SHARED_SURFACES:
        path = target / surface
        if path.exists() or path.is_symlink():
            keys[surface] = _provenance_key(target, path, package_root)

    local_owned = _receipt_local_surfaces(receipt)
    present_hooks = [s for s in HOOK_SURFACES if s in keys]
    managed_hooks = [s for s in present_hooks if s not in local_owned]
    findings: list[CoherenceFinding] = []

    hook_keys = {s: keys[s] for s in managed_hooks}
    derived = [k for k in hook_keys.values() if k is not None]
    distinct_hook = set(derived)
    if len(distinct_hook) > 1 and _comparable(*sorted(distinct_hook)[:2]):
        findings.append(
            CoherenceFinding(
                kind="hook_surface_skew",
                severity="error",
                path=", ".join(managed_hooks),
                detail=(
                    "config-bearing and script-bearing hook surfaces resolve "
                    f"to different snapshots: {hook_keys}. A config in one "
                    "can name a script deleted from the other (the "
                    "terminal-guard incident). Repoint both surfaces at the "
                    "same snapshot."
                ),
            )
        )

    # Operator-owned (receipt source='local') hook surfaces: divergence from
    # the managed snapshot is deliberate — the operator mounted their own
    # content — so it can never abort install/update. Still worth a WARN:
    # managed configs naming scripts inside an operator mount (or vice versa)
    # re-create the incident shape under operator ownership.
    if len(distinct_hook) == 1:
        reference = next(iter(distinct_hook))
        for surface in present_hooks:
            if surface in managed_hooks:
                continue
            key = keys.get(surface)
            if key is not None and key != reference and _comparable(key, reference):
                findings.append(
                    CoherenceFinding(
                        kind="hook_surface_skew",
                        severity="warning",
                        path=surface,
                        detail=(
                            "receipt classifies this hook surface as "
                            f"operator-owned (source='local'); it resolves to "
                            f"snapshot {key!r} while the managed hook surfaces "
                            f"resolve to {reference!r}. Hook configs in managed "
                            "surfaces must not name scripts inside this mount "
                            "(and vice versa) or a deletion on either side "
                            "fail-closes the harness."
                        ),
                    )
                )

    # Non-hook shared surfaces: compare against the hook surfaces' snapshot
    # (when unanimous). Skew degrades behavior (stale Makefile.d recipes,
    # stale contracts) but cannot fail-close a harness — WARN.
    if len(distinct_hook) == 1:
        reference = next(iter(distinct_hook))
        for surface, key in keys.items():
            if surface in HOOK_SURFACES or key is None:
                continue
            if key != reference and _comparable(key, reference):
                findings.append(
                    CoherenceFinding(
                        kind="hook_surface_skew",
                        severity="warning",
                        path=surface,
                        detail=(
                            f"shared surface resolves to snapshot {key!r} "
                            f"while the hook surfaces resolve to "
                            f"{reference!r}; mixed-provenance mounts serve "
                            "stale content (e.g. a stale Makefile.d recipe) "
                            "even when hooks stay coherent."
                        ),
                    )
                )
    return findings


def _provenance_key(target: Path, path: Path, package_root: Path | None) -> str | None:
    """Snapshot identity for one mounted surface.

    - symlink into ``.workbay/remote`` → ``clone:<HEAD sha>``
    - symlink elsewhere → ``link:<snapshot root>`` — the resolved path with
      the surface's own relpath suffix stripped, so two surfaces mounted
      from one payload root (``payload/.github/hooks`` and
      ``payload/scripts/hooks``) share one key (e.g. the self-host tracked
      in-tree payload symlink — implementation note S3)
    - real dir/file under package mode → content-hash bucket against the
      installed package payload (implementation note machinery): ``package:<version>``
      when identical, else ``content:<digest>``
    - ``None`` when no key can be derived (caller skips, never guesses)
    """
    remote_root = _clone_dir(target).resolve()
    if path.is_symlink():
        resolved = path.resolve()
        if str(resolved).startswith(str(remote_root) + "/") or resolved == remote_root:
            head = _clone_head(target)
            return f"clone:{head}" if head else f"clone:{remote_root}"
        rel = path.relative_to(target).as_posix()
        text = str(resolved)
        if text.endswith("/" + rel):
            text = text[: -(len(rel) + 1)]
        return f"link:{text}"
    if not path.exists():
        return None
    return _content_bucket(target, path, package_root)


def _content_bucket(target: Path, path: Path, package_root: Path | None) -> str | None:
    """``package:<version>`` when the copied surface matches the installed
    package payload byte-for-byte (implementation note ``_hash_local_files`` machinery),
    else ``None``.

    Deliberately NOT a raw content digest: two different surfaces always hold
    different files, so digest comparison across surfaces would fabricate
    skew for every consumer-modified copy. Unmatched content is inconclusive
    — the caller skips it rather than guessing.
    """
    from .install import _package_source_root, _package_version
    from .subcommands import _hash_local_files

    local_index = _hash_local_files(path)
    if local_index is None:
        return None
    try:
        source_root = _package_source_root(package_root)
    except FileNotFoundError:
        return None
    rel = path.relative_to(target).as_posix()
    payload_equiv = source_root / rel
    if payload_equiv.exists():
        payload_index = _hash_local_files(payload_equiv)
        if payload_index is not None and payload_index == local_index:
            return f"package:{_package_version(source_root)}"
    return None


def _clone_head(target: Path) -> str | None:
    clone = _clone_dir(target)
    if not (clone / ".git").exists():
        return None
    from workbay_bootstrap.external import run_external

    try:
        return run_external(
            ["git", "-C", str(clone), "rev-parse", "HEAD"],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


# --- stale-clone + hybrid-receipt gates --------------------------------------


def _surfaces_into_clone(target: Path, receipt: dict[str, object]) -> list[str]:
    remote_root = _clone_dir(target).resolve()
    into: list[str] = []
    entries = receipt.get("surfaces")
    if not isinstance(entries, list):
        return into
    for entry in entries:
        if not (isinstance(entry, dict) and entry.get("path")):
            continue
        surface = target / str(entry["path"])
        if surface.is_symlink():
            resolved = surface.resolve()
            if str(resolved).startswith(str(remote_root) + "/"):
                into.append(str(entry["path"]))
    return into


def _stale_clone_gate(
    target: Path, receipt: dict[str, object]
) -> list[CoherenceFinding]:
    recorded = receipt.get("remote_sha")
    if not recorded:
        return []
    linked = _surfaces_into_clone(target, receipt)
    if not linked:
        return []
    head = _clone_head(target)
    if head and head != recorded:
        return [
            CoherenceFinding(
                kind="clone_stale",
                severity="warning",
                path=_CLONE_RELPATH,
                detail=(
                    f"clone HEAD {head[:12]} != receipt remote_sha "
                    f"{str(recorded)[:12]} while surfaces "
                    f"{linked} symlink into it; rerun update to refresh "
                    "(offline check — nothing was fetched)."
                ),
            )
        ]
    return []


def _hybrid_receipt_gate(
    target: Path, receipt: dict[str, object]
) -> list[CoherenceFinding]:
    if str(receipt.get("source_kind") or "git_overlay") != "package":
        return []
    linked = _surfaces_into_clone(target, receipt)
    if not linked:
        return []
    return [
        CoherenceFinding(
            kind="hybrid_receipt",
            severity="warning",
            path=", ".join(linked),
            detail=(
                "receipt says source_kind=package but these surfaces still "
                "symlink into the .workbay/remote clone — the mixed-"
                "provenance substrate of the terminal-guard incident. "
                "Reinstall so package mode owns every surface, or re-adopt "
                "as git_overlay."
            ),
        )
    ]


# --- CLI gate entry point (make check-harness-coherence) ---------------------


def main(argv: list[str] | None = None) -> int:
    """Gate entry point: ``python -m workbay_bootstrap.coherence [target]``.

    Prints every finding; exits 1 when any ``error``-severity finding exists
    (warnings alone stay green so the offline-only stale-clone / hybrid rows
    never block CI — implementation note severity contract).
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Assess installed hook-surface coherence."
    )
    parser.add_argument(
        "target",
        nargs="?",
        default=".",
        help="overlay target to assess (default: cwd)",
    )
    args = parser.parse_args(argv)

    findings = assess_hook_coherence(Path(args.target))
    for finding in findings:
        print(
            f"{finding.severity.upper():7s} {finding.kind} [{finding.path}] "
            f"{finding.detail}"
        )
    errors = [f for f in findings if f.severity == "error"]
    if errors:
        print(f"check-harness-coherence: {len(errors)} error(s)")
        return 1
    print(
        "check-harness-coherence: coherent"
        + (f" ({len(findings)} warning(s))" if findings else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
