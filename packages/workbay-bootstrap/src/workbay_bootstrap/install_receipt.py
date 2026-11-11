"""Install step receipts and pre-flight (implementation note S6)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

GIT_WRITE_SIDECAR_REL = Path(".workbay") / "git-writes.json"
INSTALL_RECEIPT_REL = Path(".workbay") / "install-receipt.json"

StepStatus = Literal["ok", "failed", "deferred", "skipped"]
FailureClass = Literal["system", "application"] | None


@dataclass
class TrackedFileSnapshot:
    status: str
    dirty: set[str] = field(default_factory=set)
    head_blobs: dict[str, str] = field(default_factory=dict)
    content_blobs: dict[str, str | None] = field(default_factory=dict)
    reason: str | None = None


def _receipt_git(target: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    from workbay_bootstrap.external import run_external

    return run_external(["git", *args], cwd=target, call_class="git", check=check, capture_output=True)


def snapshot_tracked_files(target: Path) -> TrackedFileSnapshot:
    """Read Git state without staging, refreshing the index, or writing blobs."""
    try:
        probe = _receipt_git(target, "rev-parse", "--show-toplevel", check=False)
        if probe.returncode:
            return TrackedFileSnapshot(status="not_a_git_checkout")
        root = Path(os.fsdecode(probe.stdout).rstrip("\n"))
        status = _receipt_git(root, "--no-optional-locks", "status", "--porcelain=v1", "-z", "--untracked-files=no")
        records = iter(status.stdout.split(b"\0"))
        dirty: set[str] = set()
        for record in records:
            if not record:
                continue
            dirty.add(os.fsdecode(record[3:]))
            if b"R" in record[:2] or b"C" in record[:2]:
                dirty.add(os.fsdecode(next(records)))
        # An unborn branch has no HEAD blobs; staged files are already dirty.
        head = _receipt_git(root, "rev-parse", "--verify", "HEAD", check=False)
        entries = _receipt_git(root, "ls-tree", "-rz", "HEAD").stdout if head.returncode == 0 else b""
        blobs: dict[str, str] = {}
        modes: dict[str, bytes] = {}
        for entry in entries.split(b"\0"):
            if not entry:
                continue
            metadata, path = entry.split(b"\t", 1)
            mode, _, oid = metadata.split()
            name = os.fsdecode(path)
            blobs[name] = oid.decode("ascii")
            modes[name] = mode
        content: dict[str, str | None] = {}
        for path in dirty:
            dest = root / path
            if dest.is_symlink():
                data = os.fsencode(os.readlink(dest))
                algorithm = "sha256" if len(blobs.get(path, "")) == 64 else "sha1"
                content[path] = hashlib.new(algorithm, b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            elif not dest.exists():
                content[path] = None
            elif modes.get(path) == b"160000":
                content[path] = _receipt_git(dest, "rev-parse", "HEAD").stdout.decode("ascii").strip()
            elif dest.is_dir():
                # A carved directory can replace a formerly tracked file or
                # symlink. The old blob is gone; its children are untracked.
                content[path] = None
            else:
                # Git applies the path's clean filters, so CRLF/check-out
                # conversions and executable-bit-only changes are not content.
                content[path] = (
                    _receipt_git(root, "hash-object", f"--path={path}", "--", path).stdout.decode("ascii").strip()
                )
        return TrackedFileSnapshot(status="ok", dirty=dirty, head_blobs=blobs, content_blobs=content)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        return TrackedFileSnapshot(status="unavailable", reason=f"{type(exc).__name__}: {exc}")


# PATHS crosses the operator's shell, Make's `--paths '$(PATHS)'` expansion and
# slice-commit's shlex.split. These cannot cross all three verbatim.
_FACADE_UNSAFE = {"'": "a single quote", '"': "a double quote", "\\": "a backslash", "$": "a dollar sign"}


def _lifecycle_paths(paths: list[str]) -> tuple[str | None, str | None]:
    """Return a PATHS value that slice-commit splits back into ``paths``, or why none exists."""
    for path in paths:
        try:
            path.encode("utf-8")
        except UnicodeEncodeError:
            return None, "a reported path is not valid UTF-8"
        for ch in path:
            if ch in _FACADE_UNSAFE:
                return None, f"a reported path contains {_FACADE_UNSAFE[ch]}"
            if unicodedata.category(ch) == "Cc":
                return None, "a reported path contains a control character"
    # Double quotes keep spaces inside one shlex token, and a leading quote
    # stops argparse reading a leading-dash value as an option.
    return " ".join(f'"{path}"' if " " in path or path.startswith("-") else path for path in paths), None


@dataclass
class TrackedChangeReport:
    status: str
    files: list[dict[str, str]] = field(default_factory=list)
    commit_recipe: dict[str, Any] | None = None
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        result = {"status": self.status, "files": self.files, "commit_recipe": self.commit_recipe}
        if self.reason is not None:
            result["reason"] = self.reason
        return result

    @classmethod
    def between(cls, target: Path, before: TrackedFileSnapshot, after: TrackedFileSnapshot) -> TrackedChangeReport:
        from workbay_bootstrap.surfaces import SURFACE_PROVENANCE

        for snapshot in (before, after):
            if snapshot.status != "ok":
                return cls(status=snapshot.status, reason=snapshot.reason)
        candidates = after.dirty | {
            path
            for path in before.head_blobs.keys() | after.head_blobs.keys()
            if before.head_blobs.get(path) != after.head_blobs.get(path)
        }
        files = []
        for path in sorted(candidates - before.dirty):
            current = after.content_blobs.get(path) if path in after.dirty else after.head_blobs.get(path)
            if before.head_blobs.get(path) == current:
                continue
            kind = "unclassified"
            for surface in sorted(SURFACE_PROVENANCE, key=len, reverse=True):
                if path == surface or path.startswith(surface + "/"):
                    kind = SURFACE_PROVENANCE[surface]
                    break
            if path == ".workbay-bootstrap.json":
                kind = "bootstrap_ledger"
            files.append({"path": path, "kind": kind})
        if not files:
            return cls(status="no_changes")
        try:
            facade = (target / "Makefile.d/lifecycle.mk").read_text()
        except OSError:
            facade = ""
        lifecycle_paths, fallback_reason = None, None
        if re.search(r"^maint-start\s*:(?!=)", facade, re.MULTILINE):
            # Without PATHS, slice-commit stages every tracked change (git add -u).
            lifecycle_paths, fallback_reason = _lifecycle_paths([entry["path"] for entry in files])
        if lifecycle_paths is not None:
            recipe = {
                "kind": "lifecycle",
                "commands": [
                    'make maint-start TASK=<ref> OBJECTIVE="..."',
                    f'make slice-commit TASK=<ref> MSG="..." PATHS={shlex.quote(lifecycle_paths)}',
                    "make plan-done TASK=<ref>",
                ],
            }
        else:
            paths = " ".join(shlex.quote(entry["path"]) for entry in files)
            # Literal pathspecs also protect names containing Git glob syntax.
            # Ordinary names keep the familiar, copyable git command form.
            prefix = (
                "git --literal-pathspecs" if any(ch in entry["path"] for entry in files for ch in "*?[:") else "git"
            )
            recipe = {
                "kind": "git",
                "commands": [
                    f"{prefix} add -- {paths}",
                    f'{prefix} commit -m "..." -- {paths}',
                ],
            }
            if fallback_reason is not None:
                recipe["fallback_reason"] = f"{fallback_reason}, which the lifecycle PATHS cannot carry losslessly"
        return cls(status="changed", files=files, commit_recipe=recipe)

    def print_block(self) -> None:
        if self.status == "unavailable":
            print(f"warning: tracked change report unavailable: {self.reason}", file=sys.stderr)
        if not self.files:
            return
        print("Tracked files changed by WorkBay:")
        for kind in dict.fromkeys(entry["kind"] for entry in self.files):
            print(f"  {kind}:")
            for entry in self.files:
                if entry["kind"] == kind:
                    print(f"    {entry['path']}")
        print("Commit from the target repository (review first):")
        if (self.commit_recipe or {}).get("fallback_reason"):
            print(f"  # git, not make slice-commit: {self.commit_recipe['fallback_reason']}")
        for command in (self.commit_recipe or {}).get("commands", []):
            print(f"  {command}")


@dataclass
class StepReceipt:
    step: str
    status: StepStatus = "ok"
    reason: str | None = None
    failure_class: FailureClass = None
    criticality: str = "continue"

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "step": self.step,
            "status": self.status,
        }
        if self.reason is not None:
            payload["reason"] = self.reason
        if self.failure_class is not None:
            payload["failure_class"] = self.failure_class
        if self.criticality != "continue":
            payload["criticality"] = self.criticality
        return payload


@dataclass
class InstallReceipt:
    steps: list[StepReceipt] = field(default_factory=list)
    presync_projects: list[str] = field(default_factory=list)
    prewarm_refs: list[str] = field(default_factory=list)
    gitonly_mcp_tools: list[dict[str, str] | str] = field(default_factory=list)
    git_writes: list[dict[str, Any]] = field(default_factory=list)
    offline_latch: bool = False
    tracked_changes: TrackedChangeReport | None = None

    def record(self, receipt: StepReceipt) -> None:
        self.steps.append(receipt)

    def record_git_write(self, entry: dict[str, Any]) -> None:
        self.git_writes.append(entry)

    def ok(self, step: str, **extra: Any) -> None:
        self.record(StepReceipt(step=step, status="ok", **extra))

    def deferred(
        self, step: str, *, reason: str, failure_class: FailureClass = "system"
    ) -> None:
        self.record(
            StepReceipt(
                step=step,
                status="deferred",
                reason=reason,
                failure_class=failure_class,
                criticality="defer",
            )
        )

    def failed(
        self,
        step: str,
        *,
        reason: str,
        failure_class: FailureClass,
        criticality: str = "abort",
    ) -> None:
        self.record(
            StepReceipt(
                step=step,
                status="failed",
                reason=reason,
                failure_class=failure_class,
                criticality=criticality,
            )
        )

    def skipped(self, step: str, *, reason: str) -> None:
        """Record that a step was intentionally not run (e.g. gitonly abort pending)."""
        self.record(
            StepReceipt(
                step=step,
                status="skipped",
                reason=reason,
                criticality="skip",
            )
        )

    def attach_to_manifest(self, manifest: dict[str, object]) -> None:
        manifest["install_steps"] = [step.as_dict() for step in self.steps]
        if self.tracked_changes is not None:
            manifest["tracked_changes"] = self.tracked_changes.as_dict()
        if self.presync_projects:
            manifest["presync_projects"] = self.presync_projects
        if self.prewarm_refs:
            manifest["prewarm_refs"] = self.prewarm_refs
        if self.gitonly_mcp_tools:
            manifest["gitonly_mcp_tools"] = self.gitonly_mcp_tools
        # git_writes is a run-local log (argv, absolute cwd, duration_ms). It
        # is not a durable install FACT: a repeat install of an unchanged tree
        # clones vs fetches and always restamps wall-clock timing, so attaching
        # it to the tracked ledger trains operators to commit noise (RLSE-11).
        # Persist it on the gitignored sidecar instead.
        if self.offline_latch:
            manifest["offline_latch"] = True

    def write_tracked_changes(self, target: Path, manifest: dict[str, Any]) -> None:
        """Attach the CLI's report and persist it without rewriting the ledger.

        The report describes this run, including a possible ledger change. It
        belongs on a local receipt rather than becoming another tracked diff.
        """
        if self.tracked_changes is None:
            return
        manifest["tracked_changes"] = self.tracked_changes.as_dict()
        dest = target / INSTALL_RECEIPT_REL
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    def write_git_write_measurements(self, target: Path) -> None:
        """Write the run-local git-write log next to the tracked ledger.

        ``.workbay/`` is gitignored, so timings and host paths stay local.
        """
        if not self.git_writes:
            return
        dest = Path(target) / GIT_WRITE_SIDECAR_REL
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(self.git_writes, indent=2) + "\n", encoding="utf-8")

    def write_abort_snapshot(
        self,
        target: Path,
        *,
        profile: str,
        source_kind: str,
        remote_url: str | None = None,
        remote_ref: str | None = None,
        remote_sha: str | None = None,
        package_version: str | None = None,
        mcp_servers: Mapping[str, Mapping[str, Any]] | None = None,
        execution_mode: str = "local_ok",
        embeddings_mode: str = "unspecified",
        surfaces: list[dict[str, str]] | None = None,
        configs: list[dict[str, str]] | None = None,
    ) -> None:
        """Persist classified step receipts when a critical install step aborts.

        On an update over an existing install, the previous manifest still
        accurately describes the surfaces/configs materialized on disk when the
        abort fired *before* any surface mutation (presync). Preserve it as the
        snapshot base instead of clobbering it with empty ``surfaces``/``configs``
        — otherwise doctor would misread every still-managed surface as foreign
        drift.

        Gitonly hard abort (ROUND 5) runs *after* surface/config materialization.
        Callers pass the just-materialized ``surfaces`` / ``configs`` so the
        snapshot describes the non-MCP tree already on disk; repair then retries
        tools only and must not assume a bare tree.

        Fresh snapshots use :func:`_build_install_manifest` so capability-mode
        fields (``execution_mode`` / ``embeddings_mode``) follow the same
        key-absence rules as a successful install write.
        """
        from workbay_bootstrap.install import (
            BOOTSTRAP_MANIFEST_NAME,
            SCHEMA_VERSION,
            _build_install_manifest,
        )

        manifest_path = target / BOOTSTRAP_MANIFEST_NAME
        manifest: dict[str, Any] | None = None
        if manifest_path.is_file():
            try:
                existing = json.loads(manifest_path.read_text())
            except (OSError, json.JSONDecodeError):
                existing = None
            if isinstance(existing, dict):
                manifest = existing
        if manifest is None:
            manifest = _build_install_manifest(
                remote_url=remote_url,
                remote_ref=remote_ref,
                remote_sha=remote_sha,
                source_kind=source_kind,
                package_version=package_version,
                profile=profile,
                surfaces=list(surfaces or []),
                configs=list(configs or []),
                mcp_servers=mcp_servers,
                plugin_overrides_path=None,
                execution_mode=execution_mode,
                embeddings_mode=embeddings_mode,
            )
            manifest["schema_version"] = SCHEMA_VERSION
        else:
            # Prefer the just-materialized tree when the abort fired after
            # surfaces/configs landed (gitonly). Leave prior keys alone when
            # the caller did not pass them (presync-before-surfaces path).
            #
            # Empty lists must not erase a prior non-empty manifest: this
            # helper is bookkeeping only and cannot prove managed surfaces
            # were removed. ``surfaces=None`` still preserves prior (presync
            # control). Non-empty lists replace. Empty lists only land when
            # the prior key is missing/empty (ROUND 6 / D2).
            if surfaces:
                manifest["surfaces"] = list(surfaces)
            elif surfaces is not None and not manifest.get("surfaces"):
                manifest["surfaces"] = list(surfaces)
            if configs:
                manifest["configs"] = list(configs)
            elif configs is not None and not manifest.get("configs"):
                manifest["configs"] = list(configs)
        self.attach_to_manifest(manifest)
        self.write_git_write_measurements(target)
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


class InstallPreflightError(RuntimeError):
    def __init__(
        self, message: str, *, failure_class: FailureClass = "application"
    ) -> None:
        super().__init__(message)
        self.failure_class = failure_class


class InstallExecutionError(RuntimeError):
    """Install aborted after a classified step failure (implementation note S6)."""

    def __init__(self, message: str, *, failure_class: FailureClass = "system") -> None:
        super().__init__(message)
        self.failure_class = failure_class


def run_install_preflight(
    *,
    target: Path,
    source_root: Path,
    profile: str,
    source_kind: str,
) -> None:
    """Fail fast before mutating the target tree."""
    if not os.access(target, os.W_OK):
        raise InstallPreflightError(
            f"target is not writable: {target}",
            failure_class="system",
        )
    if source_kind == "package" and not source_root.is_dir():
        raise InstallPreflightError(
            f"package payload root is missing: {source_root}",
            failure_class="application",
        )
    # Generator presence is optional — legacy refs omit it and _run_generator
    # no-ops; preflight must not be stricter than the executor.
    for rel in (".claude", ".codex", ".grok", ".vscode"):
        path = target / rel
        if path.exists() and not os.access(path.parent, os.W_OK):
            raise InstallPreflightError(
                f"cannot write harness config parent for {rel}",
                failure_class="system",
            )
    try:
        usage = shutil.disk_usage(target)
        if usage.free < 50 * 1024 * 1024:
            raise InstallPreflightError(
                "insufficient disk space for install (<50MiB free)",
                failure_class="system",
            )
    except OSError:
        pass


def receipt_failed_steps(manifest: dict[str, object]) -> list[dict[str, Any]]:
    steps = manifest.get("install_steps")
    if not isinstance(steps, list):
        return []
    return [
        step
        for step in steps
        if isinstance(step, dict) and step.get("status") in {"failed", "deferred"}
    ]
