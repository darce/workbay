"""Install step receipts and pre-flight (implementation note S6)."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

StepStatus = Literal["ok", "failed", "deferred", "skipped"]
FailureClass = Literal["system", "application"] | None


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

    def record(self, receipt: StepReceipt) -> None:
        self.steps.append(receipt)

    def record_git_write(self, entry: dict[str, Any]) -> None:
        self.git_writes.append(entry)

    def ok(self, step: str, **extra: Any) -> None:
        self.record(StepReceipt(step=step, status="ok", **extra))

    def deferred(self, step: str, *, reason: str, failure_class: FailureClass = "system") -> None:
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

    def attach_to_manifest(self, manifest: dict[str, object]) -> None:
        manifest["install_steps"] = [step.as_dict() for step in self.steps]
        if self.presync_projects:
            manifest["presync_projects"] = self.presync_projects
        if self.prewarm_refs:
            manifest["prewarm_refs"] = self.prewarm_refs
        if self.gitonly_mcp_tools:
            manifest["gitonly_mcp_tools"] = self.gitonly_mcp_tools
        if self.git_writes:
            manifest["git_writes"] = self.git_writes
        if self.offline_latch:
            manifest["offline_latch"] = True

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
    ) -> None:
        """Persist classified step receipts when a critical install step aborts.

        On an update over an existing install, the previous manifest still
        accurately describes the surfaces/configs materialized on disk (the
        abort fired before any surface mutation). Preserve it as the snapshot
        base instead of clobbering it with empty ``surfaces``/``configs`` —
        otherwise doctor would misread every still-managed surface as foreign
        drift.

        Fresh snapshots use :func:`_build_install_manifest` so capability-mode
        fields (``execution_mode`` / ``embeddings_mode``) follow the same
        key-absence rules as a successful install write.
        """
        import json

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
                surfaces=[],
                configs=[],
                mcp_servers=mcp_servers,
                plugin_overrides_path=None,
                execution_mode=execution_mode,
                embeddings_mode=embeddings_mode,
            )
            manifest["schema_version"] = SCHEMA_VERSION
        self.attach_to_manifest(manifest)
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


class InstallPreflightError(RuntimeError):
    def __init__(self, message: str, *, failure_class: FailureClass = "application") -> None:
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
        if isinstance(step, dict)
        and step.get("status") in {"failed", "deferred"}
    ]
