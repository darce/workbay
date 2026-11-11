"""Dual-write host observations while preserving the legacy tracked bytes."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from workbay_protocol import BOOTSTRAP_MANIFEST_NAME
from workbay_protocol.bootstrap_ledger_view import HOST_RECEIPT_REL, baseline_digest, split_ledger


def _write_host_receipt(target: Path, manifest: Mapping[str, Any]) -> None:
    _tracked, host = split_ledger(manifest)
    sidecar = target / HOST_RECEIPT_REL
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    # Sparse/abort writes can happen before the consumer's root ignore exists.
    ignore = sidecar.parent / ".gitignore"
    existing = ignore.read_text(encoding="utf-8") if ignore.exists() else ""
    additions = [entry for entry in ("/host-receipt.json", "/.gitignore") if entry not in existing.splitlines()]
    if additions:
        separator = "\n" if existing and not existing.endswith("\n") else ""
        ignore.write_text(existing + separator + "\n".join(additions) + "\n", encoding="utf-8")
    receipt = {"schema_version": 1, "baseline_digest": baseline_digest(manifest), "facts": host}
    sidecar.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_install_manifest(target: Path, manifest: Mapping[str, Any]) -> None:
    """Write the additive receipt and exactly the pre-expansion ledger format.

    The caller retains its existing writer lock and validation. Serialization
    order, whitespace and all tracked host fields stay compatible with legacy
    readers until the contract step.
    """
    target = Path(target)
    _write_host_receipt(target, manifest)
    (target / BOOTSTRAP_MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n")


def refresh_host_receipt(target: Path) -> None:
    """Mirror an external policy/abort writer without rewriting tracked bytes."""
    from workbay_bootstrap.embedding_provision import _bootstrap_writer_lock

    target = Path(target)
    if not (target / BOOTSTRAP_MANIFEST_NAME).is_file():
        return
    with _bootstrap_writer_lock(target):
        try:
            manifest = json.loads((target / BOOTSTRAP_MANIFEST_NAME).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return  # No readable snapshot exists to attest to.
        if isinstance(manifest, dict):
            _write_host_receipt(target, manifest)
