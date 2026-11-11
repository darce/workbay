"""Read-only shared declarations and host observations for bootstrap ledgers.

Legacy ledgers remain readable during expand/migrate/contract. The host receipt
supplies observations only; shared policy and ownership come from the ledger.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path, PureWindowsPath
from typing import Any

from workbay_protocol import BOOTSTRAP_MANIFEST_NAME

HOST_RECEIPT_REL = Path(".workbay/host-receipt.json")
HOST_FACT_KEYS = frozenset(
    {
        "install_steps",
        "presync_projects",
        "prewarm_refs",
        "offline_latch",
        "git_writes",
        "embeddings_inference_receipt",
    }
)
_CONFIG_DECLARATION_KEYS = frozenset({"path", "kind", "ownership"})
_SHARED_DECLARATION_KEYS = frozenset(
    {
        "schema_version",
        "source_kind",
        "remote_ref",
        "remote_sha",
        "package_version",
        "gitonly_closure_ref",
        "stack_distribution",
        "stack_version",
        "stack_members",
        "profile",
        "mcp_servers",
        "execution_mode",
        "embeddings_mode",
        "codemap_mode",
        "embeddings_operator_policy",
    }
)


class InvalidLedgerShape(ValueError):
    """The tracked ledger is not a JSON object."""


class InvalidHostReceipt(ValueError):
    """The host receipt has malformed JSON or an unsupported wire shape."""


def _has_absolute_path(value: object) -> bool:
    if isinstance(value, str):
        if value.startswith(("link:", "git+")):
            value = value.split(":", 1)[1] if value.startswith("link:") else value[4:]
        return value.startswith(("/", "file://")) or PureWindowsPath(value).is_absolute()
    if isinstance(value, Mapping):
        return any(_has_absolute_path(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_absolute_path(item) for item in value)
    return False


def split_ledger(doc: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Project shared declarations and host facts without modifying ``doc``.

    Config identities declare ``action=managed``; actual execution rows belong
    to the host. Absolute registrations, local-copy measurements and recursive
    path-bearing extensions are host facts, while git/network pins stay shared.
    """
    tracked: dict[str, Any] = {}
    host: dict[str, Any] = {}
    for key, value in doc.items():
        if key == "configs" and isinstance(value, list):
            tracked[key] = [
                {
                    **{k: v for k, v in entry.items() if k in _CONFIG_DECLARATION_KEYS and not _has_absolute_path(v)},
                    "action": "managed",
                }
                if isinstance(entry, dict)
                else entry
                for entry in value
                if not isinstance(entry, dict) or not _has_absolute_path(entry.get("path"))
            ]
            if tracked[key] != value:
                host[key] = value
        elif key == "surfaces" and isinstance(value, list):
            tracked[key] = [
                {
                    k: v
                    for k, v in entry.items()
                    if not _has_absolute_path(v) and not (k == "provenance_key" and entry.get("source") == "local")
                }
                if isinstance(entry, dict)
                else entry
                for entry in value
                if not isinstance(entry, dict) or not _has_absolute_path(entry.get("path"))
            ]
            if tracked[key] != value:
                host[key] = value
        elif key in HOST_FACT_KEYS or _has_absolute_path(value):
            host[key] = value
        else:
            tracked[key] = value
    return tracked, host


def baseline_digest(doc: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical shared-declaration projection.

    Observations embedded by legacy/expand writers do not affect the binding.
    Every shared declaration, including policy and extensions, does.
    """
    tracked, _host = split_ledger(doc)
    return hashlib.sha256(json.dumps(tracked, sort_keys=True).encode("utf-8")).hexdigest()


def _validate_host_receipt(receipt: object, source: object) -> Mapping[str, Any]:
    if not isinstance(receipt, Mapping):
        raise InvalidHostReceipt(f"{source}: host receipt is not a JSON object")
    if type(receipt.get("schema_version")) is not int or receipt.get("schema_version") != 1:
        raise InvalidHostReceipt(f"{source}: expected host receipt schema_version 1")
    digest = receipt.get("baseline_digest")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise InvalidHostReceipt(f"{source}: baseline_digest must be a SHA-256 hex digest")
    if not isinstance(receipt.get("facts"), Mapping):
        raise InvalidHostReceipt(f"{source}: facts is not a JSON object")
    return receipt


def parse_host_receipt(data: bytes, *, source: object = HOST_RECEIPT_REL) -> dict[str, Any]:
    """Parse already-read UTF-8 receipt bytes with the shared typed errors.

    The caller retains responsibility for containment, identity and read bounds.
    """
    try:
        receipt = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidHostReceipt(f"{source}: invalid JSON host receipt") from exc
    return dict(_validate_host_receipt(receipt, source))


def overlay_ledger_view(doc: Mapping[str, Any], receipt: Mapping[str, Any] | None) -> dict[str, Any]:
    """Overlay host observations without I/O or modifying either input.

    Missing or stale receipts leave declarations unchanged. Shared policy and
    ownership always come from ``doc``; explicit legacy observations win.
    """
    if not isinstance(doc, Mapping):
        raise InvalidLedgerShape("ledger is not a JSON object")
    if receipt is None:
        return dict(doc)
    receipt = _validate_host_receipt(receipt, HOST_RECEIPT_REL)
    if receipt["baseline_digest"] != baseline_digest(doc):
        return dict(doc)
    tracked, embedded_host = split_ledger(doc)
    declarations, candidates = split_ledger(receipt["facts"])
    accepted = {}
    for key, value in candidates.items():
        if key in HOST_FACT_KEYS:
            accepted[key] = value
        elif key in {"configs", "surfaces"}:
            if declarations.get(key) == tracked.get(key):
                accepted[key] = value
        elif key not in tracked and key not in _SHARED_DECLARATION_KEYS:
            accepted[key] = value
    # A legacy writer's explicit observations supersede an older host receipt.
    return {**doc, **accepted, **embedded_host}


def load_ledger_view(root: Path, *, manifest_name: str = BOOTSTRAP_MANIFEST_NAME) -> dict[str, Any]:
    """Load a legacy or split ledger, overlaying matching host observations.

    Missing or stale receipts leave the ledger unchanged. Malformed receipts
    raise ``InvalidHostReceipt``. No files or directories are created.
    """
    root = Path(root)
    path = root / manifest_name
    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise InvalidLedgerShape(f"{path}: ledger is not a JSON object")
    receipt_path = root / HOST_RECEIPT_REL
    try:
        receipt = parse_host_receipt(receipt_path.read_bytes(), source=receipt_path)
    except FileNotFoundError:
        receipt = None
    return overlay_ledger_view(doc, receipt)
