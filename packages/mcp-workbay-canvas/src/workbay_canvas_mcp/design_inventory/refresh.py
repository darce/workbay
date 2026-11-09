"""Atomic inventory artifact refresh and --check (WBC-03 S8)."""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from workbay_canvas_mcp.design_inventory.models import InventoryDocument, parse_inventory
from workbay_canvas_mcp.design_inventory.render import render_inventory_markdown
from workbay_canvas_mcp.ux_map.models import _StrictModel

OUTPUT_ORDER = ("inventory.json", "inventory.md", "digest-manifest.json")


class ChangeReport(_StrictModel):
    source_digest_before: str | None
    source_digest_after: str
    outputs_before: dict[str, str | None]
    outputs_after: dict[str, str]
    changed: list[str]


def refresh_generated(*, out_dir: Path, doc: InventoryDocument) -> ChangeReport:
    """Build all three files in a sibling temp dir, validate, then swap atomically."""
    outputs = _render_outputs(doc)
    prior_bytes = {name: _read_if_exists(out_dir / name) for name in OUTPUT_ORDER}
    source_digest_before = _source_digest_from_bytes(prior_bytes["inventory.json"])
    outputs_before = {name: None if data is None else _sha256(data) for name, data in prior_bytes.items()}
    outputs_after = {name: _sha256(data) for name, data in outputs.items()}
    changed = sorted(name for name in OUTPUT_ORDER if outputs_before[name] != outputs_after[name])

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f".{out_dir.name}.", dir=str(out_dir.parent)))
    replaced: list[str] = []
    try:
        for name, data in outputs.items():
            _write_fsync(tmp / name, data)
        for name in OUTPUT_ORDER:
            os.replace(str(tmp / name), str(out_dir / name))
            replaced.append(name)
        _fsync_dir(out_dir)
    except BaseException:
        for name in replaced:
            _restore(out_dir / name, prior_bytes[name])
        _fsync_dir(out_dir)
        raise
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    return ChangeReport(
        source_digest_before=source_digest_before,
        source_digest_after=doc.source_digest,
        outputs_before=outputs_before,
        outputs_after=outputs_after,
        changed=changed,
    )


def check_generated(*, out_dir: Path, doc: InventoryDocument) -> str | None:
    """Return unified-diff summary or None when byte-identical."""
    expected = _render_outputs(doc)
    chunks: list[str] = []
    for name, exp in expected.items():
        path = out_dir / name
        if not path.is_file():
            chunks.append(f"--- {name}\n+++ {name}\n@@ missing generated file @@\n")
            continue
        actual = path.read_bytes()
        if actual == exp:
            continue
        a = actual.decode("utf-8").splitlines(keepends=True)
        b = exp.decode("utf-8").splitlines(keepends=True)
        chunks.extend(difflib.unified_diff(a, b, fromfile=name, tofile=name))
    if not chunks:
        return None
    return "".join(chunks)


def _render_outputs(doc: InventoryDocument) -> dict[str, bytes]:
    validated = parse_inventory(doc.model_dump(mode="json"))
    inventory_json = (json.dumps(validated.model_dump(mode="json"), indent=2, sort_keys=False) + "\n").encode("utf-8")
    inventory_md = render_inventory_markdown(validated).encode("utf-8")
    manifest = (
        json.dumps(
            {
                "inventory.json": _sha256(inventory_json),
                "inventory.md": _sha256(inventory_md),
            },
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    return {
        "inventory.json": inventory_json,
        "inventory.md": inventory_md,
        "digest-manifest.json": manifest,
    }


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _read_if_exists(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError:
        return None


def _source_digest_from_bytes(data: bytes | None) -> str | None:
    if data is None:
        return None
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    digest = payload.get("source_digest")
    return digest if isinstance(digest, str) else None


def _write_fsync(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _restore(path: Path, data: bytes | None) -> None:
    if data is None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        return
    _write_fsync(path, data)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
