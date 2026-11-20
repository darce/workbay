"""Task-start graph adapter and read-time plan provenance comparison.

No regeneration: an existing manifest is compared and never published again.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .plan_decomposer import (
    NoDecompositionError,
    extract_decomposition_payload,
)

MANIFEST_LOCK_TIMEOUT = 5.0


def decomposition_digest(payload: bytes) -> str:
    """Hash the exact fence interior, including newlines and trailing spaces."""
    return hashlib.sha256(payload).hexdigest()


def read_plan_source_status(root: Path | str, manifest: Mapping[str, Any]) -> dict[str, str]:
    """Compare with current plan bytes on every call; never write any source."""
    source = manifest.get("plan_source")
    if source is None:
        return {"state": "untracked", "reason": "manifest has no plan_source"}
    try:
        from .generate_lane_manifest import build_manifest_from_plan

        if not isinstance(source, Mapping) or source.get("algorithm") != "sha256" or source.get("version") != 1:
            raise ValueError("invalid plan_source")
        selected = (Path(root).resolve() / source["path"]).resolve()
        selected.relative_to(Path(root).resolve())
        raw = selected.read_bytes()
        # Validate through the generation path too, so malformed JSON, unknown
        # prerequisites and cycles all produce unknown rather than false drift.
        current = build_manifest_from_plan(
            root=root,
            task_ref=str(manifest.get("task_ref") or "source"),
            plan_path=selected,
            plan_bytes=raw,
        )["plan_source"]["decomposition_digest"]
        if current != source.get("decomposition_digest"):
            return {"state": "drifted", "reason": "plan edited since manifest"}
        return {"state": "matched", "reason": "decomposition digest matches manifest"}
    except (OSError, ValueError, RuntimeError, TypeError, KeyError) as exc:
        return {"state": "unknown", "reason": f"{type(exc).__name__}: {exc}"[:500]}


def _existing_result(path: Path, digest: str | None) -> dict[str, Any]:
    existing = json.loads(path.read_bytes())
    source = existing.get("plan_source") if isinstance(existing, dict) else None
    if source is None:
        outcome = "untracked"
    else:
        outcome = "matched" if isinstance(source, dict) and source.get("decomposition_digest") == digest else "drifted"
    return {"outcome": outcome, "manifest_path": str(path), "decomposition_digest": digest}


def start_plan_graph(root: Path | str, task_ref: str, plan_path: Path | str) -> dict[str, Any]:
    """Generate once under the manifest writer lock; typed refusal otherwise."""
    from .generate_lane_manifest import build_manifest_from_plan
    from .lane_manifest import _persist_manifest_unlocked, manifest_write_lock

    root = Path(root).resolve()
    path = root / "config" / "lane-orchestration" / f"{task_ref}.json"
    receipt: dict[str, Any] = {"manifest_path": str(path), "decomposition_digest": None}
    try:
        if not task_ref or Path(task_ref).name != task_ref or task_ref in {".", ".."}:
            raise ValueError("task_ref must be a file-safe task identity")
        selected = (root / plan_path).resolve()
        selected.relative_to(root)
        raw = selected.read_bytes()
        # Legacy manifests require no plan parsing or rewriting.
        if path.exists():
            existing = _existing_result(path, None)
            if existing["outcome"] == "untracked":
                return existing
        payload = extract_decomposition_payload(raw)
        receipt["decomposition_digest"] = decomposition_digest(payload)
        # Build validates JSON, dependencies and source even when comparing an existing file.
        manifest = build_manifest_from_plan(root=root, task_ref=task_ref, plan_path=selected, plan_bytes=raw)
        # Use the same read of bytes for both generation and the receipt.
        receipt["decomposition_digest"] = manifest["plan_source"]["decomposition_digest"]
    except NoDecompositionError:
        return {**receipt, "outcome": "skipped", "reason": "no_decomposition"}
    except (OSError, ValueError, RuntimeError, TypeError) as exc:
        return {**receipt, "outcome": "failed", "reason": "invalid_decomposition", "detail": str(exc)[:500]}
    try:
        # Fast comparison is read-only; first publication rechecks under the lock.
        if path.exists():
            return _existing_result(path, receipt["decomposition_digest"])
        with manifest_write_lock(path, timeout=MANIFEST_LOCK_TIMEOUT):
            if path.exists():
                return _existing_result(path, receipt["decomposition_digest"])
            _persist_manifest_unlocked(manifest, path)
    except (OSError, RuntimeError, ValueError) as exc:
        return {**receipt, "outcome": "failed", "reason": "manifest_publication_failed", "detail": str(exc)[:500]}
    return {**receipt, "outcome": "generated"}


def _dispose(root: Path, task_ref: str, apply: bool) -> dict[str, Any]:
    try:
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime
        from workbay_handoff_mcp.import_export import expire_terminal_plan_cursors

        configure_runtime(
            RuntimeConfig.for_workspace(root, state_dir=root / ".task-state", current_task_auto_regen=False)
        )
        return expire_terminal_plan_cursors(task_ref=task_ref, apply=apply)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}"[:500],
            "would_expire": [],
            "expired": [],
            "raced": [],
            "failed": [],
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--task", required=True)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--dispose-terminal-cursors", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if args.dispose_terminal_cursors:
        result = _dispose(args.root.resolve(), args.task, args.apply)
        ok = result.get("ok") is True
    else:
        if args.plan is None or args.apply:
            parser.error("generation requires --plan; --apply is only for --dispose-terminal-cursors")
        result = start_plan_graph(args.root, args.task, args.plan)
        ok = result.get("outcome") != "failed"
    print(json.dumps(result))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
