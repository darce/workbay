"""Derived delivery of planning bytes owned by the existing review-run row.

The snapshot is delivery, never a verdict store. File subjects use SHA256;
branch subjects retain their existing commit identity. Canon v0.26.2 DATA-14,
No dual writes (Designing Data-Intensive Applications, ch. 11; CARD-02/P12).
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .runtime import get_runtime_config
from .shared_write_context import run_subprocess

SUBJECT_KEY = "plan_review_subject"
COMPLETE_KEY = "plan_review_completed"
SUBJECT_SCHEMA = "workbay/plan-review-subject@1"


def subject_root() -> Path:
    config = get_runtime_config()
    return (config.git_workspace_root or config.workspace_root).resolve()


def digest_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def metadata(raw: str | None) -> dict:
    try:
        value = json.loads(raw or "{}")
    except (ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def source_bytes(root: Path, subject_path: str) -> bytes:
    """Read a regular, in-workspace logical source without path redirection."""
    logical = root.resolve() / subject_path
    resolved = logical.resolve(strict=True)
    if resolved != logical or not resolved.is_file():
        raise ValueError("review_subject_path_redirected_or_not_file")
    resolved.relative_to(root.resolve())
    return resolved.read_bytes()


def _revision(root: Path, ref: str, kind: str) -> str:
    if not ref or ref.startswith("-") or any(c in ref for c in ("\0", "\n", "\r")):
        raise ValueError("review_source_ref_invalid")
    result = run_subprocess(
        ["git", "-C", str(root), "rev-parse", "--verify", "--end-of-options", f"{ref}^{{{kind}}}"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    oid = result.stdout.strip()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", oid):
        raise ValueError("review_source_ref_unresolvable")
    return oid


def freeze_subject(
    root: Path,
    task_ref: str,
    subject_path: str,
    source_ref: str,
    review_run_id: str,
    expected_sha: str | None = None,
) -> dict[str, str]:
    # Pin code citations once. The reviewed artifact may be uncommitted.
    commit = _revision(root, source_ref, "commit")
    tree = _revision(root, commit, "tree")
    content = source_bytes(root, subject_path)
    digest = digest_bytes(content)
    if expected_sha is not None and expected_sha != digest:
        raise ValueError("review_subject_digest_mismatch")
    directory = get_runtime_config().state_dir / "review-subjects"
    directory.mkdir(parents=True, exist_ok=True)
    # Run identity, rather than path spelling or a caller-selected filename.
    snapshot = directory / (digest_bytes(review_run_id.encode()) + ".plan")
    try:
        with snapshot.open("xb") as stream:
            stream.write(content)
        snapshot.chmod(0o444)
    except FileExistsError:
        if snapshot.is_symlink() or snapshot.read_bytes() != content:
            raise ValueError("review_snapshot_identity_conflict") from None
    return {
        "schema": SUBJECT_SCHEMA,
        "freeze_id": review_run_id,
        "task_ref": task_ref,
        "review_run_id": review_run_id,
        "subject_path": subject_path,
        "subject_sha256": digest,
        "source_commit": commit,
        "source_tree": tree,
        "snapshot_path": str(snapshot.resolve()),
        "source_root": str(root.resolve()),
        "frozen_at": datetime.now(UTC).isoformat(),
    }


def check_frozen_subject(root: Path, row: Any, expected_sha256: str) -> dict:
    """Called inside the row's write transaction, with no Git subprocesses.

    Canon v0.26.2 CON-11, Atomicity violation (check-then-act), Operating
    Systems: Three Easy Pieces, ch. 32: check the expected row at the effect.
    """
    subject = metadata(row["judge_pin_json"]).get(SUBJECT_KEY)
    if not isinstance(subject, dict) or subject.get("schema") != SUBJECT_SCHEMA:
        raise ValueError("review_subject_unbound")
    digest = row["subject_sha"]
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256 or "") or digest != expected_sha256:
        raise ValueError("review_subject_digest_mismatch")
    for key in ("review_run_id", "task_ref", "subject_path"):
        if subject.get(key) != row[key]:
            raise ValueError("review_subject_identity_mismatch")
    if subject.get("freeze_id") != row["review_run_id"] or subject.get("subject_sha256") != digest:
        raise ValueError("review_subject_identity_mismatch")
    snapshot = Path(subject["snapshot_path"])
    if snapshot.is_symlink() or digest_bytes(snapshot.read_bytes()) != digest:
        raise ValueError("review_snapshot_bytes_changed")
    if digest_bytes(source_bytes(Path(subject.get("source_root", str(root))), row["subject_path"])) != digest:
        raise ValueError("review_subject_bytes_changed")
    return subject
