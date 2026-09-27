"""Durable launch intents for the preflight transport.

Execution results are deliberately absent here: neither a verdict nor transport
EOF establishes disappearance of a process whose launch was authorized.
"""

import fcntl
import json
import os
import uuid
from pathlib import Path


# Activation requires authenticated completion reconciliation; results and elapsed
# time cannot establish cleanup. Receipt retention remains active independently.
REPLACEMENT_GUARD_ACTIVE = False


def create_launch(root: Path, *, flight_id: str, host: str, kind: str) -> tuple[Path, dict]:
    """Retain one immutable intent before the caller starts its host transport."""
    if uuid.UUID(hex=flight_id).hex != flight_id:
        raise ValueError("invalid flight identity")
    launch_id = uuid.uuid4().hex
    record = {
        "schema": "workbay.probe-launch.v1",
        "flight_id": flight_id,
        "launch_id": launch_id,
        "host": host,
        "kind": kind,
        "owner_nonce": uuid.uuid4().hex,
        "cleanup_state": "unknown",
        "receipt": None,
    }
    path = root / f"probe-launch-{flight_id}-{launch_id}.json"
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w", encoding="utf-8") as stream:
        json.dump(record, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    parent_fd = os.open(root.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    return path, record


def retain_receipt(path: Path, *, flight_id: str, launch_id: str, receipt: dict) -> None:
    """Accept the authenticated transport's first stable supervisor identity.

    The caller must bind the SSH destination before calling. The lock is also
    the future cleanup claimant's serialization boundary; a replay cannot
    replace even an identical receipt.
    """
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        record = json.loads(path.read_text(encoding="utf-8"))
        if (record["flight_id"], record["launch_id"]) != (flight_id, launch_id) or record["receipt"] is not None:
            raise ValueError("ownership linkage mismatch or replay")
        if not isinstance(receipt, dict) or set(receipt) != {
            "host",
            "pid",
            "uid",
            "start_time",
            "command",
            "owner_nonce",
        }:
            raise ValueError("invalid process receipt")
        if receipt["host"] != record["host"] or receipt["owner_nonce"] != record["owner_nonce"]:
            raise ValueError("foreign process receipt")
        if (
            type(receipt["pid"]) is not int
            or receipt["pid"] <= 0
            or type(receipt["uid"]) is not int
            or receipt["uid"] < 0
        ):
            raise ValueError("invalid process identity")
        if not isinstance(receipt["start_time"], str) or not receipt["start_time"].isdigit():
            raise ValueError("invalid process start time")
        if not isinstance(receipt["command"], str) or not receipt["command"]:
            raise ValueError("missing executable identity")
        record["receipt"] = receipt
        record["cleanup_state"] = "pending"
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with os.fdopen(
                os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w", encoding="utf-8"
            ) as stream:
                json.dump(record, stream, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)
