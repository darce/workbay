"""Native handoff producer; the caller owns the SQLite transaction.

Git discovery belongs before BEGIN IMMEDIATE. Only DML runs in stage_change.
R3 is an optional package capability, imported from its small storage module,
never its public facade. Missing source/package evidence is explicitly unknown.
No lifecycle, custody, review, or execution proof is inferred from a task write.

Canon v0.26.2 DATA-14, No dual writes (Designing Data-Intensive Applications,
ch. 11; engineering.md:232), DOM-06, Outbox or it didn't happen (Learning
Domain-Driven Design, ch. 9; engineering.md:684). Original corpus unavailable.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import socket
import sqlite3
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import uuid4

if TYPE_CHECKING:
    from workbay_protocol.resource_lifecycle import GitSource, ResourceIdentity

from .runtime import get_runtime_config
from .shared_write_context import ResolvedWriteContext


@dataclass(frozen=True)
class ProducerBinding:
    resource: ResourceIdentity | None = None
    source: GitSource | None = None
    host: str | None = None
    reason: str | None = None


def _git(root: Path, *args: str) -> str | None:
    # Hook/CI Git overrides must not redirect the owner/source probes.
    env = {
        "PATH": os.defpath,
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
    }
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", "-c", "core.fsmonitor=false", "-C", str(root), *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def prepare_binding(ctx: ResolvedWriteContext, worktree_path: str | None = None) -> ProducerBinding:
    """Resolve only Git identity metadata; never open auth/credential files."""
    try:
        from workbay_protocol.resource_lifecycle import GitSource, Owner, ResourceIdentity
    except ModuleNotFoundError as exc:
        if exc.name != "workbay_protocol.resource_lifecycle":
            raise
        return ProducerBinding(reason="resource_protocol_unavailable")
    config = get_runtime_config()
    root = Path(worktree_path) if worktree_path else config.compaction_config_root
    root_text = _git(root, "rev-parse", "--show-toplevel")
    revision = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")
    branch = _git(root, "branch", "--show-current")
    if not root_text or not revision or not tree:
        return ProducerBinding(reason="source_unavailable")
    if ctx.commit_sha != revision or ctx.branch != branch:
        return ProducerBinding(reason="actor_source_mismatch")
    # Read just the origin identity, not git credential helpers or credentials.
    # Never include the raw value in a receipt, exception, or evidence payload.
    remote = _git(root, "config", "--local", "--get", "remote.origin.url")
    if not remote:
        return ProducerBinding(reason="owner_unavailable")
    if "://" in remote:
        try:
            parsed = urlsplit(remote)
            host, path = parsed.hostname, parsed.path
        except ValueError:
            return ProducerBinding(reason="owner_unavailable")
    else:
        match = re.fullmatch(r"(?:[^/@:]+@)?([^/:]+):(.+)", remote)
        if match is None:
            return ProducerBinding(reason="owner_unavailable")
        host, path = match.groups()
    namespace = f"repo:{host}/{path.strip('/').removesuffix('.git')}"
    if not host or not re.fullmatch(r"repo:[A-Za-z0-9][A-Za-z0-9._/-]*", namespace):
        return ProducerBinding(reason="owner_unavailable")
    return ProducerBinding(
        resource=ResourceIdentity(
            owner=Owner(repository=namespace),
            resource_kind="git_worktree",
            identity=str(Path(root_text).resolve()),
        ),
        source=GitSource(revision=revision, tree=tree),
        host=socket.gethostname(),
    )


def _event_store():
    try:
        return importlib.import_module("workbay_orchestrator_mcp.orchestration.resource_events")
    except ModuleNotFoundError as exc:
        if exc.name not in {
            "workbay_orchestrator_mcp",
            "workbay_orchestrator_mcp.orchestration",
            "workbay_orchestrator_mcp.orchestration.resource_events",
            "workbay_protocol.resource_lifecycle",
        }:
            raise
        return None


def replay_receipt(conn: sqlite3.Connection, task_ref: str, event_id: str | None) -> dict:
    """Recover the original outbox receipt without repeating producer DML."""
    if (
        not event_id
        or not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='resource_events'").fetchone()
    ):
        return {"knowledge": "unknown", "reason": "original_resource_event_unavailable"}
    operation = (
        "handoff:"
        + hashlib.sha256(json.dumps([task_ref, event_id.strip()], separators=(",", ":")).encode()).hexdigest()
    )
    row = conn.execute(
        "SELECT id,event_id,payload_json FROM resource_events WHERE operation_id=? AND wb_task=? ORDER BY id LIMIT 1",
        (operation, task_ref),
    ).fetchone()
    if row is None:
        return {"knowledge": "unknown", "reason": "original_resource_event_unavailable"}
    payload = json.loads(row[2])
    snapshot = None
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lifecycle_resource_producer_evidence'"
    ).fetchone():
        evidence = conn.execute(
            "SELECT payload_json FROM lifecycle_resource_producer_evidence WHERE ref=?", (payload["transaction_ref"],)
        ).fetchone()
        if evidence is not None:
            snapshot = json.loads(evidence[0])
    return {"knowledge": "observed", "sequence": row[0], "event_id": row[1], "inserted": False, "snapshot": snapshot}


def stage_change(
    conn: sqlite3.Connection,
    *,
    binding: ProducerBinding | None,
    task_ref: str,
    ctx: ResolvedWriteContext,
    snapshot: dict,
    event_id: str | None,
    state_change: Callable[[sqlite3.Connection], object],
) -> dict:
    """Stage R3 + authoritative DML atomically. Never commit or publish.

    Evidence is the intended native row snapshot retained on this authority,
    not a qualified retention proof. Task writes have no heartbeat contract;
    stream stays unspecified rather than inventing a cadence/deadline.
    """
    if not conn.in_transaction:
        raise ValueError("lifecycle_resource_transaction_required")
    reason = binding.reason if binding else "source_not_prepared"
    store = _event_store()
    if store is None:
        reason = reason or "resource_events_package_unavailable"
    if reason or binding is None or binding.resource is None or binding.source is None:
        state_change(conn)
        return {"knowledge": "unknown", "reason": reason or "source_unavailable"}
    from workbay_protocol.resource_lifecycle import (
        Correlation,
        EvidenceRef,
        Held,
        Hold,
        LifecycleEvent,
        Proof,
        Registration,
        WorkBayIdentity,
        event_identity,
    )

    payload = json.dumps(
        {**snapshot, "producer_host": binding.host}, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    digest = "sha256:" + hashlib.sha256(payload.encode()).hexdigest()
    operation = (
        "handoff:"
        + hashlib.sha256(
            json.dumps(
                [task_ref, (event_id.strip() if event_id else None) or uuid4().hex], separators=(",", ":")
            ).encode()
        ).hexdigest()
    )
    evidence = EvidenceRef(ref="sqlite:handoff-mutation/" + operation.split(":")[1], digest=digest)
    correlation = Correlation(workbay=WorkBayIdentity(task=task_ref, lane=ctx.lane_id))
    registration = Registration(
        resource=binding.resource,
        source=binding.source,
        operation="withdraw_materialization",
        policy_version="consumer-retention-v1",
        evidence=(evidence,),
        correlation=correlation,
    )
    event = LifecycleEvent(
        event_id=event_identity(binding.resource, operation, "registered"),
        operation_id=operation,
        phase="registered",
        resource=binding.resource,
        source=binding.source,
        policy_version=registration.policy_version,
        evidence=(evidence,),
        correlation=correlation,
        outcome=Held(scope="retention", holds=(Hold(code="proof_unknown", subject="retention.qualification"),)),
        observations=(Proof(evidence=(evidence,)),),
        durable_committed_at=datetime.now(timezone.utc),
        transaction_ref=evidence.ref,
        # Existing public event_id carries an explicit synthetic test marker;
        # ordinary writes never masquerade as synthetic observations.
        synthetic_tag=event_id if event_id and event_id.startswith("synthetic:") else None,
    )
    store.initialize_resource_events(conn)
    conn.execute("""CREATE TABLE IF NOT EXISTS lifecycle_resource_producer_evidence (
        ref TEXT PRIMARY KEY, payload_json TEXT NOT NULL, digest TEXT NOT NULL
    )""")

    def mutate(connection):
        state_change(connection)
        connection.execute(
            "INSERT INTO lifecycle_resource_producer_evidence(ref,payload_json,digest) VALUES (?,?,?)",
            (evidence.ref, payload, digest),
        )

    sequence, inserted = store.record_resource_event(
        conn,
        event,
        registration=registration,
        state_change=mutate,
    )
    return {"knowledge": "observed", "sequence": sequence, "inserted": inserted, "event_id": event.event_id}
