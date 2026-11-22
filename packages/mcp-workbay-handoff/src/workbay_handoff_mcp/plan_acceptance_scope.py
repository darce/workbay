"""Exact, independently signed qualification of a native planning result.

The workspace DB custodian provisions STORAGE_SQL and enrolls a task's scope
authority in plan_scope_authorities. Enrollment and revocation are protected
DB administration. The ordinary CLI can inspect evidence and deliver a
signature only. Use a dedicated Ed25519 key held by an independent adjudicator.
The canonical signed bytes are ASCII JSON with sorted keys and compact
separators, excluding the top-level signature field.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

from .config import RuntimeConfig
from .projection_event_dedupe import fetch_projection_event
from .review_findings_recording import _batch_payload_fingerprint, _decode_batch_receipt
from .review_findings_support import _canonical_task_ref_from_handoff
from .review_subject import COMPLETE_KEY, SUBJECT_KEY, SUBJECT_SCHEMA, digest_bytes, metadata
from .runtime import configure_runtime
from .shared_schema import _get_db_connection
from .verified_tests import _record_bytes

STORAGE_SQL = """
CREATE TABLE IF NOT EXISTS plan_scope_authorities (
    task_ref TEXT PRIMARY KEY,
    authority_id TEXT NOT NULL,
    review_agent TEXT NOT NULL,
    public_key TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plan_scope_decisions (
    context_sha256 TEXT NOT NULL,
    authority_sha256 TEXT NOT NULL,
    document TEXT NOT NULL,
    PRIMARY KEY (context_sha256, authority_sha256)
);
"""
SCHEMA = "workbay/plan-acceptance-scope@1"
_RAW_FIELDS = ("severity", "file_path", "description", "failure_scenario", "impact", "origin", "impact_reason")
_PLAN_LIMIT = 1024 * 1024
_OID = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


class ScopeRefusal(ValueError):
    """A typed, read-only refusal of qualification."""


@dataclass(frozen=True)
class Selection:
    repository: Path
    task_ref: str
    path: str
    branch: str
    review_task_ref: str | None = None

    @property
    def review_task(self) -> str:
        return self.review_task_ref or self.task_ref


@dataclass(frozen=True)
class _PinnedSource:
    ref: str
    tip: str
    tree: str
    content: bytes
    review_revision: str


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _digest(document: dict) -> str:
    return digest_bytes(_record_bytes(document))


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, timeout=10, env=env)


def _oid(repo: Path, ref: str) -> str:
    result = _git(repo, "rev-parse", "--verify", "--end-of-options", ref)
    text = result.stdout.decode("ascii").strip()
    if result.returncode != 0 or not _OID.fullmatch(text):
        raise ScopeRefusal("plan_scope_source_unbound")
    return text


def _document(path: Path, limit: int) -> dict:
    def unique(pairs):
        value = dict(pairs)
        if len(value) != len(pairs):
            raise ValueError("duplicate JSON key")
        return value

    if path.is_symlink() or not path.is_file():
        raise ScopeRefusal("plan_scope_document_unavailable")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ScopeRefusal("plan_scope_document_unavailable")
    value = json.loads(data, object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ScopeRefusal("plan_scope_document_invalid")
    return value


@contextmanager
def _connection(selection: Selection, *, effect: bool = False):
    try:
        root = selection.repository.resolve()
        # Neither CLI state-dir flags nor caller environment can select the
        # trust store for this effect. The native repository DB is protected.
        native_state = root / ".task-state"
        config = RuntimeConfig.for_repo(root, state_dir=native_state)
        if (
            config.workspace_root != root
            or config.state_dir != native_state
            or not config.db_path.is_file()
            or config.db_path.is_symlink()
        ):
            raise ScopeRefusal("plan_scope_store_unavailable")
        configure_runtime(config)
        with _get_db_connection(begin_immediate=effect) as conn:
            if not effect:
                conn.execute("BEGIN")
            yield conn, config
    except ScopeRefusal:
        raise
    except (
        ImportError,
        KeyError,
        OSError,
        RuntimeError,
        sqlite3.Error,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ):
        raise ScopeRefusal("plan_scope_evidence_unavailable") from None


def _retained_inventory(conn, config, run: dict, subject: dict) -> tuple[dict, list[dict]]:
    run_id = run["review_run_id"]
    run_hash = digest_bytes(run_id.encode())
    prefix = "planreview_" + run_hash

    def decision(suffix: str) -> dict:
        name = prefix + suffix
        event = fetch_projection_event(conn, name)
        rows = conn.execute(
            "SELECT id, rationale FROM decisions WHERE task_ref = ? AND decision = ?",
            (run["task_ref"], name),
        ).fetchall()
        if (
            len(rows) != 1
            or event is None
            or event["task_ref"] != run["task_ref"]
            or event["target_table"] != "decisions"
            or event["tool_name"] != "record_event.decision"
            or event["target_id"] != rows[0]["id"]
        ):
            raise ScopeRefusal("plan_scope_result_unbound")
        return metadata(rows[0]["rationale"])

    manifest = decision("_result")
    count = manifest.get("finding_count")
    if (
        run["verdict_decision"] != prefix + "_verdict"
        or manifest != decision("_verdict")
        or manifest.get("decision") != run["verdict_decision"]
        or manifest.get("verdict") != run["verdict"]
        or type(count) is not int
        or count < 1
    ):
        raise ScopeRefusal("plan_scope_result_unbound")

    result = _document(config.state_dir / "plan-review-delivery" / run_hash / "result.json", _PLAN_LIMIT)
    binding = decision("_binding")
    findings = result.get("findings")
    if (
        manifest.get("result_sha256") != _digest(result)
        or result.get("binding") != binding
        or result.get("verdict") != run["verdict"]
        or not isinstance(findings, list)
        or len(findings) != count
        or any(
            binding.get(k) != subject[k]
            for k in (
                "freeze_id",
                "task_ref",
                "review_run_id",
                "subject_path",
                "subject_sha256",
                "source_commit",
                "source_tree",
            )
        )
    ):
        raise ScopeRefusal("plan_scope_result_unbound")

    inventory = []
    for start in range(0, count, 100):
        receipt = fetch_projection_event(conn, prefix + f"_findings_{start // 100}")
        if receipt is None or receipt["task_ref"] != run["task_ref"] or receipt["target_table"] != "review_findings":
            raise ScopeRefusal("plan_scope_inventory_incomplete")
        ids, fingerprint = _decode_batch_receipt(receipt["tool_name"])
        chunk = findings[start : start + 100]
        if any(not isinstance(raw, dict) or set(raw) != set(_RAW_FIELDS) for raw in chunk):
            raise ScopeRefusal("plan_scope_inventory_incomplete")
        expected = [
            dict(raw, finding_id=f"NWI04-{run_hash[:20]}-{index + 1:06d}") for index, raw in enumerate(chunk, start)
        ]
        if ids is None or len(ids) != len(chunk) or fingerprint != _batch_payload_fingerprint(expected):
            raise ScopeRefusal("plan_scope_inventory_incomplete")
        for index, (finding_id, raw) in enumerate(zip(ids, chunk, strict=True)):
            rows = conn.execute(
                """SELECT * FROM review_findings WHERE task_ref = ? AND finding_id = ?
                   AND review_mode = 'planning'""",
                (run["task_ref"], finding_id),
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    """SELECT * FROM review_findings_archive WHERE task_ref = ? AND finding_id = ?
                       AND review_mode = 'planning'""",
                    (run["task_ref"], finding_id),
                ).fetchall()
            if (
                len(rows) != 1
                or (index == 0 and rows[0]["id"] != receipt["target_id"])
                or not all(_text(raw[k]) for k in _RAW_FIELDS)
                or raw["impact"] != "follow_up"
            ):
                raise ScopeRefusal("plan_scope_inventory_incomplete")
            inventory.append(dict(rows[0]))

    ids = {row["id"] for row in inventory}
    linked = conn.execute(
        "SELECT id FROM review_findings WHERE review_run_id = ? "
        "UNION SELECT id FROM review_findings_archive WHERE review_run_id = ?",
        (run_id, run_id),
    )
    if len(ids) != count or any(row["id"] not in ids for row in linked):
        raise ScopeRefusal("plan_scope_inventory_incomplete")
    return manifest, inventory


def _canonical_selection(conn, selection: Selection) -> Selection:
    task_ref = _canonical_task_ref_from_handoff(conn, selection.task_ref)
    review_task = _canonical_task_ref_from_handoff(conn, selection.review_task)
    if task_ref is None or review_task is None:
        raise ScopeRefusal("plan_scope_task_unbound")
    return replace(selection, task_ref=task_ref, review_task_ref=review_task)


def _completed_run(conn, selection: Selection) -> tuple[dict, dict]:
    run = conn.execute(
        """SELECT * FROM review_runs WHERE task_ref = ? AND lower(subject_path) = lower(?)
           AND review_mode = 'planning' ORDER BY reviewed_at DESC, id DESC LIMIT 1""",
        (selection.review_task, selection.path),
    ).fetchone()
    if run is None or run["verdict"] != "pass_with_findings":
        raise ScopeRefusal("plan_scope_raw_verdict_ineligible")
    run = dict(run)
    pin = metadata(run["judge_pin_json"])
    subject = pin.get(SUBJECT_KEY)
    if (
        pin.get(COMPLETE_KEY) is not True
        or not isinstance(subject, dict)
        or subject.get("schema") != SUBJECT_SCHEMA
        or any(subject.get(k) != run[k] for k in ("review_run_id", "task_ref", "subject_path"))
        or run["subject_path"] != selection.path
        or subject.get("freeze_id") != run["review_run_id"]
        or subject.get("subject_sha256") != run["subject_sha"]
    ):
        raise ScopeRefusal("plan_scope_review_unbound")

    return run, subject


def _pin_source(selection: Selection, run: dict, subject: dict) -> _PinnedSource:
    root = selection.repository.resolve()
    source = Path(subject["source_root"])
    reviewed_tip = subject["source_commit"]
    ref = "refs/heads/" + selection.branch
    if (
        _git(root, "check-ref-format", ref).returncode != 0
        or run["branch"] != selection.branch
        or run["commit_sha"] != reviewed_tip
        or not _OID.fullmatch(reviewed_tip)
        or RuntimeConfig.for_repo(source).workspace_root != root
        or _oid(root, reviewed_tip + "^{tree}") != subject["source_tree"]
    ):
        raise ScopeRefusal("plan_scope_source_unbound")
    tip = _oid(root, ref + "^{commit}")
    if _git(root, "merge-base", "--is-ancestor", reviewed_tip, tip).returncode != 0:
        raise ScopeRefusal("plan_scope_source_unbound")
    blob = _git(root, "show", f"{tip}:{selection.path}")
    if blob.returncode != 0 or digest_bytes(blob.stdout) != run["subject_sha"]:
        raise ScopeRefusal("plan_scope_plan_changed")
    return _PinnedSource(ref, tip, _oid(root, tip + "^{tree}"), blob.stdout, _digest(run))


def _evidence(
    conn, config, selection: Selection, source: _PinnedSource | None = None
) -> tuple[dict, list[dict], bytes]:
    run, subject = _completed_run(conn, selection)
    if source is None:
        source = _pin_source(selection, run, subject)
    elif source.review_revision != _digest(run):
        raise ScopeRefusal("plan_scope_readiness_changed")
    snapshot = config.state_dir / "review-subjects" / (digest_bytes(run["review_run_id"].encode()) + ".plan")
    if (
        str(snapshot) != subject.get("snapshot_path")
        or snapshot.is_symlink()
        or digest_bytes(snapshot.read_bytes()) != run["subject_sha"]
    ):
        raise ScopeRefusal("plan_scope_plan_changed")

    manifest, current = _retained_inventory(conn, config, run, subject)
    inventory = {row["id"]: row for row in current}
    # Ask the adjudicator to account for older planning rows in both task
    # identities. A mutable status, a different citation path, or missing
    # lineage cannot discard evidence. Include archived rows still retained.
    for table in ("review_findings", "review_findings_archive"):
        for row in conn.execute(
            f"""SELECT * FROM {table} WHERE task_ref IN (?, ?) AND (
                review_mode = 'planning' OR review_run_id IN (
                    SELECT review_run_id FROM review_runs WHERE review_mode = 'planning'
                )
            )""",
            (selection.task_ref, selection.review_task),
        ):
            item = dict(row)
            if inventory.setdefault(item["id"], item) != item:
                raise ScopeRefusal("plan_scope_inventory_ambiguous")
    rows = list(inventory.values())
    revisions = [
        dict(id=row["id"], finding_id=row["finding_id"], revision_sha256=_digest(row))
        for row in sorted(rows, key=lambda item: item["id"])
    ]
    context = {
        "repository": str(config.workspace_root),
        "task_ref": selection.task_ref,
        "review_task_ref": selection.review_task,
        "path": selection.path,
        "raw_sha256": run["subject_sha"],
        "source_ref": source.ref,
        "source_tip": source.tip,
        "source_tree": source.tree,
        "reviewed_source_commit": subject["source_commit"],
        "reviewed_source_tree": subject["source_tree"],
        "reviewed_source_root": subject["source_root"],
        "review_run_id": run["review_run_id"],
        "review_agent": run["agent"],
        "review_session": run["session"],
        "review_revision_sha256": source.review_revision,
        "raw_verdict": run["verdict"],
        "result_sha256": manifest["result_sha256"],
        "finding_count": manifest["finding_count"],
        "native_result_ids": [row["id"] for row in current],
        "inventory": revisions,
    }
    return context, rows, source.content


def _authority(conn, context: dict) -> dict:
    if (
        conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'plan_scope_authorities' AND type = 'table'").fetchone()
        is None
    ):
        raise ScopeRefusal("plan_scope_authority_unavailable")
    grant = conn.execute("SELECT * FROM plan_scope_authorities WHERE task_ref = ?", (context["task_ref"],)).fetchone()
    if (
        grant is None
        or not _text(grant["authority_id"])
        or not _text(context["review_agent"])
        or not _text(context["review_session"])
        or not _text(grant["review_agent"])
        or grant["review_agent"] != context["review_agent"]
        or grant["authority_id"].casefold() == context["review_agent"].casefold()
    ):
        raise ScopeRefusal("plan_scope_authority_unavailable")
    return dict(grant)


def _verify(document: dict, context: dict, rows: list[dict], content: bytes, grant: dict) -> dict:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    signed = {k: v for k, v in document.items() if k != "signature"}
    if (
        set(signed) != {"schema", "context", "authority_id", "dispositions"}
        or signed["schema"] != SCHEMA
        or signed["authority_id"] != grant["authority_id"]
        or not isinstance(signed["context"], dict)
        or _digest(signed["context"]) != _digest(context)
    ):
        raise ScopeRefusal("plan_scope_binding_changed")
    try:
        key = Ed25519PublicKey.from_public_bytes(bytes.fromhex(grant["public_key"]))
        key.verify(bytes.fromhex(document["signature"]), _record_bytes(signed))
    except (InvalidSignature, KeyError, TypeError, ValueError):
        raise ScopeRefusal("plan_scope_signature_unverified") from None

    dispositions = document["dispositions"]
    if not isinstance(dispositions, list) or len(dispositions) != len(context["inventory"]):
        raise ScopeRefusal("plan_scope_dispositions_incomplete")
    rows_by_id = {row["id"]: row for row in rows}
    residual = []
    for finding, disposition in zip(context["inventory"], dispositions, strict=True):
        if (
            not isinstance(disposition, dict)
            or type(disposition.get("id")) is not int
            or disposition["id"] != finding["id"]
            or not _text(disposition.get("owner"))
            or not _text(disposition.get("rationale"))
        ):
            raise ScopeRefusal("plan_scope_disposition_unverified")
        scope = disposition.get("applicability"), disposition.get("disposition")
        fields = {"id", "owner", "rationale", "applicability", "disposition"}
        if (
            scope == ("out_of_scope", "tracked_elsewhere")
            and set(disposition) == fields | {"reference"}
            and _text(disposition["reference"])
        ):
            residual.append(
                dict(
                    finding,
                    **{k: disposition[k] for k in ("owner", "reference", "rationale", "applicability", "disposition")},
                )
            )
            continue
        if not (
            scope == ("in_scope", "addressed_in_plan")
            and set(disposition) == fields | {"plan_quote"}
            and _text(disposition["plan_quote"])
            and disposition["plan_quote"].encode() in content
        ):
            raise ScopeRefusal("plan_scope_disposition_unverified")
        row = rows_by_id[finding["id"]]
        if (
            row["status"] not in {"fixed", "resolved_on_branch", "integrated"}
            or row.get("resolved_on_branch_at_commit") not in {context["reviewed_source_commit"], context["source_tip"]}
            or not _text(row.get("verification_evidence"))
        ):
            raise ScopeRefusal("plan_scope_in_scope_finding_unresolved")
    return {
        "context": context,
        "authority_id": grant["authority_id"],
        "decision_sha256": _digest(document),
        "residual_findings": residual,
    }


def inspect_scope(selection: Selection) -> dict:
    with _connection(selection) as (conn, config):
        context, rows, _ = _evidence(conn, config, selection)
        grant = _authority(conn, context)
        return {"schema": SCHEMA, "context": context, "authority_id": grant["authority_id"], "findings": rows}


def _read_source(selection: Selection) -> _PinnedSource:
    with _connection(selection) as (conn, _):
        selection = _canonical_selection(conn, selection)
        run, subject = _completed_run(conn, selection)
        return _pin_source(selection, run, subject)


def record_scope(selection: Selection, path: Path) -> dict:
    source = _read_source(selection)
    with _connection(selection, effect=True) as (conn, config):
        document = _document(path, 8 * _PLAN_LIMIT)
        context, rows, content = _evidence(conn, config, selection, source)
        grant = _authority(conn, context)
        proof = _verify(document, context, rows, content, grant)
        key = _digest(context), _digest(grant)
        value = _record_bytes(document).decode("ascii")
        conn.execute("INSERT OR IGNORE INTO plan_scope_decisions VALUES (?, ?, ?)", (*key, value))
        row = conn.execute(
            "SELECT document FROM plan_scope_decisions WHERE context_sha256 = ? AND authority_sha256 = ?",
            key,
        ).fetchone()
        if row["document"] != value:
            raise ScopeRefusal("plan_scope_decision_conflict")
        return proof


@contextmanager
def verified_scope(selection: Selection, *, effect: bool = False, expected: dict | None = None):
    """Hold the native review writer fence across the acceptance ref CAS."""
    source = _read_source(selection) if effect else None
    with _connection(selection, effect=effect) as (conn, config):
        context, rows, content = _evidence(conn, config, selection, source)
        grant = _authority(conn, context)
        row = conn.execute(
            "SELECT document FROM plan_scope_decisions WHERE context_sha256 = ? AND authority_sha256 = ?",
            (_digest(context), _digest(grant)),
        ).fetchone()
        if row is None:
            raise ScopeRefusal("plan_scope_missing_or_stale")
        proof = _verify(json.loads(row["document"]), context, rows, content, grant)
        if expected is not None and proof != expected:
            raise ScopeRefusal("plan_scope_readiness_changed")
        yield proof


def scoped_recovery_required(selection: Selection) -> bool:
    """Detect scoped recovery without fabricating a receipt from Git presence."""
    if not (selection.repository / ".task-state" / "handoff.db").exists():
        return False
    with _connection(selection) as (conn, _):
        row = conn.execute(
            """SELECT verdict FROM review_runs WHERE task_ref = ? COLLATE NOCASE AND lower(subject_path) = lower(?)
               AND review_mode = 'planning' ORDER BY reviewed_at DESC, id DESC LIMIT 1""",
            (selection.review_task, selection.path),
        ).fetchone()
        if row is not None and row["verdict"] == "pass_with_findings":
            return True
        return (
            conn.execute(
                """SELECT 1 FROM decisions WHERE task_ref = ? COLLATE NOCASE
               AND substr(decision, 1, 20) = 'plan_accept_applied:'
               AND json_extract(CASE WHEN json_valid(rationale) THEN rationale ELSE '{}' END, '$.path') = ?
               AND json_type(CASE WHEN json_valid(rationale) THEN rationale ELSE '{}' END, '$.scope') = 'object'
               LIMIT 1""",
                (selection.task_ref, selection.path),
            ).fetchone()
            is not None
        )
