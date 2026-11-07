"""Trusted-review request, verdict evaluation, and fenced patch release.

This module is deliberately host-side. It never runs repository hooks and
creates the release commit from a temporary Git index, then advances the
requested branch with compare-and-swap.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Mapping

from . import control_path_review_policy as review_policy
from .git_control_paths import QuarantineVerdict, classify_patch_reach

GIT_OPERATION_TIMEOUT_SECONDS = 10
_VERDICT_KEYS = {"verdict", "patch_sha256", "import_chain_reviewed", "findings"}
_FINDING_KEYS = {"severity", "file", "summary"}
_SEVERITIES = {"low", "medium", "high", "critical"}


@dataclass(frozen=True)
class ReviewRefusal:
    status: Literal["refusal"]
    code: Literal["operator_required", "not_quarantined", "invalid_quarantine"]
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {"status": self.status, "code": self.code, "reason": self.reason}


@dataclass(frozen=True)
class ReviewRequest:
    status: Literal["review_required"]
    patch_sha256: str
    patch_base64: str
    patch_text: str | None
    import_chains: dict[str, list[str]]
    reach_status: str
    reach_cause: str

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "patch_sha256": self.patch_sha256,
            "patch_base64": self.patch_base64,
            "patch_text": self.patch_text,
            "import_chains": self.import_chains,
            "reach_status": self.reach_status,
            "reach_cause": self.reach_cause,
        }


@dataclass(frozen=True)
class Evaluation:
    status: Literal["release", "hold"]
    reasons: tuple[str, ...]
    patch_sha256: str
    verdict_digest: str
    reviewer: dict[str, str]
    implementer: dict[str, str]
    review_policy: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "patch_sha256": self.patch_sha256,
            "verdict_digest": self.verdict_digest,
            "reviewer": dict(self.reviewer),
            "implementer": dict(self.implementer),
            "review_policy": self.review_policy,
        }


@dataclass(frozen=True)
class ReleaseResult:
    status: Literal["release", "refusal"]
    reason: str = ""
    old_tip: str = ""
    new_tip: str = ""
    receipt_path: str = ""
    receipt: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reason": self.reason,
            "old_tip": self.old_tip,
            "new_tip": self.new_tip,
            "receipt_path": self.receipt_path,
            "receipt": self.receipt,
        }


def _quarantine_files(quarantine: object) -> tuple[list[dict[str, object]], str, str]:
    if isinstance(quarantine, QuarantineVerdict):
        return (
            [
                {"path": item.path, "reach": item.reach, "import_chain": list(item.import_chain)}
                for item in quarantine.files
            ],
            quarantine.status,
            quarantine.cause,
        )
    if not isinstance(quarantine, Mapping):
        return [], "degraded", "invalid_quarantine"
    status = quarantine.get("status", "ok")
    cause = quarantine.get("cause", "")
    if not isinstance(status, str) or status not in {"ok", "degraded"}:
        return [], "degraded", "invalid_quarantine"
    if not isinstance(cause, str):
        return [], "degraded", "invalid_quarantine"
    raw_files = quarantine.get("files", [])
    files: list[dict[str, object]] = []
    if isinstance(raw_files, Mapping):
        iterable = [
            {"path": path, **(value if isinstance(value, Mapping) else {})} for path, value in raw_files.items()
        ]
    elif isinstance(raw_files, list):
        iterable = raw_files
    else:
        return [], "degraded", "invalid_quarantine"
    for item in iterable:
        if not isinstance(item, Mapping):
            return [], "degraded", "invalid_quarantine"
        path, reach, chain = item.get("path"), item.get("reach"), item.get("import_chain", [])
        if (
            not isinstance(path, str)
            or not isinstance(reach, str)
            or reach not in {"direct", "package_via_hook_import", "none"}
        ):
            return [], "degraded", "invalid_quarantine"
        if not isinstance(chain, (list, tuple)) or not all(isinstance(part, str) for part in chain):
            return [], "degraded", "invalid_quarantine"
        files.append({"path": path, "reach": reach, "import_chain": list(chain)})
    return files, status, cause


def review_request(quarantine: object, patch_bytes: bytes) -> ReviewRequest | ReviewRefusal:
    """Build the exact patch/hash/import-chain input for a trusted reviewer."""
    files, status, cause = _quarantine_files(quarantine)
    if not files and cause == "invalid_quarantine":
        return ReviewRefusal("refusal", "invalid_quarantine", "quarantine record is malformed")
    in_reach = [item for item in files if item["reach"] != "none"]
    if any(item["reach"] == "direct" for item in in_reach):
        return ReviewRefusal("refusal", "operator_required", "direct hook and Git controls require operator review")
    if not in_reach:
        return ReviewRefusal("refusal", "not_quarantined", "patch contains no quarantined path")
    chains = {str(item["path"]): list(item["import_chain"]) for item in in_reach}
    try:
        patch_text = patch_bytes.decode("utf-8")
    except UnicodeDecodeError:
        patch_text = None
    return ReviewRequest(
        "review_required",
        hashlib.sha256(patch_bytes).hexdigest(),
        base64.b64encode(patch_bytes).decode("ascii"),
        patch_text,
        chains,
        status,
        cause,
    )


def _strict_verdict(verdict: object) -> tuple[dict[str, object] | None, str]:
    if not isinstance(verdict, Mapping):
        return None, "malformed_verdict"
    malformed = set(verdict) != _VERDICT_KEYS
    normalized: dict[str, object] = {}
    verdict_name = verdict.get("verdict")
    if not isinstance(verdict_name, str) or verdict_name not in {"release", "hold"}:
        malformed = True
    else:
        normalized["verdict"] = verdict_name
    patch_hash = verdict.get("patch_sha256")
    if not isinstance(patch_hash, str) or re.fullmatch(r"[0-9a-f]{64}", patch_hash) is None:
        malformed = True
    else:
        normalized["patch_sha256"] = patch_hash
    reviewed = verdict.get("import_chain_reviewed")
    if not isinstance(reviewed, list) or not all(isinstance(path, str) and path for path in reviewed):
        malformed = True
    else:
        normalized["import_chain_reviewed"] = list(reviewed)
    findings = verdict.get("findings")
    if not isinstance(findings, list):
        malformed = True
        findings = []
    clean_findings: list[dict[str, str]] = []
    for finding in findings:
        if not isinstance(finding, Mapping) or set(finding) != _FINDING_KEYS:
            malformed = True
            continue
        severity, file, summary = finding.get("severity"), finding.get("file"), finding.get("summary")
        if (
            not isinstance(severity, str)
            or severity.lower() not in _SEVERITIES
            or not isinstance(file, str)
            or not isinstance(summary, str)
        ):
            malformed = True
            continue
        clean_findings.append({"severity": severity.lower(), "file": file, "summary": summary})
    if isinstance(findings, list):
        normalized["findings"] = clean_findings
    return normalized, "malformed_verdict" if malformed else ""


def _identity(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    identity: dict[str, str] = {}
    for key in ("lane", "backend", "model"):
        field = value.get(key)
        if isinstance(field, str) and field and field == field.strip():
            identity[key] = field
    return identity


def evaluate_verdict(
    verdict: object,
    *,
    patch_bytes: bytes,
    quarantine: object,
    reviewer: object,
    implementer: object,
    repo_root: str | Path | None = None,
    release_context: Mapping[str, object] | None = None,
) -> Evaluation:
    """Evaluate all release conditions and retain every failing reason."""
    expected_hash = hashlib.sha256(patch_bytes).hexdigest()
    normalized, parse_error = _strict_verdict(verdict)
    canonical = verdict if isinstance(verdict, Mapping) else {"invalid": repr(verdict)}
    try:
        verdict_digest = hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
    except (TypeError, ValueError):
        verdict_digest = hashlib.sha256(repr(verdict).encode("utf-8", "replace")).hexdigest()

    reasons: list[str] = []
    # SECD-03 lexicons/security.md:253: recheck authority for these bytes.
    if repo_root is None:
        reasons.append("trusted_repo_root_missing")
    else:
        try:
            from .hook_reach import _clear_hook_reach_cache

            _clear_hook_reach_cache()
            actual = classify_patch_reach(patch_bytes.decode("utf-8"), repo_root)
            supplied_files, supplied_status, supplied_cause = _quarantine_files(quarantine)
            actual_files, actual_status, actual_cause = _quarantine_files(actual)
            if (supplied_files, supplied_status, supplied_cause) != (actual_files, actual_status, actual_cause):
                reasons.append("quarantine_mismatch")
            quarantine = actual
        except (OSError, UnicodeError, ValueError, TypeError):
            reasons.append("reach_reclassification_failed")
    if parse_error:
        reasons.append(parse_error)
    if normalized is not None:
        if normalized.get("verdict") == "hold":
            reasons.append("verdict_is_hold")
        if any(item["severity"] in {"high", "critical"} for item in normalized.get("findings", [])):
            reasons.append("high_or_critical_finding")
        if "patch_sha256" in normalized and normalized["patch_sha256"] != expected_hash:
            reasons.append("patch_sha256_mismatch")
        files, quarantine_status, quarantine_cause = _quarantine_files(quarantine)
        package_files = [item for item in files if item["reach"] == "package_via_hook_import"]
        direct_files = [item for item in files if item["reach"] == "direct"]
        if direct_files:
            reasons.append("operator_required")
        if not package_files and not direct_files:
            reasons.append("not_quarantined")
        if quarantine_status == "degraded":
            reasons.append(f"reach_probe_degraded:{quarantine_cause or 'unknown'}")
        required: set[str] = set()
        for item in package_files:
            required.add(str(item["path"]))
            chain = list(item["import_chain"])
            required.update(chain)
        if "import_chain_reviewed" in normalized:
            reviewed = set(normalized["import_chain_reviewed"])
            missing = sorted(required - reviewed)
            if missing:
                reasons.extend(f"import_chain_not_reviewed:{path}" for path in missing)
    else:
        files, _status, _cause = _quarantine_files(quarantine)
        if any(item["reach"] == "direct" for item in files):
            reasons.append("operator_required")

    reviewer_identity = _identity(reviewer)
    implementer_identity = _identity(implementer)
    if reviewer_identity.get("backend") != "codex-remote":
        reasons.append("reviewer_backend_untrusted")
    if (
        not reviewer_identity.get("lane")
        or not implementer_identity.get("lane")
        or not reviewer_identity.get("model")
        or not implementer_identity.get("model")
        or not implementer_identity.get("backend")
    ):
        reasons.append("reviewer_identity_incomplete")
    same_model = (
        reviewer_identity.get("backend"),
        reviewer_identity.get("model"),
    ) == (
        implementer_identity.get("backend"),
        implementer_identity.get("model"),
    )
    if reviewer_identity.get("lane") and reviewer_identity.get("lane") == implementer_identity.get("lane"):
        reasons.append("reviewer_lane_matches_implementer")
    if isinstance(reviewer, Mapping) and isinstance(implementer, Mapping):
        if reviewer.get("dispatch_id") and reviewer.get("dispatch_id") == implementer.get("dispatch_id"):
            reasons.append("review_dispatch_not_independent")

    resolved_policy: dict[str, object] = {}
    independent_dispatch = False
    try:
        # The release repository's Git common-dir authority is re-derived and
        # re-walked here on every use; verdict fields, release context and
        # Git environment overrides cannot select another authority.
        policy = review_policy.load_policy(repo_root)
        resolved_policy = policy.to_dict()
        if policy.mode == "distinct_dispatch":
            context = dict(release_context) if release_context is not None else {}
            if repo_root is not None:
                context.update(
                    repo_root=str(Path(repo_root).resolve()), patch_sha256=expected_hash, verdict_digest=verdict_digest
                )
            resolved_policy.update(
                review_policy.check_dispatch_proof(
                    policy,
                    context,
                    author=implementer,
                    reviewer=reviewer,
                )
            )
            independent_dispatch = True
    except review_policy.PolicyHold as exc:
        reasons.append(str(exc))
    except OSError:
        reasons.append("review_policy_authority_unavailable")
    except (ValueError, TypeError, RecursionError):
        reasons.append("review_policy_invalid_document")
    if same_model and not independent_dispatch:
        reasons.append("reviewer_matches_implementer")

    unique_reasons = tuple(dict.fromkeys(reasons))
    return Evaluation(
        "release" if not unique_reasons else "hold",
        unique_reasons,
        expected_hash,
        verdict_digest,
        reviewer_identity,
        implementer_identity,
        resolved_policy,
    )


def _git(
    root: Path, args: list[str], *, env: dict[str, str], input_bytes: bytes | None = None
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(root), "-c", "core.hooksPath=/dev/null", *args],
        input=input_bytes,
        capture_output=True,
        check=True,
        timeout=GIT_OPERATION_TIMEOUT_SECONDS,
        env=env,
    )


def _git_output(root: Path, args: list[str], *, env: dict[str, str]) -> str:
    completed = _git(root, args, env=env)
    return completed.stdout.decode("utf-8", "replace").strip()


def _release_refusal(reason: str, *, old_tip: str = "") -> ReleaseResult:
    return ReleaseResult("refusal", reason, old_tip=old_tip)


class ProductionHistoryUnavailable(RuntimeError):
    """A prepared host mutation cannot safely proceed or be acknowledged."""


def _production_admission(root: Path, task_ref: object, lane: object, branch: str):
    """Discover authority through the public lookup; never create a legacy root."""
    if not isinstance(lane, str) or not lane:
        return None
    try:
        from workbay_handoff_mcp import get_runtime_config
        from workbay_handoff_mcp import lane_completion_production as store
        from workbay_handoff_mcp.handoff_state import get_handoff_state
        from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError

        try:
            get_runtime_config()
        except RuntimeNotConfiguredError:
            from workbay_handoff_mcp import RuntimeConfig, configure_runtime
            from workbay_handoff_mcp.config import ConsumerRootResolutionError

            try:
                config = RuntimeConfig.for_repo(root)
            except ConsumerRootResolutionError:
                return None
            # The standalone release CLI has no orchestrator runtime. Discover
            # only an existing authoritative DB; legacy calls create no state.
            if not config.db_path.is_file():
                return None
            configure_runtime(config)
        if not isinstance(task_ref, str) or not task_ref:
            state = get_handoff_state(branch=branch, read_profile="identity")
            task_ref = (state.get("data", {}).get("active") or {}).get("task_ref")
        if not task_ref:
            return None
        try:
            admission = store.get_lane_production_admission(task_ref=task_ref, lane_id=lane)["data"]
        except store.ProductionRefusal:
            # No authenticated current root is available. This includes legacy
            # registrations without a contract; they keep their old behavior.
            return None
        identity = admission["binding"]["identity"]
        env = review_policy.git_environment()
        common = _git_output(root, ["rev-parse", "--path-format=absolute", "--git-common-dir"], env=env)
        repository_common = _git_output(
            Path(identity["repository"]), ["rev-parse", "--path-format=absolute", "--git-common-dir"], env=env
        )
        if Path(common).resolve() != Path(repository_common).resolve() or identity["branch"] != branch:
            raise ProductionHistoryUnavailable("production_history_unavailable: foreign repository or branch")
        return admission
    except ProductionHistoryUnavailable:
        raise
    except Exception as exc:
        raise ProductionHistoryUnavailable(f"production_history_unavailable: {type(exc).__name__}") from exc


def _production_head(admission):
    """The current head from a one-row read; independent of the history size."""
    from workbay_handoff_mcp import lane_completion_production as store

    data = store.get_lane_production_history(admission["binding"], admission["admission_id"], limit=1)["data"]
    return store.ProductionHead(**data["head"])


def _production_history(admission):
    """Rows visible inside the store's bounded collection, possibly a prefix.

    Only recovery scans rows. Imports and releases address their own operation
    and never refuse because a long-lived lane outgrew the bounded read.
    """
    from workbay_handoff_mcp import lane_completion_production as store

    first = store.get_lane_production_history(admission["binding"], admission["admission_id"])["data"]
    rows = list(first["history"])
    if first["next_cursor"] is not None:
        rows.extend(
            store.get_lane_production_history(
                admission["binding"], admission["admission_id"], cursor=first["next_cursor"]
            )["data"]["history"]
        )
    return rows


class _OperationExists(Exception):
    """The store already holds this operation phase under another payload."""


class _ProductionOperation:
    """One stable prepare/settle pair, shared by the two host producers.

    The operation is addressed by its id: a new prepare is appended at the
    current head, and an existing one is detected by the store's idempotency
    refusal rather than by reading the whole admission history.
    """

    def __init__(self, admission, kind, operation_id, facts, *, prepared=None):
        self.admission = admission
        self.kind = kind
        self.operation_id = operation_id
        self.facts = facts
        self.prepared = prepared
        self.settled = None
        if prepared is not None:
            return
        try:
            self._append("prepare", facts)
        except _OperationExists:
            self._load_existing()

    def _load_existing(self):
        matching = [row for row in _production_history(self.admission) if row["operation_id"] == self.operation_id]
        prepared = next((row for row in matching if row["evidence_kind"] == "production_prepare"), None)
        if prepared is None:
            # Recorded beyond the bounded read: still an existing intent, so
            # the caller must not repeat the effect.
            self.prepared = {"operation_id": self.operation_id, "payload": None}
            return
        if prepared["payload"]["facts"] != self.facts or prepared["payload"]["producer_kind"] != self.kind:
            raise ProductionHistoryUnavailable("production_history_unavailable: conflicting host operation")
        self.prepared = prepared
        self.settled = next((row for row in matching if row["evidence_kind"] == "production_settle"), None)

    def _append(self, phase, facts):
        from workbay_handoff_mcp import lane_completion_production as store

        try:
            request = store.ProductionAppend(
                binding=self.admission["binding"],
                admission_id=self.admission["admission_id"],
                operation_id=self.operation_id,
                producer_kind=self.kind,
                producer_version="1",
                writer_authority_ref=f"host:{self.kind}:v1",
                expected_head=_production_head(self.admission),
                facts=facts,
            )
            return getattr(store, f"record_production_{phase}")(request)
        except store.ProductionRefusal as exc:
            if exc.code == store.RefusalCode.IDEMPOTENCY_CONFLICT:
                raise _OperationExists(phase) from exc
            raise ProductionHistoryUnavailable(f"production_history_unavailable: {exc.code.value}") from exc
        except Exception as exc:
            raise ProductionHistoryUnavailable(f"production_history_unavailable: {type(exc).__name__}") from exc

    def settle(self, facts):
        if self.settled is not None:
            return
        try:
            self._append("settle", facts)
        except _OperationExists:
            pass  # The original settlement stands; a repeat appends nothing.
        self.settled = True


def _production_patch_path(root: Path, operation_id: str) -> Path:
    common = _git_output(
        root, ["rev-parse", "--path-format=absolute", "--git-common-dir"], env=review_policy.git_environment()
    )
    return Path(common) / "workbay-production" / f"{hashlib.sha256(operation_id.encode()).hexdigest()}.patch"


def _retain_production_patch(path: Path, patch: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != patch:
            raise ProductionHistoryUnavailable("production_history_unavailable: retained patch conflict")
    else:
        with os.fdopen(fd, "wb") as stream:
            stream.write(patch)
            stream.flush()
            os.fsync(stream.fileno())
        for parent in (path.parent, path.parent.parent):
            directory = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    return {"path": str(path), "sha256": hashlib.sha256(patch).hexdigest(), "bytes": len(patch), "kind": "source_patch"}


def _production_facts(artifact, *, outcome, commits=(), publication=None, extra_artifacts=()):
    return dict(
        outcome=outcome,
        product_artifacts=[artifact, *extra_artifacts],
        commits=list(commits),
        publication=publication,
        coverage_complete=False,
        source_gap="remote_source_journal_unavailable",
    )


def reconcile_trusted_release(root: Path, *, task_ref: str | None, lane_id: str, branch: str) -> dict[str, int]:
    """Resolve original open release intents from their retained commit refs."""
    admission = _production_admission(root, task_ref, lane_id, branch)
    if admission is None:
        return {"pending": 0}
    rows = _production_history(admission)
    settled = {row["operation_id"] for row in rows if row["evidence_kind"] == "production_settle"}
    pending = 0
    for row in rows:
        if (
            row["evidence_kind"] != "production_prepare"
            or row["payload"]["producer_kind"] != "trusted_release"
            or row["operation_id"] in settled
        ):
            continue
        facts = row["payload"]["facts"]
        try:
            patch = Path(facts["patch_path"]).read_bytes()
            if not facts["source_patch_verified"] or hashlib.sha256(patch).hexdigest() != facts["patch_sha256"]:
                pending += 1
                continue
            env = review_policy.git_environment()
            oid = _git_output(root, ["rev-parse", "--verify", facts["object_ref"] + "^{commit}"], env=env)
            parent = _git_output(root, ["show", "-s", "--format=%P", oid], env=env)
            tree = _git_output(root, ["show", "-s", "--format=%T", oid], env=env)
            if parent != facts["old_oid"]:
                pending += 1
                continue
            with tempfile.TemporaryDirectory(prefix="workbay-release-reconcile-") as scratch:
                env["GIT_INDEX_FILE"] = str(Path(scratch) / "index")
                _git(root, ["read-tree", parent], env=env)
                _git(root, ["apply", "--cached", "--binary", "-"], env=env, input_bytes=patch)
                expected_tree = _git_output(root, ["write-tree"], env=env)
            if tree != expected_tree:
                pending += 1
                continue
            current = _git_output(root, ["rev-parse", "--verify", facts["ref"]], env=env)
            publication = {"ref": facts["ref"], "old_oid": parent, "new_oid": oid} if current == oid else None
            operation = _ProductionOperation(admission, "trusted_release", row["operation_id"], facts, prepared=row)
            artifact = {
                "path": facts["patch_path"],
                "sha256": facts["patch_sha256"],
                "bytes": len(patch),
                "kind": "source_patch",
            }
            operation.settle(
                _production_facts(
                    artifact,
                    outcome="reconciled",
                    commits=[{"oid": oid, "parent_oid": parent, "tree_oid": tree}],
                    publication=publication,
                )
            )
        except Exception as exc:
            import logging

            logging.getLogger(__name__).warning("production_history_unavailable: release reconciliation: %s", exc)
            pending += 1
    return {"pending": pending}


def release(
    repo_root: str | Path,
    lane_branch: str,
    expected_tip: str,
    patch_bytes: bytes,
    verdict_document: Mapping[str, object],
) -> ReleaseResult:
    """Evaluate the verdict on the host, then release via a branch CAS.

    The document contains verdict, quarantine, reviewer/implementer identities,
    lane and quarantine_path. Evaluation receipts are output only.
    """
    if not isinstance(verdict_document, Mapping):
        return _release_refusal("invalid_verdict_document")
    if not isinstance(patch_bytes, bytes):
        return _release_refusal("invalid_patch")
    try:
        root = Path(repo_root).resolve()
    except (OSError, TypeError, ValueError):
        return _release_refusal("invalid_repo_root")
    lane = verdict_document.get("lane")
    quarantine_path = verdict_document.get("quarantine_path")
    context = None
    if isinstance(lane, str) and isinstance(quarantine_path, str) and quarantine_path:
        try:
            quarantine_location = Path(quarantine_path)
            if not quarantine_location.is_absolute():
                quarantine_location = root / quarantine_location
            context = {
                "lane": lane,
                "quarantine_path": str(quarantine_location.resolve()),
                "lane_branch": lane_branch,
                "expected_tip": expected_tip,
            }
            for key in ("source_commit", "source_ordinal"):
                if key in verdict_document:
                    context[key] = verdict_document[key]
        except (OSError, ValueError, RuntimeError):
            return _release_refusal("invalid_quarantine_path")
    if isinstance(lane, str) and lane and lane_branch:
        try:
            reconcile_trusted_release(root, task_ref=verdict_document.get("task_ref"), lane_id=lane, branch=lane_branch)
        except ProductionHistoryUnavailable:
            return _release_refusal("production_history_unavailable")
    # SECD-05 lexicons/security.md:255: receipts cannot confer authority.
    evaluation = evaluate_verdict(
        verdict_document.get("verdict"),
        patch_bytes=patch_bytes,
        quarantine=verdict_document.get("quarantine"),
        reviewer=verdict_document.get("reviewer"),
        implementer=verdict_document.get("implementer"),
        repo_root=root,
        release_context=context,
    )
    if evaluation.status != "release":
        return _release_refusal("review_not_released:" + ",".join(evaluation.reasons))
    patch_hash = evaluation.patch_sha256
    reviewer = evaluation.reviewer
    verdict_digest = evaluation.verdict_digest
    if not isinstance(lane, str) or not lane or not isinstance(quarantine_path, str) or not quarantine_path:
        return _release_refusal("receipt_location_or_lane_missing")
    if not lane_branch or "\x00" in lane_branch:
        return _release_refusal("invalid_branch")

    # Shared with the policy authority lookup, so both resolve one ref store.
    env = review_policy.git_environment()
    receipt_path: Path | None = None
    receipt_created = False
    old_tip = ""
    production_operation = None
    production_artifact = None
    production_commits = []
    production_publication = None
    production_outcome = "release_failed"

    def finish(result):
        if production_operation is not None and production_artifact is not None:
            try:
                production_operation.settle(
                    _production_facts(
                        production_artifact,
                        outcome=production_outcome,
                        commits=production_commits,
                        publication=production_publication,
                    )
                )
            except Exception as exc:
                # The product and branch must survive an evidence outage.
                import logging

                logging.getLogger(__name__).warning("production_history_unavailable: trusted release: %s", exc)
                result = ReleaseResult(
                    result.status,
                    "production_history_unavailable",
                    old_tip=result.old_tip,
                    new_tip=result.new_tip,
                    receipt_path=result.receipt_path,
                    receipt=result.receipt,
                )
        return result

    try:
        with tempfile.TemporaryDirectory(prefix="workbay-control-release-") as scratch:
            env["GIT_INDEX_FILE"] = str(Path(scratch) / "index")
            _git(root, ["check-ref-format", "--branch", lane_branch], env=env)
            branch_ref = f"refs/heads/{lane_branch}"
            old_tip = _git_output(root, ["rev-parse", "--verify", branch_ref], env=env)
            if old_tip != expected_tip:
                return finish(_release_refusal("branch_moved", old_tip=old_tip))
            admission = _production_admission(root, verdict_document.get("task_ref"), lane, lane_branch)
            if admission is not None:
                key = json.dumps([admission["admission_id"], expected_tip, patch_hash, verdict_digest])
                operation_id = "trusted_release:" + hashlib.sha256(key.encode()).hexdigest()
                patch_path = _production_patch_path(root, operation_id)
                object_ref = "refs/workbay/production/" + hashlib.sha256(operation_id.encode()).hexdigest()
                quarantine_file = Path(quarantine_path)
                if not quarantine_file.is_absolute():
                    quarantine_file = root / quarantine_file
                try:
                    source_patch_verified = quarantine_file.read_bytes() == patch_bytes
                except OSError:
                    source_patch_verified = False
                production_operation = _ProductionOperation(
                    admission,
                    "trusted_release",
                    operation_id,
                    dict(
                        old_oid=expected_tip,
                        ref=branch_ref,
                        patch_sha256=patch_hash,
                        patch_path=str(patch_path),
                        verdict_digest=verdict_digest,
                        object_ref=object_ref,
                        source_patch_verified=source_patch_verified,
                    ),
                )
                if not source_patch_verified:
                    return _release_refusal("production_history_unavailable", old_tip=old_tip)
                if production_operation.prepared is not None:
                    # Never re-run commit-tree with a new timestamp for an open intent.
                    return _release_refusal("production_history_unavailable", old_tip=old_tip)
                production_artifact = _retain_production_patch(patch_path, patch_bytes)
            _git(root, ["read-tree", expected_tip], env=env)
            _git(root, ["apply", "--cached", "--binary", "--whitespace=nowarn", "-"], env=env, input_bytes=patch_bytes)
            tree = _git_output(root, ["write-tree"], env=env)

            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            env.update(
                {
                    "GIT_AUTHOR_NAME": "Workbay trusted review",
                    "GIT_AUTHOR_EMAIL": "trusted-review@workbay.invalid",
                    "GIT_COMMITTER_NAME": "Workbay trusted review",
                    "GIT_COMMITTER_EMAIL": "trusted-review@workbay.invalid",
                    "GIT_AUTHOR_DATE": now,
                    "GIT_COMMITTER_DATE": now,
                }
            )
            message = f"Release trusted-reviewed patch {patch_hash[:12]}"
            new_tip = _git_output(root, ["commit-tree", tree, "-p", expected_tip, "-m", message], env=env)
            if production_operation is not None:
                production_commits.append({"oid": new_tip, "parent_oid": expected_tip, "tree_oid": tree})
                # Keep the exact returned object even when receipt I/O or the
                # target branch CAS fails, independently of the worker spool.
                _git(root, ["update-ref", object_ref, new_tip, ""], env=env)

        # The temporary index is now fully cleaned up. Receipt I/O and the
        # branch CAS are the only remaining operations, with the CAS last.
        quarantine = Path(quarantine_path)
        if not quarantine.is_absolute():
            quarantine = root / quarantine
        receipt_path = quarantine.with_name(quarantine.name + ".release.json")
        artifact: dict[str, object] = {
            "lane": lane,
            "branch": lane_branch,
            "old_tip": old_tip,
            "new_tip": new_tip,
            "patch_sha256": patch_hash,
            "reviewer_lane": reviewer["lane"],
            "reviewer_backend": reviewer["backend"],
            "verdict_digest": verdict_digest,
            "released_at_utc": now,
            "review_policy": evaluation.review_policy,
        }
        receipt_bytes = (json.dumps(artifact, sort_keys=True, indent=2) + "\n").encode("utf-8")
        receipt_fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        receipt_created = True
        with os.fdopen(receipt_fd, "wb") as stream:
            stream.write(receipt_bytes)
            stream.flush()
            os.fsync(stream.fileno())

        try:
            _git(root, ["update-ref", branch_ref, new_tip, expected_tip], env=env)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            try:
                receipt_path.unlink()
            except OSError:
                pass
            return finish(_release_refusal("branch_moved", old_tip=old_tip))
        production_outcome = "released"
        production_publication = {"ref": branch_ref, "old_oid": old_tip, "new_oid": new_tip}
        return finish(
            ReleaseResult("release", old_tip=old_tip, new_tip=new_tip, receipt_path=str(receipt_path), receipt=artifact)
        )
    except ProductionHistoryUnavailable:
        return _release_refusal("production_history_unavailable", old_tip=old_tip)
    except subprocess.TimeoutExpired:
        if receipt_created and receipt_path is not None:
            try:
                receipt_path.unlink()
            except OSError:
                pass
        return finish(_release_refusal("git_operation_timeout"))
    except subprocess.CalledProcessError as exc:
        if receipt_created and receipt_path is not None:
            try:
                receipt_path.unlink()
            except OSError:
                pass
        stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else str(exc.stderr or "")
        if "invalid branch name" in stderr:
            return finish(_release_refusal("invalid_branch"))
        return finish(_release_refusal("git_operation_failed"))
    except (OSError, ValueError):
        if receipt_created and receipt_path is not None:
            try:
                receipt_path.unlink()
            except OSError:
                pass
        return finish(_release_refusal("release_io_failed"))


def _read_cli_input() -> dict[str, object]:
    value = json.load(__import__("sys").stdin)
    if not isinstance(value, dict):
        raise ValueError("stdin JSON must be an object")
    return value


def _patch_from_input(value: Mapping[str, object]) -> bytes:
    encoded = value.get("patch_base64")
    if isinstance(encoded, str):
        return base64.b64decode(encoded, validate=True)
    text = value.get("patch_text")
    if isinstance(text, str):
        return text.encode("utf-8")
    raise ValueError("patch_base64 or patch_text is required")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("request", "evaluate", "release"))
    args = parser.parse_args(argv)
    try:
        value = _read_cli_input()
        patch_bytes = _patch_from_input(value)
        if args.operation == "request":
            result: object = review_request(value.get("quarantine"), patch_bytes)
        elif args.operation == "evaluate":
            result = evaluate_verdict(
                value.get("verdict"),
                patch_bytes=patch_bytes,
                quarantine=value.get("quarantine"),
                reviewer=value.get("reviewer"),
                implementer=value.get("implementer"),
                repo_root=value.get("repo_root"),
                release_context=value.get("release_context"),
            )
        else:
            root, branch, tip = (
                value.get("repo_root"),
                value.get("lane_branch"),
                value.get("expected_tip"),
            )
            if not isinstance(root, str) or not isinstance(branch, str) or not isinstance(tip, str):
                raise ValueError("repo_root, lane_branch, and expected_tip are required")
            result = release(root, branch, tip, patch_bytes, value)
        output = result.to_dict() if hasattr(result, "to_dict") else result
        print(json.dumps(output, sort_keys=True))
        return 0
    except (ValueError, json.JSONDecodeError, OSError) as exc:
        print(json.dumps({"status": "refusal", "reason": f"invalid_input:{exc}"}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
