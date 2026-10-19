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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Mapping

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

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "patch_sha256": self.patch_sha256,
            "verdict_digest": self.verdict_digest,
            "reviewer": dict(self.reviewer),
            "implementer": dict(self.implementer),
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
        if isinstance(field, str) and field:
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
        or not reviewer_identity.get("model")
        or not implementer_identity.get("model")
        or not implementer_identity.get("backend")
    ):
        reasons.append("reviewer_identity_incomplete")
    elif (
        reviewer_identity.get("backend"),
        reviewer_identity.get("model"),
    ) == (
        implementer_identity.get("backend"),
        implementer_identity.get("model"),
    ):
        reasons.append("reviewer_matches_implementer")

    unique_reasons = tuple(dict.fromkeys(reasons))
    return Evaluation(
        "release" if not unique_reasons else "hold",
        unique_reasons,
        expected_hash,
        verdict_digest,
        reviewer_identity,
        implementer_identity,
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
    # SECD-05 lexicons/security.md:255: receipts cannot confer authority.
    evaluation = evaluate_verdict(
        verdict_document.get("verdict"),
        patch_bytes=patch_bytes,
        quarantine=verdict_document.get("quarantine"),
        reviewer=verdict_document.get("reviewer"),
        implementer=verdict_document.get("implementer"),
        repo_root=root,
    )
    if evaluation.status != "release":
        return _release_refusal("review_not_released:" + ",".join(evaluation.reasons))
    patch_hash = evaluation.patch_sha256
    reviewer = evaluation.reviewer
    lane = verdict_document.get("lane")
    quarantine_path = verdict_document.get("quarantine_path")
    verdict_digest = evaluation.verdict_digest
    if not isinstance(lane, str) or not lane or not isinstance(quarantine_path, str) or not quarantine_path:
        return _release_refusal("receipt_location_or_lane_missing")
    if not lane_branch or "\x00" in lane_branch:
        return _release_refusal("invalid_branch")

    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    for variable in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_PREFIX"):
        env.pop(variable, None)
    receipt_path: Path | None = None
    receipt_created = False
    old_tip = ""
    try:
        with tempfile.TemporaryDirectory(prefix="workbay-control-release-") as scratch:
            env["GIT_INDEX_FILE"] = str(Path(scratch) / "index")
            _git(root, ["check-ref-format", "--branch", lane_branch], env=env)
            branch_ref = f"refs/heads/{lane_branch}"
            old_tip = _git_output(root, ["rev-parse", "--verify", branch_ref], env=env)
            if old_tip != expected_tip:
                return _release_refusal("branch_moved", old_tip=old_tip)
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
            return _release_refusal("branch_moved", old_tip=old_tip)
        return ReleaseResult(
            "release", old_tip=old_tip, new_tip=new_tip, receipt_path=str(receipt_path), receipt=artifact
        )
    except subprocess.TimeoutExpired:
        if receipt_created and receipt_path is not None:
            try:
                receipt_path.unlink()
            except OSError:
                pass
        return _release_refusal("git_operation_timeout")
    except subprocess.CalledProcessError as exc:
        if receipt_created and receipt_path is not None:
            try:
                receipt_path.unlink()
            except OSError:
                pass
        stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else str(exc.stderr or "")
        if "invalid branch name" in stderr:
            return _release_refusal("invalid_branch")
        return _release_refusal("git_operation_failed")
    except (OSError, ValueError):
        if receipt_created and receipt_path is not None:
            try:
                receipt_path.unlink()
            except OSError:
                pass
        return _release_refusal("release_io_failed")


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
