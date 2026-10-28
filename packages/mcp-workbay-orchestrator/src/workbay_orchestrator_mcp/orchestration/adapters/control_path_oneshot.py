"""Bind a quarantined patch and a trusted review to one host release operation.

Reviewer and implementer identities are host-observed inputs. Reviewer text
supplies only a verdict; neither receipts nor worker claims confer authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping

from . import control_path_release as control


@dataclass(frozen=True)
class OneShotResult:
    status: Literal["released", "hold", "refused"]
    reasons: tuple[str, ...] = ()
    patch_sha256: str = ""
    verdict_digest: str = ""
    release_commit: str | None = None
    receipt_path: str | None = None
    evaluation: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "patch_sha256": self.patch_sha256,
            "verdict_digest": self.verdict_digest,
            "release_commit": self.release_commit,
            "receipt_path": self.receipt_path,
            "evaluation": self.evaluation,
        }


@dataclass(frozen=True)
class QuarantineCase:
    record_path: Path
    patch_path: Path
    record: dict[str, object]
    patch_bytes: bytes
    request: control.ReviewRequest


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value: str) -> object:
    raise ValueError(f"invalid JSON constant: {value}")


def _json(text: str) -> object:
    return json.loads(text, object_pairs_hook=_object, parse_constant=_invalid_constant)


def load_quarantine(record_path: str | Path) -> QuarantineCase | OneShotResult:
    """Read the producer's sibling record/patch pair and bind all four hashes."""
    try:
        location = Path(record_path).resolve()
        if location.suffix != ".json":
            raise ValueError("expected quarantine record")
        record = _json(location.read_text(encoding="utf-8"))
        if not isinstance(record, dict) or not isinstance(record.get("quarantine"), dict):
            raise ValueError("invalid quarantine record")
        patch_path = location.with_suffix(".patch")
        patch_bytes = patch_path.read_bytes()
        request = control.review_request(record["quarantine"], patch_bytes)
        if isinstance(request, control.ReviewRefusal):
            return OneShotResult("refused", (request.code,))
        stored = record.get("review_request")
        expected = {k: v for k, v in request.to_dict().items() if k not in {"patch_text", "patch_base64"}}
        patch_hash = hashlib.sha256(patch_bytes).hexdigest()
        if not (
            isinstance(stored, dict)
            and patch_hash == patch_path.stem == stored.get("patch_sha256") == request.patch_sha256
            and stored == expected
        ):
            return OneShotResult("refused", ("patch_record_mismatch",))
        return QuarantineCase(location, patch_path, record, patch_bytes, request)
    except (OSError, ValueError, TypeError, UnicodeError, RuntimeError, RecursionError):
        return OneShotResult("refused", ("patch_record_mismatch",))


def build_review_brief(case: QuarantineCase) -> str:
    """Render the exact bytes, reach evidence and strict verdict contract."""
    request = case.request
    context = {
        "patch_sha256": request.patch_sha256,
        "import_chains": request.import_chains,
        "reach_status": request.reach_status,
        "reach_cause": request.reach_cause,
    }
    schema = {
        "verdict": "release | hold",
        "patch_sha256": request.patch_sha256,
        "import_chain_reviewed": ["every quarantined path and every member of its import chain"],
        "findings": [{"severity": "low | medium | high | critical", "file": "path", "summary": "description"}],
    }
    encoding, patch = ("UTF-8", request.patch_text) if request.patch_text is not None else ("base64", request.patch_base64)
    return (
        "Review these quarantined patch bytes and every listed import chain.\n"
        + json.dumps(context, sort_keys=True, ensure_ascii=False, indent=2)
        + "\nThe verdict must have exactly these four keys and this schema:\n"
        + json.dumps(schema, sort_keys=True, ensure_ascii=False, indent=2)
        + "\nAny high or critical finding means hold.\n"
        "import_chain_reviewed must be a list containing every quarantined path and every chain member.\n"
        "Answer with exactly one JSON object and nothing else\n"
        f"\nPatch ({encoding}):\n{patch}\n"
    )


def parse_reviewer_verdict(text: str) -> dict[str, object] | None:
    """Accept one bare or fenced JSON object, with no repair or selection."""
    if not isinstance(text, str):
        return None
    content = text.strip()
    if content.startswith("```"):
        fenced = re.fullmatch(r"```(?:json)?[ \t]*\r?\n(.*?)\r?\n```", content, re.DOTALL)
        if fenced is None:
            return None
        content = fenced.group(1)
    try:
        verdict = _json(content)
    except (ValueError, RecursionError):
        return None
    if not isinstance(verdict, dict) or set(verdict) != control._VERDICT_KEYS:
        return None
    return verdict


def release_quarantined(
    repo_root: str | Path,
    record_path: str | Path,
    reviewer_text: str,
    *,
    implementer: Mapping[str, object],
    reviewer: Mapping[str, object],
    lane_branch: str,
    expected_tip: str,
    source_commit: str | None = None,
    source_ordinal: int | None = None,
) -> OneShotResult:
    """Load and bind bytes, then let release evaluate once and enforce its result.

    A record releases only for the lane it was recorded for: its lane_id must
    equal the host-observed implementer lane before any verdict is read.

    Step 2 does not return its Evaluation. A successful receipt supplies its
    fields; otherwise evaluation is empty rather than fabricated. No second
    evaluation or policy read is used to reconstruct missing evidence.
    """
    try:
        location = Path(record_path)
        if not location.is_absolute():
            location = Path(repo_root) / location
    except (TypeError, ValueError):
        return OneShotResult("refused", ("patch_record_mismatch",))
    case = load_quarantine(location)
    if isinstance(case, OneShotResult):
        return case
    patch_hash = case.request.patch_sha256
    # SECD-03: the record names the lane that produced these bytes. It only
    # gates the host-observed implementer lane, compared exactly; neither
    # identity is derived from the record.
    recorded, observed = case.record.get("lane_id"), implementer.get("lane")
    if not isinstance(recorded, str) or not recorded:
        return OneShotResult("refused", ("quarantine_lane_unbound",), patch_hash)
    if not isinstance(observed, str) or observed != recorded:
        return OneShotResult("refused", ("quarantine_lane_mismatch",), patch_hash)
    verdict = parse_reviewer_verdict(reviewer_text)
    if verdict is None:
        return OneShotResult("hold", ("verdict_unparseable",), patch_hash)
    # Only the reviewer's final text supplies the verdict. Host identities and
    # the checked quarantine record supply all other release inputs.
    document: dict[str, object] = {
        "verdict": verdict,
        "quarantine": case.record["quarantine"],
        "reviewer": dict(reviewer),
        "implementer": dict(implementer),
        "lane": implementer.get("lane"),
        "quarantine_path": str(case.patch_path),
    }
    if source_commit is not None:
        document["source_commit"] = source_commit
    if source_ordinal is not None:
        document["source_ordinal"] = source_ordinal
    result = control.release(repo_root, lane_branch, expected_tip, case.patch_bytes, document)
    reasons: tuple[str, ...] = ()
    status: Literal["released", "hold", "refused"] = "released"
    if result.status != "release":
        prefix = "review_not_released:"
        if result.reason.startswith(prefix):
            status = "hold"
            reasons = tuple(result.reason[len(prefix):].split(","))
        else:
            status = "refused"
            reasons = (result.reason,)
    verdict_digest = control.review_policy.digest(verdict)
    receipt = result.receipt or {}
    evaluation = {}
    if result.status == "release":
        evaluation = control.Evaluation(
            "release", (), receipt["patch_sha256"], receipt["verdict_digest"],
            control._identity(document["reviewer"]), control._identity(document["implementer"]), receipt["review_policy"],
        ).to_dict()
    return OneShotResult(
        status, reasons, patch_hash, verdict_digest,
        result.new_tip or None, result.receipt_path or None, evaluation,
    )


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("operation", choices=("brief", "release"))
    try:
        args = parser.parse_args(argv)
        value = _json(sys.stdin.read())
        if not isinstance(value, dict):
            raise ValueError("stdin JSON must be an object")
        required = ["record_path"]
        if args.operation == "release":
            required += ["repo_root", "reviewer_text", "lane_branch", "expected_tip"]
        for key in required:
            if not isinstance(value.get(key), str) or (key != "reviewer_text" and not value[key]):
                raise ValueError(f"{key} is required")
        if args.operation == "brief":
            case = load_quarantine(value["record_path"])
            if isinstance(case, QuarantineCase):
                print(json.dumps({"status": "review_required", "patch_sha256": case.request.patch_sha256,
                                  "brief": build_review_brief(case)}, sort_keys=True))
                return 0
            result = case
        else:
            for key in ("implementer", "reviewer"):
                if not isinstance(value.get(key), dict):
                    raise ValueError(f"{key} must be a host-observed identity object")
            source_commit, source_ordinal = value.get("source_commit"), value.get("source_ordinal")
            if source_commit is not None and (not isinstance(source_commit, str) or not source_commit):
                raise ValueError("source_commit must be a nonempty string")
            if source_ordinal is not None and (not isinstance(source_ordinal, int) or isinstance(source_ordinal, bool)):
                raise ValueError("source_ordinal must be an integer")
            result = release_quarantined(
                value["repo_root"], value["record_path"], value["reviewer_text"],
                implementer=value["implementer"], reviewer=value["reviewer"],
                lane_branch=value["lane_branch"], expected_tip=value["expected_tip"],
                source_commit=source_commit, source_ordinal=source_ordinal,
            )
        print(json.dumps(result.to_dict(), sort_keys=True))
        return {"released": 0, "hold": 3, "refused": 2}[result.status]
    except (ValueError, OSError, RecursionError) as exc:
        print(json.dumps(OneShotResult("refused", (f"invalid_input:{exc}",)).to_dict(), sort_keys=True))
        return 64


if __name__ == "__main__":
    raise SystemExit(main())
