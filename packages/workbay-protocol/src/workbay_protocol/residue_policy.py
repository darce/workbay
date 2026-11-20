"""Pure residue decision contract and source-derived stdlib deployment.

Observation, journaling, locks, generation rechecks and destructive effects
belong to adapters. Eligibility here never asserts that deletion occurred.
"""

import base64
from collections.abc import Mapping
import hashlib
import json
from pathlib import Path
import re


policy_v1 = {"version": 1, "action": "execute_delete"}


def _source_documents():
    root = Path(__file__).parent
    return {name: (root / (name + ".py")).read_bytes()
            for name in ("residue_policy", "regenerable_content")}


def _digest_documents(sources):
    return hashlib.sha256(b"".join(
        name.encode("ascii") + b"\0" + sources[name] for name in sorted(sources)
    )).hexdigest()


def _source_digest():
    return _digest_documents(_source_documents())


def _source_references(value, generation):
    return isinstance(value, list) and bool(value) and all(
        isinstance(ref, Mapping)
        and isinstance(ref.get("ref"), str) and bool(ref["ref"])
        and ref.get("state") == "terminal"
        and ref.get("verified") is True
        and ref.get("generation") == generation
        for ref in value
    )


def _custody_references(value):
    return isinstance(value, list) and bool(value) and all(
        isinstance(ref, Mapping)
        and isinstance(ref.get("path"), str) and bool(ref["path"])
        and isinstance(ref.get("sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]) is not None
        and ref.get("verified") is True
        for ref in value
    )


def assess_residue(facts, policy, previous=None):
    """Assess execute-delete facts without mutating inputs or any resource.

    Unknown process/dependency coverage or observations hold deletion. Positive
    generation confirmation, authority and product custody are required
    independently of completion and age. ``previous`` can fence the resource
    identity/generation; it is not an effect receipt.
    """
    result = {
        "resource_id": facts.get("resource_id") if isinstance(facts, dict) else None,
        "generation": facts.get("generation") if isinstance(facts, dict) else None,
        "state": "refused", "reason": "proof_incomplete", "paths": [],
        "policy_digest": _source_digest(), "effect_status": "not_attempted",
    }
    if not isinstance(facts, dict) or not all(
        isinstance(facts.get(key), str) and bool(facts[key]) for key in ("resource_id", "generation")
    ):
        return {**result, "reason": "invalid_facts"}
    if policy != {"version": 1, "action": "execute_delete"} or facts.get("action") != "execute_delete":
        return {**result, "reason": "unsupported_policy_or_action"}
    if previous is not None:
        if not isinstance(previous, dict):
            return {**result, "reason": "invalid_previous"}
        for key in ("resource_id", "generation"):
            if key in previous and previous[key] != facts[key]:
                return {**result, "reason": key + "_mismatch"}
    for protection in ("path_escape", "protected_root", "retained", "pinned", "cited", "unique"):
        if facts.get(protection):
            return {**result, "reason": protection}
    if facts.get("generation_matches") is False:
        return {**result, "reason": "generation_mismatch"}
    if facts.get("generation_matches") is not True:
        return {**result, "reason": "generation_unverified"}
    if facts.get("owner_verified") is not True:
        return {**result, "reason": "owner_unverified"}
    if facts.get("process_coverage") != "complete":
        return {**result, "state": "held", "reason": "process_coverage_unknown"}
    for field in ("live_actors", "leases", "locks", "mounts"):
        if not isinstance(facts.get(field), list) or facts[field] != []:
            return {**result, "state": "held", "reason": "resource_in_use"}
    if facts.get("dependency_coverage") != "complete":
        return {**result, "state": "held", "reason": "dependency_coverage_unknown"}
    for field in ("borrowers", "references"):
        if not isinstance(facts.get(field), list) or facts[field] != []:
            return {**result, "state": "held", "reason": "resource_in_use"}
    required = ("source_terminal", "product_preserved", "inventory_complete", "inventory_regenerable")
    if (
        not all(facts.get(key) is True for key in required)
        or not _source_references(facts.get("source_proof_refs"), facts["generation"])
        or not _custody_references(facts.get("custody_refs"))
    ):
        return result
    return {**result, "state": "eligible", "reason": "terminal_product_preserved"}


def render_runtime():
    """Render exact policy sources, a claimed digest, and a JSON command.

    The result can be imported by file or executed under ``python -S -I`` in an
    empty deployment directory. Verify the payload before compiling either
    module; integrity failure returns a typed refusal instead of executing it.
    """
    sources = _source_documents()
    encoded = {name: base64.b64encode(value).decode("ascii") for name, value in sources.items()}
    header = "SOURCES = " + repr(encoded) + "\nEXPECTED_POLICY_DIGEST = " + repr(_digest_documents(sources)) + "\n"
    return header + _RUNTIME


_RUNTIME = '''
import base64
import hashlib
import json
import sys

def _refuse(reason):
    return {'state': 'refused', 'reason': reason, 'paths': [],
            'policy_digest': ACTUAL_POLICY_DIGEST, 'effect_status': 'not_attempted'}

ACTUAL_POLICY_DIGEST = None
_policy = None
try:
    _sources = {name: base64.b64decode(value, validate=True) for name, value in SOURCES.items()}
    ACTUAL_POLICY_DIGEST = hashlib.sha256(b''.join(
        name.encode('ascii') + b'\\0' + _sources[name] for name in sorted(_sources)
    )).hexdigest()
except (ValueError, TypeError, AttributeError, UnicodeError):
    pass
if ACTUAL_POLICY_DIGEST is not None and ACTUAL_POLICY_DIGEST == EXPECTED_POLICY_DIGEST:
    _policy = {'__name__': 'deployed_residue_policy', '__file__': __file__}
    exec(compile(_sources['residue_policy'], '<residue_policy>', 'exec'), _policy)
    _policy['_source_digest'] = lambda: ACTUAL_POLICY_DIGEST
    _content = {'__name__': 'deployed_regenerable_content', '__file__': __file__}
    exec(compile(_sources['regenerable_content'], '<regenerable_content>', 'exec'), _content)
    classify_ignored = _content['classify_ignored']

def evaluate(request):
    if _policy is None:
        return _refuse('policy_digest_mismatch')
    if not isinstance(request, dict):
        return _refuse('invalid_request')
    return _policy['assess_residue'](request.get('facts'), request.get('policy'), request.get('previous'))

if __name__ == '__main__':
    try:
        request = json.load(sys.stdin)
    except (ValueError, UnicodeError, OSError):
        result = _refuse('invalid_request')
    else:
        result = evaluate(request)
    print(json.dumps(result, sort_keys=True))
'''
