"""One-shot trusted-launcher stdio adapter for the wave claim authority.

This binds cooperative same-UID sessions, not hostile tenants. A supervisor must
bound stdin/storage wall time. No retries or authority fallback are performed.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import re
import sys
from pathlib import Path

REQUEST_LIMIT = 16 * 1024
RESPONSE_LIMIT = 64 * 1024
CONFIG_LIMIT = 1024 * 1024
SCOPE = ("authority_id", "authority_epoch", "task_ref", "wave_id")


def _refusal(code):
    error = {"code": code}
    if code == "outcome_unknown":
        error["message"] = "Replay the identical request with the same operation_id and binding."
    return {"ok": False, "error": error}, (3 if code in {"outcome_unknown", "unavailable"} else 2)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("nonfinite number")


def _float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite number")
    return result


def _decode(raw):
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float)


def _binding(binding):
    if not isinstance(binding, dict):
        raise ValueError("binding")
    root = binding["root"]
    if not isinstance(root, str) or not Path(root).is_absolute():
        raise ValueError("root")
    path = Path(root)
    if str(path.resolve(strict=True)) != root or not path.is_dir():
        raise ValueError("noncanonical root")
    authority = binding["authority"]
    if not isinstance(authority, dict) or set(authority) != set(SCOPE):
        raise ValueError("authority")
    for key in ("authority_id", "task_ref", "wave_id"):
        if not isinstance(authority[key], str) or not authority[key].strip():
            raise ValueError("scope")
    if type(authority["authority_epoch"]) is not int or authority["authority_epoch"] < 1:
        raise ValueError("epoch")
    for key in ("principal", "session_id"):
        if not isinstance(binding[key], str) or not binding[key].strip():
            raise ValueError("identity")
    return path


def binding_digest(binding):
    """Return the stable fingerprint of a validated resolved binding.

    Only the authority route and its principal/session are included. Extra
    trusted configuration keys are deliberately excluded from the digest.
    """
    _binding(binding)
    pinned = {
        "root": binding["root"],
        "authority": binding["authority"],
        "principal": binding["principal"],
        "session_id": binding["session_id"],
    }
    canonical = json.dumps(pinned, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode(
        "ascii"
    )
    return hashlib.sha256(canonical).hexdigest()


def _encode(response):
    return (json.dumps(response, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n").encode("ascii")


def serve(stdin, binding, *, execute_fn=None):
    """Return (envelope, exit status); validate everything before entering core.

    stdin is a binary stream. execute_fn has the frozen core signature. Identity
    and scope are supplied by trusted launcher configuration, never the request.
    """
    try:
        root = _binding(binding)
    except Exception:
        return _refusal("binding_invalid")
    try:
        raw = stdin.read(REQUEST_LIMIT + 1)
        if len(raw) > REQUEST_LIMIT:
            return _refusal("invalid_request")
        request = _decode(raw)
        if not isinstance(request, dict):
            return _refusal("invalid_request")
        if {"root", "principal", "session_id", "owner"} & request.keys():
            return _refusal("invalid_request")
        for key in SCOPE:
            if key in request and (
                type(request[key]) is not type(binding["authority"][key]) or request[key] != binding["authority"][key]
            ):
                return _refusal("invalid_request")
    except Exception:
        return _refusal("invalid_request")
    if execute_fn is None:
        try:
            from .coordclaims import execute

            execute_fn = execute
        except Exception:
            return _refusal("unavailable")
    try:
        response = execute_fn(
            root,
            request,
            authority=dict(binding["authority"]),
            principal=binding["principal"],
            session_id=binding["session_id"],
        )
        if (
            not isinstance(response, dict)
            or type(response.get("schema_version")) is not int
            or response["schema_version"] != 1
            or type(response.get("ok")) is not bool
        ):
            return _refusal("outcome_unknown")
        if len(_encode(response)) > RESPONSE_LIMIT:
            return _refusal("outcome_unknown")
        # Storage may fail after journal commit. Preserve the core body while
        # directing the caller to replay the same operation_id and binding.
        if response.get("error_code") == "storage_error":
            return response, 3
        return response, (0 if response["ok"] else 2)
    except Exception:
        # Core may already have committed a receipt. Never assert rollback.
        return _refusal("outcome_unknown")


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError("arguments")


def main(argv=None):
    try:
        parser = _Parser(description=__doc__)
        parser.add_argument("--bindings", required=True)
        parser.add_argument("--binding", required=True)
        parser.add_argument("--binding-sha256")
        args = parser.parse_args(argv)
        if args.binding_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", args.binding_sha256):
            raise ValueError("binding digest")
        config_path = Path(args.bindings)
        if not config_path.is_absolute():
            raise ValueError("absolute config required")
        with config_path.open("rb") as stream:
            raw = stream.read(CONFIG_LIMIT + 1)
        if len(raw) > CONFIG_LIMIT:
            raise ValueError("config size")
        config = _decode(raw)
        if type(config["schema_version"]) is not int or config["schema_version"] != 1:
            raise ValueError("schema")
        binding = config["bindings"][args.binding]
        if args.binding_sha256 is not None:
            actual_digest = binding_digest(binding)
            if not hmac.compare_digest(args.binding_sha256, actual_digest):
                response, status = _refusal("binding_mismatch")
                binding = None
    except Exception:
        response, status = _refusal("binding_invalid")
    else:
        if binding is not None:
            response, status = serve(sys.stdin.buffer, binding)
    sys.stdout.buffer.write(_encode(response))
    return status


if __name__ == "__main__":
    sys.exit(main())
