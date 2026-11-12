"""One-command, alternating observations of the real job path and plain Codex.

Run with ``python -m workbay_orchestrator_mcp.orchestration.provider_probe``.
Each job reserves normally against a fresh disposable subject. No approvals,
retries, or review-cycle exemptions are requested. Trace is deliberately refused
until the selected executable's lack of file logging can be established.

Reports are a whitelist: provider text is kept only as a known refusal phrase;
any other text is reported as ``message_redacted`` plus its length.
"""

from __future__ import annotations

import argparse
import base64
import collections
import json
import os
import re
import secrets
import shlex
import signal
import subprocess
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

SCHEMA = "workbay/provider-probe@1"
_REFUSALS = (
    "Selected model is at capacity. Please try a different model.",
    "Flex capacity unavailable.",
    "We're currently experiencing high demand, which may cause temporary errors.",
    "Policy-restricted capacity is unavailable",
)
# Server-sent text that is a prefix of a longer sentence (contract section 2).
_REFUSAL_PREFIXES = ("Policy-restricted capacity is unavailable",)
# Diagnostic prefixes a complete stderr line may carry before a known phrase.
_STDERR_PREFIXES = ("ERROR: ", "Error: ", "error: ", "remote_agent: provider_model_refused (retryable): ")
_STDERR_WINDOW_LINES = 3
_STDERR_LINE_MAX = 512
# remote_agent.sh exits 4 after a successful turn that committed nothing.
_RC_NO_CHANGE = 4
_FACT_TYPES = {
    "turn.started": "turn_started",
    "turn.completed": "turn_completed",
    "turn.failed": "turn_failed",
    "item.completed": "items_completed",
}
_ARGV_FLAGS = frozenset(
    {
        "exec",
        "-",
        "--json",
        "--ignore-user-config",
        "-C",
        "--cd",
        "-m",
        "--model",
        "-c",
        "--config",
        "--output-schema",
        "-o",
        "--output-last-message",
        "-s",
        "--sandbox",
    }
)
# Whitelisted value shapes. None admits a URL, header, query or JSON fragment.
_REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_ERROR_CODE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}\Z")
_TOKEN = re.compile(r"[a-z][a-z0-9_.:-]{0,95}\Z")


class ProbeError(Exception):
    """A typed inability to measure; never a provider verdict."""

    def __init__(self, reason: str, *, arms: list[dict[str, Any]] | None = None):
        super().__init__(reason)
        self.arms = arms or []


def _known_refusal(text: str) -> str | None:
    """The fixed refusal phrase wholly contained in ``text``, else None."""
    return next((phrase for phrase in _REFUSALS if phrase in text), None)


def _message_fields(text: Any) -> dict[str, Any]:
    """Report a provider message as a known phrase or as its length only.

    The retained ``message`` is always one of this module's constants, never
    provider bytes; anything else in the text sets ``message_redacted``.
    """
    if not isinstance(text, str):
        return dict(message=None, message_redacted=False, message_length=None)
    known = _known_refusal(text)
    return dict(message=known, message_redacted=text != known, message_length=len(text))


def _reason_fields(value: Any) -> dict[str, Any]:
    """A typed ledger reason identifier, or only the redacted text's length."""
    if value is None or (isinstance(value, str) and _TOKEN.fullmatch(value)):
        return dict(reason=value)
    return dict(reason=None, reason_redacted=True, reason_length=len(value) if isinstance(value, str) else None)


def _safe_token(value: Any) -> str | None:
    return value if isinstance(value, str) and _TOKEN.fullmatch(value) else None


def _safe_id(value: Any) -> str | None:
    if not isinstance(value, str) or not _REQUEST_ID.fullmatch(value) or value.lower().startswith(("sk-", "bearer")):
        return None
    return value


def _safe_code(value: Any) -> str | None:
    if not isinstance(value, str) or not _ERROR_CODE.fullmatch(value) or value.lower().startswith(("sk-", "bearer")):
        return None
    return value


def filter_trace_line(line: str) -> dict[str, Any]:
    """Extract fields, never retain the line or arbitrary header values.

    This filter does not authorize trace mode: file-logging safety is a separate
    precondition. It also handles non-debug diagnostics arriving on the pipe.
    """
    fields: dict[str, Any] = {}
    status = re.search(r'(?i)\b(?:http_status(?:_code)?|status)["\']?\s*[=:]\s*["\']?(\d{3})\b', line)
    if status and 100 <= int(status[1]) <= 599:
        fields["http_status"] = int(status[1])
    for key, names, check in (
        ("request_id", "x-request-id|request_id|cf-ray", _safe_id),
        ("error_code", "error_code|codex_error_info", _safe_code),
    ):
        match = re.search(rf'(?i)\b(?:{names})["\']?\s*[:=]\s*["\']?([A-Za-z0-9_.-]+)', line)
        if match and (value := check(match[1])) is not None:
            fields[key] = value
    return fields


class StderrRefusals:
    """Bounded rolling window of complete stderr lines; nothing is retained.

    A refusal is recognised only when one line, or up to three adjacent lines
    joined by one space, is exactly a known diagnostic (after one fixed
    prefix). A longer line that merely mentions the phrase is narration.
    """

    def __init__(self) -> None:
        self.window: collections.deque[str | None] = collections.deque(maxlen=_STDERR_WINDOW_LINES)
        self.pending = ""
        self.overflow = False
        self.phrase: str | None = None

    def feed(self, text: str) -> None:
        *complete, rest = (self.pending + text).split("\n")
        for line in complete:
            self._line(None if self.overflow else line)
            self.overflow = False
        self.pending = rest
        if len(self.pending) > _STDERR_LINE_MAX:
            self.pending, self.overflow = "", True

    def close(self) -> None:
        # The last line is complete at EOF even without a newline.
        if self.pending or self.overflow:
            self._line(None if self.overflow else self.pending)
        self.pending, self.overflow = "", False

    def _line(self, line: str | None) -> None:
        if line is not None:
            line = line.strip()
            if len(line) > _STDERR_LINE_MAX:
                line = None
        # An unreadable or oversized line breaks adjacency.
        self.window.append(line)
        lines = list(self.window)
        for start in range(len(lines)):
            if any(part is None or not part for part in lines[start:]):
                continue
            joined = " ".join(lines[start:])
            for prefix in _STDERR_PREFIXES:
                if joined.startswith(prefix):
                    joined = joined[len(prefix) :]
                    break
            if joined in _REFUSALS or any(joined.startswith(p) for p in _REFUSAL_PREFIXES):
                self.phrase = _known_refusal(joined)
                return


class StreamFacts:
    """Parse only provider error records; model narration has no authority."""

    def __init__(self) -> None:
        self.counts = {name: 0 for name in _FACT_TYPES.values()}
        self.counts["malformed_lines"] = 0
        self.fields: dict[str, Any] = dict(
            **_message_fields(None), http_status=None, error_code=None, request_id=None
        )
        self.context: dict[str, Any] | None = None
        self.records = 0
        self.refused = False
        self.last_turn: str | None = None
        self.stderr = StderrRefusals()

    def feed_stderr(self, text: str) -> None:
        self.stderr.feed(text)
        self._stderr_refusal()

    def close_stderr(self) -> None:
        self.stderr.close()
        self._stderr_refusal()

    def _stderr_refusal(self) -> None:
        if self.stderr.phrase is not None and not self.refused:
            self.refused = True
            self.fields.update(_message_fields(self.stderr.phrase))

    def feed(self, line: str) -> None:
        if not line.strip():
            return
        try:
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("not an event")
        except ValueError:
            self.counts["malformed_lines"] += 1
            return
        kind = record.get("type")
        if kind == "workbay.provider_probe.context":
            # The payload prints this before the child starts; later copies
            # come from the child and carry no authority.
            if self.context is None and not self.records:
                self.context = record
            return
        self.records += 1
        if kind in _FACT_TYPES:
            self.counts[_FACT_TYPES[kind]] += 1
        if kind in {"turn.started", "turn.completed", "turn.failed"}:
            self.last_turn = kind
        if kind == "error":
            self._error(record)
        elif kind == "turn.failed":
            self._error(record)
        elif kind in {"item.started", "item.completed"}:
            item = record.get("item")
            if isinstance(item, dict) and item.get("type") == "error":
                self._error(item)

    def _error(self, record: dict[str, Any]) -> None:
        # Error envelopes can nest a provider JSON object in message or error.
        pending = [record]
        for _ in range(32):
            if not pending:
                break
            error = pending.pop(0)
            if not isinstance(error, dict):
                continue
            message = error.get("message")
            if isinstance(message, str):
                known = _known_refusal(message) is not None
                # A later unrelated error never replaces an observed refusal.
                if known or not self.refused:
                    self.fields.update(_message_fields(message))
                self.refused |= known
                try:
                    nested = json.loads(message)
                except ValueError:
                    nested = None
                if isinstance(nested, dict):
                    pending.append(nested)
            status = error.get("http_status_code", error.get("http_status"))
            if isinstance(status, (int, str)) and not isinstance(status, bool) and str(status).isdigit():
                if 100 <= int(status) <= 599:
                    self.fields["http_status"] = int(status)
            code = error.get("error_code", error.get("codex_error_info", error.get("code")))
            if isinstance(code, dict):
                code = code.get("code", code.get("type"))
            if (value := _safe_code(code)) is not None:
                self.fields["error_code"] = value
            for key in ("request_id", "x-request-id", "cf-ray"):
                if (value := _safe_id(error.get(key))) is not None:
                    self.fields["request_id"] = value
                    break
            for key in ("error", "headers"):
                if isinstance(error.get(key), dict):
                    pending.append(error[key])

    def no_change_completed(self) -> bool:
        """Positive proof that a wrapper no-change exit followed a clean turn."""
        return (
            not self.refused
            and self.counts["turn_completed"] > 0
            and self.counts["turn_failed"] == 0
            and self.counts["malformed_lines"] == 0
            and self.last_turn == "turn.completed"
        )

    def classification(self, rc: int, *, no_change_proven: bool = False) -> str:
        if rc != 0 and self.refused:
            return "provider_model_refused"
        if rc == 0 and self.counts["turn_completed"] > 0 and self.counts["turn_failed"] == 0:
            return "ok"
        # Only the job wrapper's own no-change status, only with provenance
        # (``no_change_proven``) and a cleanly completed final turn.
        if rc == _RC_NO_CHANGE and no_change_proven and self.no_change_completed():
            return "ok"
        return "agent_failed"


def _literal(raw: str) -> str | None:
    # Match remote_agent.sh's non-executing env reader, including quoted text.
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1] if "'" not in raw[1:-1] else None
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1] if not any(c in raw[1:-1] for c in "$`\\") else None
    return raw if re.fullmatch(r"[A-Za-z0-9@._:/-]*", raw) else None


def resolve_host(root: Path) -> str:
    host = os.environ.get("REMOTE_HOST") or os.environ.get("WORKBAY_REMOTE_GATE_HOST")
    if not host:
        config = root / ".workbay" / "remote-gate.env"
        if config.is_file():
            for line in config.read_text(encoding="utf-8").splitlines():
                line = line.lstrip(" \t")
                if line.startswith(("export ", "export\t")):
                    line = line[6:].lstrip(" \t")
                key, sep, value = line.partition("=")
                if sep and key == "REMOTE_GATE_HOST" and (literal := _literal(value)) is not None:
                    host = literal
    if not host:
        raise ProbeError("remote_host_unconfigured")
    if host.startswith("-") or any(c.isspace() or ord(c) < 32 for c in host):
        raise ProbeError("remote_host_invalid")
    return host


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _subject() -> str:
    return "provider-probe/" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(6)


def _execute(
    argv: list[str],
    *,
    root: Path,
    env: dict[str, str],
    timeout: int,
    stdout: Callable[[str], None],
    stderr: Callable[[str], None],
    payload: bytes | None = None,
) -> int:
    """Drain both pipes concurrently, retaining only callback-selected fields."""
    try:
        process = subprocess.Popen(
            argv,
            cwd=root,
            env=env,
            stdin=subprocess.PIPE if payload else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as exc:
        raise ProbeError("transport_unavailable") from exc
    failures: list[Exception] = []

    def consume(pipe, callback):
        try:
            for raw in pipe:
                callback(raw.decode("utf-8", errors="replace"))
        except Exception as exc:
            failures.append(exc)
        finally:
            pipe.close()

    def send():
        try:
            if process.stdin is not None:
                process.stdin.write(payload)
        except BrokenPipeError:
            pass  # The observed process exit determines whether this ran.
        finally:
            if process.stdin is not None:
                process.stdin.close()

    threads = [
        threading.Thread(target=consume, args=(process.stdout, stdout), daemon=True),
        threading.Thread(target=consume, args=(process.stderr, stderr), daemon=True),
    ]
    if payload:
        threads.append(threading.Thread(target=send, daemon=True))
    for thread in threads:
        thread.start()
    try:
        rc = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        raise ProbeError("transport_timeout") from exc
    finally:
        for thread in threads:
            thread.join(timeout=2)
    if any(thread.is_alive() for thread in threads):
        raise ProbeError("transport_pipe_incomplete")
    if failures:
        raise ProbeError("transport_output_invalid") from failures[0]
    return rc


# Sent as code on ssh's command line; request bytes alone use stdin. Codex's
# stderr is inherited by ssh, with no tee, redirect, log file or capture file.
# This runs a plain Codex invocation, without WorkBay's job/preflight wrapper.
_DIRECT_PROGRAM = r"""
import base64, hashlib, json, os, signal, subprocess, sys, tempfile
from pathlib import Path
request = json.load(sys.stdin)
with tempfile.TemporaryDirectory(prefix="workbay-provider-direct-") as name:
    cwd = Path(name)
    schema, brief, result = cwd/"schema.json", cwd/"brief.txt", cwd/"result.json"
    schema.write_bytes(base64.b64decode(request["schema"], validate=True))
    prompt = base64.b64decode(request["brief"], validate=True)
    brief.write_bytes(prompt)
    # A neutral empty repository avoids adding --skip-git-repo-check to argv.
    subprocess.run(["git", "init", "-q", str(cwd)], check=True, timeout=5,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    env = dict(os.environ)
    env.pop("RUST_LOG", None)
    env.update(request["env"])
    values = {"{brief_file}": str(brief), "{schema_file}": str(schema),
              "{result_file}": str(result), "{brief_inline}": prompt.decode(),
              "{schema_inline}": schema.read_text()}
    argv = [values.get(arg, arg) for arg in request["argv"]]
    # Reuse today's dispatch resolver, not a local copy of its permission policy.
    if any(arg.startswith("permissions.") and ".extends=" in arg for arg in argv):
        from workbay_orchestrator_mcp.orchestration.execution_preflight import materialize_execution_argv
        env["TMPDIR"] = str(cwd)
        argv = materialize_execution_argv(argv, cwd=cwd, output_dir=cwd, env=env)
    print(json.dumps(dict(type="workbay.provider_probe.context", cwd=str(cwd),
                          argv=argv, argv_digest=hashlib.sha256(json.dumps(argv, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                          env_keys=sorted(env), stdin="pipe")), flush=True)
    child = subprocess.Popen([request["binary"], *argv], cwd=cwd, env=env,
                             stdin=subprocess.PIPE, start_new_session=True)
    try:
        child.communicate(prompt, timeout=request["timeout"])
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
        sys.exit(124)
    sys.exit(child.returncode if child.returncode >= 0 else 128-child.returncode)
"""


def _decision(line: str) -> dict[str, Any] | None:
    prefix = "remote_agent: review_cycle "
    if not line.startswith(prefix):
        return None
    try:
        value = json.loads(line[len(prefix) :])
    except ValueError:
        return None
    if not isinstance(value, dict) or _safe_token(value.get("status")) is None:
        return None
    # Ledger rows can contain author environment/proposals: retain typed
    # decisions, not their payloads or free text. No raw stderr is retained.
    decision: dict[str, Any] = {"status": value["status"], **_reason_fields(value.get("reason"))}
    row = value.get("row")
    if isinstance(row, dict):
        kept = {}
        if type(row.get("round")) is int:
            kept["round"] = row["round"]
        for key in ("outcome_class", "settlement_status"):
            if (field := _safe_token(row.get(key))) is not None:
                kept[key] = field
        if kept:
            decision["row"] = kept
    return decision


def _arm(
    arm: str, round_number: int, started: str, rc: int, facts: StreamFacts, *, no_change_proven: bool = False
) -> dict[str, Any]:
    fields = dict(facts.fields)
    if arm == "job":
        fields.update(http_status=None, error_code=None, request_id=None)
    return dict(
        arm=arm,
        round=round_number,
        started_at=started,
        ended_at=_now(),
        exit_code=rc,
        classification=facts.classification(rc, no_change_proven=no_change_proven),
        **fields,
        turn_facts=facts.counts,
        trace="not_supported_job_arm" if arm == "job" else "off",
    )


def _stamp_identity(spec_path: Path, *, branch: str, head_sha: str) -> None:
    """Stamp dispatch identity with the adapter's own writer, then verify it."""
    from .adapters.remote_exec import _stamp_spool_identity

    _stamp_spool_identity(spec_path, branch=branch, head_sha=head_sha)
    try:
        stamped = json.loads(spec_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ProbeError("spec_identity_unavailable") from exc
    # The adapter's writer degrades silently; a probe must not.
    if not isinstance(stamped, dict) or (stamped.get("branch"), stamped.get("head_sha")) != (branch, head_sha):
        raise ProbeError("spec_identity_unavailable")


def _write_private(path: Path, payload: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(payload)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def _dispatch_branch(review_kind: str | None, *, consumer: str, subject: str) -> str:
    """The ``--branch`` and spec ``branch`` identity of one job arm.

    Only ``analysis`` may name the disposable subject: shell_reserve skips
    validate_shell_subject for it and its receipt never retains a subject
    (review_cycle_dispatch.py:1468, :2680-2682). An implement/gate receipt
    stamped with a disposable branch would retain every later unregistered
    dispatch from this commit's descendants. Those kinds keep the consumer's
    branch, as the adapter stamps it, so the unmodified subject guard refuses
    the disposable subject before any receipt exists.
    """
    return subject if review_kind == "analysis" else consumer


def _prepare_job_spec(
    private: Path, name: str, *, rendered: Any, supplied: Path | None, branch: str, head_sha: str
) -> Path:
    """Write one dispatch-identical spec pair into the private directory.

    A rendered default goes through ``write_agent_spec`` (JSON plus the
    NUL-delimited ``.argv`` sidecar). A supplied spec is copied with its own
    sidecar, or one encoded exactly as that writer encodes it. Both are then
    stamped like a spooled dispatch with ``branch`` and ``head_sha``.
    """
    if supplied is None:
        from .backend_spec import write_agent_spec

        try:
            spec_path, _ = write_agent_spec(rendered, private / name)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ProbeError("spec_write_failed") from exc
    else:
        spec_path = private / (name + ".json")
        sidecar = supplied.with_suffix(".argv") if supplied.suffix == ".json" else Path(str(supplied) + ".argv")
        if sidecar.is_file():
            argv_bytes = sidecar.read_bytes()
        else:
            argv = json.loads(supplied.read_text(encoding="utf-8"))["argv"]
            if any("\0" in arg for arg in argv):
                raise ProbeError("unsupported_agent_spec")
            argv_bytes = b"\0".join(arg.encode("utf-8") for arg in argv) + b"\0"
        # Sidecar first: the JSON's existence implies its argv exists.
        _write_private(private / (name + ".argv"), argv_bytes)
        _write_private(spec_path, supplied.read_bytes())
    _stamp_identity(spec_path, branch=branch, head_sha=head_sha)
    return spec_path


def _redacted_argv(argv: list[str]) -> list[str]:
    # Shape plus option names, never arbitrary operator/provider values.
    return [a if a in _ARGV_FLAGS else "<redacted>" for a in argv]


def _profile_diff(spec: dict[str, Any], job_proof: dict[str, Any], direct: dict[str, Any]) -> dict[str, Any]:
    job = dict(
        argv=_redacted_argv(spec["argv"]),
        env_keys=sorted(job_proof.get("environment_keys", [])),
        path_prepend=["<redacted>" for _ in spec.get("path_prepend", [])],
        cwd="job_sandbox" if job_proof.get("cwd") else None,
        stdin="file",
    )
    plain = dict(
        argv=_redacted_argv(direct.get("argv", [])),
        env_keys=sorted(direct.get("env_keys", [])),
        path_prepend=[],
        cwd="neutral_directory" if direct.get("cwd") else None,
        stdin=direct.get("stdin"),
    )
    diff = {key: {"job": job[key], "direct": plain[key]} for key in job if job[key] != plain[key]}
    # Redaction can hide differing values with the same shape. Compare the
    # model-boundary digest, rather than treating redacted equality as evidence.
    job_digest = job_proof.get("live_context", {}).get("argv_digest")
    direct_digest = direct.get("argv_digest")
    if job_digest and direct_digest and job_digest != direct_digest:
        diff["argv"] = dict(job=job["argv"], direct=plain["argv"], materialized_values_differ=True)
    return diff


def _verdict(arms: list[dict[str, Any]]) -> str:
    kinds = {name: {a["classification"] for a in arms if a["arm"] == name} for name in ("job", "direct")}
    if any({"ok", "provider_model_refused"}.issubset(values) for values in kinds.values()):
        return "mixed_time_dependent"
    job = kinds["job"] == {"provider_model_refused"}
    direct = kinds["direct"] == {"provider_model_refused"}
    if job and direct:
        return "both_refused"
    if job:
        return "job_only_refused"
    if direct:
        return "direct_only_refused"
    return "both_ok"


def _validate_spec(spec: Any) -> None:
    if (
        not isinstance(spec, dict)
        or spec.get("backend_id") != "codex-remote"
        or not isinstance(spec.get("binary"), str)
        or not spec["binary"]
    ):
        raise ProbeError("unsupported_agent_spec")
    argv = spec.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv) or argv[0] != "exec":
        raise ProbeError("unsupported_agent_spec")
    for index, arg in enumerate(argv):
        if arg in {"-C", "--cd"} and (index + 1 == len(argv) or argv[index + 1] != "."):
            raise ProbeError("unsupported_cwd")
        if arg.startswith("--cd=") and arg != "--cd=.":
            raise ProbeError("unsupported_cwd")
        if arg.startswith("-C") and arg != "-C":
            raise ProbeError("unsupported_cwd")
    if spec.get("stdin") != "{brief_file}" or argv[-1] != "-":
        raise ProbeError("unsupported_stdin")
    allowed = {"{brief_file}", "{brief_inline}", "{schema_file}", "{schema_inline}", "{result_file}"}
    if any(re.search(r"\{[A-Za-z_]+\}", arg) and arg not in allowed for arg in argv):
        raise ProbeError("unsupported_placeholder")
    env = spec.get("env", {})
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise ProbeError("unsupported_agent_env")
    if "RUST_LOG" in env or spec.get("env_file"):
        raise ProbeError("trace_unsafe")


def _run(args: argparse.Namespace) -> dict[str, Any]:
    # Strings in the supplied 0.159.2 executable expose log_dir/file logging,
    # but cannot prove its absence in exec mode. Never probe that with tracing.
    if args.trace:
        raise ProbeError("trace_unsafe")
    if args.backend != "codex-remote":
        raise ProbeError("unsupported_backend")
    if args.rounds < 1 or args.timeout < 1:
        raise ProbeError("invalid_bounds")
    triplet = (args.agent_spec, args.schema, args.brief)
    if any(triplet) and not all(triplet):
        raise ProbeError("input_triplet_required")
    git = subprocess.run(
        ["git", "rev-parse", "--show-toplevel", "HEAD", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if git.returncode or len(lines := git.stdout.splitlines()) != 3 or lines[2] == "HEAD":
        raise ProbeError("current_branch_unavailable")
    root, head_sha, branch = Path(lines[0]), lines[1], lines[2]
    host = resolve_host(root)
    remote_agent = root / "scripts" / "remote_agent.sh"
    if not remote_agent.is_file():
        raise ProbeError("job_runner_unavailable")
    env = dict(os.environ)
    env.pop("RUST_LOG", None)
    env["WORKBAY_REMOTE_GATE_HOST"] = host
    # An inherited authority token is scoped to the consumer, not this probe.
    # Let remote_agent reserve/settle normally for the fresh subject.
    env.pop("WORKBAY_REVIEW_CYCLE_TOKEN", None)
    with tempfile.TemporaryDirectory(prefix="workbay-provider-probe-") as directory:
        private = Path(directory)
        private.chmod(0o700)
        rendered = None
        if all(triplet):
            supplied, schema_path, brief_path = (Path(p).resolve(strict=True) for p in triplet)
            spec = json.loads(supplied.read_text(encoding="utf-8"))
        else:
            from .backend_spec import build_agent_spec

            supplied = None
            try:
                rendered = build_agent_spec(
                    args.backend, model=args.model, effort=args.effort, speed=args.speed, workspace_root=root
                )
                spec = rendered.to_json_dict()
            except (ValueError, RuntimeError) as exc:
                raise ProbeError("spec_render_failed") from exc
            schema_path, brief_path = (private / name for name in ("schema.json", "brief.txt"))
            schema_path.write_text(
                json.dumps(
                    {
                        "type": "object",
                        "properties": {"ok": {"type": "boolean"}},
                        "required": ["ok"],
                        "additionalProperties": False,
                    }
                ),
                encoding="utf-8",
            )
            brief_path.write_text("ping", encoding="utf-8")
        _validate_spec(spec)
        brief_bytes, schema_bytes = brief_path.read_bytes(), schema_path.read_bytes()
        arms: list[dict[str, Any]] = []
        proof: dict[str, Any] = {}
        differences: dict[str, Any] = {}
        for round_number in range(1, args.rounds + 1):
            order = ("job", "direct") if round_number % 2 else ("direct", "job")
            for name in order:
                started, facts = _now(), StreamFacts()
                if name == "job":
                    subject = _subject()
                    outputs = {
                        key: private / f"r{round_number}-{key}"
                        for key in ("result-out", "stream-out", "provenance-out", "debug-out")
                    }
                    identity = _dispatch_branch(args.review_kind, consumer=branch, subject=subject)
                    spec_path = _prepare_job_spec(
                        private,
                        f"r{round_number}-agent",
                        rendered=rendered,
                        supplied=supplied,
                        branch=identity,
                        head_sha=head_sha,
                    )
                    argv = [
                        str(remote_agent),
                        "build",
                        "--branch",
                        identity,
                        "--agent-spec",
                        str(spec_path),
                        "--schema",
                        str(schema_path),
                        "--brief",
                        str(brief_path),
                    ]
                    for flag, path in outputs.items():
                        argv.extend(("--" + flag, str(path)))
                    argv.extend(("--timeout", str(args.timeout), "--review-subject-branch", subject))
                    if args.review_kind:
                        argv.extend(("--review-kind", args.review_kind))
                    job_env = dict(env)
                    job_env.setdefault("WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH", "1")
                    job_env.setdefault(
                        "WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON",
                        f"workbay provider-probe transient ref {subject}",
                    )
                    decisions = []
                    branch_guard_length = None

                    def job_stderr(line, facts=facts, decisions=decisions):
                        nonlocal branch_guard_length
                        # scripts/hooks/check_branch_naming.py:_format_pre_push_block
                        if line.startswith("BLOCKED: Cannot push '"):
                            branch_guard_length = len(line.rstrip("\r\n"))
                        if (decision := _decision(line)) is not None:
                            decisions.append(decision)
                        facts.feed_stderr(line)

                    rc = _execute(
                        argv,
                        root=root,
                        env=job_env,
                        timeout=args.timeout + 60,
                        stdout=lambda line: None,
                        stderr=job_stderr,
                    )
                    facts.close_stderr()
                    reserve = decisions[0] if decisions else None
                    ledger = {"reserve": reserve, "settle": decisions[-1] if len(decisions) > 1 else None}

                    def job_arm(no_change_proven=False, ledger=ledger, subject=subject, rc=rc, facts=facts):
                        arm = _arm(name, round_number, started, rc, facts, no_change_proven=no_change_proven)
                        arm.update(review_subject_branch=subject, ledger=ledger)
                        return arm

                    if branch_guard_length is not None:
                        arm = job_arm()
                        arm.update(
                            classification="job_arm_refused:branch_guard",
                            message=None, message_redacted=True, message_length=branch_guard_length,
                        )
                        raise ProbeError("job_arm_refused:branch_guard", arms=[*arms, arm])
                    if reserve is None:
                        arm = job_arm(ledger={"reserve": None, "settle": None})
                        raise ProbeError("job_arm_refused:reserve_unavailable", arms=[*arms, arm])
                    if reserve["status"] != "reserved":
                        reason = reserve.get("reason") or reserve["status"]
                        arm = job_arm(ledger={"reserve": reserve, "settle": None})
                        arm.update(classification="job_arm_refused:" + reason)
                        raise ProbeError("job_arm_refused:" + reason, arms=[*arms, arm])
                    arm = job_arm()
                    if not outputs["stream-out"].is_file():
                        raise ProbeError("job_stream_unavailable", arms=[*arms, arm])
                    with outputs["stream-out"].open(encoding="utf-8", errors="replace") as stream:
                        for line in stream:
                            facts.feed(line)
                    if not facts.records:
                        raise ProbeError("job_stream_unavailable", arms=[*arms, arm])
                    previous_binary = proof.get("binary")
                    try:
                        provenance = json.loads(outputs["provenance-out"].read_text(encoding="utf-8"))
                        proof = provenance["execution_preflight"]
                        binary = proof["binary"]["path"]
                        if not isinstance(binary, str) or not binary.startswith("/"):
                            raise ValueError("no absolute binary")
                    except (OSError, ValueError, KeyError, TypeError) as exc:
                        raise ProbeError("job_provenance_unavailable", arms=[*arms, job_arm()]) from exc
                    if previous_binary is not None and previous_binary != proof["binary"]:
                        raise ProbeError("job_binary_changed", arms=[*arms, job_arm()])
                    # Provenance is proven here; only now may exit 4 read as
                    # the wrapper's no-change harvest of a completed turn.
                    arm = job_arm(no_change_proven=True)
                else:
                    request = dict(
                        binary=proof["binary"]["path"],
                        argv=spec["argv"],
                        env=spec.get("env", {}),
                        brief=base64.b64encode(brief_bytes).decode(),
                        schema=base64.b64encode(schema_bytes).decode(),
                        timeout=args.timeout,
                    )
                    interpreter_info = proof.get("interpreter") or {}
                    interpreter = (
                        interpreter_info.get("executable", "python3")
                        if isinstance(interpreter_info, dict)
                        else "python3"
                    )
                    if not isinstance(interpreter, str) or not interpreter:
                        raise ProbeError("job_provenance_unavailable", arms=arms)
                    command = shlex.join([interpreter, "-c", _DIRECT_PROGRAM])
                    argv = [
                        "ssh",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ConnectTimeout=10",
                        "-o",
                        "ServerAliveInterval=30",
                        "-o",
                        "ServerAliveCountMax=4",
                        host,
                        command,
                    ]

                    def diagnostic(line, facts=facts):
                        facts.fields.update(filter_trace_line(line))
                        facts.feed_stderr(line)

                    rc = _execute(
                        argv,
                        root=root,
                        env=env,
                        timeout=args.timeout + 30,
                        payload=json.dumps(request).encode(),
                        stdout=facts.feed,
                        stderr=diagnostic,
                    )
                    facts.close_stderr()
                    if not facts.records or not facts.context:
                        raise ProbeError(
                            "direct_transport_failed", arms=[*arms, _arm(name, round_number, started, rc, facts)]
                        )
                    arm = _arm(name, round_number, started, rc, facts)
                    differences.update(_profile_diff(spec, proof, facts.context))
                if arm["classification"] == "agent_failed":
                    raise ProbeError(name + "_arm_failed", arms=[*arms, arm])
                arms.append(arm)
        verdict = _verdict(arms)
        return dict(
            schema=SCHEMA,
            arms=arms,
            variables=differences,
            verdict=verdict,
            named_variable=",".join(sorted(differences))
            if verdict in {"job_only_refused", "direct_only_refused"}
            else None,
        )


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ProbeError("invalid_arguments")


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--backend", default="codex-remote")
    parser.add_argument("--model", required=True)
    parser.add_argument("--effort", required=True)
    parser.add_argument("--speed", default="standard")
    parser.add_argument("--agent-spec")
    parser.add_argument("--schema")
    parser.add_argument("--brief")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--review-kind", choices=("implement", "gate", "analysis"))
    parser.add_argument("--trace", action="store_true", help="refused until no-file-logging safety is proven")
    parser.add_argument("--json", action="store_true")
    try:
        report = _run(parser.parse_args(argv))
    except ProbeError as exc:
        print(json.dumps(dict(schema=SCHEMA, status="error", reason=str(exc), arms=exc.arms)))
        return 1
    except (OSError, ValueError, subprocess.SubprocessError):
        print(json.dumps(dict(schema=SCHEMA, status="error", reason="probe_input_or_transport_invalid")))
        return 1
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
