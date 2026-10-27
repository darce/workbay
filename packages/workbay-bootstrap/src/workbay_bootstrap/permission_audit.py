"""Lint a consumer Claude ``settings.local.json`` against the managed policy.

``audit`` is pure. ``--propose`` writes a separate pruned document. Nothing
in this module rewrites ``settings.local.json``.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import secrets
import shlex
import stat
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

Finding = dict[str, str]


class PermissionAuditInputError(ValueError):
    """The consumer permission subtree has an invalid shape."""


class _UnsafeProposalDestination(Exception):
    """The proposal path aliases a protected or unsafe destination."""


KIND_ORDER = (
    "broad_grant",
    "shadowed_by_policy",
    "mcp_path_bypass",
    "mcp_bypass_unresolved",
    "stale_path",
    "one_shot",
    "fixed_cap_env",
)

# Dropped from a proposal. Broad grants and fixed-cap env keys stay so the
# operator can decide; they are still reported.
DROP_KINDS = frozenset(
    {
        "shadowed_by_policy",
        "mcp_path_bypass",
        "mcp_bypass_unresolved",
        "stale_path",
        "one_shot",
    }
)

_CANON = {
    "broad_grant": "SEC-04 lexicons/security.md:69; SECD-02 lexicons/security.md:252",
    "shadowed_by_policy": "SECD-05 lexicons/security.md:255",
    "mcp_path_bypass": "SECD-03 lexicons/security.md:253",
    "mcp_bypass_unresolved": "SECD-05 lexicons/security.md:255",
    "stale_path": "P6 PRINCIPLES.md:59",
    "one_shot": "",
    "fixed_cap_env": "COST-03 docs/canon/CANON-internal.md:75",
}

# Longest first so ``python3`` and ``.venv/bin/python`` win over ``python``.
_BROAD_TOOLS = (
    ".venv/bin/python",
    "python3",
    "python",
    "uv run",
    "bash",
    "sh",
    "ssh",
    "curl",
    "cat",
    "cd",
    "find",
    "xargs",
    "rm",
    "node",
    "npx",
    "npm",
    "git",
)

_BROAD_SENTINELS = frozenset({"Bash(*)", "Read(//**)"})

_MCP_TOOL_PREFIX = {
    "mcp-workbay-handoff": "mcp__workbay-handoff-mcp__",
    "mcp-workbay-orchestrator": "mcp__workbay-orchestrator-mcp__",
}

_ABS_PATH = re.compile(r"(?:^|[\s(\"'])(/[^\s\"'`;|&<>()*?]+)")
_SHELL_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$", re.DOTALL)
_EXECUTION_WRAPPERS = frozenset({"env", "command", "exec", "nohup", "nice", "timeout"})
_BROAD_EXECUTABLES = frozenset(tool for tool in _BROAD_TOOLS if " " not in tool and "/" not in tool)

LOCAL_SETTINGS_REL = Path(".claude") / "settings.local.json"


def load_managed_policy() -> dict[str, Any]:
    """Load the packaged Claude permission policy (schema 1)."""
    from workbay_system import data_root

    path = data_root() / "config" / "claude-permissions" / "policy.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def audit(local_doc: Mapping[str, Any] | Any, policy: Mapping[str, Any] | Any) -> list[Finding]:
    """Return one finding per offending allow rule or fixed-cap env key.

    A rule matches at most one kind. Dead, bypass, and stale rules outrank a
    broad grant or a one-shot label so the proposal drops them once.
    """
    if not isinstance(local_doc, Mapping) or not isinstance(policy, Mapping):
        return []
    restrictions = _restrictions(policy)
    typed = _typed_mcp_binaries(policy)
    findings: list[Finding] = []
    for index, rule in enumerate(_allow_rules(local_doc)):
        finding = _classify_rule(rule, f"permissions.allow[{index}]", restrictions, typed)
        if finding is not None:
            findings.append(finding)
    findings.extend(_fixed_cap_findings(local_doc))
    return findings


def propose(local_doc: Mapping[str, Any] | Any, findings: list[Finding]) -> dict[str, Any]:
    """Return a pruned copy. Broad grants and env keys are kept."""
    if isinstance(local_doc, Mapping):
        pruned: dict[str, Any] = deepcopy(dict(local_doc))
    else:
        pruned = {}
    drop_indices: set[int] = set()
    for finding in findings:
        if finding.get("kind") not in DROP_KINDS:
            continue
        match = re.fullmatch(r"permissions\.allow\[(\d+)\]", finding.get("rule_id", ""))
        if match is not None:
            drop_indices.add(int(match.group(1)))
    permissions = pruned.get("permissions")
    if isinstance(permissions, dict):
        allow = permissions.get("allow")
        if isinstance(allow, list):
            permissions["allow"] = [rule for index, rule in enumerate(allow) if index not in drop_indices]
    return pruned


def run_permissions_audit(target: Path, *, emit_json: bool, propose_path: Path | None) -> int:
    """Print findings and optionally write a proposal. Never edits the source."""
    target = Path(target)
    local_path = target / LOCAL_SETTINGS_REL
    try:
        local_doc = _read_local_doc(local_path)
        policy = load_managed_policy()
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"permissions-audit: {exc}", file=sys.stderr)
        return 1
    try:
        findings = audit(local_doc, policy)
    except PermissionAuditInputError as exc:
        unknown = _unknown_finding(str(exc))
        if emit_json:
            print(json.dumps([unknown], indent=2))
        else:
            print(_format_finding(unknown))
        return 2
    if emit_json:
        print(json.dumps(findings, indent=2))
    elif findings:
        for finding in findings:
            print(_format_finding(finding))
    else:
        print("permissions-audit: no findings.")
    if propose_path is not None:
        destination = Path(propose_path)
        proposal = json.dumps(propose(local_doc, findings), indent=2) + "\n"
        try:
            _write_proposal(destination, local_path, proposal.encode("utf-8"))
        except _UnsafeProposalDestination:
            print("permissions-audit: refusing unsafe proposal destination", file=sys.stderr)
            return 2
        except OSError:
            print("permissions-audit: failed to write proposal", file=sys.stderr)
            return 2
        print(f"permissions-audit: wrote proposal {destination}", file=sys.stderr)
    return 0


def _write_proposal(destination: Path, source: Path, payload: bytes) -> None:
    """Publish through a fresh inode relative to a pinned destination directory."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    parent = destination.parent.resolve(strict=True)
    name = destination.name
    if not name or name in {".", ".."}:
        raise _UnsafeProposalDestination
    source_name = source.parent.resolve(strict=False) / source.name
    if parent / name == source_name:
        raise _UnsafeProposalDestination

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(parent, flags)
    temporary_name: str | None = None
    temporary_fd: int | None = None
    try:
        _reject_unsafe_destination(directory_fd, name, source)
        for _attempt in range(10):
            candidate = f".{name}.{secrets.token_hex(8)}.tmp"
            try:
                temporary_fd = os.open(
                    candidate,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=directory_fd,
                )
            except FileExistsError:
                continue
            temporary_name = candidate
            break
        if temporary_fd is None or temporary_name is None:
            raise OSError("could not create a temporary proposal file")

        with os.fdopen(temporary_fd, "wb") as stream:
            temporary_fd = None
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())

        _reject_unsafe_destination(directory_fd, name, source)
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temporary_name = None
        os.fsync(directory_fd)
    finally:
        if temporary_fd is not None:
            os.close(temporary_fd)
        if temporary_name is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


def _reject_unsafe_destination(directory_fd: int, name: str, source: Path) -> None:
    try:
        destination_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(destination_stat.st_mode):
        raise _UnsafeProposalDestination
    try:
        source_stat = source.stat()
    except FileNotFoundError:
        return
    if (destination_stat.st_dev, destination_stat.st_ino) == (source_stat.st_dev, source_stat.st_ino):
        raise _UnsafeProposalDestination


def _read_local_doc(local_path: Path) -> dict[str, Any]:
    if not local_path.is_file():
        return {}
    payload = json.loads(local_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{local_path} must be a JSON object")
    return payload


def _format_finding(finding: Finding) -> str:
    canon = f" [{finding['canon']}]" if finding["canon"] else ""
    return f"{finding['kind']} [{finding['rule_id']}]: {finding['rule']} — {finding['detail']}{canon}"


def _allow_rules(local_doc: Mapping[str, Any]) -> list[str]:
    if "permissions" not in local_doc:
        return []
    permissions = local_doc["permissions"]
    if not isinstance(permissions, Mapping):
        raise PermissionAuditInputError("permissions must be an object")
    if "allow" not in permissions:
        return []
    allow = permissions["allow"]
    if not isinstance(allow, list):
        raise PermissionAuditInputError("permissions.allow must be a list")
    if any(not isinstance(rule, str) for rule in allow):
        raise PermissionAuditInputError("permissions.allow entries must be strings")
    return allow


def _rules(policy: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rules = policy.get("rules")
    if not isinstance(rules, list):
        return []
    return [rule for rule in rules if isinstance(rule, Mapping) and isinstance(rule.get("rule"), str)]


def _restrictions(policy: Mapping[str, Any]) -> list[tuple[str, str]]:
    ranked = {"deny": 0, "ask": 1}
    rows: list[tuple[int, int, str, str]] = []
    for index, rule in enumerate(_rules(policy)):
        effect = rule.get("effect")
        if effect not in ranked:
            continue
        rows.append((ranked[str(effect)], index, str(rule["rule"]), str(effect)))
    rows.sort()
    return [(pattern, effect) for _rank, _index, pattern, effect in rows]


def _typed_mcp_binaries(policy: Mapping[str, Any]) -> set[str]:
    present: set[str] = set()
    for rule in _rules(policy):
        name = str(rule["rule"])
        for binary, prefix in _MCP_TOOL_PREFIX.items():
            if name.startswith(prefix):
                present.add(binary)
    return present


def _classify_rule(
    rule: str,
    rule_id: str,
    restrictions: list[tuple[str, str]],
    typed_binaries: set[str],
) -> Finding | None:
    mcp_bypass = _mcp_binary(rule, typed_binaries)
    if mcp_bypass is not None:
        kind, binary = mcp_bypass
        if kind == "mcp_bypass_unresolved":
            detail = f"Bash grant contains {binary}, but its executable cannot be established safely"
        else:
            detail = f"Bash grant invokes {binary} where a typed MCP tool exists"
        finding = _finding(
            kind,
            rule_id,
            _public_rule_description(rule),
            detail,
        )
        if _print_mcp_argument_status(rule, typed_binaries) == "destination_not_identifier":
            finding["print_exemption_refusal"] = "destination_not_identifier"
        return finding
    if _print_mcp_argument_status(rule, typed_binaries) == "print_only":
        return None
    effect = _covering_effect(rule, restrictions)
    if effect is not None:
        return _finding(
            "shadowed_by_policy",
            rule_id,
            _public_rule_description(rule),
            f"local allow is covered by a policy {effect} and does not grant the command",
        )
    if rule.strip() not in _BROAD_SENTINELS:
        missing = _missing_absolute_paths(rule)
        if missing:
            return _finding(
                "stale_path",
                rule_id,
                _public_rule_description(rule),
                "absolute path does not exist",
            )
    if _is_broad(rule):
        return _finding("broad_grant", rule_id, _public_rule_description(rule), "prefix grant on a general tool")
    if _is_one_shot(rule):
        return _finding("one_shot", rule_id, _public_rule_description(rule), "literal Bash rule with no wildcard")
    return None


def _fixed_cap_findings(local_doc: Mapping[str, Any]) -> list[Finding]:
    env = local_doc.get("env")
    if not isinstance(env, Mapping):
        return []
    findings: list[Finding] = []
    for index, key in enumerate(sorted(str(name) for name in env if isinstance(name, str))):
        if _is_fixed_cap_env(key):
            findings.append(
                _finding(
                    "fixed_cap_env",
                    f"env[{index}]",
                    "fixed-cap environment key",
                    "fixed cap env; admission is measured, not a count",
                )
            )
    return findings


def _finding(kind: str, rule_id: str, description: str, detail: str) -> Finding:
    return {
        "kind": kind,
        "rule_id": rule_id,
        "rule": description,
        "canon": _CANON[kind],
        "detail": detail,
    }


def _unknown_finding(detail: str) -> Finding:
    return {
        "kind": "unknown",
        "rule_id": "permissions",
        "rule": "permissions subtree",
        "canon": "",
        "detail": detail,
    }


def _public_rule_description(rule: str) -> str:
    if _bash_body(rule) is not None:
        return "Bash command permission"
    return "permission rule"


def _bash_body(rule: str) -> str | None:
    text = rule.strip()
    if not (text.startswith("Bash(") and text.endswith(")")):
        return None
    return text[len("Bash(") : -1].strip()


def _is_broad(rule: str) -> bool:
    text = rule.strip()
    if text in _BROAD_SENTINELS:
        return True
    body = _bash_body(text)
    if body is None:
        return False
    try:
        segments = _shell_segments(body)
    except ValueError:
        return False
    for segment in segments:
        if segment[-1] == "*":
            prefix = segment[:-1]
        elif segment[-1].endswith(":*"):
            prefix = [*segment[:-1], segment[-1][:-2]]
        else:
            continue
        resolved = _resolve_executable(prefix)
        if resolved is None:
            continue
        executable, arguments = resolved
        executable_name = _executable_basename(executable)
        if (executable_name in _BROAD_EXECUTABLES and not arguments) or (
            executable_name == "uv" and arguments == ["run"]
        ):
            return True
    return False


def _is_one_shot(rule: str) -> bool:
    if _bash_body(rule) is None:
        return False
    return "*" not in rule and "?" not in rule


def _mcp_binary(rule: str, typed_binaries: set[str]) -> tuple[str, str] | None:
    body = _bash_body(rule)
    if body is None:
        return None
    invocation = body.strip()
    for suffix in (":*", " *"):
        if invocation.endswith(suffix):
            invocation = invocation[: -len(suffix)].rstrip()
    try:
        segments = _shell_segments(invocation)
    except ValueError:
        segments = []
    for segment in segments:
        resolved = _resolve_executable(segment)
        if resolved is not None:
            binary = _token_mcp_name(resolved[0], typed_binaries)
            if binary is not None:
                return "mcp_path_bypass", binary
    candidates = _typed_mcp_tokens(invocation, typed_binaries)
    if not candidates:
        return None
    candidate = candidates[0]
    if _print_mcp_argument_status(rule, typed_binaries) == "print_only":
        return None
    return "mcp_bypass_unresolved", candidate


def _print_mcp_argument_status(rule: str, typed_binaries: set[str]) -> str:
    """Decide the print exemption and expose a typed destination refusal.

    Bash re-evaluates array subscripts in printf -v destinations even when
    single-quoted. Only a static plain identifier can receive this exemption.
    """
    body = _bash_body(rule)
    if body is None:
        return "not_print_only"
    try:
        segments = _shell_segments(body)
    except ValueError:
        return "not_print_only"
    if len(segments) != 1:
        return "not_print_only"
    tokens = segments[0]
    if tokens[0] not in {"echo", "printf"}:
        return "not_print_only"
    if tokens[0] == "printf":
        index = 1
        while index < len(tokens):
            option = tokens[index]
            if option == "--" or not option.startswith("-") or option == "-":
                break
            if option.startswith("-v"):
                destination = option[2:]
                if not destination:
                    index += 1
                    destination = tokens[index] if index < len(tokens) else ""
                # shlex removed one quote layer; nested quotes remain invalid.
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", destination) is None:
                    return "destination_not_identifier"
            else:
                # Unknown options cannot establish inert builtin behavior.
                return "not_print_only"
            index += 1
    if _has_shell_expansion(body):
        return "not_print_only"
    # Control operators split segments; quoted punctuation stays a static word.
    # Redirection operators remain in the segment and are not print arguments.
    arguments = tokens[1:]
    if any(not argument or all(char in "<>" for char in argument) for argument in arguments):
        return "not_print_only"
    if any(_token_mcp_name(argument, typed_binaries) is not None for argument in arguments):
        return "print_only"
    return "not_print_only"


def _has_shell_expansion(text: str) -> bool:
    quote: str | None = None
    escaped = False
    for char in text:
        if quote == "'":
            if char == "'":
                quote = None
            continue
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if quote == '"':
            if char == '"':
                quote = None
            elif char in "$`":
                return True
            continue
        if char in "'\"":
            quote = char
            continue
        if char in "$`*?[":
            return True
    return False


def _resolve_executable(invocation: str | list[str]) -> tuple[str, list[str]] | None:
    """Resolve the command after a bounded set of wrappers and assignments.

    Unknown syntax deliberately returns ``None``; callers must not treat that
    as proof that a grant cannot invoke a sensitive executable.
    """
    try:
        tokens = _shell_tokens(invocation) if isinstance(invocation, str) else invocation
    except ValueError:
        return None
    index = _skip_assignments(tokens, 0)
    for _depth in range(len(tokens) + 1):
        if index >= len(tokens):
            return None
        executable = tokens[index]
        if "$" in executable or "`" in executable or all(char in ";&|()<>\n" for char in executable):
            return None
        wrapper = _executable_basename(executable)
        if wrapper not in _EXECUTION_WRAPPERS:
            return executable, tokens[index + 1 :]
        next_index = _skip_wrapper(tokens, index, wrapper)
        if next_index is None:
            return None
        index = _skip_assignments(tokens, next_index)
    return None


def _skip_wrapper(tokens: list[str], index: int, wrapper: str) -> int | None:
    """Return the wrapped command index, consuming the wrapper's own options."""
    if wrapper == "env":
        return _skip_env_options(tokens, index + 1)
    if wrapper == "command":
        return _skip_simple_options(tokens, index + 1, {"-p"})
    if wrapper == "exec":
        return _skip_exec_options(tokens, index + 1)
    if wrapper == "nohup":
        return _skip_simple_options(tokens, index + 1, set())
    if wrapper == "nice":
        return _skip_nice_options(tokens, index + 1)
    if wrapper == "timeout":
        return _skip_timeout_options(tokens, index + 1)
    return None


def _skip_env_options(tokens: list[str], index: int) -> int | None:
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in {"-i", "--ignore-environment", "-0", "--null", "-v", "--debug"}:
            index += 1
            continue
        if token in {"-u", "--unset", "-C", "--chdir"}:
            index += 2
            if index > len(tokens):
                return None
            continue
        if token.startswith("--unset=") or token.startswith("--chdir="):
            index += 1
            continue
        if token.startswith("-u") and len(token) > 2 or token.startswith("-C") and len(token) > 2:
            index += 1
            continue
        if token.startswith("-") and token != "-":
            if token.startswith("-") and all(char in "i0v" for char in token[1:]):
                index += 1
                continue
            return None
        if _SHELL_ASSIGNMENT.fullmatch(token):
            index += 1
            continue
        return index
    return None


def _skip_simple_options(tokens: list[str], index: int, options: set[str]) -> int | None:
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in options:
            index += 1
            continue
        if token.startswith("-") and token != "-":
            return None
        return index
    return None


def _skip_exec_options(tokens: list[str], index: int) -> int | None:
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in {"-c", "-l"}:
            index += 1
            continue
        if token == "-a":
            index += 2
            if index > len(tokens):
                return None
            continue
        if token.startswith("-a") and len(token) > 2:
            index += 1
            continue
        if token.startswith("-") and token != "-":
            if all(char in "cl" for char in token[1:]):
                index += 1
                continue
            return None
        return index
    return None


def _skip_nice_options(tokens: list[str], index: int) -> int | None:
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1
        if token in {"-n", "--adjustment"}:
            index += 2
            if index > len(tokens):
                return None
            continue
        if token.startswith("--adjustment=") or re.fullmatch(r"-n-?\d+", token) or re.fullmatch(r"-\d+", token):
            index += 1
            continue
        if token.startswith("-") and token != "-":
            return None
        return index
    return None


def _skip_timeout_options(tokens: list[str], index: int) -> int | None:
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            index += 1
            break
        if token in {"--preserve-status", "--foreground", "--verbose"} or re.fullmatch(r"-[vfp]+", token):
            index += 1
            continue
        if token in {"-k", "--kill-after", "-s", "--signal"}:
            index += 2
            if index > len(tokens):
                return None
            continue
        if token.startswith("--kill-after=") or token.startswith("--signal="):
            index += 1
            continue
        if token.startswith("-k") and len(token) > 2 or token.startswith("-s") and len(token) > 2:
            index += 1
            continue
        if token.startswith("-") and token != "-":
            return None
        break
    if index >= len(tokens):
        return None
    return index + 1 if index + 1 < len(tokens) else None


def _skip_assignments(tokens: list[str], index: int) -> int:
    while index < len(tokens) and _SHELL_ASSIGNMENT.fullmatch(tokens[index]):
        index += 1
    return index


def _shell_tokens(text: str) -> list[str]:
    lexer = shlex.shlex(text, posix=True, punctuation_chars=";&|()<>\n")
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    lexer.commenters = ""
    return list(lexer)


def _shell_segments(text: str) -> list[list[str]]:
    """Split at unquoted control operators and keep every command stage."""
    segments: list[list[str]] = []
    start = 0
    quote: str | None = None
    escaped = False
    for index, char in enumerate(text):
        if quote == "'":
            if char == "'":
                quote = None
            continue
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if quote == '"':
            if char == '"':
                quote = None
            continue
        if char in "'\"":
            quote = char
            continue
        if char in ";&|()\n":
            segment = _shell_tokens(text[start:index])
            if segment:
                segments.append(segment)
            start = index + 1
    segment = _shell_tokens(text[start:])
    if segment:
        segments.append(segment)
    return segments


def _typed_mcp_tokens(invocation: str, typed_binaries: set[str]) -> list[str]:
    try:
        tokens = _shell_tokens(invocation)
    except ValueError:
        return [binary for binary in sorted(typed_binaries) if _typed_mcp_pattern(binary).search(invocation)]
    found: list[str] = []
    for token in tokens:
        for binary in _typed_mcp_tokens_in(token, typed_binaries):
            if binary not in found:
                found.append(binary)
    return found


def _typed_mcp_tokens_in(token: str, typed_binaries: set[str]) -> list[str]:
    if _SHELL_ASSIGNMENT.fullmatch(token):
        token = token.split("=", 1)[1]
    candidate = _token_mcp_name(token, typed_binaries)
    if candidate is not None:
        return [candidate]
    try:
        nested = _shell_tokens(token)
    except ValueError:
        return [binary for binary in sorted(typed_binaries) if _typed_mcp_pattern(binary).search(token)]
    if nested == [token]:
        return [binary for binary in sorted(typed_binaries) if _typed_mcp_pattern(binary).search(token)]
    found: list[str] = []
    for part in nested:
        for binary in _typed_mcp_tokens_in(part, typed_binaries):
            if binary not in found:
                found.append(binary)
    return found


def _token_mcp_name(token: str, typed_binaries: set[str]) -> str | None:
    token = token.removesuffix(":*")
    basename = _executable_basename(token)
    return basename if basename in typed_binaries else None


def _typed_mcp_pattern(binary: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z0-9_.-]){re.escape(binary)}(?![A-Za-z0-9_.-])")


def _executable_basename(executable: str) -> str:
    return executable.replace("\\", "/").rsplit("/", 1)[-1]


def _covering_effect(rule: str, restrictions: list[tuple[str, str]]) -> str | None:
    for pattern, effect in restrictions:
        if fnmatch.fnmatchcase(rule, pattern):
            return effect
    return None


def _missing_absolute_paths(rule: str) -> list[str]:
    missing: list[str] = []
    seen: set[str] = set()
    for match in _ABS_PATH.finditer(rule):
        path = match.group(1).rstrip(",:")
        if not path or path in seen:
            continue
        seen.add(path)
        if not Path(path).exists():
            missing.append(path)
    return missing


def _is_fixed_cap_env(key: str) -> bool:
    if not key.startswith("WORKBAY_"):
        return False
    return key.endswith("_MAX_LANES") or key.endswith("_WIDTH") or key.endswith("_CAP")
