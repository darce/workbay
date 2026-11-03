"""Materialize the bounded native hook closure selected by actual Claude settings.

No configured command or payload module runs here. The catalogue describes
imports (including optional imports), reviewed dynamic loaders, and shell
sources/handlers. A dependency change requires a deliberate catalogue update.
File readiness does not qualify the consumer's Python runtime or its harness.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.metadata as metadata
import json
import os
import re
import stat
import sys
from contextlib import ExitStack
from pathlib import Path

from . import hook_helpers as fs

_payload_root = fs._payload_root
_Refused = fs._Refused

# Static imports, separately from path loaders and shell edges below. Keeping
# these distinct detects a missing optional import rather than silently pruning
# it from the closure when the installed payload changes.
_IMPORTS = {
    "_active_task_context.py": ("_worktree_identity.py",),
    "_bash_isolation_guard.py": ("_branch_isolation_guard.py", "_harness_protocol.py"),
    "_branch_isolation_guard.py": ("_harness_protocol.py",),
    "_guard_main_branch_inline.py": ("_branch_isolation_guard.py", "_harness_protocol.py", "_interp.py"),
    "_harness_protocol.py": (),
    "_hook_failure_sink.py": (),
    "_interp.py": (),
    "_protocol.py": ("_interp.py",),
    "_run_guard.py": (),
    "_shell_tool_payload.py": (),
    "_worktree_drift.py": ("_active_task_context.py", "_bash_isolation_guard.py", "_harness_protocol.py", "_protocol.py"),
    "_worktree_identity.py": (),
    "ace-detect.py": (),
    "advise-worktree-cd.py": ("_active_task_context.py", "_protocol.py"),
    "capture-agent-errors.py": ("_hook_failure_sink.py", "_interp.py", "_protocol.py", "resolve_handoff_src.py"),
    "coherence-self-check.py": ("resolve_handoff_src.py",),
    "filter-test-output.py": ("_protocol.py",),
    "guard-agent-remote-only-implementation.py": (),
    "guard-bash-lifecycle-primitives.py": (),
    "guard-bash-main-branch.py": ("_bash_isolation_guard.py", "_harness_protocol.py", "_interp.py", "_protocol.py", "_worktree_identity.py"),
    "guard-bash-remote-gate-routing.py": (),
    "guard-codemap-first.py": (),
    "guard-generated-render-write.py": ("_protocol.py", "resolve_handoff_src.py"),
    "guard-network-sourced-authority.py": ("_protocol.py",),
    "guard-rationale-size.py": ("_protocol.py",),
    "guard-review-finding-resolve.py": ("_protocol.py",),
    "guard-task-plan-findings.py": ("_protocol.py", "resolve_handoff_src.py"),
    "record-file-touch.py": ("_hook_failure_sink.py", "_interp.py", "_protocol.py", "resolve_handoff_src.py"),
    "resolve_handoff_src.py": (),
    "slim-handoff-response.py": ("_protocol.py",),
    "validate-mcp-dict-params.py": ("_protocol.py",),
}
# A finite catalogue cannot soundly analyze arbitrary Python (aliases, exec,
# string programs and subprocess paths included). Accept the reviewed native
# executable syntax only. Comments/formatting may vary; any new executable
# syntax requires auditing its closure and updating this catalogue. A future
# package with different syntax receives source_catalog refusal, never a partial
# "ready" receipt. The receipt separately hashes the actual source bytes.
# Representation executable-ast@2 (CPython 3.12 ast.dump): the published wheel
# build scrubs internal references from comments and docstrings, so docstring
# text is elided while its slot stays. All other constants are fingerprinted.
_PYTHON_AST = {
    '_active_task_context.py': '593802ca364b3e2aa5976a2913afb24fe709e1783ad3ada0418b35e2d41c050e',
    '_bash_isolation_guard.py': '652df082f7a7b627c027be6fa50d8f41d2afc2a84a6b8bff7b2538ddae73d324',
    '_branch_isolation_guard.py': 'ac7f192ce28aa1e3884a83d31f65e5705bce451ae5d56bba6cd09a04ea53e682',
    '_guard_main_branch_inline.py': '078d97a2de9c1253e2b99823508d2fb5543c54d2210dd8ed596230d36fd29015',
    '_harness_protocol.py': '24d768090bcfd01cc53686591a396222ba18c2271d7f28f2637d3388c60264bf',
    '_hook_failure_sink.py': '3365ce251ba017b15ec00ec51b7e08b38ceaf53a6f901b3e025f270b8676d827',
    '_interp.py': '15466f3436c46ca08a4fde61db49a5b940b33705b9b46b64b9e5b2f52a194675',
    '_protocol.py': 'e7a18c1a2c4de3484311b5a79df4d6b6b0752b42fb6f18148fab22bd4eb3ab48',
    '_run_guard.py': '7201d5cd612bddcf40ec49c9b3433b9980d06884908d5db7d7ec5f03aa12cedd',
    '_shell_tool_payload.py': 'e63a9d724c0e72c747f760c670edefa3107fc2b757d6af48b6a24e54bc1ed3f0',
    '_worktree_drift.py': '562921f5bb1fc429c8a06e3adaa2a9cfddc647b9bd5038995b0d52ab600a59e8',
    '_worktree_identity.py': '2e536f49d0420e04bb3affdde6088ee0e52860b3b1f4d8add2ecb5316edd9213',
    'ace-detect.py': 'd3b9372d53cf3bb290f1941ba7ceddd51456c68743277cc42a0b5495f3df8ad2',
    'advise-worktree-cd.py': '7e5616d16afac53148ac12c3435518a47ddd44a84581cf84aacf4e21c24ea346',
    'capture-agent-errors.py': 'c5b63a043e2fe14d613b0fb8c7bc8c2fb161310bb8a89b78249919f399b65322',
    'coherence-self-check.py': 'd6e63d07499904be812104094821400dab70b9ed9f01feb1cb90ec80c77cb423',
    'filter-test-output.py': '2c2f918a400624a762ba59f1995698b48792da6e56c8fdf36534a6864891a6b8',
    'guard-agent-remote-only-implementation.py': '50a6fd8207093ebf972766025370faf10a4010d5cc6e25048983958255386763',
    'guard-bash-lifecycle-primitives.py': '351e44fdfb1d2771bcef2ff08c02e40185a817643e853d9a7bebfa41299b47a6',
    'guard-bash-main-branch.py': '7bef349ec2aa96566373131372be80491e340f0ca039a0136b0c1ba43f109e7b',
    'guard-bash-remote-gate-routing.py': 'e34c1df86b12c0ba8bb0c9ed5897981d0ca8f94785c7902f7587d9c6600870a4',
    'guard-codemap-first.py': '023458859958e34652b8f2bcd8ecd9c16563fcc00b1cab71060349bf1e53da79',
    'guard-generated-render-write.py': '2bdb377082c7df94d039791cfc6cecf210839b9c3918acd924f44e228bb0b654',
    'guard-network-sourced-authority.py': '2f79fb43efe929517a41cbc415d8442c35b8346b42d66d0fdf1d88d7aad2449c',
    'guard-rationale-size.py': 'b1338f9a429e852349ac9ded156fa94d9cb497ee389615efbba74544405a98ef',
    'guard-review-finding-resolve.py': '541613007c91b8e24164e4d5ae59b89cf4707f9c67a8d1ca03278a96b1f5f3c7',
    'guard-task-plan-findings.py': 'd15e51aa0a08db055a39b0e5b2d7e2a056e2b00ec4bbdba5877fd19d884d5123',
    'record-file-touch.py': '545ffa63bd7c127f655755e3c65b1aaa202d56409586813e640d92de708fbfea',
    'resolve_handoff_src.py': '467cc14de9d05020be3159a173da67810f1b917a064893d6c23363731c372662',
    'slim-handoff-response.py': '203538bbb4244627ac4fc5173aaf86ca925e99f558f112d3c68acbe3865ff11c',
    'validate-mcp-dict-params.py': '1aa50037255a703dcc323cabd71733fff0d3b1a9d1bda04dd85e41f42918f934',
}

_PATH_DEPS = {
    "_run_guard.py": ("_interp.py", "resolve_handoff_src.py", "guard-codemap-first.py"),
    "_worktree_drift.py": ("_shell_tool_payload.py",),
    "capture-agent-errors.py": ("_shell_tool_payload.py",),
    "filter-test-output.py": ("_shell_tool_payload.py",),
    "guard-bash-lifecycle-primitives.py": ("_shell_tool_payload.py", "_harness_protocol.py"),
    "guard-bash-main-branch.py": ("_shell_tool_payload.py",),
    "guard-bash-remote-gate-routing.py": ("_shell_tool_payload.py",),
    "guard-codemap-first.py": ("_shell_tool_payload.py",),
    "guard-main-branch.sh": ("_resolve_repo_root.sh", "_guard_main_branch_inline.py", "_active_task_context.py"),
    "guard-worktree-drift.sh": ("_resolve_repo_root.sh", "_worktree_drift.py"),
}
_SHELL_DIGESTS = {
    "_launch_python.sh": "0496b391fc05309b7f84b214793653418b606f79a4b183e6e0588bac3f9e569b",
    "_resolve_repo_root.sh": "d5a5c8005f59fc3137515a908fceac340b18f70e42ff3ac32b13afdcf0421cde",
    "guard-main-branch.sh": "26cba9ed908b1102167549c58f2d8fc4398d356fc8276dab0478829e4c386d06",
    "guard-worktree-drift.sh": "25572ab304e991ef2469dd203db0eb66dcd4ca4e113f68ee9d982fde41ab939a",
}
_ENTRYPOINTS = frozenset({
    "_launch_python.sh", "_run_guard.py", "ace-detect.py", "advise-worktree-cd.py",
    "capture-agent-errors.py", "coherence-self-check.py", "ensure-agent-surfaces.sh",
    "filter-test-output.py", "guard-agent-remote-only-implementation.py",
    "guard-bash-lifecycle-primitives.py", "guard-bash-main-branch.py",
    "guard-bash-remote-gate-routing.py", "guard-codemap-first.py",
    "guard-generated-render-write.py", "guard-main-branch.sh",
    "guard-network-sourced-authority.py", "guard-rationale-size.py",
    "guard-review-finding-resolve.py", "guard-task-plan-findings.py",
    "guard-worktree-drift.sh", "record-file-touch.py", "slim-handoff-response.py",
    "validate-mcp-dict-params.py",
})
_EXTERNAL = {"workbay_protocol": "workbay-protocol", "workbay_handoff_mcp": "mcp-workbay-handoff", "yaml": "PyYAML"}
_CONFIG_NAMES = ("settings.json", "settings.local.json")
_ANCHOR = '"$CLAUDE_PROJECT_DIR/scripts/hooks/'
_COMMAND = re.compile(r'(bash|python3) "\$CLAUDE_PROJECT_DIR/scripts/hooks/([A-Za-z0-9_-]+\.(?:sh|py))"')
_WRAPPED = (
    _ANCHOR + '_launch_python.sh" ' + _ANCHOR + '_run_guard.py" '
    '--fail-mode=closed --policy-aware ' + _ANCHOR + 'guard-codemap-first.py"'
)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _stamp(info: os.stat_result) -> tuple:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _Refused(f"config_invalid: duplicate JSON key {key}")
        result[key] = value
    return result


def _commands(data: bytes, path: str) -> set[str]:
    try:
        config = json.loads(data, object_pairs_hook=_unique_object)
    except (ValueError, UnicodeError) as exc:
        raise _Refused(f"config_invalid: {path}: {exc}") from exc
    if not isinstance(config, dict) or not isinstance(config.get("hooks", {}), dict):
        raise _Refused(f"config_invalid: expected object with hooks object: {path}")
    selected = set()
    for event, bindings in config.get("hooks", {}).items():
        if event not in {"PreToolUse", "PostToolUse", "SessionStart", "UserPromptSubmit", "Stop"} or not isinstance(bindings, list):
            raise _Refused(f"config_invalid: unsupported hook event/shape: {path}: {event}")
        for binding in bindings:
            if not isinstance(binding, dict) or not isinstance(binding.get("hooks"), list):
                raise _Refused(f"config_invalid: expected hook binding: {path}: {event}")
            for hook in binding["hooks"]:
                if not isinstance(hook, dict) or hook.get("type") != "command" or not isinstance(hook.get("command"), str):
                    raise _Refused(f"config_command: only bounded native command hooks are supported: {path}: {event}")
                command = hook["command"]
                if command == _WRAPPED:
                    selected.update({"_launch_python.sh", "_run_guard.py", "guard-codemap-first.py"})
                    continue
                match = _COMMAND.fullmatch(command)
                if match is None:
                    raise _Refused(f"config_command: unsupported command syntax: {path}: {event}")
                interpreter, name = match.groups()
                if name not in _ENTRYPOINTS or name in {"_launch_python.sh", "_run_guard.py"} or (
                    interpreter != ("bash" if name.endswith(".sh") else "python3")
                ):
                    raise _Refused(f"config_command: unknown native command/path: {name}")
                selected.add(name)
    return selected


def _config_snapshot(root: int, stack: ExitStack):
    directory = fs._existing_directory(root, ".claude", stack)
    if directory is None:
        raise _Refused("config_missing: .claude/settings.json or settings.local.json required")
    snapshots, selected = {}, set()
    for name in _CONFIG_NAMES:
        try:
            data, info = fs._read_file(directory, name)
        except FileNotFoundError:
            snapshots[name] = None
        else:
            snapshots[name] = (data, _stamp(info))
            selected.update(_commands(data, f".claude/{name}"))
    if all(value is None for value in snapshots.values()):
        raise _Refused("config_missing: no actual Claude settings file exists")
    return directory, snapshots, selected


def _recheck_config(root: int, directory: int, snapshots: dict) -> None:
    current = os.open(".claude", fs._DIR_FLAGS, dir_fd=root)
    try:
        if fs._identity(os.fstat(current)) != fs._identity(os.fstat(directory)):
            raise _Refused("config_changed: .claude directory replaced")
        for name, expected in snapshots.items():
            try:
                data, info = fs._read_file(current, name)
            except FileNotFoundError:
                actual = None
            else:
                actual = (data, _stamp(info))
            if actual != expected:
                raise _Refused(f"config_changed: .claude/{name}; retry from stable configuration")
    finally:
        os.close(current)


def _executable_ast(tree: ast.Module) -> str:
    # Only a str literal that is the first statement of a module/class/function
    # body is a docstring. Its text is blanked, never removed: adding, moving or
    # dropping a docstring, or editing any other string, changes the digest.
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                first.value.value = ""
    return _digest(ast.dump(tree, include_attributes=False).encode())


def _validate_python(name: str, data: bytes) -> set[str]:
    tree = ast.parse(data, filename=name)
    if _executable_ast(tree) != _PYTHON_AST[name]:
        raise _Refused(f"source_catalog: native Python syntax changed; audit imports/path/subprocess dependencies before extending catalogue: {name}")
    # The fingerprint bounds all dynamic loaders, their globals, inline programs
    # and subprocess paths. AST import parsing verifies the catalogue edges,
    # including imports nested in functions and optional try/except blocks.
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                raise _Refused(f"source_import: unsupported relative import: {name}")
            modules.append(node.module or "")
        elif isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "import_module":
            if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
                raise _Refused(f"source_import: unbounded dynamic import: {name}")
            modules.append(node.args[0].value)
    local, external = set(), set()
    for module in modules:
        root = module.split(".")[0]
        if root + ".py" in _IMPORTS:
            if module != root:
                raise _Refused(f"source_import: unsupported local package: {name}: {module}")
            local.add(root + ".py")
        elif root in _EXTERNAL:
            external.add(_EXTERNAL[root])
        elif root not in sys.stdlib_module_names:
            raise _Refused(f"source_import: uncatalogued dependency: {name}: {module}")
    if local != set(_IMPORTS[name]):
        raise _Refused(f"source_import: local import catalogue differs: {name}")
    return external


# SessionStart shell surface. Operands of tests, messages and Git tracked-source
# reads are data; a local script that is run, sourced or handed to a launcher is
# a dependency. Anything outside this finite vocabulary refuses.
_SHELL_PATH = re.compile(r"[\w/-]+\.(?:py|sh)\b")
_SHELL_EXECUTOR = re.compile(r"(?m)(?:^|[;|&()]\s*)\s*(?:source\b|\.\s|exec\b|eval\b|bash\b|sh\b|python[\d.]*\b|\$)")
_SHELL_LOADERS = re.compile(
    r"(?:source|\.|exec|eval|command|builtin|enable|alias|trap|env|nohup|nice|timeout|xargs|sudo|doas|time|coproc"
    r"|bash|sh|dash|zsh|ksh|perl|ruby|node|php|lua|uv|uvx|pipx|pip[\d.]*|python[\d.]*)"
)
_SHELL_DATA = frozenset({"[", "[[", "test", "echo", "printf", "cmp", "realpath", "readlink", "dirname", "basename"})
_SHELL_GIT_READS = frozenset({"ls-files", "ls-tree", "cat-file", "rev-parse"})
_SHELL_DECLARE = frozenset({"local", "readonly", "export", "declare", "typeset"})
_SHELL_ENVIRONMENT = frozenset({"PATH", "BASH_ENV", "ENV"})
_SHELL_RESERVED = frozenset({"if", "then", "else", "elif", "fi", "do", "done", "while", "until", "!", "{", "}"})
_SHELL_ASSIGN = re.compile(r"([A-Za-z_]\w*)\+?=(.*)", re.S)
_SHELL_VARIABLE = re.compile(r"\$(?:[A-Za-z_]\w*|[0-9@*#?$!-])")


def _shell_commands(name: str, text: str):
    """Split shell into simple commands of (word, is_redirect_target).

    Quotes are removed; a command substitution is parsed as its own command and
    its raw text stays in the enclosing word, so data flowing through it remains
    visible. Here-documents, backquotes and similar constructs refuse.
    """
    commands, functions = [], set()

    def unsupported(what):
        return _Refused(f"source_shell: unsupported shell construct ({what}): {name}")

    def dollar(i):
        follow = text[i + 1:i + 2]
        if follow == "(":
            return text[i:(end := parse(i + 2, nested=True))], end
        if follow == "{":
            end = text.find("}", i + 2)
            if end < 0 or any(token in text[i + 2:end] for token in ("$(", "`", "{")):
                raise unsupported("parameter expansion")
            return text[i:end + 1], end + 1
        if follow in ("'", '"'):
            raise unsupported("dollar quoting")
        match = _SHELL_VARIABLE.match(text, i)
        return (match.group(), match.end()) if match else ("$", i + 1)

    def double(i):
        parts = []
        while i < len(text):
            char = text[i]
            if char == '"':
                return "".join(parts), i + 1
            if char == "`":
                raise unsupported("backquote")
            if char == "$":
                piece, i = dollar(i)
            else:
                piece, i = (text[i + 1:i + 2], i + 2) if char == "\\" else (char, i + 1)
            parts.append(piece)
        raise unsupported("unterminated quote")

    def parse(i, nested):
        current, word, redirect, depth = [], None, False, 0

        def end_word():
            nonlocal word, redirect
            if word is not None:
                current.append((word, redirect))
                word, redirect = None, False

        def end_command():
            end_word()
            if current:
                commands.append(list(current))
                current.clear()

        while i < len(text):
            char = text[i]
            if char == "\\":
                if text[i + 1:i + 2] != "\n":
                    word = (word or "") + text[i + 1:i + 2]
                i += 2
            elif char == "'":
                end = text.find("'", i + 1)
                if end < 0:
                    raise unsupported("unterminated quote")
                word, i = (word or "") + text[i + 1:end], end + 1
            elif char in "\"$":
                piece, i = double(i + 1) if char == '"' else dollar(i)
                word = (word or "") + piece
            elif char == "`":
                raise unsupported("backquote")
            elif char == "#" and word is None:
                end = text.find("\n", i)
                i = len(text) if end < 0 else end
            elif char in " \t":
                end_word()
                i += 1
            elif char in "<>" or text.startswith("&>", i):
                if text.startswith("<<", i) or text[i + 1:i + 2] == "(":
                    raise unsupported("here-document or process substitution")
                if word is not None and word.isdigit():
                    word = None
                end_word()
                i += 1
                while i < len(text) and text[i] in "<>&|":
                    i += 1
                redirect = True
            elif char in "\n;&|":
                end_command()
                i += 1
            elif char == "(":
                if word is not None and not current:
                    functions.add(word)
                end_command()
                depth, i = depth + 1, i + 1
            elif char == ")":
                end_command()
                i += 1
                if depth:
                    depth -= 1
                elif nested:
                    return i
                else:
                    raise unsupported("unbalanced parenthesis")
            else:
                word, i = (word or "") + char, i + 1
        if nested or depth:
            raise unsupported("unbalanced parenthesis")
        end_command()
        return i

    parse(0, nested=False)
    return commands, functions


def _validate_shell(name: str, data: bytes) -> None:
    if name in _SHELL_DIGESTS:
        if _digest(data) != _SHELL_DIGESTS[name]:
            raise _Refused(f"source_shell: reviewed shell dependency catalogue changed: {name}")
        return
    # SessionStart's platform-owned consumer-safety guard may change without
    # changing its dependency-free shell surface. Do not infer dependencies by
    # running it. Any new interpreter, shell source, eval, executable variable,
    # or executed local script requires an explicit catalogue extension.
    refused = _Refused(f"source_shell: unbounded or uncatalogued local dependency: {name}")
    text = data.decode("utf-8")
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    if name != "ensure-agent-surfaces.sh" or _SHELL_EXECUTOR.search(code):
        raise refused
    commands, functions = _shell_commands(name, text)
    if functions & (_SHELL_DATA | {"git"}):
        raise refused
    tainted = set()

    def names_path(word):
        return bool(_SHELL_PATH.search(word)) or any(v in tainted for v in re.findall(r"\$\{?(\w+)", word))

    def assign(word):
        variable, value = _SHELL_ASSIGN.fullmatch(word).groups()
        if variable in _SHELL_ENVIRONMENT:
            raise refused
        if names_path(value):
            tainted.add(variable)

    for words in commands:
        index = 0
        while index < len(words) and words[index][0] in _SHELL_RESERVED:
            index += 1
        prefix = []
        while index < len(words) and not words[index][1] and _SHELL_ASSIGN.fullmatch(words[index][0]):
            prefix.append(words[index][0])
            index += 1
        position = next((k for k in range(index, len(words)) if not words[k][1]), None)
        if position is None:
            for word in prefix:
                assign(word)
            if any(names_path(word) for word, _ in words[index:]):
                raise refused
            continue
        command = words[position][0]
        args = [word for k, (word, _) in enumerate(words[index:], index) if k != position]
        for word in prefix:
            if _SHELL_ASSIGN.fullmatch(word)[1] in _SHELL_ENVIRONMENT:
                raise refused
        if command in _SHELL_DECLARE:
            for word in args:
                if _SHELL_ASSIGN.fullmatch(word):
                    assign(word)
                elif names_path(word):
                    raise refused
            continue
        if command == "command" and args[:1] in (["-v"], ["-V"]):
            continue
        if command == "function" and args[:1] and args[0] in _SHELL_DATA | {"git"}:
            raise refused
        if command.startswith("$") or "/" in command or _SHELL_PATH.search(command) or _SHELL_LOADERS.fullmatch(command):
            raise refused
        if not any(names_path(word) for word in prefix + args):
            continue
        if command == "git" and not prefix:
            k = 0
            while k < len(args) and args[k] in {"-C", "--no-optional-locks", "--literal-pathspecs"}:
                k += 2 if args[k] == "-C" else 1
            if k < len(args) and args[k] in _SHELL_GIT_READS:
                continue
        elif command in _SHELL_DATA and not prefix:
            continue
        raise refused


def _runtime_requirements(distributions: set[str]) -> list[dict[str, object]]:
    # Metadata only: do not import third-party packages to discover or qualify
    # dependencies. Requires-Dist retains exact native specifiers and markers.
    result = []
    for name in sorted(distributions):
        try:
            distribution = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            result.append({"distribution": name, "version": None, "requires_dist": [], "provenance": "metadata_unavailable", "qualified": False})
        else:
            result.append({"distribution": name, "version": distribution.version, "requires_dist": sorted(distribution.requires or []), "provenance": "installed_distribution_metadata", "qualified": False})
    return result


def _load_sources(selected: set[str], source: dict, stack: ExitStack):
    try:
        path = _payload_root()
        source["root"] = str(path)
        from .install import _package_version

        source["version"] = _package_version(path)
        root = fs._keep(stack, fs._absolute_directory(path))
        scripts = fs._keep(stack, os.open("scripts", fs._DIR_FLAGS, dir_fd=root))
        hooks = fs._keep(stack, os.open("hooks", fs._DIR_FLAGS, dir_fd=scripts))
        pending, sources, stamps, modes, external = list(selected), {}, {}, {}, set()
        while pending:
            name = pending.pop()
            if name in sources:
                continue
            data, info = fs._read_file(hooks, name)
            mode = stat.S_IMODE(info.st_mode)
            if mode not in {0o644, 0o755}:
                raise _Refused(f"source_mode: expected native 0644/0755: {name}")
            if name.endswith(".py"):
                external.update(_validate_python(name, data))
            else:
                _validate_shell(name, data)
            sources[name], stamps[name], modes[name] = data, _stamp(info), mode
            pending.extend(_IMPORTS.get(name, ()))
            pending.extend(_PATH_DEPS.get(name, ()))
        sources = dict(sorted(sources.items()))
        source["files"] = {f"scripts/hooks/{name}": _digest(data) for name, data in sources.items()}
        source["modes"] = {f"scripts/hooks/{name}": f"{modes[name]:04o}" for name in sources}
        if "_interp.py" in sources:
            external.update({"pydantic", "fastmcp", "workbay-protocol"})
        if {"capture-agent-errors.py", "record-file-touch.py", "_run_guard.py"}.intersection(sources):
            external.add("mcp-workbay-handoff")
        source["runtime_requirements"] = _runtime_requirements(external)
        return path, root, scripts, hooks, sources, stamps, modes
    except (OSError, ValueError, SyntaxError, UnicodeError, ImportError, _Refused) as exc:
        raise _Refused(f"source_unavailable: verify installed workbay-system configured-hook payload: {exc}") from exc


def _recheck_sources(loaded) -> None:
    path, root, scripts, hooks, sources, stamps, _ = loaded
    try:
        fs._bound(path, root, scripts, hooks)
        if _payload_root() != path:
            raise _Refused("installed payload origin changed")
        for name, expected in sources.items():
            data, info = fs._read_file(hooks, name)
            if data != expected or _stamp(info) != stamps[name]:
                raise _Refused(f"payload file changed: {name}")
    except (OSError, _Refused) as exc:
        raise _Refused(f"source_changed: {exc}") from exc


def provision_configured_hooks(*, target: Path, check: bool = False) -> dict[str, object]:
    """Return a single ready/missing/refused receipt; never execute hooks.

    Check is read-only. Apply creates only missing files and needed directories
    after complete preflight. Exclusive creates never overwrite a concurrent
    writer. Rollback removes only this call's unchanged inodes and bytes.
    Concurrent mutation is detected at bounded checkpoints; no claim of a
    filesystem-wide transaction or qualified package runtime is made.
    """
    target = Path(target)
    source = {
        "kind": "package", "package": "workbay-system", "version": None, "root": None,
        "files": {}, "catalog": "native-configured-hooks@2", "runtime_qualified": False, "runtime_requirements": [],
        "runtime_limits": [
            "File readiness only; consumer Python versions, dependency versions/origins, hook execution, and harness/model readiness are not qualified.",
            "Python executable syntax and shell dependency surfaces are version-gated by a finite catalogue; changed native code requires a deliberate closure audit and catalogue update.",
            "Native guards may require workbay-protocol, mcp-workbay-handoff and their declared runtime dependencies; no packages are installed by this operation.",
            "The optional source-checkout branch_naming.py probe uses the installed workbay_handoff_mcp fallback in consumers; no package source trees are copied.",
            "SessionStart consumer-safety qualification is external; this provisioner never runs generators or creates plugin manifests.",
        ],
    }
    receipt = {"ok": False, "status": "refused", "target": str(target.absolute()), "source": source,
               "config": [], "files": [], "reason": None}
    created, directories = [], []
    hooks = None
    with ExitStack() as stack:
        try:
            root = fs._keep(stack, fs._absolute_directory(target))
            fs._git_root(target)
            config_dir, snapshots, selected = _config_snapshot(root, stack)
            receipt["config"] = [{"path": f".claude/{name}", "sha256": _digest(value[0])}
                                 for name, value in snapshots.items() if value is not None]
            loaded = _load_sources(selected, source, stack)
            sources, modes = loaded[4], loaded[6]
            scripts = fs._existing_directory(root, "scripts", stack)
            hooks = fs._existing_directory(scripts, "hooks", stack) if scripts is not None else None
            files = fs._actions(hooks, sources)
            receipt["files"] = files

            def stable():
                fs._bound(target, root, scripts, hooks)
                _recheck_config(root, config_dir, snapshots)
                _recheck_sources(loaded)

            stable()
            if check:
                missing = any(entry["action"] == "missing" for entry in files)
                receipt.update(ok=not missing, status="missing" if missing else "ready",
                               reason="hooks_missing: apply provision-configured-hooks to materialize missing files" if missing else None)
                return receipt
            if not files:
                receipt.update(ok=True, status="ready")
                return receipt
            if scripts is None:
                os.mkdir("scripts", dir_fd=root)
                scripts = fs._keep(stack, os.open("scripts", fs._DIR_FLAGS, dir_fd=root))
                directories.append((root, "scripts", fs._identity(os.fstat(scripts))))
            if hooks is None:
                stable()
                os.mkdir("hooks", dir_fd=scripts)
                hooks = fs._keep(stack, os.open("hooks", fs._DIR_FLAGS, dir_fd=scripts))
                directories.append((scripts, "hooks", fs._identity(os.fstat(hooks))))
            for entry in files:
                stable()
                name = Path(entry["path"]).name
                if entry["action"] == "identical":
                    data, _ = fs._read_file(hooks, name)
                    if data != sources[name]:
                        raise _Refused(f"target_changed: {entry['path']}")
                    continue
                descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                                     modes[name], dir_fd=hooks)
                written = bytearray()
                created.append((name, fs._identity(os.fstat(descriptor)), written))
                try:
                    # Only the newly created descriptor: creation must retain
                    # the native executable wrapper mode under any umask.
                    os.fchmod(descriptor, modes[name])
                    data = sources[name]
                    while len(written) < len(data):
                        count = os.write(descriptor, data[len(written):])
                        if count <= 0:
                            raise OSError("short configured-hook write")
                        written.extend(data[len(written):len(written) + count])
                finally:
                    os.close(descriptor)
                entry["action"] = "created"
            stable()
            if any(entry["action"] != "identical" for entry in fs._actions(hooks, sources)):
                raise _Refused("target_changed: hook disappeared after creation")
            receipt.update(ok=True, status="ready", reason=None)
        except (OSError, ValueError, RecursionError, _Refused) as exc:
            retained = fs._rollback(hooks, created, directories)
            reason = str(exc) if isinstance(exc, _Refused) else f"filesystem_refused: {exc}"
            if retained:
                reason += "; rollback_retained: concurrent or unreadable changes at " + ", ".join(retained)
            receipt.update(ok=False, status="refused", files=[], reason=reason)
    return receipt
