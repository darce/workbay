"""Runtime-derived import reach for code invoked by repository hooks.

The hook set is read from the checked-out harness configurations and the Git
hook directory. Local hook imports are followed with ``ast``; remaining
imports are resolved in one bounded probe using the same venv interpreter the
hooks use. Results are cached only while their source/config/install inputs
remain unchanged.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Callable, Literal

REACH_PROBE_TIMEOUT_SECONDS = 8
_PAYLOAD_HOOK_ROOT = "packages/workbay-system/workbay_system/payload/scripts/hooks"
_MAKE_TARGET_ENTRYPOINTS: dict[str, frozenset[str]] = {
    # Make routes through the installed `workbay-overlay-tooling` console
    # entrypoint in source checkouts and through the payload script in a
    # consumer checkout. Include the CLI's finite generator dispatch target.
    "plugins-build": frozenset(
        {
            "packages/workbay-system/workbay_system/overlay_tooling/cli.py",
            "packages/workbay-system/workbay_system/overlay_tooling/_generator.py",
            "packages/workbay-system/workbay_system/payload/scripts/generate_agent_workflows.py",
        }
    ),
    "generate-agent-workflows": frozenset(
        {
            "packages/workbay-system/workbay_system/overlay_tooling/cli.py",
            "packages/workbay-system/workbay_system/overlay_tooling/_generator.py",
            "packages/workbay-system/workbay_system/payload/scripts/generate_agent_workflows.py",
        }
    ),
}
_PROBE_PROGRAM = r"""
import importlib.util, json, sys
result = {}
for name in json.loads(sys.argv[1]):
    try:
        spec = importlib.util.find_spec(name)
        result[name] = {
            "origin": None if spec is None else spec.origin,
            "submodule_search_locations": (
                None if spec is None or spec.submodule_search_locations is None
                else list(spec.submodule_search_locations)
            ),
        }
    except Exception:
        result[name] = {"origin": None, "submodule_search_locations": None}
print(json.dumps(result, sort_keys=True))
"""


@dataclass(frozen=True)
class ReachResult:
    """Repository-relative hook paths and the evidence chains that reach them."""

    paths: frozenset[str]
    chains: dict[str, list[str]]
    status: Literal["ok", "degraded"]
    cause: str = ""
    notes: tuple[str, ...] = ()


_CACHE: dict[str, tuple[str, ReachResult, dict[Path, str]]] = {}
_MISSING_SOURCE = "<missing-source>"


def _config_files(root: Path) -> list[Path]:
    candidates = [*root.glob(".claude/settings*.json"), root / ".codex/hooks.json"]
    github_hooks = root / ".github/hooks"
    if github_hooks.is_dir():
        candidates.extend(github_hooks.glob("*.json"))
    return sorted(path for path in candidates if path.is_file())


def _commands(value: object) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "command" and isinstance(child, str):
                found.append(child)
            else:
                found.extend(_commands(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_commands(child))
    return found


def _paths_in_command(command: str) -> list[str]:
    """Extract literal repository-relative Python and shell hook paths."""
    paths: list[str] = []
    try:
        tokens = shlex.split(command, comments=True)
    except ValueError:
        return []
    for token in tokens:
        value = token.rstrip(";,|&")
        rooted = re.match(r"^(?:\$\{[A-Za-z_][A-Za-z0-9_]*\}|\$[A-Za-z_][A-Za-z0-9_]*)/", value)
        if rooted is not None:
            value = value[rooted.end() :]
        while value.startswith("./"):
            value = value[2:]
        if value.startswith("/") or not value.endswith((".py", ".sh")):
            continue
        parts = value.split("/")
        if not parts or any(part in {"", ".", ".."} for part in parts):
            continue
        if not all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in parts):
            continue
        if parts[-1].endswith((".py", ".sh")) and value not in paths:
            paths.append(value)
    return paths


def _load_interp(root: Path) -> ModuleType | None:
    interp_path = root / "scripts/hooks/_interp.py"
    try:
        spec = importlib.util.spec_from_file_location("_workbay_hook_interp", interp_path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except (ImportError, OSError, SyntaxError, ValueError):
        return None


def _resolve_hook_interpreter(root: Path) -> str:
    """Resolve the hooks' interpreter through their own `_interp` helper."""
    module = _load_interp(root)
    if module is None:
        return ""
    resolver = getattr(module, "_venv_python", None)
    if not callable(resolver):
        return ""
    try:
        value = resolver(str(root))
    except Exception:  # The caller must degrade safely on resolver failure.
        return ""
    return value if isinstance(value, str) else ""


def _install_metadata(interpreter: str) -> list[tuple[str, int]]:
    if not interpreter:
        return []
    venv = Path(interpreter).parent.parent.resolve()
    metadata: list[tuple[str, int]] = []
    try:
        for path in venv.rglob("*"):
            if path.suffix == ".pth" or path.name == "direct_url.json":
                try:
                    metadata.append((str(path), path.stat().st_mtime_ns))
                except OSError:
                    metadata.append((str(path), -1))
    except OSError:
        pass
    return sorted(metadata)


def _content_key(root: Path, interpreter: str, config_files: list[Path]) -> str:
    digest = hashlib.sha256()
    source_files = [path for path in (root / "scripts/hooks").rglob("*") if path.is_file()]
    github_hooks = root / ".github/hooks"
    if github_hooks.is_dir():
        source_files.extend(
            path for path in github_hooks.rglob("*") if path.is_file() and path.suffix in {".py", ".sh"}
        )
    for path in sorted(set(config_files + source_files)):
        try:
            relative = path.relative_to(root).as_posix()
            payload = path.read_bytes()
        except OSError:
            relative, payload = str(path), b"<unreadable>"
        digest.update(relative.encode("utf-8", "surrogateescape"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(payload).digest())
    digest.update(interpreter.encode("utf-8", "surrogateescape"))
    for path, mtime_ns in _install_metadata(interpreter):
        digest.update(path.encode("utf-8", "surrogateescape"))
        digest.update(str(mtime_ns).encode("ascii"))
    return digest.hexdigest()


def _module_files(hooks_root: Path, *, prefix: str = "") -> dict[str, Path]:
    modules: dict[str, Path] = {}
    for path in hooks_root.rglob("*.py"):
        relative = path.relative_to(hooks_root).with_suffix("")
        if relative.name == "__init__":
            relative = relative.parent
        if relative.parts:
            modules[prefix + ".".join(relative.parts)] = path
    return modules


def _resolve_local_module(name: str, modules: dict[str, Path]) -> Path | None:
    parts = name.split(".")
    for size in range(len(parts), 0, -1):
        module = ".".join(parts[:size])
        path = modules.get(module)
        if path is not None:
            return path
    return None


def _literal_path(node: ast.AST, bindings: dict[str, str], anchors: dict[str, str]) -> str | None:
    """Evaluate only literals and explicitly file-anchored path operations."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return bindings.get(node.id, anchors.get(node.id))
    if isinstance(node, ast.Attribute) and node.attr == "parent":
        value = _literal_path(node.value, bindings, anchors)
        return str(Path(value).parent) if value is not None else None
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute) and node.value.attr == "parents":
        value = _literal_path(node.value.value, bindings, anchors)
        index = node.slice
        if value is not None and isinstance(index, ast.Constant) and type(index.value) is int and index.value >= 0:
            parents = Path(value).parents
            return str(parents[index.value]) if index.value < len(parents) else None
    if isinstance(node, ast.Call) and not node.keywords:
        if isinstance(node.func, ast.Name) and node.func.id in anchors and len(node.args) == 1:
            if node.func.id in {"Path", "str"}:
                return _literal_path(node.args[0], bindings, anchors)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "resolve" and not node.args:
            value = _literal_path(node.func.value, bindings, anchors)
            return str(Path(value).resolve()) if value is not None else None
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Div, ast.Add)):
        left = _literal_path(node.left, bindings, anchors)
        right = _literal_path(node.right, bindings, anchors)
        if left is not None and right is not None:
            return str(Path(left) / right) if isinstance(node.op, ast.Div) else left + right
    return None


def _shell_targets(
    path: Path,
    root: Path,
    *,
    inline_sources: list[tuple[Path, str]] | None = None,
    passthrough: list[int | None] | None = None,
    shell_edges: list[tuple[Path, Path]] | None = None,
    _visited: set[Path] | None = None,
) -> list[Path]:
    """Trace repository Python reached by a shell hook, failing closed otherwise.

    The supported shell grammar is intentionally small: control structures and
    function definitions are inert syntax, but their bodies are still scanned.
    Only known shell utilities, sourced in-repository shell files, literal
    Python targets, and the reviewed Make routes are accepted.
    """
    root = root.resolve()
    targets: list[Path] = []
    visited = _visited if _visited is not None else set()
    inline = inline_sources if inline_sources is not None else []
    pass_results = passthrough if passthrough is not None else []
    edges = shell_edges if shell_edges is not None else []
    path.resolve()

    def fail(shell_file: Path, line_number: int) -> None:
        relative = _relative_path(root, shell_file) or shell_file.as_posix()
        try:
            excerpt = shell_file.read_text(encoding="utf-8").splitlines()[line_number - 1].strip()
        except (OSError, UnicodeError, IndexError):
            excerpt = ""
        diagnostic = f":{excerpt}" if excerpt else ""
        if parsed_command:
            diagnostic += f":parsed={parsed_command!r}"
        raise ValueError(f"shell_hook_unparsed:{relative}:{line_number}{diagnostic}")

    def normalize_path(token: str, bindings: dict[str, str | None]) -> str | None:
        value = token
        if value in bindings:
            return bindings[value]
        if value.startswith("$") and not value.startswith("${") and value[1:] in bindings:
            return bindings[value[1:]]
        if value.startswith("${") and value.endswith("}") and value[2:-1] in bindings:
            return bindings[value[2:-1]]
        # The root aliases are established only by the recognized repo-root
        # resolution idioms below. Any other shell expansion stays unknown.
        for name in ("REPO_ROOT", "repo_root", "root", "CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT"):
            value = value.replace("${" + name + "}", bindings.get(name) or "")
            value = re.sub(rf"\${name}(?![A-Za-z0-9_])", bindings.get(name) or "", value)
        for name in ("HOOK_DIR", "GUARD_DIR"):
            replacement = bindings.get(name)
            if replacement is not None:
                value = value.replace("${" + name + "}", replacement).replace("$" + name, replacement)
        if value.startswith("${") or re.search(r"\$[A-Za-z_]|`", value):
            return None
        marker_matches = [marker for marker in ("scripts/hooks/", ".github/hooks/") if marker in value]
        if len(marker_matches) > 1:
            return None
        if marker_matches:
            marker = marker_matches[0]
            suffix = value.split(marker, 1)[1]
            candidate = marker + suffix
            if not re.fullmatch(r"(?:scripts/hooks|\.github/hooks)/[A-Za-z0-9_./-]+\.(?:py|sh)", candidate):
                return None
            return candidate
        if value in bindings:
            return bindings[value]
        if value.startswith(("$", "${")):
            name = value[2:-1] if value.startswith("${") and value.endswith("}") else value[1:]
            return bindings.get(name)
        return None

    def split_commands(value: str) -> list[str]:
        def substitution_end(text: str, start: int) -> int:
            depth = 2 if text.startswith("$((", start) else 1
            index = start + (3 if depth == 2 else 2)
            quote = ""
            escaped = False
            while index < len(text) and depth:
                char = text[index]
                if escaped:
                    escaped = False
                elif char == "\\" and quote != "'":
                    escaped = True
                elif char in {"'", '"'}:
                    if not quote:
                        quote = char
                    elif quote == char:
                        quote = ""
                elif not quote and text.startswith("$(", index):
                    depth += 1
                    index += 1
                elif not quote and char == "(":
                    depth += 1
                elif not quote and char == ")":
                    depth -= 1
                index += 1
            return index if not depth else len(text)

        parts: list[str] = []
        start = 0
        quote = ""
        escaped = False
        index = 0
        while index < len(value):
            char = value[index]
            if value.startswith("$(", index) and quote != "'":
                index = substitution_end(value, index)
                continue
            if escaped:
                escaped = False
                index += 1
                continue
            if char == "\\" and quote != "'":
                escaped = True
                index += 1
                continue
            if char in {"'", '"'}:
                if not quote:
                    quote = char
                elif quote == char:
                    quote = ""
                index += 1
                continue
            if quote:
                index += 1
                continue
            if value.startswith(("&&", "||"), index):
                parts.append(value[start:index])
                index += 2
                start = index
                continue
            if char in ";|":
                parts.append(value[start:index])
                index += 1
                start = index
                continue
            index += 1
        parts.append(value[start:])
        return parts

    def scan_substitutions(text: str, shell_file: Path, line_number: int, bindings: dict[str, str | None]) -> str:
        result: list[str] = []
        index = 0
        while index < len(text):
            if text.startswith("$((", index):
                depth = 2
                end = index + 3
                while end < len(text) and depth:
                    if text[end] == "(":
                        depth += 1
                    elif text[end] == ")":
                        depth -= 1
                    end += 1
                if depth:
                    fail(shell_file, line_number)
                result.append(" __shell_expansion__ ")
                index = end
                continue
            if text.startswith("$(", index):
                depth = 1
                quote = ""
                escaped = False
                end = index + 2
                while end < len(text) and depth:
                    char = text[end]
                    if escaped:
                        escaped = False
                    elif char == "\\" and quote != "'":
                        escaped = True
                    elif char in {"'", '"'}:
                        if not quote:
                            quote = char
                        elif quote == char:
                            quote = ""
                    elif not quote and text.startswith("$(", end):
                        depth += 1
                        end += 1
                    elif not quote and char == "(":
                        depth += 1
                    elif not quote and char == ")":
                        depth -= 1
                    end += 1
                if depth:
                    fail(shell_file, line_number)
                body = text[index + 2 : end - 1]
                for nested in split_commands(body):
                    scan_command(nested, shell_file, line_number, bindings, in_substitution=True)
                result.append(" __shell_expansion__ ")
                index = end
                continue
            if text[index] == "`":
                end = text.find("`", index + 1)
                if end < 0:
                    fail(shell_file, line_number)
                for nested in split_commands(text[index + 1 : end]):
                    scan_command(nested, shell_file, line_number, bindings, in_substitution=True)
                result.append(" __shell_expansion__ ")
                index = end + 1
                continue
            result.append(text[index])
            index += 1
        return "".join(result)

    safe_utilities = {
        "[",
        ":",
        "cat",
        "cd",
        "dirname",
        "echo",
        "git",
        "git-lfs",
        "kill",
        "mkdir",
        "mktemp",
        "ps",
        "printf",
        "pwd",
        "read",
        "rm",
        "rmdir",
        "sleep",
        "test",
        "trap",
        "umask",
        "wait",
    }
    function_names: set[str] = set()
    python_targets = 0
    passthrough_line: int | None = None
    parsed_command = ""

    def add_target(relative: str, shell_file: Path, line_number: int) -> Path:
        target = root / relative
        if not target.is_file():
            fail(shell_file, line_number)
        if target not in targets:
            targets.append(target)
        edge = (shell_file.resolve(), target.resolve())
        if edge not in edges:
            edges.append(edge)
        return target

    def process_python(words: list[str], shell_file: Path, line_number: int, bindings: dict[str, str | None]) -> None:
        nonlocal python_targets
        if len(words) < 2:
            fail(shell_file, line_number)
        if words[1] == "-c":
            if len(words) < 3 or "__shell_expansion__" in words[2]:
                fail(shell_file, line_number)
            inline.append((shell_file, words[2]))
            return
        if words[1] == "-":
            return  # The caller attaches and parses the Python heredoc body.
        index = 1
        while index < len(words) and words[index].startswith("-"):
            if words[index] in {"-I", "-u", "-B", "-E", "-s"}:
                index += 1
                continue
            fail(shell_file, line_number)
        if index >= len(words):
            fail(shell_file, line_number)
        relative = normalize_path(words[index], bindings)
        if relative is None or not relative.endswith(".py"):
            fail(shell_file, line_number)
        python_targets += 1
        add_target(relative, shell_file, line_number)

    def scan_command(
        raw: str,
        shell_file: Path,
        line_number: int,
        bindings: dict[str, str | None],
        *,
        in_substitution: bool = False,
    ) -> None:
        nonlocal parsed_command
        parsed_command = raw.strip()
        text = scan_substitutions(raw.strip(), shell_file, line_number, bindings).strip()
        parsed_command = text
        if not text or text in {"{", "}", ";;"}:
            return
        text = re.sub(r"^(?:(?:if|elif|while|until|!|then|do)\s+)+", "", text)
        text = re.sub(r"\s+(?:then|do)$", "", text)
        if not text or text in {"then", "do", "fi", "done", "else", "return", "break", "continue", "esac"}:
            return
        try:
            words = shlex.split(text, comments=True)
        except ValueError:
            fail(shell_file, line_number)
            return
        if not words:
            return
        while words and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", words[0]):
            name, value = words.pop(0).split("=", 1)
            if name in {"REPO_ROOT", "repo_root", "root", "CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT"}:
                bindings[name] = ""
            elif name == "HOOK_DIR":
                current_rel = _relative_path(root, shell_file)
                bindings[name] = (
                    current_rel.rsplit("/", 1)[0] if current_rel and "/" in current_rel else "scripts/hooks"
                )
            elif name == "GUARD_DIR":
                bindings[name] = "scripts/hooks"
            else:
                bindings[name] = normalize_path(value, bindings)
        if not words:
            return
        name = words.pop(0)
        if name in {"__shell_expansion__"}:
            return
        if name == "exec":
            if words == ["$_chosen", "-I", "$@"]:
                nonlocal passthrough_line
                passthrough_line = line_number
                return
            if words and words[0] in {"python", "python3"}:
                process_python(words, shell_file, line_number, bindings)
                return
            fail(shell_file, line_number)
        if name == "$_interp" and len(words) >= 3 and words[:2] == ["-I", "-c"]:
            if "__shell_expansion__" in words[2]:
                fail(shell_file, line_number)
            tail = words[3:]
            while tail:
                token = tail.pop(0)
                if token in {"<", ">", "2>", "1>"}:
                    if not tail:
                        fail(shell_file, line_number)
                    tail.pop(0)
                elif token.startswith(("</", ">")) or re.fullmatch(r"\d+[<>]&\d+", token) or token == "&":
                    continue
                else:
                    fail(shell_file, line_number)
            inline.append((shell_file, words[2]))
            return
        if name in {"python", "python3"}:
            process_python([name, *words], shell_file, line_number, bindings)
            return
        if name in {".", "source"}:
            if not words:
                fail(shell_file, line_number)
            relative = normalize_path(words[0], bindings)
            if relative is None or not relative.endswith(".sh"):
                fail(shell_file, line_number)
            sourced = add_target(relative, shell_file, line_number)
            trace_file(sourced, bindings)
            return
        if name == "make":
            args = list(words)
            targets_seen: list[str] = []
            while args:
                token = args.pop(0)
                if token in {"-C", "-f", "-j", "--directory"}:
                    if args:
                        args.pop(0)
                    continue
                if token.startswith((">", "<")) or re.match(r"\d+[<>]", token):
                    continue
                if token.startswith("-") or re.match(r"[A-Za-z_][A-Za-z0-9_]*=", token):
                    continue
                targets_seen.append(token)
            if not targets_seen:
                fail(shell_file, line_number)
            for make_target in targets_seen:
                declared = _MAKE_TARGET_ENTRYPOINTS.get(make_target)
                if declared is None:
                    fail(shell_file, line_number)
                for relative in sorted(declared):
                    add_target(relative, shell_file, line_number)
            return
        if name == "command":
            args = list(words)
            while args and args[0] in {"-p", "-v", "--"}:
                option = args.pop(0)
                if option == "-v":
                    return
            if not args:
                return
            name = args.pop(0)
            words = [name, *args]
            if name in {"python", "python3"}:
                process_python(words, shell_file, line_number, bindings)
                return
            if name == "make":
                scan_command("make " + " ".join(words[1:]), shell_file, line_number, bindings)
                return
            if name not in safe_utilities:
                fail(shell_file, line_number)
            return
        if name in {"set", "local", "export", "readonly", "shift", "eval"}:
            if name == "eval":
                fail(shell_file, line_number)
            return
        if name in {
            "(",
            ")",
            "exit",
            "return",
            "true",
            "false",
            "printf",
            "echo",
            "[",
            ":",
            "test",
            "cd",
            "dirname",
            "pwd",
            "cat",
            "git",
            "git-lfs",
            "kill",
            "mkdir",
            "mktemp",
            "ps",
            "read",
            "rm",
            "rmdir",
            "sleep",
            "trap",
            "umask",
            "wait",
        }:
            return
        if name in function_names:
            return
        fail(shell_file, line_number)

    def trace_file(shell_file: Path, bindings: dict[str, str | None]) -> None:
        resolved = shell_file.resolve()
        if resolved in visited:
            return
        if _relative_path(root, resolved) is None:
            fail(shell_file, 1)
        visited.add(resolved)
        try:
            lines = shell_file.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError):
            fail(shell_file, 1)
            return
        current_rel = _relative_path(root, shell_file)
        current_dir = current_rel.rsplit("/", 1)[0] if current_rel and "/" in current_rel else ""
        bindings.setdefault("HOOK_DIR", current_dir)
        if current_dir.endswith("/git"):
            bindings.setdefault("GUARD_DIR", "scripts/hooks")
        function_names.update(
            match.group(1)
            for line in lines
            if (match := re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(\)\s*\{\s*$", line))
        )
        in_case = False
        index = 0
        while index < len(lines):
            original = lines[index]
            line_number = index + 1
            line = original.strip()
            index += 1
            if not line or line.startswith("#"):
                continue
            if line.endswith("\\"):
                start_line = line_number
                continued = line[:-1]
                while index < len(lines):
                    next_line = lines[index].strip()
                    index += 1
                    if next_line.endswith("\\"):
                        continued += " " + next_line[:-1]
                    else:
                        continued += " " + next_line
                        break
                line = continued
                line_number = start_line
            if line.endswith("=$(") or line == "$ (":
                # The contents are scanned as shell below, including any
                # Python heredoc body; the surrounding command substitution
                # itself only captures command output.
                continue
            if line.startswith(")"):
                tail = line[1:].strip()
                if tail == "&":
                    continue
                if tail:
                    for command_text in split_commands(tail):
                        scan_command(command_text, shell_file, line_number, bindings)
                continue
            heredoc = re.search(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1", line)
            if heredoc:
                delimiter = heredoc.group(2)
                body: list[str] = []
                while index < len(lines):
                    candidate = lines[index]
                    index += 1
                    if candidate.strip() == delimiter:
                        break
                    body.append(candidate)
                else:
                    fail(shell_file, line_number)
                try:
                    words = shlex.split(scan_substitutions(line, shell_file, line_number, bindings), comments=True)
                except ValueError:
                    fail(shell_file, line_number)
                    continue
                if any(word in {"python", "python3"} for word in words) and "-" in words:
                    # Unquoted heredocs can interpolate shell data into Python;
                    # those expansions are not a static source and degrade.
                    if not heredoc.group(1) and any("$" in item or "`" in item for item in body):
                        fail(shell_file, line_number)
                    python_source = "\n".join(body) + "\n"
                    inline.append((shell_file, python_source))
                    try:
                        tree = ast.parse(python_source)
                    except SyntaxError:
                        fail(shell_file, line_number)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Import):
                            imports = [alias.name for alias in node.names]
                        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                            imports = [node.module]
                        else:
                            continue
                        for module_name in imports:
                            local = root / "scripts/hooks" / (module_name.replace(".", "/") + ".py")
                            if local.is_file() and local not in targets:
                                targets.append(local)
                                edges.append((shell_file.resolve(), local.resolve()))
                scan_command(line, shell_file, line_number, bindings)
                continue
            if re.match(r"[A-Za-z_][A-Za-z0-9_]*\s*\(\)\s*\{\s*$", line) or re.match(
                r"function\s+[A-Za-z_][A-Za-z0-9_]*\s*\{?\s*$", line
            ):
                continue
            if line in {"{", "}", "then", "do", "else", "fi", "done", "return", "esac"}:
                if line == "esac":
                    in_case = False
                continue
            if re.match(r"done(?:\s|$)", line):
                continue
            if line.startswith("case ") and re.search(r"\s+in\s*$", line):
                in_case = True
                continue
            if re.match(r"for\s+.+\s+in\s+.+;?\s+do$", line):
                continue
            if re.match(r"(?:for|select)\s+.+\s+in\s+.+$", line) and line.endswith("; do"):
                continue
            if in_case:
                if line == "esac":
                    in_case = False
                    continue
                if ")" in line:
                    line = line.rsplit(")", 1)[1].strip().removesuffix(";;").strip()
                    if not line:
                        continue
            for command_text in split_commands(line):
                scan_command(command_text, shell_file, line_number, bindings)

    initial: dict[str, str | None] = {}
    relative = _relative_path(root, path)
    if relative and "/" in relative:
        initial["HOOK_DIR"] = relative.rsplit("/", 1)[0]
        if initial["HOOK_DIR"].endswith("/git"):
            initial["GUARD_DIR"] = "scripts/hooks"
    trace_file(path, initial)
    if passthrough is not None:
        pass_results.append(passthrough_line)
    if passthrough_line is not None and python_targets:
        fail(path, passthrough_line)
    return targets


def _file_anchored_path(node: ast.AST, anchors: dict[str, str]) -> bool:
    """Accept only Path(__file__) with optional resolve()/parent chains."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Path":
        return (
            "Path" in anchors
            and not node.keywords
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "__file__"
            and "__file__" in anchors
        )
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "resolve":
        return not node.args and not node.keywords and _file_anchored_path(node.func.value, anchors)
    if isinstance(node, ast.Attribute) and node.attr == "parent":
        return _file_anchored_path(node.value, anchors)
    return False


def _scope_for(node: ast.AST, parents: dict[ast.AST, ast.AST], tree: ast.Module) -> ast.AST:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return current
    return tree


def _scope_write_count(scope: ast.AST, name: str) -> int:
    """Count bindings in one scope without attributing nested scopes to it."""
    pending = list(reversed(list(getattr(scope, "body", ()))))
    count = 0
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, (ast.Store, ast.Del)):
            count += 1
        elif isinstance(node, ast.arg) and node.arg == name:
            count += 1
        elif isinstance(node, ast.ExceptHandler) and node.name == name:
            count += 1
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == name:
            count += 1
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            count += sum((alias.asname or alias.name.split(".")[0]) == name for alias in node.names)
        elif isinstance(node, (ast.Global, ast.Nonlocal)) and name in node.names:
            count += 1
        pending.extend(reversed(list(ast.iter_child_nodes(node))))
    return count


def _scope_file_anchor(name: str, scope: ast.AST, anchors: dict[str, str]) -> str | None:
    """Resolve one un-rebound local assigned directly from Path(__file__)."""
    if _scope_write_count(scope, name) != 1 or not hasattr(scope, "body"):
        return None
    values: list[ast.AST] = []
    for statement in scope.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name) and target.id == name:
                values.append(statement.value)
        elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
            if statement.target.id == name and statement.value is not None:
                values.append(statement.value)
    if len(values) != 1 or not _file_anchored_path(values[0], anchors):
        return None
    return _literal_path(values[0], {}, anchors)


def _scope_literal_path(name: str, scope: ast.AST, bindings: dict[str, str], anchors: dict[str, str]) -> str | None:
    """Resolve one single-assignment local path expression within its scope."""
    if _scope_write_count(scope, name) != 1 or not hasattr(scope, "body"):
        return None
    values: list[ast.AST] = []
    pending = list(reversed(list(getattr(scope, "body", ()))))
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == name:
                values.append(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == name and node.value is not None:
                values.append(node.value)
        pending.extend(reversed(list(ast.iter_child_nodes(node))))
    if len(values) != 1:
        return None
    return _literal_path(values[0], bindings, anchors)


def _ancestor_search_bases(anchor: str, root: Path) -> tuple[list[Path], bool] | None:
    """Return file ancestors through repo root and whether higher ones were clipped."""
    path = Path(anchor)
    repository = root.resolve()
    try:
        path.relative_to(repository)
    except ValueError:
        return None
    ancestors = list(path.parents)
    try:
        root_index = ancestors.index(repository)
    except ValueError:
        return None
    return ancestors[: root_index + 1], root_index + 1 < len(ancestors)


def _loop_join_segments(node: ast.AST, loop_name: str) -> tuple[str, ...] | None:
    """Recognize a loop path joined only with constant string segments."""
    if isinstance(node, ast.Name):
        return () if node.id == loop_name else None
    if (
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.Div)
        and isinstance(node.right, ast.Constant)
        and type(node.right.value) is str
    ):
        left = _loop_join_segments(node.left, loop_name)
        return left + (node.right.value,) if left is not None else None
    return None


def _loop_body_nodes(loop: ast.For) -> list[ast.AST]:
    """Walk one loop body without crossing nested scopes or loops."""
    found: list[ast.AST] = []
    pending = list(reversed(loop.body))
    while pending:
        node = pending.pop()
        if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda, ast.For, ast.AsyncFor, ast.While)
        ):
            continue
        found.append(node)
        pending.extend(reversed(list(ast.iter_child_nodes(node))))
    return found


def _dynamic_load_argument(node: ast.Call, name: str) -> ast.AST | None:
    position = 1 if name == "spec_from_file_location" else 0
    keyword_name = {
        "spec_from_file_location": "location",
        "run_path": "path_name",
        "run_module": "mod_name",
    }[name]
    return (
        node.args[position]
        if len(node.args) > position
        else next((keyword.value for keyword in node.keywords if keyword.arg == keyword_name), None)
    )


def _run_guard_sibling_targets(
    tree: ast.Module, node: ast.Call, root: Path, parents: dict[ast.AST, ast.AST]
) -> set[Path] | None:
    """Resolve _run_guard's finite helper-name loader from its literal callsites."""
    scope = _scope_for(node, parents, tree)
    if not isinstance(scope, ast.FunctionDef) or scope.name != "_load_sibling_module":
        return None
    if len(node.args) < 2 or not isinstance(node.args[1], ast.Name) or node.args[1].id != "path":
        return None
    path_values: list[ast.AST] = []
    for statement in scope.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name) and target.id == "path":
                path_values.append(statement.value)
    if len(path_values) != 1:
        return None
    expression = path_values[0]
    if (
        not isinstance(expression, ast.BinOp)
        or not isinstance(expression.op, ast.Div)
        or not isinstance(expression.left, ast.Name)
        or expression.left.id != "_HOOKS_DIR"
        or not isinstance(expression.right, ast.JoinedStr)
    ):
        return None
    pieces = expression.right.values
    if (
        len(pieces) != 2
        or not isinstance(pieces[0], ast.FormattedValue)
        or not isinstance(pieces[0].value, ast.Name)
        or pieces[0].value.id != "name"
        or not isinstance(pieces[1], ast.Constant)
        or pieces[1].value != ".py"
    ):
        return None

    names: set[str] = set()
    for candidate in ast.walk(tree):
        if not isinstance(candidate, ast.Call) or not isinstance(candidate.func, ast.Name):
            continue
        if candidate.func.id != "_load_sibling_module":
            continue
        if (
            not candidate.args
            or not isinstance(candidate.args[0], ast.Constant)
            or not isinstance(candidate.args[0].value, str)
        ):
            return None
        names.add(candidate.args[0].value)
    targets = {root / "scripts/hooks" / f"{name}.py" for name in names}
    return targets if names and all(path.is_file() for path in targets) else None


def _declared_generator_target(
    tree: ast.Module, node: ast.Call, root: Path, source: Path, parents: dict[ast.AST, ast.AST]
) -> Path | None:
    """Resolve the Make-declared payload script loaded by overlay_tooling._generator."""
    if _relative_path(root, source) != "packages/workbay-system/workbay_system/overlay_tooling/_generator.py":
        return None
    scope = _scope_for(node, parents, tree)
    if not isinstance(scope, ast.FunctionDef) or scope.name != "main":
        return None
    if len(node.args) < 2 or not isinstance(node.args[1], ast.Name) or node.args[1].id != "path":
        return None
    path_values = [
        statement.value
        for statement in scope.body
        if isinstance(statement, ast.Assign)
        and len(statement.targets) == 1
        and isinstance(statement.targets[0], ast.Name)
        and statement.targets[0].id == "path"
    ]
    if (
        len(path_values) != 1
        or not isinstance(path_values[0], ast.Call)
        or not isinstance(path_values[0].func, ast.Name)
        or path_values[0].func.id != "_generator_script"
        or path_values[0].args
        or path_values[0].keywords
    ):
        return None
    generator = next(
        (child for child in tree.body if isinstance(child, ast.FunctionDef) and child.name == "_generator_script"),
        None,
    )
    if (
        generator is None
        or generator.args.args
        or generator.args.kwonlyargs
        or generator.args.vararg
        or generator.args.kwarg
    ):
        return None
    returns = [child for child in ast.walk(generator) if isinstance(child, ast.Return) and child.value is not None]
    if len(returns) != 1:
        return None
    expression = returns[0].value
    suffix: list[str] = []
    while isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Div):
        if not isinstance(expression.right, ast.Constant) or not isinstance(expression.right.value, str):
            return None
        suffix.append(expression.right.value)
        expression = expression.left
    if not isinstance(expression, ast.Name) or expression.id != "PACKAGE_ROOT":
        return None
    suffix.reverse()
    if suffix != ["workbay_system", "payload", "scripts", "generate_agent_workflows.py"]:
        return None
    imports_package_root = any(
        isinstance(child, ast.ImportFrom)
        and child.module == "workbay_system.overlay_tooling._paths"
        and any(alias.name == "PACKAGE_ROOT" and alias.asname in {None, "PACKAGE_ROOT"} for alias in child.names)
        for child in tree.body
    )
    if not imports_package_root:
        return None
    package_root = root / "packages/workbay-system"
    target = package_root.joinpath(*suffix)
    target_rel = _relative_path(root, target)
    declared = _MAKE_TARGET_ENTRYPOINTS.get("generate-agent-workflows", frozenset())
    return target if target_rel in declared and target.is_file() else None


def _finite_expression_paths(
    node: ast.AST,
    bindings: dict[str, str | set[str]],
    anchors: dict[str, str],
    literal_maps: dict[str, dict[str, str]],
) -> set[str] | None:
    """Evaluate bounded literal path expressions and finite dictionary values."""
    fixed_bindings = {key: value for key, value in bindings.items() if isinstance(value, str)}
    literal = _literal_path(node, fixed_bindings, anchors)
    if literal is not None:
        return {literal}
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, ast.Name):
        value = bindings.get(node.id)
        if isinstance(value, str):
            return {value}
        return set(value) if isinstance(value, set) else None
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
        mapping = literal_maps.get(node.value.id)
        if mapping is None:
            return None
        if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
            value = mapping.get(node.slice.value)
            return {value} if value is not None else None
        return set(mapping.values()) if mapping else None
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Div, ast.Add)):
        left = _finite_expression_paths(node.left, bindings, anchors, literal_maps)
        right = _finite_expression_paths(node.right, bindings, anchors, literal_maps)
        if left is None or right is None:
            return None
        if isinstance(node.op, ast.Div):
            return {str(Path(parent) / child) for parent in left for child in right}
        return {parent + child for parent in left for child in right}
    if isinstance(node, ast.Call) and not node.keywords:
        if isinstance(node.func, ast.Name) and node.func.id in {"str", "Path"} and len(node.args) == 1:
            return _finite_expression_paths(node.args[0], bindings, anchors, literal_maps)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "resolve" and not node.args:
            values = _finite_expression_paths(node.func.value, bindings, anchors, literal_maps)
            return {str(Path(value).resolve()) for value in values} if values is not None else None
    return None


def _scope_finite_paths(
    name: str,
    scope: ast.AST,
    bindings: dict[str, str],
    anchors: dict[str, str],
    literal_maps: dict[str, dict[str, str]],
) -> set[str] | None:
    """Resolve a single-write local path from a finite literal dictionary."""
    if not isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)) or _scope_write_count(scope, name) != 1:
        return None
    local_values: dict[str, str | set[str]] = dict(bindings)
    assignments: list[tuple[str, ast.AST]] = []
    pending = list(reversed(scope.body))
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            assignments.append((node.targets[0].id, node.value))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
            assignments.append((node.target.id, node.value))
        pending.extend(reversed(list(ast.iter_child_nodes(node))))
    for _ in range(max(1, len(assignments))):
        changed = False
        for target, expression in assignments:
            if _scope_write_count(scope, target) != 1:
                continue
            value = _finite_expression_paths(expression, local_values, anchors, literal_maps)
            if value is not None and local_values.get(target) != value:
                local_values[target] = value
                changed = True
        if not changed:
            break
    value = local_values.get(name)
    if isinstance(value, str):
        return {value}
    return set(value) if isinstance(value, set) else None


def _reflective_attribute_is_data(node: ast.Attribute, parents: dict[ast.AST, ast.AST]) -> bool:
    """Exempt only un-rebound method fields and attributes consumed as booleans."""
    if isinstance(node.ctx, (ast.Store, ast.Del)):
        if not isinstance(node.value, ast.Name) or node.value.id != "self":
            return False
        scope: ast.AST = node
        while scope in parents:
            scope = parents[scope]
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
                break
        if not isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return False
        if not isinstance(parents.get(scope), ast.ClassDef):
            return False
        positional = scope.args.posonlyargs + scope.args.args
        if not positional or positional[0].arg != "self":
            return False
        # Include every binding form, even in nested code: uncertainty about
        # the receiver must retain the module's opaque classification.
        for child in ast.walk(scope):
            if isinstance(child, ast.Name) and child.id == "self" and isinstance(child.ctx, (ast.Store, ast.Del)):
                return False
            if isinstance(child, ast.arg) and child is not positional[0] and child.arg == "self":
                return False
            if (
                child is not scope
                and isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.ExceptHandler, ast.MatchAs, ast.MatchStar))
                and child.name == "self"
            ):
                return False
            if isinstance(child, ast.alias) and (child.asname or child.name.split(".")[0]) == "self":
                return False
            if isinstance(child, (ast.Global, ast.Nonlocal)) and "self" in child.names:
                return False
            if isinstance(child, ast.MatchMapping) and child.rest == "self":
                return False
        return True
    if isinstance(node.ctx, ast.Load):
        current: ast.AST = node
        parent = parents.get(current)
        while isinstance(parent, ast.BoolOp):
            current = parent
            parent = parents.get(current)
        return (
            isinstance(parent, (ast.If, ast.While, ast.IfExp, ast.Assert)) and parent.test is current
        ) or (isinstance(parent, ast.UnaryOp) and isinstance(parent.op, ast.Not) and parent.operand is current)
    return False


def _dynamic_loads(
    tree: ast.Module,
    root: Path,
    local: set[Path],
    unresolved: set[str],
    source: Path,
    reachable: set[Path],
    notes: set[str],
    *,
    source_has_file_anchor: bool = True,
) -> None:
    """Fail closed on mutable names, independently of scope or execution order.

    Count bindings across the entire AST, then discover calls with the same
    exhaustive traversal. No deferred execution or lexical scope is simulated.
    """
    stores: dict[str, int] = {}
    opaque = False
    reflective = {"exec", "eval", "globals", "locals", "vars", "setattr", "__import__"}
    parents: dict[ast.AST, ast.AST] = {
        child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)
    }
    for child in ast.walk(tree):
        names: list[str] = []
        if isinstance(child, ast.Name):
            if isinstance(child.ctx, (ast.Store, ast.Del)):
                names.append(child.id)
            if child.id in reflective:
                opaque = True
        elif (
            isinstance(child, ast.Attribute)
            and child.attr in reflective
            and not _reflective_attribute_is_data(child, parents)
        ):
            opaque = True
        elif isinstance(child, ast.arg):
            names.append(child.arg)
        elif isinstance(child, (ast.Global, ast.Nonlocal)):
            names.extend(child.names)
        elif isinstance(child, (ast.Import, ast.ImportFrom)):
            names.extend(alias.asname or alias.name.split(".")[0] for alias in child.names)
            if any(alias.name in reflective for alias in child.names):
                opaque = True
            if isinstance(child, ast.ImportFrom) and any(alias.name == "*" for alias in child.names):
                opaque = True
        elif isinstance(
            child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.ExceptHandler, ast.MatchAs, ast.MatchStar)
        ):
            if child.name is not None:
                names.append(child.name)
        elif isinstance(child, ast.MatchMapping) and child.rest is not None:
            names.append(child.rest)
        for name in names:
            stores[name] = stores.get(name, 0) + 1

    anchors: dict[str, str] = {}
    stable: dict[str, str] = {}
    literal_maps: dict[str, dict[str, str]] = {}
    if not opaque:
        if source_has_file_anchor and "__file__" not in stores:
            anchors["__file__"] = str(source)
        if "str" not in stores:
            anchors["str"] = ""
        for statement in tree.body:
            if (
                isinstance(statement, ast.Assign)
                and isinstance(statement.value, ast.Dict)
                and len(statement.targets) == 1
            ):
                target = statement.targets[0]
                mapping: dict[str, str] = {}
                for key, value in zip(statement.value.keys, statement.value.values, strict=True):
                    if not (
                        isinstance(key, ast.Constant)
                        and isinstance(key.value, str)
                        and isinstance(value, ast.Constant)
                        and isinstance(value.value, str)
                    ):
                        mapping = {}
                        break
                    mapping[key.value] = value.value
                is_mutated = any(
                    isinstance(candidate, ast.Subscript)
                    and isinstance(candidate.ctx, ast.Store)
                    and isinstance(candidate.value, ast.Name)
                    and candidate.value.id == getattr(target, "id", None)
                    for candidate in ast.walk(tree)
                )
                if isinstance(target, ast.Name) and stores.get(target.id) == 1 and mapping and not is_mutated:
                    literal_maps[target.id] = mapping
            if isinstance(statement, ast.ImportFrom) and statement.module == "pathlib" and not statement.level:
                if any(alias.name == "Path" and alias.asname in {None, "Path"} for alias in statement.names):
                    if stores.get("Path") == 1:
                        anchors["Path"] = ""
        # Literal file-anchored assignments may appear inside a function, as in
        # a lazy helper loader. Resolve only single-write names, iterating so a
        # chain of local aliases remains finite and mutation still degrades.
        for _ in range(max(1, len(stores))):
            changed = False
            for statement in ast.walk(tree):
                if not isinstance(statement, (ast.Assign, ast.AnnAssign)) or statement.value is None:
                    continue
                value = _literal_path(statement.value, stable, anchors)
                targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                for target in targets:
                    if (
                        isinstance(target, ast.Name)
                        and stores.get(target.id) == 1
                        and value is not None
                        and stable.get(target.id) != value
                    ):
                        stable[target.id] = value
                        changed = True
            if not changed:
                break

    # Model only the repository's ancestor-search loader idiom. The iterator
    # must be .parents on Path(__file__) (optionally resolved / parent-chained),
    # or on one unique local assigned from that exact anchor. Its loaded path
    # must join the loop variable to constant string segments only.
    loop_call_targets: dict[int, set[Path]] = {}
    for loop in ast.walk(tree):
        if not isinstance(loop, ast.For) or not isinstance(loop.target, ast.Name):
            continue
        loop_name = loop.target.id
        if not isinstance(loop.iter, ast.Attribute) or loop.iter.attr != "parents":
            continue
        scope = _scope_for(loop, parents, tree)
        if _scope_write_count(scope, loop_name) != 1:
            continue
        anchor_expr = loop.iter.value
        if _file_anchored_path(anchor_expr, anchors):
            anchor = _literal_path(anchor_expr, {}, anchors)
        elif isinstance(anchor_expr, ast.Name):
            anchor = _scope_file_anchor(anchor_expr.id, scope, anchors)
        else:
            anchor = None
        bases = _ancestor_search_bases(anchor, root) if anchor is not None else None
        if bases is None:
            continue
        ancestor_dirs, ignored_outside = bases

        body_nodes = _loop_body_nodes(loop)
        candidate_segments: dict[str, tuple[tuple[str, ...], int]] = {}
        candidate_writes: dict[str, int] = {}
        for child in body_nodes:
            if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
                candidate_writes[child.id] = candidate_writes.get(child.id, 0) + 1
        scope_has_global = any(
            isinstance(child, (ast.Global, ast.Nonlocal)) and any(name in child.names for name in candidate_writes)
            for child in ast.walk(scope)
        )
        if scope_has_global:
            continue

        for statement_index, statement in enumerate(loop.body):
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
                target = statement.targets[0]
                expression = statement.value
            elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
                target = statement.target
                expression = statement.value
            else:
                continue
            if not isinstance(target, ast.Name):
                continue
            segments = _loop_join_segments(expression, loop_name)
            if segments:
                candidate_segments[target.id] = (segments, statement_index)

        candidate_segments = {
            name: value
            for name, value in candidate_segments.items()
            if candidate_writes.get(name) == 1 and _scope_write_count(scope, name) == 1
        }
        for child in body_nodes:
            if not isinstance(child, ast.Call):
                continue
            name = (
                child.func.attr
                if isinstance(child.func, ast.Attribute)
                else (child.func.id if isinstance(child.func, ast.Name) else "")
            )
            if name not in {"spec_from_file_location", "run_path"}:
                continue
            argument = _dynamic_load_argument(child, name)
            statement = child
            while parents.get(statement) is not loop:
                statement = parents.get(statement, loop)
            statement_index = loop.body.index(statement) if statement in loop.body else -1
            if isinstance(argument, ast.Name) and argument.id in candidate_segments:
                segments, assignment_index = candidate_segments[argument.id]
                if assignment_index >= statement_index:
                    continue
            else:
                segments = _loop_join_segments(argument, loop_name) if argument is not None else None
            if not segments:
                continue
            targets: set[Path] = set()
            for directory in ancestor_dirs:
                target = directory.joinpath(*segments)
                relative = _relative_path(root, target)
                if relative is None:
                    raise ValueError("dynamic_hook_load_unparsed")
                repository_target = root / relative
                targets.add(repository_target)
                if repository_target.is_file():
                    local.add(repository_target)
            reachable.update(targets)
            loop_call_targets[id(child)] = targets
            if ignored_outside:
                notes.add("ancestor_search_outside_repo_ignored")

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = (
            node.func.attr
            if isinstance(node.func, ast.Attribute)
            else (node.func.id if isinstance(node.func, ast.Name) else "")
        )
        if name not in {"spec_from_file_location", "run_path", "run_module"}:
            continue
        if id(node) in loop_call_targets:
            continue
        argument = _dynamic_load_argument(node, name)
        literal = _literal_path(argument, stable, anchors) if argument is not None else None
        if literal is None and isinstance(argument, ast.Name) and not opaque:
            scope = _scope_for(node, parents, tree)
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
                literal = _scope_literal_path(argument.id, scope, stable, anchors)
        if not literal:
            sibling_targets = (
                _run_guard_sibling_targets(tree, node, root, parents)
                if source.name == "_run_guard.py" and name == "spec_from_file_location"
                else None
            )
            if sibling_targets is not None:
                local.update(sibling_targets)
                reachable.update(sibling_targets)
                continue
            generator_target = (
                _declared_generator_target(tree, node, root, source, parents)
                if name == "spec_from_file_location"
                else None
            )
            if generator_target is not None:
                local.add(generator_target)
                reachable.add(generator_target)
                continue
            finite_targets: set[str] | None = None
            if argument is not None and not opaque:
                scope = _scope_for(node, parents, tree)
                if isinstance(argument, ast.Name):
                    finite_targets = _scope_finite_paths(argument.id, scope, stable, anchors, literal_maps)
                elif (
                    isinstance(argument, ast.Call)
                    and isinstance(argument.func, ast.Name)
                    and argument.func.id == "str"
                    and len(argument.args) == 1
                    and isinstance(argument.args[0], ast.Name)
                ):
                    finite_targets = _scope_finite_paths(argument.args[0].id, scope, stable, anchors, literal_maps)
            if not finite_targets:
                raise ValueError("dynamic_hook_load_unparsed")
            for finite_literal in finite_targets:
                target = Path(finite_literal)
                if not target.is_absolute():
                    target = root / target
                if _relative_path(root, target) is None or not target.is_file():
                    raise ValueError("dynamic_hook_load_unparsed")
                if name == "run_module":
                    unresolved.add(finite_literal)
                else:
                    local.add(target)
                    reachable.add(target)
            continue
        if name == "run_module":
            unresolved.add(literal)
        else:
            target = root / literal
            if _relative_path(root, target) is None or not target.is_file():
                raise ValueError("dynamic_hook_load_unparsed")
            local.add(target)


def _imports(
    path: Path,
    module_name: str,
    modules: dict[str, Path],
    root: Path,
    sources: dict[Path, str],
    notes: set[str],
    *,
    source_text: str | None = None,
) -> tuple[set[Path], set[str], set[Path]]:
    try:
        if source_text is None:
            payload = path.read_bytes()
            sources[path] = hashlib.sha256(payload).hexdigest()
            source = payload.decode("utf-8")
        else:
            source = source_text
        tree = ast.parse(source)
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise ValueError(f"hook_parse_failed:{path}") from exc

    local: set[Path] = set()
    unresolved: set[str] = set()
    reachable: set[Path] = set()
    _dynamic_loads(tree, root, local, unresolved, path, reachable, notes, source_has_file_anchor=source_text is None)
    for target in reachable:
        if not target.is_file():
            sources.setdefault(target, _MISSING_SOURCE)
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parent = module_name.split(".")[:-1]
                if node.level <= len(parent):
                    base = ".".join(parent[: len(parent) - node.level + 1] + ([base] if base else []))
            if base:
                names.append(base)
            for alias in node.names:
                child = ".".join(part for part in (base, alias.name) if part)
                if child:
                    names.append(child)
        for name in names:
            target = _resolve_local_module(name, modules)
            if target is not None:
                local.add(target)
            elif name and name != "*":
                unresolved.add(name)
    return local, unresolved, reachable


def _relative_path(root: Path, path: str | Path) -> str | None:
    try:
        return Path(path).resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        return None


def _source_tree(origin: str | None, locations: object, module_name: str, root: Path) -> Path | None:
    parts = module_name.split(".")
    if isinstance(locations, list):
        for location in locations:
            if not isinstance(location, str):
                continue
            candidate = Path(location)
            for _ in range(max(0, len(parts) - 1)):
                candidate = candidate.parent
            candidate = candidate.parent
            if _relative_path(root, candidate) is not None:
                return candidate
    if not isinstance(origin, str) or origin in {"built-in", "frozen"}:
        return None
    candidate = Path(origin)
    if not candidate.is_absolute():
        candidate = root / candidate
    if _relative_path(root, candidate) is None:
        return None
    source = candidate.parent
    if candidate.name == "__init__.py":
        for _ in range(len(parts)):
            source = source.parent
    else:
        for _ in range(max(0, len(parts) - 1)):
            source = source.parent
    if _relative_path(root, source) is None:
        return None
    return source


def _record_best(chains: dict[str, list[str]], path: str, chain: list[str]) -> None:
    previous = chains.get(path)
    if previous is None or len(chain) < len(previous):
        chains[path] = chain


def _build_result(root: Path, runner: Callable[..., object], interpreter: str, sources: dict[Path, str]) -> ReachResult:
    hook_root = root / "scripts/hooks"
    payload_root = f"{_PAYLOAD_HOOK_ROOT}/"
    paths: set[str] = set()
    chains: dict[str, list[str]] = {}
    notes: set[str] = set()

    def finish(status: Literal["ok", "degraded"], cause: str = "") -> ReachResult:
        return ReachResult(frozenset(paths), chains, status, cause, tuple(sorted(notes)))

    config_files = _config_files(root)
    config_invalid = False
    entrypoints: set[str] = set()
    configured_commands: list[str] = []

    for config in config_files:
        try:
            document = json.loads(config.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            config_invalid = True
            continue
        for command in _commands(document):
            configured_commands.append(command)
            entrypoints.update(_paths_in_command(command))

    git_hooks = hook_root / "git"
    if git_hooks.is_dir():
        entrypoints.update(
            path.relative_to(root).as_posix() for path in git_hooks.iterdir() if path.is_file() and not path.suffix
        )

    for entrypoint in sorted(entrypoints):
        paths.add(entrypoint)
        chains[entrypoint] = [entrypoint]
        if entrypoint.startswith("scripts/hooks/"):
            twin = payload_root + entrypoint.removeprefix("scripts/hooks/")
            paths.add(twin)
            chains[twin] = [entrypoint, twin]

    if config_invalid:
        return finish("degraded", "hook_config_invalid")
    if not interpreter:
        return finish("degraded", "interp_missing")

    modules = _module_files(hook_root)
    github_hooks = root / ".github/hooks"
    if github_hooks.is_dir():
        modules.update(_module_files(github_hooks))
    system_package = root / "packages/workbay-system/workbay_system"
    if system_package.is_dir():
        modules.update(_module_files(system_package, prefix="workbay_system."))
    pending: list[tuple[Path, list[str]]] = []
    inline_pending: list[tuple[Path, list[str], str]] = []
    shell_chains: dict[Path, list[str]] = {}
    for entrypoint in sorted(entrypoints):
        path = root / entrypoint
        if path.suffix == ".py" and path.is_file():
            pending.append((path, [entrypoint]))
        elif path.is_file():
            inline_sources: list[tuple[Path, str]] = []
            shell_edges: list[tuple[Path, Path]] = []
            passthrough: list[int | None] = []
            try:
                shell_targets = _shell_targets(
                    path,
                    root,
                    inline_sources=inline_sources,
                    passthrough=passthrough,
                    shell_edges=shell_edges,
                )
            except (OSError, UnicodeError, ValueError) as exc:
                cause = str(exc)
                if not cause.startswith("shell_hook_unparsed:"):
                    cause = f"shell_hook_unparsed:{entrypoint}:1"
                return finish("degraded", cause)
            if path.name == "_launch_python.sh":
                pass_line = passthrough[0] if passthrough else None
                if pass_line is None:
                    return finish("degraded", f"shell_hook_unparsed:{entrypoint}:1")
                for command in configured_commands:
                    command_paths = _paths_in_command(command)
                    for index, command_path in enumerate(command_paths):
                        if command_path == entrypoint and not any(
                            candidate.endswith(".py") for candidate in command_paths[index + 1 :]
                        ):
                            return finish("degraded", f"shell_hook_unparsed:{entrypoint}:{pass_line}")

            edge_map: dict[Path, list[Path]] = {}
            for parent, child in shell_edges:
                edge_map.setdefault(parent.resolve(), []).append(child.resolve())
            start = path.resolve()
            shell_chains[start] = [entrypoint]
            queue = [start]
            while queue:
                shell_parent = queue.pop(0)
                parent_chain = shell_chains[shell_parent]
                for child in edge_map.get(shell_parent, []):
                    child_rel = _relative_path(root, child)
                    if child_rel is None:
                        return finish("degraded", f"shell_hook_unparsed:{entrypoint}:1")
                    child_chain = parent_chain + [child_rel]
                    paths.add(child_rel)
                    _record_best(chains, child_rel, child_chain)
                    if child.suffix == ".py":
                        pending.append((child, child_chain))
                    if child.suffix == ".sh" and child not in shell_chains:
                        shell_chains[child] = child_chain
                        queue.append(child)
            for shell_file, source_text in inline_sources:
                source_chain = shell_chains.get(
                    shell_file.resolve(), [entrypoint, _relative_path(root, shell_file) or entrypoint]
                )
                inline_pending.append((shell_file, source_chain, source_text))
            # Keep a shell edge that the parser reported even if it had no
            # executable Python child (for example the launcher's argv pass-through).
            for target in shell_targets:
                target_rel = _relative_path(root, target)
                if target_rel is not None and target.suffix == ".sh":
                    paths.add(target_rel)

    imports_to_probe: dict[str, list[list[str]]] = {}
    visited: set[str] = set()
    work: list[tuple[Path, list[str], str | None]] = [(*item, None) for item in pending]
    work.extend((*item[:2], item[2]) for item in inline_pending)
    while work:
        current, chain, source_text = work.pop()
        relative = _relative_path(root, current)
        visit_key = (
            relative if source_text is None else f"{relative}#inline:{hashlib.sha256(source_text.encode()).hexdigest()}"
        )
        if relative is None or visit_key in visited:
            continue
        visited.add(visit_key)
        paths.add(relative)
        _record_best(chains, relative, chain)
        module_name = current.stem
        if current.is_relative_to(hook_root):
            module_name = current.relative_to(hook_root).with_suffix("").as_posix().replace("/", ".")
        elif current.is_relative_to(github_hooks):
            module_name = current.relative_to(github_hooks).with_suffix("").as_posix().replace("/", ".")
        elif current.is_relative_to(system_package):
            module_name = "workbay_system." + current.relative_to(system_package).with_suffix("").as_posix().replace(
                "/", "."
            )
        try:
            local_imports, external_imports, dynamic_reach = _imports(
                current,
                module_name,
                modules,
                root,
                sources,
                notes,
                source_text=source_text,
            )
        except ValueError as exc:
            return finish("degraded", str(exc).split(":", 1)[0])
        for target in dynamic_reach:
            target_rel = _relative_path(root, target)
            if target_rel is not None:
                paths.add(target_rel)
                _record_best(chains, target_rel, chain + [target_rel])
        for child in sorted(local_imports, key=str):
            child_rel = _relative_path(root, child)
            if child_rel is not None:
                work.append((child, chain + [child_rel], None))
        for name in external_imports:
            imports_to_probe.setdefault(name, []).append(chain)

    if not imports_to_probe:
        return finish("ok")

    command = [interpreter, "-c", _PROBE_PROGRAM, json.dumps(sorted(imports_to_probe))]
    try:
        completed = runner(
            command,
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=REACH_PROBE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return finish("degraded", "spec_probe_timeout")
    except OSError:
        return finish("degraded", "spec_probe_failed")
    if getattr(completed, "returncode", 1) != 0:
        return finish("degraded", "spec_probe_failed")
    stdout = getattr(completed, "stdout", "")
    if isinstance(stdout, bytes):
        try:
            stdout = stdout.decode("utf-8")
        except UnicodeError:
            return finish("degraded", "spec_probe_failed")
    try:
        result = json.loads(stdout)
    except (TypeError, json.JSONDecodeError):
        return finish("degraded", "spec_probe_failed")
    if not isinstance(result, dict) or set(result) != set(imports_to_probe):
        return finish("degraded", "spec_probe_failed")

    for name in sorted(imports_to_probe):
        value = result[name]
        if not isinstance(value, dict):
            return finish("degraded", "spec_probe_failed")
        origin_value = value.get("origin")
        locations = value.get("submodule_search_locations")
        if origin_value is not None and not isinstance(origin_value, str):
            return finish("degraded", "spec_probe_failed")
        if locations is not None and (
            not isinstance(locations, list) or not all(isinstance(location, str) for location in locations)
        ):
            return finish("degraded", "spec_probe_failed")
        source = _source_tree(origin_value, locations, name, root)
        if source is None:
            continue
        source_rel = _relative_path(root, source)
        if source_rel is None:
            continue
        paths.add(source_rel)
        best_chain = min(imports_to_probe[name], key=len)
        origin = origin_value
        origin_rel = _relative_path(root, origin) if isinstance(origin, str) else None
        evidence = list(best_chain)
        if origin_rel is not None and origin_rel not in evidence:
            evidence.append(origin_rel)
        _record_best(chains, source_rel, evidence)

    return finish("ok")


def hook_reach_set(
    repo_root: str | Path,
    *,
    runner: Callable[..., object] = subprocess.run,
) -> ReachResult:
    """Return hook entrypoints and their repository-local import source trees.

    A failed interpreter/spec probe is explicitly degraded. Callers must treat
    every ``packages/*/src/**`` path as reachable in that state.
    """
    root = Path(repo_root).resolve()
    interpreter = _resolve_hook_interpreter(root)
    key = _content_key(root, interpreter, _config_files(root))
    root_key = str(root)
    cached = _CACHE.get(root_key)
    if cached is not None and cached[0] == key:
        try:
            unchanged = all(
                not path.is_file()
                if digest == _MISSING_SOURCE
                else hashlib.sha256(path.read_bytes()).hexdigest() == digest
                for path, digest in cached[2].items()
            )
        except OSError:
            unchanged = False
        if unchanged:
            return cached[1]
    sources: dict[Path, str] = {}
    result = _build_result(root, runner, interpreter, sources)
    _CACHE[root_key] = (key, result, sources)
    return result


def _clear_hook_reach_cache() -> None:
    """Private test seam for isolating process-cache assertions."""
    _CACHE.clear()
