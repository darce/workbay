"""Fail-closed classification for advisory style/type verification.

Dispatch callers get a static, side-effect-free shape check. The runtime gate
passes its PATH and PYTHONPATH, which enables stronger executable/module
origin checks and contained ``make -n`` recipe inspection [SEC-01][WEB-24]
[ARCH-13]. Unknown syntax, origins, and write-mode flags remain blocking.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path

__all__ = ["STYLE_MAKE_TARGETS", "STYLE_PROGRAMS", "is_style_only_command"]

STYLE_PROGRAMS = frozenset(
    {
        "ruff",
        "mypy",
        "black",
        "isort",
        "pyright",
        "flake8",
        "pylint",
        "docformatter",
    }
)

# Runtime downgrades use exact target names only. The subject controls its
# Makefile, so each accepted target is also dry-run and recursively classified.
STYLE_MAKE_TARGETS = frozenset({"lint", "lint-scripts", "format-check", "mypy", "types"})

# Dispatch warnings are not merge decisions. Recognize common package-scoped
# lint/type targets statically so API request processing never evaluates a
# caller-selected Makefile. Deliberately exclude format/fix targets.
_DISPATCH_STYLE_MAKE_TARGET_RE = re.compile(r"^(?:lint|mypy|typecheck)-[a-z0-9][a-z0-9._-]*$")
_ASSIGNMENT_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=")
_SEGMENT_SEPARATORS = frozenset({"&&", "||", ";", "\n"})
_UNSAFE_SHELL_TOKENS = frozenset({"|", "&", ">", ">>", "<", "<<", ";;"})
_UNTRUSTED_ENV_ASSIGNMENTS = frozenset({"PATH", "PYTHONPATH", "PYTHONHOME"})
_UNTRUSTED_MAKE_ASSIGNMENTS = _UNTRUSTED_ENV_ASSIGNMENTS | frozenset({"SHELL", "MAKE", "MAKEFLAGS"})
_MAX_MAKE_DEPTH = 4


def _origin_is_trusted(
    origin: Path,
    *,
    subject_root: Path,
    interpreter_roots: tuple[Path, ...] | None,
) -> bool:
    """Trust external origins, or subject origins under the gate environment."""
    if not _is_within(origin, subject_root):
        return True
    if interpreter_roots is None:
        return False
    return any(_is_within(origin, root) for root in interpreter_roots)


def _tokenize(command: str) -> list[str] | None:
    if "$(" in command or "`" in command:
        return None
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|<>\n")
        lexer.whitespace = " \t\r"
        lexer.whitespace_split = True
        lexer.commenters = ""
        return list(lexer)
    except ValueError:
        return None


def _has_path_separator(program: str) -> bool:
    return "/" in program or "\\" in program


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _resolved_trusted_program(
    program: str,
    *,
    path: str,
    cwd: Path,
    subject_root: Path,
    interpreter_roots: tuple[Path, ...] | None = None,
) -> Path | None:
    resolved = shutil.which(program, path=path)
    if resolved is None:
        return None
    candidate = Path(resolved)
    executable = candidate if candidate.is_absolute() else cwd / candidate
    executable = executable.absolute()
    # Inspect the physical target, but retain the invoked symlink path: Python
    # uses that path to discover its virtualenv and therefore its real module
    # search path.
    return (
        executable
        if _origin_is_trusted(
            executable.resolve(),
            subject_root=subject_root,
            interpreter_roots=interpreter_roots,
        )
        else None
    )


def _interpreter_environment_roots(
    executable: Path,
    *,
    cwd: Path,
    pythonpath: str | None,
) -> tuple[Path, ...] | None:
    """Return roots reported by the gate interpreter, or fail closed.

    ``sys.base_prefix`` describes the interpreter below a virtualenv and can
    be broad (for example ``/usr``), so it is evidence for validating the
    probe but not itself a trust root. Exact package roots from ``site`` are
    included because an interpreter may legitimately import system packages.
    """
    probe = (
        "import json,site,sys; "
        "print(json.dumps({"
        "'prefix': sys.prefix, "
        "'base_prefix': sys.base_prefix, "
        "'site_packages': site.getsitepackages(),"
        "}))"
    )
    env: dict[str, str] = dict(os.environ)
    env.pop("PYTHONHOME", None)
    if pythonpath is not None:
        env["PYTHONPATH"] = pythonpath
    try:
        result = subprocess.run(
            [str(executable), "-I", "-c", probe],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        located = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(located, dict):
        return None
    prefix = located.get("prefix")
    base_prefix = located.get("base_prefix")
    site_packages = located.get("site_packages")
    if (
        not isinstance(prefix, str)
        or not prefix
        or not isinstance(base_prefix, str)
        or not base_prefix
        or not isinstance(site_packages, list)
        or any(not isinstance(item, str) or not item for item in site_packages)
    ):
        return None
    if not Path(prefix).is_absolute() or not Path(base_prefix).is_absolute():
        return None

    try:
        resolved_prefix = Path(prefix).resolve()
        resolved_base_prefix = Path(base_prefix).resolve()
    except (OSError, RuntimeError, ValueError):
        return None

    roots: list[Path] = []
    # A system interpreter reports the same prefix and base_prefix. Its prefix
    # can be broad (for example ``/usr``), so trusting it would let any subject
    # path beneath that directory masquerade as an environment-owned origin.
    # A distinct prefix is the interpreter's virtual-environment boundary.
    if resolved_prefix != resolved_base_prefix:
        roots.append(resolved_prefix)
    for item in site_packages:
        candidate = Path(item)
        if not candidate.is_absolute():
            return None
        try:
            resolved = candidate.resolve()
        except (OSError, RuntimeError, ValueError):
            return None
        if resolved not in roots:
            roots.append(resolved)
    return tuple(roots) if roots else None


def _python_module_is_trusted(
    module: str,
    *,
    executable: Path,
    cwd: Path,
    pythonpath: str | None,
    subject_root: Path,
    interpreter_roots: tuple[Path, ...] | None = None,
) -> bool:
    """Locate a top-level module without importing it and reject subject origins."""
    probe = (
        "import importlib.util,json,sys; "
        "s=importlib.util.find_spec(sys.argv[1]); "
        "print(json.dumps(None if s is None else "
        "[s.origin, list(s.submodule_search_locations or [])]))"
    )
    env: dict[str, str] = dict(os.environ)
    env.pop("PYTHONHOME", None)
    if pythonpath is not None:
        env["PYTHONPATH"] = pythonpath
    try:
        result = subprocess.run(
            [str(executable), "-c", probe, module],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        located = json.loads(result.stdout) if result.returncode == 0 else None
    except (json.JSONDecodeError, OSError, subprocess.TimeoutExpired):
        return False
    if not isinstance(located, list) or len(located) != 2:
        return False
    origin, search_locations = located
    candidates: list[Path] = []
    if isinstance(origin, str) and origin not in {"built-in", "frozen"}:
        try:
            candidates.append(Path(origin).resolve())
        except (OSError, RuntimeError, ValueError):
            return False
    if isinstance(search_locations, list):
        for item in search_locations:
            if not isinstance(item, str):
                return False
            try:
                candidates.append(Path(item).resolve())
            except (OSError, RuntimeError, ValueError):
                return False
    return bool(candidates) and all(
        _origin_is_trusted(
            item,
            subject_root=subject_root,
            interpreter_roots=interpreter_roots,
        )
        for item in candidates
    )


def _direct_style_program_is_trusted(
    program: str,
    *,
    executable: Path,
    cwd: Path,
    path: str,
    pythonpath: str | None,
    subject_root: Path,
    interpreter_roots: tuple[Path, ...] | None = None,
) -> bool:
    """Reject Python console scripts whose implementation resolves in-subject.

    A trusted launcher path is not sufficient: conventional console scripts
    import their named package under the caller's PYTHONPATH. Native binaries
    do not have that import boundary. Unknown script launchers fail closed.
    """
    try:
        with executable.open("rb") as stream:
            prefix = stream.read(4096)
    except OSError:
        return False
    if prefix.startswith((b"\x7fELF", b"MZ")) or prefix[:4] in {
        b"\xca\xfe\xba\xbe",
        b"\xce\xfa\xed\xfe",
        b"\xcf\xfa\xed\xfe",
        b"\xfe\xed\xfa\xce",
        b"\xfe\xed\xfa\xcf",
    }:
        return True
    first_line = prefix.splitlines()[0] if prefix else b""
    if not first_line.startswith(b"#!"):
        return False
    try:
        shebang = shlex.split(first_line[2:].decode("utf-8", "strict"))
    except (UnicodeDecodeError, ValueError):
        return False
    if not shebang:
        return False
    interpreter_token = shebang[0]
    if Path(interpreter_token).name == "env":
        env_args = [arg for arg in shebang[1:] if not arg.startswith("-")]
        if not env_args:
            return False
        interpreter_token = env_args[0]
    interpreter_name = Path(interpreter_token).name
    if not re.fullmatch(r"python(?:3(?:\.\d+)?)?", interpreter_name):
        return False
    if _has_path_separator(interpreter_token):
        interpreter = Path(interpreter_token)
        if not interpreter.is_absolute():
            interpreter = cwd / interpreter
        try:
            interpreter = interpreter.absolute()
            if not interpreter.exists() or not _origin_is_trusted(
                interpreter.resolve(),
                subject_root=subject_root,
                interpreter_roots=interpreter_roots,
            ):
                return False
        except OSError:
            return False
    else:
        resolved = _resolved_trusted_program(
            interpreter_token,
            path=path,
            cwd=cwd,
            subject_root=subject_root,
            interpreter_roots=interpreter_roots,
        )
        if resolved is None:
            return False
        interpreter = resolved
    return _python_module_is_trusted(
        program,
        executable=interpreter,
        cwd=cwd,
        pythonpath=pythonpath,
        subject_root=subject_root,
        interpreter_roots=interpreter_roots,
    )


def _read_only_style_invocation(program: str, args: list[str]) -> bool:
    def has_flag(*names: str) -> bool:
        return any(token in names or any(token.startswith(name + "=") for name in names) for token in args)

    if has_flag("--fix", "--fix-only", "--unsafe-fixes"):
        return False
    if program == "ruff":
        if not args:
            return False
        if args[0] == "check":
            return True
        return args[0] == "format" and has_flag("--check", "--diff")
    if program == "mypy":
        return not has_flag("--install-types")
    if program == "black":
        return has_flag("--check", "--diff")
    if program == "isort":
        return has_flag("--check", "--check-only", "--diff")
    if program == "docformatter":
        return has_flag("--check", "--diff") and not has_flag("--in-place", "-i")
    return program in {"pyright", "flake8", "pylint"}


def _make_parts(
    args: list[str], *, cwd: Path, subject_root: Path, runtime: bool
) -> tuple[Path, list[str], list[str]] | None:
    make_cwd = cwd
    targets: list[str] = []
    forwarded: list[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in {"-C", "--directory"}:
            if index + 1 >= len(args):
                return None
            directory = args[index + 1]
            make_cwd = (make_cwd / directory).resolve()
            forwarded.extend((arg, directory))
            index += 2
            continue
        if arg.startswith("--directory="):
            directory = arg.split("=", 1)[1]
            if not directory:
                return None
            make_cwd = (make_cwd / directory).resolve()
            forwarded.append(arg)
            index += 1
            continue
        if assignment := _ASSIGNMENT_RE.match(arg):
            if assignment.group(1) in _UNTRUSTED_MAKE_ASSIGNMENTS:
                return None
            forwarded.append(arg)
            index += 1
            continue
        if arg.startswith("-"):
            return None
        targets.append(arg)
        forwarded.append(arg)
        index += 1
    if not targets:
        return None
    if runtime:
        if not make_cwd.is_dir() or not _is_within(make_cwd, subject_root):
            return None
        if any(target not in STYLE_MAKE_TARGETS for target in targets):
            return None
    elif any(not _dispatch_style_make_target(target) for target in targets):
        return None
    return make_cwd, targets, forwarded


def _dispatch_style_make_target(target: str) -> bool:
    if target in STYLE_MAKE_TARGETS:
        return True
    if not _DISPATCH_STYLE_MAKE_TARGET_RE.fullmatch(target):
        return False
    words = set(target.split("-"))
    return not words.intersection({"all", "fix", "test", "tests"})


def _make_invocation_is_style_only(
    args: list[str],
    *,
    cwd: Path,
    path: str | None,
    pythonpath: str | None,
    subject_root: Path,
    depth: int,
    interpreter_roots: tuple[Path, ...] | None = None,
) -> bool:
    parts = _make_parts(args, cwd=cwd, subject_root=subject_root, runtime=path is not None)
    if parts is None:
        return False
    make_cwd, _targets, forwarded = parts
    # API/dispatch callers intentionally stop at the static shape check.
    if path is None:
        return True
    if depth >= _MAX_MAKE_DEPTH:
        return False
    make_program = _resolved_trusted_program(
        "make",
        path=path,
        cwd=cwd,
        subject_root=subject_root,
        interpreter_roots=interpreter_roots,
    )
    if make_program is None:
        return False
    try:
        make_env: dict[str, str] = dict(os.environ)
        make_env["PATH"] = path
        resolved = subprocess.run(
            [str(make_program), "--no-print-directory", "-n", *forwarded],
            cwd=cwd,
            env=make_env,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    recipe_lines = [line.strip() for line in resolved.stdout.splitlines() if line.strip()]
    return (
        resolved.returncode == 0
        and bool(recipe_lines)
        and all(
            _classify_command(
                line,
                cwd=make_cwd,
                path=path,
                pythonpath=pythonpath,
                subject_root=subject_root,
                depth=depth + 1,
                interpreter_roots=interpreter_roots,
            )
            for line in recipe_lines
        )
    )


def _segment_is_style_only(
    tokens: list[str],
    *,
    cwd: Path,
    path: str | None,
    pythonpath: str | None,
    subject_root: Path,
    depth: int,
    interpreter_roots: tuple[Path, ...] | None = None,
) -> bool:
    while tokens and (assignment := _ASSIGNMENT_RE.match(tokens[0])):
        if assignment.group(1) in _UNTRUSTED_ENV_ASSIGNMENTS:
            return False
        tokens = tokens[1:]
    if not tokens:
        return False

    python_launcher: str | None = None
    if len(tokens) > 2 and tokens[0] in {"python", "python3"} and tokens[1] == "-m":
        python_launcher = tokens.pop(0)
        tokens.pop(0)
    elif tokens[0] in {"uv", "uvx", "poetry", "hatch"}:
        return False

    program = tokens[0]
    if _has_path_separator(program):
        return False
    if program == "make" and python_launcher is None:
        return _make_invocation_is_style_only(
            tokens[1:],
            cwd=cwd,
            path=path,
            pythonpath=pythonpath,
            subject_root=subject_root,
            depth=depth,
            interpreter_roots=interpreter_roots,
        )
    if program not in STYLE_PROGRAMS or not _read_only_style_invocation(program, tokens[1:]):
        return False
    if path is None:
        return True

    executable_name = python_launcher or program
    executable = _resolved_trusted_program(
        executable_name,
        path=path,
        cwd=cwd,
        subject_root=subject_root,
        interpreter_roots=interpreter_roots,
    )
    if executable is None:
        return False
    if python_launcher is None:
        return _direct_style_program_is_trusted(
            program,
            executable=executable,
            cwd=cwd,
            path=path,
            pythonpath=pythonpath,
            subject_root=subject_root,
            interpreter_roots=interpreter_roots,
        )
    return _python_module_is_trusted(
        program,
        executable=executable,
        cwd=cwd,
        pythonpath=pythonpath,
        subject_root=subject_root,
        interpreter_roots=interpreter_roots,
    )


def _classify_command(
    command: str,
    *,
    cwd: Path,
    path: str | None,
    pythonpath: str | None,
    subject_root: Path,
    depth: int,
    interpreter_roots: tuple[Path, ...] | None = None,
) -> bool:
    tokens = _tokenize(command)
    if not tokens:
        return False
    segments: list[list[str]] = [[]]
    for token in tokens:
        if token in _UNSAFE_SHELL_TOKENS:
            return False
        if token in _SEGMENT_SEPARATORS:
            if not segments[-1]:
                return False
            segments.append([])
        else:
            segments[-1].append(token)
    if not segments[-1]:
        return False

    segment_cwd = cwd
    saw_style = False
    for segment in segments:
        if segment[0] == "cd":
            if len(segment) != 2:
                return False
            candidate = (segment_cwd / segment[1]).resolve()
            if path is not None and not _is_within(candidate, subject_root):
                return False
            if path is not None and not candidate.is_dir():
                return False
            segment_cwd = candidate
            continue
        if not _segment_is_style_only(
            segment,
            cwd=segment_cwd,
            path=path,
            pythonpath=pythonpath,
            subject_root=subject_root,
            depth=depth,
            interpreter_roots=interpreter_roots,
        ):
            return False
        saw_style = True
    return saw_style


def is_style_only_command(
    command: str,
    *,
    cwd: str | os.PathLike[str] | None = None,
    path: str | None = None,
    pythonpath: str | None = None,
    python_executable: str | os.PathLike[str] | None = None,
) -> bool:
    """Return whether every non-neutral segment is proven read-only style work.

    With no ``path``, this is a static warning classifier and performs no
    subprocess or filesystem evaluation. Supplying ``path`` opts into runtime
    trust checks and contained Makefile recipe resolution. The gate should
    pass its already-resolved interpreter as ``python_executable`` so an
    environment provisioned inside the subject can be trusted by origin.
    """
    if not isinstance(command, str) or not command.strip():
        return False
    base = (Path.cwd() if cwd is None else Path(cwd)).resolve()
    interpreter_roots: tuple[Path, ...] | None = None
    if path is not None and python_executable is not None:
        executable = Path(python_executable)
        if not executable.is_absolute():
            executable = base / executable
        try:
            executable = executable.absolute()
        except OSError:
            return False
        interpreter_roots = _interpreter_environment_roots(
            executable,
            cwd=base,
            pythonpath=pythonpath,
        )
        if interpreter_roots is None:
            return False
    return _classify_command(
        command.strip(),
        cwd=base,
        path=path,
        pythonpath=pythonpath,
        subject_root=base,
        depth=0,
        interpreter_roots=interpreter_roots,
    )
