"""Select merge-gate tests by reachability from the integration delta.

GRPH-03: answer "will this change affect X" by reachability, not by running
the whole suite. The full suite belongs to a release cut only. Unknown
runners fail safe to full (SECD-05) rather than silently selecting nothing.
"""

from __future__ import annotations

import ast
import re
import shlex
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Literal, Sequence

MODE_DELTA = "delta"
MODE_FULL = "full"
MODE_NONE = "none"

REASON_CHANGED_PATH_UNMAPPED = "changed_path_unmapped"
REASON_CHANGED_PATH_SYMLINK_ESCAPE = "changed_path_symlink_escape"
REASON_CONFIG_CHANGED = "config_changed"
REASON_INTEGRATION_DELTA = "integration_delta"
REASON_NO_REACHABLE_TESTS = "no_reachable_tests"
REASON_PACKAGE_ROOT_UNRESOLVED = "package_root_unresolved"
REASON_RELEASE_CUT = "release_cut"
REASON_RUNNER_UNSUPPORTED = "runner_unsupported"
REASON_ROW_CONTEXT_UNRESOLVED = "row_context_unresolved"

MergeGateMode = Literal["delta", "full", "none"]

_ENV_ASSIGN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=(.*)$")
_PYTHON_INTERPRETER = re.compile(r"^python(\d+(\.\d+)?)?$")
_UNSAFE_TOKENS = frozenset({"|", "||", ";", "&", "(", ")", "{", "}", "`"})
_UNSAFE_VALUE_CHARS = frozenset({"$", "`", "!", "\n"})
_EMBEDDED_CONTROL = re.compile(r"[;|&(){}`]")
# shlex.split treats newline/CR as whitespace and leaves redirections, comments,
# and command substitution as ordinary tokens. Reject that class on the raw
# string so a trailing ``false`` cannot be delta-replaced (TSEL03ARV-002).
_HIDDEN_SHELL_STRUCTURE = re.compile(r"[\n\r`$<>#]|\$\(")
# Tokens the selector cannot expand: globs, tilde, parameters, backticks, and
# brace lists. Reject them in the one direct-runner predicate (TSEL03ER2RV-002).
_UNMODELED_SHELL_EXPANSION_CHARS = frozenset("*?[$`~")
_BRACE_EXPANSION = re.compile(r"\{[^{}]+,[^{}]+\}")

# Vitest forceRerunTriggers (stated explicitly, not left implicit):
# default triggers rerun the whole suite when package.json, vitest.config.*,
# or vite.config.* change. Merge-gate selection treats those, plus lockfiles
# and tsconfig, as config — a delta `--changed` run would still miss them.
_VITEST_CONFIG_BASENAMES = frozenset(
    {
        "package.json",
        "package-lock.json",
        "npm-shrinkwrap.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "bun.lock",
        "bun.lockb",
        "tsconfig.json",
    }
)
_PYTEST_CONFIG_BASENAMES = frozenset({"conftest.py", "pyproject.toml", "pytest.ini"})

# REF-26: one allowlist decides whether a delta may reuse the row argv.
# Unknown flags, ``--`` pass-through, and wrappers stay unresolved (SECD-05).
_VITEST_VALUE_OPTIONS = frozenset(
    {
        "--project",
        "--reporter",
        "-r",
        "--config",
        "-c",
        "--dir",
        "--root",
        "--environment",
        "--pool",
        "--maxWorkers",
        "--minWorkers",
        "--testTimeout",
        "--hookTimeout",
        "--teardownTimeout",
        "--mode",
        "--outputFile",
        "--testNamePattern",
        "-t",
        "--exclude",
        "--include",
        "--setupFiles",
        "--shard",
        "--maxConcurrency",
        "--diff",
    }
)
_VITEST_FLAG_OPTIONS = frozenset(
    {
        "--run",
        "--watch",
        "-w",
        "--ui",
        "--open",
        "--silent",
        "-s",
        "--passWithNoTests",
        "--allowOnly",
        "--bail",
        "--coverage",
        "--update",
        "-u",
        "--standalone",
        "--color",
        "--no-color",
        "--isolate",
        "--no-isolate",
        "--fileParallelism",
        "--inspect",
        "--inspectBrk",
        "--hideSkippedTests",
        "--expandSnapshotDiff",
        "--disableConsoleIntercept",
        "--printConsoleTrace",
        "--typecheck",
    }
)
_PYTEST_VALUE_OPTIONS = frozenset(
    {
        "-m",
        "-k",
        "-p",
        "-o",
        "-c",
        "-n",
        "-W",
        "--tb",
        "--capture",
        "--maxfail",
        "--durations",
        "--basetemp",
        "--rootdir",
        "--confcutdir",
        "--import-mode",
        "--log-level",
        "--log-format",
        "--log-date-format",
        "--log-file",
        "--log-cli-level",
        "--override-ini",
        "--color",
        "--code-highlight",
        "--cov",
        "--cov-report",
        "--junitxml",
        "--junit-xml",
        "--dist",
        "--max-worker-restart",
        "--numprocesses",
        "--pdbcls",
    }
)
_PYTEST_FLAG_OPTIONS = frozenset(
    {
        "-q",
        "--quiet",
        "-v",
        "--verbose",
        "-s",
        "-x",
        "--exitfirst",
        "-l",
        "--showlocals",
        "-h",
        "--help",
        "--version",
        "--lf",
        "--last-failed",
        "--ff",
        "--failed-first",
        "--nf",
        "--new-first",
        "--cache-clear",
        "--co",
        "--collect-only",
        "--strict-markers",
        "--strict-config",
        "--disable-warnings",
        "--disable-pytest-warnings",
        "--sw",
        "--stepwise",
        "--pdb",
        "--trace",
        "--no-header",
        "--no-summary",
        "--keep-duplicates",
        "--cov-append",
    }
)


@dataclass(frozen=True, slots=True)
class RowExecutionContext:
    """cwd, env assignments, and runner invocation parsed from a row command."""

    cwd: str | None
    env: tuple[tuple[str, str], ...]
    invocation: str


@dataclass(frozen=True, slots=True)
class _DirectRunnerInvocation:
    """Allowlisted direct vitest/pytest argv, split into prefix/options/paths."""

    kind: Literal["vitest", "pytest"]
    prefix: tuple[str, ...]
    options: tuple[str, ...]
    positionals: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MergeGateTestSelection:
    """Typed merge-gate selection: delta, full, or none, with a reason."""

    mode: MergeGateMode
    command: str | None
    reason: str
    cwd: str | None = None
    env: tuple[tuple[str, str], ...] = ()
    package_root: str | None = None


def infer_merge_gate_runner(command: str) -> str:
    """Infer vitest/pytest from a stored row command; anything else is unknown."""
    try:
        tokens = [token.lower() for token in shlex.split(command or "")]
    except ValueError:
        tokens = (command or "").strip().lower().split()
    if any(token == "vitest" or token.endswith("vitest") for token in tokens):
        return "vitest"
    if "npm" in tokens and "test" in tokens:
        return "vitest"
    if any(token == "pytest" or token.endswith("pytest") for token in tokens):
        return "pytest"
    text = (command or "").strip().lower()
    if "npm test" in text or "npm run test" in text:
        return "vitest"
    return "unknown"


def parse_row_execution_context(
    command: str,
    *,
    package_root: str | Path | None = None,
) -> RowExecutionContext | None:
    """Parse leading ``cd`` / ``NAME=value`` prefixes from a row command.

    Returns None when the command is ambiguous (pipes, ``;``, ``||``,
    subshells, newlines, redirections, command substitution, multiple
    ``cd``, a missing cwd dir) so callers fail safe to the verbatim row
    command.
    """
    if not (command or "").strip():
        return None
    if _raw_row_hides_shell_structure(command):
        return None
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if not tokens:
        return None
    env: list[tuple[str, str]] = []
    cwd: str | None = None
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if _is_control_token(token):
            return None
        env_match = _ENV_ASSIGN.match(token)
        if env_match is not None:
            value = env_match.group(1)
            if any(char in value for char in _UNSAFE_VALUE_CHARS):
                return None
            env.append((token.partition("=")[0], value))
            index += 1
            continue
        if token == "cd":
            if cwd is not None:
                return None
            if index + 1 >= len(tokens):
                return None
            target = tokens[index + 1]
            if _is_control_token(target) or target == "&&":
                return None
            if target.startswith("-"):
                return None
            if any(char in target for char in _UNSAFE_VALUE_CHARS):
                return None
            if index + 2 >= len(tokens) or tokens[index + 2] != "&&":
                return None
            cwd = target
            index += 3
            continue
        break
    rest = tokens[index:]
    if not rest:
        return None
    if any(_is_control_token(token) or token == "&&" for token in rest):
        return None
    if any(token.startswith("$(") or token.startswith("`") for token in rest):
        return None
    resolved_cwd = _resolve_row_cwd(cwd, package_root)
    if cwd is not None and resolved_cwd is None:
        return None
    return RowExecutionContext(
        cwd=resolved_cwd,
        env=tuple(env),
        invocation=shlex.join(rest),
    )


def _raw_row_hides_shell_structure(command: str) -> bool:
    """True when tokenization would hide separators or other shell structure."""
    return _HIDDEN_SHELL_STRUCTURE.search(command) is not None


def _direct_runner_invocation(invocation: str) -> _DirectRunnerInvocation | None:
    """Return the allowlisted vitest/pytest shape, or None to run verbatim.

    REF-26: one predicate decides whether a delta may reuse the row argv.
    Direct ``vitest run`` / ``npx vitest run`` and ``pytest`` /
    ``python -m pytest`` / ``<interpreter> -m pytest`` keep their tokens.
    Package-manager scripts (``npm``, ``pnpm``, ``yarn``), ``uv run``,
    ``npx`` of anything but vitest, ``--`` pass-through, node ids, and
    options we do not model are unresolved (SECD-05). Unmodeled shell
    expansion in any token (``*``, ``?``, ``[``, ``~``, ``$``, backticks,
    ``{a,b}``) is also unresolved so a quoted glob cannot replace a
    shell-expanded filter (TSEL03ER2RV-002).
    """
    try:
        tokens = shlex.split(invocation or "")
    except ValueError:
        return None
    if not tokens:
        return None
    if any(_token_has_unmodeled_shell_expansion(token) for token in tokens):
        return None
    vitest_prefix = _vitest_invocation_prefix(tokens)
    if vitest_prefix is not None:
        split = _split_known_options(
            tokens[len(vitest_prefix) :],
            flags=_VITEST_FLAG_OPTIONS,
            value_options=_VITEST_VALUE_OPTIONS,
        )
        if split is None:
            return None
        options, positionals = split
        return _DirectRunnerInvocation(
            kind="vitest",
            prefix=vitest_prefix,
            options=options,
            positionals=positionals,
        )
    pytest_prefix = _pytest_invocation_prefix(tokens)
    if pytest_prefix is not None:
        split = _split_known_options(
            tokens[len(pytest_prefix) :],
            flags=_PYTEST_FLAG_OPTIONS,
            value_options=_PYTEST_VALUE_OPTIONS,
        )
        if split is None:
            return None
        options, positionals = split
        if any("::" in item or item.startswith("@") for item in positionals):
            return None
        return _DirectRunnerInvocation(
            kind="pytest",
            prefix=pytest_prefix,
            options=options,
            positionals=positionals,
        )
    return None


def _token_has_unmodeled_shell_expansion(token: str) -> bool:
    """True when a token still needs shell expansion the selector does not model."""
    return any(char in _UNMODELED_SHELL_EXPANSION_CHARS for char in token) or (
        _BRACE_EXPANSION.search(token) is not None
    )


def _vitest_invocation_prefix(tokens: Sequence[str]) -> tuple[str, ...] | None:
    if len(tokens) >= 3 and tokens[0] == "npx" and tokens[1] == "vitest" and tokens[2] == "run":
        return ("npx", "vitest", "run")
    if len(tokens) >= 2 and tokens[0] == "vitest" and tokens[1] == "run":
        return ("vitest", "run")
    return None


def _pytest_invocation_prefix(tokens: Sequence[str]) -> tuple[str, ...] | None:
    if tokens[0] == "pytest":
        return ("pytest",)
    if len(tokens) >= 3 and _is_python_interpreter(tokens[0]) and tokens[1] == "-m" and tokens[2] == "pytest":
        return (tokens[0], "-m", "pytest")
    return None


def _is_python_interpreter(token: str) -> bool:
    return _PYTHON_INTERPRETER.fullmatch(Path(token).name) is not None


def _split_known_options(
    tokens: Sequence[str],
    *,
    flags: frozenset[str],
    value_options: frozenset[str],
) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    """Split allowlisted flags from positionals; None if any token is unmodeled."""
    options: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return None
        if token.startswith("--") and "=" in token:
            name = token.partition("=")[0]
            if name in value_options or name in flags:
                options.append(token)
                index += 1
                continue
            return None
        if token in flags:
            options.append(token)
            index += 1
            continue
        if token in value_options:
            if index + 1 >= len(tokens):
                return None
            value = tokens[index + 1]
            if value == "--" or value.startswith("-"):
                return None
            options.append(token)
            options.append(value)
            index += 2
            continue
        if token.startswith("-"):
            return None
        positionals = tokens[index:]
        if any(item == "--" or item.startswith("-") for item in positionals):
            return None
        return tuple(options), tuple(positionals)
    return tuple(options), ()


def _is_control_token(token: str) -> bool:
    if token == "&&":
        return False
    return token in _UNSAFE_TOKENS or _EMBEDDED_CONTROL.search(token) is not None


def _resolve_row_cwd(cwd: str | None, package_root: str | Path | None) -> str | None:
    if cwd is None:
        return None
    path = Path(cwd)
    if not path.is_absolute():
        if package_root is None:
            return None
        path = Path(package_root) / cwd
    if not path.is_dir():
        return None
    return str(path.resolve())


def _resolve_reachability_package_root(
    *,
    row_cwd: str | None,
    app_root: str | Path | None,
    subject_root: str | Path | None,
) -> Path | None:
    """Row cwd, else recorded app_root, else the subject. Missing dirs are unresolved."""
    subject = Path(subject_root).resolve() if subject_root is not None else None
    candidate: Path | None = None
    if row_cwd:
        path = Path(row_cwd)
        if not path.is_dir():
            return None
        candidate = path.resolve()
    elif app_root is not None and str(app_root).strip():
        path = Path(str(app_root).strip())
        if not path.is_absolute():
            if subject is None:
                return None
            path = subject / path
        if not path.is_dir():
            return None
        candidate = path.resolve()
    elif subject is not None and subject.is_dir():
        candidate = subject
    else:
        return None
    if subject is not None:
        try:
            candidate.relative_to(subject)
        except ValueError:
            return None
    return candidate


@dataclass(frozen=True, slots=True)
class _NormalizedChangedPaths:
    """Lexical package mapping, or a fail-safe reason when classification is unsafe."""

    paths: tuple[str, ...] | None
    reason: str | None = None


def _posix_is_unsafe(posix: str) -> bool:
    """True when a git path is absolute or contains a parent segment."""
    if not posix:
        return False
    if posix.startswith("/") or posix.startswith("\\"):
        return True
    path = PurePosixPath(posix)
    if path.is_absolute():
        return True
    return any(part == ".." for part in path.parts)


def _package_prefix_parts(
    package: Path,
    subject: Path | None,
) -> tuple[str, ...] | None:
    """Repo-relative parts of ``package`` under ``subject``, or None if unmappable."""
    if subject is None:
        return ()
    try:
        relative = package.relative_to(subject)
    except ValueError:
        return None
    parts = PurePosixPath(relative.as_posix()).parts
    if parts == (".",):
        return ()
    return parts


def _normalize_changed_paths_to_package(
    changed_paths: Sequence[str],
    *,
    package_root: str | Path,
    subject_root: str | Path | None,
) -> _NormalizedChangedPaths:
    """Map subject-relative git paths onto ``package_root``.

    Containment is decided on the lexical, repo-relative name. Paths outside
    the package are omitted (not evidence for this row). Absolute paths and
    parent-segment spellings cannot be classified safely and fail with
    ``changed_path_unmapped``. A lexically in-package path whose real target
    escapes the package fails with ``changed_path_symlink_escape``. Both
    reasons tell the caller to fail safe to full (SECD-05).
    """
    package = Path(package_root).resolve()
    subject = Path(subject_root).resolve() if subject_root is not None else None
    prefix = _package_prefix_parts(package, subject)
    if prefix is None:
        return _NormalizedChangedPaths(paths=None, reason=REASON_CHANGED_PATH_UNMAPPED)
    mapped: list[str] = []
    for raw in changed_paths:
        original = str(raw).replace("\\", "/")
        if not original:
            continue
        if _posix_is_unsafe(original):
            return _NormalizedChangedPaths(paths=None, reason=REASON_CHANGED_PATH_UNMAPPED)
        posix = _normalize_path(original)
        if not posix:
            continue
        if _posix_is_unsafe(posix):
            return _NormalizedChangedPaths(paths=None, reason=REASON_CHANGED_PATH_UNMAPPED)
        parts = tuple(part for part in PurePosixPath(posix).parts if part not in {".", ""})
        if prefix:
            if parts[: len(prefix)] != prefix:
                continue
            rest = parts[len(prefix) :]
            relative = PurePosixPath(*rest).as_posix() if rest else "."
        else:
            relative = PurePosixPath(*parts).as_posix() if parts else "."
        abs_path = (subject / posix) if subject is not None else (package / posix)
        try:
            abs_path.resolve().relative_to(package)
        except (ValueError, OSError):
            return _NormalizedChangedPaths(paths=None, reason=REASON_CHANGED_PATH_SYMLINK_ESCAPE)
        mapped.append(relative)
    return _NormalizedChangedPaths(paths=tuple(mapped), reason=None)


def _rebase_paths_to_execution_cwd(
    paths: Sequence[str],
    *,
    package_root: str | Path | None,
    execution_cwd: str | Path | None,
) -> tuple[str, ...] | None:
    """Rewrite package-relative paths so they are valid from the command cwd."""
    if package_root is None or execution_cwd is None:
        return tuple(paths)
    package = Path(package_root).resolve()
    cwd = Path(execution_cwd).resolve()
    if package == cwd:
        return tuple(paths)
    try:
        prefix = package.relative_to(cwd)
    except ValueError:
        return None
    if prefix == Path("."):
        return tuple(paths)
    return tuple((prefix / path).as_posix() for path in paths)


def resolve_lane_check_command(
    row_command: str,
    *,
    changed_paths: Sequence[str],
    merge_base: str,
    integration_ref: str,
    package_root: str | Path | None = None,
    app_root: str | Path | None = None,
    release: bool = False,
) -> MergeGateTestSelection:
    """Map a selector result onto the command lane-check should run.

    Delta reuses the row's direct runner invocation and appends the
    selector. Full (config, unsupported/wrapped invocation, release cut,
    or a None command) keeps the row command so the gate fails safe to
    more testing. None stays none and is never a pass.

    Pytest reachability is rooted at the row cwd, else the recorded app_root,
    else the subject. Paths are normalised to that package so a nested
    ``packages/demo`` lane is not scanned as ``<subject>/tests``.
    """
    context = parse_row_execution_context(row_command, package_root=package_root)
    if context is None:
        return MergeGateTestSelection(
            mode=MODE_FULL,
            command=row_command,
            reason=REASON_ROW_CONTEXT_UNRESOLVED,
        )
    direct = _direct_runner_invocation(context.invocation)
    if direct is None:
        return MergeGateTestSelection(
            mode=MODE_FULL,
            command=row_command,
            reason=REASON_ROW_CONTEXT_UNRESOLVED,
        )
    runner = direct.kind
    reachability_root = _resolve_reachability_package_root(
        row_cwd=context.cwd,
        app_root=app_root,
        subject_root=package_root,
    )
    selection_paths: Sequence[str] = changed_paths
    selection_root = package_root
    if _normalize_runner(runner) == "pytest":
        if reachability_root is None:
            return MergeGateTestSelection(
                mode=MODE_FULL,
                command=row_command,
                reason=REASON_PACKAGE_ROOT_UNRESOLVED,
            )
        mapped = _normalize_changed_paths_to_package(
            changed_paths,
            package_root=reachability_root,
            subject_root=package_root,
        )
        if mapped.reason is not None or mapped.paths is None:
            return MergeGateTestSelection(
                mode=MODE_FULL,
                command=row_command,
                reason=mapped.reason or REASON_CHANGED_PATH_UNMAPPED,
                package_root=str(reachability_root),
            )
        selection_paths = mapped.paths
        selection_root = reachability_root
    execution_cwd = context.cwd if context.cwd is not None else package_root
    selected = select_merge_gate_test_cmd(
        runner=runner,
        changed_paths=selection_paths,
        merge_base=merge_base,
        integration_ref=integration_ref,
        package_root=selection_root,
        release=release,
        execution_cwd=execution_cwd,
    )
    package_root_str = str(reachability_root) if reachability_root is not None else None
    if selected.mode == MODE_DELTA and selected.command:
        rebuilt = _delta_command_for_direct_runner(
            direct,
            selected_command=selected.command,
            merge_base=merge_base,
        )
        if rebuilt is None:
            return MergeGateTestSelection(
                mode=MODE_FULL,
                command=row_command,
                reason=REASON_ROW_CONTEXT_UNRESOLVED,
                cwd=context.cwd,
                env=context.env,
                package_root=package_root_str,
            )
        return MergeGateTestSelection(
            mode=selected.mode,
            command=rebuilt,
            reason=selected.reason,
            cwd=context.cwd,
            env=context.env,
            package_root=package_root_str,
        )
    if selected.mode == MODE_NONE:
        return MergeGateTestSelection(
            mode=selected.mode,
            command=selected.command,
            reason=selected.reason,
            package_root=package_root_str,
        )
    return MergeGateTestSelection(
        mode=MODE_FULL,
        command=row_command,
        reason=selected.reason,
        package_root=package_root_str,
    )


def select_merge_gate_test_cmd(
    *,
    runner: str,
    changed_paths: Sequence[str],
    merge_base: str,
    integration_ref: str,
    package_root: str | Path | None = None,
    release: bool = False,
    execution_cwd: str | Path | None = None,
) -> MergeGateTestSelection:
    """Return the merge-gate test command reachable from ``changed_paths``.

    ``merge_base`` is the already-computed merge-base sha of ``HEAD`` and
    ``integration_ref``. Vitest must receive that sha via ``--changed``; a
    bare ``--changed`` flag only sees uncommitted edits and is never emitted.
    """
    del integration_ref  # callers pass the ref they used to compute merge_base
    if release:
        return MergeGateTestSelection(
            mode=MODE_FULL,
            command=_full_command(_normalize_runner(runner)),
            reason=REASON_RELEASE_CUT,
        )
    normalized = _normalize_runner(runner)
    paths = tuple(_normalize_path(path) for path in changed_paths)
    if normalized == "vitest":
        return _select_vitest(paths, merge_base)
    if normalized == "pytest":
        return _select_pytest(paths, package_root, execution_cwd)
    return MergeGateTestSelection(
        mode=MODE_FULL,
        command=None,
        reason=REASON_RUNNER_UNSUPPORTED,
    )


def _normalize_runner(runner: str) -> str:
    text = (runner or "").strip().lower()
    if text == "vitest" or text.endswith("vitest"):
        return "vitest"
    if text == "pytest" or text.endswith("pytest"):
        return "pytest"
    return text


def _normalize_path(path: str | Path) -> str:
    posix = str(path).replace("\\", "/")
    while posix.startswith("./"):
        posix = posix.removeprefix("./")
    return posix


def _full_command(runner: str) -> str | None:
    if runner == "vitest":
        return "npx vitest run"
    if runner == "pytest":
        return "python -m pytest"
    return None


def _delta_command_for_direct_runner(
    direct: _DirectRunnerInvocation,
    *,
    selected_command: str,
    merge_base: str,
) -> str | None:
    """Rebuild the delta argv from the row's tokens, or None to run verbatim."""
    if direct.kind == "vitest":
        return shlex.join((*direct.prefix, *direct.options, *direct.positionals, "--changed", merge_base))
    if direct.kind == "pytest":
        selected_paths = _generic_pytest_paths(selected_command)
        if selected_paths is None:
            return None
        paths = _intersect_pytest_paths(selected_paths, direct.positionals)
        if paths is None:
            return None
        return shlex.join((*direct.prefix, *direct.options, *paths))
    return None


def _generic_pytest_paths(command: str) -> tuple[str, ...] | None:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    prefix = ("python", "-m", "pytest")
    if tuple(tokens[:3]) != prefix:
        return None
    paths = tokens[3:]
    if any(path.startswith("-") for path in paths):
        return None
    return tuple(paths)


def _intersect_pytest_paths(
    selected: Sequence[str],
    filters: Sequence[str],
) -> tuple[str, ...] | None:
    """Keep selected paths that stay inside the row's positional filters.

    An empty intersection fails closed so the caller can run the verbatim
    row instead of widening the original scope.
    """
    if not filters:
        return tuple(selected)
    kept = tuple(path for path in selected if any(_pytest_path_in_filter(path, bound) for bound in filters))
    if not kept:
        return None
    return kept


def _pytest_path_in_filter(path: str, bound: str) -> bool:
    candidate = _normalize_path(path)
    prefix = _normalize_path(bound)
    if not candidate or not prefix:
        return False
    if candidate == prefix:
        return True
    return candidate.startswith(prefix.rstrip("/") + "/")


def _select_vitest(changed_paths: Sequence[str], merge_base: str) -> MergeGateTestSelection:
    if any(_is_vitest_config(path) for path in changed_paths):
        return MergeGateTestSelection(
            mode=MODE_FULL,
            command=_full_command("vitest"),
            reason=REASON_CONFIG_CHANGED,
        )
    return MergeGateTestSelection(
        mode=MODE_DELTA,
        command=f"npx vitest run --changed {merge_base}",
        reason=REASON_INTEGRATION_DELTA,
    )


def _is_vitest_config(path: str) -> bool:
    name = Path(path).name
    lowered = name.lower()
    if lowered in _VITEST_CONFIG_BASENAMES:
        return True
    if lowered.startswith("tsconfig.") and lowered.endswith(".json"):
        return True
    if lowered.startswith(("vitest.config.", "vite.config.", "vitest.workspace.")):
        return True
    return False


def _select_pytest(
    changed_paths: Sequence[str],
    package_root: str | Path | None,
    execution_cwd: str | Path | None = None,
) -> MergeGateTestSelection:
    if any(_is_pytest_config(path) for path in changed_paths):
        return MergeGateTestSelection(
            mode=MODE_FULL,
            command=_full_command("pytest"),
            reason=REASON_CONFIG_CHANGED,
        )
    selected = _reachable_pytest_paths(changed_paths, package_root)
    if not selected:
        return MergeGateTestSelection(
            mode=MODE_NONE,
            command=None,
            reason=REASON_NO_REACHABLE_TESTS,
        )
    command_paths = _rebase_paths_to_execution_cwd(
        selected,
        package_root=package_root,
        execution_cwd=execution_cwd,
    )
    if command_paths is None:
        return MergeGateTestSelection(
            mode=MODE_FULL,
            command=_full_command("pytest"),
            reason=REASON_CHANGED_PATH_UNMAPPED,
        )
    return MergeGateTestSelection(
        mode=MODE_DELTA,
        command=shlex.join(("python", "-m", "pytest", *command_paths)),
        reason=REASON_INTEGRATION_DELTA,
        package_root=str(Path(package_root).resolve()) if package_root is not None else None,
    )


def _is_pytest_config(path: str) -> bool:
    return Path(path).name in _PYTEST_CONFIG_BASENAMES


def _is_pytest_test_file(path: str) -> bool:
    name = Path(path).name
    if name in _PYTEST_CONFIG_BASENAMES:
        return False
    if not name.endswith(".py"):
        return False
    if name.startswith("test_") or name.endswith("_test.py"):
        return True
    posix = f"/{path.strip('/')}/"
    return "/tests/" in posix


def _reachable_pytest_paths(
    changed_paths: Sequence[str],
    package_root: str | Path | None,
) -> tuple[str, ...]:
    selected: set[str] = set()
    for path in changed_paths:
        if _is_pytest_test_file(path):
            selected.add(path)
    root = Path(package_root) if package_root is not None else None
    changed_modules = {
        module
        for path in changed_paths
        if not _is_pytest_test_file(path)
        for module in (_path_to_module(path),)
        if module
    }
    if root is not None and changed_modules:
        tests_dir = root / "tests"
        if tests_dir.is_dir():
            for test_path in sorted(tests_dir.rglob("*.py")):
                if not test_path.is_file() or not _is_pytest_test_file(_relative_to_root(root, test_path)):
                    continue
                relative = _relative_to_root(root, test_path)
                if _test_imports_any(test_path, changed_modules):
                    selected.add(relative)
    return tuple(sorted(selected))


def _relative_to_root(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _path_to_module(path: str) -> str | None:
    posix = _normalize_path(path)
    if not posix.endswith(".py"):
        return None
    posix = posix[:-3]
    if posix.endswith("/__init__"):
        posix = posix[: -len("/__init__")]
    parts = [part for part in posix.split("/") if part and part not in {".", ".."}]
    if parts and parts[0] == "src":
        parts = parts[1:]
    if len(parts) >= 3 and parts[0] == "packages" and parts[2] == "src":
        parts = parts[3:]
    if not parts:
        return None
    return ".".join(parts)


def _test_imports_any(test_path: Path, modules: Iterable[str]) -> bool:
    try:
        source = test_path.read_text(encoding="utf-8")
    except OSError:
        return False
    imported = _imported_names(source)
    for module in modules:
        for name in imported:
            if name == module or name.startswith(f"{module}."):
                return True
    return False


def _imported_names(source: str) -> set[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            for alias in node.names:
                if alias.name != "*":
                    names.add(f"{node.module}.{alias.name}")
    return names
