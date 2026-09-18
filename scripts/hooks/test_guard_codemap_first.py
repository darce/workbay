"""Table-driven finite source-query classifier (implementation note C2).

One row per Finite classifier grammar clause, plus the C2 falsifiers:

- a quoted/heredoc grep must NOT be classified ``source_query``
- an unsupported construct must be ``unknown_source``
- unsupported search is denied in enforced mode; unrelated commands run
- the wrapper performs one ledger read on a source query and zero on
  unrelated commands (and never rereads from the handler)
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pytest

HOOK_SCRIPT = Path(__file__).resolve().parent / "guard-codemap-first.py"
WRAPPER = Path(__file__).resolve().parent / "_run_guard.py"
WRAP_MODULE = (
    Path(__file__).resolve().parents[2]
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "scripts"
    / "_guard_wrap.py"
)
PAYLOAD_HOOK = (
    Path(__file__).resolve().parents[2]
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "scripts"
    / "hooks"
    / "guard-codemap-first.py"
)
PAYLOAD_RUN_GUARD = (
    Path(__file__).resolve().parents[2]
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "scripts"
    / "hooks"
    / "_run_guard.py"
)

Kind = Literal["source_query", "unrelated", "unknown_source"]

SOURCE_SUFFIXES = (
    ".py",
    ".pyi",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".go",
    ".rs",
    ".c",
    ".h",
    ".cc",
    ".cpp",
    ".hpp",
    ".java",
    ".sh",
    ".rb",
    ".sql",
)
SOURCE_DIRS = ("src", "lib", "scripts", "packages", "tests")
CONFIG_FILES = (
    "pyproject.toml",
    "package.json",
    "config.yaml",
    "config.yml",
    "setup.ini",
    ".env",
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_hook():
    return _load(HOOK_SCRIPT, "guard_codemap_first_under_test")


def _load_wrap():
    return _load(WRAP_MODULE, "guard_wrap_under_test")


def _load_wrapper():
    return _load(WRAPPER, "run_guard_codemap_under_test")


def _bash(command: str) -> dict[str, Any]:
    return {"tool_name": "Bash", "tool_input": {"command": command}}


def _grep(**fields: Any) -> dict[str, Any]:
    return {"tool_name": "Grep", "tool_input": dict(fields)}


def _glob(**fields: Any) -> dict[str, Any]:
    return {"tool_name": "Glob", "tool_input": dict(fields)}


def _nested_bash_c(inner: str, layers: int) -> str:
    body = inner
    for _ in range(layers):
        body = "bash -c " + json.dumps(body)
    return body


@dataclass(frozen=True)
class Clause:
    clause: str
    payload: dict[str, Any] | str
    kind: Kind
    reason_contains: str


CLASSIFIER_CASES: list[Clause] = [
    Clause(
        "bound_64kib",
        _bash("rg foo src/" + ("x" * (64 * 1024))),
        "unknown_source",
        "64",
    ),
    Clause(
        "bound_256_tokens",
        _bash("rg foo " + " ".join(f"src/f{i}.py" for i in range(255))),
        "unknown_source",
        "256",
    ),
    Clause(
        "nested_wrappers_over_four",
        _bash(_nested_bash_c("rg foo src/a.py", 5)),
        "unknown_source",
        "wrapper",
    ),
    Clause(
        "nested_wrappers_at_four",
        _bash(_nested_bash_c("rg foo src/a.py", 4)),
        "source_query",
        "source",
    ),
    Clause(
        "inspect_every_command_not_only_first",
        _bash("true && rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "compound_or_second_command",
        _bash("false || rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "semicolon_second_command",
        _bash("true; rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "grep_explicit_source_file",
        _bash("grep -n foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "rg_explicit_source_file",
        _bash("rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "ag_source_directory",
        _bash("ag foo lib/"),
        "source_query",
        "source",
    ),
    Clause(
        "ack_source_directory",
        _bash("ack foo packages/"),
        "source_query",
        "source",
    ),
    Clause(
        "git_grep",
        _bash("git grep -n foo"),
        "source_query",
        "omitted",
    ),
    Clause(
        "git_C_literal_grep",
        _bash("git -C /tmp/repo grep -n foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "git_grep_path_after_double_dash",
        _bash("git grep foo -- src/mod.py"),
        "source_query",
        "source",
    ),
    Clause(
        "env_wrapper",
        _bash("env FOO=1 rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "leading_assignment",
        _bash("FOO=1 BAR=2 rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "command_wrapper",
        _bash("command rg foo src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "bash_c_literal",
        _bash("bash -c 'rg foo src/a.py'"),
        "source_query",
        "source",
    ),
    Clause(
        "sh_c_literal",
        _bash("sh -c 'rg foo src/a.py'"),
        "source_query",
        "source",
    ),
    Clause(
        "omitted_path",
        _bash("rg foo"),
        "source_query",
        "omitted",
    ),
    Clause(
        "repository_root_dot",
        _bash("rg foo ."),
        "source_query",
        "root",
    ),
    Clause(
        "source_extension_filter_rg_type",
        _bash("rg -t py foo"),
        "source_query",
        "type",
    ),
    Clause(
        "source_extension_filter_grep_include",
        _bash("grep -R --include='*.py' foo ."),
        "source_query",
        "source",
    ),
    Clause(
        "file_list_discovery_rg_files",
        _bash("rg --files src"),
        "source_query",
        "list",
    ),
    Clause(
        "unknown_suffix_on_search",
        _bash("rg foo vendor/pkg.dat"),
        "unknown_source",
        "unknown",
    ),
    Clause(
        "unknown_directory_on_search",
        _bash("rg foo vendor/"),
        "unknown_source",
        "unknown",
    ),
    Clause(
        "literal_log_exception",
        _bash("rg -F 'timeout' logs/run.log"),
        "unrelated",
        "literal",
    ),
    Clause(
        "literal_txt_under_logs",
        _bash("grep -F timeout logs/trace.txt"),
        "unrelated",
        "literal",
    ),
    Clause(
        "source_dir_precedes_config_suffix",
        _bash("rg -F foo src/config.json"),
        "source_query",
        "source",
    ),
    Clause(
        "recursive_option_blocks_log_exception",
        _bash("grep -r -F timeout logs/run.log"),
        "unknown_source",
        "unknown",
    ),
    Clause(
        "filename_listing_blocks_log_exception",
        _bash("rg -l -F timeout logs/run.log"),
        "unknown_source",
        "list",
    ),
    Clause(
        "printf_pipe_grep_unrelated",
        _bash("printf 'hello' | grep hello"),
        "unrelated",
        "literal",
    ),
    Clause(
        "cat_source_pipe_is_source_discovery",
        _bash("cat src/a.py | grep hello"),
        "source_query",
        "source",
    ),
    Clause(
        "mixed_source_and_log_paths",
        _bash("rg foo logs/run.log src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "unknown_pipe_origin",
        _bash("mystery | grep foo"),
        "unknown_source",
        "origin",
    ),
    Clause(
        "quoted_shell_looking_text_is_data",
        _bash("printf 'rg foo src'"),
        "unrelated",
        "data",
    ),
    Clause(
        "quoted_grep_is_not_source_query",
        _bash("echo 'grep -n foo src/a.py'"),
        "unrelated",
        "data",
    ),
    Clause(
        "heredoc_body_is_data",
        _bash("cat <<'EOF' | grep x\nhello\nEOF"),
        "unrelated",
        "heredoc",
    ),
    Clause(
        "unquoted_bash_heredoc_is_opaque",
        _bash("bash <<EOF\nrg foo src/a.py\nEOF"),
        "unknown_source",
        "heredoc",
    ),
    Clause(
        "command_substitution_unsupported",
        _bash("rg foo $(echo src)"),
        "unknown_source",
        "substitution",
    ),
    Clause(
        "backtick_substitution_unsupported",
        _bash("rg foo `echo src`"),
        "unknown_source",
        "substitution",
    ),
    Clause(
        "eval_unsupported",
        _bash("eval 'rg foo src/a.py'"),
        "unknown_source",
        "eval",
    ),
    Clause(
        "dynamic_search_path",
        _bash("rg foo $SRC"),
        "unknown_source",
        "dynamic",
    ),
    Clause(
        "nonliteral_bash_c_body",
        _bash('bash -c "$CMD"'),
        "unknown_source",
        "nonliteral",
    ),
    Clause(
        "python_c_unsupported",
        _bash("python -c 'print(open(\"src/a.py\").read())'"),
        "unknown_source",
        "python",
    ),
    Clause(
        "alias_unsupported",
        _bash("alias g=grep; g foo src/a.py"),
        "unknown_source",
        "alias",
    ),
    Clause(
        "pytest_unrelated",
        _bash("pytest -q"),
        "unrelated",
        "unrelated",
    ),
    Clause(
        "make_test_unrelated",
        _bash("make test"),
        "unrelated",
        "unrelated",
    ),
    Clause(
        "git_status_unrelated",
        _bash("git status"),
        "unrelated",
        "unrelated",
    ),
    Clause(
        "native_grep_source_path",
        _grep(pattern="timeout", path="src/workbay.py"),
        "source_query",
        "source",
    ),
    Clause(
        "native_grep_omitted_path",
        _grep(pattern="timeout"),
        "source_query",
        "omitted",
    ),
    Clause(
        "native_grep_source_glob",
        _grep(pattern="timeout", glob="*.py"),
        "source_query",
        "type",
    ),
    Clause(
        "native_grep_literal_log",
        _grep(pattern="timeout", path="logs/run.log"),
        "unrelated",
        "literal",
    ),
    Clause(
        "native_glob_source_rule",
        _glob(pattern="**/*.py", path="src"),
        "source_query",
        "source",
    ),
    Clause(
        "native_glob_not_automatically_exempt",
        _glob(pattern="logs/*.log"),
        "unknown_source",
        "unknown",
    ),
    Clause(
        "unrelated_native_edit_not_blocked",
        {"tool_name": "Edit", "tool_input": {"file_path": "src/a.py"}},
        "unrelated",
        "unrelated",
    ),
    Clause(
        "redirection_from_source_file",
        _bash("grep foo < src/a.py"),
        "source_query",
        "source",
    ),
    Clause(
        "source_path_after_double_dash",
        _bash("grep -F -- foo src/a.py"),
        "source_query",
        "source",
    ),
]


for _suffix in SOURCE_SUFFIXES:
    CLASSIFIER_CASES.append(
        Clause(
            f"source_suffix{_suffix}",
            _bash(f"rg foo path/file{_suffix}"),
            "source_query",
            "source",
        )
    )

for _directory in SOURCE_DIRS:
    CLASSIFIER_CASES.append(
        Clause(
            f"source_dir_{_directory}",
            _bash(f"rg foo {_directory}/pkg"),
            "source_query",
            "source",
        )
    )

for _config in CONFIG_FILES:
    CLASSIFIER_CASES.append(
        Clause(
            f"literal_config_{_config.replace('.', '_')}",
            _bash(f"rg -F timeout {_config}"),
            "unrelated",
            "literal",
        )
    )


@pytest.mark.parametrize("case", CLASSIFIER_CASES, ids=lambda case: case.clause)
def test_classify_source_query_grammar_clause(case: Clause) -> None:
    hook = _load_hook()
    result = hook.classify_source_query(case.payload)
    assert result.kind == case.kind, (
        f"{case.clause}: expected {case.kind} got {result.kind} "
        f"reason={result.reason!r} spans={result.spans!r}"
    )
    assert case.reason_contains.lower() in result.reason.lower(), (
        f"{case.clause}: expected reason to contain {case.reason_contains!r}, "
        f"got {result.reason!r}"
    )
    assert isinstance(result.spans, tuple)


def test_falsifier_quoted_grep_is_not_source_query() -> None:
    hook = _load_hook()
    result = hook.classify_source_query(_bash("printf 'rg foo src'"))
    assert result.kind != "source_query"
    assert result.kind == "unrelated"


def test_falsifier_heredoc_grep_is_not_source_query() -> None:
    hook = _load_hook()
    result = hook.classify_source_query(_bash("cat <<'EOF' | grep x\nhello\nEOF"))
    assert result.kind != "source_query"
    assert result.kind == "unrelated"


def test_falsifier_unsupported_construct_is_unknown_source() -> None:
    hook = _load_hook()
    result = hook.classify_source_query(_bash("rg foo $(echo src)"))
    assert result.kind == "unknown_source"


def test_unrelated_does_not_read_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = _load_hook()

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("unrelated commands must not read the ledger")

    monkeypatch.setattr(hook, "load_codemap_mode", _boom)
    decision = hook.decide_from_payload(_bash("git status"))
    assert decision.status == "allow"


def test_source_query_reads_policy_once(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = _load_hook()
    reads: list[object] = []

    def _once(*args: object, **kwargs: object) -> object:
        reads.append((args, kwargs))
        return hook.ModeRead(
            status="ok", value="enforced", origin="explicit", intent="known"
        )

    monkeypatch.setattr(hook, "load_codemap_mode", _once)
    decision = hook.decide_from_payload(
        _bash("rg foo src/a.py"),
        repo_root=".",
    )
    assert len(reads) == 1
    assert decision.status == "refuse_source_query"


def test_enforced_unknown_source_refuses_unsupported_source_query() -> None:
    hook = _load_hook()
    classification = hook.classify_source_query(_bash("rg foo $(echo src)"))
    mode = hook.ModeRead(
        status="ok", value="enforced", origin="explicit", intent="known"
    )
    decision = hook.decide_guard(classification, mode)
    assert decision.status == "refuse_source_query"
    assert decision.reason == "unsupported_source_query"


def test_enforced_source_query_refuses() -> None:
    hook = _load_hook()
    classification = hook.classify_source_query(_bash("rg foo src/a.py"))
    mode = hook.ModeRead(
        status="ok", value="enforced", origin="explicit", intent="known"
    )
    decision = hook.decide_guard(classification, mode)
    assert decision.status == "refuse_source_query"


def test_available_source_query_advises() -> None:
    hook = _load_hook()
    classification = hook.classify_source_query(_bash("rg foo src/a.py"))
    mode = hook.ModeRead(
        status="ok", value="available", origin="explicit", intent="known"
    )
    decision = hook.decide_guard(classification, mode)
    assert decision.status == "advise"


def test_off_source_query_allows() -> None:
    hook = _load_hook()
    classification = hook.classify_source_query(_bash("rg foo src/a.py"))
    mode = hook.ModeRead(status="ok", value="off", origin="explicit", intent="known")
    decision = hook.decide_guard(classification, mode)
    assert decision.status == "allow"


def test_legacy_missing_field_advises() -> None:
    hook = _load_hook()
    classification = hook.classify_source_query(_bash("rg foo src/a.py"))
    mode = hook.ModeRead(
        status="field_missing", value=None, origin="missing", intent="unknown"
    )
    decision = hook.decide_guard(classification, mode)
    assert decision.status == "advise"


def test_malformed_policy_is_broken_enforcement() -> None:
    hook = _load_hook()
    classification = hook.classify_source_query(_bash("rg foo src/a.py"))
    mode = hook.ModeRead(
        status="malformed", value=None, origin="missing", intent="unknown"
    )
    decision = hook.decide_guard(classification, mode)
    assert decision.status == "broken_enforcement"


def test_import_error_is_unknown_intent(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = _load_hook()
    monkeypatch.setattr(hook, "load_codemap_mode", None)
    decision = hook.decide_from_payload(_bash("rg foo src/a.py"), repo_root=".")
    assert decision.status == "advise"
    assert decision.classification.kind == "source_query"


def test_malformed_search_payload_fails_loudly() -> None:
    hook = _load_hook()
    result = hook.classify_source_query("{not-json rg foo src")
    assert result.kind == "unknown_source"


def test_root_and_payload_hook_are_byte_identical() -> None:
    assert HOOK_SCRIPT.is_file()
    assert PAYLOAD_HOOK.is_file()
    assert HOOK_SCRIPT.read_bytes() == PAYLOAD_HOOK.read_bytes()


def test_root_and_payload_run_guard_are_byte_identical() -> None:
    assert WRAPPER.read_bytes() == PAYLOAD_RUN_GUARD.read_bytes()


def test_wrap_guard_command_emits_policy_aware_flag() -> None:
    wrap = _load_wrap()
    wrapped = wrap.wrap_guard_command("python3 scripts/hooks/guard-codemap-first.py")
    assert "--policy-aware" in wrapped
    assert wrapped.endswith("scripts/hooks/guard-codemap-first.py")
    assert wrap.wrap_guard_command(wrapped) == wrapped


def test_wrap_guard_command_keeps_fail_mode_closed() -> None:
    wrap = _load_wrap()
    wrapped = wrap.wrap_guard_command(
        "python3 scripts/hooks/guard-codemap-first.py",
        fail_mode="closed",
    )
    assert "--fail-mode=closed" in wrapped
    assert "--policy-aware" in wrapped


def _write_manifest(root: Path, mode: str) -> None:
    payload = {
        "schema_version": 1,
        "remote_url": "https://example.invalid/repo.git",
        "remote_ref": "main",
        "remote_sha": "0" * 40,
        "codemap_mode": mode,
    }
    (root / ".workbay-bootstrap.json").write_text(json.dumps(payload), encoding="utf-8")


def _run_wrapper(
    root: Path,
    *args: str,
    stdin: str,
    env_extra: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, str(WRAPPER), *args],
        input=stdin,
        capture_output=True,
        text=True,
        timeout=15,
        cwd=root,
        env=env,
    )


def test_wrapper_allows_unrelated_when_handler_missing(tmp_path: Path) -> None:
    (tmp_path / "scripts" / "hooks").mkdir(parents=True)
    (tmp_path / ".github" / "hooks").mkdir(parents=True)
    result = _run_wrapper(
        tmp_path,
        "--fail-mode=closed",
        "--policy-aware",
        "scripts/hooks/guard-codemap-first.py",
        stdin=json.dumps(_bash("git status")),
    )
    assert result.returncode == 0, result.stderr


def test_wrapper_honors_explicit_off_when_handler_missing(tmp_path: Path) -> None:
    (tmp_path / "scripts" / "hooks").mkdir(parents=True)
    (tmp_path / ".github" / "hooks").mkdir(parents=True)
    _write_manifest(tmp_path, "off")
    result = _run_wrapper(
        tmp_path,
        "--fail-mode=closed",
        "--policy-aware",
        "scripts/hooks/guard-codemap-first.py",
        stdin=json.dumps(_bash("rg foo src/a.py")),
    )
    assert result.returncode == 0, result.stderr


def test_wrapper_refuses_unsupported_search_when_enforced(tmp_path: Path) -> None:
    (tmp_path / "scripts" / "hooks").mkdir(parents=True)
    (tmp_path / ".github" / "hooks").mkdir(parents=True)
    _write_manifest(tmp_path, "enforced")
    result = _run_wrapper(
        tmp_path,
        "--fail-mode=closed",
        "--policy-aware",
        "scripts/hooks/guard-codemap-first.py",
        stdin=json.dumps(_bash("rg foo $(echo src)")),
    )
    assert result.returncode == 2, result.stderr
    assert "unsupported_source_query" in result.stderr


def test_wrapper_allows_unrelated_when_enforced(tmp_path: Path) -> None:
    (tmp_path / "scripts" / "hooks").mkdir(parents=True)
    (tmp_path / ".github" / "hooks").mkdir(parents=True)
    _write_manifest(tmp_path, "enforced")
    result = _run_wrapper(
        tmp_path,
        "--fail-mode=closed",
        "--policy-aware",
        "scripts/hooks/guard-codemap-first.py",
        stdin=json.dumps(_bash("pytest -q")),
    )
    assert result.returncode == 0, result.stderr


def test_direct_handler_uses_passed_policy_and_does_not_reread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hook = _load_hook()
    reads: list[object] = []

    def _track(*args: object, **kwargs: object) -> object:
        reads.append((args, kwargs))
        return hook.ModeRead(
            status="ok", value="enforced", origin="explicit", intent="known"
        )

    monkeypatch.setattr(hook, "load_codemap_mode", _track)
    passed = hook.ModeRead(
        status="ok", value="enforced", origin="explicit", intent="known"
    )
    decision = hook.decide_from_payload(
        _bash("rg foo src/a.py"),
        mode_read=passed,
        repo_root=str(tmp_path),
    )
    assert reads == []
    assert decision.status == "refuse_source_query"
