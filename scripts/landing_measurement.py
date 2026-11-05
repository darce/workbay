"""Producer-owned measurement provenance for the landing gate.

This module is the out-of-band completion and identity surface. A printable
pytest summary is forensic evidence only; it cannot certify that a measured
run finished [RLSE-05, AGT-04].

Compatibility contract [DATA-03]
    Identity is ``producer_fingerprint``: SHA-256 over ``COMPAT_VERSION`` and
    the bytes of every producer layer actually invoked (``landing_gate.py``,
    this module, and the ``gate_ids.sh`` wrapper that executed). It is *not*
    the git HEAD of the measured worktree and it is not a self-asserted
    token. The local trusted-producer boundary is those files next to the
    consumer process: the verifier recomputes the digest from that exact set
    and rejects a receipt or scope whose fingerprint does not match. A
    selected sibling named ``gate_ids.sh`` is only the default when the
    executed wrapper is unknown; producers must bind the wrapper path that
    actually ran [LANDGA-S1C-M04]. This proves the artifacts were produced by
    the producer code the consumer is executing; it is not PKI and does not
    authenticate a remote party. Baseline and subject HEADs and worktree
    paths may differ; ``LANDING_GATE_SCRIPT`` already allows one producer for
    both measurements. Missing identity is ``scope_unverified``; unequal or
    untrusted fingerprints or compat tokens are ``producer_mismatched``.

    A complete receipt proves that the bounded producer holding the local key
    issued the authenticated status and final artifact manifest. A status or
    receipt without valid producer authentication is refused. It also records
    tripwires for accidental narrowing from ambient pytest options, project
    configuration, or plugin drift. It does not prove that subject tests told
    the truth: the measured child runs the subject's conftest and test code in
    the producer's process, so that code is inside the trust boundary and can
    read the key or rewrite its proof. Project configuration is inside that
    boundary too. ``_pytest_unbound_discovery_hooks``, selection comparisons,
    and the configuration check detect accidental drift; they are not
    anti-forgery controls. Decision 28140 tracks a stronger boundary, such as
    sandboxing or a separate principal for subject code, as a separate design
    item.

Bounded lifetimes [RES-02, RES-06]
    ``run_bounded_pytest`` applies a finite wall-clock bound to the pytest
    process group, kills the tree on expiry, and returns a typed timeout
    outcome. Callers must not publish success-shaped id/scope/receipt artifacts
    for a timed-out or unavailable run.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Mapping, Sequence

COMPAT_VERSION = "landing-measurement/v1"
RECEIPT_SCHEMA = "landing_measurement_receipt/v1"
ATTEMPT_SCHEMA = "landing_measurement_attempt/v1"
RECEIPT_COMPLETE = "complete"
BOUNDED_PYTEST_PRODUCER = "run_bounded_pytest"
DEFAULT_TIMEOUT_SEC = 3600
TIMEOUT_EXIT = 5
UNAVAILABLE_EXIT = 4
COMPLETE_REFUSED_EXIT = 2
SIGNAL_EXIT = 6
WRAPPER_LAYER_NAME = "gate_ids.sh"
RUN_MARKER_ENV = "LANDING_GATE_RUN_MARKER"
# pytest source: _pytest/config/__init__.py (pytest 9.1.1): ADDOPTS at
# 1523-1526 extends argv; PLUGINS at 874 loads plugins; DISABLE_PLUGIN_AUTOLOAD
# at 1578 disables entry-point plugins, which can alter collection/selection.
PYTEST_OPTION_ENV_VARS = ("PYTEST_ADDOPTS", "PYTEST_PLUGINS", "PYTEST_DISABLE_PLUGIN_AUTOLOAD")


def _bound_pytest_environment(env: Mapping[str, str]) -> tuple[dict[str, str], list[str]]:
    bound = dict(env)
    removed = [name for name in PYTEST_OPTION_ENV_VARS if name in bound]
    for name in removed:
        del bound[name]
    return bound, removed


def _python_warning_options(env: Mapping[str, str]) -> list[str]:
    return [option for option in env.get("PYTHONWARNINGS", "").split(",") if option]


def _python_interpreter_flags(env: Mapping[str, str]) -> tuple[list[str], list[str]]:
    flags: list[str] = []
    honored: set[str] = set()
    if env.get("PYTHONDONTWRITEBYTECODE"):
        flags.append("-B")
        honored.add("PYTHONDONTWRITEBYTECODE")
    utf8 = env.get("PYTHONUTF8")
    if utf8 == "1":
        flags.extend(("-X", "utf8"))
        honored.add("PYTHONUTF8")
    elif utf8 == "0":
        flags.extend(("-X", "utf8=0"))
        honored.add("PYTHONUTF8")
    if env.get("PYTHONDEVMODE"):
        flags.extend(("-X", "dev"))
        honored.add("PYTHONDEVMODE")
    # -W keeps sys.warnoptions as ordinary pytest sees it. Under -I it cannot import
    # categories from PYTHONPATH, so the bootstrap applies the entries again and the
    # session proof confirms them. Until then PYTHONWARNINGS stays listed as ignored.
    for option in _python_warning_options(env):
        flags.extend(("-W", option))
    ignored = sorted(name for name in env if name.startswith("PYTHON") and name != "PYTHONPATH" and name not in honored)
    return flags, ignored


DESCENDANT_SNAPSHOT_UNAVAILABLE = "descendant_snapshot_unavailable"
PROCESS_TABLE_SNAPSHOT_ATTEMPTS = 3
PROCESS_TABLE_SNAPSHOT_TIMEOUT_SEC = 1
_PYTEST_EXECUTED = 0
_PYTEST_STARTED = False
_PYTEST_PLANNED_SELECTION: dict[str, Any] | None = None
_PYTEST_EXPECTED_ITEMS: dict[str, Any] | None = None
_PYTEST_LOADED_CONFIGURATION: dict[str, Any] | None = None
_PYTEST_NORECURSEDIRS_PRUNED: set[str] = set()
_PYTEST_PROOF_FD_ENV = "LANDING_GATE_SESSION_PROOF_FD"
_SESSION_PROOF_LIMIT = 65536
_PYTEST_SELECTION_OPTIONS = (
    "keyword",
    "markexpr",
    "deselect",
    "ignore",
    "ignore_glob",
    "pyargs",
    "collectonly",
    "keepduplicates",
    "importmode",
    "doctestmodules",
    "doctestglob",
)
_PYTEST_SELECTION_INI = frozenset(
    {
        "addopts",
        "testpaths",
        "python_files",
        "python_classes",
        "python_functions",
        "norecursedirs",
        "collect_imported_tests",
        "consider_namespace_packages",
        "doctest_optionflags",
        "doctest_encoding",
    }
)
_PYTEST_REPORT_ONLY_ADDOPTS = frozenset({"--strict-markers", "--strict-config"})
_PYTEST_REPORT_ONLY_TRACEBACKS = frozenset({"auto", "long", "short", "line", "native", "no"})
_PYTEST_REPORT_CHARS = frozenset("fEwspxXPaA")

# Import the installed pytest before subject paths can shadow it, and load the
# producer plugin by its exact trusted file, never by a subject-controlled name.
_PYTEST_BOOTSTRAP = """
import importlib.util, json, os, pathlib, sys
runtime = json.loads(sys.argv[1])
sys.path.insert(0, str(pathlib.Path(runtime['pytest']).parent.parent))
import pytest, _pytest
if (str(pathlib.Path(pytest.__file__).resolve()) != runtime['pytest']
        or str(pathlib.Path(_pytest.__file__).resolve()) != runtime['_pytest']):
    raise SystemExit('pytest_runtime_origin_mismatched')
spec = importlib.util.spec_from_file_location('_landing_measurement_producer_plugin', sys.argv[2])
plugin = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = plugin
spec.loader.exec_module(plugin)
# Restore the measured application's imports only after trusted bootstrapping.
sys.path[:0] = [os.getcwd(), *filter(None, os.environ.get('PYTHONPATH', '').split(os.pathsep))]
# PYTHONWARNINGS categories may live on PYTHONPATH, so they are applied only now.
plugin._apply_python_warnings(json.loads(sys.argv[3]))
raise SystemExit(pytest.main(sys.argv[4:], plugins=[plugin]))
"""


def _print_pytest_session_marker(phase: str) -> None:
    token = os.environ.get(RUN_MARKER_ENV)
    if token:
        # Quiet pytest can leave its progress line unterminated at finish.
        print(f"\n{RUN_MARKER_ENV}={token} session={phase}", flush=True)


_PYTHON_WARNINGS_UNRESOLVED: list[str] | None = None


def _apply_python_warnings(options: Sequence[str]) -> None:
    """Apply PYTHONWARNINGS entries once the application's imports are restored.

    Under -I, startup cannot import a warning category defined on PYTHONPATH, so
    the same entries fail there. Each entry that still fails is kept with its reason.
    """
    import warnings

    global _PYTHON_WARNINGS_UNRESOLVED
    unresolved = []
    for option in options:
        try:
            warnings._setoption(option)  # the parser -W uses at interpreter startup
        except Exception as exc:
            unresolved.append(f"{type(exc).__name__}: {exc}")
    _PYTHON_WARNINGS_UNRESOLVED = unresolved


def _pytest_selection_options(namespace: Any) -> dict[str, Any]:
    # Copy lists too: a conftest can mutate argparse values in place.
    return json.loads(json.dumps({name: getattr(namespace, name, None) for name in _PYTEST_SELECTION_OPTIONS}))


def _pytest_selection_ini(config: Any) -> dict[str, Any]:
    settings = {}
    for name in sorted(_PYTEST_SELECTION_INI):
        try:
            settings[name] = config.getini(name)
        except ValueError:
            # Supported pytest versions do not all register the same settings.
            continue
    return json.loads(json.dumps(settings))


def _pytest_ini_configuration(config: Any, overrides: Sequence[str] | None) -> dict[str, Any]:
    from _pytest.config.findpaths import load_config_dict_from_file

    # The binding rules care about explicitly configured selection settings,
    # not pytest defaults or unrelated (possibly unregistered) plugin options.
    configured = dict(load_config_dict_from_file(config.inipath) or {}) if config.inipath else {}
    for setting in overrides or ():
        name, _, value = setting.partition("=")
        configured[name] = value
    settings = {}
    for name in sorted(_PYTEST_SELECTION_INI.intersection(configured)):
        try:
            settings[name] = config.getini(name)
        except ValueError:
            # A disabled plugin does not register this setting, so pytest cannot return it.
            # Keep the explicit text: pytest 9 wraps file values in ConfigValue, older ones do not.
            settings[name] = getattr(configured[name], "value", configured[name])
    return json.loads(json.dumps(settings, default=os.fspath))


def pytest_load_initial_conftests(early_config: Any) -> None:
    global _PYTEST_PLANNED_SELECTION, _PYTEST_EXPECTED_ITEMS, _PYTEST_LOADED_CONFIGURATION
    namespace = early_config.known_args_namespace
    _PYTEST_PLANNED_SELECTION = {
        "targets": list(namespace.file_or_dir),
        "options": _pytest_selection_options(namespace),
        "ini": _pytest_selection_ini(early_config),
    }
    # Snapshot the configuration pytest loaded. A test can rewrite or delete the
    # file during the run, so the proof must not read it again at session finish.
    _PYTEST_LOADED_CONFIGURATION = {
        "ini": _pytest_ini_configuration(early_config, namespace.override_ini),
        "config_path": str(early_config.inipath) if early_config.inipath else None,
        "config_sha256": sha256_file(early_config.inipath),
    }
    _PYTEST_EXPECTED_ITEMS = None
    _PYTEST_NORECURSEDIRS_PRUNED.clear()


# Keep this module importable without pytest installed. This is pytest's hook
# marker protocol, equivalent to @pytest.hookimpl(tryfirst=True).
pytest_load_initial_conftests.pytest_impl = {"tryfirst": True}


def _pytest_item_manifest(items: Sequence[Any]) -> dict[str, Any]:
    nodeids = sorted(item.nodeid for item in items)
    return {"count": len(nodeids), "sha256": hashlib.sha256(json.dumps(nodeids).encode()).hexdigest()}


def _pytest_expected_selection(items: Sequence[Any]) -> dict[str, Any]:
    from types import SimpleNamespace

    from _pytest.main import pytest_collection_modifyitems as deselect_by_nodeid
    from _pytest.mark import deselect_by_keyword, deselect_by_mark

    # Apply installed pytest's selectors to describe the producer-observed
    # set. Subject code in this child can influence the set and is inside the
    # stated trust boundary; this comparison only detects accidental drift.
    options = _PYTEST_PLANNED_SELECTION["options"]
    config = SimpleNamespace(
        option=SimpleNamespace(**options),
        getoption=lambda name: options[name],
        hook=SimpleNamespace(pytest_deselected=lambda **kwargs: None),
    )
    expected = list(items)
    deselect_by_keyword(expected, config)
    deselect_by_mark(expected, config)
    deselect_by_nodeid(expected, config)
    return _pytest_item_manifest(expected)


def pytest_collection_modifyitems(items: list[Any]) -> Iterator[None]:
    global _PYTEST_EXPECTED_ITEMS
    _PYTEST_EXPECTED_ITEMS = _pytest_expected_selection(items)
    yield


pytest_collection_modifyitems.pytest_impl = {"hookwrapper": True, "tryfirst": True}


def pytest_ignore_collect(collection_path: Any, config: Any) -> Iterator[None]:
    """Record pytest's configured directory pruning beneath explicit targets."""
    from _pytest.pathlib import fnmatch_ex

    outcome = yield
    if outcome.get_result() is not True or not Path(collection_path).is_dir():
        return
    candidate = Path(collection_path).absolute()
    targets = _PYTEST_PLANNED_SELECTION["targets"] if _PYTEST_PLANNED_SELECTION else []
    for raw_target in targets:
        if not isinstance(raw_target, str) or "::" in raw_target:
            continue
        target = Path(raw_target)
        if not target.is_absolute():
            target = Path.cwd() / target
        target = target.absolute()
        if candidate == target or not candidate.is_relative_to(target):
            continue
        patterns = config.getini("norecursedirs")
        if any(fnmatch_ex(pattern, candidate) for pattern in patterns):
            _PYTEST_NORECURSEDIRS_PRUNED.add(str(candidate))
            return


pytest_ignore_collect.pytest_impl = {"hookwrapper": True, "trylast": True}


def _pytest_unbound_discovery_hooks(config: Any) -> list[str]:
    import inspect

    import _pytest

    trusted_root = Path(_pytest.__file__).resolve().parent
    unbound = []
    # These hooks can suppress files before an item-set snapshot exists. This
    # records external implementations as a drift tripwire, not a barrier to
    # subject code in the measured child.
    for name in ("pytest_ignore_collect", "pytest_collect_file", "pytest_collect_directory", "pytest_collection"):
        hook = getattr(config.hook, name, None)
        if hook is None:
            continue
        for implementation in hook.get_hookimpls():
            try:
                origin = Path(inspect.getfile(implementation.function)).resolve()
            except (OSError, TypeError):
                unbound.append(name)
                continue
            if not origin.is_relative_to(trusted_root) and origin != Path(__file__).resolve():
                unbound.append(f"{name}:{origin}")
    # The built-in ignore hook also consumes variables from conftest modules;
    # these must not bypass the custom-hook guard above.
    for plugin in config.pluginmanager.get_plugins():
        for name in ("collect_ignore", "collect_ignore_glob"):
            if getattr(plugin, name, None):
                unbound.append(f"{name}:{getattr(plugin, '__file__', '<plugin>')}")
    return sorted(set(unbound))


def pytest_sessionstart(session: Any) -> None:
    """Stdlib-only plugin loaded by the bounded producer in its pytest child."""
    global _PYTEST_EXECUTED, _PYTEST_STARTED
    _PYTEST_EXECUTED = 0
    _PYTEST_STARTED = True
    _print_pytest_session_marker("start")


def pytest_runtest_logreport(report: Any) -> None:
    global _PYTEST_EXECUTED
    if report.when == "call" and not report.skipped:
        _PYTEST_EXECUTED += 1


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    _write_pytest_session_proof(session)
    _print_pytest_session_marker(f"executed:{_PYTEST_EXECUTED}")
    # Project configuration and plugins can change selection after argv was
    # bound. A collection-only or zero-execution session is not a measurement.
    if not session.config.option.collectonly and _PYTEST_EXECUTED > 0:
        _print_pytest_session_marker("finish")
    else:
        _print_pytest_session_marker("not_executed")


def _write_pytest_session_proof(session: Any) -> None:
    import _pytest
    import pytest

    # Missing when the loading hook never ran; an absent ini then fails binding.
    loaded = _PYTEST_LOADED_CONFIGURATION or {}
    proof = {
        "run_token": os.environ.get(RUN_MARKER_ENV),
        "started": _PYTEST_STARTED,
        "executed": _PYTEST_EXECUTED,
        "collectonly": session.config.option.collectonly,
        "runtime": {
            "pytest": str(Path(pytest.__file__).resolve()),
            "_pytest": str(Path(_pytest.__file__).resolve()),
        },
        "plugin": str(Path(__file__).resolve()),
        "python_warnings_unresolved": _PYTHON_WARNINGS_UNRESOLVED,
        "args": list(session.config.invocation_params.args),
        "addopts": list(session.config.getini("addopts")),
        "ini": loaded.get("ini"),
        "config_path": loaded.get("config_path"),
        "config_sha256": loaded.get("config_sha256"),
        "selection": {
            "planned": _PYTEST_PLANNED_SELECTION,
            "effective": {
                "targets": list(session.config.args),
                "options": _pytest_selection_options(session.config.option),
                "ini": _pytest_selection_ini(session.config),
            },
            "expected_items": _PYTEST_EXPECTED_ITEMS,
            "selected_items": _pytest_item_manifest(session.items),
            "unbound_discovery_hooks": _pytest_unbound_discovery_hooks(session.config),
            "norecursedirs_pruned": sorted(_PYTEST_NORECURSEDIRS_PRUNED),
        },
    }
    data = json.dumps(proof, sort_keys=True).encode()
    fd = os.environ.get(_PYTEST_PROOF_FD_ENV)
    if fd is not None:
        if len(data) > _SESSION_PROOF_LIMIT:
            data = json.dumps(
                {
                    "error": "pytest_session_proof_too_large",
                    "size": len(data),
                    "limit": _SESSION_PROOF_LIMIT,
                },
                sort_keys=True,
            ).encode()
        os.write(int(fd), data)


def _session_proof_is_complete(proof: Any, token: str, runtime: Any) -> bool:
    if not isinstance(proof, dict) or not isinstance(runtime, dict):
        return False
    return (
        proof.get("run_token") == token
        and proof.get("started") is True
        and isinstance(proof.get("executed"), int)
        and proof["executed"] > 0
        and proof.get("collectonly") is False
        and proof.get("runtime") == runtime
        and proof.get("plugin") == str(Path(__file__).resolve())
        and _pytest_configuration_is_bound(proof)
    )


def _pytest_configuration_is_bound(proof: Mapping[str, Any]) -> bool:
    ini = proof.get("ini")
    if not isinstance(ini, dict):
        return False
    if not _pytest_addopts_are_bound(proof.get("addopts")):
        return False
    selection = proof.get("selection")
    if not isinstance(selection, dict):
        return False
    unconditionally_unbound = _PYTEST_SELECTION_INI - {"addopts", "testpaths", "norecursedirs"}
    if unconditionally_unbound.intersection(ini):
        return False
    if "testpaths" in ini and not _pytest_has_recorded_target(selection):
        return False
    if "norecursedirs" in ini and selection.get("norecursedirs_pruned"):
        return False
    return _pytest_selection_is_bound(selection)


def _pytest_addopts_are_bound(addopts: Any) -> bool:
    if not isinstance(addopts, list) or not all(isinstance(token, str) for token in addopts):
        return False
    for token in addopts:
        if token in _PYTEST_REPORT_ONLY_ADDOPTS:
            continue
        if token.startswith("-r") and len(token) > 2 and set(token[2:]) <= _PYTEST_REPORT_CHARS:
            continue
        if token.startswith("--tb=") and token[5:] in _PYTEST_REPORT_ONLY_TRACEBACKS:
            continue
        return False
    return True


def _pytest_has_recorded_target(selection: Mapping[str, Any]) -> bool:
    planned = selection.get("planned")
    if not isinstance(planned, dict):
        return False
    targets = planned.get("targets")
    return isinstance(targets, list) and any(isinstance(target, str) and target.strip() for target in targets)


def _pytest_selection_is_bound(selection: Any) -> bool:
    if not isinstance(selection, dict):
        return False
    return (
        isinstance(selection.get("planned"), dict)
        and selection["planned"] == selection.get("effective")
        and isinstance(selection.get("expected_items"), dict)
        and selection["expected_items"] == selection.get("selected_items")
        and selection.get("unbound_discovery_hooks") == []
        and isinstance(selection.get("norecursedirs_pruned"), list)
    )


@contextmanager
def _open_regular_artifact(path: Path | str) -> Iterator[tuple[BinaryIO, int]]:
    # O_NONBLOCK closes the check/open race for FIFOs. Validate the opened
    # object, and pin a finite read budget even if another writer appends.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise OSError("artifact_not_regular_file")
        yield handle, before.st_size
        after = os.fstat(handle.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise OSError("artifact_changed_during_read")


def sha256_file(path: Path | str | None) -> str | None:
    if path is None:
        return None
    digest = hashlib.sha256()
    try:
        with _open_regular_artifact(path) as (handle, remaining):
            while remaining:
                chunk = handle.read(min(remaining, 1024 * 1024))
                if not chunk:
                    raise OSError("artifact_truncated_during_read")
                digest.update(chunk)
                remaining -= len(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def read_file_snapshot(path: Path | str) -> dict[str, Any]:
    """Read bytes once and return path, text, and SHA-256 of those exact bytes."""
    candidate = Path(path)
    with _open_regular_artifact(candidate) as (handle, size):
        data = handle.read(size)
        if len(data) != size:
            raise OSError("artifact_truncated_during_read")
    return {
        "path": candidate,
        "text": data.decode("utf-8", errors="surrogateescape"),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def try_file_snapshot(path: Path | str | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        return read_file_snapshot(path)
    except OSError:
        return None


def producer_fingerprint(*paths: Path | str, layer_names: Sequence[str] | None = None) -> str:
    """Immutable extractor/producer identity. Worktree HEADs are not this value.

    ``layer_names`` pins the digest slot for a relocated executed wrapper so a
    ``/tmp`` snapshot of ``gate_ids.sh`` still occupies the wrapper layer
    rather than hashing an accidental basename [LANDGA-S1C-M04].
    """
    digest = hashlib.sha256()
    digest.update(COMPAT_VERSION.encode("utf-8"))
    for index, raw in enumerate(paths):
        path = Path(raw)
        name = layer_names[index] if layer_names is not None else path.name
        digest.update(b"\0")
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


#: Security-relevant producer layers next to ``landing_gate.py``. Split modules
#: are included so a fingerprint cannot silently omit parser/scope/probe/policy
#: bytes after REVIEW-M-07.
_PRODUCER_SIBLINGS = (
    "landing_measurement.py",
    "gate_ids.sh",
    "landing_gate_parsing.py",
    "landing_gate_scope.py",
    "landing_gate_probe.py",
    "landing_gate_policy.py",
)


def default_producer_paths(
    gate_script: Path | str | None = None,
    *,
    wrapper: Path | str | None = None,
) -> list[Path]:
    """Return the invoked gate script plus every security-relevant producer layer.

    Sibling lookup prefers files next to ``gate_script``. Missing required
    split modules then fall back to this module's directory so a relocated
    wrapper cannot silently drop parser/scope/probe/policy bytes.

    When ``wrapper`` is supplied it occupies the ``gate_ids.sh`` layer: the
    bytes that executed, not a selected sibling of the same name
    [LANDGA-S1C-M04]. A missing wrapper path is fail-closed (the selected
    sibling is not substituted).
    """
    executing_dir = Path(__file__).resolve().parent
    gate = Path(gate_script).resolve() if gate_script is not None else executing_dir / "landing_gate.py"
    paths = [gate]
    seen = {gate.resolve()}
    search_dirs: list[Path] = []
    for directory in (gate.parent, executing_dir):
        resolved_dir = directory.resolve()
        if resolved_dir not in search_dirs:
            search_dirs.append(resolved_dir)
    wrapper_path = Path(wrapper) if wrapper is not None else None
    if wrapper_path is not None and not wrapper_path.is_file():
        raise FileNotFoundError(f"executed producer wrapper is not a file: {wrapper_path}")

    def _add(candidate: Path) -> bool:
        if not candidate.is_file():
            return False
        resolved = candidate.resolve()
        if resolved in seen:
            return True
        paths.append(resolved)
        seen.add(resolved)
        return True

    for name in _PRODUCER_SIBLINGS:
        if name == WRAPPER_LAYER_NAME and wrapper_path is not None:
            _add(wrapper_path)
            continue
        for directory in search_dirs:
            if _add(directory / name):
                break
    _add(Path(__file__).resolve())
    return paths


def _layer_names_for(paths: Sequence[Path], *, wrapper: Path | str | None) -> list[str]:
    wrapper_resolved = Path(wrapper).resolve() if wrapper is not None else None
    names: list[str] = []
    for path in paths:
        if wrapper_resolved is not None and path.resolve() == wrapper_resolved:
            names.append(WRAPPER_LAYER_NAME)
        else:
            names.append(path.name)
    return names


def trusted_producer_fingerprint(
    gate_script: Path | str | None = None,
    *,
    wrapper: Path | str | None = None,
) -> str:
    """Digest of the local trusted producer files. Not a bearer token."""
    paths = default_producer_paths(gate_script, wrapper=wrapper)
    return producer_fingerprint(*paths, layer_names=_layer_names_for(paths, wrapper=wrapper))


def _is_terminal_returncode(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


# Pytest reserves these exits for a run that did not produce a usable test
# measurement: interruption, internal error, usage error, and a missing
# interpreter/launcher.  A receipt may exist for each, but none can certify a
# completed arm [OBS-08].
_INCOMPLETE_RETURN_CODES = frozenset({2, 3, 4, 127})


def _is_measurement_returncode(value: Any) -> bool:
    return _is_terminal_returncode(value) and value not in _INCOMPLETE_RETURN_CODES


def receipt_path_for_raw(raw_path: Path | str) -> Path:
    return Path(raw_path).with_suffix(".receipt")


def attempt_path_for_raw(raw_path: Path | str) -> Path:
    return Path(raw_path).with_suffix(".attempt")


def write_attempt_marker(
    path: Path | str,
    *,
    run_id: str | None,
    status: str = "incomplete",
    pid: int | None = None,
) -> Path:
    """Publish the current attempt before any later crash can leave stale complete evidence."""
    destination = Path(path)
    payload: dict[str, Any] = {
        "schema": ATTEMPT_SCHEMA,
        "status": status if isinstance(status, str) and status.strip() else "incomplete",
        "run_id": run_id if isinstance(run_id, str) and run_id.strip() else None,
        "pid": pid,
    }
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


def load_attempt(path: Path | str) -> dict[str, Any] | None:
    return load_receipt(path)


def attempt_run_id_from_payload(payload: Mapping[str, Any] | None) -> str | None:
    if not payload:
        return None
    if payload.get("schema") != ATTEMPT_SCHEMA:
        return None
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    return run_id.strip()


def _attempt_payload_from_snapshot(snapshot: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not snapshot:
        return None
    text = snapshot.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _json_payload_from_snapshot(snapshot: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not snapshot:
        return None
    text = snapshot.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _nonempty_path(value: Any) -> bool:
    # Keep text validation strict: argparse supplies path flags as Path objects,
    # while fingerprints, run ids, and other text fields must remain strings.
    if not isinstance(value, (str, os.PathLike)):
        return False
    try:
        path_text = os.fspath(value)
    except TypeError:
        return False
    return isinstance(path_text, str) and bool(path_text.strip())


def _raw_has_session_markers(raw: Path | str | None, token: str) -> bool:
    if raw is None:
        return False
    marker = f"{RUN_MARKER_ENV}={token} session=".encode()
    first_start = last_finish = None
    try:
        with _open_regular_artifact(raw) as (handle, remaining):
            # Cap line buffers too: a single enormous output line is not a marker.
            continued = False
            position = 0
            while remaining:
                fragment = handle.readline(min(65536, remaining))
                if not fragment:
                    raise OSError("artifact_truncated_during_read")
                remaining -= len(fragment)
                whole_line = not continued and fragment.endswith(b"\n")
                if whole_line or (not continued and len(fragment) < 65536):
                    line = fragment.rstrip(b"\r\n")
                    if line == marker + b"start" and first_start is None:
                        first_start = position
                    if line == marker + b"finish":
                        last_finish = position
                continued = not fragment.endswith(b"\n")
                position += 1
    except OSError:
        return False
    return first_start is not None and last_finish is not None and first_start < last_finish


def _producer_status_is_complete(
    payload: Mapping[str, Any] | None, raw_hash: str | None, raw: Path | str | None = None
) -> bool:
    """Accept completion only from the bounded pytest producer's returned proof."""
    if payload is None or payload.get("producer") != BOUNDED_PYTEST_PRODUCER:
        return False
    if not _status_authenticates(payload):
        return False
    if payload.get("pytest_attested") is not True:
        return False
    if payload.get("outcome") != "returned" or payload.get("timed_out") is not False:
        return False
    if not _is_measurement_returncode(payload.get("returncode")):
        return False
    if not _nonempty_text(payload.get("run_token")):
        return False
    if not _session_proof_is_complete(
        payload.get("pytest_session"), payload["run_token"], payload.get("pytest_runtime")
    ):
        return False
    if not _raw_has_session_markers(raw, payload["run_token"]):
        return False
    status_hash = payload.get("raw_sha256")
    return _nonempty_text(status_hash) and status_hash == raw_hash


def _status_auth_key(*, create: bool = False) -> bytes:
    """Local producer authority, separate from caller-controlled artifacts.

    Like the trusted producer source, this private key must be protected from
    modification by the artifact author. This is a local same-user boundary,
    not protection against code in the measured child, which shares the
    producer's OS principal and can read the key. Only the runner creates the
    key; receipt issuance can only read it.
    """
    namespace = hashlib.sha256(str(Path(__file__).resolve()).encode()).hexdigest()[:24]
    directory = Path("/tmp") / f"landing-measurement-{os.getuid()}-{namespace}"
    if create:
        directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise OSError("unsafe producer authority directory")
    key_path = directory / "status.key"
    if create:
        try:
            fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "wb") as handle:
                handle.write(secrets.token_bytes(32))
    fd = os.open(key_path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise OSError("unsafe producer authority key")
        key = handle.read(33)
    if len(key) != 32:
        raise OSError("invalid producer authority key")
    return key


def _status_auth_bytes(payload: Mapping[str, Any]) -> bytes:
    unsigned = {name: value for name, value in payload.items() if name != "producer_auth"}
    return json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()


def _status_authenticates(payload: Mapping[str, Any]) -> bool:
    signature = payload.get("producer_auth")
    if not isinstance(signature, str):
        return False
    try:
        expected = hmac.digest(_status_auth_key(), _status_auth_bytes(payload), "sha256").hex()
        return hmac.compare_digest(signature, expected)
    except (OSError, TypeError, ValueError):
        return False


def _manifest_auth_bytes(payload: Mapping[str, Any]) -> bytes:
    unsigned = {name: value for name, value in payload.items() if name != "manifest_auth"}
    return b"landing-measurement-receipt\0" + json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()


def _manifest_authenticates(payload: Mapping[str, Any]) -> bool:
    signature = payload.get("manifest_auth")
    if not isinstance(signature, str):
        return False
    try:
        expected = hmac.digest(_status_auth_key(), _manifest_auth_bytes(payload), "sha256").hex()
        return hmac.compare_digest(signature, expected)
    except (OSError, TypeError, ValueError):
        return False


def _resolved_manifest_path(value: Any) -> str | None:
    if not _nonempty_path(value):
        return None
    try:
        return str(Path(value).resolve())
    except OSError:
        return None


def _status_binds_scope(
    payload: Mapping[str, Any] | None, scope_hash: str | None, run_id: str | None, returncode: Any
) -> bool:
    return (
        payload is not None
        and _nonempty_text(scope_hash)
        and payload.get("scope_sha256") == scope_hash
        and payload.get("run_id") == run_id
        and payload.get("returncode") == returncode
    )


def write_completion_receipt(
    path: Path | str,
    *,
    status: str,
    pytest_returncode: int | None,
    raw: Path | str | None,
    ids: Path | str | None = None,
    nids: Path | str | None = None,
    scope: Path | str | None = None,
    fingerprint: str,
    timeout_seconds: int,
    error: str | None = None,
    run_id: str | None = None,
    producer_status: Mapping[str, Any] | None = None,
    producer_script: Path | str | None = None,
    producer_wrapper: Path | str | None = None,
    trusted_fingerprint: str | None = None,
) -> Path:
    destination = Path(path)
    raw_hash = sha256_file(raw)
    if raw is not None and raw_hash is None:
        error = "raw_artifact_unavailable"
    ids_hash = sha256_file(ids)
    nids_hash = sha256_file(nids) if nids is not None else None
    scope_hash = sha256_file(scope)
    if status == RECEIPT_COMPLETE:
        if not _producer_status_is_complete(producer_status, raw_hash, raw):
            status = "incomplete"
        if not _status_binds_scope(producer_status, scope_hash, run_id, pytest_returncode):
            status = "incomplete"
            error = "producer_scope_mismatched_or_unbound"
        if not _is_measurement_returncode(pytest_returncode):
            status = "incomplete"
        if not raw_hash or not ids_hash or not nids_hash or not scope_hash:
            status = "incomplete"
        if not _nonempty_path(producer_script) or not _nonempty_path(producer_wrapper):
            status = "incomplete"
        if not _nonempty_text(fingerprint):
            status = "incomplete"
        if not _nonempty_text(run_id):
            status = "incomplete"
        if producer_status is not None and producer_status.get("run_id") not in (None, run_id):
            status = "incomplete"
        if trusted_fingerprint is not None and fingerprint != trusted_fingerprint:
            status = "incomplete"
    if status != RECEIPT_COMPLETE:
        ids_hash = None
        nids_hash = None
        scope_hash = None
    payload: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "status": status,
        "pytest_returncode": pytest_returncode if _is_terminal_returncode(pytest_returncode) else None,
        "raw_sha256": raw_hash,
        "ids_sha256": ids_hash,
        "nids_sha256": nids_hash,
        "scope_sha256": scope_hash,
        "producer_fingerprint": fingerprint,
        "compat": COMPAT_VERSION,
        "producer_compat": COMPAT_VERSION,
        "producer": BOUNDED_PYTEST_PRODUCER,
        "producer_script": _resolved_manifest_path(producer_script),
        "producer_wrapper": _resolved_manifest_path(producer_wrapper),
        "producer_run_token": (
            producer_status.get("run_token")
            if isinstance(producer_status, Mapping) and _nonempty_text(producer_status.get("run_token"))
            else None
        ),
        "pytest_env_removed": list(producer_status.get("pytest_env_removed", [])) if producer_status else [],
        # Absent or null is unknown (never recorded, or not isolated); it must not read as "nothing ignored".
        "ignored_python_env": (
            list(producer_status["ignored_python_env"])
            if isinstance(producer_status, Mapping) and isinstance(producer_status.get("ignored_python_env"), list)
            else None
        ),
        "pytest_runtime": producer_status.get("pytest_runtime") if producer_status else None,
        "pytest_session": producer_status.get("pytest_session") if producer_status else None,
        "timeout_seconds": int(timeout_seconds),
        "run_id": run_id if isinstance(run_id, str) and run_id.strip() else None,
        "artifacts": {
            "raw": str(Path(raw).resolve()) if raw is not None else None,
            "ids": str(Path(ids).resolve()) if ids is not None else None,
            "nids": str(Path(nids).resolve()) if nids is not None else None,
            "scope": str(Path(scope).resolve()) if scope is not None else None,
        },
    }
    if error:
        payload["error"] = error
    if status == RECEIPT_COMPLETE:
        # Issuance already validated authenticated runner status. Authenticate
        # the entire final manifest too; public source fingerprints are identity,
        # not proof that these particular artifacts were produced by a run.
        payload["manifest_auth"] = hmac.digest(_status_auth_key(), _manifest_auth_bytes(payload), "sha256").hex()
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


def load_receipt(path: Path | str) -> dict[str, Any] | None:
    candidate = Path(path)
    try:
        text = read_file_snapshot(candidate)["text"]
    except OSError:
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _current_attempt_bind(
    raw_path: Path,
    receipt_run_id: str,
    *,
    attempt_payload: Mapping[str, Any] | None = None,
    allow_reread: bool = True,
) -> bool:
    """Refuse a complete receipt that is not the currently published attempt.

    A missing attempt marker is fail-closed: a completion receipt without the
    producer's current-attempt publication cannot distinguish a prior success
    from a new crash after lock setup [LANDGA-S1C-H02].
    """
    payload: Mapping[str, Any] | None = attempt_payload
    if payload is None:
        if not allow_reread:
            return False
        marker = attempt_path_for_raw(raw_path)
        if not marker.is_file():
            return False
        payload = load_attempt(marker)
        if payload is None:
            return False
    marker_run = attempt_run_id_from_payload(payload)
    if marker_run is None:
        return False
    return marker_run == receipt_run_id.strip() and payload.get("status") == RECEIPT_COMPLETE


def _receipt_has_trusted_producer(receipt: Mapping[str, Any], trusted_fingerprint: str | None) -> bool:
    if receipt.get("schema") != RECEIPT_SCHEMA or receipt.get("status") != RECEIPT_COMPLETE:
        return False
    if not _is_measurement_returncode(receipt.get("pytest_returncode")):
        return False
    if receipt.get("compat") != COMPAT_VERSION or receipt.get("producer_compat") != COMPAT_VERSION:
        return False
    if receipt.get("producer") != BOUNDED_PYTEST_PRODUCER:
        return False
    fingerprint = receipt.get("producer_fingerprint")
    producer_script = receipt.get("producer_script")
    producer_wrapper = receipt.get("producer_wrapper")
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        return False
    if not _nonempty_path(producer_script) or not _nonempty_path(producer_wrapper):
        return False
    if not _nonempty_text(receipt.get("producer_run_token")):
        return False
    try:
        expected = (
            trusted_fingerprint
            if trusted_fingerprint is not None
            else trusted_producer_fingerprint(producer_script, wrapper=producer_wrapper)
        )
    except (OSError, ValueError):
        return False
    return fingerprint.strip() == str(expected).strip()


def _receipt_binds_current_attempt(
    raw_path: Path,
    receipt: Mapping[str, Any],
    *,
    allow_reread: bool,
    attempt_payload: Mapping[str, Any] | None,
) -> bool:
    run_id = receipt.get("run_id")
    return (
        isinstance(run_id, str)
        and bool(run_id.strip())
        and _current_attempt_bind(
            raw_path,
            run_id,
            attempt_payload=attempt_payload,
            allow_reread=allow_reread,
        )
    )


def _receipt_manifest_matches(
    raw_path: Path,
    receipt: Mapping[str, Any],
    *,
    ids_path: Path | str | None,
    scope_path: Path | str | None,
) -> bool:
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return False
    expected_paths = {
        "raw": str(Path(raw_path).resolve()),
        "nids": _resolved_manifest_path(ids_path),
        "scope": _resolved_manifest_path(scope_path),
    }
    if expected_paths["nids"] is None or expected_paths["scope"] is None:
        return False
    if any(artifacts.get(name) != expected_path for name, expected_path in expected_paths.items()):
        return False
    return _nonempty_text(artifacts.get("ids"))


def _receipt_artifact_hashes_match(
    raw_path: Path,
    receipt: Mapping[str, Any],
    *,
    ids_path: Path | str | None,
    scope_path: Path | str | None,
    raw_sha256: str | None,
    ids_sha256: str | None,
    scope_sha256: str | None,
    allow_reread: bool,
) -> bool:
    ids_hash = receipt.get("ids_sha256")
    nids_hash = receipt.get("nids_sha256")
    scope_hash = receipt.get("scope_sha256")
    if not all(isinstance(value, str) and value for value in (ids_hash, nids_hash, scope_hash)):
        return False
    observed_raw = raw_sha256 if raw_sha256 is not None else (sha256_file(raw_path) if allow_reread else None)
    observed_ids = ids_sha256 if ids_sha256 is not None else (sha256_file(ids_path) if allow_reread else None)
    observed_scope = scope_sha256 if scope_sha256 is not None else (sha256_file(scope_path) if allow_reread else None)
    return (
        receipt.get("raw_sha256") == observed_raw
        and ids_path is not None
        and observed_ids == nids_hash
        and scope_path is not None
        and observed_scope == scope_hash
    )


def receipt_authenticates_raw(
    raw_path: Path,
    receipt: Mapping[str, Any] | None,
    *,
    ids_path: Path | str | None = None,
    scope_path: Path | str | None = None,
    trusted_fingerprint: str | None = None,
    raw_sha256: str | None = None,
    ids_sha256: str | None = None,
    scope_sha256: str | None = None,
    allow_reread: bool = True,
    attempt_payload: Mapping[str, Any] | None = None,
) -> bool:
    """Authenticate the exact caller-supplied artifact paths as one manifest.

    Sibling filenames derived from ``raw_path`` are not consulted. A complete
    receipt must attest a returned run, an integer pytest return code, required
    compatibility/producer/run identity, and hashes for raw, IDs, and scope.
    When ``nids_sha256`` is present it is the consumed canonical ID bytes
    (``.nids``), not forensic raw ``.ids`` [LANDGA-S1C-M03]. Callers that
    already hashed an immutable snapshot may pass those digests instead of
    re-reading the files [LANDGA-S1C-M06]. A published current-attempt marker
    must bind the receipt run id [LANDGA-S1C-H02].
    """
    if receipt is None or not _manifest_authenticates(receipt):
        return False
    if not _receipt_has_trusted_producer(receipt, trusted_fingerprint):
        return False
    if not _receipt_binds_current_attempt(
        raw_path,
        receipt,
        allow_reread=allow_reread,
        attempt_payload=attempt_payload,
    ):
        return False
    if not _receipt_manifest_matches(raw_path, receipt, ids_path=ids_path, scope_path=scope_path):
        return False
    return _receipt_artifact_hashes_match(
        raw_path,
        receipt,
        ids_path=ids_path,
        scope_path=scope_path,
        raw_sha256=raw_sha256,
        ids_sha256=ids_sha256,
        scope_sha256=scope_sha256,
        allow_reread=allow_reread,
    )


def _measurement_snapshots_for_label(
    label: str,
    raw_path: Path,
    *,
    ids_path: Path | None,
    scope_path: Path | None,
    supplied: Mapping[str, Any] | None,
    error_cls: type[Exception],
) -> dict[str, Any]:
    if supplied is not None:
        raw_snapshot = supplied.get("raw")
        if not isinstance(raw_snapshot, dict) or not raw_snapshot.get("sha256"):
            raise error_cls(f"could not read the {label} pytest log at {raw_path}: snapshot missing")
        ids_snapshot = supplied.get("ids") if isinstance(supplied.get("ids"), dict) else None
        scope_snapshot = supplied.get("scope") if isinstance(supplied.get("scope"), dict) else None
        attempt_snapshot = supplied.get("attempt") if isinstance(supplied.get("attempt"), dict) else None
        receipt_snapshot = supplied.get("receipt") if isinstance(supplied.get("receipt"), dict) else None
    else:
        try:
            raw_snapshot = read_file_snapshot(raw_path)
        except OSError as exc:
            raise error_cls(f"could not read the {label} pytest log at {raw_path}: {exc}") from exc
        ids_snapshot = try_file_snapshot(ids_path)
        scope_snapshot = try_file_snapshot(scope_path)
        attempt_snapshot = try_file_snapshot(attempt_path_for_raw(raw_path))
        receipt_snapshot = try_file_snapshot(receipt_path_for_raw(raw_path))
    return {
        "raw_hash": raw_snapshot.get("sha256"),
        "ids_hash": ids_snapshot.get("sha256") if ids_snapshot else None,
        "scope_hash": scope_snapshot.get("sha256") if scope_snapshot else None,
        "receipt": _json_payload_from_snapshot(receipt_snapshot),
        "attempt": _attempt_payload_from_snapshot(attempt_snapshot),
    }


def _measurement_label_is_complete(
    label: str,
    raw_path: Path,
    *,
    ids_path: Path | None,
    scope_path: Path | None,
    supplied: Mapping[str, Any] | None,
    error_cls: type[Exception],
) -> bool:
    artifacts = _measurement_snapshots_for_label(
        label,
        raw_path,
        ids_path=ids_path,
        scope_path=scope_path,
        supplied=supplied,
        error_cls=error_cls,
    )
    receipt = artifacts["receipt"]
    attempt = artifacts["attempt"]
    if receipt is None or attempt is None:
        return False
    return receipt_authenticates_raw(
        raw_path,
        receipt,
        ids_path=ids_path,
        scope_path=scope_path,
        raw_sha256=artifacts["raw_hash"] if isinstance(artifacts["raw_hash"], str) else None,
        ids_sha256=artifacts["ids_hash"] if isinstance(artifacts["ids_hash"], str) else None,
        scope_sha256=artifacts["scope_hash"] if isinstance(artifacts["scope_hash"], str) else None,
        allow_reread=False,
        attempt_payload=attempt,
    )


def measurement_completeness(
    raw_logs: Mapping[str, Path | None],
    *,
    ids_logs: Mapping[str, Path | None] | None = None,
    scope_logs: Mapping[str, Path | None] | None = None,
    error_cls: type[Exception] = RuntimeError,
    artifact_snapshots: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Completeness requires a producer-owned receipt bound to the supplied artifacts.

    A terminal pytest summary, wrapped or bare, is not enough. IDs and scope
    are authenticated at the caller-supplied paths, never as inferred siblings.
    When ``artifact_snapshots`` is supplied, hashes come from that immutable
    read rather than a second open of the same paths [LANDGA-S1C-M06].
    """
    ids_logs = ids_logs or {}
    scope_logs = scope_logs or {}
    provided = {label: path for label, path in sorted(raw_logs.items()) if path is not None}
    if not provided:
        return {"outcome": "not_measured", "incomplete": [], "checked": []}
    incomplete: list[str] = []
    for label, path in sorted(raw_logs.items()):
        if path is None or not _measurement_label_is_complete(
            label,
            Path(path),
            ids_path=ids_logs.get(label),
            scope_path=scope_logs.get(label),
            supplied=artifact_snapshots.get(label) if artifact_snapshots else None,
            error_cls=error_cls,
        ):
            incomplete.append(label)
    return {
        "outcome": "incomplete" if incomplete else "complete",
        "incomplete": incomplete,
        "checked": sorted(str(item) for item in provided),
    }


def _processes_with_marker(marker: str) -> set[int]:
    """Find only descendants carrying this run's inherited environment marker."""
    needle = f"{RUN_MARKER_ENV}={marker}".encode()
    found: set[int] = set()
    proc_root = Path("/proc")
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return found
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            if needle in (entry / "environ").read_bytes().split(b"\0"):
                found.add(int(entry.name))
        except (OSError, ValueError):
            continue
    return found


def _descendants_from_process_table(
    root_pid: int,
    *,
    signals: list[str] | None = None,
    attempts: int = PROCESS_TABLE_SNAPSHOT_ATTEMPTS,
) -> set[int]:
    """Return the transitive descendants from a bounded portable ``ps`` snapshot."""
    # The pre-kill snapshot is the only reading that can retain escaped
    # descendants, so retry a bounded number of times on a busy host. Each
    # attempt still has its own finite timeout; a degraded snapshot must not
    # turn the reaper's timeout into a hang.
    for _ in range(attempts):
        try:
            snapshot = subprocess.run(
                ["ps", "-A", "-o", "pid=,ppid="],
                capture_output=True,
                text=True,
                check=False,
                timeout=PROCESS_TABLE_SNAPSHOT_TIMEOUT_SEC,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if snapshot.returncode != 0 or not isinstance(snapshot.stdout, str):
            continue

        children: dict[int, set[int]] = {}
        for line in snapshot.stdout.splitlines():
            fields = line.split()
            if len(fields) != 2:
                continue
            try:
                pid, ppid = (int(field) for field in fields)
            except ValueError:
                continue
            if pid <= 0 or ppid < 0:
                continue
            children.setdefault(ppid, set()).add(pid)

        descendants: set[int] = set()
        pending = [root_pid]
        current_pid = os.getpid()
        while pending:
            parent = pending.pop()
            for child in children.get(parent, set()):
                if child in descendants or child == root_pid or child == current_pid:
                    continue
                descendants.add(child)
                pending.append(child)
        return descendants

    if signals is not None and DESCENDANT_SNAPSHOT_UNAVAILABLE not in signals:
        signals.append(DESCENDANT_SNAPSHOT_UNAVAILABLE)
    return set()


def _kill_process_group(proc: subprocess.Popen[Any], *, marker: str | None = None) -> list[str]:
    signals: list[str] = []
    fallback_enabled = False
    fallback_snapshot: set[int] = set()
    fallback_pending: set[int] = set()
    fallback_signalled: set[int] = set()
    # The fallback retains IDs only for this bounded 60-iteration (~3-second)
    # cleanup window. Once SIGKILL succeeds or the target is gone, it is marked
    # handled and never signalled again, so a PID reused within that window is
    # not re-killed; every pending ID came from a descendant snapshot.
    if marker is not None:
        # A non-empty marker scan is the precise Linux fast path. An empty scan
        # means either that /proc is unavailable or that this run has no
        # marker-visible target, so the process-table fallback is safe and has
        # the same descendant-only outcome.
        fallback_enabled = not _processes_with_marker(marker)
        if fallback_enabled:
            # This must precede killpg: detached grandchildren become children
            # of pid 1 as soon as their direct parent is killed.
            # Retain this initial closure because after killpg the direct child
            # is dead and ps no longer has a parent chain back to proc.pid; a
            # later fork by an already-detached descendant is therefore a known
            # limit of ps-based tracking without /proc, not an oversight.
            fallback_snapshot = _descendants_from_process_table(proc.pid, signals=signals)
            fallback_pending.update(fallback_snapshot)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except OSError:
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
    if marker is None:
        return signals
    # A child can call setsid(2) and escape the process group. Repeatedly scan
    # for this run's private marker until every matching process is gone; the
    # marker prevents collateral kills of unrelated work [LANDGA-L0BR-M03].
    for _ in range(60):
        pids = _processes_with_marker(marker)
        if fallback_enabled:
            # Follow-up scans retain the original one-attempt bound; only the
            # irreplaceable pre-kill closure receives the bounded retry budget.
            fresh_snapshot = _descendants_from_process_table(proc.pid, signals=signals, attempts=1)
            fallback_snapshot.update(fresh_snapshot)
            fallback_pending.update(pid for pid in fresh_snapshot if pid not in fallback_signalled)
            pids.update(fallback_pending)
        if not pids:
            return signals
        for pid in pids:
            if pid == os.getpid():
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                if fallback_enabled and pid in fallback_pending:
                    fallback_pending.discard(pid)
                    fallback_signalled.add(pid)
                continue
            except (PermissionError, OSError):
                continue
            if fallback_enabled and pid in fallback_pending:
                fallback_pending.discard(pid)
                fallback_signalled.add(pid)
        time.sleep(0.05)
    return signals


def _bounded_pytest_status(
    *,
    outcome: str,
    timed_out: bool,
    returncode: int | None,
    error: str | None,
    raw: Path,
    pytest_attested: bool,
    run_id: str | None,
    run_token: str,
    argv_sha256: str,
    signals: Sequence[str] = (),
    pytest_env_removed: Sequence[str] = (),
    ignored_python_env: Sequence[str] | None = None,
    pytest_session_proof_size: int | None = None,
    pytest_session_proof_limit: int | None = None,
) -> dict[str, Any]:
    status = {
        "outcome": outcome,
        "timed_out": timed_out,
        "returncode": returncode,
        "error": error,
        "raw_sha256": sha256_file(raw),
        "producer": BOUNDED_PYTEST_PRODUCER,
        "pytest_attested": pytest_attested,
        "run_id": run_id,
        "run_token": run_token,
        "argv_sha256": argv_sha256,
        "signals": list(signals),
        "pytest_env_removed": list(pytest_env_removed),
        "ignored_python_env": None if ignored_python_env is None else sorted(ignored_python_env),
    }
    if pytest_session_proof_size is not None and pytest_session_proof_limit is not None:
        status["pytest_session_proof_size"] = pytest_session_proof_size
        status["pytest_session_proof_limit"] = pytest_session_proof_limit
    return status


def _pytest_command_matches_current_interpreter(argv: Sequence[str]) -> bool:
    if len(argv) < 3 or str(argv[1]) != "-m" or str(argv[2]) != "pytest":
        return False
    try:
        return Path(str(argv[0])).resolve() == Path(os.path.realpath(__import__("sys").executable)).resolve()
    except OSError:
        return False


def _pytest_scope_refusal(
    argv: Sequence[str], scope_path: Path | None, effective_env: Mapping[str, str] | None = None
) -> str | None:
    for name in PYTEST_OPTION_ENV_VARS:
        if name in (os.environ if effective_env is None else effective_env):
            return f"pytest_env_unbound:{name}"
    if scope_path is None:
        return "pytest_scope_required"
    try:
        scope = json.loads(read_file_snapshot(scope_path)["text"])
    except (OSError, ValueError):
        return "pytest_scope_unavailable"
    return _pytest_scope_payload_refusal(argv, scope)


def _pytest_scope_payload_refusal(argv: Sequence[str], scope: Any, cwd: Path | str | None = None) -> str | None:
    if not isinstance(scope, dict):
        return "pytest_scope_malformed"
    targets = scope.get("pytest_targets")
    options = scope.get("pytest_options")
    if not isinstance(targets, list) or not all(isinstance(item, str) for item in targets):
        return "pytest_scope_malformed"
    if not isinstance(options, list) or not all(isinstance(item, str) for item in options):
        return "pytest_scope_malformed"
    if cwd is not None and "worktree" in scope:
        if _resolved_manifest_path(scope["worktree"]) != str(Path(cwd).resolve()):
            return "pytest_worktree_mismatched"
    if "interpreter" in scope:
        if _resolved_manifest_path(scope["interpreter"]) != str(Path(str(argv[0])).resolve()):
            return "pytest_interpreter_mismatched"
    return None if list(argv[3:]) == [*targets, *options] else "pytest_targets_mismatched"


def _pytest_runtime_available(
    argv: Sequence[str], *, cwd: Path | str | None, env: Mapping[str, str], timeout_seconds: int
) -> tuple[dict[str, str] | None, str | None]:
    """Resolve installed pytest with cwd and PYTHONPATH excluded from imports."""
    try:
        probe = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                "import json, pathlib, pytest, _pytest; print(json.dumps({"
                "'pytest': str(pathlib.Path(pytest.__file__).resolve()), "
                "'_pytest': str(pathlib.Path(_pytest.__file__).resolve())}))",
            ],
            cwd=str(cwd) if cwd is not None else None,
            env=dict(env),
            capture_output=True,
            text=True,
            check=False,
            timeout=min(10, max(1, timeout_seconds)),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"pytest runtime probe failed: {type(exc).__name__}: {exc}"
    if probe.returncode != 0:
        detail = (probe.stderr or probe.stdout).strip()[:400]
        return None, f"pytest runtime probe exited {probe.returncode}: {detail}"
    try:
        runtime = json.loads(probe.stdout)
    except ValueError:
        return None, "pytest_runtime_origin_unavailable"
    if not isinstance(runtime, dict) or not all(_nonempty_path(runtime.get(name)) for name in ("pytest", "_pytest")):
        return None, "pytest_runtime_origin_unavailable"
    return runtime, None


def _read_session_proof(handle: BinaryIO) -> Any:
    handle.seek(0)
    try:
        return json.loads(handle.read(_SESSION_PROOF_LIMIT + 1))
    except (ValueError, UnicodeError, RecursionError):
        # Deeply nested bytes from a killed child exceed the parser's depth limit; treat them as no proof.
        return None


def _timeout_python_env(ignored: list[str], session_proof: Any) -> list[str] | None:
    """Classify a killed child's PYTHONWARNINGS status; only its own bounded proof can settle it."""
    unresolved = session_proof.get("python_warnings_unresolved") if isinstance(session_proof, dict) else None
    if not isinstance(unresolved, list):
        # Session finish never wrote a proof, so whether the warnings applied is unobserved, not honored.
        return None
    if not unresolved:
        return [name for name in ignored if name != "PYTHONWARNINGS"]
    # An entry that failed to apply stays named as ignored, as on the completed path.
    return ignored


def run_bounded_pytest(
    argv: Sequence[str],
    *,
    cwd: Path | str | None,
    raw_path: Path | str,
    timeout_seconds: int,
    env: Mapping[str, str] | None = None,
    run_id: str | None = None,
    scope_path: Path | str | None = None,
) -> dict[str, Any]:
    """Run argv, capturing stdout/stderr into ``raw_path``, with a finite wait.

    Outcomes are ``returned``, ``signaled``, ``timeout``, or ``unavailable``.
    An optional pre-run scope is required for later completion issuance.
    """
    raw = Path(raw_path)
    run_token = secrets.token_hex(16)
    command = [str(item) for item in argv]
    argv_sha256 = hashlib.sha256(json.dumps(command, separators=(",", ":")).encode("utf-8")).hexdigest()
    pytest_attested = _pytest_command_matches_current_interpreter(command)
    caller_env = env if env is not None else os.environ
    child_env, removed_env = _bound_pytest_environment(caller_env)
    python_flags, isolated_python_env = _python_interpreter_flags(caller_env)
    # Only the -I bootstrap child ignores PYTHON* variables. Any other child honours them,
    # so its status records None (unknown) unless the isolated child actually started.
    ignored_python_env: list[str] | None = None
    python_warnings = _python_warning_options(caller_env)
    child_env[RUN_MARKER_ENV] = run_token
    scope_snapshot = try_file_snapshot(scope_path)
    scope_hash = scope_snapshot.get("sha256") if scope_snapshot else None
    scope_refusal = None
    if scope_path is not None:
        scope_refusal = (
            _pytest_scope_payload_refusal(
                command, _json_payload_from_snapshot(scope_snapshot), cwd if cwd is not None else Path.cwd()
            )
            if pytest_attested
            else "pytest_invocation_required"
        )
    try:
        raw.parent.mkdir(parents=True, exist_ok=True)
        with raw.open("wb") as handle, tempfile.TemporaryFile() as proof_handle:
            if scope_refusal:
                return _bounded_pytest_status(
                    outcome="unavailable",
                    timed_out=False,
                    returncode=None,
                    error=scope_refusal,
                    raw=raw,
                    pytest_attested=False,
                    run_id=run_id,
                    run_token=run_token,
                    argv_sha256=argv_sha256,
                    pytest_env_removed=removed_env,
                    ignored_python_env=ignored_python_env,
                )
            runtime_error = None
            runtime = None
            if pytest_attested:
                runtime, runtime_error = _pytest_runtime_available(
                    command,
                    cwd=cwd,
                    env=child_env,
                    timeout_seconds=timeout_seconds,
                )
                if runtime is None:
                    return _bounded_pytest_status(
                        outcome="unavailable",
                        timed_out=False,
                        returncode=None,
                        error=runtime_error,
                        raw=raw,
                        pytest_attested=False,
                        run_id=run_id,
                        run_token=run_token,
                        argv_sha256=argv_sha256,
                        pytest_env_removed=removed_env,
                        ignored_python_env=ignored_python_env,
                    )
                command = [
                    sys.executable,
                    *python_flags,
                    "-I",
                    "-c",
                    _PYTEST_BOOTSTRAP,
                    json.dumps(runtime),
                    str(Path(__file__).resolve()),
                    json.dumps(python_warnings),
                    *command[3:],
                ]
                child_env[_PYTEST_PROOF_FD_ENV] = str(proof_handle.fileno())
            try:
                proc = subprocess.Popen(
                    command,
                    cwd=str(cwd) if cwd is not None else None,
                    env=child_env,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    pass_fds=(proof_handle.fileno(),) if pytest_attested else (),
                )
            except OSError as exc:
                return _bounded_pytest_status(
                    outcome="unavailable",
                    timed_out=False,
                    returncode=None,
                    error=f"{type(exc).__name__}: {exc}",
                    raw=raw,
                    pytest_attested=pytest_attested,
                    run_id=run_id,
                    run_token=run_token,
                    argv_sha256=argv_sha256,
                    pytest_env_removed=removed_env,
                    ignored_python_env=ignored_python_env,
                )
            if pytest_attested:
                ignored_python_env = isolated_python_env
            try:
                returncode = proc.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                cleanup_signals = _kill_process_group(proc, marker=run_token)
                if ignored_python_env is not None and "PYTHONWARNINGS" in ignored_python_env:
                    # Session finish usually never ran, so only a proof the killed child did write can settle it.
                    ignored_python_env = _timeout_python_env(ignored_python_env, _read_session_proof(proof_handle))
                return _bounded_pytest_status(
                    outcome="timeout",
                    timed_out=True,
                    returncode=None,
                    error=None,
                    raw=raw,
                    pytest_attested=pytest_attested,
                    run_id=run_id,
                    run_token=run_token,
                    argv_sha256=argv_sha256,
                    pytest_env_removed=removed_env,
                    ignored_python_env=ignored_python_env,
                    signals=cleanup_signals,
                )
            outcome = "signaled" if isinstance(returncode, int) and returncode < 0 else "returned"
            session_proof = _read_session_proof(proof_handle)
            if (
                isinstance(session_proof, dict)
                and session_proof.get("python_warnings_unresolved") == []
                and ignored_python_env is not None
            ):
                # The measured child applied every PYTHONWARNINGS entry.
                ignored_python_env = [name for name in ignored_python_env if name != "PYTHONWARNINGS"]
            execution_error = None
            proof_size = None
            proof_limit = None
            oversized_proof = (
                isinstance(session_proof, dict) and session_proof.get("error") == "pytest_session_proof_too_large"
            )
            if oversized_proof:
                proof_size = session_proof.get("size")
                proof_limit = session_proof.get("limit")
                oversized_proof = (
                    type(proof_size) is int
                    and type(proof_limit) is int
                    and proof_size > proof_limit
                    and proof_limit == _SESSION_PROOF_LIMIT
                )
            if pytest_attested and outcome == "returned":
                if oversized_proof:
                    pytest_attested = False
                    execution_error = "pytest_session_proof_too_large"
                elif not _session_proof_is_complete(session_proof, run_token, runtime) or not _raw_has_session_markers(
                    raw, run_token
                ):
                    pytest_attested = False
                    execution_error = "pytest_session_not_executed_or_incomplete"
                    if isinstance(session_proof, dict) and not _pytest_configuration_is_bound(session_proof):
                        execution_error = "pytest_configuration_unbound"
            result = _bounded_pytest_status(
                outcome=outcome,
                timed_out=False,
                returncode=returncode,
                error=execution_error,
                raw=raw,
                pytest_attested=pytest_attested,
                run_id=run_id,
                run_token=run_token,
                argv_sha256=argv_sha256,
                pytest_env_removed=removed_env,
                ignored_python_env=ignored_python_env,
                pytest_session_proof_size=proof_size if oversized_proof else None,
                pytest_session_proof_limit=proof_limit if oversized_proof else None,
            )
            result["pytest_runtime"] = runtime
            result["pytest_session"] = session_proof
            result["scope_sha256"] = scope_hash
            result["producer_auth"] = hmac.digest(
                _status_auth_key(create=True), _status_auth_bytes(result), "sha256"
            ).hex()
            return result
    except OSError as exc:
        return _bounded_pytest_status(
            outcome="unavailable",
            timed_out=False,
            returncode=None,
            error=f"{type(exc).__name__}: {exc}",
            raw=raw,
            pytest_attested=pytest_attested,
            run_id=run_id,
            run_token=run_token,
            argv_sha256=argv_sha256,
            pytest_env_removed=removed_env,
            ignored_python_env=ignored_python_env,
        )


def write_status(path: Path | str, payload: Mapping[str, Any]) -> None:
    Path(path).write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_timeout(value: str) -> int:
    try:
        timeout = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a positive integer") from exc
    if timeout <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive integer")
    return timeout


def _run_pytest_cli(args: argparse.Namespace) -> int:
    refusal = None
    if not _pytest_command_matches_current_interpreter(args.command):
        refusal = "--run-pytest requires the current interpreter followed by -m pytest"
    else:
        refusal = _pytest_scope_refusal(args.command, args.scope)
    if refusal is not None:
        raw = Path(args.raw)
        try:
            raw.parent.mkdir(parents=True, exist_ok=True)
            raw.touch()
        except OSError:
            pass
        result = _bounded_pytest_status(
            outcome="unavailable",
            timed_out=False,
            returncode=None,
            error=refusal,
            raw=raw,
            pytest_attested=False,
            run_id=args.run_id,
            run_token=secrets.token_hex(16),
            argv_sha256=hashlib.sha256(
                json.dumps([str(item) for item in args.command], separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        )
        if args.status is not None:
            write_status(args.status, result)
        return UNAVAILABLE_EXIT
    result = run_bounded_pytest(
        args.command,
        cwd=args.cwd,
        raw_path=args.raw,
        timeout_seconds=args.timeout,
        run_id=args.run_id,
        scope_path=args.scope,
    )
    if args.status is not None:
        write_status(args.status, result)
    if result["outcome"] == "returned" and result.get("pytest_attested") is True:
        return 0
    if result["outcome"] == "timeout":
        return TIMEOUT_EXIT
    if result["outcome"] == "signaled":
        return SIGNAL_EXIT
    return UNAVAILABLE_EXIT


def _fingerprint_from_args(args: argparse.Namespace) -> str:
    wrapper = args.fingerprint_wrapper
    if args.fingerprint:
        return str(args.fingerprint)
    if args.fingerprint_script:
        return trusted_producer_fingerprint(args.fingerprint_script, wrapper=wrapper)
    if args.scope and Path(args.scope).is_file():
        try:
            payload = json.loads(read_file_snapshot(args.scope)["text"])
        except (OSError, ValueError):
            payload = {}
        if isinstance(payload, dict) and payload.get("producer_fingerprint"):
            return str(payload["producer_fingerprint"])
    return trusted_producer_fingerprint(wrapper=wrapper)


def _trusted_fingerprint_from_args(args: argparse.Namespace) -> str:
    wrapper = args.fingerprint_wrapper
    if args.fingerprint_script:
        return trusted_producer_fingerprint(args.fingerprint_script, wrapper=wrapper)
    return trusted_producer_fingerprint(wrapper=wrapper)


def _load_status_payload(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        loaded = json.loads(read_file_snapshot(path)["text"])
    except (OSError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _is_producer_status(payload: Mapping[str, Any]) -> bool:
    if payload.get("producer") != BOUNDED_PYTEST_PRODUCER:
        return False
    return payload.get("outcome") in {"returned", "timeout", "signaled", "unavailable"}


def _can_issue_complete(args: argparse.Namespace, payload: Mapping[str, Any]) -> bool:
    if args.raw is None or not _is_producer_status(payload):
        return False
    raw_hash = sha256_file(args.raw)
    if not _producer_status_is_complete(payload, raw_hash, args.raw):
        return False
    return _status_binds_scope(
        payload, sha256_file(args.scope), args.run_id or payload.get("run_id"), payload.get("returncode")
    )


def _write_receipt_cli(args: argparse.Namespace) -> int:
    status_payload = _load_status_payload(args.from_status)
    source_outcome = status_payload.get("outcome")
    requested_complete = args.receipt_status == RECEIPT_COMPLETE or source_outcome == "returned"
    error = status_payload.get("error")
    if source_outcome in {"timeout", "signaled", "unavailable"}:
        status = source_outcome
        pytest_returncode = None
        fingerprint = _fingerprint_from_args(args)
    elif requested_complete and _can_issue_complete(args, status_payload):
        status = RECEIPT_COMPLETE
        pytest_returncode = status_payload.get("returncode")
        fingerprint = _trusted_fingerprint_from_args(args)
    else:
        status = args.receipt_status or source_outcome or "incomplete"
        if status in {RECEIPT_COMPLETE, "returned"}:
            status = "incomplete"
            if requested_complete and not error:
                error = "complete receipt requires producer-issued bounded-pytest status"
        if status not in {RECEIPT_COMPLETE, "timeout", "incomplete", "unavailable"}:
            status = "incomplete"
        pytest_returncode = args.pytest_returncode
        if pytest_returncode is None and "returncode" in status_payload:
            pytest_returncode = status_payload.get("returncode")
        if status in {"timeout", "unavailable"}:
            pytest_returncode = None
        fingerprint = _fingerprint_from_args(args)
    run_id = args.run_id or status_payload.get("run_id")
    try:
        written = write_completion_receipt(
            args.write_receipt,
            status=status,
            pytest_returncode=pytest_returncode,
            raw=args.raw,
            ids=args.ids,
            nids=args.nids,
            scope=args.scope,
            fingerprint=fingerprint,
            timeout_seconds=args.timeout,
            error=error if isinstance(error, str) else None,
            run_id=str(run_id) if run_id else None,
            producer_status=status_payload,
            producer_script=args.fingerprint_script,
            producer_wrapper=args.fingerprint_wrapper,
            trusted_fingerprint=(_trusted_fingerprint_from_args(args) if status == RECEIPT_COMPLETE else None),
        )
    except OSError:
        return UNAVAILABLE_EXIT
    issued = load_receipt(written)
    if requested_complete and (not issued or issued.get("status") != RECEIPT_COMPLETE):
        return COMPLETE_REFUSED_EXIT
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-pytest", action="store_true")
    parser.add_argument("--write-receipt", type=Path)
    parser.add_argument("--raw", type=Path)
    parser.add_argument("--run-status", dest="status", type=Path)
    parser.add_argument("--from-status", type=Path)
    parser.add_argument("--ids", type=Path)
    parser.add_argument("--nids", type=Path)
    parser.add_argument("--scope", type=Path)
    parser.add_argument("--cwd", type=Path)
    parser.add_argument("--timeout", type=_parse_timeout, default=DEFAULT_TIMEOUT_SEC)
    parser.add_argument("--receipt-status", dest="receipt_status", type=str)
    parser.add_argument("--pytest-returncode", type=int, default=None)
    parser.add_argument("--run-id", dest="run_id", default=None)
    parser.add_argument("--fingerprint", default=None)
    parser.add_argument("--fingerprint-script", type=Path)
    parser.add_argument("--fingerprint-wrapper", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.run_pytest:
        command = list(args.command)
        if command and command[0] == "--":
            command = command[1:]
        if not command or args.raw is None:
            parser.error("--run-pytest requires --raw and a command after --")
        args.command = command
        return _run_pytest_cli(args)
    if args.write_receipt is not None:
        return _write_receipt_cli(args)
    parser.error("specify --run-pytest or --write-receipt")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
