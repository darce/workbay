"""Use-time package-origin checks for hooks."""

from __future__ import annotations

import importlib
import importlib.metadata as metadata
import importlib.util
import io
import json
import os
import shutil
import site
import subprocess
import sys
import time
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent
REPO_ROOT = next(parent for parent in HOOKS_DIR.parents if (parent / "scripts" / "hooks" / "_interp.py").is_file())


def _load_interp():
    spec = importlib.util.spec_from_file_location("test_interp_origin_guard_module", HOOKS_DIR / "_interp.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _point_fixture_venv_at(tmp_path: Path, checkout: Path, monkeypatch) -> Path:
    package_dir = checkout / "packages" / "workbay-origin-fixture" / "src" / "workbay_origin_fixture"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("VALUE = 'loaded'\n", encoding="utf-8")
    site_packages = tmp_path / "venv" / "lib" / "pythonX" / "site-packages"
    site_packages.mkdir(parents=True)
    (site_packages / "editable-workbay-origin.pth").write_text(str(package_dir.parent) + "\n", encoding="utf-8")
    monkeypatch.setattr(sys, "path", list(sys.path))
    site.addsitedir(str(site_packages))
    return package_dir / "__init__.py"


def test_sibling_worktree_package_origin_is_refused_loudly(tmp_path: Path, monkeypatch, capsys) -> None:
    interp = _load_interp()
    primary = tmp_path / "primary"
    sibling = tmp_path / "sibling"
    origin = _point_fixture_venv_at(tmp_path, sibling, monkeypatch)
    # The synthetic fixture checkout represents the checkout containing _interp.py.
    monkeypatch.setattr(interp, "_hook_checkout_anchors", lambda: (str(primary),))

    with pytest.raises(ImportError, match="hook_package_origin_untrusted"):
        interp.assert_workbay_origins_trusted("workbay_origin_fixture")

    error = capsys.readouterr().err
    assert "hook_package_origin_untrusted" in error
    assert str(origin) in error
    assert str(primary) in error

    marker = tmp_path / "package-imported"
    (origin.parent / "__init__.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\n", encoding="utf-8"
    )
    interp._install_workbay_origin_guard()
    try:
        with pytest.raises(ImportError, match="hook_package_origin_untrusted"):
            importlib.import_module("workbay_origin_fixture")
    finally:
        sys.meta_path.remove(interp._ORIGIN_FINDER)
        sys.modules.pop("workbay_origin_fixture", None)
    assert not marker.exists()


def test_primary_checkout_package_origin_is_accepted(tmp_path: Path, monkeypatch, capsys) -> None:
    interp = _load_interp()
    primary = tmp_path / "primary"
    _point_fixture_venv_at(tmp_path, primary, monkeypatch)
    # Keep this synthetic checkout as the explicit trust anchor for the fixture.
    monkeypatch.setattr(interp, "_hook_checkout_anchors", lambda: (str(primary),))

    interp.assert_workbay_origins_trusted("workbay_origin_fixture")
    assert capsys.readouterr().err == ""


def test_payload_interp_stays_identical_to_shared_hook_helper() -> None:
    payload_copy = (
        REPO_ROOT / "packages" / "workbay-system" / "workbay_system" / "payload" / "scripts" / "hooks" / "_interp.py"
    )
    assert payload_copy.read_bytes() == (HOOKS_DIR / "_interp.py").read_bytes()
    payload_test = payload_copy.with_name("test_interp_origin_guard.py")
    assert payload_test.read_bytes() == Path(__file__).resolve().read_bytes()
    payload_reinject = payload_copy.with_name("reinject-context.py")
    assert payload_reinject.read_bytes() == (HOOKS_DIR / "reinject-context.py").read_bytes()


def _load_interp_from(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_unrelated_py_argv_does_not_widen_scanned_imports(tmp_path: Path, monkeypatch) -> None:
    """An extra ``.py`` argument is not the launched hook (SEC-04)."""
    interp = _load_interp()
    entry = tmp_path / "hook_entry.py"
    entry.write_text("import os\n", encoding="utf-8")
    unrelated = tmp_path / "unrelated.py"
    unrelated.write_text("import workbay_protocol\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(entry), "--flag", str(unrelated)])

    assert interp._current_hook_import_names() == set()


def test_run_guard_scans_handler_not_later_py_args(tmp_path: Path, monkeypatch) -> None:
    """``_run_guard.py <handler>`` scans the handler, not every later ``.py``."""
    interp = _load_interp()
    guard = tmp_path / "_run_guard.py"
    guard.write_text("import sys\n", encoding="utf-8")
    handler = tmp_path / "handler.py"
    handler.write_text("import workbay_handoff_mcp\n", encoding="utf-8")
    unrelated = tmp_path / "notes.py"
    unrelated.write_text("import workbay_protocol\n", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [str(guard), "--fail-mode=closed", str(handler), str(unrelated)],
    )

    assert interp._current_hook_import_names() == {"workbay_handoff_mcp"}


def test_hook_from_nongit_cwd_trusts_its_own_checkout(tmp_path: Path) -> None:
    """A hook launched outside any checkout still trusts packages from its own tree.

    The trust anchor is the worktree that contains the running ``_interp.py``,
    not the process cwd (the reinject suite launches from a tmp workspace).
    """
    nongit = tmp_path / "nongit-cwd"
    nongit.mkdir()
    script = nongit / "launch_hook.py"
    script.write_text(
        "import sys\n"
        f"sys.path.insert(0, {str(HOOKS_DIR)!r})\n"
        "from _interp import ensure_deps_interpreter\n"
        "ensure_deps_interpreter()\n"
        "import workbay_protocol\n"
        "sys.stdout.write(workbay_protocol.__file__ + '\\n')\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(nongit),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "hook_package_origin_untrusted" not in result.stderr
    assert "hook_origin_anchor_unavailable" not in result.stderr
    assert str(REPO_ROOT) in result.stdout


def test_sibling_worktree_hijack_exits_nonzero(tmp_path: Path) -> None:
    """Hook code in checkout A must refuse a ``workbay_*`` origin in sibling checkout B."""
    primary = tmp_path / "checkout-a"
    primary.mkdir()
    git_env = os.environ.copy()
    git_env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    git_env["GIT_CONFIG_SYSTEM"] = "/dev/null"
    git_env["GIT_AUTHOR_NAME"] = "hookq"
    git_env["GIT_AUTHOR_EMAIL"] = "hookq@example.com"
    git_env["GIT_COMMITTER_NAME"] = "hookq"
    git_env["GIT_COMMITTER_EMAIL"] = "hookq@example.com"

    def git(*args: str, cwd: Path | None = None) -> None:
        command = ["git"]
        if cwd is not None:
            command.extend(["-C", str(cwd)])
        command.extend(args)
        subprocess.run(command, check=True, capture_output=True, text=True, env=git_env)

    git("init", str(primary))
    hooks = primary / "scripts" / "hooks"
    hooks.mkdir(parents=True)
    shutil.copyfile(HOOKS_DIR / "_interp.py", hooks / "_interp.py")
    hook = hooks / "hook_entry.py"
    hook.write_text(
        "from _interp import ensure_deps_interpreter\n"
        "ensure_deps_interpreter()\n"
        "import workbay_hijack_probe\n"
        "raise SystemExit(0)\n",
        encoding="utf-8",
    )
    (primary / "README").write_text("checkout-a\n", encoding="utf-8")
    git("add", ".", cwd=primary)
    git("-c", "user.email=hookq@example.com", "-c", "user.name=hookq", "commit", "-m", "init", cwd=primary)
    sibling = tmp_path / "checkout-b"
    git("worktree", "add", "--detach", str(sibling), cwd=primary)
    package_parent = sibling / "src"
    package_dir = package_parent / "workbay_hijack_probe"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("VALUE = 'hijack'\n", encoding="utf-8")
    proc_env = os.environ.copy()
    proc_env["PYTHONPATH"] = str(package_parent)
    # cwd is the sibling on purpose: a cwd anchor would trust this origin.
    result = subprocess.run(
        [sys.executable, str(hook)],
        cwd=str(sibling),
        env=proc_env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode != 0, result.stderr
    assert "hook_package_origin_untrusted" in result.stderr
    assert "workbay_hijack_probe" in result.stderr
    assert "hook_origin_anchor_unavailable" not in result.stderr


def test_missing_hook_anchor_fails_closed_with_typed_reason(tmp_path: Path, monkeypatch) -> None:
    """No git anchor and no non-editable prefix install fails closed (SECD-05, OBS-08)."""
    loose = tmp_path / "loose-hooks"
    loose.mkdir()
    shutil.copyfile(HOOKS_DIR / "_interp.py", loose / "_interp.py")
    interp = _load_interp_from(loose / "_interp.py", "loose_interp_anchor_unavailable")
    _point_fixture_venv_at(tmp_path, tmp_path / "outside-checkout", monkeypatch)
    with pytest.raises(ImportError, match="hook_origin_anchor_unavailable"):
        interp.assert_workbay_origins_trusted("workbay_origin_fixture")


def test_noneditable_prefix_install_outside_checkout_is_trusted(tmp_path: Path, monkeypatch) -> None:
    """A wheel under the interpreter prefix is trusted; an editable ``.pth`` redirect is not."""
    interp = _load_interp()
    prefix = tmp_path / "consumer-venv"
    site_packages = prefix / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    package_dir = site_packages / "workbay_consumer_wheel"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("VALUE = 'wheel'\n", encoding="utf-8")
    monkeypatch.setattr(sys, "prefix", str(prefix), raising=False)
    monkeypatch.setattr(site, "getsitepackages", lambda: [str(site_packages)])
    monkeypatch.setattr(sys, "path", list(sys.path))
    site.addsitedir(str(site_packages))

    interp.assert_workbay_origins_trusted("workbay_consumer_wheel")


def test_origin_probe_timeout_names_probe_timeout(monkeypatch) -> None:
    interp = _load_interp()

    def explode(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="python", timeout=interp._ORIGIN_PROBE_TIMEOUT_SECONDS)

    monkeypatch.setattr(interp.subprocess, "run", explode)
    with pytest.raises(ImportError, match="probe_timeout") as caught:
        interp.assert_workbay_origins_trusted("workbay_protocol", interpreter="/tmp/hookq-other-python")
    assert f"deadline_seconds={interp._ORIGIN_PROBE_TIMEOUT_SECONDS}" in str(caught.value)


@pytest.mark.parametrize(
    ("requested_timeout", "expected_timeout", "warns"),
    [
        (None, 5, False),
        (12.5, 12.5, False),
        (0, 5, True),
        (-1, 5, True),
        (float("nan"), 5, True),
        (float("inf"), 5, True),
        (True, 5, True),
        ("45", 5, True),
        (1e308, 5, True),
        (61, 5, True),
        (60, 60, False),
    ],
)
def test_origin_probe_timeout_uses_caller_budget(
    requested_timeout: float | None,
    expected_timeout: float,
    warns: bool,
    monkeypatch,
    capsys,
) -> None:
    interp = _load_interp()
    observed: list[float] = []

    def timeout(*_args, **kwargs):
        observed.append(kwargs["timeout"])
        raise subprocess.TimeoutExpired(cmd="python", timeout=kwargs["timeout"])

    monkeypatch.setattr(interp.subprocess, "run", timeout)
    options = {} if requested_timeout is None else {"origin_probe_timeout_seconds": requested_timeout}
    status, payload = interp._probe_runtime_origins("/tmp/hookq-other-python", {"workbay_protocol"}, None, **options)

    assert (status, payload) == ("timeout", {"deadline_seconds": expected_timeout})
    assert observed == [expected_timeout]
    stderr = capsys.readouterr().err
    if warns:
        assert "using default deadline_seconds=5" in stderr
        assert len(stderr.splitlines()) == 1
    else:
        assert stderr == ""


def test_origin_probe_timeout_names_callers_deadline_and_recovery_hint(monkeypatch, capsys) -> None:
    interp = _load_interp()
    caller_deadline = 17

    def timeout(*_args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="python", timeout=kwargs["timeout"])

    monkeypatch.setattr(interp.subprocess, "run", timeout)
    with pytest.raises(ImportError, match="probe_timeout") as caught:
        interp.assert_workbay_origins_trusted(
            "workbay_protocol",
            interpreter="/tmp/hookq-other-python",
            origin_probe_timeout_seconds=caller_deadline,
        )

    assert f"deadline_seconds={caller_deadline}" in str(caught.value)
    assert "check the interpreter with" in str(caught.value)
    assert len(capsys.readouterr().err.splitlines()) == 1


def _write_dist_info(directory: Path, dist_name: str, top_name: str, direct_url: str | None = None) -> Path:
    dist_info = directory / f"{dist_name}-1.0.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {dist_name}\nVersion: 1.0\n\n", encoding="utf-8")
    (dist_info / "top_level.txt").write_text(f"{top_name}\n", encoding="utf-8")
    if direct_url is not None:
        (dist_info / "direct_url.json").write_text(direct_url, encoding="utf-8")
    return dist_info


def test_origin_probe_and_inprocess_agree_when_two_distributions_claim_one_name(tmp_path: Path, monkeypatch) -> None:
    """Scan order decides a collision in the probe exactly as it does in process (API-09)."""
    import venv

    interp = _load_interp()
    top_name = "workbay_probe_claim_7bfed3"
    editable_record = '{"url":"file:///fixture-editable","dir_info":{"editable":true}}'
    wheel_record = '{"url":"file:///fixture.whl","archive_info":{}}'
    # Directory A, earlier on sys.path: an editable record that claims the name.
    claimant_dir = tmp_path / "claimant"
    _write_dist_info(claimant_dir, "zz_other", top_name, editable_record)
    # Directory B, the interpreter's own prefix site-packages: the package and a wheel record.
    prefix = tmp_path / "consumer-venv"
    venv.create(prefix, with_pip=False, symlinks=os.name != "nt")
    runtime = prefix / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    site_dir = Path(
        subprocess.run(
            [str(runtime), "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout.strip()
    )
    package = site_dir / top_name
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 'claimed'\n", encoding="utf-8")
    _write_dist_info(site_dir, top_name, top_name, wheel_record)
    monkeypatch.setenv("PYTHONPATH", str(claimant_dir))
    monkeypatch.setattr(sys, "path", [str(claimant_dir), str(site_dir), *sys.path])
    importlib.invalidate_caches()

    status, payload = interp._probe_runtime_origins(str(runtime), {top_name}, None, origin_probe_timeout_seconds=30)
    assert status == "ok", payload
    probed = payload["packages"][top_name]
    assert Path(probed["origin"]).resolve() == (package / "__init__.py").resolve()
    assert probed["direct_url"] == interp._read_direct_url(top_name) == editable_record

    hook_anchor = tmp_path / "hook-checkout"
    hook_anchor.mkdir()
    monkeypatch.setattr(interp, "_hook_checkout_anchors", lambda: (str(hook_anchor),))
    with pytest.raises(ImportError, match="hook_package_origin_untrusted"):
        interp.assert_workbay_origins_trusted(top_name, interpreter=str(runtime), origin_probe_timeout_seconds=30)


def _run_probe_with_sitecustomize(tmp_path: Path, monkeypatch, sitecustomize: str, name: str):
    interp = _load_interp()
    probe_root = tmp_path / "probe"
    probe_root.mkdir()
    (probe_root / "sitecustomize.py").write_text(sitecustomize, encoding="utf-8")
    package = probe_root / name
    package.mkdir()
    (package / "__init__.py").write_text("VALUE = 'probe fixture'\n", encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(probe_root))

    status, payload = interp._probe_runtime_origins(sys.executable, {name}, None)

    assert status == "ok", payload
    return interp, payload, package


def test_origin_probe_reports_distribution_scan_fallback_across_subprocess(tmp_path: Path, monkeypatch, capsys) -> None:
    name = "workbay_probe_scan_degraded_7bfed3"
    sitecustomize = (
        "import importlib.metadata\n"
        "def fail_packages_distributions():\n"
        "    raise RuntimeError('fixture scan failure')\n"
        "importlib.metadata.packages_distributions = fail_packages_distributions\n"
    )

    _interp, payload, package = _run_probe_with_sitecustomize(tmp_path, monkeypatch, sitecustomize, name)

    assert payload["packages"][name]["origin"] == str(package / "__init__.py")
    assert payload["packages"][name]["direct_url"] is None
    assert capsys.readouterr().err.splitlines() == [
        f"hook_origin_scan_failed: error=RuntimeError interpreter={sys.executable}"
    ]


def test_origin_probe_reports_claimant_lookup_fallback_across_subprocess(tmp_path: Path, monkeypatch, capsys) -> None:
    name = "workbay_probe_claimant_degraded_7bfed3"
    sitecustomize = (
        "import importlib.metadata\n"
        "class FailingDistributionMap:\n"
        "    def get(self, _name, _default=None):\n"
        "        raise LookupError('fixture claimant lookup failure')\n"
        "def packages_distributions():\n"
        "    return FailingDistributionMap()\n"
        "importlib.metadata.packages_distributions = packages_distributions\n"
    )

    _interp, payload, package = _run_probe_with_sitecustomize(tmp_path, monkeypatch, sitecustomize, name)

    assert payload["packages"][name]["origin"] == str(package / "__init__.py")
    assert payload["packages"][name]["direct_url"] is None
    assert capsys.readouterr().err.splitlines() == [
        f"hook_origin_claimant_lookup_failed: error=LookupError interpreter={sys.executable}"
    ]


@pytest.mark.parametrize("carried", ["", "nan", "inf", "-inf", "not-a-deadline", "x" * 80])
def test_invalid_carried_deadline_reports_fresh_budget_fallback(carried: str, monkeypatch, capsys) -> None:
    interp = _load_interp()
    _fake_clock(monkeypatch, 1000.0)
    _isolated_env(monkeypatch, WORKBAY_HOOK_REEXEC="1", **{interp._HOOK_DEADLINE_ENV: carried})

    budget = interp._start_subprocess_budget(20)

    assert budget is not None
    assert budget.take(20) == 20
    rendered = repr(carried)
    if len(rendered) > 40:
        rendered = rendered[:37] + "..."
    assert capsys.readouterr().err.splitlines() == [f"hook_deadline_carried_invalid: value={rendered}"]


def test_absent_carried_deadline_stays_silent(monkeypatch, capsys) -> None:
    interp = _load_interp()
    _fake_clock(monkeypatch, 1000.0)
    _isolated_env(monkeypatch, WORKBAY_HOOK_REEXEC="1")

    budget = interp._start_subprocess_budget(20)

    assert budget is not None
    assert budget.take(20) == 20
    assert capsys.readouterr().err == ""


def test_ensure_without_budget_resets_the_previous_subprocess_budget(monkeypatch) -> None:
    interp = _load_interp()
    clock = _fake_clock(monkeypatch, 1000.0)
    _isolated_env(monkeypatch)
    monkeypatch.setattr(interp, "_venv_python", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(interp, "_guard_current_hook", lambda **_kwargs: None)
    monkeypatch.setattr(interp, "_install_workbay_origin_guard", lambda: None)
    monkeypatch.setattr(interp, "_warn_interpreter_skew", lambda: None)
    monkeypatch.setattr(sys, "argv", [])
    observed_timeouts: list[float] = []

    def successful_git(*args, **kwargs):
        observed_timeouts.append(kwargs["timeout"])
        return subprocess.CompletedProcess(args, 0, stdout="/checkout\n", stderr="")

    monkeypatch.setattr(interp.subprocess, "run", successful_git)
    interp.ensure_deps_interpreter(subprocess_budget_seconds=1)
    assert interp._SUBPROCESS_BUDGET is not None

    clock["now"] += 2
    interp.ensure_deps_interpreter()
    interp._git_repo_root()

    assert interp._SUBPROCESS_BUDGET is None
    assert observed_timeouts == [5]


def test_origin_probe_scans_distributions_once_for_every_name(tmp_path: Path, monkeypatch, capsys) -> None:
    interp = _load_interp()
    prefix = tmp_path / "venv"
    site_packages = prefix / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    expected: dict[str, str] = {}
    for suffix in ("a", "b"):
        top_name = f"workbay_probe_once_{suffix}_7bfed3"
        package = site_packages / top_name
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("VALUE = 'once'\n", encoding="utf-8")
        expected[top_name] = f'{{"url":"file:///fixture-{suffix}","dir_info":{{"editable":true}}}}'
        _write_dist_info(site_packages, f"unmatched_once_{suffix}", top_name, expected[top_name])
    monkeypatch.setattr(sys, "prefix", str(prefix), raising=False)
    monkeypatch.setattr(site, "getsitepackages", lambda: [str(site_packages)])
    monkeypatch.setattr(sys, "path", [str(site_packages), *sys.path])
    scan_calls: list[bool] = []
    original_packages_distributions = metadata.packages_distributions

    def tracked_packages_distributions():
        scan_calls.append(True)
        return original_packages_distributions()

    monkeypatch.setattr(metadata, "packages_distributions", tracked_packages_distributions)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(sorted(expected))))

    exec(interp._ORIGIN_PROBE_CODE, {})

    output = json.loads(capsys.readouterr().out)
    assert {name: output["packages"][name]["direct_url"] for name in expected} == expected
    assert len(scan_calls) == 1


def test_origin_probe_resolves_unmatched_top_level_distribution_through_scan(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    interp = _load_interp()
    prefix = tmp_path / "venv"
    site_packages = prefix / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    package = site_packages / "workbay_probe_fallback_7bfed3"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 'fallback'\n", encoding="utf-8")
    dist_info = site_packages / "unmatched_fixture_distribution-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: unmatched-fixture-distribution\nVersion: 1.0\n\n", encoding="utf-8"
    )
    (dist_info / "top_level.txt").write_text("workbay_probe_fallback_7bfed3\n", encoding="utf-8")
    expected_direct_url = '{"url":"file:///fixture-fallback","dir_info":{"editable":true}}'
    (dist_info / "direct_url.json").write_text(expected_direct_url, encoding="utf-8")
    monkeypatch.setattr(sys, "prefix", str(prefix), raising=False)
    monkeypatch.setattr(site, "getsitepackages", lambda: [str(site_packages)])
    monkeypatch.setattr(sys, "path", [str(site_packages), *sys.path])
    scan_calls: list[bool] = []
    original_packages_distributions = metadata.packages_distributions

    def tracked_packages_distributions():
        scan_calls.append(True)
        return original_packages_distributions()

    monkeypatch.setattr(metadata, "packages_distributions", tracked_packages_distributions)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(["workbay_probe_fallback_7bfed3"])))

    exec(interp._ORIGIN_PROBE_CODE, {})

    output = json.loads(capsys.readouterr().out)
    assert output["packages"]["workbay_probe_fallback_7bfed3"]["direct_url"] == expected_direct_url
    assert len(scan_calls) == 1


def test_origin_probe_skips_a_claimant_without_direct_url(tmp_path: Path, monkeypatch, capsys) -> None:
    interp = _load_interp()
    prefix = tmp_path / "venv"
    site_packages = prefix / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    top_name = "workbay_probe_collision_7bfed3"
    package = site_packages / top_name
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 'collision'\n", encoding="utf-8")

    candidate_dist_info = site_packages / f"{top_name}-1.0.dist-info"
    candidate_dist_info.mkdir()
    (candidate_dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {top_name}\nVersion: 1.0\n\n", encoding="utf-8"
    )
    (candidate_dist_info / "top_level.txt").write_text(f"{top_name}\n", encoding="utf-8")

    mapped_dist_info = site_packages / "unrelated_editable_probe-1.0.dist-info"
    mapped_dist_info.mkdir()
    (mapped_dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: unrelated-editable-probe\nVersion: 1.0\n\n", encoding="utf-8"
    )
    (mapped_dist_info / "top_level.txt").write_text(f"{top_name}\n", encoding="utf-8")
    expected_direct_url = '{"url":"file:///fixture-editable","dir_info":{"editable":true}}'
    (mapped_dist_info / "direct_url.json").write_text(expected_direct_url, encoding="utf-8")

    monkeypatch.setattr(sys, "prefix", str(prefix), raising=False)
    monkeypatch.setattr(site, "getsitepackages", lambda: [str(site_packages)])
    monkeypatch.setattr(sys, "path", [str(site_packages), *sys.path])
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps([top_name])))

    exec(interp._ORIGIN_PROBE_CODE, {})

    output = json.loads(capsys.readouterr().out)
    assert output["packages"][top_name]["direct_url"] == expected_direct_url


def test_ensure_threads_caller_probe_deadline_to_subprocess(monkeypatch) -> None:
    interp = _load_interp()
    monkeypatch.setattr(interp, "_venv_python", lambda _repo_root=None: "/tmp/hookq-other-python")
    monkeypatch.setattr(interp, "_current_hook_import_names", lambda _repo_root=None: {"workbay_protocol"})
    monkeypatch.setattr(interp, "_prefix_site_packages", lambda: ())
    monkeypatch.setattr(interp, "_hook_checkout_anchors", lambda: ("/trusted",))
    monkeypatch.setattr(interp, "_install_workbay_origin_guard", lambda: None)
    monkeypatch.setattr(interp, "_warn_interpreter_skew", lambda: None)
    monkeypatch.setenv("WORKBAY_HOOK_REEXEC", "1")
    observed: list[float] = []

    class _SuccessfulProbe:
        returncode = 0
        stdout = json.dumps(
            {
                "prefix": "/tmp/hookq-other-prefix",
                "site_packages": [],
                "packages": {
                    "workbay_protocol": {
                        "origin": "/trusted/workbay_protocol.py",
                        "locations": [],
                        "direct_url": None,
                    }
                },
            }
        )

    def successful_probe(*_args, **kwargs):
        observed.append(kwargs["timeout"])
        return _SuccessfulProbe()

    monkeypatch.setattr(interp.subprocess, "run", successful_probe)

    interp.ensure_deps_interpreter(origin_probe_timeout_seconds=23.5)

    assert observed == [23.5]


_HOOK_DEADLINE_ENV = "WORKBAY_HOOK_SUBPROCESS_DEADLINE"


class _Reexec(Exception):
    """Raised by the fake ``os.execv``: the process image would be replaced here."""


def _isolated_env(monkeypatch, **values: str) -> dict[str, str]:
    dropped = {"WORKBAY_HOOK_REEXEC", _HOOK_DEADLINE_ENV, "CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT"}
    env = {key: value for key, value in os.environ.items() if key not in dropped}
    env.update(values)
    monkeypatch.setattr(os, "environ", env)
    return env


def _fake_clock(monkeypatch, start: float) -> dict[str, float]:
    clock = {"now": start}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    return clock


def test_exhausted_budget_refuses_the_origin_probe_and_names_the_budget(monkeypatch) -> None:
    interp = _load_interp()
    clock = _fake_clock(monkeypatch, 500.0)
    _isolated_env(monkeypatch)

    def venv_python_that_used_the_budget(*_args, **_kwargs):
        clock["now"] += 40
        return "/tmp/hookq-other-python"

    monkeypatch.setattr(interp, "_venv_python", venv_python_that_used_the_budget)
    monkeypatch.setattr(interp, "_current_hook_import_names", lambda _repo_root=None: {"workbay_protocol"})
    monkeypatch.setattr(interp, "_hook_checkout_anchors", lambda: ("/trusted",))
    spawned: list[object] = []
    monkeypatch.setattr(interp.subprocess, "run", lambda *args, **_kwargs: spawned.append(args))

    with pytest.raises(ImportError, match="probe_timeout") as caught:
        interp.ensure_deps_interpreter(origin_probe_timeout_seconds=30, subprocess_budget_seconds=40)

    message = str(caught.value)
    assert "interpreter=/tmp/hookq-other-python deadline_seconds=0 " in message
    assert "hook_subprocess_budget_seconds=40 exhausted" in message
    assert "check the interpreter with" in message
    assert spawned == []


def test_exhausted_budget_skips_git_and_version_skew_without_spawning(monkeypatch, capsys) -> None:
    interp = _load_interp()
    clock = _fake_clock(monkeypatch, 500.0)
    _isolated_env(monkeypatch)
    monkeypatch.setattr(sys, "argv", [])

    def venv_python_that_used_the_budget(*_args, **_kwargs):
        clock["now"] += 40
        return ""

    monkeypatch.setattr(interp, "_venv_python", venv_python_that_used_the_budget)
    monkeypatch.setattr(interp, "_install_workbay_origin_guard", lambda: None)
    spawned: list[object] = []
    monkeypatch.setattr(interp.subprocess, "run", lambda *args, **_kwargs: spawned.append(args))

    interp.ensure_deps_interpreter(subprocess_budget_seconds=40)

    assert interp._git_repo_root() == ""
    assert interp._primary_checkout_root() == ""
    assert interp._git_from_dir("/tmp", "rev-parse", "--show-toplevel") == ""
    assert interp._probe_pkg_version(sys.executable) == ""
    assert spawned == []
    stderr = capsys.readouterr().err.splitlines()
    assert len(stderr) == 1
    assert "hook subprocess budget of 40s exhausted" in stderr[0]


@pytest.mark.parametrize("budget_seconds", [40, None])
def test_reexec_carries_the_hook_deadline_in_the_environment(budget_seconds, monkeypatch) -> None:
    interp = _load_interp()
    clock = _fake_clock(monkeypatch, 1000.0)
    env = _isolated_env(monkeypatch)
    monkeypatch.setattr(interp, "_venv_python", lambda *_args, **_kwargs: "/tmp/hookq-other-python")
    monkeypatch.setattr(interp, "_guard_current_hook", lambda **_kwargs: clock.__setitem__("now", clock["now"] + 7))
    monkeypatch.setattr(interp, "_install_workbay_origin_guard", lambda: None)
    monkeypatch.setattr(interp, "_warn_interpreter_skew", lambda: None)
    captured: dict[str, str] = {}

    def execv(path, argv):
        captured.update(env)
        raise _Reexec(path)

    monkeypatch.setattr(os, "execv", execv)
    options = {} if budget_seconds is None else {"subprocess_budget_seconds": budget_seconds}

    with pytest.raises(_Reexec):
        interp.ensure_deps_interpreter(**options)

    assert captured["WORKBAY_HOOK_REEXEC"] == "1"
    if budget_seconds is None:
        assert _HOOK_DEADLINE_ENV not in captured
    else:
        assert float(captured[_HOOK_DEADLINE_ENV]) == 1000.0 + budget_seconds


@pytest.mark.parametrize(
    ("reexec", "carried", "expected_grant"),
    [
        ("1", "1010.0", 10),
        ("1", None, 20),
        ("1", "nan", 20),
        ("1", "inf", 20),
        ("1", "-inf", 20),
        ("1", "not-a-deadline", 20),
        ("1", "1000000000.0", 20),
        ("1", "990.0", 0),
        (None, "1010.0", 20),
    ],
)
def test_second_pass_budget_only_shortens_from_the_carried_deadline(
    reexec: str | None, carried: str | None, expected_grant: float, monkeypatch, capsys
) -> None:
    interp = _load_interp()
    _fake_clock(monkeypatch, 1000.0)
    values = {}
    if reexec is not None:
        values["WORKBAY_HOOK_REEXEC"] = reexec
    if carried is not None:
        values[_HOOK_DEADLINE_ENV] = carried
    _isolated_env(monkeypatch, **values)
    monkeypatch.setattr(interp, "_venv_python", lambda *_args, **_kwargs: "/tmp/hookq-other-python")
    monkeypatch.setattr(interp, "_current_hook_import_names", lambda _repo_root=None: {"workbay_protocol"})
    monkeypatch.setattr(interp, "_prefix_site_packages", lambda: ())
    monkeypatch.setattr(interp, "_hook_checkout_anchors", lambda: ("/trusted",))
    monkeypatch.setattr(interp, "_install_workbay_origin_guard", lambda: None)
    monkeypatch.setattr(interp, "_warn_interpreter_skew", lambda: None)
    monkeypatch.setattr(os, "execv", lambda path, _argv: (_ for _ in ()).throw(_Reexec(path)))
    observed: list[float] = []

    def probe(*_args, **kwargs):
        observed.append(kwargs["timeout"])
        packages = {"workbay_protocol": {"origin": "/trusted/workbay_protocol.py", "locations": [], "direct_url": None}}
        stdout = json.dumps({"prefix": "/tmp/hookq-other-prefix", "site_packages": [], "packages": packages})
        return subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")

    monkeypatch.setattr(interp.subprocess, "run", probe)

    def second_pass() -> None:
        try:
            interp.ensure_deps_interpreter(origin_probe_timeout_seconds=30, subprocess_budget_seconds=20)
        except _Reexec:
            assert reexec is None

    if expected_grant == 0:
        with pytest.raises(ImportError, match="hook_subprocess_budget_seconds=20 exhausted"):
            second_pass()
        assert observed == []
    else:
        second_pass()
        assert observed == [expected_grant]
    if carried is None:
        assert capsys.readouterr().err == ""


def test_origin_probe_overflow_from_subprocess_is_probe_failed(monkeypatch, capsys) -> None:
    interp = _load_interp()

    def overflow(*_args, **_kwargs):
        raise OverflowError("timeout too large")

    monkeypatch.setattr(interp.subprocess, "run", overflow)
    with pytest.raises(ImportError, match="probe_failed") as caught:
        interp.assert_workbay_origins_trusted("workbay_protocol", interpreter="/tmp/hookq-other-python")
    assert "exit=OverflowError" in str(caught.value)
    assert len(capsys.readouterr().err.splitlines()) == 1


def test_origin_probe_unlaunchable_interpreter_is_probe_failed(capsys) -> None:
    """``subprocess.run`` raises ValueError for a NUL in argv; that refuses, it does not escape."""
    interp = _load_interp()
    with pytest.raises(ImportError, match="probe_failed") as caught:
        interp.assert_workbay_origins_trusted("workbay_protocol", interpreter="/tmp/hookq\0python")
    assert "exit=ValueError" in str(caught.value)
    assert len(capsys.readouterr().err.splitlines()) == 1


def test_origin_probe_nonzero_names_probe_failed(monkeypatch) -> None:
    interp = _load_interp()

    class _Failed:
        returncode = 3
        stdout = ""
        stderr = "probe boom"

    monkeypatch.setattr(interp.subprocess, "run", lambda *_args, **_kwargs: _Failed())
    with pytest.raises(ImportError, match="probe_failed") as caught:
        interp.assert_workbay_origins_trusted("workbay_protocol", interpreter="/tmp/hookq-other-python")
    assert "exit=3" in str(caught.value)
