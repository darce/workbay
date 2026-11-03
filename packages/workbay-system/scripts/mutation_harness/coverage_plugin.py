"""Internal pytest plugin that writes one coverage.py data file per process."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def pytest_configure(config: object) -> None:
    try:
        import coverage as coverage_module
        from coverage import Coverage
    except ImportError as exc:
        raise RuntimeError("coverage.py is required for mutation coverage") from exc
    data_file = os.environ.get("MUTATION_HARNESS_COVERAGE_FILE")
    source_root = os.environ.get("MUTATION_HARNESS_COVERAGE_ROOT")
    if not data_file or not source_root:
        raise RuntimeError("mutation coverage plugin environment is incomplete")
    startup_config = os.environ.get("MUTATION_HARNESS_COVERAGE_CONFIG")
    if not startup_config:
        raise RuntimeError("mutation coverage startup configuration is missing")
    os.environ["COVERAGE_PROCESS_START"] = startup_config
    # sitecustomize also runs while xdist bootstraps a worker, before xdist has
    # set PYTEST_XDIST_WORKER. Discard that unscoped auto-collector and replace
    # it with the phase-aware collector below. Ordinary test child processes
    # never load this pytest plugin, so their startup collector remains active.
    auto_coverage = getattr(coverage_module.process_startup, "coverage", None)
    if auto_coverage is not None:
        auto_coverage.stop()
        auto_coverage._auto_save = False
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "controller")
    process_file = f"{data_file}.{worker_id}.{os.getpid()}"
    coverage = Coverage(
        data_file=process_file,
        data_suffix=False,
        source=[str(Path(source_root).resolve())],
    )
    coverage.start()
    config.pluginmanager.register(_PerTestCoverage(coverage), "mutation-per-test-coverage")


class _PerTestCoverage:
    def __init__(self, coverage: object) -> None:
        self.coverage = coverage

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_call(self, item: object):  # type: ignore[no-untyped-def]
        """Attribute only the test call, never shared fixture setup/teardown.

        A broader ``pytest_runtest_protocol`` context incorrectly assigns a
        session fixture to the first item that triggers it.  Selecting only
        that item can omit a later test that asserts on the shared value.
        Fixture execution therefore remains in the empty context and forces
        the selector's conservative ``unattributed_execution`` fallback.
        """
        self.coverage.switch_context(item.nodeid)
        yield
        self.coverage.switch_context("")

    def pytest_unconfigure(self, config: object) -> None:
        self.coverage.stop()
        self.coverage.save()
