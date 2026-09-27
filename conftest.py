"""Repo-root pytest bootstrap.

The live-``.task-state`` hermeticity guard lives in ``wb_live_state_guard`` so it
can load as a plugin regardless of pytest ``rootdir`` (package-scoped runs set
``rootdir`` to the package dir, above which this file is never collected). The
``WORKBAY_TEST_TIER`` selector lives in ``wb_test_tier`` for the same reason.
This conftest registers both for full-tree runs; each package
``tests/conftest.py`` registers the same modules for package-scoped runs.
"""

from __future__ import annotations

import os

import pytest

pytest_plugins = ["wb_live_state_guard", "wb_test_tier"]


@pytest.fixture(scope="session", autouse=True)
def _isolate_codemap_environment(tmp_path_factory: pytest.TempPathFactory):
    """Keep test repos and detached runners out of the operator's CBM cache."""
    if os.environ.get("WORKBAY_TESTS_ALLOW_REAL_CODEMAP") == "1":
        yield
        return

    root = tmp_path_factory.mktemp("codemap-cache")
    cache_dir = root / "cache"
    runtime_dir = root / "runtime"
    cache_dir.mkdir()
    runtime_dir.mkdir()
    stub = root / "codebase-memory-mcp"
    stub.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' \'{"ok": true, "note": "stubbed_by_tests"}\'\n',
        encoding="utf-8",
    )
    stub.chmod(0o755)

    values = {
        "WORKBAY_CODEMAP_REINDEX": "0",
        "CBM_CACHE_DIR": str(cache_dir),
        "CBM_RUNTIME_DIR": str(runtime_dir),
        "CODEBASE_MEMORY_MCP": str(stub),
    }
    missing = object()
    previous = {name: os.environ.get(name, missing) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, old_value in previous.items():
            if old_value is missing:
                os.environ.pop(name, None)
            else:
                os.environ[name] = old_value
