"""Repo-root pytest bootstrap.

The live-``.task-state`` hermeticity guard lives in ``wb_live_state_guard`` so it
can load as a plugin regardless of pytest ``rootdir`` (package-scoped runs set
``rootdir`` to the package dir, above which this file is never collected). This
conftest registers it for full-tree runs; each package ``tests/conftest.py``
registers the same module for package-scoped runs.
"""

from __future__ import annotations

pytest_plugins = ["wb_live_state_guard"]
