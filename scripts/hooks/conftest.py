"""Pytest bootstrap for scripts/hooks unit tests.

The hooks tree is outside both package testpaths and the non-integration
gate path list, so it never inherits a package-level bootstrap. Fixtures in
reinject-context, compact-session, and compaction-failed modules seed
handoff state via set_handoff_state; without a write-actor identity that
raises WriteActorAttributionError during setup.
"""

from __future__ import annotations

import os

# Default write-actor identity for tests that drive the handoff write-path
# (set_handoff_state in reinject/compact/compaction_failed fixtures). Outside a
# live harness no transcript env var is set, so the write would raise
# WriteActorAttributionError; setdefault keeps a real harness's own identity
# when one is present.
os.environ.setdefault("WORKBAY_HANDOFF_DEFAULT_AGENT", "scripts-hooks-tests")
