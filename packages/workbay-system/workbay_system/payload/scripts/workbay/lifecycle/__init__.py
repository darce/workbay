"""Lifecycle runner package (implementation note).

Owns parsing, git resolution, JSON receipt emission, and projection
calls for the ``make task-start`` / ``make context`` / ``make
slice-start`` family of lifecycle Make targets. The package is invoked
as ``python <abs-path>/scripts/workbay/lifecycle <subcommand>`` via the
package ``__main__`` entry point so that the directory and its sibling
modules (``cli``, ``resolver``, ``projection``, per-subcommand
handlers) coexist.
"""

__all__: list[str] = []
