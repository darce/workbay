"""Per-subcommand handler modules for the lifecycle runner.

implementation note ships ``skill_broadcast`` (plan-review / plan-analyze) and
``shell_out`` (review-run / handoff-review-run / handoff-close-check).
Slices 2-6 add resolver-backed handlers (context, task-start,
slice-start, review-ready, close-check, status, tasks,
project-events-replay) and replace the matching stub entries in
:data:`cli.STUB_HANDLERS`.
"""

__all__: list[str] = []
