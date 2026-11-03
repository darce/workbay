"""Parallel mutant sweep over an edgeless conflict graph (GRPH-09 / GRPH-32).

Makespan model (must honour; do not claim optimality)
-----------------------------------------------------
- **Edgeless conflict graph (GRPH-09, GRPH-32):** no mutant reads another
  mutant's output, and implementation note uses one mutant per sandbox, so there are zero
  precedence edges and zero resource edges. Chromatic number is 1: the whole
  sweep is a single parallel round; no mutant waits on another for data.
- **Frozen topology (GRPH-39):** each worker mutates its own tree copy. Workers
  MUST NEVER share a writable tree — that precondition is what licenses the
  parallelism.
- **P||Cmax (GRPH-31):** with ``p`` identical workers the makespan floor is
  ``max(longest_single_mutant, total_work / p)`` plus the serial spine
  (baseline run + final adjudication). P||Cmax is strongly NP-hard, so this
  module uses LPT / longest-remaining-first *list scheduling* and does **not**
  claim an optimal schedule.
- **Unknown durations:** when no per-mutant duration hints are available, fall
  back to manifest order and record observed durations on each result so a
  later run can order by them (still LPT list scheduling, not optimal).

Implementation notes
--------------------
Pool size defaults to ``min(cores - 1, N)`` (overridable). Work is
subprocess-bound, so a ``ThreadPoolExecutor`` over injectable runner callables
is sufficient — no multiprocessing.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Mapping, Sequence

from mutation_harness.models import Mutant, MutantResult

RunnerFn = Callable[[Mutant], MutantResult]


def default_jobs(n_mutants: int, *, cores: int | None = None) -> int:
    """Default pool size: ``min(cores - 1, N)``, at least 1 when N > 0."""
    if n_mutants <= 0:
        return 1
    c = cores if cores is not None else (os.cpu_count() or 2)
    return max(1, min(max(1, c - 1), n_mutants))


def order_for_dispatch(
    mutants: Sequence[Mutant],
    *,
    duration_hints: Mapping[str, float] | None = None,
) -> tuple[list[Mutant], str]:
    """Return dispatch order and schedule mode label.

    When any duration is known (hint map or ``Mutant.expected_duration``),
    sort longest-first (LPT list order). Otherwise preserve manifest order.
    Does not claim optimality either way.
    """
    hints = dict(duration_hints or {})

    def duration_of(m: Mutant) -> float | None:
        if m.id in hints:
            return float(hints[m.id])
        if m.expected_duration is not None:
            return float(m.expected_duration)
        return None

    known = [(m, duration_of(m)) for m in mutants]
    if any(d is not None for _, d in known):
        # LPT list order: longest first; unknowns treated as 0 so known long
        # jobs lead. Stable among equal durations via original index.
        # No optimality claim (P||Cmax is strongly NP-hard).
        index = {m.id: i for i, m in enumerate(mutants)}
        ordered = sorted(
            known,
            key=lambda pair: (
                -(pair[1] if pair[1] is not None else 0.0),
                index[pair[0].id],
            ),
        )
        return [m for m, _ in ordered], "lpt"
    return list(mutants), "manifest"


def run_sweep(
    mutants: Sequence[Mutant],
    *,
    runner: RunnerFn,
    jobs: int | None = None,
    duration_hints: Mapping[str, float] | None = None,
    progress: Callable[[dict], None] | None = None,
    cores: int | None = None,
) -> tuple[list[MutantResult], str, int]:
    """Execute all mutants in parallel; return results in **manifest order**.

    Parameters
    ----------
    mutants:
        Manifest-ordered mutant list (output order is always this order).
    runner:
        Injectable callable ``Mutant -> MutantResult``. Production uses
        sandbox+pytest; tests inject fakes with synthetic durations.
    jobs:
        Worker pool size. Default ``min(cores-1, N)``.
    duration_hints:
        Optional ``mutant_id -> seconds`` for LPT dispatch ordering.
    progress:
        Optional line-oriented event sink (one dict per event).
    cores:
        Override for ``os.cpu_count()`` when computing the default pool size.

    Returns
    -------
    (results_in_manifest_order, schedule_mode, jobs_used)

    Scheduling is LPT list scheduling when durations are known, else manifest
    order. No optimality claim.
    """
    mutant_list = list(mutants)
    if not mutant_list:
        return [], "manifest", 1

    n = len(mutant_list)
    jobs_used = jobs if jobs is not None else default_jobs(n, cores=cores)
    jobs_used = max(1, min(jobs_used, n))

    dispatch_order, schedule_mode = order_for_dispatch(
        mutant_list, duration_hints=duration_hints
    )

    if progress:
        progress(
            {
                "event": "sweep_start",
                "mutant_count": n,
                "jobs": jobs_used,
                "schedule_mode": schedule_mode,
            }
        )

    by_id: dict[str, MutantResult] = {}

    def _wrapped(m: Mutant) -> MutantResult:
        if progress:
            progress({"event": "dispatch", "mutant_id": m.id})
        return runner(m)

    # Thread pool over independent sandboxed work (edgeless conflict graph).
    with ThreadPoolExecutor(max_workers=jobs_used) as pool:
        futures = {pool.submit(_wrapped, m): m for m in dispatch_order}
        for fut in as_completed(futures):
            m = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001 — fail closed per mutant
                result = MutantResult(
                    mutant_id=m.id,
                    status="error",
                    killing_tests=[],
                    duration=0.0,
                    error_message=f"runner raised: {exc}",
                )
            by_id[result.mutant_id] = result
            if progress:
                progress(
                    {
                        "event": "mutant_complete",
                        "mutant_id": result.mutant_id,
                        "status": result.status,
                        "duration": result.duration,
                    }
                )

    # Deterministic manifest order — never completion order.
    results = [
        by_id.get(
            m.id,
            MutantResult(
                mutant_id=m.id,
                status="error",
                error_message="missing result from worker",
            ),
        )
        for m in mutant_list
    ]

    if progress:
        progress(
            {
                "event": "sweep_done",
                "mutant_count": n,
                "jobs": jobs_used,
                "schedule_mode": schedule_mode,
            }
        )

    return results, schedule_mode, jobs_used
