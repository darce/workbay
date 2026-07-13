"""``workbay-hostgov`` — standalone host-memory admission probe (internal).

A single admission implementation shared by the in-process orchestrator gate
and out-of-process callers (notably ``scripts/remote_gate.sh``'s pre-``make``
hook on the OCI gate host). ``probe`` reports the host snapshot + admission
verdict and maps the verdict onto an exit code so a shell caller can gate on
it without parsing:

    0   allow    (spawn may proceed)
    75  defer    (retryable — pressure/slots; caller should back off and retry)
    76  refuse   (a resource floor is breached; do not spawn)

remote_gate.sh treats any non-zero as "back off" (its own ``exit 75``), so the
distinct 75/76 split is informational for humans and richer callers.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .host_resources import (
    COST_HEAVY,
    AdmissionDecision,
    evaluate_admission,
    load_host_memory_policy,
    probe_host,
    resolve_live_admission,
)

_EXIT_BY_DECISION = {"allow": 0, "defer": 75, "refuse": 76}


def _evaluate(*, cost_class: str, workspace_root: Path, held_slots: int | None) -> AdmissionDecision:
    # held_slots=None => count live slots (the real gate path); an explicit int
    # is an override for testing/manual what-if probing.
    if held_slots is None:
        return resolve_live_admission(workspace_root, cost_class)
    resources = probe_host(force=True)
    policy = load_host_memory_policy(workspace_root)
    return evaluate_admission(resources, cost_class, policy, held_slots)


def _render_human(decision: AdmissionDecision) -> str:
    snap = decision.snapshot
    avail_gib = snap.available_ram / 1024**3
    return (
        f"admission: {decision.decision.upper()} ({decision.reason})\n"
        f"  cost_class={decision.cost_class} derived_width={decision.derived_width} "
        f"held_slots={decision.held_slots} enforced={decision.enforced}\n"
        f"  platform={snap.platform} available_ram={avail_gib:.1f}GiB "
        f"pressure={snap.pressure}" + (f" probe_error={snap.probe_error}" if snap.probe_error else "")
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workbay-hostgov",
        description="Host-memory admission probe (exit 0 allow / 75 defer / 76 refuse).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    probe = sub.add_parser("probe", help="probe host + report admission verdict")
    probe.add_argument("--json", action="store_true", help="emit the decision as JSON")
    probe.add_argument(
        "--cost-class",
        default=COST_HEAVY,
        choices=[COST_HEAVY, "suite", "light"],
        help="spawn cost class to evaluate (default: heavy)",
    )
    probe.add_argument(
        "--workspace-root",
        default=".",
        help="repo root to load the host_memory policy from (default: cwd; "
        "absent/invalid contract => enforce defaults)",
    )
    probe.add_argument(
        "--held-slots",
        type=int,
        default=None,
        help="override the live held-slot count (default: count the slot registry)",
    )
    args = parser.parse_args(argv)

    if args.command == "probe":
        held = None if args.held_slots is None else max(0, args.held_slots)
        decision = _evaluate(
            cost_class=args.cost_class,
            workspace_root=Path(args.workspace_root).expanduser(),
            held_slots=held,
        )
        if args.json:
            print(json.dumps(decision.to_dict(), sort_keys=True))
        else:
            print(_render_human(decision))
        return _EXIT_BY_DECISION.get(decision.decision, 76)

    parser.print_help(sys.stderr)  # pragma: no cover - argparse requires subcommand
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
