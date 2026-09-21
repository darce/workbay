"""Deterministic pre-merge landing gate (internal).

The internal campaign produced three wrong measurements before it
produced a correct one, and every wrong measurement was found by hand, after
the fact, by an operator who happened to distrust the number.  That is the
part that does not scale.  This module turns each of those discoveries into a
typed outcome a gate run can refuse on, so the next occurrence costs a refusal
instead of a landing.

The three:

``subject_behind_baseline``
    A branch gated at its own tip measures its "FIXED" set against commits it
    simply does not have.  On lc-ic1 that read as ten fixes; two of them were
    already on ``main``.  Gate the merge *result*, not the branch tip.

``environment_inconsistent``
    A lane worktree's ``.venv`` symlinks to the root ``.venv``, whose
    ``zz_dev_redirect_*.pth`` points at the ROOT checkout.  The run then
    executes the branch's tests against main's packages, and every symbol the
    branch ADDS becomes a phantom ``ImportError``.  That produced 29 fictional
    regressions on one run.  Resolve the guarded modules first and refuse if
    any of them lands outside the worktree.

checkout-token normalization
    Some tests are parameterized on the repository DIRECTORY NAME, so one
    failure carries a different id in every checkout and the diff reports one
    phantom NEW plus one phantom FIXED per such test.  Collapse the token
    before comparing -- and say how many ids the collapse merged, because a
    normalizing gate that is silent about normalizing is just a different way
    to be wrong [OBS-08].

Every outcome is a string from a closed set rather than a boolean, so a caller
cannot read "not regressed" as "safe to merge" [heuristics: outcome enum over
deceptive booleans].  ``gate_report`` fails closed on an outcome it does not
recognise.

Parser, scope, probe, and policy live in sibling modules; this file is the
compatible public API and CLI [REVIEW-M-07].
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

# Script and runpy wrappers (gate_ids tests, LANDING_GATE_SCRIPT overrides)
# may execute this file without putting its directory on sys.path. Keep the
# directory available for callers that import helper modules by name, but load
# the split producer modules below from explicit sibling paths. A sys.path
# bootstrap alone is insufficient: an already-cached private module wins over
# path lookup [REVIEW-M-07, CARD-12].
_SCRIPTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
_MISSING_MODULE = object()
_SIBLING_NAMES = (
    "landing_measurement",
    "landing_gate_policy",
    "landing_gate_parsing",
    "landing_gate_probe",
    "landing_gate_scope",
)


def _load_sibling(name: str) -> Any:
    """Load a producer sibling from this facade's directory by file path.

    The facade is commonly loaded by path, while the split modules retain
    canonical top-level names for script compatibility. Replacing a cached
    module only when its origin is elsewhere preserves that compatibility but
    prevents a private module in ``sys.modules`` from crossing the export
    boundary. Dependencies are loaded in order below so their own canonical
    imports also resolve to this same sibling directory.
    """
    sibling = Path(_SCRIPTS_DIR) / f"{name}.py"
    existing = sys.modules.get(name, _MISSING_MODULE)
    existing_file = getattr(existing, "__file__", None)
    if existing_file is not None:
        try:
            if Path(existing_file).resolve() == sibling.resolve():
                return existing
        except (OSError, RuntimeError):
            pass

    spec = importlib.util.spec_from_file_location(name, sibling)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load landing-gate sibling {name!r} from {sibling}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        if existing is _MISSING_MODULE:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = existing
        raise
    return module


# Load dependency layers by their own paths before binding the compatible
# facade API. In particular, parsing's scrubbed regex comes from the exported
# ``landing_gate_parsing.py`` even when private canonical modules are cached.
_sibling_snapshot = {name: sys.modules.get(name, _MISSING_MODULE) for name in _SIBLING_NAMES}
try:
    lm = _load_sibling("landing_measurement")
    _policy = _load_sibling("landing_gate_policy")
    _parsing = _load_sibling("landing_gate_parsing")
    _probe = _load_sibling("landing_gate_probe")
    _scope = _load_sibling("landing_gate_scope")
except BaseException:
    for name, previous in _sibling_snapshot.items():
        if previous is _MISSING_MODULE:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    raise

_ids_bound_to_raw = _parsing._ids_bound_to_raw
_suite_executed = _parsing._suite_executed
_subject_suite_executed = _parsing._subject_suite_executed
extract_failure_ids = _parsing.extract_failure_ids
failure_id_diff = _parsing.failure_id_diff
normalize_failure_id = _parsing.normalize_failure_id
normalize_failure_ids = _parsing.normalize_failure_ids

DIFF_OUTCOMES = _policy.DIFF_OUTCOMES
ENVIRONMENT_OUTCOMES = _policy.ENVIRONMENT_OUTCOMES
FRESHNESS_OUTCOMES = _policy.FRESHNESS_OUTCOMES
LandingGateError = _policy.LandingGateError
MEASUREMENT_OUTCOMES = _policy.MEASUREMENT_OUTCOMES
SCOPE_OUTCOMES = _policy.SCOPE_OUTCOMES
_policy_gate_report = _policy.gate_report

DEFAULT_GUARDED_MODULES = _probe.DEFAULT_GUARDED_MODULES
import_path_roots = _probe.import_path_roots
import_pythonpath = _probe.import_pythonpath
subject_freshness = _probe.subject_freshness
_DEFAULT_PROBE = _probe._PROBE
_environment_consistency = _probe.environment_consistency

_load_scope = _scope._load_scope
_stable_pytest_options = _scope._stable_pytest_options
build_invocation_scope = _scope.build_invocation_scope
compare_invocation_scopes = _scope.compare_invocation_scopes

_PROBE = _DEFAULT_PROBE
_JSON_ERROR_LIMIT = 400


def environment_consistency(
    worktree: Path | str,
    modules: Sequence[str],
    *,
    python: str,
    extra_paths: Sequence[Path | str] = (),
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Resolve guarded modules; ``_PROBE`` remains patchable on this facade."""
    return _environment_consistency(
        worktree,
        modules,
        python=python,
        extra_paths=extra_paths,
        env=env,
        probe_source=_PROBE,
    )


def measurement_completeness(
    raw_logs: "dict[str, Path | None]",
    *,
    ids_logs: "dict[str, Path | None] | None" = None,
    scope_logs: "dict[str, Path | None] | None" = None,
    artifact_snapshots: "dict[str, dict[str, Any]] | None" = None,
) -> dict[str, Any]:
    """Did the producing run finish, as proven by a producer-owned receipt?

    A printable pytest summary -- equals-wrapped or quiet ``-q`` -- is not
    completion evidence. The producer writes a receipt only after the pytest
    process returns and binds it to the exact caller-supplied raw/ID/scope
    bytes [N9REVI-H-01, REVIEW-M-04].

    A requested side whose path is ``None`` is dropped only when *no* side was
    supplied. Once any raw is present, every other named side must also be
    present and authenticated: a complete baseline plus an omitted subject
    must not score as ``measurement.outcome=complete``.
    """
    return lm.measurement_completeness(
        raw_logs,
        ids_logs=ids_logs,
        scope_logs=scope_logs,
        error_cls=LandingGateError,
        artifact_snapshots=artifact_snapshots,
    )


def gate_report(
    *,
    freshness: dict[str, Any],
    environment: dict[str, Any],
    diff: dict[str, Any],
    measurement: dict[str, Any],
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Combine the probes into one typed verdict. See ``landing_gate_policy``."""
    return _policy_gate_report(
        freshness=freshness,
        environment=environment,
        diff=diff,
        measurement=measurement,
        scope=scope,
    )


class _UsageError(Exception):
    """CLI could not even start: missing args or an unknown flag."""


class _LandingGateArgumentParser:
    """argparse wrapper that turns usage errors into ``_UsageError``."""

    def __init__(self, parser: Any) -> None:
        self._parser = parser
        self._parser.error = self.error

    def add_argument(self, *args: Any, **kwargs: Any) -> Any:
        return self._parser.add_argument(*args, **kwargs)

    def parse_args(self, argv: list[str] | None) -> Any:
        try:
            return self._parser.parse_args(argv)
        except SystemExit as exc:
            status = exc.code if isinstance(exc.code, int) else 2
            if status == 0:
                raise
            raise _UsageError("invalid arguments") from exc

    def error(self, message: str) -> None:
        raise _UsageError(message)


def _effective_argv(argv: list[str] | None) -> list[str]:
    """Normalize CLI argv once so ``main(None)`` sees the real process args."""
    if argv is None:
        return list(sys.argv[1:])
    return list(argv)


def _json_requested(argv: list[str] | None) -> bool:
    return "--json" in _effective_argv(argv)


def _bounded_error(message: str) -> str:
    return " ".join(str(message).split())[:_JSON_ERROR_LIMIT]


def _cli_refusal(*, json_mode: bool, outcome: str, error: str) -> int:
    import sys as _sys

    diagnostic = _bounded_error(error)
    if json_mode:
        print(
            json.dumps(
                {
                    "error": diagnostic,
                    "mergeable": False,
                    "outcome": outcome,
                    "recognised": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"landing gate: {diagnostic}", file=_sys.stderr)
    return 2


def _snapshot_text(snapshot: Mapping[str, Any] | None) -> str | None:
    if not snapshot:
        return None
    text = snapshot.get("text")
    return text if isinstance(text, str) else None


def _ids_bound_from_text(raw_text: str | None, ids_text: str | None) -> tuple[list[str], str]:
    if raw_text is None:
        return [], "ids_not_measured"
    extracted = extract_failure_ids(raw_text)
    if extracted["outcome"] != "extracted":
        return [], "ids_not_measured"
    extracted_ids = list(extracted["ids"])
    if ids_text is None:
        return [], "ids_not_measured"
    supplied = ids_text.splitlines()
    if normalize_failure_ids(extracted_ids) != normalize_failure_ids(supplied):
        return extracted_ids, "mismatch"
    return extracted_ids, "bound"


def _scope_from_snapshot(snapshot: Mapping[str, Any] | None) -> Any:
    text = _snapshot_text(snapshot)
    if text is None:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return {"pytest_targets": "invalid-json"}


def _gate_artifact_snapshots(args: Any) -> dict[str, dict[str, Any]]:
    snapshots: dict[str, dict[str, Any]] = {}
    for label, raw_path, ids_path, scope_path in (
        ("baseline", args.baseline_raw, args.baseline_ids, args.baseline_scope),
        ("subject", args.subject_raw, args.subject_ids, args.subject_scope),
    ):
        if raw_path is None and ids_path is None and scope_path is None:
            continue
        if raw_path is not None:
            try:
                raw_snap: dict[str, Any] | None = lm.read_file_snapshot(raw_path)
            except OSError as exc:
                raise LandingGateError(f"could not read the {label} pytest log at {raw_path}: {exc}") from exc
        else:
            raw_snap = None
        snapshots[label] = {
            "raw": raw_snap,
            "ids": lm.try_file_snapshot(ids_path),
            "scope": lm.try_file_snapshot(scope_path),
            "receipt": lm.try_file_snapshot(lm.receipt_path_for_raw(raw_path)) if raw_path is not None else None,
            "attempt": lm.try_file_snapshot(lm.attempt_path_for_raw(raw_path)) if raw_path is not None else None,
        }
    return snapshots


def _measurement_with_id_binding(
    args: Any,
    measurement: dict[str, Any],
    snapshots: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    mismatched: list[str] = []
    unmeasured: list[str] = []
    for label, raw_path, ids_path in (
        ("baseline", args.baseline_raw, args.baseline_ids),
        ("subject", args.subject_raw, args.subject_ids),
    ):
        if raw_path is None:
            continue
        side = snapshots.get(label) or {}
        _ids, status = _ids_bound_from_text(
            _snapshot_text(side.get("raw") if isinstance(side.get("raw"), dict) else None),
            _snapshot_text(side.get("ids") if isinstance(side.get("ids"), dict) else None),
        )
        if status == "mismatch":
            mismatched.append(label)
        elif status == "ids_not_measured":
            unmeasured.append(label)
    incomplete = list(measurement.get("incomplete") or [])
    if mismatched:
        return {
            **measurement,
            "outcome": "ids_mismatched",
            "incomplete": incomplete + [item for item in mismatched if item not in incomplete],
        }
    if unmeasured:
        return {
            **measurement,
            "outcome": "incomplete",
            "incomplete": incomplete + [item for item in unmeasured if item not in incomplete],
        }
    return measurement


def _run_extract_ids(path: Path, *, json_mode: bool = False) -> int:
    import sys as _sys

    report = extract_failure_ids(path)
    if report["outcome"] == "extracted":
        for node_id in report["ids"]:
            print(node_id)
        return 0
    if json_mode:
        return _cli_refusal(
            json_mode=True,
            outcome=str(report["outcome"]),
            error=f"extract-ids outcome {report['outcome']} for {path}",
        )
    print(
        f"landing gate: extract-ids outcome {report['outcome']}",
        file=_sys.stderr,
    )
    return 1


def _run_emit_scope(args: Any, parser: Any) -> int:
    import sys as _sys

    if not args.worktree:
        parser.error("the following arguments are required: --worktree")
    payload = build_invocation_scope(
        worktree=args.worktree,
        pytest_targets=args.scope_targets or [],
        pytest_options=args.scope_options or [],
        interpreter=args.python or _sys.executable,
        producer_wrapper=args.producer_wrapper,
        producer_script=Path(__file__),
    )
    try:
        args.emit_scope.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        raise LandingGateError(f"could not write invocation scope at {args.emit_scope}: {exc}") from exc
    return 0


def _run_print_pythonpath(args: Any, parser: Any) -> int:
    if not args.worktree:
        parser.error("the following arguments are required: --worktree")
    print(import_pythonpath(args.worktree))
    return 0


def _query_mode_exit(args: Any, parser: Any) -> int | None:
    if args.extract_ids is not None:
        return _run_extract_ids(args.extract_ids, json_mode=bool(args.json))
    if args.emit_scope is not None:
        return _run_emit_scope(args, parser)
    if args.print_pythonpath:
        return _run_print_pythonpath(args, parser)
    return None


def _run_gate_report(args: Any) -> int:
    import sys as _sys

    interpreter = args.python or _sys.executable
    modules = args.modules or list(DEFAULT_GUARDED_MODULES)
    src_roots = import_path_roots(args.worktree)
    snapshots = _gate_artifact_snapshots(args)
    measurement = _measurement_with_id_binding(
        args,
        measurement_completeness(
            {"baseline": args.baseline_raw, "subject": args.subject_raw},
            ids_logs={"baseline": args.baseline_ids, "subject": args.subject_ids},
            scope_logs={"baseline": args.baseline_scope, "subject": args.subject_scope},
            artifact_snapshots=snapshots,
        ),
        snapshots,
    )
    baseline_side = snapshots.get("baseline") or {}
    subject_side = snapshots.get("subject") or {}
    baseline_ids, baseline_bind = _ids_bound_from_text(
        _snapshot_text(baseline_side.get("raw") if isinstance(baseline_side.get("raw"), dict) else None),
        _snapshot_text(baseline_side.get("ids") if isinstance(baseline_side.get("ids"), dict) else None),
    )
    subject_ids, subject_bind = _ids_bound_from_text(
        _snapshot_text(subject_side.get("raw") if isinstance(subject_side.get("raw"), dict) else None),
        _snapshot_text(subject_side.get("ids") if isinstance(subject_side.get("ids"), dict) else None),
    )
    if baseline_bind != "bound":
        baseline_ids = []
    if subject_bind != "bound":
        subject_ids = []
    subject_text = _snapshot_text(subject_side.get("raw") if isinstance(subject_side.get("raw"), dict) else None)
    subject_executed = subject_bind == "bound" and bool(subject_text) and _suite_executed(subject_text)
    report = gate_report(
        freshness=subject_freshness(args.worktree, args.subject_ref, integration_ref=args.integration_ref),
        environment=environment_consistency(args.worktree, modules, python=interpreter, extra_paths=src_roots),
        measurement=measurement,
        diff=failure_id_diff(
            baseline_ids,
            subject_ids,
            subject_executed=subject_executed,
        ),
        scope=compare_invocation_scopes(
            _scope_from_snapshot(baseline_side.get("scope") if args.baseline_scope is not None else None),
            _scope_from_snapshot(subject_side.get("scope") if args.subject_scope is not None else None),
        ),
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(_human_summary(report))
    return 0 if report["mergeable"] else 1


def _build_parser() -> Any:
    import argparse

    inner = argparse.ArgumentParser(description=__doc__)
    parser = _LandingGateArgumentParser(inner)
    parser.add_argument("--worktree", type=Path, help="the subject checkout")
    # Required for a gate run, but --print-pythonpath / --extract-ids /
    # --emit-scope are pure queries and have no subject; the requirement is
    # enforced below instead of by argparse so those queries do not need a
    # dummy ref.
    parser.add_argument("--subject-ref")
    parser.add_argument("--integration-ref", default="main")
    parser.add_argument("--baseline-ids", type=Path, help="failure ids measured on the baseline")
    parser.add_argument("--subject-ids", type=Path, help="failure ids measured on the subject")
    parser.add_argument(
        "--baseline-raw",
        type=Path,
        help="raw pytest log the baseline ids were extracted from (proves the run finished)",
    )
    parser.add_argument(
        "--subject-raw",
        type=Path,
        help="raw pytest log the subject ids were extracted from (proves the run finished)",
    )
    parser.add_argument(
        "--baseline-scope",
        type=Path,
        help="invocation-scope sidecar produced with the baseline ids",
    )
    parser.add_argument(
        "--subject-scope",
        type=Path,
        help="invocation-scope sidecar produced with the subject ids",
    )
    parser.add_argument("--python", default=None, help="interpreter the gate run will use")
    parser.add_argument(
        "--module",
        action="append",
        dest="modules",
        help=f"guarded module (repeatable); default: {', '.join(DEFAULT_GUARDED_MODULES)}",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--print-pythonpath",
        action="store_true",
        help=(
            "print the canonical import path for --worktree and exit; the id-producing "
            "run consumes this instead of maintaining a second copy of the list"
        ),
    )
    parser.add_argument(
        "--extract-ids",
        type=Path,
        metavar="RAW",
        help="print pytest node ids from a raw log's short-test-summary region and exit",
    )
    parser.add_argument(
        "--emit-scope",
        type=Path,
        metavar="FILE",
        help="write a non-secret invocation-scope sidecar and exit",
    )
    parser.add_argument(
        "--scope-target",
        action="append",
        dest="scope_targets",
        default=None,
        help="pytest target recorded by --emit-scope (repeatable)",
    )
    parser.add_argument(
        "--scope-option",
        action="append",
        dest="scope_options",
        default=None,
        help="pytest option recorded by --emit-scope (repeatable)",
    )
    parser.add_argument(
        "--producer-wrapper",
        type=Path,
        default=None,
        help="executed gate_ids.sh wrapper whose bytes are bound into producer identity",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Exit 0 only when the composite gate says ``mergeable``.

    Exit 1 is a refusal with a named outcome; exit 2 is a probe that could not
    establish a fact.  The two are kept apart on purpose -- "the branch is not
    mergeable" and "the gate could not tell" are different operator actions,
    and collapsing them is the silence-as-success this module exists to remove
    [OBS-08].

    ``--json`` emits a typed ``mergeable=false`` envelope on every invalid CLI
    argument and ``LandingGateError`` [REVIEW-M-06]. Effective argv is
    normalized once so a subprocess ``main(None)`` still honors ``--json``
    [LANDGA-S1C-M02].
    """
    effective = _effective_argv(argv)
    json_mode = _json_requested(effective)
    parser = _build_parser()
    try:
        args = parser.parse_args(effective)
        query_exit = _query_mode_exit(args, parser)
        if query_exit is not None:
            return query_exit
        if not args.worktree:
            parser.error("the following arguments are required: --worktree")
        if not args.subject_ref:
            parser.error("the following arguments are required: --subject-ref")
        return _run_gate_report(args)
    except _UsageError as exc:
        return _cli_refusal(json_mode=json_mode, outcome="unavailable", error=str(exc) or "invalid arguments")
    except LandingGateError as exc:
        return _cli_refusal(json_mode=json_mode, outcome="probe_failed", error=str(exc))


def _scope_human_lines(scope: dict[str, Any]) -> list[str]:
    """Name the scope outcome and, on mismatch, the differing targets/options."""
    lines = [f"scope:       {scope.get('outcome')}"]
    if scope.get("outcome") != "scope_mismatched":
        return lines
    baseline = scope.get("baseline") if isinstance(scope.get("baseline"), dict) else {}
    subject = scope.get("subject") if isinstance(scope.get("subject"), dict) else {}
    base_targets = list(baseline.get("pytest_targets") or [])
    subj_targets = list(subject.get("pytest_targets") or [])
    if base_targets != subj_targets:
        lines.append(f"  targets:   baseline={base_targets} subject={subj_targets}")
    base_opts = _stable_pytest_options(list(baseline.get("pytest_options") or []))
    subj_opts = _stable_pytest_options(list(subject.get("pytest_options") or []))
    if base_opts != subj_opts:
        lines.append(f"  options:   baseline={base_opts} subject={subj_opts}")
    return lines


def _human_summary(report: dict[str, Any]) -> str:
    fresh, env, diff = report["freshness"], report["environment"], report["diff"]
    measure = report["measurement"]
    lines = [
        f"outcome:     {report['outcome']}",
        f"mergeable:   {report['mergeable']}",
        f"recognised:  {report['recognised']}",
        f"freshness:   {fresh.get('outcome')} (behind {fresh.get('behind')})",
        f"environment: {env.get('outcome')}"
        + (f" offenders={','.join(env.get('offenders', []))}" if env.get("offenders") else ""),
        f"measurement: {measure.get('outcome')}"
        + (f" truncated={','.join(measure.get('incomplete', []))}" if measure.get("incomplete") else ""),
        f"diff:        {diff.get('outcome')} new={len(diff.get('new', []))} "
        f"fixed={len(diff.get('fixed', []))} collapsed={diff.get('collapsed')}",
    ]
    if report.get("scope") is not None:
        lines.extend(_scope_human_lines(report["scope"]))
    for rel in diff.get("new", []):
        lines.append(f"  NEW   {rel}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
