"""Installed WorkBay entry point for deterministic canon checks."""

from __future__ import annotations

import argparse
import difflib
import os
import re
import sqlite3
import sys
from pathlib import Path

from workbay_protocol.canon_lint import CanonDecisionLintReport, CanonLintError, CanonLintReport
from workbay_protocol.cli_contract import ExitCode, emit_json

from . import engine
from .adapters.decision import lint_decision_text
from .canon_loader import CanonLoadError
from .config import CONFIG_PATH, load_config, update_pin, verify_bump_target

_CANON_ROOT_ENV = "WORKBAY_HEURISTICS_CANON_ROOT"
_PUBLISHED_CANON = Path("Development/heuristics-canon-current")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="workbay canon")
    verbs = parser.add_subparsers(dest="command", required=True)
    for verb, help_text in (
        ("lint", "Check workspace files using the repository's configured adapters"),
        ("check-drift", "Verify the release manifest and the registered rule rows"),
    ):
        subparser = verbs.add_parser(verb, help=help_text)
        subparser.add_argument("--workspace-root", type=Path, default=Path.cwd())
        subparser.add_argument("--canon-root", type=Path)
        subparser.add_argument("--json", action="store_true", help="Emit a schema-validated report")
        if verb == "lint":
            selection = subparser.add_mutually_exclusive_group()
            selection.add_argument("--files", nargs="+", metavar="PATH", help="Scan only these workspace-relative files")
            selection.add_argument("--staged", action="store_const", const="staged", dest="mode",
                                   help="Scan staged changes as stored in the Git index")
            selection.add_argument("--changed", action="store_const", const="changed", dest="mode",
                                   help="Scan staged, unstaged and untracked changes in the workspace")
            subparser.set_defaults(mode="tracked")
            subparser.add_argument("--quiet", action="store_true", help="Suppress clean results")
    decision = verbs.add_parser("lint-decision", help="Check decision text for policy advisories")
    decision.add_argument("--decision", required=True)
    decision.add_argument("--rationale", default="")
    decision.add_argument("--files", nargs="+", metavar="PATH", help="Associated paths for context")
    decision.add_argument("--json", action="store_true", help="Emit a schema-validated report")
    decision.add_argument("--quiet", action="store_true", help="Suppress clean results")
    bump = verbs.add_parser("bump", help="Verify a published release and update the repository pin")
    bump.add_argument("--workspace-root", type=Path, default=Path.cwd())
    bump.add_argument("--canon-root", type=Path, help=f"Candidate release (default: ~/{_PUBLISHED_CANON})")
    bump.add_argument("--json", action="store_true", help="Emit a structured pin update receipt")
    metrics = verbs.add_parser("reversal-metrics", help="Report Canon-triggered decision reversal metrics from handoff.db")
    metrics.add_argument("--workspace-root", type=Path, default=Path.cwd())
    metrics.add_argument("--db-path", type=Path, help="Path to handoff.db (default: <workspace-root>/.task-state/handoff.db)")
    metrics.add_argument("--task-ref", help="Filter metrics to a single task_ref")
    metrics.add_argument("--json", action="store_true", help="Emit structured JSON output")
    return parser


def _canon_root(root: Path, explicit: Path | None, configured: str | None, version: str) -> Path:
    if explicit is not None:
        return explicit.expanduser()
    if configured is not None:
        return root / Path(configured).expanduser()
    environment = os.environ.get(_CANON_ROOT_ENV)
    if environment is not None:
        if not environment:
            raise engine.CanonRunError("canon_root_unavailable", f"{_CANON_ROOT_ENV} is empty")
        return Path(environment).expanduser()
    current = Path.home() / _PUBLISHED_CANON
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        pinned = current.with_name(f"heuristics-canon-v{version}")
        try:
            if pinned.is_dir():
                return pinned
        except PermissionError as exc:
            raise CanonLoadError(
                "canon_root_permission_denied", "Permission denied probing the pinned Canon root", path=str(pinned)
            ) from exc
    return current


def _emit_report(report: CanonLintReport, as_json: bool) -> None:
    if as_json:
        emit_json(report.model_dump(mode="json", by_alias=True))
        return
    if not report.ok:
        return
    if report.command == "check-drift":
        print(f"Canon {report.canon_version}: verified {report.checks_checked} registered checks")
        return
    for finding in report.findings:
        print(f"{finding.path}:{finding.line}: {finding.tier} {finding.rule_id} {finding.evidence_snippet}")
    print(f"{report.files_checked} files checked, {len(report.findings)} findings, {report.exempted_count} exempted")


def _lint_decision(args: argparse.Namespace) -> int:
    report = CanonDecisionLintReport(
        ok=True, advisories=lint_decision_text(args.decision, args.rationale),
    )
    if report.advisories or not args.quiet:
        if args.json:
            emit_json(report.model_dump(mode="json"))
        else:
            for advisory in report.advisories:
                print(f"{advisory.tier} {advisory.rule_id}: {advisory.message} ({advisory.matched_signal})")
            print(f"{len(report.advisories)} advisories")
    return int(ExitCode.OK)


def _reversal_metrics(args: argparse.Namespace) -> int:
    db_path = args.db_path.expanduser() if args.db_path is not None else (args.workspace_root / ".task-state/handoff.db")
    if not db_path.is_file():
        payload = {"ok": False, "command": "reversal-metrics", "error": f"handoff database not found: {db_path}"}
        if args.json:
            emit_json(payload)
        else:
            print(f"workbay canon: {payload['error']}", file=sys.stderr)
        return int(ExitCode.POLICY_REFUSED)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(decisions)").fetchall()}
        if not {"supersedes_decision_id", "reversal_cause"} <= cols:
            total = int(conn.execute(
                "SELECT COUNT(*) FROM decisions WHERE (? IS NULL OR task_ref = ?)",
                (args.task_ref, args.task_ref),
            ).fetchone()[0])
            metrics = {
                "total_decisions": total,
                "reversals_total": 0,
                "canon_reversals": 0,
                "canon_reversal_rate": 0.0,
                "by_rule_id": {},
            }
        else:
            groups = conn.execute(
                """
                SELECT CASE
                           WHEN supersedes_decision_id IS NOT NULL
                                AND LOWER(reversal_cause) LIKE 'canon:%'
                                AND LENGTH(TRIM(substr(reversal_cause, 7))) > 0
                           THEN UPPER(TRIM(substr(reversal_cause, 7)))
                       END AS rule_id,
                       COUNT(*) AS decisions_total,
                       COUNT(supersedes_decision_id) AS reversals_total
                FROM decisions
                WHERE (? IS NULL OR task_ref = ?)
                GROUP BY rule_id
                """,
                (args.task_ref, args.task_ref),
            )
            total_decisions = reversals_total = 0
            by_rule_id: dict[str, int] = {}
            for rule_id, decisions_count, reversals_count in groups:
                total_decisions += decisions_count
                reversals_total += reversals_count
                if rule_id is not None:
                    by_rule_id[rule_id] = decisions_count
            canon_reversals = sum(by_rule_id.values())
            metrics = {
                "total_decisions": total_decisions,
                "reversals_total": reversals_total,
                "canon_reversals": canon_reversals,
                "canon_reversal_rate": canon_reversals / total_decisions if total_decisions else 0.0,
                "by_rule_id": by_rule_id,
            }
    finally:
        conn.close()
    payload = {"ok": True, "command": "reversal-metrics", "task_ref": args.task_ref, **metrics}
    if args.json:
        emit_json(payload)
    else:
        scope = args.task_ref or "all tasks"
        print(
            f"Canon reversal metrics ({scope}): {metrics['canon_reversals']} canon reversals / "
            f"{metrics['reversals_total']} total reversals / {metrics['total_decisions']} decisions "
            f"({metrics['canon_reversal_rate']:.2%})"
        )
        for rule_id, count in sorted(metrics["by_rule_id"].items()):
            print(f"  {rule_id}: {count}")
    return int(ExitCode.OK)


def _bump(args: argparse.Namespace) -> int:
    receipt = {"schema": "workbay-canon-lint/bump@1", "command": "bump", "path": CONFIG_PATH.as_posix()}
    try:
        root = engine.workspace_root(args.workspace_root)
        canon_root = args.canon_root.expanduser() if args.canon_root is not None else Path.home() / _PUBLISHED_CANON
        release = verify_bump_target(canon_root)
        previous, before, after = update_pin(root, release)
    except (CanonLoadError, engine.CanonRunError) as exc:
        path = getattr(exc, "path", None)
        print(f"workbay canon: {exc.code}: {exc}" + (f" ({path})" if path else ""), file=sys.stderr)
        if args.json:
            error = CanonLintError(code=exc.code, message=str(exc), path=path if path and path.strip() else None)
            emit_json({**receipt, "ok": False, "errors": [error.model_dump(mode="json")]})
        return int(ExitCode.POLICY_REFUSED)
    if args.json:
        emit_json({
            **receipt,
            "ok": True,
            "changed": before != after,
            "previous": {"canon_version": previous.canon_version, "manifest_sha256": previous.manifest_sha256},
            "current": {"canon_version": release.version, "manifest_sha256": release.manifest_sha256},
        })
    elif before != after:
        for line in difflib.unified_diff(before.splitlines(), after.splitlines(),
                                         f"a/{CONFIG_PATH}", f"b/{CONFIG_PATH}", lineterm=""):
            print(line)
    else:
        print(f"Canon {release.version} is already pinned in {CONFIG_PATH}")
    return int(ExitCode.OK)


def canon_main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "lint-decision":
        return _lint_decision(args)
    if args.command == "reversal-metrics":
        return _reversal_metrics(args)
    if args.command == "bump":
        return _bump(args)
    try:
        root = engine.workspace_root(args.workspace_root)
        config = load_config(root)
        canon_root = _canon_root(root, args.canon_root, config.canon_root, config.canon_version)
        code, report = engine.run(
            args.command, root, canon_root, config,
            mode=getattr(args, "mode", "tracked"), files=getattr(args, "files", None),
        )
    except (CanonLoadError, engine.CanonRunError) as exc:
        print(f"workbay canon: {exc.code}: {exc}", file=sys.stderr)
        path = getattr(exc, "path", None)
        report = CanonLintReport(
            command=args.command,
            ok=False,
            errors=[CanonLintError(code=exc.code, message=str(exc), path=path if path and path.strip() else None)],
        )
        code = ExitCode.POLICY_REFUSED
    if not (getattr(args, "quiet", False) and report.ok and not report.findings):
        _emit_report(report, args.json)
    return int(code)
