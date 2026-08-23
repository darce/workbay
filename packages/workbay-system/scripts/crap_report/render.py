"""Render CrapReport to JSON-serializable dict and Markdown."""

from __future__ import annotations

from crap_report.models import CrapReport, MethodScore


def to_json_dict(report: CrapReport) -> dict:
    """Schema version 1 payload."""

    def _row(m: MethodScore) -> dict:
        return {
            "file": m.file,
            "name": m.name,
            "line_start": m.line_start,
            "line_end": m.line_end,
            "comp": m.comp,
            "cov": m.cov,
            "crap": m.crap,
            "coverage_unknown": m.coverage_unknown,
            "coverage_status": m.coverage_status,
            "excluded": m.excluded,
        }

    return {
        "schema_version": report.schema_version,
        "advisory": True,
        "formula": report.formula,
        "threshold": report.threshold,
        "coverage_kind": report.coverage_kind,
        "provenance": report.provenance,
        "summary": report.summary,
        "methods": [_row(m) for m in report.methods],
        "unmeasured_high_cc": [_row(m) for m in report.unmeasured_high_cc],
    }


def _table(rows: list[MethodScore], *, threshold: float, score_col: str = "CRAP") -> list[str]:
    lines = [
        f"| Rank | {score_col} | CC | Cov% | File | Method | Lines | Flags |",
        "| ---: | ---: | ---: | ---: | --- | --- | ---: | --- |",
    ]
    for i, m in enumerate(rows, 1):
        flags: list[str] = []
        if m.coverage_status != "measured":
            flags.append(m.coverage_status)
        if m.coverage_unknown:
            flags.append("unknown_cov")
        if m.crap > threshold and not m.coverage_unknown:
            flags.append("crappy")
        if m.excluded:
            flags.append("excluded")
        flag_s = ",".join(flags) if flags else ""
        val = m.comp if score_col == "CC" else m.crap
        lines.append(
            f"| {i} | {val:.2f} | {m.comp} | {m.cov:.1f} | `{m.file}` | `{m.name}` "
            f"| {m.line_start}-{m.line_end} | {flag_s} |"
        )
    return lines


def to_markdown_table(report: CrapReport, *, top_n: int | None = 50) -> str:
    """Human-readable ranked table; unmeasured high-CC is a separate section."""
    total_methods = len(report.methods)
    rows = report.methods
    if top_n is not None:
        rows = rows[:top_n]
    s = report.summary
    header = (
        f"Methods ranked: **{s['methods']}** · CRAPpy: **{s['crappy']}** · "
        f"unmeasured omitted: **{s.get('unmeasured_omitted', 0)}**"
    )
    if len(rows) < total_methods:
        header += f" · showing {len(rows)} of {total_methods}"
    lines = [
        f"# CRAP report (threshold={report.threshold}, kind={report.coverage_kind})",
        "",
        header,
        "",
    ]
    if report.unmeasured_high_cc:
        lines.append(
            "> **Note:** methods outside the coverage report are **not** scored as "
            "0% coverage (that flooded rankings in dogfood). They appear under "
            "*Unmeasured high-CC* as pure complexity hints. Pass "
            "`--include-unmeasured` for classic CRAP(cov=0) behavior."
        )
        lines.append("")
    lines.append("## Measured change-risk ranking")
    lines.append("")
    if not rows:
        lines.append("_No measured methods._")
    else:
        lines.extend(_table(rows, threshold=report.threshold, score_col="CRAP"))
    if report.unmeasured_high_cc:
        total_um = len(report.unmeasured_high_cc)
        um = report.unmeasured_high_cc
        if top_n is not None:
            um = um[:top_n]
        lines.append("")
        um_heading = "## Unmeasured high-CC (informational)"
        if len(um) < total_um:
            um_heading += f" (showing {len(um)} of {total_um})"
        lines.append(um_heading)
        lines.append("")
        lines.append(
            "These were not in the coverage JSON (or had empty ranges). "
            "Score column is **CC only**, not CRAP."
        )
        lines.append("")
        lines.extend(_table(um, threshold=report.threshold, score_col="CC"))
    lines.append("")
    lines.append(
        "_Advisory ranking only — not a merge gate. "
        "See heuristics-canon [TEST-11] / proxy-outcome-integrity._"
    )
    return "\n".join(lines) + "\n"
