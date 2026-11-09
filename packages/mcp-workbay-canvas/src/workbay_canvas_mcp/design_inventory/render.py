"""Deterministic Markdown for an InventoryDocument (WBC-03 S8)."""

from __future__ import annotations

from workbay_canvas_mcp.design_inventory.models import InventoryDocument, TokenResolvedUnsupported


def render_inventory_markdown(doc: InventoryDocument) -> str:
    lines: list[str] = [
        "# Design inventory",
        "",
        f"- product: `{_cell(doc.product)}`",
        f"- source_digest: `{_cell(doc.source_digest)}`",
        "",
        "## Coverage",
        "",
        "| key | value |",
        "| --- | --- |",
    ]
    coverage = doc.coverage.model_dump(mode="json")
    for key, value in coverage.items():
        if key == "limitations":
            continue
        lines.append(f"| `{_cell(str(key))}` | {_cell(str(value))} |")
    lines.extend(["", "## Limitations", ""])
    for item in doc.coverage.limitations:
        lines.append(f"- `{_cell(item)}`")

    lines.extend(
        [
            "",
            "## Tokens",
            "",
            "| name | context | expression | status | resolved |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for token in doc.tokens:
        lines.append(
            "| `{name}` | `{context}` | `{expression}` | `{status}` | `{resolved}` |".format(
                name=_cell(token.name),
                context=_cell(token.context),
                expression=_cell(token.expression),
                status=_cell(token.status),
                resolved=_cell(_resolved_label(token.resolved)),
            )
        )

    lines.extend(
        [
            "",
            "## Components",
            "",
            "| id | name | export | status | token_refs | unresolved_token_refs |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for component in doc.components:
        export = f"{component.export_ref.export_name}@{component.export_ref.path}"
        lines.append(
            "| `{id}` | `{name}` | `{export}` | `{status}` | `{refs}` | `{unresolved}` |".format(
                id=_cell(component.id),
                name=_cell(component.name),
                export=_cell(export),
                status=_cell(component.export_ref.status),
                refs=_cell(",".join(component.token_refs)),
                unresolved=_cell(",".join(component.unresolved_token_refs)),
            )
        )

    lines.extend(
        [
            "",
            "## Assets",
            "",
            "| path | status | mime | digest | license |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for asset in doc.assets:
        lines.append(
            "| `{path}` | `{status}` | `{mime}` | `{digest}` | `{license}` |".format(
                path=_cell(asset.path),
                status=_cell(asset.status),
                mime=_cell(asset.mime),
                digest=_cell(asset.digest or ""),
                license=_cell(asset.license),
            )
        )

    lines.extend(
        [
            "",
            "## Compiled CSS",
            "",
            "| output | status | reason | stale_paths |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in doc.compiled_css_freshness:
        lines.append(
            "| `{output}` | `{status}` | `{reason}` | `{stale}` |".format(
                output=_cell(row.output),
                status=_cell(row.status),
                reason=_cell("" if row.reason is None else row.reason),
                stale=_cell(",".join(row.stale_paths)),
            )
        )

    lines.extend(
        [
            "",
            "## Source digests",
            "",
            "| path | digest |",
            "| --- | --- |",
        ]
    )
    for row in doc.source_digests:
        lines.append(f"| `{_cell(row.path)}` | `{_cell(row.digest)}` |")

    return "\n".join(lines).rstrip("\n") + "\n"


def _resolved_label(resolved: object) -> str:
    if isinstance(resolved, TokenResolvedUnsupported):
        return f"unsupported:{resolved.reason}"
    kind = getattr(resolved, "kind", "")
    value = getattr(resolved, "value", "")
    return f"{kind}:{value}"


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").replace("\r", "")
