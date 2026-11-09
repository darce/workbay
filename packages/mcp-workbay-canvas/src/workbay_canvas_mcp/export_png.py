"""PNG export for canvas-mcp.

internal deliverable. Composes the Slice-2 first-party SVG
emitter with an external SVG-to-PNG rasteriser so the PNG arm of
``export_canvas`` reuses one source of truth for layout and styling.

The rasteriser is intentionally pluggable:

- Production calls leave ``converter=None`` and pick up the default
  cairosvg backend, which ships behind the ``export-png`` optional
  extra of this package. Canvas is its own uv project (not a root
  workspace member), so in a WorkBay git checkout run
  ``uv sync --locked --extra export-png`` from the Canvas package directory.
- When cairosvg is unavailable, :func:`_load_default_converter` raises
  :class:`~workbay_canvas_mcp.export.ExportError` with stable reason
  ``"missing_dependency"`` and an actionable detail string — never a
  raw ``ImportError`` stack trace.
- Tests inject their own ``converter`` callable so the PNG path can be
  exercised without the native cairo library.

Layout requirements flow through :func:`to_svg` unchanged: an unplaced
node raises ``ExportError(reason="layout_required")`` before the
converter is ever invoked, so missing-layout failures stay distinct
from missing-backend failures at the reason-token level.
"""

from __future__ import annotations

from typing import Callable, Final

from workbay_canvas_mcp.export import ExportError
from workbay_canvas_mcp.export_svg import to_svg
from workbay_canvas_mcp.models import Canvas

PNG_CONTENT_TYPE: Final = "image/png"
PNG_MAGIC: Final = b"\x89PNG\r\n\x1a\n"

SvgToPngConverter = Callable[[bytes], bytes]

_MISSING_DEP_DETAIL: Final = (
    "install the Canvas package's 'export-png' extra to enable PNG export: "
    "in a WorkBay git checkout, run `uv sync --locked --extra export-png` from "
    "the Canvas package directory (its own uv project and lockfile), then run Canvas "
    "with that directory's .venv (cairosvg requires native libcairo)"
)


def _load_default_converter() -> SvgToPngConverter:
    """Return the default cairosvg-backed SVG-to-PNG converter.

    Raises :class:`ExportError` with reason ``"missing_dependency"`` if
    cairosvg cannot be imported. Translating the ``ImportError`` here
    keeps the stable reason token at the boundary so callers never see
    a backend-specific exception.
    """

    try:
        import cairosvg  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ExportError(
            reason="missing_dependency",
            detail=_MISSING_DEP_DETAIL,
        ) from exc

    def convert(svg_bytes: bytes) -> bytes:
        result = cairosvg.svg2png(bytestring=svg_bytes)
        if not isinstance(result, bytes):  # cairosvg returns bytes by default
            raise ExportError(
                reason="missing_dependency",
                detail="cairosvg.svg2png returned non-bytes output",
            )
        return result

    return convert


def to_png(canvas: Canvas, *, converter: SvgToPngConverter | None = None) -> bytes:
    """Render a Canvas to PNG bytes.

    ``to_svg(canvas)`` is invoked first so ``layout_required`` raises
    before the converter is loaded — separating "the canvas is not
    renderable" from "the backend is not installed" at the reason-token
    level. The SVG output is UTF-8 encoded and handed to ``converter``,
    which defaults to :func:`_load_default_converter`.
    """

    svg_text = to_svg(canvas)
    convert = converter if converter is not None else _load_default_converter()
    return convert(svg_text.encode("utf-8"))


__all__ = [
    "PNG_CONTENT_TYPE",
    "PNG_MAGIC",
    "SvgToPngConverter",
    "to_png",
]
