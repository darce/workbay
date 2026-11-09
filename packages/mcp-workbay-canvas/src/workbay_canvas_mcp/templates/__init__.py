"""Canvas template registry (internal).

A template is a pure ``(TemplateInput) -> Canvas`` function plus a
:class:`TemplateSpec` registry entry. :func:`build_template_canvas` is the
pure core the ``instantiate_template`` MCP tool wraps with persistence:
it looks up the template, validates the raw input against the registered
Pydantic model, and calls the template's ``instantiate``.

Ship-with templates register themselves at import time at the bottom of
this module. New templates add a module under this package and one
``register_template`` call here.
"""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field, ValidationError, computed_field

from workbay_canvas_mcp.models import Canvas


class TemplateError(Exception):
    """Structured error raised by :func:`build_template_canvas`.

    ``reason`` is a stable machine code (``"unknown_template"`` or
    ``"invalid_input"``). ``details`` carries the pydantic validation
    error list for ``invalid_input``, else ``None``.
    """

    def __init__(self, reason: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.details = details


class TemplateSpec(BaseModel):
    """A registry entry binding a ``template_id`` to its model and builder.

    ``input_model``, ``instantiate``, and ``seed`` are excluded from
    serialization so ``list_templates`` returns the safe ``{template_id,
    description, input_model_name}`` projection over MCP without leaking
    Python types or callables.

    ``seed`` is the optional opt-in side-effecting hook a template may
    expose to seed durable design-system state (roster components, a
    design-document entry) from a validated input. It is invoked by the
    ``instantiate_template`` tool only when the caller passes
    ``seed=True`` (see :func:`run_template_seed`); the pure
    ``instantiate`` path never touches I/O. The second and third
    arguments are the ``CanvasStore`` and stored Canvas (with the store
    typed ``Any`` here to keep the registry free of a store import).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    template_id: str
    description: str
    input_model: type[BaseModel] = Field(exclude=True, repr=False)
    instantiate: Callable[[Any], Canvas] = Field(exclude=True, repr=False)
    seed: Callable[[Any, Any, Canvas], None] | None = Field(default=None, exclude=True, repr=False)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def input_model_name(self) -> str:
        return self.input_model.__name__


TEMPLATE_REGISTRY: dict[str, TemplateSpec] = {}


def register_template(spec: TemplateSpec) -> None:
    """Register ``spec`` under its ``template_id`` (last write wins)."""

    TEMPLATE_REGISTRY[spec.template_id] = spec


def list_templates() -> list[TemplateSpec]:
    """Return every registered template spec, sorted by ``template_id``."""

    return [TEMPLATE_REGISTRY[key] for key in sorted(TEMPLATE_REGISTRY)]


def _resolve(template_id: str, input: dict[str, Any]) -> tuple[TemplateSpec, BaseModel]:
    """Look up ``template_id`` and validate ``input`` against its model.

    Raises :class:`TemplateError` with ``reason="unknown_template"`` for an
    unregistered ``template_id`` and ``reason="invalid_input"`` (carrying
    the pydantic error list in ``details``) when ``input`` fails model
    validation.
    """

    spec = TEMPLATE_REGISTRY.get(template_id)
    if spec is None:
        raise TemplateError("unknown_template", f"unknown template_id: {template_id!r}")

    try:
        model = spec.input_model.model_validate(input)
    except ValidationError as exc:
        raise TemplateError(
            "invalid_input",
            f"invalid input for template {template_id!r}",
            details=exc.errors(),
        ) from exc

    return spec, model


def build_template_canvas(
    template_id: str,
    input: dict[str, Any],
    canvas_ref_override: str | None = None,
) -> Canvas:
    """Render a registered template to a :class:`Canvas` (pure, no I/O).

    Raises :class:`TemplateError` as documented on :func:`_resolve`. When
    ``canvas_ref_override`` is given, it replaces the template's default
    ``canvas_ref`` on the produced Canvas.
    """

    spec, model = _resolve(template_id, input)
    canvas = spec.instantiate(model)
    if canvas_ref_override is not None:
        canvas = canvas.model_copy(update={"canvas_ref": canvas_ref_override})
    return canvas


def run_template_seed(template_id: str, input: dict[str, Any], store: Any, canvas: Canvas) -> None:
    """Invoke the template's opt-in seed hook against ``store`` if present.

    Resolves and re-validates ``input`` (the same loud ``TemplateError``
    contract as :func:`build_template_canvas`), then calls the spec's
    ``seed`` hook when one is registered. A template without a seed hook
    is a no-op, so callers can request seeding uniformly regardless of
    template. The hook owns its own idempotency and must not clobber
    existing design-document fields (it augments, never rewrites).
    """

    spec, model = _resolve(template_id, input)
    if spec.seed is not None:
        spec.seed(model, store, canvas)


def _register_builtin_templates() -> None:
    from workbay_canvas_mcp.templates import lane_dependency, scope_intake, user_flow

    register_template(
        TemplateSpec(
            template_id=scope_intake.TEMPLATE_ID,
            description=scope_intake.DESCRIPTION,
            input_model=scope_intake.ScopeIntakeInput,
            instantiate=scope_intake.instantiate,
            seed=scope_intake.seed,
        )
    )
    register_template(
        TemplateSpec(
            template_id=lane_dependency.TEMPLATE_ID,
            description=lane_dependency.DESCRIPTION,
            input_model=lane_dependency.LaneDependencyInput,
            instantiate=lane_dependency.instantiate,
            seed=lane_dependency.seed,
        )
    )
    register_template(
        TemplateSpec(
            template_id=user_flow.TEMPLATE_ID,
            description=user_flow.DESCRIPTION,
            input_model=user_flow.UserFlowInput,
            instantiate=user_flow.instantiate,
            seed=None,
        )
    )


_register_builtin_templates()
