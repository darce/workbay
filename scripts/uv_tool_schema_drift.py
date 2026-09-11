#!/usr/bin/env python3
"""Detect HANDOFF_SCHEMA_VERSION skew between installed uv tools and in-tree source.

Installed ``uv tool`` environments (``~/.local/share/uv/tools/<name>/`` or
``$UV_TOOL_DIR``) vendor their own copy of ``workbay_handoff_mcp``. When the
in-tree ``HANDOFF_SCHEMA_VERSION`` advances, those tools silently lag and every
handoff-touching subprocess they spawn can fail closed with
``SchemaVersionMismatchError`` — while the root ``.venv`` (and every existing
gate that probes it) still looks healthy. The Makefile documents that gap as
REV-A-002; this detector closes it.

Also detects ``MIN_COMPATIBLE_READER_VERSION`` skew and absence: a tool can
match the schema constant while carrying a missing or wrong reader floor, so
both constants are extracted via :mod:`ast` and reported distinctly.

Reinstall argv preserves package extras recorded in each tool's
``uv-receipt.toml`` so repair cannot silently strip capabilities (e.g.
``embeddings``). A missing or unparseable receipt refuses reinstall rather
than emitting an extras-free command [OBS-08].

Stdlib-only. Does not import workbay packages. Reads schema versions by parsing
``shared_schema.py`` with :mod:`ast` so a broken / partial install cannot
contaminate the probe.

Feeds:
* ``--check`` — fail-fast gate (exit 1 on any skew)
* ``--print-drifted-names`` — ``make sync-uv-tool-schema-drift`` reinstall loop
* doctor ``_doctor_uv_tool_schema_skew`` (loads this module from the monorepo
  target, same isolated-spec pattern as ``version_of_drift``)
"""

from __future__ import annotations

import ast
import os
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Literal

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from workspace_members import repo_root  # noqa: E402

# Relative path of the in-tree schema sentinel. Joined to repo_root only — never
# re-prefixed with packages/ (same double-prefix trap version_of_drift guards).
_IN_TREE_SCHEMA_RELPATH = (
    "packages/mcp-workbay-handoff/src/workbay_handoff_mcp/shared_schema.py"
)

# Workspace packages that can be reinstalled as uv tools from the working tree.
# Keep aligned with workbay_bootstrap.gitonly_closure.GITONLY_CLOSURE_PACKAGES
# (plus nothing extra): the reinstall loop only force-reinstalls these, and only
# when packages/<name>/pyproject.toml exists.
_REINSTALLABLE_TOOL_PACKAGES: frozenset[str] = frozenset(
    {
        "workbay-protocol",
        "mcp-workbay-handoff",
        "mcp-workbay-orchestrator",
        "workbay-bootstrap",
        "workbay-system",
        "workbay",
    }
)

# Runtime closure members pulled via --with on every tool reinstall so
# [tool.uv.sources] workspace pins resolve under --no-sources. Mirror of
# gitonly_closure.GITONLY_RUNTIME_MEMBERS; keep in lockstep.
_RUNTIME_WITH_MEMBERS: tuple[str, ...] = (
    "workbay-protocol",
    "mcp-workbay-handoff",
    "mcp-workbay-orchestrator",
    "workbay-bootstrap",
    "workbay-system",
)

# Orchestrator-only git-sourced extra (gitonly_closure.GITONLY_PACKAGE_EXTRA_MEMBERS).
_ORCHESTRATOR_EXTRA_MEMBERS: tuple[str, ...] = ("workbay-codex-bridge",)

_RECEIPT_NAME = "uv-receipt.toml"

# Floor status strings (stable; asserted by tests).
FLOOR_MATCH = "match"
FLOOR_DIFFER = "differ"
FLOOR_ABSENT = "absent"

FloorStatus = Literal["match", "differ", "absent"]

# Schema fields: (HANDOFF_SCHEMA_VERSION, MIN_COMPATIBLE_READER_VERSION).
# Either element is None when that assignment is missing. Both None means the
# file is missing, unreadable, or unparsable.
SchemaFields = tuple[int | None, int | None]
FieldsReader = Callable[[Path], SchemaFields]
# Legacy single-constant reader (schema only); still accepted via adaptors.
SchemaReader = Callable[[Path], "int | None"]
ToolsRootResolver = Callable[[], Path]
ExtrasByPackage = Mapping[str, list[str]]
ExtrasLoader = Callable[[str], ExtrasByPackage]


class ReceiptUnavailableError(RuntimeError):
    """uv-receipt.toml missing, unreadable, or malformed; refuse extras-stripping reinstall.

    Losing optional capabilities (e.g. embeddings) silently is worse than
    refusing to act [OBS-08]. Callers must not fall back to an extras-free argv.
    """


class ProbeUnavailableError(RuntimeError):
    """The drift probe could not determine an answer.

    Distinct from "no drift": callers must not treat this as clean [OBS-08].
    """


def default_tools_root() -> Path:
    """uv's tool install directory.

    Honours ``UV_TOOL_DIR`` (uv's own override), else
    ``$XDG_DATA_HOME/uv/tools``, else ``~/.local/share/uv/tools``. Matches
    ``uv tool dir`` resolution so hermetic tests can pin a temp root the same
    way production installs do.
    """
    env = os.environ.get("UV_TOOL_DIR")
    if env:
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg).expanduser() / "uv" / "tools"
    return Path.home() / ".local" / "share" / "uv" / "tools"


def tool_receipt_path(tools_root: Path, package: str) -> Path:
    """Path to ``uv-receipt.toml`` for an installed tool under *tools_root*."""
    return tools_root / package / _RECEIPT_NAME


def read_receipt_extras(receipt_path: Path) -> dict[str, list[str]]:
    """Map requirement name -> extras list from a tool's ``uv-receipt.toml``.

    Raises :class:`ReceiptUnavailableError` when the receipt is missing,
    unreadable, or does not contain a parseable ``tool.requirements`` array.
    Absent ``extras`` on a requirement means no extras (empty list), which is
    distinct from an unavailable receipt.
    """
    if not receipt_path.is_file():
        raise ReceiptUnavailableError(
            f"uv receipt unavailable: missing file {receipt_path}"
        )
    try:
        raw = receipt_path.read_bytes()
    except OSError as exc:
        raise ReceiptUnavailableError(
            f"uv receipt unavailable: unreadable {receipt_path}: {exc}"
        ) from exc
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ReceiptUnavailableError(
            f"uv receipt unavailable: malformed TOML {receipt_path}: {exc}"
        ) from exc
    tool = data.get("tool")
    if not isinstance(tool, dict):
        raise ReceiptUnavailableError(
            f"uv receipt unavailable: missing [tool] table in {receipt_path}"
        )
    requirements = tool.get("requirements")
    if not isinstance(requirements, list):
        raise ReceiptUnavailableError(
            f"uv receipt unavailable: missing tool.requirements array in {receipt_path}"
        )
    extras_map: dict[str, list[str]] = {}
    for index, entry in enumerate(requirements):
        if not isinstance(entry, dict):
            raise ReceiptUnavailableError(
                f"uv receipt unavailable: requirements[{index}] is not a table "
                f"in {receipt_path}"
            )
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise ReceiptUnavailableError(
                f"uv receipt unavailable: requirements[{index}] missing string "
                f"name in {receipt_path}"
            )
        extras_val = entry.get("extras", [])
        if extras_val is None:
            extras_val = []
        if not isinstance(extras_val, list) or not all(
            isinstance(item, str) for item in extras_val
        ):
            raise ReceiptUnavailableError(
                f"uv receipt unavailable: requirements[{index}].extras must be "
                f"an array of strings in {receipt_path}"
            )
        extras_map[name] = list(extras_val)
    return extras_map


def path_spec_with_extras(directory: Path, extras: list[str] | None) -> str:
    """Format a path-based requirement, attaching extras when present.

    uv accepts ``/path/to/pkg[extra1,extra2]`` for path requirements with
    extras. Empty or missing extras yields the bare path.
    """
    path_str = str(directory)
    if not extras:
        return path_str
    return f"{path_str}[{','.join(extras)}]"


def read_schema_fields(path: Path) -> SchemaFields:
    """Parse schema and reader-floor integer constants from shared_schema.py.

    Returns ``(HANDOFF_SCHEMA_VERSION, MIN_COMPATIBLE_READER_VERSION)``. Either
    element is ``None`` when that assignment is absent or not a simple integer
    constant. Both ``None`` when the file is missing, unreadable, or
    unparsable. Never imports the module.

    Callers distinguish floor ABSENT (schema is an int, floor is None) from an
    unreadable file (both None) and from a floor that merely differs (both
    ints, unequal).
    """
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return (None, None)
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return (None, None)
    schema: int | None = None
    floor: int | None = None
    wanted = {
        "HANDOFF_SCHEMA_VERSION": "schema",
        "MIN_COMPATIBLE_READER_VERSION": "floor",
    }
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            which = wanted.get(target.id)
            if which is None:
                continue
            value = node.value
            if isinstance(value, ast.Constant) and isinstance(value.value, int):
                if which == "schema":
                    schema = value.value
                else:
                    floor = value.value
    return (schema, floor)


def read_schema_version(path: Path) -> int | None:
    """Parse ``HANDOFF_SCHEMA_VERSION = <int>`` from a shared_schema.py path.

    Returns ``None`` when the file is missing, unreadable, unparsable, or the
    assignment is not a simple integer constant. Never imports the module.
    """
    schema, _floor = read_schema_fields(path)
    return schema


def floor_status(installed_floor: int | None, in_tree_floor: int | None) -> FloorStatus:
    """Classify installed reader floor relative to in-tree.

    * ``absent`` — vendored module defines no MIN_COMPATIBLE_READER_VERSION
    * ``differ`` — both present and unequal
    * ``match`` — both present and equal, or in-tree floor also absent
    """
    if installed_floor is None:
        return FLOOR_ABSENT
    if in_tree_floor is None:
        # In-tree has no floor to compare; installed value is not a skew signal.
        return FLOOR_MATCH
    if installed_floor != in_tree_floor:
        return FLOOR_DIFFER
    return FLOOR_MATCH


def in_tree_schema_path(repo: Path | None = None) -> Path:
    root = (repo or repo_root()).resolve()
    return root / _IN_TREE_SCHEMA_RELPATH


def in_tree_schema_version(
    repo: Path | None = None,
    *,
    reader: SchemaReader | FieldsReader = read_schema_version,
) -> int | None:
    """In-tree ``HANDOFF_SCHEMA_VERSION``, or ``None`` when unreadable."""
    result = reader(in_tree_schema_path(repo))
    if isinstance(result, tuple):
        return result[0]
    return result


def in_tree_schema_fields(
    repo: Path | None = None,
    *,
    reader: FieldsReader = read_schema_fields,
) -> SchemaFields:
    """In-tree ``(schema, floor)``, or ``(None, None)`` when unreadable."""
    return reader(in_tree_schema_path(repo))


def _iter_tool_dirs(tools_root: Path) -> Iterable[Path]:
    """Immediate child directories under the uv tools root (one per tool name)."""
    if not tools_root.is_dir():
        return ()
    return (
        child
        for child in sorted(tools_root.iterdir())
        if child.is_dir() and not child.name.startswith(".")
    )


def tool_vendored_schema_path(tool_dir: Path) -> Path | None:
    """Locate ``workbay_handoff_mcp/shared_schema.py`` under a tool install.

    uv places site-packages at ``lib/pythonX.Y/site-packages/`` (POSIX) or
    ``Lib/site-packages/`` (Windows). Prefer a direct glob so the probe stays
    free of interpreter discovery and works when the tool's python is broken.
    """
    if not tool_dir.is_dir():
        return None
    matches = sorted(tool_dir.glob("**/site-packages/workbay_handoff_mcp/shared_schema.py"))
    if not matches:
        return None
    # Prefer the shallowest match (real site-packages over nested copies).
    return min(matches, key=lambda p: len(p.parts))


def tool_schema_version(
    tool_dir: Path,
    *,
    reader: SchemaReader | FieldsReader = read_schema_version,
) -> int | None:
    """Vendored schema version for one tool env, or ``None`` when absent."""
    path = tool_vendored_schema_path(tool_dir)
    if path is None:
        return None
    result = reader(path)
    if isinstance(result, tuple):
        return result[0]
    return result


def tool_schema_fields(
    tool_dir: Path,
    *,
    reader: FieldsReader = read_schema_fields,
) -> SchemaFields | None:
    """Vendored ``(schema, floor)`` for one tool, or ``None`` when no vendored file.

    Raises :class:`ProbeUnavailableError` when a vendored path is present but
    the reader yields no schema (unreadable or unparsable). Absent vendored
    handoff remains a real skip (``None``), not blindness [OBS-08].
    """
    path = tool_vendored_schema_path(tool_dir)
    if path is None:
        return None
    schema, floor = reader(path)
    if schema is None:
        raise ProbeUnavailableError(
            f"vendored schema unreadable/unparsable for tool "
            f"{tool_dir.name} at {path}"
        )
    return (schema, floor)


def _coerce_fields_reader(
    reader: SchemaReader | FieldsReader,
) -> FieldsReader:
    """Adapt a legacy schema-only reader or a fields reader to FieldsReader."""

    def adapted(path: Path) -> SchemaFields:
        result = reader(path)
        if isinstance(result, tuple):
            return result  # type: ignore[return-value]
        # Legacy SchemaReader: floor not probed → treat as ABSENT only when
        # schema was readable; both None when unreadable.
        if result is None:
            return (None, None)
        return (result, None)

    return adapted


def installed_drift(
    *,
    repo: Path | None = None,
    tools_root: Path | None = None,
    reader: SchemaReader | FieldsReader = read_schema_fields,
) -> dict[str, tuple[int, int]]:
    """Tools whose vendored HANDOFF_SCHEMA_VERSION differs from in-tree source.

    Returns ``tool_name -> (installed_version, in_tree_version)``. Tools with
    no vendored ``workbay_handoff_mcp`` are skipped (not drift). When the
    in-tree sentinel cannot be read, raises :class:`ProbeUnavailableError`
    rather than fail-open clean — "could not determine" is not "no drift"
    [OBS-08].

    Schema-only map kept for callers that unpack two ints (doctor facet).
    Floor skew and absence are reported via :func:`installed_floor_drift` and
    the combined check/report path.
    """
    fields_reader = _coerce_fields_reader(reader)
    source_schema, _source_floor = in_tree_schema_fields(repo, reader=fields_reader)
    if source_schema is None:
        raise ProbeUnavailableError(
            f"in-tree schema sentinel unreadable/unparsable: "
            f"{in_tree_schema_path(repo)}"
        )
    root = tools_root if tools_root is not None else default_tools_root()
    drift: dict[str, tuple[int, int]] = {}
    for tool_dir in _iter_tool_dirs(root):
        fields = tool_schema_fields(tool_dir, reader=fields_reader)
        if fields is None:
            continue
        installed_schema, _installed_floor = fields
        if installed_schema != source_schema:
            drift[tool_dir.name] = (installed_schema, source_schema)
    return drift


def installed_floor_drift(
    *,
    repo: Path | None = None,
    tools_root: Path | None = None,
    reader: SchemaReader | FieldsReader = read_schema_fields,
) -> dict[str, tuple[FloorStatus, int | None, int | None]]:
    """Tools whose MIN_COMPATIBLE_READER_VERSION is absent or differs.

    Returns ``tool_name -> (status, installed_floor, in_tree_floor)`` where
    *status* is ``absent`` or ``differ`` (never ``match``). ``installed_floor``
    is ``None`` when the constant is absent. Tools with no vendored handoff are
    skipped. When in-tree schema is unreadable, raises
    :class:`ProbeUnavailableError` rather than fail-open clean [OBS-08].
    """
    fields_reader = _coerce_fields_reader(reader)
    source_schema, source_floor = in_tree_schema_fields(repo, reader=fields_reader)
    if source_schema is None:
        raise ProbeUnavailableError(
            f"in-tree schema sentinel unreadable/unparsable: "
            f"{in_tree_schema_path(repo)}"
        )
    root = tools_root if tools_root is not None else default_tools_root()
    drift: dict[str, tuple[FloorStatus, int | None, int | None]] = {}
    for tool_dir in _iter_tool_dirs(root):
        fields = tool_schema_fields(tool_dir, reader=fields_reader)
        if fields is None:
            continue
        _installed_schema, installed_floor = fields
        status = floor_status(installed_floor, source_floor)
        if status == FLOOR_MATCH:
            continue
        drift[tool_dir.name] = (status, installed_floor, source_floor)
    return drift


def drifted_names(
    *,
    repo: Path | None = None,
    tools_root: Path | None = None,
    reader: SchemaReader | FieldsReader = read_schema_fields,
) -> list[str]:
    """Sorted tool names with schema or reader-floor skew (reinstall candidates)."""
    names = set(installed_drift(repo=repo, tools_root=tools_root, reader=reader))
    names.update(
        installed_floor_drift(repo=repo, tools_root=tools_root, reader=reader)
    )
    return sorted(names)


def build_reinstall_argv(
    package: str,
    *,
    repo: Path | None = None,
    tools_root: Path | None = None,
    extras_by_package: ExtrasByPackage | None = None,
    extras_loader: ExtrasLoader | None = None,
) -> list[str]:
    """``uv`` argv (without the ``uv`` binary) to reinstall *package* from tree.

    Path-based, ``--no-sources``, ``--force``, with every other runtime-closure
    member as ``--with <path>`` so workspace pins resolve. Package path specs
    carry extras from the tool's ``uv-receipt.toml`` so reinstall preserves
    optional capabilities by construction.

    *extras_by_package* injects a name→extras map (tests). *extras_loader*
    injects a callable ``package -> map`` (same style as *reader* / *tools_root*).
    When neither is provided, the receipt under *tools_root* (or
    :func:`default_tools_root`) is read. A missing or malformed receipt raises
    :class:`ReceiptUnavailableError` — never silently strips extras [OBS-08].

    Only valid for packages in :data:`_REINSTALLABLE_TOOL_PACKAGES` whose
    ``packages/<name>`` directory exists.
    """
    if package not in _REINSTALLABLE_TOOL_PACKAGES:
        raise ValueError(f"not a reinstallable workbay uv tool package: {package}")
    root = (repo or repo_root()).resolve()
    pkg_dir = root / "packages" / package
    if not (pkg_dir / "pyproject.toml").is_file():
        raise ValueError(f"missing in-tree package for reinstall: {pkg_dir}")

    if extras_by_package is not None:
        extras_map = dict(extras_by_package)
    elif extras_loader is not None:
        extras_map = dict(extras_loader(package))
    else:
        tools = tools_root if tools_root is not None else default_tools_root()
        extras_map = read_receipt_extras(tool_receipt_path(tools, package))

    argv = ["tool", "install", "--no-sources", "--force"]
    with_members = [m for m in _RUNTIME_WITH_MEMBERS if m != package]
    if package == "mcp-workbay-orchestrator":
        for extra in _ORCHESTRATOR_EXTRA_MEMBERS:
            if extra not in with_members:
                with_members.append(extra)
    for member in with_members:
        member_dir = root / "packages" / member
        if (member_dir / "pyproject.toml").is_file():
            member_extras = extras_map.get(member, [])
            argv.extend(
                ["--with", path_spec_with_extras(member_dir, member_extras)]
            )
    from_extras = extras_map.get(package, [])
    argv.extend(["--from", path_spec_with_extras(pkg_dir, from_extras), package])
    return argv


def reinstallable_drifted_names(
    *,
    repo: Path | None = None,
    tools_root: Path | None = None,
    reader: SchemaReader | FieldsReader = read_schema_fields,
) -> list[str]:
    """Drifted tools that have an in-tree package dir (safe to reinstall)."""
    root = (repo or repo_root()).resolve()
    names: list[str] = []
    for name in drifted_names(repo=root, tools_root=tools_root, reader=reader):
        if name not in _REINSTALLABLE_TOOL_PACKAGES:
            continue
        if (root / "packages" / name / "pyproject.toml").is_file():
            names.append(name)
    return names


def _format_schema_drift(drift: dict[str, tuple[int, int]]) -> str:
    return "\n".join(
        f"  {name}: installed schema {installed} != in-tree {source}"
        for name, (installed, source) in sorted(drift.items())
    )


def _format_floor_drift(
    drift: dict[str, tuple[FloorStatus, int | None, int | None]],
) -> str:
    lines: list[str] = []
    for name, (status, installed_floor, source_floor) in sorted(drift.items()):
        if status == FLOOR_ABSENT:
            lines.append(
                f"  {name}: installed MIN_COMPATIBLE_READER_VERSION absent "
                f"(in-tree floor {source_floor})"
            )
        else:
            lines.append(
                f"  {name}: installed MIN_COMPATIBLE_READER_VERSION "
                f"{installed_floor} != in-tree {source_floor}"
            )
    return "\n".join(lines)


def _format_drift(drift: dict[str, tuple[int, int]]) -> str:
    """Backward-compatible schema-only formatter (doctor / older callers)."""
    return _format_schema_drift(drift)


def reinstall_drifted(
    *,
    repo: Path | None = None,
    tools_root: Path | None = None,
    reader: SchemaReader | FieldsReader = read_schema_fields,
    uv_bin: str | None = None,
) -> int:
    """Reinstall every reinstallable drifted tool from the working tree.

    Returns 0 when nothing drifted or every reinstall succeeded; non-zero on
    the first failed ``uv tool install`` or when a tool's receipt is
    unavailable (refuses extras-stripping reinstall). Stdlib-only
    (``subprocess`` + ``shutil.which``); does not import workbay packages.
    """
    root = (repo or repo_root()).resolve()
    tools = tools_root if tools_root is not None else default_tools_root()
    names = reinstallable_drifted_names(
        repo=root, tools_root=tools, reader=reader
    )
    if not names:
        all_drifted = drifted_names(repo=root, tools_root=tools, reader=reader)
        if all_drifted:
            print(
                "sync-uv-tool-schema-drift: drifted tools not reinstallable: "
                + ", ".join(all_drifted),
                file=sys.stderr,
            )
            return 1
        print("sync-uv-tool-schema-drift: no drift")
        return 0
    uv = uv_bin or shutil.which("uv")
    if not uv:
        print(
            "sync-uv-tool-schema-drift: `uv` not found on PATH",
            file=sys.stderr,
        )
        return 2
    for name in names:
        try:
            argv = [
                uv,
                *build_reinstall_argv(name, repo=root, tools_root=tools),
            ]
        except ReceiptUnavailableError as exc:
            print(
                f"sync-uv-tool-schema-drift: refusing reinstall of {name}: {exc}",
                file=sys.stderr,
            )
            return 3
        print(f"sync-uv-tool-schema-drift: reinstalling {name} from working tree")
        print("  +", " ".join(argv))
        proc = subprocess.run(argv, cwd=str(root), check=False)
        if proc.returncode != 0:
            print(
                f"sync-uv-tool-schema-drift: reinstall of {name} failed "
                f"(exit {proc.returncode})",
                file=sys.stderr,
            )
            return proc.returncode
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        if "--print-drifted-names" in argv:
            print("\n".join(drifted_names()))
            return 0
        if "--print-reinstallable-names" in argv:
            print("\n".join(reinstallable_drifted_names()))
            return 0
        if "--reinstall" in argv:
            return reinstall_drifted()
        if "--check" in argv:
            schema_drift = installed_drift()
            floor_drift = installed_floor_drift()
            if schema_drift or floor_drift:
                parts: list[str] = []
                if schema_drift:
                    parts.append(
                        "uv tool HANDOFF_SCHEMA_VERSION drift (installed tool "
                        "vendored schema != in-tree source):\n"
                        + _format_schema_drift(schema_drift)
                    )
                if floor_drift:
                    parts.append(
                        "uv tool MIN_COMPATIBLE_READER_VERSION drift (absent or "
                        "!= in-tree floor):\n"
                        + _format_floor_drift(floor_drift)
                    )
                parts.append(
                    "Reinstall drifted tools from the working tree with: "
                    "make sync-uv-tool-schema-drift"
                )
                print("\n".join(parts), file=sys.stderr)
                return 1
            return 0
    except ProbeUnavailableError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    print(__doc__)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
