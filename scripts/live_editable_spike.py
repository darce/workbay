#!/usr/bin/env python3
"""internal — thin prototypes comparing live-editable mechanisms."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

Winner = Literal["dev_install_bypass", "editable_aware_hook"]


@dataclass(frozen=True)
class MechanismScore:
    privacy_isolation: str
    dx: str
    blast_radius: str
    notes: tuple[str, ...]


@dataclass(frozen=True)
class SpikeComparison:
    mechanism_a: MechanismScore
    mechanism_b: MechanismScore
    winner: Winner
    rationale: str


def site_packages(venv_root: Path) -> Path:
    py = venv_root / "bin" / "python"
    probe = subprocess.run(
        [str(py), "-c", "import site; print(site.getsitepackages()[0])"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(probe.stdout.strip())


def package_src_dir(package_root: Path) -> Path:
    src = package_root / "src"
    if not src.is_dir():
        raise FileNotFoundError(f"expected src layout under {package_root}")
    return src.resolve()


def read_project_names(package_root: Path) -> tuple[str, str]:
    data = tomllib.loads((package_root / "pyproject.toml").read_text(encoding="utf-8"))
    dist_name = data["project"]["name"]
    import_name = import_top_level_name(package_root)
    return dist_name, import_name


def import_top_level_name(package_root: Path) -> str:
    names = [p.name for p in (package_root / "src").iterdir() if p.is_dir()]
    if len(names) != 1:
        raise ValueError(f"expected one importable package dir under src/: {names}")
    return names[0]


def dist_info_dir(site: Path, dist_name: str) -> Path | None:
    normalized = dist_name.replace("-", "_").lower()
    matches = sorted(site.glob(f"{normalized}-*.dist-info"))
    return matches[0] if matches else None


def remove_installed_copy(site: Path, dist_name: str, import_name: str) -> None:
    target = site / import_name
    if target.exists():
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
    info = dist_info_dir(site, dist_name)
    if info is not None:
        shutil.rmtree(info)
    for pth in site.glob(f"*{import_name}*.pth"):
        pth.unlink()


def write_src_redirect_pth(site: Path, import_name: str, src_dir: Path) -> Path:
    pth = site / f"zz_dev_redirect_{import_name}.pth"
    pth.write_text(f"{src_dir}\n", encoding="utf-8")
    return pth


def install_dev_redirect(
    *,
    venv_root: Path,
    package_root: Path,
    dist_name: str | None = None,
    import_name: str | None = None,
) -> Path:
    package_root = package_root.resolve()
    if dist_name is None or import_name is None:
        dn, iname = read_project_names(package_root)
        dist_name = dist_name or dn
        import_name = import_name or iname
    site = site_packages(venv_root)
    remove_installed_copy(site, dist_name, import_name)
    return write_src_redirect_pth(site, import_name, package_src_dir(package_root))


def reinstall_copy_editable(*, venv_root: Path, package_root: Path) -> None:
    py = venv_root / "bin" / "python"
    subprocess.run(
        [str(py), "-m", "pip", "install", "-e", str(package_root.resolve())],
        check=True,
        capture_output=True,
        text=True,
    )


def probe_import_path(*, venv_root: Path, import_name: str) -> str:
    py = venv_root / "bin" / "python"
    script = (
        "import importlib, inspect; "
        f"mod = importlib.import_module({import_name!r}); "
        "print(inspect.getfile(mod))"
    )
    proc = subprocess.run([str(py), "-c", script], check=True, capture_output=True, text=True)
    return proc.stdout.strip()


def probe_import_resolves_to_src(*, venv_root: Path, import_name: str) -> bool:
    return "/src/" in probe_import_path(venv_root=venv_root, import_name=import_name).replace("\\", "/")


def probe_import_resolves_to_site_packages(*, venv_root: Path, import_name: str) -> bool:
    path = probe_import_path(venv_root=venv_root, import_name=import_name).replace("\\", "/")
    return "/site-packages/" in path and "/src/" not in path


def probe_live_edit(
    *,
    venv_root: Path,
    package_root: Path,
    import_name: str,
    marker: str = "PLAN0065_SPIKE_LIVE",
) -> bool:
    src_pkg = package_src_dir(package_root) / import_name
    init_py = src_pkg / "__init__.py"
    original = init_py.read_text(encoding="utf-8")
    line = f"\n{marker} = True  # plan0065 spike\n"
    try:
        init_py.write_text(original + line, encoding="utf-8")
        py = venv_root / "bin" / "python"
        script = (
            "import importlib; "
            f"mod = importlib.import_module({import_name!r}); "
            f"print(getattr(mod, {marker!r}, False))"
        )
        proc = subprocess.run([str(py), "-c", script], check=True, capture_output=True, text=True)
        return proc.stdout.strip() == "True"
    finally:
        init_py.write_text(original, encoding="utf-8")


def probe_scrub_hook_runs_on_editable(package_root: Path) -> dict[str, Any]:
    root = str(package_root.resolve())
    sys.path.insert(0, root)
    try:
        from hatchling.metadata.core import ProjectMetadata
        from hatchling.builders.wheel import WheelBuilder
        from hatch_build import ScrubAtBuildHook
    finally:
        sys.path.pop(0)

    meta = ProjectMetadata(root, None)
    build_cfg = WheelBuilder(root, meta).config
    results: dict[str, int] = {}
    for version in ("editable", "standard"):
        hook = ScrubAtBuildHook(root, {}, build_cfg, meta, "/tmp", "wheel")
        build_data: dict[str, Any] = {}
        hook.initialize(version, build_data)
        results[version] = len(build_data.get("force_include", {}))
    return results


def prototype_mechanism_b_hook(package_root: Path) -> dict[str, Any]:
    """Thin (b) prototype: skip scrub on editable, keep scrub on standard."""
    root = str(package_root.resolve())
    sys.path.insert(0, root)
    try:
        from hatchling.metadata.core import ProjectMetadata
        from hatchling.builders.wheel import WheelBuilder
        from hatch_build import ScrubAtBuildHook
    finally:
        sys.path.pop(0)

    class EditableAwareScrubHook(ScrubAtBuildHook):
        def initialize(self, version: str, build_data: dict[str, Any]) -> None:
            if version == "editable":
                build_data["editable_redirect"] = str(package_src_dir(package_root))
                return
            super().initialize(version, build_data)

    meta = ProjectMetadata(root, None)
    build_cfg = WheelBuilder(root, meta).config
    out: dict[str, Any] = {}
    for version in ("editable", "standard"):
        hook = EditableAwareScrubHook(root, {}, build_cfg, meta, "/tmp", "wheel")
        build_data: dict[str, Any] = {}
        hook.initialize(version, build_data)
        out[version] = {
            "force_include": len(build_data.get("force_include", {})),
            "editable_redirect": build_data.get("editable_redirect"),
        }
    return out


def probe_hatch_editable_version_signal(package_root: Path, venv_python: Path) -> str | None:
    """Run hatchling wheel build for editable and capture hook version label."""
    log = package_root / ".spike_hook_versions.log"
    log.unlink(missing_ok=True)
    probe_py = package_root / "_spike_hook_probe.py"
    probe_py.write_text(
        "from hatchling.builders.hooks.plugin.interface import BuildHookInterface\n"
        "from hatchling.plugins import hookimpl\n\n"
        "class _Probe(BuildHookInterface):\n"
        "    PLUGIN_NAME = 'spike_probe'\n"
        "    def initialize(self, version, build_data):\n"
        f"        open({str(log)!r}, 'a').write(version + '\\n')\n\n"
        "@hookimpl\n"
        "def hatch_register_build_hook():\n"
        "    return _Probe\n",
        encoding="utf-8",
    )
    try:
        subprocess.run(
            [str(venv_python), "-m", "pip", "wheel", str(package_root), "--no-deps", "-w", "/tmp"],
            check=False,
            capture_output=True,
            text=True,
            cwd=package_root,
        )
    finally:
        probe_py.unlink(missing_ok=True)
    if not log.is_file():
        return None
    lines = [line.strip() for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]
    log.unlink(missing_ok=True)
    return lines[-1] if lines else None


def compare_mechanisms(*, package_root: Path) -> SpikeComparison:
    hook_probe = probe_scrub_hook_runs_on_editable(package_root)
    b_proto = prototype_mechanism_b_hook(package_root)
    editable_scrubs = hook_probe.get("editable", 0) > 0
    b_skips_on_editable = b_proto["editable"]["force_include"] == 0
    mechanism_a = MechanismScore(
        privacy_isolation="strong — publish build path byte-untouched",
        dx="requires explicit dev-install step after uv sync",
        blast_radius="new shared dev-install script + per-member metadata",
        notes=(
            "supersedes copy-editable by removing site-packages copy before .pth",
            "redirect points at checkout-local src/",
        ),
    )
    mechanism_b = MechanismScore(
        privacy_isolation="weaker — hook must branch on editable vs publish",
        dx="single uv sync / pip install -e path",
        blast_radius="editable branch in all 8 hatch_build.py scrub hooks",
        notes=(
            f"production hook scrubs editable (force_include={hook_probe.get('editable', 0)})",
            f"thin prototype skips scrub on editable={b_skips_on_editable}",
            "incorrect branch risks shipping unscrubbed wheels",
        ),
    )
    winner: Winner = "dev_install_bypass" if editable_scrubs else "editable_aware_hook"
    rationale = (
        "Mechanism (b) still force-includes scrubbed copies on editable builds; "
        "a thin prototype shows skipping scrub is possible but adds a privacy-critical "
        "branch to every member hook. Mechanism (a) keeps publish builds untouched."
        if editable_scrubs
        else "Mechanism (b) already skips scrub on editable without publish risk."
    )
    return SpikeComparison(
        mechanism_a=mechanism_a,
        mechanism_b=mechanism_b,
        winner=winner,
        rationale=rationale,
    )


def comparison_as_dict(package_root: Path) -> dict[str, Any]:
    payload = asdict(compare_mechanisms(package_root=package_root))
    payload["hook_probe"] = probe_scrub_hook_runs_on_editable(package_root)
    payload["mechanism_b_prototype"] = prototype_mechanism_b_hook(package_root)
    return payload


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-root", type=Path, default=Path("packages/workbay-protocol"))
    parser.add_argument("--emit-json", action="store_true")
    args = parser.parse_args(argv)
    payload = comparison_as_dict(args.package_root.resolve())
    if args.emit_json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"winner={payload['winner']}")
        print(payload["rationale"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
