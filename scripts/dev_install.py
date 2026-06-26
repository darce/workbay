#!/usr/bin/env python3
"""implementation note — dev-install bypass: live unscrubbed redirects for WorkBay members."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from workspace_members import iter_workspace_members  # noqa: E402


@dataclass(frozen=True)
class DevEditableMember:
    package_relpath: str
    dist_name: str
    import_names: tuple[str, ...]
    path_entry: str  # repo-relative segment written into .pth (resolved at install)
    exempt: bool = False
    exempt_reason: str = ""


# Membership + import surface are DERIVED from the single registry
# (scripts/workspace_members.py), so this list cannot drift from pyproject. Only
# the dev-install redirect *policy* lives here: members with no dedicated
# live-editable redirect stay copy-editable. ``workbay-stack`` is a pins-only
# aggregator; ``workbay`` is the umbrella meta-dist.
_COPY_ONLY_EXEMPTIONS: dict[str, str] = {
    "workbay-stack": "pins-only aggregator; no meaningful live src surface",
    "workbay": "umbrella meta-dist; copy-editable only (no dedicated redirect)",
}


def _build_members() -> tuple[DevEditableMember, ...]:
    members: list[DevEditableMember] = []
    for member in iter_workspace_members(_SCRIPTS_DIR.parent):
        reason = _COPY_ONLY_EXEMPTIONS.get(member.dist_name, "")
        members.append(
            DevEditableMember(
                package_relpath=member.package_relpath,
                dist_name=member.dist_name,
                import_names=member.import_names,
                path_entry=member.src_relpath,
                exempt=bool(reason),
                exempt_reason=reason,
            )
        )
    return tuple(members)


MEMBERS: tuple[DevEditableMember, ...] = _build_members()


def repo_root(start: Path | None = None) -> Path:
    cur = (start or Path.cwd()).resolve()
    for parent in [cur, *cur.parents]:
        if (parent / ".git").exists():
            return parent
    raise RuntimeError("could not locate repo root")


def site_packages(venv_root: Path) -> Path:
    py = venv_root / "bin" / "python"
    proc = subprocess.run(
        [str(py), "-c", "import site; print(site.getsitepackages()[0])"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(proc.stdout.strip())


def dist_info_dir(site: Path, dist_name: str) -> Path | None:
    normalized = dist_name.replace("-", "_").lower()
    matches = sorted(site.glob(f"{normalized}-*.dist-info"))
    return matches[0] if matches else None


def _dist_info_name(dist_name: str, version: str) -> str:
    normalized_name = re.sub(r"[-_.]+", "_", dist_name).lower()
    normalized_version = re.sub(r"[^A-Za-z0-9.]+", "_", version)
    return f"{normalized_name}-{normalized_version}.dist-info"


def ensure_dist_metadata(*, site: Path, repo: Path, member: DevEditableMember) -> None:
    pyproject = repo / member.package_relpath / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data["project"]
    version = project["version"]
    info = site / _dist_info_name(member.dist_name, version)

    if info.is_dir():
        return

    normalized = member.dist_name.replace("-", "_").lower()
    for stale in site.glob(f"{normalized}-*.dist-info"):
        shutil.rmtree(stale)

    info.mkdir()

    metadata = [
        "Metadata-Version: 2.1",
        f"Name: {project['name']}",
        f"Version: {version}",
    ]
    for dependency in project.get("dependencies", []):
        metadata.append(f"Requires-Dist: {dependency}")
    (info / "METADATA").write_text("\n".join(metadata) + "\n", encoding="utf-8")
    scripts = project.get("scripts", {})
    if scripts:
        lines = ["[console_scripts]"]
        lines.extend(f"{name} = {target}" for name, target in scripts.items())
        (info / "entry_points.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (info / "INSTALLER").write_text("workbay-dev-install\n", encoding="utf-8")
    (info / "RECORD").write_text("", encoding="utf-8")


def remove_installed_copy(site: Path, member: DevEditableMember) -> None:
    for import_name in member.import_names:
        for target in (site / import_name, site / f"{import_name}.py"):
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
        for pth in site.glob(f"*{import_name}*.pth"):
            pth.unlink()

def install_member_redirect(*, repo: Path, venv_root: Path, member: DevEditableMember) -> Path | None:
    if member.exempt:
        return None
    site = site_packages(venv_root)
    remove_installed_copy(site, member)
    ensure_dist_metadata(site=site, repo=repo, member=member)
    pth = site / f"zz_dev_redirect_{member.import_names[0]}.pth"
    pth.write_text(f"{(repo / member.path_entry).resolve()}\n", encoding="utf-8")
    return pth


def install_all(*, repo: Path, venv_root: Path) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for member in MEMBERS:
        if member.exempt:
            results.append({"member": member.dist_name, "status": "exempt", "reason": member.exempt_reason})
            continue
        pth = install_member_redirect(repo=repo, venv_root=venv_root, member=member)
        results.append({"member": member.dist_name, "status": "redirect", "pth": str(pth)})
    return results


def probe_import_is_live(*, venv_root: Path, import_name: str) -> bool:
    py = venv_root / "bin" / "python"
    script = (
        "import importlib, inspect; "
        f"mod = importlib.import_module({import_name!r}); "
        "path = inspect.getfile(mod).replace('\\\\', '/'); "
        "print('site-packages' not in path)"
    )
    proc = subprocess.run([str(py), "-c", script], check=True, capture_output=True, text=True)
    return proc.stdout.strip() == "True"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=None)
    parser.add_argument("--venv", type=Path, default=None)
    parser.add_argument("--emit-json", action="store_true")
    args = parser.parse_args(argv)
    repo = (args.repo or repo_root()).resolve()
    venv = (args.venv or repo / ".venv").resolve()
    if not venv.is_dir():
        raise SystemExit(f"venv not found: {venv}")
    payload = {"repo": str(repo), "venv": str(venv), "results": install_all(repo=repo, venv_root=venv)}
    if args.emit_json:
        print(json.dumps(payload, indent=2))
    else:
        for row in payload["results"]:
            print(f"{row['member']}: {row['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
