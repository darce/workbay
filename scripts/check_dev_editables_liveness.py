#!/usr/bin/env python3
"""implementation note D4 — drift check: dev redirects present, copy-editables absent."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def _load_dev_install(repo: Path):
    path = repo / "scripts" / "dev_install.py"
    spec = importlib.util.spec_from_file_location("dev_install", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--venv", type=Path, default=None)
    parser.add_argument("--emit-json", action="store_true")
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    venv = (args.venv or repo / ".venv").resolve()
    dev = _load_dev_install(repo)
    if not venv.is_dir():
        raise SystemExit(f"venv missing: {venv}")
    site = dev.site_packages(venv)
    rows: list[dict[str, str]] = []
    ok = True
    for member in dev.MEMBERS:
        if member.exempt:
            rows.append({"member": member.dist_name, "status": "exempt"})
            continue
        import_name = member.import_names[0]
        pth = site / f"zz_dev_redirect_{import_name}.pth"
        copy = site / import_name
        copy_py = site / f"{import_name}.py"
        live = dev.probe_import_is_live(venv_root=venv, import_name=import_name)
        if pth.is_file() and live and not copy.exists() and not copy_py.exists():
            rows.append({"member": member.dist_name, "status": "live"})
        else:
            ok = False
            rows.append(
                {
                    "member": member.dist_name,
                    "status": "copy_editable_regression",
                    "pth": str(pth),
                    "copy_dir": str(copy),
                    "copy_py": str(copy_py),
                    "live": str(live),
                }
            )
    payload = {"ok": ok, "repo": str(repo), "venv": str(venv), "members": rows}
    if args.emit_json:
        print(json.dumps(payload, indent=2))
    else:
        for row in rows:
            print(f"{row['member']}: {row['status']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
