from __future__ import annotations

import os
from pathlib import Path


def _resolve_package_root() -> Path:
    here = Path(__file__).resolve()
    direct = here.parents[2]
    if (direct / "workbay_system" / "payload").is_dir() and (direct / "pyproject.toml").is_file():
        return direct

    for start in (Path.cwd(), *Path.cwd().parents):
        for candidate in (start / "packages" / "workbay-system", start):
            if (
                candidate.is_dir()
                and (candidate / "pyproject.toml").is_file()
                and (candidate / "workbay_system" / "payload").is_dir()
            ):
                return candidate.resolve()

    override = os.environ.get("WORKBAY_SYSTEM_PACKAGE_ROOT")
    if override:
        return Path(override).resolve()

    return direct


PACKAGE_ROOT = _resolve_package_root()
PACKAGES_ROOT = PACKAGE_ROOT.parent
