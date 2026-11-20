"""Shared pytest console for handoff scenarios that require native qualification.

Value contract: ``qualified_handoff_console`` is a function-scoped
``HandoffConsoleBuilder``. Calling it with a Bash script body returns a frozen
``HandoffConsole(path, env)``: the canonical console path and a fresh, complete
subprocess environment. Pass ``env`` to subprocesses (or install its values with
``monkeypatch`` for in-process lifecycle calls). Building never changes os.environ.

Callers vary stdout, stderr, exit status, argument-dependent responses and other
scenario effects through the script body. Arguments reach Bash unchanged. Each
call writes a separate script and returns a separate environment, so earlier
values remain usable after later calls. Within one test all calls reuse one
venv, interpreter, canonical console, and pristine copies of the current local
handoff/protocol sources with hashed RECORD metadata. No dependency is installed.

The provider CLI's ``main`` is replaced at normal site startup by a test-only
transport to the selected script. The native resolver and its isolated probe
are untouched: they validate the real console, interpreter and recorded provider
sources. Source drift remains a real refusal; later builder calls do not repair
it. The returned environment disables optional embeddings and removes inherited
Python source overrides so it describes this runtime. Qualification is guaranteed
for the unmodified fixture under that environment, subject to the real probe's
resource budget and the workspace's install policy.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import os
import shutil
import sysconfig
import tomllib
import venv
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pytest


@dataclass(frozen=True)
class HandoffConsole:
    path: Path
    env: dict[str, str]


class HandoffConsoleBuilder(Protocol):
    def __call__(self, body: str) -> HandoffConsole: ...


def _build_runtime(root: Path) -> Path:
    prefix = root / ".venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(prefix)
    site = next((prefix / "lib").glob("python*/site-packages"))
    # Dependencies are read-only links; WorkBay sources are always local copies.
    dependencies = Path(sysconfig.get_paths()["purelib"])
    for dependency in dependencies.iterdir():
        name = dependency.name.lower()
        if (
            "workbay" in name
            or name.endswith(".pth")
            or name in {"__pycache__", "sitecustomize.py", "usercustomize.py"}
            or name.startswith(("onnx", "pil", "fastembed", "sentence_transformers"))
        ):
            continue
        (site / dependency.name).symlink_to(dependency, target_is_directory=dependency.is_dir())

    repo = Path(__file__).resolve().parent
    for package, top in (
        ("mcp-workbay-handoff", "workbay_handoff_mcp"),
        ("workbay-protocol", "workbay_protocol"),
    ):
        project = repo / "packages" / package
        shutil.copytree(
            project / "src" / top,
            site / top,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        data = tomllib.loads((project / "pyproject.toml").read_text())["project"]
        info = site / f"{package.replace('-', '_')}-{data['version']}.dist-info"
        info.mkdir()
        (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {package}\nVersion: {data['version']}\n")
        scripts = data.get("scripts", {})
        (info / "entry_points.txt").write_text(
            "[console_scripts]\n" + "".join(f"{name} = {target}\n" for name, target in scripts.items())
        )
        with (info / "RECORD").open("w", newline="") as stream:
            writer = csv.writer(stream)
            for source in [*(site / top).rglob("*.py"), info / "METADATA", info / "entry_points.txt"]:
                blob = source.read_bytes()
                digest = base64.urlsafe_b64encode(hashlib.sha256(blob).digest()).decode().rstrip("=")
                writer.writerow([source.relative_to(site).as_posix(), "sha256=" + digest, len(blob)])
        for name, target in scripts.items():
            module, function = target.split(":")
            console = prefix / "bin" / name
            console.write_text(
                f"#!{prefix}/bin/python\nfrom {module} import {function}\n"
                f'if __name__ == "__main__":\n    {function}()\n'
            )
            console.chmod(0o755)

    # Only the provider entrypoint is substituted. -I -S qualification never
    # executes sitecustomize and still checks every original provider source.
    (site / "sitecustomize.py").write_text(
        "import os\nimport sys\nimport types\n"
        "_scenario = os.environ.get('WORKBAY_TEST_HANDOFF_SCENARIO')\n"
        "if _scenario:\n"
        "    cli = types.ModuleType('workbay_handoff_mcp.cli')\n"
        "    def _scenario_main():\n"
        f"        os.execv({shutil.which('bash')!r}, ['bash', _scenario, *sys.argv[1:]])\n"
        "    cli.main = _scenario_main\n"
        "    sys.modules[cli.__name__] = cli\n"
    )
    return prefix / "bin" / "mcp-workbay-handoff"


@pytest.fixture
def qualified_handoff_console(tmp_path: Path) -> HandoffConsoleBuilder:
    """Build isolated scenario values sharing one genuinely qualifiable runtime."""
    root = tmp_path / "qualified-handoff"
    console: Path | None = None
    count = 0

    def build(body: str) -> HandoffConsole:
        nonlocal console, count
        if console is None:
            console = _build_runtime(root)
        count += 1
        scenario = root / f"scenario-{count}.bash"
        scenario.write_text(body)
        env = os.environ.copy()
        for name in ("PYTHONPATH", "PYTHONHOME"):
            env.pop(name, None)
        env.update(
            {
                "MCP_WORKBAY_HANDOFF_BIN": str(console),
                "WORKBAY_TEST_HANDOFF_SCENARIO": str(scenario),
                "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED": "1",
                "WORKBAY_REINJECT_SEMANTIC": "0",
                "PYTHONNOUSERSITE": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        return HandoffConsole(console, env)

    return build
