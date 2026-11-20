#!/usr/bin/env python3
"""OBS-05 web gate: sequential tests, mounted browser, build and scratch staging.

Uses the checkout's locked dependencies and installed Playwright browser. Never
installs dependencies, collects Fleet observations, or writes a delivery receipt.
"""
from __future__ import annotations

import argparse
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import urllib.request


def run(argv: list[str], *, cwd: Path, env: dict[str, str]) -> int:
    print('+ ' + shlex.join(argv), flush=True)
    try:
        result = subprocess.run(argv, cwd=cwd, env=env, check=False)
    except OSError as exc:
        print(f'Setup failed: {exc}', file=sys.stderr)
        return 2
    return result.returncode if result.returncode >= 0 else 128 - result.returncode


def free_port() -> int:
    """A loopback port for the scratch Vite server; sibling lanes may hold any fixed one."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def stage_smoke(package: Path) -> None:
    """Verify scratch HTTP bytes against the builder's manifest, without installing."""
    root = package / 'src/workbay_canvas_mcp/web_dist'
    manifest = json.loads((root / 'build-manifest.json').read_text())
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(root))
    with http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f'http://127.0.0.1:{server.server_port}/'
            # Ignore outbound proxy settings for this owned loopback server.
            client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            for name, expected in manifest['files'].items():
                with client.open(base + name, timeout=10) as response:
                    actual = 'sha256:' + hashlib.sha256(response.read()).hexdigest()
                if actual != expected:
                    raise ValueError(f'staged asset digest mismatch: {name}')
        finally:
            server.shutdown()
            thread.join(timeout=5)
    print('Scratch staged assets match ' + manifest['source_digest'], flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, help='Retain gate logs and scratch staged assets here')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    app = repo / 'apps' / 'workbay-canvas-web'
    env = os.environ.copy()
    env.setdefault('VENV_PYTHON', sys.executable)
    # Respect an explicit browser path on other hosts; this is the VM's measured cache.
    if 'PLAYWRIGHT_BROWSERS_PATH' not in env and Path('/home/gate/.cache/ms-playwright').is_dir():
        env['PLAYWRIGHT_BROWSERS_PATH'] = '/home/gate/.cache/ms-playwright'
    with tempfile.TemporaryDirectory(prefix='obs05-web-') as temp:
        output = (args.output_dir or Path(temp)).resolve()
        output.mkdir(parents=True, exist_ok=True)
        rc = run(['npm', 'run', 'test', '--', 'tests/unit/fleet-blockers.test.ts',
                  'tests/unit/fleet.test.ts', 'tests/unit/fleet-main.test.ts', '--maxWorkers=1'], cwd=app, env=env)
        if rc:
            return rc
        config = output / 'playwright.config.mjs'
        origin = f'http://127.0.0.1:{free_port()}'
        config.write_text(
            'import { defineConfig } from ' + json.dumps(str(app / 'node_modules/@playwright/test/index.mjs')) + ';\n'
            'export default defineConfig(' + json.dumps({
                'testDir': str(app / 'tests/local'), 'testMatch': 'fleet-refresh.spec.ts',
                'workers': 1, 'retries': 0, 'reporter': [['list']],
                'outputDir': str(output / 'browser-artifacts'),
                'use': {'baseURL': origin, 'headless': True},
                'webServer': {
                    'command': shlex.join(['node', str(app / 'node_modules/vite/bin/vite.js'),
                                          '--host', '127.0.0.1', '--port', origin.rsplit(':', 1)[1], '--strictPort']),
                    'cwd': str(app), 'url': origin + '/tests/fixtures/fleet-shell-harness.html',
                    'reuseExistingServer': False, 'timeout': 20000,
                },
            }) + ');\n'
        )
        # Playwright owns the scratch Vite process and always retires it on exit.
        rc = run(['node', str(app / 'node_modules/@playwright/test/cli.js'), 'test', '-c', str(config)], cwd=app, env=env)
        if rc:
            return rc
        rc = run(['npm', 'run', 'build'], cwd=app, env=env)
        if rc:
            return rc
        package = output / 'scratch-package'
        builder = [sys.executable, str(repo / 'scripts/build_canvas_web_package_data.py'),
                   '--app-root', str(app), '--package-root', str(package)]
        rc = run([*builder, '--dist', str(app / 'dist')], cwd=repo, env=env)
        if rc:
            return rc
        rc = run([*builder, '--check'], cwd=repo, env=env)
        if rc:
            return rc
        try:
            stage_smoke(package)
        except (OSError, ValueError) as exc:
            print(f'Scratch asset smoke failed: {exc}', file=sys.stderr)
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
