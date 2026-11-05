"""Native qualification of the two WorkBay native console contracts.

Selection is Git scoped by the callers. Versions describe providers; they never
admit them. Like _interp's origin guard, wheels belong to the selected prefix
and editable sources belong to the hook's Git checkout. Only full passes may
produce stat-bound receipts inside the selected environment.
The probe skips site startup, checks installed ownership, then imports public
contracts under a read-only audit guard. It never invokes a console or a model.
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path

# Public console entrypoints in the two package pyprojects. The core profile is
# RuntimeConfig.tool_profile == 'all'; embeddings have a separate install policy.
ENTRYPOINTS = {
    "mcp-workbay-handoff": ("mcp-workbay-handoff", "workbay_handoff_mcp.cli"),
    "mcp-workbay-orchestrator": ("mcp-workbay-orchestrator", "workbay_orchestrator_mcp.cli"),
}
# 8 GiB host: 1.7 s calm, 17–21 s under swap; bytecode warming did not help.
# One shared cold-resolution budget leaves headroom in the ~120 s client window.
# Canon v0.26.2 RES-02/RES-03 (Release It!, ch. 5): bound slow failure.
PROBE_SECONDS = 60.0
PROBE_SECONDS_RANGE = (30.0, 90.0)
PROBE_SECONDS_ENV = "WORKBAY_NATIVE_PROBE_SECONDS"
RECEIPT_SCHEMA = "workbay/native-receipt@1"
MAX_OUTPUT = 1024 * 1024
MAX_FILE = 4 * 1024 * 1024
MAX_FILES = 2048


class NativeRuntimeUnavailable(ValueError):
    code = "native_runtime_unqualified"

    def __init__(self, console: str, reasons: list[str]):
        self.reasons = tuple(reasons)
        super().__init__(
            f"{self.code}: {console}: {'; '.join(reasons)}; "
            "MCP server binaries not installed with the required native contracts — "
            "run workbay-bootstrap install (or `make provision-env` for a development workspace)"
        )


class NativeRuntimeUnverified(NativeRuntimeUnavailable):
    """Qualification exhausted its evidence budget; no console may be launched."""

    code = "native_runtime_unverified"

    def __init__(self, console, reasons, elapsed_seconds, budget_seconds):
        self.elapsed_seconds = elapsed_seconds
        self.budget_seconds = budget_seconds
        self.reasons = tuple(reasons)
        ValueError.__init__(
            self,
            f"{self.code}: {console}: {'; '.join(reasons)}; "
            f"unverified: elapsed_seconds={elapsed_seconds:.3f} budget_seconds={budget_seconds:.3f}; "
            "qualification incomplete; retry when the runtime is responsive",
        )


class ProbeTimeout(ValueError):
    def __init__(self, elapsed_seconds, budget_seconds):
        self.elapsed_seconds = elapsed_seconds
        self.budget_seconds = budget_seconds
        super().__init__(
            f"unverified:probe_timeout elapsed_seconds={elapsed_seconds:.3f} budget_seconds={budget_seconds:.3f}"
        )


def probe_budget_seconds() -> float:
    try:
        value = float(os.environ.get(PROBE_SECONDS_ENV, ""))
    except ValueError:
        return PROBE_SECONDS
    return (
        value if math.isfinite(value) and PROBE_SECONDS_RANGE[0] <= value <= PROBE_SECONDS_RANGE[1] else PROBE_SECONDS
    )


def _json(text: str):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result

    def constant(_value):
        raise ValueError("nonfinite_json")

    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant)


def _bytes(path: Path, limit: int = MAX_FILE) -> bytes:
    # Nonblocking open also refuses FIFOs/devices without hanging the resolver.
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("not_regular_file")
        blob = stream.read(limit + 1)
    if len(blob) > limit:
        raise ValueError("file_too_large")
    return blob


def _fingerprint(path: Path) -> str:
    if path.is_dir():
        entries = []
        for root, dirs, files in os.walk(path, followlinks=False):
            dirs[:] = sorted(d for d in dirs if d != "__pycache__")
            for name in [*dirs, *sorted(f for f in files if not f.endswith(".pyc"))]:
                child = Path(root) / name
                entries.append((str(child.relative_to(path)), str(child.resolve())))
                if len(entries) > MAX_FILES:
                    raise ValueError("provider_file_limit")
        return hashlib.sha256(json.dumps(entries).encode()).hexdigest()
    return hashlib.sha256(_bytes(path)).hexdigest()


def _binding(path: Path):
    """Bind both a symlink and its target; inode/ctime catch restored mtimes."""

    def fields(value):
        return {
            name: getattr(value, "st_" + name) for name in ("size", "mtime_ns", "ctime_ns", "ino", "dev", "uid", "mode")
        }

    try:
        return {"path": str(path.resolve(strict=True)), "stat": fields(path.stat()), "lstat": fields(path.lstat())}
    except FileNotFoundError:
        if path.is_symlink():
            raise ValueError("dangling_binding")
        return None


def _check_stats(stats):
    if not isinstance(stats, dict) or not 1 <= len(stats) <= MAX_FILES:
        raise ValueError("probe_malformed_stats")
    for filename, binding in stats.items():
        if not isinstance(filename, str) or not Path(filename).is_absolute() or _binding(Path(filename)) != binding:
            raise ValueError("probe_identity_drift")


class QualifiedConsole(str):
    """A path carrying this selection's verdict, for a cheap exec-boundary check."""

    def __new__(cls, path, name, stats):
        value = super().__new__(cls, path)
        value.console_name = name
        value.qualification_stats = stats
        return value


def recheck_console(console):
    if not isinstance(console, QualifiedConsole):
        raise NativeRuntimeUnavailable(str(console), ["missing_qualification_verdict"])
    try:
        _check_stats(console.qualification_stats)
    except (OSError, ValueError) as exc:
        raise NativeRuntimeUnavailable(console.console_name, ["probe_identity_drift"]) from exc


def _receipt_path(path: Path, name: str) -> Path:
    return path.parent.parent / (".workbay-native-" + name + ".json")


def _discard_receipt(path):
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass  # Never let an unusable cache prevent a full qualification.


def _receipt_owner(path, stats):
    """A private receipt must not let a different principal vouch for sources."""
    owner = path.parent.stat()
    if owner.st_mode & 0o022 or (hasattr(os, "getuid") and owner.st_uid != os.getuid()):
        raise ValueError("receipt_environment_permissions")
    for binding in stats.values():
        if binding is not None and any(
            binding[k]["uid"] != owner.st_uid or binding[k]["mode"] & 0o022 for k in ("stat", "lstat")
        ):
            # Symlink permissions are normally 0777; the target and its parent
            # enforce access. The link's owner and identity still bind it.
            if (
                binding["stat"]["uid"] != owner.st_uid
                or binding["stat"]["mode"] & 0o022
                or binding["lstat"]["uid"] != owner.st_uid
                or not stat.S_ISLNK(binding["lstat"]["mode"])
            ):
                raise ValueError("receipt_source_permissions")


def _read_receipt(path, key):
    try:
        identity = path.lstat()
        if (
            not stat.S_ISREG(identity.st_mode)
            or stat.S_IMODE(identity.st_mode) != 0o600
            or identity.st_uid != path.parent.stat().st_uid
        ):
            raise ValueError("receipt_permissions")
        data = _json(_bytes(path).decode("utf-8"))
        if (
            set(data) != {"schema", "key", "proof", "stats", "records"}
            or data["schema"] != RECEIPT_SCHEMA
            or data["key"] != key
        ):
            raise ValueError("receipt_key")
        proof = {**data["proof"], "stats": data["stats"]}
        _validate_proof(proof, key["request"])
        records = {f: d for f, d in proof["files"].items() if Path(f).name == "RECORD"}
        if not records or data["records"] != records:
            raise ValueError("receipt_records")
        _receipt_owner(path, data["stats"])
        _check_stats(data["stats"])
        return proof
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RecursionError):
        _discard_receipt(path)
        return None


def _write_receipt(path, key, proof):
    """Only the parent writes, after a full probe and its final identity check."""
    temporary = None
    try:
        _receipt_owner(path, proof["stats"])
        data = {
            "schema": RECEIPT_SCHEMA,
            "key": key,
            "proof": {k: v for k, v in proof.items() if k != "stats"},
            "stats": proof["stats"],
            "records": {f: d for f, d in proof["files"].items() if Path(f).name == "RECORD"},
        }
        fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            os.chmod(temporary, 0o600)
            json.dump(data, stream, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        _check_stats(proof["stats"])
        os.replace(temporary, path)
    except OSError:
        # A read-only tool env can still be fully qualified on every launch.
        pass
    except ValueError as exc:
        if str(exc) != "receipt_source_permissions" and str(exc) != "receipt_environment_permissions":
            raise
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def _console_details(console: str, name: str) -> tuple[Path, Path]:
    path = Path(console).absolute().resolve(strict=True)
    if not path.is_file() or (os.name != "nt" and not os.access(path, os.X_OK)):
        raise ValueError("console_not_executable")
    if path.parent.name not in {"bin", "Scripts"}:
        raise ValueError("console_outside_environment")
    venv = path.parent.parent
    cfg = _bytes(venv / "pyvenv.cfg", 16384).decode("utf-8")
    if any(line.lower().replace(" ", "") == "include-system-site-packages=true" for line in cfg.splitlines()):
        raise ValueError("nonisolated_environment")
    body = _bytes(path)
    if path.suffix.lower() == ".exe":
        with zipfile.ZipFile(path) as archive:
            info = archive.getinfo("__main__.py")
            if info.file_size > 65536:
                raise ValueError("launcher_too_large")
            source = archive.read(info).decode("utf-8")
        # distlib embeds the interpreter shebang immediately before its zip.
        prefix = body[: body.find(b"PK\x03\x04")]
        header = prefix[prefix.rfind(b"#!") :].decode("utf-8").strip()
    else:
        source = body.decode("utf-8")
        if not source:
            raise ValueError("console_empty")
        header = source.splitlines()[0]
    if not header.startswith("#!"):
        raise ValueError("console_no_shebang")
    command = shlex.split(header[2:], posix=os.name != "nt")
    if len(command) != 1:
        raise ValueError("console_ambiguous_interpreter")
    python = Path(command[0].strip('"'))
    if command[0] == "/bin/sh":
        # uv/pip's long-shebang wrapper is also valid Python (a docstring).
        lines = source.splitlines()
        if len(lines) < 3 or not lines[1].startswith("'''exec' ") or lines[2] != "' '''":
            raise ValueError("console_unsupported_shell_wrapper")
        tokens = shlex.split(lines[1][len("'''exec' ") :])
        if len(tokens) != 3 or tokens[1:] != ["$0", "$@"]:
            raise ValueError("console_unsupported_shell_wrapper")
        python = Path(tokens[0])
    if not python.is_absolute() or python.parent.resolve() != path.parent:
        raise ValueError("console_foreign_interpreter")
    if not python.name.lower().startswith("python") or not os.access(python, os.X_OK):
        raise ValueError("console_missing_interpreter")
    tree = ast.parse(source)
    guards = [n for n in tree.body if isinstance(n, ast.If)]
    main_test = ast.dump(ast.parse("__name__ == '__main__'", mode="eval").body)
    if len(guards) != 1 or ast.dump(guards[0].test) != main_test or guards[0].orelse:
        raise ValueError("console_missing_main_guard")
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.If)):
            continue
        if not (
            isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
        ):
            raise ValueError("console_unexpected_statement")
    for node in ast.walk(guards[0]):
        if isinstance(node, ast.stmt) and not isinstance(node, (ast.If, ast.Assign, ast.Expr)):
            raise ValueError("console_unexpected_statement")
        if isinstance(node, ast.Assign) and [ast.unparse(t) for t in node.targets] != ["sys.argv[0]"]:
            raise ValueError("console_unexpected_assignment")
        if (
            isinstance(node, ast.If)
            and node is not guards[0]
            and not (isinstance(node.test, ast.Call) and ast.unparse(node.test.func) == "sys.argv[0].endswith")
        ):
            raise ValueError("console_unexpected_condition")
    if not any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "main"
        and not n.args
        and not n.keywords
        for n in ast.walk(guards[0])
    ):
        raise ValueError("console_missing_entrypoint_call")
    expected = ENTRYPOINTS[name][1]
    imports = [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    if (
        len(imports) != 1
        or imports[0].module != expected
        or [(a.name, a.asname) for a in imports[0].names] != [("main", None)]
    ):
        raise ValueError("console_entrypoint_mismatch")
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(a.name not in {"sys", "re"} or a.asname for a in node.names):
            raise ValueError("console_unexpected_import")
        if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
            raise ValueError("console_unexpected_definition")
        if isinstance(node, ast.Call):
            func = ast.unparse(node.func)
            if func not in {"main", "sys.exit", "re.sub", "sys.argv[0].endswith"}:
                raise ValueError("console_unexpected_call")
    return path, python


def git_roots(start: str | Path) -> list[str]:
    """Actual worktree then its same-Git primary; no environment anchors."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}

    def git(*args):
        try:
            proc = subprocess.run(
                ["git", "-C", str(start), "rev-parse", *args], capture_output=True, text=True, timeout=5, env=env
            )
            return proc.stdout.strip() if proc.returncode == 0 else ""
        except (OSError, subprocess.SubprocessError):
            return ""

    root = git("--show-toplevel")
    if not root:
        return []
    common = git("--path-format=absolute", "--git-common-dir")
    roots = [str(Path(root).resolve())]
    primary = str(Path(common).resolve().parent) if common else ""
    # A bare repo or a forged common-dir path is not a primary worktree.
    if primary and primary not in roots and (Path(primary) / ".git").is_dir():
        roots.append(primary)
    return roots


def _run_probe(python: Path, request: dict, *, timeout: float | None = None) -> dict:
    """Bound both wall time and allocated output, including a broken interpreter."""
    budget = probe_budget_seconds() if timeout is None else timeout
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="workbay-native-") as private:
        env = {key: os.environ[key] for key in ("SYSTEMROOT", "WINDIR") if key in os.environ}
        env.update(
            HOME=private,
            XDG_CACHE_HOME=private,
            TMPDIR=private,
            TEMP=private,
            PYTHONDONTWRITEBYTECODE="1",
            WORKBAY_HANDOFF_EMBEDDINGS_DISABLED="1",
        )
        proc = subprocess.Popen(
            [str(python), "-I", "-B", "-S", str(Path(__file__).resolve()), "--probe", json.dumps(request)],
            cwd=private,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=(os.name != "nt"),
        )
        output = []

        def kill():
            try:
                if os.name != "nt":
                    import signal

                    os.killpg(proc.pid, signal.SIGKILL)
                elif proc.poll() is None:
                    proc.kill()
            except ProcessLookupError:
                pass

        def read():
            data = bytearray()
            while len(data) <= MAX_OUTPUT:
                block = proc.stdout.read(min(65536, MAX_OUTPUT + 1 - len(data)))
                if not block:
                    break
                data.extend(block)
            output.append(bytes(data))
            if len(data) > MAX_OUTPUT:
                kill()

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        try:
            proc.wait(timeout=budget)
            reader.join(timeout=0.2)
            if reader.is_alive():
                kill()
                raise ValueError("probe_incomplete_output")
        except subprocess.TimeoutExpired as exc:
            raise ProbeTimeout(time.monotonic() - started, budget) from exc
        finally:
            if proc.poll() is None or reader.is_alive():
                kill()
            proc.wait(timeout=1)
            reader.join(timeout=1)
            # Closing a buffered reader held by another thread could hang.
            if not reader.is_alive():
                proc.stdout.close()
        if not output or len(output[0]) > MAX_OUTPUT:
            raise ValueError("probe_output_limit")
        result = _json(output[0].decode("utf-8"))
        if proc.returncode:
            detail = result.get("reason", "") if isinstance(result, dict) else ""
            raise ValueError("probe_nonzero:" + str(detail)[:120])
        return _validate_proof(result, request)


def _validate_proof(result, request):
    if not isinstance(result, dict) or set(result) != {
        "schema",
        "console",
        "python",
        "files",
        "capabilities",
        "providers",
        "stats",
    }:
        raise ValueError("probe_malformed")
    if (
        result["schema"] != "workbay/native-console@1"
        or result["console"] != request["console"]
        or result["python"] != request["python"]
    ):
        raise ValueError("probe_identity_mismatch")
    files = result["files"]
    if not isinstance(files, dict) or not 1 <= len(files) <= MAX_FILES:
        raise ValueError("probe_malformed_files")
    if any(
        not isinstance(f, str) or not Path(f).is_absolute() or not isinstance(d, str) or len(d) != 64
        for f, d in files.items()
    ):
        raise ValueError("probe_malformed_files")
    required = {"RuntimeConfig.for_workspace", "merge_embedding_env", "HandoffState", "LifecycleEvent"}
    if request["console"] == "mcp-workbay-orchestrator":
        required.update({"review_cycle_dispatch.dispatch", "resource_runtime.run_public", "PublicRequest"})
    if (
        not isinstance(result["capabilities"], list)
        or not all(isinstance(v, str) for v in result["capabilities"])
        or not required <= set(result["capabilities"])
    ):
        raise ValueError("probe_missing_capabilities")
    required_providers = {"workbay_handoff_mcp", "workbay_protocol"}
    if request["console"] == "mcp-workbay-orchestrator":
        required_providers.add("workbay_orchestrator_mcp")
    if not isinstance(result["providers"], dict) or set(result["providers"]) != required_providers:
        raise ValueError("probe_missing_providers")
    for top, provider in result["providers"].items():
        if not isinstance(provider, dict) or set(provider) != {"name", "version", "package", "editable"}:
            raise ValueError("probe_malformed_provider")
        if (
            not isinstance(provider["version"], str)
            or not provider["version"]
            or type(provider["editable"]) is not bool
        ):
            raise ValueError("probe_malformed_provider")
        if (
            not isinstance(provider["package"], str)
            or not Path(provider["package"]).is_absolute()
            or provider["package"] not in files
        ):
            raise ValueError("probe_unbound_provider")
    if (
        not isinstance(result["stats"], dict)
        or not set(files) <= result["stats"].keys()
        or any(result["stats"][f] is None for f in files)
    ):
        raise ValueError("probe_unbound_stats")
    return result


def uv_tool_bin_dirs() -> list[str]:
    """Existing uv install bin-dir order, shared with the external MCP router."""
    dirs = [os.environ.get("UV_TOOL_BIN_DIR"), os.environ.get("XDG_BIN_HOME")]
    if os.environ.get("XDG_DATA_HOME"):
        dirs.append(os.path.join(os.path.dirname(os.environ["XDG_DATA_HOME"]), "bin"))
    dirs.append(os.path.join(os.path.expanduser("~"), ".local", "bin"))
    return list(dict.fromkeys(d for d in dirs if d))


def installed_tools(name: str) -> list[str]:
    return [
        str(Path(directory).joinpath(*rel))
        for directory in uv_tool_bin_dirs()
        for rel in ((name,), ("bin", name), ("Scripts", name + ".exe"))
    ]


def select_console(
    name: str, roots: list[str], *, tools=None, override: str | None = None, workspace: str | Path | None = None
) -> str:
    """Return a qualified native console, or a typed hold. Never install/resolve."""
    if name not in ENTRYPOINTS:
        raise ValueError("unknown_native_console")
    candidates = (
        [override]
        if override
        else [
            str(Path(root) / ".venv" / folder / filename)
            for root in roots
            for folder, filename in [("bin", name), ("Scripts", name + ".exe"), ("Scripts", name)]
        ]
        + (installed_tools(name) if tools is None else list(tools))
    )
    if os.environ.get("PYTHONHOME"):
        raise NativeRuntimeUnavailable(name, ["pythonhome_override_refused"])
    python_path = os.environ.get("PYTHONPATH", "").split(os.pathsep) if os.environ.get("PYTHONPATH") else []
    if any(not Path(p).is_absolute() for p in python_path):
        raise NativeRuntimeUnavailable(name, ["relative_pythonpath_refused"])
    reasons = []
    # Source authority follows the shipped hook, not an arbitrary caller's env.
    anchors = git_roots(Path(__file__).resolve().parent)
    if len(candidates) > 32:
        raise NativeRuntimeUnavailable(name, ["candidate_limit"])
    started = time.monotonic()
    budget = probe_budget_seconds()
    deadline = started + budget
    timed_out = False
    for candidate in dict.fromkeys(candidates):
        if time.monotonic() >= deadline:
            timed_out = True
            reasons.append("unverified:resolution_deadline")
            break
        try:
            path, python = _console_details(candidate, name)
            before = {
                str(p): _binding(p)
                for p in (Path(candidate).absolute(), path, python, path.parent.parent / "pyvenv.cfg")
            }
            request = {
                "console": name,
                "script": str(path),
                "python": str(python),
                "anchors": anchors,
                "workspace": str(Path(workspace or (roots[0] if roots else ".")).absolute()),
                "python_path": python_path,
                "embedding_process": {
                    k: os.environ[k]
                    for k in ("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED", "WORKBAY_REINJECT_SEMANTIC")
                    if k in os.environ
                },
            }
            key = {
                "request": request,
                "workspace": request["workspace"],
                "candidate": str(Path(candidate).absolute()),
                "interpreter": before[str(python)],
            }
            receipt_path = _receipt_path(path, name)
            proof = _read_receipt(receipt_path, key)
            if proof is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProbeTimeout(time.monotonic() - started, budget)
                proof = _run_probe(python, request, timeout=remaining)
                # The child binds stats before hashing, then checks them after
                # its full integrity pass. Recheck those identities here without
                # hashing the same install again in the parent.
                _check_stats(proof["stats"])
                _check_stats(before)
                proof["stats"].update(before)
                _write_receipt(receipt_path, key, proof)
            _check_stats(proof["stats"])
            _check_stats(before)
            if Path(candidate).resolve() != path:
                raise ValueError("probe_identity_drift")
            return QualifiedConsole(str(Path(candidate).absolute()), name, proof["stats"])
        except (
            OSError,
            ValueError,
            TypeError,
            AttributeError,
            RecursionError,
            ImportError,
            SyntaxError,
            KeyError,
            zipfile.BadZipFile,
            subprocess.SubprocessError,
        ) as exc:
            if isinstance(exc, ProbeTimeout):
                timed_out = True
            if isinstance(exc, FileNotFoundError):
                reasons.append(f"{candidate}: missing")
            else:
                reasons.append(f"{candidate}: {str(exc)[:160]}")
    if timed_out:
        raise NativeRuntimeUnverified(name, reasons, time.monotonic() - started, budget)
    raise NativeRuntimeUnavailable(name, reasons or ["no installed candidates"])


def _probe(request: dict) -> dict:
    """Executed by the candidate interpreter, with site startup disabled."""
    import base64
    import importlib
    import importlib.abc
    import importlib.metadata as metadata
    import importlib.util
    from urllib.parse import unquote, urlparse

    script = Path(request["script"])
    python = Path(request["python"])
    if Path(sys.executable).absolute() != python.absolute():
        raise ValueError("interpreter_identity_mismatch")
    prefix = script.parent.parent
    sites = [
        p
        for p in (
            prefix / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages",
            prefix / "Lib" / "site-packages",
        )
        if p.is_dir()
    ]
    if len(sites) != 1:
        raise ValueError("interpreter_site_layout")
    site = sites[0]
    files = {}
    stats = {}

    def watch(path):
        path = Path(path).absolute()
        binding = _binding(path)
        if str(path) in stats and stats[str(path)] != binding:
            raise ValueError("probe_identity_drift")
        stats[str(path)] = binding
        if len(stats) > MAX_FILES:
            raise ValueError("provider_file_limit")

    # Directory stats detect newly introduced .pth files and import shadows;
    # negative policy paths are bound too, so a new manifest forces re-probing.
    for path in (
        site,
        script.parent,
        prefix / "Scripts",
        prefix / "bin",
        *[Path(p) for p in request.get("python_path", [])],
    ):
        watch(path)

    def remember(path):
        path = Path(path).absolute()
        if len(files) >= MAX_FILES:
            raise ValueError("provider_file_limit")
        watch(path)
        if path.is_dir():
            for root, dirs, _ in os.walk(path, followlinks=False):
                dirs[:] = [d for d in dirs if d != "__pycache__"]
                watch(Path(root))
        files[str(path)] = _fingerprint(path)
        watch(path)
        return path

    remember(Path(__file__).resolve())
    remember(Path(__file__).with_name("_interp.py"))
    remember(script)
    remember(python)
    remember(prefix / "pyvenv.cfg")
    sys.path[:0] = [str(script.parent), *request.get("python_path", [])]
    sys.path.append(str(site))
    # The existing _interp origin policy admits same-checkout editable sources
    # and noneditable prefix installs. Reuse it rather than weakening that rule.
    spec = importlib.util.spec_from_file_location("_native_origin_policy", Path(__file__).with_name("_interp.py"))
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    dist_names = {"workbay_handoff_mcp": "mcp-workbay-handoff", "workbay_protocol": "workbay-protocol"}
    if request["console"] == "mcp-workbay-orchestrator":
        dist_names["workbay_orchestrator_mcp"] = "mcp-workbay-orchestrator"
    providers = {}
    packages = {}
    for top, name in dist_names.items():
        # Bound metadata before asking importlib.metadata to parse it. Only the
        # named provider's dist-info is relevant; never scan arbitrary METADATA.
        prefix_name = name.replace("-", "_") + "-"
        infos = [
            p
            for p in site.iterdir()
            if p.name.replace("-", "_").startswith(prefix_name.replace("-", "_")) and p.name.endswith(".dist-info")
        ]
        if len(infos) != 1 or not infos[0].resolve().is_relative_to(site.resolve()):
            raise ValueError("provider_missing_or_ambiguous:" + name)
        info = infos[0]
        for file in info.iterdir():
            if file.is_file():
                remember(file)
        dist = metadata.PathDistribution(info)
        if (dist.metadata["Name"] or "").replace("_", "-").lower() != name:
            raise ValueError("provider_metadata_name_mismatch")
        direct = _json(dist.read_text("direct_url.json") or "{}")
        editable = direct.get("dir_info", {}).get("editable") is True
        source = site
        if editable:
            url = urlparse(direct.get("url", ""))
            if url.scheme != "file" or url.netloc not in {"", "localhost"}:
                raise ValueError("editable_source_invalid")
            source = Path(unquote(url.path)).resolve()
            if not guard._origin_under_trust_roots(str(source), tuple(request["anchors"]), (), editable=True):
                raise ValueError("editable_provider_untrusted")
            source = source / "src"
        package = source / top
        if not package.is_dir() or not guard._origin_under_trust_roots(
            str(package), tuple(request["anchors"]), (str(site.resolve()),), editable=editable
        ):
            raise ValueError("provider_origin_untrusted:" + top)
        records = {str(Path(dist.locate_file(f)).absolute()): f for f in (dist.files or [])}
        remember(package)
        remember(info)
        packages[top] = package
        if not editable:
            # RECORD is the wheel's own source manifest: each recorded module must
            # be on disk with its bound digest and size, not only the modules a
            # directory walk happens to find. dist.files drops rows whose file is
            # missing, so read the bounded RECORD itself. Bytecode stays inert.
            import csv
            from pathlib import PurePosixPath

            rows = list(csv.reader(_bytes(info / "RECORD").decode("utf-8").splitlines()))
            owned = [
                row for row in rows if row and PurePosixPath(row[0]).parts[:1] == (top,) and row[0].endswith(".py")
            ]
            if not owned or len(owned) > MAX_FILES:
                raise ValueError("provider_source_limit")
            if any(len(row) != 3 or ".." in PurePosixPath(row[0]).parts for row in owned):
                raise ValueError("provider_record_escape")
            if len({str(PurePosixPath(row[0])) for row in owned}) != len(owned):
                raise ValueError("provider_record_duplicate")
            for relative, digest, size in owned:
                path = Path(dist.locate_file(relative)).absolute()
                if not os.path.lexists(path):
                    raise ValueError("provider_record_source_missing:" + relative)
                if not path.resolve().is_relative_to(package.resolve()):
                    raise ValueError("provider_source_escape")
                if not digest.startswith("sha256=") or not size.isdigit():
                    raise ValueError("provider_record_missing")
                remember(path)
                if (
                    base64.urlsafe_b64encode(bytes.fromhex(files[str(path)])).decode().rstrip("=")
                    != digest[len("sha256=") :]
                ):
                    raise ValueError("provider_record_mismatch")
                if path.stat().st_size != int(size):
                    raise ValueError("provider_record_size_mismatch")
        paths = list(package.rglob("*.py"))
        if not paths or len(paths) > MAX_FILES:
            raise ValueError("provider_source_limit")
        for path in paths:
            if not path.resolve().is_relative_to(package.resolve()):
                raise ValueError("provider_source_escape")
            remember(path)
            if not editable:
                record = records.get(str(path.absolute()))
                if record is None or record.hash is None or record.hash.mode != "sha256":
                    raise ValueError("provider_record_missing")
                actual = base64.urlsafe_b64encode(bytes.fromhex(files[str(path.absolute())])).decode().rstrip("=")
                if actual != record.hash.value:
                    raise ValueError("provider_record_mismatch")
        providers[top] = {
            "name": name,
            "version": dist.version,
            "package": str(package.resolve()),
            "editable": editable,
        }
        if name == ENTRYPOINTS[request["console"]][0]:
            eps = [e for e in dist.entry_points if e.group == "console_scripts" and e.name == request["console"]]
            if len(eps) != 1 or eps[0].value != ENTRYPOINTS[request["console"]][1] + ":main":
                raise ValueError("provider_entrypoint_mismatch")
    # Only static .pth entries are needed for supported editable source layouts.
    # Skip virtualenv's distutils compatibility hook; never execute .pth code.
    for pth in site.glob("*.pth"):
        remember(pth)
        for line in _bytes(pth, 65536).decode("utf-8").splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            if line.startswith(("import ", "import\t")):
                if pth.name == "_virtualenv.pth" and line.strip() == "import _virtualenv":
                    continue
                raise ValueError("unsupported_executable_pth")
            path = (site / line).resolve()
            sys.path.append(str(path))

    # Bind potential shadows too: creating __init__.py in an existing namespace
    # directory changes that directory, not its search root. A receipt must not
    # bypass the foreign-import refusal that a fresh probe would have produced.
    for entry in sys.path:
        base = Path(entry).absolute()
        watch(base)
        if base.is_dir():
            for top in dist_names:
                watch(base / (top + ".py"))
                watch(base / top)
                if (base / top).is_dir():
                    watch(base / top / "__init__.py")

    # Mediate every WorkBay import before any candidate source is executed.
    class ReadSource(importlib.machinery.SourceFileLoader):
        # -B prevents writes, but also bypass existing bytecode: source facts
        # must describe the code actually imported, even with a stale .pyc.
        def get_code(self, fullname):
            return compile(_bytes(Path(self.path)), self.path, "exec", dont_inherit=True)

    class Origins(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            top = fullname.split(".")[0]
            if not top.startswith("workbay_"):
                return None
            if top not in providers:
                raise ImportError("undeclared_provider:" + top)
            result = importlib.machinery.PathFinder.find_spec(fullname, path)
            if (
                result is None
                or result.origin is None
                or not Path(result.origin).resolve().is_relative_to(Path(providers[top]["package"]))
            ):
                raise ImportError("foreign_import:" + fullname)
            origin = Path(result.origin).absolute()
            if origin.suffix != ".py" or str(origin) not in files or _fingerprint(origin) != files[str(origin)]:
                raise ImportError("unowned_import:" + fullname)
            result.loader = ReadSource(fullname, str(origin))
            return result

    sys.meta_path.insert(0, Origins())

    def readonly(event, args):
        if event == "open":
            mode, flags = args[1], args[2]
            if (isinstance(mode, str) and any(c in mode for c in "wax+")) or (
                isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
            ):
                raise PermissionError("probe_write_refused")
        if event.startswith(("socket.", "subprocess.", "sqlite3.connect")) or event in {
            "os.system",
            "os.mkdir",
            "os.remove",
            "os.rename",
            "os.rmdir",
            "os.symlink",
            "os.link",
            "os.chmod",
            "os.chown",
            "os.truncate",
            "os.putenv",
            "os.unsetenv",
            "os.exec",
            "os.fork",
            "os.posix_spawn",
        }:
            raise PermissionError("probe_effect_refused:" + event)

    sys.addaudithook(readonly)

    def public(module, name):
        value = getattr(importlib.import_module(module), name)
        if not callable(value):
            raise ValueError("missing_public_callable:" + module + ":" + name)
        return value

    entry = ENTRYPOINTS[request["console"]][1]
    cli = packages[entry.split(".")[0]] / "cli.py"
    cli_tree = ast.parse(_bytes(cli))
    if not any(isinstance(n, ast.FunctionDef) and n.name == "main" for n in cli_tree.body):
        raise ValueError("missing_public_entrypoint")

    # The console imports its entrypoint eagerly, so every module-level WorkBay
    # import there must name a bound provider source (editables have no RECORD).
    def bound(module):
        parts = module.split(".")
        base = packages[parts[0]].joinpath(*parts[1:])
        return any(str(p.absolute()) in files for p in (base / "__init__.py", base.with_suffix(".py")))

    for node in cli_tree.body:
        if isinstance(node, ast.ImportFrom):
            module = (
                ".".join(
                    entry.split(".")[: len(entry.split(".")) - node.level] + ([node.module] if node.module else [])
                )
                if node.level
                else node.module
            )
            names = [
                module,
                *(
                    module + "." + a.name
                    for a in node.names
                    if module.split(".")[0] in packages and bound(module + "." + a.name)
                ),
            ]
        elif isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        else:
            continue
        for module in names:
            if module.split(".")[0].startswith("workbay_") and (
                module.split(".")[0] not in packages or not bound(module)
            ):
                raise ValueError("entrypoint_dependency_missing:" + module)
    config = public("workbay_handoff_mcp.config", "RuntimeConfig")
    if not callable(config.for_workspace) or config.__dataclass_fields__["tool_profile"].default != "all":
        raise ValueError("unsupported_native_profile")
    merge = public("workbay_handoff_mcp.embedding_env", "merge_embedding_env")
    state = public("workbay_protocol.handoff", "HandoffState")
    if "active" not in state.model_fields:
        raise ValueError("missing_handoff_schema")
    event = public("workbay_protocol.resource_lifecycle", "LifecycleEvent")
    if not {"event_id", "resource", "transaction_ref", "phase"} <= event.model_fields.keys():
        raise ValueError("missing_native_event_schema")
    capabilities = ["RuntimeConfig.for_workspace", "merge_embedding_env", "HandoffState", "LifecycleEvent"]
    if request["console"] == "mcp-workbay-orchestrator":
        public("workbay_orchestrator_mcp.orchestration.review_cycle_dispatch", "dispatch")
        public("workbay_orchestrator_mcp.orchestration.resource_runtime", "run_public")
        public("workbay_orchestrator_mcp.orchestration.resource_runtime", "PublicRequest")
        capabilities += ["review_cycle_dispatch.dispatch", "resource_runtime.run_public", "PublicRequest"]
    # Reuse the public install-policy reader. Optional models are never imported.
    policy_module = importlib.import_module("workbay_protocol.embedding_policy")
    workspace = Path(request["workspace"])
    from workbay_protocol.paths import MANIFEST_NAME_PRECEDENCE

    for filename in MANIFEST_NAME_PRECEDENCE:
        path = workspace / filename
        watch(path)
        if path.exists():
            remember(path)
    watch(workspace / ".workbay/embedding.env")
    if (workspace / ".workbay/embedding.env").exists():
        remember(workspace / ".workbay/embedding.env")
    policy = policy_module.load_embeddings_policy(workspace)
    if policy.status not in {"ok", "manifest_missing", "field_missing"}:
        raise ValueError("embedding_profile_unreadable")
    if policy.embeddings_mode == "verified" and policy.embeddings_policy != "off":
        env = merge(request["workspace"], process_env=request.get("embedding_process", {}))
        disabled = (
            env.get("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED", "").lower() in {"1", "true", "yes", "on"}
            or env.get("WORKBAY_REINJECT_SEMANTIC") == "0"
        )
        if not disabled:
            for name in ("onnxruntime",):
                if importlib.util.find_spec(name) is None:
                    raise ValueError("verified_embedding_capability_missing:" + name)
            capabilities.append("verified_embeddings_modules")
    for filename, digest in files.items():
        if _fingerprint(Path(filename)) != digest:
            raise ValueError("probe_identity_drift")
    _check_stats(stats)
    return {
        "schema": "workbay/native-console@1",
        "console": request["console"],
        "python": str(python),
        "providers": providers,
        "files": files,
        "capabilities": capabilities,
        "stats": stats,
    }


if __name__ == "__main__":
    try:
        result = _probe(_json(sys.argv[2]))
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__, "reason": str(exc)[:160]}))
        raise SystemExit(1)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
