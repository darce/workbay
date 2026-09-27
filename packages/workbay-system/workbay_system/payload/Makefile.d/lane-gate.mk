# Loaded by the monorepo root via payload/Makefile.d/*.mk.
# Root Makefile.d/*.mk is gitignored and is not included while
# packages/workbay-system/Makefile.d exists; this is the rail copy.
#
# lane-check / lane-intake / lane-refresh
#
# Judge vs subject
# ----------------
# The worker daemon runs:
#   make -f <orchestrator_root>/Makefile -C <lane-worktree> lane-check TASK=... LANE=...
# The tree that owns the `-f` Makefile is the **judge**. The `-C` worktree
# (CURDIR) is the **subject**.
#
# lane-check verifies the **subject**. Declared test_commands run with cwd
# equal to the subject. Their imports must see the subject's in-tree
# `packages/*/src` directories (so a lane's own edits are what verification
# loads) and must keep inherited PYTHONPATH entries the lane's commands need.
#
# The judging instrument is never taken from the subject or the ambient
# environment:
#   * the rail fragment is included from the judge (root Makefile)
#   * ORCHESTRATOR_ROOT is derived from MAKEFILE_LIST / JUDGE_ROOT
#     (a command-line assignment still wins; an inherited environment
#     value does not — `?=` would let the environment win)
#   * the gate script imports load_manifest / resolve_lane_python from
#     the judge's orchestrator src
#   * the verification interpreter is resolve_lane_python(judge)
#
# Status tokens (stdout/stderr) are the discriminators; GNU make maps every
# non-zero recipe exit to its own exit 2:
#   LANE_GATE_STATUS=passed          exit 0  — declared verification ran and passed
#   LANE_GATE_STATUS=validated       exit 0  — intake/refresh preconditions validated
#   LANE_GATE_STATUS=skipped         exit 2  — lane declares no behavioral verification
#   LANE_GATE_STATUS=failed          exit 1  — declared verification ran and failed
#   LANE_GATE_STATUS=system_error    exit 3  — interpreter/manifest/lane unusable
#
# Alongside every terminal status, the gate prints
#   LANE_GATE_STYLE_DEBT=<n>         count of advisory (style/type) failures
# plus LANE_GATE_ADVISORY lines for downgraded commands and classifier
# availability. The status
# vocabulary is deliberately unchanged: a consumer that only knows the old
# tokens keeps working, and one that wants the debt reads its own field
# instead of parsing prose.
# Snapshot refusals retain exit 3 and emit LANE_GATE_DETAIL=<cause> on stderr:
# git_probe_timeout, ambient_git_env:<VAR>, ancestor_repository=<path>,
# git_rc=<n>:<first stderr line>, oserror:<text>, or unsnapshotable_entry=<name>.
# Non-repository advisories also carry a detail; clean repository passes do not.
# LANE_GATE_GIT_PROBE_TIMEOUT bounds each Git probe in seconds (default 60;
# empty, invalid, nonpositive and nonfinite values use the default).
# Every terminal status also carries
#   LANE_GATE_CONCURRENCY=<n>        gates seen running at once, this one included
# and a failure observed with n > 1 is additionally tagged
#   LANE_GATE_ADVISORY=failed_under_concurrent_verification
# The status vocabulary is unchanged and the failure stays `failed`: the tag
# tells a consumer to re-run serially before treating the red as a landing
# blocker, it does not downgrade it. Measured 2026-09-06: two orchestrator gates
# started together on one host disagreed by 53 test nodes, and re-running that
# exact node set alone passed 95 of 96, so 52 of the 53 were false. All of them
# were in timing-sensitive modules that a saturated box starves.
#
# LANE_GATE_ENFORCE_STYLE accepts 1/true/yes/on to restore blocking style
# enforcement. Empty/0/false/no/off retain the mandated advisory policy;
# unknown values also remain advisory so configuration drift cannot turn
# ruff/mypy debt into an accidental merge blocker.
# Skipped and passed do not share a representation. Unknown/unreadable/
# unresolvable and confirmed-failing do not share a representation.
#
# Load sentinel: `-include $(JUDGE_ROOT)/…/Makefile.d/*.mk` is silent when
# this fragment is absent. Make then reports "No rule to make target
# lane-check", the consumer used to classify that as skipped, and a
# failing lane was handed off as merge_ready. Consumers/the monorepo
# assert this sentinel after the include, the same way check.mk does.

WORKBAY_LANE_GATE_MK_LOADED := 1

ORCHESTRATOR_ROOT := $(if $(JUDGE_ROOT),$(JUDGE_ROOT),$(abspath $(dir $(firstword $(MAKEFILE_LIST)))))
# Pin discovery in later fragments to the judge, avoiding subject PATH probes.
WORKBAY_REPO_ROOT := $(if $(JUDGE_ROOT),$(JUDGE_ROOT),$(ORCHESTRATOR_ROOT))
LANE_GATE_BOOTSTRAP_PYTHON := python3

export ORCHESTRATOR_ROOT
export TASK
export LANE
export LANE_BASE_SHA

define LANE_GATE_PY
import atexit
import json
import hashlib
import math
import stat
import os
import re
import signal
import shlex
import subprocess
import sys

style_debt = 0
gate_concurrency = 1
gate_registration = None

# Keep this tuple in parity with
# packages/workbay-system/workbay_system/payload/scripts/workbay_lifecycle/interpreter_skew.py.
_GIT_ENV_OVERRIDE_KEYS = (
    "GIT_DIR",
    "GIT_COMMON_DIR",
    "GIT_WORK_TREE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_INDEX_FILE",
    "GIT_NAMESPACE",
    "GIT_CEILING_DIRECTORIES",
)

def _git_env():
    env = {**os.environ, "LC_ALL": "C"}
    for name in _GIT_ENV_OVERRIDE_KEYS:
        env.pop(name, None)
    return env

def _emit(status, reason=None, extra=""):
    print("LANE_GATE_STYLE_DEBT=" + str(style_debt), flush=True)
    print("LANE_GATE_CONCURRENCY=" + str(gate_concurrency), flush=True)
    if reason:
        print("LANE_GATE_REASON=" + reason, flush=True)
    print("LANE_GATE_STATUS=" + status, flush=True)
    print("LANE_GATE_TERMINAL=1", flush=True)
    if extra:
        print(extra, file=sys.stderr, flush=True)

def _system(reason, extra=""):
    _emit("system_error", reason, extra)
    raise SystemExit(3)

def _refuse_ambient_git_env():
    for name in _GIT_ENV_OVERRIDE_KEYS:
        if os.environ.get(name):
            _system("worktree_snapshot_unavailable", _detail("ambient_git_env:" + name))

# Git failures retain exit 3; detail identifies the cause independently.
# LANE_GATE_GIT_PROBE_TIMEOUT bounds each metadata probe (default 60 seconds).
def _detail(cause):
    return "LANE_GATE_DETAIL=" + cause

def _first_line(text):
    if isinstance(text, bytes):
        text = os.fsdecode(text)
    return next((line.strip() for line in (text or "").splitlines() if line.strip()), "")

def _git_probe_timeout():
    try:
        value = float(os.environ.get("LANE_GATE_GIT_PROBE_TIMEOUT", "60"))
    except ValueError:
        return 60.0
    return value if math.isfinite(value) and value > 0 else 60.0

def _unsafe_task_ref(value):
    if not value:
        return True
    if value in (".", "..") or ".." in value:
        return True
    if "/" in value:
        return True
    if os.path.isabs(value):
        return True
    if os.sep in value or (os.altsep and os.altsep in value):
        return True
    return False

def _unsafe_lane_id(value):
    # Lane is a dict key, not a path segment. Live ids are slash-namespaced
    # (lane/some-id). Still refuse empty, .., absolute, and extra separators.
    if not value:
        return True
    if value in (".", "..") or ".." in value:
        return True
    if os.path.isabs(value):
        return True
    if value.startswith("/") or value.endswith("/"):
        return True
    if value.count("/") > 1:
        return True
    if os.altsep and os.altsep in value:
        return True
    return False

action = sys.argv[1] if len(sys.argv) > 1 else "check"
task = os.environ.get("TASK", "").strip()
lane = os.environ.get("LANE", "").strip()
root = os.environ.get("ORCHESTRATOR_ROOT", "").strip()
if not task or not lane or not root:
    _system("missing_task_or_lane")
if _unsafe_task_ref(task):
    _system("unreadable_manifest", "task ref must be a single path segment")
if _unsafe_lane_id(lane):
    _system("unreadable_manifest", "lane id must not be a path escape")

src_roots = [
    os.path.join(root, "packages", "mcp-workbay-orchestrator", "src"),
    os.path.join(root, "packages", "workbay-protocol", "src"),
]
for src_root in src_roots:
    if not os.path.isdir(src_root):
        _system("unresolvable_interpreter", "required source root is missing: %r" % (src_root,))
inherited_raw = os.environ.get("PYTHONPATH", "")
inherited = [os.path.abspath(p) for p in inherited_raw.split(os.pathsep) if p]
# Isolate the *gate* process from inherited PYTHONPATH so the instrument
# cannot be swapped by the ambient environment. Do not clobber os.environ:
# child test_commands need those entries.
sys.path[:] = [p for p in sys.path if p and os.path.abspath(p) not in inherited]
sys.path[:0] = src_roots

try:
    from workbay_orchestrator_mcp.orchestration._env import resolve_lane_python
except Exception as exc:
    _system("unresolvable_interpreter", str(exc))

git_probe_timeout = _git_probe_timeout()
git_env = _git_env()
try:
    interp = resolve_lane_python(root, git_timeout=git_probe_timeout, git_env=git_env)
except Exception as exc:
    _system("unresolvable_interpreter", str(exc))

if not interp or not os.path.exists(interp):
    _system("unresolvable_interpreter", "resolved path missing: %r" % (interp,))

print("LANE_GATE_PYTHON=" + interp, flush=True)

# Operator policy (internal): ruff/mypy findings are post-launch
# cleanup, not merge blockers. The classifier is narrow and positive -- only a
# command it can affirmatively name as style work is downgraded to advisory;
# everything else, including composites, still blocks. If the judge tree
# predates the module, degrade to the old all-blocking behaviour rather than
# refusing the gate, and say so instead of failing silently [OBS-08].
try:
    from workbay_orchestrator_mcp.orchestration.lane_gate_style import (
        is_style_only_command,
    )
except Exception:
    print("LANE_GATE_STYLE_CLASSIFIER=unavailable", flush=True)
    print("LANE_GATE_ADVISORY=style_classifier_unavailable", flush=True)
    def is_style_only_command(_cmd, **_kwargs):
        return False

enforce_style = os.environ.get("LANE_GATE_ENFORCE_STYLE", "").strip().lower() in (
    "1", "true", "yes", "on",
)

try:
    from workbay_orchestrator_mcp.orchestration.lane_manifest import load_manifest
    manifest = load_manifest(task, orchestrator_root=root)
except FileNotFoundError as exc:
    _system("unreadable_manifest", str(exc))
except Exception as exc:
    _system("unreadable_manifest", str(exc))

lanes = manifest.get("lanes") if isinstance(manifest, dict) else None
if not isinstance(lanes, dict) or lane not in lanes:
    _system("unknown_lane", "lane %r is not in the manifest" % (lane,))

if action in ("intake", "refresh"):
    # The intake caller holds the shared landing mutation lock throughout
    # this subprocess and the subsequent landing. Recheck its evidence here,
    # after prerequisite loading, before permitting that mutation.
    _refuse_ambient_git_env()
    validated_base = os.environ.get("LANE_BASE_SHA", "").strip()
    if validated_base:
        try:
            current = subprocess.run(
                ["git", "-C", root, "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=git_probe_timeout,
                check=False, env=git_env,
            )
        except subprocess.TimeoutExpired:
            _system("unresolvable_base", _detail("git_probe_timeout"))
        except OSError as exc:
            _system("unresolvable_base", str(exc))
        if current.returncode or current.stdout.strip() != validated_base:
            _emit("failed", "base_moved")
            raise SystemExit(1)
    _emit("validated")
    raise SystemExit(0)

commands = []
spec = lanes.get(lane) if isinstance(lanes.get(lane), dict) else {}
raw = spec.get("test_commands")
if not isinstance(raw, list):
    _system(
        "unreadable_manifest",
        "lane %r test_commands must be a list" % (lane,),
    )
if raw == []:
    # Query the same repository-scoped store as lane_prompt using the judge's
    # resolved interpreter (the bootstrap Python need not have dependencies).
    import json
    lookup = '''import json, sys
from pathlib import Path
root, task, lane = sys.argv[1:]
sys.path.insert(0, str(Path(root) / "packages/mcp-workbay-orchestrator/src"))
from workbay_handoff_mcp import RuntimeConfig, configure_runtime
from workbay_orchestrator_mcp.lanes import get_lane_activity
configure_runtime(RuntimeConfig.for_repo(root))
activity = get_lane_activity(lane_id=lane, task_ref=task, sections="lane")
if not activity.get("ok") and activity.get("error") != "Lane not found for task_ref.":
    raise RuntimeError(str(activity))
print(json.dumps(str((activity.get("lane") or {}).get("test_cmd") or "").strip()))
'''
    try:
        result = subprocess.run(
            [interp, "-I", "-c", lookup, root, task, lane],
            cwd=root, capture_output=True, text=True, timeout=git_probe_timeout,
            check=True, env=git_env,
        )
        row_command = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        _system("unreadable_lane_store", str(exc))
    if row_command:
        commands = [row_command]
else:
    for index, item in enumerate(raw):
        if not isinstance(item, str):
            _system(
                "unreadable_manifest",
                "lane %r test_commands[%d] must be a string" % (lane, index),
            )
        if not item.strip():
            _system(
                "unreadable_manifest",
                "lane %r test_commands[%d] must not be blank" % (lane, index),
            )
    commands = list(raw)

if not commands:
    _emit("skipped")
    raise SystemExit(2)

env = os.environ.copy()
try:
    from pathlib import Path
    from workbay_orchestrator_mcp.orchestration._env import pythonpath_env
    env = pythonpath_env(Path(root), task_ref=task, lane_id=lane)
except Exception:
    pass

# Subject package trees first so lane edits win over judge copies.
subject = os.getcwd()
subject_srcs = []
packages_root = os.path.join(subject, "packages")
if os.path.isdir(packages_root):
    for name in sorted(os.listdir(packages_root)):
        candidate = os.path.join(packages_root, name, "src")
        if os.path.isdir(candidate):
            subject_srcs.append(candidate)
existing = env.get("PYTHONPATH", "")
env["PYTHONPATH"] = os.pathsep.join([p for p in [*subject_srcs, existing] if p])
env["PATH"] = os.path.dirname(interp) + os.pathsep + env.get("PATH", "")
env["LANE_GATE_PYTHON"] = interp

def _truthy_env(name):
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")

_selected_cwds = []
_selected_envs = []

def _emit_selection(mode, reason, command="", cwd="", package_root=""):
    print("LANE_GATE_SELECTION_MODE=" + mode, flush=True)
    print("LANE_GATE_SELECTION_REASON=" + reason, flush=True)
    if command:
        print("LANE_GATE_SELECTION_COMMAND=" + command, flush=True)
    if cwd:
        print("LANE_GATE_SELECTION_CWD=" + cwd, flush=True)
    if package_root:
        print("LANE_GATE_SELECTION_PACKAGE_ROOT=" + package_root, flush=True)

def _integration_ref():
    env_ref = os.environ.get("LANE_BASE_SHA", "").strip()
    if env_ref:
        return env_ref
    for key in ("base_sha", "base_ref", "target_sha", "target_ref", "integration_ref"):
        value = spec.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def _git_subject(args):
    result = subprocess.run(
        ["git", "-C", subject] + args,
        capture_output=True, text=True, timeout=git_probe_timeout,
        check=False, env=git_env,
    )
    if result.returncode != 0:
        raise RuntimeError(_first_line(result.stderr) or _first_line(result.stdout) or "git failed")
    return result.stdout.strip()

def _apply_merge_gate_selection(commands):
    global _selected_cwds, _selected_envs
    if _truthy_env("LANE_CHECK_FULL_SUITE"):
        _emit_selection("full", "release_cut", commands[0] if commands else "")
        return commands
    integration_ref = _integration_ref()
    if not integration_ref:
        _emit_selection("full", "integration_ref_unresolved")
        return commands
    try:
        merge_base = _git_subject(["merge-base", "HEAD", integration_ref])
        changed = _git_subject(["diff", "--name-only", merge_base + "..HEAD"])
        changed_paths = [path for path in changed.splitlines() if path.strip()]
    except Exception:
        _emit_selection("full", "git_probe_failed")
        return commands
    try:
        from workbay_orchestrator_mcp.orchestration.merge_gate_test_selection import (
            resolve_lane_check_command,
        )
    except Exception:
        _emit_selection("full", "selector_unavailable")
        return commands
    selected_commands = []
    selected_cwds = []
    selected_envs = []
    raw_app_root = spec.get("app_root")
    app_root = raw_app_root.strip() if isinstance(raw_app_root, str) else ""
    for cmd in commands:
        try:
            result = resolve_lane_check_command(
                cmd,
                changed_paths=changed_paths,
                merge_base=merge_base,
                integration_ref=integration_ref,
                package_root=subject,
                app_root=app_root or None,
            )
        except Exception:
            _emit_selection("full", "selector_exception")
            selected_commands.append(cmd)
            selected_cwds.append(None)
            selected_envs.append({})
            continue
        result_cwd = getattr(result, "cwd", None) or None
        result_env = dict(getattr(result, "env", ()) or ())
        result_package_root = getattr(result, "package_root", None) or ""
        _emit_selection(
            result.mode,
            result.reason,
            result.command or "",
            result_cwd or "",
            result_package_root,
        )
        if result.mode == "none":
            _emit("skipped", result.reason or "no_reachable_tests")
            raise SystemExit(2)
        if result.command:
            selected_commands.append(result.command)
        else:
            selected_commands.append(cmd)
        selected_cwds.append(result_cwd)
        selected_envs.append(result_env)
    _selected_cwds = selected_cwds
    _selected_envs = selected_envs
    return selected_commands or commands

try:
    commands = _apply_merge_gate_selection(commands)
except Exception:
    _emit_selection("full", "selector_exception")

# GATECONC. A gate sharing the host with another gate is measurably unreliable,
# so the run reports the overlap rather than refusing: lane parallelism is the
# point of the system and a gate that declined to run under it would stall every
# wave. Presence is not the test -- a crashed gate leaves its file behind, so a
# peer counts only while its pid is alive, and a dead one is reaped on sight.
LANE_GATE_REGISTRY = os.path.join(".task-state", "lane-gate-active")

def _registry_dir():
    return os.path.join(root, LANE_GATE_REGISTRY)

def _pid_is_live(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # EPERM means a live process we do not own. Anything else is unknown,
        # and an unknown peer must not be reaped out from under its owner.
        return True
    return True

def _register_gate():
    """Announce this gate. A registry we cannot write degrades to solo, loudly."""
    directory = _registry_dir()
    path = os.path.join(directory, "%d.json" % (os.getpid(),))
    try:
        os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "task": task, "lane": lane}))
    except OSError as exc:
        print("LANE_GATE_ADVISORY=concurrency_registry_unavailable", flush=True)
        print("LANE_GATE_REGISTRY_ERROR=" + str(exc), file=sys.stderr, flush=True)
        return None
    return path

def _live_peer_count():
    """Live registrations other than ours; reap the dead so they never inflate it."""
    directory = _registry_dir()
    try:
        names = os.listdir(directory)
    except OSError:
        return 0
    peers = 0
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            pid = int(name[: -len(".json")])
        except ValueError:
            continue
        if pid == os.getpid():
            continue
        if _pid_is_live(pid):
            peers += 1
            continue
        try:
            os.unlink(os.path.join(directory, name))
        except OSError:
            pass
    return peers

def _unregister_gate(path):
    """A leaked registration would make every later gate report a phantom peer."""
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass

def _overlap_advisory():
    if gate_concurrency > 1:
        print("LANE_GATE_ADVISORY=failed_under_concurrent_verification", flush=True)

snapshot_detail = ""

def _is_repository():
    _refuse_ambient_git_env()
    try:
        probe = subprocess.run(
            ["git", "-C", subject, "rev-parse", "--show-toplevel"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=git_env, timeout=git_probe_timeout,
        )
    except subprocess.TimeoutExpired:
        _system("worktree_snapshot_unavailable", _detail("git_probe_timeout"))
    except OSError as exc:
        _system("worktree_snapshot_unavailable", _detail("oserror:" + str(exc)))
    except subprocess.SubprocessError as exc:
        _system("worktree_snapshot_unavailable", _detail("subprocess_error:" + str(exc)))
    if probe.returncode == 0:
        repository_root = os.path.realpath(os.fsdecode(probe.stdout).strip())
        if repository_root == os.path.realpath(subject):
            return True
        _system("worktree_snapshot_unavailable", _detail("ancestor_repository=" + repository_root))
    ancestor = os.path.realpath(subject)
    while True:
        metadata = os.path.join(ancestor, ".git")
        try:
            mode = os.lstat(metadata).st_mode
            if not stat.S_ISDIR(mode) or os.listdir(metadata):
                _system("worktree_snapshot_unavailable", _detail("ancestor_repository=" + ancestor))
        except FileNotFoundError:
            pass
        except OSError as exc:
            _system("worktree_snapshot_unavailable", _detail("oserror:" + str(exc)))
        parent = os.path.dirname(ancestor)
        if parent == ancestor:
            break
        ancestor = parent
    print(_detail("git_rc=%d:%s" % (probe.returncode, _first_line(probe.stderr))), file=sys.stderr, flush=True)
    return False

def _worktree_state():
    """Snapshot HEAD, index and candidate bytes/modes; refuse unreadable state."""
    global snapshot_detail
    def git(*args):
        return subprocess.run(
            ["git", "-C", subject, *args], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=git_probe_timeout, check=True,
            env=git_env,
        ).stdout
    try:
        head = git("rev-parse", "HEAD")
        index = git("ls-files", "--stage", "-z")
        names = git("ls-files", "--cached", "--others", "--exclude-standard", "-z")
        candidates = []
        for name in sorted(set(names.split(b"\0")) - {b""}):
            path = os.path.join(os.fsencode(subject), name)
            try:
                mode = os.lstat(path).st_mode
            except FileNotFoundError:
                candidates.append((name, None, None))
                continue
            if stat.S_ISLNK(mode):
                identity = os.readlink(path)
            elif stat.S_ISREG(mode):
                with open(path, "rb") as candidate:
                    identity = hashlib.file_digest(candidate, "sha256").digest()
            else:
                snapshot_detail = _detail("unsnapshotable_entry=" + os.fsdecode(name))
                return None
            candidates.append((name, mode, identity))
        return head, index, candidates
    except subprocess.TimeoutExpired:
        snapshot_detail = _detail("git_probe_timeout")
    except subprocess.CalledProcessError as exc:
        snapshot_detail = _detail("git_rc=%d:%s" % (exc.returncode, _first_line(exc.stderr)))
    except OSError as exc:
        snapshot_detail = _detail("oserror:" + str(exc))
    except subprocess.SubprocessError as exc:
        snapshot_detail = _detail("subprocess_error:" + str(exc))
    return None

def _stream_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value if isinstance(value, str) else ""

def _close_capture_pipes(process):
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass

# Verification may begin from a worker's intentional edits. What matters for
# artifact integrity is that commands do not change that candidate underneath
# the gate. A post-run comparison catches both tracked writes and new files.
mutation_guard = _is_repository()
if not mutation_guard:
    print("LANE_GATE_ADVISORY=mutation_guard=not_a_repository", flush=True)
worktree_before = _worktree_state() if mutation_guard else ()
if worktree_before is None:
    _system("worktree_snapshot_unavailable", snapshot_detail)
verified_non_style = False

# Announce before the first command so a peer starting now can see us, and drop
# the announcement on every exit path -- including SystemExit from a failure or
# a timeout -- which is what atexit buys over a try/finally around the loop.
gate_registration = _register_gate()
if gate_registration:
    atexit.register(_unregister_gate, gate_registration)
gate_concurrency = _live_peer_count() + 1

for index, cmd in enumerate(commands):
    print("LANE_GATE_CMD=" + cmd, flush=True)
    gate_concurrency = max(gate_concurrency, _live_peer_count() + 1)
    run_cwd = subject
    run_env = env
    if index < len(_selected_cwds) and _selected_cwds[index]:
        run_cwd = _selected_cwds[index]
        if not os.path.isabs(run_cwd):
            run_cwd = os.path.join(subject, run_cwd)
    if index < len(_selected_envs) and _selected_envs[index]:
        run_env = env.copy()
        run_env.update(_selected_envs[index])
    try:
        command_tokens = shlex.split(cmd)
    except ValueError:
        command_tokens = []
    while command_tokens and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", command_tokens[0]):
        command_tokens.pop(0)
    prog = command_tokens[0] if command_tokens else ""
    # `cd` is a shell builtin and is the neutral first segment in real lane
    # command shapes. The classifier validates its path after execution.
    if prog == "cd":
        prog = ""
    # Slash-containing relative executables are launched with cwd=run_cwd
    # (TSEL03ER2RV-001). Resolve them against that directory once here so
    # nested rows such as ../../.venv/bin/python are not judged from the
    # subject process cwd. Bare names still use PATH; absolute paths stay.
    if prog and not os.path.isabs(prog) and os.sep in prog:
        prog = os.path.join(run_cwd, prog)
    if prog and not os.path.isabs(prog) and os.sep not in prog:
        found = False
        for folder in run_env.get("PATH", "").split(os.pathsep):
            cand = os.path.join(folder, prog) if folder else prog
            if cand and os.path.isfile(cand) and os.access(cand, os.X_OK):
                found = True
                break
        if not found:
            _emit("system_error", "unresolvable_command", prog)
            raise SystemExit(3)
    elif prog and not (os.path.exists(prog) and os.access(prog, os.X_OK)):
        _emit("system_error", "unresolvable_command", prog)
        raise SystemExit(3)
    try:
        command_process = subprocess.Popen(
            cmd,
            shell=True,
            cwd=run_cwd,
            env=run_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            command_stdout, command_stderr = command_process.communicate(
                timeout=int(os.environ.get("LANE_GATE_CMD_TIMEOUT", "1800"))
            )
        except subprocess.TimeoutExpired as command_timeout:
            try:
                os.killpg(command_process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            artifact_failure = False
            try:
                command_stdout, command_stderr = command_process.communicate(timeout=1)
            except subprocess.TimeoutExpired as term_timeout:
                try:
                    os.killpg(command_process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    command_stdout, command_stderr = command_process.communicate(timeout=1)
                except subprocess.TimeoutExpired as drain_timeout:
                    # A detached descendant can retain either captured pipe
                    # after SIGKILL. Do not let its EOF determine the gate's
                    # lifetime: close our readers and reap the leader under a
                    # separate bound, retaining whatever output was captured.
                    artifact_failure = True
                    command_stdout = _stream_text(
                        drain_timeout.output or term_timeout.output or command_timeout.output
                    )
                    command_stderr = _stream_text(
                        drain_timeout.stderr or term_timeout.stderr or command_timeout.stderr
                    )
                    _close_capture_pipes(command_process)
                    try:
                        command_process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
            else:
                # The shell may have exited on TERM while another group
                # member ignored it after closing the captured descriptors.
                try:
                    os.killpg(command_process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if command_stdout:
                print(command_stdout, end="", flush=True)
            if command_stderr:
                print(command_stderr, end="", file=sys.stderr, flush=True)
            gate_concurrency = max(gate_concurrency, _live_peer_count() + 1)
            _overlap_advisory()
            if artifact_failure:
                _emit(
                    "timed_out",
                    "environment_artifact",
                    "command timeout cleanup exceeded its bounded capture/reap drain",
                )
            else:
                _emit("timed_out", "command_timeout")
            raise SystemExit(124)
        result = subprocess.CompletedProcess(
            cmd, command_process.returncode, command_stdout, command_stderr
        )
    except subprocess.TimeoutExpired:
        gate_concurrency = max(gate_concurrency, _live_peer_count() + 1)
        _overlap_advisory()
        _emit("timed_out", "command_timeout")
        raise SystemExit(124)
    except OSError as exc:
        _emit("system_error", "unresolvable_command", str(exc))
        raise SystemExit(3)
    gate_concurrency = max(gate_concurrency, _live_peer_count() + 1)
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)
    if result.returncode == 127:
        _emit("system_error", "unresolvable_command")
        raise SystemExit(3)
    style_only = is_style_only_command(
        cmd,
        cwd=run_cwd,
        path=run_env.get("PATH"),
        pythonpath=run_env.get("PYTHONPATH"),
        python_executable=interp,
    )
    if result.returncode != 0:
        # A missing optional style module is an execution-environment error,
        # not evidence that behavioral verification failed. Stay fail-closed:
        # syntax alone never grants advisory success to an untrusted tool.
        # Static classification (no PATH) is required: the runtime classifier
        # cannot trust a module that is not installed, and would otherwise
        # report this unexecuted check as failed.
        if is_style_only_command(cmd, cwd=run_cwd) and re.search(
            r"(?m)^.+: No module named (?:mypy|ruff)$$", result.stderr
        ):
            _emit("system_error", "unresolvable_style_module")
            raise SystemExit(3)
        if not enforce_style and style_only:
            # Debt, not a blocker. Keep going: advisory must not mean "stop
            # early" -- the declared tests after this command are the ones
            # that actually gate the merge.
            print("LANE_GATE_ADVISORY=" + cmd, flush=True)
            style_debt += 1
            continue
        _overlap_advisory()
        _emit("failed")
        raise SystemExit(1)
    if not style_only:
        verified_non_style = True

worktree_after = _worktree_state() if mutation_guard else ()
if worktree_after is None:
    _system("worktree_snapshot_unavailable", snapshot_detail)
if worktree_after != worktree_before:
    _overlap_advisory()
    _emit("failed", "worktree_changed_during_verification")
    raise SystemExit(1)

if not verified_non_style:
    _emit("skipped", "style_only_verification")
    raise SystemExit(2)

_emit("passed")
raise SystemExit(0)
endef
export LANE_GATE_PY

define LANE_DAG_PY
import json
import os
import subprocess
import sys

task = os.environ.get("TASK", "").strip()
root = os.environ.get("ORCHESTRATOR_ROOT", "").strip()

def _fail(reason, detail, code=3):
    print("LANE_DAG_STATUS=failed", flush=True)
    print("LANE_DAG_REASON=" + reason, flush=True)
    if detail:
        print(detail, file=sys.stderr, flush=True)
    raise SystemExit(code)

if not task:
    _fail("missing_task", "lane-dag requires TASK=<task-ref>")
if not root:
    _fail("missing_orchestrator_root", "ORCHESTRATOR_ROOT could not be resolved")
if task in (".", "..") or ".." in task or "/" in task or os.path.isabs(task):
    _fail("invalid_task_ref", "task ref must be a single path segment")

source_roots = [
    os.path.join(root, "packages", "mcp-workbay-orchestrator", "src"),
    os.path.join(root, "packages", "workbay-protocol", "src"),
    os.path.join(root, "packages", "mcp-workbay-handoff", "src"),
]

if os.environ.get("WORKBAY_LANE_DAG_RESOLVED") != "1":
    sys.path[:0] = source_roots
    try:
        from workbay_orchestrator_mcp.orchestration._env import resolve_lane_python
        interp = resolve_lane_python(root)
    except Exception as exc:
        _fail("unresolvable_interpreter", str(exc))
    if not interp or not os.path.exists(interp):
        _fail("unresolvable_interpreter", "resolved path missing: %r" % (interp,))
    env = os.environ.copy()
    env["WORKBAY_LANE_DAG_RESOLVED"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(source_roots)
    result = subprocess.run([interp, "-c", os.environ["LANE_DAG_PY"]], env=env, text=True)
    raise SystemExit(result.returncode)

sys.path[:0] = source_roots
try:
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_orchestrator_mcp import api
    api.configure_runtime(RuntimeConfig.for_workspace(root, state_dir=os.path.join(root, ".task-state")))
    report = api.lane_dag(task)
except Exception as exc:
    _fail("lane_dag_unavailable", "%s: %s" % (type(exc).__name__, exc))

if not isinstance(report, dict):
    _fail("invalid_lane_dag_response", "lane_dag returned a non-object response")
if report.get("schema_version") == 2:
    report = dict(report, **report.get("data", {}), **report.get("scope", {}))
if report.get("ok") is not True:
    _fail(str(report.get("error_type") or "lane_dag_failed"), str(report.get("error") or "lane_dag failed"))

print(report["ascii"], flush=True)
print(json.dumps(report["json"], sort_keys=True, indent=2), flush=True)
print("LANE_DAG_STATUS=rendered", flush=True)
raise SystemExit(0)
endef
export LANE_DAG_PY

.PHONY: lane-check lane-intake lane-refresh
.PHONY: lane-dag

lane-check: ## Run the verification the lane manifest declares for LANE
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_GATE_STATUS=system_error"; echo "LANE_GATE_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_GATE_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) - check

lane-intake: ## Validate TASK/LANE can be intaken (manifest + interpreter)
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_GATE_STATUS=system_error"; echo "LANE_GATE_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_GATE_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) - intake

lane-refresh: ## Validate TASK/LANE can be refreshed (manifest + interpreter)
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_GATE_STATUS=system_error"; echo "LANE_GATE_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_GATE_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) - refresh

lane-dag: ## Render TASK's validated lane DAG as ASCII and JSON
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_DAG_STATUS=failed"; echo "LANE_DAG_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_DAG_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) -
