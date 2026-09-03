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
#   LANE_GATE_STATUS=skipped         exit 2  — lane declares no verification
#   LANE_GATE_STATUS=failed          exit 1  — declared verification ran and failed
#   LANE_GATE_STATUS=system_error    exit 3  — interpreter/manifest/lane unusable
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
LANE_GATE_BOOTSTRAP_PYTHON := python3

export ORCHESTRATOR_ROOT
export TASK
export LANE

define LANE_GATE_PY
import os
import subprocess
import sys

def _emit(status, reason=None, extra=""):
    if reason:
        print("LANE_GATE_REASON=" + reason, flush=True)
    print("LANE_GATE_STATUS=" + status, flush=True)
    print("LANE_GATE_TERMINAL=1", flush=True)
    if extra:
        print(extra, file=sys.stderr, flush=True)

def _system(reason, extra=""):
    _emit("system_error", reason, extra)
    raise SystemExit(3)

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

src = os.path.join(root, "packages", "mcp-workbay-orchestrator", "src")
inherited_raw = os.environ.get("PYTHONPATH", "")
inherited = [os.path.abspath(p) for p in inherited_raw.split(os.pathsep) if p]
# Isolate the *gate* process from inherited PYTHONPATH so the instrument
# cannot be swapped by the ambient environment. Do not clobber os.environ:
# child test_commands need those entries.
sys.path[:] = [p for p in sys.path if p and os.path.abspath(p) not in inherited]
sys.path.insert(0, src)

try:
    from workbay_orchestrator_mcp.orchestration._env import resolve_lane_python
except Exception as exc:
    _system("unresolvable_interpreter", str(exc))

try:
    interp = resolve_lane_python(root)
except Exception as exc:
    _system("unresolvable_interpreter", str(exc))

if not interp or not os.path.exists(interp):
    _system("unresolvable_interpreter", "resolved path missing: %r" % (interp,))

print("LANE_GATE_PYTHON=" + interp, flush=True)

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
    _emit("validated")
    raise SystemExit(0)

commands = []
spec = lanes.get(lane) if isinstance(lanes.get(lane), dict) else {}
raw = spec.get("test_commands")
if isinstance(raw, list):
    commands = [str(item) for item in raw if str(item).strip()]

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

for cmd in commands:
    print("LANE_GATE_CMD=" + cmd, flush=True)
    prog = cmd.strip().split()[0] if cmd.strip() else ""
    if prog and not os.path.isabs(prog) and os.sep not in prog:
        found = False
        for folder in env.get("PATH", "").split(os.pathsep):
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
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=int(os.environ.get("LANE_GATE_CMD_TIMEOUT", "1800")),
        )
    except subprocess.TimeoutExpired as exc:
        if exc.stdout:
            print(exc.stdout, end="", flush=True)
        if exc.stderr:
            print(exc.stderr, end="", file=sys.stderr, flush=True)
        _emit("timed_out", "command_timeout")
        raise SystemExit(124)
    except OSError as exc:
        _emit("system_error", "unresolvable_command", str(exc))
        raise SystemExit(3)
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)
    if result.returncode == 127:
        _emit("system_error", "unresolvable_command")
        raise SystemExit(3)
    if result.returncode != 0:
        _emit("failed")
        raise SystemExit(1)

_emit("passed")
raise SystemExit(0)
endef
export LANE_GATE_PY

.PHONY: lane-check lane-intake lane-refresh

lane-check: ## Run the verification the lane manifest declares for LANE
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_GATE_STATUS=system_error"; echo "LANE_GATE_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_GATE_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) - check

lane-intake: ## Validate TASK/LANE can be intaken (manifest + interpreter)
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_GATE_STATUS=system_error"; echo "LANE_GATE_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_GATE_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) - intake

lane-refresh: ## Validate TASK/LANE can be refreshed (manifest + interpreter)
	@command -v $(LANE_GATE_BOOTSTRAP_PYTHON) >/dev/null 2>&1 || { echo "LANE_GATE_STATUS=system_error"; echo "LANE_GATE_REASON=unresolvable_interpreter" >&2; exit 3; }
	@printf '%s\n' "$$LANE_GATE_PY" | $(LANE_GATE_BOOTSTRAP_PYTHON) - refresh
