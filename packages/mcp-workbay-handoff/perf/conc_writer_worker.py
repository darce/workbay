"""One concurrent-writer worker. argv: <src_root> <state_dir> <ops>"""
import json, os, statistics, sys, time
from pathlib import Path
SRC, STATE, OPS = sys.argv[1], sys.argv[2], int(sys.argv[3])
sys.path.insert(0, str(Path(SRC) / "packages/mcp-workbay-handoff/src"))
os.environ["WORKBAY_HANDOFF_STATE_DIR"] = STATE
from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.runtime import configure_runtime
ws = Path(STATE).parent
cfg = RuntimeConfig.for_workspace(ws, state_dir=Path(STATE))
configure_runtime(cfg)
from workbay_handoff_mcp import shared_schema

# --- ablation hooks (PROBE_ABLATE) ---
_abl = os.environ.get("PROBE_ABLATE", "")
if _abl:
    import contextlib
    from workbay_handoff_mcp import db_writer_liveness as _dwl
    if "noreg" in _abl:
        _dwl.register_db_writer = lambda *a, **k: "abl-writer-id"
        _dwl.unregister_db_writer = lambda *a, **k: None
        if hasattr(shared_schema, "register_db_writer"):
            shared_schema.register_db_writer = _dwl.register_db_writer
        if hasattr(shared_schema, "unregister_db_writer"):
            shared_schema.unregister_db_writer = _dwl.unregister_db_writer
    if "nolock" in _abl:
        @contextlib.contextmanager
        def _nolock(path, *, blocking=True):
            yield True
        _dwl._registry_file_lock = _nolock
    if "nobeat" in _abl:
        @contextlib.contextmanager
        def _nobeat(*a, **k):
            yield None
        _dwl.db_writer_heartbeat = _nobeat
        if hasattr(shared_schema, "db_writer_heartbeat"):
            shared_schema.db_writer_heartbeat = _nobeat
# --- end ablation hooks ---
get = shared_schema._get_db_connection
with get(begin_immediate=True) as c:
    c.execute("CREATE TABLE IF NOT EXISTS probe_ops(id INTEGER PRIMARY KEY, pid INT, ts REAL)")
# barrier: all workers wait for a common start time passed via env
start_at = float(os.environ["PROBE_START_AT"])
while time.time() < start_at:
    pass
lat = []
for _ in range(OPS):
    t0 = time.perf_counter()
    with get(begin_immediate=True) as c:
        c.execute("INSERT INTO probe_ops(pid, ts) VALUES (?,?)", (os.getpid(), time.time()))
    lat.append((time.perf_counter() - t0) * 1000.0)
lat.sort()
print(json.dumps({
    "pid": os.getpid(), "n": len(lat),
    "median": statistics.median(lat), "p95": lat[int(len(lat)*0.95)],
    "max": lat[-1], "mean": statistics.mean(lat),
}))
