"""Run P concurrent writer processes against one scratch DB. argv: <src_root> <procs> <ops>"""
import json, os, statistics, subprocess, sys, tempfile, time
from pathlib import Path
SRC, P, OPS = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
tmp = Path(tempfile.mkdtemp(prefix="conc-"))
state = tmp / ".task-state"; state.mkdir(parents=True)
PY = sys.executable
WORKER = str(Path(__file__).resolve().parent / "conc_writer_worker.py")
# seed the schema once so workers do not race on migration
subprocess.run([PY, WORKER, SRC, str(state), "1"],
               env={**os.environ, "PROBE_START_AT": str(time.time())},
               capture_output=True, check=True)
start_at = time.time() + 6.0
env = {**os.environ, "PROBE_START_AT": str(start_at)}
procs = [subprocess.Popen([PY, WORKER, SRC, str(state), str(OPS)],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True)
         for _ in range(P)]
t0 = time.time()
outs = [p.communicate() for p in procs]
wall = time.time() - t0 - 6.0
rows = []
for o, e in outs:
    line = [l for l in o.strip().splitlines() if l.startswith("{")]
    if not line:
        print("WORKER FAILED:", e.strip()[-400:]); continue
    rows.append(json.loads(line[-1]))
meds = [r["median"] for r in rows]; p95s = [r["p95"] for r in rows]; mx = [r["max"] for r in rows]
print(json.dumps({
    "src": SRC.split("/")[-1], "procs": P, "ops_each": OPS, "workers_ok": len(rows),
    "wall_s": round(wall, 2),
    "median_of_medians_ms": round(statistics.median(meds), 3),
    "worst_worker_p95_ms": round(max(p95s), 3),
    "worst_op_ms": round(max(mx), 3),
}))
