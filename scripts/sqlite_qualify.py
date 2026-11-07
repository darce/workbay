#!/usr/bin/env python3
"""Qualify the SQLite engine of *this* interpreter against a copy of handoff.db.

Usage::

    python3 -I scripts/sqlite_qualify.py --db <copy-of-handoff.db> --json

Stdlib only. The source DB is opened read-only and backed up into a scratch
directory; every check that writes (FTS integrity-check commands, trigger
exercise, package migrations, the concurrency probe) runs on a scratch copy,
never on ``--db``. A path inside a ``.task-state`` directory is refused: copy
the production DB first.

Report sections (JSON on stdout, diagnostics on stderr):

* ``engine``: ``sqlite3.sqlite_version``, ``sqlite3.version``, the linked
  library (``/proc/self/maps`` where available), ``PRAGMA compile_options``
  with derived FTS5 / JSON / threadsafe / defensive flags, and interpreter
  identity (``sys.orig_argv[0]`` resolved and compared to ``sys.executable``
  by file identity, not by executability).
* ``schema``: ``PRAGMA integrity_check``, ``PRAGMA user_version``, each FTS5
  table's ``'integrity-check'`` command, and a rolled-back exercise of every
  trigger that writes an FTS5 table or its shadow tables (the
  ``may not write to shadow table`` class of version/config skew).
* ``handoff_package``: the handoff package's own connection factory (with its
  write-lock authorizer) and ``_prepare_handoff_connection`` migrations open
  the scratch copy, and the authorizer reports RESERVED correctly.
* ``concurrency``: for N in ``--writers`` (default 1,4,8), N threads each run
  short ``BEGIN IMMEDIATE`` transactions with one concurrent
  ``wal_checkpoint(TRUNCATE)``; reports p50/p95/p99 lock wait and the count
  of ``SQLITE_BUSY`` results ([PERF-01] percentiles, [PERF-06] measured).

Exit status ([AGT-21]): 0 every check ran and passed; 3 a schema check ran and
failed (integrity, FTS integrity, FTS trigger write, package migration or
authorizer); 4 a check could not run (source unreadable or refused, handoff
package not importable, concurrency probe error). 3 wins over 4.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import shutil
import site
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
import warnings
from pathlib import Path

EXIT_OK = 0
EXIT_SCHEMA_FAILURE = 3
EXIT_PROBE_UNAVAILABLE = 4

_REPO_ROOT = Path(__file__).resolve().parents[1]
_IN_TREE_PACKAGE_PATHS = (
    _REPO_ROOT / "packages" / "mcp-workbay-handoff" / "src",
    _REPO_ROOT / "packages" / "workbay-protocol" / "src",
)
# Mirrors HANDOFF_SQLITE_BUSY_TIMEOUT_MS in workbay_handoff_mcp.shared_schema;
# duplicated so the engine and concurrency sections need no package import.
_DEFAULT_BUSY_TIMEOUT_MS = 30000
_FTS5_SHADOW_SUFFIXES = ("_content", "_data", "_idx", "_docsize", "_config")
_PROBE_TABLE = "_sqlite_qualify_probe"


def _diag(message: str) -> None:
    print(f"sqlite_qualify: {message}", file=sys.stderr)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _is_busy(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if code is not None:
        return code & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    text = str(exc).lower()
    return "locked" in text or "busy" in text


# ---------------------------------------------------------------------------
# engine
# ---------------------------------------------------------------------------


def _interpreter_identity() -> dict:
    orig_argv = getattr(sys, "orig_argv", None) or []
    argv0 = orig_argv[0] if orig_argv else None
    resolved = None
    if argv0:
        resolved = argv0 if os.sep in argv0 else shutil.which(argv0)
    same_file = None
    if resolved and sys.executable:
        try:
            same_file = os.path.samefile(resolved, sys.executable)
        except OSError:
            same_file = False
    return {
        "argv0": argv0,
        "argv0_resolved": resolved,
        "argv0_realpath": os.path.realpath(resolved) if resolved else None,
        "sys_executable": sys.executable,
        "sys_executable_realpath": os.path.realpath(sys.executable) if sys.executable else None,
        "argv0_is_sys_executable": same_file,
        "python_version": platform.python_version(),
        "implementation": sys.implementation.name,
        "venv": sys.prefix != sys.base_prefix,
        "isolated": bool(sys.flags.isolated),
    }


def _linked_library() -> dict:
    extension = getattr(sys.modules.get("_sqlite3"), "__file__", None)
    mapped: list[str] = []
    maps = Path("/proc/self/maps")
    if maps.exists():
        for line in maps.read_text(errors="replace").splitlines():
            parts = line.split(None, 5)
            if len(parts) == 6 and "sqlite" in os.path.basename(parts[5]).lower():
                if parts[5] not in mapped:
                    mapped.append(parts[5])
        shared = [p for p in mapped if "libsqlite3" in os.path.basename(p)]
    else:
        shared = []
    if shared:
        linkage = "shared"
    elif "_sqlite3" in sys.builtin_module_names:
        extension, linkage = "<built-in>", "static-in-interpreter"
    elif extension and maps.exists():
        linkage = "static-in-extension"
    else:
        linkage = "unknown"
    return {
        "extension": extension,
        "shared_libraries": [os.path.realpath(p) for p in shared],
        "linkage": linkage,
    }


def engine_report() -> dict:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        module_version = getattr(sqlite3, "version", None)
    conn = sqlite3.connect(":memory:")
    try:
        options = [row[0] for row in conn.execute("PRAGMA compile_options")]
        try:
            conn.execute("CREATE VIRTUAL TABLE fts5_probe USING fts5(body)")
            conn.execute("DROP TABLE fts5_probe")
            fts5 = True
        except sqlite3.Error:
            fts5 = False
        try:
            json_ok = conn.execute("SELECT json_valid('{\"a\":1}')").fetchone()[0] == 1
        except sqlite3.Error:
            json_ok = False
        defensive = None
        if hasattr(conn, "getconfig") and hasattr(sqlite3, "SQLITE_DBCONFIG_DEFENSIVE"):
            defensive = bool(conn.getconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE))
    finally:
        conn.close()
    threadsafe = next((o.split("=", 1)[1] for o in options if o.startswith("THREADSAFE=")), None)
    return {
        "sqlite_version": sqlite3.sqlite_version,
        "sqlite3_module_version": module_version,
        "library": _linked_library(),
        "compile_options": options,
        "features": {
            "fts5": fts5,
            "json": json_ok,
            "threadsafe": threadsafe,
            "python_threadsafety": getattr(sqlite3, "threadsafety", None),
            "defensive_default": defensive,
        },
        "interpreter": _interpreter_identity(),
    }


# ---------------------------------------------------------------------------
# scratch copies
# ---------------------------------------------------------------------------


def _refuse_live_path(db: Path) -> str | None:
    if any(part == ".task-state" for part in db.resolve().parts):
        return f"refusing {db}: inside a .task-state directory; pass a copy"
    return None


def _scratch_copy(source: Path, dest: Path) -> None:
    src = sqlite3.connect(f"{source.resolve().as_uri()}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


def _fts5_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL").fetchall()
    return sorted(name for name, sql in rows if re.search(r"\bUSING\s+fts5\b", sql, re.IGNORECASE))


def _fts_writing_triggers(conn: sqlite3.Connection, fts_tables: list[str]) -> dict[str, list[tuple[str, str]]]:
    """Map base table -> [(trigger, sql)] for triggers that write an FTS5 table or its shadows."""
    targets = set(fts_tables) | {t + s for t in fts_tables for s in _FTS5_SHADOW_SUFFIXES}
    pattern = re.compile(
        r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+[\"`\[]?(" + "|".join(map(re.escape, targets)) + r")\b",
        re.IGNORECASE,
    )
    by_table: dict[str, list[tuple[str, str]]] = {}
    if not targets:
        return by_table
    for name, tbl, sql in conn.execute(
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
    ):
        if sql and pattern.search(sql) and tbl not in targets:
            by_table.setdefault(tbl, []).append((name, sql))
    return by_table


def _update_columns(conn: sqlite3.Connection, table: str, triggers: list[tuple[str, str]]) -> list[str]:
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info({_quote(table)})")]
    chosen: list[str] = []
    for _name, sql in triggers:
        head = re.split(r"\bBEGIN\b", sql, maxsplit=1, flags=re.IGNORECASE)[0]
        if not re.search(r"\bUPDATE\b", head, re.IGNORECASE):
            continue
        match = re.search(r"\bUPDATE\s+OF\s+(.+?)\s+ON\b", head, re.IGNORECASE | re.DOTALL)
        wanted = [c.strip().strip('"`[]') for c in match.group(1).split(",")] if match else columns[1:2]
        chosen.extend(c for c in wanted if c in columns and c not in chosen)
    return chosen


def _exercise_triggers(conn: sqlite3.Connection, fts_tables: list[str]) -> list[dict]:
    """Fire every FTS-writing trigger on one existing row, then roll back."""
    results = []
    for table, triggers in sorted(_fts_writing_triggers(conn, fts_tables).items()):
        entry: dict = {"table": table, "triggers": [name for name, _ in triggers]}
        row = conn.execute(f"SELECT rowid FROM {_quote(table)} ORDER BY rowid DESC LIMIT 1").fetchone()
        if row is None:
            entry.update(status="skipped", reason="table has no rows")
            results.append(entry)
            continue
        rowid = row[0]
        qt = _quote(table)
        conn.execute("SAVEPOINT qualify_triggers")
        try:
            cols = _update_columns(conn, table, triggers)
            if cols:
                assignments = ", ".join(f"{_quote(c)} = {_quote(c)}" for c in cols)
                conn.execute(f"UPDATE {qt} SET {assignments} WHERE rowid = ?", (rowid,))
            conn.execute(f"CREATE TEMP TABLE _qualify_row AS SELECT * FROM {qt} WHERE rowid = ?", (rowid,))
            conn.execute(f"DELETE FROM {qt} WHERE rowid = ?", (rowid,))
            conn.execute(f"INSERT INTO {qt} SELECT * FROM temp._qualify_row")
            entry.update(status="ok", updated_columns=cols)
        except sqlite3.Error as exc:
            entry.update(status="failed", error=_error_text(exc))
        finally:
            conn.execute("ROLLBACK TO qualify_triggers")
            conn.execute("RELEASE qualify_triggers")
            conn.execute("DROP TABLE IF EXISTS temp._qualify_row")
        results.append(entry)
    return results


def schema_report(scratch: Path, *, defensive: bool) -> dict:
    conn = sqlite3.connect(scratch, isolation_level=None)
    try:
        if defensive:
            conn.setconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE, True)
        integrity = [row[0] for row in conn.execute("PRAGMA integrity_check")]
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
        meta_version = None
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_meta'").fetchone():
            row = conn.execute("SELECT value FROM schema_meta WHERE key = 'schema_version'").fetchone()
            meta_version = row[0] if row else None
        fts_tables = _fts5_tables(conn)
        fts_checks = []
        for name in fts_tables:
            try:
                conn.execute(f"INSERT INTO {_quote(name)}({_quote(name)}) VALUES('integrity-check')")
                fts_checks.append({"table": name, "status": "ok"})
            except sqlite3.Error as exc:
                fts_checks.append({"table": name, "status": "failed", "error": _error_text(exc)})
        trigger_checks = _exercise_triggers(conn, fts_tables)
    finally:
        conn.close()
    ok = (
        integrity == ["ok"]
        and all(c["status"] == "ok" for c in fts_checks)
        and all(c["status"] != "failed" for c in trigger_checks)
    )
    return {
        "status": "ok" if ok else "failed",
        "defensive": defensive,
        "integrity_check": integrity,
        "user_version": user_version,
        "schema_meta_version": meta_version,
        "fts5_integrity": fts_checks,
        "fts_trigger_writes": trigger_checks,
    }


# ---------------------------------------------------------------------------
# handoff package: migrations + authorizer guard
# ---------------------------------------------------------------------------


def _import_shared_schema(extra_paths: list[str]):
    # addsitedir honours .pth files (editable installs); the added entries are
    # moved first so they win over the system dist-packages that -I keeps.
    for path in extra_paths:
        before = list(sys.path)
        site.addsitedir(path)
        added = [p for p in sys.path if p not in before]
        sys.path[:] = added + before
    try:
        from workbay_handoff_mcp import shared_schema
    except ImportError:
        for path in _IN_TREE_PACKAGE_PATHS:
            if path.is_dir() and str(path) not in sys.path:
                sys.path.insert(0, str(path))
        from workbay_handoff_mcp import shared_schema
    return shared_schema


def package_report(scratch: Path, extra_paths: list[str]) -> dict:
    try:
        shared_schema = _import_shared_schema(extra_paths)
    except ImportError as exc:
        return {"status": "unavailable", "error": _error_text(exc)}
    report: dict = {
        "module": getattr(shared_schema, "__file__", None),
        "package_schema_version": getattr(shared_schema, "HANDOFF_SCHEMA_VERSION", None),
    }
    conn = None
    try:
        conn = shared_schema.connect_handoff_db(scratch)
        shared_schema._prepare_handoff_connection(conn)
        report["user_version_after"] = conn.execute("PRAGMA user_version").fetchone()[0]
        report["schema_version_after"] = shared_schema.handoff_schema_version(conn)
        holds = shared_schema.holds_write_lock
        conn.isolation_level = None
        before = holds(conn)
        conn.execute("BEGIN IMMEDIATE")
        during = holds(conn)
        conn.execute("ROLLBACK")
        after = holds(conn)
        report["authorizer"] = {
            "tracked_connection": type(conn).__name__,
            "holds_write_lock": {"before": before, "begin_immediate": during, "after_rollback": after},
        }
        conn.close()
        conn = None
        shared_schema.assert_boot_schema_compatible(scratch)
        ok = before is False and during is True and after is False
        report["status"] = "ok" if ok else "failed"
    except Exception as exc:  # noqa: BLE001 - any migration/guard error is the finding
        report.update(status="failed", error=_error_text(exc))
        _diag("handoff package open failed:\n" + traceback.format_exc())
    finally:
        if conn is not None:
            conn.close()
    return report


# ---------------------------------------------------------------------------
# concurrency probe
# ---------------------------------------------------------------------------


def _percentiles(samples: list[float]) -> dict:
    if not samples:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(samples)

    def rank(p: int) -> float:
        # Nearest-rank percentile: no interpolation, always an observed sample.
        index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * p / 100) - 1))
        return round(ordered[index], 3)

    return {"p50": rank(50), "p95": rank(95), "p99": rank(99), "max": round(ordered[-1], 3)}


def _writer(path: Path, writer_id: int, args, deadline: float, start: threading.Barrier, out: dict) -> None:
    waits: list[float] = []
    txns: list[float] = []
    busy = 0
    errors: list[str] = []
    conn = sqlite3.connect(path, timeout=args.busy_timeout_ms / 1000.0, isolation_level=None)
    try:
        conn.execute(f"PRAGMA busy_timeout={args.busy_timeout_ms}")
        payload = "x" * 256
        start.wait()
        for seq in range(args.txns):
            if time.monotonic() >= deadline:
                break
            t0 = time.perf_counter()
            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if _is_busy(exc):
                    busy += 1
                    continue
                raise
            t1 = time.perf_counter()
            try:
                conn.execute(
                    f"INSERT INTO {_PROBE_TABLE}(writer, seq, payload) VALUES (?, ?, ?)", (writer_id, seq, payload)
                )
                if args.hold_ms:
                    time.sleep(args.hold_ms / 1000.0)
                conn.execute("COMMIT")
            except sqlite3.OperationalError as exc:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                if _is_busy(exc):
                    busy += 1
                    continue
                raise
            waits.append((t1 - t0) * 1000.0)
            txns.append((time.perf_counter() - t0) * 1000.0)
    except Exception as exc:  # noqa: BLE001 - reported as probe failure
        errors.append(_error_text(exc))
    finally:
        conn.close()
    out[writer_id] = {"waits": waits, "txns": txns, "busy": busy, "errors": errors}


def _checkpoint(path: Path, args, start: threading.Barrier, out: dict) -> None:
    conn = sqlite3.connect(path, timeout=args.busy_timeout_ms / 1000.0, isolation_level=None)
    try:
        conn.execute(f"PRAGMA busy_timeout={args.busy_timeout_ms}")
        start.wait()
        time.sleep(args.hold_ms * args.txns / 2000.0)  # land mid-run
        t0 = time.perf_counter()
        busy_flag, log_frames, checkpointed = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        out["checkpoint"] = {
            "mode": "TRUNCATE",
            "ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "busy": busy_flag,
            "log_frames": log_frames,
            "checkpointed_frames": checkpointed,
        }
    except sqlite3.Error as exc:
        out["checkpoint"] = {"mode": "TRUNCATE", "error": _error_text(exc), "busy_error": _is_busy(exc)}
    finally:
        conn.close()


def concurrency_report(scratch: Path, args) -> dict:
    setup = sqlite3.connect(scratch, isolation_level=None)
    try:
        journal = setup.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        setup.execute("PRAGMA synchronous=NORMAL")
        setup.execute(
            f"CREATE TABLE IF NOT EXISTS {_PROBE_TABLE}"
            "(id INTEGER PRIMARY KEY, writer INTEGER NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL)"
        )
    finally:
        setup.close()
    runs = []
    ok = True
    for n in args.writers:
        out: dict = {}
        start = threading.Barrier(n + 1)
        deadline = time.monotonic() + args.deadline_s
        threads = [
            threading.Thread(target=_writer, args=(scratch, i, args, deadline, start, out), daemon=True)
            for i in range(n)
        ]
        threads.append(threading.Thread(target=_checkpoint, args=(scratch, args, start, out), daemon=True))
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=max(0.0, deadline - time.monotonic()) + args.busy_timeout_ms / 1000.0 + 5)
        elapsed = time.perf_counter() - t0
        hung = sum(t.is_alive() for t in threads)
        writers = [out[i] for i in range(n) if i in out]
        waits = [w for r in writers for w in r["waits"]]
        txns = [x for r in writers for x in r["txns"]]
        errors = [e for r in writers for e in r["errors"]]
        run = {
            "writers": n,
            "planned_txns": n * args.txns,
            "committed_txns": len(txns),
            "sqlite_busy": sum(r["busy"] for r in writers),
            "wait_ms": _percentiles(waits),
            "txn_ms": _percentiles(txns),
            "elapsed_s": round(elapsed, 3),
            "checkpoint": out.get("checkpoint"),
            "truncated_by_deadline": len(txns) + sum(r["busy"] for r in writers) < n * args.txns,
            "errors": errors[:5],
            "hung_threads": hung,
        }
        if errors or hung or len(writers) != n or "error" in (run["checkpoint"] or {"error": "missing"}):
            ok = False
        runs.append(run)
    return {
        "status": "ok" if ok else "error",
        "journal_mode": journal,
        "busy_timeout_ms": args.busy_timeout_ms,
        "hold_ms": args.hold_ms,
        "txns_per_writer": args.txns,
        "runs": runs,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--db", required=True, type=Path, help="copy of a handoff.db (opened read-only)")
    parser.add_argument("--json", action="store_true", help="emit the full JSON report on stdout")
    parser.add_argument("--writers", default="1,4,8", help="comma-separated writer counts (default 1,4,8)")
    parser.add_argument("--txns", type=int, default=50, help="BEGIN IMMEDIATE transactions per writer")
    parser.add_argument("--hold-ms", type=float, default=1.0, help="work held inside each write transaction")
    parser.add_argument("--busy-timeout-ms", type=int, default=_DEFAULT_BUSY_TIMEOUT_MS)
    parser.add_argument("--deadline-s", type=float, default=60.0, help="per-N wall-clock bound")
    parser.add_argument(
        "--defensive", action="store_true", help="enable SQLITE_DBCONFIG_DEFENSIVE for the schema checks"
    )
    parser.add_argument("--skip-package", action="store_true", help="do not import the handoff package")
    parser.add_argument(
        "--package-path",
        action="append",
        default=[],
        help="site dir holding the handoff package and its deps (repeatable; .pth honoured; -I drops PYTHONPATH)",
    )
    parser.add_argument("--keep-scratch", action="store_true", help="keep the scratch directory")
    args = parser.parse_args(argv)
    try:
        args.writers = [int(n) for n in args.writers.split(",") if n.strip()]
    except ValueError:
        parser.error("--writers must be comma-separated integers")
    if not args.writers or min(args.writers) < 1 or args.txns < 1:
        parser.error("--writers and --txns must be positive")
    if args.defensive and not hasattr(sqlite3, "SQLITE_DBCONFIG_DEFENSIVE"):
        parser.error("--defensive needs Python 3.12+ (sqlite3.Connection.setconfig)")
    return args


def _summary_line(report: dict) -> str:
    engine = report["engine"]
    parts = [f"sqlite {engine['sqlite_version']}", f"exit {report['exit_code']}"]
    for key in ("schema", "handoff_package", "concurrency"):
        section = report.get(key)
        if section:
            parts.append(f"{key}={section['status']}")
    for run in (report.get("concurrency") or {}).get("runs", []):
        w = run["wait_ms"]
        parts.append(
            f"N={run['writers']} wait p50/p95/p99={w['p50']}/{w['p95']}/{w['p99']}ms busy={run['sqlite_busy']}"
        )
    return "; ".join(parts)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report: dict = {"engine": engine_report(), "source_db": str(args.db)}
    unavailable: list[str] = []
    failed: list[str] = []

    refusal = _refuse_live_path(args.db)
    if refusal is None and not args.db.is_file():
        refusal = f"{args.db} is not a file"
    if refusal:
        _diag(refusal)
        unavailable.append("source")
        report["error"] = refusal
    else:
        scratch_dir = Path(tempfile.mkdtemp(prefix="sqlite-qualify-"))
        try:
            schema_db = scratch_dir / "schema.db"
            package_db = scratch_dir / "package.db"
            probe_db = scratch_dir / "probe.db"
            try:
                for dest in (schema_db, package_db, probe_db):
                    _scratch_copy(args.db, dest)
            except sqlite3.Error as exc:
                _diag(f"cannot copy {args.db}: {_error_text(exc)}")
                unavailable.append("source")
                report["error"] = _error_text(exc)
            else:
                try:
                    report["schema"] = schema_report(schema_db, defensive=args.defensive)
                except sqlite3.Error as exc:
                    report["schema"] = {"status": "failed", "error": _error_text(exc)}
                if report["schema"]["status"] != "ok":
                    failed.append("schema")

                if args.skip_package:
                    report["handoff_package"] = {"status": "skipped"}
                else:
                    report["handoff_package"] = package_report(package_db, args.package_path)
                    if report["handoff_package"]["status"] == "unavailable":
                        _diag("handoff package not importable: " + report["handoff_package"]["error"])
                        unavailable.append("handoff_package")
                    elif report["handoff_package"]["status"] != "ok":
                        failed.append("handoff_package")

                try:
                    report["concurrency"] = concurrency_report(probe_db, args)
                except sqlite3.Error as exc:
                    report["concurrency"] = {"status": "error", "error": _error_text(exc)}
                if report["concurrency"]["status"] != "ok":
                    unavailable.append("concurrency")
        finally:
            if args.keep_scratch:
                report["scratch_dir"] = str(scratch_dir)
            else:
                shutil.rmtree(scratch_dir, ignore_errors=True)

    if failed:
        code = EXIT_SCHEMA_FAILURE
    elif unavailable:
        code = EXIT_PROBE_UNAVAILABLE
    else:
        code = EXIT_OK
    report.update(exit_code=code, failed=failed, unavailable=unavailable)
    if args.json:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        print(_summary_line(report))
    return code


if __name__ == "__main__":
    sys.exit(main())
