"""Unwired C10 sibling metadata census; no deletion or sibling lock authority.

The shell function must run in the caller's shell (redirect its output to a file,
not a command substitution) to retain fd 7 through the caller's eventual delete.
Only native publication writers taking .queue-publication.lock are serialized.
Agent-made references below the C10 metadata domains remain an accepted residual.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal, Sequence
from urllib.parse import unquote


@dataclass(frozen=True)
class CustodyVerdict:
    state: Literal["CLEAR", "HELD", "UNKNOWN"]
    reason: str = ""
    referrer: str = ""


@dataclass(frozen=True)
class CustodyReport:
    verdicts: dict[str, CustodyVerdict]
    domain: str | None = None
    entries: int = 0
    venvs: int = 0
    elapsed_ms: int = 0


def parse_custody_output(output: str, keys: Sequence[str]) -> CustodyReport:
    """An incomplete, duplicated or malformed batch cannot prove any absence."""
    unknown = CustodyReport({key: CustodyVerdict("UNKNOWN", "census_output_incomplete") for key in keys})
    lines = output.splitlines()
    if len(set(keys)) != len(keys) or not lines or lines[-1] != "END":
        return unknown
    if len(lines) != len(keys) + 3:
        return unknown
    header = re.fullmatch(r"CENSUS entries=([0-9]+) venvs=([0-9]+) ms=([0-9]+)", lines[0])
    digest = re.fullmatch(r"DOMAIN ([0-9a-f]{64})", lines[1])
    if header is None or digest is None:
        return unknown
    verdicts: dict[str, CustodyVerdict] = {}
    for line in lines[2:-1]:
        parts = line.split(" ")
        if len(parts) < 3 or parts[0] != "CUSTODY" or parts[1] not in unknown.verdicts or parts[1] in verdicts:
            return unknown
        key, state = parts[1:3]
        if state == "CLEAR" and len(parts) == 3:
            verdicts[key] = CustodyVerdict("CLEAR")
        elif state == "UNKNOWN" and len(parts) == 4 and parts[3]:
            verdicts[key] = CustodyVerdict("UNKNOWN", unquote(parts[3]))
        elif state == "HELD" and len(parts) == 5 and all(parts[3:]):
            verdicts[key] = CustodyVerdict("HELD", unquote(parts[3]), unquote(parts[4]))
        else:
            return unknown
    if verdicts.keys() != unknown.verdicts.keys():
        return unknown
    entries, venvs, elapsed = map(int, header.groups())
    return CustodyReport(verdicts, digest[1], entries, venvs, elapsed)


# Standalone stdlib-only code: never import/execute the metadata being inspected.
# The stat traversal is shared by both modes so DOMAIN has one source of truth.
CUSTODY_CENSUS_PY = r'''import ast
import hashlib
import json
import math
import os
import re
import stat
import sys
import time
from urllib.parse import quote, unquote, urlsplit

MAX_ENTRIES = 50000
MAX_FILE_BYTES = 2097152
MAX_TOTAL_BYTES = 67108864
IMPORT_PTH = {
    "_virtualenv.pth": "69ac3d8f27e679c81b94ab30b3b56e9cd138219b1ba94a1fa3606d5a76a1433d",
    "a1_coverage.pth": "ef2ed06d19867ec669c09a804060666a9cd5e383af0a9d11aa2de79b77d448e8",
}
started = time.monotonic()
root = os.path.abspath(sys.argv[1])
deadline = min(float(sys.argv[2]), started + 6)
mode = sys.argv[3]
keys = sys.argv[4:]
key_pattern = re.compile(r"[A-Za-z0-9-]+-[0-9a-f]{8}")
held = {}
unknown = ""
entries = {}
venvs = 0
bytes_read = 0
listed = 0

def check_time():
    if not math.isfinite(deadline) or time.monotonic() >= deadline:
        raise ValueError("deadline")

def note_unknown(reason):
    global unknown
    if not unknown:
        unknown = reason

def signature(s):
    return (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns)

def visit(path, *, require_owner=True):
    check_time()
    s = os.lstat(path)
    if require_owner and s.st_uid != os.geteuid():
        raise ValueError("incoming_owner_unknown:" + path)
    if not (stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode) or stat.S_ISLNK(s.st_mode)):
        raise ValueError("incoming_type_unknown:" + path)
    if path not in entries and len(entries) >= MAX_ENTRIES:
        raise ValueError("incoming_inventory_partial")
    if path in entries and entries[path] != signature(s):
        raise ValueError("incoming_domain_changed:" + path)
    entries[path] = signature(s)
    return s

def children(path, *, require_owner=True):
    global listed
    # Bound discovery as well as stats, including unrelated ROOT names. Never
    # recursively walk ROOT or an ordinary package/sandbox subdirectory.
    before = visit(path, require_owner=require_owner)
    names = []
    with os.scandir(path) as stream:
        for entry in stream:
            check_time()
            listed += 1
            if listed > MAX_ENTRIES:
                raise ValueError("incoming_inventory_partial")
            names.append(entry.name)
    if signature(os.lstat(path)) != signature(before):
        raise ValueError("incoming_domain_changed:" + path)
    return sorted(names)

target_names = {prefix + key: key for key in keys for prefix in
                ("", ".venv-lane-", ".venv-sync-stamp-")}

def reference(value, parent, reason, referrer):
    check_time()
    joined = os.path.join(parent, value)
    raw = os.path.abspath(joined)
    try:
        resolved = os.path.realpath(joined, strict=True)
    except FileNotFoundError:
        resolved = os.path.realpath(joined)
    for candidate in (raw, resolved):
        if candidate.startswith(root + os.sep):
            first = candidate[len(root) + 1:].split(os.sep, 1)[0]
            key = target_names.get(first)
            if key is not None and key not in held:
                held[key] = (reason, referrer)

def link(path, s):
    if stat.S_ISLNK(s.st_mode):
        value = os.readlink(path)
        if mode == "full":
            reference(value, os.path.dirname(path), "incoming_link", path)
        # A broken link not naming a target is merely an inventory entry.
        # lstat, rather than stat, deliberately counts that link.

def read_metadata(path, s, *, first_line=False):
    global bytes_read
    if not stat.S_ISREG(s.st_mode) or s.st_size > MAX_FILE_BYTES:
        raise ValueError("incoming_metadata_partial:" + path)
    if mode != "full":
        return None
    check_time()
    # NOFOLLOW and NONBLOCK keep a replaced file from becoming an unbounded
    # symlink/FIFO read. The outer timeout also covers slow filesystem calls.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if signature(os.fstat(stream.fileno())) != signature(s):
            raise ValueError("incoming_domain_changed:" + path)
        data = stream.readline(MAX_FILE_BYTES + 1) if first_line else stream.read(MAX_FILE_BYTES + 1)
        if signature(os.fstat(stream.fileno())) != signature(s):
            raise ValueError("incoming_domain_changed:" + path)
    check_time()
    bytes_read += len(data)
    if len(data) > MAX_FILE_BYTES or bytes_read > MAX_TOTAL_BYTES:
        raise ValueError("incoming_metadata_partial:bytes")
    return data

def read_executable(path, s):
    # Bound the bytes read, never a bin/ file's size: an ordinary venv holds
    # a 24 MB native bin/ruff. Only a "#!" first line is metadata.
    global bytes_read
    if mode != "full":
        return None
    check_time()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb", buffering=0) as stream:
        if signature(os.fstat(stream.fileno())) != signature(s):
            raise ValueError("incoming_domain_changed:" + path)
        data = stream.read(2)
        bytes_read += len(data)
        if bytes_read > MAX_TOTAL_BYTES:
            raise ValueError("incoming_metadata_partial:bytes")
        if data == b"#!":
            chunks = [data]
            fetched = len(data)
            while fetched <= MAX_FILE_BYTES:
                check_time()
                chunk = stream.read(min(4096, MAX_FILE_BYTES + 1 - fetched))
                bytes_read += len(chunk)
                fetched += len(chunk)
                if fetched > MAX_FILE_BYTES:
                    raise ValueError("incoming_metadata_partial:" + path)
                if bytes_read > MAX_TOTAL_BYTES:
                    raise ValueError("incoming_metadata_partial:bytes")
                chunks.append(chunk)
                if not chunk or b"\n" in chunk:
                    break
            # A chunk may include script bytes after the shebang. Charge them
            # above, but only interpret the first line as metadata.
            data = b"".join(chunks).partition(b"\n")[0]
        if signature(os.fstat(stream.fileno())) != signature(s):
            raise ValueError("incoming_domain_changed:" + path)
    check_time()
    if len(data) > MAX_FILE_BYTES:
        raise ValueError("incoming_metadata_partial:" + path)
    if bytes_read > MAX_TOTAL_BYTES:
        raise ValueError("incoming_metadata_partial:bytes")
    return data

def text_metadata(path, s):
    data = read_metadata(path, s)
    return None if data is None else data.decode("utf-8")

def static_editable_mappings(tree):
    names = {"MAPPING", "NAMESPACES"}
    bindings = {name: [] for name in names}
    literal_nodes = (ast.Constant, ast.Dict, ast.List, ast.Tuple, ast.Set, ast.Load)
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
        elif isinstance(statement, ast.AnnAssign):
            target = statement.target
        else:
            continue
        if isinstance(target, ast.Name) and target.id in names:
            value = statement.value
            if value is None or any(not isinstance(node, literal_nodes) for node in ast.walk(value)):
                return False
            bindings[target.id].append(target)
    # An absent name holds no value a finder could compute, so one bound name
    # suffices; an unbound name must then never occur.
    if any(len(targets) > 1 for targets in bindings.values()) or not any(bindings.values()):
        return False
    binding_nodes = {targets[0] for targets in bindings.values() if targets}
    unbound = {name for name, targets in bindings.items() if not targets}
    # Builtins and attributes that reach module globals by name.
    dynamic = {"globals", "vars", "locals", "exec", "eval", "compile", "getattr", "setattr",
               "delattr", "__import__", "attrgetter", "__dict__", "f_globals", "f_locals"}
    named = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.ExceptHandler) + tuple(
        getattr(ast, kind) for kind in ("MatchAs", "MatchStar") if hasattr(ast, kind))
    match_mapping = getattr(ast, "MatchMapping", ())
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    for node in ast.walk(tree):
        check_time()
        # Names bound through a string field, which no ast.Name records.
        if isinstance(node, named):
            bound = [node.name]
        elif isinstance(node, ast.alias):
            if node.name == "*":
                return False
            bound = [node.asname or node.name.partition(".")[0]]
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            bound = node.names
        elif isinstance(node, ast.arg):
            bound = [node.arg]
        elif isinstance(node, match_mapping):
            bound = [node.rest]
        else:
            bound = []
        if names.intersection(bound):
            return False
        if isinstance(node, ast.Attribute) and (node.attr in names or node.attr in dynamic):
            return False
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in names:
            return False
        if isinstance(node, ast.Name) and node.id in dynamic:
            return False
        if not isinstance(node, ast.Name) or node.id not in names or node in binding_nodes:
            continue
        if node.id in unbound or not isinstance(node.ctx, ast.Load):
            return False
        parent = parents[node]
        read = None
        if isinstance(parent, ast.Subscript) and parent.value is node and isinstance(parent.ctx, ast.Load):
            read = parent
        if isinstance(parent, ast.Compare) and any(
                comparator is node and isinstance(op, (ast.In, ast.NotIn))
                for op, comparator in zip(parent.ops, parent.comparators)):
            continue
        test = parent
        if isinstance(parent, ast.UnaryOp) and isinstance(parent.op, ast.Not):
            test = parents.get(parent)
        if isinstance(test, ast.If) and test.test in (node, parent):
            continue
        if isinstance(parent, ast.Attribute) and parent.value is node and parent.attr in (
                "get", "items", "keys", "values", "copy"):
            call = parents.get(parent)
            if isinstance(call, ast.Call) and call.func is parent:
                read = call
        # A read value stays a value: as a receiver or subscript base it could
        # mutate a list held inside the mapping. Mutation through a local alias
        # or a callee remains an accepted residual of hand-written finders.
        outer = parents.get(read)
        if read is None or (isinstance(outer, (ast.Attribute, ast.Subscript)) and outer.value is read):
            return False
    return True

def site_packages(path):
    for name in children(path):
        item = os.path.join(path, name)
        s = visit(item)
        link(item, s)
        if name.endswith(".dist-info"):
            if not stat.S_ISDIR(s.st_mode):
                raise ValueError("incoming_metadata_partial:" + item)
            direct = os.path.join(item, "direct_url.json")
            try:
                ds = visit(direct)
            except FileNotFoundError:
                continue
            raw = text_metadata(direct, ds)
            if raw is not None:
                doc = json.loads(raw)
                url = urlsplit(doc["url"])
                if url.scheme == "file":
                    if url.netloc not in ("", "localhost") or not url.path.startswith("/"):
                        raise ValueError("incoming_url_unknown:" + direct)
                    reference(unquote(url.path), item, "incoming_direct_url", direct)
            continue
        finder = name.startswith("__editable__") and name.endswith(".py")
        if not (name.endswith((".pth", ".egg-link")) or finder):
            continue
        raw = read_metadata(item, s)
        if raw is None:
            continue
        text = raw.decode("utf-8")
        if finder:
            tree = ast.parse(text)
            for node in ast.walk(tree):
                check_time()
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    reference(node.value, path, "incoming_editable_finder", item)
            if not static_editable_mappings(tree):
                note_unknown("dynamic_editable_metadata_unknown:" + item)
            continue
        lines = text.splitlines()
        if name.endswith(".egg-link"):
            lines = lines[:1]
        digest = hashlib.sha256(raw).hexdigest()
        imports = any(line.startswith(("import ", "import\t")) for line in lines)
        if (imports or name in IMPORT_PTH) and IMPORT_PTH.get(name) != digest:
            note_unknown("dynamic_pth_metadata_unknown:" + item + ":sha256=" + digest)
        for line in lines:
            line = line.rstrip()
            if not line or line.startswith("#") or line.startswith(("import ", "import\t")):
                continue
            reference(line, path, "incoming_pth_reference", item)

def library(path):
    for name in children(path):
        item = os.path.join(path, name)
        s = visit(item)
        link(item, s)
        if not re.fullmatch(r"python[0-9]+\.[0-9]+[a-z]*", name):
            continue
        if not stat.S_ISDIR(s.st_mode):
            raise ValueError("incoming_metadata_partial:" + item)
        # Enumerate one interpreter metadata directory, never package bodies.
        for child in children(item):
            sub = os.path.join(item, child)
            cs = visit(sub)
            link(sub, cs)
            if child in ("site-packages", "dist-packages"):
                if not stat.S_ISDIR(cs.st_mode):
                    raise ValueError("incoming_metadata_partial:" + sub)
                site_packages(sub)

def venv(path):
    global venvs
    venvs += 1
    for name in children(path):
        item = os.path.join(path, name)
        s = visit(item)
        link(item, s)
        if name == "bin":
            if not stat.S_ISDIR(s.st_mode):
                raise ValueError("incoming_metadata_partial:" + item)
            for child in children(item):
                executable = os.path.join(item, child)
                es = visit(executable)
                link(executable, es)
                if stat.S_ISREG(es.st_mode):
                    line = read_executable(executable, es)
                    if line is not None and line.startswith(b"#!"):
                        command = line[2:].decode("utf-8").strip().split()
                        if not command:
                            raise ValueError("incoming_shebang_unknown:" + executable)
                        reference(command[0], item, "incoming_shebang", executable)
        elif name == "pyvenv.cfg":
            text = text_metadata(item, s)
            if text is not None:
                for line in text.splitlines():
                    field, sep, value = line.partition("=")
                    if sep and field.strip() == "home":
                        reference(value.strip(), path, "incoming_pyvenv_home", item)
        elif name in ("lib", "lib64"):
            # uv's lib64 -> lib is a duplicate, not an additional domain.
            if stat.S_ISLNK(s.st_mode) and name == "lib64" and os.path.realpath(item) == os.path.join(path, "lib"):
                continue
            if not stat.S_ISDIR(s.st_mode):
                raise ValueError("incoming_metadata_partial:" + item)
            library(item)
        elif name in ("site-packages", "dist-packages"):
            if not stat.S_ISDIR(s.st_mode):
                raise ValueError("incoming_metadata_partial:" + item)
            site_packages(item)

def census():
    if mode not in ("full", "stat") or len(set(keys)) != len(keys) or not keys or any(not key_pattern.fullmatch(k) for k in keys):
        raise ValueError("invalid_census_arguments")
    if os.path.realpath(root) != root:
        raise ValueError("incoming_root_alias_unknown")
    for name in children(root):
        key = name.removeprefix(".venv-lane-")
        if key in keys or not key_pattern.fullmatch(key):
            continue
        path = os.path.join(root, name)
        sandbox = not name.startswith(".venv-lane-")
        s = visit(path, require_owner=not sandbox)
        link(path, s)
        if not stat.S_ISDIR(s.st_mode):
            # A key-shaped regular file is not a sibling sandbox. A linked
            # domain cannot be safely omitted, even when its hop is disjoint.
            if stat.S_ISLNK(s.st_mode):
                note_unknown("incoming_link_domain_unknown:" + path)
            continue
        if name.startswith(".venv-lane-"):
            venv(path)
        else:
            # Sandbox top-level entries are only stat'ed and readlink'ed; their
            # contents are never parsed. Venv metadata retains owner checks.
            for child in children(path, require_owner=False):
                item = os.path.join(path, child)
                link(item, visit(item, require_owner=False))
    # Detect changes during the bounded read, including directory membership.
    for path, before in entries.items():
        check_time()
        if signature(os.lstat(path)) != before:
            raise ValueError("incoming_domain_changed:" + path)

try:
    census()
except (OSError, ValueError, SyntaxError, UnicodeError, KeyError, TypeError, RecursionError) as exc:
    note_unknown(str(exc) or type(exc).__name__)

if mode == "stat":
    note_unknown("stat_only")
digest = hashlib.sha256()
for path, sig in sorted(entries.items()):
    # Tuple order is dev, inode, mode, size, mtime_ns, ctime_ns. DOMAIN's
    # documented record contains path, mtime_ns, size and inode only.
    digest.update(json.dumps([path, sig[4], sig[3], sig[1]], ensure_ascii=True, separators=(",", ":")).encode("ascii") + b"\n")
print("CENSUS entries=%d venvs=%d ms=%d" % (len(entries), venvs, (time.monotonic() - started) * 1000))
print("DOMAIN " + digest.hexdigest())
for key in keys:
    if key in held:
        reason, referrer = held[key]
        print("CUSTODY", key, "HELD", quote(reason, safe=""), quote(referrer, safe=""))
    elif unknown:
        print("CUSTODY", key, "UNKNOWN", quote(unknown, safe=""))
    else:
        print("CUSTODY", key, "CLEAR")
print("END")
'''


def build_custody_shell_function() -> str:
    """Build `_custody_census full|stat KEY...` using ROOT and guard helpers.

    Caller owns fd 8/9 already; fd 7 remains held in this shell until exit.
    The stat mode conveys a digest, never a fresh CLEAR verdict.
    """
    return r'''
_custody_unknown() {
  _custody_reason=$1
  shift
  printf 'CENSUS entries=0 venvs=0 ms=0\nDOMAIN %064d\n' 0
  for _custody_key in "$@"; do
    printf 'CUSTODY %s UNKNOWN %s\n' "$_custody_key" "$_custody_reason"
  done
  printf 'END\n'
}
_custody_census() {
  _custody_mode=$1
  shift
  _remaining=$(_guard_remaining) || { _custody_unknown deadline "$@"; return; }
  _remaining=$(python3 -c 'import sys; print(min(6.0, float(sys.argv[1])))' "$_remaining") || { _custody_unknown deadline "$@"; return; }
  _custody_lock="$ROOT/.queue-publication.lock"
  if [ -L "$_custody_lock" ] || { [ -e "$_custody_lock" ] && { [ ! -f "$_custody_lock" ] || [ ! -O "$_custody_lock" ]; }; }; then
    _custody_unknown publication_identity_unknown "$@"
    return
  fi
  _custody_before=$(stat -Lc '%d:%i' -- "$_custody_lock" 2>/dev/null) || _custody_before=''
  # Revalidation reuses the existing open description: reopening fd 7 would
  # release the publication fence between the full and stat-only observations.
  if ! { [ -f /proc/self/fd/7 ] && [ "$_custody_lock" -ef /proc/self/fd/7 ]; } 2>/dev/null; then
    if ! exec 7>>"$_custody_lock"; then
      _custody_unknown publication_open_unknown "$@"
      return
    fi
  fi
  if [ -L "$_custody_lock" ] || [ ! -f "$_custody_lock" ] || [ ! -O "$_custody_lock" ] || [ ! "$_custody_lock" -ef /proc/self/fd/7 ]; then
    _custody_unknown publication_identity_unknown "$@"
    return
  fi
  _custody_opened=$(stat -Lc '%d:%i' -- /proc/self/fd/7) || { _custody_unknown publication_identity_unknown "$@"; return; }
  if [ -n "$_custody_before" ] && [ "$_custody_before" != "$_custody_opened" ]; then
    _custody_unknown publication_identity_changed "$@"
    return
  fi
  if ! flock -n 7; then
    _custody_unknown publication_busy "$@"
    return
  fi
  if [ -L "$_custody_lock" ] || [ ! -f "$_custody_lock" ] || [ ! "$_custody_lock" -ef /proc/self/fd/7 ]; then
    _custody_unknown publication_identity_changed "$@"
    return
  fi
  timeout --foreground --signal=KILL "$_remaining" python3 - "$ROOT" "$_guard_deadline" "$_custody_mode" "$@" <<'WORKBAY_CUSTODY_PY'
''' + CUSTODY_CENSUS_PY + '''WORKBAY_CUSTODY_PY
}
'''
