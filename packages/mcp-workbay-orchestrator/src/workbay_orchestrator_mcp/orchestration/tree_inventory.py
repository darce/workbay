"""Lossless leaf inventories from binary, NUL-delimited ``git ls-tree -rz``.

Paths remain bytes; only the ASCII mode, object type and OID are decoded.
Malformed or incomplete evidence returns ``(None, "tree_inventory_invalid")``
instead of an empty inventory or a partially validated prefix.
"""

from __future__ import annotations

import os
import re
import subprocess

TreeEntries = dict[bytes, tuple[str, str, str]]
_HEADER = re.compile(rb"(100644|100755|120000|160000) (blob|commit) ([0-9a-f]{40}|[0-9a-f]{64})")


def parse_tree_inventory(payload: bytes) -> tuple[TreeEntries | None, str | None]:
    """Validate a complete recursive leaf inventory, rejecting duplicate paths.

    A successful empty tree is ``({}, None)``. Every refusal has no entries and
    the typed reason ``tree_inventory_invalid``. A path may contain tabs,
    newlines, quotes or non-UTF-8 bytes; only NUL terminates a record.
    """
    if not isinstance(payload, bytes) or (payload and not payload.endswith(b"\0")):
        return None, "tree_inventory_invalid"

    entries: TreeEntries = {}
    for record in payload.split(b"\0")[:-1]:
        header, separator, path = record.partition(b"\t")
        match = _HEADER.fullmatch(header)
        if not separator or not path or match is None:
            return None, "tree_inventory_invalid"
        mode, kind, oid = match.groups()
        if (mode == b"160000") != (kind == b"commit") or path in entries:
            return None, "tree_inventory_invalid"
        entries[path] = (mode.decode("ascii"), kind.decode("ascii"), oid.decode("ascii"))
    return entries, None


def read_tree_inventory(ref: str) -> tuple[TreeEntries | None, str | None]:
    """Read a full tree from the current Git repository without changing it.

    Git failures, I/O errors and the 30-second read deadline return the same
    typed refusal as malformed output. Replacement objects are disabled so
    the inventory describes the requested tree itself.
    """
    try:
        result = subprocess.run(
            ["git", "--no-replace-objects", "ls-tree", "-rz", "--full-tree", "--end-of-options", ref],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=30,
            env=dict(os.environ, GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1"),
        )
    except (OSError, subprocess.SubprocessError):
        return None, "tree_inventory_invalid"
    if result.returncode:
        return None, "tree_inventory_invalid"
    return parse_tree_inventory(result.stdout)
