"""Doctor shell renderers derived from the backend registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from workbay_orchestrator_mcp.orchestration.backend_registry import AuthPort


def _registry():
    from workbay_orchestrator_mcp.orchestration import backend_registry

    return backend_registry


def _tracked_0xalpha_model():
    from workbay_orchestrator_mcp.orchestration.oxalpha_lane_config import TRACKED_0XALPHA_MODEL

    return TRACKED_0XALPHA_MODEL


def _vm_home_path(path: str) -> str:
    """Render a port path for the remote shell: ``~/x`` and bare ``x`` become ``$HOME/x``."""
    if path.startswith("/"):
        return path
    if path.startswith("~/"):
        return "$HOME/" + path[2:]
    return "$HOME/" + path


# Public, unauthenticated list-price source for the doctor's "price" line.
# Fetched at most once per doctor run; failure degrades to "unavailable".
OXALPHA_MODELS_LIST_URL = "https://openrouter.ai/api/v1/models"


def doctor_auth_ports() -> list[tuple[str, "AuthPort"]]:
    """Every declared ``AuthPort`` in registry order (backend id, port).

    One doctor ``auth`` line is rendered per entry — the doctor holds no table
    of its own, so adding a port here adds a line there (implementation note S5).
    """
    registry = _registry()
    return [(name, spec.auth) for name, spec in registry.BACKENDS.items() if spec.auth is not None]


def _doctor_auth_artifact(port) -> str:
    """VM path whose presence the doctor reports (never its contents/perms)."""
    if port.kind == "env_file":
        return _vm_home_path(port.env_file or "")
    return _vm_home_path(port.artifact or "")


def render_doctor_auth_line(name: str, port) -> str:
    """Bash for one ``auth    : <backend> (<kind>) <path>: <state>`` line.

    env_file ports report ``present`` (non-empty and readable), ``empty``,
    ``unreadable`` or ``MISSING``; device_login ports report ``exists`` or
    ``MISSING``.

    Presence + kind only: the doctor never prints the value, never sources the
    file, and never reports the perms of a file that could leak (SEC-08).
    """
    artifact = _doctor_auth_artifact(port)
    if not artifact:
        raise ValueError(f"AuthPort for {name!r} declares no artifact/env_file")
    label = f"{name} ({port.kind})"
    prefix = f"auth    : {label} {artifact}: "
    if port.kind == "env_file":
        # An env file that exists but is empty or unreadable cannot authenticate
        # anything; say so instead of a false "present" (S5-L-02).
        return (
            f'if test -s "{artifact}" && test -r "{artifact}"; then echo "{prefix}present"; '
            f'elif test -e "{artifact}" && ! test -r "{artifact}"; then echo "{prefix}unreadable"; '
            f'elif test -e "{artifact}"; then echo "{prefix}empty"; '
            f'else echo "{prefix}MISSING"; fi\n'
        )
    # device_login artifacts are opaque (vendor-owned JSON); existence is all
    # the doctor can truthfully report.
    return f'if test -e "{artifact}"; then echo "{prefix}exists"; else echo "{prefix}MISSING"; fi\n'


def _render_doctor_key_info_block(name: str, port) -> str:
    """Budget + list-price lines for a key_info port (0xalpha, implementation note S5).

    The registry-rendered auth probe runs on the VM (bearer on stdin, key never
    on argv) and its ``limit=… usage=… remaining=…`` data line is echoed; the
    slug's current list price comes from the public models index, fetched once
    per doctor run with ``--max-time 10`` and reported as ``unavailable`` on
    any failure — never a hang, never a fabricated "$0".
    """
    probe = _registry().render_auth_probe(port)
    if "WB_DOCTOR_PROBE_EOF" in probe:
        raise ValueError("rendered auth probe collides with the doctor heredoc delimiter")
    slug = _tracked_0xalpha_model()
    return f"""_wb_probe=$(mktemp "${{TMPDIR:-/tmp}}/wb-doctor-probe.XXXXXX") || _wb_probe=""
if [ -n "$_wb_probe" ]; then
cat > "$_wb_probe" <<'WB_DOCTOR_PROBE_EOF'
{probe.strip()}
WB_DOCTOR_PROBE_EOF
_wb_out=$(bash "$_wb_probe" 2>/dev/null </dev/null); _wb_rc=$?
rm -f "$_wb_probe"
_wb_line=$(printf '%s\\n' "$_wb_out" | grep '^limit=' | head -n 1)
_wb_marker=$(printf '%s\\n' "$_wb_out" | grep '^{port.marker_prefix()}_' | head -n 1)
echo "budget  : {name} ${{_wb_line:-unavailable}} (probe exit $_wb_rc${{_wb_marker:+, $_wb_marker}}; threshold {port.min_remaining_usd} USD)"
else
echo "budget  : {name} unavailable (mktemp failed)"
fi
if [ -z "${{_wb_models_json:-}}" ] && command -v curl >/dev/null 2>&1; then
_wb_models_json=$(curl -sS --max-time 10 --connect-timeout 5 "{OXALPHA_MODELS_LIST_URL}" 2>/dev/null) || _wb_models_json=""
fi
_wb_price=""
if [ -n "${{_wb_models_json:-}}" ] && command -v python3 >/dev/null 2>&1; then
_wb_price=$(printf '%s' "$_wb_models_json" | python3 -c '
import json, sys
slug = sys.argv[1]
try:
    doc = json.load(sys.stdin)
    rows = doc.get("data") or []
    row = next((r for r in rows if isinstance(r, dict) and r.get("id") == slug), None)
    if row is None:
        print("not listed")
    else:
        pricing = row.get("pricing") or {{}}
        print("prompt=%s completion=%s USD/token" % (pricing.get("prompt", "?"), pricing.get("completion", "?")))
except Exception:
    pass
' "{slug}" 2>/dev/null)
fi
echo "price   : {slug} ${{_wb_price:-unavailable}} (list, {OXALPHA_MODELS_LIST_URL})"
"""


def render_doctor_auth_script() -> str:
    """Bash streamed to the VM by ``remote_agent.sh doctor`` (``bash -s``).

    One ``auth`` line per declared port; key_info ports additionally get the
    ``budget`` and ``price`` lines. ``set +e``: every line reports, none aborts.
    """
    parts = ["set +e\n"]
    for name, port in doctor_auth_ports():
        parts.append(render_doctor_auth_line(name, port))
        if port.key_info_url:
            parts.append(_render_doctor_key_info_block(name, port))
    return "".join(parts)
