"""Auth-probe shell renderers derived from the backend registry."""

from __future__ import annotations

import re


def _auth_marker_prefix(port) -> str:
    """Uppercase token for the probe's stdout markers; see :meth:`AuthPort.marker_prefix`."""
    return port.marker_prefix()


def _render_status_command(port) -> str:
    """``"$BIN" <status_argv...>`` with every token already whole-token validated."""
    return " ".join(['"$BIN"', *port.status_argv])


# Python fragment run ON THE VM by the key-info stage: argv[1] = response body
# path, argv[2] = policy threshold (USD). Exit 0 + one ``limit=… usage=…
# remaining=…`` line on stdout; 15 = budget exhausted / uncapped key refused
# by policy; 14 = any other shape (missing fields, non-JSON, non-numbers).
# Diagnostics go to stderr. Field names pinned by VERIFY-3 (implementation note).
KEY_INFO_PARSER_PY = r"""
import json, math, sys
try:
    body = open(sys.argv[1], encoding="utf-8").read()
    doc = json.loads(body)
except Exception as exc:
    print("key-info: non-JSON body (%s)" % type(exc).__name__, file=sys.stderr)
    sys.exit(14)
data = doc.get("data") if isinstance(doc, dict) else None
if not isinstance(data, dict):
    print("key-info: no data envelope", file=sys.stderr)
    sys.exit(14)
missing = [k for k in ("limit", "limit_remaining", "usage") if k not in data]
if missing:
    print("key-info: missing field(s) %s" % ",".join(missing), file=sys.stderr)
    sys.exit(14)
def num(v):
    # json.loads accepts NaN/Infinity; a non-finite budget is unverifiable, never OK.
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return float(v)
limit, remaining, usage = data["limit"], data["limit_remaining"], data["usage"]
if limit is None:
    print("key-info: limit is null (uncapped key refused by policy)", file=sys.stderr)
    sys.exit(15)
if remaining is None:
    print("key-info: limit_remaining is null", file=sys.stderr)
    sys.exit(15)
limit_n, remaining_n, usage_n = num(limit), num(remaining), num(usage)
if limit_n is None or remaining_n is None or usage_n is None:
    print("key-info: non-numeric or non-finite limit/limit_remaining/usage", file=sys.stderr)
    sys.exit(14)
threshold = float(sys.argv[2])
def fmt(v):
    return "%d" % v if float(v).is_integer() else repr(float(v))
# The reading is printed BEFORE the threshold verdict so a below-threshold
# probe still carries limit/usage/remaining to the host (implementation note S5: the
# budget alert names the numbers, not just the state).
print("limit=%s usage=%s remaining=%s" % (fmt(limit_n), fmt(usage_n), fmt(remaining_n)))
if remaining_n < threshold:
    print("key-info: limit_remaining %r below threshold %r" % (remaining_n, threshold), file=sys.stderr)
    sys.exit(15)
"""

_KEY_INFO_LINE_RE = re.compile(r"^limit=(?P<limit>\S+) usage=(?P<usage>\S+) remaining=(?P<remaining>\S+)$")

# Python fragment run ON THE VM by the credits stage: argv[1] = response body
# path, argv[2] = policy threshold (USD). Same contract as KEY_INFO_PARSER_PY:
# exit 0 + one machine-readable line; 15 = budget exhausted; 14 = any other
# shape. ``num()`` is copied verbatim — bool is not a number, non-finite is
# never OK, json.loads accepts NaN/Infinity and must be rejected.
CREDITS_PARSER_PY = r"""
import json, math, sys
try:
    body = open(sys.argv[1], encoding="utf-8").read()
    doc = json.loads(body)
except Exception as exc:
    print("credits: non-JSON body (%s)" % type(exc).__name__, file=sys.stderr)
    sys.exit(14)
data = doc.get("data") if isinstance(doc, dict) else None
if not isinstance(data, dict):
    print("credits: no data envelope", file=sys.stderr)
    sys.exit(14)
missing = [k for k in ("total_credits", "total_usage") if k not in data]
if missing:
    print("credits: missing field(s) %s" % ",".join(missing), file=sys.stderr)
    sys.exit(14)
def num(v):
    # json.loads accepts NaN/Infinity; a non-finite budget is unverifiable, never OK.
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return float(v)
credits, usage = data["total_credits"], data["total_usage"]
credits_n, usage_n = num(credits), num(usage)
if credits_n is None or usage_n is None:
    print("credits: non-numeric or non-finite total_credits/total_usage", file=sys.stderr)
    sys.exit(14)
threshold = float(sys.argv[2])
def fmt(v):
    return "%d" % v if float(v).is_integer() else repr(float(v))
available = credits_n - usage_n
# The reading is printed BEFORE the threshold verdict so a below-threshold
# probe still carries credits/usage/available to the host (implementation note S5: the
# budget alert names the numbers, not just the state).
print("credits=%s credit_usage=%s credits_available=%s" % (fmt(credits_n), fmt(usage_n), fmt(available)))
if available < threshold:
    print("credits: credits_available %r below threshold %r" % (available, threshold), file=sys.stderr)
    sys.exit(15)
"""

_CREDITS_LINE_RE = re.compile(
    r"^credits=(?P<credits>\S+) credit_usage=(?P<credit_usage>\S+) credits_available=(?P<credits_available>\S+)$"
)

# Reads the already-fetched key-info body (argv[1]). ``is_free_tier: true`` is
# the single-field tell that the account has never been funded; exit 15 with a
# distinct stderr line so the two exhaustion reasons stay separable.
FREE_TIER_CHECK_PY = r"""
import json, sys
try:
    doc = json.loads(open(sys.argv[1], encoding="utf-8").read())
except Exception:
    sys.exit(0)
data = doc.get("data") if isinstance(doc, dict) else None
if isinstance(data, dict) and data.get("is_free_tier") is True:
    print("key-info: is_free_tier is true (account has never been funded)", file=sys.stderr)
    sys.exit(15)
"""


def _vm_home_path(path: str) -> str:
    """Render a port path for the remote shell: ``~/x`` and bare ``x`` become ``$HOME/x``."""
    if path.startswith("/"):
        return path
    if path.startswith("~/"):
        return "$HOME/" + path[2:]
    return "$HOME/" + path


def _render_key_info_ok_branch(port, *, prefix: str, env_var: str) -> str:
    """``KEY_RC=0`` body: print the key-info line, then maybe the credits stage."""
    head = f'    printf \'%s\\n\' "$KEY_LINE"\n'
    if not getattr(port, "credits_url", None):
        return f"{head}    echo {prefix}_AUTH_OK\n    exit 0\n"
    return head + _render_credits_stage(port, prefix=prefix, env_var=env_var)


def _render_credits_stage(port, *, prefix: str, env_var: str) -> str:
    """Second authenticated fetch of the account credit pool.

    Runs only when ``port.credits_url`` is set, and only after a successful
    key-info parse. Bearer stays on stdin via ``curl -K -`` (SECD-02 / WEB-16).
    Prints the credits reading before the verdict; ``is_free_tier: true`` on
    the already-fetched key-info body is a distinct exhaustion reason.
    """
    return f"""    CREDITS_URL="{port.credits_url}"
    CREDITS_TMP=$(mktemp "${{TMPDIR:-/tmp}}/credits.XXXXXX") || {{ echo {prefix}_AUTH_UNVERIFIED; exit 14; }}
    trap 'rm -f "$KEY_TMP" "$CREDITS_TMP"' EXIT
    HTTP_CODE=$(printf 'header = "Authorization: Bearer %s"\\n' "${env_var}" | \
curl -sS --max-time 10 --connect-timeout 5 -K - -w '%{{http_code}}' -o "$CREDITS_TMP" "$CREDITS_URL")
    case "$HTTP_CODE" in
      401)
        echo {prefix}_AUTH_FAILED
        exit 13
        ;;
      2[0-9][0-9])
        ;;
      *)
        echo "credits: http ${{HTTP_CODE:-none}} from $CREDITS_URL" >&2
        echo {prefix}_AUTH_UNVERIFIED
        exit 14
        ;;
    esac
    CREDITS_LINE=$(python3 - "$CREDITS_TMP" "$MIN_REMAINING" <<'PY'
{CREDITS_PARSER_PY.strip()}
PY
)
    CREDITS_RC=$?
    python3 - "$KEY_TMP" <<'PY'
{FREE_TIER_CHECK_PY.strip()}
PY
    FREE_RC=$?
    case "$CREDITS_RC" in
      0|15)
        [ -n "$CREDITS_LINE" ] && printf '%s\\n' "$CREDITS_LINE"
        ;;
      *)
        if [ "$FREE_RC" -eq 15 ]; then
          echo {prefix}_BUDGET_EXHAUSTED
          exit 15
        fi
        echo {prefix}_AUTH_UNVERIFIED
        exit 14
        ;;
    esac
    if [ "$CREDITS_RC" -eq 15 ] || [ "$FREE_RC" -eq 15 ]; then
      echo {prefix}_BUDGET_EXHAUSTED
      exit 15
    fi
    echo {prefix}_AUTH_OK
    exit 0
"""


def _render_key_info_stage(port, *, prefix: str, env_var: str) -> str:
    """Authenticated step for an env_file port that declares ``key_info_url``.

    Runs after the env file is sourced (the binary's ``--version`` health
    check runs BEFORE sourcing — see ``_render_binary_health_check``). The
    bearer header is fed to curl on STDIN (``-K -``) — never ``-H`` on argv,
    which is visible in ``ps`` for the probe's lifetime (SECD-02 / WEB-16).
    A key containing a double quote or a backslash cannot be placed in a curl config line
    safely and is refused as invalid (S4-L-02). Exit map: 12 no key / unsafe
    key after sourcing; 13 HTTP 401; 15 budget exhausted or uncapped; 14
    anything else (non-2xx, non-JSON, non-finite, missing fields, no
    python3/curl, curl timeout). Threshold comes from the port — one source.
    When ``port.credits_url`` is set the success path continues into a second
    bearer-on-stdin fetch of the account credit pool (same curl idioms) and
    treats ``is_free_tier: true`` on the key-info body as exhausted too.
    """
    threshold = repr(float(port.min_remaining_usd if port.min_remaining_usd is not None else 0.0))
    ok_branch = _render_key_info_ok_branch(port, prefix=prefix, env_var=env_var)
    return f"""if [ -z "${{{env_var}:-}}" ]; then
  echo {prefix}_AUTH_INVALID
  exit 12
fi
case "${env_var}" in
  *'"'*|*'\\'*)
    echo "key-info: {env_var} contains a double quote or backslash; refusing to build the curl config" >&2
    echo {prefix}_AUTH_INVALID
    exit 12
    ;;
esac
if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
  echo "key-info: curl and python3 are required on the VM" >&2
  echo {prefix}_AUTH_UNVERIFIED
  exit 14
fi
KEY_INFO_URL="{port.key_info_url}"
MIN_REMAINING="{threshold}"
KEY_TMP=$(mktemp "${{TMPDIR:-/tmp}}/key-info.XXXXXX") || {{ echo {prefix}_AUTH_UNVERIFIED; exit 14; }}
trap 'rm -f "$KEY_TMP"' EXIT
HTTP_CODE=$(printf 'header = "Authorization: Bearer %s"\\n' "${env_var}" | curl -sS --max-time 10 --connect-timeout 5 -K - -w '%{{http_code}}' -o "$KEY_TMP" "$KEY_INFO_URL")
case "$HTTP_CODE" in
  401)
    echo {prefix}_AUTH_FAILED
    exit 13
    ;;
  2[0-9][0-9])
    ;;
  *)
    echo "key-info: http ${{HTTP_CODE:-none}} from $KEY_INFO_URL" >&2
    echo {prefix}_AUTH_UNVERIFIED
    exit 14
    ;;
esac
KEY_LINE=$(python3 - "$KEY_TMP" "$MIN_REMAINING" <<'PY'
{KEY_INFO_PARSER_PY.strip()}
PY
)
KEY_RC=$?
case "$KEY_RC" in
  0)
{ok_branch}    ;;
  15)
    # KEY_LINE is empty when limit/limit_remaining is null (no reading to carry).
    [ -n "$KEY_LINE" ] && printf '%s\\n' "$KEY_LINE"
    echo {prefix}_BUDGET_EXHAUSTED
    exit 15
    ;;
  *)
    echo {prefix}_AUTH_UNVERIFIED
    exit 14
    ;;
esac
"""


def _render_binary_health_check(prefix: str) -> str:
    """``"$BIN" --version`` before the env file is touched (S4-M-02).

    A present-but-broken binary is an install problem (exit 10 → MISSING
    remedy names the binary), never a credential one; running it before the
    env file is sourced also keeps the key out of the failing process.
    """
    return f"""if ! "$BIN" --version >/dev/null 2>&1; then
  echo {prefix}_INSTALL_BROKEN
  exit 10
fi
"""


def render_env_file_auth_probe(port) -> str:
    """Bash probe for an env_file-kind AuthPort (exit codes per the contract above).

    The cursor-remote literal from implementation note S7 with the binary, env file, key
    variable and status command substituted from ``port``; empty ``status_argv``
    honestly reports ``exit 14`` (never a false green).
    """
    if port.kind != "env_file" or not port.env_var:
        raise ValueError(f"render_env_file_auth_probe needs an env_file AuthPort, got kind={port.kind!r}")
    prefix = _auth_marker_prefix(port)
    bin_path = _vm_home_path(port.binary)
    env_file = _vm_home_path(port.require_env_file())
    env_var = port.env_var
    # Only key_info ports add the pre-source health check; the cursor literal
    # (status_argv ports) is reproduced byte-for-byte.
    health_block = _render_binary_health_check(prefix) if port.key_info_url else ""
    if port.status_argv:
        status_block = f"""STATUS_OUT=$({_render_status_command(port)} 2>&1)
STATUS_RC=$?
COMBINED=$(printf '%s' "$STATUS_OUT")
case "$COMBINED" in
  *"Not logged in"*|*"Authentication required"*)
    echo {prefix}_AUTH_FAILED
    exit 13
    ;;
esac
if [ "$STATUS_RC" -ne 0 ]; then
  echo {prefix}_AUTH_UNVERIFIED
  exit 14
fi
echo {prefix}_AUTH_OK
exit 0
"""
    elif port.key_info_url:
        status_block = _render_key_info_stage(port, prefix=prefix, env_var=env_var)
    else:
        status_block = f"""echo {prefix}_AUTH_UNVERIFIED
exit 14
"""
    # Trust boundary (S4-L-04): `set -a; . "$ENVF"` EXECUTES the env file. That
    # file is operator-written on the VM at mode 0600 by
    # scripts/provision_remote_auth.sh (never by a lane), so sourcing it is by
    # design — the probe trusts the VM operator, not the credential's content.
    return f"""
set +e
BIN="{bin_path}"
ENVF="{env_file}"
if ! test -x "$BIN"; then
  echo {prefix}_INSTALL_MISSING
  exit 10
fi
{health_block}if ! test -f "$ENVF" || ! test -r "$ENVF"; then
  echo {prefix}_AUTH_MISSING
  exit 11
fi
if ! grep -Eq '^[[:space:]]*{env_var}=[^#[:space:]=][^[:space:]]*' "$ENVF"; then
  echo {prefix}_AUTH_INVALID
  exit 12
fi
set -a
# shellcheck disable=SC1090
. "$ENVF"
set +a
{status_block}"""


def render_device_login_auth_probe(port) -> str:
    """Bash probe for a device_login-kind AuthPort (exit codes per the contract above).

    0 when the login artifact exists AND ``status_argv`` succeeds; 10 binary
    missing; 11 artifact missing; 14 when ``status_argv`` is empty (the port can
    only report unverified) or the status command fails ambiguously; 13 when it
    reports an explicit logged-out state.
    """
    if port.kind != "device_login" or not port.artifact:
        raise ValueError(f"render_device_login_auth_probe needs a device_login AuthPort, got kind={port.kind!r}")
    prefix = _auth_marker_prefix(port)
    bin_path = _vm_home_path(port.binary)
    artifact = _vm_home_path(port.artifact)
    if port.status_argv:
        status_block = f"""STATUS_OUT=$({_render_status_command(port)} 2>&1)
STATUS_RC=$?
COMBINED=$(printf '%s' "$STATUS_OUT")
case "$COMBINED" in
  *"Not logged in"*|*"Authentication required"*|*"not logged in"*)
    echo {prefix}_AUTH_FAILED
    exit 13
    ;;
esac
if [ "$STATUS_RC" -ne 0 ]; then
  echo {prefix}_AUTH_UNVERIFIED
  exit 14
fi
echo {prefix}_AUTH_OK
exit 0
"""
    else:
        # No headless status command declared: artifact presence alone is never
        # a green — report honest unverified.
        status_block = f"""echo {prefix}_AUTH_UNVERIFIED
exit 14
"""
    return f"""
set +e
BIN="{bin_path}"
ARTIFACT="{artifact}"
if ! test -x "$BIN"; then
  echo {prefix}_INSTALL_MISSING
  exit 10
fi
if ! test -e "$ARTIFACT" || ! test -r "$ARTIFACT"; then
  echo {prefix}_AUTH_MISSING
  exit 11
fi
{status_block}"""


def render_auth_probe(port) -> str:
    """Pick the renderer by ``port.kind``."""
    if port.kind == "env_file":
        return render_env_file_auth_probe(port)
    return render_device_login_auth_probe(port)
