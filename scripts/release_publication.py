"""Retained git publication subjects and host-private, operator-only approvals.

This mediates callers of effect(), not arbitrary Git/API credentials. Authority
uses the native Git-common-dir/private-owner pattern, with a distinct scope.
Readers never provision a receiver key. See remote-main-publication-v1.md.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import os
import pwd
import re
import selectors
import shutil
import signal
import ssl
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

MAIN = "refs/heads/main"
HELD = 3
SCOPE = "workbay/remote-main-publication@1"
GITHUB_POLICY = "github-public-exact-tag-absence@1"
GITHUB_API_VERSION = "2022-11-28"


class PublicationHeld(ValueError):
    """Preparation is retained; publication needs operator action or a new plan."""


def require(condition, reason):
    if not condition:
        raise PublicationHeld(reason)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def git(repo, *args, input=None, transport=False, packet_trace=False):
    # Do not inherit worker-selected Git directories, config injection, or replace
    # objects. Transport uses the frozen URL, without a second global URL rewrite.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    env["GIT_TERMINAL_PROMPT"] = "0"
    if transport is not False:
        return transport_git(repo, args, env, transport if isinstance(transport, dict) else {},
                             input=input, packet_trace=packet_trace)
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        input=input,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout.strip()


def bounded_probe(argv, env, *, timeout=30):
    """Bound both packet diagnostics and stdout, including a stalled peer."""
    chunks = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout
    with subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          start_new_session=True) as process:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")
                while selector.get_map():
                    require(time.monotonic() < deadline, "remote_namespace_unknown:probe_timeout")
                    for key, _ in selector.select(min(0.2, max(0, deadline - time.monotonic()))):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if not data:
                            selector.unregister(key.fileobj)
                        else:
                            chunks[key.data].extend(data)
                            require(sum(map(len, chunks.values())) <= 4 * 1024 * 1024,
                                    "remote_namespace_unknown:probe_limit")
                process.wait(timeout=max(0.01, deadline - time.monotonic()))
            out, err = (chunks[k].decode("utf-8", errors="strict") for k in ("stdout", "stderr"))
            if process.returncode:
                raise PublicationHeld("remote_namespace_unknown:probe_failed:" + err[-2000:])
            return out.strip(), err
        except (UnicodeError, subprocess.TimeoutExpired) as exc:
            raise PublicationHeld("remote_namespace_unknown:incomplete_probe") from exc
        finally:
            # Also reap transport helper descendants on overflow/timeout.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def transport_git(repo, args, env, settings, *, input=None, packet_trace=False):
    """Use the frozen endpoint/trust, never the export's executable Git config.

    A temporary bare shell shares only the object directory. It has no mutable
    remotes, URL rewrites, includes, helpers, hooks or implicit ref updates.
    CA bytes live in the content-addressed subject, not a mutable CA pathname.
    """
    for key in list(env):
        if key.lower() in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"} or key in {
            "CURL_CA_BUNDLE", "SSL_CERT_FILE", "SSL_CERT_DIR"}:
            env.pop(key)
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
               GIT_SSH_COMMAND="ssh -F /dev/null -oBatchMode=yes -oStrictHostKeyChecking=yes")
    objects = common(repo) / "objects"
    with tempfile.TemporaryDirectory(prefix="publication-transport-") as directory:
        shell = Path(directory)
        (shell / "objects").mkdir()
        (shell / "refs").mkdir()
        (shell / "HEAD").write_text("ref: refs/heads/unused\n")
        (shell / "config").write_text("[core]\n\tbare = true\n")
        env.update(GIT_DIR=str(shell), GIT_OBJECT_DIRECTORY=str(objects))
        options = ["-c", "protocol.version=2", "-c", "http.sslVerify=true",
                   "-c", "http.followRedirects=false", "-c", "core.hooksPath=/dev/null"]
        if settings.get("ca_pem"):
            ca = shell / "ca.pem"
            ca.write_text(settings["ca_pem"])
            options += ["-c", "http.sslCAInfo=" + str(ca)]
        argv = ["git", *options, *args]
        if packet_trace:
            env["GIT_TRACE_PACKET"] = "1"
            return bounded_probe(argv, env)
        return subprocess.run(argv, input=input, env=env, check=True, capture_output=True,
                              text=True, timeout=120).stdout.strip()


def transport_settings(repo, url):
    """Allow CA trust only; arbitrary export transport configuration is ignored."""
    if not url.startswith("https://"):
        return {}

    def setting(key, *options):
        try:
            return git(repo, "config", *options, "--get-urlmatch", "http." + key, url)
        except subprocess.CalledProcessError as exc:
            require(exc.returncode == 1, "tls_configuration_unknown")
            return ""

    require(setting("sslVerify", "--type=bool") != "false", "insecure_tls_unsupported")
    require(not setting("sslCAPath"), "tls_ca_directory_unknown:use_CA_bundle")
    path = setting("sslCAInfo", "--path")
    if not path:
        return {}
    path = Path(path)
    if not path.is_absolute():
        path = Path(repo) / path
    try:
        with path.open("rb") as stream:
            raw = stream.read(1024 * 1024 + 1)
        require(len(raw) <= 1024 * 1024, "tls_ca_bundle_too_large")
        pem = raw.decode("ascii")
        ssl.create_default_context(cadata=pem)
    except (OSError, UnicodeError, ssl.SSLError) as exc:
        raise PublicationHeld("tls_ca_trust_unknown") from exc
    return {"ca_pem": pem}


def github_repository(url):
    """Conservative recognition of the *resolved push URL*, never remote name.

    No userinfo other than the fixed SSH user, ports, escapes, queries, URL
    helpers, alternate hosts or path aliases enter this service profile.
    """
    match = re.fullmatch(
        r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)"
        r"([A-Za-z0-9][A-Za-z0-9-]{0,38})/([A-Za-z0-9_.-]{1,104})", url)
    if not match:
        return None
    owner, name = match.groups()
    name = name.removesuffix(".git")
    if not name or len(name) > 100 or name in (".", ".."):
        return None
    return owner + "/" + name


def github_json(raw):
    def unique(pairs):
        result = dict(pairs)
        require(len(result) == len(pairs), "remote_namespace_unknown:github_duplicate_field")
        return result

    return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: require(
        False, "remote_namespace_unknown:github_non_json_constant"))


def _github_https_get(path):
    """Isolated read-only worker; no Git config, credentials, proxies or redirects.

    The parent supervises wall time and output even during DNS, TLS or headers.
    It starts Python with -I and a minimal environment, so platform CA trust
    cannot be replaced by SSL_CERT_FILE/SSL_CERT_DIR or caller Python modules.
    """
    connection = http.client.HTTPSConnection("api.github.com", timeout=10,
                                             context=ssl.create_default_context())
    try:
        connection.request("GET", path, headers={
            "Accept": "application/vnd.github+json", "User-Agent": "workbay-publication",
            "X-GitHub-Api-Version": GITHUB_API_VERSION, "Cache-Control": "no-cache"})
        response = connection.getresponse()
        require(response.status in (200, 404), "remote_namespace_unknown:github_status")
        require(not any(response.getheader(k) for k in
                        ("Location", "WWW-Authenticate", "Retry-After", "Content-Encoding")),
                "remote_namespace_unknown:github_response")
        remaining = response.getheader("X-RateLimit-Remaining", "")
        require(remaining.isdecimal() and int(remaining) > 0,
                "remote_namespace_unknown:github_rate_limit")
        require(response.getheader("Content-Type", "").split(";", 1)[0] == "application/json",
                "remote_namespace_unknown:github_content_type")
        require(response.getheader("X-GitHub-Api-Version-Selected") == GITHUB_API_VERSION,
                "remote_namespace_unknown:github_api_version")
        raw = response.read(1024 * 1024 + 1)
        require(len(raw) <= 1024 * 1024, "remote_namespace_unknown:github_body_limit")
        length = response.getheader("Content-Length")
        require(length is None or (length.isdecimal() and int(length) == len(raw)),
                "remote_namespace_unknown:github_incomplete_body")
        return response.status, github_json(raw.decode("utf-8"))
    finally:
        connection.close()


def github_get(path, deadline):
    require(path.startswith("/repos/") and not any(c in path for c in "\r\n?#"),
            "remote_namespace_unknown:github_path")
    remaining = deadline - time.monotonic()
    require(remaining > 0, "remote_namespace_unknown:github_timeout")
    # A subprocess bounds slow-drip headers/DNS as well as body reads. This
    # worker only returns observations; it cannot grant publication authority.
    code = ("import json,runpy,sys; m=runpy.run_path(sys.argv[1]); "
            "print(json.dumps(m['_github_https_get'](sys.argv[2])))")
    try:
        out, _ = bounded_probe(
            [sys.executable, "-I", "-B", "-c", code, str(Path(__file__).resolve()), path],
            {"PATH": os.defpath, "LANG": "C.UTF-8"}, timeout=min(10, remaining))
        status, data = github_json(out)
        require(type(status) is int and status in (200, 404) and isinstance(data, dict),
                "remote_namespace_unknown:github_schema")
        return status, data
    except (ValueError, OSError) as exc:
        raise PublicationHeld("remote_namespace_unknown:github_observation_failed") from exc


def github_profile(url, deadline):
    repository = github_repository(url)
    require(repository is not None, "remote_namespace_unknown:github_endpoint")
    status, data = github_get("/repos/" + repository, deadline)
    full_name = data.get("full_name")
    require(status == 200 and type(data.get("id")) is int and data["id"] > 0
            and data.get("private") is False and data.get("visibility") == "public"
            and isinstance(full_name, str) and full_name.casefold() == repository.casefold()
            and github_repository("https://github.com/" + full_name) == full_name
            and data.get("clone_url") == "https://github.com/" + full_name + ".git"
            and data.get("ssh_url") == "git@github.com:" + full_name + ".git"
            and data.get("url") == "https://api.github.com/repos/" + full_name,
            "remote_namespace_unknown:github_repository_identity")
    return dict(policy=GITHUB_POLICY, api_version=GITHUB_API_VERSION,
                api_origin="https://api.github.com", trust="isolated-platform-ca@1",
                push_url=url, repository=full_name, repository_id=data["id"])


def reference_settings(repo, url):
    settings = transport_settings(repo, url)
    if github_repository(url) is not None:
        settings["reference_provider"] = github_profile(url, time.monotonic() + 30)
    return settings


def github_absent_tags(url, settings, missing):
    """Service contract proof, not inference from ref-object JSON shape.

    Exact public repository accessibility and identity bracket exact 404s.
    A 200 for an unadvertised ref is UNKNOWN, even with a plausible object.
    """
    provider = settings.get("reference_provider")
    if not provider:
        if missing:
            raise PublicationHeld("remote_namespace_unknown:destination_unadvertised:" + missing[0])
        return
    deadline = time.monotonic() + 30
    require(provider == github_profile(url, deadline), "remote_namespace_unknown:github_identity_changed")
    require(len(missing) <= 32, "remote_namespace_unknown:github_ref_limit")
    for dest in missing:
        require(dest.startswith("refs/tags/"), "remote_namespace_unknown:destination_unadvertised:" + dest)
        path = "/repos/" + provider["repository"] + "/git/ref/" + quote(dest[5:], safe="/")
        status, data = github_get(path, deadline)
        require(status == 404 and data.get("message") == "Not Found"
                and data.get("status") == "404"
                and data.get("documentation_url") == "https://docs.github.com/rest/git/refs#get-a-reference"
                and set(data) <= {"message", "status", "documentation_url"},
                "remote_namespace_unknown:github_exact_absence_unproved:" + dest)
    if missing:
        require(provider == github_profile(url, deadline), "remote_namespace_unknown:github_identity_changed")


def common(repo):
    return Path(git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()


def authority(repo):
    return common(repo) / "workbay" / "remote-main-publication"


def store(repo):
    # Custody relocation does not select an issuer/key or grant authority.
    return Path(os.environ.get("RELEASE_PUBLIC_PREPARED_ROOT", str(authority(repo) / "prepared"))).absolute()


def private_dir(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_stat(path, directory=True)


def private_stat(path, *, directory=False):
    info = path.lstat()
    require(not stat.S_ISLNK(info.st_mode), "symlink_custody")
    require(info.st_uid == os.getuid() and not info.st_mode & 0o077, "insecure_custody")
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode), "invalid_custody_type")


def private_bytes(path):
    private_stat(path.parent, directory=True)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(
            stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077, "insecure_custody"
        )
        return stream.read()


def append(path, raw):
    """Durable immutable identity: publish complete bytes, never replace."""
    private_dir(path.parent)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
            temporary.chmod(0o400)
            try:
                os.link(temporary, path)
            except FileExistsError:
                require(private_bytes(path) == raw, "immutable_identity_conflict")
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)


def source_identity(repo):
    repo = Path(git(repo, "rev-parse", "--show-toplevel")).resolve()
    # Export reads tracked working files, so HEAD alone is insufficient. Bind
    # tracked modifications too, including index/worktree differences and modes.
    storage = common(repo)
    info = storage.stat()
    return dict(
        repository=str(repo),
        common_dir=str(storage),
        storage_identity=[info.st_dev, info.st_ino],
        head=git(repo, "rev-parse", "HEAD"),
        tree=git(repo, "rev-parse", "HEAD^{tree}"),
        working_diff=digest(git(repo, "diff", "--binary", "HEAD", "--")),
    )


def config(repo, key):
    try:
        return git(repo, "config", "--get-all", key).splitlines()
    except subprocess.CalledProcessError as exc:
        if exc.returncode != 1:
            raise
        return []


def remote_identity(repo, remote=None):
    branch = git(repo, "symbolic-ref", "--quiet", "--short", "HEAD") if remote is None else ""
    if remote is None:
        choices = (
            config(repo, f"branch.{branch}.pushRemote")
            or config(repo, "remote.pushDefault")
            or config(repo, f"branch.{branch}.remote")
            or ["origin"]
        )
        require(len(choices) == 1, "ambiguous_remote")
        remote = choices[0]
    require(isinstance(remote, str) and remote and not remote.startswith("-"), "invalid_remote")
    named = remote in git(repo, "remote").splitlines()
    if named:
        require(config(repo, f"remote.{remote}.mirror") not in (["true"], ["yes"], ["1"]), "mirror_push_unsupported")
        urls = git(repo, "remote", "get-url", "--push", "--all", remote).splitlines()
    else:
        # Git does not enumerate a remote defined only via -c. For a literal,
        # apply its longest pushInsteadOf match, else Git's read URL resolver.
        try:
            rules = git(repo, "config", "--null", "--get-regexp", r"^url\..*\.pushinsteadof$")
        except subprocess.CalledProcessError as exc:
            if exc.returncode != 1:
                raise
            rules = ""
        matches = []
        for rule in rules.split("\0"):
            if "\n" in rule:
                key, prefix = rule.split("\n", 1)
                if remote.startswith(prefix):
                    matches.append((len(prefix), key[4:-14], prefix))
        if matches:
            _, base, prefix = max(matches, key=lambda item: item[0])
            urls = [base + remote[len(prefix) :]]
        else:
            urls = [git(repo, "ls-remote", "--get-url", remote)]
    require(len(urls) == 1, "multiple_push_urls_unsupported")
    url = urls[0]
    require(url and not url.startswith("-") and "::" not in url, "unsafe_transport")
    if ":" not in url:
        url = str((Path(repo) / url).resolve())
    elif url.startswith("file://"):
        url = "file://" + str(Path(url[7:]).resolve())
    local_identity = None
    if url.startswith("/") or url.startswith("file://"):
        info = Path(url.removeprefix("file://")).stat()
        local_identity = [info.st_dev, info.st_ino]
    return dict(
        requested=remote,
        push_url=url,
        named=named,
        local_identity=local_identity,
        configured_push=config(repo, f"remote.{remote}.push") if named else [],
    )


def v2_refs(trace):
    """Read only the client's actual packet exchange, not agent/version hints.

    Git's v2 ls-refs contract attaches symref-target to each symbolic entry
    when symrefs was requested. Missing entries prove nothing about dangling
    or hidden refs. A truncated/older/opaque exchange is UNKNOWN.
    """
    packets = re.findall(r"packet:\s+ls-remote([<>]) (.*)", trace)
    state, capable, symrefs = "version", False, False
    refs, symbolic, entries = {}, {}, set()
    for direction, value in packets:
        if state == "version":
            require((direction, value) == ("<", "version 2"), "remote_namespace_unknown:protocol")
            state = "capabilities"
        elif state == "capabilities":
            require(direction == "<", "remote_namespace_unknown:capabilities")
            if value == "ls-refs" or value.startswith("ls-refs="):
                capable = True
            if value == "0000":
                require(capable, "remote_namespace_unknown:ls_refs_unavailable")
                state = "command"
        elif state == "command":
            require((direction, value) == (">", "command=ls-refs"), "remote_namespace_unknown:command")
            state = "request"
        elif state == "request":
            require(direction == ">", "remote_namespace_unknown:request")
            if value == "0001":
                state = "arguments"
        elif state == "arguments":
            require(direction == ">", "remote_namespace_unknown:arguments")
            symrefs |= value == "symrefs"
            if value == "0000":
                require(symrefs, "remote_namespace_unknown:symrefs_unrequested")
                state = "refs"
        elif state == "refs":
            require(direction == "<", "remote_namespace_unknown:response")
            if value == "0000":
                state = "done"
                continue
            fields = value.split(" ")
            require(len(fields) >= 2, "remote_namespace_unknown:ref_entry")
            oid, name, *attributes = fields
            require((oid == "unborn" or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", oid))
                    and name not in entries and (name == "HEAD" or name.startswith("refs/")),
                    "remote_namespace_unknown:ref_entry")
            entries.add(name)
            for attr in attributes:
                if attr.startswith("symref-target:"):
                    target = attr[len("symref-target:"):]
                    require(name not in symbolic and target.startswith("refs/"),
                            "remote_namespace_unknown:symref_entry")
                    symbolic[name] = target
                else:
                    require(re.fullmatch(r"peeled:([0-9a-f]{40}|[0-9a-f]{64})", attr),
                            "remote_namespace_unknown:ref_attribute")
            require(oid != "unborn" or name in symbolic, "remote_namespace_unknown:unborn")
            if oid != "unborn" and name.startswith("refs/"):
                refs[name] = oid
        else:
            require((direction, value) in (("<", "0002"), (">", "0000")),
                    "remote_namespace_unknown:extra_exchange")
    require(state == "done", "remote_namespace_unknown:incomplete_exchange")
    return refs, symbolic


def remote_refs(repo, identity, *, destinations=(), transport=None):
    """Observe OIDs without discarding the receiver's symbolic mappings.

    Only direct destinations are supported. An OID lease does not bind a
    symbolic name, even when its referent currently has the same OID. Network
    advertisements can omit mappings. Local inspection includes unborn refs;
    network proof requires a v2 per-ref entry or the public GitHub exact-tag
    absence service profile. Receiver admins must
    keep that namespace stable through receive-pack (see the contract).
    """
    settings = transport if transport is not None else reference_settings(repo, identity["push_url"])
    network = identity["local_identity"] is None
    result = git(repo, "ls-remote", "--symref", identity["push_url"], transport=settings, packet_trace=network)
    if network:
        out, trace = result
        proven_refs, proven_symbolic = v2_refs(trace)
    else:
        out = result
    refs, symbolic = {}, {}
    for line in out.splitlines():
        fields = line.split("\t")
        require(len(fields) == 2, "remote_namespace_unknown")
        value, name = fields
        if value.startswith("ref: "):
            require(name not in symbolic, "remote_namespace_unknown")
            symbolic[name] = value[5:]
        else:
            require(re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value), "remote_namespace_unknown")
            require(name not in refs, "remote_namespace_unknown")
            if name.startswith("refs/") and not name.endswith("^{}"):
                refs[name] = value
    if network:
        require(refs == proven_refs and all(proven_symbolic.get(k) == v for k, v in symbolic.items()),
                "remote_namespace_unknown:advertisement_mismatch")
    missing = []
    for dest in destinations:
        require(dest not in symbolic, "symbolic_destination_unsupported:" + dest)
        # --symref alone cannot prove absence: older protocols/receivers may
        # advertise only HEAD's mapping, and dangling aliases may be omitted.
        if network:
            require(dest not in proven_symbolic, "symbolic_destination_unsupported:" + dest)
            if dest not in proven_refs:
                missing.append(dest)
            continue
        receiver = identity["push_url"].removeprefix("file://")
        try:
            target = git(receiver, "symbolic-ref", "--quiet", "--no-recurse", dest)
        except subprocess.CalledProcessError as exc:
            require(exc.returncode == 1, "remote_namespace_unknown")
        else:
            require(not target, "symbolic_destination_unsupported:" + dest)
    if network:
        github_absent_tags(identity["push_url"], settings, missing)
    return refs


def resolve_updates(repo, objects, remote=None, refspecs=None, *, transport=None):
    """Resolve supported Git routes before deciding authority; fail closed on
    wildcard/matching/deletion/force/duplicate/ambiguous routes. No push here.
    All supported sources must identify the checked export HEAD.
    """
    identity = remote_identity(repo, remote)
    settings = transport if transport is not None else reference_settings(objects, identity["push_url"])
    refs = remote_refs(objects, identity, transport=settings)
    if refspecs is None:
        refspecs = identity["configured_push"]
        if not refspecs:
            branch = git(repo, "symbolic-ref", "--quiet", "--short", "HEAD")
            mode = (config(repo, "push.default") or ["simple"])[-1]
            require(mode in ("simple", "current", "upstream"), "default_push_unsupported")
            dest = "refs/heads/" + branch
            if mode in ("simple", "upstream"):
                upstream = config(repo, f"branch.{branch}.merge")
                require(len(upstream) == 1, "default_destination_unknown")
                dest = upstream[0]
                require(mode != "simple" or dest == "refs/heads/" + branch, "simple_destination_mismatch")
            refspecs = ["HEAD:" + dest]
    require(bool(refspecs), "empty_update_set")
    result = []
    head = git(objects, "rev-parse", "HEAD^{commit}")
    for spec in refspecs:
        require(isinstance(spec, str) and spec and not any(c in spec for c in "+*^~?[]\\ \n\t"), "unsupported_refspec")
        require(spec.count(":") <= 1, "invalid_refspec")
        src, dest = spec.split(":") if ":" in spec else (spec, spec)
        require(src and dest, "deletion_unsupported")
        if ":" not in spec and spec == "HEAD":
            # Git uses the attached source branch here; it does not create a
            # branch literally named HEAD. Detached shorthand is ambiguous.
            dest = git(objects, "symbolic-ref", "--quiet", "HEAD")
        if not dest.startswith("refs/"):
            require("refs/tags/" + dest not in refs, "ambiguous_destination")
            dest = "refs/heads/" + dest
        require(dest.startswith(("refs/heads/", "refs/tags/")), "unsupported_destination")
        git(objects, "check-ref-format", dest)
        require(dest not in [u["destination"] for u in result], "duplicate_destination")
        sha = git(objects, "rev-parse", "--verify", src + "^{commit}")
        require(sha == head, "only_checked_export_supported")
        result.append(dict(destination=dest, destination_kind="direct", new=sha, old=None))
    refs = remote_refs(objects, identity, destinations=[u["destination"] for u in result], transport=settings)
    for update in result:
        update["old"] = refs.get(update["destination"])
    return identity, result


def prepare(repo, export, *, remote, refspecs, gate, release):
    """Freeze after consumer-install and construction on the existing public tip."""
    require(gate.get("verdict") == "pass", "install_receipt_required")
    export = Path(export)
    require(not git(export, "status", "--porcelain", "--untracked-files=no"), "checked_export_worktree_changed")
    source = source_identity(repo)
    identity = remote_identity(repo, remote)
    settings = reference_settings(export, identity["push_url"])
    observed_identity, updates = resolve_updates(repo, export, remote, refspecs, transport=settings)
    require(identity == observed_identity, "remote_configuration_changed")
    export_head = git(export, "rev-parse", "HEAD^{commit}")
    tree = git(export, "rev-parse", export_head + "^{tree}")
    candidate = export_head
    main = next((u for u in updates if u["destination"] == MAIN), None)
    if main and main["old"]:
        git(export, "fetch", "--no-tags", identity["push_url"], main["old"], transport=settings)
        # Use the already scrubbed export commit's author/committer/message.
        # Hash the exact tree onto the verified existing public tip, then freeze.
        raw = git(export, "cat-file", "commit", export_head)
        headers, message = raw.split("\n\n", 1)
        kept = [line for line in headers.splitlines() if line.startswith(("author ", "committer "))]
        content = f"tree {tree}\nparent {main['old']}\n" + "\n".join(kept) + "\n\n" + message + "\n"
        candidate = git(export, "hash-object", "-t", "commit", "-w", "--stdin", input=content)
    for update in updates:
        update["new"] = candidate
        if update["destination"].startswith("refs/tags/"):
            require(update["old"] in (None, candidate), "existing_version_tag_requires_bump")
    git(export, "update-ref", "refs/workbay/prepared", candidate)
    git(export, "update-ref", "refs/workbay/checked-export", export_head)
    require(source_identity(repo) == source, "source_changed_during_preparation")
    require(remote_identity(repo, remote) == identity, "remote_configuration_changed")
    subject = dict(
        schema=SCOPE,
        source=source,
        remote=identity,
        transport=settings,
        export_commit=export_head,
        prepared_commit=candidate,
        prepared_tree=tree,
        updates=updates,
        gate=gate,
        release=json.loads(encoded(release)),
    )
    plan_id = digest(subject)
    root = store(repo)
    if root == authority(repo) / "prepared":
        private_dir(authority(repo))
    private_dir(root)
    target = root / plan_id
    # The object database is retained independently of mutable HEAD/worktree.
    # Publishing uses only subject['prepared_commit'].
    if target.exists():
        require(private_bytes(target / "plan.json") == encoded(subject), "plan_conflict")
    else:
        stage = Path(tempfile.mkdtemp(prefix=".prepare-", dir=root))
        try:
            shutil.copytree(export / ".git", stage / "objects.git")
            # The retained repository has no hooks, remotes, implicit ref updates,
            # object alternates, or worker-selected transport config.
            (stage / "objects.git" / "config").write_text("[core]\n\tbare = true\n")
            (stage / "objects.git" / "HEAD").write_text("ref: refs/workbay/prepared\n")
            shutil.rmtree(stage / "objects.git" / "hooks", ignore_errors=True)
            require(
                not (stage / "objects.git" / "objects" / "info" / "alternates").exists(),
                "alternate_objects_unsupported",
            )
            append(stage / "plan.json", encoded(subject))
            try:
                os.rename(stage, target)
            except FileExistsError:
                require(private_bytes(target / "plan.json") == encoded(subject), "plan_conflict")
            fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    return plan_id, subject


def load(repo, plan_id):
    require(isinstance(plan_id, str) and re.fullmatch(r"[0-9a-f]{64}", plan_id), "invalid_plan_id")
    root = store(repo) / plan_id
    subject = json.loads(private_bytes(root / "plan.json"))
    require(digest(subject) == plan_id and subject["schema"] == SCOPE, "plan_digest_mismatch")
    require(
        all(u.get("destination_kind") == "direct" for u in subject["updates"]),
        "destination_binding_required_prepare_again",
    )
    if "reference_provider" in subject.get("transport", {}):
        github_absent_tags(subject["remote"]["push_url"], subject["transport"], [])
    # Local identities are checked after the provider's network wait too.
    require(subject["source"] == source_identity(repo), "source_changed_prepare_again")
    require(remote_identity(repo, subject["remote"]["requested"]) == subject["remote"], "remote_configuration_changed")
    objects = root / "objects.git"
    require(
        (objects / "config").read_text() == "[core]\n\tbare = true\n"
        and not (objects / "hooks").exists()
        and not (objects / "objects" / "info" / "alternates").exists(),
        "retained_transport_tampered",
    )
    require(
        git(objects, "rev-parse", subject["prepared_commit"] + "^{tree}") == subject["prepared_tree"],
        "prepared_tree_mismatch",
    )
    require(subject["gate"]["verdict"] == "pass", "install_receipt_required")
    return subject, objects


def receiver_key(repo):
    # Critical prior art 23752: only read existing preprovisioned identity.
    root = authority(repo)
    try:
        private_stat(root, directory=True)
        key = private_bytes(root / "receiver.key")
    except FileNotFoundError as exc:
        raise PublicationHeld("preprovisioned_receiver_identity_required") from exc
    require(len(key) == 32, "receiver_key_length")
    return key


def validate_approval(repo, plan_id):
    key = receiver_key(repo)
    receipt = json.loads(private_bytes(authority(repo) / "approvals" / (plan_id + ".json")))
    signature = receipt.pop("hmac_sha256")
    require(
        receipt.get("schema") == SCOPE
        and receipt.get("plan_id") == plan_id
        and receipt.get("issuer") == hashlib.sha256(key).hexdigest()
        and receipt.get("operator_uid") == os.getuid()
        and isinstance(receipt.get("operator"), str)
        and bool(receipt["operator"]),
        "approval_identity_mismatch",
    )
    require(
        hmac.compare_digest(signature, hmac.new(key, encoded(receipt), hashlib.sha256).hexdigest()),
        "approval_authentication_failed",
    )
    return receipt


def effect(repo, plan_id, *, remote=None, refspecs=None):
    """Shared effect API. No boolean, caller receipt, executor, or extra update
    can authorize a main push. Resolved plans always consume this boundary.
    """
    subject, objects = load(repo, plan_id)
    if remote is not None:
        require(remote_identity(repo, remote) == subject["remote"], "requested_tuple_changed")
    if refspecs is not None:
        identity, updates = resolve_updates(repo, objects, remote or subject["remote"]["requested"], refspecs,
                                            transport=subject.get("transport", {}))
        # The observed old values can now equal the approved new values on an
        # identical retry. They never replace the plan's bound preimage below.
        requested = [(u["destination"], u["new"]) for u in updates]
        frozen = [(u["destination"], u["new"]) for u in subject["updates"]]
        require(identity == subject["remote"] and requested == frozen, "requested_tuple_changed")
    if any(u["destination"] == MAIN for u in subject["updates"]):
        validate_approval(repo, plan_id)
    current = remote_refs(objects, subject["remote"], destinations=[u["destination"] for u in subject["updates"]],
                          transport=subject.get("transport", {}))
    # CON-11: repeat source/endpoint/custody validation after the network wait.
    load(repo, plan_id)
    if all(current.get(u["destination"]) == u["new"] for u in subject["updates"]):
        return dict(status="already_applied", plan_id=plan_id, updates=subject["updates"])
    for u in subject["updates"]:
        require(current.get(u["destination"]) == u["old"], "remote_tip_changed_prepare_again")
        if u["destination"].startswith("refs/heads/") and u["old"]:
            git(objects, "merge-base", "--is-ancestor", u["old"], u["new"])
        elif u["destination"].startswith("refs/tags/"):
            require(u["old"] in (None, u["new"]), "existing_version_tag_requires_bump")
    leases = [f"--force-with-lease={u['destination']}:{u['old'] or ''}" for u in subject["updates"]]
    specs = [f"{u['new']}:{u['destination']}" for u in subject["updates"]]
    load(repo, plan_id)
    # FF proof above + exact expected-old leases + atomic update set. Never raw
    # --force, HEAD, or a second export after approval. A racing writer loses CAS.
    git(
        objects,
        "-c",
        "push.followTags=false",
        "push",
        "--atomic",
        *leases,
        subject["remote"]["push_url"],
        *specs,
        transport=subject.get("transport", {}),
    )
    observed = remote_refs(objects, subject["remote"], destinations=[u["destination"] for u in subject["updates"]],
                           transport=subject.get("transport", {}))
    require(all(observed.get(u["destination"]) == u["new"] for u in subject["updates"]), "publication_readback_unknown")
    return dict(status="published", plan_id=plan_id, updates=subject["updates"])


def operator(repo, operation, plan_id=None):
    """Only explicit operator setup/approval can create key/approval material.
    Like native recovery, a real operator terminal is the trust assumption, not
    an unforgeable human detector when an unrestricted same-UID agent has a PTY.
    """
    require(sys.stdin.isatty() and os.isatty(sys.stdin.fileno()), "operator_tty_required")
    if operation == "setup":
        print("Set up a dedicated remote-main publication issuer. Type setup remote-main:", file=sys.stderr, flush=True)
        require(sys.stdin.readline().strip() == "setup remote-main", "operator_confirmation_mismatch")
        root = authority(repo)
        private_dir(root)
        if not (root / "receiver.key").exists():
            append(root / "receiver.key", os.urandom(32))
        receiver_key(repo)
        return dict(status="issuer_ready")
    require(operation == "approve", "unknown_operator_operation")
    subject, objects = load(repo, plan_id)
    key = receiver_key(repo)
    current = remote_refs(objects, subject["remote"], destinations=[u["destination"] for u in subject["updates"]],
                          transport=subject.get("transport", {}))
    require(
        all(current.get(u["destination"]) == u["old"] for u in subject["updates"]), "remote_tip_changed_prepare_again"
    )
    print(json.dumps(dict(plan_id=plan_id, subject=subject), indent=2), file=sys.stderr)
    print(
        "Approve this exact repository/remote/ref/commit/update set. Enter plan SHA-256:", file=sys.stderr, flush=True
    )
    require(sys.stdin.readline().strip() == plan_id, "operator_confirmation_mismatch")
    # Revalidate after the human wait; CAS still protects the later effect.
    load(repo, plan_id)
    receipt = dict(
        schema=SCOPE,
        plan_id=plan_id,
        issuer=hashlib.sha256(key).hexdigest(),
        operator_uid=os.getuid(),
        operator=pwd.getpwuid(os.getuid()).pw_name,
        approved_at=datetime.now(timezone.utc).isoformat(),
    )
    receipt["hmac_sha256"] = hmac.new(key, encoded(receipt), hashlib.sha256).hexdigest()
    path = authority(repo) / "approvals" / (plan_id + ".json")
    if path.exists():
        validate_approval(repo, plan_id)
    else:
        append(path, encoded(receipt))
    return dict(status="approved", plan_id=plan_id)
