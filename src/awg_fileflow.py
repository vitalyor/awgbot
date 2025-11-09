# src/awg_fileflow.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
# read_interface_obf_params() now supports runtime fallback if config file lacks obfuscation keys.
import ipaddress
import os
import base64

# ─────────────────────────────────────────────────────────────────────────────
# Imports that work both when this file is imported as a top-level module
# (e.g. "import awg_fileflow") and when imported as part of the package
# (e.g. "from src.awg_fileflow import ...").
# ─────────────────────────────────────────────────────────────────────────────
try:
    # when used as a package: `from src.awg_fileflow import ...`
    from .core.docker import run_cmd  # type: ignore
    from .util import AWG_CONNECT_HOST  # type: ignore
except Exception:
    # when used as a top-level module: `import awg_fileflow`
    from core.docker import run_cmd  # type: ignore
    from util import AWG_CONNECT_HOST  # type: ignore

# ─────────────────────────────────────────────────────────────────────────────
# Paths & constants (compatible with your stack)
# ─────────────────────────────────────────────────────────────────────────────
AWG_CONTAINER = os.getenv("AWG_CONTAINER", "amnezia-awg")
WG_IFACE = os.getenv("AWG_IFACE", "wg0")
CONF_DIR = "/opt/amnezia/awg"
CONF_PATH = f"{CONF_DIR}/{WG_IFACE}.conf"
PSK_PATH = f"{CONF_DIR}/wireguard_psk.key"
SERVER_PUB = f"{CONF_DIR}/wireguard_server_public_key.key"
LOCK_PATH = f"{CONF_DIR}/.conf.lock"

# Default (fallback) obfuscation values (used if not present in file).
# If you want to force specific values, set these ENV vars in the AWG container.
ENV_OBF_DEFAULTS = {
    "Jc": os.getenv("AWG_JC"),
    "Jmin": os.getenv("AWG_JMIN"),
    "Jmax": os.getenv("AWG_JMAX"),
    "S1": os.getenv("AWG_S1"),
    "S2": os.getenv("AWG_S2"),
    "H1": os.getenv("AWG_H1"),
    "H2": os.getenv("AWG_H2"),
    "H3": os.getenv("AWG_H3"),
    "H4": os.getenv("AWG_H4"),
}

# ─────────────────────────────────────────────────────────────────────────────
# Low-level shell helpers
# ─────────────────────────────────────────────────────────────────────────────


def _sh(cmd: str, timeout: int = 15) -> Tuple[int, str, str]:
    """Run a command inside the AWG container."""
    full = f"docker exec {AWG_CONTAINER} sh -lc {repr(cmd)}"
    rc, out, err = run_cmd(full, timeout=timeout)
    return rc, (out or ""), (err or "")


def _require_ok(cmd: str, timeout: int = 15) -> str:
    rc, out, err = _sh(cmd, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"cmd failed: {cmd}\nrc={rc}\n{err}")
    return out


def _secure_conf_perms() -> None:
    """Make sure config & keys have strict perms (mirrors app behavior)."""
    _require_ok(
        f"chown root:root {CONF_PATH} {PSK_PATH} {SERVER_PUB} 2>/dev/null || true"
    )
    _require_ok(f"chmod 600 {CONF_PATH} {PSK_PATH} {SERVER_PUB} 2>/dev/null || true")
    _require_ok(f"chmod 700 {CONF_DIR} 2>/dev/null || true")


def _with_lock(cmd: str) -> str:
    """
    Serialize edits to {CONF_PATH}. Uses flock if present; otherwise just runs.
    """
    # Prefer util-linux flock if available
    rc, _, _ = _sh("command -v flock >/dev/null 2>&1")
    if rc == 0:
        return _require_ok(f"flock -x {LOCK_PATH} -c {repr(cmd)}")
    # Fallback: best effort without lock
    return _require_ok(cmd)


# ─────────────────────────────────────────────────────────────────────────────
# WG inspection
# ─────────────────────────────────────────────────────────────────────────────


def wg_dump() -> List[List[str]]:
    """
    wg show <iface> dump → list of rows split by tabs.
      Row 0 (interface): [0]priv, [1]pub, [2]listen_port, [3]fwmark, ...
      Peer rows (NR>1):  [0]pub, [1]psk, [2]endpoint, [3]allowed_ips, [4]handshake, [5]rx, [6]tx, [7]keepalive
    """
    out = _require_ok(f"wg show {WG_IFACE} dump || true")
    rows: List[List[str]] = []
    for line in (out or "").splitlines():
        parts = line.strip().split("\t")
        if parts:
            rows.append(parts)
    return rows


def listen_port_from_dump(rows: List[List[str]]) -> Optional[int]:
    if not rows:
        return None
    try:
        return int(rows[0][2])
    except Exception:
        return None


def server_subnet() -> ipaddress.IPv4Network:
    """
    Detect WG subnet for wg0, first via ip addr, then via Address= in config.
    """
    out = _require_ok("ip -brief addr || ip a || true")
    cidr = None
    for line in out.splitlines():
        if line.startswith(WG_IFACE) and "/" in line:
            toks = line.split()
            for tok in toks:
                if "/" in tok and tok.count(".") == 3:
                    cidr = tok
                    break
    if not cidr:
        cfg = _require_ok(f"cat {CONF_PATH}")
        for line in cfg.splitlines():
            s = line.strip()
            if s.lower().startswith("address"):
                _, val = s.split("=", 1)
                cidr = val.strip()
                break
    if not cidr:
        raise RuntimeError("Cannot detect WG subnet")
    return ipaddress.ip_network(cidr, strict=False)


def used_client_ips(rows: List[List[str]]) -> List[ipaddress.IPv4Address]:
    used: List[ipaddress.IPv4Address] = []
    for parts in rows[1:]:
        if len(parts) < 4:
            continue
        allowed = parts[3].strip()
        if not allowed or "/" not in allowed:
            continue
        try:
            ip = ipaddress.ip_interface(allowed).ip
            used.append(ip)
        except Exception:
            pass
    return used


def alloc_free_ip(
    subnet: ipaddress.IPv4Network, used: List[ipaddress.IPv4Address]
) -> ipaddress.IPv4Address:
    used_set = set(used)
    for ip in subnet.hosts():
        # skip .0 and .1
        if ip.packed[-1] < 2:
            continue
        if ip in used_set:
            continue
        return ip
    raise RuntimeError("No free IPs left in WG subnet")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def server_public_key() -> str:
    return _require_ok(f"cat {SERVER_PUB}").strip()


def shared_psk() -> str:
    return _require_ok(f"cat {PSK_PATH}").strip()


def get_dns_ip() -> str:
    """
    Return amnezia-dns IP if available, otherwise 1.1.1.1
    """
    try:
        rc, out, _ = _sh("getent hosts amnezia-dns")
        if rc == 0 and out:
            first_line = out.strip().splitlines()[0]
            ip = first_line.split()[0].strip()
            if ip.count(".") == 3:
                return ip
    except Exception:
        pass
    try:
        rc, out, _ = run_cmd(
            "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' amnezia-dns"
        )
        if rc == 0:
            ip = (out or "").strip()
            if ip and ip.count(".") == 3:
                return ip
    except Exception:
        pass
    return "1.1.1.1"


def read_interface_obf_params() -> Dict[str, int]:
    """
    Read Jc/Jmin/Jmax/S1/S2/H1..H4 from [Interface] in wg0.conf (if any).
    Always merge with runtime `wg show <iface>` output for missing keys.
    """
    want_keys = ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4")
    merged: Dict[str, int] = {}
    # 1) From file (case-insensitive)
    try:
        cfg = _require_ok(f"cat {CONF_PATH}")
        in_iface = False
        for line in cfg.splitlines():
            s = line.strip()
            if not s:
                continue
            if s.lower() == "[interface]":
                in_iface = True
                continue
            if s.lower() == "[peer]":
                break
            if in_iface and "=" in s and not s.startswith("#"):
                k, v = [x.strip() for x in s.split("=", 1)]
                kl = k.lower()
                for want in want_keys:
                    if kl == want.lower():
                        try:
                            merged[want] = int(v)
                        except Exception:
                            pass
                        break
    except Exception:
        pass

    # 2) For missing keys, try runtime `wg show <iface>`
    missing_keys = [k for k in want_keys if k not in merged]
    if missing_keys:
        try:
            out = _require_ok(f"wg show {WG_IFACE} || true")
            for line in out.splitlines():
                s = line.strip()
                if ":" not in s:
                    continue
                k, v = s.split(":", 1)
                kl = k.strip().lower()
                val = v.strip()
                for want in missing_keys:
                    if kl == want.lower():
                        try:
                            merged[want] = int(val)
                        except Exception:
                            pass
                        break
        except Exception:
            pass
    return merged


def _ensure_interface_has_obf(conf_text: str) -> str:
    lines = conf_text.splitlines()
    out: List[str] = []
    in_iface = False
    want = ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4")
    seen: Dict[str, bool] = {k: False for k in want}

    def append_missing():
        for key in want:
            if not seen[key] and ENV_OBF_DEFAULTS.get(key) is not None:
                out.append(f"{key} = {int(ENV_OBF_DEFAULTS[key])}")

    for line in lines:
        s = line.strip()
        if s == "[Interface]":
            in_iface = True
            out.append(line)
            continue
        if s == "[Peer]":
            if in_iface:
                append_missing()
            in_iface = False
            out.append(line)
            continue

        if in_iface and "=" in s and not s.startswith("#"):
            k = s.split("=", 1)[0].strip()
            kl = k.lower()
            for key in want:
                if kl == key.lower():
                    seen[key] = True
                    break

        out.append(line)

    if in_iface:
        append_missing()

    return "\n".join(out) + ("\n" if out and out[-1] != "" else "")


# ─────────────────────────────────────────────────────────────────────────────
# Config text helpers
# ─────────────────────────────────────────────────────────────────────────────


def _read_conf() -> str:
    return _require_ok(f"cat {CONF_PATH}")


def _write_conf_text_atomic(conf_text: str) -> None:
    # 1) Basic sanity on the text we intend to write
    if "[Interface]" not in conf_text or len(conf_text.strip()) < 40:
        raise RuntimeError("Refusing to write suspicious conf_text (pre-check)")

    # ensure trailing newline to avoid partial last line truncation issues
    if not conf_text.endswith("\n"):
        conf_text = conf_text + "\n"

    # write flow: /tmp/<iface>.conf.b64 -> /tmp/<iface>.conf.tmp -> CONF_PATH
    encoded = base64.b64encode(conf_text.encode("utf-8")).decode("ascii")
    tmp_b64   = f"/tmp/{WG_IFACE}.conf.b64"
    tmp_plain = f"/tmp/{WG_IFACE}.conf.tmp"

    # create temp payload and decode to a plain file
    _require_ok(f"set -e; umask 077; cat > {tmp_b64} <<'B64'\n{encoded}\nB64")
    _require_ok(f"set -e; umask 077; base64 -d {tmp_b64} > {tmp_plain}")

    # 2) Move atomically under a file lock if flock exists
    rc, _, _ = _sh("command -v flock >/dev/null 2>&1")
    if rc == 0:
        _require_ok(f"flock -x {LOCK_PATH} -c 'mv -f {tmp_plain} {CONF_PATH}; rm -f {tmp_b64}'")
    else:
        _require_ok(f"mv -f {tmp_plain} {CONF_PATH}; rm -f {tmp_b64}")

    # 3) permissions and sync to be extra safe
    _secure_conf_perms()
    _sh("sync >/dev/null 2>&1 || true")
    _sh("sleep 0.1 || true")

    # 4) Re-validate by readback with diagnostics
    rc, written, err = _sh(f"cat {CONF_PATH} 2>/dev/null || true")
    if rc != 0 or not written:
        # print some diag to help debugging
        sz = _require_ok(f"stat -c %s {CONF_PATH} 2>/dev/null || echo 0").strip()
        sha = _require_ok(f"sha256sum {CONF_PATH} 2>/dev/null | awk '{{print $1}}' || echo 0").strip()
        raise RuntimeError(f"Refusing to write suspicious conf_text (readback fail): rc={rc}, size={sz}, sha256={sha}, err={err}")

    # normalize and validate content
    w = written.strip()
    if "[Interface]" not in w or len(w) < 40:
        # include a short head for debugging
        head = "\n".join(w.splitlines()[:20])
        raise RuntimeError(f"Refusing to write suspicious conf_text (post-write): HEAD:\n{head}")

    # success


def _append_peer_block_in_text(
    conf_text: str, pubkey: str, psk_val: str, client_ip_cidr: str
) -> str:
    block = (
        "\n[Peer]\n"
        f"PublicKey = {pubkey}\n"
        f"PresharedKey = {psk_val}\n"
        f"AllowedIPs = {client_ip_cidr}\n"
        "PersistentKeepalive = 25\n"
    )
    return conf_text.rstrip() + block


def _drop_peer_block_in_text(conf_text: str, pubkey: str) -> str:
    out_lines: List[str] = []
    lines = conf_text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip() == "[Peer]":
            j = i + 1
            buf = [line]
            keep = True
            while j < len(lines) and lines[j].strip() != "[Peer]":
                buf.append(lines[j])
                if lines[j].strip().startswith("PublicKey"):
                    _, val = lines[j].split("=", 1)
                    if val.strip() == pubkey:
                        keep = False
                j += 1
            if keep:
                out_lines.extend(buf)
            i = j
            continue
        else:
            out_lines.append(line)
            i += 1
    return "\n".join(out_lines) + ("\n" if out_lines and out_lines[-1] != "" else "")


def _strip_to_interface_only(conf_text: str) -> str:
    """
    Return only [Interface] section (everything before the first [Peer]).
    """
    lines = conf_text.splitlines()
    out: List[str] = []
    for line in lines:
        if line.strip() == "[Peer]":
            break
        out.append(line)
    if out and out[-1] != "":
        out.append("")
    return "\n".join(out)


def _has_peer_in_text(conf_text: str, pubkey: str) -> bool:
    lines = conf_text.splitlines()
    i = 0
    while i < len(lines):
        if lines[i].strip() == "[Peer]":
            j = i + 1
            while j < len(lines) and lines[j].strip() != "[Peer]":
                s = lines[j].strip()
                if s.startswith("PublicKey"):
                    _, val = s.split("=", 1)
                    if val.strip() == pubkey:
                        return True
                j += 1
            i = j
        else:
            i += 1
    return False


def _ensure_slash32(ip_or_cidr: str) -> str:
    ip_or_cidr = ip_or_cidr.strip()
    if "/" not in ip_or_cidr:
        ip_or_cidr = f"{ip_or_cidr}/32"
    _ = ipaddress.ip_interface(ip_or_cidr)  # validate
    return ip_or_cidr


def apply_syncconf() -> None:
    """
    Apply config the same way the official app does:
      wg-quick strip <CONF_PATH> > /tmp/<iface>.stripped
      wg syncconf <iface> /tmp/<iface>.stripped
    """
    _secure_conf_perms()
    _require_ok(f'wg-quick strip "{CONF_PATH}" > /tmp/{WG_IFACE}.stripped')
    # quick sanity: the stripped file should not be empty and must contain [Interface]
    rc, stripped, _ = _sh(f"cat /tmp/{WG_IFACE}.stripped 2>/dev/null || true")
    if rc != 0 or not stripped or "[Interface]" not in stripped:
        head = "\n".join((stripped or "").splitlines()[:20])
        raise RuntimeError(f"wg-quick strip produced empty/bad output; HEAD:\n{head}")
    _require_ok(f"wg syncconf {WG_IFACE} /tmp/{WG_IFACE}.stripped")


def _upsert_peer_block_in_file(cli_pub: str, client_ip_cidr: str) -> None:
    """
    Remove any [Peer] block with the same pubkey and append a fresh one,
    preserving/ensuring obfuscation params in [Interface], then apply syncconf.
    """
    conf = _read_conf()
    conf = _ensure_interface_has_obf(conf)
    client_ip_cidr = _ensure_slash32(client_ip_cidr)
    conf = _drop_peer_block_in_text(conf, cli_pub)
    conf2 = _append_peer_block_in_text(conf, cli_pub, shared_psk(), client_ip_cidr)
    _write_conf_text_atomic(conf2)
    apply_syncconf()


# ─────────────────────────────────────────────────────────────────────────────
# Public API for the debug workflow
# ─────────────────────────────────────────────────────────────────────────────


def list_peers() -> List[Dict[str, Any]]:
    rows = wg_dump()
    out: List[Dict[str, Any]] = []
    for parts in rows[1:]:
        if len(parts) < 4:
            continue
        out.append(
            {
                "pubkey": parts[0],
                "preshared_key": parts[1] if len(parts) > 1 else "",
                "endpoint": parts[2] if len(parts) > 2 else "",
                "allowed_ips": parts[3] if len(parts) > 3 else "",
            }
        )
    return out


def facts() -> Dict[str, Any]:
    """
    Return facts for quick sanity checks.
    """
    rows = wg_dump()
    return {
        "server_pub": server_public_key(),
        "listen_port": listen_port_from_dump(rows),
        "obf": read_interface_obf_params(),
    }


def alloc_ip_from_runtime() -> str:
    """
    Return a free /32 from current runtime dump.
    """
    rows = wg_dump()
    subnet = server_subnet()
    used = used_client_ips(rows)
    ip = alloc_free_ip(subnet, used)
    return f"{ip}/32"


def ensure_interface_obf() -> None:
    """
    Ensure [Interface] section has all obfuscation keys from config and runtime.
    If missing, append them, write the config, but do not apply.
    """
    conf = _read_conf()
    want_keys = ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4")
    # 1) Parse current obf params from file section [Interface] (case-insensitive)
    have = {}
    in_iface = False
    lines = conf.splitlines()
    for line in lines:
        s = line.strip()
        if s.lower() == "[interface]":
            in_iface = True
            continue
        if s.lower() == "[peer]":
            if in_iface:
                break
        if in_iface and "=" in s and not s.startswith("#"):
            k, v = [x.strip() for x in s.split("=", 1)]
            kl = k.lower()
            for want in want_keys:
                if kl == want.lower():
                    try:
                        have[want] = int(v)
                    except Exception:
                        pass
                    break
    # 2) Merge with runtime
    merged = read_interface_obf_params()
    # 3) Find missing keys, prepare additions
    missing = [k for k in want_keys if k not in have and k in merged]
    if not missing:
        return

    # Helper: find where to insert lines into [Interface]
    def insert_into_interface_section(conf_lines: List[str], additions: List[str]) -> List[str]:
        out = []
        in_iface = False
        inserted = False
        for idx, line in enumerate(conf_lines):
            s = line.strip()
            if s.lower() == "[interface]":
                in_iface = True
                out.append(line)
                continue
            if in_iface and s.lower() == "[peer]":
                # Insert additions just before first [Peer]
                out.extend(additions)
                inserted = True
                out.append(line)
                in_iface = False
                continue
            out.append(line)
        if in_iface and not inserted:
            # No [Peer] found, append at end of [Interface]
            out.extend(additions)
        return out

    additions = [f"{k} = {merged[k]}" for k in missing]
    new_lines = insert_into_interface_section(lines, additions)
    new_conf = "\n".join(new_lines)
    if not new_conf.endswith("\n"):
        new_conf += "\n"
    _write_conf_text_atomic(new_conf)


def add_peer_with_pubkey(cli_pub: str, client_ip: str) -> None:
    """
    Add a peer using an already-known public key.
    """
    client_ip = _ensure_slash32(client_ip)
    _upsert_peer_block_in_file(cli_pub, client_ip)


def remove_peer(pubkey: str) -> None:
    conf = _read_conf()
    conf2 = _drop_peer_block_in_text(conf, pubkey)
    _write_conf_text_atomic(conf2)
    apply_syncconf()


def clean_all_peers() -> None:
    """
    Keep only [Interface] in the file, preserving/adding obfuscation, then apply.
    """
    conf = _read_conf()
    conf2 = _strip_to_interface_only(conf)
    conf2 = _ensure_interface_has_obf(conf2)
    _write_conf_text_atomic(conf2)
    apply_syncconf()


def make_client_conf_text(cli_priv: str, assigned_ip: str) -> str:
    """
    Build a client config (AWG compatible) using server params.
    """
    assigned_ip = _ensure_slash32(assigned_ip)
    srv_pub = server_public_key()
    port = listen_port_from_dump(wg_dump()) or 0
    dns_ip = get_dns_ip()
    params = read_interface_obf_params()
    endpoint_host = AWG_CONNECT_HOST  # keep deterministic; no autodetect here
    endpoint = f"{endpoint_host}:{port}" if endpoint_host else f":{port}"

    lines = [
        "[Interface]",
        f"Address = {assigned_ip}",
        f"DNS = {dns_ip}, 1.0.0.1",
        f"PrivateKey = {cli_priv}",
    ]
    for key in ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4"):
        if key in params:
            lines.append(f"{key} = {params[key]}")

    lines += [
        "",
        "[Peer]",
        f"PublicKey = {srv_pub}",
        f"PresharedKey = {shared_psk()}",
        "AllowedIPs = 0.0.0.0/0, ::/0",
        f"Endpoint = {endpoint}",
        "PersistentKeepalive = 25",
        "",
    ]
    return "\n".join(lines)


def create_peer_with_generated_keys(ip_cidr: str) -> Dict[str, Any]:
    """
    Full flow (mirrors the good shell script):
      - generate client keys (wg genkey → pubkey)
      - ensure [Interface] has obfuscation values (preserve or fill from ENV)
      - remove any old [Peer] blocks with the same pubkey
      - append fresh [Peer] with shared PSK
      - apply via wg-quick strip + wg syncconf
      - build client .conf for import (with J*/H* from [Interface])
    """
    ip_cidr = _ensure_slash32(ip_cidr)
    # ensure obf present before appending
    ensure_interface_obf()

    cli_priv = _require_ok("wg genkey").strip()
    cli_pub = _require_ok(f'printf %s "{cli_priv}" | wg pubkey').strip()

    _upsert_peer_block_in_file(cli_pub, ip_cidr)

    srv_pub = server_public_key()
    rows = wg_dump()
    port = listen_port_from_dump(rows) or 0
    dns_ip = get_dns_ip()
    params = read_interface_obf_params()
    endpoint_host = AWG_CONNECT_HOST
    endpoint = f"{endpoint_host}:{port}" if endpoint_host else f":{port}"

    # client config
    lines = [
        "[Interface]",
        f"Address = {ip_cidr}",
        f"DNS = {dns_ip}, 1.0.0.1",
        f"PrivateKey = {cli_priv}",
    ]
    for key in ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4"):
        if key in params:
            lines.append(f"{key} = {params[key]}")
    lines += [
        "",
        "[Peer]",
        f"PublicKey = {srv_pub}",
        f"PresharedKey = {shared_psk()}",
        "AllowedIPs = 0.0.0.0/0, ::/0",
        f"Endpoint = {endpoint}",
        "PersistentKeepalive = 25",
        "",
    ]
    client_conf = "\n".join(lines)

    return {
        "client_private": cli_priv,
        "client_public": cli_pub,
        "assigned_ip": ip_cidr,
        "server_public": srv_pub,
        "listen_port": port,
        "endpoint": endpoint,
        "client_conf": client_conf,
    }
