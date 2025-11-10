# src/awg.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

# read_interface_obf_params() now supports runtime fallback if config file lacks obfuscation keys.
import ipaddress
import os
import base64
import re
import json
from datetime import datetime, timezone

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

# Allow explicit DNS override for amnezia-dns detection
AWG_DNS_IP_OVERRIDE = os.getenv("AWG_DNS_IP", "").strip()

# Prefer explicit config path from env, otherwise fall back to default layout
CONF_PATH_ENV = os.getenv("AWG_CONFIG_PATH")
DEFAULT_IFACE = "wg0"

# Try to detect interface name from runtime first


def _detect_iface_from_runtime() -> Optional[str]:
    # 1) wg show interfaces
    try:
        rc, out, _ = _sh("wg show interfaces 2>/dev/null || true", timeout=5)
        cand = (out or "").strip().split()
        if cand:
            # If multiple, take the first (primary)
            return cand[0]
    except Exception:
        pass
    # 2) ip -o link show type wireguard
    try:
        rc, out, _ = _sh(
            "ip -o link show type wireguard 2>/dev/null || true", timeout=5
        )
        for line in (out or "").splitlines():
            # format: "7: wg0: <...>"
            parts = line.split(":", 2)
            if len(parts) >= 2:
                name = parts[1].strip()
                if name:
                    return name
    except Exception:
        pass
    return None


# Determine interface name (prefer runtime detection)
runtime_iface = _detect_iface_from_runtime()
_env_iface = os.getenv("AWG_IFACE", "").strip() or None
if runtime_iface:
    _iface_detected = runtime_iface
elif _env_iface and _env_iface.lower() != "none":
    _iface_detected = _env_iface
elif CONF_PATH_ENV:
    _base = os.path.basename(CONF_PATH_ENV)
    _iface_detected = _base.split(".")[0] if _base else DEFAULT_IFACE
else:
    _iface_detected = DEFAULT_IFACE

WG_IFACE = _iface_detected

if CONF_PATH_ENV:
    CONF_PATH = CONF_PATH_ENV
    CONF_DIR = os.path.dirname(CONF_PATH) or "/opt/amnezia/awg"
else:
    CONF_DIR = "/opt/amnezia/awg"
    CONF_PATH = f"{CONF_DIR}/{WG_IFACE}.conf"

PSK_PATH = f"{CONF_DIR}/wireguard_psk.key"
SERVER_PUB = f"{CONF_DIR}/wireguard_server_public_key.key"
LOCK_PATH = f"{CONF_DIR}/.conf.lock"
CLIENTS_TABLE = f"{CONF_DIR}/clientsTable"

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
    # Use double quotes around the whole command; escape internal double quotes and backslashes
    safe = cmd.replace("\\", "\\\\").replace('"', '\\"')
    full = f'docker exec {AWG_CONTAINER} sh -lc "{safe}"'
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


# Filesystem/transfer helpers


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _docker_cp_local_to_container(local_path: str, container_dst: str) -> None:
    """Copy a local file into the AWG container using `docker cp`.
    Avoids here-doc/base64 quirks and BusyBox limitations."""
    # container_dst must be an absolute path inside the container
    if not container_dst.startswith("/"):
        raise ValueError("container_dst must be absolute path")
    rc, out, err = run_cmd(
        f"docker cp {local_path} {AWG_CONTAINER}:{container_dst}", timeout=30
    )
    if rc != 0:
        raise RuntimeError(f"docker cp failed: rc={rc}\n{err}")

def _write_text_into_container(container_path: str, text: str) -> None:
    """Write text content to a file inside the AWG container using docker cp."""
    tmp_dir = "/app/data/tmp"
    _ensure_dir(tmp_dir)
    local_tmp = os.path.join(tmp_dir, os.path.basename(container_path) + ".new")
    with open(local_tmp, "w", encoding="utf-8") as f:
        f.write(text)
    _docker_cp_local_to_container(local_tmp, container_path)


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


_IPV4_RE = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")


def get_dns_ip() -> str:
    """
    Return amnezia-dns IPv4 if available (inside AWG container), else fallback to 1.1.1.1.
    Order: explicit override env → ahostsv4 → hosts → BusyBox ping → fallback.
    """
    if AWG_DNS_IP_OVERRIDE and _IPV4_RE.match(AWG_DNS_IP_OVERRIDE):
        return AWG_DNS_IP_OVERRIDE

    # Strategy 1: getent ahostsv4 — parse first token in Python
    try:
        rc, out, _ = _sh("getent ahostsv4 amnezia-dns 2>/dev/null || true", timeout=3)
        text = (out or "").strip()
        if text:
            ip = text.split()[0]
            if _IPV4_RE.match(ip):
                return ip
    except Exception:
        pass

    # Strategy 2: getent hosts — parse first token in Python
    try:
        rc, out, _ = _sh("getent hosts amnezia-dns 2>/dev/null || true", timeout=3)
        text = (out or "").strip()
        if text:
            ip = text.split()[0]
            if _IPV4_RE.match(ip):
                return ip
    except Exception:
        pass

    # Strategy 3: BusyBox ping — parse (IP) from first line
    try:
        rc, out, _ = _sh("ping -c1 -W1 amnezia-dns 2>/dev/null || true", timeout=3)
        text = (out or "").strip()
        if text:
            import re as _re

            m = _re.search(r"\((\d{1,3}(?:\.\d{1,3}){3})\)", text)
            if m:
                return m.group(1)
    except Exception:
        pass

    return "1.1.1.1"


def _detect_external_host_fallback() -> str:
    """Best-effort external host detection inside AWG container."""
    try:
        rc, out, _ = _sh("curl -fsS ifconfig.me 2>/dev/null || true", timeout=10)
        cand = (out or "").strip()
        if cand and cand.count(".") == 3:
            return cand
    except Exception:
        pass
    try:
        rc, out, _ = _sh(
            "hostname -I 2>/dev/null | awk '{print $1}' || true", timeout=5
        )
        cand = (out or "").strip()
        if cand and cand.count(".") == 3:
            return cand
    except Exception:
        pass
    return ""


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

    params = read_interface_obf_params()

    def append_missing():
        for key in want:
            if not seen[key]:
                val = params.get(key) if isinstance(params, dict) else None
                if val is None and ENV_OBF_DEFAULTS.get(key) is not None:
                    try:
                        val = int(ENV_OBF_DEFAULTS[key])
                    except Exception:
                        val = None
                if val is not None:
                    out.append(f"{key} = {int(val)}")

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
    # sanity
    if "[Interface]" not in conf_text:
        raise RuntimeError("Refusing to write conf_text without [Interface]")
    conf_text = conf_text.replace("\r\n", "\n").replace("\r", "\n")
    if not conf_text.endswith("\n"):
        conf_text += "\n"

    # 1) Write to a local temp file (inside the bot container)
    tmp_dir = "/app/data/tmp"
    _ensure_dir(tmp_dir)
    local_tmp = os.path.join(tmp_dir, f"{WG_IFACE}.conf.new")
    with open(local_tmp, "w", encoding="utf-8") as f:
        f.write(conf_text)

    # quick sanity on local file
    if os.path.getsize(local_tmp) < 40:
        raise RuntimeError("Refusing to write suspiciously small config (local tmp)")

    # 2) Copy into AWG container as .new (no shell quoting headaches)
    container_new = f"{CONF_DIR}/{WG_IFACE}.conf.new"
    _docker_cp_local_to_container(local_tmp, container_new)

    # 3) Apply inside container: mv -> strip -> syncconf (single call)
    # Keep it simple; no nested quotes or here-docs
    apply_cmd = (
        f"set -e; "
        f"mv -f {container_new} {CONF_PATH}; "
        f"chown root:root {CONF_PATH} 2>/dev/null || true; "
        f"chmod 600 {CONF_PATH} 2>/dev/null || true; "
        f"chmod 700 {CONF_DIR} 2>/dev/null || true; "
        f"wg-quick strip {CONF_PATH} > /tmp/{WG_IFACE}.stripped; "
        f"test -s /tmp/{WG_IFACE}.stripped; "
        f"wg syncconf {WG_IFACE} /tmp/{WG_IFACE}.stripped"
    )
    _require_ok(apply_cmd, timeout=60)

    # 4) post-check on container file
    rsz = int(_require_ok(f"stat -c %s {CONF_PATH}", timeout=10))
    if rsz < 40:
        head = _require_ok(f"head -n 20 {CONF_PATH} || true", timeout=5)
        raise RuntimeError(
            f"Refusing to write suspicious conf_text (post-write): size={rsz}\nHEAD:\n{head}"
        )


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
    iface = ipaddress.ip_interface(
        ip_or_cidr if "/" in ip_or_cidr else f"{ip_or_cidr}/32"
    )
    if iface.network.prefixlen != 32:
        raise ValueError("Client IP must be /32")
    return str(iface)

def _append_clients_table_entry(
    client_pub: str,
    client_name: str,
    email: str = "",
    uuid: str = "",
    created_at_iso: Optional[str] = None,
    deleted: bool = False,
    deleted_at: Optional[str] = None,
) -> None:
    """Append a client record to CLIENTS_TABLE. If file is missing/malformed, create a new JSON array.
    Structure:
      {
        "clientId": <client public key>,
        "userData": {"clientName": ..., "creationDate": <RFC822-like>},
        "addInfo": {"email": ..., "uuid": ..., "created_at": <ISO8601>, "deleted": bool, "deleted_at": str|None}
      }
    """
    try:
        raw = _require_ok(f"cat {CLIENTS_TABLE} 2>/dev/null || true", timeout=5)
        data = json.loads(raw) if raw.strip() else []
        if not isinstance(data, list):
            data = []
    except Exception:
        data = []

    # idempotent by clientId
    for it in data:
        if isinstance(it, dict) and it.get("clientId") == client_pub:
            return

    rfc = datetime.now().ctime()  # Mon Nov 10 08:35:32 2025
    iso = created_at_iso or datetime.now(timezone.utc).isoformat()

    rec = {
        "clientId": client_pub,
        "userData": {
            "clientName": client_name,
            "creationDate": rfc,
        },
        "addInfo": {
            "email": email,
            "uuid": uuid,
            "created_at": iso,
            "deleted": bool(deleted),
            "deleted_at": deleted_at,
        },
    }

    text = json.dumps(data + [rec], ensure_ascii=False, indent=4) + "\n"
    _write_text_into_container(CLIENTS_TABLE, text)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: synthesize [Interface] from runtime
# ─────────────────────────────────────────────────────────────────────────────


def _synthesize_interface_from_runtime() -> str:
    """
    Build a minimal [Interface] from runtime (wg dump + ip addr) so that we can
    safely restore a valid config file if it was truncated/empty.
    Includes obfuscation params from runtime where available.
    """
    rows = wg_dump()
    if not rows:
        raise RuntimeError("Cannot synthesize interface: wg dump is empty")
    # wg dump row 0: [0]=private key, [1]=public key, [2]=listen port
    try:
        priv = rows[0][0].strip()
        port = int(rows[0][2])
    except Exception as e:
        raise RuntimeError(f"Cannot synthesize interface: bad wg dump header ({e})")

    # detect configured subnet
    net = server_subnet()  # e.g., IPv4Network('10.8.1.0/24')
    cidr = str(net)

    # merge obfuscation params from runtime
    params = read_interface_obf_params()

    lines = [
        "[Interface]",
        f"PrivateKey = {priv}",
        f"Address = {cidr}",
        f"ListenPort = {port}",
    ]
    for key in ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4"):
        if key in params:
            lines.append(f"{key} = {params[key]}")
    lines.append("")  # trailing newline
    return "\n".join(lines)


def apply_syncconf() -> None:
    """
    Apply config the same way the official app does:
      wg-quick strip <CONF_PATH> > /tmp/<iface>.stripped
      wg syncconf <iface> /tmp/<iface>.stripped
    """
    _secure_conf_perms()
    _require_ok(f'wg-quick strip "{CONF_PATH}" > /tmp/{WG_IFACE}.stripped', timeout=30)
    rc, stripped, _ = _sh(
        f"cat /tmp/{WG_IFACE}.stripped 2>/dev/null || true", timeout=5
    )
    if rc != 0 or not stripped or "[Interface]" not in stripped:
        head = "\n".join((stripped or "").splitlines()[:20])
        raise RuntimeError(f"wg-quick strip produced empty/bad output; HEAD:\n{head}")
    _require_ok(f"wg syncconf {WG_IFACE} /tmp/{WG_IFACE}.stripped", timeout=60)


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


# ─────────────────────────────────────────────────────────────────────────────
# Auto-allocation public helpers
# ─────────────────────────────────────────────────────────────────────────────


def next_free_ip() -> str:
    """Return next available /32 as string, e.g. '10.8.1.23/32'."""
    return alloc_ip_from_runtime()


def add_peer_with_pubkey_auto(cli_pub: str) -> Dict[str, Any]:
    """Allocate a free /32 and add peer for the given public key. Returns summary."""
    ip_cidr = alloc_ip_from_runtime()
    _upsert_peer_block_in_file(cli_pub, ip_cidr)
    rows = wg_dump()
    port = listen_port_from_dump(rows) or 0
    endpoint_host = _detect_external_host_fallback() or AWG_CONNECT_HOST
    if not endpoint_host:
        raise RuntimeError(
            "Cannot determine endpoint host (runtime detect failed and AWG_CONNECT_HOST is empty)"
        )
    endpoint = f"{endpoint_host}:{port}"
    return {
        "assigned_ip": ip_cidr,
        "server_public": server_public_key(),
        "listen_port": port,
        "endpoint": endpoint,
    }


def create_peer_with_generated_keys_auto(meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Generate keys and allocate a free /32 automatically. Returns full summary including client config."""
    ip_cidr = alloc_ip_from_runtime()
    return create_peer_with_generated_keys(ip_cidr, meta=meta)


def ensure_interface_obf() -> None:
    """
    Ensure [Interface] section has all obfuscation keys from config and runtime.
    If the cfg file lost its [Interface], synthesize it from runtime and write it back.
    """
    conf = _read_conf()

    # If file lost its [Interface], synthesize from runtime and write immediately
    if "[Interface]" not in conf:
        synthesized = _synthesize_interface_from_runtime()
        _write_conf_text_atomic(synthesized)
        return

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
    def insert_into_interface_section(
        conf_lines: List[str], additions: List[str]
    ) -> List[str]:
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


def remove_peer_by_ip(ip_or_cidr: str) -> Dict[str, Any]:
    """
    Remove peer(s) whose AllowedIPs exactly match the given IP (normalized to /32).
    Returns a summary with count and removed pubkeys. Raises KeyError if nothing matched.
    """
    target = _ensure_slash32(ip_or_cidr)
    conf = _read_conf()

    lines = conf.splitlines()
    new_lines: List[str] = []
    i = 0
    removed_pub: List[str] = []

    while i < len(lines):
        if lines[i].strip() == "[Peer]":
            j = i + 1
            buf = [lines[i]]
            pub = None
            allowed = None
            # capture this peer block
            while j < len(lines) and lines[j].strip() != "[Peer]":
                s = lines[j].strip()
                if s.startswith("PublicKey") and "=" in s:
                    _, v = s.split("=", 1)
                    pub = v.strip()
                if s.startswith("AllowedIPs") and "=" in s:
                    _, v = s.split("=", 1)
                    allowed = v.strip()
                buf.append(lines[j])
                j += 1

            if allowed == target:
                # drop this block
                if pub:
                    removed_pub.append(pub)
                # do not append buf to new_lines
            else:
                # keep this block
                new_lines.extend(buf)
            i = j
            continue
        else:
            new_lines.append(lines[i])
            i += 1

    if not removed_pub:
        raise KeyError(f"No peer with AllowedIPs = {target}")

    # preserve interface and apply
    new_conf = "\n".join(new_lines)
    if new_conf and not new_conf.endswith("\n"):
        new_conf += "\n"
    _write_conf_text_atomic(new_conf)

    return {
        "removed_count": len(removed_pub),
        "removed_pubkeys": removed_pub,
        "ip": target,
    }


def clean_all_peers(strict_verify: bool = True) -> Dict[str, Any]:
    """
    Remove all [Peer] sections and keep only a valid [Interface] built from runtime.
    This mirrors the app behavior more safely than editing the file in-place.

    Steps:
      1) Count current peers from runtime (wg dump).
      2) Synthesize a clean [Interface] from runtime (private key, port, subnet, obf params).
      3) Write + apply atomically (wg-quick strip → wg syncconf).
      4) Verify that no peers remain (optional strict check).

    Returns a summary with removed_count and current facts.
    """
    # 1) Count peers before
    before = list_peers()
    removed_count = len([p for p in before if p.get("allowed_ips")])

    # 2) Build interface from runtime (always) to avoid stale/invalid file contents
    iface_conf = _synthesize_interface_from_runtime()
    iface_conf = _ensure_interface_has_obf(iface_conf)

    # 3) Apply
    _write_conf_text_atomic(iface_conf)

    # 4) Verify
    rows = wg_dump()
    peers_left = [r for r in rows[1:] if len(r) >= 4 and r[3].strip()]
    if strict_verify and peers_left:
        # Defensive: include a short dump excerpt for diagnostics
        dump_head = "\n".join("\t".join(r[:5]) for r in rows[:5])
        raise RuntimeError(
            f"Expected zero peers after clean, found {len(peers_left)} left. Dump:\n{dump_head}"
        )

    return {
        "removed_count": removed_count,
        "listen_port": listen_port_from_dump(rows),
        "obf": read_interface_obf_params(),
    }


def make_client_conf_text(cli_priv: str, assigned_ip: str) -> str:
    assigned_ip = _ensure_slash32(assigned_ip)
    srv_pub = server_public_key()
    port = listen_port_from_dump(wg_dump()) or 0
    dns_ip = get_dns_ip()  # single IP or "1.1.1.1"
    # Build DNS list: prefer internal amnezia-dns IP if available, then CF pair
    if dns_ip == "1.1.1.1":
        dns_list = ["1.0.0.1", "1.1.1.1"]
    else:
        dns_list = [dns_ip, "1.0.0.1", "1.1.1.1"]
    params = read_interface_obf_params()
    endpoint_host = _detect_external_host_fallback() or AWG_CONNECT_HOST
    if not endpoint_host:
        raise RuntimeError(
            "Cannot determine endpoint host (runtime detect failed and AWG_CONNECT_HOST is empty)"
        )
    endpoint = f"{endpoint_host}:{port}"

    lines = [
        "[Interface]",
        f"Address = {assigned_ip}",
        f"DNS = {', '.join(dns_list)}",
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


def create_peer_with_generated_keys(ip_cidr: str, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ip_cidr = _ensure_slash32(ip_cidr)
    ensure_interface_obf()

    cli_priv = _require_ok("wg genkey", timeout=10).strip()
    cli_pub = _require_ok(f'printf %s "{cli_priv}" | wg pubkey', timeout=10).strip()

    _upsert_peer_block_in_file(cli_pub, ip_cidr)

    client_conf = make_client_conf_text(cli_priv, ip_cidr)

        # Record into clientsTable with extended metadata (non-fatal on error)
    client_name = (meta or {}).get("clientName") or (meta or {}).get("name") or ip_cidr
    email = (meta or {}).get("email", "")
    uuid = (meta or {}).get("uuid", "")
    created_at = (meta or {}).get("created_at")
    deleted_flag = bool((meta or {}).get("deleted", False))
    deleted_at = (meta or {}).get("deleted_at")
    try:
        _append_clients_table_entry(
            cli_pub,
            client_name=client_name,
            email=email,
            uuid=uuid,
            created_at_iso=created_at,
            deleted=deleted_flag,
            deleted_at=deleted_at,
        )
    except Exception:
        pass

    rows = wg_dump()
    port = listen_port_from_dump(rows) or 0
    endpoint_host = _detect_external_host_fallback() or AWG_CONNECT_HOST
    if not endpoint_host:
        raise RuntimeError(
            "Cannot determine endpoint host (runtime detect failed and AWG_CONNECT_HOST is empty)"
        )
    endpoint = f"{endpoint_host}:{port}"

    return {
        "client_private": cli_priv,
        "client_public": cli_pub,
        "assigned_ip": ip_cidr,
        "server_public": server_public_key(),
        "listen_port": port,
        "endpoint": endpoint,
        "client_conf": client_conf,
    }
