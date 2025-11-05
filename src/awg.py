# awg.py
import json, re, base64, zlib
from typing import Dict, Any, List, Optional
from util import (
    docker_read_file, docker_write_file_atomic, docker_exec, shq,
    get_awg_bin, AWG_CONTAINER, AWG_CONFIG_PATH, AWG_CONNECT_HOST, AWG_LISTEN_PORT
)

def _email(tg_id: int, name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return f"{tg_id}-{safe}"

def _server_listen_port() -> int:
    # сначала wg0.conf
    try:
        wgconf = docker_read_file(AWG_CONTAINER, AWG_CONFIG_PATH)
        for line in wgconf.splitlines():
            m = re.match(r"\s*ListenPort\s*=\s*(\d+)\s*$", line)
            if m: return int(m.group(1))
    except Exception:
        pass
    # затем wg/awg show
    try:
        out = docker_exec(AWG_CONTAINER, "sh", "-lc", f"{get_awg_bin()} show 2>/dev/null || true")
        mm = re.search(r"listening port:\s*(\d+)", out)
        if mm: return int(mm.group(1))
    except Exception:
        pass
    return AWG_LISTEN_PORT or 51280

def _server_pubkey() -> str:
    try:
        return docker_read_file(AWG_CONTAINER, "/opt/amnezia/awg/wireguard_server_public_key.key").strip()
    except Exception:
        try:
            out = docker_exec(AWG_CONTAINER, "sh", "-lc", f"{get_awg_bin()} show 2>/dev/null || true")
            mm = re.search(r"public key:\s*([A-Za-z0-9+/=\-]+)", out)
            if mm: return mm.group(1).strip()
        except Exception: pass
    return ""

def _next_ip() -> str:
    try:
        out = docker_exec(
            AWG_CONTAINER, "sh", "-lc",
            r'grep -oE "AllowedIPs[[:space:]]*=[[:space:]]*[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+" '
            + shq(AWG_CONFIG_PATH) +
            r' | awk -F. "{print $4}" | sort -n | uniq || true'
        )
        used = {x.strip() for x in out.splitlines() if x.strip()}
    except Exception:
        used = set()
    for i in range(1, 255):
        if str(i) not in used:
            return f"10.8.1.{i}/32"
    raise RuntimeError("Нет свободных IP в 10.8.1.0/24")

def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")

def add_user(tg_id: int, name: str) -> Dict[str, str]:
    if not AWG_CONNECT_HOST:
        raise RuntimeError("Не задан AWG_CONNECT_HOST.")
    listen_port = _server_listen_port()
    server_pub = _server_pubkey()
    assigned_ip = _next_ip()

    bin_ = get_awg_bin()
    priv = docker_exec(AWG_CONTAINER, "sh", "-lc", f"{bin_} genkey").strip()
    pub  = docker_exec(AWG_CONTAINER, "sh", "-lc", f"printf %s {shq(priv)} | {bin_} pubkey").strip()
    psk  = docker_exec(AWG_CONTAINER, "sh", "-lc", f"{bin_} genpsk || true").strip()

    # применяем live
    docker_exec(
        AWG_CONTAINER, "sh","-lc",
        f'TMP="/tmp/psk-$$.key"; printf %s {shq(psk)} > "$TMP"; '
        f'{bin_} set wg0 peer {shq(pub)} preshared-key "$TMP" allowed-ips {shq(assigned_ip)}; rm -f "$TMP"'
    )
    # фиксируем в wg0.conf
    docker_exec(
        AWG_CONTAINER, "sh", "-lc",
        f'TS=$(date +%Y%m%d-%H%M%S); cp -f {shq(AWG_CONFIG_PATH)} {shq(AWG_CONFIG_PATH)}.bak-$TS; '
        f'{{ echo; echo "[Peer]"; echo "PublicKey = {pub}"; echo "PresharedKey = {psk}"; echo "AllowedIPs = {assigned_ip}"; }} >> {shq(AWG_CONFIG_PATH)}'
    )

    # clientsTable
    email = _email(tg_id, name)
    docker_exec(
        AWG_CONTAINER, "sh","-lc",
        "CT=/opt/amnezia/awg/clientsTable; TS=$(date +%Y%m%d-%H%M%S); cp -f \"$CT\" \"$CT.bak-$TS\" || true; "
        f'PUB={shq(pub)}; IP={shq(assigned_ip)}; EMAIL={shq(email)}; DATE="$(date -u)"; '
        'if grep -q \\"clientId\\" "$CT" 2>/dev/null; then '
        '  TMP="$(mktemp)"; sed \'$d\' "$CT" > "$TMP"; echo "," >> "$TMP"; '
        '  cat >> "$TMP" <<EOF\n{\n  "clientId": "'"$PUB"'",\n  "userData": {\n    "allowedIps": "'"$IP"'",\n    "clientName": "'"$EMAIL"'",\n    "creationDate": "'"$DATE"'" \n  }\n}\nEOF\n'
        '  echo "]" >> "$TMP"; mv "$TMP" "$CT"; '
        'else '
        '  cat > "$CT" <<EOF\n[\n{\n  "clientId": "'"$PUB"'",\n  "userData": {\n    "allowedIps": "'"$IP"'",\n    "clientName": "'"$EMAIL"'",\n    "creationDate": "'"$DATE"'" \n  }\n}\n]\nEOF\n'
        "fi"
    )

    endpoint = f"{AWG_CONNECT_HOST}:{listen_port}"

    # собираем легальный для Amnezia wrapper (vpn://) с last_config
    client_last_json = {
        "interface":{"privateKey": priv, "address": assigned_ip.split("/")[0], "dns":["1.1.1.1","1.0.0.1"]},
        "peer":{"publicKey": server_pub, "presharedKey": psk, "endpoint": endpoint,
                "allowedIPs":["0.0.0.0/0","::/0"], "persistentKeepalive":25},
    }
    wrapper = {
        "containers":[{"container":"amnezia-awg","amnezia_wg":{
            "last_config": client_last_json, "port": str(listen_port), "transport_proto":"udp"}}],
        "defaultContainer":"amnezia-awg","description":name,"dns1":"1.1.1.1","dns2":"1.0.0.1",
        "hostName":AWG_CONNECT_HOST,"nameOverriddenByUser":True
    }
    wjson = json.dumps(wrapper, ensure_ascii=False, separators=(",",":"))
    vpn_url = "vpn://" + _b64url(b"\x00\x00\x07\x43" + zlib.compress(wjson.encode("utf-8"),9))

    return {
        "email": _email(tg_id, name),
        "public_key": pub, "private_key": priv, "psk": psk,
        "assigned_ip": assigned_ip, "endpoint": endpoint, "vpn_url": vpn_url,
        "port": str(listen_port), "server_pub": server_pub or ""
    }

def _read_clients_table() -> List[Dict[str, Any]]:
    try:
        raw = docker_read_file(AWG_CONTAINER, "/opt/amnezia/awg/clientsTable")
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _write_clients_table(lst: List[Dict[str, Any]]):
    docker_write_file_atomic(
        AWG_CONTAINER, "/opt/amnezia/awg/clientsTable",
        json.dumps(lst, ensure_ascii=False, indent=4)
    )

def _remove_peer_live(pub: str):
    bin_ = get_awg_bin()
    docker_exec(AWG_CONTAINER, "sh", "-lc",
                f'{bin_} set wg0 peer {shq(pub)} remove 2>/dev/null || true')

def remove_user_by_name(tg_id: int, name: str) -> bool:
    email = _email(tg_id, name)
    entries = _read_clients_table()
    idx, pub, ip = -1, None, None
    for i, it in enumerate(entries):
        try:
            cid = (it.get("clientId") or "").strip()
            ud  = it.get("userData") or {}
            cname = (ud.get("clientName") or "").strip()
            allowed = (ud.get("allowedIps") or "").strip()
            if cname == email:
                idx, pub, ip = i, (cid or None), (allowed or None)
                break
        except Exception:
            continue

    removed = False
    if idx >= 0:
        if pub: _remove_peer_live(pub); removed = True
        # чистим wg0.conf блоки по pub/ip (busybox-совместимый awk)
        try:
            script = (
                "awk -v PUB=" + shq(pub or "") + " -v IP=" + shq(ip or "") +
                r" 'function flush(){ if (!del) { for(i=1;i<=n;i++){ print buf[i] } } n=0; del=0 }"
                r" BEGIN{ n=0; del=0 }"
                r" /^\[Peer\]/{ flush() }"
                r" { line=$0; buf[++n]=line;"
                r"   if (PUB != \"\" && line ~ /PublicKey[[:space:]]*=[[:space:]]*/) { if (index(line, PUB)) del=1 }"
                r"   if (IP  != \"\" && line ~ /AllowedIPs[[:space:]]*=[[:space:]]*/) { if (index(line, IP))  del=1 }"
                r" }"
                r" END{ flush() }'"
                " " + shq(AWG_CONFIG_PATH) + " > " + shq(AWG_CONFIG_PATH) + ".new && mv " +
                shq(AWG_CONFIG_PATH) + ".new " + shq(AWG_CONFIG_PATH)
            )
            docker_exec(AWG_CONTAINER, "sh", "-lc", script)
        except Exception:
            pass
        new_entries = entries[:idx] + entries[idx+1:]
        _write_clients_table(new_entries)
        return True

    return removed

def find_user(tg_id: int, name: str) -> Optional[Dict[str, str]]:
    if not AWG_CONNECT_HOST:
        raise RuntimeError("Не задан AWG_CONNECT_HOST.")
    listen_port = _server_listen_port()
    server_pub = _server_pubkey()
    email = _email(tg_id, name)

    client_pub = ""
    try:
        for it in _read_clients_table():
            cid = (it.get("clientId") or "")
            ud = it.get("userData") or {}
            if (ud.get("clientName") or "") == email:
                client_pub = cid.strip(); break
    except Exception:
        pass
    if not client_pub:
        return None

    return {
        "email": email,
        "public_key": client_pub,
        "endpoint": f"{AWG_CONNECT_HOST}:{listen_port}",
        "server_pub": server_pub,
        "port": str(listen_port),
    }