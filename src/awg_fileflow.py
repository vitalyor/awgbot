# src/awg_fileflow.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
import ipaddress
import os
import tempfile
import shutil

# наши утилиты
from core.docker import run_cmd
from util import AWG_CONNECT_HOST

# параметры окружения/пути (совместимы с вашим стеком)
AWG_CONTAINER = os.getenv("AWG_CONTAINER", "amnezia-awg")
WG_IFACE = os.getenv("AWG_IFACE", "wg0")
CONF_DIR = "/opt/amnezia/awg"
CONF_PATH = f"{CONF_DIR}/{WG_IFACE}.conf"
PSK_PATH = f"{CONF_DIR}/wireguard_psk.key"
SERVER_PUB = f"{CONF_DIR}/wireguard_server_public_key.key"

# ───────────────────── низкоуровневые вызовы ─────────────────────


def _sh(cmd: str, timeout: int = 8) -> Tuple[int, str, str]:
    full = f"docker exec {AWG_CONTAINER} sh -lc {repr(cmd)}"
    rc, out, err = run_cmd(full)
    return rc, (out or ""), (err or "")


def _require_ok(cmd: str, timeout: int = 8) -> str:
    rc, out, err = _sh(cmd, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"cmd failed: {cmd}\nrc={rc}\n{err}")
    return out


# ───────────────────── парсинг состояния wg ─────────────────────


def wg_dump() -> List[List[str]]:
    """
    wg show <iface> dump → список строк (колонки табами):
      0-я строка (интерфейс): [0]priv, [1]pub, [2]listen_port, [3]fwmark
      peer-строки (NR>1):     [0]pub, [1]psk, [2]endpoint, [3]allowed_ips, [4]handshake, [5]rx, [6]tx, [7]keepalive
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
        return int(rows[0][2])  # listen_port в [2]
    except Exception:
        return None


def server_subnet() -> ipaddress.IPv4Network:
    """
    Определяем подсеть wg0. Сначала пробуем ip addr, потом Address в конфиге.
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
        if len(parts) < 4:  # peers
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
        if ip.packed[-1] < 2:  # .0,.1 пропускаем
            continue
        if ip in used_set:
            continue
        return ip
    raise RuntimeError("No free IPs left in WG subnet")


# ───────────────────── вспомогательные ─────────────────────


def server_public_key() -> str:
    return _require_ok(f"cat {SERVER_PUB}").strip()


def shared_psk() -> str:
    return _require_ok(f"cat {PSK_PATH}").strip()


def get_dns_ip() -> str:
    """
    Возвращает IP контейнера amnezia-dns (если есть), иначе 1.1.1.1
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


def read_interface_obf_params() -> dict:
    """
    Читает из [Interface] секции wg0.conf поля AWG: Jc/Jmin/Jmax/S1/S2/H1..H4 (если есть)
    """
    try:
        cfg = _require_ok(f"cat {CONF_PATH}")
    except Exception:
        return {}
    res = {}
    for line in cfg.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = [x.strip() for x in s.split("=", 1)]
        if k in ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4"):
            try:
                res[k] = int(v)
            except Exception:
                pass
    return res


# ───────────────────── работа с файлом конфигурации ─────────────────────


def _atomic_write(path: str, content: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


def _read_conf() -> str:
    return _require_ok(f"cat {CONF_PATH}")


def _append_peer_block_in_text(
    conf_text: str, pubkey: str, psk_val: str, client_ip: str
) -> str:
    block = (
        "\n[Peer]\n"
        f"PublicKey = {pubkey}\n"
        f"PresharedKey = {psk_val}\n"
        f"AllowedIPs = {client_ip}/32\n"
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
            # собрать блок
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


def apply_setconf() -> None:
    """
    Применяет текущий файл конфигурации к интерфейсу.
    """
    _require_ok(f"wg setconf {WG_IFACE} {CONF_PATH}")


# ───────────────────── публичные функции «песочницы» ─────────────────────


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


def add_peer_via_file_and_setconf(cli_pub: str, client_ip: str) -> None:
    """
    Добавляет peer, редактируя файл wg0.conf и применяя wg setconf.
    """
    conf = _read_conf()
    conf2 = _append_peer_block_in_text(conf, cli_pub, shared_psk(), client_ip)
    _atomic_write(
        "/mnt/.awg_conf.tmp", conf2
    )  # записать на хосте? мы в боте — пишем в контейнер через docker exec
    # мы внутри бота, поэтому перепишем файл через контейнер: echo > file не надёжен, используем here-doc
    payload = conf2.replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`")
    _require_ok(f"cat > {CONF_PATH} <<'EOF'\n{payload}\nEOF")
    apply_setconf()


def remove_peer_via_file_and_setconf(pubkey: str) -> None:
    conf = _read_conf()
    conf2 = _drop_peer_block_in_text(conf, pubkey)
    payload = conf2.replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`")
    _require_ok(f"cat > {CONF_PATH} <<'EOF'\n{payload}\nEOF")
    apply_setconf()


def make_client_conf_text(cli_priv: str, assigned_ip: str) -> str:
    """
    Сборка клиентского .conf (AWG-совместимый), используя серверные параметры.
    """
    srv_pub = server_public_key()
    port = listen_port_from_dump(wg_dump()) or 0
    dns_ip = get_dns_ip()
    params = read_interface_obf_params()  # Jc/Jmin/Jmax/S1/S2/H1..H4
    endpoint = f"{AWG_CONNECT_HOST}:{port}"

    lines = [
        "[Interface]",
        f"PrivateKey = {cli_priv}",
        f"Address = {assigned_ip}",
        f"DNS = {dns_ip}",
    ]
    for key in ("Jc", "Jmin", "Jmax", "S1", "S2", "H1", "H2", "H3", "H4"):
        if key in params:
            lines.append(f"{key} = {params[key]}")
    lines += [
        "",
        "[Peer]",
        f"PublicKey = {srv_pub}",
        f"PresharedKey = {shared_psk()}",
        f"Endpoint = {endpoint}",
        "AllowedIPs = 0.0.0.0/0, ::/0",
        "PersistentKeepalive = 25",
        "",
    ]
    return "\n".join(lines)


def alloc_ip_from_runtime() -> str:
    """
    Возвращает свободный /32 в строковом виде, глядя на текущий wg dump.
    """
    rows = wg_dump()
    subnet = server_subnet()
    used = used_client_ips(rows)
    ip = alloc_free_ip(subnet, used)
    return f"{ip}/32"
