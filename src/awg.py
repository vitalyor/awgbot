from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
import ipaddress
import os
import random

# Наши утилиты
from core.docker import run_cmd
from core.state import load_state
from util import AWG_CONNECT_HOST

AWG_CONTAINER = os.getenv("AWG_CONTAINER", "amnezia-awg")
WG_IFACE = os.getenv("AWG_IFACE", "wg0")
CONF_DIR = "/opt/amnezia/awg"
CONF_PATH = f"{CONF_DIR}/{WG_IFACE}.conf"
PSK_PATH = f"{CONF_DIR}/wireguard_psk.key"
SERVER_PUB_PATH = f"{CONF_DIR}/wireguard_server_public_key.key"

# ==== низкоуровневые вызовы в контейнер ====


def _sh(cmd: str, timeout: int = 8) -> Tuple[int, str, str]:
    """
    Выполняет команду в контейнере amnezia-awg и возвращает (rc, stdout, stderr).
    """
    full = f"docker exec {AWG_CONTAINER} sh -lc {repr(cmd)}"
    rc, out, err = run_cmd(full)
    return rc, (out or ""), (err or "")


def _require_ok(cmd: str, timeout: int = 8) -> str:
    rc, out, err = _sh(cmd, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"cmd failed: {cmd}\nrc={rc}\n{err}")
    return out


# ==== парсинг / состояние сервера ====


def _wg_dump() -> List[List[str]]:
    """
    wg show wg0 dump → список строк, разбитых по табам.
    Формат:
      [0] interface|peer
      [1] pubkey
      [2] priv_or_psk
      [3] listen_port | endpoint
      [4] fwmark | allowed_ips
      ...
    Возвращаем только строки с peer (а также первую interface-строку отдельно).
    """
    out = _require_ok(f"wg show {WG_IFACE} dump || wg show all dump || true")
    rows = []
    for line in (out or "").splitlines():
        parts = line.strip().split("\t")
        if not parts or parts[0] != WG_IFACE:
            continue
        rows.append(parts)
    return rows


def _listen_port_from_dump(rows: List[List[str]]) -> Optional[int]:
    # Первая строка (interface) имеет listen_port в колонке [3]
    # У тебя в выводе первая строка имеет много чисел – это нормально для разных версий wg.
    # Поэтому берём ПЕРВУЮ строку и пытаемся распарсить [3] как порт.
    if not rows:
        return None
    try:
        return int(rows[0][3])
    except Exception:
        return None


def _server_subnet() -> ipaddress.IPv4Network:
    """
    Определяем подсеть wg0. В твоём окружении wg0 имеет 10.8.1.0/24.
    Берём из ip addr (надёжнее, чем читать конфиг).
    """
    out = _require_ok("ip -brief addr || ip a || true")
    cidr = None
    for line in out.splitlines():
        if line.startswith(WG_IFACE) and "/" in line:
            # пример: "wg0  UNKNOWN  10.8.1.0/24 ..."
            toks = line.split()
            for tok in toks:
                if "/" in tok and tok.count(".") == 3:
                    cidr = tok
                    break
    if not cidr:
        # запасной путь — прочитать Address из wg0.conf
        cfg = _require_ok(f"cat {CONF_PATH}")
        for line in cfg.splitlines():
            s = line.strip()
            if s.lower().startswith("address"):
                # Address = 10.8.1.0/24
                _, val = s.split("=", 1)
                cidr = val.strip()
                break
    if not cidr:
        raise RuntimeError("Cannot detect WG subnet for wg0")
    return ipaddress.ip_network(cidr, strict=False)


def _used_client_ips(rows: List[List[str]]) -> List[ipaddress.IPv4Address]:
    """
    Собираем все занятые /32 из колонки allowed_ips по peers.
    """
    used: List[ipaddress.IPv4Address] = []
    # Первая строка — интерфейс, вторая в твоём выводе — серверная /32 (10.8.1.1/32).
    for parts in rows[1:]:
        if len(parts) < 5:
            continue
        allowed = parts[4].strip()  # e.g. "10.8.1.23/32"
        if not allowed or "/" not in allowed:
            continue
        try:
            ip = ipaddress.ip_interface(allowed).ip
            used.append(ip)  # только IPv4
        except Exception:
            pass
    return used


def _alloc_free_ip(
    subnet: ipaddress.IPv4Network, used: List[ipaddress.IPv4Address]
) -> ipaddress.IPv4Address:
    """
    Ищем свободный IP в подсети, пропуская network (.0) и, как правило, .1 (сервер).
    Начинаем с .2.
    """
    used_set = set(used)
    # Пройдём все хосты подсети начиная со .2
    hosts = list(subnet.hosts())
    for ip in hosts:
        if ip.packed[-1] < 2:  # .0 или .1
            continue
        if ip in used_set:
            continue
        return ip
    raise RuntimeError("No free IPs left in WG subnet")


def _server_public_key() -> str:
    return _require_ok(f"cat {SERVER_PUB_PATH}").strip()


def _psk_path() -> str:
    # общий PSK, как у тебя в контейнере
    return PSK_PATH


def _gen_client_keypair() -> Tuple[str, str]:
    """
    Генерируем приватный/публичный ключ клиента в контейнере.
    """
    priv = _require_ok("wg genkey")
    pub = _require_ok(f"printf %s {priv} | wg pubkey")
    return priv.strip(), pub.strip()


def _wg_add_peer(pubkey: str, client_ip: str) -> None:
    """
    Добавляем peer в running-config wg0 с allowed-ips и preshared-key.
    """
    psk_path = _psk_path()
    _require_ok(
        f"wg set {WG_IFACE} peer {pubkey} preshared-key {psk_path} allowed-ips {client_ip}/32 persistent-keepalive 25"
    )
    # Persist running config:
    # 1) If /etc/wireguard/wg0.conf exists → use wg-quick save
    # 2) Else append a [Peer] block into /opt/amnezia/awg/wg0.conf
    rc, _, _ = _sh(f"test -f /etc/wireguard/{WG_IFACE}.conf")
    if rc == 0:
        _require_ok(f"wg-quick save {WG_IFACE}")
    else:
        # Heredoc через sh -lc может ломаться из-за кавычек. Используем printf с ANSI C quoting ($'...').
        psk_val = _require_ok(f"cat {PSK_PATH}").strip()
        block = (
            "[Peer]\n"
            f"PublicKey = {pubkey}\n"
            f"PresharedKey = {psk_val}\n"
            f"AllowedIPs = {client_ip}/32\n"
            "PersistentKeepalive = 25\n"
        )
        # Экранируем переходы строк для $'...'
        block_escaped = block.replace("\\", "\\\\").replace("\n", "\\n")
        append_cmd = f"printf %s $'{block_escaped}' >> {CONF_PATH}"
        _require_ok(append_cmd)


def _wg_remove_peer_by_pubkey(pubkey: str) -> bool:
    # Remove from running config
    rc, _, _ = _sh(f"wg set {WG_IFACE} peer {pubkey} remove")
    ok_run = (rc == 0)
    # Persist:
    rc_etc, _, _ = _sh(f"test -f /etc/wireguard/{WG_IFACE}.conf")
    if rc_etc == 0:
        # native persistence
        _sh(f"wg-quick save {WG_IFACE}")
        return ok_run
    # else: edit /opt/amnezia/awg/wg0.conf and drop the peer block by its PublicKey
    awk_prog = (
        'awk -v pk="{pk}" \''
        'BEGIN{{inpeer=0; keep=1; buf=""}}'
        '/^\\[Peer\\]$/{{'
        '  if(inpeer){{ if(keep){{printf "%s", buf}} buf=""; }}'
        '  inpeer=1; keep=1; buf=$0 ORS; next'
        '}}'
        '{{'
        '  if(inpeer){{'
        '    buf=buf $0 ORS;'
        '    if($0 ~ /^PublicKey[ ]*=/){{'
        '      key=$0; sub(/^PublicKey[ ]*=[ ]*/, "", key);'
        '      if(key==pk) keep=0;'
        '    }}'
        '    next'
        '  }}'
        '  print'
        '}}'
        'END{{ if(inpeer){{ if(keep) printf "%s", buf }} }}\' {conf} > {conf}.tmp && mv {conf}.tmp {conf}'
    ).format(pk=pubkey, conf=CONF_PATH)
    rc2, _, _ = _sh(awk_prog)
    return ok_run and (rc2 == 0)


def _read_interface_obf_params() -> dict:
    """
    Try to read Jc/Jmin/Jmax/S1/S2 and optionally H1..H4 from the server Interface block
    in CONF_PATH. Returns dict with any found integer values.
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


def _gen_awg_obf_params() -> dict:
    """Generate reasonable AWG obfuscation params and distinct H1..H4."""
    jc = random.randint(3, 8)
    jmin = random.randint(30, 60)
    jmax = random.randint(max(70, jmin + 10), 1200)
    s1 = random.randint(0, 100)
    s2 = random.randint(0, 100)
    hs = set()
    while len(hs) < 4:
        hs.add(random.randint(1, 2**31 - 1))
    h1, h2, h3, h4 = list(hs)
    return {"Jc": jc, "Jmin": jmin, "Jmax": jmax, "S1": s1, "S2": s2, "H1": h1, "H2": h2, "H3": h3, "H4": h4}


# ==== вспомогательная функция для получения IP DNS-контейнера ====

def _get_dns_ip() -> str:
    """
    Возвращает IP контейнера amnezia-dns, если он доступен в той же докер-сети.
    Порядок попыток:
      1) Внутри amnezia-awg: getent hosts amnezia-dns
      2) На хосте: docker inspect ... amnezia-dns
    Фолбэк — 1.1.1.1
    """
    try:
        rc, out, _ = _sh("getent hosts amnezia-dns | awk '{print $1}'")
        if rc == 0:
            ip = (out or "").strip().splitlines()[0].strip()
            # простая валидация IPv4
            if ip.count('.') == 3:
                return ip
    except Exception:
        pass
    try:
        rc, out, _ = run_cmd("docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' amnezia-dns")
        if rc == 0:
            ip = (out or '').strip()
            if ip and ip.count('.') == 3:
                return ip
    except Exception:
        pass
    return "1.1.1.1"

# ==== публичный API (используется bot.py) ====


def list_all() -> List[Dict[str, Any]]:
    """
    Возвращает список всех peer'ов (по живой конфигурации wg0).
    Поля: pubkey, allowed_ips, endpoint, latest_handshake, transfer_rx, transfer_tx.
    """
    rows = _wg_dump()
    out: List[Dict[str, Any]] = []
    # peers начинаются с индекса 1 (0 — интерфейс), но на некоторых системах 2-я строка может быть "server /32".
    for parts in rows[1:]:
        if len(parts) < 5:
            continue
        d: Dict[str, Any] = {
            "pubkey": parts[1],
            "preshared_key": parts[2],
            "endpoint": parts[3],
            "allowed_ips": parts[4],
        }
        # хвостовые метрики зависят от версии wg
        if len(parts) >= 9:
            d["latest_handshake"] = parts[5]
            d["transfer_rx"] = parts[6]
            d["transfer_tx"] = parts[7]
        out.append(d)
    return out


def find_user(tid: int, name: str) -> Optional[Dict[str, Any]]:
    """
    Находит peer по (tid,name) через assigned_ip из state.json.
    Возвращает словарь с endpoint/port/ip/pubkey, если найден.
    """
    st = load_state()
    u = st.get("users", {}).get(str(tid), {})
    pr = next(
        (
            p
            for p in u.get("profiles", [])
            if not p.get("deleted")
            and p.get("type") in ("amneziawg", "awg")
            and p.get("name") == name
        ),
        None,
    )
    if not pr:
        return None
    ip = (pr.get("assigned_ip") or "").split("/")[0]
    if not ip:
        return None
    rows = _wg_dump()
    # port с интерфейса
    port = _listen_port_from_dump(rows) or 0
    # найти peer по allowed_ips
    for parts in rows[1:]:
        if len(parts) < 5:
            continue
        allowed = parts[4]
        if allowed.startswith(f"{ip}/"):
            endpoint = parts[3]  # как правило "(none)" для клиента за NAT
            return {
                "pubkey": parts[1],
                "allowed_ip": allowed,
                "endpoint": endpoint,
                "port": str(port),
            }
    return None


def remove_user_by_name(tid: int, name: str) -> bool:
    """
    Удаляет peer, соответствующий (tid,name), используя assigned_ip из state.json.
    """
    st = load_state()
    u = st.get("users", {}).get(str(tid), {})
    pr = next(
        (
            p
            for p in u.get("profiles", [])
            if not p.get("deleted")
            and p.get("type") in ("amneziawg", "awg")
            and p.get("name") == name
        ),
        None,
    )
    if not pr:
        return False
    ip = (pr.get("assigned_ip") or "").split("/")[0]
    if not ip:
        return False
    rows = _wg_dump()
    for parts in rows[1:]:
        if len(parts) < 5:
            continue
        allowed = parts[4]
        if allowed.startswith(f"{ip}/"):
            pub = parts[1]
            return _wg_remove_peer_by_pubkey(pub)
    return False


def add_user(tid: int, name: str) -> Dict[str, Any]:
    """
    Создаёт нового клиента:
      - генерирует ключи клиента
      - выделяет свободный /32
      - добавляет peer в wg0 (allowed-ips, preshared-key, keepalive)
      - сохраняет в конфиг через wg-quick save
      - возвращает данные для state.json и отображения в боте

    Возвращаемое:
    {
      "email": f"{tid}.{name}@bot",
      "vpn_url": "<WG config text for now>",  # временно: текст WireGuard-конфига
      "endpoint": "<host:port>",              # внешний endpoint
      "assigned_ip": "10.8.1.X/32"
    }
    """
    rows = _wg_dump()
    subnet = _server_subnet()
    used = _used_client_ips(rows)
    client_ip = _alloc_free_ip(subnet, used)
    client_ip_cidr = f"{client_ip}/32"

    # порт слушателя сервера
    port = _listen_port_from_dump(rows)
    if not port:
        # запасной путь: прочесть из конфига
        cfg = _require_ok(f"cat {CONF_PATH}")
        port = 33925
        for line in cfg.splitlines():
            s = line.strip()
            if s.lower().startswith("listenport"):
                _, val = s.split("=", 1)
                try:
                    port = int(val.strip())
                except Exception:
                    pass
                break

    # ключи и добавление
    cli_priv, cli_pub = _gen_client_keypair()
    _wg_add_peer(cli_pub, str(client_ip))

    # серверный публичный ключ и общий PSK
    # determine AWG obfuscation params: prefer server interface values for J*/S*; generate distinct H* per client
    iface_params = _read_interface_obf_params()
    if iface_params:
        # use server Jc/Jmin/Jmax/S1/S2 if present, otherwise generate defaults
        awg_params = {
            "Jc": iface_params.get("Jc", None),
            "Jmin": iface_params.get("Jmin", None),
            "Jmax": iface_params.get("Jmax", None),
            "S1": iface_params.get("S1", None),
            "S2": iface_params.get("S2", None),
        }
        # generate H1..H4 per-client to keep them unique
        gen = _gen_awg_obf_params()
        awg_params.update({k: gen[k] for k in ("H1", "H2", "H3", "H4")})
        # fill missing J*/S* with generated defaults if any None
        for k in ("Jc","Jmin","Jmax","S1","S2"):
            if awg_params.get(k) is None:
                awg_params[k] = gen[k if k in gen else k]
    else:
        awg_params = _gen_awg_obf_params()

    srv_pub = _server_public_key()
    psk = _require_ok(f"cat {PSK_PATH}").strip()

    # внешний endpoint (хост берём из util.AWG_CONNECT_HOST, порт — listen_port)
    endpoint = f"{AWG_CONNECT_HOST}:{port}"

    # Получаем IP DNS-контейнера
    dns_ip = _get_dns_ip()

    # Сформируем клиентский .conf (на данный момент это и будет "vpn_url" для вывода)
    wg_conf = (
        "[Interface]\n"
        f"PrivateKey = {cli_priv}\n"
        f"Address = {client_ip_cidr}\n"
        f"DNS = {dns_ip}\n"
        f"Jc = {awg_params['Jc']}\n"
        f"Jmin = {awg_params['Jmin']}\n"
        f"Jmax = {awg_params['Jmax']}\n"
        f"S1 = {awg_params['S1']}\n"
        f"S2 = {awg_params['S2']}\n"
        f"H1 = {awg_params['H1']}\n"
        f"H2 = {awg_params['H2']}\n"
        f"H3 = {awg_params['H3']}\n"
        f"H4 = {awg_params['H4']}\n"
        "\n"
        "[Peer]\n"
        f"PublicKey = {srv_pub}\n"
        f"PresharedKey = {psk}\n"
        f"Endpoint = {endpoint}\n"
        "AllowedIPs = 0.0.0.0/0, ::/0\n"
        "PersistentKeepalive = 25\n"
    )

    return {
        "email": f"{tid}.{name}@bot",
        "vpn_url": wg_conf,  # временно: текст WG-конфига (заменим на vpn:// позже)
        "endpoint": endpoint,
        "assigned_ip": client_ip_cidr,
        # опционально можно вернуть и pubkey, если решим сохранять его в state:
        "pubkey": cli_pub,
    }
