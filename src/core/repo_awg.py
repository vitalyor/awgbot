# src/core/repo_awg.py
# ЕДИНЫЙ слой предметной логики для AWG.
# Работает напрямую с файлами внутри контейнера amnezia-awg через services.util.

from __future__ import annotations
import json
import ipaddress
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone

from services.logger_setup import get_logger
from services.util import (
    docker_read_file,
    docker_write_file_atomic,
    docker_exec,
    shq,
    AWG_CONTAINER,
)

log = get_logger("core.repo_awg")

BASE_PATH = "/opt/amnezia/awg"
CLIENTS_TABLE = f"{BASE_PATH}/clientsTable"
WG_CONF = f"{BASE_PATH}/wg0.conf"


# ===== helpers =====


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json_from_container(path: str) -> Optional[Any]:
    txt = docker_read_file(AWG_CONTAINER, path)
    if txt is None or txt.strip() == "":
        return None
    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        log.warning({"event": "awg_json_decode_fail", "path": path, "err": str(e)})
        return None


def _write_json_to_container(path: str, data: Any) -> None:
    docker_write_file_atomic(
        AWG_CONTAINER, path, json.dumps(data, ensure_ascii=False, indent=2)
    )


def _wg_dump() -> List[str]:
    rc, out, err = docker_exec(AWG_CONTAINER, ["wg", "show", "wg0", "dump"])
    if rc != 0:
        log.warning({
            "event": "docker_exec_failed",
            "container": AWG_CONTAINER,
            "cmd": ["wg", "show", "wg0", "dump"],
            "rc": rc,
            "err": err,
        })
        return []
    return out.strip().splitlines()


def _parse_interface_from_conf(conf_text: str) -> Dict[str, str]:
    res: Dict[str, str] = {}
    for raw in (conf_text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = [s.strip() for s in line.split("=", 1)]
            if k in (
                "ListenPort",
                "Address",
                "DNS",
                "Endpoint",
                "Jc",
                "Jmin",
                "Jmax",
                "S1",
                "S2",
                "H1",
                "H2",
                "H3",
                "H4",
                "PrivateKey",
            ):
                res[k] = v
    return res


def _build_conf_from_clients(
    interface_kv: Dict[str, str], clients: Dict[str, Any]
) -> str:
    # Секция [Interface] — переносим ключевые строки как есть (без дублей)
    lines: List[str] = []
    lines.append("[Interface]\n")
    for key in (
        "PrivateKey",
        "Address",
        "ListenPort",
        "Jc",
        "Jmin",
        "Jmax",
        "S1",
        "S2",
        "H1",
        "H2",
        "H3",
        "H4",
    ):
        if key in interface_kv:
            lines.append(f"{key} = {interface_kv[key]}\n")

    # peers из clientsTable (skip deleted)
    for pubkey, data in (clients or {}).items():
        ai = (data or {}).get("addInfo", {}) or {}
        if ai.get("deleted"):
            continue
        ud = (data or {}).get("userData", {}) or {}
        ip = ud.get("ip")
        psk = ud.get("psk") or ""
        if not ip:
            # пропускаем битые записи
            continue
        ip_cidr = ip if "/" in ip else f"{ip}/32"
        lines.append("\n[Peer]\n")
        lines.append(f"PublicKey = {pubkey}\n")
        if psk:
            lines.append(f"PresharedKey = {psk}\n")
        lines.append(f"AllowedIPs = {ip_cidr}\n")
        lines.append("PersistentKeepalive = 25\n")

    return "".join(lines)


def _ensure_clients_table_dict() -> Dict[str, Any]:
    ct = _read_json_from_container(CLIENTS_TABLE)
    if ct is None:
        return {}
    if isinstance(ct, list):
        # мигрируем legacy список к dict
        migrated: Dict[str, Any] = {}
        for it in ct:
            cid = it.get("clientId")
            if cid:
                migrated[cid] = {k: v for k, v in it.items() if k != "clientId"}
        _write_json_to_container(CLIENTS_TABLE, migrated)
        return migrated
    if isinstance(ct, dict):
        return ct
    # что-то не то — начнём с пустого
    return {}


def _gen_wg_keypair() -> Tuple[str, str]:
    rc, priv, err = docker_exec(AWG_CONTAINER, ["wg", "genkey"])
    if rc != 0:
        log.error({
            "event": "docker_exec_failed",
            "container": AWG_CONTAINER,
            "cmd": ["wg", "genkey"],
            "rc": rc,
            "err": err,
        })
        raise RuntimeError(f"wg genkey failed: {err}")
    rc, pub, err = docker_exec(
        AWG_CONTAINER,
        f"printf %s {shq(priv.strip())} | wg pubkey",
    )
    if rc != 0:
        raise RuntimeError(f"wg pubkey failed: {err}")
    return priv.strip(), pub.strip()


def _gen_psk() -> str:
    rc, out, err = docker_exec(AWG_CONTAINER, ["sh", "-lc", "wg genpsk"])
    if rc != 0:
        raise RuntimeError(f"wg genpsk failed: {err}")
    return out.strip()


def _occupied_ips(ct: Dict[str, Any]) -> set:
    used = set()
    # из clientsTable
    for _, data in (ct or {}).items():
        ud = (data or {}).get("userData", {}) or {}
        ip = ud.get("ip")
        if ip:
            try:
                used.add(ipaddress.ip_address(ip))
            except Exception:
                pass
    # из wg dump (на всякий)
    dump = _wg_dump()
    for line in dump[1:]:
        parts = line.split("\t")
        if len(parts) >= 5 and parts[4] and parts[4] != "(none)":
            for token in parts[4].split(","):
                token = token.strip()
                if token.endswith("/32"):
                    try:
                        used.add(ipaddress.ip_address(token.split("/")[0]))
                    except Exception:
                        pass
    return used


def _first_free_ip(subnet: str, used: set) -> str:
    net = ipaddress.ip_network(subnet, strict=False)
    # обычно .1 — сервер; начинаем с .2
    first_host = None
    for i, host in enumerate(net.hosts()):
        if i == 0:
            first_host = host  # вероятно .1
            continue
        if host not in used:
            return str(host)
    raise RuntimeError("No available IPs in subnet")


def _sync_runtime(conf_path: str = WG_CONF) -> None:
    # wg-quick strip + wg syncconf
    cmd = f"wg-quick strip {shq(conf_path)} > /tmp/wg0.stripped && test -s /tmp/wg0.stripped && wg syncconf wg0 /tmp/wg0.stripped"
    rc, out, err = docker_exec(AWG_CONTAINER, cmd)
    if rc != 0:
        raise RuntimeError(f"syncconf failed: {err or out}")


# ===== public API =====


def facts() -> dict:
    txt = docker_read_file(AWG_CONTAINER, WG_CONF)
    port = None
    subnet = None
    dns = None
    endpoint = None
    if txt:
        iface = _parse_interface_from_conf(txt)
        port = iface.get("ListenPort")
        # Address может быть списком, берём первый
        addr = iface.get("Address")
        if addr:
            subnet = addr.split(",")[0].strip()
        dns = iface.get("DNS")
        endpoint = iface.get("Endpoint")
    return {"port": port, "subnet": subnet, "dns": dns, "endpoint": endpoint}


def list_profiles() -> List[dict]:
    ct = _ensure_clients_table_dict()
    dump = _wg_dump()
    allowed_by_pub: Dict[str, str] = {}
    for line in dump[1:]:
        parts = line.split("\t")
        if len(parts) >= 5:
            allowed_by_pub[parts[0]] = parts[4] or "(none)"

    profiles: List[dict] = []
    for pub, data in ct.items():
        data = data or {}
        ud = data.get("userData", {}) or {}
        ai = data.get("addInfo", {}) or {}
        ip = ud.get("ip")
        profiles.append(
            {
                "uuid": ai.get("uuid"),
                "clientId": pub,
                "name": ud.get("clientName") or ud.get("name"),
                "allowed_ips": allowed_by_pub.get(pub, "(none)"),
                "deleted": bool(ai.get("deleted") or data.get("deleted")),
                "owner_tid": ai.get("owner_tid"),
                "userData": ud,
                "addInfo": ai,
            }
        )
    return profiles


def create_profile(profile_data: dict) -> str:
    # читаем интерфейс
    conf_text = docker_read_file(AWG_CONTAINER, WG_CONF) or ""
    iface = _parse_interface_from_conf(conf_text)
    subnet = None
    addr = iface.get("Address")
    if addr:
        subnet = addr.split(",")[0].strip()
    if not subnet:
        raise RuntimeError("Cannot determine subnet from wg0.conf [Interface]/Address")

    # таблица клиентов (dict)
    ct = _ensure_clients_table_dict()

    # ключи и psk в контейнере
    priv, pub = _gen_wg_keypair()
    psk = _gen_psk()

    # свободный IP
    used = _occupied_ips(ct)
    ip = _first_free_ip(subnet, used)

    # запись в таблицу
    import uuid as _uuid

    client_uuid = str(_uuid.uuid4())
    user_data = {
        "clientName": profile_data.get("name") or f"peer-{client_uuid[:8]}",
        "privateKey": priv,
        "psk": psk,
        "ip": ip,
        "created": _now_iso(),
    }
    add_info = {
        "uuid": client_uuid,
        "owner_tid": profile_data.get("owner_tid"),
        "deleted": False,
        "created_at": _now_iso(),
        "source": "bot",
    }
    ct[pub] = {"userData": user_data, "addInfo": add_info}
    _write_json_to_container(CLIENTS_TABLE, ct)

    # пересобираем конфиг из интерфейса и таблицы
    new_conf = _build_conf_from_clients(iface, ct)
    docker_write_file_atomic(AWG_CONTAINER, WG_CONF, new_conf)
    _sync_runtime(WG_CONF)

    log.info({"event": "awg_created", "uuid": client_uuid, "ip": ip})
    return client_uuid


def find_profile_by_uuid(uuid: str) -> Optional[dict]:
    for p in list_profiles():
        if p.get("uuid") == uuid:
            return p
    return None


def delete_profile_by_uuid(uuid: str) -> bool:
    ct = _ensure_clients_table_dict()
    target_pub = None
    changed = False

    # помечаем deleted и находим pubkey
    for pub, data in ct.items():
        ai = (data or {}).get("addInfo", {}) or {}
        if ai.get("uuid") == uuid and not ai.get("deleted"):
            ai["deleted"] = True
            ai["deleted_at"] = _now_iso()
            data["addInfo"] = ai
            ct[pub] = data
            target_pub = pub
            changed = True
            break

    if not changed:
        return False

    _write_json_to_container(CLIENTS_TABLE, ct)

    # пересобрать конфиг без этого peer
    conf_text = docker_read_file(AWG_CONTAINER, WG_CONF) or ""
    iface = _parse_interface_from_conf(conf_text)
    new_conf = _build_conf_from_clients(iface, ct)
    docker_write_file_atomic(AWG_CONTAINER, WG_CONF, new_conf)
    _sync_runtime(WG_CONF)

    log.info({"event": "awg_deleted", "uuid": uuid, "pub": target_pub})
    return True
