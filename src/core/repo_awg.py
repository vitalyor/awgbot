# src/core/repo_awg.py
# ЕДИНЫЙ слой предметной логики для AWG (Amnezia WireGuard).
# Работает напрямую с файлами и командами внутри контейнера amnezia-awg через services.util.

from __future__ import annotations

import json
import ipaddress
import uuid as uuidlib
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import shlex
import time

from services.logger_setup import get_logger
from services.util import (
    docker_exec,
    docker_read_file,
    docker_write_file_atomic,
    AWG_CONTAINER,
    AWG_CONFIG_PATH,
)

log = get_logger("core.repo_awg")

CLIENTS_TABLE = "/opt/amnezia/awg/clientsTable"


# ===== helpers =====


def _now_iso() -> str:
    """Возвращает время в ISO-8601 формате без микросекунд (UTC)."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _ctime_like() -> str:
    """Формат времени в стиле Mon Nov 10 08:35:32 2025."""
    return datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S %Y")


def _read_clients_table() -> List[Dict[str, Any]]:
    """Чтение и нормализация clientsTable."""
    try:
        txt = docker_read_file(AWG_CONTAINER, CLIENTS_TABLE)
        raw = json.loads(txt) if txt.strip() else []
    except Exception:
        raw = []

    # Старые версии — dict -> превращаем в list
    if isinstance(raw, dict):
        items = []
        for cid, data in raw.items():
            entry = {"clientId": cid}
            if isinstance(data, dict):
                entry.update(data)
            items.append(entry)
        raw = items
    if not isinstance(raw, list):
        raw = []

    # Нормализация недостающих полей
    changed = False
    for it in raw:
        ud = it.setdefault("userData", {}) or {}
        ai = it.setdefault("addInfo", {}) or {}
        cid = it.get("clientId", "")
        if "clientName" not in ud:
            ud["clientName"] = f"AWG-{str(cid)[:8]}"
            changed = True
        if "creationDate" not in ud:
            ud["creationDate"] = _ctime_like()
            changed = True
        ai.setdefault("type", "awg")
        ai.setdefault("uuid", ai.get("uuid") or cid)
        ai.setdefault("owner_tid", None)
        ai.setdefault("email", ai.get("email"))
        ai.setdefault("created_at", ai.get("created_at") or _now_iso())
        ai.setdefault("source", "bot")
        ai.setdefault("notes", "")
        it["userData"] = ud
        it["addInfo"] = ai

    if changed:
        try:
            docker_write_file_atomic(
                AWG_CONTAINER,
                CLIENTS_TABLE,
                json.dumps(raw, ensure_ascii=False, indent=2),
            )
        except Exception as e:
            log.warning({"event": "awg_clientsTable_autofix_failed", "err": str(e)})

    return raw


def _write_clients_table(items: List[Dict[str, Any]]) -> None:
    """Атомарная запись clientsTable."""
    docker_write_file_atomic(
        AWG_CONTAINER, CLIENTS_TABLE, json.dumps(items, ensure_ascii=False, indent=2)
    )


def _wg_dump_allowed_map() -> Dict[str, str]:
    """Собирает карту {pubkey -> AllowedIPs} из вывода wg show wg0 dump."""
    rc, out, _ = docker_exec(
        AWG_CONTAINER, ["sh", "-lc", "wg show wg0 dump 2>/dev/null || true"]
    )
    if rc != 0 or not out:
        return {}
    lines = out.strip().splitlines()
    if not lines:
        return {}
    m: Dict[str, str] = {}
    for line in lines[1:]:
        parts = line.split("\t")
        # wg dump peer columns (tab-separated):
        # 0 pubkey, 1 preshared, 2 endpoint, 3 allowed_ips, 4 latest_handshake, 5 rx, 6 tx, 7 persistent_keepalive
        if len(parts) >= 4:
            m[parts[0]] = parts[3]
    return m


def list_profiles() -> List[dict]:
    """Возвращает список профилей AWG в нормализованном виде."""
    clients = _read_clients_table()
    allowed_map = _wg_dump_allowed_map()
    profiles: List[Dict[str, Any]] = []
    for c in clients:
        cid = c.get("clientId", "")
        ud = c.get("userData", {}) or {}
        ai = c.get("addInfo", {}) or {}

        # Фолбэк: если рантайм ещё не подхватил пира, подставляем ip/32 из userData
        allowed = allowed_map.get(cid)
        if not allowed:
            ip = ud.get("ip")
            allowed = f"{ip}/32" if ip else "(none)"

        profiles.append(
            {
                "uuid": ai.get("uuid") or cid,
                "clientId": cid,
                "name": ud.get("clientName"),
                "allowed_ips": allowed,
                "owner_tid": ai.get("owner_tid"),
                "userData": ud,
                "addInfo": ai,
            }
        )
    return profiles


def _gen_wg_keypair() -> tuple[str, str]:
    """Генерирует пару приватный/публичный ключ внутри контейнера.
    Избегаем передачи stdin через docker_exec (input_bytes не поддерживается):
    для публичного ключа используем пайп через `printf`.
    """
    # приватный ключ
    rc, priv, err = docker_exec(AWG_CONTAINER, ["sh", "-lc", "wg genkey"])
    if rc != 0 or not (priv or "").strip():
        raise RuntimeError(f"wg genkey failed: {err}")
    priv = priv.strip()

    # публичный ключ: безопасно экранируем приватный ключ и прокидываем через printf | wg pubkey
    quoted_priv = shlex.quote(priv)
    cmd = f"sh -lc 'printf %s {quoted_priv} | wg pubkey'"
    rc, pub, err = docker_exec(AWG_CONTAINER, cmd)
    if rc != 0 or not (pub or "").strip():
        raise RuntimeError(f"wg pubkey failed: {err}")
    pub = pub.strip()
    return priv, pub


def _gen_psk() -> str:
    """Генерирует PSK внутри контейнера."""
    rc, psk, err = docker_exec(AWG_CONTAINER, ["sh", "-lc", "wg genpsk"])
    if rc != 0 or not psk.strip():
        raise RuntimeError(f"wg genpsk failed: {err}")
    return psk.strip()


def _get_next_ip(clients: List[Dict[str, Any]], subnet_cidr: str) -> str:
    """Выдаёт следующий свободный IP из подсети."""
    net = ipaddress.ip_network(subnet_cidr, strict=False)
    used = set()
    for c in clients:
        ip_str = (c.get("userData") or {}).get("ip")
        if ip_str:
            try:
                used.add(ipaddress.ip_address(ip_str))
            except Exception:
                pass
    # пропускаем первый хост (.1) — сервер
    hosts = list(net.hosts())
    for h in hosts[1:]:
        if h not in used:
            return str(h)
    raise RuntimeError("No available IPs in subnet")


def _name_in_use_for_owner(owner_tid: int, name: str) -> bool:
    if not name:
        return False
    name_norm = " ".join(name.split()).lower()
    for p in list_profiles():
        if p.get("owner_tid") != owner_tid:
            continue
        # AWG-репозиторий проверяет только AWG-профили (этот файл)
        pname = (p.get("name") or "").strip().lower()
        if pname == name_norm:
            return True
    return False


def facts() -> dict:
    """
    Возвращает информацию из [Interface] wg0.conf:
    - listen_port: int | None
    - addresses: List[str] (как в конфиге, CIDR)
    - server_ip: str | None (первый IP без маски, если из Address можно извлечь)
    - subnet: str | None (первый CIDR из Address)
    - dns: str | None
    - endpoint: str | None
    """
    listen_port = None
    addresses: list[str] = []
    dns = None
    endpoint = None

    try:
        lines = docker_read_file(AWG_CONTAINER, AWG_CONFIG_PATH).splitlines()
    except Exception:
        lines = []

    for line in lines:
        l = line.strip()
        if not l or l.startswith("#") or l.startswith(";"):
            continue
        if l.startswith("ListenPort"):
            try:
                listen_port = int(l.split("=", 1)[1].strip())
            except Exception:
                listen_port = None
        elif l.startswith("Address"):
            # Address = ip1/mask, ip2/mask, ...
            val = l.split("=", 1)[1].strip()
            for chunk in val.split(","):
                c = chunk.strip()
                if c:
                    addresses.append(c)
        elif l.startswith("DNS"):
            dns = l.split("=", 1)[1].strip()
        elif l.startswith("Endpoint"):
            endpoint = l.split("=", 1)[1].strip()

    subnet = addresses[0] if addresses else None

    # Попробуем вытащить "server_ip" из первого CIDR (если там ip/mask)
    server_ip = None
    if subnet and "/" in subnet:
        try:
            server_ip = subnet.split("/", 1)[0]
        except Exception:
            server_ip = None

    return {
        "listen_port": listen_port,
        "addresses": addresses,
        "server_ip": server_ip,
        "subnet": subnet,
        "dns": dns,
        "endpoint": endpoint,
    }


def _apply_runtime_sync() -> None:
    """Применяет изменения wg0.conf к рантайму WireGuard внутри контейнера.
    Используем wg-quick strip + wg syncconf.
    """
    cmd = (
        "set -e; "
        "TMP=$(mktemp /tmp/wg0.stripped.XXXXXX); "
        f'wg-quick strip "{AWG_CONFIG_PATH}" > "$TMP"; '
        'test -s "$TMP"; '
        'wg syncconf wg0 "$TMP"; '
        'rm -f "$TMP"'
    )
    rc, out, err = docker_exec(AWG_CONTAINER, ["sh", "-lc", cmd])
    if rc != 0:
        log.warning(
            {
                "event": "awg_syncconf_failed",
                "code": rc,
                "err": err.strip(),
            }
        )


def _wg_dump_has_pub(pubkey: str) -> bool:
    rc, out, _ = docker_exec(
        AWG_CONTAINER, ["sh", "-lc", "wg show wg0 dump 2>/dev/null || true"]
    )
    if rc != 0 or not out:
        return False
    for line in out.splitlines()[1:]:
        if line.startswith(pubkey + "\t"):
            return True
    return False


def _wait_peer_in_dump(pubkey: str, attempts: int = 10, sleep_s: float = 0.2) -> None:
    """Коротко ждём, пока peer появится в wg dump (сгладить гонку после syncconf)."""
    for _ in range(attempts):
        if _wg_dump_has_pub(pubkey):
            return
        time.sleep(sleep_s)


def _sync_wg_conf_from_table() -> None:
    """Перестраивает wg0.conf на основе clientsTable (игнорируя deleted=True)."""
    clients = _read_clients_table()
    try:
        base_lines = docker_read_file(AWG_CONTAINER, AWG_CONFIG_PATH).splitlines()
    except Exception:
        base_lines = []

    # вырезаем все [Peer]
    conf_lines = []
    for line in base_lines:
        if line.strip().startswith("[Peer]"):
            break
        conf_lines.append(line)

    # добавляем ТОЛЬКО живые peers (deleted=True пропускаем)
    for client in clients:
        ai = client.get("addInfo") or {}
        if ai.get("deleted") is True:
            continue
        ud = client.get("userData", {}) or {}
        cid = client.get("clientId")
        ip = ud.get("ip")
        if not cid or not ip:
            continue
        conf_lines.append("")
        conf_lines.append("[Peer]")
        conf_lines.append(f"PublicKey = {cid}")
        if ud.get("psk"):
            conf_lines.append(f"PresharedKey = {ud['psk']}")
        conf_lines.append(f"AllowedIPs = {ip}/32")

    docker_write_file_atomic(
        AWG_CONTAINER, AWG_CONFIG_PATH, "\n".join(conf_lines) + "\n"
    )
    _apply_runtime_sync()


def create_profile(profile_data: dict) -> str:
    """Создаёт новый профиль AWG и добавляет его в clientsTable."""
    owner_tid = int(profile_data.get("owner_tid") or 0)
    name = (profile_data.get("name") or "").strip()

    if _name_in_use_for_owner(owner_tid, name):
        raise ValueError(f"Имя «{name}» уже занято среди ваших AWG-профилей")
    clients = _read_clients_table()
    f = facts()
    subnet = f.get("subnet") or "10.8.0.0/24"

    priv, pub = _gen_wg_keypair()
    psk = _gen_psk()
    ip = _get_next_ip(clients, subnet)

    client_uuid = str(uuidlib.uuid4())
    user_data = {
        "clientName": profile_data.get("name") or f"peer-{client_uuid[:8]}",
        "privateKey": priv,
        "psk": psk,
        "ip": ip,
        "created": _now_iso(),
        "creationDate": _ctime_like(),
    }
    # sanitize email-like tag if provided
    _raw_email = profile_data.get("email")
    _safe_email = None
    if isinstance(_raw_email, str):
        _safe_email = _raw_email.strip().replace(" ", "_") or None

    add_info = {
        "uuid": client_uuid,
        "owner_tid": profile_data.get("owner_tid"),
        "created_at": _now_iso(),
        "type": "awg",
        "email": _safe_email,
        "source": "bot",
        "notes": "",
    }
    record = {"clientId": pub, "userData": user_data, "addInfo": add_info}
    clients.append(record)
    _write_clients_table(clients)
    _sync_wg_conf_from_table()
    # коротко дождёмся появления пира в рантайме (сглаживает гонку)
    _wait_peer_in_dump(pub)
    return client_uuid


def find_profile_by_uuid(uuid: str) -> Optional[dict]:
    """Находит профиль по UUID."""
    return next((p for p in list_profiles() if p.get("uuid") == uuid), None)


def delete_profile_by_uuid(uuid: str) -> bool:
    """Удаляет профиль из clientsTable по UUID и пересобирает wg0.conf."""
    items = _read_clients_table()
    new_items = []
    changed = False
    for it in items:
        ai = it.get("addInfo", {}) or {}
        if ai.get("uuid") == uuid:
            changed = True
            continue
        new_items.append(it)
    if changed:
        _write_clients_table(new_items)
        _sync_wg_conf_from_table()
    return changed
