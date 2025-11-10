import json, re, uuid, urllib.parse, time
import os
from typing import Dict, Any, Optional
from services.util import (
    docker_read_file, docker_write_file_atomic, docker_exec, docker_restart, shq,
    XRAY_CONTAINER, XRAY_CONFIG_PATH, XRAY_INBOUND_INDEX, XRAY_CONNECT_HOST
)

# === Stage 0 backup/rotation policy ===
XRAY_BACKUP_KEEP = int(os.environ.get("XRAY_BACKUP_KEEP", "10"))
XRAY_DATEFMT = "%Y%m%d-%H%M%S"

# ========================= helpers to read/write config =========================

def _load_cfg() -> Dict[str, Any]:
    raw = docker_read_file(XRAY_CONTAINER, XRAY_CONFIG_PATH)
    return json.loads(raw)

def _save_cfg(cfg: Dict[str, Any]):
    """Atomically write server.json with timestamped backup and rotation (Stage 0)."""
    # 1) make timestamped backup (best-effort)
    try:
        docker_exec(
            XRAY_CONTAINER, "sh", "-lc",
            f"set -e; if [ -f {shq(XRAY_CONFIG_PATH)} ]; then cp -f {shq(XRAY_CONFIG_PATH)} {shq(XRAY_CONFIG_PATH)}.bak-$(date +%Y%m%d-%H%M%S); fi; true"
        )
    except Exception:
        pass
    # 2) rotate old backups (keep only XRAY_BACKUP_KEEP most recent)
    try:
        docker_exec(
            XRAY_CONTAINER, "sh", "-lc",
            f"ls -1 {shq(XRAY_CONFIG_PATH)}.bak-* 2>/dev/null | sort -r | awk 'NR>{XRAY_BACKUP_KEEP}{{print $0}}' | xargs -r rm -f 2>/dev/null || true"
        )
    except Exception:
        pass
    # 3) atomic write
    docker_write_file_atomic(
        XRAY_CONTAINER, XRAY_CONFIG_PATH, json.dumps(cfg, ensure_ascii=False, indent=2)
    )

def _get_inbound(cfg: Dict[str, Any]) -> Dict[str, Any]:
    inb = cfg.get("inbounds", [])
    if not inb or XRAY_INBOUND_INDEX >= len(inb):
        raise RuntimeError("Не найден inbound XRay.")
    ib = inb[XRAY_INBOUND_INDEX]
    if ib.get("protocol") != "vless":
        raise RuntimeError(f"Ожидался inbound protocol=vless, а получено: {ib.get('protocol')!r}")
    return ib

# === Suspend / Resume (без смены UUID) ===

def _find_client(ib: Dict[str, Any], email: str):
    clients = ib.get("settings", {}).get("clients", []) or []
    for i, c in enumerate(clients):
        if (c.get("email") or "") == email:
            return i, c
    return -1, None

def suspend_user_by_name(tg_id: int, name: str) -> Optional[Dict[str, str]]:
    """
    Временно отключает клиента: удаляет его из server.json, возвращает snapshot {"uuid","flow","email"}.
    Позже можно восстановить resume_user_by_name(...) с тем же UUID.
    """
    email = _email(tg_id, name)
    cfg = _load_cfg()
    ib = _get_inbound(cfg)
    idx, cli = _find_client(ib, email)
    if idx < 0 or not cli:
        return None
    snap = {"uuid": cli.get("id", ""), "flow": cli.get("flow", "xtls-rprx-vision"), "email": email}
    # удаляем из списка
    clients = ib.setdefault("settings", {}).setdefault("clients", [])
    ib["settings"]["clients"] = clients[:idx] + clients[idx+1:]
    _save_cfg(cfg)
    docker_restart(XRAY_CONTAINER)
    return snap

def resume_user_by_name(tg_id: int, name: str, uuid: str, flow: Optional[str] = None) -> bool:
    """
    Возвращает клиента обратно в server.json с тем же UUID (и flow, если указан).
    Если уже есть — ничего не делаем.
    """
    email = _email(tg_id, name)
    cfg = _load_cfg()
    ib = _get_inbound(cfg)
    idx, cli = _find_client(ib, email)
    if idx >= 0 and cli:
        # уже присутствует
        return True
    flow_to_use = (flow or _flow(ib) or "xtls-rprx-vision")
    clients = ib.setdefault("settings", {}).setdefault("clients", [])
    clients.append({"id": uuid, "flow": flow_to_use, "email": email})
    _save_cfg(cfg)
    docker_restart(XRAY_CONTAINER)
    return True

def _flow(ib: Dict[str, Any]) -> str:
    clients = ib.get("settings", {}).get("clients", [])
    return clients[0].get("flow", "xtls-rprx-vision") if clients else "xtls-rprx-vision"

def _reality_pbk(ib: Dict[str, Any]) -> str:
    """
    Возвращает publicKey для Reality:
    - сначала пытается взять rs.publicKey
    - если пусто, и есть privateKey — считает через `xray x25519 -i`
    """
    rs = ib.get("streamSettings", {}).get("realitySettings", {})
    # 1) уже есть publicKey
    pbk = (rs.get("publicKey") or "").strip()
    if pbk:
        return pbk
    # 2) считаем из privateKey
    priv = (rs.get("privateKey") or "").strip()
    if not priv:
        return ""
    out = docker_exec(
        XRAY_CONTAINER, "sh", "-lc",
        f"xray x25519 -i {shq(priv)} 2>/dev/null || true"
    )
    m = re.search(r"Public key:\s*([A-Za-z0-9_\-+/=]+)", out or "")
    return (m.group(1).strip() if m else "")

def _email(tg_id: int, name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return f"{tg_id}-{safe}"

def _detect_host() -> str:
    """
    XRAY_CONNECT_HOST из ENV, иначе — внешний IP (как в твоём shell):
    curl ifconfig.me/ip -> строгий IPv4; если нет — hostname -I (первый адрес).
    Выполняется ВНУТРИ контейнера xray.
    """
    if XRAY_CONNECT_HOST:
        return XRAY_CONNECT_HOST

    # ifconfig.me (строгий IPv4)
    try:
        cmd = r"""(command -v curl >/dev/null && (curl -s --max-time 3 ifconfig.me/ip || curl -s --max-time 3 ifconfig.me) \
                || wget -qO- --timeout=3 ifconfig.me/ip || wget -qO- --timeout=3 ifconfig.me) 2>/dev/null \
                | tr -d '\r' | sed -n 's/.*\b\([0-9]\{1,3\}\(\.[0-9]\{1,3\}\)\{3\}\)\b.*/\1/p' | head -n1"""
        out = docker_exec(XRAY_CONTAINER, "sh", "-lc", cmd).strip()
        if out:
            return out
    except Exception:
        pass

    # hostname -I (первый адрес)
    try:
        out = docker_exec(XRAY_CONTAINER, "sh", "-lc", "hostname -I 2>/dev/null | awk '{print $1}'").strip()
        if out:
            return out
    except Exception:
        pass

    return ""  # крайний случай

# ========================= client config & uri =========================

def _client_json(host: str, port: int, uid: str, sni: str, pbk: str, sid: str, flow: str) -> Dict[str, Any]:
    return {
        "inbounds": [
            {"listen": "127.0.0.1", "port": 10808, "protocol": "socks", "settings": {"udp": True}}
        ],
        "log": {"loglevel": "error"},
        "outbounds": [
            {
                "protocol": "vless",
                "settings": {
                    "vnext": [{
                        "address": host,
                        "port": port,
                        "users": [{"id": uid, "flow": flow, "encryption": "none"}]
                    }]
                },
                "streamSettings": {
                    "network": "tcp",
                    "security": "reality",
                    "realitySettings": {
                        "fingerprint": "chrome",
                        "serverName": sni,
                        "publicKey": pbk,
                        "shortId": sid,
                        "spiderX": ""
                    }
                }
            }
        ],
    }

def _last_cfg_str(host: str, port: int, uid: str, sni: str, pbk: str, sid: str, flow: str) -> str:
    return json.dumps(_client_json(host, port, uid, sni, pbk, sid, flow), ensure_ascii=False, indent=4)

def _vless_uri(
    host: str, port: int, uid: str, sni: str, sid: str, flow: str, pbk: str, tag: Optional[str] = None
) -> str:
    """
    Универсальный VLESS REALITY URI “как в шеле”:
    vless://<uuid>@<host>:<port>?encryption=none&security=reality&sni=<sni>&fp=chrome&pbk=<pbk>&sid=<sid>&type=tcp&flow=<flow>#<tag>
    """
    fp = "chrome"
    net_type = "tcp"
    flow = flow or "xtls-rprx-vision"
    qs = "&".join([
        "encryption=none",
        "security=reality",
        f"sni={sni}",
        f"fp={fp}",
        f"pbk={pbk}",
        f"sid={sid}",
        f"type={net_type}",
        f"flow={flow}",
    ])
    label = urllib.parse.quote((tag if tag is not None else host) or "", safe="")
    return f"vless://{uid}@{host}:{port}?{qs}#{label}"

# ========================= public API =========================

def add_user(tg_id: int, name: str) -> Dict[str, str]:
    # XRAY_CONNECT_HOST приоритетен, но если пуст — пробуем autodetect (как в шеле)
    host = XRAY_CONNECT_HOST or _detect_host()
    if not host:
        raise RuntimeError("Не задан XRAY_CONNECT_HOST и не удалось autodetect HOST.")

    cfg = _load_cfg()
    ib = _get_inbound(cfg)
    flow = _flow(ib)
    port = int(ib.get("port", 443))
    rs = ib.get("streamSettings", {}).get("realitySettings", {})
    sni = (rs.get("serverNames") or ["www.cloudflare.com"])[0]
    sid = (rs.get("shortIds") or [""])[0]
    pbk = _reality_pbk(ib)

    uid = str(uuid.uuid4())
    email = _email(tg_id, name)

    clients = ib.setdefault("settings", {}).setdefault("clients", [])
    # защита от дублей email (редкий, но неприятный кейс)
    if any(c.get("email") == email for c in clients):
        raise RuntimeError("Профиль с таким именем уже существует в Xray (дублирующий email).")

    clients.append({"id": uid, "flow": flow, "email": email})
    _save_cfg(cfg)
    docker_restart(XRAY_CONTAINER)

    client_obj = _client_json(host, port, uid, sni, pbk, sid, flow)
    client_json = json.dumps(client_obj, ensure_ascii=False, indent=2)
    last_cfg_str = _last_cfg_str(host, port, uid, sni, pbk, sid, flow)

    # тег в URI — host (как в твоём shell)
    uri = _vless_uri(host, port, uid, sni, sid, flow, pbk, tag=host)

    return {
        "uuid": uid,
        "email": email,
        "uri": uri,
        "client_json": client_json,
        "last_config_str": last_cfg_str,
        "port": port,
        "sni": sni
    }

def remove_user_by_name(tg_id: int, name: str) -> bool:
    email = _email(tg_id, name)
    cfg = _load_cfg()
    ib = _get_inbound(cfg)
    clients = ib.get("settings", {}).get("clients", [])
    new_clients = [c for c in clients if c.get("email") != email]
    if len(new_clients) == len(clients):
        return False
    ib["settings"]["clients"] = new_clients
    _save_cfg(cfg)
    docker_restart(XRAY_CONTAINER)
    return True

def find_user(tg_id: int, name: str) -> Optional[Dict[str, str]]:
    # XRAY_CONNECT_HOST приоритетен, но если пуст — autodetect как в шеле
    host = XRAY_CONNECT_HOST or _detect_host()
    if not host:
        return None

    email = _email(tg_id, name)
    cfg = _load_cfg()
    ib = _get_inbound(cfg)
    port = int(ib.get("port", 443))
    rs = ib.get("streamSettings", {}).get("realitySettings", {})
    sni = (rs.get("serverNames") or ["www.cloudflare.com"])[0]
    sid = (rs.get("shortIds") or [""])[0]
    pbk = _reality_pbk(ib)

    for c in ib.get("settings", {}).get("clients", []):
        if c.get("email") == email:
            flow = c.get("flow", "xtls-rprx-vision")
            client_json = json.dumps(_client_json(host, port, c["id"], sni, pbk, sid, flow), ensure_ascii=False, indent=2)
            last_cfg_str = _last_cfg_str(host, port, c["id"], sni, pbk, sid, flow)
            uri = _vless_uri(host, port, c["id"], sni, sid, flow, pbk, tag=host)
            return {
                "uuid": c["id"],
                "email": email,
                "uri": uri,
                "client_json": client_json,
                "last_config_str": last_cfg_str,
                "port": port,
                "sni": sni
            }
    return None

def has_user(tg_id: int, name: str) -> bool:
    """
    Быстрая проверка: есть ли клиент с email=<tgid>-<name> в server.json.
    """
    email = _email(tg_id, name)
    try:
        cfg = _load_cfg()
        ib = _get_inbound(cfg)
        for c in ib.get("settings", {}).get("clients", []):
            if (c.get("email") or "") == email:
                return True
    except Exception:
        pass
    return False


def get_status(tg_id: int, name: str) -> str:
    """
    Возвращает 'active' если клиент найден в server.json, иначе 'absent'.
    """
    return "active" if has_user(tg_id, name) else "absent"

def is_bot_email(email: str) -> bool:
    """True если email соответствует формату бота (<tgid>-<name>)."""
    if not email or "-" not in email:
        return False
    left, _ = email.split("-", 1)
    return left.isdigit()



def list_all() -> list[dict]:
    """
    Возвращает список всех клиентов Xray вида:
    [{"tid": int, "name": str, "uuid": str, "sni": str, "port": int, "flow": str}, ...]
    Источник — текущий server.json (читаем через _load_cfg()).
    """
    try:
        cfg = _load_cfg()
        inbounds = cfg.get("inbounds", []) or []
        if not inbounds or XRAY_INBOUND_INDEX >= len(inbounds):
            return []

        ib = inbounds[XRAY_INBOUND_INDEX]
        port = ib.get("port")
        # SNI берём из realitySettings; flow по умолчанию — как у первого клиента/inbound
        rs = (ib.get("streamSettings") or {}).get("realitySettings") or {}
        sni = (rs.get("serverNames") or [""])[0] if isinstance(rs.get("serverNames"), list) else (rs.get("serverNames") or "")

        default_flow = _flow(ib)
        clients = (ib.get("settings") or {}).get("clients") or []

        res = []
        for c in clients:
            email = c.get("email", "")
            uuid = c.get("id", "")
            flow_val = c.get("flow", default_flow)

            source = "foreign"
            tid, pname = 0, email
            if email and "-" in email:
                parts = email.split("-", 1)
                if len(parts) == 2 and parts[0].isdigit():
                    source = "bot"
                    tid = int(parts[0])
                    pname = parts[1]

            res.append({
                "tid": tid,
                "name": pname,
                "uuid": uuid,
                "sni": sni,
                "port": port,
                "flow": flow_val,
                "source": source,
                "email": email,
            })
        return res
    except Exception as e:
        import logging
        logging.getLogger("awgbot").warning({"event": "xray_list_all_fail", "err": str(e)})
        return []


# ========================= (опционально) диагностика =========================

def universal_uri_diagnostic() -> Dict[str, Any]:
    """
    Возвращает распарсенные ключевые поля из ПЕРВОГО inbound и итоговый URI,
    без привязки к конкретному пользователю (UUID берётся из первого клиента).
    Полезно для самопроверки конфигурации “как в шеле”.
    """
    try:
        cfg = _load_cfg()
        ib = _get_inbound(cfg)
        port = int(ib.get("port", 443))
        proto = ib.get("protocol", "")
        flow = _flow(ib)
        rs = ib.get("streamSettings", {}).get("realitySettings", {})
        sni = (rs.get("serverNames") or [""])[0]
        sid = (rs.get("shortIds") or [""])[0]
        pbk = _reality_pbk(ib)
        clients = ib.get("settings", {}).get("clients", [])
        uid = (clients[0] or {}).get("id") if clients else ""
        host = XRAY_CONNECT_HOST or _detect_host()

        ok = (proto == "vless" and uid and host and port and pbk and sid and sni)
        uri = _vless_uri(host, port, uid, sni, sid, flow, pbk, tag=host) if ok else ""

        return {
            "ok": bool(ok),
            "proto": proto, "port": port, "sni": sni, "sid": sid,
            "publicKey": pbk, "uuid": uid, "host": host, "flow": flow,
            "uri": uri,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ========================= точечный апдейт клиента по email =========================

def ensure_user_uuid_flow(tg_id: int, name: str, uuid_val: str, flow_val: Optional[str] = None) -> bool:
    """
    Обновляет существующего клиента (uuid/flow) по email=<tgid>-<name>.
    Если клиента нет — добавляет.
    Возвращает True, если изменения применены успешно.
    """
    try:
        email = _email(tg_id, name)
        cfg = _load_cfg()
        ib = _get_inbound(cfg)
        clients = ib.setdefault("settings", {}).setdefault("clients", [])
        idx, cli = _find_client(ib, email)

        if idx >= 0 and cli:
            changed = False
            if cli.get("id") != uuid_val:
                cli["id"] = uuid_val
                changed = True
            if flow_val and cli.get("flow") != flow_val:
                cli["flow"] = flow_val
                changed = True
            if changed:
                _save_cfg(cfg)
                docker_restart(XRAY_CONTAINER)
            return True

        # не найден — добавляем новый
        flow_to_use = flow_val or _flow(ib) or "xtls-rprx-vision"
        clients.append({"id": uuid_val, "flow": flow_to_use, "email": email})
        _save_cfg(cfg)
        docker_restart(XRAY_CONTAINER)
        return True
    except Exception as e:
        import logging
        logging.getLogger("awgbot").warning({"event": "xray_ensure_user_uuid_flow_fail", "err": str(e)})
        return False