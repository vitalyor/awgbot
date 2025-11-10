# src/core/repo_xray.py
# Предметная логика для XRay. Прямая работа с файлами в контейнере amnezia-xray.
from __future__ import annotations

import json
import uuid as uuidlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from services.logger_setup import get_logger
from services.util import (
    docker_exec,
    docker_read_file,
    docker_write_file_atomic,
    XRAY_CONTAINER,
)

log = get_logger("core.repo_xray")

# Локальные константы путей внутри контейнера XRAY
XRAY_SERVER_JSON = "/opt/amnezia/xray/server.json"
CLIENTS_TABLE = "/opt/amnezia/xray/clientsTable"


# ===== helpers =====


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _ctime_like() -> str:
    return datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S %Y")


def _read_json(container: str, path: str, default):
    try:
        txt = docker_read_file(container, path)
        return json.loads(txt) if txt.strip() else default
    except Exception as e:
        log.warning({"msg": f"Failed to read JSON {path}: {e}"})
        return default


def _write_json(container: str, path: str, obj) -> None:
    docker_write_file_atomic(
        container, path, json.dumps(obj, ensure_ascii=False, indent=2)
    )


def _read_clients_table() -> List[Dict[str, Any]]:
    raw = _read_json(XRAY_CONTAINER, CLIENTS_TABLE, [])
    if isinstance(raw, dict):
        # Защита от старого формата
        raw = [
            {"clientId": k, **(v if isinstance(v, dict) else {})}
            for k, v in raw.items()
        ]
    if not isinstance(raw, list):
        raw = []

    changed = False
    for it in raw:
        ud = it.setdefault("userData", {}) or {}
        ai = it.setdefault("addInfo", {}) or {}
        cid = it.get("clientId", "")

        if "clientName" not in ud:
            ud["clientName"] = f"XRAY-{str(cid)[:8]}"
            changed = True
        if "creationDate" not in ud:
            ud["creationDate"] = _ctime_like()
            changed = True

        ai.setdefault("type", "xray")
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
            _write_json(XRAY_CONTAINER, CLIENTS_TABLE, raw)
        except Exception as e:
            log.warning({"event": "xray_clientsTable_autofix_failed", "err": str(e)})
    return raw


def _write_clients_table(items: List[Dict[str, Any]]) -> None:
    _write_json(XRAY_CONTAINER, CLIENTS_TABLE, items)


def _listen_port() -> Optional[int]:
    srv = _read_json(XRAY_CONTAINER, XRAY_SERVER_JSON, {})
    try:
        inb = (srv.get("inbounds") or [])[0]
        return int(inb.get("port"))
    except Exception:
        return None


def facts() -> dict:
    return {"listen_port": _listen_port(), "count_profiles": len(_read_clients_table())}


def list_profiles() -> List[dict]:
    items = _read_clients_table()
    out: List[Dict[str, Any]] = []
    for it in items:
        cid = it.get("clientId", "")
        ud = it.get("userData", {}) or {}
        ai = it.get("addInfo", {}) or {}
        out.append(
            {
                "uuid": ai.get("uuid") or cid,
                "clientId": cid,
                "name": ud.get("clientName"),
                "owner_tid": ai.get("owner_tid"),
                "userData": ud,
                "addInfo": ai,
            }
        )
    return out


def _name_in_use_for_owner(owner_tid: int, name: str) -> bool:
    if not name:
        return False
    name_norm = " ".join(name.split()).lower()
    for p in list_profiles():
        if p.get("owner_tid") != owner_tid:
            continue
        if (p.get("name") or "").strip().lower() == name_norm:
            return True
    return False


def add_user(tg_id: int, name: str) -> Dict[str, Any]:
    name = (name or "").strip()
    if _name_in_use_for_owner(int(tg_id), name):
        raise ValueError(f"Имя «{name}» уже занято среди ваших XRAY-профилей")

    items = _read_clients_table()
    new_uuid = str(uuidlib.uuid4())
    client_id = new_uuid  # для XRAY clientId == uuid

    record = {
        "clientId": client_id,
        "userData": {
            "clientName": (name or f"XRAY-{new_uuid[:8]}"),
            "creationDate": _ctime_like(),
        },
        "addInfo": {
            "type": "xray",
            "uuid": new_uuid,
            "owner_tid": int(tg_id),
            "email": f"{int(tg_id)}-{(name or '').strip().replace(' ', '_')}",
            "created_at": _now_iso(),
            # Полей deleted/deleted_at больше не используем — храним только живые записи.
            "source": "bot",
            "notes": "",
        },
    }

    items.append(record)
    _write_clients_table(items)
    return {
        "uuid": new_uuid,
        "clientId": client_id,
        "email": record["addInfo"]["email"],
        "name": record["userData"]["clientName"],
        "port": _listen_port(),
    }


def find_user(tg_id: int, name: str) -> Optional[dict]:
    name_norm = (name or "").strip().lower()
    for p in list_profiles():
        if p.get("owner_tid") != int(tg_id):
            continue
        if (p.get("name") or "").strip().lower() == name_norm:
            return p
    return None


def remove_user_by_name(tg_id: int, name: str) -> bool:
    name_norm = (name or "").strip().lower()
    items = _read_clients_table()
    new_items = []
    changed = False
    for it in items:
        ai = it.get("addInfo", {}) or {}
        ud = it.get("userData", {}) or {}
        if (
            ai.get("owner_tid") == int(tg_id)
            and (ud.get("clientName") or "").strip().lower() == name_norm
        ):
            changed = True
            continue  # физическое удаление
        new_items.append(it)
    if changed:
        _write_clients_table(new_items)
    return changed


# Совместимость для бота
def find_profile_by_uuid(uuid_str: str) -> Optional[dict]:
    return next((p for p in list_profiles() if p.get("uuid") == uuid_str), None)


def create_profile(d: dict) -> str:
    res = add_user(int(d.get("owner_tid")), d.get("name"))
    return res.get("uuid")


def delete_profile_by_uuid(_uuid: str) -> bool:
    # Удаление по uuid
    items = _read_clients_table()
    new_items = []
    changed = False
    for it in items:
        ai = it.get("addInfo", {}) or {}
        if ai.get("uuid") == _uuid:
            changed = True
            continue
        new_items.append(it)
    if changed:
        _write_clients_table(new_items)
    return changed


# ===== экспорт клиентского «конфига» =====


def render_client_config(uuid: str) -> str:
    """Отдаёт JSON-сниппет для клиента XRAY (vless), без сетевого хоста."""
    prof = find_profile_by_uuid(uuid)
    if not prof:
        raise ValueError("Profile not found")

    port = _listen_port()
    email = (prof.get("addInfo") or {}).get("email")
    cid = prof.get("clientId")

    obj = {
        "protocol": "vless",
        "uuid": cid,
        "email": email,
        "server": {"host": "<SERVER_HOST>", "port": port or 443},
        "transport": {"type": "tcp", "security": "tls"},
    }
    return json.dumps(obj, ensure_ascii=False, indent=2) + "\n"
