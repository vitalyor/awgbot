# src/core/repo_xray.py
# ЕДИНЫЙ слой предметной логики для XRAY.
# Работает напрямую с файлами внутри контейнера amnezia-xray через services.util.
from __future__ import annotations

import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from services.logger_setup import get_logger
from services.util import (
    docker_read_file,
    docker_write_file_atomic,
    XRAY_CONTAINER,
    XRAY_CONFIG_PATH,
)

log = get_logger("core.repo_xray")

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
    # "Mon Nov 10 08:35:32 2025"
    return datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S %Y")


def _read_json_from_container(path: str) -> Optional[Any]:
    """Прочитать JSON из контейнера, не падать, если файла нет/пустой/битый."""
    try:
        txt = docker_read_file(XRAY_CONTAINER, path)
    except Exception as e:
        log.warning({"event": "xray_json_read_fail", "path": path, "err": str(e)})
        return None
    if not txt or txt.strip() == "":
        return None
    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        log.warning({"event": "xray_json_decode_fail", "path": path, "err": str(e)})
        return None


def _write_json_list(path: str, items: List[Dict[str, Any]]) -> None:
    """Пишем ВСЕГДА как массив объектов (совместимо со сторонним UI)."""
    docker_write_file_atomic(
        XRAY_CONTAINER, path, json.dumps(items, ensure_ascii=False, indent=2)
    )


def _normalize_to_list(raw: Any, kind: str = "xray") -> List[Dict[str, Any]]:
    """
    Приводим к массиву:
      - dict {"id": {...}} -> [{"clientId": "id", ...}]
      - list оставляем как есть
    Заодно гарантируем userData.clientName/creationDate и addInfo.* каркас.
    """
    items: List[Dict[str, Any]] = []

    if isinstance(raw, dict):
        for cid, data in raw.items():
            entry = {"clientId": cid}
            if isinstance(data, dict):
                entry.update(data)
            items.append(entry)
    elif isinstance(raw, list):
        items = list(raw)
    elif raw in (None, ""):
        items = []
    else:
        # неизвестное — начнем с пустого
        items = []

    changed = False
    for it in items:
        # поля
        cid = it.get("clientId")
        ud = it.setdefault("userData", {}) or {}
        ai = it.setdefault("addInfo", {}) or {}

        # имя: приоритезируем clientName; если нет — синтез
        if "clientName" not in ud:
            name_src = ud.get("name") or ai.get("email")
            if not name_src:
                # короткий fallback по UUID
                name_src = f"XR-{str(cid)[:8]}"
            ud["clientName"] = name_src
            changed = True
        # убрать legacy userData.name
        if "name" in ud:
            ud.pop("name", None)
            changed = True

        if "creationDate" not in ud:
            ud["creationDate"] = _ctime_like()
            changed = True

        # addInfo каркас
        ai.setdefault("type", kind)
        ai.setdefault("uuid", cid)
        ai.setdefault("owner_tid", None)
        ai.setdefault("email", ai.get("email"))
        ai.setdefault("created_at", _now_iso())
        ai.setdefault("deleted", False)
        ai.setdefault("deleted_at", None)
        ai.setdefault("source", "bot")
        ai.setdefault("notes", "")

        it["userData"] = ud
        it["addInfo"] = ai

    # Возможна сортировка по created_at для детерминизма (не обязательно)
    # items.sort(key=lambda x: (x.get("addInfo", {}).get("created_at") or "", x.get("clientId") or ""))

    if changed:
        log.info({"event": "xray_clientsTable_normalized_in_mem", "added_fields": True})
    return items


# ===== public API for bot =====


def facts() -> Dict[str, Any]:
    server = _read_json_from_container(XRAY_CONFIG_PATH) or {}
    port = None
    inbounds = server.get("inbounds")
    if isinstance(inbounds, list) and inbounds:
        try:
            port = inbounds[0].get("port")
        except Exception:
            port = None

    ct_raw = _read_json_from_container(CLIENTS_TABLE)
    ct = _normalize_to_list(ct_raw, "xray")
    return {"listen_port": port, "count_profiles": len(ct)}


def list_profiles() -> List[Dict[str, Any]]:
    ct_raw = _read_json_from_container(CLIENTS_TABLE)
    items = _normalize_to_list(ct_raw, "xray")
    profiles: List[Dict[str, Any]] = []
    for it in items:
        cid = it.get("clientId")
        ud = it.get("userData", {}) or {}
        ai = it.get("addInfo", {}) or {}
        profiles.append(
            {
                "uuid": ai.get("uuid") or cid,
                "clientId": cid,
                "name": ud.get("clientName"),
                "email": ai.get("email"),
                "owner_tid": ai.get("owner_tid"),
                "deleted": bool(ai.get("deleted")),
                "addInfo": ai,
                "userData": ud,
            }
        )
    return profiles


def find_user(tg_id: int, name: str) -> Optional[Dict[str, Any]]:
    name = (name or "").strip()
    if not name:
        return None
    for p in list_profiles():
        if (
            (p.get("owner_tid") == tg_id)
            and (p.get("name") == name)
            and not p.get("deleted")
        ):
            return p
    return None


def add_user(tg_id: int, name: str) -> Dict[str, Any]:
    # читаем и нормализуем к списку
    raw = _read_json_from_container(CLIENTS_TABLE)
    items = _normalize_to_list(raw, "xray")

    import uuid as _uuid

    new_uuid = str(_uuid.uuid4())
    client_id = new_uuid  # XRAY: clientId == UUID

    record = {
        "clientId": client_id,
        "userData": {
            "clientName": name,
            "creationDate": _ctime_like(),
        },
        "addInfo": {
            "type": "xray",
            "uuid": new_uuid,
            "owner_tid": tg_id,
            "email": f"{tg_id}-{name}",
            "created_at": _now_iso(),
            "deleted": False,
            "deleted_at": None,
            "source": "bot",
            "notes": "",
        },
    }

    items.append(record)
    _write_json_list(CLIENTS_TABLE, items)

    return {
        "uuid": new_uuid,
        "clientId": client_id,
        "email": record["addInfo"]["email"],
        "name": name,
        "port": facts().get("listen_port"),
    }


def remove_user_by_name(tg_id: int, name: str) -> bool:
    raw = _read_json_from_container(CLIENTS_TABLE)
    items = _normalize_to_list(raw, "xray")

    changed = False
    for it in items:
        ud = it.get("userData", {}) or {}
        ai = it.get("addInfo", {}) or {}
        if (
            ai.get("owner_tid") == tg_id
            and ud.get("clientName") == name
            and not ai.get("deleted")
        ):
            ai["deleted"] = True
            ai["deleted_at"] = _now_iso()
            it["addInfo"] = ai
            changed = True

    if changed:
        _write_json_list(CLIENTS_TABLE, items)
    return changed


# совместимость некоторых вызовов (бот местами ожидает эти имена)
find_profile_by_uuid = lambda uuid_str: next(
    (p for p in list_profiles() if p.get("uuid") == uuid_str), None
)
create_profile = lambda d: add_user(int(d.get("owner_tid")), d.get("name")).get("uuid")
delete_profile_by_uuid = lambda u: False  # не используется для xray
