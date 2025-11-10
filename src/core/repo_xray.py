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
    """Возвращает ISO-8601 UTC без микросекунд, с суффиксом 'Z'."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _ctime_like() -> str:
    """Формат времени в стиле: Mon Nov 10 08:35:32 2025."""
    return datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S %Y")


def _read_json_from_container(path: str) -> Optional[Any]:
    """Прочитать JSON из контейнера, не падать, если файла нет/битый."""
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
    """Пишем строго как массив объектов (совместимо со сторонним UI)."""
    payload = json.dumps(items, ensure_ascii=False, indent=2)
    docker_write_file_atomic(XRAY_CONTAINER, path, payload)


def _normalize_to_list(raw: Any, kind: str = "xray") -> List[Dict[str, Any]]:
    """
    Приводим данные к массиву и гарантируем наличие обязательных полей.
    Поддерживаем две формы исходника:
      - dict { "<clientId>": { ... }, ... }
      - list [ { "clientId": "...", ... }, ... ]
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
        items = []

    changed = False
    for it in items:
        cid = it.get("clientId")
        ud = it.setdefault("userData", {}) or {}
        ai = it.setdefault("addInfo", {}) or {}

        # Гарантируем clientName
        if "clientName" not in ud:
            name_src = ai.get("email") or f"XRAY-{str(cid)[:8]}"
            ud["clientName"] = name_src
            changed = True

        # Гарантируем creationDate
        if "creationDate" not in ud:
            ud["creationDate"] = _ctime_like()
            changed = True

        # Обязательные поля addInfo
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

    if changed:
        log.info({"event": "xray_clientsTable_normalized_in_mem", "added_fields": True})

    return items


def _name_in_use_for_owner(owner_tid: int, name: str) -> bool:
    if not name:
        return False
    name_norm = " ".join(name.split()).lower()
    for p in list_profiles():
        if p.get("owner_tid") != owner_tid:
            continue
        if p.get("deleted"):
            continue
        # XRAY-репозиторий проверяет только XRAY-профили (этот файл)
        pname = (p.get("name") or "").strip().lower()
        if pname == name_norm:
            return True
    return False


# ===== public API for bot =====


def facts() -> Dict[str, Any]:
    """Основные сведения о сервере XRAY (порт + кол-во профилей)."""
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
    """Возвращает список профилей XRAY в нормализованном виде."""
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
    """Поиск профиля по Telegram ID и имени (активного, не deleted)."""
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
    """Создание нового XRAY пользователя (запись только в clientsTable)."""
    # нормализация имени (обрежем пробелы)
    name = (name or "").strip()
    # запрет дубликатов для одного owner_tid
    if _name_in_use_for_owner(int(tg_id), name):
        raise ValueError(f"Имя «{name}» уже занято среди ваших XRAY-профилей")
    raw = _read_json_from_container(CLIENTS_TABLE)
    items = _normalize_to_list(raw, "xray")

    import uuid as _uuid

    new_uuid = str(_uuid.uuid4())
    client_id = new_uuid  # XRAY: clientId == UUID

    record = {
        "clientId": client_id,
        "userData": {
            # сохраняем человекочитаемое имя
            "clientName": (name or f"XRAY-{new_uuid[:8]}"),
            "creationDate": _ctime_like(),
        },
        "addInfo": {
            "type": "xray",
            "uuid": new_uuid,
            "owner_tid": tg_id,
            "email": f"{tg_id}-{(name or '').strip().replace(' ', '_')}",
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
    """Полное удаление записи XRAY из clientsTable по имени и owner_tid.
    Раньше было soft-delete (deleted=true), теперь — жёсткое удаление.
    Возвращает True, если хотя бы одна запись была удалена.
    """
    raw = _read_json_from_container(CLIENTS_TABLE)
    items = _normalize_to_list(raw, "xray")

    name = (name or "").strip()
    if not name:
        return False

    before = len(items)
    kept: List[Dict[str, Any]] = []
    for it in items:
        ud = it.get("userData", {}) or {}
        ai = it.get("addInfo", {}) or {}
        # сохраняем только те, кто НЕ совпадает с (owner_tid, name)
        if not (ai.get("owner_tid") == tg_id and (ud.get("clientName") or "").strip() == name):
            kept.append(it)

    if len(kept) != before:
        _write_json_list(CLIENTS_TABLE, kept)
        return True
    return False


# ===== совместимость с интерфейсом бота =====


def find_profile_by_uuid(uuid_str: str):
    return next((p for p in list_profiles() if p.get("uuid") == uuid_str), None)


def create_profile(d: dict) -> str:
    """
    Совместимый вход: {'owner_tid': <int|str|None>, 'name': <str|None>}
    Возвращает uuid созданного XRAY-клиента.
    """
    raw_tid = d.get("owner_tid", 0)
    try:
        tg_id = (
            int(raw_tid) if raw_tid is not None and str(raw_tid).strip() != "" else 0
        )
    except Exception:
        tg_id = 0
    name = (d.get("name") or "").strip()
    return add_user(tg_id, name).get("uuid")


def delete_profile_by_uuid(uuid_str: str) -> bool:
    """Полное удаление XRAY-профиля по UUID/ClientId из clientsTable.
    Совместимость с интерфейсом бота: возвращает True, если удалили хотя бы одну запись.
    """
    raw = _read_json_from_container(CLIENTS_TABLE)
    items = _normalize_to_list(raw, "xray")

    before = len(items)
    kept: List[Dict[str, Any]] = []
    for it in items:
        ai = it.get("addInfo", {}) or {}
        cid = it.get("clientId")
        # считаем совпадением либо addInfo.uuid, либо сам clientId
        if (ai.get("uuid") == uuid_str) or (cid == uuid_str):
            continue  # удаляем
        kept.append(it)

    if len(kept) != before:
        _write_json_list(CLIENTS_TABLE, kept)
        return True
    return False
