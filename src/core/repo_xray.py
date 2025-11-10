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
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json_from_container(path: str) -> Optional[Any]:
    """Прочитать JSON из контейнера, не падать, если файла нет/пустой/битый."""
    try:
        txt = docker_read_file(XRAY_CONTAINER, path)
    except Exception as e:
        log.warning({
            "event": "xray_json_read_fail",
            "path": path,
            "err": str(e),
        })
        return None

    if not txt or txt.strip() == "":
        return None

    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        log.warning({
            "event": "xray_json_decode_fail",
            "path": path,
            "err": str(e),
        })
        return None


def _write_json_to_container(path: str, data: Any) -> None:
    docker_write_file_atomic(
        XRAY_CONTAINER, path, json.dumps(data, ensure_ascii=False, indent=2)
    )


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

    ct = _read_json_from_container(CLIENTS_TABLE)
    if isinstance(ct, list):
        count_profiles = len(ct)
    elif isinstance(ct, dict):
        count_profiles = len(ct)
    else:
        count_profiles = 0

    return {"listen_port": port, "count_profiles": count_profiles}


def list_profiles() -> List[Dict[str, Any]]:
    ct = _read_json_from_container(CLIENTS_TABLE)
    profiles: List[Dict[str, Any]] = []
    if isinstance(ct, list):
        # legacy array
        for item in ct:
            cid = item.get("clientId")
            ud = item.get("userData", {}) or {}
            ai = item.get("addInfo", {}) or {}
            profiles.append(
                {
                    "uuid": ai.get("uuid") or cid,
                    "clientId": cid,
                    "name": ud.get("name") or ud.get("clientName"),
                    "email": ud.get("email") or ai.get("email"),
                    "owner_tid": ai.get("owner_tid"),
                    "deleted": bool(ai.get("deleted") or item.get("deleted")),
                    "addInfo": ai,
                    "userData": ud,
                }
            )
    elif isinstance(ct, dict):
        for cid, data in ct.items():
            data = data or {}
            ud = data.get("userData", {}) or {}
            ai = data.get("addInfo", {}) or {}
            profiles.append(
                {
                    "uuid": ai.get("uuid") or cid,
                    "clientId": cid,
                    "name": ud.get("name") or ud.get("clientName"),
                    "email": ud.get("email") or ai.get("email"),
                    "owner_tid": ai.get("owner_tid"),
                    "deleted": bool(ai.get("deleted") or data.get("deleted")),
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
    # clientsTable — источник истины по XRAY профилям
    ct = _read_json_from_container(CLIENTS_TABLE)
    if ct is None:
        ct = {}
    if isinstance(ct, list):
        # миграция legacy списка к dict
        migrated: Dict[str, Any] = {}
        for it in ct:
            cid = it.get("clientId") or it.get("uuid")
            if cid:
                migrated[cid] = {k: v for k, v in it.items() if k != "clientId"}
        ct = migrated

    import uuid as _uuid

    new_uuid = str(_uuid.uuid4())
    client_id = new_uuid  # для XRAY clientId == UUID

    record = {
        "userData": {
            "name": name,
            "email": f"{tg_id}-{name}",
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
    ct[client_id] = record
    _write_json_to_container(CLIENTS_TABLE, ct)

    return {
        "uuid": new_uuid,
        "clientId": client_id,
        "email": record["userData"]["email"],
        "name": name,
        "port": facts().get("listen_port"),
    }


def remove_user_by_name(tg_id: int, name: str) -> bool:
    ct = _read_json_from_container(CLIENTS_TABLE)
    if not isinstance(ct, (dict, list)):
        return False

    changed = False
    if isinstance(ct, list):
        for it in ct:
            ai = it.get("addInfo", {}) or {}
            ud = it.get("userData", {}) or {}
            uname = ud.get("name") or ud.get("clientName")
            if ai.get("owner_tid") == tg_id and uname == name and not ai.get("deleted"):
                ai["deleted"] = True
                ai["deleted_at"] = _now_iso()
                it["addInfo"] = ai
                changed = True
        if changed:
            _write_json_to_container(CLIENTS_TABLE, ct)
        return changed

    # dict
    for cid, data in ct.items():
        data = data or {}
        ai = data.get("addInfo", {}) or {}
        ud = data.get("userData", {}) or {}
        uname = ud.get("name") or ud.get("clientName")
        if ai.get("owner_tid") == tg_id and uname == name and not ai.get("deleted"):
            ai["deleted"] = True
            ai["deleted_at"] = _now_iso()
            data["addInfo"] = ai
            ct[cid] = data
            changed = True
            break

    if changed:
        _write_json_to_container(CLIENTS_TABLE, ct)
    return changed


# совместимость некоторых вызовов
find_profile_by_uuid = lambda uuid_str: next(
    (p for p in list_profiles() if p.get("uuid") == uuid_str), None
)
create_profile = lambda d: add_user(int(d.get("owner_tid")), d.get("name")).get("uuid")
delete_profile_by_uuid = lambda u: False  # не используется для xray
