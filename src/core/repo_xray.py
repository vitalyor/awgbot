import os
import json
import subprocess
import datetime
import uuid
import shutil
import tempfile

from services.logger_setup import get_logger
log = get_logger("core.repo_xray")

XRAY_DIR = "/opt/amnezia/xray"
SERVER_JSON = os.path.join(XRAY_DIR, "server.json")
CLIENTS_TABLE = os.path.join(XRAY_DIR, "clientsTable")


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.warning(f"Failed to read JSON {path}: {e}")
        return None


def _write_json_atomic(path, data):
    log.debug(f"Writing JSON atomically to {path}")
    dir_name = os.path.dirname(path)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_name, encoding="utf-8") as tf:
        json.dump(data, tf, indent=2, ensure_ascii=False)
        tempname = tf.name
    os.replace(tempname, path)


def _backup_file(path):
    if os.path.exists(path):
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        backup_path = f"{path}.bak-{timestamp}"
        shutil.copy2(path, backup_path)
        log.info(f"Backup created: {backup_path}")


def list_profiles():
    clients = _read_json(CLIENTS_TABLE)
    if not clients:
        return []
    profiles = []
    for clientId, data in clients.items():
        addInfo = data.get("addInfo", {})
        userData = data.get("userData", {})
        profile = {
            "uuid": addInfo.get("uuid"),
            "clientId": clientId,
            "name": userData.get("name"),
            "email": userData.get("email"),
            "deleted": data.get("deleted", False),
            "owner_tid": addInfo.get("owner_tid"),
        }
        profiles.append(profile)
    return profiles


def find_profile_by_uuid(uuid_str):
    clients = _read_json(CLIENTS_TABLE)
    if not clients:
        return None
    for clientId, data in clients.items():
        addInfo = data.get("addInfo", {})
        if addInfo.get("uuid") == uuid_str:
            userData = data.get("userData", {})
            profile = {
                "uuid": addInfo.get("uuid"),
                "clientId": clientId,
                "name": userData.get("name"),
                "email": userData.get("email"),
                "deleted": data.get("deleted", False),
                "owner_tid": addInfo.get("owner_tid"),
            }
            return profile
    return None


def create_profile(profile_data):
    log.info(f"Creating new Xray profile for {profile_data.get('name')} (owner_tid={profile_data.get('owner_tid')})")
    clients = _read_json(CLIENTS_TABLE)
    if clients is None:
        clients = {}

    new_uuid = str(uuid.uuid4())
    clientId = profile_data.get("clientId") or str(uuid.uuid4())
    userData = {
        "name": profile_data.get("name"),
        "email": profile_data.get("email"),
    }
    addInfo = {
        "uuid": new_uuid,
        "owner_tid": profile_data.get("owner_tid"),
    }
    new_profile = {
        "userData": userData,
        "addInfo": addInfo,
    }
    clients[clientId] = new_profile
    _write_json_atomic(CLIENTS_TABLE, clients)

    log.info(f"Created Xray profile {new_uuid} for {profile_data.get('name')}")
    return {
        "uuid": new_uuid,
        "clientId": clientId,
        "name": userData.get("name"),
        "email": userData.get("email"),
        "deleted": False,
        "owner_tid": addInfo.get("owner_tid"),
    }


def delete_profile_by_uuid(uuid_str):
    clients = _read_json(CLIENTS_TABLE)
    if not clients:
        return False
    found = False
    for clientId, data in clients.items():
        addInfo = data.get("addInfo", {})
        if addInfo.get("uuid") == uuid_str:
            if data.get("deleted", False):
                # Already deleted
                log.warning(f"Profile {uuid_str} already marked deleted")
                return False
            data["deleted"] = True
            data["deleted_at"] = datetime.datetime.utcnow().isoformat() + "Z"
            clients[clientId] = data
            found = True
            break
    if not found:
        return False
    log.info(f"Profile {uuid_str} marked deleted")
    _backup_file(CLIENTS_TABLE)
    _write_json_atomic(CLIENTS_TABLE, clients)
    return True


def facts():
    server_data = _read_json(SERVER_JSON)
    listen_port = None
    if server_data:
        inbounds = server_data.get("inbounds")
        if inbounds and isinstance(inbounds, list) and len(inbounds) > 0:
            listen_port = inbounds[0].get("port")
    clients = _read_json(CLIENTS_TABLE)
    count_profiles = len(clients) if clients else 0
    return {
        "listen_port": listen_port,
        "count_profiles": count_profiles,
    }
