import os
import json
import subprocess
import shutil
import fcntl
import uuid as uuidlib
from typing import List, Optional
from datetime import datetime
from services.logger_setup import get_logger
log = get_logger("core.repo_awg")

BASE_PATH = "/opt/amnezia/awg"
LOCKS_PATH = os.path.join(BASE_PATH, "locks")
BACKUP_KEEP = 5
CLIENTS_TABLE = os.path.join(BASE_PATH, "clientsTable")
WG_CONF = os.path.join(BASE_PATH, "wg0.conf")

def _acquire_flock(path, mode):
    """Open file and acquire exclusive flock, return file object."""
    f = open(path, mode)
    fcntl.flock(f, fcntl.LOCK_EX)
    return f

def _atomic_write_json(path, data):
    tmp_path = path + ".new"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    # Backup rotation
    for i in reversed(range(1, BACKUP_KEEP)):
        prev = f"{path}.{i}"
        prev_next = f"{path}.{i+1}"
        if os.path.exists(prev):
            os.rename(prev, prev_next)
    if os.path.exists(path):
        os.rename(path, f"{path}.1")
    os.rename(tmp_path, path)

def _read_clients_table():
    try:
        with open(CLIENTS_TABLE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Failed to read clientsTable: {e}")
        return []

def _write_clients_table(clients_data):
    os.makedirs(os.path.dirname(CLIENTS_TABLE), exist_ok=True)
    # use flock to prevent concurrent writes
    with _acquire_flock(CLIENTS_TABLE, "a+"):
        _atomic_write_json(CLIENTS_TABLE, clients_data)

def _sync_wg_conf():
    log.debug("Regenerating wg0.conf from clientsTable...")
    # Re-generate wg0.conf from clientsTable and call wg syncconf
    clients = _read_clients_table()
    # Read base config (interface section)
    conf_lines = []
    with open(WG_CONF, "r", encoding="utf-8") as f:
        lines = f.readlines()
    # Find where [Peer] sections start
    peer_idx = None
    for idx, line in enumerate(lines):
        if line.strip().startswith("[Peer]"):
            peer_idx = idx
            break
    if peer_idx is not None:
        conf_lines = lines[:peer_idx]
    else:
        conf_lines = lines
    # Now, append peers from clientsTable
    for client in clients:
        if client.get("addInfo", {}).get("deleted"):
            continue
        user_data = client.get("userData", {})
        conf_lines.append("\n[Peer]\n")
        conf_lines.append(f"PublicKey = {client['clientId']}\n")
        conf_lines.append(f"PresharedKey = {user_data.get('psk','')}\n")
        conf_lines.append(f"AllowedIPs = {user_data.get('ip','')}/32\n")
    # Write to temp conf
    tmp_conf = WG_CONF + ".new"
    with open(tmp_conf, "w", encoding="utf-8") as f:
        f.writelines(conf_lines)
        f.flush()
        os.fsync(f.fileno())
    # Atomically move and syncconf
    os.rename(tmp_conf, WG_CONF)
    subprocess.run(["wg", "syncconf", "wg0", WG_CONF], check=True)
    log.info("wg0.conf successfully synchronized.")

def _get_wg_dump():
    try:
        result = subprocess.run(["wg", "show", "wg0", "dump"], capture_output=True, text=True, check=True)
        return result.stdout.strip().splitlines()
    except subprocess.CalledProcessError:
        return []

def list_profiles() -> List[dict]:
    clients_data = _read_clients_table()
    wg_dump = _get_wg_dump()
    allowed_ips_map = {}
    for line in wg_dump[1:]:  # skip header line
        parts = line.split('\t')
        if len(parts) >= 5:
            pubkey = parts[0]
            allowed_ips = parts[4]
            allowed_ips_map[pubkey] = allowed_ips
    profiles = []
    for client in clients_data:
        client_id = client.get("clientId", "")
        user_data = client.get("userData", {})
        add_info = client.get("addInfo", {})
        uuid = add_info.get("uuid", "")
        owner_tid = add_info.get("owner_tid", "")
        deleted = add_info.get("deleted", False)
        name = user_data.get("clientName", "")
        allowed_ips = allowed_ips_map.get(client_id, "(none)")
        profile = {
            "uuid": uuid,
            "clientId": client_id,
            "name": name,
            "allowed_ips": allowed_ips,
            "deleted": deleted,
            "owner_tid": owner_tid,
            "userData": user_data,
            "addInfo": add_info,
        }
        profiles.append(profile)
    return profiles

def _generate_wg_keypair():
    log.debug("Generating WireGuard keypair")
    priv = subprocess.check_output(["wg", "genkey"]).decode().strip()
    pub = subprocess.check_output(["wg", "pubkey"], input=priv.encode()).decode().strip()
    return priv, pub

def _generate_psk():
    log.debug("Generating preshared key")
    return subprocess.check_output(["wg", "genpsk"]).decode().strip()

def _get_next_ip(clients, subnet):
    import ipaddress
    net = ipaddress.ip_network(subnet)
    used = set()
    for c in clients:
        ip = c.get("userData", {}).get("ip")
        if ip:
            used.add(ipaddress.ip_address(ip))
    # skip .1 (server), start from .2
    for host in net.hosts():
        if str(host) == str(list(net.hosts())[0]):
            continue
        if host not in used:
            return str(host)
    raise RuntimeError("No available IPs in subnet")

def create_profile(profile_data: dict) -> str:
    log.info(f"Creating new AWG profile for {profile_data.get('name')} (owner_tid={profile_data.get('owner_tid')})")
    clients = _read_clients_table()
    facts_data = facts()
    subnet = facts_data.get("subnet", "10.10.0.0/24")
    # Generate keys and IP
    priv, pub = _generate_wg_keypair()
    psk = _generate_psk()
    ip = _get_next_ip(clients, subnet)
    # Build client entry
    client_uuid = str(uuidlib.uuid4())
    user_data = {
        "clientName": profile_data.get("name", f"peer-{client_uuid[:8]}"),
        "privateKey": priv,
        "psk": psk,
        "ip": ip,
        "created": datetime.utcnow().isoformat() + "Z",
    }
    add_info = {
        "uuid": client_uuid,
        "owner_tid": profile_data.get("owner_tid", ""),
        "deleted": False,
        "created": datetime.utcnow().isoformat() + "Z",
    }
    client = {
        "clientId": pub,
        "userData": user_data,
        "addInfo": add_info,
    }
    clients.append(client)
    _write_clients_table(clients)
    _sync_wg_conf()
    log.info(f"Created AWG profile {client_uuid} with IP {ip}")
    return client_uuid

def find_profile_by_uuid(uuid: str) -> Optional[dict]:
    profiles = list_profiles()
    for profile in profiles:
        if profile.get("uuid") == uuid:
            return profile
    return None

def delete_profile_by_uuid(uuid: str) -> bool:
    clients = _read_clients_table()
    found = False
    for client in clients:
        add_info = client.get("addInfo", {})
        if add_info.get("uuid") == uuid and not add_info.get("deleted", False):
            add_info["deleted"] = True
            add_info["deleted_at"] = datetime.utcnow().isoformat() + "Z"
            found = True
    if found:
        _write_clients_table(clients)
        _sync_wg_conf()
        log.info(f"Profile {uuid} marked as deleted")
    else:
        log.warning(f"Attempted to delete nonexistent or already deleted profile {uuid}")
    return found

def facts() -> dict:
    # Read the [Interface] section from wg0.conf
    port = None
    address = None
    dns = None
    endpoint = None
    subnet = None
    with open(WG_CONF, "r", encoding="utf-8") as f:
        lines = f.readlines()
    for line in lines:
        l = line.strip()
        if l.startswith("ListenPort"):
            port = l.split("=")[1].strip()
        elif l.startswith("Address"):
            address = l.split("=")[1].strip()
            # try to extract subnet (just pick the first one)
            if "/" in address:
                subnet = address.split(",")[0].strip()
        elif l.startswith("DNS"):
            dns = l.split("=")[1].strip()
        elif l.startswith("Endpoint"):
            endpoint = l.split("=")[1].strip()
    return {
        "port": port,
        "subnet": subnet,
        "dns": dns,
        "endpoint": endpoint,
    }
