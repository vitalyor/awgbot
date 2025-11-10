# src/features/sync/collect.py
from __future__ import annotations
from typing import Dict, Any, List
import logging

from core.state import load_state
from core import repo_xray as XR

logger = logging.getLogger(__name__)


def profiles_active(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [p for p in user.get("profiles", []) if not p.get("deleted")]


def sync_collect():
    """
    Снимок состояния:
    - users/profiles из state.json
    - клиенты Xray из server.json

    Учитываем только "своих" (source=bot), foreign считаем отдельно.
    """
    st = load_state()

    # --- Xray ---
    try:
        xlist = XR.list_all() or []
    except Exception as e:
        logger.warning({"event": "xray_list_all_fail_collect", "err": str(e)})
        xlist = []

    xray_bot = [c for c in xlist if (c.get("source") == "bot")]
    xray_foreign = [c for c in xlist if (c.get("source") != "bot")]
    xray_by_key = {(c.get("tid"), c.get("name")): c for c in xray_bot}

    only_in_state, only_in_xray, diverged, suspended, active = [], [], [], [], []
    users = st.get("users", {})
    for tid_str, urec in users.items():
        try:
            tid = int(tid_str)
        except Exception:
            continue
        for p in profiles_active(urec):
            if p.get("type") != "xray":
                continue
            key = (tid, p.get("name"))
            present = key in xray_by_key
            is_susp = bool(p.get("suspended"))
            if present and not is_susp:
                xr = xray_by_key.get(key, {})
                diffs = []
                st_uuid = (p.get("uuid") or "").strip()
                xr_uuid = (xr.get("uuid") or "").strip()
                if st_uuid and xr_uuid and st_uuid != xr_uuid:
                    diffs.append("uuid")
                st_flow = (p.get("flow") or "").strip()
                xr_flow = (xr.get("flow") or "").strip()
                if st_flow and xr_flow and st_flow != xr_flow:
                    diffs.append("flow")
                (diverged if diffs else active).append(
                    {
                        "tid": tid,
                        "name": p["name"],
                        **({"diffs": diffs} if diffs else {}),
                    }
                )
            elif present and is_susp:
                suspended.append({"tid": tid, "name": p["name"]})
            elif not present and is_susp:
                suspended.append({"tid": tid, "name": p["name"]})
            else:
                only_in_state.append({"tid": tid, "name": p["name"]})

    state_keys = set()
    for tid_str, urec in users.items():
        try:
            tid = int(tid_str)
        except Exception:
            continue
        for p in profiles_active(urec):
            if p.get("type") == "xray":
                state_keys.add((tid, p.get("name")))

    for c in xray_bot:
        key = (int(c.get("tid") or 0), c.get("name") or "")
        if key not in state_keys:
            only_in_xray.append({"tid": key[0], "name": key[1], "uuid": c.get("uuid")})

    counters = {
        "only_in_state": len(only_in_state),
        "only_in_xray": len(only_in_xray),
        "diverged": len(diverged),
        "suspended": len(suspended),
        "active": len(active),
        "foreign": len(xray_foreign),
        "profiles_state": sum(
            1
            for u in users.values()
            for p in profiles_active(u)
            if p.get("type") == "xray"
        ),
        "clients_xray": len(xray_bot),
        "users": len(users),
    }

    return {
        "counters": counters,
        "only_in_state": only_in_state,
        "only_in_xray": only_in_xray,
        "diverged": diverged,
        "suspended": suspended,
        "active": active,
        "foreign": xray_foreign,
    }
