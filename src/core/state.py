# src/core/state.py
from __future__ import annotations
import os, json, time, hashlib
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Any

from logger_setup import get_logger

from core.repo_awg import list_profiles as awg_list_profiles
from core.repo_xray import list_profiles as xray_list_profiles

logger = get_logger()

# Пути и лимиты (как было в bot.py)
DATA_DIR = "/app/data"
STATE_PATH = os.path.join(DATA_DIR, "state.json")
HEARTBEAT_PATH = os.path.join(DATA_DIR, "heartbeat")

STATE_BACKUPS_DIR = os.getenv("STATE_BACKUPS_DIR", "/app/data/backups")
STATE_BACKUPS_KEEP = int(os.getenv("STATE_BACKUPS_KEEP", "20"))
STATE_BACKUP_MIN_INTERVAL_SEC = int(os.getenv("STATE_BACKUP_MIN_INTERVAL_SEC", "30"))

# Память для автобэкапов
_last_state_backup_ts: float = 0.0
_last_state_backup_fingerprint: str = ""


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _state_backup_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S")


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _list_state_backups() -> list[Path]:
    try:
        p = Path(STATE_BACKUPS_DIR)
        if not p.exists():
            return []
        items = [x for x in p.glob("state-*.json") if x.is_file()]
        items.sort(key=lambda x: x.stat().st_mtime, reverse=True)  # newest first
        return items
    except Exception:
        return []


def _rotate_state_backups() -> tuple[int, int]:
    """
    Оставляет только STATE_BACKUPS_KEEP последних бэкапов.
    Возвращает (total_before, removed).
    """
    items = _list_state_backups()
    total = len(items)
    removed = 0
    try:
        if total > STATE_BACKUPS_KEEP:
            for x in items[STATE_BACKUPS_KEEP:]:
                try:
                    x.unlink(missing_ok=True)
                    removed += 1
                except Exception:
                    pass
    finally:
        try:
            logger.info(
                {
                    "event": "state_backup_rotate",
                    "total": total,
                    "keep": STATE_BACKUPS_KEEP,
                    "removed": removed,
                }
            )
        except Exception:
            pass
    return total, removed


def _auto_backup_state_json(state_obj: dict) -> None:
    """
    Делает бэкап state.json, только если содержимое изменилось с прошлого сохранения
    и соблюдён минимальный интервал между бэкапами.
    """
    global _last_state_backup_ts, _last_state_backup_fingerprint
    try:
        dump = json.dumps(
            state_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        fp = hashlib.sha256(dump.encode("utf-8")).hexdigest()
        if fp == _last_state_backup_fingerprint:
            return
        now = time.time()
        if (now - _last_state_backup_ts) < max(0, STATE_BACKUP_MIN_INTERVAL_SEC):
            return

        _ensure_dir(STATE_BACKUPS_DIR)
        ts = _state_backup_timestamp()
        bpath = os.path.join(STATE_BACKUPS_DIR, f"state-{ts}.json")
        with open(bpath, "w", encoding="utf-8") as f:
            f.write(dump)

        logger.info({"event": "state_backup_ok", "path": bpath})

        total_before, removed = _rotate_state_backups()
        try:
            total_after = len(_list_state_backups())
        except Exception:
            total_after = total_before - removed
        logger.info(
            {
                "event": "state_backup_rotate",
                "total": total_after,
                "keep": STATE_BACKUPS_KEEP,
                "removed": removed,
            }
        )

        _last_state_backup_fingerprint = fp
        _last_state_backup_ts = now

    except Exception as e:
        logger.warning({"event": "state_backup_postsave_fail", "error": str(e)})


def save_state(st: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)
    try:
        _auto_backup_state_json(st)
    except Exception:
        try:
            logger.warning({"event": "state_backup_postsave_fail"})
        except Exception:
            pass


def load_state() -> Dict[str, Any]:
    """
    Загружает состояние из state.json.
    Теперь state.json хранит только пользователей (без профилей).
    """
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(STATE_PATH):
        return {"users": {}}  # users: {tg_id: {...}}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        st = json.load(f)
    # нормализация
    users = st.setdefault("users", {})
    changed = False
    from .state import now_iso as _now  # локальный импорт, чтобы избежать циклов

    for tid, rec in list(users.items()):
        if not isinstance(rec, dict):
            users[tid] = {
                "allowed": False,
                "username": "",
                "first_name": "",
                "created_at": _now(),
            }
            changed = True
            continue
        if "allowed" not in rec:
            rec["allowed"] = False
            changed = True
        if "username" not in rec:
            rec["username"] = ""
            changed = True
        if "first_name" not in rec:
            rec["first_name"] = ""
            changed = True
        if "created_at" not in rec:
            rec["created_at"] = _now()
            changed = True
    if changed:
        save_state(st)
    return st


def ensure_user_bucket(
    st: Dict[str, Any], tg_id: int, username: str, first_name: str
) -> Dict[str, Any]:
    """
    Обеспечивает наличие записи пользователя в состоянии.
    Теперь не содержит логику по профилям.
    """
    u = st["users"].setdefault(
        str(tg_id),
        {
            "allowed": False,
            "username": username or "",
            "first_name": first_name or "",
            "created_at": now_iso(),
        },
    )
    if "allowed" not in u:
        u["allowed"] = False
    if "created_at" not in u:
        u["created_at"] = now_iso()
    if username:
        u["username"] = username
    if first_name:
        u["first_name"] = first_name
    return u


def get_user_profiles(user_id: int) -> list[dict]:
    """
    Возвращает список профилей пользователя user_id,
    объединяя профили из репозиториев awg и xray.
    Фильтрует по addInfo.owner_tid == user_id.
    """
    awg_profiles = awg_list_profiles()
    xray_profiles = xray_list_profiles()
    combined = awg_profiles + xray_profiles
    filtered = [p for p in combined if getattr(p, "addInfo", None) and getattr(p.addInfo, "owner_tid", None) == user_id]
    return filtered


def sync_user_profiles(user_id: int) -> dict:
    """
    Возвращает объект с user_id и списком профилей пользователя.
    """
    profiles = get_user_profiles(user_id)
    return {"user_id": user_id, "profiles": profiles}
