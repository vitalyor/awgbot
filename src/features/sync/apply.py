# src/features/sync/apply.py
from __future__ import annotations
from typing import Dict, Any, List
import logging

from core.state import load_state, save_state, now_iso
from core import repo_xray as XR

logger = logging.getLogger(__name__)


def profiles_active(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [p for p in user.get("profiles", []) if not p.get("deleted")]


def _get_state_profile(
    st: dict, tid: int, name: str
) -> tuple[dict | None, dict | None]:
    """(urec, profile) из state.json для пользователя tid и профиля name (type=xray, не удалённый)."""
    urec = st.get("users", {}).get(str(tid))
    if not isinstance(urec, dict):
        return None, None
    for p in profiles_active(urec):
        if p.get("type") == "xray" and p.get("name") == name:
            return urec, p
    return urec, None


def _log_apply(event: str, **kw):
    try:
        logger.info({"event": event, **kw})
    except Exception:
        pass


def _find_xray_bot_client(tid: int, name: str) -> dict | None:
    """Ищет 'своего' клиента Xray по (tid,name)."""
    try:
        xlist = XR.list_all() or []
    except Exception:
        xlist = []
    for c in xlist:
        if c.get("source") != "bot":
            continue
        if int(c.get("tid") or 0) == int(tid) and (c.get("name") or "") == name:
            return c
    return None


# ----------- ABSENT -----------


def sync_absent_apply_one(tid: int, name: str) -> tuple[bool, str]:
    """
    Починить ONLY_IN_STATE для одного профиля: добавить профиль в Xray.
    Возвращает (ok, reason).
    """
    st = load_state()
    urec, pr = _get_state_profile(st, tid, name)
    if not urec:
        _log_apply(
            "sync_absent_apply_one",
            tid=tid,
            name=name,
            ok=False,
            reason="user_not_in_state",
        )
        return False, "user_not_in_state"
    if not pr:
        _log_apply(
            "sync_absent_apply_one",
            tid=tid,
            name=name,
            ok=False,
            reason="profile_not_in_state",
        )
        return False, "profile_not_in_state"
    if pr.get("suspended"):
        _log_apply(
            "sync_absent_apply_one",
            tid=tid,
            name=name,
            ok=False,
            reason="profile_suspended",
        )
        return False, "profile_suspended"

    try:
        if XR.find_user(tid, name):
            _log_apply(
                "sync_absent_apply_one",
                tid=tid,
                name=name,
                ok=False,
                reason="already_present",
            )
            return False, "already_present"
    except Exception:
        pass

    try:
        res = XR.add_user(tid, name)
        if isinstance(res, dict) and res.get("uuid"):
            pr["uuid"] = res["uuid"]
        pr["last_xray_sync_at"] = now_iso()
        save_state(st)
        _log_apply(
            "sync_absent_apply_one",
            tid=tid,
            name=name,
            ok=True,
            reason="ok",
            uuid=pr.get("uuid"),
        )
        return True, "ok"
    except Exception as e:
        _log_apply(
            "sync_absent_apply_one",
            tid=tid,
            name=name,
            ok=False,
            reason="xray_add_fail",
            error=str(e),
        )
        return False, "xray_add_fail"


def sync_absent_apply_all() -> dict:
    from .collect import sync_collect as _collect

    snap = _collect()
    items = snap.get("only_in_state", [])
    total = len(items)
    done = skipped = errors = 0
    results = []
    for it in items:
        tid = int(it.get("tid") or 0)
        name = it.get("name") or ""
        ok, reason = sync_absent_apply_one(tid, name)
        results.append({"tid": tid, "name": name, "ok": ok, "reason": reason})
        if ok:
            done += 1
        else:
            if reason in (
                "user_not_in_state",
                "profile_not_in_state",
                "profile_suspended",
                "already_present",
            ):
                skipped += 1
            else:
                errors += 1
    summary = {
        "total": total,
        "done": done,
        "skipped": skipped,
        "errors": errors,
        "items": results,
    }
    _log_apply("sync_absent_apply_all", **summary)
    return summary


# ----------- EXTRA -----------


def sync_extra_apply_one(tid: int, name: str) -> tuple[bool, str]:
    """
    Починить ONLY_IN_XRAY (свои): удалить "лишнего" клиента из Xray.
    Работает ТОЛЬКО для клиентов source=bot.
    """
    target = _find_xray_bot_client(tid, name)
    if not target:
        _log_apply(
            "sync_extra_apply_one",
            tid=tid,
            name=name,
            ok=False,
            reason="not_found_in_xray",
        )
        return False, "not_found_in_xray"

    try:
        ok = XR.remove_user_by_name(tid, name)
        if not ok:
            _log_apply(
                "sync_extra_apply_one",
                tid=tid,
                name=name,
                ok=False,
                reason="xray_remove_fail",
            )
            return False, "xray_remove_fail"
        _log_apply(
            "sync_extra_apply_one",
            tid=tid,
            name=name,
            ok=True,
            reason="ok",
            uuid=target.get("uuid"),
        )
        return True, "ok"
    except Exception as e:
        _log_apply(
            "sync_extra_apply_one",
            tid=tid,
            name=name,
            ok=False,
            reason="xray_remove_fail",
            error=str(e),
        )
        return False, "xray_remove_fail"


def sync_extra_apply_all() -> dict:
    from .collect import sync_collect as _collect

    snap = _collect()
    items = snap.get("only_in_xray", [])
    total = len(items)
    done = skipped = errors = 0
    results = []
    for it in items:
        tid = int(it.get("tid") or 0)
        name = it.get("name") or ""
        ok, reason = sync_extra_apply_one(tid, name)
        results.append({"tid": tid, "name": name, "ok": ok, "reason": reason})
        if ok:
            done += 1
        else:
            if reason == "not_found_in_xray":
                skipped += 1
            else:
                errors += 1
    summary = {
        "total": total,
        "done": done,
        "skipped": skipped,
        "errors": errors,
        "items": results,
    }
    _log_apply("sync_extra_apply_all", **summary)
    return summary


# ----------- DIVERGED -----------


def sync_diverged_update_db_one(tid: int, name: str) -> tuple[bool, str]:
    """
    Обновляет БД (state.json) по факту из Xray для одного diverged профиля.
    Разрешено даже если профиль suspended или у пользователя снят доступ.
    """
    st = load_state()
    urec, pr = _get_state_profile(st, tid, name)
    if not urec or not pr:
        _log_apply(
            "sync_diverged_update_db_one",
            tid=tid,
            name=name,
            ok=False,
            reason="profile_not_in_state",
        )
        return False, "profile_not_in_state"

    xr = _find_xray_bot_client(tid, name)
    if not xr:
        _log_apply(
            "sync_diverged_update_db_one",
            tid=tid,
            name=name,
            ok=False,
            reason="not_found_in_xray",
        )
        return False, "not_found_in_xray"

    if xr.get("uuid"):
        pr["uuid"] = xr["uuid"]
    if pr.get("flow") is not None and xr.get("flow"):
        pr["flow"] = xr["flow"]
    pr["last_xray_sync_at"] = now_iso()
    save_state(st)

    _log_apply(
        "sync_diverged_update_db_one",
        tid=tid,
        name=name,
        ok=True,
        reason="ok",
        uuid=pr.get("uuid"),
    )
    return True, "ok"


def sync_diverged_rebuild_xray_one(tid: int, name: str) -> tuple[bool, str]:
    """
    Пересобирает запись в Xray, приводя её к данным из БД (uuid/flow).
    Пропускаем, если профиль suspended или у пользователя снят доступ.
    """
    st = load_state()
    urec, pr = _get_state_profile(st, tid, name)
    if not urec or not pr:
        _log_apply(
            "sync_diverged_rebuild_xray_one",
            tid=tid,
            name=name,
            ok=False,
            reason="profile_not_in_state",
        )
        return False, "profile_not_in_state"

    if not urec.get("allowed", False):
        _log_apply(
            "sync_diverged_rebuild_xray_one",
            tid=tid,
            name=name,
            ok=False,
            reason="user_disallowed",
        )
        return False, "user_disallowed"

    if pr.get("suspended"):
        _log_apply(
            "sync_diverged_rebuild_xray_one",
            tid=tid,
            name=name,
            ok=False,
            reason="profile_suspended",
        )
        return False, "profile_suspended"

    want_uuid = (pr.get("uuid") or "").strip()
    want_flow = (pr.get("flow") or "").strip() or None
    if not want_uuid:
        _log_apply(
            "sync_diverged_rebuild_xray_one",
            tid=tid,
            name=name,
            ok=False,
            reason="no_uuid_in_state",
        )
        return False, "no_uuid_in_state"

    try:
        if hasattr(XR, "ensure_user_uuid_flow"):
            ok = bool(XR.ensure_user_uuid_flow(tid, name, want_uuid, want_flow))
        else:
            XR.remove_user_by_name(tid, name)
            ok = bool(XR.resume_user_by_name(tid, name, want_uuid, want_flow))
        if not ok:
            _log_apply(
                "sync_diverged_rebuild_xray_one",
                tid=tid,
                name=name,
                ok=False,
                reason="xray_update_fail",
            )
            return False, "xray_update_fail"
    except Exception as e:
        _log_apply(
            "sync_diverged_rebuild_xray_one",
            tid=tid,
            name=name,
            ok=False,
            reason="xray_update_exc",
            error=str(e),
        )
        return False, "xray_update_exc"

    pr["last_xray_sync_at"] = now_iso()
    save_state(st)
    _log_apply(
        "sync_diverged_rebuild_xray_one",
        tid=tid,
        name=name,
        ok=True,
        reason="ok",
        uuid=pr.get("uuid"),
    )
    return True, "ok"


def sync_diverged_update_db_all() -> dict:
    from .collect import sync_collect as _collect

    snap = _collect()
    items = snap.get("diverged", [])
    total = len(items)
    done = skipped = errors = 0
    results = []
    for it in items:
        tid = int(it.get("tid") or 0)
        name = it.get("name") or ""
        ok, reason = sync_diverged_update_db_one(tid, name)
        results.append({"tid": tid, "name": name, "ok": ok, "reason": reason})
        if ok:
            done += 1
        else:
            if reason in ("profile_not_in_state", "not_found_in_xray"):
                skipped += 1
            else:
                errors += 1
    summary = {
        "total": total,
        "done": done,
        "skipped": skipped,
        "errors": errors,
        "items": results,
    }
    _log_apply("sync_diverged_update_db_all", **summary)
    return summary


def sync_diverged_rebuild_xray_all() -> dict:
    from .collect import sync_collect as _collect

    snap = _collect()
    items = snap.get("diverged", [])
    total = len(items)
    done = skipped = errors = 0
    results = []
    for it in items:
        tid = int(it.get("tid") or 0)
        name = it.get("name") or ""
        ok, reason = sync_diverged_rebuild_xray_one(tid, name)
        results.append({"tid": tid, "name": name, "ok": ok, "reason": reason})
        if ok:
            done += 1
        else:
            if reason in (
                "user_disallowed",
                "profile_suspended",
                "profile_not_in_state",
                "no_uuid_in_state",
            ):
                skipped += 1
            else:
                errors += 1
    summary = {
        "total": total,
        "done": done,
        "skipped": skipped,
        "errors": errors,
        "items": results,
    }
    _log_apply("sync_diverged_rebuild_xray_all", **summary)
    return summary
