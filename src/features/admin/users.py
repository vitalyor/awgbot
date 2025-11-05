# src/features/admin/users.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from datetime import datetime

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import ContextTypes

from core.ui import edit_or_send
from core.state import load_state, save_state, now_iso
import xray as XR
import awg as AWG

# --- helpers (локальные, без зависимости от bot.py) ---

def _profiles_active(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [p for p in user.get("profiles", []) if not p.get("deleted")]

def _xray_status_for_user(user_rec: Dict[str, Any], tg_id: int, pname: str) -> tuple[str, str]:
    """Возвращает ("active"|"suspended"|"absent", удобочитаемая метка)."""
    pr = next((p for p in _profiles_active(user_rec) if p.get("type") == "xray" and p.get("name") == pname), None)
    if not pr:
        return ("absent", "Отсутствует ⚠️")
    if pr.get("suspended"):
        return ("suspended", "Приостановлен ⏸")
    try:
        info = XR.find_user(tg_id, pname)
        return ("active", "Активен ▶️") if info else ("absent", "Отсутствует ⚠️")
    except Exception:
        return ("absent", "Отсутствует ⚠️")

# --- экспортируемые вьюхи ---

async def show_admin_user_list(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    page: int = 0,
    page_size: int = 10,
):
    st = load_state()
    items = sorted(st.get("users", {}).items(), key=lambda kv: int(kv[0]))
    total = len(items)
    start, end = page * page_size, min((page + 1) * page_size, total)

    rows = []
    for tid, rec in items[start:end]:
        tag = "✅" if rec.get("allowed") else "⛔"
        uname = rec.get("username") or "-"
        rows.append([InlineKeyboardButton(f"{tag} {tid} @{uname}", callback_data=f"admin_user_open:{tid}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_list_page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_list_page:{page+1}"))
    rows.append(nav or [InlineKeyboardButton("⬅️ Назад", callback_data="admin_menu")])

    kb = InlineKeyboardMarkup(rows)
    txt = f"Пользователи {start+1}–{end} из {total}"
    if update.callback_query:
        await update.callback_query.edit_message_text(txt, reply_markup=kb)
    else:
        await update.effective_chat.send_message(txt, reply_markup=kb)

async def show_admin_user_card(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tid: str,
    replace: bool = False,
    note: str = "",
):
    st = load_state()
    rec = st.get("users", {}).get(tid)
    if not rec:
        await update.effective_chat.send_message("Пользователь не найден.")
        return

    tag = "✅ Разрешить → Запретить" if rec.get("allowed") else "⛔ Запретить → Разрешить"
    lines = [
        f"<b>Пользователь</b> <code>{tid}</code>",
        f"username: <code>@{rec.get('username') or '-'}</code>",
        f"имя: <code>{rec.get('first_name') or '-'}</code>",
        f"доступ: <code>{'yes' if rec.get('allowed') else 'no'}</code>",
    ]
    if note:
        lines += ["", note]

    rows = [
        [InlineKeyboardButton(tag, callback_data=f"admin_user_toggle:{tid}")],
        [InlineKeyboardButton("👤 Профили", callback_data=f"admin_user_profiles:{tid}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admin_list")],
    ]
    kb = InlineKeyboardMarkup(rows)
    txt = "\n".join(lines)

    if replace and update.callback_query:
        try:
            await update.callback_query.edit_message_text(txt, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            pass
    await edit_or_send(update, context, txt, kb, parse_mode="HTML")

async def show_admin_user_profiles(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tid: str,
    note: str = "",
):
    st = load_state()
    urec = st.get("users", {}).get(tid)
    if not urec:
        await edit_or_send(update, context, "Пользователь не найден.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_list")]]))
        return

    act = _profiles_active(urec)
    rows = []
    if not act:
        rows = [[InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_user_open:{tid}")]]
        await edit_or_send(update, context, "У пользователя нет активных конфигураций.", InlineKeyboardMarkup(rows))
        return

    for p in act:
        name, ptype = p.get("name"), p.get("type")
        if ptype == "xray":
            status, _ = _xray_status_for_user(urec, int(tid), name)
            left = f"{name} · {'▶️' if status=='active' else '⏸' if status=='suspended' else '⚠️'}"
        else:
            left = f"{name} · {ptype}"
        rows.append([InlineKeyboardButton(left, callback_data=f"admin_prof_open:{tid}:{name}:{ptype}")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_user_open:{tid}")])
    kb = InlineKeyboardMarkup(rows)
    txt = f"Конфигурации пользователя <code>{tid}</code>" + (f"\n\n{note}" if note else "")
    await edit_or_send(update, context, txt, kb, parse_mode="HTML")

async def show_admin_profile_card(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tid: str,
    pname: str,
    ptype: str,
    note: str = "",
):
    st = load_state()
    urec = st.get("users", {}).get(tid, {})
    pr = next((p for p in _profiles_active(urec) if p.get("name") == pname and p.get("type") == ptype), None)
    if not pr:
        await show_admin_user_profiles(update, context, tid, note="Профиль не найден.")
        return

    if ptype == "xray":
        info = None
        try:
            info = XR.find_user(int(tid), pname)
        except Exception:
            info = None
        status, status_label = _xray_status_for_user(urec, int(tid), pname)
        lines = [f"<b>{pname}</b> · Xray"]
        if info:
            lines.append(f"• UUID: <code>{info.get('uuid','')}</code>")
            lines.append(f"• SNI: <code>{info.get('sni','')}</code>")
            lines.append(f"• Port: <code>{info.get('port','')}</code>")
        lines.append(f"• Статус: <b>{status_label}</b>")
        if note:
            lines += ["", note]

        rows = []
        if status == "active":
            rows.append([InlineKeyboardButton("⏸ Приостановить", callback_data=f"admin_prof_suspend:{tid}:{pname}")])
        else:
            rows.append([InlineKeyboardButton("▶️ Возобновить", callback_data=f"admin_prof_resume:{tid}:{pname}")])
        rows.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"admin_prof_del:{tid}:{pname}:{ptype}")])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_user_profiles:{tid}")])
        kb = InlineKeyboardMarkup(rows)
        await edit_or_send(update, context, "\n".join(lines), kb, parse_mode="HTML")
        return

    if ptype in ("amneziawg", "awg"):
        info = AWG.find_user(int(tid), pname)
        if not info:
            await show_admin_user_profiles(update, context, tid, note="Конфигурация AmneziaWG не найдена на сервере.")
            return
        lines = [
            f"<b>{pname}</b> · AmneziaWG",
            f"• Endpoint: <code>{info.get('endpoint','')}</code>",
            f"• Port: <code>{info.get('port','')}</code>",
        ]
        if note:
            lines += ["", note]
        rows = [
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"admin_prof_del:{tid}:{pname}:{ptype}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_user_profiles:{tid}")],
        ]
        kb = InlineKeyboardMarkup(rows)
        await edit_or_send(update, context, "\n".join(lines), kb, parse_mode="HTML")
        return