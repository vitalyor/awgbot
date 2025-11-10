# bot.py
from __future__ import annotations
import os, io, json, re, uuid, base64, zlib, threading, time, subprocess, shlex, qrcode, logging
from datetime import datetime, UTC
from functools import wraps
from typing import Dict, Any, List, Optional
from pathlib import Path

# --- загрузка secret.env ДО любых импортов util/xray/awg и ДО чтения TOKEN ---
SECRETS_FILE = "/run/secrets/secret.env"


def load_env_kv_file(path: str, overwrite: bool = True) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if not k:
                    continue
                if not overwrite and k in os.environ:
                    continue
                os.environ[k] = v
    except Exception:
        pass


def _fallback_get_from_file(path: str, key: str) -> str | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() == key:
                    return v.strip()
    except Exception:
        pass
    return None


load_env_kv_file(SECRETS_FILE, overwrite=True)

ERROR_NOTIFY_COOLDOWN_SEC = int(os.getenv("ERROR_NOTIFY_COOLDOWN_SEC", "600"))

from services.logger_setup import get_logger

logger = get_logger()


def ensure_rid(context) -> str:
    rid = context.chat_data.get("_rid") if getattr(context, "chat_data", None) else None
    if not rid:
        rid = uuid.uuid4().hex[:8]
        try:
            context.chat_data["_rid"] = rid
        except Exception:
            pass
    return rid


def _cmd_name_from_update(update) -> str:
    try:
        if getattr(update, "message", None) and update.message and update.message.text:
            return (update.message.text.split()[0] or "").strip()
        if (
            getattr(update, "callback_query", None)
            and update.callback_query
            and update.callback_query.data
        ):
            return f"[cb] {update.callback_query.data}"
    except Exception:
        pass
    return "(unknown)"


# --- теперь можно импортировать ---
from telegram import (
    Update,
    InputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from services.util import XRAY_CONNECT_HOST, AWG_CONNECT_HOST
from features.status.render import render_status_full, build_status_kb
from features.admin.users import (
    show_admin_user_list,
    show_admin_user_card,
    show_admin_user_profiles,
    show_admin_profile_card,
)
from features.sync.render import (
    sync_render,
    build_sync_kb,
    sync_header,
    sync_filter_items,
    sync_status_label,
    split_text_for_telegram,
)
from features.sync.collect import sync_collect
from features.sync.apply import (
    sync_absent_apply_one,
    sync_absent_apply_all,
    sync_extra_apply_one,
    sync_extra_apply_all,
    sync_diverged_update_db_one,
    sync_diverged_update_db_all,
    sync_diverged_rebuild_xray_one,
    sync_diverged_rebuild_xray_all,
)
from core.ui import (
    SAFE_TXT,
    ensure_main_menu_button,
    clean_and_send,
    edit_or_send,
    _edit_cb_with_fallback,
    autoclean_command_input,
)
from core.state import (
    DATA_DIR,
    STATE_PATH,
    HEARTBEAT_PATH,
    load_state,
    save_state,
    ensure_user_bucket,
    now_iso,
)


# ====== IMPORT DOCKER/STATUS UTILS FROM CORE ======
from core.docker import (
    run_cmd,
    _docker_exec,
    dir_size_bytes,
    tcp_check,
)
from core.status_probe import (
    human_seconds,
    docker_stats,
    humanize_uptime,
    prettify_container_status,
    summarize_counters,
    status_probe,
)
from core.decorators import log_command, admin_only, with_request_id
from core import repo_awg as AWG
from core import repo_xray as XR


# --- а теперь читаем переменные с fallback ---
TOKEN = os.getenv("TELEGRAM_TOKEN") or _fallback_get_from_file(
    SECRETS_FILE, "TELEGRAM_TOKEN"
)
if not TOKEN:
    raise SystemExit(
        "TELEGRAM_TOKEN не задан (ожидался в .env или в /run/secrets/secret.env)"
    )

ADMIN_IDS_RAW = (
    os.getenv("ADMIN_IDS") or _fallback_get_from_file(SECRETS_FILE, "ADMIN_IDS") or ""
).strip()
if not ADMIN_IDS_RAW:
    raise SystemExit(
        "ADMIN_IDS не задан (ожидался в .env или в /run/secrets/secret.env)"
    )
ADMIN_IDS = {int(tok) for tok in re.split(r"[,\s]+", ADMIN_IDS_RAW) if tok.isdigit()}
if not ADMIN_IDS:
    raise SystemExit("ADMIN_IDS пуст или не содержит числовых ID")
# ===== Watchdog настройки из ENV =====
_BOOT_TS = time.time()
WATCHDOG_ENABLED = os.getenv("WATCHDOG_ENABLED", "1") == "1"
WATCHDOG_INTERVAL_SEC = int(os.getenv("WATCHDOG_INTERVAL_SEC", "300"))
WATCHDOG_COOLDOWN_SEC = int(os.getenv("WATCHDOG_COOLDOWN_SEC", "600"))
WATCHDOG_AUTORESTART = os.getenv("WATCHDOG_AUTORESTART", "0") == "1"
HEARTBEAT_WARN_SEC = int(os.getenv("HEARTBEAT_WARN_SEC", "120"))
HEARTBEAT_CRIT_SEC = int(os.getenv("HEARTBEAT_CRIT_SEC", "300"))
WATCHDOG_TG_NOTIFY = os.getenv("WATCHDOG_TG_NOTIFY", "1") == "1"
WATCHDOG_TG_TIMEOUT = int(os.getenv("WATCHDOG_TG_TIMEOUT", "5"))
WATCHDOG_BOOT_GRACE_SEC = int(os.getenv("WATCHDOG_BOOT_GRACE_SEC", "60"))


# ===== /sync: фильтры и режимы =====
SYNC_DEFAULT_FILTER = "all"  # all|absent|extra|suspended|diverged
SYNC_DEFAULT_MODE = "compact"  # compact|detailed

SYNC_FILTERS = {
    "all": "Все",
    "absent": "Только отсутствующие",
    "extra": "Только лишние в Xray",
    "suspended": "Только приостановленные",
    "diverged": "Только с расхождениями",
}

SYNC_MODE_LABEL = {
    "compact": "🧷 Компактный вид",
    "detailed": "📋 Подробный вид",
}

# ===== Прочие настройки из ENV =====
NOTIFY_USER_ON_ACCESS_CHANGE = (
    os.getenv("NOTIFY_USER_ON_ACCESS_CHANGE", "1") == "1"
)  # уведомлять юзера при изменении доступа
CB_DEBOUNCE_MS = int(os.getenv("CB_DEBOUNCE_MS", "2000"))  # антидубль для callback, мс
STATUS_LOADER_COOLDOWN_SEC = int(
    os.getenv("STATUS_LOADER_COOLDOWN_SEC", "5")
)  # как часто показывать "Загружаю ресурсы…"
CMD_DEBOUNCE_MS = int(os.getenv("CMD_DEBOUNCE_MS", "1200"))  # антидубль для команд, мс

logger.info(
    {
        "event": "boot",
        "token_len": len(TOKEN),
        "env_token_in_env": bool(os.getenv("TELEGRAM_TOKEN")),
    }
)


# Лимиты конфигураций: по 5 на каждый протокол (итого 10)
MAX_XRAY = int(os.environ.get("MAX_XRAY", "5"))
MAX_AWG = int(os.environ.get("MAX_AWG", "5"))


# ========= УТИЛИТЫ (только локальные, без docker) =========


def _notify_user_simple(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str
) -> None:
    if not NOTIFY_USER_ON_ACCESS_CHANGE:
        return
    try:
        # отправляем асинхронно через create_task, чтобы не блокировать текущий хэндлер
        context.application.create_task(
            context.bot.send_message(chat_id=chat_id, text=text)
        )
    except Exception:
        pass


def is_admin_id(tid: int) -> bool:
    return tid in ADMIN_IDS


def _auto_suspend_all_xray(st: Dict[str, Any], tid: int) -> tuple[int, int, int]:
    """
    Приостанавливает все активные Xray-профили пользователя tid.
    Возвращает (total, done, skipped):
      total   — всего Xray-профилей
      done    — успешно приостановлены (сняты с сервера и помечены suspended)
      skipped — уже были suspended или не получилось снять
    """
    key = str(tid)
    urec = st.get("users", {}).get(key, {})
    total = done = skipped = 0
    for p in profiles_active(urec):
        if p.get("type") != "xray":
            continue
        total += 1
        if p.get("suspended"):
            skipped += 1
            continue
        try:
            snap = XR.suspend_user_by_name(int(tid), p["name"])
        except Exception:
            snap = None
        if snap:
            p["suspended"] = True
            p["susp_uuid"] = snap.get("uuid")
            p["susp_flow"] = snap.get("flow")
            done += 1
        else:
            skipped += 1
    return total, done, skipped


def profiles_active(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [p for p in user.get("profiles", []) if not p.get("deleted")]


def profiles_active_by_type(user: Dict[str, Any], typ: str) -> List[Dict[str, Any]]:
    return [p for p in profiles_active(user) if p.get("type") == typ]


def md_limit_reached(user: Dict[str, Any], typ: str) -> bool:
    if typ == "xray":
        return len(profiles_active_by_type(user, "xray")) >= MAX_XRAY
    if typ in ("amneziawg", "awg"):
        return len(profiles_active_by_type(user, "amneziawg")) >= MAX_AWG
    return False


def _iter_xray_profiles(user_rec: Dict[str, Any]):
    """Итерирует НЕудалённые Xray-профили пользователя (из state.json)."""
    for p in profiles_active(user_rec):
        if p.get("type") == "xray":
            yield p


# ===== СТАТУС ПРОФИЛЯ XRAY =====
def xray_profile_status_for_user(
    user_rec: Dict[str, Any], tg_id: int, pname: str
) -> tuple[str, str]:
    """
    Возвращает (status, label):
      - ("active", "Активен ▶️")        — профиль есть в Xray и не помечен как suspended
      - ("suspended", "Приостановлен ⏸") — профиль помечен suspended в state.json
      - ("absent", "Отсутствует ⚠️")     — профиль не найден в Xray (удалён/рассинхрон)
    """
    try:
        pr = next(
            (
                p
                for p in profiles_active(user_rec)
                if p.get("name") == pname and p.get("type") == "xray"
            ),
            None,
        )
        if not pr:
            return ("absent", "Отсутствует ⚠️")
        if pr.get("suspended"):
            return ("suspended", "Приостановлен ⏸")
        # не приостановлен — проверим наличие в Xray
        try:
            info = XR.find_user(tg_id, pname)
            if info:
                return ("active", "Активен ▶️")
            else:
                return ("absent", "Отсутствует ⚠️")
        except Exception:
            return ("absent", "Отсутствует ⚠️")
    except Exception:
        return ("absent", "Отсутствует ⚠️")


def sanitize_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", (name or "").strip())


def _qr_png_bytes(text: str) -> bytes:
    img = qrcode.make(text)
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio.getvalue()


# ===== ЧТЕНИЕ ЛОГОВ =====

LOG_FILE_PATH = Path("/app/data/logs/bot.log")


def _tail_lines(path: Path, n: int = 50) -> list[str]:
    """Эффективно читает последние n строк текстового файла."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            size = end
            chunk = 1024
            data = b""
            while size > 0 and data.count(b"\n") <= n:
                jump = min(chunk, size)
                f.seek(end - jump)
                data = f.read(jump) + data
                end -= jump
                size -= jump
            lines = data.splitlines()
            return [ln.decode("utf-8", "replace") for ln in lines[-n:]]
    except Exception:
        return []


def _format_log_line(js: dict) -> str:
    """Делаем короткую человеческую строку из JSON-строки лога."""
    ts = js.get("ts", "-")
    ev = js.get("event", js.get("msg", "-"))
    lvl = js.get("level", "-")
    rid = js.get("rid", "-")
    uid = js.get("uid", "-")
    cmd = js.get("cmd", "-")
    et = js.get("error_type", "")
    if et:
        ev = f"{ev} ({et})"
    # Пример: 2025-11-03T05:55:10Z ERROR handler_error rid=abcd1234 uid=123 /status (RuntimeError)
    return f"{ts} {lvl:<5} {ev} rid={rid} uid={uid} cmd={cmd}"


async def _sync_report_send_or_edit(update, context, flt: str, mode: str):
    """
    Собирает данные, рисует первую часть с кнопками, хвостовые части — без кнопок.
    Перерисовывает текущее сообщение, если оно «последнее», иначе шлёт новое и удаляет старое.
    """
    data = sync_collect()
    # лог
    logger.info(
        {
            "event": "sync_report",
            "filter": flt,
            "mode": mode,
            **data.get("counters", {}),
        }
    )

    parts = sync_render(data, flt, mode)
    kb = build_sync_kb(flt, mode)

    # 1-я часть — через наш _edit_cb_with_fallback (он сам решит редактировать или слать новое + удалить старое)
    m = await _edit_cb_with_fallback(
        update,
        context,
        parts[0],
        kb=kb,
        parse_mode="HTML",
    )

    # сохранить выбранные настройки в чат/юзера
    context.chat_data["sync_filter"] = flt
    context.chat_data["sync_mode"] = mode
    if m:
        context.user_data["last_bot_msg_id"] = m.message_id

    # Хвостовые части — просто досылаем подряд (без кнопок)
    if len(parts) > 1:
        for i, chunk in enumerate(parts[1:], start=2):
            caption = f"— продолжение ({i}/{len(parts)}) —"
            msg = await update.effective_chat.send_message(
                f"<i>{caption}</i>\n\n{chunk}",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            context.user_data["last_bot_msg_id"] = msg.message_id


def _sync_collect_probe() -> dict:
    """
    Собирает диагностику рассинхрона Xray ↔ state.json (read-only).
    Возвращает dict:
    {
      "ts": "iso",
      "rows": [
         {
            "tid": int,
            "username": str,
            "name": str,       # имя профиля
            "suspended": bool, # флаг в state.json
            "present": bool,   # найден ли в Xray (XR.find_user)
            "status": "active"|"suspended"|"absent",
            "label": "Активен ▶️"|"Приостановлен ⏸"|"Отсутствует ⚠️",
         },
         ...
      ],
      "totals": {"active":N, "suspended":N, "absent":N, "all":N, "users":N}
    }
    """
    st = load_state()
    rows: list[dict] = []
    t_active = t_susp = t_absent = 0
    user_count = 0

    users = st.get("users", {}) if isinstance(st, dict) else {}
    for tid_str, rec in users.items():
        try:
            tid = int(tid_str)
        except Exception:
            continue
        user_count += 1
        uname = rec.get("username") or ""
        for p in _iter_xray_profiles(rec):
            pname = p.get("name") or "-"
            is_susp = bool(p.get("suspended"))
            # Проверяем наличие в Xray только если профиль не отмечен как удалён
            present = False
            try:
                present = bool(XR.find_user(tid, pname))
            except Exception:
                present = False

            if is_susp:
                status = "suspended"
                label = "Приостановлен ⏸"
                t_susp += 1
            else:
                if present:
                    status = "active"
                    label = "Активен ▶️"
                    t_active += 1
                else:
                    status = "absent"
                    label = "Отсутствует ⚠️"
                    t_absent += 1

            rows.append(
                {
                    "tid": tid,
                    "username": uname,
                    "name": pname,
                    "suspended": is_susp,
                    "present": present,
                    "status": status,
                    "label": label,
                }
            )

    probe = {
        "ts": now_iso(),
        "rows": rows,
        "totals": {
            "active": t_active,
            "suspended": t_susp,
            "absent": t_absent,
            "all": len(rows),
            "users": user_count,
        },
    }
    return probe


def _sync_render_page(
    probe: dict, page: int = 0, page_size: int = 10
) -> tuple[str, InlineKeyboardMarkup]:
    rows = probe.get("rows", [])
    totals = probe.get("totals", {})
    n = len(rows)
    pages = max(1, (n + page_size - 1) // page_size)
    page = max(0, min(page, pages - 1))
    a = page * page_size
    b = min(a + page_size, n)

    # Заголовок со сводкой
    head = [
        "🧩 <b>Синхронизация (диагностика)</b>",
        f"Время: <code>{probe.get('ts','-')}</code>",
        f"Пользователей: <b>{totals.get('users',0)}</b> · Профилей Xray: <b>{totals.get('all',0)}</b>",
        f"▶️ Активны: <b>{totals.get('active',0)}</b> · ⏸ Приостановлены: <b>{totals.get('suspended',0)}</b> · ⚠️ Отсутствуют: <b>{totals.get('absent',0)}</b>",
        "",
    ]

    # Тело страницы
    body: list[str] = []
    if n == 0:
        body.append("Нет Xray-профилей в базе.")
    else:
        for i, row in enumerate(rows[a:b], start=a + 1):
            tid = row["tid"]
            uname = ("@" + row["username"]) if row.get("username") else "—"
            pname = row["name"]
            label = row["label"]
            body.append(f"{i}. <code>{tid}</code> {uname} · <b>{pname}</b> — {label}")

    # Пагинация
    footer = [f"", f"Страница {page+1} из {pages}"]

    text = "\n".join(head + body + footer)

    # Кнопки: пагинация + обновить + назад
    nav_row = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️", callback_data=f"admin_sync_page:{page-1}")
        )
    if page < pages - 1:
        nav_row.append(
            InlineKeyboardButton("➡️", callback_data=f"admin_sync_page:{page+1}")
        )

    rows_kb = []
    if nav_row:
        rows_kb.append(nav_row)
    rows_kb.append(
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_sync_refresh")]
    )
    rows_kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin_menu")])

    return text, InlineKeyboardMarkup(rows_kb)


async def _sync_show(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    # 1) собрать свежую пробу
    probe = _sync_collect_probe()
    # 2) отрендерить страницу
    text, kb = _sync_render_page(probe, page=page, page_size=10)

    # 3) перерисовать текущее сообщение (если пришли из колбэка) или отправить новое
    if getattr(update, "callback_query", None) and update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=kb,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass

    await edit_or_send(update, context, text, kb, parse_mode="HTML", edit_last=True)


# ========= ОБОЛОЧКИ ДЛЯ КЛЮЧЕЙ AMNEZIA (vpn://) =========
def b64url_nopad(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def build_amnezia_wrapper_json(
    name: str, host: str, port: str, last_cfg_str: str
) -> str:
    wrapper = {
        "containers": [
            {
                "container": "amnezia-xray",
                "xray": {
                    "last_config": last_cfg_str,
                    "port": port,
                    "transport_proto": "tcp",
                },
            }
        ],
        "defaultContainer": "amnezia-xray",
        "description": name,
        "dns1": "1.1.1.1",
        "dns2": "1.0.0.1",
        "hostName": host,
        "nameOverriddenByUser": True,
    }
    return json.dumps(wrapper, ensure_ascii=False, separators=(",", ":"))


def make_vpn_url_from_json_str(wrapper_json: str) -> str:
    header4 = b"\x00\x00\x07\x43"
    comp = zlib.compress(wrapper_json.encode("utf-8"), level=9)
    return "vpn://" + b64url_nopad(header4 + comp)


# ========= ВСПОМОГАТЕЛЬНОЕ ДЛЯ UI =========


def main_menu_text(user: dict, is_admin: bool) -> str:
    first = user.get("first_name") or ""
    x_count = len([p for p in profiles_active(user) if p.get("type") == "xray"])
    awg_count = len(
        [p for p in profiles_active(user) if p.get("type") in ("amneziawg", "awg")]
    )

    badge = "👑 Администратор\n" if is_admin else ""
    greet = f"👋 Привет, {first}!\n" if first else "👋 Привет!\n"
    limits = (
        f"Лимиты: Xray — {MAX_XRAY}, AmneziaWG — {MAX_AWG} (всего до {MAX_XRAY + MAX_AWG}).\n"
        f"Сейчас: Xray — {x_count}, AmneziaWG — {awg_count}.\n"
    )
    return (greet + badge + "\n" + limits + "Выберите действие:").strip()


def main_menu_kb(allowed: bool, is_admin: bool = False) -> InlineKeyboardMarkup:
    if not allowed:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔓 Запросить доступ", callback_data="req_access")]]
        )

    rows = [
        [InlineKeyboardButton("👤 Мои конфигурации", callback_data="my_profiles")],
        [InlineKeyboardButton("➕ Новая конфигурация", callback_data="create")],
        [InlineKeyboardButton("ℹ️ Помощь", callback_data="help_menu")],
    ]

    if is_admin:
        rows.append(
            [
                InlineKeyboardButton(
                    "📊 Статус инфраструктуры", callback_data="status_refresh"
                )
            ]
        )
        rows.append([InlineKeyboardButton("🩺 Health", callback_data="status_health")])
        rows.append(
            [
                InlineKeyboardButton(
                    "🛠 Панель администратора", callback_data="admin_menu"
                )
            ]
        )

    return InlineKeyboardMarkup(rows)


def back_kb(cb: str = "menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=cb)]])


# ========= ОБРАБОТЧИКИ =========
async def show_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    welcome: bool = False,
    prefer_edit: bool = False,
):
    st = load_state()
    u = update.effective_user
    user = ensure_user_bucket(st, u.id, u.username or "", u.first_name or "")
    save_state(st)

    is_admin = is_admin_id(u.id)
    allowed = user.get("allowed", False) or is_admin

    if not allowed:
        uname_label = ("@" + (u.username or "")).strip() if (u.username or "") else "—"
        txt = (
            f"Ваш ID: <code>{u.id}</code>\n"
            f"Ваш логин: <code>{uname_label}</code>\n\n"
            "Передайте эти данные администратору для выдачи доступа.\n"
            "Вы также можете отправить заявку кнопкой ниже."
        )
        if prefer_edit:
            await edit_or_send(
                update,
                context,
                txt,
                main_menu_kb(False),
                add_menu_button=False,
                parse_mode="HTML",
            )
        else:
            await clean_and_send(
                update,
                context,
                txt,
                main_menu_kb(False),
                add_menu_button=False,
                parse_mode="HTML",
            )
        return

    txt = main_menu_text(user, is_admin)
    kb = main_menu_kb(True, is_admin)

    if prefer_edit:
        await edit_or_send(update, context, txt, kb, add_menu_button=False)
    else:
        await clean_and_send(update, context, txt, kb, add_menu_button=False)


async def show_app_picker(update, context, pname: str, for_edit: bool = True):
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🌐 Другие клиенты (VLESS)",
                    callback_data=f"prof_app_generic:{pname}",
                )
            ],
            [
                InlineKeyboardButton(
                    "🛡 AmneziaVPN", callback_data=f"prof_app_amnezia:{pname}"
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"prof_open:{pname}:xray")],
        ]
    )
    txt = f"Выберите приложение для <b>{pname}</b> · Xray"
    if for_edit:
        await edit_or_send(update, context, txt, kb, parse_mode="HTML")
    else:
        await clean_and_send(update, context, txt, kb, parse_mode="HTML")


@autoclean_command_input
@with_request_id
@log_command
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context, welcome=True)


@autoclean_command_input
@with_request_id
@log_command
async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context, welcome=False)


@autoclean_command_input
async def cmd_my(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    is_admin = u.id in ADMIN_IDS
    lines = [
        f"👤 <b>{u.full_name}</b> @{u.username or '-'}",
        f"🆔 <code>{u.id}</code>",
        f"🔐 Админ: {'<b>да</b>' if is_admin else 'нет'}",
        f"📦 Лимиты: MAX_PROFILES={os.getenv('MAX_PROFILES','-')}, MAX_XRAY={os.getenv('MAX_XRAY','-')}, MAX_AWG={os.getenv('MAX_AWG','-')}",
    ]
    await update.effective_message.reply_html("\n".join(lines))


@autoclean_command_input
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await edit_or_send(
        update,
        context,
        f"Доступные действия:\n"
        f"• ➕ Создать конфигурацию — выбрать протокол и имя\n"
        f"• 📄 Мои конфигурации — список, выдача ключей/файлов, удаление\n\n"
        f"Лимиты: Xray — {MAX_XRAY}, AmneziaWG — {MAX_AWG}.\n\n"
        "Команды:\n"
        "/start — главное меню\n"
        "/admin — панель администратора",
    )


@with_request_id
@log_command
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    # дефолты из ENV
    try:
        cb_debounce_ms = int(os.getenv("CB_DEBOUNCE_MS", "2000"))
    except Exception:
        cb_debounce_ms = 2000

    try:
        loader_cooldown_sec = int(os.getenv("STATUS_LOADER_COOLDOWN_SEC", "5"))
    except Exception:
        loader_cooldown_sec = 5

    # === антидубль колбэков ===
    try:
        key = (
            update.effective_chat.id if update.effective_chat else 0,
            query.message.message_id if getattr(query, "message", None) else 0,
            data,
        )
        now_ts = time.time()
        last = context.chat_data.get("_last_cb")
        debounce = cb_debounce_ms / 1000.0
        if last and last.get("key") == key and (now_ts - last.get("ts", 0)) < debounce:
            return
        context.chat_data["_last_cb"] = {"key": key, "ts": now_ts}
    except Exception:
        pass
    # === /антидубль ===

    # ===== Кнопки статуса (теперь только refresh) =====
    if data == "status_refresh":
        # покажем аккуратный лоудер внизу текста (не спамим чаще cooldown)
        try:
            now_ts = time.time()
            last_ts = float(context.chat_data.get("_last_full_loader_ts", 0))
            if (now_ts - last_ts) >= loader_cooldown_sec:
                curr = (query.message.text or "").rstrip()
                loader = "⏳ <b>Обновляю…</b>\n<i>Секунду…</i>"
                if "Обновляю" not in curr and "Загружаю" not in curr:
                    preview = (curr + ("\n\n" if curr else "") + loader).strip()
                    await query.edit_message_text(
                        preview,
                        reply_markup=build_status_kb(),
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                context.chat_data["_last_full_loader_ts"] = now_ts
        except Exception:
            pass

        # перерисовка через cmd_status — он сам заменит это же сообщение на полный статус
        context.chat_data["_allow_nested_from_cb"] = True  # разрешить вложенный вызов
        context.chat_data["_suppress_log_once"] = True  # не дублировать логи
        await cmd_status(update, context)
        return

    if data == "status_to_menu":
        # Перерисовываем ЭТО ЖЕ сообщение в главное меню
        await show_menu(update, context, welcome=False, prefer_edit=True)
        return

    if data == "menu":
        st = load_state()
        u = update.effective_user
        user = ensure_user_bucket(st, u.id, u.username or "", u.first_name or "")
        save_state(st)
        await show_menu(update, context, welcome=False, prefer_edit=False)
        return

    st = load_state()
    u = update.effective_user
    user = ensure_user_bucket(st, u.id, u.username or "", u.first_name or "")
    save_state(st)

    if data == "req_access":
        if is_admin_id(u.id):
            await edit_or_send(
                update, context, "У вас уже есть полный доступ как у администратора."
            )
            return
        txt = f"Заявка на доступ:\nID: `{u.id}`  username: `@{u.username}`"
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Одобрить", callback_data=f"admin_approve:{u.id}"
                    )
                ]
            ]
        )
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(chat_id=aid, text=txt, reply_markup=kb)
            except Exception:
                pass
        await edit_or_send(
            update, context, "Заявка отправлена администратору. Ожидайте одобрения."
        )
        return

    if not (user.get("allowed", False) or is_admin_id(u.id)):
        st2 = load_state()
        rec2 = st2.get("users", {}).get(str(u.id), {})
        if rec2.get("allowed", False):
            await show_menu(update, context, welcome=False, prefer_edit=True)
            return
        await edit_or_send(
            update, context, "⛔ Доступ пока не выдан. Обратитесь к администратору."
        )
        return

    if data == "create":
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Xray (Reality/VLESS)", callback_data="create_type:xray"
                    )
                ],
                [InlineKeyboardButton("AmneziaWG", callback_data="create_type:awg")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
            ]
        )
        await edit_or_send(update, context, "Выберите протокол:", kb)
        return

    if data.startswith("create_type:"):
        typ = data.split(":", 1)[1]
        context.user_data["create_typ"] = (
            "amneziawg" if typ in ("awg", "amneziawg") else typ
        )
        context.user_data["awaiting_name"] = True
        await edit_or_send(
            update,
            context,
            "Введите имя конфигурации (латиница/цифры/._-):",
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="create")]]
            ),
        )
        return

    if data == "my_profiles":
        # Stage 0 freeze: профилей в state.json больше нет; список будет из clientsTable на следующем этапе
        active = []
        if not active:
            empty_kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "➕ Создать конфигурацию", callback_data="create"
                        )
                    ],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ]
            )
            await edit_or_send(
                update, context, "У вас пока нет конфигураций.", empty_kb
            )
            return
        rows = []
        for p in active:
            label = p["name"]
            t = p["type"]
            # добавим значок статуса только для xray
            if t == "xray":
                status, _ = xray_profile_status_for_user(user, u.id, p["name"])
                if status == "active":
                    label = f"{label} · ▶️"
                elif status == "suspended":
                    label = f"{label} · ⏸"
                else:
                    label = f"{label} · ⚠️"
            else:
                # для awg пока без статусов
                label = f"{label} · {t}"
            rows.append(
                [
                    InlineKeyboardButton(
                        label, callback_data=f"prof_open:{p['name']}:{t}"
                    )
                ]
            )
        await edit_or_send(
            update, context, "Ваши конфигурации:", InlineKeyboardMarkup(rows)
        )
        return

    if data.startswith("prof_open:"):
        _, pname, ptype = data.split(":", 2)
        pr = next(
            (
                p
                for p in profiles_active(user)
                if p["name"] == pname and p["type"] == ptype
            ),
            None,
        )
        if not pr:
            await edit_or_send(
                update,
                context,
                SAFE_TXT,
                InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                ),
            )
            return
        if ptype == "xray":
            # статус профиля
            status, status_label = xray_profile_status_for_user(user, u.id, pname)
            info = None
            if status != "absent":
                try:
                    info = XR.find_user(u.id, pname)
                except Exception:
                    info = None

            lines = [f"<b>{pname}</b> · Xray"]
            if info:
                lines.append(f"• UUID: <code>{info['uuid']}</code>")
                lines.append(f"• SNI: <code>{info['sni']}</code>")
                lines.append(f"• Port: <code>{info['port']}</code>")
            lines.append(f"• Статус: <b>{status_label}</b>")

            # Кнопки: выдачу настроек показываем только если активен
            rows = []
            if status == "active":
                rows.append(
                    [
                        InlineKeyboardButton(
                            "📱 Получить настройки",
                            callback_data=f"prof_get_app:{pname}",
                        )
                    ]
                )
            else:
                # подсказывающее сообщение
                if status == "suspended":
                    lines.append(
                        "Профиль приостановлен администратором — выдача ключей временно недоступна."
                    )
                else:
                    lines.append(
                        "Профиль не найден на сервере Xray — обратитесь к администратору или пересоздайте конфигурацию."
                    )

            rows.append(
                [
                    InlineKeyboardButton(
                        "🗑 Удалить", callback_data=f"prof_del:{pname}:{ptype}"
                    )
                ]
            )
            rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")])
            kb = InlineKeyboardMarkup(rows)
            text = "\n".join(lines)
            await edit_or_send(update, context, text, kb, parse_mode="HTML")
            return
        elif ptype in ("amneziawg", "awg"):
            # Показываем карточку AWG без обращения к устаревшим find_user
            try:
                fac = AWG.facts()
                listen_port = fac.get("listen_port")
            except Exception:
                listen_port = None
            ep = f"{AWG_CONNECT_HOST}:{listen_port}" if listen_port else ""
            kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔑 Ключ для Amnezia (vpn://)",
                            callback_data=f"prof_get_vpn:{pname}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "🗑 Удалить", callback_data=f"prof_del:{pname}:amneziawg"
                        )
                    ],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")],
                ]
            )
            text = (
                f"<b>{pname}</b> · AmneziaWG\n"
                + (f"• Endpoint: <code>{ep}</code>\n" if ep else "")
                + (f"• Port: <code>{listen_port}</code>\n" if listen_port else "")
            )
            await edit_or_send(update, context, text or "AmneziaWG", kb, parse_mode="HTML")
            return

    if data.startswith("prof_get_vpn:"):
        pname = data.split(":", 1)[1]
        prof = next((p for p in profiles_active(user) if p.get("name") == pname), None)
        if not prof:
            await edit_or_send(
                update,
                context,
                "Конфигурация не найдена.",
                InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                ),
            )
            return
        ptype = prof.get("type")
        if ptype == "xray":
            info_x = XR.find_user(u.id, pname)
            if not info_x:
                await edit_or_send(
                    update,
                    context,
                    "Конфигурация Xray не найдена в конфиге сервера.",
                    InlineKeyboardMarkup(
                        [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                    ),
                )
                return
            wrapper = build_amnezia_wrapper_json(
                pname, XRAY_CONNECT_HOST, info_x["port"], info_x["last_config_str"]
            )
            vpn_str = make_vpn_url_from_json_str(wrapper)
            text = (
                f"<b>{pname} — ключи для Amnezia (Xray)</b>\n\n<code>{vpn_str}</code>"
            )
            await edit_or_send(
                update,
                context,
                text,
                InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                ),
                parse_mode="HTML",
            )
            return
        if ptype in ("amneziawg", "awg"):
            stored_vpn = prof.get("vpn_url")
            if stored_vpn:
                text = f"<b>{pname} — ключи для Amnezia (AmneziaWG)</b>\n\n<code>{stored_vpn}</code>"
                await edit_or_send(
                    update,
                    context,
                    text,
                    InlineKeyboardMarkup(
                        [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                    ),
                    parse_mode="HTML",
                )
                return
            # Старые записи могли не сохранять vpn_url — надёжнее пересоздать
            await edit_or_send(
                update,
                context,
                "Конфигурация AmneziaWG создана старой версией бота без сохранения ключа импорта.\nПересоздайте конфигурацию для получения строки импорта.",
                InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                ),
            )
            return
        await edit_or_send(
            update,
            context,
            "Неизвестный тип конфигурации.",
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
            ),
        )
        return

    if data.startswith("prof_get_uri:"):
        pname = data.split(":", 1)[1]
        status_enum, status_label = xray_profile_status_for_user(user, u.id, pname)
        if status_enum != "active":
            await edit_or_send(
                update,
                context,
                f"<b>{pname}</b> · Xray\nСтатус: <b>{status_label}</b>\n\nВыдача URI недоступна.",
                InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                ),
                parse_mode="HTML",
            )
            return
        info = XR.find_user(u.id, pname)
        if not info:
            await edit_or_send(
                update,
                context,
                "Конфигурация Xray не найдена в конфиге сервера.",
                InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
                ),
            )
            return

    if data.startswith("prof_del:"):
        _, pname, ptype = data.split(":", 2)
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Да, удалить",
                        callback_data=f"prof_del_confirm:{pname}:{ptype}",
                    ),
                    InlineKeyboardButton(
                        "❌ Отмена", callback_data=f"prof_open:{pname}:{ptype}"
                    ),
                ],
                [InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")],
            ]
        )
        await edit_or_send(
            update,
            context,
            f"Удалить конфигурацию <b>{pname}</b> ({ptype})? Это действие необратимо.",
            kb,
            parse_mode="HTML",
        )
        return

    if data.startswith("prof_del_confirm:"):
        _, pname, ptype = data.split(":", 2)
        ok = False
        try:
            if ptype == "xray":
                ok = XR.remove_user_by_name(u.id, pname)
            elif ptype in ("amneziawg", "awg"):
                prof = next((p for p in profiles_active(user) if p["name"] == pname and p["type"] in ("amneziawg", "awg")), None)
                if prof and prof.get("uuid"):
                    ok = AWG.delete_profile_by_uuid(prof["uuid"])
                else:
                    ok = False
        except Exception:
            ok = False
        txt = (
            "Конфигурация удалена ✅"
            if ok
            else "Конфигурация не найдена на сервере, но помечена удалённой локально."
        )
        await edit_or_send(
            update,
            context,
            txt,
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="my_profiles")]]
            ),
        )
        return

    if data.startswith("prof_get_app:"):
        pname = data.split(":", 1)[1]
        await show_app_picker(update, context, pname, for_edit=True)
        return

    if data.startswith("prof_app_generic:"):
        pname = data.split(":", 1)[1]
        status, _ = xray_profile_status_for_user(user, update.effective_user.id, pname)
        if status != "active":
            msg = "Профиль недоступен для выдачи настроек: "
            msg += (
                "приостановлен ⏸." if status == "suspended" else "отсутствует в Xray ⚠️."
            )
            await edit_or_send(
                update,
                context,
                msg,
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return

        info = XR.find_user(update.effective_user.id, pname)
        if not info:
            await edit_or_send(
                update,
                context,
                "Конфигурация Xray не найдена в конфиге сервера.",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return
        vless = info["uri"]
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🧾 Показать QR-код",
                        callback_data=f"prof_toggle_qr_vless:{pname}:showqr",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                    )
                ],
            ]
        )
        txt = f"<b>{pname}</b> · VLESS (для v2rayNG / Nekoray / Clash)\n\n<code>{vless}</code>"
        await edit_or_send(update, context, txt, kb, parse_mode="HTML")
        return

    if data.startswith("prof_toggle_qr_vless:"):
        _, rest = data.split(":", 1)
        pname, action = rest.rsplit(":", 1)

        # статус — QR только для активного профиля
        status, _ = xray_profile_status_for_user(user, update.effective_user.id, pname)
        if status != "active":
            await edit_or_send(
                update,
                context,
                "Профиль недоступен: неактивен для выдачи QR.",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return

        info = XR.find_user(update.effective_user.id, pname)
        if not info:
            await edit_or_send(
                update,
                context,
                "Конфигурация Xray не найдена в конфиге сервера.",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return

        vless = info["uri"]

        # 1) удаляем СООБЩЕНИЕ, из которого пришёл колбэк (это всегда актуальное)
        try:
            if update and update.callback_query and update.callback_query.message:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=update.callback_query.message.message_id,
                )
        except Exception:
            pass

        # 2) отправляем новое — либо фото (QR), либо текст (URI), и сохраняем id
        ud = context.user_data
        if action == "showqr":
            png = _qr_png_bytes(vless)
            kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔗 Показать URI",
                            callback_data=f"prof_toggle_qr_vless:{pname}:showuri",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                        )
                    ],
                ]
            )
            msg = await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=png,
                caption=f"{pname} · VLESS (QR)",
                reply_markup=kb,
            )
            ud["last_bot_msg_id"] = msg.message_id
            return

        # action == "showuri"
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🧾 Показать QR-код",
                        callback_data=f"prof_toggle_qr_vless:{pname}:showqr",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                    )
                ],
            ]
        )
        txt = f"<b>{pname}</b> · VLESS (для v2rayNG / Nekoray / Clash)\n\n<code>{vless}</code>"
        msg = await update.effective_chat.send_message(
            txt, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True
        )
        ud["last_bot_msg_id"] = msg.message_id
        return

    if data.startswith("prof_app_amnezia:"):
        pname = data.split(":", 1)[1]
        st = load_state()
        u = update.effective_user
        user = ensure_user_bucket(st, u.id, u.username or "", u.first_name or "")
        save_state(st)

        pr = next(
            (
                p
                for p in profiles_active(user)
                if p.get("name") == pname and p.get("type") == "xray"
            ),
            None,
        )
        if not pr:
            await edit_or_send(
                update,
                context,
                "Конфигурация не найдена.",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return

        # ★ ПРОВЕРКА СТАТУСА: активен ли профиль на сервере Xray?
        status_enum, status_label = xray_profile_status_for_user(user, u.id, pname)  # ★
        if status_enum != "active":  # ★
            await edit_or_send(  # ★
                update,
                context,  # ★
                f"<b>{pname}</b> · Xray\nСтатус: <b>{status_label}</b>\n\nВыдача ключей для Amnezia недоступна.",  # ★
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_open:{pname}:xray"
                            )
                        ]
                    ]
                ),  # ★
                parse_mode="HTML",  # ★
            )  # ★
            return  # ★

        info_x = XR.find_user(u.id, pname)
        if not info_x:
            await edit_or_send(
                update,
                context,
                "Конфигурация Xray не найдена в конфиге сервера.",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return

        status, _ = xray_profile_status_for_user(user, u.id, pname)
        if status != "active":
            await edit_or_send(
                update,
                context,
                "Профиль недоступен для импорта в Amnezia: не активен.",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад", callback_data=f"prof_get_app:{pname}"
                            )
                        ]
                    ]
                ),
            )
            return

        wrapper = build_amnezia_wrapper_json(
            pname, XRAY_CONNECT_HOST, info_x["port"], info_x["last_config_str"]
        )
        vpn_str = make_vpn_url_from_json_str(wrapper)
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data=f"prof_get_app:{pname}")]]
        )
        txt = f"<b>{pname} — ключ для Amnezia</b>\n\n<code>{vpn_str}</code>"
        await edit_or_send(update, context, txt, kb, parse_mode="HTML")
        return

    if data == "help_menu":
        txt = (
            "Доступные действия:\n"
            "• ➕ Новая конфигурация — выбрать протокол и имя\n"
            "• 👤 Мои конфигурации — выдача ключей/файлов, удаление\n\n"
            f"Лимиты: Xray — {MAX_XRAY}, AmneziaWG — {MAX_AWG}.\n"
        )
        await edit_or_send(update, context, txt, back_kb("menu"))
        return

    # ===== /sync: фильтры/режим/обновление =====
    if data.startswith("sync_filter:"):
        flt = data.split(":", 1)[1]
        if flt not in SYNC_FILTERS:
            flt = SYNC_DEFAULT_FILTER
        mode = context.chat_data.get("sync_mode", SYNC_DEFAULT_MODE)
        await _sync_report_send_or_edit(update, context, flt, mode)
        return

    if data.startswith("sync_mode:"):
        mode = data.split(":", 1)[1]
        if mode not in ("compact", "detailed"):
            mode = SYNC_DEFAULT_MODE
        flt = context.chat_data.get("sync_filter", SYNC_DEFAULT_FILTER)
        await _sync_report_send_or_edit(update, context, flt, mode)
        return

    if data == "sync_refresh":
        flt = context.chat_data.get("sync_filter", SYNC_DEFAULT_FILTER)
        mode = context.chat_data.get("sync_mode", SYNC_DEFAULT_MODE)
        await _sync_report_send_or_edit(update, context, flt, mode)
        return

    # ===== Админские колбэки =====
    if data.startswith("admin_approve:"):
        target_id = int(data.split(":", 1)[1])
        st = load_state()
        tu = st["users"].get(str(target_id))
        if not tu:
            await edit_or_send(update, context, "Пользователь не найден в БД.")
            return
        tu["allowed"] = True
        tu["allowed_at"] = now_iso()
        tu["allowed_by"] = update.effective_user.id
        save_state(st)
        await edit_or_send(
            update,
            context,
            f"Доступ выдан пользователю <code>{target_id}</code>.",
            parse_mode="HTML",
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ Доступ к боту одобрен.\n\nИспользуйте кнопки ниже.",
                reply_markup=main_menu_kb(True, is_admin=False),
            )
        except Exception:
            pass
        return

    if data == "admin_menu":
        await show_admin_menu(update, context, edit=True)
        return

    if data == "admin_add":
        context.user_data["admin_mode"] = "await_user_id_or_username"
        await edit_or_send(
            update,
            context,
            "Отправьте ID пользователя или @username для выдачи доступа.",
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="admin_menu")]]
            ),
        )
        return

    if data == "admin_list":
        await show_admin_user_list(update, context, page=0)
        return

    if data.startswith("admin_list_page:"):
        page = int(data.split(":", 1)[1])
        await show_admin_user_list(update, context, page=page)
        return

    if data.startswith("admin_user_open:"):
        tid = data.split(":", 1)[1]
        await show_admin_user_card(update, context, tid)
        return

    if data.startswith("admin_user_toggle:"):
        tid = data.split(":", 1)[1]
        st = load_state()
        urec = st["users"].get(tid)
        if not urec:
            await edit_or_send(update, context, "Пользователь не найден.")
            return

        new_allowed = not urec.get("allowed", False)

        if new_allowed:
            # Разрешаем доступ
            urec["allowed"] = True
            urec["allowed_at"] = now_iso()
            urec["allowed_by"] = update.effective_user.id
            save_state(st)

            _notify_user_simple(
                context,
                int(tid),
                "✅ Вам вновь выдан доступ к боту. Откройте меню, чтобы управлять конфигурациями.",
            )

            # Остаёмся на той же карточке
            await show_admin_user_card(
                update, context, tid, replace=True, note="✅ Доступ разрешён."
            )
            return

        # Запрещаем доступ + автоприостановка Xray
        urec["allowed"] = False
        save_state(st)

        # Промежуточный лоудер в ТОЙ ЖЕ карточке
        await edit_or_send(
            update,
            context,
            "⏳ Приостанавливаю Xray-профили пользователя…",
            None,
            parse_mode="HTML",
            edit_last=True,
        )

        total, done, skipped = _auto_suspend_all_xray(st, int(tid))
        save_state(st)

        _notify_user_simple(
            context,
            int(tid),
            "⛔ Ваш доступ к боту отозван."
            + (
                f"\n⏸ Ваши Xray-профили приостановлены ({done} из {total})."
                if total
                else ""
            ),
        )

        note = f"⛔ Доступ запрещён. ⏸ Приостановлено: {done} из {total}." + (
            f" Пропущено: {skipped}." if skipped else ""
        )
        # Возвращаемся на карточку пользователя (без перехода в список конфигов)
        await show_admin_user_card(update, context, tid, replace=True, note=note)
        return

    if data.startswith("admin_user_profiles:"):
        tid = data.split(":", 1)[1]
        await show_admin_user_profiles(update, context, tid)
        return

    if data.startswith("admin_prof_open:"):
        _, tid, pname, ptype = data.split(":", 3)
        await show_admin_profile_card(update, context, tid, pname, ptype)
        return

    if data.startswith("admin_prof_del:"):
        _, tid, pname, ptype = data.split(":", 3)
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Да, удалить",
                        callback_data=f"admin_prof_del_confirm:{tid}:{pname}:{ptype}",
                    ),
                    InlineKeyboardButton(
                        "❌ Отмена",
                        callback_data=f"admin_prof_open:{tid}:{pname}:{ptype}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад", callback_data=f"admin_user_profiles:{tid}"
                    )
                ],
            ]
        )
        await edit_or_send(
            update,
            context,
            f"Удалить конфигурацию <b>{pname}</b> ({ptype}) у пользователя <code>{tid}</code>?",
            kb,
            parse_mode="HTML",
        )
        return

    if data.startswith("admin_prof_del_confirm:"):
        _, tid, pname, ptype = data.split(":", 3)
        try:
            if ptype == "xray":
                XR.remove_user_by_name(int(tid), pname)
            elif ptype in ("amneziawg", "awg"):
                # Удаляем AWG-профиль по IP из записи пользователя
                ip_cidr = None
                st = load_state()
                urec = st["users"].get(tid, {})
                for p in urec.get("profiles", []):
                    if p.get("name") == pname and p.get("type") in ("amneziawg", "awg") and not p.get("deleted"):
                        ip_cidr = p.get("assigned_ip") or ""
                        break
                if ip_cidr:
                    try:
                        AWG.delete_profile_by_uuid(p.get("uuid"))
                    except Exception:
                        pass
        except Exception:
            pass
        await show_admin_user_profiles(
            update, context, tid, note="Конфигурация удалена."
        )
        return

    if data.startswith("admin_prof_suspend:"):
        _, tid, pname = data.split(":", 2)
        # найти профиль в state
        st = load_state()
        urec = st["users"].get(tid, {})
        pr = next(
            (
                p
                for p in profiles_active(urec)
                if p.get("name") == pname and p.get("type") == "xray"
            ),
            None,
        )
        if not pr:
            await show_admin_user_profiles(
                update, context, tid, note="Профиль не найден."
            )
            return
        # вызвать XR.suspend_user_by_name
        snap = XR.suspend_user_by_name(int(tid), pname)
        if snap:
            pr["suspended"] = True
            pr["susp_uuid"] = snap.get("uuid")
            pr["susp_flow"] = snap.get("flow")
            save_state(st)
            await show_admin_profile_card(
                update, context, tid, pname, "xray", note="Профиль приостановлен."
            )
        else:
            await show_admin_profile_card(
                update,
                context,
                tid,
                pname,
                "xray",
                note="Профиль уже отсутствует в Xray (возможно, уже приостановлен/удалён).",
            )
        return

    if data.startswith("admin_prof_resume:"):
        _, tid, pname = data.split(":", 2)
        st = load_state()
        urec = st["users"].get(tid, {})
        # ⬇️ блок: если доступ снят — сразу выходим с пояснением
        if not urec.get("allowed", False):
            await show_admin_profile_card(
                update,
                context,
                tid,
                pname,
                "xray",
                note="🔒 Доступ у пользователя снят — возобновление отклонено.",
            )
            return

        pr = next(
            (
                p
                for p in profiles_active(urec)
                if p.get("name") == pname and p.get("type") == "xray"
            ),
            None,
        )
        if not pr:
            await show_admin_user_profiles(
                update, context, tid, note="Профиль не найден."
            )
            return
        uuid = pr.get("susp_uuid") or pr.get("uuid")
        flow = pr.get("susp_flow")  # опционально
        ok = False
        if uuid:
            ok = XR.resume_user_by_name(int(tid), pname, uuid, flow)
        if ok:
            pr["suspended"] = False
            pr["uuid"] = uuid
            save_state(st)
            await show_admin_profile_card(
                update, context, tid, pname, "xray", note="Профиль возобновлён."
            )
        else:
            await show_admin_profile_card(
                update,
                context,
                tid,
                pname,
                "xray",
                note="Не удалось возобновить (см. логи).",
            )
        return

    # === Массово: приостановить все Xray профили пользователя ===
    if data.startswith("admin_user_suspend_all_xray:"):
        tid = data.split(":", 1)[1]
        st = load_state()
        urec = st["users"].get(tid, {})
        if not urec:
            await edit_or_send(
                update, context, "Пользователь не найден.", back_kb("admin_list")
            )
            return

        # ⏳ предварительное уведомление
        await edit_or_send(
            update, context, "⏳ Приостанавливаю все Xray-профили…", None
        )

        total = done = skipped = 0
        for p in profiles_active(urec):
            if p.get("type") != "xray":
                continue
            total += 1
            if p.get("suspended"):
                skipped += 1
                continue
            snap = XR.suspend_user_by_name(int(tid), p["name"])
            if snap:
                p["suspended"] = True
                p["susp_uuid"] = snap.get("uuid")
                p["susp_flow"] = snap.get("flow")
                done += 1
            else:
                skipped += 1

        save_state(st)
        note = f"⏸ Приостановлено: {done} из {total}." + (
            f" Пропущено: {skipped}." if skipped else ""
        )
        await show_admin_user_profiles(update, context, tid, note=note)
        return

    # === Массово: возобновить все Xray профили пользователя ===
    if data.startswith("admin_user_resume_all_xray:"):
        tid = data.split(":", 1)[1]
        st = load_state()
        urec = st["users"].get(tid, {})
        if not urec:
            await edit_or_send(
                update, context, "Пользователь не найден.", back_kb("admin_list")
            )
            return

        # ⬇️ блокирующая проверка
        if not urec.get("allowed", False):
            await show_admin_user_profiles(
                update,
                context,
                tid,
                note="🔒 Доступ у пользователя снят — массовое возобновление заблокировано.",
            )
            return

        # ⏳ предварительное уведомление
        await edit_or_send(update, context, "🔁 Возобновляю все Xray-профили…", None)

        total = done = skipped = 0
        for p in profiles_active(urec):
            if p.get("type") != "xray":
                continue
            total += 1
            if not p.get("suspended"):
                skipped += 1
                continue
            uuid = p.get("susp_uuid") or p.get("uuid")
            flow = p.get("susp_flow")
            ok = False
            if uuid:
                ok = XR.resume_user_by_name(int(tid), p["name"], uuid, flow)
            if ok:
                p["suspended"] = False
                if uuid:
                    p["uuid"] = uuid
                done += 1
            else:
                skipped += 1

        save_state(st)
        note = f"▶️ Возобновлено: {done} из {total}." + (
            f" Пропущено: {skipped}." if skipped else ""
        )
        await show_admin_user_profiles(update, context, tid, note=note)
        return
    # === /sync массовые действия (только "свои" записи) ===
    if data == "sync_apply_absent_all":
        # запускаем массовое добавление отсутствующих (только не suspended)
        summary = sync_absent_apply_all()
        text = (
            "🧩 <b>Починка отсутствующих завершена</b>\n"
            f"Всего: <b>{summary.get('total',0)}</b>\n"
            f"Выполнено: <b>{summary.get('done',0)}</b>\n"
            f"Пропущено: <b>{summary.get('skipped',0)}</b>\n"
            f"Ошибок: <b>{summary.get('errors',0)}</b>\n"
        )
        # покажем краткий результат и обновим отчёт
        await _edit_cb_with_fallback(update, context, text, parse_mode="HTML")
        flt = context.chat_data.get("sync_filter", SYNC_DEFAULT_FILTER)
        mode = context.chat_data.get("sync_mode", SYNC_DEFAULT_MODE)
        await _sync_report_send_or_edit(update, context, flt, mode)
        return

    if data == "sync_apply_diverged_db_all":
        summary = sync_diverged_update_db_all()
        txt = (
            "🧭 <b>Обновление БД по Xray (diverged)</b>\n"
            f"Всего: <b>{summary['total']}</b>\n"
            f"Обновлено: <b>{summary['done']}</b>\n"
            f"Пропущено: <b>{summary['skipped']}</b>\n"
            f"Ошибок: <b>{summary['errors']}</b>\n"
        )
        await edit_or_send(
            update,
            context,
            txt,
            InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Назад к отчёту", callback_data="sync_refresh"
                        )
                    ]
                ]
            ),
            parse_mode="HTML",
        )
        return

    if data == "sync_apply_diverged_xray_all":
        summary = sync_diverged_rebuild_xray_all()
        txt = (
            "🔁 <b>Пересборка в Xray по БД (diverged)</b>\n"
            f"Всего: <b>{summary['total']}</b>\n"
            f"Изменено: <b>{summary['done']}</b>\n"
            f"Пропущено: <b>{summary['skipped']}</b>\n"
            f"Ошибок: <b>{summary['errors']}</b>\n"
            "<i>Профили с suspended или у пользователей без доступа не менялись.</i>"
        )
        await edit_or_send(
            update,
            context,
            txt,
            InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Назад к отчёту", callback_data="sync_refresh"
                        )
                    ]
                ]
            ),
            parse_mode="HTML",
        )
        return

    if data == "sync_apply_extra_all":
        # запускаем массовое удаление лишних (только source=bot)
        summary = sync_extra_apply_all()
        text = (
            "🧹 <b>Удаление лишних завершено</b>\n"
            f"Всего: <b>{summary.get('total',0)}</b>\n"
            f"Выполнено: <b>{summary.get('done',0)}</b>\n"
            f"Пропущено: <b>{summary.get('skipped',0)}</b>\n"
            f"Ошибок: <b>{summary.get('errors',0)}</b>\n"
        )
        await _edit_cb_with_fallback(update, context, text, parse_mode="HTML")
        flt = context.chat_data.get("sync_filter", SYNC_DEFAULT_FILTER)
        mode = context.chat_data.get("sync_mode", SYNC_DEFAULT_MODE)
        await _sync_report_send_or_edit(update, context, flt, mode)
        return

    if data == "admin_sync":
        # Лоудер: если текущее сообщение не последнее — отправим новое и удалим старое
        try:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="admin_menu")]]
            )
            await _edit_cb_with_fallback(
                update,
                context,
                "⏳ Загружаю отчёт по синхронизации…",
                kb=kb,
                parse_mode="HTML",
            )
        except Exception:
            pass

        # Перерисуем этим же сообщением (или новым, если так решит fallback)
        context.chat_data["_allow_nested_from_cb"] = True
        context.chat_data["_suppress_log_once"] = True
        await cmd_sync(update, context)
        return

    if data == "admin_sync_refresh":
        # просто показать заново страницу 0 (свежая проба)
        context.chat_data["_allow_nested_from_cb"] = True
        context.chat_data["_suppress_log_once"] = True
        await _sync_show(update, context, page=0)
        return

    if data.startswith("admin_sync_page:"):
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 0
        context.chat_data["_allow_nested_from_cb"] = True
        context.chat_data["_suppress_log_once"] = True
        await _sync_show(update, context, page=page)
        return

    if data == "status_health":
        context.chat_data["_allow_nested_from_cb"] = True
        context.chat_data["_suppress_log_once"] = True
        await cmd_health(update, context)
        return


@with_request_id
@log_command
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # запомним последний message_id пользователя
    try:
        if getattr(update, "message", None) and update.message:
            context.chat_data["last_user_msg_id"] = update.message.message_id
    except Exception:
        pass
    st = load_state()
    u = update.effective_user
    user = ensure_user_bucket(st, u.id, u.username or "", u.first_name or "")
    save_state(st)

    if user.get("allowed") and context.user_data.get("awaiting_name"):
        name_raw = update.message.text or ""
        name = sanitize_name(name_raw)
        orig = (name_raw or "").strip()

        # Пустая строка
        if not orig:
            await update.message.reply_text(
                "Имя пустое. Введите имя латиницей: буквы, цифры, точка, дефис или подчёркивание."
            )
            return

        # Недопустимые символы — не принимаем (не молча заменяем)
        if orig != name:
            await update.message.reply_text(
                "Недопустимые символы. Разрешены: A–Z, a–z, 0–9, точка ., дефис -, подчёркивание _. Без пробелов."
            )
            return

        # Ограничение длины
        if len(name) > 32:
            await update.message.reply_text("Слишком длинное имя. Максимум 32 символа.")
            return
        typ = context.user_data.get("create_typ", "xray")
        if md_limit_reached(user, typ):
            limit_msg = (
                f"Достигнут лимит для {('Xray' if typ=='xray' else 'AmneziaWG')}: "
                + (str(MAX_XRAY) if typ == "xray" else str(MAX_AWG))
            )
            await update.message.reply_text(limit_msg)
            context.user_data.pop("awaiting_name", None)
            return
        if any(
            p["name"] == name and not p.get("deleted") for p in profiles_active(user)
        ):
            await update.message.reply_text(
                "Конфигурация с таким именем уже существует. Введите другое имя."
            )
            return

        try:
            if typ == "xray":
                # Stage 0 freeze: создаём в Xray, но НЕ пишем профили в state.json
                created = XR.add_user(u.id, name)
                try:
                    await update.message.delete()
                except Exception:
                    pass
                await show_app_picker(update, context, name, for_edit=True)

            elif typ in ("amneziawg", "awg"):
                meta = {"name": name, "owner_tid": u.id}
                prof_uuid = AWG.create_profile(meta)
                profile = AWG.find_profile_by_uuid(prof_uuid)
                facts_data = AWG.facts()

                created = {
                    "vpn_url": (
                        f"[Interface]\nAddress = {profile['userData']['ip']}/32\n"
                        f"PrivateKey = {profile['userData']['privateKey']}\n"
                        f"DNS = {facts_data.get('dns')}\n"
                        f"[Peer]\nPublicKey = {profile['clientId']}\n"
                        f"Endpoint = {facts_data.get('endpoint')}:{facts_data.get('port')}\n"
                        f"PresharedKey = {profile['userData']['psk']}\n"
                    ),
                    "endpoint": f"{facts_data.get('endpoint')}:{facts_data.get('port')}",
                    "assigned_ip": f"{profile['userData']['ip']}/32",
                    "pubkey": profile["clientId"],
                    "uuid": prof_uuid,
                }
                # Stage 0 freeze: не пишем профили в state.json
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="menu")]])
                try:
                    await update.message.delete()
                except Exception:
                    pass
                txt = (
                    f"<b>{name}</b> (AmneziaWG) создан ✅\n\n"
                    f"<b>Импорт в Amnezia:</b>\n<code>{created['vpn_url']}</code>\n\n"
                    f"<i>Endpoint:</i> <code>{created['endpoint']}</code>\n"
                    f"<i>IP:</i> <code>{created['assigned_ip']}</code>"
                )
                await edit_or_send(update, context, txt, kb, parse_mode="HTML")
            else:
                await update.message.reply_text("Неизвестный тип конфигурации.")
        except Exception as e:
            await update.message.reply_text(f"Ошибка: {e}")
        finally:
            context.user_data.pop("awaiting_name", None)
            context.user_data.pop("create_typ", None)
        return


async def show_admin_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False
):
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Добавить доступ", callback_data="admin_add")],
            [InlineKeyboardButton("👥 Все пользователи", callback_data="admin_list")],
            [
                InlineKeyboardButton(
                    "🔄 Синхронизация (диагностика)", callback_data="admin_sync"
                )
            ],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )
    txt = "Панель администратора"
    if edit and update.callback_query:
        await update.callback_query.edit_message_text(txt, reply_markup=kb)
    else:
        await edit_or_send(update, context, txt, kb)


@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_admin_menu(update, context, edit=False)


def resolve_user_id(arg: str) -> Optional[int]:
    st = load_state()
    arg = (arg or "").strip()
    if not arg:
        return None
    if arg.startswith("@"):
        uname = arg[1:].lower()
        for tid, rec in st.get("users", {}).items():
            if (rec.get("username") or "").lower() == uname:
                try:
                    return int(tid)
                except Exception:
                    return None
        return None
    if re.fullmatch(r"\d+", arg):
        try:
            return int(arg)
        except Exception:
            return None
    return None


@autoclean_command_input
@admin_only
async def cmd_allow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    arg = " ".join(context.args) if context.args else ""
    tid = resolve_user_id(arg)
    if not tid:
        await update.message.reply_text(
            "Укажите ID или @username: /allow 123456 или /allow @user"
        )
        return
    st = load_state()
    urec = st["users"].setdefault(
        str(tid),
        {
            "allowed": False,
            "username": "",
            "first_name": "",
            "profiles": [],
            "created_at": now_iso(),
        },
    )
    urec["allowed"] = True
    urec["allowed_at"] = now_iso()
    urec["allowed_by"] = update.effective_user.id
    save_state(st)
    await update.message.reply_text(
        f"✅ Доступ выдан <code>{tid}</code>", parse_mode="HTML"
    )
    try:
        await context.bot.send_message(
            chat_id=tid, text="✅ Доступ к боту одобрен. Воспользуйтесь меню."
        )
    except Exception:
        pass


@autoclean_command_input
@admin_only
async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    arg = " ".join(context.args) if context.args else ""
    tid = resolve_user_id(arg)
    if not tid:
        await update.message.reply_text(
            "Укажите ID или @username: /revoke 123456 или /revoke @user"
        )
        return

    st = load_state()
    urec = st["users"].get(str(tid))
    if not urec:
        await update.message.reply_text("Пользователь не найден.")
        return

    # 1) запрет доступа
    urec["allowed"] = False
    save_state(st)

    # 2) автоприостановка Xray-профилей
    total, done, skipped = _auto_suspend_all_xray(st, tid)
    save_state(st)

    # 3) итоги админу
    msg_admin = (
        f"⛔ Доступ запрещён <code>{tid}</code>.\n"
        f"⏸ Приостановлено Xray-профилей: {done} из {total}."
        + (f" Пропущено: {skipped}." if skipped else "")
    )
    await update.message.reply_html(msg_admin)

    # 4) уведомление пользователю (опционально)
    note_user = "⛔ Ваш доступ к боту отозван." + (
        f"\n⏸ Ваши Xray-профили приостановлены ({done} из {total})." if total else ""
    )
    _notify_user_simple(context, tid, note_user)


@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Пороговые значения из ENV
    warn_sec = int(os.getenv("HEALTH_WARN_SEC", "60"))
    crit_sec = int(os.getenv("HEALTH_CRIT_SEC", "180"))
    tcp_to = int(os.getenv("HEALTH_TCP_TIMEOUT_MS", "800"))

    ok, warn, crit = [], [], []

    # 1) Heartbeat
    try:
        age = time.time() - os.path.getmtime(HEARTBEAT_PATH)
        if age < warn_sec:
            ok.append(f"heartbeat {human_seconds(age)} назад")
        elif age < crit_sec:
            warn.append(f"heartbeat {human_seconds(age)} назад")
        else:
            crit.append(f"heartbeat старый ({human_seconds(age)} назад)")
    except Exception:
        crit.append("heartbeat отсутствует")

    # 2) Docker
    rc_ver, out_ver, err_ver = run_cmd("docker version --format '{{.Server.Version}}'")
    (ok if rc_ver == 0 and out_ver else crit).append(
        f"docker-proxy {'OK (daemon ' + out_ver + ')' if (rc_ver == 0 and out_ver) else 'ошибка (' + (err_ver or str(rc_ver)) + ')'}"
    )

    # 3) Контейнеры
    rc_ps, out_ps, _ = run_cmd("docker ps --format '{{.Names}}\\t{{.Status}}'")
    statuses = {}
    if rc_ps == 0 and out_ps:
        for line in out_ps.splitlines():
            try:
                n, s = line.split("\t", 1)
                statuses[n] = s
            except Exception:
                pass
    need = (
        os.getenv(
            "HEALTH_REQUIRE_CONTAINERS", "amnezia-awg,amnezia-xray,amnezia-dns,awgbot"
        )
        .strip()
        .split(",")
    )
    for name in [x.strip() for x in need if x.strip()]:
        st = statuses.get(name, "")
        if not st:
            crit.append(f"{name}: не запущен")
        else:
            low = st.lower()
            if low.startswith("up") and "unhealthy" not in low:
                ok.append(f"{name}: {humanize_uptime(st)}")
            elif "restarting" in low or "unhealthy" in low:
                warn.append(f"{name}: {st}")
            else:
                crit.append(f"{name}: {st}")

    # 4) Конфиги
    xray_c = os.getenv("XRAY_CONTAINER", "amnezia-xray")
    xray_cfg = os.getenv("XRAY_CONFIG_PATH", "/opt/amnezia/xray/server.json")
    rc_x, _, _ = _docker_exec(xray_c, f"test -r {shlex.quote(xray_cfg)}")
    (ok if rc_x == 0 else crit).append(
        "Xray конфиг OK" if rc_x == 0 else "Xray конфиг недоступен"
    )

    awg_c = os.getenv("AWG_CONTAINER", "amnezia-awg")
    awg_cfg = os.getenv("AWG_CONFIG_PATH", "/opt/amnezia/awg/wg0.conf")
    rc_a, _, _ = _docker_exec(awg_c, f"test -r {shlex.quote(awg_cfg)}")
    (ok if rc_a == 0 else crit).append(
        "AmneziaWG конфиг OK" if rc_a == 0 else "AmneziaWG конфиг недоступен"
    )

    # 5) /app/data
    try:
        tmp = os.path.join(DATA_DIR, ".health_wtest")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(tmp)
        rc_df, out_df, _ = run_cmd(
            'df -h /app/data | tail -n 1 | awk \'{print $4" свободно ("$5" занято)"}\''
        )
        ok.append(
            f"/app/data запись OK; {out_df}"
            if rc_df == 0 and out_df
            else "/app/data запись OK"
        )
    except Exception as e:
        crit.append(f"/app/data запись ошибка ({e})")

    # 6) TCP-щупалки портов (Xray)
    try:
        info = None
        # небольшой хак: если XRAY_CONNECT_HOST известен, возьмём порт из конфига XR.find_user недоступен тут — дернем 443 как дефолт
        host = XRAY_CONNECT_HOST
        ports = set()
        # один порт точно: 443 (дефолт), плюс попробуем из docker ps вытащить опубликованный
        ports.add(443)
        rc_pi, out_pi, _ = run_cmd(
            "docker ps --format '{{.Names}}\\t{{.Ports}}' | grep amnezia-xray || true"
        )
        if rc_pi == 0 and out_pi:
            # ищем "0.0.0.0:443->443/tcp"
            m = re.findall(r":(\d+)->\d+/(?:tcp|udp)", out_pi)
            for p in m:
                try:
                    ports.add(int(p))
                except:
                    pass
        good = any(tcp_check(host, p, timeout_ms=tcp_to) for p in ports)
        (ok if good else warn).append(
            f"Xray TCP порт {'OK' if good else 'недоступен'} ({host}:{'/'.join(map(str,ports))})"
        )
    except Exception:
        warn.append("Xray TCP проверка не выполнена")

    # TL;DR
    tldr = f"OK={len(ok)} WARN={len(warn)} CRIT={len(crit)}"
    emoji = "🟢" if not crit and not warn else ("🟡" if not crit else "🔴")
    lines = [f"{emoji} Health: {tldr}"]
    if crit:
        lines.append("Критичное:")
        lines += [f"• {x}" for x in crit]
    if warn:
        lines.append("Предупреждения:")
        lines += [f"• {x}" for x in warn]
    if not crit and not warn:
        lines.append("Все ключевые проверки в норме.")

    await update.effective_message.reply_text("\n".join(lines))


# =================== /boom (тестовая команда для проверки логов) ===================
@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_boom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Специальная команда для тестирования глобального обработчика ошибок."""
    logger.info({"event": "boom_triggered", "by": update.effective_user.id})
    # Намеренно кидаем исключение
    raise RuntimeError("💥 Искусственная ошибка для теста error-handler")


@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # дефолты из ENV
    try:
        loader_cooldown_sec = int(os.getenv("STATUS_LOADER_COOLDOWN_SEC", "5"))
    except Exception:
        loader_cooldown_sec = 5

    loader = "⏳ <b>Загружаю статус…</b>\n<i>Это может занять 1–2 секунды.</i>"

    # 1) показать лоудер (если пришли командой — отправляем новое сообщение)
    if getattr(update, "callback_query", None) and update.callback_query:
        # пришли из колбэка — редактируем текущее
        try:
            curr = (update.callback_query.message.text or "").rstrip()
            if "Загружаю" not in curr:
                preview = (curr + ("\n\n" if curr else "") + loader).strip()
                await update.callback_query.edit_message_text(
                    preview,
                    reply_markup=build_status_kb(),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
        except Exception:
            pass
        target_chat_id = update.effective_chat.id
        target_msg_id = update.callback_query.message.message_id
    else:
        # пришли /status — шлём новое сообщение с лоудером
        sent = await update.effective_message.reply_html(
            loader,
            reply_markup=build_status_kb(),
            disable_web_page_preview=True,
        )
        target_chat_id = sent.chat.id
        target_msg_id = sent.message_id

    # 2) собрать и отрендерить полный статус
    probe = status_probe()
    lines = render_status_full(probe)
    text = "\n".join(lines)

    # 3) перерисовать то же сообщение
    try:
        await context.bot.edit_message_text(
            chat_id=target_chat_id,
            message_id=target_msg_id,
            text=text,
            reply_markup=build_status_kb(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        # если вдруг не получилось — просто отправим новым
        await update.effective_chat.send_message(
            text,
            reply_markup=build_status_kb(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    flt = context.chat_data.get("sync_filter", SYNC_DEFAULT_FILTER)
    mode = context.chat_data.get("sync_mode", SYNC_DEFAULT_MODE)

    # 1) показываем лоудер именно в ТОМ ЖЕ сообщении (если пришли из callback),
    #    либо шлём новое и удаляем старое — это сделает _edit_cb_with_fallback
    await _edit_cb_with_fallback(
        update,
        context,
        "⏳ Готовлю отчёт /sync…",
        kb=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="admin_menu")]]
        ),
        parse_mode="HTML",
    )

    # 2) отрисовываем (перерисовываем) отчёт — внутри уже используется _edit_cb_with_fallback
    await _sync_report_send_or_edit(update, context, flt, mode)


@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_loglevel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /loglevel INFO | DEBUG | WARNING | ERROR
    Меняет уровень логгера и всех его хендлеров на лету.
    """
    arg = (context.args[0] if context.args else "").upper()
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    if arg not in levels:
        await update.effective_message.reply_text(
            "Укажите уровень: /loglevel DEBUG | INFO | WARNING | ERROR"
        )
        return

    lvl = levels[arg]
    # сам логгер
    logger.setLevel(lvl)
    # все хендлеры — в тот же уровень
    try:
        for h in logger.handlers:
            h.setLevel(lvl)
    except Exception:
        pass

    logger.info({"event": "loglevel_changed", "to": arg})
    await update.effective_message.reply_text(f"✅ Уровень логов: {arg}")


@autoclean_command_input
@with_request_id
@log_command
@admin_only
async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /logs            -> последние 50 строк про ошибки (handler_error/cmd_error)
    /logs 100        -> последние 100 строк про ошибки
    /logs all 100    -> последние 100 произвольных строк (всё подряд, без фильтра)
    """
    # разбор аргументов
    args = [a.lower() for a in (context.args or [])]
    show_all = False
    lines_count = 50
    if args:
        if args[0].isdigit():
            lines_count = max(1, min(1000, int(args[0])))
        elif args[0] == "all":
            show_all = True
            if len(args) > 1 and args[1].isdigit():
                lines_count = max(1, min(1000, int(args[1])))

    if not LOG_FILE_PATH.exists():
        await update.effective_message.reply_text("Лог-файл ещё не создан.")
        return

    raw = _tail_lines(LOG_FILE_PATH, lines_count)
    if not raw:
        await update.effective_message.reply_text("Лог пуст или не удалось прочитать.")
        return

    # фильтр по ошибкам (по умолчанию)
    events_err = {"handler_error", "cmd_error", "access_denied"}
    out_lines: list[str] = []
    for line in raw:
        try:
            js = json.loads(line)
        except Exception:
            if show_all:
                out_lines.append(line.strip())
            continue
        if show_all or (js.get("event") in events_err or js.get("level") in ("ERROR",)):
            out_lines.append(_format_log_line(js))

    if not out_lines:
        await update.effective_message.reply_text("Подходящих записей нет (всё чисто).")
        return

    # если влезает в сообщение — шлём текстом, иначе — файлом
    text = (
        "```\n" + "\n".join(out_lines[-400:]) + "\n```"
    )  # ограничим, чтобы точно влезало
    if len(text) <= 3500:
        await update.effective_message.reply_markdown(text)
    else:
        # сформировать временный файл-вывод
        buf = io.BytesIO("\n".join(out_lines).encode("utf-8"))
        buf.name = f"logs-tail-{lines_count}.txt"
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=InputFile(buf),
            caption=f"Последние {lines_count} строк лога"
            + (" (всё)" if show_all else " (ошибки)"),
        )


# ========= ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК =========
from telegram.error import TelegramError

# Ключ = (тип_ошибки, команда). Значение = {"ts": последний_уведомлённый_ts, "suppressed": счетчик_подавленных}
_ERR_CACHE: dict[tuple[str, str], dict] = {}


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    err_type = type(err).__name__ if err else "Exception"
    cmd = _cmd_name_from_update(update) if update else "(no-update)"
    uid = None
    try:
        if hasattr(update, "effective_user") and update.effective_user:
            uid = update.effective_user.id
    except Exception:
        pass

    rid = ensure_rid(context)

    # 1) Лог со стеком (всегда)
    logger.exception(
        {
            "event": "handler_error",
            "rid": rid,
            "uid": uid,
            "cmd": cmd,
            "error_type": err_type,
        }
    )

    # 2) Дружелюбный ответ пользователю
    try:
        if (
            hasattr(context, "bot")
            and hasattr(update, "effective_chat")
            and update.effective_chat
        ):
            if isinstance(err, TelegramError):
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Временная ошибка Telegram API, попробуйте ещё раз.",
                )
            else:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Упс, произошла ошибка. Подробности уже в логах.",
                )
    except Exception:
        pass

    # 3) Уведомление админам с антиспамом (конфигурируемый кулдаун)
    key = (err_type, cmd)
    now = time.time()
    rec = _ERR_CACHE.get(key)

    if rec:
        # Было уведомление в окне кулдауна — копим подавленные
        if now - rec["ts"] < ERROR_NOTIFY_COOLDOWN_SEC:
            rec["suppressed"] += 1
            return
        # Окно прошло — сообщаем и сбрасываем счётчик подавленных
        suppressed = rec.get("suppressed", 0)
        rec["ts"] = now
        rec["suppressed"] = 0
    else:
        # Первое событие — уведомляем немедленно
        _ERR_CACHE[key] = {"ts": now, "suppressed": 0}
        suppressed = 0

    brief = (
        f"⚠️ Ошибка: {err_type}\n"
        f"Команда: {cmd}\n"
        f"RID: {rid}\n"
        f"Пользователь: {uid or '-'}"
    )
    if suppressed:
        brief += f"\n(подавлено повторов: {suppressed})"

    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=brief)
        except Exception:
            pass


# ===== Watchdog: фоновая проверка окружения и зависимостей =====
_WATCH_LAST_SENT_TS = 0  # антиспам уведомлений админу


def _parse_docker_ps() -> dict:
    """Возвращает dict: name -> status строка"""
    rc, out, _ = run_cmd("docker ps --format '{{.Names}}\t{{.Status}}'")
    res = {}
    if rc == 0 and out:
        for line in out.splitlines():
            try:
                n, s = line.split("\t", 1)
                res[n] = s
            except Exception:
                pass
    return res


def _status_severity(status: str) -> str:
    """
    Возвращает 'ok' | 'warn' | 'crit' на основе docker Status строки.
    Примеры:
      'Up 3 hours'            -> ok
      'Up 3 hours (healthy)'  -> ok
      'Up 1 min (unhealthy)'  -> warn
      'Restarting (1) ...'    -> warn
      'Exited (0) ...'        -> crit
      'Created'               -> crit
      '' (не найден)          -> crit
    """
    s = (status or "").strip().lower()
    if not s:
        return "crit"
    if s.startswith("up"):
        if "unhealthy" in s or "health: starting" in s:
            return "warn"
        return "ok"
    if "restarting" in s:
        return "warn"
    if s.startswith("exited") or s.startswith("created") or "dead" in s:
        return "crit"
    # по умолчанию — подозрительно
    return "warn"


def _status_is_ok(status: str) -> bool:
    return _status_severity(status) == "ok"


def _status_is_warn(status: str) -> bool:
    return _status_severity(status) == "warn"


def _watchdog_once() -> dict:
    """
    Выполняет одну проверку. Возвращает словарь результата:
    {
      "ok": [строки],
      "warn": [строки],
      "crit": [строки],
      "tldr": "краткая сводка"
    }
    """
    ok, warn, crit = [], [], []

    # 1) Docker daemon через прокси
    rc_ver, out_ver, err_ver = run_cmd("docker version --format '{{.Server.Version}}'")
    if rc_ver == 0 and out_ver:
        ok.append(f"docker-proxy: OK (daemon {out_ver})")
    else:
        crit.append(f"docker-proxy: ошибка ({err_ver or rc_ver})")

    # 2) Контейнеры
    statuses = _parse_docker_ps()
    important = [
        os.getenv("AWG_CONTAINER", "amnezia-awg"),
        os.getenv("XRAY_CONTAINER", "amnezia-xray"),
        os.getenv("DNS_CONTAINER", "amnezia-dns"),
        "awgbot",
    ]
    for name in important:
        st = statuses.get(name, "")
        if not st:
            crit.append(f"{name}: не запущен")
        elif _status_is_ok(st):
            ok.append(f"{name}: {st}")
        elif _status_is_warn(st):
            warn.append(f"{name}: {st}")
        else:
            crit.append(f"{name}: {st}")

    # 3) Конфиги XRay / AWG доступны внутри контейнеров
    xray_c = os.getenv("XRAY_CONTAINER", "amnezia-xray")
    xray_cfg = os.getenv("XRAY_CONFIG_PATH", "/opt/amnezia/xray/server.json")
    rc_x, _, _ = _docker_exec(xray_c, f"test -r {shlex.quote(xray_cfg)}")
    (ok if rc_x == 0 else crit).append(
        f"XRay конфиг {'OK' if rc_x == 0 else 'нет доступа'} ({xray_c}:{xray_cfg})"
    )

    awg_c = os.getenv("AWG_CONTAINER", "amnezia-awg")
    awg_cfg = os.getenv("AWG_CONFIG_PATH", "/opt/amnezia/awg/wg0.conf")
    rc_a, _, _ = _docker_exec(awg_c, f"test -r {shlex.quote(awg_cfg)}")
    (ok if rc_a == 0 else crit).append(
        f"AmneziaWG конфиг {'OK' if rc_a == 0 else 'нет доступа'} ({awg_c}:{awg_cfg})"
    )

    # 4) /app/data и heartbeat
    # запись в /app/data
    try:
        tmp = os.path.join(DATA_DIR, ".watch_wtest")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(tmp)
        ok.append("/app/data запись: OK")
    except Exception as e:
        crit.append(f"/app/data запись: ошибка ({e})")

    # heartbeat возраст
    try:
        age = time.time() - os.path.getmtime(HEARTBEAT_PATH)
        if age < HEARTBEAT_WARN_SEC:
            ok.append(f"heartbeat: {human_seconds(age)} назад")
        elif age < HEARTBEAT_CRIT_SEC:
            warn.append(f"heartbeat: {human_seconds(age)} назад")
        else:
            crit.append(f"heartbeat: старый ({human_seconds(age)} назад)")
    except Exception:
        crit.append("heartbeat: нет файла/доступа")

    # TL;DR
    tldr = f"OK={len(ok)}  WARN={len(warn)}  CRIT={len(crit)}"
    return {"ok": ok, "warn": warn, "crit": crit, "tldr": tldr}


def _try_autorestart(statuses: dict, names: list[str]) -> list[str]:
    """Пробует рестартануть контейнеры из names, если они не ОК. Возвращает список перезапущенных."""
    restarted = []
    for name in names:
        st = statuses.get(name, "")
        if not _status_is_ok(st):
            rc, _, err = run_cmd(f"docker restart {shlex.quote(name)}")
            if rc == 0:
                restarted.append(name)
            else:
                logger.warning(
                    {
                        "event": "watchdog_restart_fail",
                        "container": name,
                        "error": err or rc,
                    }
                )
    return restarted


import urllib.request, urllib.parse, ssl


def _safe_send_telegram(text: str) -> None:
    """
    Отправляет текст всем ADMIN_IDS через Telegram Bot API напрямую из фонового потока.
    Не требует JobQueue/PTB цикла. Токен берём из уже загруженного TOKEN.
    """
    if not WATCHDOG_TG_NOTIFY:
        logger.info({"event": "watchdog_notify_skipped", "reason": "disabled"})
        return
    if not ADMIN_IDS:
        logger.info({"event": "watchdog_notify_skipped", "reason": "no_admins"})
        return

    base = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    ctx = ssl.create_default_context()
    for aid in ADMIN_IDS:
        try:
            data = urllib.parse.urlencode(
                {
                    "chat_id": str(aid),
                    "text": text,
                    "disable_web_page_preview": "true",
                }
            ).encode("utf-8")
            req = urllib.request.Request(base, data=data, method="POST")
            with urllib.request.urlopen(
                req, timeout=WATCHDOG_TG_TIMEOUT, context=ctx
            ) as resp:
                if resp.status != 200:
                    logger.warning({"event": "watchdog_tg_non200", "code": resp.status})
        except Exception as e:
            logger.warning({"event": "watchdog_tg_send_fail", "error": str(e)})


def _watchdog_notify_admins(msg: str):
    # и в лог запишем, и в Telegram отправим
    logger.warning({"event": "watchdog_alert", "text": msg})
    _safe_send_telegram(msg)


def _watchdog_worker():
    global _WATCH_LAST_SENT_TS
    logger.info(
        {
            "event": "watchdog_start",
            "interval_sec": WATCHDOG_INTERVAL_SEC,
            "autorestart": WATCHDOG_AUTORESTART,
        }
    )
    while True:
        try:
            res = _watchdog_once()
            # NEW: игнорим WARN в первые N секунд после старта бота
            within_grace = (time.time() - _BOOT_TS) < WATCHDOG_BOOT_GRACE_SEC
            if within_grace:
                issues = res["crit"][:]  # только критичное
            else:
                issues = res["warn"] + res["crit"]

            if issues:
                now = time.time()
                if now - _WATCH_LAST_SENT_TS >= WATCHDOG_COOLDOWN_SEC:
                    _WATCH_LAST_SENT_TS = now
                    lines = ["⚠️ Watchdog обнаружил проблемы", res["tldr"], ""]
                    if res["crit"]:
                        lines.append("Критичное:")
                        lines.extend(f"• {x}" for x in res["crit"])
                    if not within_grace and res["warn"]:
                        lines.append("Предупреждения:")
                        lines.extend(f"• {x}" for x in res["warn"])

                    text = "\n".join(lines)

                    if WATCHDOG_AUTORESTART:
                        statuses = _parse_docker_ps()
                        names = [
                            os.getenv("AWG_CONTAINER", "amnezia-awg"),
                            os.getenv("XRAY_CONTAINER", "amnezia-xray"),
                            os.getenv("DNS_CONTAINER", "amnezia-dns"),
                        ]
                        restarted = _try_autorestart(statuses, names)
                        if restarted:
                            text += "\n\n♻️ Перезапущены: " + ", ".join(restarted)

                    _watchdog_notify_admins(text)
                else:
                    logger.info(
                        {
                            "event": "watchdog_skip_notify",
                            "cooldown_sec": WATCHDOG_COOLDOWN_SEC,
                        }
                    )
            else:
                logger.info({"event": "watchdog_ok"})
        except Exception:
            logger.exception({"event": "watchdog_fail"})
        time.sleep(WATCHDOG_INTERVAL_SEC)


# ========= РОУТИНГ =========
def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_TOKEN не задан")

    app = Application.builder().token(TOKEN).build()

    # Глобальный обработчик ошибок
    app.add_error_handler(global_error_handler)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("allow", cmd_allow))
    app.add_handler(CommandHandler("revoke", cmd_revoke))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("my", cmd_my))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("logs", cmd_logs))
    app.add_handler(CommandHandler("boom", cmd_boom))
    app.add_handler(CommandHandler("loglevel", cmd_loglevel))
    app.add_handler(CommandHandler("sync", cmd_sync))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(~filters.COMMAND, on_text))

    # Heartbeat: фоновый поток обновляет файл каждые 15 секунд
    os.makedirs(DATA_DIR, exist_ok=True)

    def _heartbeat_worker():
        while True:
            try:
                with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                    f.write(now_iso())
            except Exception as e:
                logger.warning({"event": "heartbeat_write_fail", "error": str(e)})
            time.sleep(15)

    threading.Thread(target=_heartbeat_worker, daemon=True).start()
    # Heartbeat-поток уже запущен выше
    if WATCHDOG_ENABLED:
        threading.Thread(target=_watchdog_worker, daemon=True).start()

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
