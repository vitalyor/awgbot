# src/features/sync/render.py
from __future__ import annotations
from typing import List
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

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


def build_sync_kb(active_filter: str, mode: str) -> InlineKeyboardMarkup:
    def _radio(code: str) -> str:
        return ("• " if code == active_filter else "○ ") + SYNC_FILTERS[code]

    rows = [
        [
            InlineKeyboardButton(_radio("all"), callback_data="sync_filter:all"),
            InlineKeyboardButton(_radio("absent"), callback_data="sync_filter:absent"),
        ],
        [
            InlineKeyboardButton(_radio("extra"), callback_data="sync_filter:extra"),
            InlineKeyboardButton(
                _radio("suspended"), callback_data="sync_filter:suspended"
            ),
        ],
        [
            InlineKeyboardButton(
                _radio("diverged"), callback_data="sync_filter:diverged"
            ),
        ],
        [
            InlineKeyboardButton(
                SYNC_MODE_LABEL["compact" if mode == "detailed" else "detailed"],
                callback_data="sync_mode:"
                + ("compact" if mode == "detailed" else "detailed"),
            ),
        ],
        [
            InlineKeyboardButton(
                "🧩 Починить отсутствующие", callback_data="sync_apply_absent_all"
            ),
            InlineKeyboardButton(
                "🧹 Убрать лишние", callback_data="sync_apply_extra_all"
            ),
        ],
        [
            InlineKeyboardButton(
                "🧭 Обновить БД по Xray", callback_data="sync_apply_diverged_db_all"
            ),
            InlineKeyboardButton(
                "🔁 Пересобрать в Xray по БД",
                callback_data="sync_apply_diverged_xray_all",
            ),
        ],
        [
            InlineKeyboardButton("🔄 Обновить", callback_data="sync_refresh"),
        ],
        [
            InlineKeyboardButton("🏠 В главное меню", callback_data="menu"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def sync_render(data: dict, flt: str, mode: str) -> List[str]:
    """
    Рендер отчёта /sync.
    - В счётчиках показываем foreign.
    - В detailed режиме добавляем раздел с перечнем чужих записей (read-only).
    - Действия (кнопки) нигде не предлагаются для foreign.
    """
    c = data.get("counters", {})
    only_in_state = data.get("only_in_state", [])
    only_in_xray = data.get("only_in_xray", [])
    diverged = data.get("diverged", [])
    suspended = data.get("suspended", [])
    active = data.get("active", [])
    foreign = data.get("foreign", [])

    hdr = (
        "<b>Синхронизация Xray ↔ БД</b>\n"
        f"• Всего пользователей в БД: <b>{c.get('users', 0)}</b>\n"
        f"• Профилей Xray в БД: <b>{c.get('profiles_state', 0)}</b>\n"
        f"• Клиентов Xray (свои): <b>{c.get('clients_xray', 0)}</b>\n"
        f"• Чужих клиентов Xray: <b>{c.get('foreign', 0)}</b>\n"
    )

    lines = [hdr]

    def fmt_pairs(items):
        return (
            "\n".join(
                f"• <code>{i.get('tid',0)}</code> · <b>{i.get('name','')}</b>"
                for i in items
            )
            or "—"
        )

    if flt == "all":
        body = []
        body.append(
            f"<b>Отсутствуют в Xray (есть в БД):</b>\n{fmt_pairs(only_in_state)}"
        )
        body.append(
            f"<b>Есть в Xray (свои), отсутствуют в БД:</b>\n{fmt_pairs(only_in_xray)}"
        )
        if diverged:
            body.append(f"<b>Расхождения:</b>\n{fmt_pairs(diverged)}")
        if suspended:
            body.append(f"<b>Приостановлены:</b>\n{fmt_pairs(suspended)}")
        if active:
            body.append(f"<b>Активны:</b>\n{fmt_pairs(active)}")

        if mode == "detailed" and foreign:
            fx = "\n".join(
                f"• uuid=<code>{f.get('uuid','')}</code> · sni=<code>{f.get('sni','')}</code> · port=<code>{f.get('port','')}</code>"
                for f in foreign
            )
            body.append(
                "<b>Чужие клиенты Xray (не управляются ботом, действий не будет):</b>\n"
                + fx
            )

        lines.append("\n\n".join(body))

    elif flt == "absent":
        lines.append(
            "<b>Отсутствуют в Xray (есть в БД):</b>\n" + fmt_pairs(only_in_state)
        )
    elif flt == "extra":
        lines.append(
            "<b>Есть в Xray (свои), отсутствуют в БД:</b>\n" + fmt_pairs(only_in_xray)
        )
    elif flt == "diverged":
        lines.append(
            "<b>Расхождения:</b>\n" + (fmt_pairs(diverged) if diverged else "—")
        )
    elif flt == "suspended":
        lines.append(
            "<b>Приостановлены:</b>\n" + (fmt_pairs(suspended) if suspended else "—")
        )
    elif flt == "active":
        lines.append("<b>Активны:</b>\n" + (fmt_pairs(active) if active else "—"))
    else:
        lines.append("Неизвестный фильтр.")

    tail = (
        "\n\n<i>Примечание:</i> чужие клиенты Xray (созданные не ботом) "
        "учитываются только информативно и не затрагиваются автоматическими действиями."
    )
    text = "\n".join(lines) + tail
    return [text]


# === helpers exported for bot.py (diagnostics UI pieces) ===

def sync_header(c: dict) -> str:
    return (
        "🧭 <b>Синхронизация (диагностика)</b>\n"
        f"Пользователей: <b>{c.get('users',0)}</b>\n"
        f"Профилей (state.json): <b>{c.get('profiles_state',0)}</b>\n"
        f"Клиентов Xray: <b>{c.get('clients_xray',0)}</b>\n"
        f"Только в Xray: <b>{c.get('only_in_xray',0)}</b>\n"
        f"Только в state.json: <b>{c.get('only_in_state',0)}</b>\n"
        f"Расхождения: <b>{c.get('diverged',0)}</b>\n"
        f"Приостановленные: <b>{c.get('suspended',0)}</b>\n"
        f"Активные: <b>{c.get('active',0)}</b>"
    )

def sync_filter_items(data: dict, flt: str) -> list[dict]:
    if flt == "all":
        # порядок: absent, extra, suspended, diverged, active
        tagged = (
            [dict(x, _tag="absent") for x in data.get("only_in_state", [])]
            + [dict(x, _tag="extra") for x in data.get("only_in_xray", [])]
            + [dict(x, _tag="suspended") for x in data.get("suspended", [])]
            + [dict(x, _tag="diverged") for x in data.get("diverged", [])]
            + [dict(x, _tag="active") for x in data.get("active", [])]
        )
        return tagged
    if flt == "absent":
        return [dict(x, _tag="absent") for x in data.get("only_in_state", [])]
    if flt == "extra":
        return [dict(x, _tag="extra") for x in data.get("only_in_xray", [])]
    if flt == "suspended":
        return [dict(x, _tag="suspended") for x in data.get("suspended", [])]
    if flt == "diverged":
        return [dict(x, _tag="diverged") for x in data.get("diverged", [])]
    if flt == "active":
        return [dict(x, _tag="active") for x in data.get("active", [])]
    return []

def sync_status_label(tag: str, diffs: list[str] | None = None) -> str:
    if tag == "active":
        return "Активен ▶️"
    if tag == "suspended":
        return "Приостановлен ⏸"
    if tag == "absent":
        return "Отсутствует в Xray ⚠️"
    if tag == "extra":
        return "Лишний в Xray 🧩"
    if tag == "diverged":
        return "Расхождение ❗" + (f" ({', '.join(diffs)})" if diffs else "")
    return tag

def split_text_for_telegram(s: str, limit: int = 3500, safe_txt: str = "\u2060") -> list[str]:
    """
    Режет длинный текст на части < limit символов.
    Стараться резать по \n. Гарантирует, что список не пуст.
    """
    s = s or safe_txt
    if len(s) <= limit:
        return [s]
    parts, buf = [], []
    total = 0
    for line in s.splitlines(keepends=True):
        ln = len(line)
        if (total + ln) > limit and buf:
            parts.append("".join(buf))
            buf, total = [line], ln
        else:
            buf.append(line)
            total += ln
    if buf:
        parts.append("".join(buf))
    return parts or [safe_txt]