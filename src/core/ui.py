# src/core/ui.py
from __future__ import annotations
import time
from typing import Optional
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import ContextTypes

SAFE_TXT = "\u2060"  # невидимый символ


def _salt_text(txt: str) -> str:
    n = int((time.time() * 100) % 7) + 1  # 1..7
    return (txt or "") + (SAFE_TXT * n)


def _is_command_message(update) -> bool:
    try:
        return bool(
            getattr(update, "message", None)
            and isinstance(update.message.text, str)
            and update.message.text.strip().startswith("/")
        )
    except Exception:
        return False


async def _delete_user_message_if_command(update, context) -> None:
    if not _is_command_message(update):
        return
    try:
        chat_id = update.effective_chat.id if update.effective_chat else None
        msg_id = update.message.message_id if update.message else None
        if chat_id and msg_id:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass


def autoclean_command_input(fn):
    from functools import wraps

    @wraps(fn)
    async def wrapper(update, context, *args, **kwargs):
        await _delete_user_message_if_command(update, context)
        return await fn(update, context, *args, **kwargs)

    return wrapper


def _cb_message_is_last(update, context) -> bool:
    try:
        q = getattr(update, "callback_query", None)
        if not q or not q.message:
            return True
        cb_mid = q.message.message_id
        last_bot_mid = int(context.user_data.get("last_bot_msg_id") or 0)
        last_user_mid = int(context.chat_data.get("last_user_msg_id") or 0)
        last_known = max(last_bot_mid, last_user_mid)
        return last_known <= cb_mid
    except Exception:
        return True


def ensure_main_menu_button(
    kb: Optional[InlineKeyboardMarkup],
    add_menu_button: bool = True,
) -> Optional[InlineKeyboardMarkup]:
    if not add_menu_button:
        return kb
    if kb is None:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 В главное меню", callback_data="menu")]]
        )
    rows = list(kb.inline_keyboard or [])
    try:
        exists = any(
            (getattr(btn, "callback_data", None) or "") == "menu"
            for row in rows
            for btn in row
        )
    except Exception:
        exists = False
    if not exists:
        rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


async def clean_and_send(
    update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    kb: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = None,
    add_menu_button: bool = True,
) -> Message:
    ud = context.user_data
    last_msg_id = ud.pop("last_bot_msg_id", None)
    if last_msg_id:
        try:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id, message_id=last_msg_id
            )
        except Exception:
            pass

    safe_text = text if (isinstance(text, str) and text.strip()) else SAFE_TXT
    kb = ensure_main_menu_button(kb, add_menu_button=add_menu_button)
    sent = await update.effective_chat.send_message(
        safe_text,
        reply_markup=kb,
        parse_mode=parse_mode,
        disable_web_page_preview=True,
    )
    ud["last_bot_msg_id"] = sent.message_id
    return sent


async def edit_or_send(
    update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    kb: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = None,
    add_menu_button: bool = True,
    edit_last: bool = False,
) -> Message:
    if not text:
        if getattr(update, "callback_query", None) and update.callback_query:
            try:
                return await update.callback_query.edit_message_reply_markup(
                    reply_markup=ensure_main_menu_button(
                        kb, add_menu_button=add_menu_button
                    )
                )
            except Exception:
                pass
        text = SAFE_TXT

    # callback-путь
    if getattr(update, "callback_query", None) and update.callback_query:
        q = update.callback_query
        try:
            if not _cb_message_is_last(update, context):
                sent = await update.effective_chat.send_message(
                    text or SAFE_TXT,
                    reply_markup=ensure_main_menu_button(
                        kb, add_menu_button=add_menu_button
                    ),
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
                context.user_data["last_bot_msg_id"] = sent.message_id
                context.chat_data["last_status_msg_id"] = sent.message_id
                try:
                    await context.bot.delete_message(
                        chat_id=q.message.chat.id,
                        message_id=q.message.message_id,
                    )
                except Exception:
                    pass
                return sent

            try:
                return await q.edit_message_text(
                    text or SAFE_TXT,
                    reply_markup=ensure_main_menu_button(
                        kb, add_menu_button=add_menu_button
                    ),
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
            except Exception as e1:
                emsg = (str(e1) or "").lower()
                if "message is not modified" in emsg:
                    return await q.edit_message_text(
                        _salt_text(text or SAFE_TXT),
                        reply_markup=ensure_main_menu_button(
                            kb, add_menu_button=add_menu_button
                        ),
                        parse_mode=parse_mode,
                        disable_web_page_preview=True,
                    )
                # fallback — шлём новое
                sent = await update.effective_chat.send_message(
                    _salt_text(text or SAFE_TXT),
                    reply_markup=ensure_main_menu_button(
                        kb, add_menu_button=add_menu_button
                    ),
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
                context.user_data["last_bot_msg_id"] = sent.message_id
                context.chat_data["last_status_msg_id"] = sent.message_id
                return sent
        except Exception:  # ← ЭТОЙ СТРОКИ НЕ ХВАТАЛО
            pass

    # обычный путь (не callback)
    if edit_last:
        try:
            last_id = int(context.chat_data.get("last_status_msg_id") or 0)
            if last_id:
                return await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=last_id,
                    text=text or SAFE_TXT,
                    reply_markup=ensure_main_menu_button(
                        kb, add_menu_button=add_menu_button
                    ),
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
        except Exception:
            pass

    sent = await update.effective_chat.send_message(
        text or SAFE_TXT,
        reply_markup=ensure_main_menu_button(kb, add_menu_button=add_menu_button),
        parse_mode=parse_mode,
        disable_web_page_preview=True,
    )
    context.user_data["last_bot_msg_id"] = sent.message_id
    context.chat_data["last_status_msg_id"] = sent.message_id
    return sent


async def _edit_cb_with_fallback(
    update, context, text: str, *, kb=None, parse_mode="HTML"
):
    q = update.callback_query
    chat_id = update.effective_chat.id if update.effective_chat else None
    last_id = context.chat_data.get("last_status_msg_id")

    kb = ensure_main_menu_button(kb, add_menu_button=True)

    try:
        if q and not _cb_message_is_last(update, context):
            m = await update.effective_chat.send_message(
                (text or SAFE_TXT),
                reply_markup=kb,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            context.user_data["last_bot_msg_id"] = m.message_id
            context.chat_data["last_status_msg_id"] = m.message_id
            try:
                await context.bot.delete_message(
                    chat_id=q.message.chat.id, message_id=q.message.message_id
                )
            except Exception:
                pass
            return m
    except Exception:
        pass

    try:
        return await q.edit_message_text(
            text or SAFE_TXT,
            reply_markup=kb,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except Exception as e1:
        emsg = (str(e1) or "").lower()
        if "message is not modified" in emsg:
            try:
                return await q.edit_message_text(
                    _salt_text(text or SAFE_TXT),
                    reply_markup=kb,
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    if chat_id and last_id:
        try:
            return await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=last_id,
                text=(text or SAFE_TXT),
                reply_markup=kb,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    try:
        m = await update.effective_message.reply_html(
            _salt_text(text or SAFE_TXT),
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        context.chat_data["last_status_msg_id"] = m.message_id
        context.user_data["last_bot_msg_id"] = m.message_id
        return m
    except Exception:
        try:
            if q:
                await q.answer(
                    "Не удалось обновить сообщение. Проверь лог /logs.", show_alert=True
                )
        except Exception:
            pass
        return None
