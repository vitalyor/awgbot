# src/core/decorators.py
from __future__ import annotations
import os, re, time, uuid
from functools import wraps
from typing import Optional

from logger_setup import get_logger

logger = get_logger()

SECRETS_FILE = "/run/secrets/secret.env"
CB_DEBOUNCE_MS = int(os.getenv("CB_DEBOUNCE_MS", "2000"))
CMD_DEBOUNCE_MS = int(os.getenv("CMD_DEBOUNCE_MS", "1200"))


def _fallback_get_from_file(path: str, key: str) -> Optional[str]:
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


def _load_admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_IDS") or _fallback_get_from_file(SECRETS_FILE, "ADMIN_IDS") or ""
    ids = {int(tok) for tok in re.split(r"[,\s]+", raw.strip()) if tok.isdigit()}
    return ids


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


def log_command(fn):
    @wraps(fn)
    async def wrapper(update, context, *args, **kwargs):
        # антидубль для callback
        try:
            q = getattr(update, "callback_query", None)
            allow_nested = bool(getattr(context, "chat_data", {})) and bool(
                context.chat_data.get("_allow_nested_from_cb", False)
            )
            if q is not None and not allow_nested:
                key = (
                    update.effective_chat.id if update.effective_chat else 0,
                    q.message.message_id if getattr(q, "message", None) else 0,
                    (q.data or ""),
                )
                now_ts = time.time()
                last = context.chat_data.get("_last_cb2")
                debounce = CB_DEBOUNCE_MS / 1000.0
                if last and last.get("key") == key and (now_ts - last.get("ts", 0)) < debounce:
                    return
                context.chat_data["_last_cb2"] = {"key": key, "ts": now_ts}
            if allow_nested:
                try:
                    context.chat_data.pop("_allow_nested_from_cb", None)
                except Exception:
                    pass
        except Exception:
            pass

        # антидубль для /команд
        try:
            msg = getattr(update, "message", None)
            if msg and msg.text and msg.text.startswith("/"):
                cmd_token = (msg.text.split()[0] or "").strip()
                chat_id = update.effective_chat.id if update.effective_chat else 0
                key = (chat_id, cmd_token)
                now_ts = time.time()
                last = context.chat_data.get("_last_cmd")
                debounce = CMD_DEBOUNCE_MS / 1000.0
                if last and last.get("key") == key and (now_ts - last.get("ts", 0)) < debounce:
                    return
                context.chat_data["_last_cmd"] = {"key": key, "ts": now_ts}
        except Exception:
            pass

        suppress = bool(getattr(context, "chat_data", {})) and bool(
            context.chat_data.pop("_suppress_log_once", False)
        )
        rid = ensure_rid(context)
        cmd = _cmd_name_from_update(update)
        uid = None
        try:
            if update.effective_user:
                uid = update.effective_user.id
        except Exception:
            pass

        if not suppress:
            logger.info({"event": "cmd_start", "rid": rid, "uid": uid, "cmd": cmd})
        t0 = time.time()
        try:
            res = await fn(update, context, *args, **kwargs)
            dt = int((time.time() - t0) * 1000)
            if not suppress:
                logger.info({"event": "cmd_ok", "rid": rid, "uid": uid, "cmd": cmd, "ms": dt})
            return res
        except Exception:
            dt = int((time.time() - t0) * 1000)
            if not suppress:
                logger.error({"event": "cmd_error", "rid": rid, "uid": uid, "cmd": cmd, "ms": dt}, exc_info=True)
            raise

    return wrapper


def admin_only(fn):
    @wraps(fn)
    async def wrapper(update, context, *args, **kwargs):
        rid = ensure_rid(context)
        try:
            uid = update.effective_user.id if update and update.effective_user else None
        except Exception:
            uid = None
        cmd = _cmd_name_from_update(update)
        ADMIN_IDS = _load_admin_ids()
        if ADMIN_IDS and uid not in ADMIN_IDS:
            try:
                logger.warning({"event": "access_denied", "rid": rid, "uid": uid, "cmd": cmd})
            except Exception:
                pass
            try:
                return await update.effective_message.reply_text("⛔ Доступ запрещён.")
            except Exception:
                return
        return await fn(update, context, *args, **kwargs)

    return wrapper


def with_request_id(fn):
    @wraps(fn)
    async def wrapper(update, context, *args, **kwargs):
        rid = str(uuid.uuid4())[:8]
        try:
            context.chat_data["_rid"] = rid
        except Exception:
            pass
        context.args = getattr(context, "args", [])
        return await fn(update, context, *args, **kwargs)

    return wrapper