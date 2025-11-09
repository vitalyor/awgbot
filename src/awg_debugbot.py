# src/awg_debugbot.py
from __future__ import annotations
import os, json, logging
from typing import Optional
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Импортируем из того же места, где лежит основной код
try:
    from .awg_fileflow import (
        facts,
        list_peers,
        create_peer_with_generated_keys,
        remove_peer,
        clean_all_peers,
    )
except Exception:
    from awg_fileflow import (
        facts,
        list_peers,
        create_peer_with_generated_keys,
        remove_peer,
        clean_all_peers,
    )

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("awg_debugbot")

ALLOWED_USER_IDS = (
    set(map(int, os.getenv("AWG_DEBUGBOT_ADMINS", "").split(",")))
    if os.getenv("AWG_DEBUGBOT_ADMINS")
    else None
)


def _allowed(uid: int) -> bool:
    return (ALLOWED_USER_IDS is None) or (uid in ALLOWED_USER_IDS)


async def cmd_facts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_user.id):
        return
    await update.message.reply_text(
        "```json\n" + json.dumps(facts(), indent=2) + "\n```", parse_mode="Markdown"
    )


async def cmd_peers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_user.id):
        return
    await update.message.reply_text(
        "```json\n" + json.dumps(list_peers(), indent=2) + "\n```",
        parse_mode="Markdown",
    )


async def cmd_gen(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /gen 10.8.1.X/32")
        return
    ip = ctx.args[0]
    try:
        res = create_peer_with_generated_keys(ip)
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")
        return
    text = (
        f"*Client public:* `{res['client_public']}`\n"
        f"*Endpoint:* `{res['endpoint']}`\n\n"
        f"```conf\n{res['client_conf']}\n```"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_rm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /rm <client_pubkey>")
        return
    try:
        remove_peer(ctx.args[0])
        await update.message.reply_text("✅ removed")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_clean(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_user.id):
        return
    try:
        clean_all_peers()
        await update.message.reply_text("✅ cleaned (kept only [Interface])")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


def main():
    token = os.environ.get("AWG_DEBUG_BOT_TOKEN")
    if not token:
        raise SystemExit("Set AWG_DEBUG_BOT_TOKEN")
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("facts", cmd_facts))
    app.add_handler(CommandHandler("peers", cmd_peers))
    app.add_handler(CommandHandler("gen", cmd_gen))
    app.add_handler(CommandHandler("rm", cmd_rm))
    app.add_handler(CommandHandler("clean", cmd_clean))
    log.info("awg_debugbot is running")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
