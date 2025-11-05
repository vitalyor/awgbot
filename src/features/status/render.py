# src/features/status/render.py
from __future__ import annotations
import os, time
from datetime import datetime
from typing import List

from core.docker import run_cmd
from core.status_probe import humanize_uptime, docker_stats, human_seconds

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def build_status_kb(_want_full: bool | None = None) -> InlineKeyboardMarkup:
    # сейчас всегда длинный статус; только refresh + в меню
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить", callback_data="status_refresh")],
            [InlineKeyboardButton("🏠 В главное меню", callback_data="status_to_menu")],
        ]
    )

def render_status_full(probe: dict) -> List[str]:
    """
    Всегда рендерит ПОЛНЫЙ статус на основе probe из core.status_probe.status_probe().
    """
    now_local = datetime.now().astimezone().strftime("%H:%M:%S %d.%m.%Y")

    # docker ps для аптаймов контейнеров
    rc_ps, out_ps, _ = run_cmd("docker ps --format '{{.Names}}\\t{{.Status}}'")
    statuses: dict[str, str] = {}
    if rc_ps == 0 and out_ps:
        for line in out_ps.splitlines():
            try:
                name, status = line.split("\t", 1)
                statuses[name] = status
            except Exception:
                pass

    important = [
        os.getenv("AWG_CONTAINER", "amnezia-awg"),
        os.getenv("XRAY_CONTAINER", "amnezia-xray"),
        os.getenv("DNS_CONTAINER", "amnezia-dns"),
        "awgbot",
    ]

    summary = probe.get("summary", "—")
    proxy_line = probe.get("proxy_line", "docker-proxy: —")
    xray_line = probe.get("xray_line", "XRay конфиг: —")
    awg_line = probe.get("awg_line", "AmneziaWG конфиг: —")
    storage_line = probe.get("storage_line", "/app/data: —")
    hb_line = probe.get("hb_line", "heartbeat: —")

    # Контейнеры — обычным списком
    cont_block: list[str] = []
    for name in important:
        st = statuses.get(name, "не запущен")
        low = st.lower()
        if ("unhealthy" in low) or ("restarting" in low):
            badge = "🟡"
            nice = humanize_uptime(st) if "up" in low else (st or "не запущен")
        elif ("up" in low) or ("healthy" in low):
            badge = "🟢"
            nice = humanize_uptime(st) if "up" in low else (st or "не запущен")
        else:
            badge = "🔴"
            nice = st or "не запущен"
        cont_block.append(f"{badge} {name} — {nice}")

    # Аптайм бота рассчитываем локально из probe при его наличии
    bot_uptime = probe.get("uptime_bot")
    if not bot_uptime:
        # запасной расчёт, если его не передали
        bot_uptime = human_seconds(0)

    lines: list[str] = [
        f"🧩 <b>Статус</b> <code>{now_local}</code>",
        f"⏱️ Аптайм бота: <code>{bot_uptime}</code>",
        summary,
        "",
        "Контейнеры",
        *cont_block,
        "",
        "Инфраструктура",
        f"• {proxy_line}",
        f"• {xray_line}",
        f"• {awg_line}",
        f"• {storage_line}",
        f"• {hb_line}",
        "",
        "📊 Ресурсы",
    ]

    # docker stats
    stats = docker_stats()
    if stats:
        for name in important:
            s = stats.get(name)
            if s:
                lines.append(
                    f"• {name}: CPU {s['cpu']}, Память {s['mem']} ({s['memp']})"
                )

    rc_df, out_df, _ = run_cmd(
        'df -h /app/data | tail -n 1 | awk \'{print $2" всего, " $4" свободно ("$5" занято)"}\''
    )
    if rc_df == 0 and out_df:
        lines.append(f"💽 /app/data: {out_df}")

    return lines