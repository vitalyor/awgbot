from __future__ import annotations
import os, re, time, shlex
from datetime import datetime
from typing import Any

from core.docker import run_cmd, _docker_exec, dir_size_bytes


def human_seconds(s: float) -> str:
    s = int(s)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{'' if s==0 else f' {s}s'}"
    h, m = divmod(m, 60)
    return f"{h}h{'' if m==0 else f' {m}m'}"


def docker_stats() -> dict:
    """
    Возвращает словарь: {name: {"cpu": "1.23%", "mem": "123.4MiB / 512MiB", "memp": "24.1%"}}
    Использует: docker stats --no-stream
    """
    fmt = "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
    rc, out, err = run_cmd(f"docker stats --no-stream --format '{fmt}'", timeout=8)
    stats = {}
    if rc != 0 or not out:
        return stats
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        name, cpu, mem, memp = parts
        stats[name] = {"cpu": cpu.strip(), "mem": mem.strip(), "memp": memp.strip()}
    return stats


def humanize_uptime(status_text: str) -> str:
    """
    Превращает хвост после 'Up ...' в короткий RU-вид.
    """
    st = (status_text or "").strip()
    m = re.search(r"\bUp\s+(.+)", st, flags=re.I)
    if not m:
        return st
    tail = m.group(1)
    tail = re.sub(r"\babout\b", "", tail, flags=re.I)
    tail = re.sub(r"\(healthy\)|\(unhealthy\)|\(.*?health.*?\)", "", tail, flags=re.I)
    tail = tail.replace("healthy", "").replace("unhealthy", "")
    tail = re.sub(r"less\s+than\s+a\s+second", "less than 1 second", tail, flags=re.I)
    tail = re.sub(r"less\s+than\s+1\s*second", "<1 second", tail, flags=re.I)
    tail = re.sub(r"\b(an|a)\b", "1", tail, flags=re.I)
    repl = [
        (r"\bweeks?\b", "нед"),
        (r"\bdays?\b", "дн"),
        (r"\bhours?\b", "ч"),
        (r"\bminutes?\b", "мин"),
        (r"\bseconds?\b", "с"),
        (r"<1\s*second", "<1 с"),
    ]
    for pat, ru in repl:
        tail = re.sub(pat, ru, tail, flags=re.I)
    tail = re.sub(r"\s+", " ", tail).strip().strip(",").strip()
    return f"работает {tail}"


def prettify_container_status(name: str, status_text: str) -> str:
    st = status_text or ""
    low = st.lower()
    if "unhealthy" in low or "restarting" in low:
        emoji = "🟡"
    elif "up" in low or "healthy" in low:
        emoji = "🟢"
    else:
        emoji = "🔴"
    rus = humanize_uptime(st) if "up" in low else st
    rus = rus.replace("unhealthy", "с проблемами").replace("healthy", "здоров")
    rus = rus.replace("Restarting", "перезапуск").replace("Exited", "остановлен")
    return f"{emoji} {name} — {rus or 'не запущен'}"


def summarize_counters(ok: int, warn: int, bad: int) -> str:
    total = ok + warn + bad
    if bad > 0:
        return f"❌ Есть ошибки: {bad} пункт(а). Всего проверок: {total}."
    if warn > 0:
        return f"⚠️ Есть предупреждения: {warn} пункт(а). Всего проверок: {total}."
    return f"✅ Всё в порядке. Всего проверок: {total}."


def status_probe() -> dict:
    """
    Собирает фактическое состояние, но НЕ формирует текст сообщений.
    """
    probe: dict[str, Any] = {}
    ok = warn = bad = 0

    rc_ver, out_ver, err_ver = run_cmd("docker version --format '{{.Server.Version}}'")
    if rc_ver == 0 and out_ver:
        probe["proxy_line"] = f"🟢 docker-proxy — OK (демон {out_ver})"
        ok += 1
    else:
        probe["proxy_line"] = f"🔴 docker-proxy — ошибка ({err_ver or rc_ver})"
        bad += 1

    rc_ps, out_ps, _ = run_cmd("docker ps --format '{{.Names}}\\t{{.Status}}'")
    statuses = {}
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
    cont_lines = []
    for name in important:
        st = statuses.get(name, "не запущен")
        low = st.lower()
        if ("unhealthy" in low) or ("restarting" in low):
            cont_lines.append(
                f"🟡 {name} — {humanize_uptime(st) if 'up' in low else st}"
            )
            warn += 1
        elif ("up" in low) or ("healthy" in low):
            cont_lines.append(
                f"🟢 {name} — {humanize_uptime(st) if 'up' in low else st}"
            )
            ok += 1
        else:
            cont_lines.append(f"🔴 {name} — {st or 'не запущен'}")
            bad += 1
    probe["containers"] = cont_lines

    xray_c = os.getenv("XRAY_CONTAINER", "amnezia-xray")
    xray_cfg = os.getenv("XRAY_CONFIG_PATH", "/opt/amnezia/xray/server.json")
    rc_x, _, _ = _docker_exec(xray_c, f"test -r {shlex.quote(xray_cfg)}")
    if rc_x == 0:
        probe["xray_line"] = f"🟢 XRay конфиг доступен в {xray_c}"
        ok += 1
    else:
        probe["xray_line"] = f"🔴 XRay конфиг недоступен в {xray_c}"
        bad += 1

    awg_c = os.getenv("AWG_CONTAINER", "amnezia-awg")
    awg_cfg = os.getenv("AWG_CONFIG_PATH", "/opt/amnezia/awg/wg0.conf")
    rc_a, _, _ = _docker_exec(awg_c, f"test -r {shlex.quote(awg_cfg)}")
    if rc_a == 0:
        probe["awg_line"] = f"🟢 AmneziaWG конфиг доступен в {awg_c}"
        ok += 1
    else:
        probe["awg_line"] = f"🔴 AmneziaWG конфиг недоступен в {awg_c}"
        bad += 1

    can_write = True
    try:
        tmp = os.path.join("/app/data", ".wtest")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(tmp)
    except Exception:
        can_write = False

    size_mb = dir_size_bytes("/app/data") / (1024 * 1024)
    if can_write:
        probe["storage_line"] = f"🟢 /app/data — запись: да, объём: {size_mb:.1f} МБ"
        ok += 1
    else:
        probe["storage_line"] = f"🔴 /app/data — запись: нет, объём: {size_mb:.1f} МБ"
        bad += 1

    try:
        hb_age = time.time() - os.path.getmtime("/app/data/heartbeat")
        if hb_age < 120:
            probe["hb_line"] = f"🟢 heartbeat: {human_seconds(hb_age)} назад"
            ok += 1
        else:
            probe["hb_line"] = f"🟡 heartbeat: {human_seconds(hb_age)} назад"
            warn += 1
    except Exception:
        probe["hb_line"] = "🔴 heartbeat: недоступен"
        bad += 1

    probe["uptime_bot"] = human_seconds(
        time.time() - float(os.getenv("_BOOT_TS", "0") or "0")
    )
    probe["ts"] = datetime.now().astimezone().strftime("%H:%M:%S %d.%m.%Y")
    probe["summary"] = summarize_counters(ok, warn, bad)
    probe["ok"] = ok
    probe["warn"] = warn
    probe["bad"] = bad
    probe["important"] = important
    return probe
