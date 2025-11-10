from __future__ import annotations
import os, shlex, subprocess
from pathlib import Path

from services.logger_setup import get_logger
log = get_logger("core.docker")

# Белый список контейнеров, в которые разрешаем docker exec
ALLOWED_CONTAINERS = {
    os.getenv("AWG_CONTAINER", "amnezia-awg"),
    os.getenv("XRAY_CONTAINER", "amnezia-xray"),
    os.getenv("DNS_CONTAINER", "amnezia-dns"),
    "awgbot",
}

def run_cmd(cmd: str, timeout: int = 6):
    """
    Выполняет shell-команду. Возвращает (rc, stdout, stderr).
    Не бросает исключения.
    """
    log.debug(f"Running cmd: {cmd}")
    try:
        p = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        rc, stdout, stderr = p.returncode, p.stdout.strip(), p.stderr.strip()
        log.debug(f"rc={rc}, stdout={stdout[:100]!r}, stderr={stderr[:100]!r}")
        return rc, stdout, stderr
    except Exception as e:
        rc, stdout, stderr = 999, "", str(e)
        log.debug(f"rc={rc}, stdout={stdout[:100]!r}, stderr={stderr[:100]!r}")
        return rc, stdout, stderr

def dir_size_bytes(path: str) -> int:
    try:
        total = 0
        p = Path(path)
        if not p.exists():
            return 0
        for x in p.rglob("*"):
            if x.is_file():
                total += x.stat().st_size
        return total
    except Exception:
        return 0

def tcp_check(host: str, port: int, timeout_ms: int = 800) -> bool:
    """Быстрая проверка TCP-порта (без TLS)."""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout_ms / 1000.0)
        sock.connect((host, int(port)))
        sock.close()
        return True
    except Exception:
        return False

def _docker_exec(container: str, cmd: str, timeout: int = 6):
    if container not in ALLOWED_CONTAINERS:
        return 998, "", f"container {container} not allowed"
    safe = f"docker exec {shlex.quote(container)} sh -lc {shlex.quote(cmd)}"
    log.debug(f"Running cmd: {safe}")
    rc, stdout, stderr = run_cmd(safe, timeout=timeout)
    log.debug(f"rc={rc}, stdout={stdout[:100]!r}, stderr={stderr[:100]!r}")
    return rc, stdout, stderr