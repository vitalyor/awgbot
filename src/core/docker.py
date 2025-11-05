from __future__ import annotations
import os, shlex, subprocess
from pathlib import Path

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
    try:
        p = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return p.returncode, p.stdout.strip(), p.stderr.strip()
    except Exception as e:
        return 999, "", str(e)

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
    return run_cmd(safe, timeout=timeout)