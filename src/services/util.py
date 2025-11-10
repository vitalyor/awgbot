import os, subprocess, uuid, time, re, logging
from datetime import datetime, UTC
from typing import Optional, Union, List

logger = logging.getLogger("awgbot")

# ====== ПУТИ ХРАНЕНИЯ ======
DATA_DIR = "/app/data"
STATE_PATH = os.path.join(DATA_DIR, "state.json")

# ====== XRAY ENV ======
XRAY_CONTAINER     = os.environ.get("XRAY_CONTAINER", "amnezia-xray")
XRAY_CONFIG_PATH   = os.environ.get("XRAY_CONFIG_PATH", "/opt/amnezia/xray/server.json")
XRAY_INBOUND_INDEX = int(os.environ.get("XRAY_INBOUND_INDEX", "0"))
XRAY_CONNECT_HOST  = os.environ.get("XRAY_CONNECT_HOST", "")

# ====== AWG ENV ======
AWG_CONTAINER    = os.environ.get("AWG_CONTAINER", "amnezia-awg")
AWG_CONFIG_PATH  = os.environ.get("AWG_CONFIG_PATH", "/opt/amnezia/awg/wg0.conf")
AWG_CONNECT_HOST = os.environ.get("AWG_CONNECT_HOST", "")
AWG_LISTEN_PORT  = int(os.environ.get("AWG_LISTEN_PORT", "0"))
AWG_BIN          = os.environ.get("AWG_BIN", "awg")

# ====== ТЮНИНГ ПО УМОЛЧАНИЮ (можно переопределить через ENV) ======
DOCKER_EXEC_TIMEOUT     = int(os.environ.get("DOCKER_EXEC_TIMEOUT", "10"))
DOCKER_EXEC_RETRIES     = int(os.environ.get("DOCKER_EXEC_RETRIES", "1"))
DOCKER_EXEC_RETRY_SECS  = float(os.environ.get("DOCKER_EXEC_RETRY_SECS", "2"))
DOCKER_RESTART_TIMEOUT  = int(os.environ.get("DOCKER_RESTART_TIMEOUT", "30"))
DOCKER_UP_TIMEOUT       = int(os.environ.get("DOCKER_UP_TIMEOUT", "30"))

# ========= базовые утилиты =========
def run(cmd: list[str], timeout: int = 30) -> str:
    start = time.monotonic()
    try:
        p = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timeout {timeout}s: {' '.join(cmd)}")

    duration = round(time.monotonic() - start, 2)
    if p.returncode != 0:
        err = (p.stderr or "").strip()
        msg = err or f"command failed ({' '.join(cmd)})"
        logger.warning({"event": "run_failed", "cmd": cmd, "code": p.returncode, "err": msg, "t": duration})
        raise RuntimeError(msg)

    out = (p.stdout or "").strip()
    logger.debug({"event": "run_ok", "cmd": cmd, "t": duration})
    return out

# Ошибки, на которых имеет смысл попробовать повтор
_RETRYABLE_ERR_RE = re.compile(
    r"(i/o timeout|context deadline exceeded|cannot connect to the docker daemon|"
    r"HTTP .* error|OCI runtime .* failed|EOF)",
    re.IGNORECASE
)

def _should_retry(errmsg: str) -> bool:
    return bool(_RETRYABLE_ERR_RE.search(errmsg or ""))

def docker_exec(
    container: str,
    cmd: Union[str, List[str]],
    *,
    timeout: int = DOCKER_EXEC_TIMEOUT,
    retries: int = DOCKER_EXEC_RETRIES,
    retry_delay: float = DOCKER_EXEC_RETRY_SECS,
) -> tuple[int, str, str]:
    """
    Запускает команду внутри контейнера и возвращает (rc, stdout, stderr).
    - cmd: list → исполняется как argv без шелла: docker exec -i <ctr> <argv...>
    - cmd: str  → исполняется через оболочку контейнера: sh -lc "<cmd>"
    Не выбрасывает исключение при rc!=0; повторяет попытку на ретраибл-ошибках.
    """
    last_err = ""
    for attempt in range(retries + 1):
        if isinstance(cmd, list):
            argv = ["docker", "exec", "-i", container, *cmd]
            cmd_repr = cmd
        elif isinstance(cmd, str):
            argv = ["docker", "exec", "-i", container, "sh", "-lc", cmd]
            cmd_repr = cmd
        else:
            raise TypeError("cmd must be str or list[str]")

        t0 = time.monotonic()
        p = subprocess.run(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        rc = p.returncode
        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()
        dt = round(time.monotonic() - t0, 2)

        if rc == 0:
            logger.debug({"event": "docker_exec_ok", "container": container, "cmd": cmd_repr, "t": dt})
            return rc, out, err

        # Ошибка — решить, повторять ли
        last_err = err or f"command failed ({' '.join(argv)})"
        if attempt < retries and _should_retry(last_err):
            logger.warning({"event": "docker_exec_retry", "try": attempt + 1, "container": container, "cmd": cmd_repr, "err": last_err})
            time.sleep(retry_delay)
            continue

        logger.warning({"event": "docker_exec_failed", "container": container, "cmd": cmd_repr, "rc": rc, "err": last_err, "t": dt})
        return rc, out, err

def docker_read_file(container: str, path: str, timeout: int = DOCKER_EXEC_TIMEOUT) -> str:
    rc, out, err = docker_exec(container, ["sh", "-lc", f"cat {shq(path)}"], timeout=timeout)
    if rc != 0:
        raise RuntimeError(err or f"failed to read {path}")
    return out

def docker_write_file_atomic(container: str, path: str, content: str, timeout: int = 15):
    """
    Надёжная запись файла в контейнер: docker cp -> mv.
    Гарантируем наличие директории и логируем успешную запись.
    """
    host_tmp = f"/tmp/awgbot_{os.getpid()}_{uuid.uuid4().hex}.tmp"
    os.makedirs(os.path.dirname(host_tmp), exist_ok=True)
    with open(host_tmp, "w", encoding="utf-8") as f:
        f.write(content)
    try:
        run(["docker", "cp", host_tmp, f"{container}:{path}.tmp"], timeout=timeout)
        rc, _, err = docker_exec(
            container,
            ["sh", "-lc", f"mkdir -p $(dirname {shq(path)}) && mv {shq(path)}.tmp {shq(path)}"],
            timeout=timeout,
        )
        if rc != 0:
            raise RuntimeError(err or "docker mv failed")
        logger.info({"event": "docker_write_atomic", "container": container, "path": path})
    finally:
        try:
            os.remove(host_tmp)
        except OSError:
            pass

def shq(s: str) -> str:
    return "'" + (s or "").replace("'", "'\"'\"'") + "'"

def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")

def get_awg_bin() -> str:
    try:
        rc, out, _ = docker_exec(
            AWG_CONTAINER, ["sh", "-lc", f"command -v {shq(AWG_BIN)} >/dev/null && echo OK || echo NO"],
        )
        if rc == 0 and out.strip() == "OK":
            return AWG_BIN
    except Exception:
        pass
    return "wg"

# ========= рестарт с ожиданием Up/healthy =========
def _inspect_state(container: str, timeout: int = 6) -> tuple[bool, Optional[str]]:
    """
    Возвращает (running, health_status|None)
    health_status: 'healthy' | 'unhealthy' | 'starting' | None (если healthcheck не настроен)
    """
    fmt = "{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{end}}"
    try:
        out = run(["docker", "inspect", "-f", fmt, container], timeout=timeout) or ""
        parts = out.split()
        running = (len(parts) > 0 and parts[0].lower() == "true")
        health = (parts[1].strip() if len(parts) > 1 else None) or None
        return running, health
    except Exception:
        return False, None

def docker_restart(
    container: str,
    *,
    timeout: int = DOCKER_RESTART_TIMEOUT,
    wait_up: bool = True,
    up_timeout: int = DOCKER_UP_TIMEOUT,
    wait_healthy: bool = False,
    retry_once: bool = True,
    poll_every: float = 1.0,
) -> None:
    """
    Синхронный рестарт контейнера с ожиданием Up [/healthy] и одним повтором при неудаче.
    Блокирует поток вызвавшего кода до завершения попытки.
    """
    def _wait() -> bool:
        deadline = time.time() + up_timeout
        time.sleep(min(1.0, poll_every))  # короткая пауза, чтобы Docker успел сменить состояние
        while time.time() < deadline:
            running, health = _inspect_state(container)
            if running and (not wait_healthy or health == "healthy"):
                logger.info({"event": "docker_up", "container": container, "health": health})
                return True
            time.sleep(poll_every)
        return False

    t0 = time.monotonic()
    logger.info({
        "event": "docker_restart",
        "container": container,
        "wait_up": wait_up,
        "wait_healthy": wait_healthy,
        "up_timeout": up_timeout
    })
    run(["docker", "restart", container], timeout=timeout)

    if wait_up:
        if _wait():
            logger.info({"event": "docker_restart_ok", "container": container, "t": round(time.monotonic() - t0, 2)})
            return
        if retry_once:
            logger.warning({"event": "docker_restart_retry", "container": container})
            run(["docker", "restart", container], timeout=timeout)
            if _wait():
                logger.info({"event": "docker_restart_ok2", "container": container, "t": round(time.monotonic() - t0, 2)})
                return
        logger.error({
            "event": "docker_restart_failed",
            "container": container,
            "wait_healthy": wait_healthy,
            "up_timeout": up_timeout
        })
        raise RuntimeError(f"{container} did not become Up{' & healthy' if wait_healthy else ''} within {up_timeout}s")