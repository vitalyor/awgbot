# /opt/awgbot/src/logger_setup.py
import os, re, json, gzip, logging, logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Any

LOG_DIR = Path("/app/data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "bot.log"

_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LEVEL = getattr(logging, _LEVEL, logging.INFO)

# соберём известные секреты: всё из ENV, чьи ключи содержат токен/секрет/пароль/ключ/pbk
_SECRET_VALUES: list[str] = []
_secret_key_re = re.compile(r"(TOKEN|SECRET|PASSWORD|PASS|API_KEY|PBK)", re.I)
for k, v in os.environ.items():
    if v and _secret_key_re.search(k):
        _SECRET_VALUES.append(str(v))

def _mask(s: str) -> str:
    # заменяем точные вхождения значений на ***
    for val in _SECRET_VALUES:
        if val and val in s:
            s = s.replace(val, "***")
    return s

def _mask_obj(obj: Any) -> Any:
    # рекурсивно маскируем строки внутри dict/list
    if isinstance(obj, str):
        return _mask(obj)
    if isinstance(obj, dict):
        return {k: _mask_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mask_obj(x) for x in obj]
    return obj

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        # базовый payload
        payload: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "file": record.pathname,
            "line": record.lineno,
            "func": record.funcName,
        }
        # если msg — словарь, сольём; иначе как строку
        msg = record.getMessage()
        if isinstance(record.msg, dict):
            payload.update(record.msg)  # уже структурный
        else:
            payload["msg"] = msg
        # исключение -> stack
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # маскируем секреты в payload
        payload = _mask_obj(payload)
        return json.dumps(payload, ensure_ascii=False)

class GzipTimedRotator(logging.handlers.TimedRotatingFileHandler):
    """Ротация раз в сутки с автосжатием .gz и хранением N файлов."""
    def __init__(self, filename, when="midnight", interval=1, backupCount=14, encoding="utf-8"):
        super().__init__(filename, when=when, interval=interval, backupCount=backupCount, encoding=encoding, utc=True)
    def rotate(self, source, dest):
        # переименовали — теперь сжимаем старый файл в .gz
        try:
            with open(source, "rb") as f_in, gzip.open(dest + ".gz", "wb") as f_out:
                f_out.writelines(f_in)
            os.remove(source)
        except Exception:
            # в крайнем случае — обычное переименование
            try:
                os.replace(source, dest)
            except Exception:
                pass

def get_logger(name: str = "awgbot") -> logging.Logger:
    # жёсткая защита от дублей: чистим хендлеры и у текущего логгера, и у корневого
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.propagate = False

    logger = logging.getLogger(name)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.propagate = False

    logger.setLevel(LEVEL)

    # размерная ротация (1MB x 5) -> JSON
    size_handler = logging.handlers.RotatingFileHandler(
        LOG_PATH, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
    )
    size_handler.setFormatter(JsonFormatter())
    size_handler.setLevel(LEVEL)

    # суточная ротация с gzip (14 суток) -> JSON
    time_handler = GzipTimedRotator(LOG_PATH, backupCount=14)
    time_handler.setFormatter(JsonFormatter())
    time_handler.setLevel(LEVEL)

    # поток в stdout для docker logs -> краткий человекочитаемый
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    stream_handler.setLevel(LEVEL)

    logger.addHandler(size_handler)
    logger.addHandler(time_handler)
    logger.addHandler(stream_handler)
    return logger