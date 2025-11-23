from __future__ import annotations

import logging
import logging.handlers
import os
from logging.config import dictConfig
from pathlib import Path

from config import LOG_LEVEL


def setup_logging() -> None:
    """Configure application-wide logging with sane defaults."""
    log_level = LOG_LEVEL.upper()
    
    # Определяем путь к директории логов
    # Если переменная окружения LOG_DIR не задана, используем текущую директорию
    log_dir = Path(os.getenv("LOG_DIR", "."))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "bot.log"
    error_log_file = log_dir / "bot_errors.log"

    handlers_config = {
        "console": {
            "class": "logging.StreamHandler",
            "level": log_level,
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": log_level,
            "formatter": "standard",
            "filename": str(log_file),
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
            "encoding": "utf-8",
        },
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",
            "formatter": "standard",
            "filename": str(error_log_file),
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
            "encoding": "utf-8",
        },
    }
    
    # Если запущено в systemd или как сервис, не выводим в консоль
    if os.getenv("SYSTEMD_SERVICE") == "1":
        handlers = ["file", "error_file"]
    else:
        handlers = ["console", "file", "error_file"]

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                },
            },
            "handlers": handlers_config,
            "root": {
                "level": log_level,
                "handlers": handlers,
            },
        }
    )

    logging.getLogger(__name__).debug("Logging configured with level %s, handlers: %s", log_level, handlers)

