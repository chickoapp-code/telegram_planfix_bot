from __future__ import annotations

import logging
from logging.config import dictConfig

from config import LOG_LEVEL


def setup_logging() -> None:
    """Configure application-wide logging with sane defaults."""
    log_level = LOG_LEVEL.upper()

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": log_level,
                    "formatter": "standard",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {
                "level": log_level,
                "handlers": ["console"],
            },
        }
    )

    logging.getLogger(__name__).debug("Logging configured with level %s", log_level)

