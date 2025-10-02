import logging
import os
from datetime import datetime


_INITIALIZED = False


def setup_logging():
    global _INITIALIZED
    if _INITIALIZED:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = os.getenv(
        "LOG_FORMAT",
        "%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s",
    )
    datefmt = os.getenv("LOG_DATEFMT", "%Y-%m-%d %H:%M:%S")

    # Configure root logger only once
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    # Reduce noise from third-party libs unless LOG_LEVEL is DEBUG
    if level > logging.DEBUG:
        for noisy in ("urllib3", "requests", "psycopg2"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        # Suppress yfinance ERROR logs (delisted symbols, etc.) but keep CRITICAL
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    _INITIALIZED = True


def get_logger(name: str | None = None) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name or __name__)
