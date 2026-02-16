"""
logging_attribution_suite_v1.py
Quant v1.0 — Dedicated logger for the Attribution Suite

Provides:
- UTC timestamps
- Rotating log files
- Consistent formatting
- Module-level loggers
- Automatic directory creation

Used by all modules in attribution_suite_v1.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone

# Root directory for logs
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "attribution_suite_v1"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "attribution_suite.log"


def _utc_timestamp() -> str:
    """Return current UTC timestamp as string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def get_logger(name: str) -> logging.Logger:
    """
    Create or retrieve a logger with rotating file output.
    Each module in the attribution suite should call:
        logger = get_logger("module_name")
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Already configured

    logger.setLevel(logging.INFO)

    # Log format
    formatter = logging.Formatter(
        fmt="[{asctime}] [{name}] {message}",
        datefmt="%Y-%m-%d %H:%M:%S UTC",
        style="{",
    )

    # Rotating file handler (5 MB per file, keep 5 backups)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    # Stream handler (console)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.propagate = False

    logger.info("Logger initialised for module '%s'", name)

    return logger