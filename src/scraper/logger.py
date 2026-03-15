"""
logger.py — Structured logging setup for the EMMA scraper.

Outputs JSON-formatted log lines to stderr and optionally to a rotating log file.
Structured format enables easy parsing by log aggregation tools.

Usage:
    from src.scraper.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Discovered %d documents", count, extra={"issue_id": "abc123"})

Or at startup (call once from cli.py):
    from src.scraper.logger import configure_logging
    configure_logging(level="INFO", log_file="data/logs/scraper.log")
"""

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from typing import Optional


class JSONFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.

    Output example:
        {"ts": "2026-03-15T06:00:01Z", "level": "INFO", "module": "issue_search",
         "msg": "Found 12 issues for 'Manhattan College'", "issue_count": 12}
    """

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "ts": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "module": record.module,
            "msg": record.getMessage(),
        }

        # Include any extra fields attached via extra={}
        skip_keys = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message",
            "taskName",
        }
        for key, val in record.__dict__.items():
            if key not in skip_keys:
                entry[key] = val

        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str)


def configure_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_output: bool = True,
) -> None:
    """
    Configure root logger for the scraper.

    Args:
        level:       Log level string ("DEBUG", "INFO", "WARNING", "ERROR").
        log_file:    Optional path to a rotating log file.
        json_output: If True, use JSON formatter; otherwise use plain text.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Clear any existing handlers
    root.handlers.clear()

    formatter: logging.Formatter
    if json_output:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(module)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Stderr handler (always)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    root.addHandler(stderr_handler)

    # File handler (optional)
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,   # 10 MB per file
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Call configure_logging() first."""
    return logging.getLogger(name)
