"""Structured logging setup for QBO ToProcess."""

import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(
    name: str = "qbo_toprocess",
    log_to_file: bool = True,
    log_level: int = logging.INFO,
) -> logging.Logger:
    """
    Set up and return a configured logger.

    Args:
        name: Logger name
        log_to_file: Whether to also log to a file
        log_level: Logging level

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(log_level)

    # Console handler with simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler with detailed format
    if log_to_file:
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f"qbo_toprocess_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "qbo_toprocess") -> logging.Logger:
    """Get an existing logger or create a new one."""
    return logging.getLogger(name)
