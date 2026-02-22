"""Configure Rich console + file logging."""

import logging

from rich.console import Console
from rich.logging import RichHandler

from harvester.settings import LOG_PATH

terminal = Console()

_ready = False


def init_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up the 'harvester' logger (idempotent)."""
    global _ready

    logger = logging.getLogger("harvester")
    if _ready:
        return logger

    logger.setLevel(level)

    # Pretty console output via Rich
    ch = RichHandler(console=terminal, show_path=False, markup=True)
    ch.setLevel(level)

    # Persistent log file
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s"))

    logger.addHandler(ch)
    logger.addHandler(fh)

    _ready = True
    return logger
