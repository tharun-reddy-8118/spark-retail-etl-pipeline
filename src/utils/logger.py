import logging
from pathlib import Path

from src.utils.config import PROJECT_ROOT

# ==============================
# Log Directory
# ==============================

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "etl.log"


# ==============================
# Logger Configuration
# ==============================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger instance.

    Args:
        name (str): Module name (__name__)

    Returns:
        logging.Logger
    """
    return logging.getLogger(name)