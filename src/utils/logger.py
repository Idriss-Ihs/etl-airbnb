import logging
from pathlib import Path

def get_logger(log_file="etl_log.txt"):
    """Configure and return a logger."""
    log_path = Path(log_file)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger("ETL_Logger")
    return logger
