import os
import pandas as pd
from pathlib import Path
import yaml
from src.utils.logger import get_logger

def load_config(config_path="src/config/settings.yaml"):
    """Load YAML config."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def extract_data():
    """Extract Airbnb datasets into data/raw/."""
    cfg = load_config()
    logger = get_logger(cfg["etl"]["log_file"])
    raw_path = Path(cfg["data_paths"]["raw"])
    raw_path.mkdir(parents=True, exist_ok=True)

    sources = cfg["data_sources"]
    for name, filename in sources.items():
        src_file = raw_path / filename
        if src_file.exists():
            logger.info(f"{name} already exists, skipping.")
            continue

        logger.info(f"Downloading {name} dataset...")
        url = f"https://data.insideairbnb.com/united-states/ny/new-york-city/2024-01-01/data/{filename}"
        try:
            df = pd.read_csv(url)
            df.to_csv(src_file, index=False)
            logger.info(f"{name} extracted successfully → {src_file}")
        except Exception as e:
            logger.error(f"Failed to extract {name}: {e}")

if __name__ == "__main__":
    extract_data()
