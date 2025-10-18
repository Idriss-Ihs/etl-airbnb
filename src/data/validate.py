import pandas as pd
from pathlib import Path
import yaml
from src.utils.logger import get_logger

def load_config(config_path="src/config/settings.yaml"):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def validate_data():
    """Basic validation on processed dataset."""
    cfg = load_config()
    logger = get_logger(cfg["etl"]["log_file"])
    processed_path = Path(cfg["data_paths"]["processed"])
    merged_file = processed_path / "airbnb_merged.csv"

    if not merged_file.exists():
        logger.error("No merged dataset found for validation.")
        return

    df = pd.read_csv(merged_file)
    logger.info(f"Validating merged dataset ({df.shape[0]} rows)")

    issues = {}
    issues["missing_ratio"] = df.isna().mean().to_dict()
    issues["duplicates"] = int(df.duplicated().sum())
    issues["columns"] = list(df.columns)

 
    report_path = processed_path / "validation_report.txt"
    with open(report_path, "w") as f:
        f.write("=== Airbnb Data Validation Report ===\n")
        f.write(f"Total rows: {df.shape[0]}\n\n")
        f.write(f"Duplicate rows: {issues['duplicates']}\n\n")
        f.write("Missing values (ratio):\n")
        for k, v in issues["missing_ratio"].items():
            f.write(f"  {k}: {v:.2f}\n")

    logger.info(f"Validation report saved → {report_path}")

if __name__ == "__main__":
    validate_data()
