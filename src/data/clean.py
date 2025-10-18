import pandas as pd
from pathlib import Path
import yaml
from src.utils.logger import get_logger

def load_config(config_path="src/config/settings.yaml"):
    """Load YAML config."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def basic_clean(df: pd.DataFrame, drop_threshold=0.6):
    """
    Basic data cleaning:
    - Drops columns with > drop_threshold null ratio
    - Fills numeric NaNs with median, categorical with mode
    - Strips column names
    """
    df.columns = [c.strip() for c in df.columns]

    na_ratio = df.isna().mean()
    drop_cols = na_ratio[na_ratio > drop_threshold].index
    df = df.drop(columns=drop_cols)

    for col in df.columns:
        if df[col].dtype == "O":  # categorical
            mode_val = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            df[col] = df[col].fillna(mode_val)
        else:
            df[col] = df[col].fillna(df[col].median())
    return df

def clean_all():
    """Clean each raw dataset and save to data/interim."""
    cfg = load_config()
    logger = get_logger(cfg["etl"]["log_file"])

    raw_path = Path(cfg["data_paths"]["raw"])
    interim_path = Path(cfg["data_paths"]["interim"])
    interim_path.mkdir(parents=True, exist_ok=True)

    files = cfg["data_sources"]
    for name, filename in files.items():
        try:
            df = pd.read_csv(raw_path / filename)
            logger.info(f"Cleaning {name} ({df.shape[0]} rows, {df.shape[1]} cols)")
            df_clean = basic_clean(df)
            output_file = interim_path / f"{name}_clean.csv"
            df_clean.to_csv(output_file, index=False)
            logger.info(f"{name} cleaned → {output_file}")
        except Exception as e:
            logger.error(f"Failed to clean {name}: {e}")

if __name__ == "__main__":
    clean_all()
