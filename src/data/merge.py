import pandas as pd
from pathlib import Path
import yaml
from src.utils.logger import get_logger

def load_config(config_path="src/config/settings.yaml"):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def merge_datasets():
    """Memory-efficient merge using chunked processing."""
    cfg = load_config()
    logger = get_logger(cfg["etl"]["log_file"])
    interim_path = Path(cfg["data_paths"]["interim"])
    processed_path = Path(cfg["data_paths"]["processed"])
    processed_path.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Loading listings and reviews...")
        listings = pd.read_csv(interim_path / "listings_clean.csv",
                               usecols=["id", "name", "neighbourhood", "price", "room_type"],
                               low_memory=False)
        reviews = pd.read_csv(interim_path / "reviews_clean.csv",
                              usecols=["listing_id", "date"],
                              low_memory=False)

        # Precompute review counts
        logger.info("Computing review counts...")
        review_counts = reviews.groupby("listing_id").size().reset_index(name="review_count")

        logger.info("Merging calendar in chunks...")
        chunk_iter = pd.read_csv(interim_path / "calendar_clean.csv",
                                 chunksize=1000000,  # 1M rows per chunk
                                 usecols=["listing_id", "date", "available"],
                                 low_memory=False)

        output_file = processed_path / "airbnb_merged.csv"
        first_chunk = True

        for i, chunk in enumerate(chunk_iter):
            logger.info(f"Processing chunk {i+1} ...")
            merged = pd.merge(chunk, listings, how="left",
                              left_on="listing_id", right_on="id")
            merged = pd.merge(merged, review_counts, how="left", on="listing_id")

            merged.to_csv(output_file, index=False, mode="w" if first_chunk else "a",
                          header=first_chunk)
            first_chunk = False

        logger.info(f"All chunks processed and saved to {output_file}")

    except Exception as e:
        logger.error(f"Chunked merge failed: {e}")

if __name__ == "__main__":
    merge_datasets()
