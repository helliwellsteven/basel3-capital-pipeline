import pandas as pd
from pathlib import Path
import logging

# Sets up a logger for this module — prints timestamped messages to the console
# instead of bare print() statements. Standard practice in production pipelines.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# The expected columns for a Basel III position file.
# Defined at the module level so they can be reused in tests later.
EXPECTED_COLUMNS = [
    "trade_id",
    "trade_date",
    "counterparty_id",
    "asset_class",
    "notional_usd",
    "risk_weight",
    "reporting_entity",
]

class BronzeIngestion:
    def __init__(self, source_path: str, output_path: str):
        # Converts the string paths to Path objects — cleaner to work with
        # and handles Windows vs Mac path separators automatically
        self.source_path = Path(source_path)
        self.output_path = Path(output_path)
        self.df = None  # DataFrame starts empty — gets populated in read_source()

    def read_source(self):
        # Reads the CSV and stores it as a DataFrame on the object
        # Raises a clear error if the file doesn't exist
        if not self.source_path.exists():
            raise FileNotFoundError(f"Source file not found: {self.source_path}")
        self.df = pd.read_csv(self.source_path)
        logger.info(f"Read {len(self.df)} records from {self.source_path}")

    def validate_schema(self):
        # Checks that every expected column is present
        # Missing columns raise an error immediately — fail fast, fail loud
        missing = set(EXPECTED_COLUMNS) - set(self.df.columns)
        if missing:
            raise ValueError(f"Schema validation failed. Missing columns: {missing}")
        logger.info("Schema validation passed")

    def write_bronze(self):
        # Creates the output folder if it doesn't exist, then writes Parquet
        # Parquet is columnar and compressed — standard Bronze layer format
        self.output_path.mkdir(parents=True, exist_ok=True)
        output_file = self.output_path / "positions_bronze.parquet"
        self.df.to_parquet(output_file, index=False)
        logger.info(f"Wrote {len(self.df)} records to {output_file}")

    def run(self):
        # The single entry point — orchestrates all steps in order
        # This is the only method you call from outside the class
        logger.info("Starting Bronze ingestion")
        self.read_source()
        self.validate_schema()
        self.write_bronze()
        logger.info("Bronze ingestion complete")


if __name__ == "__main__":
    BronzeIngestion(
        source_path="data/raw/bronze/positions_raw.csv",
        output_path="data/raw/bronze",
    ).run()