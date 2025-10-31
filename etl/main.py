import logging
import sys
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from extract_data import extract_data
from validate_and_transform_data import validate_data, transform_data
from load_data import load_data
from config import engine, DB_TABLE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    logger.info("ETL process started")

    try:
        # === EXTRACT ===
        logger.info("Starting data extraction...")
        raw_data = extract_data()
        logger.info(f"Data extracted successfully: {len(raw_data)} records")

        # === VALIDATE ===
        required_columns = ["name", "alpha2Code", "alpha3Code", "region", "subregion", "population"]
        unique_key = "alpha3Code"

        if not validate_data(raw_data, unique_key=unique_key, required_columns=required_columns):
            logger.warning("Data validation failed. Process terminated.")
            return

        # === TRANSFORM ===
        logger.info("Starting data transformation...")
        transformed_data = transform_data(raw_data)
        logger.info("Data transformation completed successfully.")

        # === LOAD ===
        logger.info("Loading data into database...")
        load_data(transformed_data, engine, DB_TABLE)
        logger.info("Data loaded successfully into database.")

    except (ValueError, SQLAlchemyError) as e:
        logger.exception(f"ETL process failed due to data or database error: {e}")
        sys.exit(2)
    except Exception as e:
        logger.exception(f"Unexpected error occurred during ETL: {e}")
        sys.exit(1)
    finally:
        logger.info("ETL process finished")

if __name__ == "__main__":
    main()
