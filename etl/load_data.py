import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

def load_data(df: pd.DataFrame, engine: Engine, table_name: str) -> None:
    """
    Loads transformed data into a PostgreSQL table.
    Uses an UPSERT strategy (insert or update if conflict).
    """

    if df.empty:
        logger.warning("No data to load. Skipping database operation.")
        return

    try:
        logger.info(f"Preparing to load {len(df)} records into '{table_name}'...")

        temp_table = f"{table_name}_staging"

        df.to_sql(temp_table, engine, if_exists='replace', index=False)
        logger.info(f"Data successfully written to temporary table '{temp_table}'.")

        with engine.begin() as conn:
            columns = ', '.join(df.columns)
            updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in df.columns if col != 'alpha3Code'])

            upsert_sql = text(f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT (alpha3Code) DO UPDATE
                SET {updates};
            """)

            conn.execute(upsert_sql)
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
            logger.info(f"Upsert completed successfully into '{table_name}'.")

    except SQLAlchemyError as e:
        logger.exception("Database error during data load.")
        raise
    except Exception as e:
        logger.exception("Unexpected error during data load.")
        raise
