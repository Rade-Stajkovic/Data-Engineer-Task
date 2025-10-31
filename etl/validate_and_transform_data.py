import pandas as pd
import logging

logger = logging.getLogger(__name__)

def validate_data(raw_data: pd.DataFrame, unique_key: str, required_columns: list[str]) -> bool:
    logger.info("Starting data validation...")

    if raw_data.empty:
        logger.warning("DataFrame is empty. Nothing to load")
        return False
    
    missing_cols = [col for col in required_columns if col not in raw_data.columns]
    if missing_cols:
        raise ValueError(f"Missing requred columns: {missing_cols}")
    
    if not pd.Series(raw_data[unique_key]).is_unique:
        raise ValueError("Primary key constraint violated in column {unique_key}")
    
    if raw_data[required_columns].isnull().any().any():
        null_columns = raw_data[required_columns].columns[raw_data[required_columns].isnull().any()].tolist()
        raise ValueError(f"Null values found in columns: {null_columns}")
    
    if "population" in raw_data.columns and (raw_data["population"] < 0).any():
        raise ValueError("Invalid population values found (negative numbers).")
    
    logger.info("Data validation passed successfully")
    return True
    
def transform_data(valid_data: list) -> pd.DataFrame:
    data = []
    for c in valid_data:
        try:
            currency_info = c.get("currencies")
            if currency_info:
                currencies_flat = {code: info.get("name") for code, info in currency_info.items()}
            else:
                currencies_flat = None

            data.append({
                "name_common": c.get("name", {}).get("common"),
                "name_official": c.get("name", {}).get("official"),
                "cca3": c.get("cca3"),
                "capital": c.get("capital")[0] if c.get("capital") else None,
                "region": c.get("region"),
                "subregion": c.get("subregion"),
                "population": c.get("population"),
                "area": c.get("area"),
                "languages": c.get("languages"),  
                "currencies": currencies_flat,
                "flag_png": c.get("flags", {}).get("png"),
                "flag_svg": c.get("flags", {}).get("svg")
            })
        except Exception as e:
            logger.warning(f"Skipping malformed record: {c.get('cca3', 'N/A')} ({e})")

    df = pd.DataFrame(data)

    logging.info(f"Transformed {df.shape[0]} countries into DataFrame")
    return df

