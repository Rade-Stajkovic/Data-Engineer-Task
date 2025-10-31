import pandas as pd
import logging

def transform_countries(countries: list) -> pd.DataFrame:
    """Convert API data to pandas DataFrame"""
    data = []
    for c in countries:
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
    df = pd.DataFrame(data)

    logging.info(f"Transformed {df.shape[0]} countries into DataFrame")
    return df
