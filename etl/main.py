import logging
from etl.extract_data import extract_countries
from etl.transform_data import transform_countries
from etl.load_data import load_to_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    countries = extract_countries()
    if not countries:
        logging.warning("No data extracted. Exiting.")
        return
    df = transform_countries(countries)
    load_to_db(df)

if __name__ == "__main__":
    main()
