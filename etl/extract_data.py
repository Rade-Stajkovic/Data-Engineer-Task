import os
import requests
from dotenv import load_dotenv
import logging

load_dotenv()

API_URL = os.getenv("API_URL")
API_FIELDS = os.getenv("API_FIELDS")

def extract_countries() -> list:
    """Fetch country data from API"""
    url = f"{API_URL}?fields={API_FIELDS}"
    try:
        logging.info(f"Fetching data from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch countries: {e}")
        return []
