import requests
from requests.adapters import HTTPAdapter
import logging
from requests.packages.urllib3.util.retry import Retry
from .config import API_URL, API_FIELDS

logger = logging.getLogger(__name__)

def extract_data() -> list:
   
    url = f"{API_URL}?fields={API_FIELDS}"

    retry_strategy = Retry(
        total=3, 
        backoff_factor=0.5, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)

    try:
        logging.info(f"Fetching data from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} records from API")

        return data
    
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error while fetching data: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Conection error: {e}")
        raise
    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
    finally:
        session.close()
