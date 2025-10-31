from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("HOST_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_TABLE = os.getenv("POSTGRES_TABLE")

API_URL = os.getenv("API_URL")
API_FIELDS = os.getenv("API_FIELDS")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)