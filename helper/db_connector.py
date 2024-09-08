from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv() # load .env file

# initiate variable to get env values for Amazon Sales Data Database
DB_USERNAME_SALES = os.getenv("DB_USERNAME_SALES")
DB_PASSWORD_SALES = os.getenv("DB_PASSWORD_SALES")
DB_HOST_SALES = os.getenv("DB_HOST_SALES")
DB_NAME_SALES = os.getenv("DB_NAME_SALES")

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# fetch data from amazone sales data
def postgres_engine_sales():
    engine = create_engine(f"postgresql://{DB_USERNAME_SALES}:{DB_PASSWORD_SALES}@{DB_HOST_SALES}/{DB_NAME_SALES}")
    return engine

# load all dataset to database
def postgres_engine_load():
    engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    return engine