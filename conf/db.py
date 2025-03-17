import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

user=os.getenv("DB_USER")
password=os.getenv("DB_PASSWORD")
host=os.getenv("DB_HOST")
port=os.getenv("DB_PORT")
db_name=os.getenv("DB_NAME")

print(f"DB_USER={user}, DB_HOST={host}")

URI=f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

engine=create_engine(URI, echo=True, pool_size=5, max_overflow=0)

SessionLocal = sessionmaker(bind=engine)