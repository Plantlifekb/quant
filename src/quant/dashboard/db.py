import os
from sqlalchemy import create_engine

DB_URL = os.environ.get("DATABASE_URL", "postgresql://quant:quant@db:5432/quant")

def get_engine():
    return create_engine(DB_URL)