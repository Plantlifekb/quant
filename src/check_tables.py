# check_tables.py
import os
from sqlalchemy import create_engine, inspect

DB = os.getenv("DATABASE_URL", "sqlite:///C:/Quant/data/quant_dev.sqlite")
e = create_engine(DB)
insp = inspect(e)
print("Using DB:", e.url)
print("Tables:", insp.get_table_names())