from sqlalchemy import text
from quant.common.db import create_db_engine  # this exists in your repo

engine = create_db_engine()

sql = open("migrations/0002_orchestrator_tables.sql").read()

with engine.begin() as conn:
    conn.execute(text(sql))

print("Migration applied.")