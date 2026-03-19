from sqlalchemy import text
from quant.engine.db import create_db_engine

def ingest_prices():
    engine = create_db_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        # TODO: real ingestion logic