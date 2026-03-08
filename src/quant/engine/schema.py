from sqlalchemy import text
from quant.engine.db import create_db_engine

def create_schema():
    engine = create_db_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS task_metadata (
                task_name TEXT PRIMARY KEY,
                status TEXT,
                last_run TIMESTAMP,
                last_success TIMESTAMP,
                last_error TIMESTAMP
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS task_run_history (
                id SERIAL PRIMARY KEY,
                task_name TEXT,
                run_started TIMESTAMP,
                run_finished TIMESTAMP,
                status TEXT,
                error_text TEXT
            );
        """))