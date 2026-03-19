# quant/engine/metadata.py

from sqlalchemy import text
from quant.engine.db import create_db_engine


def ensure_metadata_tables():
    """
    Ensures the metadata tables required by the orchestrator exist.
    This function is deterministic, idempotent, and safe to run on every engine startup.
    """

    engine = create_db_engine()

    ddl = """
    CREATE TABLE IF NOT EXISTS task_metadata (
        task_name TEXT PRIMARY KEY,
        status TEXT,
        last_run TIMESTAMP,
        last_success TIMESTAMP,
        last_error TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS task_run_history (
        id SERIAL PRIMARY KEY,
        task_name TEXT NOT NULL,
        run_started TIMESTAMP NOT NULL,
        run_finished TIMESTAMP,
        status TEXT NOT NULL,
        error_text TEXT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))