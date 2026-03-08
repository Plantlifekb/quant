# quant/engine/tasks/ingestion.py
import os
import logging
from typing import Dict, Any

import pandas as pd
import psycopg2
from psycopg2 import sql

logger = logging.getLogger("quant.engine.tasks.ingestion")

# Keep a small helper to import fetcher modules dynamically
def _load_fetcher(module_name: str):
    logger.info("Loaded fetcher module: %s", module_name)
    module = __import__(module_name, fromlist=["run"])
    return module


def write_prices_to_db(df: pd.DataFrame) -> int:
    """
    Write normalized prices to Postgres. Returns number of rows written.

    This function is defensive about the connection source:
    - It prefers DATABASE_URL / PG_DSN / PGCONN if provided and looks like a Postgres URL.
    - It will ignore sqlite:// style URLs and instead build a psycopg2 connection
      from PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.
    - If a SQLAlchemy-style postgres URL is provided (postgresql:// or postgresql+psycopg2://),
      it will use SQLAlchemy's to_sql path for bulk writes.
    """
    # Prefer common env vars (DATABASE_URL first)
    pg_dsn = os.getenv("DATABASE_URL") or os.getenv("PG_DSN") or os.getenv("PGCONN")

    # Build explicit connection kwargs from env vars (libpq style)
    conn_kwargs = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE", os.getenv("PG_DB", "quant")),
        "user": os.getenv("PGUSER", "quant"),
        "password": os.getenv("PGPASSWORD", os.getenv("PG_PASS", "quant")),
    }

    # If pg_dsn looks like a sqlite URL, ignore it
    if isinstance(pg_dsn, str) and pg_dsn.strip().lower().startswith("sqlite://"):
        logger.debug("Ignoring sqlite PG_DSN: %s", pg_dsn)
        pg_dsn = None

    # If pg_dsn is a SQLAlchemy-friendly Postgres URL, use to_sql path
    if isinstance(pg_dsn, str) and (
        pg_dsn.startswith("postgresql+psycopg2://")
        or pg_dsn.startswith("postgresql://")
        or pg_dsn.startswith("postgres://")
    ):
        try:
            from sqlalchemy import create_engine

            logger.info("Using SQLAlchemy engine for bulk write")
            engine = create_engine(pg_dsn, pool_pre_ping=True)
            # Use to_sql for bulk append; method='multi' speeds up inserts
            df.to_sql("prices", engine, schema="public", if_exists="append", index=False, method="multi")
            return len(df)
        except Exception as e:
            logger.exception("SQLAlchemy bulk write failed, will fall back to psycopg2: %s", e)
            # fall through to psycopg2 path

    # Try to connect with psycopg2. Prefer explicit kwargs; if that fails, try pg_dsn as libpq string.
    conn = None
    try:
        # Try explicit kwargs first (safer when envs are set)
        logger.debug("Attempting psycopg2 connect with explicit kwargs")
        conn = psycopg2.connect(**{k: v for k, v in conn_kwargs.items() if v is not None})
    except Exception as first_exc:
        logger.debug("psycopg2 connect with kwargs failed: %s", first_exc)
        # If we have a pg_dsn string that is not sqlite, try it as a libpq string
        if pg_dsn:
            try:
                logger.debug("Attempting psycopg2 connect with PG_DSN")
                conn = psycopg2.connect(pg_dsn)
            except Exception as second_exc:
                logger.exception("psycopg2 connect with PG_DSN failed: %s", second_exc)
                raise
        else:
            # No pg_dsn to try, re-raise the original exception
            raise

    # At this point we should have a valid psycopg2 connection
    try:
        with conn:
            with conn.cursor() as cur:
                # Convert DataFrame to list of tuples in the expected column order.
                # The ingestion normalizer should ensure columns match the DB schema.
                # Adjust columns/order here to match your actual table schema.
                expected_cols = ["Date", "Adj_Close", "Close", "High", "Low", "Open", "Volume", "ticker"]
                missing = [c for c in expected_cols if c not in df.columns]
                if missing:
                    logger.warning("DataFrame missing expected columns: %s", missing)

                # Prepare rows for insertion; convert pandas types to Python native types
                rows = []
                for _, r in df[expected_cols].iterrows():
                    rows.append(tuple(r.tolist()))

                # Example insert statement; replace with your actual upsert logic
                insert_sql = sql.SQL(
                    """
                    INSERT INTO prices (date, adj_close, close, high, low, open, volume, ticker)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """
                )
                if rows:
                    cur.executemany(insert_sql.as_string(conn), rows)
                # rowcount may be -1 for executemany depending on driver; return len(rows) as conservative count
                written = len(rows)
                logger.info("Wrote %d rows to DB", written)
                return written
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            logger.debug("Error closing connection", exc_info=True)


def task_ingest_and_write() -> Dict[str, Any]:
    """
    Top-level task wrapper used by tests and orchestration.
    Loads the configured fetcher, runs ingestion, normalizes data, and writes to DB.
    Returns a dict with status and metadata.
    """
    # Which fetcher module to use? Default to a known fetcher used in tests.
    fetcher_module = os.getenv("QUANT_FETCHER", "quant.ingestion_5years_quant_v1")
    logger.info("Loaded fetcher module: %s", fetcher_module)

    try:
        fetcher = _load_fetcher(fetcher_module)
    except Exception:
        logger.exception("Failed to import fetcher module: %s", fetcher_module)
        return {"status": "fetcher_import_failed", "fetcher": fetcher_module}

    try:
        # The fetcher.run() is expected to return a normalized pandas DataFrame
        final = fetcher.run()
        if not isinstance(final, pd.DataFrame):
            logger.error("Fetcher did not return a DataFrame")
            return {"status": "fetcher_return_invalid", "type": type(final).__name__}
        logger.info("Fetched data using %s.run", fetcher_module)
    except Exception:
        logger.exception("Fetcher run failed")
        return {"status": "fetch_failed", "fetcher": fetcher_module}

    try:
        rows = write_prices_to_db(final)
        return {"status": "ok", "rows_written": rows}
    except Exception:
        logger.exception("DB write failed")
        return {"status": "db_write_failed"}