# quant/engine/tasks/ingestion.py
import os
import logging
from typing import Dict, Any, List

import pandas as pd
import psycopg2
from psycopg2 import sql

logger = logging.getLogger("quant.engine.tasks.ingestion")


def _load_fetcher(module_name: str):
    logger.info("Loaded fetcher module: %s", module_name)
    module = __import__(module_name, fromlist=["run"])
    return module


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize incoming DataFrame column names to canonical names expected by DB.
    Returns a new DataFrame with canonical columns:
      Date, Adj_Close, Close, High, Low, Open, Volume, ticker
    Raises KeyError if required columns cannot be found after normalization.
    """
    df_norm = df.copy()
    # map lower-stripped column name -> original column name
    col_map = {c.strip().lower(): c for c in df_norm.columns}

    def find_col(*variants: str):
        for v in variants:
            key = v.strip().lower()
            if key in col_map:
                return col_map[key]
        return None

    rename_map = {}

    # Date
    dcol = find_col("date", "day", "trade_date", "timestamp")
    if dcol:
        rename_map[dcol] = "Date"

    # Adjusted close
    acol = find_col("adj_close", "adjclose", "adjusted_close", "adjustedclose", "adj close")
    if acol:
        rename_map[acol] = "Adj_Close"

    # Close, High, Low, Open, Volume
    for name, canonical in [
        ("close", "Close"),
        ("high", "High"),
        ("low", "Low"),
        ("open", "Open"),
        ("volume", "Volume"),
    ]:
        found = find_col(name)
        if found:
            rename_map[found] = canonical

    # Ticker / symbol
    tcol = find_col("ticker", "symbol", "sym", "tick")
    if tcol:
        rename_map[tcol] = "ticker"

    if rename_map:
        df_norm = df_norm.rename(columns=rename_map)

    # Ensure ticker exists
    if "ticker" not in df_norm.columns:
        logger.warning("No ticker column found in DataFrame; filling with 'UNKNOWN'")
        df_norm["ticker"] = "UNKNOWN"

    # Ensure Date exists and is converted to date
    if "Date" in df_norm.columns:
        try:
            df_norm["Date"] = pd.to_datetime(df_norm["Date"]).dt.date
        except Exception:
            df_norm["Date"] = pd.to_datetime(df_norm["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        logger.error("No Date column found after normalization")
        raise KeyError("Missing Date column after normalization")

    # Ensure Adj_Close exists; if not, try to fall back to Close
    if "Adj_Close" not in df_norm.columns and "Close" in df_norm.columns:
        logger.info("Adj_Close missing; using Close as Adj_Close")
        df_norm["Adj_Close"] = df_norm["Close"]

    expected_cols = ["Date", "Adj_Close", "Close", "High", "Low", "Open", "Volume", "ticker"]
    missing = [c for c in expected_cols if c not in df_norm.columns]
    if missing:
        logger.error("After normalization, DataFrame missing expected columns: %s", missing)
        raise KeyError(f"Missing expected columns for DB write: {missing}")

    # Reorder to canonical order
    return df_norm[expected_cols]


def write_prices_to_db(df: pd.DataFrame) -> int:
    """
    Write normalized prices to Postgres. Returns number of rows written.

    Behavior:
    - Prefer DATABASE_URL / PG_DSN / PGCONN if provided and looks like Postgres.
    - Ignore sqlite:// style URLs and instead build a psycopg2 connection
      from PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.
    - If a SQLAlchemy-style Postgres URL is provided, use SQLAlchemy's to_sql path for bulk writes.
    - Ensure the target table exists before inserting.
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
            df.to_sql("prices", engine, schema="public", if_exists="append", index=False, method="multi")
            return len(df)
        except Exception as e:
            logger.exception("SQLAlchemy bulk write failed, will fall back to psycopg2: %s", e)
            # fall through to psycopg2 path

    # Normalize DataFrame columns to canonical set
    df_norm = _normalize_dataframe(df)

    # Try to connect with psycopg2. Prefer explicit kwargs; if that fails, try pg_dsn as libpq string.
    conn = None
    try:
        logger.debug("Attempting psycopg2 connect with explicit kwargs")
        conn = psycopg2.connect(**{k: v for k, v in conn_kwargs.items() if v is not None})
    except Exception as first_exc:
        logger.debug("psycopg2 connect with kwargs failed: %s", first_exc)
        if pg_dsn:
            try:
                logger.debug("Attempting psycopg2 connect with PG_DSN")
                conn = psycopg2.connect(pg_dsn)
            except Exception as second_exc:
                logger.exception("psycopg2 connect with PG_DSN failed: %s", second_exc)
                raise
        else:
            raise

    try:
        with conn:
            with conn.cursor() as cur:
                # Ensure the target table exists (create if missing)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.prices (
                      date date NOT NULL,
                      adj_close numeric,
                      close numeric,
                      high numeric,
                      low numeric,
                      open numeric,
                      volume bigint,
                      ticker text,
                      PRIMARY KEY (date, ticker)
                    );
                    """
                )

                # Prepare rows for insertion
                rows: List[tuple] = []
                for _, r in df_norm.iterrows():
                    row = []
                    for val in r.tolist():
                        if pd.isna(val):
                            row.append(None)
                        elif hasattr(val, "isoformat"):
                            row.append(val.isoformat())
                        else:
                            row.append(val)
                    rows.append(tuple(row))

                # Insert using parameterized query; ON CONFLICT DO NOTHING to avoid duplicates
                insert_sql = sql.SQL(
                    """
                    INSERT INTO public.prices (date, adj_close, close, high, low, open, volume, ticker)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """
                )
                if rows:
                    cur.executemany(insert_sql.as_string(conn), rows)
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
    fetcher_module = os.getenv("QUANT_FETCHER", "quant.ingestion_5years_quant_v1")
    logger.info("Loaded fetcher module: %s", fetcher_module)

    try:
        fetcher = _load_fetcher(fetcher_module)
    except Exception:
        logger.exception("Failed to import fetcher module: %s", fetcher_module)
        return {"status": "fetcher_import_failed", "fetcher": fetcher_module}

    try:
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