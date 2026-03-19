import os
import logging
from typing import Dict, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Fetcher: requires SQLAlchemy engine
from quant.ingestion_5years_quant_v1 import run as fetch_run

# Correct engine factory
from quant.engine.db import create_db_engine

LOG = logging.getLogger("quant.engine.tasks.ingestion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


# ----------------------------------------------------------------------
# DB connection (legacy psycopg2 write path)
# ----------------------------------------------------------------------
def _db_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(url)


# ----------------------------------------------------------------------
# CLEAN, SAFE, INSTITUTIONAL-GRADE NORMALIZATION
# ----------------------------------------------------------------------
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Institutional‑grade normalization:
    - Requires a real ticker column (no inference from df.name)
    - Enforces uppercase, trimmed tickers
    - Normalizes price/volume columns
    - Ensures Date column exists and is converted properly
    """

    # 1. Enforce presence of a real ticker column
    if "ticker" not in df.columns:
        raise RuntimeError("Normalization error: missing ticker column")

    df["ticker"] = (
        df["ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # 2. Normalize column names
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("date", "index"):
            col_map[c] = "Date"
        elif lc in ("adj_close", "adjclose"):
            col_map[c] = "Adj_Close"
        elif lc == "close":
            col_map[c] = "Close"
        elif lc == "high":
            col_map[c] = "High"
        elif lc == "low":
            col_map[c] = "Low"
        elif lc == "open":
            col_map[c] = "Open"
        elif lc in ("volume", "vol"):
            col_map[c] = "Volume"

    df = df.rename(columns=col_map)

    # 3. Ensure Date column exists
    if "Date" not in df.columns:
        if df.index.name in (None, "date", "Date"):
            df = df.reset_index()
        else:
            raise RuntimeError("Normalization error: missing Date column")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

    # 4. Numeric coercion
    for col in ("Adj_Close", "Close", "High", "Low", "Open"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5. Volume normalization
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")

    LOG.info("%s: After normalize, cols=%s",
             df["ticker"].iat[0] if "ticker" in df.columns and len(df) else "df",
             list(df.columns))

    return df


# ----------------------------------------------------------------------
# Legacy psycopg2 write path
# ----------------------------------------------------------------------
def write_prices_to_db(df: pd.DataFrame) -> int:
    if df.empty:
        LOG.info("No rows to write")
        return 0

    expected = ["Date", "Adj_Close", "Close", "High", "Low", "Open", "Volume", "ticker"]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                r["Date"],
                None if pd.isna(r["Adj_Close"]) else float(r["Adj_Close"]),
                None if pd.isna(r["Close"]) else float(r["Close"]),
                None if pd.isna(r["High"]) else float(r["High"]),
                None if pd.isna(r["Low"]) else float(r["Low"]),
                None if pd.isna(r["Open"]) else float(r["Open"]),
                None if pd.isna(r["Volume"]) else int(r["Volume"]),
                str(r["ticker"]),
            )
        )

    insert_sql = """
    INSERT INTO public.prices (date, adj_close, close, high, low, open, volume, ticker)
    VALUES %s
    ON CONFLICT (date, ticker) DO UPDATE
      SET adj_close = EXCLUDED.adj_close,
          close     = EXCLUDED.close,
          high      = EXCLUDED.high,
          low       = EXCLUDED.low,
          open      = EXCLUDED.open,
          volume    = EXCLUDED.volume;
    """

    conn = _db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, template=None, page_size=100)
        LOG.info("Wrote %d rows to DB", len(rows))
        return len(rows)
    finally:
        conn.close()


# ----------------------------------------------------------------------
# MAIN TASK: fetch + normalize + write
# ----------------------------------------------------------------------
def task_ingest_and_write(*args, **kwargs) -> Dict[str, Any]:
    LOG.info("Starting ingestion task")

    # 1. Fetch using SQLAlchemy engine
    try:
        engine = create_db_engine()
        fetched = fetch_run(engine)
        LOG.info("Fetched data using quant.ingestion_5years_quant_v1.run")
    except Exception:
        LOG.exception("Fetcher failed")
        return {"status": "fetch_failed"}

    # 2. Normalize or accept write-through mode
    try:
        if isinstance(fetched, list):
            dfs = [_normalize_df(df) for df in fetched]
            final = pd.concat(dfs, ignore_index=True)

        elif isinstance(fetched, pd.DataFrame):
            final = _normalize_df(fetched)

        elif isinstance(fetched, dict):
            dfs = []
            for t, df in fetched.items():
                df["ticker"] = t
                dfs.append(_normalize_df(df))
            final = pd.concat(dfs, ignore_index=True)

        elif fetched is None:
            # Fetcher already wrote directly to DB
            LOG.info("Fetcher returned None (write-through mode). Skipping normalization.")
            return {"status": "ok", "rows_written": None, "last_date": None}

        else:
            LOG.error("Unexpected fetcher return type: %s", type(fetched))
            return {"status": "fetch_unexpected_type"}

        if final.empty:
            LOG.info("Ingestion returned no rows")
            return {"status": "no_data"}

    except Exception:
        LOG.exception("Normalization failed")
        return {"status": "normalize_failed"}

    # 3. Write to DB
    try:
        rows_written = write_prices_to_db(final)
        last_date = None
        if "Date" in final.columns and not final["Date"].isna().all():
            last_date = str(max(final["Date"]))
        return {"status": "ok", "rows_written": rows_written, "last_date": last_date}

    except Exception:
        LOG.exception("DB write failed")
        last_date = None
        if "Date" in final.columns and not final["Date"].isna().all():
            last_date = str(max(final["Date"]))
        return {"status": "db_write_failed", "last_date": last_date}