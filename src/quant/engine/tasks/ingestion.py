# quant/engine/tasks/ingestion.py
import os
import logging
from datetime import datetime
from typing import Dict, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from quant.ingestion_5years_quant_v1 import run as fetch_run

LOG = logging.getLogger("quant.engine.tasks.ingestion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def _db_conn():
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    dbname = os.getenv("PGDATABASE", "quant")
    user = os.getenv("PGUSER", "quant")
    password = os.getenv("PGPASSWORD", "quant")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("date", "index"):
            col_map[c] = "Date"
        elif lc in ("adj_close", "adjclose"):
            col_map[c] = "Adj_Close"
        elif lc in ("close",):
            col_map[c] = "Close"
        elif lc in ("high",):
            col_map[c] = "High"
        elif lc in ("low",):
            col_map[c] = "Low"
        elif lc in ("open",):
            col_map[c] = "Open"
        elif lc in ("volume", "vol"):
            col_map[c] = "Volume"
        elif lc in ("ticker", "symbol"):
            col_map[c] = "ticker"
    df = df.rename(columns=col_map)

    if "Date" not in df.columns and df.index.name in (None, "Date", "date"):
        df = df.reset_index()

    if "ticker" not in df.columns:
        if hasattr(df, "name") and isinstance(df.name, str):
            df["ticker"] = df.name
        else:
            df["ticker"] = df.get("Symbol") or df.get("symbol") or "UNKNOWN"

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

    for col in ("Adj_Close", "Close", "High", "Low", "Open"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")

    LOG.info("%s: After normalize, cols=%s", df["ticker"].iat[0] if "ticker" in df.columns and len(df) else "df", list(df.columns))
    return df


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
          close = EXCLUDED.close,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          open = EXCLUDED.open,
          volume = EXCLUDED.volume
    ;
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


def task_ingest_and_write(*args, **kwargs) -> Dict[str, Any]:
    LOG.info("Starting ingestion task")
    try:
        fetched = fetch_run()
        LOG.info("Fetched data using quant.ingestion_5years_quant_v1.run")
    except Exception:
        LOG.exception("Fetcher failed")
        return {"status": "fetch_failed"}

    try:
        if isinstance(fetched, list):
            dfs = []
            for df in fetched:
                dfs.append(_normalize_df(df))
            final = pd.concat(dfs, ignore_index=True)
        elif isinstance(fetched, pd.DataFrame):
            final = _normalize_df(fetched)
        else:
            if isinstance(fetched, dict):
                dfs = []
                for t, df in fetched.items():
                    df["ticker"] = t
                    dfs.append(_normalize_df(df))
                final = pd.concat(dfs, ignore_index=True)
            else:
                LOG.error("Unexpected fetcher return type: %s", type(fetched))
                return {"status": "fetch_unexpected_type"}

        if final.empty:
            LOG.info("Ingestion returned no rows")
            return {"status": "no_data"}
    except Exception:
        LOG.exception("Normalization failed")
        return {"status": "normalize_failed"}

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
