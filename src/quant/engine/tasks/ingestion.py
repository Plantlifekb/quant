# quant/engine/tasks/ingestion.py
"""
Wrapper task to run the existing ingestion_5years_quant_v1.run() fetcher
and write results into public.prices (ticker, date, close).

Place this file at: quant/engine/tasks/ingestion.py
"""

from __future__ import annotations

import os
import sys
import logging
import importlib
from typing import Optional, Dict

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Prefer your project's logger if available
try:
    from logging_quant_v1 import log  # existing project logger
except Exception:
    log = logging.getLogger("ingestion.task")
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(handler)
    log.setLevel(logging.INFO)


def _locate_and_import_fetcher():
    """
    Try to import ingestion_5years_quant_v1 as a module.
    If not importable, attempt to load it from a few likely file locations
    relative to this file and the repo root.
    Returns the module object.
    """
    module_name = "ingestion_5years_quant_v1"
    try:
        return importlib.import_module(module_name)
    except Exception:
        # Try a set of candidate paths relative to this file and repo root
        here = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
        candidates = [
            os.path.join(repo_root, "ingestion_5years_quant_v1.py"),
            os.path.join(repo_root, "quant", "ingestion_5years_quant_v1.py"),
            os.path.join(repo_root, "src", "ingestion_5years_quant_v1.py"),
            os.path.join(os.getcwd(), "ingestion_5years_quant_v1.py"),
            os.path.join(os.getcwd(), "quant", "ingestion_5years_quant_v1.py"),
            os.path.join(here, "..", "..", "ingestion_5years_quant_v1.py"),
        ]



        for p in candidates:
            p = os.path.abspath(os.path.normpath(p))
            if os.path.exists(p):
                spec = importlib.util.spec_from_file_location(module_name, p)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = mod
                    spec.loader.exec_module(mod)
                    log.info(f"[ingestion.task] Loaded fetcher from {p}")
                    return mod
        raise ImportError(
            "Could not import ingestion_5years_quant_v1. "
            "Place the file on PYTHONPATH or next to the repo root so the wrapper can find it."
        )


def _build_pg_dsn() -> str:
    """Build a psycopg2 DSN from env vars or DATABASE_URL."""
    pg_dsn = os.getenv("DATABASE_URL") or os.getenv("PG_DSN")
    if pg_dsn:
        return pg_dsn

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "quant")
    user = os.getenv("PGUSER", "quant")
    password = os.getenv("PGPASSWORD", "")
    # psycopg2 accepts either a URL or a space-separated DSN
    return f"host={host} port={port} dbname={db} user={user} password={password}"


def write_prices_to_db(df: pd.DataFrame, pg_dsn: Optional[str] = None) -> int:
    """
    Bulk upsert DataFrame rows into public.prices (ticker, date, close).
    Returns number of rows attempted to write.
    """
    if pg_dsn is None:
        pg_dsn = _build_pg_dsn()

    # Keep only the columns we need for prices table
    if "ticker" not in df.columns or "date" not in df.columns or "close" not in df.columns:
        raise ValueError("DataFrame must contain columns: ticker, date, close")

    to_write = df[["ticker", "date", "close"]].dropna(subset=["ticker", "date"]).copy()
    if to_write.empty:
        log.info("[ingestion.task] No rows to write to prices table.")
        return 0

    # Ensure date format is YYYY-MM-DD
    to_write["date"] = pd.to_datetime(to_write["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    records = list(to_write.itertuples(index=False, name=None))

    insert_sql = """
    INSERT INTO public.prices (ticker, date, close)
    VALUES %s
    ON CONFLICT (ticker, date) DO UPDATE
      SET close = EXCLUDED.close
    ;
    """

    conn = psycopg2.connect(pg_dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, records, page_size=1000)
        log.info(f"[ingestion.task] Wrote {len(records)} rows to public.prices")
        return len(records)
    finally:
        conn.close()


def task_ingest_and_write(api_key: Optional[str] = None) -> Dict[str, object]:
    """
    Task wrapper the orchestrator can call.
    - runs the fetcher (ingestion_5years_quant_v1.run)
    - writes results to public.prices
    - returns a small status dict
    """
    log.info("[ingestion.task] Starting ingestion wrapper")
    mod = _locate_and_import_fetcher()

    if not hasattr(mod, "run"):
        raise AttributeError("Fetcher module does not expose run(api_key: Optional[str]) -> pd.DataFrame")

    # Ensure TICKER_REFERENCE_PATH defaults to the repo config if not set
    if not os.getenv("TICKER_REFERENCE_PATH"):
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        candidate = os.path.join(repo_root, "quant", "config", "ticker_reference.csv")
        if os.path.exists(candidate):
            os.environ["TICKER_REFERENCE_PATH"] = candidate
            log.info(f"[ingestion.task] Set TICKER_REFERENCE_PATH={candidate}")

    try:
        df = mod.run(api_key=api_key)
    except Exception as e:
        log.exception(f"[ingestion.task] Fetcher run() failed -> {e}")
        return {"status": "fetch_failed", "rows_written": 0, "last_date": None, "error": str(e)}

    if df is None or df.empty:
        log.info("[ingestion.task] Ingestion returned no data")
        return {"status": "no_data", "rows_written": 0, "last_date": None}

    try:
        rows = write_prices_to_db(df)
    except Exception as e:
        log.exception(f"[ingestion.task] DB write failed -> {e}")
        return {"status": "db_write_failed", "rows_written": 0, "last_date": None, "error": str(e)}

    last_date = df["date"].max() if "date" in df.columns else None
    status = {"status": "ok", "rows_written": rows, "last_date": last_date}
    log.info(f"[ingestion.task] Completed ingestion wrapper: {status}")
    return status


# CLI entry for manual testing
if __name__ == "__main__":
    import json
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY", None)
    result = task_ingest_and_write(api_key=api_key)
    print(json.dumps(result, indent=2))

# Backwards-compatible entrypoint expected by orchestrator
def run(*args, **kwargs):
    return task_ingest_and_write(*args, **kwargs)

