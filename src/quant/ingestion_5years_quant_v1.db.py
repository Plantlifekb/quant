*** Begin Patch
*** Add File: quant/ingestion_5years_quant_v1.db.py
+#!/usr/bin/env python3
+# Quant v1.0 — 5-Year Ingestion (Fetcher + DB Write, Container-Safe)
+import os
+import time
+from datetime import datetime, timedelta, date
+from typing import Optional, List
+
+import numpy as np
+import pandas as pd
+import yfinance as yf
+from sqlalchemy import text
+
+from quant.logging_quant_v1 import log
+from quant.db.utils import write_df, read_df
+from quant.db.connection import get_engine
+
+# ----------------------------------------------------------------------
+# Calendar helpers
+# ----------------------------------------------------------------------
+
+def last_trading_day(d: date) -> date:
+    while d.weekday() >= 5:
+        d -= timedelta(days=1)
+    return d
+
+def calendar_5y_with_buffer(end: date, periods: int = 1260, buffer_days: int = 60) -> pd.DatetimeIndex:
+    return pd.bdate_range(end=end, periods=periods + buffer_days)
+
+# ----------------------------------------------------------------------
+# Normalization
+# ----------------------------------------------------------------------
+
+def normalize(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
+    if isinstance(df.columns, pd.MultiIndex):
+        df.columns = ["_".join([c for c in col if c]) for col in df.columns]
+
+    df = df.reset_index()
+
+    if "Date" not in df.columns:
+        df.rename(columns={df.columns[0]: "Date"}, inplace=True)
+
+    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
+    df["ticker"] = str(ticker).strip().upper()
+
+    cols = list(df.columns)
+    rename = {}
+
+    suffix = f"_{ticker}"
+    for c in cols:
+        base = c
+        if c.endswith(suffix):
+            base = c[: -len(suffix)]
+        if base.startswith("Price_"):
+            base = base.replace("Price_", "")
+        base_stripped = base.strip()
+        if base_stripped in ["Open", "High", "Low", "Close", "Adj Close", "AdjClose", "Volume"]:
+            if base_stripped in ["Adj Close", "AdjClose"]:
+                rename[c] = "Adj_Close"
+            else:
+                rename[c] = base_stripped
+
+    static_map = {"Adj Close": "Adj_Close", "AdjClose": "Adj_Close"}
+    for k, v in static_map.items():
+        if k in df.columns and k not in rename:
+            rename[k] = v
+
+    df = df.rename(columns=rename)
+
+    for col in ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]:
+        if col not in df.columns:
+            df[col] = pd.NA
+
+    keep = ["Date", "ticker", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
+    df = df[[c for c in keep if c in df.columns]]
+    df = df.dropna(subset=["Date"])
+    df = df.sort_values("Date").reset_index(drop=True)
+    return df
+
+# ----------------------------------------------------------------------
+# Fetcher
+# ----------------------------------------------------------------------
+
+def fetch_prices_for_ticker(ticker: str, start: Optional[date] = None, end: Optional[date] = None) -> pd.DataFrame:
+    ticker = str(ticker).strip().upper()
+    if end is None:
+        end = last_trading_day(date.today())
+    if start is None:
+        start = end - timedelta(days=365 * 5)
+
+    try:
+        df = yf.download(ticker, start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(), progress=False)
+        if df.empty:
+            log.warning(f"No data returned for {ticker}")
+            return pd.DataFrame()
+        df = df.reset_index()
+        return normalize(df, ticker)
+    except Exception as e:
+        log.exception(f"Failed to fetch {ticker}: {e}")
+        return pd.DataFrame()
+
+# ----------------------------------------------------------------------
+# DB write helpers
+# ----------------------------------------------------------------------
+
+def write_chunked(table_name: str, df: pd.DataFrame, if_exists: str = "append", chunk_size: int = 1000):
+    if df.empty:
+        return 0
+    write_df(table_name, df, if_exists=if_exists, index=False)
+    return len(df)
+
+def upsert_prices_postgres(df: pd.DataFrame, table_name: str = "prices"):
+    if df.empty:
+        return 0
+    engine = get_engine()
+    rows = df.to_dict(orient="records")
+    stmt = f"""
+    INSERT INTO {table_name} (Date, ticker, Open, High, Low, Close, Adj_Close, Volume)
+    VALUES (:Date, :ticker, :Open, :High, :Low, :Close, :Adj_Close, :Volume)
+    ON CONFLICT (ticker, Date) DO UPDATE SET
+      Open = EXCLUDED.Open,
+      High = EXCLUDED.High,
+      Low = EXCLUDED.Low,
+      Close = EXCLUDED.Close,
+      Adj_Close = EXCLUDED.Adj_Close,
+      Volume = EXCLUDED.Volume
+    """
+    with engine.begin() as conn:
+        conn.execute(text(stmt), rows)
+    return len(rows)
+
+# ----------------------------------------------------------------------
+# Orchestration
+# ----------------------------------------------------------------------
+
+def ingest_tickers(tickers: List[str], table_name: str = "ingestion_prices", retries: int = 2, delay: int = 3):
+    total = 0
+    for t in tickers:
+        attempt = 0
+        while attempt <= retries:
+            attempt += 1
+            try:
+                df = fetch_prices_for_ticker(t)
+                if df.empty:
+                    log.info(f"No rows for {t}, skipping")
+                    break
+                rows_written = write_chunked(table_name, df, if_exists="append")
+                total += rows_written
+                log.info(f"Wrote {rows_written} rows for {t} to {table_name}")
+                break
+            except Exception as e:
+                log.exception(f"Error ingesting {t} attempt {attempt}: {e}")
+                if attempt <= retries:
+                    time.sleep(delay)
+                else:
+                    log.error(f"Giving up on {t} after {attempt} attempts")
+    return total
+
+# ----------------------------------------------------------------------
+# CLI entrypoint
+# ----------------------------------------------------------------------
+
+def load_ticker_list(path: Optional[str] = None) -> List[str]:
+    if path and os.path.exists(path):
+        df = pd.read_csv(path)
+        if "ticker" in df.columns:
+            return df["ticker"].astype(str).tolist()
+        return df.iloc[:, 0].astype(str).tolist()
+    return ["AAPL", "MSFT", "GOOG"]
+
+def main():
+    db_url = os.getenv("QUANT_DB_URL", "sqlite:///./quant_data.db")
+    log.info(f"Using DB URL {db_url}")
+    tickers_path = os.getenv("TICKERS_CSV", "data/tickers.csv")
+    tickers = load_ticker_list(tickers_path)
+    start = datetime.utcnow()
+    rows = ingest_tickers(tickers, table_name="ingestion_prices")
+    elapsed = (datetime.utcnow() - start).total_seconds()
+    log.info(f"Ingestion complete rows={rows} elapsed_s={elapsed}")
+
+if __name__ == "__main__":
+    main()
*** End Patch