#!/usr/bin/env python3
# Quant v1.0 — 5-Year Ingestion (Fetcher + DB Write, Container-Safe)

import os
import time
from datetime import datetime, timedelta, date
from io import StringIO
from pathlib import Path
from typing import Optional, List

import numpy as np
import pandas as pd
import yfinance as yf
import requests
from sqlalchemy import text

from quant.logging_quant_v1 import log


# ----------------------------------------------------------------------
# Calendar helpers
# ----------------------------------------------------------------------


def last_trading_day(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def calendar_5y_with_buffer(
    end: date, periods: int = 1260, buffer_days: int = 60
) -> pd.DatetimeIndex:
    return pd.bdate_range(end=end, periods=periods + buffer_days)


# ----------------------------------------------------------------------
# Normalization
# ----------------------------------------------------------------------


def normalize(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([c for c in col if c]) for col in df.columns]

    df = df.reset_index()

    if "Date" not in df.columns:
        df.rename(columns={df.columns[0]: "Date"}, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["ticker"] = str(ticker).strip().upper()

    cols = list(df.columns)
    rename = {}

    suffix = f"_{ticker}"
    for c in cols:
        base = c
        if c.endswith(suffix):
            base = c[: -len(suffix)]
        if base.startswith("Price_"):
            base = base.replace("Price_", "")
        base_stripped = base.strip()
        if base_stripped in ["Open", "High", "Low", "Close", "Adj Close", "AdjClose", "Volume"]:
            if base_stripped in ["Adj Close", "AdjClose"]:
                rename[c] = "Adj_Close"
            else:
                rename[c] = base_stripped

    static_map = {"Adj Close": "Adj_Close", "AdjClose": "Adj_Close"}
    for k, v in static_map.items():
        if k in df.columns and k not in rename:
            rename[k] = v

    df = df.rename(columns=rename)

    for col in ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]:
        if col not in df.columns:
            df[col] = np.nan

    for col in ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info(f"[ingestion_5years_quant_v1] {ticker}: After normalize, cols={list(df.columns)}")

    return df[["Date", "ticker", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]]


# ----------------------------------------------------------------------
# Fetchers
# ----------------------------------------------------------------------


def fetch_yahoo(t: str, start: str, end: str, retries: int = 3, delay: float = 1.0) -> Optional[pd.DataFrame]:
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(t, start=start, end=end, progress=False, auto_adjust=False)
            if df is None or df.empty:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: Yahoo returned EMPTY (attempt {attempt}/{retries})"
                )
            else:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: Yahoo OK ({len(df)} rows, attempt {attempt})"
                )
                return normalize(df, t)
        except Exception as e:
            log.info(
                f"[ingestion_5years_quant_v1] {t}: Yahoo error on attempt {attempt}/{retries} -> {e}"
            )
        time.sleep(delay)
    return None


def fetch_alpha_vantage(
    t: str, start: str, end: str, key: str, retries: int = 2, delay: float = 1.5
) -> Optional[pd.DataFrame]:
    for attempt in range(1, retries + 1):
        try:
            url = (
                "https://www.alphavantage.co/query"
                "?function=TIME_SERIES_DAILY_ADJUSTED"
                f"&symbol={t}&apikey={key}&outputsize=full&datatype=csv"
            )
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: Alpha Vantage HTTP {r.status_code} "
                    f"(attempt {attempt}/{retries})"
                )
                time.sleep(delay)
                continue

            df = pd.read_csv(StringIO(r.text))
            if "timestamp" not in df.columns:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: Alpha Vantage malformed "
                    f"(attempt {attempt}/{retries})"
                )
                time.sleep(delay)
                continue

            df.rename(
                columns={
                    "timestamp": "Date",
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "adjusted_close": "Adj_Close",
                    "volume": "Volume",
                },
                inplace=True,
            )

            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df[(df["Date"] >= pd.to_datetime(start)) & (df["Date"] <= pd.to_datetime(end))]

            if df.empty:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: Alpha Vantage empty after filter "
                    f"(attempt {attempt}/{retries})"
                )
                time.sleep(delay)
                continue

            log.info(
                f"[ingestion_5years_quant_v1] {t}: Alpha Vantage OK ({len(df)} rows, attempt {attempt})"
            )
            return normalize(df, t)
        except Exception as e:
            log.info(
                f"[ingestion_5years_quant_v1] {t}: Alpha Vantage error on attempt {attempt}/{retries} -> {e}"
            )
            time.sleep(delay)
    return None


# ----------------------------------------------------------------------
# Reindex / fill
# ----------------------------------------------------------------------


def reindex_and_fill(df: pd.DataFrame, cal: pd.DatetimeIndex, t: str) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        log.info(f"[ingestion_5years_quant_v1] {t}: reindex_and_fill received EMPTY frame")
        return None

    df = df.set_index("Date").reindex(cal)
    df[["Open", "High", "Low", "Close", "Adj_Close", "Volume"]] = df[
        ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]
    ].ffill().bfill()

    df.index.name = "Date"
    df = df.reset_index()
    df["ticker"] = t

    non_nan = df[["Open", "High", "Low", "Close", "Adj_Close", "Volume"]].notna().sum().sum()
    if non_nan == 0:
        log.info(
            "[ingestion_5years_quant_v1] "
            f"{t}: All OHLCV values NaN after reindex/fill — marking ticker as FAILED"
        )
        return None

    return df


# ----------------------------------------------------------------------
# Main run: fetch + DB write
# ----------------------------------------------------------------------


def run(engine, api_key: Optional[str] = None) -> None:
    log.info("[ingestion_5years_quant_v1] === Starting 5-year ingestion ===")

    base_dir = Path(__file__).resolve().parent
    default_ref = base_dir / "config" / "ticker_reference.csv"

    ticker_ref_path = os.getenv("TICKER_REFERENCE_PATH", str(default_ref))

    if not os.path.exists(ticker_ref_path):
        raise FileNotFoundError(f"Ticker reference not found at {ticker_ref_path}")

    if api_key is None:
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")

    tickers_df = pd.read_csv(ticker_ref_path)
    tickers_df["ticker"] = tickers_df["ticker"].astype(str).str.strip().str.upper()

    required_cols = {"ticker", "company_name", "market_sector"}
    lower_cols = {c.lower(): c for c in tickers_df.columns}
    missing = required_cols - set(lower_cols.keys())
    if missing:
        raise ValueError(f"Ticker reference missing required columns: {missing}")

    # Normalize column names to expected ones
    tickers_df = tickers_df.rename(
        columns={
            lower_cols["ticker"]: "ticker",
            lower_cols["company_name"]: "company_name",
            lower_cols["market_sector"]: "market_sector",
        }
    )

    tickers = tickers_df["ticker"].tolist()

    today = datetime.utcnow().date()
    end = last_trading_day(today)
    cal = calendar_5y_with_buffer(end=end, periods=1260, buffer_days=60)

    start = cal.min().strftime("%Y-%m-%d")
    end_s = (cal.max() + timedelta(days=1)).strftime("%Y-%m-%d")

    frames: List[pd.DataFrame] = []
    success: List[str] = []
    failed: List[str] = []

    for t in tickers:
        try:
            log.info(f"[ingestion_5years_quant_v1] Ingesting {t} ...")
            time.sleep(0.7)

            df = fetch_yahoo(t, start, end_s)
            if df is None:
                log.info(f"[ingestion_5years_quant_v1] {t}: Falling back to Alpha Vantage")
                df = fetch_alpha_vantage(t, start, end_s, api_key)

            if df is None:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: FAILED (no data from Yahoo or Alpha Vantage)"
                )
                failed.append(t)
                continue

            df = reindex_and_fill(df, cal, t)
            if df is None:
                log.info(
                    f"[ingestion_5years_quant_v1] {t}: FAILED (no valid OHLCV after reindex/fill)"
                )
                failed.append(t)
                continue

            frames.append(df)
            success.append(t)
        except Exception as e:
            log.info(f"[ingestion_5years_quant_v1] {t}: FATAL error -> {e}")
            failed.append(t)
            continue

    if not frames:
        log.info("[ingestion_5years_quant_v1] No tickers ingested — raising RuntimeError")
        raise RuntimeError("No tickers ingested — ingestion_5years_quant_v1 aborted")

    final = pd.concat(frames, ignore_index=True)

    final = final.merge(
        tickers_df[["ticker", "company_name", "market_sector"]],
        on="ticker",
        how="left",
    )

    final["date"] = pd.to_datetime(final["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    final.drop(columns=["Date"], inplace=True)

    final.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj_Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )

    final["run_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    final = final[
        [
            "date",
            "ticker",
            "company_name",
            "market_sector",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "run_date",
        ]
    ]

    log.info(f"[ingestion_5years_quant_v1] Ingestion complete. Rows: {len(final)}")
    if failed:
        log.info(f"[ingestion_5years_quant_v1] Failed tickers: {', '.join(failed)}")

    # ------------------------------------------------------------------
    # DB write with upsert into prices
    # ------------------------------------------------------------------
    insert_sql = text(
        """
        INSERT INTO prices (
            date,
            ticker,
            company_name,
            market_sector,
            open,
            high,
            low,
            close,
            adj_close,
            volume,
            run_date
        )
        VALUES (
            :date,
            :ticker,
            :company_name,
            :market_sector,
            :open,
            :high,
            :low,
            :close,
            :adj_close,
            :volume,
            :run_date
        )
        ON CONFLICT (ticker, date) DO UPDATE SET
            company_name   = EXCLUDED.company_name,
            market_sector  = EXCLUDED.market_sector,
            open           = EXCLUDED.open,
            high           = EXCLUDED.high,
            low            = EXCLUDED.low,
            close          = EXCLUDED.close,
            adj_close      = EXCLUDED.adj_close,
            volume         = EXCLUDED.volume,
            run_date       = EXCLUDED.run_date
        """
    )

    records = final.to_dict(orient="records")
    with engine.begin() as conn:
        for row in records:
            conn.execute(insert_sql, row)

    log.info(
        f"[ingestion_5years_quant_v1] DB write complete. Upserted {len(records)} rows into prices."
    )


if __name__ == "__main__":
    from quant.engine.db import create_db_engine

    eng = create_db_engine()
    run(eng)