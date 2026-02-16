# C:\Quant\scripts\data\build_quant_prices_v1.py

import logging
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd
from dateutil import parser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("build_quant_prices_v1")

BASE = Path(r"C:\Quant")

# --------------------------------------------------------------------
# CONFIGURE INPUT / OUTPUT HERE IF NEEDED
# --------------------------------------------------------------------
# INPUT:
#   A raw prices file with at least: date, ticker, and either close/adj_close
#   You can change this path if your raw file is elsewhere.
RAW_PRICES = BASE / "data" / "ingestion" / "ingestion_5years.csv"

# OUTPUT:
#   Cleaned, 5-year, daily, per-ticker prices file used by the dashboard
OUT_PRICES = BASE / "data" / "analytics" / "quant_prices_v1.csv"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {c: c.strip().lower() for c in df.columns}
    df = df.rename(columns=col_map)

    # unify common variants
    rename_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in {"close_price", "price", "adj close", "adj_close_price"}:
            # we'll resolve close vs adj_close below
            continue
        if lc in {"open_price"}:
            rename_map[c] = "open"
        if lc in {"high_price"}:
            rename_map[c] = "high"
        if lc in {"low_price"}:
            rename_map[c] = "low"
        if lc in {"volume_shares", "vol"}:
            rename_map[c] = "volume"
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def robust_parse_date(series: pd.Series) -> pd.Series:
    def _parse(x):
        if pd.isna(x):
            return pd.NaT
        return parser.parse(str(x), dayfirst=True)

    return series.apply(_parse)


def detect_price_columns(df: pd.DataFrame):
    cols = {c.lower(): c for c in df.columns}

    close_col = None
    adj_close_col = None

    # candidates for close
    for cand in ["close", "close_price", "price"]:
        if cand in cols:
            close_col = cols[cand]
            break

    # candidates for adj_close
    for cand in ["adj_close", "adj close", "adj_close_price"]:
        if cand in cols:
            adj_close_col = cols[cand]
            break

    if close_col is None and adj_close_col is None:
        raise ValueError(
            "Raw prices file must contain at least one of: "
            "'close', 'close_price', 'price', 'adj_close', 'adj close', 'adj_close_price'"
        )

    return close_col, adj_close_col


def clean_and_align_prices(raw_path: Path, out_path: Path):
    log.info(f"Loading raw prices from: {raw_path}")
    df = pd.read_csv(raw_path)

    log.info("Normalizing column names")
    df = normalize_columns(df)

    # basic required columns
    if "date" not in df.columns:
        raise ValueError("Raw prices file must contain a 'date' column")
    if "ticker" not in df.columns:
        raise ValueError("Raw prices file must contain a 'ticker' column")

    close_col, adj_close_col = detect_price_columns(df)

    log.info(f"Using close column: {close_col}")
    if adj_close_col:
        log.info(f"Using adj_close column: {adj_close_col}")
    else:
        log.info("No adj_close column detected; will only output 'close'")

    # parse date
    log.info("Parsing dates with robust mixed-format parser")
    df["date"] = robust_parse_date(df["date"])
    df = df.dropna(subset=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    # normalize ticker
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    # keep only relevant columns
    keep_cols = ["date", "ticker"]
    for c in ["open", "high", "low", "volume"]:
        if c in df.columns:
            keep_cols.append(c)
    if close_col is not None:
        keep_cols.append(close_col)
    if adj_close_col is not None:
        keep_cols.append(adj_close_col)

    df = df[keep_cols].copy()

    # rename price columns to canonical names
    if close_col is not None and close_col != "close":
        df = df.rename(columns={close_col: "close"})
    if adj_close_col is not None and adj_close_col != "adj_close":
        df = df.rename(columns={adj_close_col: "adj_close"})

    # drop exact duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    after = len(df)
    if after < before:
        log.info(f"Dropped {before - after} duplicate (ticker, date) rows")

    # restrict to last 5 years based on max date in file
    max_date = df["date"].max()
    if pd.isna(max_date):
        raise ValueError("No valid dates found in raw prices file")

    # Keep full ingestion history — no truncation
    df = df.copy()
    log.info("Keeping full ingestion history (no 5-year truncation applied)")

    # sort
    df = df.sort_values(["ticker", "date"])

    # reindex to business days per ticker and forward-fill
    log.info("Reindexing to business days per ticker and forward-filling within each ticker")
    cleaned_list = []
    for ticker, g in df.groupby("ticker", sort=False):
        g = g.sort_values("date")
        start = g["date"].min()
        end = g["date"].max()
        idx = pd.bdate_range(start=start, end=end, tz=None)
        g = g.set_index("date").reindex(idx)
        g.index.name = "date"
        g["ticker"] = ticker

        # forward-fill prices and volumes within ticker
        for col in g.columns:
            if col == "ticker":
                continue
            g[col] = g[col].ffill()

        cleaned_list.append(g.reset_index())

    cleaned = pd.concat(cleaned_list, ignore_index=True)

    # final sanity: drop rows where we still have no close/adj_close
    if "close" in cleaned.columns and "adj_close" in cleaned.columns:
        cleaned = cleaned.dropna(subset=["close", "adj_close"], how="all")
    elif "close" in cleaned.columns:
        cleaned = cleaned.dropna(subset=["close"])
    elif "adj_close" in cleaned.columns:
        cleaned = cleaned.dropna(subset=["adj_close"])

    # enforce column order
    final_cols = ["date", "ticker"]
    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        if c in cleaned.columns and c not in final_cols:
            final_cols.append(c)
    cleaned = cleaned[final_cols].copy()

    # summary
    log.info(f"Final cleaned rows: {len(cleaned)}")
    log.info(f"Tickers: {cleaned['ticker'].nunique()}")
    log.info(f"Date range: {cleaned['date'].min().date()} to {cleaned['date'].max().date()}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(out_path, index=False)
    log.info(f"Wrote cleaned prices to: {out_path}")


if __name__ == "__main__":
    clean_and_align_prices(RAW_PRICES, OUT_PRICES)