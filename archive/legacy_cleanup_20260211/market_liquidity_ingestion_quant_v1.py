r"""
Quant v1.0 — market_liquidity_ingestion_quant_v1.py
Version: v1.0 (patched for ingestion_5years.csv)

Purpose:
- Build the canonical market-liquidity dataset required by the Liquidity Cost Model.
- Compute ADV, spread_bps (proxy), volatility_20, and clean price/volume fields.

Input file:
C:\Quant\data\ingestion\ingestion_5years.csv

Expected columns:
date, ticker, company_name, market_sector,
open, high, low, close, adj_close, volume, run_date

Output:
C:\Quant\data\analytics\quant_market_liquidity_timeseries.csv
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("market_liquidity_ingestion_quant_v1")

# ---------------------------------------------------------------------
# File locations
# ---------------------------------------------------------------------

RAW_MARKET_FILE = PROJECT_ROOT / "data" / "ingestion" / "ingestion_5years.csv"
OUT_LIQUIDITY_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_market_liquidity_timeseries.csv"

ADV_LOOKBACK = 20
VOL_LOOKBACK = 20
EPS = 1e-12

# ---------------------------------------------------------------------
# Load raw OHLCV
# ---------------------------------------------------------------------

def load_raw_ohlcv() -> pd.DataFrame:
    logger.info("Loading OHLCV from %s", RAW_MARKET_FILE)
    df = pd.read_csv(RAW_MARKET_FILE)

    required = {"date", "ticker", "close", "volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in ingestion_5years.csv: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df.dropna(subset=["date", "ticker", "close", "volume"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    logger.info("Loaded %d OHLCV rows.", len(df))
    return df

# ---------------------------------------------------------------------
# Feature construction
# ---------------------------------------------------------------------

def compute_adv(df: pd.DataFrame) -> pd.DataFrame:
    df["dollar_volume"] = df["close"] * df["volume"]
    df["adv_20"] = (
        df.groupby("ticker")["dollar_volume"]
        .rolling(ADV_LOOKBACK, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )
    return df

def compute_volatility(df: pd.DataFrame) -> pd.DataFrame:
    df["return"] = (
        df.groupby("ticker")["close"]
        .pct_change()
        .fillna(0.0)
    )
    df["volatility_20"] = (
        df.groupby("ticker")["return"]
        .rolling(VOL_LOOKBACK, min_periods=1)
        .std()
        .reset_index(level=0, drop=True)
    )
    return df

def compute_spread_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback spread model:
    spread_bps = 10 + 5 * volatility_20
    """
    df["spread_bps"] = 10 + 5 * df["volatility_20"].fillna(0.0)
    return df

# ---------------------------------------------------------------------
# Build liquidity dataset
# ---------------------------------------------------------------------

def build_liquidity_timeseries(ohlcv: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building liquidity dataset.")

    df = ohlcv.copy()
    df = compute_adv(df)
    df = compute_volatility(df)
    df = compute_spread_proxy(df)

    out = df[[
        "date",
        "ticker",
        "close",
        "adv_20",
        "spread_bps",
        "volatility_20",
    ]].rename(columns={"close": "close_price"})

    logger.info("Built %d liquidity rows.", len(out))
    return out.sort_values(["date", "ticker"]).reset_index(drop=True)

# ---------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------

def save_liquidity(df: pd.DataFrame) -> None:
    logger.info("Saving liquidity dataset to %s", OUT_LIQUIDITY_FILE)
    df.to_csv(OUT_LIQUIDITY_FILE, index=False, encoding="utf-8")

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    logger.info("Starting market liquidity ingestion (patched version).")

    ohlcv = load_raw_ohlcv()
    liquidity = build_liquidity_timeseries(ohlcv)
    save_liquidity(liquidity)

    logger.info("Completed market liquidity ingestion successfully.")

if __name__ == "__main__":
    main()