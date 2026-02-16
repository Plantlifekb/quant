#!/usr/bin/env python3
# ==================================================================================================
# Quant v1.0 — Daily Enrichment (Governed)
# ==================================================================================================
# PURPOSE:
#   Take governed OHLCV ingestion output and compute daily, deterministic, reproducible features
#   suitable for downstream modeling and master dataset assembly.
#
# INPUTS:
#   C:\Quant\data\ingestion\ingestion_5years.csv
#
# OUTPUTS:
#   C:\Quant\data\enrichment\enriched_daily.csv
#
# SCHEMA (Quant v1.0, lowercase, non-drifting):
#   date              (YYYY-MM-DD)
#   ticker
#   company_name
#   market_sector
#   open
#   high
#   low
#   close
#   adj_close
#   volume
#   ret
#   log_ret
#   vol_5
#   vol_20
#   sma_5
#   sma_20
#   ema_12
#   ema_26
#   high_low_spread
#   volume_zscore
#   gap
#   overnight_ret
#   sector_rel_ret
#   outlier_flag
#   missing_flag
#   run_date         (YYYY-MM-DD HH:MM:SS UTC)
#
# GOVERNANCE:
#   • No schema drift.
#   • All column names lowercase.
#   • No overwriting ingestion outputs.
#   • Deterministic, reproducible calculations.
#   • No silent failures.
#   • All logs via logging_quant_v1.
#
# ==================================================================================================
# END OF HEADER — IMPLEMENTATION BEGINS BELOW
# ==================================================================================================

import os
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logging_quant_v1 import log

# ==================================================================================================
# PATHS
# ==================================================================================================

ROOT = r"C:\Quant"
DATA_INGESTION_DIR = os.path.join(ROOT, "data", "ingestion")
DATA_ENRICHMENT_DIR = os.path.join(ROOT, "data", "enrichment")

INGESTION_CSV = os.path.join(DATA_INGESTION_DIR, "ingestion_5years.csv")
OUT_CSV = os.path.join(DATA_ENRICHMENT_DIR, "enriched_daily.csv")

# ==================================================================================================
# CORE ENRICHMENT LOGIC
# ==================================================================================================

def compute_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """
    df: ingestion frame with columns:
        date, ticker, company_name, market_sector,
        open, high, low, close, adj_close, volume, run_date
    """

    # Ensure correct types
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Group by ticker for per-ticker features
    grouped = df.groupby("ticker", group_keys=False)

    def enrich_ticker(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").reset_index(drop=True)

        # Basic returns
        g["ret"] = g["close"] / g["close"].shift(1) - 1
        g["log_ret"] = np.log(g["close"]) - np.log(g["close"].shift(1))

        # Rolling volatility (on log returns)
        g["vol_5"] = g["log_ret"].rolling(window=5, min_periods=5).std()
        g["vol_20"] = g["log_ret"].rolling(window=20, min_periods=20).std()

        # Rolling averages
        g["sma_5"] = g["close"].rolling(window=5, min_periods=5).mean()
        g["sma_20"] = g["close"].rolling(window=20, min_periods=20).mean()

        # EMAs
        g["ema_12"] = g["close"].ewm(span=12, adjust=False).mean()
        g["ema_26"] = g["close"].ewm(span=26, adjust=False).mean()

        # High-low spread
        g["high_low_spread"] = g["high"] - g["low"]

        # Volume z-score (per ticker)
        vol_mean = g["volume"].rolling(window=20, min_periods=20).mean()
        vol_std = g["volume"].rolling(window=20, min_periods=20).std()
        g["volume_zscore"] = (g["volume"] - vol_mean) / vol_std.replace(0, np.nan)

        # Gap (open - previous close)
        g["gap"] = g["open"] - g["close"].shift(1)

        # Overnight return (open vs previous close)
        g["overnight_ret"] = g["open"] / g["close"].shift(1) - 1

        return g

    log("[enrichment_daily_quant_v1] Computing per-ticker features ...")
    df = grouped.apply(enrich_ticker)

    # Sector-relative return: ret - sector mean(ret) per date
    log("[enrichment_daily_quant_v1] Computing sector-relative returns ...")
    sector_group = df.groupby(["date", "market_sector"], group_keys=False)
    sector_mean_ret = sector_group["ret"].transform("mean")
    df["sector_rel_ret"] = df["ret"] - sector_mean_ret

    # Outlier flag: simple z-score on ret (per ticker)
    log("[enrichment_daily_quant_v1] Computing outlier flags ...")
    ret_group = df.groupby("ticker", group_keys=False)
    ret_mean = ret_group["ret"].transform("mean")
    ret_std = ret_group["ret"].transform("std").replace(0, np.nan)
    ret_z = (df["ret"] - ret_mean) / ret_std
    df["outlier_flag"] = (ret_z.abs() > 4).astype(int)

    # Missing flag: any critical field missing
    log("[enrichment_daily_quant_v1] Computing missing-data flags ...")
    critical_cols: List[str] = [
        "open", "high", "low", "close", "adj_close", "volume", "ret", "log_ret"
    ]
    df["missing_flag"] = df[critical_cols].isna().any(axis=1).astype(int)

    return df

# ==================================================================================================
# MAIN
# ==================================================================================================

def run() -> pd.DataFrame:
    log("[enrichment_daily_quant_v1] === Starting daily enrichment ===")

    if not os.path.exists(INGESTION_CSV):
        raise FileNotFoundError(f"Ingestion file not found at {INGESTION_CSV}")

    os.makedirs(DATA_ENRICHMENT_DIR, exist_ok=True)

    log(f"[enrichment_daily_quant_v1] Reading ingestion data from {INGESTION_CSV}")
    df = pd.read_csv(INGESTION_CSV)

    required_cols = {
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
    }
    missing = required_cols - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"Ingestion frame missing required columns: {missing}")

    # Normalize column names to lowercase (if not already)
    df.columns = [c.lower() for c in df.columns]

    enriched = compute_enrichment(df)

    # Set run_date for this enrichment run
    enriched["run_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Final column ordering
    final_cols = [
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
        "ret",
        "log_ret",
        "vol_5",
        "vol_20",
        "sma_5",
        "sma_20",
        "ema_12",
        "ema_26",
        "high_low_spread",
        "volume_zscore",
        "gap",
        "overnight_ret",
        "sector_rel_ret",
        "outlier_flag",
        "missing_flag",
        "run_date",
    ]

    # Ensure all expected columns exist
    for col in final_cols:
        if col not in enriched.columns:
            enriched[col] = np.nan

    enriched = enriched[final_cols].sort_values(["ticker", "date"]).reset_index(drop=True)

    # Basic sanity: ensure we didn't lose rows
    if len(enriched) != len(df):
        log(f"[enrichment_daily_quant_v1] WARNING: Row count changed from {len(df)} to {len(enriched)}")

    # Write output
    enriched.to_csv(OUT_CSV, index=False, encoding="utf-8")
    log(f"[enrichment_daily_quant_v1] Enrichment complete. Rows: {len(enriched)}")
    log(f"[enrichment_daily_quant_v1] Output written to {OUT_CSV}")

    return enriched

# ==================================================================================================

if __name__ == "__main__":
    df_enriched = run()
    log(f"[enrichment_daily_quant_v1] Enrichment complete (main). Rows: {len(df_enriched)}")