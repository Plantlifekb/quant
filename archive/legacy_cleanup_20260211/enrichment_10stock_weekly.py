"""
Module: enrichment_10stock_weekly.py
Quant variant: 10-stock weekly strategy
Purpose:
    Compute minimal enrichment fields needed to run a weekly 10-stock selection:
    - daily_return
    - weekly_return
    - volatility_20d
    - score (weekly_return)
    - monday_flag

Inputs:
    C:\Quant\data\ingestion\ingestion_5years.csv
        Required columns:
            date (ISO-8601)
            ticker
            close

Outputs:
    C:\Quant\data\enriched\enriched_10stock_weekly.csv
        Columns:
            date
            ticker
            close
            daily_return
            weekly_return
            volatility_20d
            score
            monday_flag

Governance:
    - No schema drift
    - All columns lowercase
    - Deterministic behaviour
    - No writing outside governed paths
"""

import os
import pandas as pd
from datetime import datetime

INPUT_PATH = r"C:\Quant\data\ingestion\ingestion_5years.csv"
OUTPUT_DIR = r"C:\Quant\data\enriched"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "enriched_10stock_weekly.csv")


def load_ingestion(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Basic governance checks
    required_cols = {"date", "ticker", "close"}
    missing = required_cols - set(c.lower() for c in df.columns)
    if missing:
        raise ValueError(f"Missing required columns in ingestion file: {missing}")

    # Normalize column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    # Ensure date is datetime
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

    # Sort deterministically
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def compute_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    # Group by ticker for rolling operations
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    # 5 trading days ago close for weekly_return
    df["close_lag_5"] = df.groupby("ticker")["close"].shift(5)
    df["weekly_return"] = (df["close"] - df["close_lag_5"]) / df["close_lag_5"]

    # 20-day rolling volatility of daily_return
    df["volatility_20d"] = (
        df.groupby("ticker")["daily_return"]
        .rolling(window=20, min_periods=20)
        .std()
        .reset_index(level=0, drop=True)
    )

    # Score = weekly_return (locked choice)
    df["score"] = df["weekly_return"]

    # Monday flag
    df["monday_flag"] = df["date"].dt.weekday == 0

    # Select and order final columns
    out_cols = [
        "date",
        "ticker",
        "close",
        "daily_return",
        "weekly_return",
        "volatility_20d",
        "score",
        "monday_flag",
    ]
    df_out = df[out_cols].copy()

    # Deterministic sort
    df_out = df_out.sort_values(["date", "ticker"]).reset_index(drop=True)
    return df_out


def main():
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Ingestion file not found: {INPUT_PATH}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_ing = load_ingestion(INPUT_PATH)
    df_enriched = compute_enrichment(df_ing)

    df_enriched.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")


if __name__ == "__main__":
    main()