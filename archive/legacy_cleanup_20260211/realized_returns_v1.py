# =====================================================================
# Module: realized_returns_v1.py
# Quant Version: v1.0
#
# Purpose:
#   Generate the canonical realized returns dataset for Quant v1.0.
#   This file becomes the single source of truth for all realized
#   returns used by:
#       - the governed backtest engine
#       - the attribution engine
#       - performance analytics
#
# Description:
#   - Reads governed price history produced by ingestion_5years_quant_v1.py
#   - Computes close-to-close arithmetic returns
#   - Enforces ISO-8601 dates, lowercase columns, and deterministic output
#   - Writes quant_realized_returns_v1.csv to the governed analytics directory
#
# Inputs:
#   C:\Quant\data\analytics\quant_prices_v1.csv
#       Required columns:
#           - date (ISO-8601)
#           - ticker (uppercase)
#           - close
#
# Outputs:
#   C:\Quant\data\analytics\quant_realized_returns_v1.csv
#       Columns:
#           - date
#           - ticker
#           - return_close_to_close
#
# Governance Rules:
#   - No schema drift: output columns must match exactly.
#   - No hidden data sources: only governed price file may be used.
#   - Deterministic: same inputs must produce identical outputs.
#   - All dates must be ISO-8601.
#   - All tickers must be uppercase.
#   - All columns must be lowercase.
#
# Provenance:
#   - This module is part of the governed Quant v1.0 pipeline.
#   - Any change requires version bump (e.g., realized_returns_v1_1.py).
# =====================================================================

import os
import pandas as pd

DATA_DIR = r"C:\Quant\data"
ANALYTICS_DIR = os.path.join(DATA_DIR, "analytics")

PRICES_FILE = os.path.join(ANALYTICS_DIR, "quant_prices_v1.csv")
OUT_FILE = os.path.join(ANALYTICS_DIR, "quant_realized_returns_v1.csv")


def load_prices() -> pd.DataFrame:
    """
    Load governed price history and enforce schema.
    """
    df = pd.read_csv(PRICES_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Price file missing required columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()

    return df[["date", "ticker", "close"]]


def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute close-to-close arithmetic returns per ticker.
    """
    df = df.sort_values(["ticker", "date"])
    df["return_close_to_close"] = df.groupby("ticker")["close"].pct_change()
    df = df.dropna(subset=["return_close_to_close"])
    return df[["date", "ticker", "return_close_to_close"]]


def main():
    print("Loading governed price history...")
    prices = load_prices()

    print("Computing realized returns...")
    realized = compute_returns(prices)

    print(f"Writing canonical realized returns to: {OUT_FILE}")
    realized.to_csv(OUT_FILE, index=False, encoding="utf-8")

    print("Realized returns generation complete.")


if __name__ == "__main__":
    main()