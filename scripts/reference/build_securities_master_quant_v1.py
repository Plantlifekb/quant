"""
build_securities_master_quant_v1.py

Quant v1.0 — Governed Securities Master Builder

Inputs:
    C:\Quant\config\ticker_reference.csv

Outputs:
    C:\Quant\data\reference\securities_master.parquet

Schema:
    ticker
    company_name
    sector
    industry
    country
    currency
    start_date   (default = 1900-01-01)
    end_date     (default = 2099-12-31)
"""

import pandas as pd
from pathlib import Path
import sys

BASE = Path(r"C:\Quant")
REF_IN = BASE / "config" / "ticker_reference.csv"
OUT = BASE / "data" / "reference" / "securities_master.parquet"


def fail(msg):
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg):
    print(f"✔ {msg}")


def main():
    print("\n=== BUILDING SECURITIES MASTER (Quant v1.0) ===\n")

    if not REF_IN.exists():
        fail(f"Ticker reference not found: {REF_IN}")

    try:
        df = pd.read_csv(REF_IN)
    except Exception as e:
        fail(f"Error reading ticker_reference.csv: {e}")

    # Normalise column names
    df.columns = [c.lower().strip() for c in df.columns]

    required = {"ticker", "company_name", "market_sector"}
    missing = required - set(df.columns)
    if missing:
        fail(f"ticker_reference.csv missing required columns: {missing}")

    # Build governed master
    master = pd.DataFrame()
    master["ticker"] = df["ticker"].astype(str).str.upper()
    master["company_name"] = df["company_name"]
    master["sector"] = df["market_sector"]

    # Add placeholder fields for future expansion
    master["industry"] = "UNKNOWN"
    master["country"] = "UNKNOWN"
    master["currency"] = "UNKNOWN"

    # Add time‑varying universe fields
    master["start_date"] = "1900-01-01"
    master["end_date"] = "2099-12-31"

    OUT.parent.mkdir(parents=True, exist_ok=True)

    try:
        master.to_parquet(OUT, index=False)
    except Exception as e:
        fail(f"Error writing securities_master.parquet: {e}")

    ok(f"Wrote securities_master.parquet → {OUT}")
    print("\n🎉 Securities master built successfully.\n")


if __name__ == "__main__":
    main()