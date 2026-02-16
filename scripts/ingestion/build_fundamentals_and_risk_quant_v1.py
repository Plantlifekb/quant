"""
build_fundamentals_and_risk_quant_v1.py

Completes the Quant v1.0 ingestion layer by producing:

- data/ingestion/fundamentals.parquet
- data/ingestion/risk_model.parquet

Sources:
- fundamentals → quant_master.csv (values)
- metadata (company_name, sector) → ticker_reference.csv
- risk model → quant_risk_model_v1.csv
"""

import sys
from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")

# ----------------------------------------------------------------------
# Source files
# ----------------------------------------------------------------------
SRC_FUND = BASE / "data" / "master" / "quant_master.csv"
SRC_META = BASE / "config" / "ticker_reference.csv"
SRC_RISK = BASE / "data" / "analytics" / "risk" / "quant_risk_model_v1.csv"

# ----------------------------------------------------------------------
# Output files
# ----------------------------------------------------------------------
OUT_FUND = BASE / "data" / "ingestion" / "fundamentals.parquet"
OUT_RISK = BASE / "data" / "ingestion" / "risk_model.parquet"


def fail(msg):
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg):
    print(f"✔ {msg}")


def main():
    print("\n=== BUILDING FUNDAMENTALS + RISK MODEL (Quant v1.0) ===\n")

    # ==================================================================
    # 1. LOAD FUNDAMENTALS (quant_master.csv)
    # ==================================================================
    if not SRC_FUND.exists():
        fail(f"Fundamentals source not found: {SRC_FUND}")

    try:
        fund = pd.read_csv(SRC_FUND)
    except Exception as e:
        fail(f"Error reading fundamentals CSV: {e}")

    fund.columns = [c.lower() for c in fund.columns]

    if "ticker" not in fund.columns:
        fail("quant_master.csv must contain 'ticker'")

    # ==================================================================
    # 2. LOAD METADATA FROM TICKER REFERENCE
    # ==================================================================
    if not SRC_META.exists():
        fail(f"Ticker reference not found: {SRC_META}")

    try:
        meta = pd.read_csv(SRC_META)
    except Exception as e:
        fail(f"Error reading ticker_reference.csv: {e}")

    meta.columns = [c.lower() for c in meta.columns]

    required_meta = ["ticker", "company_name", "market_sector"]
    missing = [c for c in required_meta if c not in meta.columns]
    if missing:
        fail(f"ticker_reference.csv missing required fields: {missing}")

    meta = meta.rename(columns={"market_sector": "sector"})

    # ==================================================================
    # 3. MERGE FUNDAMENTALS WITH METADATA
    # ==================================================================
    merged = pd.merge(fund, meta, on="ticker", how="left")

    if merged["sector"].isna().any():
        fail("Some tickers in quant_master.csv do not appear in ticker_reference.csv")

    # Add optional fields if missing
    for col in ["industry", "country", "currency"]:
        if col not in merged.columns:
            merged[col] = "UNKNOWN"

    # ==================================================================
    # 4. WRITE FUNDAMENTALS PARQUET
    # ==================================================================
    OUT_FUND.parent.mkdir(parents=True, exist_ok=True)
    try:
        merged.to_parquet(OUT_FUND, index=False)
    except Exception as e:
        fail(f"Error writing fundamentals.parquet: {e}")

    ok(f"Wrote fundamentals.parquet → {OUT_FUND}")

    # ==================================================================
    # 5. RISK MODEL
    # ==================================================================
    if not SRC_RISK.exists():
        fail(f"Risk model source not found: {SRC_RISK}")

    try:
        risk = pd.read_csv(SRC_RISK)
    except Exception as e:
        fail(f"Error reading risk model CSV: {e}")

    risk.columns = [c.lower() for c in risk.columns]

    if "date" not in risk.columns or "ticker" not in risk.columns:
        fail("Risk model must contain 'date' and 'ticker' columns")

    risk["date"] = pd.to_datetime(risk["date"])

    if risk.duplicated(subset=["date", "ticker"]).any():
        fail("Risk model contains duplicate (date, ticker) rows")

    OUT_RISK.parent.mkdir(parents=True, exist_ok=True)
    try:
        risk.to_parquet(OUT_RISK, index=False)
    except Exception as e:
        fail(f"Error writing risk_model.parquet: {e}")

    ok(f"Wrote risk_model.parquet → {OUT_RISK}")

    print("\n🎉 Governed fundamentals + risk model built successfully.\n")


if __name__ == "__main__":
    main()