"""
build_ingestion_parquet_quant_v1.py

Promotes the legacy ingestion_5years.csv into governed Quant v1.0
ingestion outputs:

- data/ingestion/prices.parquet
- data/reference/securities_master.parquet
"""

import sys
from pathlib import Path

import pandas as pd

BASE = Path(r"C:\Quant")

SRC_CSV = BASE / "data" / "ingestion" / "ingestion_5years.csv"
TICKER_REF = BASE / "config" / "ticker_reference.csv"

PRICES_OUT = BASE / "data" / "ingestion" / "prices.parquet"
SECMASTER_OUT = BASE / "data" / "reference" / "securities_master.parquet"


def fail(msg: str) -> None:
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"✔ {msg}")


def main() -> None:
    print("\n=== BUILDING GOVERNED INGESTION PARQUETS (Quant v1.0) ===\n")

    # ------------------------------------------------------------------
    # 1. Load 5-year ingestion CSV
    # ------------------------------------------------------------------
    if not SRC_CSV.exists():
        fail(f"Source ingestion file not found: {SRC_CSV}")

    try:
        df = pd.read_csv(SRC_CSV)
    except Exception as e:
        fail(f"Error reading {SRC_CSV}: {e}")

    required_cols = [
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
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        fail(f"ingestion_5years.csv missing required columns: {missing}")

    # Parse dates
    df["date"] = pd.to_datetime(df["date"])

    # ------------------------------------------------------------------
    # 2. Build prices.parquet (date, ticker, price, return)
    # ------------------------------------------------------------------
    prices = df[["date", "ticker", "adj_close"]].rename(columns={"adj_close": "price"})

    # Sort for stable return calculation
    prices = prices.sort_values(["ticker", "date"])

    # Compute simple returns by ticker
    prices["return"] = (
        prices.groupby("ticker")["price"].pct_change()
    )

    # Drop first row per ticker where return is NaN
    prices = prices.dropna(subset=["return"])

    # Sanity checks
    if prices.duplicated(subset=["date", "ticker"]).any():
        fail("prices would contain duplicate (date, ticker) rows")

    if prices[["date", "ticker", "price", "return"]].isna().any().any():
        fail("prices contains NaNs in required fields")

    PRICES_OUT.parent.mkdir(parents=True, exist_ok=True)
    try:
        prices.to_parquet(PRICES_OUT, index=False)
    except Exception as e:
        fail(f"Error writing {PRICES_OUT}: {e}")

    ok(f"Wrote governed prices.parquet → {PRICES_OUT}")

    # ------------------------------------------------------------------
    # 3. Build securities_master.parquet
    #    ticker, company_name, sector, industry, country, currency
    # ------------------------------------------------------------------
    # Start from ticker_reference if available, otherwise derive from ingestion
    if TICKER_REF.exists():
        try:
            ref = pd.read_csv(TICKER_REF)
        except Exception as e:
            fail(f"Error reading {TICKER_REF}: {e}")

        # Normalise expected columns
        # We allow some flexibility and then fill from ingestion where needed
        cols = ref.columns.str.lower()
        ref.columns = cols

        # Try to map to contract fields
        col_map = {}
        for target, candidates in {
            "ticker": ["ticker"],
            "company_name": ["company_name", "name"],
            "sector": ["sector", "market_sector"],
            "industry": ["industry", "gics_industry"],
            "country": ["country", "country_of_risk"],
            "currency": ["currency"],
        }.items():
            for c in candidates:
                if c in ref.columns:
                    col_map[target] = c
                    break

        missing_core = [k for k in ["ticker", "company_name", "sector"] if k not in col_map]
        if missing_core:
            # Fall back to ingestion-derived master
            print(
                "⚠ ticker_reference.csv missing some core fields; "
                "falling back to ingestion-derived securities_master for those."
            )
            base_master = (
                df[["ticker", "company_name", "market_sector"]]
                .drop_duplicates()
                .rename(columns={"market_sector": "sector"})
            )
            base_master["industry"] = "UNKNOWN"
            base_master["country"] = "UNKNOWN"
            base_master["currency"] = "UNKNOWN"
        else:
            master = ref.rename(columns={v: k for k, v in col_map.items()})
            base_master = master[["ticker", "company_name", "sector"]].drop_duplicates()
            # Optional fields with defaults
            base_master["industry"] = master.get("industry", "UNKNOWN")
            base_master["country"] = master.get("country", "UNKNOWN")
            base_master["currency"] = master.get("currency", "UNKNOWN")
    else:
        print(f"⚠ ticker_reference.csv not found at {TICKER_REF}, deriving master from ingestion only.")
        base_master = (
            df[["ticker", "company_name", "market_sector"]]
            .drop_duplicates()
            .rename(columns={"market_sector": "sector"})
        )
        base_master["industry"] = "UNKNOWN"
        base_master["country"] = "UNKNOWN"
        base_master["currency"] = "UNKNOWN"

    # Ensure no NaNs in required fields
    for c in ["ticker", "company_name", "sector"]:
        if base_master[c].isna().any():
            fail(f"securities_master would contain NaNs in required column: {c}")

    SECMASTER_OUT.parent.mkdir(parents=True, exist_ok=True)
    try:
        base_master.to_parquet(SECMASTER_OUT, index=False)
    except Exception as e:
        fail(f"Error writing {SECMASTER_OUT}: {e}")

    ok(f"Wrote governed securities_master.parquet → {SECMASTER_OUT}")

    print("\n🎉 Governed ingestion layer built successfully.\n")


if __name__ == "__main__":
    main()