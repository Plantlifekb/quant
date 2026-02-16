#!/usr/bin/env python3
from pathlib import Path
import pandas as pd

ANALYTICS = Path(r"C:\Quant\data\analytics")
OUT = ANALYTICS / "company_metadata.csv"
CANDIDATES = [
    ANALYTICS / "weekly_picks_quant_v2.parquet",
    ANALYTICS / "quant_weekly_picks_quant_v1.parquet",
    ANALYTICS / "quant_weekly_picks_quant_v1.backup.parquet",
]

def main():
    df = None
    for p in CANDIDATES:
        if p.exists():
            try:
                df = pd.read_parquet(p)
                break
            except Exception:
                continue
    if df is None or df.empty:
        print("No picks parquet found or readable. Exiting.")
        return
    df.columns = [c.strip() for c in df.columns]
    ticker_col = None
    for cand in ["ticker", "symbol", "asset"]:
        if cand in df.columns:
            ticker_col = cand
            break
    if ticker_col is None:
        print("No ticker column found in picks. Exiting.")
        return
    if "company_name" not in df.columns:
        df["company_name"] = None
    if "sector" not in df.columns:
        df["sector"] = None
    meta = df[[ticker_col, "company_name", "sector"]].drop_duplicates(subset=[ticker_col]).rename(columns={ticker_col: "ticker"})
    meta.to_csv(OUT, index=False)
    print("Wrote company metadata to", OUT)

if __name__ == "__main__":
    main()