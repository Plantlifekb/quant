import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"C:\Quant")
DATA_ANALYTICS = BASE_DIR / "data" / "analytics"
DATA_REFERENCE = BASE_DIR / "data" / "reference"

OPTIMISER_FILE = DATA_ANALYTICS / "optimiser_regime_quant_v1.parquet"
SECMASTER_FILE = DATA_REFERENCE / "securities_master.parquet"
OUTPUT_FILE = DATA_ANALYTICS / "quant_weekly_picks_quant_v1.parquet"

TOP_N = 10
PORTFOLIO_TYPE = "long_only"

def build_weekly_picks():
    # Load optimiser output
    opt = pd.read_parquet(OPTIMISER_FILE)

    # Assume columns: date, ticker, weight, portfolio_type (or similar)
    opt["date"] = pd.to_datetime(opt["date"]).dt.date

    # Filter to long-only portfolio if needed
    if "portfolio_type" in opt.columns:
        opt_lo = opt[opt["portfolio_type"] == PORTFOLIO_TYPE].copy()
    else:
        opt_lo = opt.copy()

    # Latest date
    latest_date = opt_lo["date"].max()
    df_latest = opt_lo[opt_lo["date"] == latest_date].copy()

    # Sort by weight descending and take top N
    df_latest = df_latest.sort_values("weight", ascending=False).head(TOP_N)

    # Load security master for metadata
    sec = pd.read_parquet(SECMASTER_FILE)  # columns: ticker, company_name, sector
    df = df_latest.merge(sec, on="ticker", how="left")

    # Rank
    df = df.sort_values("weight", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1

    # Standardised columns
    df_out = df[[
        "date",
        "rank",
        "ticker",
        "company_name",
        "sector",
        "weight"
    ]].rename(columns={"date": "as_of_date"})

    # Write governed output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(OUTPUT_FILE, index=False)

    print(f"[weekly_picks_quant_v1] Wrote weekly picks for {latest_date} to {OUTPUT_FILE}")

if __name__ == "__main__":
    try:
        build_weekly_picks()
    except Exception as e:
        # print full error for logs and exit non-zero so orchestrator can detect failure
        import traceback, sys
        traceback.print_exc()
        print(f"[weekly_picks_quant_v1] FAILED: {e}")
        sys.exit(1)
    else:
        sys.exit(0)
