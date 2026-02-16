"""
attribution_engine_v1.py

Canonical attribution engine for Quant v1.

- Source: C:\Quant\data\ingestion\ingestion_5years.csv
- Output: C:\Quant\data\analytics\reporting\quant_report_v1.csv

Computes:
- Daily returns per ticker
- 20-day rolling volatility
- 60-day rolling beta vs equal-weight market return
- Contribution = daily return

Writes rows with:
- REPORT_SECTION = "REPORT_ATTRIBUTION_ROLLING"
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

BASE_DIR = Path(r"C:\Quant")
INGESTION_FILE = BASE_DIR / "data" / "ingestion" / "ingestion_5years.csv"
REPORT_DIR = BASE_DIR / "data" / "analytics" / "reporting"
REPORT_FILE = REPORT_DIR / "quant_report_v1.csv"

def load_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Ingestion file not found: {path}")

    # Assume standard OHLCV with header; if not, this can be adjusted
    df = pd.read_csv(path)
    # Try to normalise column names
    cols = {c.lower(): c for c in df.columns}
    # Expect at least date, ticker, close
    date_col = next((c for c in df.columns if c.lower().startswith("date")), None)
    ticker_col = next((c for c in df.columns if "ticker" in c.lower()), None)
    close_col = next((c for c in df.columns if "close" in c.lower()), None)

    if date_col is None or ticker_col is None or close_col is None:
        raise ValueError(
            f"Expected date/ticker/close columns in {path}, "
            f"found: {list(df.columns)}"
        )

    df = df[[date_col, ticker_col, close_col]].copy()
    df.columns = ["date", "ticker", "close"]

    # Parse dates (day-first because of 07/01/2026 style)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values(["ticker", "date"])
    return df

def compute_attribution(df_prices: pd.DataFrame) -> pd.DataFrame:
    # Daily returns per ticker
    df_prices["return"] = df_prices.groupby("ticker")["close"].pct_change()

    # Equal-weight market return per date
    mkt = (
        df_prices
        .groupby("date", as_index=False)["return"]
        .mean()
        .rename(columns={"return": "mkt_return"})
    )
    df = df_prices.merge(mkt, on="date", how="left")

    # 20-day rolling volatility per ticker
    df["rolling_volatility"] = (
        df.groupby("ticker")["return"]
        .rolling(window=20, min_periods=10)
        .std()
        .reset_index(level=0, drop=True)
    )

    # 60-day rolling beta vs equal-weight market
    def rolling_beta(group, window=60, min_periods=20):
        r = group["return"]
        m = group["mkt_return"]
        cov = (
            r.rolling(window=window, min_periods=min_periods)
            .cov(m)
        )
        var = (
            m.rolling(window=window, min_periods=min_periods)
            .var()
        )
        beta = cov / var.replace(0, np.nan)
        return beta

    df["rolling_beta"] = (
        df.groupby("ticker", group_keys=False)
        .apply(rolling_beta)
    )

    # Contribution = daily return (can later be replaced with weight * return)
    df["contribution"] = df["return"]

    # Keep only rows where we have meaningful attribution
    mask = df[["contribution", "rolling_beta", "rolling_volatility"]].notnull().all(axis=1)
    df_attr = df[mask].copy()

    # Build report schema
    df_attr["REPORT_SECTION"] = "REPORT_ATTRIBUTION_ROLLING"
    df_attr = df_attr[[
        "date",
        "REPORT_SECTION",
        "ticker",
        "contribution",
        "rolling_beta",
        "rolling_volatility",
    ]]

    # Sort for readability
    df_attr = df_attr.sort_values(["date", "ticker"])
    return df_attr

def main():
    print("=" * 80)
    print(f"[{datetime.now()}] Quant v1 — Attribution Engine (from ingestion_5years)")
    print("=" * 80)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[{datetime.now()}] Loading prices from: {INGESTION_FILE}")
    df_prices = load_prices(INGESTION_FILE)
    print(f"[{datetime.now()}] Loaded {len(df_prices):,} price rows for {df_prices['ticker'].nunique()} tickers.")

    print(f"[{datetime.now()}] Computing attribution metrics (returns, beta, volatility, contribution)...")
    df_attr = compute_attribution(df_prices)
    print(f"[{datetime.now()}] Computed {len(df_attr):,} attribution rows.")

    if df_attr.empty:
        raise RuntimeError("Attribution computation produced no rows. Check ingestion_5years.csv content.")

    # Write report
    df_attr.to_csv(REPORT_FILE, index=False)
    latest_date = df_attr["date"].max()
    print(f"[{datetime.now()}] quant_report_v1.csv written to: {REPORT_FILE}")
    print(f"[{datetime.now()}] Latest attribution date in report: {latest_date.date()}")

    print("=" * 80)
    print(f"[{datetime.now()}] Attribution engine completed successfully.")
    print("=" * 80)

if __name__ == "__main__":
    main()