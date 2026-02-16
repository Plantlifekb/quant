"""
Quant v1.0 — Portfolio performance reporting

Builds:
    C:\Quant\data\analytics\portfolio_performance_quant_v1.parquet
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")
DATA_INGESTION = BASE / "data" / "ingestion"
DATA_ANALYTICS = BASE / "data" / "analytics"

PRICES = DATA_INGESTION / "prices.parquet"
OPT = DATA_ANALYTICS / "optimiser_regime_quant_v1.parquet"
OUT = DATA_ANALYTICS / "portfolio_performance_quant_v1.parquet"


def main():
    print("\n=== BUILDING PORTFOLIO PERFORMANCE (Quant v1.0) ===\n")

    prices = pd.read_parquet(PRICES)
    opt = pd.read_parquet(OPT)

    prices["date"] = pd.to_datetime(prices["date"])
    opt["date"] = pd.to_datetime(opt["date"])

    prices = prices[["date", "ticker", "return"]].copy()
    opt = opt[["date", "ticker", "weight", "regime"]].copy()

    df = opt.merge(prices, on=["date", "ticker"], how="inner")

    df["contrib"] = df["weight"] * df["return"]
    perf = (
        df.groupby("date", as_index=False)
        .agg(
            portfolio_return=("contrib", "sum"),
            regime=("regime", "first"),
        )
        .sort_values("date")
    )

    perf["cumulative_return"] = (1 + perf["portfolio_return"]).cumprod() - 1

    OUT.parent.mkdir(parents=True, exist_ok=True)
    perf.to_parquet(OUT, index=False)

    print(f"✔ Wrote portfolio_performance_quant_v1.parquet → {OUT}\n")
    print("🎉 Portfolio performance reporting built successfully.\n")


if __name__ == "__main__":
    main()