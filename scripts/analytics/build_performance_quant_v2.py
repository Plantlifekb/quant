"""
Quant v2.0 — Performance Builder (Full History)

Computes daily and cumulative performance for each strategy.

Inputs:
    • portfolio_quant_v2.parquet   (date, ticker, weight[, strategy])
    • fundamentals.parquet         (date, ticker, ret)

Output:
    • C:\Quant\data\analytics\performance_quant_v2.parquet
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Quant")

PORT = BASE / "data" / "analytics" / "portfolio_quant_v2.parquet"
FUNDAMENTALS = BASE / "data" / "ingestion" / "fundamentals.parquet"
PERF_OUT = BASE / "data" / "analytics" / "performance_quant_v2.parquet"


def main():
    print("\n=== BUILDING PERFORMANCE (Quant v2.0) ===\n")

    port = pd.read_parquet(PORT)
    port["date"] = pd.to_datetime(port["date"])
    port["ticker"] = port["ticker"].astype(str).str.upper()

    # If no strategy column, assume long-only
    if "strategy" not in port.columns:
        port["strategy"] = "long_only"

    fnd = pd.read_parquet(FUNDAMENTALS)
    fnd["date"] = pd.to_datetime(fnd["date"])
    fnd["ticker"] = fnd["ticker"].astype(str).str.upper()

    rows = []

    for (strategy, date), p in port.groupby(["strategy", "date"]):
        p = p.copy()
        r = fnd[fnd["date"] == date][["ticker", "ret"]]
        merged = p.merge(r, on="ticker", how="left")

        if merged.empty:
            continue

        w = merged["weight"].astype(float).values
        ret = merged["ret"].astype(float).fillna(0).values

        port_ret = float(w @ ret)

        rows.append({
            "strategy": strategy,
            "date": date,
            "port_ret": port_ret,
        })

    perf = pd.DataFrame(rows).sort_values(["strategy", "date"]).reset_index(drop=True)

    # Cumulative performance per strategy
    perf["cum_ret"] = perf.groupby("strategy")["port_ret"].cumsum()
    perf["cum_growth"] = (1 + perf["port_ret"]).groupby(perf["strategy"]).cumprod()

    PERF_OUT.parent.mkdir(parents=True, exist_ok=True)
    perf.to_parquet(PERF_OUT, index=False)

    print(f"✔ Wrote performance to: {PERF_OUT}")
    print("\n🎉 Performance Builder (Quant v2.0) completed successfully.\n")


if __name__ == "__main__":
    main()